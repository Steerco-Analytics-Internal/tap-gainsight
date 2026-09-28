"""Behavior tests for every stream, against doc-shaped responses."""

from __future__ import annotations

import copy
import json
import typing as t
from dataclasses import dataclass, field

import pytest
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_gainsight import client
from tests.conftest import BASE_URL, load, make_tap, query_page, query_url

CTA_URL = f"{BASE_URL}/v2/cockpit/cta/list"
T0 = 1707121475253  # 2024-02-05T08:24:35.253Z in epoch milliseconds.


def iso(ms: int) -> str:
    return client.to_iso_datetime(ms)


def company_row(i: int, modified: t.Optional[int] = None) -> dict:
    """The documented Company read row, plus Gsid and ModifiedDate."""
    row = copy.deepcopy(load("company_query_response.json")["data"][0])
    row["Gsid"] = f"1P02COMPANY{i:04d}"
    row["ModifiedDate"] = T0 + i * 1000 if modified is None else modified
    return row


def timeline_row(i: int, modified: t.Optional[int] = None) -> dict:
    """The documented Timeline read record, plus Gsid and LastModifiedDate."""
    row = copy.deepcopy(load("timeline_query_response.json")["data"]["records"][0])
    row["Gsid"] = f"1A01ACTIVITY{i:04d}"
    row["LastModifiedDate"] = T0 + i * 1000 if modified is None else modified
    return row


def cta_row(i: int, modified: t.Optional[str] = None) -> dict:
    """The documented Fetch CTA record, with ModifiedDate from the deleted-CTA sample."""
    row = copy.deepcopy(load("cta_list_response.json")["data"][0])
    row["Gsid"] = f"1S01CTA{i:04d}"
    row["ModifiedDate"] = modified or f"2024-02-05T09:{i % 60:02d}:35.253Z"
    return row


def deleted_row(i: int, deleted: t.Optional[str] = None) -> dict:
    row = copy.deepcopy(load("delete_log_response.json")["data"]["records"][0])
    row["RecordId"] = f"1P02DELETED{i:04d}"
    row["DeletedOn"] = deleted or f"2024-02-05T09:{i % 60:02d}:16Z"
    return row


@dataclass
class Spec:
    name: str
    url: str
    make_row: t.Callable[[int], dict]
    page: t.Callable[[t.List[dict]], dict]
    token_key: str
    first_token: int
    replication_key: str
    context: t.Optional[dict] = None
    extra_urls: t.List[str] = field(default_factory=list)


SPECS = {
    "Company": Spec("Company", query_url("Company"), company_row, query_page, "offset", 0, "ModifiedDate"),
    "timeline": Spec(
        "timeline",
        query_url("activity_timeline"),
        timeline_row,
        lambda rows: query_page(rows, records_shape=True),
        "offset",
        0,
        "LastModifiedDate",
    ),
    "cta": Spec(
        "cta",
        CTA_URL,
        cta_row,
        lambda rows: {**load("cta_list_response.json"), "data": rows},
        "pageNumber",
        1,
        "ModifiedDate",
    ),
    "deleted_records": Spec(
        "deleted_records",
        query_url("record_delete_log"),
        deleted_row,
        lambda rows: {**load("delete_log_response.json"), "data": {"records": rows}},
        "offset",
        0,
        "DeletedOn",
        context={"delete_log": "record_delete_log"},
        extra_urls=[query_url("record_delete_log_high_volume")],
    ),
}
ALL = list(SPECS)


def calls_to(api, url):
    return [r for r in api.mocker.request_history if r.url.split("?")[0].lower() == url.lower()]


def register(api, spec, responses):
    api.mocker.post(spec.url, responses)
    empty = spec.page([])
    for url in spec.extra_urls:
        api.mocker.post(url, json=empty)


def records(stream, spec):
    return list(stream.get_records(spec.context))


def messages(capsys) -> t.List[dict]:
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.startswith("{")]


def bookmark(state_message: dict, spec: Spec) -> t.Any:
    stream_state = state_message["value"]["bookmarks"][spec.name]
    if spec.context:
        for partition in stream_state["partitions"]:
            if partition["context"] == spec.context:
                return partition.get("replication_key_value")
        return None
    return stream_state.get("replication_key_value")


def state_for(spec: Spec, value: str) -> dict:
    entry = {"replication_key": spec.replication_key, "replication_key_value": value}
    if spec.context:
        return {"bookmarks": {spec.name: {"partitions": [{"context": spec.context, **entry}]}}}
    return {"bookmarks": {spec.name: entry}}


# Pagination


@pytest.mark.parametrize("name", ALL)
def test_pagination_stops_on_a_short_page(api, name):
    spec = SPECS[name]
    rows = [spec.make_row(i) for i in range(3)]
    register(api, spec, [{"json": spec.page(rows[:2])}, {"json": spec.page(rows[2:])}])
    stream = make_tap().streams[name]
    stream.page_size = 2
    assert len(records(stream, spec)) == 3
    tokens = [r.json()[spec.token_key] for r in calls_to(api, spec.url)]
    assert tokens == [spec.first_token, spec.first_token + (2 if spec.token_key == "offset" else 1)]


@pytest.mark.parametrize("name", ALL)
def test_pagination_with_an_exact_multiple_of_the_limit(api, name):
    spec = SPECS[name]
    rows = [spec.make_row(i) for i in range(4)]
    register(
        api,
        spec,
        [
            {"json": spec.page(rows[:2])},
            {"json": spec.page(rows[2:])},
            {"json": spec.page([])},
        ],
    )
    stream = make_tap().streams[name]
    stream.page_size = 2
    assert [r[spec.replication_key] is not None for r in records(stream, spec)] == [True] * 4
    tokens = [r.json()[spec.token_key] for r in calls_to(api, spec.url)]
    step = 2 if spec.token_key == "offset" else 1
    assert tokens == [spec.first_token + step * n for n in range(3)]
    for request in calls_to(api, spec.url):
        body = request.json()
        assert body.get("limit", body.get("pageSize")) == 2


# Empty results


@pytest.mark.parametrize("name", ALL)
def test_empty_result(api, name):
    spec = SPECS[name]
    register(api, spec, [{"json": spec.page([])}])
    assert records(make_tap().streams[name], spec) == []
    assert len(calls_to(api, spec.url)) == 1


@pytest.mark.parametrize(
    "fixture",
    [
        "company_query_no_data_response.json",
        "custom_object_query_empty_response.json",
    ],
)
def test_documented_empty_replies_end_the_stream(api, fixture):
    api.mocker.post(query_url("Company"), json=load(fixture))
    assert list(make_tap().streams["Company"].get_records(None)) == []


# Incremental state


@pytest.mark.parametrize("name", ALL)
def test_incremental_from_nothing(api, name, capsys):
    spec = SPECS[name]
    rows = [spec.make_row(i) for i in (2, 0, 1)]
    register(api, spec, [{"json": spec.page(rows)}])
    make_tap().streams[name].sync()
    body = calls_to(api, spec.url)[0].json()
    assert "where" not in body
    states = [m for m in messages(capsys) if m["type"] == "STATE"]
    expected = max(client.to_iso_datetime(r[spec.replication_key]) for r in rows)
    assert bookmark(states[-1], spec).startswith(expected[:19])


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Company", "2024-01-01 00:00:00"),
        ("timeline", "2024-01-01 00:00:00"),
        ("cta", "2023-12-31"),
        ("deleted_records", "2024-01-01 00:00:00"),
    ],
)
def test_incremental_from_start_date(api, name, expected):
    spec = SPECS[name]
    register(api, spec, [{"json": spec.page([])}])
    make_tap(start_date="2024-01-01T00:00:00Z").streams[name].sync()
    condition = calls_to(api, spec.url)[0].json()["where"]["conditions"][0]
    assert condition["value"] == [expected]
    assert condition["operator"] == "GTE"


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Company", "2024-02-05 08:24:35"),
        ("timeline", "2024-02-05 08:24:35"),
        ("cta", "2024-02-04"),
        ("deleted_records", "2024-02-05 08:24:35"),
    ],
)
def test_incremental_from_a_bookmark_advances_it(api, name, expected, capsys):
    spec = SPECS[name]
    rows = [spec.make_row(i) for i in (5, 7)]
    register(api, spec, [{"json": spec.page(rows)}])
    state = state_for(spec, "2024-02-05T08:24:35.253000+00:00")
    tap = make_tap(state=state, start_date="2020-01-01T00:00:00Z")
    tap.streams[name].sync()
    condition = calls_to(api, spec.url)[0].json()["where"]["conditions"][0]
    assert condition["value"] == [expected]
    states = [m for m in messages(capsys) if m["type"] == "STATE"]
    final = bookmark(states[-1], spec)
    newest = client.to_iso_datetime(rows[-1][spec.replication_key])
    assert final.startswith(newest[:19])
    assert final > "2024-02-05T08:24:35.253000+00:00"


# Null and missing fields, dates, labels, lookups


def test_mda_nulls_missing_fields_dates_labels_and_lookups(api):
    full = company_row(1)
    full["Stage"] = "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3"
    full["License_Type__gc"] = "1I00AABNSR5CLLWSP3F09ND6JHASXGUW8BW4"
    full["Csm__gr.Email"] = "jnash@heroku.com"
    nulls = {"Gsid": "1P02COMPANYNULL", "ModifiedDate": None, "Stage": None, "Name": None}
    missing = {"Gsid": "1P02COMPANYMISS"}
    multi = {"Gsid": "1P02COMPANYMULT", "Stage": [
        "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3", "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF4"
    ]}
    semi = {"Gsid": "1P02COMPANYSEMI", "Stage": "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3;unknown"}
    api.mocker.post(query_url("Company"), json=query_page([full, nulls, missing, multi, semi]))
    got = {r["Gsid"]: r for r in make_tap().streams["Company"].get_records(None)}

    record = got[full["Gsid"]]
    assert record["ModifiedDate"] == iso(full["ModifiedDate"])
    assert record["Renewal_Date"] == "2018-03-22T18:23:38.667000+00:00"
    assert record["Stage_label"] == "Kicked Off"
    assert record["License_Type__gc_label"] == "External"
    assert record["Csm__gr.Email"] == "jnash@heroku.com"

    assert got["1P02COMPANYNULL"]["ModifiedDate"] is None
    assert got["1P02COMPANYNULL"]["Name"] is None
    assert got["1P02COMPANYNULL"]["Stage_label"] is None
    assert got["1P02COMPANYMISS"]["Stage_label"] is None
    assert "Name" not in got["1P02COMPANYMISS"]
    assert got["1P02COMPANYMULT"]["Stage_label"] == ["Kicked Off", "Launched"]
    assert got["1P02COMPANYSEMI"]["Stage_label"] == ["Kicked Off", None]


def test_cta_records_keep_documented_values(api):
    page = load("cta_list_response.json")
    page["data"][1]["Quoted_ARR__gc"] = None
    api.mocker.post(CTA_URL, json=page)
    got = list(make_tap().streams["cta"].get_records(None))
    assert [r["TypeId__gr.Name"] for r in got] == ["Risk", "Risk"]
    assert got[0]["DueDate"] == "2020-04-14T11:30:00Z"
    assert got[0]["associatedRecords"][0]["source"] == "MDA"
    assert got[1]["Quoted_ARR__gc"] is None
    assert "ModifiedDate" not in got[0]


def test_deleted_records_read_both_logs(api, capsys):
    api.mocker.post(query_url("record_delete_log"), json=load("delete_log_response.json"))
    api.mocker.post(
        query_url("record_delete_log_high_volume"),
        json=load("custom_object_query_empty_response.json"),
    )
    make_tap().streams["deleted_records"].sync()
    out = messages(capsys)
    got = [m["record"] for m in out if m["type"] == "RECORD"]
    assert len(got) == 4
    assert got[0] == {
        "RecordId": "1P02BVHREJTPN50SVSAWIPFJMV40UK9GJH6U",
        "DeletedOn": "2024-02-05T07:17:16Z",
        "ObjectName": "Company",
    }
    assert len(calls_to(api, query_url("record_delete_log_high_volume"))) == 1


# Failures


@pytest.mark.parametrize("name", ALL)
@pytest.mark.parametrize("status", [401, 403])
def test_auth_failure_fails_fast_with_a_clear_message(api, name, status):
    spec = SPECS[name]
    register(api, spec, [{"status_code": status, "json": load("unauthorized_response.json")}])
    with pytest.raises(FatalAPIError, match=f"{status} Client Error.*rejected the access key.*GS_APIG_2401"):
        records(make_tap().streams[name], spec)
    assert len(calls_to(api, spec.url)) == 1


@pytest.mark.parametrize("name", ALL)
def test_unauthorized_body_on_200_fails_fast(api, name):
    spec = SPECS[name]
    register(api, spec, [{"json": load("unauthorized_response.json")}])
    with pytest.raises(FatalAPIError, match="rejected the access key"):
        records(make_tap().streams[name], spec)
    assert len(calls_to(api, spec.url)) == 1


@pytest.mark.parametrize("name", ALL)
def test_429_and_5xx_are_retried(api, name, sleeps):
    spec = SPECS[name]
    register(
        api,
        spec,
        [
            {"status_code": 429, "text": "Too Many Requests"},
            {"status_code": 502, "text": "Bad Gateway"},
            {"json": spec.page([spec.make_row(1)])},
        ],
    )
    assert len(records(make_tap().streams[name], spec)) == 1
    assert len(calls_to(api, spec.url)) == 3
    assert len(sleeps) >= 2


@pytest.mark.parametrize("name", ALL)
@pytest.mark.parametrize("status", [429, 500, 503])
def test_retries_give_up_with_the_status_and_body(api, name, status):
    spec = SPECS[name]
    register(api, spec, [{"status_code": status, "text": "upstream says no"}])
    with pytest.raises(RetriableAPIError, match=f"{status} .*Body: upstream says no"):
        records(make_tap().streams[name], spec)
    assert len(calls_to(api, spec.url)) == client.MAX_TRIES


@pytest.mark.parametrize("name", ALL)
def test_malformed_json_fails(api, name):
    spec = SPECS[name]
    register(api, spec, [{"text": "<html>maintenance</html>"}])
    with pytest.raises(FatalAPIError, match="is not JSON.*maintenance"):
        records(make_tap().streams[name], spec)


@pytest.mark.parametrize("name", ALL)
def test_other_4xx_fails_with_status_and_body(api, name):
    spec = SPECS[name]
    register(api, spec, [{"status_code": 400, "json": load("cta_list_invalid_select_response.json")}])
    with pytest.raises(FatalAPIError, match="400 Client Error.*COCKPIT_5101"):
        records(make_tap().streams[name], spec)


def test_documented_cta_error_on_200_fails(api):
    api.mocker.post(CTA_URL, json=load("cta_list_invalid_select_response.json"))
    with pytest.raises(FatalAPIError, match="result=false.*Invalid fields in select clause"):
        list(make_tap().streams["cta"].get_records(None))


def test_unexpected_data_shape_fails(api):
    api.mocker.post(query_url("Company"), json={"result": True, "data": "surprise"})
    with pytest.raises(FatalAPIError, match="Unexpected `data` shape"):
        list(make_tap().streams["Company"].get_records(None))


# Rate limiter


class CountingLimiter:
    def __init__(self):
        self.calls = 0

    def acquire(self):
        self.calls += 1
        return 0.0


@pytest.mark.parametrize("name", ALL)
def test_every_request_attempt_goes_through_the_rate_limiter(api, name):
    spec = SPECS[name]
    register(
        api,
        spec,
        [{"status_code": 429, "text": "slow"}, {"json": spec.page([spec.make_row(1)])}],
    )
    tap = make_tap()
    limiter = CountingLimiter()
    tap._rate_limiter = limiter
    records(tap.streams[name], spec)
    assert limiter.calls == len(calls_to(api, spec.url)) == 2


def test_the_limiter_throttles_a_long_sync(api, sleeps):
    rows = [company_row(i) for i in range(3)]
    api.mocker.post(query_url("Company"), [{"json": query_page([r])} for r in rows] + [{"json": query_page([])}])
    tap = make_tap()
    fake_now = [0.0]
    tap._rate_limiter = client.RateLimiter(
        calls=2, period=60, clock=lambda: fake_now[0], sleep=lambda s: fake_now.__setitem__(0, fake_now[0] + s)
    )
    stream = tap.streams["Company"]
    stream.page_size = 1
    assert len(list(stream.get_records(None))) == 3
    # Four requests at two per minute: the third waits a full window.
    assert fake_now[0] == 60.0


# Catalog selection


def catalog_with(tap, stream_name, deselect=(), select=()):
    catalog = tap.catalog_dict
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            breadcrumb = item["breadcrumb"]
            if not breadcrumb:
                item["metadata"]["selected"] = entry["tap_stream_id"] == stream_name
            elif breadcrumb[-1] in deselect and entry["tap_stream_id"] == stream_name:
                item["metadata"]["selected"] = False
            elif breadcrumb[-1] in select and entry["tap_stream_id"] == stream_name:
                item["metadata"]["selected"] = True
    return catalog


def test_select_list_follows_the_catalog(api, capsys):
    api.mocker.post(query_url("Company"), json=query_page([{**company_row(1), "Stage": "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3", "Health_Notes__gc": "ok"}]))
    catalog = catalog_with(make_tap(), "Company", deselect={"Health_Notes__gc", "Stage", "Csm__gr.Email"})
    tap = make_tap(catalog=catalog)
    stream = tap.streams["Company"]
    select = stream.select_paths()
    assert "Health_Notes__gc" not in select and "Csm__gr.Email" not in select
    # The label is still selected, so its id field is selected for the query.
    assert "Stage" in select and "Csm__gr.Name" in select
    tap.sync_all()
    body = calls_to(api, query_url("Company"))[0].json()
    assert body["select"] == select
    record = next(m["record"] for m in messages(capsys) if m["type"] == "RECORD")
    assert "Health_Notes__gc" not in record and "Stage" not in record
    assert record["Stage_label"] == "Kicked Off"


def test_deselected_label_does_not_add_its_id(api):
    catalog = catalog_with(make_tap(), "Company", deselect={"Stage", "Stage_label"})
    stream = make_tap(catalog=catalog).streams["Company"]
    assert "Stage" not in stream.select_paths()


def test_cta_select_follows_the_catalog(api):
    api.mocker.post(CTA_URL, json={"result": True, "data": []})
    catalog = catalog_with(make_tap(), "cta", deselect={"Comments", "Quoted_ARR__gc"})
    tap = make_tap(catalog=catalog)
    tap.sync_all()
    select = calls_to(api, CTA_URL)[0].json()["select"]
    assert "Comments" not in select and "Quoted_ARR__gc" not in select
    assert "ModifiedDate" in select and "customDate__gc" in select
