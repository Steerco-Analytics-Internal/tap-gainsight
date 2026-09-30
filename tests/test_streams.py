"""Behavior tests for every stream, against a fake API that honors queries."""

from __future__ import annotations

import copy
import datetime
import json
import logging
import typing as t
from dataclasses import dataclass, field

import pytest
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_gainsight import client
from tests.conftest import BASE_URL, QueryEngine, load, make_tap, query_url

CTA_URL = f"{BASE_URL}/v2/cockpit/cta/list"
CTA_DELETED_URL = f"{BASE_URL}/v2/cockpit/cta/deleted/list"
T0 = 1707121475253  # 2024-02-05T08:24:35.253Z in epoch milliseconds.
UTC = datetime.timezone.utc
DAY_MS = 86_400_000
DEFAULT: t.Any = object()


def moment(ms: int) -> datetime.datetime:
    return client.EPOCH + datetime.timedelta(milliseconds=ms)


def iso_ms(ms: int) -> str:
    """ISO with milliseconds and Z, as the CTA samples show."""
    value = moment(ms)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def iso_s(ms: int) -> str:
    """ISO with whole seconds and Z, as the delete log sample shows."""
    return moment(ms).strftime("%Y-%m-%dT%H:%M:%SZ")


def company_row(i: int, modified: t.Any = DEFAULT) -> dict:
    """The documented Company read row, plus Gsid and ModifiedDate."""
    row = copy.deepcopy(load("company_query_response.json")["data"][0])
    row["Gsid"] = f"1P02COMPANY{i:04d}"
    row["ModifiedDate"] = T0 + i * 1000 if modified is DEFAULT else modified
    return row


def timeline_row(i: int, modified: t.Any = DEFAULT) -> dict:
    """The documented Timeline read record, plus Gsid and ModifiedDate."""
    row = copy.deepcopy(load("timeline_query_response.json")["data"]["records"][0])
    row["Gsid"] = f"1A01ACTIVITY{i:04d}"
    row["ModifiedDate"] = T0 + i * 1000 if modified is DEFAULT else modified
    return row


def cta_row(i: int, modified: t.Any = DEFAULT) -> dict:
    """The documented Fetch CTA record, with a ModifiedDate like the deleted-CTA sample."""
    row = copy.deepcopy(load("cta_list_response.json")["data"][0])
    row["Gsid"] = f"1S01CTA{i:04d}"
    row["ModifiedDate"] = iso_ms(T0 + i * DAY_MS) if modified is DEFAULT else modified
    return row


def cta_deleted_row(i: int, modified: t.Any = DEFAULT) -> dict:
    row = copy.deepcopy(load("cta_deleted_list_response.json")["data"][0])
    row["Gsid"] = f"1S01DELCTA{i:04d}"
    row["ModifiedDate"] = iso_ms(T0 + i * DAY_MS) if modified is DEFAULT else modified
    return row


def deleted_row(i: int, deleted: t.Any = DEFAULT) -> dict:
    row = copy.deepcopy(load("delete_log_response.json")["data"]["records"][0])
    row["RecordId"] = f"1P02DELETED{i:04d}"
    row["DeletedOn"] = iso_s(T0 + i * 1000) if deleted is DEFAULT else deleted
    return row


@dataclass
class Spec:
    name: str
    url: str
    make_row: t.Callable[..., dict]
    rk: str
    key: str
    shape: str = "list"
    unordered: bool = False
    context: t.Optional[dict] = None
    extra_urls: t.List[str] = field(default_factory=list)
    date_fields: t.Tuple[str, ...] = ("ModifiedDate",)
    # First filter value for start_date 2024-01-01T00:00:00Z.
    start_value: str = "2023-12-31 00:00:00"


SPECS = {
    "Company": Spec("Company", query_url("Company"), company_row, "ModifiedDate", "Gsid"),
    "timeline": Spec("timeline", query_url("activity_timeline"), timeline_row, "ModifiedDate", "Gsid", shape="records"),
    "cta": Spec("cta", CTA_URL, cta_row, "ModifiedDate", "Gsid", unordered=True, start_value="2023-12-31"),
    "cta_deleted": Spec(
        "cta_deleted", CTA_DELETED_URL, cta_deleted_row, "ModifiedDate", "Gsid", unordered=True, start_value="2023-12-31"
    ),
    "deleted_records": Spec(
        "deleted_records",
        query_url("record_delete_log"),
        deleted_row,
        "DeletedOn",
        "RecordId",
        shape="records",
        context={"delete_log": "record_delete_log"},
        extra_urls=[query_url("record_delete_log_high_volume")],
        date_fields=("DeletedOn",),
    ),
}
ALL = list(SPECS)
CTA_STREAMS = {"cta", "cta_deleted"}


def serve(api, spec: Spec, rows: t.List[dict], **kwargs) -> QueryEngine:
    engine = QueryEngine(rows, spec.date_fields, shape=spec.shape, unordered=spec.unordered, **kwargs)
    api.serve(spec.url, engine)
    for url in spec.extra_urls:
        api.serve(url, QueryEngine([], spec.date_fields, shape=spec.shape))
    return engine


def register_then_serve(api, spec: Spec, failures: t.List[dict], rows: t.List[dict]) -> QueryEngine:
    """Answer the first requests with `failures`, then from an engine."""
    engine = QueryEngine(rows, spec.date_fields, shape=spec.shape, unordered=spec.unordered)
    api.mocker.post(spec.url, failures + [{"json": lambda request, context: engine.respond(request.json())}])
    for url in spec.extra_urls:
        api.serve(url, QueryEngine([], spec.date_fields, shape=spec.shape))
    return engine


def calls_to(api, url):
    return [r for r in api.mocker.request_history if r.url.split("?")[0] == url]


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
    entry = {"replication_key": spec.rk, "replication_key_value": value}
    if spec.context:
        return {"bookmarks": {spec.name: {"partitions": [{"context": spec.context, **entry}]}}}
    return {"bookmarks": {spec.name: entry}}


def first_filter(engine: QueryEngine) -> dict:
    return engine.bodies[0]["where"]["conditions"][0]


# Paging


@pytest.mark.parametrize("name", ALL)
@pytest.mark.parametrize("count", [5, 6])
def test_every_row_arrives_once_across_pages(api, capsys, name, count):
    """Covers a short last page (5 rows) and an exact multiple of the limit (6)."""
    spec = SPECS[name]
    rows = [spec.make_row(i) for i in range(count)]
    serve(api, spec, rows)
    stream = make_tap(start_date="2024-02-01T00:00:00Z").streams[name]
    stream.page_size = 2
    stream.sync()
    got = [m["record"][spec.key] for m in messages(capsys) if m["type"] == "RECORD"]
    assert sorted(got) == sorted(r[spec.key] for r in rows)
    assert len(got) == len(set(got))


@pytest.mark.parametrize("name", ["Company", "timeline", "deleted_records"])
def test_chain_pages_use_and_only_expressions_from_offset_zero(api, name):
    spec = SPECS[name]
    engine = serve(api, spec, [spec.make_row(i) for i in range(4)])
    stream = make_tap().streams[name]
    stream.page_size = 2
    records(stream, spec)
    assert all(b["limit"] == 2 and b["offset"] == 0 for b in engine.bodies)
    expressions = {b["where"]["expression"] for b in engine.bodies}
    assert expressions <= {"A", "A AND B", "A AND B AND C"}
    drains = [b for b in engine.bodies if len(b["where"]["conditions"]) >= 2 and b["where"]["conditions"][1]["operator"] == "LT"]
    assert drains and all(b["orderBy"] == {spec.key: "asc"} for b in drains)
    last = engine.bodies[-1]
    assert last["where"]["conditions"][0]["operator"] == "IS_NULL"
    assert last["orderBy"] == {spec.key: "asc"}


def test_keyset_handles_ties_on_the_replication_key(api):
    rows = [company_row(i, modified=T0) for i in range(5)]
    serve(api, SPECS["Company"], rows)
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    got = [r["Gsid"] for r in stream.get_records(None)]
    assert sorted(got) == sorted(r["Gsid"] for r in rows)
    assert len(got) == len(set(got))


def test_keyset_fails_when_the_api_misreads_the_cursor(api):
    serve(api, SPECS["Company"], [company_row(i) for i in range(4)], ignore_offset=True, naive_offset_hours=-8)
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    with pytest.raises(FatalAPIError, match="did not return the rows it listed"):
        list(stream.get_records(None))


def test_a_row_that_moves_before_its_second_is_drained_is_not_an_error(api):
    rows = [company_row(i) for i in range(4)]
    engine = serve(api, SPECS["Company"], rows)

    def move(request_number, engine):
        if request_number == 2:
            rows[1]["ModifiedDate"] = T0 + 99_000

    engine.before_request = move
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    got = {r["Gsid"] for r in stream.get_records(None)}
    assert got == {r["Gsid"] for r in rows}
    probes = [b for b in engine.bodies if b["select"] == ["Gsid", "ModifiedDate"]]
    assert len(probes) == 1
    assert probes[0]["where"]["conditions"][0]["operator"] == "EQ"
    assert probes[0]["orderBy"] == {"ModifiedDate": "asc"}


def test_page_size_one_still_reads_every_row(api):
    rows = [company_row(i) for i in range(3)] + [company_row(9, modified=T0)]
    serve(api, SPECS["Company"], rows)
    stream = make_tap().streams["Company"]
    stream.page_size = 1
    got = [r["Gsid"] for r in stream.get_records(None)]
    assert sorted(got) == sorted(r["Gsid"] for r in rows)


def test_a_full_page_before_the_lower_bound_fails(api):
    # A server that ignores the filter returns the same early rows forever.
    api.mocker.post(query_url("Company"), json={"result": True, "data": [company_row(0), company_row(1)]})
    stream = make_tap(start_date="2025-01-01T00:00:00Z").streams["Company"]
    stream.page_size = 2
    with pytest.raises(FatalAPIError, match="had no row at or after that value"):
        stream.sync()


def test_keyset_row_without_a_tiebreaker_fails(api):
    rows = [company_row(i) for i in range(3)]
    del rows[1]["Gsid"]
    serve(api, SPECS["Company"], rows)
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    with pytest.raises(FatalAPIError, match="lacks Gsid"):
        list(stream.get_records(None))


def test_ctas_outside_their_window_are_emitted_and_counted(api, capsys, caplog):
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="tap-gainsight")
    # The server reads the day values as UTC-8 and includes all of the end
    # day, so the window [02-04, 02-05] it serves runs to 02-06 08:00 UTC,
    # and it returns a CTA from 02-06 there too.
    rows = [cta_row(0, modified="2024-02-06T03:00:00.000Z"), cta_row(1)]
    serve(api, SPECS["cta"], rows, naive_offset_hours=-8, btw_whole_end_day=True)
    make_tap(start_date="2024-02-05T00:00:00Z").streams["cta"].sync()
    got = {m["record"]["Gsid"] for m in messages(capsys) if m["type"] == "RECORD"}
    assert got == {r["Gsid"] for r in rows}
    assert "outside the window" in caplog.text


def test_cta_windows_grow_after_sparse_windows(api):
    engine = serve(api, SPECS["cta"], [])
    make_tap(start_date="2024-01-01T00:00:00Z").streams["cta"].sync()
    spans = []
    for body in engine.bodies:
        values = body["where"]["conditions"][0]["value"]
        if len(values) == 2:
            first, last = (datetime.date.fromisoformat(v) for v in values)
            spans.append((last - first).days)
    assert spans[:3] == [1, 2, 4]
    assert max(spans) == 366
    # Windows overlap by a day: each starts on the day the last one ended.
    starts = [b["where"]["conditions"][0]["value"] for b in engine.bodies if len(b["where"]["conditions"][0]["value"]) == 2]
    assert all(starts[i + 1][0] == starts[i][1] for i in range(len(starts) - 1))


# Empty results


@pytest.mark.parametrize("name", ALL)
def test_empty_result(api, name):
    spec = SPECS[name]
    serve(api, spec, [])
    assert records(make_tap().streams[name], spec) == []


@pytest.mark.parametrize(
    "fixture",
    ["company_query_no_data_response.json", "custom_object_query_empty_response.json"],
)
def test_documented_empty_replies_end_the_stream(api, fixture):
    api.mocker.post(query_url("Company"), json=load(fixture))
    assert list(make_tap().streams["Company"].get_records(None)) == []


# Incremental state


@pytest.mark.parametrize("name", ALL)
def test_incremental_from_nothing(api, name, capsys):
    spec = SPECS[name]
    rows = [spec.make_row(i) for i in (2, 0, 1)]
    engine = serve(api, spec, rows)
    make_tap().streams[name].sync()
    condition = first_filter(engine)
    if name in CTA_STREAMS:
        assert condition["value"] == ["2000-01-01", "2000-01-02"]
    else:
        assert condition["operator"] == "IS_NOT_NULL"
    states = [m for m in messages(capsys) if m["type"] == "STATE"]
    assert bookmark(states[-1], spec) == client.to_iso_datetime(rows[0][spec.rk])


@pytest.mark.parametrize("name", ALL)
def test_incremental_from_start_date_looks_back_24_hours(api, name):
    spec = SPECS[name]
    engine = serve(api, spec, [])
    make_tap(start_date="2024-01-01T00:00:00Z").streams[name].sync()
    condition = first_filter(engine)
    assert condition["value"][0] == spec.start_value
    assert condition["operator"] == ("BTW" if name in CTA_STREAMS else "GTE")


@pytest.mark.parametrize("name", ALL)
def test_incremental_from_a_bookmark_advances_it(api, name, capsys):
    spec = SPECS[name]
    rows = [spec.make_row(i) for i in (5, 7)]
    engine = serve(api, spec, rows)
    old = "2024-02-05T08:24:35.253000+00:00"
    make_tap(state=state_for(spec, old), start_date="2020-01-01T00:00:00Z").streams[name].sync()
    # Whole seconds for the query API, whole days for the CTA APIs.
    if name in CTA_STREAMS:
        expected_start = datetime.datetime(2024, 2, 4, tzinfo=UTC)
    else:
        expected_start = datetime.datetime(2024, 2, 4, 8, 24, 35, tzinfo=UTC)
    assert client.parse_api_datetime(first_filter(engine)["value"][0]) == expected_start
    states = [m for m in messages(capsys) if m["type"] == "STATE"]
    final = bookmark(states[-1], spec)
    assert final == client.to_iso_datetime(rows[-1][spec.rk])
    assert client.parse_api_datetime(final) > client.parse_api_datetime(old)


# Null and missing fields, dates, labels, lookups


def test_mda_nulls_missing_fields_dates_labels_and_lookups(api):
    full = company_row(1)
    full["Stage"] = "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3"
    full["License_Type__gc"] = "1I00AABNSR5CLLWSP3F09ND6JHASXGUW8BW4"
    full["Csm__gr.Email"] = "jnash@heroku.com"
    nulls = {"Gsid": "1P02COMPANYNULL", "ModifiedDate": T0, "Stage": None, "Name": None}
    missing = {"Gsid": "1P02COMPANYMISS", "ModifiedDate": T0}
    multi = {"Gsid": "1P02COMPANYMULT", "ModifiedDate": T0, "Stage": [
        "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3", "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF4"
    ]}
    semi = {"Gsid": "1P02COMPANYSEMI", "ModifiedDate": T0, "Stage": "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3;unknown"}
    no_date = {"Gsid": "1P02COMPANYNODT", "ModifiedDate": None}
    serve(api, SPECS["Company"], [full, nulls, missing, multi, semi, no_date])
    got = {r["Gsid"]: r for r in make_tap().streams["Company"].get_records(None)}

    record = got[full["Gsid"]]
    assert record["ModifiedDate"] == "2024-02-05T08:24:36.253000+00:00"
    assert record["Renewal_Date"] == "2018-03-22T18:23:38.667000+00:00"
    assert record["Stage"] == "Kicked Off"
    assert record["License_Type__gc"] == "External"
    assert record["Csm__gr.Email"] == "jnash@heroku.com"

    assert got["1P02COMPANYNULL"]["Name"] is None
    assert got["1P02COMPANYNULL"]["Stage"] is None
    assert "Stage" not in got["1P02COMPANYMISS"]
    assert "Name" not in got["1P02COMPANYMISS"]
    assert got["1P02COMPANYMULT"]["Stage"] == '["Kicked Off", "Launched"]'
    # An id with no item stays as the id.
    assert got["1P02COMPANYSEMI"]["Stage"] == '["Kicked Off", "unknown"]'
    assert got["1P02COMPANYNODT"]["ModifiedDate"] is None


def test_documented_cta_rows_without_modified_date_arrive_in_the_null_pass(api, capsys):
    page = load("cta_list_response.json")["data"]
    page[1]["Quoted_ARR__gc"] = None
    engine = serve(api, SPECS["cta"], page)
    make_tap(start_date="2024-01-01T00:00:00Z").streams["cta"].sync()
    got = [m["record"] for m in messages(capsys) if m["type"] == "RECORD"]
    assert sorted(r["Name"] for r in got) == ["Today", "Tomorrow"]
    assert [r["TypeId__gr.Name"] for r in got] == ["Risk", "Risk"]
    assert {r["DueDate"] for r in got} == {"2020-04-14T11:30:00Z"}
    assert {r["associatedRecords"][0]["source"] for r in got} <= {"MDA", "SFDC"}
    assert engine.bodies[-1]["where"]["conditions"][0]["operator"] == "IS_NULL"


def test_cta_deleted_has_no_null_pass(api):
    engine = serve(api, SPECS["cta_deleted"], [])
    make_tap(start_date="2024-01-01T00:00:00Z").streams["cta_deleted"].sync()
    assert engine.bodies
    assert all(b["where"]["conditions"][0]["operator"] == "BTW" for b in engine.bodies)


def test_deleted_records_read_both_logs(api, capsys):
    api.serve(query_url("record_delete_log"), QueryEngine(load("delete_log_response.json")["data"]["records"], {"DeletedOn"}, shape="records"))
    api.mocker.post(query_url("record_delete_log_high_volume"), json=load("custom_object_query_empty_response.json"))
    make_tap().streams["deleted_records"].sync()
    got = [m["record"] for m in messages(capsys) if m["type"] == "RECORD"]
    assert len(got) == 4
    assert got[0] == {
        "RecordId": "1P02BVHREJTPN50SVSAWIPFJMV40UK9GJH6U",
        "DeletedOn": "2024-02-05T07:17:16Z",
        "ObjectName": "Company",
    }
    assert calls_to(api, query_url("record_delete_log_high_volume"))


def test_other_high_volume_log_errors_still_fail(api):
    api.serve(query_url("record_delete_log"), QueryEngine([], {"DeletedOn"}, shape="records"))
    body = load("company_query_no_data_response.json")
    body["errorCode"], body["errorDesc"] = "GSOBJ_1023", "Invalid API called, please refer to API documentation"
    api.mocker.post(query_url("record_delete_log_high_volume"), status_code=400, json=body)
    with pytest.raises(FatalAPIError, match="GSOBJ_1023"):
        make_tap().streams["deleted_records"].sync()


def test_object_not_found_on_another_path_still_fails(api):
    api.mocker.post(query_url("Company"), status_code=400, json=load("describe_not_found_response.json"))
    with pytest.raises(FatalAPIError, match="OBJECT_NOT_FOUND"):
        list(make_tap().streams["Company"].get_records(None))


# Failures


@pytest.mark.parametrize("name", ALL)
@pytest.mark.parametrize("status", [401, 403])
def test_auth_failure_fails_fast_with_a_clear_message(api, name, status):
    spec = SPECS[name]
    serve(api, spec, [])
    api.mocker.post(spec.url, status_code=status, json=load("unauthorized_response.json"))
    with pytest.raises(FatalAPIError, match=f"{status} Client Error.*rejected the access key.*GS_APIG_2401"):
        records(make_tap().streams[name], spec)
    assert len(calls_to(api, spec.url)) == 1


@pytest.mark.parametrize("name", ALL)
def test_unauthorized_body_on_200_fails_fast(api, name):
    spec = SPECS[name]
    serve(api, spec, [])
    api.mocker.post(spec.url, json=load("unauthorized_response.json"))
    with pytest.raises(FatalAPIError, match="rejected the access key"):
        records(make_tap().streams[name], spec)
    assert len(calls_to(api, spec.url)) == 1


def test_documented_invalid_authorization_headers_code_names_the_access_key(api):
    body = load("company_query_no_data_response.json")
    body["errorCode"], body["errorDesc"] = "GSOBJ_1024", "Invalid authorization headers, please re-check your API request"
    api.mocker.post(query_url("Company"), status_code=400, json=body)
    with pytest.raises(FatalAPIError, match="400 Client Error.*rejected the access key"):
        list(make_tap().streams["Company"].get_records(None))


@pytest.mark.parametrize("name", ALL)
def test_429_and_5xx_are_retried(api, name, sleeps):
    spec = SPECS[name]
    register_then_serve(
        api,
        spec,
        [{"status_code": 429, "text": "Too Many Requests"}, {"status_code": 502, "text": "Bad Gateway"}],
        [spec.make_row(1)],
    )
    got = records(make_tap(start_date="2024-02-01T00:00:00Z").streams[name], spec)
    assert len(got) == 1
    statuses = [r for r in calls_to(api, spec.url)][:3]
    assert len(statuses) == 3
    assert len(sleeps) >= 2


@pytest.mark.parametrize("name", ALL)
@pytest.mark.parametrize("status", [429, 500, 503])
def test_retries_give_up_with_the_status_and_body(api, name, status):
    spec = SPECS[name]
    serve(api, spec, [])
    api.mocker.post(spec.url, status_code=status, text="upstream says no")
    with pytest.raises(RetriableAPIError, match=f"{status} .*Response: HTTP {status}, a 16-byte body") as info:
        records(make_tap().streams[name], spec)
    assert "upstream says no" not in str(info.value)
    assert len(calls_to(api, spec.url)) == client.MAX_TRIES


@pytest.mark.parametrize("name", ALL)
def test_malformed_json_fails(api, name):
    spec = SPECS[name]
    serve(api, spec, [])
    api.mocker.post(spec.url, text="<html>maintenance</html>")
    with pytest.raises(FatalAPIError, match="is not JSON.*a 24-byte body"):
        records(make_tap().streams[name], spec)


@pytest.mark.parametrize("name", ALL)
def test_other_4xx_fails_with_status_and_body(api, name):
    spec = SPECS[name]
    serve(api, spec, [])
    api.mocker.post(spec.url, status_code=400, json=load("cta_list_invalid_select_response.json"))
    with pytest.raises(FatalAPIError, match="400 Client Error.*COCKPIT_5101"):
        records(make_tap().streams[name], spec)


def test_documented_cta_error_on_200_fails(api):
    api.mocker.post(CTA_URL, json=load("cta_list_invalid_select_response.json"))
    with pytest.raises(FatalAPIError, match="result=false.*errorCode COCKPIT_5101 \\(the CTA request is invalid\\)"):
        list(make_tap().streams["cta"].get_records(None))


def test_unexpected_data_shape_fails(api):
    api.mocker.post(query_url("Company"), json={"result": True, "data": "surprise"})
    with pytest.raises(FatalAPIError, match="Unexpected `data` shape"):
        list(make_tap().streams["Company"].get_records(None))


def test_a_redirect_names_the_host_and_is_not_retried(api):
    api.mocker.post(CTA_URL, status_code=307, headers={"Location": "https://other.example.com/x"})
    with pytest.raises(FatalAPIError, match="307 redirect .*other.example.com"):
        list(make_tap().streams["cta"].get_records(None))
    assert len(calls_to(api, CTA_URL)) == 1


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
    register_then_serve(api, spec, [{"status_code": 429, "text": "slow"}], [spec.make_row(1)])
    tap = make_tap(start_date="2024-02-01T00:00:00Z")
    discovery_calls = len(api.mocker.request_history)
    limiter = CountingLimiter()
    tap._rate_limiter = limiter
    records(tap.streams[name], spec)
    assert limiter.calls == len(api.mocker.request_history) - discovery_calls


def test_the_limiter_throttles_a_long_sync(api):
    serve(api, SPECS["Company"], [company_row(i) for i in range(3)])
    tap = make_tap()
    fake_now = [0.0]
    tap._rate_limiter = client.RateLimiter(
        calls=2, period=60, clock=lambda: fake_now[0], sleep=lambda s: fake_now.__setitem__(0, fake_now[0] + s)
    )
    stream = tap.streams["Company"]
    stream.page_size = 2
    assert len(list(stream.get_records(None))) == 3
    # Keyset pages 1 and 2, then the null pass: 3 requests at 2 a minute.
    assert fake_now[0] == 60.0


# Null replication keys


def test_null_replication_key_rows_log_a_count(api, caplog):
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    serve(api, SPECS["Company"], [company_row(1), company_row(2, modified=None), company_row(3, modified=None)])
    make_tap().streams["Company"].sync()
    assert "synced 2 record(s) with a null ModifiedDate" in caplog.text


# Catalog selection


def catalog_with(tap, stream_name, deselect=()):
    catalog = tap.catalog_dict
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            breadcrumb = item["breadcrumb"]
            if not breadcrumb:
                item["metadata"]["selected"] = entry["tap_stream_id"] == stream_name
            elif breadcrumb[-1] in deselect and entry["tap_stream_id"] == stream_name:
                item["metadata"]["selected"] = False
    return catalog


def test_select_list_follows_the_catalog(api, capsys):
    row = {**company_row(1), "Stage": "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3", "Health_Notes__gc": "ok"}
    engine = serve(api, SPECS["Company"], [row])
    catalog = catalog_with(make_tap(), "Company", deselect={"Health_Notes__gc", "Csm__gr.Email"})
    tap = make_tap(catalog=catalog)
    select = tap.streams["Company"].select_paths()
    assert "Health_Notes__gc" not in select and "Csm__gr.Email" not in select
    assert "Stage" in select and "Csm__gr.Name" in select
    tap.sync_all()
    assert engine.bodies[0]["select"] == select
    record = next(m["record"] for m in messages(capsys) if m["type"] == "RECORD")
    assert "Health_Notes__gc" not in record
    assert record["Stage"] == "Kicked Off"


def test_keys_are_selected_even_when_deselected(api):
    catalog = catalog_with(make_tap(), "Company", deselect={"Gsid", "ModifiedDate"})
    select = make_tap(catalog=catalog).streams["Company"].select_paths()
    assert "Gsid" in select and "ModifiedDate" in select


def test_a_deselected_picklist_is_not_queried(api):
    catalog = catalog_with(make_tap(), "Company", deselect={"Stage"})
    assert "Stage" not in make_tap(catalog=catalog).streams["Company"].select_paths()


def test_cta_select_follows_the_catalog(api):
    engine = serve(api, SPECS["cta"], [])
    catalog = catalog_with(make_tap(), "cta", deselect={"Comments", "Quoted_ARR__gc", "ModifiedDate"})
    make_tap(catalog=catalog, start_date="2026-01-01T00:00:00Z").sync_all()
    select = engine.bodies[0]["select"]
    assert "Comments" not in select and "Quoted_ARR__gc" not in select
    assert "ModifiedDate" in select and "customDate__gc" in select


def test_a_deselected_picklist_whose_names_fail_is_fine(api):
    catalog = catalog_with(make_tap(), "Company", deselect={"License_Type__gc"})
    api.dropdown = {"result": False, "errorDesc": "gone"}
    assert "Company" in make_tap(catalog=catalog).streams
