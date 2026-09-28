"""Regression tests for the review findings on feat/gainsight-tap.

Each test failed on commit 8afa720, before its fix. The finding number is
in each test name.
"""

from __future__ import annotations

import copy
import datetime
import json
import logging

import pytest
import requests_mock as requests_mock_lib
from singer_sdk.exceptions import FatalAPIError

from tests.conftest import (
    BASE_URL,
    QueryEngine,
    describe_entry,
    doc_field,
    load,
    make_tap,
    query_page,
    query_url,
    standard_fields,
)

T0 = 1707121475253  # 2024-02-05T08:24:35.253Z
CTA_URL = f"{BASE_URL}/v2/cockpit/cta/list"
CTA_DELETED_URL = f"{BASE_URL}/v2/cockpit/cta/deleted/list"
UTC = datetime.timezone.utc


def now() -> datetime.datetime:
    return datetime.datetime.now(UTC)


def iso_z(moment: datetime.datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


_DEFAULT = object()


def company(i: int, modified=_DEFAULT) -> dict:
    """A query row. Pass `modified=None` for a null ModifiedDate."""
    value = T0 + i * 1000 if modified is _DEFAULT else modified
    return {"Gsid": f"1P02C{i:04d}", "Name": f"Company {i}", "ModifiedDate": value}


def serve_company(api, rows, **kwargs) -> QueryEngine:
    return api.serve(query_url("company"), QueryEngine(rows, {"ModifiedDate", "CreatedDate"}, **kwargs))


def serve_empty_delete_logs(api):
    for log in ("record_delete_log", "record_delete_log_high_volume"):
        api.serve(query_url(log), QueryEngine([], {"DeletedOn"}, shape="records"))


def capture(caplog):
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    caplog.set_level(logging.INFO)
    return caplog


def singer_messages(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]


# Finding 1: "No data found" can arrive as HTTP 400.


def gsobj_1011_body() -> dict:
    """The documented failure body, with GSOBJ_1011 from the Error Codes article."""
    body = load("company_query_no_data_response.json")
    body["errorCode"] = "GSOBJ_1011"
    body["errorDesc"] = "No entity matches the given criteria, please re-check your request"
    return body


@pytest.mark.parametrize("body", [load("company_query_no_data_response.json"), gsobj_1011_body()])
def test_f1_empty_reply_at_http_400_is_an_empty_page(api, body):
    api.mocker.post(query_url("company"), status_code=400, json=body)
    assert list(make_tap().streams["Company"].get_records(None)) == []


def test_f1_gsobj_1011_wording_at_http_200_is_an_empty_page(api):
    api.mocker.post(query_url("company"), json=gsobj_1011_body())
    assert list(make_tap().streams["Company"].get_records(None)) == []


# Finding 2: offset paging on ModifiedDate skips rows edited mid-sync.


@pytest.mark.parametrize("change", ["edit", "delete"])
def test_f2_a_row_changed_between_pages_does_not_hide_another_row(api, change):
    rows = [company(i) for i in range(1, 6)]
    engine = serve_company(api, rows)

    def mutate(request_number, engine):
        if request_number == 2:
            if change == "edit":
                rows[0]["ModifiedDate"] = T0 + 100_000
            else:
                engine.rows.remove(rows[0])

    engine.before_request = mutate
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    got = {r["Gsid"] for r in stream.get_records(None)}
    expected = {r["Gsid"] for r in rows}
    if change == "delete":
        expected.discard("1P02C0001")
    assert expected <= got


def test_f2_keyset_uses_the_documented_expression_syntax(api):
    engine = serve_company(api, [company(i) for i in range(1, 4)])
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    list(stream.get_records(None))
    second = engine.bodies[1]
    assert second["offset"] == 0
    assert second["where"]["expression"] == "A OR (B AND C)"
    ops = [(c["name"], c["operator"]) for c in second["where"]["conditions"]]
    assert ops == [("ModifiedDate", "GT"), ("ModifiedDate", "EQ"), ("Gsid", "GTE")]


# Finding 3: deleted CTAs are never tombstoned.


def test_f3_cta_deleted_stream_follows_the_documented_contract(api, capsys):
    fixture = load("cta_deleted_list_response.json")
    engine = api.serve(CTA_DELETED_URL, QueryEngine(fixture["data"], {"ModifiedDate"}))
    tap = make_tap(start_date="2024-02-04T00:00:00Z")
    stream = tap.streams["cta_deleted"]
    assert stream.primary_keys == ["Gsid"]
    assert stream.replication_key == "ModifiedDate"
    stream.sync()

    documented = load("cta_deleted_list_request.json")
    documented_condition_keys = {"fieldName", "value", "alias", "operator"}
    for request in [r for r in api.mocker.request_history if r.path == "/v2/cockpit/cta/deleted/list"]:
        assert request.method == "POST"
        assert request.headers["accesskey"] == "test-access-key"
        body = request.json()
        assert set(body) == set(documented)
        assert body["select"] == documented["select"]
        for condition in body["where"]["conditions"]:
            assert set(condition) == documented_condition_keys
            assert condition["fieldName"] == "ModifiedDate"
    records = [m["record"] for m in singer_messages(capsys) if m["type"] == "RECORD"]
    assert sorted(r["Gsid"] for r in records) == sorted(r["Gsid"] for r in fixture["data"])
    assert engine.bodies


# Finding 4: a null replication-key value crashes the state update.


@pytest.mark.parametrize("bookmarked", [False, True])
def test_f4_null_replication_key_rows_are_emitted_without_a_bookmark(api, capsys, caplog, bookmarked):
    capture(caplog)
    rows = [company(1), company(2, modified=None), company(3)]
    serve_company(api, rows)
    state = {"bookmarks": {"Company": {"replication_key": "ModifiedDate", "replication_key_value": "2024-01-01T00:00:00+00:00"}}} if bookmarked else {}
    tap = make_tap(state=state)
    tap.streams["Company"].sync()
    messages = singer_messages(capsys)
    got = {m["record"]["Gsid"] for m in messages if m["type"] == "RECORD"}
    assert got == {"1P02C0001", "1P02C0002", "1P02C0003"}
    final = [m for m in messages if m["type"] == "STATE"][-1]["value"]["bookmarks"]["Company"]
    assert final["replication_key_value"].startswith("2024-02-05T08:24:38.253")
    assert "1 record(s) with a null ModifiedDate" in caplog.text


# Finding 5: 24-hour lookback on the MDA and delete log date filters.


def test_f5_mda_filter_looks_back_24_hours(api):
    engine = serve_company(api, [])
    make_tap(start_date="2024-01-02T00:00:00Z").streams["Company"].sync()
    condition = engine.bodies[0]["where"]["conditions"][0]
    assert condition["operator"] == "GTE"
    assert condition["value"] == ["2024-01-01T00:00:00.000+0000"]


def test_f5_delete_log_filter_looks_back_24_hours(api):
    serve_empty_delete_logs(api)
    make_tap(start_date="2024-01-02T00:00:00Z").streams["deleted_records"].sync()
    body = [r for r in api.mocker.request_history if r.path.endswith("/record_delete_log")][0].json()
    assert body["where"]["conditions"][0]["value"] == ["2024-01-01 00:00:00"]


# Finding 6: CTA paging has no documented order, so slice by ModifiedDate.


def cta_rows(moments):
    base = load("cta_list_response.json")["data"][0]
    rows = []
    for i, moment in enumerate(moments):
        row = copy.deepcopy(base)
        row["Gsid"] = f"1S01CTA{i:04d}"
        row["ModifiedDate"] = iso_z(moment)
        rows.append(row)
    return rows


def slice_bounds(body):
    conditions = {c["operator"]: c["value"][0] for c in body["where"]["conditions"] if c["value"]}
    return conditions.get("GTE"), conditions.get("LT")


def test_f6_cta_windows_are_halved_until_each_fits_one_page(api):
    day = (now() - datetime.timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = cta_rows([day + datetime.timedelta(hours=h) for h in range(0, 24, 3)])
    engine = api.serve(CTA_URL, QueryEngine(rows, {"ModifiedDate"}, unordered=True))
    tap = make_tap(start_date=iso_z(day))
    stream = tap.streams["cta"]
    stream.page_size = 3
    got = [r["Gsid"] for r in stream.get_records(None)]
    assert sorted(got) == sorted(r["Gsid"] for r in rows)
    assert len(got) == len(set(got))
    widths = []
    for body in engine.bodies:
        start, end = slice_bounds(body)
        if start and end:
            widths.append(datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S.%f%z") - datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%f%z"))
    assert widths[0] == datetime.timedelta(days=1)
    assert min(widths) < datetime.timedelta(days=1)
    assert min(widths) >= datetime.timedelta(hours=1)


def test_f6_an_overflowing_minimum_slice_is_paged_with_a_warning(api, caplog):
    capture(caplog)
    moment = (now() - datetime.timedelta(days=1)).replace(minute=0, second=0, microsecond=0)
    rows = cta_rows([moment + datetime.timedelta(minutes=m) for m in range(5)])
    api.serve(CTA_URL, QueryEngine(rows, {"ModifiedDate"}))
    stream = make_tap(start_date=iso_z(moment)).streams["cta"]
    stream.page_size = 2
    got = [r["Gsid"] for r in stream.get_records(None)]
    assert sorted(got) == sorted(r["Gsid"] for r in rows)
    assert "more CTAs than one page" in caplog.text


# Finding 7: a delete gap over 15 days must log an error.


def test_f7_deleted_records_gap_over_15_days_logs_an_error(api, caplog, capsys):
    capture(caplog)
    serve_empty_delete_logs(api)
    old = (now() - datetime.timedelta(days=20)).isoformat()
    state = {"bookmarks": {"deleted_records": {"partitions": [
        {"context": {"delete_log": "record_delete_log"}, "replication_key": "DeletedOn", "replication_key_value": old},
    ]}}}
    make_tap(state=state).streams["deleted_records"].sync()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and "20 days" in errors[0].getMessage()


def test_f7_cta_deleted_gap_over_15_days_logs_an_error(api, caplog):
    capture(caplog)
    api.serve(CTA_DELETED_URL, QueryEngine([], {"ModifiedDate"}))
    old = (now() - datetime.timedelta(days=30)).isoformat()
    state = {"bookmarks": {"cta_deleted": {"replication_key": "ModifiedDate", "replication_key_value": old}}}
    make_tap(state=state).streams["cta_deleted"].sync()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and "30 days" in errors[0].getMessage()


# Finding 8: discovery during sync must not drop selected streams or columns.


def select_all(catalog, stream_ids):
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            item["metadata"]["selected"] = entry["tap_stream_id"] in stream_ids
    return catalog


def test_f8_a_selected_stream_that_discovery_drops_raises(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    catalog = select_all(make_tap().catalog_dict, {"Company", "obj1__gc"})
    del api.describes["obj1__gc"]
    with pytest.raises(Exception, match="obj1__gc"):
        make_tap(catalog=catalog).streams


def test_f8_a_selected_column_that_discovery_drops_raises(api):
    catalog = select_all(make_tap().catalog_dict, {"Company"})
    api.dropdown = {"result": False, "errorDesc": "gone"}
    with pytest.raises(Exception, match="License_Type__gc_label"):
        make_tap(catalog=catalog).streams


# Finding 9: CompanyId__gr.Name is in the CTA schema, so select it.


def test_f9_cta_selects_the_company_name(api):
    engine = api.serve(CTA_URL, QueryEngine([], {"ModifiedDate"}))
    make_tap(start_date=iso_z(now() - datetime.timedelta(days=1))).streams["cta"].sync()
    assert "CompanyId__gr.Name" in engine.bodies[0]["select"]


# Finding 10: the access key must not follow a redirect.


def test_f10_a_query_redirect_is_not_followed(api):
    api.mocker.post(query_url("company"), status_code=302, headers={"Location": "https://evil.example.com/steal"})
    api.mocker.register_uri(requests_mock_lib.ANY, "https://evil.example.com/steal", json=query_page([]))
    with pytest.raises(FatalAPIError, match="evil.example.com"):
        list(make_tap().streams["Company"].get_records(None))
    assert not [r for r in api.mocker.request_history if r.hostname == "evil.example.com"]


def test_f10_a_metadata_redirect_is_not_followed(api):
    api.mocker.get(f"{BASE_URL}/v1/meta/services/objects/list", status_code=301, headers={"Location": "https://evil.example.com/list"})
    api.mocker.register_uri(requests_mock_lib.ANY, "https://evil.example.com/list", json=load("object_list_response.json"))
    with pytest.raises(Exception, match="evil.example.com"):
        make_tap()
    assert not [r for r in api.mocker.request_history if r.hostname == "evil.example.com"]


# Hardening: object name case, hidden and deleted fields, high-volume log.


def test_h_describe_and_query_use_names_exactly_as_listed(api):
    engine = serve_company(api, [])
    tap = make_tap()
    assert "company" in api.describe_calls()[0]
    assert "Company" not in api.describe_calls()[0]
    tap.streams["Company"].sync()
    assert engine.bodies


def test_h_hidden_and_deleted_fields_are_left_out(api):
    fields = api.describes["company"]["fields"]
    fields.append(doc_field("Name__gc", "company", fieldName="Old_Field__gc", meta={"hidden": True}))
    fields.append(doc_field("Name__gc", "company", fieldName="Gone_Field__gc", meta={"deleted": True}))
    engine = serve_company(api, [])
    stream = make_tap().streams["Company"]
    assert "Old_Field__gc" not in stream.schema["properties"]
    assert "Gone_Field__gc" not in stream.schema["properties"]
    stream.sync()
    assert not {"Old_Field__gc", "Gone_Field__gc"} & set(engine.bodies[0]["select"])


def test_h_high_volume_log_that_does_not_exist_counts_as_empty(api, capsys):
    api.serve(query_url("record_delete_log"), QueryEngine(load("delete_log_response.json")["data"]["records"], {"DeletedOn"}, shape="records"))
    api.mocker.post(query_url("record_delete_log_high_volume"), status_code=400, json=load("describe_not_found_response.json"))
    make_tap().streams["deleted_records"].sync()
    records = [m for m in singer_messages(capsys) if m["type"] == "RECORD"]
    assert len(records) == 4
