"""Regression tests for the delta review of 8afa720..5d5d3d5.

Tests in the N and T sections failed on commit 5d5d3d5, before their fix.
Tests in the Guards section passed there too. They pin behavior the fixes
must keep.
"""

from __future__ import annotations

import copy
import datetime
import json
import logging
import re

import pytest
from singer_sdk.exceptions import FatalAPIError

from tests.conftest import (
    BASE_URL,
    QueryEngine,
    describe_entry,
    doc_field,
    load,
    make_tap,
    query_url,
    standard_fields,
)

UTC = datetime.timezone.utc
T0 = 1707121475253  # 2024-02-05T08:24:35.253Z
CTA_URL = f"{BASE_URL}/v2/cockpit/cta/list"
CTA_DELETED_URL = f"{BASE_URL}/v2/cockpit/cta/deleted/list"
DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
QUERY_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
AND_ONLY = re.compile(r"^[A-Z](?: AND [A-Z])*$")
SECOND = 1000
DAY = 86_400_000


def now():
    return datetime.datetime.now(UTC)


def iso_z(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def cta_rows(moments, prefix="1S01CTA"):
    base = load("cta_list_response.json")["data"][0]
    rows = []
    for i, moment in enumerate(moments):
        row = copy.deepcopy(base)
        row["Gsid"] = f"{prefix}{i:04d}"
        row["ModifiedDate"] = iso_z(moment)
        rows.append(row)
    return rows


def company(i, modified):
    return {"Gsid": f"1P02C{i:04d}", "Name": f"Company {i}", "ModifiedDate": modified}


def records_of(capsys, stream=None):
    out = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    return [m["record"] for m in out if m["type"] == "RECORD" and (stream is None or m["stream"] == stream)]


def capture(caplog):
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG)
    return caplog


def window_bodies(engine):
    return [b for b in engine.bodies if b["where"]["conditions"][0]["operator"] != "IS_NULL"]


# N1: the CTA window check must not fail on harmless extra rows, and CTA
# filters use the documented BTW with date-only values.


def cta_state(bookmark):
    return {"bookmarks": {"cta": {"replication_key": "ModifiedDate", "replication_key_value": bookmark.isoformat()}}}


@pytest.mark.parametrize("grain", [SECOND, DAY])
def test_n1_cta_extra_rows_from_a_coarse_server_are_emitted_not_fatal(api, capsys, grain):
    bookmark = (now() - datetime.timedelta(days=1, hours=3)).replace(microsecond=253000)
    start = bookmark - datetime.timedelta(hours=24)
    early = start.replace(microsecond=100000) if grain == SECOND else start.replace(hour=0, minute=30)
    rows = cta_rows([early, bookmark])
    api.serve(CTA_URL, QueryEngine(rows, {"ModifiedDate"}, grain_ms=grain))
    make_tap(state=cta_state(bookmark)).streams["cta"].sync()
    got = {r["Gsid"] for r in records_of(capsys)}
    assert got == {r["Gsid"] for r in rows}


@pytest.mark.parametrize("stream_name, url", [("cta", CTA_URL), ("cta_deleted", CTA_DELETED_URL)])
def test_n1_cta_windows_use_btw_with_date_only_values(api, stream_name, url):
    engine = api.serve(url, QueryEngine([], {"ModifiedDate"}))
    make_tap(start_date="2026-09-01T12:00:00Z").streams[stream_name].sync()
    bodies = window_bodies(engine)
    assert bodies
    for body in bodies:
        (condition,) = body["where"]["conditions"]
        assert body["where"]["expression"] == "A"
        assert condition["operator"] == "BTW"
        assert condition["fieldName"] == "ModifiedDate"
        assert all(DATE_ONLY.match(v) for v in condition["value"])
    assert bodies[0]["where"]["conditions"][0]["value"][0] == "2026-08-31"


def test_n1_a_full_one_day_window_is_paged_not_split_into_hours(api, capsys, caplog):
    capture(caplog)
    day = (now() - datetime.timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = cta_rows([day + datetime.timedelta(hours=h) for h in range(1, 6)])
    engine = api.serve(CTA_URL, QueryEngine(rows, {"ModifiedDate"}))
    stream = make_tap(start_date=iso_z(day)).streams["cta"]
    stream.page_size = 2
    stream.sync()
    assert sorted({r["Gsid"] for r in records_of(capsys)}) == sorted(r["Gsid"] for r in rows)
    for body in window_bodies(engine):
        first, last = (datetime.date.fromisoformat(v) for v in body["where"]["conditions"][0]["value"])
        assert last - first >= datetime.timedelta(days=1)
    assert "more CTAs than one page" in caplog.text


@pytest.mark.parametrize("whole_end_day", [False, True])
@pytest.mark.parametrize("stream_name, url", [("cta", CTA_URL), ("cta_deleted", CTA_DELETED_URL)])
def test_guard_no_cta_is_skipped_under_either_btw_reading(api, capsys, caplog, whole_end_day, stream_name, url):
    base = (now() - datetime.timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    moments = [base + datetime.timedelta(days=d, hours=h) for d in range(5) for h in (0, 11, 23)]
    moments += [base + datetime.timedelta(days=2, hours=23, minutes=59, seconds=59)]
    rows = cta_rows(moments)
    api.serve(url, QueryEngine(rows, {"ModifiedDate"}, btw_whole_end_day=whole_end_day, unordered=True))
    capture(caplog)
    stream = make_tap(start_date=iso_z(base)).streams[stream_name]
    # Two adjacent days hold at most 7 CTAs, so every shortest window fits
    # one page, and longer windows must be halved to fit.
    stream.page_size = 8
    stream.sync()
    assert {r["Gsid"] for r in records_of(capsys)} == {r["Gsid"] for r in rows}
    assert "more CTAs than one page" not in caplog.text


# N2 and N3: the MDA filters use the documented form, and paging needs no
# milliseconds and no parentheses.


def test_n2_mda_filter_uses_the_documented_datetime_form(api):
    engine = api.serve(query_url("Company"), QueryEngine([], {"ModifiedDate"}))
    make_tap(start_date="2024-01-02T00:00:00Z").streams["Company"].sync()
    condition = engine.bodies[0]["where"]["conditions"][0]
    assert (condition["operator"], condition["value"]) == ("GTE", ["2024-01-01 00:00:00"])


def same_second_rows():
    # Four rows share one second, at different milliseconds.
    offsets = [0, 1_000, 2_000, 2_100, 2_200, 2_900, 2_950, 5_000]
    return [company(i, T0 + off) for i, off in enumerate(offsets)]


@pytest.mark.parametrize("grain", [None, SECOND])
def test_n2_paging_is_a_whole_second_and_only_chain(api, capsys, grain):
    rows = same_second_rows()
    engine = api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}, grain_ms=grain))
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    stream.sync()
    got = [r["Gsid"] for r in records_of(capsys)]
    assert sorted(got) == sorted(r["Gsid"] for r in rows)
    assert len(got) == len(set(got))
    for body in engine.bodies:
        where = body["where"]
        assert AND_ONLY.match(where["expression"]), where["expression"]
        assert len(where["conditions"]) <= 3
        for condition in where["conditions"]:
            if condition["name"] == "ModifiedDate" and condition["value"]:
                assert QUERY_DATETIME.match(condition["value"][0])


@pytest.mark.parametrize("change", ["edit", "delete"])
def test_n2_a_row_changed_between_pages_hides_no_other_row(api, capsys, change):
    rows = [company(i, T0 + i * SECOND) for i in range(1, 7)]
    engine = api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}))

    def mutate(request_number, engine):
        if request_number == 2:
            if change == "edit":
                rows[0]["ModifiedDate"] = T0 + 100 * SECOND
            else:
                engine.rows.remove(rows[0])

    engine.before_request = mutate
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    stream.sync()
    got = {r["Gsid"] for r in records_of(capsys)}
    assert {r["Gsid"] for r in rows[1:]} <= got


def test_n3_no_request_uses_parentheses(api):
    rows = [company(i, T0 + i * SECOND) for i in range(5)]
    api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}))
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    stream.sync()


# N4: routine schema changes warn. Only discovery failures in this run raise.


def catalog_selecting(tap, stream_ids):
    catalog = tap.catalog_dict
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            item["metadata"]["selected"] = entry["tap_stream_id"] in stream_ids
    return catalog


def test_n4_a_field_deleted_by_an_admin_warns_and_syncs(api, caplog):
    capture(caplog)
    catalog = catalog_selecting(make_tap(), {"Company"})
    fields = api.describes["company"]["fields"]
    api.describes["company"]["fields"] = [f for f in fields if f["fieldName"] != "Health_Notes__gc"]
    streams = make_tap(catalog=catalog).streams
    assert "Company" in streams
    assert "Company.Health_Notes__gc" in caplog.text


def test_n4_a_field_flagged_deleted_warns_and_syncs(api, caplog):
    capture(caplog)
    catalog = catalog_selecting(make_tap(), {"Company"})
    for field in api.describes["company"]["fields"]:
        if field["fieldName"] == "Health_Notes__gc":
            field["meta"]["deleted"] = True
    assert "Health_Notes__gc" not in make_tap(catalog=catalog).streams["Company"].schema["properties"]
    assert "Company.Health_Notes__gc" in caplog.text


def test_n4_a_hidden_field_stays_in_the_schema_and_syncs(api, capsys):
    catalog = catalog_selecting(make_tap(), {"Company"})
    for field in api.describes["company"]["fields"]:
        if field["fieldName"] == "Health_Notes__gc":
            field["meta"]["hidden"] = True
    row = {**company(1, T0), "Health_Notes__gc": "still here"}
    engine = api.serve(query_url("Company"), QueryEngine([row], {"ModifiedDate"}))
    make_tap(catalog=catalog).sync_all()
    assert "Health_Notes__gc" in engine.bodies[0]["select"]
    assert records_of(capsys, "Company")[0]["Health_Notes__gc"] == "still here"


def test_n4_an_object_removed_from_gainsight_warns(api, caplog):
    capture(caplog)
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    catalog = catalog_selecting(make_tap(), {"Company", "obj1__gc"})
    del api.describes["obj1__gc"]
    api.list_payload["data"] = [o for o in api.list_payload["data"] if o["objectName"] != "obj1__gc"]
    assert "Company" in make_tap(catalog=catalog).streams
    assert "obj1__gc" in caplog.text


# N5: query paths use the documented casing for documented objects.


@pytest.mark.parametrize(
    "stream_name, path_name",
    [("Company", "Company"), ("Company_Person", "Company_Person"), ("GsUser", "GsUser")],
)
def test_n5_documented_objects_use_documented_query_casing(api, stream_name, path_name):
    engine = api.serve(query_url(path_name), QueryEngine([], {"ModifiedDate"}))
    make_tap().streams[stream_name].sync()
    assert engine.bodies
    assert all(name == name.lower() for call in api.describe_calls() for name in call)


def test_guard_timeline_queries_activity_timeline(api):
    engine = api.serve(query_url("activity_timeline"), QueryEngine([], {"ModifiedDate"}, shape="records"))
    make_tap().streams["timeline"].sync()
    assert engine.bodies


def test_guard_other_objects_use_the_listed_name(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    engine = api.serve(query_url("obj1__gc"), QueryEngine([], {"ModifiedDate"}))
    make_tap().streams["obj1__gc"].sync()
    assert engine.bodies


# N6: full-table streams and the null pass page by Gsid, not offset.


def test_n6_full_table_pages_by_gsid(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", [doc_field("Gsid", "obj1__gc")])
    rows = [{"Gsid": f"G{i}"} for i in range(5)]
    engine = api.serve(query_url("obj1__gc"), QueryEngine(rows, set()))
    stream = make_tap().streams["obj1__gc"]
    stream.page_size = 2
    stream.sync()
    assert all(b["offset"] == 0 for b in engine.bodies)
    assert engine.bodies[1]["where"] == {
        "conditions": [{"name": "Gsid", "alias": "A", "value": ["G1"], "operator": "GT"}],
        "expression": "A",
    }


def test_n6_null_pass_pages_by_gsid(api, capsys):
    rows = [company(i, None) for i in range(3)]
    engine = api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}))
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    stream.sync()
    nulls = [b for b in engine.bodies if b["where"]["conditions"][0]["operator"] == "IS_NULL"]
    assert all(b["offset"] == 0 for b in nulls)
    assert [(c["name"], c["operator"]) for c in nulls[1]["where"]["conditions"]] == [
        ("ModifiedDate", "IS_NULL"), ("Gsid", "GT"),
    ]
    assert len(records_of(capsys)) == 3


# N7: the delete log check reads the row by RecordId, ordered by DeletedOn.


def test_n7_delete_log_check_orders_by_deleted_on(api):
    rows = [
        {"RecordId": f"1P02D{i:04d}", "DeletedOn": f"2024-02-05T07:17:1{i}Z", "ObjectName": "Company"}
        for i in range(4)
    ]
    engine = api.serve(query_url("record_delete_log"), QueryEngine(rows, {"DeletedOn"}, shape="records", naive_offset_hours=-8))
    api.serve(query_url("record_delete_log_high_volume"), QueryEngine([], {"DeletedOn"}, shape="records"))
    stream = make_tap().streams["deleted_records"]
    stream.page_size = 2
    with pytest.raises(FatalAPIError, match="did not return the rows it listed"):
        stream.sync()
    probe = [b for b in engine.bodies if b["where"]["conditions"][0]["name"] == "RecordId"][0]
    assert probe["orderBy"] == {"DeletedOn": "asc"}
    assert probe["where"]["conditions"][0]["operator"] == "EQ"


# Guards: these passed before the fixes and must keep passing.


def test_guard_a_transient_dropdown_failure_for_a_selected_label_raises(api):
    catalog = catalog_selecting(make_tap(), {"Company"})
    api.dropdown = {"result": False, "errorDesc": "gone"}
    with pytest.raises(Exception, match="Company.License_Type__gc_label"):
        make_tap(catalog=catalog).streams


def test_guard_a_failed_lookup_target_describe_raises_for_selected_columns(api):
    catalog = catalog_selecting(make_tap(), {"Company"})
    api.failing["gsuser"] = (400, load("describe_not_found_response.json"))
    with pytest.raises(Exception, match="Company.Csm__gr.Email"):
        make_tap(catalog=catalog).streams


def test_guard_a_selected_object_whose_describe_failed_raises(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    catalog = catalog_selecting(make_tap(), {"obj1__gc"})
    api.failing["obj1__gc"] = (500, {"result": False})
    with pytest.raises(Exception, match="obj1__gc"):
        make_tap(catalog=catalog).streams


def test_guard_cta_custom_columns_lost_to_a_failed_cs_cta_describe_raise(api):
    catalog = catalog_selecting(make_tap(), {"cta"})
    api.failing["cs_cta"] = (400, load("describe_not_found_response.json"))
    with pytest.raises(Exception, match="cta.Quoted_ARR__gc"):
        make_tap(catalog=catalog).streams


# T3: coarse-grain servers. The CTA cases are in the N1 tests, and the MDA
# second-grain case is in the N2 tests.


@pytest.mark.parametrize("name, url, rows, fields", [
    ("Company", query_url("Company"), [company(i, T0 + i * SECOND) for i in range(4)], {"ModifiedDate"}),
    (
        "deleted_records",
        query_url("record_delete_log"),
        [{"RecordId": f"1P02D{i:04d}", "DeletedOn": f"2024-02-05T07:17:1{i}Z", "ObjectName": "Company"} for i in range(4)],
        {"DeletedOn"},
    ),
])
def test_t3_a_date_grain_query_server_fails_loudly_never_silently(api, capsys, name, url, rows, fields):
    shape = "records" if name == "deleted_records" else "list"
    api.serve(url, QueryEngine(rows, fields, shape=shape, grain_ms=DAY))
    api.serve(query_url("record_delete_log_high_volume"), QueryEngine([], {"DeletedOn"}, shape="records"))
    stream = make_tap().streams[name]
    stream.page_size = 2
    key = "RecordId" if name == "deleted_records" else "Gsid"
    try:
        stream.sync()
    except FatalAPIError as exc:
        assert "did not return the rows it listed" in str(exc)
    else:
        assert {r[key] for r in records_of(capsys)} == {r[key] for r in rows}


def test_t3_a_second_grain_delete_log_reads_every_row_once(api, capsys):
    rows = [
        {"RecordId": f"1P02D{i:04d}", "DeletedOn": f"2024-02-05T07:17:1{i // 2}Z", "ObjectName": "Company"}
        for i in range(6)
    ]
    api.serve(query_url("record_delete_log"), QueryEngine(rows, {"DeletedOn"}, shape="records", grain_ms=SECOND))
    api.serve(query_url("record_delete_log_high_volume"), QueryEngine([], {"DeletedOn"}, shape="records"))
    stream = make_tap().streams["deleted_records"]
    stream.page_size = 2
    stream.sync()
    got = [r["RecordId"] for r in records_of(capsys)]
    assert sorted(got) == sorted(r["RecordId"] for r in rows)
    assert len(got) == len(set(got))
