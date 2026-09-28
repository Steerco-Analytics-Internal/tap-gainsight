"""Edge cases in schema building, discovery batching and error text."""

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError

from tap_gainsight import client
from tap_gainsight import tap as tap_module
from tap_gainsight.client import GainsightAuthError
from tap_gainsight.streams import CtaStream, ObjectPlan, lookup_columns
from tests.conftest import (
    QueryEngine,
    describe_entry,
    doc_field,
    load,
    lookup_field,
    make_tap,
    query_page,
    query_url,
    standard_fields,
)


def test_long_error_bodies_are_cut_to_an_excerpt(api):
    api.mocker.post(query_url("Company"), status_code=400, text="x" * 2000)
    with pytest.raises(FatalAPIError) as info:
        list(make_tap().streams["Company"].get_records(None))
    assert "x" * client.BODY_EXCERPT_LENGTH + "..." in str(info.value)
    assert "x" * (client.BODY_EXCERPT_LENGTH + 1) not in str(info.value)


def test_user_agent_setting_is_sent(api):
    api.mocker.post(query_url("Company"), json=query_page([]))
    list(make_tap(user_agent="steerco-hotglue/1").streams["Company"].get_records(None))
    request = [r for r in api.mocker.request_history if r.path == "/v1/data/objects/query/Company"][0]
    assert request.headers["User-Agent"] == "steerco-hotglue/1"


def test_lookup_without_a_lookup_name_adds_no_columns():
    field = lookup_field("company", "Csm", "Csm__gr", "gsuser")
    del field["meta"]["lookupDetail"]["lookupName"]
    assert lookup_columns(field, {"gsuser": {"Name", "Email"}}) == []


def test_fields_without_a_name_are_skipped():
    plan = ObjectPlan([{"dataType": "STRING"}, "not a dict", doc_field("Gsid", "x")])
    assert list(plan.properties) == ["Gsid"]
    assert plan.is_filterable_datetime("Missing") is False
    assert plan.is_sortable("Missing") is False


def test_an_object_without_modified_date_is_full_table(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", [doc_field("Gsid", "obj1__gc")])
    api.mocker.post(query_url("obj1__gc"), json=query_page([{"Gsid": "1"}]))
    stream = make_tap(start_date="2024-01-01T00:00:00Z").streams["obj1__gc"]
    assert stream.replication_key is None
    stream.sync()
    body = [r for r in api.mocker.request_history if r.path.endswith("/obj1__gc")][0].json()
    assert "where" not in body
    assert body["orderBy"] == {"Gsid": "asc"}


def test_a_non_filterable_modified_date_is_not_a_replication_key(api):
    fields = standard_fields("obj1__gc")
    fields[2]["meta"]["filterable"] = False
    api.describes["obj1__gc"] = describe_entry("obj1__gc", fields)
    assert make_tap().streams["obj1__gc"].replication_key is None


def test_cta_skips_unnamed_and_duplicate_custom_fields(api):
    stream = CtaStream(
        make_tap(),
        custom_fields=[
            {"dataType": "STRING"},
            {"fieldName": "Comments", "dataType": "STRING"},
            {"fieldName": "Extra__gc", "dataType": "STRING"},
        ],
    )
    assert list(stream.select_map)[-1] == "Extra__gc"
    assert stream.select_map["Comments"] == "Comments"


def test_single_object_batches_drop_a_failing_object(api, monkeypatch):
    monkeypatch.setattr(tap_module, "DESCRIBE_BATCH_SIZE", 1)
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    api.failing["obj1__gc"] = (400, load("describe_not_found_response.json"))
    streams = make_tap().streams
    assert "obj1__gc" not in streams and "Company" in streams


def test_auth_failure_during_one_by_one_retry_raises(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    api.failing["obj1__gc"] = (400, load("describe_not_found_response.json"))
    calls = {"n": 0}
    original = api._describe

    def describe(request, context):
        calls["n"] += 1
        if calls["n"] > 1:
            context.status_code = 401
            return load("unauthorized_response.json")
        return original(request, context)

    api.mocker.post("https://acme.gainsightcloud.com/v1/meta/services/objects/describe", json=describe)
    with pytest.raises(GainsightAuthError):
        make_tap()


def test_connection_errors_in_streams_are_retried(api):
    engine = QueryEngine([{"Gsid": "1", "ModifiedDate": 1}], {"ModifiedDate"})
    api.mocker.post(
        query_url("Company"),
        [
            {"exc": requests.exceptions.ConnectionError("reset")},
            {"json": lambda request, context: engine.respond(request.json())},
        ],
    )
    assert len(list(make_tap().streams["Company"].get_records(None))) == 1


def test_parse_api_datetime_edges():
    from tap_gainsight.client import parse_api_datetime, to_epoch_ms

    assert parse_api_datetime(None) is None
    assert to_epoch_ms(None) is None
    assert to_epoch_ms("2024-02-05T08:34:35.253+05:30") == to_epoch_ms("2024-02-05T03:04:35.253Z")
    assert to_epoch_ms("2024-02-05") == to_epoch_ms("2024-02-05 00:00:00")
    with pytest.raises(ValueError, match="Unrecognized date value"):
        parse_api_datetime("next Tuesday")


def test_base_hooks_must_be_overridden(api):
    from tap_gainsight.client import GainsightStream, SecondChainStream
    from tap_gainsight.streams import CtaSlicedStream

    tap = make_tap()
    stream = tap.streams["Company"]
    with pytest.raises(NotImplementedError):
        list(GainsightStream.fetch_rows(stream, None))
    with pytest.raises(NotImplementedError):
        SecondChainStream.base_payload(stream)
    assert SecondChainStream.can_sort_by_tiebreaker(stream) is True
    with pytest.raises(NotImplementedError):
        CtaSlicedStream.select_list(tap.streams["cta"])


def test_full_table_streams_have_no_filter_start(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", [doc_field("Gsid", "obj1__gc")])
    assert make_tap(start_date="2024-01-01T00:00:00Z").streams["obj1__gc"].filter_start(None) is None


def test_paging_keys_are_selected_even_if_the_catalog_says_no(api, monkeypatch):
    stream = make_tap().streams["Company"]
    monkeypatch.setattr(type(stream), "is_property_selected", lambda self, name: name == "Name")
    assert stream.select_paths() == ["Name", "Gsid", "ModifiedDate"]
    cta = make_tap().streams["cta"]
    monkeypatch.setattr(type(cta), "is_property_selected", lambda self, name: name == "Name")
    assert cta.select_list() == ["name", "ModifiedDate"]


def test_cta_null_pass_pages_until_short(api):
    rows = [{"Gsid": f"1S01NULL{i}", "ModifiedDate": None} for i in range(3)]
    engine = QueryEngine(rows, {"ModifiedDate"})
    api.serve("https://acme.gainsightcloud.com/v2/cockpit/cta/list", engine)
    stream = make_tap().streams["cta"]
    stream.page_size = 2
    got = [r["Gsid"] for r in stream.get_records(None)]
    assert sorted(got) == sorted(r["Gsid"] for r in rows)
    null_pages = [b["pageNumber"] for b in engine.bodies if b["where"]["conditions"][0]["operator"] == "IS_NULL"]
    assert null_pages == [1, 2]


def test_an_unsortable_gsid_falls_back_to_offset_paging(api):
    api.describes["obj1__gc"] = describe_entry(
        "obj1__gc", [doc_field("Gsid", "obj1__gc", meta={"sortable": False})]
    )
    engine = QueryEngine([{"Gsid": f"G{i}"} for i in range(3)], set())
    api.serve(query_url("obj1__gc"), engine)
    stream = make_tap().streams["obj1__gc"]
    stream.page_size = 2
    assert len(list(stream.get_records(None))) == 3
    assert [(b.get("orderBy"), b["offset"]) for b in engine.bodies] == [(None, 0), (None, 2)]


def _select_only(catalog, stream_id):
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            item["metadata"]["selected"] = entry["tap_stream_id"] == stream_id
    return catalog


def test_a_selected_column_gone_from_a_fixed_schema_stream_warns(api, caplog):
    import logging

    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    catalog = _select_only(make_tap().catalog_dict, "cta_deleted")
    for entry in catalog["streams"]:
        if entry["tap_stream_id"] == "cta_deleted":
            entry["schema"]["properties"]["Retired__gc"] = {"type": ["null", "string"]}
            entry["metadata"].append({"breadcrumb": ["properties", "Retired__gc"], "metadata": {"selected": True}})
    assert "cta_deleted" in make_tap(catalog=catalog).streams
    assert "cta_deleted.Retired__gc" in caplog.text


def test_a_selected_lookup_column_whose_field_was_deleted_warns(api, caplog):
    import logging

    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    catalog = _select_only(make_tap().catalog_dict, "Company")
    fields = api.describes["company"]["fields"]
    api.describes["company"]["fields"] = [f for f in fields if f["fieldName"] != "Csm"]
    assert "Company" in make_tap(catalog=catalog).streams
    assert "Company.Csm__gr.Email" in caplog.text
