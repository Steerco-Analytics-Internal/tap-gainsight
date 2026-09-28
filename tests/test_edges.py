"""Edge cases in schema building, discovery batching and error text."""

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError

from tap_gainsight import client
from tap_gainsight import tap as tap_module
from tap_gainsight.client import GainsightAuthError
from tap_gainsight.streams import CtaStream, ObjectPlan, lookup_columns
from tests.conftest import (
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
    request = [r for r in api.mocker.request_history if r.path == "/v1/data/objects/query/company"][0]
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
    api.mocker.post(
        query_url("Company"),
        [{"exc": requests.exceptions.ConnectionError("reset")}, {"json": query_page([{"Gsid": "1"}])}],
    )
    assert len(list(make_tap().streams["Company"].get_records(None))) == 1
