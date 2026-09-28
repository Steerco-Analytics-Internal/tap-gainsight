"""Discovery: object list, batched describe, lookups, picklists, failures."""

import logging

import pytest

from tap_gainsight import tap as tap_module
from tap_gainsight.client import GainsightAPIError, GainsightAuthError
from tap_gainsight.streams import CtaStream, MDAObjectStream
from tests.conftest import (
    describe_entry,
    doc_field,
    load,
    make_tap,
    standard_fields,
)


def streams_by_name(tap):
    return dict(tap.streams)


def test_discovery_emits_a_stream_per_listed_object(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    streams = streams_by_name(make_tap())
    # Listed objects that describe, with canonical names for standard ones.
    assert {"Company", "Company_Person", "GsUser", "obj1__gc"} <= set(streams)
    # Dedicated objects never get a generic stream.
    assert "activity_timeline" not in streams and "cs_cta" not in streams
    assert {"timeline", "cta", "deleted_records"} <= set(streams)
    # Listed objects that fail describe are dropped, not fatal.
    assert "email_logs" not in streams and "autoindex10__gc" not in streams


def test_discovery_batches_describe_calls(api):
    make_tap().discover_streams()
    first = api.describe_calls()[0]
    assert len(first) > 1
    assert first[0] == "Company"


def test_company_schema_has_every_field_custom_lookup_and_label(api):
    company = streams_by_name(make_tap())["Company"]
    props = company.schema["properties"]
    assert company.primary_keys == ["Gsid"]
    assert company.replication_key == "ModifiedDate"
    # Custom field from describe.
    assert props["Health_Notes__gc"] == {"type": ["null", "string"]}
    # Type mapping.
    assert props["ModifiedDate"] == {"type": ["null", "string"], "format": "date-time"}
    assert props["ARR"] == {"type": ["null", "number"]}
    assert props["Is_Active__gc"] == {"type": ["null", "boolean"]}
    assert props["Renewal_Date"] == {"type": ["null", "string"]}
    # Lookup to GsUser adds Name and Email with the documented dot notation.
    assert props["Csm__gr.Name"] == {"type": ["null", "string"]}
    assert props["Csm__gr.Email"] == {"type": ["null", "string"]}
    assert props["CreatedBy__gr.Name"] == {"type": ["null", "string"]}
    # Picklists get a label column next to the id.
    assert "Stage_label" in props and "License_Type__gc_label" in props
    for schema in props.values():
        assert "null" in schema["type"]


def test_company_person_gets_the_company_name_through_its_lookup(api):
    stream = streams_by_name(make_tap())["Company_Person"]
    assert "Company_ID__gr.Name" in stream.schema["properties"]
    assert "Company_ID__gr.Email" not in stream.schema["properties"]


def test_lookup_columns_need_the_target_field(api):
    api.describes["gsuser"] = describe_entry("gsuser", standard_fields("gsuser"))
    props = streams_by_name(make_tap())["Company"].schema["properties"]
    assert "Csm__gr.Name" not in props and "Csm__gr.Email" not in props


def test_dropdown_api_is_called_for_category_only_picklists(api):
    stream = streams_by_name(make_tap())["Company"]
    id_field, items = stream.plan.labels["License_Type__gc_label"]
    assert id_field == "License_Type__gc"
    assert items["1I00AABNSR5CLLWSP3F09ND6JHASXGUW8BW4"] == "External"
    dropdown_calls = [r for r in api.mocker.request_history if "/dropdowns/" in r.path]
    assert len(dropdown_calls) == 1


def test_a_failing_dropdown_leaves_the_label_out(api):
    api.dropdown = {"result": False, "errorDesc": "gone"}
    stream = streams_by_name(make_tap())["Company"]
    assert "License_Type__gc_label" not in stream.schema["properties"]
    assert "Stage_label" in stream.schema["properties"]


def test_timeline_schema_comes_from_describe(api):
    timeline = streams_by_name(make_tap())["timeline"]
    props = timeline.schema["properties"]
    assert timeline.object_name == "activity_timeline"
    assert timeline.path == "/v1/data/objects/query/activity_timeline"
    assert timeline.replication_key == "LastModifiedDate"
    for name in ("Gsid", "contextname", "GsCompanyId", "GsRelationshipId", "AuthorId", "Subject", "Notes", "ActivityDate", "Ant__CustomNumber__c"):
        assert name in props


def test_timeline_without_a_modified_date_is_full_table(api):
    api.describes["activity_timeline"] = describe_entry(
        "activity_timeline", [doc_field("Gsid", "activity_timeline")]
    )
    assert streams_by_name(make_tap())["timeline"].replication_key is None


def test_timeline_is_dropped_when_describe_fails(api):
    del api.describes["activity_timeline"]
    assert "timeline" not in streams_by_name(make_tap())


def test_cta_gets_custom_fields_from_the_cs_cta_describe(api):
    cta = streams_by_name(make_tap())["cta"]
    assert isinstance(cta, CtaStream)
    assert cta.schema["properties"]["Quoted_ARR__gc"] == {"type": ["null", "number"]}
    assert "customDate__gc" in cta.date_fields
    assert cta.select_map["Quoted_ARR__gc"] == "Quoted_ARR__gc"
    # Standard cs_cta fields do not duplicate the documented base schema.
    assert "Gsid" not in {k for k in cta.select_map if k not in CtaStream.base_select}


def test_cta_without_cs_cta_in_the_list_uses_the_base_schema(api):
    api.list_payload["data"] = [
        item for item in api.list_payload["data"] if item["objectName"] != "cs_cta"
    ]
    cta = streams_by_name(make_tap())["cta"]
    assert set(cta.schema["properties"]) == set(CtaStream.base_schema["properties"])
    assert not any("cs_cta" in call for call in api.describe_calls())


def test_bad_optional_object_is_dropped_and_the_batch_retried(api, caplog):
    # The SDK's tap logger does not propagate to the root logger.
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    api.failing["obj1__gc"] = (400, load("describe_not_found_response.json"))
    streams = streams_by_name(make_tap())
    assert "obj1__gc" not in streams
    assert "Company" in streams and "Company_Person" in streams
    assert ["obj1__gc"] in api.describe_calls()
    assert "Dropping object obj1__gc" in caplog.text


def test_company_describe_failure_raises(api):
    api.failing["company"] = (400, load("describe_not_found_response.json"))
    with pytest.raises(GainsightAPIError, match="required object Company"):
        make_tap()


def test_company_missing_from_describe_raises(api):
    del api.describes["company"]
    with pytest.raises(GainsightAPIError, match="required object Company"):
        make_tap()


def test_company_with_no_fields_raises(api):
    api.describes["company"] = describe_entry("company", [])
    with pytest.raises(GainsightAPIError, match="required object Company"):
        make_tap()


def test_optional_object_with_no_fields_is_dropped(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", [])
    assert "obj1__gc" not in streams_by_name(make_tap())


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failure_on_the_object_list_raises(api, status):
    api.mocker.get(
        "https://acme.gainsightcloud.com/v1/meta/services/objects/list",
        status_code=status,
        json=load("unauthorized_response.json"),
    )
    with pytest.raises(GainsightAuthError):
        make_tap()


def test_auth_failure_on_describe_raises_even_for_optional_objects(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    api.failing["obj1__gc"] = (401, load("unauthorized_response.json"))
    with pytest.raises(GainsightAuthError):
        make_tap()


def test_auth_failure_on_a_dropdown_raises(api):
    api.mocker.get(
        "https://acme.gainsightcloud.com/v1/meta/services/dropdowns/1I00K3A4X4T2UWD3COJ3FU0KMKYXZL9WEEFK",
        status_code=401,
        json=load("unauthorized_response.json"),
    )
    with pytest.raises(GainsightAuthError):
        make_tap()


def test_allowlist_limits_streams_and_adds_unlisted_objects(api):
    api.describes["person"] = describe_entry("person", standard_fields("person"))
    streams = streams_by_name(make_tap(objects=["person", "company_person"]))
    mda = {n for n, s in streams.items() if isinstance(s, MDAObjectStream) and n != "timeline"}
    assert mda == {"Company", "Person", "Company_Person"}
    # GsUser is still described, so lookup columns still resolve.
    assert "Csm__gr.Email" in streams["Company"].schema["properties"]


def test_allowlist_ignores_dedicated_objects(api):
    streams = streams_by_name(make_tap(objects=["activity_timeline", "cs_cta"]))
    assert "activity_timeline" not in streams and "cs_cta" not in streams


def test_unreadable_objects_are_skipped(api):
    api.describes["obj1__gc"] = describe_entry("obj1__gc", standard_fields("obj1__gc"))
    for item in api.list_payload["data"]:
        if item["objectName"] == "obj1__gc":
            item["readable"] = False
    assert "obj1__gc" not in streams_by_name(make_tap())


def test_large_object_lists_are_described_in_batches(api, monkeypatch):
    monkeypatch.setattr(tap_module, "DESCRIBE_BATCH_SIZE", 2)
    make_tap().discover_streams()
    assert all(len(call) <= 2 for call in api.describe_calls())


def test_discovery_uses_the_shared_rate_limiter(api):
    tap = make_tap()
    before = len(tap.rate_limiter._sent)
    tap.discover_streams()
    metadata_calls = len(api.mocker.request_history)
    assert len(tap.rate_limiter._sent) > before
    assert len(tap.rate_limiter._sent) <= metadata_calls
