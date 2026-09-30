"""Picklist names from a live describe's shape.

A live tenant's describe (2026-09-30) lists a picklist's items under
`options`, as {"value": <item GSID>, "label": <name>}, and keeps the dropdown
category id in `meta.properties.PICKLIST_CATEGORY_ID`. The tap read neither,
so records carried only item GSIDs. A picklist field now carries the names.
"""

import json

import pytest

from tests.conftest import BASE_URL, cta_fields, describe_entry, load, make_tap
from tests.test_regressions_delta import catalog_selecting
from tests.test_streams import SPECS, company_row, serve

LIVE_FIELDS = load("describe_picklist_fields_live.json")
LIVE = {field["fieldName"]: field for field in LIVE_FIELDS}


def item(field_name, label):
    return next(o["value"] for o in LIVE[field_name]["options"] if o["label"] == label)


def category(field_name):
    return LIVE[field_name]["meta"]["properties"]["PICKLIST_CATEGORY_ID"]


def with_live_fields(api):
    api.describes["company"]["fields"].extend(json.loads(json.dumps(LIVE_FIELDS)))


def dropdown_calls(api, category_id):
    return [r for r in api.mocker.request_history if r.path.endswith(f"/dropdowns/{category_id}")]


def test_options_give_item_names_with_inactive_items(api):
    with_live_fields(api)
    stream = make_tap().streams["Company"]

    assert stream.picklists["CompanyType"] == {
        item("CompanyType", "Customer"): "Customer",
        item("CompanyType", "Former Customer"): "Former Customer",
        item("CompanyType", "Prospect"): "Prospect",
    }
    assert "SubSegment_multi_select__gc" in stream.picklists
    assert stream.schema["properties"]["CompanyType"] == {"type": ["null", "string"]}
    assert "CompanyType_label" not in stream.schema["properties"]
    assert dropdown_calls(api, category("CompanyType")) == []


def test_a_field_with_no_options_uses_its_category_id(api):
    with_live_fields(api)
    api.mocker.get(
        f"{BASE_URL}/v1/meta/services/dropdowns/{category('Tags')}",
        json={"requestId": "test", "result": True, "data": {"categoryDetails": {}, "childItems": []}},
    )
    stream = make_tap().streams["Company"]

    assert len(dropdown_calls(api, category("Tags"))) == 1
    assert "Tags" not in stream.picklists


def test_records_carry_the_names_of_live_picklist_items(api):
    with_live_fields(api)
    rows = [company_row(1), company_row(2)]
    rows[0].update(
        CompanyType=item("CompanyType", "Customer"),
        SubSegment_multi_select__gc=";".join(
            [item("SubSegment_multi_select__gc", "Generics"), item("SubSegment_multi_select__gc", "Specialty")]
        ),
    )
    rows[1].update(CompanyType=item("CompanyType", "Prospect"), SubSegment_multi_select__gc="1I00DELETEDITEM")
    serve(api, SPECS["Company"], rows)

    got = {r["Gsid"]: r for r in make_tap().streams["Company"].get_records(None)}
    first, second = got[rows[0]["Gsid"]], got[rows[1]["Gsid"]]
    assert first["CompanyType"] == "Customer"
    assert first["SubSegment_multi_select__gc"] == '["Generics", "Specialty"]'
    assert second["CompanyType"] == "Prospect"
    # An id with no item stays as the id.
    assert second["SubSegment_multi_select__gc"] == "1I00DELETEDITEM"


def cta_describe_with(api, field):
    api.describes["cs_cta"] = describe_entry("cs_cta", cta_fields() + [field])


def test_a_cta_custom_picklist_carries_item_names(api):
    field = json.loads(json.dumps(LIVE["CompanyType"]))
    field.update(fieldName="Risk_Type__gc", objectName="cs_cta")
    cta_describe_with(api, field)
    stream = make_tap().streams["cta"]

    assert stream.schema["properties"]["Risk_Type__gc"] == {"type": ["null", "string"]}
    row = stream.post_process({"Risk_Type__gc": item("CompanyType", "Former Customer")})
    assert row == {"Risk_Type__gc": "Former Customer"}


def test_a_selected_cta_picklist_whose_names_fail_raises(api):
    field = json.loads(json.dumps(LIVE["Tags"]))
    field.update(fieldName="Risk_Tags__gc", objectName="cs_cta")
    cta_describe_with(api, field)
    catalog = catalog_selecting(make_tap(), {"cta"})
    api.dropdown = {"result": False, "errorDesc": "gone"}
    with pytest.raises(Exception, match=r"cta\.Risk_Tags__gc\b"):
        make_tap(catalog=catalog).streams
