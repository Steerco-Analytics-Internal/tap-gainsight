"""Picklist labels from a live describe's shape.

A live tenant's describe (2026-09-30) lists a picklist's items under
`options`, as {"value": <item GSID>, "label": <name>}, and keeps the dropdown
category id in `meta.properties.PICKLIST_CATEGORY_ID`. The tap read neither,
so no picklist got a label column and records carried only item GSIDs.
"""

import json

from tests.conftest import BASE_URL, load, make_tap
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


def test_options_give_a_label_column_with_inactive_items(api):
    with_live_fields(api)
    stream = make_tap().streams["Company"]

    id_field, items = stream.plan.labels["CompanyType_label"]
    assert id_field == "CompanyType"
    assert items == {
        item("CompanyType", "Customer"): "Customer",
        item("CompanyType", "Former Customer"): "Former Customer",
        item("CompanyType", "Prospect"): "Prospect",
    }
    assert "SubSegment_multi_select__gc_label" in stream.plan.labels
    assert stream.schema["properties"]["CompanyType_label"] == {"type": ["null", "string"]}
    assert dropdown_calls(api, category("CompanyType")) == []


def test_a_field_with_no_options_uses_its_category_id(api):
    with_live_fields(api)
    api.mocker.get(
        f"{BASE_URL}/v1/meta/services/dropdowns/{category('Tags')}",
        json={"requestId": "test", "result": True, "data": {"categoryDetails": {}, "childItems": []}},
    )
    stream = make_tap().streams["Company"]

    assert len(dropdown_calls(api, category("Tags"))) == 1
    assert "Tags_label" not in stream.schema["properties"]


def test_records_carry_the_names_of_live_picklist_items(api):
    with_live_fields(api)
    rows = [company_row(1), company_row(2)]
    rows[0].update(
        CompanyType=item("CompanyType", "Customer"),
        SubSegment_multi_select__gc=";".join(
            [item("SubSegment_multi_select__gc", "Generics"), item("SubSegment_multi_select__gc", "Specialty")]
        ),
    )
    rows[1].update(CompanyType=item("CompanyType", "Prospect"))
    serve(api, SPECS["Company"], rows)

    got = {r["Gsid"]: r for r in make_tap().streams["Company"].get_records(None)}
    first, second = got[rows[0]["Gsid"]], got[rows[1]["Gsid"]]
    assert first["CompanyType"] == item("CompanyType", "Customer")
    assert first["CompanyType_label"] == "Customer"
    assert first["SubSegment_multi_select__gc_label"] == '["Generics", "Specialty"]'
    assert second["CompanyType_label"] == "Prospect"
