"""A field of unknown Gainsight type is text, so Hotglue can write it.

Model N's first sync failed in Hotglue with `ArrowInvalid: Could not convert
'True' with type str: tried to convert to boolean`. PICKLIST and other
unmapped fields had the schema ["null", "string", "number", "boolean",
"object", "array"]. The SDK turned each value into True or False. Hotglue's
parquet target made a boolean column for the field, then wrote str(value).
"""

import json
import logging

from tests.conftest import CONFIG, cta_fields, describe_entry, doc_field, make_tap
from tests.test_e2e import parse, run
from tests.test_streams import SPECS, company_row, serve

STAGE_ID = "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3"
OTHER_STAGE_ID = "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF4"


def test_no_schema_field_names_more_than_one_json_type(api):
    # Hotglue's target picks a column type and a value type by different
    # rules, so a field with two JSON types can fail the sync.
    for stream in make_tap().streams.values():
        for name, prop in stream.schema["properties"].items():
            types = prop.get("type")
            types = [types] if isinstance(types, str) else list(types or [])
            kinds = [kind for kind in types if kind != "null"]
            assert len(kinds) <= 1, f"{stream.name}.{name} has types {types}"


def test_unknown_type_values_reach_the_output_as_text(api, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(CONFIG))
    catalog = json.loads(run(["--config", str(config_path), "--discover"]))
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            if not item["breadcrumb"]:
                item["metadata"]["selected"] = entry["tap_stream_id"] == "Company"
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(catalog))

    rows = [company_row(i) for i in range(1, 5)]
    rows[0].update(Stage=STAGE_ID, Is_Active__gc=False)
    rows[1].update(Stage=[STAGE_ID, OTHER_STAGE_ID], Is_Active__gc=True)
    rows[2].update(Stage=0)
    rows[3].update(Stage=True)
    serve(api, SPECS["Company"], rows)

    messages = parse(run(["--config", str(config_path), "--catalog", str(catalog_path)]))
    schema = next(m for m in messages if m["type"] == "SCHEMA" and m["stream"] == "Company")
    assert schema["schema"]["properties"]["Stage"]["type"] == ["null", "string"]
    got = {m["record"]["Gsid"]: m["record"] for m in messages if m["type"] == "RECORD"}
    records = [got[row["Gsid"]] for row in rows]

    assert [record["Stage"] for record in records] == [
        STAGE_ID,
        json.dumps([STAGE_ID, OTHER_STAGE_ID]),
        "0",
        "true",
    ]
    assert records[0]["Stage_label"] == "Kicked Off"
    assert records[1]["Stage_label"] == '["Kicked Off", "Launched"]'
    assert records[0]["Is_Active__gc"] is False
    assert records[1]["Is_Active__gc"] is True


def test_the_tap_logs_each_unknown_type_with_a_count(api, caplog):
    caplog.set_level(logging.INFO)
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    make_tap().streams["Company"]
    assert (
        "Stream Company sends 2 fields as text because the tap does not map "
        "their Gainsight types: PICKLIST (2)."
    ) in caplog.text


def test_a_cta_custom_field_of_unknown_type_is_text(api):
    api.describes["cs_cta"] = describe_entry(
        "cs_cta",
        cta_fields() + [doc_field("Name__gc", "cs_cta", fieldName="Risk__gc", dataType="PICKLIST")],
    )
    stream = make_tap().streams["cta"]
    assert stream.schema["properties"]["Risk__gc"] == {"type": ["null", "string"]}
    assert stream.post_process({"Risk__gc": ["High", "Legal"]}) == {"Risk__gc": '["High", "Legal"]'}
