"""End to end: the CLI discovers, then syncs with a catalog and state."""

import json

from click.testing import CliRunner

from tap_gainsight.tap import TapGainsight
from tests.conftest import BASE_URL, CONFIG, QueryEngine, load, query_url
from tests.test_streams import company_row, cta_deleted_row, cta_row, timeline_row

SYNCED = ["Company", "timeline", "cta", "cta_deleted", "deleted_records"]


def run(args):
    result = CliRunner(mix_stderr=False).invoke(TapGainsight.cli, args, catch_exceptions=False)
    assert result.exit_code == 0, result.stderr
    return result.stdout


def parse(stdout):
    return [json.loads(line) for line in stdout.splitlines() if line.strip()]


def test_discover_then_sync_with_catalog_and_state(api, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({**CONFIG, "start_date": "2024-02-01T00:00:00Z"}))

    # Discovery emits every stream with a full schema.
    catalog = json.loads(run(["--config", str(config_path), "--discover"]))
    entries = {entry["tap_stream_id"]: entry for entry in catalog["streams"]}
    assert set(SYNCED) | {"Company_Person", "GsUser"} <= set(entries)
    company_props = entries["Company"]["schema"]["properties"]
    assert "Health_Notes__gc" in company_props and "Csm__gr.Email" in company_props
    assert entries["Company"]["key_properties"] == ["Gsid"]

    # Select five streams and drop one custom field from Company.
    for tap_stream_id, entry in entries.items():
        for item in entry["metadata"]:
            if not item["breadcrumb"]:
                item["metadata"]["selected"] = tap_stream_id in SYNCED
            elif tap_stream_id == "Company" and item["breadcrumb"][-1] == "Is_Active__gc":
                item["metadata"]["selected"] = False
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(catalog))

    old = "2024-02-05T08:24:35.253000+00:00"
    state = {"bookmarks": {"Company": {"replication_key": "ModifiedDate", "replication_key_value": old}}}
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(state))

    rows = [company_row(5), company_row(9)]
    rows[0]["Health_Notes__gc"] = "Renewal looks safe"
    rows[0]["Is_Active__gc"] = True
    company = api.serve(query_url("company"), QueryEngine(rows, {"ModifiedDate"}))
    api.serve(query_url("activity_timeline"), QueryEngine([timeline_row(1)], {"ModifiedDate"}, shape="records"))
    api.serve(f"{BASE_URL}/v2/cockpit/cta/list", QueryEngine([cta_row(3)], {"ModifiedDate"}, unordered=True))
    api.serve(f"{BASE_URL}/v2/cockpit/cta/deleted/list", QueryEngine([cta_deleted_row(2)], {"ModifiedDate"}, unordered=True))
    api.serve(query_url("record_delete_log"), QueryEngine(load("delete_log_response.json")["data"]["records"], {"DeletedOn"}, shape="records"))
    api.mocker.post(query_url("record_delete_log_high_volume"), status_code=400, json=load("describe_not_found_response.json"))

    messages = parse(run([
        "--config", str(config_path),
        "--catalog", str(catalog_path),
        "--state", str(state_path),
    ]))

    # Message order: SCHEMA before a stream's RECORDs, STATE after them.
    seen_schema = set()
    last_record_index = {}
    for index, message in enumerate(messages):
        if message["type"] == "SCHEMA":
            seen_schema.add(message["stream"])
        elif message["type"] == "RECORD":
            assert message["stream"] in seen_schema
            last_record_index[message["stream"]] = index
    assert set(last_record_index) == set(SYNCED)
    state_indexes = [i for i, m in enumerate(messages) if m["type"] == "STATE"]
    assert state_indexes and state_indexes[-1] > max(last_record_index.values())
    assert messages[-1]["type"] == "STATE"

    # Only selected streams, and the deselected field is gone.
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == set(SYNCED)
    company_records = [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == "Company"]
    assert company_records[0]["Health_Notes__gc"] == "Renewal looks safe"
    assert "Is_Active__gc" not in company_records[0]

    # The query used the bookmark less 24 hours, and selected the custom field.
    body = company.bodies[0]
    assert body["where"]["conditions"][0]["value"] == ["2024-02-04T08:24:35.253+0000"]
    assert "Health_Notes__gc" in body["select"] and "Is_Active__gc" not in body["select"]

    # Bookmarks advance.
    bookmarks = messages[-1]["value"]["bookmarks"]
    assert bookmarks["Company"]["replication_key_value"] == "2024-02-05T08:24:44.253000+00:00"
    assert bookmarks["timeline"]["replication_key_value"] == "2024-02-05T08:24:36.253000+00:00"
    assert bookmarks["cta"]["replication_key_value"] == "2024-02-05T11:24:35.253Z"
    assert bookmarks["cta_deleted"]["replication_key_value"] == "2024-02-05T10:24:35.253Z"
    delete_partitions = {
        p["context"]["delete_log"]: p.get("replication_key_value")
        for p in bookmarks["deleted_records"]["partitions"]
    }
    assert delete_partitions["record_delete_log"] == "2024-02-05T08:46:15Z"
