"""Hotglue's per-stream record limit, `_hg_max_records_limit`.

A field-sample job sends it in the config. Each named stream must stop at
its limit before it sends another request, the run must exit cleanly, and no
bookmark may move.
"""

import json

import pytest
from click.testing import CliRunner
from singer_sdk.exceptions import ConfigValidationError

from tap_gainsight.client import RECORD_LIMITS_SETTING, SecondChainStream
from tap_gainsight.tap import TapGainsight
from tests.conftest import BASE_URL, CONFIG, QueryEngine, load, make_tap, query_url, requests_to
from tests.test_streams import company_row, cta_deleted_row, cta_row, timeline_row

SYNCED = ["Company", "timeline", "cta", "cta_deleted", "deleted_records"]
OLD = "2024-02-05T08:24:35.253000+00:00"


def invoke(args):
    return CliRunner(mix_stderr=False).invoke(TapGainsight.cli, args)


def discover_catalog(tmp_path):
    config_path = tmp_path / "discover-config.json"
    config_path.write_text(json.dumps(CONFIG))
    result = invoke(["--config", str(config_path), "--discover"])
    assert result.exit_code == 0, result.stderr
    catalog = json.loads(result.stdout)
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            if not item["breadcrumb"]:
                item["metadata"]["selected"] = entry["tap_stream_id"] in SYNCED
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(catalog))
    return catalog_path


def serve_every_stream(api, company_rows=12):
    company = api.serve(
        query_url("Company"),
        QueryEngine([company_row(n) for n in range(company_rows)], {"ModifiedDate"}),
    )
    api.serve(
        query_url("activity_timeline"),
        QueryEngine([timeline_row(n) for n in range(5)], {"ModifiedDate"}, shape="records"),
    )
    api.serve(
        f"{BASE_URL}/v2/cockpit/cta/list",
        QueryEngine([cta_row(n) for n in range(5)], {"ModifiedDate"}, unordered=True),
    )
    api.serve(
        f"{BASE_URL}/v2/cockpit/cta/deleted/list",
        QueryEngine([cta_deleted_row(n) for n in range(5)], {"ModifiedDate"}, unordered=True),
    )
    api.serve(
        query_url("record_delete_log"),
        QueryEngine(load("delete_log_response.json")["data"]["records"], {"DeletedOn"}, shape="records"),
    )
    high_volume = api.serve(
        query_url("record_delete_log_high_volume"),
        QueryEngine(load("delete_log_response.json")["data"]["records"], {"DeletedOn"}, shape="records"),
    )
    return company, high_volume


def sample_run(tmp_path, limits, state=None):
    catalog_path = discover_catalog(tmp_path)
    config_path = tmp_path / "sample-config.json"
    config_path.write_text(json.dumps({**CONFIG, RECORD_LIMITS_SETTING: limits}))
    args = ["--config", str(config_path), "--catalog", str(catalog_path)]
    if state is not None:
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps(state))
        args += ["--state", str(state_path)]
    result = invoke(args)
    assert result.exit_code == 0, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def records(messages, stream):
    return [m["record"] for m in messages if m["type"] == "RECORD" and m["stream"] == stream]


def test_a_sample_run_stops_every_named_stream_and_exits_cleanly(api, tmp_path):
    serve_every_stream(api)
    limits = {name: 2 for name in SYNCED}
    messages = sample_run(tmp_path, limits)
    for name in SYNCED:
        assert len(records(messages, name)) == 2, name


def company_requests(api, tmp_path, limits):
    tmp_path.mkdir(exist_ok=True)
    company, _ = serve_every_stream(api, company_rows=12)
    messages = sample_run(tmp_path, limits)
    return len(records(messages, "Company")), len(company.bodies)


def test_a_stream_stops_before_it_sends_another_request(api, tmp_path, monkeypatch):
    # Pages of 3 rows, one second apart. The rows in a page's last second are
    # held until a drain request confirms the second is complete. So 3 rows
    # cost the scan and its drain, and nothing after.
    monkeypatch.setattr(SecondChainStream, "page_size", 3)
    assert company_requests(api, tmp_path, {"Company": 3}) == (3, 2)


def test_a_limit_past_one_page_reads_only_the_pages_it_needs(api, tmp_path, monkeypatch):
    monkeypatch.setattr(SecondChainStream, "page_size", 3)
    rows, requests = company_requests(api, tmp_path, {"Company": 4})
    assert rows == 4
    assert requests == 3


def test_a_limited_read_sends_fewer_requests_than_a_full_read(api, tmp_path, monkeypatch):
    monkeypatch.setattr(SecondChainStream, "page_size", 3)
    full_rows, full_requests = company_requests(api, tmp_path / "full", {"timeline": 1})
    limited_rows, limited_requests = company_requests(api, tmp_path / "limited", {"Company": 3})
    assert (full_rows, limited_rows) == (12, 3)
    assert limited_requests < full_requests


def test_the_limit_counts_across_partitions(api, tmp_path):
    _, high_volume = serve_every_stream(api)
    messages = sample_run(tmp_path, {"deleted_records": 1})
    assert len(records(messages, "deleted_records")) == 1
    assert high_volume.bodies == []
    assert requests_to(api.mocker, "/v1/data/objects/query/record_delete_log_high_volume") == []


def test_a_sample_run_moves_no_bookmark(api, tmp_path):
    serve_every_stream(api)
    state = {"bookmarks": {"Company": {"replication_key": "ModifiedDate", "replication_key_value": OLD}}}
    messages = sample_run(tmp_path, {name: 2 for name in SYNCED}, state=state)
    bookmarks = [m for m in messages if m["type"] == "STATE"][-1]["value"]["bookmarks"]
    assert bookmarks["Company"]["replication_key_value"] == OLD
    for name in ("timeline", "cta", "cta_deleted"):
        assert "replication_key_value" not in bookmarks.get(name, {}), name
    for partition in bookmarks.get("deleted_records", {}).get("partitions", []):
        assert "replication_key_value" not in partition


def test_a_stream_the_setting_leaves_out_has_no_limit(api, tmp_path):
    serve_every_stream(api, company_rows=12)
    messages = sample_run(tmp_path, {"timeline": 1})
    assert len(records(messages, "timeline")) == 1
    assert len(records(messages, "Company")) == 12


@pytest.mark.parametrize(
    "value",
    ["10", [10], {"Company": 0}, {"Company": -1}, {"Company": "10"}, {"Company": 2.5}, {"Company": True}],
)
def test_a_bad_setting_is_a_config_error(api, value):
    with pytest.raises(ConfigValidationError, match=RECORD_LIMITS_SETTING):
        make_tap(**{RECORD_LIMITS_SETTING: value}).streams


@pytest.mark.parametrize("value", [[10], {"Company": 0}])
def test_discovery_refuses_a_bad_setting(api, tmp_path, value):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({**CONFIG, RECORD_LIMITS_SETTING: value}))
    result = invoke(["--config", str(config_path), "--discover"])
    assert result.exit_code != 0
    assert isinstance(result.exception, ConfigValidationError)
    assert RECORD_LIMITS_SETTING in str(result.exception)
