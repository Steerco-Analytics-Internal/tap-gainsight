"""The tap is read-only, enforced in code, and keeps secrets out of output.

Each test failed on commit 9f6ec2c, before the safety layer existed.
"""

from __future__ import annotations

import json
import logging

import pytest
import requests
from click.testing import CliRunner
from singer_sdk.exceptions import ConfigValidationError

from tests.conftest import BASE_URL, CONFIG, QueryEngine, load, make_tap, query_url


def safety():
    from tap_gainsight import safety as module

    return module


def body(payload):
    return json.dumps(payload).encode()


# 1. The allowlist.


@pytest.mark.parametrize(
    "method, path, payload",
    [
        ("GET", "/v1/meta/services/objects/list?po=company&em=false", None),
        ("POST", "/v1/meta/services/objects/describe", load("describe_request.json")),
        ("GET", "/v1/meta/services/dropdowns/1I00K3A4X4T2UWD3COJ3FU0KMKYXZL9WEEFK", None),
        ("POST", "/v1/data/objects/query/Company", load("company_query_request.json")),
        ("POST", "/v1/data/objects/query/activity_timeline", load("timeline_query_request.json")),
        ("POST", "/v1/data/objects/query/record_delete_log", load("delete_log_request.json")),
        ("POST", "/v1/data/objects/query/record_delete_log_high_volume", load("delete_log_request.json")),
        # The Fetch CTA sample also sends "linkedObject". The tap does not,
        # so the allowlist leaves it out.
        ("POST", "/v2/cockpit/cta/list", {k: v for k, v in load("cta_list_request.json").items() if k != "linkedObject"}),
        ("POST", "/v2/cockpit/cta/deleted/list", load("cta_deleted_list_request.json")),
    ],
)
def test_documented_reads_are_allowed(method, path, payload):
    safety().check_request(method, BASE_URL + path, body(payload) if payload else None)


@pytest.mark.parametrize("method", ["PUT", "DELETE", "PATCH"])
@pytest.mark.parametrize("path", ["/v1/data/objects/query/Company", "/v1/data/objects/Company", "/v2/cockpit/cta/list"])
def test_write_methods_are_refused(method, path):
    with pytest.raises(safety().GainsightSafetyError, match="not on the tap's read-only allowlist"):
        safety().check_request(method, BASE_URL + path, None)


@pytest.mark.parametrize(
    "path",
    [
        "/v1/data/objects/Company",  # Insert API.
        "/v1/data/objects/query/../Company",
        "/v1/data/objects/query/..%2FCompany",
        "/v1/data/objects/query/Company%2F..",
        "/v1/data/objects/query/Company/extra",
        "/v1/data/objects/query/Company/",
        "/v1/data/objects/query//Company",
        "/v1/data/objects/query/./Company",
        "/v1/data/objects/query/",
        "/v2/cockpit/cta",
        "/v2/cockpit/cta/list/extra",
        "/v2/cockpit/cta/deleted",
        "/v1/ant/es/activity",
        "/v1/peoplemgmt/v1.0/people",
        "/prefix/v1/data/objects/query/Company",
    ],
)
def test_tricky_or_write_paths_are_refused(path):
    with pytest.raises(safety().GainsightSafetyError):
        safety().check_request("POST", BASE_URL + path, body({"select": ["Gsid"]}))


def test_unexpected_query_keys_are_refused():
    with pytest.raises(safety().GainsightSafetyError, match="query keys"):
        safety().check_request("GET", BASE_URL + "/v1/meta/services/objects/list?po=company&delete=true", None)


def test_a_refused_request_makes_no_network_call(monkeypatch):
    calls = []
    monkeypatch.setattr(requests.Session, "send", lambda self, request, **kw: calls.append(request))
    session = requests.Session()
    session.trust_env = False
    for method, path in [("DELETE", "/v1/data/objects/Company/1P02"), ("POST", "/v1/data/objects/Company")]:
        prepared = session.prepare_request(requests.Request(method, BASE_URL + path, json={"records": [{}]}))
        with pytest.raises(safety().GainsightSafetyError):
            safety().send(session, prepared, limiter=None, pinned_host="acme.gainsightcloud.com")
    # requests may normalize "..", so the dot trick is checked after that too.
    prepared = session.prepare_request(requests.Request("POST", BASE_URL + "/v1/data/objects/query/../Company", json={}))
    with pytest.raises(safety().GainsightSafetyError):
        safety().send(session, prepared, limiter=None, pinned_host="acme.gainsightcloud.com")
    assert calls == []


@pytest.mark.parametrize("method", ["PUT", "DELETE", "PATCH"])
def test_a_stream_forced_to_write_is_refused_before_io(api, method):
    stream = make_tap().streams["Company"]
    before = len(api.mocker.request_history)
    stream.rest_method = method
    with pytest.raises(safety().GainsightSafetyError):
        list(stream.get_records(None))
    assert len(api.mocker.request_history) == before


def test_a_stream_pointed_at_the_insert_path_is_refused_before_io(api):
    stream = make_tap().streams["Company"]
    before = len(api.mocker.request_history)
    stream.object_name = "../Company"
    with pytest.raises(safety().GainsightSafetyError):
        list(stream.get_records(None))
    assert len(api.mocker.request_history) == before


# 2. Request bodies carry read keys only.


@pytest.mark.parametrize(
    "path, payload, match",
    [
        ("/v1/data/objects/query/Company", {"select": ["Gsid"], "records": [{"Name": "x"}]}, "not read keys"),
        ("/v1/data/objects/query/Company", {"select": ["Gsid"], "data": {"Name": "x"}}, "not read keys"),
        ("/v1/data/objects/query/Company", {"select": ["Gsid"], "where": {"conditions": [{"records": []}], "expression": "A"}}, "write keys"),
        ("/v1/data/objects/query/Company", {"select": ["Gsid"], "pageSize": 1}, "not read keys"),
        ("/v2/cockpit/cta/list", {"select": ["name"], "limit": 1}, "not read keys"),
        ("/v2/cockpit/cta/list", {"select": ["name"], "where": {"data": {}}}, "write keys"),
        ("/v1/meta/services/objects/describe", {"objectNames": ["company"], "updateKeys": ["Gsid"]}, "not read keys"),
    ],
)
def test_write_payloads_are_refused(path, payload, match):
    with pytest.raises(safety().GainsightSafetyError, match=match):
        safety().check_request("POST", BASE_URL + path, body(payload))


@pytest.mark.parametrize("raw, match", [(b"not json", "not JSON"), (b"[1, 2]", "JSON object"), (None, "JSON object")])
def test_malformed_post_bodies_are_refused(raw, match):
    with pytest.raises(safety().GainsightSafetyError, match=match):
        safety().check_request("POST", BASE_URL + "/v1/data/objects/query/Company", raw)


def test_a_get_with_a_body_is_refused():
    with pytest.raises(safety().GainsightSafetyError, match="no body"):
        safety().check_request("GET", BASE_URL + "/v1/meta/services/objects/list", b'{"records": []}')


# 3. The rate settings.


@pytest.mark.parametrize("value", [0, 101, 1000, -5])
def test_max_requests_per_minute_can_only_lower_the_limit(value):
    with pytest.raises(ConfigValidationError, match="max_requests_per_minute must be from 1 to 100"):
        make_tap(max_requests_per_minute=value)


def test_max_requests_per_minute_sets_the_limiter(api):
    assert make_tap(max_requests_per_minute=30).rate_limiter.calls == 30
    assert make_tap().rate_limiter.calls == 30


def test_max_requests_must_be_positive():
    with pytest.raises(ConfigValidationError, match="max_requests must be 1 or more"):
        make_tap(max_requests=0)


def test_max_requests_stops_the_run_with_a_clear_error(api):
    discovery = make_tap()
    discovery_calls = len(api.mocker.request_history)
    rows = [{"Gsid": f"1P02C{i:04d}", "ModifiedDate": 1707121475253 + i * 1000} for i in range(9)]
    api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}))
    before = len(api.mocker.request_history)
    tap = make_tap(max_requests=discovery_calls + 2)
    stream = tap.streams["Company"]
    stream.page_size = 2
    with pytest.raises(safety().GainsightRequestCapError, match=f"allows {discovery_calls + 2} per run"):
        list(stream.get_records(None))
    assert len(api.mocker.request_history) - before == discovery_calls + 2
    assert discovery is not None


def test_max_requests_counts_retries(api):
    api.mocker.post(query_url("Company"), status_code=503, text="busy")
    discovery_calls = len(api.mocker.request_history) if make_tap() else 0
    tap = make_tap(max_requests=discovery_calls + 3)
    with pytest.raises(safety().GainsightRequestCapError):
        list(tap.streams["Company"].get_records(None))


# 4. No secrets or record values in logs.


SENTINEL_KEY = "SENTINEL-ACCESS-KEY-7f3a9c"
SENTINEL_VALUE = "SENTINEL-RECORD-VALUE-51b2"


def test_the_access_key_and_record_values_never_reach_logs_or_errors(api, tmp_path, caplog):
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG)
    # The fake API echoes the key in error bodies, as a careless server might.
    echo = {"result": False, "errorCode": "GSOBJ_1002", "errorDesc": f"bad key {SENTINEL_KEY}"}
    api.failing["obj1__gc"] = (400, echo)
    api.describes["obj1__gc"] = api.describes["gsuser"]
    rows = [{"Gsid": "1P02C0001", "Name": SENTINEL_VALUE, "ModifiedDate": 1707121475253}]
    api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}))
    api.mocker.post(f"{BASE_URL}/v2/cockpit/cta/list", status_code=400, json=echo)

    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({**CONFIG, "access_key": SENTINEL_KEY}))
    from tap_gainsight.tap import TapGainsight

    runner = CliRunner(mix_stderr=False)
    discover = runner.invoke(TapGainsight.cli, ["--config", str(config_path), "--discover"])
    catalog = json.loads(discover.stdout)
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            if not item["breadcrumb"]:
                item["metadata"]["selected"] = entry["tap_stream_id"] in {"Company", "cta"}
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(catalog))
    sync = runner.invoke(TapGainsight.cli, ["--config", str(config_path), "--catalog", str(catalog_path)])

    # The sync fails on the cta error, after Company synced.
    assert sync.exception is not None
    error_text = str(sync.exception)
    assert "***" in error_text
    for text in (discover.stdout, discover.stderr, sync.stderr, caplog.text, error_text):
        assert SENTINEL_KEY not in text
    assert SENTINEL_KEY not in sync.stdout
    assert SENTINEL_VALUE in sync.stdout  # The record itself.
    for text in (discover.stderr, sync.stderr, caplog.text, error_text):
        assert SENTINEL_VALUE not in text
    # The key was really sent, so the check above means something.
    assert any(r.headers.get("AccessKey") == SENTINEL_KEY for r in api.mocker.request_history)


def test_a_row_error_names_fields_not_values(api):
    rows = [{"Name": SENTINEL_VALUE, "ModifiedDate": 1707121475253 + i} for i in range(3)]
    api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}))
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    with pytest.raises(Exception) as info:
        list(stream.get_records(None))
    assert "lacks Gsid" in str(info.value)
    assert SENTINEL_VALUE not in str(info.value)
