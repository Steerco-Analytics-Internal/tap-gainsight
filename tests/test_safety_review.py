"""Regression tests for the adversarial safety review of d1ff83c.

Each test failed on commit d1ff83c, before its fix.
"""

from __future__ import annotations

import io
import json
import logging

import pytest
import requests
from singer_sdk.exceptions import ConfigValidationError

from tests.conftest import BASE_URL, CONFIG, QueryEngine, load, make_tap, query_url

# The probe values from probe_domain.py and probe_geturl.py.
BAD_DOMAINS = [
    "evil.com",
    "acme.gainsightcloud.com.evil.com",
    "acme.gainsightcloud.com@evil.com",
    "user:pw@evil.com",
    "http://acme.gainsightcloud.com",
    "acme.gainsightcloud.com/some/path",
    "acme.gainsightcloud.com:8443",
    "127.0.0.1",
    "169.254.169.254",
    "[::1]",
    "{access_key}.evil.com",
    "acme.gainsightcloud.com#@evil.com",
    "acme.gainsightcloud.com\\@evil.com",
    "acme.gainsightcloud.com?x=1",
    " acme.gainsightcloud.com",
    "acme .gainsightcloud.com",
    "ftp://acme.gainsightcloud.com",
    "https://acme.gainsightcloud.com/",
]
PINNED = "acme.gainsightcloud.com"


def client_module():
    from tap_gainsight import client

    return client


def safety():
    from tap_gainsight import safety as module

    return module


class Capture(requests.adapters.HTTPAdapter):
    def __init__(self):
        super().__init__()
        self.sent = []

    def send(self, request, **kwargs):  # pragma: no cover - must never run
        self.sent.append(request)
        raise AssertionError(f"A request left the tap: {request.url}")


# 1. The host is pinned.


@pytest.mark.parametrize("domain", BAD_DOMAINS)
def test_bad_domains_are_rejected_by_config_validation(domain):
    with pytest.raises(ConfigValidationError):
        make_tap(domain=domain)


@pytest.mark.parametrize("domain", BAD_DOMAINS)
def test_bad_domains_never_reach_the_network_even_without_validation(domain):
    session = requests.Session()
    adapter = Capture()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    with pytest.raises(ValueError):
        client = client_module().GainsightMetadataClient(
            {"domain": domain, "access_key": "SECRETKEY"}, client_module().RateLimiter(), session=session
        )
        client.list_objects()
    assert adapter.sent == []


def test_the_get_url_probe_cannot_put_the_key_in_dns():
    from tap_gainsight.streams import CtaDeletedStream
    from tap_gainsight.tap import TapGainsight

    with pytest.raises(ConfigValidationError):
        TapGainsight(config={"access_key": "SECRETKEY", "domain": "{access_key}.evil.com"}, parse_env_config=False)
    original = TapGainsight.discover_streams
    TapGainsight.discover_streams = lambda self: []
    try:
        tap = TapGainsight(
            config={"access_key": "SECRETKEY", "domain": "{access_key}.evil.com"},
            parse_env_config=False,
            validate_config=False,
        )
        with pytest.raises(ValueError):
            CtaDeletedStream(tap).prepare_request(None, {"mode": "nulls", "page": 1})
    finally:
        TapGainsight.discover_streams = original


def test_a_custom_domain_must_be_entered_twice(api):
    with pytest.raises(ConfigValidationError, match="not under gainsightcloud.com"):
        make_tap(domain="companyapi.yourcompany.com")
    with pytest.raises(ConfigValidationError, match="must equal"):
        make_tap(domain="companyapi.yourcompany.com", custom_domain="other.yourcompany.com")
    with pytest.raises(ConfigValidationError, match="not a host name"):
        make_tap(domain="169.254.169.254", custom_domain="169.254.169.254")
    assert client_module().pinned_host("companyapi.yourcompany.com", "companyapi.yourcompany.com") == (
        "companyapi.yourcompany.com"
    )


@pytest.mark.parametrize(
    "url, match",
    [
        ("http://acme.gainsightcloud.com/v1/data/objects/query/Company", "only https"),
        ("https://user:pw@acme.gainsightcloud.com/v1/data/objects/query/Company", "user information"),
        ("https://acme.gainsightcloud.com@evil.com/v1/data/objects/query/Company", "user information"),
        ("https://acme.gainsightcloud.com:8443/v1/data/objects/query/Company", "explicit port"),
        ("https://acme.gainsightcloud.com:443/v1/data/objects/query/Company", "explicit port"),
        ("https://evil.com/v1/data/objects/query/Company", "only talks to"),
        ("https://169.254.169.254/v1/data/objects/query/Company", "only talks to"),
        ("https://acme.gainsightcloud.com.evil.com/v1/data/objects/query/Company", "only talks to"),
    ],
)
def test_send_refuses_any_other_destination_before_io(url, match):
    session = requests.Session()
    session.trust_env = False
    adapter = Capture()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    prepared = session.prepare_request(requests.Request("POST", url, json={"select": ["Gsid"]}))
    with pytest.raises(safety().GainsightSafetyError, match=match):
        safety().send(session, prepared, limiter=None, pinned_host=PINNED)
    assert adapter.sent == []


def test_listed_object_names_that_are_not_plain_are_skipped(api):
    api.list_payload["data"].append({**api.list_payload["data"][0], "objectName": "{access_key}"})
    api.list_payload["data"].append({**api.list_payload["data"][0], "objectName": "../Company"})
    streams = make_tap().streams
    assert not any("{" in name or "/" in name for name in streams)
    assert all("{" not in n and "/" not in n for call in api.describe_calls() for n in call)


# 2. No response bodies or record values in logs or exceptions.

VAL = "SENTINEL-RECORD-VALUE-51b2"


@pytest.fixture
def all_logs():
    """Capture the root, backoff, SDK and tap loggers at DEBUG."""
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setLevel(logging.DEBUG)
    names = ["", "backoff", "singer_sdk", "tap-gainsight"]
    saved = []
    for name in names:
        logger = logging.getLogger(name)
        saved.append((logger, logger.level))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    yield buffer
    for logger, level in saved:
        logger.removeHandler(handler)
        logger.setLevel(level)


@pytest.mark.parametrize(
    "status, kwargs",
    [
        (200, {"json": {"result": True, "data": {"rows": [{"Name": VAL}]}}}),
        (400, {"json": {"result": False, "errorCode": "GSOBJ_1000", "errorDesc": f"Bad row Name={VAL}"}}),
        (400, {"json": {"result": False, "errorCode": "GSOBJ_1000", "errorDesc": "Bad row", "data": [{"Name": VAL}]}}),
        (400, {"json": [{"Name": VAL}]}),
        (503, {"text": f"upstream said {VAL}"}),
        (429, {"json": {"errorDesc": f"slow down: {VAL}"}}),
    ],
)
def test_record_values_never_reach_logs_or_exceptions(api, all_logs, status, kwargs):
    api.mocker.post(query_url("Company"), status_code=status, **kwargs)
    with pytest.raises(Exception) as info:
        list(make_tap().streams["Company"].get_records(None))
    assert VAL not in str(info.value)
    assert VAL not in all_logs.getvalue()


def test_metadata_retries_never_log_the_body(api, all_logs):
    api.mocker.get(f"{BASE_URL}/v1/meta/services/objects/list", status_code=503, text=f"oops {VAL}")
    with pytest.raises(Exception) as info:
        make_tap()
    assert VAL not in str(info.value)
    assert VAL not in all_logs.getvalue()
    assert "Backing off" in all_logs.getvalue()


def test_an_unexpected_shape_names_only_keys_and_types():
    from singer_sdk.exceptions import FatalAPIError

    with pytest.raises(FatalAPIError) as info:
        client_module().extract_rows({"data": {"rows": [{"Name": VAL}], "count": 1}})
    assert "keys {rows: list, count: int}" in str(info.value)
    assert VAL not in str(info.value)


# 3. The default rate is 30 a minute.


def test_the_default_rate_is_30_a_minute(api):
    assert make_tap().rate_limiter.calls == 30
    assert make_tap(max_requests_per_minute=100).rate_limiter.calls == 100


# 4. Batch mode is not supported.


def test_batch_config_is_rejected():
    batch = {"encoding": {"format": "jsonl", "compression": "gzip"}, "storage": {"root": "file:///tmp"}}
    with pytest.raises(ConfigValidationError, match="batch_config is not supported"):
        make_tap(batch_config=batch)


# 5. Hardening.


@pytest.mark.parametrize("header", ["X-HTTP-Method-Override", "X-Method-Override", "Authorization", "Cookie"])
def test_headers_off_the_allowlist_are_refused(header):
    session = requests.Session()
    session.trust_env = False
    prepared = session.prepare_request(
        requests.Request("POST", f"https://{PINNED}/v1/data/objects/query/Company", json={"select": ["Gsid"]}, headers={header: "DELETE"})
    )
    with pytest.raises(safety().GainsightSafetyError, match="header allowlist"):
        safety().send(session, prepared, limiter=None, pinned_host=PINNED)


def test_the_get_describe_entry_is_gone():
    with pytest.raises(safety().GainsightSafetyError):
        safety().check_request("GET", f"https://{PINNED}/v1/meta/services/objects/company/describe", None)


def test_sessions_ignore_the_environment(api):
    tap = make_tap()
    assert tap.metadata_client().session.trust_env is False
    assert tap.streams["Company"].requests_session.trust_env is False
    session = requests.Session()
    prepared = session.prepare_request(requests.Request("GET", f"https://{PINNED}/v1/meta/services/objects/list"))
    with pytest.raises(safety().GainsightSafetyError, match="trust_env"):
        safety().send(session, prepared, limiter=None, pinned_host=PINNED)


def test_a_netrc_file_cannot_add_an_authorization_header(api, tmp_path, monkeypatch):
    netrc = tmp_path / "netrc"
    netrc.write_text(f"machine {PINNED} login someone password hunter2\n")
    netrc.chmod(0o600)
    monkeypatch.setenv("NETRC", str(netrc))
    api.serve(query_url("Company"), QueryEngine([], {"ModifiedDate"}))
    make_tap().streams["Company"].sync()
    assert api.mocker.request_history
    assert not any("Authorization" in r.headers for r in api.mocker.request_history)
    assert json.dumps(CONFIG)  # The config itself is unchanged.
    assert load("describe_request.json")
