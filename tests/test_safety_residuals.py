"""Regression tests for the four residuals from the safety check of bbdf150.

Each test failed on commit bbdf150, before its fix.
"""

from __future__ import annotations

import http.client
import io
import json
import logging
import types

import pytest
import requests
import urllib3
from singer_sdk.exceptions import ConfigValidationError

from tests.conftest import BASE_URL, CONFIG, QueryEngine, make_tap, query_url

VAL = "sentinel-record-value-51b2"
LIST_URL = f"{BASE_URL}/v1/meta/services/objects/list"


# 1. Cookies are never stored or sent.


class CookieServer(requests.adapters.HTTPAdapter):
    """A transport that answers with Set-Cookie, as a load balancer does.

    requests-mock does not store response cookies in the session, so this
    builds a real urllib3 response. requests then stores cookies exactly as
    it would from the network.
    """

    def __init__(self, respond):
        super().__init__()
        self.respond = respond
        self.requests = []

    def send(self, request, **kwargs):
        self.requests.append(request)
        body = json.dumps(self.respond(request)).encode()
        message = http.client.HTTPMessage()
        message["Content-Type"] = "application/json"
        message["Set-Cookie"] = "AWSALB=abc123; Path=/"
        original = types.SimpleNamespace(msg=message, isclosed=lambda: True, close=lambda: None)
        raw = urllib3.HTTPResponse(
            body=io.BytesIO(body),
            headers={"Content-Type": "application/json", "Set-Cookie": "AWSALB=abc123; Path=/"},
            status=200,
            preload_content=False,
            original_response=original,
        )
        return self.build_response(request, raw)


def test_a_metadata_set_cookie_is_not_stored_or_sent():
    from tap_gainsight.client import GainsightMetadataClient, RateLimiter

    server = CookieServer(lambda request: {"result": True, "data": []})
    session = requests.Session()
    session.mount("https://", server)
    client = GainsightMetadataClient(CONFIG, RateLimiter(), session=session)
    client.list_objects()
    client.list_objects()
    assert len(server.requests) == 2
    assert "Cookie" not in server.requests[1].headers
    assert len(session.cookies) == 0


def test_a_query_set_cookie_does_not_stop_the_sync(api):
    rows = [{"Gsid": f"1P02C{i:04d}", "ModifiedDate": 1707121475253 + i * 1000} for i in range(5)]
    engine = QueryEngine(rows, {"ModifiedDate"})
    server = CookieServer(lambda request: engine.respond(json.loads(request.body)))
    stream = make_tap().streams["Company"]
    # Discovery ran against the fake API. The sync goes through the server.
    api.mocker.stop()
    stream.requests_session.mount("https://", server)
    stream.page_size = 2
    assert len(list(stream.get_records(None))) == 5
    assert len(server.requests) > 1
    assert not any("Cookie" in r.headers for r in server.requests)
    assert len(stream.requests_session.cookies) == 0


# 2. The stricter response summary.


@pytest.fixture
def all_logs():
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.setLevel(logging.DEBUG)
    saved = []
    for name in ["", "backoff", "singer_sdk", "tap-gainsight"]:
        logger = logging.getLogger(name)
        saved.append((logger, logger.level))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    yield buffer
    for logger, level in saved:
        logger.removeHandler(handler)
        logger.setLevel(level)


@pytest.mark.parametrize(
    "body",
    [
        {"result": False, "errorCode": "GSOBJ_1008", "errorDesc": f"Row {VAL} is bad = true"},
        {"result": False, "errorCode": "GSOBJ_1008", "errorDesc": [VAL]},
        {"result": False, "errorCode": "GSOBJ_1008", "title": f"Bad row {VAL}"},
        {"result": False, "errorCode": [VAL], "title": {"x": VAL}},
        {"result": False, "errorCode": f"{VAL}", "errorDesc": "x"},
    ],
)
def test_residual_leak_cases_never_reach_logs_or_exceptions(api, all_logs, body):
    from singer_sdk.exceptions import FatalAPIError

    api.mocker.post(query_url("Company"), status_code=400, json=body)
    # A clean API error, not a crash on an odd field type.
    with pytest.raises(FatalAPIError) as info:
        list(make_tap().streams["Company"].get_records(None))
    assert VAL not in str(info.value)
    assert VAL not in all_logs.getvalue()


def test_the_summary_shape():
    import requests

    from tap_gainsight.client import response_summary

    def response(status, content, content_type="application/json"):
        r = requests.Response()
        r.status_code = status
        r._content = content.encode()
        r.headers["Content-Type"] = content_type
        return r

    assert response_summary(response(400, '{"errorCode": "GSOBJ_1011", "errorDesc": "No entity"}')) == (
        "HTTP 400, errorCode GSOBJ_1011 (no rows match the criteria)"
    )
    assert response_summary(response(400, '{"title": "OBJECT_NOT_FOUND", "errorDesc": "x"}')) == (
        "HTTP 400, title OBJECT_NOT_FOUND (the object was not found)"
    )
    assert response_summary(response(400, '{"errorCode": "GSOBJ_9999"}')) == "HTTP 400, errorCode GSOBJ_9999"
    assert response_summary(response(400, '{"errorCode": 7}')) == "HTTP 400, errorCode of type int"
    assert response_summary(response(400, '{"title": "has spaces"}')) == (
        "HTTP 400, title that is not a code (10 characters)"
    )
    assert response_summary(response(400, '{"errorCode": "' + "A" * 51 + '"}')) == (
        "HTTP 400, errorCode that is not a code (51 characters)"
    )
    assert response_summary(response(502, "<html>busy</html>", "text/html")) == "HTTP 502, a 17-byte body"


# 3. No trailing newline sneaks past a regex.


# "acme\n" was already rejected, because the suffix lands after the newline.
# It stays as a guard.
@pytest.mark.parametrize("domain", ["acme.gainsightcloud.com\n", "https://acme.gainsightcloud.com\n", "acme\n"])
def test_a_trailing_newline_in_the_domain_is_rejected(domain):
    from tap_gainsight.client import pinned_host

    with pytest.raises(ValueError):
        pinned_host(domain)
    with pytest.raises(ConfigValidationError):
        make_tap(domain=domain)


def test_a_trailing_newline_in_a_custom_domain_is_rejected():
    from tap_gainsight.client import pinned_host

    with pytest.raises(ValueError):
        pinned_host("companyapi.yourcompany.com\n", "companyapi.yourcompany.com\n")


def test_a_listed_object_name_with_a_trailing_newline_is_skipped(api):
    api.list_payload["data"].append({**api.list_payload["data"][0], "objectName": "evil__gc\n"})
    make_tap().streams
    assert not any("\n" in name for call in api.describe_calls() for name in call)


# 4. Object names in config are checked.


@pytest.mark.parametrize("names", [["Company", "../Company"], ["Person\n"], ["{access_key}"], ["a b"]])
def test_bad_object_names_in_config_are_rejected(names):
    with pytest.raises(ConfigValidationError, match="not plain API names"):
        make_tap(objects=names)


def test_a_non_string_object_name_is_rejected_by_the_schema():
    """A guard: the SDK's JSON schema already rejected this."""
    with pytest.raises(ConfigValidationError, match="not of type 'string'"):
        make_tap(objects=[7])


def test_good_object_names_in_config_are_accepted(api):
    """A guard: valid names must keep working."""
    assert "Company" in make_tap(objects=["Person", "renewal__gc"]).streams
