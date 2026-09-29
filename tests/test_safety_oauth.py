"""Safety rules for M2M OAuth: the token request and the Authorization header.

The token request is the one POST that is not a data read. It is allowed
only with its documented path, form body and Basic header, to the pinned
host. Every other request may carry only a Bearer token.
"""

from __future__ import annotations

import base64
import json

import pytest
import requests

from tests.conftest import BASE_URL

PINNED = "acme.gainsightcloud.com"
TOKEN_URL = f"{BASE_URL}/v1/users/m2m/oauth/token"
QUERY_URL = f"{BASE_URL}/v1/data/objects/query/Company"
BASIC = "Basic " + base64.b64encode(b"key:secret").decode()
BEARER = "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.abc-_DEF"
FORM = "grant_type=client_credentials"


def safety():
    from tap_gainsight import safety as module

    return module


class Refuse(requests.adapters.HTTPAdapter):
    """A transport that fails the test if a request reaches it."""

    def send(self, request, **kwargs):  # pragma: no cover - must never run
        raise AssertionError(f"A request left the tap: {request.url}")


class Answer(requests.adapters.HTTPAdapter):
    """A transport that records requests and answers 200."""

    def __init__(self):
        super().__init__()
        self.sent = []

    def send(self, request, **kwargs):
        self.sent.append(request)
        response = requests.Response()
        response.status_code = 200
        response.request = request
        response._content = b"{}"
        return response


class Limiter:
    def acquire(self):
        return 0.0


def session_with(adapter):
    session = requests.Session()
    session.trust_env = False
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def send(method, url, headers, data=None, json_body=None, adapter=None):
    session = session_with(adapter or Refuse())
    prepared = session.prepare_request(
        requests.Request(method, url, headers=headers, data=data, json=json_body)
    )
    return safety().send(session, prepared, Limiter(), pinned_host=PINNED)


# 1. The token request.


def test_the_token_request_is_allowed_with_basic_and_the_documented_form():
    adapter = Answer()
    send("POST", TOKEN_URL, {"Authorization": BASIC}, data={"grant_type": "client_credentials"}, adapter=adapter)
    assert len(adapter.sent) == 1
    safety().check_request("POST", TOKEN_URL, FORM)
    safety().check_request("POST", TOKEN_URL, FORM.encode())


@pytest.mark.parametrize(
    "body",
    [
        None,
        "",
        "grant_type=password",
        "grant_type=client_credentials&scope=write",
        "grant_type=client_credentials&grant_type=client_credentials",
        "grant_type=client_credentials&client_secret=x",
        json.dumps({"grant_type": "client_credentials"}),
        "not form data",
    ],
)
def test_any_other_token_body_is_refused(body):
    with pytest.raises(safety().GainsightSafetyError, match="form data|exactly the fields"):
        safety().check_request("POST", TOKEN_URL, body)


@pytest.mark.parametrize(
    "method, path",
    [
        ("GET", "/v1/users/m2m/oauth/token"),
        ("PUT", "/v1/users/m2m/oauth/token"),
        ("DELETE", "/v1/users/m2m/oauth/token"),
        ("POST", "/v1/users/m2m/oauth/token/introspect"),
        ("POST", "/v1/users/m2m/oauth/token/"),
        ("POST", "/v1/users/m2m/oauth/revoke"),
        ("POST", "/v1/users/m2m/oauth"),
    ],
)
def test_only_the_token_path_and_method_are_allowed(method, path):
    with pytest.raises(safety().GainsightSafetyError, match="not on the tap's read-only allowlist"):
        safety().check_request(method, BASE_URL + path, FORM)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.com/v1/users/m2m/oauth/token",
        "https://acme.gainsightcloud.com.evil.com/v1/users/m2m/oauth/token",
        "https://other.gainsightcloud.com/v1/users/m2m/oauth/token",
        "http://acme.gainsightcloud.com/v1/users/m2m/oauth/token",
        "https://acme.gainsightcloud.com:8443/v1/users/m2m/oauth/token",
    ],
)
def test_the_token_request_to_any_other_host_is_refused(url):
    with pytest.raises(safety().GainsightSafetyError, match="Refused"):
        send("POST", url, {"Authorization": BASIC}, data={"grant_type": "client_credentials"})


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": BEARER},
        {"Authorization": "Basic not base64!"},
        {"Authorization": "basic " + BASIC.split(" ")[1]},
        {"Authorization": BASIC + "\r\nX-Evil: 1"},
    ],
)
def test_the_token_request_needs_a_basic_header(headers):
    with pytest.raises(safety().GainsightSafetyError, match="requires Basic authorization"):
        safety().check_authorization("POST", TOKEN_URL, headers)


def test_the_token_request_never_carries_the_access_key():
    with pytest.raises(safety().GainsightSafetyError, match="must not carry AccessKey"):
        send("POST", TOKEN_URL, {"Authorization": BASIC, "AccessKey": "k"}, data={"grant_type": "client_credentials"})


# 2. Authorization on data requests.


def test_bearer_on_a_data_request_is_allowed():
    adapter = Answer()
    send("POST", QUERY_URL, {"Authorization": BEARER}, json_body={"select": ["Gsid"]}, adapter=adapter)
    assert adapter.sent[0].headers["Authorization"] == BEARER


@pytest.mark.parametrize(
    "value",
    [
        BASIC,
        "Token abc",
        "Digest abc",
        "bearer abc",
        "Bearer ",
        "Bearer a b",
        "Bearer abc\r\nX-Evil: 1",
        "DELETE",
    ],
)
@pytest.mark.parametrize(
    "method, url",
    [
        ("POST", QUERY_URL),
        ("GET", f"{BASE_URL}/v1/meta/services/objects/list"),
        ("POST", f"{BASE_URL}/v2/cockpit/cta/list"),
    ],
)
def test_authorization_that_is_not_bearer_is_refused_on_a_data_request(method, url, value):
    with pytest.raises(safety().GainsightSafetyError, match="only Bearer authorization"):
        safety().check_authorization(method, url, {"Authorization": value})


def test_basic_on_a_non_token_path_is_refused_by_send():
    with pytest.raises(safety().GainsightSafetyError, match="only Bearer authorization"):
        send("POST", QUERY_URL, {"Authorization": BASIC}, json_body={"select": ["Gsid"]})


def test_basic_on_the_introspect_path_is_refused_by_send():
    with pytest.raises(safety().GainsightSafetyError, match="only Bearer authorization"):
        send("POST", TOKEN_URL + "/introspect", {"Authorization": BASIC}, json_body={"access_token": "x"})


def test_a_data_request_cannot_carry_both_credentials():
    with pytest.raises(safety().GainsightSafetyError, match="not both"):
        send("POST", QUERY_URL, {"Authorization": BEARER, "AccessKey": "k"}, json_body={"select": ["Gsid"]})


def test_the_access_key_alone_is_still_allowed():
    safety().check_authorization("POST", QUERY_URL, {"AccessKey": "k"})
