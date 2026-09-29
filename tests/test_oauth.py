"""M2M OAuth: config validation, the token request, caching, refresh and errors.

The token request and response follow Gainsight's "Generate REST API Key"
page, "Get Access Token API". The form body is RFC 6749 section 4.4, which
that page links.
"""

from __future__ import annotations

import base64
import json
import logging
import time
import traceback

import pytest
import requests
import requests_mock as requests_mock_lib
from click.testing import CliRunner
from singer_sdk.exceptions import ConfigValidationError, FatalAPIError

from tap_gainsight import client
from tap_gainsight.client import (
    GainsightAPIError,
    GainsightAuth,
    GainsightAuthError,
    GainsightMetadataClient,
    GainsightTokenError,
    RateLimiter,
    request_secrets,
    response_summary,
)
from tap_gainsight.tap import TapGainsight
from tests.conftest import (
    ACCESS_KEY,
    BASE_URL,
    CLIENT_ID,
    CLIENT_SECRET,
    CONFIG,
    OAUTH_CONFIG,
    TOKEN_URL,
    QueryEngine,
    load,
    make_oauth_tap,
    make_tap,
    query_url,
    token_response,
)

LIST_URL = f"{BASE_URL}/v1/meta/services/objects/list"
BASIC = "Basic " + base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
IP_REFUSAL = {
    "result": False,
    "errorCode": "GS_APIG_2402",
    "errorDesc": "IP Address does not lie in range of whitelisted ips",
}


def token_calls(mocker):
    return [r for r in mocker.request_history if r.url == TOKEN_URL]


def data_calls(mocker):
    return [r for r in mocker.request_history if r.url != TOKEN_URL]


def oauth_metadata_client(config=None):
    return GainsightMetadataClient(config or OAUTH_CONFIG, RateLimiter())


# 1. Config validation.


@pytest.mark.parametrize(
    "credentials",
    [
        {"access_key": ACCESS_KEY},
        {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
    ],
)
def test_each_valid_credential_combination_passes(api, credentials):
    api.mocker.post(TOKEN_URL, json=token_response())
    tap = TapGainsight(config={"domain": "acme", **credentials}, parse_env_config=False)
    assert tap.auth.method == ("oauth" if credentials.get("client_id") else "access_key")


@pytest.mark.parametrize(
    "credentials, message",
    [
        ({}, "No credentials are set"),
        ({"access_key": ""}, "The `access_key` setting must be"),
        ({"access_key": "", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}, "The `access_key` setting must be"),
        ({"access_key": ACCESS_KEY, "client_id": CLIENT_ID}, "not both"),
        ({"access_key": ACCESS_KEY, "client_secret": CLIENT_SECRET}, "not both"),
        ({"access_key": ACCESS_KEY, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}, "not both"),
        ({"client_id": CLIENT_ID}, "needs both `client_id`"),
        ({"client_secret": CLIENT_SECRET}, "needs both `client_id`"),
        ({"client_id": CLIENT_ID, "client_secret": ""}, "The `client_secret` setting must be"),
    ],
)
def test_each_invalid_combination_fails_before_any_request(credentials, message):
    with requests_mock_lib.Mocker() as m:
        with pytest.raises(ConfigValidationError, match=message) as info:
            TapGainsight(config={"domain": "acme", **credentials}, parse_env_config=False)
        # Discovery skips config validation, so the tap's auth checks too.
        with pytest.raises(ConfigValidationError, match=message):
            TapGainsight(
                config={"domain": "acme", **credentials}, parse_env_config=False, validate_config=False
            )
        assert m.call_count == 0
    for secret in (ACCESS_KEY, CLIENT_ID, CLIENT_SECRET):
        assert secret not in str(info.value)


VALID = {"access_key": ACCESS_KEY, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}


@pytest.mark.parametrize("setting", ["access_key", "client_id", "client_secret"])
@pytest.mark.parametrize(
    "shape",
    [
        lambda value: f" {value}",  # Leading space.
        lambda value: f"{value}\n",  # Trailing newline.
        lambda value: f"{value}\tx",  # Tab inside.
        lambda value: f"{value}\x00x",  # NUL.
        lambda value: f"{value}\x1bx",  # Escape.
        lambda value: f"{value}\x7fx",  # DEL.
    ],
    ids=["leading-space", "trailing-newline", "tab", "nul", "escape", "del"],
)
def test_a_malformed_credential_fails_validation_before_any_request(api, setting, shape):
    if setting == "access_key":
        credentials = {"access_key": shape(ACCESS_KEY)}
    else:
        credentials = {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, setting: shape(VALID[setting])}
    for validate in (True, False):
        with pytest.raises(ConfigValidationError, match=f"The `{setting}` setting must be") as info:
            TapGainsight(
                config={"domain": "acme", **credentials}, parse_env_config=False, validate_config=validate
            )
        for secret in VALID.values():
            assert secret not in str(info.value)
    assert api.mocker.call_count == 0


@pytest.mark.parametrize("setting", ["access_key", "client_id", "client_secret"])
def test_a_credential_that_is_not_text_fails(setting):
    # The SDK turns secret settings into text, so this reaches only
    # callers that build a config by hand.
    with pytest.raises(ValueError, match=f"The `{setting}` setting must be"):
        client.auth_method({**OAUTH_CONFIG, setting: 12345})


def test_the_metadata_client_refuses_bad_credentials_before_any_request():
    with requests_mock_lib.Mocker() as m:
        with pytest.raises(ValueError, match="needs both"):
            GainsightMetadataClient({"domain": "acme", "client_id": CLIENT_ID}, RateLimiter())
        assert m.call_count == 0


# 2. The token request.


def test_the_token_request_has_the_documented_shape():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, json=token_response())
        auth = GainsightAuth(OAUTH_CONFIG, RateLimiter())
        assert m.call_count == 0  # Lazy: nothing is fetched until needed.
        assert auth.headers() == {"Authorization": "Bearer token-1"}
        (request,) = m.request_history
        assert request.method == "POST"
        assert request.url == TOKEN_URL
        assert request.headers["Authorization"] == BASIC
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert "accesskey" not in {name.lower() for name in request.headers}
        assert request.text == "grant_type=client_credentials"


def test_the_token_is_cached_for_the_run():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
        m.get(LIST_URL, json=load("object_list_response.json"))
        metadata = oauth_metadata_client()
        metadata.list_objects()
        metadata.list_objects()
        assert len(token_calls(m)) == 1
        for request in data_calls(m):
            assert request.headers["Authorization"] == "Bearer token-1"
            assert "accesskey" not in {name.lower() for name in request.headers}


def test_the_token_is_refreshed_within_five_minutes_of_expiry():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response("token-1", 600)}, {"json": token_response("token-2", 600)}])
        auth = GainsightAuth(OAUTH_CONFIG, RateLimiter())
        assert auth.token() == "token-1"
        time.sleep(299)  # 301 seconds left: more than the margin.
        assert auth.token() == "token-1"
        time.sleep(2)  # 299 seconds left: inside the margin.
        assert auth.token() == "token-2"
        assert len(token_calls(m)) == 2


def test_a_short_token_lifetime_refreshes_at_half_its_life():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response("token-1", 60)}, {"json": token_response("token-2", 60)}])
        auth = GainsightAuth(OAUTH_CONFIG, RateLimiter())
        assert auth.token() == "token-1"
        time.sleep(29)  # 31 seconds left: more than half the lifetime.
        assert auth.token() == "token-1"
        time.sleep(2)  # 29 seconds left: less than half.
        assert auth.token() == "token-2"
        assert len(token_calls(m)) == 2


def test_a_token_without_expires_in_lasts_the_documented_day():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": {"access_token": "token-1"}}, {"json": token_response("token-2")}])
        auth = GainsightAuth(OAUTH_CONFIG, RateLimiter())
        auth.token()
        time.sleep(client.DEFAULT_TOKEN_LIFETIME_SECONDS - client.TOKEN_REFRESH_MARGIN_SECONDS - 1)
        assert auth.token() == "token-1"
        time.sleep(2)
        assert auth.token() == "token-2"


@pytest.mark.parametrize(
    "payload, message",
    [
        ({"token_type": "Bearer", "expires_in": 86400}, "no usable `access_token`"),
        ({"access_token": "", "expires_in": 86400}, "no usable `access_token`"),
        ({"access_token": "has space", "expires_in": 86400}, "no usable `access_token`"),
        ({"access_token": 12, "expires_in": 86400}, "no usable `access_token`"),
        ([], "no usable `access_token`"),
        ({"access_token": "t", "expires_in": 0}, "not a finite positive number"),
        ({"access_token": "t", "expires_in": "86400"}, "not a finite positive number"),
        ({"access_token": "t", "expires_in": True}, "not a finite positive number"),
        ({"access_token": "t", "expires_in": -5}, "not a finite positive number"),
    ],
)
def test_an_unusable_token_response_fails(payload, message):
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, json=payload)
        with pytest.raises(GainsightTokenError, match=message):
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()


@pytest.mark.parametrize("lifetime", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_expires_in_fails(monkeypatch, lifetime):
    # simplejson, which requests prefers, refuses NaN and Infinity in a
    # body. The standard json module accepts them. Either way the tap must
    # refuse the value, so the parsed payload is set directly.
    monkeypatch.setattr(client, "json_or_none", lambda response: {"access_token": "t", "expires_in": lifetime})
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, json={})
        with pytest.raises(GainsightTokenError, match="not a finite positive number"):
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()


@pytest.mark.parametrize("body", ['{"access_token": "t", "expires_in": NaN}', '{"access_token": "t", "expires_in": Infinity}'])
def test_a_non_finite_expires_in_on_the_wire_fails(body):
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, text=body)
        with pytest.raises(GainsightTokenError):
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()


@pytest.mark.parametrize(
    "status, message",
    [
        (400, "Gainsight refused the token request. Check the OAuth API Key and Secret"),
        (401, "did not accept the OAuth API Key or Secret"),
        (403, "did not accept the OAuth API Key or Secret"),
    ],
)
def test_a_rejected_token_request_names_the_oauth_settings_without_echoing_them(status, message):
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, status_code=status, json={"error": "unauthorized"})
        with pytest.raises(GainsightTokenError, match=message) as info:
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()
        assert m.call_count == 1
    text = str(info.value)
    for secret in (CLIENT_ID, CLIENT_SECRET, BASIC, BASIC.split(" ")[1]):
        assert secret not in text


def test_the_token_request_retries_429_and_5xx(sleeps):
    with requests_mock_lib.Mocker() as m:
        m.post(
            TOKEN_URL,
            [{"status_code": 429, "text": "slow"}, {"status_code": 503, "text": "busy"}, {"json": token_response()}],
        )
        assert GainsightAuth(OAUTH_CONFIG, RateLimiter()).token() == "token-1"
        assert m.call_count == 3


def test_the_token_request_gives_up_after_max_tries():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, status_code=500, text="boom")
        with pytest.raises(GainsightTokenError, match="gave up"):
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()
        assert m.call_count == client.MAX_TRIES


def test_a_token_request_connection_error_is_raised_without_secrets():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, exc=requests.exceptions.ConnectionError(f"down {CLIENT_SECRET}"))
        with pytest.raises(GainsightTokenError, match="token request failed") as info:
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()
    assert CLIENT_SECRET not in str(info.value)


def test_other_token_failures_raise_api_errors():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, status_code=404, text="missing")
        with pytest.raises(GainsightTokenError, match="404 from the OAuth token request") as info:
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()
        assert not isinstance(info.value, GainsightAuthError)


def test_a_token_redirect_is_not_followed():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, status_code=302, headers={"Location": "https://evil.com/token"})
        with pytest.raises(GainsightTokenError, match="evil.com"):
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()
        assert m.call_count == 1


def test_the_token_request_counts_against_max_requests(api):
    from tap_gainsight.safety import GainsightRequestCapError

    api.mocker.post(TOKEN_URL, json=token_response())
    with pytest.raises(GainsightRequestCapError):
        make_oauth_tap(max_requests=1).discover_streams()
    assert len(token_calls(api.mocker)) == 1
    assert len(data_calls(api.mocker)) == 0


# 3. One gated refresh and one retry after an auth failure.

# A token is replaced after an auth failure only when it is older than this.
AGED = client.TOKEN_MIN_AGE_FOR_REFRESH_SECONDS + 1
UNAUTHORIZED_2401 = load("unauthorized_response.json")
UNAUTHORIZED_1024 = {"result": False, "errorCode": "GSOBJ_1024", "errorDesc": "Invalid authorization headers"}


def aged_metadata_client():
    """An OAuth metadata client whose first token is old enough to replace."""
    metadata = oauth_metadata_client()
    metadata.auth.token()
    time.sleep(AGED)
    return metadata


@pytest.mark.parametrize(
    "failure",
    [
        {"status_code": 401, "json": {}},
        {"status_code": 200, "json": UNAUTHORIZED_2401},
        {"status_code": 400, "json": UNAUTHORIZED_1024},
    ],
    ids=["http-401", "2401-at-200", "1024-at-400"],
)
def test_an_auth_failure_on_a_metadata_call_refreshes_once_and_retries(failure):
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
        m.get(LIST_URL, [failure, {"json": load("object_list_response.json")}])
        assert len(aged_metadata_client().list_objects()) == 11
        assert [r.headers["Authorization"] for r in data_calls(m)] == ["Bearer token-1", "Bearer token-2"]
        assert len(token_calls(m)) == 2


def test_a_second_401_on_a_metadata_call_is_not_retried():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
        m.get(LIST_URL, status_code=401, json={})
        with pytest.raises(GainsightAuthError, match="rejected the OAuth access token.*`client_id`"):
            aged_metadata_client().list_objects()
        assert len(data_calls(m)) == 2
        assert len(token_calls(m)) == 2


def test_a_401_on_a_fresh_token_fails_fast_with_no_token_fetch():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
        m.get(LIST_URL, status_code=401, json={})
        with pytest.raises(GainsightAuthError, match="rejected the OAuth access token"):
            oauth_metadata_client().list_objects()
        assert len(data_calls(m)) == 1
        assert len(token_calls(m)) == 1


def test_a_401_on_a_token_that_is_no_longer_current_is_not_refreshed():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
        auth = GainsightAuth(OAUTH_CONFIG, RateLimiter())
        auth.token()
        time.sleep(AGED)
        assert auth.refresh_after_failure("some-older-token") is False
        assert auth.refresh_after_failure(None) is False
        assert auth.refresh_after_failure("token-1") is True
        assert auth.token() == "token-2"
        assert len(token_calls(m)) == 2


def test_alternating_401_and_429_on_a_metadata_call_fetch_at_most_one_token(sleeps):
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, [{"json": token_response(f"token-{i}")} for i in range(1, 10)])
        m.get(LIST_URL, [{"status_code": 401, "json": {}}, {"status_code": 429, "text": "slow"}] * 10)
        with pytest.raises(GainsightAuthError):
            aged_metadata_client().list_objects()
        assert len(token_calls(m)) == 2


def test_a_401_with_the_access_key_is_never_retried():
    with requests_mock_lib.Mocker() as m:
        m.get(LIST_URL, status_code=401, json={})
        with pytest.raises(GainsightAuthError, match="rejected the access key.*`access_key`"):
            GainsightMetadataClient(CONFIG, RateLimiter()).list_objects()
        assert m.call_count == 1


def company_rows():
    return [{"Gsid": "1P02C0001", "Name": "Acme", "ModifiedDate": 1707121475253}]


def company_queries(api):
    return [r for r in api.mocker.request_history if r.url == query_url("Company")]


@pytest.mark.parametrize(
    "failure",
    [
        {"status_code": 401, "json": {}},
        {"status_code": 200, "json": UNAUTHORIZED_2401},
        {"status_code": 400, "json": UNAUTHORIZED_1024},
    ],
    ids=["http-401", "2401-at-200", "1024-at-400"],
)
def test_an_auth_failure_on_a_stream_refreshes_once_and_retries(api, failure):
    api.mocker.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
    stream = make_oauth_tap().streams["Company"]
    engine = QueryEngine(company_rows(), {"ModifiedDate"})
    api.mocker.post(
        query_url("Company"),
        [failure, {"json": lambda request, context: engine.respond(request.json())}],
    )
    time.sleep(AGED)
    records = list(stream.get_records(None))
    assert [r["Gsid"] for r in records] == ["1P02C0001"]
    assert [r.headers["Authorization"] for r in company_queries(api)][:2] == ["Bearer token-1", "Bearer token-2"]
    assert len(token_calls(api.mocker)) == 2


def test_a_second_401_on_a_stream_is_not_retried(api):
    api.mocker.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
    stream = make_oauth_tap().streams["Company"]
    api.mocker.post(query_url("Company"), status_code=401, json={})
    time.sleep(AGED)
    with pytest.raises(FatalAPIError, match="rejected the OAuth access token. Check `client_id`"):
        list(stream.get_records(None))
    assert len(company_queries(api)) == 2
    assert len(token_calls(api.mocker)) == 2


def test_a_401_on_a_fresh_token_fails_a_stream_fast_with_no_token_fetch(api):
    api.mocker.post(TOKEN_URL, [{"json": token_response("token-1")}, {"json": token_response("token-2")}])
    stream = make_oauth_tap().streams["Company"]
    api.mocker.post(query_url("Company"), status_code=401, json={})
    with pytest.raises(FatalAPIError, match="rejected the OAuth access token"):
        list(stream.get_records(None))
    assert len(company_queries(api)) == 1
    assert len(token_calls(api.mocker)) == 1


def test_alternating_401_and_429_on_one_page_fetch_at_most_one_token(api, sleeps):
    api.mocker.post(TOKEN_URL, [{"json": token_response(f"token-{i}")} for i in range(1, 10)])
    stream = make_oauth_tap().streams["Company"]
    api.mocker.post(
        query_url("Company"), [{"status_code": 401, "json": {}}, {"status_code": 429, "text": "slow"}] * 10
    )
    time.sleep(AGED)
    with pytest.raises(FatalAPIError):
        list(stream.get_records(None))
    # Backoff sleeps age each new token, and still no second one is fetched.
    assert len(company_queries(api)) >= 3
    assert len(token_calls(api.mocker)) == 2


def test_a_stream_sends_the_current_token_after_a_refresh(api):
    api.mocker.post(TOKEN_URL, [{"json": token_response("token-1", 600)}, {"json": token_response("token-2")}])
    tap = make_oauth_tap()
    api.serve(query_url("Company"), QueryEngine(company_rows(), {"ModifiedDate"}))
    time.sleep(400)  # The discovery token is now inside the refresh margin.
    list(tap.streams["Company"].get_records(None))
    assert {r.headers["Authorization"] for r in company_queries(api)} == {"Bearer token-2"}


# 3a. A failed token request is never a failure of one object.


def test_a_describe_401_with_a_failing_token_endpoint_fails_discovery(api):
    api.mocker.post(TOKEN_URL, [{"json": token_response("token-1")}, {"status_code": 401, "json": {}}])

    def describe(request, context):
        time.sleep(AGED)  # The token ages, so the 401 earns a refresh.
        context.status_code = 401
        return {}

    api.mocker.post(f"{BASE_URL}/v1/meta/services/objects/describe", json=describe)
    with pytest.raises(GainsightTokenError, match="did not accept the OAuth API Key or Secret"):
        make_oauth_tap()
    assert len(token_calls(api.mocker)) == 2


def test_a_failing_token_endpoint_fails_the_dropdown_loop(api, sleeps):
    from tap_gainsight.tap import TapGainsight as Tap

    api.mocker.post(TOKEN_URL, [{"json": token_response("token-1")}] + [{"status_code": 503, "text": "busy"}] * 20)

    def dropdown(request, context):
        time.sleep(AGED)
        context.status_code = 401
        return {}

    api.mocker.get(
        requests_mock_lib.ANY,
        json=dropdown,
        additional_matcher=lambda r: "/v1/meta/services/dropdowns/" in r.url,
    )
    with pytest.raises(GainsightTokenError, match="gave up"):
        Tap(config=dict(OAUTH_CONFIG), parse_env_config=False)


def test_a_failing_token_endpoint_fails_a_cli_discovery(api, tmp_path):
    api.mocker.post(TOKEN_URL, status_code=500, text="boom")
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(OAUTH_CONFIG))
    result = CliRunner(mix_stderr=False).invoke(TapGainsight.cli, ["--config", str(config_path), "--discover"])
    assert result.exit_code != 0
    assert isinstance(result.exception, GainsightTokenError)
    assert not result.stdout.strip()


# 3b. Wrapped errors carry no credential in their traceback.


def formatted(exc):
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def test_a_wrapped_metadata_error_traceback_has_no_secret():
    with requests_mock_lib.Mocker() as m:
        m.get(LIST_URL, exc=requests.exceptions.ConnectionError(f"down {ACCESS_KEY}"))
        with pytest.raises(GainsightAPIError) as info:
            GainsightMetadataClient(CONFIG, RateLimiter()).list_objects()
    text = formatted(info.value)
    assert "failed: down ***" in text
    assert ACCESS_KEY not in text


def test_a_wrapped_token_error_traceback_has_no_secret():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, exc=requests.exceptions.ConnectionError(f"down {CLIENT_SECRET} {BASIC}"))
        with pytest.raises(GainsightTokenError) as info:
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()
    text = formatted(info.value)
    for secret in (CLIENT_ID, CLIENT_SECRET, BASIC.split(" ")[1]):
        assert secret not in text


def test_a_retried_out_token_error_traceback_has_no_secret():
    secret = "SECRET_CODE_SHAPED_5"
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, status_code=503, json={"errorCode": secret})
        with pytest.raises(GainsightTokenError) as info:
            GainsightAuth({**OAUTH_CONFIG, "client_secret": secret}, RateLimiter()).token()
    assert secret not in formatted(info.value)


# 4. GS_APIG_2402, the IP allowlist refusal.


def test_ip_refusal_on_a_metadata_call_has_its_own_message():
    with requests_mock_lib.Mocker() as m:
        m.get(LIST_URL, status_code=403, json=IP_REFUSAL)
        with pytest.raises(GainsightAuthError) as info:
            GainsightMetadataClient(CONFIG, RateLimiter()).list_objects()
        assert m.call_count == 1
    text = str(info.value)
    assert client.IP_NOT_ALLOWED_MESSAGE in text
    assert "GS_APIG_2402" in text
    assert "whitelisted" not in text  # errorDesc is never quoted.


def test_ip_refusal_on_the_token_request_has_its_own_message():
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, status_code=403, json=IP_REFUSAL)
        with pytest.raises(GainsightTokenError, match="only allows listed IP addresses"):
            GainsightAuth(OAUTH_CONFIG, RateLimiter()).token()


def test_ip_refusal_on_a_stream_has_its_own_message(api):
    stream = make_tap().streams["Company"]
    api.mocker.post(query_url("Company"), status_code=403, json=IP_REFUSAL)
    with pytest.raises(FatalAPIError) as info:
        list(stream.get_records(None))
    assert client.IP_NOT_ALLOWED_MESSAGE in str(info.value)
    assert len([r for r in api.mocker.request_history if r.url == query_url("Company")]) == 1


def test_the_2401_code_is_still_the_auth_failure():
    assert client.KNOWN_ERRORS["GS_APIG_2401"] == "Gainsight rejected the credential"
    assert client.is_unauthorized_payload(load("unauthorized_response.json"))
    assert not client.is_unauthorized_payload(IP_REFUSAL)


# 5. Redaction.


def test_request_secrets_covers_every_credential_form():
    request = requests.Request("POST", TOKEN_URL, headers={"Authorization": BASIC}).prepare()
    found = request_secrets(request)
    assert BASIC in found and BASIC.split(" ")[1] in found
    assert CLIENT_ID in found and CLIENT_SECRET in found
    bearer = requests.Request("POST", LIST_URL, headers={"Authorization": "Bearer TOKEN_X1"}).prepare()
    assert request_secrets(bearer) == ["Bearer TOKEN_X1", "TOKEN_X1"]
    broken = requests.Request("POST", TOKEN_URL, headers={"Authorization": "Basic %%%"}).prepare()
    assert request_secrets(broken) == ["Basic %%%", "%%%"]


def fake_response(request_headers, payload, status=400):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(payload).encode()
    response.request = requests.Request("POST", LIST_URL, headers=request_headers).prepare()
    return response


def test_an_echoed_bearer_token_is_redacted():
    # A code-shaped token passes the code check, so only redaction hides it.
    response = fake_response({"Authorization": "Bearer TOKEN_SECRET_1"}, {"errorCode": "TOKEN_SECRET_1"})
    text = response_summary(response)
    assert "TOKEN_SECRET_1" not in text and "errorCode ***" in text


def test_an_echoed_client_secret_is_redacted_even_on_a_bearer_request():
    response = fake_response({"Authorization": "Bearer t"}, {"errorCode": "CLIENT_SECRET_CODE"})
    assert "CLIENT_SECRET_CODE" not in response_summary(response, ["CLIENT_SECRET_CODE"])


def test_an_echoed_client_secret_on_the_token_request_is_redacted():
    secret = "SECRET_CODE_SHAPED_9"
    config = {**OAUTH_CONFIG, "client_secret": secret}
    with requests_mock_lib.Mocker() as m:
        m.post(TOKEN_URL, status_code=401, json={"errorCode": secret, "title": secret})
        with pytest.raises(GainsightTokenError) as info:
            GainsightAuth(config, RateLimiter()).token()
    assert secret not in str(info.value)
    assert "errorCode ***" in str(info.value)


def test_an_echoed_token_on_a_stream_error_is_redacted(api):
    token = "TOKEN_CODE_SHAPED_7"
    api.mocker.post(TOKEN_URL, json=token_response(token))
    stream = make_oauth_tap().streams["Company"]
    api.mocker.post(query_url("Company"), status_code=400, json={"result": False, "errorCode": token})
    with pytest.raises(FatalAPIError) as info:
        list(stream.get_records(None))
    assert token not in str(info.value)


# 6. End to end under OAuth.


def test_discovery_under_oauth_end_to_end(api, tmp_path, caplog):
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG)
    api.mocker.post(TOKEN_URL, json=token_response("token-e2e"))
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(OAUTH_CONFIG))
    result = CliRunner(mix_stderr=False).invoke(
        TapGainsight.cli, ["--config", str(config_path), "--discover"], catch_exceptions=False
    )
    assert result.exit_code == 0, result.stderr
    streams = {entry["tap_stream_id"] for entry in json.loads(result.stdout)["streams"]}
    assert {"Company", "GsUser", "Company_Person", "timeline", "cta"} <= streams
    # One token for the whole run, and a Bearer token on every other call.
    assert len(token_calls(api.mocker)) == 1
    calls = data_calls(api.mocker)
    assert calls
    for request in calls:
        assert request.headers["Authorization"] == "Bearer token-e2e"
        assert "accesskey" not in {name.lower() for name in request.headers}
    for text in (result.stdout, result.stderr, caplog.text):
        for secret in (CLIENT_ID, CLIENT_SECRET, BASIC.split(" ")[1], "token-e2e"):
            assert secret not in text
    # Nothing is written to disk but the config the test wrote.
    assert [p.name for p in tmp_path.iterdir()] == ["config.json"]
