"""HTTP layer for the Gainsight CS API: rate limiter, metadata client, base stream.

Every endpoint, header and body field used here traces to a Gainsight doc page.
The README has the full table. Doc links used in this module:

- Data Management APIs (describe, object list, dropdowns, delete log):
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs
- Company API (query shape, "No data found" failure, rate limits):
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Company_API_Documentation
- Custom Object API (query shape, empty result, epoch dates):
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Custom_Object_API/Gainsight_Custom_Object_API_Documentation
- Generate REST API Key (M2M OAuth token request and response):
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Generate_REST_API/Generate_REST_API_Key
"""

from __future__ import annotations

import base64
import collections
import datetime
import http.cookiejar
import json
import logging
import math
import re
import threading
import time
import typing as t
from urllib.parse import urlparse

import backoff
import requests

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # Python before 3.9.
    from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # type: ignore
from singer_sdk import metrics
from singer_sdk.exceptions import ConfigValidationError, FatalAPIError
from singer_sdk.streams import RESTStream

from tap_gainsight import safety

DEFAULT_HOST_SUFFIX = ".gainsightcloud.com"
MAX_TRIES = 8
MAX_WAIT_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 300

# Docs: "Synchronous API Calls: 100 API calls per min", a fixed window.
# Source: every API page above, section "Throttling Limits".
RATE_LIMIT_CALLS = 100
# The tap's default is lower, to share the tenant's allowance with other
# integrations.
DEFAULT_REQUESTS_PER_MINUTE = 30
RATE_LIMIT_PERIOD_SECONDS = 60.0

# Docs, Data Management APIs, "Get API - categoryID", Sample Failure Response.
UNAUTHORIZED_ERROR_CODE = "GS_APIG_2401"
# Seen from a live tenant: "IP Address does not lie in range of whitelisted
# ips". Access-key connections must list caller IPs. M2M OAuth need not.
IP_NOT_ALLOWED_ERROR_CODE = "GS_APIG_2402"
IP_NOT_ALLOWED_MESSAGE = (
    "Gainsight refused the request because this connection only allows listed "
    "IP addresses. Use M2M OAuth credentials (OAuth API Key and Secret), which "
    "have no IP requirement, or ask your Gainsight admin to allow the caller's "
    "IP address."
)
# Generate REST API Key, "Get Access Token API", Sample Success Response:
# "expires_in": 86400. The tap uses it when a response leaves it out.
DEFAULT_TOKEN_LIFETIME_SECONDS = 86400
# A token this close to expiry is replaced before the next call. A short
# lifetime uses half of it instead, so a token is never stale on arrival.
TOKEN_REFRESH_MARGIN_SECONDS = 300
# After an auth failure, a token younger than this is not replaced. A
# fresh token that fails will fail again, so the run stops instead.
TOKEN_MIN_AGE_FOR_REFRESH_SECONDS = 10
# Documented empty replies. Company API, "Read API", Sample Failure
# Response: "No data found for given criteria". Error Codes article:
# GSOBJ_1011, "No entity matches the given criteria", HTTP 400.
NO_DATA_ERROR_DESCS = (
    "no data found for given criteria",
    "no entity matches the given criteria",
)
NO_DATA_ERROR_CODES = {"GSOBJ_1011"}
# Error Codes article: GSOBJ_1024, "Invalid authorization headers", HTTP 400.
AUTH_ERROR_CODES = {UNAUTHORIZED_ERROR_CODE, "GSOBJ_1024"}
# Data Management APIs, "Get Describe OMD", Sample Failure Response.
OBJECT_NOT_FOUND_TITLE = "OBJECT_NOT_FOUND"

# The docs do not say which time zone a filter value without an offset is
# read in. Every incremental filter starts this far before its bookmark, so
# no tenant time zone can skip a row. Targets dedupe the overlap by key.
FILTER_LOOKBACK = datetime.timedelta(hours=24)
# Data Management APIs and Retrieve Deleted Data API: "Deleted data is
# available for retrieval via REST API only for 15 days after deletion."
DELETE_RETENTION = datetime.timedelta(days=15)
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
# Hotglue's field-sample job sends a record limit per stream in this setting.
# Hotglue's own SDK reads it. The Meltano SDK doesn't, so the tap applies it.
RECORD_LIMITS_SETTING = "_hg_max_records_limit"

# Gainsight data types with a known JSON shape. The docs name STRING, GSID,
# DATETIME and LOOKUP as describe `dataType` values. The rest come from the
# operator table in the Custom Object API page. Any other type is sent as
# text, because the docs do not show its value shape.
STRING_DATA_TYPES = {"STRING", "GSID", "EMAIL", "URL", "LOOKUP", "RICHTEXTAREA"}
NUMBER_DATA_TYPES = {"NUMBER", "PERCENTAGE", "CURRENCY"}
BOOLEAN_DATA_TYPES = {"BOOLEAN"}
DATETIME_DATA_TYPES = {"DATETIME"}
DATE_DATA_TYPES = {"DATE"}
KNOWN_DATA_TYPES = (
    STRING_DATA_TYPES
    | NUMBER_DATA_TYPES
    | BOOLEAN_DATA_TYPES
    | DATETIME_DATA_TYPES
    | DATE_DATA_TYPES
)


class GainsightAPIError(Exception):
    """Raised when a Gainsight metadata call fails."""

    def __init__(self, message: str, status_code: t.Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class GainsightAuthError(GainsightAPIError):
    """Raised when Gainsight rejects the access key."""


class GainsightRedirectError(GainsightAuthError):
    """Raised on a 3xx. The access key is never sent to another host."""


class GainsightTokenError(GainsightAPIError):
    """Raised when the M2M OAuth token request fails, for any reason.

    Discovery never treats it as a failure of one object.
    """


class _RetriableError(GainsightAPIError):
    """A call failed with a status that is safe to retry."""


class RateLimiter:
    """Client-side fixed-count limiter: at most `calls` requests per `period`.

    One instance is shared by every stream and metadata call in a tap run.
    It keeps the send times of the last `calls` requests and waits until the
    oldest one leaves the window.
    """

    def __init__(
        self,
        calls: int = RATE_LIMIT_CALLS,
        period: float = RATE_LIMIT_PERIOD_SECONDS,
        clock: t.Optional[t.Callable[[], float]] = None,
        sleep: t.Optional[t.Callable[[float], None]] = None,
    ) -> None:
        if calls < 1 or period <= 0:
            raise ValueError("A rate limit needs calls >= 1 and period > 0.")
        self.calls = calls
        self.period = period
        self._clock = clock or time.monotonic
        self._sleep = sleep or (lambda seconds: time.sleep(seconds))
        self._sent: t.Deque[float] = collections.deque()
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Block until a request may be sent. Return the seconds waited."""
        waited = 0.0
        with self._lock:
            while True:
                now = self._clock()
                while self._sent and now - self._sent[0] >= self.period:
                    self._sent.popleft()
                if len(self._sent) < self.calls:
                    self._sent.append(now)
                    return waited
                delay = self.period - (now - self._sent[0])
                self._sleep(delay)
                waited += delay
                # The oldest send, and any sent at the same moment, are now a
                # full period old, so drop them here. Comparing clock values
                # again can loop forever: float rounding can leave them a hair
                # under the period, and a sleep of that hair may not move the
                # clock at all.
                oldest = self._sent[0]
                while self._sent and self._sent[0] <= oldest:
                    self._sent.popleft()


_LABEL = r"[a-z0-9-]+"
GAINSIGHT_HOST = re.compile(rf"^{_LABEL}(\.{_LABEL})*\.gainsightcloud\.com$")
CUSTOM_HOST = re.compile(rf"^{_LABEL}(\.{_LABEL})*\.[a-z][a-z0-9-]*$")
_BARE_HOST_CHARS = re.compile(r"^[A-Za-z0-9.-]+$")
# Whitespace a paste can carry around the address. Steerco's connection
# check strips the same set, so both read a pasted address the same way.
_PASTE_WHITESPACE = " \t\n\r\f\v\u00a0"


def pinned_host(domain: str, custom_domain: t.Optional[str] = None) -> str:
    """Return the one host the tap may call. Raise ValueError otherwise.

    `domain` is a bare host, optionally after `https://`. Whitespace around
    it and slashes after it are dropped, as a pasted address carries them.
    A single label, such as `acme`, becomes `acme.gainsightcloud.com`. The
    host must be a subdomain of gainsightcloud.com, or equal `custom_domain`,
    so a custom host is a deliberate, double-entered choice. User
    information, ports, paths, queries, fragments, braces and inner
    whitespace are refused.
    """
    raw = domain.strip(_PASTE_WHITESPACE) if isinstance(domain, str) else ""
    host = raw[len("https://"):] if raw.lower().startswith("https://") else raw
    host = host.rstrip("/")
    if not host or not _BARE_HOST_CHARS.fullmatch(host):
        raise ValueError(
            f"The `domain` setting {domain!r} is not a bare host. Use a host such "
            "as acme.gainsightcloud.com, optionally after https://, with no "
            "port, path, user information or spaces."
        )
    host = host.lower()
    if "." not in host:
        host = f"{host}{DEFAULT_HOST_SUFFIX}"
    if GAINSIGHT_HOST.fullmatch(host):
        return host
    if custom_domain is not None:
        custom = str(custom_domain).lower()
        if custom != host:
            raise ValueError(
                f"The `custom_domain` setting {custom_domain!r} must equal the "
                f"`domain` host {host!r}."
            )
        if not CUSTOM_HOST.fullmatch(host):
            raise ValueError(f"The custom domain {host!r} is not a host name.")
        return host
    raise ValueError(
        f"The `domain` host {host!r} is not under gainsightcloud.com. For a "
        "custom Gainsight domain, also set `custom_domain` to the same host."
    )


def normalize_domain(domain: str, custom_domain: t.Optional[str] = None) -> str:
    """Return `https://<pinned host>`. See pinned_host."""
    return f"https://{pinned_host(domain, custom_domain)}"


# Fixed descriptions for the error codes the tap handles. errorDesc is
# never logged: Gainsight's templates put record values in it, as in
# GSOBJ_1005 "Invalid dateTime format (%s(columnName)= %s(columnValue))".
KNOWN_ERRORS = {
    "GSOBJ_1011": "no rows match the criteria",
    "GSOBJ_1005": "a date or date-time value has an invalid format",
    "GSOBJ_1023": "the API path is invalid",
    "GSOBJ_1024": "the authorization headers are invalid",
    "GS_APIG_2401": "Gainsight rejected the credential",
    "GS_APIG_2402": "the caller's IP address is not on the connection's allowlist",
    "COCKPIT_5101": "the CTA request is invalid",
    "OBJECT_NOT_FOUND": "the object was not found",
}
# A code is capitals, digits and underscores, up to 50 characters.
_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,49}")


def _code_field(name: str, value: t.Any) -> str:
    """Describe an errorCode or title without echoing anything else."""
    if not isinstance(value, str):
        return f"{name} of type {type(value).__name__}"
    if not _CODE.fullmatch(value):
        return f"{name} that is not a code ({len(value)} characters)"
    known = KNOWN_ERRORS.get(value)
    return f"{name} {value} ({known})" if known else f"{name} {value}"


def request_secrets(request: t.Any) -> t.List[str]:
    """Return the credential values in a request's headers.

    That is the AccessKey value, the whole Authorization value, the part
    after its scheme, and for Basic, the decoded client ID and secret.
    """
    headers = getattr(request, "headers", None) or {}
    found = [headers.get("AccessKey") or ""]
    authorization = headers.get("Authorization") or ""
    found.append(authorization)
    scheme, _, credential = authorization.partition(" ")
    found.append(credential)
    if scheme == "Basic":
        try:
            pair = base64.b64decode(credential, validate=True).decode("utf-8")
        except ValueError:
            pair = ""
        found += [pair, *pair.split(":", 1)]
    return [value for value in found if value]


def redact(text: str, secrets: t.Iterable[str]) -> str:
    """Replace each secret in `text` with ***, longest first."""
    for secret in sorted({s for s in secrets if s}, key=len, reverse=True):
        text = text.replace(secret, "***")
    return text


def response_summary(response: requests.Response, secrets: t.Iterable[str] = ()) -> str:
    """Describe an error response without its data.

    For the documented error shape: the status, and `errorCode` and `title`
    when each is a code of up to 50 capitals, digits and underscores. A
    known code adds the tap's own fixed description. Any other value gives
    only its type or length. `errorDesc` is never included. For any other
    body: the status and the body length. The request's credentials and
    `secrets` are redacted.
    """
    payload = json_or_none(response)
    parts = [f"HTTP {response.status_code}"]
    if isinstance(payload, dict) and ({"errorCode", "errorDesc", "title"} & set(payload)):
        for name in ("errorCode", "title"):
            if payload.get(name) is not None:
                parts.append(_code_field(name, payload[name]))
    else:
        parts.append(f"a {len(response.content or b'')}-byte body")
    text = ", ".join(parts)
    # A code-shaped credential could pass the code check. Redact it anyway.
    request = getattr(response, "request", None)
    return redact(text, [*request_secrets(request), *secrets])


def refuse_cookies(session: requests.Session) -> None:
    """Never store or send a cookie, such as a load balancer's AWSALB."""
    session.cookies.set_policy(http.cookiejar.DefaultCookiePolicy(allowed_domains=[]))


def json_or_none(response: requests.Response) -> t.Any:
    """Return the parsed body, or None when the body is not JSON."""
    try:
        return response.json()
    except ValueError:
        return None


def _code(payload: dict) -> t.Optional[str]:
    """Return errorCode when it is a string. A list or object is not a code."""
    code = payload.get("errorCode")
    return code if isinstance(code, str) else None


def is_no_data_response(payload: t.Any) -> bool:
    """Return True for a documented `result: false` empty reply.

    Matches both documented wordings and the GSOBJ_1011 code.
    """
    if not isinstance(payload, dict) or payload.get("result") is not False:
        return False
    if _code(payload) in NO_DATA_ERROR_CODES:
        return True
    desc = str(payload.get("errorDesc") or "").lower()
    return any(text in desc for text in NO_DATA_ERROR_DESCS)


def is_object_not_found(payload: t.Any) -> bool:
    """Return True for the documented "Requested object not found." reply."""
    if not isinstance(payload, dict) or payload.get("result") is not False:
        return False
    text = f"{payload.get('errorDesc') or ''} {payload.get('detail') or ''}".lower()
    return payload.get("title") == OBJECT_NOT_FOUND_TITLE or "object not found" in text


def is_unauthorized_payload(payload: t.Any) -> bool:
    """Return True when a body carries a documented authorization error code."""
    return isinstance(payload, dict) and _code(payload) in AUTH_ERROR_CODES


def is_ip_not_allowed_payload(payload: t.Any) -> bool:
    """Return True when a body carries GS_APIG_2402, the IP allowlist refusal."""
    return isinstance(payload, dict) and _code(payload) == IP_NOT_ALLOWED_ERROR_CODE


def redirect_message(response: requests.Response, label: str) -> str:
    """Describe a refused redirect, naming the host it pointed to."""
    location = response.headers.get("Location") or ""
    host = urlparse(location).netloc or location or "an unknown host"
    return (
        f"{response.status_code} redirect from {label} to {host}. The tap does "
        "not follow redirects, so no credential goes to another host. "
        "Check the `domain` setting."
    )


def extract_rows(payload: t.Any) -> list:
    """Return the records from a Gainsight query response.

    The docs show two success shapes. The Company, Custom Object and CTA
    pages show `data` as a list. The Timeline and delete log pages show
    `data.records`. Both are accepted, and so is the documented "No data
    found" failure. Any other shape raises, so a changed contract fails
    loudly.
    """
    if not isinstance(payload, dict):
        raise FatalAPIError(f"Expected a JSON object, got {type(payload).__name__}.")
    if is_no_data_response(payload):
        return []
    data = payload.get("data")
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "records" in data:
        records = data["records"]
        if records is None:
            return []
        if isinstance(records, list):
            return records
    if isinstance(data, dict):
        shape = ", ".join(f"{key}: {type(value).__name__}" for key, value in data.items())
        detail = f"an object with keys {{{shape}}}"
    else:
        detail = f"a {type(data).__name__}"
    raise FatalAPIError(f"Unexpected `data` shape in the response: {detail}.")


def to_iso_datetime(value: t.Any) -> t.Any:
    """Convert an epoch-milliseconds value to an ISO 8601 UTC string.

    Docs, Custom Object API: "Date and DateTime values are shown in epoch
    format." Strings, such as CTA dates, pass through unchanged.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value
    return (EPOCH + datetime.timedelta(milliseconds=value)).isoformat()


_DATE_TEXT = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})"
    r"(?:[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?)?"
    r"(Z|[+-]\d{2}:?\d{2})?$"
)


def parse_api_datetime(value: t.Any) -> t.Optional[datetime.datetime]:
    """Parse an API date value to an aware UTC datetime.

    Accepts epoch milliseconds, as the query API returns, and the ISO forms
    the CTA and delete log APIs return. A value with no offset is UTC.
    Returns None for None. Raises ValueError for any other value.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return EPOCH + datetime.timedelta(milliseconds=value)
    match = _DATE_TEXT.fullmatch(str(value).strip())
    if not match:
        raise ValueError(f"Unrecognized date value: {value!r}")
    year, month, day, hour, minute, second, fraction, offset = match.groups()
    moment = datetime.datetime(
        int(year),
        int(month),
        int(day),
        int(hour or 0),
        int(minute or 0),
        int(second or 0),
        int((fraction or "0").ljust(6, "0")),
        tzinfo=datetime.timezone.utc,
    )
    if offset and offset != "Z":
        sign = 1 if offset[0] == "+" else -1
        digits = offset[1:].replace(":", "")
        moment -= sign * datetime.timedelta(
            hours=int(digits[:2]), minutes=int(digits[2:])
        )
    return moment


def to_epoch_ms(value: t.Any) -> t.Optional[int]:
    """Return a date value as whole epoch milliseconds, for exact compares."""
    moment = parse_api_datetime(value)
    if moment is None:
        return None
    return (moment - EPOCH) // datetime.timedelta(milliseconds=1)


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def as_utc(value: datetime.datetime) -> datetime.datetime:
    """Return a plain, timezone-aware UTC datetime.

    The SDK hands over pendulum datetimes. They are rebuilt as standard
    datetimes first, field by field, so no float rounding can move a second.
    """
    plain = datetime.datetime(
        value.year,
        value.month,
        value.day,
        value.hour,
        value.minute,
        value.second,
        value.microsecond,
        tzinfo=value.tzinfo or datetime.timezone.utc,
    )
    return plain.astimezone(datetime.timezone.utc)


def record_limits(config: t.Mapping[str, t.Any]) -> t.Dict[str, int]:
    """Return the record limit for each stream named in RECORD_LIMITS_SETTING.

    Raises ValueError on a bad value, so it never turns into a full read.
    """
    raw = config.get(RECORD_LIMITS_SETTING)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{RECORD_LIMITS_SETTING} must be an object, got {type(raw).__name__}.")
    limits: t.Dict[str, int] = {}
    for name, limit in raw.items():
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError(
                f"{RECORD_LIMITS_SETTING} for stream {name!r} must be a whole "
                f"number of at least 1, got {limit!r}."
            )
        limits[str(name)] = limit
    return limits


def load_zone(name: t.Optional[str]) -> t.Optional[datetime.tzinfo]:
    """Return the IANA zone `name`, or None for UTC. Raises ValueError."""
    if not name:
        return None
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(
            f"Unknown filter_timezone {name!r}. Use an IANA name, such as "
            "America/Los_Angeles."
        ) from exc


def format_query_datetime(
    value: datetime.datetime, zone: t.Optional[datetime.tzinfo] = None
) -> str:
    """Format a DATETIME filter value for the query API.

    Uses `yyyy-MM-dd HH:mm:ss`, the form in the delete log sample request:
    `"value": ["2024-02-05 00:00:00"]`. The value is in UTC, or in `zone`
    when the `filter_timezone` setting names one. The zone's rules apply,
    so daylight saving time is handled.
    """
    moment = as_utc(value)
    if zone is not None:
        moment = moment.astimezone(zone)
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def json_schema_for(data_type: t.Optional[str]) -> dict:
    """Map a Gainsight data type to a nullable JSON Schema."""
    kind = (data_type or "").upper()
    if kind in STRING_DATA_TYPES or kind in DATE_DATA_TYPES:
        return {"type": ["null", "string"]}
    if kind in DATETIME_DATA_TYPES:
        return {"type": ["null", "string"], "format": "date-time"}
    if kind in NUMBER_DATA_TYPES:
        return {"type": ["null", "number"]}
    if kind in BOOLEAN_DATA_TYPES:
        return {"type": ["null", "boolean"]}
    return text_schema()


def text_schema() -> dict:
    """Return the schema of a field sent as text: a nullable string.

    A field of unknown type is text, never a list of JSON types. The Meltano
    SDK turns every value of a field whose types include "boolean" into
    `value != 0`, so "Active" arrives as True. Hotglue's parquet target then
    makes a boolean column but writes str(value), and the sync fails.
    """
    return {"type": ["null", "string"]}


def is_known_type(data_type: t.Optional[str]) -> bool:
    """Return True for a Gainsight data type with a known JSON shape."""
    return (data_type or "").upper() in KNOWN_DATA_TYPES


def to_text(value: t.Any) -> t.Optional[str]:
    """Return a text field's value as a string, or None.

    Anything that is not a string is JSON-encoded, so a multi-select list
    stays readable: ["a", "b"] becomes '["a", "b"]'.
    """
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def is_date_type(data_type: t.Optional[str]) -> bool:
    """Return True for Gainsight DATE and DATETIME fields."""
    kind = (data_type or "").upper()
    return kind in DATE_DATA_TYPES or kind in DATETIME_DATA_TYPES


def _has_control_characters(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)


def _is_set(config: t.Mapping[str, t.Any], name: str) -> bool:
    """Return True when a setting has a value. An empty string is unset."""
    value = config.get(name)
    return value is not None and value != ""


def _check_credential(config: t.Mapping[str, t.Any], name: str) -> None:
    """Raise ValueError unless a credential can go in a header as is.

    The message names the setting and never includes its value.
    """
    value = config.get(name)
    if not isinstance(value, str) or value != value.strip() or _has_control_characters(value):
        raise ValueError(
            f"The `{name}` setting must be text with no leading or trailing "
            "whitespace and no control characters."
        )


class AuthChoice(t.NamedTuple):
    method: str  # "access_key" or "oauth".
    # A log line about settings the choice ignores: (level, message).
    notice: t.Optional[t.Tuple[int, str]] = None


def choose_auth(config: t.Mapping[str, t.Any]) -> AuthChoice:
    """Choose the credential method. Raise ValueError when none is usable.

    Both `client_id` and `client_secret` select M2M OAuth, and a stored
    `access_key` is then ignored. A settings form may keep an old secret it
    cannot clear, so the pair wins instead of failing. Otherwise
    `access_key` is used. Only the credentials in use are checked. No
    message includes a value.
    """
    has_key = _is_set(config, "access_key")
    has_id = _is_set(config, "client_id")
    has_secret = _is_set(config, "client_secret")
    if has_id and has_secret:
        _check_credential(config, "client_id")
        _check_credential(config, "client_secret")
        notice = None
        if has_key:
            notice = (
                logging.INFO,
                "Using M2M OAuth from `client_id` and `client_secret`. The "
                "stored `access_key` is ignored.",
            )
        return AuthChoice("oauth", notice)
    if has_key:
        _check_credential(config, "access_key")
        notice = None
        if has_id or has_secret:
            missing = "client_secret" if has_id else "client_id"
            notice = (
                logging.WARNING,
                f"M2M OAuth needs `{missing}`, which is not set. Using "
                "`access_key` instead.",
            )
        return AuthChoice("access_key", notice)
    if has_id or has_secret:
        raise ValueError(
            "M2M OAuth needs both `client_id` (the OAuth API Key) and "
            "`client_secret` (the OAuth API Secret). One of them is missing."
        )
    raise ValueError(
        "No credentials are set. Set `access_key`, or `client_id` and "
        "`client_secret` for M2M OAuth."
    )


def auth_method(config: t.Mapping[str, t.Any]) -> str:
    """Return "access_key" or "oauth". See choose_auth."""
    return choose_auth(config).method


class GainsightAuth:
    """Supplies the credential header for every API call in a tap run.

    With `access_key`, it is the AccessKey header. With M2M OAuth, it is a
    Bearer token from the token request. The token is fetched before the
    first call and kept in memory only. It is fetched again near expiry,
    and after an auth failure when `refresh_after_failure` allows it. One
    instance is shared by every stream and metadata call.
    """

    def __init__(
        self,
        config: t.Mapping[str, t.Any],
        rate_limiter: RateLimiter,
        budget: t.Optional[safety.RequestBudget] = None,
        session: t.Optional[requests.Session] = None,
    ) -> None:
        self.host = pinned_host(config["domain"], config.get("custom_domain"))
        choice = choose_auth(config)
        self.method = choice.method
        self.notice = choice.notice
        self.rate_limiter = rate_limiter
        self.budget = budget
        self._access_key = str(config.get("access_key") or "")
        self._client_id = str(config.get("client_id") or "")
        self._client_secret = str(config.get("client_secret") or "")
        self._token: t.Optional[str] = None
        self._fetched_at = 0.0
        self._expires_at = 0.0
        self._margin = float(TOKEN_REFRESH_MARGIN_SECONDS)
        self._lock = threading.Lock()
        self._session = session or requests.Session()
        self._session.trust_env = False
        refuse_cookies(self._session)

    @property
    def is_oauth(self) -> bool:
        return self.method == "oauth"

    @property
    def credential(self) -> str:
        """Names what Gainsight rejected on an auth failure."""
        return "the OAuth access token" if self.is_oauth else "the access key"

    @property
    def settings_hint(self) -> str:
        """Names the settings to check on an auth failure."""
        if self.is_oauth:
            return "`client_id`, `client_secret` and `domain`"
        return "`access_key` and `domain`"

    def headers(self) -> t.Dict[str, str]:
        """Return the credential header for the next API call."""
        if not self.is_oauth:
            # Docs: pass the access key in the "accesskey" header.
            return {"AccessKey": self._access_key}
        return {"Authorization": f"Bearer {self.token()}"}

    def token(self) -> str:
        """Return a token with more than the refresh margin left."""
        with self._lock:
            now = time.monotonic()
            if self._token is None or now >= self._expires_at - self._margin:
                self._fetch()
            assert self._token is not None
            return self._token

    @staticmethod
    def token_in(headers: t.Mapping[str, str]) -> t.Optional[str]:
        """Return the Bearer token a request carried, or None."""
        scheme, _, token = (headers.get("Authorization") or "").partition(" ")
        return token if scheme == "Bearer" and token else None

    def refresh_after_failure(self, failed_token: t.Optional[str]) -> bool:
        """Fetch a new token after an auth failure. Return True if it did.

        Only when the token that failed is still the current one, and was
        fetched more than TOKEN_MIN_AGE_FOR_REFRESH_SECONDS ago. Otherwise
        the caller raises the auth error at once. This keeps a run from
        fetching a token per page or per retry.
        """
        with self._lock:
            if failed_token is None or failed_token != self._token:
                return False
            if time.monotonic() - self._fetched_at <= TOKEN_MIN_AGE_FOR_REFRESH_SECONDS:
                return False
            self._fetch()
            return True

    def failed_auth(self, response: requests.Response) -> bool:
        """Return True when an OAuth call failed on its credential.

        That is HTTP 401, or a documented auth error code at any status.
        """
        return self.is_oauth and (
            response.status_code == 401 or is_unauthorized_payload(json_or_none(response))
        )

    def secrets(self) -> t.List[str]:
        """Every credential value, for redaction."""
        return [
            value
            for value in (self._access_key, self._client_id, self._client_secret, self._token)
            if value
        ]

    def _fetch(self) -> None:
        label = "the OAuth token request"
        basic = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode("utf-8")).decode("ascii")
        # Docs, Get Access Token API: POST to the token path with the header
        # "Authorization: Basic base64(client_id:client_secret)".
        request = requests.Request(
            "POST",
            f"https://{self.host}{safety.TOKEN_PATH}",
            data=dict(safety.TOKEN_BODY),
            headers={"Authorization": f"Basic {basic}", "Accept": "application/json"},
        )

        @backoff.on_exception(
            lambda: backoff.expo(factor=2, max_value=MAX_WAIT_SECONDS),
            (
                _RetriableError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ),
            max_tries=MAX_TRIES,
        )
        def _call() -> requests.Response:
            response = safety.send(
                self._session,
                self._session.prepare_request(request),
                self.rate_limiter,
                self.budget,
                pinned_host=self.host,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            status = response.status_code
            if status == 429 or status >= 500:
                raise _RetriableError(
                    f"{status} from {label}: {response_summary(response, self.secrets())}", status
                )
            return response

        # `from None` everywhere below: a chained exception could carry a
        # credential in its message or in the traceback.
        try:
            response = _call()
        except _RetriableError as exc:
            raise GainsightTokenError(
                f"{exc} (gave up after {MAX_TRIES} tries)", exc.status_code
            ) from None
        except requests.exceptions.RequestException as exc:
            raise GainsightTokenError(
                redact(f"{label} failed: {exc}", [*self.secrets(), basic])
            ) from None

        status = response.status_code
        summary = response_summary(response, self.secrets())
        if 300 <= status < 400:
            raise GainsightTokenError(redirect_message(response, label), status)
        payload = json_or_none(response)
        if is_ip_not_allowed_payload(payload):
            raise GainsightTokenError(f"{status}: {IP_NOT_ALLOWED_MESSAGE} Response: {summary}", status)
        if status == 400:
            raise GainsightTokenError(
                f"400: Gainsight refused the token request. Check the OAuth API Key and "
                f"Secret (`client_id` and `client_secret`) and `domain`. Response: {summary}",
                status,
            )
        if status in (401, 403) or is_unauthorized_payload(payload):
            raise GainsightTokenError(
                f"{status}: Gainsight did not accept the OAuth API Key or Secret. "
                f"Check `client_id`, `client_secret` and `domain`. Response: {summary}",
                status,
            )
        if status >= 400:
            raise GainsightTokenError(f"{status} from {label}: {summary}", status)
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not isinstance(token, str) or not safety.BEARER_TOKEN.fullmatch(token):
            raise GainsightTokenError(
                f"{label} returned no usable `access_token`. Response: {summary}", status
            )
        lifetime = payload.get("expires_in", DEFAULT_TOKEN_LIFETIME_SECONDS)
        if (
            isinstance(lifetime, bool)
            or not isinstance(lifetime, (int, float))
            or not math.isfinite(lifetime)
            or lifetime <= 0
        ):
            raise GainsightTokenError(
                f"{label} returned an `expires_in` that is not a finite positive "
                f"number of seconds. Response: {summary}",
                status,
            )
        now = time.monotonic()
        self._token = token
        self._fetched_at = now
        self._expires_at = now + lifetime
        self._margin = min(float(TOKEN_REFRESH_MARGIN_SECONDS), lifetime / 2)


class GainsightMetadataClient:
    """Calls the Data Management metadata APIs used at discovery time.

    Retries 429, 5xx, connection errors and timeouts with exponential
    backoff. Raises GainsightAuthError on 401, 403 or the documented
    `GS_APIG_2401` body, GainsightTokenError when an OAuth token request
    fails, and GainsightAPIError on any other failure.
    """

    # Docs, Data Management APIs, "Post Describe OMD", Sample Request. Only
    # `objectNames` changes. The other flags are the documented values.
    DESCRIBE_FLAGS = {
        "includeChilds": "true",
        "childLevels": "0",
        "populatePickListOptions": "true",
        "useCollectionId": "false",
        "removeDeleted": "false",
        "removeHidden": "false",
        "host": "MDA",
        "sortFieldsByLabel": "true",
        "populateFieldId": "false",
        "honorUserContext": "true",
        "honorCustomLookup": "true",
        "populateAutoSuggestDetails": "true",
        "externalContext": "false",
    }

    def __init__(
        self,
        config: t.Mapping[str, t.Any],
        rate_limiter: RateLimiter,
        session: t.Optional[requests.Session] = None,
        budget: t.Optional[safety.RequestBudget] = None,
        auth: t.Optional[GainsightAuth] = None,
    ) -> None:
        self.host = pinned_host(config["domain"], config.get("custom_domain"))
        self.base_url = f"https://{self.host}"
        self.rate_limiter = rate_limiter
        self.budget = budget
        self.auth = auth or GainsightAuth(config, rate_limiter, budget)
        self.session = session or requests.Session()
        # No .netrc or proxy settings: they could add an Authorization header.
        self.session.trust_env = False
        refuse_cookies(self.session)
        self.session.headers.update({"Accept": "application/json"})
        if not self.auth.is_oauth:
            # Docs: pass the access key in the "accesskey" header. Header
            # names are case-insensitive in HTTP.
            self.session.headers.update(self.auth.headers())

    def _send(
        self,
        method: str,
        path: str,
        label: str,
        params: t.Optional[dict] = None,
        body: t.Optional[dict] = None,
    ) -> dict:
        url = f"{self.base_url}{path}"

        refreshed = False

        def summary(response: requests.Response) -> str:
            return response_summary(response, self.auth.secrets())

        def _send_once() -> t.Tuple[requests.PreparedRequest, requests.Response]:
            headers = self.auth.headers() if self.auth.is_oauth else None
            prepared = self.session.prepare_request(
                requests.Request(method, url, params=params, json=body, headers=headers)
            )
            response = safety.send(
                self.session,
                prepared,
                self.rate_limiter,
                self.budget,
                pinned_host=self.host,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            return prepared, response

        @backoff.on_exception(
            lambda: backoff.expo(factor=2, max_value=MAX_WAIT_SECONDS),
            (
                _RetriableError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ),
            max_tries=MAX_TRIES,
        )
        def _call() -> requests.Response:
            nonlocal refreshed
            prepared, response = _send_once()
            # A token can lapse early. At most one new token and one retry
            # per call, across every backoff attempt.
            if not refreshed and self.auth.failed_auth(response):
                refreshed = True
                if self.auth.refresh_after_failure(self.auth.token_in(prepared.headers)):
                    _, response = _send_once()
            status = response.status_code
            if status == 429 or status >= 500:
                raise _RetriableError(f"{status} from {label}: {summary(response)}", status)
            return response

        # `from None`: a chained exception could carry a credential.
        try:
            response = _call()
        except _RetriableError as exc:
            raise GainsightAPIError(
                f"{exc} (gave up after {MAX_TRIES} tries)", exc.status_code
            ) from None
        except requests.exceptions.RequestException as exc:
            raise GainsightAPIError(redact(f"{label} failed: {exc}", self.auth.secrets())) from None

        status = response.status_code
        if 300 <= status < 400:
            raise GainsightRedirectError(redirect_message(response, label), status)
        payload = json_or_none(response)
        if is_ip_not_allowed_payload(payload):
            raise GainsightAuthError(
                f"{status}: {IP_NOT_ALLOWED_MESSAGE} Response: {summary(response)}", status
            )
        if status in (401, 403) or is_unauthorized_payload(payload):
            raise GainsightAuthError(
                f"{status}: Gainsight rejected {self.auth.credential} on {label}. Check "
                f"{self.auth.settings_hint}. Response: {summary(response)}",
                status,
            )
        if status >= 400:
            raise GainsightAPIError(f"{status} from {label}: {summary(response)}", status)
        if not isinstance(payload, dict):
            raise GainsightAPIError(
                f"{label} did not return a JSON object: {summary(response)}",
                status,
            )
        if payload.get("result") is False:
            raise GainsightAPIError(f"{label} failed: {summary(response)}", status)
        return payload

    def list_objects(self) -> t.List[dict]:
        """Return object summaries from "Get Lite API Call OMD".

        Endpoint and parameters are the documented sample:
        GET /v1/meta/services/objects/list?po=company&em=false
        """
        payload = self._send(
            "GET",
            "/v1/meta/services/objects/list",
            "the object list",
            params={"po": "company", "em": "false"},
        )
        data = payload.get("data")
        if not isinstance(data, list):
            raise GainsightAPIError(
                f"The object list returned no `data` list. Keys: {sorted(payload)}"
            )
        return [item for item in data if isinstance(item, dict)]

    def describe(self, object_names: t.Sequence[str]) -> t.Dict[str, dict]:
        """Return describe results keyed by lowercase object name.

        Uses "Post Describe OMD": POST /v1/meta/services/objects/describe.
        """
        body = {"objectNames": list(object_names), **self.DESCRIBE_FLAGS}
        payload = self._send(
            "POST",
            "/v1/meta/services/objects/describe",
            f"describe for {', '.join(object_names)}",
            body=body,
        )
        data = payload.get("data")
        if isinstance(data, dict):
            data = [data]
        described: t.Dict[str, dict] = {}
        for item in data or []:
            if isinstance(item, dict) and item.get("objectName"):
                described[str(item["objectName"]).lower()] = item
        return described

    def dropdown_items(self, category_id: str) -> t.Dict[str, str]:
        """Return {item GSID: item name} for one dropdown category.

        Uses "Get API - categoryID": GET /v1/meta/services/dropdowns/{categoryId}.
        """
        payload = self._send(
            "GET",
            f"/v1/meta/services/dropdowns/{category_id}",
            f"dropdown category {category_id}",
        )
        data = payload.get("data") or {}
        items = data.get("childItems") if isinstance(data, dict) else None
        labels: t.Dict[str, str] = {}
        for item in items or []:
            if isinstance(item, dict) and item.get("gsid"):
                labels[str(item["gsid"])] = item.get("name")
        return labels


class GainsightStream(RESTStream):
    """Base class for Gainsight CS streams."""

    rest_method = "POST"
    page_size = 5000

    # Fields whose epoch-millisecond values become ISO 8601 strings.
    date_fields: t.Set[str] = set()
    # Fields whose values become text. See to_text.
    text_fields: t.Set[str] = set()

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        # Discovery skips config validation, so a bad value raises here too.
        try:
            limit = record_limits(self.config).get(self.name)
        except ValueError as exc:
            raise ConfigValidationError(str(exc)) from exc
        if limit is not None:
            self.ABORT_AT_RECORD_COUNT = limit
            # A page never needs more rows than the limit.
            self.page_size = min(self.page_size, limit)

    @property
    def is_limited(self) -> bool:
        """True when a record limit is set, as in a field-sample job or a dry run."""
        return self.ABORT_AT_RECORD_COUNT is not None

    @property
    def url_base(self) -> str:
        return f"https://{self.pinned_host}"

    @property
    def pinned_host(self) -> str:
        return pinned_host(self.config["domain"], self.config.get("custom_domain"))

    @property
    def requests_session(self) -> requests.Session:
        """The SDK session, with the environment ignored.

        trust_env off stops .netrc and proxy settings from adding headers,
        such as Authorization.
        """
        session = self._requests_session or requests.Session()
        session.trust_env = False
        refuse_cookies(session)
        self._requests_session = session
        return session

    @property
    def auth(self) -> GainsightAuth:
        return self._tap.auth  # type: ignore[attr-defined]

    @property
    def http_headers(self) -> dict:
        # Docs: header "accesskey" or, for M2M OAuth, a Bearer token, and
        # "Content Type: JSON". The SDK sends the body with `json=`, which
        # sets Content-Type: application/json.
        headers = self.auth.headers()
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config["user_agent"]
        return headers

    @property
    def rate_limiter(self) -> RateLimiter:
        return self._tap.rate_limiter  # type: ignore[attr-defined]

    # The prepared request that already had its one auth retry.
    _auth_retried_for: t.Any = None

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: t.Optional[dict],
    ) -> requests.Response:
        # Runs once per attempt, so retries count against the limits too.
        # safety.send refuses anything but an allowed read, and never
        # follows a redirect.
        def send() -> t.Tuple[requests.PreparedRequest, requests.Response]:
            prepared = prepared_request
            if self.auth.is_oauth:
                # The token may have been replaced since the request was built.
                prepared = prepared_request.copy()
                prepared.headers.update(self.auth.headers())
            response = safety.send(
                self.requests_session,
                prepared,
                self.rate_limiter,
                self._tap.request_budget,  # type: ignore[attr-defined]
                pinned_host=self.pinned_host,
                timeout=self.timeout,
            )
            return prepared, response

        sent, response = send()
        # A token can lapse early. The SDK retries a page by calling this
        # again with the same prepared request, so one page gets at most
        # one new token and one retry, across every backoff attempt.
        if self._auth_retried_for is not prepared_request and self.auth.failed_auth(response):
            self._auth_retried_for = prepared_request
            if self.auth.refresh_after_failure(self.auth.token_in(sent.headers)):
                _, response = send()
        self._write_request_duration_log(
            endpoint=self.path, response=response, context=context, extra_tags=None
        )
        self.validate_response(response)
        return response

    def summary(self, response: requests.Response) -> str:
        """response_summary, with every configured credential redacted."""
        return response_summary(response, self.auth.secrets())

    def response_error_message(self, response: requests.Response) -> str:
        message = super().response_error_message(response)
        if response.status_code in (401, 403) or is_unauthorized_payload(
            json_or_none(response)
        ):
            message += (
                f". Gainsight rejected {self.auth.credential}. Check {self.auth.settings_hint}"
            )
        return redact(f"{message}. Response: {self.summary(response)}", self.auth.secrets())

    def is_empty_reply(self, response: requests.Response, payload: t.Any) -> bool:
        """Return True when a non-success reply means "no rows" for this stream."""
        return False

    def validate_response(self, response: requests.Response) -> None:
        """Fail on errors. Accept the documented empty replies at any 4xx.

        The empty-reply check runs before the status check, because the
        Error Codes article lists GSOBJ_1011 "No entity matches the given
        criteria" as HTTP 400.
        """
        status = response.status_code
        path = urlparse(response.url).path
        if 300 <= status < 400:
            raise FatalAPIError(redirect_message(response, f"path {path}"))
        payload = json_or_none(response)
        if status < 500 and status != 429:
            if is_no_data_response(payload) or self.is_empty_reply(response, payload):
                return
        if is_ip_not_allowed_payload(payload):
            raise FatalAPIError(
                f"{status}: {IP_NOT_ALLOWED_MESSAGE} Path {path}. Response: {self.summary(response)}"
            )
        super().validate_response(response)
        if payload is None:
            raise FatalAPIError(
                f"{status}: the response for path {path} is not JSON. "
                f"Response: {self.summary(response)}"
            )
        if is_unauthorized_payload(payload):
            raise FatalAPIError(
                f"{status}: Gainsight rejected {self.auth.credential} for path {path}. "
                f"Check {self.auth.settings_hint}. Response: {self.summary(response)}"
            )
        if isinstance(payload, dict) and payload.get("result") is False:
            raise FatalAPIError(
                f"{status}: Gainsight returned result=false for path {path}. "
                f"Response: {self.summary(response)}"
            )

    def backoff_wait_generator(self) -> t.Generator[float, None, None]:
        return backoff.expo(factor=2, max_value=MAX_WAIT_SECONDS)

    def backoff_max_tries(self) -> int:
        return MAX_TRIES

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        yield from extract_rows(response.json())

    def post_process(
        self, row: dict, context: t.Optional[dict] = None
    ) -> t.Optional[dict]:
        for field in self.date_fields:
            if field in row:
                row[field] = to_iso_datetime(row[field])
        for field in self.text_fields:
            if field in row:
                row[field] = to_text(row[field])
        return row

    def log_unknown_types(self, unknown_types: t.Mapping[str, t.Any]) -> None:
        """Log the Gainsight types this stream sends as text, with counts.

        `unknown_types` maps a field name to its describe `dataType`.
        """
        if not unknown_types:
            return
        counts = collections.Counter(
            str(kind or "no type").upper() for kind in unknown_types.values()
        )
        summary = ", ".join(f"{kind} ({count})" for kind, count in sorted(counts.items()))
        self.logger.info(
            "Stream %s sends %d fields as text because the tap does not map "
            "their Gainsight types: %s.",
            self.name,
            len(unknown_types),
            summary,
        )

    def is_property_selected(self, name: str) -> bool:
        """Return True when the catalog selects a top-level property."""
        return bool(self.mask.get(("properties", name), True))

    # Request loop. Each stream yields rows from `fetch_rows`, built on
    # `post_page`, instead of the SDK's single-paginator loop.

    _request_counter: t.Any = None
    _null_bookmark_rows = 0
    _limited_rows = 0

    def request_records(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        # The limit counts rows across every partition of the stream, so a
        # partition after the limit sends no request at all.
        limit = self.ABORT_AT_RECORD_COUNT
        if limit is not None and self._limited_rows >= limit:
            return
        with metrics.http_request_counter(self.name, self.path) as counter:
            counter.context = context
            self._request_counter = counter
            rows = self.fetch_rows(context)
            try:
                for row in rows:
                    yield row
                    self._limited_rows += 1
                    if limit is not None and self._limited_rows >= limit:
                        # Stop before the next request.
                        return
            finally:
                close = getattr(rows, "close", None)
                if close is not None:
                    close()
                self._request_counter = None

    def _check_max_record_limit(self, current_record_index: int) -> None:
        """Leave the limit to request_records, so a limited sync ends cleanly.

        The SDK raises an abort exception at the limit, and a sync outside a
        dry run exits with an error. request_records stops at the limit
        instead, before it sends another request.
        """

    def fetch_rows(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        raise NotImplementedError

    def post_page(self, context: t.Optional[dict], token: dict) -> t.List[dict]:
        """Send one request built from `token` and return its rows."""
        prepared = self.prepare_request(context, next_page_token=token)
        response = self.request_decorator(self._request)(prepared, context)
        if self._request_counter is not None:
            self._request_counter.increment()
        self.update_sync_costs(prepared, response, context)
        return list(self.parse_response(response))

    def format_filter(self, value: datetime.datetime) -> str:
        """Format a query API filter value in the `filter_timezone` zone."""
        return format_query_datetime(value, load_zone(self.config.get("filter_timezone")))

    def filter_start(self, context: t.Optional[dict]) -> t.Optional[datetime.datetime]:
        """Return the bookmark or start_date, less FILTER_LOOKBACK."""
        if not self.replication_key:
            return None
        start = self.get_starting_timestamp(context)
        return as_utc(start) - FILTER_LOOKBACK if start else None

    def log_delete_gap(self, context: t.Optional[dict]) -> None:
        """Log an error when the start is older than Gainsight keeps deletes."""
        start = self.get_starting_timestamp(context)
        if not start:
            return
        gap = utc_now() - as_utc(start)
        if gap > DELETE_RETENTION:
            self.logger.error(
                "%s: the sync starts from %s, which is %d days ago. Gainsight "
                "keeps deleted records for %d days only, so deletes from the "
                "first %d days of that gap are lost. Run this stream at least "
                "every 14 days.",
                self.name,
                as_utc(start).isoformat(),
                round(gap / datetime.timedelta(days=1)),
                DELETE_RETENTION.days,
                round((gap - DELETE_RETENTION) / datetime.timedelta(days=1)),
            )

    def _increment_stream_state(
        self, latest_record: dict, *, context: t.Optional[dict] = None
    ) -> None:
        # A record limit reads only part of the data, so it moves no bookmark.
        if self.is_limited:
            return
        # A null replication key cannot be compared, and the SDK raises a
        # TypeError on it. The record is still emitted. Only the bookmark
        # skips it.
        if self.replication_key and latest_record.get(self.replication_key) is None:
            self._null_bookmark_rows += 1
            return
        super()._increment_stream_state(latest_record, context=context)

    def sync(self, context: t.Optional[dict] = None) -> None:
        self._null_bookmark_rows = 0
        self._limited_rows = 0
        super().sync(context)
        if self._null_bookmark_rows:
            self.logger.warning(
                "%s: synced %d record(s) with a null %s. They do not move the "
                "bookmark.",
                self.name,
                self._null_bookmark_rows,
                self.replication_key,
            )


class SecondChainStream(GainsightStream):
    """A query-API stream read as a chain of whole seconds.

    Offset paging over a sort on ModifiedDate skips a row whenever an
    earlier row is edited or deleted between pages. This chain asks for
    rows after a point instead, with only documented request forms:
    AND-joined conditions and `yyyy-MM-dd HH:mm:ss` values in UTC.

    1. Scan: read `key GTE L` (or `key IS_NOT_NULL` on a first run),
       ordered by (key, tie-breaker). Every row in a second before the
       page's last second is complete, so those rows are emitted.
    2. Drain: read the page's last second s with
       `key GTE s AND key LT s+1s AND tie-breaker GT g`, ordered by the
       tie-breaker, until a page is short. This is the one extension
       beyond the documented two-term `A AND B`.
    3. Set L to s+1s and scan again.

    The scan page listed rows in second s. If the drain returns none of
    them, the tap reads the first such row by its tie-breaker. If it still
    has a value in second s, the API read the filter in another time zone
    or grain, and the stream fails instead of skipping rows. Rows a coarse
    server returns outside the range asked for are duplicates. They are
    dropped and counted in a debug log.

    A last pass reads rows whose key is null. Streams with no replication
    key read by tie-breaker: `tie-breaker GT g`, ordered by the tie-breaker.
    """

    tiebreaker = "Gsid"

    def base_payload(self) -> t.Dict[str, t.Any]:
        raise NotImplementedError

    def can_sort_by_tiebreaker(self) -> bool:
        return True

    # Request building

    def condition(self, name: str, operator: str, value: t.List[t.Any]) -> dict:
        return {"name": name, "alias": "", "value": value, "operator": operator}

    def prepare_request_payload(
        self, context: t.Optional[dict], next_page_token: t.Optional[dict]
    ) -> dict:
        token = next_page_token or {}
        payload = self.base_payload()
        if token.get("select"):
            payload["select"] = token["select"]
        conditions = [dict(c) for c in token.get("conditions") or []]
        if conditions:
            for alias, condition in zip("ABCDEFGH", conditions):
                condition["alias"] = alias
            payload["where"] = {
                "conditions": conditions,
                "expression": " AND ".join(c["alias"] for c in conditions),
            }
        if token.get("order"):
            payload["orderBy"] = token["order"]
        payload["limit"] = self.page_size
        payload["offset"] = token.get("offset", 0)
        return payload

    # Paging

    def _second(self, row: dict) -> t.Optional[datetime.datetime]:
        moment = parse_api_datetime(row.get(self.replication_key))
        return moment.replace(microsecond=0) if moment else None

    def _tiebreak(self, row: dict) -> str:
        value = row.get(self.tiebreaker)
        if value is None:
            raise FatalAPIError(
                f"{self.name}: a row lacks {self.tiebreaker}, which paging "
                f"needs. The row has the fields {sorted(row)}."
            )
        return str(value)

    def fetch_rows(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        if not self.replication_key:
            yield from self.tiebreak_rows(context, [])
            return
        rk, tb = self.replication_key, self.tiebreaker
        lower = self.filter_start(context)
        lower = lower.replace(microsecond=0) if lower else None
        while True:
            if lower is None:
                first = self.condition(rk, "IS_NOT_NULL", [])
            else:
                first = self.condition(rk, "GTE", [self.format_filter(lower)])
            page = self.post_page(
                context,
                {"conditions": [first], "order": {rk: "asc", tb: "asc"}},
            )
            kept = [
                row
                for row in page
                if self._second(row) is not None
                and (lower is None or self._second(row) >= lower)
            ]
            self._log_duplicates(len(page) - len(kept))
            if len(page) < self.page_size:
                yield from kept
                break
            if not kept:
                raise FatalAPIError(
                    f"{self.name}: a full page from {rk} GTE "
                    f"{self.format_filter(lower) if lower else 'the start'} "
                    "had no row at or after that value. The API may read the "
                    "filter at a coarser grain. Stopping so no rows are skipped."
                )
            last = self._second(kept[-1])
            assert last is not None
            known = [row for row in kept if self._second(row) == last]
            done = [row for row in kept if self._second(row) < last]
            yield from done
            yield from self._drain_second(context, last, known)
            lower = last + datetime.timedelta(seconds=1)
        yield from self.tiebreak_rows(context, [self.condition(rk, "IS_NULL", [])])

    def _drain_second(
        self, context: t.Optional[dict], second: datetime.datetime, known: t.List[dict]
    ) -> t.Iterable[dict]:
        rk, tb = self.replication_key, self.tiebreaker
        bounds = [
            self.condition(rk, "GTE", [self.format_filter(second)]),
            self.condition(
                rk, "LT", [self.format_filter(second + datetime.timedelta(seconds=1))]
            ),
        ]
        known_id = self._tiebreak(known[0]) if known else None
        found = 0
        after: t.Optional[str] = None
        while True:
            conditions = bounds + ([self.condition(tb, "GT", [after])] if after else [])
            rows = self.post_page(context, {"conditions": conditions, "order": {tb: "asc"}})
            next_after = self._tiebreak(rows[-1]) if rows else None
            in_second = [row for row in rows if self._second(row) == second]
            self._log_duplicates(len(rows) - len(in_second))
            found += len(in_second)
            yield from in_second
            if len(rows) < self.page_size:
                break
            after = next_after
        if found == 0 and known_id is not None:
            self._check_row_moved(context, known_id, second)

    def _check_row_moved(
        self, context: t.Optional[dict], row_id: str, second: datetime.datetime
    ) -> None:
        rk, tb = self.replication_key, self.tiebreaker
        rows = self.post_page(
            context,
            {
                "select": [tb, rk],
                "conditions": [self.condition(tb, "EQ", [row_id])],
                "order": {rk: "asc"},
            },
        )
        if any(self._second(row) == second for row in rows):
            raise FatalAPIError(
                f"{self.name}: the API did not return the rows it listed for "
                f"{rk} {self.format_filter(second)}, although row {tb} "
                f"{row_id} still has that value. It may read the filter in "
                "another time zone or at a coarser grain. Stopping so no rows "
                "are skipped. If your Gainsight tenant reads filter times in "
                "its local time zone, set filter_timezone."
            )

    def tiebreak_rows(
        self, context: t.Optional[dict], base: t.List[dict]
    ) -> t.Iterable[dict]:
        """Read rows matching `base`, paged by `tie-breaker GT g`."""
        tb = self.tiebreaker
        if not self.can_sort_by_tiebreaker():
            yield from self.offset_rows(context, base)
            return
        after: t.Optional[str] = None
        while True:
            conditions = base + ([self.condition(tb, "GT", [after])] if after else [])
            rows = self.post_page(context, {"conditions": conditions, "order": {tb: "asc"}})
            full = len(rows) >= self.page_size
            after = self._tiebreak(rows[-1]) if full else None
            yield from rows
            if not full:
                return

    def offset_rows(self, context: t.Optional[dict], base: t.List[dict]) -> t.Iterable[dict]:
        """Page with `offset` when the tie-breaker cannot be sorted on."""
        offset = 0
        while True:
            rows = self.post_page(context, {"conditions": base, "offset": offset})
            yield from rows
            if len(rows) < self.page_size:
                return
            offset += self.page_size

    def _log_duplicates(self, count: int) -> None:
        if count:
            self.logger.debug(
                "%s: dropped %d row(s) outside the range asked for. They are "
                "duplicates of rows read elsewhere.",
                self.name,
                count,
            )
