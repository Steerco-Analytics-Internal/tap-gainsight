"""Read-only enforcement. Every HTTP request the tap makes goes through `send`.

`send` checks the request against READ_ONLY_ALLOWLIST and its body against
the read keys for that endpoint, before any network I/O. A request that
fails a check raises GainsightSafetyError, whatever the config or catalog.
This keeps the tap from changing data in a Gainsight tenant, even if a bug
elsewhere builds a write request.
"""

from __future__ import annotations

import json
import re
import typing as t
from urllib.parse import parse_qs, urlsplit

import requests

# Object API names are identifiers: letters, digits and underscores, such
# as Company or renewal__gc. No dots, slashes or percent signs.
_NAME = r"[A-Za-z0-9_]+"

# Body keys allowed per endpoint. Docs: Custom Object API "Read API"
# (select, where, orderBy, limit, offset); Call To Action (CTA) API "Fetch
# CTA API" (select, where, pageSize, pageNumber); Data Management APIs
# "Post Describe OMD" (objectNames and the describe flags).
QUERY_BODY_KEYS = frozenset({"select", "where", "orderBy", "limit", "offset"})
CTA_BODY_KEYS = frozenset({"select", "where", "pageSize", "pageNumber"})
DESCRIBE_BODY_KEYS = frozenset(
    {
        "objectNames",
        "includeChilds",
        "childLevels",
        "populatePickListOptions",
        "useCollectionId",
        "removeDeleted",
        "removeHidden",
        "host",
        "sortFieldsByLabel",
        "populateFieldId",
        "honorUserContext",
        "honorCustomLookup",
        "populateAutoSuggestDetails",
        "externalContext",
    }
)
# Keys that carry data to write. They must not appear anywhere in a body.
WRITE_KEYS = frozenset({"records", "data", "lookups", "updateKeys"})

# Docs: "Generate REST API Key", "Get Access Token API". The docs show no
# body and link RFC 6749 section 4.4, the client credentials grant. Its one
# required form field is grant_type=client_credentials.
TOKEN_PATH = "/v1/users/m2m/oauth/token"
TOKEN_BODY = {"grant_type": "client_credentials"}

# Authorization values, by request. The token request sends Basic
# base64(client_id:client_secret). Every other request may send only a
# Bearer token, in the RFC 6750 b64token form.
BASIC_AUTHORIZATION = re.compile(r"Basic [A-Za-z0-9+/]+={0,2}")
BEARER_TOKEN = re.compile(r"[A-Za-z0-9._~+/-]+=*")
BEARER_AUTHORIZATION = re.compile(rf"Bearer {BEARER_TOKEN.pattern}")


class AllowedRequest(t.NamedTuple):
    method: str
    path: str  # An anchored regular expression.
    query_keys: t.FrozenSet[str]
    body_keys: t.FrozenSet[str]
    # "json" for a JSON object body, "form" for form data.
    body_format: str = "json"


# The only requests the tap may send. Each is a documented read endpoint.
READ_ONLY_ALLOWLIST: t.Tuple[AllowedRequest, ...] = (
    # Data Management APIs, "Get Lite API Call OMD".
    AllowedRequest("GET", r"^/v1/meta/services/objects/list$", frozenset({"po", "em"}), frozenset()),
    # Data Management APIs, "Post Describe OMD".
    AllowedRequest("POST", r"^/v1/meta/services/objects/describe$", frozenset(), DESCRIBE_BODY_KEYS),
    # Data Management APIs, "Get API - categoryID".
    AllowedRequest("GET", r"^/v1/meta/services/dropdowns/[A-Za-z0-9]+$", frozenset(), frozenset()),
    # Company, Custom Object and Timeline "Read API", and the delete logs.
    AllowedRequest("POST", rf"^/v1/data/objects/query/{_NAME}$", frozenset(), QUERY_BODY_KEYS),
    # Call To Action (CTA) API, "Fetch CTA API".
    AllowedRequest("POST", r"^/v2/cockpit/cta/list$", frozenset(), CTA_BODY_KEYS),
    # Retrieve Deleted Data API, CTA, "Endpoint One".
    AllowedRequest("POST", r"^/v2/cockpit/cta/deleted/list$", frozenset(), CTA_BODY_KEYS),
    # Generate REST API Key, "Get Access Token API", for M2M OAuth. It reads
    # a token and changes no tenant data.
    AllowedRequest("POST", rf"^{re.escape(TOKEN_PATH)}$", frozenset(), frozenset(TOKEN_BODY), "form"),
)


# The request header names the tap sends, lowercase. requests adds the
# last four. Anything else, such as X-HTTP-Method-Override, is refused.
# check_authorization limits the Authorization value per request.
ALLOWED_HEADERS = frozenset(
    {
        "accesskey",
        "authorization",
        "content-type",
        "content-length",
        "user-agent",
        "accept",
        "accept-encoding",
        "connection",
    }
)


class GainsightSafetyError(RuntimeError):
    """Raised before sending a request that is not a documented read."""


class GainsightRequestCapError(RuntimeError):
    """Raised when a run reaches the max_requests setting."""


class RequestBudget:
    """Counts requests in a run, and stops at `max_requests` if set."""

    def __init__(self, max_requests: t.Optional[int] = None) -> None:
        self.max_requests = max_requests
        self.sent = 0

    def spend(self) -> None:
        if self.max_requests is not None and self.sent >= self.max_requests:
            raise GainsightRequestCapError(
                f"Stopped after {self.sent} requests: the max_requests setting "
                f"allows {self.max_requests} per run. Raise it, or narrow the "
                "catalog, and run again."
            )
        self.sent += 1


def _write_keys_in(value: t.Any) -> t.Set[str]:
    found: t.Set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in WRITE_KEYS:
                found.add(key)
            found |= _write_keys_in(item)
    elif isinstance(value, list):
        for item in value:
            found |= _write_keys_in(item)
    return found


def check_request(method: str, url: str, body: t.Optional[bytes | str]) -> None:
    """Raise GainsightSafetyError unless the request is an allowed read."""
    method = (method or "").upper()
    parts = urlsplit(url)
    path = parts.path
    if "%" in path or "//" in path or "/./" in path or "/../" in path or path.endswith("/.."):
        raise GainsightSafetyError(f"Refused {method} {path}: the path is not a plain API path.")
    rule = next(
        (r for r in READ_ONLY_ALLOWLIST if r.method == method and re.fullmatch(r.path, path)),
        None,
    )
    if rule is None:
        raise GainsightSafetyError(
            f"Refused {method} {path}: not on the tap's read-only allowlist."
        )
    query_keys = set(parse_qs(parts.query, keep_blank_values=True))
    if not query_keys <= rule.query_keys:
        raise GainsightSafetyError(
            f"Refused {method} {path}: query keys {sorted(query_keys - rule.query_keys)} "
            "are not allowed."
        )
    if method == "GET":
        if body:
            raise GainsightSafetyError(f"Refused GET {path}: a GET must have no body.")
        return
    if rule.body_format == "form":
        _check_form_body(method, path, body)
        return
    try:
        payload = json.loads(body) if body else None
    except ValueError as exc:
        raise GainsightSafetyError(f"Refused {method} {path}: the body is not JSON.") from exc
    if not isinstance(payload, dict):
        raise GainsightSafetyError(f"Refused {method} {path}: the body must be a JSON object.")
    extra = set(payload) - rule.body_keys
    if extra:
        raise GainsightSafetyError(
            f"Refused {method} {path}: body keys {sorted(extra)} are not read keys."
        )
    written = _write_keys_in(payload)
    if written:
        raise GainsightSafetyError(
            f"Refused {method} {path}: the body carries write keys {sorted(written)}."
        )


def _check_form_body(method: str, path: str, body: t.Optional[bytes | str]) -> None:
    """Raise unless the body is exactly the token request's form data."""
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else (body or "")
    try:
        form = parse_qs(text, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise GainsightSafetyError(f"Refused {method} {path}: the body is not form data.") from exc
    if form != {key: [value] for key, value in TOKEN_BODY.items()}:
        raise GainsightSafetyError(
            f"Refused {method} {path}: the body must be exactly the fields {sorted(TOKEN_BODY)} "
            "with their documented values."
        )


def check_destination(url: str, pinned_host: str) -> None:
    """Raise unless `url` is https to exactly `pinned_host`, on the default port."""
    parts = urlsplit(url)
    if parts.scheme != "https":
        raise GainsightSafetyError(f"Refused {parts.scheme or 'no'} scheme: only https is allowed.")
    if "@" in parts.netloc or parts.username is not None or parts.password is not None:
        raise GainsightSafetyError("Refused a URL with user information.")
    if ":" in parts.netloc.strip("[]") or parts.port is not None:
        raise GainsightSafetyError("Refused a URL with an explicit port.")
    if (parts.hostname or "") != pinned_host.lower():
        raise GainsightSafetyError(
            f"Refused host {parts.hostname!r}: the tap only talks to {pinned_host!r}."
        )


def check_headers(headers: t.Mapping[str, str]) -> None:
    """Raise unless every header name is one the tap sends."""
    extra = sorted(name for name in headers if name.lower() not in ALLOWED_HEADERS)
    if extra:
        raise GainsightSafetyError(f"Refused headers {extra}: not on the tap's header allowlist.")


def _is_token_request(method: str, url: str) -> bool:
    return (method or "").upper() == "POST" and urlsplit(url).path == TOKEN_PATH


def check_authorization(method: str, url: str, headers: t.Mapping[str, str]) -> None:
    """Raise unless the credential headers fit the request.

    The token request must carry Basic authorization and no AccessKey.
    Every other request may carry Bearer authorization or AccessKey, never
    both, and never another Authorization scheme. Values are not echoed.
    """
    names = {name.lower(): value for name, value in headers.items()}
    authorization = names.get("authorization")
    path = urlsplit(url).path
    if _is_token_request(method, url):
        if authorization is None or not BASIC_AUTHORIZATION.fullmatch(authorization):
            raise GainsightSafetyError(
                f"Refused POST {path}: the header allowlist requires Basic authorization on the token request."
            )
        if "accesskey" in names:
            raise GainsightSafetyError(f"Refused POST {path}: the token request must not carry AccessKey.")
        return
    if authorization is None:
        return
    if not BEARER_AUTHORIZATION.fullmatch(authorization):
        raise GainsightSafetyError(
            f"Refused {(method or '').upper()} {path}: the header allowlist permits only Bearer authorization here."
        )
    if "accesskey" in names:
        raise GainsightSafetyError(
            f"Refused {(method or '').upper()} {path}: send AccessKey or Authorization, not both."
        )


def send(
    session: requests.Session,
    prepared: requests.PreparedRequest,
    limiter: t.Any,
    budget: t.Optional[RequestBudget] = None,
    *,
    pinned_host: str,
    **kwargs: t.Any,
) -> requests.Response:
    """The single place the tap sends HTTP. Checks, then rate limits, then sends.

    The checks: the session ignores the environment (no .netrc or proxy
    settings), the URL is https to the pinned host, the headers are on the
    header allowlist, the Authorization form fits the request, and the
    method, path and body are an allowed read. Redirects are never
    followed: requests would resend the credential headers to the new host.
    """
    if session.trust_env:
        raise GainsightSafetyError(
            "Refused a session with trust_env on: .netrc or proxy settings could add headers."
        )
    check_destination(prepared.url or "", pinned_host)
    check_headers(prepared.headers or {})
    check_authorization(prepared.method or "", prepared.url or "", prepared.headers or {})
    check_request(prepared.method or "", prepared.url or "", prepared.body)
    if budget is not None:
        budget.spend()
    limiter.acquire()
    kwargs["allow_redirects"] = False
    return session.send(prepared, **kwargs)
