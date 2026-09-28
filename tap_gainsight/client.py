"""HTTP layer for the Gainsight CS API: rate limiter, metadata client, base stream.

Every endpoint, header and body field used here traces to a Gainsight doc page.
The README has the full table. Doc links used in this module:

- Data Management APIs (describe, object list, dropdowns, delete log):
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs
- Company API (query shape, "No data found" failure, rate limits):
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Company_API_Documentation
- Custom Object API (query shape, empty result, epoch dates):
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Custom_Object_API/Gainsight_Custom_Object_API_Documentation
"""

from __future__ import annotations

import collections
import datetime
import threading
import time
import typing as t
from urllib.parse import urlparse

import backoff
import requests
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.pagination import BaseOffsetPaginator, BasePageNumberPaginator
from singer_sdk.streams import RESTStream

DEFAULT_HOST_SUFFIX = ".gainsightcloud.com"
BODY_EXCERPT_LENGTH = 500
MAX_TRIES = 8
MAX_WAIT_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 300

# Docs: "Synchronous API Calls: 100 API calls per min", a fixed window.
# Source: every API page above, section "Throttling Limits".
RATE_LIMIT_CALLS = 100
RATE_LIMIT_PERIOD_SECONDS = 60.0

# Docs, Data Management APIs, "Get API - categoryID", Sample Failure Response.
UNAUTHORIZED_ERROR_CODE = "GS_APIG_2401"
# Docs, Company API, "Read API", Sample Failure Response.
NO_DATA_ERROR_DESC = "no data found for given criteria"

# Gainsight data types with a known JSON shape. The docs name STRING, GSID,
# DATETIME and LOOKUP as describe `dataType` values. The rest come from the
# operator table in the Custom Object API page. Any other type accepts every
# JSON type, because the docs do not show its value shape.
STRING_DATA_TYPES = {"STRING", "GSID", "EMAIL", "URL", "LOOKUP", "RICHTEXTAREA"}
NUMBER_DATA_TYPES = {"NUMBER", "PERCENTAGE", "CURRENCY"}
BOOLEAN_DATA_TYPES = {"BOOLEAN"}
DATETIME_DATA_TYPES = {"DATETIME"}
DATE_DATA_TYPES = {"DATE"}
ANY_TYPE = ["null", "string", "number", "boolean", "object", "array"]


class GainsightAPIError(Exception):
    """Raised when a Gainsight metadata call fails."""

    def __init__(self, message: str, status_code: t.Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class GainsightAuthError(GainsightAPIError):
    """Raised when Gainsight rejects the access key."""


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


def normalize_domain(value: str) -> str:
    """Return `https://<host>` for a domain, a URL, or a bare subdomain.

    A value with no dot, such as `acme`, becomes `acme.gainsightcloud.com`.
    """
    raw = (value or "").strip()
    if not raw:
        raise ValueError("The `domain` setting is empty.")
    if "://" not in raw:
        raw = f"https://{raw}"
    host = urlparse(raw).netloc
    if not host:
        raise ValueError(f"The `domain` setting has no host: {value!r}")
    if "." not in host:
        host = f"{host}{DEFAULT_HOST_SUFFIX}"
    return f"https://{host}"


def body_excerpt(response: requests.Response) -> str:
    """Return the start of a response body, for error messages."""
    text = response.text or ""
    if len(text) > BODY_EXCERPT_LENGTH:
        return text[:BODY_EXCERPT_LENGTH] + "..."
    return text


def is_no_data_response(payload: t.Any) -> bool:
    """Return True for the documented `result: false` "No data found" reply."""
    return (
        isinstance(payload, dict)
        and payload.get("result") is False
        and NO_DATA_ERROR_DESC in str(payload.get("errorDesc") or "").lower()
    )


def is_unauthorized_payload(payload: t.Any) -> bool:
    """Return True when a body carries the documented Unauthorized error code."""
    return (
        isinstance(payload, dict)
        and payload.get("errorCode") == UNAUTHORIZED_ERROR_CODE
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
    raise FatalAPIError(
        f"Unexpected `data` shape in the response: {str(data)[:BODY_EXCERPT_LENGTH]}"
    )


def to_iso_datetime(value: t.Any) -> t.Any:
    """Convert an epoch-milliseconds value to an ISO 8601 UTC string.

    Docs, Custom Object API: "Date and DateTime values are shown in epoch
    format." Strings, such as CTA dates, pass through unchanged.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value
    moment = datetime.datetime.fromtimestamp(value / 1000, tz=datetime.timezone.utc)
    return moment.isoformat()


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


def format_query_datetime(value: datetime.datetime) -> str:
    """Format a DATETIME filter value for the query API.

    Uses `yyyy-MM-dd HH:mm:ss` in UTC, the form in the delete log sample
    request: `"value": ["2024-02-05 00:00:00"]`.
    """
    return as_utc(value).strftime("%Y-%m-%d %H:%M:%S")


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
    return {"type": list(ANY_TYPE)}


def is_date_type(data_type: t.Optional[str]) -> bool:
    """Return True for Gainsight DATE and DATETIME fields."""
    kind = (data_type or "").upper()
    return kind in DATE_DATA_TYPES or kind in DATETIME_DATA_TYPES


class GainsightMetadataClient:
    """Calls the Data Management metadata APIs used at discovery time.

    Retries 429, 5xx, connection errors and timeouts with exponential
    backoff. Raises GainsightAuthError on 401, 403 or the documented
    `GS_APIG_2401` body, and GainsightAPIError on any other failure.
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
    ) -> None:
        self.base_url = normalize_domain(config["domain"])
        self.rate_limiter = rate_limiter
        self.session = session or requests.Session()
        # Docs: pass the access key in the "accesskey" header. Header names
        # are case-insensitive in HTTP.
        self.session.headers.update(
            {"AccessKey": config["access_key"], "Accept": "application/json"}
        )

    def _send(
        self,
        method: str,
        path: str,
        label: str,
        params: t.Optional[dict] = None,
        body: t.Optional[dict] = None,
    ) -> dict:
        url = f"{self.base_url}{path}"

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
            self.rate_limiter.acquire()
            response = self.session.request(
                method,
                url,
                params=params,
                json=body,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            status = response.status_code
            if status == 429 or status >= 500:
                raise _RetriableError(
                    f"{status} from {label}: {body_excerpt(response)}", status
                )
            return response

        try:
            response = _call()
        except _RetriableError as exc:
            raise GainsightAPIError(
                f"{exc} (gave up after {MAX_TRIES} tries)", exc.status_code
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise GainsightAPIError(f"{label} failed: {exc}") from exc

        status = response.status_code
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if status in (401, 403) or is_unauthorized_payload(payload):
            raise GainsightAuthError(
                f"{status}: Gainsight rejected the access key on {label}. Check "
                f"`access_key` and `domain`. Body: {body_excerpt(response)}",
                status,
            )
        if status >= 400:
            raise GainsightAPIError(
                f"{status} from {label}: {body_excerpt(response)}", status
            )
        if not isinstance(payload, dict):
            raise GainsightAPIError(
                f"{label} did not return a JSON object: {body_excerpt(response)}",
                status,
            )
        if payload.get("result") is False:
            raise GainsightAPIError(
                f"{label} failed: {body_excerpt(response)}", status
            )
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
                f"The object list returned no `data` list: {str(payload)[:500]}"
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


class RowCountOffsetPaginator(BaseOffsetPaginator):
    """Offset paginator that stops when a page has fewer rows than the limit.

    The next offset is the current offset plus the limit. The docs say to
    pass "Offset as 5001" for the second page of 5000. That would skip one
    row if offsets start at 0, as the samples show, so the tap uses 5000.
    A repeated row is harmless, because targets dedupe on the key.
    """

    def has_more(self, response: requests.Response) -> bool:
        return len(extract_rows(response.json())) >= self._page_size


class RowCountPageNumberPaginator(BasePageNumberPaginator):
    """Page-number paginator that stops when a page is short."""

    def __init__(self, start_value: int, page_size: int) -> None:
        super().__init__(start_value)
        self._page_size = page_size

    def has_more(self, response: requests.Response) -> bool:
        return len(extract_rows(response.json())) >= self._page_size


class GainsightStream(RESTStream):
    """Base class for Gainsight CS streams."""

    rest_method = "POST"
    page_size = 5000

    # Fields whose epoch-millisecond values become ISO 8601 strings.
    date_fields: t.Set[str] = set()

    @property
    def url_base(self) -> str:
        return normalize_domain(self.config["domain"])

    @property
    def http_headers(self) -> dict:
        # Docs: header "accesskey" and "Content Type: JSON". The SDK sends the
        # body with `json=`, which sets Content-Type: application/json.
        headers = {"AccessKey": self.config["access_key"]}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config["user_agent"]
        return headers

    @property
    def rate_limiter(self) -> RateLimiter:
        return self._tap.rate_limiter  # type: ignore[attr-defined]

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: t.Optional[dict],
    ) -> requests.Response:
        # Runs once per attempt, so retries count against the limit too.
        self.rate_limiter.acquire()
        return super()._request(prepared_request, context)

    def response_error_message(self, response: requests.Response) -> str:
        message = super().response_error_message(response)
        if response.status_code in (401, 403):
            message += ". Gainsight rejected the access key. Check `access_key` and `domain`"
        return f"{message}. Body: {body_excerpt(response)}"

    def validate_response(self, response: requests.Response) -> None:
        """Fail on HTTP errors, and on a 200 whose body reports a failure."""
        super().validate_response(response)
        path = urlparse(response.url).path
        try:
            payload = response.json()
        except ValueError as exc:
            raise FatalAPIError(
                f"{response.status_code}: the response for path {path} is not "
                f"JSON. Body: {body_excerpt(response)}"
            ) from exc
        if is_unauthorized_payload(payload):
            raise FatalAPIError(
                f"{response.status_code}: Gainsight rejected the access key for "
                f"path {path}. Check `access_key` and `domain`. "
                f"Body: {body_excerpt(response)}"
            )
        if is_no_data_response(payload):
            return
        if isinstance(payload, dict) and payload.get("result") is False:
            raise FatalAPIError(
                f"{response.status_code}: Gainsight returned result=false for "
                f"path {path}. Body: {body_excerpt(response)}"
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
        return row

    def is_property_selected(self, name: str) -> bool:
        """Return True when the catalog selects a top-level property."""
        return bool(self.mask.get(("properties", name), True))
