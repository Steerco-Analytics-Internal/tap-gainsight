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
import re
import threading
import time
import typing as t
from urllib.parse import urlparse

import backoff
import requests
from singer_sdk import metrics
from singer_sdk.exceptions import FatalAPIError
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


class GainsightRedirectError(GainsightAuthError):
    """Raised on a 3xx. The access key is never sent to another host."""


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


def json_or_none(response: requests.Response) -> t.Any:
    """Return the parsed body, or None when the body is not JSON."""
    try:
        return response.json()
    except ValueError:
        return None


def is_no_data_response(payload: t.Any) -> bool:
    """Return True for a documented `result: false` empty reply.

    Matches both documented wordings and the GSOBJ_1011 code.
    """
    if not isinstance(payload, dict) or payload.get("result") is not False:
        return False
    if payload.get("errorCode") in NO_DATA_ERROR_CODES:
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
    return isinstance(payload, dict) and payload.get("errorCode") in AUTH_ERROR_CODES


def redirect_message(response: requests.Response, label: str) -> str:
    """Describe a refused redirect, naming the host it pointed to."""
    location = response.headers.get("Location") or ""
    host = urlparse(location).netloc or location or "an unknown host"
    return (
        f"{response.status_code} redirect from {label} to {host}. The tap does "
        "not follow redirects, so the access key never goes to another host. "
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
    match = _DATE_TEXT.match(str(value).strip())
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


def format_api_datetime(value: datetime.datetime) -> str:
    """Format a DATETIME filter value with milliseconds and a UTC offset.

    Uses `yyyy-MM-dd'T'HH:mm:ss.SSSZ`, the DateTime format in the Timeline
    APIs page, for example `2026-04-14T10:30:00.000+0000`. The explicit
    offset leaves no time zone to guess, and milliseconds let a keyset
    cursor match a row exactly.
    """
    moment = as_utc(value)
    millis = moment.microsecond // 1000
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}+0000"


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
                allow_redirects=False,
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
        if 300 <= status < 400:
            raise GainsightRedirectError(redirect_message(response, label), status)
        payload = json_or_none(response)
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
        # Redirects are never followed. requests would resend the AccessKey
        # header to the new host, because it strips only Authorization.
        response = self.requests_session.send(
            prepared_request, timeout=self.timeout, allow_redirects=False
        )
        self._write_request_duration_log(
            endpoint=self.path, response=response, context=context, extra_tags=None
        )
        self.validate_response(response)
        return response

    def response_error_message(self, response: requests.Response) -> str:
        message = super().response_error_message(response)
        if response.status_code in (401, 403) or is_unauthorized_payload(
            json_or_none(response)
        ):
            message += ". Gainsight rejected the access key. Check `access_key` and `domain`"
        return f"{message}. Body: {body_excerpt(response)}"

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
        super().validate_response(response)
        if payload is None:
            raise FatalAPIError(
                f"{status}: the response for path {path} is not JSON. "
                f"Body: {body_excerpt(response)}"
            )
        if is_unauthorized_payload(payload):
            raise FatalAPIError(
                f"{status}: Gainsight rejected the access key for path {path}. "
                f"Check `access_key` and `domain`. Body: {body_excerpt(response)}"
            )
        if isinstance(payload, dict) and payload.get("result") is False:
            raise FatalAPIError(
                f"{status}: Gainsight returned result=false for path {path}. "
                f"Body: {body_excerpt(response)}"
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

    # Request loop. Each stream yields rows from `fetch_rows`, built on
    # `post_page`, instead of the SDK's single-paginator loop.

    _request_counter: t.Any = None
    _null_bookmark_rows = 0

    def request_records(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        with metrics.http_request_counter(self.name, self.path) as counter:
            counter.context = context
            self._request_counter = counter
            try:
                yield from self.fetch_rows(context)
            finally:
                self._request_counter = None

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

    def offset_rows(self, context: t.Optional[dict], mode: str) -> t.Iterable[dict]:
        """Page with `offset` until a page is short."""
        offset = 0
        while True:
            rows = self.post_page(context, {"mode": mode, "offset": offset})
            yield from rows
            if len(rows) < self.page_size:
                return
            offset += self.page_size

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
        # A null replication key cannot be compared, and the SDK raises a
        # TypeError on it. The record is still emitted. Only the bookmark
        # skips it.
        if self.replication_key and latest_record.get(self.replication_key) is None:
            self._null_bookmark_rows += 1
            return
        super()._increment_stream_state(latest_record, context=context)

    def sync(self, context: t.Optional[dict] = None) -> None:
        self._null_bookmark_rows = 0
        super().sync(context)
        if self._null_bookmark_rows:
            self.logger.warning(
                "%s: synced %d record(s) with a null %s. They do not move the "
                "bookmark.",
                self.name,
                self._null_bookmark_rows,
                self.replication_key,
            )


class KeysetStream(GainsightStream):
    """A query-API stream paged by keyset on (replication key, tie-breaker).

    Offset paging over a sort on ModifiedDate skips a row whenever an
    earlier row is edited or deleted between pages. Keyset paging asks for
    the rows after the last one seen instead:

        A OR (B AND C)
        A: <key> GT x    B: <key> EQ x    C: <tie-breaker> GTE g

    `GTE` keeps the last row (the anchor) in the next page. When it is
    there, it is dropped. When it is missing, the tap reads that row by its
    tie-breaker. If the row still has the value x, the API did not match the
    cursor value, and the stream fails instead of skipping rows.

    After the keyset pass, a second pass reads rows whose key is null, with
    offset paging sorted by the tie-breaker. A stream with no replication
    key uses offset paging sorted by the tie-breaker.
    """

    tiebreaker = "Gsid"

    def format_cursor(self, value: datetime.datetime) -> str:
        return format_api_datetime(value)

    def base_payload(self) -> t.Dict[str, t.Any]:
        raise NotImplementedError

    def can_sort_by_tiebreaker(self) -> bool:
        return True

    def fetch_rows(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        if not self.replication_key:
            yield from self.offset_rows(context, "offset")
            return
        cursor: t.Optional[t.Tuple[int, str]] = None
        while True:
            rows = self.post_page(context, {"mode": "keyset", "cursor": cursor})
            full = len(rows) >= self.page_size
            if cursor is not None:
                rows = self._without_anchor(context, rows, cursor)
            next_cursor = self._cursor_of(rows[-1]) if full and rows else None
            yield from rows
            if not full:
                break
            if next_cursor is None or next_cursor == cursor:
                raise FatalAPIError(
                    f"{self.name}: keyset paging made no progress at {cursor}. "
                    "Use a page size of 2 or more."
                )
            cursor = next_cursor
        yield from self.offset_rows(context, "nulls")

    def _cursor_of(self, row: dict) -> t.Tuple[int, str]:
        value = to_epoch_ms(row.get(self.replication_key))
        key = row.get(self.tiebreaker)
        if value is None or key is None:
            raise FatalAPIError(
                f"{self.name}: a keyset row lacks {self.replication_key} or "
                f"{self.tiebreaker}: {str(row)[:BODY_EXCERPT_LENGTH]}"
            )
        return value, str(key)

    def _is_row(self, row: dict, cursor: t.Tuple[int, str]) -> bool:
        return (
            to_epoch_ms(row.get(self.replication_key)) == cursor[0]
            and str(row.get(self.tiebreaker)) == cursor[1]
        )

    def _without_anchor(
        self, context: t.Optional[dict], rows: t.List[dict], cursor: t.Tuple[int, str]
    ) -> t.List[dict]:
        kept = [row for row in rows if not self._is_row(row, cursor)]
        if len(kept) < len(rows):
            return kept
        probe = self.post_page(context, {"mode": "probe", "tiebreak": cursor[1]})
        if probe and to_epoch_ms(probe[0].get(self.replication_key)) == cursor[0]:
            raise FatalAPIError(
                f"{self.name}: the API did not match the keyset cursor "
                f"{self.replication_key} = {self.format_cursor(parse_api_datetime(cursor[0]))}, "
                f"although row {self.tiebreaker} {cursor[1]} still has that "
                "value. It may read the DateTime filter format or time zone "
                "differently. Stopping so no rows are skipped."
            )
        return rows

    def where(self, conditions: t.List[dict], expression: str) -> dict:
        for alias, condition in zip("ABCDEFGH", conditions):
            condition["alias"] = alias
        return {"conditions": conditions, "expression": expression}

    def condition(self, name: str, operator: str, value: t.List[t.Any]) -> dict:
        return {"name": name, "alias": "", "value": value, "operator": operator}

    def prepare_request_payload(
        self, context: t.Optional[dict], next_page_token: t.Optional[dict]
    ) -> dict:
        token = next_page_token or {"mode": "offset", "offset": 0}
        mode = token["mode"]
        rk, tb = self.replication_key, self.tiebreaker
        payload = self.base_payload()
        order: t.Dict[str, str] = {}
        if mode == "keyset":
            cursor = token.get("cursor")
            if cursor is not None:
                value = self.format_cursor(parse_api_datetime(cursor[0]))
                payload["where"] = self.where(
                    [
                        self.condition(rk, "GT", [value]),
                        self.condition(rk, "EQ", [value]),
                        self.condition(tb, "GTE", [cursor[1]]),
                    ],
                    "A OR (B AND C)",
                )
            else:
                start = self.filter_start(context)
                if start:
                    first = self.condition(rk, "GTE", [self.format_cursor(start)])
                else:
                    first = self.condition(rk, "IS_NOT_NULL", [])
                payload["where"] = self.where([first], "A")
            order = {rk: "asc", tb: "asc"}
        elif mode == "nulls":
            payload["where"] = self.where([self.condition(rk, "IS_NULL", [])], "A")
            order = {tb: "asc"}
        elif mode == "probe":
            payload["select"] = [tb, rk]
            payload["where"] = self.where(
                [self.condition(tb, "EQ", [token["tiebreak"]])], "A"
            )
        elif self.can_sort_by_tiebreaker():
            order = {tb: "asc"}
        if order:
            payload["orderBy"] = order
        payload["limit"] = 1 if mode == "probe" else self.page_size
        payload["offset"] = token.get("offset", 0)
        return payload

