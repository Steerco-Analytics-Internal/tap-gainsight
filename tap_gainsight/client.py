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

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # Python before 3.9.
    from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # type: ignore
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
                f"needs: {str(row)[:BODY_EXCERPT_LENGTH]}"
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
