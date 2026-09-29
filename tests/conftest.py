"""Shared test harness: a fake Gainsight API built from the doc fixtures.

Every response shape comes from a file in tests/fixtures. SOURCES.json gives
the doc URL for each file. Where a test needs data the docs do not give,
such as a second object's describe, it copies a documented entry and changes
only the names. Helpers below say which fixture they start from.
"""

from __future__ import annotations

import copy
import datetime
import json
import pathlib
import random
import re
import time
import typing as t

import pytest
import requests_mock as requests_mock_lib

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
BASE_URL = "https://acme.gainsightcloud.com"
ACCESS_KEY = "test-access-key"
CONFIG = {"access_key": ACCESS_KEY, "domain": "acme.gainsightcloud.com"}
# M2M OAuth: the "OAuth API Key" and "OAuth API Secret" from Connectors 2.0.
CLIENT_ID = "test-oauth-api-key"
CLIENT_SECRET = "test-oauth-api-secret"
OAUTH_CONFIG = {"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "domain": "acme.gainsightcloud.com"}
TOKEN_URL = f"{BASE_URL}/v1/users/m2m/oauth/token"


def token_response(token: str = "token-1", expires_in: t.Any = 86400) -> dict:
    """A token reply in the documented "Get Access Token API" sample shape."""
    return {"access_token": token, "token_type": "Bearer", "expires_in": expires_in}


def load(name: str) -> t.Any:
    return json.loads((FIXTURES / name).read_text())


# Documented describe entry and fields, from "Post Describe OMD".
DOC_DESCRIBE_ENTRY = load("describe_response.json")["data"][0]
DOC_FIELDS = {f["fieldName"]: f for f in DOC_DESCRIBE_ENTRY["fields"]}


def doc_field(template: str, object_name: str, **overrides: t.Any) -> dict:
    """Copy a documented describe field and change names or type.

    `template` is a fieldName from the documented sample: Name__gc (STRING),
    Gsid (GSID), CreatedDate or ModifiedDate (DATETIME), CreatedBy or
    ModifiedBy (LOOKUP to gsuser).
    """
    field = copy.deepcopy(DOC_FIELDS[template])
    field["objectName"] = object_name
    meta_overrides = overrides.pop("meta", None)
    field.update(overrides)
    if "dataType" in overrides:
        field["meta"]["properties"]["sourceType"] = overrides["dataType"]
    if meta_overrides:
        field["meta"].update(meta_overrides)
    return field


def lookup_field(
    object_name: str, field_name: str, lookup_name: str, target: str
) -> dict:
    """Copy the documented CreatedBy lookup and point it at `target`."""
    field = doc_field("CreatedBy", object_name, fieldName=field_name, label=field_name)
    detail = field["meta"]["lookupDetail"]
    detail["lookupName"] = lookup_name
    detail["lookupObjects"][0]["objectName"] = target
    return field


def describe_entry(object_name: str, fields: t.List[dict]) -> dict:
    entry = copy.deepcopy(DOC_DESCRIBE_ENTRY)
    entry["objectName"] = object_name
    entry["label"] = object_name
    entry["fields"] = fields
    return entry


def standard_fields(object_name: str) -> t.List[dict]:
    """The documented Gsid, CreatedDate and ModifiedDate fields."""
    return [
        doc_field("Gsid", object_name),
        doc_field("CreatedDate", object_name),
        doc_field("ModifiedDate", object_name),
    ]


def company_fields() -> t.List[dict]:
    fields = standard_fields("company")
    fields += [
        doc_field("Name__gc", "company", fieldName="Name", label="Name"),
        # A custom field, as in the documented Name__gc.
        doc_field("Name__gc", "company", fieldName="Health_Notes__gc", label="Health Notes"),
        doc_field("Name__gc", "company", fieldName="ARR", label="ARR", dataType="CURRENCY"),
        doc_field("Name__gc", "company", fieldName="Renewal_Date", label="Renewal Date", dataType="DATE"),
        doc_field("Name__gc", "company", fieldName="Is_Active__gc", label="Active", dataType="BOOLEAN"),
        # A picklist with inline items in the dropdown API's item shape.
        doc_field(
            "Name__gc",
            "company",
            fieldName="Stage",
            label="Stage",
            dataType="PICKLIST",
            meta={
                "picklistItems": [
                    {"gsid": "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF3", "name": "Kicked Off"},
                    {"gsid": "1I0054U9FAKXZ0H26HO92M3F1G5SPWVQDNF4", "name": "Launched"},
                ]
            },
        ),
        # A picklist that only names its dropdown category.
        doc_field(
            "Name__gc",
            "company",
            fieldName="License_Type__gc",
            label="License Type",
            dataType="PICKLIST",
            meta={"categoryId": "1I00K3A4X4T2UWD3COJ3FU0KMKYXZL9WEEFK"},
        ),
        lookup_field("company", "Csm", "Csm__gr", "gsuser"),
        doc_field("CreatedBy", "company"),
    ]
    return fields


def gsuser_fields() -> t.List[dict]:
    return standard_fields("gsuser") + [
        doc_field("Name__gc", "gsuser", fieldName="Name", label="Name"),
        doc_field("Name__gc", "gsuser", fieldName="Email", label="Email", dataType="EMAIL"),
    ]


def company_person_fields() -> t.List[dict]:
    return standard_fields("company_person") + [
        lookup_field("company_person", "Company_ID", "Company_ID__gr", "company"),
    ]


def timeline_fields() -> t.List[dict]:
    """Documented Timeline field names on the documented describe entries.

    Gsid, CreatedDate and ModifiedDate are the standard system fields from
    the describe sample. The rest are from the Timeline Read and Insert
    samples. Whether a live activity_timeline describe has ModifiedDate is
    unconfirmed.
    """
    fields = standard_fields("activity_timeline")
    for name in ("contextname", "GsRelationshipId", "GsCompanyId", "AuthorId", "Subject", "Notes"):
        fields.append(doc_field("Name__gc", "activity_timeline", fieldName=name, label=name))
    fields.append(doc_field("Name__gc", "activity_timeline", fieldName="Ant__CustomNumber__c", dataType="NUMBER"))
    fields.append(doc_field("CreatedDate", "activity_timeline", fieldName="ActivityDate"))
    return fields


def cta_fields() -> t.List[dict]:
    return standard_fields("cs_cta") + [
        doc_field("Name__gc", "cs_cta", fieldName="Quoted_ARR__gc", dataType="NUMBER"),
        doc_field("Name__gc", "cs_cta", fieldName="customDate__gc", dataType="DATE"),
    ]


def default_describes() -> t.Dict[str, dict]:
    return {
        "company": describe_entry("company", company_fields()),
        "gsuser": describe_entry("gsuser", gsuser_fields()),
        "company_person": describe_entry("company_person", company_person_fields()),
        "activity_timeline": describe_entry("activity_timeline", timeline_fields()),
        "cs_cta": describe_entry("cs_cta", cta_fields()),
    }


def object_list(extra: t.Iterable[str] = ()) -> dict:
    """The documented object list, plus summaries copied for `extra` names."""
    payload = load("object_list_response.json")
    template = payload["data"][0]
    for name in extra:
        item = copy.deepcopy(template)
        item["objectName"] = name
        payload["data"].append(item)
    return payload


def query_page(rows: t.List[dict], records_shape: bool = False) -> dict:
    """A query success response in a documented shape."""
    if records_shape:
        payload = load("timeline_query_response.json")
        payload["data"]["records"] = rows
        return payload
    payload = load("company_query_response.json")
    payload["data"] = rows
    return payload


EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_WHERE_VALUE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})"
    r"(?:[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?)?"
    r"(Z|[+-]\d{2}:?\d{2})?$"
)


def to_ms(value: t.Any, naive_offset_hours: float = 0, ignore_offset: bool = False) -> t.Optional[int]:
    """Parse an epoch-ms number or a date string to epoch milliseconds.

    `naive_offset_hours` is the server's time zone for values with no offset.
    `ignore_offset` simulates a server that drops an explicit offset.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    match = _WHERE_VALUE.match(str(value))
    if not match:
        raise ValueError(f"Unparseable date value: {value!r}")
    year, month, day, hour, minute, second, fraction, offset = match.groups()
    moment = datetime.datetime(
        int(year), int(month), int(day), int(hour or 0), int(minute or 0), int(second or 0),
        int((fraction or "0").ljust(6, "0")), tzinfo=datetime.timezone.utc,
    )
    if offset and not ignore_offset:
        if offset != "Z":
            sign = 1 if offset[0] == "+" else -1
            digits = offset[1:].replace(":", "")
            moment -= sign * datetime.timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
    else:
        moment -= datetime.timedelta(hours=naive_offset_hours)
    return int((moment - EPOCH).total_seconds() * 1000)


class QueryEngine:
    """Evaluates a query body over rows, as the documented query API does.

    It honors `where` (conditions and `expression`), `orderBy`, `limit` and
    `offset`, or `pageSize` and `pageNumber` for the CTA APIs. Condition
    keys are `name` for MDA queries and `fieldName` for CTA calls.
    """

    def __init__(
        self,
        rows: t.List[dict],
        date_fields: t.Iterable[str],
        shape: str = "list",
        unordered: bool = False,
        naive_offset_hours: float = 0,
        ignore_offset: bool = False,
        grain_ms: t.Optional[int] = None,
        btw_whole_end_day: bool = False,
    ) -> None:
        """`grain_ms` truncates filter values, as a server comparing at a
        coarser grain would: 1000 for seconds, 86_400_000 for dates.
        `btw_whole_end_day` reads the second BTW value as the end of that day
        instead of its first instant.
        """
        self.grain_ms = grain_ms
        self.btw_whole_end_day = btw_whole_end_day
        self.rows = rows
        self.date_fields = set(date_fields)
        self.shape = shape
        self.unordered = unordered
        self.naive_offset_hours = naive_offset_hours
        self.ignore_offset = ignore_offset
        self.bodies: t.List[dict] = []
        self.before_request: t.Optional[t.Callable[[int, "QueryEngine"], None]] = None

    def _value(self, field_name: str, value: t.Any, from_row: bool) -> t.Any:
        if field_name in self.date_fields:
            if from_row:
                return to_ms(value)
            ms = to_ms(value, self.naive_offset_hours, self.ignore_offset)
            if self.grain_ms and ms is not None:
                ms -= ms % self.grain_ms
            return ms
        return value

    def _test(self, row: dict, condition: dict) -> bool:
        field_name = condition.get("name") or condition.get("fieldName")
        operator = condition["operator"]
        actual = self._value(field_name, row.get(field_name), True)
        if operator == "IS_NULL":
            return actual is None
        if operator == "IS_NOT_NULL":
            return actual is not None
        if actual is None:
            return False
        expected = [self._value(field_name, v, False) for v in condition["value"]]
        ops = {
            "EQ": lambda a, b: a == b[0],
            "NE": lambda a, b: a != b[0],
            "GT": lambda a, b: a > b[0],
            "GTE": lambda a, b: a >= b[0],
            "LT": lambda a, b: a < b[0],
            "LTE": lambda a, b: a <= b[0],
            "BTW": lambda a, b: b[0] <= a <= b[1],
        }
        if operator == "BTW" and self.btw_whole_end_day and field_name in self.date_fields:
            return expected[0] <= actual < expected[1] + 86_400_000
        return ops[operator](actual, expected)

    def _matches(self, row: dict, where: t.Optional[dict]) -> bool:
        if not where:
            return True
        if "(" in where["expression"] or ")" in where["expression"]:
            raise AssertionError(f"Undocumented parentheses in {where['expression']!r}")
        results = {c["alias"]: self._test(row, c) for c in where["conditions"]}
        tokens = re.findall(r"\(|\)|AND|OR|[A-Z]", where["expression"])
        python = " ".join(
            {"AND": "and", "OR": "or", "(": "(", ")": ")"}.get(tok, str(results.get(tok)))
            for tok in tokens
        )
        return bool(eval(python, {"__builtins__": {}}))

    def respond(self, body: dict) -> dict:
        self.bodies.append(body)
        if self.before_request:
            self.before_request(len(self.bodies), self)
        rows = [r for r in self.rows if self._matches(r, body.get("where"))]
        for key, direction in reversed(list((body.get("orderBy") or {}).items())):
            present = [r for r in rows if r.get(key) is not None]
            missing = [r for r in rows if r.get(key) is None]
            present.sort(key=lambda r: self._value(key, r[key], True), reverse=direction == "desc")
            rows = present + missing
        if self.unordered:
            random.Random(len(self.bodies)).shuffle(rows)
        if "pageSize" in body:
            size, number = body["pageSize"], body["pageNumber"]
            page = rows[(number - 1) * size : number * size]
        else:
            page = rows[body.get("offset", 0) : body.get("offset", 0) + body["limit"]]
        page = [{k: v for k, v in r.items()} for r in page]
        if self.shape == "records":
            return {"result": True, "errorCode": None, "errorDesc": None, "data": {"records": page}}
        return {"result": True, "errorCode": None, "errorDesc": None, "data": page}


class FakeGainsight:
    """A fake Gainsight tenant on a case-sensitive requests-mock Mocker.

    Metadata endpoints are registered up front. Object names must match the
    object list exactly, so a case mismatch fails the test.
    """

    def __init__(self, mocker: requests_mock_lib.Mocker) -> None:
        self.mocker = mocker
        self.describes = default_describes()
        self.failing: t.Dict[str, t.Tuple[int, dict]] = {}
        self.list_payload = object_list(extra=["activity_timeline", "cs_cta"])
        self.dropdown = load("dropdown_response.json")
        self.register()

    def register(self) -> None:
        self.mocker.get(
            f"{BASE_URL}/v1/meta/services/objects/list",
            json=lambda request, context: self.list_payload,
        )
        self.mocker.post(
            f"{BASE_URL}/v1/meta/services/objects/describe",
            json=self._describe,
        )
        self.mocker.get(
            requests_mock_lib.ANY,
            json=self._dropdown,
            additional_matcher=lambda r: "/v1/meta/services/dropdowns/" in r.url,
        )

    def _describe(self, request: t.Any, context: t.Any) -> dict:
        names = request.json()["objectNames"]
        for name in names:
            if name in self.failing:
                status, body = self.failing[name]
                context.status_code = status
                return body
        entries = [self.describes[n] for n in names if n in self.describes]
        if not entries:
            context.status_code = 400
            return load("describe_not_found_response.json")
        return {"requestId": "test", "result": True, "data": entries}

    def _dropdown(self, request: t.Any, context: t.Any) -> dict:
        return self.dropdown

    def describe_calls(self) -> t.List[t.List[str]]:
        return [
            r.json()["objectNames"]
            for r in self.mocker.request_history
            if r.path.endswith("/v1/meta/services/objects/describe")
        ]

    def serve(self, url: str, engine: QueryEngine) -> QueryEngine:
        """Answer POSTs to `url` from `engine`."""
        self.mocker.post(url, json=lambda request, context: engine.respond(request.json()))
        return engine


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> t.List[float]:
    """Record sleeps instead of sleeping. Covers backoff and the limiter.

    A fake monotonic clock advances by each sleep, so the rate limiter
    sees time pass in tests that make more than 100 calls.
    """
    recorded: t.List[float] = []
    clock = [time.monotonic()]

    def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(time, "sleep", fake_sleep)
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    return recorded


@pytest.fixture(autouse=True)
def _no_real_sleep(sleeps: t.List[float]) -> None:
    """Keep every test fast and offline."""


@pytest.fixture
def api() -> t.Iterator[FakeGainsight]:
    with requests_mock_lib.Mocker(case_sensitive=True) as mocker:
        yield FakeGainsight(mocker)


def make_tap(**config: t.Any) -> t.Any:
    from tap_gainsight.tap import TapGainsight

    state = config.pop("state", None)
    catalog = config.pop("catalog", None)
    return TapGainsight(
        config={**CONFIG, **config},
        state=state,
        catalog=catalog,
        parse_env_config=False,
    )


def make_oauth_tap(**config: t.Any) -> t.Any:
    """make_tap, with M2M OAuth credentials in place of the access key."""
    from tap_gainsight.tap import TapGainsight

    state = config.pop("state", None)
    catalog = config.pop("catalog", None)
    return TapGainsight(
        config={**OAUTH_CONFIG, **config},
        state=state,
        catalog=catalog,
        parse_env_config=False,
    )


def query_url(object_name: str) -> str:
    return f"{BASE_URL}/v1/data/objects/query/{object_name}"


def requests_to(mocker: requests_mock_lib.Mocker, path: str) -> t.List[t.Any]:
    return [r for r in mocker.request_history if r.path == path]


SENT_REQUESTS: t.List[t.Tuple[str, str, t.Any]] = []


@pytest.fixture(scope="session", autouse=True)
def every_request_is_on_the_allowlist() -> t.Iterator[None]:
    """Record every request the suite sends, and check each at the end.

    Requests are recorded at the transport adapters: the requests-mock
    adapter and the real HTTPAdapter. Every request that leaves a Session
    passes through one of them. A request the tap refuses never gets there.
    """
    import requests.adapters
    import requests_mock.adapter

    from tap_gainsight.safety import check_request

    originals = {}
    for cls in (requests_mock.adapter.Adapter, requests.adapters.HTTPAdapter):
        originals[cls] = cls.send

        def recording_send(self: t.Any, request: t.Any, *args: t.Any, _send=cls.send, **kwargs: t.Any) -> t.Any:
            SENT_REQUESTS.append((request.method, request.url, request.body))
            return _send(self, request, *args, **kwargs)

        cls.send = recording_send  # type: ignore[method-assign]
    try:
        yield
    finally:
        for cls, send in originals.items():
            cls.send = send  # type: ignore[method-assign]
    assert SENT_REQUESTS, "The suite sent no requests, so the check proved nothing."
    for method, url, body in SENT_REQUESTS:
        check_request(method, url, body)


@pytest.fixture(scope="session", autouse=True)
def no_real_network() -> t.Iterator[None]:
    """Refuse every real socket connect, so no test can reach the network."""
    import socket

    original = socket.socket.connect

    def refuse(self: socket.socket, address: t.Any) -> None:
        raise OSError(f"Tests may not open real connections, but one tried {address!r}.")

    socket.socket.connect = refuse  # type: ignore[method-assign]
    try:
        yield
    finally:
        socket.socket.connect = original  # type: ignore[method-assign]

