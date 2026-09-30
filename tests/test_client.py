"""Unit tests for the HTTP layer."""

import datetime

import pytest
import requests_mock as requests_mock_lib
from singer_sdk.exceptions import FatalAPIError

from tap_gainsight import client
from tap_gainsight.client import (
    GainsightAPIError,
    GainsightAuthError,
    GainsightMetadataClient,
    RateLimiter,
    extract_rows,
    format_query_datetime,
    json_schema_for,
    normalize_domain,
    to_iso_datetime,
)
from tests.conftest import ACCESS_KEY, BASE_URL, CONFIG, load


@pytest.mark.parametrize(
    "value, expected",
    [
        ("acme.gainsightcloud.com", "https://acme.gainsightcloud.com"),
        ("https://acme.gainsightcloud.com", "https://acme.gainsightcloud.com"),
        ("HTTPS://ACME.GainsightCloud.com", "https://acme.gainsightcloud.com"),
        ("acme", "https://acme.gainsightcloud.com"),
        ("eu.acme.gainsightcloud.com", "https://eu.acme.gainsightcloud.com"),
        ("  acme.gainsightcloud.com  ", "https://acme.gainsightcloud.com"),
        ("\tacme.gainsightcloud.com\r\n", "https://acme.gainsightcloud.com"),
        ("acme.gainsightcloud.com\u00a0", "https://acme.gainsightcloud.com"),
        ("https://acme.gainsightcloud.com/", "https://acme.gainsightcloud.com"),
        ("acme.gainsightcloud.com//", "https://acme.gainsightcloud.com"),
        (" https://acme.gainsightcloud.com/ ", "https://acme.gainsightcloud.com"),
    ],
)
def test_normalize_domain(value, expected):
    assert normalize_domain(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "/",
        "https://",
        "https:///",
        "http://acme.gainsightcloud.com",
        "acme .gainsightcloud.com",
        "acme.gainsightcloud.com /",
        "https://acme.gainsightcloud.com/v1/ui/home",
        "acme.gainsightcloud.com\u200b",
    ],
)
def test_normalize_domain_rejects_empty(value):
    with pytest.raises(ValueError):
        normalize_domain(value)


# Every data type the docs list: STRING, GSID, DATETIME and LOOKUP from the
# describe sample, and the operator table's String, Boolean, Date, DateTime,
# Number, GSID, Email, Dropdown List, Multi select dropdown list, Currency
# and Percentage. The docs do not give describe enums for the dropdown
# types, so the tap treats any unknown type as "any JSON value".
@pytest.mark.parametrize(
    "data_type, expected",
    [
        ("STRING", {"type": ["null", "string"]}),
        ("GSID", {"type": ["null", "string"]}),
        ("LOOKUP", {"type": ["null", "string"]}),
        ("EMAIL", {"type": ["null", "string"]}),
        ("URL", {"type": ["null", "string"]}),
        ("RICHTEXTAREA", {"type": ["null", "string"]}),
        ("DATE", {"type": ["null", "string"]}),
        ("DATETIME", {"type": ["null", "string"], "format": "date-time"}),
        ("NUMBER", {"type": ["null", "number"]}),
        ("CURRENCY", {"type": ["null", "number"]}),
        ("PERCENTAGE", {"type": ["null", "number"]}),
        ("BOOLEAN", {"type": ["null", "boolean"]}),
        ("datetime", {"type": ["null", "string"], "format": "date-time"}),
        ("PICKLIST", {"type": ["null", "string"]}),
        ("MULTISELECTDROPDOWNLIST", {"type": ["null", "string"]}),
        ("SOMETHING_NEW", {"type": ["null", "string"]}),
        (None, {"type": ["null", "string"]}),
    ],
)
def test_json_schema_for_every_documented_type(data_type, expected):
    schema = json_schema_for(data_type)
    assert schema == expected
    assert "null" in schema["type"]


def test_extract_rows_accepts_every_documented_shape():
    assert len(extract_rows(load("company_query_response.json"))) == 3
    assert len(extract_rows(load("custom_object_query_response.json"))) == 3
    assert len(extract_rows(load("timeline_query_response.json"))) == 1
    assert len(extract_rows(load("delete_log_response.json"))) == 4
    assert len(extract_rows(load("cta_list_response.json"))) == 2
    assert extract_rows(load("custom_object_query_empty_response.json")) == []
    assert extract_rows(load("company_query_no_data_response.json")) == []
    assert extract_rows({"result": True, "data": None}) == []
    assert extract_rows({"result": True, "data": {"records": None}}) == []


@pytest.mark.parametrize(
    "payload",
    [[], "text", {"data": "Delete process initiated"}, {"data": {"rows": []}}],
)
def test_extract_rows_rejects_unknown_shapes(payload):
    with pytest.raises(FatalAPIError):
        extract_rows(payload)


def test_to_iso_datetime():
    assert to_iso_datetime(1521743018667) == "2018-03-22T18:23:38.667000+00:00"
    assert to_iso_datetime("2024-02-05T08:34:35.253Z") == "2024-02-05T08:34:35.253Z"
    assert to_iso_datetime(None) is None
    assert to_iso_datetime(True) is True


def test_format_query_datetime_matches_the_delete_log_sample_format():
    documented = load("delete_log_request.json")["where"]["conditions"][0]["value"][0]
    ours = format_query_datetime(datetime.datetime(2024, 2, 5))
    assert ours == documented == "2024-02-05 00:00:00"
    aware = datetime.datetime(
        2024, 2, 5, 10, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=5))
    )
    assert format_query_datetime(aware) == "2024-02-05 05:00:00"


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def test_rate_limiter_allows_the_documented_100_calls_per_minute():
    fake = FakeClock()
    limiter = RateLimiter(clock=fake.clock, sleep=fake.sleep)
    assert limiter.calls == 100 and limiter.period == 60
    for _ in range(100):
        assert limiter.acquire() == 0
    assert fake.sleeps == []
    assert limiter.acquire() == 60
    assert fake.sleeps == [60]


def test_rate_limiter_waits_only_until_the_oldest_call_expires():
    fake = FakeClock()
    limiter = RateLimiter(calls=2, period=10, clock=fake.clock, sleep=fake.sleep)
    limiter.acquire()
    fake.now = 4
    limiter.acquire()
    fake.now = 7
    assert limiter.acquire() == pytest.approx(3)
    fake.now = 30
    assert limiter.acquire() == 0


def test_rate_limiter_rejects_bad_limits():
    with pytest.raises(ValueError):
        RateLimiter(calls=0)
    with pytest.raises(ValueError):
        RateLimiter(period=0)


def metadata_client():
    return GainsightMetadataClient(CONFIG, RateLimiter())


def test_metadata_client_sends_the_access_key_header():
    with requests_mock_lib.Mocker() as m:
        m.get(f"{BASE_URL}/v1/meta/services/objects/list", json=load("object_list_response.json"))
        objects = metadata_client().list_objects()
        assert len(objects) == 11
        request = m.request_history[0]
        assert request.headers["accesskey"] == ACCESS_KEY
        assert request.qs == {"po": ["company"], "em": ["false"]}


@pytest.mark.parametrize(
    "status, body",
    [
        (401, {"message": "no"}),
        (403, {"message": "no"}),
        (200, load("unauthorized_response.json")),
        (400, load("unauthorized_response.json")),
    ],
)
def test_metadata_auth_failure_fails_fast(status, body):
    with requests_mock_lib.Mocker() as m:
        m.get(f"{BASE_URL}/v1/meta/services/objects/list", status_code=status, json=body)
        with pytest.raises(GainsightAuthError, match="rejected the access key"):
            metadata_client().list_objects()
        assert m.call_count == 1


def test_metadata_retries_429_then_succeeds(sleeps):
    with requests_mock_lib.Mocker() as m:
        m.get(
            f"{BASE_URL}/v1/meta/services/objects/list",
            [
                {"status_code": 429, "text": "slow down"},
                {"status_code": 503, "text": "busy"},
                {"json": load("object_list_response.json")},
            ],
        )
        assert len(metadata_client().list_objects()) == 11
        assert m.call_count == 3
        assert len(sleeps) == 2


def test_metadata_gives_up_after_max_tries():
    with requests_mock_lib.Mocker() as m:
        m.get(f"{BASE_URL}/v1/meta/services/objects/list", status_code=500, text="boom")
        with pytest.raises(GainsightAPIError, match="500 from the object list: HTTP 500, a 4-byte body"):
            metadata_client().list_objects()
        assert m.call_count == client.MAX_TRIES


def test_metadata_connection_errors_are_retried_then_raised():
    import requests

    with requests_mock_lib.Mocker() as m:
        m.get(
            f"{BASE_URL}/v1/meta/services/objects/list",
            exc=requests.exceptions.ConnectionError("down"),
        )
        with pytest.raises(GainsightAPIError, match="failed: down"):
            metadata_client().list_objects()
        assert m.call_count == client.MAX_TRIES


def test_metadata_timeouts_are_retried_then_raised():
    import requests

    with requests_mock_lib.Mocker() as m:
        m.get(
            f"{BASE_URL}/v1/meta/services/objects/list",
            exc=requests.exceptions.ReadTimeout("slow"),
        )
        with pytest.raises(GainsightAPIError, match="slow"):
            metadata_client().list_objects()
        assert m.call_count == client.MAX_TRIES


@pytest.mark.parametrize(
    "status, kwargs, message",
    [
        (400, {"json": load("describe_not_found_response.json")}, "400 from the object list"),
        (200, {"text": "<html>not json</html>"}, "did not return a JSON object"),
        (200, {"json": {"result": False, "errorDesc": "bad"}}, "the object list failed"),
        (200, {"json": {"result": True, "data": {}}}, "no `data` list"),
    ],
)
def test_metadata_failures_raise_with_body(status, kwargs, message):
    with requests_mock_lib.Mocker() as m:
        m.get(f"{BASE_URL}/v1/meta/services/objects/list", status_code=status, **kwargs)
        with pytest.raises(GainsightAPIError, match=message):
            metadata_client().list_objects()


def test_describe_sends_the_documented_body():
    documented = load("describe_request.json")
    with requests_mock_lib.Mocker() as m:
        m.post(
            f"{BASE_URL}/v1/meta/services/objects/describe",
            json=load("describe_response.json"),
        )
        described = metadata_client().describe(["customobj__gc"])
        assert m.request_history[0].json() == documented
        assert set(described) == {"customobj__gc"}
        assert [f["fieldName"] for f in described["customobj__gc"]["fields"]] == [
            "Name__gc", "Gsid", "CreatedDate", "ModifiedDate", "CreatedBy", "ModifiedBy",
        ]


def test_describe_accepts_a_single_object_data():
    entry = load("describe_response.json")["data"][0]
    with requests_mock_lib.Mocker() as m:
        m.post(
            f"{BASE_URL}/v1/meta/services/objects/describe",
            json={"result": True, "data": entry},
        )
        assert set(metadata_client().describe(["customobj__gc"])) == {"customobj__gc"}


def test_dropdown_items_reads_the_documented_response():
    with requests_mock_lib.Mocker() as m:
        m.get(
            f"{BASE_URL}/v1/meta/services/dropdowns/1I00K3A4X4T2UWD3COJ3FU0KMKYXZL9WEEFK",
            json=load("dropdown_response.json"),
        )
        items = metadata_client().dropdown_items("1I00K3A4X4T2UWD3COJ3FU0KMKYXZL9WEEFK")
        assert items["1I00AABNSR5CLLWSP3IK3IVPS8983FQVHJ2H"] == "Viewer"
        assert len(items) == 5


class SpinGuard(RuntimeError):
    pass


def test_rate_limiter_makes_progress_when_float_rounding_leaves_a_sliver():
    """CI hung here. From a clock start of 73.991..., after sleeping the
    computed delay the oldest send is 59.999999999999986 seconds old, a hair
    under the period. The old loop then slept 1.4e-14 seconds, which cannot
    move a float clock near 134, and spun forever. The fake clock starts at
    the machine's uptime, so only a freshly booted CI runner hit it.
    """
    now = [73.99103330961584]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 1000:
            raise SpinGuard("the limiter is spinning without progress")
        now[0] += seconds

    limiter = RateLimiter(calls=30, period=60, clock=lambda: now[0], sleep=sleep)
    for _ in range(80):
        limiter.acquire()
    # 80 calls at 30 a minute: the 31st and 61st calls each wait a window.
    assert len(sleeps) == 2
    assert all(seconds == pytest.approx(60) for seconds in sleeps)
