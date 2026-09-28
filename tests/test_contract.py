"""Contract tests: each request matches the documented request shape.

The documented shapes come from the verbatim request fixtures.
"""

import re

import pytest

from tests.conftest import ACCESS_KEY, load, make_tap, query_page, query_url

# Docs, Custom Object API, "enum operators ... supported in Read API".
DOCUMENTED_OPERATORS = {
    "EQ", "NE", "LT", "GT", "LTE", "GTE", "BTW", "IS_NULL", "IS_NOT_NULL",
    "CONTAINS", "DOES_NOT_CONTAINS", "STARTS_WITH", "ENDS_WITH",
    "INCLUDES", "IN", "EXCLUDES", "NOT_IN",
}
QUERY_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
CTA_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def documented_query_keys():
    keys = set()
    for name in (
        "company_query_request.json",
        "custom_object_query_request.json",
        "timeline_query_request.json",
        "delete_log_request.json",
    ):
        keys |= set(load(name))
    return keys


def documented_condition_keys(fixture, key="where"):
    conditions = load(fixture)[key]["conditions"]
    return [set(c) for c in conditions]


def only_request(api, path):
    requests = [r for r in api.mocker.request_history if r.path == path.lower()]
    assert requests, f"no request to {path}"
    return requests[0]


def assert_documented_headers(request):
    assert request.method == "POST"
    assert request.headers["accesskey"] == ACCESS_KEY
    assert request.headers["Content-Type"] == "application/json"


def assert_query_condition(condition, documented_keys):
    assert set(condition) in documented_keys
    assert condition["operator"] in DOCUMENTED_OPERATORS
    assert isinstance(condition["value"], list)


@pytest.mark.parametrize(
    "stream_name, object_name, request_fixture",
    [
        ("Company", "Company", "company_query_request.json"),
        ("Company_Person", "Company_Person", "custom_object_query_request.json"),
        ("timeline", "activity_timeline", "timeline_query_request.json"),
    ],
)
def test_mda_query_matches_the_documented_shape(api, stream_name, object_name, request_fixture):
    api.mocker.post(query_url(object_name), json=query_page([]))
    tap = make_tap(start_date="2024-02-05T08:34:35Z")
    stream = tap.streams[stream_name]
    stream.sync()

    request = only_request(api, f"/v1/data/objects/query/{object_name}")
    assert_documented_headers(request)
    body = request.json()
    documented = load(request_fixture)
    assert set(body) <= documented_query_keys()
    assert set(body) == set(documented)
    assert body["select"] == stream.select_paths()
    assert set(body["select"]) >= set(stream.plan.fields)
    assert body["limit"] == 5000 and body["offset"] == 0
    assert body["where"]["expression"] == "A"
    (condition,) = body["where"]["conditions"]
    assert_query_condition(condition, documented_condition_keys(request_fixture))
    assert condition == {
        "name": stream.replication_key,
        "alias": "A",
        "value": ["2024-02-05 08:34:35"],
        "operator": "GTE",
    }
    assert QUERY_DATETIME.match(condition["value"][0])
    assert list(body["orderBy"].values()) == ["asc"] * len(body["orderBy"])
    assert list(body["orderBy"])[0] == stream.replication_key


def test_full_table_query_has_no_where(api):
    api.mocker.post(query_url("Company"), json=query_page([]))
    make_tap().streams["Company"].sync()
    body = only_request(api, "/v1/data/objects/query/Company").json()
    assert "where" not in body
    assert body["orderBy"] == {"ModifiedDate": "asc", "Gsid": "asc"}


def test_company_select_includes_lookup_paths(api):
    api.mocker.post(query_url("Company"), json=query_page([]))
    make_tap().streams["Company"].sync()
    select = only_request(api, "/v1/data/objects/query/Company").json()["select"]
    assert {"Csm__gr.Name", "Csm__gr.Email", "CreatedBy__gr.Name", "Health_Notes__gc"} <= set(select)
    assert not any(name.endswith("_label") for name in select)


def test_cta_request_matches_the_documented_shape(api):
    api.mocker.post("https://acme.gainsightcloud.com/v2/cockpit/cta/list", json={"result": True, "data": []})
    tap = make_tap(start_date="2024-02-05T08:34:35Z")
    tap.streams["cta"].sync()

    request = only_request(api, "/v2/cockpit/cta/list")
    assert_documented_headers(request)
    body = request.json()
    documented = load("cta_list_request.json")
    assert set(body) <= set(documented)
    assert set(body) == {"select", "where", "pageSize", "pageNumber"}
    assert body["pageSize"] == 1000 and body["pageNumber"] == 1
    assert body["select"][:5] == ["name", "Comments", "CompanyId", "CreatedDate", "ModifiedDate"]
    (condition,) = body["where"]["conditions"]
    # Docs, Fetch CTA sample: {"fieldName", "value", "alias", "operator"}.
    assert set(condition) in documented_condition_keys("cta_list_request.json")
    assert condition == {
        "fieldName": "ModifiedDate",
        "value": ["2024-02-04"],
        "alias": "A",
        "operator": "GTE",
    }
    assert CTA_DATE.match(condition["value"][0])
    assert body["where"]["expression"] == "A"


def test_cta_modified_date_filter_is_documented():
    """The Retrieve Deleted Data page selects and filters CTAs on ModifiedDate."""
    documented = load("cta_deleted_list_request.json")
    assert "ModifiedDate" in documented["select"]
    assert documented["where"]["conditions"][0]["fieldName"] == "ModifiedDate"


def test_delete_log_request_matches_the_documented_shape(api):
    for log in ("record_delete_log", "record_delete_log_high_volume"):
        api.mocker.post(query_url(log), json=load("custom_object_query_empty_response.json"))
    tap = make_tap(start_date="2024-02-05T00:00:00Z")
    stream = tap.streams["deleted_records"]
    stream.sync()

    documented = load("delete_log_request.json")
    for log in ("record_delete_log", "record_delete_log_high_volume"):
        request = only_request(api, f"/v1/data/objects/query/{log}")
        assert_documented_headers(request)
        body = request.json()
        assert set(body) <= documented_query_keys()
        assert body["select"] == documented["select"]
        (condition,) = body["where"]["conditions"]
        assert set(condition) == set(documented["where"]["conditions"][0])
        assert condition["name"] == "DeletedOn"
        assert condition["value"] == documented["where"]["conditions"][0]["value"]
        assert condition["operator"] in DOCUMENTED_OPERATORS
        assert body["limit"] == 5000 and body["offset"] == 0
