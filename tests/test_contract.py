"""Contract tests: each request matches the documented request shape.

The documented shapes come from the verbatim request fixtures. Where the
tap goes beyond a sample, such as the keyset expression, the test says so.
"""

import re

import pytest

from tests.conftest import (
    ACCESS_KEY,
    BASE_URL,
    QueryEngine,
    load,
    make_tap,
    query_url,
)
from tests.test_streams import company_row, cta_row, deleted_row, timeline_row

# Docs, Custom Object API, "enum operators ... supported in Read API".
DOCUMENTED_OPERATORS = {
    "EQ", "NE", "LT", "GT", "LTE", "GTE", "BTW", "IS_NULL", "IS_NOT_NULL",
    "CONTAINS", "DOES_NOT_CONTAINS", "STARTS_WITH", "ENDS_WITH",
    "INCLUDES", "IN", "EXCLUDES", "NOT_IN",
}
# Retrieve Deleted Data API sample: "2024-02-05 00:00:00".
QUERY_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
# CTA and Task samples: date-only values, as in "2020-04-14".
DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Documented expressions join aliases with AND, as in "A AND B". The tap
# sends at most three terms. The third is the one extension beyond the docs.
EXPRESSION = re.compile(r"^[A-Z](?: AND [A-Z]){0,2}$")


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


def documented_condition_key_sets(fixture):
    return [set(c) for c in load(fixture)["where"]["conditions"]]


def requests_to(api, path):
    found = [r for r in api.mocker.request_history if r.path == path]
    assert found, f"no request to {path}"
    return found


def assert_documented_headers(request):
    assert request.method == "POST"
    assert request.headers["accesskey"] == ACCESS_KEY
    assert request.headers["Content-Type"] == "application/json"


def assert_where(where, key_sets, date_format):
    assert EXPRESSION.match(where["expression"])
    for condition in where["conditions"]:
        assert set(condition) in key_sets
        assert condition["operator"] in DOCUMENTED_OPERATORS
        assert isinstance(condition["value"], list)
        if condition["operator"] in {"IS_NULL", "IS_NOT_NULL"}:
            assert condition["value"] == []
    for condition in where["conditions"]:
        if condition.get("name", condition.get("fieldName")) in {"ModifiedDate", "DeletedOn"}:
            assert all(date_format.match(value) for value in condition["value"])


@pytest.mark.parametrize(
    "stream_name, object_name, request_fixture, make_row, shape",
    [
        ("Company", "Company", "company_query_request.json", company_row, "list"),
        ("timeline", "activity_timeline", "timeline_query_request.json", timeline_row, "records"),
    ],
)
def test_mda_queries_match_the_documented_shape(api, stream_name, object_name, request_fixture, make_row, shape):
    rows = [make_row(i) for i in range(3)]
    engine = api.serve(query_url(object_name), QueryEngine(rows, {"ModifiedDate"}, shape=shape))

    def move_second_row(request_number, engine):
        if request_number == 2:
            rows[1]["ModifiedDate"] += 99_000

    engine.before_request = move_second_row
    tap = make_tap(start_date="2024-02-05T08:34:35Z")
    stream = tap.streams[stream_name]
    stream.page_size = 2
    stream.sync()

    requests = requests_to(api, f"/v1/data/objects/query/{object_name}")
    documented = load(request_fixture)
    key_sets = documented_condition_key_sets(request_fixture)
    for request in requests:
        assert_documented_headers(request)
        body = request.json()
        assert set(body) <= documented_query_keys()
        assert_where(body["where"], key_sets, QUERY_DATETIME)
        assert body["offset"] == 0

    first, drain, probe = (r.json() for r in requests[:3])
    # First page: the documented body keys, filtered from the start less 24 hours.
    assert set(first) == set(documented)
    assert first["select"] == stream.select_paths()
    assert set(first["select"]) >= set(stream.plan.fields)
    assert first["limit"] == 2
    assert first["where"] == {
        "conditions": [{"name": "ModifiedDate", "alias": "A", "value": ["2024-02-04 08:34:35"], "operator": "GTE"}],
        "expression": "A",
    }
    assert first["orderBy"] == {"ModifiedDate": "asc", "Gsid": "asc"}
    # Drain of the page's last second, ordered by Gsid.
    assert drain["where"] == {
        "conditions": [
            {"name": "ModifiedDate", "alias": "A", "value": ["2024-02-05 08:24:36"], "operator": "GTE"},
            {"name": "ModifiedDate", "alias": "B", "value": ["2024-02-05 08:24:37"], "operator": "LT"},
        ],
        "expression": "A AND B",
    }
    assert drain["orderBy"] == {"Gsid": "asc"}
    # The row listed in that second moved, so the tap reads it by Gsid.
    assert probe["select"] == ["Gsid", "ModifiedDate"]
    assert [(c["name"], c["operator"]) for c in probe["where"]["conditions"]] == [("Gsid", "EQ")]
    assert probe["orderBy"] == {"ModifiedDate": "asc"}
    # Null pass last.
    last = requests[-1].json()
    assert last["where"]["conditions"][0]["operator"] == "IS_NULL"
    assert last["orderBy"] == {"Gsid": "asc"}


def test_a_three_term_drain_is_the_widest_request(api):
    rows = [company_row(i, modified=1707121475253) for i in range(5)]
    engine = api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}))
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    stream.sync()
    widest = max(engine.bodies, key=lambda b: len(b["where"]["conditions"]))
    assert widest["where"]["expression"] == "A AND B AND C"
    assert [(c["name"], c["operator"]) for c in widest["where"]["conditions"]] == [
        ("ModifiedDate", "GTE"), ("ModifiedDate", "LT"), ("Gsid", "GT"),
    ]


def test_full_table_object_pages_by_gsid(api):
    from tests.conftest import describe_entry, doc_field

    api.describes["obj1__gc"] = describe_entry("obj1__gc", [doc_field("Gsid", "obj1__gc")])
    engine = api.serve(query_url("obj1__gc"), QueryEngine([{"Gsid": f"G{i}"} for i in range(3)], set()))
    stream = make_tap().streams["obj1__gc"]
    stream.page_size = 2
    assert len(list(stream.get_records(None))) == 3
    assert [(b.get("where"), b["orderBy"], b["offset"]) for b in engine.bodies] == [
        (None, {"Gsid": "asc"}, 0),
        ({"conditions": [{"name": "Gsid", "alias": "A", "value": ["G1"], "operator": "GT"}], "expression": "A"}, {"Gsid": "asc"}, 0),
    ]


def test_company_select_includes_lookup_paths(api):
    engine = api.serve(query_url("Company"), QueryEngine([], {"ModifiedDate"}))
    list(make_tap().streams["Company"].get_records(None))
    select = engine.bodies[0]["select"]
    assert {"Csm__gr.Name", "Csm__gr.Email", "CreatedBy__gr.Name", "Health_Notes__gc"} <= set(select)
    assert not any(name.endswith("_label") for name in select)


@pytest.mark.parametrize(
    "stream_name, path, documented_select",
    [
        ("cta", "/v2/cockpit/cta/list", None),
        ("cta_deleted", "/v2/cockpit/cta/deleted/list", load("cta_deleted_list_request.json")["select"]),
    ],
)
def test_cta_requests_match_the_documented_shape(api, stream_name, path, documented_select):
    engine = api.serve(f"{BASE_URL}{path}", QueryEngine([cta_row(1)], {"ModifiedDate"}, unordered=True))
    make_tap(start_date="2024-02-05T08:34:35Z").streams[stream_name].sync()
    # Fetch CTA sample condition: {"fieldName", "value", "alias", "operator"}.
    key_sets = documented_condition_key_sets("cta_list_request.json")
    documented_keys = set(load("cta_list_request.json")) | set(load("cta_deleted_list_request.json"))
    for request in requests_to(api, path):
        assert_documented_headers(request)
        body = request.json()
        assert set(body) <= documented_keys
        assert set(body) == {"select", "where", "pageSize", "pageNumber"}
        assert body["pageSize"] == 1000 and body["pageNumber"] == 1
        assert_where(body["where"], key_sets, DATE_ONLY)
        if documented_select is not None:
            assert body["select"] == documented_select
    first = engine.bodies[0]
    # The documented CTA filter: BTW with date-only values, as in the
    # Fetch CTA "Between" sample and the deleted-Task sample.
    assert first["where"] == {
        "conditions": [
            {"fieldName": "ModifiedDate", "value": ["2024-02-04", "2024-02-05"], "alias": "A", "operator": "BTW"},
        ],
        "expression": "A",
    }


def test_cta_select_starts_with_the_documented_fields(api):
    engine = api.serve(f"{BASE_URL}/v2/cockpit/cta/list", QueryEngine([], {"ModifiedDate"}))
    make_tap(start_date="2026-09-01T00:00:00Z").streams["cta"].sync()
    select = engine.bodies[0]["select"]
    assert select[:6] == ["name", "Comments", "CompanyId", "CompanyId__gr.Name", "CreatedDate", "ModifiedDate"]
    # The Fetch CTA sample selects "name" and "CompanyId__gr.Name".
    assert {"name", "CompanyId__gr.Name"} <= set(load("cta_list_request.json")["select"])


def test_cta_modified_date_filter_uses_the_documented_condition_keys(api):
    """The deleted-CTA sample filters ModifiedDate with fieldName conditions."""
    engine = api.serve(f"{BASE_URL}/v2/cockpit/cta/deleted/list", QueryEngine([], {"ModifiedDate"}))
    make_tap(start_date="2026-09-01T00:00:00Z").streams["cta_deleted"].sync()
    documented = load("cta_deleted_list_request.json")["where"]["conditions"][0]
    sent = engine.bodies[0]["where"]["conditions"][0]
    assert sent["fieldName"] == documented["fieldName"] == "ModifiedDate"
    assert set(sent) - {"value"} == set(documented) - {"literal"}


def test_delete_log_requests_match_the_documented_shape(api):
    for log in ("record_delete_log", "record_delete_log_high_volume"):
        rows = [deleted_row(i) for i in range(3)] if log == "record_delete_log" else []
        api.serve(query_url(log), QueryEngine(rows, {"DeletedOn"}, shape="records"))
    stream = make_tap(start_date="2024-02-06T00:00:00Z").streams["deleted_records"]
    stream.page_size = 2
    stream.sync()

    documented = load("delete_log_request.json")
    key_sets = documented_condition_key_sets("delete_log_request.json")
    for log in ("record_delete_log", "record_delete_log_high_volume"):
        requests = requests_to(api, f"/v1/data/objects/query/{log}")
        for request in requests:
            assert_documented_headers(request)
            body = request.json()
            assert set(body) <= documented_query_keys()
            assert body["select"] == documented["select"]
            assert_where(body["where"], key_sets, QUERY_DATETIME)
        first = requests[0].json()
        (condition,) = first["where"]["conditions"]
        assert condition == {"name": "DeletedOn", "alias": "A", "value": ["2024-02-05 00:00:00"], "operator": "GTE"}
        assert first["orderBy"] == {"DeletedOn": "asc", "RecordId": "asc"}
