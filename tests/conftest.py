"""Shared test harness: a fake Gainsight API built from the doc fixtures.

Every response shape comes from a file in tests/fixtures. SOURCES.json gives
the doc URL for each file. Where a test needs data the docs do not give,
such as a second object's describe, it copies a documented entry and changes
only the names. Helpers below say which fixture they start from.
"""

from __future__ import annotations

import copy
import json
import pathlib
import time
import typing as t

import pytest
import requests_mock as requests_mock_lib

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
BASE_URL = "https://acme.gainsightcloud.com"
ACCESS_KEY = "test-access-key"
CONFIG = {"access_key": ACCESS_KEY, "domain": "acme.gainsightcloud.com"}


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
    """Field names from the Timeline Read API sample request."""
    fields = [
        doc_field("Gsid", "activity_timeline"),
        doc_field("CreatedDate", "activity_timeline"),
        doc_field("ModifiedDate", "activity_timeline", fieldName="LastModifiedDate"),
    ]
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


class FakeGainsight:
    """Registers the metadata endpoints on a requests-mock Mocker."""

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
            if name.lower() in self.failing:
                status, body = self.failing[name.lower()]
                context.status_code = status
                return body
        entries = [
            self.describes[n.lower()] for n in names if n.lower() in self.describes
        ]
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


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> t.List[float]:
    """Record sleeps instead of sleeping. Covers backoff and the limiter."""
    recorded: t.List[float] = []
    monkeypatch.setattr(time, "sleep", lambda seconds: recorded.append(seconds))
    return recorded


@pytest.fixture(autouse=True)
def _no_real_sleep(sleeps: t.List[float]) -> None:
    """Keep every test fast and offline."""


@pytest.fixture
def api() -> t.Iterator[FakeGainsight]:
    with requests_mock_lib.Mocker() as mocker:
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


def query_url(object_name: str) -> str:
    return f"{BASE_URL}/v1/data/objects/query/{object_name}"


def requests_to(mocker: requests_mock_lib.Mocker, path: str) -> t.List[t.Any]:
    return [r for r in mocker.request_history if r.path == path.lower()]
