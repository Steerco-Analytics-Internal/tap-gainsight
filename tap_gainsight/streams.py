"""Stream definitions for Gainsight CS."""

from __future__ import annotations

import datetime
import typing as t

from singer_sdk import typing as th

from tap_gainsight.client import (
    GainsightStream,
    RowCountOffsetPaginator,
    RowCountPageNumberPaginator,
    as_utc,
    format_query_datetime,
    is_date_type,
    json_schema_for,
)

if t.TYPE_CHECKING:
    from singer_sdk import Tap

# Suffix for the tap-made column that holds a picklist item's label.
LABEL_SUFFIX = "_label"

# Related fields to select through a lookup, by target object. Paths use the
# documented dot notation: "csm__gr.email" (Company API Read sample) and
# "OwnerId__gr.Email", "OwnerId__gr.Name", "CompanyId__gr.Name" (Fetch CTA
# and Retrieve Deleted Data samples).
LOOKUP_RELATED_FIELDS = {
    "gsuser": ("Name", "Email"),
    "company": ("Name",),
}

LABEL_SCHEMA = {"type": ["null", "string", "array"], "items": {"type": "string"}}


def _containers(field: dict) -> t.List[dict]:
    meta = field.get("meta") if isinstance(field.get("meta"), dict) else {}
    props = meta.get("properties") if isinstance(meta.get("properties"), dict) else {}
    return [field, meta, props]


def picklist_items(field: dict) -> t.Optional[t.Dict[str, t.Any]]:
    """Return {item GSID: label} from a describe field, if it carries items.

    The docs do not show a picklist field in a describe sample. This reads
    any list under a key that contains "picklist", whose entries have a
    `gsid`. That is the item shape of the documented dropdown API.
    """
    for container in _containers(field):
        for key, value in container.items():
            if "picklist" not in key.lower() or not isinstance(value, list):
                continue
            items = {
                str(item["gsid"]): item.get("name", item.get("label"))
                for item in value
                if isinstance(item, dict) and item.get("gsid")
            }
            if items:
                return items
    return None


def picklist_category_id(field: dict) -> t.Optional[str]:
    """Return a dropdown `categoryId` from a describe field, if present."""
    for container in _containers(field):
        value = container.get("categoryId")
        if isinstance(value, str) and value:
            return value
    return None


def lookup_columns(
    field: dict, related_fields: t.Mapping[str, t.Set[str]]
) -> t.List[str]:
    """Return dot-notation columns to select through a lookup field.

    A column is added only when the target object's describe has the field,
    so a select never names a field the tenant lacks.
    """
    meta = field.get("meta") if isinstance(field.get("meta"), dict) else {}
    detail = meta.get("lookupDetail")
    if not meta.get("hasLookup") or not isinstance(detail, dict):
        return []
    lookup_name = detail.get("lookupName")
    if not lookup_name:
        return []
    columns: t.List[str] = []
    for target in detail.get("lookupObjects") or []:
        target_name = str((target or {}).get("objectName", "")).lower()
        available = related_fields.get(target_name)
        for related in LOOKUP_RELATED_FIELDS.get(target_name, ()):
            if available is not None and related in available:
                columns.append(f"{lookup_name}.{related}")
    return columns


class ObjectPlan:
    """Schema and select plan for one MDA object, built from its describe."""

    def __init__(
        self,
        fields: t.Iterable[dict],
        related_fields: t.Optional[t.Mapping[str, t.Set[str]]] = None,
        dropdowns: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
    ) -> None:
        related_fields = related_fields or {}
        dropdowns = dropdowns or {}
        self.properties: t.Dict[str, dict] = {}
        self.fields: t.Dict[str, dict] = {}
        self.date_fields: t.Set[str] = set()
        self.lookup_columns: t.List[str] = []
        # label column -> (id field, {item GSID: label})
        self.labels: t.Dict[str, t.Tuple[str, t.Mapping[str, t.Any]]] = {}

        for field in fields:
            name = field.get("fieldName") if isinstance(field, dict) else None
            if not name:
                continue
            data_type = field.get("dataType")
            self.fields[name] = field
            self.properties[name] = json_schema_for(data_type)
            if is_date_type(data_type):
                self.date_fields.add(name)

        for name, field in list(self.fields.items()):
            for column in lookup_columns(field, related_fields):
                if column not in self.properties:
                    self.properties[column] = {"type": ["null", "string"]}
                    self.lookup_columns.append(column)
            items = picklist_items(field)
            if items is None:
                category = picklist_category_id(field)
                if category:
                    items = dropdowns.get(category)
            if items:
                label = f"{name}{LABEL_SUFFIX}"
                if label not in self.properties:
                    self.properties[label] = dict(LABEL_SCHEMA)
                    self.labels[label] = (name, items)

    @property
    def schema(self) -> dict:
        return {"type": "object", "properties": self.properties}

    def is_filterable_datetime(self, name: str) -> bool:
        field = self.fields.get(name)
        if not field or str(field.get("dataType", "")).upper() != "DATETIME":
            return False
        meta = field.get("meta") if isinstance(field.get("meta"), dict) else {}
        return meta.get("filterable", True) is not False

    def is_sortable(self, name: str) -> bool:
        field = self.fields.get(name)
        if not field:
            return False
        meta = field.get("meta") if isinstance(field.get("meta"), dict) else {}
        return meta.get("sortable", True) is not False


def resolve_label(value: t.Any, items: t.Mapping[str, t.Any]) -> t.Any:
    """Map a picklist GSID, or a list of them, to labels."""
    if value is None:
        return None
    if isinstance(value, list):
        return [items.get(str(item)) for item in value]
    if isinstance(value, str) and ";" in value:
        return [items.get(part.strip()) for part in value.split(";")]
    return items.get(str(value))


class MDAObjectStream(GainsightStream):
    """An MDA object, read with the Data Management query API.

    Docs: POST /v1/data/objects/query/{objectName} with select, where,
    orderBy, limit and offset. Max 5000 rows per call.
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Custom_Object_API/Gainsight_Custom_Object_API_Documentation
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Company_API_Documentation
    """

    replication_key_candidates: t.Tuple[str, ...] = ("ModifiedDate",)

    def __init__(
        self,
        tap: Tap,
        object_name: str,
        plan: ObjectPlan,
        name: t.Optional[str] = None,
        replication_key_candidates: t.Optional[t.Tuple[str, ...]] = None,
    ) -> None:
        self.object_name = object_name
        self.plan = plan
        self.date_fields = set(plan.date_fields)
        if replication_key_candidates is not None:
            self.replication_key_candidates = replication_key_candidates
        super().__init__(tap=tap, name=name or object_name, schema=plan.schema)

        self.primary_keys = ["Gsid"] if "Gsid" in plan.fields else []
        self.replication_key = next(
            (
                key
                for key in self.replication_key_candidates
                if plan.is_filterable_datetime(key)
            ),
            None,
        )

    @property
    def path(self) -> str:  # type: ignore[override]
        return f"/v1/data/objects/query/{self.object_name}"

    def get_new_paginator(self) -> RowCountOffsetPaginator:
        return RowCountOffsetPaginator(start_value=0, page_size=self.page_size)

    def select_paths(self) -> t.List[str]:
        """Return every selected field and lookup path, in schema order.

        A selected label column pulls in its id field, because the label is
        computed from the id. The SDK drops the id again if it is deselected.
        """
        paths: t.List[str] = []
        for name in self.plan.properties:
            if name in self.plan.labels:
                continue
            if self.is_property_selected(name):
                paths.append(name)
        for label, (id_field, _) in self.plan.labels.items():
            if self.is_property_selected(label) and id_field not in paths:
                paths.append(id_field)
        return paths

    def order_by(self) -> t.Dict[str, str]:
        order: t.Dict[str, str] = {}
        if self.replication_key:
            order[self.replication_key] = "asc"
        if self.plan.is_sortable("Gsid"):
            order["Gsid"] = "asc"
        return order

    def filter_value(self, context: t.Optional[dict]) -> t.Optional[str]:
        if not self.replication_key:
            return None
        start = self.get_starting_timestamp(context)
        return format_query_datetime(start) if start else None

    def prepare_request_payload(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any],
    ) -> dict:
        payload: t.Dict[str, t.Any] = {"select": self.select_paths()}
        start = self.filter_value(context)
        if start:
            payload["where"] = {
                "conditions": [
                    {
                        "name": self.replication_key,
                        "alias": "A",
                        "value": [start],
                        "operator": "GTE",
                    }
                ],
                "expression": "A",
            }
        order = self.order_by()
        if order:
            payload["orderBy"] = order
        payload["limit"] = self.page_size
        payload["offset"] = next_page_token or 0
        return payload

    def post_process(
        self, row: dict, context: t.Optional[dict] = None
    ) -> t.Optional[dict]:
        row = super().post_process(row, context) or row
        for label, (id_field, items) in self.plan.labels.items():
            row[label] = resolve_label(row.get(id_field), items)
        return row


class DeletedRecordsStream(GainsightStream):
    """Deleted-record log, for tombstoning. Deletes are kept for 15 days.

    Docs, Data Management APIs, "Retrieve Deleted Data API":
    POST /v1/data/objects/query/record_delete_log (low volume objects) and
    POST /v1/data/objects/query/record_delete_log_high_volume (high volume).
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs
    """

    name = "deleted_records"
    path = "/v1/data/objects/query/{delete_log}"
    primary_keys = ["ObjectName", "RecordId"]
    replication_key = "DeletedOn"
    date_fields = {"DeletedOn"}

    schema = th.PropertiesList(
        th.Property("RecordId", th.StringType),
        th.Property("DeletedOn", th.DateTimeType),
        th.Property("ObjectName", th.StringType),
    ).to_dict()

    @property
    def partitions(self) -> t.List[dict]:
        return [
            {"delete_log": "record_delete_log"},
            {"delete_log": "record_delete_log_high_volume"},
        ]

    def get_new_paginator(self) -> RowCountOffsetPaginator:
        return RowCountOffsetPaginator(start_value=0, page_size=self.page_size)

    def prepare_request_payload(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any],
    ) -> dict:
        payload: t.Dict[str, t.Any] = {
            "select": ["RecordId", "DeletedOn", "ObjectName"],
        }
        start = self.get_starting_timestamp(context)
        if start:
            payload["where"] = {
                "conditions": [
                    {
                        "name": "DeletedOn",
                        "value": [format_query_datetime(start)],
                        "alias": "A",
                        "operator": "GTE",
                    }
                ],
                "expression": "A",
            }
        payload["orderBy"] = {"DeletedOn": "asc"}
        payload["limit"] = self.page_size
        payload["offset"] = next_page_token or 0
        return payload


class CtaStream(GainsightStream):
    """Cockpit CTAs from the Fetch CTA API. Risks are CTAs of type Risk.

    Docs, Call To Action (CTA) API, "Fetch CTA API": POST /v2/cockpit/cta/list
    with select, where, pageSize and pageNumber. Max 1000 CTAs per request.
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Call_To_Action_(CTA)_API_Documentation
    Field names come from the Fetch CTA and Retrieve Deleted Data samples:
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Retrieve_Deleted_Data_API
    """

    name = "cta"
    path = "/v2/cockpit/cta/list"
    primary_keys = ["Gsid"]
    replication_key = "ModifiedDate"
    page_size = 1000
    date_fields = {"CreatedDate", "ModifiedDate"}

    # Fields the API returns by default, per the samples.
    base_schema = th.PropertiesList(
        th.Property("Gsid", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("TypeId", th.StringType),
        th.Property("TypeId__gr.Name", th.StringType),
        th.Property("StatusId", th.StringType),
        th.Property("StatusId__gr.Name", th.StringType),
        th.Property("PriorityId", th.StringType),
        th.Property("PriorityId__gr.Name", th.StringType),
        th.Property("ReasonId", th.StringType),
        th.Property("ReasonId__gr.Name", th.StringType),
        th.Property("CompanyId", th.StringType),
        th.Property("CompanyId__gr.Name", th.StringType),
        th.Property("OwnerId", th.StringType),
        th.Property("OwnerId__gr.Name", th.StringType),
        th.Property("OwnerId__gr.FirstName", th.StringType),
        th.Property("OwnerId__gr.LastName", th.StringType),
        th.Property("OwnerId__gr.Email", th.StringType),
        # The samples return DueDate as "2020-04-14T11:30:00Z" and "2024-02-19".
        th.Property("DueDate", th.StringType),
        th.Property("IsClosed", th.BooleanType),
        th.Property("IsImportant", th.BooleanType),
        th.Property("EntityType", th.StringType),
        th.Property("Comments", th.StringType),
        th.Property("CreatedDate", th.DateTimeType),
        th.Property("ModifiedDate", th.DateTimeType),
        th.Property("ModifiedById", th.StringType),
        th.Property(
            "associatedRecords",
            th.ArrayType(
                th.ObjectType(
                    th.Property("recordId", th.StringType),
                    th.Property("objectName", th.StringType),
                    th.Property("source", th.StringType),
                )
            ),
        ),
    ).to_dict()

    # Schema property -> select entry. The docs' samples select "name",
    # "Comments", "CompanyId" and "ModifiedDate". CreatedDate is the standard
    # MDA audit field from the describe sample. It is not in a CTA sample.
    base_select = {
        "Name": "name",
        "Comments": "Comments",
        "CompanyId": "CompanyId",
        "CreatedDate": "CreatedDate",
        "ModifiedDate": "ModifiedDate",
    }

    def __init__(
        self, tap: Tap, custom_fields: t.Optional[t.Iterable[dict]] = None
    ) -> None:
        properties = dict(self.base_schema["properties"])
        self.select_map = dict(self.base_select)
        date_fields = set(type(self).date_fields)
        for field in custom_fields or []:
            name = field.get("fieldName")
            if not name or name in properties:
                continue
            properties[name] = json_schema_for(field.get("dataType"))
            self.select_map[name] = name
            if is_date_type(field.get("dataType")):
                date_fields.add(name)
        self.date_fields = date_fields
        super().__init__(
            tap=tap, schema={"type": "object", "properties": properties}
        )

    def get_new_paginator(self) -> RowCountPageNumberPaginator:
        return RowCountPageNumberPaginator(start_value=1, page_size=self.page_size)

    def filter_value(self, context: t.Optional[dict]) -> t.Optional[str]:
        """Return the `yyyy-MM-dd` lower bound for ModifiedDate.

        The CTA samples filter ModifiedDate with date-only values. The tap
        goes back one day from the bookmark, so a tenant time zone ahead of
        or behind UTC can never skip a row. Targets dedupe the overlap.
        """
        start = self.get_starting_timestamp(context)
        if not start:
            return None
        day = (as_utc(start) - datetime.timedelta(days=1)).date()
        return day.isoformat()

    def prepare_request_payload(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[t.Any],
    ) -> dict:
        select = [
            entry
            for prop, entry in self.select_map.items()
            if self.is_property_selected(prop)
        ]
        payload: t.Dict[str, t.Any] = {"select": select}
        start = self.filter_value(context)
        if start:
            payload["where"] = {
                "conditions": [
                    {
                        "fieldName": "ModifiedDate",
                        "value": [start],
                        "alias": "A",
                        "operator": "GTE",
                    }
                ],
                "expression": "A",
            }
        payload["pageSize"] = self.page_size
        payload["pageNumber"] = next_page_token or 1
        return payload
