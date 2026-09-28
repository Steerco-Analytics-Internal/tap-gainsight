"""Stream definitions for Gainsight CS."""

from __future__ import annotations

import datetime
import typing as t
from urllib.parse import urlparse

from singer_sdk import typing as th

from singer_sdk.exceptions import FatalAPIError

from tap_gainsight.client import (
    GainsightStream,
    KeysetStream,
    format_api_datetime,
    format_query_datetime,
    is_date_type,
    is_object_not_found,
    json_schema_for,
    parse_api_datetime,
    utc_now,
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


def is_retired_field(field: dict) -> bool:
    """Return True for a describe field flagged hidden or deleted.

    The describe sample's field `meta` has `hidden`. Its lookup details
    carry `deleted`. Either flag, on the field or its meta, leaves the field
    out of the schema and the select list.
    """
    meta = field.get("meta") if isinstance(field.get("meta"), dict) else {}
    return any(
        container.get(flag) is True
        for container in (field, meta)
        for flag in ("hidden", "deleted")
    )


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
            if not name or is_retired_field(field):
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


class MDAObjectStream(KeysetStream):
    """An MDA object, read with the Data Management query API.

    Docs: POST /v1/data/objects/query/{objectName} with select, where,
    orderBy, limit and offset. Max 5000 rows per call.
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Custom_Object_API/Gainsight_Custom_Object_API_Documentation
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Company_API_Documentation
    Paging is keyset on (ModifiedDate, Gsid). See KeysetStream.
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

        has_gsid = "Gsid" in plan.fields
        self.primary_keys = ["Gsid"] if has_gsid else []
        # Keyset paging needs Gsid as the tie-breaker. Without it the stream
        # is full table.
        self.replication_key = next(
            (
                key
                for key in self.replication_key_candidates
                if has_gsid and plan.is_filterable_datetime(key)
            ),
            None,
        )

    @property
    def path(self) -> str:  # type: ignore[override]
        return f"/v1/data/objects/query/{self.object_name}"

    def can_sort_by_tiebreaker(self) -> bool:
        return self.plan.is_sortable(self.tiebreaker)

    def select_paths(self) -> t.List[str]:
        """Return every selected field and lookup path, in schema order.

        A selected label column pulls in its id field, because the label is
        computed from the id. The SDK drops the id again if it is deselected.
        The key and tie-breaker are always selected, for paging.
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
        for required in (self.tiebreaker, self.replication_key):
            if required and required in self.plan.fields and required not in paths:
                paths.append(required)
        return paths

    def base_payload(self) -> t.Dict[str, t.Any]:
        return {"select": self.select_paths()}

    def post_process(
        self, row: dict, context: t.Optional[dict] = None
    ) -> t.Optional[dict]:
        row = super().post_process(row, context) or row
        for label, (id_field, items) in self.plan.labels.items():
            row[label] = resolve_label(row.get(id_field), items)
        return row


class DeletedRecordsStream(KeysetStream):
    """Deleted-record log, for tombstoning. Deletes are kept for 15 days.

    Docs, Data Management APIs, "Retrieve Deleted Data API":
    POST /v1/data/objects/query/record_delete_log (low volume objects) and
    POST /v1/data/objects/query/record_delete_log_high_volume (high volume).
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs
    Paging is keyset on (DeletedOn, RecordId). Filter values use the
    documented `yyyy-MM-dd HH:mm:ss` form. The sample DeletedOn values have
    whole seconds. If live values have milliseconds, the keyset anchor
    check fails the stream instead of skipping rows.
    """

    name = "deleted_records"
    path = "/v1/data/objects/query/{delete_log}"
    primary_keys = ["ObjectName", "RecordId"]
    replication_key = "DeletedOn"
    tiebreaker = "RecordId"
    date_fields = {"DeletedOn"}

    schema = th.PropertiesList(
        th.Property("RecordId", th.StringType),
        th.Property("DeletedOn", th.DateTimeType),
        th.Property("ObjectName", th.StringType),
    ).to_dict()

    _warned_missing_log = False

    @property
    def partitions(self) -> t.List[dict]:
        return [
            {"delete_log": "record_delete_log"},
            {"delete_log": "record_delete_log_high_volume"},
        ]

    def format_cursor(self, value: datetime.datetime) -> str:
        return format_query_datetime(value)

    def base_payload(self) -> t.Dict[str, t.Any]:
        return {"select": ["RecordId", "DeletedOn", "ObjectName"]}

    def fetch_rows(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        self.log_delete_gap(context)
        yield from super().fetch_rows(context)

    def is_empty_reply(self, response: t.Any, payload: t.Any) -> bool:
        """Treat a missing high-volume log as empty. A tenant may not have one."""
        path = urlparse(response.url).path
        if not path.endswith("/record_delete_log_high_volume"):
            return False
        if not is_object_not_found(payload):
            return False
        if not self._warned_missing_log:
            self._warned_missing_log = True
            self.logger.warning(
                "record_delete_log_high_volume does not exist in this tenant. "
                "Treating it as empty."
            )
        return True


class CtaSlicedStream(GainsightStream):
    """Base for the Cockpit CTA list APIs, read in ModifiedDate windows.

    The CTA list APIs page with pageNumber and pageSize, and the docs give
    no sort order. Paging an unordered result can skip or repeat rows. So
    the tap reads ModifiedDate windows that each fit in one page:

    - The first window is one day, from the bookmark less 24 hours.
    - A window that returns a full page is halved and read again, down to
      one hour.
    - A one-hour window that still fills a page is paged, with a warning.
    - After a window under half full, the next window doubles, up to a year.

    Every returned row must have a ModifiedDate inside its window. If one
    does not, the API read the filter differently, and the stream fails.
    """

    replication_key = "ModifiedDate"
    primary_keys = ["Gsid"]
    page_size = 1000
    date_fields = {"CreatedDate", "ModifiedDate"}
    include_null_pass = False

    first_window = datetime.timedelta(days=1)
    min_window = datetime.timedelta(hours=1)
    max_window = datetime.timedelta(days=366)
    # The start when there is no bookmark and no start_date.
    history_start = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)

    def select_list(self) -> t.List[str]:
        raise NotImplementedError

    def fetch_rows(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        start = self.filter_start(context) or self.history_start
        end = utc_now() + datetime.timedelta(hours=1)
        width = self.first_window
        window_start = start
        while window_start < end:
            window_end = min(window_start + width, end)
            token = {"mode": "window", "start": window_start, "end": window_end, "page": 1}
            rows = self.post_page(context, token)
            if len(rows) >= self.page_size and window_end - window_start > self.min_window:
                width = max((window_end - window_start) / 2, self.min_window)
                continue
            if len(rows) >= self.page_size:
                self.logger.warning(
                    "%s: the window %s to %s has more CTAs than one page. "
                    "Paging it with pageNumber. The API documents no sort "
                    "order, so a CTA edited during the sync can be missed "
                    "until the next run.",
                    self.name,
                    window_start.isoformat(),
                    window_end.isoformat(),
                )
            count = 0
            while True:
                self._check_window(rows, window_start, window_end)
                count += len(rows)
                yield from rows
                if len(rows) < self.page_size:
                    break
                token = {**token, "page": token["page"] + 1}
                rows = self.post_page(context, token)
            window_start = window_end
            if count < self.page_size // 2:
                width = min(width * 2, self.max_window)
        if self.include_null_pass:
            page = 1
            while True:
                rows = self.post_page(context, {"mode": "nulls", "page": page})
                yield from rows
                if len(rows) < self.page_size:
                    break
                page += 1

    def _check_window(
        self, rows: t.List[dict], start: datetime.datetime, end: datetime.datetime
    ) -> None:
        for row in rows:
            moment = parse_api_datetime(row.get("ModifiedDate"))
            if moment is not None and not start <= moment < end:
                raise FatalAPIError(
                    f"{self.name}: CTA {row.get('Gsid')} has ModifiedDate "
                    f"{row.get('ModifiedDate')}, outside the requested window "
                    f"{start.isoformat()} to {end.isoformat()}. The API may read "
                    "the filter in another time zone. Stopping so no rows are "
                    "skipped."
                )

    def prepare_request_payload(
        self,
        context: t.Optional[dict],
        next_page_token: t.Optional[dict],
    ) -> dict:
        token = next_page_token or {"mode": "nulls", "page": 1}
        payload: t.Dict[str, t.Any] = {"select": self.select_list()}
        if token["mode"] == "window":
            conditions = [
                {
                    "fieldName": "ModifiedDate",
                    "value": [format_api_datetime(token["start"])],
                    "alias": "A",
                    "operator": "GTE",
                },
                {
                    "fieldName": "ModifiedDate",
                    "value": [format_api_datetime(token["end"])],
                    "alias": "B",
                    "operator": "LT",
                },
            ]
            payload["where"] = {"conditions": conditions, "expression": "A AND B"}
        else:
            payload["where"] = {
                "conditions": [
                    {
                        "fieldName": "ModifiedDate",
                        "value": [],
                        "alias": "A",
                        "operator": "IS_NULL",
                    }
                ],
                "expression": "A",
            }
        payload["pageSize"] = self.page_size
        payload["pageNumber"] = token["page"]
        return payload


class CtaStream(CtaSlicedStream):
    """Cockpit CTAs from the Fetch CTA API. Risks are CTAs of type Risk.

    Docs, Call To Action (CTA) API, "Fetch CTA API": POST /v2/cockpit/cta/list
    with select, where, pageSize and pageNumber. Max 1000 CTAs per request.
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Call_To_Action_(CTA)_API_Documentation
    Field names come from the Fetch CTA and Retrieve Deleted Data samples:
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Retrieve_Deleted_Data_API
    """

    name = "cta"
    path = "/v2/cockpit/cta/list"
    include_null_pass = True

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
    # "Comments", "CompanyId", "CompanyId__gr.Name" and "ModifiedDate".
    # CreatedDate is the standard MDA audit field from the describe sample.
    # It is not in a CTA sample.
    base_select = {
        "Name": "name",
        "Comments": "Comments",
        "CompanyId": "CompanyId",
        "CompanyId__gr.Name": "CompanyId__gr.Name",
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
            if not name or name in properties or is_retired_field(field):
                continue
            properties[name] = json_schema_for(field.get("dataType"))
            self.select_map[name] = name
            if is_date_type(field.get("dataType")):
                date_fields.add(name)
        self.date_fields = date_fields
        super().__init__(
            tap=tap, schema={"type": "object", "properties": properties}
        )

    def select_list(self) -> t.List[str]:
        select = [
            entry
            for prop, entry in self.select_map.items()
            if self.is_property_selected(prop)
        ]
        if "ModifiedDate" not in select:
            select.append("ModifiedDate")
        return select


class CtaDeletedStream(CtaSlicedStream):
    """Deleted CTAs, for tombstoning. Gainsight keeps them for 15 days.

    Docs, Retrieve Deleted Data API, CTA, "Endpoint One":
    POST /v2/cockpit/cta/deleted/list. The request select list and the
    response fields are the documented CTA sample.
    https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Retrieve_Deleted_Data_API
    """

    name = "cta_deleted"
    path = "/v2/cockpit/cta/deleted/list"
    date_fields = {"ModifiedDate"}

    # Documented request: "select": ["name", "Comments", "CompanyId", "ModifiedDate"].
    documented_select = ["name", "Comments", "CompanyId", "ModifiedDate"]

    schema = th.PropertiesList(
        th.Property("Gsid", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Comments", th.StringType),
        th.Property("CompanyId", th.StringType),
        th.Property("DueDate", th.StringType),
        th.Property("EntityType", th.StringType),
        th.Property("IsClosed", th.BooleanType),
        th.Property("ModifiedById", th.StringType),
        th.Property("ModifiedDate", th.DateTimeType),
        th.Property("OwnerId", th.StringType),
        th.Property("OwnerId__gr.Email", th.StringType),
        th.Property("OwnerId__gr.Name", th.StringType),
        th.Property("PriorityId", th.StringType),
        th.Property("PriorityId__gr.Name", th.StringType),
        th.Property("ReasonId", th.StringType),
        th.Property("ReasonId__gr.Name", th.StringType),
        th.Property("StatusId", th.StringType),
        th.Property("StatusId__gr.Name", th.StringType),
        th.Property("TypeId", th.StringType),
        th.Property("TypeId__gr.Name", th.StringType),
    ).to_dict()

    def select_list(self) -> t.List[str]:
        return list(self.documented_select)

    def fetch_rows(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        self.log_delete_gap(context)
        yield from super().fetch_rows(context)
