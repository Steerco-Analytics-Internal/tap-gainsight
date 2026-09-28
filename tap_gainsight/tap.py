"""Gainsight CS tap class and discovery."""

from __future__ import annotations

import typing as t

from singer_sdk import Stream, Tap
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk import typing as th

from tap_gainsight.client import (
    GainsightAPIError,
    GainsightAuthError,
    GainsightMetadataClient,
    RATE_LIMIT_CALLS,
    RateLimiter,
    load_zone,
)
from tap_gainsight.safety import RequestBudget
from tap_gainsight.streams import (
    CtaDeletedStream,
    CtaStream,
    DeletedRecordsStream,
    LABEL_SUFFIX,
    MDAObjectStream,
    ObjectPlan,
    picklist_category_id,
    picklist_items,
)

# Objects are keyed by lowercase name. Describe calls use the name exactly
# as the object list returns it, such as "company". Query paths and stream
# names for these documented standard objects use the documented casing:
# the Company API page queries /v1/data/objects/query/Company. Every other
# object uses its listed name in both.
CANONICAL_NAMES = {
    "company": "Company",
    "company_person": "Company_Person",
    "person": "Person",
    "gsuser": "GsUser",
}
REQUIRED_OBJECT = "company"
TIMELINE_OBJECT = "activity_timeline"
CTA_OBJECT = "cs_cta"

# Objects with a dedicated stream. They never get a generic MDA stream.
DEDICATED_OBJECTS = {
    TIMELINE_OBJECT,
    CTA_OBJECT,
    "record_delete_log",
    "record_delete_log_high_volume",
}

# The docs give no limit on object names per describe call. 25 keeps each
# response small.
DESCRIBE_BATCH_SIZE = 25


class TapGainsight(Tap):
    """Singer tap for Gainsight CS (NXT)."""

    name = "tap-gainsight"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_key",
            th.StringType,
            required=True,
            secret=True,
            description="Gainsight REST API Access Key, sent as the AccessKey header.",
        ),
        th.Property(
            "domain",
            th.StringType,
            required=True,
            description=(
                "Tenant base URL or subdomain, such as acme.gainsightcloud.com. "
                "The scheme is optional."
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest modified date to sync for incremental streams.",
        ),
        th.Property(
            "filter_timezone",
            th.StringType,
            description=(
                "Optional IANA time zone name, such as America/Los_Angeles. "
                "Set it when the tenant reads query filter times in its local "
                "time zone. Unset means UTC."
            ),
        ),
        th.Property(
            "max_requests_per_minute",
            th.IntegerType,
            description=(
                "Optional client-side request rate, from 1 to 100. Gainsight "
                "documents 100 synchronous calls a minute, so the default is "
                "100 and it can only be lowered."
            ),
        ),
        th.Property(
            "max_requests",
            th.IntegerType,
            description=(
                "Optional hard cap on requests in one run, retries included. "
                "The run stops with an error when it is reached."
            ),
        ),
        th.Property(
            "objects",
            th.ArrayType(th.StringType),
            description=(
                "Optional allowlist of MDA object API names. When set, only "
                "Company and these objects get a stream."
            ),
        ),
    ).to_dict()

    _rate_limiter: t.Optional[RateLimiter] = None
    # Discovery failures in this run, used by the catalog check.
    _failed_objects: t.Set[str] = set()
    _failed_dropdowns: t.Set[str] = set()
    _stream_objects: t.Dict[str, str] = {}

    def _validate_config(
        self, *, raise_errors: bool = True, warnings_as_errors: bool = False
    ) -> t.Tuple[t.List[str], t.List[str]]:
        warnings, errors = super()._validate_config(
            raise_errors=raise_errors, warnings_as_errors=warnings_as_errors
        )
        problems: t.List[str] = []
        try:
            load_zone(self.config.get("filter_timezone"))
        except ValueError as exc:
            problems.append(str(exc))
        rate = self.config.get("max_requests_per_minute")
        if rate is not None and not (isinstance(rate, int) and 1 <= rate <= RATE_LIMIT_CALLS):
            problems.append(
                f"max_requests_per_minute must be from 1 to {RATE_LIMIT_CALLS}, "
                f"got {rate!r}. It can only lower the documented limit."
            )
        cap = self.config.get("max_requests")
        if cap is not None and not (isinstance(cap, int) and cap >= 1):
            problems.append(f"max_requests must be 1 or more, got {cap!r}.")
        if problems and raise_errors:
            raise ConfigValidationError("Config validation failed: " + " ".join(problems))
        errors.extend(problems)
        return warnings, errors

    _request_budget: t.Optional[RequestBudget] = None

    @property
    def rate_limiter(self) -> RateLimiter:
        if self._rate_limiter is None:
            calls = self.config.get("max_requests_per_minute") or RATE_LIMIT_CALLS
            self._rate_limiter = RateLimiter(calls=min(int(calls), RATE_LIMIT_CALLS))
        return self._rate_limiter

    @property
    def request_budget(self) -> RequestBudget:
        """One count of requests for the whole run, discovery included."""
        if self._request_budget is None:
            self._request_budget = RequestBudget(self.config.get("max_requests"))
        return self._request_budget

    def metadata_client(self) -> GainsightMetadataClient:
        return GainsightMetadataClient(
            self.config, self.rate_limiter, budget=self.request_budget
        )

    def _describe_all(
        self,
        client: GainsightMetadataClient,
        names: t.List[str],
        required: t.Set[str],
    ) -> t.Dict[str, dict]:
        """Describe objects in batches. Drop a failing optional object.

        A failed batch is retried one object at a time, so one bad object
        does not drop the others. Auth errors, and any failure on a required
        object, raise.
        """
        described: t.Dict[str, dict] = {}
        dropped: t.Set[str] = set()
        for start in range(0, len(names), DESCRIBE_BATCH_SIZE):
            batch = names[start : start + DESCRIBE_BATCH_SIZE]
            try:
                described.update(client.describe(batch))
                continue
            except GainsightAuthError:
                raise
            except GainsightAPIError as exc:
                if len(batch) == 1:
                    self._drop_or_raise(batch[0], exc, required)
                    dropped.add(batch[0].lower())
                    continue
                self.logger.warning(
                    "Describe failed for a batch of %d objects. Retrying one "
                    "at a time. Error: %s",
                    len(batch),
                    exc,
                )
            for name in batch:
                try:
                    described.update(client.describe([name]))
                except GainsightAuthError:
                    raise
                except GainsightAPIError as exc:
                    self._drop_or_raise(name, exc, required)
                    dropped.add(name.lower())
        for name in names:
            if name.lower() not in described and name.lower() not in dropped:
                self._drop_or_raise(
                    name,
                    GainsightAPIError(f"Describe returned nothing for {name}."),
                    required,
                )
        return described

    def _drop_or_raise(
        self, name: str, exc: Exception, required: t.Set[str]
    ) -> None:
        self._failed_objects = self._failed_objects | {name.lower()}
        if name.lower() in required:
            raise GainsightAPIError(
                f"Discovery failed for required object {name}: {exc}"
            ) from exc
        self.logger.warning("Dropping object %s. Describe failed: %s", name, exc)

    def _dropdown_labels(
        self,
        client: GainsightMetadataClient,
        descriptions: t.Iterable[dict],
    ) -> t.Dict[str, t.Dict[str, t.Any]]:
        """Fetch dropdown items for picklist fields that carry only a category."""
        dropdowns: t.Dict[str, t.Dict[str, t.Any]] = {}
        for description in descriptions:
            for field in description.get("fields") or []:
                if not isinstance(field, dict) or picklist_items(field):
                    continue
                category = picklist_category_id(field)
                if not category or category in dropdowns:
                    continue
                try:
                    dropdowns[category] = client.dropdown_items(category)
                except GainsightAuthError:
                    raise
                except GainsightAPIError as exc:
                    self.logger.warning(
                        "No labels for dropdown category %s: %s", category, exc
                    )
                    self._failed_dropdowns = self._failed_dropdowns | {category}
                    dropdowns[category] = {}
        return dropdowns

    def discover_streams(self) -> t.List[Stream]:
        self._failed_objects = set()
        self._failed_dropdowns = set()
        client = self.metadata_client()
        listed = client.list_objects()

        allowlist = [str(name) for name in self.config.get("objects") or []]
        allow = {name.lower() for name in allowlist}

        # Lowercase key -> object name exactly as the object list returns it.
        # Describe and query calls use that name. Stream names use
        # CANONICAL_NAMES for the documented standard objects.
        listed_names: t.Dict[str, str] = {}
        for item in listed:
            name = str(item.get("objectName") or "")
            if name and item.get("readable") is not False:
                listed_names[name.lower()] = name

        def api_name(key: str, fallback: str) -> str:
            return listed_names.get(key, fallback)

        stream_objects: t.Dict[str, str] = {
            REQUIRED_OBJECT: api_name(REQUIRED_OBJECT, "Company")
        }
        for key, name in listed_names.items():
            if key in DEDICATED_OBJECTS or (allow and key not in allow):
                continue
            stream_objects.setdefault(key, name)
        for name in allowlist:
            key = name.lower()
            if key not in DEDICATED_OBJECTS:
                stream_objects.setdefault(key, api_name(key, name))

        # GsUser and Company are described for lookup columns even when the
        # allowlist leaves out their streams.
        to_describe: t.Dict[str, str] = dict(stream_objects)
        to_describe.setdefault("gsuser", api_name("gsuser", "GsUser"))
        to_describe.setdefault(TIMELINE_OBJECT, api_name(TIMELINE_OBJECT, TIMELINE_OBJECT))
        if CTA_OBJECT in listed_names:
            to_describe.setdefault(CTA_OBJECT, listed_names[CTA_OBJECT])

        # Stream name -> object key, for the catalog check.
        self._stream_objects = {
            CANONICAL_NAMES.get(key, name): key for key, name in stream_objects.items()
        }
        self._stream_objects.update({"timeline": TIMELINE_OBJECT, "cta": CTA_OBJECT})

        described = self._describe_all(
            client, list(to_describe.values()), required={REQUIRED_OBJECT}
        )
        related_fields = {
            key: {
                str(field.get("fieldName"))
                for field in described[key].get("fields") or []
                if isinstance(field, dict)
            }
            for key in ("gsuser", "company")
            if key in described
        }
        dropdowns = self._dropdown_labels(client, described.values())

        def plan_for(key: str) -> ObjectPlan:
            return ObjectPlan(
                described[key].get("fields") or [], related_fields, dropdowns
            )

        streams: t.List[Stream] = []
        for key, name in stream_objects.items():
            if key not in described:
                continue
            plan = plan_for(key)
            if not plan.fields:
                self._drop_or_raise(
                    name,
                    GainsightAPIError("Describe returned no fields."),
                    {REQUIRED_OBJECT},
                )
                continue
            streams.append(
                MDAObjectStream(
                    self,
                    CANONICAL_NAMES.get(key, name),
                    plan,
                    name=CANONICAL_NAMES.get(key, name),
                )
            )

        if TIMELINE_OBJECT in described and plan_for(TIMELINE_OBJECT).fields:
            streams.append(
                MDAObjectStream(
                    self,
                    to_describe[TIMELINE_OBJECT],
                    plan_for(TIMELINE_OBJECT),
                    name="timeline",
                    # The Timeline docs name no modified-date field. The tap
                    # uses ModifiedDate, the standard describe field, only if
                    # the live describe has it. Otherwise timeline is full table.
                    replication_key_candidates=("ModifiedDate",),
                )
            )

        cta_custom = [
            field
            for field in (described.get(CTA_OBJECT) or {}).get("fields") or []
            if isinstance(field, dict)
            and str(field.get("fieldName", "")).endswith("__gc")
        ]
        streams.append(CtaStream(self, custom_fields=cta_custom))
        streams.append(CtaDeletedStream(self))
        streams.append(DeletedRecordsStream(self))
        self._check_input_catalog(streams)
        return streams

    def _check_input_catalog(self, streams: t.List[Stream]) -> None:
        """Compare the input catalog with this run's discovery.

        Discovery runs again at sync time. When a selected stream or column
        is missing because a describe, dropdown or lookup-target call failed
        in this run, the run fails and names it, because the SDK would skip
        it without a word. When it is missing because Gainsight no longer
        has it, such as a field an admin deleted, the tap logs a warning and
        syncs the rest.
        """
        catalog = self.input_catalog
        if not catalog:
            return
        produced = {stream.name: stream for stream in streams}
        failed: t.List[str] = []
        gone: t.List[str] = []
        for stream_id, entry in catalog.items():
            selection = entry.metadata.resolve_selection()
            if not selection.get((), False):
                continue
            stream = produced.get(stream_id)
            if stream is None:
                key = self._stream_objects.get(stream_id, stream_id.lower())
                (failed if key in self._failed_objects else gone).append(stream_id)
                continue
            schema = entry.schema.to_dict() if entry.schema else {}
            available = stream.schema.get("properties", {})
            for column in schema.get("properties", {}):
                if not selection.get(("properties", column), False) or column in available:
                    continue
                name = f"{stream_id}.{column}"
                (failed if self._lost_to_failure(stream, column) else gone).append(name)
        if gone:
            self.logger.warning(
                "The catalog selects %s, which Gainsight no longer has. Syncing "
                "the rest. Run discovery again to update the catalog.",
                ", ".join(sorted(gone)),
            )
        if failed:
            raise GainsightAPIError(
                "The catalog selects "
                + ", ".join(sorted(failed))
                + ", but a metadata call failed in this run, so discovery could "
                "not produce them. Check the warnings above for failed describe "
                "or dropdown calls, and run the tap again."
            )

    def _lost_to_failure(self, stream: Stream, column: str) -> bool:
        """Return True when a metadata failure in this run removed `column`."""
        if isinstance(stream, CtaStream):
            return CTA_OBJECT in self._failed_objects
        plan = getattr(stream, "plan", None)
        if plan is None:
            return False
        if column.endswith(LABEL_SUFFIX):
            field = plan.fields.get(column[: -len(LABEL_SUFFIX)])
            return bool(field) and picklist_category_id(field) in self._failed_dropdowns
        if "__gr." in column:
            lookup = column.split(".", 1)[0]
            for field in plan.fields.values():
                meta = field.get("meta") if isinstance(field.get("meta"), dict) else {}
                detail = meta.get("lookupDetail") or {}
                if detail.get("lookupName") != lookup:
                    continue
                targets = {
                    str((target or {}).get("objectName", "")).lower()
                    for target in detail.get("lookupObjects") or []
                }
                return bool(targets & self._failed_objects)
        return False


if __name__ == "__main__":
    TapGainsight.cli()
