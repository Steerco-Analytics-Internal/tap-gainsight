"""Gainsight CS tap class and discovery."""

from __future__ import annotations

import typing as t

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_gainsight.client import (
    GainsightAPIError,
    GainsightAuthError,
    GainsightMetadataClient,
    RateLimiter,
)
from tap_gainsight.streams import (
    CtaDeletedStream,
    CtaStream,
    DeletedRecordsStream,
    MDAObjectStream,
    ObjectPlan,
    picklist_category_id,
    picklist_items,
)

# Objects are keyed by lowercase name. Calls use the name exactly as the
# object list returns it, such as "company". These standard objects keep
# the names the API pages use as their stream names.
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
            "objects",
            th.ArrayType(th.StringType),
            description=(
                "Optional allowlist of MDA object API names. When set, only "
                "Company and these objects get a stream."
            ),
        ),
    ).to_dict()

    _rate_limiter: t.Optional[RateLimiter] = None

    @property
    def rate_limiter(self) -> RateLimiter:
        if self._rate_limiter is None:
            self._rate_limiter = RateLimiter()
        return self._rate_limiter

    def metadata_client(self) -> GainsightMetadataClient:
        return GainsightMetadataClient(self.config, self.rate_limiter)

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
                    dropdowns[category] = {}
        return dropdowns

    def discover_streams(self) -> t.List[Stream]:
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
                    self, name, plan, name=CANONICAL_NAMES.get(key, name)
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
        """Raise when the input catalog selects what discovery cannot produce.

        Discovery runs again at sync time. A stream or column can drop out
        between runs, for example when a describe or dropdown call fails. The
        SDK would then skip it without a word, so this check fails the run
        and names what is missing.
        """
        catalog = self.input_catalog
        if not catalog:
            return
        produced = {stream.name: stream for stream in streams}
        missing_streams: t.List[str] = []
        missing_columns: t.List[str] = []
        for stream_id, entry in catalog.items():
            selection = entry.metadata.resolve_selection()
            if not selection.get((), False):
                continue
            stream = produced.get(stream_id)
            if stream is None:
                missing_streams.append(stream_id)
                continue
            schema = entry.schema.to_dict() if entry.schema else {}
            available = stream.schema.get("properties", {})
            for column in schema.get("properties", {}):
                if selection.get(("properties", column), False) and column not in available:
                    missing_columns.append(f"{stream_id}.{column}")
        if missing_streams or missing_columns:
            parts = []
            if missing_streams:
                parts.append(f"streams {', '.join(sorted(missing_streams))}")
            if missing_columns:
                parts.append(f"columns {', '.join(sorted(missing_columns))}")
            raise GainsightAPIError(
                "The catalog selects "
                + " and ".join(parts)
                + ", but discovery could not produce them. Check the warnings "
                "above for failed describe or dropdown calls, or run discovery "
                "again and update the catalog."
            )


if __name__ == "__main__":
    TapGainsight.cli()
