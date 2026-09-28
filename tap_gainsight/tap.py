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
    CtaStream,
    DeletedRecordsStream,
    MDAObjectStream,
    ObjectPlan,
    picklist_category_id,
    picklist_items,
)

# Objects are keyed by lowercase name. The object list returns lowercase
# names, such as "company". These standard objects keep the names the API
# pages use in their endpoint paths, for stable stream names.
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

        # Lowercase key -> API name used in query paths and stream names.
        stream_objects: t.Dict[str, str] = {REQUIRED_OBJECT: "Company"}
        listed_names = {}
        for item in listed:
            name = str(item.get("objectName") or "")
            if not name or item.get("readable") is False:
                continue
            listed_names[name.lower()] = name
            key = name.lower()
            if key in DEDICATED_OBJECTS or (allow and key not in allow):
                continue
            stream_objects.setdefault(key, CANONICAL_NAMES.get(key, name))
        for name in allowlist:
            key = name.lower()
            if key not in DEDICATED_OBJECTS:
                stream_objects.setdefault(key, CANONICAL_NAMES.get(key, name))

        # GsUser and Company are described for lookup columns even when the
        # allowlist leaves out their streams.
        to_describe: t.Dict[str, str] = dict(stream_objects)
        to_describe.setdefault("gsuser", "GsUser")
        to_describe.setdefault(TIMELINE_OBJECT, TIMELINE_OBJECT)
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
        for key, api_name in stream_objects.items():
            if key not in described:
                continue
            plan = plan_for(key)
            if not plan.fields:
                self._drop_or_raise(
                    api_name,
                    GainsightAPIError("Describe returned no fields."),
                    {REQUIRED_OBJECT},
                )
                continue
            streams.append(MDAObjectStream(self, api_name, plan))

        if TIMELINE_OBJECT in described and plan_for(TIMELINE_OBJECT).fields:
            streams.append(
                MDAObjectStream(
                    self,
                    TIMELINE_OBJECT,
                    plan_for(TIMELINE_OBJECT),
                    name="timeline",
                    replication_key_candidates=("ModifiedDate", "LastModifiedDate"),
                )
            )

        cta_custom = [
            field
            for field in (described.get(CTA_OBJECT) or {}).get("fields") or []
            if isinstance(field, dict)
            and str(field.get("fieldName", "")).endswith("__gc")
        ]
        streams.append(CtaStream(self, custom_fields=cta_custom))
        streams.append(DeletedRecordsStream(self))
        return streams


if __name__ == "__main__":
    TapGainsight.cli()
