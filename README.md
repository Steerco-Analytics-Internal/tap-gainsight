# tap-gainsight

`tap-gainsight` is a Singer tap for Gainsight CS (also called Gainsight NXT).
It is built with the Meltano Singer SDK. Hotglue runs it for Steerco customers.
Steerco's ETL maps its streams into accounts, contacts, activities, risks and
custom objects.

> **Before release:** the test fixtures come from the examples in Gainsight's
> docs. No live tenant was available. Check each request and response shape
> against a live tenant before you ship. Gainsight's docs contain at least two
> malformed samples (see [Test fixtures](#test-fixtures)), so treat every shape
> as unconfirmed until a live response matches it.

## Configuration

| Setting | Required | Description |
|---|---|---|
| `access_key` | Yes | Gainsight REST API Access Key. The tap sends it in the `AccessKey` header. It is a secret. |
| `domain` | Yes | Tenant base URL or subdomain, such as `acme.gainsightcloud.com`. The scheme is optional. The tap always uses `https://<host>`. A bare name with no dot, such as `acme`, becomes `acme.gainsightcloud.com`. |
| `start_date` | No | ISO 8601 date-time. The earliest modified date for incremental streams on their first run. |
| `objects` | No | Allowlist of MDA object API names, such as `["Person", "Renewal__gc"]`. When set, only `Company` and these objects get a stream. When not set, every readable object gets a stream. |

Example `config.json`:

```json
{
  "access_key": "YOUR_ACCESS_KEY",
  "domain": "acme.gainsightcloud.com",
  "start_date": "2024-01-01T00:00:00Z"
}
```

## Streams

| Stream | Source | Key | Replication key |
|---|---|---|---|
| `Company` | MDA object `Company` | `Gsid` | `ModifiedDate` |
| One stream per readable MDA object, such as `Company_Person`, `GsUser` or `Renewal__gc` | MDA object of the same name | `Gsid` | `ModifiedDate`, when the object has it |
| `timeline` | MDA object `activity_timeline` | `Gsid` | `ModifiedDate` or `LastModifiedDate`, whichever the describe returns. Full table if neither. |
| `cta` | Fetch CTA API | `Gsid` | `ModifiedDate` |
| `deleted_records` | `record_delete_log` and `record_delete_log_high_volume` | `ObjectName`, `RecordId` | `DeletedOn` |

Stream names:

- `Company`, `Company_Person`, `Person` and `GsUser` keep the names the API
  pages use in their endpoint paths.
- Any other object uses the name the object list returns, such as
  `renewal__gc`.
- The API-backed streams use short snake_case names.

### Notes on each stream

- **MDA objects.** The schema comes from the describe API at discovery time.
  Every standard and custom field comes through. Every property is nullable.
  The query selects every property the catalog selects.
- **Lookups.** A lookup to `GsUser`, such as an owner or CSM, also selects
  `<lookup>__gr.Name` and `<lookup>__gr.Email`. A lookup to `Company` also
  selects `<lookup>__gr.Name`. They are flat columns named with the documented
  dot path, for example `Csm__gr.Email`. A column is added only when the
  target object's describe has the field.
- **Picklists.** Picklist fields return item GSIDs. The tap adds a
  `<field>_label` column with the item name next to the id. A multi-select
  value becomes a list of names. See [Known gaps](#known-gaps-and-unverified-behavior).
- **Dates.** The query API returns Date and DateTime values as epoch
  milliseconds. The tap converts them to ISO 8601 UTC strings. `DATE` fields
  are strings with no `format`, because a date's time zone is not documented.
- **`timeline`.** Its schema comes from the `activity_timeline` describe, so
  custom `Ant__` fields come through. The docs show fields such as
  `contextname`, `GsCompanyId`, `GsRelationshipId`, `AuthorId` and `Gsid`.
- **`cta`.** Risks are CTAs whose `TypeId__gr.Name` is `Risk`. There is no
  separate risks stream. Steerco filters by type in its own ETL. When the
  object list contains `cs_cta`, the tap describes it and adds its custom
  `__gc` fields to the schema and the select list.
- **`deleted_records`.** Gainsight keeps deleted records for **15 days only**.
  Run this stream at least every few days, and never less often than every
  14 days, or tombstones are lost.

### Types

| Gainsight data type | JSON Schema |
|---|---|
| `STRING`, `GSID`, `LOOKUP`, `EMAIL`, `URL`, `RICHTEXTAREA` | nullable string |
| `DATETIME` | nullable string, `format: date-time` |
| `DATE` | nullable string |
| `NUMBER`, `CURRENCY`, `PERCENTAGE` | nullable number |
| `BOOLEAN` | nullable boolean |
| Anything else, such as a dropdown or multi-select type | any JSON type, nullable |

## How discovery works

Discovery runs with only `access_key` and `domain`. It makes these calls:

1. It lists objects with the Lite object list. Objects marked `readable: false`
   are skipped.
1. It describes every object in batches of 25 with Post Describe. It also
   describes `GsUser`, `Company`, `activity_timeline` and, if listed, `cs_cta`.
1. It fetches dropdown items for picklist fields that name only a category.
1. It builds one stream per object, plus `timeline`, `cta` and
   `deleted_records`.

Failures:

- An auth failure (HTTP 401 or 403, or error code `GS_APIG_2401`) raises
  at once.
- A failure on `Company` raises.
- If a describe batch fails, the tap retries each object alone. An optional
  object that still fails logs a warning and is dropped.
- A failed dropdown call logs a warning. That picklist gets no label column.

Every tap run discovers again, so new custom fields appear on the next run.

## Sync behavior

- **Pagination.** MDA queries use `limit` 5000 and `offset`. CTAs use
  `pageSize` 1000 and `pageNumber`. Paging stops when a page has fewer rows
  than the limit.
- **Incremental filters.** MDA queries filter the replication key with `GTE`
  and a `yyyy-MM-dd HH:mm:ss` UTC value. This form is from the delete log
  sample. CTAs filter `ModifiedDate` with `GTE` and a `yyyy-MM-dd` value,
  as the CTA samples do. The CTA filter starts one day before the bookmark,
  so no time zone offset can skip a row. Targets dedupe the overlap by key.
- **Rate limits.** Gainsight documents 100 synchronous calls per minute.
  The tap has one client-side limiter for all streams and metadata calls.
  Retries count against it.
- **Retries.** HTTP 429, 5xx, connection errors and timeouts back off
  exponentially, up to 8 tries and 60 seconds a wait. The stream then fails.
- **Errors.** Any other 4xx fails the stream at once. So does a 200 whose
  body has `result: false`, a body that is not JSON, or a `data` shape the
  docs do not show. The error includes the status and a body excerpt. One
  exception: the documented "No data found for given criteria" reply counts
  as an empty page.

## API reference

Gainsight publishes no OpenAPI or Swagger spec for CS (NXT). The only specs
found were for Gainsight PX, and third-party profiles. Neither is a valid
source for this tap. The contract is the doc pages below. Each request in the
code has a comment with its doc link.

| Call | Method and path | Doc page and section |
|---|---|---|
| Object list | `GET /v1/meta/services/objects/list?po=company&em=false` | [Data Management APIs](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs), Get Lite API Call OMD |
| Describe objects | `POST /v1/meta/services/objects/describe` | [Data Management APIs](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs), Post Describe OMD |
| Dropdown items | `GET /v1/meta/services/dropdowns/{categoryId}` | [Data Management APIs](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs), Get API - categoryID |
| Query an MDA object | `POST /v1/data/objects/query/{objectName}` | [Company API](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Company_API_Documentation), Read API. [Custom Object API](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Custom_Object_API/Gainsight_Custom_Object_API_Documentation), Read API |
| Timeline activities | `POST /v1/data/objects/query/activity_timeline` | [Timeline APIs](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Timeline_API/Timeline_APIs), Read API |
| CTAs | `POST /v2/cockpit/cta/list` | [Call To Action (CTA) API](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Call_To_Action_(CTA)_API_Documentation), Fetch CTA API. [Retrieve Deleted Data API](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Retrieve_Deleted_Data_API), CTA |
| Deleted records | `POST /v1/data/objects/query/record_delete_log` and `.../record_delete_log_high_volume` | [Data Management APIs](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs), Retrieve Deleted Data API |

Other facts and where they come from:

| Fact | Source |
|---|---|
| Header `accesskey`, and Content Type JSON | Authentication and Headers sections of every page above |
| 100 synchronous calls a minute, fixed window | Throttling Limits sections of the same pages |
| Query `select`, `where`, `orderBy`, `limit`, `offset`, and the operator enums | Custom Object API, Read API |
| At most 5000 rows a query, and `offset` is a starting index | Company API and Custom Object API, Read API |
| Query dates return as epoch milliseconds | Custom Object API, Sample Success Response notes |
| Empty results: `data.records: []`, or `result: false` with "No data found" | Custom Object API and Company API, Read API failure samples |
| Unauthorized body `GS_APIG_2401` | Data Management APIs, failure samples |
| CTA fields, `fieldName` conditions, `pageSize`, `pageNumber`, at most 1000 a page | Call To Action (CTA) API, Fetch CTA API |
| CTA `ModifiedDate` and `ModifiedById` fields, and date-only filters on `ModifiedDate` | Retrieve Deleted Data API, CTA |
| Deleted data kept for 15 days | Data Management APIs and Retrieve Deleted Data API |

## Known gaps and unverified behavior

Check these against a live tenant before release:

- **Picklist metadata.** No describe sample in the docs shows a picklist field.
  The tap reads labels from any list under a describe key containing
  "picklist" whose items have `gsid`. That is the dropdown API's item shape.
  Otherwise it reads a `categoryId` and calls the dropdown API. If a live
  describe uses another shape, picklists get no label column. Ids still sync.
- **Dropdown and multi-select type names.** The docs do not give their
  describe `dataType` values. They map to "any JSON type".
- **Response `data` shape.** The Company and CTA pages show `data` as a list.
  The Timeline and delete log pages show `data.records`. The tap accepts
  both.
- **Offset for page 2.** The docs say to pass "Offset as 5001" for the second
  page. The samples start at offset 0, so the tap uses 5000. At worst this
  repeats one row. It never skips one.
- **DateTime filter format.** Only the delete log sample shows a DateTime
  filter value (`2024-02-05 00:00:00`). The tap assumes UTC.
- **CTA `CreatedDate`.** No CTA sample selects it. The tap selects it because
  it is the standard MDA audit field. If the Fetch CTA API rejects it, the
  stream fails with `COCKPIT_5101` and says so.
- **CTA relationship fields and `ClosedDate`.** The CTA docs name
  "RelationshipID" and "Relationship Type" in prose only, with no API names.
  `ClosedDate` appears only in a Task sample. None of these are in the schema.
- **`timeline` replication key.** The Timeline docs do not document a
  modified-date filter. The tap uses `ModifiedDate` or `LastModifiedDate` only
  when the describe says it is a filterable DATETIME.
- **Object list scope.** The documented list call passes `po=company`
  ("parent object marker"). The docs do not say whether it limits the list.
  Use `objects` to name any object the list leaves out, such as `Person`.
- **Select size.** The docs state no limit on select list length, and queries
  are POST bodies. The tap does not split wide objects across requests.
- **Scorecards.** Scorecard objects are documented by display name only, and
  Scorecard 2.0 fact objects have per-scorecard generated names. There is no
  scorecard stream. A tenant can add a scorecard object by API name in
  `objects`.
- **Bookmarks.** If a bookmarked row is later deleted, the next run's newest
  row can be older than the old bookmark. The SDK then stores the older value.
  This re-reads rows. It never skips them.

## Local development

You need Python 3.10 and Poetry 1.6.1.

```bash
poetry install
poetry run tap-gainsight --about
poetry run tap-gainsight --config config.json --discover > catalog.json
poetry run tap-gainsight --config config.json --catalog catalog.json --state state.json
poetry run pytest
```

`pytest` reports coverage for `tap_gainsight/` and fails under 90%.

### Test fixtures

`tests/fixtures/` holds request and response samples copied from Gainsight's
docs. `tests/fixtures/SOURCES.json` gives the doc page and section for each
file. Two doc samples are not valid JSON:

- The describe response is missing a comma. `describe_response.verbatim.txt`
  keeps the doc text. `describe_response.json` adds only that comma.
- The Fetch CTA request has `//` comments inside the JSON.
  `cta_list_request.verbatim.txt` keeps the doc text. `cta_list_request.json`
  removes only the comments.

`tests/test_fixtures.py` checks that each repair changes nothing else. Where a
test needs data the docs lack, such as a second object's describe, it copies a
documented entry and changes only names and types. `tests/conftest.py` says
which entry each helper starts from.

The suite covers:

- Contract tests that match each request to the documented shape.
- Behavior tests for each stream.
- An end-to-end run of the CLI: `--discover`, then a sync with a catalog and
  state.

## CI

`.github/workflows/ci.yml` defines one job named `CI`. The org ruleset
requires it on every PR to `main`. Don't rename it, because the ruleset
matches the literal string `CI`. The job runs the full test suite with the
coverage gate, and `--about`.
