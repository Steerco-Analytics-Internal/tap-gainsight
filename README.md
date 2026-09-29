# tap-gainsight

`tap-gainsight` is a Singer tap for Gainsight CS (also called Gainsight NXT).
It is built with the Meltano Singer SDK. Hotglue runs it for Steerco customers.
Steerco's ETL maps its streams into accounts, contacts, activities, risks and
custom objects.

> **Before release:** the test fixtures come from the examples in Gainsight's
> docs. No live tenant was available. Check each request and response shape
> against a live tenant before you ship. Gainsight's docs contain at least two
> malformed samples (see [Test fixtures](#test-fixtures)), so treat every shape
> as unconfirmed until a live response matches it. The list in
> [Known gaps](#known-gaps-and-unverified-behavior) says what to check first.

## Safety

The tap is read-only, and the code enforces it. Every HTTP request goes
through one function, `send` in `tap_gainsight/safety.py`. Before any
network I/O it checks the destination, the headers, the method, the path
and the body, and refuses anything else with `GainsightSafetyError`,
whatever the config or catalog.

### Pinned host

The tap talks to one host, fixed at startup from the config:

- `domain` must be a bare host, optionally after `https://`. User
  information (`@`), ports, paths, queries, fragments, braces and whitespace
  are rejected, and so is `http://`. A single label, such as `acme`, becomes
  `acme.gainsightcloud.com`.
- The host must match `^[a-z0-9-]+(\.[a-z0-9-]+)*\.gainsightcloud\.com$`,
  ignoring case.
- A custom Gainsight domain, such as `companyapi.yourcompany.com`, also
  needs `custom_domain` set to the same host. Entering it twice makes it a
  deliberate choice. IP addresses are refused.
- `send` requires `https`, no user information, no explicit port, and a
  host exactly equal to the pinned host.

A bad `domain` fails config validation before any request.

### Allowlist

The allowlist (`READ_ONLY_ALLOWLIST`) holds only the documented read
endpoints the tap uses:

| Method | Path | Query keys | Body keys |
|---|---|---|---|
| GET | `/v1/meta/services/objects/list` | `po`, `em` | none |
| POST | `/v1/meta/services/objects/describe` | none | `objectNames` and the describe flags |
| GET | `/v1/meta/services/dropdowns/{categoryId}` | none | none |
| POST | `/v1/data/objects/query/{object}` (also timeline and the delete logs) | none | `select`, `where`, `orderBy`, `limit`, `offset` |
| POST | `/v2/cockpit/cta/list` | none | `select`, `where`, `pageSize`, `pageNumber` |
| POST | `/v2/cockpit/cta/deleted/list` | none | `select`, `where`, `pageSize`, `pageNumber` |

The checks:

- Every path pattern is anchored. `{object}` is letters, digits and
  underscores only. A path with `%`, `//`, `.` or `..` segments, or an extra
  segment, is refused. So are PUT, DELETE and PATCH, and the insert path
  `POST /v1/data/objects/{object}`.
- A GET has no body. A POST body is a JSON object with only the read keys
  above, and no `records`, `data`, `lookups` or `updateKeys` key anywhere in
  it.
- Only these request headers are sent: `AccessKey`, `Content-Type`,
  `Content-Length`, `User-Agent`, `Accept`, `Accept-Encoding` and
  `Connection`. Any other, such as `Authorization` or a method-override
  header, is refused.
- Every session has `trust_env` off, so `.netrc` files and proxy settings
  cannot add headers. `send` refuses a session with it on.
- Every session refuses to store cookies, so a load balancer cookie such as
  `AWSALB` is never sent back. A `Cookie` header is still refused.
- Redirects are never followed, so the access key never goes to another host.
- Object names from the object list that are not plain identifiers are
  skipped. Names in the `objects` setting must be plain identifiers, or
  config validation fails. Every host and name pattern must match the whole
  value, so a trailing newline cannot slip through.
- The default rate is 30 requests a minute, below Gainsight's documented
  100, to share the tenant's allowance. `max_requests_per_minute` can set 1
  to 100, and `max_requests` caps the run.
- A field-sample job's record limit, `_hg_max_records_limit`, stops each
  named stream before its next request. A bad value is a config error, so
  it never turns into a full read. See [Record limits](#record-limits).
- `batch_config` is rejected. The tap writes Singer messages to stdout only.

### What errors and logs show

Errors and logs never quote a response body, `errorDesc` or a record value.
Gainsight's error templates put values in `errorDesc`, as in "Invalid
dateTime format (%s(columnName)= %s(columnValue))". For the documented error
shape, a message gives:

- the HTTP status;
- `errorCode` and `title`, only when each is a code of up to 50 capitals,
  digits and underscores. Any other value gives only its type or length;
- the tap's own fixed description for a code it handles: GSOBJ_1011,
  GSOBJ_1005, GSOBJ_1023, GSOBJ_1024, GS_APIG_2401, COCKPIT_5101 and
  OBJECT_NOT_FOUND.

For any other body, a message gives the status and the body length only.
An unexpected response shape is described by its key names and types. Any
echoed access key is replaced by `***`.

### Tests

The test suite records every request it sends and checks each one against
the allowlist at the end of the run. It also refuses every real socket
connection, so no test can reach the network.

## Configuration

| Setting | Required | Description |
|---|---|---|
| `access_key` | Yes | Gainsight REST API Access Key. The tap sends it in the `AccessKey` header. It is a secret. |
| `domain` | Yes | Tenant host under gainsightcloud.com, such as `acme.gainsightcloud.com`, optionally after `https://`. A bare name with no dot, such as `acme`, becomes `acme.gainsightcloud.com`. See [Pinned host](#pinned-host). |
| `custom_domain` | No | Only for a custom Gainsight domain, such as `companyapi.yourcompany.com`. It must equal the `domain` host. |
| `start_date` | No | ISO 8601 date-time. The earliest modified date for incremental streams on their first run. |
| `filter_timezone` | No | IANA time zone name, such as `America/Los_Angeles`. Set it only when the tenant reads query filter times in its local time zone. Query API and delete log filter values are then sent in that zone, with its daylight saving rules. Unset means UTC. An unknown name fails config validation. See [Filter time zone](#filter-time-zone). |
| `max_requests_per_minute` | No | Client-side request rate, from 1 to 100. The default is 30, below Gainsight's documented 100, to share the tenant's allowance with other integrations. |
| `max_requests` | No | Hard cap on requests in one run, retries and discovery included. The run stops with an error when it is reached. |
| `objects` | No | Allowlist of MDA object API names, such as `["Person", "Renewal__gc"]`. When set, only `Company` and these objects get a stream. When not set, every readable object gets a stream. |
| `_hg_max_records_limit` | No | Set by Hotglue, not by users. Maps stream names to the most records to write, as in `{"Company": 10}`. See [Record limits](#record-limits). |

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
| One stream per readable MDA object, such as `Company_Person`, `GsUser` or `renewal__gc` | MDA object of the same name | `Gsid` | `ModifiedDate`, when the object has it as a filterable DATETIME |
| `timeline` | MDA object `activity_timeline` | `Gsid` | `ModifiedDate` only if the live describe returns it. Otherwise full table. |
| `cta` | Fetch CTA API | `Gsid` | `ModifiedDate` |
| `cta_deleted` | Deleted CTA list API | `Gsid` | `ModifiedDate` |
| `deleted_records` | `record_delete_log` and `record_delete_log_high_volume` | `ObjectName`, `RecordId` | `DeletedOn` |

Stream names:

- `Company`, `Company_Person`, `Person` and `GsUser` keep the names the API
  pages use in their endpoint paths.
- Any other object uses the name the object list returns, such as
  `renewal__gc`.
- The API-backed streams use short snake_case names.

Describe calls use an object's name exactly as the object list returns it,
such as `company`. Query paths use the documented casing for the documented
objects: `Company`, `Company_Person`, `Person`, `GsUser` and
`activity_timeline`, and the two delete logs as their page shows them. Every
other object uses its listed name.

### Notes on each stream

- **MDA objects.** The schema comes from the describe API at discovery time.
  Every standard and custom field comes through, including hidden fields.
  Fields the describe flags `deleted` are left out. Every property is
  nullable. The query selects every property the catalog selects, plus
  `Gsid` and `ModifiedDate`, which paging needs.
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
  custom `Ant__` fields come through. The Timeline docs name no modified-date
  field. The stream is incremental on `ModifiedDate` only when the live
  describe has that field. If it does not, `timeline` is a full-table stream
  and re-reads every activity each run. Check this on a live tenant.
- **`cta`.** Risks are CTAs whose `TypeId__gr.Name` is `Risk`. There is no
  separate risks stream. Steerco filters by type in its own ETL. When the
  object list contains `cs_cta`, the tap describes it and adds its custom
  `__gc` fields to the schema and the select list.
- **`cta_deleted` and `deleted_records`.** These are tombstones. Gainsight
  keeps deleted records for **15 days only**. See
  [Sync frequency](#sync-frequency-for-deletes).
- **Null replication keys.** A record whose replication key is null is still
  emitted, but it does not move the bookmark. The log gives a count per run.

## Sync frequency for deletes

Gainsight keeps deleted records for 15 days. Run `deleted_records` and
`cta_deleted` at least every 14 days, and preferably daily. When a run starts
from a bookmark or `start_date` more than 15 days old, the tap logs an
**ERROR** with the size of the gap and still syncs what Gainsight has. Deletes
from the part of the gap older than 15 days are lost. Alert on that log line.

## Filter time zone

The query API and delete log filters send `yyyy-MM-dd HH:mm:ss` values with
no offset, in UTC. The docs do not say which zone the API reads them in.
If a tenant reads them in its local zone, the whole-second chain would skip
rows, so the tap stops the stream with an error that ends: "If your
Gainsight tenant reads filter times in its local time zone, set
filter_timezone." Set `filter_timezone` to the tenant's zone and run again.
CTA filters use whole days with overlapping windows, so they need no
setting.

## How discovery works

Discovery runs with only `access_key` and `domain`. It makes these calls:

1. It lists objects with the Lite object list. Objects marked `readable: false`
   are skipped.
1. It describes every object in batches of 25 with Post Describe. It also
   describes `GsUser`, `Company`, `activity_timeline` and, if listed, `cs_cta`.
1. It fetches dropdown items for picklist fields that name only a category.
1. It builds one stream per object, plus `timeline`, `cta`, `cta_deleted` and
   `deleted_records`.

Failures:

- An auth failure (HTTP 401 or 403, or error code `GS_APIG_2401` or
  `GSOBJ_1024`) raises at once. So does any redirect.
- A failure on `Company` raises.
- If a describe batch fails, the tap retries each object alone. An optional
  object that still fails logs a warning and is dropped.
- A failed dropdown call logs a warning. That picklist gets no label column.

Every tap run discovers again, so new custom fields appear on the next run.
When a run gets a catalog, the tap compares it with that discovery:

- A selected stream or column that is missing because a describe, dropdown
  or lookup-target call failed in this run fails the run, and the error
  names each one. The SDK would otherwise skip it without a word.
- A selected stream or column that Gainsight no longer has, such as an
  object or field an admin deleted, logs a warning. The rest syncs. Run
  discovery again to update the catalog.

## Record limits

Hotglue's field-sample job sends `_hg_max_records_limit`, a map from stream
name to a whole number of at least 1. Hotglue's own SDK reads it. The Meltano
SDK doesn't, so the tap applies it.

- A stream it names stops once it wrote that many records. It sends no
  request after that, and the run exits without an error.
- The limit counts across all partitions of a stream. After
  `deleted_records` reaches its limit, the other delete logs get no request.
- A limited stream moves no bookmark, because it read only part of the data.
- A stream it leaves out has no limit.
- The whole-second chain can still send one drain request after a scan page
  before it emits that page's last second. So a limit of 10 costs at most a
  few requests per stream, never a full read.
- Any other value, such as a string, 0 or a fraction, is a config error. A
  sync stops before any request. Discovery skips config validation, so it
  fails when it builds the streams.

## Sync behavior

- **MDA paging (whole-second chain).** Streams with a replication key read
  in a chain of whole seconds, 5000 rows a page, with AND-only filters:
  1. Scan: `ModifiedDate GTE L` (or `IS_NOT_NULL` on a first run), ordered by
     `ModifiedDate` then `Gsid`. Rows in every second before the page's last
     second are complete, so the tap emits them.
  1. Drain the page's last second s:
     `ModifiedDate GTE s AND ModifiedDate LT s+1s AND Gsid GT g`, ordered by
     `Gsid`, until a page is short.
  1. Set L to s+1s and scan again.

  A row edited or deleted during the sync cannot hide another row, as it can
  with offset paging, and no filter needs milliseconds. If a drain returns
  none of the rows the scan listed for its second, the tap reads one of them
  by `Gsid`. If it still has that second, the API read the filter in another
  time zone or grain, and the stream fails rather than skip rows. Rows a
  server returns outside the range asked for are duplicates: the tap drops
  them and counts them in a debug log. A last pass reads rows with a null
  `ModifiedDate`: `ModifiedDate IS_NULL`, then `AND Gsid GT g`.
- **Full-table MDA streams.** Objects with no usable replication key page by
  `Gsid GT g`, ordered by `Gsid`. An object whose `Gsid` is not sortable
  falls back to `offset` paging.
- **Delete log paging.** The same chain on `(DeletedOn, RecordId)`. A tenant
  with no high-volume log gets "Requested object not found" there. The tap
  treats that as empty and logs a warning. Any other error fails.
- **CTA windows.** The CTA list APIs page by `pageNumber` and document no sort
  order, so paging can skip or repeat rows. The tap reads `ModifiedDate`
  windows that each fit in one page of 1000, with the documented filter
  `ModifiedDate BTW [first day, last day]` and `yyyy-MM-dd` values:
  - The docs do not say whether `BTW` includes all of the last day or only its
    first instant. Windows overlap by one day, each starting on the day the
    last one ended, so either reading covers every day. At worst a CTA
    arrives twice.
  - The first window is [d, d+1], where d is the day of the bookmark or
    `start_date` less 24 hours. That is also the shortest window.
  - A longer window that fills a page is halved and read again. A shortest
    window that fills a page is read twice with `pageNumber`, and the two
    reads are merged by `Gsid`, with a warning. An unordered page read can
    miss a CTA edited during the sync. It is lost only if both reads miss
    it. A miss in a recent window is picked up by the next run's lookback.
    A miss in an older backfill window is picked up only when that CTA is
    edited again.
  - After a window less than half full, the next window doubles, up to 366
    days. Windows run to tomorrow, UTC.

  A CTA outside its window comes from a server that reads the days in
  another time zone. It is emitted, and counted in a debug log. With no
  bookmark and no `start_date`, windows start at 2000-01-01, about 36 calls
  for an empty tenant. `cta` also reads CTAs with a null `ModifiedDate` in a
  last pass, with the documented `IS_NULL` filter. `cta_deleted` has no such
  pass, because the Retrieve Deleted Data samples show no `IS_NULL` filter.
- **Date filters and the 24-hour lookback.** Query API filters use
  `yyyy-MM-dd HH:mm:ss` in UTC, the form in the delete log sample. CTA
  filters use `yyyy-MM-dd`. The docs do not say which time zone a filter
  value is read in, so every first filter starts 24 hours before its
  bookmark or `start_date`. Targets dedupe the overlap by key.
- **Rate limits.** Gainsight documents 100 synchronous calls per minute.
  The tap sends at most 30 a minute by default, through one client-side
  limiter for all streams and metadata calls. Retries count against it.
- **Retries.** HTTP 429, 5xx, connection errors and timeouts back off
  exponentially, up to 8 tries and 60 seconds a wait. The stream then fails.
- **Redirects.** The tap never follows a redirect, because `requests` would
  resend the `AccessKey` header to the new host. A 3xx fails with the host it
  pointed to.
- **Errors.** Any other 4xx fails the stream at once. So does a 200 whose
  body has `result: false`, a body that is not JSON, or a `data` shape the
  docs do not show. The error includes the status and a response summary. The
  documented empty replies count as an empty page at HTTP 200 or any 4xx:
  "No data found for given criteria" and GSOBJ_1011 "No entity matches the
  given criteria", which the Error Codes article lists as HTTP 400.

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
| CTAs | `POST /v2/cockpit/cta/list` | [Call To Action (CTA) API](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Call_To_Action_(CTA)_API_Documentation), Fetch CTA API |
| Deleted CTAs | `POST /v2/cockpit/cta/deleted/list` | [Retrieve Deleted Data API](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Cockpit_API/Retrieve_Deleted_Data_API), CTA, Endpoint One |
| Deleted records | `POST /v1/data/objects/query/record_delete_log` and `.../record_delete_log_high_volume` | [Data Management APIs](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Data_Management_APIs/Data_Management_APIs), Retrieve Deleted Data API |

Other facts and where they come from:

| Fact | Source |
|---|---|
| Header `accesskey`, and Content Type JSON | Authentication and Headers sections of every page above |
| 100 synchronous calls a minute, fixed window | Throttling Limits sections of the same pages |
| Query `select`, `where`, `orderBy`, `limit`, `offset`, and the operator enums | Custom Object API, Read API |
| At most 5000 rows a query, and `offset` is a starting index | Company API and Custom Object API, Read API |
| Query dates return as epoch milliseconds | Custom Object API, Sample Success Response notes |
| Empty results: `data.records: []`, `result: false` with "No data found", and GSOBJ_1011 at HTTP 400 | Custom Object API and Company API, Read API failure samples. [Error Codes](https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Error_Codes_for_Company%2C_Relationship%2C_and_Custom_Object_APIs) |
| Auth error codes `GS_APIG_2401` and `GSOBJ_1024` | Data Management APIs failure samples. Error Codes |
| "Requested object not found" (`OBJECT_NOT_FOUND`) | Data Management APIs, Get Describe OMD failure sample |
| DateTime format `yyyy-MM-dd'T'HH:mm:ss.SSSZ` | Timeline APIs, custom field Data Type table |
| CTA fields, `fieldName` conditions, `pageSize`, `pageNumber`, at most 1000 a page | Call To Action (CTA) API, Fetch CTA API |
| CTA `ModifiedDate` and `ModifiedById`, deleted-CTA select list and response fields | Retrieve Deleted Data API, CTA |
| Deleted data kept for 15 days | Data Management APIs and Retrieve Deleted Data API |

## Known gaps and unverified behavior

Check these against a live tenant before release, roughly in this order:

- **Three-term AND.** The docs show `A AND B`. The drain step sends
  `A AND B AND C`. That is the one extension beyond the documented form.
- **Filter time zone in the chain.** The first filter has a 24-hour
  lookback. Later scan and drain filters come from row values and assume the
  API reads `yyyy-MM-dd HH:mm:ss` in UTC, or in `filter_timezone` when it is
  set. If it does not, the drain check fails the stream loudly and names the
  setting. It never skips rows silently.
- **Date-grain query filters.** If the query API compared DateTime filters
  by date only, a one-second drain could not work. The drain check then
  fails the stream loudly.
- **CTA `BTW` reading.** The overlapping windows cover either reading of the
  end day, at the cost of some duplicate CTAs.
- **`timeline` replication key.** See the stream note above.
- **Object name case.** Describe calls use the listed name, such as
  `company`. Queries use the documented casing for documented objects, such
  as `Company`, and the listed name for the rest. If the query API is
  case-sensitive in another way, discovery works but those queries fail.
- **Picklist metadata.** No describe sample in the docs shows a picklist field.
  The tap reads labels from any list under a describe key containing
  "picklist" whose items have `gsid`. That is the dropdown API's item shape.
  Otherwise it reads a `categoryId` and calls the dropdown API. If a live
  describe uses another shape, picklists get no label column. Ids still sync.
- **Deleted flags.** A field-level `deleted` flag is inferred from the
  lookup detail's `deleted` key in the describe sample. Hidden fields are
  kept, because hiding changes the UI, not the data.
- **Deleted CTAs with a null `ModifiedDate`.** `cta_deleted` does not read
  them. The docs show no `IS_NULL` filter for that endpoint.
- **Dropdown and multi-select type names.** The docs do not give their
  describe `dataType` values. They map to "any JSON type".
- **Response `data` shape.** The Company and CTA pages show `data` as a list.
  The Timeline and delete log pages show `data.records`. The tap accepts
  both.
- **CTA `CreatedDate`.** No CTA sample selects it. The tap selects it because
  it is the standard MDA audit field. If the Fetch CTA API rejects it, the
  stream fails with `COCKPIT_5101` and says so.
- **CTA relationship fields and `ClosedDate`.** The CTA docs name
  "RelationshipID" and "Relationship Type" in prose only, with no API names.
  `ClosedDate` appears only in a Task sample. None of these are in the schema.
- **Object list scope.** The documented list call passes `po=company`
  ("parent object marker"). The docs do not say whether it limits the list.
  Use `objects` to name any object the list leaves out, such as `Person`.
- **Select size.** The docs state no limit on select list length, and queries
  are POST bodies. The tap does not split wide objects across requests.
- **Scorecards.** Scorecard objects are documented by display name only, and
  Scorecard 2.0 fact objects have per-scorecard generated names. There is no
  scorecard stream. A tenant can add a scorecard object by API name in
  `objects`.

## Local development

You need Python 3.10 and Poetry 1.6.1.

```bash
poetry install
poetry run tap-gainsight --about
poetry run tap-gainsight --config config.json --discover > catalog.json
poetry run tap-gainsight --config config.json --catalog catalog.json --state state.json
poetry run pytest
```

`pytest` reports coverage for `tap_gainsight/` and fails under 90%. Each
test has a 60-second timeout (`pytest-timeout`), so a hang fails fast with a
traceback of every thread instead of stalling CI.

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

The fake API in `tests/conftest.py` is case-sensitive, and its query engine
honors `where`, `expression`, `orderBy`, `limit` and `offset`, and
`pageSize` and `pageNumber`. It rejects parentheses in `expression`. It can
serve CTA pages in an unstable order, misread time zones, compare filters at
second or date grain, and read `BTW` as including all of the end day. The
tests use these modes to show what the paging schemes protect against.

The suite covers:

- Contract tests that match each request to the documented shape.
- Behavior tests for each stream.
- Regression tests for the review findings, in `tests/test_regressions.py`
  and `tests/test_regressions_delta.py`.
- An end-to-end run of the CLI: `--discover`, then a sync with a catalog and
  state.

## CI

`.github/workflows/ci.yml` defines one job named `CI`. The org ruleset
requires it on every PR to `main`. Don't rename it, because the ruleset
matches the literal string `CI`. The job runs the full test suite with the
coverage gate, and `--about`.
