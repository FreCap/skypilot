# Progressive SkyServe dashboard loading and actionable health

_Created: 2026-08-01_

_Status: v0-v1d deployed; v1e implemented pending CI/merge/rollout_
_Last updated: 2026-08-14_

## Problems

Before v0, the services list blocked its first useful rows on one aggregate status request. The deployed v0 now renders metadata first, but it starts the summary only after metadata settles. Production verification measured 6.36 seconds for the sequential pair and 3.09 seconds when both requests started together.

Production profiling on 2026-08-14 found a new first-paint floor: both the
controller-backed metadata projection and controller-backed summary took
6.18-6.55 seconds for six services, while the existing direct PostgreSQL
replica-summary route returned in 0.13 seconds. The metadata payload was only
15 KB, so the delay came from controller compatibility transport rather than
database result size or browser rendering. The direct replica projection must
therefore carry the persisted lifecycle fields needed to paint list rows; live
controller fields remain progressive enrichment.

The service detail still couples unrelated costs in one deferred full-status request: live autoscaler data, 24 hours of history, current replica detail, every retained attempt, endpoint resolution, and service YAML. For `boltz-l4-fleet`, the page transferred about 2.5 MB of 24-hour history even though its initial selection is one hour; the equivalent one-hour history was 113 KB. The direct PostgreSQL history query took 87 ms for one hour and 170 ms for 24 hours, while the scheduled status path retained a roughly 2.8-second controller-transport floor. The full fleet response also serialized hundreds of replica rows although the page displayed at most 50 historical attempts.

The deployed v1c bounded replica projection intentionally omitted pricing and
replica endpoints to remove handle, cluster-record, and provider work from the
initial Overview path. That made the page fast, but it also left the service
cost card permanently at `Unavailable in bounded replica view` and every
current replica price at `Not loaded`. Pricing is not inherently coupled to
endpoint resolution: placer-managed service versions already persist one
complete exact-location-to-nominal-cost catalog, and replica rows already
persist their version, exact location, and zero-cost provenance. The dashboard
should consume that durable data independently instead of restoring full
status.

The list also renders retained terminal attempts as a red `+N failed` suffix beside the live replica count. That is not the service-health contract. At investigation time `boltz-l4-fleet` was `READY` with 1/1 serving replica and no cleanup failure, while the database retained 321 unsuccessful attempts across old and current versions. SkyServe had already replaced those attempts. The primary UI therefore described normal automatic recovery as a current incident and offered no useful operator action.

## Goals

Both routes should paint useful, trustworthy information as soon as each data class is available. Slow or optional sections should have stable placeholders and must not delay service identity or current health. Refreshes must preserve the last good snapshot instead of blanking the page.

Health presentation must distinguish current service impact from retained attempt history. A healthy service that meets its serving target should explicitly say that no action is required. Raw replica statuses must remain available for diagnosis, with reason-specific guidance when the current service is degraded.

Current-fleet pricing should arrive independently after first paint, preserve
last-good data during refresh, and never contact a controller, cloud provider,
or replica endpoint. The UI must identify the value as a version-catalog
placement estimate rather than a live provider quote or provider bill.

## Background

The existing `/serve/status` contract supports `metadata_only` and `summary_only`, but both are scheduled through the Serve controller compatibility transport. This is appropriate for live autoscaler state and backward compatibility, but unnecessary for centralized PostgreSQL history and persisted replica rows. The v0 detail hook intentionally orders metadata before the full request, so its first paint does not compete with heavy enrichment. Production profiling now satisfies the v1 trigger: the deferred payload remains operationally expensive after first paint.

The production observation is decisive for the status semantics:

- Service: `boltz-l4-fleet`, version 49, status `READY`, replicas 1/1.
- Retained terminal attempts across all versions: 199 `FAILED_PROVISION`, 64 `FAILED_INITIAL_DELAY`, 20 `FAILED_PROBING`, and 38 `FAILED`.
- Current version retained 19 `FAILED_PROVISION`, 2 `FAILED_PROBING`, 1 `FAILED`, and 1 `READY`.
- There was no `FAILED_CLEANUP` row. The serving target was met, so there was no current operator action.

## Solution

### v0: fast snapshot, staged enrichment, and honest health language

Add a backward-compatible `metadata_only` projection to the Serve status request. It returns a slim persisted service-row snapshot without deserializing the service spec or YAML, serializing replicas, aggregating replica rows, reading the autoscaler or history, resolving endpoints, or calling cloud/Kubernetes APIs. The optional field defaults to false, is mutually exclusive with history and autoscaler-target hydration, and requires an API version bump plus a new optional controller-RPC field and Serve library-version gate. Existing callers keep their current behavior. The response marks the projection explicitly so the dashboard cannot mistake an intentionally absent replica list for a complete empty one.

Existing long-running Serve v9 controllers ignore unknown protobuf fields, which would otherwise turn a metadata request into a full status read during a mixed-version rollout. The default server runner therefore routes metadata through a compatibility command built on the slim status-snapshot primitive already available in Serve v9. This lets existing services receive the fast first phase without being recreated. Newly updated runners may use the optional RPC field directly.

The services list will request metadata first and render rows immediately. Replica counts, endpoint resolution, and other computed cells will render skeleton placeholders. After metadata lands, a separate summary request will fetch aggregate replica counts and deferred endpoints and merge records by service name. Refreshes will keep the visible snapshot while individual fields refresh, and stale-response fencing will cover both phases. If enrichment fails, the metadata stays visible and deferred fields settle as unavailable with a refresh-to-retry explanation instead of displaying an indefinite loading state.

The service detail route will use an ordered pipeline rather than starting every request together:

1. Metadata-only status renders identity, persisted status, version, uptime, policy, resources, and page structure.
2. A single service-scoped full status request starts only after the fast snapshot is visible and only for the Overview tab. It fills replica counts, autoscaler target, endpoint, 24-hour history, pricing, placement inputs, and replica detail behind section-level placeholders. The Versions and Placement tabs do not start this full request until the user opens Overview. A failed full read preserves the metadata snapshot and identifies the unavailable computed sections with a refresh-to-retry action.

This two-request v0 avoids a third controller query while still preventing the expensive read from competing with the first paint. The existing REST request contract may be extended with optional flags, but defaults must preserve old client behavior. The fast snapshot must never wait for replica, autoscaler, endpoint, or history enrichment.

Replace the list's red `+N failed` suffix with a neutral `N past attempts` disclosure. Preserve the summary response's status histogram so cleanup failures and ordinary terminal history cannot be collapsed into the same count. Add a service health summary whose decision is based on current service status, known serving capacity, the autoscaler target when available, and active recovery state, not the existence of retained terminal rows:

- `READY` before the autoscaler target arrives: `Serving`, with target-dependent health still loading. Do not claim that the target is met from service status alone.
- Ready capacity meeting a known target: `Healthy`, followed by `Past attempts were replaced automatically. No action required.`
- Ready capacity below a known target while replicas are pending, provisioning, starting, or recovering: `Scaling automatically`; this is normal operation and does not ask for human action.
- `CONTROLLER_FAILED`, service `FAILED` with no serving replica, or another explicit current hard fault: `Needs attention`, with the immediate reason and a link or instruction to inspect controller or replica logs.
- `FAILED_CLEANUP`: `Cleanup needs verification`, explaining the risk of leaked cloud resources.
- `UNKNOWN` replica state: `Replica state needs verification`; keep it with current or uncertain rows rather than hiding it in past attempts.
- Provisioning, readiness, or runtime attempt failures while the target remains met: neutral history with reason-specific explanations, not a red incident state.

The dashboard must not infer a stalled autoscaler from a single snapshot. If capacity is below target without an explicit hard fault, it reports the observed shortfall and automatic activity rather than inventing an operator action.

On the detail page, show current or uncertain replica rows first. Put terminal replica records in a collapsed `Past attempts` section with a count and explanation. Retain the raw `FAILED_*` code in the row or tooltip, but use plain-language reason labels and avoid red emphasis unless the current health decision is actionable. Paginate or bound rendered historical rows so a large fleet does not create hundreds or thousands of DOM rows at once.

### v1: independent persisted history, counts, and replica pages

Production profiling shows that v0's deferred full request remains expensive, so v1 replaces it on the Overview route with bounded, independently rendered reads. These routes are dashboard-coupled, read-only APIs served directly by the API server when consolidated Serve makes the central database authoritative. They inherit the existing authentication and authorization middleware and use `asyncio.to_thread` for synchronous PostgreSQL and serialization work; they do not allocate request-executor slots or contact the Serve controller. Non-consolidated services keep replica authority on the remote controller, so every direct route returns `available: false` with reason `non_consolidated` in that topology and the dashboard preserves the v0 controller-backed fallback.

The API contracts are:

- `GET /serve/{service_name}/history?hours=N&section=S&expected_service_hash=H` returns only the requested aggregate history sections, where repeated `section` values select `requests`, `replicas`, `prediction`, or `autoscaler`. `hours` is bounded to the 72-hour retention contract. A hash mismatch returns `409` rather than mixing same-name service incarnations.
- `GET /serve/replica-summaries?service_name=N` accepts repeated optional `service_name` filters and returns `{available, observed_at, summaries}`. Each snake-case summary contains `service_name`, `service_hash`, `replica_unit`, `replica_status_counts`, `replica_capacity_counts`, `current_or_uncertain_count`, and `past_attempt_count`. The two totals count physical attempt rows; logical planned capacity remains a separate histogram. Omitting the filter selects all non-pool services. The query scans normalized compact state once rather than issuing one query per service.
- `GET /serve/{service_name}/replicas?scope=current_or_uncertain|past_attempts&limit=N&cursor=C&expected_service_hash=H` returns `{available, service_name, service_hash, scope, replica_unit, observed_at, total, replicas, next_cursor}` with snake-case, descending, cursor-paginated lightweight rows. The physical-row `total` uses the same scope classification as the summary, so disclosure counts and pagination do not depend on loaded rows. `1 <= limit <= 100`, with a default of 50. Both scopes expose explicit load-more behavior; current or uncertain rows are never silently truncated, while past pages remain inside their disclosure. Rich current-row pricing and endpoint enrichment may arrive separately; past attempts never resolve handles, endpoints, or pricing.
- `GET /serve/{service_name}/pricing?expected_service_hash=H&replica_id=I`
  returns one independently loadable persisted pricing projection. Repeated
  `replica_id` selects at most 100 visible current-or-uncertain rows; omitted
  IDs request only the whole-current-fleet aggregate. The two request modes do
  not overlap: the no-ID response owns `aggregate` and returns no replica
  results, while an ID response sets `aggregate` to null and returns exactly
  one result for every distinct requested ID. An ID absent from the current
  scope at that snapshot returns `not_current_or_uncertain`, so a status transition
  cannot leave the browser waiting indefinitely. The response contains
  `{available, service_name, service_hash, observed_at, price_basis,
  aggregate, replicas}`. `price_basis` is `version_catalog`. `aggregate`
  contains `available`, `unavailable_reason`, `coverage`,
  `known_hourly_cost`, Spot and non-Spot subtotals, tracked/priced/excluded
  physical-replica counts, and exclusion-reason counts. `coverage` is exactly
  `empty`, `complete`, `partial`, or `none`: an empty fleet has a known `$0`
  total, an all-zero-cost fleet has complete `$0` coverage, and an all-excluded
  fleet has no coverage and must not be displayed as zero. `empty` and
  `complete` carry numeric totals (including zero); `partial` carries numeric
  known subtotals and is explicitly a lower bound; `none` carries null cost
  totals with nonzero tracked/excluded counts. Each selected replica contains
  `replica_id`, nullable `pricing_fingerprint`, `hourly_cost`, `price_source`,
  and `hourly_cost_exclusion_reason`. A row absent at the snapshot has no
  fingerprint; that result settles the price request and immediately refreshes
  the current replica page instead of being cached or merged. Aggregate-level oversize reason is
  `projection_too_large`. The stable row and group exclusion vocabulary is
  `missing_version_catalog`, `unsupported_version_catalog`,
  `invalid_version_catalog`, `catalog_too_large`, `missing_location`,
  `invalid_location`, `location_not_in_version_catalog`,
  `ambiguous_legacy_location`, `catalog_price_unavailable`,
  `purchase_option_mismatch`, `unknown_node_count`, and
  `pricing_identity_too_large`, plus row-only `not_current_or_uncertain`. A
  hash mismatch returns `409`.
  The server rejects more than 100 raw repeated parameters before deduplicating
  them, and rejects IDs outside the positive PostgreSQL `INTEGER` domain.

All four direct-read route families preserve the read permissions of
`POST /serve/status`: authenticated viewers are explicitly allowlisted for the
exact GET patterns, while write operations remain unavailable. They return no
credentials, handles, stored YAML, or secrets. SkyServe status is currently a
global read rather than workspace-filtered; these routes do not broaden that
visibility. If status gains workspace filtering, the dashboard routes must use
the same authorization helper rather than maintaining a second policy.

The pricing projection uses the same repeatable-read service-incarnation fence
as replica pages. Its aggregate scans only current cost-tracked rows and groups
catalog-dependent rows in PostgreSQL by version, status, purchase option, exact
persisted location, and explicit zero-cost provenance. Explicit-zero rows use
a null location group key and never read, size-check, or group their persisted
location. It excludes `PENDING`, completed
failure attempts, and `PREEMPTED`; it conservatively includes stopping,
cleanup-uncertain, unknown, null, and future statuses because their external
resources may still exist. An ID-mode query selects at most the requested 100
rows by primary key and emits an explicit result for IDs missing from its
current-scope result.

Hard projection bounds are part of the API contract: at most 10,000
cost-tracked physical rows, 4,096 aggregate placement groups, 128 distinct live
versions, 10,000 total catalog entries, and 10,000 entries in any one catalog.
Serialized catalog limits are 8 MiB per catalog and 16 MiB across all catalogs
in one projection. Catalog metadata is inspected as JSONB, but an admitted
body is selected as bounded text and decoded under fail-closed exception
handling so a hostile JSON number cannot fail first in the database driver's
implicit JSON decoder. A separate 64 KiB limit applies to any catalog-dependent
replica's combined persisted location and pinned-override identity.
Before grouping, a primary-key-only current-cost-row probe uses `limit + 1` so
many rows in one group cannot cause an unbounded aggregate scan; the group
query also uses `limit + 1`. Catalog metadata is inspected before catalog JSON
is selected, including PostgreSQL `octet_length(catalog::text)`. A SQL
size predicate and `CASE` detect an oversized catalog-dependent replica
identity before selecting or grouping its JSON body; those rows are grouped by
the exclusion flag with a null location key and counted as
`pricing_identity_too_large`. If an aggregate exceeds a row, group, version,
catalog-entry, or catalog-byte bound, `aggregate.available` is false
with `unavailable_reason` equal to `projection_too_large`, all aggregate
numbers and coverage are null, and no partial total is returned. ID mode still
settles all requested IDs: explicit zero-cost rows remain `$0`, while
catalog-dependent rows affected by a bound return `catalog_too_large`. Missing
or structurally invalid catalog metadata remains an ordinary explicit
per-placement exclusion rather than an unbounded JSON read. The 64 KiB
identity limit is not a whole-aggregate failure: aggregate and ID modes both
return `pricing_identity_too_large` for only the affected catalog-dependent
rows. In ID mode its fingerprint is null and the result is a request-scoped,
non-cacheable exclusion. Result
cardinality and all catalog/location JSON selected into the API process are
thus bounded independently of retained attempts, version history, and the
catalog's larger placement-time maximum.

Each admitted version catalog builds its exact, legacy-shape, coordinate, and
cost indexes once per pricing request. Every placement group and requested row
then performs bounded indexed matching instead of rebuilding a cost map or
rescanning as many as 10,000 catalog entries. This keeps resolver work
proportional to catalog entries plus projected rows/groups, rather than their
product, while retaining the shared strict matching semantics.

`is_zero_cost: true` resolves to `0.0` without requiring a catalog, location,
or node count; this is the only zero-cost precedence rule. Otherwise cataloged
Kubernetes locations resolve to `0.0` through their stored catalog entry. The
row's persisted `Location` is resolved against its own immutable
service-version catalog by a pure strict matcher shared with placement code:
exact equality wins; an instance-type-less or shape-less legacy location is
accepted only when exactly one catalog entry is compatible. The matcher does
not construct a task or resources object and never uses the placer's ambiguous
cheapest fallback. A missing `location` may fall back only to
`Location.from_resources_override`, which requires the placer pin signature
of cloud and region and then passes through the same strict matcher. The
replica column's purchase option must agree with the resolved
`Location.use_spot`; disagreement fails closed as
`purchase_option_mismatch`. Missing/corrupt catalogs,
missing/unresolvable/ambiguous locations, and catalog entries with no nominal
price remain explicitly excluded; no provider, cluster-record, handle,
endpoint, or YAML fallback runs on this route. The known total is a lower bound
whenever coverage is partial.

A placement-catalog entry is a per-node price. New catalog writes add a
strictly positive top-level `num_nodes` to the existing JSON value without a
database migration; placer ranking continues to use the unchanged per-node
entry values, while the dashboard multiplies a resolved positive entry by that
version's node count. The pricing read never infers a node count: any positive
legacy catalog entry without an explicit validated `num_nodes` is excluded as
`unknown_node_count`, including when the service's current global semantics are
logical because a physical-to-logical rolling transition can leave older
physical versions active. A future controller-owned compare-and-set backfill
may improve legacy coverage, but is not part of this read-only enrichment.
Explicit zero-cost provenance and a resolved zero-valued catalog entry remain
exactly zero for any node count. A physical-backend hourly estimate is never
multiplied by logical `planned_capacity`.

Numeric replica IDs are reusable within one service incarnation. Both the
bounded current-row projection and ID-mode pricing response therefore carry an
opaque `pricing_fingerprint`. It always hashes both the physical-record
identity and every persisted price input: version, purchase option, exact
location or pinned override, and zero-cost provenance. The record component is
the durable `replica_record_id` when present and a deterministic legacy identity
otherwise. This remains safe when an existing record is relabeled in place to
a newer service version with a different catalog. The browser merges or
retains a price only when service hash, replica ID, and fingerprint all match;
a mismatched in-flight response is ignored and retried.
Each priced row additionally reports `price_source` as
`zero_cost_provenance` or `version_catalog`, so explicit reserved-capacity zero
is not mislabeled as a catalog lookup.

`past_attempts` is server-defined as exactly `FAILED`, `FAILED_INITIAL_DELAY`, `FAILED_PROBING`, or `FAILED_PROVISION`, matching the existing UI contract. `current_or_uncertain` contains every other known or future state, especially `FAILED_CLEANUP`, `UNKNOWN`, `PREEMPTED`, and `SHUTTING_DOWN`, so potentially live or verification-worthy rows remain visible. The summary response reports exact physical row and logical planned-capacity counts as observed in one bounded repeatable-read database snapshot, together with `observed_at`; the UI does not call an older count a live value.

Replica cursors are opaque, versioned keyset cursors over descending `replica_id`. They carry the service hash, scope, first-page maximum replica ID, and last replica ID. Each request supplies the expected service hash, so recreating a same-name service invalidates the cursor with `409`; the first-page maximum keeps later pages stable when newer attempts arrive. Each page filters before decoding legacy JSON/pickle replica state and reads at most `limit + 1` rows. Rows that transition between current and past across refreshes are deduplicated by replica ID, and a manual or visibility refresh replaces the current page and exact totals rather than adding counts from different snapshots.

The dashboard starts the list metadata and summary requests together and renders either phase as it arrives. On a cold consolidated detail route, the cheap existing metadata projection is the required identity anchor because it supplies the current service hash. As soon as that hash lands, the dashboard fans out one hour of direct history, the batched replica summary, the dedicated no-ID pricing aggregate, and the first current-replica page concurrently while the controller-backed live summary proceeds independently. After a current page lands, separate ID-mode pricing requests select only rows that do not already have a result, in chunks no larger than 100. Row responses never update aggregate state, preventing chunk order from replacing a newer aggregate. Direct responses are merged only when they match the anchored visible service incarnation. A refresh may reuse the visible hash to start those reads immediately, but a `409` invalidates that anchor and restarts from metadata. Each section has its own loading and unavailable state, and stale-response fencing checks both the route generation and service hash. Selecting 12 or 24 hours requests only that range; choosing a smaller range reuses already loaded data. A 404 from an older server or `available: false` from a non-consolidated server uses the existing v0 full-status path.

For the services list in v1e, the batched replica-summary route also returns
the compact persisted service status, uptime, policy, and requested-resource
string from the same repeatable-read query. That projection is the canonical
first paint in consolidated mode and may create rows before controller
metadata arrives. The controller summary remains authoritative for endpoints,
autoscaler targets, request pressure, and other live fields and merges later.
Older/non-consolidated servers keep the v0 metadata-first fallback. A direct
first paint ends the page-level loading state while deferred cells retain their
own loading indicators. A top-level `service_metadata_included: true`
capability marker prevents a new dashboard bundle from treating a replica-only
response from an older API pod as first-paint metadata during a rolling update.

Pricing has its own loading, unavailable, and last-good state. A failure never
blanks replica counts, endpoints, history, or prior prices. Newly loaded current
rows trigger pricing only for their missing IDs. Positive current-row results,
including explicit zero, are immutable for a
service-hash/replica-ID/pricing-fingerprint tuple and may be retained for that
record. Negative results are retried on the next
visibility or manual pricing refresh because a placement catalog may be
backfilled after the first read. Past attempts never trigger a pricing request
and cached current-row prices are never merged into a row that moves to the
past-attempt scope. `PENDING` and `PREEMPTED` rows remain outside the fleet-cost
aggregate but, while visible in the current-or-uncertain table, may display
their nominal persisted placement price. The aggregate drives the service cost
card and cost-per-1K calculation; row results drive only the current replica
table. A version-catalog result is labeled `Each replica version's deployment
catalog; reserved $0 from persisted placement provenance; compute estimate,
not a provider bill`; a non-consolidated legacy full-status fallback retains
its existing current-catalog label. The UI displays explicit zero as `$0`, and
distinguishes empty, all-zero, partially covered, excluded, loading, and
unavailable pricing.

Past attempts are not requested until the disclosure is opened. Further pages load explicitly and retain the existing neutral explanation that replaced attempts are diagnostic history, not a current incident. Current or uncertain rows remain visible outside the disclosure, including cleanup failures and unknown states that may require verification. The direct replica query filters and bounds rows before deserializing replica state or resolving optional current-row cluster records. The existing full-status fallback continues to provide YAML on non-consolidated or older servers; lazy YAML is outside this performance slice.

During v1b, the existing full replica status request remains but no longer carries history. Direct selected-range history loads independently after the metadata hash anchor. A 404 from an older server or `non_consolidated` response falls back to a controller-backed summary status request for the selected history range, while the full replica request continues independently. A landed legacy service row whose nullable hash was never backfilled also uses that controller-backed path instead of waiting indefinitely for a hash. This keeps the compatibility path complete without making the modern path transfer the same history twice.

The controller-backed summary remains the authoritative fresh source for autoscaler target and request-pressure fields. Minute history may render first but must retain its observation time and must not be presented as a fresh target. If one independent enrichment fails, the last good data in other sections remains visible and only that section offers refresh-to-retry guidance.

Direct selected-range history introduced in v1b requires API version 66. The replica-summary and replica-page routes introduced in v1c require API version 67, because an exact v1b server legitimately reports version 66 while returning `404` for those later routes. The dashboard therefore uses separate capability constants: a replica-route `404` from version 66 triggers the v0 full-status fallback, while the same `404` from version 67 is a real missing-service result. Existing clients and all existing `/serve/status` behavior remain unchanged. The new dashboard assets are served by the same API-server release that owns the routes; an already-open page spanning a server rollback may show the affected section as unavailable until reload, but must keep the last good snapshot rather than blanking the page.

Persisted pricing requires API version 71. A pricing-route `404` from a server
below 71 means unsupported and settles only pricing as unavailable; it must not
restore the expensive full-status request on an otherwise modern consolidated
server. A `404` from version 71 or later is a real missing-service result. A
non-consolidated response retains the existing v0 full-status fallback, which
already supplies current-catalog pricing. Existing clients, replica routes, and
all `/serve/status` defaults remain unchanged.

This v1 does not change the clusters dashboard. Production measured the active cluster list at about 0.58 seconds and workspaces at about 0.27 seconds, so cluster cache seeding and future pagination-plugin preload guards are lower-priority, independently shippable follow-ups.

Deliver v1 as four mergeable milestones to keep review and rollback boundaries narrow:

1. v1a starts list metadata and summary concurrently and makes either arrival order monotonic. Merged in PR #1147.
2. v1b adds capability-gated selected-range direct history while retaining the existing full replica path and fallback. Merged in PR #1151.
3. v1c adds the direct batched replica-summary projection and current/past replica pagination, then removes the eager full replica request from Overview. Merged in PR #1156.
4. v1d restores current-fleet and visible-current-row pricing from immutable
   placement catalogs through an independently capability-gated projection.

Each milestone updates this canonical design in place, runs its focused frontend and backend tests, and passes the complete CI rollup on its exact pushed SHA before merge. Later milestones start from the verified merge of the preceding milestone rather than an unmerged stack.

## Alternatives considered

Only adding skeleton rows would improve perceived loading but still make the fast request compete with full history and would not fix the misleading health semantics.

Removing failed rows would make the page calmer but erase useful diagnostic evidence. Treating every terminal row as `Needs attention` is also incorrect because SkyServe intentionally retains prior attempts after automatic replacement.

Fetching every service in separate requests before rendering would amplify request and controller load. A single metadata projection followed by scoped enrichment provides progressive rendering without an uncontrolled fan-out.

Only starting the existing detail requests concurrently reduced measured wall time, but it delayed metadata by about half a second under contention and retained the multi-megabyte payload. Direct bounded PostgreSQL reads provide the stronger latency and carrying-cost boundary.

Returning every replica and collapsing it client-side preserves the old API shape but keeps database deserialization, pricing, transfer, and memory proportional to retained history. Cursor pagination makes those costs proportional to what the operator opens.

Restoring the old full-status request solely for prices would again deserialize
every retained attempt, batch cluster records, and couple pricing to controller
transport and endpoint resolution. Calling `Resources.get_cost()` on every
dashboard refresh would describe a fresher catalog but reintroduce runtime
pricing work and would disagree with the immutable nominal costs used by the
service's own placer. A new cross-version TTL cache would duplicate the
existing version catalog and require an invalidation contract. v1d instead
exposes the durable price actually associated with each placement decision and
labels its version-time semantics honestly.

## Implementation details

The v1 implementation areas include the additive placement-catalog node-count
field and shared strict location matcher, the Serve dashboard REST router,
indexed Serve-state page queries, API-version constants,
`sky/dashboard/src/data/connectors/services.jsx`, the service detail hook, and
focused tests. Preserve request-version, service-hash, and replica-record
fencing, cache-key separation, visibility refresh behavior, last-good
snapshots, and all existing `/serve/status` defaults. No central database
migration is required because the replica table already has
`(service_name, status)` and primary-key `(service_name, replica_id)` indexes,
while `version_specs.placement_catalog` already owns the immutable price cache.

Tests must cover direct-route bounds and hash mismatches, raw-before-dedup ID
validation, aggregate-only versus row-only request modes, every-ID settlement,
hard row/group/version/catalog-entry/catalog-byte/location-byte fail-closed
behavior, including many rows in one placement group,
empty/all-zero/partial/none coverage, the distinct
v66-history, v67-replica, and v71-pricing
capability gates, current versus past and cost-tracked classification, cursor
stability, handle-free serialization, exact and unique legacy catalog matching,
ambiguous and purchase-option mismatches, zero-cost rows, missing/corrupt/
oversized catalogs and locations, new single-/multi-node catalogs and
zero/positive legacy catalogs without node-count evidence,
node-count multiplication without logical-capacity multiplication,
replica-ID reuse, in-place version relabeling, and fingerprint mismatch,
no-provider/no-handle pricing,
PostgreSQL query compilation and execution without a central-SQLite branch,
selected-range history loading, independent failure states, stale responses, route changes,
retained last-good data during refresh, concurrent list merging in either
arrival order, aggregate/chunk response reordering, incremental price loading
for newly paged current rows, retryable negative row results, current-to-past
transitions, missing-ID nullable fingerprints with immediate page refresh,
past-attempt non-loading, and paginated past-attempt disclosure.
Build the dashboard and manually verify the services list and
`boltz-l4-fleet` detail route before merge. Production deployment remains a
separate explicitly authorized action.

## Verification evidence and open gates

Local evidence on 2026-08-04:

- `format.sh --files ...` completed YAPF/isort, mypy (883 source files),
  pylint (10.00/10), dashboard ESLint (zero warnings), and Prettier.
- The focused dashboard connector/detail suites passed all 113 tests.
- The focused pricing, API route, viewer-RBAC, and placement-catalog Python
  suites passed.
- The optimized dashboard production build compiled, type-checked,
  prerendered all 30 pages, and completed successfully.
- Two independent adversarial reviews accepted the final pricing contract and
  implementation with no remaining correctness, security, or performance
  blockers.
- The real-PostgreSQL aggregate/ID pricing test is collected, but skipped on
  this host because Docker and `SKYPILOT_TEST_POSTGRES_URL` are unavailable.

The real-PostgreSQL pricing test must execute in the required PostgreSQL CI
lane; its local skip does not close the merge gate. Manual `boltz-l4-fleet`
verification, the full CI rollup on the exact pushed SHA, merge, and any
production deployment remain open gates until their evidence is recorded
here.

## Release and rollback

Land through a PR from a clean worktree based on the current `origin/improvements`. Require the full CI rollup on the exact pushed SHA. If deployment is separately requested, use the current private OCI/Helm production path, preserve existing Helm values, and verify the final Helm revision, image and commit, API health, dashboard routes, and data-plane service health separately.

Rollback is the prior Helm revision. The API additions are optional and backward compatible, so rollback does not require a database migration.
