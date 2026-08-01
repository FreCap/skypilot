# Progressive SkyServe dashboard loading and actionable health

_Created: 2026-08-01_

_Status: v0 deployed; v1a merged; v1b implemented pending merge; v1c accepted_
_Last updated: 2026-08-01_

## Problems

Before v0, the services list blocked its first useful rows on one aggregate status request. The deployed v0 now renders metadata first, but it starts the summary only after metadata settles. Production verification measured 6.36 seconds for the sequential pair and 3.09 seconds when both requests started together.

The service detail still couples unrelated costs in one deferred full-status request: live autoscaler data, 24 hours of history, current replica detail, every retained attempt, endpoint resolution, and service YAML. For `boltz-l4-fleet`, the page transferred about 2.5 MB of 24-hour history even though its initial selection is one hour; the equivalent one-hour history was 113 KB. The direct PostgreSQL history query took 87 ms for one hour and 170 ms for 24 hours, while the scheduled status path retained a roughly 2.8-second controller-transport floor. The full fleet response also serialized hundreds of replica rows although the page displayed at most 50 historical attempts.

The list also renders retained terminal attempts as a red `+N failed` suffix beside the live replica count. That is not the service-health contract. At investigation time `boltz-l4-fleet` was `READY` with 1/1 serving replica and no cleanup failure, while the database retained 321 unsuccessful attempts across old and current versions. SkyServe had already replaced those attempts. The primary UI therefore described normal automatic recovery as a current incident and offered no useful operator action.

## Goals

Both routes should paint useful, trustworthy information as soon as each data class is available. Slow or optional sections should have stable placeholders and must not delay service identity or current health. Refreshes must preserve the last good snapshot instead of blanking the page.

Health presentation must distinguish current service impact from retained attempt history. A healthy service that meets its serving target should explicitly say that no action is required. Raw replica statuses must remain available for diagnosis, with reason-specific guidance when the current service is degraded.

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
- `GET /serve/replica-summaries?service_name=N` returns one batched persisted projection for the selected or all non-pool services: service hash, replica unit, physical status counts, logical planned-capacity counts, current-or-uncertain count, past-attempt count, and observation time. The query scans normalized compact state once rather than issuing one query per service.
- `GET /serve/{service_name}/replicas?scope=current_or_uncertain|past_attempts&limit=N&cursor=C&expected_service_hash=H` returns a descending, cursor-paginated lightweight replica projection with `1 <= limit <= 100` and a default of 50. The response includes `total` and `next_cursor`, so disclosure counts and pagination do not depend on loaded rows. Both scopes expose explicit load-more behavior; current or uncertain rows are never silently truncated, while past pages remain inside their disclosure. Rich current-row pricing and endpoint enrichment may arrive separately; past attempts never resolve handles, endpoints, or pricing.

All three routes preserve the read permissions of `POST /serve/status`: authenticated viewers are explicitly allowlisted for the three exact GET patterns, while write operations remain unavailable. They return no credentials, handles, stored YAML, or secrets. SkyServe status is currently a global read rather than workspace-filtered; these routes do not broaden that visibility. If status gains workspace filtering, the dashboard routes must use the same authorization helper rather than maintaining a second policy.

`past_attempts` is server-defined as exactly `FAILED`, `FAILED_INITIAL_DELAY`, `FAILED_PROBING`, or `FAILED_PROVISION`, matching the existing UI contract. `current_or_uncertain` contains every other known or future state, especially `FAILED_CLEANUP`, `UNKNOWN`, `PREEMPTED`, and `SHUTTING_DOWN`, so potentially live or verification-worthy rows remain visible. The summary response reports exact physical row and logical planned-capacity counts as observed in one bounded repeatable-read database snapshot, together with `observed_at`; the UI does not call an older count a live value.

Replica cursors are opaque, versioned keyset cursors over descending `replica_id`. They carry the service hash, scope, first-page maximum replica ID, and last replica ID. Each request supplies the expected service hash, so recreating a same-name service invalidates the cursor with `409`; the first-page maximum keeps later pages stable when newer attempts arrive. Each page filters before decoding legacy JSON/pickle replica state and reads at most `limit + 1` rows. Rows that transition between current and past across refreshes are deduplicated by replica ID, and a manual or visibility refresh replaces the current page and exact totals rather than adding counts from different snapshots.

The dashboard starts the list metadata and summary requests together and renders either phase as it arrives. On a cold consolidated detail route, the cheap existing metadata projection is the required identity anchor because it supplies the current service hash. As soon as that hash lands, the dashboard fans out one hour of direct history, the batched replica summary, and the first current-replica page concurrently while the controller-backed live summary proceeds independently. Direct responses are merged only when they match the anchored visible service incarnation. A refresh may reuse the visible hash to start those reads immediately, but a `409` invalidates that anchor and restarts from metadata. Each section has its own loading and unavailable state, and stale-response fencing checks both the route generation and service hash. Selecting 12 or 24 hours requests only that range; choosing a smaller range reuses already loaded data. A 404 from an older server or `available: false` from a non-consolidated server uses the existing v0 full-status path.

Past attempts are not requested until the disclosure is opened. Further pages load explicitly and retain the existing neutral explanation that replaced attempts are diagnostic history, not a current incident. Current or uncertain rows remain visible outside the disclosure, including cleanup failures and unknown states that may require verification. The direct replica query filters and bounds rows before deserializing replica state or resolving optional current-row cluster records. The existing full-status fallback continues to provide YAML on non-consolidated or older servers; lazy YAML is outside this performance slice.

During v1b, the existing full replica status request remains but no longer carries history. Direct selected-range history loads independently after the metadata hash anchor. A 404 from an older server or `non_consolidated` response falls back to a controller-backed summary status request for the selected history range, while the full replica request continues independently. A landed legacy service row whose nullable hash was never backfilled also uses that controller-backed path instead of waiting indefinitely for a hash. This keeps the compatibility path complete without making the modern path transfer the same history twice.

The controller-backed summary remains the authoritative fresh source for autoscaler target and request-pressure fields. Minute history may render first but must retain its observation time and must not be presented as a fresh target. If one independent enrichment fails, the last good data in other sections remains visible and only that section offers refresh-to-retry guidance.

The API version advances to 66 for the new routes. Existing clients and all existing `/serve/status` behavior remain unchanged. The new dashboard assets are served by the same API-server release that owns the routes; an already-open page spanning a server rollback may show the affected section as unavailable until reload, but must keep the last good snapshot rather than blanking the page.

This v1 does not change the clusters dashboard. Production measured the active cluster list at about 0.58 seconds and workspaces at about 0.27 seconds, so cluster cache seeding and future pagination-plugin preload guards are lower-priority, independently shippable follow-ups.

Deliver v1 as three mergeable milestones to keep review and rollback boundaries narrow:

1. v1a starts list metadata and summary concurrently and makes either arrival order monotonic. Merged in PR #1147.
2. v1b adds capability-gated selected-range direct history while retaining the existing full replica path and fallback. Implemented pending merge.
3. v1c adds the direct batched replica-summary projection and current/past replica pagination, then removes the eager full replica request from Overview.

Each milestone updates this canonical design in place, runs its focused frontend and backend tests, and passes the complete CI rollup on its exact pushed SHA before merge. Later milestones start from the verified merge of the preceding milestone rather than an unmerged stack.

## Alternatives considered

Only adding skeleton rows would improve perceived loading but still make the fast request compete with full history and would not fix the misleading health semantics.

Removing failed rows would make the page calmer but erase useful diagnostic evidence. Treating every terminal row as `Needs attention` is also incorrect because SkyServe intentionally retains prior attempts after automatic replacement.

Fetching every service in separate requests before rendering would amplify request and controller load. A single metadata projection followed by scoped enrichment provides progressive rendering without an uncontrolled fan-out.

Only starting the existing detail requests concurrently reduced measured wall time, but it delayed metadata by about half a second under contention and retained the multi-megabyte payload. Direct bounded PostgreSQL reads provide the stronger latency and carrying-cost boundary.

Returning every replica and collapsing it client-side preserves the old API shape but keeps database deserialization, pricing, transfer, and memory proportional to retained history. Cursor pagination makes those costs proportional to what the operator opens.

## Implementation details

The v1 implementation areas include the Serve dashboard REST router, indexed Serve-state page queries, API-version constants, `sky/dashboard/src/data/connectors/services.jsx`, the services list, the service detail hook, history range controls, and focused tests. Preserve request-version and service-hash fencing, cache-key separation, visibility refresh behavior, last-good snapshots, and all existing `/serve/status` defaults. No central database migration is required because the replica table already has `(service_name, status)` and primary-key `(service_name, replica_id)` indexes.

Tests must cover direct-route bounds and hash mismatches, current versus past classification, cursor stability, handle-free serialization, selected-range history loading, independent failure states, stale responses, route changes, retained last-good data during refresh, concurrent list merging in either arrival order, and paginated past-attempt disclosure. Build the dashboard and manually verify the services list and `boltz-l4-fleet` detail route before merge. Production deployment remains a separate explicitly authorized action.

## Release and rollback

Land through a PR from a clean worktree based on the current `origin/improvements`. Require the full CI rollup on the exact pushed SHA. If deployment is separately requested, use the current private OCI/Helm production path, preserve existing Helm values, and verify the final Helm revision, image and commit, API health, dashboard routes, and data-plane service health separately.

Rollback is the prior Helm revision. The API additions are optional and backward compatible, so rollback does not require a database migration.
