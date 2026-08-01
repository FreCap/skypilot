# Progressive SkyServe dashboard loading and actionable health

_Created: 2026-08-01_

_Status: Implemented, pending merge and rollout_
_Last updated: 2026-08-01_

## Problems

The services list blocks its first useful rows on one aggregate status request. In production that request took about 2.9 seconds for six services because it includes replica-status aggregation, including thousands of retained terminal attempts. The service detail route starts its summary/history request and its full replica-inventory request together. For `boltz-l4-fleet`, the full response materializes more than 300 retained replica attempts and the dashboard renders every row, so expensive work can contend with the fast request and then create a large DOM.

The list also renders retained terminal attempts as a red `+N failed` suffix beside the live replica count. That is not the service-health contract. At investigation time `boltz-l4-fleet` was `READY` with 1/1 serving replica and no cleanup failure, while the database retained 321 unsuccessful attempts across old and current versions. SkyServe had already replaced those attempts. The primary UI therefore described normal automatic recovery as a current incident and offered no useful operator action.

## Goals

Both routes should paint useful, trustworthy information as soon as each data class is available. Slow or optional sections should have stable placeholders and must not delay service identity or current health. Refreshes must preserve the last good snapshot instead of blanking the page.

Health presentation must distinguish current service impact from retained attempt history. A healthy service that meets its serving target should explicitly say that no action is required. Raw replica statuses must remain available for diagnosis, with reason-specific guidance when the current service is degraded.

## Background

The existing `/serve/status` contract supports `summary_only`, but even that mode aggregates retained replica rows. The detail hook already issues separate summary and full requests, yet it starts them concurrently and couples 24-hour history plus autoscaler target computation to the first summary. Full status serializes all retained replicas and the replica table renders them all.

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

### v1: avoid eager historical payloads if profiling still shows material cost

If v0 verification shows that deferred full status remains operationally expensive, add a paginated replica-history endpoint backed by indexed `service_name`, `status`, and `replica_id` reads. The Overview route would fetch current/nonterminal replicas only; terminal history would load on expansion. This is independently shippable and should not be included in v0 unless measurements show that client-side collapsing is insufficient.

## Alternatives considered

Only adding skeleton rows would improve perceived loading but still make the fast request compete with full history and would not fix the misleading health semantics.

Removing failed rows would make the page calmer but erase useful diagnostic evidence. Treating every terminal row as `Needs attention` is also incorrect because SkyServe intentionally retains prior attempts after automatic replacement.

Fetching every service in separate requests before rendering would amplify request and controller load. A single metadata projection followed by scoped enrichment provides progressive rendering without an uncontrolled fan-out.

## Implementation details

Expected implementation areas include `sky/server/requests/payloads.py`, API-version and Serve status runner/RPC projection code, a slim Serve-state metadata query, `sky/dashboard/src/data/connectors/services.jsx`, the services list, the service detail hook, and their focused tests. Preserve request-version fencing, cache-key separation, visibility refresh behavior, and old status defaults. The deferred summary may opt into endpoint hydration, but the metadata projection itself must remain free of Kubernetes or cloud calls.

Tests must cover metadata projection cost boundaries, default API compatibility, two-phase list merging, the ordered metadata-then-full detail requests, stale responses, route changes, retained last-good data during refresh, healthy service messaging with historical failures, actionable failure messaging, and bounded/collapsed history rendering. Build the dashboard and perform authenticated live verification on both requested production routes after deployment.

## Release and rollback

Land through a PR from a clean worktree based on the current `origin/improvements`. Require the full CI rollup on the exact pushed SHA. Deploy the merged commit through the current private OCI/Helm production path, preserving existing Helm values. Verify the final Helm revision, image and commit, API health, services list, `boltz-l4-fleet` detail page, and data-plane service health separately.

Rollback is the prior Helm revision. The API additions are optional and backward compatible, so rollback does not require a database migration.
