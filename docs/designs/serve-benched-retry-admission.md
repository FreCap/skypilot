# Preserve consumed bench retries through launch admission

_Created: 2026-07-21_

## Problem

SkyServe temporarily benches a spot-placement location after a launch failure.
After the retry TTL expires, selecting that location deliberately refreshes the
bench timestamp so a burst of scale-ups consumes only one probe per TTL window.
The replica row is then persisted as `PENDING` and admitted to the bounded
launch pool on a later controller pass.

Launch admission currently rejects every queued row whose location is not
effectively active. The retry selection itself has just refreshed the timestamp,
so the one permitted probe appears benched and is discarded before
`sky.launch` starts. A location that fails once can therefore remain unusable
forever even after capacity recovers.

This was observed on `boltz-l4-fleet`: SkyPilot measured 99 free reserved GPU
slots in `prod_research_cluster_eks`, selected Kubernetes retry probes, then
discarded each queued row as benched and scaled paid AWS capacity instead.

## Goals

Allow the single retry selected after a bench TTL to pass bounded launch
admission, while continuing to reject queued siblings when a newer failure
benches their location. Preserve the existing one-probe-per-TTL behavior,
resource admission limits, controller ownership fences, and failure-driven
replanning.

## Background

`SpotPlacer._consume_retry_if_benched()` records the retry reservation before
`ReplicaInfo` is constructed. `ReplicaInfo.created_at` therefore orders the
queued row after the bench timestamp consumed for its own retry. A later launch
failure calls `set_preemptive()` and records a timestamp after rows already
queued for that location.

The same ordering already protects reactivation: `set_active(...,
selected_at=...)` refuses to clear a bench recorded after the successful
launch was selected.

## Solution

Add a `SpotPlacer.is_launch_admissible(location, selected_at)` query with the
following contract:

1. Unknown locations are not admissible.
2. Effectively active locations are admissible.
3. A stored `PREEMPTED` location is admissible only when its bench timestamp is
   no newer than the queued row's selection timestamp. This is the retry lease
   consumed immediately before that row was created.
4. Missing selection or bench timestamps fail closed.

Use this query in `_refresh_thread_pool()` for queued `PENDING` launches.
Failures completed in the current refresh remain authoritative through the
existing `failed_spot_locations` check and always discard queued siblings.

No database, API, service-spec, or Boltz Platform schema change is required.
The behavior change is internal to SkyServe placement and admission.

## Alternatives considered

Removing the admission-time active-location check would let the intended retry
start, but it would also admit stale siblings after a failure observed on an
earlier refresh. That weakens the fail-fast replanning behavior and is rejected.

Moving retry consumption from selection back to launch failure would avoid the
admission conflict but restore the burst bug where many scale-ups select the
same expired bench before the first probe fails.

Persisting an explicit retry-token field would provide stronger identity than
timestamp ordering, but adds schema and recovery surface without improving the
existing ordering model used by successful-launch reactivation. The timestamp
lease is the smallest compatible correction.

## Rollout and rollback

Ship as a SkyPilot control-plane release. No service YAML update is needed.
After deployment, observe one controlled Kubernetes retry and verify that its
row progresses from `PENDING` to a real `sky.launch` request. Confirm current R2
secret injection before declaring reserved capacity recovered.

Rollback is the prior SkyPilot image. Rolling back restores the retry deadlock
but does not require data migration.

## Test plan

- Unit-test the placer's admissibility contract for active locations, consumed
  TTL retries, missing timestamps, and a newer failure after selection.
- Unit-test manager admission to prove a consumed TTL retry starts exactly once.
- Keep coverage proving a genuinely benched queued wave is discarded and a
  same-refresh failure overrides sibling admission.
- Run the focused placer and replica-manager unit tests.
- Run `bash format.sh --files` for the changed Python files before committing.
