# Batch SkyServe HA recovery fallback reads

_Created: 2026-07-24_

## Problem

The consolidation HA sweep already snapshots version-backed service liveness
once. Orphan service rows and latest-version placeholders need two defensive
fallbacks: the latest committed version and the raw service mode/incarnation.
Those fallbacks currently open up to two database sessions per exceptional
service. A restart with many interrupted registrations therefore turns one
recovery sweep into O(services) database round trips before cleanup can make
progress.

## Behavior contract

One sweep reads each fallback class in bounded batches. Existing committed
versions remain recoverable, while rows with no committed YAML are retired only
when their current mode and incarnation still match. Missing, recreated, or
wrong-mode rows are skipped. The existing transactional retirement helper
remains the final authority, so a concurrent version commit or lifecycle change
cannot be overwritten by a stale sweep.

Normal version-backed services add no query. Exceptional work changes from up
to two statements per service to `ceil(candidates / 250)` committed-version
statements plus `ceil(unbootable / 250)` raw-identity statements. Controller
liveness, in-flight start detection, leader fencing, retry behavior, recovery
scripts, cleanup, and load-balancer reconciliation are unchanged.

## Solution

Add batch equivalents of the existing single-row committed-version and raw
identity reads. They deduplicate and sort service names, use the established
250-name SQL chunk size, omit missing rows from their result maps, and return
without opening the database for empty input.

The HA sweep builds the exceptional candidate list from its existing liveness
snapshot. It loads committed versions once, then loads raw identities only for
candidates that still lack committed YAML. Per-service handling consumes those
maps and retains the transactional retirement recheck.

## Alternatives

An outer join in `get_service_liveness_snapshots()` could include versionless
rows, but that helper has another consumer whose version-backed visibility
contract would change. Keeping the single-row fallbacks preserves behavior but
retains the query storm. The two narrow batch helpers are the smallest safe
seam.

## Rollout and tests

No schema, migration, or compatibility gate is required. Unit tests cover
empty, duplicate, missing, placeholder, committed, wrong-mode, incarnation,
and 250-name chunk boundaries, plus the transactional retirement fence. The
full Serve unit inventory and jobs/Serve integration suite cover adjacent
liveness and recovery behavior. Merge only after the exact rebased head passes
all visible relevant CI checks; revert the commit to restore single-row reads.
