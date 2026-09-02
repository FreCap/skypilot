# Managed-job terminal cluster reconciliation

Status: Deployed

Last updated: 2026-09-02

## Context

Managed-job provider cleanup is owned by the disposable controller-manager
attempt. Current controllers leave a terminal job out of scheduler `DONE` when
cleanup fails, and a replacement manager claims that terminal family as
cleanup-only work. This protects jobs finalized by current code.

Older controllers could write `DONE` after cleanup failed. A `DONE` row is not
eligible for the cleanup-only scheduler, so a surviving managed-job cluster
row can retain live provider resources indefinitely. Ordinary cluster refresh
eventually removes the database row after provider absence is observed, but it
does not reclaim resources while they are still present and closes accounting
at observation time rather than the provider termination time.

## Goals

- Re-admit terminal `DONE` jobs with an attributable live managed-job cluster
  row to the canonical cleanup-only scheduler path.
- Keep provider effects in the controller manager; recovery only repairs
  durable scheduler state.
- Make the repair idempotent and safe under API-server leader replacement.
- Preserve terminal workload outcomes and prevent workload execution during
  repair.
- Document the operational correction of the incident interval without
  deleting genuine historical cost.

## Non-goals

- Provider billing APIs are not called from request or scheduler paths.
- Historical estimates are not replaced with cloud invoices.
- Pool jobs are not candidates because their worker cluster is shared.
- This change does not retry arbitrary user-cluster teardown.

## Public contract and invariants

There is no new CLI or API contract.

The lifecycle invariants are:

1. A current dedicated managed job reaches scheduler `DONE` only after its
   canonical provider/storage cleanup succeeds.
2. A terminal task family in `WAITING` is always classified as cleanup-only;
   it cannot enter the workload execution path.
3. During leader recovery, a terminal `DONE` job is reset to `WAITING` only
   when a current `clusters` row is attributable to that exact job and an
   expected generated task-cluster name matches.
4. Recovery performs no provider effect. The cleanup-only controller manager
   owns teardown, retries with backoff, and returns the job to `DONE` only
   after cleanup succeeds.
5. A concurrent disappearance of the cluster row is harmless: the canonical
   teardown observes absence idempotently and finalizes the cleanup-only job.
6. Terminal task statuses are never rewritten by reconciliation.

## Architecture

The leader-elected managed-job recovery phase already runs after the previous
controller family is drained and before new fixed slots are admitted. It
performs one additional durable repair:

1. Read the bounded current managed-job cluster candidate inventory from the
   global-state gateway.
2. Derive candidate job IDs from `workload_id`; support legacy rows only when
   the generated task-cluster name proves the association.
3. Read the slim job/task snapshots for those IDs and retain only dedicated,
   terminal, scheduler-`DONE` jobs with a matching launched task identity.
4. In one managed-job database transaction, compare-and-set those exact jobs
   from `DONE` to `WAITING`, clear stale controller/slot ownership, and confirm
   every task is still terminal.
5. Let the existing scheduler claim each row as cleanup-only work. Its existing
   retry loop remains the single provider-cleanup implementation.

The candidate inventory is current state, not history, so the work is bounded
by live managed-job cluster rows. The repair runs once per elected recovery,
which is sufficient for pre-fix rows; current finalizers cannot create this
state under invariant 1.

## Incident data correction

For managed job 6036, AWS CloudTrail proves that all 32 instances were
terminated at `2026-09-01T20:52:24Z` (`1788295944`). The state database later
closed the interval at its observation time, `2026-09-02T16:41:52Z`
(`1788367312`). The operational repair:

- asserted the exact cluster hash and current interval before mutation;
- retained the launch boundary and replaced only the terminal boundary with
  the CloudTrail timestamp;
- advanced `usage_updated_at`, allowing the existing estimated-spend rollup to
  replace affected daily rows;
- verified that no live cluster row or AWS instance remains and that the API
  reads the corrected rollup.

The genuine interval before AWS termination remains intact.

## Deployment and rollback

The state repair is additive and idempotent. Deploying it causes any legacy
`DONE` orphan to become cleanup-only work during elected recovery. Rolling
back after a row is reset is safe: current controllers already understand
terminal `WAITING` cleanup adoption. Rolling back before adoption leaves the
same visible `WAITING` row for a later current controller.

No compatibility path or feature flag is introduced, so no stacked removal
change is required.

Production deployed release `1.1.1638` from merge commit
`1cad3f6d0442926f30ee26b2b8a294cceeceb245` at Helm revision 753. The API,
controller, and executor deployments are pinned to image digest
`sha256:5de0e97ab88b66bf0496a84059cfb96b3d97cde7174948a7cdfeddddd15b41a2`;
the immutable chart digest is
`sha256:8f6b5ee2ffe5951c37631e4af1a16b5cf84f72fd432d376acc3a13f504ff3c97`.
Because the fixed three-node control-plane fleet could not fit the HA API
surge replica, the rollout temporarily reduced executors to their supported HA
minimum of two through Helm, completed the API rollout, and then restored all
seven executors through Helm. Both stages used atomic rollback and preserved
at least two API replicas and two executors.

## Verification plan

- Unit-test candidate filtering: attributed current rows, legacy exact-name
  rows, unrelated services/pools, malformed IDs, nonterminal jobs, pool jobs,
  and tasks without a launch attempt.
- Database-test the compare-and-set: only `DONE` plus all-terminal candidates
  become `WAITING`; controller identity is cleared; concurrent/nonterminal
  changes are preserved.
- Test recovery ordering: orphan requeue occurs after stale-owner recovery and
  before the recovery gate opens.
- Re-run managed-job ownership, cleanup-only scheduler, and refresh ownership
  suites.
- In production, verify the exact incident interval, rollup rows, API total,
  live cluster inventory, and AWS instance inventory after correction.

## Open gates

- [x] Implementation and focused tests pass.
- [x] Formatting and Python static checks pass for changed files. The
  repository formatter's unrelated dashboard phase still reports the
  pre-existing `capacityPlanExpiryTick` hook warning.
- [x] Production incident interval is corrected and rollup read-back matches.
- [x] No live AWS resources remain for job 6036.
- [x] Reconciliation code is reviewed and deployed to the API server. Leader
  recovery completed with zero legacy orphan candidates; the API reported
  commit `1cad3f6d0442926f30ee26b2b8a294cceeceb245`, all role deployments were
  Ready, and no live clusters or in-progress managed jobs remained.
