# Fence Provisioning State Against Concurrent Down

## Context

Cluster operations normally serialize on the per-cluster lock. `sky down` is
intentionally higher priority: it kills other cluster requests, force-unlocks
the lock, tears down resources, and deletes the cluster row. Request killing is
best effort, so a launch process can continue after `down` has taken over.

Provisioning currently writes cluster state by name alone. This creates three
failure modes after `down` deletes the row or a later launch reuses the name:

- post-provision setup dereferences a missing handle and raises an incidental
  `AttributeError`;
- an intermediate handle update can overwrite the replacement launch's row;
- the final unconditional upsert can recreate a row that `down` deleted and
  publish the stale launch as `UP`.

The cluster table already assigns a UUID `cluster_hash` to each cluster
generation, and `add_or_update_cluster()` already supports conditional updates
by that hash. Provisioning does not carry that identity through its lifecycle.

## Behavior Contract

- Once `down` deletes a cluster generation, writes from that generation must
  not recreate the cluster row.
- If the same name is reused, the successful-completion path of a stale launch
  must not modify the replacement generation's handle, owner, status, history,
  or lifecycle events.
- A missing or replaced row during post-provision setup must raise an explicit
  cluster-lifecycle error instead of an optional-value `AttributeError`.
- Normal new-cluster, existing-cluster, retry, failover, and ready transitions
  keep their existing state and history behavior.
- This fence covers local cluster-state persistence. Cloud cleanup remains the
  responsibility of the existing request-cancellation and teardown paths.
  Failed-attempt teardown plus name-based SSH and metadata cleanup are
  unchanged and will be audited separately because they have cloud-side
  semantics beyond persistence fencing.

## Design

Treat `cluster_hash` as the optimistic concurrency token for one provisioning
run.

1. `add_or_update_cluster()` returns the hash of the row it inserted or
   updated. When given `existing_cluster_hash`, use that identity for both the
   conditional cluster update and its history update.
2. `_check_existing_cluster()` captures the current row's hash in
   `ToProvisionConfig`. `RetryingVmProvisioner` retains it across zone and
   cloud retries. A new launch captures the generated hash from its first INIT
   write. Every later INIT write uses the conditional-update path.
3. The successful provision result carries the hash into runtime setup and
   final post-processing.
4. Handle, owner, event, and final-ready writes accept the expected hash. A
   missing or different generation either raises the existing `ValueError`
   persistence contract for a required state mutation or skips a best-effort
   event. No fenced write falls back to an insert.
5. Post-provision setup explicitly checks its optional handle before mutation.
   It reports `ClusterDoesNotExist`, rather than an incidental `AttributeError`.
   Enable BasedPyright's `reportOptionalMemberAccess` diagnostic for that module
   so future unchecked optional dereferences fail static analysis.

The first INIT insert for a genuinely new cluster remains an upsert by name.
Before that row exists there is no handle on which `down` can operate, and the
normal per-cluster lock excludes another launch. Once the row exists, the
provisioner immediately retains its generated hash and all subsequent writes
are fenced.

## Alternatives

1. Check only whether the row exists before each write. This has a time-of-check
   to time-of-use race and cannot distinguish a replacement generation.
2. Add only a `None` guard in post-provision setup. This removes one incidental
   exception but leaves both stale overwrite and row resurrection bugs.
3. Rely exclusively on killing the launch request. Request killing is already
   best effort by design, so persistence still needs a defensive boundary.
4. Add a tombstone table or distributed cancellation lease. That could also
   fence cloud-side work, but it adds schema and lifecycle complexity beyond
   the database corruption demonstrated here.

## Milestones

1. Add regression tests that reproduce deletion and same-name replacement
   between INIT and later provisioning writes.
2. Return and propagate the generation hash across retries and post-processing.
3. Fence intermediate and final persistence writes by generation.
4. Add the explicit optional-handle guard and scoped BasedPyright diagnostic.
5. Run focused database and backend tests, BasedPyright, Ruff, async-lifecycle,
   and repository formatting.

## Rollout

This is a backward-compatible local database and control-flow correction. It
adds no schema, migration, or configuration. Existing callers that ignore the
new return value or omit the optional generation fence retain current behavior.

## Test Plan

- Verify a deleted generation cannot be reinserted by its final ready write.
- Verify a replacement generation is unchanged by stale handle, owner, event,
  and ready writes.
- Verify new and existing provisioning retain one hash across retries.
- Verify a missing post-provision handle raises `ClusterDoesNotExist`.
- Run the affected global-state and backend unit tests.
- Run BasedPyright and Ruff on changed Python files.
- Verify async-lifecycle output exactly matches its reviewed baseline.
- Run repository formatting and its mypy and pylint checks.
