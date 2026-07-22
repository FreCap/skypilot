# Fence Provision Failure State Cleanup by Generation

## Context

Cluster operations normally serialize on the per-cluster lock. `sky down` is
intentionally higher priority: it kills other requests best effort, force
unlocks the cluster lock, and deletes the cluster row. A launch process can
therefore survive `down` while a later launch creates a new cluster generation
with the same display name.

The successful provisioning path now carries the cluster row's `cluster_hash`
as a generation token. The outer `ResourcesUnavailableError` handler does not.
It records retry or failure events by name and, after a terminal failure,
deletes the cluster row by name. This permits the following ordering:

1. an old launch writes generation A in `INIT`;
2. `down` deletes A and force unlocks the name;
3. a later launch writes same-name generation B;
4. the surviving old launch exhausts provisioning and its failure handler
   deletes B.

The state layer can reproduce this without cloud access. A stale name-based
failure cleanup deletes the replacement row even though its hash differs.

## Behavior Contract

- A provisioning failure may record events or remove state only for the
  cluster generation established by that provisioning run.
- If that generation was deleted or replaced, stale retry and terminal-failure
  state cleanup must be a no-op for the replacement generation.
- If provisioning fails before it establishes any cluster generation, the
  failure handler owns no cluster row and must not mutate a same-name row.
- Matching-generation retry events, failure events, history backfill, usage
  interval closure, stop transitions, and termination retain their existing
  behavior.
- Existing unfenced callers of `remove_cluster()` retain name-based behavior.
- This design covers outer failure-state cleanup only. Per-attempt cloud
  teardown and name-based SSH, metadata, and YAML cleanup remain a separate
  audit because they have resource-identity and provider-side semantics.

## Design

Treat `RetryingVmProvisioner._active_cluster_hash` as the identity of the state
owned by the current provisioning run.

1. Expose that value through a read-only `active_cluster_hash` property. It is
   initialized from an existing cluster generation and set from the first
   successful `INIT` write for a new cluster.
2. When `provision_with_retries()` raises `ResourcesUnavailableError`, the
   outer handler captures the provisioner's active hash. Retry and terminal
   failure events pass it to the existing event fence. If no hash was ever
   established, the handler skips cluster-state events and removal.
3. Extend `remove_cluster()` with an optional `existing_cluster_hash`. When it
   is supplied, the snapshot query, stop update, and terminate delete all
   require both name and hash. A missing or replaced generation is a no-op.
   History and usage updates are derived only from the matching snapshot row.
4. The outer terminal-failure handler passes the active hash to
   `remove_cluster()`. A stale handler can close or delete its own generation,
   but it cannot act on a replacement.

The final update or delete remains conditionally filtered even after the
snapshot read. This preserves the fence if another transaction replaces the
row between the read and mutation under PostgreSQL's normal transaction
isolation. SQLite follows the same predicate without requiring a separate
compatibility path.

## Alternatives

1. Check the row hash before calling the existing name-based removal. That
   introduces a time-of-check to time-of-use race.
2. Rely on request cancellation or the cluster lock. Request cancellation is
   best effort, and `down` deliberately force unlocks the lock.
3. Store the generation on `ResourcesUnavailableError`. This would broaden an
   exception used across many provisioning layers when the provisioner object
   already owns the lifecycle token.
4. Fence every teardown side effect in this change. Cloud resources, SSH
   config, local metadata, and database state use different identities and
   failure semantics. Combining them would make this state corruption fix
   harder to verify and risk suppressing required cloud cleanup.

## Milestones

1. Add state-layer regressions for stale terminate and stop operations against
   a replacement generation.
2. Add backend regressions proving retry and terminal-failure cleanup use the
   provisioner's active hash and skip state cleanup before a hash exists.
3. Add the read-only provisioner property and generation-aware state cleanup.
4. Run focused state and backend tests, static analysis, formatting, and the
   full visible pull-request check rollup.

## Rollout

This is a backward-compatible control-flow and query correction. It adds no
schema, migration, or configuration. Callers that omit the optional hash keep
their current behavior.

## Test Plan

- Verify a stale terminate cannot delete a same-name replacement generation.
- Verify a stale stop cannot mark a same-name replacement `STOPPED` or clear
  its cached IPs.
- Verify a matching hash preserves current stop and terminate behavior,
  including history and usage bookkeeping.
- Verify retry and terminal-failure events carry the active generation hash.
- Verify a failure before the first `INIT` write performs no name-based state
  cleanup.
- Run the affected global-state and backend unit tests.
- Run BasedPyright, Ruff, async lifecycle checks, and repository formatting.
