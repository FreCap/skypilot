# Serialize Cluster Resource Operations Across Forced Status Unlocks

## Context

Cluster launches and teardowns normally serialize on the per-cluster status
lock. `sky stop` and `sky down` are higher priority operations: they kill
same-cluster requests best effort, force unlock the status lock, and acquire a
new lock instance before teardown.

Force unlocking is necessary for cancellation, but it removes the mutual
exclusion guarantee from a launch process that survives cancellation. The old
process can continue provisioning, running runtime setup, or cleaning up a
failed attempt while `stop`, `down`, or a replacement launch operates on the
same cloud identity. Cloud resource names, SSH config paths, metadata paths,
and generated cluster YAML paths are deterministic from the display name, not
the database generation hash.

This permits two concrete failures:

1. A stale failed launch can terminate resources belonging to a same-name
   replacement launch.
2. A stale successful launch can recreate or modify resources after `down`
   has already torn them down, then race with a replacement launch.

Database generation predicates prevent stale row mutation, but cannot make a
provider API call or shared-file deletion atomic with a row-hash check.

## Behavior Contract

- At most one provisioning, teardown, or status-refresh cleanup may act on a
  cluster's cloud resource identity and shared per-name artifacts at a time.
- `stop` and `down` retain their existing best-effort cancellation and forced
  status-unlock behavior.
- Force unlocking the status lock must not release the resource-operation
  exclusion held by a surviving launch.
- A teardown waits for a surviving provider operation to leave its critical
  section before it queries or mutates provider resources.
- A same-name replacement launch waits until the preceding teardown and all
  surviving earlier launches have left the resource critical section.
- Existing generation predicates remain required for database mutations.
- Operations on different cluster display names remain independent.
- A routine status refresh does not wait behind a provider operation. It
  returns the cached record when it cannot immediately acquire the resource
  lock.
- Dry runs retain their existing behavior and do not require cloud-resource
  serialization because they do not mutate provider or shared cluster state.

## Design

Add a second distributed lock identity for each cluster display name:
`<cluster_name>_resource_operations`.

The existing status lock remains responsible for status serialization,
cancellation, and user-facing contention reporting. The new resource-operation
lock is responsible only for provider resources and shared per-name artifacts.
It is never force unlocked.

1. Non-dry-run provisioning acquires the status lock and then the
   resource-operation lock before checking or creating cluster state. It holds
   both through provider provisioning, runtime setup, final state publication,
   and failure cleanup. Immediately after both locks are acquired, it checks
   the request cancellation context before reading state or entering provider
   work. This prevents a launch canceled by `down` from winning the resource
   lock queue and delaying teardown with new work.
2. `stop` and `down` keep killing requests and force unlocking the status lock.
   After acquiring the fresh status lock, teardown acquires the same
   resource-operation lock before status refresh, provider teardown, or local
   cleanup.
3. Both paths use the same lock order, status first and resource second. A
   force-unlocked launch can continue while holding the resource lock, but it
   does not reacquire the status lock. Teardown can therefore wait without a
   lock cycle.
4. The teardown path never force unlocks the resource-operation lock. If a
   surviving request has not stopped yet, teardown retries through the
   existing contention loop instead of permitting concurrent provider calls.
5. Status refresh already runs under the status lock, but it can also remove
   SSH, YAML, metadata, and database state after observing manually stopped or
   terminated resources. Before querying the provider and performing that
   cleanup, it tries the resource-operation lock without blocking. If the lock
   is unavailable, it returns the cached record. Provisioning and teardown
   tell their nested refresh calls that they already hold the resource lock so
   the non-reentrant distributed lock is not acquired twice.
6. Dry runs continue to use only the status lock because they do not perform
   provider or shared-artifact mutations.

The resource lock intentionally covers the complete launch critical section,
not only provider calls. Failure cleanup removes deterministic YAML, metadata,
network, and SSH artifacts after provider teardown, and final launch
publication can add the same SSH artifact. Releasing the lock between these
steps would retain the original race.

## Alternatives

1. Check the database generation immediately before provider teardown. This
   has a time-of-check to time-of-use race and provider APIs cannot participate
   in the database transaction.
2. Give each generation a different provider resource name. This changes
   externally visible identities and requires coordinated support across every
   provider, legacy Ray YAML, SSH config, metadata, and recovery paths.
3. Rely on request cancellation. Cancellation is best effort and was the
   premise already violated by the observed stale-process races.
4. Stop force unlocking the status lock. That removes teardown priority and
   can leave users unable to clean up a hung launch.
5. Force unlock both locks. This reproduces the same provider race under a
   different lock name.

## Milestones

1. Add the resource-operation lock ID helper.
2. Acquire the resource-operation lock for non-dry-run provisioning while
   preserving the existing retry and blocked-request message.
3. Acquire the same lock in teardown after the fresh status lock and never
   force unlock it.
4. Fence status-refresh provider work and cleanup with a non-blocking resource
   lock acquisition, while avoiding nested reacquisition from provisioning and
   teardown.
5. Add ordering regressions proving replacement provisioning, teardown, and
   status-refresh cleanup cannot enter resource work while an earlier launch
   holds the resource lock.
6. Run focused backend and lock tests, affected static analysis, repository
   formatting, and the full visible pull-request check rollup.

## Rollout

This is a control-flow change with no schema, API, or configuration migration.
Contention is scoped by cluster display name. A teardown may wait longer for a
provider call that does not respond promptly to request cancellation. That is
an intentional safety tradeoff: reporting lock contention is preferable to
simultaneously mutating the same provider resources.

## Test Plan

- Verify provisioning acquires the status lock before the resource-operation
  lock and holds both while entering the provisioning body.
- Verify dry-run provisioning does not acquire the resource-operation lock.
- Verify teardown force unlocks only the status lock.
- Verify teardown cannot enter `teardown_no_lock()` until a surviving launch
  releases the resource-operation lock.
- Verify a replacement launch cannot enter `_check_existing_cluster()` while
  an earlier launch holds the resource-operation lock.
- Verify a canceled launch that acquires both locks exits before checking
  existing state or performing provider work.
- Verify a status refresh returns its cached record without provider or cleanup
  work when the resource-operation lock is held.
- Verify nested provisioning and teardown refreshes do not reacquire the
  resource-operation lock.
- Verify different cluster names use different resource-operation lock IDs.
- Run the affected backend and lock unit tests.
- Run BasedPyright, Ruff, async lifecycle checks, and repository formatting.
