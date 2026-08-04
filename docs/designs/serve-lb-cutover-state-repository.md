# SkyServe load-balancer cutover state repository

## Context

`sky/serve/serve_state.py` is 4,930 lines after the schema foundation was
extracted. It still combines general service, version, replica, cleanup,
placement, paid-capacity, reserved-fill, and load-balancer persistence. Lines
1,425 through 1,952 form a complete PostgreSQL repository for the external
load balancer high-availability saga.

The cutover repository changes with the two-slot load-balancer protocol and
its fencing rules. The rest of `serve_state.py` changes with service lifecycle,
status projection, capacity policy, and other persistence protocols. The
schema foundation now provides a cycle-free dependency for separating these
reasons to change.

## Responsibility map

### Load-balancer cutover state repository

- Callers: the Serve controller's role-reconciliation loop, service startup
  and load-balancer-mode changes, and the Kubernetes load-balancer adapter.
- Dependencies: the shared `services` table and process-wide database manager,
  PostgreSQL compare-and-set updates and row locks, JSON demand snapshots,
  wall-clock timestamps, and `lb_ha` state contracts.
- State owned: HA enablement, active and pending slots, cutover generation and
  phase, drain start, last active demand, and the restart-safe demand handoff.
- Failure modes: stale-controller promotion, selector mutation after ownership
  loss, split active roles, generation reuse, lost demand floors, premature
  drain completion, or malformed persisted state.
- Performance sensitivity: controller-loop single-row reads and bounded
  transactional writes; the extraction must add no forwarding call, query,
  copy, database-manager construction, or longer lock lifetime.
- Change cadence: external-load-balancer HA migration, rollback, promotion,
  draining, and demand-handoff semantics.

### General service and lifecycle repositories

- Callers: Serve APIs, service startup and teardown, controller recovery,
  status and log routing, storage cleanup, and version management.
- Dependencies: service and version tables, lifecycle fences, serialized
  service specifications, cleanup intents, status contracts, and workspace
  identity.
- State owned: service identity and status, controller ownership, lifecycle
  epochs, version specifications, uptime, and cleanup work.
- Failure modes: stale-owner writes, same-name replacement races, incompatible
  serialized specifications, orphaned cleanup, or incorrect status projection.
- Performance sensitivity: hot-path slim reads, batch sizes, lock ordering,
  serialization, and query count.
- Change cadence: service lifecycle, recovery, API projection, version
  compatibility, and cleanup behavior.

### Capacity and replica repositories

- Callers: replica managers, autoscalers, paid-capacity admission,
  reserved-fill arbitration, placement recovery, and history writers.
- Dependencies: replica and capacity tables, transactional leases and claims,
  placement observations, persisted replica records, and policy modules.
- State owned: replica inventory, placement observations, paid claims and
  waiters, reserved-fill rounds and leases, and demand-capacity observations.
- Failure modes: overlaunch, leaked claims, stale leases, incorrect accounting,
  whole-fleet query regressions, or cross-controller races.
- Performance sensitivity: controller-tick batching, query counts, transaction
  duration, memory copies, and provider-query avoidance.
- Change cadence: autoscaling economics, placement admission, replica
  compatibility, and arbitration policy.

The cutover repository has materially different callers, dependencies, state,
failure modes, and reasons to change from both remaining groups.

## Design

Create `sky/serve/lb_cutover_state.py` containing the existing 17-function
repository:

- PostgreSQL enforcement and owner predicates;
- durable cutover reads;
- HA migration and rollback transitions;
- preparation, commit, drain completion, and abort transitions;
- last-demand and demand-handoff persistence; and
- the row-lock guard spanning Kubernetes selector mutations.

The implementation imports `services_table` and `_db_manager` directly from
`serve_state_schema`. `sky/serve/serve_state.py` imports the implementation
module and exposes every historical name as a direct alias. No production
caller changes import path.

The moved public functions retain `sky.serve.serve_state` as their historical
`__module__`. The facade and implementation share the exact table, database
manager, SQLAlchemy modules, `lb_ha` module, and process-wide `time` module.
There are no wrappers, classes, protocols, registries, factories, dependency
injection layers, or new package hierarchy.

## Behavior contract

- Every existing `sky.serve.serve_state` cutover symbol remains available and
  is the exact same function object as its implementation-module counterpart.
- Function signatures, return values, exceptions, docstrings, context-manager
  behavior, and historical `__module__` identities are unchanged.
- The facade and implementation use the exact same `services_table`,
  `_db_manager`, and cached engine. Existing tests that replace
  `_db_manager._engine` continue to drive all cutover operations.
- Patching `serve_state.time.time` continues to control cutover and demand
  timestamps because both modules reference the same imported `time` module.
- PostgreSQL-only enforcement, compare-and-set predicates, query count,
  transaction boundaries, commit and rollback ordering, and row-lock lifetime
  are unchanged.
- JSON formats, database formats, migration behavior, public import paths,
  remote commands, lifecycle ordering, and user-visible behavior are unchanged.
- Importing `lb_cutover_state` before or after `serve_state` creates no second
  schema graph or database manager.

## Alternatives

### Keep the repository in `serve_state.py`

This avoids structural churn, but the block is a complete high-risk protocol
repository with only three production caller areas and a different reason to
change from the surrounding service and capacity persistence. The schema
foundation removed the previous cycle and private-state constraint. Keeping it
now has the higher carrying cost.

### Extract only demand handoff or only the row-lock guard

Those operations share the same owner predicates, generation state, database
manager, table, and cutover transaction protocol. Splitting them would create
cross-module private dependencies and obscure the saga rather than clarify
ownership.

### Introduce an `LbCutoverRepository` class or protocol

There is one process-wide database manager and no second implementation,
construction variation, or instance state. A class or protocol would add
construction and mocking surface without establishing a useful boundary.

### Move production callers to the new module

Changing imports would remove the historical facade and enlarge the rollout
surface. Direct aliases preserve caller and monkeypatch compatibility at no
runtime cost.

## Milestones

1. Add and run characterization tests against the current monolith.
2. Move the 16 functions without semantic edits.
3. Add direct historical facade aliases and pin public function identities.
4. Prove AST equivalence, direct alias identity, shared dependency identity,
   import-order safety, clock patching, and unchanged query and transaction
   behavior.
5. Run focused cutover, controller, Kubernetes LB, service, Serve state, and
   PostgreSQL tests, then formatting, static checks, and the component suite.

## Changed-path-to-test matrix

| Changed path | Responsibility | Verification |
|---|---|---|
| `sky/serve/lb_cutover_state.py` | PostgreSQL cutover and demand-handoff repository | repository contract; PostgreSQL cutover saga; controller LB HA; Kubernetes LB tests |
| `sky/serve/serve_state.py` | historical facade aliases | repository contract; Serve state; service and controller tests |
| `tests/unit_tests/test_serve_lb_cutover_state_contract.py` | direct identity, shared state, import order, clock and structural characterization | focused pytest before and after extraction |
| `docs/designs/serve-lb-cutover-state-repository.md` | canonical behavior and verification contract | documentation review |

## Performance evidence

Measure alternating subprocess cold imports of `sky.serve.serve_state` before
and after extraction. The contract test asserts direct aliases and one shared
database manager and table. AST comparison must prove that all moved function
bodies are unchanged. PostgreSQL tests cover the same compare-and-set and
row-lock operations, so no additional query, transaction, copy, or call frame
is introduced.

## Rollout and rollback

This is an internal structural extraction with no migration or feature flag.
Rollout is the normal package deployment after exact-head CI. Rollback is a
normal revert because database and serialized formats do not change. The PR
must remain open if any relevant CI job is absent, skipped, or non-green.
