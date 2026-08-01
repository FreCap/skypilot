# Serve Autoscaler Compatibility Policy Extraction

## Context

`sky/serve/autoscalers.py` is 7,740 lines and owns several distinct concerns:

- the stable `Autoscaler` strategy facade and common scaling lifecycle;
- request-rate, concurrency, fallback, and queue-length strategies, including
  their mutable metrics, hysteresis, persistence, and locking;
- GPU-shape resolution and bounded provider-cost lookup caches;
- pure exact-card compatibility allocation and actuation policy.

The compatibility policy is implemented by four module-level functions:
`_allocate_compatibility_target`, `_replica_is_retiring_card_supply`,
`_merge_fresh_target_into_downscale_hold`, and
`_revalidate_actuation_target`. Together they occupy about 330 lines. They are
called by the common autoscaler lifecycle and by both instance-aware QPS and
concurrency strategies, but own no autoscaler instance state, locks, I/O,
persistence, background tasks, or provider clients.

## Responsibility Map

### Strategy facade and lifecycle

- Callers: Serve controller and replica manager, service update and recovery
  paths, autoscaler tests.
- Dependencies: service specs, replica state, reserved capacity, spot placer,
  operator notifications, global user state, and managed-job state.
- State: versions, replica snapshots, request metrics, hysteresis, rollout and
  rebalance state, caches, and locks.
- Failure modes: unsafe scale decisions, rollout stalls, persistence drift,
  lock races, or provider calls on controller-critical paths.
- Performance sensitivity: one decision tick plus hot request-metric ingest.
- Change cadence: controller lifecycle, rollout, persistence, and policy
  orchestration.

### Exact-card compatibility allocation and actuation policy

- Callers: common reserved-fill and cost-rebalance logic, instance-aware QPS,
  and concurrency autoscalers.
- Dependencies: plain lists, dictionaries, numeric values, and the read-only
  `ReplicaInfo.status_property` projection.
- State: none beyond function-local collections.
- Failure modes: wrong card attribution, exceeding replica fences, relaunching
  retiring supply, losing held demand, or allowing unbacked reassignment.
- Performance sensitivity: bounded in-memory allocation on each scaling tick;
  no I/O or copies beyond the existing local collections.
- Change cadence: accelerator compatibility, supply reuse, and exact-card
  actuation invariants.

### Concrete scaling strategies

- Callers: the strategy factory in `Autoscaler.from_spec` and the Serve
  controller.
- Dependencies: strategy-specific request histories, concurrency envelopes,
  queue metrics, GPU catalogs, and dynamic-state schemas.
- State: strategy-specific mutable metrics, targets, histories, and locks.
- Failure modes: demand undercount, oscillation, stale state, or invalid
  version transitions.
- Performance sensitivity: request ingestion and decision generation.
- Change cadence: request-rate, concurrency, queue, and fallback semantics.

The policy responsibility differs materially from the strategy facade and
concrete strategies in callers, dependencies, state ownership, failure modes,
and reasons to change. Its four functions share one invariant: convert demand,
adopted targets, and materialized supply into an exact-card target without
bypassing aggregate or per-card fences.

## Proposed Design

Move the four pure functions unchanged into
`sky/serve/autoscaler_compatibility.py`. Import the exact function objects into
`sky.serve.autoscalers`, retain their historical private names there, and set
their `__module__` attributes back to `sky.serve.autoscalers`.

All existing internal calls continue resolving the names in the historical
facade. This preserves monkeypatch behavior, signatures, function identity,
qualified names, and pickle resolution without wrappers or callbacks. The new
module does not import `autoscalers`; its only runtime dependency is typing and
it uses a type-only import for `ReplicaInfo`, so the boundary is acyclic.

This is a facade-first plain-module extraction. It does not introduce a new
strategy, abstract base class, registry, factory, dependency injection layer,
or state owner.

## Behavior Contract

- Allocation results, dictionary ordering, tie-breaking, float epsilon,
  aggregate floors, per-card floors, and supply-tier order remain identical.
- Retirement classification keeps the exact `is_scale_down` and `preempted`
  semantics.
- Downscale-hold merging and actuation revalidation keep all empty-result
  rejection paths and configured-card ordering.
- `sky.serve.autoscalers` retains the four historical private attributes with
  unchanged signatures, qualified names, module identity, and pickle paths.
- Existing callers continue looking up the facade globals, so tests and users
  that monkeypatch those attributes keep controlling every call site.
- No serialized autoscaler state, configuration, database format, public API,
  CLI output, remote command, lifecycle order, query count, provider call, or
  lock acquisition changes.

## Alternatives

### Leave the functions in `autoscalers.py`

This has zero immediate change risk, but keeps a complete accelerator-policy
leaf embedded between the GPU-shape mixin and strategy classes. The policy is
already shared by two strategies and changes for exact-card invariants rather
than strategy lifecycle, so the ownership boundary is durable enough to earn
one module.

### Extract only `_allocate_compatibility_target`

This is smaller but leaves the same compatibility target split across
allocation, retirement, held-target merge, and actuation revalidation. It
would produce a partial owner and make future invariants cross modules.

### Introduce a strategy object or allocator class

There is no second implementation or construction lifecycle. An object would
add hidden state and dependency plumbing to pure functions without improving
the contract.

### Call the new module directly from strategy methods

This would bypass historical `autoscalers` monkeypatches. Keeping all call
sites on the facade globals preserves the established test and extension seam.

## Implementation Milestones

1. Add characterization tests against the current implementation and run them
   on the exact base before moving code.
2. Move the four functions without behavioral edits and add direct-alias,
   import-order, signature, module, and pickle contracts.
3. Run focused autoscaler tests, the full Serve autoscaler component matrix,
   formatting and static analysis, import checks, and diff checks.
4. Compare exact-base and head import time plus representative allocation and
   revalidation throughput with balanced samples.

## Test Plan

- Characterization: representative floors, fixed work, compatibility groups,
  supply tiers, priorities, retirement, held-target merge, and every
  actuation-revalidation rejection or reassignment branch.
- Facade: exact object aliases, signatures, module and qualified names, pickle
  round trips, both import orders, and facade monkeypatch interception.
- Regression: `test_serve_autoscaler.py`, `test_concurrency_autoscaler.py`,
  `test_reserved_capacity_fill.py`, `test_serve_cost_rebalance.py`,
  `test_serve_controller.py`, and the decision-contract tests.
- Static: `bash format.sh --files` for all changed Python files,
  `git diff --check`, compile/import checks, mypy, and Pylint through the
  repository formatter.
- CI: prove pull-request workflow path filters include the changed files and
  map each changed path to its relevant job before merge.

## Performance and Rollback

The extraction adds no wrapper frame, callback, allocation, I/O, lock, query,
or copy. Python imports one small local module while loading `autoscalers`; a
balanced import benchmark must show no material regression. Direct calls use
the same function objects and bytecode after relocation. Rollback is a single
structural revert because no data or API migration occurs.
