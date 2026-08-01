# SkyServe Controller History Persistence

## Context

`sky/serve/controller.py` is 4,313 lines and changes for several independent
reasons. Its controller runtime owns load-balancer synchronization and HA
fencing, autoscaling and replica reconciliation, service updates, HTTP route
registration, and best-effort history persistence. This design considers one
bounded extraction only: the history persistence methods at lines 2,171-2,442.

Line count is only a prioritization signal. The extraction is justified by the
history family having a separate dependency (`serve_history`), failure policy
(observability must not fail routing), execution model (blocking persistence is
offloaded to the executor), and change cadence from controller reconciliation.

## Responsibility map

### Controller runtime and API facade

- Callers: the controller process, authenticated FastAPI routes, external load
  balancers, the autoscaler loop, and service-update handlers.
- Dependencies: FastAPI, replica manager, autoscaler, load-balancer HA state,
  service persistence, Kubernetes load-balancer discovery, and routing specs.
- State owned: service incarnation and owner fences, manager and routing locks,
  LB session ledger, autoscaler and replica-manager instances, pending updates,
  HA cutover state, and background-task lifecycle.
- Failure modes: split-brain mutation, stale route disclosure, unsafe drain,
  event-loop stalls, lifecycle-order drift, and failed controller supervision.
- Performance sensitivity: every LB sync and autoscaler tick; locks, database
  reads, provider calls, and executor transitions are load-bearing.
- Change cadence: HA rollout, autoscaling, replica lifecycle, update
  reconciliation, routing, and controller supervision.

### Load-balancer report preparation and application

- Callers: normal LB sync, HA role transitions, promotion, and drain proof.
- Dependencies: replica snapshots, compatibility demand, capacity hints,
  routing generations, session authority, and the manager and routing locks.
- State owned: prepared report data, demand handoff, occupancy, drain view, and
  disclosure fences.
- Failure modes: mixed-epoch disclosure, incorrect demand, lost occupancy,
  stale drain proof, or mutation after ownership loss.
- Performance sensitivity: latency-sensitive LB sync with strict three-phase
  ordering and bounded blocking work.
- Change cadence: capacity semantics, accelerator compatibility, HA, and
  load-balancer protocol changes.

### Best-effort history persistence and projection

- Callers: normal LB sync, draining-LB history-only sync, and focused controller
  tests through the historical `SkyServeController` methods.
- Dependencies: `serve_history`, the shared `time`, `asyncio`, and
  `common_utils` modules, already-computed replica/capacity dictionaries, and
  read-only autoscaler projections.
- State owned: no locks or durable state; it reads the service identity,
  history session id, applied version, and autoscaler snapshot from the
  controller, then delegates durable writes to `serve_history`.
- Failure modes: malformed reporter sessions, duplicate or cross-incarnation
  history, database failure leaking into routing, invented exact-card overlays,
  or blocking the event loop.
- Performance sensitivity: one executor submission for each present history
  family and autoscaler snapshot; no extra database reads, copies, or provider
  calls may be introduced.
- Change cadence: history schema, validation, aggregation, and observability
  failure policy.

### Autoscaling, replica lifecycle, and service updates

- Callers: controller control loops and authenticated service-management APIs.
- Dependencies: `Autoscaler`, `ReplicaManager`, placement, reserved capacity,
  service state, and update persistence.
- State owned: target and fill state, replica lifecycle, placement and cost
  state, pending updates, retry timers, and applied configuration.
- Failure modes: unsafe scaling, lost updates, leaked replicas, rollout stalls,
  or provider calls on controller-critical paths.
- Performance sensitivity: periodic fleet scans, lock hold time, and bounded
  provider and database operations.
- Change cadence: scaling policies, placement, cost rebalancing, lifecycle, and
  configuration updates.

## Proposed seam

Move the nine private history persistence and projection methods to
`sky/serve/controller_history.py` as plain functions. Bind those functions
directly on `SkyServeController` under the existing method names. The controller
remains the stable facade and continues to own authority checks, LB sync
ordering, already-computed snapshots, locks, and runtime lifecycle.

The extracted functions intentionally accept the controller instance as their
first argument. This preserves direct method binding and instance/class patch
sites without a wrapper call, state transfer object, callback registry, new
class, or circular import. The helper module must not import the controller.

## Behavior contract

- Existing `SkyServeController._persist_*`, `_record_*`, and
  `_get_accelerator_history_breakdown` names remain available and patchable,
  with their historical function module and qualified-name identities.
- The normal and history-only LB routes return the same acknowledgement fields
  and preserve their authority checks and ordering.
- Missing history remains an accepted no-op. Invalid bounded input is
  acknowledged and dropped. Infrastructure failures return `False` for LB
  history and remain non-fatal for autoscaler history.
- Reporter session validation, service-incarnation fencing, history payloads,
  timestamps, exact-card completeness checks, and durable writer calls are
  unchanged.
- Each present history family still performs exactly one executor submission
  and one durable writer call. Autoscaler history still samples `time.time()`
  once before its executor submission.
- Imports stay at module scope. No public import path, wire shape, serialized
  identity, database format, CLI output, remote command, or lifecycle ordering
  changes.

## Alternatives

- Do nothing: lower immediate carrying cost, but history schema and failure
  policy continue to be interleaved with HA and reconciliation code in the
  stateful controller. This is the baseline the extraction must beat.
- Move only reporter-session validation: too small and leaves history ownership
  split across modules.
- Add a `HistoryWriter` class or protocol: no second implementation exists and
  construction or dependency injection would add state and indirection.
- Pass an immutable snapshot DTO: unnecessary copying and a new compatibility
  surface for data already owned by the controller.
- Keep forwarding wrappers in `controller.py`: preserves implementation module
  names but adds permanent call depth and leaves duplicate ownership.
- Move LB report preparation too: rejected because it shares controller locks,
  authority, autoscaler mutation, routing generations, and HA ordering.

## Milestones

1. Add characterization tests on the unsplit controller for direct method
   binding and patchability, executor and writer call counts, timestamps,
   validation, acknowledgements, and exact-card projection.
2. Run those tests before moving production behavior.
3. Move the method bodies without behavioral edits and bind direct aliases on
   `SkyServeController`.
4. Prove source-AST equivalence for the moved bodies after normalizing the
   first-argument annotation, then run focused and Serve component tests.
5. Measure import time and representative history projection/writer dispatch.

## Test and CI plan

Changed-path-to-test matrix:

| Changed path | Responsibility | Local evidence | CI job |
| --- | --- | --- | --- |
| `sky/serve/controller.py` | facade bindings and LB callers | controller, LB sync, and HA tests | Python Tests / Unit Tests |
| `sky/serve/controller_history.py` | history validation, persistence, failure policy, projection | new contract tests plus controller history tests | Python Tests / Unit Tests |
| `tests/unit_tests/test_serve_controller_history_contract.py` | direct-binding and call-count characterization | execute directly before and after extraction | Python Tests / Unit Tests |
| this design | canonical contract | doc and diff checks | Format and static analysis |

Run the new characterization suite before and after the move, focused history
and LB sync tests, the relevant Serve controller and HA suites, `format.sh
--files` for changed Python files, compile/import checks, mypy and pylint through
the formatter, and `git diff --check`. Inspect the current workflow path filters
and require the complete relevant check rollup on the exact pushed SHA.

## Performance and rollout

The extraction adds no wrapper, object, query, provider call, snapshot copy, or
lock. Compare cold imports and representative calls before and after. Treat a
repeatable material regression as a blocker.

This is an internal structural change with no migration or feature flag. Roll
back by reverting the extraction commit. Merge only after exact-head CI and
review are fully clear.
