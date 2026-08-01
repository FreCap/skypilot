# SkyServe Load Balancer Retry Policy

## Context

`sky/serve/load_balancer.py` is 4,651 lines and changes for several independent
reasons. Its primary class owns request queueing and admission, replica
occupancy, controller synchronization and HA, connection-pool lifecycle,
capacity and history projection, upstream transport, retry orchestration, and
the standalone runtime. Line count is only a prioritization signal. Most of
these responsibilities share locks and event-loop ordering and should remain
together.

This design considers one bounded extraction: the stateless classification of
proxy outcomes for passive eviction, safe replay, and pre-dispatch accounting.
That policy is currently implemented by two private exception types and three
pure functions at the top of the load-balancer module.

## Responsibility map

### Proxy failure and replay policy

- Callers: passive replica eviction, async occupancy attempt accounting, the
  upstream transport boundary, and retry-exhaustion handling.
- Dependencies: `httpx`'s transport exception taxonomy and HTTP method
  idempotency semantics.
- State owned: none. The exception values carry one status code or a
  pre-dispatch marker; the classifiers read only their arguments.
- Failure modes: evicting a saturated replica, replaying a non-idempotent
  request with an ambiguous outcome, retaining a reservation after a proven
  pre-dispatch failure, or returning the wrong terminal status.
- Performance sensitivity: pure `isinstance` and set-membership checks on each
  failed proxy attempt; the extraction must add no wrapper, allocation, I/O,
  lock, query, or network call.
- Change cadence: transport taxonomy, retriable response semantics, and HTTP
  replay safety. These changed separately from queue, HA, TLS, and occupancy
  protocols in July 2026.

### Request queueing and occupancy admission

- Callers: the catch-all proxy route, queue wakeups, occupancy probes, and
  completion callbacks.
- Dependencies: asyncio conditions and futures, priority and accelerator
  scheduling, request-body budgets, capacity hints, and controller-published
  replica metadata.
- State owned: waiter buckets, queue depth, body reservations, active request
  counts, optimistic occupancy reservations, probe generations, and demand
  gauges under the client-pool lock.
- Failure modes: starvation, leaked slots or bodies, over-admission, stale
  occupancy, disconnect leaks, or incompatible placement.
- Performance sensitivity: lock duration, bounded queue scans, and event-loop
  wakeup ordering on every admission and release.
- Change cadence: fairness, capacity, accelerator compatibility, async request
  lifecycle, and demand handoff.

### Proxy transport and response lifecycle

- Callers: queue-admitted inference requests and retry orchestration.
- Dependencies: `httpx.AsyncClient`, replica TLS, ASGI streaming responses,
  request metadata, timeouts, and background release callbacks.
- State owned: per-client in-flight counts, draining-client ownership,
  streaming response release, failed URL exclusions, and request-local body
  and scheduling metadata.
- Failure modes: response buffering, premature connection closure, leaked
  slots, duplicate upstream dispatch, or incorrect headers and status codes.
- Performance sensitivity: the data-plane hot path and exactly one upstream
  network attempt per selected replica.
- Change cadence: TLS, streaming, transport, replica retirement, and request
  metadata.

### Controller synchronization, HA, and runtime

- Callers: owned background loops, readiness and capacity endpoints, the
  external load-balancer process, and the controller.
- Dependencies: controller sync APIs, role and routing generations, replica
  probes, history persistence, FastAPI, Uvicorn, and graceful drain.
- State owned: ready replicas, role epochs, routing generations, history
  acknowledgements, background tasks, clients, and runtime shutdown order.
- Failure modes: stale routing, split-brain readiness, lost history, invalid
  idle proof, leaked clients, or reordered shutdown.
- Performance sensitivity: bounded periodic control-plane work that cannot
  block request handling.
- Change cadence: HA rollout, controller protocols, observability, replica
  lifecycle, and process supervision.

## Proposed seam

Move the complete retry-policy family to
`sky/serve/load_balancer_retry.py`:

- `_RetriableStatusError`
- `_PreDispatchError`
- `_is_dead_connection_error`
- `_is_definitely_not_dispatched`
- `_can_retry_proxy_failure`

Import those exact objects into `sky.serve.load_balancer`, which remains the
stable facade for all existing callers and tests. Restore the historical
module names on the two exception classes and three functions so imports,
pickling of callable objects, tracebacks, and inspection remain compatible.
There are no forwarding wrappers and the implementation module does not import
the load balancer, so the boundary cannot create a circular dependency.

A plain module is the right abstraction. There is no second algorithm,
constructed object, lifecycle, or external interface translation, so a
strategy, adapter, class, protocol, registry, factory, or dependency-injection
layer would add a false concept.

## Behavior and compatibility contract

- Preserve the exact idempotent-method set and case-insensitive method check.
- Preserve explicit retry for configured retriable replica statuses.
- Preserve the distinction between definite pre-dispatch failures and
  ambiguous read, write, protocol, and generic timeout failures.
- Preserve passive eviction only for network and protocol errors, never for
  timeout saturation.
- Preserve exception inheritance, messages, `status_code`, signatures,
  historical import paths, module and qualified names, and callable identity.
- Preserve every call site, lock boundary, request attempt count, response
  status, queue notification, client operation, and network operation.
- Keep all imports at module scope and introduce no new data, config, wire,
  database, CLI, or lifecycle surface.

## Alternatives

- Do nothing: zero immediate change, but a complete transport policy remains
  mixed into a 4,422-line stateful orchestrator even though four distinct
  call-site families depend on it.
- Move only the replay classifier: too small and splits ownership of the
  exception taxonomy from the decisions that consume it.
- Move retry orchestration or upstream transport: rejected because those
  methods share request-local bodies, clients, policy hooks, occupancy
  reservations, queue gauges, streaming release, and event-loop ordering.
- Add a strategy object: there is one fixed safety policy and no independent
  construction or replacement need.
- Keep forwarding wrappers in the facade: creates duplicate callable
  identities and a permanent hot-path frame without compatibility benefit.

## Milestones

1. Add characterization tests on the unsplit module for signatures, ASTs,
   historical identities, exception payloads, eviction classification, and the
   complete replay-safety matrix.
2. Run those tests before moving production behavior.
3. Move the exact implementations and re-export the exact objects through the
   historical facade.
4. Prove normalized AST equivalence and run focused load-balancer suites.
5. Measure balanced cold imports and representative classifier calls.

## Test and CI plan

Changed-path-to-test matrix:

| Changed path | Responsibility | Local evidence | CI job |
| --- | --- | --- | --- |
| `sky/serve/load_balancer.py` | stable facade and four caller families | retry, eviction, request queue, auth, occupancy, sync, and rollout tests | Python Tests / Unit Tests |
| `sky/serve/load_balancer_retry.py` | exception taxonomy, eviction, pre-dispatch, and replay policy | new contract plus retry-routing and eviction tests | Python Tests / Unit Tests |
| `tests/unit_tests/test_serve_load_balancer_retry_contract.py` | source, identity, pickle, and decision characterization | execute directly before and after extraction | Python Tests / Unit Tests |
| this design | canonical behavior and rollout contract | format and diff checks | Format and static analysis |

Run the characterization test before and after the move, the complete focused
retry and eviction suites, request-queue and load-balancer component tests,
`format.sh --files` for every changed Python file, compile and import checks,
mypy and Pylint through the formatter, and `git diff --check`. Inspect current
workflow filters and require every relevant check on the exact pushed SHA.

## Performance and rollout

The extraction uses direct aliases. It adds no wrapper, object, branch, lock,
copy, query, I/O, or network operation. Compare balanced cold imports and a
representative mix of classifier calls before and after. A repeatable material
regression blocks publication.

This is an internal structural change with no migration or feature flag.
Rollback is a normal revert. Merge only after exact-head CI and review are
fully clear.

## Validation evidence

The extraction was developed against exact `origin/improvements` base
`567471713b165adfadadbf4664401e8a2c80d2f5`. The facade decreased from 4,651
to 4,598 lines; the extracted implementation is 66 lines. The deterministic
diff report classifies the four-file change as `[M]` with 361 significant lines
before this evidence section.

- The characterization contract passed before production code moved. After
  extraction it proves normalized AST identity for all five symbols, direct
  facade aliases, historical module and qualified names, callable pickling,
  both import orders, exception payloads, eviction classification, and the
  replay-safety matrix.
- The final 17-file load-balancer matrix collected and passed 470 tests. It
  covers retry routing, passive eviction, queue admission, auth, rollout,
  controller sync, HA observability, launcher and HTTP runtime, request
  metadata, capacity, demand, load accounting, occupancy, local async routing,
  and replica TLS.
- `format.sh --files` passed YAPF, isort, mypy over 825 source files, Pylint at
  10.00, dashboard lint, and dashboard formatting. Changed-file Ruff, all
  three import-linter contracts, isolated CI-equivalent BasedPyright 1.39.9,
  compileall, and `git diff --check` passed.
- Ten alternating import samples, with both `cwd` and `PYTHONPATH` pinned and
  `load_balancer.__file__` asserted, measured a 0.649803-second base median and
  0.660583-second branch median, a 1.66 percent delta within run-to-run noise.
  Ten alternating samples of 200,000 mixed classifier iterations measured a
  0.086036-second base median and 0.084342-second branch median, a favorable
  1.97 percent delta. Direct aliases add no runtime wrapper, allocation, lock,
  copy, I/O, query, or network operation.

The pull-request workflows for `improvements` have no path filters. Python
Tests / Unit Tests collects the complete changed test surface, while Resource
Lifetime reruns load-balancer synchronization. Format, mypy, Pylint, Ruff,
BasedPyright, import-linter, async lifecycle, and worker-floor import also cover
the changed production paths. A live cloud smoke test is not warranted for an
AST-identical stateless policy extraction with no process, wire, configuration,
or network change; the focused local HTTP and async-router tests exercise every
moved call-site family.
