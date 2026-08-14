# SkyServe load-balancer request metadata boundary

## Problem

`sky/serve/load_balancer.py` is 4,805 lines and its primary class owns several
independently changing responsibilities.  Line count is only a prioritization
signal; most of the class remains a stateful request-lifecycle orchestrator and
should stay together.  One complete lower-state leaf is the scheduling-header
policy: it validates request priority, validates and canonicalizes the exact
accelerator set against controller-published metadata, and removes both
scheduling-only headers before forwarding a request upstream.

Keeping that policy inside the load balancer mixes HTTP metadata validation
with queue admission, replica occupancy, HA synchronization, connection-pool
lifecycle, retry orchestration, and response streaming.  The policy has a
separate reason to change and can move without moving any lifecycle state.

## Responsibility map

### Scheduling request metadata

- **Callers:** queue admission in `_proxy_with_retries` parses priority and
  accelerator compatibility; upstream transport in `_proxy_request_to` strips
  internal scheduling headers.
- **Dependencies:** Starlette/FastAPI request and HTTP error contracts,
  scheduling-header constants, and the controller-published immutable
  accelerator catalog and compatibility version.
- **State owned:** no mutable lifecycle state.  Accelerator parsing reads two
  load-balancer fields but does not mutate them.
- **Failure modes:** duplicate headers being coalesced, non-ASCII or oversized
  values escaping validation, unknown accelerator widening, incorrect 400/503
  responses, or internal headers leaking to a replica.
- **Performance sensitivity:** once per request on the proxy hot path; the move
  must add no wrapper frame, request-body read, copy beyond the existing header
  list, lock, query, or network call.
- **Change cadence:** scheduling header schemas and mixed-version controller/LB
  compatibility.  Priority and exact-accelerator support arrived in separate
  changes from proxy, occupancy, and HA lifecycle work.

### Queue admission and accelerator scheduling

- **Callers:** the catch-all proxy route and queue wakeups.
- **Dependencies:** asyncio conditions, priority heaps, capacity hints,
  compatibility demand, occupancy reservations, and load-balancing policy.
- **State owned:** waiting and active counts, per-priority waiter buckets,
  grants, reservations, body budgets, and demand gauges.
- **Failure modes:** starvation, leaked grants, over-admission, disconnect
  leaks, incompatible placement, or incorrect demand.
- **Performance sensitivity:** lock and event-loop ordering plus bounded queue
  scans on every admission or wakeup.
- **Change cadence:** fairness, capacity, queue limits, and accelerator-aware
  scheduling.

### Proxy transport and response lifecycle

- **Callers:** queue-admitted HTTP requests and retry orchestration.
- **Dependencies:** httpx clients, TLS, ASGI streaming responses, timeout and
  replay policy, passive eviction, and client-drain accounting.
- **State owned:** per-client in-flight counts, streaming-release ownership,
  failed-replica exclusions, and retry state.
- **Failure modes:** ambiguous replay, leaked slots or connections, response
  buffering, premature client close, or wrong upstream status.
- **Performance sensitivity:** data-plane hot path with one upstream network
  operation per attempt and streaming without buffering.
- **Change cadence:** transport failures, TLS, retries, draining, and streaming
  semantics.

### Controller synchronization, occupancy, and HA

- **Callers:** owned background loops and readiness/capacity endpoints.
- **Dependencies:** controller APIs, role generations, replica occupancy
  probes, durable request history, and client-pool reconciliation.
- **State owned:** ready set, role and routing generations, probe generations,
  capacity snapshots, history acknowledgements, and background tasks.
- **Failure modes:** stale routing, split-brain readiness, invalid idle proof,
  lost history, or leaked retired clients.
- **Performance sensitivity:** bounded periodic control-plane and per-replica
  network work that must not block request handling.
- **Change cadence:** rollout fencing, controller protocols, observability, and
  async-workload capacity semantics.

## Proposed boundary

Move these five implementations to
`sky/serve/load_balancer_request_metadata.py`:

- `_priority_header_error`
- `_parse_request_priority`
- `_accelerator_header_error`
- `_parse_request_accelerators`
- `_headers_without_request_priority`

Keep `SkyServeLoadBalancer` as the public facade.  Attach the exact moved
function objects in the historical class body using the same staticmethod and
classmethod descriptors as today.  Set their historical module and qualified
names in the implementation module.  This preserves class imports, descriptor
behavior, signatures, runtime annotations, private callable identity, and
existing call sites without wrappers.

A plain module is sufficient.  There is no second implementation, constructed
object, independent lifecycle, or algorithm variant, so an adapter, strategy,
factory, mixin, registry, abstract base class, or dependency-injection layer
would add a false concept.

## Behavior and compatibility contract

- Preserve every accepted value, returned priority/accelerator tuple, error
  status, detail string, and `Retry-After` header.
- Preserve duplicate-header detection from raw ASGI headers and the compatible
  fallback for lightweight test/request objects.
- Preserve omission behavior for legacy versus versioned accelerator catalogs.
- Preserve removal of every priority and accelerator header before proxying,
  including duplicates and case variants.
- Preserve the historical `SkyServeLoadBalancer` descriptors, signatures,
  function module/qualified names, and both import orders.
- Exercise both import orders in fresh Python processes so module caching cannot
  hide a circular import or a stale facade-to-implementation binding.
- Do not change request bodies, queue state, scheduling order, proxy retries,
  network call count, controller synchronization, or public imports.

## Alternatives considered

- **Keep the methods in place:** zero carrying cost, but leaves a complete HTTP
  validation/translation policy embedded between queue accounting methods and
  makes the 4,805-line orchestrator the only place to test or evolve it.
- **Extract only pure priority parsing:** too small and leaves one scheduling
  header policy split across owners.
- **Move queue admission with the metadata policy:** rejected because it moves
  mutable event-loop, condition, reservation, and demand state and would create
  a second lifecycle owner.
- **Move async action/body classification too:** rejected because it consumes
  cached bodies and participates in occupancy and completion accounting rather
  than scheduling-header translation.
- **Wrap new module functions from the facade:** rejected because wrappers add
  a hot-path frame and create two callable identities.

## Milestones and rollback

1. Add and commit characterization tests for all five descriptors, identities,
   signatures, import behavior, validation branches, and header stripping.
2. Move the exact implementations and attach them directly in the facade.
3. Prove normalized AST-body equivalence, run the changed-path test matrix and
   static checks, and measure balanced cold imports plus direct callable
   identity to rule out wrapper overhead.
4. Publish only after rebasing on the latest `origin/improvements`; rollback is
   a normal revert because there is no data, config, API, or deployment
   migration.

## Test and CI plan

| Changed path or seam | Evidence |
| --- | --- |
| request-metadata implementation and facade descriptors | new characterization contract plus existing request-queue header tests |
| queue-admission callers | `tests/unit_tests/test_serve_request_queue.py` |
| proxy transport and retry callers | `tests/unit_tests/test_lb_retry_routing.py`, `tests/unit_tests/test_serve_load_balancer_eviction.py`, and focused LB auth tests |
| import and compatibility boundary | fresh-process metadata-first and facade-first identity/pickle checks, `compileall`, import-linter, mypy, Pylint, Ruff, and BasedPyright |
| design and changed Python files | `bash format.sh --files ...` and `git diff --check` |

The pull-request workflows for `improvements` must be inspected before
publication.  Relevant tests must not be excluded by path filters.  A live
cloud smoke test is not expected for a structurally identical local HTTP
metadata extraction, but test collection and the exact rationale will be
recorded in the PR.

## Validation evidence

The extraction was developed against exact base
`25f62b0911f488073b99af22e9571d908b1bcbcd`.  The facade decreased from 4,805
to 4,647 lines and the implementation module is 187 lines.  The
characterization commit preceded the production move.

- The five normalized function AST fingerprints are unchanged.  The facade
  descriptors, signatures, historical module/qualified names, implementation
  identities, and prior pickling behavior are pinned.  A post-merge audit found
  that the original in-process contract exercised only facade-first import;
  fresh-process coverage now pins both facade-first and metadata-first import.
- The complete 16-file load-balancer matrix collected 447 tests and exited
  successfully in both parallel and serial runs.  It covers request metadata,
  queue admission, capacity/demand/occupancy, proxy retry and eviction, auth,
  HA, TLS, rollout, synchronization, launcher, HTTP runtime, and local async
  routing lifecycle.
- `format.sh --files` passed YAPF, isort, full mypy over 812 source files,
  Pylint 10.00, and dashboard lint/format.  Ruff, isolated CI-equivalent
  BasedPyright 1.39.9 with zero findings, import-linter with all three contracts
  kept, Python 3.11 and 3.14 compileall, and `git diff --check` passed.
- Ten balanced CPU-time import samples measured a 0.7917685-second base median
  and 0.761356-second branch median, a favorable 3.84 percent delta.
  Eight balanced samples of 200,000 iterations, each performing priority
  parsing, accelerator parsing, and header stripping, measured 0.413762 seconds
  on the base and 0.4201715 seconds on the branch, a 1.55 percent delta.  Direct
  function identity adds no wrapper frame, and identical AST bodies add no
  lock, copy, request-body read, query, or network operation.

`.github/workflows/pytest.yml`, `static-analysis.yml`, `format.yml`, `mypy.yml`,
and `pylint.yml` all trigger for pull requests to `improvements` without paths
or paths-ignore filters.  The Unit Tests job collects the entire changed test
surface.  Limited-dependency Serve imports, Resource Lifetime's load-balancer
sync tests, Ruff, BasedPyright, import-linter, worker-floor import, format,
mypy, and Pylint cover the implementation and facade boundary in CI.
