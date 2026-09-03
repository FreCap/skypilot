# CLAUDE.md - SkyPilot Development Guide

This document provides guidance for AI assistants working with the SkyPilot codebase.

## Project Overview

SkyPilot is a system to run, manage, and scale AI workloads on any AI infrastructure. It provides a unified interface across 25+ cloud providers (AWS, GCP, Azure, Kubernetes, Slurm, and many others), enabling users to launch compute resources, run jobs, and serve models without vendor lock-in.

**Key capabilities:**
- **Clusters**: Launch and manage compute clusters across clouds and Kubernetes
- **Managed jobs**: Run jobs with automatic recovery from preemptions and failures
- **Multi-cloud and multi-Kubernetes**: Unified interface across 25+ clouds and multiple K8s clusters
- Cost optimization and GPU availability maximization

## Repository Authority

Treat `boltz-bio/skypilot` and its `improvements` branch as the sole
authoritative source for code, history, issues, pull requests, comparisons, and
development baselines in this repository. Fetch, diff, blame, search, and branch
from `origin/improvements`.

Do not add, fetch, consult, compare, or cherry-pick from
`skypilot-org/skypilot`, including its `main` or `master` branches, unless the
user explicitly requests that upstream repository. Pin cross-repository modules
and artifacts to an immutable commit or tag from `boltz-bio/skypilot`.

## Boltz Production Deployment Authority

Deploy and update the Boltz SkyPilot control plane directly with the reviewed
Helm deployment workflow (`helm upgrade --install`, preserving the live release
values on upgrades). A SkyPilot deployment does not require a corresponding
`boltz-bio/boltz-platform` change or pull request. Do not treat a platform
repository pin, Terraform/Terragrunt state, or an open platform PR as the
deployment authority or as a prerequisite for deploying, validating, or
finishing a SkyPilot change.

Ground deployment claims in the live Helm release, its immutable image/chart
version, rollout state, and post-deploy verification. Repository tags or open
pull requests alone do not prove what is deployed.

## Repository Structure

```
skypilot/
├── sky/                    # Main source code
│   ├── __init__.py         # Package exports and version
│   ├── core.py             # Core orchestration logic
│   ├── task.py             # Task definition and YAML parsing
│   ├── resources.py        # Cloud resource specifications
│   ├── optimizer.py        # Resource allocation optimizer
│   ├── execution.py        # Task execution pipeline
│   ├── exceptions.py       # Custom exceptions
│   ├── skypilot_config.py  # Configuration management
│   ├── clouds/             # Cloud provider abstractions (25+ providers)
│   ├── backends/           # Execution backends (Ray-based VM, Docker)
│   ├── provision/          # Cloud resource provisioning
│   ├── jobs/               # Managed job lifecycle
│   ├── serve/              # Model serving (SkyServe)
│   ├── skylet/             # On-cluster execution agent
│   ├── client/             # SDK and CLI
│   ├── server/             # API server and dashboard backend
│   ├── dashboard/          # Web UI (Next.js)
│   ├── utils/              # 50+ utility modules
│   ├── catalog/            # Cloud pricing and instance catalogs
│   ├── data/               # Storage and data handling
│   ├── adaptors/           # Cloud-specific metadata adapters
│   └── schemas/            # Protobuf definitions and API schemas
├── tests/                  # Test suite
│   ├── unit_tests/         # Unit tests with subdirectories per module
│   ├── smoke_tests/        # Quick validation tests
│   ├── integration_tests/  # Component/process and end-to-end tests
│   ├── kubernetes/         # K8s-specific tests
│   └── conftest.py         # Pytest fixtures
├── examples/               # 50+ usage examples
├── llm/                    # 45+ LLM training/serving examples
├── docs/                   # Sphinx documentation
├── charts/                 # Helm charts for K8s deployment
└── format.sh               # Code formatting script
```

## Development Setup

### Supported Python Runtime

- CPython 3.14 or newer is the only supported runtime.
- Python 3.13 and older are not a compatibility target. Do not add new
  compatibility branches, tests, packaging metadata, or CI gates for those
  versions.
- The deployed hub image still runs CPython 3.10: `boltz/Dockerfile.overlay`
  builds on the upstream `berkeleyskypilot/skypilot-nightly` base, and every
  role (API server, controller, executor) reported Python 3.10.19 on
  2026-09-02. The repository `Dockerfile` (`python:3.14.5-slim`) is not that
  image. Until the overlay base moves to Python 3.14, deployed code paths must
  keep working on 3.10: keep the compatibility they already rely on, and keep
  changes that need Python 3.11+ syntax, stdlib, or behavior undeployed.
- Existing controller, worker, packaging, or CI pins below Python 3.14 are
  migration debt rather than a compatibility contract; retire them together
  with the overlay base image, not piecemeal.

### Environment Setup

```bash
# Create virtual environment with uv (Python 3.14, the target runtime; the
# deployed hub image still runs 3.10, see "Supported Python Runtime")
# --seed is required to ensure pip is installed (needed for building wheels)
uv venv --seed --python 3.14
source .venv/bin/activate

# Install in editable mode with all cloud support
uv pip install -e ".[all]"
# Or specific clouds only:
# uv pip install -e ".[aws,gcp,kubernetes]"

# Install development dependencies
uv pip install -r requirements-dev.txt

# Optional: Install pre-commit hooks
uv pip install pre-commit
pre-commit install
```

### Environment Variables

```bash
export SKYPILOT_DEV=1                      # Enable development mode
export SKYPILOT_DEBUG=1                    # Enable debug logging
```

## Code Formatting and Linting

**Always run `format.sh` before committing:**

```bash
bash format.sh         # Format changed files (vs origin/improvements)
bash format.sh --all   # Format entire codebase
bash format.sh --files path/to/file.py  # Format specific files
```

The script runs:
1. **YAPF** - Google style for all Python code
2. **isort** - Import sorting (Google profile)
3. **mypy** - Type checking
4. **pylint** - Linting with custom rules

### Tool Versions (must match exactly)

From `requirements-dev.txt`:
- yapf==0.43.0
- pylint==4.0.4
- mypy==1.19.1
- isort==5.12.0
- ruff==0.15.21
- import-linter==2.13
- basedpyright==1.39.9
- flake8==7.3.0
- flake8-async==27.7.1

### Excluded from Formatting

- `sky/schemas/generated/` - Auto-generated protobuf files
- `build/` - Build artifacts

## Testing

### Running Tests

```bash
# Unit tests (fast, no cloud resources)
pytest tests/unit_tests/

# Specific test file
pytest tests/unit_tests/test_resources.py
```

### Test Layers and Provider Substitution

Use these four test layers consistently:

1. **Unit tests** exercise one unit of policy or orchestration in process. They
   may use focused mocks or stubs for immediate collaborators, but they do not
   prove that a provisioning workflow is integrated correctly.
2. **Production-interface component/process integration tests** enter through
   one real HTTP/CLI/process or production scheduling boundary and keep the
   component implementation intact. Adjacent components may be real or may be
   replaced at narrow owned interfaces. These tests prove that component's
   cardinality, concurrency, memory, cancellation, and responsiveness
   contracts, but are not system E2E proof.
3. **Unpaid provider-interface end-to-end tests** run the real public workflow,
   controller, persistence, planning, and reconciliation paths while replacing
   only external provider network calls with a typed fake provisioning adapter.
   The fake must model asynchronous provider behavior such as delayed status,
   partial success, capacity exhaustion, preemption, lost acknowledgements, and
   deletion; it must not replace the production orchestration being tested.
4. **Paid end-to-end tests** run the same workflows through real provider
   adapters and billable resources. Keep them explicitly approved, bounded in
   size and time, and require teardown plus provider-side absence evidence.

Provisioning must have one narrow, typed substitution boundary consumed by
production orchestration through normal dependency injection. Use named,
immutable request and result types and a `Protocol` or ABC; the production
adapter and typed fake must implement the same interface. Maintain one shared
behavioral contract suite and run it against the fake in ordinary CI and each
real adapter in its paid suite.

A graph of monkeypatches over controller, worker, database, and provider
internals is not end-to-end evidence. Such a test is a unit or integration test
and must be described as one. If an unpaid end-to-end test cannot be written by
swapping only the provider interface, simplify or introduce that production
interface before adding more patches.

Every production incident must gain the cheapest regression test at the
highest practical fidelity. If a typed provider substitution boundary already
exists, provider lifecycle and reconciliation incidents require an unpaid E2E
test through that boundary. If the boundary is incomplete, add an honestly
labeled component/process regression for the immediate defect and make
completing the narrow boundary explicit follow-up work. A paid test validates
the real adapter and environment after unpaid gates pass; it is not a substitute
for a deterministic unpaid regression.

An incident regression must say which pre-fix behavior it rejects and include
a negative or structural control that would fail if the affected production
path were bypassed. A test that merely executes nearby code, asserts only a
successful result, or replaces the defective owner itself is coverage, not a
regression proof.

Verification and observer code must never control, prolong, or falsify the
production state it observes. Model admission, processing/occupancy, and
terminal-publication clocks explicitly, and keep the dependency graph from
stimulus through production effects to proof acyclic. A verifier may consume
those effects; success or failure of the verifier must not determine when the
observed work finishes or when its terminal state is published. A failed proof
may stop future, never-offered stimulus, but the driver must drain already-
offered work through its real terminal-publication path before it exits.

Each non-unit test must state its layer in the module docstring and enter
through the exact public or production scheduling boundary named there. An
unpaid provider E2E must use PostgreSQL plus the real API server, controller,
and executor processes; it may replace only the registered provider facet. Its
fake provider state must survive process restart so the test can exercise lost
acknowledgements, partial waves, delayed visibility, preemption, and deletion
lag without patching controller or database internals. Include a negative
control that fails if production bypasses the facet or if the scenario no
longer traverses the claimed entry point.

Keep the filesystem taxonomy honest as well: unit tests live under
``tests/unit_tests``; process/component and end-to-end tests live under
``tests/integration_tests`` (or the established paid smoke-test location) and
carry exactly one of ``component``, ``unpaid_e2e``, or ``paid_e2e``. Do not
leave a component test in the unit tree or use ``e2e`` in a filename for a
test that replaces the controller, database, planner, or another internal
orchestration layer.

``operator_fixture`` is an orthogonal execution-requirement marker, not a test
layer. Apply it together with ``component`` when a test needs an existing
operator-supplied cluster or comparable external fixture. Such tests must skip
cleanly when their explicit fixture option is absent; hermetic component CI
excludes this marker.

### CI Tests via PR Comments

Trigger CI tests on pull requests using comments:
- `/quicktest-core` - Run quick core tests
- `/smoke-test` - Run smoke tests (launches cloud clusters)
- `/smoke-test --kubernetes --postgres` - Test with PostgreSQL backend on Kubernetes
- `/smoke-test --kubernetes --remote-server --postgres` - Test remote API server with PostgreSQL

### Test Configuration

From `pyproject.toml`:
- Uses pytest-xdist with 16 parallel workers
- Environment: `SKYPILOT_DEBUG=1`, `SKYPILOT_DEV=1`
- Buildkite integration for CI

### Checking Buildkite CI Status

To check CI test results for a PR:

1. **Get Buildkite URL from GitHub**: Check the PR's commit status checks to find the Buildkite build URL (may take a moment to appear after triggering)
2. **Fetch build logs via Buildkite API**: Use the Buildkite API to retrieve detailed logs and test results

```bash
# Example: Get build details (requires BUILDKITE_TOKEN)
curl -H "Authorization: Bearer $BUILDKITE_TOKEN" \
  "https://api.buildkite.com/v2/organizations/skypilot/pipelines/skypilot/builds/<build-number>"
```

## Code Style Guidelines

### General Principles

- Follow [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- Use f-strings over `.format()` for readability
- Use `class MyClass:` not `class MyClass(object):`
- Use `abc` module for abstract classes
- Use Python typing with `if typing.TYPE_CHECKING:` for type-only imports
- Always place imports at the top of the file, not inside function definitions. If this causes a circular dependency, try to resolve it (e.g., restructure modules or use `if typing.TYPE_CHECKING:`). Only as a last resort, place the import inside the function with a comment explaining why. Import performance is not a valid reason for in-function imports; use `LazyImport` instead (see below).

### TODOs and FIXMEs

Always include author attribution:
```python
# TODO(username): Description of what needs to be done
# FIXME(username): Description of what needs fixing
```

### Exceptions vs Assertions

- Use exceptions for errors (better error messages)
- Use `assert` only for debugging/proof-checking

### Lazy Imports

For modules with significant import time (>100ms) that are imported during `import sky`, use `LazyImport` from `sky/adaptors/common.py`:

```python
from sky.adaptors.common import LazyImport
heavy_module = LazyImport('heavy_module')
```

Measure import time with:
```bash
python -X importtime -c "import sky" 2> import.log
tuna import.log  # pip install tuna
```

## Architecture Patterns

### Root-Cause Design and a Single Happy Path

Design changes for the long-term steady state, not merely for the immediate
symptom. Start from the root cause and prefer solutions that reduce complexity,
clarify ownership, and make responsibilities more modular. Gain flexibility
through small composable interfaces and explicit invariants, not through
duplicated implementations, accumulating conditionals, or parallel happy paths.

- Choose one canonical path for each supported behavior and route new work
  through it. Refactor or delete superseded paths instead of leaving multiple
  equally valid implementations that can drift.
- Treat a timeout increase, retry, feature flag, compatibility shim, fallback,
  or one-off special case as a temporary mitigation unless it removes the root
  cause and leaves the architecture simpler. Shipping a mitigation does not
  complete the underlying change.
- When compatibility or staged rollout temporarily requires old and new paths,
  define the steady-state winner before implementation. Mark the old path as
  deprecated, state the migration and rollback gates, and create the concrete
  removal path described in **Transitional Feature PR Stacks** below.
- Keep temporary code isolated behind a narrow boundary, instrument which path
  is used, and test both the transition and final steady states. Remove the
  compatibility branch, flag, metrics, and transition-only tests when the
  documented removal gate passes.
- Evaluate designs by their final topology: the completed system should have
  fewer concepts and ownership boundaries, while supporting future variation
  through the canonical abstraction rather than additional special cases.
- Treat prolonged iteration on one problem as evidence that the abstraction
  may be wrong or the implementation is not KISS enough. If work continues for
  hours without converging, stop adding patches, restate the invariant, and
  simplify or replace the abstraction before proceeding. Optimize for durable
  correctness and useful output per engineering/token cost, not for preserving
  sunk implementation effort.
- During a long fix-forward initiative, merge coherent checkpoints once one
  named invariant and its deterministic regression gates are green. A
  checkpoint is not a backup of partial or knowingly red work. Record
  source-complete, deployed, activated, and production-proven as separate
  states so a merge never overstates operational completion.

### Critical-Path Complexity

- Treat request event loops, admission and dispatch, reconciliation ticks,
  provider effects, and teardown as critical paths. Include nested callbacks
  and retries when calculating their complexity.
- Count every independently scaling dimension (for example replicas x nodes,
  pages x retries, or demand classes x accelerator shapes). Include work and
  I/O hidden behind properties, callbacks, iterators, serializers, ORM lazy
  loads, and called helpers; syntax and module boundaries do not reduce the
  caller's effective complexity.
- Budget remote round trips, serialized wall time, bytes, and retained objects
  as well as CPU operations. An O(N) loop that performs N sequential database,
  provider, DNS, or subprocess calls is not acceptable merely because its CPU
  complexity is linear; batch it or use bounded parallelism and prove the
  end-to-end deadline at the enforced maximum N.
- Do not put a whole-registry scan inside each item's retry, reconciliation,
  enqueue/dequeue operation, or periodic poll. One O(N) aggregate pass per
  bounded tick is valid only with a numeric maximum resident/window
  cardinality and latency budget.
- O(N²) work is forbidden on critical paths unless N is hard-enforced as small
  in the same owning component and a max-bound benchmark plus a structural
  complexity test proves the budget. Configuration convention or today's
  observed fleet size is not a hard bound.
- Outside critical paths, O(N²) is acceptable only for bounded offline,
  migration, build-time, or operator-invoked work where the simpler algorithm
  is materially clearer, the maximum N is documented and enforced, and the
  operation cannot delay request handling, reconciliation, teardown, or
  provider cleanup. A helper inherits critical-path status from any production
  caller; moving the loop into a helper does not make it offline.
- A total stream may be unbounded only when work is O(1) per item, the owning
  component hard-bounds every resident, queued, and in-flight window, and its
  completion formula is explicit in total N (for example,
  ``ceil(N / window) * per_window_deadline``). An unbounded stream does not
  have a fixed convergence deadline.
- Enforce a cardinality or byte bound before materializing, submitting,
  serializing, or retaining a collection. A check inside the eventual callee
  does not protect the caller that already allocated an unbounded list, task
  set, SQL parameter set, or payload.
- Scale tests must enter through the production interface, fill the maximum
  configured resident/window cardinality, exercise multiple windows, and
  prove health/control-plane responsiveness. If total N is bounded, exercise
  that bound too; do not only call helpers against pre-populated internal
  state.
- Complexity regressions must instrument the real SQL, provider, subprocess,
  filesystem, or socket boundary through that production entry point and
  assert calls/statements, rows or bytes, and lock/worker cardinality. AST and
  mock guards are useful structural supplements, but cannot by themselves
  prove production-path cost.
- Every critical-path owner must keep one adjacent, reviewable budget naming:
  the hard maximum resident/window N and where it is enforced; total N when it
  is bounded; work per item and per tick; worker, subprocess, file-descriptor,
  connection, queued-item, queued-byte, output-byte, and resident-memory
  bounds; the per-operation deadline; and the resulting worst-case round or
  stream formula. Tests must derive their max-cardinality assertions from the
  same production constants. A timeout without the cardinality/parallelism
  formula is not a convergence bound.
- An exceptional critical-path O(N²) implementation must name its owner,
  rationale, enforced N, measured max-bound result, and removal/re-evaluation
  gate in code. Its test must fail if the bound is increased without updating
  that evidence. Otherwise use an indexed, incremental, or one-pass design.

### Aggregate Concurrency Budgets

- Bound fan-out at the narrowest owner shared by all work competing for the
  same process memory, provider phase, database pool, or other finite resource.
  Independently bounded sibling pools do not bound their simultaneous sum.
- Before adding or changing a worker pool, inventory overlapping executors,
  queued submissions and retained payload bytes, nested SDK pools and
  connections, nested fan-out, and child processes. Give the shared owner one
  explicit aggregate budget and make internal lanes sum to that budget.
- Regression tests must exercise sibling producers concurrently through the
  production scheduling interface and assert aggregate active work, workers,
  bounded queued work, cancellation, terminal shutdown, and progress under
  dependency contention. They must also prove every production producer routes
  through the shared owner. Testing each producer at its local limit is
  insufficient.
- Executor shutdown is a lifecycle state, not a cache miss. Once the owning
  controller is terminal, racing submissions must fail and no caller may
  recreate workers. Tests must cover submit-versus-shutdown races and assert
  that queued futures are cancelled and all workers/processes are reaped.

### Async Poller and Freshness Safety

- Keep qualification and safety proofs acyclic. A producer may create work, a
  reducer may commit its terminal evidence, and an observer may read that
  evidence; an observer must never cancel, shorten, complete, resubmit, or
  otherwise mutate the work whose outcome it is proving.
- For a multi-actor proof, document an explicit capability matrix and one-way
  dependency graph. Finalizers consume the frozen ownership scope only after
  every already-offered item has reached its terminal reducer; an observer
  verdict is not teardown authority and must not be a prerequisite for item
  completion.
- Exercise the production proof coordinator with injected observer failure,
  timeout, and caller cancellation. These cases must drain all already-offered
  work, publish terminal evidence, run the scope-fenced finalizer exactly once,
  and preserve the original proof verdict without leaking provider resources.
- Model provider mutation submission and provider-state observation as separate
  phases with separate bounded capacity. A scarce mutation slot may cover the
  exact native submit call, but must never remain occupied while polling for
  eventual absence or readiness.
- Persist the handoff between those phases before releasing mutation capacity.
  On restart, resume only the persisted phase: never infer that submission did
  or did not occur from a missing thread, local result, timeout, or exception.
- Releasing a mutation slot is not permission to release economic identity.
  Retain claims, debits, associations, pointers, and request-retention pins
  until exact provider evidence authorizes their atomic settlement.
- Never run synchronous database, provider, filesystem, subprocess, or DNS I/O
  on an event loop that owns request liveness, leases, deadlines, cancellation,
  or safety heartbeats. Move it behind one bounded single-flight interface and
  prove a blocked dependency cannot prevent unrelated timers or probes from
  progressing.
- Define poll cadence explicitly as start-to-start or finish-to-start. Ordering,
  expiry, sleep, and duration arithmetic must use one monotonic clock instance
  in one process; never persist or compare absolute monotonic readings produced
  by another process, replay, or test fixture. Wall-clock timestamps may be
  persisted only as observational metadata, not safety ordering. Cadence
  regressions must include a slow observation and a shifted synthetic clock
  origin so dependency latency cannot become an accidental extra interval or
  an effectively unbounded sleep.
- Cancellation, owner loss, and interrupted observation mean ``UNKNOWN``; they
  are not negative provider or health evidence. A canceled operation must not
  increment failure counters, revoke capacity, or publish a failed receipt.
- Isolate work-item failures. One malformed target or unexpected exception may
  fail that exact item closed, but must not cancel siblings or kill a persistent
  polling loop.
- A persistent poller constructs its TLS context, HTTP session, connector, and
  worker pools once per lifecycle. Per-item recreation defeats pooling and can
  turn bounded concurrency into unbounded retained sockets or contexts.
- Safety freshness must remain live during an indefinite optional persistence,
  telemetry, or composition stall. Bound active, queued, and retained tasks,
  sockets, results, and database batches across target churn—not only connector
  acquisitions for the current snapshot.

### Durable Concurrency and Ownership Fences

- Treat an in-process lock, a PostgreSQL transaction, and any optimistic
  compare-and-set as one concurrency protocol. Never read a whole durable
  record, drop the owning lock, and later write that stale record after another
  writer may have committed. A database row lock during only the second write
  does not prevent that lost update.
- Keep provider and other unbounded remote I/O outside the manager lock. For
  the bounded persistence phase, either reacquire the shared owning lock for
  one deadline-bounded transaction or use field-scoped patches with an exact
  revision predicate. State which mechanism serializes every competing writer
  and test it with the real production writers, not surrogate SQL updates.
- A durable owner fence must include stable lifecycle/configuration identity
  plus a controller incarnation and monotonically changing owner epoch. PID,
  host/IP, thread identity, or lease freshness alone are not identity: they can
  be reused after restart. Tests must rotate incarnation/epoch while preserving
  PID and IP and prove the stale writer is rejected.
- Persist safety-coupled state atomically. If a row becoming ineligible also
  invalidates a route, claim, lease, debit, or association, change both in the
  same transaction. Post-commit effects may refresh process-local caches or
  wake workers, but correctness must survive process death before those effects.
- Do not reuse a policy-specific persistence fence as a generic health or
  observation fence. The database predicate must distinguish universal owner
  identity from per-row policy eligibility; exercise pools and every supported
  action mode against real PostgreSQL.

### Typed Internal State and Compatibility Boundaries

- Represent cross-module domain state that has multiple fields, invariants, or
  an independent lifecycle with a frozen dataclass (or an equivalently typed
  record with named fields). Do not use positional or versioned tuples for such
  state.
- Do not use ``Optional[bool]`` as a state machine. Use an explicit enum whose
  members name every supported state.
- Produce one immutable planner result per reconciliation generation. All
  persistence, actuation, and observability consumers for that generation must
  use that result instead of recomputing it from mutable state.
- Do not use a process-local environment variable as durable policy authority
  for a multi-writer controller. Persist policy in the canonical configuration
  or PostgreSQL state and bind every side effect to that same authority.
- Do not model internal schema evolution as a union of tuple lengths, optional
  key sets, or branches on ``len()``. Decode an actually supported old API or
  persisted representation once at its boundary, validate it, and immediately
  return the single current domain type used by all core logic.
- Make invalid states unrepresentable in the canonical type's constructor.
  Avoid ``Any`` in internal domain contracts and aggregate return values; use a
  named typed result instead.
- Compatibility code requires a concrete external producer or retained state,
  a removal gate, and boundary-focused tests. Test fixtures are not a reason to
  preserve a second production representation.

### Design Documents and Implementation Plans

For non-trivial changes, the repository must contain the canonical design at
`docs/designs/<descriptive-slug>.md` before implementation begins.

- Iterate on that file in place as decisions change. Do not create timestamped
  replacement copies that can diverge from the implementation.
- Keep the design's behavior contract, alternatives, milestones, rollout, and
  test plan synchronized with the code throughout implementation and review.
- Commit the canonical design and include subsequent design corrections in the
  same PR as the implementation they affect. Do not leave the only current copy
  untracked or outside the repository.
- External planning locations such as `~/agent-plans` or a `.plans` symlink may
  be used as scratch space or a pointer, but they are never authoritative. Sync
  an accepted external plan into `docs/designs/` and continue editing only the
  repository copy.
- Run adversarial review against the exact repository design. If that review or
  implementation changes the contract, update the repository file first and
  re-review the updated version before proceeding.

### Cloud Provider Abstraction

All cloud providers inherit from `sky.clouds.Cloud` (in `sky/clouds/cloud.py`). Cloud objects are **lightweight and stateless**. Key design principles:

- Methods should be inexpensive to call
- Don't store heavy state in cloud objects
- Cache cloud-specific queries in `sky/clouds/utils/` modules
- Each cloud implements feature flags via `CloudImplementationFeatures`

Adding a new cloud provider:
1. Create `sky/clouds/<provider>.py` inheriting from `Cloud`
2. Implement required abstract methods
3. Add provisioning logic in `sky/provision/<provider>/`
4. Register in `sky/clouds/__init__.py`

### Client-Server Architecture

SkyPilot uses a client-server model with API versioning:

- Client (SDK/CLI) in `sky/client/`
- Server in `sky/server/`
- API version in `sky/server/constants.py`

**Backward Compatibility Rules (from v0.10.0+):**
- Changes must be backward compatible
- Bump `API_VERSION` when introducing API changes
- Use `@versions.minimal_api_version(N)` decorator for new SDK methods
- Handle version differences with `versions.get_remote_api_version()`

### Central Database Policy

The central/API-server database is PostgreSQL-only. SQLite support for this
database path is deprecated.

- Do not consider SQLite as a design, implementation, migration, or test target
  for central/API-server state when PostgreSQL is available. Avoid compatibility
  branches, fallback behavior, and speculative SQLite coverage in that scope.
- New central/API-server tables and migrations must target PostgreSQL. Do not
  add SQLite compatibility code or SQLite-specific tests for them.
- Store centralized operational history, including minute-level Serve replica
  status snapshots, in PostgreSQL when that data is already collected.
- Keep this scope distinct from local or controller databases that still
  officially support SQLite.

### SkyServe External Load Balancer Policy

- Warm-standby external load balancer high availability is mandatory for new
  non-pool services, not a choice. `load_balancer.high_availability` is still
  accepted in the service YAML for backward compatibility, but the value is
  discarded with a warning. Rationale: with a single slot,
  `force_all_live_unknown` is unconditionally true during the maxSurge
  overlap of every rollout (`controller.py`, the
  `not drain_authoritative and not ha_enabled` term), which marks every live
  replica occupancy-unknown and makes the logical retirement gate abort an
  in-progress drain wave. Two slots remove that term. Do not reintroduce a
  way to select the topology, and do not add opt-in-only paths for it.
- High availability is NOT a fix for drain proof across load balancer
  restarts, and must not be described as one. A rollout replaces both slots
  (the load balancer pod template pins the controller image digest), so the
  per-process session id changes either way and `_ReplicaDrainTracker`
  resets its acknowledgement exactly as it would for a single slot. A
  restarted load balancer still cannot re-acknowledge a replica that is
  already off route, because all four acknowledgement sets are structurally
  unreachable for it. That gap is fixed by the controller-advertised drain
  watchlist, not by topology. See
  `docs/designs/serve-drain-proof-across-lb-restarts.md`.
- Existing persisted services retain their durable load balancer mode until an
  explicit update runs the PostgreSQL-backed migration or rollback protocol.
  Unrelated updates and old pickles must not silently change that mode.
- Pools have no inference endpoint and remain outside the two-slot load
  balancer topology.

### Protobuf Regeneration

When modifying `.proto` files in `sky/schemas/proto/`:

```bash
python -m grpc_tools.protoc \
    --proto_path=sky/schemas/generated=sky/schemas/proto \
    --python_out=. \
    --grpc_python_out=. \
    --pyi_out=. \
    sky/schemas/proto/*.proto
```

### Dependency Management

Dependencies are defined in `sky/setup_files/dependencies.py`:

- **Core dependencies**: Listed in `install_requires`
- **Cloud-specific**: Defined in `extras_require` (e.g., `aws`, `gcp`, `kubernetes`)
- **Development**: Listed in `requirements-dev.txt`

When updating dependencies:

1. Check version constraints carefully - some packages have breaking changes
2. Consider Python version compatibility (3.14 target; the deployed hub
   interpreter is 3.10 until the overlay base image moves)
3. Test with both minimum and latest allowed versions
4. Document version constraints with comments explaining why

## Key Modules Reference

| Module | Purpose |
|--------|---------|
| `sky/core.py` | Core orchestration (launch, exec, stop, down) |
| `sky/task.py` | Task YAML parsing and specification |
| `sky/resources.py` | Resource requirements (GPUs, memory, disk) |
| `sky/optimizer.py` | Cloud selection and cost optimization |
| `sky/client/sdk.py` | Python SDK implementation |
| `sky/client/cli/` | CLI commands |
| `sky/backends/cloud_vm_ray_backend.py` | Main execution backend for clusters |
| `sky/provision/provisioner.py` | Resource provisioning |
| `sky/jobs/` | Managed jobs with recovery and scheduling |

## Major Feature Designs

Canonical designs are required only for major features and must be checked
into `docs/designs/` as Markdown files before implementation begins. A major
feature has at least one of these characteristics:

- It introduces a new cross-cutting architecture or responsibility boundary
  spanning multiple core subsystems, clouds, backends, or control/data planes.
- It adds durable state, migrations, or an operational component that requires
  a staged deployment and rollback strategy.
- It adds a broad public interface or configuration contract whose adoption
  requires coordinated compatibility, migration, or activation work.

Do not add designs for localized features, bug fixes, routine refactors,
tests, documentation-only work, or maintenance changes. Use a descriptive
kebab-case filename such as
`docs/designs/managed-container-image-distribution.md`.

- Iterate on that file in place as decisions change. Do not create timestamped
  replacement copies that can diverge from the implementation.
- Treat the checked-in design as a living source of truth. Update it in the same
  change whenever scope, architecture, public interfaces, compatibility,
  migrations, rollout steps, or verification status changes.
- Distinguish completed work from pending operational or follow-up work. Do not
  mark a feature complete while its design still describes stale behavior or
  unfinished required gates.
- Include, at minimum, status and last-updated metadata, goals and non-goals,
  the public contract, architecture and invariants, implementation phases,
  deployment and rollback behavior, verification evidence, and open gates.
- Record intentional implementation departures and their rationale in the
  design instead of letting code and design silently diverge.
- External planning locations such as `~/agent-plans` or
  `agent/feature-plans/` may be scratch space or pointers, but are never
  authoritative. Sync an accepted plan into `docs/designs/` and continue
  editing only the repository copy.
- Run adversarial review against the exact repository design. If review or
  implementation changes the contract, update the design first and re-review
  the updated version before proceeding.

## Pull Request Guidelines

### Transitional Feature PR Stacks

When a feature introduces temporary transition, compatibility, dual-write,
rollout, or fallback code that should be removed after the new behavior is
confirmed, create the removal change at the same time as the feature change.

- Submit the feature/transition PR and the cleanup/removal PR as a stack using
  [gh-stack](https://github.github.com/gh-stack/). Do not leave the cleanup as
  only a TODO or a future issue.
- When an initiative replaces an old solution with a better-architected one,
  mark the old path as deprecated in the code, user/operator documentation,
  and canonical design as applicable. Immediately create the stacked PR that
  removes the deprecated path; do not wait until after rollout to author it.
- Keep the removal PR in draft or otherwise blocked until the feature's
  documented validation and rollout gates have passed. The feature PR must
  link to the removal PR, and the removal PR must state its exact merge gate.
- Include tests in both PRs: the transition PR must test mixed/rollout states,
  while the removal PR must test the final steady state without the temporary
  path.
- Record both PRs and the evidence required to unblock removal in the canonical
  design document. Update the stacked removal PR whenever implementation or
  rollout decisions change.

1. **Branch from improvements**, create descriptive branch name
2. **Run `format.sh`** before committing
3. **Add tests** for core system changes
4. **Run smoke tests** for significant changes
5. **Include `Tested:` section** in PR description with test plan
6. **Delete branch** after merging

### PR Description Format

PRs should include:
- **Summary**: Brief description of changes (1-3 bullet points)
- **Test plan**: How the changes were tested (commands run, manual verification steps)

**Important**: Always generate a manual test plan describing how to verify the changes work correctly. Include specific commands, expected outputs, or UI verification steps. Whenever possible, add unit tests and smoke tests for the changes.

### Commit Message Format

Use the `[Area] Description` format:
- `[Core]` for core system changes
- `[CLI]` for CLI changes
- `[API]` for API server changes
- `[Docs]` for documentation
- `[AWS]`, `[GCP]`, `[Azure]`, `[Kubernetes]` for cloud-specific changes
- `[Jobs]` for managed jobs
- `[Dashboard]` for web UI changes
- `[Serve]` for model serving (SkyServe)
- `[Test]` for testing changes
- `[CI]` for CI/CD changes

Examples:
- `[Core] Fix cluster status refresh logic`
- `[AWS] Add support for new instance types`
- `[Docs] Update installation guide`

## API Server Testing

### Local API Server (Recommended for Development)

```bash
# Always restart API server after code changes to pick up changes
sky api stop
sky api start

# Verify server is running
sky api status
```

### Dashboard Development

**For local API server development**, rebuild the dashboard before restarting:

```bash
# Install dependencies (first time or after package.json changes)
npm --prefix sky/dashboard install

# Rebuild the dashboard
npm --prefix sky/dashboard run build

# Then restart the API server
sky api stop
sky api start
```

**For remote API server (Docker/Kubernetes)**, the Dockerfile automatically builds the dashboard - no manual build needed before `docker build`.

The dashboard is a Next.js application. For development with hot reloading:

```bash
# Run dashboard in development mode (separate from API server)
cd sky/dashboard
npm run dev
```

### Mocking Remote API Server Locally

To test remote API server behavior locally:

```bash
# Start local API server (runs on port 46580 by default)
sky api stop
sky api start

# Forward to a different port to simulate remote server
socat TCP-LISTEN:46590,fork TCP:127.0.0.1:46580 &

# Connect to the forwarded port as if it were a remote server
sky api login -e http://127.0.0.1:46590
```

### Remote API Server (Kubernetes Deployment)

For testing with a remote API server on Kubernetes:

```bash
# Build local changes
helm repo add skypilot https://helm.skypilot.co
helm repo update
helm dependency build ./charts/skypilot

# Build Docker image
DOCKER_IMAGE=my-repo/skypilot:v1
docker buildx build --push --platform linux/amd64 -t $DOCKER_IMAGE -f Dockerfile .

# Deploy (NEW installation)
NAMESPACE=skypilot
RELEASE_NAME=skypilot
helm upgrade --install $RELEASE_NAME ./charts/skypilot --devel \
    --namespace $NAMESPACE \
    --create-namespace \
    --set apiService.image=$DOCKER_IMAGE
```

#### Upgrading Existing Deployments

**CRITICAL:** Always use `--reuse-values` to preserve database/credential config:

```bash
# Upgrade existing deployment (keeps PostgreSQL, auth, etc.)
helm upgrade skypilot ./charts/skypilot -n skypilot --reuse-values \
    --set apiService.image=$DOCKER_IMAGE

# Check current values / rollback if needed
helm get values skypilot -n skypilot
helm rollback skypilot <revision> -n skypilot
```

#### PostgreSQL Backend

```bash
# Create connection secret
kubectl create secret generic db-uri -n skypilot \
    --from-literal=uri="postgresql://user:pass@host:5432/db"

# Deploy with PostgreSQL
helm upgrade --install skypilot ./charts/skypilot -n skypilot \
    --set apiService.dbConnectionSecretName=db-uri \
    --set storage.enabled=false
```

## Critical Code Paths (Handle with Care)

The following modules contain complex, stateful logic that requires careful review when modifying:

**Managed Jobs Recovery:**
- `sky/jobs/controller.py` - Job lifecycle management, state transitions, async coordination
- `sky/jobs/recovery_strategy.py` - Preemption recovery, retry logic for managed jobs
- `sky/backends/cloud_vm_ray_backend.py` - Execution backend with complex state handling

These modules handle edge cases like preemption during job submission, controller failures mid-recovery, and race conditions between concurrent operations. Changes can have subtle effects on job reliability.

**API Server Performance & Robustness:**
- `sky/server/` - API server with memory efficiency, low latency requirements
- `sky/backends/backend_utils.py` - Cluster status caching, network resilience, SSH handling

These modules are optimized for performance. Be cautious about adding blocking calls, memory-heavy operations, or changing caching behavior.

**CLI/SDK Interface Design:**
- `sky/client/cli/` - Command-line interface
- `sky/client/sdk.py` - Python SDK

Be cautious about adding new interfaces or changing existing UX significantly. Keep the interface clean and minimal for both CLI and SDK.

**Backward Compatibility:**
- Test with old server + new client, and old client + new server
- Ensure existing clusters and jobs continue working after server upgrades
- `/quicktest-core` tests some backward compatibility scenarios
- See `docs/source/developers/CONTRIBUTING.md` for API versioning guidelines

## Common Pitfalls

1. **Always restart API server after code changes** - Run `sky api stop; sky api start` to pick up changes
2. **Don't modify `sky/schemas/generated/`** - These are auto-generated
3. **Match formatter versions exactly** - Version mismatches cause CI failures
4. **Consider import time** - Heavy imports slow down CLI responsiveness
5. **API versioning** - Always maintain backward compatibility

## Useful Commands

```bash
# Profile CLI performance
uv pip install py-spy
py-spy record -t -o sky.svg -- python -m sky.cli status

# Check cloud credentials
sky check

# View cluster status
sky status

# Launch a cluster
sky launch <cluster-name> <cluster-spec.yaml>

# Execute a command on the cluster
sky exec <cluster-name> -- bash

# SSH into the cluster
ssh <cluster-name>

# Launch a managed job
sky jobs launch <job-spec.yaml>
```

## Documentation

- **User docs**: https://docs.skypilot.co/
- **Source**: `docs/source/` (Sphinx)
- **Build docs**: See `.readthedocs.yml`

## Additional Resources

- **Full contributing guide**: `docs/source/developers/CONTRIBUTING.md`
- **User docs**: https://docs.skypilot.co/
- **GitHub Issues**: https://github.com/boltz-bio/skypilot/issues
- **Slack**: http://slack.skypilot.co
