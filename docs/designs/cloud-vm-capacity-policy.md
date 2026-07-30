# Cloud VM Capacity Policy

## Status

Implemented.

## Context

`sky/backends/cloud_vm_ray_backend.py` is 7,142 lines and contains the Cloud VM
provisioning retry orchestrator, resource-handle state, task execution,
teardown, log transport, and a low-state family of provider-capacity policy
helpers. File size is only a prioritization signal. This design extracts the
capacity-policy family because it has a complete boundary and materially
different consumers, dependencies, state, failure modes, and reasons to
change from the retry orchestrator.

The capacity policy was introduced and then hardened across several focused
capacity and Serve changes. It now has one caller in Serve placement policy,
several callers in Cloud VM retry orchestration, and a dedicated 1,000-line
regression suite. The implementation is stateless apart from access to the
existing process-shared `capacity_cache`; the retry orchestrator owns blocked
resources, attempt history, cluster generations, cleanup, and provider
lifecycle ordering.

## Before responsibility map

### Provider-capacity evidence and terminal classification

- Callers: `RetryingVmProvisioner`, Serve replica failure handling, placement
  outcome recording, and focused classification tests.
- Dependencies: typed AWS and GCP error codes, explicit exception causes,
  `ResourcesUnavailableError.failover_history`, and defensive traversal
  bounds.
- State owned: immutable provider-code sets and traversal limits; no mutable
  state.
- Failure modes: treating authentication or network failures as capacity,
  treating quota as zonal capacity, accepting malformed or cyclic histories,
  or exhausting controller CPU or memory on adversarial histories.
- Performance sensitivity: classification is on the failure path, but traversal
  must remain bounded to 32 levels and 1,024 nodes with no extra provider,
  database, or network call.
- Change cadence: provider error shapes, terminal failure aggregation, and
  Serve placement policy.

### Exact capacity-cache eligibility and key projection

- Callers: `RetryingVmProvisioner` cache read, mark, cooldown, and successful
  clear paths plus focused capacity-storm tests.
- Dependencies: `Resources`, regions and zones, active cloud identity,
  `skypilot_config`, and `capacity_cache` key contracts.
- State owned: no mutable state. It projects exact immutable cache keys and
  evaluates whether provider evidence covers the full demand.
- Failure modes: cross-account suppression, cross-accelerator contamination,
  caching partial requests, sharing quota across regions, or caching
  unsupported providers and on-demand launches.
- Performance sensitivity: pure projection runs once per attempt and must not
  add provider queries, database reads, copies, or retries.
- Change cadence: cache key versions, provider eligibility, identity formats,
  and demand-proof rules.

### Provision retry orchestration

- Callers: Cloud VM launch, start, recovery, and provider failover paths.
- Dependencies: optimizer candidates, cloud feature checks, zone iteration,
  capacity-cache I/O, cluster config generation, provider provisioning, Ray
  setup, teardown, global state, and user-visible progress.
- State owned: blocked resources, attempt histories, active cluster hashes,
  wheel state, provision records, resource handles, and lifecycle ordering.
- Failure modes: capacity exhaustion, leaked infrastructure, stale cluster
  state, invalid failover, incomplete cleanup, or partially configured
  clusters.
- Performance sensitivity: provider calls, SSH, teardown, locks, retry
  backoff, and config generation dominate.
- Change cadence: provider lifecycle, failover behavior, image placement,
  cluster state, cleanup fencing, and runtime setup.

## Chosen seam

Move the pure evidence, classification, exact-key projection, and demand-proof
functions to `sky/provision/capacity_policy.py`. Keep direct aliases in
`sky.backends.cloud_vm_ray_backend` for every historical symbol, including the
public `classify_resources_unavailable_error` entrypoint. The moved functions
retain `sky.backends.cloud_vm_ray_backend` as their historical `__module__`,
so reflection and pickle identities continue resolving through the façade.

The side-effectful cache access, metrics, operator notification, placement
history recording, and mutable retry loop remain in
`cloud_vm_ray_backend.py`. This keeps existing late-bound monkeypatch seams
such as `_record_capacity_metric`, `_record_insufficient_quota_notification`,
and `_capacity_cache_exhausted_zone_names` intact.

## Why this abstraction

A façade-first plain module is sufficient:

- A strategy would imply interchangeable capacity algorithms. There is one
  conservative policy and no second implementation.
- An adapter would be appropriate for provider SDK translation, but these
  helpers consume already-normalized exceptions, resources, and cache
  contracts rather than adapting a provider API.
- A factory or builder is unnecessary because the result is a classification
  string, boolean proof, or immutable key.
- A class would introduce artificial lifecycle and hidden state around pure
  functions.
- Moving the whole `RetryingVmProvisioner` would relocate a 1,465-line
  stateful orchestrator without separating responsibilities.
- Moving only the cache-key functions would leave the shared provider-code
  taxonomy split across modules. Moving side-effectful cache access would
  break established façade-local monkeypatch seams or require forwarding
  wrappers.

The chosen boundary moves the complete low-state policy leaf and leaves
orchestration and side effects where their state and patch ownership already
live.

## Behavior contract

- All historical `cloud_vm_ray_backend` symbols remain direct aliases, not
  wrappers.
- Function signatures, return values, exception behavior, historical
  `__module__`, and pickle identity remain unchanged.
- Capacity and quota codes remain provider-scoped and unknown codes remain
  conservatively unclassified.
- Terminal traversal retains the same depth, node, cycle, and malformed-entry
  limits.
- Cache eligibility remains limited to the exact supported cloud, account,
  region, zone, instance type, accelerator set, node count, and Spot demand.
- No provider, database, metric, notification, or placement-history call is
  added, removed, or reordered.
- Existing façade-local monkeypatch sites continue controlling retry behavior.

## Implementation milestones

1. Add characterization assertions for signatures, historical module
   identity, and pickle identity on the unchanged implementation.
2. Add `sky/provision/capacity_policy.py` with unchanged function bodies.
3. Replace the original definitions with direct façade aliases and remove
   imports used only by the moved policy.
4. Extend the contract test to prove new-module and façade identity.
5. Run the focused and component test matrix, static tools, import checks, and
   performance comparison.

## Changed-path-to-test matrix

| Changed path or seam | Tests |
| --- | --- |
| `sky/provision/capacity_policy.py` provider classification | `test_failover_classification.py` terminal, provider-code, mixed-code, cycle, and bounded-history cases |
| exact cache-key and demand proofs | `test_failover_classification.py`, `test_capacity_storm_path.py` |
| `cloud_vm_ray_backend.py` façade aliases and retry callers | capacity-policy contract assertions, retry-zone cases, `test_cloud_vm_ray_backend.py` |
| Serve terminal classifier caller | `test_serve_replica_managers.py` and `test_serve_placement_history.py` |
| import and serialization compatibility | capacity-policy signature, direct identity, historical module, and pickle assertions |

## Performance evidence plan

Compare alternating fresh-process imports of
`sky.backends.cloud_vm_ray_backend` before and after the extraction. Verify
that direct aliases add no wrapper frame. Runtime bodies for the moved
functions must be AST-equivalent after removing location metadata, proving no
new provider call, cache query, copy, loop, or retry.

## CI mapping

The pull-request workflows targeting `improvements` have no relevant
changed-path filters. The Unit Tests matrix collects the full
`tests/unit_tests` tree containing the focused tests above. Format, mypy,
Pylint, BasedPyright, import-linter, limited-dependency, and compile checks
cover the new production module and the façade.

## Validation evidence

- The characterization test passed before extraction and now proves 14
  historical call signatures, direct new-module/façade identity, historical
  `__module__`, and pickle round trips. Signature comparison strips annotation
  rendering because Python 3.14 represents `Optional[ForwardRef(...)]` as a
  union while direct object identity already preserves annotation metadata.
- All 16 moved function bodies are AST-equivalent to the
  `7b5f87c0a9ea5b90809c2582648df0d4283b32d4` baseline when source locations
  are ignored.
- The final formatted state passes 465 focused unit tests:
  54 failover-classification, 2 capacity-storm, 331 Serve replica-manager,
  66 Cloud VM backend, and 12 placement-history cases.
- The Cloud VM backend integration file collects four parametrized cases,
  proving the changed production path remains represented in the integration
  suite without launching cloud resources locally.
- `format.sh --files` passes YAPF, isort, mypy across 768 source files, Pylint
  at 10.00, dashboard ESLint, and Prettier. Ruff, import-linter, compileall,
  both staged and unstaged `git diff --check`, and an isolated
  BasedPyright 1.39.9 run all pass.
- Eight alternating fresh-process import samples measured a 0.979283-second
  baseline median and 0.976104-second extracted median, a 0.325% improvement.
  Direct aliases add no wrapper frame, and AST equivalence proves no provider,
  database, cache-query, copy, loop, or retry change in the moved bodies.

## Rollout and rollback

This is a local structural extraction with no CLI, API, wire, schema,
database, configuration, serialized-data, or remote-command change. Rollback
is a single commit revert.
