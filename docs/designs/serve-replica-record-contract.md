# SkyServe Replica Record Contract

## Context

`sky/serve/replica_managers.py` is 8,095 lines and currently owns two
materially different components:

- the versioned per-replica record, including lifecycle status derivation,
  storage compatibility, endpoint and presentation projection, and readiness
  probing; and
- the stateful service orchestrator, including recovery, launch admission,
  placement, scaling, drain ordering, teardown, and background reconciliation.

The record contract occupies 865 lines. It is read and reconstructed directly
by persistence, autoscaling, controller, API, and test callers that do not need
the manager implementation. The manager consumes the same record as its state
unit, but owns process threads, locks, lifecycle fences, and durable actuation.

## Responsibility map

### Replica record and status contract

- Callers: `serve_state`, autoscalers, controller status and recovery paths,
  paid and reserved capacity accounting, API projection, and focused tests.
- Dependencies: `ReplicaStatus`, `ProcessStatus`, cluster records and handles,
  endpoint lookup, pricing and resource rendering, placement locations, TLS
  probe clients, and the versioned JSON and legacy pickle contracts.
- State owned: one replica's identity, version, launch and readiness state,
  placement provenance, logical width, retirement metadata, and compatibility
  version.
- Failure modes: status drift, incompatible persisted rows or pickles, lost
  `None` image keys, extra cluster queries, wrong endpoints or costs, and probe
  exceptions escaping a fleet round.
- Performance sensitivity: every controller tick and status fleet scan touches
  these objects. The extraction must add no wrapper call, copy, query, lock, or
  network operation.
- Change cadence: schema compatibility, status semantics, API projection, and
  replica probe transport.

### Service replica orchestration

- Callers: the Serve controller, autoscaler decisions, service updates,
  recovery, and scale-up and scale-down APIs.
- Dependencies: lifecycle and controller-owner fences, worker pools and
  watchdogs, placement catalogs, paid and reserved capacity brokers, drain
  reports, service versions, and database mutation helpers.
- State owned: manager locks, worker pools, queued launches and teardowns,
  logical target generations, placement state, retry budgets, and recovery
  progress.
- Failure modes: split ownership, overlaunch, leaked capacity, stale
  retirements, incorrect lifecycle ordering, and controller stalls.
- Performance sensitivity: bounded thread counts, batched database and SSH
  operations, lock hold time, and controller tick duration.
- Change cadence: scaling policy, placement admission, recovery, and lifecycle
  protocols.

### External cluster lifecycle gateway

- Callers: replica launch and teardown workers plus recovery cleanup.
- Dependencies: task parsing, SDK launch and down requests, workspace context,
  TLS and security-group mutation, ownership guards, and drain observation.
- State owned: in-flight request IDs, cancellation signals, retry counters, and
  per-call drain observations.
- Failure modes: stale-owner cloud mutations, retry storms, incomplete cleanup,
  or exceeding a bounded drain.
- Performance sensitivity: remote cloud operations dominate, but watchdog and
  retry cadence are correctness-sensitive.
- Change cadence: provider failure classification, security, and teardown
  policy.

### Logical capacity reconciliation

- Callers: logical autoscaling, rolling updates, load-balancer synchronization,
  and controller recovery.
- Dependencies: exact-card targets, observed capacity and occupancy
  generations, placement catalogs, and retirement fences.
- State owned: reconcile snapshots, target generations, pending launch
  admissions, and recovery waves.
- Failure modes: double capacity, under-capacity, stale target admission, or
  unsafe retirement.
- Performance sensitivity: whole-fleet scans and bounded actuation per tick.
- Change cadence: logical-replica semantics and autoscaling policy.

## Decision

Move `ReplicaStatusProperty`, replica resource-state encoding and decoding, and
`ReplicaInfo` into `sky/serve/replica_info.py`. Move the shared
`_is_valid_drain_started_at` validator with the record contract and re-export it
to the manager because drain admission and storage decoding enforce the same
persisted timestamp invariant.

Keep `sky.serve.replica_managers` as the stable facade with direct aliases for
all moved symbols. Restore the historical `sky.serve.replica_managers` module
identity on both classes and the moved functions so existing imports, pickle
globals, help output, and serialized identities remain valid. Keep the same
logger and imported dependency module objects so patches of
`replica_managers.backend_utils`, `global_user_state`, `replica_tls`,
`resources_utils`, and related seams continue to affect record behavior.

This is a facade-first plain-module extraction. There is one record contract,
so an abstract base class, protocol, registry, strategy, factory, or dependency
injection layer would add surface without a second implementation. Direct
aliases avoid forwarding methods and hot-path overhead.

## Behavior contract

- Every `ReplicaStatusProperty` input maps to the same `ReplicaStatus`.
- `ReplicaInfo` construction, attributes, version, validation, and
  representation remain unchanged.
- Versioned JSON output, JSON reconstruction, legacy pickle migration, class
  globals, and pickle bytes remain compatible.
- Resource override encoding retains `None` image-region keys losslessly and
  continues to decode legacy `"null"` keys.
- Cluster handle, URL, pricing, resource strings, and status payloads preserve
  output and database, endpoint, pricing, and rendering call counts.
- Pool and HTTP probes preserve request method, TLS client, headers, timeout,
  result tuple, logging, and broad exception containment.
- The manager retains its lock, thread, recovery, launch, drain, scaling, and
  lifecycle ordering.

## Alternatives considered

### Leave the record in `replica_managers`

This avoids one module but keeps a widely consumed data and compatibility
contract buried in an 8,095-line stateful orchestrator. The record has its own
callers, state, failure modes, and reasons to change, so the module boundary is
not line-count-only.

### Extract only `ReplicaStatusProperty`

This is lower risk but leaves storage, projection, probing, and compatibility
behavior mixed into the manager. It removes fewer than 200 lines and does not
establish the actual per-replica record boundary.

### Extract launch and teardown first

Those workers are cohesive, but `launch_cluster` currently resolves facade
functions that tests and controllers patch by name. Preserving those seams
would require a forwarding wrapper or injected callbacks. The record can move
with direct aliases and no additional call layer.

### Split `ReplicaInfo` into storage, presentation, and probe classes

There is no second implementation and callers rely on one object that carries
the durable replica identity. Multiple collaborators would increase
construction and compatibility surface. The record remains cohesive as one
versioned entity even though its methods project and probe that entity.

## Implementation milestones

1. Add and run characterization tests on the existing classes.
2. Move the record implementation without behavioral edits.
3. Add direct facade aliases and restore historical module identities.
4. Prove AST equality, output and pickle compatibility, call counts, and
   representative storage and projection timing.
5. Run focused, component, formatting, static, and exact-head CI gates.

## Changed-path-to-test matrix

| Changed path | Responsibility | Verification |
|---|---|---|
| `sky/serve/replica_info.py` | status, storage, projection, and probing record | new contract suite, lazy-handle, probe, status, cost, and persistence tests |
| `sky/serve/replica_managers.py` | facade aliases and manager consumer | facade identity and pickle tests plus full replica-manager suite |
| `tests/unit_tests/test_serve_replica_record_contract.py` | characterization and extraction contract | run before and after the production move |
| this design | canonical responsibility and compatibility contract | format and diff checks |

## CI mapping

The Python Tests workflow must collect the new unit test and the existing Serve
unit suites for both changed Python paths. Static analysis, format, Pylint,
mypy, and import-linter jobs must run on the exact pushed head. If path filters
skip any relevant job, update CI coverage or leave the PR open.

## Performance evidence

Compare the baseline and extracted implementations using identical in-memory
status and storage records. Record byte output, pickle output, database and
endpoint call counts, and repeated `to_storage_dict` and
`from_storage_dict` timing. Measure cold `replica_managers` import time in
alternating subprocess samples. Accept only identical call counts and no
material regression.

## Rollout and rollback

This changes Python module ownership only. There is no schema, API, database,
configuration, CLI, or deployment migration. Rollback is the single commit
revert. Merge only after every relevant check succeeds on the exact pushed SHA
and no actionable review thread remains.
