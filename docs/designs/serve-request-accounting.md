# Serve request accounting ownership

Status: accepted

## Problem

`sky/serve/serve_utils.py` owns several independent Serve concerns. Its
`RequestsAggregator` contract and `RequestTimestamp` implementation form a
379-line in-memory request-accounting model, but they live beside controller
HTTP transport, service lifecycle locking, persistence coordination, status
projection, termination, log streaming, and presentation helpers.

The accounting model has one production consumer, the external load balancer,
and changes for arrival compatibility, rejection counts, prediction-time
histograms, acknowledgement, pruning, and failed-sync restoration. It does not
use service lifecycle locks, database state, controller transport, replica
management, log files, or CLI presentation.

## Responsibility map

| Responsibility | Callers | Dependencies | State | Failure modes | Performance | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Request accounting contract and implementation | `SkyServeLoadBalancer` and focused unit tests | bounded deques, clock, histogram constants, request attributes | timestamps, compatibility profiles, cumulative and acknowledged histories | dropped or duplicated demand, unbounded memory, wrong acknowledgement, histogram drift | one synchronous operation per request plus minute-boundary pruning | load-balancer demand and telemetry |
| Controller request routing and owner fencing | SDK, CLI, dashboard, controller helpers | owner rows, HTTP, auth tokens, retry policy | transient owner fingerprints and responses | stale-owner mutation, retry amplification, timeout | network hot path | HA and controller API |
| Service lifecycle and persistence coordination | Serve launch, update, recovery, termination | locks, state store, task/YAML preparation | durable service, pool, replica, and lifecycle epochs | race, orphan, incompatible recovery | DB and cloud calls | lifecycle policy |
| Status, log, and presentation helpers | SDK, CLI, dashboard, controllers | state projection, filesystem, formatting, legacy code generation | transient projections and log cursors | compatibility or output drift | read and streaming latency | user-visible Serve UX |

## Decision

Move `RequestsAggregator` and `RequestTimestamp` unchanged to
`sky/serve/request_aggregator.py`. Keep direct aliases in
`sky.serve.serve_utils` and restore the historical `__module__` value on both
classes. The load balancer continues importing the stable `serve_utils`
facade, so this is a structural ownership change only.

This is a plain-module extraction. A strategy hierarchy would be speculative
because there is one implementation. A repository or adapter does not fit
in-memory accounting, and a forwarding wrapper would add a hot-path call frame
without preserving class identity.

## Behavior contract

- Public imports from `sky.serve.serve_utils` keep identical class objects.
- Pickles continue naming `sky.serve.serve_utils.RequestsAggregator` and
  `sky.serve.serve_utils.RequestTimestamp`.
- Request attributes, timestamps, bounded deque behavior, pruning cadence,
  payload bytes, acknowledgement, drain/restore ordering, validation errors,
  and `repr()` remain unchanged.
- Load-balancer call counts and synchronization ordering remain unchanged.
- The extraction adds no wrapper, copy, timer, request, lock, or database call.

## Implementation milestones

1. Add characterization tests on the unsplit tree.
2. Move the two classes without behavioral edits.
3. Add facade identity and pickle compatibility assertions.
4. Run focused, adjacent, formatting, static-analysis, import, and CI gates.

## Test and rollout plan

The new characterization suite covers public identity, pickle payloads,
bounded timestamp batches, request and rejection history, prediction
histograms, acknowledgement, and failed-sync restoration. Existing
load-balancer tests cover all production integration points. Validate source
equivalence by comparing class ASTs before and after extraction, run the Serve
load-balancer suites, `format.sh --files`, `git diff --check`, import-linter,
compileall, and measure alternating cold imports. Roll out as a normal
backward-compatible code change with no migration, configuration, or API
version change.
