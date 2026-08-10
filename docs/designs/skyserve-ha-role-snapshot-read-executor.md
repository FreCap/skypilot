# SkyServe HA role snapshot read executor

## Problem

Each SkyServe HA role heartbeat runs in a controller-owned executor, then reads
the load-balancer Pods and Service concurrently. The inner snapshot helper
currently constructs and destroys a two-worker executor for every heartbeat.
That repeats thread lifecycle work on the controller's liveness path even
though the read shape and maximum outer concurrency are fixed.

## Behavior contract

- A controller owns one snapshot-read executor for its entire lifespan.
- Stable role heartbeats and legacy-to-HA transitions use that same executor.
- The two Kubernetes reads remain concurrent and retain their deterministic,
  fail-closed error ordering.
- Direct helper callers that do not supply an executor retain a self-owned
  fallback with the existing synchronous lifetime.
- Controller shutdown stops accepting queued role work and snapshot reads
  without blocking the event loop.
- The change performs exactly the same Pod-list, Service-read, and ownership
  validation calls as before.

## Design

`SkyServeController` creates a four-worker snapshot-read executor next to its
existing two-worker role executor and shuts both down in `lifespan()`. Both
controller snapshot call sites pass the read executor to
`get_lb_role_snapshot()`.

The inner helper accepts an optional `concurrent.futures.Executor`. When one is
provided, it submits the Pod and Service reads without assuming ownership of
the executor. When omitted, it creates and shuts down the historical local
two-worker pool. A four-worker shared pool matches the maximum two concurrent
outer role requests times two independent reads, so this removes executor
churn without adding queueing at the supported concurrency bound.

## Alternatives

- Serializing the Pod and Service reads would remove the inner pool but add
  their latencies on a heartbeat liveness path.
- Reusing the two-worker outer role executor for nested reads can deadlock when
  both outer workers wait for inner work queued to the same saturated pool.
- Lazily constructing the shared pool would add synchronization and leak risk
  to a resource with an explicit controller lifecycle.

## Rollout and recovery

The change is process-local and has no schema, protocol, or durable-state
migration. A controller restart reconstructs both pools. Snapshot failures and
authority changes continue to fail closed, and direct callers can roll
independently through the optional fallback.

## Changed-path-to-test matrix

| Path | Invariant | Coverage |
| --- | --- | --- |
| `sky/serve/controller.py` | one pool across stable/transition reads; shutdown owns both pools | focused controller transition and HA lifespan/reuse tests |
| `sky/serve/lb_k8s.py` | supplied pool avoids construction; fallback and error order remain intact | provided-executor, deterministic-error, and direct snapshot tests |
| `tests/unit_tests/test_serve_controller.py` | synthetic controllers mirror production ownership | complete controller test file |
| `tests/unit_tests/test_serve_lb_ha.py` | lifecycle, concurrency, fencing, and exact submission count | focused HA matrix and complete HA test file |

## Performance evidence

Tests assert that repeated heartbeats observe the same executor identity, that
a supplied executor performs exactly two submissions while local executor
construction is forbidden, and that backend call counts do not increase. The
worker bound follows directly from two outer workers times two inner reads.
