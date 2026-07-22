# Non-blocking SkyServe replica termination

## Status

Accepted design for an urgent, independently deployable SkyServe control-plane
liveness fix.

## Problem

`POST /controller/terminate_replica` is an async FastAPI route, but it performs
synchronous PostgreSQL reads and calls `ReplicaManager.scale_down()` directly.
`scale_down()` acquires the replica manager's process-local `threading.Lock`.
Large-fleet recovery, probing, and placement operations can hold that lock for
tens of seconds or minutes. While the route waits for the lock, it monopolizes
the controller's uvicorn event loop, delaying health checks, owner checks, and
load-balancer sync. The supervisor can then replace an otherwise healthy
controller child.

The exact base also prevents the supervisor from replacing a still-live child
solely because HTTP health is stale. That closes the overlapping-writer failure
mode, but a blocked event loop would still delay owner checks, LB sync, and
administrative responses. The two fixes are complementary: supervision keeps
one live child authoritative, while this change keeps that child responsive
during termination admission.

The client path compounds the ambiguity. Controller HTTP calls use a 10-second
read timeout, while the Skylet gRPC hop also defaults to 10 seconds and its
generic wrapper retries transient `UNAVAILABLE` failures. A caller can time out
or replay the destructive request while the controller is still waiting to
commit it.

This behavior is independent of accelerator compatibility. It applies to any
large SkyServe fleet and therefore belongs in a separate, reversible change.

## Behavior contract

1. Replica termination must not block the controller event loop while waiting
   for PostgreSQL, the replica-manager lock, or the durable teardown write.
2. The public payload and response contract remain unchanged. A successful
   request returns HTTP 200 only after termination has reached the existing
   durable acceptance boundary. The endpoint does not return HTTP 202.
3. The durable acceptance boundary is the existing owner-fenced transition
   that records teardown as scheduled and removes the replica from routing, or
   the existing owner-fenced completion of an immediate absence cleanup. The
   background down worker may complete later.
4. The current controller-owner fence, service-incarnation fence, replica
   manager lock, 400 validation, 404 missing-replica response, and 409 failed or
   already-terminating responses remain intact.
5. A destructive termination transport call is attempted once. A timeout,
   disconnect, or gRPC `UNAVAILABLE` result is ambiguous and must not be
   replayed automatically. The caller re-reads replica state before an explicit
   retry. Trying the next controller admin credential after an actual HTTP 401
   remains allowed because authentication rejected the request before the
   handler ran.
6. The HTTP and Skylet gRPC deadlines must cover a normal large-fleet lock wait.
   A termination-specific constant defines a 600-second acceptance budget,
   with 10 seconds of transport margin on the outer gRPC deadline. It does not
   couple termination behavior to the update-service timeout by name.
7. The change requires no database migration, protobuf change, public API
   version bump, or dashboard change.

## Design

### Controller execution

Keep request body parsing on the async route so malformed JSON and exact
`int`/`bool` validation retain the current FastAPI behavior. Move all remaining
synchronous work into one synchronous helper:

- read and validate the current replica row;
- preserve the current SHUTTING_DOWN and failed-status checks;
- call `ReplicaManager.scale_down()` with the existing lock and `purge` value;
- assemble the existing success response.

The route serializes termination admission with an `asyncio.Lock`, invokes that
helper through the running loop's default executor, and awaits it. The async
admission lock preserves the route's prior duplicate-request ordering without
blocking health or LB sync. Awaiting the executor work preserves
response-after-commit semantics while freeing the event loop to answer health,
owner, role, and LB sync requests.

Executor work cannot be cancelled after it starts. If its ASGI request task is
cancelled or disconnected, the route keeps the async admission lock until that
already-started operation reaches a durable outcome, then propagates the
cancellation. This prevents a cancelled request from exposing a stale READY
read to a duplicate termination while the first mutation is still in flight.

Do not introduce an in-memory work queue. An in-memory queue could acknowledge
a request that disappears when the controller child changes, and it would add
a second termination scheduler beside the durable replica state.

### Timeouts and replay

`serve_utils.terminate_replica()` supplies a method-specific controller timeout
of `(connect_timeout, TERMINATE_REPLICA_TIMEOUT_SECONDS)`. The new constant is
600 seconds. The shared controller helper already has a single transport
attempt in the exact base revision, and that invariant remains covered by
tests.

`SkyletClient.terminate_replica()` defaults its gRPC deadline to the termination
budget plus 10 seconds while preserving any explicit caller-provided deadline.
`RpcRunner.terminate_replica()` invokes the Skylet call with a single-attempt
retry budget. The generic Skylet helper gains an optional `max_attempts`
argument whose default remains five, preserving every other RPC.
`UNIMPLEMENTED` and `UNKNOWN` still produce the existing legacy fallback signal
on their first result.

Concurrent authenticated requests wait on the route's per-controller async
admission lock, so at most one termination helper occupies a default-executor
worker. The replica-manager lock remains authoritative across termination and
all other manager operations. If operational evidence shows that serial admin
admission is too restrictive, a dedicated executor with replica-keyed ordering
can be added separately with explicit lifecycle ownership.

### Concurrency and recovery

The async admission lock and manager lock continue to serialize two concurrent
terminations. The first accepted request durably schedules the down worker. A
later request then re-reads state, observes SHUTTING_DOWN and receives the
current 409 result, or observes the removed row and receives the current 404.

If ownership changes while executor work is waiting, the old manager cannot
persist through the existing service hash plus controller PID/IP fence. If the
owner changes after the scheduled state commits, the replacement controller
re-drives the durable teardown. The HTTP response is therefore never evidence
of provider deletion, only durable teardown acceptance, matching current
semantics.

## Alternatives rejected

### Increase only the client timeout

This avoids the common 10-second timeout but leaves the controller event loop
blocked and can still trigger child churn.

### Return HTTP 202 and terminate in a background task

This would change the success boundary and could lose accepted work on a child
restart unless a new durable operation queue and idempotency key were added.

### Bypass or shorten the replica-manager lock

The lock protects replica read-modify-write state. Bypassing it risks duplicate
workers, torn status, and owner-fence violations.

### Automatically retry timeouts or disconnects

Transport failure does not prove the handler failed before committing. Automatic
replay creates ambiguous destructive delivery semantics. Explicit state-read
then retry is safer and is already idempotent through SHUTTING_DOWN.

### Add operation tokens or make duplicate requests return 200

Deterministic response replay would require durable operation identity or a
tombstone. That is useful future work but not required to fix event-loop
liveness and should not expand this urgent patch.

## Implementation milestones

1. Extract the synchronous controller termination helper and execute it through
   the event loop executor.
2. Add the method-specific 600-second controller timeout.
3. Raise the terminate-replica Skylet deadline and make only that RPC
   single-attempt.
4. Add focused controller, HTTP transport, gRPC transport, and manager recovery
   tests.
5. Format, run focused suites, then run the relevant Serve unit-test shard.

## Test plan

Automated tests must cover:

- a blocked `scale_down()` while controller health remains responsive;
- no 200 response before the blocking manager call completes;
- concurrent duplicate requests are admitted serially and schedule one down
  worker;
- cancellation or disconnect does not release duplicate admission before the
  already-started operation completes;
- an owner-loss or persistence exception returns a non-success response;
- unchanged invalid JSON, exact type validation, missing replica, failed
  replica, and already-terminating results;
- one HTTP attempt with the explicit 600-second timeout;
- a 401 credential overlap retry without transport replay;
- one Skylet gRPC attempt on `UNAVAILABLE`, a default deadline of 610 seconds,
  preservation of explicit caller deadlines, and unchanged `UNIMPLEMENTED`
  fallback;
- a scheduled termination is re-driven once after controller recreation.

Focused commands:

```bash
pytest -q -n 0 tests/unit_tests/test_serve_controller_event_loop.py
pytest -q -n 0 tests/unit_tests/test_serve_terminate_validation.py
pytest -q -n 0 tests/unit_tests/test_serve_terminate_transport.py
pytest -q -n 0 tests/unit_tests/test_skylet_grpc_cancellable.py
pytest -q -n 0 tests/unit_tests/test_serve_utils.py
pytest -q -n 0 tests/unit_tests/test_serve_replica_managers.py
pytest -q -n 0 tests/unit_tests/test_serve_restart_bounded_drain_resume.py
pytest -q -n 0 tests/unit_tests/test_serve_graceful_drain.py
pytest -q -n 0 tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py
pytest -q -n 0 tests/unit_tests/test_serve_child_supervision.py
pytest -q -n 0 tests/unit_tests/test_serve_controller_respawn.py
bash format.sh --files \
  sky/serve/constants.py \
  sky/serve/controller.py \
  sky/serve/serve_utils.py \
  sky/serve/serve_rpc_utils.py \
  sky/backends/cloud_vm_ray_backend.py \
  sky/backends/skylet_rpc.py \
  tests/unit_tests/test_serve_controller_event_loop.py \
  tests/unit_tests/test_serve_terminate_validation.py \
  tests/unit_tests/test_serve_terminate_transport.py \
  tests/unit_tests/test_skylet_grpc_cancellable.py
```

Manual rollout verification:

1. On a staging fleet, hold or simulate replica-manager lock contention and
   request termination.
2. While termination waits, require controller health 200, current owner, fresh
   active and standby LB sync, and zero new request rejections.
3. Release contention and require one HTTP 200 plus one durable SHUTTING_DOWN
   transition.
4. Restart the controller child after the durable transition and verify one
   recovery re-drive without a second provider teardown worker.
5. Deploy the image with Helm `--reuse-values`, verify exact image digest and
   runtime commit, and observe controller child stability before attempting any
   production cleanup.

## Rollout and rollback

Release this as a separate SkyServe patch. The code is additive and works with
old clients. A new client transport path against an old controller only waits
longer; event-loop liveness improves when the new controller is active. Mixed
controller ownership remains protected by the existing owner tuple.

Rollback requires only restoring the previous image. No data rollback is
needed. During rollout, do not interpret HTTP 200 as provider termination; keep
the existing post-acceptance replica-state and provider-state checks.
