# SkyServe Controller Transport Gateway

## Context

`sky/serve/serve_utils.py` is a 3,922-line facade that currently owns several
unrelated families:

- controller owner validation, URL routing, authentication, and HTTP retries;
- service lifecycle locks, validation, and recovery;
- status projection and wire compatibility;
- termination coordination and log streaming; and
- remote command generation.

The controller transport family is a low-state leaf. It reads one existing
owner snapshot, derives a fenced target, performs one bounded HTTP operation,
and returns the response. It does not own lifecycle locks, durable state,
process supervision, status payloads, cleanup ordering, or command generation.

## Responsibility map

### Controller transport and owner fencing

- Callers: service update/status/termination helpers in `serve_utils`, the
  placement API route, and supervised local-controller readiness in
  `service.py`.
- Dependencies: `serve_state` owner snapshots, Serve endpoint/header
  constants, admin-token rings, `requests`, IP normalization, hashing, and
  bounded retry timing.
- State owned: no durable or process lifecycle state; only per-call headers,
  token index, retry attempt, URL, and owner fingerprint.
- Failure modes: same-name successor routing, stale-owner requests,
  unauthenticated controller access, unbounded connection waits, incorrect
  IPv6 URLs, or replaying application failures.
- Performance sensitivity: invoked by status fanout and control writes; the
  extraction must add no wrapper, owner read, HTTP call, retry, token read,
  copy, lock, or allocation beyond the existing operation.
- Change cadence: HA routing, controller authentication, and transport policy.

### Lifecycle, validation, and recovery

- Callers: service apply/update/down, HA recovery, replica management, and
  managed-job pool recovery.
- Dependencies: file and database locks, lifecycle epochs, configuration,
  tasks, resources, workspaces, images, and process liveness.
- State owned: lifecycle epochs, lock handles, recovery snapshots, and cleanup
  ordering.
- Failure modes: split ownership, leaked children, stale cleanup, and invalid
  task acceptance.
- Performance sensitivity: lock hold time, recovery scan count, and process
  checks.
- Change cadence: lifecycle and recovery protocols.

### Status projection and wire compatibility

- Callers: CLI, server endpoints, dashboard, and controller polling.
- Dependencies: service and replica persistence, cluster handles, autoscaler
  responses, pickle/base64 compatibility, and legacy fallbacks.
- State owned: status dictionary shape and wire payloads.
- Failure modes: incompatible payloads, excessive reads, and incorrect counts.
- Performance sensitivity: query count, fleet scans, serialization, and
  payload size.
- Change cadence: API compatibility and presentation.

### Termination and log streaming

- Callers: CLI/SDK commands, recovery cleanup, and server handlers.
- Dependencies: lifecycle locks, request cancellation, cluster teardown,
  backends, log files, and owner snapshots.
- State owned: cancellation and teardown ordering plus active follow loops.
- Failure modes: deleting a replacement service, leaked replicas, blank logs,
  and unbounded workers.
- Performance sensitivity: bounded cancellation, teardown workers, and
  single-row log polling.
- Change cadence: cleanup durability and remote execution.

### Remote command generation

- Callers: Serve server handlers executing controller operations over SSH.
- Dependencies: shell quoting, consolidation configuration, user identity, and
  protocol-version gates.
- State owned: generated command text.
- Failure modes: unsafe quoting, old-controller incompatibility, and incorrect
  subprocess configuration.
- Performance sensitivity: remote execution dominates generation cost.
- Change cadence: remote command compatibility.

## Decision

Move the controller transport constants, owner type and error, fingerprint and
URL resolution, retry helpers, and placement-state request into
`sky/serve/controller_transport.py`.

Keep `sky.serve.serve_utils` as the stable facade with direct aliases for every
moved symbol currently available from that module. Restore the historical
`sky.serve.serve_utils` module identity on moved functions and the exception so
existing imports and pickle globals remain stable. Internal call sites may keep
using the facade. No caller migration is required for the structural change.

This is a plain module extraction. A class, protocol, strategy, registry,
factory, dependency injection layer, and wrapper facade add no value because
there is one transport policy and one implementation. Direct aliases avoid a
second forwarding call on status and control paths.

The gateway owns its private retry settings after the move. Shared request and
logging objects remain identical to the facade objects so existing behavior and
test seams are retained. `serve_state` stays lazily imported in the extracted
module because importing it eagerly creates a static import cycle through
service specification code; the facade already loads the same module for its
other responsibilities.

## Behavior contract

- Owner fingerprints retain their exact validation, normalization, JSON
  encoding, and SHA-256 output.
- Owner resolution performs one owner-row read per attempt and rejects missing,
  replaced, unroutable, or malformed owners before HTTP.
- Local and remote IPv4/IPv6 routing remains byte-for-byte identical.
- Controller-owner and authorization headers retain their exact precedence.
- Authentication rotates only after HTTP 401. Caller-supplied authorization
  remains authoritative.
- Connection and timeout failures alone are retried. Application responses are
  not replayed.
- Default timeout, retry count, backoff, logging level, and URL re-resolution
  remain unchanged.
- Placement-state validation and exception behavior remain unchanged.
- Existing `serve_utils` imports, callable identity, function module names, and
  exception pickle round trips remain valid.

## Alternatives considered

### Leave the gateway in `serve_utils`

This avoids one module and facade block, but leaves transport ownership mixed
with lifecycle, projection, teardown, logs, and code generation. The gateway
already has a cohesive dependency set and independent callers, so the extra
module has a clear owner rather than merely reducing line count.

### Extract only fingerprinting and URL normalization

This is smaller but leaves owner fencing split across modules and keeps retry,
authentication, and URL selection coupled to unrelated `serve_utils` code. It
would create a utility fragment rather than one owned gateway.

### Introduce a transport class or injectable client

There is no second implementation or construction variation. A class would add
lifecycle and dependency-injection surface without removing current state.

### Move callers to the new module immediately

Direct caller migration is unnecessary for responsibility ownership and would
expand compatibility risk. The stable facade allows later opt-in imports
without combining that cleanup with the structural extraction.

## Implementation milestones

1. Add and run characterization tests against the existing `serve_utils`
   implementation.
2. Move the gateway implementation without behavioral edits.
3. Add direct facade aliases and historical module identities.
4. Prove moved function ASTs, output fingerprints, facade identity, pickle
   behavior, owner-read and HTTP-call counts, and representative timing.
5. Run focused and component suites, formatting/static checks, and exact-head
   CI before merge.

## Changed-path-to-test matrix

| Changed path | Responsibility | Verification |
|---|---|---|
| `sky/serve/controller_transport.py` | owner fencing and HTTP gateway | new contract suite plus existing controller URL/retry tests |
| `sky/serve/serve_utils.py` | compatibility facade and internal callers | facade identity/pickle tests, full `test_serve_utils.py`, API compatibility tests |
| `tests/unit_tests/test_serve_controller_transport_contract.py` | characterization coverage | run directly before and after extraction |
| this design | canonical contract | formatting and diff checks |

## Performance evidence

Compare baseline and extracted versions with identical mocked owner snapshots
and HTTP responses. Record owner-row reads, token-ring reads, HTTP calls, and
request headers for representative GET, POST, local-owner, IPv4, and IPv6
cases. Benchmark repeated successful request construction with network I/O
mocked. The extraction is acceptable only with identical call counts and no
material timing regression.

## Rollout and rollback

This changes module ownership only. There is no schema, configuration, wire,
CLI, lifecycle, or deployment migration. Rollback is the single commit revert.
Merge only after all relevant checks succeed on the exact pushed SHA and no
actionable review thread remains.
