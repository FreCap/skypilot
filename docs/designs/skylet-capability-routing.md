# Skylet capability-negotiated transport routing

## Status

Milestone C1 is implemented after the first adversarial review returned
`RESHAPE` and the exact amended design at `4c4689e82c` passed a second review
with `PURSUE`. It adds the wire contract, worker-side advertisement, typed
client gateway, and strict parser without changing any existing transport
choice.

C2 through C4 remain design-only. A fresh review of those milestones returned
`RESHAPE` because channel incarnation fencing, retry ownership, downstream
typed-failure handling, and removal-ledger transitions were underspecified.
This revision closes those findings. Runtime implementation remains blocked
until an adversarial review passes against this exact amended commit.

This design compares SkyPilot `4fc716827c` with dstack `c9ebdaad6b`. The useful
dstack concept is one compatibility handshake before selecting a worker API.
SkyPilot needs a stricter contract because clusters, SSH tunnels, and Skylet
processes can all be replaced independently.

## Problem

SkyPilot currently spreads transport selection through core, Managed Jobs,
Serve, and `CloudVmRayBackend`. Twelve files inspect
`is_grpc_enabled_with_flag`, several methods catch broad gRPC failures and then
run an SSH implementation, and `CloudVmRayBackend.get_job_status()` owns both
transport policy and job-status semantics.

This creates three responsibility problems:

1. Every lifecycle consumer can make a different compatibility decision.
2. A timeout or unavailable endpoint can be mistaken for evidence that a
   method is unsupported.
3. Migrating one worker method does not create a reusable boundary for the
   next method.

The first target is ordinary `GetJobStatus`. On the worker wire, an empty
request means latest job and the current RPC returns an empty `job_statuses`
map when the worker has no jobs. The public backend contract returns
`{None: None}` for that case. Central routing must canonicalize this difference
before gRPC becomes authoritative. Capability advertisement attests only to
the worker-wire contract, not the future router canonicalization.

## Goals

- Advertise exact worker method contract versions through one typed service.
- Make one process-local router own negotiation, selection, invocation,
  fallback, and response canonicalization.
- Bind cached decisions to cluster incarnation, endpoint, tunnel generation,
  and the most recently observed Skylet boot identity, with an explicit bound
  on when a boot change can be discovered.
- Allow legacy routing only from local policy or explicit unsupported-method
  evidence.
- Route only ordinary `CloudVmRayBackend.get_job_status()` first.
- Preserve mixed-version compatibility during a bounded rollback window.
- Record every legacy symbol and temporary migration artifact that must be
  deleted after promotion.
- Reuse Datadog and structured logs. Add no telemetry database.

## Non-goals

- Do not route `get_job_status_with_system_recovery()` in the first migration.
- Do not combine Managed Jobs and Serve policy state machines.
- Do not infer capabilities from a SkyPilot or Skylet version string.
- Do not make capability state durable in PostgreSQL or cluster records.
- Do not change public job-status semantics.
- Do not enable a transport after an authentication, timeout, internal, or
  malformed-response failure.
- Do not migrate every Skylet RPC in one release.

## Responsibility model

| Owner | Retained responsibility |
| --- | --- |
| Skylet method implementation | Execute the worker-local operation and return its typed response. |
| Capabilities service | Advertise exact implemented semantic contracts and one boot identity. |
| Transport router | Negotiate, cache, select, invoke, classify errors, and canonicalize transport output. |
| `CloudVmRayBackend` | Provide a compatibility facade and the temporary narrow SSH callback. |
| Core, Managed Jobs, Serve | Consume job status and retain their domain-specific policy. |
| Datadog and structured logs | Observe shadow route decisions and unexplained divergence. |

The router never owns job recovery, retries above the transport boundary,
terminal-state policy, autoscaling, or Serve reconciliation.

## Wire contract

Add `sky/schemas/proto/skyletv1.proto`:

```proto
syntax = "proto3";

package skylet.v1;

service CapabilitiesService {
  rpc GetCapabilities(GetCapabilitiesRequest)
      returns (SkyletCapabilitiesV1);
}

message GetCapabilitiesRequest {}

message SkyletMethodCapabilityV1 {
  string service = 1;
  string method = 2;
  repeated uint32 contract_versions = 3;
}

message SkyletCapabilitiesV1 {
  uint32 schema_version = 1;
  string skylet_boot_id = 2;
  string skylet_version = 3;
  string skypilot_version = 4;
  string skypilot_commit = 5;
  repeated SkyletMethodCapabilityV1 methods = 6;
}
```

The first advertised tuple is:

```text
jobs.v1.JobsService / GetJobStatus / 1
```

Contract 1 has separate worker-wire and router obligations.

The advertised worker-wire contract means:

- explicit requested IDs are represented in the worker response;
- an unknown requested ID maps to `None`;
- an empty request means latest job;
- latest job with no jobs returns an empty `job_statuses` map;
- the base `job_statuses` map is authoritative;
- system-recovery maps are outside this first contract.

The future router contract canonicalizes the exact empty-request and
empty-response pair to `{None: None}`. The Skylet does not advertise or attest
to that router-only obligation.

The server derives protobuf service and method names from descriptors. Only
semantic contract versions are maintained manually. Advertisement is sorted
by `(service, method)` and contract versions are positive, unique, and sorted.

## Boot identity

`start_grpc_server()` creates one canonical lowercase UUID for each gRPC
server lifetime and passes it to `CapabilitiesServiceImpl`. Calls on the same
server return the same ID. A restarted Skylet returns a different ID.

The servicer stores an immutable serialized response and parses a fresh
response for each call. Diagnostic version and commit strings must never
affect routing.

## Client value contract

`SkyletClient.get_capabilities()` invokes the capability RPC with a
capability-specific unary call whose response deserializer returns the exact
raw payload. It rejects `len(payload) > 65536` before calling
`SkyletCapabilitiesV1.FromString()`. It never enforces this bound by
reserializing a decoded message, because protobuf decoding may discard or
coalesce wire representations such as duplicate singular fields. This is a
parser bound, not a transport-allocation bound: the shared channel continues
to have unlimited receive size.

The parser then produces frozen, slotted values with tuple fields. It rejects:

- schema versions other than 1;
- a noncanonical boot UUID;
- duplicate or unsorted methods;
- a service name longer than 255 ASCII characters, with fewer than two
  dot-separated identifiers, or with any identifier outside
  `[A-Za-z_][A-Za-z0-9_]{0,63}`;
- a method name outside `[A-Za-z_][A-Za-z0-9_]{0,127}`;
- an empty, duplicate, unsorted, or zero contract version list;
- more than 64 contract versions for one method;
- more than 256 advertised methods;
- an exact raw response larger than 64 KiB.

Future additive protobuf fields are allowed. Support requires exact membership
of `(jobs.v1.JobsService, GetJobStatus, 1)` and is never inferred.

## Channel and cache identity

Milestone C2 adds these frozen, slotted process-local values in
`sky/backends/skylet_transport.py`:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class SkyletChannelKeyV1:
    cluster_hash: str | None
    endpoint: str
    tunnel_generation: str


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletChannelSnapshotV1:
    channel: typing.Any
    key: SkyletChannelKeyV1


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletLogicalKeyV1:
    channel_key: SkyletChannelKeyV1
    skylet_boot_id: uuid.UUID
```

`SSHTunnelInfo` gains `generation: str`. Every tunnel created by new code gets
a canonical lowercase UUID and persists `(port, pid, generation)`. An old
`(port, pid)` value decodes to the bounded synthetic generation
`legacy:<pid>:<port>`. New code accepts only those two tuple shapes, a canonical
UUID in a triple, or the bounded legacy sentinel produced by pair decoding.
Malformed metadata fails closed and never supplies a cache key. Old control
planes remain able to read a triple because their existing readers consume
indices 0 and 1. New code remains able to read a pair written after rollback.
The pickle column is unchanged, so this needs neither a handle-version bump nor
a database migration.

Global state adds one read that returns `cluster_hash` and tunnel metadata from
the same cluster-table row. Every tunnel publish and clear uses a compare-and-
set writer fenced by cluster name, the observed `cluster_hash` when non-null,
and the complete observed prior tunnel metadata including generation. A
zero-row publish means the cluster incarnation or tunnel identity changed. The
caller terminates only its newly opened unpublished process and retries from a
new row snapshot. A stale close cannot clear a replacement tunnel.

`CloudVmRayResourceHandle.get_grpc_channel_with_snapshot()` constructs the
channel and key from the same local `SSHTunnelInfo` and same-row cluster hash on
every lock-free fast path, exclusive-lock recheck, shared-lock wakeup, and
newly opened path. `endpoint` is the exact target string used to create that
channel. The handle must not read cluster hash after constructing the channel.
The existing `get_grpc_channel()` remains a compatibility facade returning only
`.channel`.

The provisional cache key is `SkyletChannelKeyV1`. A successful advertisement
is stored with the full `SkyletLogicalKeyV1`. The cache contains capability
evidence only, never channels, stubs, handles, backend objects, or domain
status. A changed cluster hash, endpoint, or tunnel generation is a different
provisional key even if a port or PID is reused. A missing cluster hash permits
negotiation but forbids publication.

The cache lives only in `sky/backends/skylet_transport.py` and obeys all of the
following rules:

- a bounded LRU holds at most 1024 published provisional entries;
- each publication chooses one monotonic expiry with injected jitter from the
  closed 45 through 60 second interval;
- one in-flight capability load exists per provisional key, and no network I/O
  occurs while the mutex is held;
- exact capability-RPC `UNIMPLEMENTED` may publish a provisional-key absence
  sentinel because no boot ID is available;
- a valid advertisement, including method omission, publishes the complete
  advertisement under its logical key;
- transient, authentication, internal, malformed, and request-cancellation
  outcomes publish no negative evidence;
- `invalidate(channel_key)` removes the entry and any protocol-violation
  override for that provisional key;
- `force_refresh(snapshot, loader)` bypasses cached evidence and joins only an
  already-running forced refresh for the same provisional key;
- cancelling one waiter does not cancel the leader or other waiters;
- a cancelled leader publishes nothing, wakes waiters, and lets one
  uncancelled waiter become the next leader rather than replaying the leader's
  cancellation into unrelated requests;
- a non-cancellation leader failure wakes current waiters with the same typed
  failure but publishes no entry;
- `os.register_at_fork(after_in_child=...)` replaces entries, flights, mutexes,
  and waiter state in the child.

A tunnel change invalidates immediately because the next snapshot has a new
provisional key. A worker restart does not emit an independent cache
invalidation signal. A boot change is discovered only after expiry, a forced
refresh, or a method or transport failure that invalidates and refreshes the
entry. When a fresh handshake observes boot B, publishing B atomically removes
method-absence and protocol-violation decisions for boot A under the same
provisional key. Until one of those refresh triggers occurs, boot A evidence
may remain usable only through its existing TTL. The design makes no claim of
instant boot invalidation while a cached advertisement is healthy.

## Router API

Milestone C3 introduces:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class SkyletMethodContractV1:
    service: str
    method: str
    version: int

class SkyletRoute(enum.Enum):
    GRPC = 'grpc'
    LEGACY = 'legacy'

@dataclasses.dataclass(frozen=True, slots=True)
class SkyletRouteDecisionV1:
    route: SkyletRoute
    reason: str
    channel_key: SkyletChannelKeyV1 | None
    skylet_boot_id: uuid.UUID | None

class SkyletTransportRouter:
    def get_job_status(self, handle, job_ids, *, stream_logs,
                       legacy_runner): ...
```

The narrow `legacy_runner` executes only the old SSH status read. It does not
receive the backend as an authority object and remains explicitly temporary.
`CloudVmRayBackend.get_job_status()` becomes one router delegation after
authoritative promotion.

### Mode and shadow contract

The sole mode source is `SKYPILOT_SKYLET_ROUTING_MODE`, with exact values
`off`, `shadow`, and `authoritative_get_job_status`. Missing or empty means
`off`. Any other nonempty value raises an explicit configuration error. Mode
parsing lives in `skylet_transport.py`; it is not added to the boolean-only
`env_options.Options` contract.

In C3, `off` performs no capability call. `shadow` performs only capability
negotiation and never invokes the proposed `GetJobStatus`. A shadow timeout,
cancellation, malformed advertisement, logging failure, or unexpected
exception is logged with bounded fields and discarded. The characterized
current status path then runs exactly once and returns or raises exactly as it
does in `off`. Shadow comparison records the final actual route, including an
SSH fallback after initial gRPC selection, rather than the initial flag.

Shadow qualification measures added wall-clock latency as well as decision
divergence. A failed handshake that consumes the ordinary ten-second RPC
deadline on every cache miss is not non-interfering merely because the final
value matches. The test-cluster gate must prove cache-miss latency fits the
existing polling budget or commit a separately reviewed smaller shadow-only
deadline. Shadow adds no background queue, durable state, or telemetry store.

## Authoritative attempt and retry contract

C4 makes the router the sole retry owner for ordinary `GetJobStatus` in every
mode. The authoritative path must not call
`backend_utils.invoke_skylet_with_retries()`: that helper owns a nested retry
loop and classifies both `UNKNOWN` and `UNIMPLEMENTED` as method absence. Other
Skylet methods continue using it unchanged. During the rollback window, `off`
and `shadow` preserve the characterized selector and fallback in
`SkyletTransportRouter._get_job_status_compatibility`.

One top-level ordinary-status call has one loop of at most five route attempts,
one `common_utils.Backoff(initial_backoff=0.5)`, and at most four
`context_utils.sleep_with_cancellation()` calls. The budget includes capability
handshakes and method calls together. No cache loader, compatibility helper, or
method helper may start a nested attempt loop, and there is no terminal sleep.

Each route attempt takes these steps in order:

1. Check request-context cancellation.
2. Evaluate local policy. Local gRPC disabled or
   `provision_runtime_metadata.has_skylet is False` selects the temporary
   legacy adapter without opening a channel.
3. Obtain one atomic `SkyletChannelSnapshotV1`.
4. Resolve capability evidence for that snapshot's exact provisional key.
5. If the exact method contract is supported, invoke it once using the channel
   from that same snapshot. Cached evidence may be reused with a newly
   constructed channel only when the complete provisional key is identical.
6. Canonicalize the result or apply the exhaustive typed classification.

Only a retryable, non-connection-refused gRPC `UNAVAILABLE` from the capability
handshake or method call advances to another ordinary route attempt. Before
retrying, the router invalidates the exact failed key. The next iteration
always obtains a new atomic snapshot. A changed cluster hash, endpoint, or
tunnel generation renegotiates under the new key; an unchanged key still
forces a fresh handshake after invalidation. The backoff sleeps only when
another attempt remains.

The existing immediate connection-refused `UNAVAILABLE` case,
`DEADLINE_EXCEEDED`, non-context `CANCELLED`, `RESOURCE_EXHAUSTED`, and known
tunnel command, channel-ready, tunnel-lock, or startup-exhaustion failures are
typed unavailable after one attempt. Arbitrary implementation exceptions are
not relabeled as transport unavailability. Request-context cancellation stays
`asyncio.CancelledError`, dispatches no legacy call, and consumes no later
attempt.

### Post-advertisement `UNIMPLEMENTED`

An advertised method returning exact `UNIMPLEMENTED` consumes its current
route attempt and sets a one-shot forced-refresh state for the same top-level
call. If another attempt remains, the next iteration begins without backoff,
invalidates the old key, obtains a new atomic channel snapshot, and performs
the forced capability refresh. This uses the same five-attempt loop and never
creates a nested retry budget. If no attempt remains, the call fails with a
typed protocol error because no fresh compatibility evidence was established.

The forced-refresh iteration follows these rules:

1. A changed channel key discards all evidence from the old key.
2. A fresh capability RPC returning exact `UNIMPLEMENTED`, or a valid fresh
   advertisement omitting the method, selects the normal bounded legacy route
   for that fresh evidence.
3. If the same logical key still advertises the method, the first method
   `UNIMPLEMENTED` is a confirmed protocol violation. The method is not called
   again.
4. If a different boot advertises the method, invoke it once on the refreshed
   snapshot. A second `UNIMPLEMENTED` is a protocol violation for only that new
   logical key and does not trigger another refresh.
5. Unavailable, authentication, internal, cancellation, or malformed refresh
   evidence raises its typed result and never dispatches legacy.

A confirmed violation may publish a legacy override only for the violating
logical key and only through the 45 through 60 second cache TTL. It applies to
the current call and later calls during that TTL. A tunnel-key change misses
it, a newly observed boot removes it, and expiry requires a fresh handshake.
C5 converts this compatibility override into a typed protocol error before the
legacy adapter can be deleted.

## Error contract

Retain `SkyletUnavailableError` and `SkyletInternalError`. Add a small common
base plus `SkyletAuthenticationError`, `SkyletProtocolError`, and
`SkyletApplicationError`. No new type joins `SKYLET_GRPC_FALLBACK_ERRORS`, and
no raw `grpc.RpcError` escapes authoritative ordinary-status routing.

| Evidence | Route or typed result | Cache action | Legacy allowed |
| --- | --- | --- | --- |
| Local gRPC policy disabled or no Skylet runtime | local-policy legacy | no capability entry | yes during compatibility |
| Capability RPC exact `UNIMPLEMENTED` | capability-RPC-absent legacy | provisional absence, at most 60 seconds | yes |
| Valid advertisement omits the exact contract | method-absent legacy | boot-bound advertisement, at most 60 seconds | yes |
| Advertised method exact `UNIMPLEMENTED` | forced-refresh procedure above | invalidate, then optional boot-bound violation override | only after fresh absence or confirmed violation |
| Retryable non-connection-refused `UNAVAILABLE` | retry, then `SkyletUnavailableError` | invalidate before every retry | no |
| Connection-refused `UNAVAILABLE`, `DEADLINE_EXCEEDED`, non-context `CANCELLED`, `RESOURCE_EXHAUSTED`, or known channel failure | `SkyletUnavailableError` | invalidate when a key exists | no |
| Request-context cancellation | `asyncio.CancelledError` | no negative publication | no |
| `UNAUTHENTICATED` or `PERMISSION_DENIED` | `SkyletAuthenticationError` | invalidate | no |
| `INTERNAL` | `SkyletInternalError` | keep valid capability evidence | no |
| `UNKNOWN`, `DATA_LOSS`, `ALREADY_EXISTS`, `ABORTED`, `OUT_OF_RANGE`, malformed protobuf or response, or invalid enum | `SkyletProtocolError` | invalidate | no |
| `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, or `NOT_FOUND` | `SkyletApplicationError` | keep valid capability evidence | no |

For a successful response, explicit requested IDs must be represented exactly,
with unknown IDs mapped to `None`. For `job_ids is None`, an empty wire map
canonicalizes to `{None: None}` and a nonempty map contains exactly one latest
job entry. Unknown enums, missing or extra explicit keys, or multiple latest
entries are protocol errors. Public core continues returning `{}` before the
backend for an explicit empty list.

## Downstream typed-failure contract

The router owns transport retry and classification, not Jobs or Serve
lifecycle policy.

`sky.jobs.utils.get_job_status()` catches `SkyletUnavailableError` explicitly
and returns `(None, <bounded nonempty transient transport reason>)`, matching
its existing transient status-read outcome so current controller debounce and
cluster recheck policy remains authoritative. It does not convert
`asyncio.CancelledError`. Authentication, protocol, application, and internal
errors propagate unchanged rather than becoming a false no-job result.

`SkyServeReplicaManager._handle_job_status_results()` catches
`SkyletUnavailableError` around each future, records one bounded error, and
continues consuming later futures in the same round. It never calls
`_handle_preemption` for typed transport unavailability because transport
failure alone is not provider or SSH evidence of preemption. The existing
`CommandError` branch and its fresh-state recheck remain unchanged. Other typed
errors retain the existing round-level failure behavior and remain visible to
the outer fetcher loop. `ReplicaInfo.probe_pool()` already contains failures at
its item boundary and receives no transport-policy responsibility.

## Milestones

### C0: canonical design

- Commit this design before runtime implementation.
- Adversarially review the exact committed design.

### C1: additive wire, server, parser, and typed client

- Add and generate the dedicated capability proto.
- Bump `SKYLET_VERSION` from 42 to 43 so `attempt_skylet` restarts an
  already-running version-42 Skylet instead of silently leaving the new RPC
  unavailable.
- Add a per-server boot ID and immutable server advertisement.
- Add the typed `SkyletClient.get_capabilities()` gateway.
- Add strict immutable parsing and conformance tests.
- Make no routing or fallback change.
- Deploy to the test cluster and verify ordinary API/controller/executor
  health. Before that deployment, launch a disposable version-42 Kubernetes
  cluster from the old control plane. After deployment, run `attempt_skylet`,
  prove that it restarts to version 43, and call the capability RPC directly
  through its Skylet tunnel. Fresh-cluster qualification alone is
  insufficient.

### C2: channel binding and single-flight cache

- Add persisted tunnel generation with strict old-pair and new-triple decoding.
- Add the same-row cluster-hash and tunnel snapshot plus incarnation-fenced
  compare-and-set publication and clearing.
- Return channel plus tunnel snapshot atomically from every handle path.
- Add bounded single-flight capability caching with precise leader, waiter,
  fork, expiry, and honest boot-refresh behavior.
- Keep current transport choice unchanged.

### C3: shadow decision

- Add `off`, `shadow`, and `authoritative_get_job_status` modes.
- Parse only `SKYPILOT_SKYLET_ROUTING_MODE`, default missing or empty to `off`,
  and reject every other nonempty value.
- In shadow, negotiate only and execute the current path exactly once,
  containing negotiation, cancellation, parser, and logging failures.
- Compare proposed route with actual existing selection in structured logs and
  Datadog, including added cache-miss latency. Do not add a stats store and do
  not dual-read every status call.

### C4: authoritative ordinary job status

- Move SSH status execution into one temporary legacy adapter.
- Delegate `CloudVmRayBackend.get_job_status()` to the router and move the
  characterized off/shadow compatibility branch into that one owner.
- Use one five-attempt router loop with a new atomic snapshot per attempt,
  exact-key invalidation and renegotiation, no nested retry helper, and one
  bounded post-advertisement forced refresh.
- Canonicalize gRPC and SSH output, including no-job latest.
- Make only typed unavailability transient in Managed Jobs, and contain it per
  future in Serve without inferring preemption.
- Keep `get_job_status_with_system_recovery()` unchanged.
- Enable only on the test cluster until mixed-version and fault qualification
  passes.

### C5: compatibility closure

- Do not delete the legacy adapter or missing-capability route until an
  enforced support policy proves every reachable ordinary-status target both
  has a supported Skylet runtime and is permitted to use gRPC, or rejects it
  with an explicit upgrade-required error before routing. Offline, long-lived,
  local-policy-disabled, and `has_skylet=False` targets remain reachable until
  that policy rejects them. A version-43 floor alone is insufficient.
- Convert the protocol-violation legacy override into a typed protocol error
  before deleting the adapter.
- Use zero unexplained legacy dispatch in existing Datadog evidence only as
  supporting evidence, not as the deletion authority.
- Close rollback, delete temporary routing/shadow code, and then migrate other
  Skylet methods one at a time.

## Test plan

### C1 tests

- valid response round trip and exact support membership;
- future additive unknown field acceptance;
- schema, UUID, exact name grammar, ordering, duplicate, zero-version,
  per-method version count, method count, and raw pre-decode size rejection;
- an oversized wire payload that could shrink after protobuf decoding is
  rejected from its original bytes before `FromString` runs;
- stable boot ID for one servicer and different ID for a new servicer;
- descriptor-backed advertised method and registered handler;
- capability-specific raw unary invocation and timeout forwarding;
- old control plane plus new Skylet ignores the additive service;
- new control plane plus old Skylet receives exact gRPC `UNIMPLEMENTED`.

### C2 identity and cache tests

- Pair decoding yields exactly `legacy:<pid>:<port>`, canonical triples round
  trip, malformed shapes fail closed, old readers consume tuple fields 0 and
  1, and new readers accept a pair written after rollback.
- Every fast, exclusive-lock, shared-lock, and new-tunnel return proves channel
  endpoint and key came from the same tunnel object and same-row cluster hash.
- A same-name cluster recreation between open and publish causes a zero-row
  fenced write, terminates only the unpublished process, and leaves the
  replacement row unchanged. A stale close cannot clear a new generation.
- Independent cluster-hash, endpoint, and generation changes are cache misses,
  including endpoint and PID reuse with a new generation.
- N same-key callers make one load while key A never blocks key B. Failed and
  cancelled leaders publish nothing; a cancelled leader lets one peer become
  leader; a cancelled waiter does not affect the leader or other peers.
- TTL edges, 1024 to 1025 LRU eviction, missing-hash uncached behavior, and
  post-fork empty state are deterministic under injected clocks and jitter.
- A forced refresh observing boot B removes boot-A omission and violation
  decisions. A separate test records the intentional bound that healthy boot-A
  evidence remains usable until expiry without a refresh-triggering signal.
- Positive advertisement, exact capability-service absence, and boot-bound
  omission are the only cacheable evidence classes. Existing C1 parser, client,
  Python 3.10 import-floor, and tunnel multiprocess tests remain green.

### C3 shadow tests

- Missing and empty mode parse to off, all three exact values parse, and any
  other nonempty value fails configuration.
- Off performs no capability call. Every supported, absent, unavailable,
  cancelled, malformed, and logging-failure shadow case runs the current path
  exactly once with the identical value or exception and never invokes the
  proposed method.
- Comparison records final gRPC success or SSH fallback, uses only bounded
  fields and reason tokens, and excludes endpoint, RPC details, credentials,
  boot ID, and cluster hash from metric labels.
- A cache-miss shadow failure remains within the accepted measured polling
  latency budget.

### C4 retry, response, consumer, and ledger tests

- Retryable `UNAVAILABLE` succeeds on route attempts 2 and 5 and fails typed on
  attempt 5. The exhausted case obtains five snapshots and performs exactly
  four cancellation-aware sleeps.
- Cluster hash, endpoint, or generation changes between attempts force a new
  handshake and prevent old channel or decision reuse. An unchanged key after
  invalidation also forces a handshake.
- Handshake and method failures share the same five-attempt budget. No test can
  observe a 25-attempt nested product.
- Connection-refused `UNAVAILABLE`, `DEADLINE_EXCEEDED`, non-context
  `CANCELLED`, `RESOURCE_EXHAUSTED`, and known tunnel acquisition failures are
  one-attempt typed unavailable outcomes. Cancellation before an attempt,
  during RPC, and during backoff remains `asyncio.CancelledError`.
- Every gRPC status in the table proves exact type, cache action, attempt count,
  and zero unapproved legacy calls. No raw `grpc.RpcError` escapes.
- Same-key and same-boot method `UNIMPLEMENTED`, a changed boot, fresh service
  absence, fresh omission, refresh failure, violation expiry, tunnel change,
  and newly observed boot each exercise the one-shot forced-refresh contract.
- Latest no-job is exactly `{None: None}`. Latest with a job has one entry;
  unknown explicit IDs map to `None`; missing or extra keys, multiple latest
  entries, malformed responses, and invalid enums are protocol errors. Public
  explicit empty list remains `{}`.
- Managed Jobs converts only typed unavailable to a bounded transient tuple,
  propagates cancellation, and does not hide auth, protocol, application, or
  internal errors.
- A Serve round whose first future is typed unavailable still consumes a later
  failed-user-job future, and never invokes preemption handling for the
  unavailable replica. Existing `CommandError`, pool, lock-free polling, and
  preemption behavior remains green.
- Backend characterization proves one ordinary router delegation and no inline
  gRPC, fallback tuple, SSH codegen, result checking, or payload decode.
  `get_job_status_with_system_recovery()` never calls the router or adapter and
  retains both current branches.
- Manifest fixtures prove `PLA-M4-061` and `PLA-M4-062` resolve at every move,
  `PLA-M4-104` and `PLA-M4-105` cannot be present with null provenance, and
  `PLA-M4-106` is already present with historical provenance. Current-phase
  validation passes while final-phase validation remains blocked by every
  unsatisfied removal gate.

### Manual test plan

1. Before deploying C1, launch a disposable CPU-only Kubernetes cluster from
   the version-42 control plane and prove its running Skylet reports version
   42.
2. Deploy C1 with `--reuse-values`, keeping physical action authority false.
3. Prove all API, controller, and executor replicas report the merge SHA and
   remain Ready with zero restarts.
4. Exercise the ordinary control-plane path that calls
   `attempt_skylet`; prove it detects the version mismatch and restarts that
   same cluster's Skylet at version 43.
5. Open the existing Skylet tunnel and call `GetCapabilities` twice.
6. Verify schema 1, the exact `GetJobStatus` contract, stable boot ID, version
   diagnostics, and identical deterministic payloads.
7. Restart the disposable Skylet and verify a changed boot ID.
8. Tear the cluster down and prove no live resource remains.
9. Deploy C2's exact SHA with `--reuse-values` and action authority false.
   Repeated lookups on one v43 worker must retain generation and boot ID.
   Killing only the local tunnel must produce a new generation and cache miss
   while retaining the worker boot ID.
10. Roll back to the C1 image, prove it reads the persisted triple and calls the
    worker, then roll forward and prove C2 reads any pair written by C1.
11. Deploy C3 first in `off` and prove zero shadow events, then enable `shadow`
    only on the test cluster. Exercise v43 supported, v43 local-policy-disabled,
    v42 capability-absent, unreachable, tunnel-replaced, and worker-restarted
    cases. Query existing Datadog logs for bounded decision and cache-miss
    latency evidence.
12. Deploy C4a and prove v42, local-policy, v43 gRPC, and characterized broad
    fallback results are unchanged through the temporary adapter.
13. Deploy C4b first in `off`, then `shadow`, then
    `authoritative_get_job_status` only on the test cluster. Exercise latest,
    known ID, unknown ID, no jobs, tunnel stop, timeout, cancellation, auth,
    malformed response, invalid enum, advertised-method `UNIMPLEMENTED`,
    Managed Jobs, Serve, and pool probes.
14. Roll back to the immutable C3 or C4a image against v43 workers, then roll
    forward. At every step prove the exact image SHA, Ready replicas, zero
    unexpected restarts, clean logs, action authority false, and no disposable
    resource residue.

## Rollout and rollback

C1 is additive. Old clients ignore the new service, and existing routing is
untouched. Rollback is an ordinary control-plane image rollback; clusters
already running the new Skylet may continue advertising the additive method.

The compatibility matrix is explicit:

| Peer combination | Expected behavior |
| --- | --- |
| Old SDK or CLI with new API server | Unchanged; C1 does not bump the public API version. |
| New SDK or CLI with old API server | Unchanged; capability negotiation is server-side. |
| New control plane with old Skylet | The exact capability RPC returns `UNIMPLEMENTED`; existing routing remains unchanged in C1. |
| Old control plane with new Skylet | The control plane ignores the additive service. |

C2 is non-authoritative but changes persisted tunnel metadata. Its rollback
contract is bidirectional pair/triple readability, and every stale metadata
writer is incarnation-fenced. The worker protocol is unchanged, so C2 neither
bumps `SKYLET_VERSION` beyond 43 nor requires a worker restart.

C3 defaults off. Shadow enablement is test-cluster-only and requires both
decision-divergence and cache-miss latency evidence from existing Datadog
collection. No telemetry table or client dependency is added.

C4a is behavior-preserving but runtime-affecting because every legacy ordinary
status call crosses the adapter. C4b deploys off, then shadow, then
authoritative only on the test cluster while an immutable C3 or C4a rollback
image remains available. Promotion requires the full error matrix, retry and
re-key proof, typed Jobs and Serve behavior, unchanged system recovery, and
manifest consistency. No production-wide default changes in C2 through C4.

## Required legacy deletion after migration

At C4 promotion, replace the full ordinary status body in
`CloudVmRayBackend.get_job_status()`, including:

- its `is_grpc_enabled_with_flag` branch;
- inline request/stub/conversion logic;
- broad `SKYLET_GRPC_FALLBACK_ERRORS` catch;
- inline `JobLibCodeGen.get_job_status`, `run_on_head`, return-code and
  empty-output checks, and `load_statuses_payload`.

### Removal-ledger transition contract

C2 adds permanent router infrastructure and changes no removal row. Before C3
introduces runtime symbols, add planned `PLA-M4-104` with null provenance for
`SkyletRoutingMode.OFF`, `SkyletRoutingMode.SHADOW`, and the shadow comparator.
After the runtime commit exists, a follow-up manifest commit records its exact
40-character SHA and changes only `planned -> present` plus status history.

Before C4a introduces the adapter, add planned `PLA-M4-105` with null
provenance for `sky.backends.skylet_legacy.get_job_status_via_ssh`. Activate it
with the exact runtime SHA after that symbol exists. In the same runtime commit
that moves SSH execution, replace only `PLA-M4-061.locators` with:

```yaml
locators:
- kind: python_call_within
  path: sky/backends/skylet_legacy.py
  symbol: get_job_status_via_ssh
  call: command_runner
```

Keep row 061's historical `introduced_by`, `present` status, full history,
gates, evidence, and blocker unchanged. A backend locator disappearing is not
removal evidence.

`PLA-M4-106` is existing debt, not a planned future artifact. It is `present`
with `introduced_by: 069fa2a05dd978936eb65f9a9e85312bee254544`, the commit that
introduced the ordinary status codegen and payload-decoder pairing. Commit
`48d5fb7391a3fd85f27dbe042927c527407a42bf` later moved the codegen method to
its current module but did not create the debt. Row 106 locates only
`JobLibCodeGen.get_job_status` and `job_lib.load_statuses_payload`, depends on
`PLA-M4-105`, and retains the system-recovery status test as a reference. It
must not include `job_lib.get_statuses_payload`, which remains used by the
separate system-recovery compatibility path.

In the same C4b runtime commit that moves characterized compatibility fallback
into the router, replace only `PLA-M4-062.locators` with:

```yaml
locators:
- kind: python_attribute
  path: sky/backends/skylet_transport.py
  symbol: SkyletTransportRouter._get_job_status_compatibility
  attribute: SKYLET_GRPC_FALLBACK_ERRORS
```

Keep row 062's historical provenance, `present` status, full history, gates,
evidence, and blocker unchanged. The authoritative branch must not contain that
attribute. No runtime commit may leave either existing row with an unresolved
locator, and neither row advances merely because its responsibility moved.

After the enforced ordinary-status support gate closes, delete:

- the temporary legacy SSH adapter;
- the temporary shadow comparator and mode branch;
- temporary mixed-version shadow tests, while retaining a frozen permanent
  compatibility corpus;
- `JobLibCodeGen.get_job_status`;
- `job_lib.load_statuses_payload` when no remaining compatibility caller
  exists;
- the missing-capability and protocol-violation legacy routes only after every
  reachable target has supported Skylet and gRPC policy, or is rejected with
  an explicit upgrade-required error before routing.

Offline, long-lived, local-policy-disabled, and `has_skylet=False` targets all
block adapter deletion until that enforced policy upgrades or rejects them.
The protocol-violation override must first become a typed protocol error.
Datadog evidence is supporting evidence only; a v43 floor or a quiet telemetry
window cannot authorize deletion.

Only after every Skylet method migrates, delete:

- `SKYLET_GRPC_FALLBACK_ERRORS`;
- all remaining per-method `is_grpc_enabled_with_flag` branches in backend,
  core, Managed Jobs, and Serve;
- `CloudVmRayResourceHandle.is_grpc_enabled_with_flag`, its persisted field,
  and serialization compatibility.

`get_job_status_with_system_recovery()` and its legacy branches are explicitly
not removal targets of the ordinary-status milestone. Its use of
`job_lib.get_statuses_payload` requires a later obligation when that separate
compatibility path migrates.

## Alternatives rejected

### Version threshold negotiation

dstack uses this effectively for a smaller runner/shim contract, but one
binary version cannot represent SkyPilot's independently migrated methods.
Invalid and development versions must not fail open.

### Health RPC overload

Ping, health, and semantic capability negotiation have different failure and
cache contracts. A dedicated service keeps them separate.

### Fall back on any gRPC failure

This hides outages, authentication failures, server bugs, and malformed data
as compatibility. Only explicit unsupported evidence may choose legacy.

### Durable capability storage

Tunnel and boot identities are process-local and short-lived. PostgreSQL state
would become stale authority and add migration work without improving safety.

### One universal lifecycle state machine

The router should share transport actuation only. Managed Jobs and Serve keep
separate reducers because their recovery and scaling policies differ.
