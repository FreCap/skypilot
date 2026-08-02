# Skylet capability-negotiated transport routing

## Status

Reshaped after adversarial review. Milestone C1 remains the first
implementation slice, but runtime work is blocked until the exact amended
design passes a second adversarial review. C1 adds the wire contract,
worker-side advertisement, typed client gateway, and strict parser without
changing any existing transport choice.

This design compares SkyPilot `fbf0c1bef3` with dstack `c9ebdaad6b`. The useful
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
  and Skylet boot identity.
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

Milestone C2 extends `SSHTunnelInfo` with a generation UUID. The existing
pickled metadata stores `(port, pid, generation)`. Old `(port, pid)` tuples are
read with the bounded synthetic generation `legacy:<pid>:<port>` so no durable
schema migration is needed.

The provisional lookup key is:

```text
(cluster_hash, endpoint, tunnel_generation)
```

The successful logical identity is:

```text
(cluster_hash, endpoint, tunnel_generation, skylet_boot_id)
```

The cache lives only in `sky/backends/skylet_transport.py`. It is not owned by
a backend instance, resource handle, gRPC stub, controller, or database row.

Cache rules:

- bounded LRU with at most 1024 entries;
- monotonic expiry with jitter from 45 through 60 seconds;
- one in-flight handshake per provisional key;
- no network I/O while holding the cache mutex;
- independently cancellable waiters;
- a failed or cancelled leader publishes no entry;
- forked child processes start with empty entries, locks, and flights;
- a missing cluster hash forces an uncached negotiation;
- transient, internal, authentication, and malformed outcomes never create a
  negative entry;
- exact capability-RPC `UNIMPLEMENTED` or a valid method omission may create a
  legacy decision until expiry.

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

## Error contract

| Evidence | Route or error | Negative cache | Legacy allowed |
| --- | --- | --- | --- |
| Local gRPC policy disabled or no Skylet runtime | local-policy legacy | no | yes |
| Capability RPC returns exact `UNIMPLEMENTED` | capability-RPC-absent legacy | at most 60 seconds | yes |
| Valid response omits the exact contract | method-absent legacy | boot-bound, at most 60 seconds | yes |
| Advertised method returns exact `UNIMPLEMENTED` after one forced renegotiation | protocol violation plus bounded legacy | boot-bound, at most 60 seconds | yes once |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, non-context `CANCELLED`, `RESOURCE_EXHAUSTED` | unavailable error | evict | no |
| Request-context cancellation | cancellation | no | no |
| `UNAUTHENTICATED`, `PERMISSION_DENIED` | authentication error | evict | no |
| `INTERNAL` | internal error | no | no |
| `UNKNOWN`, `DATA_LOSS`, malformed response, invalid enum | protocol error | evict | no |
| Application `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `NOT_FOUND` | typed application error | no | no |

`UNKNOWN` must no longer share method-absence classification with
`UNIMPLEMENTED`. New router errors must not join
`SKYLET_GRPC_FALLBACK_ERRORS`.

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

- Add persisted tunnel generation with old-tuple decoding.
- Return channel plus tunnel snapshot atomically from the handle.
- Add bounded boot-bound single-flight caching.
- Keep current transport choice unchanged.

### C3: shadow decision

- Add `off`, `shadow`, and `authoritative_get_job_status` modes.
- Default to `off` outside the test rollout.
- In shadow, negotiate only and execute the current path unchanged.
- Compare proposed route with actual existing selection in structured logs and
  Datadog. Do not add a stats store and do not dual-read every status call.

### C4: authoritative ordinary job status

- Move SSH status execution into one temporary legacy adapter.
- Delegate `CloudVmRayBackend.get_job_status()` to the router.
- Canonicalize gRPC and SSH output, including no-job latest.
- Keep `get_job_status_with_system_recovery()` unchanged.
- Enable only on the test cluster until mixed-version and fault qualification
  passes.

### C5: compatibility closure

- Do not delete the legacy adapter or missing-capability route until an
  enforced support policy proves every reachable cluster is upgraded to
  `SKYLET_VERSION >= 43`, or rejects an older Skylet with an explicit upgrade
  error. Offline and long-lived clusters count as reachable until that policy
  rejects them.
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

### Later router tests

- N-thread single flight, failed leader, waiter cancellation, expiry, LRU,
  fork reset, tunnel replacement, changed boot ID, and cluster recreation;
- every gRPC status in the error table;
- requested missing IDs, all enums, latest job, and no-job latest;
- one capability call per cache interval, no SSH after gRPC success, and no
  fallback on non-`UNIMPLEMENTED` errors;
- `get_job_status_with_system_recovery()` remains byte-for-byte routed through
  its current path.

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

C2 is also non-authoritative. C3 defaults off. C4 promotion requires the test
cluster gate, explicit error-matrix qualification, and a compatible rollback
image. No production-wide default changes in these milestones.

## Required legacy deletion after migration

At C4 promotion, replace the full ordinary status body in
`CloudVmRayBackend.get_job_status()`, including:

- its `is_grpc_enabled_with_flag` branch;
- inline request/stub/conversion logic;
- broad `SKYLET_GRPC_FALLBACK_ERRORS` catch;
- inline `JobLibCodeGen.get_job_status`, `run_on_head`, return-code and
  empty-output checks, and `load_statuses_payload`.

Before moving SSH code, add a removal-manifest row locating every operation in
the temporary adapter. A move must not make existing `PLA-M4-061` or
`PLA-M4-062` appear complete while the responsibility still exists.

After the enforced minimum-Skylet support gate closes, delete:

- the temporary legacy SSH adapter;
- the temporary shadow comparator and mode branch;
- temporary mixed-version shadow tests, while retaining a frozen permanent
  compatibility corpus;
- `JobLibCodeGen.get_job_status`;
- `job_lib.get_statuses_payload` when no remaining caller exists;
- `job_lib.load_statuses_payload` when no remaining compatibility caller
  exists;
- the missing-capability negative route only after every reachable older
  Skylet is upgraded or rejected with an explicit upgrade error.

Only after every Skylet method migrates, delete:

- `SKYLET_GRPC_FALLBACK_ERRORS`;
- all remaining per-method `is_grpc_enabled_with_flag` branches in backend,
  core, Managed Jobs, and Serve;
- `CloudVmRayResourceHandle.is_grpc_enabled_with_flag`, its persisted field,
  and serialization compatibility.

`get_job_status_with_system_recovery()` and its legacy branches are explicitly
not removal targets of the ordinary-status milestone.

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
