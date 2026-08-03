# Skylet capability-negotiated transport routing

## Status

Milestone C1 is implemented after the first adversarial review returned
`RESHAPE` and the exact amended design at `4c4689e82c` passed a second review
with `PURSUE`. It adds the wire contract, worker-side advertisement, typed
client gateway, and strict parser without changing any existing transport
choice.

C2 through C4 remain design-only. Their first focused review returned
`RESHAPE` because channel incarnation fencing, retry ownership, downstream
typed-failure handling, and removal-ledger transitions were underspecified.
Commit `650935ca40` closed those findings. Review of that exact commit found
four more blockers, which commit `dcd6262c01` addressed: nullable cluster
incarnation, forced-refresh attempt accounting, objective shadow promotion
evidence, and the exact legacy-adapter transcript. Review of `dcd6262c01` then
found four sequencing gaps: null-to-null row recreation remained an ABA hole,
C3 depended on a C4a adapter that did not yet exist, forced-refresh failures
had contradictory transitions, and split API pods could not generate genuine
request-body events. This revision closes those gaps. Runtime implementation
remains blocked until an adversarial review passes against this exact amended
commit.

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
- Allow legacy routing only from local policy, explicit unsupported-method
  evidence, or the bounded unfenced-null-incarnation safety route.
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
class SkyletCapabilityChannelSnapshotV1:
    channel: typing.Any
    key: SkyletChannelKeyV1
    publishable: bool


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletLogicalKeyV1:
    channel_key: SkyletChannelKeyV1
    skylet_boot_id: uuid.UUID


class TunnelMutationResult(enum.Enum):
    UPDATED = 'updated'
    CONFLICT = 'conflict'
    UNFENCED_CLUSTER_INCARNATION = 'unfenced_cluster_incarnation'
```

`SSHTunnelInfo` gains `generation: str`. Every tunnel created by new code gets
a canonical lowercase UUID and persists `(port, pid, generation)`. An old
`(port, pid)` value decodes to the bounded synthetic generation
`legacy:<pid>:<port>`. Both shapes require exact integers, excluding booleans,
with `1 <= port <= 65535` and `1 <= pid <= 2**31 - 1`. A triple additionally
requires a canonical lowercase UUID generation. New code accepts only those
two tuple shapes, or the bounded legacy sentinel produced internally by valid
pair decoding. Malformed persisted metadata fails closed and never supplies a
channel, cache key, open, replacement, normal clear, or process signal, even
when the row has a non-null cluster hash. Recovery requires an explicit
authoritative repair that compares the exact non-null cluster hash and exact
raw malformed bytes before clearing; automatic tunnel acquisition never
guesses. Old control planes remain able to read a triple because their existing
readers consume indices 0 and 1. New code remains able to read a valid bounded
pair written after rollback. The pickle column is unchanged, so this needs
neither a handle-version bump nor a database migration.

Global state adds one read that returns `cluster_hash` and tunnel metadata from
the same cluster-table row. A non-null `cluster_hash` is the only cluster-row
incarnation fence. For such a row, every tunnel publish and clear uses one
compare-and-set update fenced by cluster name, equality with the exact observed
hash, and the complete observed prior tunnel metadata including generation.
The metadata predicate uses SQL `IS NULL` for a null prior value and equality
for a non-null serialized value.

The non-null-hash update must affect exactly one row. A zero-row result means
the cluster incarnation or tunnel identity changed. The shared global-state
compare-and-set API returns only `TunnelMutationResult.UPDATED` for one row,
`CONFLICT` for zero rows, or `UNFENCED_CLUSTER_INCARNATION` under the null rule
below; more than one row is an invariant failure. On publish, the caller first
requires a non-null snapshot, then opens the process, then compares and sets.
`CONFLICT` terminates only that newly opened unpublished process and retries
from a new same-row snapshot. On clear, compare-and-set to null occurs before
any signal. Only `UPDATED` permits signalling the exact observed process;
`CONFLICT` leaves replacement metadata and its process untouched. Neither
predicate may be weakened to preserve availability.

An observed null `cluster_hash` is not a fence. SQL `IS NULL` cannot distinguish
delete and reinsert when both incarnations have null hashes, even if the tunnel
metadata is byte-identical. New C2 code therefore applies this exact fail-closed
contract before any mutation or process operation:

1. The open-and-publish path returns
   `TunnelMutationResult.UNFENCED_CLUSTER_INCARNATION` before calling
   `open_ssh_tunnel`, issuing SQL, or registering a process.
2. Tunnel clear returns
   `TunnelMutationResult.UNFENCED_CLUSTER_INCARNATION` without issuing SQL or
   sending any signal, even when the observed PID and generation still look
   live.
3. The existing channel-only `get_grpc_channel()` compatibility facade may
   reuse a well-formed, healthy persisted tunnel. It exposes no cache identity,
   performs no tunnel recovery, and preserves the current transport choice for
   every pre-C4 Skylet RPC. Missing, malformed, or unhealthy metadata returns
   typed tunnel unavailability before opening a process.
4. Only `get_capability_channel_with_snapshot()` may expose the null-hash key.
   It returns `SkyletCapabilityChannelSnapshotV1(publishable=False)` over that
   same healthy persisted tunnel. The C3 decision-only proposal is its sole
   caller and invokes only `GetCapabilities`; cache publication rejects this
   snapshot. Generic method-routing code does not accept this snapshot type.
5. In C4 authoritative mode, a null-hash same-row observation selects the
   explicitly temporary legacy adapter with reason
   `unfenced_cluster_incarnation`. It never invokes advertised `GetJobStatus`
   from unfenced evidence. This route remains until the row gains an
   authoritative non-null hash or is rejected by upgrade policy.

The read-only null-hash handshake is deliberately not incarnation-fenced. It
is useful for compatibility observation because it cannot mutate the row,
cache evidence, or signal a process, and the C3 proposal invokes no status
method. The separately executed current inline body remains authoritative
until C4. No part of this design claims that two null-hash rows represent the
same cluster incarnation.

For non-null rows,
`CloudVmRayResourceHandle.get_grpc_channel_with_snapshot()` constructs the
channel and key from the same local `SSHTunnelInfo` and same-row cluster hash on
every lock-free fast path, exclusive-lock recheck, shared-lock wakeup, and newly
opened path. `endpoint` is the exact target string used to create that channel.
The handle must not read cluster hash after constructing the channel. This
generic snapshot API never returns a null-hash key. The existing
`get_grpc_channel()` remains a channel-only compatibility facade and, for a
null row, may take only the healthy persisted-tunnel path. The capability-only
snapshot API may take that same path with `publishable=False`. Neither null path
enters exclusive open, publish, replacement, or clear code.

The provisional cache key is `SkyletChannelKeyV1`. A successful advertisement
is stored with the full `SkyletLogicalKeyV1`. The cache contains capability
evidence only, never channels, stubs, handles, backend objects, or domain
status. A changed non-null cluster hash, endpoint, or tunnel generation is a
different provisional key even if a port or PID is reused. A missing cluster
hash permits only the read-only C3 negotiation above and forbids publication,
tunnel recovery, and authoritative method invocation; C4 uses the temporary
legacy route instead.

The negative evidence type is exactly `SkyletCapabilityRpcAbsentV1`, not a
boolean or generic failure sentinel. The C3 classifier
`_classify_get_capabilities_failure()` is its sole factory. It returns this
value only when the RPC path is exactly
`skylet.v1.CapabilitiesService/GetCapabilities` and gRPC status is exactly
`UNIMPLEMENTED`; every other status raises its typed result. Cache publication
accepts the typed value only with a matching
`SkyletCapabilityChannelSnapshotV1(publishable=True)` and non-null channel key.
An `UNIMPLEMENTED` from `JobsService/GetJobStatus`, an `UNKNOWN`, a fabricated
value, or any null-hash capability snapshot cannot publish absence.

The cache lives only in `sky/backends/skylet_transport.py` and obeys all of the
following rules:

- a bounded LRU holds at most 1024 published provisional entries;
- each publication chooses one monotonic expiry with injected jitter from the
  closed 45 through 60 second interval;
- at most one network-bearing capability flight, ordinary or forced, exists per
  provisional key, and no network I/O occurs while the mutex is held;
- the exact C3-classified capability-RPC `UNIMPLEMENTED` may publish
  `SkyletCapabilityRpcAbsentV1` under the provisional key because no boot ID is
  available;
- a valid advertisement, including method omission, publishes the complete
  advertisement under its logical key;
- transient, authentication, internal, malformed, and request-cancellation
  outcomes publish no negative evidence;
- `invalidate(channel_key)` removes the entry for that provisional key;
- a normal caller joins either an ordinary or forced flight for the same key
  and may consume its result;
- `force_refresh(snapshot, loader)` bypasses cached evidence. It joins an
  already-running forced flight. If an ordinary flight is running, the forced
  caller treats it only as a barrier: it waits for completion, discards that
  result, rechecks cancellation and key state, and then starts or joins the
  subsequent forced flight. Ordinary and forced network loads never overlap for
  one key;
- cancelling one waiter does not cancel the leader or other waiters;
- synchronous waiters call condition wait with an injected maximum 50 ms
  quantum, then check request-context cancellation and deadline after every
  wake or timeout. A cancelled waiter removes only itself and raises the exact
  request cancellation; it neither busy-spins nor cancels, poisons, or replaces
  the flight. The injected clock and wait quantum make this deterministic in
  tests;
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
method-absence evidence for boot A under the same provisional key. Until one
of those refresh triggers occurs, boot A evidence may remain usable only
through its existing TTL. The design makes no claim of
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
class SkyletCapabilityRpcAbsentV1:
    channel_key: SkyletChannelKeyV1

@dataclasses.dataclass(frozen=True, slots=True)
class SkyletRouteDecisionV1:
    route: SkyletRoute | None
    reason: str
    channel_key: SkyletChannelKeyV1 | None
    skylet_boot_id: uuid.UUID | None
    cache_status: str
    boot_observed: bool
    latency_ms: int

class GetJobStatusShadowObservation:
    def mark_actual_route(self, route: SkyletRoute) -> None: ...
    def __enter__(self) -> 'GetJobStatusShadowObservation': ...
    def __exit__(self, exc_type, exc, traceback) -> bool: ...

class SkyletTransportRouter:
    def propose_get_job_status_route(
        self,
        handle,
    ) -> SkyletRouteDecisionV1: ...

    def observe_current_get_job_status(
        self,
        proposal: SkyletRouteDecisionV1 | None,
    ) -> 'GetJobStatusShadowObservation': ...
```

`propose_get_job_status_route()` is decision-only. It evaluates local policy,
obtains at most one `SkyletCapabilityChannelSnapshotV1` through
`get_capability_channel_with_snapshot()`, negotiates capabilities, and returns
one bounded proposal. It is the only API allowed to consume a
`publishable=False` null-hash snapshot. It never accepts `job_ids`,
`stream_logs`, a command runner, or a status callback, and it never invokes
either `GetJobStatus` implementation.
Expected negotiation failures, request cancellation during the proposal, and
unexpected ordinary exceptions become a `route=None` proposal with the exact
closed reason, so none can replace the current status result. Process-fatal
`KeyboardInterrupt` and `SystemExit` are not swallowed.
`observe_current_get_job_status()` likewise converts any ordinary setup failure
to the no-op observer before the current body starts.

C3 contains observation around the current inline
`CloudVmRayBackend.get_job_status()` body. In `off`, it constructs only a no-op
observer and performs no router call or emission. In `shadow`, it calls the
decision-only proposal exactly once and then enters the object returned by
`observe_current_get_job_status()`. The existing body remains inline and is
executed exactly once. The only statements inserted into its established
branches are:

1. mark actual route `grpc` immediately before the current gRPC `try` body;
2. retain the existing broad fallback catch and debug log unchanged;
3. overwrite actual route with `legacy` immediately before the current
   `JobLibCodeGen.get_job_status()` statement; and
4. let the observer's `__exit__` emit once after the body's return or exception.

Thus successful gRPC records `grpc`; a caught gRPC failure followed by SSH
records final route `legacy`; an uncaught gRPC error records `grpc`; and any
SSH return or error records `legacy`. The observer never reads or changes the
status value. Its `__exit__` classifies only success, error, or cancellation,
swallows its own event-building and logging failures, always returns false,
and therefore preserves the exact returned object or exact raised exception.
No callback, copied body, second status execution, or legacy adapter exists in
C3.

C4a introduces only the adapter below and replaces the inline SSH transcript
with one adapter call at the already-marked legacy branch. The C3 proposal and
observer remain in place. C4b adds
`SkyletTransportRouter.get_job_status(handle, job_ids, *, stream_logs,
legacy_runner)`, deletes the public decision-only proposal and backend observer
integration, and moves their decision and final-route recording into private
router helpers around `_get_job_status_compatibility`. At C5, the shadow mode,
observer, emitter, and comparison are deleted; the authoritative private route
decision remains. The narrow `legacy_runner` introduced in C4a never receives
the backend or handle as an authority object.

### C4a legacy adapter transcript

`sky/backends/skylet_legacy.py` defines the exact temporary boundary:

```python
class LegacyStatusCommandRunner(typing.Protocol):
    def __call__(
        self,
        code: str,
        *,
        stream_logs: bool,
        require_outputs: bool,
        separate_stderr: bool,
    ) -> tuple[int, str, str]: ...


def get_job_status_via_ssh(
    job_ids: list[int] | None,
    *,
    stream_logs: bool,
    command_runner: LegacyStatusCommandRunner,
) -> dict[int | None, job_lib.JobStatus | None]: ...
```

The backend supplies one callback with backend and handle already bound. The
adapter executes this transcript exactly once and in this order:

1. `code = job_lib.JobLibCodeGen.get_job_status(job_ids)`.
2. Call `command_runner(code, stream_logs=stream_logs,
   require_outputs=True, separate_stderr=True)` and unpack exactly
   `(returncode, stdout, stderr)`.
3. Call `subprocess_utils.handle_returncode(returncode, code,
   'Failed to get job status.', stderr)` with those exact values.
4. If `stdout` is empty after a zero return code, raise
   `exceptions.CommandFailureException(command=code,
   failure='produced no output', error_msg='Failed to get job status.',
   detailed_reason=f'stderr="{stderr}"')`.
5. Return `job_lib.load_statuses_payload(stdout)` unchanged.

The adapter does not catch decoder failures, retry, negotiate capabilities,
canonicalize protobuf values, choose a route, inspect the backend or handle,
or make a second command call. `stream_logs=False` and `stream_logs=True` are
forwarded unchanged. Nonzero return codes and malformed payloads propagate the
same exception types and messages as the current inline body. For
`job_ids=None`, the unchanged generated code asks the runtime for its latest
job; its no-job payload decodes to `{None: None}`.

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

Mode pins null-hash behavior. C3 `shadow` may use the capability-specific
snapshot for one uncached proposal and then executes the unchanged current body
once. Its `proposed_route` reflects the uncached advertisement when one is
obtained, but its reason is always `unfenced_cluster_incarnation` and
`cache_status` is always `uncached`; without a usable tunnel the proposed route
is `none`. C4 `authoritative_get_job_status` never turns that proposal into
method authority: it selects the temporary legacy adapter with
`unfenced_cluster_incarnation`. C5 cannot remove that route or adapter until an
enforced upgrade policy supplies a non-null incarnation or rejects the target
before routing.

### Datadog shadow event and promotion gate

C3 emits one unsampled structured log after every shadow proposal and actual
status completion on the test cluster. Existing Datadog log collection is the
only sink. The event name is `skylet_transport_shadow_v1`; schema version is
integer `1`; and the event has exactly these application fields:

| Field | Type and allowed values |
| --- | --- |
| `event_name` | literal `skylet_transport_shadow_v1` |
| `schema_version` | integer `1` |
| `image_sha` | exact 40-character control-plane commit |
| `process_role` | `api`, `controller`, or `executor` |
| `mode` | literal `shadow` |
| `method_contract` | literal `jobs.v1.JobsService/GetJobStatus/1` |
| `proposed_route` | `grpc`, `legacy`, or `none` |
| `actual_route` | `grpc`, `legacy`, or `none` |
| `reason` | one value from the closed enum below |
| `comparison` | `match`, `expected_compatibility_mismatch`, `unexplained_mismatch`, or `not_comparable` |
| `outcome` | `success`, `error`, or `cancelled` |
| `cache_status` | `hit`, `miss`, `uncached`, `not_applicable`, or `error` |
| `boot_observed` | boolean |
| `shadow_latency_ms` | nonnegative integer measured only around proposal negotiation and containment |

The closed `reason` enum is:

```text
supported
local_policy_disabled
no_skylet_runtime
unfenced_cluster_incarnation
capability_rpc_absent
method_absent
capability_unavailable
capability_deadline
capability_cancelled
capability_resource_exhausted
capability_auth
capability_internal
capability_protocol
capability_malformed
shadow_internal_error
```

No endpoint, RPC detail, credential, cluster hash, boot ID, job ID, exception
string, or provider payload is emitted. `process_role` is read only from the
process-owned `SKYPILOT_API_SERVER_ROLE` environment variable and is never
accepted from a request body, handle, or caller argument. Shadow enablement on
the test cluster requires the explicit split topology, so `all` and every
unknown role fail event construction. `api` remains a schema-valid value for
forward compatibility, but an explicit API pod does not execute request
bodies and therefore must not emit this event during split-topology
qualification. `expected_compatibility_mismatch` is
allowed only when `reason=capability_rpc_absent`, the proposal is legacy, and
the characterized current path succeeds through gRPC. `not_comparable` is
allowed only when either route is `none` or the actual outcome is error or
cancelled. Equal non-none routes are `match`; every other successful unequal
route is `unexplained_mismatch`.

The Platform SkyPilot on-call is the promotion owner. Before the measurement
window, the exact rendered test-cluster manifests must prove separate `api`,
`controller`, and `executor` Deployments with matching
`SKYPILOT_API_SERVER_ROLE` values. A direct role test must prove
`sky.server.requests.executor._request_execution_enabled()` is false in an
explicit API process, that no request worker starts there, and that the same
ordinary requests are claimed only by executor or controller workers. This is
the evidence for excluding API from required event coverage; synthetic
API-role events are forbidden.

After a five-minute warmup on the exact C3 image, the owner records Datadog
query permalinks for one continuous 30-minute window. The read-only
qualification fixture drives at least 100 ordinary status executions through
queued executor request bodies and at least 100 through the real controller
polling path. It obtains at least 20 genuine cache misses for each required
role by rotating the disposable target's tunnel through 20 distinct persisted
generations under one non-null cluster hash and executing both role paths once
per generation. It exposes no event-only endpoint, process-role override, or
direct emitter hook. The required role set is exactly `{controller, executor}`;
each required role contributes at least 100 events and at least 20
`cache_status:miss` events. The same submitted-traffic interval must contain
zero `process_role:api` shadow events, corroborating that API pods scheduled but
did not execute the request bodies.

The base query is:

```text
service:skypilot @event_name:skylet_transport_shadow_v1
@schema_version:1 @image_sha:<exact-c3-sha> @mode:shadow
```

Promotion passes only when all four conditions hold in that exact window:

1. The rendered-manifest, direct role test, request-claim evidence, and zero
   API-event query prove the split topology described above.
2. The base query meets the event and cache-miss counts for both controller and
   executor.
3. Datadog `p95(@shadow_latency_ms)` grouped by `@process_role` is at most
   500 milliseconds for controller and executor separately.
4. The base query plus `@comparison:unexplained_mismatch` returns exactly zero
   events across every emitted role, including any schema-valid API event.

Any missing event field, unknown enum value, missing required role, unexpected
API execution event, threshold breach, or unexplained divergence is a failed
gate. Expected compatibility mismatches
remain visible and counted but do not satisfy or bypass another condition. A
failed handshake that consumes the ordinary ten-second deadline will therefore
fail the latency gate if it materially affects the window. Changing the
percentile, threshold, window, role counts, reason enum, or comparison rules
requires another committed design review. Shadow adds no background queue,
database, custom Datadog client, or Prometheus metric.

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
3. Read cluster hash and tunnel metadata from one row. A null hash selects the
   temporary legacy adapter with reason `unfenced_cluster_incarnation` before
   exposing a cache key or invoking a capability or status RPC.
4. Obtain one atomic non-null `SkyletChannelSnapshotV1`.
5. Resolve capability evidence for that snapshot's exact provisional key.
6. If the exact method contract is supported, invoke it once using the channel
   from that same snapshot. Cached evidence may be reused with a newly
   constructed channel only when the complete provisional key is identical.
7. Canonicalize the result or apply the exhaustive typed classification.

Exactly two outcomes can advance the loop: retryable gRPC `UNAVAILABLE`, and
the one allowed post-advertisement `UNIMPLEMENTED` forced-refresh transition.
Every other outcome returns or raises from its current attempt. The transition
table is exhaustive:

| Current outcome | Attempt consumed | Key action | Sleep | Next state |
| --- | --- | --- | --- | --- |
| Success or approved legacy decision | one | retain evidence as specified | zero | return |
| Retryable non-connection-refused `UNAVAILABLE`, attempts 1 through 4, including a capability or method call during forced refresh | one | invalidate the failed provisional key; retain `refresh_required` only when fresh evidence has not yet been obtained | one cancellation-aware backoff | next forced-refresh attempt when refresh remains required, otherwise next normal attempt |
| Retryable non-connection-refused `UNAVAILABLE`, attempt 5 | one | invalidate the failed provisional key | zero | raise `SkyletUnavailableError` |
| First advertised-method `UNIMPLEMENTED`, attempts 1 through 4 | one | invalidate, consume the top-level one-shot transition, and set `refresh_required` | zero | next forced-refresh attempt |
| First advertised-method `UNIMPLEMENTED`, attempt 5 | one | invalidate | zero | raise `SkyletProtocolError` because refresh proof cannot fit the budget |
| Any advertised-method `UNIMPLEMENTED` after the one-shot transition was consumed | one | invalidate; publish no negative or override | zero | raise `SkyletProtocolError` with no refresh or legacy dispatch |
| Authoritative same-row observation has null `cluster_hash` | one | no cache or tunnel mutation | zero | return the temporary legacy adapter result with reason `unfenced_cluster_incarnation` |
| Terminal typed gRPC or known channel error | one | table-specific cache action | zero | raise typed error |
| Request-context cancellation before an attempt starts | zero | no negative publication | zero | raise `asyncio.CancelledError` |

Each normal or forced-refresh iteration consumes exactly one of the five
attempt slots and obtains exactly one atomic channel snapshot when network
routing is allowed. Local-policy legacy consumes one attempt but opens no
channel. The loop counter never resets. Before a retryable-unavailable
transition, the router invalidates the exact failed key. The next iteration
always obtains a new atomic snapshot. A changed cluster hash, endpoint, or
tunnel generation renegotiates under the new key; an unchanged key still
forces a fresh handshake after invalidation. Retryable `UNAVAILABLE` never
resets the loop counter or the consumed one-shot flag. When it occurs before a
forced refresh obtains fresh capability evidence, `refresh_required` remains
set, so the next iteration is another forced refresh after the ordinary
backoff. When it occurs during the changed-boot method call after fresh
evidence was obtained, `refresh_required` is cleared and the next iteration is
normal.

An exhausted all-`UNAVAILABLE` call therefore performs exactly five attempts,
five network-bearing snapshots, and four sleeps. If advertised
`UNIMPLEMENTED` occurs on attempt `k` where `k <= 4`, the first forced refresh
is attempt `k + 1` and there is no sleep between them. Every retryable
`UNAVAILABLE` after that consumes one remaining slot and, except on attempt 5,
one ordinary backoff sleep. For example, method `UNIMPLEMENTED` on attempt 1
followed by forced-refresh `UNAVAILABLE` on attempts 2 through 5 performs five
attempts and three sleeps. A later advertised-method `UNIMPLEMENTED` performs
no sleep and is terminal protocol error. No path exceeds five attempts or four
sleeps.

The existing immediate connection-refused `UNAVAILABLE` case,
`DEADLINE_EXCEEDED`, non-context `CANCELLED`, `RESOURCE_EXHAUSTED`, and known
tunnel command, channel-ready, tunnel-lock, or startup-exhaustion failures are
typed unavailable after one attempt. Arbitrary implementation exceptions are
not relabeled as transport unavailability. Request-context cancellation stays
`asyncio.CancelledError`, dispatches no legacy call, and consumes no later
attempt.

### Post-advertisement `UNIMPLEMENTED`

An advertised method returning exact `UNIMPLEMENTED` follows the transition
table above. The one-shot transition flag belongs to the top-level call, so
cache loading, retryable unavailability, method invocation, and a boot change
cannot reset it. The next outer iteration calls `force_refresh()` on its newly
acquired snapshot and uses no nested helper or retry budget. Retryable
`UNAVAILABLE` may repeat that forced-refresh state within the remaining outer
slots, but it does not grant another post-advertisement transition.

The forced-refresh iteration follows these rules:

1. A changed channel key discards all evidence from the old key.
2. A fresh capability RPC returning exact `UNIMPLEMENTED`, or a valid fresh
   advertisement omitting the method, selects the normal bounded legacy route
   for that fresh evidence.
3. If the same logical key still advertises the method, the original method
   `UNIMPLEMENTED` is confirmed. Invalidate that key and raise
   `SkyletProtocolError`; the method is not called again and legacy is not
   dispatched.
4. If a different boot advertises the method, invoke it once on the refreshed
   snapshot within that same attempt. Success returns normally. Retryable
   non-connection-refused `UNAVAILABLE` invalidates the key, clears
   `refresh_required`, sleeps when a slot remains, and advances to a normal
   attempt. Exact `UNIMPLEMENTED` is a later advertised-method failure, so it
   invalidates the new key and raises `SkyletProtocolError` with no legacy
   dispatch, refresh, or sleep.
5. Retryable non-connection-refused `UNAVAILABLE` from the forced capability
   RPC invalidates the key, keeps `refresh_required`, and follows the ordinary
   backoff and outer-budget rule. On attempt 5 it raises typed unavailable with
   no sleep.
6. Authentication, internal, cancellation, malformed, deadline, resource, and
   every other nonretryable refresh outcome is terminal in the current attempt
   and never dispatches legacy. It performs the error table's cache action and
   no sleep.

No protocol-violation override exists. Only fresh capability-service absence
or a fresh valid advertisement that omits the contract may select legacy after
the one-shot transition.

## Error contract

Retain `SkyletUnavailableError` and `SkyletInternalError`. Add a small common
base plus `SkyletAuthenticationError`, `SkyletProtocolError`, and
`SkyletApplicationError`. No new type joins `SKYLET_GRPC_FALLBACK_ERRORS`, and
no raw `grpc.RpcError` escapes authoritative ordinary-status routing.

| Evidence | Route or typed result | Cache action | Legacy allowed |
| --- | --- | --- | --- |
| Local gRPC policy disabled or no Skylet runtime | local-policy legacy | no capability entry | yes during compatibility |
| Authoritative same-row observation has null `cluster_hash` | unfenced-incarnation legacy | no cache or tunnel mutation | yes during compatibility |
| Capability RPC exact `UNIMPLEMENTED` | capability-RPC-absent legacy | provisional absence, at most 60 seconds | yes |
| Valid advertisement omits the exact contract | method-absent legacy | boot-bound advertisement, at most 60 seconds | yes |
| First advertised method exact `UNIMPLEMENTED` | one forced-refresh transition within the outer budget | invalidate | only if fresh evidence proves capability-service or method absence |
| Advertised method exact `UNIMPLEMENTED` after the one-shot transition is consumed | `SkyletProtocolError` | invalidate; publish no override | no |
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
  compare-and-set publication and clearing for non-null hashes. Fail closed
  before SQL, process open, or process signal for null hashes; permit only an
  explicitly uncached, read-only handshake through an already healthy tunnel.
- Return the exact `UPDATED`, `CONFLICT`, or
  `UNFENCED_CLUSTER_INCARNATION` mutation result. Enforce numeric tuple bounds
  and quarantine malformed metadata until exact-hash, exact-bytes repair.
- Return channel plus tunnel snapshot atomically from every non-null
  snapshot-bearing handle path; preserve channel-only legacy reads without
  exposing null cache identity.
- Add bounded single-flight capability caching with precise leader, waiter,
  ordinary/forced barrier, cancellable synchronous polling, fork, expiry, and
  honest boot-refresh behavior.
- Keep current transport choice unchanged.

### C3: shadow decision

- Add `off`, `shadow`, and `authoritative_get_job_status` modes.
- Parse only `SKYPILOT_SKYLET_ROUTING_MODE`, default missing or empty to `off`,
  and reject every other nonempty value.
- Add the decision-only `propose_get_job_status_route()` API and the contained
  `observe_current_get_job_status()` seam. Keep the current backend body inline,
  mark the exact current gRPC and SSH branches, and execute status exactly once
  with identical return and exception behavior. Do not introduce the C4a
  adapter or a status callback in C3.
- Add `SkyletCapabilityRpcAbsentV1` and its sole exact-`GetCapabilities`
  `UNIMPLEMENTED` factory; reject every other negative-publication source.
- Compare proposed route with actual existing selection in structured logs and
  the exact unsampled Datadog event. Block C4a until the owned 30-minute
  controller/executor coverage, split-API nonexecution, p95 latency, and
  zero-unexplained-divergence gate passes. Do not add a stats store and do not
  dual-read every status call.

### C4: authoritative ordinary job status

- Move SSH status execution into one temporary legacy adapter with the exact
  typed callback signature, five-operation transcript, and six-case matrix in
  C4a. Retain the C3 observer around the once-executed inline body.
- In C4b, delegate `CloudVmRayBackend.get_job_status()` to the router, remove
  the public C3 proposal and backend observer integration, and move the
  characterized off/shadow compatibility branch plus observation into the
  router.
- Use one five-attempt router loop with a new atomic snapshot per attempt,
  exact-key invalidation and renegotiation, no nested retry helper, and one
  bounded post-advertisement forced refresh.
- Canonicalize gRPC and SSH output, including no-job latest.
- Make only typed unavailability transient in Managed Jobs, and contain it per
  future in Serve without inferring preemption.
- Keep `get_job_status_with_system_recovery()` unchanged.
- Route every reachable null-hash ordinary-status row through the explicit
  `unfenced_cluster_incarnation` compatibility decision and block adapter
  deletion until each row is upgraded or rejected.
- Enable only on the test cluster until mixed-version and fault qualification
  passes.

### C5: compatibility closure

- Do not delete the legacy adapter or missing-capability route until an
  enforced support policy proves every reachable ordinary-status target both
  has a supported Skylet runtime and is permitted to use gRPC, or rejects it
  with an explicit upgrade-required error before routing. Offline, long-lived,
  null-hash, local-policy-disabled, and `has_skylet=False` targets remain
  reachable until that policy rejects them. A version-43 floor alone is
  insufficient.
- Retain the rule that a repeated advertised-method `UNIMPLEMENTED` is a typed
  protocol error and never compatibility evidence.
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
  trip, and boundary tables accept port 1/65535 and PID 1/`2**31 - 1` while
  rejecting zero, overflow, booleans, non-integers, wrong arity, and
  noncanonical generations. Old readers consume tuple fields 0 and 1, and new
  readers accept a valid bounded pair written after rollback.
- Malformed metadata under a non-null hash proves zero channel, cache-key,
  open, replacement, normal-clear, and process-signal calls. Only the explicit
  repair path with exact non-null hash and exact raw-byte predicates may clear
  it; either mismatch changes zero rows.
- Every non-null fast, exclusive-lock, shared-lock, and new-tunnel snapshot
  return proves channel endpoint and key came from the same tunnel object and
  same-row cluster hash. Null compatibility returns prove channel-only output
  and no exposed cache identity.
- `test_tunnel_mutation_fails_closed_for_null_hash_aba` seeds a null-hash row
  with prior tunnel metadata, takes the same-row snapshot, then deletes and
  reinserts the same name with a null hash and byte-identical serialized
  metadata whose PID spy represents a replacement process. Both
  open-and-publish and clear with the old snapshot return
  `TunnelMutationResult.UNFENCED_CLUSTER_INCARNATION`. The test
  asserts zero SQL mutation calls, zero `open_ssh_tunnel` calls, zero process
  registrations, zero process-signal calls, a still-live replacement-process
  spy, and byte-identical replacement values. This is the executable
  null-to-null ABA proof; no `IS NULL` predicate is presented as an incarnation
  fence.
- `test_tunnel_cas_nonnull_hash_fences_recreation` separately deletes and
  reinserts an old non-null-hash row with a new non-null hash and byte-identical
  metadata. Its stale publish affects zero rows through hash equality,
  returns `TunnelMutationResult.CONFLICT`, terminates only its own newly opened
  unpublished process, and preserves the replacement process. A matching
  publish and clear each return `UPDATED`. A stale-clear case changes only
  generation after the snapshot and proves `CONFLICT`, zero rows, and no
  replacement-process signal.
- Null-hash read-only negotiation tests prove that well-formed healthy
  persisted metadata permits exactly one capability RPC with zero cache read,
  publication, SQL mutation, tunnel recovery, status RPC, or process signal.
  Missing, malformed, and unhealthy metadata each fail typed before process
  open. C4 tests prove even the healthy case cannot invoke advertised
  `GetJobStatus`, records `unfenced_cluster_incarnation`, and calls the
  temporary legacy adapter exactly once.
- Independent cluster-hash, endpoint, and generation changes are cache misses,
  including endpoint and PID reuse with a new generation.
- N same-key callers make one load while key A never blocks key B. A normal
  caller joins a forced flight. A forced caller behind an ordinary flight waits
  as a barrier, discards its result, and then produces exactly one forced load,
  with no network overlap. Failed and cancelled leaders publish nothing; a
  cancelled leader lets one peer become leader. Injected 50 ms condition
  polling proves a synchronous cancelled waiter exits within one quantum and
  does not affect the leader or other peers.
- TTL edges, 1024 to 1025 LRU eviction, missing-hash uncached behavior, and
  post-fork empty state are deterministic under injected clocks and jitter.
- A forced refresh observing boot B removes boot-A omission decisions. A
  separate test records the intentional bound that healthy boot-A
  evidence remains usable until expiry without a refresh-triggering signal.
- Positive advertisement, typed exact capability-service absence, and
  boot-bound omission are the only cacheable evidence classes. Factory tests
  prove only exact `CapabilitiesService/GetCapabilities` `UNIMPLEMENTED`
  constructs `SkyletCapabilityRpcAbsentV1`; wrong RPC path, method-level
  `UNIMPLEMENTED`, `UNKNOWN`, fabricated sentinel, and `publishable=False`
  snapshots cannot publish. Existing C1 parser, client, Python 3.10
  import-floor, and tunnel multiprocess tests remain green.

### C3 shadow tests

- Missing and empty mode parse to off, all three exact values parse, and any
  other nonempty value fails configuration.
- Off performs no capability call and uses only the no-op observer. The C3
  decision-only API accepts no job IDs, stream flag, runner, or status callback
  and never invokes the proposed method.
- Every supported, absent, unfenced-null-row, unavailable, cancelled,
  malformed, unexpected-proposal, observer-setup-failure, and logging-failure
  shadow case runs the current inline path exactly once with the same object
  identity or exact exception identity. A recording observer proves gRPC
  success marks `grpc`,
  broad-fallback then SSH marks final `legacy`, uncaught gRPC error remains
  `grpc`, direct SSH remains `legacy`, and one event is attempted only from
  `__exit__`. Event failure never changes the body result.
- Event-schema tests assert the exact fourteen field names, types, closed
  reason enum, comparison truth table, integer latency, and omission of every
  forbidden target or error detail. Unknown fields and enum values fail the
  event builder before logging.
- Comparison records final gRPC success or SSH fallback. Exact capability-RPC
  absence plus actual gRPC success is the only expected mismatch; all other
  successful unequal routes are unexplained.
- Split-role tests prove the rendered role environment, API
  `_request_execution_enabled()` false result, absence of API request workers,
  and executor/controller-only claims. The read-only Datadog fixture then uses
  real queued executor bodies and controller polling plus 20 distinct tunnel
  generations to generate at least 100 unsampled events and 20 misses for each
  required role. The recorded 30-minute query proves zero API execution events,
  complete controller/executor coverage, per-required-role p95 latency at most
  500 milliseconds, and exactly zero unexplained mismatches.

### C4a legacy adapter tests

The adapter test uses one recording `LegacyStatusCommandRunner`, asserts one
call, and compares the command to the frozen current
`JobLibCodeGen.get_job_status()` output for every row in this matrix:

| Case | Inputs and runner result | Required assertion |
| --- | --- | --- |
| Latest no job | `job_ids=None`, `stream_logs=True`, `(0, message_utils.encode_payload({None: None}), '')` | exact latest-job code and flags; returns `{None: None}` |
| One ID | `job_ids=[7]`, `stream_logs=False`, `(0, message_utils.encode_payload({7: JobStatus.RUNNING.value}), '')` | exact one-ID code and flags; returns typed status 7 |
| Several IDs | `job_ids=[7, 9]`, `stream_logs=True`, `(0, message_utils.encode_payload({7: JobStatus.SUCCEEDED.value, 9: None}), '')` | exact list code and flags; preserves decoded known and `None` values |
| Nonzero | one ID and `(23, 'ignored', 'remote failure')` | exact `handle_returncode` error text and stderr; decoder not called |
| Empty stdout | one ID and `(0, '', 'remote warning')` | exact `CommandFailureException` fields from the transcript |
| Malformed payload | one ID and `(0, 'not-a-payload', '')` | unchanged decoder exception; no retry or second runner call |

Every row asserts `require_outputs is True`, `separate_stderr is True`, and
the exact supplied `stream_logs` value. Separate AST checks prove the adapter
accepts no backend or handle, contains the five transcript operations in
order, and owns no route, cache, retry, or protobuf canonicalization branch.

### C4 retry, response, consumer, and ledger tests

- Retryable `UNAVAILABLE` succeeds on route attempts 2 and 5 and fails typed on
  attempt 5. The exhausted case obtains five snapshots and performs exactly
  four cancellation-aware sleeps.
- Cluster hash, endpoint, or generation changes between attempts force a new
  handshake and prevent old channel or decision reuse. An unchanged key after
  invalidation also forces a handshake.
- Handshake and method failures share the same five-attempt budget. No test can
  observe a 25-attempt nested product.
- Advertised `UNIMPLEMENTED` on attempts 1 and 4 consumes the one-shot
  transition and makes the next attempt a forced refresh with zero intervening
  sleeps. On attempt 5 it raises protocol error with no refresh. Same-boot
  confirmation makes no second method call and raises protocol. A changed-boot
  second `UNIMPLEMENTED`, or any later advertised `UNIMPLEMENTED` after the flag
  is consumed, invalidates and raises protocol with no legacy call, refresh, or
  sleep.
- Forced-refresh capability `UNAVAILABLE` on attempts 2 through 4 retains
  `refresh_required`, invalidates once per failed key, sleeps once per advance,
  and consumes only the remaining outer slots; attempt 5 raises typed
  unavailable without sleep. Changed-boot method `UNAVAILABLE` clears
  `refresh_required`, invalidates, sleeps, and advances normally. Exact
  sequences assert snapshots, cache actions, attempts, and sleeps, including
  `[UNIMPLEMENTED, UNAVAILABLE, UNAVAILABLE, UNAVAILABLE, UNAVAILABLE]` as five
  attempts and three sleeps.
- Connection-refused `UNAVAILABLE`, `DEADLINE_EXCEEDED`, non-context
  `CANCELLED`, `RESOURCE_EXHAUSTED`, and known tunnel acquisition failures are
  one-attempt typed unavailable outcomes. Cancellation before an attempt,
  during RPC, and during backoff remains `asyncio.CancelledError`.
- Every gRPC status in the table proves exact type, cache action, attempt count,
  and zero unapproved legacy calls. No raw `grpc.RpcError` escapes.
- Same-key and same-boot confirmation, a changed boot, fresh service absence,
  fresh omission, retryable and nonretryable refresh failure, tunnel change,
  and newly observed boot each exercise the one-shot forced-refresh contract.
  No case publishes or consumes a protocol-violation override.
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
   while retaining the worker boot ID. Before deployment, the disposable
   global-state test must prove publish and clear from a null-hash snapshot
   perform no SQL, process open, or process signal across a null-to-null
   delete/reinsert with byte-identical metadata. Inventory every reachable
   null-hash row. C3 may observe one only through a healthy persisted tunnel;
   C4 must route it through the temporary compatibility adapter, and C5 remains
   blocked until the inventory is empty or every remaining row is rejected.
10. Roll back to the C1 image, prove it reads the persisted triple and calls the
    worker, then roll forward and prove C2 reads any pair written by C1.
11. Deploy C3 first in `off` and prove zero shadow events, then enable `shadow`
    only on the split-role test cluster. Prove rendered role values and that API
    pods start no request workers, then exercise v43 supported, v43
    local-policy-disabled, v42 capability-absent, unfenced null-hash,
    unreachable, tunnel-replaced, and worker-restarted cases through genuine
    executor bodies and controller polling. After warmup, the Platform SkyPilot
    on-call records the exact 30-minute Datadog controller/executor coverage,
    zero API execution, p95 latency, and zero-unexplained-divergence query
    permalinks. Any failed condition blocks C4a.
12. Deploy C4a only after the six-row generated-code, runner-argument,
    nonzero, empty-output, malformed-payload, and no-job adapter matrix passes.
    Prove v42, local-policy, v43 gRPC, and characterized broad fallback results
    remain unchanged through the temporary adapter.
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
contract is bidirectional pair/triple readability. New C2 publication and
clearing are incarnation-fenced only for non-null cluster hashes; null-hash
rows fail closed before mutation or process action and are explicitly not
fenced. Rolling back to C1 preserves tuple readability but re-enables C1's
unfenced name-only null-row writes, so a rollback cannot claim ABA safety for
those legacy rows. C2 qualification therefore includes the exact null-to-null
delete/reinsert ABA with byte-identical metadata, not only null-to-nonnull
replacement. The rollback owner must either accept that bounded legacy risk or
keep C2 while relaunching/remediating the row to obtain a non-null
hash. Null-row fail-closed cleanup can leave an old tunnel process for its
owning legacy process or cluster teardown to reap; C2 never guesses that the
PID belongs to the observed incarnation. The worker protocol is unchanged, so
C2 neither bumps `SKYLET_VERSION` beyond 43 nor requires a worker restart.

C3 defaults off. Shadow enablement is split-role test-cluster-only. C4a is
blocked until the exact unsampled Datadog event has 30 continuous minutes of
required controller and executor coverage, direct proof that API pods execute
no request body and emit zero qualification events, per-required-role p95 at
most 500 milliseconds, and zero unexplained divergence on the exact image SHA.
No telemetry table, custom Datadog client, or metric dependency is added.

C4a is behavior-preserving but runtime-affecting because every legacy ordinary
status call crosses the exact five-operation adapter transcript. Its six-case
matrix and same-result live qualification are mandatory. C4b deploys off, then
shadow, then authoritative only on the test cluster while an immutable C3 or
C4a rollback image remains available. Null-hash rows remain on the temporary
compatibility route and block adapter deletion, not C4b test-cluster
enablement. Promotion requires the full error matrix, exact attempt and sleep
counts, retry and re-key proof, typed Jobs and
Serve behavior, unchanged system recovery, and manifest consistency. No
production-wide default changes in C2 through C4.

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
`SkyletRoutingMode.OFF`, `SkyletRoutingMode.SHADOW`,
`SkyletTransportRouter.propose_get_job_status_route`,
`SkyletTransportRouter.observe_current_get_job_status`,
`SkyletCapabilityRpcAbsentV1`,
`SkyletTransportRouter._classify_get_capabilities_failure`, and the shadow
comparator/emitter.
After the runtime commit exists, a follow-up manifest commit records its exact
40-character SHA and changes only `planned -> present` plus status history.
Its test gate retains the decision-only API, once-executed inline-body and exact
return/exception identity contract, typed exact-RPC absence-publication
contract, event schema, comparison truth table, and split-role coverage
contract. Its telemetry gate requires the recorded
30-minute Datadog window with controller and executor coverage, direct and live
proof of zero API request-body execution events, per-required-role p95 at most
500 milliseconds, and zero unexplained divergence before shadow code can later
be removed.

In the C4b runtime commit, delete the two C3-only public symbols and the backend
observer call sites, then replace only their row-104 locators with the private
router locations below. Keep row 104's provenance, status, history, gates,
evidence, and blocker unchanged:

```yaml
- kind: python_symbol
  path: sky/backends/skylet_transport.py
  symbol: SkyletTransportRouter._decide_get_job_status_route
- kind: python_symbol
  path: sky/backends/skylet_transport.py
  symbol: SkyletTransportRouter._observe_get_job_status
```

The `OFF`, `SHADOW`, typed absence, classifier, and emitter locators remain. No
C4b commit may leave a C3 public-symbol locator unresolved, and moving
observation into the router does not satisfy a removal gate.

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
removal evidence. Row 105's permanent test gate retains the exact generated
code, runner flags, nonzero, empty-output, malformed-payload, and no-job matrix
after the temporary adapter is deleted.

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
- `SkyletCapabilityRpcAbsentV1` and its exact-RPC classifier when the
  missing-capability route closes;
- temporary mixed-version shadow tests, while retaining a frozen permanent
  compatibility corpus;
- `JobLibCodeGen.get_job_status`;
- `job_lib.load_statuses_payload` when no remaining compatibility caller
  exists;
- the missing-capability legacy route only after every reachable target has
  supported Skylet and gRPC policy, or is rejected with an explicit
  upgrade-required error before routing.

Offline, long-lived, null-hash, local-policy-disabled, and
`has_skylet=False` targets all block adapter deletion until that enforced
policy upgrades or rejects them.
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
as compatibility. Only explicit unsupported evidence, local policy, or the
bounded unfenced-null-incarnation safety decision may choose legacy.

### Durable capability storage

Tunnel and boot identities are process-local and short-lived. PostgreSQL state
would become stale authority and add migration work without improving safety.

### One universal lifecycle state machine

The router should share transport actuation only. Managed Jobs and Serve keep
separate reducers because their recovery and scaling policies differ.
