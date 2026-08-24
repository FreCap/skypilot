# SkyServe prediction-time history

## Status

Accepted for implementation on 2026-07-22 after explicit product approval to
replace full HTTP response-time history with customer-facing prediction time.

Updated 2026-08-24. The reporter-minute histogram is implemented, but its
asynchronous completion count is explicitly approximate. The PostgreSQL
ledger, stable API ingest path, load-balancer bind/lookup/transition
integration, and coverage-aware status/dashboard summary are implemented,
locally qualified, and deployed dark in SkyPilot 1.1.1460 / Helm revision 588.
Serve058 is at the live PostgreSQL head and both load-balancer slots run the
same immutable 1.1.1460 image. Read-only receipt probes and the fresh dashboard
summary are production proven. The source now projects the exact ledger through
the controller-free current-demand read. The first live qualification exposed
an unbounded ledger fan-out: a clean submission performed a pre-bind lookup and
a completion performed a lookup before its terminal write, while lookup bursts
could consume the load balancer's entire API transport pool. The checked-in
correction removes those two steady-state reads and bounds the transport, but is
not yet deployed or production-proven; the 10,000-request qualification remains
open. Adversarial review of the separate Boltz
caller found that caller-maintained first-attempt and heartbeat authority was
both more complicated and less reliable than the already-transactional server
boundary. The corrected contract below makes the stable exact request identity
and intent digest the retry-idempotency boundary, while requiring homogeneous
server and platform activation and a stable service incarnation. The
ledger is the single additive Serve058 migration directly after released
Serve057; the unrelated draft immutable-owner cleanup and sequencing migration
are not part of this feature.

Serve058 support is selected by schema presence and has no separate environment
flag or second activation restart. The protocol is request-opt-in: callers that
omit the exact protocol headers keep the legacy path, while a caller that
declares protocol 1 must receive and validate a complete protocol-1 receipt or
fail closed. The live Boltz caller integration is a separate rollout gate. It
uses the existing `POST /v1/models/model:predict` request and HTTP 200
`IN_PROGRESS` acknowledgement; it does not require a v2 model endpoint, a
worker-specific HMAC, EFS, or a service recreation.

## Problem

The service dashboard currently labels and charts full SkyServe HTTP completion
time. That value includes authentication, load-balancer queueing, replica
selection, retry backoff, proxying, and response delivery. It can therefore
show five minutes for a prediction whose model execution was much shorter. The
product requirement is prediction time, not HTTP latency.

Boltz model runtimes emit the exact `boltz.prediction.duration` metric, but the
SkyPilot API server does not have a provider-independent telemetry query path.
The established asynchronous replica protocol returns `processing_time_ms` in
terminal `async_status` responses. Production fast-ack SkyPilot integrations do
not poll that endpoint through SkyServe, however. They deliver the same terminal
fields through an out-of-band completion marker. Synchronous replicas do not
return an equivalent duration field, so SkyServe must observe their replica
execution boundary.

This design supersedes the previous full HTTP response-time design. It must not
infer prediction duration from async occupancy, treat a fast async submission
acknowledgement as completion, or put Datadog into the online control path.

The same dashboard also needs an exact count of unique asynchronous logical
requests. Reporter-minute histograms cannot supply that count: an active load
balancer restart creates a new reporter session, so delivery of the same
terminal marker through both sessions is intentionally additive today. A
10,000-request qualification also needs a durable receipt distinguishing work
that was definitely rejected before dispatch from a dispatch whose outcome is
unknown. Neither property can be reconstructed from aggregate history.

## Behavior contract

Prediction history covers successful and failed predictions that reach a
replica. It excludes SkyServe authentication, admission and queue time, retry
backoff between attempts, load-balancer-generated errors, and downstream
delivery to the caller.

### Synchronous requests

For a request outside the known asynchronous protocol, timing starts
immediately before the accepted upstream HTTP dispatch and ends when that
replica returns terminal response headers. A retriable response or transport
error does not produce a sample for that attempt. The final non-retriable
response produces one sample:

- HTTP 2xx is `succeeded`;
- every other terminal upstream status is `failed`.

This boundary is replica service time rather than exact in-process handler
time. It includes the short load-balancer-to-replica network interval and, for
a streaming model, ends when response headers begin rather than when model
streaming finishes. The current Boltz synchronous contract returns prediction
results only after handler completion, so this is the closest available
customer-facing boundary without changing every model image.

### Asynchronous requests

Known asynchronous actions are `async_predict`, `async_status`,
`async_capacity`, and `async_cancel`.

- A nonempty SkyServe stable-job header still identifies a legacy
  platform-held async submission for aggregate occupancy, even when its body
  omits `action`. Ledger-qualified work is narrower: it requires RFC 8785/JCS,
  duplicate-free JSON with `action: async_predict`, a dedicated execution-ID
  header equal to payload `request_id`, and a separate stable-job header. Its
  acceptance response never produces a prediction-time sample.
- The execution request ID identifies one logical compute attempt. The caller
  reuses it after a lost HTTP acknowledgement or activity retry, but generates
  a distinct ID for an intentional manager retry, model retry, or OOM
  escalation. SkyPilot never guesses that a terminal request should acquire a
  new generation: a terminal receipt remains final for that execution ID.
- `async_predict` is only an acceptance acknowledgement and never produces a
  prediction-time sample.
- Non-terminal `async_status` responses do not produce a sample.
- An HTTP 2xx terminal `async_status` response with a nonnegative finite
  `processing_time_ms` and nonempty `request_id` produces one sample. The
  model-reported duration is authoritative and the poll's own HTTP lifetime is
  ignored.
- `SUCCEEDED` is `succeeded`. `FAILED`, `EXPIRED`, `CANCELED`, and `CANCELLED`
  are `failed`. `NOT_FOUND` is not a completion.
- An authenticated `POST /_lb/prediction-completed` with the same nonempty
  `request_id`, immutable intent digest, ledger `attempt_id`, `attempt_no`, and
  expected revision, terminal status, and bounded nonnegative integer
  `processing_time_us` produces the same sample after the exact ledger commit.
  This is the completion-marker bridge for fast-ack callers whose terminal
  event is delivered outside SkyServe.
- `async_capacity`, `async_cancel`, malformed JSON, missing durations, and
  unknown status values produce no sample.

The load balancer may retain a bounded least-recently-used set as a hot-path
optimization, but it is never the uniqueness authority. The final ledger
migration stores
one incarnation-scoped logical-request row plus append-only attempt identities
whose state advances monotonically. The
logical primary key uses a SHA-256 of the request ID rather than the raw
identifier and points to exactly one current attempt. Identical terminal
delivery is idempotent across processes and failover; a different terminal
status, duration, or intent for the same exact attempt is a conflict and is
never added as another completion. Only a durable
`REJECTED_PRE_DISPATCH` receipt authorizes a distinct retained attempt under the
same logical request. An `AMBIGUOUS` attempt never authorizes replay under that
execution request ID.

Samples are assigned to their completion-observation minute. Histograms use
inclusive upper-bound buckets of 100 ms, 250 ms, 500 ms, 1 s, 2.5 s, 5 s,
10 s, 30 s, 60 s, 120 s, 300 s, 600 s, 1,200 s, 1,800 s, and 3,600 s, plus one
overflow bucket. Bucket boundaries are versioned constants shared by
collection, validation, persistence, serialization, and the dashboard.

Prediction histograms remain observability only. The exact ledger is different:
for a ledger-qualified stable asynchronous submission, binding the selected
route before transport is a dispatch-safety boundary. A missing, stale, or
conflicting PostgreSQL receipt fails that request closed before upstream
transport. Ledger failure must still never fail controller liveness,
autoscaling, ordinary synchronous requests, or services that do not declare the
ledger protocol.

## Data flow

```text
sync upstream dispatch -> terminal upstream headers -> measured duration
selected async route -> exact projection-fenced bind -> PostgreSQL ledger
          |                         |
          |                         +-> conservative dispatch receipt
same request/digest retry -> same bind -> existing receipt, no second send
lost submit ack -> authenticated receipt lookup -> existing receipt or wait
          v
     upstream send -> accepted / ambiguous transition
          |
async completion marker -> authenticated durable terminal transition
          |                                      |
          +-> LB reporter-minute histogram       +-> exact status summary
                         |                                      |
              PostgreSQL prediction history                    v
                         |                           service dashboard counts
                         v
              dashboard latency distribution
```

## Load-balancer aggregation

Request JSON action parsing reuses the body already cached for proxying. It
does not retain model input after the request ends. A nonempty stable-job
header classifies a platform-held async submission without parsing its body.
For header-free direct callers, only JSON bodies up to 64 KiB are inspected for
the small established action envelope; larger bodies are treated as
synchronous. Synchronous timing is scoped to the final accepted upstream
attempt. For async status, the existing raw-body stream is forwarded unchanged
while a bounded copy is retained for terminal JSON parsing. A body over the
fixed parsing cap is forwarded but not parsed or recorded. A response with
non-identity content encoding is also forwarded but not parsed, because
observability must not alter or decompress the proxy's raw response stream.
The established Boltz async-status response is uncompressed.

The completion callback is registered before the catch-all proxy and remains
behind the existing data-plane bearer middleware. For a ledger-qualified
request it accepts one bounded JSON object with `request_id`, `attempt_id`,
attempt number, expected revision, immutable intent digest, terminal `status`,
and bounded nonnegative integer `processing_time_us`. It never forwards the
request to a replica. The load balancer submits the terminal transition directly
through the stable API Service using the purpose-specific LB-sync token.
PostgreSQL atomically requires the same request digest, attempt UUID, attempt
number, and a current revision at least as new as the submitted fence. A 409
from an older exact-revision API triggers one current-receipt lookup and one
retry fenced to that looked-up revision. The callback returns success only after
PostgreSQL accepts it. Identical at-least-once delivery
returns the same receipt; conflicting delivery returns 409; temporary
persistence failure returns 503 so the terminal reporter retains and retries the
already-durable outcome. Status polling may feed the latency histogram, but it
does not mutate the exact ledger unless it supplies this complete fence.
An absent, malformed, stale, or mismatched completion response is a lost-ack
candidate, not proof that the transition failed. The reporter performs the
authenticated read-only receipt lookup and retries delivery unless that lookup
proves the same attempt already terminal with the identical outcome. Only an
authenticated receipt proving a conflicting terminal outcome is a permanent
conflict.

An authenticated `POST /_lb/async-request-receipt` recovers a lost receipt
without queueing, route selection, provider dispatch, or a ledger mutation. Its
exact body is `ledger_protocol_version`, execution `request_id`, and
`intent_sha256`; the expected incarnation is the required
`X-SkyServe-Service-Incarnation` header and therefore does not add a fourth body
field. A found attempt returns the complete seven-field receipt plus the
protocol, incarnation, and four attempt receipt headers. An absent attempt
returns 404 with the protocol and matching incarnation headers and `No durable
request attempt exists.` A 404 is a
point-in-time observation and is never standalone proof that a concurrent bind
did not commit immediately afterward. The endpoint is therefore recovery and
diagnostics, not dispatch authority. An old load balancer has no dedicated
route and no protocol advertisement; allowing an exact caller to reach one is
an activation-gate violation.

The exact request identity and semantic intent digest are the retry-idempotency
boundary. A no-dispatch prepare activity first creates one append-once,
encrypted PostgreSQL `SkyPilotDispatchIntentV1` record keyed by the stable
execution attempt ID. The row contains the complete canonical semantic
projection, protocol version, stable job ID, recorded provider and service
name, the exact SkyServe service incarnation, stable object identities, every
dispatch-affecting wire option, and its digest; it never contains expiring
presigned credentials or bearer tokens. Prepare requires the authenticated
capacity response to attest the exact `(service_name, service_incarnation)`
tuple and rejects a name different from the selected deployment. The LB echoes
the incarnation on every protocol-1 response. Every exact POST, receipt lookup,
and completion carries it in exactly one
`X-SkyServe-Service-Incarnation` header; the receipt parser requires the echoed
value to equal the prepared value, not merely to be present. The LB compares
the request value with its immutable local service hash before
reading or writing the ledger or selecting a route. Create is idempotent: a retry reads an existing row
before consulting live inputs, and concurrent creators race on the unique key
and then read the winner. Consequently mutable pipeline input, feature flags,
provider configuration, and later code deployment cannot rewrite an already
prepared intent. The Temporal workflow records only the bounded row reference,
digest, provider, service name, and execution attempt ID, not the potentially
large or sensitive model input.

The separate exact-submit activity accepts that recorded handle, loads and
decrypts the append-once projection, verifies its version and digest, and uses
only it to build the body and sends only through the row's recorded
`endpoint_authority`; current deployment configuration is neither a target nor
a fallback. It may refresh transport fields explicitly excluded
from the semantic digest, such as an expired presigned query credential for the
same stable object identity. It must not re-read prediction, pipeline, feature-
flag, provider, or service-selection state. Missing, changed, undecryptable, or
digest-mismatched prepared state fails closed before any network send.
Each load balancer commits one atomic projection-fenced bind before transport;
the bind itself returns an existing non-rejected attempt without dispatch
authority, so the steady path needs no preceding read.
The PostgreSQL logical-request lock serializes concurrent binders: exactly one
fresh bind has `dispatch_authorized: true`; every concurrent or later matching
POST receives the complete existing receipt and performs no worker send. A
current `REJECTED_PRE_DISPATCH` receipt is the only state from which the same
POST may append a successor transport attempt. No caller heartbeat,
process-memory `possible_send` bit, or Temporal attempt number participates in
that decision.

The protocol-1 prepare activity, exact-submit activity, PostgreSQL record shape,
and their dedicated projection/body builders are one frozen compatibility unit.
They must not delegate semantic construction to mutable shared provider
helpers. Compute API currently runs Temporal without worker versioning, so an
activity retry may execute on a later deployment. A later binary must load the
same prepared row and produce the same protocol-1 semantic projection and
digest. Golden vectors cover every semantic field, all five supported caller
paths, and the allowed refreshed transport credentials. Any future semantic
projection change uses a new row version, newly named prepare and submit
activities, and a workflow patch. The current and preceding two protocol
handlers remain registered, and a handler remains longer while any workflow or
nonterminal ledger row can still reference it. Changing V1 semantics behind
the existing names or through a shared helper is forbidden.

This simplification is valid only after both load-balancer slots are
homogeneous Serve058 writers and old platform workers are drained or fenced
from the newly named exact activities. A normal update preserves the service
hash. A delete/recreate changes it, so every older prepared intent fails closed
at the LB even if an address is accidentally reused; it cannot silently create
a same-name attempt in the new ledger namespace. Operators still avoid
recreation while prepared intents are open because it strands rather than
duplicates them. A crash after bind and before worker transport deliberately remains
`DISPATCH_MAY_HAVE_OCCURRED`; a matching retry recovers that receipt and does
not send again. This provides at-most-once dispatch, not guaranteed execution.

The load balancer keeps a bounded dictionary keyed by observation-minute epoch.
Each value contains two fixed-length integer arrays, `succeeded` and `failed`.
The current load-balancer Pod UID remains the reporter identity. Pruning uses
the existing one-hour request-history window and runs on minute boundaries.

Prediction history uses its own top-level sync payload and acknowledgement:

```text
prediction_time_history = {
  bucket_seconds: 60,
  histogram_version: 1,
  buckets: [{
    bucket_start: <aligned epoch second>,
    outcome_counts: {
      "succeeded": [16 nonnegative integers],
      "failed": [16 nonnegative integers]
    }
  }]
}
```

Zero-only outcome arrays may be omitted on the wire. Missing outcomes normalize
to zero arrays. A snapshot is acknowledged only when the controller returns
`prediction_time_history_accepted: true`. An old controller ignores the new
payload and omits that field, so a new load balancer retains its bounded
cumulative counters. The drain history endpoint carries and acknowledges the
same payload.

## PostgreSQL schema and idempotency

Migration 023 creates `serve_prediction_time_history`. The central Serve DB
path is PostgreSQL-only for this table.

```text
service_name         text        primary key
service_hash         text        primary key
reporter_session_id  text        primary key
bucket_start         timestamptz primary key
observed_at          timestamptz not null
prediction_count     integer     not null check >= 0
succeeded_counts     integer[]   not null
failed_counts        integer[]   not null
```

Every array has the fixed version-1 length and nonnegative values. The writer
derives `prediction_count` as the sum of all array elements. Reporter-minute
histograms are cumulative, so an upsert replaces stored arrays only when the
incoming count is at least the stored count. Duplicate and out-of-order reports
cannot double-count or decrement one reporter's history.

Lookups use `(service_name, service_hash, bucket_start desc)`. The hourly
retention sweep deletes rows older than the existing 72-hour Serve history
retention. Service-hash predicates prevent same-name recreation leakage.

Migration 022 and `serve_response_time_history` remain intact as retained
compatibility state. New load balancers stop emitting its payload and the
status API stops reading or exposing it, so no old HTTP latency appears in the
replacement card. Their retention does not authorize source rollback after the
final ledger migration.

### Exact asynchronous request ledger

The single additive post-Serve057 migration creates the PostgreSQL-only
`serve_async_requests` and
`serve_async_request_attempts` tables. Together they are the dispatch receipt
and materialized status index, not a second campaign controller and not the
scientific source of truth. Attempt history is normalized so a successful
retry after a definite pre-dispatch rejection retains both exact receipts.

```text
serve_async_requests
  service_name          text        primary key
  service_hash          text        primary key
  request_key_sha256    text        primary key
  intent_sha256         text        not null
  current_attempt_id    uuid        not null, deferred foreign key
  current_attempt_no    integer     not null check > 0
  created_at            timestamptz not null
  updated_at            timestamptz not null

serve_async_request_attempts
  service_name          text        primary key, foreign key
  service_hash          text        primary key, foreign key
  request_key_sha256    text        primary key, foreign key
  attempt_id            uuid        primary key
  attempt_no            integer     not null check > 0
  state                 text        not null
  revision              bigint      not null check > 0
  dispatch_binding      jsonb
  accepted_at           timestamptz
  terminal_at           timestamptz
  terminal_status       text
  processing_time_us    bigint
  created_at            timestamptz not null
  updated_at            timestamptz not null
```

`request_key_sha256` is the SHA-256 of the bounded raw execution request ID. The
raw ID, model input, result, route URL, and query string are
never stored. `intent_sha256` is supplied by the durable inference controller
and identifies its immutable canonical intent. The live Boltz provider computes
the lowercase SHA-256 over UTF-8 RFC 8785/JCS bytes for this object:

```json
{
  "version": 1,
  "service_name": "...",
  "service_incarnation": "...",
  "stable_job_id": "...",
  "execution_request_id": "...",
  "http_method": "POST",
  "request_path": "/v1/models/model:predict",
  "priority": 0,
  "accelerator_compatibility": ["..."],
  "payload": {},
  "results_url": "s3://...",
  "completion_marker_url": "s3://..."
}
```

`payload` is the pre-encryption logical compute payload. Canonical S3 URIs are
included. The projection also includes the exact service incarnation, request
path/method, priority, accelerator-compatibility claim, and any other header or
option that can affect queueing, route selection, or worker behavior. Encrypted
bytes, IVs, presigned HTTPS URLs/query strings, rotating bearer credentials,
telemetry-only fields, the private selected route URL, and the ledger receipt
are excluded. Those excluded transport capabilities may change only while
remaining derivable from a frozen identity. SkyPilot cannot decrypt or
recompute this digest. The authenticated platform owns it and PostgreSQL
enforces equality for the execution ID.

The ledger's `attempt_no` is a transport-dispatch attempt inside that one
logical compute attempt. It advances only when the previous dispatch has an
exact `REJECTED_PRE_DISPATCH` receipt. It is not the platform's model-retry or
OOM-retry generation. The caller supplies a new execution request ID for those
semantic retries, preventing SkyPilot from mistaking a lost acknowledgement for
permission to execute terminal work again.

`dispatch_binding` has one strict version-1 shape with two distinct version
fences: the current route-contract service version and the selected worker's
replica version. It also stores the route projection generation and SHA-256,
route-source epoch, replica ID and canonical replica-record UUID, projected
accelerator and count, `is_zero_cost`, and a canonical location object.
Location contains either cloud region/zone or the
Kubernetes context, physical cluster UID, and reserved pool key. It never
contains the private route URL. The stable API transaction verifies the current
service hash, fresh route head, exact projection fence, advertised URL identity,
and matching replica record before storing this envelope. The private route
identity therefore carries the selected replica version. The route-contract
version must still match the current projection contract, while the replica row
and its zero-cost admission are checked against the selected-worker version.
This preserves routing to compatible previous active versions during a normal
service update instead of falsely stamping every route as the newest version.

Serve058 has no marker-evidence column. The live reporter does not have a
marker-object digest, so an unused nullable field would create a misleading
contract. SkyPilot must not fabricate worker, device, Pod, accelerator,
projection, or scientific-verification facts from the immutable dispatch
binding. GPU occupancy and wide-node device-use proof remain placement/runtime
qualification concerns outside this operational request ledger. Any future
marker attestation requires a new reviewed schema and protocol.

A terminal state means only that PostgreSQL durably accepted an authenticated,
exactly fenced terminal receipt for this attempt. The receipt does not by itself
prove scientific output correctness. Protocol 1 intentionally has no heartbeat,
scientific-verification, retry-evidence, worker-authentication, or quiescence
sub-protocol. Adding any of those later requires a new reviewed schema and wire
contract; it must not overload nullable fields in Serve058.

The closed current-attempt state machine is:

```text
REJECTED_PRE_DISPATCH -> append DISPATCH_MAY_HAVE_OCCURRED attempt
DISPATCH_MAY_HAVE_OCCURRED -> ACCEPTED | AMBIGUOUS |
                              REJECTED_PRE_DISPATCH
ACCEPTED -> SUCCEEDED | FAILED | CANCELLED | EXPIRED | AMBIGUOUS
AMBIGUOUS -> SUCCEEDED | FAILED | CANCELLED | EXPIRED
```

PostgreSQL guards both insertion and update of attempt rows. Every inserted
attempt begins at revision 1 in one of two exact repository-produced shapes:
bound `DISPATCH_MAY_HAVE_OCCURRED`, or null-binding
`REJECTED_PRE_DISPATCH` when the LB proves rejection before route selection.
Both have no acceptance or terminal fields. The new logical request begins at
attempt number 1 and points to that row in the same deferred transaction.
Direct SQL cannot manufacture an initially accepted, ambiguous, or terminal
attempt, a rejected attempt with a binding, or a may-have-occurred attempt
without one. Existing update triggers enforce all later state shapes and
monotonic transitions. An `AMBIGUOUS` attempt never moves backward to
`ACCEPTED`; only an authoritative terminal receipt can resolve it.

The bind is named `DISPATCH_MAY_HAVE_OCCURRED` because a PostgreSQL commit and
an HTTP send cannot be atomic. Binding commits before `client.send()`. A crash
after the commit and before the send therefore remains conservatively
ambiguous. If the final route/client checkout fails while the process survives,
the exact attempt transitions to `REJECTED_PRE_DISPATCH`; if that update is
lost, no retry is authorized. Only `REJECTED_PRE_DISPATCH` permits a new
attempt. Every
mutation locks the logical request and its current attempt and matches
`attempt_id` plus per-attempt `revision`. Advancing after a definitive rejection
inserts a new attempt and atomically advances the logical pointer. All prior
rows remain immutable. A late terminal
callback for an old attempt conflicts instead of overwriting a successor.
No upstream HTTP response proves pre-dispatch rejection in the live v1 worker
contract. Every non-2xx response is therefore `AMBIGUOUS`; a 4xx response must
not be converted into retry authorization merely because it looks like a
client-side failure. `REJECTED_PRE_DISPATCH` may be recorded only from LB-local
proof that no upstream send could occur, such as route/client checkout failure
before transport or a rigorously classified pre-connect failure.
An `AMBIGUOUS` attempt may later receive the exact terminal callback, but it can
never authorize another send under the same execution request ID. This is an
intentional fail-closed boundary: a future evidence-backed retry design must
use a future schema/protocol rather than weakening Serve058.

The unique logical-request attempt number and composite attempt identity
prevent an attempt from moving between requests. Deferred reciprocal foreign
keys guarantee the logical pointer always names an attempt belonging to the
same request at commit. Indexes on
`(service_name, service_hash, state, updated_at)` and
`(service_name, service_hash, terminal_at)` support bounded summaries. A row is
retained for the entire lifetime of its service incarnation: deleting even a
terminal logical request would forget that the stable ID was already dispatched
and could authorize a duplicate. `DISPATCH_MAY_HAVE_OCCURRED`, `ACCEPTED`, and
`AMBIGUOUS` are therefore never age-pruned. The ledger migration installs
unconditional delete guards on logical requests and attempts, so it does not
implement garbage collection, including for old incarnations. A future
maintenance migration may replace those guards with an explicitly fenced
old-incarnation sweeper only after it proves service and worker quiescence and
a late-receipt grace interval. Service-hash predicates prevent same-name
recreation leakage in current reads and writes, but do not by themselves make
deletion safe.

### Boltz prepared dispatch intent

The platform database adds one PostgreSQL-only, insert/read-only table before
exact caller activation. This is caller recovery state, distinct from the
SkyServe receipt ledger:

```text
sky_pilot_dispatch_intent_v1
  organization_id        text        primary key
  workspace_id           text        primary key
  execution_request_id   text        primary key
  workflow_id            text        not null
  workflow_run_id        text        not null
  stable_job_id           text        not null
  path_kind               text        not null
  provider                text        not null check = 'skypilot'
  service_name            text        not null
  service_incarnation     uuid        not null
  endpoint_authority      text        not null
  projection_version      integer     not null check = 1
  projection_sha256       text        not null
  intent_sha256           text        not null
  wrapped_dek             jsonb       not null
  encrypted_projection    jsonb       not null
  plaintext_size_bytes    integer     not null check between 1 and 1048576
  created_at              timestamptz not null
```

The ownership tuple is the tenant scope plus execution request ID. Text fields
use the same explicit UTF-8 and length bounds as their corresponding workflow
and SkyServe headers; both digests are lowercase 64-character SHA-256. The
projection is JCS-canonical before hashing, LZ4-compressed when useful, and
sealed with the platform payload codec's versioned AES-256-GCM envelope. The
same PostgreSQL row stores the dedicated random DEK wrapped under a KEK derived
from the retained platform `SECRET_KEY` and its recorded `KEK_VERSION`; no S3,
EFS, PVC, or process cache is needed to recover the key. The base secret and
every historical KEK version remain available for the full row lifetime. Key
loss, authentication failure, unknown envelope/version, tenant
mismatch, oversized plaintext, or digest mismatch is a permanent fail-closed
error before receipt lookup or network send. No raw secret, bearer token,
presigned query credential, or EFS path is stored.

Database constraints and a trigger reject every `UPDATE` and `DELETE`; the
application exposes only create-if-absent and scoped read operations. An insert
loser reads, decrypts, and validates the winning row rather than trusting
`skipDuplicates` or rebuilding from current inputs. The initial implementation
has no garbage collector and never shreds its dedicated key, so a lost prepare
acknowledgement cannot outlive its retry authority and recreate mutable state.
A future retention design requires a separate reviewed migration proving the
owning Temporal workflow is durably closed, every SkyServe receipt is terminal,
the complete workflow-history/retry retention plus late-delivery horizon has
elapsed, and deletion cannot authorize a new attempt. A mutable `closed_at`
field is intentionally absent from the frozen row; lifecycle indexing, if
needed later, belongs in a separately fenced table.

## Controller compatibility

During a mixed rollout, new controllers accept both the legacy
`response_time_history` payload and the new `prediction_time_history` payload.
Legacy persistence remains an observability-only compatibility path, but it is
not exposed by the new API. New load balancers emit only prediction history.

This permits a controller-first mixed rollout for aggregate history. It does
not authorize application-source rollback after the final ledger migration:

- old load balancer to new controller keeps its legacy delivery contract;
- new load balancer to old controller retains unacknowledged prediction
  counters until it reaches a new controller;
- migration 023 and its unused rows remain in place across a fix-forward
  deactivation.

The ledger server uses an additive, dark, mixed-version rollout. A Helm upgrade applies
Serve058 and rolls the control plane while preserving the live service,
replicas, route projections, and ordinary traffic. Schema presence is the only
server activation condition: a Serve058 API server advertises protocol 1 to new
load balancers after it sees the Serve058 Alembic head. There is no separate
environment flag, activation deployment, EFS checkpoint, dual schema, service
delete/recreate step, or boltz-platform runtime pin. The unrelated
immutable-owner cleanup is deferred and is not a prerequisite or member of this
migration.

Protocol negotiation is explicit in both directions:

- old caller to new server: absent request opt-in headers select the unchanged
  legacy path during the dark server-first rollout;
- new exact caller to a pre-Serve058 server is intentionally unsupported: that
  server cannot reject the new headers before forwarding the body. The exact
  caller is therefore activated only after both live load-balancer slots are
  proven to run the same Serve058+ image, and activation is fix-forward;
- new caller to new server: once the server advertises protocol 1, every exact
  response must carry the same protocol advertisement plus attempt UUID,
  attempt number, revision, and state; a missing or malformed receipt fails
  closed and the same exact request is recovered or retried without changing
  provider, request identity, body semantics, or service incarnation;
- mixed API/controller/LB Pods: an LB does not accept protocol-1 requests until
  its controller sync advertises schema-backed protocol 1.

The durable inference submitter opts in on its existing
`POST /v1/models/model:predict` call with protocol 1, a bounded execution
request ID equal to canonical body `request_id`, a separate bounded stable job
ID, and the immutable intent digest. The body must be recursively canonical,
duplicate-free RFC 8785/JCS JSON and is bounded by the service's configured
request-queue body ceiling (currently 1 MiB), not the legacy 64 KiB
action-observation cap.
The existing worker may return any 2xx acknowledgement, including the live HTTP
200 `{request_id, status: "IN_PROGRESS"}` response.

The execution ID and intent are retry-stable application identities. The intent
digest covers a documented JCS semantic projection: model inputs, options,
stable object identities, all dispatch-affecting feature decisions, the
recorded provider and service name, and the execution request ID. It excludes
expiring transport credentials such as presigned URL query parameters. The
actual canonical request body may therefore contain refreshed credentials on a
Temporal activity retry only when those refreshed fields are derived from the
frozen object identity and remain outside the semantic digest. The caller
repeats the same exact POST contract; the PostgreSQL bind decides whether it
may dispatch or must return the existing receipt. A semantic change requires a
new execution ID, prepared-intent row, protocol version, and digest.

Once the Boltz SkyPilot provider is activated in exact mode, its execution
identity is mandatory and the legacy 429/503 spill branch is removed for that
provider. A future or old process cannot omit identity and silently regain a
paid fallback. The selected provider is also stable across Temporal activity
retries: an execution carrying a SkyPilot exact identity remains restricted to
SkyPilot even if live warm/load signals would reorder Baseten or another
provider on the retry. Only SkyPilot's reserved-first, Spot-only residual
policy may add paid capacity for that execution. Platform workers must be
homogeneously exact-capable (or fenced from new work) before activation. The
workflow records the provider choice before selecting the newly named exact
activity; Baseten and other providers retain their existing activity and
cancellation behavior. Existing Temporal histories remain on their recorded
legacy activity, so replay does not change command arguments. Activation is
fix-forward; an old caller binary is not a safe rollback after exact work has
been accepted.

A completion-marker polling timeout is not permission to resubmit and is not
allowed to abandon an `ACCEPTED` ledger row. The workflow retains the same
recorded provider, execution ID, placement, and receipt and continues bounded
poll attempts without another provider send. It durably reports a terminal
status only from the existing marker/result authority or from an explicit
reviewed expiry/cancellation decision; a local polling deadline alone proves
neither failure nor quiescence.

The current SkyPilot asynchronous worker cannot be remotely cancelled through
the shared load balancer. After an exact `ACCEPTED` receipt, the compute-api
workflow therefore owns marker observation and terminal reporting in a durable
non-cancellable Temporal scope. A parent cancellation request may record user
intent, but it does not abandon observation or falsely report `CANCELLED` while
the worker may still run. Likewise, an unreadable or malformed marker is not an
authoritative failure: observation continues against the same execution and
placement. This deliberately prefers a visibly unresolved exact row over a
fabricated terminal result. Any future remote-cancel or expiry transition must
prove handler quiescence and survive late marker/result races before it may
replace this policy.

After route selection, the LB performs one atomic `bind` with
`allow_new_attempt: true`. It returns an existing current receipt without
dispatch authority, or creates a fresh projection-fenced attempt. Only a
current `REJECTED_PRE_DISPATCH` receipt permits creation of a successor;
`DISPATCH_MAY_HAVE_OCCURRED`, `ACCEPTED`, `AMBIGUOUS`, and terminal receipts
never authorize another provider send. If no provider route can be selected,
the pre-dispatch error path performs the read-only lookup before it records a
durable rejection, preserving existing-attempt recovery without charging every
successful submission for that read. Service-hash isolation
keeps late writes from an older incarnation fenced if the service is ever
explicitly recreated. The operator must not recreate the service while exact
rows are nonterminal, because the new incarnation cannot recover the old
namespace and repeating application work against it could dispatch again.

## API and dashboard

`replica_status_history` remains the compatibility envelope and adds:

```text
prediction_time_histogram_version: 1
prediction_time_bucket_upper_bounds_seconds: [15 finite values]
prediction_time_samples: [{
  timestamp: <minute epoch second>,
  outcome_counts: {
    "succeeded": [16 aggregated counts],
    "failed": [16 aggregated counts]
  }
}]
```

The API aggregates reporter sessions by minute after a bounded,
incarnation-scoped read. Response-time fields are removed from the new payload.

API 92 makes `GET /serve/{service}/demand` the canonical current-activity read
and adds an `async_request_summary` object containing the PostgreSQL observation
time, source, exact unique
counts by ledger state, operational terminal-receipt total, terminal-receipt
counts by status, and protocol coverage. Caller opt-in means exact rows are only
`partial` until a separate complete-producer-cohort proof exists. A partial or
unavailable exact summary never replaces the legacy processing, queue-depth, or
request-rate telemetry; its counts are labelled
`protocol-covered (partial)`. Only `coverage: complete` may promote exact totals
to the primary request card. `ACCEPTED` is not relabelled as confirmed
processing, and `AMBIGUOUS` remains separate from accepted/in-flight and
terminal. Protocol 1 has no heartbeat signal, so the primary processing count
continues to use the fresh legacy occupancy telemetry. Queue depth continues to
come from the fresh durable LB demand feed because the in-memory service queue
is not replaced by a PostgreSQL queue. GPU/device coverage is verified by the
reserved-capacity and worker-runtime qualification, not inferred from request
ledger rows. The history envelope retains the same exact summary only as a
mixed-version compatibility field; a new current read never falls back to an
older history value after explicitly reporting the ledger unavailable.

The dashboard renders the exact summary's PostgreSQL source, observation age,
and refresh-failure state beside every exact count. If a direct-demand or
history refresh fails, retaining the last known counts is allowed only when
they remain visibly stale; cached exact rows
must never look like a fresh zero or fresh current total.

The dashboard replaces the `Response time` card with `Prediction time`. It
defaults to all predictions and offers All, Succeeded, and Failed filters. The
upper chart shows approximate p50, p95, and p99 by minute. The lower chart shows
the fixed-bucket distribution over the selected range. Empty minutes remain
gaps rather than fabricated zero duration. The explanatory copy distinguishes
synchronous replica service time from model-reported asynchronous processing
time.

## Cost model

The stable-job header short-circuits async action detection. Header-free JSON
requests only parse action envelopes up to 64 KiB, bounding event-loop work;
larger bodies are never parsed for observability. After classification, a
synchronous terminal upstream response performs two monotonic-clock reads, a
fixed-bound lookup, and one integer increment. Async terminal parsing and
completion callbacks copy at most a small bounded JSON object. A body that
cannot be parsed, including one nested deeply enough to exhaust the decoder's
recursion limit, is rejected as a client error and never recorded. No model
input, result, raw request ID, presigned URL, or route URL is persisted.

At steady state, an active load balancer still sends one changed histogram
minute on each 20-second sync. Ledger-qualified async traffic adds one logical
row per unique request plus one bounded attempt row per authorized dispatch and
bounded monotonic updates. Ten thousand current logical requests are
deliberately request-rate-scaled storage. Digest-only identity constrains the
per-row privacy and storage cost, but the
ledger migration has no garbage collector; retained old incarnations continue to consume
request-rate-scaled storage until a separately designed guarded maintenance
migration is implemented.

The Boltz caller additionally retains one encrypted prepared projection per
logical execution. The plaintext ceiling is 1 MiB, so the deliberately
conservative upper bound for the 10,000-request qualification is approximately
10 GiB plus JSON/encryption/index overhead; LZ4 makes the expected scientific
payload footprint materially smaller but is not used for capacity planning.
This is the explicit cost of making mutable pipeline state retry-stable without
placing sensitive bodies in Temporal history. Initial retention is indefinite
and PostgreSQL-authoritative. The small versioned wrapped DEK lives in the same
row; no EFS/PVC, S3 object, or process-local cache participates in prepared-
intent recovery.

## Alternatives considered

Keeping full HTTP latency was rejected because it answers a different product
question and makes queueing look like model execution. Querying Datadog from
the API was rejected because it introduces provider credentials and an
external telemetry dependency into customer-facing history. Per-request
PostgreSQL was originally rejected because storage and privacy exposure scale
with traffic. That decision is superseded for stable asynchronous requests:
exact cross-LB idempotency and safe retry receipts cannot be produced from
aggregates. Hashing raw IDs, excluding payloads and URLs, strict evidence
envelope bounds, and incarnation fencing constrain the per-request cost. A
future guarded old-incarnation retention policy remains an explicit follow-up.
Reusing
`api_requests` was rejected because it would assign API-handler executor,
claim, lease, and queue semantics to model requests. Making the LB connect to
PostgreSQL was rejected because it would broaden data-plane credentials; the
existing stable API Service and LB-sync token remain the only write boundary.
Making the service queue itself a PostgreSQL queue was rejected because it
would create a second scheduler/controller and change latency and failure
semantics. Async occupancy transitions were rejected because busy episodes are
not prediction completions. Changing every model image to return a new sync
duration header remains a possible future precision improvement, but it would
not make this SkyPilot rollout useful for current images.

Keeping the `async_predict` connection open until completion was rejected. It
would turn fast acknowledgements back into multi-minute HTTP requests,
reintroduce proxy timeout and rollout-drain failure modes, and still need the
durable marker after a disconnect. The marker already carries the authoritative
duration and outcome. The platform reports that small terminal event back to
SkyServe while `async_capacity` remains solely responsible for routing
availability.

## Rollout and fix-forward

This rollout is an additive Helm upgrade from released Serve057. It does not
stop traffic, delete or recreate `boltz-l4-fleet`, replace its service hash, add
a migration ceiling, introduce EFS, or require a separate activation restart.

1. Back up PostgreSQL, build one immutable SkyPilot image, and qualify the
   additive Serve057-to-Serve058 migration against real PostgreSQL.
2. Deploy that image with the existing Helm release and preserved values. The
   migration installs the two ledger tables; schema-aware API/controller/LB
   processes then advertise protocol 1. Existing callers still omit opt-in
   headers and continue on the legacy path, so this is a dark deployment.
3. Prove both load-balancer slots, the API server, controllers, executors,
   ordinary traffic, reserved-fill placement, and dashboard history stay
   healthy. Verify the service and existing compatible replicas were retained.
4. Build and qualify the boltz-platform caller that emits the canonical request body,
   retry-stable semantic intent digest, and protocol request headers. Before
   possible send, its no-dispatch prepare activity atomically creates or reads
   the encrypted, append-once PostgreSQL V1 intent and returns the bounded
   handle recorded in workflow history. Prepare requires the authenticated
   capacity response's exact service-name/incarnation tuple; every later exact
   operation presents the expected incarnation header, requires the echoed
   value, targets only the recorded endpoint authority, and refuses a
   recreated or wrong-name service. Its dedicated V1 submit activity loads
   only that row, validates the full response receipt, persists the fence,
   recovers a lost receipt
   through the read-only authenticated endpoint, and includes it in the
   existing completion callback. Automatic retries repeat the same exact
   request identity and intent and rely on the PostgreSQL bind—not Temporal
   attempt or heartbeat state—to prevent another worker send. Every compute-api
   path retains and retries terminal delivery until PostgreSQL acknowledges the exact transition;
   terminalization is not a three-attempt best-effort side effect. The backend
   TIO router rejects SkyPilot and contains no copy of this protocol. Fence or
   drain old platform workers before routing new work through exact mode, and
   record the SkyPilot provider choice before selecting the new exact activity.
5. Activate the test compute-api cohort without a mixed-worker interval. The
   current Temporal deployment has no worker versioning and uses one task
   queue, so first scale its workers to zero and wait for every old Pod to
   terminate, then deploy and start only the qualified image. Existing workflow
   histories retain their patched legacy branch; only workflows first executed
   by the new homogeneous cohort select the newly named exact activity. This is
   a one-time bounded test outage, not a Terraform, Kueue, or shared scheduler
   change.
6. Ramp from a bounded smoke to the 10,000-request qualification. Fix forward
   if a defect appears; do not drop Serve058 tables or roll an old writer across
   the committed schema.

Before caller activation, the dark server feature can be bypassed by continuing
to omit opt-in headers. After caller activation, rollback to an old caller is
unsafe because it can replay after a lost acknowledgement and cannot complete
exact rows. Fix forward instead. Already-bound exact attempts remain in
PostgreSQL and their owning workflows retry exact completion until acknowledged;
the durable tables are never deleted. Do not roll a pre-Serve058 load balancer
into the service or recreate the service while any exact request is nonterminal.

Each new exact submission enters the same atomic server bind path without a
read-before-write transaction. Bind returns the existing current receipt without
dispatch authority for every non-rejected duplicate, so a repeated POST never
authorizes a second worker send. If a request cannot select a provider route,
the LB performs the read-only lookup before recording a durable pre-dispatch
rejection; this preserves prompt lost-ack recovery with zero ready routes. A
valid 409 receipt is a protocol response, not a generic failure: ACCEPTED or
terminal means the outcome was recovered; DISPATCH_MAY_HAVE_OCCURRED or
AMBIGUOUS remains unresolved and never dispatches again; only
REJECTED_PRE_DISPATCH permits a new selected-route bind. Every exact response
includes the protocol advertisement plus attempt UUID, attempt number,
revision, and state. After bind, the LB forwards only the nonsecret attempt
UUID, attempt number, and revision needed to correlate the selected worker
path.

A later terminal reporter presents the request ID, intent digest, attempt UUID,
attempt number, and its last observed revision. For a terminal operation only,
PostgreSQL treats that revision as a minimum: under the same per-request lock it
requires the exact current attempt and intent, rejects a future revision, and
advances the current row. Thus a revision-1 dispatch response is not stranded
after the LB commits ACCEPTED revision 2, and the normal completion path needs
one write rather than a lookup plus a write. A new LB retries the old exact-
revision API contract once after a 409 by resolving the current receipt, so the
change remains safe during a two-version rollout. The LB permits at most 16
ledger HTTP calls at once and at most eight read-only lookups; bind and terminal
writes can use all 16, so a reconciliation burst cannot occupy the whole window.

The exact coverage gate remains independent from protocol activation. Until the
entire producer cohort is proven, the dashboard labels ledger counts partial and
continues to show legacy request processing, queue depth, and rate. It never
turns zero ledger rows into a claim of zero total traffic.

Completion requires the exact release and Helm revision, healthy homogeneous
writers and both LB slots, one synchronous sample, one exact asynchronous
terminal sample over the live HTTP-200 async acknowledgement path, duplicate
terminal delivery producing one completion, PostgreSQL/API/UI agreement, and
the 10,000-request qualification. The load test must show that only durable
pre-dispatch rejections produce a successor dispatch attempt; repeated exact
POSTs for accepted/ambiguous work recover the receipt and never reach a worker
again. Reserved-capacity qualification separately proves every configured
card and location is selectable and every GPU on wider replicas performs work;
the request ledger does not manufacture that evidence.

## Test plan

- Unit-test prediction histogram boundaries, outcome partitioning,
  completion-minute assignment, pruning, acknowledgement races, and stale
  snapshots.
- Test synchronous dispatch timing, retriable and transport failures, async
  acknowledgement exclusion, terminal-status parsing, malformed and oversized
  status bodies, outcome mapping, and request-ID deduplication.
- Test authenticated completion callbacks, malformed and oversized callback
  bodies, duplicate keys, and protocol values whose JSON type is bool or float,
  duplicate and conflicting terminal delivery across LB sessions, and
  retryable persistence failures that do not rerun prediction work. Prove the
  terminal reporter atomically advances from revision 1 through a current
  post-accept revision without weakening the exact attempt fence. Prove a new LB
  falls back once to the old exact-revision API after a 409.
- Test normal sync, old-controller missing acknowledgement, legacy payload
  compatibility, persistence errors, and bounded drain flush.
- Execute migration 023 against PostgreSQL and verify array constraints,
  idempotent upserts, multi-reporter aggregation, incarnation fences, retention,
  and API serialization.
- Execute the one final additive migration directly after Serve057 against
  PostgreSQL and verify digest/envelope
  constraints, route projection and replica-record fencing, every legal and
  illegal state transition, expected-revision and attempt fencing, duplicate
  bind and terminal receipts, old-attempt conflicts, service recreation
  isolation, unconditional logical/attempt delete guards, reciprocal-pointer
  consistency, an insert guard that rejects every non-initial state/revision/
  field shape, and exact status aggregation.
- Prove an atomic bind needs no preceding lookup, a read-only bind lookup creates
  no row, the no-route rejection path recovers every existing state with zero
  ready routes, and the protocol fails closed when the
  Serve058 schema is unavailable. Prove schema058 automatically advertises
  protocol 1 while schema057 suppresses it, without an environment gate.
- Prove the bounded data-plane receipt lookup is authenticated and read-only,
  returns a full validated receipt when found, advertises an exact 404 without
  receipt headers when absent, rejects malformed and duplicate-key bodies, and
  never treats a 404 or old-server no-advertisement as standalone replay
  authority.
- Prove stable job ID and execution request ID are independent, the execution
  header exactly matches canonical duplicate-free `async_predict` JSON, and
  every malformed, duplicate, control-action, over-configured-ceiling, or
  mismatched request has zero ledger side effects. Include a canonical body
  larger than 64 KiB but below the configured 1 MiB ceiling.
- Exercise a crash after bind and before send, route removal during body
  buffering, definite connect failure, ambiguous read/write/timeout failure,
  every upstream non-2xx and configured retriable HTTP response remaining
  ambiguous, an HTTP 200 accepted response, stale terminal callback, two or
  more consecutive definite LB-local pre-dispatch retries, and a
  bind-commit/LB-crash ambiguity. Prove no upstream response or post-connect
  error can authorize replay under that execution request ID.
- Exercise a service update whose current route projection contains compatible
  READY replicas from the newest and previous active versions. Prove each route
  binds with the same route-contract fence and its own selected-worker version,
  while an inactive/mismatched replica version fails closed.
- Test dashboard normalization, approximate quantiles, outcome selection,
  selected-range aggregation, synchronized range selection, empty history, and
  absence of response-time copy and fields. Prove exact snapshot age and
  refresh failure remain visible when cached counts are retained, and that
  unavailable/partial ledger
  coverage preserves legacy live request telemetry and labels exact subset
  counts rather than replacing them with zero.
- Rehearse the additive dark Helm rollout from schema057: prove one migration,
  schema-backed advertisement, old-caller legacy behavior, homogeneous
  Serve058 load-balancer and platform-worker activation gates,
  new-caller/new-server exact receipts, retained service identity and replicas,
  and late old-hash rejection. Prove the gate refuses activation when any old
  load-balancer slot or old exact-activity worker remains. No precursor ceiling,
  owner-cleanup migration, EFS, or second activation deployment participates.
- Run focused Serve and PostgreSQL tests, dashboard tests and production build,
  formatter and type checks, and the complete visible PR CI rollup.
- In boltz-platform, test recursively canonical request bytes and the stable
  semantic intent digest across refreshed presigned URLs, HTTP 200
  `IN_PROGRESS` acceptance, complete receipt parsing, a replay-authorizing
  `REJECTED_PRE_DISPATCH` requiring a matching full body and non-2xx response,
  malformed receipts failing closed, and lost-ack recovery by retrying the same
  request identity and intent. Race concurrent matching POSTs and prove exactly
  one gets dispatch authority; lose the first response and prove an automatic
  activity retry recovers the same receipt without another worker send. Exercise a
  bind-before-send crash and preserve its unresolved fail-closed receipt.
  Mutate pipeline input, feature flags, live provider order, and the configured
  service name after prepare and before an activity retry; prove the retry
  loads the same encrypted append-once V1 projection and sends the same intent
  to the recorded service. Crash prepare after its PostgreSQL insert but before
  its Temporal result and prove the retry recovers that row rather than
  rebuilding from live state. Corrupt, delete, or digest-mismatch the prepared
  row and prove exact submit fails before a receipt lookup or network send.
  Recreate a same-name service after prepare, including a test that reuses the
  old endpoint authority, and prove its different incarnation rejects POST,
  lookup, and completion before ledger access or route selection. A normal
  policy update must retain the incarnation and continue successfully.
  Point the selected deployment at a different service's healthy LB and prove
  prepare rejects the capacity response's service-name mismatch. Change live
  endpoint configuration after prepare and prove submit still targets only the
  recorded authority and never falls back.
  Prove activation is refused for a mixed old LB slot, an old platform worker,
  or a pending service recreation; mandatory exact identity never regains
  legacy spill. Re-run a protocol-1 activity retry on a newer worker binary and
  prove its golden semantic projection and digest remain unchanged; prove a
  semantic change requires a new activity name while the old handler remains
  registered. Rehearse the test-worker scale-to-zero activation and prove no
  old and new worker Pods overlap. Prove provider pinning when live dispatch
  signals change across an activity retry and for every supported
  accelerator-backed SkyPilot route, with no L4-only exact-mode assumption.
  Test successful and failed durable
  completion from every caller path, golden vectors for each V1 projection,
  Temporal history replay compatibility across the current and preceding two
  registered protocol handlers,
  provider routing to the recorded SkyServe service, and indefinite
  at-least-once terminal delivery across an unavailable load balancer. Exercise
  a committed completion whose response headers are lost or mismatched and
  prove read-only recovery without a duplicate/conflicting transition. Exercise
  marker polling beyond both configured local timeouts and prove the workflow
  repolls the same placement without a second submit or provider switch. Cancel
  the parent after exact acceptance and feed malformed/unreadable markers;
  prove the durable non-cancellable observer stays alive, makes no false
  terminal transition, and eventually reports the authoritative marker exactly
  once.

## Verification evidence

Local SkyPilot source qualification completed on 2026-08-23:

- the complete asynchronous-ledger PostgreSQL suite passed against a real
  PostgreSQL 14 server, including an explicit released Serve057-to-Serve058
  upgrade;
- the focused ledger, load-balancer retry/routing, controller-proxy,
  authentication, history/status, and request-metadata contract suites passed,
  including the authenticated receipt-recovery endpoint, positive-only schema
  cache, and caller-mirrored RFC 8785/JCS vectors;
- 159 focused dashboard tests passed, and the optimized production dashboard
  build completed successfully; and
- YAPF, isort, mypy across 979 source files, pylint at 10/10, dashboard ESLint,
  and Prettier passed.

This is source evidence, not rollout evidence. The separate caller change,
additive Helm deployment, live HTTP-200 acknowledgement smoke, duplicate
terminal delivery, PostgreSQL/API/UI agreement, and 10,000-request production
qualification remain open gates.

The API-92 current-demand projection revision was locally qualified on
2026-08-22 with 101 focused backend tests passing, 166 dashboard tests passing,
dashboard lint and Prettier clean, changed-Python Pylint at 10/10, and an
optimized production build. Sixteen
real-PostgreSQL cases were skipped because this environment had neither Docker
nor `SKYPILOT_TEST_POSTGRES_URL`; the included service-row lock-contention
regression therefore remains a required real-PostgreSQL pre-merge gate.
