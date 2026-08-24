# SkyServe prediction-time history

## Status

Accepted for implementation on 2026-07-22 after explicit product approval to
replace full HTTP response-time history with customer-facing prediction time.

Updated 2026-08-24. The reporter-minute histogram is implemented, but its
asynchronous completion count remains explicitly approximate for callers
outside the exact protocol. The PostgreSQL ledger, stable API ingest path,
load-balancer bind/lookup/transition integration, and coverage-aware status/
dashboard summary are implemented. The exact API/ledger path is production-
qualified through the provider-local test caller; visual dashboard rendering
remains source-qualified and was historically exercised, but the final
campaign did not capture a nonzero UI sample. The current homogeneous baseline
is SkyPilot
1.1.1470 / Helm revision 594 on immutable image
`sha256:7e1ef1c2043812073fe45d7c472a346bd679b5a4bbb1b3b8417699c2b7c8f2c0`;
Serve058 is at the live PostgreSQL head and both load-balancer slots are
healthy on that image. Read-only receipt probes and the fresh current-demand
API summary are production proven. The source projects the exact ledger through
the controller-free current-demand read. The first live qualification exposed
an unbounded ledger fan-out: a clean submission performed a pre-bind lookup and
a completion performed a lookup before its terminal write, while lookup bursts
could consume the load balancer's entire API transport pool. PR #1699 removed
those two steady-state reads and bounded the transport. Its three-transaction
steady-state behavior is production proven. PR #1700 then removed harmless
route-publication churn from exact pre-bind conflicts while preserving a typed
fail-closed retry boundary. It merged at
`9552669c0bbcbea9d2ee331569dc69fa4c7f0196`, is deployed in 1.1.1470, and passed
a guarded 10,000-request production campaign with exactly 10,000 terminal
successes, zero non-200 prediction responses, zero receipt-recovery lookups,
and no nonterminal attempts. Full Platform application producer activation is
not claimed. Adversarial
review of the separate Boltz
caller found that caller-maintained first-attempt and heartbeat authority was
both more complicated and less reliable than the already-transactional server
boundary. The corrected contract below makes the stable exact request identity
and intent digest the retry-idempotency boundary, while requiring homogeneous
server and provider-local activation and a stable service incarnation. Any
Boltz-side work is restricted to `ml_models/providers/skypilot/**`; Platform
application, common-backend, and Temporal changes are out of scope and are not
claimed by this design. The
ledger is the single additive Serve058 migration directly after released
Serve057; the unrelated draft immutable-owner cleanup and sequencing migration
are not part of this feature.

Serve058 support is selected by schema presence and has no separate environment
flag or second activation restart. The protocol is request-opt-in: callers that
omit the exact protocol headers keep the legacy path, while a caller that
declares protocol 1 must receive and validate a complete protocol-1 receipt or
fail closed. The provider-local Boltz qualification caller is proven; full
Platform application producer integration remains a separate out-of-scope
rollout. The protocol uses the existing `POST /v1/models/model:predict` request
and HTTP 200 `IN_PROGRESS` acknowledgement; it does not require a v2 model
endpoint, a worker-specific HMAC, EFS, or a service recreation.

## Problem

Before this design, the service dashboard labeled and charted full SkyServe
HTTP completion time. That value included authentication, load-balancer queueing, replica
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

The exact request identity and semantic intent digest are the server-side retry-
idempotency boundary. Every exact POST, receipt lookup, and completion carries
the expected service incarnation; the load balancer requires that value to
match its immutable service hash before ledger access or route selection. Its
PostgreSQL logical-request lock serializes concurrent binders: exactly one fresh
bind has `dispatch_authorized: true`, while every matching duplicate returns the
existing receipt and performs no worker send. A current
`REJECTED_PRE_DISPATCH` receipt is the only state from which the same logical
request may append a successor transport attempt. A crash after bind and before
worker transport deliberately remains `DISPATCH_MAY_HAVE_OCCURRED`; a matching
retry recovers that receipt and does not send again. This provides at-most-once
dispatch, not guaranteed execution.

For the production qualification cohort, the provider-local harness freezes the
canonical body, request ID, semantic intent digest, service incarnation, and
endpoint before first send and reuses that immutable preimage for recovery. Its
mode-0600 manifest and event journal are qualification evidence, not a new
Platform database or runtime authority. No Platform application, common-
backend, Temporal activity/workflow, or worker-drain contract is introduced by
this design. Full application-producer adoption would require separate product
authorization and a separate design; until then, exact ledger coverage remains
explicitly partial.

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
service hash, fresh route head, selected projection, advertised URL identity,
and matching replica record before storing this envelope. The private route
identity therefore carries the selected replica version. The route-contract
version must still match the current projection contract, while the replica row
and its zero-cost admission are checked against the selected-worker version.
This preserves routing to compatible previous active versions during a normal
service update instead of falsely stamping every route as the newest version.

The exact bind does not require selected immutable generation G to remain the
head by the time PostgreSQL commits. It validates G's supplied digest,
incarnation, supported producer, complete payload, and advertised non-alias
selected URL before reading fresh current head H in the same transaction.
Within one validated owner lineage, `H.generation < G.generation` is corruption
and returns unavailable. When G differs from a monotonic H, the bind may
continue only if
`{service_version, complete routing_spec, selected URL exact public wire,
selected URL exact private identity}` is identical. Capacity hints and
unrelated route additions, removals, or identity churn are excluded. The full
routing spec remains included because it owns compatibility, queueing, and
admission; same-version routing-spec drift is corruption. Identity/wire
derivation and the durable `dispatch_binding` always use H, followed by current
replica, active-version, record, cost, location, and worker-admission checks.

A missing/pruned G, expired head, normal service/controller/epoch movement, or
selected-route movement raises the additive machine code
`route_authority_changed_before_bind.v1` only when the request-key lock proves
that no request/attempt row exists and before any insert or provider send. A
retained G with the wrong digest, a generic 409, any existing or rejected
attempt, an invalid selected URL, a regressed head, and ambiguous/post-send
state never carry reselection authority.

The LB recognizes only HTTP 409 plus that exact code. It captures both the sync
generation used for selection and the current sync generation when the typed
409 is observed. A coalesced route-only sync does not drain or acknowledge
demand/history, and the body preimage is reused unchanged. A higher source
epoch or same-epoch/higher generation can be reselected after a complete sync
newer than selection. An identical generation/digest is valid only after a
complete coherent apply strictly newer than the conflict-observed generation,
which covers an expired lease renewed in place without trusting an older
pre-conflict sync. Lower fences and equal-generation/different-digest responses
fail closed. Concurrent waiters share success and failure, URL health is not
penalized, and three typed no-send refreshes are bounded independently from the
provider `max_retries` budget.

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

### Provider-local qualification state

This design adds no Boltz Platform table. The qualification harness writes one
private mode-0600 manifest plus an append-only event journal under a mode-0700
evidence directory. The manifest freezes the service incarnation, endpoint,
request identities, intent digests, stable object identities, and bounded
dispatch options used by that campaign. Authentication tokens, encryption keys,
signed URLs, nonces, and ciphertext are not copied into the checked evidence
provenance. This state exists only to make the provider-local test repeatable;
SkyServe's PostgreSQL ledger remains the sole server dispatch authority.

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

The route-authority correction is additive across mixed versions. An old API
does not emit the machine code, so a new LB treats its human 409 as generic. An
old LB ignores the new response field and retains generic failure. Only a new
API plus new LB performs route refresh/reselection, and an incomplete, legacy,
or malformed sync response cannot satisfy its exact projection/ledger fence.
The provider retry budget and provider send ordering are unchanged.

The provider-local qualification harness opts in on the existing
`POST /v1/models/model:predict` call with protocol 1, a bounded request ID, a
bounded stable job ID, and an immutable intent digest. The body is recursively
canonical, duplicate-free RFC 8785/JCS JSON and stays below the service's 1 MiB
request-queue ceiling. The existing worker may return any 2xx acknowledgement,
including the live HTTP 200 `{request_id, status: "IN_PROGRESS"}` response.

The harness's execution ID and intent are retry-stable qualification
identities. Its digest covers model inputs, options, stable object identities,
the selected service, and request ID while excluding expiring transport
credentials. A retry reuses the same frozen request preimage; the PostgreSQL
bind decides whether it may dispatch or must return the existing receipt. A
semantic change requires a new execution ID and digest. This opt-in does not
change Platform provider selection, fallback, Temporal history, cancellation,
or worker deployment.

A local marker-poll timeout is not permission for the harness to blindly
resubmit an `ACCEPTED` or ambiguous attempt. It retains the same request
identity and receipt, uses the authenticated read-only lookup for lost-ack
adjudication, and reports terminal state only from the existing marker/result
authority. An unreadable marker remains unresolved instead of becoming a
fabricated terminal result. Any broader production cancellation or expiry
contract is separate, unauthorized Platform application work.

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

The provider-local harness retains bounded private manifest/event evidence for
each qualification campaign. That operator evidence is not a production
database, has no online authority, and contains no authentication token,
encryption key, signed URL, nonce, or ciphertext in its checked provenance.
SkyServe's PostgreSQL request/attempt rows are the only request-rate-scaled
central storage added by this design; no EFS/PVC or Platform table participates.

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

The completed rollout was an additive Helm upgrade from released Serve057. It
did not stop traffic, delete or recreate `boltz-l4-fleet`, replace its service
hash, add a migration ceiling, introduce EFS, or require a separate activation
restart.

1. **Complete:** back up PostgreSQL, build one immutable SkyPilot image, and
   qualify the additive Serve057-to-Serve058 migration against real PostgreSQL.
2. **Complete:** deploy that image with the existing Helm release and preserved
   values. Schema-aware API/controller/LB processes advertise protocol 1 while
   callers that omit opt-in headers continue on the legacy path.
3. **Complete:** prove both load-balancer slots, the API server, controllers,
   executors, ordinary traffic, reserved-fill placement, and dashboard history
   remain healthy.
4. **Complete for the provider-local qualification cohort:** build the exact
   caller only under `ml_models/providers/skypilot/**`. It freezes canonical
   request identity, intent digest, service incarnation, endpoint authority,
   and protocol headers and relies on the PostgreSQL bind—not caller retry or
   heartbeat state—to prevent another worker send. No Platform application,
   common-backend, router, or Temporal workflow/worker code is changed.
5. **Complete:** run the provider-local caller directly against the elected
   service. No compute-api deployment, Temporal worker drain, or mixed Platform
   cohort is part of this qualification.
6. **Complete:** ramp from 1 to 100, 1,000, and 10,000 requests and reconcile
   every terminal receipt. Fix forward on the server; never drop Serve058
   tables or roll an old writer across the committed schema.

Before caller activation, the dark server feature can be bypassed by continuing
to omit opt-in headers. After caller activation, rollback to an old caller is
unsafe because it can replay after a lost acknowledgement and cannot complete
exact rows. Fix forward instead. Already-bound exact attempts remain in
PostgreSQL and their callers or terminal reporters retry exact completion until
acknowledged; the durable tables are never deleted. Do not roll a pre-Serve058 load balancer
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

The provider-local qualification gate is complete: release 1.1.1470 / Helm
revision 594 ran healthy homogeneous writers and both LB slots, the live exact
path produced terminal samples over HTTP-200 asynchronous acknowledgements,
and the final PostgreSQL attempt census increased by exactly 10,000
`SUCCEEDED` rows with zero nonterminal attempts. The campaign sent no successor
dispatch for accepted or ambiguous work and needed no receipt-recovery lookup.
The current-demand API projection remained `fresh`/`complete` with two
reporters, zero request QPS, zero queued, zero in-flight, and zero rejected
requests after the campaign. Its PostgreSQL exact summary remained explicitly
`partial` because only the provider-local cohort speaks the protocol; it showed
12,103 `SUCCEEDED`, two historical `FAILED`, 156 historical
`REJECTED_PRE_DISPATCH`, and no accepted/ambiguous/nonterminal tail. Full
Platform application producer activation remains a separate non-goal.
Reserved-capacity qualification separately proves every configured card and
location is selectable and every GPU on wider replicas performs work; the
request ledger does not manufacture that evidence.

The private 10,000-request manifest and event journal are content-addressed by
SHA-256
`f751f71dd8f9852c5de2df578a1141e5cdcf38018e7a1e389c40a652dd837398`
and `0bb00d1cdcd00e22437cbd027debcdc521a52d20af3232515b9df534b73accd9`,
respectively. They record peak 179 running slots, zero minimum free slots, and
peak queue depth 15. This is authenticated load-balancer/harness evidence; it
does not manufacture a contemporaneous nonzero current-demand API or visual UI
receipt.

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
- Exercise capacity-only and unrelated-route G-to-H movement, selected-route
  wire/identity change and removal, pruned G versus wrong retained digest,
  expired H unable to mask an invalid G digest or selected URL, monotonic
  `H >= G`, same-version routing-spec corruption, producer/controller lineage,
  and expired-head same-generation renewal. Prove the typed code creates no
  row or provider send; generic/existing/ambiguous outcomes never reselect; 32
  concurrent success, same-fence failure, and exception waiters perform one
  route-only sync; equal-fence freshness is newer than conflict observation;
  request bytes are identical; and `max_retries: 1` still permits exactly one
  fresh bind/provider attempt.
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
  schema-backed advertisement, legacy behavior for callers without opt-in,
  homogeneous Serve058 load balancers, provider-local exact receipts, retained
  service identity and replicas, and late old-hash rejection. No precursor
  ceiling, owner-cleanup migration, EFS, Platform worker activation, or second
  activation deployment participates.
- Run focused Serve and PostgreSQL tests, dashboard tests and production build,
  formatter and type checks, and the complete visible PR CI rollup.
- Full Platform application/common-backend/Temporal producer activation is
  deliberately outside this plan and requires separately authorized design and
  review. The current qualification must not add or test such code. The
  provider-local harness instead covers canonical request identity, stable
  intent digest, complete receipt parsing, same-identity lost-ack recovery,
  service-incarnation/endpoint fencing, and exact terminal reconciliation
  directly against SkyServe.

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

The source evidence is now supplemented by the revision-594 rollout: the
provider-local caller, homogeneous Helm deployment, live HTTP-200
acknowledgement path, PostgreSQL terminal census, post-campaign fresh/complete
current-demand API projection, and 10,000-request production qualification all
passed. Duplicate/late terminal delivery remains a source and real-PostgreSQL
idempotency proof. The rollout did not capture a nonzero visual dashboard
sample during the campaign, and the exact summary correctly remains protocol-
covered/partial rather than claiming full producer coverage. Full Platform
application producer activation is a separate non-goal.

At its 2026-08-22 local checkpoint, the API-92 current-demand projection had
101 focused backend tests passing, 166 dashboard tests passing, dashboard lint
and Prettier clean, changed-Python Pylint at 10/10, and an optimized production
build. Sixteen real-PostgreSQL cases were unavailable in that particular
environment; the complete asynchronous-ledger PostgreSQL run recorded above
subsequently supplied the required real-PostgreSQL qualification before the
production rollout.
