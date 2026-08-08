# Production-Grade Multi-Replica API Server

Status: M0-M4 merged in PR #1070 and live-accepted on the isolated deployment;
the split-role metrics-completeness correction is merged, Rainier's PostgreSQL
request-store cutover is complete, and the typed RWX authority-fence verifier
plus desired-scale role disruption budgets are implemented and statically
accepted; the private durable HA-observer canary contract is specified but its
implementation and independent acceptance remain pending; the Rainier RWX
storage deployment, role-split HA, and live scrape acceptance are pending;
production fleet rollout and M5 compatibility cleanup remain fleet-gated; the
Review-29 operator-authorization correction is accepted and its implementation
remains pending

Last updated: 2026-08-08

Canonical owner: this file. External plans and pull request descriptions must
link here rather than restating a divergent contract.

## Summary

SkyPilot currently supports an external PostgreSQL database and an experimental
RollingUpdate API Deployment, but the API pod is not stateless. Each pod still
owns a local request database, an in-process queue manager, request executors,
background daemons, controller supervisors, request logs, and upload staging
state. Running more than one replica therefore risks split ownership and does
not provide production-grade availability.

This design separates three responsibilities:

1. Stateless HTTP API replicas accept, authenticate, validate, and durably
   enqueue requests.
2. Active-active executor workers claim durable requests from PostgreSQL and
   run request subprocesses.
3. Active-standby controller workers supervise singleton managed-jobs and
   SkyServe control loops under PostgreSQL-backed leadership and fencing.

PostgreSQL is authoritative for request state, queue delivery, ownership,
leases, and schema compatibility. A ReadWriteMany filesystem is the first
production artifact and log backend. The existing blob and log provider
interfaces leave object storage as a future backend without coupling the HA
control-plane migration to one cloud.

The test deployment targets Kubernetes context `boltz-test`, which is an alias
of `boltz-platform-test-eks-cluster`. It uses a dedicated namespace and Helm
release named `skypilot-ha`; it must not modify the existing `test` namespace or
the shared `gitops-hub-rainier` SkyPilot release.

The 2026-08-04 Rainier preflight originally found an all-role 1.1.1084 pod on
SQLite and a 200-Gi `gp2` `ReadWriteOnce` claim. Rainier subsequently completed
the one-way PostgreSQL request-store cutover in release 1.1.1089. That database
cutover is historical input to this design, not work for the storage migration
to repeat.

A read-only production re-audit on 2026-08-08 at 06:10 UTC found Helm revision
370 running 1.1.1166 at commit
`606b4b29703dd2a6e69f57e49db685e85a3c6468` with a healthy single all-role
pod. The image is pinned to
`sha256:ad1fe699b9b940d669f6161cafcd1d719a5d8e4742572854adc9a7b5bf0c2013`
and the chart to
`sha256:520ffca476dfcdeb8b10a90ce3403a956e9035dc4aeeac3f261951695a7c84e4`.
The pod explicitly uses PostgreSQL and the durable cutover gate, but still uses
`Recreate` and the same 200-Gi `gp2` `ReadWriteOnce` claim. Its exact EBS volume
is unencrypted and had no snapshot at audit time.
The cluster has three on-demand `m6i.8xlarge` nodes, one in each zone, and has
no EFS filesystem, EFS CSI add-on, or EFS Pod Identity association.
The request-store prerequisite and guarded rollout implementation are therefore
complete; the RWX state migration, capacity expansion, role split, and live
acceptance remain pending. Changing only the Deployment strategy remains
invalid and the chart correctly rejects it.

That preflight also records the operational reason to complete the migration.
One Recreate upgrade produced a 94-second interval with no Ready API pod, and a
later redundant Recreate produced an 88-second interval. An atomic rollback to
a pre-guarded SQLite revision also failed after that revision's regular
migration Job had been removed. The isolated HA rollback result remains valid:
its source and target revisions both use PostgreSQL, additive schemas, and the
blocking revision-scoped hook. Production must never interpret native rollback
to a pre-M1 SQLite revision as a supported recovery mechanism.

## Why the Existing RollingUpdate Path Is Insufficient

The current code has useful foundations:

- Central state can use PostgreSQL.
- Helm can run a separate database migration Job.
- API pods can verify schema revision instead of running migrations.
- Cluster operations and consolidated controllers use PostgreSQL advisory
  locks in several critical paths.
- Serve controllers persist controller IP and port ownership.
- File mounts and logs can be placed on ReadWriteMany storage.

The remaining blockers are architectural:

- `sky.server.server` starts the queue manager, executors, retention loops,
  controller daemons, and Uvicorn in one supervisor.
- `SqliteRequestBackend` and the default queue are pod-local.
- FastAPI lifespan startup schedules internal daemons in every API process.
- Cancellation targets a local process ID and cannot reliably reach an
  executor on another pod.
- `LocalFilesystemBlobStorage.reset_on_startup()` deletes client state that
  another replica may still be using.
- Sticky ingress sessions reduce routing failures but do not make uploads,
  logs, or request lookup replica-independent.
- Some Serve recovery paths reconstruct a directory but still retain
  compatibility with persisted scripts and pod-local control files.
- The chart explicitly states that replicas greater than one are not well
  tested and defaults to a one-replica Recreate strategy.

The closed upstream PR `skypilot-org/skypilot#6192` is a useful prototype but
not an implementation base. It combined request persistence, locking, request
forwarding, sticky routing, and chart changes in one large change. Reviewers
requested independently reversible changes, and its upload correctness still
depended on affinity. This design instead makes every request endpoint
replica-independent and keeps each migration milestone deployable.

## Goals

- Keep ordinary API requests available while an API pod is deleted, drained,
  or replaced.
- Ensure a request has one live execution owner at a time.
- Never automatically replay a mutating request after ambiguous owner loss.
- Keep running managed jobs and SkyServe services under controller-pod loss.
- Promote a standby controller worker without concurrent unfenced control
  loops.
- Run database migrations exactly once before new replicas become ready.
- Support a version-skew window during rolling upgrades through additive,
  expand-first schema changes.
- Preserve request lookup, cancellation, logs, and uploads regardless of which
  API replica receives the follow-up request.
- Provide independently deployable and independently revertible stacked
  commits.
- End each test-cluster iteration with no failed Helm release, no stale
  migration Job, no stuck terminating pod, and no orphaned test workload.

## Non-Goals

- Replacing Datadog. Existing Datadog collection remains the metrics and
  tracing backend.
- Making the test PostgreSQL instance highly available. The production
  contract requires managed or otherwise highly available PostgreSQL; the test
  cluster may use an isolated single-instance PostgreSQL server to exercise API
  and worker failover.
- Exactly-once cloud-provider side effects after an unknowable network
  partition. SkyPilot instead refuses automatic replay when execution outcome
  is ambiguous and reconciles authoritative state before an operator or client
  retries.
- Migrating local controller databases that officially continue to support
  SQLite.
- Requiring an object store in the first rollout. The storage provider
  interface remains the seam for that follow-up.

## Behavior Contract

### Availability

- At least two API replicas are Ready before HA mode is considered healthy.
- Gracefully deleting any one Ready API pod while issuing short authenticated
  requests through the Kubernetes Service must produce zero HTTP 5xx responses
  and zero transport failures.
- A hard pod or node crash may break an already established transport
  connection. The supported client retry policy must hide that failure for
  retry-safe short requests by reusing the same client request ID. A hard crash
  must not create a duplicate durable request or execution generation.
- A rolling upgrade with `maxUnavailable: 0` must retain at least one Ready API
  endpoint throughout the rollout.
- Existing long-lived HTTP or WebSocket streams may reconnect using the same
  request ID. Their underlying request and log state must remain available.
- API readiness is false when PostgreSQL is unreachable, the request schema is
  incompatible, or shared storage cannot perform its read-write sentinel
  check.
- SIGTERM makes the pod fail readiness before it begins application shutdown.
  A pre-stop drain interval allows EndpointSlice and kube-proxy state to
  converge before Uvicorn exits.
- The zero-error conformance probe runs from an in-cluster canary against the
  ClusterIP Service. Port-forward is not availability evidence because it pins
  a connection to one pod.

### Request ownership

- Request creation and queue insertion occur in one PostgreSQL transaction.
- The handler registry, not the caller, assigns each request an immutable
  execution class. The initial classes are `normal` and `controller`.
- Preconditions are durable queue state. The API does not wait for a
  precondition in an in-memory background task before enqueueing.
- A queue row can be claimed only with `SELECT ... FOR UPDATE SKIP LOCKED`.
- A worker evaluates the durable precondition before claiming an execution
  generation. An unmet precondition advances `available_at` and leaves the
  request queued. Timeout or an invalid ownership fence terminalizes the
  request transactionally.
- Claiming increments a monotonically increasing execution generation and
  records a random claim token, worker instance ID, and lease deadline.
- Every status, result, error, heartbeat, and terminal update from an executor
  compares the same request ID, execution generation, and claim token.
- A stale executor cannot commit after ownership changes.
- A live claim is never concurrently assigned to another worker.
- On worker loss:
  - Read-only and explicitly replayable requests may return to the queue only
    after request-specific reconciliation.
  - Mutating requests with an ambiguous outcome become terminal `CANCELLED`
    rows with `should_retry=true` and a durable `interrupted_reason`. This is
    the semantic interrupted state without adding a status value that older
    clients cannot decode. They are not automatically executed again.
  - A launch is replayed only after the existing cluster-state reconciliation
    proves it remains safe.
- API cancellation first records durable cancellation intent. The owning
  worker observes it, signals only the matching local generation, and records
  acknowledgement. Cancellation never sends a signal to a PID without also
  matching the worker instance and execution generation.

### Durable request format

- New PostgreSQL rows never store an arbitrary pickled callable.
- A stable handler name resolves through a server-side handler registry.
  Built-in handlers register in core; plugins register their own stable names
  during MAIN and EXECUTOR plugin loading.
- Each handler registration declares its payload type, supported payload
  versions, execution class, replay policy, and cancellation policy. A request
  cannot override this metadata in its payload.
- Request bodies use a versioned envelope containing payload type, payload
  schema version, producer server version, and Pydantic JSON.
- An executor advertises the producer and payload versions it can decode. It
  skips incompatible rows instead of claiming and corrupting them.
- Each release decodes at least the previous supported producer version during
  the rolling-upgrade window.
- Handler or payload renames keep aliases until the producer version that used
  the old name is outside the rollback window.
- Legacy SQLite rows may still contain pickles while the compatibility role
  exists. The one-time cutover importer decodes them only with the trusted
  compatibility image and writes the stable PostgreSQL envelope.

### Private HA-observer canary

The cleanup observer must prove that the current controller leader can claim a
new durable request. Ordinary status or health endpoints do not provide an
immutable admission timestamp, an immutable first-claim timestamp, or retry
identity. API revision 74 and additive API-request schema revision 009 therefore
add two private endpoints:

- `POST /internal/ha-observer/v1/canaries` admits or replays one harmless
  controller-class canary; and
- `GET /internal/ha-observer/v1/canaries/{request_id}` returns only its bounded
  evidence projection.

Both endpoints require a valid, revocable SkyPilot service-account token. The
authenticated user must have `user_type=sa`. Its normalized ID and the
deployment-owned `SKYPILOT_HA_OBSERVER_PRINCIPAL_ID` must each match exactly
`^sa-[0-9a-f]{16}$` and compare byte for byte; no case folding, alternate UUID
spelling, whitespace normalization, or merely nonempty value is accepted. The
operator-configured viewer allowlist is global rather than account-scoped, so
it may add only these exact POST and GET routes; endpoint authorization remains
the account boundary because every request independently enforces the exact
configured principal. The routes are absent from the built-in viewer
allowlist. An unset principal, a non-service-account identity, a different
principal, a non-PostgreSQL request backend, or non-HA chart configuration
fails closed. The typed Helm value owns the environment variable;
generic extra environment values cannot override it.

The POST body forbids extra fields and has exactly this canonical value shape:

```json
{"attempt_id":"<canonical-lowercase-UUID>","scheduled_submit_at":"YYYY-MM-DDTHH:MM:00Z","schema_version":1,"slot":"YYYY-MM-DDTHH:MM:00Z"}
```

`slot` and `scheduled_submit_at` must be byte-identical UTC minute starts. The
canonical bytes are UTF-8 JSON with keys sorted lexicographically, no optional
whitespace, and the string/integer encodings shown above. The idempotency digest
is lowercase SHA-256 over `<normalized-principal-id>`, one LF byte, and those
canonical bytes. The request ID is the canonical string form of UUIDv5 namespace
`938aa8b1-a76a-5b69-bd50-f61473b5a85d` with that 64-character digest as its
name. Neither a caller-supplied request ID nor a caller-supplied handler is
accepted.

The first insertion is valid only when PostgreSQL `clock_timestamp()` is in the
half-open interval `[slot, slot + 30 seconds)`. This matches the observer
CronJob's 30-second starting deadline and rejects future, late, or backdated
first admissions. An exact replay by the same principal remains valid after the
window and never inserts or re-enqueues work. The first insert returns HTTP 201;
an exact replay returns HTTP 200 with `replayed=true`. Reuse of the same
`(attempt_id, slot)` by a different principal or body returns 409. One
configured principal may admit at most one request per slot even if it supplies
different attempt UUIDs; that conflict also returns 409. This database-enforced
bound prevents the observer credential from flooding the controller queue
during the 30-second window. A request row without its canary row, a canary row
without the exact request identity, or any other partial/corrupt collision
returns 503 and performs no repair.

Schema 009 adds `api_ha_observer_canaries`, keyed by and cascading from
`api_requests.request_id`, with immutable `principal_id`, `attempt_id`, `slot`,
`scheduled_submit_at`, `idempotency_key_sha256`, and database-clock
`admitted_at`. It has one unique `(attempt_id, slot)` identity across principal
rotations and a second unique `(principal_id, slot)` admission bound. First
admission inserts the request, queue, and canary rows in one PostgreSQL
transaction; a rollback can expose none of them. Nullable `first_claimed_at`,
`first_worker_instance_id`, and `first_controller_generation` fields are
written once in the same transaction that changes the request and queue rows
from queued to claimed. Nullable `terminal_at` is written exactly once from
PostgreSQL `clock_timestamp()` in the same transaction that first changes this
canary request to a terminal status, regardless of whether the terminal path is
success, failure, cancellation, conflict handling, or recovery. A terminal
request with null `terminal_at`, a nonterminal request with nonnull
`terminal_at`, or a later attempt to rewrite it is corrupt and fails closed.
The schema and endpoint enforce causal lower bounds: a claimed terminal row
must satisfy `admitted_at <= first_claimed_at <= terminal_at`; a terminal row
whose entire first-claim tuple is null must satisfy
`admitted_at <= terminal_at`. Migration 009 installs equivalent PostgreSQL
checks: every nonnull first-claim time is at or after admission, every nonnull
terminal time is at or after admission, and when both are nonnull terminal is
at or after first claim. Every POST/GET transaction also projects its own
PostgreSQL `clock_timestamp()` as `observed_at` and fails closed unless every
nonnull evidence timestamp is at or before that database observation time.
Any violation makes GET return 503 and resets the external evidence chain.
These fields are never derived from application clocks, request `created_at`,
request `finished_at`, queue `updated_at`, heartbeats, or logs and never change
on a replay, lease recovery, or leadership handoff. A compatibility all-role
claim may have a null first controller generation; production observation
rejects it.

The schema rollout is a two-release stack. The first compatibility release
does not run migration 009 or expose the observer; it only widens every current
schema-008 Serve/request consumer to accept the known additive 009 head. Live
inventory must prove every API, executor, controller, and compatibility process
runs that release before the second release may execute migration 009 and add
the endpoints. This makes old-application/new-database rollback deliberate
rather than assuming an exact-008 binary can interpret a future Alembic head.
Mixed-version tests cover compatibility code on both 008 and 009; the migration
release remains able to run against retained 009 after application rollback.

The registered canary is a SHORT, CONTROLLER, READ_ONLY-replay-policy request.
Its module-level handler performs no cloud, Kubernetes, filesystem, controller,
or user-state mutation and returns only
`{"outcome":"ok","schema_version":1}`. Terminal replay never creates a new queue
row. The POST response and GET response expose only `schema_version`,
`attempt_id`, `slot`, `request_id`, `idempotency_key_sha256`, `admitted_at`,
`first_claimed_at`, `controller_generation`, `worker_instance_id`,
`terminal_status`, `terminal_at`, and database-clock `observed_at`; POST
additionally exposes `replayed`.
Timestamps use canonical UTC RFC 3339. `terminal_status` is null until terminal
and otherwise is one of `SUCCEEDED`, `FAILED`, or `CANCELLED`.

The request GC enforces a non-configurable two-hour floor for canary request
rows, independent of a lower ordinary `requests_retention_hours` setting;
deletion after that floor cascades to the canary row. The default ordinary
retention remains 24 hours. Missing evidence after the fixed floor resets the
external chain rather than being reconstructed. The S3 chain, not this
operational table, is the 35-day evidence authority.

### Controller ownership

- API replicas never start managed-jobs or SkyServe controller supervisors.
- Controller workers compete for a PostgreSQL advisory leader lock over a
  dedicated session.
- Leader health is checked continuously. Loss of the lock session makes the
  process unready, stops new controller claims, and begins terminating child
  controllers. PostgreSQL can release a failed session before the old process
  exits, so process termination is not treated as the fencing boundary.
- Durable controller ownership includes the worker instance ID and a
  monotonically increasing generation, not only a pod-local PID and IP.
- Controller state commits and reservations for new external mutations compare
  the current generation. A child controller receives the generation at
  startup and must revalidate it before every durable state transition or new
  provider-side action.
- Controller-owned SDK calls carry the server-owned controller instance and
  generation as admission metadata. The API replica proves that exact
  generation still owns both advisory locks before accepting the nested
  request. This check is the admission linearization point: work admitted
  before handoff remains durable, while a fenced child cannot submit fresh
  provider work through the replacement leader. Ordinary clients carry no
  controller metadata; partial, malformed, and stale metadata fail closed.
- A managed-job scheduler claim stores the outer controller instance and
  generation beside its pod-local PID. The claim transaction takes a shared
  lock on the exact live `api_controller_leadership` row, so generation
  advancement either waits for the old claim to commit or fences it before it
  can write. A stale scheduler cannot claim a `WAITING` job after handoff.
- Before a replacement managed-job scheduler starts, recovery waits for old
  detached controllers to drain, resets every nonterminal job owned by another
  generation, and only then launches scheduler processes. Status refresh never
  interprets a PID from another generation as a local controller failure and
  therefore cannot terminalize or tear down that job.
- Stopping a scheduler because its outer controller generation was fenced is a
  fail-stop process exit, not managed-job cancellation. The outer controller
  uses non-catchable termination for each entire detached scheduler process
  tree, not only its recorded parent PID. A scheduler that independently loses
  database proof of its outer generation terminates its isolated process group
  immediately without running job finalizers. Neither path may set
  `CANCELLING`, `CANCELLED`, or `DONE`, revoke the job token, download final
  logs, or tear down workload resources. The PID and process-start timestamp
  are revalidated immediately before forced termination so a stale PID record
  cannot target a reused process. Only the replacement generation may reset
  stale ownership and resume monitoring. User cancellation remains a separate
  durable cancel-intent path and retains graceful cleanup semantics.
- When the replacement scheduler resumes a task that is already `RUNNING`, it
  transitions its outer schedule state from `LAUNCHING` to `ALIVE` before
  entering the monitor loop. Resume deliberately skips the cluster-launch
  context, so that state transition must not depend on a new launch.
- A newly elected leader may coexist briefly with an isolated old process, but
  the old generation cannot reserve or commit new work. An action whose
  provider-side result became ambiguous during lock-session loss is reconciled
  and never automatically repeated.
- Only the elected leader claims requests in the `controller` execution class.
  Its bounded specialized executor pool owns controller-starting subprocesses
  so their lifecycle is tied to leadership.
- Standby workers remain healthy and Ready while not leader.
- Singleton maintenance loops either run under the controller leader or use a
  narrower PostgreSQL advisory lock for each execution.

### Artifacts and logs

- All API and worker pods mount the same ReadWriteMany claim at the same paths.
- Shared uploads use content-addressed final paths, per-blob PostgreSQL
  advisory locks, per-upload unique staging directories, and atomic rename.
- Shared storage startup never wipes another replica's client state.
- Request and controller logs are written to shared paths and can be streamed
  by any API replica.
- Temporary download assembly remains pod-local because it can be regenerated.
- Stored Serve version YAML and submitted YAML in PostgreSQL are authoritative.
  Recovery reconstructs control files from durable rows. A persisted script
  must not be the only copy of a controller input file.

### Migrations and version skew

- In guarded HA mode, Helm runs the migration image as a blocking pre-install
  and pre-upgrade hook. The hook uses the target release image and must
  complete before any Deployment template is applied.
- HA mode requires `apiService.dbConnectionSecretName` to name a Secret that
  exists before Helm starts. A chart-managed `dbConnectionString` Secret cannot
  satisfy a pre-install hook because regular chart resources are created after
  pre-install hooks.
- Migration hook Jobs are revision-scoped and retained until their configured
  TTL so operators can inspect the exact image, logs, and result. A retry of
  the same Helm revision deletes the previous hook Job before creating its
  replacement.
- Direct configuration reconciliation has three separate Helm phases. A
  weighted `pre-install,pre-upgrade` seed Job runs only after the migration
  hook succeeds, commits and reads back the deterministic database generation,
  and then exits. Its digest-pinned image contains the seed code; the
  secret-free desired configuration is canonical JSON embedded directly in
  the Job manifest with a 262,144-byte limit, so no non-Job hook resource can be
  orphaned or deleted before consumption. Regular API/all-role or split-role
  Deployments carry the accepted generation annotation and roll under Helm's
  bounded `--wait`. A distinct `post-install,post-upgrade` verifier Job then
  checks the exact database generation and every topology-selected Deployment
  generation/readiness. Every seed and verifier Job has a diagnostic TTL
  constrained to 86,400--604,800 seconds; successful verifiers additionally use
  `hook-succeeded` for eager cleanup, and retries use
  `before-hook-creation`. Release-managed, least-privilege RBAC permits the
  verifier to read only the exact role Deployments. Tests cover retry, failure,
  TTL cleanup, and uninstall residue.
- API and worker pods run in verify-only migration mode.
- New schema revisions are additive during the expand phase.
- A release may read both the old and new representation while mixed versions
  are running.
- Consumers are upgraded before producers when introducing a new envelope
  version. Independently of rollout order, an API producer computes the common
  supported version from Ready executor and controller instance
  advertisements and does not emit a version that any eligible execution
  class cannot decode.
- A newly introduced handler remains unavailable with a precise compatibility
  error until at least one healthy worker for its execution class advertises
  support. Existing handlers continue using the newest mutually supported
  envelope version.
- Destructive contract migrations occur only after the previous reader version
  is outside the rollback window.
- A pod with an unsupported schema revision remains unready and exits with a
  precise error.

## Runtime Architecture

| Component | Replicas | Serves public API | Claims normal requests | Claims controller requests | Runs singleton controllers |
| --- | ---: | --- | --- | --- | --- |
| API Deployment | 2 or more | Yes | No | No | No |
| Executor Deployment | 2 or more | No | Yes | No | No |
| Controller Deployment | 2 or more | No | No | Leader only | Leader only |
| Migration Job | 1 per Helm revision | No | No | No | No |
| PostgreSQL | External production service | No | Durable authority | Durable authority | Durable authority |
| Shared filesystem | RWX | Upload and log reads | Upload and log writes | Controller files and logs | Controller files and logs |

Splitting executors from controller supervisors is intentional. Active-active
request throughput and active-standby controller ownership have different
failure and scaling semantics. Combining them makes a busy executor rollout
also churn controller leadership.

The compatibility entrypoint keeps `--role=all` while the fleet migrates. HA
mode uses explicit `api`, `executor`, and `controller` roles and fails Helm
rendering if the PostgreSQL or RWX prerequisites are absent.

## PostgreSQL Request Schema

The new PostgreSQL-only `api_requests` Alembic schema contains:

### `api_requests`

- Existing request fields required by `Request`.
- `handler_name TEXT NOT NULL`.
- `payload_type TEXT NOT NULL`.
- `payload_format TEXT NOT NULL`.
- `payload_version INTEGER NOT NULL`.
- `producer_version TEXT NOT NULL`.
- `payload_json JSONB NOT NULL`.
- `execution_class TEXT NOT NULL`.
- `execution_generation BIGINT NOT NULL DEFAULT 0`.
- `claim_token UUID`.
- `worker_instance_id UUID`.
- `lease_expires_at TIMESTAMPTZ`.
- `heartbeat_at TIMESTAMPTZ`.
- `cancel_requested_at TIMESTAMPTZ`.
- `cancel_acknowledged_at TIMESTAMPTZ`.
- `interrupted_reason TEXT`.
- `created_at` and `updated_at` database timestamps.

### `api_request_queue`

- `request_id` as primary key and foreign key with cascade delete.
- `schedule_type`.
- `priority`.
- `available_at`.
- `enqueued_at`.
- Stable ordering sequence.
- Serialized queue flags currently carried in the multiprocessing tuple.
- `precondition_type`.
- `precondition_payload JSONB`.
- `precondition_deadline TIMESTAMPTZ`.
- `precondition_attempts BIGINT NOT NULL DEFAULT 0`.
- `delivery_state TEXT NOT NULL`.
- `claim_generation BIGINT`.

### `api_server_instances`

- `instance_id` UUID primary key.
- `role`.
- `pod_name`, `pod_uid`, and `pod_ip`.
- `version`.
- `started_at`, `heartbeat_at`, and `draining_at`.
- `ready` and role-specific health detail.
- Supported handler and payload-version ranges for that role.

### `api_controller_leadership`

- Singleton controller-role key.
- Monotonically increasing `generation`.
- Current `instance_id`.
- PostgreSQL backend PID and generation-specific advisory-lock key for the
  session that holds both the controller election lock and generation lock.
- `acquired_at`, `heartbeat_at`, and `released_at`.

The elected process advances this row only while holding the dedicated
PostgreSQL advisory lock. It also takes a generation-specific session lock
before committing the new row. A generation is current only when the row is
unreleased and `pg_locks` proves that the recorded backend still holds both
exact granted exclusive advisory locks. The generation lock prevents backend
PID reuse during a handoff from making an old row appear live. The persisted
generation and live session proof together form the fence passed to child
controllers.

### `api_controller_action_reservations`

- Stable logical action ID and resource identity.
- Controller generation and owning instance ID.
- Action type, state, and provider operation identifier when available.
- Created, updated, and reconciliation timestamps.

The current generation reserves a new external mutation transactionally before
calling a provider. A unique logical action cannot be reserved by two
generations. After ambiguous owner loss, the new leader reconciles the
reservation and provider state before deciding whether the action is complete,
failed, or safe to retry.

The queue row remains until the request reaches a terminal state. A claim
transaction locks an eligible queue row, increments the request execution
generation, records the worker, token, and lease on the request, and marks the
queue delivery as claimed with the same generation. Terminalization deletes
the queue row in the same transaction. A reconciled retry changes the existing
row back to queued only after a fenced status transition. This retained
delivery record lets the lease reaper reason about an owner loss without
reconstructing a missing message, and there is no separate message broker to
reconcile with request state.

`ClusterStartCompletePrecondition` and
`ServiceReplicaLaunchPrecondition` become registered durable precondition
types. Their JSON fields include every value currently held in the Python
object. Adding a new precondition requires a stable type name, JSON schema, and
compatibility test.

## Process Refactor

Shared bootstrap moves out of the `if __name__ == '__main__'` block into
testable runtime functions:

- `initialize_common_runtime()` loads plugins, verifies central schemas,
  restores server identity, and initializes permission state.
- `run_api_role()` starts Uvicorn only.
- `run_executor_role()` starts PostgreSQL queue consumers and request
  subprocess pools.
- `run_controller_role()` starts leader election, the controller-class request
  executor, controller supervisors, and singleton maintenance loops.
- `run_all_role()` preserves the current single-process deployment during
  migration.

FastAPI lifespan is limited to API-local tasks such as event-loop monitoring
and version checks. It does not submit controller daemons.

Executor workers register and heartbeat their instance. Shutdown first marks
the instance draining, stops claims, waits for bounded in-flight work, then
fences or interrupts remaining generations.

Controller workers expose an internal health endpoint. Readiness reports
healthy for a standby and reports unhealthy for a leader that loses its lock
or cannot fence its children.

When metrics are enabled, every API, executor, and controller pod starts a
role-local metrics server for its own Prometheus multiprocess registry. The
enablement marker is active only when
`SKY_API_SERVER_METRICS_ENABLED=true`; unset or `false` values do not start a
listener. The metrics server is independent of controller leadership, so
controller standbys remain scrapeable and a newly promoted leader does not
introduce a target gap. Role startup blocks until the metrics listener has
successfully bound, before executor/controller readiness or controller election
can begin.
An unexpected listener exit after startup sends the role its normal termination
signal, closing readiness and causing the Deployment to replace the pod rather
than leaving a Ready target with no metrics. Executor and controller
supervisors, request workers, and their controller subprocesses share one
pod-local `PROMETHEUS_MULTIPROC_DIR`; the directory is created before the
Python runtime imports metric definitions.
After all chart hooks and credential setup, the supervisor entrypoint clears
and recreates that directory as its final pre-exec step: the multiprocess
reaper removes dead-process live gauges but deliberately preserves counters
and histograms, so hook-created or stale files cannot be inherited.
This is required for controller-emitted series such as
`sky_serve_system_oom_recovery_events_total` to reach a scraped endpoint.
Collectors that query shared durable state remain registered only on API
roles, avoiding another copy on every executor and controller target. Plugin
custom collectors keep their historical API/all ownership for the same
reason. A plugin that needs role-local telemetry emits multiprocess-aware
Prometheus metric types into that role's local registry instead of registering
a custom collector on every scrape target.

The handler registry routes Serve lifecycle mutations, managed-jobs operations
that create or supervise consolidated controllers, and internal
controller-maintenance submissions to the `controller` execution class.
Read-only status and log handlers remain `normal` only when they cannot spawn,
refresh, or terminate a controller. Normal executors reject controller-class
rows even if a malformed client attempts to request that routing.
The M1 compatibility `all` role may claim both classes; explicit HA roles
remove that exception.

API shutdown follows this order:

1. Atomically mark the instance draining and fail readiness.
2. Run the configured pre-stop propagation delay.
3. Stop accepting new connections.
4. Drain existing short requests and detach reconnectable streams.
5. Exit before the Kubernetes termination deadline.

## Helm Contract

HA mode introduces:

```yaml
apiService:
  highAvailability:
    enabled: true
  replicas: 2
  upgradeStrategy: RollingUpdate

requestStore:
  backend: postgres

executorService:
  replicas: 2

controllerService:
  replicas: 2

storage:
  enabled: true
  accessMode: ReadWriteMany
  # Optional when infrastructure owns and pre-populates the RWX claim.
  existingClaim: ""

databaseMigration:
  enabled: true
```

The chart enforces:

- An external PostgreSQL connection is configured.
- `requestStore.backend=postgres`. This selector is independent of HA mode so
  M1 can exercise the durable backend with one compatibility pod before roles
  split.
- `apiService.replicas >= 2`.
- Executor and controller replicas are at least two.
- Storage is enabled with `ReadWriteMany`, unless non-local blob and log
  providers are explicitly declared.
- `storage.existingClaim`, when nonempty, selects a claim that infrastructure
  created and populated before the rollout. The chart does not render its
  default claim in that mode. The declared `storage.accessMode` remains part of
  the HA guard, and rollout preflight must separately prove the live referenced
  claim is bound and actually advertises `ReadWriteMany`.
- Guarded HA pins RollingUpdate to zero unavailable replicas
  (`maxUnavailable: 0` or `0%`) and an absolute `maxSurge: 1`.
  Compatibility-mode RollingUpdate retains its existing configurable values,
  but HA rendering fails before producing manifests when either invariant is
  violated. The absolute single surge gives every role a replacement slot
  without allowing percentage rounding or an operator override to multiply its
  per-role temporary capacity. Helm and the post-seed reconciler may roll all
  three Deployments concurrently, so rollout preflight must prove aggregate
  cluster headroom for one surge API, executor, and controller pod (up to three
  temporary pods), not one temporary pod total.
- Guarded API Deployments set `minReadySeconds: 10` and
  `progressDeadlineSeconds: 600`, in addition to the pre-stop drain and a
  termination grace period longer than the drain budget. Ten continuously
  Ready seconds keep a newly started endpoint from immediately consuming the
  old pod's availability slot; the finite progress deadline matches the direct-
  Helm operator artifact's 600-second rollout-status budget.
- API Service selectors match only API pods.
- API, executor, and controller Deployments have distinct labels and commands.
- With `apiService.metrics.enabled=true`, all three Deployments expose the
  configured metrics port, carry the same selected Prometheus discovery
  annotations, and are scraped as independent pod targets. Executor and
  controller health ports must differ from the metrics port.
- Bundled `/gpu-metrics` and `/endpoints-metrics` federation jobs discover the
  API Service's named `metrics` port rather than embedding `9090`, so a custom
  `apiService.metrics.port` is one consistent chart-wide setting.
- Pod anti-affinity or topology-spread constraints avoid placing all replicas
  on one node when the cluster has capacity.
- Role PodDisruptionBudgets use the integer `maxUnavailable: 1`, preserving the
  desired replica count minus one as healthy as a role is deliberately scaled.
  Kubernetes computes this threshold from the owning Deployment's desired
  `.spec.replicas`, not the temporary number of healthy surge pods. At desired
  scale two, a three-healthy-pod surge can therefore report two allowed
  disruptions; this PDB is an availability floor, not a one-eviction surge
  mutex. The Deployment's separate `maxUnavailable: 0` contract governs its
  rolling update.
- Migration hooks finish before Deployments roll.
- The direct-Helm bundle owns a revision-scoped seed hook Job, generation-
  annotated role Deployments, and a distinct post-rollout verifier hook Job;
  Terraform owns none of them. Migration succeeds before
  seed. The seed deep-merges with deployment-owned keys winning, preserves
  runtime-only keys, replaces `workspaces` wholesale, prunes retired keys only
  when explicitly armed, and is idempotent on fresh and existing databases. A
  successful seed restarts and waits for the API, executor, and controller
  Deployments when guarded HA is active, because every split role loads shared
  server configuration in memory. Compatibility mode retains its API-only
  restart and wait. Reconciliation is complete only after every exact named
  Deployment reports a successful rollout within the same bounded 600-second
  per-Deployment budget.
- The chart and direct-Helm artifact own workload naming and reject
  `fullnameOverride`; the fixed release name determines rendered Deployments
  and service accounts. Platform-owned Pod Identity associations use an
  explicit, independently validated service-account name, and the stable
  revision-scoped config-seed hook receives explicit role Deployment names rather than
  inferring them from Terraform-owned Helm values.
- A revision-specific migration Job is removed after success and retained long
  enough on failure for diagnosis.
- The test deployment creates an isolated `skypilot-ha-efs` StorageClass using
  the test cluster's existing EFS CSI provisioner, a dedicated base path, and
  `reclaimPolicy: Delete`. It does not place SkyPilot data under the
  mmseqs-specific base path or leave a retained access point after cleanup.

Sticky ingress affinity defaults to false in HA mode. Follow-up operations use
PostgreSQL request IDs and shared artifacts, so routing to any API replica is
correct.

## Stacked Implementation and Deployment Plan

Each milestone is one reviewable commit and may become one stacked pull
request. Every milestone is deployed to the isolated test release before the
next begins.

### M0: Canonical design

- Add this design.
- Record the rejected alternatives and deletion map.
- Run adversarial review against this exact file.

Deployment: none, because documentation does not change runtime state.

### M1: PostgreSQL request store and durable queue

- Add the request schema and migration.
- Implement PostgreSQL request storage.
- Implement transactional enqueue and leased queue claims.
- Replace persisted callables and request-body pickles with the stable handler
  registry and versioned JSON envelope.
- Persist and evaluate request preconditions in the queue.
- Implement durable cancellation intent and worker-local acknowledgement.
- Select the PostgreSQL backend explicitly with
  `requestStore.backend=postgres`. M2 makes that selector a mandatory HA
  prerequisite when roles split.
- Keep the current all-in-one role for this milestone.

Implementation status: M1 passed its isolated `skypilot-ha` acceptance gate.
The exact candidate image used PostgreSQL for request state and queue delivery
on RWX storage. Short and long requests, streaming, cancellation
acknowledgement, terminal history across an API pod restart, and daemon lease
reacquisition all passed. A configuration-only rollback to SQLite wrote a
request only to the legacy store, and the release then returned cleanly to the
PostgreSQL compatibility image. The one-way cutover test refused a live
`RUNNING` source row without explicit interruption, imported a mixed legacy
history, queued only eligible rows, converted the interrupted row
deterministically, and produced the same report on an idempotent rerun.
Real-PostgreSQL tests also prove that concurrent first importers serialize to
one fresh import and one idempotent result. The SQLite backend, local queue,
all-in-one role, and pickled legacy rows are deliberately still present for
the rollback window and are listed in the Legacy Code Removal Map below.

The first isolated bootstrap attempt exposed one import-order edge: server
config loading created the config schema before global user state could prove
that the shared PostgreSQL schema was fresh. Explicit `bootstrap` mode now
defers only the import-time database config overlay. The migration job
initializes global user state first and then explicitly initializes the config,
Serve, managed-jobs, and request schemas. A real-PostgreSQL subprocess test
starts from an empty schema and covers the same fresh interpreter import path.
The same live rollout also proved that spawned Uvicorn workers cannot rely on
the supervisor's module-level queue factory. Every process now resolves the
PostgreSQL queue from the explicit request-backend environment, while an
explicitly registered plugin factory retains precedence.

Two additional cutover defects were found by deploying and exercising the
candidate rather than relying on local mocks. First, a frozen SQLite source
must be reopened with SQLite read-only URI semantics for idempotent
verification. Second, interrupting a frozen `RUNNING` row must reuse the
committed database cutover timestamp, otherwise each rerun changes
`finished_at` and therefore the logical hash. The importer now uses a
transaction-scoped PostgreSQL advisory lock for the absent-marker race, the
database clock for the initial cutover timestamp, and the marker timestamp for
all reruns.

Deployment:

1. Install isolated PostgreSQL and RWX storage in `skypilot-ha`. PostgreSQL is
   a dedicated test dependency in the namespace with a dynamically provisioned
   PVC whose reclaim policy is Delete. Production values still require an
   external highly available PostgreSQL service.
2. Deploy one all-role pod with `requestStore.backend=postgres`.
3. Submit, query, cancel, and stream representative short and long requests.
4. Restart the pod and prove queued and completed rows remain available.
5. Exercise the explicit cutover importer with seeded legacy SQLite rows.
6. Roll back configuration before cutover, and after cutover roll back only to
   the M1 compatibility image that understands PostgreSQL.

Rainier production uses the same logical boundary with independently managed
storage and an explicit application/infrastructure ownership split.

Before any migration plan or application artifact is saved, operators first
land the SkyPilot direct-deployment bundle and its replacement config-seed
mechanism, then freeze the live release and infrastructure state. A locked
state inspection must find exactly these four root application addresses:
`helm_release.skypilot`, `kubernetes_config_map_v1.seed_config`,
`kubernetes_job_v1.seed_config`, and
`terraform_data.reconcile_api_server`; any mismatch stops the handoff. Because
Terragrunt downloads the SkyPilot control-plane module as the root, one
immutable SkyPilot module revision requires language version 1.7 or newer,
deletes the four resource blocks, and contains a permanent `removed` block with
`lifecycle { destroy = false }` for each address. Platform-generated sibling
tombstones are forbidden: they conflict with the predecessor declarations and
can disappear on a later pin. A repository guard makes the four module-root
tombstones permanent. The Rainier unit switches to that infrastructure-only
revision. Its saved Terragrunt/OpenTofu plan
must forget exactly those four addresses with zero Helm, Kubernetes, or AWS
mutation. A human using the approved non-admin deployment identity applies it
once and proves all four state addresses are absent while the live release,
values, manifest, revision, Deployment UID, pod UIDs, and database-config raw
and canonical digests are unchanged; a second platform plan must be zero-change.
The four tombstones remain permanently. The forgotten seed ConfigMap and Job
remain inert until a later direct-Helm cleanup proves replacement-seed parity.

The immutable SkyPilot commit is shared by the Rainier control plane,
research-production EKS pool, research-usw2 spoke-workspace EKS pool, and
multi-tenant AWS-VM unit. The handoff gate therefore includes a separate
reviewed saved plan for all four units: Rainier has exactly the four forgets,
and every other unit has zero managed-resource actions. Output-only changes are
enumerated and cannot mask an action.

After that handoff, `boltz-platform` owns only static infrastructure and
infra-scoped migration helpers through reviewed saved Terragrunt/OpenTofu plans
and human applies, including the evidence store and out-of-band observer
CronJob. It must not declare or import the SkyPilot release, generate
or apply application values, seed application database configuration, restart
SkyPilot Deployments, or mutate chart-owned objects. A SkyPilot build, upgrade,
or rollback never requires a platform change or apply.

SkyPilot application runtime is owned exclusively by direct Helm operations
performed by an explicitly authorized operator. Every named release bundle
contains an operation ID and digest, exact chart archive and SHA-256/OCI
provenance, image digest,
secret-free user-values capture, computed all-values audit, complete stage
overlay and target values, rendered-manifest digest, database-head and
placement compatibility, preflight, and fix-forward command. Ordinary changes
use `helm upgrade --reuse-values` against the existing release with a bounded
wait/timeout; production migration stages use neither `--install`, `--atomic`,
nor native `helm rollback`. A `--reset-values -f <complete-target-values>`
operation is allowed only when the complete render and named retired-key diff
are reviewed. Application and infrastructure stages may consume each other's
accepted evidence, but neither tool may mutate resources owned by the other.

H0's bundle also sets
`configReconciliation.handoffGuard.expectedRawConfigSha256` over the exact live
`config_yaml.api_server_config` text and `expectedConfigSha256` over its
canonical decoded JSON. Both 64-character lowercase digests are required
together. The `requiredPaths` string array contains `/gcp/vpc_name`,
`/aws/ingress_source_ranges`, and `/kubernetes/allowed_contexts` plus an exact,
exclusive enumeration of every live workspace. For workspace name `N`, form
RFC 6901 component `E(N)` by escaping `~` as `~0` and `/` as `~1`. A disabled
workspace contributes only
`/workspaces/E(N)/kubernetes/disabled`, whose value must be the JSON Boolean
`true`; an enabled workspace contributes exactly both
`/workspaces/E(N)/kubernetes/namespace` and
`/workspaces/E(N)/kubernetes/allowed_contexts`. Missing, extra, mixed, or
incomplete workspace coverage fails closed. The two row hashes bind the exact
pointed values; every pointer must also resolve. The seed Job
must match that captured row before mutation, require its deterministic merge
to be a complete config no-op, write only the separate seed-generation row,
and prove the raw bytes, canonical digest, and required values unchanged after
commit. The post-hook repeats both raw and canonical digests plus required-path
checks in a new read-only transaction. Missing or changed security paths fail
closed; a new generation marker alone is never handoff parity evidence.

This transition supports only the built-in configuration schema. Every enabled
operation sets literal `configReconciliation.pluginsUnsupported: true`, which
is included in the generation, after proving Rainier has no configured API
plugin schema. The helper rejects a nonempty stored top-level `plugins` value
and validates the complete row with strict built-in unknown-field rejection. It
does not import or install plugins, and a row containing plugin-provided fields
fails closed instead of being partially preserved.

The guard remains persisted through phase 0 and no-pod-change H2/H0-C
operations. Clearing it changes the seed generation, so only a later reviewed
application operation whose contract already permits a generation rollout may
clear both hashes and the path list. It never causes an otherwise forbidden
rollout merely to remove handoff evidence.

Stable cluster policy may remain platform-owned only when it does not encode a
SkyPilot release, chart, image, or template identity. The direct-Helm bundle or
another application-owned object owns every exact old/new template-digest
handoff. A stable platform admission engine may validate that object's narrow
schema and enforce the hard quota, but changing a SkyPilot application artifact
must never require a platform PR or apply.

An emergency application or infrastructure change invalidates every dependent
saved artifact and continuous observation clock. Operators first recapture the
live state, then regenerate and review the affected Helm artifacts and
Terragrunt plans before the migration resumes. No application runtime state is
backfilled into Terraform.

1. Provision a dedicated rotation-enabled, `prevent_destroy` SkyPilot-state
   KMS key, the EFS driver, an encrypted General Purpose filesystem with
   Elastic Throughput, a dedicated daily AWS Backup plan and vault with at least
   35 days of retention, and one mount target per availability zone. Create two
   Terraform-owned static access points in the cluster unit and statically
   bound RWX PV/PVC pairs in the dedicated
   `deployment/terragrunt/environments/gitops-hub-rainier/skypilot-kubernetes-infrastructure`
   unit: the application state root and a distinct authority root. The state
   access point is owned by UID/GID 0 so migration can preserve existing root-owned
   `.sky` and `.ssh` metadata. Workloads mount state read-write and authority
   read-only; they cannot reach the authority directory through the state
   access point. Only migration Jobs ever mount authority read-write, and their
   identity and RBAC are removed before application restart. Dynamic
   access-point UID/GID allocation is not part of production. Protect the
   filesystem, access points, backup and filesystem policies, PVs, and claims
   from accidental destroy.
2. Add `helm.sh/resource-policy: keep` to the legacy release-owned claim in a
   no-pod-change direct-Helm bundle and prove the API Deployment and pod
   identities did not change. Only after that application operation is accepted
   may platform IaC assume durable ownership of the kept claim
   before the chart stops rendering it; the annotation alone only orphans the
   object. The imported declaration must reproduce the exact live Helm labels
   and annotations so Terraform does not strip Helm ownership metadata. The
   exact legacy PV must be adopted with `Retain` before any cutover can orphan
   the claim.
3. Start a unique cutover-attempt generation and run an explicitly armed,
   non-authoritative online preseed from the live RWO
   source to the inactive RWX target. Bound both copy and verification I/O,
   retain aggregate path-free evidence, and treat source churn as expected
   rather than as a reason to mutate request state. Verify ownership, modes,
   symlinks, hashes, and usable capacity before selecting the target.
   In the same reviewed Terraform stage, complete and retain an online baseline
   EBS snapshot and an encrypted copy under the SkyPilot-state key. This primes
   the incremental snapshot chains before downtime; it is useful recovery
   evidence but cannot replace the quiesced snapshots in the final gate. The
   preseed emits canonical, path-free evidence containing its manifest SHA-256,
   entry count, byte count, completion time, and source and target identities.
   That evidence and both baseline snapshot IDs are retained for the attempt;
   neither an unbound copy nor an unbound baseline can satisfy a later gate.
   `storage.existingClaim` only selects a claim; it deliberately does not copy
   data or infer that copying was safe.
4. Use a reviewed direct-Helm quiesce artifact to drain API traffic, explicitly
   resolve active requests under the existing PostgreSQL interruption contract,
   and scale the all-role API Deployment to zero. That Helm revision cannot
   create a snapshot, run a finalizer, select RWX, or restart traffic. After
   Kubernetes observes zero API
   pods and no preseed or application pod mounts either claim, publish one
   digest-sealed attempt intent on the authority access point. Its exact v1
   schema records the positive monotonic `attempt_generation`, observed
   `zero_at`, `work_cutoff` exactly `zero_at + 45 minutes`,
   `api_ready_deadline` exactly `zero_at + 120 minutes`, source PVC
   namespace/name/UID, Helm release namespace/name, source PV name/UID and EBS
   volume ID, target filesystem ID, state access-point ID, state PV name/UID and
   PVC namespace/name/UID, authority access-point ID, and authority PV name/UID
   and PVC namespace/name/UID. It also binds the accepted preseed evidence's
   canonical SHA-256, manifest SHA-256, entry count, byte count, and completion
   time plus both distinct baseline source/encrypted snapshot IDs. A
   generation's intent, evidence, identities, snapshots, and deadlines can
   never be replaced or extended. A retry must publish a new generation
   containing a new preseed and baseline pair.

   Intent and completion-fence publication share one no-clobber protocol. The
   writer opens a deterministic generation-scoped same-directory temporary name
   with `O_CREAT|O_EXCL|O_NOFOLLOW`, writes canonical UTF-8 JSON, removes every
   write bit, and fsyncs the file. It publishes with `linkat` to a
   generation-specific intent name or the single canonical completion path
   `fence.json`, so `EEXIST` fails instead of replacing data, fsyncs the
   directory, unlinks the temporary name, fsyncs the directory again, and
   requires the final regular file to have link count one. Only one completion
   fence may ever commit; its exact bytes record the winning generation. If a
   writer restarts with a temporary or final name present, it opens without
   following links and may
   finish cleanup or re-emit a digest only after proving the inode, exact
   canonical bytes, schema, generation, and expected hash match. Any differing
   artifact or unexplained link fails closed. It never renames over, truncates,
   replaces, or recopies an existing intent or fence.

   A subsequent, independently reviewed saved plan receives the exact intent
   hash. Its graph prevents the quiesced snapshot resources from starting until
   the quiescence observations and completed online preseed are accepted. It
   creates and waits for a retained incremental `aws_ebs_snapshot` of the exact
   legacy volume, then creates and waits for a retained
   `aws_ebs_snapshot_copy` encrypted by the dedicated SkyPilot-state KMS key.
   The 2026-08-08 audit found no existing snapshot for the unencrypted source
   volume, so a retained source volume alone does not satisfy this gate.

   Only after both quiesced snapshots complete does Terraform start the final
   pod. Its
   `request-store-evidence-verifier` init container receives the existing
   PostgreSQL secret but no state mount, begins an explicit read-only
   transaction, and emits only the sanitized marker digest/fields, current
   schema/counts/logical hash, and database observation time into a pod-local
   shared evidence file. The
   credential-free filesystem finalizer consumes that evidence, performs one
   bounded final filesystem sync, and commits an atomic, digest-sealed
   target-completion fence on the separate authority access point. This stage
   must not run a SQLite importer, write request rows, alter database schemas,
   or replace the already-complete PostgreSQL cutover.
   The fence is the regular, non-symlink, read-only JSON file published through
   that protocol. Schema version 1 records `status=complete`,
   `attempt_generation`, `zero_at`, both fixed deadlines, Helm release
   namespace/name, source PVC namespace/name/UID, PV name/UID and EBS volume ID,
   all four distinct baseline and quiesced source/encrypted snapshot IDs, target
   filesystem ID, state access-point ID, state PV name/UID and PVC
   namespace/name/UID, authority access-point ID, authority PV name/UID and PVC
   namespace/name/UID, the final path-free manifest SHA-256, entry count, byte
   count, a sanitized PostgreSQL evidence object and its SHA-256, and completion
   timestamp. Every preseed and baseline field must exactly match the intent,
   whose SHA-256 is recorded as `generation_intent_sha256`; the quiesced pair
   and final manifest are new fields completed under that same intent.

   The PostgreSQL evidence object accepts exactly these v1 keys and no unknown
   keys: `schema_version=1`,
   `metadata_key="sqlite-to-postgres-cutover.v1"`,
   `cutover_marker_sha256`, `cutover_format_version`,
   `cutover_completed_at`, `cutover_request_count`, `cutover_queue_count`,
   `cutover_logical_sha256`, `observed_at`, `database_schema_revision`,
   `current_request_count`, `current_queue_count`,
   `current_nonterminal_count`, `current_claimed_count`, and
   `current_logical_sha256`. `cutover_marker_sha256` hashes the complete
   canonical historical marker without exposing its source path or request IDs.
   The current fields come from one PostgreSQL
   `REPEATABLE READ, READ ONLY` transaction, use the database clock, require
   `current_request_count >= cutover_request_count`, and require the queue,
   nonterminal, and claimed counts to be literal integer zero. Historical queue
   count cannot exceed historical request count, and timestamps must prove
   `cutover_completed_at <= zero_at < observed_at < completed_at < work_cutoff`.
   The evidence hash covers canonical sorted UTF-8 JSON bytes. The finalizer
   emits the SHA-256 of the exact fence bytes as gate evidence and never writes
   the completed status before all copy, snapshot, and verification checks
   pass. It refuses to publish at or after the 45-minute work cutoff and proves
   `completed_at < work_cutoff` from the fixed intent.

   A pre-fence abort has two separately reviewed, ownership-correct phases.
   Phase A is a saved infrastructure plan that disarms and removes every
   finalizer, verifier, preseed writer, arming input, and write-capable RBAC
   while keeping API zero and traffic blocked. Only after those writers are
   absent may a read-only `O_NOFOLLOW`/`lstat` proof establish that no fence
   exists. With no remaining writer, phase B is a reviewed direct-Helm
   fix-forward artifact whose operator wrapper rechecks fence absence
   immediately before it removes the drain block and restarts the accepted
   one-pod legacy-RWO plus PostgreSQL revision. Fence appearance at any point
   fails phase B and selects fix-forward recovery. The operator,
   approved deploy identity, and reviewers must be on call, and a rehearsal
   must show both abort phases plus pod readiness fit in the reserved 75 minutes
   before starting cutover; the API must be Ready by the fixed 120-minute
   deadline. Completed attempt artifacts remain retained.

   An out-of-process `rainier-cutover-watchdog`, started from the approved
   operator host with the exact intent hash, owns both timers independently of
   Terragrunt. Snapshot resources have bounded create timeouts and every
   migration Job has an `activeDeadlineSeconds` no later than the work cutoff.
   If the fence is absent at the cutoff, the watchdog cancels the active Helm or
   infrastructure operation, waits at most two minutes for exit, and records
   the process and, when applicable, backend-lock evidence. A lock may be
   force-unlocked only by the named deploy lead after two independent checks
   prove no apply process or session remains. A fresh
   refresh/discovery step finds every generation-tagged snapshot, adopts any
   in-progress or completed artifact without waiting for it to finish, and
   produces a new abort-A plan; a saved plan made before cancellation is
   invalid. The watchdog continues through abort-A, absence proof, the
   direct-Helm abort-B operation, and pod readiness, pages on any missed
   intermediate deadline, and declares an incident rather than success if API
   Ready is not observed by 120 minutes. The nonproduction rehearsal must cover
   cancellation, stale-lock recovery, refresh/import, the fresh abort-A plan,
   the fresh abort-B Helm artifact, and the full 75-minute rollback reserve.

   If legacy becomes writable after abort, a retry uses a new generation, new
   digest-sealed intent and timestamps, a new online preseed and baseline pair,
   new quiesced snapshots, final sync, manifest, and fence hash. No artifact
   from an aborted generation can satisfy a later generation. Once a fence
   commits, abort to legacy is invalid and recovery is fix-forward only.
5. While the API remains at zero, remove every preseed, intent, PostgreSQL
   evidence, and final-sync Job plus every ConfigMap, secret reference, service
   account, Role, RoleBinding, arming input, and read-write authority mount. A
   reviewed plan and live inventory must prove no migration writer can restart
   before the first workload mounts the replacement claim.
6. Apply a reviewed direct-Helm one-API-pod compatibility revision with the
   replacement RWX claim and unchanged PostgreSQL request store, but without
   split-role HA. The typed chart-native `rwxAuthorityFence` value, rather than
   unrestricted extra Helm values, mounts only the authority claim read-only in a verifier
   init container running from a digest-pinned helper image. It requires an
   explicit `storage.existingClaim` equal to the statically provisioned state
   claim; a chart-created/dynamic claim cannot satisfy the fence. It also
   requires the PostgreSQL request store's built-in execution-quiescence backend
   enforcement, so alternate storage or queue plugins cannot sit outside the
   evidence. Before the workload can start it uses no-follow file
   operations, requires a regular non-symlink file with no write bits, validates
   every version/status/identity/deadline field, and matches the exact fence-byte
   SHA-256 supplied from accepted finalizer evidence. Main containers receive no
   authority mount. While this input is enabled, chart validation rejects a nonempty
   escape-hatch `apiService.sidecarContainers` or
   `databaseConnection`/`executorService`/`controllerService`
   `extraVolumes`/`extraVolumeMounts` array; only the typed verifier may mount
   the authority volume. The digest input has no default, is populated only
   after step 4, and makes a skipped or partial finalizer fail closed. The same
   verifier remains on every later role and is not removed by cleanup.
   Accept this revision as the fix-forward rollback target for the storage
   boundary. The legacy RWO claim remains retained and unmounted.
7a. Before capacity, apply two independently reviewed prerequisite PRs and saved
   plans. The first pins the owning AWS provider to exactly 6.26.0, the first
   accepted version that can encode managed-node-group
   `update_strategy = "MINIMAL"`, and must show no unrelated production drift.
   Before the production pin is accepted, upgrade an exact disposable copy of
   the relevant state from the former provider to 6.26.0 and back again, proving
   both saved plans are zero-change; this is the rollback rehearsal, not a claim
   inferred from a lock-file edit.

   The second prerequisite declaratively enables Amazon VPC CNI network-policy
   enforcement in standard startup mode. Because this changes the whole cluster,
   its review first inventories every live `NetworkPolicy` and every required
   ingress/egress edge, then runs a semantic reachability matrix covering the
   existing workloads, DNS, API server, PostgreSQL, External Secrets, Datadog,
   and required AWS endpoints. A deny-all control and its explicitly allowed
   peer run on each currently available zone; success means the policy agent is
   programmed before the control pod becomes Ready and denied destinations
   remain unreachable afterward, not merely that a `NetworkPolicy` object
   exists. Standard mode may default-allow before programming, so the design
   makes no zero-packet startup claim. Capacity probes carry no token, Secret,
   or workload identity, and IAM, RBAC, database credentials, and exact endpoint
   authentication remain the authorization boundaries for real workloads.
   The exact add-on version and prior configuration are captured by the
   approved non-admin deployment identity. A separately saved rollback restores
   those exact values. Neither prerequisite adds paid nodes.
7b. Also before capacity, after the Sky-side PriorityClass and schema-head
   compatibility PRs merge and while every workload still runs on fixed
   three-node legacy capacity, a platform plan first creates only the dedicated
   LB PriorityClass, scoped 16-pod ResourceQuota, and stable admission engine.
   None contains a chart, image, release, or template digest. The direct-Helm
   bundle owns a narrowly validated application policy that binds the exact
   controller/principal/template/resource rule. Its initial policy permits only
   each exact captured
   untyped predecessor template and its exact typed successor, bound to the
   existing controller, owner, one-replica shape, resources, and rollout
   semantics. This lets Kubernetes replace a crashed predecessor in the
   ordering interval without opening a mutable old-image path. The unchanged
   legacy ASG `max=3` prevents physical or billed expansion during this bounded
   mixed-profile conversion; any other old-image create is rejected.
   Convert one owner at a time under that two-digest allowlist and retire its
   untyped predecessor digest only after both typed warm slots are Ready; the
   next owner cannot advance early.

   This first application-owned admission version binds the exact typed legacy-placement
   Deployment and Pod template, not a future target selector. Every later
   placement transition uses a reviewed two-digest handoff: admit only the
   exact current and next immutable templates, roll and prove the next Ready
   template, then retire the old digest. The phase-8 pre-target step advances
   to the exact legacy-pinned toleration profile: it adds the target taint
   toleration but retains an immutable required legacy-node-group selector or
   affinity even after target nodes exist. C-hub later atomically replaces that
   legacy constraint with each owner's exact target selector under the same
   old/new-digest handoff. No admission version may permit target placement
   before that owner's C-hub move.

   After the platform admission resources are accepted, a reviewed direct-Helm
   bundle first installs the two-digest policy, proves the stable engine has
   accepted it, and then applies the exact combined image, chart, and values as one pod in
   `all` role on the legacy nodes against schema 008. This compatibility
   artifact accepts API-request heads 008 and 009, renders
   `serve.externalLoadBalancer.priorityClassName`, does not run migration 009,
   and exposes no observer endpoint. Reconcile the eight warm-standby services
   one service at a time: roll the standby slot, promote it through the durable
   cutover protocol, then roll the former active slot. Each service must retain
   a Ready selected endpoint and both accepted slots before the next begins.
   Acceptance proves exactly 16 steady quota-scoped LB pods, zero legacy-profile
   LB pods, zero available surge slots, exact quota resource products, and no
   rejected or Pending pod, plus RWX/PostgreSQL continuity and authority-fence
   verification. Archive the exact render and direct-Helm legacy-placement
   one-pod rollback artifact as `compat-one-pod-rwx-008-009-legacy`. This is a
   zero-node prerequisite; failure rolls the affected service
   and forbids target creation. Its pre-authored direct-Helm abort keeps the
   compatibility image running, advances the application-owned admission
   policy to the exact current plus rollback-untyped legacy digests, sets the
   PriorityClass value empty, and reconciles every
   converted service standby/promote/former-active back to the exact untyped
   legacy profile. Only after 16 Ready untyped pods, zero typed pods, and stable
   endpoints are proved may a platform plan remove the stable admission engine,
   quota, and PriorityClass; the direct-Helm abort then restores the phase-6
   image and removes its application policy. Until removal, abort evidence
   proves the class remains exactly value 0, non-global, and
   `PreemptLowerPriority`; weakening it cannot substitute for reversal. Even an
   abort before the first conversion proves zero typed pods before removing
   those guards. Once phase
   7b is accepted, phase 6 is superseded and this abort is forbidden; rollback
   uses `compat-one-pod-rwx-008-009-legacy` until the final-target revision is
   accepted. Only after zero untyped pods are
   proved does the scoped quota become the complete LB hard cap used by the
   paid-capacity model.
8. Before the first paid target plan, an approved live inventory captures every
   non-DaemonSet Pod and its owning Deployment, StatefulSet, Job, or CronJob;
   standalone Pod; namespace/service account; rendered-template digest;
   replicas, surge and disruption behavior; priority; requests/limits; volumes;
   topology; and current node. It includes Argo CD, CoreDNS, the EBS CSI
   controller, External Secrets, AWS load-balancer controller, external-dns,
   golink, Datadog clusterAgent, PostgreSQL-facing components, every
   Argo-managed add-on, and anything absent from repository defaults. Dormant
   Jobs and CronJobs reserve their maximum concurrency. Every real owner that
   replaces a -1000 `hub-system-capacity` reservation must have captured
   effective priority strictly greater than -1000 and
   `preemptionPolicy=PreemptLowerPriority`; a missing/`Never` policy or lower
   effective priority blocks capacity pending a separately reviewed handoff.
   Controller-created
   dynamic Pods, especially SkyServe external-load-balancer Deployments, bind a
   typed immutable template profile, persisted service/LB mode, exact service
   account, resources, placement, and admitted replica/concurrency ceiling.
   The audited baseline is exactly eight warm-standby services and no retained
   single-LB services, hence `N_warm=8`, `N_single=0`, and pools contribute
   zero. The steady pod bound is
   `L_steady=2*N_warm+N_single=16`. Each warm-slot Deployment retains
   `Recreate`: reconciliation replaces the unselected standby, proves exactly
   one Ready/nonterminating Pod UID before selector promotion, and replaces the
   former active only after cutover. It therefore adds no same-slot surge. Only
   a retained single-LB Deployment uses `RollingUpdate` with `maxSurge=1` and
   `maxUnavailable=0`. Thus `L_rollout=N_single=0`, and the exact aggregate hard
   bound is `L_lb_slots=L_steady+L_rollout=2*N_warm+2*N_single=16` LB pods. Any
   persisted-mode drift from those accepted counts blocks the target plan until
   the numbers, capacity proof, and paid approval are revised and re-reviewed.

   Every LB pod uses the dedicated immutable PriorityClass
   `rainier-skyserve-external-lb` with `value=0`, `globalDefault=false`, and
   `preemptionPolicy=PreemptLowerPriority`. Its value is strictly between the
   `rainier-capacity-reservation` class at -1000 and the
   `rainier-skypilot-control-plane` class at +1000, so real LB pods reclaim only
   lower-priority capacity reservations without outranking role pods. A fail-
   closed admission rule binds the full class definition to the exact
   controller service account, owner, labels, template digest, one-replica
   Deployment, warm-slot `Recreate` plus durable standby/promote/former-active
   ordering, retained-single `RollingUpdate 1/0`, and captured nonempty requests/
   limits. A
   typed `serve.externalLoadBalancer.priorityClassName` chart value is carried
   through a reserved server-owned environment variable; every controller-
   capable role renders that exact value into generated LB Pod specs. Empty
   remains backward compatible outside the guarded profile, while the Rainier
   direct-Helm preflight rejects an empty value before the infrastructure quota
   may be enabled. A
   PriorityClass-scoped ResourceQuota atomically caps pods at 16 and caps CPU
   and memory request/limit totals at 16 times that captured profile; a missing
   CPU limit or any profile mismatch fails the prerequisite rather than
   weakening the quota. Kubernetes quota admission is the concurrent hard
   backstop: excess creations are rejected even if several controllers race.
   Every current instance plus any retained-single rollout slot enters the
   model; the accepted 8/0 inventory has no rollout slot. Every minute predicate
   proves the class name/value/global-default/
   preemption policy, quota values, used counts, persisted mode counts, expected
   Ready pods, and zero quota-rejected or Pending LB pods.
   Unowned, mutable-template, or unbounded work fails the gate.

   A pre-target saved-plan sequence adds only the exact target toleration to
   this frozen cohort while every workload remains Ready on legacy. It retains
   an immutable required legacy-node-group selector or affinity in every
   non-DaemonSet template, so a restart cannot land on a target after target
   creation but before its owner-specific C-hub gate. A
   fail-closed admission restriction for the exact taint key/value permits only
   the accepted namespace/service-account/template identities and reviewed
   migration resources. Broad `Exists`, empty-key, mutable-label-only, or
   unbound-service-account exceptions are forbidden. Required `kube-system`
   DaemonSets receive separately enumerated exceptions and remain in per-node
   overhead. Rainier must prove its exact Kubernetes version supports the
   admission mechanism; otherwise a replacement is designed and re-reviewed
   before target creation.

   For generated LB objects, that sequence performs the reviewed admission
   two-digest handoff from the accepted legacy template to the legacy-pinned
   toleration template, proves every replacement Ready on legacy, and retires
   the legacy digest. It does not permit target placement. Each later C-hub
   service move repeats the same bounded handoff, atomically replacing the
   required legacy constraint with the target selector, and retires the
   predecessor only after readiness.

   Let `H_mem` and `H_cpu` be the maximum simultaneous memory and CPU requests
   of that digest-bound non-role cohort, including all 16 permitted SkyServe
   external-LB pod slots, rollout surge, dormant batch work, and dynamic-
   controller ceilings, and let `D_zone` be exact per-zone DaemonSet/system
   overhead. `H` is one scenario-indexed resource/topology vector, not a second
   copy of live work. For each accepted worst-case scenario, the live cohort
   contributes `H_live`; inert reservations reproduce only its currently
   inactive delta `H-H_live`, shape by shape and zone by zone. A live pod and a
   reservation can never both count the same shape. Six targets are eligible
   only if concrete scheduling of `H_live + (H-H_live) = H`, `D_zone`, and the
   SkyPilot footprint below leaves all documented aggregate and per-zone
   reserves. If it does not, topology, physical and dollar ceilings, and paid
   approval are revised and re-reviewed before any apply.

   Before paid overlap, an independently active `rainier-capacity-guard` starts
   in signed stable legacy-three mode, binding the approval, exact legacy MNG/
   ASG, count three, absolute UTC hard end, and 24-hour cleanup reserve. Before
   plan A the operator arms a distinct signed creation-transition bound to the
   exact three future Terraform addresses, MNG names, subnet/AZ pairs,
   ownership tags, fixed size-two inputs, configured-nine destination, and
   transient-ten ceiling. Missing future targets are legal only in that bounded
   transition. As each appears, the guard binds the immutable EKS MNG and
   generated ASG returned by `DescribeNodegroup`; all instances with any signed
   target tag count before binding, and an unreconciled identity freezes. A
   separate recorded action accepts stable overlap-nine/transient-ten only
   after all exact identities, configured desired/max nine, and normal
   InService/nonterminated nine converge.

   In every mode, at least once per minute the guard proves each present bound
   MNG's exact min/desired/max and enumerates EC2 by both exact ASG membership
   and signed MNG/ASG tags. Every state except literal `terminated`--including
   `pending`, `running`, `stopping`, `stopped`, and `shutting-down`--counts
   against the physical ceiling; an unknown state, unclassified member, tag/
   membership mismatch, or discovery failure fails closed. Stable overlap mode
   proves configured nine, normal nine, and at most the approved tenth legacy
   `AZRebalance` instance. The signed normal-retirement transition later permits
   only the exact legacy group to move from nine to stable target-six; a
   cost-stop uses its own signed transition. Every change follows guard-first,
   one permitted action, destination-mode acceptance. Partial ordering freezes.
   The guard publishes conservative spend, neither writes HA evidence nor starts
   its clock, and remains through stable-six acceptance after legacy retirement.

   Saved cluster plan A then creates only three labeled, dedicated-tainted,
   zone-scoped node groups with `min=desired=max=2`; the retained legacy group
   stays `min=desired=max=3` and schedulable. The three target groups use
   `MINIMAL` with repair disabled. The legacy group's exact live update/repair
   behavior is captured and frozen; it is never updated, repaired, replaced, or
   scaled during overlap. Version, launch-template/AMI, capacity, and scaling
   inputs are frozen, every group is `prevent_destroy`, the legacy group is
   adopted without replacement, and no launch template uses `latest_version`.
   Before plan A is saved, its configuration explicitly removes
   `create_before_destroy` from the legacy `aws_eks_node_group.main` and pins
   `launch_template.version` to the captured live integer rather than
   `aws_launch_template.eks_nodes.latest_version`. Both source changes must be
   no-ops for the legacy resource in plan A; otherwise stop. Leaving either
   behavior live could turn an incidental launch-template edit into a full
   three-node replacement surge, producing twelve physical instances--above
   the approved transient-ten and dollar ceilings. Launch-template drift is
   therefore counted alongside AZ rebalancing as a possible transient source,
   but unlike the approved single AZ-rebalance instance it is prohibited by
   configuration and plan.
   Exact values come from the approved non-admin identity and saved plan, never
   repository defaults. Acceptance B requires exactly two
   Ready nodes per bound subnet/AZ, Ready CNI and policy agents, and matched
   startup, positive, and negative controls in every target zone. A failure
   leaves legacy scheduling unchanged. Configured maxima total nine; the legacy
   ASG may transiently own a tenth `AZRebalance` instance, and no path above
   that approved physical ceiling may run. Plan A cannot start until the exact
   creation-transition mode is active and cannot reach acceptance B until the
   stable overlap-nine/transient-ten mode is accepted.
9. Separately reviewed ownership-correct C-hub artifacts move each allowlisted
   non-role owner,
   including every current SkyServe external-load-balancer Deployment and its
   bounded dynamic template, onto the target selector one controller at a time
   while legacy remains schedulable. Each has an exact reverse-selector plan and must pass its own
   rollout/PDB, dependency, desired-plus-surge, and dormant-concurrency canaries
   on targets. Singleton and `Recreate` owners use declared maintenance
   semantics. Missing, Pending, or unhealthy work reverses that controller
   before retry; a bulk drain is not migration evidence. Platform-owned hub
   resources use saved infrastructure plans; SkyPilot-generated external load
   balancers use the accepted controller reconciliation path under its exact
   admission handoff. Platform IaC never adopts a SkyPilot runtime owner.

   C-sky is two ownership-separated artifacts: a saved infrastructure plan
   creates only the inert capacity proof, and a reviewed direct-Helm operation
   relocates only the one-pod `Recreate` compatibility Deployment in its
   declared maintenance window. The live 16-CPU/96-GiB pod plus a required-affinity 14-GiB
   delta is one future controller shape. Eight other role probes model three
   API, three executor, and two full controller placements, so the exact future
   role footprint is 546 GiB/84 CPU rather than a double-counted fourth
   controller. Five more reservations model two state monitors, two authority
   monitors, and the one-at-a-time observer. All fourteen use the final target
   placement but a digest-pinned pause image, unique identity, no token, Secret,
   Service, storage, RBAC, workload entrypoint, or release-selector overlap,
   and an enforced-after-readiness deny-all policy.

   Digest-pinned inert `hub-system-capacity` reservations separately model
   only every accepted dormant or rollout-only hub shape absent from the live
   scenario. Their generated resources are the exact inactive delta
   `H-H_live`; they never reproduce already-live resources or add a second copy
   of `H_mem`/`H_cpu`, and they do not alter the exact fourteen SkyPilot
   placeholders. C-sky passes only when the API, all fourteen placeholders,
   the complete live hub cohort, its complementary dormant/rollout
   reservations, and aggregate/per-zone reserves are Ready on the six targets.
   It then archives the exact running image, chart, chart values, one-pod RWX/
   all-role values, final selector/toleration, fence, PostgreSQL input,
   resources, typed LB contract, render, direct-Helm operation artifact, and
   separate infrastructure proof as `compat-one-pod-rwx-008-009-target`. A
   zero-change render/diff and idempotent direct-Helm reapply must preserve the
   Ready target one-pod API and target-selected LB cohort. That exact target-
   placement revision supersedes `compat-one-pod-rwx-008-009-legacy` for every
   post-taint and post-009 rollback; mixing legacy placement with target values
   or inferring placement from live state is forbidden.
   Only then may saved cluster plan D add the legacy `NoSchedule` taint and
   explicitly drain any unexpected residual non-DaemonSet pod to an empty
   inventory. Plan D contains no workload or capacity change.

   The fourteen reservations remain through HA. Real roles and monitors use a
   dedicated higher `PriorityClass` and reclaim only their matching capacity by
   preemption. A placeholder is removed only after its real replacement is
   Ready; a hub reservation is removed only after its owner is Ready or its
   maximum-concurrency shape is exercised. Each handoff proves the real owner's
   effective priority is greater than -1000 with
   `preemptionPolicy=PreemptLowerPriority` before relying on preemption. The
   observer reservation is retired
   once after HA behind a fresh capacity gate rather than repeatedly preempted
   in its 30-second window. The target taint and admission allowlist prevent an
   unrelated workload from winning any handoff gap.
10a. In the next SkyPilot PR and reviewed direct-Helm artifact, deploy the
   compatibility-only image already accepted in phase 7b with the exact
   final-target one-pod values archived after C-sky, and apply guarded
   role-split HA with explicit API/
   executor/controller roles and RollingUpdate. This release accepts
   API-request schema 008 and 009 but neither runs migration 009 nor exposes the
   observer API. The Helm preflight revalidates
   `compat-one-pod-rwx-008-009-target`--its archived image, chart, complete
   target-placement values, RWX claim, PostgreSQL store, authority fence, typed
   chart values, LB quota contract, and one-pod rollback target--byte for byte
   before applying the role split. The legacy-placement revision is forbidden
   after plan D. Live inventory must prove every
   API, executor, controller, and compatibility process runs it against schema
   008. The plan and render
   must preserve the typed PostgreSQL request-store input, load-bearing
   environment variables on every role, nonsticky ingress, role PDBs, topology
   spreading, `maxSurge: 1`, `maxUnavailable: 0`, `minReadySeconds: 10`, and
   `progressDeadlineSeconds: 600`. A separate saved infrastructure plan installs
   platform-owned two-replica state and authority monitors without changing the
   Helm release. `rwx-state-monitor` writes and fsyncs only its
   dedicated per-replica sentinel path on the state claim. The two-replica
   `rwx-authority-monitor` has no state mount, Kubernetes API RBAC, or service
   account token; it mounts only the authority claim read-only and hashes the
   accepted fence every 60 seconds. Each monitor container requests and limits
   50m CPU and 64 MiB memory; both Deployments use topology spreading and a PDB,
   and their four pods are represented by the accepted infrastructure probes in
   the capacity proof.
10b. Only after that fleet proof, use a distinct reviewed direct-Helm artifact
   to deploy the migration/endpoint release.
   It may run additive migration 009 and expose the private observer routes but
   cannot combine unrelated schema, capacity, storage, role, or scheduling
   changes. Re-prove every HA, metrics, authority, request-continuity, and
   placement predicate. After migration 009 commits, the phase-6 exact-008
   binary is explicitly superseded and forbidden. Exercise a direct-Helm
   fix-forward rollback to exact `compat-one-pod-rwx-008-009-target` in one-pod/all-role
   mode against retained schema 009, prove target scheduling, request
   continuity, authority-fence and LB-admission verification, and readiness,
   then fix forward to the migration/endpoint
   release and re-prove HA. This is an operator-declared maintenance exercise:
   reducing 2/2/2 HA to the one-pod `Recreate` target may gap the API, and the
   evidence records the exact unavailable interval rather than claiming a zero-
   gap transition. No other post-HA phase may introduce a planned availability
   gap. This exercised final-target artifact is the sole post-009 one-pod
   rollback target; native Helm rollback, the legacy-placement artifact, and
   every pre-10a image or values set are invalid. A separate infrastructure
   plan installs the out-of-band minute observer suspended and without a seed;
   no Job or evidence clock may start, and the plan cannot mutate Helm.
11. In a separate saved plan, retire the nine role and five infrastructure
   placeholders only after all six role pods, four monitor pods, and the
   suspended observer specification are accepted. Remove each hub reservation
   only after its exact owner or maximum-concurrency exercise is accepted.
   Live inventory must prove no placeholder remains, the target taint and
   frozen toleration allowlist are unchanged, all hub and SkyPilot workloads
   remain Ready, and the aggregate/per-zone reserves still pass.
12. After HA conformance, takeover, fix-forward rollback proof, and legacy-node
   empty/taint evidence are accepted, use a separate infrastructure activation
   PR and saved plan. It reads but cannot mutate Helm and binds the exact
   release-bundle/fence/node identities and paid-capacity
   approval and the private observer service-account principal. The reviewed
   activation input contains a future exact first UTC slot and attempt UUID.
   A digest-pinned one-shot seed-writer Job performs an S3 conditional create
   with `If-None-Match: *`; on a precondition failure it succeeds only after a
   GET proves byte-identical content, the expected digest, and exactly one
   retained object version, otherwise it reports a collision. Ordinary
   `aws_s3_object` writes cannot satisfy this contract. Terraform waits for that
   Job without changing the Helm release. A final
   capacity gate revalidates the observer request, target taints,
   pinned role requests/replicas, and aggregate/per-zone buffers after
   reservation retirement. Observer unsuspension is the infrastructure graph's
   final mutation, performed only after every other resource and check is
   accepted. The apply must finish
   before the reviewed slot. The first
   eligible interval is the first slot whose canary is actually admitted by
   PostgreSQL in its 30-second window and accepted by the next Job; neither
   earlier uptime, the seed itself, nor pre-activation samples count.
13. Keep the pre-authored transition cleanup PR draft until the exact 168-hour
   no-reset observation contract in Monitoring passes, a completed post-copy
   EFS recovery point, a successful isolated restore rehearsal from that point,
   effective 35-day retention evidence,
   fix-forward rollback proof, and exact no-destroy plans pass. Before removing
   the legacy MNG, arm the signed normal-retirement transition bound to its exact
   Terraform/MNG/ASG identities and an unchanged target-six cohort; if a
   post-HA cost-stop already accepted target-six with legacy scaled to zero, use
   the signed identity-removal variant and never scale it back up. Continue
   counting every legacy instance until literal `terminated`, then accept stable
   target-six only after the legacy identity is absent and target configured/
   InService/nonterminated counts are exactly six. Cleanup then removes
   transition code and only forgets retained legacy objects from Terraform
   state; it does not delete the PVC, PV, EBS volume, snapshots, backups, or
   data. Any eventual data deletion is a separate explicitly authorized change.

The refreshed platform stack starts by repurposing
`boltz-bio/boltz-platform#7823` as the one-time root
four-address application ownership handoff and immutable reusable-module pin.
Its old 1.1.1087 runtime payload is obsolete. The pinned SkyPilot module commit
contains the resource deletions, language floor, and permanent tombstones.
The platform PR removes the now-inert Helm/ECR provider configuration,
application inputs, and application-value assertions, replacing them with a
static ownership guard; direct-Helm H0 artifacts own those assertions. Its
Rainier plan must contain exactly
four root `forget` actions for `helm_release.skypilot`,
`kubernetes_config_map_v1.seed_config`,
`kubernetes_job_v1.seed_config`, and
`terraform_data.reconcile_api_server`, with zero remote mutation. Its human
apply must not change the live release or inert legacy seed objects. The same
shared pin must have separate zero-managed-resource-action saved plans for the
research-production EKS pool, research-usw2 spoke-workspace EKS pool, and
multi-tenant AWS-VM unit before the Rainier handoff is accepted. The remaining
linear infrastructure stages map to `#7824` (inert EFS and both RWX object pairs),
`#7829` (legacy retention and generation-scoped online preseed), `#7830`
(quiescence infrastructure only), a new finalizer PR, a separate
writer-retirement PR, and `#7831` (infrastructure prerequisites and gates for
one-pod RWX compatibility). They are followed by a provider-6.26 prerequisite,
a cluster-wide CNI-enforcement prerequisite, pre-target hub admission, target
create/acceptance, ownership-correct C-hub infrastructure stages, C-sky
capacity-proof resources, and a separate legacy-taint PR. `#7832` is
repurposed into monitor, reservation-retirement, and observer-evidence
infrastructure stages; it contains no Helm release. `#7833` remains the
non-destructive cleanup descendant. SkyPilot PRs separately deliver the chart
and application code, and reviewed direct-Helm artifacts perform quiescence,
one-pod RWX, LB-profile compatibility, target relocation, role HA, schema 009,
and rollback drills after their corresponding platform gates. Platform
observer infrastructure performs evidence activation without mutating Helm.
Final PR numbers and exact cross-repository evidence edges are written
back here when opened.

A pre-authored alternative draft stack roots infrastructure abort-A on the
finalizer stage; its descendant direct-Helm abort-B artifact cannot run before
abort-A writer retirement and stable absence proof. It is used only if no
fence committed and never merges into the successful fix-forward path. Each
infrastructure stage requires its own complete saved plan and human apply, and
each application stage requires its own reviewed Helm artifact and explicitly
authorized operation. Merging either repository is not evidence that an earlier
live gate passed.

The current resource candidate preserves the all-role pod's measured 128
controller-class long-worker budget. Each controller requests 16 CPU, is
limited to 28 CPU, and requests and limits 110 GiB. Each active-active executor
requests 8 CPU, is limited to 16 CPU, and requests and limits 64 GiB; each must publish
`health_detail.long_workers=64`, preserving a combined normal-request budget of
128. Each stateless API replica requests and is limited to 4 CPU and 8 GiB.
Memory request equals limit for every role so scheduler placement cannot
silently overcommit node memory.
Six `m6i.8xlarge` nodes, exactly two per availability zone, are the minimum
steady candidate. Five nodes are structurally insufficient: a valid
steady placement can leave one zone with a single node and force all three
zone-spread surge pods, requesting 182 GiB in total, into that zone.

The current single three-AZ managed node group cannot make exactly two nodes in
each zone a declarative invariant. The rollout therefore creates three
zone-scoped two-node groups before HA and retains the three-node legacy group
through the observation window. Before it can be tainted, the exact live hub
cohort adopts the target toleration under an admission fence, then moves one
controller at a time through reversible C-hub plans. C-sky separately relocates
the compatibility pod and proves capacity. Only plan D taints legacy and drains
an unexpected residual Pod. This normally owns nine nodes while only the six
target nodes remain schedulable; legacy `AZRebalance` may transiently own a
tenth. Cleanup retires the empty legacy group only after
the seven-day gates, leaving the three zone-scoped groups as the six-node
steady state.

Moving from three to six steady nodes adds three on-demand instances. At
2026-08-08 us-east-1 prices, compute is $4.608 per hour, about $3,364 per
730-hour month, plus about $12 per month for three 50-GiB gp3 root volumes.
The transition adds all six new nodes before it retires any existing node. The
six-new-node increment costs $9.216 per hour, about $1,548 of compute plus $5.52
of prorated root-volume storage for seven days; all nine normally owned nodes
cost $13.824 per hour, about $2,322 for seven days. A possible tenth
`AZRebalance` node costs another $1.536 for every hour it exists. Each extra day
of six-node overlap after a reset adds about $221 of compute. The eventual
steady delta is the three-node $4.608-per-hour figure above.
EFS Standard storage is $0.30 per used GiB-month, Elastic Throughput reads are
$0.03/GiB and writes are $0.06/GiB, and warm EFS backup storage is $0.05 per
used GiB-month. At 200 GiB used—the claim capacity, not an enforced EFS
quota—storage is about $60 per month plus $10 per month for one full warm
backup; one full copy is about $12 of EFS writes and one full verification read
is about $6. The online baseline source snapshot and encrypted copy add up to
about $20 per month if each initially bills the full 200 GiB at
$0.05/GiB-month. The quiesced pair normally bills only changed blocks in those
primed chains, but four independently full 200-GiB snapshots would cost up to
about $40 per month. Actual used and changed blocks, incremental backup size,
EFS usage above 200 GiB, and traffic govern the bill. The retained 200-GiB gp2
source remains about $20 per month during the rollback window. The dedicated
customer-managed KMS key adds about $1 per month plus request charges. The
isolated restore rehearsal temporarily adds restored EFS storage, throughput,
and any effective backup-restore charges; its actual duration and bytes are
recorded with the evidence.

Before the target-create plan is approved, an identified management approver
must record approval for six new `m6i.8xlarge` nodes in us-east-1: the
incremental $9.216/hour (approximately $1,548 for seven days), the nine-node
total $13.824/hour (approximately $2,322 for seven days), the possible tenth
legacy `AZRebalance` instance and its bounded incremental charge, and the
$4.608/hour (approximately $3,364 per 730-hour month) steady increase after the
legacy three are downscaled. The approval specifies an absolute UTC overlap end
and maximum billed overlap hours; generic urgency or implementation approval is
not paid-capacity approval.

Before target creation, signed, state-bound no-destroy cost-stop branches are
authored and reviewed. The pre-HA unwind has exact pre-D, post-D, and
partial-10a variants; it never guesses the current state. It first freezes or
cancels an unaccepted apply. If 10a began, it restores the exact
`compat-one-pod-rwx-008-009-target` direct-Helm revision and proves it Ready on
targets. If plan D applied, it reverses D by removing only the exact recorded
legacy `NoSchedule` taint and proves every legacy node schedulable. It then
reverses C-sky and each C-hub two-digest selector/admission handoff one owner at
a time, proves all accepted work Ready on legacy and every target digest
retired, arms the guard's exact target-zero transition, and only then scales
the target groups to zero. After accepted HA, the other branch scales the
already empty legacy group to zero while preserving its group, state, data, and
the six-node steady state.
Neither branch guesses which side owns workloads. The approval includes a
bounded cleanup-only reserve after the normal end during which an identified
human may apply only the state-appropriate cost-reducing plan even if the
observation chain reset; applying it stops/resets progression but does not trade
data safety for evidence. The capacity guard pages before both deadlines. A new
identified approval is required to continue overlap beyond the normal end or
cleanup reserve, unfreeze a node-group input, or exceed ten physical nodes.

Before activation, schedule the steady 2/2/2 topology and all future surge
shapes against exact `D_zone`, the complete digest-bound live hub cohort, its
16-pod dynamic SkyServe-LB quota ceiling, and only the complementary
dormant/rollout reservation delta needed to make live plus reserved work equal
`H_mem`/`H_cpu` once. The live compatibility pod plus its co-located
14-GiB delta count as one controller; the remaining eight role probes model
three API, three executor, and two controller placements. With those nine role
probes, the live pod, and five infrastructure-reservation probes scheduled
alongside that full hub model, require at
least 110 GiB and 16 CPU unrequested cluster-wide, and in every zone require at
least one node with 32 GiB and 4 CPU unrequested. If any surge pod is Pending
or either reserve is absent, increase capacity before the Helm rollout;
reducing `maxSurge` is not an allowed workaround.

Scheduling all fourteen isolated probes alongside the live compatibility pod,
accepted hub owners, and dormant/rollout reservations is the rollout-capacity
proof. The remaining
aggregate and per-zone thresholds are explicit system and incident-response
buffers, not a claim that another 110-GiB controller fits contiguously after all
three surge pods are present. Normal controller availability comes from the
accepted pair being spread across zones; a further node failure during an
active rollout halts the rollout and requires capacity restoration before it
resumes.

Rainier's storage authorization and state boundary are explicit:

- Platform IaC owns a dedicated versioned S3 observation-evidence bucket with
  Object Lock compliance retention of at least 35 days, default encryption,
  owner enforcement, TLS-only and public-access policies, and `prevent_destroy`.
  The minute observer can GET only deterministic predecessor/seed keys and
  conditionally append its deterministic sample/reset/checkpoint keys; it
  cannot list. The one-shot seed writer has separate short-lived GET/PUT
  authority for the exact reviewed seed key and `ListBucketVersions` only for
  that singleton-key prefix; it cannot write a minute key. The exporter has a
  separate read-only List/Get identity for the attempt prefix. None may delete,
  alter retention, or change bucket configuration.

  Both writers use a digest-pinned helper that sends exact
  `If-None-Match: *`; a normal Terraform `aws_s3_object` is forbidden. The
  bucket policy explicitly denies every `PutObject` into both disjoint seed and
  minute prefixes when `s3:if-none-match` is absent or not exactly `*`,
  regardless of allowed principal. A seed retry succeeds only when exact-key
  GET plus version listing proves one byte-identical retained version and the
  expected digest. An existing minute key is always collision/reset, never
  success. More than one version of any seed or minute key is evidence
  corruption; no consumer chooses a winner.
- The dedicated SkyPilot-state KMS key is owned by platform IaC, has automatic
  rotation, least-privilege EFS/EBS/Backup use, and `prevent_destroy`. It is not
  the EKS secrets key and is the sole key accepted for the EFS filesystem and
  encrypted snapshot copies.
- The dedicated AWS Backup vault, plan, and selection are Terraform-owned and
  protected from destroy. The plan runs daily and retains EFS recovery points
  for at least 35 days; merely enabling EFS automatic backup and inheriting an
  account-editable default plan is insufficient. HA cleanup requires the
  effective plan/selection, a post-copy completed recovery point, and a restore
  rehearsal from that exact point. The rehearsal restores to an isolated
  temporary filesystem under the same KMS key and never selects either
  production claim. Scheduled EFS backups taken while the state claim is live
  are crash-consistent and may observe different mutable files at different
  instants; the rehearsal therefore must not compare the restored mutable tree
  with the cutover manifest or claim point-in-time equality. Before the backup
  window, `rwx-state-monitor` atomically writes and fsyncs a recovery-point
  sentinel with unique known content. The selected backup must start after that
  sentinel's completion time. The restore mounts both isolated access-point
  roots read-only and verifies the exact digest-sealed authority fence bytes,
  the selected recovery-point sentinel, required directory structure,
  readability, ownership, modes, and symlink safety. It records a new
  restore-specific path-free inventory and hashes as evidence, not as an
  equality assertion against production. The production verifier is expected
  to reject the restored filesystem's new identities. Temporary-resource
  cleanup and cost are recorded and authorized separately from rehearsal
  success.
- The Kubernetes provider in every Rainier unit that needs cluster access uses
  exec-based `aws eks get-token` acquisition at apply time. The cluster unit may
  retain its Helm provider solely for platform-owned releases such as Argo CD,
  but that provider also uses exec-based token acquisition. The SkyPilot
  control-plane and Kubernetes-infrastructure roots declare no Helm provider;
  direct SkyPilot Helm obtains its own short-lived operator authentication
  outside Terraform. A 15-minute `data.aws_eks_cluster_auth` token may feed
  neither provider nor be embedded in a saved plan. Every apply requires an
  approved non-admin hub deploy identity, STS account `255203429798`, and a
  context that reaches the private EKS endpoint; the read-only administrator
  audit identity is not apply authorization.
- The EFS CSI node service account uses its own EKS Pod Identity role. Its
  identity policy grants `ClientMount`, `ClientWrite`, and `ClientRootAccess`
  only for the managed filesystem and either exact access point through a mount target.
  `DescribeMountTargets` is scoped to the same filesystem. The EC2 node role
  receives no broad EFS client policy.
- The EFS filesystem policy allows that same role, requires one of the two
  exact access points through a mount target, and denies
  unencrypted transport. Both static PVs use `tls` and `iam` mount options, so
  neither anonymous NFS nor a non-TLS fallback is a supported path. The
  filesystem pins `performance_mode=generalPurpose` and
  `throughput_mode=elastic`.
- Static provisioning needs no controller-side access-point permissions.
  Rainier therefore has no dynamic EFS StorageClass and does not attach
  `AmazonEFSCSIDriverPolicy` to the CSI controller. The isolated `skypilot-ha`
  conformance release keeps its disposable dynamic-provisioning contract; it
  is not production storage precedent.
- The cluster Terraform state exports both exact `fs-id::fsap-id` handles only
  after mount targets, backup and filesystem policies, the node Pod Identity
  association, and the managed CSI add-on are ready. The new Kubernetes-
  infrastructure unit consumes that output through a real dependency. Mocks
  are allowed for plan and validate, never apply, so a fake handle cannot enter
  its Kubernetes state and the two states cannot apply concurrently.
- The new `gitops-hub-rainier/skypilot-kubernetes-infrastructure` unit owns both
  retained, `prevent_destroy` static PV/PVC pairs outside Helm. Each PV is
  pre-bound to its exact namespaced claim and
  both objects in a pair use a distinct non-empty sentinel class, for which no
  StorageClass object exists. This prevents default-class admission from
  changing a PVC and prevents dynamic provisioning. Both pairs advertise
  `ReadWriteMany` and mount their exact static access point with `tls`, `iam`,
  and `noresvport`. The state and authority claims cannot bind to each other's
  PV. This provisioning revision does not change live Helm claim selection, the
  `Recreate` strategy, PostgreSQL request-store configuration, or pod identity;
  those changes occur only at the guarded stages above.

  This unit inherits the repository `root.hcl` S3 backend, so its state key is
  `gitops-hub-rainier/skypilot-kubernetes-infrastructure/opentofu.tfstate` in
  `boltz-platform-opentofu-state-255203429798`, with encryption and native S3
  locking. It has real Terragrunt dependencies on `../cluster` for the EKS
  endpoint/name/CA and ready EFS access-point handles and on
  `../skypilot-control-plane` for the infrastructure-owned namespace identity.
  Its generated Kubernetes provider uses apply-time `aws eks get-token` exec
  authentication under the approved non-admin account-255203429798 identity;
  it declares no Helm provider. Mocks are allowed only for `init`, `validate`,
  and `plan`. Apply ordering is cluster storage/CSI first, the already-stable
  infrastructure-only control-plane namespace second if needed, and this
  Kubernetes-infrastructure state last; no two dependent states apply in
  parallel.

### M2: Split API and executor roles

- Refactor shared bootstrap.
- Add explicit API and executor entrypoints.
- Render separate API and executor Deployments.
- Move request GC to a distributed singleton execution.
- Add role health and instance heartbeats.

Implementation status: the M2 candidate has explicit `api` and `executor`
supervisors. API pods start Uvicorn only and enqueue every request, including
short log streams and Jobs wait operations, through PostgreSQL. Executor pods
start the durable short and long worker pools, publish PostgreSQL-backed
readiness leases, and own the temporary M2 controller compatibility paths.
API shutdown no longer inspects or interrupts fleet-wide executor work.
Cancellation intent is delivered to the exact worker instance, claim token,
and execution generation through the durable heartbeat path.

Schema revision `002` adds the role-instance table and advertised handler and
payload compatibility. Periodic maintenance loops use dedicated PostgreSQL
advisory-lock sessions so two executor replicas elect one active owner and
promote a standby after session loss. First-writer initialization of the
server identity is now atomic, and the shared config bootstrap stages complete
files before an atomic rename so concurrent pods cannot expose partial state.
HA mode also renders independent API and executor PodDisruptionBudgets with
integer `maxUnavailable: 1`. These are part of M2's role split, not optional
rollout polish: an autoscaler eviction of the only remaining ready API during a
graceful replica deletion otherwise defeats the availability contract even
when durable request delivery is correct. Review 17 supersedes the original
`minAvailable: 1` implementation so the healthy floor follows an intentional
increase in desired role replicas rather than remaining fixed at one.

The M2 compatibility boundary is explicit. Until M3, executor replicas may
claim both normal and controller execution classes, and the existing
leader-elected managed-jobs refresh thread plus durable SkyServe daemons remain
inside the executor lifecycle. M3 must remove that routing exception and those
controller ownership paths from executors. The complete deletion targets
remain enumerated in the Legacy Code Removal Map.

Local acceptance is complete. It includes the complete serial
real-PostgreSQL request suite, including cross-process migration locking and
the one-slot ordinary-pool saturation regression; role-isolation, shutdown,
request-executor, server, Uvicorn-loop, database utility, migration utility,
global-system-config, and connection-pool tests; schema generation; Helm lint;
and all 219 Helm unit tests. The default and HA Helm renders and server-side
dry runs also pass. Formatting, mypy across 770 files, Pylint at 10/10, and
dashboard lint and formatting pass for the changed files.

The first revision 7 failure injection used commit
`c6c6ac28c04d93b983690abc57999d24e3176ab7` and image digest
`sha256:69df023f83983cae8baca665ddcbef76c0c04947a14cf04f25619bb2ccc13cca`.
The first API deletion completed with 14,333 raw health requests and 111
authenticated durable submit-and-get probes at zero failures. During the
second deletion, Karpenter independently evicted the sole remaining ready API
as underutilized. The final sample recorded 26,330 health calls with 415
failures and 241 durable probes with 36 submit and 36 lookup failures. This is
negative evidence, not acceptance. It moved the role-scoped disruption budgets
from M4 into M2; the exact amended image must repeat the full zero-failure gate.

Revision 8 then exposed a separate control-plane isolation defect during the
deliberately blocked mutation test. The executor supervisor, durable request
store, and ordinary central-database work all resolved to the same
process-local `QueuePool(pool_size=1)`. A queue precondition blocked on the
locked `clusters` table and held that one ordinary connection, so unrelated
instance and execution-claim heartbeats timed out even though PostgreSQL and
the durable request tables remained healthy. Fencing still prevented duplicate
execution, but liveness must not depend on an unrelated workload transaction.

M2 therefore gives the PostgreSQL request store a named, process-local engine
namespace distinct from the ordinary central-database engine. The namespace
retains the same strict one-connection pool policy, so this is isolation rather
than unbounded pool growth. Queue metadata, role leases, claim heartbeats, and
cancellation acknowledgements use the request-control namespace. Preconditions
and workload handlers continue to use the ordinary central-database namespace.
The connection budget already reserves capacity outside the one ordinary
connection per process for advisory-lock sessions, asynchronous calls, and
role traffic; the one lazy request-control connection per process is now an
explicit member of that reserve. A regression test must hold the ordinary
one-slot pool while proving a role heartbeat and execution-claim heartbeat
complete through the request-control pool. Live acceptance must repeat the
blocked-mutation test and show no request-control pool timeout or stale daemon
claim.

Fresh request-schema bootstrap must also work with that strict one-slot pool.
The existing distributed Alembic session lock re-entered the same pool for its
post-lock revision check, which can self-deadlock before any migration runs.
All PostgreSQL Alembic session locks therefore use the existing dedicated
`NullPool` advisory-lock engine, release nonwinning probe sessions before
sleeping, and keep the winning session only for the migration lifetime. The
schema work continues through the bounded request-control pool. The
real-PostgreSQL regression starts from an empty schema, so it covers both this
fresh-bootstrap ordering and the later heartbeat isolation.

Revision 8, with digest
`sha256:0e9122cac657351dc12eef7ecce05ee1cb8630979b153ee318ee7b7004588a00`,
proved the role and failure semantics before the pool-isolation correction.
Two API deletions completed under continuous traffic with 48,884 raw health
checks and no health or submission failures. The PodDisruptionBudget rejected
eviction of the only remaining ready API with HTTP 429. Request
`3760f57e-a75d-4b5e-9855-3c1ffce9fc9b` was submitted through one API, executed
by an executor, and fetched through the other API in generation 1. Remote
cancellation of request `732d850c-ba28-4300-93bf-5bd311fd2c3a` was executed by
a different executor and acknowledged in about 3.86 seconds. Hard-killing the
owner of mutating request `efdf2e90-f71a-41c9-b5b2-95098d853e0c` fenced the
generation, terminalized it as an ambiguous interrupted outcome, and created
no cluster row. An idle-executor deletion and a bounded read-only recovery
also passed.

Revision 9 used the M2 runtime tree at
`5fef35ad19e32932c971edd053a0575b4f213c7a` and exact image digest
`sha256:dbdb605934a0d159535b1b051c25262ec16914de4fa8737b285f86ad79f7c809`.
The migration hook completed on that digest and schema revision `002`; two API
and two executor replicas then became ready with zero restarts. During an
`ACCESS EXCLUSIVE` lock on the ordinary `clusters` table, claimed request
`a4de37f6-e567-48a1-996e-64966e1c266c` advanced its heartbeat from
07:53:28.099 to 07:53:48.110 UTC and its lease from 07:53:58.099 to
07:54:18.110 UTC while all four role leases remained fresh. Cancellation
request `00ccb040-f304-4a56-9839-e0f23c9c4970` ran on the other executor and
terminalized the target without waiting for the ordinary pool. The queue row
was removed, no matching cluster existed, and current role logs contained no
QueuePool timeout or failed-heartbeat error. The owner's later stale-claim log
is the expected post-cancellation write fence.

The same revision passed live singleton and API failover. Evicting the
singleton owner moved all nine PostgreSQL advisory locks to the survivor in
about two seconds; all five daemon request generations advanced once and
remained freshly heartbeating while the replacement became ready. Evicting an
API replica also produced the required HTTP 429 rejection while one protected
replica was at risk, then rolled to a ready replacement. Across the final
sample the in-cluster canary recorded 120,375 health successes, zero health
failures, 583 authenticated submissions, zero submission failures, and 557
successful result lookups. Thirteen of its 25 bounded result timeouts were the
revision 8 baseline while old executors drained during the revision 9 rollout;
the remaining 12 occurred while the test intentionally locked the `clusters`
workload table. The revision 9 executors resumed successful lookups after each
unlock, and every recent request reconciled to a terminal state. Both role
PodDisruptionBudgets report `minAvailable: 1` and one currently allowed
disruption, both roles have two fresh ready leases, and all four current role
pods run the exact revision 9 digest with zero restarts.

Deployment:

1. Deploy two API replicas and two executor replicas.
2. Verify both role-scoped PodDisruptionBudgets report one healthy protected
   replica, then run an in-cluster ClusterIP canary and continuous
   authenticated traffic while deleting each API pod.
3. Prove follow-up request lookup and logs work through a different API pod.
4. Delete an idle executor and prove new work continues.
5. Delete a busy executor and prove ambiguous mutating work is interrupted,
   not duplicated.

### M3: Split and fence controller ownership

- Add the controller role and Deployment.
- Move managed-jobs and SkyServe supervisors out of API lifespan and executor
  workers.
- Add registry-owned execution classes and route all controller-starting
  requests to the elected controller's specialized executor pool.
- Extend leader-loss handling to terminate and fence child controllers.
- Persist worker instance and controller generation ownership.
- Reconstruct Serve control files from durable version state during recovery.

M3 uses API-request schema revision `003` and managed-job schema revision
`026`. API revision `003` adds the controller-leadership and
controller-action-reservation tables described above plus a nullable
`controller_generation` on each request. Managed-job revision `026` adds
nullable `controller_instance_id` and `controller_generation` ownership to
`job_info`; null remains the compatibility representation for jobs created by
the all-role server.

A controller acquires its advisory leader lock through a dedicated PostgreSQL
session and advances the durable generation, takes a generation-specific
advisory lock, and records that session's backend PID and generation-lock key
on the same session before it can claim work. Every claim heartbeat, RUNNING
transition, result write, retry, and terminal write for a controller-class
request must match the request claim, current unreleased leadership row, and
live `pg_locks` entries for that PID, the election-lock key, and the
generation-lock key. Losing the leader session therefore fences the old
generation immediately, even before a standby advances the generation and
while another database connection in the stale pod remains usable.

PostgreSQL queue consumers receive an immutable set of allowed execution
classes. Explicit executor roles claim only `normal`; only the current
controller leader starts a bounded pool that claims `controller`; the
compatibility `all` role remains the sole temporary consumer of both. The
first M3 leader also transactionally fences controller-class claims left by an
M2 executor. Reconcile and read-only work returns to the durable queue under a
new execution generation. Ambiguous mutating work becomes terminal and is
never replayed.

Controller promotion also has an explicit M2-to-M3 mixed-version gate. An M2
executor advertises controller handlers in its durable instance lease, while
an M3 executor advertises only normal handlers. Controller standbys refuse
leadership until the last all-role or executor advertisement that includes a
controller handler has been quiet for the configured executor termination
grace plus ten seconds. This uses the last database heartbeat even after the
old instance marks itself draining, because M2 published draining before its
worker pools had necessarily exited. A pod waiting on this gate is unready and
publishes `phase=waiting-for-executor-cutover`, so Helm cannot accept the new
controller Deployment before the mixed-version window closes. The winner
rechecks the gate while holding leadership and before spawning any worker or
subsystem child. An active leader continuously rechecks the same condition and
becomes unready, fences its children, releases leadership, and exits if a
legacy consumer reappears during rollback.

Controller-owned subsystems still contain nested SDK calls that submit normal
work back through the HTTP API. Loopback is not a valid target after the
controller moves out of the API pod. In HA mode, Helm therefore injects
`SKYPILOT_API_SERVER_ENDPOINT` into every non-API role with the stable
ClusterIP API Service URL. The endpoint is deployment-owned: request payloads
cannot replace it, and the clean server environment propagated to consolidated
managed-jobs and SkyServe children retains it. A controller role health-checks
that existing remote endpoint and must never call `sky api start` or bind a
local API listener. The compatibility `all` role keeps the old behavior of
clearing a client endpoint and starting its colocated API server until that
role is removed.

The stable Service route does not bypass API authentication. An installation
that requires bearer authentication on direct ClusterIP traffic supplies
`SKYPILOT_SERVICE_ACCOUNT_TOKEN` from a Secret through the controller and
executor `extraEnvs`; both that credential and the internal endpoint are
removed from persisted request environments. The service-account identity must
have the same permissions that controller-owned nested actions require. OAuth
proxy deployments that authenticate only at ingress continue to use the
private ClusterIP path without an added token.

Every non-replayable controller-class request reserves its stable logical
action before its handler starts. Reservation ownership includes the
controller instance and generation; only that generation may advance the
reservation to running or terminal. A handoff marks an unfinished prior
reservation ambiguous rather than silently repeating it. This request-level
fence composes with the existing resource-specific protections: managed jobs
retain the consolidation-mode session lock and exact controller process
records, while SkyServe retains service lifecycle epochs, incarnation-scoped
resource identities, and exact controller PID/IP ownership. These inner fences
remain authoritative before individual provider-side actions, rather than
adding a second parallel implementation for every cloud operation.

Managed-job scheduler ownership composes with that request fence. Each
`WAITING` to `LAUNCHING` transition records the current outer instance and
generation and, in the same managed-job database transaction, takes a shared
row lock on the leadership record after proving its two advisory locks are
live. Generation advancement takes the conflicting row write lock. Thus an
old claim either commits before handoff and is identified as stale by the new
leader, or observes the new generation and fails closed.

The managed-job recovery gate remains present from before the inner
consolidation lock acquisition until recovery is complete. Whenever any
nonterminal managed job exists, the new leader pays the bounded post-acquire
drain interval, including when the row happens to be `WAITING` with no PID.
Recovery then resets stale-generation ownership before it starts any
replacement scheduler. Detached scheduler processes also probe the exact
outer generation and exit on mismatch or loss of database proof. The timing
wait is only a drain aid; correctness comes from the transactional generation
fence.

Status refresh reads PID, instance, and generation from one snapshot and
rechecks the same fields immediately before terminalization. For a
nonterminal job whose local controller died, it first commits
`FAILED_CONTROLLER` under the exact live outer generation and snapshot, then
performs provider cleanup, and only then marks the schedule state `DONE`.
Terminal task state is therefore the durable no-recovery decision before any
destructive provider request. If leadership changes after that decision, the
replacement leader treats the terminal job as cleanup work rather than
resetting it for recovery, may safely repeat the idempotent teardown, and
finishes `DONE` under its own generation. A nonterminal row owned by another
generation remains recovery work, never evidence that the current leader's
local controller crashed. This distinction is required because PIDs are
meaningful only inside their owning pod.

After a scheduler reclaims a job whose durable task status is already
`RUNNING`, the resumed controller changes the schedule state from `LAUNCHING`
to `ALIVE` before monitoring. This is an ownership-state correction, not a new
cluster launch. The transition retains the same instance and generation
predicate, so a concurrent handoff fences it like every other scheduler write.

Revision 16 used commit
`72af110bbcec49411fc33f334f30446bc207d494` and exact image digest
`sha256:9259b33b6dd0b14ee714a4fb2d5062a17aab02f7233f3c47f969693575db6472`.
An active-pod deletion advanced controller generation 18 to 19, restored the
managed-job scheduler to `ALIVE`, retained workload pod UID
`c34f6580-2743-4e94-bc35-a0229d8fdd6f`, and continued from tick 525 through
tick 552 with zero workload restarts and zero API or Serve canary failures.
The subsequent PostgreSQL-session failure injection correctly fenced
generation 19 and promoted generation 20 in about two seconds, but exposed a
different child-termination bug. The old outer controller delivered `SIGTERM`
to its detached scheduler. That scheduler interpreted the signal as user
cancellation, ran normal finalizers, deleted the workload at tick 552, and
left outer schedule state `DONE`. This is negative evidence, not M3
acceptance. The fail-stop contract above is required before repeating both
failure modes with a fresh long-running job.

Revision 17 used commit
`a03c0c0abdfff91db3a1e86cf903ad14cd5d58fc` and exact image digest
`sha256:a312d720fb4975523f533b1821ec9770f4a279a7744a593d214c2c9abc9aee4e`.
The migration Job completed on that digest, all three roles rolled to two Ready
replicas, and schema revisions remained API `003` and managed jobs `026`.
Terminating generation 23's PostgreSQL lock session promoted generation 24;
the detached scheduler log ended immediately after reporting that generation
23 was no longer current, without cancellation or cleanup finalizers. Job 3
returned to `ALIVE` and its original workload pod continued through tick 62
with zero restarts. Deleting the generation 24 leader then promoted generation
25 and retained the same workload through tick 115.

A fresh two-replica Serve service was added for the combined gate. Active-pod
deletion promoted generation 25 to 26 while the service stayed `READY`, the
managed job returned to `ALIVE`, both Serve replica UIDs and the job workload
UID remained unchanged, and the API and Serve canaries recorded zero failures.
Karpenter later forcefully terminated a node that independently hosted the job
and one Serve replica during the first database-session sample. That event
changed the workload UIDs and produced two Serve 503s, so the sample was
rejected rather than attributed to controller failover. After SkyPilot
recovered both workloads, the three test pods were marked non-disruptable to
isolate the controller failure injection and the canary baseline was reset.
Terminating generation 27's lock session then promoted generation 28, restored
the job to `ALIVE`, kept the service `READY`, retained job pod UID
`0c132233-af62-45c9-b242-731fe797f09e` and Serve pod UIDs
`c4ffd558-8f5e-42fd-acd4-136a7818e764` and
`66d0f2d4-5dea-4a6a-a9dc-cd53c6790537`, and left all three at zero restarts.
The final API canary sample was 1,194 successes and zero failures; the reset
Serve sample was 122 successes and zero failures. PostgreSQL showed exactly
the election and generation advisory locks for the live generation. M3 is
accepted.

Standbys publish Ready with `phase=standby`. A leader publishes its generation
and continuously probes and heartbeats the lock-owning session. On SIGTERM it
first becomes unready, stops controller claims, interrupts the specialized
worker pools, fail-stops local detached schedulers, releases its durable
leadership row, and exits. If the lock session is lost, it follows the same
sequence but cannot mark the already-lost session released; the live
PostgreSQL proof is the immediate fence, and the old process exits without
running managed-job finalizers. Singleton maintenance loops start only with
leader-owned resources or retain their narrower PostgreSQL session locks.

The chart renders two controller replicas with a distinct label, role command,
health port, resources, credentials, shared storage, and PostgreSQL
configuration. HA validation requires at least two. M3 intentionally leaves
the controller PodDisruptionBudget and topology-spread rollout policy for M4,
but active and standby deletion are both failure-injected before M3 is
accepted.

Deployment:

1. Deploy two controller replicas.
2. Start a minimal Kubernetes managed job and SkyServe service.
3. Delete the active controller pod repeatedly.
4. Verify standby promotion, one live generation, no duplicate resources, and
   uninterrupted workload data plane.
5. Terminate the leader PostgreSQL session and repeat the proof.

### M4: Migration ordering, disruption safety, and stateless routing

- Make the migration Job a blocking `pre-install,pre-upgrade` Helm hook in
  guarded HA mode. HA validation requires a pre-existing PostgreSQL connection
  Secret, while compatibility mode retains the regular Job so existing
  chart-managed connection Secrets remain valid.
- Extend disruption budgets to controller replicas. Default API, executor, and
  controller topology spread to `kubernetes.io/hostname` with
  `whenUnsatisfiable=DoNotSchedule`; operators may select a zone topology key
  or explicitly disable the constraint. Because Helm `--reuse-values` can omit
  defaults introduced by a newer chart, the templates also apply the hostname
  default when an older HA release has no topology key. An explicitly present
  empty string remains the opt-out; YAML null cannot carry that distinction
  through Helm's value coalescing.
- Complete readiness-first termination for every role with a pod-local
  `emptyDir` drain marker. A Kubernetes pre-stop hook creates the marker and
  waits for `apiService.highAvailability.readinessDrainSeconds`, which defaults
  to 20 seconds. API and role-health readiness fail immediately when the
  marker exists, and PostgreSQL role heartbeats publish `draining` on their
  next update. Helm rejects a drain interval shorter than the configured API
  readiness failure-detection window and rejects any role termination budget
  that does not leave at least ten seconds after the pre-stop interval.
- Disable sticky ingress sessions whenever guarded HA mode is enabled, even if
  the compatibility default `ingress.sessionAffinity=true` remains set.
  Guarded HA also rejects custom NGINX affinity annotations so
  `ingress.annotations` cannot silently restore pod affinity. Transport retry
  annotations remain because endpoint convergence can race an in-flight
  connection, but uploads and follow-up requests never depend on affinity.
- Add an HA conformance script that drives traffic during pod deletion and
  rolling upgrades. The script is namespace- and release-scoped, requires an
  explicit destructive-test confirmation, consumes an existing token Secret
  without printing token material, records exact revisions and images, and
  fails on any raw health, authenticated submission, durable lookup, or stream
  fetch error. During graceful deletion it independently requires the target
  role readiness endpoint to return 503 while the container is alive, its
  PostgreSQL lease heartbeat to publish `ready=false` and `draining=true`, and
  its Kubernetes Pod condition to become unready before termination.
- Update administrator documentation and remove the experimental warning only
  for the guarded HA configuration.

Implementation status: M4 is complete and live-accepted on the isolated
deployment. Chart schema generation, chart lint, all 239 Helm unit tests,
guarded live-value
server-side dry-run, shell syntax and ShellCheck for the conformance harness,
targeted role-runtime tests, formatting, mypy, Pylint, dashboard checks, and a
warning-as-error Sphinx build pass. The fast drain-marker unit test proves
readiness fails without opening PostgreSQL.

A post-acceptance telemetry review found that M2 had kept metrics-server
startup restricted to `all` and `api` roles. The split-role chart also omitted
metrics enablement, `PROMETHEUS_MULTIPROC_DIR`, the metrics container port, and
scrape annotations from executor and controller pods. Controller-owned metrics
therefore had no target, and a standby did not bind a metrics port until after
promotion. This follow-up separates role-local metrics serving from
leader-only background maintenance, starts it before controller election,
and makes the chart discover every role pod. The correction changes no request
backend, leadership, Serve recovery, or activation behavior. Production HA
rollout requires live evidence that every Ready role pod remains scrapeable
through a controller handoff before metrics-dependent rollout clocks may
start.

Exact-head validation after rebasing onto the current `improvements` base found
and fixed a fresh-bootstrap regression in the shared PostgreSQL schema. The
managed-jobs ownership columns are present in current table metadata before
Alembic stamps revision 026. The generic add-column helper previously attempted
the duplicate DDL, caught PostgreSQL's duplicate-column error, and then
continued inside the aborted transaction. It now inspects the current schema
before issuing DDL, so an already-converged column is a true no-op and an
unexpected DDL error rolls back the migration. All 38 request and batch
PostgreSQL cases pass with zero skips, and the complete PostgreSQL migration
and database-utility matrix passes all 257 cases, including fresh subprocess
bootstrap and all central schemas sharing one search path.

The first live M4 attempt used commit
`ca47898533a904f89ce5a40b821cb1274966989c` and exact image digest
`sha256:8626e96e446f6994ae3f23f90fa018be843971ae7f35e57a6a4790b30dbf1366`.
Revision 18's migration hook completed successfully at 13:23:10 UTC, one
second before the first target-image role pod was created. All three
Deployments rolled to two ready, zero-restart replicas; at least 3,222
conformance probes, 4,572 existing API probes, and 1,441 Serve probes remained
error-free, and all three long-running workload pods retained their UIDs with
zero restarts. The harness then stopped before deletion and rollback because
the role Deployments had no topology constraints. `--reuse-values` retained
the older release's missing keys instead of loading the new chart defaults.
This is negative evidence, not M4 acceptance. The templates now distinguish a
missing reused-value key from the explicit empty-string opt-out, and the full
sequence must repeat with a new exact image.

The second live attempt used commit
`af919839b549e45059ad22022e94395d0a2cb5f4` and exact image digest
`sha256:e3e09c656d952412296dda6cc4a4c4c95d504e3e0fcef4dd5e6911f0a5c73c3e`.
Revision 19 rendered all three topology constraints, completed its migration
hook one second before target pod creation, and rolled under 4,974 raw health,
310 submit and get, and 309 stream successes with zero failures. Immediately
afterward, Karpenter began evicting one API replica for underutilization while
the harness used a raw Pod delete on the other. A raw delete bypasses the
Eviction API and therefore did not atomically consume the PodDisruptionBudget
before the concurrent autoscaler eviction. Both replicas terminated and the
existing one-hertz API canary recorded 70 transport failures before
replacements became ready.

The deleted target's retained PostgreSQL row proves the runtime path worked:
`ready=false`, `draining=true`, phase `draining`, with a heartbeat at
13:43:25 UTC. The harness nevertheless reported `WAIT` because its probe
subprocess did not set the server database-selection marker and it searched
only ready API pods after both APIs were unavailable. This is negative
conformance evidence, not a runtime heartbeat failure. Graceful test
disruptions now use the Kubernetes Eviction API with PDB-aware retry, so a
manual test eviction and Karpenter eviction serialize atomically. Durable row
queries may run through any ready API, executor, or controller pod and
explicitly initialize server database selection.

The third live attempt used commit
`40086217bd28cb09c539ecd83643b6efb41026be` and the same exact runtime digest
`sha256:e3e09c656d952412296dda6cc4a4c4c95d504e3e0fcef4dd5e6911f0a5c73c3e`.
Revision 20's migration hook completed at 13:52:18 UTC, one second before its
first target pod, and the tag-based role rollout completed. The first
post-rollout assertion then found one durable lookup with HTTP code `000`.
PostgreSQL and executor logs prove that request was not lost or duplicated: it
was created at 13:55:07.767 UTC, generation 2 claimed it at 13:55:37.956 UTC,
and it succeeded at 13:55:38.174 UTC. The canary's fixed 30-second client
deadline expired just before the queued request completed while executors were
turning over.

This is negative harness evidence, not M4 acceptance and not a durable-request
failure. A blocking durable lookup must still fail on an actual broken
connection, but its client deadline must cover a valid queue delay during a
guarded executor rollout. The harness now uses a 120-second lookup deadline,
records timestamps, curl exit codes, and sanitized error text for every
authenticated operation, and preserves canary, Helm, workload, PDB, Job, and
event evidence before cleanup on both success and failure. The complete
upgrade, controlled-eviction, rollback, and final-upgrade sequence must repeat
from a fresh canary.

The fourth live attempt used commit
`4bc5a28356` and revision 21. Its migration hook completed successfully, all
roles rolled to the exact target digest, and 2,699 health probes plus 168
complete submit, lookup, and stream cycles had zero failures. After the
post-rollout stability check passed, Karpenter won a new Eviction API race for
one API replica. The captured state showed one available API replica and zero
allowed API disruptions, so the harness correctly refused to start its own
disruption but stopped instead of waiting for redundancy to recover.

This is negative harness evidence, not M4 acceptance. External PDB-governed
disruption is expected in the target environment. Before each controlled
drain, the harness now waits for all roles and PDBs to regain stable redundant
state, selects one current ready target, and retries selection if an external
eviction wins the race. It still requires two fully observed controlled drains
per role; external evictions cannot satisfy that count.

The fifth live attempt used commit `b66d67acc2` and revision 22. Its migration
hook completed at 14:09:36 UTC, one second before the first target pod. All
roles rolled to the exact digest while 3,624 health probes and 223 complete
durable request cycles remained error-free. The harness waited through
additional Karpenter disruptions, submitted a controlled Eviction for API pod
`skypilot-ha-api-server-6b9574f8fd-2zfd9`, and the API accepted it. Kubernetes
then began termination and the readiness probe returned 503, but the harness
stopped before its drain observations because it did not see that pod in the
PDB's `disruptedPods` map during its 15-second polling window.

This is negative harness evidence, not M4 acceptance. `disruptedPods` is
controller bookkeeping, not the admission interface; the PDB controller may
remove the entry as soon as it observes the admitted pod terminating or
unready. The successful Eviction subresource response is the atomic PDB
admission result. The harness now validates that response as a Kubernetes
`Status` with a successful 2xx code, then proceeds directly to the durable
lease, readiness endpoint, and Pod-condition drain proofs.

The sixth live attempt used commit `fe17783c5c` and exact image
`m4f-fe17783c5c@sha256:e3e09c656d952412296dda6cc4a4c4c95d504e3e0fcef4dd5e6911f0a5c73c3e`.
It upgraded image A at revision 22 to image B at revision 23, rolled back
through revision 24, and finished on image B at revision 25. Revision 23's
migration completed at 14:19:16 UTC before the first target pod at 14:19:17;
revision 25's migration completed at 14:33:19 before its first target pod at
14:33:20.

All six controlled Evictions returned successful code 201 admissions. Both
replicas of the API, executor, and controller roles independently returned 503
from their pod-local readiness endpoints, published `ready=false`,
`draining=true`, and phase `draining` in PostgreSQL, and changed their
Kubernetes Ready condition to false before termination. The six retained rows
record drain heartbeats from 14:24:40 through 14:29:50 UTC.

The preserved canary log contains 9,529 health probes, 592 authenticated
submissions, 591 completed durable lookups, and 591 stream fetches with zero
bad status codes; one final submission was still in flight when cleanup
captured the log. The balanced final assertion itself contained 9,445 health
probes and 586 complete submit, lookup, and stream cycles with zero failures.
Over the same run window the independent API and Serve canaries recorded 1,075
and 1,123 successful probes respectively, with zero failures. Managed job 3
remained `RUNNING` with recovery count 2. The service remained `READY` with two
ready replicas, both load balancer Deployments remained ready, and all three
workload pods were ready with zero restarts.

Final revision 25 has two ready, zero-restart replicas for every role, the
three role PDBs each allow one disruption, and all conformance Jobs, the
temporary canary, and its PDB were removed. This satisfies the M4 acceptance
gate on the isolated deployment.

Post-acceptance cleanup cancelled managed job 3 and brought down
`ha-m3e-fail-stop-serve` through the authenticated API before removing test
fixtures. Both requests succeeded, all three workload pods terminated, both
cloud LoadBalancer Services completed their finalizers, and AWS reported no
remaining load balancers with the test prefix. Revision 26 then disabled the
test-only external Serve load balancer mode and cleared all six authentication
Secret and key values. Its migration and three-role rollout completed under
139 additional API health probes with zero failures.

The dedicated workload namespace, M2/M3 canaries, migration Job, test Secrets,
and namespace, system, and cluster RBAC fixtures were then deleted. The
retained isolated release consists only of the two ready, zero-restart replicas
for each role, their PDBs and API Service, PostgreSQL, RWX state, and declared
service account and Helm metadata. Both API pods reach the shared health
endpoint, revision 26 is deployed, and no M2/M3 workload or cloud load balancer
residue remains.

Deployment:

1. Render and server-side dry-run the guarded chart, including the hook,
   controller PDB, topology constraints, pre-stop lifecycle, and non-sticky
   ingress.
2. Start the in-cluster conformance canary and record image A.
3. Run an image A to image B rolling upgrade. The revision-scoped image B
   migration hook must finish before any image B pod is created, and the
   additive request schema must remain readable by the overlapping image A
   pods.
4. Gracefully drain two image B replicas of each API, executor, and controller
   role. Prove the direct readiness, durable lease, and Pod-condition drain
   signals precede termination while raw and authenticated canaries remain
   error-free.
5. Run Helm rollback to image A, then upgrade to image B again under the same
   canary. Verify hook ordering, role readiness, PDB health, and exact pod image
   digests after the final rollout.
6. Remove the conformance canary and superseded hook Jobs, while retaining the
   healthy isolated release and its declared PostgreSQL and RWX dependencies.

### M5: Compatibility cleanup gate

- Confirm all production-target Helm values use explicit roles, PostgreSQL, and
  shared artifact storage.
- Confirm the rollback window no longer includes a release that reads the
  legacy request database or local queue.
- Delete the compatibility code listed below in a dedicated final commit.

The test-cluster migration can prove the code is removable, but fleet-wide
deletion requires production configuration evidence. Until that gate, the
compatibility paths remain isolated behind `--role=all` and cannot be selected
by HA mode.

## Test Plan

### Unit and integration

- PostgreSQL CRUD parity for every `RequestBackend` method.
- Concurrent request creation returns one winner.
- Concurrent claims return each request exactly once.
- Durable preconditions survive API and executor restarts, reschedule without
  busy-looping, and fail transactionally on timeout or stale ownership.
- Handler registry tests reject unknown names and preserve old-name aliases.
- Registry tests require complete routing metadata for every handler and prove
  request payloads cannot select or escalate execution class.
- Payload envelope tests cover current and previous producer versions and prove
  an incompatible executor leaves a row unclaimed.
- Mixed-version tests prove producers emit only the common supported envelope
  version and new handlers remain gated until a compatible worker is Ready.
- Lease heartbeat and token checks reject stale writes.
- Cancellation reaches only the matching worker and generation.
- Ambiguous lost-owner transitions do not auto-replay mutating requests.
- Replayable launch reconciliation tests cover safe and unsafe states.
- API-role tests assert no queue manager, executor, or controller starts.
- Executor-role tests assert no public listener starts.
- Controller-role tests assert standby, promotion, lock-session loss, child
  fencing, and re-acquisition.
- Metrics-role tests assert API, executor, and controller supervisors each
  start and stop one role-local metrics server, controller metrics start before
  leadership election, built-in shared-state and plugin custom collectors
  remain API-only while multiprocess metrics remain role-local, and split roles
  fail closed without a multiprocess directory, when the configured metrics
  listener cannot bind, or with a health/metrics port collision. A metrics task
  that dies after its startup barrier terminates the owning role, while normal
  role shutdown asks Uvicorn to finish its lifespan, then cancels and joins the
  remaining metrics loop without that fail-stop. Literal true/false/unset
  marker tests prevent a present-but-false environment value from opening the
  endpoint.
- A production-composition metrics test starts a fresh Python subprocess with
  a fresh multiprocess directory, increments
  `sky_serve_system_oom_recovery_events_total` through the real SkyServe
  observability module, starts a controller-role listener on an ephemeral
  port, and proves an HTTP `GET /metrics` returns the labeled sample before a
  clean Uvicorn lifespan shutdown.
- Managed-job PostgreSQL tests prove a stale outer generation cannot claim a
  waiting job, generation advancement serializes with an in-flight claim, and
  recovery resets stale ownership before a replacement scheduler starts.
- Managed-job refresh tests prove a stale-generation PID is never interpreted
  as a current local crash and cannot cause cluster teardown or
  `FAILED_CONTROLLER`. They also prove terminalization commits before provider
  cleanup, a stale owner cannot terminalize, and a replacement generation can
  finish cleanup for a terminal job without resetting it for recovery.
- Nested controller admission tests prove current controller metadata is
  attached to authenticated SDK requests, partial or malformed metadata is
  rejected, and a generation whose advisory-lock proof is gone cannot submit
  new API work.
- Managed-job resume tests prove an already-running task restores schedule
  state to `ALIVE` under the reclaimed generation without launching a second
  workload.
- Detached managed-job controller tests prove the process exits when its outer
  generation loses either advisory-lock proof without running cancellation or
  cleanup finalizers, while all-role local mode keeps its compatibility
  behavior.
- Controller-shutdown tests prove detached scheduler termination is
  non-catchable across the complete scheduler process tree, revalidates the
  recorded process start time, and cannot enter managed-job cancellation,
  terminalization, token revocation, or workload teardown. A subprocess
  sentinel test proves generation-loss exit does not run coroutine finalizers.
  Durable user cancellation tests continue to prove that the separate
  cancel-intent path performs those finalizers.
- Non-API role tests assert nested SDK work resolves the stable API Service,
  never starts a loopback API listener, and cannot inherit a client-supplied
  endpoint or service-account token through the request envelope. Compatibility
  `all`-role tests retain the local-server bootstrap contract.
- Normal executors reject controller-class rows. Only a current controller
  leader may claim them, and a stale generation cannot reserve a new external
  mutation.
- Shared blob tests cover concurrent chunk upload, atomic commit, GC locking,
  and no startup wipe.
- Migration tests cover empty bootstrap, additive upgrade, verify-only success,
  verify-only mismatch, and two concurrent migration attempts.
- Storage-finalizer tests prove the completed fence is absent until both
  quiesced snapshots, bounded final copy, manifest verification, and read-only
  PostgreSQL checks succeed. They bind observed API-zero time, fixed deadlines,
  canonical preseed evidence, both baseline snapshots, and both state and
  authority AP/PV/PVC identities into the attempt hash, reject extension of one
  generation, and
  validate the exact path-free PostgreSQL evidence schema/digest from one
  repeatable-read, read-only transaction, including all literal-zero
  queue/active counts and historical-versus-current request counts. They
  prove an expired work cutoff cannot write the fence. Abort tests prove phase
  A removes all writers before absence proof, phase B rechecks absence before
  restart, and any fence appearance fails recovery. An
  abort-to-legacy-write-to-retry regression requires a fresh generation,
  preseed, four snapshot IDs, final manifest, and fence. No-clobber publication
  tests cover crashes before and after `linkat`, exact-existing recovery,
  `EEXIST`, unexpected hard links, interrupted writes, symlinks, permissions,
  malformed fields, and stable intent/fence-byte hashing. Watchdog tests cover
  apply cancellation at the fixed work cutoff, bounded process exit,
  independently proven stale-lock recovery, discovery/adoption of
  generation-tagged snapshots, invalidation of old plans, fresh abort plans,
  and the 120-minute readiness deadline.
- Fence init-container render and execution tests reject an absent, writable,
  symlinked, malformed, wrong-identity, or wrong-hash fence and accept only the
  exact post-finalizer digest. Snapshots prove only the init verifier receives
  the separate authority claim read-only, the state claim cannot reach that
  access-point root, and the verifier remains on the compatibility revision and
  all three HA roles after cleanup. Negative chart tests prove sidecars and
  database/executor/controller volume escape hatches cannot mount or alias the
  authority claim while the fence is enabled, and reject a mutable helper
  image, a chart-created claim, or disabled built-in quiescence enforcement.
- Terraform tests prove the baseline and quiesced snapshot copies use the
  dedicated KMS key, wait for their source snapshots, and cannot be destroyed;
  the final snapshot graph first requires observed API zero, resolved active
  requests, no state-mounting application pod, and completed/absent preseed,
  then the finalizer depends on completed quiesced copies. Provider tests ban
  planned static EKS tokens and require exec-based token acquisition. Backup
  tests prove the dedicated 35-day plan, vault, selection, throughput mode, and
  destroy protections. Restore tests treat a live EFS recovery point as
  crash-consistent, require its pre-window sentinel and exact authority-fence
  bytes, verify safe structure/metadata, and reject equality claims between a
  mutable restore inventory and the cutover manifest.
- Ownership-handoff tests run from an exact pre-handoff state fixture and prove
  the only planned state changes forget the four root application addresses
  `helm_release.skypilot`, `kubernetes_config_map_v1.seed_config`,
  `kubernetes_job_v1.seed_config`, and
  `terraform_data.reconcile_api_server` through permanent `destroy = false`
  tombstones, with zero Helm, Kubernetes, or AWS action. Post-apply simulation
  proves all four addresses remain absent on every later plan. Separate saved
  plans for all three other shared-pin production consumers have zero managed-
  resource actions. SkyPilot CI rejects removal of a module-root tombstone;
  platform CI rejects any Helm provider in the SkyPilot control-plane or
  Kubernetes-infrastructure roots, release/chart/image/application-values input,
  seed object, rollout restart, or release-specific admission digest.
  Direct-Helm harness tests require
  immutable chart/image/operation digests, captured `values --all`, manifest
  and history, complete render and diff, default `--reuse-values`, and reject
  native rollback, `--atomic`, unreviewed `--reset-values`, or a platform plan
  containing an application mutation. Seed parity tests cover fresh and
  existing databases, merge/list/workspace/prune semantics, all-role and
  split-role reload, migration-before-seed and seed-before-rollout ordering,
  H0 raw-byte and canonical-row no-op parity, required-path preservation for
  GCP VPC, AWS ingress, global Kubernetes contexts, and every workspace
  boundary, missing/mutated-path rejection, the required literal no-plugin
  attestation, nonempty-plugin and ordinary/plugin-field rejection, and proof
  that no plugin loader, import, or installer executes,
  the 262,144-byte input bound, pre-seed failure, post-rollout verification,
  retry, failure TTLs, interrupted-client-after-success and uninstall residue,
  and revision-scoped cleanup of the
  forgotten inert seed objects only after parity.
- Capacity tests count the live compatibility pod plus a co-located 14-GiB
  delta as one controller placement and schedule three API, three executor, and
  two full controller probes against only the three zone-scoped node groups
  while legacy nodes are tainted and empty. They prove probes have no
  SkyPilot image, Service, storage, Secret, service-account token, RBAC, or
  selector overlap; assert exact requests/limits, dedicated taints and
  priorities, and per-zone and aggregate buffers. Tests bind the full LB class
  definition and prove any retained-single LB surge preempts its matching -1000
  reservation but never a +1000 control-plane pod; the accepted 8/0 inventory
  has no LB surge reservation. Five additional pods reserve
  the exact state-monitor, authority-monitor, and observer resources/placement;
  tests prove all fourteen probes plus the live pod schedule together with the
  digest-bound live hub cohort, the exact 16-steady/zero-surge/16-total
  SkyServe-LB ceiling, exact per-zone overhead, and inert dormant/rollout
  reservations equal only to `H-H_live`. Tests prove live plus reservations
  equals each scenario's `H_mem`/`H_cpu` exactly once and reject duplicate or
  missing shapes. Inventory tests fail on unowned, mutable-template, or
  unbounded work. Admission and quota tests reject broad/empty-key or
  unallowlisted target tolerations, wrong LB principals/templates/resources,
  the seventeenth scoped LB pod, and concurrent over-cap creates. Warm-slot
  tests reject RollingUpdate and prove Recreate converges to one Ready/
  nonterminating desired-revision UID before promotion and before replacing the
  former active; retained-single fixtures alone exercise `RollingUpdate 1/0`.
  C-hub tests
  move each owner separately, require desired-plus-surge
  readiness and dependency canaries on targets, and exercise its exact reverse
  selector before plan D can taint legacy. Cost-stop tests separately exercise
  pre-D, post-D, and partial-10a entry states. They require an unaccepted 10a
  rollout to return to the exact target one-pod revision, require post-D state
  to remove only the captured legacy taint and prove legacy schedulable before
  any reverse selector, reverse every C-sky/C-hub two-digest handoff to Ready
  legacy owners, and reject target-zero scaling until all target digests have
  retired. Handoff
  tests prove higher-priority real pods preempt the matching reservations and
  no reservation is removed before its replacement is Ready; the observer
  reservation is removed only by the separate post-HA plan, and a
  fresh activation gate revalidates the pinned workload inventory and capacity
  before final unsuspension.
  HA tests separately require two `long_workers=64` executor reports and 128
  controller long workers in both original pods and the promoted replacement.
  Managed-node-group tests require fixed 2/2/2 target and three-node legacy
  sizes, target-only `MINIMAL` updates and disabled repair, captured/frozen
  legacy update/repair behavior, no legacy `create_before_destroy`, an exact
  integer legacy launch-template version with no `latest_version` reference,
  a plan-A no-op for the legacy resource, frozen version/AMI inputs, and a
  normal maximum of nine plus the explicitly approved single-instance legacy
  `AZRebalance` transient. Guard tests cover stable legacy-three, partial target
  creation with tagged instances before ASG binding, exact stable-nine
  acceptance, normal nine-to-six retirement, cost-stop-specific transitions,
  and fail-closed missing/extra/ambiguous or out-of-order identities. Provider
  rollback tests use a disposable exact state.
  CNI tests cover the complete live policy/reachability matrix and matched
  programming/readiness controls in every legacy and target zone; they do not
  claim isolation before standard-mode policy programming.
- Private-observer API tests cover additive PostgreSQL schema 009 upgrade,
  retained-schema downgrade refusal, the exact table constraints and cascade,
  canonical body/digest/UUID vectors shared with Terraform, new admission
  inside the database-clock window, future and late rejection, identical replay
  inside and outside the window, concurrent identical submission, conflicting
  principal/body reuse, unique one-admission-per-principal-slot enforcement,
  and request/canary half-state failure without repair. Transaction tests prove
  a first admission exposes request, queue, and canary rows together or none.
  Claim tests prove the database writes the first timestamp, worker, and
  controller generation atomically once across retries and takeover. Terminal-
  path tests cover success, failure, cancellation, reservation conflict, and
  recovery; every path writes canary `terminal_at` from the database clock in
  the terminal request transaction exactly once, and corruption tests reject
  missing, rewritten, inverted (`terminal_at < first_claimed_at`), or future
  evidence. They require `admitted_at <= first_claimed_at <= terminal_at <=
  observed_at` for claimed terminals and `admitted_at <= terminal_at <=
  observed_at` with an entirely null claim tuple for terminal-before-claim
  outcomes. Endpoint
  tests require the exact configured service-account principal and PostgreSQL
  HA mode, expose no generic request/payload/log fields, never re-enqueue a
  terminal replay, and keep both routes out of the default viewer allowlist.
  Rollback-target tests archive distinct legacy- and target-placement one-pod
  revisions, zero-change reapply the target revision before plan D, and reject
  the legacy revision after tainting or schema 009.
  Registry and payload-compatibility tests prove the no-op is SHORT,
  CONTROLLER, READ_ONLY, has a fixed result, and cannot select a handler or
  execution class. GC tests prove the fixed two-hour canary floor overrides a
  lower ordinary retention. Helm tests prove the typed principal matches
  `^sa-[0-9a-f]{16}$`, reaches only API pods, compares byte for byte at runtime,
  cannot be overridden by generic environment escape hatches, and is rejected
  unless service-account auth, PostgreSQL, and split-role HA are enabled.
- Observation tests exercise the minute CronJob's timeout/concurrency policy,
  least-privilege identities, append-only Object-Lock evidence, conditional
  writes, and seed-writer retry/collision behavior. Bucket-policy tests prove
  headerless and non-`*` PutObject is explicitly denied for both writer
  principals, exact conditional writes are allowed only on their disjoint key
  shapes, cross-prefix writes fail, and duplicate versions are rejected.
  Observer tests additionally cover
  digest-chain restart, first-slot
  bootstrap, current-slot POST-before-query ordering, predecessor projection
  verification, failed and missing minute resets, approval-end enforcement,
  lossless export, and independent 10,080-minute cleanup-CI recomputation.
  Fixtures include endpoint-bearing warm-standby and retained single-LB
  services plus a pool with no endpoint/LB.
- Helm tests cover valid HA render and every invalid prerequisite.
- Helm metrics tests prove executor and controller pods receive the fixed
  multiprocess environment, expose the configured port, and carry standard or
  dedicated pod-discovery annotations only when metrics are enabled. They also
  inject stale counter and histogram files from a pre-deploy hook and prove the
  directory reset is the final command before the Python supervisor exec.
  Custom-port rendering proves the API runtime, role pods, API Service, and
  bundled federation discovery all select the same non-default port.
- Helm snapshots prove Service selectors and PDB selectors do not overlap
  roles, every role PDB renders integer `maxUnavailable: 1`, and no role PDB
  renders `minAvailable`.
- Termination tests prove readiness fails before shutdown and the pre-stop
  budget fits within `terminationGracePeriodSeconds`.

### Live test-cluster conformance

- Record exact image digest, Git commit, Helm revision, pod UIDs, and database
  revision for every deployment.
- Run an in-cluster canary at at least 10 requests per second against the
  ClusterIP Service, plus continuous authenticated status/request probes.
- Gracefully delete one API pod, then the other, while requiring zero raw
  transport or HTTP failures.
- Hard-kill one API pod and prove retry-safe short requests complete without a
  user-visible failure or duplicate request ID. Record raw reconnects
  separately from final request outcomes.
- Drain a node containing an API pod when cluster capacity permits.
- Delete standby and active controller pods.
- Capture startup configuration from both original controller pods and require
  exactly 128 long workers in each. After deleting the active pod, require the
  promoted standby and its replacement to report the same 128-worker budget
  before takeover acceptance.
- Delete idle and busy executor pods.
- Terminate the active controller advisory-lock database session.
- During both controller failure modes, keep a long-running managed job active
  and prove its task remains nonterminal, its workload identity is not torn
  down, ownership advances to the promoted generation, and progress continues.
- Roll image A to B and B to A.
- Queue requests produced by both A and B while executor versions overlap and
  prove compatible workers claim each row.
- Assert:
  - zero ordinary-request HTTP 5xx responses,
  - at most documented reconnects for long streams,
  - no duplicate execution generation,
  - no duplicate controller generation,
  - no orphaned Kubernetes workload,
  - all Deployments and PDBs healthy,
  - migration Job completed once,
  - no stale Helm release or terminating pod.

### Static verification

- Run `bash format.sh --files` for changed Python files.
- Run targeted unit tests serially where macOS formatter or linter behavior
  requires it.
- Run Helm unit tests.
- Run the API server and PostgreSQL test suites.
- Run the full repository CI rollup on the exact pushed SHA for every stacked
  pull request before merge.

The typed RWX authority-fence and desired-scale role-PDB implementation was
statically accepted on 2026-08-08 with 22 verifier unit tests, all 32
control-plane Terraform tests, 164 targeted Helm cases across role Deployments
and disruption budgets, all 346 chart cases, strict Mypy, Isort, Pylint, and
repository diff checks. Its pull request still requires the full exact-head
repository CI rollup above before merge.

## Monitoring

No new telemetry pipeline is introduced. Existing Datadog collection receives:

- API availability and latency by role and pod.
- Queue depth, claim age, lease expiry, and interrupted count.
- Ready API, executor, and controller instance counts.
- Controller leader identity, generation, and lock-session health.
- Migration duration and result.
- Shared storage read/write sentinel failures.

Scraping is role- and pod-scoped. API, executor, and both controller targets
must be present independently; the API Service is not a proxy for metrics
emitted by controller or executor children. A controller handoff may reset a
pod-local counter but must not make the standby target unavailable. Rollout
queries aggregate across pod targets using ordinary Prometheus counter-reset
semantics and pair every metrics-dependent gate with gap-free target-health
evidence. A missing target or scrape gap invalidates the sample and resets any
continuous observation clock.

Kubernetes events, pod logs, PostgreSQL rows, Helm status, and active traffic
canaries remain separate evidence sources. A green pod rollout alone does not
prove request continuity.

Rainier's cleanup clock is exactly 168 continuous hours. It starts only after
the guarded HA revision, controlled rollout and controller-takeover exercises,
fix-forward one-pod rollback proof, and legacy-node taint/drain have all been
accepted after the complete C-hub/C-sky migration and empty-node proof. A
wall-clock timer, PR age, or Helm uptime never unlocks cleanup.

Platform IaC installs a `rainier-ha-observer` CronJob inside the private cluster
with `* * * * *`, `concurrencyPolicy: Forbid`, a 30-second starting deadline,
and `activeDeadlineSeconds: 55`. Its narrowly scoped identity has read-only
Kubernetes, PostgreSQL, and Datadog access plus AWS permissions limited to
backup observation, deterministic evidence-key `Get`, and conditional append;
it has no bucket-list permission. A distinct read-only exporter identity owns
attempt-prefix List/Get. At most one 250m CPU, 256-MiB observer pod is
represented by the accepted observer-reservation probe until the post-HA
retirement gate.
Every UTC minute it evaluates the immediately preceding closed UTC-minute slot
and writes one canonical sample to a
dedicated versioned, destroy-protected S3 evidence prefix using a unique key and
`If-None-Match: *`. Its Pod Identity can put new
sample/reset/checkpoint objects but cannot overwrite or delete them. The bucket
policy independently denies missing or non-`*` conditional headers for both
this prefix and the disjoint seed prefix; identity policy alone is not the
append-only boundary.
Each sample binds the attempt ID, accepted fence digest, exact direct-Helm
release-bundle digest and infrastructure commits, node-group identities,
paid-capacity approval ID and hard
end, predicate result and source query IDs, prior-sample digest, and observer
build digest. Failed predicates and missing UTC-minute slots append explicit
reset events. A Job reads and validates only the deterministic preceding-minute
object (or the exact digest-sealed activation seed), so every fresh CronJob
process resumes without a mutable local clock or an ever-growing S3 scan; a
missing predecessor starts a reset chain. The exporter, not the minute Job,
scans and validates the full chain.

The activation plan takes an explicit, reviewed attempt UUID and first UTC slot
far enough in the future for the approved apply to finish. Its immutable seed
binds the exact observer principal, first slot, canonical POST body,
idempotency digest, and derived request ID. It deliberately contains no claimed
admission or execution time and is not an accepted minute. A digest-pinned
conditional-write helper creates the seed before the CronJob can be
unsuspended. A retry accepts an existing seed only after proving its exact bytes,
digest, key, and singleton Object-Lock version; a collision cannot create a
second version. Tests inject lost acknowledgement, exact retry, mismatched
content, and duplicate-version state. The CronJob at the
first slot reads that seed and POSTs the first canary within the database-clock
30-second insertion window; if the apply or first Job misses the slot, no value
is backdated and the chain records a reset. A fresh attempt/seed is required if
the configured first slot was never admitted.

For every later job starting at minute `T`, the observer first POSTs or exactly
replays the canary for slot `T`, then evaluates closed slot `T-1`. It GETs only
the predecessor request committed by the seed or preceding sample and requires
matching attempt, slot, principal-bound digest and request ID, terminal
`SUCCEEDED`, a nonnull production controller generation/worker instance, and
`0 <= first_claimed_at - admitted_at <= 60 seconds` plus nonnull database-clock
`first_claimed_at <= terminal_at <= admitted_at + 75 seconds` and
`terminal_at <= observed_at`. The request row's `finished_at` is
not accepted as timing evidence. Because the predecessor may have been admitted
at the end of its 30-second window, the Job bounded-polls that exact GET through
two separate database-derived deadlines. A first claim must appear by
`admitted_at + 60 seconds`; after a timely claim, terminal `SUCCEEDED` and its
once-written `terminal_at` must appear by `admitted_at + 75 seconds`. Polling stops at the
earlier of the applicable database deadline and the Job's own 50-second work
deadline. This gives a legally last-second claim 15 seconds to finish while
still leaving at least five seconds under `activeDeadlineSeconds: 55` for the
conditional S3 write. In the worst cadence case, a predecessor admitted at
`T-1 + 30 seconds` has a terminal deadline of `T + 45 seconds`, so
`concurrencyPolicy: Forbid` does not skip the next minute. A Job admitted near
its `startingDeadlineSeconds: 30` limit still reaches the predecessor's latest
legal claim boundary near `T + 30 seconds`; an immediate GET may not falsely
reset a valid 31-to-60-second claim, and a timely claim may not be called failed
before its separate terminal deadline. It then writes the sample for `T-1`,
including the exact successor request identity. A late new POST,
missing/corrupt row, failed or late terminal status, mismatched projection, or
claim outside the bound emits a reset instead of reconstructing evidence from
request timestamps, queue timestamps, heartbeats, or logs. Native-cadence data
still has a declared maximum age checked in every minute's sample.

An exporter on the same approved network verifies the append-only chain and
materializes the canonical platform-repository artifact
`deployment/evidence/rainier-ha-observation.json`. It contains every accepted
minute or an equivalently lossless interval encoding, the continuous start and
end, all resets and reasons, source query IDs, fence and release identities,
approval hard end, chain head and backing S3 object/version manifest, and the
computed `eligible_at`. Cleanup CI independently validates the schema and hash
chain, expands the minute sequence, rejects a gap or failed predicate, requires
at least 10,080 consecutive accepted closed UTC-minute slots, and requires
`eligible_at <= approval_hard_end`. A scheduled evidence workflow may refresh
the artifact and cleanup PR, but cannot make the gate pass early. Any direct
Helm change, rollout revision, observer gap, evidence mismatch, or projected
eligibility beyond the approval hard end resets the clock and, for the latter,
also requires renewed identified paid-capacity approval.

The following are evaluated in every minute unless their native cadence is
slower; any failed, stale, or missing observation resets the clock:

- authenticated in-cluster and external-ingress API canaries succeed with no
  HTTP 5xx or terminal request failure. For every currently `READY`, endpoint-
  bearing non-pool Serve service, an authenticated LB-local health/capacity
  control-path probe succeeds without enqueueing, dispatching, or starting a
  backend. The observer never submits synthetic inference. At a declared native
  cadence it validates independently observed, authenticated organic data-plane
  evidence when such traffic exists; absence of organic traffic is not a failed
  predicate. A zero-backend service instead proves scale-to-zero plus endpoint/
  LB health and zero observer dispatch; the absence of a backing replica is not
  a failure. Any observer-caused capacity
  transition, minimum-replica drift, or ordinary on-demand activation fails the
  minute. Pools have no inference endpoint and are verified as having neither
  an endpoint nor an external load-balancer Deployment;
- API, executor, and controller Deployments each have desired, current, Ready,
  Updated, and Available counts exactly two. Every role PDB has
  `currentHealthy=2`, `desiredHealthy=1`, `expectedPods=2`, and
  `disruptionsAllowed=1`; a surge sample is ineligible. There are no unexpected
  restarts, CrashLoops, Pending pods, or unplanned rollout revisions;
- exactly one current controller generation owns leadership, no stale owner or
  duplicate execution appears, expired active leases remain zero, each
  synthetic request is claimed within 60 seconds, and interrupted-request
  counters have no unexplained increase;
- only the six labeled target nodes are schedulable for role pods; none reports
  MemoryPressure, and the 110-GiB/16-CPU aggregate plus 32-GiB/4-CPU-per-zone
  unrequested reserves remain satisfied after accounting for DaemonSets,
  external load balancers, and all other workloads;
- EFS CSI pods and mounts stay healthy, both `rwx-state-monitor` replicas and
  both `rwx-authority-monitor` replicas stay Ready, the read/write state
  sentinel and exact-fence read-only digest checks have zero failures and
  sub-one-second p99 latency, and EFS `PercentIOLimit` remains below 80%;
- the newest scheduled EFS recovery point is `COMPLETED` and less than 26 hours
  old, with no failed or expired backup job;
- PostgreSQL CPU remains below 70% at p95, connections below 70% of the
  configured maximum, and connection/error counters show no HA-attributable
  increase; and
- fleet reconciliation remains `READY`; each endpoint-bearing non-pool service
  has exactly the replica count required by its persisted durable external-LB
  mode (two for warm standby, one for a retained single-LB mode); its LB sync
  and non-dispatch control-path probe stay green, and independently generated
  organic data-plane evidence stays green whenever such traffic exists. The
  observer never creates that traffic. An unrelated update may not silently
  change that durable mode, and pools continue to require zero endpoint/LB
  replicas.

The completed isolated restore rehearsal and its restore-specific sentinel and
inventory evidence are a separate cleanup prerequisite; they do not repair or
backdate a reset clock.

## Rollback

- Migrations are additive until the M5 cleanup gate.
- Every milestone can roll back its Deployment image without dropping new
  tables.
- HA mode can be disabled to return to one `--role=all` pod on the accepted RWX
  claim and PostgreSQL request store during the migration window.
- Rolling back does not delete request, queue, or controller ownership rows.
- A failed migration hook blocks the rollout and leaves the previous
  Deployments serving.
- Production rollback is a new reviewed direct-Helm fix-forward upgrade pinned
  to the last accepted M1-or-newer image and complete values. Native
  `helm rollback` to a
  stored pre-M1 revision is not a supported recovery path: it can select the
  SQLite backend, the legacy claim, and the old non-hook migration resource.
- Disabling `apiService.metrics.enabled` removes all role metrics ports and
  scrape annotations together; it is an observability rollback and cannot be
  used to claim a metrics-dependent rollout gate.
- The isolated test release remains installed and healthy after final
  conformance. Failed revisions, one-shot canaries, stale migration Jobs, and
  abandoned test workloads are removed. Its dedicated PostgreSQL and EFS
  resources remain only as declared dependencies of the running release.

The storage cutover is a deliberate rollback boundary:

1. Before the RWX completion fence commits, operators may restart the last
   accepted one-pod legacy-RWO plus PostgreSQL revision only through abort phase
   A (remove all writers while API stays zero), stable no-fence proof, and abort
   phase B (apply-time no-fence recheck and restart). It does not import SQLite
   rows or alter PostgreSQL. The work cutoff reserves 75 minutes for this path,
   and API readiness is due by the fixed 120-minute deadline.
2. The four generation-specific snapshot IDs, bounded final sync, target
   verification, observed zero time, and unextended deadlines are prerequisites
   to committing the digest-sealed RWX fence.
3. Once that fence commits, the legacy RWO claim is never writable or
   selectable by a workload. Recovery verifies the committed generation while
   the API remains zero and advances through writer retirement to the accepted
   one-pod RWX plus PostgreSQL compatibility revision; it does not recopy.
4. After that compatibility revision is accepted, HA rollback is a reviewed
   direct-Helm fix-forward revision using the same RWX claim and PostgreSQL store.
   Before schema 009 and after C-sky, its exact image and complete final-target
   values are `compat-one-pod-rwx-008-009-target`, exercised in both one-pod/all-
   role and split-role form. Once schema 009 commits, only that archived and
   post-009-exercised target-placement artifact may be selected before fixing
   forward; the phase-7b legacy-placement revision, phase-6 exact-008 binary,
   native Helm rollback, and selection of the retained legacy claim are
   forbidden.
5. The legacy PVC, PV, EBS volume, baseline and quiesced source snapshots, and
   encrypted snapshot copies remain retained for audit and separately
   authorized disaster recovery; they are not a second writer or an ordinary
   rollback target.

This avoids an unsafe dual-write protocol and makes the irreversible boundary
explicit.

## Rejected Alternatives

### Sticky routing plus per-pod state

Rejected because cookies are a probability reducer, not an ownership model.
Pod deletion still loses state, and uploads can cross replicas.

### Forward requests to the pod that created them

Rejected because it turns pod identity into durable routing state, fails when
the owner pod disappears, and makes draining dependent on proxy chains.

### Run every controller in every API replica and rely only on local PID checks

Rejected because PIDs are pod-local and do not fence cloud mutations.

### Automatically replay all expired request leases

Rejected because a worker can lose database connectivity after issuing a cloud
mutation. Automatic replay can duplicate external side effects.

### One large implementation pull request

Rejected because storage, queue, runtime roles, controller fencing, and Helm
rollout must remain independently revertible.

## Legacy Code Removal Map

The following paths are intentionally retained during the migration window.
They must be deleted, not merely left dormant, after the M5 fleet gate.

### Local request database

- `sky/server/requests/requests.py`
  - `SqliteRequestBackend`
  - module-level SQLite connection initialization and `close_db_async()`
  - `recover_db_and_logs()`, `reset_db_and_logs()`, and startup wipe branches
    whose only purpose is recovering the local request database
  - per-request `filelock` serialization used only by the SQLite backend
- `sky/server/requests/storage.py`
  - fallback construction of `SqliteRequestBackend`
- `sky/server/constants.py`
  - `API_SERVER_REQUEST_DB_PATH`
- SQLite-specific request recovery tests, including tests whose contract is
  that a pod-local database is wiped or recovered

Retained local or controller SQLite databases outside the central API request
path are not part of this deletion.

### In-process request queue

- `sky/server/requests/queues/mp_queue.py`
- `sky/server/requests/queues/local_queue.py`
- `LocalQueueBackend`, `MultiprocessingQueueBackend`,
  `LocalQueueFactory`, and `MultiprocessingQueueFactory` in
  `sky/server/requests/queues/base.py`
- Queue-manager startup, port checks, process tracking, and shutdown in
  `sky/server/requests/executor.py` and `sky/server/server.py`
- `DEFAULT_QUEUE_MANAGER_PORT` and its tests
- `preconditions.background_tasks` and API-process
  `Precondition.wait_async()` scheduling after every precondition type has a
  durable queue representation

Plugin queue interfaces may remain only if a supported external backend still
uses them. The default must no longer silently select a pod-local queue.

### All-in-one server role

- `--role=all` compatibility entrypoint and chart rendering
- The monolithic startup block in `sky/server/server.py` after callers have
  migrated to explicit roles
- FastAPI lifespan submission of `daemons.INTERNAL_REQUEST_DAEMONS`
- Request-dispatch branches that allow normal executors to run Serve or
  managed-jobs controller-starting handlers after every such handler has
  registry-owned `controller` routing
- API-pod controller and executor resource reservations
- Shutdown logic that coordinates all Uvicorn and executor processes through a
  pod-local file lock

API-local Uvicorn worker coordination may remain where it does not imply
cross-role ownership.

### Adjacent-version controller cutover guards

- `recent_legacy_controller_consumers()` and its M2 handler-advertisement
  intersection in `sky/server/requests/postgres.py`
- `SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS`, the
  `waiting-for-executor-cutover` controller supervisor phases, and the
  pre-acquisition, post-acquisition, and active-leader regression probes in
  `sky/server/runtime.py`
- The controller Deployment calculation and injection of executor termination
  grace plus ten seconds
- The `all`-role null-generation exception in
  `_controller_claim_is_current()` and the queue-construction exception that
  permits controller execution without a generation only in that role
- Tests whose only contract is safe M2 or `all`-role overlap

Delete these only after M3 is outside the fleet rollback window and no
registered `all` or M2 executor instance can advertise controller handlers.
The controller generation, dual advisory-lock proof, durable action
reservations, and stale-write predicates are steady-state safety mechanisms
and must remain.

### Colocated controller API bootstrap

- The `all`-role branch in managed-jobs recovery that clears
  `SKYPILOT_API_SERVER_ENDPOINT` and calls `sky api start`
- Tests whose only contract is that a consolidated controller starts a
  loopback API server in its own process namespace

Delete these with the `all` role. Explicit controller and executor roles must
continue to use the stable API Service endpoint; that routing is steady-state
behavior, not a migration guard.

### Pod-local artifact lifecycle

- `LocalFilesystemBlobStorage.reset_on_startup()` destructive client-state
  cleanup
- The default local blob selection in
  `sky/server/blob/blob_storage.py` for remote HA deployments
- Startup logic that wipes uploaded task files or logs because one pod
  restarted
- Chart support for HA mode with `storage.enabled=false` or ReadWriteOnce
  storage

`LocalFilesystemBlobStorage` may remain for a standalone local developer
server only if it is explicitly selected and cannot be used by HA mode.

### Routing workarounds

- HA reliance on `ingress.sessionAffinity`
- `SKYPILOT_APISERVER_UUID` where it exists only to route follow-up calls to
  the creating pod
- `SKYPILOT_ROLLING_UPDATE_ENABLED` branches that switch request behavior
  based on pod overlap rather than durable ownership
- Non-idempotent ingress retry annotations that are no longer required after
  client request IDs and transactional enqueue provide retry safety
- Documentation that instructs operators to use sticky sessions for upload or
  request correctness
- Any pre-stop or ingress retry setting whose only purpose was hiding pod-local
  ownership. The readiness-first drain remains because endpoint propagation is
  a Kubernetes transport concern.

OAuth session cookies are authentication state and are not part of this
removal.

### Recreate and experimental chart paths

- The `replicas > 1 is not well tested` warning
- Recreate as the default for guarded HA installations
- Chart branches that allow a supposedly HA release to render one API replica
- Documentation describing the guarded HA configuration as experimental

Recreate may remain for explicit standalone local compatibility until the
all-in-one role is removed.

### Pickled request execution envelope

- Pickled `entrypoint` and `request_body` persistence in
  `sky/server/requests/requests.py`
- Database decoding of arbitrary callables
- Compatibility aliases in the handler and payload registries after their
  producer versions leave the rollback window
- Tests that define persisted callable pickles as the current request storage
  contract

Pickle-based return values and serialized domain objects are separate
compatibility surfaces. They require their own migration before removal and
must not be silently claimed by this request-dispatch deletion.

### Persisted Serve recovery script compatibility

- Recovery behavior in `sky/serve/serve_state.py`,
  `sky/serve/serve_utils.py`, and `sky/serve/server/impl.py` that treats
  `serve_ha_recovery_script` as the authoritative or only copy of controller
  inputs
- Embedded references to pod-local `~/.sky/serve/<service>/config.yaml`,
  `task.yaml.tmp`, or equivalent files
- Compatibility stubs that exist only to deserialize and execute old recovery
  scripts

The `serve_ha_recovery_script` table may be dropped only after every active
service has durable version YAML and the rollback window excludes readers that
require the script.

## Completion Evidence

This migration is complete only when all of the following are true:

- M1 through M4 commits are merged with full CI on their exact pushed SHAs.
- The merged image is deployed to `skypilot-ha`.
- Live conformance passes for API, executor, controller, migration, rollback,
  and cleanup cases.
- The final Helm release is deployed, all role Deployments are healthy, and no
  test workload or stale migration resource remains.
- The `skypilot-ha` namespace, release, PostgreSQL PVC, EFS access point, and
  `skypilot-ha-efs` StorageClass are retained as the declared clean test
  deployment. One-shot canaries, failed revisions, and unrelated
  cluster-scoped residue are absent.
- The design reflects the code that actually shipped.
- M5 removals are either merged after fleet evidence or tracked as explicit
  gated deletions with owners and objective removal conditions. Passing a test
  rollout alone is not evidence that the fleet rollback window is closed.

## Adversarial Review Record

### Review 1: RESHAPE

The first challenge found four blocking omissions:

- API-process precondition tasks were not durable.
- Pickled callables and bodies had no mixed-version execution contract.
- Zero-error pod deletion lacked readiness-first endpoint draining and a valid
  in-cluster measurement surface.
- The rollback text incorrectly allowed returning to a pre-PostgreSQL image
  after new requests had been written.

This revision makes preconditions transactional queue state, defines a stable
versioned handler envelope, specifies Kubernetes drain behavior and ClusterIP
canaries, and establishes an explicit irreversible storage-cutover boundary.

### Review 2: PURSUE

The final challenge checked the revised design against the current request
backend, queue manager, precondition scheduling, consolidated jobs and Serve
controller paths, authentication session storage, blob and log providers, and
Helm migration and RollingUpdate templates.

- The work remains necessary: the current RollingUpdate option still runs
  pod-local request and execution ownership, so chart settings alone cannot
  provide multi-replica correctness.
- Sticky routing, request forwarding, or a warm standby do not satisfy the
  durable ownership and replica-independent follow-up contract.
- The migration is split at independently reversible boundaries, and the
  irreversible PostgreSQL cutover is explicit.
- Queue delivery survives owner loss, controller-starting requests are
  registry-routed to the elected controller, and durable generation and action
  reservations fence stale controller work.
- Version negotiation makes mixed consumer and producer rollouts safe, while
  destructive cleanup remains gated on fleet evidence.
- Graceful deletion and hard-crash behavior have separate measurable
  guarantees, and the final test state is a retained healthy isolated release
  with no scratch residue.

No blocking correctness gap remains in the design. The highest implementation
risks are complete handler classification, reconciling ambiguous provider
actions, and preserving the current single-pod compatibility path through M1.
The milestone tests make each of those a release gate.

### Review 3: PURSUE

The M3 implementation review exposed an adjacent-version race not covered by
the original design: an M2 executor may keep controller-capable workers alive
after publishing its draining state. The amended design uses the durable
instance advertisement and last heartbeat as a cutover gate for the complete
executor termination grace plus ten seconds. Both controller replicas remain
unready while that gate is closed, and an active leader exits if a legacy
consumer reappears. This closes the rollout and rollback window without a new
coordination system or a permanent compatibility path.

### Review 4: PURSUE

The exact M3 write predicates exposed a second fencing gap: an unreleased
leadership row alone can outlive its advisory-lock session until the standby
advances the generation. A backend PID check alone also admits PID reuse. The
revised contract binds each generation to a second, unique advisory lock on
the election-lock session and proves both exact lock keys through `pg_locks`.
This immediately invalidates the old generation when its session disappears.
The bounded extra lock-manager and predicate cost applies only to
controller-owned work and is preferable to a time-based split-brain window.

### Review 5: PURSUE

Live managed-job and SkyServe launches exposed a role-separation gap: their
controller subprocesses still submit nested SDK requests to a loopback API
server that no longer shares their pod. Restoring an API sidecar would
reintroduce the lifecycle and state coupling this design removes, while
rewriting every nested action as direct controller logic would be a much
larger compatibility surface.

The stable private API Service is the smallest durable boundary. Explicit
non-API roles fail closed if that endpoint is local or unhealthy and never
start a listener; only the compatibility `all` role retains local bootstrap.
The endpoint is reserved in Helm, and both it and an optional operator-supplied
service-account credential are stripped from persisted request environments.
This keeps mixed-version rollback schema-neutral, prevents client endpoint or
credential injection, and preserves authenticated installations without
adding a second routing or identity system.

### Review 6: PURSUE

The live generation 12 to 13 database-session failure test exposed a
managed-job race that the request-level fence did not cover. A job was briefly
`WAITING` with no PID, so recovery skipped its drain wait. A detached scheduler
from the old pod then reclaimed it and wrote a pod-local PID after the new
leader's snapshot. Status refresh treated that soon-dead foreign PID as a
current local crash, tore down the workload, and marked the job
`FAILED_CONTROLLER`.

An unconditional timing delay or a shared-file check in the scheduler would
reduce the observed window but would not establish ownership after a session
loss or network partition. The additive managed-job generation columns and
same-transaction leadership row lock reuse the existing outer fence without a
second election system. Ordered stale-owner recovery and a detached-controller
watchdog close both sides of the race. The permanent cost is two nullable
columns, one claim predicate, and one ownership probe; that is justified by
the otherwise destructive false terminalization seen in the live test.

### Review 7: PURSUE

The first generation-fenced live deletion preserved the workload and advanced
its progress, but the replacement row stayed `LAUNCHING`. The resumed task
correctly bypasses `StrategyExecutor.launch()` because its remote job is
already `RUNNING`; that also bypasses `scheduled_launch()`, which is the only
normal writer of the `ALIVE` schedule state.

Leaving this state stuck would make the ownership model internally
inconsistent and misclassify a monitoring controller as launching for the
rest of a long job. Re-entering the launch context is worse because it couples
a pure monitoring resume to provider-side launch behavior. The smallest sound
fix is one generation-fenced `LAUNCHING` to `ALIVE` transition after durable
resume classification and before the monitor loop. It adds no schema, process,
or steady-state probe.

### Review 8: PURSUE

The controller-loss review rejected catchable scheduler termination as a
fence. A detached scheduler that receives ordinary `SIGTERM` can interpret it
as user cancellation and enter workload cleanup even after its outer
generation has lost PostgreSQL leadership. The implemented fail-stop path
revalidates the scheduler process start time, terminates the complete process
tree with a non-catchable signal, and has a subprocess sentinel proving that
coroutine finalizers do not run. Durable user cancellation remains a separate
intent path and retains its normal cleanup behavior.

### Review 9: PURSUE

The M4 review required rollout safety to be a guarded contract rather than a
collection of best-effort defaults. It confirmed four gates:

- The migration must block before target-image pod creation, which requires a
  pre-existing connection Secret for a pre-install hook.
- Pre-stop sleep must be long enough for readiness propagation and leave a
  separate process-termination budget.
- Drain acceptance must prove the direct endpoint and PostgreSQL lease
  transition in addition to the Kubernetes Pod condition.
- Guarded HA must reject custom affinity annotations, not only suppress the
  chart's default cookie.

The chart validation and conformance harness now enforce those gates.

### Review 10: PURSUE

The exact-head review found a remaining handoff window in managed-job status
refresh. The refresh thread could decide that a local controller died, begin a
provider teardown, and only afterward attempt the generation-fenced terminal
write. If the outer lock session disappeared between those operations, the
replacement generation could recover the job while the old thread was still
tearing its workload down. The same stale child could also submit a fresh
nested SDK mutation through the stable API Service because admission did not
carry the originating controller generation.

Holding a database transaction across a minutes-long provider call was
rejected because connection loss would silently discard the fence and the row
lock would delay promotion. Marking `DONE` before cleanup was also rejected
because a crash would permanently hide leaked resources from retry. The
revised ordering commits the exact-snapshot terminal task decision first,
keeps the schedule state eligible for cleanup retry, performs idempotent
teardown, and marks `DONE` last. A replacement generation adopts terminal
cleanup but never recovers the workload. Controller-origin admission metadata
closes the independent stale-child submission path at the API boundary. This
uses the existing generation and task state, adds no schema, and preserves the
compatibility `all` role.

### Review 11: legacy queue enqueue shape

The first complete cloud test rollup found that compatibility queues were
receiving the new durable `QueueItem` object. Existing queue plugins and the
shared API test fixture still implement the documented three-tuple `put`
contract. The PostgreSQL path already inserts the queue row in the request
transaction and never calls the local enqueue closure, so carrying durable
claim metadata into that closure had no HA value.

Non-durable enqueue therefore retains the historical
`(request_id, ignore_return_value, retryable)` tuple. PostgreSQL attaches
generation and claim-token metadata only when it dequeues a durable row. A
focused contract test now fails if the compatibility shape changes before the
M5 plugin and local-queue removal gate.

### Review 12: split-role metrics completeness

The production-readiness review challenged the assumption that the existing
API metrics endpoint represented controller and executor work after the M2/M3
role split. It did not. Prometheus multiprocess files are local to a pod, the
API Service selects only API pods, and the controller did not start its
background loop while it was a standby. Metrics emitted by controller request
workers and child controllers therefore could not appear on the API target.

Using shared storage for multiprocess files was rejected because Prometheus'
client registry is process-host local and stale PID files from different pods
would collide. Registering shared-state collectors on every role was also
rejected because it would multiply identical database-derived series. The
correct boundary is one metrics server and one clean multiprocess directory
per role pod, with independent pod discovery. Metrics serving starts before
controller election, while leader-only maintenance retains its existing
post-fence lifecycle. Built-in shared-state collectors and plugin custom
collectors retain API/all ownership; plugin role-local instruments use the
multiprocess registry. The chart creates the directory early for hooks, then
clears and recreates it after every arbitrary hook/setup command immediately
before `exec`, because dead counter and histogram files are not reaped. The
runtime fails closed when a split role lacks its multiprocess directory or the
metrics listener cannot bind, and a post-start listener failure terminates the
role; both runtime and Helm validation reject a role-health and metrics port
collision. Rollout acceptance treats every role target and scrape interval as
required evidence.

### Review 13: guarded-rollout completion

The exact-head rollout review found that the post-seed reconciler targeted
Deployments derived from `release_name`, while the chart
also permits `fullnameOverride`. A caller using that escape hatch could seed
the database successfully and then fail to restart the actual workloads,
leaving stale configuration in memory. Deriving only the Deployment names
dynamically would still leave infrastructure-managed Pod Identity associations
pointing at the release-derived service account, so the current chart/direct-
Helm contract rejects `fullnameOverride`, retains the release name as its single
naming authority, and passes exact role Deployment names to the independent
seed reconciler.

The same review made the capacity boundary explicit. Both Helm and the
post-seed reconciler may roll API, executor, and controller Deployments at the
same time. `maxSurge: 1` is therefore a per-role bound, not a whole-release
bound, and activation must prove aggregate headroom for as many as three surge
pods. Serializing only post-seed waits would not protect Helm upgrades. The HA
guard accepts the equivalent zero-unavailable forms `0` and `0%`, but keeps
the surge bound as absolute integer `1` so percentage rounding cannot silently
increase temporary capacity. Focused boundary tests cover all three choices.

### Review 14: post-PostgreSQL Rainier storage refresh

The 2026-08-08 refresh rejected the old production stack's attempt to repeat
the SQLite-to-PostgreSQL importer. Rainier's one-way cutover completed in
1.1.1089 and the live all-role pod already uses PostgreSQL with its durable
gate. Re-running the importer while changing storage would conflate two
irreversible boundaries and could rewrite authoritative request history. The
remaining finalizer is therefore storage-only: it requires API zero, fences
both filesystems, copies and verifies the final delta, commits the RWX fence,
and validates PostgreSQL evidence read-only.

The same review rejected treating a Helm keep annotation as durable ownership.
The exact legacy claim and volume remain the pre-fence recovery and audit
source, so their live metadata must be adopted without a Terraform/Helm
ownership fight and the PV's reclaim policy must become `Retain` before Helm
stops rendering the claim. No snapshot existed at audit time. After API zero
and quiescence, the final gate therefore requires a completed source snapshot
and completed encrypted copy before the RWX fence can commit. The review also
rejected a full-speed post-copy hash pass after a bandwidth-limited copy; every
source read in the online stage shares the same bounded-I/O contract.

Finally, the review separated migration-writer retirement from the first RWX
workload start. An inert generation flag is not sufficient steady-state proof:
the Jobs, scripts, ConfigMaps, RBAC, and arming inputs must be absent while the
API is still zero. The following one-pod RWX/PostgreSQL revision is the storage
rollback target; only its acceptance unlocks capacity expansion and role-split
HA.

### Review 15: fail-closed Rainier activation

Exact-head review on 2026-08-08 rejected a writable in-tree fence, an abort that
could race fence publication, a deadline chosen before API zero, memory
overcommit, and a capacity proof that could borrow the legacy nodes. It also
found that saved cluster plans embedded a short-lived EKS token and that the
cleanup clock, restore test, EFS throughput mode, controller worker proof, and
paid-capacity approval were not objective.

The corrected contract isolates a digest-sealed fence behind a distinct EFS
access point and read-only workload claim, with a typed chart verifier and no
authority mount in application containers. A direct-Helm quiesce artifact
records one generation's observed zero time and unextendable deadlines; a
graph-ordered finalizer plan cannot snapshot before quiescence. Abort first
removes all writers through an infrastructure plan, then proves absence, and
only a direct-Helm fix-forward artifact may restart legacy. Any
retry gets entirely new evidence. Memory requests equal limits, the legacy
group is tainted and drained before six-node-only proof, and capacity and HA
are separate stack stages.

The same correction requires apply-time exec authentication, an approved
non-admin deploy context, General Purpose plus Elastic EFS, an isolated
content-verified restore, exact controller/executor worker evidence, explicit
on-demand approval, and a 168-hour clock whose enumerated health, capacity,
storage, backup, database, and data-plane gaps all reset it.

### Review 16: crash and evidence closure

Exact-head adversarial review on 2026-08-08 found that the prior intent did not
bind its preseed/baseline evidence, rename publication could replace an existing
fence, a hung provider apply could consume the abort reserve, and the two abort
PRs were not causally stacked. It also found that a live EFS backup cannot be
compared with the old cutover manifest as though it were an application-
consistent snapshot. The corrected contract binds every generation input,
publishes intent and fence with no-clobber hard-link semantics and exact-
existing recovery, gives an out-of-process watchdog authority to cancel at the
fixed cutoff and require fresh abort plans, roots abort-B on abort-A, and uses a
recovery-point sentinel plus restore-specific inventory for crash-consistent
backup evidence.

The same review separated isolated role-shaped scheduler probes from HA
activation, assigned explicit owners to both state and authority monitoring,
pinned managed-node-group `MINIMAL` updates and repair-off settings to bound
the configured normal fleet to nine nodes, and made extended billed time
require renewed identified approval. Review 19 below accounts separately for
the legacy ASG's one-node `AZRebalance` transient. Finally, cleanup now depends
on an append-only, reset-aware one-minute
evidence chain and CI-validated 10,080-minute artifact. Serve predicates derive
their endpoint and external-LB expectations from pool status and each persisted
durable mode rather than assuming every service owns two load balancers.

### Review 17: desired-scale disruption semantics

The final cross-stack audit on 2026-08-08 found that the chart's fixed
`minAvailable: 1` role budgets diverged from Rainier's desired-scale
`maxUnavailable: 1` contract. A follow-up semantic review rejected the initial
claim that changing the field would cap voluntary evictions at one during a
surge: Kubernetes derives `desiredHealthy` from the owning Deployment's
desired `.spec.replicas`, so with desired two and three currently healthy pods,
either form can report two allowed disruptions. The corrected contract is
instead explicit: integer `maxUnavailable: 1` preserves desired replicas minus
one and automatically raises the healthy floor if a role is deliberately
scaled above two. Deployment `maxUnavailable: 0` independently protects the
rolling update. Focused Helm tests assert the scale-aware PDB field without
claiming it is a surge mutex.

### Review 18: durable observer admission and capacity prerequisites

The final cross-stack implementation audit found that the proposed minute
observer had no API contract capable of proving its core predicate. HTTP
middleware assigned a random request ID, the queue exposed only mutable
timestamps, and the activation seed could not truthfully contain a future
database admission time. Treating `created_at`, queue `updated_at`, a heartbeat,
or a log line as first-claim evidence would let retries and clock skew produce a
false 168-hour acceptance. The design was reshaped around a private,
principal-bound, idempotent controller canary: schema 009 stores database-clock
admission and first-claim evidence, API 74 exposes only the restricted
projection, and the scheduled seed binds a future request identity without
claiming that admission already happened. The minute pipeline submits the
current request and evaluates the preceding one; Review 19 below adds bounded
polling for the predecessor's remaining legal claim interval.

The same audit found that the pinned AWS provider could not encode `MINIMAL`,
the VPC CNI did not enforce the probes' deny-all policy, mutable launch-template
selection could cause replacement surge, and the pause image
was not digest-pinned. The corrected stack adds reviewed provider and CNI
prerequisites, exact live node-group pins captured under the approved identity,
no replacement-surge lifecycle, the verified immutable pause digest, and an
observed no-egress test before paid capacity.

### Review 19: observer and cross-stack adversarial closure

Independent review of the first API-74/schema-009 specification found that an
authorized principal could vary attempt UUIDs and flood one minute, admission
atomicity named only part of the three-row write, a predecessor admitted near
second 30 could still claim legally after the next Job's first GET, an ordinary
Terraform S3 object could create duplicate Object-Lock versions on retry, and a
low ordinary request-retention setting could erase next-minute evidence. The
corrected contract adds unique `(principal_id, slot)` admission, explicit
request/queue/canary transactionality, bounded predecessor polling, a
digest-pinned conditional seed writer ordered before final unsuspension, and a
fixed two-hour canary GC floor. A compatibility-only predecessor release widens
all exact-008 consumers before any process is allowed to create schema 009.

The same review recomputed capacity with the live 96-GiB compatibility pod,
split target creation from legacy taint/drain, accounted for the legacy ASG's
possible tenth `AZRebalance` instance, preserved low-priority reservations
until higher-priority real pods replace them, and retires the observer
reservation before activation behind a fresh capacity gate instead of recurring
preemption. It also separates deterministic no-List minute-observer access from
the exact-key seed writer and prefix-listing exporter identities, and adds a
pre-authorized cost-reducing emergency plan and cleanup reserve. Cluster-wide CNI enablement
now requires a complete live policy/reachability matrix, an explicitly bounded
standard-mode programming/readiness proof, and matched per-zone controls before
and after target creation; the provider
rollback claim requires a disposable exact-state rehearsal. Independent
adversarial re-review of this exact revision remains required before
implementation acceptance.

### Review 20: sole-node-group and conditional-write closure

The exact Review-19 adversarial pass rejected implementation acceptance. Repo
and live evidence showed that Rainier's only legacy managed node group also
hosts CoreDNS, the EBS CSI controller, Argo CD, External Secrets, the AWS load-
balancer controller, external-dns, golink, Datadog clusterAgent, and additional
hub workloads. Tainting and draining it directly would strand the hub. The
corrected contract therefore makes a complete live owner/template/resource and
dynamic-controller inventory a paid-capacity prerequisite, freezes who may
tolerate the target taint through admission, models `H_mem`/`H_cpu` and
`D_zone`, creates targets without changing legacy, migrates every hub owner
reversibly through C-hub, relocates SkyPilot separately through C-sky, and lets
only plan D taint an already empty legacy group. Both pre-HA unwind and post-HA
legacy-zero cost-stop branches are authored before target creation.

The same pass found that versioning plus Object Lock does not by itself prevent
a second retained version. Both seed and minute writes now require exact
`If-None-Match: *` from a digest-pinned helper, while a bucket-policy Deny makes
missing or non-`*` headers impossible even when an identity policy grants
`PutObject`. Disjoint key authority, singleton seed retry proof, collision/reset
semantics, cross-prefix denial, and duplicate-version rejection are explicit
tests. Independent adversarial re-review of the new exact revision remains a
required gate before implementation acceptance.

### Review 21: observer terminal-deadline closure

The exact Review-20 cross-stack pass found that polling only through the legal
60-second first-claim boundary also required terminal success at that same
instant. A controller that first claimed legally near the boundary had no time
to run even the no-op handler, so the observer could record a false reset. The
corrected contract separates the immutable claim bound from a 15-second
execution grace: claim evidence must be present by `admitted_at + 60 seconds`,
terminal success by `admitted_at + 75 seconds`, and both remain bounded by the
existing 50-second per-Job work budget and 55-second active deadline. The
worst-case predecessor deadline remains five seconds before the next scheduled
Job, preserving `concurrencyPolicy: Forbid` without overlap.

### Review 22: bounded capacity and post-009 rollback closure

Exact-hash re-review of Review 21 accepted the observer timing and protocol but
rejected four capacity and rollback ambiguities. Counting only ASG Pending and
InService members could hide stopped, stopping, shutting-down, or unknown
instances from the approved physical/cost ceiling. The guard now enumerates
exact ASG and managed-node-group membership, counts every nonterminated state,
fails closed on unknown or mismatched membership, and independently proves
each min/desired/max plus the nine-normal/ten-transient bounds.

The same pass found that `H_mem`/`H_cpu` already included live, dormant, surge,
and dynamic work while the reservation text could add a second full `H` beside
the live cohort. Reservations now represent only the scenario- and zone-exact
inactive delta `H-H_live`, with tests rejecting duplicate shapes. Dynamic
SkyServe load balancers now have an exact audited bound: eight warm services
produce 16 steady pods and use Recreate with no same-slot surge; retained-single
services alone contribute one legal RollingUpdate surge each. The accepted 8/0
inventory therefore has a 16-pod bound. A typed chart-to-generated-Pod
PriorityClass contract, exact
identity/template/resource admission, and scoped ResourceQuota provide an
atomic concurrent hard cap; any
persisted-mode drift blocks rollout and reopens the capacity review.

Finally, merely naming the compatibility release after migration 009 did not
prove that its one-pod rollback shape had ever run. Phase 7b applies the exact
008/009-aware image and legacy-placement values in one-pod/all-role form on
schema 008. C-sky later archives the distinct exact final-target values after
relocation, and phase 10a uses that target-placement artifact for role split.
After 009 commits, the rollout deliberately exercises that exact target one-pod
revision against retained 009 before fixing forward. The phase-7b legacy-
placement artifact cannot be selected after the legacy taint, and the earlier
phase-6 exact-008 binary is explicitly forbidden once phase 7b is accepted.

### Review 23: pre-capacity LB-profile ordering

Cross-stack phase-order audit found that the first accepted quota text tried to
enforce the generated-LB PriorityClass before deploying an image capable of
rendering it. The old phase-6 image cannot be assumed to understand a future
chart value, and waiting until post-capacity role split would make the
`L_lb_slots`/H proof circular. The compatibility one-pod acceptance therefore moves
to zero-node phase 7b on schema 008. That exact artifact installs and exercises
the PriorityClass/admission/quota contract and rolls all eight warm-standby
services on legacy capacity before target planning. Its render remains the
pre-target rollback revision; after C-sky, the separately archived exact target-
placement values supersede it for role split and post-009 rollback. Admission is
likewise staged rather than assuming
future capacity: phase 7b binds the typed legacy-placement profile, phase 8
uses a bounded old/new digest handoff to a target-tolerating but still required-
legacy-pinned profile, and C-hub later atomically replaces that constraint with
each owner's target selector. Each predecessor digest is retired only after the
successor is Ready. A pre-acceptance phase-7b abort
uses the inverse exact-template handoff to return every converted service to
the untyped legacy profile before removing admission/quota/class resources and
restoring phase 6; after acceptance, that old image is no longer selectable.

### Review 24: placement and scale-to-zero observer safety

Adversarial cross-stack review found two hidden activation paths. A target
toleration without required legacy affinity could let a restarted workload land
on target nodes before its owner-specific C-hub gate. The intermediate profile
now retains an immutable required legacy-node-group constraint until C-hub
replaces it atomically with the target selector. Separately, an unconditional
minute-level inference canary would defeat SkyServe scale-to-zero and could
activate accelerator or ordinary on-demand capacity outside the approved CPU-
node scope. Every endpoint now receives only an LB-local non-dispatch control-
path probe. The observer never submits inference; it may validate independently
generated organic data-plane evidence at a declared native cadence, but absence
of organic traffic is not failure and zero-backend services must remain at
zero.

### Review 25: replacement, identity, maintenance, and preemption closure

Final exact-contract review closed four additional gaps. Phase 7b admission now
permits only each captured untyped predecessor and exact typed successor, so a
ReplicaSet can replace a crashed active predecessor before its owner converts;
the old digest retires only after both typed slots are Ready. The observer
principal language now matches the implementation's exact
`^sa-[0-9a-f]{16}$` byte contract. The post-009 2/2/2-to-one-pod rollback drill
is explicitly a declared maintenance event that may gap the API. Finally, the
LB PriorityClass is fully fixed at value 0/non-global/`PreemptLowerPriority`
between -1000 reservations and +1000 control-plane pods, and every other real
reservation replacement must prove effective priority greater than -1000 with
preemption enabled before relying on scheduler handoff.

### Review 26: causal evidence, complete cost-stop, and Helm ownership

Exact-hash review of the Review-25 revision rejected two remaining safety
gaps. First, the pre-HA cost stop could not recover from post-D or partial-10a
state. Its three state-bound variants now cancel an unaccepted rollout,
restore the exact target one-pod artifact when necessary, remove only the
captured legacy taint, prove legacy schedulable, reverse every C-sky/C-hub
two-digest handoff owner by owner, and permit target-zero only after target
digests retire. Second, a database-clock terminal timestamp had an upper bound
but no causal lower bound or observation bound. Schema 009 and both private
responses now require the claimed or terminal-before-claim ordering through a
per-transaction `observed_at`, with inverted and future evidence failing
closed.

The same correction applies the Boltz SkyPilot deployment boundary: the
application release and database config seed are operated directly with
reviewed Helm artifacts, while `boltz-platform` owns independent static
infrastructure only. Four permanent `destroy = false` state tombstones perform
the one-time, mutation-free release/seed/restart ownership handoff. All rollout,
rollback, abort, activation, stack mapping, and
test language above now preserves that split. Independent exact-byte review of
this updated revision was treated as a merge gate.

### Review 27: complete handoff and executable seed-hook lifecycle

Exact-byte review of the first Review-26 revision rejected three contradictions.
The platform-stack map now requires all four root `forget` actions rather than
only the release, the SkyPilot control-plane and Kubernetes-infrastructure roots
no longer configure a Helm provider, and direct Helm uses its own short-lived
operator authentication. The cluster root may retain its unrelated platform-
release Helm provider with exec authentication.

The same review found that one seed hook could not both commit before target
Deployments were applied and verify their later rollout. It also found that a
revision-named hook ConfigMap would not be release-managed, had no TTL, and
could be deleted before its consumer Job. The corrected chart contract embeds
the size-bounded canonical config directly in a weighted pre-upgrade seed Job,
rolls regular generation-annotated Deployments under `--wait`, and uses a
separate post-upgrade read-only verifier. Only Jobs remain as seed hook residue;
their success/failure lifecycles and TTLs are bounded and tested. Independent
exact-byte review of this correction was then rerun.

A second exact pass required the verifier TTL on successful Jobs as well as
failures: `hook-succeeded` is eager client-side cleanup, not protection against
an interrupted Helm client. The same bounded TTL now covers every outcome.
Independent exact review accepted commit
`e7d484f85571e89887ad903d8d59df9b9681e437` with no remaining blocker.

### Review 28: executable module-root handoff and bounded legacy capacity

Cross-repository review of the platform implementation found that Terragrunt
downloads the SkyPilot control-plane module as the root. The four tombstones
therefore move into the same SkyPilot module revision that deletes the resource
blocks; generating them platform-side would conflict before the pin and lose
the permanent policy after a later pin. Because that immutable commit is shared,
all four production consumers now require saved plans, with exactly four
forgets only in Rainier and zero managed-resource actions elsewhere. Phase 0
also retires inert platform Helm/ECR/application inputs and repoints ownership
and application assertions to the direct-Helm artifact.

The review also found that generation-only seed evidence could miss loss of
security-load-bearing config. H0 now proves raw-byte and canonical whole-row
no-op parity plus explicit GCP VPC, AWS ingress, global Kubernetes-context, and
workspace-boundary values before and after the seed transaction and again in
the post-verifier. The transition also requires a generation-bound no-plugin
attestation and strict built-in-schema validation. The storage phase names its
new Kubernetes-infrastructure
state, backend, authentication, dependencies, and apply order. Finally, plan A
removes legacy `create_before_destroy` and pins the captured integer launch-
template version as plan no-ops; otherwise an incidental three-node replacement
could create twelve physical instances and exceed the transient-ten approval.
Independent exact cross-repository review accepted the corrected contract with
no remaining blocker; implementation remains gated on preserving these bytes.

### Review 29: authorized direct-Helm operator boundary

Follow-up review found that Review 28 accidentally described every direct-Helm
application stage as human-only. That is stricter than the organization
operating contract: an explicitly authorized operator, including an agent
acting under a user's deployment instruction, may execute the reviewed Helm
artifact and its monitoring gates. The human-only boundary remains on the
separately reviewed production Terraform/OpenTofu state handoff and other
infrastructure applies. The two ownership domains remain disjoint, and an
ordinary SkyPilot rollout still neither waits for nor modifies
`boltz-platform`. Exact review accepted this operator-boundary correction with
no remaining blocker.
