# Production-Grade Multi-Replica API Server

Status: PostgreSQL request authority and the split API/executor/controller
topology are live and were revalidated on 2026-08-22 at Helm revision 590 /
release 1.1.1464 with two API, three executor, and two controller Pods. Guarded
HA has no PVC or CSI mount. The private durable HA-observer canary and M5
compatibility cleanup remain independently gated. The former executable
RWX/EFS migration plan has been removed and is available only in Git history.
The API rolls with one surge and zero unavailable replicas; controller and
executor roles roll in place by default so a packed control plane cannot stall
on an unschedulable temporary Pod.

Last updated: 2026-08-25

Canonical owner: this file owns the role split, PostgreSQL request delivery,
controller leadership, execution fencing, and availability contract. External
plans and pull request descriptions must link here rather than restating it.

Storage supersession: storage work is governed only by
`docs/designs/stateless-ha-control-plane-storage.md`: PostgreSQL structured
authority, fail-closed rejection of local uploaded bytes, bounded pod-local
`emptyDir`, and no control-plane PVC or EFS steady state. The test-only fresh
cutover has superseded the former EFS-to-S3 migration design.

## Summary

Production now runs the implemented role split: two stateless API replicas,
three active-active PostgreSQL request executors, and two active-standby
controller workers under PostgreSQL leadership and fencing. PostgreSQL is the
request, queue, lease, and controller-ownership authority. Bounded Pod-local
volumes hold only reconstructible materializations; there is no shared
filesystem dependency. That completed storage boundary is governed only by
`docs/designs/stateless-ha-control-plane-storage.md`.

This design separates three responsibilities:

1. Stateless HTTP API replicas accept, authenticate, validate, and durably
   enqueue requests.
2. Active-active executor workers claim durable requests from PostgreSQL and
   run request subprocesses.
3. Active-standby controller workers supervise singleton managed-jobs and
   SkyServe control loops under PostgreSQL-backed leadership and fencing.

The removed RWX rollout, sizing, cost, rollback, and implementation material is
available in Git history as a record of how the live role split was reached.
It cannot authorize a new EFS/RWX change.

The test deployment targets Kubernetes context `boltz-test`, which is an alias
of `boltz-platform-test-eks-cluster`. It uses a dedicated namespace and Helm
release named `skypilot-ha`; it must not modify the existing `test` namespace or
the shared `gitops-hub-rainier` SkyPilot release.

The historical preflight began from one all-role pod, SQLite request state, and
Recreate upgrades that caused 88--94 seconds without a Ready API pod. The
one-way PostgreSQL cutover and role split have since completed. Exact old image,
claim, snapshot, and migration instructions remain in Git history; they are not
runtime or rollback inputs. Production must never interpret native rollback to
a pre-PostgreSQL revision as a supported recovery mechanism.

## Why the Existing RollingUpdate Path Is Insufficient

The current code has useful foundations:

- Central state can use PostgreSQL.
- Helm can run a separate database migration Job.
- API pods can verify schema revision instead of running migrations.
- Cluster operations and consolidated controllers use PostgreSQL advisory
  locks in several critical paths.
- Serve controllers persist controller IP and port ownership.
- File-mount and log call sites already have provider seams; the independent
  storage design owns removal of their live transitional filesystem backend.

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
- Defining the supported local-artifact boundary. Guarded HA consumes the
  PostgreSQL plus bounded-emptyDir contract;
  `stateless-ha-control-plane-storage.md` alone defines its fresh recreation,
  rejected upload surface, and no-PVC steady state.

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
  request ID. Their PostgreSQL request state remains available; a raw local log
  tail lost with its pod is reported as an explicit gap/unavailable result.
- API readiness is false when PostgreSQL is unreachable or the request schema
  is incompatible. Helm rejects an invalid PVC-free local-artifact profile
  before deployment; local quota exhaustion fails the affected operation and
  leaves PostgreSQL authority unchanged.
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

- Every API, executor, and controller role consumes PostgreSQL durable state;
  role correctness never depends on pod affinity or a path shared between
  pods.
- Guarded HA rejects API-uploaded local workdirs, local file mounts, and every
  non-null file-mount blob ID before staging bytes. Remote object URIs and
  server/workspace-owned volumes remain outside the control-plane filesystem.
- Raw local log files are best-effort operational diagnostics. PostgreSQL
  lifecycle/status remains replica-independent and Kubernetes/Datadog owns the
  test installation's retained operational logs.
- Temporary projection/materialization is bounded and pod-local because it is
  regenerated from PostgreSQL or an external immutable workload source.
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
  TTL cleanup, and uninstall residue. In guarded HA, every migration, seed, and
  verifier Job in the fresh guarded profile validates the exact database head,
  PostgreSQL configuration generation, and PVC-free rendered release before it
  mutates durable state. There is no storage-authority row, legacy initializer,
  or object-provider bootstrap.
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
| Bounded emptyDir | One per pod | Disposable projections only | Disposable projections/logs | Disposable projections/logs | No durable authority |

Splitting executors from controller supervisors is intentional. Active-active
request throughput and active-standby controller ownership have different
failure and scaling semantics. Combining them makes a busy executor rollout
also churn controller leadership.

The compatibility entrypoint keeps `--role=all` while the fleet migrates. HA
mode uses explicit `api`, `executor`, and `controller` roles and fails Helm
rendering if PostgreSQL or the bounded PVC-free volume contract is absent. The
qualified image independently provides fail-closed local-upload admission;
Helm cannot infer application behavior from an image reference.

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

Production SkyPilot application runtime is owned exclusively by direct Helm.
The reviewed bundle pins immutable chart and image identities, captures
retained values and the rendered diff, and upgrades the existing release with
`--reuse-values`. `boltz-platform` owns only minimum static infrastructure
and identity/RBAC boundaries; it owns neither the SkyPilot Helm release nor an
application-version pin. The one-way retained-PostgreSQL, fresh-service-
lifecycle/PVC-free cutover and its pre-detach/post-detach recovery boundary are
defined only by
`docs/designs/stateless-ha-control-plane-storage.md`. This role-split design
adds no admission policy, second Helm release, or storage-specific rollout
controller.

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
- `storage.enabled=false`, no `storage.existingClaim`, and bounded disk-backed
  emptyDir plus matching ephemeral-storage requests/limits for every role.
  Guarded HA rejects any PVC, EFS, fallback, or unbounded local volume.
- Guarded HA keeps the API RollingUpdate contract at zero unavailable replicas
  (`maxUnavailable: 0` or `0%`) and an absolute `maxSurge: 1`; rendering fails
  before producing manifests when either API invariant is violated. The API is
  live request ingress, so it retains a Ready endpoint before an old one is
  removed.
- Controller and executor Deployments remain RollingUpdate and independently
  configurable, but default to absolute `maxSurge: 0` and
  `maxUnavailable: 1`. Each role has at least two replicas, PostgreSQL-backed
  fencing or durable work, and a role PodDisruptionBudget, so the default keeps
  one replica available while replacing the other without requiring a
  temporary third Pod. Operators may select `1/0` for either role only after
  rollout preflight proves room for that role's surge.
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
  mutex. The API Deployment's separate `maxUnavailable: 0` contract governs
  its rolling update; controller and executor Deployments default to the same
  one-unavailable floor as their PDBs.
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
Sticky ingress affinity defaults to false in HA mode. Follow-up operations use
PostgreSQL request IDs and replica-independent durable artifact references, so
routing to any API replica is correct.

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
on the then-current shared storage. Short and long requests, streaming, cancellation
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

Deployment evidence and storage supersession:

M1 completed the one-way SQLite-to-PostgreSQL request-store migration and its
real-PostgreSQL acceptance. Production PostgreSQL is authoritative and the
legacy importer must never be rerun. The role split briefly shipped on one
shared RWX claim; the fresh-lifecycle cutover has now removed it. The former
executable EFS/RWX copy, backup, Terraform, Helm, cost, rollback, and capacity
plan remains available in Git history only.
`docs/designs/stateless-ha-control-plane-storage.md` owns the completed
retained-PostgreSQL, bounded-emptyDir cutover. No text in this file authorizes a
new EFS/RWX change.

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

A 2026-09-03 100-launch qualification exposed the next boundary inside the
request store. Each executor supervisor used the same one-slot
`api-requests-control` pool for its two dispatchers, role heartbeat, and every
active execution-claim heartbeat. A checked-out request transaction therefore
caused 15-second pool timeouts in the supervisor, after which 30-second claims
expired and provider-effect fences correctly rejected the stale handlers.

Renewable authority now uses one separate, strict, process-local
`api-requests-liveness` pool. The role lease and execution-claim renewals are
the only writers on this lane. Claim renewal selects its exact current row with
`FOR UPDATE SKIP LOCKED`; a briefly locked but still-current claim remains
valid for the current heartbeat and is retried on the next cadence, while an
expired or replaced claim still returns the existing definitive revocation.
This prevents one request-row writer from creating head-of-line blocking among
unrelated claims without increasing the ordinary or request-control pool,
changing the 30-second lease, or weakening any execution fence. The gate is a
real-PostgreSQL test with 64 simultaneous claims, one deliberately locked row,
concurrent dispatcher progress, and a fresh role heartbeat, all within one
10-second heartbeat cycle. Production qualification remains open.

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

Historical isolated M4 acceptance sequence (not a production rollback plan):

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

ControllerManager process replacement uses the same contract at a finer
boundary. The runtime owns a fixed number of disposable slots. Before one dead
slot attempt can be reset, its guardian proves the complete local process
family absent, closes nested request admission for the exact
`(instance, generation, slot, attempt)`, and proves every admitted request
family quiescent. The reset sends every non-`INACTIVE`, non-`DONE` row owned by
that exact attempt back to `WAITING`; it does not classify terminal rows as
finished scheduler work merely because workload execution is finished.

The single `WAITING` claim path locks the job row and all durable task-status
rows in one transaction. It returns `cleanup_only=true` exactly when at least
one task exists and every locked task is terminal. Ordinary claims preserve
the existing priority order and execution path. A cleanup-only claim installs
the same per-job controller context, invokes the canonical cluster/pool,
ephemeral-storage, and local-file cleanup, and revokes the shared API token. It
does not construct `JobController`, invoke workload callbacks, or relaunch a
task. Final `DONE` is a compare-and-set requiring the same exact slot attempt,
`LAUNCHING`, admission still open, and all tasks still terminal.

Ordinary final `DONE` writes made inside a disposable slot also require
`controller_slot_quiescing=false`. Transient provider, token, or database
failures retain the cleanup-only claim and retry with capped exponential
backoff, without replaying an already-completed cleanup phase. Exact ownership
loss exits immediately so the guardian can drain and reset the row; a
replacement attempt then classifies the still-terminal task family as
cleanup-only again. `DONE` is never reset. This gives manager death one
canonical adoption path and keeps workload recovery and terminal resource
reconciliation disjoint.

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
health port, resources, credentials, bounded pod-local storage, and
PostgreSQL configuration. HA validation requires at least two. M3 intentionally leaves
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
for each role, their PDBs and API Service, PostgreSQL, the then-current durable
byte provider, and declared service account and Helm metadata. Both API pods
reach the shared health endpoint, revision 26 is deployed, and no M2/M3
workload or cloud load balancer residue remains.

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
5. The isolated release exercised its then-compatible image-B-to-image-A
   rollback and returned to image B under the same canary. Production now uses
   the fix-forward contract below and never uses this historical exercise to
   authorize native rollback.
6. Remove the conformance canary and superseded hook Jobs, while retaining the
   healthy isolated release and its declared PostgreSQL and bounded-emptyDir
   dependencies.

### M5: Compatibility cleanup gate

- Confirm all production-target Helm values use explicit roles, PostgreSQL,
  rejected local uploads, and the bounded PVC-free profile.
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
- Disposable-slot tests prove exact local-family and nested-request quiescence
  precede reset; terminal rows return through the ordinary `WAITING` claim as
  cleanup-only work; `DONE` never resets; manager death permits exact
  re-adoption without workload relaunch; and a quiescing or replaced attempt
  cannot publish final `DONE`. Controller tests prove cleanup-only adoption
  reuses pool cancel, non-pool down/status, ephemeral-storage deletion, local
  file cleanup, and token revocation while constructing no `JobController` and
  invoking no workload callback.
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
- Role-split tests start with empty role-local filesystems and prove that
  API/executor/controller failover reconstructs supported state from
  PostgreSQL. Guarded upload requests fail before body consumption and raw-log
  loss is explicit rather than misrouted.
- Migration tests cover empty bootstrap, additive upgrade, verify-only success,
  verify-only mismatch, and two concurrent migration attempts.
- Retained-PostgreSQL fresh service recreation, local-artifact admission,
  bounded emptyDir, and exact EFS detachment/deletion tests are owned
  exclusively by
  `docs/designs/stateless-ha-control-plane-storage.md`; this role-split suite
  neither recreates nor qualifies the removed RWX plan.
- Direct-Helm harness tests require immutable chart/image/operation digests,
  captured `values --all`, manifest and history, complete render and diff,
  default `--reuse-values`, and rejection of native rollback, `--atomic`,
  unreviewed `--reset-values`, or any infrastructure plan that mutates the
  SkyPilot application release. Fence-release tests separately reject native
  rollback, `--atomic`, historical revision reuse, an overlapping application
  mutation, a stale/lost lease token, and every PostgreSQL mode/generation
  mismatch. No SkyPilot platform pin is a deployment gate.
- Seed parity tests cover fresh and existing databases,
  merge/list/workspace/prune semantics, all-role and split-role reload,
  migration-before-seed and seed-before-rollout ordering, preservation of every
  security-sensitive required path, built-in-schema and no-plugin attestation,
  the 262,144-byte input bound, pre-seed failure, post-rollout verification,
  retry/failure TTLs, interrupted-client-after-success, uninstall residue, and
  revision-scoped cleanup.
- Placement/capacity tests prove the exact 2/2/2 role pods plus one bounded API
  surge schedule across failure domains with their declared requests/limits
  and priorities, do not overlap SkyServe worker/LB selectors, and retain
  required node and zone headroom. A controller or executor surge is included
  only when its explicit rollout override enables one. Provider-specific node
  migration, taint, cost-stop, and infrastructure rollback sequences are
  deployment runbook evidence, not unnamed plans in this canonical application
  design.
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
  Compatibility-target tests archive distinct schema-008 and schema-009
  split-role direct-Helm bundles, zero-change reapply the compatible 008 bundle
  before the one-way observer activation, and accept only a reviewed
  009-capable fix-forward after that activation.
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

Historical storage test counts are not acceptance evidence for the
current storage design. Role/PDB/Helm changes require the exact-head suites
above; storage changes require the independent suite and gates in
`docs/designs/stateless-ha-control-plane-storage.md`.

## Monitoring

No new telemetry pipeline is introduced. Existing Datadog collection receives:

- API availability and latency by role and pod.
- Queue depth, claim age, lease expiry, and interrupted count.
- Ready API, executor, and controller instance counts.
- Controller leader identity, generation, and lock-session health.
- Migration duration and result.
- Local byte/inode pressure from Kubernetes/container telemetry and typed local-
  upload rejection responses through the ordinary API request/error pipeline.
  This storage change introduces no second telemetry service.

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
fix-forward one-pod recovery proof, and the currently approved placement/
capacity migration have all been accepted with empty superseded nodes. A
wall-clock timer, PR age, or Helm uptime never unlocks cleanup; provider-
specific runbook step names are not part of this application contract.

This clock gates only the role-split M5 compatibility cleanup described by
this file. It does not gate the independently reviewed fresh no-EFS cutover or
exact SkyPilot PVC/access-point removal.

The separately reviewed observation deployment installs a
`rainier-ha-observer` CronJob inside the private cluster
with `* * * * *`, `concurrencyPolicy: Forbid`, a 30-second starting deadline,
and `activeDeadlineSeconds: 55`. Its narrowly scoped identity has read-only
Kubernetes, PostgreSQL, and Datadog access plus AWS permissions limited to
backup observation, deterministic evidence-key `Get`, and conditional append;
it has no bucket-list permission. A distinct read-only exporter identity owns
attempt-prefix List/Get. At most one 250m CPU, 256-MiB observer pod may run;
the live placement preflight must include that exact bounded workload without
depending on an unnamed reservation plan.
Every UTC minute it evaluates the immediately preceding closed UTC-minute slot
and writes one canonical sample to a
dedicated versioned, destroy-protected S3 evidence prefix using a unique key and
`If-None-Match: *`. Its Pod Identity can put new
sample/reset/checkpoint objects but cannot overwrite or delete them. The bucket
policy independently denies missing or non-`*` conditional headers for both
this prefix and the disjoint seed prefix; identity policy alone is not the
append-only boundary.
Each sample binds the attempt ID, exact rendered PVC-free storage profile and
accepted live-pod readiness evidence, exact direct-Helm release-bundle
digest, immutable environment/placement evidence, any applicable capacity-cost
approval receipt and hard end, predicate result and source query IDs, prior-
sample digest, and observer build digest. Failed predicates and missing UTC-
minute slots append explicit reset events. A Job reads and validates
only the deterministic preceding-minute
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
end, all resets and reasons, source query IDs, PVC-free-storage/admission-policy
and release identities, approval hard end, chain head and backing S3
object/version manifest, and the
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
- every role pod remains on the exact approved failure-domain and node-class
  inventory; no hosting node reports pressure; and the reviewed per-zone and
  aggregate headroom for the required API surge plus any explicitly configured
  controller or executor surge remains satisfied after accounting for
  DaemonSets, external load balancers, the observer, and all other live
  workloads;
- the exact rendered PVC-free storage profile is running in all seven Ready role
  pods, every local volume remains within its byte/inode budget, and local-
  upload rejection has no bypass. Any PVC/EFS mount or I/O fails the storage
  gate;
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

The storage design owns its own inventory/import rehearsal and production
cutover evidence. That evidence neither repairs nor backdates this role-split
observation clock.

## Rollback and fix-forward

- Migrations remain additive until the M5 cleanup gate. A failed migration or
  seed hook blocks the rollout and leaves the previous Deployments serving.
- Rolling back never deletes request, queue, execution, or controller-ownership
  rows.
- Production recovery is a reviewed direct-Helm fix-forward upgrade pinned to
  an immutable compatible image/chart and the complete retained values. Native
  `helm rollback` to a stored pre-PostgreSQL or incompatible storage revision is
  unsupported.
- After the fresh PVC-free cutover, guarded HA cannot return to `--role=all`.
  Recovery fixes the split API, executor, and controller topology forward with
  a PostgreSQL/emptyDir-capable image.
  The all-role entrypoint may remain only for explicit non-HA/local
  compatibility until its M5 removal gate.
- Disabling `apiService.metrics.enabled` removes all role metrics ports and
  scrape annotations together; it cannot be used to satisfy a metrics-dependent
  rollout gate.
- The one-way storage boundary, pre-commit abort, post-commit fix-forward rule,
  and exact infrastructure retention/deletion behavior are owned exclusively by
  `docs/designs/stateless-ha-control-plane-storage.md`. This file defines no
  alternate storage rollback.

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
- Chart branches that let guarded HA mount a PVC/RWO/RWX path, leave emptyDir
  unbounded, or accept local uploads

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
- The isolated role-split release retains only its declared PostgreSQL and
  bounded PVC-free local-storage dependencies. One-shot canaries, failed
  revisions, and unrelated cluster-scoped residue are absent.
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

At that stage, the same review made the capacity boundary explicit: Helm and
the post-seed reconciler may roll API, executor, and controller Deployments at
the same time, so the original `maxSurge: 1` setting required aggregate
headroom for as many as three surge Pods. Review 31 supersedes that requirement
for controller and executor after production showed that reserved surge
headroom can deadlock a packed control plane. The API's absolute single-surge,
zero-unavailable contract from this review remains unchanged.

### Reviews 14--28: historical rollout closure

These reviews drove the already-deployed PostgreSQL role split, controller
ownership, disruption, observer, capacity, and direct-Helm contracts above. They
also contained the now-superseded executable EFS/RWX migration plan. The
accepted non-storage corrections are incorporated into the normative sections
of this file; the review-by-review record remains in Git history. Storage
cutover, rollback, cleanup, and infrastructure authority now live only in
`docs/designs/stateless-ha-control-plane-storage.md`.

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

### Review 30: disposable managed-job terminal-cleanup adoption

Implementation review of fixed ControllerManager slots found that outer-
generation recovery alone did not define what happens when a disposable
manager dies after workload terminalization but before provider cleanup and
`DONE`. Preserving the terminal row under a dead attempt stranded cleanup;
resetting it through the ordinary workload controller risked relaunching a
finished task. A refresh-thread provider fallback would add a second cleanup
happy path with different context and fencing.

The accepted correction keeps one reset queue and one canonical cleanup
implementation. Exact local and nested-request quiescence precede reset, the
atomic claim classifies the locked task family, and a terminal family enters a
cleanup-only controller coroutine. The coroutine reuses the normal cleanup
effects but cannot instantiate workload execution. Exact-attempt,
non-quiescing, all-terminal `DONE` closes the final stale-write window; claim
loss exits for guardian-managed re-adoption. Focused adversarial tests cover
pool and non-pool effects, ephemeral storage, callback/relaunch exclusion,
phase-aware retry, exact finalization, and death between claim and completion.
No compatibility branch or refresh-owned provider fallback remains in the
steady-state contract.

### Review 31: packed-control-plane role rollouts

Production rollout evidence showed that the original per-role `maxSurge: 1`
contract can deadlock an otherwise healthy upgrade when the control-plane nodes
fit the desired API, executor, and controller Pods but not three additional
surge Pods. Adding node capacity is not an application correctness requirement:
controller leadership is fenced in PostgreSQL and executor requests are
durable and reclaimable by the remaining replica.

The steady-state chart therefore keeps the ingress API's guarded `1/0`
strategy unchanged and defaults only controller and executor to `0/1`.
Per-role values remain configurable for installations with proven surge
headroom. Templates use key-preserving lookup rather than truthiness defaults,
so an explicit integer zero and a retained release that lacks the newly added
map both render deterministically. Focused Helm tests cover defaults, explicit
zero overrides, null/missing retained values, schema generation, and the
unchanged API strategy.
