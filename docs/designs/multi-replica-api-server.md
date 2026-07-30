# Production-Grade Multi-Replica API Server

Status: accepted and in implementation as Step 2 of the dstack maturity port

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
- RollingUpdate uses `maxUnavailable: 0`.
- API Deployments set `minReadySeconds`, a bounded progress deadline, pre-stop
  drain, and a termination grace period longer than the drain budget.
- API Service selectors match only API pods.
- API, executor, and controller Deployments have distinct labels and commands.
- Pod anti-affinity or topology-spread constraints avoid placing all replicas
  on one node when the cluster has capacity.
- PodDisruptionBudgets preserve one API, executor, and controller pod.
- Migration hooks finish before Deployments roll.
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
`minAvailable: 1`. These are part of M2's role split, not optional rollout
polish: an autoscaler eviction of the only remaining ready API during a
graceful replica deletion otherwise defeats the availability contract even
when durable request delivery is correct.

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
rechecks the same fields immediately before cleanup or terminalization. A row
owned by another generation is recovery work, never evidence that the current
leader's local controller crashed. This distinction is required because PIDs
are meaningful only inside their owning pod.

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

Implementation status: the local M4 candidate is complete. Chart schema
generation, chart lint, all 236 Helm unit tests, guarded live-value
server-side dry-run, shell syntax and ShellCheck for the conformance harness,
targeted role-runtime tests, formatting, mypy, Pylint, dashboard checks, and a
warning-as-error Sphinx build pass. The fast drain-marker unit test proves
readiness fails without opening PostgreSQL. The Docker-backed PostgreSQL lease
test could not start its disposable `postgres:16` container in the local
testcontainers environment; the live isolated deployment must therefore prove
the marker-driven heartbeat transition before M4 acceptance.

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
- Managed-job PostgreSQL tests prove a stale outer generation cannot claim a
  waiting job, generation advancement serializes with an in-flight claim, and
  recovery resets stale ownership before a replacement scheduler starts.
- Managed-job refresh tests prove a stale-generation PID is never interpreted
  as a current local crash and cannot cause cluster teardown or
  `FAILED_CONTROLLER`.
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
- Helm tests cover valid HA render and every invalid prerequisite.
- Helm snapshots prove Service selectors and PDB selectors do not overlap
  roles.
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

## Monitoring

No new telemetry pipeline is introduced. Existing Datadog collection receives:

- API availability and latency by role and pod.
- Queue depth, claim age, lease expiry, and interrupted count.
- Ready API, executor, and controller instance counts.
- Controller leader identity, generation, and lock-session health.
- Migration duration and result.
- Shared storage read/write sentinel failures.

Kubernetes events, pod logs, PostgreSQL rows, Helm status, and active traffic
canaries remain separate evidence sources. A green pod rollout alone does not
prove request continuity.

## Rollback

- Migrations are additive until the M5 cleanup gate.
- Every milestone can roll back its Deployment image without dropping new
  tables.
- HA mode can be disabled to return to one `--role=all` pod during the
  migration window.
- Rolling back does not delete request, queue, or controller ownership rows.
- A failed migration hook blocks the rollout and leaves the previous
  Deployments serving.
- The isolated test release remains installed and healthy after final
  conformance. Failed revisions, one-shot canaries, stale migration Jobs, and
  abandoned test workloads are removed. Its dedicated PostgreSQL and EFS
  resources remain only as declared dependencies of the running release.

The storage cutover is a deliberate rollback boundary:

1. Before cutover, the compatibility image blocks new submissions, drains or
   interrupts active legacy requests, imports local rows, verifies counts and
   hashes, and records a PostgreSQL cutover marker.
2. After the marker commits, all new requests use PostgreSQL.
3. A post-cutover rollback may return to the M1 compatibility image, but never
   to a pre-M1 image that can only read SQLite.
4. The legacy SQLite database is retained read-only through the rollback window
   for audit, not used as a second writer.

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
