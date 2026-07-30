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

- Helm runs the migration image as a blocking pre-install and pre-upgrade hook.
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
- `acquired_at`, `heartbeat_at`, and `released_at`.

The elected process advances this row only while holding the dedicated
PostgreSQL advisory lock. The persisted generation, not advisory-lock
ownership alone, is the fence passed to child controllers.

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

Deployment:

1. Deploy two API replicas and two executor replicas.
2. Run an in-cluster ClusterIP canary and continuous authenticated traffic
   while deleting each API pod.
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

Deployment:

1. Deploy two controller replicas.
2. Start a minimal Kubernetes managed job and SkyServe service.
3. Delete the active controller pod repeatedly.
4. Verify standby promotion, one live generation, no duplicate resources, and
   uninterrupted workload data plane.
5. Terminate the leader PostgreSQL session and repeat the proof.

### M4: Migration ordering, disruption safety, and stateless routing

- Make the migration Job a blocking Helm hook.
- Add PDBs, topology spread, and role-specific readiness.
- Disable sticky sessions in HA mode.
- Add an HA conformance script that drives traffic during pod deletion and
  rolling upgrades.
- Update administrator documentation and remove the experimental warning only
  for the guarded HA configuration.

Deployment:

1. Run an image A to image B rolling upgrade under traffic.
2. Run a schema expand upgrade under mixed API versions.
3. Prove zero ordinary-request 5xx responses, bounded stream reconnects, and no
   duplicate request generations.
4. Run Helm rollback and then upgrade again.

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
