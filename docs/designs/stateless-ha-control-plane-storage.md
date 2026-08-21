# Stateless HA control-plane storage

Status: Fresh-install design accepted. The PostgreSQL configuration-authority
prerequisite is merged, and F1--F3 are source-implemented and locally
qualified. Nothing in this change is deployed or live-qualified yet. The
remaining path is normal teardown of every test service, one PVC-free Helm
cutover, clean service recreation, and the production-style failure proof. PRs
#1642 and #1643 are superseded by this design and must not merge.

Last updated: 2026-08-21

Canonical owner: this file owns removal of the SkyPilot control-plane PVC and
shared-filesystem dependency. The 2/2/2 role split and PostgreSQL request and
controller fencing remain owned by
`docs/designs/multi-replica-api-server.md`. Reserved-capacity placement remains
owned by `docs/designs/serve-multi-pool-reserved-capacity-fill.md`.

This document records the operator decision that this SkyPilot installation is
test-only and its active services may be normally deleted and recreated. It
does not require or authorize dropping the shared PostgreSQL database, and it
does not authorize deleting a Helm release, PVC, EFS access point, or
filesystem without an exact live inventory and an explicit execution step.

## Decision

The current installation will not migrate EFS contents. It will start the
supported fleet from a fresh service lifecycle in the existing PostgreSQL
authority:

- PostgreSQL remains the only durable control-plane authority.
- API, executor, and controller pods use separate, bounded, disk-backed
  `emptyDir` volumes for local materializations, generated files, caches, and
  logs.
- Guarded HA rejects API-uploaded local workdirs and local file mounts. The
  supported `boltz-l4-fleet` service is self-contained: its immutable model
  image and weights already come from external object storage and its canonical
  service YAML contains no local workdir or file mount.
- Request, queue, service, version, controller configuration, cluster, SSH-key,
  placement, claim, debit, and recovery state remains in PostgreSQL.
- Raw local log files are operational diagnostics, not recovery authority.
  Containers continue to emit stdout/stderr for the cluster log collector;
  request activity and dashboard counts come from PostgreSQL. The guarded
  profile rejects raw API request-log streaming explicitly because the API,
  executor, and controller roles do not share a file. It must never touch or
  stream the API pod's same-named but unrelated local path.
- Guarded HA renders no PVC, EFS mount, shared path, FUSE mount, or filesystem
  fallback.

There is no storage protocol selector, `LEGACY_EFS` state, importer, dual write,
retained-row normalization, migration-intent catalog, or post-cutover cleanup
stack. Every active service is normally deleted before cutover, and
`boltz-l4-fleet` is recreated only after its old service row is absent. Fenced
terminal request history may remain in PostgreSQL, but no retained Serve
incarnation participates in bootstrap or recovery. Ordinary schema
bootstrap/verification still runs at the new image's exact head; this design
skips an EFS *data* migration, not required PostgreSQL schema management.

This decision deliberately does **not** add a generic S3 BlobStorage or
LogProvider now. The current fork has neither implementation, and the connected
account has no qualified server-owned control-plane bucket. Adding S3 would
therefore require a new provider, schema, IAM/KMS/bucket boundary, runtime
wiring, and failure protocol. None of that is needed to run or recover the
current self-contained fleet. If a future supported workload requires durable
API uploads or durable raw-log download, that is a new feature with its own
design; it must replace the fail-closed admission rule rather than create a
hidden second path.

## Why the design changed

The former design assumed production state had to survive an EFS-to-S3
migration. That assumption produced:

- 21 new storage tables;
- an EFS importer and migration-intent state machine;
- `LEGACY_EFS`/`S3_V1` mixed-version routing;
- restore-incarnation fencing;
- multipart-upload leases and cleanup receipts;
- 8--9 application PRs plus new S3/KMS/IAM infrastructure; and
- a draft cleanup PR whose migration history would permanently retain most of
  the transitional schema.

The operator has now confirmed that the installation is test-only and its state
may be discarded. Preserving those transition mechanisms would add permanent
code for a migration that will never run. The steady-state design is therefore
the migration plan: recreate once on the final topology.

## Goals

- Remove EFS and every PVC/shared-path dependency from guarded HA.
- Preserve the PostgreSQL restart and lost-acknowledgement guarantees used by
  `boltz-l4-fleet` placement and actuation.
- Recover the service after controller-pod loss using PostgreSQL only.
- Keep two API, two executor, and two controller pods.
- Fail before accepting a feature that still needs cross-pod local bytes.
- Bound every pod-local byte and avoid node-root DiskPressure.
- Make the cutover one-way, small, and fix-forward.
- Avoid KubeRay, Terraform/Terragrunt expansion, a boltz-platform runtime pin,
  and an additional control-plane storage service.

## Non-goals

- Preserving active SkyPilot services, jobs, clusters, uploads, raw logs, or
  caches across this test cutover. Normal teardown may leave fenced terminal
  and historical PostgreSQL rows; retaining or deleting those rows is not a
  storage-correctness requirement.
- Importing, inspecting, repacking, or preserving the current EFS tree.
- Supporting local workdir or local file-mount upload in guarded HA.
- Making raw request/controller log files durable across total pod loss.
- Providing a generic S3 blob/log backend in this change.
- Supporting database point-in-time restore across an in-flight provider call.
  Active test workloads are normally quiesced and recreated instead.
- Deleting a base EFS filesystem, mount target, CSI driver, or any resource not
  proven to be owned exclusively by this SkyPilot release.
- Changing reserved-fill, Kueue, worker scratch, model R2 storage, or paid Spot
  placement.

## Supported public contract

The fresh guarded-HA profile supports:

- ordinary SkyPilot operations whose complete durable input is represented in
  PostgreSQL and provider state;
- SkyServe services and pools whose task YAML uses immutable images, inline
  setup/run commands, remote object URIs, or server/workspace-owned volumes;
- generated SkyPilot SSH keys, because their key material is already stored in
  PostgreSQL and can be materialized into a pod-local file;
- service/controller restart, leadership transfer, replica recovery, and
  reserved-fill reconciliation from PostgreSQL; and
- fresh request telemetry and dashboard status from PostgreSQL even when a
  provider or controller is stalled.

The profile rejects, before reading or staging bytes:

- `/upload` and `/upload_v2` publication;
- any request carrying `file_mounts_blob_id`;
- a local `workdir` or local `file_mounts` source;
- a process-local modified service catalog that is not part of the immutable
  server image;
- mutable SSH-node-pool uploads or edits that would publish server-local key or
  config bytes; and
- any compatibility fallback that requires a retained local controller file.

Remote object URIs and declared workspace/server-owned volumes remain valid;
they do not make the control-plane filesystem authoritative. The error must
identify the unsupported local input and tell the caller to use an immutable
image or approved remote object URI.

Raw-log behavior is explicit. PostgreSQL lifecycle/status/telemetry remains
available, but the guarded profile does not expose raw API request logs through
the local `LogProvider`: the execution role writes a different pod-local path
from the API role that serves the log route. The API returns a typed
unavailable/not-supported response without creating or opening a local file.
The same rule covers Serve/Jobs log-return routes and cluster/Serve/Jobs
sync-down/download routes whose producer and consumer can land on different
roles. Kubernetes/Datadog collection remains the operational log history for
this test deployment.

## Invariants

1. No guarded-HA pod references a PersistentVolumeClaim.
2. No path, inode, mtime, file lock, directory listing, or local PID is durable
   or cross-pod authority.
3. Every provider-mutating request is committed and fenced in PostgreSQL before
   its handler is allowed to act. This existing request/claim contract is not
   weakened by the storage change.
4. A service controller may use local files only after reconstructing their
   exact bytes from the elected PostgreSQL service/version row. Restarting on a
   different pod must not require a predecessor directory.
5. Central configuration is read from PostgreSQL. A pod-local projection is a
   process input, not an authority or synchronization primitive.
6. Generated SkyPilot SSH files are derived from PostgreSQL. An external key is
   accepted only through a read-only projected Secret.
7. Upload rejection occurs before an HTTP body is consumed or a staging path is
   created.
8. Guarded HA uses only chart-owned, disk-backed `emptyDir` volumes with fixed
   reviewed byte `sizeLimit` values and fixed matching role ephemeral-storage
   requests/limits. Operator extras may be read-only Secret, ConfigMap,
   projected, or downward-API sources; operator-supplied writable volumes,
   including every custom `emptyDir`, are rejected. Kubernetes has no hard
   inode quota for `emptyDir`; inode exhaustion is an externally observed live
   threshold, not a chart knob that implies a fictitious volume limit.
9. ENOSPC or quota exhaustion fails the local operation and leaves PostgreSQL
   authority unchanged. It cannot produce a partially committed service or
   launch.
10. After the fresh cutover, rollback never remounts EFS. Recovery is another
    fresh recreation or a fix-forward image.

## Current source audit

Audit base: source at `2f9562eb5`, the exact `improvements` merge of PR #1647
atop release 1.1.1415 and PR #1646. This includes PR #1645's pre-import identity
fix and PR #1646's API-only correction: controller and executor pods receive
`IS_SKYPILOT_SERVER=true`, while the API pod reserves that operator override
but does not receive the marker because `sky api start` owns API
initialization. PR #1647 also makes the workspace-generation lock reentrant so
a cold request child cannot self-deadlock during PostgreSQL permission
initialization, and makes the Terraform config seed commit the required
`revision` and SHA-256 `digest` fields with its YAML.

| Surface | Already ready | Remaining blocker |
| --- | --- | --- |
| API requests and queue | PostgreSQL request rows, queue, leases, execution generations, cancellation, retention pins, and provider-action fencing are live. The guarded profile no longer stats the obsolete SQLite cutover gate and still serves terminal `/api/get` results from PostgreSQL. | Real-PostgreSQL and live restart qualification. |
| Server config and workspace policy | PR #1640 makes guarded central config and Casbin reconciliation PostgreSQL-authoritative and removes guarded config ConfigMap authority. PR #1647 fixes cold-child reentrant permission initialization and seeds YAML, revision, and digest as one PostgreSQL row update. | Verify the final PVC-free pod restart and exact seeded receipt; do not reintroduce a shared config lock. |
| Serve | YAML, submitted YAML, service/version rows, controller config bytes/digests, placement, snapshots, claims, and recovery data are in PostgreSQL. Guarded recovery now requires the elected PostgreSQL YAML and complete config snapshot and refuses predecessor-local fallbacks. | Recreate the service cleanly, then prove controller replacement from PostgreSQL in the live installation. |
| Managed Jobs | Structured job state, environment, YAML/config snapshots, and generated SSH material are in PostgreSQL. Guarded admission rejects local bytes after policy mutation and before volume preparation. | Optional live remote-object/image-only smoke test; Managed Jobs are not required for the fleet cutover. |
| Uploads | `BlobStorage` remains an interface for other profiles. Both legacy and v2 upload entrypoints and old-client blob IDs fail before body reads or request admission in guarded HA; its cleanup daemons do not start. | Live negative request proof. |
| Logs | Lifecycle and request status are PostgreSQL-backed; containers emit stdout/stderr. Guarded raw-log/sync-down/download routes now fail before local path resolution while terminal result/status remains usable. | Verify cluster log collection and the typed live response. |
| SSH | Generated key material is in PostgreSQL and `auth_utils` recreates missing managed-key files. Guarded mutable SSH-node-pool publication fails before body reads. | Verify generated-key rematerialization after a pod replacement. |
| Helm | The guarded 2/2/2 render now requires `storage.enabled=false`, emits no PVC mount, gives every role separate chart-owned bounded disk emptyDirs, locks the matching role ephemeral-storage requests/limits, rejects operator writable volumes, and projects the complete controller hold to all roles. | Server-side diff and live canary qualification of the fixed byte budgets and observed inode threshold. |

The current `boltz-l4-fleet.serve.yaml` is compatible with this profile: it has
no `workdir` or `file_mounts`; its model image and weights are fetched from
external immutable storage by the worker runtime. That fact must be rechecked
against the exact submitted YAML before recreation.

## Disposition of PRs #1642 and #1643

PR #1642 (`332778fefb88ee3220fa01fd3de985ac796abb87`) is conflicted with current
`improvements` and adds 3,613 lines. Its runtime is intentionally inert: it
creates 21 tables and implements only portions of legacy initialization,
migration intent, upload allocation, and restore fencing. It has no S3 client,
no BlobStorage integration, no LogProvider integration, no owner admission, and
no PVC-free Helm path. Merging it would not remove one EFS mount or make the
fleet restart-safe.

Draft PR #1643 (`9625ee5191af250bf2f3307d2a1d6028c8b94174`) removes some transition
APIs but deliberately retains the 21-table migration history and still has no
runtime provider. It is not a usable cleanup for a fresh-only design.

Both PRs should be closed as superseded. None of their production files are
required. Useful adversarial cases may be rewritten for the smaller boundary,
but their schema and repository must not be cherry-picked.

## Final topology

```text
client
  -> 2 API pods (HTTP admission only)
       -> PostgreSQL request + queue transaction
            -> 2 executor pods (ordinary handlers)
            -> 1 elected of 2 controller pods (controller handlers)
                 -> pod-local Serve child controllers

PostgreSQL: every durable structured input, owner, fence, and lifecycle row
emptyDir:   bounded disposable projections, generated files, caches, raw logs
S3/R2:      workload-owned immutable images/data only; not control-plane state
EFS/PVC:    absent from every SkyPilot pod
```

The controller advisory-lock generation is the cross-pod exclusion mechanism.
The active controller executes Serve up/update and spawns its child in the same
pod-local filesystem. A successor controller obtains leadership only after the
old PostgreSQL session is fenced, reads the elected service/version/config from
PostgreSQL, recreates local projections, and spawns a new child. It never reads
the predecessor pod's directory.

## Required code changes

### F1: Guarded ephemeral-artifact admission

Implementation status: source-complete and covered by focused unit tests; not
deployed.

Minimum source files:

- `sky/server/file_mount_uploads.py`: reject both upload protocols before body
  consumption in guarded HA.
- `sky/server/requests/executor.py` or the common request-admission boundary:
  reject a non-null `file_mounts_blob_id` independent of client version.
- raw request logs and local artifact return paths: guard generic request-log
  streaming, Serve `/logs`, Jobs `/logs`, cluster/Serve/Jobs sync-down, and
  `/download`. Return a typed unavailable/not-supported result without
  deriving, creating, touching, or opening a pod-local path. PostgreSQL
  request completion and result retrieval remain usable.
- `sky/ssh_node_pools/`: reject mutable SSH-node-pool key/config uploads
  before reading the request body or writing a local file.
- `sky/jobs/server/core.py` and `sky/serve/server/impl.py`: validate the final
  policy-mutated DAG contains no local workdir/file mount or unpackaged
  modified catalog before any storage or provider action.
- `sky/core.py`, `sky/execution.py`, and the API `/validate` path: apply the
  same common final-DAG check immediately after server-side policy mutation and
  before volume resolution, pre-mount, storage construction, or provider work.
- focused tests in `tests/unit_tests/` for zero-byte rejection, old-client
  payloads, remote URI acceptance, no staging directory creation, raw-log and
  sync/download unavailability without a local touch, status-only PostgreSQL
  completion, and SSH-node-pool body rejection.

Prefer one predicate owned by the server runtime, not duplicated environment
interpretations in each product.

### F2: PostgreSQL-only fresh Serve recovery

Implementation status: source-complete for the fresh-only contract and covered
by recovery tests that make predecessor file reads fail; not deployed. No
importer, retained-row adapter, or migration was added.

The current source already reconstructs a fresh service from one fenced
PostgreSQL snapshot: `serve_utils.py` materializes the committed bytes and
`service.py` selects the committed recovery version/config before spawning the
child. This phase is therefore a deletion/tightening proof, not a new recovery
subsystem. Minimum source files:

- `sky/serve/service.py`: require fresh recovery YAML and controller config from
  PostgreSQL and delete only the fresh-inert fallback that reads a predecessor
  task/config file.
- `sky/serve/serve_utils.py`: retain pod-local materialization helpers for
  committed bytes; prove no shared-path promote/restore semantics are
  authoritative for a newly created service.
- `sky/serve/serve_state.py`: retain the canonical version fields and the
  PostgreSQL recovery script; prove fresh recovery does not consult a legacy
  filesystem path.
- `sky/skypilot_config.py`: keep central reads PostgreSQL-authoritative and make
  scoped child projections explicitly pod-local.

No new table is required for the fleet. No obsolete EFS-transition table is
introduced, so no corresponding cleanup revision is needed. Historical
migration files remain immutable.

### F3: PVC-free guarded Helm render

Implementation status: source-complete. Helm unit tests and guarded
positive/negative renders pass locally; no server-side production diff or live
canary has run.

Minimum chart files:

- `charts/skypilot/templates/api-deployment.yaml`
- `charts/skypilot/templates/executor-deployment.yaml`
- `charts/skypilot/templates/controller-deployment.yaml`
- `charts/skypilot/templates/pvc.yaml`
- `charts/skypilot/values.yaml`
- `charts/skypilot/values.schema.json`
- the three role Helm-unit suites with exact rendered-volume inventory checks

Guarded HA must require the PVC-free profile rather than merely allow it. Each
role receives its own chart-owned disk-backed bounded volumes. Chart-owned
credential/cache volumes (including AWS CLI, gcloud, and other chart-owned
credential volumes) have fixed byte `sizeLimit` values whose aggregate fits
the fixed role ephemeral-storage request/limit. The API role receives that
request/limit only in guarded HA, so ordinary non-HA installs keep their
existing scheduling contract. Deployment values cannot enlarge those budgets.
Operator extras are restricted to read-only Kubernetes projections; custom
writable volumes are unsupported. Log rotation and cache cleanup must keep
byte and inode telemetry below their thresholds. The initial fixed values are
conservative source-reviewed limits, not estimates derived from the historical
25.4 GB EFS total; changing them requires another reviewed chart change plus a
fresh bounded canary.

The chart also projects the existing full `serve.controllerHold` value into
API, controller, and executor roles. This repairs its current cross-role
inconsistency; it does not change the hold into a teardown-drain mode, and the
cutover enables it only after normal teardown is quiescent.

Negative render assertions inspect every role Pod for shared mounts and reject:

- `persistentVolumeClaim`;
- operator-provided NFS, CSI, hostPath, generic ephemeral PVC, `emptyDir`, or
  any other writable/shared source;
- `/root/.sky`, `/root/.ssh`, or `/root/sky_logs` backed by a PVC;
- `storage.existingClaim` in guarded HA; and
- any override of a chart-owned volume or role ephemeral-storage budget; and
- a missing ephemeral-storage request/limit on the long-lived role container.

Non-HA upstream compatibility may retain its existing optional PVC behavior;
it is not a valid guarded-HA topology.

### F4: Delete superseded transition code

Do not merge `sky/control_plane_storage/`, API request revision 016 from #1642,
its 21-table schema, admin command, or tests. Close #1642/#1643 after the
replacement feature PR is reviewable. Remove stale design and chart language
that says guarded HA requires RWX.

## Implementation plan and size

1. **D0 -- this design: complete.** The fresh-lifecycle contract and
   disposition of #1642/#1643 are accepted.
2. **D1 -- admission and recovery cleanup: source-complete.** F1/F2 implement
   the fresh-only boundary. Focused tests pass; real-PostgreSQL qualification
   remains a deployment gate.
3. **D2 -- PVC-free chart: source-complete.** F3 positive and negative Helm
   renders pass locally. Server-side diff and live sizing remain deployment
   gates.
4. **D3 -- fresh cutover: pending.** Publish one immutable image, stop/delete
   active test services normally, upgrade the existing SkyPilot release without
   the claim while retaining its PostgreSQL authority, and recreate
   `boltz-l4-fleet` from canonical YAML.
5. **D4 -- failure proof and deletion: pending.** Complete the verification
   horizon, then remove only the exact release-owned claim/access point after a
   separate ownership check.

The source-complete implementation touches 23 production/chart files and seven
test/fixture files: about 735 added production/chart lines and 1,110 added test
lines before documentation. It adds no central storage tables, importer,
provider, daemon, or infrastructure module. F1 is the broadest source boundary
because every byte-accepting and raw-log surface must fail before touching a
path. F2 remains a small tightening of the existing PostgreSQL recovery path,
not a replacement recovery subsystem.

This is materially smaller than #1642/#1643, but it is not a one-line Helm
toggle. Rendering emptyDir while silently accepting local uploads or a legacy
recovery fallback would convert EFS failures into nondeterministic pod-local
loss.

## Fresh cutover protocol

The exact live commands and resource names are recorded in the deployment
change, not hard-coded here.

1. Reconfirm that the installation is test-only and inventory all SkyPilot
   services/jobs/clusters, the database, Helm release, PVC/PV, EFS access point,
   and any non-SkyPilot consumers.
2. Take external API admission out of service with a reviewed, reversible
   ingress-only Helm update while leaving the internal API Service and 2/2/2
   roles running. Use the internal API through `kubectl exec` for teardown.
   The current `serve.controllerHold` is not a teardown-drain mode: it is
   projected only to API pods in the old chart and it rejects `serve down`.
   Do not pretend it can both quiesce controllers and permit normal teardown,
   and do not add a permanent storage-specific drain flag.
3. Complete the reserved-fill fail-closed controller gate and put the relevant
   Kueue ClusterQueues on Hold so no new reserved workload can be admitted
   during teardown. Re-read PostgreSQL and Kueue immediately before mutation.
4. Run normal teardown for every active test service, job, and cluster that
   could depend on EFS. For `boltz-l4-fleet`, prove the service row is absent
   and its replica clusters, Kubernetes Pods/Workloads, load balancer,
   nonterminal requests, claims, debits, pins, and intents are gone. Do not
   begin with manual row deletion. Repeat the active-inventory check until it
   converges to zero; any concurrent mutation aborts the window.
5. After teardown and provider/request quiescence, preserve the existing
   PostgreSQL database and its fenced historical rows; do not export or import
   product rows or EFS bytes. Keep ingress and Kueue held. The old chart cannot
   provide a cross-role full hold, so these zero-work boundaries are the
   pre-roll safety proof.
6. Render and server-side diff the exact Helm release with guarded HA,
   `storage.enabled=false`, bounded emptyDir, and the retained database/auth
   values, with `serve.controllerHold=true` staged for every role. The diff
   must contain no PVC reference.
7. Deploy the immutable image with direct Helm, `--reuse-values`, explicit
   stored-value removal, and blocking readiness. The target image's ordinary
   PostgreSQL schema bootstrap/verification runs as the blocking Helm hook in
   this upgrade. “No migration” means no EFS data migration/importer, not
   skipping required PostgreSQL schema management. Do **not** use `--atomic`
   or a native Helm rollback across this one-way cutover: failure remains held
   and is repaired with a new fix-forward image.
8. Require 2/2/2 Ready pods, the exact PostgreSQL schema head, zero EFS/PVC
   mounts, and consistent full-hold projection before admitting work. Release
   the controller hold in a separate reviewed Helm update while external
   ingress remains absent.
9. Recreate `boltz-l4-fleet` through the internal API from the canonical YAML
   and exact required Secrets. This is a clean bootstrap: no retained service
   row, version, replica, controller path, or EFS byte is an input. Record the
   new service incarnation and endpoint.
10. Activate the reserved-fill protocol only after its independent Kueue and
    proof gates pass. Prove READY convergence and no paid spill while ingress
    remains absent, then restore the reviewed ingress separately.
11. Delete the old claim/access point only after the acceptance horizon and an
    exact ownership proof. A base filesystem or CSI component shared by another
    workload remains untouched.

## Verification

### Source and chart

- Real-PostgreSQL tests cover both an empty database and an existing database
  with fenced terminal history at every central lineage's exact head.
- `/upload`, `/upload_v2`, `/upload_v2/blob`, and mutable SSH-node-pool routes
  fail before request-body/form iteration and create no file or database row.
- Every request type rejects a non-null `file_mounts_blob_id` under guarded HA.
- Generic request logs, Serve/Jobs logs, cluster/Serve/Jobs sync-down, and
  `/download` return typed unavailable/not-supported responses without a local
  mkdir/open/touch; ordinary status-only request completion still reads its
  terminal status/result from PostgreSQL.
- Policy-mutated Serve/Jobs DAGs reject local workdir/file mounts but accept
  the fleet's exact self-contained YAML. The exact server catalog is package
  native and no process-local modified catalog is staged.
- Fresh Serve up/update commits complete YAML and controller configuration to
  PostgreSQL before success.
- Recovery with an empty local filesystem reconstructs the elected version and
  controller config from PostgreSQL. It retains the fenced PostgreSQL recovery
  script but never reads predecessor-local YAML/config or an embedded legacy
  config fallback.
- A delayed old controller cannot commit after leadership transfer.
- Helm tests render 2/2/2 with every chart-owned emptyDir bounded by the fixed
  profile, matching role ephemeral-storage resources, operator extras limited
  to read-only projections, consistent full-hold projection, and zero PVC
  objects, claims, mounts, or EFS references.
- Deliberately removing a local materialization and restarting a role changes
  no durable state.

### Live test installation

- Two API, two executor, and two controller pods are Ready.
- `findmnt`, pod specs, and `/proc/mounts` show no SkyPilot PVC/EFS path.
- PostgreSQL is the only structured authority and all readiness checks pass
  with the PVC absent.
- A fresh `boltz-l4-fleet` reaches READY from canonical YAML.
- Deleting the elected controller pod causes the standby to acquire a higher
  generation, reconstruct the service, and resume reconciliation without a
  manual file copy.
- Deleting one API and one executor pod does not lose request identity or cause
  duplicate provider mutation.
- Free compatible reserved GPUs are committed before paid residual and no
  paid Spot launch occurs when reserved capacity covers demand.
- Dashboard processing, queued, in-flight, completed, and freshness fields
  remain available from PostgreSQL during a controller/provider stall.
- Pod ephemeral byte use and inode use remain below 70% of their respective
  hard byte limit and observed inode threshold during up,
  update, controller replacement, and one complete reconciliation interval.
- Raw-log loss after deleting its owner pod is explicit and does not affect
  status, telemetry, or recovery.
- The horizon covers immediate, +10 minutes, +30 minutes, and one complete
  stale/quiescence interval. Any failure resets the horizon.

## Rollback boundary

Before active services are removed and the PVC-free release rolls, abort the
cutover and continue running the existing test release; no partial storage
protocol exists.

After a service is recreated on the PVC-free release, retained-filesystem
rollback is intentionally unsupported. Recreate again or deploy a fix-forward
image. Do not remount EFS or reintroduce a legacy file fallback. PostgreSQL,
canonical service YAML, and external worker artifacts are the recovery source.

The old EFS resource may remain detached during the acceptance horizon, but it
is never mounted as a fallback. Its later deletion is independent and requires
an exact ownership receipt.

## Rejected alternatives

### Merge #1642 and clean it later

Rejected because its inert 21-table schema is not on the path to the supported
fresh test runtime. The cleanup cannot erase migration history, and no table
removes the PVC without provider and product integration that do not exist.

### Import or normalize EFS before recreation

Rejected because the operator explicitly does not require retained state. An
importer would be transition-only code with no steady-state purpose.

### Add a control-plane S3 bucket now

Rejected for this scope because the fleet does not use local uploaded bytes,
the source has no S3 blob/log backend, and no qualified bucket exists. A real
implementation is valuable only when durable opaque bytes become a supported
requirement; pretending an existing model bucket is control-plane storage would
mix identities and ownership.

### Put uploads and raw logs in PostgreSQL

Rejected because the current supported profile can reject uploads and use the
cluster log pipeline. Adding bulk bytea rows would enlarge backups and database
I/O for a feature the installation does not use.

### Render emptyDir but leave uploads enabled

Rejected because chunk requests can land on different API replicas and a
restart can lose the only copy. A warning is not a correctness boundary.

### Keep EFS for logs only

Rejected because one retained mount keeps the PVC, CSI, IAM, availability, and
support surface alive.

### Collapse to one all-role pod

Rejected because it would give up the PostgreSQL 2/2/2 failover contract and
does not test the controller path being qualified for reserved fill.

### KubeRay, FUSE, cache service, or Terraform expansion

Rejected because none is needed for PostgreSQL authority plus bounded local
materialization.

## Open gates

- Resolve any findings from final independent review of the exact committed
  implementation and this synchronized design revision.
- Confirm with the exact submitted fleet YAML that there is no local workdir,
  local file mount, or hidden client blob ID.
- Confirm the target controller uses no process-local modified catalog file.
- Run the remaining real-PostgreSQL tests for F1/F2.
- Qualify the encoded emptyDir byte limits and 70% inode/byte warning threshold
  with a fresh live canary.
- Complete a server-side production Helm diff.
- Obtain a synchronized pre-cutover inventory and explicit normal-teardown
  target list for active test services/jobs/clusters. The shared PostgreSQL
  database is not a destructive target.
- Prove the ingress-only maintenance update removes external API admission
  while the internal API remains available for normal teardown.
- Complete controller/API/executor failure proof and the full acceptance
  horizon.
- Prove exact PVC/access-point ownership before deletion.

Until the F1--F3 image/chart is deployed and the live cutover gates pass, the
current EFS claim remains a real runtime dependency. The operator's permission
to recreate state removes the need for migration code; it does not make an
undeployed emptyDir render safe by itself.
