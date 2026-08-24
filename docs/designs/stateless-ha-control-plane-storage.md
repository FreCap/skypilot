# Stateless HA control-plane storage

Status: Complete and live-qualified for the SkyPilot guarded-HA installation.
Helm revision 594 / release 1.1.1470 runs with `storage.enabled=false`,
PostgreSQL authority, bounded pod-local state, two API Pods, three executor
Pods, and two controller Pods. The `skypilot` namespace has no PVC, and none of
those role Pods has a PVC or CSI volume. `boltz-l4-fleet` was cleanly recreated
from its canonical service YAML and now stores its structured durable state in
PostgreSQL; that recreation was not an EFS import or a claim that the deleted
service incarnation was recovered. PRs #1642 and #1643 remain superseded and
must not merge.

Last updated: 2026-08-24

Canonical owner: this file owns removal of the SkyPilot control-plane PVC and
shared-filesystem dependency. The multi-replica role split and PostgreSQL request and
controller fencing remain owned by
`docs/designs/multi-replica-api-server.md`. Reserved-capacity placement remains
owned by `docs/designs/serve-multi-pool-reserved-capacity-fill.md`.

This document records the operator decision that this SkyPilot installation is
test-only and its active services may be normally deleted and recreated. It
does not require or authorize dropping the shared PostgreSQL database, and it
does not authorize deleting a Helm release, PVC, EFS access point, or
filesystem without an exact live inventory and an explicit execution step.

## Decision

The completed cutover did not migrate EFS contents. It started the supported
fleet from a fresh service lifecycle while retaining the existing PostgreSQL
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
stack. Active services were normally deleted before the cutover, and
`boltz-l4-fleet` was recreated only after its old service row was absent. Fenced
terminal request history may remain in PostgreSQL, but no retained Serve
incarnation participated in bootstrap or recovery. Ordinary schema
bootstrap/verification still runs at each new image's exact head; this design
skipped an EFS *data* migration, not required PostgreSQL schema management.

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
- Reconstruct service-controller state after controller-pod loss using
  PostgreSQL only.
- Keep the API, executor, and controller roles independently replicated; the
  live topology is two API, three executor, and two controller Pods.
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
- Importing, inspecting, repacking, or preserving the former EFS tree.
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
- structured request telemetry from PostgreSQL. The provider-local terminal
  census and fresh current-demand projection are qualified in the reserved-
  capacity design; they are not evidence for this storage closeout, and full
  producer coverage remains explicitly separate.

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

## Implementation and qualification audit

The source boundary was introduced across PRs #1640 and #1645--#1648. Release
1.1.1470 at Helm revision 594 is the current live receipt for the final storage
topology. The separately completed request/data-plane qualification is tracked
in the reserved-capacity design and is not silently promoted to storage
evidence.

| Surface | Implemented boundary | Qualification status |
| --- | --- | --- |
| API requests and queue | PostgreSQL owns request rows, queue, leases, execution generations, cancellation, retention pins, and provider-action fences. The guarded profile does not consult the obsolete SQLite cutover gate. | Deployed on the PVC-free release. The separate exact 10,000-request gate passed on revision 594. |
| Server config and workspace policy | Guarded central config and Casbin reconciliation are PostgreSQL-authoritative. Config seeding commits YAML, revision, and digest together. | Deployed across all seven Ready role Pods; no shared config lock or volume is present. |
| Serve | YAML, service/version rows, controller config bytes and digests, placement, snapshots, claims, and recovery state are in PostgreSQL. Guarded recovery rejects predecessor-local fallbacks. | The old service was removed and a new incarnation was created from canonical YAML. This closeout does not characterize that clean recreation as recovery of the deleted incarnation. |
| Managed Jobs | Structured job state, environment, YAML/config snapshots, and generated SSH material are in PostgreSQL. Guarded admission rejects local bytes after policy mutation and before volume preparation. | Source-qualified and deployed; a separate Managed Jobs smoke test was not a prerequisite for the fleet's storage cutover. |
| Uploads and raw logs | Guarded legacy/v2 upload, local blob-ID, raw-log, sync-down, and download paths fail before treating a pod-local path as durable authority. | Source tests are the evidence for these negative surfaces. No additional live route matrix is claimed here. |
| SSH | Generated key material is PostgreSQL-owned; mutable SSH-node-pool publication is rejected before local-byte admission. | Source-qualified and deployed. |
| Helm | Guarded HA requires `storage.enabled=false`, emits no PVC mount, gives every role separate bounded disk-backed `emptyDir`, and rejects operator writable volumes. | Focused Helm tests pass, and revision 594 has two API, three executor, and two controller Pods with no PVC or CSI volume. |

The submitted `boltz-l4-fleet` definition used for the clean recreation has no
local `workdir` or `file_mounts`; its model image and weights come from external
immutable storage. Future definitions must preserve that guarded-HA contract.

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
            -> 3 executor pods (ordinary handlers)
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

Implementation status: complete, covered by focused unit tests, and deployed in
the PVC-free release. This status does not claim a separate live negative test
for every rejected route.

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

Implementation status: complete for the fresh-only contract, covered by
recovery tests that make predecessor file reads fail, and deployed. The live
fleet receipt is a clean recreation from canonical YAML, not a claim that the
deleted predecessor incarnation was recovered. No importer, retained-row
adapter, or migration was added.

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

Implementation status: complete. Helm unit tests and guarded positive/negative
renders pass, and the live revision renders bounded local volumes with no PVC
or CSI mount on any of its seven role Pods.

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
2. **D1 -- admission and recovery cleanup: complete.** F1/F2 implement the
   fresh-only boundary and run in the PostgreSQL-only release.
3. **D2 -- PVC-free chart: complete.** F3 positive and negative Helm renders
   pass, and the live release renders no PVC or CSI mount for any guarded role.
4. **D3 -- fresh cutover: complete.** The immutable image was deployed without
   the claim while retaining PostgreSQL, and `boltz-l4-fleet` was recreated
   from canonical YAML.
5. **D4 -- transition retirement: complete.** The obsolete release-owned
   PVC/PV/access-point path is absent. The last initiative-specific RWX
   authority-fence fixture and its one-Pod compatibility assertion are removed;
   generic non-HA PVC support remains an independent SkyPilot feature.

The implemented change touched 23 production/chart files and seven test/fixture
files: about 735 added production/chart lines and 1,110 added test lines before
documentation. It added no central storage tables, importer, provider, daemon,
or infrastructure module. F1 is the broadest source boundary because every
byte-accepting and raw-log surface must fail before touching a path. F2 remains
a small tightening of the existing PostgreSQL recovery path, not a replacement
recovery subsystem.

This is materially smaller than #1642/#1643, but it is not a one-line Helm
toggle. Rendering emptyDir while silently accepting local uploads or a legacy
recovery fallback would convert EFS failures into nondeterministic pod-local
loss.

## Completed cutover receipt

The one-time procedure is complete and is intentionally not retained here as an
executable runbook. Git history contains the reviewed sequence. Its durable
outcome is:

- PostgreSQL and its fenced history were retained; no EFS bytes were imported.
- The old fleet lifecycle was removed through supported teardown, and the new
  service incarnation was created from the canonical YAML and required Secrets.
- Helm revision 594 runs release 1.1.1470 with `storage.enabled=false`, bounded
  disk-backed `emptyDir` volumes, and no SkyPilot PVC or CSI mount.
- The release-owned claim/access-point transition path is absent. Shared base
  filesystems, StorageClasses, and CSI components outside SkyPilot release
  ownership were not changed.
- Future upgrades retain PostgreSQL and use ordinary direct-Helm fix-forward.
  They do not repeat teardown, recreate the service, or revive a storage
  transition merely because the control-plane image changes.

## Verification status

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
- Helm tests render guarded multi-replica roles with every chart-owned emptyDir
  bounded by the fixed profile, matching role ephemeral-storage resources,
  operator extras limited to read-only projections, consistent full-hold
  projection, and zero PVC objects, claims, mounts, or EFS references.
- Deliberately removing a local materialization and restarting a role changes
  no durable state.

### Live installation receipt

- Helm revision 594 / release 1.1.1470 has two API, three executor, and two
  controller Pods Ready on one homogeneous image and chart cohort.
- The `skypilot` namespace has no PVC. Pod specs contain no PVC or CSI volume,
  and guarded HA runs with `storage.enabled=false`.
- PostgreSQL remains the structured authority while every role uses only
  bounded, disposable local materializations.
- `boltz-l4-fleet` was cleanly recreated from canonical YAML as a new service
  incarnation. A point-in-time revision-594 receipt reached 280/280 fresh Ready
  workers without a paid launch and then advanced to a coherent 290/290 at
  06:44 UTC as ten more PHX slots were confirmed. This is reserved-capacity
  evidence, not proof that the deleted service incarnation was recovered.
- Exact 10,000-request completion and a fresh post-campaign current-demand API
  projection passed separately on revision 594. No nonzero revision-594 visual
  dashboard sample is claimed. Paid L4 residual qualification remains open in
  `docs/designs/serve-multi-pool-reserved-capacity-fill.md`. None of those
  request/economic results is inferred from this storage receipt.

## Steady-state rollback boundary

The one-way storage cutover is complete. A rollback that remounts the former
filesystem or reintroduces a predecessor-local fallback is unsupported. Repair
with a fix-forward control-plane image; if test service state must be discarded,
use supported teardown and a clean recreation from canonical YAML. PostgreSQL
remains the structured authority, while canonical service YAML and immutable
external worker artifacts remain the clean-bootstrap inputs.

No SkyPilot-owned EFS transition resource remains. A shared base filesystem,
StorageClass, or CSI component outside this release may continue to exist for
other workloads; its existence is not a SkyPilot dependency or deletion target.

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

Rejected because it would give up the PostgreSQL multi-replica failover contract and
does not test the controller path being qualified for reserved fill.

### KubeRay, FUSE, cache service, or Terraform expansion

Rejected because none is needed for PostgreSQL authority plus bounded local
materialization.

## Open gates

No storage-cutover gate remains. Serve request-load, controller failover, and
reserved-capacity qualification completed in their respective designs; the
bounded paid-Spot residual/drain exercise remains there. Any failure must fix
forward without remounting shared storage. Generic non-HA PVC support is
outside this production profile and does not authorize a guarded-HA PVC or EFS
fallback.
