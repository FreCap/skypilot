# Stateless HA control-plane storage

Status: Proposed canonical design, awaiting independent re-review. Review 12
is NO-GO on the superseded source-registry design. Review 13 replaced that
machinery with a bounded restore-age proof, but exact-head review found five
remaining contract defects. Review 14 incorporates their narrow corrections
below without restoring a provider registry or a second storage path. No
application code, infrastructure, migration, or deployment has been performed
from this design. This design may merge only after an independent review returns
GO on its exact diff. Gate 1 separately authorizes implementation and
destructive decommission; it is not a design-merge gate.

Last updated: 2026-08-19

This file owns control-plane blob and log storage, database-restore interaction
with those bytes, and removal of the production SkyPilot EFS claim. The live
API/executor/controller role split, PostgreSQL request delivery, controller
leadership, and Serve actuation stay owned by
`docs/designs/multi-replica-api-server.md`.

## Decision

Guarded production HA has one steady-state storage path:

- PostgreSQL is the sole structured and transactional authority.
- One private, versioned, server-owned S3 bucket stores immutable blob and log
  bytes with SSE-KMS.
- Every readable object is an exact bucket, key, version ID, byte count, and
  digest committed in PostgreSQL. Prefix listing is never readable authority.
- Bounded disk-backed `emptyDir` holds upload parts, materializations, generated
  files, and log spool. Those paths are disposable.
- No SkyPilot PVC, EFS mount, shared path, FUSE mount, or filesystem fallback
  remains in guarded HA.

The 23.8 GB EFS tree moves to S3, not into the approximately 5.52 GB Aurora
database. PostgreSQL stores ownership, references, state, clocks, fences,
digests, sizes, and exact S3 identities; it does not store opaque archives or
log payloads.

The deletion proof is intentionally small. A production database may be
promoted only when the data inside it is at most seven days old. Blob bytes are
not physically deleted until fourteen days after an append-only PostgreSQL
tombstone. Therefore every database that is still eligible for promotion must
contain the tombstone before the S3 version can disappear. Snapshot copies do
not become younger when copied, and old snapshots may remain as archives but
can never become production authority.

This removes the superseded provider-source registry, source/GC generations,
per-source isolated-restore farm, per-tombstone backup joins, possible-commit
source scans, WAL/LSN reasoning, and log restore-safe tombstones. It keeps one
database promotion fence, fresh incarnation-scoped keys, one exact-version
blob GC path, and one bounded old-incarnation orphan sweep. There is no new
database product, queue, cache, CRD, webhook, or always-on service.

## Fixed protocol constants and safety proof

Version 1 fixes these immutable constants:

| Constant | Value | Meaning |
| --- | --- | --- |
| `MAX_PROMOTABLE_DATA_AGE_V1` (`H`) | 168 hours | Oldest database content that may become production authority |
| `CANONICAL_HEARTBEAT_PERIOD_V1` | 5 minutes | Maximum interval between canonical database heartbeats |
| `BLOB_DELETE_DELAY_V1` | 336 hours after tombstone | Earliest physical deletion of a retained blob version |
| `BLOB_TOMBSTONE_COMPACTION_DELAY_V1` | 336 hours after exact absence | Earliest removal of the compact tombstone and detailed deletion rows |
| `OLD_INCARNATION_SWEEP_DELAY_V1` | 336 hours after retirement | Earliest deletion of an unreferenced old-incarnation version |
| `CONTROL_PLANE_LOG_READABILITY_RETENTION_V1` | 30 days from S3 version creation | Fixed readable lifetime; later reads return a typed expiry gap while physical lifecycle cleanup may finish later |

`BLOB_DELETE_DELAY_V1` is two full promotion windows. The second full delay
before tombstone compaction is deliberately conservative. Log readability ends
at its exact database-computed boundary; asynchronous S3 deletion is not a
second product-retention clock. No runtime setting, Helm value, Aurora retention
change, snapshot copy, or operator waiver may change these constants.

Let `t0` be the database-clock commit time of a blob tombstone. Physical delete
cannot begin before `t0 + 336h`. At that instant a promotable database must
contain a canonical heartbeat at or after `now - 168h`, hence at or after
`t0 + 168h`. That database necessarily contains the tombstone. A source from
before `t0` is already too old to promote. Creating or copying an AWS resource
does not alter the heartbeat stored in its database pages.

The tombstone and heartbeat transactions serialize through the same singleton
lock and monotonic database sequence. The tombstone records that sequence and
its incarnation; a later heartbeat cannot commit without observing a sequence
after it. The age comparison therefore proves ordered database content, not
merely two unrelated wall-clock samples.

The same argument applies to a retired incarnation. After 336 hours, every
database from that incarnation is older than the 168-hour promotion ceiling;
an exact inventory may delete only old-incarnation versions absent from the
current database's committed reference set. A promotion racing deletion first
closes the shared promotion/GC fence and waits for bounded in-flight version
deletes to quiesce.

After exact blob absence at `t1`, the compact tombstone remains until at least
`t1 + 336h`. At compaction, any still-promotable source either contains the
tombstone or was created from the canonical database after the retired blob row
was removed. A pre-tombstone source is permanently outside `H`. Thus compaction
needs no backup inventory or tombstone-membership registry.

Increasing `H`, admitting a source already older than `H`, or making an
archival source promotable is forbidden in version 1. It would require a new
design that proves previously deleted S3 references cannot be exposed. A
configuration change can never retroactively broaden the restore window.

## Goals and non-goals

### Goals

- Remove the production guarded-HA control plane's EFS/PVC dependency without
  weakening request, Job, Serve, or cluster state durability, while making log
  byte expiry explicit and typed.
- Give each durable byte one immutable S3 version and one PostgreSQL owner,
  lifecycle, reference set, and fence.
- Recover all three roles from PostgreSQL, S3, Secrets, and workload identity
  after total pod loss.
- Make database restore, blob deletion, and cutover fix-forward, bounded, and
  testable with one happy path each.
- Delete only the exact SkyPilot EFS resources after an independently
  sufficient provider no-new-client fence and one reset-free 168-hour
  observation horizon.

### Non-goals

- Generic non-HA/local chart `storage.*`, user-supplied PVC, and local
  `BlobStorage` support remain separate compatibility surfaces.
- This does not put bulk bytes in PostgreSQL, add an S3 filesystem/FUSE layer,
  preserve EFS as rollback, or add a second structured authority.
- This does not create a general backup catalog, restore product, object
  lifecycle framework, or cross-cloud database migration mechanism.
- This does not update a `boltz-platform` SkyPilot pin or justify a broad
  Terraform/Terragrunt refactor. SkyPilot fixes merge forward and deploy with
  direct Helm.
- Version 1 promotion sources are limited to Aurora physical PITR/automated
  backup restores, direct/manual physical DB-cluster snapshots, physical
  snapshot copies or RAM shares, and AWS Backup physical restores. Copy-on-write
  Aurora clones, Aurora Global Database members, Blue/Green deployments,
  logical replication, and logical dump/import databases are non-promotable and
  never receive SkyPilot S3 writer authority. Supporting any excluded source
  requires a new protocol review; provider resource age or apparent replication
  health cannot waive that boundary.

## Production evidence and scope

Read-only evidence captured on 2026-08-18 and refreshed on 2026-08-19 shows:

- Helm revision 436 runs SkyPilot 1.1.1349 from commit
  `b34661c43015c05d5bb2a6358b1d9335fbd465f1`, image digest
  `sha256:07579af96b42de183b404d8cb23a6452598e59f22c7a9f29810694fbe2bf08d3`.
- API, executor, and controller each have two Ready pods. All six mount
  `skypilot-state-rwx` at `/root/.sky`, `/root/.ssh`, and `/root/sky_logs`.
- The single SkyPilot volume handle is
  `efs:fs-00a7dd95ad52c0ade::fsap-027d9430f450bb777`; its access-point root is
  `/dynamic_provisioning/pvc-8001cb94-a060-402c-be6a-7899d9dd972c`.
- At 2026-08-19T06:12:43Z the filesystem reports 23,801,901,056 metered bytes,
  all in Standard storage. There is no second EFS authority root.
- Aurora automated PITR retention is seven days. One manual snapshot dated
  2026-08-08 is already older than `H`; it is archival and non-promotable.
  There is no Aurora AWS Backup plan and no Aurora clone in the audited
  hub/us-east-1 scope.
- PostgreSQL already owns requests, queues, leases, controller ownership, and
  operational cluster, Job, and Serve records. `BlobStorage` and API 41+
  content-addressed `blob_id` exist, but blob bytes and several log/read paths
  still use POSIX paths.
- `boltz-l4-fleet` authority modes are outside this storage change. This design
  neither promotes `DURABLE_INTENT` nor changes demand, placement, or paid/zero-
  cost actuation.

The implementation must refresh this evidence immediately before mutation.
Discovery of another SkyPilot filesystem root blocks cutover and updates this
design; it is not folded into a generic migration loop.

## Public contract

Guarded HA has one legal committed storage mode after cutover: `S3_V1`. Every
API, executor, and controller pod carries the same immutable storage protocol,
generation, S3 fingerprint, and database incarnation. A role is not Ready and
cannot perform a durable mutation when any value differs from PostgreSQL.

`S3_V1` fingerprints one canonical JSON document. It contains the protocol and
all fixed constants; AWS partition, account, region, expected bucket owner and
bucket ARN; every exact root; versioning state `Enabled`;
`BucketOwnerEnforced`; all four Public Access Block values; default SSE-KMS key
ARN and encryption-context schema; canonical bucket-policy, KMS-key-policy and
KMS-grant digests; lifecycle-rule digest; and the complete access-bearing
principal set, including the exact runtime, migration, promotion/config-
verifier, staging-cleanup, and GC principal ARNs. IAM/policy documents are
normalized by parsed statement identity and canonical JSON rather than provider
response ordering. No image digest, Helm revision, or mutable application value
is part of this static storage fingerprint.

Before generation commit, every database promotion, and every application or
fence rollout, a schema-owned verifier reads those exact S3/KMS controls,
recomputes the fingerprint, and stores a database-clock receipt. Its provider
configuration permissions are read-only; one disjoint capability may only
conditionally create, read/checksum, and exact-version delete a random staging
sentinel. Each role independently refreshes the same bounded configuration
check at least every five minutes and fails readiness or durable mutation when
its last successful receipt is older than ten minutes. A mismatch cannot be
accepted by copying PostgreSQL's expected digest, and the sentinel never
exercises authoritative deletion. Runtime, Helm, migration, staging-cleanup,
and GC identities cannot change any fingerprint input.

The public upload/download, request, Job, Serve, cluster, and log APIs retain
their existing shapes. Callers never choose an S3 key/version and never receive
bucket credentials. API 41+ keeps the existing content-addressed upload flow.
One bounded server-side per-user adapter preserves API 24--40 until
`MIN_COMPATIBLE_API_VERSION > 40`; its stacked removal PR is independent of EFS
decommission.

Post-cutover recovery is fix-forward with an S3-capable split-role image.
`--role=all`, a PVC, EFS, a POSIX fallback, or an old Helm revision cannot be a
production recovery path. Outside guarded HA the generic chart remains source
compatible.

Application deployment remains direct Helm with immutable image/chart digests
and `--reuse-values`. A separately authorized direct-Helm release owns the
admission fence. `boltz-platform` owns only minimum static S3/KMS/IAM and
identity/RBAC boundaries plus surgical EFS desired-state removal; it does not
own the SkyPilot release, image allowlist, or a SkyPilot pin.

## S3 blob contract

The bucket has disjoint roots:

- `staging/` for disposable uploads and multipart parts;
- `authoritative/blobs/` for client archives;
- `authoritative/legacy/` for migrated manifests/content; and
- `authoritative/logs/` for common log segments.

Authoritative keys contain deployment UUID, database incarnation UUID, domain,
owner, lifecycle epoch, and a random attempt ID. They are never reused or
overwritten. Every authoritative `PutObject` or multipart completion uses
conditional creation; a conflict is reconciled against the one expected
attempt and never creates a replacement version under the same key.

Bucket controls require TLS, BucketOwnerEnforced ownership, Public Access
Block, the exact SSE-KMS key, and the expected encryption context. Explicit
resource-policy denies prevent runtime, migration, Helm, and staging-cleanup
identities from calling `DeleteObject` or `DeleteObjectVersion` under any
authoritative blob, legacy, or log root. The digest-pinned storage-GC Job may
delete only an exact version claimed through its schema-owned PostgreSQL
function: a tombstoned blob/legacy version or an old-incarnation complement. It
cannot issue bare deletes. Current-incarnation log versions have one physical
cleanup path: the fingerprinted lifecycle below. Lifecycle may expire staging
and the exact log root; it can never expire authoritative blob or legacy
versions. The complete lifecycle and resource-policy digests are fingerprint
inputs, and runtime, Helm, migration, staging-cleanup, and GC identities cannot
change them.

An upload transaction reserves owner/deployment byte, object, and row quota and
records its immutable key, expected size/digest, incarnation, lifecycle epoch,
and attempt before S3 I/O. No PostgreSQL lock spans an S3 call. Publication
records the verified version ID, size, digest, KMS identity, and completion
receipt before a blob becomes `READY`. Lost responses reconcile only that key
and attempt. Staging cleanup may abort its own multipart upload and delete only
its own staging versions.

A blob reference, logical retirement, and attachment serialize on the same
PostgreSQL row. Request, Job, and Serve-version references are explicit and
transactional. Once the append-only deletion tombstone commits, reactivation is
forbidden. Re-uploading identical content creates a fresh lifecycle epoch,
attempt, key, quota reservation, and version; it cannot reuse deleted bytes.

The migration represents existing client archives as `CLIENT_ARCHIVE_V1` and
legacy directory-shaped state as an immutable `LEGACY_TREE_V1` manifest plus
content objects. Unknown ownership, unreadable entries, escaping symlinks,
special files, unresolved credentials, or a mismatched second inventory block
cutover. Secrets and workload credentials stay in Kubernetes Secrets or
projected identity and are never copied to S3.

Materialization is a context-managed capability under a fenced PostgreSQL
lease. It verifies the exact archive or manifest graph and safely extracts it
into a private bounded `emptyDir` path for one subprocess/action scope. Path
traversal, special files, hardlink/symlink escape, file-count, expanded-byte,
depth, and inode limits fail closed. Scope exit, cancellation, lease loss, pod
death, or restore removes the disposable path. No extracted tree is authority.

## Database incarnation and promotion

One PostgreSQL singleton stores storage generation/fingerprint, deployment UUID,
active incarnation UUID, writer gate, promotion lease/fencing token, and the
last canonical heartbeat. An append-only incarnation table stores each
activation/retirement receipt. Only the elected canonical leader under
`WRITERS_OPEN` may advance the heartbeat, at least every five minutes. Each
heartbeat binds the active incarnation, a monotonic sequence, and the database
clock; a future, regressed, wrong-incarnation, or missing value is invalid.
The transition release begins this heartbeat before the first promotable
`S3_V1` recovery checkpoint. Any older backup without a valid heartbeat is non-
promotable after `S3_V1`; provider retention does not grandfather it.

The provider restore workflow creates every candidate without application
network, endpoint, Secret, or workload-identity access. As soon as PostgreSQL is
available, its first database effect CASes the copied singleton from
`WRITERS_OPEN` to `RESTORE_FENCED` under a random pending-promotion token. No
SkyPilot process can run before that receipt. Failure or ambiguity destroys or
leaves the candidate isolated and permanently non-promotable; it never assumes
that a copied open-gate value is current authority. A restored, copied,
detached, or candidate database cannot advance its heartbeat while fenced.

Every candidate starts network-isolated from application roles. Its only
database credential can execute the schema-owned promotion verifier. The
separate promotion identity first records one immutable candidate receipt that
binds the restore operation family, candidate/source ARNs and resource IDs,
account/region, engine/version, original physical recovery point or PITR target,
copy/share provenance, provider request IDs, and terminal provider status. It
then proves the operation is one of the physical Version 1 families above and
that the candidate is not a clone-group member, Global Database member,
Blue/Green source or target, logical replica, or logical import. Missing,
ambiguous, changing, or unsupported provenance rejects and destroys or leaves
the candidate isolated. This per-attempt classification receipt is not a
provider-source registry, is never consulted by blob GC, and cannot make a
source younger.

The database-resident `last_canonical_heartbeat` is the sole source-data age.
Provider resource creation, snapshot-copy, AWS Backup copy, restore-job, and
cluster-rename times are never substituted for it. Where the physical restore
operation exposes a semantic maximum data cutoff, such as a requested PITR
target, a stored heartbeat later than that cutoff is an integrity failure, not
an instruction to take the minimum. Before any endpoint, Secret, DNS, or
security-group switch, promotion requires all of the following:

- the canonical heartbeat is no older than the current database clock minus
  168 hours, is not in the future, and passes the sequence/incarnation checks;
- deployment UUID, storage generation/fingerprint, KMS identity, and schema
  protocol match exactly;
- the provider candidate receipt identifies one allowed physical source and
  passes every exclusion above;
- every `READY` or `RETIRED` blob/legacy object and every unexpired committed
  log segment exists at its exact version with expected size, content digest,
  metadata and KMS identity;
- a `TOMBSTONED`, `DELETING`, or `DELETED` blob/legacy version is never made
  readable: presence is retained and charged, while absence is accepted only
  when the append-only tombstone/sequence is valid and its fixed 336-hour delete
  boundary has passed (or the selected database already contains the exact per-
  version receipt proving an authorized deletion at or after that boundary and
  provider-confirmed absence); an earlier or unclassified absence rejects
  promotion;
- an expired log segment is projected as `EXPIRED_BY_POLICY` from its original
  S3 creation time whether its exact version remains present or lifecycle has
  removed it; an unexpired absence rejects promotion;
- restored nonterminal upload, materialization, and log-writer attempts are
  classified for invalidation and old-incarnation accounting, never replayed or
  treated as readable references;
- a complete S3 version/MPU inventory fits the deployment physical-capacity
  budget; unknown old-incarnation bytes count conservatively until swept; and
- all current production writers, leases, provider actions, and storage-GC
  permits are fenced and quiescent.

While writers are closed, promotion stores the complete inventory byte/object
totals as a conservative deployment physical-usage floor. New reservations add
to that floor; only exact deletion/absence or a later complete inventory may
lower it. Per-owner logical quota still comes from PostgreSQL, while unknown
old-incarnation bytes consume deployment headroom until the one sweep removes
them. A restore therefore cannot manufacture quota by omitting newer rows.

The promotion transaction creates a random new incarnation UUID, invalidates
all restored nonterminal upload/materialization/log writer state, records the
state-aware present/absent/expired projection above, records the old incarnation
as pending retirement, and keeps writers closed. It does not release quota from
an inventory omission; the ordinary exact-absence transaction does that later.
All later attempts, leases, multipart IDs, log segments, and S3 keys are fresh.
After state-aware reference verification and endpoint isolation readback, a
final transaction opens writers and the deployment rolls to the new
incarnation. No restored attempt is replayed or adopted.

`retired_at` is recorded only after the old application identities, writer
leases, provider-action handlers, multipart workers, and GC permits have a
durable quiescence receipt. Any late old-incarnation effect invalidates that
retirement time and blocks its sweep; elapsed wall time alone cannot repair it.

Physical PITR/automated backup, direct/manual snapshot, copied or RAM-shared
snapshot, and AWS Backup sources may supply candidate bytes only when this
content-based age, state-aware reference, and identity proof passes. A copy or
share does not refresh age. Once a static source's stored heartbeat ages past
168 hours it is permanently archival for this protocol even if the provider
still allows restore. Provider creation therefore does not race blob GC and
requires no source registry. Raw provider restore authority is separate from
production promotion authority; ordinary runtime/Helm roles cannot switch the
database endpoint or open the writer gate. Break-glass use is an audited
incident and does not waive the verifier or admit an excluded source family.

## Exact-version blob GC and old-incarnation sweep

Blob GC has one retained-data state path:

`READY -> RETIRED -> TOMBSTONED -> DELETING -> DELETED`

The `TOMBSTONED` transaction locks the blob and quota rows, proves all request,
Job, and Serve references absent, rejects active upload/materialization leases,
records every exact authoritative version, and writes a random append-only
tombstone UUID/digest, lifecycle epoch, incarnation UUID, serialized singleton
sequence, and database-clock time. It retains all byte/object/row quota and
performs no S3 call.

At `tombstoned_at + 336h`, the bounded digest-pinned GC CronJob may claim the
row only under the current storage generation, incarnation, open writer gate,
and promotion fence. Before each individual `DeleteObjectVersion`, a schema-
owned function issues one short-lived permit for that exact row/version after
rechecking the same values and the absence of references. Promotion closes new
permits and waits through the maximum provider-call deadline before changing
incarnation. A lost response is reconciled with exact-version `HEAD`; the worker
never uses bare `DeleteObject`, a prefix, or a different version.

After every version is proven absent, one transaction releases exact byte and
object quota and records `DELETED`. Detailed rows and the compact tombstone stay
row-charged for another 336 hours. Only then may the same Job remove them and
release row quota. The age proof above replaces backup joins; no isolated
restore or aggregate tombstone-membership proof is involved.

Current-incarnation prepared objects use the same exact-version worker after a
schema-owned transaction proves their attempt never committed. On database
promotion, restored nonterminal attempts are simply invalidated and their keys
remain under the retired incarnation; they do not become readable, reusable,
or per-family restore-quarantine rows.

One old-incarnation sweep handles bytes absent from the restored database. It
runs no earlier than `retired_at + 336h`, obtains a complete paginated inventory
of all authoritative versions and MPUs under that incarnation, and joins it to
the current database's exact committed object set, including every live,
retained, tombstoned, and not-yet-compacted blob/legacy row and every unexpired
log segment. It retains every match and may delete only the exact-version
complement, with the same per-call promotion fence; an unreferenced MPU is
aborted only by its exact upload ID. Missing pages, changing inventory,
ambiguous ownership, or a new promotion abort the sweep. This one inventory
batch replaces cross-family restore quarantine and possible-commit-point
classification.

The GC identity has no upload/copy, bare-delete, bucket-policy, lifecycle, KMS,
application-provider, admission-policy, or database-table mutation authority.
Its database credential can invoke only schema-owned claim/progress/complete
functions. If GC is absent, quota remains charged and alarms grow; foreground
provider work is unaffected.

## Common log contract

One common library covers request execution, managed Job, Serve controller and
replica, ordinary cluster provisioning/history, and control-plane role logs:

1. a bounded local spool accepts typed frames;
2. a fenced writer publishes immutable S3 segments;
3. PostgreSQL records stream ownership, incarnation, writer epoch, exact
   versions, offsets, terminal state, and typed gaps; and
4. one reader implements follow, tail, and download from that index.

The PostgreSQL stream/segment row is prepared before S3 I/O and committed only
after exact version verification. Lost responses reconcile the one segment key.
A provider action never waits for log durability after its own durable action
row commits; log failure records `UNKNOWN_TAIL` or a bounded truncation gap and
never replays the action.

Log bytes use one fixed policy instead of blob tombstones. The exact S3 version
creation time determines `expires_at = created_at + 30d`. Before that instant a
reader requires the exact version. At or after it, the API returns the typed
`EXPIRED_BY_POLICY` interval even if provider lifecycle has not yet removed the
bytes. A restored database computes expiry from the original version creation
time; restore or copy time never refreshes it.

The fingerprinted `authoritative/logs/` lifecycle uses `Expiration.Days = 30`
for current versions, `NoncurrentVersionExpiration.NoncurrentDays = 1` for the
version made noncurrent by the lifecycle-created successor/delete marker, and a
separate expired-delete-marker cleanup rule. S3 day rounding and asynchronous
execution mean physical bytes normally outlive the exact 30-day API boundary
and have no protocol upper-bound deletion instant. That lag never makes them
readable again. Lingering versions and delete markers remain in the deployment
physical-usage floor and cost telemetry until exact inventory proves absence; a
lifecycle delay cannot manufacture quota. No lifecycle rule outside the exact
log and staging roots may delete a version. Log metadata may compact only after
expiry plus 336 hours, preserving the stream-level typed gap. These physical
semantics follow AWS's documented [versioned-bucket lifecycle
rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html).

This deliberately allows early segments of a long-lived job to age out after
30 days. A longer product log promise requires a new fixed protocol value and
forward-only migration for newly created segments; it cannot re-admit expired
bytes or be inferred from a restored database.

## Pod, identity, and admission contract

The final chart uses only bounded disk-backed `emptyDir`, Secret, ConfigMap,
downward-API, and projected service-account-token volumes. Every workload has
measured ephemeral byte/inode/concurrency limits; a missing limit rejects
`S3_V1`.

One built-in Kubernetes `ValidatingAdmissionPolicy` and binding, owned by the
separate `skypilot-storage-fence` Helm release, contains:

- a namespace-wide principal-independent safe-volume floor; and
- targeted exact image, command, identity, workload-class, protocol,
  fingerprint, operation-token, and resource-shape checks.

The policy is self-contained: `spec.paramKind` and binding `spec.paramRef` are
absent. State, image digests, principals, tokens, workload classes, safe volume
kinds, and limits are inline CEL constants in each immutable rendered revision.
The routine SkyPilot Helm identity cannot mutate the policy/binding; a separate
fence identity applies it under the PostgreSQL cutover lease.

The floor covers Pods, PodTemplates, and every nested PodSpec in Deployments,
ReplicaSets, StatefulSets, DaemonSets, Jobs, CronJobs, and
ReplicationControllers, plus `pods/ephemeralcontainers`. In `S3_ONLY`, each
`volumes[*]` has exactly one positively allowed source. `emptyDir` must be disk-
backed with a present bounded `sizeLimit`, and projected items may contain only
Secret, ConfigMap, downward-API, or service-account-token sources. Every other
source is denied, including generic `ephemeral`, PVC, CSI, NFS, every
`hostPath`, block/cloud/network volumes, FUSE, and any future source. All
container kinds reject `volumeDevices`, privileged/`SYS_ADMIN`, and host or
bidirectional mount propagation.

The policy uses `failurePolicy: Fail`, `validationActions: [Deny]`, equivalent-
version matching, and explicit CREATE/UPDATE rules. In `S3_ONLY`, it has no
name, label, owner, ServiceAccount, or principal exception to the floor. This is
necessary because live `skypilot-api-sa` can create arbitrary names and
alternate ServiceAccounts in the namespace. Tests derive the disallowed
`VolumeSource` complement from the production Kubernetes OpenAPI schema and
impersonate that exact ServiceAccount at every PodSpec nesting; an untested
field fails CI.

`TRANSITION` and `SEALED` add only their closed, phase-scoped EFS volume tuples:
the exact claim/PV/volume handle/access point, read-only mode where required,
digest-pinned image and command, ServiceAccount, operation token, and resource
shape must all match. This is a positive entry in that immutable policy
revision, not a name, label, or identity-only bypass. Changing any field is
denied. Claim name alone is never authority: immediately before I/O, the
schema-owned operation re-reads the pinned PVC/PV UIDs and resourceVersions,
CSI volume handle, provider access point, lease, and token; ordinary identities
cannot replace or mutate them. `S3_ONLY` removes those tuples permanently.

Cutover uses three complete policy revisions under one database lease:

1. `TRANSITION` admits the exact audited EFS release, prepared S3 releases, and
   the exact migration and verification workloads.
2. `SEALED` denies every application, controller, and general Pod/template
   CREATE/UPDATE. Its sole workload shape is one retryable, digest-pinned
   cleanup Job with the exact access point, command, ServiceAccount, and
   operation token. Before `S3_V1`, its schema-owned capability cannot delete;
   after commit it may remove only the audited tree and is never storage
   authority or rollback.
3. `S3_ONLY` admits only digest-pinned S3 workloads and the temporary observer;
   the namespace floor rejects all historical EFS/PVC revisions.

Each update reads the committed database mode immediately before mutation and
records the policy/binding UID, resourceVersion, and canonical digest. Native
Helm rollback, `--atomic`, and historical fence reuse are forbidden. After
`S3_V1` commits, only the exact cleanup class may touch EFS until the provider
client fence is read back; no application or fallback EFS class can return.

## Migration and cutover

The migration inventories only the one audited PVC/PV/access point. Every safe
relative path receives a digest, byte count, owner, and one outcome: blob,
legacy manifest/content, log, already-structured PostgreSQL value, Secret/
projected credential, disposable cache/generated file, or blocking unknown.
Two inventories and two imports must be identical.

Before production, an isolated rehearsal restores the EFS backup, imports it,
reconstructs request/Job/Serve/cluster state without EFS, and proves
`boltz-l4-fleet` recovery causes no provider mutation. It measures the full
maintenance path, including backup/restore, import, `SEALED`, generation commit,
EFS cleanup/unmount, provider client fence, `S3_ONLY`, primary and prebuilt
fix-forward rollouts, 2/2/2 readiness, historical-revision denial, PVC/PV
unbind, and maintenance reopening. Every rehearsal restore uses a new isolated
filesystem and ends by deleting its access points, mount targets, filesystem,
and disposable recovery directory, with provider `NotFound`, a terminal restore-
job receipt, and no automatic-backup selection or retained recovery point for
the temporary resource. The owner approves one immutable RTO and component
deadlines from that evidence.

Production cutover is one ordered operation under a PostgreSQL database-clock
lease and random fencing token:

1. Install/read back `TRANSITION`; deploy the transition image directly with
   Helm `upgrade --reuse-values`; prepare schemas, S3 fingerprint, imports, and
   both immutable S3-capable rollout artifacts without changing authority.
2. Enter maintenance, reject new mutations, drain/classify work, and scale the
   six role pods to zero. Prove no unclassified writer remains.
3. Run the exact importer with a read-only mount and finish the one-root import.
4. Capture the final manifest, create a new EFS backup after zero-writer proof,
   restore it to one newly created network-isolated temporary filesystem, and
   require exact manifest equality. Verify every retained PostgreSQL reference
   resolves according to its state to an exact S3 version or typed gap. Delete
   the temporary restore's access points, mount targets, filesystem, restore
   directory; require provider `NotFound`, a terminal restore-job receipt, no
   automatic backup enrollment, and an exact zero-resource receipt. The source
   recovery point remains sealed only through the observation horizon: no
   SkyPilot or ordinary deploy identity may restore it, copy it, or create
   another EFS resource, and a vault lock/retention date beyond the planned
   terminal cleanup blocks cutover.
5. Install/read back `SEALED`, run all negative admission probes, and commit
   `S3_V1` only while the lease, deadline, manifests, quotas, backup, and both
   prebuilt fix-forward artifacts remain current and the isolated restore has
   the terminal cleanup receipt above. A lost response is read from PostgreSQL.
   Before this commit only, failure may restore the exact `TRANSITION` revision
   and unchanged EFS generation.
6. Keep every application identity fenced. Require Aurora's latest restorable
   time to pass the `S3_V1` commit and a post-commit manual snapshot to become
   available. Bind both provider receipts to the committed generation,
   incarnation, heartbeat sequence, and database-clock time. Neither receipt
   changes source age or bypasses the promotion verifier.
7. Create the exact `SEALED` cleanup Job. It verifies the new
   generation, deletes only the audited SkyPilot tree, flushes/unmounts,
   terminates, and writes the last-client receipt. A lost Job may retry the same
   idempotent operation; no other EFS workload is admissible. The provider-fence
   identity then installs and reads back `EFS_CLIENT_FENCE_V1` as specified
   below.
8. Install/read back `S3_ONLY`, including live `skypilot-api-sa` negative
   probes, then deploy the no-PVC chart. If the primary rollout misses its
   deadline, deploy the already-built S3-only fix-forward artifact; EFS is never
   restored.
9. Require 2/2/2 readiness, the state-aware reference/absence checks above, one-
   pod-per-role and total-blackout recovery, no EFS mounts, and denial of every
   retained historical revision. Delete the unbound PVC/PV under their verified
   `Retain` contract, revoke EFS backup/restore and new-filesystem authority,
   prove the sealed final recovery point is non-operational, reopen maintenance,
   and release the lease.

Every phase has an absolute database-clock deadline. A missed post-commit
deadline is an incident and fix-forward outage, not permission to weaken the
fence, extend the clock, run new code, or remount EFS. No `boltz-platform`
SkyPilot pin changes; application artifacts merge forward and deploy with Helm.

## EFS provider fence and decommission

EFS deletion uses one clock:
`EFS_DECOMMISSION_OBSERVATION_V1 = 168h`. Diagnostic checks at immediate,
+10m, +30m, +24h, one-pod-per-role, and total blackout never shorten it.
CloudWatch is a positive reset signal, not the no-client authority, because AWS
publishes EFS metrics sparsely and supplies no bounded publication-finality SLA.

`EFS_CLIENT_FENCE_V1` is independently sufficient and is installed after the
last cleanup/unmount but before the clock:

- For a proven-exclusive filesystem, delete the SkyPilot access point and all
  mount targets; require access-point `NotFound` and an empty mount-target list.
- For a shared filesystem, delete only the SkyPilot access point. Also prove
  either that every mount-target TCP/2049 rule excludes every SkyPilot client
  SG/CIDR, or that a custom filesystem policy denies anonymous/direct mounts and
  every SkyPilot `ClientMount`, `ClientWrite`, and `ClientRootAccess` principal
  while preserving enumerated non-SkyPilot clients. If neither proof is safe,
  the clock cannot start.
- In both cases revoke runtime/CSI/deploy authority to recreate the client path
  or weaken the fence. Also revoke their `StartRestoreJob`, EFS create,
  access-point create, mount-target create, backup-copy and backup-selection
  authority; enumerate terminal restore jobs and prove no temporary restored
  filesystem remains. Apply only the target-limited desired-state change so
  Helm, CSI, Terraform, Terragrunt, and AWS Backup cannot recreate a SkyPilot
  client or filesystem. The immutable receipt binds filesystem/access-point
  IDs, mount targets, SG and filesystem-policy digests, principal set, cleanup
  and isolated-restore-cleanup receipts, the sealed final recovery-point ARN and
  deletion eligibility, admission-fence digest, and empty post-apply plan.

Cutover ships one suspended five-minute `EfsDecommissionObserverV1` CronJob in
the fence release. It has read-only Kubernetes/AWS/database access and cannot
mount or mutate EFS. After the EFS application-code/config cleanup release is
deployed and read back, activation sets `stable_since` to the first UTC minute
not earlier than application cleanup, last cleanup unmount, last mount-pod
termination, isolated-restore cleanup, backup/restore-authority revocation, and
provider-fence readback. Every sample re-reads the exact fence, namespace
PodSpecs, EFS/restore-job inventory, release/admission receipts, and database
incarnation.

On the exclusive branch, `EFS_CW_PERIOD_V1 = 60s` and
`EFS_CW_FINALIZATION_LAG_V1 = 60m`. The observer queries exact `AWS/EFS`/
`FileSystemId` `ClientConnections` and `TotalIOBytes`, both with `Sum`, using
UTC-aligned inclusive start/exclusive end, ascending scan, every pagination
token, terminal `Complete`, unique on-grid timestamps, and finite nonnegative
values. A minute is finalized only after two identical canonical reads at least
one five-minute run apart. Positive values reset. Returned zero is clean.
Missing points are `SPARSE_SILENCE`, never numeric zero, and count as clean only
while the independent provider fence and contiguous observer chain remain
current. Partial/error/malformed/changing results reset. The 60-minute lag is a
protocol delay, not an AWS SLA.

On the shared branch, EFS metrics are filesystem-wide and cannot identify an
access point or principal. They are diagnostic only; unrelated shared I/O
neither authorizes nor blocks SkyPilot-only cleanup. The acceptance proof is the
continuous provider fence, namespace floor, and observer chain.

Exclusive final authorization requires unchanged inputs and two identical
CloudWatch reads of the fixed `[stable_since, stable_since + 168h)` interval
five minutes apart. Shared final authorization instead requires two identical
provider-fence/namespace/desired-state samples five minutes apart; its
filesystem metrics remain non-authoritative. Any forbidden PodSpec, mount, late
positive exclusive sample, observation gap over ten minutes, fence or desired-
state drift, release/image/fingerprint/incarnation change, database restore, or
receipt mismatch invalidates the epoch. `S3_V1` never permits a remount, EFS
restore, new SkyPilot filesystem, or new access point: an attempt is an incident
and resets observation, but cannot open a new approved fence epoch. Returning to
EFS would require a new design and forward protocol; no EFS reactivation,
restore, or remount occurs during or after an accepted horizon.

After acceptance, both branches delete the sealed final EFS recovery point,
exact SkyPilot backup selection/automatic-backup policy and restore authority,
then require recovery-point `NotFound`, no active SkyPilot restore job, no
restored temporary EFS resource, and an empty target-limited plan. Historical
terminal restore-job records are receipts, not resources to delete. The
exclusive branch also deletes the filesystem, security group, remaining CSI/
IAM/StorageClass edges, and exact saved Terraform objects, then requires
provider `NotFound`. The shared branch deletes no shared data, filesystem, mount
target, security group, backup plan used by other resources, or post-horizon
path; its SkyPilot directory and access point were already removed before the
clock. The observer and temporary identity are removed only after the branch-
specific final receipt.

These metric/fence semantics follow the AWS documentation for
[EFS CloudWatch publication](https://docs.aws.amazon.com/efs/latest/ug/monitoring-cloudwatch.html),
[EFS metrics](https://docs.aws.amazon.com/efs/latest/ug/efs-metrics.html),
[`GetMetricData`](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_GetMetricData.html),
[AWS Backup EFS restore](https://docs.aws.amazon.com/aws-backup/latest/devguide/restoring-efs.html),
[access-point deletion](https://docs.aws.amazon.com/efs/latest/ug/delete-access-point.html),
[mount-target deletion](https://docs.aws.amazon.com/efs/latest/APIReference/API_DeleteMountTarget.html),
and [EFS network/IAM authorization](https://docs.aws.amazon.com/efs/latest/ug/iam-access-control-nfs-efs.html).

## Implementation and cleanup stack

The exact number of PRs may split for review safety, but each behavior has one
canonical path. Transition and removal PRs are authored together and stacked;
temporary code is not left as a TODO.

| # | Scope | Required result / merge gate |
| --- | --- | --- |
| 1 | Canonical design | This file and removal of the executable-looking EFS plan from the role-split design; merge only after exact independent GO |
| 2 | Surgical static infrastructure | Reuse a suitable private versioned bucket or add only minimum S3/KMS/IAM, the complete fingerprint/readback contract, conditional-create, all-authoritative-root ordinary-delete denies, exact log lifecycle, GC identity, quotas/alarms, and fence RBAC; no live EFS mutation, broad Terragrunt change, mutable image allowlist, or SkyPilot pin |
| 3 | Blob path | Upload/reference/quota schema, immutable exact-version S3 backend, API 41+ behavior, bounded API 24--40 adapter, Serve refs, legacy import, and scoped materialization |
| 4 | Common logs | One typed prepare/publish/commit/index/read path for request, Job, Serve, cluster, and role logs; fixed 30-day readability, explicit eventual current/noncurrent/delete-marker lifecycle, and `EXPIRED_BY_POLICY` gaps |
| 5 | Promotion and GC | Canonical heartbeat, physical-source allowlist, 168-hour promotion limit, one incarnation fence, fresh keys, state-aware exact-reference/absence verification, 336-hour blob tombstone GC, and one old-incarnation sweep; no provider-source registry or restore farm |
| 6 | Cutover and observer | Generation/fingerprint, exact importer/verifier, isolated EFS-restore cleanup and retryable cleanup workloads, no-PVC chart, self-contained admission policy with exhaustive floor, one cutover lease, post-commit provider/backup-restore fence, and suspended observer |
| 7 | EFS application cleanup / observer activation | Remove EFS code, values, mounts, importer, and migration identity after S3-only acceptance; retain generic non-HA PVC support and the independently gated client adapter; activate the 168-hour observer |
| 8 | Exact infrastructure decommission | Both: delete the sealed final recovery point and SkyPilot backup/restore authority after the horizon. Exclusive: also delete only remaining SkyPilot EFS objects. Shared: remove only SkyPilot declarations/authority and prove shared resources unchanged |
| 9 | Transition cleanup | Remove observer/fence transition states/metrics/tests after final receipts; remove API 24--40 adapter separately only after its compatibility gate |

The unmerged RWX stacks in boltz-platform PRs #7824 and #7829--#7833 and draft
design PR #8443 are not implementation inputs; the canonical design supersedes
them. Existing unrelated platform history is not reverted. Every infrastructure
plan is saved, target-limited to the named objects, and reviewed for an empty
unrelated diff.

## Verification

Automated fault and adversarial tests must prove:

- upload prepare/publish/commit ordering, conditional-create conflicts, lost
  responses, multipart cleanup, exact-version reads, owner/reference/retirement
  races, transactional quota oversubscription, and no authoritative blob/
  legacy lifecycle or ordinary-principal deletion under any authoritative root;
- the canonical fingerprint changes for every bucket owner/ARN/region/root,
  versioning, ownership, Public Access Block, KMS/encryption-context,
  policy/grant, lifecycle or principal change; provider response reordering does
  not change it; stale/mismatched readback fails role readiness and mutation;
- every allowed physical database source uses the database-resident heartbeat;
  snapshot/AWS Backup copies do not refresh age; 167h59m passes, 168h plus one
  tick fails; the 2026-08-08 snapshot fails; clone, Global Database, Blue/Green,
  logical-replication and logical-import candidates always fail; an increase to
  `H` fails closed;
- candidate network isolation, old-writer/lease/GC quiescence, lost promotion
  responses, fresh incarnation IDs/keys/leases, live-reference failure, and
  total role blackout recovery without replaying a provider effect; a restore
  taken at `tombstone + 200h` passes at `tombstone + 350h` after lawful exact
  deletion, while an absent live/retired/unexpired reference and an absent
  tombstoned version before its 336-hour boundary fail;
- blob tombstone/delete/absence/compaction boundaries at 335h59m and 336h,
  per-version permit loss, promotion racing each delete, partial multi-object
  deletion, stale worker rejection, exact quota release, and no reactivation or
  key reuse after tombstone;
- restored nonterminal mutations are invalidated without per-family quarantine;
  old-incarnation inventory retains every current exact reference, deletes only
  the complement after 336 hours, and aborts on missing/changing pages or a new
  promotion;
- all log-producing domains use the common path; typed frames cannot be spoofed
  by payload; hard pod loss creates `UNKNOWN_TAIL`; fixed 30-day readability
  returns `EXPIRED_BY_POLICY` before/after lifecycle deletion and never blocks
  or replays the underlying provider action; current expiration creates a
  delete marker, noncurrent physical deletion may be delayed, lingering bytes
  remain charged, and ordinary principals cannot delete an unexpired segment;
- API 41+ compatibility, API 24--40 per-user isolation/expiry, Serve reference
  lifetimes, legacy manifest determinism, safe extraction, and every byte,
  object, row, inode, expansion, spool, and concurrency bound;
- the one VAP/binding has no parameter object, `SEALED` denies every shape
  except the exact cleanup Job, that Job cannot delete pre-commit and retries
  idempotently post-commit, every mutation of each transition-only EFS tuple is
  denied, stale/recreated PVC or PV identity fails runtime revalidation, and
  live `skypilot-api-sa` dry runs cover the OpenAPI-exhaustive non-allowlisted
  `VolumeSource` set at every PodSpec nesting, including generic ephemeral PVC
  and every `hostPath`;
- stored-value Helm renders, direct `--reuse-values`, fence/application mutual
  exclusion, killed operators at each phase, primary/fix-forward rollout,
  isolated EFS restore and exact terminal resource cleanup, post-commit PITR/
  snapshot checkpoint, 2/2/2 readiness, historical-revision denial, and a full
  no-EFS `boltz-l4-fleet` recovery rehearsal within the measured RTO; and
- last-client cleanup before the provider fence; exclusive and shared fence
  branches; denied mount/filesystem/backup-restore recreation authority; exact
  CloudWatch period/stat/lag/pagination/sparse-silence semantics; every reset;
  remount always forbidden after `S3_V1`; denial at 167h59m; final double read;
  final recovery-point deletion; no shared-resource mutation; provider
  NotFound/empty plan; and observer removal only afterward.

Production telemetry reports storage generation/fingerprint/incarnation,
fingerprint-receipt age and mismatch state, canonical-heartbeat age, rejected
promotion reason/source age, S3 physical inventory floor, blob/upload/reference/
tombstone/GC ages and quotas, retired-incarnation sweep age/bytes, log spool/
expiry/gap counts and physically pending expired bytes, S3/KMS errors, cutover
phase/deadlines, and EFS client-fence/observer/reset/sparse/positive/unknown,
restore-job, temporary-filesystem, and recovery-point state. It contains no
keys, paths, credentials, operation tokens, or signed URLs.

## Open gates

1. Before any implementation PR merges or any application, infrastructure,
   migration, or decommission mutation runs, storage, database, Serve/Jobs, and
   platform owners approve this exact design and PR/removal boundaries. This is
   not a prerequisite for merging the design after independent GO.
2. A fresh audit reconfirms the single EFS source, 23.8 GB tree, Aurora size,
   seven-day PITR, all retained/manual/copied/RAM-shared/AWS Backup/clone/
   global/Blue-Green sources, database endpoint/Secret/network promotion
   authority, EFS backup selections/recovery points/vault retention and restore
   authority, existing IAM/RBAC, and every exact infrastructure address. Every
   source older than `H` and every non-physical or excluded source family is
   marked non-promotable in the recovery runbook.
3. Owners approve the immutable 168h/336h/336h/30d constants, maximum upload and
   archive/materialization dimensions, permanent owner/deployment quota, S3
   physical-capacity floor, and log-expiry product behavior.
4. S3/KMS positive/negative probes pass without EFS change. The complete
   canonical fingerprint and bounded readback receipt pass. Runtime, migration,
   Helm, and staging identities cannot delete authoritative blob, legacy, or
   log versions; lifecycle touches only staging/log roots; GC cannot upload,
   copy, bare-delete, or mutate bucket/KMS policy.
5. Promotion and GC tests prove the bounded-age theorem at every boundary and
   with every allowed and excluded source/copy class, state-aware live/
   tombstoned/deleted/expired reference behavior, exact old-writer/delete-permit
   quiescence, and the single old-incarnation sweep. No provider-source registry,
   per-source isolated database restore, backup join, WAL/LSN, or possible-
   commit scan is introduced.
6. Blob/log/API/materialization integrations pass real PostgreSQL/S3 crash
   tests and total-blackout recovery. No foreground provider action waits on log
   shipping after its action row commits.
7. Stored-value rendering, importer, final EFS backup/restore equality, exact
   isolated-restore resource cleanup, exhaustive live-SA admission probes,
   measured RTO, immutable primary/fix-forward artifacts, post-commit Aurora
   recovery checkpoint, cleanup, provider client/backup-restore fence, no-PVC
   rollout, 2/2/2 readiness, and historical-revision denial all pass before
   maintenance opens.
8. After EFS application cleanup, the observer proves the exact reset-free
   168-hour branch-specific horizon. Exclusive deletion requires terminal
   CloudWatch/fence receipts, provider NotFound, final recovery-point deletion,
   and an empty plan. Shared completion requires final recovery-point deletion,
   access-point/client-path absence, and a plan proving no shared resource or
   data mutation. Neither branch permits a post-`S3_V1` remount or restore.

## Adversarial review record

Reviews 1--8 iterated on cutover ordering, exact-version deletion, quota, and
restore crash handling. Review 9 returned GO on the Review 8 correction diff
from `81463b717c4a0744eded4d92d0dc7c3074d604d6` to
`058fc840a2280554c04703f56c2e4460d0daea25` (SHA-256
`7d0f5d8df6ff853d57f48ad795ab302b50840a17b5da2a788d23a3ef7d68d123`),
but later review superseded that authorization.

Review 10 was NO-GO because the design depended on Aurora WAL/timeline evidence
the provider does not expose, admission could be bypassed through broad live
namespace authority, and EFS deletion lacked one exact observer horizon.
Review 11 replaced WAL proof with a provider-source registry, added a namespace
floor, and defined the observer.

### Review 12: NO-GO

Review 12 evaluated exact head
`4afb8a39b47dd468a87607345071b5fd63b908aa` and whole-diff SHA-256
`7fc78b610dd072b1ae0fcc76f5dc8b328080e8921be2ea2b1b0df781f20fcd29`.
It found incomplete clone/global/Blue-Green/RAM/AWS Backup coverage and a
source-creation/GC race; lossy aggregate tombstone compaction; a non-exhaustive
volume floor; ambiguous VAP parameters; an unsafe sparse-metric EFS clock; and a
design-merge/Gate-1 contradiction. It did not revive the WAL/LSN issue.

### Review 13: NO-GO

The simplification audit found a stronger and smaller invariant in the live
seven-day PITR contract. This revision proves restore safety from content age,
not provider resource history: the fenced database heartbeat survives snapshots
and copies, `H` never expands, blob deletion waits 336 hours, and old sources
remain archival. It removes the provider registry/restore farm/backup joins and
most restore quarantine; retains one promotion fence, one exact blob GC, and one
old-incarnation sweep; gives logs fixed lifecycle with typed expiry; makes the
VAP self-contained and its volume floor exhaustive; and makes the provider EFS
fence—not sparse CloudWatch absence—the no-client authority.

Independent review evaluated exact head
`8e42ccf26b1e7cb2f7534e2c208cfc0a04b305b5` against base
`512281d3eccb8c2c7315c90bd82cb126832d5740` (whole-diff SHA-256
`9b8ec30bb46cdff189f142ee11f36ccc5999235628dadbaf6432594d0646a1f5`).
It found five blockers: an all-reference-exists promotion check contradicted
lawful tombstone/log absence; Blue/Green contradicted clone/logical-source
exclusion; the S3 fingerprint and authoritative-log delete deny were incomplete;
the production EFS restore/recovery point lacked terminal cleanup and the
observer still permitted remount; and a fixed 30-day physical log-byte lifetime
did not match versioned S3 lifecycle semantics.

### Review 14: READY FOR INDEPENDENT RE-REVIEW

This revision keeps the bounded-age theorem and corrects only those five
contracts. Promotion now verifies references by durable state; Version 1 admits
only enumerated physical restore families; one complete S3/KMS fingerprint and
all-authoritative-root delete policy are explicit; every temporary EFS restore
and final recovery point has terminal cleanup while post-`S3_V1` remount remains
forbidden; and logs have an exact 30-day readability boundary with explicit
eventual current/noncurrent/delete-marker lifecycle. It adds no source registry,
restore farm, backup join, WAL/LSN dependency, database product, control-plane
PVC, or alternate EFS path.

This exact diff awaits independent review. It authorizes no code, chart,
infrastructure, migration, deployment, merge, cutover, or EFS deletion.

Completion means production runs 2/2/2 with PostgreSQL structured authority,
exact-version SSE-KMS S3 blobs, fixed-readability S3 logs, bounded `emptyDir`, a
healthy seven-day promotion gate and exact-version GC, zero unreconciled current
attempts, and no SkyPilot PVC, access point, filesystem mount, backup recovery
point created for cutover, restore authority, path fallback, or guarded-HA EFS
code. All transition/removal PRs and branch-specific EFS cleanup are complete
while unrelated generic storage remains unchanged.
