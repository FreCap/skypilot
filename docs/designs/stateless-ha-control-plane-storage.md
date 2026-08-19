# Stateless HA control-plane storage

Status: Proposed canonical design. Review 7 returned NO-GO on unsafe historical
negative proof and incomplete restore mutation coverage. Review 8 incorporates
the conservative corrections and awaits independent re-review plus Gate 1 owner
approval. The production inventory is verified, but no application code,
infrastructure, migration, or deployment has been performed from this design.
Implementation starts only after Gate 1.

Last updated: 2026-08-19

This file owns control-plane blob and log storage, removal of the SkyPilot EFS
claim, and storage-specific migration and recovery. The live role split,
PostgreSQL request delivery, controller leadership, and execution fencing stay
owned by `docs/designs/multi-replica-api-server.md`.

## Decision

The steady state has one storage path:

- PostgreSQL is the sole structured and transactional authority.
- A private, versioned, server-owned S3 bucket stores immutable blob and log
  bytes with SSE-KMS.
- Kubernetes Secrets and projected workload identity provide deployment
  credentials.
- Bounded `emptyDir` volumes hold upload parts, materializations, generated
  files, and log spool that can be recreated or reported lost.
- No SkyPilot PVC, EFS mount, shared path, FUSE mount, or filesystem fallback
  remains.

The explicit single-process local-development backend remains available outside
guarded HA. It is not a production fallback and cannot be selected by the
committed `S3_V1` generation.

PostgreSQL is not a bulk byte store. S3 is not a database. A reader obtains an
exact bucket, key, version ID, byte count, and digest from a committed
PostgreSQL row; it never discovers authority by listing a prefix.

The bucket has disjoint `staging/` and `authoritative/` namespaces.
Runtime identities may delete only their own fenced `staging/` objects and may
abort only their own incomplete multipart uploads in either namespace. Bucket
policy denies `DeleteObject` and `DeleteObjectVersion` in all authoritative
blob, legacy-tree, and log namespaces to every runtime, migration, Helm, and
staging-cleanup identity. Only the exact digest-pinned storage-GC Job identity
may delete an exact PostgreSQL-adjudicated object version after either the
restore-safe retained-data protocol or the never-committed prepared-object
abort protocol below. It cannot mutate bucket policy, lifecycle, versioning,
encryption, or the KMS key.

This design adds no database product, queue, cache, custom controller, CRD,
admission webhook, secret system, or always-on Deployment. One bounded
CronJob performs restore-safe exact-version GC through the same PostgreSQL
state machine; failure to run it affects quota headroom, not correctness or
provider actions. The design does not require Object Lock, a general object-
lifecycle framework, a configuration-generation subsystem, or a repository
for arbitrary file projections. It adds no DynamoDB table, external commit
oracle, or second restore-history authority: a historical PostgreSQL view is
never treated as proof that a later commit did not happen.

## Goals

- Remove the production guarded-HA control plane's EFS/PVC dependency without
  weakening request, Job, Serve, controller, log, or recovery durability.
- Give every durable byte one immutable S3 version and one authoritative
  PostgreSQL reference, owner, lifecycle, and fence.
- Keep every pod disposable and prove full role blackout recovery from
  PostgreSQL, S3, Secrets, and workload identity alone.
- Make the migration one way, rehearsed, bounded by an approved recovery-time
  objective, and fix-forward after the `S3_V1` commit.
- Delete only the exact production EFS resources after application cleanup and
  a no-read/no-write horizon.

## Non-goals

- This does not remove or redefine the generic chart's public
  `storage.enabled`, RWO/RWX, or user-supplied PVC behavior for single-process,
  non-HA, local-development, or unrelated installations. Their supported
  backend remains a separate generic chart concern.
- This does not put archives or logs in PostgreSQL, add an object-store
  filesystem/FUSE layer, or retain EFS as a rollback or compatibility path.
- Logical retirement alone does not release permanent byte, object, or row
  quota. The restore-safe GC protocol is the only path that converts an
  immutable retirement decision into exact-version absence and then releases
  quota. S3 lifecycle never expires authoritative objects.
- This does not change the public upload wire format, add a new API version,
  add a broad Terragrunt refactor, or update a `boltz-platform` SkyPilot pin.

## Public contract and scope

Guarded production HA has one legal durable mode after cutover: `S3_V1` with
PostgreSQL authority, exact-version S3 bytes, and no SkyPilot PVC. The chart
must reject guarded HA if a PVC, EFS volume, filesystem fallback, or incomplete
storage fingerprint is also selected. The three production role Deployments
and every ReplicaSet/Pod they create carry the same immutable storage-protocol
and fingerprint annotations.

Post-cutover guarded HA admits only the split API, executor, and controller
roles. `--role=all` remains a non-HA/local compatibility surface, not a
production recovery path. Production recovery preserves the split topology and
fixes forward with an S3-capable image.

Outside guarded HA, existing generic `storage.*` values and local `BlobStorage`
remain source compatible. This initiative removes only the production-HA EFS
selection, its transition code, and the exact EFS infrastructure named below.
It does not silently reinterpret a generic user's PVC values. Any future
repo-wide removal of generic PVC support is a separate breaking design.

The application interface remains the existing upload, request, Job, Serve,
log, and download APIs. A caller never supplies an S3 key/version and never
receives bucket credentials. Authorization continues to be the authenticated
SkyPilot principal; object references are internal opaque metadata. Existing
API 41+ clients remain unchanged, and the bounded API 24--40 adapter below is
the sole compatibility path until those versions age out.

Application deployment ownership remains direct Helm: every SkyPilot image,
chart, value, hook, and role rollout is an immutable reviewed Helm bundle
applied to the existing release with `--reuse-values`. `boltz-platform` owns
only the minimum static S3/KMS/IAM and the RBAC/identity boundary that separates
fence ownership; it does not own the SkyPilot release, application values,
mutable image-digest allowlist, or a SkyPilot version pin. The
`skypilot-storage-fence` artifact is a second direct-Helm release from the same
reviewed SkyPilot bundle, applied by its separately authorized identity before
the ordinary release. Both mutations run as sequential phases under the same
PostgreSQL cutover lease and fencing token. Future image or storage-generation
rollouts update that fence release, roll the application, and tighten the fence
without a Terraform/Terragrunt or platform-pin change.

## Production baseline and gap

Read-only evidence captured on 2026-08-18 and revalidated after the 2026-08-19
direct Helm rollout shows:

- Helm revision 434 runs SkyPilot 1.1.1343 from commit
  `43124ca7645277493e7b27429301f4d121ff6985`, image digest
  `sha256:4130adcdefccbb0c10e3fe1b2750d85b5fe24b1db8a52d2f5cad81eb5fead4ee`.
- API, executor, and controller Deployments each have two Ready pods.
- All six pods mount one claim, `skypilot-state-rwx`, at `/root/.sky`,
  `/root/.ssh`, and `/root/sky_logs`.
- There is one SkyPilot PV/PVC/access point, with volume handle
  `efs:fs-00a7dd95ad52c0ade::fsap-027d9430f450bb777`.
- The CSI-created access-point root is
  `/dynamic_provisioning/pvc-8001cb94-a060-402c-be6a-7899d9dd972c`.
- The one source filesystem reports 23,823,790,080 bytes (approximately
  23.82 GB). There is no second authority root.
- PostgreSQL already owns requests, queue state, leases, controller ownership,
  and the operational cluster, Job, and Serve records.
- `BlobStorage` exists, API 41+ already uses content-addressed `blob_id`, and
  active request and managed-Job rows already retain `file_mounts_blob_id`.
- `LogProvider` abstracts request-log reads, but request, managed-Job, Serve,
  and ordinary-cluster provisioning/history writers and readers still include
  POSIX paths such as `global_user_state.provision_log_path`.

The target topology is deliberately small, but the implementation is a major
cross-domain migration. Blob and log transitions remain separate review units:

| Area | Exists | Required change |
| --- | --- | --- |
| Blob bytes | Path-shaped `BlobStorage` and EFS | S3 implementation plus PostgreSQL blob/upload metadata |
| Blob ownership | Request and Job blob IDs | Persist the blob ID on each Serve version and make all three lifetimes authoritative |
| Upload API | API 41+ `/upload_v2`; API 24--40 `/upload` | Keep 41+ wire behavior; add one server-side per-user adapter for 24--40 |
| Materialization | Durable extracted directories | Scoped verified extraction under bounded `emptyDir` |
| Logs | Path writers; partial read seam | One spool/ship/index/read path for request, Job, Serve, and ordinary-cluster logs |
| Cutover | EFS-selected Helm values | One PostgreSQL generation/fingerprint and a no-PVC chart mode |
| Infrastructure | One live EFS source | Reuse a suitable private S3 bucket/prefix or add the minimum S3/KMS/IAM resources, then surgically delete EFS |

## Required invariants

1. Exactly one committed storage generation is active: `EFS_V1` before
   cutover or `S3_V1` after it.
2. One canonical fingerprint binds generation, bucket ARN, region, deployment
   prefix, and KMS key ARN. Every role verifies it before becoming Ready.
3. PostgreSQL records an upload or log segment before its S3 write and records
   the exact verified version before making it readable. Every authoritative
   `PutObject` or multipart completion is conditional on nonexistence
   (`If-None-Match: *`). A bucket policy requires conditional creation for the
   authoritative namespaces while using `s3:ObjectCreationOperation` to exempt
   multipart part operations that cannot carry that precondition. Staging and
   authoritative keys are disjoint; a staging key can never become a durable
   reference.
4. S3 keys are unique and never overwritten. HTTP 409/412 from a conditional
   create is a fenced reconciliation outcome: the writer verifies the one
   expected attempt key/version/digest or quarantines it; it never invents a
   replacement key, creates another version, or treats the conflict as
   success. Readers use exact versions and digests. Prefix listing is
   never readable authority. Multipart recovery may enumerate the one exact
   attempt key, and a fenced restore must inventory all versions/MPUs under the
   authoritative roots; both use listing only to discover candidates for exact
   PostgreSQL verification, quarantine, or absence proof.
5. No PostgreSQL lock is held across an S3 call. Lost responses are reconciled
   from the domain row and exact key, never by blind replay.
6. No durable blob or log authority consists only of a pod-local path. A
   compatibility path hint may remain only when its PostgreSQL content/object
   reference is authoritative and the path is regenerated. Materialization
   and spool paths themselves are valid only inside their scoped owner.
7. EFS and S3 are never simultaneous authorities. Production does not dual
   write.
8. Missing log bytes are represented by an ordered PostgreSQL gap, or by the
   constrained domain-row permanent-quota marker when no new stream metadata
   row may be created. After the PostgreSQL stream/fence admission succeeds and
   the provider action begins, a log failure never replays or blocks that
   provider operation.
9. Unknown source files, unresolved credentials, missing owner references, or
   an incomplete restore proof block cutover.
10. After `S3_V1` commits, EFS rollback is forbidden; recovery is fix-forward
    on PostgreSQL and S3.
11. A blob reference, content-addressed reactivation, and logical retirement
    serialize on the same PostgreSQL blob row. No request, Job, or Serve
    version can attach while the row is retired. The same authenticated owner
    may atomically reactivate its retained, exact verified version before a new
    attachment; no upload or new S3 version is created. `S3_V1` retirement is
    never by itself physical deletion; only the restore-safe authorization and
    exact-version GC protocol below may remove the bytes later.
12. An upload, blob publication, or log publication can have only one current
    attempt generation and lease. A stale pod cannot publish after takeover.
13. Staging cleanup can abort a fenced incomplete multipart upload in either
    namespace and delete object versions only in `staging/`. No application,
    migration, Helm, or staging-cleanup identity can delete any object version
    in `authoritative/`, including one that never became visible in PostgreSQL.
    The separately fenced storage-GC identity can delete only an exact version
    whose durable domain row has reached `DELETE_ELIGIBLE`, or a never-committed
    prepared version whose adjudication row has reached
    `ABANDONED_PREPARED`.
14. Permanent PostgreSQL quota reservations bound authoritative blob bytes and
    object count per authenticated owner and deployment, and log bytes and
    stream/segment count per durable owner/domain and deployment. They also
    bound retained blob/root/child/upload and log stream/segment/gap metadata
    rows. Outstanding upper-
    bound reservations and every object version in the authoritative namespace
    in `PUBLISHING`,
    `PREPARED_PUBLISHED`, `READY`, `RETIRED_RETAINED`, `COMMITTED`,
    `QUARANTINED`, `DELETE_AUTHORIZED`, `DELETE_ELIGIBLE`,
    `ABANDONED_PREPARED`, `RESTORE_QUARANTINED`, `DELETING`, or immutable-
    orphan state count; retirement, abort, or restore invalidation cannot create
    quota headroom. `DELETED` metadata remains row-charged until its separate
    compaction horizon.
15. Physical GC is serialized with reference insertion and reactivation. The
    irreversible `DELETE_AUTHORIZED` transaction records the exact object
    versions and permanently prevents new references, but grants no S3-delete
    permission. A later `DELETE_ELIGIBLE` transaction proves the supported
    ancestry-aware restore universe excludes every point that can omit the
    tombstone and every promotable backup/PITR chain contains its exact
    transaction. Only then may physical deletion begin.
    Quota is released only after exact-version absence is read back. A stale or
    lost GC response cannot delete a reactivated object, reuse a key, or turn
    absence into success without the matching eligibility row.
16. Every S3 version published for an uncommitted storage generation has a
    durable preparation row, attempt generation, source-manifest digest, exact
    version reference, and quota charge. Only the uninterrupted current
    PostgreSQL timeline, after lost-commit reconciliation against its current
    singleton, may prove a live pre-commit abort and CAS it to
    `ABANDONED_PREPARED`. A restored historical view never supplies that
    negative proof. No future preparation may adopt its key or bytes, and only
    exact-version absence releases its charge.
17. One PostgreSQL cutover lease and fencing token serialize every storage-
    fence mutation with every guarded-HA application Helm mutation. Each fence
    mutation performs a fresh committed-mode/generation check under that lease;
    neither an earlier observation nor Kubernetes policy self-validation is a
    substitute.
18. A restored PostgreSQL instance remains under its restore-promotion writer
    fence while an initial transaction durably invalidates every restored
    nonterminal S3 mutation and a complete all-root version/MPU inventory
    classifies all possible authoritative versions, including keys created
    later than the selected database snapshot. Present, recorded, or possibly
    published versions become charged, unreadable, non-reusable
    `RESTORE_QUARANTINED`
    records, never `ABANDONED_PREPARED`. Only an attempt with no recorded exact
    version, no completed version under its immutable key, and no incomplete
    multipart upload may become `RESTORE_UNPUBLISHED_ABSENT`; any ambiguity keeps
    the fence closed. No role may become Ready and no write/lease/commit
    function may resume while the gate is `RESTORE_FENCED`.
19. The restore-promotion ID and gate cover storage-generation preparation,
    ordinary blob/upload parent and child publication, materialization leases,
    legacy manifest/content construction, and log stream writer epochs/segment
    preparation. Every domain mutation function checks the current open gate,
    promotion ID, and its existing domain fence. The restore transaction records
    a new random promotion ID; all post-restore attempts, writer epochs,
    materializations, multipart IDs, lifecycle epochs, and S3 keys are fresh.
20. `RESTORE_QUARANTINED` can reach physical deletion only through
    `DELETE_AUTHORIZED -> DELETE_ELIGIBLE` and the persisted
    `deletion_origin = DELETE_ELIGIBLE` path. Eligibility additionally proves
    both that the current-timeline tombstone is in every promotable chain and
    that every later point on the superseded source lineage or descendant that
    could contain a commit has expired or is explicitly non-promotable. A newer
    valid backup therefore protects every version it may reference. Restore-
    quarantined bytes never enter the fast `ABANDONED_PREPARED` GC path.

## Storage generation and fingerprint

A singleton PostgreSQL row stores:

- monotonic generation, mode, and minimum reader/writer protocol;
- the canonical S3 fingerprint, including the disjoint staging and
  authoritative namespace roots, bucket-policy digest, public-access and
  object-ownership settings, retention/restore-horizon policy, GC identity,
  and permanent quota policy digest;
- prepared and committed timestamps;
- the source PV/PVC/access-point identity and final manifest digest;
- the final migration counts and PostgreSQL snapshot receipt;
- the backup, isolated-restore, Helm release, and operator receipts; and
- the current restore-promotion owner, random fencing ID, database-clock lease
  expiry, writer gate (`RESTORE_FENCED` or `WRITERS_OPEN`), and invalidation/
  final receipt; and
- the current cutover-lease owner, random fencing token, expiry, operation
  phase, and last reconciled fence/application receipt.

There may be one higher prepared generation. Prepared blob/log rows are
invisible to ordinary readers. A serializable transaction validates the final
receipts, including the current `SEALED` admission-policy UID/resourceVersion/
digest and negative probes, and changes the singleton from `EFS_V1` to `S3_V1`.
The committed mode cannot move backwards. A transition-capable binary may run
the exact audited `EFS_V1` workload while PostgreSQL still reports `EFS_V1`.
Only a workload rendered and annotated to use `S3_V1` (or an S3 N/N+1 pair)
refuses readiness and every durable write while PostgreSQL reports `EFS_V1`;
having S3-capable code in the transition image is not activation.

Every prepared generation first receives a
`control_plane_storage_preparation_attempts` parent row containing its random
attempt ID, creating restore-promotion ID, immutable source-manifest digest,
state, and optional `invalidated_by_promotion_id`; its state advances from
`PREPARED` to exactly one of `COMMITTED`, live-timeline `ABANDONED`, or restore-
only `RESTORE_INVALIDATED`. Every shadow object then receives a
`control_plane_storage_prepared_objects` child row containing that attempt,
unique random key, expected digest/bytes, quota reservation, and state.
Publication records the exact S3 version before the child becomes
`PREPARED_PUBLISHED`; ordinary readers cannot resolve that state. The storage-
generation commit consumes the one noninvalidated attempt's exact prepared set
into its domain rows and marks the parent `COMMITTED` atomically. Every prepare,
lease, publish, and generation-commit function rejects an attempt created under
a different restore-promotion ID or carrying an invalidation receipt. If the
commit response is lost, the current-timeline operator reconciles the singleton
before classifying any object.

A proven pre-commit abort on the uninterrupted current database timeline
classifies every unconsumed row for that attempt.
Before the classification transaction, every lost publish response and
`PREPARING`/`PUBLISHING` row is reconciled against its one expected key: an
incomplete multipart upload is fenced-aborted and proven absent, an existing
object is fully verified and records its exact version, and an ambiguous result
blocks abort completion and quota release. The transaction CASes a published
version to `ABANDONED_PREPARED`, records the database-clock abort receipt and
exact version/digest/bytes, and permanently prevents adoption. A proven-absent
row becomes `ABORTED_ABSENT` and may release only its unused reservation. The
parent becomes `ABANDONED` only after all of its children have one of those
durable classifications. A changed source manifest or retry creates a new
preparation attempt with new random keys; it never overwrites, reuses, or
silently extends an abandoned set.
A retry of the same abort returns the same durable classifications and receipt;
a later preparation abort remains disjoint. All attempts are bounded by the
same prepared-object byte/object/row quotas. Their exact-version cleanup is
defined below and remains legal while the committed mode is `EFS_V1`. None of
these live-abort classifications may be inferred from a restored snapshot.

This is the only new storage/configuration generation. Bucket, prefix, KMS, and
mode are not independently mutable Helm switches.

After the initial cutover, a higher `S3_V1` generation may change only bounded
protocol, quota, retention, and sizing policy; moving bytes to another bucket,
prefix, region, or KMS key requires a separate migration design. The ordinary
update protocol prepares generation N+1, deploys a mixed-generation binary
whose immutable annotation names both current N and prepared N+1, proves every
2/2/2 role can read N and is capable of N+1 while still writing only N, then
commits N+1 in PostgreSQL. Roles switch authority only from that commit and a
follow-up direct-Helm rollout removes N from the annotation. The admission fence
allows only that exact N/N+1 S3 pair; it never re-admits EFS. Before commit the
prepared generation can be abandoned only through the typed prepared-object
protocol above without changing N. After commit recovery is fix-forward on
N+1. A shorter retention value cannot authorize immediate deletion: it starts
a fresh full horizon and ancestry-aware restore-coverage proof under N+1.

The fingerprint names four nonoverlapping roots: disposable upload staging,
authoritative client archives, authoritative legacy manifests/content, and
authoritative log segments. Bucket policy requires TLS, the exact SSE-KMS key,
and conditional create on every authoritative root; denies overwrite, unscoped
copy, `DeleteObject`, and `DeleteObjectVersion` there to every identity except
the exact storage-GC principal; and permits ordinary object deletion only under
staging to the owning fenced identity. The GC principal receives
`DeleteObjectVersion` but not `DeleteObject`, so it cannot create a delete
marker or make a later `If-None-Match: *` create appear safe.
`AbortMultipartUpload` is permitted for the owning fenced incomplete upload in
either root because that action cannot remove an object version. The bounded
restore identity may list and read/HEAD versions and MPUs under the exact roots
and, only after source-writer quiescence, abort source-writer MPUs; it cannot
put/copy bytes or delete any completed version. Lifecycle may abort stale
multipart uploads in either root and expire staging versions after their
maximum reconciler horizon. It has no current- or noncurrent-version expiration
rule for any authoritative root. Runtime, migration, restore, cleanup, and Helm
identities
also lack bucket-policy, lifecycle, encryption, and versioning mutation; the
bucket must read back `Versioning=Enabled`, not suspended, at readiness and
restore promotion. Policy simulation and live positive/negative probes use
each identity independently; identity policy cannot weaken the bucket-policy
deny.

The bucket is private by contract, not by convention: S3 Object Ownership is
`BucketOwnerEnforced`, all four Public Access Block settings are true, ACL APIs
are denied, and the bucket and KMS key are destroy-protected. The KMS key is
enabled with rotation; its grants restrict use through regional S3 and the
exact bucket object-ARN encryption context. It has an independently alerted
deletion/disable boundary, and runtime, migration, restore, Helm, and GC
identities cannot disable or schedule deletion of it. A changed ownership,
public-access, versioning, bucket-policy, lifecycle, encryption, or key state
fails readiness and restore promotion.

Every production database restore uses the existing PostgreSQL backup/PITR and
S3 versioning under one conservative protocol. A restored database starts
unreachable to every SkyPilot role credential. Before it can expose that
database, the bounded restore owner acquires the singleton's database-clock
promotion lease and random fencing ID; the same lease owns old-writer
quiescence, restore classification, and later database endpoint/credential
exposure. The owner fences the superseded database endpoint and every old API,
executor, controller, migration, log-writer, and GC workload, waits through
their maximum lease and S3-request deadline, and records the selected restore
point, source timeline, and closed source-writer cutoff. If that cutoff or
quiescence is unprovable, promotion remains closed.

The first serializable transaction under that token sets `RESTORE_FENCED` and
CASes every restored nonterminal storage-generation parent/child, blob
lifecycle/upload row including its persisted part and destination-publication
state, legacy import parent/child, materialization lease, log writer epoch/open
marker, and segment preparation to its domain's `RESTORE_INVALIDATED` state with
`invalidated_by_promotion_id`; the read-only materialization lease uses the
equivalent `RESTORE_INVALIDATED_LOCAL`. It removes each from commit, publish,
adoption,
materialization, and writer-takeover eligibility. A terminal state in the
selected snapshot is not by itself readable authority: only committed domain
rows survive after their exact versions verify. Deletion, abandonment, and
invalidation terminals remain tombstones. Invalidation is
unconditional: a historical snapshot cannot prove whether any nonterminal
attempt committed in a later backup on the superseded source timeline, so it
may never classify one as ordinary `ABANDONED_PREPARED`.

`control_plane_storage_restore_quarantine` is the PostgreSQL domain for that
uncertainty, not a new service or byte store. Each row binds the source mutation
family, owner/domain, original parent and child IDs, original promotion/attempt/
writer/lifecycle epochs, immutable key, recorded or discovered exact version
ID, digest and byte count, quota charge, selected restore point/source timeline,
closed source-writer cutoff, later-backup inventory digest, classifying
promotion ID, and presence observation. It is unreadable, cannot receive a
reference, cannot be reactivated or adopted, and its key/version can never be
reused.

While the same promotion fence remains held and all source writers remain
quiescent, the restore owner takes a complete paginated inventory of **all**
current and noncurrent versions and incomplete multipart uploads under every
authoritative client-archive, legacy-manifest/content, and log root. It does
not limit discovery to keys named by the historical database. It aborts every
fenced source-writer multipart upload, inventories all roots again, and requires
two complete identical post-quiescence inventory digests before classification.
Any failed page, continuation-token discontinuity, unabortable upload, late
version, or changed digest keeps `RESTORE_FENCED`. Staging-only versions use
their existing exact-key cleanup and never become authority.

The owner then joins the complete inventory to the selected snapshot's
committed domain references, its invalidated mutation rows, and immutable
object metadata. A selected-snapshot committed reference survives only after
its exact version, owner/domain, promotion/attempt or writer identity,
digest/size, and KMS identity verify. Every recorded exact version, every
verified version present for an invalidated mutation, and every version that
can match a lost publish response is transferred atomically with its quota
charge into `RESTORE_QUARANTINED`, even if a recorded exact version is now
absent. A later source-timeline version whose mutation row did not yet exist at
the selected restore point is still restore-quarantined when its immutable
metadata exactly identifies a SkyPilot family, owner/domain, source promotion,
attempt/writer epoch, digest, and size. The restored database view is never
used as proof that such a later key was unpublished.

A version matched by a selected-snapshot `DELETE_AUTHORIZED`,
`DELETE_ELIGIBLE`, `DELETING`, `DELETED`, or `ABANDONED_PREPARED` row remains
bound to that no-reactivation tombstone, not to readable authority or a new
quarantine row. Promotion reconciles its presence and charge. Retained-data
eligibility/deleting receipts are revalidated against the current restore
universe before new delete work; a durably selected-snapshot
`ABANDONED_PREPARED` receipt may resume only its exact fast cleanup. This
terminal reconciliation and the committed-row verification above are included
in the all-root classification digest.

Only a restored mutation child with no recorded exact version may become
`RESTORE_UNPUBLISHED_ABSENT`, and only after the complete inventories prove its
exact key has no current or noncurrent version and no incomplete multipart
upload. That state releases only unused reservation. A discovered version that
has a valid owner/domain and storage envelope but no domain or mutation binding
becomes a typed immutable-orphan row and remains unreadable and charged through
the full restore-safe protocol. Missing or contradictory metadata, ambiguous
ownership/family, an unverifiable digest/size/KMS identity, multiple candidate
bindings, a lost list/HEAD/GET response, or any unaccounted version blocks
promotion; the operator never chooses a candidate or turns uncertainty into
absence. Listing is discovery only and never makes bytes readable.

The final promotion transaction confirms every readable authoritative exact S3
version, reconciles required quota charges and exact absence observations,
proves every invalidated mutation is linked only to
`RESTORE_QUARANTINED` or `RESTORE_UNPUBLISHED_ABSENT`, records the complete
cross-family invalidation/classification and all-root version/MPU inventory
digest, and changes the writer gate to `WRITERS_OPEN`. Only that receipt lets
the same fenced owner
expose the restored database endpoint/credentials and lets any API, executor,
or controller become Ready. Every domain's write, lease, publish,
materialization, writer-takeover, commit, and GC claim/progress function checks
the current promotion ID and open gate. New work creates fresh random domain
IDs, epochs,
multipart IDs, and keys under that promotion ID; a restored ID, key, epoch, or
sequence value is never reused. Storage generations are promotion-scoped, so a
restored prepared N+1 cannot collide with an N+1 that committed only on the
abandoned lineage. Missing referenced versions, any unaccounted object or MPU,
or incomplete invalidation blocks promotion. Lease loss leaves
`RESTORE_FENCED`; a successor rotates the fencing ID, adopts the durable
invalidations/quarantine, and completes the same reconciliation before it can
open or expose the database. This is an operator restore step, not a permanent
coverage worker, commit oracle, or new service.

### Restore-promotion coverage by mutation family

The promotion fence is one shared PostgreSQL contract, not a family-specific
recovery loop. The existing parent/child rows below add the same
`restore_promotion_id`; every authoritative S3 create also carries it in object
metadata with the domain's existing attempt/writer identifiers and digest.
Schema-owned mutation functions require that ID to equal the singleton and
require `WRITERS_OPEN`, except for the restore owner's narrowly scoped
invalidation/quarantine functions while `RESTORE_FENCED`.

| Mutation family | Durable parent/child fence | Restore classification | Fresh post-restore work |
| --- | --- | --- | --- |
| Storage-generation shadow/import | `control_plane_storage_preparation_attempts` with `mutation_family = STORAGE_GENERATION`; every `control_plane_storage_prepared_objects` child carries the same promotion and attempt IDs | Parent becomes `RESTORE_INVALIDATED`; every present, recorded, or possibly published child version becomes one `RESTORE_QUARANTINED` row; only an exact no-version/no-multipart child becomes `RESTORE_UNPUBLISHED_ABSENT` | New preparation parent, attempt ID, source-manifest binding, multipart IDs, and staging/authoritative keys |
| Ordinary client blob/archive upload | Nonterminal `file_mount_blobs` lifecycle epoch plus `file_mount_uploads`; the upload's persisted part and destination-publication state carries promotion ID, upload ID, lifecycle epoch, and lease | `PREPARING`/`UPLOADING`/`VERIFYING`/`PUBLISHING` parent and publication state is invalidated; staging is exactly cleaned; every possible authoritative destination version is restore-quarantined, never attached to the blob | New lifecycle epoch, upload/lease/multipart IDs, and staging/authoritative keys; the old epoch moves to immutable quarantine provenance, stays separately charged, and cannot satisfy content-addressed reuse |
| `LEGACY_TREE_V1` manifest/content construction | The importer uses `control_plane_storage_preparation_attempts` with `mutation_family = LEGACY_TREE_V1`; every manifest/content preparation child carries that promotion/attempt ID before the atomic root/child commit | An incomplete graph is invalidated as one attempt; every possible manifest or content version is individually restore-quarantined and linked to that graph; no partial graph becomes readable | A full source re-inventory creates a new attempt and fresh manifest/content keys; no restored child or manifest is adopted |
| Blob materialization | `control_plane_storage_materializations` stores promotion ID, random materialization ID, blob lifecycle epoch, lease token/expiry, pod scope, and disposable path digest; it has no authoritative S3 writer | The lease becomes `RESTORE_INVALIDATED_LOCAL`; the old `emptyDir` path is never reused and creates no quarantine row; promotion still waits for old-pod quiescence so the stale lease cannot block reference/retirement state | A fresh materialization ID, lease, and private path reads only the selected snapshot's committed exact version |
| Log stream/segment publication | `control_plane_log_streams` stores promotion ID on every writer epoch/open-coverage marker; every `control_plane_log_segments` preparation carries promotion ID, writer epoch, random segment ID/key, interval, and lease | A nonterminal writer epoch and its `PREPARING`/`PUBLISHING` segments are invalidated; every possible segment version is restore-quarantined; the selected snapshot's last contiguous committed offset closes with an exact gap or `UNKNOWN_TAIL`, and no provider effect is replayed | A fresh writer epoch and segment IDs/keys resume only under the current promotion ID after the gap boundary; a previously reserved sequence/interval or old segment is never reused |
| Exact-version GC deletion | Every deletion claim/progress row carries promotion ID, deletion generation, immutable origin/provenance, lease, and exact version set | The source GC workload and request horizon quiesce before inventory; retained deletion/eligibility receipts stay tombstones and are revalidated, selected-snapshot `ABANDONED_PREPARED` may resume only exact cleanup, and no delete runs while `RESTORE_FENCED` | A new claim uses the current promotion ID/open gate; resume preserves the exact origin/provenance/version tuple and never substitutes a key or version |

Committed blob roots/legacy graphs/log segments already present in the selected
snapshot remain readable authority after exact-version verification. For the
creation families, the table governs only nonterminal mutation state; the GC
row preserves and revalidates existing tombstones. A family cannot substitute
its ordinary takeover/retry path for restore classification, because that path
has no proof about commits recorded only in a newer source-timeline backup.

## Blob and upload contract

The existing `BlobStorage` seam becomes object-aware rather than adding a
parallel repository. Its durable domain tables are:

- `file_mount_blobs`: authenticated owner, `blob_id`, generation, exact S3
  reference, byte count, archive and logical-tree digests, upload state,
  representation kind, lifecycle epoch, creating restore-promotion ID,
  retention deadline, and reference/retirement state; and
- `file_mount_uploads`: authenticated owner, upload identity, part count,
  restore-promotion ID, staging and authoritative keys derived from that fresh
  random upload identity, attempt generation, lease token/expiry, received-part
  checksums/ETags, multipart IDs, expiry, and final blob ID.

`file_mount_blob_objects` is an import-only child table for
`LEGACY_TREE_V1`. It records every content object by stable ordinal, kind,
logical-entry digest, exact authoritative S3 version, byte count, and content
digest, plus its creating restore-promotion/import-attempt IDs. The root blob
row records the exact manifest version/digest, object count, total bytes, and
tree digest. The complete root-plus-child set is committed atomically; neither
a prefix listing nor an S3 manifest alone is structured authority.

`control_plane_storage_quotas` stores each fingerprinted
`(scope_kind, scope_id, storage_kind)` limit plus used/reserved byte, object,
and retained-metadata-row counters.
`control_plane_storage_quota_reservations` names its upload or log attempt,
byte/object upper bound, owner/domain and deployment scopes, attempt generation,
state, and expiry. The domain-row transition and counter/reservation transition
occur in one PostgreSQL transaction. Committing an authoritative version
converts the reservation to permanent usage; aborting an attempt releases only
unused or staging-only reservation. A published prepared-generation version is
not unused: a current-timeline abort remains charged through
`ABANDONED_PREPARED`, while restore-invalidated or possibly published work
remains charged through `RESTORE_QUARANTINED`; only exact-version absence after
the applicable deletion protocol releases either charge. Reconciliation is
idempotent by attempt generation, so concurrent API pods cannot oversubscribe a
limit.
Staging has separate owner/deployment byte and object quotas. A staging charge
is released only after exact-key abort/delete and absence reconciliation; an
expiry timestamp by itself never frees it.

Restore invalidation never overwrites a nonterminal blob epoch in place with a
new attempt. It atomically copies that epoch's upload/part/destination identity,
quota, and any exact version into immutable restore-quarantine provenance and
marks the old epoch invalid. The logical `(owner, blob_id)` root may then admit
one fresh lifecycle epoch with a new promotion/upload identity and physical
keys while the old epoch remains separately charged. Schema constraints prevent
the fresh root from resolving, attaching, reactivating, or releasing quota from
the quarantined epoch; if that transfer is incomplete, admission for the same
logical blob remains blocked.

The blob identity is the composite `(authenticated_owner_id, blob_id)`, and
that composite reference is copied through request, Job, and immutable Serve
version rows. Equal content from different owners does not collapse their
authorization boundary. `created_by`, display names, and request submitter text
are not storage-owner identities.

An upload advances `PREPARING -> UPLOADING -> VERIFYING -> PUBLISHING ->
READY`. The server creates `PREPARING`, its unique staging and authoritative
keys, permanent quota reservation, attempt generation, and current restore-
promotion ID in the same transaction before S3 I/O. Every upload lease and CAS
requires that promotion ID plus `WRITERS_OPEN`. A leased owner creates or
discovers the one multipart upload for that exact staging key and persists the
returned multipart ID before uploading a part. A
lost create response is reconciled with `ListMultipartUploads` only for the
persisted unique staging key. Exactly one upload whose initiation belongs to
that fenced attempt may be adopted; zero remains absent, and multiple or
unprovable results abort or quarantine the attempt rather than choosing one.
It is never handled by blindly creating a second upload. Each received HTTP
chunk is bounded-spooled and checksummed, and its intended part number/checksum
commits before S3 I/O. `ListParts` reconciles a lost part response before the
row records its ETag.

Finalization leases and CASes the row to `VERIFYING`, then completes the
unique staging multipart upload with its persisted ordered part set. A lost
completion response is reconciled by `HEAD` of that exact staging key,
including version ID, attempt metadata, size, digest, and SSE-KMS identity.
Absence resumes or aborts only the recorded multipart upload; presence never
triggers a second staging object. A present object that cannot be proved to
belong to the same attempt is quarantined. Only the same attempt generation can
continue verification. The server streams the exact staging version,
recomputes the archive digest and logical-tree digest, rejects
traversal, links or special types outside the contract, expanded-byte and
inode-limit violations, and requires the logical-tree digest to equal the
client `blob_id`. `HEAD` metadata alone never proves integrity.

After verification, the same lease CASes to `PUBLISHING`. A bounded object is
streamed from an exact-version `GET` into conditional `PutObject`; a larger
object uses a persisted destination multipart ID, range-bounded
`UploadPartCopy` from that exact staging version, and conditional
`CompleteMultipartUpload`. Thus the unique authoritative blob key's final
create is always conditional on nonexistence; bare `CopyObject` is not an
allowed publisher. Destination multipart initiation uses the same exact-key,
attempt-metadata, persist-before-part, and ambiguous-multiple quarantine rules
as staging; a fenced incomplete destination upload may be aborted, but no
authoritative object version may be deleted. A lost response or 409/412 is
reconciled only against that exact key, version, attempt metadata, size, digest,
and KMS identity; a mismatch quarantines the attempt. The server streams and
verifies the returned
authoritative version before one PostgreSQL transaction records it, converts
the permanent quota reservation to usage, verifies the still-current restore-
promotion ID/open gate, and CASes the blob to `READY`. Only then may cleanup
delete the staging version. Lease expiry lets another API pod reconcile this
same state machine only within the same promotion ID, so chunks and finalization
may reach different API replicas without creating competing authority. A
restore never invokes this ordinary takeover path for a nonterminal upload; it
uses the invalidation/quarantine contract in the family table above.

The upload admission policy has finite, fingerprinted deployment values for
all of these dimensions; a production `S3_V1` fingerprint is invalid if any is
missing, nonpositive, or exceeds the S3 multipart limit:

- maximum HTTP request body and maximum spooled bytes per chunk;
- maximum parts (never above 10,000) and maximum aggregate archive bytes;
- maximum archive members, relative-path depth, expanded bytes, and expanded
  inodes;
- maximum active upload sessions per authenticated owner and per API pod; and
- maximum staging bytes and object/multipart count per authenticated owner and
  deployment, including expired attempts until cleanup proves absence;
- maximum distinct authenticated storage owners and durable log-domain scopes
  per deployment;
- maximum simultaneous materializations per owner, per request, and per pod,
  plus aggregate materialized bytes/inodes per pod;
- maximum authoritative blob bytes and object count per owner and deployment,
  plus retained root/child/upload rows, counting outstanding upper-bound
  reservations and all retained versions; and
- maximum authoritative log bytes, stream/segment count, and retained
  stream/segment/gap rows per durable owner/domain and deployment, with
  separate system-domain reserves so one tenant cannot consume provider-
  control log capacity.

Gate 2 records the measured numeric values and headroom; tests bind those exact
values into the storage fingerprint. Crossing a byte/member/depth/expansion
limit fails with 413 and aborts the fenced upload; malformed paths/checksums
fail with 400; a competing owner/session/attempt fails with 409; and a
concurrency or spool admission ceiling fails with 429 and `Retry-After` before
accepting bytes. No limit silently truncates a blob. API 24--40 callers need no
new checksum or session token: the server computes every part checksum and
binds the one permitted legacy slot to the authenticated principal and server-
generated attempt. Two archives racing for one `(owner, blob_id)` may converge
only when both exact bytes/digests verify; otherwise the loser is quarantined
and the request fails 409.

Permanent quota is checked and reserved before accepting the first body byte or
opening a new provider action's log stream. A blob whose declared upper bound
cannot fit fails with 413; owner/deployment concurrency or aggregate upload
budget exhaustion fails with 429 and `Retry-After`. Quota values cannot be
reduced below committed plus reserved usage, and increasing them changes the
fingerprint through a higher prepared/committed `S3_V1` generation followed by
the reviewed Helm rollout; Helm cannot mutate them independently. Log quota
never blocks a provider effect: when no permanent log reservation fits, the
PostgreSQL admission transaction marks a metadata-only stream
`PERMANENT_QUOTA_TRUNCATING` from
offset zero and the provider action proceeds. At terminal, the drained byte
counter closes the exact truncation interval; hard loss produces an
`UNKNOWN_TAIL` with permanent-quota reason instead of inventing the end. For an
already-admitted action, exhausting its reservation enters the same state at
the proven offset; neither case cancels or replays the action.

If even the permanent stream/gap row budget is exhausted, the same provider-
admission transaction sets the existing durable action row's constrained
`log_storage_state=PERMANENT_QUOTA_UNAVAILABLE` before the effect starts and
creates no stream row. Readers synthesize one typed unknown-tail frame at
offset zero from that server-owned domain field; they never report an empty
complete stream. This consumes no new storage-domain metadata and bounds the
degraded state by the owning domain's existing retention policy.

Quota transactions lock deployment scope before owner/domain scope and sort
same-kind rows by stable identifier. Commit charges exact published bytes/
objects and releases the unused upper-bound difference atomically. Expiry alone
never releases a reservation whose authoritative create may have occurred; its
attempt reconciler must prove staging-only absence or charge the possible
authoritative version first. A staging reservation likewise remains charged
until abort/delete plus exact-key absence is proven.

API 41+ preserves the existing client `blob_id` algorithm, archive canonical
form, and supported safe-symlink semantics exactly. Server verification uses
golden archives from current clients; it rejects escapes and unsupported file
types without redefining the digest or silently changing a valid archive.

API 41+ keeps its existing wire contract unchanged: `GET /upload_v2/blob`,
`POST /upload_v2`, the content-addressed `blob_id`, and the subsequent request
payload all remain compatible. There is no API 91 and no separate API 41--90
adapter. The existence check preserves its retention side effect: it locks the
owner's blob row. A `READY` row receives a bounded attachment-grace extension.
A `RETIRED_RETAINED` row with the same owner and `blob_id` is content-addressed
reuse, not a new object: the same transaction verifies that its exact
authoritative version and permanent quota usage remain recorded, CASes it back
to `READY` with a new lifecycle epoch and bounded grace, and returns true. It
does not call S3 or create a version. `QUARANTINED`,
`RESTORE_QUARANTINED`, missing-metadata, wrong-owner, or non-retained rows never
reactivate. Request admission locks the same row and
attaches its current lifecycle epoch only while it is `READY`; attaching the
same owner's identical retained bytes after another caller reactivated them is
safe. A stale true response presented while the row is retired still fails.
Thus retirement and reference insertion remain serialized without making an
immutable content address permanently unusable or changing the wire contract.

API 24--40 keeps `/upload`. Because those clients cannot bind a blob ID in the
next request, PostgreSQL permits one completed, unclaimed legacy upload per
authenticated user. The next request from that user with a nonempty
`file_mounts_mapping` claims it, injects its blob ID, and persists the request
reference in the same request-admission transaction. A second concurrent
upload, missing/expired slot, ambiguous retry, unauthenticated remote caller,
or cross-user claim fails closed. The adapter remains until
`MIN_COMPATIBLE_API_VERSION > 40`. The explicit single-process local backend
keeps its local upload behavior outside guarded HA; it does not create an
anonymous production slot. If final verification computes the same owner/blob
identity as a `RETIRED_RETAINED` row, it verifies against and reactivates that
exact authoritative version, deletes only its staging copy, and creates no new
authoritative version. This is the only legacy adapter.

The EFS importer does not pretend an extracted directory is the original
client archive. Duplicate archive members, directory entries, ordering, and
overlapping paths can be lost during extraction, so byte-for-byte archive
reconstruction is not generally possible. Migration therefore supports two
explicit representations:

- `CLIENT_ARCHIVE_V1` is accepted only when the original archive is present
  and its archive and logical-tree digests reproduce the existing client
  `blob_id`; and
- `LEGACY_TREE_V1` stores a deterministic safe manifest plus content objects
  for the observed extracted tree. Its root row retains the grandfathered
  `blob_id`, exact authoritative manifest version/digest, tree digest, object
  count/bytes, representation kind, and importer version. Each content object
  has one exact authoritative version in `file_mount_blob_objects`; the
  manifest contains only canonical logical entries and stable content IDs, and
  its file entries must match those child rows one for one. It never claims an
  archive digest or passes through the ordinary upload finalizer.

The versioned manifest encoding is deterministic and safe: it carries only the
schema version and sorted entries containing a canonical relative-path byte
encoding, supported kind (`directory`, `file`, or already-supported safe
symlink), normalized permission bits, size, content ID/digest for files, and
validated relative target for symlinks. It excludes timestamps, owners, device
IDs, S3 keys, version IDs, and arbitrary extension fields. Absolute/parent/NUL
paths, duplicate normalized or case-colliding paths, unsupported modes/types,
and references to an absent child row are invalid.

`LEGACY_TREE_V1` is import-only and read-only. New uploads always create
`CLIENT_ARCHIVE_V1`; no runtime fallback reads EFS. A source tree with a
symlink/special file outside the existing safe contract, case/path collision,
unstable second inventory, or digest mismatch blocks cutover.

The importer creates one `control_plane_storage_preparation_attempts` parent
with `mutation_family = LEGACY_TREE_V1`, current restore-promotion ID, random
attempt ID, and source-manifest digest before staging any content. Every legacy
content/manifest preparation and S3 metadata binds those same IDs. The importer
stages and verifies every content object and the canonical manifest, publishes
each into its disjoint immutable authoritative namespace, then commits all exact
versions plus the root row and parent `COMMITTED` state in one PostgreSQL
transaction after rechecking the promotion ID/open gate. Materialization reads
that transactionally consistent graph,
verifies the manifest bytes/digest and its exact correspondence to child rows,
then verifies every content object's version, size, and digest before rebuilding
the tree. It never lists a prefix or treats manifest-provided bucket/key text as
authority. A missing, extra, duplicate, or mismatched child row blocks import
or materialization.

Owner backfill is evidence-driven. The importer takes the owner component from
the existing per-user API-server storage namespace, then requires every
request reference to have the same authenticated `api_requests.user_id` and
every managed-Job reference to have the same durable `job_info.user_hash`.
Display names and `version_specs.created_by` are never accepted. A Serve
version with a blob must be linked to one retained create/update API request
whose authenticated owner and `file_mounts_blob_id` match. If that unique join
is unavailable, the current owner must submit a successor version through the
normal authenticated API and retire the ambiguous version from every rollback-
eligible/active set before cutover. Historical ambiguous versions are marked
`LEGACY_UNRECOVERABLE` and cannot be elected or restored; they do not acquire a
guessed owner. Conflicting per-user directory evidence or more than one owner
for a blob is a blocking unknown, not an operator-editable mapping.

The request transaction continues to retain `file_mounts_blob_id`. Managed Job
registration continues to copy it into the existing Job row before the request
reference can expire. Serve creation/update persists the canonical
`file_mounts_mapping` and blob ID on the immutable Serve version before
releasing the request reference. It never persists the translated pod-local
absolute paths used by `core.up` or `core.update`. Recovery rematerializes the
blob and translates a short-lived copy of the task only inside the fenced
controller action.

Every request, Job, or Serve reference insertion locks the blob row and may
commit only while it is `READY`. Logical retirement locks the same row,
rechecks all three reference domains and the retention deadline, and CASes
`READY -> RETIRED_RETAINED`. New references then fail closed, but the exact S3
version, lifecycle epoch, permanent quota usage, and PostgreSQL row remain
restorable and eligible for same-owner content-addressed reactivation as
defined above until the restore-safe GC authorization transaction. S3
lifecycle never expires an authoritative current or noncurrent version.
Authoritative objects that never became visible in PostgreSQL remain immutable
orphans until the same restore-safe adjudication proves their exact origin and
eligibility; their permanent quota charge makes leakage bounded and
observable.

`control_plane_storage_materializations` is a durable lease table containing
the current restore-promotion ID, random materialization ID, blob lifecycle
epoch, lease token/expiry, pod scope, and disposable path digest. Its claim,
renew, and release functions require the current promotion ID/open gate and the
same committed blob lifecycle epoch; it has no S3 write authority.
`materialize(blob_ref)` is a context-managed capability over that lease. It
dispatches on the committed representation: it verifies and safely extracts
the exact `CLIENT_ARCHIVE_V1` version, or verifies the complete exact root/child
graph and rebuilds `LEGACY_TREE_V1`. It uses a private `emptyDir` directory and
returns that path only for one executor subprocess, controller action, or
synchronous API scope. Cancellation, fence loss, scope exit, or pod death
removes the path.
Retry rematerializes with a fresh materialization ID/path; a restore invalidates
the old lease and never adopts its path. No extracted tree is durable authority.

## Restore-safe retention and physical GC

Finite permanent quotas require a complete release path; merely retaining all
committed bytes forever would turn S3 into another eventually full filesystem
and would guarantee future log truncation. Physical deletion is therefore part
of the steady-state contract, but it is deliberately narrower than S3
lifecycle or a general object-management service.

The storage fingerprint defines finite positive retention periods for client
blobs, legacy trees, and each log domain, plus one
`restore_compatibility_horizon`. Every object retention period is at least that
horizon. Gate 2 sets the horizon no shorter than the maximum supported
PostgreSQL PITR window, retained automated/manual backup window, rollback
window, maximum writer/lease/reconciler lifetime, and measured clock/error
headroom. A backup outside the inventoried horizon is explicitly archival and
non-promotable unless its referenced S3 versions are still present; the restore
verifier always fails closed on a missing exact version.

`control_plane_storage_restore_coverage` stores monotonically advancing,
database-clock evidence of the complete PostgreSQL restore universe and the
latest successful isolated restore. The evidence pins the PostgreSQL system
identifier, full timeline-history/ancestor graph with fork points, every
retained base backup, continuous restorable WAL ranges on each lineage, and
every manual, copied, cross-region, and cross-account backup plus its durable
promotion-eligible or non-promotable state. It names provider-native IDs and
content digests but contains no credentials. A timestamp or numerically larger
LSN is never compared across divergent timelines as evidence.

Each deletion authorization records its exact PostgreSQL transaction identity,
system identifier, timeline, commit LSN, and tombstone digest. Coverage marks
that tombstone durable only when every still-promotable restore chain is proven
by ancestry and replay range to contain that exact transaction; every older or
divergent restore point that could omit it must first expire or receive a
durable non-promotable receipt. A chain on a descendant timeline qualifies only
when its recorded fork point is after the tombstone on an ancestor that
contains it. Thus a scalar oldest timestamp or LSN can summarize monitoring but
can never authorize deletion.

For each restore-quarantine batch the same domain binds the selected restore
point, PostgreSQL system identifier and source timeline ancestry, the closed
source-writer cutoff, the complete all-root S3 inventory digest, and the exact
backup/WAL inventory generation covering every point that could contain a
commit absent from the selected snapshot. The uncertainty interval is the
selected restore point exclusive through the proven source-writer cutoff
inclusive on that source lineage, including every promotable backup or
descendant chain that can reach a point in the interval. If the cutoff or the
lineage is not provable, the batch can never become deletion-eligible.

A batch gains `possible_commit_points_cleared` only when every point in that
uncertainty interval has expired or has a durable non-promotable receipt and
the current coverage inventory matches or dominates the batch's recorded
inventory generation. "Dominates" means the same system/timeline ancestry is
preserved, every previously recorded backup/WAL range remains present with the
same identity or a durable non-promotable receipt, and every newly discovered
chain is included. Any newer valid/promotable backup or PITR chain that
could contain the commit protects every exact version it may reference and
blocks clearance, whether or not operators currently intend to select it.
Merely inspecting the historical database, failing to find a row, comparing
timestamps/LSNs, or sampling one newer backup is not proof. Discovery of a
previously unlisted backup invalidates the receipt. An unhealthy, incomplete,
or stale coverage receipt stops new `DELETE_ELIGIBLE` transitions; it does not
prevent creation of a logical tombstone and never blocks ordinary reads,
writes, provider work, or cleanup already durably claimed from an eligible
generation.

`control_plane_storage_deletions.deletion_origin` is one exact persisted enum:
`DELETE_ELIGIBLE` or `ABANDONED_PREPARED`. It is not a caller-supplied label.
The schema-owned claim function initializes it atomically from the predecessor
row and it remains immutable through `DELETING`, `DELETED`, and the compact
receipt. Restore-quarantined data first completes the full authorization/
eligibility protocol, so its deletion predecessor and origin are exactly
`DELETE_ELIGIBLE`; `RESTORE_QUARANTINED` is never a third deletion-origin value.
An immutable nullable `restore_quarantine_id` separately preserves its source
family, batch, and version provenance and cannot be supplied by the caller.

The target chart renders one bounded `control-plane-storage-gc` CronJob. It
uses a dedicated ServiceAccount and workload identity, a digest-pinned SkyPilot
image, `concurrencyPolicy: Forbid`, a finite active deadline, and no public
endpoint. The separately owned admission fence validates its exact image,
command, arguments, Secret projections, protocol/fingerprint, and bounded
scratch. Its identity can read the storage domain rows, read/HEAD the named
authoritative versions, and call `DeleteObjectVersion`; it cannot upload or
copy authoritative bytes, create delete markers, list outside the four exact
roots, mutate application domains, or change S3/KMS configuration. Its
dedicated database credential cannot insert an authorization or update tables
directly; it may execute only the schema-owned claim/progress/complete
functions for an existing `DELETE_ELIGIBLE` or `ABANDONED_PREPARED` predecessor
and its persisted deletion work. The existing leader-elected control-plane
retention loop creates retained-data authorizations and advances their
restore-safe eligibility through ordinary fenced PostgreSQL transactions but
has no authoritative-
delete IAM. Every claim/resume/progress row stores the current restore-promotion
ID, and each function requires that ID plus `WRITERS_OPEN`. While mode is
`EFS_V1`, the claim rules may select an `ABANDONED_PREPARED` predecessor; they
may also
claim `DELETE_ELIGIBLE` only when its immutable `restore_quarantine_id` links a
fully adjudicated shadow/import subject to the matching authorization,
source-lineage clearance, and current-timeline tombstone receipt. Resume under
`EFS_V1` permits exactly the corresponding `deletion_origin =
ABANDONED_PREPARED`, or `deletion_origin = DELETE_ELIGIBLE` plus that immutable
quarantine link. It rejects an unlinked `DELETE_ELIGIBLE` predecessor/origin.
All retained-authority, log-retention, and ordinary immutable-orphan work
requires committed `S3_V1`. This narrow transition rule prevents a restored
EFS-mode migration quarantine from permanently consuming the quota needed for
a fresh cutover without changing the two-value deletion-origin enum.
The routine Helm identity cannot impersonate the GC ServiceAccount or alter its
workload identity. If the Job is absent or unhealthy, permanent usage and
alarms grow but no data becomes eligible by elapsed wall time alone.

Blob GC uses these states:

`READY -> RETIRED_RETAINED -> DELETE_AUTHORIZED -> DELETE_ELIGIBLE -> DELETING -> DELETED`

The `DELETE_AUTHORIZED` transaction is the irreversible boundary. It locks the
blob row and deployment/owner quotas in canonical order; rechecks that every
request, Job, and Serve reference is absent; rejects an active upload,
materialization, reactivation, ordinary `QUARANTINED`, restore-quarantine
provenance, or lease; verifies that the fingerprinted retention deadline passed
by the database clock; and records every exact root/child bucket, key, version
ID, byte count, and digest plus a random deletion generation, `authorized_at`,
and the exact authorization transaction/timeline identity. Restore quarantine
uses the separate typed authorization below rather than bypassing this reject.
The transaction permanently disables reactivation and retains the full quota
charge. It explicitly grants no physical-delete eligibility, so no restore-
coverage observation is required to create the tombstone. No S3 call occurs
while the transaction is locked.

A separate transaction CASes `DELETE_AUTHORIZED -> DELETE_ELIGIBLE`. It locks
the same row and quotas, rechecks that no reference, reactivation, materializer,
writer, takeover, or newer lifecycle epoch exists, and requires a fresh
restore-coverage receipt whose complete promotion universe excludes every
point that can omit the tombstone and whose ancestry-aware coverage proves
every still-promotable chain contains the exact authorization transaction and
tombstone digest. Therefore every database point that
operators may promote already contains the irreversible tombstone before any
exact S3 version can disappear. The transaction records the coverage
generation and eligibility timestamp and retains every byte/object/row quota
charge.

Terminal log streams use the same boundary after their domain retention and
writer-fence horizons pass. The authorization transaction proves the stream is
terminal, its open-coverage marker is closed, no writer or takeover can append,
and the ordered committed/gap projection is final. It records the exact segment
versions in stable sequence order. Gaps contain no S3 version. Immutable-orphan
deletion requires a separately typed adjudication row that binds the original
attempt, proves no domain row can reference it in any supported restore point,
and uses the same authorization-then-eligibility boundary; an operator cannot
supply an arbitrary object key.

Restore-quarantine cleanup has the explicit state path
`RESTORE_QUARANTINED -> DELETE_AUTHORIZED -> DELETE_ELIGIBLE -> DELETING ->
DELETED`. Its schema-owned authorization function locks the quarantine batch,
quarantine subject, any restored source-family row that exists, and quotas;
requires the promotion classification receipt;
proves there is no selected-snapshot committed reference, current reference,
lease, writer, materializer, takeover, or reactivation; and waits through the
fingerprinted retention horizon. It records the exact present or recorded-
absent versions, source family/batch and all-root inventory digest, creates the
irreversible tombstone on the current restored PostgreSQL timeline, and retains
the full byte/object/row charge. It makes no S3 call and never invokes the
ordinary blob authorization function's quarantine reject.

The typed eligibility function then requires both independent proofs: current-
timeline ancestry-aware coverage says every still-promotable chain contains
that exact authorization/tombstone transaction, and the matching quarantine
batch has a fresh `possible_commit_points_cleared` receipt whose source-lineage
backup inventory matches or is dominated by the current complete inventory.
Any source-lineage or descendant backup that could contain a later commit, or
any newer valid backup that references the exact version, blocks eligibility.
Only then does the predecessor become `DELETE_ELIGIBLE`; its immutable
`restore_quarantine_id` remains provenance and the later GC claim records the
existing `deletion_origin = DELETE_ELIGIBLE`. A recorded version already
observed absent still follows this complete path and releases quota only after
the deletion worker re-verifies exact-version absence. Ordinary ambiguous
`QUARANTINED` never enters this function. Thus restore uncertainty cannot use a
fast prepared-object shortcut.

Prepared-object cleanup uses the separate live-timeline path
`PREPARED_PUBLISHED -> ABANDONED_PREPARED -> DELETING -> DELETED`. The abort
transaction may create `ABANDONED_PREPARED` only on the uninterrupted current
PostgreSQL timeline, under the current restore-promotion ID and open writer
gate, after lost-commit reconciliation and a fresh singleton read prove the
preparation attempt never committed. It binds the attempt, source-manifest
digest, abort receipt, and every exact version. Because that current-timeline
abort is durable proof that the bytes never became readable authority, it need
not wait for a retained-data restore horizon. A stale attempt cannot alter the
adjudication, and quota remains charged until every recorded version is proven
absent.

A restored nonterminal `PREPARED` or `PREPARED_PUBLISHED` attempt never enters
that live-abort transition. Promotion invalidates its parent; every present,
recorded, or possibly published child becomes `RESTORE_QUARANTINED`, and only a
child with no recorded exact version plus complete no-version/no-MPU proof
becomes `RESTORE_UNPUBLISHED_ABSENT`. A row already durably
`ABANDONED_PREPARED` in the selected snapshot remains a terminal abort
tombstone and may resume the fast exact-version cleanup. If an EFS-mode restore
reintroduces a nonterminal migration attempt after prior cleanup, it uses the
restore-quarantine path above; if it reintroduces an already-abandoned row, its
durable deletion receipt and exact absence are reconciled forward. Neither case
recreates bytes, makes the old attempt committable, or reuses its IDs or keys.

The leased GC owner may CAS only `DELETE_ELIGIBLE` or
`ABANDONED_PREPARED` to `DELETING`, atomically records the immutable predecessor
as `deletion_origin = DELETE_ELIGIBLE` or
`deletion_origin = ABANDONED_PREPARED`, copies any schema-derived immutable
`restore_quarantine_id`, then deletes only the recorded version IDs. Claim and
resume revalidate that origin/predecessor/provenance tuple and the singleton's
exact EFS/S3 rule above; a caller cannot relabel work. A lost response or worker
is reconciled with that tuple and version-specific `HEAD`: present retries that
same deletion, absent advances that item, and a different current version or
key state is irrelevant because bare `DeleteObject` and key reuse are
forbidden. A stale generation cannot change the authorization, eligibility,
abort adjudication, quarantine provenance, or quota. After every recorded
version is proven absent, one PostgreSQL transaction CASes
`DELETING -> DELETED`, releases its exact permanent or prepared byte/object
quota, and writes the compact deletion receipt. Metadata rows remain charged
until a second fingerprinted tombstone
horizon passes and ancestry-aware coverage proves the exact `DELETED` receipt
is present in every supported restore chain; then the same job may
remove the root, child/segment, and per-object deletion rows and release row
quota while atomically advancing one bounded aggregate deletion watermark/
count/digest per owner or log domain. Keys contain immutable random attempt
generations and are never reused. Before that compaction, a later upload of the
same `(owner, blob_id)` is not reactivation: its admission CASes the retained
root row to a new `PREPARING` lifecycle epoch with a new upload identity, random
staging/authoritative keys, and fresh quota reservation while preserving the old
compact deletion receipt. After compaction it inserts the new root epoch.
`DELETE_AUTHORIZED`, `DELETE_ELIGIBLE`, and `DELETING` reject that admission
with bounded retry; no request can attach to the old epoch.

The production backup and restore procedure always uses an S3-capable image
whose promotion verifier checks every restored PostgreSQL reference before
opening traffic. A restored `READY` or `RETIRED_RETAINED` row still requires its
exact S3 version. A restored `DELETE_AUTHORIZED`, `DELETE_ELIGIBLE`, `DELETING`,
`DELETED`, or `ABANDONED_PREPARED` row is itself a no-reactivation tombstone:
exact absence is reconciled forward to its deletion receipt and quota release,
while presence remains charged and can be deleted only after the restored
database re-establishes the applicable eligibility/adjudication checks. A
restored uncommitted `PREPARED` parent or `PREPARED_PUBLISHED` child is instead
unconditionally invalidated and classified under the restore-promotion fence
above; neither exact object presence nor a snapshot predating its later abort
can make it resumable or committable.
Restoring a database point older than the current supported floor may fail
because a version was correctly collected; it can never silently substitute
another version, list a prefix as authority, or resurrect EFS. Every promotable
backup/PITR chain must be ancestry-proven to contain the exact authorization
tombstones in an eligibility batch, and at least one such provider backup plus
continuous PITR coverage must remain restorable. Periodic isolated restores
advance coverage evidence before the next batch, and an expired restore proof
freezes GC rather than weakening recovery.

## Common log contract

One common library and schema replace all control-plane log paths; this does
not introduce another service:

1. A local spool accepts bytes from request, managed-Job, Serve, and ordinary-
   cluster provisioning/history writers.
2. A shipper publishes immutable S3 segments.
3. PostgreSQL indexes stream ownership, writer fence, ordered segment versions,
   byte offsets, newline counts, terminal state, and explicit gaps.
4. One reader implements stream, follow, tail, and download from that index.

The product scope is exact:

- request execution stdout/stderr and request-status logs, including every
  executor generation;
- managed-Job controller and submitted workload logs for each recovery/task
  attempt;
- Serve controller, load-balancer, replica setup/runtime, and typed provider-
  action logs for each service/replica/action generation; and
- ordinary-cluster provisioning, setup, run/exec, teardown, and downloaded
  workload logs that the API currently persists beneath a server path.

Kubernetes container diagnostics/events, ingress/access logs, client-local
files, cloud-provider audit logs, and application artifacts/checkpoints are
excluded. They remain in their owning observability or object-artifact system;
the common log API does not scrape or copy them.

Every stream identity is
`(domain, durable_domain_id, attempt_generation, writer_role, stream_kind)`.
`durable_domain_id` is a foreign key to the request UUID, managed-Job ID,
service-hash plus lifecycle epoch, Serve replica/action record, or cluster
record UUID as applicable. `attempt_generation` is the domain's existing
executor generation, Job recovery/task attempt, controller-owner/action
generation, or provider-action generation; it is never inferred from pod name
or wall time. The stream row also stores the exact domain fence that admitted
the action. One presentation may concatenate attempts only in durable attempt
order and inserts an explicit attempt-boundary record. It never merges byte
offsets across attempts. A missing interval remains `GAP`, `QUOTA_TRUNCATION`,
`PERMANENT_QUOTA_TRUNCATION`, or `UNKNOWN_TAIL`, not a successful empty
attempt.

The canonical reader returns typed `LogFrameV1` records, never an in-band text
sentinel. The negotiated wire form is UTF-8 NDJSON with media type
`application/vnd.skypilot.log-frames-v1+json`; each line is one strict JSON
object with unknown fields rejected. Every frame carries the stream identity,
a monotonically increasing frame sequence, attempt generation, and frame kind.
`DATA` carries `payload_b64` plus exact start/end offsets.
`ATTEMPT_BOUNDARY` carries the two durable attempt identities. `GAP` carries a
closed start/end interval and one server-owned reason enum; `UNKNOWN_TAIL`
carries the proven start, one server-owned reason, and no invented end.
`QUOTA_TRUNCATION` and
`PERMANENT_QUOTA_TRUNCATION` are distinct reason enums. User bytes can appear
only inside base64 `DATA`, so bytes that spell a marker, JSON object, or frame prefix
cannot impersonate metadata.

The existing stream/follow/tail/download routes use that one versioned frame
encoder internally. Structured clients negotiate the
typed representation; the SDK and CLI decode it before rendering. Human text
rendering prefixes every decoded data line or partial-line fragment with `| `
and every server metadata frame with `! SKYPILOT_LOG_FRAME_V1 `; it never emits
unprefixed user bytes. A user line beginning with either prefix is therefore
still rendered beneath `| ` and cannot look like metadata. Exact-byte download
writes only `DATA` payloads and emits a separate machine-readable index beside
the file; its completion result is false whenever a gap/truncation frame exists.
A legacy raw client may receive concatenated data for compatibility, but
neither the server nor UI may call that response complete without the
structured completeness field. Attempt and gap authority always comes from
PostgreSQL frames, never by parsing displayed text.

The domain tables are `control_plane_log_streams` and
`control_plane_log_segments`; gaps are typed segment rows with constrained
reason enums and offset shapes, not a separate generic object model or caller-
provided message. Before a child/provider action starts, PostgreSQL creates
the stream, current restore-promotion ID, a fresh writer generation/lease, and a
durable open-coverage marker. Every writer claim, renewal, segment reservation,
publish, and terminal transition requires that promotion ID and
`WRITERS_OPEN`.
That marker ensures an expired writer is distinguishable from a clean terminal
stream even when PostgreSQL and the pod disappear together. The only no-stream
case is the transactional permanent-row-quota domain marker above.

For each segment, the writer reserves its sequence and byte interval and
commits `PREPARING`, its unique key in the immutable authoritative-log
namespace, digest, permanent quota reservation, restore-promotion ID, fresh
random segment ID, and writer generation. The same leased generation and
promotion CAS `PREPARING -> PUBLISHING` before S3 I/O, then CAS
`PUBLISHING -> COMMITTED` with the exact version and permanent quota usage. The
S3 create is conditional on nonexistence. A lost response or 409/412 is
reconciled by `HEAD` of the unique key, attempt metadata, and digest; a mismatch
is quarantined and never overwritten. If an authoritative create occurred but
can never commit, reconciliation converts its reservation to permanent leaked-
object usage and alarms; it cannot delete or reuse the key. A successor locks
the stream row, advances the writer generation within the same restore-
promotion ID, reconciles predecessor
`PREPARING`/`PUBLISHING` rows, resumes at the last contiguous committed offset,
and converts any unprovable interval into a gap. A stale generation cannot
publish or close the stream. After a restore, this ordinary takeover is
forbidden: the old writer/segments follow the family-table quarantine path, the
open-coverage marker closes with a gap or `UNKNOWN_TAIL`, and any continuing
domain action receives a new writer epoch and fresh segment IDs/keys without
replaying its provider effect.

A terminal barrier records the last proven offset and closes the open-coverage
marker. If spool overflow or hard pod loss occurs while PostgreSQL is
unavailable, takeover observes the expired generation plus the still-open
marker and commits an `UNKNOWN_TAIL` gap from the last proven offset; it never
reports completeness or invents an exact end offset. When counters survive,
the gap records the exact lost interval. The reader emits a typed frame for
either form of gap or quota truncation and never concatenates noncontiguous
bytes as if they were complete. Log attempt keys are authoritative and are
never deleted merely because a stream row is missing, retired, or invisible.
Only the restore-safe authorization and exact-version GC protocol may remove
them. Prefix listing remains diagnostic and never establishes ownership.

Flush thresholds and spool quotas are measured from production log rates and
configured per role. The chart requires finite positive values for maximum
segment bytes, flush interval, per-stream spool bytes, and per-pod spool bytes;
there are no unproven universal constants. Sizing must satisfy:

`pod spool >= sum(active stream rates * tolerated S3 outage) + one segment per stream`

and remain within the pod's ephemeral-storage request, limit, and `emptyDir`
`sizeLimit`. At a hard ceiling the shipper keeps draining the child, discards
only the excess log bytes, counts the lost offset range, and commits a gap when
PostgreSQL is available. It does not block, cancel, or replay provider work.
Hard pod loss is reconciled as a gap from the last committed offset when the
source cannot be resumed.

Each stream attempts to reserve its configured maximum authoritative bytes and
segment count against both durable-owner/domain and deployment permanent quotas
before the provider action starts. Publishing converts the reservation
incrementally to permanent usage. A stream that cannot reserve starts in the
metadata-only truncating state above. If an already-admitted stream exhausts
its reserved range, the shipper drains and counts excess bytes, commits the
closed `PERMANENT_QUOTA_TRUNCATION` frame at terminal (or typed unknown tail on
hard loss), and never borrows from another domain or blocks the provider
action.

Domain success and log completeness are separate. A Job or Serve action can
finish with incomplete logs; the UI/API reports that state. Recovery may ship
or reconcile the recorded spool/attempt, but never repeats the provider action.
Process diagnostics continue to use container stdout and the existing external
collector, not this user-log store.

With healthy PostgreSQL/S3, a live byte becomes visible to `stream`/`follow`
within ten seconds; a full segment may publish sooner. A terminal writer has 30
seconds to flush and close after the domain action exits. Takeover must commit
the successor, a typed gap, or `UNKNOWN_TAIL` within 120 seconds after the
predecessor lease expires. These are maximum product bounds, not monitoring
targets, and tests force each deadline. `tail` and `download` read the same
ordered index and therefore expose the same attempt boundaries and gaps.

PostgreSQL admission precedes a provider effect: if the stream/fence/open-
coverage transaction is unavailable, that new provider action does not start.
After the provider action has begun, logging is never on its success path. If
S3 is unavailable, bytes remain in bounded spool until it recovers; quota
overflow records the lost range. If PostgreSQL becomes unavailable, the writer
continues the already-admitted provider action and bounded local spool but
cannot make a segment authoritative; recovery publishes under the same writer
generation only when the restore-promotion ID remains current, otherwise the
restore fence invalidates it and records a gap. If both are unavailable or the pod dies,
takeover uses the durable open-coverage marker and last committed offset to
record an exact gap when counters survived or `UNKNOWN_TAIL` otherwise. None of
these outcomes replays, blocks completion of, or changes the durable status of
the provider action.

Terminal stream retention is explicit on the stream row. Expiry logically
retires the stream, but its exact committed S3 versions and quota remain until
the restore-safe authorization and exact-version absence protocol completes. A
typed gap needs no S3 object and remains in PostgreSQL until the same domain
metadata horizon permits compaction.
Request, managed-Job, Serve, and
ordinary cluster provisioning/history logs all use this contract; legacy
`global_user_state.provision_log_path` and equivalent path fields become
regenerable compatibility hints backed by the stream reference, and their
one-root bytes are imported or explicitly classified before cutover.

## Pod, identity, and capacity contract

The final chart renders bounded disk-backed `emptyDir` volumes for uploads and
materialization, log spool, generated `.sky` files, and ephemeral `.ssh`
material. Durable credential sources remain existing Kubernetes Secrets or
projected workload identity. No EFS credential file is copied to S3 as a
shortcut.

A built-in Kubernetes `ValidatingAdmissionPolicy` and binding permanently
fence the three SkyPilot control-plane Deployments after cutover. They live in
a separate `skypilot-storage-fence` release/object ownership boundary, never in
the `skypilot` Helm release or its history. The routine SkyPilot release
identity is namespaced and has no permission to update or delete admission
policies or bindings; a separately audited fence identity applies this second
direct-Helm release, with cluster-admin retained as explicit break-glass.
Platform IaC owns only that identity/RBAC separation, not the mutable policy
artifact or its image allowlist. Negative `auth can-i` proof is a cutover
receipt.

The Kubernetes policy cannot be trusted to validate replacement of itself.
Every fence command therefore runs through the same bounded PostgreSQL cutover
lease as the application Helm command. The lease uses the database clock, one
random fencing token, finite expiry/renewal, and a CASed operation phase; only
one of `FENCE_MUTATION` or `APPLICATION_MUTATION` may be active. Before each
fence mutation, including abort recovery and ordinary post-cutover image-
allowlist updates, the holder freshly reads the committed storage mode and
generation under that token and records the expected old/new fence digests.
`TRANSITION` and `SEALED` require `EFS_V1`; restoring `TRANSITION` additionally
requires proof that no `S3_V1` commit occurred. `S3_ONLY` and every later
S3-only fence update require the exact committed `S3_V1` generation. An old
read, a Helm release value, or the VAP's own annotation is not mode evidence.
The direct-Helm operator invokes schema-owned lease functions; this adds no
deployment coordinator, webhook, or always-on service.

The fence release is changed only with a rendered, complete, immutable
`helm upgrade --install --wait` artifact. Native `helm rollback`, `--atomic`,
and reuse of a historical fence revision are forbidden because Helm's implicit
rollback could restore a pre-commit policy after the database commits. A lost
or expired lease stops new mutations; its successor first reconciles the
PostgreSQL mode, operation receipt, fence UID/resourceVersion/digest, and
application release before choosing the next forward phase. The active lease
token is included in the exact fence parameters and guarded-HA workload
annotations, so the application release is inadmissible under a different or
stale operation token.

After cutover, an ordinary rolling update uses three nonoverlapping phases
under that lease: the fence first admits only the exact current and candidate
S3 image/storage/token tuples, the application rolls and proves every old
ReplicaSet/Pod gone, and the fence then tightens to the candidate tuple alone.
The current tuple is transition overlap, not rollback authority, and neither
phase can admit EFS or an unrelated historical token.

Every Helm child has an absolute timeout shorter than the remaining lease plus
renewal reserve, and the holder records its operation digest before execution.
Lease loss terminates the local child and marks the result ambiguous. A
successor cannot start either mutation kind merely because the lease expired:
it waits through the prior child's maximum API-request deadline, proves from
the exact Helm/Kubernetes receipts that no old mutation remains in flight,
rotates the operation token in the next fence phase, and only then admits a new
application phase. This quiescence barrier prevents an expired Helm process
from overlapping its replacement.

The policy matches the control-plane's exact namespace, names, and
ServiceAccounts and requires state-specific protocol/fingerprint/lease-token
annotations and digest-pinned images. `S3_ONLY` requires `S3_V1` plus absence
of PVC/EFS volumes; `TRANSITION` has the exact EFS and no-EFS workload classes
below, and `SEALED` admits neither. It is an external rollback fence, not a
webhook or service. Its cutover has three exact, digest-bound states:

1. `TRANSITION` admits only the audited live `EFS_V1` release, the two prepared
   `S3_V1` release digests, exact migration/verification Jobs, and the exact
   no-EFS storage-GC CronJob/child Job class restricted by its database
   credential to `ABANDONED_PREPARED` claims or fully adjudicated,
   immutably linked restore-quarantine `DELETE_ELIGIBLE` claims.
2. `SEALED` denies every SkyPilot control-plane Deployment, ReplicaSet, Pod,
   hook, and alternate pod-producing create/update by the routine Helm identity
   or descendant workload controller. It is installed and its negative probes
   are committed before the PostgreSQL `S3_V1` transaction. While the database
   still records `EFS_V1`, a failed pre-commit attempt may restore only the exact
   audited `TRANSITION` policy.
3. `S3_ONLY` opens only the two prepared `S3_V1` releases, their exact hooks,
   and the fingerprint-bound storage-GC CronJob/child Jobs. It is installed
   only after the database commit. No post-commit operation can restore
   `TRANSITION` or admit EFS.

Thus a crash between Kubernetes and PostgreSQL operations leaves a fail-closed
maintenance outage, never a committed S3 database with an EFS workload still
admissible. Every retained Helm revision is rendered and submitted server-side
to prove it is denied in `S3_ONLY`. Future releases preserve the protocol
annotation and satisfy the same policy.

The policy/binding covers every pod-producing shape the routine Helm identity
or storage-GC controller can submit in the exact `skypilot` namespace:
`apps/v1` Deployments, ReplicaSets, StatefulSets, and DaemonSets; `batch/v1`
Jobs and CronJobs; their `scale` subresources where applicable; and core/v1
Pods and ReplicationControllers plus `pods/ephemeralcontainers`. It uses
`failurePolicy: Fail`, `validationActions: [Deny]`, and
equivalent-version matching, and every binding sets
`parameterNotFoundAction: Deny`; a missing parameter or CEL/runtime error
denies the request. Its resource rules enumerate `CREATE` and `UPDATE`
(including patch) explicitly. `DELETE` is outside object-shape CEL because it
cannot create or mutate a pod template and must remain available to the
separately authorized cleanup path; direct `CONNECT`/`pods/exec` is denied by
RBAC. Matching is an OR over the exact three control-plane
names, the immutable Helm release/role or hook labels, a control-plane or hook
ServiceAccount, a descendant owner reference, and the separately fingerprinted
`request.userInfo` principals/groups allowed to perform routine SkyPilot Helm
operations. A top-level create is therefore validated even when its manifest
deliberately omits every SkyPilot label, chooses another ServiceAccount, or
relies on a Helm hook annotation. The principal set is an immutable input to
the separately owned fence release, not a value supplied by the routine chart.

CEL validates the correct nested pod template for every workload kind and the
final Pod. It requires the committed storage mode, protocol, fingerprint, exact
workload class, and digest-pinned image. Its volume-source allowlist contains
only the exact bounded disk-backed `emptyDir`, Secret, ConfigMap,
projected-token, and downward-API volumes required by that workload; every
other source, including
`persistentVolumeClaim`, CSI, NFS, `hostPath`, iSCSI, CephFS, and FUSE
device/capability, is denied. It also rejects privileged/host namespaces,
unbounded disk-backed scratch, unknown init/ephemeral/sidecar containers,
mutable image references, `envFrom`, and unapproved command, arguments,
environment, capability, Secret, or ConfigMap projections. Migration,
config-seed, post-rollout verifier, and
storage-GC workloads have distinct exact workload classes and exhaustive name/
image/command/argument/environment/volume/ServiceAccount contracts; they are
not exempt from storage checks. A Job/CronJob template must propagate the
required labels and annotations so its controller-created Pod remains
independently admissible. CEL does not trust or look up a parent: each
Deployment, ReplicaSet, Job, and Pod independently proves the same committed
constants.
Matched `scale` requests are denied rather than trying to infer template safety
from a Scale object.

Cutover deletes every old control-plane ReplicaSet and completed superseded
hook Job after the new pods are Ready. The routine Helm release identity can
create/update/patch the three Deployments and create only policy-conforming
revision-scoped hook Jobs plus the exact policy-conforming storage-GC CronJob;
the principal-aware policy validates every CronJob write by that identity. It
has no direct Pod/ReplicaSet create/update/patch/delete, `pods/exec`,
Deployment/ReplicaSet `scale`, StatefulSet, DaemonSet, PVC/PV, ServiceAccount,
RBAC, or admission-policy permission. Its namespaced CronJob permission is
usable only for the exact GC object because every other create/update is denied
by the principal-aware policy.
Because Kubernetes RBAC cannot restrict a `create Job` by future object name,
the principal-aware admission rule validates every Job create by that identity,
not only recognized labels or names. Workload controllers retain only their
ordinary child-management permissions and remain subject to Pod admission.

Server-side dry runs prove denial of an old Deployment; a scaled old
ReplicaSet; a direct Pod with the normal ServiceAccount; a forged or absent
release label; a top-level unlabeled Job from the Helm identity; pre-install,
pre-upgrade, post-install, and post-upgrade hook Jobs with an old image, missing
fingerprint, alternate ServiceAccount, or forbidden volume; and a CronJob or
other pod-producing controller used as an indirection. Positive tests cover
the exact migration/seed/verifier Jobs, storage-GC CronJob/Job, and controller-
created ReplicaSet/Pod and Job/Pod requests on the production Kubernetes minor
version. Cluster-admin
is the only break-glass bypass and its use is an audited incident, not rollback
behavior.

The existing API, executor, and controller ServiceAccount arrangement remains;
the migration does not split those runtime identities. Existing workload
identity receives only the S3/KMS actions needed by the roles it already
serves. At most one temporary migration ServiceAccount/identity may read the
source claim and write the prepared S3 prefix; it is removed by cutover
cleanup. The one permanent additional identity is the exact storage-GC CronJob
identity described above; it has no application-provider, upload, policy-
mutation, or bare object-delete authority.

Sizing is a release gate, not a guess:

- the final one-root manifest replaces the approximate 23.82-GB baseline and
  sets initial S3 cost/retention alarms;
- maximum archive bytes, expansion bytes/inodes, and simultaneous
  materializations determine each role's upload/materialization `emptyDir`;
- measured stream count and byte rates determine log-spool values; and
- the sum plus measured application overhead and explicit headroom determines
  Kubernetes ephemeral-storage requests and limits.

The cutover rehearsal must fill every bound deliberately and prove clean
rejection, cleanup, and unaffected provider execution. A chart with missing or
unmeasured values cannot select `S3_V1`.

## One-root migration and cutover

The migration inventories only `skypilot-state-rwx` and its exact PV/PVC/access
point. Each safe relative path receives type, bytes, digest, and one outcome:
blob, log, already-authoritative PostgreSQL value, Secret/projected credential,
disposable cache/generated file, or blocking unknown. Symlinks that escape the
root, special files, unreadable entries, and unmatched credentials block.

Before production maintenance, an isolated rehearsal restores a current backup
of the one source filesystem, runs the importer twice, verifies idempotency,
reconstructs every recoverable cluster/Job/Serve record without EFS, and proves
that `boltz-l4-fleet` recovery causes no provider mutation. The rehearsal
records source bytes, file/inode counts, backup completion, restore duration,
import/verification duration, EFS restart duration, `TRANSITION -> SEALED` and
`SEALED -> S3_ONLY` admission-fence readbacks, a successful S3 rollout, a
deliberately failed first S3 rollout
followed by the prebuilt fix-forward release, 2/2/2 readiness, no-PVC and
historical-revision verification, PVC/PV unbind, maintenance reopening, and the
slowest observed phase. The maintenance owner sets a finite approved RTO only
after that evidence.

The migration-run row binds the RTO to the final source manifest and stores an
absolute pre-commit abort deadline, rollback-restart reserve, and one full
irreversible-tail budget. That tail budget is the measured worst-case sum of:
generation commit/readback; `S3_ONLY` fence opening/readback; the first no-PVC
Helm rollout; one already-built, immutable, S3-capable fix-forward rollout if
the first fails; 2/2/2 readiness and reference verification; denial of retained
revisions; PVC/PV unbind/finalizer handling; and maintenance-gate reopening,
with explicit headroom for every component. It is not merely an S3 pod-start
timer. Each component has its own absolute database-clock deadline and the
overall irreversible-completion deadline never moves. AWS EFS restore has no
assumed SLA; the measured rehearsal plus explicit reserves are the only
planning inputs.

The production cutover is one ordered maintenance operation. Its owner holds
and renews the one PostgreSQL cutover lease across the sequence, and CASes the
operation phase before starting either Helm child; fence and application Helm
processes never overlap:

1. Acquire the cutover lease, freshly prove committed mode `EFS_V1`, install
   and read back the separately owned `TRANSITION` fence with the exact lease
   token, then end its `FENCE_MUTATION` phase. Start a nonoverlapping
   `APPLICATION_MUTATION` phase and deploy the transition image with direct
   Helm `upgrade --reuse-values`; prepare schemas and shadow S3 objects without
   changing read or write authority. Prove the routine SkyPilot release
   identity cannot alter the fence.
2. Enter a PostgreSQL maintenance gate, reject new mutating requests/uploads,
   drain or classify existing work, record the maintenance start/deadlines,
   and scale all six role pods to zero. Every following pre-commit phase checks
   the same database-clock deadline.
3. Prove no pod, Job, debug process, or node mount is writing the claim. Run the
   migration Job with the claim read-only and finish the one-root import.
4. Capture the final manifest. Start a new backup after zero-writer proof,
   restore it to an isolated filesystem/access point, and compare the restored
   manifest exactly with the source manifest.
5. Verify every live request, Job, Serve version, cluster-log reference, and
   indexed log resolves to a committed exact S3 version or an explicit typed
   gap; verify the prepared fingerprint, PostgreSQL snapshot, migration counts,
   and no blocking unknowns.
6. In a new nonoverlapping `FENCE_MUTATION` phase, freshly re-read and prove
   PostgreSQL still records `EFS_V1`, then replace `TRANSITION` with `SEALED`.
   Read back its UID, resourceVersion, canonical policy/binding digest, and
   lease token and run every negative top-level, hook, alternate-controller,
   and direct-Pod probe. Persist that receipt in the migration-run row. If the
   seal or probe fails, freshly prove `EFS_V1`, restore only the exact audited
   `TRANSITION` digest through a forward fence upgrade, and restart the
   unchanged EFS generation before the pre-commit abort deadline.
7. Commit `S3_V1` in the serializable generation transaction only if the
   same cutover lease/fencing token remains current, the pre-commit abort
   deadline has not elapsed, the `SEALED` receipt remains current, and the
   remaining approved RTO is at least the recorded full irreversible-tail
   budget. The transaction records the database-clock checks, sealed-policy
   receipt, and exact digest of both the primary and prebuilt fix-forward Helm
   releases. If the transaction does not commit, freshly prove `EFS_V1` before
   restoring the exact `TRANSITION` policy and restarting the unchanged EFS
   generation. A lost commit response is reconciled from PostgreSQL before
   either action.
8. In a new `FENCE_MUTATION` phase, freshly prove the exact `S3_V1` generation,
   replace `SEALED` with the exact `S3_ONLY` policy, and read back its UID,
   resourceVersion, digest, and lease token before any SkyPilot Helm Deployment
   update. Admission does not evict existing objects, but every EFS Deployment,
   hook, direct Pod, and alternate controller remains denied. A crash or failed
   policy update leaves `SEALED`; recovery retries only the pre-reviewed
   `S3_ONLY` artifact after another fresh `S3_V1` check and cannot reopen
   `TRANSITION`.
9. End the fence phase, CAS the lease to `APPLICATION_MUTATION`, and deploy the
   no-PVC chart directly with Helm `upgrade --reuse-values`, pinned to the
   merged immutable image/chart digest and same lease token, then restore 2/2/2
   readiness. If that first rollout misses its component gate, immediately
   deploy the rehearsed immutable S3-capable fix-forward release under the same
   lease; no code authoring, policy weakening, or EFS rollback occurs inside
   the irreversible tail.
10. While maintenance still blocks mutations, delete the one PVC and PV under
   their verified `Retain` contract using the separately approved cutover
   identity, never the routine Helm identity. Preserve their exact manifests
   and retain the CSI-created access point, filesystem, final backup, and
   isolated restore. Prove the six
   healthy pods have no EFS mount and server-side-submit every retained
   historical Helm revision to prove the admission fence rejects it, including
   revisions that would dynamically create a new empty PVC. Only then leave
   maintenance. PVC/PV finalizer handling, negative probes, and gate reopening
   must each finish before their stored component and overall irreversible-tail
   deadlines. Release the cutover lease only after the final application and
   fence receipts plus maintenance reopening are committed.

No `boltz-platform` SkyPilot pin is updated. SkyPilot fixes merge forward,
publish an immutable overlay/chart, and deploy directly through the existing
Helm release. Infrastructure PRs contain only the S3/KMS/IAM additions or the
minimum fence-identity/RBAC separation when it does not already exist, plus the
later exact EFS deletions; they do not carry application configuration or an
image allowlist.

Before step 7, any failed receipt or deadline exhaustion automatically stops
the migration and reconciles the singleton on the same uninterrupted live EFS
timeline under the current promotion ID. Only after that proof may it CAS every
never-committed exact version for that attempt to `ABANDONED_PREPARED`, freshly
prove `EFS_V1`, restore the exact `TRANSITION` policy through a forward fence
upgrade if `SEALED`, and restart the unchanged EFS generation within the
reserved rollback interval. The transition-admitted GC may reclaim the
abandoned exact versions asynchronously; their quota remains charged until
absence, so cleanup must complete before another attempt if headroom is
insufficient. The live
claim has not been deleted. An over-RTO rehearsal or production abort requires
a new measured plan and approval; extending a running deadline is forbidden.
After step 7, Helm rollback to an EFS image or values is forbidden. The
maintenance gate covers the complete commit/fence/deploy/verify/unbind/reopen
tail. A missed `S3_ONLY` component retries only its pre-reviewed policy artifact
under the same cutover lease and a fresh `S3_V1` read; after that policy opens,
a nonoverlapping missed application component escalates immediately to the
prebuilt S3 fix-forward release. The immutable overall deadline remains visible
throughout.
Crossing the overall deadline is a declared RTO breach and incident, not
permission to reopen early, weaken the fence, extend the clock, or mount EFS.
After step 8, the external admission policy rejects historical charts
independently of whether they reference the deleted claim, create a fresh empty
claim, or ignore `S3_V1`. Cluster administrators can change admission policy,
so release ownership and review still prohibit weakening the fence or manually
combining an old image with invented values. Repair deploys a newer S3-capable
image or a higher PostgreSQL storage generation.

Immediate, +10-minute, +30-minute, +24-hour, one-pod-per-role, and total
role-blackout acceptance start the approved recovery horizon. After that
horizon and the cutover cleanup, the infrastructure-delete PR removes the
retained CSI-created access point through the AWS API; the access point is not
Terraform-owned and deleting it alone does not delete its directory. If the
filesystem is proven exclusive, the saved Terraform plan then deletes the
filesystem (and its contents), mount targets, security group, CSI/IAM edges,
and StorageClass. If it is shared, a one-time Job mounted only through the exact
retained access point runs in a separately fenced cleanup namespace and deletes
the verified SkyPilot source directory after the backup/restore hold, then the
AWS API deletes that access point; shared
filesystem resources remain. The PVC/PV were already unbound in step 10.
Unrelated edges and the required isolated backup/restore evidence are retained.

## Compact implementation stack

The expected review boundaries are below. They may split further when a diff
becomes unsafe to review; PR count is not a behavioral invariant. The EFS
application cleanup and the API 24--40 adapter cleanup are authored as stacked
drafts with the first transition, cross-linked from every transition PR, and
updated as the blob, log, and cutover branches land. The EFS cleanup's exact
merge gate is the complete production acceptance horizon. The adapter cleanup
has the independent `MIN_COMPATIBLE_API_VERSION > 40` gate, so retiring EFS is
not delayed by an older supported client while the temporary adapter still has
a concrete removal change. Transition-only code, flags, metrics, importer, and
tests are not left as TODOs.

| # | PR | Scope and merge gate |
| --- | --- | --- |
| 1 | Design and historical cleanup | This file plus removal of the executable-looking EFS plan from the role-split design; merge after exact adversarial approval |
| 2 | Surgical infrastructure add | Prefer an existing server-owned private versioned bucket only when BucketOwnerEnforced/Public Access Block, disjoint staging/authoritative roots, KMS, enforced conditional create, authoritative no-expiry, ordinary-authority delete/version-delete deny, exact-version-only GC identity, permanent quota alarms, and per-identity negative probes pass; otherwise add only those minimum bucket/KMS/IAM resources, the exact fence identity/RBAC separation if absent, and the optional temporary migration identity; no mutable policy/image allowlist, live EFS change, or broad Terragrunt apply |
| 3 | Blob transition | Domain/object/quota tables, staging-to-immutable conditional-publish S3 `BlobStorage`, promotion-scoped blob/upload/part/materialization leases and fresh physical keys, unchanged API 41+, content-addressed retained reuse, API 24--40 per-user adapter, exact promotion-scoped legacy manifest/content authority and owner import, Serve refs, and scoped materialization |
| 4 | Common-log transition | Promotion-scoped writer/segment prepare-publish-commit spool with fresh epochs/keys, permanent reservations, PostgreSQL index and typed spoof-resistant frames/gaps, takeover/terminal recovery, S3 reader, and the exact request/Job/Serve/ordinary-cluster integrations; no partial activation |
| 5 | Restore-safe retention/GC | Timeline-ancestry-aware restore coverage, all-authoritative-root version/MPU inventory, cross-family restore invalidation/quarantine and source-lineage possible-commit clearance, fingerprinted retention horizons, `DELETE_AUTHORIZED -> DELETE_ELIGIBLE` exact-version deletion protocol, exact two-value deletion-origin enum plus immutable quarantine provenance, bounded GC CronJob/identity, EFS-mode shadow-quarantine liveness, lost-response recovery, quota release, metadata compaction, and isolated-restore tests; this is required before cutover, not a future TODO |
| 6 | Cutover transition | Generation/fingerprint, promotion-scoped prepared-attempt/object rows plus current-timeline-only `ABANDONED_PREPARED` adjudication and the restore-promotion writer/endpoint fence, one-root importer/restore/quota verifier, bounded `emptyDir`, stored-value migration, one fenced PostgreSQL cutover lease, no-PVC guarded-HA mode, and separately owned principal-aware `TRANSITION -> SEALED -> S3_ONLY` admission fence covering every pod-producing shape and hook |
| 7 | EFS application cleanup | Remove `EFS_V1`, remote shared-path blob/log paths and decoders, importer, old guarded-HA values/templates/mounts, temporary identity, transition metrics, and transition-only tests after the full production horizon; retain the independently gated API 24--40 adapter and generic non-HA PVC support |
| 8 | API 24--40 adapter cleanup | Remove the sole per-user legacy upload-slot adapter and its mixed-client tests only after `MIN_COMPATIBLE_API_VERSION > 40`; this gate does not delay EFS application or infrastructure removal |
| 9 | Exact infrastructure deletion | Delete only the retained SkyPilot access point/data and saved-plan EFS/CSI/IAM/network/Terraform objects after EFS application cleanup and restore proof |

The blob and cutover transitions are substantial domain changes; the common-
log transition is the largest because it replaces several writers and readers.
Each stays disabled until its complete domain integration and fault suite land;
there is no partial production activation. No PR adds a new service, updates a
SkyPilot platform pin, or performs a broad Terragrunt refactor.

The unmerged RWX stacks in boltz-platform PRs #7824 and #7829--#7833 and the
draft design PR #8443 are not implementation inputs; PR 1 supersedes and closes
them. The final infrastructure PR removes only the live EFS resources introduced
by merged PRs #8596 and #8601 after the gates above. It does not revert
unrelated platform history or update a SkyPilot pin.

## Verification and open gates

Automated tests must cover:

- real PostgreSQL/S3 crashes at every blob prepare/publish/commit boundary,
  staging cleanup, multipart resume/copy, conditional-create conflicts,
  response loss, exact-version verification, reference/retirement/reactivation
  races in every lock ordering, stale lease/attempt rejection, cross-user
  denial, transactional byte/object/metadata-row quota oversubscription and
  recovery, retained content-addressed reuse, immutable-orphan restore
  accounting, and proof that
  neither ordinary IAM nor lifecycle can delete an authoritative version in
  any state;
- timeline-ancestry and backup-inventory races, divergent PostgreSQL timelines
  with overlapping numeric LSNs, stale/missing coverage evidence, reactivation
  versus deletion authorization, and proof that `DELETE_AUTHORIZED` alone
  cannot issue an S3 delete. `DELETE_ELIGIBLE` remains denied while any
  promotable backup/PITR chain can omit its exact tombstone transaction;
  exact-version-only GC, stale leases, lost delete responses, partial multi-
  object deletion, quota release only after absence, tombstone compaction, key
  non-reuse, old-backup promotion refusal, and an isolated restore after each
  deletion batch all pass. The persisted `deletion_origin` accepts exactly
  `DELETE_ELIGIBLE` or `ABANDONED_PREPARED` and rejects null, caller-supplied,
  or mismatched origin/provenance;
- one or many pre-commit aborts before and after exact-version publication,
  lost commit responses, lost delete responses, a changed source manifest,
  disjoint attempt keys, stale `ABANDONED_PREPARED` owners, transition-mode GC
  admission, quota remaining charged through partial deletion, and exact quota
  recovery only after every abandoned version is absent. Only an uninterrupted
  current timeline may create `ABANDONED_PREPARED`; a restored nonterminal row
  is rejected from that transition even when its recorded exact version is
  already absent. A selected-snapshot `ABANDONED_PREPARED` tombstone still
  resumes its exact fast cleanup;
- restore snapshots before a later abort **and before a later commit** exercise
  storage-generation preparation, ordinary blob/upload and destination parts,
  `LEGACY_TREE_V1` manifest/content construction, materialization leases, and
  log writer/segment publication. For each S3-writing family, fixtures cover a
  present exact version, a recorded-but-now-absent version, a lost publish
  response, no recorded version plus proven absence, incomplete/multiple MPUs,
  and a key created later on the source timeline that is absent from the
  selected database. The promotion gate stays closed until two stable,
  complete, paginated inventories account for every current/noncurrent version
  and MPU in all authoritative roots; missing pages, metadata, ownership,
  digest/KMS identity, or bindings fail closed. Present/recorded/possible
  publication becomes charged, unreadable `RESTORE_QUARANTINED`; only exact
  no-version/no-MPU proof becomes `RESTORE_UNPUBLISHED_ABSENT`, and no restored
  nonterminal object becomes `ABANDONED_PREPARED`;
- restore-quarantine tests keep every version unattachable, nonreactivatable,
  and nonreusable while a fresh logical blob epoch uses distinct quota and
  physical keys. They prove
  `RESTORE_QUARANTINED -> DELETE_AUTHORIZED -> DELETE_ELIGIBLE` requires both a
  current-timeline tombstone in every promotable chain and expiry/durable
  non-promotability of every source-lineage possible-commit point. A newer
  valid backup containing or possibly referencing the version blocks deletion;
  a timestamp or larger LSN on a divergent timeline never clears it. Exact
  absence releases quota only after full GC. Under `EFS_V1`, claim/resume accepts
  `ABANDONED_PREPARED` and only the fully adjudicated, immutably linked
  restore-quarantine `DELETE_ELIGIBLE` subject; it denies every unlinked
  `DELETE_ELIGIBLE` origin. Stale family mutators, old promotion IDs, generation
  collisions, writer epochs, materialization paths, reserved log intervals,
  multipart IDs, and S3 keys are rejected; fresh work uses new identities, and
  log recovery records a gap/`UNKNOWN_TAIL` without replaying provider effects;
- unchanged API 41+ clients, API 24--40 serialization/claim/expiry, concurrent
  uploads, archive traversal/expansion limits, `LEGACY_TREE_V1` root/child
  atomicity and manifest/object mismatch denial, Serve-version retention, and
  materialization cancellation/pod loss;
- common log prepare/publish/commit ordering, writer takeover fencing,
  follow/tail/download, lost S3 responses, simultaneous S3/PostgreSQL outage,
  spool and permanent byte/object/row-quota truncation, no-stream domain
  fallback, hard-pod-loss `UNKNOWN_TAIL`, terminal
  barriers, terminal logical retention, immutable-orphan accounting, cluster
  provision-log migration, UTF-8/partial lines, data payloads that imitate every
  display/frame marker, typed-frame corruption, legacy-raw incompleteness, and
  proof that provider actions are neither blocked nor replayed;
- exact captured production stored values (revision 434 at this baseline,
  refreshed immediately before implementation) through successive Helm
  `--reuse-values` renders, 2/2/2 RollingUpdate behavior, bounded ephemeral
  storage, and a final manifest with no PVC or EFS mount, plus server-side
  denial of every retained pre-cutover Helm revision and every top-level/hook
  Job, CronJob, alternate controller, direct Pod, label omission/forgery, and
  ServiceAccount indirection bypass described above. The same transition-
  capable image must become Ready in its exact audited EFS workload class while
  an S3-rendered instance of it fails readiness and every durable write before
  commit. Transition tests kill the operator after `SEALED` but before commit,
  after commit but before `S3_ONLY`, and during a failed `S3_ONLY` readback,
  proving only the first case can restore the exact transition policy and both
  post-commit cases remain sealed until S3-only fix-forward. They also reject
  overlapping fence/application Helm children, stale or lost cutover leases,
  every incorrect fresh PostgreSQL mode/generation, fence-release native
  rollback/`--atomic`/historical reuse, and an application manifest carrying
  the wrong lease token; and
- one-root classification, repeatable import, final backup timing, isolated
  restore equality/quota reconciliation, pre-commit abort, every absolute
  irreversible-tail component deadline, failed-primary/prebuilt-fix-forward
  recovery within the approved RTO, and a saved delete-only then empty
  infrastructure plan.

Production metrics extend existing telemetry with storage mode/generation/
fingerprint, blob upload/retirement/reactivation state, permanent quota
reserved/committed/orphan/abandoned bytes, objects, and metadata rows, restore-
coverage/tombstone ancestry age, backup-inventory generation/freshness,
possible-commit interval count/age/clearance, all-root inventory page/digest
stability, GC authorization/eligibility/deletion/absence age and failures,
deletion-origin counts, and restore-quarantine bytes/objects/rows/oldest age by
source family. They also report prepared/abandoned/restore-invalidated and
restore-unpublished-absent attempt counts by family, restore-promotion gate/age,
cutover lease/operation phase, materialization bytes, log segment age, spool
utilization, typed gap/truncation counts, S3/KMS errors, migration counts,
irreversible-tail phase/deadline, and EFS I/O until deletion. They contain no
object keys, paths, credentials, lease tokens, promotion IDs, or signed URLs.

Open gates are:

1. Storage, database, Serve/Jobs, and platform owners approve this exact
   design and its review/cleanup boundaries.
2. A fresh audit reconfirms the single source handle, byte/file/inode
   inventory, exact infrastructure addresses and ownership, existing
   ServiceAccounts/IAM, maximum archive/member/expansion/materialization
   dimensions, concurrency, log rates, complete PostgreSQL backup/PITR
   inventory, and supported restore horizon. It commits every transient and
   permanent byte/object/metadata-row quota plus each retention period into the
   candidate fingerprint and proves the retained-data maximum is operationally
   acceptable between GC cycles.
3. The infrastructure-add plan and positive/negative S3/KMS identity probes
   pass without changing EFS; staging and authoritative namespaces are
   disjoint, runtime/migration/cleanup identities cannot delete authoritative
   current or noncurrent versions, and lifecycle cannot expire any
   authoritative blob, legacy manifest/content, or log version.
4. Blob and log transitions pass real PostgreSQL/S3 fault tests and an isolated
   no-EFS controller/`boltz-l4-fleet` recovery rehearsal.
5. Restore-safe retention/GC passes exact-version deletion fault injection,
   proves no object can become eligible until every promotable current-timeline
   chain contains its exact tombstone, and proves a divergent source-lineage
   backup that could contain a later commit blocks restore-quarantine deletion.
   It exercises repeated live `ABANDONED_PREPARED` cleanup and the full
   `RESTORE_QUARANTINED -> DELETE_AUTHORIZED -> DELETE_ELIGIBLE` quota-recovery
   path under `TRANSITION`, including the narrowly linked EFS-mode claim, and
   completes a positive isolated restore. The restore cannot expose writers
   until complete stable inventories of all authoritative roots/MPUs classify
   later keys and every nonterminal mutation in every family. Tests prove no
   restored nonterminal row can enter `ABANDONED_PREPARED`, every stale family
   mutator is rejected, and subsequent work uses fresh promotion/domain/lease/
   writer/multipart IDs and physical keys. Ordinary runtime and Helm identities
   cannot delete an authoritative version, while the GC identity cannot create
   a delete marker, upload/copy bytes, mutate KMS/S3 policy, or run an
   unapproved workload.
6. Stored-value Helm rendering and the one-root importer, final backup, and
   isolated restore pass with zero unknown files or missing durable references;
   the routine SkyPilot release identity has negative authorization for the
   separately owned admission fence. Principal-aware negative probes close
   top-level Job/hook and alternate-controller indirection before commit. The
   same tests prove fence/application mutual exclusion and reject native
   rollback or `--atomic` on the fence release.
7. A measured backup/restore/import/restart rehearsal passes; the maintenance
   owner approves the finite RTO, rollback reserve, full irreversible-tail
   budget and component deadlines, exact pre-commit abort deadline, and both
   immutable S3-capable releases. `SEALED` denial is read back before the
   `S3_V1` commit; `S3_ONLY` is the only policy that can open afterward. One
   current cutover lease/fencing token spans the sequence, every fence mutation
   passes its fresh PostgreSQL mode/generation check, all irreversible
   prerequisites pass before commit, and every post-commit receipt lands within
   its stored irreversible-tail deadline.
8. Live horizons, role failovers, total blackout, request/Job/Serve recovery,
   no paid-capacity side effect, zero EFS I/O, and the surgical delete plan pass
   before cleanup/deletion.

## Adversarial review record

### Review 1: NO-GO

The first exact-diff review found two correctness blockers. The proposed
cutover committed `S3_V1` before narrowing Kubernetes admission, leaving a
crash window in which an old EFS workload remained admissible after the
database became one-way. It also combined finite permanent quotas with
indefinite retention, which would eventually exhaust storage and truncate new
logs. Nonblocking inconsistencies included a stale Helm baseline, a temporary
post-cutover all-role recovery path that the storage fence could not admit,
and incomplete private-bucket and direct-Helm ownership contracts.

### Review 2: provisional GO, superseded

The revised design closes the commit window with the externally owned
`TRANSITION -> SEALED -> S3_ONLY` admission protocol and permits EFS recovery
only before the PostgreSQL commit. It adds then-proposed restore-coverage-gated
exact-version GC with separate authorization and deletion identities, quota release only
after absence, and bounded metadata compaction. It refreshes the baseline to
Helm revision 434, preserves split-role fix-forward recovery, makes private
S3/KMS controls explicit, and assigns mutable fence policy to a separate
direct-Helm release rather than a platform pin or Terragrunt rollout. The
second pass also separated EFS cleanup from the independently gated API 24--40
adapter removal and made admission operation/GC workload coverage explicit.
That pass found no remaining P0/P1, but Review 3 below found additional restore
and abort-state gaps. It remains design history, not current approval or
evidence that implementation occurred.

### Review 3: NO-GO

Fresh independent review found that `DELETE_AUTHORIZED` still allowed physical
deletion before every supported database restore contained the tombstone: a
promotable backup from before `authorized_at` could therefore expect an S3
version already removed. It also found no typed, quota-recovering fate for
exact versions published by a preparation attempt that aborted before the
one-way commit. Two operational gaps remained: fence and application Helm
mutations were not mutually excluded by one lease with a fresh PostgreSQL mode
check, and the text incorrectly made every transition-capable binary unready
under `EFS_V1` rather than only an S3-configured workload.

### Review 4: provisional ready, superseded

The repaired contract separates the durable tombstone from physical
eligibility: `DELETE_AUTHORIZED -> DELETE_ELIGIBLE` requires the supported
restore set to exclude points before `authorized_at` and every promotable
backup/PITR chain to contain the authorization commit LSN. Quota remains
charged through exact absence. Every uncommitted published version now has a typed
`ABANDONED_PREPARED` adjudication, immutable attempt/manifest/key identity,
transition-admitted exact-version GC, lost-response reconciliation, and the
same absence-before-quota-release rule. One PostgreSQL cutover lease serializes
fence/application Helm phases, every fence mutation freshly checks committed
mode, and fence native rollback/`--atomic` is forbidden because a VAP cannot
self-validate its replacement. Finally, the readiness rule applies only to an
S3-rendered workload, not an EFS-rendered transition binary. Local exact-diff
and contradiction checks pass; independent re-approval and every numbered
implementation gate are still required.

### Review 5: NO-GO

The next independent re-review found two final consistency gaps. A restored
snapshot taken before an abort could still contain an apparently resumable
`PREPARED` parent and `PREPARED_PUBLISHED` children; calling its object an
immutable orphan did not durably prevent a writer from committing that attempt
before cleanup. The deletion worker also used a third retained-data origin
spelling while claim/resume and EFS-mode gates named `DELETE_ELIGIBLE`, leaving
the persisted origin contract ambiguous.

### Review 6: provisional ready, superseded

That revision added a fail-closed writer/endpoint promotion fence, fresh random
promotion IDs, restored preparation invalidation, exact-key reconciliation,
and one exact two-value deletion-origin enum. It nevertheless tried to turn
restored nonterminal preparation children into the ordinary live-abort domain.
Review 7 showed that a historical snapshot cannot supply the negative proof
needed by that transition and that the fence did not yet cover every mutation
family or later-created S3 key. Review 6 is design history, not approval.

### Review 7: NO-GO

The latest independent review found one correctness P1 and one coverage P2.
First, a selected PostgreSQL snapshot may predate a later successful commit on
the abandoned source lineage. Classifying its restored nonterminal attempt as
ordinary `ABANDONED_PREPARED` could therefore delete a version still referenced
by a valid newer backup. A historical database view is not a commit oracle.
Second, fencing only restored preparation rows missed ordinary blob/upload
children, legacy manifest/content construction, materialization leases, log
writer/segment state, and keys or multipart uploads created after the selected
snapshot. Scalar timestamp/LSN coverage and the blanket EFS rejection of
`DELETE_ELIGIBLE` also left the required safe deletion and quota-release path
incomplete.

### Review 8: READY FOR INDEPENDENT RE-REVIEW

Every restored nonterminal mutation is now invalidated under one promotion ID.
A complete stable inventory of every authoritative root discovers versions and
MPUs absent from the historical database. Recorded, present, or possibly
published versions enter charged, unreadable, non-reusable
`RESTORE_QUARANTINED`; only exact no-version/no-MPU proof enters
`RESTORE_UNPUBLISHED_ABSENT`. The family contract covers storage generations,
ordinary blob/upload parents and children, legacy graphs, materializations, and
log writer/segments, and all fresh work uses new identities and physical keys.

Restore quarantine now reaches deletion only through its typed full
`DELETE_AUTHORIZED -> DELETE_ELIGIBLE` path. Eligibility independently proves
the current-timeline tombstone is in every promotable ancestry chain and every
possible later-commit point on the abandoned source lineage is expired or
durably non-promotable. Newer valid backups protect their possible references.
The persisted deletion-origin enum remains exactly `DELETE_ELIGIBLE` or
`ABANDONED_PREPARED`; immutable quarantine provenance supplies the narrow
EFS-mode liveness rule without a third origin. This remains a PostgreSQL+S3
design with no DynamoDB, EFS steady state, new service, Terraform expansion, or
external commit oracle. Local exact-diff checks and an independent re-review
are still required; no implementation or deployment is implied.

Completion means production runs 2/2/2 with PostgreSQL structured authority,
exact-version SSE-KMS S3 blobs/logs, bounded `emptyDir`, a healthy restore-safe
authorization/eligibility/GC coverage path, zero unreconciled prepared
attempts, and no SkyPilot PVC, access point, filesystem mount, path fallback,
or guarded-HA EFS support code. All paired cleanups and the exact infrastructure
deletion are merged, while unrelated generic storage is unchanged.
