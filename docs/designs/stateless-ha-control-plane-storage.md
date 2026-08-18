# Stateless HA control-plane storage

Status: Proposed canonical design. The production inventory is verified, but
no application code, infrastructure, migration, or deployment has been
performed from this design. Implementation starts only after Gate 1.

Last updated: 2026-08-18

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

This design adds no database product, queue, cache, custom controller, CRD,
admission webhook, secret system, or always-on Deployment. It does not require
Object Lock, a general object-lifecycle framework, a configuration-generation
subsystem, or a repository for arbitrary file projections.

## Production baseline and gap

Read-only evidence captured on 2026-08-18 shows:

- Helm revision 432 runs SkyPilot 1.1.1339 from commit
  `61449bc2ecfeb179959351f001c584742108d720`, image digest
  `sha256:9c4b927520b09889e264d86fddcd1556a884404adad87d83ec006bc50e402e14`.
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

What is missing is deliberately narrow:

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
   the exact verified version before making it readable.
4. S3 keys are unique and never overwritten. Readers use exact versions and
   digests. Prefix listing is diagnostic only, except that multipart recovery
   may enumerate the one exact attempt key under the fenced upload contract.
5. No PostgreSQL lock is held across an S3 call. Lost responses are reconciled
   from the domain row and exact key, never by blind replay.
6. No durable blob or log authority consists only of a pod-local path. A
   compatibility path hint may remain only when its PostgreSQL content/object
   reference is authoritative and the path is regenerated. Materialization
   and spool paths themselves are valid only inside their scoped owner.
7. EFS and S3 are never simultaneous authorities. Production does not dual
   write.
8. Missing log bytes are represented by an ordered PostgreSQL gap. After the
   PostgreSQL stream/fence admission succeeds and the provider action begins,
   a log failure never replays or blocks that provider operation.
9. Unknown source files, unresolved credentials, missing owner references, or
   an incomplete restore proof block cutover.
10. After `S3_V1` commits, EFS rollback is forbidden; recovery is fix-forward
    on PostgreSQL and S3.
11. A blob reference and a delete claim serialize on the same PostgreSQL blob
    row. No request, Job, or Serve version can attach a blob after deletion is
    claimed.
12. An upload, blob publication, or log publication can have only one current
    attempt generation and lease. A stale pod cannot publish after takeover.

## Storage generation and fingerprint

A singleton PostgreSQL row stores:

- monotonic generation, mode, and minimum reader/writer protocol;
- the canonical S3 fingerprint;
- prepared and committed timestamps;
- the source PV/PVC/access-point identity and final manifest digest;
- the final migration counts and PostgreSQL snapshot receipt; and
- the backup, isolated-restore, Helm release, and operator receipts.

There may be one higher prepared generation. Prepared blob/log rows are
invisible to ordinary readers. A serializable transaction validates the final
receipts and changes the singleton from `EFS_V1` to `S3_V1`. The committed mode
cannot move backwards.

This is the only new storage/configuration generation. Bucket, prefix, KMS, and
mode are not independently mutable Helm switches.

After cutover, disaster recovery uses the existing PostgreSQL backup/PITR and
S3 versioning. A restored database is promoted only after a bounded verifier
confirms every referenced exact S3 version; this is an operator restore step,
not a permanent coverage worker.

## Blob and upload contract

The existing `BlobStorage` seam becomes object-aware rather than adding a
parallel repository. Its durable domain tables are:

- `file_mount_blobs`: authenticated owner, `blob_id`, generation, exact S3
  reference, byte count, archive and logical-tree digests, upload state,
  retention deadline, reference/delete state, and a mandatory deletion claim
  token whenever state is `DELETE_CLAIMED`; and
- `file_mount_uploads`: authenticated owner, upload identity, part count,
  deterministic object key, attempt generation, lease token/expiry, received-
  part checksums/ETags, multipart ID, expiry, and final blob ID.

The blob identity is the composite `(authenticated_owner_id, blob_id)`, and
that composite reference is copied through request, Job, and immutable Serve
version rows. Equal content from different owners does not collapse their
authorization boundary. `created_by`, display names, and request submitter text
are not storage-owner identities.

An upload advances `PREPARING -> UPLOADING -> PUBLISHING -> READY`. The server
creates `PREPARING`, its unique object key, and attempt generation before S3
I/O. A leased owner creates or discovers the one multipart upload for that
exact key and persists the returned multipart ID before uploading a part. A
lost create response is reconciled with `ListMultipartUploads` only for the
persisted unique attempt key. Exactly one upload whose initiation belongs to
that fenced attempt may be adopted; zero remains absent, and multiple or
unprovable results abort or quarantine the attempt rather than choosing one.
It is never handled by blindly creating a second upload. Each received HTTP
chunk is bounded-spooled and checksummed, and its intended part number/checksum
commits before S3 I/O. `ListParts` reconciles a lost part response before the
row records its ETag.

Finalization leases and CASes the row to `PUBLISHING`, then calls
`CompleteMultipartUpload` with the persisted ordered part set. Because an
attempt key is never overwritten, a lost completion response is reconciled by
`HEAD` of that exact key, including version ID, attempt metadata, size, digest,
and SSE-KMS identity. Absence resumes or aborts the recorded multipart upload;
presence never triggers a second object creation. Only the same attempt
generation can continue verification. Before `READY`, the server streams the
exact returned version, recomputes the archive digest and logical-tree digest,
rejects traversal, links or special types outside the contract, expanded-byte
and inode-limit violations, and requires the logical-tree digest to equal the
client `blob_id`. `HEAD` metadata alone never proves integrity. Only then can a
CAS publish the exact version as `READY`. Lease expiry lets another API pod
reconcile the same state machine, so chunks and finalization may reach
different API replicas without creating competing authority.

API 41+ preserves the existing client `blob_id` algorithm, archive canonical
form, and supported safe-symlink semantics exactly. Server verification uses
golden archives from current clients; it rejects escapes and unsupported file
types without redefining the digest or silently changing a valid archive.

API 41+ keeps its existing wire contract unchanged: `GET /upload_v2/blob`,
`POST /upload_v2`, the content-addressed `blob_id`, and the subsequent request
payload all remain compatible. There is no API 91 and no separate API 41--90
adapter. The existence check preserves its retention side effect: it locks the
owner's `READY` blob row and commits a bounded attachment grace extension
before returning true. The extension is capped by the configured maximum
unattached lifetime and cannot resurrect `DELETE_CLAIMED`; request admission
still locks that same row to attach the durable composite reference. Thus GC
cannot claim between a committed true response and its bounded grace window.

API 24--40 keeps `/upload`. Because those clients cannot bind a blob ID in the
next request, PostgreSQL permits one completed, unclaimed legacy upload per
authenticated user. The next request from that user with a nonempty
`file_mounts_mapping` claims it, injects its blob ID, and persists the request
reference in the same request-admission transaction. A second concurrent
upload, missing/expired slot, ambiguous retry, unauthenticated remote caller,
or cross-user claim fails closed. The adapter remains until
`MIN_COMPATIBLE_API_VERSION > 40`. The explicit single-process local backend
keeps its local upload behavior outside guarded HA; it does not create an
anonymous production slot. This is the only legacy adapter.

The request transaction continues to retain `file_mounts_blob_id`. Managed Job
registration continues to copy it into the existing Job row before the request
reference can expire. Serve creation/update persists the canonical
`file_mounts_mapping` and blob ID on the immutable Serve version before
releasing the request reference. It never persists the translated pod-local
absolute paths used by `core.up` or `core.update`. Recovery rematerializes the
blob and translates a short-lived copy of the task only inside the fenced
controller action.

Every request, Job, or Serve reference insertion locks the blob row and may
commit only while it is `READY`. GC locks the same row, rechecks all three
reference domains and the retention deadline in that transaction, and CASes
`READY -> DELETE_CLAIMED` with a unique claim token. New references then fail
closed. Outside the transaction, the claimant deletes only the recorded S3
version. A lost delete response is reconciled by checking that exact version:
absence lets the same token commit `DELETED`; presence retries the same exact
deletion. An error or ambiguous observation leaves `DELETE_CLAIMED` for fenced
reconciliation and is never treated as success. Orphan multipart attempts and
objects that never reached `READY` use the same attempt-generation discipline
and retention delay rather than reference-based GC.

Permanent deletion is additionally restore-fenced. At claim time GC records
the backup system's verified oldest PostgreSQL-restorable timestamp and may
delete a blob version only when every database point that could still contain
a reference has aged out, plus the configured restore safety horizon. If that
receipt is absent or stale, deletion stops. `READY` blob versions and
`COMMITTED` log versions have no independent age-based S3 expiration, current
or noncurrent: permanent deletion is exclusively the PostgreSQL row-locked GC
claim above. Bucket lifecycle may abort stale incomplete multipart uploads and
remove a separately classified non-authoritative orphan class, but it cannot
expire a committed namespace. A future tag protocol cannot weaken this unless
it proves the same reference/PITR serialization first.

`materialize(blob_ref)` is a context-managed capability. It downloads the
exact version, verifies bytes and digest, safely extracts into a private
`emptyDir` directory, and returns that path only for one executor subprocess,
controller action, or synchronous API scope. Cancellation, fence loss, scope
exit, or pod death removes the path. Retry rematerializes; no extracted tree is
durable authority.

## Common log contract

One common library and schema replace all control-plane log paths; this does
not introduce another service:

1. A local spool accepts bytes from request, managed-Job, Serve, and ordinary-
   cluster provisioning/history writers.
2. A shipper publishes immutable S3 segments.
3. PostgreSQL indexes stream ownership, writer fence, ordered segment versions,
   byte offsets, newline counts, terminal state, and explicit gaps.
4. One reader implements stream, follow, tail, and download from that index.

The domain tables are `control_plane_log_streams` and
`control_plane_log_segments`; gaps are typed segment rows, not a separate
generic object model. Before a child/provider action starts, PostgreSQL creates
the stream, a writer generation/lease, and a durable open-coverage marker.
That marker ensures an expired writer is distinguishable from a clean terminal
stream even when PostgreSQL and the pod disappear together.

For each segment, the writer reserves its sequence and byte interval and
commits `PREPARING`, unique key, digest, and writer generation. The same leased
generation CASes `PREPARING -> PUBLISHING` before S3 I/O, then CASes
`PUBLISHING -> COMMITTED` with the exact version. A lost write response is
reconciled by `HEAD` of the unique key and digest. A successor locks the stream
row, advances the writer generation, reconciles predecessor
`PREPARING`/`PUBLISHING` rows, resumes at the last contiguous committed offset,
and converts any unprovable interval into a gap. A stale generation cannot
publish or close the stream.

A terminal barrier records the last proven offset and closes the open-coverage
marker. If spool overflow or hard pod loss occurs while PostgreSQL is
unavailable, takeover observes the expired generation plus the still-open
marker and commits an `UNKNOWN_TAIL` gap from the last proven offset; it never
reports completeness or invents an exact end offset. When counters survive,
the gap records the exact lost interval. The reader emits an unmistakable
marker for either form of gap or quota truncation and never concatenates
noncontiguous bytes as if they were complete. Expired attempt keys are deleted
only after their stream-row generation proves they cannot become visible;
prefix listing remains diagnostic and never establishes ownership.

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

Domain success and log completeness are separate. A Job or Serve action can
finish with incomplete logs; the UI/API reports that state. Recovery may ship
or reconcile the recorded spool/attempt, but never repeats the provider action.
Process diagnostics continue to use container stdout and the existing external
collector, not this user-log store.

Terminal stream retention is explicit on the stream row. Committed segment
versions use the same row-locked delete claim and exact-version reconciliation
as blobs. They cannot be deleted until their domain retention has elapsed and
the oldest PostgreSQL restore point that could reference them has aged out by
the safety horizon. A typed gap needs no S3 object and remains in PostgreSQL for
the stream's metadata-retention period. Request, managed-Job, Serve, and
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
policies or bindings; a separately audited platform identity owns the fence,
with cluster-admin retained as explicit break-glass. Negative `auth can-i`
proof is a cutover receipt.

The policy matches the control-plane's exact namespace, names, and
ServiceAccounts and requires the `S3_V1` protocol/fingerprint annotations,
digest-pinned images, and absence of PVC/EFS volumes. It is an external rollback
fence, not a webhook or service. The transition policy initially admits the
audited live `EFS_V1` release and the prepared `S3_V1` release; while
maintenance is still active, its owning release narrows it one-way to `S3_V1`
before the source claim is removed. Every retained Helm revision is rendered
and submitted server-side to prove it is denied. Future releases preserve this
protocol annotation and satisfy the same policy.

The existing API, executor, and controller ServiceAccount arrangement remains;
the migration does not split it into new permanent identities. Existing
workload identity receives only the S3/KMS actions needed by the roles it
already serves. At most one temporary migration ServiceAccount/identity may
read the source claim and write the prepared S3 prefix; it is removed by the
cutover cleanup. There is no permanent migration, retention, audit, or GC
identity.

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
that `boltz-l4-fleet` recovery causes no provider mutation.

The production cutover is one ordered maintenance operation:

1. Deploy the transition image in `EFS_V1` with direct Helm
   `upgrade --reuse-values`; prepare schemas and shadow S3 objects without
   changing read or write authority. Install the separately owned transition
   admission-fence release and prove the routine SkyPilot release identity
   cannot alter it.
2. Enter a PostgreSQL maintenance gate, reject new mutating requests/uploads,
   drain or classify existing work, and scale all six role pods to zero.
3. Prove no pod, Job, debug process, or node mount is writing the claim. Run the
   migration Job with the claim read-only and finish the one-root import.
4. Capture the final manifest. Start a new backup after zero-writer proof,
   restore it to an isolated filesystem/access point, and compare the restored
   manifest exactly with the source manifest.
5. Verify every live request, Job, Serve version, cluster-log reference, and
   indexed log resolves to a committed exact S3 version or an explicit typed
   gap; verify the prepared fingerprint, PostgreSQL snapshot, migration counts,
   and no blocking unknowns.
6. Commit `S3_V1` in the serializable generation transaction.
7. Immediately narrow the separately owned admission fence to `S3_V1` before
   any SkyPilot Helm Deployment update. Admission does not evict existing EFS
   Deployment objects or any pods still terminating, but every post-commit
   old/EFS Deployment write now fails.
8. Deploy the no-PVC chart directly with Helm `upgrade --reuse-values`, pinned
   to the merged immutable image/chart digest, and restore 2/2/2 readiness.
9. While maintenance still blocks mutations, delete the one PVC and PV under
   their verified `Retain` contract. Preserve their exact manifests and retain
   the CSI-created
   access point, filesystem, final backup, and isolated restore. Prove the six
   healthy pods have no EFS mount and server-side-submit every retained
   historical Helm revision to prove the admission fence rejects it, including
   revisions that would dynamically create a new empty PVC. Only then leave
   maintenance.

No `boltz-platform` SkyPilot pin is updated. SkyPilot fixes merge forward,
publish an immutable overlay/chart, and deploy directly through the existing
Helm release. Infrastructure PRs contain only the S3/KMS/IAM additions or the
later exact EFS deletions; they do not carry application configuration.

Before step 6, abort stops the migration, reconciles prepared S3 data, and
restarts the unchanged EFS generation. The live claim has not been deleted.
After step 6, Helm rollback to an EFS image or values is forbidden. The
maintenance gate covers the short commit/fence/deploy/unbind interval. After
step 7, the external admission policy rejects historical charts independently
of whether they reference the deleted claim, create a fresh empty claim, or
ignore `S3_V1`. Cluster administrators can change admission policy, so release
ownership and review still prohibit weakening the fence or manually combining
an old image with invented values. Repair deploys a newer S3-capable image or a
higher PostgreSQL storage generation.

Immediate, +10-minute, +30-minute, +24-hour, one-pod-per-role, and total
role-blackout acceptance start the approved recovery horizon. After that
horizon and the cutover cleanup, the infrastructure-delete PR removes the
retained CSI-created access point through the AWS API; the access point is not
Terraform-owned and deleting it alone does not delete its directory. If the
filesystem is proven exclusive, the saved Terraform plan then deletes the
filesystem (and its contents), mount targets, security group, CSI/IAM edges,
and StorageClass. If it is shared, a one-time Job mounted only through the exact
retained access point deletes the verified SkyPilot source directory after the
backup/restore hold, then the AWS API deletes that access point; shared
filesystem resources remain. The PVC/PV were already unbound in step 9.
Unrelated edges and the required isolated backup/restore evidence are retained.

## Compact implementation stack

This initiative has exactly nine PRs. Each transition and its cleanup are
authored together and cross-linked; cleanup stays draft until its gate.

| # | PR | Scope and merge gate |
| --- | --- | --- |
| 1 | Design | This file and the concise supersession note in the role-split design; merge after adversarial approval |
| 2 | Infrastructure add | Prefer an existing server-owned private versioned bucket/prefix when ownership, KMS, no-expiry committed namespaces, multipart/orphan lifecycle, and negative IAM probes pass; otherwise add one bucket, KMS, narrow IAM, alarms, and optional temporary migration identity; no live EFS change |
| 3 | Blob transition | Domain tables, S3 `BlobStorage`, unchanged API 41+, API 24--40 per-user adapter, Serve refs, scoped materialization |
| 4 | Blob cleanup | Remove remote shared-path blob backend and legacy path GC after S3 acceptance and blackout |
| 5 | Log transition | Fenced prepare/publish/commit spool, shipper, PostgreSQL index/gaps, takeover/terminal recovery, S3 reader, and all request/Job/Serve/ordinary-cluster integrations; no partial activation |
| 6 | Log cleanup | Remove shared-path writers/readers and fallback after gap, failover, and retention acceptance |
| 7 | Cutover transition | Generation/fingerprint, one-root importer/restore verifier, bounded `emptyDir`, stored-value migration, no-PVC Helm mode, and separately owned one-way admission-fence chart/release |
| 8 | Cutover cleanup | Remove `EFS_V1`, importer, old storage values/templates/mounts, and temporary migration identity after the recovery horizon |
| 9 | Infrastructure delete | Explicitly delete the retained dynamic access point and apply a saved surgical plan deleting only proven SkyPilot EFS resources; merge after application cleanup and restore proof |

The blob and cutover transitions are substantial domain changes; the common
log transition is the largest because it replaces several writers and readers.
Each stays disabled until its complete domain integration and fault suite land;
there is no partial production activation. The paired cleanups and
infrastructure PRs are smaller and mechanically bounded. No PR adds a new
service or broad Terragrunt refactor.

The unmerged RWX stacks in boltz-platform PRs #7824 and #7829--#7833 and the
draft design PR #8443 are not implementation inputs; PR 1 supersedes and closes
them. PR 9 removes only the live EFS resources introduced by merged PRs #8596
and #8601 after the gates above. It does not revert unrelated platform history
or update a SkyPilot pin.

## Verification and open gates

Automated tests must cover:

- real PostgreSQL/S3 crashes at every blob prepare/publish/commit boundary,
  multipart resume, response loss, exact-version verification, reference/GC
  races in both lock orderings, stale lease/attempt rejection, and cross-user
  denial, plus PITR across a later GC claim;
- unchanged API 41+ clients, API 24--40 serialization/claim/expiry, concurrent
  uploads, archive traversal/expansion limits, Serve-version retention, and
  materialization cancellation/pod loss;
- common log prepare/publish/commit ordering, writer takeover fencing,
  follow/tail/download, lost S3 responses, simultaneous S3/PostgreSQL outage,
  quota truncation, hard-pod-loss `UNKNOWN_TAIL`, terminal barriers, terminal
  retention/GC and PITR, orphan cleanup, cluster provision-log migration,
  UTF-8/partial lines, and proof that provider actions are neither blocked nor
  replayed;
- exact revision-432 stored values through successive Helm `--reuse-values`
  renders, 2/2/2 RollingUpdate behavior, bounded ephemeral storage, and a final
  manifest with no PVC or EFS mount, plus server-side denial of every retained
  pre-cutover Helm revision; and
- one-root classification, repeatable import, final backup timing, isolated
  restore equality, pre-commit abort, post-commit fix-forward, and a saved
  delete-only then empty infrastructure plan.

Production metrics extend existing telemetry with storage mode/generation/
fingerprint, blob upload/GC state, materialization bytes, log segment age,
spool utilization, gap/truncation counts, S3/KMS errors, migration counts, and
EFS I/O until deletion. They contain no object keys, paths, credentials, or
signed URLs.

Open gates are:

1. Storage, database, Serve/Jobs, and platform owners approve this exact
   design and its nine-PR boundary.
2. A fresh audit reconfirms the single source handle, approximately 23.82-GB
   inventory, exact infrastructure addresses and ownership, existing
   ServiceAccounts/IAM, maximum archive sizes, and log rates.
3. The infrastructure-add plan and positive/negative S3/KMS identity probes
   pass without changing EFS; bucket lifecycle cannot expire `READY` blob or
   `COMMITTED` log versions independently of PostgreSQL GC.
4. Blob and log transitions pass real PostgreSQL/S3 fault tests and an isolated
   no-EFS controller/`boltz-l4-fleet` recovery rehearsal.
5. Stored-value Helm rendering and the one-root importer, final backup, and
   isolated restore pass with zero unknown files or missing durable references;
   the routine SkyPilot release identity has negative authorization for the
   separately owned admission fence.
6. The maintenance owner approves the outage and the exact pre-commit abort
   point; all final cutover receipts pass before `S3_V1` commits.
7. Live horizons, role failovers, total blackout, request/Job/Serve recovery,
   no paid-capacity side effect, zero EFS I/O, and the surgical delete plan pass
   before cleanup/deletion.

Completion means production runs 2/2/2 with PostgreSQL structured authority,
exact-version SSE-KMS S3 blobs/logs, bounded `emptyDir`, and no SkyPilot PVC,
access point, filesystem mount, path fallback, or EFS support code. All paired
cleanups and the exact infrastructure deletion are merged, while unrelated
storage is unchanged.
