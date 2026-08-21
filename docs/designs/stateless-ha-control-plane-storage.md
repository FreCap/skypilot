# Stateless HA control-plane storage

Status: Proposed canonical design, source contract complete. Application
support, infrastructure, migration tooling, and the production cutover are not
implemented. This document does not authorize a deployment or deletion. It may
merge after independent review of its exact diff; implementation and
production mutation remain separately gated.

Last updated: 2026-08-21

Canonical owner: this file owns durable control-plane bytes and removal of the
SkyPilot EFS dependency in guarded HA. The API/executor/controller split,
PostgreSQL request delivery, controller leadership, and Serve actuation remain
owned by docs/designs/multi-replica-api-server.md. Reserved-capacity placement
remains owned by docs/designs/serve-multi-pool-reserved-capacity-fill.md.

## Decision

Guarded production HA has one steady-state storage path:

- PostgreSQL is the sole structured and transactional authority.
- A private, dedicated, versioned server-owned S3 bucket stores immutable
  upload archives and durable log segments with SSE-KMS.
- PostgreSQL-fenced upload sessions let chunks land on any API replica and map
  them to one conditional S3 multipart upload. No upload requires pod affinity,
  a sticky load balancer, or a shared staging directory.
- PostgreSQL stores each exact bucket, key, version ID, byte count, SHA-256
  digest, ownership record, and reference. Prefix listing is never authority.
- Bounded disk-backed emptyDir volumes hold upload parts, materializations,
  generated files, and log spool. Those paths are disposable.
- Kubernetes Secrets and projected workload identity provide credentials and
  externally managed SSH material.
- Guarded HA has no SkyPilot PVC, EFS mount, shared path, FUSE mount, or
  filesystem fallback.

Version 1 deliberately does not delete authoritative S3 objects. Runtime and
migration identities receive no object-delete permission. This makes database
restore safe without a backup-source registry, tombstone-age theorem, restore
farm, old-incarnation sweeper, admission webhook, or week-long storage
observer. Object retention and garbage collection are a later, independently
measured design.

The live EFS filesystem reports about 25.41 GB used, while PostgreSQL already
contains the structured state. Bulk archives and logs therefore move to S3,
not into Aurora.

## Goals

- Remove EFS as a correctness, availability, recovery, and support dependency
  for the production 2/2/2 control plane.
- Preserve supported `/upload_v2`, request-log, managed-job, Serve, cluster,
  config, and generated-SSH behavior across total pod loss.
- Make every durable opaque byte immutable, content-verified, and referenced by
  an exact PostgreSQL row.
- Preserve parallel chunk upload through two or more API replicas without
  cross-pod filesystem assembly.
- Give every temporary local byte an explicit size, inode, and lifetime bound.
- Perform one evidence-backed, fix-forward cutover with no parallel production
  storage path afterward.
- Remove only exact SkyPilot-owned EFS resources after the new path is proven.

## Non-goals

- This design does not change reserved-fill placement, demand calculation,
  paid-capacity residuals, Kueue, or worker-node health.
- It does not add KubeRay, S3 FUSE, a second database, a cache service, a CRD,
  an admission webhook, or an always-on migration service.
- It does not put opaque archives or log payloads into PostgreSQL.
- It does not preserve EFS as a post-cutover rollback path.
- It does not require a boltz-platform SkyPilot runtime pin or a broad
  Terraform/Terragrunt refactor. SkyPilot application changes merge forward and
  deploy through direct Helm.
- Generic standalone/local and explicitly non-HA user-supplied storage remain
  separate compatibility surfaces. They cannot satisfy guarded HA.
- Completed-object/version deletion, completed-object lifecycle expiration,
  and storage-cost optimization are not part of version 1. Aborting an exact
  recorded incomplete multipart upload, including the bounded incomplete-part
  lifecycle below, is permitted because those parts are not published objects.

## Current production evidence

Read-only evidence refreshed on 2026-08-21 at approximately 10:10 UTC shows:

- Helm revision 478 runs immutable image 1.1.1407 at one digest as two API, two
  executor, and two controller pods; all six were Ready.
- All six roles use PostgreSQL but still mount skypilot-state-rwx at
  /root/.sky, /root/.ssh, and /root/sky_logs.
- Guarded HA currently rejects storage.enabled=false and requires
  storage.accessMode=ReadWriteMany.
- The claim uses storage class efs-rwx and access point
  fsap-027d9430f450bb777 on fs-00a7dd95ad52c0ade. The access-point path is
  /dynamic_provisioning/pvc-8001cb94-a060-402c-be6a-7899d9dd972c.
- The mounted filesystem reported 25,412,239,360 used bytes. A bounded `du`
  did not finish, so that is filesystem-level usage rather than an assertion
  that every used byte belongs to the access-point root.
- The filesystem is tagged as shared Kubernetes state even though the current
  inventory found one access point. Deleting the SkyPilot claim and access
  point does not by itself prove that the base filesystem and mount targets are
  exclusive.
- The Rainier service account uses EKS Pod Identity role
  `arn:aws:iam::255203429798:role/skypilot-api-boltz-platform-gitops-hub-rainier-eks-cluster`.
  No `skypilot-*` S3 bucket exists in that account. Its current inline policy
  broadly permits create/get/put/delete on `skypilot-*`, so it is not the
  authoritative-object policy in this design. D9 must add one surgical
  dedicated bucket/KMS boundary whose bucket policy explicitly denies runtime
  and importer deletion of completed objects/versions and enforces the exact
  conditional-write contract; no broad infrastructure expansion is justified.

This evidence must be refreshed immediately before migration. A second
SkyPilot authority root, an unreadable path, or an unclassified retained
reference blocks cutover.

## Source readiness and gaps

This source audit is against `improvements` at 008316be2 (two commits after
1.1.1409). Later implementation PRs must refresh it after rebasing.

Literal EFS removal is not source-ready. The current seams and missing work are:

| Surface | Already authoritative | Still filesystem-dependent | Required change |
| --- | --- | --- | --- |
| API requests | PostgreSQL request, queue, lease, retention-pin, and terminal state | Upload bytes and request logs | S3 object references inserted in the owning request transaction and durable log segments |
| Upload blobs | Current clients compute a content SHA-256 and send parallel `/upload_v2` chunk requests; request/job rows retain the logical blob ID | `LocalFilesystemBlobStorage` is the only backend; chunks rely on a shared staging directory, existence/GC use paths and mtimes, the server does not verify that bytes match the claimed blob ID, resolve returns a permanent path, and expired `/upload` clients write an uncorrelated mutable per-user tree | PostgreSQL upload sessions, direct conditional S3 multipart publication, verified logical aliases, owner references, scoped materialization, and removal of `/upload` at the S3_V1 boundary |
| Managed Jobs | DAG, environment, config, and original YAML are stored in PostgreSQL | Legacy disk fallbacks, blob bytes, controller/task logs | Validate retained rows, remove guarded-HA fallbacks, use S3/log provider |
| Serve | Version YAML, submitted YAML, placement, controller snapshots, and recovery scripts are in PostgreSQL | Up/update staging, controller files, replica launch artifacts, and logs | Transactional admission payload, local reconstruction, S3 logs |
| Server config | Canonical server config and Casbin policy are in PostgreSQL and database-backed Helm installs reject inline config | Startup still seeds and synchronizes ~/.sky/config.yaml through shared storage; workspace mutation uses an EFS policy lock and independently commits config and authorization | Seed an empty PostgreSQL row once, read PostgreSQL directly, atomically commit workspace config/policy/cache invalidation, and use only optional pod-local projections |
| Generated SSH | Generated per-user keypairs and cluster YAML are in PostgreSQL | /root/.ssh is shared; SSH-node-pool uploads use local key paths | Local regeneration; externally supplied keys only from projected Secrets |
| Caches and scratch | Derivable from durable state | Catalogs, locks, wheels, generated YAML, debug files, and request stages use shared roots | Bounded emptyDir only |
| Helm | Role split and PostgreSQL guards exist | Every HA role mounts one RWX claim and HA hard-fails without it; API/controller/executor/image-worker renders also retain an unused `skypilot-config` file dependency | D1 removes every guarded-HA config ConfigMap/env/mount/volume; D7 produces the PVC-free render with bounded ephemeral storage |

The existing BlobStorage abstraction is path-oriented and has no S3
implementation. The existing LogProvider abstraction reads local files but
does not own durable writing or indexing. They are useful seams, not completed
object-store support.

The current client submits chunks concurrently, and those HTTP requests may be
load-balanced across both API replicas. Replacing the shared staging directory
with one replica's emptyDir would therefore be incorrect. The steady-state
upload path sends each verified chunk to one durable multipart session; local
disk is a bounded per-request spool, never the cross-request rendezvous.

## Final topology

### PostgreSQL

PostgreSQL owns:

- the active storage protocol and generation;
- object metadata and exact immutable S3 identity;
- object ownership and references from requests, jobs, services, versions, and
  clusters;
- log-stream identity, ordered segment metadata, terminal markers, and typed
  gap records;
- migration-operation provider intent, inventory, item result, manifest digest,
  and cutover receipt; and
- all existing request, queue, job, Serve, config, Casbin authorization,
  permission-cache generation, cluster, and generated-SSH structured state.

One additive schema migration creates the storage generation and
object/log-catalog tables. A reference and its owning domain mutation commit in
one PostgreSQL transaction. An S3 upload that never receives a PostgreSQL
reference is harmless retained garbage in version 1.

PostgreSQL never treats an S3 prefix scan, local directory, object timestamp,
or Helm value as durable truth.

### PostgreSQL schema and transaction boundary

The additive PostgreSQL-only lineage contains these logical records. Exact
column names may follow repository conventions, but the keys, state machines,
and constraints are contract, not implementation suggestions:

- one storage-authority row with protocol (`LEGACY_EFS` or `S3_V1`), monotonic
  generation, database-incarnation UUID, and the committed cutover receipt. An
  `S3_V1` row also binds one immutable provider-target tuple: provider and
  region, bucket name, expected bucket-owner account, server-owned key root and
  key-schema version, KMS key ARN, required `Enabled` versioning and
  bucket-owner-enforced ownership modes, conditional-write policy-contract
  version, incomplete-multipart lifecycle horizon, and the digest/operation ID
  of the qualified policy probe. Changing that tuple requires a new storage
  generation; a Helm value or environment variable can only advertise a
  capability that exactly matches it and can never select the target;
- migration-operation intents keyed by an immutable operation ID, with a
  compare-and-swapped per-installation intent head and immutable source
  installation and EFS identity, source-manifest digest, source protocol/
  generation/incarnation, candidate generation, complete provider-target tuple,
  and qualified policy-probe receipt. Binding fields never change. Receipt-
  backed operation state advances only
  `PREPARED -> IMPORTING -> VERIFIED -> ACTIVATED`, or to terminal `ABANDONED`;
  operation-scoped provider-call authorizations and receipts record terminal or
  quiescence evidence, and only one nonterminal intent may own a source/
  candidate pair;
- logical upload heads unique on
  `(storage_generation, tenant_id, object_kind, logical_blob_id)`, containing a
  monotonic session epoch, current session ID, authority incarnation, and
  monotonic head-fence epoch. The head is the only mutable rendezvous pointer;
  advancing it requires a locked compare-and-swap from the exact terminal
  predecessor and current storage authority;
- upload sessions keyed by a server-generated UUID and unique on
  `(upload_head_id, session_epoch)`, with expected chunk count, immutable
  per-session limits including the idle timeout, authority generation and
  incarnation, database-clock `created_at`, `last_durable_progress_at`, and
  `idle_deadline_at`, lease owner/epoch/expiry, session-fence epoch, current
  attempt epoch, state, optional predecessor session, and terminal
  reason/time/fence receipt.
  Concurrent first chunks converge on the current row. An active conflicting
  chunk count or limit set is rejected. A terminal pre-publication session
  remains immutable audit evidence but may be superseded by one freshly fenced
  session epoch; concurrent successor creation converges through the upload-head
  compare-and-swap. Every multipart-create, part, and finalizer provider-call
  lease expiry is constrained to be no later than `idle_deadline_at`;
- upload attempts unique on `(upload_session_id, attempt_epoch)`, with a fresh
  authority generation/incarnation and provider-target identity, object
  UUID/key, S3 multipart upload ID and creation receipt, multipart-cleanup
  state/receipt, and terminal outcome. A rejected or conflicted immutable
  attempt is retained and a successor attempt never reuses its key;
- upload-part receipts unique on
  `(upload_session_id, attempt_epoch, part_number)`, containing exact byte
  count, part checksum, ETag, and provider-call lease epoch and expiry;
- immutable objects keyed by object UUID, containing the exact S3 identity and
  verification data below, tenant, logical object kind, representation
  encoding, origin (`NEW_UPLOAD` or `EFS_IMPORT`), and reference state;
- logical aliases unique on
  `(storage_generation, tenant_id, object_kind, logical_blob_id)`, so retries
  and content reuse resolve to one authorized object without cross-tenant
  deduplication;
- owner references unique on
  `(owner_kind, owner_id, owner_role, object_id)`; and
- log streams, segment/gap records, and migration receipts described below.

An upload session follows only:

`ALLOCATED -> MULTIPART_ACTIVE -> PUBLISHED`

or a terminal `ABORTED` result before publication. An individual attempt may be
`ACTIVE`, `COMPLETED_UNVERIFIED`, `SUCCEEDED`, `REJECTED`, or `ABORTED`; only a
verified `SUCCEEDED` attempt may advance its session. Publication creates the
immutable object with reference state `PUBLISHED_UNREFERENCED`, creates the
logical alias, and marks the session `PUBLISHED` in one PostgreSQL transaction.
The object state is deliberate: the existing upload API finishes before the
later request, Managed Job, or Serve admission has an owner row. A successfully
published but never referenced object is retained garbage in version 1, not an
owner and not live work.

`ABORTED` is terminal only for that immutable session epoch, not for the
logical digest forever. If no logical alias was published, a later first chunk
may lock the upload head and atomically insert one successor session with a
higher epoch and new immutable chunk plan/limits. If the predecessor is still
active, a conflicting plan is rejected; if an alias is already published, the
alias wins and no successor is created. Terminal session/attempt protocol state,
immutable identity, and audit fields are never rewritten or reused; only their
explicit receipt-backed multipart cleanup state may advance after fencing.

An active session cannot hold the logical head forever. Its idle deadline moves
only in the same PostgreSQL transaction that records durable progress: the
multipart-creation receipt, a new matching part receipt, or a finalization
state receipt. An existence check, duplicate/conflicting request, HTTP retry,
lease acquisition/renewal, or failed provider call does not refresh it. The
idle timeout and resulting deadlines use the PostgreSQL clock and are fixed by
the session's admitted limits; pod-local clocks and S3 timestamps are not
authority.

Provider-call lease acquisition and renewal lock the current session and use
database time. They are rejected when database time is at or past
`idle_deadline_at`; otherwise the committed expiry is
`min(database_now + lease_duration, idle_deadline_at)`. Renewal cannot move the
idle deadline or extend a lease beyond it. Durable progress may advance the
deadline only through the receipt transaction above. Thus even a live or stuck
owner cannot renew forever, and terminalization at or after the idle deadline
waits only for a lease whose maximum expiry is that same deadline.

The first-chunk allocation path owns the correctness transition. While locking
the logical head and current session, it may compare-and-swap an unpublished
`ALLOCATED` or `MULTIPART_ACTIVE` session to `ABORTED` only after database time
has reached its idle deadline and no multipart-create, part, or finalizer lease
is unexpired. Any expired/ambiguous provider-call lease first rejects the exact
attempt. The same transaction terminalizes any current nonterminal attempt,
sets multipart cleanup `PENDING` when an exact upload ID was recorded,
increments both head and session fence epochs, and records `IDLE_TIMEOUT`,
`aborted_at`, and the exact predecessor/attempt receipt before a successor can
be inserted. The existing bounded blob-maintenance loop may call the same
repository transition with `FOR UPDATE SKIP LOCKED` to reduce abandoned
multipart cost, but it is not required for liveness and introduces no second
state machine or daemon. On-access compare-and-swap remains sufficient when
maintenance has not run.

Every part receipt, finalizer result, immutable-object insertion, alias
insertion, and session transition re-locks and validates the exact authority
protocol/generation/incarnation, upload-head current session/epoch, active
head/session fence epochs, attempt epoch, and provider-call lease. If abort or
successor creation wins first, a late old request or provider response can at
most leave bytes under the old random key; it cannot record a receipt, publish
an object or alias, or mutate the successor. A later HTTP retry that begins
after the fence resolves the current session normally, but no operation already
admitted under the old epoch is retargeted silently.

The later domain admission transaction locks the alias, validates tenant,
generation, object kind, publication state, exact digest, and size, inserts the
request/job/service/version owner plus its object reference, and marks the
object `REFERENCED` if this is its first reference. The storage repository must
accept a caller-owned SQLAlchemy connection/session and must neither commit nor
open an independent transaction when one is supplied. Consequently no owner can
commit without its reference and no reference can become visible without its
owner. Requests, Managed Jobs, and Serve use this one repository contract; they
do not each implement an object-side transaction.

References do not grant authorization across tenants, and possession of a
logical blob ID is insufficient. Version 1 never transitions a referenced
object back to a deletable state; terminal owner cleanup may remove product
rows according to existing retention rules while object bytes and their audit
records remain retained.

For a new `/upload_v2` object, the logical blob ID must equal the verified
SHA-256 of the uploaded zip. Existing EFS blobs may contain only an extracted
tree because the current backend discards the original zip. The importer may
encode that tree as a canonical migration bundle whose archive digest differs
from the historical logical ID, but only with an `EFS_IMPORT` receipt recording
the full source inventory, canonical tree digest, modes, symlinks, encoding,
new object digest, and all retained owners. Runtime upload code cannot create
that exception. Materialization verifies the object digest and the canonical
tree digest before presenting the same file semantics.

Version 1 retains owner-reference audit rows and does not cascade-delete them
when a product owner ages out. This avoids inventing a second liveness/garbage-
collection protocol while completed objects are intentionally never deleted.

### S3

The S3 location is private, versioning is Enabled rather than Suspended,
ownership is bucket-owner-enforced, and objects are encrypted with one approved
KMS key. Public access is blocked and TLS is required. Application roles can
create/upload/complete/abort one recorded multipart upload, put small segments,
head exact keys, get exact versions, and list parts only for a recorded upload
in their server-owned roots. They cannot list object prefixes, copy objects,
delete completed objects or versions, change bucket configuration, or alter the
KMS key.

Before any new runtime blob or log publication provider call, allocation locks
the storage-authority row and reads the exact target tuple committed for that
generation. It derives a random key under that server root and copies the
target identity, authority generation, and authority incarnation into the
attempt or log-segment allocation. Any projected bucket, region, owner, root,
KMS, or policy-capability hint must equal the PostgreSQL tuple byte-for-byte
after canonicalization; absence or mismatch fails readiness and the call is not
issued. IAM independently grants only that tuple. The hint and IAM grant prove
capability but neither is a target selector. Cleanup uses only its recorded
attempt target as defined below. An object read continues to use its own
committed exact identity, including after an authorized later generation;
prefix discovery and "the currently configured bucket" are never read
authority.

The one migration importer uses a preparatory intent in the same PostgreSQL
authority because it must publish immutable S3 bytes before runtime changes
from `LEGACY_EFS`. After the final source inventory but before its first
provider call, an explicit prepare-import transaction locks the current
storage-authority row and migration-intent head. It verifies the installation,
source EFS identity and manifest digest, source protocol/generation/
incarnation, candidate generation, complete target tuple, and qualified policy-
probe receipt, then compare-and-swaps the head to a new operation ID and commits
the immutable `PREPARED` intent. A nonmatching source, existing nonterminal
intent, reused operation ID, or failed CAS writes no intent and permits no S3
call.

Immediately before every importer S3 call, an allocation/authorization
transaction locks and revalidates the exact `PREPARED` or `IMPORTING` intent,
its current head ownership, unchanged source authority, candidate generation,
target, and probe receipt, and records the operation-scoped call authorization.
The first authorization advances `PREPARED` to `IMPORTING` in that same
transaction. Every retry and reconciliation does the same; every response-
receipt transaction re-locks and revalidates before recording an effect. Import
allocations, call authorizations, and receipts stamp the operation ID and full
target identity. A mismatch, another state, or `ABANDONED` intent fails closed;
a late provider response can only leave unreferenced bytes under its operation-
scoped random key. The operation may become `VERIFIED` only when every manifest
item has its exact receipt and every call authorization has terminal or
execution-quiescence evidence. The final activation transaction locks the
authority, intent head, and `VERIFIED` intent, compare-and-swaps the exact
recorded source to its candidate generation, requires the full target, probe,
source protocol/generation/incarnation, installation, manifest, and operation
binding to match, commits the imported catalog and `S3_V1` authority, and marks
that same intent `ACTIVATED` atomically. This intent authorizes only the bounded
import operation. It neither selects the runtime protocol nor permits runtime
S3 writes or EFS/S3 dual-write.

Every committed object record includes:

- provider, bucket, key, and exact version ID;
- expected bucket owner and KMS key ARN;
- tenant, logical object kind, representation encoding, origin, and reference
  state; ownership is represented only by the separate reference rows;
- byte count and SHA-256 digest;
- creation and commit timestamps from PostgreSQL; and
- the storage generation and database incarnation that created it.

Keys are server generated and include a random immutable object ID. Content
digests verify bytes; they are not used alone as authorization or as the S3
key. Reads always request the exact committed version and verify size and
digest. Publication uses If-None-Match: * and the bucket policy denies a
nonconditional PutObject in the authoritative roots, so one logical object key
cannot acquire competing versions.

For multipart publication, the conditional header is mandatory on
`CompleteMultipartUpload`; the bucket policy uses `s3:if-none-match` and
`s3:ObjectCreationOperation` so `CreateMultipartUpload` and `UploadPart` are
allowed but an unconditional completion is denied. A `409` completion conflict
starts a newly fenced multipart attempt; a `412` collision is accepted only by
reconciling the immutable exact key. CopyObject is unsupported on this prefix.
These details follow the
[AWS conditional-write policy contract](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
and
[multipart conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
and are covered by a real-bucket policy test, not only an SDK mock.

Incomplete multipart parts are not committed objects. After PostgreSQL has
fenced an attempt, the same repository records multipart cleanup as `PENDING`
before issuing `AbortMultipartUpload` for only its exact recorded provider,
region, bucket, key, upload ID, expected owner, authority generation/
incarnation, and creation receipt. Cleanup never substitutes the current
process target. It is idempotent and never unfences the attempt. Because an
in-flight `UploadPart` may finish after an abort, cleanup waits out every
recorded provider-call lease, each already capped at its session idle deadline,
repeats exact abort as needed, and calls `ListParts` until the upload is absent
or the part list is empty before recording `CONFIRMED_ABSENT`. Lost abort
acknowledgement, retryable provider failure, or an unexpectedly nonempty part
list leaves the receipt retryable.
The existing bounded maintenance loop retries that same repository operation;
successor creation does not wait for provider cleanup because it uses a
different upload ID and key. A prefix-scoped
`AbortIncompleteMultipartUpload` lifecycle rule is the final cost backstop
after the documented retry horizon, not a correctness transition. No lifecycle
rule expires completed current or noncurrent object versions in version 1.

Opaque object kinds in version 1 are:

- raw upload archives used by file mounts;
- imported retained opaque files whose owning product still needs them; and
- immutable request, managed-job, Serve-controller, and replica log segments.

Small normalized YAML, config, identity, and lifecycle records remain in
PostgreSQL.

### Local ephemeral storage

Each role receives disk-backed emptyDir volumes with explicit sizeLimit,
ephemeral-storage request, and ephemeral-storage limit. Memory-backed storage
is not the default for control-plane archives or logs.

The chart and application define, test, and expose metrics for all of these
bounds: compressed bytes per chunk and upload, total chunks, concurrent uploads
per pod and tenant, materialized compressed bytes, declared and observed
uncompressed bytes, archive entry/inode count, individual entry size, path
length, log-spool bytes, and cache bytes. The HTTP layer rejects an oversized
request before exhausting emptyDir. Archive validation rejects traversal,
absolute or escaping symlinks, device/special files, duplicate/conflicting
paths, decompression beyond the declared or observed byte budget, and inode
exhaustion before exposing a materialization to its consumer.

The limits are explicit required guarded-HA chart values, not implicit library
defaults. D3/D4 select their concrete defaults from the measured production
payload and peak concurrency, record the immutable upload/materialization
limits used by each session, and reject a rollout whose aggregate reservation
can exceed the pod's ephemeral-storage limit. The current 95,000,000-byte
client chunk is supported only when it fits that admitted per-chunk bound; the
server never trusts the client constant. Changing a limit affects only newly
allocated sessions and cannot reinterpret an active upload.

ENOSPC, quota exhaustion, truncated archives, and checksum failures are typed
retryable or terminal results in PostgreSQL. They never make a partial local
directory authoritative. A per-pod admission semaphore reserves enough local
space for each accepted operation so concurrent requests cannot collectively
exceed the pod limit.

Local paths may contain:

- upload parts before immutable publication;
- an exact-version materialization while one scoped consumer owns it;
- generated config, YAML, and SSH files reconstructed from PostgreSQL or a
  projected Secret;
- bounded log segments awaiting publication; and
- derived caches that can be regenerated.

Startup may remove only that pod's ephemeral contents. Local existence,
rename, mtime, inode, or lock files never grant durable authority.

### Secrets and identity

Cloud credentials continue to use workload identity, projected service-account
tokens, and Kubernetes Secrets. Generated SkyPilot SSH keys are reconstructed
from their existing PostgreSQL records into local ephemeral storage.

Guarded HA does not accept mutable external private-key upload into PostgreSQL
or S3. An external SSH key must come from a pre-existing, explicitly projected
Kubernetes Secret or a future reviewed secret-manager backend. Production
currently has SSH node pools and sshKeySecret disabled, so this restriction
does not block the current cutover.

## Runtime contracts

### Blob publication

The current `/upload_v2` shape remains usable while its storage semantics
change. The client-supplied blob ID is the claimed SHA-256 of the complete zip;
it is a logical alias and integrity claim, never an authorization token or S3
key.

Guarded HA derives `tenant_id` from the authenticated server identity used by
the later domain admission. A client-supplied user hash is only a compatibility
routing field and cannot select another tenant's session, alias, object, or
reference.

1. The read-only existence check resolves only a tenant-scoped published alias;
   it does not allocate state. The first chunk request creates/locks the upload
   head, current session, and first server-generated attempt/object identity in
   PostgreSQL. Concurrent first chunks converge through the unique logical
   head. Before returning or rejecting any active attempt, the allocation path
   performs the database-clock abandonment check above. A nonexpired retry
   returns the same attempt when its immutable plan matches and rejects a
   conflict; an idle session is fenced to terminal `ABORTED` and one compare-
   and-swapped successor session epoch may adopt either the same or a changed
   plan with a fresh attempt/key. A published alias always returns the existing
   object. One lease epoch owns multipart creation or finalization at a time.
   Allocation copies the locked PostgreSQL authority generation/incarnation and
   exact S3 target into the attempt before releasing the transaction.
2. Each parallel chunk request may land on any API replica. It validates chunk
   number/count and HTTP bounds, spools at most that chunk to a reserved local
   path while computing its checksum and size, uploads the corresponding S3
   part, and commits the part receipt under the attempt. A row/advisory lease on
   `(session, attempt, part)` permits only one provider call for a part at a
   time and uses the database-clock acquisition/renewal cap above, preventing a
   concurrent overwrite from disagreeing with its receipt or a live owner from
   extending provider authority beyond the session idle deadline. Part numbers
   are bounded by S3's 10,000-part limit and every non-final part satisfies
   S3's minimum size. A duplicate part is accepted only if its durable checksum
   and size match.
   A provider-call lease that expires or loses its owning process is never
   reassigned against the same multipart upload: the attempt becomes
   `REJECTED`, its exact upload receives a durable cleanup receipt, and a fresh
   attempt/upload ID/key requires the client to resend all parts. Receipt-backed
   exact abort/ListParts reconciliation proceeds independently as defined
   above. This prevents a late, unfenceable `UploadPart` response from changing
   a part after another owner recorded its receipt.
3. A finalizer locks the complete receipt set, conditionally completes the
   multipart upload whose create request fixed the required SSE-KMS controls,
   revalidates the current authority, head, session, attempt, and lease epochs,
   and records its attempt epoch. A `409` rejects that attempt, creates a fresh
   fenced attempt/key, clears its part-receipt view, and asks the still-owning
   client to resend all parts. A `412` or lost response is reconciled only
   against that attempt's exact immutable key, never a prefix listing.
4. The finalizer heads the exact key, captures its version ID, and verifies
   expected owner, key, version, byte count, encryption, and KMS key. Because a
   multipart ETag or composite checksum is not the logical full-archive
   SHA-256, it performs a streaming exact-version read and computes the full
   SHA-256 before publication commits.
5. One PostgreSQL transaction inserts the immutable object in
   `PUBLISHED_UNREFERENCED`, inserts the logical alias, and marks the upload
   session `PUBLISHED`. Local part spools are removed.
6. The later request, Managed Job, or Serve admission attaches the owner
   reference in its own domain transaction as defined above.

A crash before the session records an S3 multipart upload may leave only
incomplete parts; the narrow lifecycle is the cost backstop when no exact upload
ID was durably received. A recorded upload instead follows receipt-backed exact
abort/ListParts reconciliation. A crash after object completion but before
PostgreSQL publication is recovered from the exact session key and
conditional-write invariant. Every recovery transaction revalidates authority
generation and incarnation. A lost acknowledgement cannot select a prefix-list
winner, overwrite bytes, or create a second logical alias. Digest, encryption,
owner, size, or format failure rejects that immutable attempt and requires a
fresh attempt/key; it can never overwrite or bless the bad version. No step
requires all chunks or retries to reach one pod.

The legacy `/upload` endpoint cannot be adapted safely to this protocol: its
client does not include the upload handle in the later domain request, so two
concurrent clients of one tenant cannot be bound to the correct immutable
object without a mutable "latest client directory" heuristic. The transition
image continues to serve `/upload` only while the authority row is
`LEGACY_EFS`. Activation proves the documented pre-v2 client-support horizon
has elapsed and rejects S3_V1 when such a client is still active. Once S3_V1 is
committed, `/upload` returns a minimum-client-version error and cannot publish
bytes; D10 deletes the endpoint and decoder. `/upload_v2` is the only steady-
state upload path.

BlobStorage gains a scoped materialization interface rather than exposing a
permanent shared directory. A consumer enters a materialization context, the
provider downloads and verifies the exact version into its bounded local
directory, validates archive bytes/entries against all configured bounds, and
only then exposes the extracted root. Exit removes it. The same process may
reference-count concurrent consumers of one verified local copy, but that
cache is process-local, bounded, and reconstructible. Long-lived controllers
rematerialize after takeover.

### Log publication and reading

One common log writer serves API requests, Managed Jobs, Serve controllers, and
replica launch operations. It creates immutable ordered segments and commits
their metadata in PostgreSQL. Segment order is a monotonically allocated stream
sequence, not S3 listing order. Each segment uses the same conditional
publication and lost-ack reconciliation contract as a blob.

Each stream has one PostgreSQL lease owner, monotonic lease epoch, and writer
authority generation/incarnation. Segment and gap identities are unique on
`(stream_id, sequence)` and carry that complete writer fence. Segment allocation
locks the storage authority and copies its exact S3 target, generation, and
incarnation before issuing a provider call. Metadata commit revalidates all of
them, so a deposed or pre-restore writer can at most leave an unreferenced
immutable key and cannot append after takeover. The writer flushes on a bounded
time or size threshold and before publishing a terminal lifecycle transition.

A hard-killed writer cannot write its own gap marker. After the old lease
expires, the next owner first commits an `OWNER_LOSS_TAIL_GAP` at the next
sequence in the same transaction that acquires the new epoch, unless the prior
owner durably flushed and closed the stream. The marker identifies the lost
owner/epoch and last committed sequence; it does not invent an unknowable byte
count. Only then may the new owner append. Thus a hard process kill may lose an
uncommitted local tail, but recovery exposes that durability boundary instead
of silently pretending the bytes exist.

An authority-incarnation change invalidates every writer lease immediately,
independently of its wall-clock expiry. The first new-incarnation owner commits
a `RESTORE_TAIL_GAP` at the next sequence in the same transaction that acquires
its writer epoch unless the restored database proves the prior writer durably
closed. The marker names the restore receipt, old incarnation, writer epoch,
and last committed sequence. An old segment upload that finishes later cannot
commit metadata because every segment transaction compares the current
authority incarnation.

Readers combine committed segments with a live local tail only when connected
to its current lease owner. A request handled by another API replica streams
committed segments and polls PostgreSQL until the next flush; it does not use a
shared tail file. Reconnection or owner loss resumes from committed segments by
stream sequence. Existing stream/download API shapes remain unchanged, and no
client receives S3 credentials.

Version 1 retains committed log objects indefinitely. Product-facing
expiration is not introduced by this migration.

### Managed Jobs

Before cutover, validation proves that every retained nonterminal job has its
required DAG, environment, config, original YAML, and blob reference in
PostgreSQL. Missing or ambiguous legacy rows block migration and are repaired
with product-owned evidence; they are not guessed from filenames.

Managed Job registration inserts its tenant-validated object reference using
the same caller-owned PostgreSQL transaction as the durable job row. The API
request that launches registration retains its own reference until existing
request-retention rules release the request; these are separate owners of one
immutable object, not an inferred transitive reference.

After cutover, guarded HA materializes PostgreSQL content and exact S3 blobs
locally. Disk fallback remains only for explicit local/non-HA compatibility and
cannot be selected in guarded HA. Controller and task logs use the common log
writer.

### Serve

Serve up/update admission becomes self-contained before it returns success.
Normalized task YAML, submitted YAML, placement, and sanitized controller
configuration commit in the existing version transaction. When admission names
an uploaded blob, that transaction also locks its tenant-scoped alias and
inserts the service-version object reference. The elected controller
reconstructs its local directory from that committed state.

Cross-role filesystem staging is removed. Replica launch artifacts and logs use
the common object/log path. Existing PostgreSQL controller snapshot and
takeover behavior remains authoritative.

### Config and SSH

Database-backed guarded HA already rejects inline Helm config. On a fresh
schema, only the migration job in explicit bootstrap/upgrade mode inserts one
validated empty `api_server_config` row with `ON CONFLICT DO NOTHING`; an
existing row always wins. Verify mode and every runtime role execute no seed
DML. Canonical central server-config resolution requires PostgreSQL, reads that
row directly, and fails closed if it is absent or invalid; it never overlays a
ConfigMap or shared file. Configuration updates lock the canonical PostgreSQL
config row across the complete read-modify-write, or compare-and-swap its exact
expected content digest, and commit the new content plus digest atomically. For
a workspace mutation, that same caller-owned transaction and lock boundary also
applies the exact Casbin workspace-policy delta and advances the database-backed
permission-cache generation/invalidation receipt. The config and permission
repositories accept the caller's PostgreSQL transaction and neither commits
independently. A stale writer must re-read and retry or return a conflict; it
cannot overwrite a concurrent update. A failure exposes either the old config,
policy, and cache generation or the new set, never a mixture.

Every current guarded-HA config writer routes through that one repository
transaction. `update_api_server_config_no_lock` cannot remain a public blind-
upsert escape hatch: D1 either removes it or makes it repository-private and
requires the caller's active PostgreSQL transaction plus exact expected digest.
No handler, permission callback, bootstrap helper, or background path may write
the config row outside the locked/CAS contract.

Only after commit does each process reload the committed config and in-memory
Casbin enforcer. Authorization readers compare their observed cache generation
with PostgreSQL and reload/fail closed before making a decision under a newer
generation, so a crash after commit but before local reload cannot preserve
stale authorization. Any compatibility path that requires a file receives a
versioned pod-local projection and may recreate it after restart. A pod-local
lock protects only in-process projection/reload; it has no cross-pod or durable-
update authority. No filesystem config or policy lock participates in a
guarded-HA workspace mutation. D6 removes remaining filesystem locks from
non-workspace permission operations rather than expanding D1 into unrelated
authorization behavior.

D1 introduces a distinct pod-local lock only for central-config in-process
reload; it does not globally repoint `get_skypilot_config_lock_path()`. During
the `LEGACY_EFS` transition, that existing shared lock still fences the shared
Serve `config.yaml.v*` promote/restore/scrub path, including recovery before a
controller-incarnation claim. Those operations must check the protocol on both
sides of the lock. D6 first replaces the shared Serve/Managed Jobs snapshot
path with an exact versioned projection materialized from PostgreSQL into
bounded pod-local storage, then deletes the legacy shared snapshot lock and
path. Repointing the generic helper early would create two unsynchronized locks
over the same shared files and is forbidden.

Likewise, D1's direct-PostgreSQL claim is scoped to canonical central server-
config resolution. Guarded Serve/Managed Jobs child processes retain their
explicit immutable per-owner/version snapshot semantics: after D6 the server
loads the exact snapshot from PostgreSQL, materializes it pod-locally for the
child lifetime, and may set `SKYPILOT_CONFIG` only to that server-issued path.
In a central-server context, `reload_config()` ignores file precedence and
resolves PostgreSQL directly. In a scoped child context, it validates the exact
snapshot identity before reading the local projection. Tests exercise this real
dispatch boundary; testing only `get_server_config()` or an effective-config
helper is insufficient.

The guarded-HA chart omits the `skypilot-config` ConfigMap itself, every config
volume and mount from API, controller, executor, and image-worker pods, and the
image worker's `SKYPILOT_GLOBAL_CONFIG` environment variable. A PostgreSQL
short-circuit while retaining any of those references is insufficient because
a missing unused ConfigMap can still prevent pod scheduling. Explicit non-HA
installations retain their separate file/ConfigMap compatibility behavior.

Generated SSH keypairs and cluster YAML continue to come from PostgreSQL.
Externally managed key files are read-only Secret projections. /root/.ssh is no
longer a persistent mount.

### Caches

Catalogs, generated cluster files, locks, wheels, debug dumps, and provider
caches are either reconstructed or explicitly projected. Every derived cache
is bounded and disposable. A cache miss can affect latency but cannot change
ownership, lifecycle, placement, or recovery.

## Protocol selection and mixed versions

The transition image temporarily implements both providers behind one
PostgreSQL storage-authority row; it never dual-writes. `LEGACY_EFS` selects
only the old provider before cutover, and `S3_V1` selects only PostgreSQL/S3
after cutover. Every public blob, log, materialization, config, Job, and Serve
entrypoint resolves the protocol through the same repository. There is no
per-role flag or Helm value that can contradict PostgreSQL.

For `S3_V1`, that same row also selects the exact provider target. The
transition and PVC-free charts may project the tuple as a capability hint so a
pod can construct its SDK client, but startup canonicalizes and compares every
field with PostgreSQL before readiness. A missing or divergent hint fails
closed; it cannot override, fill in, or migrate the authority row. Allocation
copies the locked tuple plus generation/incarnation into durable writer state,
and a later retry uses those recorded values rather than whichever values a new
pod happens to receive.

The release fence Secret contains the expected database incarnation plus an
anti-rollback protocol/generation floor. Those values never select a provider:
PostgreSQL remains the authority, and a mismatch only makes every role
NotReady. Before cutover the floor permits the exact initialized `LEGACY_EFS`
generation. After the one-way commit it requires `S3_V1` at that generation or
newer; the authorized activation/restore tooling rejects any attempt to lower
it. This prevents a restored pre-cutover database
from reviving EFS even while the transition image still contains both
providers. Normal Helm rendering and runtime roles cannot change the fence.

The additive storage schema advances the existing mandatory central PostgreSQL
migration head that every guarded-HA process verifies; it is not placed on an
optional or unnoticed migration lineage. Pre-feature server images therefore
cannot verify the new head. The transition rollout fences and drains every
pre-feature API, executor, and controller process, completes the blocking
migration, explicitly records the retained installation as `LEGACY_EFS`, and
proves that all 2/2/2 pods run the immutable transition digest while the
protocol is still `LEGACY_EFS`. A restarted pre-feature image fails schema
verification before serving requests or acquiring provider authority.

The `S3_V1` commit records the complete immutable target tuple, policy-contract
version and probe receipt, minimum storage capability, and qualified image,
chart, values, and rendered-manifest digests in its receipt. Activation is
refused unless the database head, quiescence evidence, exact S3/KMS policy
probe, and intended deployment digests match. EFS migration additionally
requires the current `VERIFIED` migration intent and its importer manifest to
match every immutable binding; the intent becomes `ACTIVATED` in the same
transaction. After that commit, an S3-capable process that cannot implement or
reach the recorded target fails readiness; it cannot select EFS or another
bucket. Native rollback to a pre-feature image is unsupported and fails the
database-head gate. The stacked cleanup removes `LEGACY_EFS` code after the
acceptance horizon.

A fresh guarded-HA database never bootstraps through EFS. The same activation
command has a fresh-install mode that requires an empty product database, the
release fence Secret, qualified image/chart identities, and successful
exact S3/KMS/IAM probes, then inserts the first authority row directly as
`S3_V1` with the exact target tuple and a unique install receipt. It refuses a
nonempty or previously initialized database. During the transition stack,
upgrades of an existing installation initialize `LEGACY_EFS`; after D10, fresh
guarded-HA installation supports only the receipt-backed `S3_V1` bootstrap. No
normal pod startup or Helm render chooses a protocol or provider target
automatically.

## Database restore behavior

An ordinary migration job or restart never rotates database incarnation: a
restored row would otherwise be indistinguishable from the live row and an
automatic rotation could bless an unauthorized old database. The schema
migration creates no authority row and chooses no protocol. The explicit first
authority initialization copies one release-projected expected incarnation
into the new PostgreSQL row and validates the Secret's protocol/generation
floor. The PostgreSQL row is the structured authority; the Secret is only an
external authorization/fence input, not a second mutable copy of application
state. Runtime readiness requires the PostgreSQL incarnation, protocol, and
generation to satisfy that projected fence. Normal Helm upgrades reuse the
Secret and normal application roles can only compare it; they cannot create,
update, lower, or rotate it.

An authorized restore is a separate, one-shot operator operation while all
application writers and provider mutation are fenced. The operator generates a
fresh UUID, updates the incarnation field in the release fence Secret, and runs
the restore command with the expected restored UUID, fresh UUID, immutable
restore operation ID, and the existing control-plane authorization evidence.
Its first transaction locks the storage-authority row, compare-and-swaps only
the expected old incarnation, and inserts a unique `FENCED` restore receipt
binding old/new incarnation, protocol, generation, and provider-target digest.
That commit globally invalidates every old writer before any row-by-row
recovery. A retry with the same operation and values resumes from the receipt;
a different value or already-advanced incarnation fails closed. Runtime roles
have no code path that performs this rotation during normal startup.

The restore command first proves the restored row already satisfies the
non-lowerable protocol/generation floor and that its complete S3 target tuple
matches the projected capability. It may rotate only incarnation; it cannot
change protocol, generation, provider target, or policy contract. A pre-S3_V1
snapshot is therefore not a valid restore source after cutover. The operator
may update the incarnation field of the fenced Secret only while all roles are
stopped; a failed CAS leaves them fail-closed until the operator restores the
prior incarnation input or completes the authorized repair.

Every mutable storage writer row carries the incarnation under which it was
allocated. Every create/part/finalize/publication receipt and log-segment commit
compares it with the current authority row, so the `FENCED` transaction alone is
sufficient to stop an old process from committing even if an S3 call completes
late. The restore operation then performs a bounded, retryable PostgreSQL
recovery pass before writer readiness:

- each old-incarnation upload head is handled in one locked transaction. If its
  current session is `ALLOCATED` or `MULTIPART_ACTIVE`, any current nonterminal
  attempt becomes terminal `ABORTED` with reason `RESTORE_INCARNATION` and
  cleanup `PENDING` when it has a recorded multipart upload; that session
  becomes terminal `ABORTED` with the same reason and exact restore operation
  ID, and its session-fence epoch increments. Whether the current session was
  just aborted or was already terminal, the upload head advances to the fresh
  authority incarnation and increments its head-fence epoch while retaining
  that terminal predecessor as its current audit pointer. A later request may
  compare-and-swap an exact aborted predecessor to only a new-incarnation
  session/attempt/key; a published alias still prevents a successor;
- each old-incarnation log writer lease is invalidated. The first new writer
  commits the `RESTORE_TAIL_GAP` described above while acquiring its new
  incarnation and epoch; and
- old-incarnation completed objects, aliases, owner references, and committed
  log segments present in the restored snapshot remain valid and readable.
  Effects absent from the snapshot remain unreferenced retained garbage and are
  never rediscovered by prefix listing.

When all old-incarnation mutable heads/leases are durably fenced, the command
marks the same restore receipt `RECOVERED`. Exact multipart abort/ListParts
cleanup may continue through the existing bounded maintenance path because the
fresh key makes it cost cleanup rather than correctness. Only a current
`RECOVERED` receipt and the existing provider-authority recovery gates permit
application writers to become Ready. Restored exact object references remain
readable because version 1 never deletes authoritative S3 objects. Fresh writes
include the new incarnation in their server-generated keys and cannot overwrite
an old version.

The existing control-plane authority and provider-mutation fences still apply
to a restored database. This storage design does not make an old database
automatically eligible to control infrastructure. It only guarantees that a
separately authorized restore does not expose dangling byte references.

There is no storage-source registry, restore-age calculation, snapshot scan,
tombstone compaction, or old-incarnation object sweep in version 1.

## Migration and cutover

The migration has one authority transition: EFS to S3_V1.

### Pre-commit preparation

1. Merge the feature stack and its draft cleanup PR. Publish immutable
   transition and PVC-free fix-forward images/charts.
2. Provision the minimum compliant dedicated server-owned S3 bucket and KMS
   key through one surgical
   infrastructure slice; the current account has no compliant SkyPilot bucket
   and its broad role policy is insufficient. The bucket-policy explicit deny
   and least-privilege workload grants must be effective before any upload.
   Produce the canonical provider-target tuple and qualified policy-probe
   receipt that the S3_V1 authority transaction will bind; a chart value alone
   is not activation evidence.
3. Inventory the exact PVC, PV, access point, EFS root, path types, byte counts,
   ownership, modes, symlinks, and PostgreSQL retained references.
4. Classify every path as structured state already in PostgreSQL, an object to
   import, derived disposable state, or explicitly unsupported residue.
   Unknown, special, unreadable, multiply owned, or unreferenced-but-required
   content blocks cutover. Extracted legacy blob trees receive canonical
   migration bundles and tree-digest receipts; the importer never pretends the
   repacked archive digest equals the historical client blob ID.
5. Run the importer and recovery tests against an isolated copy and isolated
   PostgreSQL database. Provider mutation is fenced during rehearsal.
6. Prove parallel chunks sent through both API replicas converge on one
   PostgreSQL upload session and one verified immutable S3 version; prove the
   later request/job/Serve admission attaches its reference atomically.
7. Prove no `/upload` request occurred during the documented supported-client
   horizon and that all known clients negotiate `/upload_v2`; any observed
   legacy client blocks activation until it is upgraded or explicitly retired.
8. Render the exact PVC-free Helm release and prove that no guarded-HA pod,
   hook, init container, or sidecar references the claim or EFS CSI.

No production authority changes in these steps.

### Freeze and commit

1. Enter a maintenance window; reject new uploads and configuration/service/job
   mutations.
2. Drain active request execution and prove no unquiesced provider handler,
   launch, update, or teardown remains.
3. Fence the application and scale API, executor, and controller roles to zero.
4. Capture the final exact EFS inventory and source manifest without issuing an
   S3 mutation.
5. Before the first importer S3 call, commit the migration-operation intent by
   the PostgreSQL compare-and-swap defined above. It binds the installation,
   exact EFS source/manifest, source generation/incarnation, immutable operation
   ID, candidate generation, complete provider target, and policy-probe receipt.
6. Copy required opaque bytes to operation-scoped random keys. Every call and
   receipt revalidates the exact intent. Verify every object twice by exact
   version, size, and digest, persist its item receipt, and advance only that
   operation to `VERIFIED` after the complete manifest succeeds and every
   operation-scoped provider call has terminal or execution-quiescence evidence.
7. In one PostgreSQL transaction, lock the current authority, intent head, and
   exact `VERIFIED` intent; compare-and-swap its recorded source to the candidate
   S3_V1 generation; require the installation, source EFS identity, manifest,
   source protocol/generation/incarnation, candidate generation, target, probe,
   and operation fields to match; commit imported object rows, logical aliases,
   owner references, qualified image/chart/values/manifest identities, and the
   generation receipt; and mark the intent `ACTIVATED`. Partial commit is
   impossible.
8. After that commit, advance the stopped release's non-lowerable fence Secret
   to the committed S3_V1 generation, then start only the PVC-free release. A
   failure after step 7 remains a fix-forward outage; it cannot reopen EFS.

Before the generation transaction commits, the same operation may resume from
its durable receipts while all application writers remain fenced. Alternatively
the operator may terminalize it as `ABANDONED` after provider quiescence; its
immutable bytes remain unreferenced garbage, every later importer call fails
closed, and a new operation requires a new intent-head CAS. Only then may the
qualified transition release restart in `LEGACY_EFS` mode. The preparatory
intent never changes runtime protocol selection. A pre-feature image is already
rejected by the central schema head. After activation commits, EFS can no longer
be selected as authority.

### Fix-forward activation

1. Deploy the PVC-free 2/2/2 release with direct Helm, immutable image/chart
   identity, retained values, and --reuse-values.
2. Require every role to read the committed S3_V1 generation, prove every
   projected provider-capability field exactly matches its PostgreSQL target,
   and pass PostgreSQL, S3 exact-version, KMS, local-space, and
   projected-identity readiness probes.
3. Verify upload/download, request logs, nonterminal Managed Job recovery,
   Serve reconstruction/takeover, config projection, generated SSH recovery,
   and cluster status after complete pod blackout.
4. Exercise one API, executor, and controller deletion and prove takeover
   without shared paths or duplicate provider mutation.
5. Keep the EFS resources intact but unmounted until the acceptance horizon
   completes.

Failure after generation commit is fixed forward with an S3_V1-capable release.
Native Helm rollback to an EFS-dependent revision is unsupported.

### EFS decommission

After acceptance:

1. Merge the stacked cleanup PR that removes importer, EFS transition,
   guarded-HA filesystem fallbacks, compatibility metrics, and transition-only
   tests.
2. Remove the exact SkyPilot PVC, PV, access point, client IAM, mount policy,
   CSI dependency if otherwise unused, and desired-state recreation path.
3. Prove a fresh render and live inventory contain no SkyPilot EFS client.
4. Delete fs-00a7dd95ad52c0ade and its mount targets only if fresh provider and
   desired-state evidence proves the filesystem is exclusive to SkyPilot.
   Otherwise leave the unrelated shared filesystem intact while SkyPilot has
   zero dependency on it.

No cleanup step infers ownership from a name, tag, or the current count of
access points alone.

## Implementation stack

The dependency graph and minimum clean stack are:

1. **D0 -- this design.** Merge the exact authority, upload-session,
   transaction, restore, rollout, and removal contract before implementation.
2. **D1 -- PostgreSQL config authority.** Stop canonical central server config
   from copying to or locking on RWX; seed the PostgreSQL row only in explicit
   bootstrap/upgrade migration modes, make verify/runtime initialization seed-
   free and fail-closed, preserve every retained row, and serialize each
   configuration read-modify-write with a PostgreSQL row lock or exact expected-
   digest CAS.
   Commit each workspace config change, exact Casbin workspace-policy delta,
   and database-backed permission-cache invalidation in that same caller-owned
   transaction, then reload in-memory enforcers. Pod-local locking remains only
   for in-process projection/reload, and no filesystem policy lock participates
   in workspace mutation. In the same D1 PR, guarded-HA chart rendering removes
   the `skypilot-config` ConfigMap, all role and image-worker config volumes/
   mounts, and `SKYPILOT_GLOBAL_CONFIG`; negative renders prove no reference
   remains. Add a distinct pod-local central reload lock without repointing
   `get_skypilot_config_lock_path()`; retain and protocol-fence that legacy
   shared lock for Serve snapshots until D6. Exercise actual `reload_config()`
   dispatch so central contexts resolve PostgreSQL while scoped child snapshots
   retain their explicit version semantics. Route every guarded-HA config writer
   through the one transaction repository and remove or internalize the blind
   `update_api_server_config_no_lock` bypass. This is the first bounded source
   PR because it removes one real EFS correctness edge without inventing the
   object protocol. It depends only on D0.
3. **D2 -- inert PostgreSQL storage foundation.** Add the protocol/generation,
   exact provider-target tuple, incarnation and `FENCED`/`RECOVERED` restore
   receipt, upload-head/session/attempt/part and cleanup-receipt,
   object/alias/reference, log-stream/writer/segment/gap, migration-intent head
   and operation, and migration-receipt schema. Stamp every mutable writer/lease
   with authority generation and incarnation; add database-clock durable-
   progress/deadline fields, a constraint that provider-lease expiry cannot
   exceed the idle deadline, and indexed current-head/old-incarnation recovery
   queries. Add the repository with the one on-access abandonment/restore-fence
   compare-and-swap, migration-intent CAS, and caller-owned transaction support
   on the existing mandatory central migration head. The migration creates no
   authority row or intent and changes no runtime routing.
   A separately invoked, idempotent `initialize-legacy` command records the
   exact retained installation, expected incarnation, EFS identity, and
   qualified transition digest before those processes start; it refuses a
   fresh/nonmatching database. A fresh database remains unactivated for D8's
   validated S3_V1 bootstrap. It depends on D0; D1 may merge before or with it.
4. **D3 -- exact S3 provider and cross-replica upload.** Add conditional
   multipart publication, checksums, exact-version verification, lost-ack
   recovery, PostgreSQL-target allocation, bounded part spool,
   lifecycle/policy probes, logical aliasing, and the object
   `PUBLISHED_UNREFERENCED` state and upload-session `PUBLISHED` state. Implement
   database-clock idle fencing on first-chunk access, exact stale-epoch
   rejection, database-clock provider-lease acquisition/renewal capped at the
   idle deadline, and receipt-backed abort/ListParts cleanup through the
   existing bounded maintenance loop; cleanup never gates a fresh-key successor.
   Integrate owner references into request, Managed Job, and Serve admission
   transactions. It depends on D2 and the static S3 boundary.
5. **D4 -- scoped materialization.** Replace permanent blob paths in guarded HA
   with checked, bounded context-managed materialization and route every
   file-mount consumer through it. It depends on D3.
6. **D5 -- common durable logs.** Add authority-incarnation-fenced leased
   writers, PostgreSQL-targeted immutable segments, owner-loss and restore-tail
   gap records, readers, downloads, and reconnect for requests, Managed Jobs,
   Serve controllers, and replica launches. Every segment receipt revalidates
   authority after provider I/O. It depends on D2 and the S3 provider from D3,
   but can proceed in parallel with D4.
7. **D6 -- remove remaining product filesystem authority.** Make Managed Jobs,
   Serve staging/reconstruction, generated SSH, catalogs, locks, wheels, and
   caches use PostgreSQL, exact S3 versions, Secrets, or bounded local state.
   Replace every remaining guarded-HA non-workspace caller of
   `~/.sky/.policy_update.lock`; D1 bypasses that lock only for its atomic
   workspace-config transaction. Replace the legacy shared Serve/Managed Jobs
   config snapshots with exact PostgreSQL-backed pod-local materializations,
   then delete their shared snapshot path and lock. It depends on D4/D5 for the
   surfaces it consumes.
8. **D7 -- PVC-free chart.** Building on D1's removal of every guarded-HA config
   ConfigMap reference, render guarded HA with disk-backed bounded emptyDir and
   no `/root/.sky`, `/root/.ssh`, `/root/sky_logs`, PVC, PV, or EFS CSI
   reference. D7 is blocked until D6 proves no guarded-HA caller still requires
   `~/.sky/.policy_update.lock` or the legacy shared snapshot lock. Add resource
   limits, readiness probes, and storage-negative render tests. It depends on
   D1 and all applicable D3--D6 runtime removals.
9. **D8 -- importer, rehearsal, and activation command.** Inventory EFS and
   PostgreSQL, CAS-commit the immutable migration-operation intent before the
   first importer S3 call, import exact versions while revalidating and stamping
   that intent, construct owner references, and activate only by atomically
   matching the exact verified intent while committing the one-way generation.
   Support receipt-backed fresh-database S3_V1 bootstrap and emit the
   verification bundle. Add the separate explicit restore operation:
   atomically rotate only incarnation into a `FENCED` receipt; then, in bounded
   locked PostgreSQL transactions, terminalize each active old-incarnation
   session and attempt, advance every old-incarnation upload head to the new
   incarnation/fence, and invalidate old log leases. Mark that same receipt
   `RECOVERED` before writer readiness. Provider abort cleanup can continue
   afterward through the existing maintenance path.
   Importer, cutover, Helm migration, and normal startup never rotate
   incarnation. It depends on D2--D7.
10. **D9 -- production activation.** Apply the dedicated minimal S3/KMS/IAM
    slice with the completed-object delete deny and conditional-write policy,
    deploy the transition image in `LEGACY_EFS`, rehearse,
    freeze/prepare-intent/import/commit, and deploy the PVC-free `S3_V1` chart.
    Static S3 infrastructure can be prepared in parallel after D2, but
    activation depends on D8.
11. **D10 -- stacked removal.** Delete `LEGACY_EFS`, importer/transition code,
    filesystem fallbacks, compatibility metrics/tests, and the exact SkyPilot
    EFS client path after acceptance. Author this draft alongside D2, the first
    change that introduces the temporary protocol row, keep it synchronized
    throughout D2--D9, and merge it only after the production gates pass.

Every feature PR links its immediately required successor and the D10 removal
PR. There is one runtime protocol selector, one object repository, one log
writer, and one materialization interface. No PR introduces KubeRay, FUSE, a
second database, a broad Terraform/Terragrunt refactor, or a boltz-platform
runtime pin. Infrastructure work is limited to the exact S3/KMS/IAM boundary
and the later deletion of the exact EFS client path; direct Helm owns SkyPilot
application deployment.

This is not a Helm-only change. The current estimate is at least 25
production/chart files, at least 18 test/design/migration files, one central
PostgreSQL migration, one importer/cutover command, and 8--9 stacked
application PRs plus the narrow infrastructure slices, depending on whether
adjacent bounded slices combine without creating a second path. A planning
range is
4,000--7,000 production lines and 4,000--6,000 test, migration, and design
lines. The common log path is the largest portion.

## Acceptance gates

### Source

- PostgreSQL migration upgrades, downgrade boundaries, idempotent retry, and
  mixed-version rejection pass against real PostgreSQL. Schema constraints and
  repository tests cover the exact authority target, authority incarnation on
  every mutable writer, upload durable-progress/deadline fields, head/session/
  attempt fences, provider-lease expiry constraint, cleanup receipts, and the
  unique migration-intent head/operation state machine.
- Parallel chunks deliberately routed across both API replicas produce one
  session, one logical alias, and one exact object without sticky routing.
- Upload allocation, multipart creation/completion acknowledgement loss, `409`,
  `412`, duplicate/conflicting parts, wrong tenant/owner, wrong version, wrong
  KMS key, digest mismatch, truncation, local ENOSPC, and pod deletion are
  covered.
- Before its PostgreSQL-clock idle deadline, an active upload rejects a
  conflicting chunk plan. At or after the deadline, concurrent first-chunk
  requests perform one on-access compare-and-swap that terminalizes the current
  attempt, aborts the unpublished session, advances both fences, and admits
  exactly one higher session epoch with a fresh key and either the same or a
  changed plan. Duplicate requests, lease renewal, failed provider calls, and
  process-local clocks do not extend the deadline; the result is identical when
  periodic maintenance never runs or races through the same repository
  transition. Immutable old identity and protocol-state audit fields remain
  unchanged (only the cleanup receipt may advance), and a published alias
  prevents all successors.
- A real-PostgreSQL renewal race starts a live provider-call owner immediately
  before the idle deadline while another replica attempts first-chunk
  abandonment. No acquisition or renewal commits at/after the deadline, no
  expiry exceeds it, and terminalization waits at most for that capped lease;
  exactly one successor wins and the old response cannot publish.
- After idle, restore, or attempt fencing, every delayed old request and every
  late multipart-create, UploadPart, or complete response fails the current
  authority/head/session/attempt/lease comparison and cannot record a receipt,
  publish an object/alias, or mutate the successor.
- Multipart cleanup tests cover abort acknowledgement loss, an UploadPart that
  completes after abort, repeated exact `AbortMultipartUpload`, and `ListParts`
  reconciliation through `CONFIRMED_ABSENT`. Cleanup remains retryable through
  the existing bounded maintenance loop and never gates fresh-key successor
  admission; the bucket lifecycle is tested only as the final cost backstop.
- A source-topology test proves first-chunk access alone can perform the idle
  transition and that only the existing bounded maintenance loop invokes the
  same repository compare-and-swap; no additional correctness-only process,
  state machine, timer, or daemon exists.
- An uploaded object is durably `PUBLISHED_UNREFERENCED` before domain
  admission; request, Job, and Serve tests prove that owner plus reference
  commit atomically through a caller-owned PostgreSQL transaction.
- S3_V1 rejects legacy `/upload` without allocating a session or writing bytes;
  `/upload_v2` remains compatible across both API replicas. An imported
  extracted blob with a historical logical ID reconstructs the verified
  canonical file tree without claiming archive-digest equality.
- Multipart policy tests prove unconditional PutObject/complete and CopyObject
  fail, conditional completion succeeds, incomplete parts are abortable, and
  completed current/noncurrent versions cannot be deleted or expired.
- Compressed bytes, uncompressed bytes, entries/inodes, paths, concurrent local
  reservations, and decompression-bomb limits fail before a partial
  materialization is exposed.
- Request, Job, Serve, and replica log streams reconnect from another API pod
  and retain ordered committed segments; killing a writer causes its next owner
  to commit a fenced tail-gap before appending.
- Managed Job and Serve recovery use only PostgreSQL, exact S3 versions,
  Secrets, and emptyDir in guarded HA.
- Bootstrap/upgrade config seed, retained-row precedence, verify-mode zero
  INSERT, missing/invalid-row fail-closed, and cross-role reload run against
  real PostgreSQL, not only mocks. A real-PostgreSQL two-writer test locks or
  exact-digest-CASes the full workspace read-modify-write and proves no lost
  config or policy update. Failpoints before commit and after commit/before
  reload prove the config row, exact Casbin workspace policy, and database-
  backed permission-cache generation expose only one atomic version and that
  every in-memory enforcer reloads or fails closed. The central/workspace path
  performs zero shared-file read/write and gives no filesystem config or policy
  lock workspace-mutation authority.
- A source inventory and real-PostgreSQL tests route every guarded-HA config
  writer through the transaction repository. No public blind-upsert variant of
  `update_api_server_config_no_lock` can bypass the expected digest, workspace
  policy mutation, or permission-cache invalidation.
- Actual guarded-HA `reload_config()` tests prove central contexts ignore file
  precedence and read PostgreSQL, while scoped Serve/Managed Jobs children may
  read only an exact server-issued version snapshot. D1 uses a distinct pod-
  local central reload lock and does not repoint the legacy shared Serve
  snapshot lock; protocol-fenced `LEGACY_EFS` restore/promote/scrub still
  serializes across pods until D6 replaces and deletes that path.
- Guarded-HA negative Helm renders for API, controller, executor, and image
  workers contain no `skypilot-config` ConfigMap object or reference, config
  volume/mount, or `SKYPILOT_GLOBAL_CONFIG`; explicit non-HA renders retain
  their compatibility path.
- Normal startup cannot rotate database incarnation; explicit restore CAS,
  idempotent receipt, stale expected-incarnation rejection, and readiness
  mismatch are tested against real PostgreSQL.
- Restoring a snapshot with a current active multipart attempt proves the first
  transaction fences the old incarnation, and the locked recovery transaction
  terminalizes the attempt and session, advances the head to the new
  incarnation/fence, and leaves the old rows immutable. Completion
  acknowledgement loss and every later old provider response cannot publish;
  the next request creates only a new-incarnation session, attempt, upload ID,
  and key while old exact-abort cleanup proceeds independently.
- Restore invalidates every old log-writer lease before wall-clock expiry. The
  new owner commits one `RESTORE_TAIL_GAP` before appending, and an old segment
  response cannot commit metadata under the fresh incarnation.
- Restore recovery is idempotent and resumable in bounded batches from the same
  `FENCED` receipt. No application writer becomes Ready until all mutable heads
  and leases are fenced and that receipt is `RECOVERED`; provider cleanup may
  still be pending at readiness.
- A restored authority row below the release's S3_V1 generation floor is
  rejected and cannot be upgraded by the restore command; runtime and normal
  Helm paths cannot lower that floor.
- A fresh empty guarded-HA database can bootstrap directly to receipt-backed
  S3_V1 after exact policy/capability checks, while a nonempty or previously
  initialized database cannot use that path.
- Two replicas with a missing capability hint, stale release generation/
  incarnation, or a divergent bucket, region, expected owner, root, KMS key, or
  policy contract cannot issue S3 calls. Allocation uses only the exact
  PostgreSQL target tuple and stamps it into the attempt or segment; reads use
  each object's committed identity rather than current process configuration.
- Importer tests prove no provider call occurs without a committed intent; two
  concurrent prepare operations produce one CAS winner; and every allocation,
  retry, provider response, and receipt rejects a missing, displaced,
  mismatched, or abandoned intent. The intent never changes runtime protocol
  selection and grants no runtime S3 write path while `LEGACY_EFS` is active.
- Guarded-HA Helm rendering contains no PVC, persistentVolumeClaim,
  /root/.sky, /root/.ssh, or /root/sky_logs persistent mount; this D7 gate is
  independent of the D1 config-ConfigMap negative render gate above.
- Before that PVC-free render, source inventory proves no guarded-HA caller of
  `~/.sky/.policy_update.lock` or the legacy shared Serve snapshot lock remains;
  explicit non-HA filesystem compatibility stays outside this gate.
- Every ephemeral volume has size and pod ephemeral-storage bounds.
- Runtime and importer identities cannot delete S3 object versions.

### Migration

- The final EFS manifest covers every path and byte with no unknown or
  unsupported retained item.
- Every active or nonterminal request/blob, job, service/version/controller,
  cluster, config, and generated-SSH reference resolves before freeze.
- No legacy `/upload` call occurs during the documented supported-client
  horizon; every active client negotiates `/upload_v2`.
- Every required blob maps to one tenant-scoped logical alias, exact immutable
  object, and explicit owner reference. Proven stale unreferenced uploads may be
  left only in the frozen EFS inventory; they are never guessed into an owner.
- No provider action lacks terminal or execution-quiescence evidence.
- The isolated rehearsal reconstructs all retained product state and performs
  no provider mutation.
- Every imported object passes exact-version, size, digest, owner, and
  encryption verification.
- Before the first production importer provider call, PostgreSQL contains one
  CAS-committed immutable operation intent binding the exact installation,
  source identity/manifest, source generation/incarnation, operation ID,
  candidate generation, full provider target, and policy-probe receipt. Audit
  evidence proves every provider request and receipt revalidated and stamped
  that intent.
- Activation with a displaced/abandoned intent or any changed source, target,
  probe, manifest, operation, incarnation, or candidate generation fails closed
  without changing runtime authority. One PostgreSQL transaction locks the
  exact `VERIFIED` intent and commits the complete reference set, S3_V1
  generation, target/probe receipt, and `ACTIVATED` state; partial commit is
  impossible.

### Production

- Two API, two executor, and two controller pods become Ready with no PVC.
- Complete deletion of all six pods recovers from PostgreSQL/S3 without manual
  file repair.
- Uploads, log streaming, retained Managed Jobs, Serve takeover, cluster
  status, config, and generated SSH all pass.
- Every Ready replica reports a capability tuple identical to PostgreSQL; a
  deliberately divergent value fails readiness without provider I/O, and S3
  request evidence names only the committed bucket, owner, root, KMS key, and
  authority generation/incarnation.
- One role at a time is deleted and replaced with no duplicate execution or
  provider mutation.
- Local ephemeral usage remains within limits under a measured peak upload and
  log workload.
- EFS receives no reads or writes after activation, including config reload,
  upload retry, log download, Job/Serve recovery, and full pod replacement.
- The acceptance horizon covers immediate, +10 minute, +30 minute, and one
  complete request/job/Serve recovery interval. Any reset repeats the horizon.
- The cleanup diff deletes the old guarded-HA path and leaves one steady-state
  implementation.

## Rollback boundary

- Before the S3_V1 generation commits, prove importer-provider quiescence,
  terminalize any committed migration intent as `ABANDONED`, reconcile
  temporary immutable uploads as retained garbage, and restart the qualified
  transition release in `LEGACY_EFS` mode. An abandoned intent cannot be
  reused; pre-feature images remain rejected by the schema head.
- After the generation commits, do not restore an EFS revision or dual-write.
  Repair schema, object access, or application behavior with an S3_V1
  fix-forward release.
- EFS deletion is a later cleanup action and never the rollback mechanism.
- A database restore does not roll the storage protocol backward.

## Rejected alternatives

### PostgreSQL-only payload storage

Rejected because opaque uploads and logs are bulk bytes with different access
and retention behavior. Putting the current EFS tree in Aurora increases
database I/O, backup size, and recovery coupling without improving authority.

### S3 FUSE or a mounted filesystem compatibility layer

Rejected because it preserves path, rename, lock, and listing semantics as
hidden authority and leaves the same multi-writer failure class.

### Sticky upload routing or one-pod local assembly

Rejected because the current client sends chunks concurrently and API replica
loss must not lose the rendezvous. Session affinity would turn a load-balancer
hint and one pod's emptyDir into hidden correctness state. PostgreSQL-fenced S3
multipart sessions let any replica accept every retry.

### Adapt legacy `/upload` through a latest-upload alias

Rejected because the old client omits its upload handle from the later domain
request. A per-tenant latest-upload pointer or FIFO would guess ownership under
concurrent clients and recreate shared mutable rendezvous state. The already-
supported `/upload_v2` content ID is the single steady-state contract.

### EFS plus S3 dual-write

Rejected because two durable byte paths require conflict selection and prevent
literal EFS removal. The migration copies under a freeze and commits one
generation once.

### Delete or lifecycle-expire objects in version 1

Rejected because deletion creates the restore/reference safety problem that
the first no-EFS release does not need to solve. Retention is cheaper and
simpler than a speculative restore-aware garbage collector.

### Keep shared paths only for logs or SSH

Rejected because any retained shared path keeps the PVC, CSI, IAM, mount, and
recovery support surface alive. Logs use immutable segments; secrets use
projected identity.

### Ignore historical state

Rejected because stale but referenced artifacts can break recovery after pod
loss. Every retained reference is classified or cutover is blocked.

## Open gates

- Independent GO review of this exact design diff.
- Review and apply one dedicated, surgical versioned S3/KMS/IAM slice; the
  current account and Pod Identity role are known not to satisfy the contract.
- Concrete guarded-HA upload, materialization, log-spool, cache, inode, and
  concurrency defaults must be selected from measured production workload in
  D3/D4 and approved before either runtime path merges; no unbounded fallback is
  permitted.
- Exact product decision for externally supplied mutable SSH-node-pool keys in
  guarded HA; current production has the feature disabled.
- Implementation and test completion for all stack items.
- Isolated migration rehearsal and production change approval.
- Evidence-backed proof of base-filesystem exclusivity before any filesystem
  deletion.

Until these gates pass, production EFS remains a real dependency and must not
be removed.
