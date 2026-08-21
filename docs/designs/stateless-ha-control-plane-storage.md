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
| Server config | Canonical server config is in PostgreSQL and database-backed Helm installs reject inline config | Startup still seeds and synchronizes ~/.sky/config.yaml through shared storage | Seed an empty PostgreSQL row once, read PostgreSQL directly, and use only optional pod-local projections |
| Generated SSH | Generated per-user keypairs and cluster YAML are in PostgreSQL | /root/.ssh is shared; SSH-node-pool uploads use local key paths | Local regeneration; externally supplied keys only from projected Secrets |
| Caches and scratch | Derivable from durable state | Catalogs, locks, wheels, generated YAML, debug files, and request stages use shared roots | Bounded emptyDir only |
| Helm | Role split and PostgreSQL guards exist | Every HA role mounts one RWX claim and HA hard-fails without it | PVC-free guarded-HA render with bounded ephemeral storage |

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
- migration inventory, item result, manifest digest, and cutover receipt; and
- all existing request, queue, job, Serve, config, cluster, and generated-SSH
  structured state.

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
  generation, database-incarnation UUID, and the committed cutover receipt;
- logical upload heads unique on
  `(storage_generation, tenant_id, object_kind, logical_blob_id)`, containing a
  monotonic session epoch and current session ID. The head is the only mutable
  rendezvous pointer; advancing it requires a locked compare-and-swap from the
  exact terminal predecessor;
- upload sessions keyed by a server-generated UUID and unique on
  `(upload_head_id, session_epoch)`, with expected chunk count, immutable
  per-session limits, lease owner/epoch/expiry, current attempt epoch, state,
  and optional predecessor session. Concurrent first chunks converge on the
  current row. An active conflicting chunk count or limit set is rejected. A
  terminal pre-publication session remains immutable audit evidence but may be
  superseded by one freshly fenced session epoch; concurrent successor creation
  converges through the upload-head compare-and-swap;
- upload attempts unique on `(upload_session_id, attempt_epoch)`, with a fresh
  object UUID/key, S3 multipart upload ID, and terminal outcome. A rejected or
  conflicted immutable attempt is retained and a successor attempt never reuses
  its key;
- upload-part receipts unique on
  `(upload_session_id, attempt_epoch, part_number)`, containing exact byte
  count, part checksum, ETag, and a provider-call lease epoch;
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
alias wins and no successor is created. Old sessions and attempts are never
rewritten or reused.

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

Incomplete multipart parts are not committed objects. The runtime may call
`AbortMultipartUpload` for its exact recorded upload, and a prefix-scoped
`AbortIncompleteMultipartUpload` lifecycle rule removes abandoned parts after
the documented retry horizon. No lifecycle rule expires completed current or
noncurrent object versions in version 1.

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
   head. A retry returns the same active attempt when its immutable plan
   matches. A conflicting plan is rejected while that session is active; after
   a terminal pre-publication abort, one compare-and-swapped successor session
   epoch may adopt the new plan. A published alias always returns the existing
   object. One lease epoch owns multipart creation or finalization at a time.
2. Each parallel chunk request may land on any API replica. It validates chunk
   number/count and HTTP bounds, spools at most that chunk to a reserved local
   path while computing its checksum and size, uploads the corresponding S3
   part, and commits the part receipt under the attempt. A row/advisory lease on
   `(session, attempt, part)` permits only one provider call for a part at a
   time, preventing a concurrent overwrite from disagreeing with its receipt.
   Part numbers are
   bounded by S3's 10,000-part limit and every non-final part satisfies S3's
   minimum size. A duplicate part is accepted only if its durable checksum and
   size match.
   A provider-call lease that expires or loses its owning process is never
   reassigned against the same multipart upload: the attempt becomes
   `REJECTED`, its exact upload is aborted best-effort, and a fresh
   attempt/upload ID/key requires the client to resend all parts. This prevents
   a late, unfenceable `UploadPart` response from changing a part after another
   owner recorded its receipt.
3. A finalizer locks the complete receipt set, conditionally completes the
   multipart upload whose create request fixed the required SSE-KMS controls,
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
incomplete parts; the narrow abort lifecycle handles them. A crash after object
completion but before PostgreSQL publication is recovered from the exact
session key and conditional-write invariant. A lost acknowledgement cannot
select a prefix-list winner, overwrite bytes, or create a second logical alias.
Digest, encryption, owner, size, or format failure rejects that immutable
attempt and requires a fresh attempt/key; it can never overwrite or bless the
bad version. No step requires all chunks or retries to reach one pod.

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

Each stream has one PostgreSQL lease owner and monotonic lease epoch. Segment
and gap identities are unique on `(stream_id, sequence)` and carry the writer
epoch, so a deposed writer cannot append after takeover. The writer flushes on
a bounded time or size threshold and before publishing a terminal lifecycle
transition.

A hard-killed writer cannot write its own gap marker. After the old lease
expires, the next owner first commits an `OWNER_LOSS_TAIL_GAP` at the next
sequence in the same transaction that acquires the new epoch, unless the prior
owner durably flushed and closed the stream. The marker identifies the lost
owner/epoch and last committed sequence; it does not invent an unknowable byte
count. Only then may the new owner append. Thus a hard process kill may lose an
uncommitted local tail, but recovery exposes that durability boundary instead
of silently pretending the bytes exist.

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
DML: they require PostgreSQL, read that row directly, and fail closed if it is
absent or invalid. They never overlay a ConfigMap or shared file. Configuration
updates commit in PostgreSQL and reload per-process state; any compatibility
path that requires a file receives a versioned pod-local projection and may
recreate it after restart. The config reload lock is pod-local because
PostgreSQL, not a filesystem lock, serializes durable updates.

The final guarded-HA chart has no config ConfigMap correctness dependency. It
may omit the ConfigMap entirely for guarded HA; calling it immutable is not a
substitute for PostgreSQL authority. Explicit non-HA installations retain their
separate file/ConfigMap compatibility behavior.

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

The `S3_V1` commit records the minimum storage capability and qualified image
and chart digests in its receipt. Activation is refused unless the database
head, importer manifest, quiescence evidence, exact S3/KMS policy probe, and
intended deployment digests match. After that commit, an S3-capable process
that cannot implement the recorded protocol fails readiness; it cannot select
EFS. Native rollback to a pre-feature image is unsupported and fails the
database-head gate. The stacked cleanup removes `LEGACY_EFS` code after the
acceptance horizon.

A fresh guarded-HA database never bootstraps through EFS. The same activation
command has a fresh-install mode that requires an empty product database, the
release fence Secret, qualified image/chart identities, and successful
exact S3/KMS/IAM probes, then inserts the first authority row directly as
`S3_V1` with a unique install receipt. It refuses a nonempty or previously
initialized database. During the transition stack, upgrades of an existing
installation initialize `LEGACY_EFS`; after D10, fresh guarded-HA installation
supports only the receipt-backed `S3_V1` bootstrap. No normal pod startup or
Helm render chooses a protocol automatically.

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

An authorized restore is a separate, one-shot operator transaction while all
application writers and provider mutation are fenced. The operator generates a
fresh UUID, updates the incarnation field in the release fence Secret, and runs
the restore command with the expected restored UUID, fresh UUID, immutable restore
operation ID, and the existing control-plane authorization evidence. The
command locks the storage-authority row, compare-and-swaps only the expected old
incarnation, inserts a unique restore receipt, and commits the fresh
incarnation. A retry with the same operation and values returns the receipt; a
different value or already-advanced incarnation fails closed. Runtime roles
have no code path that performs this rotation during normal startup.

The restore command first proves the restored row already satisfies the
non-lowerable protocol/generation floor. It may rotate only incarnation; it
cannot change protocol or generation. A pre-S3_V1 snapshot is therefore not a
valid restore source after cutover. The operator may update the incarnation
field of the fenced Secret only while all roles are stopped; a failed CAS
leaves them fail-closed until the operator restores the prior incarnation input
or completes the authorized repair.

Only after that transaction and the existing provider-authority recovery gates
pass may application writers become Ready. Restored exact object references
remain readable because version 1 never deletes authoritative S3 objects. Fresh
writes include the new incarnation in their server-generated keys and cannot
overwrite an old version.

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
4. Capture a final exact EFS inventory and copy required opaque bytes to
   immutable S3 versions.
5. Verify every object twice by exact version, size, and digest. Commit imported
   object rows, logical aliases, owner references, manifest digest, qualified
   image/chart identities, and one S3_V1 generation receipt in one PostgreSQL
   transaction.
6. After that commit, advance the stopped release's non-lowerable fence Secret
   to the committed S3_V1 generation, then start only the PVC-free release. A
   failure after step 5 remains a fix-forward outage; it cannot reopen EFS.

Before the generation transaction commits, the qualified transition release
may be restarted in `LEGACY_EFS` mode after reconciling the failed attempt. A
pre-feature image is already rejected by the central schema head. After commit,
EFS can no longer be selected as authority.

### Fix-forward activation

1. Deploy the PVC-free 2/2/2 release with direct Helm, immutable image/chart
   identity, retained values, and --reuse-values.
2. Require every role to read the committed S3_V1 generation and pass
   PostgreSQL, S3 exact-version, KMS, local-space, and projected-identity
   readiness probes.
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
2. **D1 -- PostgreSQL config authority.** Stop guarded-HA roles from copying or
   locking config on RWX; seed the PostgreSQL row only in explicit
   bootstrap/upgrade migration modes, make verify/runtime read-only and
   fail-closed, and preserve every retained row. This is the first bounded
   source PR because it removes one real EFS correctness edge without inventing
   the object protocol. It depends only on D0.
3. **D2 -- inert PostgreSQL storage foundation.** Add the protocol/generation,
   incarnation/restore receipt, upload-head/session/part,
   object/alias/reference,
   log-stream/segment/gap, and migration-receipt schema plus the repository that
   accepts caller-owned transactions on the existing mandatory central
   migration head. The migration creates no authority row and changes no
   runtime routing. A separately invoked, idempotent `initialize-legacy`
   command records the exact retained installation, expected incarnation, EFS
   identity, and qualified transition digest before those processes start; it
   refuses a fresh/nonmatching database. A fresh database remains unactivated
   for D8's validated S3_V1 bootstrap. It depends on D0; D1 may merge before or
   with it.
4. **D3 -- exact S3 provider and cross-replica upload.** Add conditional
   multipart publication, checksums, exact-version verification, lost-ack
   recovery, bounded part spool, lifecycle/policy probes, logical aliasing, and
   the object `PUBLISHED_UNREFERENCED` state and upload-session `PUBLISHED`
   state. Integrate owner references into request,
   Managed Job, and Serve admission transactions. It depends on D2 and the
   static S3 boundary.
5. **D4 -- scoped materialization.** Replace permanent blob paths in guarded HA
   with checked, bounded context-managed materialization and route every
   file-mount consumer through it. It depends on D3.
6. **D5 -- common durable logs.** Add leased writers, immutable segments,
   next-owner gap records, readers, downloads, and reconnect for requests,
   Managed Jobs, Serve controllers, and replica launches. It depends on D2 and
   the S3 provider from D3, but can proceed in parallel with D4.
7. **D6 -- remove remaining product filesystem authority.** Make Managed Jobs,
   Serve staging/reconstruction, generated SSH, catalogs, locks, wheels, and
   caches use PostgreSQL, exact S3 versions, Secrets, or bounded local state.
   It depends on D4/D5 for the surfaces it consumes.
8. **D7 -- PVC-free chart.** Render guarded HA with disk-backed bounded
   emptyDir and no `/root/.sky`, `/root/.ssh`, `/root/sky_logs`, PVC, PV, or EFS
   CSI reference. Add resource limits, readiness probes, and negative render
   tests. It depends on D1 and all applicable D3--D6 runtime removals.
9. **D8 -- importer, rehearsal, and activation command.** Inventory EFS and
   PostgreSQL, import exact versions, construct owner references, commit the
   one-way generation, support receipt-backed fresh-database S3_V1 bootstrap,
   and emit the verification bundle. Add a separate explicit restore CAS
   command; importer, cutover, Helm migration, and normal startup never rotate
   incarnation. It depends on D2--D7.
10. **D9 -- production activation.** Apply the dedicated minimal S3/KMS/IAM
    slice with the completed-object delete deny and conditional-write policy,
    deploy the transition image in `LEGACY_EFS`, rehearse,
    freeze/import/commit, and deploy the PVC-free `S3_V1` chart. Static S3
    infrastructure can be prepared in parallel after D2, but activation depends
    on D8.
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
  mixed-version rejection pass against real PostgreSQL.
- Parallel chunks deliberately routed across both API replicas produce one
  session, one logical alias, and one exact object without sticky routing.
- Upload allocation, multipart creation/completion acknowledgement loss, `409`,
  `412`, duplicate/conflicting parts, wrong tenant/owner, wrong version, wrong
  KMS key, digest mismatch, truncation, local ENOSPC, and pod deletion are
  covered.
- An active upload rejects a conflicting chunk plan; an aborted unpublished
  session can be superseded by exactly one higher session epoch; immutable old
  audit rows remain unchanged; and a published alias prevents all successors.
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
  INSERT, missing/invalid-row fail-closed, cross-role reload, and zero
  shared-file read/write are covered.
- Normal startup cannot rotate database incarnation; explicit restore CAS,
  idempotent receipt, stale expected-incarnation rejection, and readiness
  mismatch are tested against real PostgreSQL.
- A restored authority row below the release's S3_V1 generation floor is
  rejected and cannot be upgraded by the restore command; runtime and normal
  Helm paths cannot lower that floor.
- A fresh empty guarded-HA database can bootstrap directly to receipt-backed
  S3_V1 after exact policy/capability checks, while a nonempty or previously
  initialized database cannot use that path.
- Guarded-HA Helm rendering contains no PVC, persistentVolumeClaim,
  /root/.sky, /root/.ssh, or /root/sky_logs persistent mount.
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
- One PostgreSQL transaction commits the complete reference set and S3_V1
  generation; partial commit is impossible.

### Production

- Two API, two executor, and two controller pods become Ready with no PVC.
- Complete deletion of all six pods recovers from PostgreSQL/S3 without manual
  file repair.
- Uploads, log streaming, retained Managed Jobs, Serve takeover, cluster
  status, config, and generated SSH all pass.
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

- Before the S3_V1 generation commits, abort, reconcile temporary immutable
  uploads as retained garbage, and restart the qualified transition release in
  `LEGACY_EFS` mode. Pre-feature images remain rejected by the schema head.
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
