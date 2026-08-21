# Stateless HA control-plane storage

Status: Proposed canonical design. The destination and migration contract are
defined, but application support, infrastructure, migration tooling, and the
production cutover are not implemented. This document does not authorize a
deployment or deletion. It may merge after independent review of its exact
diff; implementation and production mutation remain separately gated.

Last updated: 2026-08-21

Canonical owner: this file owns durable control-plane bytes and removal of the
SkyPilot EFS dependency in guarded HA. The API/executor/controller split,
PostgreSQL request delivery, controller leadership, and Serve actuation remain
owned by docs/designs/multi-replica-api-server.md. Reserved-capacity placement
remains owned by docs/designs/serve-multi-pool-reserved-capacity-fill.md.

## Decision

Guarded production HA has one steady-state storage path:

- PostgreSQL is the sole structured and transactional authority.
- A private, versioned, server-owned S3 bucket or dedicated server-owned prefix
  stores immutable upload archives and durable log segments with SSE-KMS.
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

The live EFS payload is about 25.26 GB, while PostgreSQL already contains the
structured state. Bulk archives and logs therefore move to S3, not into
Aurora.

## Goals

- Remove EFS as a correctness, availability, recovery, and support dependency
  for the production 2/2/2 control plane.
- Preserve existing upload, request-log, managed-job, Serve, cluster, config,
  and generated-SSH behavior across total pod loss.
- Make every durable opaque byte immutable, content-verified, and referenced by
  an exact PostgreSQL row.
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
- Object deletion, lifecycle expiration, and storage-cost optimization are not
  part of version 1.

## Current production evidence

Read-only evidence captured on 2026-08-21 shows:

- Helm revision 469 runs SkyPilot 1.1.1396 as two API, two executor, and two
  controller pods.
- All six roles use PostgreSQL but still mount skypilot-state-rwx at
  /root/.sky, /root/.ssh, and /root/sky_logs.
- Guarded HA currently rejects storage.enabled=false and requires
  storage.accessMode=ReadWriteMany.
- The claim uses storage class efs-rwx and access point
  fsap-027d9430f450bb777 on fs-00a7dd95ad52c0ade. The access-point path is
  /dynamic_provisioning/pvc-8001cb94-a060-402c-be6a-7899d9dd972c.
- The filesystem reported 25,256,345,600 bytes, all in Standard storage.
- The filesystem is tagged as shared Kubernetes state even though the current
  inventory found one access point. Deleting the SkyPilot claim and access
  point does not by itself prove that the base filesystem and mount targets are
  exclusive.

This evidence must be refreshed immediately before migration. A second
SkyPilot authority root, an unreadable path, or an unclassified retained
reference blocks cutover.

## Source readiness and gaps

Literal EFS removal is not source-ready. The current seams and missing work are:

| Surface | Already authoritative | Still filesystem-dependent | Required change |
| --- | --- | --- | --- |
| API requests | PostgreSQL request, queue, lease, retention-pin, and terminal state | Upload bytes and request logs | S3 object catalog/materializer and durable log segments |
| Upload blobs | Content-addressed blob IDs and PostgreSQL references from requests/jobs | LocalFilesystemBlobStorage is the only backend; resolve returns a permanent path | Built-in S3 backend and scoped materialization lifetime |
| Managed Jobs | DAG, environment, config, and original YAML are stored in PostgreSQL | Legacy disk fallbacks, blob bytes, controller/task logs | Validate retained rows, remove guarded-HA fallbacks, use S3/log provider |
| Serve | Version YAML, submitted YAML, placement, controller snapshots, and recovery scripts are in PostgreSQL | Up/update staging, controller files, replica launch artifacts, and logs | Transactional admission payload, local reconstruction, S3 logs |
| Server config | Canonical server config is in PostgreSQL | Startup still seeds and synchronizes ~/.sky/config.yaml through shared storage | Read-only bootstrap plus local projection from PostgreSQL |
| Generated SSH | Generated per-user keypairs and cluster YAML are in PostgreSQL | /root/.ssh is shared; SSH-node-pool uploads use local key paths | Local regeneration; externally supplied keys only from projected Secrets |
| Caches and scratch | Derivable from durable state | Catalogs, locks, wheels, generated YAML, debug files, and request stages use shared roots | Bounded emptyDir only |
| Helm | Role split and PostgreSQL guards exist | Every HA role mounts one RWX claim and HA hard-fails without it | PVC-free guarded-HA render with bounded ephemeral storage |

The existing BlobStorage abstraction is path-oriented and has no S3
implementation. The existing LogProvider abstraction reads local files but
does not own durable writing or indexing. They are useful seams, not completed
object-store support.

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

### S3

The S3 location is private, versioned, owner-enforced, and encrypted with one
approved KMS key. Public access is blocked and TLS is required. Application
roles can put, head, and get only their server-owned roots. They cannot list
unbounded prefixes, delete versions, change bucket configuration, or alter the
KMS key.

Every committed object record includes:

- provider, bucket, key, and exact version ID;
- expected bucket owner and KMS key ARN;
- logical object kind and owner;
- byte count and SHA-256 digest;
- creation and commit timestamps from PostgreSQL; and
- the storage generation and database incarnation that created it.

Keys are server generated and include a random immutable object ID. Content
digests verify bytes; they are not used alone as authorization or as the S3
key. Reads always request the exact committed version and verify size and
digest. Publication uses If-None-Match: * and the bucket policy denies a
nonconditional PutObject in the authoritative roots, so one logical object key
cannot acquire competing versions.

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

The upload path:

1. Streams parts into a bounded per-upload local directory while computing size
   and SHA-256.
2. Publishes one immutable S3 object with server-generated identity,
   If-None-Match: *, and required SSE-KMS controls.
3. Reads the exact returned version metadata and verifies owner, size,
   encryption, and checksum. If the provider checksum is unavailable, it
   performs an exact-version read verification before commit.
4. Commits the object row and owning request/job reference in PostgreSQL.
5. Removes the local staging directory.

Retries use the durable upload/object ID. A conditional collision is reconciled
by an exact-key head/read and accepted only after version, metadata, and content
verification. A lost acknowledgement cannot select a prefix-list winner,
overwrite bytes, or create a second reference.

BlobStorage gains a scoped materialization interface rather than exposing a
permanent shared directory. A consumer enters a materialization context, the
provider downloads and verifies the exact version into its bounded local
directory, and exit removes it. Long-lived controllers rematerialize after
takeover.

### Log publication and reading

One common log writer serves API requests, Managed Jobs, Serve controllers, and
replica launch operations. It creates immutable ordered segments and commits
their metadata in PostgreSQL. Segment order is a monotonically allocated stream
sequence, not S3 listing order. Each segment uses the same conditional
publication and lost-ack reconciliation contract as a blob.

The writer flushes on a bounded time or size threshold and before publishing a
terminal lifecycle transition. A hard process kill may lose only an
uncommitted local tail; the stream records a typed tail-gap marker rather than
silently pretending the bytes exist. This matches the existing process-buffer
durability boundary while making it visible.

Readers combine committed segments with a live local tail only when connected
to its current owner. Reconnection or owner loss resumes from committed
segments by stream sequence. Existing stream/download API shapes remain
unchanged, and no client receives S3 credentials.

Version 1 retains committed log objects indefinitely. Product-facing
expiration is not introduced by this migration.

### Managed Jobs

Before cutover, validation proves that every retained nonterminal job has its
required DAG, environment, config, original YAML, and blob reference in
PostgreSQL. Missing or ambiguous legacy rows block migration and are repaired
with product-owned evidence; they are not guessed from filenames.

After cutover, guarded HA materializes PostgreSQL content and exact S3 blobs
locally. Disk fallback remains only for explicit local/non-HA compatibility and
cannot be selected in guarded HA. Controller and task logs use the common log
writer.

### Serve

Serve up/update admission becomes self-contained before it returns success.
Normalized task YAML, submitted YAML, placement, and sanitized controller
configuration commit in the existing version transaction. The elected
controller reconstructs its local directory from that committed state.

Cross-role filesystem staging is removed. Replica launch artifacts and logs use
the common object/log path. Existing PostgreSQL controller snapshot and
takeover behavior remains authoritative.

### Config and SSH

The Helm ConfigMap is immutable bootstrap input, not mutable runtime authority.
The API server validates and stores the canonical config in PostgreSQL; every
role projects it into its own ephemeral path.

Generated SSH keypairs and cluster YAML continue to come from PostgreSQL.
Externally managed key files are read-only Secret projections. /root/.ssh is no
longer a persistent mount.

### Caches

Catalogs, generated cluster files, locks, wheels, debug dumps, and provider
caches are either reconstructed or explicitly projected. Every derived cache
is bounded and disposable. A cache miss can affect latency but cannot change
ownership, lifecycle, placement, or recovery.

## Database restore behavior

An authorized database restore allocates a fresh database-incarnation identity
before application writers become Ready. Restored exact object references
remain readable because version 1 never deletes authoritative S3 objects.
Fresh writes use keys in the new incarnation and cannot overwrite an old
version.

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
2. Provision or select the minimum compliant S3/KMS/IAM boundary. Reuse a
   suitable server-owned bucket/key when it meets the exact policy; otherwise
   add one surgical infrastructure slice.
3. Inventory the exact PVC, PV, access point, EFS root, path types, byte counts,
   ownership, modes, symlinks, and PostgreSQL retained references.
4. Classify every path as structured state already in PostgreSQL, an object to
   import, derived disposable state, or explicitly unsupported residue.
   Unknown, special, unreadable, multiply owned, or unreferenced-but-required
   content blocks cutover.
5. Run the importer and recovery tests against an isolated copy and isolated
   PostgreSQL database. Provider mutation is fenced during rehearsal.
6. Render the exact PVC-free Helm release and prove that no guarded-HA pod,
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
   object rows, references, manifest digest, and one S3_V1 generation receipt in
   one PostgreSQL transaction.

Before the generation transaction commits, the old EFS release may be
restarted after reconciling the failed attempt. After commit, EFS can no longer
be selected as authority.

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

The minimum clean stack is:

1. Design plus additive PostgreSQL storage-generation, object-catalog, object-
   reference, log-stream, log-segment, and migration-receipt schema.
2. Built-in S3 object provider, exact-version verification, bounded upload
   spool, and context-managed materialization; route all file-mount uploads
   through it.
3. Common durable log writer and reader for requests, Managed Jobs, Serve, and
   replica operations, including reconnect and typed-tail-gap behavior.
4. Remove guarded-HA filesystem authority from Managed Jobs, Serve staging,
   config projection, generated SSH, and caches.
5. Render guarded HA without PVCs; add disk-backed emptyDir size limits,
   ephemeral-storage resources, and Helm tests that reject local authority.
6. Add the one-shot inventory/import/rehearsal/cutover command and production
   verification bundle.
7. Keep the transition cleanup as a stacked draft from the start; merge it only
   after the acceptance gates below.
8. Apply the minimum static S3/KMS/IAM addition and the later exact EFS client-
   path removal. These changes remain surgical and do not own the SkyPilot Helm
   release.

This is not a Helm-only change. The current estimate is at least 25
production/chart files, at least 18 test/design/migration files, one central
PostgreSQL migration, one importer/cutover command, and 6--8 stacked
application PRs plus the narrow infrastructure slices. A planning range is
4,000--7,000 production lines and 4,000--6,000 test, migration, and design
lines. The common log path is the largest portion.

## Acceptance gates

### Source

- PostgreSQL migration upgrades, downgrade boundaries, idempotent retry, and
  mixed-version rejection pass against real PostgreSQL.
- Upload acknowledgement loss, duplicate retry, wrong owner, wrong version,
  digest mismatch, truncation, local ENOSPC, and pod deletion are covered.
- Request, Job, Serve, and replica log streams reconnect from another API pod
  and retain ordered committed segments.
- Managed Job and Serve recovery use only PostgreSQL, exact S3 versions,
  Secrets, and emptyDir in guarded HA.
- Guarded-HA Helm rendering contains no PVC, persistentVolumeClaim,
  /root/.sky, /root/.ssh, or /root/sky_logs persistent mount.
- Every ephemeral volume has size and pod ephemeral-storage bounds.
- Runtime and importer identities cannot delete S3 object versions.

### Migration

- The final EFS manifest covers every path and byte with no unknown or
  unsupported retained item.
- Every active or nonterminal request/blob, job, service/version/controller,
  cluster, config, and generated-SSH reference resolves before freeze.
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
- EFS receives no reads or writes after activation.
- The acceptance horizon covers immediate, +10 minute, +30 minute, and one
  complete request/job/Serve recovery interval. Any reset repeats the horizon.
- The cleanup diff deletes the old guarded-HA path and leaves one steady-state
  implementation.

## Rollback boundary

- Before the S3_V1 generation commits, abort, reconcile temporary immutable
  uploads as retained garbage, and restart the unchanged EFS release.
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
- Final choice between a compliant existing server-owned S3 boundary and one
  new minimal bucket/key slice.
- Exact product decision for externally supplied mutable SSH-node-pool keys in
  guarded HA; current production has the feature disabled.
- Implementation and test completion for all stack items.
- Isolated migration rehearsal and production change approval.
- Evidence-backed proof of base-filesystem exclusivity before any filesystem
  deletion.

Until these gates pass, production EFS remains a real dependency and must not
be removed.
