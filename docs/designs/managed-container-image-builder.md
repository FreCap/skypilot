# SkyPilot Managed Container Image Builder

_Created: 2026-07-19_

_Status: target design reshaped; non-public prototype and worth gate precede
durable product implementation_

## Decision

SkyPilot should offer an explicit, content-addressed image builder that turns a
base image, a bounded local context, and build-only setup into one verified OCI
artifact. It reuses the immutable catalog and distribution contract in
[`managed-container-image-distribution.md`](managed-container-image-distribution.md),
but its data plane is independently deployable and has separate storage,
PostgreSQL metadata, workers, quotas, security, and release gates.

The first delivery is intentionally not this permanent product surface. A
non-public prototype runs the pinned build graph, sandbox, internal OCI staging,
trusted publication, and representative Boltz/non-Python benchmarks without a
central migration, task field, durable controller, or dashboard mutation. Only
after that prototype passes the pre-product gate below may migration 024,
API-version-63 syntax, the controller, and Build UX be implemented. This avoids
turning an unproven convenience into a permanent compatibility and security
obligation.

The useful Modal-like property is that construction is declared and cached
independently from replica startup. Placement never runs a Docker build or
repeats build-only setup on every replica. SkyPilot does not copy Modal's
private runtime assumptions, lazy filesystem, or memory snapshots. The output
is a normal digest-pinned OCI image usable by Docker and containerd.

## Interface and semantics

The explicit command is the primary workflow:

```text
sky image build --file image-build.yaml [--wait | --no-wait]
sky image build status BUILD_ID
sky image build retry BUILD_ID
sky image build logs BUILD_ID
sky image publish --artifact-id BUILD_OUTPUT_ID --release NAME
```

The same specification may appear in a task:

```yaml
resources:
  accelerators: L4:1
  container_image:
    distribution: global-gpu
    build:
      base:
        artifact_id: 019f5a80-8bc9-7cf2-9fa8-0123456789ab
      platform: linux/amd64
      context:
        root: .
        include:
          - pyproject.toml
          - uv.lock
          - src/
        destination: /opt/boltz
      workdir: /opt/boltz
      build_user: "0"
      shell: [/bin/sh, -euc]
      env:
        UV_LINK_MODE: copy
      setup: |
        uv sync --frozen
        python -m compileall src

# Runtime setup remains placement-dependent and runs on every replica.
setup: |
  test -r /models/current/model.safetensors
```

This YAML and the commands below describe the post-gate product contract; they
are not registered while only the prototype ships. `container_image.build` is
mutually exclusive with selecting a final image by
`ref`, `release`, or `artifact_id`. `distribution` selects the output profile.
V1 accepts only a profile whose builder adapter proves trusted canonical
publication, verification, ownership, and deletion capabilities from the
configured build cluster. The first qualifying adapter is managed AWS ECR;
external or non-AWS profiles remain distribution-only until they pass a
builder-specific capability gate.
V1 deliberately does not reserve or publish a release from build submission,
because the output digest does not exist yet and a name-to-build-spec claim is
not the catalog's permanent name-to-digest contract. The successful task pins
the returned artifact ID. A caller that wants a human name subsequently runs
`sky image publish --artifact-id BUILD_OUTPUT_ID --release NAME`; that ordinary
publication uses the already-READY fast path. The dashboard exposes the same
Publish action after build completion. Build `setup` is opt-in and never
aliases, moves, copies, or suppresses top-level runtime `setup`.

Context roots are client-local and have one unambiguous resolution rule. In a
YAML task or direct build file, a relative `context.root` is resolved against
the directory containing that YAML file, not the process working directory. A
programmatically constructed task must provide either an absolute root or an
explicit absolute `base_dir`; a relative root without that base is rejected.
The scanner resolves the root once, applies includes beneath that root, and
rejects any resolved include or symlink target that escapes it. Neither the
root nor `base_dir` is serialized to the server. After scanning, build resolve
replaces them with the logical root-manifest digest and, on a miss, the eventual
committed context manifest ID.

The builder API accepts only an ACTIVE catalog artifact with a compatible READY
managed route as its base. It never resolves a public registry, performs DNS,
or fetches OCI metadata in an API handler. CLI/YAML convenience may name an
immutable release or digest-pinned source; before builder submission the client
uses the ordinary catalog register/prepare flow, waits for READY, and replaces
that selector with the artifact ID. Mutable tags and direct private-registry
credentials remain unsupported. The build spec keys on the base manifest digest
and platform, so digest-equivalent catalog aliases converge without network
resolution or a separately trusted base-config digest.

The v1 build graph is exact and versioned:

1. Resolve one OCI base manifest digest and config for the requested platform.
2. Start one BuildKit LLB filesystem state from that digest.
3. Copy the canonical context tree to the absolute, normalized
   `context.destination`. The destination must not be `/`, `/proc`, `/sys`,
   `/dev`, or another reserved prefix. Context files remain in the final image.
4. If and only if nonempty `setup` is present, execute exactly one process with
   `argv = shell + [setup]`, cwd `workdir`, UID/GID from `build_user`, and only
   the base environment plus declared non-secret `env`. Defaults are
   `workdir=context.destination`, `build_user="0"`, and
   `shell=["/bin/sh", "-euc"]`. Those four execution fields are rejected when
   setup is absent because they would have no effect.
5. Export the resulting filesystem while preserving the base image's OCI
   entrypoint, command, environment, labels, exposed ports, stop signal,
   healthcheck, final user, and final working directory. Build-only `env`,
   `workdir`, `build_user`, and `shell` do not alter final OCI config.

The context filesystem encoding is also versioned. Paths are Unicode NFC,
slash-separated, and emitted in ascending UTF-8 byte order. Directories are
synthesized before children with mode `0755`; regular files normalize to
`0644` or `0755` from the executable bit; symlinks use `0777` and retain the
already validated relative target. Every entry has UID/GID 0 and mtime 0.
Hard links are materialized as independent regular files. Sparse extents,
xattrs, ACLs, devices, sockets, FIFOs, and platform-specific metadata are
rejected or omitted and cannot affect the graph. This canonical tree is the
only local input mounted into the LLB, so host archive ordering and ownership
never enter the build.

The final config transformation is exact. The selected base config is decoded
as OCI JSON; its runtime `config` fields, author, OS, architecture, variant,
`os.version`, and `os.features` are preserved, with the requested platform
required to match. The export preserves the ordered base layer blob digests,
sizes, and matching `rootfs.diff_ids`/history prefix. V1 accepts ordinary OCI
and Docker schema-2 gzip bases through one versioned one-to-one media-type map:
Docker config/layer types become their OCI config/layer equivalents without
changing blob digest or size. Layer descriptors with URLs, foreign or
nondistributable media types, annotations, or an unrecognized compression/media
type are rejected. This is deliberate normalization, not a claim that BuildKit
preserves arbitrary descriptor bytes.

The graph appends one verified context layer and, only when setup is present,
one verified setup layer plus corresponding normalized history entries. New
history records omit `created` and `author`, use fixed versioned `created_by`
strings with no user command text, and set `empty_layer` from the actual diff.
Top-level `created` is omitted. Unknown config extensions are rejected at
admission until the frontend version defines their preservation, and Docker
healthcheck is preserved through its explicitly versioned config extension.
Canonical JSON serialization is UTF-8, sorted-key, compact JSON. The
normalization-map version enters the compatibility hash. OCI and Docker golden
bases prove the accepted mapping before product migration 024 is allowed.

The implementation surface is pinned, not merely described as "BuildKit". The
Helm values name an immutable qualified `buildkitd` image digest and an immutable
SkyPilot gateway-frontend image digest. The controller invokes `gateway.v0`
with that digest; the custom frontend alone translates the normalized spec
into direct LLB and returns the result reference plus the exact
`containerimage.config`. The canonical context already has mtime zero. Setup
receives platform-owned `SOURCE_DATE_EPOCH=0` as a best-effort tool hint, but the
exporter does not rewrite inherited layers and arbitrary setup remains
explicitly nonreproducible.
The controller uses the BuildKit client API rather than Dockerfile or buildx
defaults. The sandbox receives no AWS/GCP registry credential. It receives a
short-lived token for an internal OCI staging registry, scoped to one random
attempt repository/tag and byte/request ceiling, and exports the single-platform
result with this fixed attribute set:

```text
type=image
push=true
oci-mediatypes=true
compression=gzip
compression-level=6
force-compression=false
```

The frontend emits no provenance or SBOM attestation result, and the controller
does not request one. The staging registry is a separately authenticated OCI
Distribution service, optionally backed by S3 or R2, never a raw object-store
URL and never a runtime fallback. A trusted publisher outside the tenant
sandbox reads the exact staging digest, verifies it, and alone holds canonical
registry credentials. AWS publication uses the paced ECR adapter from the
distribution design, so opaque BuildKit traffic cannot bypass account/region
ECR limiters.

The registry-cache exporter targets a separate workspace/platform namespace in
that internal registry and pins
`type=registry`, `mode=max`, `image-manifest=true`, the same OCI/gzip settings,
and `ignore-error=true`; a caller cannot override any exporter attribute.
Cache import failure is a recorded miss. Cache export is best effort and may be
ignored only after the mandatory final staging export succeeds; a failed cache
write produces no cache record. Final staging export, verification, and
canonical publication are never ignored. The
BuildKit daemon digest, frontend digest and protocol version, LLB definition
version, config-transform version, canonical context-metadata policy,
compression settings, cache exporter settings, and attestation policy are all fields in a
`builder_compatibility_hash` that enters both the spec hash and every cache
record. An upgrade that changes any field is a new compatibility generation,
never an in-place cache interpretation change.

After export and before catalog reservation, the trusted publisher fetches the
exact staging root and performs an independent verifier pass. V1 requires one
OCI image manifest, not an index; the exact ordered base-layer digest/size
prefix with only the versioned Docker-to-OCI media-type normalization;
one new context descriptor and zero or one setup descriptor; exact config and
descriptor digests and sizes; OCI gzip media types for new layers; the requested
platform; canonical config bytes and normalized history; and new-layer diff IDs
obtained by streaming only those appended layers. The pinned base config proves
its unchanged diff-ID prefix, so the verifier never downloads or recompresses
multi-gigabyte base layers. New layers must match the pinned gzip profile.
Extra runnable children, attestation descriptors, base blob mutation,
non-allowlisted descriptor metadata, unknown config fields, unrequested
metadata, and exporter drift fail
closed. Golden builds run against the two pinned images in CI and at deployment
capability probing. A deployment cannot advertise the builder when its BuildKit
version does not satisfy these vectors.

There is no implicit Dockerfile, package manager, shell fallback, cleanup,
second command layer, or context relocation. A base without the declared shell
or build user fails. Setup may deliberately modify any filesystem path allowed
to that user. The builder frontend version identifies this graph. Destination,
workdir, user, shell argv, environment, normalized setup bytes, network-policy
fingerprint, base manifest digest/platform, and context-retention rule are all in the spec
hash, so two implementations cannot interpret one cache key differently.

Build-time work is limited to package installation, compilation, and copying
the explicit context. The LLB and cache identity are deterministic, but an
arbitrary setup command with network access or a nondeterministic tool is not
claimed to produce reproducible output when rebuilt without a cache. Runtime
setup retains mounts, workload credentials, node rank, service discovery,
ports, health checks, mutable model selection, and placement-dependent
behavior. Users choose the boundary; there is no automatic conversion of an
existing task setup script.

A task build is a visible pre-deployment stage. The client first scans and
hashes the logical tree and serializes its small canonical manifest, but it does
not construct a USTAR bundle. `POST /images/builds/resolve` carries the
normalized build spec, catalog base artifact ID, root-manifest digest, tree
version, entry/byte counts, and manifest length/digests. The API performs no
network fetch. It computes the spec hash and runs one authorization-safe
workspace cache lookup. A valid READY match returns immediately with the
existing build/output artifact. No upload session, object capability, validator,
bundle construction, byte reservation, paid attempt, or BuildKit Job exists on
that path. A caller able to name the same workspace and spec may learn only that
workspace's corresponding immutable artifact, which their normal catalog read
permission already exposes.

On a miss, one transaction creates the unique build in `CONTEXT_REQUIRED`,
reserves one bounded pending-upload record plus manifest bytes, snapshots the
base artifact/digest/platform, and creates the random manifest-object intent.
It does not acquire an active-execution slot, paid attempt, bundle-byte
reservation, or registry permit. The client uploads only the manifest. A
sandboxed validator runs only on this miss path. After it records VALIDATED, the
client constructs or spools the strict USTAR bundle, computes its exact
length/SHA-256/MD5 and part evidence, and requests multipart capabilities. That
transaction separately reserves bundle bytes under the pending-upload quota.

Bundle commit pins the immutable context projection and moves the build to
`PENDING` only after revalidating the base's compatible READY catalog route and
acquiring its durable build reference. A missing base closes the intake without
silently choosing another digest. `max_pending_uploads`,
`max_pending_upload_bytes`, and a short abandonment TTL bound slow clients;
`max_pending_builds` bounds the committed scheduler queue. Active execution and
daily paid-attempt quota are reserved only when the scheduler admits a PENDING
build and creates its durable attempt in `PROVISIONING`. If capacity is
unavailable, the build remains PENDING with a closed waiting reason and consumes
neither active execution nor a paid attempt. The client waits for READY before
compute placement. No API worker waits, and no replica independently builds or
adopts a later result.

Concurrent submission of the same `(workspace, spec_hash)` never creates a
second build. After the unique-key conflict is resolved under lock, a still
valid READY row is the cache hit above, a `PENDING` or later active row is a
coalesced `200` response, and a `CONTEXT_REQUIRED` row is returned only to
the uploader that owns its session. Another uploader receives closed
`BUILD_CONTEXT_PENDING` and keeps no reference to the first user's context.
FAILED, CANCELLED, and OUTPUT_RETIRED rows require their explicit lifecycle
operations rather than implicit resurrection through POST. Every unused caller
upload enters the same charged ABORTING/DELETING path. Expiry or cancellation
of the bound pre-commit upload atomically cancels the intake, releases only its
pending-upload count/bytes after confirmed cleanup, and leaves the unique build
history for audited retry. No base or active-execution reference exists before
context commit.

Embedded task builds use a server-issued static policy-preflight token. Before
scanning the filesystem, the client sends the task's value-only declaration to
a bounded endpoint that runs server-side admin policy, validates every effective
resource candidate, and returns the compiled task plus a signed token binding
user, workspace, compiled-task hash, normalized static build declaration,
distribution, `linux/amd64`, context-selection rules, and expiry. The static
declaration includes the base selector, destination, setup/execution fields,
tree/format versions, and include rules, but cannot contain the not-yet-computed
`root_manifest_digest`, context manifest ID, bundle digest, or final spec hash.
Local root and `base_dir` never enter it. Policy may shape this declaration
before the token is issued; it may not introduce or change a build afterward.
V1 rejects an
embedded build before local scanning when any candidate is ARM64, unknown,
missing an image, direct, incompatible, or resolves to a different static build
declaration.

After scanning, `POST /images/builds/resolve` verifies that token, resolves the
catalog base, computes the final spec hash from the static declaration plus base
digest/platform and logical root digest, and durably stores the static-policy
hash, compiled-task hash, root digest, and final spec hash on the build. A cache
hit must match all four fields. The final submission supplies that build ID and
replaces only its build declaration with the exact READY output artifact ID.
The server verifies the substitution against the durable content binding and
the same signed compiled task instead of applying a second possibly divergent
mutation. An expired token can be refreshed only when policy produces the
identical compiled task and static declaration hash; the durable build binding
then remains valid. Otherwise preflight and resolve restart. Raw REST task
submission with an unresolved build, missing static token, or mismatched durable
content binding fails before request persistence.

The SDK/CLI preflight owns that sequencing: after policy preflight it resolves
the catalog base, performs cache resolution, uploads only on a miss, submits the build,
polls the direct build resource client-side, rewrites the local task to
`artifact_id`, and only then sends launch, managed-job, pool, or Serve
submission. Workload commands expose no embedded-build no-wait mode. Only the
direct `sky image build --no-wait` command and SDK `image_build(wait=False)`
return a typed build resource without submitting a workload; the caller may
later select its READY output artifact. Every task-bearing server endpoint
rejects an unresolved `build` selector before request persistence with closed
`IMAGE_BUILD_REQUIRES_CLIENT_PREFLIGHT`; it never starts or waits for a build in
the generic executor. Resubmitting through the client after READY is the only
continuation path.

The builder requires API version 63. Version-62 clients and servers continue to
use external/canonical images but reject the unknown build field before request
persistence.

## Component boundaries

```text
client context scanner
  -> cache resolve, then scoped upload session only on miss
  -> S3-compatible workspace context store
  -> PostgreSQL build record
  -> build controller Deployment
  -> isolated single-attempt BuildKit Job in a sandbox runtime
  -> attempt-scoped internal OCI staging registry and workspace cache
  -> trusted publisher with digest/platform/provenance verification
  -> paced canonical registry publication
  -> catalog artifact + READY canonical location
  -> optional ordinary READY-fast-path publication
  -> ordinary regional distribution
```

This entire builder package and controller are target-state additions; the
current branch contains only the distribution foundation and an older proposal
pointer. The client owns filesystem traversal and canonical manifest generation. The
API owns authorization, quotas, upload capabilities, and durable intent. The
object store owns encrypted context and log bytes. The authenticated internal
OCI service owns staging/cache blobs and may use S3 or R2 underneath.
`build_state.py` owns build metadata and leases. The build controller owns
scheduling; the trusted publisher owns verification and canonical credentials.
An ephemeral BuildKit Job owns untrusted build execution and has only
attempt/workspace-scoped internal-registry authority. The catalog owns the
final artifact, ordinary publication owns any later immutable release, and the
distribution worker owns later regional copies.

No build executes in the API process, generic request executor, Serve or jobs
controller, distribution worker, cluster head, or accelerator-bearing workload
replica. Builder code does not enter `sky/container_images/state.py` beyond the
small catalog transaction interface.

## Context custody and object storage

PostgreSQL stores no context bytes. An administrator configures one
S3-compatible store, which may be Amazon S3, Cloudflare R2, MinIO, or another
implementation that passes the capability probe:

```yaml
container_image_builder:
  context_store:
    endpoint: https://<account>.r2.cloudflarestorage.com
    bucket: skypilot-build-contexts
    credential_ref: skypilot-builder-context-store
    encryption: provider_managed
  staging_registry:
    endpoint: https://image-staging.internal.example
    token_issuer: skypilot-image-registry-auth
    storage_backend: r2
    storage_credential_ref: skypilot-builder-staging-store
    require_distribution_conformance: true
  max_context_bytes: 2147483648
  max_context_entries: 100000
  max_manifest_bytes: 67108864
  max_attempt_output_bytes: 21474836480
  max_attempt_cache_write_bytes: 10737418240
  abandoned_upload_ttl_hours: 24
  terminal_context_retention_days: 30
  failed_log_retention_days: 30
  ready_log_retention_days: 90
  executor:
    kind: kubernetes_sandbox
    runtime_class: kata-builds
    namespace: skypilot-image-build
    node_selector:
      skypilot.co/node-pool: image-builder
    require_process_sandbox: true
    require_dedicated_nodes: true

workspaces:
  default:
    container_image_builder:
      max_context_bytes: 10737418240
      max_pending_uploads: 100
      max_pending_upload_bytes: 21474836480
      max_pending_builds: 1000
      max_active_executions: 20
      max_build_attempts_per_day: 100
      max_staging_bytes: 214748364800
      max_cache_bytes: 107374182400
      max_cache_records_per_platform: 10000
      max_retained_staging_attempts: 3000
```

The staging service is a pinned, upstream OCI Distribution implementation plus
a small SkyPilot token issuer, not a custom image format. Helm owns the service,
TLS, auth, NetworkPolicy, storage driver, health probes, and bounded replicas.
Its long-lived S3/R2 credential is mounted only into that trusted service. Build
controllers request short-lived repository/action tokens whose claims bind
workspace, attempt or cache generation, allowed pull/push verbs, byte/request
ceilings, and expiry. Multiple builder clusters can use the same endpoint; each
build still runs in exactly one qualified executor and the central PostgreSQL
attempt decides ownership. Provider-specific code remains confined to the
context-store provisioner and trusted final-registry publisher.

The post-gate Terraform modules create S3 buckets, ECR realm/IAM, and an AWS
builder node pool when selected. The Cloudflare module creates the R2 bucket and
scoped API token when account credentials permit; otherwise it validates
explicit pre-provisioned IDs. Helm consumes only secret references and module
outputs. Neither Terraform nor placement creates per-build cloud resources.

`credential_ref` is an administrator-owned server secret reference, never a
user-provided access key. The API issues a workspace- and upload-scoped
capability with short expiry and presigned object URLs. Upload bytes travel
directly between client and object store. The API and request database never
proxy or retain them. Build Jobs receive short-lived read URLs for exactly one
committed manifest and bundle. A bounded logging agent may request only the
finite, actual-length create-only segment sequence described below.

Global values bound one upload, build output, cache write, or retained object;
workspace values separately bound pending-upload count/bytes, committed context
custody, queued builds, active execution, daily paid attempts, staging, and
cache use. Quota counters are PostgreSQL rows locked with upload/build intent.
Manifest bytes are reserved only after a cache miss; bundle bytes are reserved
only after manifest validation and local bundle construction. Object lengths
are rechecked before COMMITTED. A user
retry cannot bypass the daily attempt budget, which is consumed only when its
next Job is admitted; an admin may repair or explicitly override a stuck
workspace, and that override is audited.

The complete entry manifest is sensitive context custody, not PostgreSQL
metadata. After the resolve miss, the API creates an upload intent containing
only bounded manifest length, claimed SHA-256 identity digest, MD5 transport
checksum, entry/byte counts, and logical `root_manifest_digest`. It reserves
that length and creates one random manifest object key. It
returns an initial short-lived single-PUT manifest URL whose signed headers require
the exact content length, `If-None-Match: *`, and the strongest checksum passed
by the store probe: SHA-256 when supported, otherwise `Content-MD5`. MD5 is
never an identity or cache key. If that response or URL is lost, the
authenticated uploader may request a replacement capability for the same upload
intent. The transaction verifies workspace/uploader ownership,
`MANIFEST_CREATED`, expiry, object absence, and the immutable
key/length/SHA-256/MD5 claims, increments a bounded issuance counter, and signs a
new expiry. A UUID idempotency key and request hash are retained; repeating the
same pair re-signs equivalent authority without consuming another issuance,
while key reuse with different claims fails. It cannot change bytes, create a second key, or revive a terminal
upload. Existing URLs may overlap only for their short remaining lifetime and
all authorize the same create-only object.
After upload, an isolated platform-owned context-validator Job on the dedicated
sandbox builder pool streams the
pinned manifest object, verifies RFC 8785 bytes, schema, path and entry limits,
recomputes the root digest and exact USTAR bundle length, and records only its
versioned object reference, lengths, digests, count, and `VALIDATED` state on
the upload row through a bounded signed result object consumed by the
controller. It has no database, Kubernetes API, registry, cache, bundle, or
unrelated object-store authority, only the exact manifest GET and result PUT.
No manifest path or entry content enters a database row. The
immutable `container_image_context_manifests` projection is created only after
the bundle also commits. Bundle part URLs are unavailable before manifest
validation and a cache-miss build submission both succeed.

Manifest and bundle objects use the same workspace prefix, encryption,
version/generation pinning, retention, legal purge, audit, byte quota, and
fenced deletion transaction. A context is COMMITTED only when both objects are
pinned; deletion releases quota only after both exact objects and any multipart
parts are absent. There is no raw manifest or bundle download endpoint.

V1 uses one canonical, uncompressed strict-USTAR bundle per context, not pax,
GNU tar, or a multi-object blob graph. Its format identifier is
`skypilot.context-ustar.v1`. The client normalizes an explicit include set into
a bounded manifest. Each entry records a validated relative UTF-8 NFC path,
directory, regular-file, or symlink type, canonical mode, size, content digest,
and safe symlink target where applicable. Directory traversal, absolute paths,
links escaping the root, devices, sockets, recursive aliases, case-colliding
paths, duplicate normalized paths, oversized manifests, and an implicit whole
working directory are rejected. Modification and access times, UID, and GID
are excluded from input identity because bundle encoding assigns the canonical
values above. Executable bits and safe symlink identity are included. Context
paths and contents, setup text, environment values, and logs are all
potentially sensitive workspace data even though v1 has no secret delivery
mechanism.

The archive byte profile is exact:

- Entries are sorted by complete normalized path bytes, with synthesized parent
  directories therefore appearing before descendants. There is no root entry.
- A path contains no NUL, empty, `.`, or `..` component. Its UTF-8 bytes are
  split at the rightmost slash that yields a nonempty name of at most 99 bytes
  and a prefix of at most 154 bytes; a path with no valid split is rejected.
  Symlink targets must be safe relative normalized paths of at most 99 UTF-8
  bytes. Reserving one byte in each field guarantees NUL termination.
- Every header is exactly 512 bytes using the POSIX USTAR offsets. Name,
  linkname, and prefix contain their bytes followed by NUL padding. Mode, UID,
  GID, and device fields are seven ASCII octal digits plus NUL; size and mtime
  are eleven ASCII octal digits plus NUL. UID, GID, mtime, device major, and
  device minor are zero. Magic is `ustar\0`, version is `00`, uname and gname
  are all NUL, and the final 12 header bytes are NUL. Type flags are `0`, `5`,
  and `2` for regular file, directory, and symlink respectively.
- The checksum is the unsigned sum of all header bytes with its eight-byte
  field treated as spaces, encoded as six ASCII octal digits, NUL, and space.
  Directories and symlinks have size zero. Each regular file is followed by its
  exact bytes and the minimum NUL padding to a 512-byte boundary.
- Exactly two all-zero 512-byte records terminate the archive. No record,
  alignment padding, global header, sparse record, xattr, ACL, device record,
  pax key, GNU extension, compression wrapper, or trailing byte follows them.

The independently encoded entry manifest is RFC 8785 canonical JSON containing
`skypilot.context-tree.v1`, ordered typed entries, canonical modes, file sizes
and SHA-256 digests, and symlink targets. Its SHA-256 is the logical
`root_manifest_digest` and is the only context-content digest in the build spec.
The archive's separate `skypilot.context-ustar.v1` format and
`bundle_digest` protect one transport object but do not define build identity.
An encoder fix or later accepted transport can therefore produce different
bundle bytes for the same verified logical tree without invalidating the build
cache; a semantic tree change bumps the tree version and root digest.
Cross-language transport and tree golden vectors include empty, nested,
executable, symlink, maximum-field, Unicode-normalization, and every rejection
boundary.

After manifest validation on the cache-miss `CONTEXT_REQUIRED` build, the
client submits exact bundle length/digests and per-part claims. The server
atomically reserves that additional byte custody, chooses one fixed part size
and finite part count, and creates exactly one persisted multipart upload ID
for the row's random key. Parts are numbered 1
through that count, never an arbitrary 1 through 10,000 set; the count itself
cannot exceed 10,000 and the server-selected size is 8 MiB through 128 MiB. All
nonfinal parts have that exact size and the final size is derived exactly. One
URL request names at most 100 not-yet-committed part numbers and supplies each
part's client-computed SHA-256 and MD5. The returned presigned URL signs exact
`Content-Length`, the probe-selected SHA-256 or `Content-MD5` transport
checksum, upload ID, part number, key, and expiry; chunked or unsigned payloads
are rejected by the store capability probe. Reissuing a URL for one part
requires the same length and both checksum claims. No second multipart ID can
exist for the session. Multipart parts may be replaced only within that one
persisted upload ID before commit; the bundle key is therefore not described as
create-only during upload. The upload role has no ordinary `PutObject` or
second-create permission for that key. After completion the upload ID closes,
and exact version/generation or stable ETag identity plus denied overwrite makes
the finalized random object immutable. Manifest bytes are charged from miss
intent, and bundle bytes from multipart creation, until committed retention or
confirmed abort/deletion, so uploaded parts can never be uncharged custody.

Manifest validation and bundle commit form one crash-convergent state machine:

```text
MANIFEST_CREATED -> MANIFEST_VALIDATING -> VALIDATED
VALIDATED -> UPLOADING -> COMMITTING -> COMMITTED
MANIFEST_CREATED|MANIFEST_VALIDATING|VALIDATED|UPLOADING|COMMITTING
  -> REJECTED -> DELETING -> DELETED
MANIFEST_CREATED|VALIDATED|UPLOADING -> ABORTING -> DELETING -> DELETED
```

Manifest commit has its own required UUID idempotency key and request hash. The
first matching request moves MANIFEST_CREATED to MANIFEST_VALIDATING under a fenced
validator lease. Repeating the same key and claims returns the current upload;
reusing the key or session with different claims fails closed. The validator
records VALIDATED only after the pinned create-only object passes every check.
A reclaimed validator HEADs and revalidates the same pinned version or
conditional-create ETag identity, so response loss cannot authorize different
bytes.

The bundle commit transaction accepts a separate bounded UUID idempotency key
and typed completed-part ETags/checksums, lists the persisted upload's parts, proves their
aggregate size equals the already reserved authorized length, stores a hash of
the complete commit request, changes `UPLOADING` to `COMMITTING`, and acquires a
fenced commit owner, random token, and expiry. Repeating the same key and
request is idempotent; reusing the key or session with different bytes or parts
is rejected. A direct API call returns the durable resource and never holds a
request worker across object-store completion.

The commit reconciler first HEADs the unique random bundle key. If the completed
object is absent, it invokes `CompleteMultipartUpload` using only the persisted
part set. It then HEADs the exact key, captures the stable provider
version/generation returned for that object, and validates length and any
provider-supplied checksum before a matching-token transaction records bucket,
key, version/generation, size, and claimed digests as `COMMITTED`. On a crash or
lost provider response, a reclaimed lease repeats HEAD before Complete. The
unique random key, post-completion overwrite denial, and stable
version/generation make an exact
already-completed object the idempotent success result. A missing upload plus
absent object, wrong length, wrong provider checksum, or conflicting version
becomes `REJECTED`; an inconclusive provider result remains `COMMITTING` for
fenced retry.

`COMMITTING`, `REJECTED`, and ambiguous abort/delete outcomes retain the full
byte reservation because an object or multipart parts may exist. GC changes a
rejected object to `DELETING`, performs a token-fenced delete or multipart
abort, and releases quota only after version-specific deletion or exact
absence proof commits `DELETED`. Thus a crash cannot create uncharged retained
bytes, and a late response cannot commit a superseded session. A build can
reference only a COMMITTED context owned by its workspace and uploader
identity; an admin override is explicit and audited. Merely knowing a digest or
manifest ID never grants another workspace user access to context bytes or
permission to execute them.

COMMITTED does not trust a client checksum or an optional provider checksum as
build input. Before BuildKit starts, a platform-owned materializer init
container in the attempt's sandbox streams the two pinned objects, rechecks
their full lengths and SHA-256 digests, validates every USTAR header and entry
digest against the manifest, reconstructs the exact canonical tree on bounded
ephemeral storage, and rechecks the root manifest digest. It receives no base,
cache, staging, log, Kubernetes API, or database authority. After success its
object URLs expire, the tree volume is remounted read-only into BuildKit, and
only a bounded typed verification result returns to the controller. The
controller Deployment never downloads or materializes bulk context bytes. A
missing, overwritten, corrupt, trailing-data, or structurally different object
fails closed before untrusted setup execution. The object-store administrator
remains trusted to delete availability, but cannot make different bytes pass
under the same build spec.

Deduplication never crosses workspaces or uploader identities. For one uploader,
only a fully materializer-verified `(bundle_digest, root_manifest_digest)` may
replace a duplicate upload with an existing pinned object; the duplicate random
object is then fenced for GC. Until verification, every upload is separately
charged, so forged digests cannot create an existence side channel or quota
bypass. Where
the store supports object versions, reads always name the pinned version.
Otherwise the capability probe must prove conditional create, deny overwrite
through the upload role, and return a stable generation/ETag identity. Presigned
single-PUT capabilities sign `If-None-Match: *`; multipart capabilities expose
only the one persisted upload ID, which becomes unusable after completion or
abort. No API grants a general `PutObject` capability for either key. Expiry is
defense in depth, not an overwrite boundary. Object lock is enabled when
supported but is not the correctness boundary.

Transport uses TLS. The store must provide provider-managed encryption or a
configured KMS key and must deny public access. The activation probe executes,
not merely advertises, conditional single PUT, signed exact Content-Length,
SHA-256 or `Content-MD5`, multipart create/upload/list/complete/abort, HEAD,
range GET, exact delete, expiry, and overwrite rejection. It records the
selected checksum algorithm as store capability state and builder admission
fails closed if any required behavior drifts. R2's S3-compatible API is valid
for this context-and-log role with signed `Content-MD5` when that probe passes;
the materializer still verifies full SHA-256. A raw R2 bucket is neither a
BuildKit registry cache nor a runtime image registry. The separately deployed,
OCI-conformant staging registry may use R2 as its blob backend only after its
own conformance, consistency, auth, and failure-recovery qualification. The
post-worth-gate Terraform modules are
`infra/terraform/modules/aws-build-store`,
`infra/terraform/modules/aws-image-builder`, and
`infra/terraform/modules/cloudflare-r2-build-store`. These are target paths for
the product stage, not part of the pre-gate distribution release, and remain
separate because the providers and account boundaries differ.

Upload sessions expire after 24 hours by default. A GC claim rechecks that no
active build references the context, marks it DELETING with a fenced lease,
aborts multipart work and deletes the exact manifest and bundle objects, and
only then marks it DELETED. Ambiguous deletion of either object is retryable.
Terminal contexts use policy retention from build completion. Legal/admin
purge can shorten retention only after a build is terminal and its output
artifact lifecycle permits it. Byte quota is released only after confirmed
deletion. Every transition is workspace scoped and audited.

## Build specification and cache identity

The normalized specification hash includes:

- exact resolved base manifest digest and platform;
- normalized setup commands and execution shell;
- logical root-manifest digest and context-tree semantic version; transport
  bundle digest and USTAR version are deliberately excluded;
- target OCI platform;
- complete `builder_compatibility_hash`, including pinned BuildKit and frontend
  digests, direct-LLB/config versions, exporter settings, and attestation
  policy;
- destination, workdir, build user, shell argv, and declared non-secret
  environment;
- final-config preservation and descriptor-normalization versions; and
- effective network and package-source policy fingerprint.

The base is an ACTIVE READY catalog artifact. Release and digest-pinned-source
convenience is resolved through ordinary catalog preflight before this API is
called. The first builder version has no direct external-base or user-provided
credential path. Other private content must be pushed to an authorized profile
and externally adopted by exact digest before it can become a catalog base. A
later private-source feature belongs to the distribution credential broker;
task secrets, Dockerfile arguments, or inherited API credentials are never
reused implicitly.

A catalog base resolves before cache lookup to one exact, platform-compatible
READY location accessible from the build cluster. A hit revalidates the existing
output and acquires no new base reference. On a miss, the build-intent
transaction snapshots only artifact/digest/platform. After context commit it
re-resolves a compatible READY location, snapshots its revision/custody and
acquires a durable `consumer_type=build` location reference. Before scheduling,
the trusted publisher mirrors and verifies that exact base into a read-only
workspace namespace of the internal staging registry. It owns the catalog-base
pull authority; the sandbox receives only a short-lived internal-registry read
token.
Tombstone/purge, eviction, and policy rotation cannot invalidate the bytes while
the reference is live. The reference is released only when the build becomes
READY, FAILED, or CANCELLED and no verification continuation owns its lease.

The first slice has no user-secret schema, secret mount, SSH mount, private
package-manager credential field, or secret environment delivery mechanism.
That is an interface boundary, not a claim that user-controlled context, setup,
or nominally non-secret `env` cannot contain sensitive bytes. Build `env` is
persisted in the normalized spec and the schema and UI warn users not to put
credentials there; credential-shape checks are defense-in-depth warnings, not
proof or authorization. Context and logs use encrypted workspace-scoped object
keys, have no raw download endpoint, follow the bounded retention above, and
are never visible to the viewer role. Normalized input values and bounded logs
are visible only to workspace users and admins and every log read is audited.
List/detail projections shown to viewers omit setup, environment values,
context paths, object references, raw logs, and free-form command output.
Platform-owned source/canonical credentials terminate in the trusted publisher
and are never mounted into the sandbox. The BuildKit Job receives only
context-read and narrowly scoped internal-registry base-read, cache, and
attempt-staging authority; it never receives a cloud registry or canonical
output-realm credential.

A future secret feature is a separate security extension. Arbitrary user build
code must be considered trusted with any secret deliberately granted to it: it
can read, print, or exfiltrate that value. Workspace authorization, short-lived
mounts, egress policy, and log redaction can reduce exposure but can never
justify a claim that user code cannot reveal a secret.

## Durable state and retries

Builder custody tables live in PostgreSQL metadata owned by `build_state.py`,
while their bytes remain in the object store or internal OCI service. Migration
024 is designed and frozen only after the prototype gate. It is PostgreSQL-only,
uses literal DDL, and owns exactly:

```text
container_image_context_uploads
container_image_context_upload_parts
container_image_context_manifests
container_image_builder_workspace_quotas
container_image_builder_daily_usage
container_image_builds
container_image_build_attempts
container_image_build_outputs
container_image_build_log_segments
container_image_build_cache_records
```

It also adds `build_id` to canonical locations and replaces every affected
named 023 contract in one transaction: artifact producer one-of; canonical
origin one-of; location-state check with `BUILD_RESERVED`; lease-kind/state
check with `BUILD_OUTPUT`; facet state check; builder claim/expiry/output partial
indexes; and producer-aware lifecycle/purge predicates. SQLAlchemy enums,
response validation, repair routines, and schema-parity fixtures change in the
same builder merge train. A fresh database runs literal 022 to 023 to 024 and
must match metadata exactly. Old migrations never import live metadata. The
builder PR deliberately couples API-63 code and migration 024; activation of
the controller, executor, internal registry, and UI remains separately gated.

One `container_image_context_uploads` row contains only:

```text
id, build_id, workspace, uploader_id, state,
tree_version, root_manifest_digest, entry_count,
manifest_claimed_digest/length, manifest_bucket/key/version NULL,
manifest_capability_generation/issue_count/last_issued_at,
manifest_capability_idempotency_key/request_hash NULL,
bundle_format NULL, bundle_claimed_digest/length NULL,
bundle_bucket/key/version NULL, multipart_upload_id NULL,
part_size/count NULL, reserved_manifest_bytes, reserved_bundle_bytes,
manifest_validation_owner/token/expires_at NULL,
validator_job_name/network_policy_name NULL,
validator_job_manifest_hash/network_policy_hash/resource_bundle_hash NULL,
validator_observed_job_uid/network_policy_uid NULL,
validator_broker_capability_epoch NULL,
commit_idempotency_key/request_hash NULL,
commit_owner/token/expires_at NULL,
created_at, expires_at, updated_at, error_code NULL
```

The two random object keys are unique per row. Checks bind every state to the
required object identity, lease triple, and quota reservation. A separate
bounded part-evidence table has exactly `part_count` rows and stores only part
number, exact expected length, SHA-256 and MD5 claims, selected transport
checksum, returned ETag/checksum, and URL-issued expiry.
`container_image_context_manifests` is the immutable COMMITTED
projection referenced by builds; it repeats the two pinned object identities
and logical root digest but contains no entries. Logs, attempts, cache records,
and build outputs use their own bounded tables.

One `container_image_builder_workspace_quotas` row per workspace is the locked
authority for nonnegative signed-64-bit `pending_upload_count`,
`pending_upload_bytes`, `context_bytes`, `pending_build_count`,
`active_execution_count`, `staging_bytes`, and `cache_bytes`. A separate
`container_image_builder_daily_usage` row per `(workspace, UTC day)` stores the
monotonic paid-attempt count and is retained for a bounded audit window. Check
constraints reject negative values and overflow. Intent, terminal transition,
attempt admission, exact-size adjustment, cache eviction, staging deletion, and
context deletion lock the appropriate quota row before their resource row and
change the counter in the same transaction. Configuration supplies limits but
never replaces these durable counters. A bounded repair command recomputes one
workspace from authoritative rows and records drift.

The distribution design's 14-phase **Global PostgreSQL lock order** is the sole
lock authority for these tables; this document does not define a competing
builder order. In particular it places builder workspace quota and UTC-day
usage in phase 2, builds in phase 6, uploads/parts/context object custody/cache
records in phase 7, locations in phase 8, references in phase 9, and
attempt/output/staging/log rows in phase 10. Resolve miss, bundle commit,
scheduler admission, retry/reactivation, cancellation/cleanup, output
publication, and cross-origin tombstone/purge use the exact phase sequences
listed there. IDs are discovered without locks, every acquired row is
revalidated, and no Kubernetes, registry, broker, or object-store call occurs
inside the transaction. PostgreSQL crossed-path tests cover all adjacent and
full mixed sequences before migration 024 can be approved.

`container_image_builds` contains:

```text
id, workspace, spec_hash, spec_version, builder_version, platform,
static_policy_hash, compiled_task_hash, root_manifest_digest,
resolve_idempotency_key, resolve_request_hash,
base_image_id, base_digest, base_location_id NULL,
base_reference_id NULL, base_profile_revision NULL,
base_target_fingerprint NULL, base_policy_fingerprint NULL,
base_custody_id NULL, context_upload_id NULL, context_manifest_id NULL, state,
output_image_id NULL, canonical_location_id NULL,
lease_owner/token/expires_at NULL, total_attempt_count, retry_generation,
generation_attempt_count, next_retry_at NULL, error_code NULL,
cache_outcome NULL, created_by, created_at, updated_at

UNIQUE (workspace, spec_hash)
UNIQUE (context_upload_id) WHERE context_upload_id IS NOT NULL
```

Embedded builds require non-null `static_policy_hash` and
`compiled_task_hash`; direct image-build commands mark their closed origin and
leave only `compiled_task_hash` null. Every build requires the logical root
digest and final spec hash before cache lookup or row creation. Named checks
enforce those origin shapes.

Only `context_upload_id` is set while a build is `CONTEXT_REQUIRED`; the
bundle commit transaction atomically replaces it with `context_manifest_id`
when moving to `PENDING`. Only `context_manifest_id` is set from `PENDING`
onward. A READY cache hit returns the existing row and creates no new build.
Context upload sessions/manifests, output reservations, attempts, and bounded
log segments use separate builder tables. They are not appended to the
already large distribution state module. Build state is
`CONTEXT_REQUIRED -> PENDING -> PROVISIONING -> BUILDING -> VERIFYING -> PUBLISHING ->
READY`, with fenced transition to FAILED, CANCEL_REQUESTED, or CANCELLED, and
`READY -> OUTPUT_RETIRED` when catalog lifecycle makes the output nonadoptable.
CONTEXT_REQUIRED cancellation aborts the bound upload, and a lease-free PENDING build may
cancel directly. BUILDING, VERIFYING, or PUBLISHING first enters
`CANCEL_REQUESTED`; it becomes CANCELLED only after its Job, registry outcome,
and output lease settle. Workers claim bounded batches with expiring leases and
heartbeat tokens. Reclaimed VERIFYING or PUBLISHING work always re-reads the
exact registry digest instead of assuming the previous process completed.
Pending-build count is reserved at context commit and released when scheduler
admission or a terminal transition removes that queue entry. Active-execution
count and the daily paid attempt are reserved exactly once when an attempt
enters PROVISIONING and released exactly once after its exact Job/output work is
settled. A retry returning to PENDING re-enters only the pending count. Lease
recovery and same-row retry are generation fenced, so neither double releases
nor bypasses either bound.

### PostgreSQL to Kubernetes Job protocol

`container_image_build_attempts` is the cross-system intent, not merely history:

```text
build_id, retry_generation, attempt_number,
state PROVISIONING|RUNNING|JOB_SUCCEEDED|VERIFYING|PUBLISHING|SETTLING|TERMINAL,
job_name, network_policy_name, desired_job_manifest_hash,
desired_network_policy_hash,
desired_resource_bundle_hash, observed_network_policy_uid NULL,
observed_job_uid NULL,
create_owner/token/expires_at NULL, execution_owner/token/expires_at NULL,
cancel_requested_at NULL, staging_reference, broker_capability_epoch,
reserved_active_execution, reserved_staging_bytes, reserved_cache_bytes,
started_at NULL, finished_at NULL, terminal_code NULL

UNIQUE (build_id, retry_generation, attempt_number)
UNIQUE (job_name)
UNIQUE (network_policy_name)
```

Scheduler admission commits the attempt in PROVISIONING, all quota
reservations, deterministic DNS-safe Job and NetworkPolicy names derived from
the attempt key, hashes of both canonical manifests, their complete resource
bundle hash, and capability epoch before Kubernetes I/O. Helm has already
installed a namespace-wide default-deny policy, a broker-only egress path, and
one builder ServiceAccount with no Kubernetes RBAC. The ServiceAccount disables
automatic token mounting. The Job explicitly projects only a short-lived,
automatically refreshed service-account token for audience
`skypilot-image-builder-broker`; Kubernetes binds it to that Pod UID. No
per-attempt Secret or ConfigMap exists, and no presigned URL or registry token is
embedded in a reconstructible Job manifest.

A creator claims a short lease, reconstructs both manifests, rechecks
cancellation, and creates or adopts the attempt NetworkPolicy before the Job.
The policy selects only the unique attempt labels and permits only deployment-
qualified broker, internal-registry, and context/log egress gateways. Success or
`AlreadyExists` is followed by GET for each object. The controller adopts an
object only when SkyPilot ownership labels, attempt key, resource-bundle hash,
its individual manifest hash, and immutable spec match, and records both
observed UIDs under the same token. A foreign or mismatched same-name object is
never adopted or deleted and closes the attempt with
`NETWORK_POLICY_NAME_COLLISION` or `JOB_NAME_COLLISION`.

The trusted capability broker validates the projected token with TokenReview,
the bound Pod UID, Job ownership, recorded attempt/generation, resource-bundle hash,
active state, and capability epoch on every mint. It can then reissue short-lived
context-read URLs, one attempt-scoped internal-registry token, bounded log
segment PUT capabilities, and a typed result PUT capability. The broker owns no
catalog write path and the sandbox receives no cloud-registry credential. Token
expiry therefore does not make a queued or long-running Job unreconstructible;
a live matching Pod refreshes through the broker, while cancellation or
settlement increments/revokes the epoch and prevents further minting. Already
issued capabilities retain only their short bounded lifetime and cannot make an
output READY without the independent verifier and fenced publisher.

A crash after database commit but before either Create is a normal reclaimed
Create. A lost NetworkPolicy or Job Create response converges through
deterministic GET/UID adoption.
Controller takeover requires an expired create/execution lease and repeats the
same rules. Cancellation before Create moves directly to SETTLING. Cancellation
racing Create records CANCEL_REQUESTED, revokes the capability epoch, GETs the
matching Job UID, and deletes only that UID with a precondition; a replacement
object with the same name is never touched. After the Job is terminal or proven
absent, cleanup deletes only the matching NetworkPolicy UID and hash. Lost
delete responses are reconciled by GET. Foreign or replaced policies are left
untouched and surfaced for an administrator. Job deletion, eviction, deadline,
or node loss is observed by UID and
becomes a bounded retry or terminal failure. A stale controller can neither
record a new UID nor settle quotas.

Completion records the exact internal-registry digest before moving to
VERIFYING. Active-execution and worst-case byte reservations are released or
reduced exactly once only after the observed Job UID is terminal/absent and all
staging/cache outcomes have reached a fenced retained-or-absent state. The
attempt row is retained for audit. A sweeper reconciles every nonterminal DB
intent plus labeled orphan, but may adopt or delete only an exact recorded
resource bundle. Context-validator Jobs use the same default deny, projected
identity, broker, deterministic NetworkPolicy/Job name/hash/UID protocol with
fields on the upload row, but receive only manifest GET/result PUT and never
reserve paid build execution. Fault tests cover every database, policy Create,
Job Create, GET/UID, broker refresh, cancellation, node-loss, cleanup, and
settlement crash boundary.

Automatic failures use bounded exponential backoff within one generation. A
user/admin retry keeps the same build ID and immutable spec and never creates a
second row to evade uniqueness or lose history. Total attempts remain
monotonic. Cancellation ends the current generation; an authorized
resubmission of the immutable spec starts a new generation on the same row. A
terminal row does not return to PENDING until the reactivation transaction
below succeeds.

Reactivation after FAILED or CANCELLED is a fresh custody transaction on the
same build row. It first proves the same context digest is still COMMITTED. If
retention removed that context, retry may supply a caller-owned VALIDATED upload
with the same tree version, root digest, and spec-bound metadata; it re-enters
`CONTEXT_REQUIRED` and follows the ordinary miss-only manifest/bundle path. It
never accepts a different logical tree. It then resolves a current
platform-compatible READY catalog location for the same base digest, acquires a
new `consumer_type=build` reference, and replaces every base snapshot field
together. A tombstoned base or lack of a READY route fails closed. Only after
context/base custody and the pending-build slot are committed does it increment
`retry_generation`, reset the per-generation attempt budget, clear terminal
scheduling fields, and return the row to PENDING. Active execution, paid
attempt, staging, and internal-registry authority remain scheduler-admission
work.
The base digest remains in the immutable spec hash, so changing to
another qualifying physical READY location does not change build identity.

Every BuildKit attempt pushes to a create-only internal staging reference containing
workspace, build ID, retry generation, attempt number, and a hash of the random
lease token. It can never share or overwrite another attempt's tag. The token
given to BuildKit is scoped by the internal registry auth service to that
one attempt target plus its workspace/platform cache. A stale attempt has no tag
used by a later generation and no cloud registry authority. Attempt admission
reserves active execution, the daily attempt, configured maximum staging-output
bytes, and maximum cache-write bytes before the Job receives that token. The
internal registry enforces token scope, request/byte ceilings, and immutable
attempt tags. ECR calls occur only later in the trusted publisher and acquire
the distribution limiter's per-call leases. The post-export verifier determines
the exact root digest, platform evidence, and
logical compressed bytes before any canonical authority is requested. It
atomically reduces worst-case staging and cache reservations to inspected
retained bytes; an over-limit or ambiguous output remains charged at the
worst-case reservation until fenced absence is proved. Export is terminated and
the attempt fails closed if the hard per-attempt output or cache-write bound is
exceeded.

Migration 024 adds `container_image_build_outputs`, one immutable record per
attempt that reaches canonical reservation:

```text
build_id, retry_generation, attempt_number, attempt_token_hash,
image_id, canonical_location_id, digest, platform_set, reserved_bytes,
state RESERVED|COPYING|VERIFYING|READY|CANCEL_REQUESTED|FAILED,
lease_owner/token/expires_at, error_code NULL, created_at, updated_at

UNIQUE (build_id, retry_generation, attempt_number)
```

The table is the build-publication lease and catalog-to-builder lifecycle
bridge. Before canonical repository provisioning, credentials, or copy I/O, a
single transaction follows the catalog's global phases: workspace quota,
profile head/revision/custody, workspace/digest advisory key, artifact, build,
canonical location, attempt/output. It then:

1. revalidates generation, attempt, and attempt token and rejects a digest whose
   artifact is TOMBSTONED, PURGING, or PURGED permanently;
2. for a new digest, reserves artifact, location, and exact logical byte quota,
   creates the ACTIVE `managed_build` artifact, creates its canonical
   `origin_kind=BUILD` location in `BUILD_RESERVED`, and inserts the RESERVED
   output lease with the generation, attempt, and token hash;
3. for a prior provisional output from this same build, reacquires it only
   after its lease expires and every digest, profile, artifact, and location
   binding matches;
4. for an ACTIVE artifact with an exact READY canonical route under the
   requested profile, records a content-convergence candidate without changing
   its original producer or SOURCE/BUILD origin; or
5. rejects an ACTIVE artifact owned by another unresolved origin with closed
   `OUTPUT_DIGEST_REGISTERED_UNREADY`, leaving staging available for bounded
   retry and performing no canonical I/O.

Artifact and location counters are not double charged on the convergence path.
`reserved_bytes` is the verified manifest's logical compressed size, the same
counter definition used by distribution quota, rather than an estimate of
provider layer deduplication. Failure to reserve it ends the attempt before
canonical paid I/O. No source row or release is created.

Only the current RESERVED output lease can obtain short-lived authority for its
exact digest-sharded canonical repository and immutable tag. The trusted publisher
sets the output and location to COPYING, copies the exact staging digest, then
sets VERIFYING and independently verifies the destination. A reclaimed lease
always inspects the immutable destination before copying, so a crash after push
converges without rewriting an immutable tag. The READY transaction takes the
same locks, rechecks ACTIVE lifecycle, exact profile, quota reservation,
generation, attempt, both lease tokens, digest, platform/config evidence, and
origin binding, then atomically marks location, output, and build READY and
stores `output_image_id` and `canonical_location_id`. A convergence candidate
first reacquires and revalidates the existing READY location, then commits the
build with `cache_outcome=CONTENT_CONVERGED`; it does not claim that the
canonical location was produced by this build. Regional copies and runtime
validation follow the recorded exact canonical origin. Release publication is
a separate post-READY artifact publication.

A stale publisher may finish staging inspection or even canonical I/O after losing its
token, but it cannot adopt the result. The provisional artifact, location,
reserved bytes, and output record already exist, so there is no rowless
canonical orphan. Ambiguous or failed canonical outcomes remain charged and
are inspected under the output lease. After the lease settles, a FAILED or
CANCELLED build output with no release, publication, source alias, durable
reference, or other READY build is eligible after retention for the catalog's
narrow system tombstone reason `BUILD_OUTPUT_ABANDONED`; deletion then uses the
ordinary artifact-scoped, ownership-fenced purge states and releases quota only
after confirmed absence. There is no independent canonical-output GC or direct
registry deleter in builder code. Tombstone and purge reject every live output
lease. If cancellation races final READY, the first terminal transaction wins;
a committed READY build is not retroactively cancelled.

Staging references are not catalog artifacts. They are recorded by
generation/attempt and removed after a grace period by the builder's fenced
staging GC; ambiguous deletion remains charged against the pre-reserved
staging-output budget and retryable.

## Execution isolation and caching

After the prototype gate, Helm installs build-controller and trusted-publisher
Deployments separately from the distribution copy worker. Untrusted Jobs never
run on API-server, publisher, or ordinary control-plane nodes.
`BuilderExecutor` is an explicit capability boundary. The first adapter
targets Linux/amd64 Kubernetes only when an administrator has supplied a
dedicated tainted builder node pool and a sandbox RuntimeClass whose pod gets an
independent microVM or equivalent hardware-backed kernel boundary. A standard
container RuntimeClass, rootless UID mapping, namespace, or node label alone is
not tenant isolation.

Helm creates one dedicated builder namespace at activation; build admission
never creates Kubernetes namespaces. It also creates the permanent default-deny
policy, no-RBAC ServiceAccount, identity/capability broker, and qualified egress
gateways. Each claim creates one single-attempt NetworkPolicy followed by its
BuildKit Job in that namespace under the crash protocol above. The sandbox gives
each pod its own guest, so the daemon and setup process share no guest with
another workspace or attempt. The Job has no host Docker socket, host path, host PID/IPC/network,
device pass-through, service-account API token, API database route, or unrelated
workspace object authority. It has a read-only outer root filesystem where the
runtime supports it, a default-deny seccomp/AppArmor profile, dropped outer
capabilities, CPU/memory/ephemeral-storage limits, bounded deadline, and
default-deny egress opened only to its exact context, internal registry, log,
and result endpoints.
Only bounded ephemeral volumes for BuildKit state, scratch space, the
materialized read-only tree handoff, and typed result output are writable. The
setup process receives no intentionally mounted presigned URL, registry token,
BuildKit control socket, or Kubernetes token. The explicit audience-scoped
projected token is mounted only into the trusted capability agent. It grants no
Kubernetes RBAC and the agent exchanges it through the broker for attempt-
scoped capabilities; the Job manifest itself remains free of expiring secrets.

Kubernetes NetworkPolicy is pod-scoped, not container-scoped. The complete
sandbox pod is therefore one network security principal. Process isolation
prevents ordinary setup code from reading BuildKit/session state, but the threat
model does not claim that a compromised guest kernel cannot steal the projected
broker identity, attempt-scoped internal-registry token, or object capabilities.
The broker identity can mint only while this exact recorded Pod UID, attempt,
state, and capability epoch remain current. Such a registry token can
write only this attempt's untrusted staging tag and this workspace/platform
cache within hard byte/request/expiry bounds. It cannot read another workspace,
obtain cloud registry credentials, or make output READY without independent
verification. This same-workspace residual risk is explicitly accepted in v1;
a future per-request credential proxy can narrow it further.

BuildKit process isolation must remain enabled. The adapter rejects
`--oci-worker-no-process-sandbox`, host-user-namespace fallback, unconfined
seccomp, privileged host containers, or silently substituting the default
RuntimeClass. Rootless BuildKit is preferred only where its full process
sandbox passes; "rootless" is never accepted as the isolation proof. If the
pinned daemon needs an unsafe flag on the configured runtime, the capability
probe fails and builder admission stays disabled. The prototype must prove the
exact pinned BuildKit/Kata combination before migration 024 is approved. A
dedicated disposable VM per attempt is an alternative only after it qualifies
under the same contract; the product never silently weakens to a normal pod.

Activation runs a hostile conformance suite in the actual runtime: namespace
and mount escape, ptrace and process signaling, `/proc` inspection, socket and
service-account discovery, cross-job network/object/cache access, fork and disk
exhaustion, timeout/cancellation, and node loss. The admission webhook also
rejects a generated Job whose RuntimeClass, node affinity, taint tolerance,
security context, network policy, or credential projection differs from the
qualified template. One successful friendly build is not security evidence.

V1 uses one qualified internal OCI registry for attempt staging and BuildKit
registry cache, with physically separate namespaces and policies. It is
distinct from final runtime artifacts and may use S3 or R2 blob storage behind
the OCI service. Cache is namespaced by workspace, platform, and builder
compatibility generation. Each export uses a generation/attempt-scoped
create-only tag. After registry HEAD/manifest verification and a current-token
check, PostgreSQL records its digest, bytes, and compatibility metadata; future
builds import a bounded newest-first set of verified digest references, never a
mutable `latest` tag. Stale exports are invisible and enter fenced GC.

Activation requires internal-registry conformance, token-scope, immutability,
byte/request limit, encryption, availability, and exact-delete probes. Runtime
record ceilings for retained staging attempts and cache records must fit the
registry's measured capacity and workspace byte quotas. It is not included in
ECR repository quota because no staging/cache repository is created in ECR.
Cache import failure is a recorded performance miss. Cache export failure is
nonfatal only under the explicit `ignore-error=true` cache exporter and creates
no record; final staging export failure always fails the attempt. Cache entries
record last use and bytes. Fenced GC deletes only an owned exact digest/tag after
proving no active-build reference; ambiguous deletion stays charged. Failed
builds retain no unbounded local volumes, and Kubernetes Jobs/ephemeral storage
are removed after bounded diagnostic collection.

A READY spec cache hit is valid only while its output artifact is ACTIVE and
its recorded exact canonical location is READY under current policy. A normal
BUILD-origin output additionally requires that origin to point to this READY
build; a `CONTENT_CONVERGED` output instead requires the unchanged recorded
source or other-build origin and exact digest. The cache lookup first reads
candidate IDs without trusting them, then in one transaction locks artifact,
build, canonical location, and output record in the shared order and revalidates
every condition. Tombstone, PURGING, PURGED, missing canonical content, or
origin mismatch changes the row to `OUTPUT_RETIRED` in that transaction; it is
never returned or rebuilt automatically.

Catalog tombstone does not duplicate builder SQL or import the builder module.
API-63 builder startup registers a `CatalogLifecycleExtension` whose
session-taking callback delegates to
`build_state.retire_outputs_in_session(session, artifact_id)` after the catalog
locks the artifact and before commit, so artifact lifecycle and every READY
build transition to `OUTPUT_RETIRED` atomically. The extension is registered
even when the separate controller is disabled; a managed-build artifact fails
closed if it is unavailable. The defensive cache-hit revalidation above uses
the same state helper. Because a purged digest is
permanently nonresurrectable in the catalog, retrying the same row is denied. A
caller may submit a deliberately changed spec as new intent, but finalization
still fails closed if it reproduces a permanently purged digest. Restoring
retired content would require a separate audited catalog restore policy and is
outside v1.

The first production builder is CPU-only and emits `linux/amd64` because the
Boltz L4 fleet runs AMD64. SkyPilot does not build ARM64 speculatively. A later
ARM64 placement creates a distinct spec/build. Multi-platform index assembly
is a separate operation that requires all child builds READY and verifies the
resulting index. Regions and clouds never change a build key. A multi-GPU node
pulls one image and shares layers; process-per-GPU orchestration remains runtime
behavior.

## APIs, dashboard, RBAC, and audit

Every builder endpoint is a direct bounded synchronous FastAPI handler, with
blocking PostgreSQL or bounded object-metadata work run in the framework
threadpool. Build resolution performs bounded catalog-base and cache reads,
then either returns a verified READY hit or commits CONTEXT_REQUIRED intent for the
independent controller. Retry and cancellation only perform fenced transactions. None waits for
BuildKit, creates a generic request row, or needs a second request-status
resource:

```text
POST /images/builds/policy-preflights
POST /images/builds/resolve
GET  /images/builds/{build_id}/upload?workspace=W
POST /images/builds/{build_id}/upload/manifest/capability
POST /images/builds/{build_id}/upload/manifest/commit
POST /images/builds/{build_id}/upload/bundle
POST /images/builds/{build_id}/upload/parts
POST /images/builds/{build_id}/upload/commit
POST /images/builds/{build_id}/retry
POST /images/builds/{build_id}/cancel
GET  /images/builds/contexts?workspace=W&limit=50&cursor=C
GET  /images/builds?workspace=W&limit=50&cursor=C[&state=S]
GET  /images/builds/{build_id}?workspace=W
GET  /images/builds/{build_id}/logs?workspace=W&cursor=C&limit_bytes=32768
```

Resolve returns `200 ContainerImageBuild` for a verified cache hit or active
coalescing. On a miss it returns `201 ContainerImageBuild` in CONTEXT_REQUIRED,
including its upload ID, reserved manifest length, expiry, and an initial
short-lived manifest PUT capability.
Resolve carries a UUID idempotency key and request hash. A lost miss response is
recovered by repeating it: the same uploader receives the same build/upload ID
without a second quota reservation, then calls the capability route; a different
request under that key fails closed.
That bearer capability is never logged or persisted by the client library and
is not available from upload status. The uploader-only capability route accepts
a UUID idempotency key and can reissue only the row's identical create-only
intent under the bounded generation/count rules above; it is rate-limited and
never reveals a capability to another uploader or after upload expiry. Manifest commit
returns `202` with VALIDATING state; the upload projection reports VALIDATED or
a closed failure. Only then may bundle metadata reserve bytes and the parts
route return signed URLs for at most 100 predetermined part numbers. Bundle
commit accepts its UUID
idempotency key and typed completed-part evidence, returns `202` in COMMITTING,
and the same resource eventually exposes the pinned COMMITTED context. There is
no context, manifest, or blob download route. Cross-uploader and terminal
conflicts use the closed outcomes above. Retry accepts a replacement miss flow
only under the same-tree rules. Retry and cancel return the current
`200 ContainerImageBuild`. Those typed resources contain the build ID and
durable state immediately. Retry and cancel are idempotent, fenced state
transitions. A user may cancel their own active build, while an admin may cancel
any accessible active build. Terminal cancellation is a no-op only when the
requested terminal state already matches.

Build listing uses a value-validated opaque `(created_at, id)` keyset cursor,
defaults to 50, and caps at 200. Detail returns one typed build and no log body.
Logs are immutable create-only segments. The trusted log agent buffers at most
256 KiB, then asks the broker for the next sequence using the actual byte length
and SHA-256. The broker permits only sequences 0 through 63, a cumulative
16-MiB maximum, and the current attempt/capability epoch, and signs exact
`Content-Length`, checksum, key, and `If-None-Match: *`. Thus every full segment
may be 256 KiB and the last data segment may be any actual length from 1 through
256 KiB without a padded or unsigned write; an empty stream has zero data
segments. Segment evidence is HEAD-verified
before the next sequence becomes issuable.

Normal close writes one canonical-JSON FINAL marker containing attempt key,
contiguous segment count, total bytes, SHA-256 of the concatenated byte stream,
`truncated`, and a closed reason `PROCESS_EXIT|BYTE_LIMIT|CANCELLED`. FINAL is a
separate create-only key. The controller validates its counts and digest and
inserts `container_image_build_log_segments` rows containing attempt, sequence,
`DATA|FINAL` kind, pinned object identity, digest, size, and close metadata. A
missing sequence makes every later segment invisible and non-issuable until it
is reconciled.

Job termination does not depend on a surviving log agent. After a bounded grace
period, a trusted log reconciler HEADs only the 64 deterministic data keys,
chooses the longest contiguous prefix, deletes or quarantines any later orphan,
streams at most 16 MiB to recompute the exact digest, and conditionally creates
FINAL with `truncated=true` and `close_reason=LOGGER_LOST`. If a normal FINAL won
the create race, it validates and adopts that marker instead. A conflicting
marker closes with `LOG_FINAL_MISMATCH` and never exposes noncontiguous bytes.
The reconciler is generation/attempt fenced, so a dead or cancelled logger
always converges to one terminal stream and a stale logger cannot reopen it.

The read cursor opaquely binds build/attempt, segment sequence, pinned
generation, and byte offset. Responses cap at 64 KiB and each attempt at 16 MiB,
return `next_cursor`, `truncated`, and terminal close reason, and never accept a
raw object key or offset. An active stream with a missing next segment returns
its current contiguous prefix plus `terminal=false`; a terminal stream is
defined only by the verified FINAL row. Active detail polls at
five seconds while visible and backs off after errors; terminal detail does not
poll. The API never creates request rows for polling.

Browsers do not reliably preserve POSIX modes, executable bits, symlink targets,
or the two-gigabyte canonical traversal contract. V1 therefore never scans a
browser-selected directory. The CLI exposes `sky image build context prepare`
to scan/upload a canonical context and return its opaque prepared-context ID.
The Images page Build dialog selects one caller-owned prepared context and shows
only digest, entry count, bytes, expiry, and CLI refresh instructions. It also
provides base selector, amd64 platform, destination, workdir, build user, shell,
environment warning, setup editor, distribution, quota estimate, build
progress, and cancel. It preserves failed input for correction. Explicit UI
states cover builder disabled, executor unqualified, context/staging store
unavailable, capability-probe failure, pending-upload quota, scheduler quota,
and publisher backpressure. Build rows and artifact detail
show state, cache outcome, attempts, duration, redacted base identity, platform,
context digest, output artifact, and closed error. Workspace users/admins may
open bounded logs and retry; viewers receive metadata-only projections with no
logs or normalized input values. A READY build offers the ordinary Publish
dialog rather than embedding release naming in build.

The exact viewer allowlist is only `GET /images/builds` and
`GET /images/builds/:build_id`. Upload status, parts, commit, logs, and every
mutation remain default-denied to viewers. Users need workspace write access for
upload/build/retry, may cancel only their own active build, and need workspace
read access for sensitive build detail/logs. Admins may operate any accessible
workspace and change builder policy. Every route independently resolves the
workspace before database or object-store access; cross-workspace IDs return a
closed not-found response. Route-coverage tests enumerate all methods and all
three roles.

Events for upload commit/expiry, build submission/claim/retry/failure,
log access, cache deletion, output publication, and admin purge include actor,
workspace, resource ID, and closed outcome without source contents.

## Failure and lifecycle semantics

- API/worker restart cannot lose a committed upload or build intent.
- A stale lease token cannot heartbeat, publish, fail, cancel, or delete.
- Digest or platform mismatch never creates a READY artifact or release;
  already reserved provisional metadata remains nonlaunchable and follows the
  ordinary charged lifecycle cleanup path.
- Catalog-base staging/auth failure is closed and value-free; there is no
  direct external-base credential path.
- Context or log store unavailability leaves durable retryable state and never
  falls back to PostgreSQL blobs or API-server disk.
- Internal registry-cache import failure triggers a cache miss, cache export
  failure records no cache, and final staging or canonical-registry failure
  fails the build.
- Log redaction is defense in depth. Workspace-scoped encryption, authorization,
  bounded retention, platform-credential isolation, and process isolation are
  the primary controls; user-controlled inputs and output may be sensitive.
- A build output follows the catalog's tombstone/purge lifecycle. Purging an
  output never silently deletes shared cache or source context before their own
  references and retention expire.

## Rollout and test gates

Delivery has a pre-product decision point:

1. ship and qualify the distribution catalog, external exact-digest adoption,
   AWS copy path, Terraform, and read/action Images UI without builder schema or
   syntax;
2. implement a non-public builder prototype using the pinned frontend/BuildKit,
   exact catalog base, sandbox, internal OCI registry, trusted publisher, and
   context encoder, plus the default-deny namespace, projected pod identity,
   refreshable capability broker, and deterministic NetworkPolicy/Job bundle,
   but no migration 024, durable public API, product controller, or Build UI;
3. run the prototype on representative Boltz and one non-Python workload
   against external CI, including Docker/OCI base vectors and hostile sandbox
   conformance; and
4. pursue the product only if output verification and isolation pass, a warm
   nontrivial build is at least 50% faster p95 than external CI, a cold build is
   no more than 25% slower, and the internal-registry/publisher path needs no
   manual mutation across injected restart faults.

If the prototype fails, it is removed or retained only as an experimental
developer tool. Migration 024, API-63 build syntax, durable controllers,
builder Terraform, and dashboard Build UX are not created. External CI plus
exact-digest adoption remains the supported smaller solution.

After the prototype passes, product implementation order is literal migration
024 and context custody, deterministic Job/controller and trusted publisher,
catalog completion, CLI/task integration, complete dashboard UX, then optional
R2 modules. No public build syntax ships before every required worker, internal
registry, and object store is deployable. Product activation then measures the
same workloads across existing runtime setup on 1, 100, and 1,000 replicas,
external CI plus exact-digest adoption, and the managed builder with cold and
warm caches. It records submission-to-READY p50/p95, context bytes, build CPU
and storage cost, cache hit ratio, registry bytes, deployment image-ready time,
failures, retries, and operator steps. Production activation requires:

- a verified spec cache hit reaches READY within 15 seconds p95 without bundle
  upload or BuildKit execution;
- a warm nontrivial build is at least 50% faster p95 than the same external CI
  build, while a cold build is no more than 25% slower;
- prebuilding removes per-replica build setup and reduces 100-replica
  image-plus-setup readiness by at least 40% without regressing the
  already-prebuilt adoption baseline by more than 10%;
- publish and deploy require at least 75% fewer operator steps than external
  build, push, adopt, and prepare scripting;
- compute plus retained context/cache/log cost per successful build is no more
  than 25% above the external CI baseline at the same retention; and
- a 30-day canary has no isolation escape, cross-workspace access, lost intent,
  unbounded quota drift, or manual registry repair.

If these activation gates fail after product implementation, the surface stays
administrator-disabled and is not called production-ready; migration 024 still
remains a real compatibility obligation. No Modal-like latency or cost claim is
made from unit tests.

Tests cover canonical context hashing, every unsafe filesystem shape, YAML-file
and programmatic context-root resolution, and byte-exact strict-USTAR golden
vectors and rejection boundaries in two independent implementations. Upload
tests cover authorization, expiry, part and aggregate bounds, pre-completion
manifest-object custody and validation, exact signed per-part length/checksum,
SHA-256 selection on capable S3 plus signed `Content-MD5` selection on R2,
conditional-create overwrite rejection, URL reuse, unexpected/chunked payload
rejection, competing multipart-ID
rejection, manifest reservation before its capability and separate bundle
reservation before multipart creation, distinct manifest
and bundle idempotency-request conflicts, lost initial manifest-capability
response plus owner-only identical reissue, reissue-count/expiry/rate limits,
cross-uploader denial, create-only races between overlapping same-byte URLs,
every manifest-validation and
`UPLOADING -> COMMITTING -> COMMITTED` crash point, Complete response loss,
exact-version or conditional-create ETag HEAD recovery, rejected and ambiguous
object charging, and quota
release only after confirmed deletion. They also cover cross-workspace and
cross-uploader isolation, verified same-uploader deduplication, create-only
manifest writes, replacement only inside the one multipart ID, finalized bundle
overwrite denial, corruption, trailing data, and overwrite detection during
materialization.

Context-identity tests prove two resolve calls with transport-independent
logical trees produce the same spec hash and cache hit, while any path, type,
mode, content, symlink, or tree-version change does not. On a miss, v1 accepts
only the byte-exact strict-USTAR format and rejects a noncanonical bundle.
Cache-hit tests run before upload creation and prove no manifest URL, validator,
byte reservation, multipart ID, bundle construction, paid attempt, or Job
exists. Cache-miss tests prove the unique `CONTEXT_REQUIRED` binding, separate
pending-upload quotas, prohibit parts before manifest validation and bundle
reservation, atomically replace the upload reference with a committed context,
and consume active/daily attempt quota only when deterministic Job intent enters
PROVISIONING. They also cover same-spec concurrent READY and active coalescing,
cross-uploader CONTEXT_REQUIRED denial, charged cleanup of every unused upload,
pre-commit expiry/cancel slot release, and same-tree replacement context on an
explicit terminal retry.

Builder tests cover the pinned qualified daemon plus `gateway.v0` frontend,
direct LLB, every exporter and compatibility-hash field, ordered base
digest/size preservation plus the exact Docker-to-OCI normalization matrix,
foreign/URL/annotation rejection, fixed gzip for new layers, exact OCI
config/history/diff IDs, attestation rejection, capability probe, and cache
compatibility isolation. They cover catalog-only base preflight, absence of a
direct public/private-base or user-secret delivery schema,
sensitive input/log projection, viewer log denial, environment warnings,
platform-credential non-persistence, catalog-base READY route snapshot plus
durable build reference under eviction/tombstone/policy races, same-row retry
generations, and lease recovery.

Quota tests use real PostgreSQL concurrency to prove pending-upload count/bytes,
context bytes, pending builds, active execution, daily attempts, staging bytes,
and cache bytes cannot oversubscribe; every
terminal, expiry, exact-size adjustment, deletion, ambiguous provider outcome,
and same-row retry changes counters exactly once. Repair tests detect and fix a
single-workspace drift without scanning or locking unrelated workspaces.

Catalog integration tests cover fresh literal 022-to-023-to-024 migration,
metadata parity, disabled-builder rollback, activation fencing, the named
constraint replacement, pre-I/O artifact/location/byte reservation,
generation/attempt/token-scoped output and staging records, crashes and stale
attempts before and after canonical push, immutable-tag recovery, cancellation
races, no rowless canonical content, ordinary abandoned-output tombstone/purge,
DELETE_UNKNOWN charging, concurrent identical build convergence, collision
with SOURCE, other-BUILD, unready, tombstoned, and purged digests, output
digest/platform mismatch, SOURCE-versus-BUILD runtime validation, content
convergence without provenance rewriting, and ordinary artifact-ID
READY-fast-path publication. Cache tests exercise the shared session-taking
OUTPUT_RETIRED transition and defensive lock-order revalidation for every
artifact and location lifecycle state.

Interface and operations tests cover task client preflight, server rejection
and static policy-token binding before request persistence, exclusion of the
not-yet-known root digest from preflight, durable resolve-time content binding,
final exact build-to-artifact substitution, policy mutation/expiry and
identical-static-contract refresh,
mixed AMD64/ARM64/unknown/direct candidate rejection before local scan, no
embedded workload no-wait mode, direct typed
`image_build(wait=False)`, top-level setup preservation, service/job
snapshotting, direct typed mutation responses, RBAC route coverage, bounded
reads plus immutable segmented logs, dashboard prepared-context flow and every
disabled/quota/capability state, Helm security context, permanent default deny,
no-RBAC projected broker identity, deterministic NetworkPolicy and Job
Create/AlreadyExists/UID/cancellation/node-loss/cleanup recovery, broker token
refresh and capability-epoch revocation, foreign ancillary-object collision,
normal short-final log segments, byte-limit FINAL, logger-loss synthetic FINAL,
gap/orphan handling, final-digest mismatch, per-call ECR limiter
contention in the trusted publisher, internal OCI staging/cache token
scope/ownership/quota/GC, and live S3 plus R2 context-store capability and drift
tests.

Release evidence includes real PostgreSQL migration/concurrency tests, MinIO or
S3-compatible integration, the hostile sandbox conformance matrix plus a real
BuildKit build and cache hit, one ECR publication, dashboard production
build/manual pass, Terraform validation, and a security review of the executor,
Job manifest, and credential scopes. The activation runbook proves the
dedicated tainted builder pool, sandbox RuntimeClass, fixed builder namespace,
admission policy, writable-volume allowlist, network policy, node-loss cleanup,
and disabled-builder behavior before and after Helm rollback. The builder
joins the distribution design's final six paired Codex/Fable exact-head rounds
only after all of those surfaces are frozen.

## Explicit non-goals

- inferring build steps from arbitrary task setup;
- mutable base tags or implicit private-base credentials;
- privileged Docker-in-Docker or host socket access;
- GPU builds before a demonstrated requirement;
- speculative ARM64 builds;
- lazy container filesystems or memory snapshots;
- using R2 objects as runtime container images; and
- mutable release channels without a central generationed consumer snapshot.
