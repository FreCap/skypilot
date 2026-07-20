# Managed container image builder

Status: post-v0 seam and prototype gate, not a v0 public product

Owner: image build service

Last updated: 2026-07-20

## Decision

SkyPilot should eventually offer an explicit managed build operation that
produces the same immutable READY artifact consumed by
[`managed-container-image-distribution.md`](managed-container-image-distribution.md).
It must not infer a build from workload launch or arbitrary `setup` commands.
Build and publication run before deployment, retain the previous release until
the new output is verified, and keep credentials and large contexts away from
the API request path.

The first milestone is a disabled prototype used to validate cache value,
isolation, and executor feasibility. It adds no public YAML, database migration,
Build button, or always-on worker until the evidence gate passes.

## Modal-inspired boundary

The useful transferable properties are:

- preparation happens away from deployment;
- layers and build inputs are content-addressed;
- repeated build steps reuse deterministic cache keys;
- local application source can remain late-bound when it need not affect system
  dependencies;
- a new named output becomes visible only after publication succeeds; and
- model weights are not baked into every image by default.

SkyPilot does not control every runtime node and cannot assume Modal's global
filesystem, lazy container loader, or memory snapshots. The builder emits normal
OCI manifests. Any future lazy runtime is a separately detected capability.

## Proposed explicit interface

After the prototype gate, the CLI may expose:

```text
sky image build BUILD_SPEC \
    --release NAME --distribution PROFILE [--platform linux/amd64] [--no-wait]
sky image build status BUILD_ID
sky image build logs BUILD_ID
sky image build cancel BUILD_ID
```

A candidate build specification is:

```yaml
base: ghcr.io/boltz-bio/runtime@sha256:<digest>
setup:
  - apt-get update && apt-get install -y libgl1
  - pip install --require-hashes -r requirements.txt
context:
  path: .
  include:
    - requirements.txt
    - src/**
source:
  mode: late_bound
  include:
    - src/**
platforms:
  - linux/amd64
output:
  distribution: gpu-production
  release: boltz-runtime-2026-07-20
```

`setup` is intentionally supported as a build layer. It is an explicit build
field, not the workload's runtime `setup`. Commands run in order in an isolated
BuildKit executor. Changing a setup command, base digest, referenced setup file,
build argument, or platform invalidates the appropriate cache suffix.

`source.mode: late_bound` excludes application files from the dependency image
and uploads them through the normal SkyPilot workdir/file-mount path at launch.
This improves rebuild speed for code-only changes. `source.mode: image` copies
the declared source into the image and makes it part of the artifact digest.
The client shows which files affect the dependency layer and which remain
late-bound.

## Architecture selection

The default prototype builds only `linux/amd64`, matching the current Boltz GPU
fleet. SkyPilot does not build ARM64 merely because OCI supports it. Additional
platforms are built only when the user explicitly requests them or a later
deployment-set API proves they are required.

A multi-GPU instance does not require one build per card. CUDA capability,
driver compatibility, and framework variants belong in the base image or build
arguments, not the registry distribution count. One architecture manifest is
pulled once per node and may serve all visible GPUs.

Multi-platform publication is post-prototype. Before enabling it, distribution
must represent an OCI index and each child manifest as owned content so
verification, reference tracking, and deletion cannot orphan a child.

## Components

```text
client context packer
    |
    v
S3-compatible context store (R2, S3, or equivalent)
    |
    v
build coordinator in PostgreSQL
    |
    v
isolated BuildKit worker pool
    |
    v
trusted publisher -> distribution publication service -> release
```

The API validates metadata and creates one build intent. It never receives an
unbounded context body, runs BuildKit, or holds registry credentials. The client
creates a deterministic manifest of bounded paths, file digests, modes, and
sizes, then uploads missing blobs directly with short-lived object-store
credentials.

The build worker reads the immutable context manifest, runs a rootless or
otherwise isolated BuildKit executor, and writes only to a staging repository.
The trusted publisher verifies the output digest and provenance, adopts it
through the distribution publication service, and waits for its canonical
location before the release becomes visible.

## S3-compatible storage and R2

An account-level object-store profile may select R2, S3, or another compatible
service for:

- content-addressed build context blobs;
- build logs and bounded diagnostic bundles;
- SBOM, provenance, and signature artifacts;
- optional cache export/import; and
- separately managed model weights.

The object store is not emitted as `resources.container_image`, is not passed to
containerd or Docker, and does not satisfy a registry profile. The storage
profile contains endpoint, bucket, region, and named credential reference.
Secret values are resolved only by uploader or worker identities.

Uploads are resumable and deduplicated by digest. A build intent references one
immutable context-manifest digest. Retention deletes unreferenced blobs only
after build and attestation references expire.

## Cache model

The cache key includes:

- normalized builder frontend and version;
- base image digest;
- target platform;
- ordered setup commands;
- declared build arguments excluding secrets;
- digests of files read by each setup step; and
- compiler/runtime policy version.

Secret values never enter a cache key or layer. Secret mounts are ephemeral and
their use either disables shared cache export for that step or uses an explicit
secret-generation fingerprint that reveals no value.

The prototype first uses BuildKit's standard content cache with an OCI or object
store backend. A custom distributed filesystem is not required to validate the
product. Cache hit rate, bytes transferred, build latency, and eviction are
measured before any proprietary cache layer is considered.

## Publication semantics

Each build has a random ID and idempotency key. States are:

```text
PENDING -> UPLOADING -> QUEUED -> BUILDING -> VERIFYING -> PUBLISHING -> READY
                                                       \-> FAILED
```

Cancellation is allowed before trusted publication starts. Once publication
starts, cancellation stops waiting but does not guess whether the registry
write occurred. Recovery verifies staging and canonical digests.

The requested release is reserved internally but is not returned by public
release lookup until the distribution service commits it READY. A failed build
or publication leaves the prior release or service version untouched. V0
releases remain immutable, so publishing a replacement uses a new release name.
A later mutable channel must snapshot one generation across every service,
cluster, and job consumer before it can safely replace a name.

## Isolation and supply-chain boundary

- Build workers use a dedicated identity that cannot read the SkyPilot API
  database directly.
- Context reads are scoped to one build manifest.
- Registry writes target staging only.
- Only the trusted publisher can promote a verified output into managed
  distribution.
- Network egress is denied by default and enabled through named policies.
- Build secrets use ephemeral mounts and are absent from layers, logs,
  provenance arguments, and cache metadata.
- Output includes builder version, normalized spec hash, base digest, platform,
  SBOM digest, and provenance digest.
- Workloads, copy workers, lifecycle workers, and API roles cannot assume the
  builder or publisher roles.

## Prototype

The prototype is invoked only by maintainers and uses temporary state rather
than migration 024. It must demonstrate:

1. one explicit AMD64 setup-layer build;
2. a second identical build with a material cache hit;
3. a code-only late-bound change requiring no dependency-image rebuild;
4. R2 or S3 context upload without routing bytes through the API server;
5. crash recovery before and after BuildKit output publication;
6. trusted promotion into the existing distribution publication contract; and
7. zero secret values in image history, logs, database rows, or attestations.

The prototype may live behind an internal command or test harness. It cannot be
enabled in the Dashboard or normal client configuration.

## Productization gate

Public builder work proceeds only if all are true:

- at least two real services need repeated environment builds;
- median repeat build time improves by at least 50 percent or operator build
  steps fall by at least 80 percent;
- cache storage and egress cost are measured and bounded;
- BuildKit isolation and secret tests pass;
- build cancellation and ambiguous publication recovery converge;
- the distribution v0 API, UI, workers, and AWS slice are already accepted;
- the migration and rollout design is reviewed separately; and
- both adversarial reviewers return `PURSUE` on the exact prototype head.

If the gate fails, users keep building in CI and publish digest-pinned outputs
through the v0 distribution interface. That remains a complete product.

## Post-gate product scope

Only after the gate may a new migration add durable build intent, attempt,
context, log, attestation, and cache-reference rows. The public UI can then add
Build, logs, cancel, and retry. Worker pools remain separately scalable from
copy and lifecycle workers.

Future work may include:

- additional explicitly requested architectures;
- a remote Git context resolver with immutable commit verification;
- signed provenance policy;
- organization-wide base images;
- cache locality hints; and
- compatible lazy snapshotter hints when a runtime advertises support.

Model weights remain a separate data product. Memory snapshots remain a
separate runtime product. Neither is hidden inside the builder migration.

## Tests

- deterministic context manifest across filesystem ordering;
- include/exclude and symlink escape rejection;
- upload deduplication, resume, size bounds, and expired credentials;
- cache key changes for every declared input and stability for late-bound code;
- secret non-retention in layers, history, logs, cache metadata, and provenance;
- single AMD64 default and explicit architecture validation;
- worker lease loss and cancellation around each external I/O boundary;
- staging output digest and platform verification;
- previous release remains visible until new publication commits READY;
- R2/S3 endpoint compatibility without treating either bucket as OCI; and
- end-to-end prototype timing evidence.
