# Managed container image builder

Status: post-v0 seam and prototype gate, not a v0 public product

Owner: image build service

Last updated: 2026-07-23

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
  - run: apt-get update && apt-get install -y libgl1
    inputs: []
  - run: pip install --require-hashes -r /inputs/requirements.txt
    inputs:
      - requirements.txt
context:
  include:
    - requirements.txt
    - src/**
source:
  mode: late_bound
  include:
    - src/**
platform: linux/amd64
output:
  workspace: research
  distribution: gpu-production
  release: boltz-runtime-2026-07-20
  staging_repository: 123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/staging
  source_auth: registry-copy
```

`setup` is intentionally supported as a build layer. It is an explicit build
field, not the workload's runtime `setup`. Each step requires `run` and an
explicit bounded `inputs` list. The executor mounts only those context files at
`/inputs`; arbitrary shell commands cannot silently read undeclared context.
Commands run in order in an isolated BuildKit executor. Changing a setup command,
base digest, declared input, build argument, or platform invalidates the
appropriate cache suffix.

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
verification, demand fencing, and deletion cannot orphan a child.

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
- digests of every explicitly mounted setup input; and
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

The requested release is advisory prototype metadata, not a distribution
reservation. A digest does not exist yet, so temporary build state cannot
serialize against normal v0 publication. At handoff, the trusted publisher
submits the ordinary digest-backed publication transaction and may receive
`RELEASE_CONFLICT` if another caller won the name. A failed build or publication
leaves the prior release or service version untouched. True pre-build
reservation requires reviewed durable post-gate schema and expiry semantics.
V0 releases remain immutable, so publishing a replacement uses a new release
name. A later mutable channel must snapshot one generation across every service,
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

The prototype is invoked only by maintainers and uses an isolated disposable
PostgreSQL schema rather than migration 024. Its state survives coordinator and
worker crashes but is never read by a production API binary. It must demonstrate:

1. one explicit AMD64 setup-layer build;
2. a second identical build with a material cache hit;
3. a code-only late-bound change requiring no dependency-image rebuild;
4. R2 or S3 context upload without routing bytes through the API server;
5. crash recovery before and after BuildKit output publication;
6. trusted promotion into the existing distribution publication contract; and
7. deterministic handling when the advisory release name loses a publication
   race;
8. zero secret values in image history, logs, database rows, or attestations.

The repository prototype lives in `sky/container_images/builder_prototype.py`.
It is absent from the public API, SDK, YAML schema, and Dashboard, and its
maintainer CLI refuses to run unless
`SKYPILOT_IMAGE_BUILDER_PROTOTYPE=1` is set. The CLI currently validates and
content-addresses a context.

### Executable direct-evidence runner

The maintainer CLI also has an explicitly selected `--execute-direct` evidence
mode. It uses the same closed build specification, filtered context, dependency
cache key, Dockerfile generator, and BuildKit semantics as the durable
coordinator, but executes through a locally configured Docker Buildx worker and
pushes directly to the specification's staging repository. It verifies the
result through its digest-pinned registry reference before reporting success.

This mode exists to measure real images before standing up the coordinator and
worker service. It has the following intentionally narrow contract:

- `SKYPILOT_IMAGE_BUILDER_PROTOTYPE=1` remains mandatory;
- the build specification must select `distribution: direct` and no source
  authorization binding;
- registry authentication is an operator prerequisite, never a command-line
  secret argument;
- the explicit execution path creates and bootstraps one named local
  `docker-container` Buildx worker when it does not already exist, so registry
  cache export does not depend on the Docker daemon's image-store setting;
- multiline setup steps are rendered as deterministic Dockerfile heredocs and
  run with shell fail-fast behavior; changing those rendering semantics bumps
  the cache policy version rather than reusing an incompatible cache tag;
- direct evidence disables BuildKit's implicit provenance wrapper so identical
  inputs produce the same image-manifest digest. Product provenance remains an
  explicit, separately addressed and signed publication artifact rather than
  nondeterministic metadata hidden in the runnable image digest;
- output is one immutable digest-pinned OCI reference, not a named SkyPilot
  release;
- the runner has no cancellation recovery or durable state, so it must never
  be used as the production publication path; and
- a digest-keyed registry cache is created once and then treated as read-only,
  which is compatible with immutable-tag ECR repositories.

`--validate-only` remains the safe default operation. The CLI requires exactly
one of `--validate-only` or `--execute-direct`, so validation cannot
accidentally trigger a build. The executable result reports build duration,
cache hits, context identity, dependency-cache identity, log path, staging tag,
and digest-pinned reference without returning credentials.

The first live evidence pair uses isolated one-replica Serve services derived
from `boltz-l4-fleet` and `opendde-10c200s-v4`. Both use Linux AMD64 images in
one AWS region. The executable runner may itself run on a disposable CPU worker
in that registry region; a laptop-side multi-gigabyte pull is not representative
builder evidence. A same-region baseline and built-image run measure
replica `time_to_ready_seconds`; image publication time is reported separately
and is never hidden inside deployment readiness. The target is at least 120
seconds lower readiness for each service when its existing runtime setup has
that much removable work. A smaller improvement is a measured gate failure,
not rounded up to success.

The coordinator, isolated PostgreSQL schema, S3-compatible uploader, fenced
BuildKit executor, staging verification, and ordinary publication handoff
remain exercised through the internal harness and tests. They are not enabled
as a production service by the direct-evidence runner.

### Live benchmark result, July 23, 2026

The OpenDDE prototype built and published the declared Linux AMD64 setup layer,
and a managed release resolved to its same-region, digest-pinned ECR target.
The source manifest contained 3,626,317,722 compressed bytes. The built
manifest retained the same ten base-layer digests and added two layers totaling
85,565,515 compressed bytes, for 3,711,883,237 compressed bytes overall.

The repaired AWS pull plan used `credential_helper: ecr-login` on a fresh node.
It performed no `docker login`, ECR token command, or AWS CLI installation on
the pull path. Pulling the cold managed image and starting its container took
about 102.25 seconds. This proves the qualified credential-helper contract, but
it is not a startup-latency win by itself.

A same-server, same-region comparison then used Spot by default with dynamic
on-demand fallback. Every `g6.xlarge` Spot zone was initially exhausted, so the
availability measurement used the fallback while SkyServe continued seeking
Spot. Comparing the same on-demand `g6.xlarge` shape removes that placement
delay:

| Phase | Source image | Built image | Difference |
| --- | ---: | ---: | ---: |
| provision start to cluster launched | 210.31 s | 233.90 s | built +23.59 s |
| cluster launched to readiness | 140.78 s | 141.29 s | built +0.51 s |
| provision start to readiness | 351.09 s | 375.19 s | built +24.10 s |

The runtime setup in both cases still staged 7,835 objects totaling
10,036,350,627 bytes (9.35 GiB). Moving the virtual environment and source
checkout into an 85.6 MB compressed image layer therefore removed no material
readiness time, while the slightly larger cold pull added time. The 120-second
readiness target failed. End-to-end service timings were further distorted by
global launch-budget and Spot-capacity waits, so they are retained only as
diagnostics, not as builder performance evidence.

This result keeps the builder behind its productization gate. The ordinary OCI
builder and distribution contracts are still useful for reproducibility,
credential isolation, immutable publication, and avoiding runtime package
drift. Making this workload at least two minutes faster would require a
separate node-cache or model-data locality mechanism, such as a qualified
prewarmed snapshot or lazy data/runtime path. It would not be honest to hide
that mechanism inside v0 setup-layer builds or to count first-request latency
as deployment savings.

The Boltz fleet image completed a separate runtime smoke test. Its source
manifest contained 35 layers and 4,888,012,650 compressed bytes. The prototype
retained every source layer and added one 212-byte marker layer, producing a
36-layer, 4,888,012,862-byte manifest. A Spot-first service with dynamic
on-demand fallback resolved that release and eventually admitted an on-demand
`g6.2xlarge` after shared launch-budget and Spot-capacity waits. From the start
of that admitted launch, the instance was up at 24.26 seconds, the managed
container was up at 204.71 seconds, the cluster was launched at 251.15 seconds,
and a Python HTTP process in the image passed readiness at 301.20 seconds. The
provision configuration preserved `credential_helper: ecr-login`.

This was intentionally a secret-free image/runtime smoke test, not a full
Boltz model-readiness benchmark. The production run block requires four
deployment secrets that were not present in the benchmark environment. The
test proves digest resolution, private-registry authentication, cold pull, L4
container startup, command execution, and readiness. It cannot claim that the
model initialized or that inference succeeded. Because the build adds no
meaningful payload beyond its marker, it also has no credible deployment-speed
claim.

Chart upgrades must merge the new chart defaults before applying the previous
release's values. Provider-specific environment variables, volume names, and
mount paths are reserved only when the corresponding native chart credential
block is enabled and actually emits those fields. This preserves existing
custom workload-identity integrations, including projected GCP credentials and
Nebius credentials, while still rejecting real duplicate fields.
The PostgreSQL migration Job sets server mode in both its process entrypoint
and Pod environment before resolving any database engine. A Job that upgrades
ephemeral SQLite while the central PostgreSQL revision remains unchanged is a
deployment failure, never a successful rollout.

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
- setup schema requires explicit inputs, undeclared context is inaccessible, and
  only a command or mounted-input digest change invalidates its cache suffix;
- upload deduplication, resume, size bounds, and expired credentials;
- cache key changes for every declared input and stability for late-bound code;
- secret non-retention in layers, history, logs, cache metadata, and provenance;
- single AMD64 default and explicit architecture validation;
- worker lease loss and cancellation around each external I/O boundary;
- staging output digest and platform verification;
- previous release remains visible until new publication commits READY;
- R2/S3 endpoint compatibility without treating either bucket as OCI; and
- end-to-end prototype timing evidence.
