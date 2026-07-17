# Managed Container Image Builder

Status: proposal, not implemented

Last updated: 2026-07-16

## Decision summary

SkyPilot should eventually support building an immutable container image from
a digest-pinned base image and an explicit build-time setup block. The builder
should be a producer beside the external OCI importer. It must not be part of
the registry distribution worker, and it must not silently reinterpret the
existing top-level `setup` command.

The build should run once per unique build specification and target platform.
Its verified output digest should enter the managed image catalog and then use
the existing distribution and locality machinery. Top-level `setup` remains a
runtime hook for node-, replica-, mount-, credential-, or topology-dependent
work.

This capability is intentionally outside the initial managed container image
distribution implementation.

## Context

SkyPilot currently performs these task stages in order:

1. Provision a node or pod using the selected runtime image.
2. Sync the workdir and file or storage mounts.
3. Run top-level `setup` on every task node or service replica.
4. Run the workload.

This is flexible, but large services repeatedly install the same dependencies
and compile the same code. Repetition increases cold-start time, consumes
accelerator capacity while replicas initialize, and exposes every replica to
package-server and network failures.

The managed image distribution system imports and copies existing
digest-pinned OCI images. It deliberately does not create new image content.
Adding a builder should preserve that separation of responsibilities.

## Goals

- Build deterministic setup work once and reuse its immutable output.
- Preserve a runtime setup phase for operations that cannot be baked safely.
- Reuse one build across clouds and regions with the same runtime platform.
- Share one pulled image across every GPU process on a multi-GPU node.
- Make build cache identity explicit, reproducible, and workspace-scoped.
- Keep build credentials out of image layers, durable state, and logs.
- Recover safely from API server, builder, and registry interruptions.
- Feed the resulting digest into the existing catalog and distribution paths.

## Non-goals

- Automatically converting arbitrary top-level `setup` commands into builds.
- Baking cloud credentials, node identity, service discovery, or mounted state.
- Installing host GPU drivers or other host-specific runtime components.
- Building every supported architecture when no target requires it.
- Baking model weights into every image by default.
- Implementing a new image format or registry protocol outside OCI.

## Proposed task interface

The exact schema remains open for review. A minimal compatible extension could
nest a build producer under `resources.container_image`:

```yaml
resources:
  accelerators: L4:1
  container_image:
    distribution: default
    build:
      base: ghcr.io/boltz/boltz@sha256:<base-digest>
      platform: linux/amd64
      context:
        root: .
        include:
          - pyproject.toml
          - uv.lock
          - src/
      setup: |
        uv sync --frozen
        python -m compileall src

# This remains runtime work and runs on every replica.
setup: |
  test -r /models/current/model.safetensors

run: |
  exec ./start-server
```

`container_image.build` is a producer specification and is mutually exclusive
with selecting a final image through `container_image.ref`, `release`, or
`artifact_id`. Its `base` accepts the same selector forms and is resolved to an
immutable digest before hashing or scheduling the build.

The build block must be explicit. A boolean such as `build_setup: true` would
make it unclear whether top-level `setup` runs at build time, runtime, or both,
and would change behavior for commands that rely on runtime-only inputs.

## Build and runtime boundary

Appropriate build-time operations include:

- installing pinned operating-system and Python packages;
- compiling source code or CPU-side extensions;
- copying explicitly selected application files; and
- generating immutable application assets.

Runtime `setup` should retain:

- storage mounts and checks for mounted model artifacts;
- cloud, registry, and workload credentials;
- node rank, node addresses, and replica-specific configuration;
- service discovery and runtime port selection;
- GPU health checks and driver-dependent initialization; and
- selection of mutable models or datasets.

Model weights should remain separate content-addressed artifacts by default.
Embedding them should require an explicit snapshot because weights can be very
large, update independently from code, and carry separate access or licensing
constraints.

## Component boundaries

```text
Task or image build request
            |
            v
Build planner and build record
            |
            v
Isolated BuildKit worker and shared remote cache
            |
            v
Canonical registry output
            |
            v
Digest, platform, and policy verification
            |
            v
Immutable image catalog artifact
            |
            v
Existing distribution reconciler
            |
            v
Placement-specific runtime reference
```

The ownership split is:

- The builder owns build scheduling, cache identity, execution, and logs.
- The catalog owns digest-unique content identity and release bindings.
- The distribution reconciler owns canonical and regional materialization.
- The placement resolver owns selection of a verified runtime reference.
- The launcher owns pulling the resolved image and executing runtime setup.

The build must not execute inline in an API request. The API creates or reuses
a durable build record and waits or returns an asynchronous request identifier.

## Artifact and build data model

Image artifacts remain unique by `(workspace, digest)`. Producer metadata on a
digest-unique artifact row cannot represent the full relationship because:

- multiple build specifications can produce identical bytes;
- the same bytes can be imported from an external source and built by
  SkyPilot; and
- a build needs status, attempts, leases, logs, and an input artifact before an
  output digest exists.

Add a many-to-one build relation rather than using artifact-level producer
metadata as the build cache or provenance source:

```text
container_image_builds
  id
  workspace
  spec_hash
  builder_version
  platform
  base_image_id
  state
  output_image_id NULL
  lease_owner NULL
  lease_token NULL
  lease_expires_at NULL
  attempt_count
  error_code NULL
  error_message NULL
  log_reference NULL
  created_at
  updated_at

UNIQUE (workspace, spec_hash)
```

The normalized specification hash includes the builder version and target
platform, so the uniqueness constraint can remain compact. A completed build
links to the digest-unique artifact through `output_image_id`. Different build
records may legitimately point to the same artifact.

Artifact-level `producer_kind`, `producer_spec_hash`, and `builder_version`
fields, if retained, should be documented as first-acquisition metadata rather
than complete provenance. The build relation is authoritative for build cache
lookup and build provenance.

## State and recovery

The initial state machine is:

```text
PENDING -> BUILDING -> VERIFYING -> READY
              |            |
              v            v
            FAILED <-------+
```

Workers claim bounded batches using expiring, fenced leases. Every transition
checks the current lease token. Retrying a build reuses the same immutable
specification and increments its attempt count; it does not create a second
concurrent build for the same workspace and specification hash.

After a worker interruption:

- an expired `BUILDING` lease is reclaimable;
- a registry output is accepted only after its digest and platform descriptors
  are reverified;
- `READY` requires both a verified output and a catalog artifact binding; and
- service or job launch never consumes a partial build.

SkyServe must resolve and snapshot the completed artifact before creating any
replicas for a service version. Updates and recovery continue using that exact
artifact rather than rebuilding or resolving mutable inputs independently.

## Cache identity

The specification hash includes:

- the resolved base-image digest;
- normalized build commands;
- an exact content digest for every included context file;
- target operating system, architecture, and variant;
- the builder implementation version;
- declared non-secret build environment; and
- secret identifiers and mount paths, but never secret values.

File modification times do not affect the context digest. Symlink handling,
permissions, and ignored files must be normalized consistently on clients and
the server.

Changing a secret value does not naturally invalidate a safe build cache. The
interface should therefore support an explicit non-secret `cache_epoch` when a
secret-backed dependency changes without any other specification change.

BuildKit's remote registry cache should be workspace-scoped and separate from
the final runtime image. Ephemeral builders import it before execution and
export successful layers afterward. Cache failure may make a build slower, but
must not change its correctness or output verification.

## Context and secret safety

A durable image has a broader exposure boundary than a transient workdir sync.
The initial interface should require explicit context inclusion rather than
silently copying the entire workdir. It should honor the normal SkyPilot ignore
rules and reject paths that escape the declared root.

Build secrets are passed only through ephemeral BuildKit secret or SSH mounts.
They must never be:

- represented as Dockerfile `ARG` or `ENV` values;
- copied into the build context;
- stored in build specifications, database rows, or cache keys;
- written into image layers or provenance documents; or
- returned in logs, errors, API responses, or debug dumps.

Builders should run in isolated, resource-bounded workers with short-lived
registry credentials. Repository and credential provisioning remains a
provider adapter responsibility, matching the distribution design.

## Platforms, clouds, and accelerators

Build variants normally follow OCI platform identity, not cloud identity. One
`linux/amd64` output can be reused on compatible AWS, GCP, Kubernetes, Nebius,
and generic servers. Regional copies are distribution concerns and do not
trigger rebuilds.

The planner may infer a platform only when every candidate placement agrees.
Otherwise it must require a platform or build one variant for each required
platform. The current Boltz L4 fleet should build only `linux/amd64`; it should
not pay for ARM64 without an actual ARM64 placement.

Multi-GPU instances do not require per-GPU images. The runtime pulls one image
per node, and every model process on that node shares its layers. Starting one
process per GPU remains workload runtime logic.

The initial builder should be CPU-only. GPU-backed build steps can be added
later through an explicit builder resource specification. Such builds must
include the declared accelerator build profile and relevant ABI constraints in
cache identity. Host drivers must never be captured in the output image.

## Expected performance

For `R` replicas, runtime-only initialization performs approximately
`R * T_setup` aggregate setup work. Concurrent startup hides some wall-clock
time but still repeats downloads, compilation, and failure opportunities while
accelerators are allocated.

With a cached derived image, replica readiness changes approximately from:

```text
pull(base image) + full runtime setup
```

to:

```text
pull(derived image) + small runtime setup
```

The derived image may be larger, so canonical and regional locality remain
important. The build is paid once per changed specification and platform,
rather than once per replica.

## Incremental implementation

1. Add the build model, API contract, normalized specification hashing, and
   PostgreSQL build table without changing runtime selection.
2. Add one resource-bounded BuildKit worker for CPU-only `linux/amd64` builds,
   with registry-backed cache and secret mounts.
3. Verify output descriptors and bind successful builds to catalog artifacts.
4. Feed the output artifact into the existing distribution reconciler.
5. Snapshot build outputs for cluster, managed job, and SkyServe recovery.
6. Add quotas, garbage collection, metrics, audit history, and administrative
   controls based on measured use.
7. Consider native ARM64 or GPU builders only when an actual workload requires
   them.

Until this exists, users can build images in CI, publish digest-pinned outputs,
and use the managed image distribution system to place those existing images.

## Open questions

- Should the durable public interface remain under
  `resources.container_image`, or should container environment definition move
  outside compute resources?
- Should any workdir files be included by default, or should every build
  context remain explicit?
- Which provenance, signing, SBOM, and vulnerability policies should gate
  catalog admission?
- Should build logs use existing request-log storage or a dedicated bounded
  artifact store?
- What build concurrency and per-workspace quota defaults fit production
  without allowing builds to compete with workload capacity?
- Which model artifacts, if any, justify explicit image embedding instead of a
  separate content-addressed model cache?

## References

- SkyPilot task YAML specification:
  https://docs.skypilot.co/en/stable/reference/yaml-spec.html
- Modal image construction and caching:
  https://modal.com/docs/guide/images
- Docker build secrets:
  https://docs.docker.com/build/building/secrets/
- Docker cache optimization:
  https://docs.docker.com/build/cache/optimize/
- Docker multi-platform builds:
  https://docs.docker.com/build/building/multi-platform/
