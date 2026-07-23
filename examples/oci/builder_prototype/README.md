# Executable image-builder prototype

These maintainer-only specifications exercise the disabled image-builder
prototype against the two workloads used for the first live startup benchmark:

- `boltz-l4-fleet`, based on the existing Boltz 2 model image; and
- `opendde-10c200s-v4`, with its deterministic Python environment and OpenDDE
  source checkout moved out of replica startup.

The output repository is the existing private, immutable-tag ECR repository
readable by the SkyPilot AWS compute role. The runner writes a unique staging
tag, verifies the resulting digest-pinned reference, and writes each
digest-keyed cache tag only once. On first execution it creates and bootstraps
the dedicated local `skypilot-image-builder-prototype` Buildx worker with the
`docker-container` driver.

Authenticate Docker to ECR before running the prototype:

```bash
aws ecr get-login-password \
  --profile boltz-data-lake \
  --region us-east-1 |
docker login \
  --username AWS \
  --password-stdin \
  699626303757.dkr.ecr.us-east-1.amazonaws.com
```

Validate both closed specifications without building:

```bash
export SKYPILOT_IMAGE_BUILDER_PROTOTYPE=1

python -m sky.container_images.builder_prototype \
  examples/oci/builder_prototype/boltz-l4-fleet.builder.yaml \
  --context examples/oci/builder_prototype/context \
  --validate-only

python -m sky.container_images.builder_prototype \
  examples/oci/builder_prototype/opendde-10c200s-v4.builder.yaml \
  --context examples/oci/builder_prototype/context \
  --validate-only
```

Execute a real Linux AMD64 build and push:

```bash
python -m sky.container_images.builder_prototype \
  examples/oci/builder_prototype/boltz-l4-fleet.builder.yaml \
  --context examples/oci/builder_prototype/context \
  --execute-direct
```

The command returns JSON containing `reference`, `elapsed_seconds`,
`cache_hits`, and the identities of the specification, context, and dependency
cache. Run an identical build a second time to prove the registry cache hit.

For live Serve evidence, use Spot as the primary market and enable
`dynamic_ondemand_fallback` so a capacity shortage does not turn image timing
into an unbounded placement wait. Record controller registration, launch-budget
wait, Spot acquisition, node provisioning, image pull, runtime setup, and
readiness separately.

The July 23 OpenDDE comparison did not meet the 120-second readiness target.
On the same on-demand `g6.xlarge` fallback shape, the source image reached
readiness 351.09 seconds after provisioning began and the built image took
375.19 seconds. Both spent about 141 seconds between cluster launch and
readiness because both still staged 9.35 GiB of workload data. The result
validates the builder and managed pull path, but it does not justify a
faster-deployment claim or broadening this prototype into a data-locality
system.

The Boltz output was also exercised as a secret-free L4 runtime smoke test.
The managed 4,888,012,862-byte manifest resolved, authenticated with
`ecr-login`, pulled cold, started its container, executed Python, and passed an
HTTP readiness probe. This does not replace a model-readiness or inference test:
the production fleet's R2 and payload-encryption secrets are required for that
run block and were unavailable to the benchmark.

This evidence mode is not a release publisher. It has no durable coordinator,
does not create a SkyPilot release, and must not be used as the production
publication path.
