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

This evidence mode is not a release publisher. It has no durable coordinator,
does not create a SkyPilot release, and must not be used as the production
publication path.
