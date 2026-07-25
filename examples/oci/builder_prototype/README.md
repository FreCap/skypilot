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

The July 23 and July 24 OpenDDE comparisons did not meet the 120-second
readiness target. The controlled repeat fixed both services to the same
on-demand `g6.xlarge`. The source replica reached readiness in 408.823 seconds
and the built replica took 404.196 seconds. Both still staged the same 7,835
objects and 9.35 GiB of workload data. The 4.626-second difference is run
noise, not a faster-deployment result.

The Boltz output first passed a secret-free L4 runtime smoke test. A later
controlled run injected its real read-only R2 and payload-encryption secrets
into isolated one-replica services. Both the direct digest path and the
`boltz-managed` release loaded the real model and passed HTTP readiness. The
managed release used the qualified ECR-helper AMI and automatically opened
TCP/8080.

The builder changes where the cold work occurs, but does not yet reduce its
total:

| Phase | Source R2 | Direct R3 | Managed R3 |
| --- | ---: | ---: | ---: |
| replica record to cluster launched | 86.850 s | 312.347 s | 303.512 s |
| cluster launched to readiness | 167.486 s | 52.797 s | 57.907 s |
| replica record to readiness | 254.336 s | 365.144 s | 361.419 s |

The direct path saved 114.689 seconds after cluster launch, but its cold OCI
pull and unpack spent 177.158 seconds in `initialize_docker`. The managed
ECR-helper path reduced that phase to 154.233 seconds, but remained 107.083
seconds slower than the source control end to end. Registry locality and
credential automation therefore pass the correctness gate, not the cold-start
performance gate. Reaching a real two-minute total improvement requires a
separate prewarmed snapshot, node-cache, or lazy-runtime capability.

This evidence mode is not a release publisher. It has no durable coordinator,
does not create a SkyPilot release, and must not be used as the production
publication path.
