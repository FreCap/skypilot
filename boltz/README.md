# Boltz SkyPilot production image

This fork (`improvements`) carries boltz's SkyPilot serve / control-plane fixes. They reach the
live control plane through the repository's **canonical `Dockerfile`**, built directly from the
exact fork commit on Python 3.14. The same Dockerfile installs the complete source distribution,
the AWS, GCP, and Kubernetes dependencies used by this deployment, the authorization bootstrap,
and a freshly built dashboard. The Boltz release wrapper adds immutable release metadata and the
separately reviewed reserved-fill reclaim policy. There is no inherited upstream nightly runtime
and no second overlay package or Dockerfile that can drift from the source image.

- **Build/verify locally:** `./boltz/build-overlay.sh` (add `PUSH=true TAG=<ref>` to push). The
  canonical Docker build installs Node 20 in its build stage; only Docker with BuildKit and a full
  Git history are required on the caller.
- **Publish:** `.github/workflows/boltz-overlay-publish.yml` computes the next deterministic patch
  release for every `improvements` merge and publishes the image as that exact version
  (for example, `1.1.1`). The chart publisher follows the successful image run and publishes the
  same version before marking it with a Git tag. Commit identity stays in artifact metadata.
- **Consume:** deploy the matching chart and immutable image digest directly with the existing
  SkyPilot Helm release. Pin the same digest on the API, controller, and executor roles; no
  `boltz-platform` runtime pin is part of this release path.

`boltz/release_version.py` derives the release patch from every first-parent commit after the
recorded `1.1.19` epoch. The publisher stamps that version plus the exact commit, commit timestamp,
and monotonic build count into Python and OCI metadata, verifies the running interpreter is Python
3.14, then verifies both distributions, cloud clients, and dashboard before publishing. The chart
publisher follows that verified image.

### Python runtime boundary

The Boltz Helm control-plane image and its deployment-only reclaim-policy package run on Python
3.14. The generic `skypilot` distribution still declares Python 3.10 as its minimum because that
same wheel is installed on provisioned workers. Those workers currently use Python 3.10 and
`ray==2.9.3`; Ray 2.9.3 publishes no Python 3.14 wheel. Moving the worker runtime to Python 3.14 is
a separate coordinated change: upgrade and qualify Ray, rebase the Ray patch set, update VM and
Kubernetes worker bootstraps and images, then raise the generic wheel floor and its compatibility
tests together. The production control-plane image does not install Ray and is not blocked by that
worker-runtime constraint.

### Reserved-fill reclaim policy

The overlay also builds and installs the separate
`boltz-skypilot-reserved-fill-reclaim-policy` wheel. The generic SkyPilot wheel intentionally has no
deployment-policy entry point. Before activating reserved-capacity fill, run the complete fleet
preflight from the built image:

```bash
python -m boltz_reserved_fill_reclaim_policy
```

Success prints one schema-1 JSON object and exits zero. Failure prints one redacted schema-1 JSON
object and exits nonzero. The preflight verifies East's direct Kubernetes contract and PHX's
externally owned `boltz-research/be -> research-be` Kueue lane, `be-lt` workload priority, low Pod
priority, scheduler, accelerator, and Pod Identity inventory. SkyPilot observes that contract; it
does not own or mutate Kueue queues, cohorts, quotas, priorities, borrowing, or preemption policy.
The exact contract and fix-forward deployment sequence are maintained in
`docs/designs/serve-multi-pool-reserved-capacity-fill.md`.

### Enabling pushes (one-time)

Create an OIDC role in the gitops-hub account (255203429798) trusting `boltz-bio/skypilot`, with ECR
push on `skypilot-nightly-boltz`, and set its ARN as the `OVERLAY_ECR_ROLE_ARN` repo variable. Until
then the workflow builds (validating the overlay) but skips the push.
