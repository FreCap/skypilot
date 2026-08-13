# boltz overlay image

This fork (`improvements`) carries boltz's SkyPilot serve / control-plane fixes. They reach the
live control plane as a **full fork wheel** installed onto the pinned upstream
`berkeleyskypilot/skypilot-nightly` runtime base, plus a **freshly built dashboard**. Replacing the
whole wheel is deliberate — the base is pinned to an older nightly than the fork's tree (which
rebases on upstream master), so a changed-files-only overlay would mix old-wheel modules with newer
fork modules. Installing the wheel keeps source, package metadata, and the runtime version consistent.
The build context includes the complete tracked `sky/` tree and every source declared by
`setup.py`'s `py_modules`; the build and reuse checks import the authorization bootstrap so a wheel
cannot silently omit its pre-import trust boundary.
The dashboard: the base image's `sky/dashboard/out` bundle is baked at nightly-build time, so the
script always rebuilds the static export from this fork's `sky/dashboard` source and ships it in the
overlay (otherwise the deployed dashboard lags the fork's python and fork dashboard changes never
deploy).

- **Build/verify locally:** `./boltz/build-overlay.sh` (add `PUSH=true TAG=<ref>` to push). Requires
  **node/npm (Node 20+)** for the dashboard build, e.g. `mise x node@24 -- ./boltz/build-overlay.sh`.
- **Publish:** `.github/workflows/boltz-overlay-publish.yml` computes the next deterministic patch
  release for every `improvements` merge and publishes the image as that exact version
  (for example, `1.1.1`). The chart publisher follows the successful image run and publishes the
  same version before marking it with a Git tag. Commit identity stays in artifact metadata.
- **Consume:** the platform repo pins one release version and derives both the chart and image from
  it in the skypilot-control-plane terragrunt.

The upstream nightly tag is only a runtime dependency. `boltz/release_version.py` derives the
release patch from every first-parent commit after the recorded `1.1.19` epoch. The publisher
stamps that version consistently into Python metadata, CLI/API/dashboard output, the image, and the
chart.

### Reserved-fill reclaim policy

The overlay also builds and installs the separate
`boltz-skypilot-reserved-fill-reclaim-policy` wheel. The generic SkyPilot wheel intentionally has no
deployment-policy entry point. Before activating reserved-capacity fill, run the complete fleet
preflight from the built image:

```bash
python -m boltz_reserved_fill_reclaim_policy
```

Success prints one schema-1 JSON object and exits zero. Failure prints one redacted schema-1 JSON
object and exits nonzero. The preflight is expected to fail until both Kubernetes contexts match the
code-owned queue, priority, admission, scheduler, Kueue, accelerator, and Pod Identity inventory.
The exact contract and fix-forward deployment sequence are maintained in
`docs/designs/serve-multi-pool-reserved-capacity-fill.md`.

### Enabling pushes (one-time)

Create an OIDC role in the gitops-hub account (255203429798) trusting `boltz-bio/skypilot`, with ECR
push on `skypilot-nightly-boltz`, and set its ARN as the `OVERLAY_ECR_ROLE_ARN` repo variable. Until
then the workflow builds (validating the overlay) but skips the push.
