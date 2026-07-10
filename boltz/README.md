# boltz overlay image

This fork (`improvements`) carries boltz's SkyPilot serve / control-plane fixes. They reach the
live control plane as a **full fork wheel** installed onto the pinned upstream
`berkeleyskypilot/skypilot-nightly` runtime base, plus a **freshly built dashboard**. Replacing the
whole wheel is deliberate — the base is pinned to an older nightly than the fork's tree (which
rebases on upstream master), so a changed-files-only overlay would mix old-wheel modules with newer
fork modules. Installing the wheel keeps source, package metadata, and the runtime version consistent.
The dashboard: the base image's `sky/dashboard/out` bundle is baked at nightly-build time, so the
script always rebuilds the static export from this fork's `sky/dashboard` source and ships it in the
overlay (otherwise the deployed dashboard lags the fork's python and fork dashboard changes never
deploy).

- **Build/verify locally:** `./boltz/build-overlay.sh` (add `PUSH=true TAG=<ref>` to push). Requires
  **node/npm (Node 20+)** for the dashboard build, e.g. `mise x node@24 -- ./boltz/build-overlay.sh`.
- **Publish:** `.github/workflows/boltz-overlay-publish.yml` builds on every `improvements` push and
  pushes to `255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz`, tagged
  `<sky.__version__>-g<sha>` (immutable) and `<sky.__version__>-improvements` (moving).
- **Consume:** the platform repo pins `apiService.image` to the immutable tag in the
  skypilot-control-plane terragrunt.

The upstream nightly tag is only a runtime dependency. The product version comes exclusively from
`sky.__version__` and is shared by Python metadata, CLI/API/dashboard output, image tags, and charts.

### Enabling pushes (one-time)

Create an OIDC role in the gitops-hub account (255203429798) trusting `boltz-bio/skypilot`, with ECR
push on `skypilot-nightly-boltz`, and set its ARN as the `OVERLAY_ECR_ROLE_ARN` repo variable. Until
then the workflow builds (validating the overlay) but skips the push.
