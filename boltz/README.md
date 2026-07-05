# boltz overlay image

This fork (`improvements`) carries boltz's SkyPilot serve / control-plane fixes. They reach the
live control plane as an **overlay image**: the **full fork `sky/` tree** (every tracked file under
`sky/**`, tests excluded) layered onto the pinned upstream `berkeleyskypilot/skypilot-nightly` base,
plus a **freshly built dashboard**. Shadowing the whole wheel `sky/` is deliberate — the base is
pinned to an older nightly than the fork's tree (which rebases on upstream master), so a
changed-files-only overlay would mix old-wheel modules with newer fork modules; the full tree keeps
`sky/` internally consistent at the fork's commit, the composition production has been validated on.
The dashboard: the base image's `sky/dashboard/out` bundle is baked at nightly-build time, so the
script always rebuilds the static export from this fork's `sky/dashboard` source and ships it in the
overlay (otherwise the deployed dashboard lags the fork's python and fork dashboard changes never
deploy).

- **Build/verify locally:** `./boltz/build-overlay.sh` (add `PUSH=true TAG=<ref>` to push). Requires
  **node/npm (Node 20+)** for the dashboard build, e.g. `mise x node@24 -- ./boltz/build-overlay.sh`.
- **Publish:** `.github/workflows/boltz-overlay-publish.yml` builds on every `improvements` push and
  pushes to `255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz`, tagged
  `<BASE_VER>-g<sha>` (immutable) and `<BASE_VER>-improvements` (moving).
- **Consume:** the platform repo pins `apiService.image` to the immutable tag in the
  skypilot-control-plane terragrunt.

`BASE_VER` (the upstream nightly to base on) must track the platform's `chart_version`; it's set in
both `build-overlay.sh` and the workflow `env`.

### Enabling pushes (one-time)

Create an OIDC role in the gitops-hub account (255203429798) trusting `boltz-bio/skypilot`, with ECR
push on `skypilot-nightly-boltz`, and set its ARN as the `OVERLAY_ECR_ROLE_ARN` repo variable. Until
then the workflow builds (validating the overlay) but skips the push.
