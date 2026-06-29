# boltz overlay image

This fork (`improvements`) carries boltz's SkyPilot serve / control-plane fixes. They reach the
live control plane as a thin **overlay image** — only this fork's changed `sky/` files layered onto
the pinned upstream `berkeleyskypilot/skypilot-nightly` base (so we never shadow base-image package
files that must match the installed wheel).

- **Build/verify locally:** `./boltz/build-overlay.sh` (add `PUSH=true TAG=<ref>` to push). The
  overlay baseline is derived as `git merge-base HEAD upstream/master`, so add the upstream remote
  once: `git remote add upstream https://github.com/skypilot-org/skypilot.git && git fetch upstream
  master` (or pass an explicit `FORK_BASE=<sha>`).
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
