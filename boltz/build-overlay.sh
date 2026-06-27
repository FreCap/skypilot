#!/usr/bin/env bash
# Build (and optionally push) the boltz overlay image from THIS fork checkout.
# Run from anywhere inside the repo; uses the current HEAD.
#
# Overlays only the fork's changed runtime files (git diff <FORK_BASE> HEAD --
# 'sky/**', tests excluded) onto the pinned upstream nightly. Minimal on purpose:
# we never shadow base-image package files that must match the installed wheel.
#
#   PUSH=true TAG=<ecr>/skypilot-nightly-boltz:<ver>-<n> ./boltz/build-overlay.sh
#
# Env (all have sensible defaults):
#   BASE_VER   upstream nightly tag to base on (keep in sync with the platform
#              chart_version, normalized "-dev." -> ".dev"). Default below.
#   BASE_IMAGE override the full base image (default berkeleyskypilot/...:$BASE_VER)
#   FORK_BASE  the upstream-import commit on this fork (overlay diff baseline)
#   TAG        full image ref to build/push (default builds a local dev tag)
#   PUSH       "true" to docker push (default false)
#   PLATFORM   docker build --platform (default linux/amd64 — control-plane arch)
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

BASE_VER="${BASE_VER:-1.0.0.dev20260620}"
BASE_IMAGE="${BASE_IMAGE:-berkeleyskypilot/skypilot-nightly:${BASE_VER}}"
FORK_BASE="${FORK_BASE:-87aeb8a}"
PLATFORM="${PLATFORM:-linux/amd64}"
TAG="${TAG:-skypilot-nightly-boltz:${BASE_VER}-dev}"
PUSH="${PUSH:-false}"

echo ">> Overlay file set (git diff ${FORK_BASE}..HEAD -- 'sky/**'):"
files=()
while IFS= read -r f; do [ -n "$f" ] && files+=("$f"); done < <(
  git diff --name-only "$FORK_BASE" HEAD -- 'sky/**' | grep -vE '(^|/)tests?/')
if [ "${#files[@]}" -eq 0 ]; then echo "  none — aborting" >&2; exit 1; fi
printf '   %s\n' "${files[@]}"

ctx="$(mktemp -d)"; trap 'rm -rf "$ctx"' EXIT
for f in "${files[@]}"; do mkdir -p "$ctx/$(dirname "$f")"; cp "$f" "$ctx/$f"; done
cp boltz/Dockerfile.overlay "$ctx/Dockerfile"

echo ">> Building ${TAG} (${PLATFORM}, base ${BASE_IMAGE})"
docker build --platform "$PLATFORM" --build-arg "BASE_IMAGE=${BASE_IMAGE}" -t "$TAG" "$ctx"

echo ">> Verifying overlaid modules import + control-loop API present"
docker run --rm --platform "$PLATFORM" "$TAG" python -c "
import inspect
import sky.serve.controller, sky.serve.replica_managers, sky.serve.load_balancer
from sky.utils import controller_utils
import sky.server.config
assert hasattr(controller_utils, 'in_flight_launch_count')
assert 'in_flight' in inspect.signature(controller_utils.can_provision).parameters
print('overlay verify OK')"

if [ "$PUSH" = "true" ]; then echo ">> Pushing ${TAG}"; docker push "$TAG"; fi
echo ">> Done: ${TAG}"
