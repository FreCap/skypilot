#!/usr/bin/env bash
# Build (and optionally push) the boltz overlay image from THIS fork checkout.
# Run from anywhere inside the repo; uses the current HEAD.
#
# Policy: overlay the FULL fork sky/ tree (every tracked file under sky/**,
# tests excluded) onto the pinned upstream nightly base. This is deliberate,
# not an optimization opportunity:
#
#   The base image is pinned (BASE_VER, e.g. a June-20 nightly) while the fork
#   tree tracks upstream master via rebases, so the fork's checkout is NEWER
#   than the base wheel even for files the fork never touched. A partial
#   overlay (changed-files-only) would mix old-wheel modules with fork modules
#   that assume their newer counterparts — a version skew inside one image.
#   Shipping the whole fork tree keeps sky/ internally consistent at exactly
#   the fork's commit, and is the composition production has been validated
#   on. Only files the base has but the fork tree lacks fall through to the
#   wheel (COPY overlays; it never deletes).
#
# The dashboard is ALWAYS rebuilt from this fork's source (sky/dashboard ->
# out/, requires node/npm) and shipped in the overlay: the base image's bundle
# is baked at nightly-build time, so without this the deployed dashboard would
# be older than the fork's python (stale enums render wrong) and fork dashboard
# changes would never deploy. out/ is gitignored, hence built here, not tracked.
#
#   PUSH=true TAG=<ecr>/skypilot-nightly-boltz:<ver>-<n> ./boltz/build-overlay.sh
#
# Env (all have sensible defaults):
#   BASE_VER   upstream nightly tag to base on (keep in sync with the platform
#              chart_version, normalized "-dev." -> ".dev"). Default below.
#   BASE_IMAGE override the full base image (default berkeleyskypilot/...:$BASE_VER)
#   TAG        full image ref to build/push (default builds a local dev tag)
#   PUSH       "true" to docker push (default false)
#   PLATFORM   docker build --platform (default linux/amd64 — control-plane arch)
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

BASE_VER="${BASE_VER:-1.0.0.dev20260620}"
BASE_IMAGE="${BASE_IMAGE:-berkeleyskypilot/skypilot-nightly:${BASE_VER}}"
PLATFORM="${PLATFORM:-linux/amd64}"
TAG="${TAG:-skypilot-nightly-boltz:${BASE_VER}-dev}"
PUSH="${PUSH:-false}"

echo ">> Overlay file set: full fork sky/ tree at HEAD (tests excluded)"
files=()
while IFS= read -r f; do [ -n "$f" ] && files+=("$f"); done < <(
  git ls-tree -r --name-only HEAD -- 'sky' | grep -vE '(^|/)tests?/')
if [ "${#files[@]}" -eq 0 ]; then echo "  none — aborting" >&2; exit 1; fi
echo "   ${#files[@]} tracked files under sky/"

# Informational only (does NOT gate the file set): compare the fork tree
# against the base image's installed sky/ files so the log shows how much of
# the wheel we shadow and how much the fork adds. Doubles as a sanity check
# that the base image is runnable before we spend time on npm + docker build.
# -i is required to feed the heredoc to python's stdin: without it docker runs
# `python -` against a closed stdin and the listing comes back EMPTY.
echo ">> Comparing against base image sky/ contents (informational)"
base_sky_files="$(docker run --rm -i --platform "$PLATFORM" "$BASE_IMAGE" python - <<'PY'
import os, sky
root = os.path.dirname(sky.__file__)
parent = os.path.dirname(root)
for dirpath, _dirs, names in os.walk(root):
    for name in names:
        print(os.path.relpath(os.path.join(dirpath, name), parent))
PY
)"
if [ -z "$base_sky_files" ]; then
  echo "error: empty sky/ file listing from base image ${BASE_IMAGE} —" >&2
  echo "       the base image is not runnable or the listing is broken." >&2
  exit 1
fi
shadowed="$(comm -12 <(printf '%s\n' "${files[@]}" | sort) <(sort <<<"$base_sky_files") | wc -l | tr -d ' ')"
added="$(( ${#files[@]} - shadowed ))"
echo "   base image sky/ files: $(wc -l <<<"$base_sky_files" | tr -d ' ')"
echo "   overlay shadows ${shadowed} base file(s); adds ${added} file(s) new vs base"

# Build the dashboard from THIS fork's source, unconditionally: the base
# image's bundle predates the fork's python (sky/dashboard/out is baked into
# the nightly at build time and out/ is gitignored here), so shipping anything
# but a fresh build reintroduces the stale-dashboard bug. Fail loudly —
# set -e aborts the whole overlay build if npm install/build fails.
echo ">> Building dashboard (sky/dashboard -> sky/dashboard/out)"
if ! command -v npm >/dev/null 2>&1; then
  echo "error: npm not found — the overlay build requires node/npm (Node 20+)" >&2
  echo "       to build the dashboard. See boltz/README.md." >&2
  exit 1
fi
if [ -f sky/dashboard/package-lock.json ]; then
  npm --prefix sky/dashboard ci
else
  npm --prefix sky/dashboard install
fi
npm --prefix sky/dashboard run build
if [ ! -f sky/dashboard/out/index.html ]; then
  echo "error: dashboard build did not produce sky/dashboard/out/index.html" >&2
  exit 1
fi

ctx="$(mktemp -d)"; trap 'rm -rf "$ctx"' EXIT
for f in "${files[@]}"; do mkdir -p "$ctx/$(dirname "$f")"; cp "$f" "$ctx/$f"; done
# Ship ONLY the static export (out/) — never node_modules/.next; the server
# serves sky/dashboard/out directly (sky/server/constants.py: DASHBOARD_DIR),
# so the recursive COPY sky/ in the Dockerfile lands it at
# site-packages/sky/dashboard/out.
mkdir -p "$ctx/sky/dashboard"
cp -R sky/dashboard/out "$ctx/sky/dashboard/out"
echo ">> Dashboard bundle added to context: $(du -sh "$ctx/sky/dashboard/out" | cut -f1)"
cp boltz/Dockerfile.overlay "$ctx/Dockerfile"

echo ">> Building ${TAG} (${PLATFORM}, base ${BASE_IMAGE})"
docker build --platform "$PLATFORM" --build-arg "BASE_IMAGE=${BASE_IMAGE}" -t "$TAG" "$ctx"

echo ">> Verifying overlaid modules import + control-loop API + dashboard present"
docker run --rm --platform "$PLATFORM" "$TAG" python -c "
import inspect, os
import sky.serve.controller, sky.serve.replica_managers, sky.serve.load_balancer
from sky.utils import controller_utils
import sky.server.config
from sky.server import constants as server_constants
assert hasattr(controller_utils, 'in_flight_launch_count')
assert 'in_flight' in inspect.signature(controller_utils.can_provision).parameters
index = os.path.join(server_constants.DASHBOARD_DIR, 'index.html')
assert os.path.isfile(index), f'dashboard missing from image: {index}'
print('overlay verify OK')"

if [ "$PUSH" = "true" ]; then echo ">> Pushing ${TAG}"; docker push "$TAG"; fi
echo ">> Done: ${TAG}"
