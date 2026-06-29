#!/usr/bin/env bash
# Build (and optionally push) the boltz overlay image from THIS fork checkout.
# Run from anywhere inside the repo; uses the current HEAD.
#
# Overlays the fork's changed runtime files (git diff <FORK_BASE> HEAD --
# 'sky/**', tests excluded) onto the pinned upstream nightly, PLUS any fork
# sky/ module the base image is missing (a file present at the fork base but
# dropped by the newer nightly that the fork still imports). We never shadow
# base files the diff leaves unchanged — only changed files and base-missing
# modules are copied, so the deployed sky/ stays consistent with the fork.
#
#   PUSH=true TAG=<ecr>/skypilot-nightly-boltz:<ver>-<n> ./boltz/build-overlay.sh
#
# Env (all have sensible defaults):
#   BASE_VER   upstream nightly tag to base on (keep in sync with the platform
#              chart_version, normalized "-dev." -> ".dev"). Default below.
#   BASE_IMAGE override the full base image (default berkeleyskypilot/...:$BASE_VER)
#   FORK_BASE  overlay diff baseline. Default: the fork's divergence point from
#              upstream ("git merge-base HEAD $UPSTREAM_REF"), so it tracks the
#              fork automatically across upstream syncs/rebases. Set to pin one.
#   UPSTREAM_REF  ref used to derive FORK_BASE (default upstream/master). CI adds
#              the skypilot-org remote and fetches it before building.
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
UPSTREAM_REF="${UPSTREAM_REF:-upstream/master}"

# Resolve the overlay baseline: the upstream commit the fork diverges from. Prefer
# an explicit FORK_BASE; otherwise derive it as the merge-base of HEAD and the
# upstream master ref. This tracks the fork across upstream syncs/rebases instead
# of pinning a commit that vanishes when history is rewritten (the old hardcoded
# import sha did exactly that, silently widening the overlay to upstream's delta).
if [ -z "${FORK_BASE:-}" ]; then
  if ! git rev-parse --verify --quiet "${UPSTREAM_REF}^{commit}" >/dev/null; then
    echo "error: cannot resolve '${UPSTREAM_REF}' to derive the overlay baseline." >&2
    echo "       In CI the workflow fetches it; locally run:" >&2
    echo "         git remote add upstream https://github.com/skypilot-org/skypilot.git" >&2
    echo "         git fetch upstream master" >&2
    echo "       or pass an explicit FORK_BASE=<sha>." >&2
    exit 1
  fi
  FORK_BASE="$(git merge-base HEAD "${UPSTREAM_REF}")"
fi
echo ">> Overlay baseline FORK_BASE=$(git rev-parse --short "$FORK_BASE") ($(git log -1 --format=%s "$FORK_BASE"))"

echo ">> Overlay file set (git diff ${FORK_BASE}..HEAD -- 'sky/**'):"
files=()
while IFS= read -r f; do [ -n "$f" ] && files+=("$f"); done < <(
  git diff --name-only "$FORK_BASE" HEAD -- 'sky/**' | grep -vE '(^|/)tests?/')
if [ "${#files[@]}" -eq 0 ]; then echo "  none — aborting" >&2; exit 1; fi
printf '   %s\n' "${files[@]}"

# The diff above is "changed vs the fork base", which deliberately leaves
# unchanged files to the (newer) nightly wheel. But a module that exists at the
# fork base and is still imported by the fork, yet was REMOVED in the newer
# upstream nightly, is in neither set — so it vanishes from the image and the
# server crashes at startup (e.g. sky/jobs/managed_job_refresh_thread.py ->
# ImportError at boot). Detect and additionally carry those base-missing fork
# modules. We add only files the base LACKS, never shadowing base files the diff
# intentionally leaves to the newer wheel.
echo ">> Checking for fork sky/ modules absent from the base image"
base_sky_files="$(docker run --rm "$BASE_IMAGE" python - <<'PY'
import os, sky
root = os.path.dirname(sky.__file__)
parent = os.path.dirname(root)
for dirpath, _dirs, names in os.walk(root):
    for name in names:
        print(os.path.relpath(os.path.join(dirpath, name), parent))
PY
)"
carried=()
while IFS= read -r f; do
  [ -n "$f" ] || continue
  case "$f" in */tests/*|*/test/*) continue ;; esac
  printf '%s\n' "${files[@]}" | grep -qxF -- "$f" && continue  # already in diff set
  grep -qxF -- "$f" <<<"$base_sky_files" && continue           # present in base
  files+=("$f"); carried+=("$f")
done < <(git ls-tree -r --name-only HEAD -- 'sky' | grep -E '\.py$')
if [ "${#carried[@]}" -gt 0 ]; then
  echo ">> Carrying ${#carried[@]} fork module(s) absent from the base image:"
  printf '   + %s\n' "${carried[@]}"
fi

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
