#!/usr/bin/env bash
# Build, verify, and optionally publish the Boltz SkyPilot control-plane image.
#
# This is intentionally a thin release wrapper around the repository's
# canonical Dockerfile.  The canonical source image already builds the complete
# SkyPilot distribution, dashboard, and cloud dependencies on Python 3.14; this
# wrapper adds immutable release identity and the deployment-only reserved-fill
# reclaim policy.  There is no second runtime base or overlay package path.
#
#   RELEASE_VERSION=1.1.1 PUSH=true \
#     TAG=<ecr>/skypilot-nightly-boltz:1.1.1 ./boltz/build-overlay.sh
#
# Environment:
#   RELEASE_VERSION  image/package version (default: source version)
#   TAG              full image reference (default: local development tag)
#   PUSH             true to push after successful verification (default: false)
#   PLATFORM         control-plane target platform (default: linux/amd64)
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

source_version="$(awk -F"'" '/^__version__ = / {print $2; exit}' sky/__init__.py)"
if [ -z "$source_version" ]; then
  echo "error: unable to read canonical version from sky/__init__.py" >&2
  exit 1
fi
SKYPILOT_VERSION="${RELEASE_VERSION:-$source_version}"
if [[ ! "$SKYPILOT_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: release version must be <major>.<minor>.<patch>, got ${SKYPILOT_VERSION}" >&2
  exit 1
fi

PLATFORM="${PLATFORM:-linux/amd64}"
TAG="${TAG:-skypilot-nightly-boltz:${SKYPILOT_VERSION}-dev}"
PUSH="${PUSH:-false}"
if [ "$PUSH" != "true" ] && [ "$PUSH" != "false" ]; then
  echo "error: PUSH must be true or false, got ${PUSH}" >&2
  exit 1
fi

# The monotonic build identity requires complete history.  Fail before the
# expensive image build when a shallow checkout cannot produce it.
if [ "$(git rev-parse --is-shallow-repository)" = "true" ]; then
  echo "error: a full git history is required to compute the image build number" >&2
  echo "       fetch with --unshallow before running this build." >&2
  exit 1
fi
image_commit="$(git rev-parse HEAD)"
if [ -n "$(git status --porcelain)" ]; then
  image_commit="${image_commit}-dirty"
fi
image_commit_timestamp="$(git show -s --format=%cI HEAD)"
image_commit_count="$(git rev-list --count HEAD)"

# The generic SkyPilot distribution intentionally has no deployment policy.
# Prove that the separately reviewed policy package is committed before the
# canonical image is asked to compose it into the Boltz distribution.
policy_files=()
while IFS= read -r file; do
  [ -n "$file" ] && policy_files+=("$file")
done < <(git ls-tree -r --name-only HEAD -- 'boltz/reserved_fill_reclaim_policy')
if [ "${#policy_files[@]}" -eq 0 ]; then
  echo "error: deployment reclaim-policy package is missing" >&2
  exit 1
fi

echo ">> Building ${TAG} from the canonical Dockerfile (${PLATFORM}, Python 3.14)"
echo ">> Release identity: version ${SKYPILOT_VERSION}, commit ${image_commit}, checked in ${image_commit_timestamp}, build ${image_commit_count}"
docker build \
  --file "$repo_root/Dockerfile" \
  --platform "$PLATFORM" \
  --build-arg "INSTALL_FROM_SOURCE=true" \
  --build-arg "INSTALL_BOLTZ_RECLAIM_POLICY=true" \
  --build-arg "SKYPILOT_EXTRAS=aws,gcp,kubernetes" \
  --build-arg "SKYPILOT_VERSION=${SKYPILOT_VERSION}" \
  --build-arg "SKYPILOT_COMMIT_SHA=${image_commit}" \
  --build-arg "SKYPILOT_COMMIT_TIMESTAMP=${image_commit_timestamp}" \
  --build-arg "SKYPILOT_COMMIT_COUNT=${image_commit_count}" \
  --tag "$TAG" \
  "$repo_root"

echo ">> Verifying Python, provenance, distributions, cloud clients, and dashboard"
docker run --rm --platform "$PLATFORM" \
  -e "EXPECTED_SKYPILOT_VERSION=${SKYPILOT_VERSION}" \
  -e "EXPECTED_SKYPILOT_COMMIT=${image_commit}" \
  -e "EXPECTED_SKYPILOT_COMMIT_TIMESTAMP=${image_commit_timestamp}" \
  -e "EXPECTED_SKYPILOT_BUILD=${image_commit_count}" \
  "$TAG" python -c "
import importlib.metadata
import os
import sys

import boto3
import googleapiclient.discovery
import kubernetes
import skypilot_serve_system_oom_recovery_authorization
import boltz_reserved_fill_reclaim_policy
import sky
import sky.serve.controller
import sky.serve.load_balancer
import sky.serve.replica_managers
import sky.server.config
from sky.server import constants as server_constants
from sky.utils import controller_utils

assert sys.version_info[:2] >= (3, 14), sys.version
assert sky.__version__ == os.environ['EXPECTED_SKYPILOT_VERSION']
assert sky.__commit__ == os.environ['EXPECTED_SKYPILOT_COMMIT']
assert sky.__commit_timestamp__ == os.environ['EXPECTED_SKYPILOT_COMMIT_TIMESTAMP']
assert sky.__build__ == os.environ['EXPECTED_SKYPILOT_BUILD']
assert importlib.metadata.version('skypilot') == sky.__version__
assert importlib.metadata.version(
    'boltz-skypilot-reserved-fill-reclaim-policy') == sky.__version__
assert callable(controller_utils.get_serve_launch_limit)
assert callable(controller_utils.get_serve_termination_limit)
entries = tuple(importlib.metadata.entry_points().select(
    group='skypilot.reserved_fill_reclaim_policy'))
assert len(entries) == 1, entries
assert entries[0].name == 'boltz'
policy = entries[0].load()()
assert policy.policy_identity().policy_revision == (
    'boltz-reserved-fill-reclaim-policy/' +
    boltz_reserved_fill_reclaim_policy.POLICY_REVISION)
index = os.path.join(server_constants.DASHBOARD_DIR, 'index.html')
assert os.path.isfile(index), f'dashboard missing from image: {index}'
print('Boltz production image verification OK')"

test "$(docker image inspect --format \
  '{{ index .Config.Labels "org.opencontainers.image.version" }}' "$TAG")" = \
  "$SKYPILOT_VERSION"
test "$(docker image inspect --format \
  '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$TAG")" = \
  "$image_commit"
test "$(docker image inspect --format \
  '{{ index .Config.Labels "bio.boltz.skypilot.commit-timestamp" }}' "$TAG")" = \
  "$image_commit_timestamp"
test "$(docker image inspect --format \
  '{{ index .Config.Labels "bio.boltz.skypilot.commit-count" }}' "$TAG")" = \
  "$image_commit_count"

if [ "$PUSH" = "true" ]; then
  echo ">> Pushing ${TAG}"
  docker push "$TAG"
fi
echo ">> Done: ${TAG}"
