"""Constants for the API servers."""

import os

from sky.skylet import constants
from sky.utils import controller_constants

# pylint: disable=line-too-long
# The SkyPilot API version that the code currently use.
# Bump this version when the API is changed and special compatibility handling
# based on version info is needed.
# For more details and code guidelines, refer to:
# https://docs.skypilot.co/en/latest/developers/CONTRIBUTING.html#backward-compatibility-guidelines
API_VERSION = 90  # Lazy SkyServe version YAML

# The minimum peer API version that the code should still work with.
# Notes (dev):
# - This value is maintained by the CI pipeline, DO NOT EDIT this manually.
# - Compatibility code for versions lower than this can be safely removed.
# Refer to API_VERSION for more details.
MIN_COMPATIBLE_API_VERSION = 24

# The semantic version of the minimum compatible API version.
# Refer to MIN_COMPATIBLE_API_VERSION for more details.
# Note (dev): DO NOT EDIT this constant manually.
MIN_COMPATIBLE_VERSION = '0.11.0'

# The HTTP header name for the API version of the sender.
API_VERSION_HEADER = 'X-SkyPilot-API-Version'

# The HTTP header name for the SkyPilot version of the sender.
VERSION_HEADER = 'X-SkyPilot-Version'

# Minimum client API version required to launch recipes.
MIN_RECIPE_LAUNCH_API_VERSION = 33

# Minimum API version that supports upload API v2.
UPLOAD_API_V2_VERSION = 41

# Minimum server API version required for api_server_access in managed jobs.
MIN_API_ACCESS_API_VERSION = 42

# Minimum API version that supports the SSH redirect first-frame protocol.
MIN_SSH_REDIRECT_PROTOCOL_VERSION = 47

# Minimum API version that supports Sky Batch (sky.batch module).
MIN_BATCH_API_VERSION = 49

# Minimum server API version that supports immutable attempt-scoped Batch
# outputs and winner-only reduction.
MIN_BATCH_ATTEMPT_FENCING_API_VERSION = 57

# Minimum API version that supports bundling cluster credentials with the
# launch response. Lets the CLI skip the follow-up /status round-trip that
# only exists to fetch credentials for SSH config setup.
MIN_LAUNCH_CREDENTIALS_API_VERSION = 50

# Servers >= this version omit the bulky pickled `handle` from each replica
# in serve/pool status responses, shipping pre-computed `infra` /
# `resources_str` / `resources_str_full` strings instead. Older clients are
# still served the full handle on the wire so existing SDK code that reads
# `record['handle']` keeps working.
MIN_LAZY_REPLICA_HANDLE_API_VERSION = 51

# Minimum ReplicaInfo._VERSION that supports Sky Batch workers.
MIN_BATCH_REPLICA_INFO_VERSION = 6

# Minimum server API version that exposes /users/me/workspace and runs the
# server-side launch-path resolver when the client does not specify an
# active workspace. Older servers don't have the endpoint and fall back to
# the literal 'default' workspace, so the client must skip features that
# depend on per-user preferred workspace when talking to such servers.
MIN_PREFERRED_WORKSPACE_API_VERSION = 53

# Minimum server API version that supports filtering cluster status by
# workspace. Servers below this version ignore the request-body field, which
# would otherwise silently return clusters outside the requested workspaces.
MIN_STATUS_WORKSPACE_FILTER_API_VERSION = 63

# Minimum server API version that supports filtering the managed jobs queue by
# submission time (submitted_after / submitted_before, surfaced as the CLI
# --since / --after / --before flags). Older servers silently ignore these
# fields, so the client warns and shows all jobs.
MIN_JOBS_SUBMITTED_AT_FILTER_API_VERSION = 54

# Servers >= this version may report the WAITING request status (a request
# parked off its worker while waiting for a retry/resume condition). Older
# clients don't know the value and would crash parsing it, so the server
# downgrades WAITING to RUNNING on the wire for clients below this version.
MIN_WAITING_STATUS_API_VERSION = 55

# Minimum server API version that exposes the admin-only materialized
# estimated-spend endpoint used by the dashboard.
MIN_ESTIMATED_SPEND_API_VERSION = 57

# Minimum server API version that supports grouped estimated-spend chart and
# table data by job, user, or purchase option.
MIN_ESTIMATED_SPEND_BREAKDOWNS_API_VERSION = 58

# Minimum server API version that supports exact start/end UTC dates on the
# estimated-spend endpoint.
MIN_ESTIMATED_SPEND_DATE_RANGE_API_VERSION = 59

# Minimum server API version that adds durable daily service request volume to
# the estimated-spend response.
MIN_ESTIMATED_SPEND_SERVICE_REQUESTS_API_VERSION = 63

# Minimum server API version that adds the exact non-rejected request subset to
# the estimated-spend service request projection.
MIN_ESTIMATED_SPEND_NON_REJECTED_REQUESTS_API_VERSION = 68

# Minimum server API version exposing the managed image catalog.
MIN_CONTAINER_IMAGES_API_VERSION = 62

# Minimum server API version exposing actor-aware operational events.
MIN_OPERATIONAL_EVENTS_API_VERSION = 64

# Minimum API version with metadata-only SkyServe status projections.
MIN_SERVE_PROGRESSIVE_STATUS_API_VERSION = 65

# Minimum API version whose SkyServe status includes the provider-free,
# durable reserved-fill reconciliation projection.
MIN_SERVE_RESERVED_FILL_RECONCILIATION_STATUS_API_VERSION = 76

# Minimum API version with direct persisted SkyServe dashboard history reads.
MIN_SERVE_DASHBOARD_HISTORY_API_VERSION = 66

# Backward-compatible name introduced with the history route. It refers only
# to the v1b history capability; replica reads have their own later gate.
MIN_SERVE_DASHBOARD_DIRECT_READS_API_VERSION = (
    MIN_SERVE_DASHBOARD_HISTORY_API_VERSION)

# Minimum API version with batched summaries and paginated replica reads.
MIN_SERVE_DASHBOARD_REPLICA_READS_API_VERSION = 67

# Minimum API version with bounded persisted SkyServe pricing reads.
MIN_SERVE_DASHBOARD_PRICING_API_VERSION = 71

# Minimum server API version that exposes GET /api/v1/public/capacity.
MIN_PUBLIC_CAPACITY_API_VERSION = 72

# Minimum API version that scopes persisted request payload access by owner.
MIN_OWNER_SCOPED_REQUEST_ACCESS_API_VERSION = 73

# Minimum API version with the private atomic ordinary-launch binding endpoint.
MIN_ORDINARY_LAUNCH_BINDING_API_VERSION = 74
ORDINARY_LAUNCH_BINDING_PATH = '/internal/serve/ordinary-launch'
MIN_NON_POOL_LAUNCH_BINDING_API_VERSION = 80
MIN_SERVE_DURABLE_DEMAND_API_VERSION = 82
MIN_SERVE_ROUTE_PROJECTION_API_VERSION = 83
MIN_SERVE_ORDERED_CAPACITY_ADMISSION_API_VERSION = 85
# Minimum API version whose durable demand summary preserves the confirmed
# in-flight lower bound and unknown-backend count when exact coverage is
# incomplete.
MIN_SERVE_PARTIAL_IN_FLIGHT_TELEMETRY_API_VERSION = 86
MIN_EXECUTOR_TERMINATION_EVIDENCE_API_VERSION = 87
MIN_SERVE_INCREMENTAL_ROUTE_LEASES_API_VERSION = 88
MIN_SERVE_ZERO_COST_ACTUATION_API_VERSION = 89
MIN_SERVE_LAZY_VERSION_YAML_API_VERSION = 90
NON_POOL_LAUNCH_BINDING_PATH = '/internal/serve/non-pool-launch'

# Minimum API version whose Serve version-history response exposes immutable
# cross-context placement with Kubernetes/Kueue admission. Consumers must also
# require the exact advertised placement_projection_protocol_version; API 79
# advances new writes to protocol 3 with typed worker scratch.
MIN_SERVE_PLACEMENT_PROJECTION_API_VERSION = 77

# Kubernetes node info includes the SkyServe-attributed subset of preemptible
# accelerators. Older clients reject unknown KubernetesNodeInfo fields.
MIN_KUBERNETES_PREEMPTIBLE_SERVICE_BREAKDOWN_API_VERSION = 78

# Kubernetes node info retains all active priority tiers and the priority tier
# of each attributed preemptible SkyServe service.
MIN_KUBERNETES_OPERATIONAL_PRIORITY_BREAKDOWN_API_VERSION = 81
MIN_KUBERNETES_OPERATIONAL_WORKLOAD_BREAKDOWN_API_VERSION = 84

# This exact method/path pair is the only unauthenticated capacity surface.
# Keep the predicate centralized so every authentication middleware applies
# the same boundary.
PUBLIC_CAPACITY_PATH = '/api/v1/public/capacity'


def is_unauthenticated_public_request(method: str, path: str) -> bool:
    """Return whether one request is the exact public capacity read."""
    return method == 'GET' and path == PUBLIC_CAPACITY_PATH


# Minimum server version accepting the private expected-cluster-record UUID on
# controller-originated down requests. Older servers ignore unknown payload
# fields, which would silently discard the teardown fence.
MIN_RESOURCE_ACTION_EXPECTED_CLUSTER_UUID_API_VERSION = 69

# Minimum server API version whose request status payload exposes exact-
# generation ``execution_quiesced_*`` evidence. The legacy
# ``cancel_acknowledged_at`` field continues to mean signal delivery only.
MIN_REQUEST_EXECUTION_QUIESCENCE_API_VERSION = 70

# Minimum server API version that exposes the admin-only, low-cardinality
# operator notification inbox used by the dashboard.
MIN_OPERATOR_NOTIFICATIONS_API_VERSION = 60

# Prefix for API request names.
REQUEST_NAME_PREFIX = 'sky.'
# The memory (GB) that SkyPilot tries to not use to prevent OOM.
MIN_AVAIL_MEM_GB = 2
MIN_AVAIL_MEM_GB_CONSOLIDATION_MODE = 4
# Default encoder/decoder handler name.
DEFAULT_HANDLER_NAME = 'default'
# The path to the API request database.
API_SERVER_REQUEST_DB_PATH = '~/.sky/api_server/requests.db'

# The interval (seconds) for the cluster status to be refreshed in the
# background.
CLUSTER_REFRESH_DAEMON_INTERVAL_SECONDS = 60

# The interval (seconds) for the volume status to be refreshed in the
# background.
VOLUME_REFRESH_DAEMON_INTERVAL_SECONDS = 60

# Environment variable for a file path to the API cookie file.
# Keep in sync with websocket_proxy.py
API_COOKIE_FILE_ENV_VAR = f'{constants.SKYPILOT_ENV_VAR_PREFIX}API_COOKIE_FILE'
# Default file if unset.
# Keep in sync with websocket_proxy.py
API_COOKIE_FILE_DEFAULT_LOCATION = '~/.sky/cookies.txt'

# The path to the dashboard build output
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), '..', 'dashboard',
                             'out')

# The interval (seconds) for the event to be restarted in the background.
DAEMON_RESTART_INTERVAL_SECONDS = 20

# Timeout for CLI authentication sessions (polling-based auth flow).
# Used by both client (polling timeout) and server (session expiration).
AUTH_SESSION_TIMEOUT_SECONDS = 300  # 5 minutes

# Cookie header for stream request id.
STREAM_REQUEST_HEADER = 'X-SkyPilot-Stream-Request-ID'

# Server-owned controller origin carried by nested SDK requests.  The opaque
# capability authenticates the public identity fields in addition to the
# request's normal user/service-account authentication.
CONTROLLER_INSTANCE_ID_HEADER = 'X-SkyPilot-Controller-Instance-ID'
CONTROLLER_GENERATION_HEADER = 'X-SkyPilot-Controller-Generation'
CONTROLLER_ORIGIN_CAPABILITY_HEADER = (
    'X-SkyPilot-Controller-Origin-Capability')
CONTROLLER_INSTANCE_ID_ENV_VAR = 'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID'
CONTROLLER_GENERATION_ENV_VAR = 'SKYPILOT_SERVER_CONTROLLER_GENERATION'
CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR = (
    controller_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR)
CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR = (
    controller_constants.CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR)

# Exact managed-job attempt carried only by controller-internal SDK requests.
# The outer pair remains in the generic controller headers above so existing
# controller admission and authorization code has one canonical outer fence.
MANAGED_JOB_ID_HEADER = 'X-SkyPilot-Managed-Job-ID'
MANAGED_JOB_CONTROLLER_SLOT_ID_HEADER = (
    'X-SkyPilot-Managed-Job-Controller-Slot-ID')
MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_HEADER = (
    'X-SkyPilot-Managed-Job-Controller-Slot-Attempt')
MANAGED_JOB_CONTROLLER_OWNER_MODE_ENV_VAR = (
    controller_constants.MANAGED_JOB_CONTROLLER_OWNER_MODE_ENV_VAR)
MANAGED_JOB_CONTROLLER_INSTANCE_ID_ENV_VAR = (
    controller_constants.MANAGED_JOB_CONTROLLER_INSTANCE_ID_ENV_VAR)
MANAGED_JOB_CONTROLLER_GENERATION_ENV_VAR = (
    controller_constants.MANAGED_JOB_CONTROLLER_GENERATION_ENV_VAR)
MANAGED_JOB_ID_ENV_VAR = controller_constants.MANAGED_JOB_ID_ENV_VAR
MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR = (
    controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR)
MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR = (
    controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR)

# Valid empty values for pickled fields (base64-encoded pickled None)
# base64.b64encode(pickle.dumps(None)).decode('utf-8')
EMPTY_PICKLED_VALUE = 'gAROLg=='

# We do not support setting these in config.yaml because:
# 1. config.yaml can be updated dynamically, but auth middleware does not
#    support hot reload yet.
# 2. If we introduce hot reload for auth middleware, bad config might
#    invalidate all authenticated sessions and thus cannot be rolled back
#    by API users.
# TODO(aylei): we should introduce server.yaml for static server admin config,
# which is more structured than multiple environment variables and can be less
# confusing to users.
OAUTH2_PROXY_BASE_URL_ENV_VAR = 'SKYPILOT_AUTH_OAUTH2_PROXY_BASE_URL'
OAUTH2_PROXY_ENABLED_ENV_VAR = 'SKYPILOT_AUTH_OAUTH2_PROXY_ENABLED'

# The websockets library (used by uvicorn for WebSocket upgrades) defaults to
# MAX_LINE_LENGTH=8192 bytes per header line. Enterprise SSO cookies from
# oauth2proxy (Azure AD, Okta, etc.) commonly exceed 8KB, causing WebSocket
# upgrade requests to be rejected with HTTP 400. Regular HTTP requests (parsed
# by h11 with a 16KB default) are unaffected. These constants raise the limit
# so that WebSocket upgrades succeed with large auth cookies.
# The env vars are read by websockets at import time.
WEBSOCKETS_MAX_HEADER_LINE_LENGTH = '65536'
WEBSOCKETS_MAX_NUM_HEADERS = '256'

# Request ID for the on-boot sky check request.
ON_BOOT_CHECK_REQUEST_ID = 'skypilot-server-on-boot-check'

# Request logs are stored in ~/.sky/api_server/request_logs/ to avoid NFS
# performance issues in Kubernetes deployments where ~/sky_logs/ may be on
# shared storage.
REQUEST_LOG_PATH_PREFIX = '~/.sky/api_server/request_logs'

# Streaming requests (notably ``sky logs --follow``) proxy remote output
# through a local request log. Keep that spool bounded so one verbose job
# cannot exhaust the API server's shared filesystem. A connected client has
# already received the discarded prefix; reconnects retain the latest window.
STREAMING_REQUEST_LOG_MAX_BYTES = 64 * 1024 * 1024  # 64 MiB

# Default maximum size of a daemon log file before rotation (bytes).
# When a daemon log exceeds this threshold, it is backed up to .log.1 and
# then truncated. One backup is kept per daemon.
# Configurable via api_server.daemon_log_max_bytes in ~/.sky/config.yaml.
DEFAULT_DAEMON_LOG_MAX_BYTES = 128 * 1024 * 1024  # 128 MB

# Interval for the server-side heartbeat daemon that sends plugin metrics
# to Loki (e.g., GPU inventory from billing plugin).
SERVER_HEARTBEAT_INTERVAL_SECONDS = 600  # 10 minutes

# Interval for the daemon that sweeps expired managed-job API access tokens
# from the service_account_tokens table. These tokens are normally revoked
# by the jobs controller on completion, but the daemon ensures any tokens
# that leak (e.g., due to controller crash mid-cleanup) are eventually
# removed once their TTL has passed.
EXPIRED_TOKEN_CLEANUP_DAEMON_INTERVAL_SECONDS = 3600  # 1 hour
