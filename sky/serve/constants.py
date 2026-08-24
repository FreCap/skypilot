"""Constants used for SkyServe."""

from sky.utils import controller_constants
from sky.utils import serve_types

CONTROLLER_TEMPLATE = 'sky-serve-controller.yaml.j2'

# Server-owned operational fence used while persisted Serve state is rewritten.
# The ``SKYPILOT_SERVER_`` prefix prevents a client request from overriding the
# value in an executor process (see server/requests/payloads.py).  Pools use the
# managed-jobs controller and are intentionally outside this hold.
SERVE_CONTROLLER_HOLD_ENV_VAR = 'SKYPILOT_SERVER_SERVE_CONTROLLER_HOLD'

SKYSERVE_METADATA_DIR = '~/.sky/serve'

# The filelock for reserving service ports when starting a service. Two
# requirements:
#  (1) Same-pod sky.serve.service subprocesses briefly serialize socket
#      reservation and child spawn. The bound socket then owns exclusivity
#      while controller initialization continues outside the lock. Retaining
#      the lock also keeps rolling coexistence safe with older processes that
#      still use a select-then-bind sequence.
#  (2) The lock must NOT live on a network filesystem. fcntl/flock over NFS
#      requires a working NLM server (rpc.statd/lockd); many K8s NFS PVC
#      setups mount with `local_lock=none` and the server has no lockd, so
#      flock silently becomes a no-op — multiple processes "acquire" the
#      lock simultaneously and race anyway.
PORT_SELECTION_FILE_LOCK_PATH = '~/.sky/skyserve_port_selection.lock'

# Signal file path for controller to handle signals.
SIGNAL_FILE_PATH = '~/.sky/signals/sky_serve_controller_signal_{}'

# Task metadata proving which ephemeral storage/file-mount resources were
# rewritten into one service incarnation's disjoint namespace.
EPHEMERAL_STORAGE_SCOPE_METADATA_KEY = 'sky_serve_ephemeral_storage_scope'

# Internal launch context carried into the API request row. The API executor
# validates this durable owner tuple before a Serve replica launch can enter the
# worker queue, closing the HTTP-acceptance gap during teardown quiescence.
REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY = 'sky_serve_service_name'
REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY = 'sky_serve_service_hash'
REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY = 'sky_serve_service_version'
REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY = 'sky_serve_controller_pid'
REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY = 'sky_serve_controller_ip'
REPLICA_LAUNCH_FENCE_KEYS = (
    REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY,
    REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY,
    REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY,
    REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY,
    REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY,
)

# Server-owned immutable per-version worker placement projections. Caller
# values are discarded and reloaded from the central Serve database.
REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY = 'sky_serve_worker_projections'

# Diagnostic-only metadata carried alongside the existing replica launch
# fence.  The API server uses it to publish the request ID after the request
# and durable queue row have been persisted, before the HTTP acknowledgement
# is returned.  It is deliberately one nested, versioned value so old servers
# ignore it and no field can be mistaken for launch authority.
ORDINARY_LAUNCH_HANDOFF_CONTEXT_KEY = 'sky_serve_ordinary_launch_handoff'
ORDINARY_LAUNCH_HANDOFF_CONTEXT_VERSION = 1

# Closed discriminator for launch profiles that deliberately retain their
# existing request contract after ordinary launches move to durable binding.
# The API queue persists this exact tuple and both queue admission and the
# provider boundary revalidate it against the named ReplicaInfo row.
ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX = 'sky_serve_binding_excluded_'
ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY = (
    f'{ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX}profile')
ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY = (
    f'{ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX}replica_id')
ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY = (
    f'{ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX}replica_record_id')
ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY = (
    f'{ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX}request_id')
ORDINARY_LAUNCH_BINDING_EXCLUDED_GENERATION_KEY = (
    f'{ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX}generation')
ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE = 'persisted-special.v1'
ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE = (
    'system-recovery.v1')
ORDINARY_LAUNCH_BINDING_EXCLUDED_KEYS = (
    ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY,
    ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY,
    ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY,
    ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY,
    ORDINARY_LAUNCH_BINDING_EXCLUDED_GENERATION_KEY,
)

# Protocol-v2 reserved-fill authority carried in the durable API launch row.
# Unlike the underscore-prefixed resources_override fields below, these values
# survive the controller-to-API queue boundary and are revalidated by the
# executor immediately before provisioning.
RESERVED_FILL_LAUNCH_FENCE_PREFIX = 'sky_serve_reserved_fill_'
RESERVED_FILL_LAUNCH_PROTOCOL_VERSION_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}protocol_version')
RESERVED_FILL_LAUNCH_POOL_KEY = f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}pool_key'
RESERVED_FILL_LAUNCH_SERVICE_GENERATION_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}service_generation')
RESERVED_FILL_LAUNCH_SERVICE_VERSION_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}service_version')
RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}physical_cluster_uid')
RESERVED_FILL_LAUNCH_KUBERNETES_CONTEXT_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}kubernetes_context')
RESERVED_FILL_LAUNCH_ACCELERATOR_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}accelerator')
RESERVED_FILL_LAUNCH_ACCELERATOR_COUNT_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}accelerator_count')
RESERVED_FILL_LAUNCH_GATE_GENERATION_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}reconciliation_gate_generation')
RESERVED_FILL_LAUNCH_RECLAIM_FLEET_BUNDLE_SHA256_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}reclaim_fleet_bundle_sha256')
RESERVED_FILL_LAUNCH_RECLAIM_POLICY_REVISION_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}reclaim_policy_revision')
RESERVED_FILL_LAUNCH_RECLAIM_PROVIDER_INVENTORY_SHA256_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}reclaim_provider_inventory_sha256')
RESERVED_FILL_LAUNCH_WORKER_PROJECTION_SHA256_KEY = (
    f'{RESERVED_FILL_LAUNCH_FENCE_PREFIX}worker_projection_sha256')
RESERVED_FILL_LAUNCH_BASE_FENCE_KEYS = (
    RESERVED_FILL_LAUNCH_PROTOCOL_VERSION_KEY,
    RESERVED_FILL_LAUNCH_POOL_KEY,
    RESERVED_FILL_LAUNCH_SERVICE_GENERATION_KEY,
    RESERVED_FILL_LAUNCH_SERVICE_VERSION_KEY,
    RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY,
    RESERVED_FILL_LAUNCH_KUBERNETES_CONTEXT_KEY,
    RESERVED_FILL_LAUNCH_ACCELERATOR_KEY,
    RESERVED_FILL_LAUNCH_ACCELERATOR_COUNT_KEY,
)
RESERVED_FILL_LAUNCH_POLICY_FENCE_KEYS = (
    RESERVED_FILL_LAUNCH_GATE_GENERATION_KEY,
    RESERVED_FILL_LAUNCH_RECLAIM_FLEET_BUNDLE_SHA256_KEY,
    RESERVED_FILL_LAUNCH_RECLAIM_POLICY_REVISION_KEY,
    RESERVED_FILL_LAUNCH_RECLAIM_PROVIDER_INVENTORY_SHA256_KEY,
    RESERVED_FILL_LAUNCH_WORKER_PROJECTION_SHA256_KEY,
)
RESERVED_FILL_LAUNCH_FENCE_KEYS = (RESERVED_FILL_LAUNCH_BASE_FENCE_KEYS +
                                   RESERVED_FILL_LAUNCH_POLICY_FENCE_KEYS)

# Server-only allowlist for the first same-VM system-OOM recovery rollout.
# The value is a versioned JSON document binding an exact service incarnation
# to a safety-profile digest.  It is read only by the API server and is never
# forwarded into a user task's environment.
SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR = (
    'SKYPILOT_INTERNAL_SERVE_SYSTEM_OOM_RECOVERY_PROFILES')
# Recovery-capable code generation requires one exact controller-owned
# contract tuple. Candidates use contract 2 and a closed context that is
# atomically bound to the API server's own ordinary request ID.
SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY = (
    'sky_serve_system_oom_recovery_controller_contract_version')
SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION = 2
SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY = 'sky_serve_system_oom_recovery_profile_id'
SYSTEM_OOM_RECOVERY_AUTHORIZATION_VERSION_KEY = (
    'sky_serve_system_oom_recovery_authorization_version')
SYSTEM_OOM_RECOVERY_AUTHORIZATION_SHA256_KEY = (
    'sky_serve_system_oom_recovery_authorization_sha256')
SYSTEM_OOM_RECOVERY_RUNTIME_PROFILE_VERSION_KEY = (
    'sky_serve_system_oom_recovery_runtime_profile_version')
SYSTEM_OOM_RECOVERY_EXPECTED_RUNTIME_CAPABILITY_KEY = (
    'sky_serve_system_oom_recovery_expected_runtime_capability')
SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY = (
    'sky_serve_system_oom_recovery_replica_id')
SYSTEM_OOM_RECOVERY_LAUNCH_GENERATION_KEY = (
    'sky_serve_system_oom_recovery_launch_generation')
SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY = (
    'sky_serve_system_oom_recovery_launch_nonce')
SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY = (
    'sky_serve_system_oom_recovery_bound_request_id')
SYSTEM_OOM_RECOVERY_WORKSPACE_KEY = ('sky_serve_system_oom_recovery_workspace')
SYSTEM_OOM_RECOVERY_RESOURCE_ENVELOPE_SHA256_KEY = (
    'sky_serve_system_oom_recovery_resource_envelope_sha256')
SYSTEM_OOM_RECOVERY_TASK_SHA256_KEY = (
    'sky_serve_system_oom_recovery_task_sha256')
SYSTEM_OOM_RECOVERY_RUNTIME_IMAGE_DIGEST_KEY = (
    'sky_serve_system_oom_recovery_runtime_image_digest')
SYSTEM_OOM_RECOVERY_OWNED_CONTAINER_SPEC_SHA256_KEY = (
    'sky_serve_system_oom_recovery_owned_container_spec_sha256')
SYSTEM_OOM_RECOVERY_EXECUTION_ENVELOPE_SHA256_KEY = (
    'sky_serve_system_oom_recovery_execution_envelope_sha256')

# The consolidation mode lock ensures that if multiple API servers are running
# at the same time (e.g. during a rolling update), recovery can only happen once
# the previous API server has exited.
POOL_CONSOLIDATION_MODE_LOCK_ID = '~/.sky/pool_consolidation_mode_lock'
SERVE_CONSOLIDATION_MODE_LOCK_ID = '~/.sky/serve_consolidation_mode_lock'

# Time to wait in seconds for controller to setup, this involves the time to run
# cloud dependencies installation.
CONTROLLER_SETUP_TIMEOUT_SECONDS = 300
# Time to wait for controller + external-LB registration. The LB image may need
# a cold pull and must pass its sync-backed readiness probe before ``serve up``
# can truthfully publish the endpoint.
SERVICE_REGISTER_TIMEOUT_SECONDS = 420
LB_DEPLOYMENT_READY_TIMEOUT_SECONDS = 120
LB_SERVICE_ENDPOINT_READY_TIMEOUT_SECONDS = 240
LB_SERVICE_ENDPOINT_READY_POLL_SECONDS = 1
LB_DEPLOYMENT_READY_POLL_SECONDS = 1

# Legacy env var holding one controller-admin bearer token. New deployments
# should use the independent file-backed rings below. It deliberately is NOT a
# fallback for LB sync: letting one legacy credential authenticate both domains
# would allow a compromised LB to invoke destructive controller endpoints.
CONTROLLER_AUTH_TOKEN_ENV_VAR = 'SKYPILOT_SERVE_CONTROLLER_AUTH_TOKEN'

# Newline-delimited token-ring files. The first line is the primary credential;
# subsequent lines are overlap credentials accepted during a rotation. Files
# are read on every request/sync so projected Kubernetes Secret updates take
# effect without restarting the controller or LB.
LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR = ('SKYPILOT_SERVE_LB_SYNC_AUTH_TOKENS_FILE')
CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR = (
    'SKYPILOT_SERVE_CONTROLLER_ADMIN_AUTH_TOKENS_FILE')
LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR = (
    'SKYPILOT_SERVE_LB_DATA_PLANE_AUTH_ENABLED')

# A load balancer stamps the durable service incarnation it was created for.
# The stable API-server proxy rejects stale same-name LBs before forwarding.
SERVICE_HASH_HEADER = 'X-SkyPilot-Serve-Service-Hash'

# Every controller endpoint lives on an ephemeral per-service port. Callers
# stamp a fingerprint of the exact authoritative (service hash, parent PID,
# pod IP, port) tuple and the child verifies it before handling the request, so
# a recycled socket can never cross-wire a request into another controller.
CONTROLLER_OWNER_HEADER = 'X-SkyPilot-Serve-Controller-Owner'

# Env var holding a shared bearer token that guards INBOUND inference requests
# to the external load balancer (data-plane auth), held by the inference
# client. Kept DISTINCT from CONTROLLER_AUTH_TOKEN_ENV_VAR (control-plane) on
# purpose: sharing them would let an inference client reach the controller's
# destructive endpoints. The LB's readiness route (LB_HEALTH_ENDPOINT_PATH)
# stays open so k8s can probe it. External mode requires the file-backed ring;
# this legacy value is only the single-token compatibility fallback.
LB_AUTH_TOKEN_ENV_VAR = 'SKYPILOT_SERVE_LB_AUTH_TOKEN'
LB_AUTH_TOKENS_FILE_ENV_VAR = 'SKYPILOT_SERVE_LB_AUTH_TOKENS_FILE'

# Dedicated inference-client credential header consumed by the external LB.
# This MUST remain separate from Authorization: model servers commonly use the
# standard header for their own application auth. The LB removes this header
# before invoking any downstream route, so its edge credential never reaches a
# replica.
LB_AUTHORIZATION_HEADER = 'X-SkyPilot-Serve-Authorization'
LB_AUTHORIZATION_HEADER_BYTES = LB_AUTHORIZATION_HEADER.lower().encode('ascii')

# Helm renders this explicit platform-capability signal. It is the single
# source of truth for in-cluster API/controller/LB processes, avoiding a split
# between the Helm values that create RBAC/Secret projections and a separate
# persisted SkyPilot config flag.
EXTERNAL_LB_ENABLED_ENV_VAR = serve_types.EXTERNAL_LB_ENABLED_ENV_VAR
LB_HA_RBAC_READY_ENV_VAR = 'SKYPILOT_SERVE_LB_HA_RBAC_READY'

# HTTPS termination for the external LB Service, rendered by Helm for the same
# reason as the capability flag above. `_build_service_dict` runs both in the
# API server (service creation) and in the controller's periodic re-ensure
# loop; if those two processes disagreed about TLS the Service would oscillate
# every re-ensure interval. Reading one Helm-injected environment, inherited by
# consolidated controller children, keeps them in lockstep. A persisted or
# per-service config would reintroduce the frozen-snapshot split-brain.
#
# Both the certificate and the suffix are required together: a certificate with
# no hostname yields a TLS listener nobody can validate, and a hostname with no
# certificate yields a name that only answers in plaintext.
EXTERNAL_LB_HTTPS_CERT_ARN_ENV_VAR = 'SKYPILOT_SERVE_EXTERNAL_HTTPS_CERT_ARN'
EXTERNAL_LB_HTTPS_DNS_SUFFIX_ENV_VAR = (
    'SKYPILOT_SERVE_EXTERNAL_HTTPS_DNS_SUFFIX')
EXTERNAL_LB_HTTPS_SSL_POLICY_ENV_VAR = (
    'SKYPILOT_SERVE_EXTERNAL_HTTPS_SSL_POLICY')
# Set to 'true' once every consumer speaks HTTPS, to drop the plaintext
# listener. Kept separate from the settings above so enabling TLS and enforcing
# it are independently revertible steps.
EXTERNAL_LB_HTTPS_ONLY_ENV_VAR = 'SKYPILOT_SERVE_EXTERNAL_HTTPS_ONLY'

# TLS 1.2 floor with TLS 1.3 available. The AWS default (2016-08) still permits
# TLS 1.0/1.1.
DEFAULT_EXTERNAL_LB_SSL_POLICY = 'ELBSecurityPolicy-TLS13-1-2-2021-06'

# Service port for the TLS listener. The NLB terminates TLS here and forwards
# to the load balancer pod's existing plaintext port, so the LB process itself
# is unchanged.
EXTERNAL_LB_HTTPS_PORT = 443
EXTERNAL_LB_HTTPS_PORT_NAME = 'https'
EXTERNAL_LB_HTTP_PORT_NAME = 'http'

# Re-encrypt the load balancer's own hop. With HTTPS_ONLY the NLB stops
# forwarding cleartext to the pod and speaks TLS to it, so no serve traffic
# crosses a machine boundary in the clear.
#
# This activates only with HTTPS_ONLY, and cannot be split: the annotation is
# per-Service, not per-port, so during the dual-listen migration window the pod
# must stay plaintext or the 30001 listener would forward cleartext into a
# TLS-only socket.
#
# The pod mints its own throwaway certificate at startup. An NLB TLS target
# group does not validate the backend certificate, and kubelet does not
# validate HTTPS probe certificates, so this needs no distribution, no
# rotation, and no configuration -- the encryption is what is wanted here, and
# the peer is one pod away on the same VPC.
AWS_LB_BACKEND_PROTOCOL_ANNOTATION = ('service.beta.kubernetes.io/'
                                      'aws-load-balancer-backend-protocol')
AWS_LB_BACKEND_PROTOCOL_SSL = 'ssl'

# Encryption for the load-balancer-to-replica hop. Unlike the listener settings
# above this changes how the LB dials replicas, so it is read in the controller
# (which mints and injects the material) and in the LB (which pins it).
#
#   unset/'off'  - plaintext http, today's behaviour.
#   'pinned'     - https, verified against the service's own certificate. The
#                  replica must run a TLS proxy fed the injected key.
#   'unverified' - https with verification disabled. Defeats passive
#                  interception only; an active man-in-the-middle still wins.
#                  For deployments that cannot distribute the key material.
REPLICA_TLS_MODE_ENV_VAR = 'SKYPILOT_SERVE_REPLICA_TLS_MODE'
REPLICA_TLS_MODE_OFF = 'off'
REPLICA_TLS_MODE_PINNED = 'pinned'
REPLICA_TLS_MODE_UNVERIFIED = 'unverified'
REPLICA_TLS_MODES = (REPLICA_TLS_MODE_OFF, REPLICA_TLS_MODE_PINNED,
                     REPLICA_TLS_MODE_UNVERIFIED)

# The certificate is public: it is injected into the replica task so the TLS
# proxy can present it, and into the LB pod so it can pin it. Same value, two
# consumers.
REPLICA_TLS_CERT_ENV_VAR = 'SKYPILOT_SERVE_REPLICA_TLS_CERT'
# The private key goes only to replicas, and only ever as a task SECRET, so it
# is redacted from task YAML dumps and logs rather than sitting in plain envs.
REPLICA_TLS_KEY_SECRET_ENV_VAR = 'SKYPILOT_SERVE_REPLICA_TLS_KEY'

# Downward-API-injected UID of the external LB pod. Unlike a process-local
# UUID, this survives controller restarts as the durable LB incarnation key.
LB_POD_UID_ENV_VAR = 'SKYPILOT_SERVE_LB_POD_UID'
LB_SLOT_ENV_VAR = 'SKYPILOT_SERVE_LB_SLOT'
LB_IMAGE_DIGEST_ENV_VAR = 'SKYPILOT_SERVE_LB_IMAGE_DIGEST'
LB_RESOURCES_ENV_VAR = 'SKYPILOT_SERVE_LB_RESOURCES_JSON'
LB_PRIORITY_CLASS_NAME_ENV_VAR = 'SKYPILOT_SERVE_LB_PRIORITY_CLASS_NAME'
EXTERNAL_LB_SERVICE_ANNOTATIONS_ENV_VAR = (
    'SKYPILOT_SERVE_EXTERNAL_LB_SERVICE_ANNOTATIONS_JSON')

# The load balancer's readiness route; exempt from inbound bearer auth so the
# k8s readinessProbe (and any LB-level health check) can reach it. Kept here so
# the route registration and the auth middleware share one source of truth.
LB_HEALTH_ENDPOINT_PATH = '/_lb/health'
LB_LIVENESS_ENDPOINT_PATH = '/_lb/liveness'
LB_PREDICTION_COMPLETION_ENDPOINT_PATH = '/_lb/prediction-completed'
LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH = '/_lb/async-request-receipt'

# Hard cap on the number of request timestamps the LB retains between successful
# controller syncs. The batch is retained (not dropped) across a failed sync so
# the autoscaler does not lose load signal, but a PERSISTENT sync failure must
# not grow it without bound (one float per proxied request). The most recent
# CAP samples are kept -- ample for QPS autoscaling even at the top of the
# supported RPS range across several sync intervals.
LB_REQUEST_TIMESTAMP_CAP = 100_000
# Deduplicated offered-arrival windows used by logical concurrency scaling.
# Tracking saturates at the timestamp cap instead of evicting current-window
# entries and under-reporting the heaviest load.
LB_OFFERED_ARRIVAL_WINDOW_SECONDS = 300
LB_OFFERED_ARRIVAL_CAP = LB_REQUEST_TIMESTAMP_CAP
# The load balancer retains exact per-minute arrival counters for the recent
# dashboard window. One extra bucket covers the partially elapsed boundary
# minute without allowing a controller outage to grow LB memory unbounded.
LB_REQUEST_HISTORY_BUCKET_SECONDS = 60
LB_REQUEST_HISTORY_WINDOW_SECONDS = 60 * 60
LB_REQUEST_HISTORY_MAX_BUCKETS = (
    LB_REQUEST_HISTORY_WINDOW_SECONDS // LB_REQUEST_HISTORY_BUCKET_SECONDS + 1)
# Legacy full-HTTP completion histogram retained for migration 022 and rolling
# rollback compatibility. New load balancers do not emit this history.
LB_RESPONSE_TIME_HISTOGRAM_VERSION = 1
LB_RESPONSE_TIME_BUCKET_UPPER_BOUNDS_SECONDS = (
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    1200.0,
    1800.0,
    3600.0,
)
LB_RESPONSE_TIME_STATUS_CLASSES = ('1xx', '2xx', '3xx', '4xx', '5xx')
LB_RESPONSE_TIME_BUCKET_COUNT = (
    len(LB_RESPONSE_TIME_BUCKET_UPPER_BOUNDS_SECONDS) + 1)

# Replica prediction-time histogram. Bounds are deliberately fixed and coarse:
# the dashboard needs minute-level distributions, not exact per-request
# durations. The final implicit bucket contains values above one hour. Changing
# these values requires a new histogram version because stored arrays are
# interpreted by index.
LB_PREDICTION_TIME_HISTOGRAM_VERSION = 1
LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS = (
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    1200.0,
    1800.0,
    3600.0,
)
LB_PREDICTION_TIME_OUTCOMES = ('succeeded', 'failed')
LB_PREDICTION_TIME_BUCKET_COUNT = (
    len(LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS) + 1)
# Header-free async protocol actions use small JSON envelopes. Bound action
# detection so observability never parses a model-sized input on the LB event
# loop. Platform-held async submissions are identified by their stable job
# header and do not inspect the body at all.
LB_ASYNC_ACTION_BODY_MAX_BYTES = 64 * 1024
# Terminal async status bodies are small JSON objects. Forward larger bodies
# unchanged, but do not retain them for observability parsing.
LB_ASYNC_STATUS_BODY_MAX_BYTES = 64 * 1024
# Completion callbacks carry only request identity, terminal outcome, and model
# time. Keep their independently consumed request body much smaller than a
# model input so an authenticated observability caller cannot make the LB retain
# an unbounded payload.
LB_PREDICTION_COMPLETION_BODY_MAX_BYTES = 16 * 1024
# Bound process-local terminal request deduplication independently from request
# rate and the model runtime's own completed-job cache.
LB_ASYNC_PREDICTION_DEDUP_CAP = 100_000
# The dedup cap bounds how many ids are retained, not how large they are, so
# the id length has to be bounded too: without this the body cap above is the
# only ceiling and one caller chooses the retained size (cap * 16 KiB is far
# past the load balancer memory limit). Real ids are UUID-shaped; this is a
# generous ceiling that keeps worst-case retention within a few tens of MiB.
LB_ASYNC_PREDICTION_REQUEST_ID_MAX_CHARS = 256
# Explicit opt-in contract for the PostgreSQL dispatch/replay ledger.  Older
# callers omit these headers and retain the aggregate-only compatibility path;
# once a caller opts in, every missing receipt fails closed.
LB_ASYNC_LEDGER_PROTOCOL_VERSION = 1
LB_ASYNC_LEDGER_PROTOCOL_HEADER = 'X-SkyServe-Async-Ledger-Protocol'
LB_ASYNC_SERVICE_INCARNATION_HEADER = 'X-SkyServe-Service-Incarnation'
LB_ASYNC_INTENT_SHA256_HEADER = 'X-SkyServe-Async-Intent-Sha256'
# The durable platform job ID and the prediction execution ID are different
# identities.  The former deduplicates autoscaling pressure; the latter is the
# exact immutable request key returned by the worker and stored in the ledger.
LB_ASYNC_EXECUTION_REQUEST_ID_HEADER = 'X-SkyServe-Execution-Request-Id'
LB_ASYNC_ATTEMPT_ID_HEADER = 'X-SkyServe-Async-Attempt-Id'
LB_ASYNC_ATTEMPT_NO_HEADER = 'X-SkyServe-Async-Attempt-No'
LB_ASYNC_LEDGER_REVISION_HEADER = 'X-SkyServe-Async-Ledger-Revision'
LB_ASYNC_LEDGER_STATE_HEADER = 'X-SkyServe-Async-Ledger-State'

# [boltz fork] Compatibility time budget for controllers predating the
# commit-then-reconcile update protocol. Their handler can wait on the
# replica-manager lock behind a fleet-wide probe round for minutes. New
# controllers acknowledge the durable commit before taking that lock, but
# clients keep this budget during mixed-version rollouts.
UPDATE_SERVICE_TIMEOUT_SECONDS = 600
# A raw controller-config stage may still belong to an API request that is
# waiting for a controller response and then serializing an ambiguity cleanup.
# The controller GC therefore waits for both full request budgets before it
# considers a NULL-yaml stage orphaned.
ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS = 2 * UPDATE_SERVICE_TIMEOUT_SECONDS
ORPHANED_CONFIG_STAGE_SWEEP_INTERVAL_SECONDS = 60

# Replica termination waits for the fleet-wide replica-manager lock before it
# durably schedules teardown. Large-fleet recovery and probe rounds can hold
# that lock for minutes, so this destructive operation needs its own explicit
# acceptance budget instead of the generic 10-second controller/RPC timeout.
TERMINATE_REPLICA_TIMEOUT_SECONDS = 600

# The time interval in seconds for load balancer to sync with controller. Every
# time the load balancer syncs with controller, it will update all available
# replica ips for each service, also send the number of requests in last query
# interval.
LB_CONTROLLER_SYNC_INTERVAL_SECONDS = 20
LB_ROLE_HEARTBEAT_INTERVAL_SECONDS = 2
# A STABLE role snapshot is read-only but may be shared by both LB slots. Bound
# the shared provider task from its creation time so a hung Kubernetes call
# cannot make every later heartbeat join the same stale in-flight work. This is
# also the transport deadline for each Kubernetes read in that snapshot. Three
# seconds leaves retry and proxy/DB headroom inside both report freshness (6s)
# and the external LB client timeout (8s).
LB_ROLE_SNAPSHOT_TIMEOUT_SECONDS = 3
# The stable API proxy performs owner reads before and after the controller
# request. Production p99 is about 5.6s even when the controller is healthy,
# so a 5s client budget creates false heartbeat failures. Keep 2s of measured
# headroom without weakening the independent 6s freshness gate below.
LB_ROLE_HEARTBEAT_TIMEOUT_SECONDS = 8
LB_ROLE_REPORT_MAX_AGE_SECONDS = 3 * LB_ROLE_HEARTBEAT_INTERVAL_SECONDS
LB_PROMOTION_OCCUPANCY_MAX_AGE_SECONDS = 15
LB_DEMAND_HANDOFF_SECONDS = 60

# Keep the external LB client, stable API-server proxy, and controller child
# routes on one shared contract. A route added to only one layer makes the
# control channel fail at runtime even when each layer's unit tests pass.
LB_CONTROLLER_SYNC_PATH = '/controller/load_balancer_sync'
# Controller-independent, PostgreSQL-backed demand reporting.  Unlike the
# routes above, this path terminates at the stable API server and must never be
# proxied to the per-service controller process.
LB_DEMAND_REPORT_PATH = '/demand'
# Controller-independent, PostgreSQL-backed async dispatch receipts.  This
# terminates at the stable API server and is authenticated by the LB-sync ring.
LB_ASYNC_REQUEST_LEDGER_PATH = '/async-request-ledger'
LB_ASYNC_REQUEST_LEDGER_MAX_BYTES = 16 * 1024
LB_ASYNC_REQUEST_LEDGER_TIMEOUT_SECONDS = 10
# Bound one active load balancer below the stable API's PostgreSQL worker
# concurrency.  Read-only reconciliation gets half the budget so a lookup burst
# cannot starve bind and terminal receipts, which are correctness-critical
# writes on the provider dispatch path.
LB_ASYNC_REQUEST_LEDGER_MAX_CONCURRENCY = 16
LB_ASYNC_REQUEST_LEDGER_MAX_LOOKUP_CONCURRENCY = 8
LB_DEMAND_REPORT_PROTOCOL_VERSION = 2
LB_DEMAND_REPORT_MIN_PROTOCOL_VERSION = 1
LB_DEMAND_REPORT_INTERVAL_SECONDS = 5
LB_DEMAND_REPORT_TIMEOUT_SECONDS = 10
LB_DEMAND_REPORT_MAX_CLOCK_SKEW_SECONDS = 30
# A report is usable for a little over two missed intervals.  Expiry is minted
# from the PostgreSQL clock on receipt; reporter wall clocks are diagnostic
# only and cannot extend launch authority.
LB_DEMAND_REPORT_TTL_SECONDS = 15
CAPACITY_PLAN_TTL_SECONDS = 15
LB_DEMAND_REPORT_RETENTION_SECONDS = 60 * 60
LB_DEMAND_REPORT_MAX_REPORTERS = 32
LB_DEMAND_REPORT_MAX_BYTES = 512 * 1024
LB_DEMAND_WINDOW_BUCKET_SECONDS = 5
# Once the direct feed is fresh, controller status enrichment is optional for
# request visibility.  Keep that enrichment bounded so a wedged controller
# cannot make the durable request card disappear behind the client timeout.
DURABLE_DEMAND_CONTROLLER_STATUS_TIMEOUT_SECONDS = (1.0, 2.0)
LB_CONTROLLER_ROLE_PATH = '/controller/load_balancer_role'
LB_CONTROLLER_HISTORY_SYNC_PATH = (
    '/controller/load_balancer_request_history_sync')
LB_CONTROLLER_SYSTEM_RECOVERY_LEASE_PATH = (
    '/controller/system_recovery_route_lease')
LB_ROLE_PROXY_OBSERVABILITY_HEADER = ('X-SkyServe-LB-Role-Proxy-Observability')
LB_ROLE_CONTROLLER_OWNER_VERIFIED_HEADER = (
    'X-SkyServe-LB-Role-Controller-Owner-Verified')

# A recovery-capable backend can bind the same route after Ray has killed its
# first process.  The heavyweight 20-second controller sync deliberately keeps
# stale ordinary routes during controller outages, so capable routes carry an
# independent, short-lived marker/heartbeat lease.  These values are a closed
# correctness contract with the driver's replay-quiescence fence; do not make
# them service-configurable.
SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY = 'system_recovery_route_lease'
SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION = 'v1'
SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY = 'system_recovery_replica_id'
SYSTEM_RECOVERY_ROUTE_TOKEN_KEY = 'system_recovery_route_token'
# A coherent heavyweight snapshot uses this closed sentinel to revoke an
# ambiguous transport URL (duplicate normalized rows or a capable row without
# an exact marker).  It is a fence, never a routable marker.
SYSTEM_RECOVERY_ROUTE_FENCE_KEY = 'system_recovery_route_fence'
SYSTEM_RECOVERY_ROUTE_FENCE_VERSION = 'v1'
SYSTEM_RECOVERY_ROUTE_LEASE_PROTOCOL_VERSION = 1
SYSTEM_RECOVERY_ROUTE_PROBE_INTERVAL_SECONDS = 5
SYSTEM_RECOVERY_ROUTE_PROBE_TIMEOUT_SECONDS = 15
SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS = 60
SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS = 1000
SYSTEM_RECOVERY_MAX_ELIGIBLE_PROBE_INTERVAL_SECONDS = 10
SYSTEM_RECOVERY_MAX_ELIGIBLE_READINESS_TIMEOUT_SECONDS = 15
LB_SYSTEM_RECOVERY_LEASE_HEARTBEAT_INTERVAL_SECONDS = 2
LB_SYSTEM_RECOVERY_LEASE_HEARTBEAT_TIMEOUT_SECONDS = 10
# Leave one second inside the LB's total timeout for stable-proxy owner reads
# and response forwarding.
LB_SYSTEM_RECOVERY_LEASE_PROXY_TIMEOUT_SECONDS = 9

# [boltz fork] The timeout in seconds for the load balancer to sync with the
# controller (raised from the previous inline 5s). A cold 215-replica routing
# snapshot measured 14s even after batching cluster-record reads; endpoint
# resolution scales with the READY fleet and is expected to take ~33s at the
# supported 500-replica ceiling. Keep a bounded 60s outer budget so a new LB
# can become ready during a fleet-scale rollout without masking real hangs.
LB_CONTROLLER_SYNC_TIMEOUT_SECONDS = 60
# The API-service proxy must finish before the LB's outer timeout. Leave a
# five-second budget for its owner reads and response forwarding.
LB_CONTROLLER_PROXY_TIMEOUT_SECONDS = 55

# Lightweight controller-child supervision endpoint.  Do not use
# /autoscaler/info for liveness: serializing a large fleet is legitimate work
# and can exceed the parent's tight health-check timeout during launch storms.
CONTROLLER_HEALTH_ENDPOINT_PATH = '/controller/health'
CONTROLLER_PLACEMENT_ENDPOINT_PATH = '/controller/placement'
# Placement state is resident controller observability, not provider
# inventory.  Page it independently from the PostgreSQL placement-history
# page so a large fallback catalog cannot monopolize the API worker or cross
# the controller transport deadline.
PLACEMENT_STATE_PAGINATION_VERSION = 1
PLACEMENT_STATE_DEFAULT_PAGE_SIZE = 100
PLACEMENT_STATE_MAX_PAGE_SIZE = 100
PLACEMENT_STATE_MAX_OFFSET = 100_000
CONTROLLER_UPDATE_CAPABILITIES_ENDPOINT_PATH = (
    '/controller/update_capabilities')
CONTROLLER_CONFIG_UPDATE_ENDPOINT_PATH = (
    '/controller/update_service_with_config')
CONTROLLER_CONFIG_CLEANUP_ENDPOINT_PATH = (
    '/controller/cleanup_staged_update_config')
CONTROLLER_ORDINARY_LAUNCH_BINDING_ENDPOINT_PATH = (
    '/controller/internal/ordinary_launch_binding')
CONTROLLER_DEMAND_SOURCE_ENDPOINT_PATH = ('/controller/internal/demand_source')
CONTROLLER_ZERO_COST_ACTUATION_ENDPOINT_PATH = (
    '/controller/internal/zero_cost_actuation')
CONTROLLER_CAPACITY_AUTHORITY_ENDPOINT_PATH = (
    '/controller/internal/capacity_authority')
SERVE_UPDATE_CONFIG_SNAPSHOT_PROTOCOL_VERSION = 1
VERSIONED_HA_CONFIG_RECOVERY_MARKER = (
    '# SKY_SERVE_VERSIONED_CONFIG_RECOVERY_V1')
# One invocation-local JSON fence inserted by the HA leader immediately before
# it spawns `_start`. The child consumes and removes it from its environment so
# descendants cannot accidentally reuse another recovery attempt's authority.
HA_RECOVERY_OWNER_FENCE_ENV_VAR = 'SKYPILOT_SERVE_HA_RECOVERY_OWNER_FENCE'
# A fleet-scale readiness sweep can briefly starve the controller event loop
# even though the constant-time health handler is healthy.  Keep local connect
# failure detection tight, but allow the lightweight response enough time to
# run before the parent counts a liveness miss.  Three consecutive misses are
# still required before a child is replaced.
CONTROLLER_HEALTH_READ_TIMEOUT_SECONDS = 5

# [boltz fork] Cadence of the LB's per-replica async-occupancy probe (the
# `async_capacity` action). The HTTP-envelope in-flight accounting reads ~0
# for fast-ack async workloads while replicas crunch hour-long jobs, so the
# LB asks each ready replica for its true running-job count and uses it to
# deprioritize busy replicas in routing and to report real free slots on
# /_lb/capacity. Overridable via SKYPILOT_LB_OCCUPANCY_PROBE_INTERVAL_SECONDS
# (<= 0 disables the probe entirely — accounting falls back to envelope-only).
LB_OCCUPANCY_PROBE_INTERVAL_SECONDS = 10
# A capacity consumer must not interpret an old occupancy snapshot as current
# async work. Keep this longer than one ordinary probe round so a single slow
# replica does not make the aggregate flap back to the replica-level fallback.
LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS = 30
# How long an occupancy-capable url that left the ready set is retained
# (and kept probed / reported as occupancy-unknown) without a successful
# occupancy answer. Off-ready probe misses are ambiguous -- torn down vs
# transiently unreachable with async work still running -- so retention
# errs long: a retirement drain blocked on 'unknown' is still bounded by
# its own graceful_drain_seconds deadline, while pruning early would let
# it read the replica as idle and kill live async work. Also the upper
# bound for the service-spec `graceful_drain_seconds` (a drain longer
# than the retention would lose the unknown protection partway through).
# 7200 so hour-scale async jobs fit under the cap with margin: a job
# admitted the instant retirement starts runs its full duration into the
# drain, so a fleet with 3600s jobs needs a cap strictly above 3600.
# Cost of the longer retention is only probing/retaining non-answering
# off-ready urls for up to 2h -- entries answering 'torn down' or probed
# successfully resolve well before that.
LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS = 7200
LB_OCCUPANCY_PROBE_INTERVAL_ENV_VAR = (
    'SKYPILOT_LB_OCCUPANCY_PROBE_INTERVAL_SECONDS')

# [boltz fork] Per-replica timeout for one occupancy probe request. The
# action is answered by the replica's HTTP handler (the crunching happens in
# a background thread), so a healthy replica responds in milliseconds; a
# probe that needs seconds is indistinguishable from a dead pod and is
# treated as "occupancy unknown" (never as busy).
LB_OCCUPANCY_PROBE_TIMEOUT_SECONDS = 2

# The maximum retry times for load balancer for each request. After changing to
# proxy implementation, we do retry for failed requests.
# TODO(tian): Expose this option to users in yaml file.
LB_MAX_RETRY = 3

# Retry-After advertised on LB-generated 503s (no ready replicas, or
# every ready replica already shed this request). The client retry
# layer's backoff is the right waiting room: the LB's ready set only
# changes on the controller sync cadence, so holding the connection
# through in-LB sleeps rarely helps.
LB_503_RETRY_AFTER_SECONDS = 10

# Default first-retry backoff for the LB proxy retry loop
# (exponential with jitter after that). Service-overridable via
# load_balancer.retry_initial_backoff_seconds.
LB_RETRY_INITIAL_BACKOFF_SECONDS = 1

# Opt-in bounded request queue defaults. The queue is sized dynamically as
# max(min_size, ready_replicas * size_per_replica), then clipped to max_size.
# max_size is an absolute task-count bound; max_request_body_bytes bounds the
# largest payload a queued/retried request may retain, so the load balancer's
# memory exposure stays finite under overload.
LB_REQUEST_QUEUE_MIN_SIZE = 10
LB_REQUEST_QUEUE_SIZE_PER_REPLICA = 3
LB_REQUEST_QUEUE_MAX_SIZE = 1000
LB_REQUEST_QUEUE_CONCURRENCY_PER_REPLICA = 1
LB_REQUEST_QUEUE_MAX_CONCURRENCY = 32
LB_REQUEST_QUEUE_TIMEOUT_SECONDS = 120
LB_REQUEST_QUEUE_MAX_BODY_BYTES = 1 * 1024 * 1024
# Hard configuration ceilings, calibrated below the external LB's default
# 512Mi memory limit. Admitted requests are bounded by max_concurrency times
# max_request_body_bytes. Bodies read before admission share a separate runtime
# budget based on their actual sizes, preserving large queues of small requests
# without allowing worst-case payloads to exhaust the process.
# 10,000 supports the shared-fleet policy of ten waiting requests per machine
# at the 1,000-replica service ceiling. Actual queued body bytes remain capped
# independently by LB_REQUEST_QUEUE_WAITING_BODY_MEMORY_BUDGET_BYTES.
LB_REQUEST_QUEUE_MAX_SIZE_LIMIT = 10000
LB_REQUEST_QUEUE_MAX_CONCURRENCY_LIMIT = 128
LB_REQUEST_QUEUE_MAX_BODY_BYTES_LIMIT = 16 * 1024 * 1024
LB_REQUEST_QUEUE_BODY_MEMORY_BUDGET_BYTES = 128 * 1024 * 1024
LB_REQUEST_QUEUE_WAITING_BODY_MEMORY_BUDGET_BYTES = 128 * 1024 * 1024

# The timeout in seconds for load balancer to wait for a response from replica.
# Large LLMs like Llama2-70b is able to process the request within ~30 seconds.
# We set the timeout to 120s to be safe. For reference, FastChat uses 100s:
# https://github.com/lm-sys/FastChat/blob/f2e6ca964af7ad0585cadcf16ab98e57297e2133/fastchat/constants.py#L39 # pylint: disable=line-too-long
DEFAULT_LB_STREAM_TIMEOUT = 120

# Extra margin past the stream timeout before a pruned replica's drained
# httpx client is force-closed even with requests still counted in flight
# (guards against a stuck counter leaking connections forever).
LB_DRAIN_CLOSE_GRACE_SECONDS = 60

# Connect timeout for the LB -> replica hop, independent of the stream
# (read) timeout above: connections to a preempted-but-still-routed
# replica must fail fast into the retry loop even when the stream
# timeout is sized for hour-long synchronous predictions.
LB_CONNECT_TIMEOUT_SECONDS = 10
# Marked recovery routes have a bounded connection-pool wait as well as the
# bounded connect above.  The driver's 83-second fence includes both budgets.
LB_SYSTEM_RECOVERY_POOL_TIMEOUT_SECONDS = 10

# Passive LB-side replica eviction, for the window where the controller is
# paused (e.g. during a control-plane roll) and cannot update the ready set.
# After this many CONSECUTIVE dead-connection failures (refused/reset -- NOT
# connect timeouts, which indicate a merely-saturated but healthy replica) the
# LB quarantines a replica: removed from routing and kept out even if the
# controller's next sync still lists it as ready, until the TTL expires. This
# stops InstanceAwareLeastLoadPolicy from preferentially routing to a dead
# replica whose drained in-flight slots read as least-loaded, and avoids
# evict/re-add oscillation on every sync once the controller recovers.
LB_EVICTION_CONSECUTIVE_FAILURES = 3
LB_EVICTION_QUARANTINE_SECONDS = 30

# [boltz fork] Demand feed for concurrency-native autoscaling. Terminal LB
# 503s ("no ready replicas" / "all ready replicas at capacity") are short-lived
# shadow pressure that the QPS window cannot express. Callers may retry the same
# logical job, so raw counts could multiply one unit of demand. The LB instead
# dedups by the stable per-job header below and reports the number of UNIQUE jobs
# rejected within this TTL. A retry refreshes the TTL and still counts once; a
# job that lands elsewhere after the rejection decays after one window.
LB_REJECT_WINDOW_SECONDS = 360

# Request header carrying a job id that is STABLE across retries of the same
# job -- that is its contract, and what makes the reject-window dedup above
# possible. Requests without it fall back to a unique per-request key
# (documented raw-count over-estimation; the platform sends the header).
LB_JOB_ID_HEADER = 'X-SkyServe-Job-Id'

# Per-request scheduling priority consumed by the load balancer. Higher values
# are dispatched first; equal values retain arrival order. The header is never
# forwarded to replicas.
LB_REQUEST_PRIORITY_HEADER = 'X-SkyServe-Priority'
LB_REQUEST_PRIORITY_HEADER_BYTES = LB_REQUEST_PRIORITY_HEADER.lower().encode(
    'ascii')
LB_REQUEST_PRIORITY_MIN = 0
LB_REQUEST_PRIORITY_MAX = 100

# Optional exact accelerator compatibility set for one request. The controller
# advertises the configured exact card catalog over the routing sync before the
# load balancer accepts this header; omission means every configured card.
LB_REQUEST_ACCELERATORS_HEADER = 'X-SkyServe-Compatible-Accelerators'
LB_REQUEST_ACCELERATORS_HEADER_BYTES = (
    LB_REQUEST_ACCELERATORS_HEADER.lower().encode('ascii'))
LB_REQUEST_ACCELERATORS_VERSION = 1
LB_REQUEST_ACCELERATORS_MAX_BYTES = 512
LB_REQUEST_ACCELERATORS_MAX_ITEMS = 8

# On SIGTERM the external LB first deregisters (stops POSTing
# load_balancer_sync so the controller stops counting it -- avoiding a
# double-count with the maxSurge replacement) and fails readiness (so k8s
# pulls it from the Service endpoints), then waits this long for in-flight
# requests to drain before letting the server exit.
LB_DRAIN_GRACE_SECONDS = 15
# Keep the best-effort history-only shutdown flush comfortably inside the
# pod's drain grace. It must never extend termination or re-register demand.
LB_DRAIN_HISTORY_FLUSH_TIMEOUT_SECONDS = 5

# [boltz fork] Reserved-capacity fill (opt-in via
# replica_policy.reserved_capacity_fill): the controller runs a poller that
# measures FREE capacity on the service's zero-cost locations and the
# autoscaler additionally scales up onto it (bounded by max_replicas). Poll
# cadence of that poller; the realtime free-GPU query lists every pod in the
# cluster, so it must stay well above the autoscaler decision interval.
RESERVED_CAPACITY_POLL_INTERVAL_SECONDS = 60
RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR = (
    'SKYPILOT_SERVE_RESERVED_CAPACITY_POLL_SECONDS')
# A capacity snapshot older than this many poll intervals contributes 0 free
# slots: a dead poller must never assert a fill floor. Zero-cost replicas
# that ALREADY exist keep counting toward the fill target, so a poller
# outage never turns the live fill fleet into scale-down victims.
RESERVED_CAPACITY_STALE_AFTER_INTERVALS = 3
# Sentinel resources_override key marking a scale-up decision as
# capacity-driven fill: the launch path pops it and restricts placement to
# zero-cost ACTIVE locations, skipping the launch entirely when none is
# available (fill must NEVER spill to paid capacity). Underscore-prefixed so
# it can never collide with a real Resources field.
RESERVED_CAPACITY_FILL_OVERRIDE_KEY = '_reserved_fill_zero_cost_only'
# Sentinel resources_override key carrying the broker grant epoch a fill
# scale-up was emitted under. The launch path pops it (never reaches
# sky.launch) and re-checks the POOL's current round epoch right before
# committing the launch: a decision computed from a superseded allocation
# round must skip instead of launching against capacity the broker has
# since re-granted to a peer service.
RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY = '_reserved_fill_grant_epoch'
# Sentinel resources_override key carrying the pool key the grant epoch
# belongs to (always stamped alongside the epoch). Rounds and epochs are
# per-pool: the launch fence compares the carried epoch against ITS pool's
# round epoch -- a global comparison would let pool A's grant churn fence
# pool B's unrelated fill launches.
RESERVED_FILL_POOL_KEY_OVERRIDE_KEY = '_reserved_fill_pool_key'
# Internal protocol-v2 launch fences. These values are consumed by the
# replica manager before Resources.copy(): they must never reach a provider
# request.
RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY = (
    '_reserved_fill_protocol_version')
RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY = (
    '_reserved_fill_service_generation')
RESERVED_FILL_SERVICE_VERSION_OVERRIDE_KEY = '_reserved_fill_service_version'
RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY = (
    '_reserved_fill_physical_cluster_uid')
RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY = (
    '_reserved_fill_allowed_locations')
# Immutable allocation-publication identity carried only across the typed
# protocol-v2 admission seam.  These values are persisted on ReplicaInfo so a
# reconcile replay can debit rows already accepted from the same allocation
# without treating them as new ordinary demand.
RESERVED_FILL_ALLOCATION_GENERATION_OVERRIDE_KEY = (
    '_reserved_fill_allocation_generation')
RESERVED_FILL_ALLOCATION_INPUT_SHA256_OVERRIDE_KEY = (
    '_reserved_fill_allocation_input_sha256')
RESERVED_FILL_ALLOCATION_CLAIM_GENERATION_OVERRIDE_KEY = (
    '_reserved_fill_allocation_claim_generation')
RESERVED_FILL_GATE_GENERATION_OVERRIDE_KEY = (
    '_reserved_fill_reconciliation_gate_generation')
RESERVED_FILL_RECLAIM_FLEET_BUNDLE_SHA256_OVERRIDE_KEY = (
    '_reserved_fill_reclaim_fleet_bundle_sha256')
RESERVED_FILL_RECLAIM_POLICY_REVISION_OVERRIDE_KEY = (
    '_reserved_fill_reclaim_policy_revision')
RESERVED_FILL_RECLAIM_PROVIDER_INVENTORY_SHA256_OVERRIDE_KEY = (
    '_reserved_fill_reclaim_provider_inventory_sha256')
RESERVED_FILL_WORKER_PROJECTION_SHA256_OVERRIDE_KEY = (
    '_reserved_fill_worker_projection_sha256')
RESERVED_FILL_OBSERVATION_GENERATION_OVERRIDE_KEY = (
    '_reserved_fill_observation_generation')
RESERVED_FILL_OBSERVATION_SEQUENCE_OVERRIDE_KEY = (
    '_reserved_fill_observation_sequence')
RESERVED_FILL_ORDINARY_ADMISSION_SEQUENCE_OVERRIDE_KEY = (
    '_reserved_fill_ordinary_admission_sequence')
RESERVED_FILL_INTENT_IDEMPOTENCY_KEY_OVERRIDE_KEY = (
    '_reserved_fill_intent_idempotency_key')

# Internal resources_override marker for a cost-rebalance launch.  The value is
# the incumbent replica id.  ReplicaManager consumes it before sky.launch and
# persists the pairing on ReplicaInfo; it must never reach Resources.copy().
COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY = '_cost_rebalance_for_replica_id'

# [boltz fork] Reserved-fill broker: multi-service arbitration of the
# zero-cost pools (see sky/serve/reserved_capacity_broker.py). Cross-process
# lock serializing broker rounds. On a Postgres backend get_lock() resolves
# this to a session advisory lock (shared across api-server pods); on SQLite
# it is a node-local filelock (single pod, sufficient -- every serve
# controller shares the api-server pod in consolidation mode). Independent of
# the election primitive, actuation correctness rests on the per-pool round
# epoch (see the broker module), not on this lock.
RESERVED_FILL_BROKER_LOCK_ID = '~/.sky/serve_reserved_fill_broker_lock'
# Bounded by one cluster-wide realtime query plus a handful of DB
# round-trips; generous so a slow cluster query makes peers wait for the
# fresh round instead of timing out into a no-fill cycle.
RESERVED_FILL_BROKER_LOCK_TIMEOUT_SECONDS = 120
# Shared demand-placement observations and reservations use a separate lock
# from fill arbitration. The refresh path never holds it while a planner is
# waiting on the fill broker, avoiding lock-order coupling between features.
DEMAND_CAPACITY_REFRESH_LOCK_ID = '~/.sky/serve_demand_capacity_refresh_lock'
DEMAND_CAPACITY_RESERVATION_LOCK_ID = (
    '~/.sky/serve_demand_capacity_reservation_lock')
# If a ready logical backend cannot report usable slot capacity for this long,
# launch one bounded replacement wave while keeping the uncertain backend
# alive. Replacement rows are durably marked so a persistent telemetry outage
# cannot create an unbounded sequence of overlap waves across restarts.
LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS = (
    3 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
# A claim whose heartbeat is older than this is dead and drops out of
# arbitration. Comfortably above the controller respawn+boot+recovery window
# (the parent respawns children within seconds, but recovery of a large
# fleet can take minutes): a fast-respawned controller must re-adopt its own
# claim instead of colliding with its ghost, and a genuinely dead service
# must not hold a grant forever.
RESERVED_FILL_CLAIM_TTL_SECONDS = 300
RESERVED_FILL_CLAIM_TTL_ENV_VAR = (
    'SKYPILOT_SERVE_RESERVED_FILL_CLAIM_TTL_SECONDS')
# Broker lease expiry, in poll intervals. Only used to force an epoch bump
# after a period with no rounds at all (every outstanding grant is then
# suspect); NOT a liveness bound on any single controller.
RESERVED_FILL_LEASE_TTL_INTERVALS = 5
# A positive feed assignment sticks to its service for this many poll
# intervals: a single free GPU fairness-alternated between two services
# every round would never survive the local two-poll increase damping and
# idle forever.
RESERVED_FILL_STICKY_FEED_INTERVALS = 2
# Consecutive phantom observations (successful realtime query reporting NO
# labeled nodes for the claimed GPU) required before the broker rejects a
# pool's claims. Exact-context credential, RBAC, transport, and provider-read
# failures raise into BLACKOUT; an empty result is a topology/label
# observation. It can still reflect transient node or label propagation, so
# deleting every claim on one reading would turn that blip into a pool-wide
# fill outage. Suspect rounds feed 0 (conservative) but keep the claims; only a
# persistent phantom (this many rounds in a row) rejects.
RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS = 3
# Upper bound on reserved_capacity_fill.weight. isfinite alone is not
# enough: 1e308 is finite yet overflows remaining*weight / sum(weights) in
# the broker's water-fill into inf (NaN shares crash integer rounding).
# The spec rejects weights above this at construction; the broker clamps
# out-of-bound DB rows to it defensively. 1e6 preserves any sane priority
# ratio while staying far from float overflow.
RESERVED_FILL_MAX_WEIGHT = 1e6

# [boltz fork] Utilization gate (default for reserved_capacity_fill; explicit
# utilization_gate:false opts out): a claimant that demonstrates no work
# walks its whole fill entitlement down to zero in bounded steps, including
# its declared reserved floor. Positive utilization restores a cap proportional
# to demonstrated need; a large declared floor cannot inflate that cap.
# Released capacity returns to genuinely free GPUs
# where any service can take it -- including one that declares no
# reserved_capacity_fill at all and can therefore only reach the pool through
# ordinary cheapest-first demand placement.
#
# These are POOL-GLOBAL on purpose. A per-service dwell or step rate would
# let the slower-decaying claimant win every contested transient purely by
# decaying slower, which re-creates through timing exactly the static
# priority this feature removes.
#
# Continuously-zero demonstrated need required before the first release step.
# A gated writer with no usable utilization telemetry reports armed-but-blind:
# it freezes for the bounded blind grace before decay resumes. Retaining a
# static reservation requires the explicit per-service opt-out. Equals
# downscale_delay_seconds on both live services, equals
# RESERVED_FILL_CLAIM_TTL_SECONDS, and is 5 poll intervals / 15 LB syncs /
# 5x the report-staleness threshold.
RESERVED_FILL_IDLE_DWELL_SECONDS = 300.0
# At most one release step per this window, so the local drain-aware
# scale-down can actuate a step before the next one is proposed.
RESERVED_FILL_RELEASE_STEP_SECONDS = 300.0
# Each step releases this fraction of the surplus above the floor, so the
# release rate is scale-free across a 10-replica and a 200-replica fleet.
RESERVED_FILL_RELEASE_STEP_FRACTION = 0.25
# Integer termination: a pure geometric decay never reaches the floor in
# integers, and the tail would be a long sequence of 1-replica steps.
RESERVED_FILL_RELEASE_MIN_STEP = 2
# Growth room above demonstrated need, so the gate is never the binding
# constraint on a service that is actively growing. The ordinary
# autoscaler target and effective_cap stay the binding constraints on the
# way up, exactly as before this feature.
RESERVED_FILL_UTILIZATION_HEADROOM = 0.25
# Maximum heartbeat_ts - activity_ts for a claim's utilization columns to
# be trusted. This is the version-skew discriminator: an old binary
# heartbeating a migrated row advances heartbeat_ts while leaving the new
# columns frozen, and a frozen demonstrated_need of 0 would walk a busy
# service to its floor. Matches the autoscaler's own report-staleness
# threshold (3 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS).
RESERVED_FILL_ACTIVITY_MAX_LAG_SECONDS = 3.0 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS
# How long a blind claimant (no usable telemetry) freezes its release
# target before the decay resumes anyway. Freezing is the safe direction
# for a transient outage, but a permanently wedged load balancer must not
# pin a whole pool indefinitely. 15 rounds, 3x the claim TTL.
RESERVED_FILL_BLIND_GRACE_SECONDS = 900.0
# Process-wide kill switch for the gate, parsed like the poll interval and
# claim TTL overrides. Set to a false-y value to disarm the gate for every
# service in this api-server without a spec update.
RESERVED_FILL_UTILIZATION_GATE_ENV_VAR = (
    'SKYPILOT_SERVE_RESERVED_FILL_UTILIZATION_GATE')

# Default interval in seconds to probe replica endpoint.
DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS = 10
# Backward compatibility alias.
ENDPOINT_PROBE_INTERVAL_SECONDS = DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS

# The default timeout in seconds for a readiness probe request. We set the
# timeout to 15s since using actual generation in LLM services as readiness
# probe is very time-consuming (33B, 70B, ...).
DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS = 15

# Adaptive demand estimation. Measured request duration and provisioning
# lead supersede their configured values once enough live evidence exists,
# so a stale hand-set number cannot silently mis-size the fleet forever.
# Seed used while a service has not yet measured its own launch-to-ready
# time (`initial_provision_lead_time_seconds: auto`). Provisioning a GPU
# replica takes minutes on every supported cloud, so assuming zero would
# size the first bursts of a service's life as if capacity were instant.
AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS = 600.0
# Sentinel accepted by initial_provision_lead_time_seconds.
AUTOSCALER_PROVISION_LEAD_AUTO = 'auto'
# Minimum completed requests before a measured duration is trusted. One
# decision tick of a small fleet should not redefine the sizing constant.
AUTOSCALER_ADAPTIVE_DURATION_MIN_SAMPLES = 20
# Smoothing for the measured-duration EMA. Deliberately slow: sizing must
# track the workload's central tendency, not one burst of long requests.
AUTOSCALER_ADAPTIVE_DURATION_EMA_ALPHA = 0.2
# Minimum observed launch-to-ready samples before a measured lead is
# trusted, and how many recent samples the quantile is taken over.
AUTOSCALER_ADAPTIVE_LEAD_MIN_SAMPLES = 5
AUTOSCALER_ADAPTIVE_LEAD_SAMPLE_CAP = 50
# Lead quantile. Sizing against the median would leave the slower half of
# launches arriving after the SLA budget they were sized for.
AUTOSCALER_ADAPTIVE_LEAD_QUANTILE = 0.75
# A measurement older than this stops superseding configuration: a service
# that has been idle for hours must not size from a stale regime.
AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS = 6 * 60 * 60

# Autoscaler window size in seconds for query per second. We calculate qps by
# divide the number of queries in last window size by this window size.
AUTOSCALER_QPS_WINDOW_SIZE_SECONDS = 60
# Autoscaler scale decision interval in seconds.
# We will try to scale up/down every `decision_interval`.
AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS = 20
# Autoscaler no replica decision interval in seconds.
AUTOSCALER_NO_REPLICA_DECISION_INTERVAL_SECONDS = 5
# Autoscaler default upscale delays in seconds.
# We will upscale only if the target number of instances
# is larger than the current launched instances for delay amount of time.
AUTOSCALER_DEFAULT_UPSCALE_DELAY_SECONDS = 300
# Autoscaler default downscale delays in seconds.
# We will downscale only if the target number of instances
# is smaller than the current launched instances for delay amount of time.
AUTOSCALER_DEFAULT_DOWNSCALE_DELAY_SECONDS = 1200
# Default queue length threshold for pool autoscaling.
# When max_workers is set but queue_length_threshold is not specified,
# this default threshold will be used.
AUTOSCALER_DEFAULT_QUEUE_LENGTH_THRESHOLD = 1
# The default controller resources. We need 200 GB disk space to enable using
# Azure as controller, since its default image size is 150 GB.
# TODO(tian): We might need to be careful that service logs can take a lot of
# disk space. Maybe we could use a larger disk size, migrate to cloud storage or
# do some log rotation.
# Set default minimal memory to 8GB to allow at least one service to run.
CONTROLLER_RESOURCES = controller_constants.SERVE_CONTROLLER_RESOURCES
# Autostop config for the jobs controller. These are the default values for
# serve.controller.autostop in ~/.sky/config.yaml.
CONTROLLER_AUTOSTOP = controller_constants.SERVE_CONTROLLER_AUTOSTOP

# A period of time to initialize your service. Any readiness probe failures
# during this period will be ignored.
DEFAULT_INITIAL_DELAY_SECONDS = 1200
DEFAULT_MIN_REPLICAS = 0

# Default dynamic controller-port start and fixed per-service LB container
# port. Controller ports stay pod-local; only each Kubernetes LB Service
# exposes its own port 30001.
CONTROLLER_PORT_START = 20001
# Durable acknowledgement written only after the parent has killed and joined
# its controller child. None is not sufficient: recovery preclaim deliberately
# clears the port while a replacement child is still booting.
CONTROLLER_TEARDOWN_ACK_PORT = -1
LOAD_BALANCER_PORT_START = 30001

# Initial version of service.
INITIAL_VERSION = 1

# Replica ID environment variable name that can be accessed on the replica.
REPLICA_ID_ENV_VAR = 'SKYPILOT_SERVE_REPLICA_ID'

# Name of the environment variable holding the controller pod's own name.
# In external load balancer mode the controller reads its own pod spec to
# mirror its container image onto the LB Deployment it creates. The platform
# must inject this via the downward API (metadata.name). It is a hard contract:
# without it the controller cannot resolve the LB image.
POD_NAME_ENV_VAR = 'SKYPILOT_POD_NAME'
# Helm-rendered name of the stable API Deployment that owns generated external
# LB objects. Unlike the API Pod/ReplicaSet identities, this Deployment UID is
# stable across ordinary rollouts and gives Kubernetes garbage collection the
# correct release-lifetime anchor.
API_DEPLOYMENT_NAME_ENV_VAR = 'SKYPILOT_API_DEPLOYMENT_NAME'
# Existing Helm release identity, retained as the mixed-version fallback for
# charts that predate API_DEPLOYMENT_NAME_ENV_VAR. The API Deployment rendered
# by those charts is always named ``<release>-api-server``.
RELEASE_NAME_ENV_VAR = 'SKYPILOT_RELEASE_NAME'
# Downward-API-injected namespace of the API/controller pod. Controller-owned
# LB objects and their projected Secrets live beside that pod even when the
# configured Kubernetes workload namespace is different.
POD_NAMESPACE_ENV_VAR = 'SKYPILOT_POD_NAMESPACE'

# The version of the lib files that serve use. Whenever there is an API
# change for the serve_utils.ServeCodeGen, we need to bump this version, so that
# the user can be notified to update their SkyPilot serve version on the remote
# cluster.
# Changelog:
# v1.0 - Introduce rolling update.
# v2.0 - Added template-replica feature.
# v3.0 - Added pool.
# v4.0 - Added pool argument to wait_service_registration.
# v5.0 - Added pool argument to stream_serve_process_logs & stream_replica_logs.
# v6.0 - Added summary_only argument to get_service_status (cheap dashboard
#        summaries: replica_status_counts instead of full replica_info).
# v7.0 - Added include_target_num_replicas override so summary-only callers
#        can skip per-service autoscaler HTTP fetches unless they render it.
# v8.0 - Added per-GPU logical replica semantics and logical capacity hints.
# v9.0 - Added the authenticated, read-only placement snapshot endpoint.
# v10.0 - Added metadata_only to get_service_status for progressive dashboard
#         rendering without replica, autoscaler, history, or endpoint reads.
SERVE_VERSION = 10

TERMINATE_REPLICA_VERSION_MISMATCH_ERROR = (
    'The version of service is outdated and does not support manually '
    'terminating replicas. Please terminate the service and spin up again.')

# Dummy run command for pool.
POOL_DUMMY_RUN_COMMAND = 'echo "setup done"'

# Error message prefix for max number of services reached.
# This is used as a marker to detect the error in controller logs.
MAX_NUMBER_OF_SERVICES_REACHED_ERROR = 'Max number of services reached'
# Serve056 introduced the executor-side provider-effect cohort at epoch 2.
# Protocol-v2 exact-resource replay rotated it to epoch 3.  Provider-proof
# flow control rotated it again to epoch 4. Scratch-backed worker bootstrap
# rotated it to epoch 5. Projection-v6 collision repair rotated it to epoch 6.
# Projection-v7 bootstrap-supervisor repair rotated it to epoch 7. The
# protocol-v8 attested node-local cache bootstrap rotated it to epoch 8.
# Protocol-v9 cache-leaf normalization rotates it to epoch 9:
# every live API/controller/executor participant and every service capability
# tuple must agree before any new launch can carry projection protocol v9; the
# adjacent cohort remains settlement-only.
NON_POOL_CAPABILITY_COHORT_EPOCH = 9
