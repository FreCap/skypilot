"""Constants used for SkyServe."""

CONTROLLER_TEMPLATE = 'sky-serve-controller.yaml.j2'

SKYSERVE_METADATA_DIR = '~/.sky/serve'

# The filelock for selecting service ports when starting a service. Two
# requirements:
#  (1) Same-pod sky.serve.service subprocesses must serialize so they don't
#      both pick the same just-freed port between their respective
#      `find_free_port` and the controller subprocess actually `bind()`-ing.
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
REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY = 'sky_serve_controller_pid'
REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY = 'sky_serve_controller_ip'
REPLICA_LAUNCH_FENCE_KEYS = (
    REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY,
    REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY,
    REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY,
    REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY,
)

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
EXTERNAL_LB_ENABLED_ENV_VAR = 'SKYPILOT_SERVE_EXTERNAL_LB_ENABLED'

# Downward-API-injected UID of the external LB pod. Unlike a process-local
# UUID, this survives controller restarts as the durable LB incarnation key.
LB_POD_UID_ENV_VAR = 'SKYPILOT_SERVE_LB_POD_UID'
LB_RESOURCES_ENV_VAR = 'SKYPILOT_SERVE_LB_RESOURCES_JSON'

# The load balancer's readiness route; exempt from inbound bearer auth so the
# k8s readinessProbe (and any LB-level health check) can reach it. Kept here so
# the route registration and the auth middleware share one source of truth.
LB_HEALTH_ENDPOINT_PATH = '/_lb/health'
LB_LIVENESS_ENDPOINT_PATH = '/_lb/liveness'

# Hard cap on the number of request timestamps the LB retains between successful
# controller syncs. The batch is retained (not dropped) across a failed sync so
# the autoscaler does not lose load signal, but a PERSISTENT sync failure must
# not grow it without bound (one float per proxied request). The most recent
# CAP samples are kept -- ample for QPS autoscaling even at the top of the
# supported RPS range across several sync intervals.
LB_REQUEST_TIMESTAMP_CAP = 100_000

# [boltz fork] Time budget in seconds for a service update to be accepted by
# the controller. The /controller/update_service handler serializes on the
# replica-manager lock, which a readiness-probe round can hold for tens of
# seconds when replicas are unreachable (probe timeouts are user-configurable
# with no hard cap, plus the inline preemption status refresh), so the
# default 10s HTTP/gRPC timeouts would spuriously fail updates against a
# busy-but-healthy controller. Used as the HTTP read timeout on the
# controller POST and (plus margin) as the skylet gRPC deadline in VM mode.
# 600: at fleet scale the lock wait is minutes, not tens of seconds
# (measured live 2026-07-06: >120s at ~900 replicas made the CLI report a
# false failure while the update actually landed). The wait is genuine
# work, not a hang; budget generously.
UPDATE_SERVICE_TIMEOUT_SECONDS = 600

# The time interval in seconds for load balancer to sync with controller. Every
# time the load balancer syncs with controller, it will update all available
# replica ips for each service, also send the number of requests in last query
# interval.
LB_CONTROLLER_SYNC_INTERVAL_SECONDS = 20

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

# [boltz fork] Cadence of the LB's per-replica async-occupancy probe (the
# `async_capacity` action). The HTTP-envelope in-flight accounting reads ~0
# for fast-ack async workloads while replicas crunch hour-long jobs, so the
# LB asks each ready replica for its true running-job count and uses it to
# deprioritize busy replicas in routing and to report real free slots on
# /_lb/capacity. Overridable via SKYPILOT_LB_OCCUPANCY_PROBE_INTERVAL_SECONDS
# (<= 0 disables the probe entirely — accounting falls back to envelope-only).
LB_OCCUPANCY_PROBE_INTERVAL_SECONDS = 10
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
# 512Mi memory limit. The aggregate body budget leaves room for transient
# bytearray->bytes copies, queued ASGI receive buffers, clients, and Python.
LB_REQUEST_QUEUE_MAX_SIZE_LIMIT = 2000
LB_REQUEST_QUEUE_MAX_CONCURRENCY_LIMIT = 128
LB_REQUEST_QUEUE_MAX_BODY_BYTES_LIMIT = 16 * 1024 * 1024
LB_REQUEST_QUEUE_BODY_MEMORY_BUDGET_BYTES = 128 * 1024 * 1024

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

# On SIGTERM the external LB first deregisters (stops POSTing
# load_balancer_sync so the controller stops counting it -- avoiding a
# double-count with the maxSurge replacement) and fails readiness (so k8s
# pulls it from the Service endpoints), then waits this long for in-flight
# requests to drain before letting the server exit.
LB_DRAIN_GRACE_SECONDS = 15

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
# pool's claims. kubernetes_catalog returns empty dicts without raising on
# credential/cache/label-formatter failures, so a single "phantom" reading
# can be a transient kube-apiserver blip masquerading as a successful
# observation; deleting every claim on one reading turns that blip into a
# pool-wide fill outage. Suspect rounds feed 0 (conservative) but keep the
# claims; only a persistent phantom (this many rounds in a row) rejects.
RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS = 3
# Upper bound on reserved_capacity_fill.weight. isfinite alone is not
# enough: 1e308 is finite yet overflows remaining*weight / sum(weights) in
# the broker's water-fill into inf (NaN shares crash integer rounding).
# The spec rejects weights above this at construction; the broker clamps
# out-of-bound DB rows to it defensively. 1e6 preserves any sane priority
# ratio while staying far from float overflow.
RESERVED_FILL_MAX_WEIGHT = 1e6

# Default interval in seconds to probe replica endpoint.
DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS = 10
# Backward compatibility alias.
ENDPOINT_PROBE_INTERVAL_SECONDS = DEFAULT_ENDPOINT_PROBE_INTERVAL_SECONDS

# The default timeout in seconds for a readiness probe request. We set the
# timeout to 15s since using actual generation in LLM services as readiness
# probe is very time-consuming (33B, 70B, ...).
DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS = 15

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
CONTROLLER_RESOURCES = {'cpus': '4+', 'memory': '8+', 'disk_size': 200}
# Autostop config for the jobs controller. These are the default values for
# serve.controller.autostop in ~/.sky/config.yaml.
CONTROLLER_AUTOSTOP = {
    'idle_minutes': 10,
    'down': False,
}

# A period of time to initialize your service. Any readiness probe failures
# during this period will be ignored.
DEFAULT_INITIAL_DELAY_SECONDS = 1200
DEFAULT_MIN_REPLICAS = 1

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
# In external load balancer mode the controller (running in the api-server pod)
# reads its own pod spec to mirror its container image onto the LB Deployment
# it creates. The platform must inject this via the downward API
# (metadata.name). It is a hard contract: without it the controller cannot
# resolve the LB image.
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
SERVE_VERSION = 7

TERMINATE_REPLICA_VERSION_MISMATCH_ERROR = (
    'The version of service is outdated and does not support manually '
    'terminating replicas. Please terminate the service and spin up again.')

# Dummy run command for pool.
POOL_DUMMY_RUN_COMMAND = 'echo "setup done"'

# Error message prefix for max number of services reached.
# This is used as a marker to detect the error in controller logs.
MAX_NUMBER_OF_SERVICES_REACHED_ERROR = 'Max number of services reached'
