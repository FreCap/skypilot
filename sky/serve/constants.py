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

# The consolidation mode lock ensures that if multiple API servers are running
# at the same time (e.g. during a rolling update), recovery can only happen once
# the previous API server has exited.
POOL_CONSOLIDATION_MODE_LOCK_ID = '~/.sky/pool_consolidation_mode_lock'
SERVE_CONSOLIDATION_MODE_LOCK_ID = '~/.sky/serve_consolidation_mode_lock'

# Time to wait in seconds for controller to setup, this involves the time to run
# cloud dependencies installation.
CONTROLLER_SETUP_TIMEOUT_SECONDS = 300
# Time to wait in seconds for service to register on the controller.
SERVICE_REGISTER_TIMEOUT_SECONDS = 60

# Env var holding a shared bearer token that guards the controller's
# CONTROL-PLANE endpoints: the destructive ones (/controller/update_service,
# terminate_replica) AND the read-only sync/status paths (/load_balancer_sync,
# /autoscaler/info). In external load balancer mode the controller port is
# reachable on the pod network from the LB pod, so NetworkPolicy alone is
# insufficient; the platform sets this (from a k8s secret) on both the
# controller and its trusted callers, and the LB presents it on every sync so a
# leaked in-cluster foothold can no longer scrape the replica set or drive the
# controller. When unset (in-pod localhost-only default) auth is disabled and
# behavior is unchanged.
CONTROLLER_AUTH_TOKEN_ENV_VAR = 'SKYPILOT_SERVE_CONTROLLER_AUTH_TOKEN'

# Env var holding a shared bearer token that guards INBOUND inference requests
# to the external load balancer (data-plane auth), held by the inference
# client. Kept DISTINCT from CONTROLLER_AUTH_TOKEN_ENV_VAR (control-plane) on
# purpose: sharing them would let an inference client reach the controller's
# destructive endpoints. The LB's readiness route (LB_HEALTH_ENDPOINT_PATH)
# stays open so k8s can probe it. When unset, inbound auth is disabled and
# behavior is unchanged.
LB_AUTH_TOKEN_ENV_VAR = 'SKYPILOT_SERVE_LB_AUTH_TOKEN'

# The load balancer's readiness route; exempt from inbound bearer auth so the
# k8s readinessProbe (and any LB-level health check) can reach it. Kept here so
# the route registration and the auth middleware share one source of truth.
LB_HEALTH_ENDPOINT_PATH = '/_lb/health'

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
# controller (raised from the previous inline 5s).
# The controller's load_balancer_sync handler can be slow under a launch storm
# (the launch threads contend on the same database); a timeout that is too
# tight makes the sync fail, leaving the load balancer with an empty
# ready-replica list so it 503s every request even when READY replicas exist.
LB_CONTROLLER_SYNC_TIMEOUT_SECONDS = 30

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
LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS = 3600
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
# 503s ("no ready replicas" / "all ready replicas at capacity") are backlog
# pressure the QPS window cannot express: the platform's marker-timeout loop
# re-fires the SAME held job every ~30s, so summing raw rejects over a window
# would multiply one outstanding job by window/retry-period (~10x
# over-target, each over-launched GPU then pinned by hysteresis). The LB
# instead dedups rejects by the stable per-job header below and reports the
# count of UNIQUE jobs terminally rejected within this TTL window: a held job
# re-firing refreshes its TTL and still counts as one unit of pressure; once
# it lands (or spills for good) its pressure decays after one window.
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

# Default port range start for controller and load balancer. Ports will be
# automatically generated from this start port.
CONTROLLER_PORT_START = 20001
LOAD_BALANCER_PORT_START = 30001
LOAD_BALANCER_PORT_RANGE = '30001-30020'

# Size of the stable per-service controller port range used in external load
# balancer mode (serve.controller.external_load_balancer). In that mode each
# service is assigned a fixed port in [CONTROLLER_PORT_START,
# CONTROLLER_PORT_START + CONTROLLER_PORT_RANGE_SIZE) that is persisted and
# reused across controller respawns and pod rolls, so an external load
# balancer has a stable controller address to target. This bounds the number
# of concurrent services per controller pod; the k8s Service must expose the
# same range.
CONTROLLER_PORT_RANGE_SIZE = 100

# Cross-pod lock serializing stable controller-port assignment in external
# load balancer mode. On a Postgres backend get_lock() resolves this to a
# session advisory lock (shared across api-server pods); on SQLite it is a
# node-local filelock (single pod, sufficient). Needed because the
# per-node PORT_SELECTION_FILE_LOCK cannot serialize two pods scanning the
# shared DB for a free port.
CONTROLLER_PORT_ASSIGNMENT_LOCK_ID = (
    '~/.sky/serve_controller_port_assignment_lock')
CONTROLLER_PORT_ASSIGNMENT_LOCK_TIMEOUT_SECONDS = 30

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
SERVE_VERSION = 6

TERMINATE_REPLICA_VERSION_MISMATCH_ERROR = (
    'The version of service is outdated and does not support manually '
    'terminating replicas. Please terminate the service and spin up again.')

# Dummy run command for pool.
POOL_DUMMY_RUN_COMMAND = 'echo "setup done"'

# Error message prefix for max number of services reached.
# This is used as a marker to detect the error in controller logs.
MAX_NUMBER_OF_SERVICES_REACHED_ERROR = 'Max number of services reached'
