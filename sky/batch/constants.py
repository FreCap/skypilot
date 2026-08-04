"""Constants for Sky Batch."""

# Worker service (localhost HTTP on each worker node)
WORKER_SERVICE_PORT = 8290
WORKER_SERVICE_STARTUP_TIMEOUT = 60  # seconds to wait for service health
WORKER_SHUTDOWN_HEALTH_WAIT_SECONDS = 5
WORKER_SHUTDOWN_POLL_INTERVAL_SECONDS = 0.2
WORKER_FAILURE_MARKER_PATH = '/tmp/sky_batch_worker_failure.txt'
# Env var carrying the launch-unique failure marker path. Each worker launch
# uses its own marker file so a stale marker left by a crashed previous worker
# on the same node cannot fail the health check of a fresh launch.
WORKER_FAILURE_MARKER_ENV_VAR = 'SKY_BATCH_FAILURE_MARKER'

# Timeouts (in seconds)
WORKER_DISCOVERY_TIMEOUT = 300
# On resume, batches are already checkpointed so we can afford to wait longer
# for pool workers to reappear while the controller pod and the serve-side
# pool status plumbing stabilize after a restart.  Don't make this too large
# though: if the controller pod is stuck in a restart loop, we want the run
# to fail fast enough for the next attempt to take over.
WORKER_DISCOVERY_RESUME_TIMEOUT = 600
BATCH_COMPLETION_TIMEOUT = 3600  # 1 hour max per batch

# A dispatcher renews its attempt lease while the worker job is active.  If a
# controller disappears, another incarnation can reclaim the batch after the
# lease expires instead of either duplicating it immediately or waiting for a
# controller-wide timeout.
BATCH_LEASE_DURATION = 60
BATCH_LEASE_RENEW_INTERVAL = 20

# Polling interval for sdk.job_status() when waiting for batch completion
BATCH_POLL_INTERVAL = 5

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # Exponential backoff base

# Naming pattern for result batch files
# e.g., batch_00000000-00000031.jsonl for indices 0-31
BATCH_NAME_PATTERN = 'batch_{start:08d}-{end:08d}.jsonl'

# Naming pattern for intermediate input batch files
# e.g., input_batch_00000000-00000031.jsonl for indices 0-31
INPUT_BATCH_NAME_PATTERN = 'input_batch_{start:08d}-{end:08d}.jsonl'

# Temporary directory name for intermediate results
TEMP_DIR_NAME = '.sky_batch_tmp'
