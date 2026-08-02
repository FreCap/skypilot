"""Declarative database schema for managed jobs."""

import sqlalchemy
from sqlalchemy.ext import declarative

from sky.dag import DagExecution
from sky.skylet import constants

Base = declarative.declarative_base()

# `spot` table contains all the finest-grained tasks, including all the
# tasks of a managed job (called spot for legacy reason, as it is generalized
# from the previous managed spot jobs). All tasks of the same job will have the
# same `spot_job_id`.
# The `job_name` column is now deprecated. It now holds the task's name, i.e.,
# the same content as the `task_name` column.
# The `job_id` is now not really a job id, but a only a unique
# identifier/primary key for all the tasks. We will use `spot_job_id`
# to identify the job.
# TODO(zhwu): schema migration may be needed.

spot_table = sqlalchemy.Table(
    'spot',
    Base.metadata,
    sqlalchemy.Column('job_id',
                      sqlalchemy.Integer,
                      primary_key=True,
                      autoincrement=True),
    sqlalchemy.Column('job_name', sqlalchemy.Text),
    sqlalchemy.Column('resources', sqlalchemy.Text),
    sqlalchemy.Column('submitted_at', sqlalchemy.Float),
    # Indexed because non-terminal-status filtering on this column is on the
    # hot path for the pool dashboard (per-pool job listing) and skip_finished
    # queries; without it the filter is a full table scan over all (including
    # finished) tasks.
    sqlalchemy.Column('status', sqlalchemy.Text, index=True),
    sqlalchemy.Column('run_timestamp', sqlalchemy.Text),
    sqlalchemy.Column('start_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('end_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('last_recovered_at',
                      sqlalchemy.Float,
                      server_default='-1'),
    sqlalchemy.Column('recovery_count', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('job_duration', sqlalchemy.Float, server_default='0'),
    sqlalchemy.Column('failure_reason', sqlalchemy.Text),
    sqlalchemy.Column('spot_job_id', sqlalchemy.Integer, index=True),
    sqlalchemy.Column('task_id', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('task_name', sqlalchemy.Text),
    sqlalchemy.Column('specs', sqlalchemy.Text),
    sqlalchemy.Column('local_log_file', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('metadata', sqlalchemy.Text, server_default='{}'),
    sqlalchemy.Column('links', sqlalchemy.JSON, server_default=None),
    sqlalchemy.Column('logs_cleaned_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('full_resources', sqlalchemy.JSON, server_default=None),
    # Whether this task is a primary task (True) or auxiliary task (False)
    # within a job group. NULL for non-job-group jobs (single jobs/pipelines).
    # Auxiliary tasks are terminated when all primary tasks complete.
    sqlalchemy.Column('is_primary_in_job_group',
                      sqlalchemy.Boolean,
                      server_default=None),
    # Optional plugin-provided override for the user-facing status. The core
    # state machine never reads this column; it always uses `status`. Read
    # paths (status counts, status filter, returned status) may surface this
    # value instead of `status` via the optional `status_expr` seam, so a
    # plugin can present a refined status (e.g. show a still-launching job as
    # PENDING while it waits in an external scheduler queue) without altering
    # the underlying job lifecycle. NULL means "no override".
    sqlalchemy.Column('status_override', sqlalchemy.Text, server_default=None),
)
sqlalchemy.Index('ix_spot_job_task', spot_table.c.spot_job_id,
                 spot_table.c.task_id)

job_info_table = sqlalchemy.Table(
    'job_info',
    Base.metadata,
    sqlalchemy.Column('spot_job_id',
                      sqlalchemy.Integer,
                      primary_key=True,
                      autoincrement=True),
    sqlalchemy.Column('name', sqlalchemy.Text),
    sqlalchemy.Column('schedule_state', sqlalchemy.Text),
    sqlalchemy.Column('controller_pid', sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('controller_pid_started_at',
                      sqlalchemy.Float,
                      server_default=None),
    # Durable owner of the API controller generation that assigned the
    # pod-local controller PID. NULL is the compatibility all-role format.
    sqlalchemy.Column('controller_instance_id',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('controller_generation',
                      sqlalchemy.BigInteger,
                      server_default=None),
    sqlalchemy.Column('dag_yaml_path', sqlalchemy.Text),
    sqlalchemy.Column('env_file_path', sqlalchemy.Text),
    sqlalchemy.Column('dag_yaml_content', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('env_file_content', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('config_file_content',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('user_hash', sqlalchemy.Text),
    sqlalchemy.Column('workspace', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('priority',
                      sqlalchemy.Integer,
                      server_default=str(constants.DEFAULT_PRIORITY)),
    sqlalchemy.Column('priority_class', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('entrypoint', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('original_user_yaml_path',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('original_user_yaml_content',
                      sqlalchemy.Text,
                      server_default=None),
    # Indexed: every per-pool dashboard query and pool_status request filters
    # by this column. Without an index a job_info table with tens of thousands
    # of (mostly finished) rows turns each pool lookup into a full scan.
    sqlalchemy.Column('pool', sqlalchemy.Text, server_default=None, index=True),
    # Indexed: pool_status fetches per-replica used_by lists by filtering on
    # current_cluster_name; the index keeps that fast when many jobs share
    # the same pool.
    sqlalchemy.Column('current_cluster_name',
                      sqlalchemy.Text,
                      server_default=None,
                      index=True),
    sqlalchemy.Column('job_id_on_pool_cluster',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('pool_hash', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('controller_logs_cleaned_at',
                      sqlalchemy.Float,
                      server_default=None),
    # DAG execution mode: 'parallel' (job group) or 'serial' (pipeline/single)
    sqlalchemy.Column('execution',
                      sqlalchemy.Text,
                      server_default=DagExecution.SERIAL.value),
    # Infrastructure columns for efficient filtering/sorting
    sqlalchemy.Column('cloud', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('region', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('zone', sqlalchemy.Text, server_default=None),
    # Whether this job is a batch coordinator (ds.map()).  Batch jobs are
    # serialized one-at-a-time per pool by the scheduler.
    sqlalchemy.Column('is_batch',
                      sqlalchemy.Boolean,
                      server_default=sqlalchemy.sql.expression.false()),
    # Durable fencing token for the coordinator incarnation that currently
    # owns this Batch job.  Every attempt mutation checks this value so a
    # replacement controller immediately fences its predecessor.
    sqlalchemy.Column('batch_coordinator_token',
                      sqlalchemy.Text,
                      server_default=None),
    # Node names for dashboard display (comma-separated)
    sqlalchemy.Column('node_names', sqlalchemy.Text, server_default=None),
    # In consolidation mode, managed jobs shares the filemount blob managed
    # by API server. This id is a reference to the blob.
    sqlalchemy.Column('file_mounts_blob_id',
                      sqlalchemy.Text,
                      server_default=None),
)
sqlalchemy.Index('ix_job_info_schedule_priority',
                 job_info_table.c.schedule_state,
                 job_info_table.c.priority.desc(),
                 job_info_table.c.spot_job_id.asc())

# Separate table for API access token IDs associated with managed jobs.
# Maps job_id -> token_id for cleanup when the job completes.
api_access_token_table = sqlalchemy.Table(
    'api_access_tokens',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('token_id', sqlalchemy.Text, nullable=False),
)
sqlalchemy.Index('ix_api_access_tokens_token_id',
                 api_access_token_table.c.token_id)

# TODO(cooperc): drop the table in a migration
ha_recovery_script_table = sqlalchemy.Table(
    'ha_recovery_script',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('script', sqlalchemy.Text),
)

job_events_table = sqlalchemy.Table(
    'job_events',
    Base.metadata,
    sqlalchemy.Column('id',
                      sqlalchemy.Integer,
                      primary_key=True,
                      autoincrement=True),
    # See comment above for explanation of the legacy spot_job_id and
    # task_id columns.
    sqlalchemy.Column('spot_job_id', sqlalchemy.Integer, index=True),
    sqlalchemy.Column('task_id', sqlalchemy.Integer, index=True),
    sqlalchemy.Column('new_status', sqlalchemy.Text),
    sqlalchemy.Column('code', sqlalchemy.Text),
    sqlalchemy.Column('reason', sqlalchemy.Text),
    sqlalchemy.Column('timestamp',
                      sqlalchemy.DateTime(timezone=True),
                      index=True),
)

batch_state_table = sqlalchemy.Table(
    'batch_state',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('batch_idx', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('start_idx', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('end_idx', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('status',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='PENDING'),
    sqlalchemy.Column('worker_cluster', sqlalchemy.Text),
    sqlalchemy.Column('retry_count',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    # Monotonically increasing fencing token.  Every successful claim gets a
    # new value; state transitions from an older controller incarnation are
    # rejected once a newer attempt has claimed the batch.
    sqlalchemy.Column('attempt_id',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    # Coordinator incarnation that claimed the current attempt.  This remains
    # set after the attempt leaves DISPATCHED so replacement coordinators can
    # identify exactly which token-scoped worker services may be stale.
    sqlalchemy.Column('attempt_owner_token', sqlalchemy.Text),
    sqlalchemy.Column('lease_expires_at', sqlalchemy.Float),
    # Earliest wall-clock time at which a failed batch may be claimed again.
    # Persisting this makes retry backoff survive controller restarts.
    sqlalchemy.Column('next_retry_at', sqlalchemy.Float),
    sqlalchemy.Column('updated_at', sqlalchemy.Float),
    sqlalchemy.PrimaryKeyConstraint('job_id', 'batch_idx'),
)

# Durable launch intents for long-running Batch worker services.  The row is
# inserted before the external ``sdk.exec`` call, then filled with the request
# ID and exact worker job ID as they become available.  This bridges worker
# launches that happen before any batch attempt is claimed and lets a later
# coordinator clean only the exact external job created by an older one.
batch_worker_table = sqlalchemy.Table(
    'batch_worker',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('coordinator_token', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('worker_cluster', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('worker_job_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('launch_request_id', sqlalchemy.Text),
    sqlalchemy.Column('worker_job_id', sqlalchemy.Integer),
    sqlalchemy.Column('updated_at', sqlalchemy.Float),
    sqlalchemy.PrimaryKeyConstraint('job_id', 'coordinator_token',
                                    'worker_cluster'),
)
