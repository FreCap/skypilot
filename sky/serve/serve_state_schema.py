"""Shared SQLAlchemy schema and bootstrap for SkyServe state."""
import json

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import compiler as sqlalchemy_compiler
from sqlalchemy.ext import declarative

from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import resource_action_m4_state_schema
from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

Base = declarative.declarative_base()


class _ControllerIncarnationDefault(sqlalchemy.sql.expression.FunctionElement):
    """Dialect-native UUID default for the canonical current metadata."""

    inherit_cache = True
    type = sqlalchemy.Uuid(as_uuid=True)


@sqlalchemy_compiler.compiles(_ControllerIncarnationDefault, 'postgresql')
def _compile_controller_incarnation_postgres(_element, _compiler, **_kwargs):
    return 'gen_random_uuid()'


@sqlalchemy_compiler.compiles(_ControllerIncarnationDefault, 'sqlite')
def _compile_controller_incarnation_sqlite(_element, _compiler, **_kwargs):
    # Local SQLite is physically capped before Serve042, but unit tests build
    # the canonical metadata directly.  Keep that graph insertable without a
    # client-side default, which would leak this future column into statements
    # against historical schemas.
    return ("(lower(hex(randomblob(4))) || '-' || "
            "lower(hex(randomblob(2))) || '-4' || "
            "substr(lower(hex(randomblob(2))), 2) || '-' || "
            "substr('89ab', (random() & 3) + 1, 1) || "
            "substr(lower(hex(randomblob(2))), 2) || '-' || "
            "lower(hex(randomblob(6))))")


# === Database schema ===
services_table = sqlalchemy.Table(
    'services',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
    # Durable user workspace for every replica launch and recovery. The
    # controller itself may run in the system/default workspace.
    sqlalchemy.Column('workspace', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('controller_job_id',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('controller_port',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('load_balancer_port',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('status', sqlalchemy.Text),
    sqlalchemy.Column('uptime', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('policy', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('auto_restart', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('requested_resources',
                      sqlalchemy.LargeBinary,
                      server_default=None),
    sqlalchemy.Column('requested_resources_str', sqlalchemy.Text),
    sqlalchemy.Column('current_version',
                      sqlalchemy.Integer,
                      server_default=str(constants.INITIAL_VERSION)),
    sqlalchemy.Column('active_versions',
                      sqlalchemy.Text,
                      server_default=json.dumps([])),
    sqlalchemy.Column('load_balancing_policy',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('tls_encrypted', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('pool', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('controller_pid', sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('hash', sqlalchemy.Text, server_default=None),
    # Serve resource actions are activated explicitly and monotonically.  The
    # legacy default keeps both existing rows and local SQLite Serve databases
    # inert until the PostgreSQL-only action helpers promote a service.
    sqlalchemy.Column('resource_action_mode',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='legacy'),
    sqlalchemy.Column('resource_action_mode_changed_at',
                      sqlalchemy.DateTime(timezone=True),
                      server_default=None),
    *resource_action_m4_state_schema.service_candidate_columns(),
    # Monotonic name-fence token claimed by the lifecycle operation that most
    # recently owns this row.  Unlike ``hash`` (which changes only when the
    # service is recreated), this advances on every up/update/down/purge lock
    # acquisition.  Destructive commits validate both values.
    sqlalchemy.Column('lifecycle_epoch',
                      sqlalchemy.Integer,
                      server_default=None),
    # External resource namespace for this incarnation.  New rows store their
    # service hash here; NULL identifies a legacy row whose files, clusters,
    # and LB objects predate incarnation-scoped names.  Keeping the distinction
    # durable lets a same-name successor use a disjoint namespace without
    # moving live legacy resources during a rolling upgrade.
    sqlalchemy.Column('resource_scope', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('entrypoint', sqlalchemy.Text, server_default=None),
    # Pod IP where the controller process is running.
    # Written by the sky.serve.service process at startup.
    sqlalchemy.Column('controller_ip', sqlalchemy.Text, server_default=None),
    # ABA-safe owner identity for durable ordinary-launch request binding.
    # PID/IP remain routing metadata; every controller takeover installs a
    # fresh incarnation and advances the owner epoch atomically in Serve042.
    sqlalchemy.Column(
        'controller_incarnation',
        sqlalchemy.Uuid(as_uuid=True),
        nullable=False,
        # A server default mirrors Serve042 without injecting
        # this future column into historical INSERT statements.
        server_default=_ControllerIncarnationDefault()),
    sqlalchemy.Column('controller_owner_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='1'),
    # Capability is bound to the exact controller incarnation above.  Existing
    # services remain dark until a capable subprocess explicitly promotes the
    # non-pool service from legacy to bound mode.
    sqlalchemy.Column('ordinary_launch_binding_capable',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('ordinary_launch_binding_mode',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='legacy'),
    sqlalchemy.Column('ordinary_launch_binding_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    # A placement normalization updates persisted representation without
    # changing service semantics.  The requested run fences controller reload;
    # the remaining fields are the durable receipt written only after that
    # controller has loaded and validated the normalized generation.
    sqlalchemy.Column('placement_normalization_requested_run_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      sqlalchemy.ForeignKey(
                          'placement_normalization_runs.run_id',
                          name=('fk_services_placement_normalization_'
                                'requested_run'),
                          ondelete='RESTRICT'),
                      server_default=None),
    sqlalchemy.Column('placement_normalization_loaded_run_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      sqlalchemy.ForeignKey(
                          'placement_normalization_runs.run_id',
                          name=('fk_services_placement_normalization_'
                                'loaded_run'),
                          ondelete='RESTRICT'),
                      server_default=None),
    sqlalchemy.Column('placement_normalization_loaded_image_commit',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('placement_normalization_loaded_controller_pid',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('placement_normalization_loaded_controller_ip',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('placement_normalization_loaded_boot_id',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('placement_normalization_loaded_at',
                      sqlalchemy.Float,
                      server_default=None),
    # Durable one-way activation fence. Logical per-GPU semantics may be
    # enabled by an update, but cannot safely be changed back to physical
    # backend counts in place. This parent-row bit makes that rule atomic with
    # a version commit and survives controller restarts/version retirement.
    sqlalchemy.Column('logical_replica_semantics',
                      sqlalchemy.Integer,
                      server_default='0'),
    # Controller-fenced warm-standby authority. External LB HA is supported
    # only on the central PostgreSQL Serve database. Existing service rows keep
    # the disabled default until an explicit migration enables the new mode.
    sqlalchemy.Column('lb_ha_enabled',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('lb_active_slot', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('lb_cutover_generation',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('lb_pending_slot', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('lb_cutover_phase',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=lb_ha.LbCutoverPhase.STABLE.value),
    sqlalchemy.Column('lb_drain_started_at', sqlalchemy.Float),
    sqlalchemy.Column('lb_demand_handoff_generation', sqlalchemy.Integer),
    sqlalchemy.Column('lb_demand_handoff_snapshot', sqlalchemy.Text),
    sqlalchemy.Column('lb_demand_handoff_complete_at', sqlalchemy.Float),
    # Latest demand reported by the selected ACTIVE slot. This is independent
    # from an in-progress handoff so a controller restart before PREPARING
    # cannot erase the scale-down floor copied into the next cutover.
    sqlalchemy.Column('lb_last_demand_snapshot', sqlalchemy.Text),
    # Controller-owned placement policy state. Separate columns prevent the
    # replica-manager failure refresher and autoscaler loop from clobbering
    # each other's restart evidence.
    sqlalchemy.Column(
        'spot_placement_state',
        sqlalchemy.JSON(none_as_null=True).with_variant(
            postgresql.JSONB(none_as_null=True), 'postgresql')),
    sqlalchemy.Column(
        'cost_rebalance_state',
        sqlalchemy.JSON(none_as_null=True).with_variant(
            postgresql.JSONB(none_as_null=True), 'postgresql')),
)

replicas_table = sqlalchemy.Table(
    'replicas',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('replica_info', sqlalchemy.LargeBinary),
    sqlalchemy.Column('replica_state_version', sqlalchemy.Integer),
    sqlalchemy.Column('status', sqlalchemy.Text),
    sqlalchemy.Column('sky_down_status', sqlalchemy.Text),
    sqlalchemy.Column('version', sqlalchemy.Integer),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text),
    sqlalchemy.Column('created_at', sqlalchemy.Float),
    sqlalchemy.Column('is_spot', sqlalchemy.Boolean),
    sqlalchemy.Column('paid_capacity_pool_key',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column(
        'replica_state',
        sqlalchemy.JSON().with_variant(postgresql.JSONB(), 'postgresql')),
    # Neutral request association for the ordinary launch path.  Generic
    # ReplicaInfo persistence omits this scalar so old writers cannot erase a
    # durable binding by rewriting the JSON payload.
    sqlalchemy.Column('ordinary_launch_association_id',
                      sqlalchemy.Uuid(as_uuid=True)),
    # These columns are initialized and mutated only by typed resource-action
    # transitions.  Generic ReplicaInfo persistence deliberately omits them.
    # sqlalchemy.Uuid is native UUID on PostgreSQL and a portable CHAR-backed
    # UUID on SQLite, keeping the common metadata graph dialect-safe.
    sqlalchemy.Column('replica_incarnation', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('desired_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('sky_cluster_record_uuid', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('launch_action_id', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('down_action_id', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('launch_shadow_coverage_id',
                      sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('down_shadow_coverage_id', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('launch_shadow_sample_id', sqlalchemy.Uuid(as_uuid=True)),
    sqlalchemy.Column('down_shadow_sample_id', sqlalchemy.Uuid(as_uuid=True)),
    *resource_action_m4_state_schema.replica_spec_identity_columns(),
)
sqlalchemy.Index('replicas_service_status_idx', replicas_table.c.service_name,
                 replicas_table.c.status)
sqlalchemy.Index('replicas_service_version_idx', replicas_table.c.service_name,
                 replicas_table.c.version)

version_specs_table = sqlalchemy.Table(
    'version_specs',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
    sqlalchemy.Column('yaml_content', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('submitted_yaml_content',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('created_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('created_by', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('quarantined_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('quarantine_reason', sqlalchemy.Text,
                      server_default=None),
    # Historical retirement preserves operator-readable YAML separately while
    # removing the row from every live committed-version query.  The CHECK
    # below prevents partially written retirement evidence.
    sqlalchemy.Column('retired_yaml_content',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('retired_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('retirement_reason', sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('retirement_run_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      sqlalchemy.ForeignKey(
                          'placement_normalization_runs.run_id',
                          name='fk_version_specs_retirement_run',
                          ondelete='RESTRICT'),
                      server_default=None),
    sqlalchemy.Column('placement_catalog',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      server_default=None),
    # Sanitized, workspace-scoped controller configuration is versioned with
    # the service spec.  Recovery must select the config belonging to the
    # elected version rather than reading a singleton from the HA script.
    sqlalchemy.Column('controller_config',
                      sqlalchemy.LargeBinary,
                      server_default=None),
    sqlalchemy.Column('controller_config_digest',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('controller_config_snapshot_id',
                      sqlalchemy.Text,
                      server_default=None),
    # The first successful controller transition records this once.  Unlike
    # active_versions, this survives scale-to-zero and is therefore suitable
    # for choosing a proven generation after a newer version is quarantined.
    sqlalchemy.Column('controller_applied_at',
                      sqlalchemy.Float,
                      server_default=None),
    sqlalchemy.Column('controller_job_projection',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      server_default=None),
    sqlalchemy.Column('controller_work_cache',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      server_default=None),
    sqlalchemy.Column('worker_placement_projections',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      server_default=None),
    *resource_action_m4_state_schema.version_spec_identity_columns(),
    sqlalchemy.CheckConstraint(
        '((retired_at IS NULL AND retired_yaml_content IS NULL AND '
        'retirement_reason IS NULL AND retirement_run_id IS NULL) OR '
        '(retired_at IS NOT NULL AND yaml_content IS NULL AND '
        'retired_yaml_content IS NOT NULL AND retirement_reason IS NOT NULL '
        'AND retirement_run_id IS NOT NULL))',
        name='ck_version_specs_retirement_all_or_none'),
)

# One immutable manifest per successfully committed normalization phase.  Raw
# specs and YAML are intentionally absent: the manifest and row inventory keep
# only canonical digests and non-secret contract/dependency projections.
placement_normalization_runs_table = sqlalchemy.Table(
    'placement_normalization_runs',
    Base.metadata,
    sqlalchemy.Column('run_id', sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('mode', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('normalizer_version', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('schema_revision', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('release_version', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('started_at', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('completed_at', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('row_bound', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('row_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('classification_counts',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      nullable=False),
    sqlalchemy.Column('pre_inventory_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('post_inventory_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('freeze_evidence_sha256', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.CheckConstraint(
        "mode IN ('apply_supported', 'retire_terminal_historical')",
        name='ck_placement_normalization_run_mode'),
    sqlalchemy.CheckConstraint('completed_at >= started_at',
                               name='ck_placement_normalization_run_times'),
    sqlalchemy.CheckConstraint(
        'row_bound >= 0 AND row_count >= 0 AND row_count <= row_bound',
        name='ck_placement_normalization_run_row_bound'),
    sqlalchemy.CheckConstraint(
        'length(pre_inventory_sha256) = 64 AND '
        'length(post_inventory_sha256) = 64 AND '
        'length(freeze_evidence_sha256) = 64',
        name='ck_placement_normalization_run_digests'),
)

placement_normalization_rows_table = sqlalchemy.Table(
    'placement_normalization_rows',
    Base.metadata,
    sqlalchemy.Column('run_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      sqlalchemy.ForeignKey(
                          'placement_normalization_runs.run_id',
                          name='fk_placement_normalization_rows_run',
                          ondelete='RESTRICT'),
                      primary_key=True),
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('classification', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('outcome', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('original_spec_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('result_spec_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('original_row_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('result_row_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('original_column_sha256s',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      nullable=False),
    sqlalchemy.Column('result_column_sha256s',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      nullable=False),
    sqlalchemy.Column(
        'contract_projection',
        sqlalchemy.JSON(none_as_null=True).with_variant(
            postgresql.JSONB(none_as_null=True), 'postgresql')),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('dependency_facts',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      nullable=False),
    sqlalchemy.CheckConstraint("outcome IN ('unchanged', 'changed', 'retired')",
                               name='ck_placement_normalization_row_outcome'),
    sqlalchemy.CheckConstraint(
        'length(classification) > 0',
        name='ck_placement_normalization_row_classification'),
    sqlalchemy.CheckConstraint(
        'length(original_spec_sha256) = 64 AND '
        'length(result_spec_sha256) = 64 AND '
        'length(original_row_sha256) = 64 AND '
        'length(result_row_sha256) = 64',
        name='ck_placement_normalization_row_digests'),
)
sqlalchemy.Index('placement_normalization_rows_version_idx',
                 placement_normalization_rows_table.c.service_name,
                 placement_normalization_rows_table.c.version)

# Durable cleanup inventory is intentionally separate from ``version_specs``.
# Version rows are immutable deployment history, while cleanup intents track
# external storage ownership and survive until full service teardown.
ephemeral_storage_cleanup_intents_table = sqlalchemy.Table(
    'ephemeral_storage_cleanup_intents',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('resource_scope', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('storage_generation', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('yaml_content', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('pool', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('lifecycle_epoch', sqlalchemy.Integer, nullable=False),
    # True only until the operation has handed the generation to a committed
    # service/version. Ordinary exceptions may eagerly clean these rows;
    # committed generations remain until full service teardown.
    sqlalchemy.Column('provisional', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('created_at', sqlalchemy.Float, nullable=False),
)

serve_ha_recovery_script_table = sqlalchemy.Table(
    'serve_ha_recovery_script',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('script', sqlalchemy.Text),
)

# Per-name fencing token.  This row deliberately outlives the corresponding
# service row: deleting and recreating a name must advance, never reset, the
# token so an operation whose PostgreSQL advisory-lock session died cannot
# commit after a successor has acquired the name.
service_lifecycle_fences_table = sqlalchemy.Table(
    'service_lifecycle_fences',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('epoch', sqlalchemy.Integer, nullable=False),
)

# [boltz fork] Reserved-fill broker state (multi-service arbitration of the
# zero-cost fill pools; see sky/serve/reserved_capacity_broker.py). One claim
# row per fill-enabled service, upserted by its controller's capacity poller
# every poll interval (the heartbeat). Only FILL holdings are reported: they
# are broker property (arbitrated by grants); demand-placed zero-cost
# replicas are demand-protected, exempt from the grant ceiling, and derived
# from live replica rows where needed.
reserved_fill_claims_table = sqlalchemy.Table(
    'reserved_fill_claims',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    # json.dumps([kubernetes_context, gpu_name_lower]): the pool identity two
    # services collide on. Deliberately NOT full Location equality --
    # differing image_id/disk_tier/zone must still collide.
    sqlalchemy.Column('pool_key', sqlalchemy.Text),
    sqlalchemy.Column('weight', sqlalchemy.Float),
    sqlalchemy.Column('floor_replicas', sqlalchemy.Integer),
    # v1 requires all claimants of a pool to agree on this (mixed pools are
    # rejected); GPU-unit bookkeeping is v2.
    sqlalchemy.Column('gpus_per_replica', sqlalchemy.Integer),
    sqlalchemy.Column('holdings_fill', sqlalchemy.Integer),
    # Real capacity cap the claimant can materialize right now
    # (max(0, max_replicas - demand_target)); NULL = unbounded. The broker
    # clamps the effective floor, the headroom (weighted share above the
    # floor, derived at allocation time) and the feed need by it, so an
    # unattainable floor cannot permanently absorb entitlement and feed the
    # service never launches (its excess joins the burst remainder).
    sqlalchemy.Column('effective_cap', sqlalchemy.Integer, server_default=None),
    # Whether the claimant can launch on the pool right now (its zero-cost
    # tier is not benched): feeds to un-launchable claimants are wasted for a
    # whole round, so the feed split redistributes them.
    sqlalchemy.Column('launchable', sqlalchemy.Integer, server_default='1'),
    # Utilization gate signal. NULL activity_ts marks a static opt-out (and
    # every pre-030/pre-gate row), while a fresh activity_ts with NULL
    # demonstrated_need marks a current gated writer whose telemetry is blind.
    #
    # demonstrated_need: replicas this claimant can prove it is using right
    # now, fusing in-flight work, queued work, retained rejections, busy
    # fill replicas and fill replicas still booting.
    sqlalchemy.Column('demonstrated_need',
                      sqlalchemy.Integer,
                      server_default=None),
    # boot_hold: the claimant has fill replicas it already authorized still
    # coming up. Blocks a release step so the gate cannot order a fleet,
    # hold it through a 20-minute readiness delay, then cull it mid-boot
    # (pre-ready rows are the FIRST scale-down victims).
    sqlalchemy.Column('boot_hold', sqlalchemy.Integer, server_default=None),
    # activity_ts: when the two columns above were measured. Mandatory
    # anti-skew witness for an armed gate. An old binary's upsert advances
    # heartbeat_ts while leaving these frozen, and a frozen demonstrated_need
    # of 0 would walk a busy service to zero; the broker treats the signal as
    # blind unless heartbeat_ts - activity_ts is within the staleness bound.
    sqlalchemy.Column('activity_ts', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('heartbeat_ts', sqlalchemy.Float),
)

# Durable protocol switch for the reserved-fill broker.  Revision 035 starts
# this singleton at v1; protocol v2 is activated only by an explicit operator
# action after every broker process has been replaced.  Keeping the rollout
# proof beside the switch makes the transition auditable and prevents a
# multi-context service spec from implicitly changing the database protocol.
reserved_fill_protocol_state_table = sqlalchemy.Table(
    'reserved_fill_protocol_state',
    Base.metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('protocol_version',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='1'),
    # Global allocation fence for protocol-v2 claim-set incarnations.  It is
    # deliberately owned by the never-deleted singleton instead of a service
    # row, so disabling or recreating a same-name service cannot reuse a
    # generation carried by an old round or queued launch decision.
    sqlalchemy.Column('claim_generation',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('image_digest', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('deployment_generation',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('deployment_uid', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('pod_inventory_count',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('pod_inventory_sha256',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('changed_at',
                      sqlalchemy.Float,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.CheckConstraint('id = 1',
                               name='ck_reserved_fill_protocol_singleton'),
    sqlalchemy.CheckConstraint('protocol_version IN (1, 2)',
                               name='ck_reserved_fill_protocol_version'),
    sqlalchemy.CheckConstraint('claim_generation >= 0',
                               name='ck_reserved_fill_claim_generation'),
    sqlalchemy.CheckConstraint(
        "((image_digest IS NULL AND deployment_generation IS NULL AND "
        "deployment_uid IS NULL AND pod_inventory_count IS NULL AND "
        "pod_inventory_sha256 IS NULL) OR "
        "(image_digest IS NOT NULL AND deployment_generation IS NOT NULL "
        "AND deployment_uid IS NOT NULL AND pod_inventory_count IS NOT NULL "
        "AND pod_inventory_sha256 IS NOT NULL))",
        name='ck_reserved_fill_protocol_proof_all_or_none'),
    sqlalchemy.CheckConstraint(
        'protocol_version <> 2 OR image_digest IS NOT NULL',
        name='ck_reserved_fill_protocol_v2_has_proof'),
    sqlalchemy.CheckConstraint(
        "image_digest IS NULL OR (length(image_digest) = 71 AND "
        "substr(image_digest, 1, 7) = 'sha256:')",
        name='ck_reserved_fill_protocol_image_digest'),
    sqlalchemy.CheckConstraint(
        'deployment_generation IS NULL OR '
        'length(deployment_generation) > 0',
        name='ck_reserved_fill_protocol_deployment_generation'),
    sqlalchemy.CheckConstraint(
        'deployment_uid IS NULL OR length(deployment_uid) > 0',
        name='ck_reserved_fill_protocol_deployment_uid'),
    sqlalchemy.CheckConstraint(
        'pod_inventory_count IS NULL OR pod_inventory_count > 0',
        name='ck_reserved_fill_protocol_pod_inventory_count'),
    sqlalchemy.CheckConstraint(
        'pod_inventory_sha256 IS NULL OR '
        'length(pod_inventory_sha256) = 64',
        name='ck_reserved_fill_protocol_pod_inventory_sha256'),
)

# One authoritative marker per service.  The normalized edge rows below are
# consumable only when all rows match this generation and edge_count.  A
# generation-zero migration_shadow is deliberately inert: v1 reads the legacy
# row, while v2 fails closed until the owning controller atomically adopts it.
reserved_fill_service_claim_sets_table = sqlalchemy.Table(
    'reserved_fill_service_claim_sets',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('claim_set_state',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='migration_shadow'),
    sqlalchemy.Column('generation',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('edge_count',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('semantic_hash', sqlalchemy.Text, server_default=None),
    # Null only for migration shadows and bounded LEGACY_ACTIVE compatibility
    # rows.  A sequenced claim locks one exact immutable version row.
    sqlalchemy.Column('service_version',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('global_headroom',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('utilization_ceiling',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('utilization_state', sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('heartbeat_ts',
                      sqlalchemy.Float,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.CheckConstraint(
        "claim_set_state IN ('migration_shadow', 'authoritative_v2')",
        name='ck_reserved_fill_claim_set_state'),
    sqlalchemy.CheckConstraint('generation >= 0',
                               name='ck_reserved_fill_claim_set_generation'),
    sqlalchemy.CheckConstraint('edge_count >= 0',
                               name='ck_reserved_fill_claim_set_edge_count'),
    sqlalchemy.CheckConstraint(
        'service_version IS NULL OR service_version > 0',
        name='ck_reserved_fill_claim_set_service_version'),
    sqlalchemy.CheckConstraint(
        'global_headroom IS NULL OR global_headroom >= 0',
        name='ck_reserved_fill_claim_set_headroom'),
    sqlalchemy.CheckConstraint(
        'utilization_ceiling IS NULL OR utilization_ceiling >= 0',
        name='ck_reserved_fill_claim_set_utilization'),
)

# Protocol-v2 normalized claim edges.  ``pool_key`` is the versioned physical
# UID key used by v2 rounds; ``legacy_pool_key`` retains the context-based v1
# identity used only by the stable rollback projection.  Activity columns are
# retained for a lossless migration shadow but authoritative v2 writers keep
# them NULL: the one utilization governor lives on the service-set row.
reserved_fill_pool_claims_table = sqlalchemy.Table(
    'reserved_fill_pool_claims',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('pool_key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('legacy_pool_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('pool_position', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('access_context', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('physical_cluster_uid',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('accelerator_names', sqlalchemy.Text,
                      server_default=None),
    # One exact v2 worker-projection digest per case-folded accelerator in the
    # edge.  Null belongs only to migration/LEGACY_ACTIVE compatibility state;
    # sequenced readers validate the closed map against accelerator_names.
    sqlalchemy.Column('worker_projection_sha256_by_accelerator',
                      sqlalchemy.JSON(none_as_null=True).with_variant(
                          postgresql.JSONB(none_as_null=True), 'postgresql'),
                      server_default=None),
    sqlalchemy.Column('service_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('weight', sqlalchemy.Float),
    sqlalchemy.Column('floor_replicas', sqlalchemy.Integer),
    sqlalchemy.Column('gpus_per_replica', sqlalchemy.Integer),
    sqlalchemy.Column('holdings_fill', sqlalchemy.Integer),
    sqlalchemy.Column('effective_cap', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('launchable', sqlalchemy.Integer, server_default='1'),
    sqlalchemy.Column('demonstrated_need',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('boot_hold', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('activity_ts', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('heartbeat_ts', sqlalchemy.Float, nullable=False),
    sqlalchemy.CheckConstraint('pool_position >= 0',
                               name='ck_reserved_fill_pool_position'),
    sqlalchemy.CheckConstraint('service_generation >= 0',
                               name='ck_reserved_fill_pool_generation'),
    sqlalchemy.CheckConstraint('effective_cap IS NULL OR effective_cap >= 0',
                               name='ck_reserved_fill_pool_effective_cap'),
)
sqlalchemy.Index('reserved_fill_pool_claims_pool_idx',
                 reserved_fill_pool_claims_table.c.pool_key)
sqlalchemy.Index('reserved_fill_pool_claims_service_generation_idx',
                 reserved_fill_pool_claims_table.c.service_name,
                 reserved_fill_pool_claims_table.c.service_generation)

# Latest published broker round per pool (overwritten in place each round).
# Grants/feeds are the authoritative allocation record readers act on; the
# remaining columns are the broker's cross-round memory (damping baselines,
# feed stickiness, last good free measurement for blackout handling).
reserved_fill_rounds_table = sqlalchemy.Table(
    'reserved_fill_rounds',
    Base.metadata,
    sqlalchemy.Column('pool_key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('round_id', sqlalchemy.Integer),
    # Taken BEFORE the (slow) cluster query, mirroring the #108
    # snapshot/debit invariant at broker level.
    sqlalchemy.Column('snapshot_time', sqlalchemy.Float),
    # The POOL's fencing epoch: bumps only when this pool's allocation
    # changes. Readers actuating a grant compare their carried epoch
    # against it (see reserved_capacity_broker.current_epoch) -- per-pool,
    # so one pool's grant churn never fences another pool's launches.
    sqlalchemy.Column('epoch', sqlalchemy.Integer),
    # Protocol and per-service generations are part of the grant fence.  Old
    # rows and old writers resolve to protocol v1 with an empty generation map.
    sqlalchemy.Column('protocol_version',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('claim_generations',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='{}'),
    # JSON {service: grant}; null grant = single-claimant fast path (no
    # ceiling, #108 identity).
    sqlalchemy.Column('grants', sqlalchemy.Text),
    # JSON {service: feed}; sum(feeds) <= observed free by construction.
    sqlalchemy.Column('feeds', sqlalchemy.Text),
    # JSON {service: {accelerator: feed}} when the pool query returned an
    # exact-card split.  NULL preserves compatibility with rounds written
    # before exact-card publication (or providers that cannot report it).
    # For a non-NULL value, each service's card counts sum to at most its
    # aggregate feed and pool-wide card counts never exceed the measurement.
    sqlalchemy.Column('feed_by_accelerator', sqlalchemy.Text),
    # JSON {service: raw undamped entitlement} of THIS round; next round's
    # damping baseline (a move must persist across two rounds to apply).
    sqlalchemy.Column('raw_grants', sqlalchemy.Text),
    # JSON {service: {'amount': int, 'since': ts}}: sticky feed assignments.
    sqlalchemy.Column('feed_state', sqlalchemy.Text),
    # JSON {service: {'cap': int, 'hot_until': ts, 'stepped_at': ts,
    # 'blind_since': ts|null}}: the utilization gate's durable release
    # target. On the round row rather than in controller memory because
    # every serve controller is a process inside the api-server pod, so a
    # routine deploy restarts all of them at once and an in-memory decay
    # would reset pool-wide on every deploy. Written under the same lease
    # CAS as grants/feeds; a pre-gate binary's publish omits it from its
    # values dict, so a mixed-version round leaves the state untouched.
    sqlalchemy.Column('utilization_state', sqlalchemy.Text),
    # Conserved fill holdings (live + draining) at the last MEASURED round
    # (blackout rounds carry it unchanged, staying transparent to the
    # shrink confirmation): a confirmed shrink means pods are physically
    # gone, making grant down-moves immediate (no damping).
    sqlalchemy.Column('sum_holdings', sqlalchemy.Integer),
    # Last SUCCESSFULLY measured free level + its timestamp (carried
    # unchanged through measurement blackouts, which also carry the grants
    # instead of recomputing -- a blackout must not trigger releases).
    sqlalchemy.Column('last_observed_free', sqlalchemy.Integer),
    sqlalchemy.Column('last_observed_free_ts', sqlalchemy.Float),
    # Consecutive phantom observations (successful query, no labeled nodes
    # for the claimed GPU). Persisted so the consecutive-phantom claim
    # rejection gate survives writer rotation; a non-phantom observation
    # resets it to 0.
    sqlalchemy.Column('phantom_streak', sqlalchemy.Integer, server_default='0'),
    # Pre-shrink conserved-holdings baseline of an UNCONFIRMED shrink seen
    # last round (NULL = none pending). A conserved-total shrink only
    # bypasses grant damping once it persists across two consecutive
    # rounds: a drain completing between the cluster query and the row
    # scan makes both terms omit the slot for exactly one round, and
    # firing the bypass on that phantom shrink culls a warm replica.
    sqlalchemy.Column('shrink_baseline',
                      sqlalchemy.Integer,
                      server_default=None),
    # Dead-gap fence marker: set (for every pool) atomically with a
    # POST-EXPIRY lease-token acquisition and cleared only by a successful
    # publish, which is forced to bump this pool's epoch while the marker
    # is set. Without it, a post-expiry writer that acquired its token
    # (committing a fresh expires_at) and died before publishing would
    # leave the NEXT writer seeing an unexpired lease -- with unchanged
    # grants/feeds it would republish the old epoch and launches queued
    # before the dead gap would keep passing the fence unrevalidated.
    # While set, actuation fails CLOSED: the launch fence reads it as
    # never-matching (reserved_capacity_broker.current_epoch) and the
    # atomic persist refuses (add_replica_if_round_epoch), so a pool that
    # never publishes again (claims gone) cannot leak a pre-gap launch.
    sqlalchemy.Column('fence_pending', sqlalchemy.Integer, server_default='0'),
)

# Singleton lease row (id=1). The epoch only moves forward. It is the round
# writer's OWNERSHIP TOKEN and the round's ENTRY POINT: CAS-advanced (and
# committed) BEFORE the writer reads any claim/round state and before its
# slow cluster query, and the publish only lands while the lease still holds
# that exact token. Fill persists also advance it on the exact advisory-lock
# session and validate it in the replica insert transaction. A replacement
# round advances the same epoch before scanning, so silent advisory-session
# loss cannot place a stale persist inside the scan-to-publish window (see
# advance_reserved_fill_persist_token and acquire_reserved_fill_lease_token).
# A replacement writer after a lost round likewise advances it, so the stale
# publish fails and its observation is discarded.
# Fencing for actuation is the per-pool round epoch above.
reserved_fill_lease_table = sqlalchemy.Table(
    'reserved_fill_lease',
    Base.metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('epoch', sqlalchemy.Integer),
    sqlalchemy.Column('expires_at', sqlalchemy.Float),
)

# Shared raw Kubernetes accelerator observations used by demand placement.
# One row per context lets every service/controller reuse the same expensive
# cluster-wide query. ``availability`` is JSON {gpu_name_lower: free_gpus};
# NULL records a failed query and rate-limits retry storms while preserving
# the important distinction from a successful empty/zero observation.
# ``snapshot_time`` is the query start used to debit replicas that raced the
# observation; ``completed_at`` is the freshness/rate-limit clock, so a slow
# query does not publish a result that is immediately stale.
demand_capacity_observations_table = sqlalchemy.Table(
    'demand_capacity_observations',
    Base.metadata,
    sqlalchemy.Column('context', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('snapshot_time', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('completed_at', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('availability', sqlalchemy.Text, server_default=None),
)

# Global paid provider-pool admission. The pool row serializes claims from
# independent service controllers. Claims are durable only while their
# corresponding replica remains PENDING or PROVISIONING.
paid_capacity_pools_table = sqlalchemy.Table(
    'paid_capacity_pools',
    Base.metadata,
    sqlalchemy.Column('pool_key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('current_limit', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('successes_since_resize',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('last_success_at', sqlalchemy.Float),
    sqlalchemy.Column('last_failure_at', sqlalchemy.Float),
    sqlalchemy.Column('updated_at', sqlalchemy.Float, nullable=False),
)

paid_capacity_claims_table = sqlalchemy.Table(
    'paid_capacity_claims',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('pool_key',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey('paid_capacity_pools.pool_key',
                                            ondelete='CASCADE'),
                      nullable=False),
    sqlalchemy.Column('priority', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('claimed_at', sqlalchemy.Float, nullable=False),
)
sqlalchemy.Index('paid_capacity_claims_pool_idx',
                 paid_capacity_claims_table.c.pool_key)

paid_capacity_waiters_table = sqlalchemy.Table(
    'paid_capacity_waiters',
    Base.metadata,
    sqlalchemy.Column('pool_key',
                      sqlalchemy.Text,
                      sqlalchemy.ForeignKey('paid_capacity_pools.pool_key',
                                            ondelete='CASCADE'),
                      primary_key=True),
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('priority', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('first_wait_at', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('heartbeat_at', sqlalchemy.Float, nullable=False),
)
sqlalchemy.Index('paid_capacity_waiters_pool_idx',
                 paid_capacity_waiters_table.c.pool_key)


def create_table(engine: sqlalchemy.engine.Engine):
    """Creates the service and replica tables if they do not exist."""

    # Enable WAL mode to avoid locking issues.
    # See: issue #3863, #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value and
            not common_utils.is_wsl()):
        try:
            with orm.Session(engine) as session:
                session.execute(sqlalchemy.text('PRAGMA journal_mode=WAL'))
                session.commit()
        except sqlalchemy_exc.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.SERVE_DB_NAME,
        migration_utils.serve_target_version(engine),
        mode=migration_utils.configured_migration_mode())


_db_manager = db_utils.DatabaseManager('serve/services', create_table)


def ensure_tables_initialized() -> None:
    """Run pending Serve DB migrations before raw lock-session SQL."""
    _db_manager.get_engine()


def get_database_engine() -> sqlalchemy.engine.Engine:
    """Return the initialized database engine for Serve state."""
    return _db_manager.get_engine()


# Preserve the historical public identity exposed by sky.serve.serve_state.
create_table.__module__ = 'sky.serve.serve_state'
ensure_tables_initialized.__module__ = 'sky.serve.serve_state'
get_database_engine.__module__ = 'sky.serve.serve_state'
