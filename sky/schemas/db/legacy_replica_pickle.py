"""Frozen pre-JSON SkyServe replica migration boundary.

This module is migration infrastructure, not a runtime compatibility API.
Revision 010 converts the original pre-JSON fleet through ReplicaInfo v7;
revision 026 also repairs predecessor-stamped preview databases whose retained
pickle writer reached v11. Keep both exact boundaries independent of the live
Serve classes so future removal of runtime legacy decoders cannot break a fresh
database replay.
"""

import io
import math
import pickle
from typing import Any
import uuid

_CURRENT_JSON_VERSION = 18
_REPLICA_RECORD_ID_TRANSITION_NAMESPACE = uuid.UUID(
    '3b448973-9e2f-58aa-a640-27fb7c6a8884')
_PROCESS_STATUSES = frozenset(
    ('SCHEDULED', 'RUNNING', 'SUCCEEDED', 'INTERRUPTED', 'FAILED'))


class LegacyReplicaPickleError(RuntimeError):
    """A historical replica pickle is outside the frozen migration contract."""


class _FrozenReplicaInfo:
    """State-only replacement for the historical live ReplicaInfo class."""

    def __setstate__(self, state: Any) -> None:
        if not isinstance(state, dict):
            raise LegacyReplicaPickleError(
                'Legacy ReplicaInfo pickle state must be a dict.')
        self.__dict__.update(state)


class _FrozenReplicaStatusProperty:
    """State-only replacement for the historical status-property class."""

    def __setstate__(self, state: Any) -> None:
        if not isinstance(state, dict):
            raise LegacyReplicaPickleError(
                'Legacy ReplicaStatusProperty pickle state must be a dict.')
        self.__dict__.update(state)


class _FrozenProcessStatus:
    """State-only replacement for the historical ProcessStatus enum."""

    def __init__(self, value: str) -> None:
        if value not in _PROCESS_STATUSES:
            raise LegacyReplicaPickleError(
                'Legacy ReplicaInfo contains an unsupported process status.')
        self.value = value


class _FrozenCloud:
    """State-only historical cloud identity used inside resource overrides."""

    _display_name = ''

    def __setstate__(self, state: Any) -> None:
        if not isinstance(state, dict):
            raise LegacyReplicaPickleError(
                'Legacy cloud pickle state must be a dict.')

    def __str__(self) -> str:
        return self._display_name


def _frozen_cloud_type(display_name: str) -> type[_FrozenCloud]:
    return type(f'_Frozen{display_name}Cloud', (_FrozenCloud,), {
        '_display_name': display_name,
    })


_FROZEN_CLOUD_IDENTITIES = {
    identity: _frozen_cloud_type(display_name) for identity, display_name in {
        ('sky.clouds.ibm', 'IBM'): 'IBM',
        ('sky.clouds.aws', 'AWS'): 'AWS',
        ('sky.clouds.azure', 'Azure'): 'Azure',
        ('sky.clouds.cudo', 'Cudo'): 'Cudo',
        ('sky.clouds.do', 'DO'): 'DO',
        ('sky.clouds.fluidstack', 'Fluidstack'): 'Fluidstack',
        ('sky.clouds.gcp', 'GCP'): 'GCP',
        ('sky.clouds.hyperbolic', 'Hyperbolic'): 'Hyperbolic',
        ('sky.clouds.kubernetes', 'Kubernetes'): 'Kubernetes',
        ('sky.clouds.lambda_cloud', 'Lambda'): 'Lambda',
        ('sky.clouds.mithril', 'Mithril'): 'Mithril',
        ('sky.clouds.nebius', 'Nebius'): 'Nebius',
        ('sky.clouds.oci', 'OCI'): 'OCI',
        ('sky.clouds.paperspace', 'Paperspace'): 'Paperspace',
        ('sky.clouds.primeintellect', 'PrimeIntellect'): 'PrimeIntellect',
        ('sky.clouds.runpod', 'RunPod'): 'RunPod',
        ('sky.clouds.scp', 'SCP'): 'SCP',
        ('sky.clouds.seeweb', 'Seeweb'): 'Seeweb',
        ('sky.clouds.shadeform', 'Shadeform'): 'Shadeform',
        ('sky.clouds.slurm', 'Slurm'): 'Slurm',
        ('sky.clouds.ssh', 'SSH'): 'SSH',
        ('sky.clouds.vast', 'Vast'): 'Vast',
        ('sky.clouds.verda', 'Verda'): 'Verda',
        ('sky.clouds.vsphere', 'Vsphere'): 'vSphere',
        ('sky.clouds.yotta', 'Yotta'): 'Yotta',
    }.items()
}


class _MigrationUnpickler(pickle.Unpickler):
    """Redirect live replica identities to migration-owned state holders."""

    _FROZEN_IDENTITIES = {
        ('sky.serve.replica_managers', 'ReplicaInfo'): _FrozenReplicaInfo,
        ('sky.serve.replica_managers', 'ReplicaStatusProperty'): _FrozenReplicaStatusProperty,
        # Pickles written before #6666 used this facade-local enum identity.
        ('sky.serve.replica_managers', 'ProcessStatus'): _FrozenProcessStatus,
        ('sky.utils.common_utils', 'ProcessStatus'): _FrozenProcessStatus,
        **_FROZEN_CLOUD_IDENTITIES,
    }

    def find_class(self, module: str, name: str) -> Any:
        frozen = self._FROZEN_IDENTITIES.get((module, name))
        if frozen is not None:
            return frozen
        # Historical replica records need no other executable globals. Never
        # delegate to pickle's importer: a malformed retained row must not turn
        # database contents into code execution during a schema migration.
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo payload contains a forbidden global.')


def load_pre_json_replica(payload: bytes) -> _FrozenReplicaInfo:
    """Load one pre-revision-010 pickle without invoking live Serve decoders."""
    if not isinstance(payload, bytes):
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo payload must be bytes.')
    try:
        replica = _MigrationUnpickler(io.BytesIO(payload)).load()
    except LegacyReplicaPickleError:
        raise
    except Exception:
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo payload cannot be decoded.') from None
    if not isinstance(replica, _FrozenReplicaInfo):
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo payload has an unexpected root type.')
    return replica


def _object_state(value: Any, owner: str) -> dict[str, Any]:
    state = getattr(value, '__dict__', None)
    if not isinstance(state, dict):
        raise LegacyReplicaPickleError(f'{owner} state must be a dict.')
    return state


def _legacy_version(state: dict[str, Any], maximum_version: int) -> int:
    version = state.get('_version', -1)
    if version is None:
        version = -1
    if (isinstance(version, bool) or not isinstance(version, int) or
            version > maximum_version):
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo version exceeds this migration boundary.')
    return version


def _required(state: dict[str, Any], field: str) -> Any:
    try:
        return state[field]
    except KeyError as error:
        raise LegacyReplicaPickleError(
            f'Legacy ReplicaInfo is missing {field!r}.') from error


def _process_status_value(value: Any, *, allow_none: bool) -> str | None:
    if value is None and allow_none:
        return None
    raw = getattr(value, 'value', value)
    if not isinstance(raw, str) or raw not in _PROCESS_STATUSES:
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo contains an unsupported process status.')
    return raw


def _encode_resource_state(
        state: dict[str, Any] | None) -> dict[str, Any] | None:
    if state is None:
        return None
    if not isinstance(state, dict):
        raise LegacyReplicaPickleError(
            'Legacy replica location/resource override must be a dict.')
    encoded = dict(state)
    image_id = encoded.get('image_id')
    if isinstance(image_id, dict):
        encoded['image_id'] = [
            [region, image] for region, image in image_id.items()
        ]
    cloud = encoded.get('cloud')
    if cloud is not None and not isinstance(cloud, str):
        encoded['cloud'] = str(cloud)
    return encoded


def _require_json_value(value: Any, *, depth: int = 0) -> None:
    """Reject non-canonical migration output with a controlled error."""
    if depth > 20:
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo contains over-nested JSON state.')
    if value is None or type(value) in (bool, int, str):
        return
    if type(value) is float:
        if math.isfinite(value):
            return
        raise LegacyReplicaPickleError(
            'Legacy ReplicaInfo contains a non-finite JSON number.')
    if type(value) is list:
        for item in value:
            _require_json_value(item, depth=depth + 1)
        return
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise LegacyReplicaPickleError(
                'Legacy ReplicaInfo contains a non-text JSON key.')
        for item in value.values():
            _require_json_value(item, depth=depth + 1)
        return
    raise LegacyReplicaPickleError(
        'Legacy ReplicaInfo contains unsupported JSON state.')


def _transition_record_id(replica_id: Any, cluster_name: Any,
                          created_at: Any) -> str:
    if (isinstance(replica_id, bool) or not isinstance(replica_id, int) or
            replica_id < 0):
        raise LegacyReplicaPickleError(
            'Legacy replica ID must be a nonnegative integer.')
    if not isinstance(cluster_name, str) or not cluster_name:
        raise LegacyReplicaPickleError(
            'Legacy replica cluster name must be nonempty text.')
    if created_at is None:
        created_at_token = 'none'
    elif (isinstance(created_at, bool) or
          not isinstance(created_at, (int, float))):
        raise LegacyReplicaPickleError(
            'Legacy replica creation time must be finite or null.')
    else:
        normalized_created_at = float(created_at)
        if not math.isfinite(normalized_created_at):
            raise LegacyReplicaPickleError(
                'Legacy replica creation time must be finite or null.')
        created_at_token = normalized_created_at.hex()
    identity = (f'{replica_id}:{len(cluster_name)}:{cluster_name}:'
                f'{created_at_token}')
    return str(uuid.uuid5(_REPLICA_RECORD_ID_TRANSITION_NAMESPACE, identity))


def _replica_status(status: dict[str, Any]) -> str:
    launch = _process_status_value(status.get('sky_launch_status'),
                                   allow_none=True)
    down = _process_status_value(status.get('sky_down_status'), allow_none=True)
    user_app_failed = bool(status.get('user_app_failed', False))
    service_ready_now = bool(status.get('service_ready_now', False))
    first_ready_time = status.get('first_ready_time')
    preempted = bool(status.get('preempted', False))
    if launch is None or launch == 'SCHEDULED':
        return 'PENDING'
    if launch == 'RUNNING':
        if down == 'FAILED':
            return 'FAILED_CLEANUP'
        if down == 'SUCCEEDED':
            return 'UNKNOWN'
        return 'PROVISIONING'
    if launch == 'INTERRUPTED':
        return 'SHUTTING_DOWN'
    if down is not None:
        if preempted:
            return 'PREEMPTED'
        if down in ('SCHEDULED', 'RUNNING'):
            return 'SHUTTING_DOWN'
        if down == 'FAILED':
            return 'FAILED_CLEANUP'
        if user_app_failed:
            return 'FAILED'
        if launch == 'FAILED':
            return 'FAILED_PROVISION'
        if first_ready_time is None:
            return 'SHUTTING_DOWN'
        if first_ready_time == -1:
            return 'FAILED_INITIAL_DELAY'
        if not service_ready_now:
            return 'FAILED_PROBING'
        return 'UNKNOWN'
    if launch == 'FAILED' or user_app_failed:
        return 'FAILED_CLEANUP'
    if service_ready_now:
        return 'READY'
    if first_ready_time is not None and first_ready_time >= 0.0:
        return 'NOT_READY'
    return 'STARTING'


def frozen_replica_row_values(replica: Any, *,
                              maximum_version: int) -> dict[str, Any]:
    """Project one genuine pre-JSON replica into the exact v18 JSON shape."""
    state = _object_state(replica, 'Legacy ReplicaInfo')
    version = _legacy_version(state, maximum_version)
    replica_id = _required(state, 'replica_id')
    cluster_name = _required(state, 'cluster_name')
    service_version = _required(state, 'version')
    replica_port = _required(state, 'replica_port')
    created_at = state.get('created_at') if version >= 4 else None
    is_spot = state.get('is_spot', False) if version >= 0 else False
    location = state.get('location') if version >= 1 else None
    resources_override = (state.get('resources_override')
                          if version >= 2 else None)
    reserved_fill = state.get('reserved_fill', False) if version >= 5 else False
    failure_time = state.get('first_consecutive_failure_time')
    if version < 7:
        failure_times = state.get('consecutive_failure_times', ())
        failure_time = failure_times[0] if failure_times else None

    status_object = _required(state, 'status_property')
    status = _object_state(status_object, 'Legacy ReplicaStatusProperty')
    sky_launch_status = _process_status_value(status.get('sky_launch_status'),
                                              allow_none=True)
    sky_down_status = _process_status_value(status.get('sky_down_status'),
                                            allow_none=True)
    planned_capacity = state.get('planned_capacity', 1) if version >= 8 else 1
    if (isinstance(planned_capacity, bool) or
            not isinstance(planned_capacity, int) or planned_capacity < 1):
        raise LegacyReplicaPickleError(
            'Legacy planned capacity must be a positive integer.')
    drain_started_at = status.get('drain_started_at')
    if (isinstance(drain_started_at, bool) or
            not isinstance(drain_started_at, (int, float)) or
            not math.isfinite(drain_started_at) or drain_started_at <= 0):
        drain_started_at = None
    logical_retirement_committed = status.get('logical_retirement_committed')
    if type(logical_retirement_committed) is not bool:
        logical_retirement_committed = None
    replica_state = {
        'replica_info_version': _CURRENT_JSON_VERSION,
        'replica_id': replica_id,
        'cluster_name': cluster_name,
        'version': service_version,
        'replica_port': replica_port,
        'created_at': created_at,
        'replica_record_id': _transition_record_id(replica_id, cluster_name,
                                                   created_at),
        'first_not_ready_time': state.get('first_not_ready_time'),
        'first_consecutive_failure_time': failure_time,
        'status_property': {
            'sky_launch_status': sky_launch_status,
            'user_app_failed': bool(status.get('user_app_failed', False)),
            'service_ready_now': bool(status.get('service_ready_now', False)),
            'first_ready_time': status.get('first_ready_time'),
            'sky_down_status': sky_down_status,
            'is_scale_down': bool(status.get('is_scale_down', False)),
            'preempted': bool(status.get('preempted', False)),
            'purged': bool(status.get('purged', False)),
            'failed_spot_availability': bool(
                status.get('failed_spot_availability', False)),
            'drain_cap_seconds': status.get('drain_cap_seconds'),
            'drain_started_at': drain_started_at,
            'wait_for_idle_before_termination': bool(
                status.get('wait_for_idle_before_termination', False)),
            'logical_retirement_version':
                status.get('logical_retirement_version'),
            'logical_retirement_controller_epoch':
                status.get('logical_retirement_controller_epoch'),
            'logical_retirement_generation':
                status.get('logical_retirement_generation'),
            'logical_retirement_target_capacity':
                status.get('logical_retirement_target_capacity'),
            'logical_retirement_confirmed_generation':
                status.get('logical_retirement_confirmed_generation'),
            'logical_retirement_bounded_deadline':
                status.get('logical_retirement_bounded_deadline') is True,
            'logical_retirement_committed': logical_retirement_committed,
        },
        'is_spot': bool(is_spot),
        'location': _encode_resource_state(location),
        'resources_override': _encode_resource_state(resources_override),
        'planned_capacity': planned_capacity,
        'unknown_capacity_replacement': bool(
            state.get('unknown_capacity_replacement', False))
                                        if version >= 9 else False,
        'logical_bridge_capacity_verified': bool(
            state.get('logical_bridge_capacity_verified', False))
                                            if version >= 10 else False,
        'reserved_fill': bool(reserved_fill),
        'reserved_fill_pool_key': None,
        'reserved_fill_service_generation': None,
        'reserved_fill_physical_cluster_uid': None,
        'reserved_fill_kubernetes_context': None,
        'reserved_fill_allocation_generation': None,
        'reserved_fill_allocation_input_sha256': None,
        'reserved_fill_allocation_claim_generation': None,
        'reserved_fill_reconciliation_gate_generation': None,
        'reserved_fill_reclaim_fleet_bundle_sha256': None,
        'reserved_fill_reclaim_policy_revision': None,
        'reserved_fill_reclaim_provider_inventory_sha256': None,
        'reserved_fill_worker_projection_sha256': None,
        'reserved_fill_observation_generation': None,
        'reserved_fill_observation_sequence': None,
        'reserved_fill_intent_idempotency_key': None,
        'zero_cost_admission_sequence': None,
        'zero_cost_materialization_sequence': None,
        'is_zero_cost': bool(state.get('is_zero_cost', False))
                        if version >= 11 else False,
        'cost_rebalance_for_replica_id':
            state.get('cost_rebalance_for_replica_id'),
        'paid_capacity_pool_key': None,
        'system_recovery_launch_intent': None,
        'system_recovery_disposition': 'ORDINARY',
        'launch_request_id': None,
        'service_job_id': None,
        'candidate_ready_observed_at': None,
        'ordinary_release_not_before': None,
        'system_recovery_revision': 0,
        'system_recovery': None,
        'system_recovery_quarantine': None,
    }
    _require_json_value(replica_state)
    return {
        'replica_state_version': 1,
        'status': _replica_status(status),
        'sky_down_status': sky_down_status,
        'version': service_version,
        'cluster_name': cluster_name,
        'created_at': created_at,
        'is_spot': bool(is_spot),
        'replica_state': replica_state,
    }


def frozen_replica_row_values_from_pickle(
        payload: bytes, *, maximum_version: int) -> dict[str, Any]:
    """Load and project one pre-JSON pickle through the frozen boundary."""
    return frozen_replica_row_values(load_pre_json_replica(payload),
                                     maximum_version=maximum_version)
