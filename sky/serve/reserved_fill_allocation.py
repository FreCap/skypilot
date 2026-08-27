"""Atomic PostgreSQL publication of reserved-fill planner authority.

The broker owns per-pool allocation rounds.  The autoscaler needs one complete
service-wide input, however, rather than a collection of independently read
rounds.  This repository closes that boundary: it authenticates every pool
snapshot against the current claim set, exact committed observation, and
corresponding broker round, then publishes the complete map in the claim-set
row in the same transaction.

No provider call or placement decision belongs here.  The module is the sole
durable adapter between committed broker evidence and the pure
``ReservedFillPlanner``.
"""

import json
import math
import re
from typing import Any

import sqlalchemy
from sqlalchemy.engine import RowMapping

from sky.serve import pool_capacity_observation
from sky.serve import pool_capacity_observation_schema
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import reserved_fill_projection_authority
from sky.serve import serve_state_schema

_AUTHORITATIVE_CLAIM_SET = 'authoritative_v2'
_MAX_BIGINT = 2**63 - 1
_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')


class ReservedFillAllocationError(RuntimeError):
    """Base error for the durable allocation-publication boundary."""


class ReservedFillAllocationCorruptionError(ReservedFillAllocationError):
    """Durable allocation state violates its authenticated closed shape."""


def _require_nonempty_text(value: Any, subject: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f'{subject} must be nonempty text.')
    return value


def _require_positive_int(value: Any, subject: str) -> int:
    if type(value) is not int or value <= 0 or value > _MAX_BIGINT:
        raise ValueError(f'{subject} must be a positive 64-bit integer.')
    return value


def _require_nonnegative_int(value: Any, subject: str) -> int:
    if type(value) is not int or value < 0 or value > _MAX_BIGINT:
        raise ValueError(f'{subject} must be a nonnegative 64-bit integer.')
    return value


def _validate_controller_owner(owner: Any,) -> tuple[int | None, str | None]:
    if type(owner) is not tuple or len(owner) != 2:
        raise ValueError('Controller owner must be an exact (pid, ip) tuple.')
    pid, ip = owner
    if pid is not None and (type(pid) is not int or pid <= 0):
        raise ValueError('Controller owner PID must be positive or None.')
    if ip is not None and (type(ip) is not str or not ip):
        raise ValueError('Controller owner IP must be nonempty or None.')
    return pid, ip


def _reclaim_identity_columns(row: RowMapping) -> tuple[str, str, str]:
    """Decode the immutable Serve045 policy identity, or fail closed."""
    fleet_bundle = row['reclaim_fleet_bundle_sha256']
    policy_revision = row['reclaim_policy_revision']
    provider_inventory = row['reclaim_provider_inventory_sha256']
    if (type(fleet_bundle) is not str or
            _SHA256_PATTERN.fullmatch(fleet_bundle) is None or
            type(policy_revision) is not str or not policy_revision or
            type(provider_inventory) is not str or
            _SHA256_PATTERN.fullmatch(provider_inventory) is None):
        raise ReservedFillAllocationCorruptionError(
            'Reserved-fill reclaim policy identity is malformed.')
    return fleet_bundle, policy_revision, provider_inventory


def _database_now(connection: sqlalchemy.engine.Connection) -> float:
    value = connection.execute(
        sqlalchemy.text('SELECT EXTRACT(EPOCH FROM '
                        'clock_timestamp())::double precision')).scalar_one()
    return float(value)


def _decode_json_object(value: Any, subject: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise ReservedFillAllocationCorruptionError(
            f'{subject} is not serialized JSON text.')
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise ReservedFillAllocationCorruptionError(
            f'{subject} is malformed JSON.') from error
    if type(decoded) is not dict:
        raise ReservedFillAllocationCorruptionError(
            f'{subject} must be an exact JSON object.')
    return decoded


def _exact_claim_generations(value: Any) -> dict[str, int]:
    generations = _decode_json_object(value, 'Round claim generations')
    for service_name, generation in generations.items():
        if (type(service_name) is not str or not service_name or
                type(generation) is not int or generation <= 0):
            raise ReservedFillAllocationCorruptionError(
                'Round claim generations contain a malformed entry.')
    return generations


def _exact_nonnegative_counts(value: Any, subject: str) -> dict[str, int]:
    if type(value) is not dict:
        raise ReservedFillAllocationCorruptionError(
            f'{subject} must be an exact JSON object.')
    normalized: dict[str, int] = {}
    for raw_name, raw_count in value.items():
        if (type(raw_name) is not str or not raw_name or
                type(raw_count) is not int or raw_count < 0):
            raise ReservedFillAllocationCorruptionError(
                f'{subject} contains a malformed count.')
        name = raw_name.casefold()
        if name in normalized:
            raise ReservedFillAllocationCorruptionError(
                f'{subject} contains a case-folded duplicate.')
        normalized[name] = raw_count
    return normalized


def _claim_utilization_authority(
    set_row: RowMapping,) -> tuple[bool, int | None, bool, int]:
    """Decode the exact service-wide utilization witness for one map."""
    ceiling = set_row['utilization_ceiling']
    headroom = set_row['global_headroom']
    if (type(ceiling) is not int or ceiling < 0 or type(headroom) is not int or
            headroom < 0 or ceiling > headroom):
        raise ReservedFillAllocationCorruptionError(
            'Reserved-fill utilization ceiling is malformed.')
    raw_state = set_row['utilization_state']
    if raw_state is None:
        if ceiling != headroom:
            raise ReservedFillAllocationCorruptionError(
                'An ungated reserved-fill claim has a reduced ceiling.')
        return False, None, False, ceiling
    state = _decode_json_object(raw_state, 'Claim utilization state')
    required = {
        'cap', 'hot_until', 'stepped_at', 'blind_since', 'demonstrated_need',
        'boot_hold', 'blind'
    }
    if set(state) != required:
        raise ReservedFillAllocationCorruptionError(
            'Claim utilization state has an unsupported shape.')
    cap = state['cap']
    need = state['demonstrated_need']
    boot_hold = state['boot_hold']
    blind = state['blind']
    times = (state['hot_until'], state['stepped_at'])
    blind_since = state['blind_since']
    if (type(cap) is not int or cap < 0 or ceiling != min(headroom, cap) or
            type(boot_hold) is not bool or type(blind) is not bool or any(
                isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(float(value)) or value < 0
                for value in times) or
        (blind_since is not None and
         (isinstance(blind_since, bool) or
          not isinstance(blind_since, (int, float)) or
          not math.isfinite(float(blind_since)) or blind_since < 0)) or
        (need is not None and (type(need) is not int or need < 0)) or
        (blind and
         (need is not None or boot_hold)) or (not blind and need is None)):
        raise ReservedFillAllocationCorruptionError(
            'Claim utilization witness is malformed.')
    return True, need, boot_hold, ceiling


def _decode_accelerator_names(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ReservedFillAllocationCorruptionError(
                'Claim accelerator names are malformed JSON.') from error
    if type(value) is not list or not value:
        raise ReservedFillAllocationCorruptionError(
            'Claim accelerator names must be a nonempty JSON list.')
    names: list[str] = []
    for raw_name in value:
        if type(raw_name) is not str or not raw_name:
            raise ReservedFillAllocationCorruptionError(
                'Claim accelerator names contain a malformed name.')
        names.append(raw_name.casefold())
    if len(set(names)) != len(names):
        raise ReservedFillAllocationCorruptionError(
            'Claim accelerator names contain a folded duplicate.')
    return tuple(sorted(names))


def _exact_projection_digest_map(value: Any, accelerator_names: tuple[str, ...],
                                 subject: str) -> dict[str, str]:
    """Decode one closed case-folded accelerator-to-v2-digest mapping."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ReservedFillAllocationCorruptionError(
                f'{subject} is malformed JSON.') from error
    if type(value) is not dict or not value:
        raise ReservedFillAllocationCorruptionError(
            f'{subject} must be a nonempty exact JSON object.')
    normalized: dict[str, str] = {}
    for raw_card, raw_digest in value.items():
        if (type(raw_card) is not str or not raw_card or
                type(raw_digest) is not str or
                _SHA256_PATTERN.fullmatch(raw_digest) is None):
            raise ReservedFillAllocationCorruptionError(
                f'{subject} contains a malformed entry.')
        card = raw_card.casefold()
        if card in normalized:
            raise ReservedFillAllocationCorruptionError(
                f'{subject} contains a case-folded duplicate.')
        normalized[card] = raw_digest
    if set(normalized) != set(accelerator_names):
        raise ReservedFillAllocationCorruptionError(
            f'{subject} does not exactly cover the claim accelerators.')
    return dict(sorted(normalized.items()))


class ReservedFillAllocationRepository:
    """PostgreSQL-only complete allocation-map publication and read API."""

    def __init__(
        self,
        engine: sqlalchemy.engine.Engine | None = None,
    ) -> None:
        self._engine = (serve_state_schema.get_database_engine()
                        if engine is None else engine)
        if self._engine.dialect.name != 'postgresql':
            raise ValueError(
                'ReservedFillAllocationRepository is PostgreSQL-only.')

    @staticmethod
    def _lock_protocol(
        connection: sqlalchemy.engine.Connection,
        *,
        read: bool,
    ) -> tuple[RowMapping, RowMapping] | None:
        authority_table = (
            pool_capacity_observation_schema.protocol_state_sequence_table)
        query = sqlalchemy.select(authority_table).where(
            authority_table.c.id == 1).with_for_update(read=read)
        authority = connection.execute(query).mappings().one_or_none()
        return ReservedFillAllocationRepository._protocol_pair_from_authority(
            connection, authority)

    @staticmethod
    def _read_prelocked_protocol(
        connection: sqlalchemy.engine.Connection,
    ) -> tuple[RowMapping, RowMapping] | None:
        """Re-read protocol state after the caller took its first mutex."""
        authority_table = (
            pool_capacity_observation_schema.protocol_state_sequence_table)
        authority = connection.execute(
            sqlalchemy.select(authority_table).where(
                authority_table.c.id == 1)).mappings().one_or_none()
        return ReservedFillAllocationRepository._protocol_pair_from_authority(
            connection, authority)

    @staticmethod
    def _protocol_pair_from_authority(
        connection: sqlalchemy.engine.Connection,
        authority: RowMapping | None,
    ) -> tuple[RowMapping, RowMapping] | None:
        """Validate a locked protocol row and its durable projection."""
        if authority is None:
            raise ReservedFillAllocationCorruptionError(
                'Reserved-fill protocol singleton is absent.')
        if (authority['protocol_version']
                != reserved_capacity_broker.PROTOCOL_V2 or
                authority['reconciliation_gate_state']
                != pool_capacity_observation_schema.SEQUENCED_ACTIVE):
            return None
        gate_generation = authority['reconciliation_gate_generation']
        sequence = authority['zero_cost_admission_sequence']
        ordinary_sequence = authority['ordinary_zero_cost_admission_sequence']
        materialization_sequence = authority[
            'zero_cost_materialization_sequence']
        if (type(gate_generation) is not int or gate_generation <= 0 or
                type(sequence) is not int or sequence < 0 or
                type(ordinary_sequence) is not int or ordinary_sequence < 0 or
                ordinary_sequence > sequence or
                type(materialization_sequence) is not int or
                materialization_sequence < 0):
            raise ReservedFillAllocationCorruptionError(
                'Reserved-fill sequencer or gate generation is malformed.')
        _reclaim_identity_columns(authority)

        protocol_table = serve_state_schema.reserved_fill_protocol_state_table
        protocol = connection.execute(
            sqlalchemy.select(protocol_table).where(
                protocol_table.c.id == 1)).mappings().one_or_none()
        if protocol is None or protocol['protocol_version'] != authority[
                'protocol_version']:
            raise ReservedFillAllocationCorruptionError(
                'Reserved-fill protocol projections disagree.')
        claim_generation = protocol['claim_generation']
        if type(claim_generation) is not int or claim_generation < 0:
            raise ReservedFillAllocationCorruptionError(
                'Reserved-fill global claim generation is malformed.')
        return authority, protocol

    @staticmethod
    def _lock_service(
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
        *,
        read: bool,
    ) -> RowMapping | None:
        table = serve_state_schema.services_table
        row = connection.execute(
            sqlalchemy.select(table.c.name, table.c.hash,
                              table.c.controller_pid, table.c.controller_ip,
                              table.c.resource_scope,
                              table.c.current_version).where(
                                  table.c.name == service_name).with_for_update(
                                      read=read)).mappings().one_or_none()
        return ReservedFillAllocationRepository._validate_service_row(
            row, expected_service_hash, expected_controller_owner)

    @staticmethod
    def _read_prelocked_service(
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
    ) -> RowMapping | None:
        """Re-read service identity without inverting an existing lock order."""
        table = serve_state_schema.services_table
        row = connection.execute(
            sqlalchemy.select(
                table.c.name, table.c.hash, table.c.controller_pid,
                table.c.controller_ip, table.c.resource_scope,
                table.c.current_version).where(
                    table.c.name == service_name)).mappings().one_or_none()
        return ReservedFillAllocationRepository._validate_service_row(
            row, expected_service_hash, expected_controller_owner)

    @staticmethod
    def _validate_service_row(
        row: RowMapping | None,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
    ) -> RowMapping | None:
        if (row is None or row['hash'] != expected_service_hash or
                row['resource_scope'] != expected_service_hash or
            (row['controller_pid'], row['controller_ip'])
                != expected_controller_owner):
            return None
        return row

    @staticmethod
    def _lock_claim_set(
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        global_claim_generation: int,
        *,
        read: bool,
    ) -> tuple[RowMapping, tuple[RowMapping, ...]] | None:
        set_table = serve_state_schema.reserved_fill_service_claim_sets_table
        set_row = connection.execute(
            sqlalchemy.select(set_table).where(
                set_table.c.service_name == service_name).with_for_update(
                    read=read)).mappings().one_or_none()
        if set_row is None:
            return None
        generation = set_row['generation']
        edge_count = set_row['edge_count']
        if (set_row['claim_set_state'] != _AUTHORITATIVE_CLAIM_SET or
                type(generation) is not int or generation <= 0 or
                generation > global_claim_generation or
                type(edge_count) is not int or edge_count <= 0 or
                type(set_row['service_version']) is not int or
                set_row['service_version'] <= 0):
            return None

        edge_table = serve_state_schema.reserved_fill_pool_claims_table
        edges = tuple(
            connection.execute(
                sqlalchemy.select(edge_table).where(
                    edge_table.c.service_name == service_name).order_by(
                        edge_table.c.pool_position,
                        edge_table.c.pool_key).with_for_update(
                            read=read)).mappings().all())
        if (len(edges) != edge_count or any(
                edge['service_generation'] != generation for edge in edges)):
            return None
        for edge in edges:
            accelerator_names = _decode_accelerator_names(
                edge['accelerator_names'])
            _exact_projection_digest_map(
                edge['worker_projection_sha256_by_accelerator'],
                accelerator_names, 'Claim worker projection digest map')
        positions = [edge['pool_position'] for edge in edges]
        if positions != list(range(edge_count)):
            return None
        return set_row, edges

    @staticmethod
    def _lock_projection_source(
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        service_version: int,
        edges: tuple[RowMapping, ...],
    ) -> bool:
        """Recompute every edge digest from one locked immutable version."""
        table = serve_state_schema.version_specs_table
        row = connection.execute(
            sqlalchemy.select(table.c.worker_placement_projections).where(
                table.c.service_name == service_name,
                table.c.version == service_version,
                table.c.yaml_content.isnot(None),
                table.c.quarantined_at.is_(None),
                table.c.retired_at.is_(None),
            ).with_for_update(read=True)).mappings().one_or_none()
        if row is None:
            return False
        try:
            for edge in edges:
                accelerator_names = _decode_accelerator_names(
                    edge['accelerator_names'])
                projected_admissions = (
                    reserved_fill_projection_authority.
                    projected_admissions_for_edge(
                        row['worker_placement_projections'],
                        access_context=edge['access_context'],
                        accelerator_names=accelerator_names,
                        accelerator_count=edge['gpus_per_replica'],
                        require_current_protocol=True))
                expected_map = (
                    reserved_fill_projection_authority.
                    projection_sha256_by_accelerator(projected_admissions))
                actual_map = _exact_projection_digest_map(
                    edge['worker_projection_sha256_by_accelerator'],
                    accelerator_names, 'Claim worker projection digest map')
                if actual_map != expected_map:
                    return False
        except (TypeError, ValueError):
            return False
        return True

    @staticmethod
    def _lock_rounds(
        connection: sqlalchemy.engine.Connection,
        pool_snapshots: tuple[reserved_fill_planner.PoolFillSnapshot, ...],
        *,
        read: bool,
    ) -> dict[str, RowMapping] | None:
        """Lock shared pool rows in the one canonical global order.

        Every final-admission path uses protocol -> service -> sorted pool
        rounds -> claim set/edges.  Claim order remains independently
        significant for planning; sorting here is only lock acquisition.
        """
        pool_keys = [snapshot.pool_key for snapshot in pool_snapshots]
        if len(set(pool_keys)) != len(pool_keys):
            raise ValueError('Pool snapshots contain a duplicate pool key.')
        table = serve_state_schema.reserved_fill_rounds_table
        locked: dict[str, RowMapping] = {}
        for pool_key in sorted(pool_keys):
            row = connection.execute(
                sqlalchemy.select(table).where(
                    table.c.pool_key == pool_key).with_for_update(
                        read=read)).mappings().one_or_none()
            if row is None:
                return None
            locked[pool_key] = row
        return locked

    @staticmethod
    def _validate_snapshot_topology(
        snapshots: tuple[reserved_fill_planner.PoolFillSnapshot, ...],
        generation: int,
        edges: tuple[RowMapping, ...],
    ) -> bool:
        """Validate each service's access path independently of observation.

        A protocol-v2 pool is a physical identity, so multiple Kubernetes
        context aliases may legitimately consume the same pool-global
        observation.  The service still has to prove that its launch locations
        use the access context and physical UID from its own current claim
        edge; this is the per-service access attestation boundary.
        """
        if len(snapshots) != len(edges):
            return False
        physical_cards: set[tuple[str, str]] = set()
        for snapshot, edge in zip(snapshots, edges):
            if (snapshot.pool_key != edge['pool_key'] or
                    snapshot.service_generation != generation or
                    snapshot.physical_cluster_uid
                    != edge['physical_cluster_uid'] or
                    snapshot.edge_cap != edge['effective_cap']):
                return False
            if not bool(edge['launchable']) and (snapshot.grant != 0 or
                                                 snapshot.free_slots != 0):
                return False
            contexts = {location.region for location in snapshot.locations}
            widths = {
                location.accelerator_count for location in snapshot.locations
            }
            cards = {
                location.accelerator.casefold()
                for location in snapshot.locations
            }
            edge_cards = set(
                _decode_accelerator_names(edge['accelerator_names']))
            edge_projection_map = _exact_projection_digest_map(
                edge['worker_projection_sha256_by_accelerator'],
                tuple(sorted(edge_cards)), 'Claim worker projection digest map')
            if (contexts != {edge['access_context']} or cards != edge_cards or
                    widths != {edge['gpus_per_replica']} or
                    dict(snapshot.worker_projection_sha256_by_accelerator)
                    != edge_projection_map):
                return False
            topology = {(snapshot.physical_cluster_uid, card) for card in cards}
            if physical_cards.intersection(topology):
                return False
            physical_cards.update(topology)
        return True

    @staticmethod
    def _validate_round(  # pylint: disable=too-many-locals
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        snapshot: reserved_fill_planner.PoolFillSnapshot,
        round_row: RowMapping,
        now: float,
    ) -> bool:
        if (round_row['pool_key'] != snapshot.pool_key or
                round_row['protocol_version']
                != reserved_capacity_broker.PROTOCOL_V2 or
                round_row['fence_pending'] != 0):
            return False

        provenance_table = (pool_capacity_observation_schema.
                            reserved_fill_round_observation_table)
        provenance = connection.execute(
            sqlalchemy.select(provenance_table).where(
                provenance_table.c.pool_key ==
                snapshot.pool_key)).mappings().one_or_none()
        if provenance is None:
            return False

        observation_table = (pool_capacity_observation_schema.
                             demand_capacity_observations_v2_table)
        observation_row = connection.execute(
            sqlalchemy.select(observation_table).where(
                observation_table.c.pool_key == snapshot.pool_key,
                observation_table.c.observation_status.in_(
                    pool_capacity_observation_schema.COMPLETED_STATUSES),
            ).order_by(observation_table.c.observation_generation.desc()).limit(
                1).with_for_update(read=True)).mappings().one_or_none()
        if observation_row is None:
            return False
        committed = pool_capacity_observation.decode_completed_observation(
            observation_row)
        if (committed is None or
                not isinstance(committed.payload,
                               pool_capacity_observation.PoolCapacitySuccess) or
                not committed.is_authoritative_at(now)):
            return False
        gpus_per_replica = snapshot.broker_slot_width
        observed_slot_counts = dict(
            committed.payload.slot_counts(gpus_per_replica))
        observed_slots = sum(observed_slot_counts.values())
        if (committed.observation_generation != snapshot.observation_generation
                or committed.observation_sequence
                != snapshot.observation_sequence or
                committed.ordinary_admission_sequence
                != snapshot.ordinary_zero_cost_admission_sequence or
                committed.valid_until != snapshot.valid_until or
                committed.physical_cluster_uid != snapshot.physical_cluster_uid
                or provenance['observation_generation']
                != committed.observation_generation or
                provenance['observation_sequence']
                != committed.observation_sequence or
                provenance['observation_materialization_sequence']
                != committed.materialization_sequence or
                provenance['observation_payload_sha256']
                != committed.payload_sha256 or
                round_row['snapshot_time'] != committed.observed_at or
                round_row['last_observed_free'] != observed_slots or
                round_row['last_observed_free_ts'] != committed.observed_at):
            return False

        # ``access_context`` is authenticated acquisition provenance, not part
        # of protocol-v2 physical-pool identity.  Requiring it to equal this
        # service's launch context would let whichever alias won the shared
        # observation lease starve every claimant using another alias.  UID,
        # exact-card, digest, generation, sequence, and freshness evidence stay
        # pool-global here; `_validate_snapshot_topology()` separately binds
        # this service's locations to its current context/UID claim edge.

        claim_generations = _exact_claim_generations(
            round_row['claim_generations'])
        if claim_generations.get(service_name) != snapshot.service_generation:
            return False
        grants = _decode_json_object(round_row['grants'], 'Round grants')
        feeds = _decode_json_object(round_row['feeds'], 'Round feeds')
        grant = grants.get(service_name)
        feed = feeds.get(service_name)
        if (type(grant) is not int or grant < 0 or type(feed) is not int or
                feed < 0 or grant != snapshot.grant or
                feed != snapshot.free_slots):
            return False
        epoch = round_row['epoch']
        if (type(epoch) is not int or epoch <= 0 or
            (snapshot.grant_epoch is not None and
             snapshot.grant_epoch != epoch)):
            return False

        shaped_feed = _decode_json_object(round_row['feed_by_accelerator'],
                                          'Round exact-card feed')
        if shaped_feed.get(reserved_capacity_broker.BROKER_SLOT_WIDTH_KEY) != (
                snapshot.broker_slot_width):
            return False
        observed_counts = _exact_nonnegative_counts(
            shaped_feed.get(
                reserved_capacity_broker.OBSERVED_FREE_BY_ACCELERATOR_KEY),
            'Round observed exact-card capacity')
        if observed_counts != observed_slot_counts:
            return False
        service_counts = _exact_nonnegative_counts(
            shaped_feed.get(service_name), 'Round service exact-card feed')
        service_counts = {
            name: count for name, count in service_counts.items() if count > 0
        }
        snapshot_counts = snapshot.free_slots_by_accelerator
        if (snapshot_counts is None or
                service_counts != dict(snapshot_counts) or
                sum(service_counts.values()) != feed):
            return False
        return True

    @staticmethod
    def _upward_grant_is_settled(
        service_name: str,
        snapshot: reserved_fill_planner.PoolFillSnapshot,
        round_row: RowMapping,
    ) -> bool:
        """Whether this pool's damped grant has reached its raw entitlement."""
        raw_grants = _decode_json_object(round_row['raw_grants'],
                                         'Round raw grants')
        raw = raw_grants.get(service_name)
        if type(raw) is not int or raw < 0 or raw > snapshot.edge_cap:
            raise ReservedFillAllocationCorruptionError(
                'Round raw grant is malformed or exceeds its edge cap.')
        return raw <= snapshot.grant

    @staticmethod
    def _read_allocation_columns(
        connection: sqlalchemy.engine.Connection,
        service_name: str,
    ) -> RowMapping | None:
        table = (pool_capacity_observation_schema.
                 reserved_fill_service_allocation_table)
        return connection.execute(
            sqlalchemy.select(table).where(
                table.c.service_name == service_name)).mappings().one_or_none()

    @staticmethod
    def _decode_current_allocation(
        row: RowMapping,
    ) -> reserved_fill_planner.AuthenticatedAllocationMap | None:
        generation = row['allocation_generation']
        if generation == 0:
            if any(row[name] is not None
                   for name in ('allocation_input_sha256',
                                'allocation_claim_generation', 'allocation_map',
                                'allocation_published_at',
                                'allocation_gate_generation')):
                raise ReservedFillAllocationCorruptionError(
                    'Inactive allocation publication has partial authority.')
            return None
        if type(generation) is not int or generation < 0:
            raise ReservedFillAllocationCorruptionError(
                'Allocation generation is malformed.')
        raw_map = row['allocation_map']
        if type(raw_map) is not dict:
            raise ReservedFillAllocationCorruptionError(
                'Published allocation map must be a JSON object.')
        if raw_map.get('schema_version') == 5:
            # N-1 is readable only as stale authority.  The schema-6 writer
            # replaces it under the existing generation CAS; no positive
            # provider effect may consume an allocation that lacks the
            # utilization causality witness.
            return None
        try:
            allocation = (reserved_fill_planner.AuthenticatedAllocationMap.
                          from_mapping(raw_map))
        except ValueError as error:
            raise ReservedFillAllocationCorruptionError(
                'Published allocation map failed authentication.') from error
        published_at = row['allocation_published_at']
        if (allocation.allocation_generation != generation or
                allocation.allocation_input_sha256
                != row['allocation_input_sha256'] or
                allocation.allocation_claim_generation
                != row['allocation_claim_generation'] or
                isinstance(published_at, bool) or
                not isinstance(published_at, (int, float)) or
                not math.isfinite(float(published_at)) or published_at < 0):
            raise ReservedFillAllocationCorruptionError(
                'Published allocation columns disagree with the map.')
        return allocation

    def publish(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        service_name: str,
        *,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
        expected_claim_generation: int,
        expected_gate_generation: int,
        pool_snapshots: tuple[reserved_fill_planner.PoolFillSnapshot, ...],
    ) -> reserved_fill_planner.AuthenticatedAllocationMap | None:
        """Publish one complete map if every current authority fence matches.

        ``None`` is a normal stale-input result: a gate, owner, claim, round,
        or observation moved before the transaction acquired its locks.
        Malformed caller values raise ``ValueError``; malformed durable
        authenticated state raises ``ReservedFillAllocationCorruptionError``.
        """
        name = _require_nonempty_text(service_name, 'Service name')
        service_hash = _require_nonempty_text(expected_service_hash,
                                              'Expected service hash')
        owner = _validate_controller_owner(expected_controller_owner)
        claim_generation = _require_positive_int(expected_claim_generation,
                                                 'Expected claim generation')
        gate_generation = _require_positive_int(expected_gate_generation,
                                                'Expected gate generation')
        if (type(pool_snapshots) is not tuple or not pool_snapshots or any(
                not isinstance(snapshot, reserved_fill_planner.PoolFillSnapshot)
                for snapshot in pool_snapshots)):
            raise ValueError('Pool snapshots must be a nonempty immutable '
                             'tuple of PoolFillSnapshot values.')

        with self._engine.begin() as connection:
            protocol_pair = self._lock_protocol(connection, read=False)
            if protocol_pair is None:
                return None
            protocol_authority, protocol = protocol_pair
            if (protocol_authority['reconciliation_gate_generation']
                    != gate_generation):
                return None
            ordinary_admission_high_water = protocol_authority[
                'ordinary_zero_cost_admission_sequence']
            if (type(ordinary_admission_high_water) is not int or
                    ordinary_admission_high_water < 0 or
                    any(snapshot.ordinary_zero_cost_admission_sequence !=
                        ordinary_admission_high_water
                        for snapshot in pool_snapshots)):
                # Every feed in one complete map must have been observed
                # before the same ordinary-demand prefix. Ordinary demand is
                # not broker-partitioned, so any later ordinary zero-cost row
                # invalidates the evidence. Peer fill remains safe under its
                # independently authenticated broker grant.
                return None
            service_row = self._lock_service(connection,
                                             name,
                                             service_hash,
                                             owner,
                                             read=False)
            if service_row is None:
                return None
            locked_rounds = self._lock_rounds(connection,
                                              pool_snapshots,
                                              read=False)
            if locked_rounds is None:
                return None
            claim_state = self._lock_claim_set(
                connection, name, int(protocol['claim_generation']), read=False)
            if claim_state is None:
                return None
            set_row, edges = claim_state
            if set_row['generation'] != claim_generation:
                return None
            (utilization_gate_armed, utilization_demonstrated_need,
             utilization_boot_hold,
             utilization_ceiling) = _claim_utilization_authority(set_row)
            if (service_row['current_version'] != set_row['service_version'] or
                    not self._lock_projection_source(
                        connection, name, set_row['service_version'], edges)):
                return None
            if not self._validate_snapshot_topology(pool_snapshots,
                                                    claim_generation, edges):
                return None

            now = _database_now(connection)
            upward_grants_settled = True
            for snapshot in pool_snapshots:
                round_row = locked_rounds[snapshot.pool_key]
                if not self._validate_round(connection, name, snapshot,
                                            round_row, now):
                    return None
                upward_grants_settled = (upward_grants_settled and
                                         self._upward_grant_is_settled(
                                             name, snapshot, round_row))

            allocation_columns = self._read_allocation_columns(connection, name)
            if allocation_columns is None:
                raise ReservedFillAllocationCorruptionError(
                    'Claim set lost its allocation projection.')
            current = self._decode_current_allocation(allocation_columns)
            current_published_at = allocation_columns['allocation_published_at']
            if (current is not None and
                (isinstance(current_published_at, bool) or
                 not isinstance(current_published_at, (int, float)) or
                 float(current_published_at) > now)):
                raise ReservedFillAllocationCorruptionError(
                    'Current allocation publication time is in the future.')
            if current is not None and (
                    current.allocation_claim_generation == claim_generation and
                    current.service_version == set_row['service_version'] and
                    current.pool_snapshots == pool_snapshots and
                    current.reconciliation_gate_generation == gate_generation
                    and current.reclaim_fleet_bundle_sha256
                    == protocol_authority['reclaim_fleet_bundle_sha256'] and
                    current.reclaim_policy_revision
                    == protocol_authority['reclaim_policy_revision'] and
                    current.reclaim_provider_inventory_sha256
                    == protocol_authority['reclaim_provider_inventory_sha256']
                    and current.utilization_gate_armed == utilization_gate_armed
                    and current.utilization_demonstrated_need
                    == utilization_demonstrated_need and
                    current.utilization_boot_hold == utilization_boot_hold and
                    current.utilization_ceiling == utilization_ceiling and
                    current.upward_grants_settled == upward_grants_settled and
                    allocation_columns['allocation_gate_generation']
                    == gate_generation):
                return current

            previous_generation = allocation_columns['allocation_generation']
            if (type(previous_generation) is not int or
                    previous_generation < 0 or
                    previous_generation >= _MAX_BIGINT):
                raise ReservedFillAllocationCorruptionError(
                    'Allocation generation is malformed or exhausted.')
            allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
                allocation_generation=previous_generation + 1,
                allocation_claim_generation=claim_generation,
                service_version=set_row['service_version'],
                ordinary_zero_cost_admission_sequence_high_water=(
                    ordinary_admission_high_water),
                reconciliation_gate_generation=gate_generation,
                reclaim_fleet_bundle_sha256=(
                    protocol_authority['reclaim_fleet_bundle_sha256']),
                reclaim_policy_revision=(
                    protocol_authority['reclaim_policy_revision']),
                reclaim_provider_inventory_sha256=(
                    protocol_authority['reclaim_provider_inventory_sha256']),
                utilization_gate_armed=utilization_gate_armed,
                utilization_demonstrated_need=(utilization_demonstrated_need),
                utilization_boot_hold=utilization_boot_hold,
                utilization_ceiling=utilization_ceiling,
                upward_grants_settled=upward_grants_settled,
                pool_snapshots=pool_snapshots)
            allocation_table = (pool_capacity_observation_schema.
                                reserved_fill_service_allocation_table)
            updated = connection.execute(
                sqlalchemy.update(allocation_table).where(
                    allocation_table.c.service_name == name,
                    allocation_table.c.allocation_generation ==
                    previous_generation).values(
                        allocation_generation=(
                            allocation.allocation_generation),
                        allocation_input_sha256=(
                            allocation.allocation_input_sha256),
                        allocation_claim_generation=claim_generation,
                        allocation_map=allocation.to_mapping(),
                        allocation_published_at=now,
                        allocation_gate_generation=gate_generation))
            if updated.rowcount != 1:
                raise ReservedFillAllocationCorruptionError(
                    'Locked allocation publication lost its generation CAS.')
            return allocation

    def read_current_in_connection(  # pylint: disable=too-many-locals
        self,
        connection: sqlalchemy.engine.Connection,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
        *,
        protocol_and_service_prelocked: bool = False,
    ) -> reserved_fill_planner.AuthenticatedAllocationMap | None:
        """Revalidate current authority inside the caller's transaction.

        A destructive caller that already holds the protocol singleton and
        service row sets ``protocol_and_service_prelocked``.  Validation then
        re-reads those rows without reacquiring the lock-order prefix before
        locking the remaining round, claim, edge, and projection evidence.
        """
        if not isinstance(connection, sqlalchemy.engine.Connection):
            raise ValueError('Allocation validation requires a SQLAlchemy '
                             'Connection.')
        if connection.dialect.name != 'postgresql':
            raise ValueError('Allocation validation requires PostgreSQL.')
        if not connection.in_transaction():
            raise ValueError('Allocation validation requires an active '
                             'caller transaction.')
        if type(protocol_and_service_prelocked) is not bool:
            raise ValueError('Prelocked mode must be a boolean.')
        name = _require_nonempty_text(service_name, 'Service name')
        service_hash = _require_nonempty_text(expected_service_hash,
                                              'Expected service hash')
        owner = _validate_controller_owner(expected_controller_owner)
        if protocol_and_service_prelocked:
            protocol_pair = self._read_prelocked_protocol(connection)
        else:
            protocol_pair = self._lock_protocol(connection, read=True)
        if protocol_pair is None:
            return None
        protocol_authority, protocol = protocol_pair
        if protocol_and_service_prelocked:
            service_row = self._read_prelocked_service(connection, name,
                                                       service_hash, owner)
        else:
            service_row = self._lock_service(connection,
                                             name,
                                             service_hash,
                                             owner,
                                             read=True)
        if service_row is None:
            return None
        # The allocation row is read without a row lock first so its
        # authenticated pool set can drive canonical sorted round locking.
        # The service ownership lock fences every supported allocation and
        # claim-set writer; re-reading after the claim lock additionally
        # makes that dependency explicit and fail closed.
        allocation_columns = self._read_allocation_columns(connection, name)
        if allocation_columns is None:
            return None
        allocation = self._decode_current_allocation(allocation_columns)
        if allocation is None:
            return None
        locked_rounds = self._lock_rounds(connection,
                                          allocation.pool_snapshots,
                                          read=True)
        if locked_rounds is None:
            return None
        claim_state = self._lock_claim_set(connection,
                                           name,
                                           int(protocol['claim_generation']),
                                           read=True)
        if claim_state is None:
            return None
        set_row, edges = claim_state
        (utilization_gate_armed, utilization_demonstrated_need,
         utilization_boot_hold,
         utilization_ceiling) = _claim_utilization_authority(set_row)
        if (service_row['current_version'] != set_row['service_version'] or
                not self._lock_projection_source(
                    connection, name, set_row['service_version'], edges)):
            return None
        current_columns = self._read_allocation_columns(connection, name)
        if current_columns is None:
            raise ReservedFillAllocationCorruptionError(
                'Claim set lost its allocation projection.')
        current = self._decode_current_allocation(current_columns)
        if current is None:
            return None
        current_sequence = protocol_authority[
            'ordinary_zero_cost_admission_sequence']
        if (type(current_sequence) is not int or current_sequence
                < current.ordinary_zero_cost_admission_sequence_high_water):
            raise ReservedFillAllocationCorruptionError(
                'Ordinary admission sequence regressed behind the '
                'authenticated allocation high-water.')
        if (current_sequence
                != current.ordinary_zero_cost_admission_sequence_high_water):
            # Any ordinary admission after publication consumes shared
            # unpartitioned capacity.  The map is stale, not corrupt, and
            # must be rebuilt from new pool evidence before it is spent.
            return None
        allocation_column_names = ('allocation_generation',
                                   'allocation_input_sha256',
                                   'allocation_claim_generation',
                                   'allocation_map', 'allocation_published_at',
                                   'allocation_gate_generation')
        if (current != allocation or
                any(current_columns[column] != allocation_columns[column]
                    for column in allocation_column_names)):
            return None
        if (allocation.allocation_claim_generation != set_row['generation'] or
                allocation.service_version != set_row['service_version'] or
                current_columns['allocation_gate_generation']
                != protocol_authority['reconciliation_gate_generation'] or
                allocation.reconciliation_gate_generation
                != protocol_authority['reconciliation_gate_generation'] or
                allocation.reclaim_fleet_bundle_sha256
                != protocol_authority['reclaim_fleet_bundle_sha256'] or
                allocation.reclaim_policy_revision
                != protocol_authority['reclaim_policy_revision'] or
                allocation.reclaim_provider_inventory_sha256
                != protocol_authority['reclaim_provider_inventory_sha256'] or
                allocation.utilization_gate_armed != utilization_gate_armed or
                allocation.utilization_demonstrated_need
                != utilization_demonstrated_need or
                allocation.utilization_boot_hold != utilization_boot_hold or
                allocation.utilization_ceiling != utilization_ceiling or
                not self._validate_snapshot_topology(allocation.pool_snapshots,
                                                     int(set_row['generation']),
                                                     edges)):
            return None
        now = _database_now(connection)
        published_at = float(current_columns['allocation_published_at'])
        if (published_at > now or
                any(snapshot.valid_until < now
                    for snapshot in allocation.pool_snapshots)):
            return None
        upward_grants_settled = True
        for snapshot in allocation.pool_snapshots:
            round_row = locked_rounds[snapshot.pool_key]
            if not self._validate_round(connection, name, snapshot, round_row,
                                        now):
                return None
            upward_grants_settled = (upward_grants_settled and
                                     self._upward_grant_is_settled(
                                         name, snapshot, round_row))
        if allocation.upward_grants_settled != upward_grants_settled:
            return None
        return allocation

    def read_current(  # pylint: disable=too-many-locals
        self,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
    ) -> reserved_fill_planner.AuthenticatedAllocationMap | None:
        """Read and revalidate the current complete planner authority."""
        with self._engine.begin() as connection:
            return self.read_current_in_connection(connection, service_name,
                                                   expected_service_hash,
                                                   expected_controller_owner)

    def current_generation(
        self,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
    ) -> int | None:
        """Return the authenticated current generation, or no authority."""
        allocation = self.read_current(service_name, expected_service_hash,
                                       expected_controller_owner)
        if allocation is None:
            return None
        return allocation.allocation_generation
