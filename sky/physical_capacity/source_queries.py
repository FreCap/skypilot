"""Narrow PostgreSQL source reads for physical-capacity evidence scans.

This module deliberately contains no workload-state getters.  Every query
starts from a configured selector (or from an exact registry/history key
derived from one) and selects only the scalar columns authorized by the C2
evidence-scan design.  ``PartitionSourceCache`` adds negative-result-aware
partition-local caches and enforces the row/value/retained-input bounds before
the pure adapters consume a row.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import dataclasses
import time
from typing import Any, cast, Protocol

import sqlalchemy

from sky import global_user_state
from sky.jobs import state_schema as managed_job_schema
from sky.serve import serve_state_schema


class SourceReadError(RuntimeError):
    """Base class for closed evidence-source read failures."""


class RowLimitExceededError(SourceReadError):
    """A partition exceeded its source or total row budget."""


class ByteLimitExceededError(SourceReadError):
    """A source value, fetch batch, or retained snapshot exceeded its bound."""


class SourceDecodeError(SourceReadError):
    """A bounded query returned an impossible key shape or duplicate row."""


class SourceConflictError(SourceReadError):
    """Source rows contradicted each other or crossed a workspace boundary."""


class SelectorMismatchError(SourceReadError):
    """A selected source row contradicted its typed selector."""


class SourceReadDeadlineExceededError(SourceReadError):
    """Pure normalization exceeded its caller-owned partition deadline."""


@dataclasses.dataclass(frozen=True)
class SourceReadLimits:
    """Exact mapping-version-1 bounds for one partition."""

    max_source_rows: int = 10_000
    max_total_rows: int = 30_000
    max_value_bytes: int = 1 << 20
    max_fetch_batch_bytes: int = 4 << 20
    max_retained_bytes: int = 64 << 20
    fetch_batch_rows: int = 256

    def __post_init__(self) -> None:
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if (not isinstance(value, int) or isinstance(value, bool) or
                    value <= 0):
                raise ValueError(f'{field.name} must be a positive integer.')


class SourceReader(Protocol):
    """Adapter-facing interface implemented by the bounded SQL cache."""

    @property
    def source_rows(self) -> int:
        ...

    @property
    def rows_seen(self) -> int:
        ...

    def service(self, name: str) -> Mapping[str, Any] | None:
        ...

    def replicas(self, service_name: str) -> tuple[Mapping[str, Any], ...]:
        ...

    def job_info(self, spot_job_id: int) -> Mapping[str, Any] | None:
        ...

    def spot_tasks(self, spot_job_id: int,
                   task_id: int) -> tuple[Mapping[str, Any], ...]:
        ...

    def cluster(self, name: str) -> Mapping[str, Any] | None:
        ...

    def cluster_history(self, cluster_hash: str) -> Mapping[str, Any] | None:
        ...

    def check_deadline(self) -> None:
        ...

    def retain_canonical_bytes(self, encoded: bytes) -> None:
        ...


_SERVICES = serve_state_schema.services_table
_REPLICAS = serve_state_schema.replicas_table
_JOB_INFO = managed_job_schema.job_info_table
_SPOT = managed_job_schema.spot_table
_CLUSTERS = global_user_state.cluster_table
_CLUSTER_HISTORY = global_user_state.cluster_history_table


@dataclasses.dataclass(frozen=True)
class SourceRelationRequirement:
    """Exact catalog shape consumed by the mapping-version-1 queries."""

    relation: str
    columns: tuple[tuple[str, str], ...]
    index_leading_columns: tuple[tuple[str, ...], ...]


_SOURCE_SCHEMA_REQUIREMENTS = (
    SourceRelationRequirement(
        'services',
        (('name', 'text'), ('workspace', 'text'), ('status', 'text'),
         ('pool', 'integer'), ('controller_pid', 'integer'),
         ('controller_port', 'integer'), ('controller_ip', 'text'),
         ('hash', 'text'), ('lifecycle_epoch', 'integer'),
         ('resource_scope', 'text')), (('name',),)),
    SourceRelationRequirement('replicas',
                              (('service_name', 'text'),
                               ('replica_id', 'integer'), ('status', 'text'),
                               ('version', 'integer'),
                               ('cluster_name', 'text')),
                              (('service_name', 'replica_id'),)),
    SourceRelationRequirement(
        'job_info', (('spot_job_id', 'integer'), ('workspace', 'text'),
                     ('controller_instance_id', 'text'),
                     ('controller_generation', 'bigint'), ('pool', 'text'),
                     ('current_cluster_name', 'text'), ('is_batch', 'boolean')),
        (('spot_job_id',),)),
    SourceRelationRequirement('spot',
                              (('job_id', 'integer'),
                               ('spot_job_id', 'integer'),
                               ('task_id', 'integer'), ('task_name', 'text'),
                               ('status', 'text')),
                              (('job_id',), ('spot_job_id', 'task_id'))),
    SourceRelationRequirement(
        'clusters', (('name', 'text'), ('cluster_hash', 'text'),
                     ('status', 'text'), ('workspace', 'text'),
                     ('is_managed', 'integer'), ('workload_type', 'text'),
                     ('workload_id', 'text'), ('workload_task_id', 'integer')),
        (('name',),)),
    SourceRelationRequirement(
        'cluster_history',
        (('cluster_hash', 'text'), ('workspace', 'text'), ('cloud', 'text'),
         ('region', 'text'), ('zone', 'text'), ('num_nodes', 'integer'),
         ('is_managed', 'integer'), ('workload_type', 'text'),
         ('workload_id', 'text'), ('workload_task_id', 'integer')),
        (('cluster_hash',),)),
)


def source_schema_requirements() -> tuple[SourceRelationRequirement, ...]:
    """Return the immutable source catalog contract for startup validation."""
    return _SOURCE_SCHEMA_REQUIREMENTS


def _length(column: sqlalchemy.Column[Any]) -> Any:
    return sqlalchemy.func.octet_length(column).label(f'{column.name}_bytes')


def service_length_query(name: str) -> sqlalchemy.sql.Select:
    """Build the first-phase point read for one selected Serve owner."""
    return sqlalchemy.select(
        _SERVICES.c.name,
        _length(_SERVICES.c.workspace),
        _length(_SERVICES.c.status),
        _SERVICES.c.pool,
        _SERVICES.c.controller_pid,
        _SERVICES.c.controller_port,
        _length(_SERVICES.c.controller_ip),
        _length(_SERVICES.c.hash),
        _SERVICES.c.lifecycle_epoch,
        _length(_SERVICES.c.resource_scope),
    ).where(_SERVICES.c.name == name).limit(1)


def service_value_query(name: str) -> sqlalchemy.sql.Select:
    """Build the approved scalar-value read for one selected Serve owner."""
    return sqlalchemy.select(
        _SERVICES.c.name,
        _SERVICES.c.workspace,
        _SERVICES.c.status,
        _SERVICES.c.pool,
        _SERVICES.c.controller_pid,
        _SERVICES.c.controller_port,
        _SERVICES.c.controller_ip,
        _SERVICES.c.hash,
        _SERVICES.c.lifecycle_epoch,
        _SERVICES.c.resource_scope,
    ).where(_SERVICES.c.name == name).limit(1)


def replica_length_query(service_name: str,
                         limit: int) -> sqlalchemy.sql.Select:
    """Build the first-phase PK-prefix read for selected Serve replicas."""
    return sqlalchemy.select(
        _REPLICAS.c.service_name,
        _REPLICAS.c.replica_id,
        _length(_REPLICAS.c.status),
        _REPLICAS.c.version,
        _length(_REPLICAS.c.cluster_name),
    ).where(_REPLICAS.c.service_name == service_name).order_by(
        _REPLICAS.c.service_name, _REPLICAS.c.replica_id).limit(limit)


def replica_value_query(service_name: str,
                        replica_ids: Sequence[int]) -> sqlalchemy.sql.Select:
    """Build an exact approved value read for at most one probe batch."""
    return sqlalchemy.select(
        _REPLICAS.c.service_name,
        _REPLICAS.c.replica_id,
        _REPLICAS.c.status,
        _REPLICAS.c.version,
        _REPLICAS.c.cluster_name,
    ).where(_REPLICAS.c.service_name == service_name,
            _REPLICAS.c.replica_id.in_(tuple(replica_ids))).order_by(
                _REPLICAS.c.service_name, _REPLICAS.c.replica_id)


def job_info_length_query(spot_job_id: int) -> sqlalchemy.sql.Select:
    """Build the first-phase point read for one managed-job owner."""
    return sqlalchemy.select(
        _JOB_INFO.c.spot_job_id,
        _length(_JOB_INFO.c.workspace),
        _length(_JOB_INFO.c.controller_instance_id),
        _JOB_INFO.c.controller_generation,
        _length(_JOB_INFO.c.pool),
        _length(_JOB_INFO.c.current_cluster_name),
        _JOB_INFO.c.is_batch,
    ).where(_JOB_INFO.c.spot_job_id == spot_job_id).limit(1)


def job_info_value_query(spot_job_id: int) -> sqlalchemy.sql.Select:
    """Build the approved scalar-value read for one managed-job owner."""
    return sqlalchemy.select(
        _JOB_INFO.c.spot_job_id,
        _JOB_INFO.c.workspace,
        _JOB_INFO.c.controller_instance_id,
        _JOB_INFO.c.controller_generation,
        _JOB_INFO.c.pool,
        _JOB_INFO.c.current_cluster_name,
        _JOB_INFO.c.is_batch,
    ).where(_JOB_INFO.c.spot_job_id == spot_job_id).limit(1)


def spot_task_length_query(spot_job_id: int,
                           task_id: int,
                           limit: int = 2) -> sqlalchemy.sql.Select:
    """Build the first-phase indexed logical-task read (two detects a dup)."""
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ValueError('Spot-task probe limit must be a positive integer.')
    return sqlalchemy.select(
        _SPOT.c.job_id,
        _SPOT.c.spot_job_id,
        _SPOT.c.task_id,
        _length(_SPOT.c.task_name),
        _length(_SPOT.c.status),
    ).where(_SPOT.c.spot_job_id == spot_job_id,
            _SPOT.c.task_id == task_id).order_by(_SPOT.c.spot_job_id,
                                                 _SPOT.c.task_id,
                                                 _SPOT.c.job_id).limit(limit)


def spot_task_value_query(job_ids: Sequence[int]) -> sqlalchemy.sql.Select:
    """Build the approved value read for probed logical-task row IDs."""
    return sqlalchemy.select(
        _SPOT.c.job_id,
        _SPOT.c.spot_job_id,
        _SPOT.c.task_id,
        _SPOT.c.task_name,
        _SPOT.c.status,
    ).where(_SPOT.c.job_id.in_(tuple(job_ids))).order_by(
        _SPOT.c.spot_job_id, _SPOT.c.task_id, _SPOT.c.job_id)


def cluster_length_query(name: str) -> sqlalchemy.sql.Select:
    """Build the first-phase current-registry point read."""
    return sqlalchemy.select(
        _CLUSTERS.c.name,
        _length(_CLUSTERS.c.cluster_hash),
        _length(_CLUSTERS.c.status),
        _length(_CLUSTERS.c.workspace),
        _CLUSTERS.c.is_managed,
        _length(_CLUSTERS.c.workload_type),
        _length(_CLUSTERS.c.workload_id),
        _CLUSTERS.c.workload_task_id,
    ).where(_CLUSTERS.c.name == name).limit(1)


def cluster_value_query(name: str) -> sqlalchemy.sql.Select:
    """Build the approved current-registry scalar-value read."""
    return sqlalchemy.select(
        _CLUSTERS.c.name,
        _CLUSTERS.c.cluster_hash,
        _CLUSTERS.c.status,
        _CLUSTERS.c.workspace,
        _CLUSTERS.c.is_managed,
        _CLUSTERS.c.workload_type,
        _CLUSTERS.c.workload_id,
        _CLUSTERS.c.workload_task_id,
    ).where(_CLUSTERS.c.name == name).limit(1)


def cluster_history_length_query(cluster_hash: str) -> sqlalchemy.sql.Select:
    """Build the first-phase history-scalar point read."""
    return sqlalchemy.select(
        _CLUSTER_HISTORY.c.cluster_hash,
        _length(_CLUSTER_HISTORY.c.workspace),
        _length(_CLUSTER_HISTORY.c.cloud),
        _length(_CLUSTER_HISTORY.c.region),
        _length(_CLUSTER_HISTORY.c.zone),
        _CLUSTER_HISTORY.c.num_nodes,
        _CLUSTER_HISTORY.c.is_managed,
        _length(_CLUSTER_HISTORY.c.workload_type),
        _length(_CLUSTER_HISTORY.c.workload_id),
        _CLUSTER_HISTORY.c.workload_task_id,
    ).where(_CLUSTER_HISTORY.c.cluster_hash == cluster_hash).limit(1)


def cluster_history_value_query(cluster_hash: str) -> sqlalchemy.sql.Select:
    """Build the approved history scalar-value read."""
    return sqlalchemy.select(
        _CLUSTER_HISTORY.c.cluster_hash,
        _CLUSTER_HISTORY.c.workspace,
        _CLUSTER_HISTORY.c.cloud,
        _CLUSTER_HISTORY.c.region,
        _CLUSTER_HISTORY.c.zone,
        _CLUSTER_HISTORY.c.num_nodes,
        _CLUSTER_HISTORY.c.is_managed,
        _CLUSTER_HISTORY.c.workload_type,
        _CLUSTER_HISTORY.c.workload_id,
        _CLUSTER_HISTORY.c.workload_task_id,
    ).where(_CLUSTER_HISTORY.c.cluster_hash == cluster_hash).limit(1)


def _text_size(value: Any) -> int:
    if value is None:
        return 0
    if not isinstance(value, str):
        raise SourceDecodeError('Variable-width source values must be text.')
    try:
        return len(value.encode('utf-8'))
    except UnicodeEncodeError as e:
        raise SourceDecodeError(
            'Variable-width source values must be valid UTF-8.') from e


class PartitionSourceCache:
    """Bounded, negative-result-aware reads for one RR partition snapshot."""

    def __init__(self,
                 connection: sqlalchemy.engine.Connection,
                 limits: SourceReadLimits | None = None,
                 *,
                 deadline_monotonic: float | None = None,
                 clock: Any = time.monotonic) -> None:
        self._connection = connection
        self._limits = limits or SourceReadLimits()
        self._clock = clock
        self._deadline_monotonic = (clock() + 30.0 if deadline_monotonic is None
                                    else deadline_monotonic)
        if not isinstance(self._deadline_monotonic, (int, float)):
            raise ValueError('deadline_monotonic must be numeric.')
        self._source_rows = 0
        self._rows_seen = 0
        self._retained_bytes = 0
        self._services: dict[str, Mapping[str, Any] | None] = {}
        self._replicas: dict[str, tuple[Mapping[str, Any], ...]] = {}
        self._job_info: dict[int, Mapping[str, Any] | None] = {}
        self._spot_tasks: dict[tuple[int, int], tuple[Mapping[str, Any],
                                                      ...]] = {}
        self._clusters: dict[str, Mapping[str, Any] | None] = {}
        self._history: dict[str, Mapping[str, Any] | None] = {}

    @property
    def source_rows(self) -> int:
        return self._source_rows

    @property
    def rows_seen(self) -> int:
        return self._rows_seen

    def check_deadline(self) -> None:
        if self._clock() >= self._deadline_monotonic:
            raise SourceReadDeadlineExceededError(
                'Physical-capacity partition deadline exceeded.')

    def retain_canonical_bytes(self, encoded: bytes) -> None:
        """Charge one evidence envelope before the adapter retains its DTO."""
        self.check_deadline()
        if not isinstance(encoded, bytes):
            raise TypeError('encoded canonical evidence must be bytes.')
        if self._retained_bytes + len(
                encoded) > self._limits.max_retained_bytes:
            raise ByteLimitExceededError(
                'Combined source/evidence retention exceeds its byte bound.')
        self._retained_bytes += len(encoded)

    def _mapping_rows(
            self, statement: sqlalchemy.sql.Select) -> list[Mapping[str, Any]]:
        self.check_deadline()
        result = self._connection.execution_options(
            stream_results=True,
            yield_per=self._limits.fetch_batch_rows).execute(statement)
        rows: list[Mapping[str, Any]] = []
        for batch in result.mappings().partitions(
                self._limits.fetch_batch_rows):
            self.check_deadline()
            rows.extend(dict(row) for row in batch)
        self.check_deadline()
        return rows

    def _remaining(self, *, source: bool) -> int:
        total_remaining = self._limits.max_total_rows - self._rows_seen
        if not source:
            return total_remaining
        return min(total_remaining,
                   self._limits.max_source_rows - self._source_rows)

    def _probe_bytes(self, probes: Sequence[Mapping[str, Any]]) -> int:
        probe_bytes = 0
        for row in probes:
            for key, value in row.items():
                if key.endswith('_bytes'):
                    if value is None:
                        probe_bytes += 1
                        continue
                    if (not isinstance(value, int) or isinstance(value, bool) or
                            value < 0):
                        raise SourceDecodeError(
                            'Source length probes must return non-negative '
                            'integers.')
                    if value > self._limits.max_value_bytes:
                        raise ByteLimitExceededError(
                            'A source value exceeds the per-value byte bound.')
                    probe_bytes += value + 8
                elif isinstance(value, str):
                    probe_bytes += _text_size(value)
                else:
                    # Conservatively account fixed-width scalars, NULL tags,
                    # and per-field framing in the serialized probe.
                    probe_bytes += 16
        return probe_bytes

    def _preflight(self, probes: Sequence[Mapping[str, Any]]) -> int:
        batch_bytes = self._probe_bytes(probes)
        if batch_bytes > self._limits.max_fetch_batch_bytes:
            raise ByteLimitExceededError(
                'A source fetch batch exceeds its byte bound.')
        # The probe remains live while its approved value batch is decoded.
        # Counting it twice conservatively bounds that transient overlap.
        if self._retained_bytes + 2 * batch_bytes > (
                self._limits.max_retained_bytes):
            raise ByteLimitExceededError(
                'Retained source input exceeds its byte bound.')
        return batch_bytes

    def _charge_rows(self, rows: Sequence[Mapping[str, Any]], *,
                     source: bool) -> None:
        count = len(rows)
        if count > self._remaining(source=source):
            raise RowLimitExceededError('Partition row budget exceeded.')
        row_bytes = 0
        for row in rows:
            for value in row.values():
                if isinstance(value, str):
                    size = _text_size(value)
                    if size > self._limits.max_value_bytes:
                        raise ByteLimitExceededError(
                            'A source value exceeds the per-value byte bound.')
                    row_bytes += size
                elif value is not None:
                    row_bytes += 8
        if self._retained_bytes + row_bytes > self._limits.max_retained_bytes:
            raise ByteLimitExceededError(
                'Retained source input exceeds its byte bound.')
        self._rows_seen += count
        if source:
            self._source_rows += count
        self._retained_bytes += row_bytes

    def _point_read(self, length_statement: sqlalchemy.sql.Select,
                    value_statement: sqlalchemy.sql.Select, *, source: bool,
                    expected_key: tuple[str, Any]) -> Mapping[str, Any] | None:
        probes = self._mapping_rows(length_statement)
        if len(probes) > 1:
            raise SourceDecodeError('Point lookup returned duplicate rows.')
        if not probes:
            return None
        if len(probes) > self._remaining(source=source):
            raise RowLimitExceededError('Partition row budget exceeded.')
        self._preflight(probes)
        rows = self._mapping_rows(value_statement)
        if len(rows) != 1 or rows[0].get(expected_key[0]) != expected_key[1]:
            raise SourceDecodeError(
                'Source value read did not match its length probe key.')
        self._charge_rows(rows, source=source)
        return rows[0]

    def service(self, name: str) -> Mapping[str, Any] | None:
        if name not in self._services:
            self._services[name] = self._point_read(service_length_query(name),
                                                    service_value_query(name),
                                                    source=True,
                                                    expected_key=('name', name))
        return self._services[name]

    def replicas(self, service_name: str) -> tuple[Mapping[str, Any], ...]:
        cached = self._replicas.get(service_name)
        if cached is not None:
            return cached
        remaining = self._remaining(source=True)
        probes = self._mapping_rows(
            replica_length_query(service_name, max(1, remaining + 1)))
        if len(probes) > remaining:
            raise RowLimitExceededError('Partition source-row budget exceeded.')
        values: list[Mapping[str, Any]] = []
        probe_bytes = self._probe_bytes(probes)
        if self._retained_bytes + probe_bytes > self._limits.max_retained_bytes:
            raise ByteLimitExceededError(
                'Aggregate replica probes exceed the retained-input bound.')
        # The full prefix probe remains live until all approved value batches
        # have been fetched, so charge it during that overlap.
        self._retained_bytes += probe_bytes
        try:
            for offset in range(0, len(probes), self._limits.fetch_batch_rows):
                batch = probes[offset:offset + self._limits.fetch_batch_rows]
                batch_bytes = self._probe_bytes(batch)
                if batch_bytes > self._limits.max_fetch_batch_bytes:
                    raise ByteLimitExceededError(
                        'A source fetch batch exceeds its byte bound.')
                # The value batch is approximately the probe-declared text
                # plus fixed scalars. Reserve that serialized input before
                # asking the driver to materialize it.
                if self._retained_bytes + batch_bytes > (
                        self._limits.max_retained_bytes):
                    raise ByteLimitExceededError(
                        'Replica probe/value overlap exceeds the retained-input '
                        'bound.')
                raw_replica_ids = [row.get('replica_id') for row in batch]
                if any(
                        type(replica_id) is not int
                        for replica_id in raw_replica_ids):
                    raise SourceDecodeError('Replica IDs must be integers.')
                replica_ids = [
                    cast(int, replica_id) for replica_id in raw_replica_ids
                ]
                fetched = self._mapping_rows(
                    replica_value_query(service_name, replica_ids))
                expected = [
                    (service_name, replica_id) for replica_id in replica_ids
                ]
                actual = [(row.get('service_name'), row.get('replica_id'))
                          for row in fetched]
                if actual != expected:
                    raise SourceDecodeError(
                        'Replica values did not match their length probe keys.')
                self._charge_rows(fetched, source=True)
                values.extend(fetched)
        finally:
            self._retained_bytes -= probe_bytes
        cached = tuple(values)
        self._replicas[service_name] = cached
        return cached

    def job_info(self, spot_job_id: int) -> Mapping[str, Any] | None:
        if spot_job_id not in self._job_info:
            self._job_info[spot_job_id] = self._point_read(
                job_info_length_query(spot_job_id),
                job_info_value_query(spot_job_id),
                source=True,
                expected_key=('spot_job_id', spot_job_id))
        return self._job_info[spot_job_id]

    def spot_tasks(self, spot_job_id: int,
                   task_id: int) -> tuple[Mapping[str, Any], ...]:
        cache_key = (spot_job_id, task_id)
        cached = self._spot_tasks.get(cache_key)
        if cached is not None:
            return cached
        remaining = self._remaining(source=True)
        probe_limit = min(2, max(1, remaining + 1))
        probes = self._mapping_rows(
            spot_task_length_query(spot_job_id, task_id, probe_limit))
        if len(probes) > remaining:
            raise RowLimitExceededError('Partition source-row budget exceeded.')
        self._preflight(probes)
        if not probes:
            self._spot_tasks[cache_key] = ()
            return ()
        raw_job_ids = [row.get('job_id') for row in probes]
        if any(type(job_id) is not int for job_id in raw_job_ids):
            raise SourceDecodeError('Spot row IDs must be integers.')
        job_ids = [cast(int, job_id) for job_id in raw_job_ids]
        fetched = self._mapping_rows(spot_task_value_query(job_ids))
        expected = [(row.get('job_id'), row.get('spot_job_id'),
                     row.get('task_id')) for row in probes]
        actual = [(row.get('job_id'), row.get('spot_job_id'),
                   row.get('task_id')) for row in fetched]
        if actual != expected:
            raise SourceDecodeError(
                'Managed-task values did not match length probe keys.')
        self._charge_rows(fetched, source=True)
        cached = tuple(fetched)
        self._spot_tasks[cache_key] = cached
        return cached

    def cluster(self, name: str) -> Mapping[str, Any] | None:
        if name not in self._clusters:
            self._clusters[name] = self._point_read(cluster_length_query(name),
                                                    cluster_value_query(name),
                                                    source=False,
                                                    expected_key=('name', name))
        return self._clusters[name]

    def cluster_history(self, cluster_hash: str) -> Mapping[str, Any] | None:
        if cluster_hash not in self._history:
            self._history[cluster_hash] = self._point_read(
                cluster_history_length_query(cluster_hash),
                cluster_history_value_query(cluster_hash),
                source=False,
                expected_key=('cluster_hash', cluster_hash))
        return self._history[cluster_hash]


class InMemorySourceReader:
    """Small test seam with the same negative-result cache semantics."""

    def __init__(
        self,
        *,
        services: Mapping[str, Mapping[str, Any]] | None = None,
        replicas: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        job_info: Mapping[int, Mapping[str, Any]] | None = None,
        spot_tasks: Mapping[tuple[int, int], Sequence[Mapping[str, Any]]] |
        None = None,
        clusters: Mapping[str, Mapping[str, Any]] | None = None,
        history: Mapping[str, Mapping[str, Any]] | None = None,
        limits: SourceReadLimits | None = None,
        deadline_monotonic: float | None = None,
        clock: Any = time.monotonic,
    ) -> None:
        self._services = dict(services or {})
        self._replicas = {
            key: tuple(rows) for key, rows in (replicas or {}).items()
        }
        self._job_info = dict(job_info or {})
        self._spot_tasks = {
            key: tuple(rows) for key, rows in (spot_tasks or {}).items()
        }
        self._clusters = dict(clusters or {})
        self._history = dict(history or {})
        self._seen: set[tuple[str, Any]] = set()
        self._source_rows = 0
        self._rows_seen = 0
        self._retained_bytes = 0
        self._limits = limits or SourceReadLimits()
        self._clock = clock
        self._deadline_monotonic = (clock() + 30.0 if deadline_monotonic is None
                                    else deadline_monotonic)

    @property
    def source_rows(self) -> int:
        return self._source_rows

    @property
    def rows_seen(self) -> int:
        return self._rows_seen

    def check_deadline(self) -> None:
        if self._clock() >= self._deadline_monotonic:
            raise SourceReadDeadlineExceededError(
                'Physical-capacity partition deadline exceeded.')

    def retain_canonical_bytes(self, encoded: bytes) -> None:
        self.check_deadline()
        if not isinstance(encoded, bytes):
            raise TypeError('encoded canonical evidence must be bytes.')
        if self._retained_bytes + len(
                encoded) > self._limits.max_retained_bytes:
            raise ByteLimitExceededError(
                'Combined source/evidence retention exceeds its byte bound.')
        self._retained_bytes += len(encoded)

    @staticmethod
    def _row_bytes(row: Mapping[str, Any]) -> int:
        total = 0
        for value in row.values():
            total += _text_size(value) if isinstance(value, str) else 8
        return total

    def _charge(self, kind: str, key: Any, count: int, *, source: bool) -> None:
        self.check_deadline()
        marker = (kind, key)
        if marker in self._seen:
            return
        if self._rows_seen + count > self._limits.max_total_rows:
            raise RowLimitExceededError('Partition total-row budget exceeded.')
        if source and self._source_rows + count > self._limits.max_source_rows:
            raise RowLimitExceededError('Partition source-row budget exceeded.')
        self._seen.add(marker)
        self._rows_seen += count
        if source:
            self._source_rows += count

    def _charge_values(self, rows: Sequence[Mapping[str, Any]]) -> None:
        row_bytes = sum(self._row_bytes(row) for row in rows)
        if self._retained_bytes + row_bytes > self._limits.max_retained_bytes:
            raise ByteLimitExceededError(
                'Combined source/evidence retention exceeds its byte bound.')
        self._retained_bytes += row_bytes

    def service(self, name: str) -> Mapping[str, Any] | None:
        row = self._services.get(name)
        marker = ('service', name)
        if row is not None and marker not in self._seen:
            self._charge_values([row])
        self._charge('service', name, int(row is not None), source=True)
        return row

    def replicas(self, service_name: str) -> tuple[Mapping[str, Any], ...]:
        rows = self._replicas.get(service_name, ())
        marker = ('replicas', service_name)
        if marker not in self._seen:
            self._charge_values(rows)
        self._charge('replicas', service_name, len(rows), source=True)
        return rows

    def job_info(self, spot_job_id: int) -> Mapping[str, Any] | None:
        row = self._job_info.get(spot_job_id)
        marker = ('job_info', spot_job_id)
        if row is not None and marker not in self._seen:
            self._charge_values([row])
        self._charge('job_info', spot_job_id, int(row is not None), source=True)
        return row

    def spot_tasks(self, spot_job_id: int,
                   task_id: int) -> tuple[Mapping[str, Any], ...]:
        key = (spot_job_id, task_id)
        rows = self._spot_tasks.get(key, ())
        marker = ('spot', key)
        if marker not in self._seen:
            self._charge_values(rows)
        self._charge('spot', key, len(rows), source=True)
        return rows

    def cluster(self, name: str) -> Mapping[str, Any] | None:
        row = self._clusters.get(name)
        marker = ('cluster', name)
        if row is not None and marker not in self._seen:
            self._charge_values([row])
        self._charge('cluster', name, int(row is not None), source=False)
        return row

    def cluster_history(self, cluster_hash: str) -> Mapping[str, Any] | None:
        row = self._history.get(cluster_hash)
        marker = ('history', cluster_hash)
        if row is not None and marker not in self._seen:
            self._charge_values([row])
        self._charge('history',
                     cluster_hash,
                     int(row is not None),
                     source=False)
        return row


def selected_column_names(statement: sqlalchemy.sql.Select) -> tuple[str, ...]:
    """Return selected labels/names for source-column allowlist tests."""
    return tuple(column.key for column in statement.selected_columns)


def selected_table_names(statement: sqlalchemy.sql.Select) -> tuple[str, ...]:
    """Return relation names reached by one narrow source statement."""
    return tuple(
        sorted(from_clause.name for from_clause in statement.get_final_froms()))


def iter_query_builders() -> Iterable[sqlalchemy.sql.Select]:
    """Yield representative statements for static source-read audits."""
    yield service_length_query('service')
    yield service_value_query('service')
    yield replica_length_query('service', 1)
    yield replica_value_query('service', [0])
    yield job_info_length_query(1)
    yield job_info_value_query(1)
    yield spot_task_length_query(1, 0)
    yield spot_task_value_query([1])
    yield cluster_length_query('cluster')
    yield cluster_value_query('cluster')
    yield cluster_history_length_query('hash')
    yield cluster_history_value_query('hash')
