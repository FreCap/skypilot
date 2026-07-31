"""Controller-owned daemon for the temporary capacity evidence pilot."""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
import datetime
import threading
import time
from typing import Any

import sqlalchemy

from sky import sky_logging
from sky.physical_capacity import adapters
from sky.physical_capacity import config as capacity_config
from sky.physical_capacity import contracts
from sky.physical_capacity import hashing
from sky.physical_capacity import metrics
from sky.physical_capacity import repository as capacity_repository
from sky.physical_capacity import source_queries
from sky.physical_capacity import state

logger = sky_logging.init_logger(__name__)

_SLOT_SECONDS = 900

_ScanAdapter = Callable[..., contracts.PartitionEvidenceResult]


def _failure_for(error: Exception,
                 *,
                 rows_seen: int = 0) -> capacity_repository.ScanFailure:
    if isinstance(error, capacity_repository.ScanFailure):
        return error
    if isinstance(error, source_queries.RowLimitExceededError):
        code = 'row_limit_exceeded'
    elif isinstance(error, source_queries.ByteLimitExceededError):
        code = 'byte_limit_exceeded'
    elif isinstance(error, source_queries.SourceConflictError):
        code = 'source_conflict'
    elif isinstance(error, source_queries.SelectorMismatchError):
        code = 'selector_mismatch'
    elif isinstance(error, source_queries.SourceReadDeadlineExceededError):
        code = 'scan_timeout'
    else:
        code = 'source_decode_failed'
    return capacity_repository.ScanFailure(code, rows_seen=rows_seen)


def _slot_for(partition_hash: str, now: datetime.datetime) -> datetime.datetime:
    jitter = hashing.slot_jitter_seconds(partition_hash)
    timestamp = int(now.timestamp())
    slot_number = (timestamp - jitter) // _SLOT_SECONDS
    slot_timestamp = slot_number * _SLOT_SECONDS + jitter
    return datetime.datetime.fromtimestamp(slot_timestamp,
                                           tz=datetime.timezone.utc)


def _scope_hash(config: capacity_config.CapacityConfig,
                partition: contracts.SourcePartition) -> str:
    allowlist = config.allowlist or capacity_config.CapacityAllowlist()
    assert config.pilot_end_utc is not None
    return hashing.projection_scope_hash(partition,
                                         config.sources,
                                         config.pilot_end_utc,
                                         owner_kinds=allowlist.owner_kinds,
                                         providers=allowlist.providers,
                                         groups=allowlist.groups,
                                         verbs=allowlist.verbs)


class EvidenceProjector:
    """Sequential, leader-fenced evidence projector with explicit shutdown."""

    def __init__(
        self,
        config: capacity_config.CapacityConfig,
        controller: capacity_repository.ControllerIdentity,
        base_engine: sqlalchemy.engine.Engine,
        *,
        scan_adapter: _ScanAdapter = adapters.scan_partition,
        schema_requirements: Callable[
            [], Sequence[Any]] = source_queries.source_schema_requirements,
        clock: Callable[[], datetime.datetime] | None = None,
    ) -> None:
        if config.mode is not capacity_config.CapacityMode.SHADOW:
            raise ValueError('EvidenceProjector requires shadow mode.')
        if config.pilot_end_utc is None or not config.sources:
            raise ValueError('Shadow projector requires sources and pilot end.')
        self._config = config
        self._controller = controller
        self._repository = capacity_repository.ScanRepository(
            base_engine, controller)
        self._scan_adapter = scan_adapter
        self._schema_requirements = schema_requirements
        self._clock = clock or (
            lambda: datetime.datetime.now(tz=datetime.timezone.utc))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot: capacity_repository.ActivationSnapshot | None = None
        self._healthy = False

    @property
    def repository(self) -> capacity_repository.ScanRepository:
        return self._repository

    @property
    def activation_snapshot(
            self) -> capacity_repository.ActivationSnapshot | None:
        return self._snapshot

    @property
    def healthy(self) -> bool:
        return self._healthy

    def start(self) -> None:
        """Synchronously validate activation, then start the daemon."""
        if self._thread is not None:
            raise RuntimeError('Evidence projector has already been started.')
        try:
            self._repository.validate_schema(self._schema_requirements())
            assert self._config.pilot_end_utc is not None
            self._snapshot = self._repository.load_activation_snapshot(
                self._config.partitions, self._config.pilot_end_utc)
        except BaseException:
            self._repository.close()
            raise
        self._healthy = True
        expired = self._clock() >= capacity_config.pilot_end_datetime(
            self._config.pilot_end_utc)
        metrics.set_projector_health(healthy=True, expired=expired)
        self._thread = threading.Thread(target=self._run,
                                        name='physical-capacity-evidence',
                                        daemon=False)
        self._thread.start()

    def _scan_partition(
        self, partition: contracts.SourcePartition,
        handle: capacity_repository.ScanHandle
    ) -> tuple[contracts.PartitionEvidenceResult, str]:
        primary_selectors = tuple(
            selector for selector in self._config.sources
            if contracts.selector_partition(selector) == partition)
        dependencies = hashing.dependency_selectors_for_partition(
            self._config.sources, partition)

        def read(
            source_reader: source_queries.SourceReader
        ) -> tuple[contracts.PartitionEvidenceResult, str]:
            result = self._scan_adapter(
                partition,
                primary_selectors,
                dependencies,
                source_reader,
                controller_instance_id=self._controller.instance_id,
                controller_generation=self._controller.generation)
            result.validate(len(primary_selectors))
            if (result.findings.source_rows != source_reader.source_rows or
                    result.rows_seen != source_reader.rows_seen):
                raise ValueError(
                    'Adapter/source-reader row accounting differs.')
            source_reader.check_deadline()
            digest = hashing.evidence_inventory_digest(result.records)
            source_reader.check_deadline()
            return result, digest

        return self._repository.read_evidence(handle, read)

    def _run_one(self, partition: contracts.SourcePartition) -> None:
        partition_hash = hashing.source_partition_hash(partition)
        scope_hash = _scope_hash(self._config, partition)
        assert self._config.pilot_end_utc is not None
        handle = self._repository.begin_scan(partition, partition_hash,
                                             scope_hash,
                                             self._config.pilot_end_utc)
        if handle is None:
            return
        slot = capacity_config.pilot_end_datetime(handle.scheduled_slot_utc)

        findings: dict[str, int] | None = None
        rows_seen = 0
        try:
            result, digest = self._scan_partition(partition, handle)
            rows_seen = result.rows_seen
            findings = result.findings.to_dict()
            published = self._repository.publish_completed(
                handle,
                rows_seen=rows_seen,
                finding_counts=findings,
                inventory_digest=digest)
            duration = time.monotonic() - handle.started_monotonic
            lag = max(0.0, (published.completed_at - slot).total_seconds())
            metrics.record_scan(workspace=partition.workspace,
                                source_kind=partition.source_kind.value,
                                succeeded=True,
                                duration_seconds=duration,
                                lag_seconds=lag,
                                rows_seen=rows_seen,
                                findings=findings,
                                digest_changed=published.digest_changed)
        except capacity_repository.ControllerFencedError:
            self._healthy = False
            metrics.set_projector_health(healthy=False, expired=False)
            self._stop.set()
            return
        except Exception as error:  # pylint: disable=broad-except
            if self._stop.is_set():
                return
            failure = _failure_for(error, rows_seen=rows_seen)
            committed = self._repository.publish_failed(handle, failure)
            if committed:
                metrics.record_scan(workspace=partition.workspace,
                                    source_kind=partition.source_kind.value,
                                    succeeded=False,
                                    duration_seconds=time.monotonic() -
                                    handle.started_monotonic,
                                    lag_seconds=max(0.0,
                                                    (self._clock() -
                                                     slot).total_seconds()),
                                    rows_seen=failure.rows_seen)
            else:
                logger.warning('Physical-capacity evidence failure '
                               'publication did not commit.')
            logger.warning('Physical-capacity evidence scan failed with '
                           f'closed code {failure.code}.')

    def _next_wait_seconds(self, now: datetime.datetime) -> float:
        waits: list[float] = []
        for partition in self._config.partitions:
            partition_hash = hashing.source_partition_hash(partition)
            slot = _slot_for(partition_hash, now)
            next_slot = slot + datetime.timedelta(seconds=_SLOT_SECONDS)
            waits.append(max(0.0, (next_slot - now).total_seconds()))
        return min(waits, default=1.0)

    def _run(self) -> None:
        assert self._config.pilot_end_utc is not None
        pilot_end = capacity_config.pilot_end_datetime(
            self._config.pilot_end_utc)
        try:
            while not self._stop.is_set():
                now = self._repository.current_database_time()
                if now >= pilot_end:
                    assert self._snapshot is not None
                    for partition in sorted(
                            self._snapshot.durable_partitions,
                            key=lambda item:
                        (item.workspace, item.source_kind.value)):
                        if self._stop.is_set():
                            break
                        try:
                            self._repository.finalize_stale(partition)
                        except capacity_repository.ControllerFencedError:
                            self._healthy = False
                            metrics.set_projector_health(healthy=False,
                                                         expired=False)
                            self._stop.set()
                            break
                    if self._stop.is_set():
                        return
                    metrics.set_projector_health(healthy=True, expired=True)
                    return
                for partition in self._config.partitions:
                    if self._stop.is_set():
                        break
                    self._run_one(partition)
                if self._stop.is_set():
                    return
                database_now = self._repository.current_database_time()
                if self._stop.wait(self._next_wait_seconds(database_now)):
                    return
        except Exception:  # pylint: disable=broad-except
            self._healthy = False
            metrics.set_projector_health(healthy=False, expired=False)
            logger.error('Physical-capacity evidence projector stopped '
                         'unexpectedly.')
        except BaseException:  # pylint: disable=broad-except
            self._healthy = False
            metrics.set_projector_health(healthy=False, expired=False)
            logger.error('Physical-capacity evidence projector stopped '
                         'because of a process-control exception.')
            raise

    def stop(self) -> None:
        """Cancel the active query, join the daemon, and dispose its pool."""
        self._stop.set()
        self._repository.cancel_active()
        thread = self._thread
        if thread is not None:
            thread.join()
            self._thread = None
        self._repository.close()
        self._healthy = False
        metrics.set_projector_health(healthy=False, expired=False)


def start_controller_projector(
    config: capacity_config.CapacityConfig,
    *,
    controller_instance_id: str,
    controller_generation: int,
    base_engine: sqlalchemy.engine.Engine | None = None,
) -> EvidenceProjector | None:
    """Create/start a projector only for explicitly enabled shadow mode."""
    if config.mode is capacity_config.CapacityMode.DISABLED:
        return None
    identity = capacity_repository.ControllerIdentity(controller_instance_id,
                                                      controller_generation)
    projector = EvidenceProjector(config, identity, base_engine or
                                  state.initialize_and_get_db())
    projector.start()
    return projector


def stop_controller_projector(projector: EvidenceProjector | None) -> None:
    """Idempotently stop one controller projector before lease release."""
    if projector is not None:
        projector.stop()
