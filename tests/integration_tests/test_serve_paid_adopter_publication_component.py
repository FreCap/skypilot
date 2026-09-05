"""Component regression for committed paid-wave publication versus recovery.

Enter the production replica-manager refresher and post-commit materializer
concurrently. Keep their mutation runtime, scanner, task/adopter construction,
publication, identity checks, and batch-admission selection intact. Substitute
only the adjacent repository interface with an in-memory committed-wave store.
This is not PostgreSQL or provider E2E proof; no launch worker is admitted.

The negative control is the pre-fix refresher: its scanner holds the manager
mutex during each repository inspection, blocking both paid publication and
the mutex snapshots needed by readiness/route publication. The test pauses
that exact inspection, rather than replacing the scanner being tested.
"""
# pylint: disable=protected-access

from __future__ import annotations

import dataclasses
import hashlib
import pickle
import threading
from unittest import mock
import uuid

import pytest

from sky import clouds
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import paid_launch_request
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.server.requests import paid_wave_admission
from sky.server.requests import postgres as request_postgres
from sky.utils import thread_utils

pytestmark = pytest.mark.component

_SERVICE = 'adopter-component'
_HASH = 'adopter-component-hash'
_OWNER = (123, '127.0.0.1')
_WAVE_SIZE = paid_capacity.MAX_ATOMIC_PAID_ADMISSION_WAVE_MEMBERS
_YAML = 'resources:\n  ports: 8080\nrun: echo component\n'


@dataclasses.dataclass(frozen=True)
class _CommittedWave:
    specs: tuple[paid_capacity.PaidLaunchSpec, ...]
    receipt: paid_capacity.PaidLaunchReceipt
    bindings: tuple[paid_wave_admission.FusedBindingReceiptMember, ...]
    infos: tuple[replica_managers.ReplicaInfo, ...]


def _manager_and_wave(
) -> tuple[replica_managers.SkyPilotReplicaManager, _CommittedWave]:
    """Seed the exact post-transaction boundary without launching daemons."""
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = _SERVICE
    manager._service_hash = _HASH
    manager._resource_scope = _HASH
    manager._workspace = 'default'
    manager._controller_owner = _OWNER
    manager._next_replica_id = 1
    manager.latest_version = 1
    manager.yaml_content = _YAML
    manager._uses_logical_replicas = False
    manager._ordinary_launch_binding_authority = (
        ordinary_launch_binding.ControllerBindingAuthority(
            service_name=_SERVICE,
            service_hash=_HASH,
            service_workspace='default',
            service_lifecycle_epoch=1,
            controller_pid=_OWNER[0],
            controller_ip=_OWNER[1],
            controller_incarnation=uuid.UUID(
                '00000000-0000-4000-8000-000000000123'),
            controller_owner_epoch=7,
            capable=True,
            binding_mode=ordinary_launch_binding.BindingMode.BOUND,
            binding_epoch=2,
            non_pool_capable=True,
            non_pool_binding_protocol_version=(
                ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
            non_pool_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            non_pool_capability_cohort_epoch=(
                ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
            non_pool_receipt_protocol_version=(
                ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)))
    spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                       ports='8080',
                                       initial_delay_seconds=0,
                                       readiness_timeout_seconds=5,
                                       endpoint_probe_interval_seconds=1,
                                       lb_stream_timeout_seconds=10,
                                       min_replicas=0,
                                       max_replicas=_WAVE_SIZE,
                                       target_concurrency_per_replica=1,
                                       spot_placer='dynamic_fallback')
    manager._version_specs = {1: spec}
    location = spot_placer.Location(cloud=clouds.GCP(),
                                    region='us-central1',
                                    zone='us-central1-a',
                                    accelerators={'L4': 1},
                                    use_spot=True,
                                    instance_type='g2-standard-4')
    task = serve_utils.load_task_with_service_spec(_YAML, spec)
    manager._spot_placer = spot_placer.SpotPlacer(
        task,
        spec.placement_contract,
        placement_catalog=spot_placer.PlacementCatalog(((location, 0.1),),
                                                       num_nodes=1))
    pool_key = paid_capacity.pool_key(location,
                                      workspace='default',
                                      num_nodes=1,
                                      gcp_project_id='test-project')
    serialized_spec = pickle.dumps(spec, protocol=4)
    config = b'active_workspace: default\nworkspaces:\n  default:\n    gcp:\n      project_id: test-project\n'
    authority = paid_capacity.PaidLaunchVersionAuthority(
        service_spec=serialized_spec,
        service_spec_sha256=hashlib.sha256(serialized_spec).hexdigest(),
        controller_config=config,
        controller_config_digest=hashlib.sha256(config).hexdigest(),
        controller_config_snapshot_id='c' * 64)
    budget = paid_capacity.LaunchBudget(
        remaining_by_location={location: _WAVE_SIZE},
        pool_key_by_location={location: pool_key},
        states_by_pool_key={
            pool_key: {
                'remaining': _WAVE_SIZE,
                'admission_state': 'active'
            }
        },
        globally_managed=True,
        service_remaining=_WAVE_SIZE,
        service_claim_limit=_WAVE_SIZE,
        frontier_limit=_WAVE_SIZE,
        max_frontier_limit=_WAVE_SIZE,
        frontier_key_by_location={
            location: paid_capacity.frontier_key(location)
        })
    templates = manager.prepare_paid_launch_templates(
        accelerator_shapes={'L4': 1},
        version_authority=authority,
        paid_location_launch_budget=budget)
    assert len(templates) == 1
    template = templates[0]
    body_template = paid_launch_request.PaidLaunchBodyTemplate(
        submitted_bytes=template.prepared_launch_body_template)
    specs = []
    bindings = []
    members = []
    infos = []
    for ordinal in range(_WAVE_SIZE):
        replica_id = ordinal + 1
        record_id = uuid.uuid5(uuid.NAMESPACE_URL, f'component:{replica_id}')
        cluster_name = serve_utils.generate_replica_cluster_name(
            _SERVICE, replica_id, _HASH)
        request = paid_launch_request.materialize_paid_launch_request(
            body_template,
            replica_id=replica_id,
            cluster_name=cluster_name,
            launch_fence={})
        worker = paid_capacity.freeze_paid_launch_payload({
            'schema_version': 1,
            'launch_yaml_content': _YAML,
            'cluster_name': cluster_name,
            'log_file_name': serve_utils.generate_replica_launch_log_file_name(
                _SERVICE, replica_id, _HASH),
            'resources_override': paid_capacity.thaw_paid_launch_payload(
                template.resources_override),
            'retry_until_up': False,
            'frozen_controller_config_path': '/unused/config.yaml',
        })
        launch_spec = paid_capacity.PaidLaunchSpec(
            ordinal=ordinal,
            service_name=_SERVICE,
            service_hash=_HASH,
            service_lifecycle_epoch=1,
            service_version=1,
            replica_id=replica_id,
            replica_record_id=str(record_id),
            cluster_name_seed=cluster_name,
            worker_construction=worker,
            prepared_launch_request=request.submitted_bytes,
            provider_account=None,
            provider_project_id='test-project',
            cloud=template.cloud,
            workspace=template.workspace,
            region=template.region,
            zone=template.zone,
            instance_type=template.instance_type,
            pool_key=pool_key,
            frontier_key=template.frontier_key,
            accelerator=template.accelerator,
            gpu_units_per_node=1,
            num_nodes=1,
            resources_override=template.resources_override,
            catalog_evidence=paid_capacity.PaidLaunchCatalogEvidence(
                placement_catalog_sha256=template.placement_catalog_sha256,
                catalog_rank=template.catalog_rank,
                exploration_round=0,
                slot_within_pool_window=ordinal,
                version_authority=authority))
        specs.append(launch_spec)
        members.append(
            paid_capacity.PaidLaunchReceiptMember(
                replica_id=replica_id,
                replica_record_id=str(record_id),
                pool_key=pool_key,
                priority=50,
                accelerator=template.accelerator,
                plan_units=1,
                physical_gpu_units=1))
        association_id = uuid.uuid5(uuid.NAMESPACE_OID, str(record_id))
        request_id = f'paid-request-{replica_id}'
        profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
            ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
            authorization_reference=
            f'paid-capacity:{_HASH}:{record_id}:{pool_key}',
            authorization_generation=9,
            authorization_payload={'pool_key': pool_key})
        context = ordinary_launch_binding.BoundNonPoolLaunchContext(
            association_id=association_id,
            request_id=request_id,
            service_name=_SERVICE,
            replica_id=replica_id,
            replica_record_id=record_id,
            launch_generation=1,
            input_digest='a' * 64,
            profile=profile,
            capability_cohort_epoch=ordinary_launch_binding.
            NON_POOL_CAPABILITY_COHORT_EPOCH,
            capability_profile_set_digest=ordinary_launch_binding.
            supported_non_pool_profile_set_digest(),
            receipt_protocol_version=ordinary_launch_binding.
            NON_POOL_RECEIPT_PROTOCOL_VERSION)
        bindings.append(
            paid_wave_admission.FusedBindingReceiptMember(
                replica_id=replica_id,
                replica_record_id=str(record_id),
                submission_id=uuid.uuid5(uuid.NAMESPACE_DNS, str(record_id)),
                association_id=str(association_id),
                request_id=request_id,
                launch_generation=1,
                context=context))
        infos.append(
            replica_managers.ReplicaInfo.from_storage_dict(
                paid_capacity.build_pristine_paid_replica_state(
                    launch_spec,
                    replica_port='8080',
                    planned_capacity=1,
                    created_at=123.0)))
    receipt = paid_capacity.PaidLaunchReceipt(service_name=_SERVICE,
                                              service_hash=_HASH,
                                              service_lifecycle_epoch=1,
                                              service_version=1,
                                              capacity_plan_generation=9,
                                              capacity_plan_sha256='a' * 64,
                                              capacity_unit='physical-backend',
                                              members=tuple(members))
    return manager, _CommittedWave(tuple(specs), receipt, tuple(bindings),
                                   tuple(infos))


def test_committed_wave_publishes_while_scanner_repository_read_is_blocked(
        monkeypatch: pytest.MonkeyPatch) -> None:
    manager, wave = _manager_and_wave()
    by_id = {info.replica_id: info for info in wave.infos}
    read_started = threading.Event()
    release_read = threading.Event()
    published = threading.Event()
    route_snapshot = threading.Event()
    inspected = []
    materialized = []

    def _infos(service_name):
        assert service_name == _SERVICE
        return list(wave.infos)

    def _inspect(service_name, replica_id, record_id):
        assert service_name == _SERVICE
        assert by_id[replica_id].replica_record_id == record_id
        inspected.append(replica_id)
        if len(inspected) == 1:
            read_started.set()
            assert release_read.wait(timeout=10)
        return request_postgres.OrdinaryLaunchReduction(
            context=wave.bindings[replica_id - 1].context,
            disposition=request_postgres.OrdinaryLaunchReductionDisposition.
            ADOPT_ACTIVE,
            request=mock.Mock(),
            service_job_id=None,
            cancel_reason=None,
            projected=False)

    monkeypatch.setattr(serve_state, 'get_replica_infos', _infos)
    monkeypatch.setattr(serve_state, 'get_replica_infos_from_ids',
                        lambda name, ids: {key: by_id[key] for key in ids})
    monkeypatch.setattr(serve_state, 'set_service_spot_placement_state',
                        lambda *_args: True)
    monkeypatch.setattr(
        serve_state, 'get_service_controller_owner', lambda name: {
            'hash': _HASH,
            'controller_pid': _OWNER[0],
            'controller_ip': _OWNER[1],
            'status': serve_state.ServiceStatus.READY
        })
    monkeypatch.setattr(ordinary_launch_binding,
                        'list_provider_reconciliation_contexts', lambda _: [])
    monkeypatch.setattr(request_postgres, 'inspect_bound_ordinary_launch',
                        _inspect)
    reserve_batch = mock.Mock(return_value={})
    reserve_one = mock.Mock(side_effect=AssertionError('per-row admission'))
    monkeypatch.setattr(serve_state,
                        'reserve_replica_launches_running_if_capacity',
                        reserve_batch)
    monkeypatch.setattr(serve_state,
                        'reserve_replica_launch_running_if_capacity',
                        reserve_one)

    def _publish():
        materialized.extend(
            manager.materialize_paid_launch_receipt(wave.receipt, wave.bindings,
                                                    wave.specs))
        published.set()

    def _snapshot():
        with manager.lock:
            route_snapshot.set()

    scanner = thread_utils.SafeThread(target=manager._refresh_thread_pool)
    publisher = thread_utils.SafeThread(target=_publish)
    readiness = thread_utils.SafeThread(target=_snapshot)
    scanner.start()
    try:
        assert read_started.wait(timeout=5), scanner.format_exc
        publisher.start()
        readiness.start()
        assert published.wait(timeout=3), publisher.format_exc
        assert route_snapshot.wait(timeout=1)
    finally:
        release_read.set()
        for worker in (scanner, publisher, readiness):
            if worker.ident is not None:
                worker.join(timeout=10)
                assert not worker.is_alive()
    assert scanner.exception is None
    assert publisher.exception is None
    assert readiness.exception is None
    assert inspected == [1]
    assert len(materialized) == _WAVE_SIZE
    workers = dict(manager._launch_thread_pool.items())
    assert set(workers) == set(by_id)
    assert len({id(worker) for worker in workers.values()}) == _WAVE_SIZE
    assert all(worker.ident is None for worker in workers.values())
    assert all(worker.replica_record_id == by_id[key].replica_record_id
               for key, worker in workers.items())

    # The following ordinary refresher owns one batch admission; publication
    # and recovery must never reserve or start individual workers themselves.
    reserve_batch.assert_not_called()
    manager._refresh_thread_pool()
    reserve_batch.assert_called_once()
    candidates = reserve_batch.call_args.args[1]
    assert len(candidates) == _WAVE_SIZE
    assert {replica_id for replica_id, _, _ in candidates} == set(by_id)
    assert all(bound is True for _, _, bound in candidates)
    reserve_one.assert_not_called()
    assert inspected == [1]
    assert all(worker.ident is None for worker in workers.values())
