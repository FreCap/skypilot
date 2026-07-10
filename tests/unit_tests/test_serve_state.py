"""Tests for serve_state.

Focused on the new controller_ip column + atomic update introduced for HA
leader-aware routing.
"""
# Pytest fixture name collides with pylint's "private name" rule (leading
# underscore is the standard convention for fixtures injected for side
# effects). Disable for the file.
# pylint: disable=invalid-name,protected-access
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm

from sky.serve import constants as serve_constants
from sky.serve import serve_state


def _read_row(engine, name):
    """Read raw services row directly (bypassing get_service_from_name which
    does an INNER JOIN with version_specs and would skip rows without a
    version registered)."""
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(serve_state.services_table).where(
                serve_state.services_table.c.name == name)).fetchone()
    return None if result is None else dict(result._mapping)  # pylint: disable=protected-access


@pytest.fixture
def _mock_serve_db(tmp_path, monkeypatch):
    """Point serve_state at a fresh sqlite DB for the duration of one test."""
    db_path = tmp_path / 'serve_state_testing.db'
    engine = create_engine(f'sqlite:///{db_path}')

    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    # `metadata.create_all` only creates tables that don't already exist; this
    # is enough since we're starting from a brand-new DB and the alembic
    # upgrade step in `create_table` would otherwise need a full env.
    serve_state.Base.metadata.create_all(engine)
    yield engine


def _add_minimal_service(name: str,
                         controller_ip=None,
                         controller_pid=12345,
                         service_hash=None,
                         lifecycle_epoch=None,
                         resource_scope=None):
    """Add a service row with all-required-args defaults so individual tests
    only need to specify what they care about."""
    return serve_state.add_service(
        name=name,
        controller_job_id=1,
        policy='policy',
        requested_resources_str='1x[CPU:1+]',
        load_balancing_policy='round_robin',
        status=serve_state.ServiceStatus.CONTROLLER_INIT,
        tls_encrypted=False,
        pool=False,
        controller_pid=controller_pid,
        entrypoint='entry',
        # A None spec is stored as pickled None (like `add_version` does), so
        # the read path (`_get_service_from_row`) skips the spec-dependent
        # fields instead of calling SkyServiceSpec methods on it.
        spec=None,
        yaml_content='yaml: v1',
        controller_ip=controller_ip,
        service_hash=service_hash,
        lifecycle_epoch=lifecycle_epoch,
        resource_scope=resource_scope,
    )


def _insert_orphan_service_row(engine, name: str, pool: bool = False):
    """Insert a `services` row with no `version_specs` row.

    Simulates the debris stranded by the pre-atomic registration path (or an
    interrupted teardown) on an older controller; `add_service` can no longer
    produce this state."""
    with orm.Session(engine) as session:
        session.execute(serve_state.services_table.insert().values(
            name=name,
            controller_job_id=1,
            status=serve_state.ServiceStatus.CONTROLLER_INIT.value,
            requested_resources_str='1x[CPU:1+]',
            pool=int(pool),
            controller_pid=12345,
            hash='orphan',
            entrypoint='entry'))
        session.commit()


class TestAddServiceWritesControllerIp:
    """`add_service` should persist the new controller_ip column when caller
    provides POD_IP. Older callers that don't pass it must still work
    (column defaults to NULL)."""

    def test_with_controller_ip(self, _mock_serve_db):
        success = _add_minimal_service('svc1', controller_ip='10.0.0.7')
        assert success is True
        record = _read_row(_mock_serve_db, 'svc1')
        assert record is not None
        assert record['controller_ip'] == '10.0.0.7'

    def test_without_controller_ip(self, _mock_serve_db):
        # No controller_ip arg → column stays NULL (used by single-pod /
        # non-K8s deployments where the routing layer falls back to localhost).
        success = _add_minimal_service('svc2')
        assert success is True
        record = _read_row(_mock_serve_db, 'svc2')
        assert record is not None
        assert record['controller_ip'] is None

    def test_returns_false_on_duplicate(self, _mock_serve_db):
        assert _add_minimal_service('svc3', controller_ip='10.0.0.7') is True
        # Adding the same service again must not corrupt — uniqueness violation
        # is converted to False so up() can short-circuit.
        assert _add_minimal_service('svc3', controller_ip='10.0.0.8') is False
        # And the row is unchanged.
        record = _read_row(_mock_serve_db, 'svc3')
        assert record['controller_ip'] == '10.0.0.7'

    def test_persists_caller_generated_incarnation(self, _mock_serve_db):
        assert _add_minimal_service(
            'svc-known-hash', service_hash='caller-generated-hash') is True
        assert _read_row(_mock_serve_db,
                         'svc-known-hash')['hash'] == 'caller-generated-hash'


class TestAddServiceAtomicRegistration:
    """`add_service` must write the `services` row and its initial
    `version_specs` row atomically. The two-write path (add_service then
    add_or_update_version) had a crash window that stranded a `services` row
    with no version row -- invisible to the latest-version INNER JOIN, so it
    could never be recovered, removed, or have its name reused."""

    def test_registration_is_visible_via_join(self, _mock_serve_db):
        # The whole point: after the atomic write, the service is reachable
        # through get_service_from_name (which INNER-JOINs version_specs),
        # with its initial version row in place.
        assert _add_minimal_service('svc-atomic') is True
        assert serve_state.get_service_from_name('svc-atomic') is not None
        assert (serve_state.get_latest_version('svc-atomic') ==
                serve_constants.INITIAL_VERSION)
        assert serve_state.get_yaml_content(
            'svc-atomic', serve_constants.INITIAL_VERSION) == 'yaml: v1'

    def test_duplicate_does_not_write_second_version_row(self, _mock_serve_db):
        assert _add_minimal_service('svc-dup') is True
        # A duplicate name returns False so up() can short-circuit, and must
        # not write a second version row.
        assert _add_minimal_service('svc-dup') is False
        with orm.Session(_mock_serve_db) as session:
            versions = session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.version).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-dup')).fetchall()
        assert len(versions) == 1

    def test_overwrites_stale_version_row(self, _mock_serve_db):
        # A stale initial version row with no services row (left behind by an
        # interrupted teardown on an older controller) must not block
        # re-registration of the name: the initial version write is an upsert,
        # matching the old add_or_update_version semantics.
        serve_state.add_or_update_version('svc-stale',
                                          serve_constants.INITIAL_VERSION, None,
                                          'yaml: stale')
        assert _read_row(_mock_serve_db, 'svc-stale') is None  # no svc row

        assert _add_minimal_service('svc-stale') is True
        assert serve_state.get_service_from_name('svc-stale') is not None
        assert serve_state.get_yaml_content(
            'svc-stale', serve_constants.INITIAL_VERSION) == 'yaml: v1'

    def test_get_service_pool_from_db_sees_orphan_row(self, _mock_serve_db):
        # The raw-pool accessor must read a version-less row (the orphan case)
        # that get_service_from_name's inner join hides -- this is what gates
        # the mode-scoped `down --purge` cleanup of such an orphan.
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')
        assert serve_state.get_service_from_name('svc-orphan') is None
        assert serve_state.get_service_pool_from_db('svc-orphan') is False
        assert serve_state.get_service_pool_from_db('never-existed') is None


class TestServiceLifecycleEpoch:
    """A durable name epoch fences work after advisory-session loss."""

    def test_epoch_survives_delete_and_recreate(self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_a == 1
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')

        epoch_teardown = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_teardown == 2
        assert not serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_a)
        assert serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_teardown)

        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_b == 3
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-b',
                                    lifecycle_epoch=epoch_b,
                                    resource_scope='incarnation-b')
        row = _read_row(_mock_serve_db, 'svc')
        assert row['hash'] == 'incarnation-b'
        assert row['lifecycle_epoch'] == epoch_b
        assert row['resource_scope'] == 'incarnation-b'

    def test_stale_child_row_mutations_cannot_touch_successor(
            self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            'replica-a',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_a)

        epoch_delete = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_delete)
        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-b',
                                    lifecycle_epoch=epoch_b,
                                    resource_scope='incarnation-b')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            'replica-b',
            expected_service_hash='incarnation-b',
            expected_lifecycle_epoch=epoch_b)

        assert not serve_state.remove_replica(
            'svc',
            1,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_delete)
        assert serve_state.get_replica_info_from_id('svc', 1) == 'replica-b'

    def test_exact_owner_cleanup_is_idempotent_when_children_are_absent(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a')

        assert serve_state.remove_replica('svc',
                                          99,
                                          expected_service_hash='incarnation-a',
                                          expected_lifecycle_epoch=epoch)
        assert serve_state.delete_version('svc',
                                          99,
                                          expected_service_hash='incarnation-a')

    def test_stale_version_delete_cannot_touch_successor(self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')

        epoch_delete = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_delete)
        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-b',
                                    lifecycle_epoch=epoch_b,
                                    resource_scope='incarnation-b')

        assert not serve_state.delete_version(
            'svc',
            serve_constants.INITIAL_VERSION,
            expected_service_hash='incarnation-a')
        assert serve_state.get_yaml_content(
            'svc', serve_constants.INITIAL_VERSION) == 'yaml: v1'

    def test_stale_recovery_script_and_version_claims_fail(
            self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')
        assert serve_state.set_ha_recovery_script('svc', 'script-a', epoch_a)

        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert not serve_state.set_ha_recovery_script('svc', 'stale-script',
                                                      epoch_a)
        assert serve_state.get_ha_recovery_script('svc') == 'script-a'
        with pytest.raises(RuntimeError, match='lifecycle ownership'):
            serve_state.add_version('svc',
                                    expected_service_hash='incarnation-a',
                                    expected_lifecycle_epoch=epoch_a)
        assert serve_state.add_version('svc',
                                       expected_service_hash='incarnation-a',
                                       expected_lifecycle_epoch=epoch_b) == 2

        assert not serve_state.add_or_update_version(
            'svc',
            2,
            None,
            'stale: yaml',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_a)
        assert serve_state.get_yaml_content('svc', 2) is None

    def test_stale_controller_cannot_write_current_incarnation_children(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a')
        stale_owner = (100, '10.0.0.1')
        assert not serve_state.add_or_update_replica(
            'svc',
            1,
            'stale-single',
            expected_service_hash='incarnation-a',
            expected_controller_owner=stale_owner)
        assert not serve_state.add_or_update_replicas(
            'svc', [(1, 'stale-batch')],
            expected_service_hash='incarnation-a',
            expected_controller_owner=stale_owner)
        assert serve_state.get_replica_info_from_id('svc', 1) is None


class TestGetServiceFromNameReturnsControllerIp:

    def _add_with_version(self, service_name, controller_ip):
        # Reading via get_service_from_name requires a version_specs row
        # (it's an INNER JOIN); `add_service` writes the initial version row
        # in the same transaction, so this is enough for the JOIN to fire.
        _add_minimal_service(service_name, controller_ip=controller_ip)

    def test_round_trips_controller_ip(self, _mock_serve_db):
        self._add_with_version('svc-rt', controller_ip='10.4.10.8')
        record = serve_state.get_service_from_name('svc-rt')
        assert record is not None
        # The whole point: the dict KEY must exist with the persisted value.
        assert 'controller_ip' in record, (
            'controller_ip key missing from get_service_from_name() record — '
            'callers will silently fall back to localhost routing')
        assert record['controller_ip'] == '10.4.10.8'

    def test_round_trips_none_controller_ip(self, _mock_serve_db):
        # Even when the row was written without controller_ip (single-pod
        # mode), the dict must contain the key with value None — otherwise
        # `record.get('controller_ip')` and `record['controller_ip']` give
        # inconsistent answers.
        self._add_with_version('svc-rt-none', controller_ip=None)
        record = serve_state.get_service_from_name('svc-rt-none')
        assert record is not None
        assert 'controller_ip' in record
        assert record['controller_ip'] is None

    def test_record_includes_all_persisted_columns(self, _mock_serve_db):
        """Belt and braces: snapshot the keys we expect from the read path.
        If a future PR adds a column to services_table but forgets to update
        `_get_service_from_row`, this test fails loudly."""
        self._add_with_version('svc-rt-keys', controller_ip='10.0.0.1')
        record = serve_state.get_service_from_name('svc-rt-keys')
        assert record is not None
        for key in (
                'name',
                'status',
                'controller_pid',
                'controller_ip',
                'controller_port',
                'pool',
        ):
            assert key in record, f'missing key: {key}'


class TestGetServiceControllerOwner:
    """The proxy hot path reads one narrow services-table record."""

    def test_returns_only_routing_identity_without_loading_spec(
            self, _mock_serve_db, monkeypatch):
        _add_minimal_service('svc-owner', controller_ip='10.4.10.8')
        serve_state.set_service_controller_port('svc-owner', 20007)

        def fail_if_spec_loaded(*args, **kwargs):
            del args, kwargs
            raise AssertionError('owner lookup must not deserialize a spec')

        monkeypatch.setattr(serve_state, 'get_spec', fail_if_spec_loaded)
        record = serve_state.get_service_controller_owner('svc-owner')

        assert record is not None
        assert set(record) == {
            'hash',
            'status',
            'controller_pid',
            'controller_ip',
            'controller_port',
            'lifecycle_epoch',
            'resource_scope',
        }
        assert record['hash']
        assert record['status'] == serve_state.ServiceStatus.CONTROLLER_INIT
        assert record['controller_pid'] == 12345
        assert record['controller_ip'] == '10.4.10.8'
        assert record['controller_port'] == 20007

    def test_missing_row_returns_none(self, _mock_serve_db):
        assert serve_state.get_service_controller_owner('missing') is None


class TestUpdateServiceControllerPidIpAndPort:
    """The atomic update is the core of the HA-recovery DB flip — it must
    write pid, ip, AND port in a single transaction so clients never
    observe a half-flipped row (e.g. new pid + old ip + stale port that
    points at a different service's listener on the new pod).

    Recovery picks port locally (find_free_port on the recovery pod);
    that new port has to land in DB together with pid/ip, otherwise
    a `_get_controller_url` consumer reading DB between writes could
    route to the new pod with the old port and hit the wrong listener.
    """

    def test_updates_all_three_fields(self, _mock_serve_db):
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        serve_state.set_service_controller_port('svc', 20001)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True

        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] == '10.0.0.8'
        assert record['controller_port'] == 20007

    def test_can_clear_controller_ip(self, _mock_serve_db):
        # If we ever need to write None (e.g. controller is on a non-K8s pod
        # in a hybrid deploy), the column must accept NULL. Port is still
        # required (it's an int column, no NULL).
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip=None,
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] is None
        assert record['controller_port'] == 20007

    def test_no_op_when_service_missing(self, _mock_serve_db):
        # Should not raise if the row was deleted between read and write
        # (e.g. a `down` raced our recovery).
        assert serve_state.update_service_controller_pid_ip_and_port(
            'never-existed',
            controller_pid=1,
            controller_ip='10.0.0.7',
            controller_port=20001,
            expected_service_hash='missing-incarnation',
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is False
        assert _read_row(_mock_serve_db, 'never-existed') is None

    def test_does_not_touch_other_fields(self, _mock_serve_db):
        # The atomic update must only touch the three specified columns —
        # don't want to clobber e.g. status, load_balancer_port from a
        # concurrent writer.
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        serve_state.set_service_controller_port('svc', 20001)
        serve_state.set_service_load_balancer_port('svc', 30000)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True

        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] == '10.0.0.8'
        assert record['controller_port'] == 20007
        assert record['load_balancer_port'] == 30000  # untouched

    def test_rejects_purge_and_same_name_successor_with_same_pid(
            self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_ip='10.0.0.7',
                             controller_pid=777)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', old_hash, 777, '10.0.0.7', 888, '10.0.0.8') is True

        assert serve_state.remove_service_completely('svc', old_hash)
        _add_minimal_service('svc',
                             controller_ip='10.9.0.1',
                             controller_pid=888)
        successor = _read_row(_mock_serve_db, 'svc')
        assert successor['hash'] != old_hash

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=888,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=old_hash,
            expected_controller_pid=888,
            expected_controller_ip='10.0.0.8') is False
        record = _read_row(_mock_serve_db, 'svc')
        assert record['hash'] == successor['hash']
        assert record['controller_ip'] == '10.9.0.1'
        assert record['controller_port'] is None

    def test_publish_requires_preclaimed_ip_when_pid_collides(
            self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_ip='10.0.0.1',
                             controller_pid=777,
                             service_hash='same-incarnation')
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 777, '10.0.0.1', 777, '10.0.0.2') is True

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=777,
            controller_ip='10.0.0.1',
            controller_port=20001,
            expected_service_hash='same-incarnation',
            expected_controller_pid=777,
            expected_controller_ip='10.0.0.1') is False
        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=777,
            controller_ip='10.0.0.2',
            controller_port=20002,
            expected_service_hash='same-incarnation',
            expected_controller_pid=777,
            expected_controller_ip='10.0.0.2') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_ip'] == '10.0.0.2'
        assert record['controller_port'] == 20002


class TestUpdateServiceControllerPidIfOwner:

    def test_preclaim_requires_original_hash_and_pid(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=111)
        serve_state.set_service_controller_port('svc', 20001)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', service_hash, 111, None, 222, '10.0.0.2') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 222
        assert record['controller_ip'] == '10.0.0.2'
        assert record['controller_port'] is None
        # A second recovery that read owner 111 before the first claim loses.
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', service_hash, 111, None, 333, '10.0.0.3') is False
        assert _read_row(_mock_serve_db, 'svc')['controller_pid'] == 222

    def test_preclaim_rejects_same_name_successor(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=111)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', old_hash)
        # Deliberately reuse the same PID to model distinct Kubernetes pods.
        _add_minimal_service('svc', controller_pid=111)
        successor_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_if_owner(
            'svc', old_hash, 111, None, 222, '10.0.0.2') is False
        record = _read_row(_mock_serve_db, 'svc')
        assert record['hash'] == successor_hash
        assert record['controller_pid'] == 111

    def test_ip_fences_equal_pids_within_same_incarnation(self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_pid=111,
                             controller_ip='10.0.0.1',
                             service_hash='same-incarnation')
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 111, '10.0.0.1', 111, '10.0.0.2') is True
        # A stale parent on the old pod can have the same namespace-local PID,
        # but its old IP is no longer authoritative.
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 111, '10.0.0.1', 111,
            '10.0.0.3') is False
        assert _read_row(_mock_serve_db, 'svc')['controller_ip'] == '10.0.0.2'


class TestSetServiceControllerPortIfOwner:
    """Compare-and-swap port write for the in-place controller respawn: the
    UPDATE must be filtered on hash/PID/IP so a parent whose row was taken over
    by HA recovery cannot clobber the new owner's port."""

    def test_owner_updates_port(self, _mock_serve_db):
        _add_minimal_service('svc')  # seeds controller_pid=12345
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_controller_port_if_owner(
            'svc', service_hash, 12345, None, 20123) is True
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] == 20123

    def test_non_owner_is_rejected(self, _mock_serve_db):
        _add_minimal_service('svc')
        serve_state.set_service_controller_port('svc', 20123)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_controller_port_if_owner(
            'svc', service_hash, 99999, None, 20999) is False
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] == 20123

    def test_missing_service_returns_false(self, _mock_serve_db):
        assert serve_state.set_service_controller_port_if_owner(
            'never-existed', 'missing-hash', 12345, None, 20123) is False

    def test_same_pid_successor_is_rejected_by_hash(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=12345)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', old_hash)
        _add_minimal_service('svc', controller_pid=12345)
        assert serve_state.set_service_controller_port_if_owner(
            'svc', old_hash, 12345, None, 20123) is False
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] is None


class TestAcknowledgeControllerTeardown:

    def test_owner_atomically_publishes_terminal_status_and_ack(
            self, _mock_serve_db):
        _add_minimal_service('svc', service_hash='incarnation-a')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY)

        assert serve_state.acknowledge_service_controller_teardown_if_owner(
            'svc', 'incarnation-a', 12345, None)

        row = _read_row(_mock_serve_db, 'svc')
        assert row['status'] == serve_state.ServiceStatus.SHUTTING_DOWN.value
        assert (row['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)

    def test_stale_owner_cannot_change_successor_status(self, _mock_serve_db):
        _add_minimal_service('svc', service_hash='incarnation-b')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY)

        assert not serve_state.acknowledge_service_controller_teardown_if_owner(
            'svc', 'incarnation-a', 12345, None)
        assert (_read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.READY.value)


class TestSetServiceLoadBalancerPortIfOwner:
    """CAS port write for recovery's external-LB republish: the UPDATE is
    filtered on hash/PID/IP so a stale recovery cannot write to a
    same-name successor's row."""

    def test_owner_updates_port(self, _mock_serve_db):
        _add_minimal_service('svc')  # seeds controller_pid=12345
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 12345, None, 30001) is True
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30001

    def test_owner_updates_null_port(self, _mock_serve_db):
        # NULL -> port must work for the owner: recovery of an up() that
        # crashed before registration is the case this setter exists for.
        _add_minimal_service('svc')
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] is None
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 12345, None, 30001) is True
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30001

    def test_non_owner_is_rejected(self, _mock_serve_db):
        _add_minimal_service('svc')
        serve_state.set_service_load_balancer_port('svc', 30002)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 99999, None, 30001) is False
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30002

    def test_missing_service_returns_false(self, _mock_serve_db):
        assert serve_state.set_service_load_balancer_port_if_owner(
            'never-existed', 'missing-hash', 12345, None, 30001) is False

    def test_same_pid_successor_is_rejected_by_hash(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=12345)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', old_hash)
        _add_minimal_service('svc', controller_pid=12345)
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', old_hash, 12345, None, 30001) is False
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] is None


class TestSetServiceControllerIp:
    """Standalone IP setter is rarely called (the atomic version is preferred
    for recovery), but kept for symmetry with set_service_controller_port."""

    def test_basic(self, _mock_serve_db):
        _add_minimal_service('svc', controller_ip=None)
        serve_state.set_service_controller_ip('svc', '10.0.0.9')
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_ip'] == '10.0.0.9'
        # pid unchanged
        assert record['controller_pid'] == 12345


class TestRemoveServiceCompletely:
    """`remove_service_completely` deletes services / version_specs /
    serve_ha_recovery_script in one transaction. Sequential deletes had
    a real-world failure mode where the last call
    was the one most likely to be skipped
    when the subprocess died mid-cleanup, leaving the row orphaned across
    many test runs while the other tables stayed clean. This guarantees
    all-or-nothing teardown for service metadata.
    """

    def _populate(self, engine, name):
        # Seed all service tables plus a replica row so the exact incarnation
        # can be removed atomically.
        _add_minimal_service(name, controller_ip='10.0.0.1')
        serve_state.add_version(name)
        serve_state.set_ha_recovery_script(name, 'dummy script')
        # replicas: a minimal pickled ReplicaInfo proxy is hard to
        # construct here, so just write a row directly to the table.
        with orm.Session(engine) as session:
            session.execute(serve_state.replicas_table.insert().values(
                service_name=name, replica_id=1, replica_info=b'fake-pickle'))
            session.commit()

    def test_failed_cleanup_status_retains_metadata_and_reserves_name(
            self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc-retained')
        service_hash = _read_row(_mock_serve_db, 'svc-retained')['hash']

        assert serve_state.set_service_status_and_active_versions_if_owner(
            'svc-retained',
            service_hash,
            12345,
            '10.0.0.1',
            serve_state.ServiceStatus.FAILED_CLEANUP,
            expected_status=serve_state.ServiceStatus.CONTROLLER_INIT)

        with orm.Session(_mock_serve_db) as session:
            for table, column in [
                (serve_state.services_table, serve_state.services_table.c.name),
                (serve_state.replicas_table,
                 serve_state.replicas_table.c.service_name),
                (serve_state.version_specs_table,
                 serve_state.version_specs_table.c.service_name),
                (serve_state.serve_ha_recovery_script_table,
                 serve_state.serve_ha_recovery_script_table.c.service_name),
            ]:
                assert session.execute(
                    sqlalchemy.select(column).where(
                        column == 'svc-retained')).first() is not None
        # The durable services row keeps same-name H_new from registering.
        assert not _add_minimal_service('svc-retained')

    def test_removes_service_metadata_tables(self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc-rsc')
        # Sanity: rows are present.
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.serve_ha_recovery_script_table.c.service_name).
                where(serve_state.serve_ha_recovery_script_table.c.service_name
                      == 'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.service_name).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-rsc')).first() is not None

        service_hash = _read_row(_mock_serve_db, 'svc-rsc')['hash']
        assert serve_state.remove_service_completely('svc-rsc', service_hash)

        # The three metadata tables must be gone.
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc-rsc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.serve_ha_recovery_script_table.c.service_name).
                where(serve_state.serve_ha_recovery_script_table.c.service_name
                      == 'svc-rsc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.service_name).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-rsc')).first() is None
            # Replica rows are children of the incarnation and must be gone in
            # the same transaction, or a stale purge can leave rows that a
            # same-name successor reads as its own.
            assert session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc-rsc')).first() is None

    def test_does_not_touch_other_services(self, _mock_serve_db):
        """Make sure deletion is scoped to the named service."""
        self._populate(_mock_serve_db, 'svc-keep')
        self._populate(_mock_serve_db, 'svc-drop')

        drop_hash = _read_row(_mock_serve_db, 'svc-drop')['hash']
        assert serve_state.remove_service_completely('svc-drop', drop_hash)

        # svc-keep's rows must all survive.
        with orm.Session(_mock_serve_db) as session:
            for tbl_name, tbl, col_name in [
                ('services', serve_state.services_table, 'name'),
                ('version_specs', serve_state.version_specs_table,
                 'service_name'),
                ('serve_ha_recovery_script',
                 serve_state.serve_ha_recovery_script_table, 'service_name'),
                ('replicas', serve_state.replicas_table, 'service_name'),
            ]:
                col = getattr(tbl.c, col_name)
                assert session.execute(
                    sqlalchemy.select(col).where(
                        col == 'svc-keep')).first() is not None, (
                            f'svc-keep was wrongly removed from {tbl_name}')

    def test_no_op_when_nothing_to_delete(self, _mock_serve_db):
        # Should be a silent no-op when no rows exist.
        assert not serve_state.remove_service_completely(
            'never-existed', 'missing-hash')

    def test_stale_a_delete_spares_successor_b_and_all_children(
            self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc')
        hash_a = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', hash_a)

        self._populate(_mock_serve_db, 'svc')
        hash_b = _read_row(_mock_serve_db, 'svc')['hash']
        assert hash_b != hash_a

        assert not serve_state.remove_service_completely('svc', hash_a)
        assert _read_row(_mock_serve_db, 'svc')['hash'] == hash_b
        with orm.Session(_mock_serve_db) as session:
            for table, column in [
                (serve_state.replicas_table,
                 serve_state.replicas_table.c.service_name),
                (serve_state.version_specs_table,
                 serve_state.version_specs_table.c.service_name),
                (serve_state.serve_ha_recovery_script_table,
                 serve_state.serve_ha_recovery_script_table.c.service_name),
            ]:
                assert session.execute(
                    sqlalchemy.select(column).where(
                        column == 'svc')).first() is not None, table.name


class TestRecoveryVersionSelection:
    """An interrupted `sky serve update` leaves a NULL-yaml placeholder
    version (written by `add_version`) as MAX(version). Recovery must resume
    the latest version whose yaml was actually committed, otherwise the
    controller crash-loops asserting on the NULL yaml. These tests pin that
    `get_latest_committed_version` skips the placeholder."""

    def test_committed_version_skips_placeholder(self, _mock_serve_db):
        # Two committed versions, then an interrupted update creates a
        # NULL-yaml placeholder v3 as the new MAX.
        serve_state.add_or_update_version('svc', 1, 'spec-1', 'yaml: v1')
        serve_state.add_or_update_version('svc', 2, 'spec-2', 'yaml: v2')
        assert serve_state.add_version('svc') == 3  # placeholder
        # Raw MAX points at the placeholder (the bug)...
        assert serve_state.get_latest_version('svc') == 3
        # ...but recovery must pick the newest committed version, not the
        # placeholder and not the original.
        assert serve_state.get_latest_committed_version('svc') == 2

    def test_committed_version_none_when_only_placeholder(self, _mock_serve_db):
        serve_state.add_version('svc')  # placeholder v1, no committed yaml
        assert serve_state.get_latest_committed_version('svc') is None


class TestUnrecoverableServiceCleanup:
    """Rows without committed YAML must not wait on impossible recovery."""

    def test_ha_retirement_marks_failed_and_removes_script(
            self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        serve_state.set_ha_recovery_script('svc', 'unbootable script')

        assert serve_state.mark_unrecoverable_service_for_cleanup('svc',
                                                                  'orphan',
                                                                  pool=False)
        assert (_read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.FAILED_CLEANUP.value)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_purge_claim_consumes_unbootable_script(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'orphan', serve_state.ServiceStatus.SHUTTING_DOWN)
        serve_state.set_ha_recovery_script('svc', 'unbootable script')

        assert serve_state.claim_unrecoverable_service_teardown(
            'svc', 'orphan', 12345, None, 67890, '10.0.0.2')
        row = _read_row(_mock_serve_db, 'svc')
        assert row['controller_pid'] == 67890
        assert row['controller_ip'] == '10.0.0.2'
        assert (row['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_committed_version_prevents_unrecoverable_claim(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=12345,
                                    service_hash='incarnation-a')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'incarnation-a', serve_state.ServiceStatus.SHUTTING_DOWN)
        serve_state.set_ha_recovery_script('svc', 'bootable script')

        assert not serve_state.claim_unrecoverable_service_teardown(
            'svc', 'incarnation-a', 12345, None, 67890, '10.0.0.2')
        assert serve_state.get_ha_recovery_script('svc') == 'bootable script'


class TestTerminalServiceRejectsVersionWrites:

    def test_down_winner_blocks_placeholder_and_yaml_commit(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=12345,
                                    service_hash='incarnation-a')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'incarnation-a', serve_state.ServiceStatus.SHUTTING_DOWN)

        with pytest.raises(RuntimeError, match='terminal status'):
            serve_state.add_version('svc')
        assert not serve_state.add_or_update_version('svc', 2, 'spec-2',
                                                     'yaml: v2')
        assert serve_state.get_latest_version('svc') == 1
        assert serve_state.get_yaml_content('svc', 1) == 'yaml: v1'


class TestBatchReplicaUpsert:
    """add_or_update_replicas: one statement for a probe round's writes."""

    def test_batch_insert_then_batch_update(self, _mock_serve_db):
        import types

        infos = [(i, types.SimpleNamespace(replica_id=i, tag='v1'))
                 for i in range(1, 4)]
        serve_state.add_or_update_replicas('svc', infos)
        rows = {
            i: serve_state.get_replica_info_from_id('svc', i)
            for i in range(1, 4)
        }
        assert all(rows[i].tag == 'v1' for i in range(1, 4))

        # Conflict path: same keys must update in place, not duplicate.
        updated = [(i, types.SimpleNamespace(replica_id=i, tag='v2'))
                   for i in range(1, 4)]
        serve_state.add_or_update_replicas('svc', updated)
        assert all(
            serve_state.get_replica_info_from_id('svc', i).tag == 'v2'
            for i in range(1, 4))
        assert len(serve_state.get_replica_infos('svc')) == 3

    def test_empty_batch_is_noop(self, _mock_serve_db):
        serve_state.add_or_update_replicas('svc', [])
        assert not serve_state.get_replica_infos('svc')

    def test_batch_larger_than_chunk_size(self, _mock_serve_db):
        import types

        n = serve_state._REPLICA_UPSERT_CHUNK_SIZE * 2 + 17
        infos = [
            (i, types.SimpleNamespace(replica_id=i)) for i in range(1, n + 1)
        ]
        serve_state.add_or_update_replicas('svc', infos)
        assert len(serve_state.get_replica_infos('svc')) == n
