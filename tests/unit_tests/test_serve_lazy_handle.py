"""Tests for the lazy-handle changes in serve/pool status responses.

Coverage:
1. ``ReplicaInfo.to_info_dict`` always populates placement fields from the
   selected location or launched handle, populates resource strings when a
   handle is reachable, and only attaches the handle when
   ``with_handle=True``.
2. ``_decode_serve_status`` tolerates both an absent ``handle`` key and an
   explicit ``handle=None`` (the new wire shape from new servers).
3. ``serialize_serve_status`` strips the handle to ``None`` only for
   clients on API_VERSION >= MIN_LAZY_REPLICA_HANDLE_API_VERSION, and
   leaves the wire payload untouched for older clients.
4. ``_format_replica_table`` prefers pre-computed fields and falls back to
   computing from the handle when those are absent (old server).
"""
# pylint: disable=protected-access
import base64
import contextvars
import pickle
import threading
import time
from typing import List, Optional
from unittest import mock

import orjson
import pytest

from sky import clouds
from sky import exceptions
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.server import constants as server_constants
from sky.server.requests.serializers import decoders
from sky.server.requests.serializers import return_value_serializers


def _make_handle(cloud_repr='aws',
                 region='us-east-1',
                 infra_str='aws (us-east-1)',
                 simple='1x A100:1',
                 full='1x A100:1 (16 vCPUs, 64 GB)'):
    """Build a mock handle that mimics ``CloudVmRayResourceHandle``."""
    handle = mock.MagicMock()
    handle.launched_resources.cloud.__repr__ = lambda self: cloud_repr
    handle.launched_resources.region = region
    handle.launched_resources.infra.formatted_str.return_value = infra_str
    return handle, simple, full


def _make_replica_info(replica_id=1, cluster_name='r-1'):
    """Build a real ``ReplicaInfo`` with deterministic fields."""
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=cluster_name,
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    # Force a known status so we don't depend on status-machine internals.
    info.status_property.to_replica_status = lambda: (serve_state.ReplicaStatus.
                                                      READY)
    return info


class TestToInfoDictPreComputedFields:
    """ReplicaInfo serialization should precompute lightweight fields."""

    def test_populates_strings_when_handle_present(self):
        """Even with ``with_handle=False`` the small string fields are set
        so new clients never need to touch the handle blob."""
        info = _make_replica_info()
        # The launch result is authoritative if it differs from the placer's
        # earlier selection (for example, after optimizer failover).
        info.location = spot_placer.Location(
            cloud=clouds.GCP(),
            region='us-central1',
            zone=None,
        ).to_pickleable()
        handle, simple, full = _make_handle()
        handle.launched_nodes = 2
        handle.launched_resources.get_cost.return_value = 3.25
        with mock.patch.object(info, 'handle',
                               return_value=handle) as handle_call, \
             mock.patch.object(info,
                               'get_spot_location') as location_call:
            with mock.patch(
                    'sky.serve.replica_managers.resources_utils.'
                    'get_readable_resources_repr',
                    return_value=(simple, full)):
                result = info.to_info_dict(with_handle=False,
                                           with_url=False,
                                           cluster_record={'launched_at': 42})

        handle_call.assert_called_once_with({'launched_at': 42})
        location_call.assert_not_called()
        assert result['launched_at'] == 42
        assert result['cloud'] == 'aws'
        assert result['region'] == 'us-east-1'
        assert result['infra'] == 'aws (us-east-1)'
        assert result['resources_str'] == simple
        assert result['resources_str_full'] == full
        assert result['hourly_cost'] == 6.5
        assert result['hourly_cost_exclusion_reason'] is None
        # with_handle=False MUST NOT add the bulky handle to the dict.
        assert 'handle' not in result

    def test_with_handle_true_attaches_handle(self):
        info = _make_replica_info()
        handle, simple, full = _make_handle()
        with mock.patch.object(info, 'handle', return_value=handle):
            with mock.patch(
                    'sky.serve.replica_managers.resources_utils.'
                    'get_readable_resources_repr',
                    return_value=(simple, full)):
                result = info.to_info_dict(with_handle=True,
                                           with_url=False,
                                           cluster_record={'launched_at': 7})
        assert result['handle'] is handle
        # Pre-computed strings populated as well — the new clients still
        # benefit even when handle is present (e.g. mixed deployments).
        assert result['infra'] == 'aws (us-east-1)'
        assert result['resources_str'] == simple
        assert result['resources_str_full'] == full

    def test_exposes_first_ready_time_and_end_to_end_latency(self):
        info = _make_replica_info()
        info.created_at = 100.0
        info.status_property.first_ready_time = 166.5
        # Exercise the persisted replica JSON contract, not just the live
        # in-memory object.
        info = replica_managers.ReplicaInfo.from_storage_dict(
            info.to_storage_dict())

        result = info.to_info_dict(with_handle=False,
                                   with_url=False,
                                   cluster_record=None)

        assert result['ready_at'] == 166.5
        assert result['time_to_ready_seconds'] == 66.5

    @pytest.mark.parametrize('first_ready_time', [None, -1.0])
    def test_does_not_expose_missing_or_failed_readiness(
            self, first_ready_time):
        info = _make_replica_info()
        info.created_at = 100.0
        info.status_property.first_ready_time = first_ready_time

        result = info.to_info_dict(with_handle=False,
                                   with_url=False,
                                   cluster_record=None)

        assert result['ready_at'] is None
        assert result['time_to_ready_seconds'] is None

    def test_resources_str_full_falls_back_to_simple_when_none(self):
        """``get_readable_resources_repr`` returns ``(simple, None)`` if no
        full form is meaningful. Implementation must coerce to ``simple``."""
        info = _make_replica_info()
        handle, simple, _ = _make_handle()
        with mock.patch.object(info, 'handle', return_value=handle):
            with mock.patch(
                    'sky.serve.replica_managers.resources_utils.'
                    'get_readable_resources_repr',
                    return_value=(simple, None)):
                result = info.to_info_dict(with_handle=False,
                                           with_url=False,
                                           cluster_record={'launched_at': 0})
        assert result['resources_str'] == simple
        assert result['resources_str_full'] == simple

    def test_no_extra_fields_when_handle_missing(self):
        """A dead replica has cluster_record=None → handle=None → no extra
        derived fields, and ``launched_at`` is None."""
        info = _make_replica_info()
        # ``self.handle()`` must NOT be called when cluster_record is None
        # (short-circuit in to_info_dict).
        with mock.patch.object(info, 'handle') as handle_call:
            result = info.to_info_dict(with_handle=True,
                                       with_url=False,
                                       cluster_record=None)
        handle_call.assert_not_called()
        assert result['launched_at'] is None
        assert result['handle'] is None
        for key in ('cloud', 'region', 'infra', 'resources_str',
                    'resources_str_full', 'hourly_cost',
                    'hourly_cost_exclusion_reason'):
            assert key not in result

    def test_uses_selected_location_when_handle_missing(self):
        """Pre-launch replicas keep their placer cloud and region visible."""
        location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                        region='research-context',
                                        zone=None,
                                        use_spot=False)
        info = replica_managers.ReplicaInfo(replica_id=1,
                                            cluster_name='r-1',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=location,
                                            version=1,
                                            resources_override=None)
        info.status_property.to_replica_status = lambda: (
            serve_state.ReplicaStatus.PROVISIONING)

        with mock.patch.object(info, 'handle') as handle_call:
            result = info.to_info_dict(with_handle=False,
                                       with_url=False,
                                       cluster_record=None)

        handle_call.assert_not_called()
        assert result['status'] == serve_state.ReplicaStatus.PROVISIONING
        assert result['cloud'] == 'Kubernetes'
        assert result['region'] == 'research-context'
        assert result['infra'] == 'Kubernetes (research-context)'
        assert 'resources_str' not in result


class TestToInfoDictEndpointSnapshot:
    """Replica endpoint serialization should reuse the supplied cluster row."""

    def test_endpoint_uses_supplied_cluster_record_once(self):
        info = _make_replica_info(cluster_name='r-1')
        handle, simple, full = _make_handle()
        cluster_record = {
            'launched_at': 42,
            'handle': handle,
        }
        endpoint_calls = []

        def _get_endpoints(cluster, port, **kwargs):
            endpoint_calls.append((cluster, port, kwargs))
            return {8080: '1.2.3.4:8080'}

        with mock.patch.object(info, 'handle',
                               return_value=handle) as handle_call, \
             mock.patch(
                 'sky.serve.replica_managers.resources_utils.'
                 'get_readable_resources_repr',
                 return_value=(simple, full)), \
             mock.patch('sky.serve.replica_managers.backend_utils.'
                        'get_endpoints',
                        side_effect=_get_endpoints):
            result = info.to_info_dict(with_handle=False,
                                       with_url=True,
                                       cluster_record=cluster_record)

        handle_call.assert_called_once_with(cluster_record)
        assert endpoint_calls == [('r-1', 8080, {
            'cluster_record': cluster_record
        })]
        assert result['endpoint'] == 'http://1.2.3.4:8080'

    def test_endpoint_none_on_cluster_not_up_from_snapshot(self):
        info = _make_replica_info(cluster_name='r-1')
        handle, simple, full = _make_handle()
        cluster_record = {
            'launched_at': 42,
            'handle': handle,
        }

        with mock.patch.object(info, 'handle',
                               return_value=handle), \
             mock.patch(
                 'sky.serve.replica_managers.resources_utils.'
                 'get_readable_resources_repr',
                 return_value=(simple, full)), \
             mock.patch('sky.serve.replica_managers.backend_utils.'
                        'get_endpoints',
                        side_effect=exceptions.ClusterNotUpError(
                            'cluster not ready')):
            result = info.to_info_dict(with_handle=False,
                                       with_url=True,
                                       cluster_record=cluster_record)

        assert result['endpoint'] is None


class TestDecodeServeStatusTolerantHandle:
    """The new wire shape for new clients sends ``handle = None`` — old
    servers send a base64-pickle. The decoder must handle both. We don't
    test absent-key because the encoder always sets the key (either to a
    real pickled handle or to ``None`` after the version-aware
    serializer's strip)."""

    def _record(self, replica):
        return {
            'status': serve_state.ServiceStatus.READY.value,
            'replica_info': [replica],
        }

    def test_handle_none_decodes_to_none(self):
        record = self._record({
            'status': serve_state.ReplicaStatus.READY.value,
            'handle': None,
        })
        decoded = decoders._decode_serve_status([record])
        assert decoded[0]['replica_info'][0]['handle'] is None

    def test_handle_pickle_decodes_to_object(self):
        """Backwards-compat: an old server's base64-pickle handle still
        decodes correctly. We use a dict sentinel because we don't have a
        real ``CloudVmRayResourceHandle`` constructor available in tests."""
        sentinel = {'_marker': 'fake-handle-payload'}
        encoded = base64.b64encode(pickle.dumps(sentinel)).decode('utf-8')
        record = self._record({
            'status': serve_state.ReplicaStatus.READY.value,
            'handle': encoded,
        })
        with mock.patch(
                'sky.server.requests.serializers.decoders.decode_handle',
                return_value=sentinel) as mock_decode:
            decoded = decoders._decode_serve_status([record])
        mock_decode.assert_called_once_with(encoded)
        assert decoded[0]['replica_info'][0]['handle'] is sentinel


class TestSerializeServeStatusVersionAware:
    """The wire-strip happens only for clients new enough to read the
    pre-computed fields. Old clients keep receiving the full pickle."""

    def _payload(self):
        # The real producer pipeline runs the encoder first, so by the time
        # the serializer sees this, handle is a base64 string. We use a
        # marker string and assert pass-through / replace.
        return [{
            'status': serve_state.ServiceStatus.READY.value,
            'replica_info': [{
                'status': serve_state.ReplicaStatus.READY.value,
                'handle': '<base64-pickle>',
                'infra': 'aws (us-east-1)',
                'resources_str': '1x A100:1',
            }],
        }]

    def _decode_wire(self, wire: str):
        return orjson.loads(wire)

    def test_new_client_strips_handle(self):
        with mock.patch(
                'sky.server.requests.serializers.return_value_serializers.'
                'versions.get_remote_api_version',
                return_value=server_constants.
                MIN_LAZY_REPLICA_HANDLE_API_VERSION):
            wire = return_value_serializers.serialize_serve_status(
                self._payload())
        decoded = self._decode_wire(wire)
        assert decoded[0]['replica_info'][0]['handle'] is None
        # Pre-computed fields survive unchanged.
        assert decoded[0]['replica_info'][0]['infra'] == 'aws (us-east-1)'

    def test_old_client_preserves_handle(self):
        old = server_constants.MIN_LAZY_REPLICA_HANDLE_API_VERSION - 1
        with mock.patch(
                'sky.server.requests.serializers.return_value_serializers.'
                'versions.get_remote_api_version',
                return_value=old):
            wire = return_value_serializers.serialize_serve_status(
                self._payload())
        decoded = self._decode_wire(wire)
        assert decoded[0]['replica_info'][0]['handle'] == '<base64-pickle>'

    def test_unknown_client_version_preserves_handle(self):
        """``remote_api_version is None`` means we have no version
        context (e.g. legacy code path). Be safe: keep the handle."""
        with mock.patch(
                'sky.server.requests.serializers.return_value_serializers.'
                'versions.get_remote_api_version',
                return_value=None):
            wire = return_value_serializers.serialize_serve_status(
                self._payload())
        decoded = self._decode_wire(wire)
        assert decoded[0]['replica_info'][0]['handle'] == '<base64-pickle>'

    def test_none_return_value_no_error(self):
        with mock.patch(
                'sky.server.requests.serializers.return_value_serializers.'
                'versions.get_remote_api_version',
                return_value=server_constants.
                MIN_LAZY_REPLICA_HANDLE_API_VERSION):
            wire = return_value_serializers.serialize_serve_status(None)
        assert orjson.loads(wire) is None

    def test_registered_for_both_request_names(self):
        """Both ``serve.status`` and ``jobs.pool_status`` must dispatch to
        the strip-aware serializer."""
        prefix = server_constants.REQUEST_NAME_PREFIX
        serve = return_value_serializers.get_serializer(prefix + 'serve.status')
        pool = return_value_serializers.get_serializer(prefix +
                                                       'jobs.pool_status')
        assert serve is return_value_serializers.serialize_serve_status
        assert pool is return_value_serializers.serialize_serve_status


class TestFormatReplicaTableFallback:
    """The CLI table must prefer pre-computed fields (new server) and fall
    back to local handle computation (old server)."""

    def _base_record(self, **overrides):
        rec = {
            'service_name': 'svc',
            'replica_id': 1,
            'version': 1,
            'endpoint': 'http://1.2.3.4:8080',
            'launched_at': 0,
            'status': serve_state.ReplicaStatus.READY,
            'used_by': None,
            'handle': None,
        }
        rec.update(overrides)
        return rec

    def test_pre_computed_fields_used_when_present(self):
        rec = self._base_record(
            infra='aws (us-east-1)',
            resources_str='1x A100:1',
            resources_str_full='1x A100:1 (16 vCPUs, 64 GB)',
        )
        # Even if a handle is present, pre-computed wins for show_all=False.
        rendered = serve_utils._format_replica_table([rec],
                                                     show_all=False,
                                                     pool=False)
        assert 'aws (us-east-1)' in rendered
        assert '1x A100:1' in rendered
        # show_all=False uses simplified, NOT the full form.
        assert '16 vCPUs' not in rendered

    def test_show_all_prefers_full_resources_str(self):
        rec = self._base_record(
            infra='aws (us-east-1)',
            resources_str='1x A100:1',
            resources_str_full='1x A100:1 (16 vCPUs, 64 GB)',
        )
        rendered = serve_utils._format_replica_table([rec],
                                                     show_all=True,
                                                     pool=False)
        assert '16 vCPUs' in rendered

    def test_fallback_to_handle_when_pre_computed_absent(self):
        """Old server case: no ``infra``/``resources_str`` keys but a real
        handle present. Implementation must derive both locally."""
        handle, simple, full = _make_handle(simple='1x A10:1',
                                            full='1x A10:1 (8 vCPUs)',
                                            infra_str='gcp (us-central1)')
        rec = self._base_record(handle=handle)
        with mock.patch(
                'sky.serve.serve_utils.resources_utils.'
                'get_readable_resources_repr',
                return_value=(simple, full)):
            rendered = serve_utils._format_replica_table([rec],
                                                         show_all=False,
                                                         pool=False)
        assert 'gcp (us-central1)' in rendered
        assert '1x A10:1' in rendered

    def test_no_crash_when_handle_and_fields_all_missing(self):
        """Worst case: new client + a record that somehow lost everything.
        Should render '-' instead of KeyError'ing or raising."""
        rec = self._base_record()  # handle=None, no infra, no resources
        rendered = serve_utils._format_replica_table([rec],
                                                     show_all=False,
                                                     pool=False)
        # Default placeholder is "-".
        assert ' - ' in rendered or rendered.rstrip().endswith('-')


class TestServerConstantsBump:
    """Guard against accidental rollback of the version bump."""

    def test_min_lazy_replica_handle_api_version_matches_api_version(self):
        assert (server_constants.MIN_LAZY_REPLICA_HANDLE_API_VERSION
                <= server_constants.API_VERSION)

    def test_api_version_is_at_least_51(self):
        assert server_constants.API_VERSION >= 51


class TestControllerHttpRetryTightened:
    """Bundled change: single attempt + 1s connect timeout cuts dead-pool
    pool_status latency from ~7s to ~1s per pool."""

    def test_single_attempt(self):
        assert serve_utils._CONTROLLER_HTTP_RETRY_ATTEMPTS == 1

    def test_short_connect_timeout(self):
        connect, _ = serve_utils._CONTROLLER_HTTP_TIMEOUT_SECONDS
        assert connect <= 1.0


class TestGetServiceStatusPickledParallel:
    """`get_service_status_pickled` fans out across services. The change
    must preserve four contracts: (a) returned list sorted by 'name'; (b)
    None statuses filtered out; (c) one service's failed snapshot is
    omitted without aborting its healthy peers; (d) contextvars propagate
    into workers so request-scoped log redirection keeps working."""

    def _fake_status(self, name):
        return {
            'name': name,
            'status': serve_state.ServiceStatus.READY,
            'replica_info': [],
        }

    def _fake_prepared(self, name):
        return serve_utils._PreparedServiceStatus(
            record=self._fake_status(name),
            pool=False,
            include_replica_info=False)

    def test_returns_sorted_by_name(self):
        # Vary lengths because pickle/base64 metadata otherwise happens to
        # preserve lexical order for these ASCII names.
        names = ['svc-z', 'service-aa', 'mid']

        def status(name, pool, *, with_replica_info, with_replica_counts,
                   with_target_num_replicas):
            assert pool is False
            assert with_replica_info is True
            assert with_replica_counts is False
            assert with_target_num_replicas is True
            return self._fake_prepared(name)

        with mock.patch('sky.serve.serve_utils._prepare_service_status',
                        side_effect=status):
            out = serve_utils.get_service_status_pickled(names, pool=False)
        # Decode the pickled 'name' fields to verify the final order.
        decoded_names = [
            pickle.loads(base64.b64decode(s['name'].encode('utf-8')))
            for s in out
        ]
        assert decoded_names == ['mid', 'service-aa', 'svc-z']

    def test_skips_none_results(self):
        """A service that vanished mid-call (`_get_service_status` returns
        None) must be silently dropped, not stuffed into the response."""

        def side(name, pool, *, with_replica_info, with_replica_counts,
                 with_target_num_replicas):
            del (pool, with_replica_info, with_replica_counts,
                 with_target_num_replicas)
            return None if name == 'svc-gone' else self._fake_prepared(name)

        with mock.patch('sky.serve.serve_utils._prepare_service_status',
                        side_effect=side):
            out = serve_utils.get_service_status_pickled(
                ['svc-a', 'svc-gone', 'svc-b'], pool=False)
        assert len(out) == 2

    def test_failed_service_is_omitted_without_aborting_peers(self):
        """A malformed snapshot is not evidence about any peer service:
        the failing service is dropped from this poll and every healthy
        peer is still returned instead of the whole batch failing."""

        class Boom(Exception):
            pass

        def side(name, pool, *, with_replica_info, with_replica_counts,
                 with_target_num_replicas):
            del (pool, with_replica_info, with_replica_counts,
                 with_target_num_replicas)
            if name == 'svc-boom':
                raise Boom('controller went away')
            return self._fake_prepared(name)

        with mock.patch('sky.serve.serve_utils._prepare_service_status',
                        side_effect=side):
            out = serve_utils.get_service_status_pickled(
                ['svc-a', 'svc-boom', 'svc-b'], pool=False)
        decoded_names = [
            pickle.loads(base64.b64decode(s['name'].encode('utf-8')))
            for s in out
        ]
        assert decoded_names == ['svc-a', 'svc-b']

    def test_empty_input_short_circuits(self):
        """Empty `service_names` must NOT spawn an executor (cheap path
        for the very common empty-cluster case)."""
        with mock.patch('sky.serve.serve_utils._prepare_service_status') as m:
            out = serve_utils.get_service_status_pickled([], pool=False)
        assert out == []
        m.assert_not_called()

    def test_runs_concurrently_not_serial(self):
        """Workers run in parallel: all four calls must reach a barrier
        together before they can continue.

        A serial implementation would strand the first call on the barrier
        and time out instead of letting all four proceed.
        """

        barrier = threading.Barrier(4)

        def slow(name, pool, *, with_replica_info, with_replica_counts,
                 with_target_num_replicas):
            del (pool, with_replica_info, with_replica_counts,
                 with_target_num_replicas)
            try:
                barrier.wait(timeout=2)
            except threading.BrokenBarrierError as e:
                raise AssertionError('expected parallel execution') from e
            time.sleep(0.1)
            return self._fake_prepared(name)

        with mock.patch('sky.serve.serve_utils._prepare_service_status',
                        side_effect=slow):
            out = serve_utils.get_service_status_pickled(
                ['s1', 's2', 's3', 's4'], pool=False)
        assert len(out) == 4

    def test_contextvars_propagated(self):
        """request_id / user_id contextvars must be visible inside worker
        threads — without `contextvars.copy_context()` the worker would
        see the default (empty) value and lose log scoping."""
        marker: 'contextvars.ContextVar[Optional[str]]' = (
            contextvars.ContextVar('test_marker', default=None))
        seen_in_worker: List[Optional[str]] = []

        def capture(name, pool, *, with_replica_info, with_replica_counts,
                    with_target_num_replicas):
            del (pool, with_replica_info, with_replica_counts,
                 with_target_num_replicas)
            seen_in_worker.append(marker.get())
            return self._fake_prepared(name)

        marker.set('caller-tag')
        with mock.patch('sky.serve.serve_utils._prepare_service_status',
                        side_effect=capture):
            serve_utils.get_service_status_pickled(['s1', 's2', 's3'],
                                                   pool=False)
        assert seen_in_worker == ['caller-tag'] * 3

    def test_worker_cap_respected(self):
        """For 100 services, max_workers must be capped at
        `_STATUS_FANOUT_MAX_WORKERS` — a runaway thread count would
        exhaust the DB connection pool on QueuePool deployments."""
        assert serve_utils._STATUS_FANOUT_MAX_WORKERS <= 16
