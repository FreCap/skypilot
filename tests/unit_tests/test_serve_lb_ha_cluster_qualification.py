"""Pure gate tests for the real-cluster HA qualification driver."""
# pylint: disable=protected-access

import asyncio
import importlib
import pathlib
import sys
import time

from sky.serve import constants


def _load_qualification_module():
    path = (pathlib.Path(__file__).resolve().parents[1] / 'skyserve' /
            'high_availability' / 'qualify_cluster.py')
    module_dir = str(path.parent)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    return importlib.import_module('qualify_cluster')


qualify_cluster = _load_qualification_module()


def _snapshot(outcomes=None,
              ready=500,
              probed=500,
              rss=128 * 1024**2,
              pod_uid='pod-1',
              histogram_counts=None,
              failure_streak_active=False,
              last_outcome='success',
              last_failure_recovery_seconds=1,
              max_failure_recovery_seconds=1):
    phases = {}
    if histogram_counts is not None:
        phases['kubernetes_pod_authority'] = {
            'histogram': {
                'upper_bounds': [0.1, 1.0],
                'counts': histogram_counts,
            }
        }
    return {
        'lb_pod_uid': pod_uid,
        'ready_replicas': ready,
        'routing_backend_count': ready,
        'occupancy_probed_backend_count': probed,
        'process_rss_bytes': rss,
        'ha_observability': {
            'role': {
                'outcomes': outcomes or {
                    'success': 10,
                },
                'last_outcome': last_outcome,
                'failure_streak_active': failure_streak_active,
                'last_failure_recovery_seconds': last_failure_recovery_seconds,
                'max_failure_recovery_seconds': max_failure_recovery_seconds,
                'controller': {
                    'phases_seconds': phases,
                },
            }
        },
    }


def _targets():
    return [
        qualify_cluster.Target('a', 'http://a'),
        qualify_cluster.Target('b', 'http://b')
    ]


def _watch_observation(outcome='success', covered=True):
    return {
        'operation': 'endpointslice-watch',
        'outcome': outcome,
        'duration_seconds': 1,
        'covered_full_duration': covered,
    }


def test_ha_inventory_ignores_legacy_services_in_shared_namespace():
    inventory = {
        'items': [{
            'metadata': {
                'labels': {
                    'skypilot-serve-lb': 'legacy',
                },
            },
            'spec': {
                'selector': {
                    'app': 'legacy-deployment',
                },
            },
        }, {
            'metadata': {
                'labels': {
                    'skypilot-serve-lb': 'ha-service',
                },
            },
            'spec': {
                'selector': {
                    'skypilot-serve-lb-slot': 'a',
                },
            },
        }]
    }

    assert qualify_cluster._ha_inventory_names(inventory) == {'ha-service'}


def test_planned_gate_requires_every_traffic_sample_to_succeed():
    before = {'a:a': _snapshot(), 'b:a': _snapshot()}
    after = {
        'a:a': _snapshot({'success': 20}),
        'b:a': _snapshot({'success': 20})
    }
    samples = [{
        'service': name,
        'elapsed_seconds': 5,
        'expected': True,
    } for name in ('a', 'b')]
    gates = qualify_cluster.evaluate_gates(
        targets=_targets(),
        mode='planned',
        expected_backends=500,
        fault={'triggered_at_seconds': 2},
        samples=samples,
        before=before,
        after=after,
        kubectl_observations=[_watch_observation()])
    assert gates['availability']
    assert gates['role_outcomes_classified']

    samples[0]['expected'] = False
    assert not qualify_cluster.evaluate_gates(
        targets=_targets(),
        mode='planned',
        expected_backends=500,
        fault={'triggered_at_seconds': 2},
        samples=samples,
        before=before,
        after=after,
        kubectl_observations=[_watch_observation()])['availability']


def test_active_loss_gate_enforces_recovery_and_failure_taxonomy():
    before = {'a:a': _snapshot(), 'b:a': _snapshot()}
    after = {
        'a:a': _snapshot({
            'success': 20,
            'pod_not_authoritative': 1,
        }),
        'b:a': _snapshot({
            'success': 20,
            'invalid_report': 1,
        }),
    }
    samples = [{
        'service': name,
        'elapsed_seconds': elapsed,
        'expected': expected,
    } for name in ('a', 'b') for elapsed, expected in ((10, False), (12, True))]
    gates = qualify_cluster.evaluate_gates(
        targets=_targets(),
        mode='active-loss',
        expected_backends=500,
        fault={'triggered_at_seconds': 10},
        samples=samples,
        before=before,
        after=after,
        kubectl_observations=[_watch_observation()])
    assert gates['availability']
    assert not gates['role_outcomes_classified']
    assert gates['details']['recovery_seconds'] == {'a': 2, 'b': 2}


def test_active_loss_cannot_hide_sustained_failure_after_early_success():
    snapshots = {'a:a': _snapshot()}
    samples = [{
        'service': 'a',
        'elapsed_seconds': elapsed,
        'expected': expected,
    } for elapsed, expected in ((10.1, True), (11, False), (30, False))]
    gates = qualify_cluster.evaluate_gates(
        targets=[qualify_cluster.Target('a', 'http://a')],
        mode='active-loss',
        expected_backends=500,
        fault={'triggered_at_seconds': 10},
        samples=samples,
        before=snapshots,
        after=snapshots,
        kubectl_observations=[_watch_observation()])
    assert gates['details']['recovery_seconds'] == {'a': None}
    assert not gates['availability']


def test_cluster_gate_rejects_resource_sample_and_kubernetes_failures():
    before = {'a:a': _snapshot(), 'b:a': _snapshot()}
    after = {
        'a:a': _snapshot(ready=499),
        'b:a': _snapshot(rss=400 * 1024**2),
    }
    samples = [{
        'service': name,
        'elapsed_seconds': 1,
        'expected': True,
    } for name in ('a', 'b')]
    gates = qualify_cluster.evaluate_gates(
        targets=_targets(),
        mode='observe',
        expected_backends=500,
        fault={'triggered_at_seconds': 0},
        samples=samples,
        before=before,
        after=after,
        kubectl_observations=[{
            'operation': 'get',
            'outcome': '429',
            'duration_seconds': 1,
        },
                              _watch_observation()])
    assert not gates['complete_backend_samples']
    assert not gates['lb_rss_under_75_percent']
    assert not gates['kubernetes_client_clean']


def test_standby_admission_zero_does_not_fail_snapshot_completeness():
    standby_before = _snapshot()
    standby_before['ready_replicas'] = 0
    standby_after = _snapshot({'success': 20})
    standby_after['ready_replicas'] = 0
    before = {'a:a': _snapshot(), 'a:b': standby_before}
    after = {'a:a': _snapshot({'success': 20}), 'a:b': standby_after}
    gates = qualify_cluster.evaluate_gates(
        targets=[qualify_cluster.Target('a', 'http://a')],
        mode='observe',
        expected_backends=500,
        fault={'triggered_at_seconds': 0},
        samples=[{
            'service': 'a',
            'elapsed_seconds': 1,
            'expected': True,
        }],
        before=before,
        after=after,
        kubectl_observations=[_watch_observation()])
    assert gates['complete_backend_samples']


def test_planned_transient_must_be_classified_and_recover_within_bound():
    before = {'a:a': _snapshot()}
    after = {
        'a:a': _snapshot({
            'success': 20,
            'pod_not_authoritative': 1,
        })
    }
    kwargs = dict(targets=[qualify_cluster.Target('a', 'http://a')],
                  mode='planned',
                  expected_backends=500,
                  fault={'triggered_at_seconds': 1},
                  samples=[{
                      'service': 'a',
                      'elapsed_seconds': 2,
                      'expected': True,
                  }],
                  before=before,
                  after=after,
                  kubectl_observations=[_watch_observation()])
    gates = qualify_cluster.evaluate_gates(**kwargs)
    assert gates['role_outcomes_classified']
    assert gates['role_channel_recovered']

    after['a:a']['ha_observability']['role'].update(
        last_failure_recovery_seconds=1, max_failure_recovery_seconds=60)
    assert not qualify_cluster.evaluate_gates(
        **kwargs)['role_channel_recovered']

    after['a:a']['ha_observability']['role'].update(
        failure_streak_active=True,
        last_outcome='pod_not_authoritative',
        last_failure_recovery_seconds=None,
        max_failure_recovery_seconds=1)
    assert not qualify_cluster.evaluate_gates(
        **kwargs)['role_channel_recovered']


def test_same_pod_counter_reset_does_not_mask_new_role_failure():
    before = {
        'a:a': _snapshot({
            'success': 20,
            'invalid_report': 5,
        })
    }
    after = {
        'a:a': _snapshot({
            'success': 1,
            'invalid_report': 2,
        })
    }
    gates = qualify_cluster.evaluate_gates(
        targets=[qualify_cluster.Target('a', 'http://a')],
        mode='observe',
        expected_backends=500,
        fault={'triggered_at_seconds': 0},
        samples=[{
            'service': 'a',
            'elapsed_seconds': 1,
            'expected': True,
        }],
        before=before,
        after=after,
        kubectl_observations=[_watch_observation()])
    assert gates['details']['unexpected_role_outcomes'] == {
        'a:a': {
            'invalid_report': 2,
        }
    }
    assert not gates['role_outcomes_classified']


def test_kubernetes_p99_uses_only_run_window_histogram_delta():
    before = {'a:a': _snapshot(histogram_counts=[0, 0, 100])}
    after = {'a:a': _snapshot(histogram_counts=[10, 0, 100])}
    assert qualify_cluster._role_kubernetes_p99(before, after) == 0.1


def test_endpoint_continuity_rejects_vacuous_or_failed_watch():
    snapshot = {'a:a': _snapshot()}
    kwargs = dict(targets=[qualify_cluster.Target('a', 'http://a')],
                  mode='observe',
                  expected_backends=500,
                  fault={'triggered_at_seconds': 0},
                  samples=[{
                      'service': 'a',
                      'elapsed_seconds': 1,
                      'expected': True,
                  }],
                  before=snapshot,
                  after=snapshot)
    assert not qualify_cluster.evaluate_gates(
        **kwargs, kubectl_observations=[])['endpoint_ready_continuity']
    assert not qualify_cluster.evaluate_gates(
        **kwargs, kubectl_observations=[_watch_observation('error', False)
                                       ])['endpoint_ready_continuity']


def test_endpoint_watch_records_early_process_exit(monkeypatch):

    class FakeProcess:
        """Minimal early-exit asyncio subprocess double."""

        def __init__(self):
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_eof()
            self.stderr = asyncio.StreamReader()
            self.stderr.feed_data(b'forbidden')
            self.stderr.feed_eof()
            self.returncode = 1

        async def wait(self):
            return self.returncode

    async def create_subprocess(*_args, **_kwargs):
        return FakeProcess()

    monkeypatch.setattr(asyncio, 'create_subprocess_exec', create_subprocess)
    recorder = qualify_cluster.KubectlRecorder('test', None)
    asyncio.run(
        recorder.watch_endpoint_slices({'stable'}, {'items': []}, 1,
                                       time.monotonic()))
    assert recorder.observations[-1]['outcome'] == 'error'
    assert not recorder.observations[-1]['covered_full_duration']


def test_snapshot_script_authenticates_without_embedding_token_value():
    script = qualify_cluster._lb_snapshot_script()
    compile(script, '<lb-snapshot>', 'exec')
    assert constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR in script
    assert constants.LB_AUTH_TOKENS_FILE_ENV_VAR in script
    assert constants.LB_AUTHORIZATION_HEADER in script
    assert 'Bearer ' in script
    assert 'secret-test-token' not in script


def test_discovery_excludes_terminating_pod_for_replaced_slot():

    class FakeKubectl:
        """Return one stable service and a rolling-replacement Pod list."""

        async def json(self, operation, *_args):
            if operation.endswith(':service'):
                return {
                    'items': [{
                        'metadata': {
                            'name': 'stable',
                            'labels': {
                                'skypilot-serve-incarnation': 'hash',
                            },
                        },
                        'spec': {
                            'selector': {
                                'skypilot-serve-lb-slot': 'a',
                            }
                        },
                    }]
                }
            return {
                'items': [{
                    'metadata': {
                        'name': 'old-a',
                        'uid': 'old',
                        'deletionTimestamp': 'now',
                        'labels': {
                            'skypilot-serve-incarnation': 'hash',
                            'skypilot-serve-lb-slot': 'a',
                        },
                    },
                    'status': {
                        'conditions': [{
                            'type': 'Ready',
                            'status': 'True',
                        }]
                    },
                }, {
                    'metadata': {
                        'name': 'new-a',
                        'uid': 'new',
                        'labels': {
                            'skypilot-serve-incarnation': 'hash',
                            'skypilot-serve-lb-slot': 'a',
                        },
                    },
                    'status': {
                        'conditions': [{
                            'type': 'Ready',
                            'status': 'True',
                        }]
                    },
                }]
            }

    discovered = asyncio.run(
        qualify_cluster._discover_service(
            FakeKubectl(), qualify_cluster.Target('svc', 'http://svc')))
    assert discovered['slots']['a']['name'] == 'new-a'


def test_traffic_loop_dispatches_at_fixed_rate_concurrently():

    class FakeSession:
        """Track overlapping request contexts."""

        def __init__(self):
            self.active = 0
            self.max_active = 0

        def request(self, *_args, **_kwargs):
            session = self

            class Response:
                """Slow successful response context."""

                status = 200

                async def __aenter__(self):
                    session.active += 1
                    session.max_active = max(session.max_active, session.active)
                    await asyncio.sleep(0.12)
                    return self

                async def __aexit__(self, *_args):
                    session.active -= 1

                async def read(self):
                    return b''

            return Response()

    session = FakeSession()
    samples = []
    started_at = time.monotonic()
    asyncio.run(
        qualify_cluster._traffic_loop(
            qualify_cluster.Target('svc', 'http://svc'), session, 0.15, 20,
            started_at, samples))
    assert len(samples) >= 3
    assert session.max_active >= 2


def test_active_loss_boundary_precedes_delete_call():

    class FakeKubectl:
        """Record when the asynchronous deletion is submitted."""

        delete_started_at = None

        async def run(self, *_args):
            self.delete_started_at = time.monotonic()
            await asyncio.sleep(0)

    kubectl = FakeKubectl()
    started_at = time.monotonic()
    fault = asyncio.run(
        qualify_cluster._delete_active_pods(kubectl, ['active-a'], started_at))

    assert kubectl.delete_started_at is not None
    assert fault['triggered_at_seconds'] <= (kubectl.delete_started_at -
                                             started_at)
    assert fault['action'] == 'delete-active-pods'
    assert fault['pods'] == ['active-a']


def test_sample_count_gate_rejects_under_sampled_service():
    snapshot = {'a:a': _snapshot()}
    gates = qualify_cluster.evaluate_gates(
        targets=[qualify_cluster.Target('a', 'http://a')],
        mode='observe',
        expected_backends=500,
        fault={'triggered_at_seconds': 0},
        samples=[{
            'service': 'a',
            'elapsed_seconds': 1,
            'expected': True,
        }],
        before=snapshot,
        after=snapshot,
        kubectl_observations=[_watch_observation()],
        minimum_samples_per_service=2)
    assert not gates['traffic_sample_count']
