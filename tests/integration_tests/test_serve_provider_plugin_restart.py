"""Component regression for Serve's process-local provider inventory facet.

This enters the production plugin loader, provider registry, and aggregate
instance-status projection in a fresh ``spawn`` process.  A file-backed fake
provider is the single replaced boundary.  Its delayed visibility and partial
lifecycle state survive a controller-like process restart, and its singleton
method is a negative control that fails if aggregate dispatch falls back.

This is not an unpaid end-to-end test: it does not enter the public API, run a
real PostgreSQL-backed Serve controller, or exercise launch planning.
"""
# pylint: disable=protected-access

from __future__ import annotations

import contextlib
import fcntl
import json
import multiprocessing
import os
import pathlib
import time
import traceback
from types import SimpleNamespace
from typing import Any, Iterator

import pytest

from sky import provision
from sky.backends import backend_utils
from sky.provision import provider_facets
from sky.server import plugins
from sky.utils import status_lib

_PROVIDER_NAME = 'durable-inventory-fake'


@contextlib.contextmanager
def _locked_state(path: str) -> Iterator[dict[str, Any]]:
    """Read and durably update the fake provider's cross-process state."""
    with open(path, 'r+', encoding='utf-8') as state_file:
        fcntl.flock(state_file.fileno(), fcntl.LOCK_EX)
        try:
            state = json.load(state_file)
            yield state
            state_file.seek(0)
            json.dump(state, state_file, sort_keys=True)
            state_file.truncate()
            state_file.flush()
            os.fsync(state_file.fileno())
        finally:
            fcntl.flock(state_file.fileno(), fcntl.LOCK_UN)


class _DurableInventoryLifecycle:
    """Strict lifecycle whose only live behavior is its inventory facet."""

    def __init__(self, state_path: str) -> None:
        self._state_path = state_path

    def query_instances_batch(
        self,
        queries: tuple[provider_facets.InstanceStatusInventoryQueryV1, ...],
        *,
        deadline_monotonic: float,
    ) -> tuple[provider_facets.InstanceStatusInventoryObservationV1, ...]:
        with _locked_state(self._state_path) as state:
            batch_call = int(state['batch_calls']) + 1
            state['batch_calls'] = batch_call
            state['events'].append({
                'kind': 'batch',
                'pid': os.getpid(),
                'batch_call': batch_call,
                'deadline_monotonic': deadline_monotonic,
            })
            visible_after = dict(state['visible_after_batch'])
            clusters = dict(state['clusters'])

        observations = []
        for query in queries:
            cluster_name = query.cluster_name_on_cloud
            if batch_call < int(visible_after.get(cluster_name, 1)):
                observations.append(
                    provider_facets.InstanceStatusInventoryObservationV1(
                        query_id=query.query_id,
                        disposition=(
                            provider_facets.
                            InstanceStatusInventoryDispositionV1.UNKNOWN),
                        error='fake provider visibility is delayed'))
                continue
            status_name = clusters.get(cluster_name)
            entries = ()
            if status_name is not None:
                entries = (provider_facets.InstanceStatusInventoryEntryV1(
                    instance_id=f'fake-instance:{cluster_name}',
                    status=status_lib.ClusterStatus(status_name)),)
            observations.append(
                provider_facets.InstanceStatusInventoryObservationV1(
                    query_id=query.query_id,
                    disposition=(provider_facets.
                                 InstanceStatusInventoryDispositionV1.OBSERVED),
                    entries=entries))
        return tuple(observations)

    def query_instances(
        self,
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> dict[str, tuple[status_lib.ClusterStatus | None, str | None]]:
        del cluster_name, cluster_name_on_cloud, provider_config
        del non_terminated_only, retry_if_missing
        with _locked_state(self._state_path) as state:
            state['singleton_calls'] = int(state['singleton_calls']) + 1
            state['events'].append({
                'kind': 'singleton-bypass',
                'pid': os.getpid(),
            })
        raise AssertionError('aggregate inventory used a singleton fallback')

    def bootstrap_instances(self, region: str, cluster_name_on_cloud: str,
                            config: Any) -> Any:
        del region, cluster_name_on_cloud
        return config

    def run_instances(self, region: str, cluster_name: str,
                      cluster_name_on_cloud: str, config: Any) -> Any:
        del region, cluster_name, cluster_name_on_cloud, config
        raise AssertionError('inventory probe attempted a provider launch')

    def stop_instances(self,
                       cluster_name_on_cloud: str,
                       provider_config: dict[str, Any],
                       worker_only: bool = False) -> None:
        del cluster_name_on_cloud, provider_config, worker_only
        raise AssertionError('inventory probe attempted a provider stop')

    def terminate_instances(self,
                            cluster_name_on_cloud: str,
                            provider_config: dict[str, Any],
                            worker_only: bool = False) -> None:
        del cluster_name_on_cloud, provider_config, worker_only
        raise AssertionError('inventory probe attempted provider termination')

    def wait_instances(self, region: str, cluster_name_on_cloud: str,
                       state: status_lib.ClusterStatus | None) -> None:
        del region, cluster_name_on_cloud, state
        raise AssertionError('inventory probe attempted a provider wait')

    def get_cluster_info(self,
                         region: str,
                         cluster_name_on_cloud: str,
                         provider_config: dict[str, Any] | None = None) -> Any:
        del region, cluster_name_on_cloud, provider_config
        raise AssertionError('inventory probe attempted cluster-info I/O')


class DurableInventoryPlugin(plugins.BasePlugin):
    """Controller-only plugin installed from the real plugin config path."""

    load_contexts = frozenset({plugins.PluginContext.CONTROLLER})

    def __init__(self, state_path: str) -> None:
        self._state_path = state_path

    def install(self, extension_context: plugins.ExtensionContext) -> None:
        with _locked_state(self._state_path) as state:
            state['events'].append({
                'kind': 'install',
                'pid': os.getpid(),
                'context': extension_context.context.value,
            })
        provision.register_provisioner_bundle(
            provider_facets.ProvisionerBundleV1(
                canonical_name=_PROVIDER_NAME,
                instance_lifecycle=_DurableInventoryLifecycle(
                    self._state_path)))


class _FakeCloud:

    def __repr__(self) -> str:
        return _PROVIDER_NAME


def _serialize_observation(
    observation: provider_facets.InstanceStatusInventoryObservationV1,
) -> dict[str, Any]:
    return {
        'disposition': observation.disposition.value,
        'entries': [{
            'instance_id': entry.instance_id,
            'status': None if entry.status is None else entry.status.value,
        } for entry in observation.entries],
        'error': observation.error,
    }


def _run_inventory_probe(config_path: str, result_connection) -> None:
    """Load the plugin in a fresh process and enter production dispatch."""
    try:
        os.environ[plugins._PLUGINS_CONFIG_ENV_VAR] = config_path
        plugins.load_plugins(
            plugins.ExtensionContext(context=plugins.PluginContext.CONTROLLER))
        handles = {
            1: SimpleNamespace(
                launched_resources=SimpleNamespace(cloud=_FakeCloud()),
                cluster_name='display-delayed',
                cluster_name_on_cloud='provider-delayed'),
            2: SimpleNamespace(
                launched_resources=SimpleNamespace(cloud=_FakeCloud()),
                cluster_name='display-partial',
                cluster_name_on_cloud='provider-partial'),
        }
        provider_configs = {key: {'region': 'fake-region'} for key in handles}
        observations = backend_utils.query_cluster_instance_statuses_batch(
            handles, provider_configs, deadline_monotonic=time.monotonic() + 5)
        result_connection.send({
            'pid': os.getpid(),
            'observations': {
                key: _serialize_observation(observation)
                for key, observation in observations.items()
            },
        })
    except BaseException:  # pylint: disable=broad-except
        result_connection.send({'error': traceback.format_exc()})
    finally:
        result_connection.close()


def _spawn_inventory_probe(context: multiprocessing.context.BaseContext,
                           config_path: pathlib.Path) -> dict[str, Any]:
    result_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(target=_run_inventory_probe,
                              args=(str(config_path), child_connection))
    process.start()
    child_connection.close()
    try:
        if not result_connection.poll(20):
            raise AssertionError('provider inventory subprocess timed out')
        result = result_connection.recv()
    finally:
        result_connection.close()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
    assert process.exitcode == 0
    assert 'error' not in result, result.get('error')
    return result


@pytest.mark.component
def test_controller_plugin_inventory_survives_restart_and_delayed_visibility(
        tmp_path: pathlib.Path) -> None:
    state_path = tmp_path / 'provider-state.json'
    state_path.write_text(json.dumps({
        'batch_calls': 0,
        'singleton_calls': 0,
        'visible_after_batch': {
            'provider-delayed': 2,
        },
        'clusters': {
            'provider-delayed': status_lib.ClusterStatus.UP.value,
            'provider-partial': status_lib.ClusterStatus.UP.value,
        },
        'events': [],
    }),
                          encoding='utf-8')
    config_path = tmp_path / 'plugins.yaml'
    config_path.write_text(json.dumps({
        'plugins': [{
            'class': f'{__name__}.DurableInventoryPlugin',
            'parameters': {
                'state_path': str(state_path),
            },
        }],
    }),
                           encoding='utf-8')

    context = multiprocessing.get_context('spawn')
    first = _spawn_inventory_probe(context, config_path)
    assert first['observations'][1] == {
        'disposition': 'unknown',
        'entries': [],
        'error': 'fake provider visibility is delayed',
    }
    assert first['observations'][2] == {
        'disposition': 'observed',
        'entries': [{
            'instance_id': 'fake-instance:provider-partial',
            'status': 'UP',
        }],
        'error': None,
    }

    # The provider partially removes one launch while the Serve-like process
    # is down.  The next spawned process must reload the plugin and observe the
    # same durable inventory generation, not an inherited in-memory registry.
    with _locked_state(str(state_path)) as state:
        del state['clusters']['provider-partial']

    second = _spawn_inventory_probe(context, config_path)
    assert second['observations'][1] == {
        'disposition': 'observed',
        'entries': [{
            'instance_id': 'fake-instance:provider-delayed',
            'status': 'UP',
        }],
        'error': None,
    }
    assert second['observations'][2] == {
        'disposition': 'observed',
        'entries': [],
        'error': None,
    }

    with open(state_path, encoding='utf-8') as state_file:
        state = json.load(state_file)
    assert state['batch_calls'] == 2
    assert state['singleton_calls'] == 0
    assert first['pid'] != second['pid']
    installs = [
        event for event in state['events'] if event['kind'] == 'install'
    ]
    batches = [event for event in state['events'] if event['kind'] == 'batch']
    assert [event['context'] for event in installs] == [
        plugins.PluginContext.CONTROLLER.value,
        plugins.PluginContext.CONTROLLER.value,
    ]
    assert [event['pid'] for event in installs] == [first['pid'], second['pid']]
    assert [event['pid'] for event in batches] == [first['pid'], second['pid']]
    assert not any(
        event['kind'] == 'singleton-bypass' for event in state['events'])
