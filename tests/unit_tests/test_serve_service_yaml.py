"""Tests for the redacted `service_yaml` on serve status records."""

from unittest import mock

from sky.serve import serve_state
from sky.serve import serve_utils
from sky.utils import yaml_utils

_USER_YAML = yaml_utils.dump_yaml_str({
    'run': 'echo hi',
    'secrets': {
        'API_KEY': 'super-secret'
    },
})
# Rendered controller YAML as stored in the version DB: the launchable
# config (plaintext secrets) with the original user YAML embedded.
_RENDERED_YAML = yaml_utils.dump_yaml_str({
    'resources': {
        'cpus': 1
    },
    'run': 'echo hi',
    'secrets': {
        'API_KEY': 'super-secret'
    },
    '_user_specified_yaml': _USER_YAML,
})


def _service_record(yaml_content, pool=False):
    return {
        'name': 'svc',
        'pool': pool,
        'controller_port': 30001,
        'version': 1,
        'hash': 'incarnation-a',
        'resource_scope': None,
        'yaml_content': yaml_content,
    }


def _get_status(**kwargs):
    return serve_utils._get_service_status(  # pylint: disable=protected-access
        'svc',
        pool=False,
        with_replica_info=False,
        with_target_num_replicas=False,
        **kwargs)


def test_service_status_returns_redacted_user_yaml(monkeypatch):
    monkeypatch.setattr(serve_state, 'get_service_from_name',
                        lambda name: _service_record(_RENDERED_YAML))

    record = _get_status()

    assert record is not None
    assert 'super-secret' not in record['service_yaml']
    parsed = yaml_utils.safe_load(record['service_yaml'])
    assert parsed['run'] == 'echo hi'
    assert parsed['secrets']['API_KEY'] == '<redacted>'
    # The user-specified YAML is shown, not the rendered launch config.
    assert 'resources' not in parsed


def test_service_yaml_falls_back_to_rendered_and_redacts(monkeypatch):
    rendered = yaml_utils.dump_yaml_str({
        'run': 'echo hi',
        'secrets': {
            'API_KEY': 'super-secret'
        },
    })
    monkeypatch.setattr(serve_state, 'get_service_from_name',
                        lambda name: _service_record(rendered))

    record = _get_status()

    assert 'super-secret' not in record['service_yaml']
    parsed = yaml_utils.safe_load(record['service_yaml'])
    assert parsed['run'] == 'echo hi'


def test_service_yaml_skipped_when_not_requested(monkeypatch):
    get_snapshot = mock.Mock(return_value=_service_record(None))
    get_full_record = mock.Mock(
        side_effect=AssertionError('YAML-free status must use slim snapshot'))
    monkeypatch.setattr(serve_state, 'get_service_status_snapshot',
                        get_snapshot)
    monkeypatch.setattr(serve_state, 'get_service_from_name', get_full_record)
    get_yaml = mock.Mock()
    monkeypatch.setattr(serve_utils, 'get_yaml_content', get_yaml)

    record = _get_status(with_yaml=False, status_snapshot_only=True)

    assert record is not None
    assert 'service_yaml' not in record
    get_snapshot.assert_called_once_with('svc', require_version=True)
    get_full_record.assert_not_called()
    get_yaml.assert_not_called()


def test_service_yaml_read_failure_returns_empty(monkeypatch):
    monkeypatch.setattr(serve_state, 'get_service_from_name',
                        lambda name: _service_record(None))

    def _raise(*args, **kwargs):
        raise FileNotFoundError()

    monkeypatch.setattr(serve_utils, 'get_yaml_content', _raise)

    record = _get_status()

    assert record is not None
    assert record['service_yaml'] == ''


def test_pickled_status_strips_raw_yaml_for_services(monkeypatch):
    monkeypatch.setattr(serve_state,
                        'get_glob_service_names',
                        lambda names, pool=None: ['svc'])
    monkeypatch.setattr(serve_state, 'get_service_from_name',
                        lambda name: _service_record(_RENDERED_YAML))
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda name: [])
    monkeypatch.setattr(serve_state, 'get_replica_status_counts',
                        lambda name: {})

    statuses = serve_utils.get_service_status_pickled(None,
                                                      pool=False,
                                                      summary_only=True)

    decoded = serve_utils.unpickle_service_status(statuses)[0]
    assert 'yaml_content' not in decoded
    assert 'super-secret' not in decoded['service_yaml']


def test_pickled_status_keeps_raw_yaml_for_pools(monkeypatch):
    # The batch coordinator and pool worker-count updates parse the raw
    # YAML from pool status records back into a launchable task.
    rendered_pool = yaml_utils.dump_yaml_str({
        'resources': {
            'cpus': 1
        },
        'run': 'echo hi',
        'service': {
            'pool': True
        },
    })
    monkeypatch.setattr(serve_state,
                        'get_glob_service_names',
                        lambda names, pool=None: ['svc'])
    monkeypatch.setattr(serve_state, 'get_service_from_name',
                        lambda name: _service_record(rendered_pool, pool=True))
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda name: [])
    monkeypatch.setattr(serve_state, 'get_replica_status_counts',
                        lambda name: {})
    monkeypatch.setattr(serve_utils, 'get_yaml_content',
                        lambda *args, **kwargs: rendered_pool)

    statuses = serve_utils.get_service_status_pickled(None,
                                                      pool=True,
                                                      summary_only=True)

    decoded = serve_utils.unpickle_service_status(statuses)[0]
    assert decoded['yaml_content'] == rendered_pool
    assert 'service_yaml' not in decoded
