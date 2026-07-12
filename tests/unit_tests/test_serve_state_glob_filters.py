"""Tests for mode-filtered serve-name globbing."""
# pylint: disable=invalid-name,protected-access

import pytest
from sqlalchemy import create_engine

from sky.serve import serve_state


@pytest.fixture
def _mock_serve_db(tmp_path, monkeypatch):
    """Point serve_state at a fresh sqlite DB for one test."""
    db_path = tmp_path / 'serve_state_glob_filters.db'
    engine = create_engine(f'sqlite:///{db_path}')
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    serve_state.Base.metadata.create_all(engine)
    yield engine


def _add_service(name: str, *, pool: bool) -> None:
    serve_state.add_service(
        name=name,
        controller_job_id=1,
        policy='policy',
        requested_resources_str='1x[CPU:1+]',
        load_balancing_policy='round_robin',
        status=serve_state.ServiceStatus.CONTROLLER_INIT,
        tls_encrypted=False,
        pool=pool,
        controller_pid=12345,
        entrypoint='entry',
        spec=None,
        yaml_content='yaml: v1',
        service_hash=f'hash-{name}',
    )


def test_get_glob_service_names_can_filter_by_mode(_mock_serve_db):
    del _mock_serve_db
    _add_service('serve-a', pool=False)
    _add_service('serve-b', pool=False)
    _add_service('pool-a', pool=True)

    assert sorted(serve_state.get_glob_service_names()) == [
        'pool-a',
        'serve-a',
        'serve-b',
    ]
    assert sorted(serve_state.get_glob_service_names(pool=False)) == [
        'serve-a',
        'serve-b',
    ]
    assert serve_state.get_glob_service_names(pool=True) == ['pool-a']


def test_get_glob_service_names_applies_mode_filter_to_patterns(_mock_serve_db):
    del _mock_serve_db
    _add_service('serve-alpha', pool=False)
    _add_service('pool-alpha', pool=True)
    _add_service('pool-beta', pool=True)

    assert serve_state.get_glob_service_names(['*alpha'],
                                              pool=False) == ['serve-alpha']
    assert sorted(serve_state.get_glob_service_names(['pool-*'],
                                                     pool=True)) == [
                                                         'pool-alpha',
                                                         'pool-beta',
                                                     ]
