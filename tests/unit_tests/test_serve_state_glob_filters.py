"""Tests for mode-filtered serve-name globbing."""
# pylint: disable=invalid-name,protected-access

import pytest
from sqlalchemy import create_engine
from sqlalchemy import event

from sky.serve import serve_state
from sky.serve import service_spec


@pytest.fixture
def _mock_serve_db(tmp_path, monkeypatch):
    """Point serve_state at a fresh sqlite DB for one test."""
    db_path = tmp_path / 'serve_state_glob_filters.db'
    engine = create_engine(f'sqlite:///{db_path}')
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    serve_state.Base.metadata.create_all(engine)
    yield engine


def _add_service(name: str, *, pool: bool) -> None:
    spec = service_spec.SkyServiceSpec.from_yaml_config({
        'pool': {},
        'workers': 1,
    } if pool else {
        'replicas': 1,
    })
    if not pool:
        spec = spec.copy(lb_high_availability=False)
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
        spec=spec,
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


def test_get_glob_service_names_multi_pattern_union(_mock_serve_db):
    del _mock_serve_db
    _add_service('serve-alpha', pool=False)
    _add_service('serve-beta', pool=False)
    _add_service('pool-alpha', pool=True)

    # Union across patterns, with overlapping matches deduplicated.
    assert sorted(serve_state.get_glob_service_names(['serve-*',
                                                      '*alpha'])) == [
                                                          'pool-alpha',
                                                          'serve-alpha',
                                                          'serve-beta',
                                                      ]
    assert not serve_state.get_glob_service_names([])
    assert not serve_state.get_glob_service_names(['no-such-*'])


def test_get_glob_service_names_issues_single_query(_mock_serve_db):
    engine = _mock_serve_db
    _add_service('serve-alpha', pool=False)
    _add_service('serve-beta', pool=False)
    _add_service('pool-alpha', pool=True)

    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _count(unused_conn, unused_cursor, statement, *unused_args):
        if statement.lstrip().upper().startswith('SELECT'):
            statements.append(statement)

    try:
        names = serve_state.get_glob_service_names(
            ['serve-*', 'pool-*', '*alpha'])
    finally:
        event.remove(engine, 'before_cursor_execute', _count)

    assert sorted(names) == ['pool-alpha', 'serve-alpha', 'serve-beta']
    assert len(statements) == 1


def test_get_glob_service_names_treats_like_metacharacters_literally(
        _mock_serve_db):
    del _mock_serve_db
    _add_service('my_svc', pool=False)
    _add_service('myXsvc', pool=False)
    _add_service('my%svc', pool=False)

    # '_' and '%' in a pattern are literals, not SQL wildcards.
    assert serve_state.get_glob_service_names(['my_svc']) == ['my_svc']
    assert serve_state.get_glob_service_names(['my%svc']) == ['my%svc']
    # Glob wildcards still work: '?' is any single char, '*' any run.
    assert sorted(serve_state.get_glob_service_names(['my?svc'])) == [
        'my%svc',
        'myXsvc',
        'my_svc',
    ]
    assert sorted(serve_state.get_glob_service_names(['my*'])) == [
        'my%svc',
        'myXsvc',
        'my_svc',
    ]


def test_orphaned_child_names_glob_is_literal_for_metacharacters(
        _mock_serve_db):
    engine = _mock_serve_db
    with engine.begin() as conn:
        for name in ('child_a', 'childXa'):
            conn.execute(serve_state.version_specs_table.insert().values(
                service_name=name, version=1))

    assert sorted(serve_state.get_orphaned_service_child_names()) == [
        'childXa',
        'child_a',
    ]
    assert serve_state.get_orphaned_service_child_names(['child_a'
                                                        ]) == ['child_a']
    assert sorted(serve_state.get_orphaned_service_child_names(
        ['child?a'])) == ['childXa', 'child_a']
