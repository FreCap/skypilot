"""Concurrency tests for durable system configuration initialization."""
# pylint: disable=protected-access

import concurrent.futures

import sqlalchemy

from sky import global_user_state


def test_get_or_set_system_config_has_one_first_writer(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "system-config.db"}')
    global_user_state.system_config_table.create(engine)
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: engine)

    candidates = [f'candidate-{index}' for index in range(8)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        values = list(
            pool.map(
                lambda candidate: global_user_state.get_or_set_system_config(
                    'server-identity', candidate), candidates))

    assert len(set(values)) == 1
    assert values[0] in candidates
    assert global_user_state.get_system_config('server-identity') == values[0]
