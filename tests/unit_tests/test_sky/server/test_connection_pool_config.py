"""Tests for API server PostgreSQL connection-budget allocation."""

from unittest import mock

import pytest

from sky.jobs import utils as job_utils
from sky.server import config


@pytest.fixture(autouse=True)
def _disable_consolidation_mode(monkeypatch):
    monkeypatch.setattr(job_utils, 'is_consolidation_mode', lambda: False)


@mock.patch('sky.utils.common_utils.get_mem_size_gb', return_value=8)
@mock.patch('sky.utils.common_utils.get_cpu_count', return_value=4)
def test_pool_budget_reserves_capacity_for_unpooled_sessions(
        _cpu_count, _mem_size_gb):
    # 8 long + 9 short + 4 API workers + the supervisor = 22 processes.
    server_config = config.compute_server_config(deploy=True,
                                                 max_db_connections=100,
                                                 quiet=True)

    assert server_config.num_db_connections_per_worker == 1
    assert server_config.long_worker_config.num_db_connections_per_worker == 1
    assert server_config.short_worker_config.num_db_connections_per_worker == 1
    process_count = (server_config.num_server_workers +
                     server_config.long_worker_config.garanteed_parallelism +
                     server_config.short_worker_config.garanteed_parallelism +
                     1)
    assert process_count == 22
    assert process_count * server_config.num_db_connections_per_worker <= 100
    # The other 78 slots remain available to dedicated advisory-lock sessions,
    # async connections, controllers, and other deployment traffic.
    assert 100 - (process_count *
                  server_config.num_db_connections_per_worker) == 78


@mock.patch('sky.utils.common_utils.get_mem_size_gb', return_value=8)
@mock.patch('sky.utils.common_utils.get_cpu_count', return_value=4)
def test_pool_reuses_one_ordinary_connection_per_process(
        _cpu_count, _mem_size_gb):
    server_config = config.compute_server_config(deploy=True,
                                                 max_db_connections=1000,
                                                 quiet=True)

    assert server_config.num_db_connections_per_worker == 1


@mock.patch('sky.utils.common_utils.get_mem_size_gb', return_value=8)
@mock.patch('sky.utils.common_utils.get_cpu_count', return_value=4)
def test_pooling_is_disabled_when_each_process_cannot_get_one_connection(
        _cpu_count, _mem_size_gb):
    server_config = config.compute_server_config(deploy=True,
                                                 max_db_connections=21,
                                                 quiet=True)

    assert server_config.num_db_connections_per_worker == 0
    assert server_config.long_worker_config.num_db_connections_per_worker == 0
    assert server_config.short_worker_config.num_db_connections_per_worker == 0
