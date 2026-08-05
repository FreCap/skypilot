"""The truncation hint must name a flag the command actually accepts.

`sky serve status` truncates its replica table and told the operator to
"use --all". That option does not exist on `sky serve status` -- it exits
with "Error: No such option: --all" -- because show_all is reached there
through `--verbose`. `sky jobs pool status` does own `--all`, so the hint is
right for pools and wrong only for services.
"""
# pylint: disable=protected-access
import copy
import re
from unittest import mock

import pytest

from sky.client.cli import command
from sky.serve import serve_state
from sky.serve import serve_status_formatter

_ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')


def _replicas(count):
    replica = {
        'service_name': 'svc',
        'replica_id': 0,
        'version': 1,
        'endpoint': 'http://r',
        'launched_at': 1,
        'infra': 'aws/us-east-1',
        'resources_str': 'A100:1',
        'resources_str_full': 'A100:1, 8CPU',
        'status': serve_state.ReplicaStatus.READY,
        'used_by': None,
        'handle': None,
    }
    out = []
    for replica_id in range(count):
        one = copy.deepcopy(replica)
        one['replica_id'] = replica_id
        out.append(one)
    return out


def _render(*, pool, count=None):
    if count is None:
        count = serve_status_formatter._REPLICA_TRUNC_NUM + 1
    with mock.patch.object(serve_status_formatter.log_utils,
                           'readable_time_duration',
                           return_value='1m'):
        rendered = serve_status_formatter._format_replica_table(
            _replicas(count), show_all=False, pool=pool)
    return _ANSI_ESCAPE.sub('', rendered)


def _option_names(cli_command):
    names = set()
    for param in cli_command.params:
        names.update(param.opts)
    return names


def test_service_hint_names_the_verbose_flag():
    assert '(use -v to show all replicas)' in _render(pool=False)


def test_service_hint_never_names_a_nonexistent_all_flag():
    """The exact regression: --all exits with "no such option" here."""
    assert '--all' not in _render(pool=False)


def test_pool_hint_keeps_its_real_all_flag():
    assert '(use --all to show all workers)' in _render(pool=True)


@pytest.mark.parametrize('pool, flag', [(False, '-v'), (True, '--all')])
def test_the_suggested_flag_exists_on_the_command(pool, flag):
    """Pin the hint to the CLI, so a flag rename cannot silently re-break it."""
    cli_command = (command.jobs_pool_status if pool else command.serve_status)
    assert flag in _option_names(cli_command)


def test_no_hint_when_nothing_is_truncated():
    rendered = _render(pool=False,
                       count=serve_status_formatter._REPLICA_TRUNC_NUM)
    assert 'to show all' not in rendered
