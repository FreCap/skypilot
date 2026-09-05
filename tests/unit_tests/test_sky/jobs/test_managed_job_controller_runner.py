"""Tests for the standard-library-only managed-job controller runner."""
# pylint: disable=protected-access

import io

import pytest

from sky.jobs import managed_job_controller_runner


@pytest.mark.parametrize('terminal_state', ['Z', 'X', 'x'])
def test_runtime_owner_rejects_terminal_process_state(monkeypatch,
                                                      terminal_state):
    pid = 4242
    start_time_ticks = 12345
    fields_after_comm = ([terminal_state] + ['1'] * 18 +
                         [str(start_time_ticks)])
    process_stat = f'{pid} (api server) {" ".join(fields_after_comm)}'
    monkeypatch.setattr(managed_job_controller_runner,
                        'open',
                        lambda *args, **kwargs: io.StringIO(process_stat),
                        raising=False)

    assert not managed_job_controller_runner._runtime_owner_identity_matches(
        pid, start_time_ticks)
