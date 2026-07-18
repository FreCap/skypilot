"""Tests for controller-log owner lookups on the Serve log follow path."""

from unittest import mock

from sky.serve import serve_state
from sky.serve import serve_utils


def test_follow_polls_terminal_state_without_yaml_or_full_row(tmp_path):
    log_file = tmp_path / 'controller.log'
    log_file.write_text('controller-log\n')
    polls = 4
    owner_lookup = mock.Mock(side_effect=[{
        'pool': False,
        'resource_scope': None,
        'status': serve_state.ServiceStatus.READY,
    }] + [{
        'pool': False,
        'resource_scope': None,
        'status': serve_state.ServiceStatus.READY,
    }] * polls)

    def fake_follow(f, should_stop, stop_on_eof):
        del f, stop_on_eof
        for _ in range(polls):
            assert not should_stop()
        return iter(())

    with mock.patch('sky.serve.serve_utils._check_service_status_healthy',
                    return_value=None), \
         mock.patch('sky.serve.serve_utils.serve_state.'
                    'get_service_controller_owner',
                    owner_lookup), \
         mock.patch('sky.serve.serve_utils.serve_state.get_service_from_name',
                    side_effect=AssertionError(
                        'controller-log follow should not read the full '
                        'service row')), \
         mock.patch('sky.serve.serve_utils.yaml_utils.read_yaml_str',
                    side_effect=AssertionError(
                        'controller-log follow should not parse YAML')), \
         mock.patch('sky.serve.serve_utils.'
                    'generate_remote_controller_log_file_name',
                    return_value=str(log_file)), \
         mock.patch('sky.serve.serve_utils.log_utils.follow_logs',
                    side_effect=fake_follow):
        assert serve_utils.stream_serve_process_logs('svc',
                                                     stream_controller=True,
                                                     follow=True,
                                                     tail=None,
                                                     pool=False) == ''

    assert owner_lookup.call_count == polls + 1
