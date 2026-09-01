"""Pure contracts for clean SkyServe recreation over retained history."""

import copy
import datetime
import uuid

import pytest

from sky.serve import capacity_admission

_ASSOCIATION_ID = uuid.UUID('879dd26e-2b4c-4652-8304-a5542d27ee09')
_FINISHED_AT = datetime.datetime(2026,
                                 8,
                                 31,
                                 12,
                                 0,
                                 tzinfo=datetime.timezone.utc)
_QUIESCED_AT = _FINISHED_AT + datetime.timedelta(seconds=1)
_PROFILE = {
    'binding_protocol_version': 2,
    'profile_kind': 'ORDINARY_PAID',
    'profile_version': 1,
    'profile_digest': 'a' * 64,
    'capability_cohort_epoch': 14,
    'capability_profile_set_digest': 'b' * 64,
    'receipt_protocol_version': 1,
}


def _closed_rows(
    *,
    status: str = 'SUCCEEDED',
    cause: str = 'handler_succeeded',
) -> tuple[dict[str, object], dict[str, object]]:
    association = {
        'association_id': _ASSOCIATION_ID,
        'request_id': 'request-a',
        'terminal_status': status,
        'terminal_cause': cause,
        'terminal_execution_generation': 7,
        'execution_quiescence_required': True,
        'execution_quiesced_generation': 7,
        'execution_quiesced_at': _QUIESCED_AT,
        **_PROFILE,
    }
    request = {
        'request_id': 'request-a',
        'ordinary_launch_association_id': _ASSOCIATION_ID,
        'handler_name': 'sky.server.requests.non_pool_launch:launch',
        'status': status,
        'terminal_cause': cause,
        'finished_at': _FINISHED_AT,
        'execution_generation': 7,
        'execution_quiescence_required': True,
        'execution_quiesced_generation': 7,
        'execution_quiesced_at': _QUIESCED_AT,
        'resource_action_id': None,
        'resource_action_attempt': None,
        **_PROFILE,
    }
    return request, association


@pytest.mark.parametrize(('status', 'cause'),
                         [('SUCCEEDED', 'handler_succeeded'),
                          ('FAILED', 'handler_failed')])
def test_exact_terminal_request_root_is_closed(status, cause):
    request, association = _closed_rows(status=status, cause=cause)

    assert capacity_admission._retained_request_root_state(  # pylint: disable=protected-access
        request, association
    ) is capacity_admission._RetainedRequestRootState.CLOSED_QUIESCED  # pylint: disable=protected-access


@pytest.mark.parametrize(('field', 'value'), [
    ('status', 'RUNNING'),
    ('finished_at', None),
    ('execution_quiescence_required', False),
    ('execution_quiesced_generation', None),
    ('execution_quiesced_at', None),
    ('resource_action_id', uuid.UUID('1177384f-8252-46f9-af02-2cf93b2d6977')),
    ('resource_action_attempt', 1),
])
def test_unfinished_or_referenced_request_root_blocks(field, value):
    request, association = _closed_rows()
    request[field] = value

    assert capacity_admission._retained_request_root_state(  # pylint: disable=protected-access
        request,
        association) is capacity_admission._RetainedRequestRootState.BLOCKING  # pylint: disable=protected-access


def test_fully_linked_resource_action_request_root_blocks():
    request, association = _closed_rows()
    request['resource_action_id'] = uuid.UUID(
        '1177384f-8252-46f9-af02-2cf93b2d6977')
    request['resource_action_attempt'] = 1

    assert capacity_admission._retained_request_root_state(  # pylint: disable=protected-access
        request,
        association) is capacity_admission._RetainedRequestRootState.BLOCKING  # pylint: disable=protected-access


@pytest.mark.parametrize('resolution',
                         ['BOUND', 'CANCEL_REQUESTED', 'AMBIGUOUS'])
def test_terminal_request_awaiting_association_reduction_blocks(resolution):
    request, association = _closed_rows()
    association.update({
        'resolution': resolution,
        'terminal_status': None,
        'terminal_cause': None,
        'terminal_execution_generation': None,
    })

    assert capacity_admission._retained_request_root_state(  # pylint: disable=protected-access
        request,
        association) is capacity_admission._RetainedRequestRootState.BLOCKING  # pylint: disable=protected-access


@pytest.mark.parametrize(('resolution', 'terminal_updates'), [
    ('BOUND', {
        'terminal_status': 'SUCCEEDED'
    }),
    ('RESULT_RECORDED', {}),
    ('PROJECTED', {}),
])
def test_incomplete_association_terminal_tuple_is_malformed(
        resolution, terminal_updates):
    request, association = _closed_rows()
    association.update({
        'resolution': resolution,
        'terminal_status': None,
        'terminal_cause': None,
        'terminal_execution_generation': None,
        **terminal_updates,
    })

    assert capacity_admission._retained_request_root_state(  # pylint: disable=protected-access
        request,
        association) is capacity_admission._RetainedRequestRootState.MALFORMED  # pylint: disable=protected-access


@pytest.mark.parametrize(('request_updates', 'association_updates'), [
    ({
        'request_id': 'request-b'
    }, {}),
    ({
        'ordinary_launch_association_id': uuid.uuid4()
    }, {}),
    ({
        'handler_name': 'sky.server.requests.ordinary_launch:launch'
    }, {}),
    ({
        'profile_digest': 'c' * 64
    }, {}),
    ({
        'terminal_cause': 'handler_failed'
    }, {}),
    ({
        'terminal_cause': 'not-a-canonical-cause'
    }, {
        'terminal_cause': 'not-a-canonical-cause'
    }),
    ({
        'execution_generation': 8
    }, {}),
    ({
        'execution_quiesced_generation': 8
    }, {}),
    ({
        'execution_quiesced_at': _FINISHED_AT
    }, {}),
    ({}, {
        'execution_quiesced_at': _FINISHED_AT
    }),
])
def test_mismatched_terminal_request_root_is_malformed(request_updates,
                                                       association_updates):
    request, association = _closed_rows()
    request.update(copy.deepcopy(request_updates))
    association.update(copy.deepcopy(association_updates))

    assert capacity_admission._retained_request_root_state(  # pylint: disable=protected-access
        request,
        association) is capacity_admission._RetainedRequestRootState.MALFORMED  # pylint: disable=protected-access


def test_protocol_v1_request_root_remains_blocking():
    request, association = _closed_rows()
    request['handler_name'] = 'sky.server.requests.ordinary_launch:launch'
    for field in capacity_admission._BOUND_REQUEST_PROFILE_FIELDS:  # pylint: disable=protected-access
        request[field] = None
        association[field] = None

    assert capacity_admission._retained_request_root_state(  # pylint: disable=protected-access
        request,
        association) is capacity_admission._RetainedRequestRootState.BLOCKING  # pylint: disable=protected-access
