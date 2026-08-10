"""Facade contracts for the cluster-record identity persistence gateway."""
# pylint: disable=protected-access

import inspect
import pickle
import typing
import uuid

import pytest
from sqlalchemy import orm

from sky import global_user_state

_RECORD_UUID = uuid.UUID('11111111-1111-4111-8111-111111111111')


def test_cluster_record_identity_public_type_contract() -> None:
    public_types = (
        global_user_state.ClusterRecordIdentityWriteOutcome,
        global_user_state.ClusterRecordIdentityConflictError,
        global_user_state.ClusterRecordHandleChangedError,
        global_user_state.ClusterRecordRemovalOutcome,
        global_user_state.ClusterRecordIdentitySnapshot,
    )
    assert [public_type.__module__ for public_type in public_types] == [
        'sky.global_user_state',
    ] * len(public_types)
    assert [public_type.__qualname__ for public_type in public_types] == [
        'ClusterRecordIdentityWriteOutcome',
        'ClusterRecordIdentityConflictError',
        'ClusterRecordHandleChangedError',
        'ClusterRecordRemovalOutcome',
        'ClusterRecordIdentitySnapshot',
    ]
    assert issubclass(global_user_state.ClusterRecordHandleChangedError,
                      global_user_state.ClusterRecordIdentityConflictError)
    assert {
        outcome.value
        for outcome in global_user_state.ClusterRecordIdentityWriteOutcome
    } == {'inserted', 'adopted'}
    assert {
        outcome.value
        for outcome in global_user_state.ClusterRecordRemovalOutcome
    } == {'removed_exact', 'already_absent'}

    snapshot = global_user_state.ClusterRecordIdentitySnapshot(
        cluster_name='cluster',
        cluster_record_uuid=_RECORD_UUID,
        serialized_handle=b'handle',
        handle={'marker': 'exact'},
    )
    assert pickle.loads(pickle.dumps(snapshot)) == snapshot


def test_cluster_record_identity_facade_signatures() -> None:
    functions_and_parameters = (
        (global_user_state._canonical_cluster_record_uuid, ('value',)),
        (global_user_state._lock_cluster_record_uuid_in_session,
         ('session', 'record_uuid')),
        (global_user_state._commit_cluster_record_identity_in_session,
         ('session', 'cluster_name', 'cluster_record_uuid', 'insert_values')),
        (global_user_state._read_cluster_record_identity_in_session,
         ('session', 'cluster_name', 'expected_cluster_record_uuid')),
        (global_user_state.get_cluster_record_identity_snapshot,
         ('cluster_name', 'expected_cluster_record_uuid')),
    )
    for function, expected_parameters in functions_and_parameters:
        assert tuple(
            inspect.signature(function).parameters) == expected_parameters

    commit_signature = inspect.signature(
        global_user_state._commit_cluster_record_identity_in_session)
    insert_values = commit_signature.parameters['insert_values']
    assert insert_values.kind is inspect.Parameter.KEYWORD_ONLY
    assert insert_values.default is None

    assert typing.get_type_hints(
        global_user_state._canonical_cluster_record_uuid) == {
            'value': uuid.UUID | str,
            'return': uuid.UUID,
        }
    assert typing.get_type_hints(
        global_user_state._lock_cluster_record_uuid_in_session) == {
            'session': orm.Session,
            'record_uuid': uuid.UUID,
            'return': type(None),
        }
    commit_types = typing.get_type_hints(
        global_user_state._commit_cluster_record_identity_in_session)
    assert commit_types == {
        'session': orm.Session,
        'cluster_name': str,
        'cluster_record_uuid': uuid.UUID | str,
        'insert_values': typing.Mapping[str, typing.Any] | None,
        'return': global_user_state.ClusterRecordIdentityWriteOutcome,
    }
    read_types = {
        'session': orm.Session,
        'cluster_name': str,
        'expected_cluster_record_uuid': uuid.UUID | str,
        'return': global_user_state.ClusterRecordIdentitySnapshot | None,
    }
    assert typing.get_type_hints(
        global_user_state._read_cluster_record_identity_in_session
    ) == read_types
    read_types.pop('session')
    assert typing.get_type_hints(
        global_user_state.get_cluster_record_identity_snapshot) == read_types


def test_cluster_record_identity_uuid_validation_contract() -> None:
    assert global_user_state._canonical_cluster_record_uuid(
        _RECORD_UUID) is _RECORD_UUID
    assert global_user_state._canonical_cluster_record_uuid(
        str(_RECORD_UUID)) == _RECORD_UUID

    with pytest.raises(TypeError, match='UUID or canonical UUID text'):
        global_user_state._canonical_cluster_record_uuid(1)
    for invalid in (
            '',
            '11111111111141118111111111111111',
            '11111111-1111-4111-8111-11111111111A',
            '{11111111-1111-4111-8111-111111111111}',
    ):
        with pytest.raises(ValueError, match='canonical UUID text'):
            global_user_state._canonical_cluster_record_uuid(invalid)
