"""Test exception serialization and deserialization."""

import pickle

from sky import exceptions
from sky.utils import status_lib


def _serialize_deserialize(e: Exception) -> Exception:
    serialized = exceptions.serialize_exception(e)
    return exceptions.deserialize_exception(serialized)


def test_value_error():
    """Test that exceptions can be serialized and deserialized."""
    e = ValueError('test')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, ValueError)
    assert str(deserialized) == 'test'


def test_builtin_exception_attributes():
    """Built-in exception attributes are restored after construction."""
    e = TypeError('test')
    e.add_note('when serializing a result')

    deserialized = _serialize_deserialize(e)

    assert isinstance(deserialized, TypeError)
    assert str(deserialized) == 'test'
    assert deserialized.__notes__ == ['when serializing a result']


def test_execution_control_errors_are_picklable():
    """Retry and pause exceptions preserve their constructor state."""
    retryable = exceptions.ExecutionRetryableError('retry', 'later', 3)
    paused = exceptions.ExecutionPausedError('pause', 'waiting', 5,
                                             {'signal': 'ready'})

    restored_retryable = pickle.loads(pickle.dumps(retryable))
    restored_paused = pickle.loads(pickle.dumps(paused))

    assert isinstance(restored_retryable, exceptions.ExecutionRetryableError)
    assert restored_retryable.hint == 'later'
    assert restored_retryable.retry_wait_seconds == 3
    assert isinstance(restored_paused, exceptions.ExecutionPausedError)
    assert restored_paused.hint == 'waiting'
    assert restored_paused.retry_wait_seconds == 5
    assert restored_paused.continue_condition == {'signal': 'ready'}


def test_resources_unavailable_error():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.ResourcesUnavailableError(
        'test',
        failover_history=[
            ValueError('test1'),
            exceptions.ResourcesUnavailableError('test2')
        ])
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.ResourcesUnavailableError)
    assert str(deserialized) == 'test'
    assert str(deserialized.failover_history[0]) == 'test1'
    assert str(deserialized.failover_history[1]) == 'test2'
    assert deserialized.stacktrace == 'test_stacktrace'


def test_invalid_cloud_configs():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.InvalidCloudConfigs('test')
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.InvalidCloudConfigs)
    assert str(deserialized) == 'test'
    assert deserialized.stacktrace == 'test_stacktrace'


def test_provision_prechecks_error():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.ProvisionPrechecksError(reasons=[
        ValueError('test1'),
        exceptions.ResourcesUnavailableError('test2')
    ])
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.ProvisionPrechecksError)
    assert str(deserialized) == ''
    assert str(deserialized.reasons[0]) == 'test1'
    assert str(deserialized.reasons[1]) == 'test2'
    assert deserialized.stacktrace == 'test_stacktrace'


def test_command_failure_exception():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.CommandFailureException('test_command', 'test_failure',
                                           'test_error_msg',
                                           'test_detailed_reason')
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.CommandFailureException)
    assert str(deserialized).startswith('Command test_command test_failure.')
    assert deserialized.command == 'test_command'
    assert deserialized.failure == 'test_failure'
    assert deserialized.error_msg == 'test_error_msg'
    assert deserialized.detailed_reason == 'test_detailed_reason'
    assert deserialized.stacktrace == 'test_stacktrace'


def test_command_error():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.CommandError(1, 'test_command', 'test_error_msg',
                                'test_detailed_reason')
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.CommandError)
    assert str(deserialized).startswith(
        'Command test_command failed with return code 1.')
    assert deserialized.returncode == 1
    assert deserialized.command == 'test_command'
    assert deserialized.error_msg == 'test_error_msg'
    assert deserialized.detailed_reason == 'test_detailed_reason'
    assert deserialized.stacktrace == 'test_stacktrace'


def test_cluster_not_up_error():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.ClusterNotUpError('test',
                                     cluster_status=status_lib.ClusterStatus.UP)
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.ClusterNotUpError)
    assert str(deserialized) == 'test'
    assert deserialized.cluster_status == status_lib.ClusterStatus.UP
    assert deserialized.handle is None
    assert deserialized.stacktrace == 'test_stacktrace'


def test_fetch_cluster_info_error():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.FetchClusterInfoError(
        exceptions.FetchClusterInfoError.Reason.HEAD)
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.FetchClusterInfoError)
    assert str(deserialized) == ''
    assert deserialized.reason == exceptions.FetchClusterInfoError.Reason.HEAD
    assert deserialized.stacktrace == 'test_stacktrace'


def test_aws_az_fetching_error():
    """Test that exceptions can be serialized and deserialized."""
    e = exceptions.AWSAzFetchingError(
        region='us-east-1',
        reason=exceptions.AWSAzFetchingError.Reason.AUTH_FAILURE)
    setattr(e, 'stacktrace', 'test_stacktrace')
    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.AWSAzFetchingError)
    assert str(deserialized).startswith(
        'Failed to access AWS services. Please check your AWS credentials.')
    assert deserialized.region == 'us-east-1'
    assert deserialized.reason == exceptions.AWSAzFetchingError.Reason.AUTH_FAILURE
    assert deserialized.stacktrace == 'test_stacktrace'


def test_deserialize_none_input():
    """Test that None input returns RuntimeError instead of crashing."""
    e = exceptions.deserialize_exception(None)
    assert isinstance(e, RuntimeError)
    assert 'Unknown server error' in str(e)


def test_deserialize_string_input():
    """Test that string input is wrapped in RuntimeError."""
    e = exceptions.deserialize_exception('Something went wrong')
    assert isinstance(e, RuntimeError)
    assert str(e) == 'Something went wrong'

    # Empty string
    e = exceptions.deserialize_exception('')
    assert isinstance(e, RuntimeError)
    assert str(e) == ''


def test_deserialize_non_dict_input():
    """Test that non-dict inputs (list, int, bool) return RuntimeError."""
    for bad_input in [42, True, [{'loc': ['body'], 'msg': 'invalid'}]]:
        e = exceptions.deserialize_exception(bad_input)
        assert isinstance(e, RuntimeError)
        assert 'Server error' in str(e)


def test_deserialize_partial_dict():
    """Test that dicts with 'type' but missing other keys still work."""
    # Dict with only 'type' - should construct with no args
    e = exceptions.deserialize_exception({'type': 'ValueError'})
    assert isinstance(e, ValueError)

    # Dict with 'type' and 'message' but missing others
    e = exceptions.deserialize_exception({
        'type': 'ValueError',
        'message': 'test'
    })
    assert isinstance(e, ValueError)

    # Empty dict - no 'type' key, falls through to RuntimeError
    e = exceptions.deserialize_exception({})
    assert isinstance(e, RuntimeError)

    # Unknown type with message uses message in fallback
    e = exceptions.deserialize_exception({
        'type': 'NonExistent',
        'message': 'details'
    })
    assert isinstance(e, Exception)
    assert 'NonExistent' in str(e)
    assert 'details' in str(e)

    # Unknown type without message still works
    e = exceptions.deserialize_exception({'type': 'NonExistent'})
    assert isinstance(e, Exception)
    assert 'NonExistent' in str(e)


def test_wrap_unsafe_exceptions():
    """Test that non-safe exceptions are wrapped properly."""

    # Mock a cloud exception
    class MockBotoError(Exception):
        pass

    MockBotoError.__module__ = 'botocore.exceptions'

    # Create mock cloud exception
    boto_error = MockBotoError('Failed to launch instance')

    # Serialize and deserialize the exception
    wrapped = _serialize_deserialize(boto_error)

    # Verify it was converted to CloudError
    assert isinstance(wrapped, exceptions.CloudError)
    assert wrapped.cloud_provider == 'botocore'
    assert wrapped.error_type == 'MockBotoError'
    assert str(
        wrapped) == 'botocore error (MockBotoError): Failed to launch instance'

    # Verify safe exceptions pass through unchanged
    value_error = ValueError('Invalid value')
    safe_error = _serialize_deserialize(value_error)
    assert isinstance(safe_error, ValueError)
    assert str(safe_error) == 'Invalid value'

    # Verify SkyPilot exceptions pass through unchanged
    sky_error = exceptions.ClusterNotUpError('test cluster')
    sky_safe = _serialize_deserialize(sky_error)
    assert isinstance(sky_safe, exceptions.ClusterNotUpError)
    assert str(sky_safe) == 'test cluster'


def test_skypilot_exception_with_notes_round_trips():
    """Notes must not be passed to a SkyPilot exception's constructor.

    Built-in exceptions are constructed positionally, but SkyPilot ones are
    rebuilt from keyword attributes, and Python 3.14 attaches ``__notes__``
    to exceptions raised through some stdlib paths.
    """
    e = exceptions.ResourcesUnavailableError('no capacity')
    e.__notes__ = ['context added by the interpreter']

    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, exceptions.ResourcesUnavailableError)
    assert str(deserialized) == 'no capacity'
    assert deserialized.__notes__ == ['context added by the interpreter']
    # Ordinary attributes still round-trip.
    assert deserialized.no_failover is False


def test_builtin_exception_with_notes_round_trips():
    e = TypeError('Object of type set is not JSON serializable')
    e.__notes__ = ["when serializing dict item 'bad'"]

    deserialized = _serialize_deserialize(e)
    assert isinstance(deserialized, TypeError)
    assert deserialized.__notes__ == ["when serializing dict item 'bad'"]


def test_deserialize_tolerates_attribute_the_constructor_rejects():
    """An unusable attribute must not mask the original error."""
    deserialized = exceptions.deserialize_exception({
        'type': 'ResourcesUnavailableError',
        'message': 'boom',
        'args': ('boom',),
        'attributes': {
            'not_a_constructor_argument': 1
        },
    })
    assert 'boom' in str(deserialized)


def test_attribute_that_cannot_be_set_does_not_lose_the_error():
    """An unsettable attribute must not fail the whole deserialization.

    deserialize_exception runs on the error path, so an attribute that
    cannot be assigned (read-only, slotted, or type-checked like
    ``__class__``) must not replace the caller's real error with an
    unrelated one.
    """
    serialized = {
        'type': 'ValueError',
        'message': 'boom',
        'args': ('boom',),
        'attributes': {
            'context': 'while encoding',
            '__class__': int,
        },
    }

    deserialized = exceptions.deserialize_exception(serialized)

    assert isinstance(deserialized, ValueError)
    assert str(deserialized) == 'boom'
    assert getattr(deserialized, 'context') == 'while encoding'
