"""Test exception serialization and deserialization."""

import inspect
import pickle

import pytest

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


def test_kubernetes_physical_identity_error_round_trip():
    """Executor safety classification survives the API exception wire."""
    error = exceptions.KubernetesPhysicalClusterIdentityError(
        'physical target is uncertain')

    deserialized = _serialize_deserialize(error)

    assert isinstance(deserialized,
                      exceptions.KubernetesPhysicalClusterIdentityError)
    assert isinstance(deserialized, exceptions.RequestCancelled)
    assert str(deserialized) == 'physical target is uncertain'


def test_reserved_fill_launch_fence_error_round_trip():
    """Terminal exact-placement drift survives the API exception wire."""
    error = exceptions.ReservedFillLaunchFenceError('candidate changed')

    deserialized = _serialize_deserialize(error)

    assert isinstance(deserialized, exceptions.ReservedFillLaunchFenceError)
    assert isinstance(deserialized, exceptions.RequestCancelled)
    assert str(deserialized) == 'candidate changed'


def test_reserved_fill_provider_present_error_round_trip():
    """Pinned provider identity survives the API exception wire."""
    error = exceptions.ReservedFillProviderPresentError(
        'provider resource remains present', ('ns/pod@uid',))

    deserialized = _serialize_deserialize(error)

    assert isinstance(deserialized, exceptions.ReservedFillProviderPresentError)
    assert isinstance(deserialized, exceptions.ReservedFillLaunchFenceError)
    assert deserialized.provider_resource_ids == ('ns/pod@uid',)
    assert str(deserialized) == 'provider resource remains present'


def test_exception_notes_are_restored_outside_constructor_kwargs():
    error = TypeError('test')
    setattr(error, '__notes__', ['when serializing dict item bad'])

    serialized = exceptions.serialize_exception(error)
    assert serialized['notes'] == ['when serializing dict item bad']
    assert '__notes__' not in serialized['attributes']

    restored = exceptions.deserialize_exception(serialized)
    assert isinstance(restored, TypeError)
    assert getattr(restored, '__notes__') == ['when serializing dict item bad']

    legacy = dict(serialized)
    legacy.pop('notes')
    legacy['attributes'] = {
        **serialized['attributes'],
        '__notes__': ['legacy note'],
    }
    restored_legacy = exceptions.deserialize_exception(legacy)
    assert isinstance(restored_legacy, TypeError)
    assert getattr(restored_legacy, '__notes__') == ['legacy note']


def test_builtin_exception_attributes():
    """Built-in exception attributes are restored after construction."""
    e = TypeError('test')
    e.add_note('when serializing a result')
    e.request_context = {'request_id': 'request-1'}

    deserialized = _serialize_deserialize(e)

    assert isinstance(deserialized, TypeError)
    assert str(deserialized) == 'test'
    assert deserialized.__notes__ == ['when serializing a result']
    assert deserialized.request_context == {'request_id': 'request-1'}


def test_execution_control_errors_are_picklable():
    """Retry and pause exceptions preserve their constructor state."""
    retryable = exceptions.ExecutionRetryableError('retry', 'later', 3)
    paused = exceptions.ExecutionPausedError('pause', 'waiting', 5,
                                             {'signal': 'ready'})
    proof_paused = exceptions.ReservedFillProviderProofPausedError(
        'proof pause', 'waiting for renewal', 3)

    restored_retryable = pickle.loads(pickle.dumps(retryable))
    restored_paused = pickle.loads(pickle.dumps(paused))
    restored_proof_paused = pickle.loads(pickle.dumps(proof_paused))

    assert isinstance(restored_retryable, exceptions.ExecutionRetryableError)
    assert restored_retryable.hint == 'later'
    assert restored_retryable.retry_wait_seconds == 3
    assert isinstance(restored_paused, exceptions.ExecutionPausedError)
    assert restored_paused.hint == 'waiting'
    assert restored_paused.retry_wait_seconds == 5
    assert restored_paused.continue_condition == {'signal': 'ready'}
    assert isinstance(restored_proof_paused,
                      exceptions.ReservedFillProviderProofPausedError)
    assert restored_proof_paused.retry_wait_seconds == 3


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

    # A future server type must not reflect its identity or payload.
    e = exceptions.deserialize_exception({
        'type': 'NonExistent',
        'message': 'credential=secret'
    })
    assert isinstance(e, RuntimeError)
    assert str(e) == 'Server error response is malformed.'
    assert 'NonExistent' not in str(e)
    assert 'credential=secret' not in str(e)

    # Unknown types without a message use the identical value-free result.
    e = exceptions.deserialize_exception({'type': 'NonExistent'})
    assert isinstance(e, RuntimeError)
    assert str(e) == 'Server error response is malformed.'


@pytest.mark.parametrize('bad_input', [
    {
        'type': 1,
    },
    {
        'type': 'ValueError',
        'attributes': None,
    },
    {
        'type': 'ValueError',
        'attributes': [('field', 'value')],
    },
    {
        'type': 'ValueError',
        'attributes': {
            1: 'value'
        },
    },
    {
        'type': 'ValueError',
        'args': None,
    },
    {
        'type': 'ValueError',
        'message': None,
    },
    {
        'type': 'int',
    },
])
def test_deserialize_malformed_envelope_never_raises(bad_input):
    restored = exceptions.deserialize_exception(bad_input)

    assert isinstance(restored, RuntimeError)
    assert str(restored) == 'Server error response is malformed.'


def test_exception_attribute_cannot_replace_canonical_args():
    restored = exceptions.deserialize_exception({
        'type': 'ValueError',
        'message': 'safe',
        'args': ('safe',),
        'attributes': {
            'args': ('credential=secret',),
        },
    })

    assert type(restored) is RuntimeError
    assert str(restored) == 'Server error response is malformed.'
    assert 'credential=secret' not in str(restored)


def test_exception_attribute_cannot_shadow_add_note():
    restored = exceptions.deserialize_exception({
        'type': 'ValueError',
        'message': 'safe',
        'args': ('safe',),
        'attributes': {
            'add_note': 'credential=secret',
        },
        'notes': ['validated note'],
    })

    assert type(restored) is ValueError
    assert str(restored) == 'safe'
    assert restored.__notes__ == ['validated note']
    assert callable(restored.add_note)


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


def test_unregistered_skypilot_exception_uses_safe_wrapper():
    """Module origin alone does not make an exception wire-decodable."""

    class ServerLocalError(RuntimeError):
        pass

    ServerLocalError.__module__ = 'sky.serve.synthetic'
    error = ServerLocalError('server-local failure')

    assert not exceptions.is_safe_exception(error)
    serialized = exceptions.serialize_exception(error)
    restored = exceptions.deserialize_exception(serialized)

    assert serialized['type'] == 'CloudError'
    assert type(restored) is exceptions.CloudError
    assert restored.cloud_provider == 'sky'
    assert restored.error_type == 'ServerLocalError'
    assert restored.args == ('server-local failure',)


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


def test_raised_exception_context_round_trips_and_reserializes_exactly():
    original = None
    try:
        raise ValueError('inner failure')
    except ValueError:
        try:
            # Implicit chaining is the behavior under test.
            # pylint: disable-next=raise-missing-from
            raise exceptions.ResourcesUnavailableError('outer failure',
                                                       no_failover=True)
        except exceptions.ResourcesUnavailableError as error:
            original = error

    assert original is not None
    serialized = exceptions.serialize_exception(original)
    restored = exceptions.deserialize_exception(serialized)

    assert isinstance(restored, exceptions.ResourcesUnavailableError)
    assert restored.no_failover is True
    assert isinstance(restored.__context__, ValueError)
    assert str(restored.__context__) == 'inner failure'
    assert exceptions.serialize_exception(restored) == serialized


def test_unsafe_nested_exception_context_uses_safe_wire_type():

    class MockBotoError(Exception):
        pass

    MockBotoError.__module__ = 'botocore.exceptions'
    outer = RuntimeError('outer')
    outer.__context__ = MockBotoError('provider failure')

    serialized = exceptions.serialize_exception(outer)
    restored = exceptions.deserialize_exception(serialized)

    assert serialized['context']['type'] == 'CloudError'
    assert isinstance(restored.__context__, exceptions.CloudError)
    assert restored.__context__.cloud_provider == 'botocore'
    assert restored.__context__.error_type == 'MockBotoError'
    assert exceptions.serialize_exception(restored) == serialized


def test_exception_context_cycle_is_replaced_by_fixed_error():
    outer = ValueError('outer')
    inner = TypeError('inner')
    outer.__context__ = inner
    inner.__context__ = outer

    serialized = exceptions.serialize_exception(outer)
    cycle_tail = serialized['context']['context']
    restored = exceptions.deserialize_exception(serialized)

    assert cycle_tail == exceptions._sanitized_exception_envelope()  # pylint: disable=protected-access
    assert isinstance(restored.__context__, TypeError)
    assert isinstance(restored.__context__.__context__, RuntimeError)
    assert str(restored.__context__.__context__) == (
        'Server error response is malformed.')
    assert exceptions.serialize_exception(restored) == serialized


def test_exception_context_depth_is_bounded_to_eight_levels():
    errors = [ValueError(f'level-{index}') for index in range(10)]
    for outer, inner in zip(errors, errors[1:]):
        outer.__context__ = inner

    serialized = exceptions.serialize_exception(errors[0])
    current = serialized
    for index in range(8):
        assert current['type'] == 'ValueError'
        assert current['message'] == f'level-{index}'
        current = current['context']
    assert current == exceptions._sanitized_exception_envelope()  # pylint: disable=protected-access


def test_malformed_nested_context_is_sanitized_without_losing_outer_error():
    restored = exceptions.deserialize_exception({
        'type': 'ValueError',
        'message': 'outer',
        'args': ('outer',),
        'attributes': {},
        'context': ['not', 'an', 'envelope'],
    })

    assert isinstance(restored, ValueError)
    assert isinstance(restored.__context__, RuntimeError)
    assert str(restored.__context__) == 'Server error response is malformed.'


def test_deserialize_tolerates_attribute_the_constructor_rejects():
    """A forward-version attribute must not mask the known error type."""
    deserialized = exceptions.deserialize_exception({
        'type': 'ResourcesUnavailableError',
        'message': 'boom',
        'args': ('boom',),
        'attributes': {
            'not_a_constructor_argument': 1
        },
    })
    assert isinstance(deserialized, exceptions.ResourcesUnavailableError)
    assert str(deserialized) == 'boom'
    assert deserialized.not_a_constructor_argument == 1


def test_known_exception_missing_constructor_state_is_sanitized():
    """Canonical-state bypass is limited to declared transformed classes."""
    deserialized = exceptions.deserialize_exception({
        'type': 'KubernetesValidationError',
        'message': 'bad value',
        'args': ('bad value',),
        'attributes': {},
    })

    assert type(deserialized) is RuntimeError
    assert str(deserialized) == 'Server error response is malformed.'


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
            '__traceback__': 'not-a-traceback',
        },
    }

    deserialized = exceptions.deserialize_exception(serialized)

    assert isinstance(deserialized, ValueError)
    assert str(deserialized) == 'boom'
    assert getattr(deserialized, 'context') == 'while encoding'


def test_all_current_skypilot_exceptions_round_trip_exactly():
    """Every current exception class must declare a restorable wire shape."""
    factories = {
        exceptions.CloudError: lambda: exceptions.CloudError(
            'failure', 'aws', 'MockError'),
        exceptions.ResourcesUnavailableError:
            lambda: exceptions.ResourcesUnavailableError(
                'unavailable',
                no_failover=True,
                failover_history=[ValueError('capacity')]),
        exceptions.KubeAPIUnreachableError:
            lambda: exceptions.KubeAPIUnreachableError(
                'unreachable', failover_history=[ValueError('network')]),
        exceptions.KubernetesValidationError:
            lambda: exceptions.KubernetesValidationError(['spec', 'image'],
                                                         'bad value'),
        exceptions.KubernetesPhysicalClusterFenceBusyError:
            lambda: exceptions.KubernetesPhysicalClusterFenceBusyError(
                'identity capture is busy', 'context-a', 3),
        exceptions.ProvisionPrechecksError:
            lambda: exceptions.ProvisionPrechecksError([ValueError('quota')]),
        exceptions.SkyPilotExcludeArgsBaseException:
            exceptions.SkyPilotExcludeArgsBaseException,
        exceptions.CommandFailureException:
            lambda: exceptions.CommandFailureException('sky launch', 'failed',
                                                       'bad command', 'stderr'),
        exceptions.CommandError: lambda: exceptions.CommandError(
            2, 'sky launch', 'bad command', 'stderr'),
        exceptions.ClusterNotUpError: lambda: exceptions.ClusterNotUpError(
            'cluster is down', cluster_status=status_lib.ClusterStatus.UP),
        exceptions.FetchClusterInfoError:
            lambda: exceptions.FetchClusterInfoError(
                exceptions.FetchClusterInfoError.Reason.WORKER),
        exceptions.WorkspaceAmbiguousError:
            lambda: exceptions.WorkspaceAmbiguousError(['beta', 'alpha'],
                                                       'saved choice expired'),
        exceptions.AWSAzFetchingError: lambda: exceptions.AWSAzFetchingError(
            'us-east-1', exceptions.AWSAzFetchingError.Reason.AUTH_FAILURE),
        exceptions.ApiServerConnectionError: lambda: exceptions.
                                             ApiServerConnectionError('api'),
        exceptions.ApiServerAuthenticationError:
            lambda: exceptions.ApiServerAuthenticationError('api'),
        exceptions.ExecutionRetryableError:
            lambda: exceptions.ExecutionRetryableError('retry', 'later', 3),
        exceptions.ExecutionPausedError:
            lambda: exceptions.ExecutionPausedError('paused', 'waiting', 5,
                                                    {'signal': 'ready'}),
        exceptions.ProviderCreateAmbiguousError:
            lambda: exceptions.ProviderCreateAmbiguousError(
                'ambiguous create', 'replaying the idempotent create', 5),
        exceptions.ReservedFillProviderProofPausedError:
            lambda: exceptions.ReservedFillProviderProofPausedError(
                'proof paused', 'waiting for renewal', 3),
        exceptions.ServerTemporarilyUnavailableError:
            lambda: exceptions.ServerTemporarilyUnavailableError('maintenance'),
        exceptions.RequestResultUnavailableError:
            lambda: exceptions.RequestResultUnavailableError(
                'request-1', 'result endpoint unavailable'),
        exceptions.RequestResultApplicationError:
            lambda: exceptions.RequestResultApplicationError(
                'request-1', ValueError('provider rejected request')),
        exceptions.RequestResultShouldRetryError:
            lambda: exceptions.RequestResultShouldRetryError('request-1'),
    }
    exception_classes = {
        value for value in vars(exceptions).values()
        if (inspect.isclass(value) and value.__module__ == exceptions.__name__
            and issubclass(value, Exception))
    }

    for exception_class in exception_classes:
        factory = factories.get(exception_class,
                                lambda cls=exception_class: cls('message'))
        error = factory()
        error.round_trip_context = {'class': exception_class.__name__}
        error.__context__ = ValueError(
            f'context for {exception_class.__name__}')
        error.add_note(f'note for {exception_class.__name__}')
        serialized = exceptions.serialize_exception(error)

        restored = exceptions.deserialize_exception(serialized)

        assert type(restored) is exception_class
        assert restored.args == error.args
        assert str(restored) == str(error)
        assert restored.__dict__ == error.__dict__
        assert isinstance(restored.__context__, ValueError)
        assert str(
            restored.__context__) == (f'context for {exception_class.__name__}')
        assert exceptions.serialize_exception(restored) == serialized
