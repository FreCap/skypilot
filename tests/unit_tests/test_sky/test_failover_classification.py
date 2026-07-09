"""Tests for capacity/quota error classification and the failover backoff.

The classifier is the v0b prerequisite: the failover site receives a collapsed
``RuntimeError`` whose concrete cloud error code lives on a chained exception,
so classification must walk the chain and recognize the code.
"""
# pylint: disable=invalid-name,protected-access
import types

import pytest

from sky import clouds
from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend as backend
from sky.provision import capacity_cache
from sky.utils import common_utils


class _FakeClientError(Exception):
    """Mimics botocore's ClientError: a ``.response`` dict + code in the text."""

    def __init__(self, code: str):
        self.response = {'Error': {'Code': code, 'Message': f'{code} occurred'}}
        super().__init__(f'An error occurred ({code}) when calling '
                         'the RunInstances operation')


def test_classify_capacity():
    err = _FakeClientError('InsufficientInstanceCapacity')
    assert backend._classify_capacity_error(err) == 'capacity'


def test_classify_quota():
    err = _FakeClientError('VcpuLimitExceeded')
    assert backend._classify_capacity_error(err) == 'quota'


def test_classify_unrelated_returns_none():
    assert backend._classify_capacity_error(ValueError('boom')) is None
    assert backend._classify_capacity_error(
        RuntimeError('some other failure')) is None


def test_classify_walks_collapsed_chain():
    """AWS collapses the boto error into a generic RuntimeError ``from`` it."""
    cause = _FakeClientError('InsufficientInstanceCapacity')
    try:
        try:
            raise cause
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError(
                'Failed to launch instances. Max attempts exceeded.') from e
    except RuntimeError as collapsed:
        assert backend._classify_capacity_error(collapsed) == 'capacity'


def test_classify_from_text_only():
    """Even without a structured ``.response``, the code in the text matches."""
    err = RuntimeError('... (InsufficientInstanceCapacity) ...')
    assert backend._classify_capacity_error(err) == 'capacity'


def test_classify_structured_code_is_authoritative():
    """A structured, non-capacity boto code must NOT be re-classified from its
    message text -- otherwise an ``InvalidParameterValue`` whose message quotes
    a user value like 'InsufficientInstanceCapacity' would be wrongly blocked.
    Text matching is a fallback only when there is no structured code."""

    class _MisleadingError(Exception):

        def __init__(self):
            self.response = {
                'Error': {
                    'Code': 'InvalidParameterValue',
                    'Message': "tag value 'InsufficientInstanceCapacity'",
                }
            }
            super().__init__("An error occurred (InvalidParameterValue) ... "
                             "InsufficientInstanceCapacity")

    assert backend._classify_capacity_error(_MisleadingError()) is None


def test_classify_walks_past_unrelated_structured_code_to_capacity_cause():
    """A structured-but-unrelated error that is explicitly chained ``from`` a
    real capacity ClientError still classifies as capacity (the chain walk
    continues past the decided-negative link)."""
    cause = _FakeClientError('InsufficientInstanceCapacity')

    class _Unrelated(Exception):

        def __init__(self):
            self.response = {'Error': {'Code': 'InvalidParameterValue'}}
            super().__init__('unrelated')

    try:
        try:
            raise cause
        except Exception as e:  # pylint: disable=broad-except
            raise _Unrelated() from e
    except _Unrelated as collapsed:
        assert backend._classify_capacity_error(collapsed) == 'capacity'


def test_classify_ignores_implicit_context_chain():
    """A capacity error reachable only via the implicit ``__context__`` (an
    unrelated error raised *while* handling it, without ``from``) must NOT be
    classified -- classification gates a fast-fail block, so precision matters:
    a healthy shape must not be blocked because a capacity error happened to be
    in flight. Only the explicit ``__cause__`` chain is followed."""
    capacity = _FakeClientError('InsufficientInstanceCapacity')
    try:
        try:
            raise capacity
        except Exception:  # pylint: disable=broad-except
            # Bare re-raise (no ``from``): capacity lands on __context__ only.
            raise ValueError('unrelated bookkeeping failure')
    except ValueError as unrelated:
        assert unrelated.__context__ is capacity
        assert unrelated.__cause__ is None
        assert backend._classify_capacity_error(unrelated) is None


def _to_provision():
    return resources_lib.Resources(cloud=clouds.AWS(),
                                   region='us-east-1',
                                   zone='us-east-1a',
                                   instance_type='g6.4xlarge',
                                   use_spot=True)


def test_classify_unstructured_outer_does_not_override_structured_cause():
    """The whole-chain precedence rule: an unstructured outer wrapper whose text
    echoes a capacity token must NOT win over a structured inner cause that says
    the failure is something else. Text is consulted only if NO structured code
    exists anywhere in the chain."""
    inner = _FakeClientError(
        'InvalidParameterValue')  # structured, non-capacity
    try:
        try:
            raise inner
        except Exception as e:  # pylint: disable=broad-except
            # Unstructured outer wrapper whose message echoes a capacity token.
            raise RuntimeError(
                'Failed: ... (InsufficientInstanceCapacity) ...') from e
    except RuntimeError as outer:
        assert backend._classify_capacity_error(outer) is None


def test_record_and_backoff_by_reason(monkeypatch):
    """capacity -> cache mark; quota -> region block + backoff; None -> neither.

    The cache mark is a separate method from the backoff so it can run *before*
    teardown; the backoff method also blocks the whole region for a regional
    quota failure.
    """
    sleeps = []
    marks = []
    monkeypatch.setattr(backend.time, 'sleep', lambda s: sleeps.append(s))
    monkeypatch.setattr(capacity_cache, 'mark_exhausted',
                        lambda key, **kw: marks.append(key))
    monkeypatch.setattr(backend, '_capacity_cache_account',
                        lambda cloud: 'acct')

    to_provision = _to_provision()
    region = clouds.Region('us-east-1')
    zones = [clouds.Zone('us-east-1a'), clouds.Zone('us-east-1b')]
    backoff = common_utils.Backoff(initial_backoff=2.0, max_backoff_factor=15)

    # The methods only touch ``self._blocked_resources``; a dummy self exercises
    # exactly the failover-site logic.
    record = backend.RetryingVmProvisioner._record_capacity_exhaustion
    pace = backend.RetryingVmProvisioner._backoff_after_capacity_failure
    dummy = types.SimpleNamespace(_blocked_resources=set())

    # Unclassified error: no mark, no backoff, no region block.
    record(dummy, to_provision, region, zones, 1, None)
    pace(dummy, to_provision, region, None, backoff)
    assert not marks and not sleeps and not dummy._blocked_resources

    # Quota: never cached, but the whole region is blocked in-memory (so the
    # doomed sibling-AZ sweep is skipped) and the probe is paced once.
    record(dummy, to_provision, region, zones, 1, 'quota')
    pace(dummy, to_provision, region, 'quota', backoff)
    assert not marks
    assert len(sleeps) == 1
    assert len(dummy._blocked_resources) == 1
    blocked = next(iter(dummy._blocked_resources))
    assert blocked.region == 'us-east-1' and blocked.zone is None
    # Any AZ of that region is now blocked for this launch.
    assert to_provision.copy(region='us-east-1',
                             zone='us-east-1a').should_be_blocked_by(blocked)

    # Capacity: one cache mark per zone (keyed on num_nodes) + backoff; no new
    # region block (capacity is per-AZ, not regional).
    record(dummy, to_provision, region, zones, 4, 'capacity')
    pace(dummy, to_provision, region, 'capacity', backoff)
    assert len(marks) == len(zones)
    assert all(key.num_nodes == 4 for key in marks)
    assert len(sleeps) == 2
    assert len(dummy._blocked_resources) == 1  # unchanged

    # Existing-cluster launch: backoff still fires, but nothing is cached (an
    # existing cluster's num_nodes is not the actual acquisition size).
    marks.clear()
    record(dummy,
           to_provision,
           region,
           zones,
           4,
           'capacity',
           record_to_cache=False)
    assert not marks


def test_capacity_cache_keys_one_per_zone(monkeypatch):
    monkeypatch.setattr(backend, '_capacity_cache_account',
                        lambda cloud: 'acct-X')
    to_provision = _to_provision()
    region = clouds.Region('us-east-1')
    zones = [clouds.Zone('us-east-1a'), clouds.Zone('us-east-1b')]
    keys = backend._capacity_cache_keys(to_provision, region, zones, 2)
    assert {k.zone for k in keys} == {'us-east-1a', 'us-east-1b'}
    assert all(k.cloud == 'aws' and k.instance_type == 'g6.4xlarge' and
               k.use_spot and k.num_nodes == 2 and k.account == 'acct-X'
               for k in keys)

    # AWS always attempts zoned; a zoneless/empty attempt yields no keys (so an
    # unknown-success-zone clear is a safe no-op).
    assert backend._capacity_cache_keys(to_provision, region, None, 2) == []
    assert backend._capacity_cache_keys(to_provision, region, [], 2) == []


def test_capacity_cache_disabled_for_reservation_launch(monkeypatch):
    """A reservation-eligible (non-spot) launch stays out of the cache so an
    open-capacity miss elsewhere cannot suppress its paid reserved capacity.
    Spot launches (reservations do not serve spot) remain cached."""
    monkeypatch.setattr(backend, '_capacity_cache_account',
                        lambda cloud: 'acct')
    region = clouds.Region('us-east-1')
    zones = [clouds.Zone('us-east-1a')]
    on_demand = resources_lib.Resources(cloud=clouds.AWS(),
                                        region='us-east-1',
                                        zone='us-east-1a',
                                        instance_type='g6.4xlarge',
                                        use_spot=False)

    # A targeted reservation is configured for this cloud/region.
    monkeypatch.setattr(backend.skypilot_config, 'get_effective_region_config',
                        lambda **kw: ['cr-0123'])
    # Non-spot + reservation -> cache disabled (no keys for consult/record/clear).
    assert backend._capacity_cache_keys(on_demand, region, zones, 1) == []
    # Spot ignores reservations -> cache still active.
    assert len(
        backend._capacity_cache_keys(on_demand.copy(use_spot=True), region,
                                     zones, 1)) == 1

    # No reservation configured -> on-demand is cached too.
    monkeypatch.setattr(backend.skypilot_config, 'get_effective_region_config',
                        lambda **kw: [])
    assert len(backend._capacity_cache_keys(on_demand, region, zones, 1)) == 1


def test_capacity_backoff_honors_zero_disable(monkeypatch):
    """initial_seconds=0 disables backoff -> Backoff yields 0-second sleeps,
    matching the schema, which documents 0 as 'disabled'."""
    monkeypatch.setattr(
        backend.skypilot_config, 'get_nested', lambda keys, default: 0
        if keys[-1] == 'initial_seconds' else default)
    b = backend._capacity_backoff()
    assert b.current_backoff() == 0
    assert b.current_backoff() == 0


def test_capacity_cache_scope_is_aws_only():
    """Scope is AWS: only AWS records/consults. Every other cloud short-circuits
    to an empty key list (record/consult/clear no-op), so a mis-classified error
    can never wrongly block it."""
    assert backend._capacity_cache_applies(clouds.AWS()) is True
    assert backend._capacity_cache_applies(clouds.Kubernetes()) is False
    assert backend._capacity_cache_applies(clouds.GCP()) is False
    assert backend._capacity_cache_applies(None) is False

    k8s = resources_lib.Resources(cloud=clouds.Kubernetes(),
                                  instance_type='4CPU--16GB')
    assert backend._capacity_cache_keys(k8s, clouds.Region('kubernetes'), None,
                                        1) == []


def test_consult_returns_exhausted_zone_names(monkeypatch):
    """The failover-site consult reads the cache fresh with the attempt's
    current num_nodes and returns the exhausted zone names to skip."""
    # Mock the account lookup so the test does not depend on ambient AWS
    # credentials (credentialless CI resolves to '' -> no keys -> IndexError).
    monkeypatch.setattr(backend, '_capacity_cache_account',
                        lambda cloud: 'acct')
    to_provision = _to_provision()
    region = clouds.Region('us-east-1')
    zones = [clouds.Zone('us-east-1a'), clouds.Zone('us-east-1b')]
    exhausted = backend._capacity_cache_keys(to_provision, region,
                                             [clouds.Zone('us-east-1a')], 1)[0]
    monkeypatch.setattr(capacity_cache, 'active_exhausted_keys',
                        lambda keys: {exhausted})
    names = backend._capacity_cache_exhausted_zone_names(
        to_provision, region, zones, 1)
    assert names == {'us-east-1a'}


def test_consult_empty_for_non_aws():
    to_provision = resources_lib.Resources(cloud=clouds.Kubernetes(),
                                           instance_type='4CPU--16GB')
    region = clouds.Region('kubernetes')
    assert backend._capacity_cache_exhausted_zone_names(to_provision, region,
                                                        None, 1) == set()
