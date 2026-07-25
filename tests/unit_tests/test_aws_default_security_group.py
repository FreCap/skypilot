"""The default AWS security group name must not embed a hostname.

It used to be f'sky-sg-{user_and_hostname_hash()}'. On a server deployment the
hostname is the pod name, so every API-server restart minted a fresh shared
group, and `cleanup_ports` never deletes a shared default group. Measured in one
region of the fleet account: 123 such groups, 116 referenced by no network
interface, each still allowing ssh :22 from 0.0.0.0/0.
"""
import getpass
import re

from sky.clouds import aws
from sky.utils import common_utils


def test_default_name_has_no_hostname_component():
    """The whole point: restarting a host must not change the name."""
    assert aws.DEFAULT_SECURITY_GROUP_NAME == f'sky-sg-{getpass.getuser()}'
    # The legacy form ended in a 4-hex-char hostname hash.
    assert not re.match(r'^sky-sg-.+-[0-9a-f]{4}$',
                        aws.DEFAULT_SECURITY_GROUP_NAME)


def test_default_name_is_decoupled_from_the_hostname_hash():
    """The name must not contain the hostname-derived hash at all.

    Reloading the module to vary the hostname is not possible (the cloud
    registry rejects re-registration), so assert the decoupling directly: if
    the hash does not appear in the name, the name cannot vary with it.
    """
    hashed = common_utils.user_and_hostname_hash()
    assert hashed not in aws.DEFAULT_SECURITY_GROUP_NAME
    # The hash is <user>-<4 hex>; only its user half may legitimately appear.
    hostname_component = hashed.rsplit('-', 1)[-1]
    assert hostname_component not in aws.DEFAULT_SECURITY_GROUP_NAME


def test_legacy_hostname_derived_names_are_treated_as_shared():
    """A teardown must not start deleting groups other clusters still use."""
    for legacy in ('sky-sg-root-e495', 'sky-sg-root-7508', 'sky-sg-root-41c2',
                   'sky-sg-ubuntu-abcd'):
        assert aws.is_shared_default_security_group(legacy), legacy


def test_current_default_is_treated_as_shared():
    assert aws.is_shared_default_security_group(aws.DEFAULT_SECURITY_GROUP_NAME)


def test_per_cluster_names_are_not_treated_as_shared():
    """These are a single cluster's own group and must remain deletable."""
    for own in ('sky-sg-boltz-l4-fleet-3638', 'sky-sg-my-cluster',
                'sky-sg-saro-boltz25-pubchem-f-c2-80',
                'sky-sg-opendde-10c200s-v4-1234-abcdef12'):
        assert not aws.is_shared_default_security_group(own), own


def test_ambiguous_names_fail_safe():
    """An ambiguous name must be treated as shared, i.e. NOT deleted.

    Erring toward 'shared' leaks one group; erring the other way deletes a
    group other live clusters depend on.
    """
    assert aws.is_shared_default_security_group('sky-sg-run-abcd')
