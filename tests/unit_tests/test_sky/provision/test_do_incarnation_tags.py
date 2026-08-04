"""Tests for DigitalOcean cluster-incarnation tags."""

from __future__ import annotations

import copy
import hashlib
import re
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.provision.do import incarnation_tags
from sky.provision.do import instance as do_instance
from sky.provision.do import utils as do_utils

_CLUSTER_NAME = 'cluster-on-cloud'
_DOMAIN_SEPARATOR = b'skypilot-do-cluster-incarnation-v1\0'
_MARKER_PREFIX = 'skypilot-cluster-incarnation:'


class _StringSubclass(str):
    pass


def _legacy_tags() -> dict[str, str]:
    return {
        'z': 'last',
        'Name': 'user-name',
        'ray-cluster-name': 'user-ray-name',
        'a': 'first',
        'skypilot-cluster-name': 'user-sky-name',
        'skypilot-cluster-incarnation': 'user-marker',
    }


def _expected_legacy_projection() -> list[str]:
    return [
        'Name:user-name',
        'ray-cluster-name:user-ray-name',
        'skypilot-cluster-name:user-sky-name',
        'a:first',
        'skypilot-cluster-incarnation:user-marker',
        'z:last',
    ]


@pytest.mark.parametrize(
    'cluster_incarnation',
    [None, 1, True, b'bytes',
     _StringSubclass('string-subclass')],
    ids=('none', 'integer', 'boolean', 'bytes', 'string-subclass'),
)
def test_non_exact_string_incarnation_preserves_legacy_projection(
        cluster_incarnation: object):
    user_tags = _legacy_tags()
    original_tags = copy.deepcopy(user_tags)

    projected = incarnation_tags.project_instance_tags(
        _CLUSTER_NAME,
        user_tags,
        cluster_incarnation,
    )

    assert projected == _expected_legacy_projection()
    assert user_tags == original_tags


@pytest.mark.parametrize(
    'cluster_incarnation',
    ['', 'ascii-value', 'café', '\ud800', 'x' * 255, 'x' * 256, 'y' * 10_000],
    ids=('empty', 'ascii', 'unicode', 'lone-surrogate', '255-chars',
         '256-chars', 'very-long'),
)
def test_exact_string_incarnation_has_total_domain_separated_encoding(
        cluster_incarnation: str):
    projected = incarnation_tags.project_instance_tags(
        _CLUSTER_NAME,
        {},
        cluster_incarnation,
    )
    expected_digest = hashlib.sha256(
        _DOMAIN_SEPARATOR +
        cluster_incarnation.encode('utf-8', errors='surrogatepass')).hexdigest(
        )
    expected_marker = f'{_MARKER_PREFIX}v1-{expected_digest}'

    assert projected == [
        f'Name:{_CLUSTER_NAME}',
        f'ray-cluster-name:{_CLUSTER_NAME}',
        f'skypilot-cluster-name:{_CLUSTER_NAME}',
        expected_marker,
    ]
    assert re.fullmatch(
        r'skypilot-cluster-incarnation:v1-[0-9a-f]{64}',
        projected[-1],
    )
    assert len(projected[-1]) < 255


def test_legacy_and_distinct_same_name_generations_remain_distinguishable():
    legacy = incarnation_tags.project_instance_tags(_CLUSTER_NAME, {}, None)
    first_generation = incarnation_tags.project_instance_tags(
        _CLUSTER_NAME, {}, 'generation-one')
    second_generation = incarnation_tags.project_instance_tags(
        _CLUSTER_NAME, {}, 'generation-two')

    assert not any(tag.casefold().startswith(_MARKER_PREFIX) for tag in legacy)
    assert first_generation[:-1] == legacy
    assert second_generation[:-1] == legacy
    assert first_generation[-1].startswith(_MARKER_PREFIX)
    assert second_generation[-1].startswith(_MARKER_PREFIX)
    assert first_generation[-1] != second_generation[-1]


def test_marked_projection_replaces_reserved_namespace_without_mutation():
    user_tags = {
        'z': 'last',
        'SKYPILOT-CLUSTER-INCARNATION': 'mixed-case',
        'a': 'first',
        'skypilot-cluster-incarnation:suffixed': 'suffix-value',
        'skypilot-cluster-incarnation-extra': 'unrelated',
        'skypilot-cluster-name': 'user-sky-name',
    }
    original_tags = copy.deepcopy(user_tags)
    expected_digest = hashlib.sha256(_DOMAIN_SEPARATOR +
                                     b'incarnation').hexdigest()

    projected = incarnation_tags.project_instance_tags(
        _CLUSTER_NAME,
        user_tags,
        'incarnation',
    )

    assert projected == [
        f'Name:{_CLUSTER_NAME}',
        f'ray-cluster-name:{_CLUSTER_NAME}',
        'skypilot-cluster-name:user-sky-name',
        'a:first',
        'skypilot-cluster-incarnation-extra:unrelated',
        'z:last',
        f'{_MARKER_PREFIX}v1-{expected_digest}',
    ]
    assert sum(
        tag.casefold().startswith(_MARKER_PREFIX) for tag in projected) == 1
    assert user_tags == original_tags


def _create_config(cluster_incarnation: object = None) -> SimpleNamespace:
    return SimpleNamespace(
        tags={'custom': 'value'},
        cluster_incarnation=cluster_incarnation,
        node_config={
            'InstanceType': 'g-2vcpu-8gb',
            'ImageId': 'image-id',
            'DiskSize': 100,
        },
        authentication_config={'ssh_public_key': 'public-key'},
    )


def _stub_create_dependencies(monkeypatch: pytest.MonkeyPatch,
                              events: list[str]):
    projected_tags = ['projected:tag']

    def project_tags(*args):
        del args
        events.append('project-tags')
        return projected_tags

    def ssh_key_id(public_key):
        assert public_key == 'public-key'
        events.append('ssh-key')
        return {'fingerprint': 'fingerprint'}

    def create_droplet(request):
        events.append('droplet')
        return {'id': 'droplet-id', 'name': request['name']}

    def create_volume(_request):
        events.append('volume')
        return {'id': 'volume-id'}

    def attach_volume(volume_id, request):
        assert volume_id == 'volume-id'
        assert request == {'type': 'attach', 'droplet_id': 'droplet-id'}
        events.append('attach')

    projector = mock.Mock(side_effect=project_tags)
    monkeypatch.setattr(do_utils.incarnation_tags, 'project_instance_tags',
                        projector)
    monkeypatch.setattr(do_utils, 'ssh_key_id', ssh_key_id)
    droplet = mock.Mock(side_effect=create_droplet)
    volume = mock.Mock(side_effect=create_volume)
    monkeypatch.setattr(do_utils, '_create_droplet', droplet)
    monkeypatch.setattr(do_utils, '_create_volume', volume)
    monkeypatch.setattr(
        do_utils,
        'client',
        lambda: SimpleNamespace(volume_actions=SimpleNamespace(post_by_id=
                                                               attach_volume)),
    )
    return projected_tags, projector, droplet, volume


def test_create_instance_projects_once_before_io_and_reuses_exact_tag_list(
        monkeypatch: pytest.MonkeyPatch):
    events: list[str] = []
    projected_tags, projector, droplet, volume = _stub_create_dependencies(
        monkeypatch, events)
    config = _create_config('incarnation')

    do_utils.create_instance('nyc3', _CLUSTER_NAME, 'head', config)

    projector.assert_called_once_with(_CLUSTER_NAME, config.tags, 'incarnation')
    assert projector.call_args.args[1] is config.tags
    assert droplet.call_args.args[0]['tags'] is projected_tags
    assert volume.call_args.args[0]['tags'] is projected_tags
    assert events == ['project-tags', 'ssh-key', 'droplet', 'volume', 'attach']


def test_create_instance_missing_incarnation_attribute_uses_legacy_mode(
        monkeypatch: pytest.MonkeyPatch):
    events: list[str] = []
    _, projector, _, _ = _stub_create_dependencies(monkeypatch, events)
    config = _create_config()
    delattr(config, 'cluster_incarnation')

    do_utils.create_instance('nyc3', _CLUSTER_NAME, 'head', config)

    projector.assert_called_once_with(_CLUSTER_NAME, config.tags, None)


def _run_config(count: int) -> SimpleNamespace:
    return SimpleNamespace(count=count, cluster_incarnation='incarnation')


def test_resume_without_create_does_not_prepare_marker(
        monkeypatch: pytest.MonkeyPatch):
    head = {'name': f'{_CLUSTER_NAME}-abcd-head', 'status': 'active'}
    stopped_worker = {
        'name': f'{_CLUSTER_NAME}-efgh-worker',
        'status': 'off',
    }
    running_worker = dict(stopped_worker, status='active')
    filter_instances = mock.Mock(side_effect=[
        {
            stopped_worker['name']: stopped_worker
        },
        {},
        {
            head['name']: head,
            stopped_worker['name']: stopped_worker,
        },
        {
            stopped_worker['name']: stopped_worker
        },
        {},
        {
            head['name']: head,
            running_worker['name']: running_worker,
        },
    ])
    start_instance = mock.Mock()
    create_instance = mock.Mock()
    marker_projector = mock.Mock()
    monkeypatch.setattr(do_instance.utils, 'filter_instances', filter_instances)
    monkeypatch.setattr(do_instance.utils, 'start_instance', start_instance)
    monkeypatch.setattr(do_instance.utils, 'create_instance', create_instance)
    monkeypatch.setattr(incarnation_tags, 'project_instance_tags',
                        marker_projector)

    result = do_instance.run_instances('nyc3', 'display-name', _CLUSTER_NAME,
                                       _run_config(2))

    start_instance.assert_called_once_with(stopped_worker)
    create_instance.assert_not_called()
    marker_projector.assert_not_called()
    assert not result.created_instance_ids


def test_rename_without_create_does_not_prepare_marker(
        monkeypatch: pytest.MonkeyPatch):
    existing = {'name': f'{_CLUSTER_NAME}-abcd-worker', 'status': 'active'}
    filter_instances = mock.Mock(side_effect=[
        {},
        {},
        {
            existing['name']: existing
        },
        {},
        {},
        {
            existing['name']: existing
        },
    ])
    rename_instance = mock.Mock()
    create_instance = mock.Mock()
    marker_projector = mock.Mock()
    monkeypatch.setattr(do_instance.utils, 'filter_instances', filter_instances)
    monkeypatch.setattr(do_instance.utils, 'rename_instance', rename_instance)
    monkeypatch.setattr(do_instance.utils, 'create_instance', create_instance)
    monkeypatch.setattr(incarnation_tags, 'project_instance_tags',
                        marker_projector)

    result = do_instance.run_instances('nyc3', 'display-name', _CLUSTER_NAME,
                                       _run_config(1))

    rename_instance.assert_called_once()
    assert rename_instance.call_args.args[0] is existing
    create_instance.assert_not_called()
    marker_projector.assert_not_called()
    assert not result.created_instance_ids
