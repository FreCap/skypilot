"""The catalog build must say which Kubernetes contexts it enumerated.

A placement catalog is immutable for the life of a service version, and the
Kubernetes contexts inside it decide which reserved pools that version can
ever fill. A context dropped at build time -- unreachable, or not allowed in
the workspace the build ran under -- cannot be recovered downstream: the
placer never lists it, the broker never claims a pool, and the service simply
never uses the capacity.

Diagnosing that from the outside required reproducing the build by hand and
comparing catalogs across versions. These tests pin the line that makes the
comparison available directly.
"""
# pylint: disable=protected-access
from unittest import mock

from spot_placer_test_utils import make_location

from sky.serve import spot_placer

_EAST = 'prod_research_cluster_eks'
_PHX = 'phx_research_cluster_eks'


def _k8s(context: str, accelerator: str = 'A100'):
    return make_location(context,
                         accelerators={accelerator: 1},
                         use_spot=False,
                         cloud_name='Kubernetes')


def _aws():
    return make_location('us-east-1',
                         accelerators={'L4': 1},
                         use_spot=True,
                         cloud_name='AWS')


def _task(*contexts: str):
    """A task whose resources declare the given Kubernetes contexts."""
    resources = []
    for context in contexts:
        resource = mock.MagicMock()
        resource.cloud = 'Kubernetes'
        resource.region = context
        resources.append(resource)
    task = mock.MagicMock()
    task.resources = resources
    task.num_nodes = 1
    return task


def _build_and_capture(task, locations, workspace):
    """Run from_task with enumeration stubbed; return the logged records."""
    with mock.patch.object(spot_placer,
                           '_get_possible_location_from_task',
                           return_value=locations), \
         mock.patch.object(spot_placer.logger, 'info') as info:
        try:
            spot_placer.PlacementCatalog.from_task(task, workspace=workspace)
        except Exception:  # pylint: disable=broad-except
            # Cost resolution needs a real Resources; the log fires first.
            pass
    return [call.args[0] for call in info.call_args_list]


class TestEnumerationIsReported:

    def test_a_dropped_context_is_visible_in_the_log(self):
        messages = _build_and_capture(_task(_EAST, _PHX), [_k8s(_EAST)],
                                      'mt_hybrid')
        joined = '\n'.join(messages)
        assert 'declared by the task' in joined
        assert _PHX in joined
        assert _EAST in joined
        assert "workspace 'mt_hybrid'" in joined

    def test_a_complete_enumeration_is_reported_too(self):
        # Logged either way: the value is being able to compare the two lists
        # for any version, not only for a version that already went wrong.
        messages = _build_and_capture(_task(_EAST, _PHX),
                                      [_k8s(_EAST), _k8s(_PHX)], 'mt_hybrid')
        joined = '\n'.join(messages)
        assert _EAST in joined and _PHX in joined

    def test_the_workspace_is_named(self):
        messages = _build_and_capture(_task(_EAST), [_k8s(_EAST)],
                                      'rescluster-k8s-prod-east1')
        assert any("rescluster-k8s-prod-east1" in m for m in messages)

    def test_a_task_without_kubernetes_logs_nothing(self):
        # Every non-Kubernetes service would otherwise pay a log line per
        # catalog build for information it cannot act on.
        messages = _build_and_capture(_task(), [_aws()], 'default')
        assert not any('Placement catalog enumerated' in m for m in messages)

    def test_paid_locations_do_not_appear_as_contexts(self):
        messages = _build_and_capture(_task(_EAST),
                                      [_k8s(_EAST), _aws()], 'mt_hybrid')
        joined = '\n'.join(messages)
        assert 'us-east-1' not in joined.split('enumerated:')[-1]
