"""A reused placement catalog must not silently outlive the task's contexts.

Placement catalogs are built once per service version and reused on later
commits. The enumeration inside one is therefore a snapshot of the locations
that existed when it was built. Adding a Kubernetes context to a service after
that point has no effect: the context never enters the catalog, the placer
never lists it as zero-cost, and the reserved-fill broker never claims a pool
for it.

The failure is silent, which is what makes it expensive. Observed in
production: a spec declaring `phx_research_cluster_eks` alongside
`prod_research_cluster_eks` ran for six consecutive versions whose catalogs
contained only the latter, while the second cluster sat idle. Every surface an
operator would check -- the spec, the workspace config, the cluster itself --
looked correct.
"""
# pylint: disable=protected-access
from unittest import mock

from sky.serve import controller

_EAST = 'prod_research_cluster_eks'
_PHX = 'phx_research_cluster_eks'


def _task_yaml(*contexts: str) -> str:
    # Built line by line on purpose: interpolating an indented block into a
    # dedent()ed literal changes the common prefix and silently produces
    # malformed YAML, which would make every assertion here vacuous.
    lines = ['name: fleet', 'resources:', '  ports: 8080', '  any_of:']
    for context in contexts:
        lines += [
            f'    - infra: k8s/{context}',
            '      accelerators: A100:1',
            '      use_spot: false',
        ]
    lines += ['run: echo hi', '']
    return '\n'.join(lines)


def _catalog(*contexts: str) -> dict:
    return {
        'schema_version': 1,
        'entries': [{
            'location': {
                'cloud': 'Kubernetes',
                'region': context,
                'accelerators': {
                    'A100': 1
                },
            },
            'cost': 0.0,
        } for context in contexts],
    }


class TestMissingContextDetection:

    def test_a_context_absent_from_the_catalog_is_reported(self):
        missing = controller._catalog_missing_task_contexts(
            _task_yaml(_EAST, _PHX), _catalog(_EAST))
        assert missing == {_PHX}

    def test_a_complete_catalog_reports_nothing(self):
        missing = controller._catalog_missing_task_contexts(
            _task_yaml(_EAST, _PHX), _catalog(_EAST, _PHX))
        assert missing == set()

    def test_a_single_context_service_is_unaffected(self):
        missing = controller._catalog_missing_task_contexts(
            _task_yaml(_EAST), _catalog(_EAST))
        assert missing == set()

    def test_a_catalog_with_extra_contexts_is_not_a_mismatch(self):
        # Superset is fine: the task narrowed, which costs nothing.
        missing = controller._catalog_missing_task_contexts(
            _task_yaml(_EAST), _catalog(_EAST, _PHX))
        assert missing == set()

    def test_a_task_without_kubernetes_is_ignored(self):
        yaml = '\n'.join([
            'name: fleet',
            'resources:',
            '  any_of:',
            '    - infra: aws/us-east-1',
            '    - infra: gcp/us-central1',
            'run: echo hi',
            '',
        ])
        assert controller._catalog_missing_task_contexts(
            yaml, _catalog(_EAST)) == set()


class TestDegradesToPreviousBehaviour:
    """Parsing trouble must not fail an update; it must reuse as before."""

    def test_an_unparseable_task_reports_nothing(self):
        assert controller._catalog_missing_task_contexts(
            '{{ not yaml', _catalog(_EAST)) == set()

    def test_an_empty_catalog_with_no_declared_contexts_reports_nothing(self):
        yaml = 'name: fleet\nresources:\n  cpus: 2\nrun: echo hi\n'
        assert controller._catalog_missing_task_contexts(yaml, {}) == set()

    def test_a_malformed_catalog_still_reports_the_declared_context(self):
        # Nothing parseable in the catalog means nothing was enumerated, so
        # the declared context is genuinely absent.
        missing = controller._catalog_missing_task_contexts(
            _task_yaml(_EAST), {'entries': 'not-a-list'})
        assert missing == {_EAST}


class TestProductionShape:
    """The exact arrangement that ran unnoticed for six versions."""

    def test_the_observed_catalog_is_reported_as_incomplete(self):
        # boltz-l4-fleet declares two east shapes plus one PHX shape; the
        # inherited catalog carried only east.
        yaml = '\n'.join([
            'name: boltz-l4-fleet',
            'resources:',
            '  ports: 8080',
            '  any_of:',
            f'    - infra: k8s/{_EAST}',
            '      accelerators: A100-80GB:1',
            '      use_spot: false',
            f'    - infra: k8s/{_EAST}',
            '      accelerators: A100:1',
            '      use_spot: false',
            f'    - infra: k8s/{_PHX}',
            '      accelerators: H200:1',
            '      use_spot: false',
            'run: echo hi',
            '',
        ])
        missing = controller._catalog_missing_task_contexts(
            yaml, _catalog(_EAST))
        assert missing == {_PHX}

    def test_rebuilding_clears_the_mismatch(self):
        yaml = _task_yaml(_EAST, _PHX)
        stale = _catalog(_EAST)
        assert controller._catalog_missing_task_contexts(yaml, stale) == {_PHX}
        rebuilt = _catalog(_EAST, _PHX)
        assert controller._catalog_missing_task_contexts(yaml, rebuilt) == set()


class TestUnparseableTaskDoesNotRaise:

    def test_task_parse_failure_is_swallowed(self):
        with mock.patch.object(controller.task_lib.Task,
                               'from_yaml_str',
                               side_effect=RuntimeError('boom')):
            assert controller._catalog_missing_task_contexts(
                _task_yaml(_EAST), _catalog()) == set()


def _real_shape_entry(context: str, accelerator: str = 'A100-80GB') -> dict:
    """One catalog entry exactly as the controller persists it.

    Captured from the production catalog rather than invented, because the
    detector walks this structure. A fixture that drifts from the real shape
    would let the walker miss contexts that are present, and every commit
    would then rebuild a catalog that was already complete.
    """
    return {
        'location': {
            'zone': None,
            'cloud': 'Kubernetes',
            'region': context,
            'image_id': [{
                'image': 'example.dkr.ecr.us-east-1.amazonaws.com/models:v1',
                'region': 'docker',
            }],
            'use_spot': False,
            'disk_tier': None,
            'accelerators': {
                accelerator: 1
            },
            'instance_type': f'4CPU--16GB--{accelerator}:1',
            'container_image': None,
            'ephemeral_storage': None,
        },
        'hourly_cost': 0.0,
    }


def _real_shape_catalog(*contexts: str) -> dict:
    return {
        'schema_version': 1,
        'num_nodes': 1,
        'entries': [_real_shape_entry(c) for c in contexts],
    }


class TestAgainstTheRealCatalogShape:
    """No false positives on the structure actually persisted in production."""

    def test_a_complete_real_shape_catalog_reports_nothing(self):
        assert controller._catalog_missing_task_contexts(
            _task_yaml(_EAST, _PHX), _real_shape_catalog(_EAST, _PHX)) == set()

    def test_an_incomplete_real_shape_catalog_reports_the_gap(self):
        assert controller._catalog_missing_task_contexts(
            _task_yaml(_EAST, _PHX), _real_shape_catalog(_EAST)) == {_PHX}

    def test_a_nested_image_id_region_is_not_mistaken_for_a_context(self):
        # `image_id` carries its own {'region': 'docker'} entry. Treating that
        # as a location would silently mark real contexts as satisfied.
        catalog = _real_shape_catalog(_EAST)
        blob = str(catalog)
        assert "'region': 'docker'" in blob
        assert controller._catalog_missing_task_contexts(
            _task_yaml(_EAST, _PHX), catalog) == {_PHX}

    def test_unknown_extra_keys_do_not_break_detection(self):
        catalog = _real_shape_catalog(_EAST, _PHX)
        catalog['entries'][0]['some_future_field'] = {'region': 'nonsense'}
        catalog['future_top_level'] = ['anything']
        assert controller._catalog_missing_task_contexts(
            _task_yaml(_EAST, _PHX), catalog) == set()

    def test_a_paid_only_catalog_reports_every_declared_context(self):
        paid = {
            'schema_version': 1,
            'num_nodes': 1,
            'entries': [{
                'location': {
                    'cloud': 'AWS',
                    'region': 'us-east-1',
                    'zone': 'us-east-1a',
                    'accelerators': {
                        'L4': 1
                    },
                },
                'hourly_cost': 0.24,
            }],
        }
        assert controller._catalog_missing_task_contexts(
            _task_yaml(_EAST, _PHX), paid) == {_EAST, _PHX}
