"""Facade contract for the active-cluster listing gateway."""

import inspect
import typing

from sky import global_user_state


def test_get_clusters_facade_contract() -> None:
    function = global_user_state.get_clusters
    assert function.__module__ == 'sky.global_user_state'
    signature = inspect.signature(function)
    assert tuple(signature.parameters) == (
        'exclude_managed_clusters',
        'workspaces_filter',
        'user_hashes_filter',
        'cluster_names',
        'summary_response',
    )
    for parameter in signature.parameters.values():
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert [parameter.default for parameter in signature.parameters.values()
           ] == [False, None, None, None, False]
    assert typing.get_type_hints(function) == {
        'exclude_managed_clusters': bool,
        'workspaces_filter': set[str] | None,
        'user_hashes_filter': set[str] | None,
        'cluster_names': list[str] | None,
        'summary_response': bool,
        'return': list[dict[str, typing.Any]],
    }
