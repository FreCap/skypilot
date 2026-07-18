"""Characterization tests for the workspace resolution facade."""

import pickle
from unittest import mock

from sky import models
from sky.workspaces import core as workspaces_core


def test_public_result_type_and_facade_identity() -> None:
    """The core facade remains the public serialization/import surface."""
    user = models.User(id='alice', name='alice')
    workspaces = {'team-only': {}}
    with mock.patch.object(workspaces_core,
                           '_load_workspaces',
                           return_value=workspaces), mock.patch.object(
                               workspaces_core,
                               '_accessible_workspace_names_for_user',
                               return_value={'team-only'}):
        result = workspaces_core.resolve_workspace_for_user(user)

    assert type(result) is workspaces_core.WorkspaceResolution
    assert workspaces_core.WorkspaceResolution.__module__ == (
        'sky.workspaces.core')
    assert workspaces_core.resolve_workspace_for_user.__module__ == (
        'sky.workspaces.core')
    assert workspaces_core.set_user_preferred_workspace.__module__ == (
        'sky.workspaces.core')
    restored = pickle.loads(pickle.dumps(result))
    assert type(restored) is workspaces_core.WorkspaceResolution
    assert restored == result
