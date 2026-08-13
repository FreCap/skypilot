"""Controller identities and their stable user-facing metadata."""

from collections.abc import Callable
import dataclasses
import enum
from typing import Any, Optional

import colorama

from sky import skypilot_config
from sky.jobs import constants as managed_job_constants
from sky.utils import common
from sky.utils import common_utils
from sky.utils import controller_constants


@dataclasses.dataclass
class _ControllerSpec:
    """Spec for skypilot controllers."""
    controller_type: str
    name: str
    _cluster_name_func: Callable[[], str]
    _cluster_name_from_server: str | None  # For client-side only
    in_progress_hint: Callable[[bool], str]
    decline_cancel_hint: str
    _decline_down_when_failed_to_fetch_status_hint: str
    decline_down_for_dirty_controller_hint: str
    _check_cluster_name_hint: str
    default_hint_if_non_existent: str
    connection_error_hint: str
    default_resources_config: dict[str, Any]
    default_autostop_config: dict[str, Any]

    @property
    def decline_down_when_failed_to_fetch_status_hint(self) -> str:
        return self._decline_down_when_failed_to_fetch_status_hint.format(
            cluster_name=self.cluster_name)

    @property
    def check_cluster_name_hint(self) -> str:
        return self._check_cluster_name_hint.format(
            cluster_name=self.cluster_name)

    @property
    def cluster_name(self) -> str:
        """The cluster name of the controller.

        On the server-side, the cluster name is the actual cluster name,
        which is read from common.(JOB|SKY_SERVE)_CONTROLLER_NAME.

        On the client-side, the cluster name may not be accurate,
        as we may not know the exact name, because we are missing
        the server-side common.SERVER_ID. We have to wait until
        we get the actual cluster name from the server.
        """
        return (self._cluster_name_from_server if self._cluster_name_from_server
                is not None else self._cluster_name_func())

    def set_cluster_name_from_server(self, cluster_name: str) -> None:
        self._cluster_name_from_server = cluster_name


# TODO: refactor controller class to not be an enum.
class Controllers(enum.Enum):
    """Skypilot controllers."""
    # NOTE(dev): Keep this align with
    # sky/cli.py::_CONTROLLER_TO_HINT_OR_RAISE
    JOBS_CONTROLLER = _ControllerSpec(
        controller_type='jobs',
        name='managed jobs controller',
        _cluster_name_func=lambda: common.JOB_CONTROLLER_NAME,
        _cluster_name_from_server=None,
        in_progress_hint=lambda _:
        ('* {job_info}To see all managed jobs: '
         f'{colorama.Style.BRIGHT}sky jobs queue{colorama.Style.RESET_ALL}'),
        decline_cancel_hint=(
            'Cancelling the jobs controller\'s jobs is not allowed.\nTo cancel '
            f'managed jobs, use: {colorama.Style.BRIGHT}sky jobs cancel '
            f'<managed job IDs> [--all]{colorama.Style.RESET_ALL}'),
        _decline_down_when_failed_to_fetch_status_hint=(
            f'{colorama.Fore.RED}Tearing down the jobs controller while '
            'it is in INIT state is not supported (this means a job launch '
            'is in progress or the previous launch failed), as we cannot '
            'guarantee that all the managed jobs are finished. Please wait '
            'until the jobs controller is UP or fix it with '
            f'{colorama.Style.BRIGHT}sky start '
            '{cluster_name}'
            f'{colorama.Style.RESET_ALL}.'),
        decline_down_for_dirty_controller_hint=(
            f'{colorama.Fore.RED}In-progress managed jobs found. To avoid '
            f'resource leakage, cancel all jobs first: {colorama.Style.BRIGHT}'
            f'sky jobs cancel -a{colorama.Style.RESET_ALL}\n'),
        _check_cluster_name_hint=('Cluster {cluster_name} is reserved for '
                                  'managed jobs controller.'),
        default_hint_if_non_existent='No in-progress managed jobs.',
        connection_error_hint=(
            'Failed to connect to jobs controller, please try again later.'),
        default_resources_config=managed_job_constants.CONTROLLER_RESOURCES,
        default_autostop_config=managed_job_constants.CONTROLLER_AUTOSTOP)
    SKY_SERVE_CONTROLLER = _ControllerSpec(
        controller_type='serve',
        name='serve controller',
        _cluster_name_func=lambda: common.SKY_SERVE_CONTROLLER_NAME,
        _cluster_name_from_server=None,
        in_progress_hint=(
            lambda pool:
            (f'* To see detailed pool status: {colorama.Style.BRIGHT}'
             f'sky jobs pool status -v{colorama.Style.RESET_ALL}') if pool else
            (f'* To see detailed service status: {colorama.Style.BRIGHT}'
             f'sky serve status -v{colorama.Style.RESET_ALL}')),
        decline_cancel_hint=(
            'Cancelling the sky serve controller\'s jobs is not allowed.'),
        _decline_down_when_failed_to_fetch_status_hint=(
            f'{colorama.Fore.RED}Tearing down the sky serve controller '
            'while it is in INIT state is not supported (this means a sky '
            'serve up is in progress or the previous launch failed), as we '
            'cannot guarantee that all the services are terminated. Please '
            'wait until the sky serve controller is UP or fix it with '
            f'{colorama.Style.BRIGHT}sky start '
            '{cluster_name}'
            f'{colorama.Style.RESET_ALL}.'),
        decline_down_for_dirty_controller_hint=(
            f'{colorama.Fore.RED}Tearing down the sky serve controller is not '
            'supported, as it is currently serving the following services: '
            '{service_names}. Please terminate the services first with '
            f'{colorama.Style.BRIGHT}sky serve down -a'
            f'{colorama.Style.RESET_ALL}.'),
        _check_cluster_name_hint=('Cluster {cluster_name} is reserved for '
                                  'sky serve controller.'),
        default_hint_if_non_existent='No live services.',
        connection_error_hint=(
            'Failed to connect to serve controller, please try again later.'),
        default_resources_config=(
            controller_constants.SERVE_CONTROLLER_RESOURCES),
        default_autostop_config=(
            controller_constants.SERVE_CONTROLLER_AUTOSTOP))

    @classmethod
    def from_name(cls,
                  name: str | None,
                  expect_exact_match: bool = True) -> Optional['Controllers']:
        """Check if the cluster name is a controller name.

        Returns:
            The controller if the cluster name is a controller name.
            Otherwise, returns None.
        """
        if name is None:
            return None
        controller = None
        # The controller name is always the same. However, on the client-side,
        # we may not know the exact name, because we are missing the server-side
        # common.SERVER_ID. So, we will assume anything that matches the prefix
        # is a controller.
        prefix = None
        if name.startswith(common.SKY_SERVE_CONTROLLER_PREFIX):
            controller = cls.SKY_SERVE_CONTROLLER
            prefix = common.SKY_SERVE_CONTROLLER_PREFIX
        elif name.startswith(common.JOB_CONTROLLER_PREFIX):
            controller = cls.JOBS_CONTROLLER
            prefix = common.JOB_CONTROLLER_PREFIX

        if controller is not None and expect_exact_match:
            assert name == controller.value.cluster_name, (
                name, controller.value.cluster_name)
        elif controller is not None and name != controller.value.cluster_name:
            # The client-side cluster_name is not accurate. Assume that `name`
            # is the actual cluster name, so need to set the controller's
            # cluster name to the input name.

            # Assert that the cluster name is well-formed. It should be
            # {prefix}{hash}, where prefix is set above, and hash is a valid
            # user hash.
            assert prefix is not None, prefix
            assert name.startswith(prefix), name
            assert common_utils.is_valid_user_hash(name[len(prefix):]), (name,
                                                                         prefix)

            # Update the cluster name.
            controller.value.set_cluster_name_from_server(name)
        return controller

    @classmethod
    def from_type(cls, controller_type: str) -> Optional['Controllers']:
        """Get the controller by controller type.

        Returns:
            The controller if the controller type is valid.
            Otherwise, returns None.
        """
        for controller in cls:
            if controller.value.controller_type == controller_type:
                return controller
        return None


def get_controller_for_pool(pool: bool) -> Controllers:
    """Get the controller type."""
    if pool:
        return Controllers.JOBS_CONTROLLER
    return Controllers.SKY_SERVE_CONTROLLER


def high_availability_specified(cluster_name: str | None) -> bool:
    """Check if the controller high availability is specified in user config.
    """
    controller = Controllers.from_name(cluster_name, expect_exact_match=False)
    if controller is None:
        return False

    if controller.value.controller_type == 'jobs':
        # pylint: disable-next=import-outside-toplevel
        from sky.jobs import utils as managed_job_utils
        if managed_job_utils.is_consolidation_mode():
            return True
    elif controller.value.controller_type == 'serve':
        # pylint: disable-next=import-outside-toplevel
        from sky.serve import serve_utils
        if serve_utils.is_consolidation_mode():
            return True

    if skypilot_config.loaded():
        return skypilot_config.get_nested((controller.value.controller_type,
                                           'controller', 'high_availability'),
                                          False)
    return False
