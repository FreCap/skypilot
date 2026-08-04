"""Commands for managing and inspecting the SkyPilot API server."""
import json
import os

import click
import colorama
import requests as requests_lib

import sky
from sky import models
from sky import skypilot_config
from sky.client import sdk
from sky.client.cli import click_utils
from sky.client.cli import flags
from sky.server import common as server_common
from sky.server.requests import requests
from sky.skylet import constants
from sky.usage import usage_lib
from sky.utils import common_utils
from sky.utils import log_utils
from sky.utils import ux_utils
from sky.utils.cli_utils import status_utils

_NUM_REQUESTS_TO_SHOW = 50
_DEFAULT_REQUEST_FIELDS_TO_SHOW = [
    'request_id', 'name', 'user_id', 'status', 'created_at'
]
_VERBOSE_REQUEST_FIELDS_TO_SHOW = _DEFAULT_REQUEST_FIELDS_TO_SHOW + [
    'cluster_name'
]


def _complete_api_request(ctx: click.Context, param: click.Parameter,
                          incomplete: str) -> list[str]:
    """Handle shell completion for API requests."""
    del ctx, param  # Unused.
    response = server_common.make_authenticated_request(
        'GET',
        f'/api/completion/api_request?incomplete={incomplete}',
        retry=False,
        timeout=2.0,
    )
    try:
        response.raise_for_status()
    except requests_lib.exceptions.HTTPError:
        # Server may be outdated/missing this API. Silently skip.
        return []
    return response.json()


# Public alias used by the command facade while the historical private name
# remains available for compatibility.
complete_api_request = _complete_api_request


@click.group(cls=click_utils.NaturalOrderGroup)
def api():
    """SkyPilot API server commands."""
    pass


@api.command('start', cls=click_utils.DocumentedCodeCommand)
@click.option('--deploy',
              type=bool,
              is_flag=True,
              default=False,
              required=False,
              help=('Deploy the SkyPilot API server. When set to True, '
                    'SkyPilot API server will use all resources on the host '
                    'machine assuming the machine is dedicated to SkyPilot API '
                    'server; host will also be set to 0.0.0.0 to allow remote '
                    'access.'))
@click.option('--host',
              default='127.0.0.1',
              type=click.Choice(server_common.AVAILBLE_LOCAL_API_SERVER_HOSTS),
              required=False,
              help=('The host to deploy the SkyPilot API server. To allow '
                    'remote access, set this to 0.0.0.0'))
@click.option('--foreground',
              is_flag=True,
              default=False,
              required=False,
              help='Run the SkyPilot API server in the foreground and output '
              'its logs to stdout/stderr. Allowing external systems '
              'to manage the process lifecycle and collect logs directly. '
              'This is useful when the API server is managed by systems '
              'like systemd and Kubernetes.')
@click.option('--metrics',
              is_flag=True,
              default=False,
              required=False,
              help='Expose API server metrics.')
@click.option('--metrics-port',
              type=click.IntRange(1, 65535),
              default=None,
              required=False,
              help='Port used by the API server metrics endpoint.')
@click.option('--enable-basic-auth',
              is_flag=True,
              default=False,
              required=False,
              help='Enable basic authentication in the SkyPilot API server.')
@usage_lib.entrypoint
def api_start(deploy: bool, host: str, foreground: bool, metrics: bool,
              metrics_port: int | None, enable_basic_auth: bool):
    """Starts the SkyPilot API server locally."""
    sdk.api_start(deploy=deploy,
                  host=host,
                  foreground=foreground,
                  metrics=metrics,
                  metrics_port=metrics_port,
                  enable_basic_auth=enable_basic_auth)
    api_server_url = server_common.get_server_url(host)
    api_server_info = server_common.get_api_server_status(api_server_url)
    server_common.check_and_print_upgrade_hint(api_server_info, api_server_url)


@api.command('stop', cls=click_utils.DocumentedCodeCommand)
@usage_lib.entrypoint
def api_stop():
    """Stops the SkyPilot API server locally."""
    sdk.api_stop()


@api.command('logs', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.argument('request_id',
                required=False,
                type=str,
                **click_utils.get_shell_complete_args(_complete_api_request))
@click.option('--server-logs',
              is_flag=True,
              default=False,
              required=False,
              help='Stream the server logs.')
@click.option('--log-path',
              '-l',
              required=False,
              type=str,
              help='The path to the log file to stream.')
@click.option('--tail',
              required=False,
              type=int,
              help=('Number of lines to show from the end of the logs. '
                    '(default: None)'))
@click.option('--follow/--no-follow',
              is_flag=True,
              default=True,
              required=False,
              help='Follow the logs.')
@usage_lib.entrypoint
def api_logs(request_id: str | None, server_logs: bool, log_path: str | None,
             tail: int | None, follow: bool):
    """Stream the logs of a request running on SkyPilot API server."""
    if not server_logs and request_id is None and log_path is None:
        # TODO(zhwu): get the latest request ID.
        raise click.BadParameter('Please provide the request ID or log path.')
    if server_logs:
        sdk.api_server_logs(follow=follow, tail=tail)
        return

    if request_id is not None and log_path is not None:
        raise click.BadParameter(
            'Only one of request ID and log path can be provided.')
    # Only wrap request_id when it is provided; otherwise pass None so the
    # server accepts log_path-only streaming.
    req_id = (server_common.RequestId[None](request_id)
              if request_id is not None else None)
    sdk.stream_and_get(req_id, log_path, tail, follow)


@api.command('cancel', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.argument('request_ids',
                required=False,
                type=str,
                nargs=-1,
                **click_utils.get_shell_complete_args(_complete_api_request))
@flags.all_option('Cancel all your requests.')
@flags.all_users_option('Cancel all requests from all users.')
@flags.yes_option()
@usage_lib.entrypoint
# pylint: disable=redefined-builtin
def api_cancel(request_ids: list[str] | None, all: bool, all_users: bool,
               yes: bool):
    """Cancel a request running on SkyPilot API server."""
    if all or all_users:
        if not yes:
            keyword = 'ALL USERS\'' if all_users else 'YOUR'
            user_input = click.prompt(
                f'This will cancel all {keyword} requests.\n'
                f'To proceed, please type {colorama.Style.BRIGHT}'
                f'\'cancel all requests\'{colorama.Style.RESET_ALL}',
                type=str)
            if user_input != 'cancel all requests':
                raise click.Abort()
        request_ids = None
    cancelled_request_ids = sdk.get(
        sdk.api_cancel(request_ids=request_ids, all_users=all_users))
    if not cancelled_request_ids:
        click.secho('No requests need to be cancelled.', fg='green')
    elif len(cancelled_request_ids) == 1:
        click.secho(f'Cancelled 1 request: {cancelled_request_ids[0]}',
                    fg='green')
    else:
        click.secho(f'Cancelled {len(cancelled_request_ids)} requests.',
                    fg='green')


class IntOrNone(click.ParamType):
    """Int or None"""
    name = 'int-or-none'

    def convert(self, value, param, ctx):
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.lower() in ('none', 'all'):
            return None
        try:
            return int(value)
        except ValueError:
            self.fail(f'{value!r} is not a valid integer or "none" or "all"',
                      param, ctx)


INT_OR_NONE = IntOrNone()


@api.command('status', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.argument('request_id_prefixes',
                required=False,
                type=str,
                nargs=-1,
                **click_utils.get_shell_complete_args(_complete_api_request))
@click.option('--all-status',
              '-a',
              is_flag=True,
              default=False,
              required=False,
              help=('Show requests of all statuses, including finished ones '
                    '(SUCCEEDED, FAILED, CANCELLED). By default, only active '
                    'requests (PENDING, RUNNING) are shown.'))
@click.option(
    '--limit',
    '-l',
    default=_NUM_REQUESTS_TO_SHOW,
    type=INT_OR_NONE,
    required=False,
    help=(f'Number of requests to show, default is {_NUM_REQUESTS_TO_SHOW},'
          f' set to "none" or "all" to show all requests.'))
@click.option('--cluster',
              '-c',
              default=None,
              type=str,
              required=False,
              help=('Filter request by cluster name.'))
@flags.verbose_option('Show more details.')
@flags.output_format_option()
@usage_lib.entrypoint
# pylint: disable=redefined-builtin
def api_status(request_id_prefixes: list[str] | None,
               all_status: bool,
               verbose: bool,
               limit: int | None,
               cluster: str | None,
               output_format: str = 'table'):
    """List requests on SkyPilot API server."""
    if not request_id_prefixes:
        request_id_prefixes = None
    fields = _DEFAULT_REQUEST_FIELDS_TO_SHOW
    if verbose:
        fields = _VERBOSE_REQUEST_FIELDS_TO_SHOW
    request_list = sdk.api_status(request_id_prefixes, all_status, limit,
                                  fields, cluster)

    if output_format == flags.OUTPUT_FORMAT_JSON:
        click.echo(
            json.dumps([r.model_dump(mode='json') for r in request_list],
                       indent=2))
        return

    columns = ['ID', 'User', 'Name']
    if verbose:
        columns.append('Cluster')
    columns.extend(['Created', 'Status'])
    table = log_utils.create_table(columns)
    if len(request_list) > 0:
        for request in request_list:
            r_id = request.request_id
            if not verbose:
                r_id = common_utils.truncate_long_string(r_id, 36)
            req_status = requests.RequestStatus(request.status)
            user_display = status_utils.get_user_display_name(
                request.user_name or '-', request.user_id)
            row = [r_id, user_display, request.name]
            if verbose:
                row.append(request.cluster_name or '-')
            row.extend([
                log_utils.readable_time_duration(request.created_at),
                req_status.colored_str()
            ])
            table.add_row(row)
    else:
        # add dummy data for when api server is down.
        dummy_row = ['-'] * 5
        if verbose:
            dummy_row.append('-')
        table.add_row(dummy_row)
    click.echo(table)
    if limit and len(request_list) >= limit:
        click.echo()
        click.echo(
            f'Showing {limit} requests. Use "-l none" or "-l all" to show'
            f' all requests.')


@api.command('login', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.option('--endpoint',
              '-e',
              required=False,
              help='The SkyPilot API server endpoint.')
@click.option('--relogin',
              is_flag=True,
              default=False,
              help='Force relogin with OAuth2 when enabled.')
@click.option(
    '--service-account-token',
    '--token',
    '-t',
    required=False,
    help='Service account token for authentication (starts with ``sky_``).')
@click.option('--no-browser',
              is_flag=True,
              default=False,
              help='Do not attempt to open a browser locally; print the '
              'auth URL and wait for the user to open it manually. Useful '
              'on headless machines (SSH sessions, containers, etc.).')
@usage_lib.entrypoint
def api_login(endpoint: str | None, relogin: bool,
              service_account_token: str | None, no_browser: bool):
    """Logs into a SkyPilot API server.

    If your remote API server has enabled OAuth2 authentication, you can use
    one of the following methods to login:

    1. OAuth2 browser-based authentication (default)

    2. Service account token via ``--token`` flag

    3. Service account token in ``~/.sky/config.yaml``

    Examples:

    .. code-block:: bash

      # OAuth2 browser login
      sky api login -e https://api.example.com
      \b
      # OAuth2 login without opening a browser locally (e.g. over SSH)
      sky api login -e https://api.example.com --no-browser
      \b
      # Service account token login
      sky api login -e https://api.example.com --token sky_abc123...

    """
    sdk.api_login(endpoint,
                  relogin,
                  service_account_token,
                  no_browser=no_browser)


@api.command('logout', cls=click_utils.DocumentedCodeCommand)
def api_logout():
    """Logs out of the api server"""
    sdk.api_logout()


@api.command('info', cls=click_utils.DocumentedCodeCommand)
@flags.output_format_option()
@flags.config_option(expose_value=False)
@usage_lib.entrypoint
def api_info(output_format: str):
    """Shows the SkyPilot API server URL."""
    url = server_common.get_server_url()
    if output_format != flags.OUTPUT_FORMAT_JSON:
        click.echo(f'SkyPilot client version: {sky.__version__}, '
                   f'commit: {sky.__commit__}')
    try:
        api_server_info = sdk.api_info()
    except requests_lib.exceptions.RequestException:
        is_local = server_common.is_api_server_local()
        if is_local:
            click.echo('No SkyPilot API server is connected\n'
                       f'{ux_utils.INDENT_SYMBOL}To connect to an existing API '
                       'server: sky api login\n'
                       f'{ux_utils.INDENT_LAST_SYMBOL}To start a local API '
                       'server: sky api start')
        else:
            click.echo(
                f'Could not connect to SkyPilot API server at {url}\n'
                f'{ux_utils.INDENT_SYMBOL}To re-login to the API server: '
                f'sky api login --relogin -e {url}\n'
                f'{ux_utils.INDENT_LAST_SYMBOL}To logout the server: '
                'sky api logout')
        return

    api_server_user = api_server_info.user
    if api_server_user is not None:
        user = api_server_user
    else:
        user = models.User.get_current_user()

    # JSON output mode
    if output_format == flags.OUTPUT_FORMAT_JSON:
        output_data = {
            'client': {
                'version': sky.__version__,
                'commit': sky.__commit__,
            },
            'server': {
                'url': url,
                'status': api_server_info.status.value,
                'version': api_server_info.version,
                'commit': api_server_info.commit,
                'api_version': api_server_info.api_version,
            },
            'user': user.name,
        }
        click.echo(json.dumps(output_data, indent=2))
        return

    # Default table/text output

    config = skypilot_config.get_user_config()
    config = dict(config)

    # Determine where the endpoint is set.
    if constants.SKY_API_SERVER_URL_ENV_VAR in os.environ:
        location = ('Endpoint set via the environment variable '
                    f'{constants.SKY_API_SERVER_URL_ENV_VAR}')
    elif 'endpoint' in config.get('api_server', {}):
        config_path = skypilot_config.resolve_user_config_path()
        if config_path is None:
            location = 'Endpoint set to default local API server.'
        else:
            location = f'Endpoint set via {config_path}'
    else:
        location = 'Endpoint set to default local API server.'
    click.echo(f'Using SkyPilot API server and dashboard: {url}\n'
               f'{ux_utils.INDENT_SYMBOL}Status: {api_server_info.status}, '
               f'commit: {api_server_info.commit}, '
               f'version: {api_server_info.version}\n'
               f'{ux_utils.INDENT_SYMBOL}User: {user.name} ({user.id})\n'
               f'{ux_utils.INDENT_LAST_SYMBOL}{location}')
    # Show upgrade hint if available
    server_common.check_and_print_upgrade_hint(api_server_info, url)
