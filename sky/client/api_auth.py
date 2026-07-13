"""Authentication and local credential configuration for the client SDK."""

from http import cookiejar
import json
import os
import typing
from typing import Any
from urllib import parse as urlparse

import click
import colorama
import filelock

from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.client import oauth as oauth_lib
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import versions
from sky.skylet import constants
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:
    import base64
    import binascii
    import pathlib
    import secrets
    import time

    import requests
else:
    base64 = adaptors_common.LazyImport('base64')
    binascii = adaptors_common.LazyImport('binascii')
    pathlib = adaptors_common.LazyImport('pathlib')
    requests = adaptors_common.LazyImport('requests')
    secrets = adaptors_common.LazyImport('secrets')
    time = adaptors_common.LazyImport('time')

# Preserve the logger name used when these public functions lived in sdk.py.
logger = sky_logging.init_logger('sky.client.sdk')


def _save_config_updates(endpoint: str | None = None,
                         service_account_token: str | None = None) -> None:
    """Save endpoint and/or service account token to config file."""
    config_path = pathlib.Path(
        skypilot_config.get_user_config_path()).expanduser()
    with filelock.FileLock(config_path.with_suffix('.lock')):
        if not config_path.exists():
            config_path.touch()
            config: dict[str, Any] = {}
        else:
            config = skypilot_config.get_user_config()
            config = dict(config)

        # Update endpoint if provided
        if endpoint is not None:
            # We should always reset the api_server config to avoid legacy
            # service account token.
            config['api_server'] = {}
            config['api_server']['endpoint'] = endpoint

        # Update service account token if provided
        if service_account_token is not None:
            if 'api_server' not in config:
                config['api_server'] = {}
            config['api_server'][
                'service_account_token'] = service_account_token

        yaml_utils.dump_yaml(str(config_path), config)
        skypilot_config.reload_config()


def _clear_api_server_config() -> None:
    """Clear endpoint and service account token from config file."""
    config_path = pathlib.Path(
        skypilot_config.get_user_config_path()).expanduser()
    with filelock.FileLock(config_path.with_suffix('.lock')):
        if not config_path.exists():
            return

        config = skypilot_config.get_user_config()
        config = dict(config)
        if 'api_server' in config:
            # We might not have set the endpoint in the config file, so we
            # need to check before deleting.
            del config['api_server']

        yaml_utils.dump_yaml(str(config_path), config, blank=True)
        skypilot_config.reload_config()


def _validate_endpoint(endpoint: str | None) -> str:
    """Validate and normalize the endpoint URL."""
    if endpoint is None:
        endpoint = click.prompt('Enter your SkyPilot API server endpoint')
    # Check endpoint is a valid URL
    if (endpoint is not None and not endpoint.startswith('http://') and
            not endpoint.startswith('https://')):
        raise click.BadParameter('Endpoint must be a valid URL.')
    return endpoint.rstrip('/')


def _check_endpoint_in_env_var(is_login: bool) -> None:
    # If the user has set the endpoint via the environment variable, we should
    # not do anything as we can't disambiguate between the env var and the
    # config file.
    """Check if the endpoint is set in the environment variable."""
    if constants.SKY_API_SERVER_URL_ENV_VAR in os.environ:
        with ux_utils.print_exception_no_traceback():
            action = 'login to' if is_login else 'logout of'
            raise RuntimeError(f'Cannot {action} API server when the endpoint '
                               'is set via the environment variable. Run unset '
                               f'{constants.SKY_API_SERVER_URL_ENV_VAR} to '
                               'clear the environment variable.')


def _try_polling_auth(endpoint: str, no_browser: bool = False) -> str | None:
    """Try the polling-based authentication flow."""
    try:
        # Generate code verifier (random secret) and challenge (hash)
        code_verifier = common_utils.base64_url_encode(secrets.token_bytes(32))
        code_challenge = common_utils.compute_code_challenge(code_verifier)

        # Open browser to authorization page. The polling flow does not
        # require the browser to be on this machine, so if --no-browser was
        # passed or we cannot open one locally, just ask the user to visit
        # the URL themselves.
        auth_url = f'{endpoint}/auth/authorize?code_challenge={code_challenge}'
        browser_opened = False
        if not no_browser:
            browser_opened = common_utils.open_browser(auth_url)
            if not browser_opened:
                logger.debug('Failed to open browser.')
        if browser_opened:
            click.echo(f'{colorama.Fore.GREEN}Browser opened at {auth_url}'
                       f'{colorama.Style.RESET_ALL}\n'
                       f'Please click "Authorize" to complete login.\n'
                       f'{colorama.Style.DIM}Press ctrl+c to fall back to '
                       f'legacy auth method.{colorama.Style.RESET_ALL}')
        else:
            click.echo(f'{colorama.Fore.GREEN}Open this URL to complete '
                       f'login:{colorama.Style.RESET_ALL}\n\n'
                       f'{colorama.Style.BRIGHT}{auth_url}'
                       f'{colorama.Style.RESET_ALL}\n\n'
                       f'{colorama.Style.DIM}Press ctrl+c to fall back to '
                       f'legacy auth method.{colorama.Style.RESET_ALL}')

        # Poll for token
        start_time = time.time()
        while time.time(
        ) - start_time < server_constants.AUTH_SESSION_TIMEOUT_SECONDS:
            time.sleep(1)
            resp = requests.get(f'{endpoint}/api/v1/auth/token',
                                params={'code_verifier': code_verifier},
                                timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                if 'token' in data:
                    return data['token']
            elif resp.status_code != 404:
                # 404 means user hasn't clicked Authorize yet, keep polling
                logger.debug(f'Poll failed: {resp.status_code}')
                return None

        click.echo(f'{colorama.Fore.YELLOW}Authentication timed out.'
                   f'{colorama.Style.RESET_ALL}')
        return None

    except KeyboardInterrupt:
        click.echo(f'\n{colorama.Style.DIM}Interrupted.'
                   f'{colorama.Style.RESET_ALL}')
        return None
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Polling auth failed: {e}')
        return None


def _try_localhost_callback_auth(endpoint: str,
                                 no_browser: bool = False) -> str | None:
    """Try the localhost callback authentication flow (legacy)."""
    if no_browser:
        # This flow requires the browser to redirect back to a localhost port
        # on this machine, so it cannot work without a local browser.
        logger.debug('Skipping localhost callback flow: --no-browser is set.')
        return None
    server: oauth_lib.HTTPServer | None = None
    try:
        callback_port = common_utils.find_free_port(8000)
        token_container: dict[str, str | None] = {'token': None}
        server = oauth_lib.start_local_auth_server(callback_port,
                                                   token_container, endpoint)

        token_url = f'{endpoint}/token?local_port={callback_port}'
        if not common_utils.open_browser(token_url):
            return None

        click.echo(f'{colorama.Fore.GREEN}Browser opened at {token_url}'
                   f'{colorama.Style.RESET_ALL}\n'
                   f'{colorama.Style.DIM}Press ctrl+c to enter token manually.'
                   f'{colorama.Style.RESET_ALL}')

        start_time = time.time()
        while (token_container['token'] is None and time.time() - start_time
               < server_constants.AUTH_SESSION_TIMEOUT_SECONDS):
            time.sleep(1)

        if token_container['token'] is None:
            click.echo(f'{colorama.Fore.YELLOW}Authentication timed out.'
                       f'{colorama.Style.RESET_ALL}')
            return None
        return token_container['token']

    except KeyboardInterrupt:
        click.echo(f'\n{colorama.Style.DIM}Interrupted.'
                   f'{colorama.Style.RESET_ALL}')
        return None
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Localhost callback failed: {e}')
        return None
    finally:
        if server is not None:
            try:
                server.server_close()
            except Exception:  # pylint: disable=broad-except
                pass


def _try_manual_token_entry(endpoint: str) -> str | None:
    """Fall back to manual token entry."""
    try:
        token_url = f'{endpoint}/token'
        click.echo(
            f'Visit this URL to get the token:\n\n'
            f'{colorama.Style.BRIGHT}{token_url}{colorama.Style.RESET_ALL}\n')
        return click.prompt('Paste the token') or None
    except (KeyboardInterrupt, click.Abort):
        click.echo(
            f'\n{colorama.Style.DIM}Cancelled.{colorama.Style.RESET_ALL}')
        return None
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Manual token entry failed: {e}')
        return None


@usage_lib.entrypoint
@annotations.client_api
def api_login(endpoint: str | None = None,
              relogin: bool = False,
              service_account_token: str | None = None,
              no_browser: bool = False) -> None:
    """Logs into a SkyPilot API server.

    This sets the endpoint globally, i.e., all SkyPilot CLI and SDK calls will
    use this endpoint.

    To temporarily override the endpoint, use the environment variable
    `SKYPILOT_API_SERVER_ENDPOINT` instead.

    Args:
        endpoint: The endpoint of the SkyPilot API server, e.g.,
            http://1.2.3.4:46580 or https://skypilot.mydomain.com.
        relogin: Whether to force relogin with OAuth2 when enabled.
        service_account_token: Service account token for authentication.
        no_browser: If True, do not attempt to open a browser locally; print
            the auth URL and let the user open it themselves. Skips the
            localhost-callback flow, which requires a local browser.

    Returns:
        None
    """
    _check_endpoint_in_env_var(is_login=True)

    # Validate and normalize endpoint
    endpoint = _validate_endpoint(endpoint)

    def _show_logged_in_message(
            endpoint: str, dashboard_url: str, user: dict[str, Any] | None,
            server_status: server_common.ApiServerStatus) -> None:
        """Show the logged in message."""
        if server_status != server_common.ApiServerStatus.HEALTHY:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Cannot log in API server at '
                                 f'{endpoint} (status: {server_status.value})')

        identity_info = f'\n{ux_utils.INDENT_SYMBOL}{colorama.Fore.GREEN}User: '
        if user:
            user_name = user.get('name')
            user_id = user.get('id')
            if user_name and user_id:
                identity_info += f'{user_name} ({user_id})'
            elif user_id:
                identity_info += user_id
        else:
            identity_info = ''
        dashboard_msg = f'Dashboard: {dashboard_url}'
        click.secho(
            f'Logged into SkyPilot API server at: {endpoint}'
            f'{identity_info}'
            f'\n{ux_utils.INDENT_LAST_SYMBOL}{colorama.Fore.GREEN}'
            f'{dashboard_msg}',
            fg='green')

    def _set_user_hash(user_hash: str | None) -> None:
        if user_hash is not None:
            if not common_utils.is_valid_user_hash(user_hash):
                raise ValueError(f'Invalid user hash: {user_hash}')
            common_utils.set_user_hash_locally(user_hash)

    # Handle service account token authentication
    if service_account_token:
        if not service_account_token.startswith('sky_'):
            raise ValueError('Invalid service account token format. '
                             'Token must start with "sky_"')

        # Save both endpoint and token to config in a single operation
        _save_config_updates(endpoint=endpoint,
                             service_account_token=service_account_token)

        # Test the authentication by checking server health
        try:
            server_status, api_server_info = server_common.check_server_healthy(
                endpoint)
            dashboard_url = server_common.get_dashboard_url(endpoint)
            if api_server_info.user is not None:
                _set_user_hash(api_server_info.user.get('id'))
            _show_logged_in_message(endpoint, dashboard_url,
                                    api_server_info.user, server_status)

            return
        except exceptions.ApiServerConnectionError as e:
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError(
                    f'Failed to connect to API server at {endpoint}: {e}'
                ) from e
        except Exception as e:  # pylint: disable=broad-except
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError(
                    f'{colorama.Fore.RED}Service account token authentication '
                    f'failed:{colorama.Style.RESET_ALL} {e}') from None

    # OAuth2/cookie-based authentication flow
    # TODO(zhwu): this SDK sets global endpoint, which may not be the best
    # design as a user may expect this is only effective for the current
    # session. We should consider using env var for specifying endpoint.

    # Save endpoint and clear any residual service account token before the
    # first health check, so it uses cookie-based auth and the server can
    # correctly return NEEDS_AUTH when SSO is required.
    _save_config_updates(endpoint=endpoint)
    server_status, api_server_info = server_common.check_server_healthy(
        endpoint)
    if server_status == server_common.ApiServerStatus.NEEDS_AUTH or relogin:
        # We detected an auth proxy, so go through the auth proxy cookie flow.
        token: str | None = None

        # Try methods in order:
        # 1. New polling-based flow - only on servers >= API v30
        # 2. Old localhost callback flow
        # 3. Manual token entry
        remote_api_version = versions.get_remote_api_version()
        if remote_api_version is not None and remote_api_version >= 30:
            token = _try_polling_auth(endpoint, no_browser=no_browser)

        if token is None:
            # Polling auth not available or failed, try localhost callback
            token = _try_localhost_callback_auth(endpoint,
                                                 no_browser=no_browser)

        if token is None:
            # All automatic methods failed, fall back to manual entry
            token = _try_manual_token_entry(endpoint)

        if not token:
            with ux_utils.print_exception_no_traceback():
                raise ValueError('Authentication failed.')

        # Parse the token.
        # b64decode will ignore invalid characters, but does some length and
        # padding checks.
        try:
            data = base64.b64decode(token)
        except binascii.Error as e:
            raise ValueError(f'Malformed token: {token}') from e
        try:
            json_data = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f'Malformed token data: {data!r}') from e
        if not isinstance(json_data, dict):
            raise ValueError(f'Malformed token JSON: {json_data}')

        if json_data.get('v') == 1:
            user_hash = json_data.get('user')
            cookie_dict = json_data['cookies']
        elif 'v' not in json_data:
            user_hash = None
            cookie_dict = json_data
        else:
            raise ValueError(f'Unsupported token version: {json_data.get("v")}')

        parsed_url = urlparse.urlparse(endpoint)
        cookie_jar = cookiejar.MozillaCookieJar()
        for (name, value) in cookie_dict.items():
            # dict keys in JSON must be strings
            assert isinstance(name, str)
            if not isinstance(value, str):
                raise ValueError('Malformed token - bad key/value: '
                                 f'{name}: {value}')

            # See CookieJar._cookie_from_cookie_tuple
            # oauth2proxy default is Max-Age 604800
            expires = int(time.time()) + 604800
            domain = str(parsed_url.hostname)
            domain_initial_dot = domain.startswith('.')
            secure = parsed_url.scheme == 'https'
            if not domain_initial_dot:
                domain = '.' + domain

            cookie_jar.set_cookie(
                cookiejar.Cookie(
                    version=0,
                    name=name,
                    value=value,
                    port=None,
                    port_specified=False,
                    domain=domain,
                    domain_specified=True,
                    domain_initial_dot=domain_initial_dot,
                    path='',
                    path_specified=False,
                    secure=secure,
                    expires=expires,
                    discard=False,
                    comment=None,
                    comment_url=None,
                    rest=dict(),
                ))

        # Now that the cookies are parsed, save them to the cookie jar.
        server_common.set_api_cookie_jar(cookie_jar)

        # Set the user hash in the local file.
        # If the server already has a token for this user set it to the local
        # file, otherwise use the new user hash.
        if (api_server_info.user is not None and
                api_server_info.user.get('id') is not None):
            _set_user_hash(api_server_info.user.get('id'))
        else:
            _set_user_hash(user_hash)
    else:
        # Check if basic auth is enabled
        if api_server_info.basic_auth_enabled:
            if api_server_info.user is None:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'Basic auth is enabled but no valid user is found')

        # Set the user hash in the local file.
        if api_server_info.user is not None:
            _set_user_hash(api_server_info.user.get('id'))

    dashboard_url = server_common.get_dashboard_url(endpoint)

    # see https://github.com/python/mypy/issues/5107 on why
    # typing is disabled on this line
    server_common.get_api_server_status_response.cache_clear()
    # After successful authentication, check server health again to get user
    # identity
    server_status, final_api_server_info = server_common.check_server_healthy(
        endpoint)
    # Sync local user hash from the authenticated health check response.
    # This is the final source of truth for the user's identity on this
    # server, ensuring the local hash matches regardless of which auth
    # method was used earlier in the flow.
    if (final_api_server_info.user is not None and
            final_api_server_info.user.get('id') is not None):
        _set_user_hash(final_api_server_info.user.get('id'))
    _show_logged_in_message(endpoint, dashboard_url, final_api_server_info.user,
                            server_status)


@usage_lib.entrypoint
@annotations.client_api
def api_logout() -> None:
    """Logout of the API server.

    Clears all cookies and settings stored in ~/.sky/config.yaml"""
    _check_endpoint_in_env_var(is_login=False)

    if server_common.is_api_server_local():
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError('Local api server cannot be logged out. '
                               'Use `sky api stop` instead.')

    # no need to clear cookies if it doesn't exist.
    server_common.set_api_cookie_jar(cookiejar.MozillaCookieJar(),
                                     create_if_not_exists=False)
    _clear_api_server_config()
    logger.info(f'{colorama.Fore.GREEN}Logged out of SkyPilot API server.'
                f'{colorama.Style.RESET_ALL}')
