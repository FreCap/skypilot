"""CLI for actor-aware operational events."""

from __future__ import annotations

import datetime
import time

import click

from sky import exceptions
from sky.client import sdk
from sky.client.cli import click_utils
from sky.client.cli import flags
from sky.events import api_models
from sky.usage import usage_lib
from sky.utils import log_utils
from sky.utils import resources_utils

_WATCH_INTERVAL_SECONDS = 2


def _parse_boundary(value: str | None,
                    option_name: str) -> datetime.datetime | None:
    if value is None:
        return None
    try:
        seconds = resources_utils.parse_time_seconds(value)
    except ValueError:
        try:
            parsed = datetime.datetime.fromisoformat(
                value.replace('Z', '+00:00'))
        except ValueError as e:
            raise click.BadParameter(
                'use RFC3339 or a relative duration such as 30m or 2h',
                param_hint=option_name) from e
        if parsed.tzinfo is None:
            raise click.BadParameter('RFC3339 values must include a timezone',
                                     param_hint=option_name) from None
        return parsed.astimezone(datetime.timezone.utc)
    return datetime.datetime.now(
        datetime.timezone.utc) - datetime.timedelta(seconds=seconds)


def _target_names(event: api_models.OperationalEvent) -> str:
    return ', '.join(target.name for target in event.targets) or '-'


def _table(items: list[api_models.OperationalEvent],
           *,
           header: bool = True) -> str:
    table = log_utils.create_table([
        'TIME', 'KIND', 'TARGET', 'OUTCOME', 'ACTOR', 'WORKSPACE', 'REQUEST',
        'MESSAGE'
    ])
    for event in items:
        table.add_row([
            event.occurred_at,
            event.kind.value,
            _target_names(event),
            event.outcome.value,
            event.actor.name,
            event.workspace,
            event.request_id,
            event.message,
        ])
    return table.get_string(header=header)


def _print_items(items: list[api_models.OperationalEvent],
                 output_format: str,
                 *,
                 header: bool = True) -> bool:
    if not items:
        return header
    if output_format == 'json':
        for event in items:
            click.echo(event.model_dump_json())
    else:
        click.echo(_table(items, header=header))
    return False


@click.command('events', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.option('--cluster',
              type=str,
              help='Show events whose cluster name matches exactly.')
@click.option('--workspace',
              'workspaces',
              multiple=True,
              help='Filter by workspace. May be repeated.')
@click.option('--kind',
              'kinds',
              multiple=True,
              type=click.Choice([kind.value for kind in api_models.EventKind]),
              help='Filter by event kind. May be repeated.')
@click.option('--outcome',
              'outcomes',
              multiple=True,
              type=click.Choice(
                  [outcome.value for outcome in api_models.EventOutcome]),
              help='Filter by outcome. May be repeated.')
@click.option('--actor',
              'actor_ids',
              multiple=True,
              help='Filter by immutable actor ID. May be repeated.')
@click.option('--actor-type',
              'actor_types',
              multiple=True,
              type=click.Choice([
                  actor_type.value for actor_type in api_models.EventActorType
              ]),
              help='Filter by actor type. May be repeated.')
@click.option('--request-id', help='Filter by exact API request ID.')
@click.option('--since',
              help='RFC3339 lower bound or relative duration, such as 2h.')
@click.option('--until',
              help='RFC3339 upper bound or relative duration, such as 30m.')
@click.option('--limit',
              type=click.IntRange(1, 100),
              default=50,
              show_default=True)
@click.option('--cursor', help='Continue from an opaque server cursor.')
@click.option('--watch',
              is_flag=True,
              help='Poll losslessly for newly committed events.')
@click.option('--format',
              'output_format',
              type=click.Choice(['table', 'json']),
              default='table',
              show_default=True)
@usage_lib.entrypoint
def events(cluster: str | None, workspaces: tuple[str, ...], kinds: tuple[str,
                                                                          ...],
           outcomes: tuple[str, ...], actor_ids: tuple[str, ...],
           actor_types: tuple[str, ...], request_id: str | None,
           since: str | None, until: str | None, limit: int, cursor: str | None,
           watch: bool, output_format: str) -> None:
    """Shows who changed cluster lifecycle state, when, and with what result.

    Events are durable product history backed by the API server's PostgreSQL
    database. ``--watch`` uses signed server cursors and prints each newly
    committed event once.
    """
    if watch and until is not None:
        raise click.UsageError('--watch cannot be combined with --until.')
    since_value = _parse_boundary(since, '--since')
    until_value = _parse_boundary(until, '--until')
    if (since_value is not None and until_value is not None and
            since_value > until_value):
        raise click.UsageError('--since must not be later than --until.')
    try:
        initial_direction = ('newer'
                             if watch and cursor is not None else 'older')
        page = sdk.list_events(cluster=cluster,
                               workspaces=workspaces,
                               kinds=kinds,
                               outcomes=outcomes,
                               actor_ids=actor_ids,
                               actor_types=actor_types,
                               request_id=request_id,
                               since=since_value,
                               until=until_value,
                               direction=initial_direction,
                               limit=limit,
                               cursor=cursor)
        if not watch:
            if output_format == 'json':
                click.echo(page.model_dump_json(indent=2))
            elif page.items:
                click.echo(_table(page.items))
            else:
                click.echo('No operational events.')
            return

        initial_items = page.items
        if initial_direction == 'older':
            initial_items = list(reversed(initial_items))
        header = _print_items(initial_items, output_format)
        if initial_direction == 'newer' and page.has_more:
            assert page.next_cursor is not None
            poll_cursor = page.next_cursor
            should_sleep = False
        else:
            poll_cursor = page.poll_cursor
            should_sleep = True
        while True:
            if should_sleep:
                time.sleep(_WATCH_INTERVAL_SECONDS)
            page = sdk.list_events(cluster=cluster,
                                   workspaces=workspaces,
                                   kinds=kinds,
                                   outcomes=outcomes,
                                   actor_ids=actor_ids,
                                   actor_types=actor_types,
                                   request_id=request_id,
                                   since=since_value,
                                   direction='newer',
                                   limit=limit,
                                   cursor=poll_cursor)
            header = _print_items(page.items, output_format, header=header)
            if page.has_more:
                assert page.next_cursor is not None
                poll_cursor = page.next_cursor
                should_sleep = False
                continue
            poll_cursor = page.poll_cursor
            should_sleep = True
    except KeyboardInterrupt:
        return
    except exceptions.OperationalEventsUnavailableError as e:
        raise click.ClickException(str(e)) from e
    except exceptions.StaleOperationalEventCursorError as e:
        raise click.UsageError(str(e)) from e
