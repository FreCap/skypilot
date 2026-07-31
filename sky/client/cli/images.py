"""Managed container image CLI command family."""

import json
import pathlib

import click

from sky.client.cli import click_utils
from sky.client.cli import flags
from sky.client.cli import table_utils
from sky.container_images import client as container_images_sdk
from sky.container_images import models as container_image_models
from sky.usage import usage_lib

_NaturalOrderGroup = click_utils.NaturalOrderGroup
_DocumentedCodeCommand = click_utils.DocumentedCodeCommand


@click.group(cls=_NaturalOrderGroup)
def image():
    """Manage digest-pinned container image distribution."""
    pass


@image.command('publish', cls=_DocumentedCodeCommand)
@click.argument('image_ref', required=True, type=str, metavar='REF')
@click.option('--release',
              required=True,
              type=str,
              help='Required immutable release name to bind to the digest.')
@click.option('--distribution',
              required=True,
              type=str,
              help='Registry distribution whose canonical target to prepare.')
@click.option('--platform',
              default='linux/amd64',
              show_default=True,
              help='Exact OCI platform child to publish.')
@click.option('--source-auth',
              type=str,
              help='Named server-side source credential binding.')
@click.option('--no-wait', is_flag=True, help='Return after intent commit.')
@click.option('--workspace',
              '-w',
              'workspace_name',
              expose_value=True,
              callback=flags.apply_workspace_option_callback,
              help='Workspace in which to publish the image.')
@usage_lib.entrypoint
def image_publish(image_ref: str, release: str, distribution: str,
                  platform: str, source_auth: str | None, no_wait: bool,
                  workspace_name: str | None) -> None:
    """Publish a digest-pinned REF under an immutable release name.

    Publication records logical content identity and queues only canonical
    preparation. It never fans the image out to every regional target.
    """
    image_spec = container_image_models.ContainerImage(
        ref=image_ref, release=release, distribution=distribution)
    if image_spec.digest is None:
        raise click.UsageError(
            'sky image publish requires a digest-pinned OCI reference. '
            'Resolve and build mutable tags outside the API request path.')
    assert image_spec.ref is not None
    result = container_images_sdk.publish(image_spec.ref,
                                          release,
                                          distribution,
                                          workspace=workspace_name,
                                          platform=platform,
                                          source_auth=source_auth,
                                          wait=not no_wait)
    click.echo(table_utils.format_container_image_mutation(result))


@image.command('status', cls=_DocumentedCodeCommand)
@click.argument('image_ref', required=False, type=str, metavar='IMAGE')
@click.option('--workspace',
              '-w',
              'workspace_name',
              expose_value=True,
              callback=flags.apply_workspace_option_callback,
              help='Workspace whose image catalog to inspect.')
@usage_lib.entrypoint
def image_status(image_ref: str | None, workspace_name: str | None):
    """Show preparation status for an unambiguous IMAGE selector.

    Use ref=..., release=..., or artifact_id=... to select an identity
    namespace explicitly.
    """
    records = container_images_sdk.status(image_ref, workspace=workspace_name)
    click.echo(table_utils.format_container_image_table(records))


@image.command('prepare', cls=_DocumentedCodeCommand)
@click.argument('image_ref', required=True, type=str, metavar='IMAGE')
@click.option('--target',
              required=True,
              type=str,
              help='One qualified registry target name.')
@click.option('--distribution',
              required=True,
              type=str,
              help='Registry distribution to use for this image source.')
@click.option('--no-wait', is_flag=True, help='Return after intent commit.')
@click.option('--workspace',
              '-w',
              'workspace_name',
              expose_value=True,
              callback=flags.apply_workspace_option_callback,
              help='Workspace in which to prepare the image.')
@usage_lib.entrypoint
def image_prepare(image_ref: str, target: str, distribution: str, no_wait: bool,
                  workspace_name: str | None) -> None:
    """Prepare verified copies for an unambiguous IMAGE selector."""
    artifacts = container_images_sdk.status(image_ref, workspace=workspace_name)
    if len(artifacts) != 1:
        raise click.UsageError(
            'IMAGE must resolve to exactly one published artifact.')
    result = container_images_sdk.prepare(artifacts[0].id,
                                          distribution,
                                          target,
                                          workspace=workspace_name,
                                          wait=not no_wait)
    click.echo(table_utils.format_container_image_mutation(result))


@image.command('retry', cls=_DocumentedCodeCommand)
@click.argument('image_ref', required=True, type=str, metavar='IMAGE')
@click.option('--target', required=True, type=str, help='Target name to retry.')
@click.option('--distribution',
              required=True,
              type=str,
              help='Registry distribution containing the target.')
@click.option('--no-wait', is_flag=True, help='Return after retry commit.')
@click.option('--workspace',
              '-w',
              'workspace_name',
              expose_value=True,
              callback=flags.apply_workspace_option_callback,
              help='Workspace containing the image.')
@usage_lib.entrypoint
def image_retry(image_ref: str, target: str, distribution: str, no_wait: bool,
                workspace_name: str | None) -> None:
    """Retry one target for an unambiguous IMAGE selector."""
    selector = container_image_models.parse_explicit_image_selector(image_ref)
    if selector is not None and selector.release is not None:
        page = container_images_sdk.publications(workspace=workspace_name,
                                                 release=selector.release,
                                                 limit=100)
        failed = [item for item in page.items if item.get('state') == 'FAILED']
        if len(failed) == 1:
            result = container_images_sdk.retry_publication(
                failed[0]['id'], workspace=workspace_name, wait=not no_wait)
            click.echo(table_utils.format_container_image_mutation(result))
            return
    artifacts = container_images_sdk.status(image_ref, workspace=workspace_name)
    if len(artifacts) != 1:
        raise click.UsageError(
            'IMAGE must resolve to one failed publication or artifact.')
    page = container_images_sdk.locations(artifacts[0].id,
                                          workspace=workspace_name,
                                          limit=100)
    candidates = [
        item for item in page.items
        if item.get('distribution') == distribution and item.get('target_id') ==
        target and item.get('state') in ('FAILED', 'MISSING', 'EVICTED')
    ]
    if len(candidates) != 1:
        raise click.UsageError(
            'Distribution and target must identify exactly one retryable '
            'location.')
    result = container_images_sdk.retry_location(candidates[0]['id'],
                                                 workspace=workspace_name,
                                                 wait=not no_wait)
    click.echo(table_utils.format_container_image_mutation(result))


@image.group('profile', cls=_NaturalOrderGroup)
def image_profile() -> None:
    """Qualify managed registry profiles."""


@image_profile.command('qualify', cls=_DocumentedCodeCommand)
@click.argument('profile', required=True, type=str)
@click.option('--manifest',
              required=True,
              type=click.Path(exists=True,
                              dir_okay=False,
                              path_type=pathlib.Path),
              help='Secret-free Terraform qualification JSON.')
@usage_lib.entrypoint
def image_profile_qualify(profile: str, manifest: pathlib.Path) -> None:
    """Ingest a bounded Terraform qualification handoff."""
    try:
        payload = json.loads(manifest.read_text())
    except (OSError, ValueError) as error:
        raise click.UsageError(
            'Qualification manifest must be readable JSON.') from error
    if not isinstance(payload, dict):
        raise click.UsageError('Qualification manifest must be a JSON object.')
    result = container_images_sdk.qualify(profile, payload)
    click.echo(table_utils.format_container_image_mutation(result))


@image_profile.command('canary', cls=_DocumentedCodeCommand)
@click.argument('profile', required=True, type=str)
@click.option('--target', required=True, type=str)
@click.option('--backend',
              required=True,
              type=click.Choice(['aws_vm', 'aws_eks']))
@click.option('--runtime-id',
              type=str,
              help='Qualified EC2 region or EKS context. Required when a '
              'target has multiple runtime tuples.')
@click.option('--workspace',
              '-w',
              'workspace_name',
              required=True,
              callback=flags.apply_workspace_option_callback)
@click.option('--no-wait', is_flag=True, help='Return after intent commit.')
@click.option('--yes', '-y', is_flag=True, help='Confirm canary cost.')
@usage_lib.entrypoint
def image_profile_canary(profile: str, target: str, backend: str,
                         runtime_id: str | None, workspace_name: str,
                         no_wait: bool, yes: bool) -> None:
    """Run an actual-principal runtime pull canary."""
    if not yes and not click.confirm(
            'Launch a bounded billable qualification canary?'):
        raise click.Abort()
    result = container_images_sdk.canary(profile,
                                         target,
                                         backend,
                                         workspace=workspace_name,
                                         runtime_id=runtime_id,
                                         wait=not no_wait)
    click.echo(table_utils.format_container_image_mutation(result))
