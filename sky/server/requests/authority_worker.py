"""Fail-closed claim routing for resource-action authority executors.

This module intentionally does not import :mod:`sky.serve`.  Ordinary API and
executor startup may therefore construct their queue views without loading
Serve contracts or requiring the Serve033 tables.  The dedicated authority
role imports the closed Serve value parser only while resolving its mounted
routing identity.
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
from typing import Any

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.server.requests import postgres_schema
from sky.server.requests import registry as request_registry
from sky.server.requests import resource_actions as kernel_actions

ROUTING_CONFIG_PATH_ENV_VAR = (
    'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ROUTING_CONFIG')
ACTIVE_COHORT_ENV_VAR = 'SKYPILOT_RESOURCE_ACTION_AUTHORITY_ACTIVE_COHORT'
SHADOW_ROUTING_PAYLOAD_KEY = '_skypilot_resource_action_authority_v1'

_MAX_CONFIG_BYTES = 65_536
_DNS_LABEL_RE = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
_DIGEST_IMAGE_RE = re.compile(r'^.+@sha256:[0-9a-f]{64}$')
_ROUTING_CONFIG_KEYS = frozenset({
    'version',
    'cohort_id',
    'namespace',
    'deployment_name',
    'service_account_name',
    'container_name',
    'image',
    'claim_contract',
    'handler_allowlist',
})
_REQUIRED_SERVE_TABLES = (
    'serve_resource_action_worker_cohorts',
    'serve_resource_action_worker_cohort_refs',
    'serve_resource_action_shadow_coverage',
)

# Lightweight SQL-only facades keep Serve033 out of the generic request
# metadata.  These are consulted exclusively when an authority claim config is
# supplied to the PostgreSQL queue.
_WORKER_COHORTS = sqlalchemy.table(
    'serve_resource_action_worker_cohorts',
    sqlalchemy.column('cohort_id', sqlalchemy.Text),
    sqlalchemy.column('deployment_uid', sqlalchemy.Text),
    sqlalchemy.column('cohort_identity', postgresql.JSONB),
    sqlalchemy.column('cohort_identity_sha256', sqlalchemy.Text),
    sqlalchemy.column('lifecycle_state', sqlalchemy.Text),
)
_WORKER_COHORT_REFS = sqlalchemy.table(
    'serve_resource_action_worker_cohort_refs',
    sqlalchemy.column('decision_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.column('cohort_id', sqlalchemy.Text),
    sqlalchemy.column('action_type', sqlalchemy.Text),
    sqlalchemy.column('reference_state', sqlalchemy.Text),
)
_SHADOW_COVERAGE = sqlalchemy.table(
    'serve_resource_action_shadow_coverage',
    sqlalchemy.column('decision_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.column('action_type', sqlalchemy.Text),
    sqlalchemy.column('worker_cohort_ref_id', postgresql.UUID(as_uuid=True)),
)


def _closed_text(value: Any, *, name: str, maximum_bytes: int = 253) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    if not 1 <= len(value.encode('utf-8')) <= maximum_bytes:
        raise ValueError(f'{name} must be 1..{maximum_bytes} UTF-8 bytes.')
    return value


def _dns_label(value: Any, *, name: str) -> str:
    parsed = _closed_text(value, name=name, maximum_bytes=63)
    if _DNS_LABEL_RE.fullmatch(parsed) is None:
        raise ValueError(f'{name} must be a canonical DNS label.')
    return parsed


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerRoutingConfig:
    """Immutable chart-rendered inputs knowable before live attestation."""

    cohort_id: str
    namespace: str
    deployment_name: str
    service_account_name: str
    image: str

    @classmethod
    def from_value(cls, value: Any) -> AuthorityWorkerRoutingConfig:
        if type(value) is not dict:
            raise TypeError('Authority routing config must be an object.')
        if frozenset(value) != _ROUTING_CONFIG_KEYS:
            raise ValueError('Authority routing config has an invalid shape.')
        if type(value['version']) is not int or value['version'] != 1:
            raise ValueError('Authority routing config version must be 1.')
        if value['container_name'] != 'skypilot-authority-worker':
            raise ValueError('Authority routing container name is invalid.')
        if value['claim_contract'] != 'frozen_action_cohort_join_v1':
            raise ValueError('Authority routing claim contract is invalid.')
        handlers = value['handler_allowlist']
        if (type(handlers) is not list or tuple(handlers) !=
                request_registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST):
            raise ValueError('Authority routing handler allowlist is invalid.')
        image = _closed_text(value['image'],
                             name='routing.image',
                             maximum_bytes=1024)
        if _DIGEST_IMAGE_RE.fullmatch(image) is None:
            raise ValueError('Authority routing image must be digest-pinned.')
        return cls(
            cohort_id=_dns_label(value['cohort_id'], name='routing.cohort_id'),
            namespace=_closed_text(value['namespace'],
                                   name='routing.namespace'),
            deployment_name=_closed_text(value['deployment_name'],
                                         name='routing.deployment_name'),
            service_account_name=_closed_text(
                value['service_account_name'],
                name='routing.service_account_name'),
            image=image,
        )


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerClaimConfig:
    """Resolved immutable cohort identity used by one queue claimant."""

    routing: AuthorityWorkerRoutingConfig
    active_cohort_id: str
    cohort_identity_bytes: bytes
    cohort_identity_sha256: str
    deployment_uid: str
    lifecycle_state: str

    @property
    def cohort_identity(self) -> dict[str, Any]:
        value = json.loads(self.cohort_identity_bytes)
        if type(value) is not dict:
            raise RuntimeError('Resolved cohort identity lost its object root.')
        return value


def load_routing_config(
        environ: Mapping[str, str] | None = None
) -> AuthorityWorkerRoutingConfig:
    """Load exact immutable routing inputs from the projected file."""
    effective_environ = os.environ if environ is None else environ
    configured_path = effective_environ.get(ROUTING_CONFIG_PATH_ENV_VAR)
    if configured_path is None or not configured_path:
        raise RuntimeError(f'{ROUTING_CONFIG_PATH_ENV_VAR} is required.')
    path = Path(configured_path)
    try:
        raw = path.read_bytes()
    except OSError as e:
        raise RuntimeError(
            f'Cannot read authority routing config {path}.') from e
    if not 1 <= len(raw) <= _MAX_CONFIG_BYTES:
        raise RuntimeError('Authority routing config must be 1..65536 bytes.')
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise RuntimeError(
            'Authority routing config is not canonical JSON.') from e
    canonical = kernel_actions.canonical_json_bytes(value)
    if raw != canonical:
        raise RuntimeError('Authority routing config bytes are not canonical.')
    try:
        return AuthorityWorkerRoutingConfig.from_value(value)
    except (TypeError, ValueError) as e:
        raise RuntimeError(f'Invalid authority routing config: {e}') from e


def _load_active_cohort(environ: Mapping[str, str] | None = None) -> str:
    effective_environ = os.environ if environ is None else environ
    value = effective_environ.get(ACTIVE_COHORT_ENV_VAR)
    if value is None:
        raise RuntimeError(f'{ACTIVE_COHORT_ENV_VAR} is required.')
    try:
        return _dns_label(value, name='active_cohort_id')
    except (TypeError, ValueError) as e:
        raise RuntimeError(f'Invalid authority active cohort: {e}') from e


def require_private_handler_inventory() -> None:
    """Require exactly the four closed private handler registrations."""
    registrations = tuple(
        registration for registration in request_registry.registered_handlers()
        if registration.claim_scope is
        request_registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)
    names = frozenset(registration.name for registration in registrations)
    expected = frozenset(
        request_registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST)
    if names != expected or len(registrations) != len(expected):
        raise RuntimeError('Authority worker handler inventory is incomplete.')
    for registration in registrations:
        if (registration.execution_class
                is not request_registry.ExecutionClass.NORMAL or
                registration.replay_policy
                is not request_registry.ReplayPolicy.NEVER or
                registration.cancellation_policy
                is not request_registry.CancellationPolicy.FENCED_PROCESS or
                registration.aliases):
            raise RuntimeError('Authority worker handler metadata is invalid.')


def _require_serve_schema(connection: sqlalchemy.engine.Connection) -> None:
    for table_name in _REQUIRED_SERVE_TABLES:
        present = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.to_regclass(table_name))).scalar_one()
        if present is None:
            raise RuntimeError(
                f'Authority worker requires Serve033 table {table_name}.')


def _cohort_row(connection: sqlalchemy.engine.Connection,
                cohort_id: str) -> sqlalchemy.engine.RowMapping:
    row = connection.execute(
        sqlalchemy.select(_WORKER_COHORTS).where(
            _WORKER_COHORTS.c.cohort_id == cohort_id)).mappings().first()
    if row is None:
        raise RuntimeError(f'Unknown authority worker cohort {cohort_id!r}.')
    return row


def _parse_cohort_row(row: sqlalchemy.engine.RowMapping) -> tuple[Any, bytes]:
    # Import the exact closed parser only in the dedicated authority startup
    # path; generic/API executors never load this module through claim routing.
    serve_actions = importlib.import_module('sky.serve.resource_actions')
    try:
        cohort = serve_actions.ProviderAuthorityWorkerCohortV1.from_value(
            row['cohort_identity'])
        identity_bytes = cohort.canonical_bytes
        if (cohort.sha256 != row['cohort_identity_sha256'] or
                cohort.deployment_uid != row['deployment_uid'] or
                cohort.cohort_id != row['cohort_id']):
            raise ValueError('cohort row identity columns differ')
    except (KeyError, TypeError, ValueError) as e:
        raise RuntimeError('Authority worker cohort row is invalid.') from e
    return cohort, identity_bytes


def resolve_claim_config(
    connection: sqlalchemy.engine.Connection,
    *,
    environ: Mapping[str, str] | None = None,
) -> AuthorityWorkerClaimConfig:
    """Resolve and validate one claim cohort against Serve033.

    ``connection`` must be from the existing API-request PostgreSQL engine so
    the later claim query can join the Serve033 relations in the same database.
    """
    routing = load_routing_config(environ)
    active_cohort_id = _load_active_cohort(environ)
    _require_serve_schema(connection)
    own_row = _cohort_row(connection, routing.cohort_id)
    active_row = _cohort_row(connection, active_cohort_id)
    cohort, identity_bytes = _parse_cohort_row(own_row)
    active_cohort, _ = _parse_cohort_row(active_row)

    manifest = cohort.manifest
    if (manifest.cohort_id != routing.cohort_id or
            manifest.namespace != routing.namespace or
            manifest.deployment_name != routing.deployment_name or
            manifest.service_account_name != routing.service_account_name or
            manifest.container_name != 'skypilot-authority-worker' or
            manifest.image.requested_reference != routing.image or
            manifest.claim_contract != 'frozen_action_cohort_join_v1' or
            manifest.handler_allowlist
            != request_registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST):
        raise RuntimeError(
            'Mounted authority routing inputs differ from the registered '
            'cohort identity.')
    if (active_cohort.cohort_id != active_cohort_id or
            active_row['lifecycle_state'] != 'ACCEPTING'):
        raise RuntimeError(
            'Configured active authority cohort is not ACCEPTING.')
    lifecycle_state = own_row['lifecycle_state']
    if lifecycle_state not in ('ACCEPTING', 'DRAINING'):
        raise RuntimeError('Authority cohort is not claim-eligible.')
    if lifecycle_state == 'ACCEPTING' and routing.cohort_id != active_cohort_id:
        raise RuntimeError('A non-active ACCEPTING cohort cannot claim work.')

    identity_sha256 = hashlib.sha256(identity_bytes).hexdigest()
    if identity_sha256 != own_row['cohort_identity_sha256']:
        raise RuntimeError('Authority cohort canonical identity hash differs.')
    return AuthorityWorkerClaimConfig(
        routing=routing,
        active_cohort_id=active_cohort_id,
        cohort_identity_bytes=identity_bytes,
        cohort_identity_sha256=identity_sha256,
        deployment_uid=cohort.deployment_uid,
        lifecycle_state=lifecycle_state,
    )


def _json_path(column: sqlalchemy.ColumnElement[Any], *path:
               str) -> sqlalchemy.ColumnElement[Any]:
    return column[tuple(path)]


def claim_predicate(
    config: AuthorityWorkerClaimConfig,
    requests: sqlalchemy.Table = postgres_schema.REQUESTS,
    actions: sqlalchemy.Table = postgres_schema.RESOURCE_ACTIONS,
    attempts: sqlalchemy.Table = postgres_schema.RESOURCE_ACTION_ATTEMPTS,
) -> sqlalchemy.ColumnElement[bool]:
    """Return the exact existing-queue authority eligibility predicate."""
    identity = sqlalchemy.cast(config.cohort_identity, postgresql.JSONB)
    cohorts = _WORKER_COHORTS.alias('authority_claim_cohort')
    refs = _WORKER_COHORT_REFS.alias('authority_claim_ref')
    action_rows = actions.alias('authority_claim_action')
    attempt_rows = attempts.alias('authority_claim_attempt')
    coverage = _SHADOW_COVERAGE.alias('authority_claim_coverage')

    cohort_match = sqlalchemy.and_(
        cohorts.c.cohort_id == config.routing.cohort_id,
        cohorts.c.deployment_uid == config.deployment_uid,
        cohorts.c.cohort_identity == identity,
        cohorts.c.cohort_identity_sha256 == config.cohort_identity_sha256,
        cohorts.c.lifecycle_state.in_(('ACCEPTING', 'DRAINING')),
    )

    launch_cohort = _json_path(action_rows.c.immutable_spec, 'invocation',
                               'launch', 'execution_config', 'capsule',
                               'executor_cohort')
    down_cohort = _json_path(action_rows.c.immutable_spec, 'invocation', 'down',
                             'execution_config', 'capsule', 'executor_cohort')
    action_kind_match = sqlalchemy.or_(
        sqlalchemy.and_(
            requests.c.handler_name == 'serve_resource_action_launch',
            action_rows.c.action_type == 'launch', launch_cohort == identity),
        sqlalchemy.and_(requests.c.handler_name == 'serve_resource_action_down',
                        action_rows.c.action_type == 'down',
                        down_cohort == identity),
    )
    action_match = sqlalchemy.exists(
        sqlalchemy.select(1).select_from(
            action_rows.join(
                attempt_rows,
                sqlalchemy.and_(
                    attempt_rows.c.action_id == action_rows.c.action_id,
                    attempt_rows.c.attempt ==
                    requests.c.resource_action_attempt,
                )).join(refs,
                        refs.c.decision_id == action_rows.c.action_id).join(
                            cohorts, cohorts.c.cohort_id == refs.c.cohort_id)).
        where(
            action_rows.c.action_id == requests.c.resource_action_id,
            action_rows.c.current_attempt == requests.c.resource_action_attempt,
            action_rows.c.kernel_state == 'QUEUED',
            refs.c.cohort_id == config.routing.cohort_id,
            refs.c.action_type == action_rows.c.action_type,
            refs.c.reference_state == 'ACTION_ACTIVE',
            cohort_match,
            action_kind_match,
        ))

    payload = requests.c.payload_json
    shadow_route = _json_path(payload, SHADOW_ROUTING_PAYLOAD_KEY)
    shadow_kind_match = sqlalchemy.or_(
        sqlalchemy.and_(
            requests.c.handler_name == 'serve_shadow_candidate_launch',
            refs.c.action_type == 'launch', coverage.c.action_type == 'launch',
            _json_path(shadow_route, 'action_type').as_string() == 'launch'),
        sqlalchemy.and_(
            requests.c.handler_name == 'serve_shadow_candidate_down',
            refs.c.action_type == 'down', coverage.c.action_type == 'down',
            _json_path(shadow_route, 'action_type').as_string() == 'down'),
    )
    shadow_match = sqlalchemy.exists(
        sqlalchemy.select(1).select_from(
            refs.join(
                coverage,
                sqlalchemy.and_(
                    coverage.c.decision_id == refs.c.decision_id,
                    coverage.c.worker_cohort_ref_id == refs.c.decision_id,
                )).join(cohorts, cohorts.c.cohort_id == refs.c.cohort_id)).
        where(
            refs.c.cohort_id == config.routing.cohort_id,
            refs.c.reference_state == 'SHADOW_ACTIVE',
            cohort_match,
            _json_path(shadow_route, 'version').as_integer() == 1,
            _json_path(shadow_route,
                       'decision_id').as_string() == sqlalchemy.cast(
                           refs.c.decision_id, sqlalchemy.Text),
            _json_path(shadow_route,
                       'cohort_id').as_string() == config.routing.cohort_id,
            _json_path(shadow_route,
                       'deployment_uid').as_string() == config.deployment_uid,
            _json_path(shadow_route, 'executor_cohort') == identity,
            shadow_kind_match,
        ))

    return sqlalchemy.and_(
        requests.c.execution_class ==
        request_registry.ExecutionClass.NORMAL.value,
        requests.c.handler_name.in_(
            request_registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST),
        sqlalchemy.or_(action_match, shadow_match),
    )
