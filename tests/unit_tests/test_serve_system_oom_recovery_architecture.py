"""Negative architecture guards for SkyServe system-OOM recovery."""

import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# This is the closed production surface that implements or integrates the
# system-OOM capability.  It is intentionally explicit: application-level
# completion signals and an extra listener must not become implicit recovery
# authorities through a shared backend/controller module.
_SCOPED_PRODUCTION_FILES = (
    'sky/skylet/system_oom_recovery.py',
    'sky/skylet/subprocess_supervisor.py',
    'sky/skylet/job_lib.py',
    'sky/backends/system_oom_recovery.py',
    'sky/backends/task_codegen.py',
    'sky/backends/cloud_vm_ray_backend.py',
    'sky/provision/common.py',
    'sky/provision/aws/instance.py',
    'sky/serve/system_oom_recovery.py',
    'sky/serve/system_oom_recovery_observability.py',
    'sky/serve/system_recovery_state.py',
    'sky/serve/system_recovery_route_lease.py',
    'sky/serve/replica_managers.py',
    'sky/serve/load_balancer.py',
    'sky/serve/controller.py',
    'sky/serve/service.py',
)

_STEADY_STATE_PRODUCTION_FILES = _SCOPED_PRODUCTION_FILES + (
    'sky/skylet/log_lib.py',
    'sky/serve/constants.py',
    'sky/serve/replica_info.py',
    'sky/serve/serve_state.py',
    'sky/server/server.py',
)

_REMOVED_TRANSITION_TOKENS = (
    'PROFILE_VERSION_DIRECT_SHELL',
    'CAPABILITY_V1',
    'subreaper-v1+local-docker-empty-inventory-v1',
    'TrustedRecoveryProfile',
    'RequestedRecoveryProfile',
    'resolve_requested_profile',
    'SYSTEM_OOM_RECOVERY_LEGACY_CONTROLLER_CONTRACT_VERSION',
    'SYSTEM_OOM_RECOVERY_PROFILE_VERSION_KEY',
    'authorization_v1_selected',
    'authorization_v2_selected',
    'runtime_capability_v1_observed',
    'status_only_read',
    'rewrite_rollback_replica_system_recovery_state',
    'JobSystemRecoveryDetailStatus.UNSPECIFIED',
)

_FORBIDDEN_AUTHORITIES = (
    ('fixed listener port 4517',
     re.compile(r'(?<![a-z0-9])4517(?![a-z0-9])', re.I)),
    ('AWS queue/event authority',
     re.compile(r'(?<![a-z0-9])(?:sqs|eventbridge)(?![a-z0-9])', re.I)),
    ('Temporal authority',
     re.compile(r'(?<![a-z0-9])temporal(?:io)?(?![a-z0-9])', re.I)),
    ('completion-message authority',
     re.compile(
         r'(?<![a-z0-9])completion[\s_-]*(?:marker|message)'
         r'(?![a-z0-9])', re.I)),
    ('application completion authority',
     re.compile(
         r'(?<![a-z0-9])(?:application|workload)[\s_-]*'
         r'(?:completion|success)(?![a-z0-9])', re.I)),
)


def _find_forbidden_authorities(source: str) -> list[tuple[int, str]]:
    violations = []
    for line_number, line in enumerate(source.splitlines(), start=1):
        for label, pattern in _FORBIDDEN_AUTHORITIES:
            if pattern.search(line) is not None:
                violations.append((line_number, label))
    return violations


def test_system_oom_recovery_has_no_listener_or_application_event_authority(
) -> None:
    violations = []
    for relative_path in _SCOPED_PRODUCTION_FILES:
        source = (_REPO_ROOT / relative_path).read_text(encoding='utf-8')
        violations.extend(
            f'{relative_path}:{line_number}: {label}'
            for line_number, label in _find_forbidden_authorities(source))

    assert not violations, (
        'System-OOM recovery is system-owned: Ray/local job state may drive its '
        'single replay, but port 4517 and application SQS/Temporal/completion '
        'signals must not become recovery authority:\n' + '\n'.join(violations))


def test_negative_architecture_matchers_cover_closed_authorities() -> None:
    examples = (
        'listener_port = 4517',
        "queue = boto3.client('sqs')",
        'from temporalio import workflow',
        'completion_marker = receive_message()',
        'application_success_event = receive_message()',
    )

    for example in examples:
        assert _find_forbidden_authorities(example), example

    # Internal lifecycle bookkeeping is not an application completion
    # contract. Qualify event/queue authority with application/workload in the
    # matcher above so ordinary launch-thread synchronization remains legal.
    assert not _find_forbidden_authorities('launch_completion_queue.put(id)')


def test_transition_compatibility_symbols_are_absent_from_production() -> None:
    violations = []
    for relative_path in _STEADY_STATE_PRODUCTION_FILES:
        source = (_REPO_ROOT / relative_path).read_text(encoding='utf-8')
        violations.extend(
            f'{relative_path}: {token}' for token in _REMOVED_TRANSITION_TOKENS
            if token in source)

    assert not violations, (
        'The steady-state implementation must not retain deprecated v1/v2 '
        'authorization, direct-shell runtime, status-only telemetry, or v13 '
        'rollback compatibility:\n' + '\n'.join(violations))
