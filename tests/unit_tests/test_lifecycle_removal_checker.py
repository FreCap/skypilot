"""Tests for the executable provider lifecycle removal manifest checker."""

import hashlib
import importlib.util
import pathlib
import subprocess

import yaml

_CHECKER_PATH = (pathlib.Path(__file__).resolve().parents[2] / 'tools' /
                 'check_lifecycle_removals.py')
_CHECKER_SPEC = importlib.util.spec_from_file_location(
    'check_lifecycle_removals', _CHECKER_PATH)
assert _CHECKER_SPEC is not None and _CHECKER_SPEC.loader is not None
check_lifecycle_removals = importlib.util.module_from_spec(_CHECKER_SPEC)
_CHECKER_SPEC.loader.exec_module(check_lifecycle_removals)

_INTRODUCING_SHA = '1' * 40
_REMOVAL_SHA = '2' * 40
_MERGE_SHA = '3' * 40
_FIRST_PARENT_SHA = '4' * 40


def _pending_gates():
    return {
        'source': [{
            'id': 'source-absence',
            'satisfied': False,
            'evidence': [],
        }],
        'test': [],
        'telemetry': [],
        'release_window': [],
        'schema': [],
    }


def _passed_gates(*, schema=False):
    gates = {
        'source': [{
            'id': 'source-absence',
            'satisfied': True,
            'evidence': ['source-check.txt'],
        }],
        'test': [],
        'telemetry': [],
        'release_window': [],
        'schema': [],
    }
    if schema:
        gates['schema'].append({
            'id': 'postgres-catalog',
            'satisfied': True,
            'evidence': ['catalog-check.txt'],
        })
    return gates


def _artifact(*,
              artifact_id='PLA-M1-001',
              status='present',
              status_history=None,
              obligation='must_remove',
              disposition='delete_symbol',
              locators=None,
              gates=None,
              runtime_affecting=False):
    if status_history is None:
        status_history = ['planned', 'present']
    if locators is None:
        locators = [{
            'kind': 'python_symbol',
            'path': 'sample.py',
            'symbol': 'Legacy',
        }]
    if gates is None:
        gates = _pending_gates()
    evidence = {}
    if status == 'removed':
        evidence = {
            'removal_sha': _REMOVAL_SHA,
            'exact_head_ci': {
                'sha': _REMOVAL_SHA,
                'evidence': ['ci-build.txt'],
            },
            'merge': {
                'sha': _MERGE_SHA,
                'first_parent': _FIRST_PARENT_SHA,
                'second_parent': _REMOVAL_SHA,
                'evidence': ['merge-proof.txt'],
            },
        }
        if runtime_affecting:
            evidence['deployment'] = {
                'sha': _MERGE_SHA,
                'evidence': ['deployment.txt'],
            }
    return {
        'id': artifact_id,
        'milestone': artifact_id.split('-')[1],
        'introduced_by': _INTRODUCING_SHA,
        'obligation': obligation,
        'disposition': disposition,
        'scope': {
            'domain': 'test_domain',
            'store': 'central_postgresql',
            'provider': 'kubernetes',
            'operation': 'test_operation',
        },
        'runtime_affecting': runtime_affecting,
        'locators': locators,
        'replacement': 'ReplacementOwnerV1',
        'dependencies': [],
        'status': status,
        'status_history': status_history,
        'gates': gates,
        'retained_references': [],
        'evidence': evidence,
        'blocker': None,
    }


def _write_manifest(tmp_path, artifacts, coverage_gaps=None):
    if coverage_gaps is None:
        coverage_gaps = []
    manifest_path = tmp_path / 'removals.yaml'
    manifest_path.write_text(yaml.safe_dump(
        {
            'schema_version': 1,
            'coverage_gaps': coverage_gaps,
            'artifacts': artifacts,
        },
        sort_keys=False),
                             encoding='utf-8')
    return manifest_path


def _check(tmp_path, artifacts, phase='current', coverage_gaps=None):
    manifest_path = _write_manifest(tmp_path, artifacts, coverage_gaps)
    return check_lifecycle_removals.check_manifest(manifest_path,
                                                   phase,
                                                   repo_root=tmp_path)


def test_present_semantic_locators_resolve_without_importing(tmp_path):
    (tmp_path / 'sample.py').write_text("""from enum import Enum
from legacy.transport import old_call

LEGACY_TABLE = 'legacy_table'

class Mode(Enum):
    LEGACY = 'legacy'

class Owner:
    old_attribute: int = 1

    def method(self):
        return helper.old_call()

def Legacy():
    return None

def test_legacy_contract():
    return None
""",
                                        encoding='utf-8')
    locators = [
        {
            'kind': 'python_symbol',
            'path': 'sample.py',
            'symbol': 'Legacy',
        },
        {
            'kind': 'python_attribute',
            'path': 'sample.py',
            'symbol': 'Owner',
            'attribute': 'old_attribute',
        },
        {
            'kind': 'python_call_within',
            'path': 'sample.py',
            'symbol': 'Owner.method',
            'call': 'helper.old_call',
        },
        {
            'kind': 'python_enum_member',
            'path': 'sample.py',
            'symbol': 'Mode',
            'member': 'LEGACY',
        },
        {
            'kind': 'python_ast_pattern',
            'path': 'sample.py',
            'pattern': 'helper.old_call()',
            'symbol': 'Owner.method',
        },
        {
            'kind': 'path',
            'path': 'sample.py',
        },
        {
            'kind': 'packaged_path',
            'path': 'sample.py',
        },
        {
            'kind': 'runtime_metadata',
            'path': 'sample.py',
            'name': 'legacy_table',
        },
        {
            'kind': 'runtime_import',
            'path': 'sample.py',
            'module': 'legacy.transport',
            'symbol': 'old_call',
        },
        {
            'kind': 'test_node',
            'path': 'sample.py',
            'node': 'test_legacy_contract',
        },
    ]

    assert _check(tmp_path, [_artifact(locators=locators)]) == []


def test_scoped_ast_pattern_does_not_match_another_owner(tmp_path):
    (tmp_path / 'sample.py').write_text("""def migrated():
    return replacement()

def still_legacy():
    return legacy_call()
""",
                                        encoding='utf-8')
    locator = {
        'kind': 'python_ast_pattern',
        'path': 'sample.py',
        'symbol': 'migrated',
        'pattern': 'legacy_call()',
    }
    removed = _artifact(status='removed',
                        status_history=[
                            'planned', 'present', 'gating', 'ready_to_remove',
                            'removal_in_progress', 'removed'
                        ],
                        disposition='replace_content',
                        locators=[locator],
                        gates=_passed_gates())

    assert _check(tmp_path, [removed], phase='final') == []

    removed['status'] = 'present'
    removed['status_history'] = ['planned', 'present']
    removed['gates'] = _pending_gates()
    removed['evidence'] = {}
    errors = _check(tmp_path, [removed])
    assert any(
        'does not resolve to a present artifact' in error for error in errors)


def test_qualified_call_locator_does_not_match_a_different_owner(tmp_path):
    (tmp_path / 'sample.py').write_text("""def owner():
    return other.old_call()
""",
                                        encoding='utf-8')
    artifact = _artifact(locators=[{
        'kind': 'python_call_within',
        'path': 'sample.py',
        'symbol': 'owner',
        'call': 'helper.old_call',
    }])

    errors = _check(tmp_path, [artifact])

    assert any(
        'does not resolve to a present artifact' in error for error in errors)


def test_gate_mapping_must_not_be_shared_by_yaml_alias(tmp_path):
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    shared_gates = _pending_gates()
    first = _artifact(artifact_id='PLA-M1-001', gates=shared_gates)
    second = _artifact(artifact_id='PLA-M1-002', gates=shared_gates)

    errors = _check(tmp_path, [first, second])

    assert any('gates must be artifact-owned, not a YAML alias shared with '
               'PLA-M1-001' in error for error in errors)


def test_evidence_mapping_must_not_be_shared_by_yaml_alias(tmp_path):
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    shared_evidence = {}
    first = _artifact(artifact_id='PLA-M1-001')
    first['evidence'] = shared_evidence
    second = _artifact(artifact_id='PLA-M1-002')
    second['evidence'] = shared_evidence

    errors = _check(tmp_path, [first, second])

    assert any('evidence must be artifact-owned, not a YAML alias shared with '
               'PLA-M1-001' in error for error in errors)


def test_nested_gate_and_evidence_aliases_must_not_cross_artifacts(tmp_path):
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    shared_source = _pending_gates()['source']
    first_gates = _pending_gates()
    first_gates['source'] = shared_source
    second_gates = _pending_gates()
    second_gates['source'] = shared_source
    first = _artifact(artifact_id='PLA-M1-001', gates=first_gates)
    second = _artifact(artifact_id='PLA-M1-002', gates=second_gates)
    shared_merge = {
        'sha': _MERGE_SHA,
        'first_parent': _FIRST_PARENT_SHA,
        'second_parent': _REMOVAL_SHA,
        'evidence': ['merge-proof.txt'],
    }
    first['evidence'] = {'merge': shared_merge}
    second['evidence'] = {'merge': shared_merge}

    errors = _check(tmp_path, [first, second])

    assert any('gates must be artifact-owned, not a YAML alias shared with '
               'PLA-M1-001' in error for error in errors)
    assert any('evidence must be artifact-owned, not a YAML alias shared with '
               'PLA-M1-001' in error for error in errors)


def test_alias_must_not_cross_gate_and_evidence_trees(tmp_path):
    (tmp_path / 'sample.py').write_text('class Replacement:\n    pass\n',
                                        encoding='utf-8')
    shared_evidence = ['shared-proof.txt']
    first_gates = _pending_gates()
    first_gates['source'][0] = {
        'id': 'source-proof',
        'satisfied': True,
        'evidence': shared_evidence,
    }
    first = _artifact(artifact_id='PLA-M1-001', gates=first_gates)
    second = _artifact(artifact_id='PLA-M1-002',
                       status='removed',
                       status_history=[
                           'planned', 'present', 'gating', 'ready_to_remove',
                           'removal_in_progress', 'removed'
                       ],
                       gates=_passed_gates())
    second['evidence']['exact_head_ci']['evidence'] = shared_evidence

    errors = _check(tmp_path, [first, second])

    assert any('evidence must be artifact-owned, not a YAML alias shared with '
               'PLA-M1-001' in error for error in errors)


def test_replace_content_rejects_surviving_enclosing_symbol(tmp_path):
    (tmp_path / 'sample.py').write_text("""def owner():
    return replacement_call()
""",
                                        encoding='utf-8')
    removed = _artifact(status='removed',
                        status_history=[
                            'planned', 'present', 'gating', 'ready_to_remove',
                            'removal_in_progress', 'removed'
                        ],
                        disposition='replace_content',
                        locators=[{
                            'kind': 'python_symbol',
                            'path': 'sample.py',
                            'symbol': 'owner',
                        }],
                        gates=_passed_gates())

    errors = _check(tmp_path, [removed])

    assert any(
        'replace_content cannot use enclosing or physical locators' in error
        for error in errors)
    assert any('requires an absence-verifiable content locator' in error
               for error in errors)


def test_file_digest_locator_tracks_replaced_non_python_content(tmp_path):
    content_path = tmp_path / 'legacy.template'
    content_path.write_text('legacy body\n', encoding='utf-8')
    locator = {
        'kind': 'file_digest',
        'path': 'legacy.template',
        'sha256': hashlib.sha256(content_path.read_bytes()).hexdigest(),
    }
    artifact = _artifact(disposition='replace_content', locators=[locator])

    assert _check(tmp_path, [artifact]) == []

    content_path.write_text('replacement body\n', encoding='utf-8')
    artifact['status'] = 'removed'
    artifact['status_history'] = [
        'planned', 'present', 'gating', 'ready_to_remove',
        'removal_in_progress', 'removed'
    ]
    artifact['gates'] = _passed_gates()
    artifact['evidence'] = {
        'removal_sha': _REMOVAL_SHA,
        'exact_head_ci': {
            'sha': _REMOVAL_SHA,
            'evidence': ['ci-build.txt'],
        },
        'merge': {
            'sha': _MERGE_SHA,
            'first_parent': _FIRST_PARENT_SHA,
            'second_parent': _REMOVAL_SHA,
            'evidence': ['merge-proof.txt'],
        },
    }

    assert _check(tmp_path, [artifact], phase='final') == []


def test_removed_call_locator_requires_absence_and_exact_evidence(tmp_path):
    (tmp_path / 'sample.py').write_text("""def owner():
    return replacement_call()
""",
                                        encoding='utf-8')
    removed = _artifact(status='removed',
                        status_history=[
                            'planned', 'present', 'gating', 'ready_to_remove',
                            'removal_in_progress', 'removed'
                        ],
                        disposition='replace_content',
                        locators=[{
                            'kind': 'python_call_within',
                            'path': 'sample.py',
                            'symbol': 'owner',
                            'call': 'legacy_call',
                        }],
                        gates=_passed_gates(),
                        runtime_affecting=True)

    assert _check(tmp_path, [removed], phase='final') == []

    removed['evidence'].pop('deployment')
    removed['evidence']['exact_head_ci']['sha'] = '5' * 40
    errors = _check(tmp_path, [removed])
    assert any('requires evidence.deployment' in error for error in errors)
    assert any('exact_head_ci.sha must equal' in error for error in errors)


def test_removed_evidence_links_tested_head_merge_and_deployment(tmp_path):
    (tmp_path / 'sample.py').write_text('class Replacement:\n    pass\n',
                                        encoding='utf-8')
    removed = _artifact(status='removed',
                        status_history=[
                            'planned', 'present', 'gating', 'ready_to_remove',
                            'removal_in_progress', 'removed'
                        ],
                        gates=_passed_gates(),
                        runtime_affecting=True)
    assert _check(tmp_path, [removed], phase='final') == []

    removed['evidence']['merge']['second_parent'] = '6' * 40
    removed['evidence']['deployment']['sha'] = '7' * 40
    errors = _check(tmp_path, [removed])
    assert any('merge.second_parent must equal evidence.removal_sha' in error
               for error in errors)
    assert any('deployment.sha must equal evidence.merge.sha' in error
               for error in errors)


def test_present_artifact_is_incomplete_in_final_phase(tmp_path):
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    artifact = _artifact()

    assert _check(tmp_path, [artifact]) == []
    assert _check(tmp_path, [artifact], phase='final') == [
        'PLA-M1-001: final phase requires status removed'
    ]


def test_exact_coverage_gap_is_visible_and_blocks_final_phase(tmp_path):
    (tmp_path / 'sample.py').write_text(
        'class Legacy:\n    pass\n\ndef mixed_owner():\n    pass\n',
        encoding='utf-8')
    gap = {
        'id': 'PLA-GAP-001',
        'milestone': 'M4',
        'responsibility': 'Split mixed provider orchestration ownership.',
        'candidate_locators': [{
            'kind': 'python_symbol',
            'path': 'sample.py',
            'symbol': 'mixed_owner',
        }],
        'owner': 'SharedClusterPlannerV1',
        'reason': 'The function mixes retained effects with shared policy.',
        'closure_gate': 'Transcribe exact body responsibilities into rows.',
    }
    artifact = _artifact()

    assert _check(tmp_path, [artifact], coverage_gaps=[gap]) == []
    errors = _check(tmp_path, [artifact], phase='final', coverage_gaps=[gap])
    assert any('final phase requires coverage gap closure' in error
               for error in errors)

    gap['candidate_locators'][0]['symbol'] = 'missing_owner'
    errors = _check(tmp_path, [artifact], coverage_gaps=[gap])
    assert any(
        'does not resolve to a present artifact' in error for error in errors)


def test_retained_manifest_ids_must_resolve(tmp_path):
    (tmp_path / 'sample.py').write_text(
        'class Legacy:\n    pass\n\ndef mixed_owner():\n    pass\n',
        encoding='utf-8')
    gap = {
        'id': 'PLA-GAP-001',
        'milestone': 'M4',
        'responsibility': 'Split mixed provider orchestration ownership.',
        'candidate_locators': [{
            'kind': 'python_symbol',
            'path': 'sample.py',
            'symbol': 'mixed_owner',
        }],
        'owner': 'SharedClusterPlannerV1',
        'reason': 'The function mixes retained effects with shared policy.',
        'closure_gate': 'Transcribe exact body responsibilities into rows.',
    }
    retained = _artifact(artifact_id='PLA-M1-001')
    target = _artifact(artifact_id='PLA-M1-002')
    retained['retained_references'] = ['PLA-GAP-001', 'PLA-M1-002']

    assert _check(tmp_path, [retained, target], coverage_gaps=[gap]) == []

    retained['retained_references'] = ['PLA-GAP-999', 'PLA-M1-999']
    errors = _check(tmp_path, [retained, target], coverage_gaps=[gap])
    assert any("unknown retained coverage gap 'PLA-GAP-999'" in error
               for error in errors)
    assert any(
        "unknown retained artifact 'PLA-M1-999'" in error for error in errors)


def test_ready_or_removed_artifact_requires_terminal_dependencies(tmp_path):
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    dependency = _artifact(artifact_id='PLA-M1-001')
    dependent = _artifact(
        artifact_id='PLA-M1-002',
        status='ready_to_remove',
        status_history=['planned', 'present', 'gating', 'ready_to_remove'])
    dependent['dependencies'] = ['PLA-M1-001']

    errors = _check(tmp_path, [dependency, dependent])

    assert any("dependency 'PLA-M1-001' must be removed before ready_to_remove"
               in error for error in errors)

    dependency['obligation'] = 'retain_characterization'
    dependency['disposition'] = 'retain_characterization'
    errors = _check(tmp_path, [dependency, dependent])
    assert any("dependency 'PLA-M1-001' must be retained_verified before "
               'ready_to_remove' in error for error in errors)

    dependency = _artifact(artifact_id='PLA-M1-001')
    dependent = _artifact(artifact_id='PLA-M1-002',
                          status='blocked',
                          status_history=[
                              'planned', 'present', 'gating', 'ready_to_remove',
                              'blocked'
                          ])
    dependent['dependencies'] = ['PLA-M1-001']
    dependent['blocker'] = {
        'blocked_from_status': 'ready_to_remove',
        'owner': 'runtime-team',
        'issue': 'ISSUE-1',
        'evidence': ['blocked-after-readiness.txt'],
    }

    errors = _check(tmp_path, [dependency, dependent])

    assert any("dependency 'PLA-M1-001' must be removed before ready_to_remove"
               in error for error in errors)


def test_closed_values_transitions_and_wildcards_are_rejected(tmp_path):
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    artifact = _artifact(
        status='ready_to_remove',
        status_history=['planned', 'present', 'ready_to_remove'])
    artifact['scope']['provider'] = '*'
    artifact['scope']['store'] = 'Central_PostgreSQL'
    artifact['obligation'] = 'eventually_remove'
    artifact['locators'][0]['path'] = 'sample*.py'

    errors = _check(tmp_path, [artifact])

    assert errors == sorted(errors)
    assert any('invalid obligation' in error for error in errors)
    assert any('invalid status transition present -> ready_to_remove' in error
               for error in errors)
    assert any('scope.provider must be exact' in error for error in errors)
    assert any('scope.store must be a canonical lowercase token' in error
               for error in errors)
    assert any('path must not contain a wildcard' in error for error in errors)


def test_introducing_commit_must_exist_and_be_an_ancestor(tmp_path):
    subprocess.run(['git', 'init', '-q', str(tmp_path)], check=True)
    subprocess.run(
        ['git', '-C', str(tmp_path), 'config', 'user.name', 'Test'], check=True)
    subprocess.run([
        'git', '-C',
        str(tmp_path), 'config', 'user.email', 'test@example.invalid'
    ],
                   check=True)
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    subprocess.run(['git', '-C', str(tmp_path), 'add', 'sample.py'], check=True)
    subprocess.run(
        ['git', '-C', str(tmp_path), 'commit', '-qm', 'initial'], check=True)
    head = subprocess.run(
        ['git', '-C', str(tmp_path), 'rev-parse', 'HEAD'],
        check=True,
        stdout=subprocess.PIPE,
        text=True).stdout.strip()
    artifact = _artifact()
    artifact['introduced_by'] = head

    assert _check(tmp_path, [artifact]) == []

    artifact['introduced_by'] = 'f' * 40
    errors = _check(tmp_path, [artifact])
    assert any(
        'introduced_by commit does not exist' in error for error in errors)

    tree = subprocess.run(
        ['git', '-C', str(tmp_path), 'rev-parse', 'HEAD^{tree}'],
        check=True,
        stdout=subprocess.PIPE,
        text=True).stdout.strip()
    unrelated = subprocess.run(
        ['git', '-C',
         str(tmp_path), 'commit-tree', tree, '-m', 'unrelated'],
        check=True,
        stdout=subprocess.PIPE,
        text=True).stdout.strip()
    artifact['introduced_by'] = unrelated
    errors = _check(tmp_path, [artifact])
    assert any(
        'introduced_by commit is not an ancestor' in error for error in errors)


def test_terminal_git_evidence_must_exist_and_match_merge_parents(tmp_path):

    def git(*args, capture=False):
        return subprocess.run(['git', '-C', str(tmp_path), *args],
                              check=True,
                              stdout=(subprocess.PIPE if capture else None),
                              text=True)

    git('init', '-q', '-b', 'main')
    git('config', 'user.name', 'Test')
    git('config', 'user.email', 'test@example.invalid')
    (tmp_path / 'sample.py').write_text('class Replacement:\n    pass\n',
                                        encoding='utf-8')
    git('add', 'sample.py')
    git('commit', '-qm', 'initial')
    first_parent = git('rev-parse', 'HEAD', capture=True).stdout.strip()
    git('switch', '-qc', 'topic')
    (tmp_path / 'removal.txt').write_text('removed legacy owner\n',
                                          encoding='utf-8')
    git('add', 'removal.txt')
    git('commit', '-qm', 'remove legacy owner')
    removal_sha = git('rev-parse', 'HEAD', capture=True).stdout.strip()
    git('switch', '-q', 'main')
    git('merge', '--no-ff', 'topic', '-qm', 'merge removal')
    merge_sha = git('rev-parse', 'HEAD', capture=True).stdout.strip()

    artifact = _artifact(status='removed',
                         status_history=[
                             'planned', 'present', 'gating', 'ready_to_remove',
                             'removal_in_progress', 'removed'
                         ],
                         gates=_passed_gates(),
                         runtime_affecting=True)
    artifact['introduced_by'] = first_parent
    artifact['evidence'] = {
        'removal_sha': removal_sha,
        'exact_head_ci': {
            'sha': removal_sha,
            'evidence': ['ci-build.txt'],
        },
        'merge': {
            'sha': merge_sha,
            'first_parent': first_parent,
            'second_parent': removal_sha,
            'evidence': ['merge-proof.txt'],
        },
        'deployment': {
            'sha': merge_sha,
            'evidence': ['deployment-proof.txt'],
        },
    }

    assert _check(tmp_path, [artifact], phase='final') == []

    artifact['evidence']['removal_sha'] = 'f' * 40
    artifact['evidence']['exact_head_ci']['sha'] = 'f' * 40
    artifact['evidence']['merge'].update({
        'sha': 'e' * 40,
        'first_parent': 'd' * 40,
        'second_parent': 'f' * 40,
    })
    artifact['evidence']['deployment']['sha'] = 'e' * 40
    errors = _check(tmp_path, [artifact], phase='final')
    assert any('evidence.removal_sha commit does not exist' in error
               for error in errors)
    assert any(
        'evidence.merge.sha commit does not exist' in error for error in errors)

    artifact['evidence']['removal_sha'] = removal_sha
    artifact['evidence']['exact_head_ci']['sha'] = removal_sha
    artifact['evidence']['merge'].update({
        'sha': merge_sha,
        'first_parent': removal_sha,
        'second_parent': removal_sha,
    })
    artifact['evidence']['deployment']['sha'] = merge_sha
    errors = _check(tmp_path, [artifact], phase='final')
    assert any('evidence.merge.first_parent does not match Git' in error
               for error in errors)


def test_blocked_row_requires_exact_resume_state_and_evidence(tmp_path):
    artifact = _artifact(status='blocked',
                         status_history=['planned', 'present', 'blocked'])
    artifact['blocker'] = {
        'blocked_from_status': 'gating',
        'owner': '',
        'issue': 'ISSUE-1',
        'evidence': [],
    }

    errors = _check(tmp_path, [artifact])

    assert any('blocked_from_status must match' in error for error in errors)
    assert any(
        'blocker.owner must be a nonempty string' in error for error in errors)
    assert any(
        'blocker.evidence must not be empty' in error for error in errors)


def test_retained_history_checksum_and_linked_contraction(tmp_path):
    migration_path = tmp_path / 'migration.py'
    migration_path.write_text("TABLE = 'legacy_table'\n", encoding='utf-8')
    contraction = _artifact(artifact_id='PLA-M7-001',
                            status='removed',
                            status_history=[
                                'planned', 'present', 'gating',
                                'ready_to_remove', 'removal_in_progress',
                                'removed'
                            ],
                            obligation='must_contract',
                            disposition='contract_live_schema',
                            locators=[{
                                'kind': 'sql_object',
                                'path': 'migration.py',
                                'object_type': 'table',
                                'name': 'legacy_table',
                            }],
                            gates=_passed_gates(schema=True))
    retained = _artifact(
        artifact_id='PLA-M7-002',
        status='retained_verified',
        status_history=['planned', 'present', 'gating', 'retained_verified'],
        obligation='retain_history',
        disposition='retain_history',
        locators=[{
            'kind': 'path',
            'path': 'migration.py',
        }],
        gates=_passed_gates(schema=True))
    retained['checksum_sha256'] = hashlib.sha256(
        migration_path.read_bytes()).hexdigest()
    retained['linked_contractions'] = ['PLA-M7-001']

    assert _check(tmp_path, [retained, contraction], phase='final') == []

    retained['checksum_sha256'] = 'f' * 64
    errors = _check(tmp_path, [retained, contraction])
    assert any(
        'retained history checksum mismatch' in error for error in errors)


def test_planned_history_reserves_path_without_claiming_checksum(tmp_path):
    contraction = _artifact(artifact_id='PLA-M7-001',
                            status='planned',
                            status_history=['planned'],
                            obligation='must_contract',
                            disposition='contract_live_schema',
                            locators=[{
                                'kind': 'sql_object',
                                'path': 'future_migration.py',
                                'object_type': 'table',
                                'name': 'future_table',
                            }])
    contraction['introduced_by'] = None
    planned_history = _artifact(artifact_id='PLA-M7-002',
                                status='planned',
                                status_history=['planned'],
                                obligation='retain_history',
                                disposition='retain_history',
                                locators=[{
                                    'kind': 'path',
                                    'path': 'future_migration.py',
                                }])
    planned_history['introduced_by'] = None
    planned_history['linked_contractions'] = ['PLA-M7-001']

    assert _check(tmp_path, [planned_history, contraction]) == []

    planned_history['status'] = 'blocked'
    planned_history['status_history'] = ['planned', 'blocked']
    planned_history['blocker'] = {
        'blocked_from_status': 'planned',
        'owner': 'schema-team',
        'issue': 'ISSUE-2',
        'evidence': ['waiting-for-migration-design.txt'],
    }
    assert _check(tmp_path, [planned_history, contraction]) == []

    planned_history['status'] = 'planned'
    planned_history['status_history'] = ['planned']
    planned_history['blocker'] = None

    planned_history['checksum_sha256'] = 'f' * 64
    errors = _check(tmp_path, [planned_history, contraction])
    assert any('must not claim a checksum before the migration exists' in error
               for error in errors)

    planned_history.pop('checksum_sha256')
    migration_path = tmp_path / 'future_migration.py'
    migration_path.write_text("TABLE = 'future_table'\n", encoding='utf-8')
    errors = _check(tmp_path, [planned_history, contraction])
    assert any('planned retain_history path already exists' in error
               for error in errors)

    planned_history['status'] = 'present'
    planned_history['status_history'] = ['planned', 'present']
    planned_history['introduced_by'] = _INTRODUCING_SHA
    planned_history['checksum_sha256'] = hashlib.sha256(
        migration_path.read_bytes()).hexdigest()
    assert _check(tmp_path, [planned_history, contraction]) == []

    planned_history['checksum_sha256'] = 'f' * 64
    errors = _check(tmp_path, [planned_history, contraction])
    assert any(
        'retained history checksum mismatch' in error for error in errors)


def test_sql_locator_is_source_checked_but_absence_uses_schema_gate(tmp_path):
    migration_path = tmp_path / 'migration.py'
    migration_path.write_text("TABLE = 'legacy_table'\n", encoding='utf-8')
    present = _artifact(obligation='must_contract',
                        disposition='contract_live_schema',
                        locators=[{
                            'kind': 'sql_object',
                            'path': 'migration.py',
                            'object_type': 'table',
                            'name': 'missing_table',
                        }])
    retained = _artifact(
        artifact_id='PLA-M1-002',
        status='retained_verified',
        status_history=['planned', 'present', 'gating', 'retained_verified'],
        obligation='retain_history',
        disposition='retain_history',
        locators=[{
            'kind': 'packaged_path',
            'path': 'migration.py',
        }],
        gates=_passed_gates(schema=True))
    retained['checksum_sha256'] = hashlib.sha256(
        migration_path.read_bytes()).hexdigest()
    retained['linked_contractions'] = ['PLA-M1-001']
    errors = _check(tmp_path, [present, retained])
    assert any(
        'does not name the present SQL object' in error for error in errors)

    present['status'] = 'removed'
    present['status_history'] = [
        'planned', 'present', 'gating', 'ready_to_remove',
        'removal_in_progress', 'removed'
    ]
    present['gates'] = _passed_gates(schema=True)
    present['evidence'] = {
        'removal_sha': _REMOVAL_SHA,
        'exact_head_ci': {
            'sha': _REMOVAL_SHA,
            'evidence': ['ci-build.txt'],
        },
        'merge': {
            'sha': _MERGE_SHA,
            'first_parent': _FIRST_PARENT_SHA,
            'second_parent': _REMOVAL_SHA,
            'evidence': ['merge-proof.txt'],
        },
    }
    assert _check(tmp_path, [present, retained], phase='final') == []


def test_sql_contraction_requires_reverse_history_link(tmp_path):
    (tmp_path / 'migration.py').write_text("TABLE = 'legacy_table'\n",
                                           encoding='utf-8')
    contraction = _artifact(obligation='must_contract',
                            disposition='contract_live_schema',
                            locators=[{
                                'kind': 'sql_object',
                                'path': 'migration.py',
                                'object_type': 'table',
                                'name': 'legacy_table',
                            }])

    errors = _check(tmp_path, [contraction])

    assert any('requires a retain_history owner linked back' in error
               for error in errors)

    retained = _artifact(artifact_id='PLA-M1-002',
                         obligation='retain_history',
                         disposition='retain_history',
                         locators=[{
                             'kind': 'packaged_path',
                             'path': 'migration.py',
                         }])
    retained['checksum_sha256'] = hashlib.sha256(
        (tmp_path / 'migration.py').read_bytes()).hexdigest()
    retained['linked_contractions'] = ['PLA-M1-001']
    assert _check(tmp_path, [contraction, retained]) == []

    second_path = tmp_path / 'migration_copy.py'
    second_path.write_text("TABLE = 'legacy_table'\n", encoding='utf-8')
    second = _artifact(artifact_id='PLA-M1-003',
                       obligation='retain_history',
                       disposition='retain_history',
                       locators=[{
                           'kind': 'packaged_path',
                           'path': 'migration_copy.py',
                       }])
    second['checksum_sha256'] = hashlib.sha256(
        second_path.read_bytes()).hexdigest()
    second['linked_contractions'] = ['PLA-M1-001']
    contraction['retained_references'] = ['PLA-M1-003']

    errors = _check(tmp_path, [contraction, retained, second])

    assert any('must have exactly one retain_history owner' in error
               for error in errors)


def test_line_only_locator_and_duplicate_yaml_keys_are_rejected(tmp_path):
    (tmp_path / 'sample.py').write_text('class Legacy:\n    pass\n',
                                        encoding='utf-8')
    artifact = _artifact()
    artifact['locators'][0]['line'] = 1
    errors = _check(tmp_path, [artifact])
    assert any('must not use line-number identity' in error for error in errors)

    manifest_path = tmp_path / 'duplicate.yaml'
    manifest_path.write_text(
        'schema_version: 1\nschema_version: 1\nartifacts: []\n',
        encoding='utf-8')
    errors = check_lifecycle_removals.check_manifest(manifest_path,
                                                     'current',
                                                     repo_root=tmp_path)
    assert len(errors) == 1
    assert 'duplicate key' in errors[0]


def test_cli_returns_success_and_failure(tmp_path, capsys):
    artifact = _artifact(locators=[{
        'kind': 'python_symbol',
        'path': 'tools/check_lifecycle_removals.py',
        'symbol': 'ManifestChecker',
    }])
    artifact['introduced_by'] = subprocess.run(
        ['git', '-C',
         str(_CHECKER_PATH.parents[1]), 'rev-parse', 'HEAD'],
        check=True,
        stdout=subprocess.PIPE,
        text=True).stdout.strip()
    manifest_path = _write_manifest(tmp_path, [artifact])

    assert check_lifecycle_removals.main(
        ['--manifest', str(manifest_path), '--phase', 'current']) == 0
    assert 'passes current validation' in capsys.readouterr().out

    assert check_lifecycle_removals.main(
        ['--manifest', str(manifest_path), '--phase', 'final']) == 1
    assert 'final phase requires status removed' in capsys.readouterr().err
