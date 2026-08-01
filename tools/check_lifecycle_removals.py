#!/usr/bin/env python3
"""Validate the provider lifecycle removal manifest.

The checker intentionally does not import SkyPilot modules.  Python locators
are resolved with the AST so validation is deterministic and does not require
optional cloud dependencies.  PostgreSQL objects receive structural and
source-file validation only; live catalog proof remains an explicit manifest
gate.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import pathlib
import re
import subprocess
import sys
import typing

import yaml

_SCHEMA_VERSION = 1
_ARTIFACT_ID_RE = re.compile(r'PLA-(BASE|M[0-7])-\d{3}\Z')
_COVERAGE_GAP_ID_RE = re.compile(r'PLA-GAP-\d{3}\Z')
_SHA_RE = re.compile(r'[0-9a-f]{40}\Z')
_SHA256_RE = re.compile(r'[0-9a-f]{64}\Z')
_NAME_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_.:-]*\Z')
_SCOPE_VALUE_RE = re.compile(r'[a-z][a-z0-9_]*\Z')
_GLOB_CHARS = frozenset('*?[]{}')

_MILESTONES = frozenset(['BASE'] + [f'M{i}' for i in range(8)])
_OBLIGATIONS = frozenset({
    'must_remove',
    'must_contract',
    'retain_history',
    'retain_characterization',
})
_DISPOSITIONS = frozenset({
    'delete_file',
    'delete_symbol',
    'delete_branch',
    'delete_enum_member',
    'replace_content',
    'contract_live_schema',
    'retain_history',
    'retain_characterization',
})
_STATUSES = frozenset({
    'planned',
    'present',
    'gating',
    'ready_to_remove',
    'removal_in_progress',
    'removed',
    'blocked',
    'retained_verified',
})
_INCOMPLETE_STATUSES = frozenset({
    'planned',
    'present',
    'gating',
    'ready_to_remove',
    'removal_in_progress',
})
_PRESENT_STATUSES = frozenset({
    'present',
    'gating',
    'ready_to_remove',
})
_LOCATOR_KINDS = frozenset({
    'python_symbol',
    'python_attribute',
    'python_call_within',
    'python_enum_member',
    'python_ast_pattern',
    'file_digest',
    'path',
    'packaged_path',
    'sql_object',
    'runtime_metadata',
    'runtime_import',
    'test_node',
})
_CONTENT_ABSENCE_LOCATOR_KINDS = frozenset({
    'python_attribute',
    'python_call_within',
    'python_enum_member',
    'python_ast_pattern',
    'file_digest',
    'runtime_metadata',
    'runtime_import',
    'test_node',
})
_GATE_CATEGORIES = (
    'source',
    'test',
    'telemetry',
    'release_window',
    'schema',
)
_BROAD_SCOPE_VALUES = frozenset({
    'all',
    'any',
    'migrated',
    'promoted',
    'all providers',
    'all promoted providers',
})

_TOP_LEVEL_KEYS = frozenset({'schema_version', 'coverage_gaps', 'artifacts'})
_COVERAGE_GAP_KEYS = frozenset({
    'id',
    'milestone',
    'responsibility',
    'candidate_locators',
    'owner',
    'reason',
    'closure_gate',
})
_ARTIFACT_REQUIRED_KEYS = frozenset({
    'id',
    'milestone',
    'introduced_by',
    'obligation',
    'disposition',
    'scope',
    'runtime_affecting',
    'locators',
    'replacement',
    'dependencies',
    'status',
    'status_history',
    'gates',
    'retained_references',
    'evidence',
    'blocker',
})
_ARTIFACT_OPTIONAL_KEYS = frozenset({
    'checksum_sha256',
    'linked_contractions',
})
_SCOPE_KEYS = frozenset({'domain', 'store', 'provider', 'operation'})
_GATE_KEYS = frozenset({'id', 'satisfied', 'evidence'})
_BLOCKER_KEYS = frozenset({
    'blocked_from_status',
    'owner',
    'issue',
    'evidence',
})
_EVIDENCE_KEYS = frozenset({
    'removal_sha',
    'exact_head_ci',
    'merge',
    'deployment',
})
_PROOF_KEYS = frozenset({'sha', 'evidence'})
_MERGE_PROOF_KEYS = frozenset({
    'sha',
    'first_parent',
    'second_parent',
    'evidence',
})

_NORMAL_TRANSITIONS = {
    'planned': frozenset({'present'}),
    'present': frozenset({'gating'}),
    'gating': frozenset({'ready_to_remove', 'retained_verified'}),
    'ready_to_remove': frozenset({'removal_in_progress'}),
    'removal_in_progress': frozenset({'removed'}),
    'removed': frozenset(),
    'retained_verified': frozenset(),
}


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
        loader: _UniqueKeyLoader,
        node: yaml.nodes.MappingNode,
        deep: bool = False) -> typing.Dict[typing.Any, typing.Any]:
    mapping: typing.Dict[typing.Any, typing.Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                'while constructing a mapping', node.start_mark,
                f'duplicate key: {key!r}', key_node.start_mark)
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                                 _construct_unique_mapping)


def _has_glob(value: str) -> bool:
    return any(character in value for character in _GLOB_CHARS)


def _is_nonempty_string(value: typing.Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _format_key_set(keys: typing.Iterable[typing.Any]) -> str:
    return ', '.join(sorted(str(key) for key in keys))


def _assignment_names(node: ast.AST) -> typing.Set[str]:
    names: typing.Set[str] = set()
    targets: typing.List[ast.AST] = []
    if isinstance(node, ast.Assign):
        targets.extend(node.targets)
    elif isinstance(node, ast.AnnAssign):
        targets.append(node.target)
    elif isinstance(node, (ast.Import, ast.ImportFrom)):
        for alias in node.names:
            names.add(alias.asname or alias.name.split('.')[0])
        return names
    for target in targets:
        for candidate in ast.walk(target):
            if isinstance(candidate, ast.Name):
                names.add(candidate.id)
    return names


def _node_body(node: ast.AST) -> typing.Optional[typing.List[ast.stmt]]:
    if isinstance(
            node,
        (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        return node.body
    return None


def _resolve_python_symbol(tree: ast.Module,
                           symbol: str) -> typing.Optional[ast.AST]:
    if symbol in ('<module>', '__module__'):
        return tree
    current: ast.AST = tree
    for part in symbol.replace('::', '.').split('.'):
        body = _node_body(current)
        if body is None:
            return None
        found: typing.Optional[ast.AST] = None
        for statement in body:
            if isinstance(
                    statement,
                (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if statement.name == part:
                    found = statement
                    break
            elif part in _assignment_names(statement):
                found = statement
                break
        if found is None:
            return None
        current = found
    return current


def _qualified_name(node: ast.AST) -> typing.Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _qualified_name(node.value)
        if parent is not None:
            return f'{parent}.{node.attr}'
    return None


def _contains_attribute(owner: ast.AST, attribute: str) -> bool:
    body = _node_body(owner)
    if body is not None:
        for statement in body:
            if attribute in _assignment_names(statement):
                return True
    return any(
        isinstance(node, ast.Attribute) and node.attr == attribute
        for node in ast.walk(owner))


def _contains_call(owner: ast.AST, call_name: str) -> bool:
    for node in ast.walk(owner):
        if not isinstance(node, ast.Call):
            continue
        actual_name = _qualified_name(node.func)
        if actual_name is None:
            continue
        if actual_name == call_name:
            return True
        # A bare method name deliberately matches a qualified call.  Once a
        # locator supplies a qualifier, however, accepting an arbitrary
        # suffix would let a different owner satisfy the removal obligation.
        if '.' not in call_name and actual_name.endswith(f'.{call_name}'):
            return True
    return False


def _contains_enum_member(enum_node: ast.AST, member: str) -> bool:
    body = _node_body(enum_node)
    if body is None:
        return False
    return any(member in _assignment_names(statement) for statement in body)


def _pattern_node(pattern: str) -> ast.AST:
    parsed = ast.parse(pattern)
    if len(parsed.body) != 1:
        raise ValueError(
            'pattern must contain exactly one expression or statement')
    node = parsed.body[0]
    if isinstance(node, ast.Expr):
        return node.value
    return node


def _contains_ast_pattern(owner: ast.AST, pattern: str) -> bool:
    expected = _pattern_node(pattern)
    expected_dump = ast.dump(expected, include_attributes=False)
    return any(
        type(node) is type(expected) and  # pylint: disable=unidiomatic-typecheck
        ast.dump(node, include_attributes=False) == expected_dump
        for node in ast.walk(owner))


def _contains_runtime_metadata(tree: ast.Module, name: str) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == name:
            return True
        if isinstance(node, ast.Attribute) and node.attr == name:
            return True
        if isinstance(node, ast.Constant) and node.value == name:
            return True
    return False


def _contains_runtime_import(tree: ast.Module, module: str,
                             symbol: typing.Optional[str]) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == module and symbol is None:
                    return True
        elif isinstance(node, ast.ImportFrom) and node.module == module:
            if symbol is None:
                return True
            if any(alias.name == symbol for alias in node.names):
                return True
    return False


class ManifestChecker:
    """Checks one parsed manifest against the repository tree."""

    def __init__(self, repo_root: pathlib.Path, phase: str):
        self._repo_root = repo_root.resolve()
        self._phase = phase
        self._errors: typing.List[str] = []
        self._artifacts_by_id: typing.Dict[str,
                                           typing.Mapping[str,
                                                          typing.Any]] = {}
        self._coverage_gap_ids: typing.Set[str] = set()
        self._mutable_node_owners: typing.Dict[int, str] = {}
        self._ast_cache: typing.Dict[pathlib.Path,
                                     typing.Optional[ast.Module]] = {}
        self._git_available = self._detect_git_checkout()
        self._provenance_cache: typing.Dict[str, typing.Tuple[bool, bool]] = {}

    def _detect_git_checkout(self) -> bool:
        try:
            result = subprocess.run([
                'git', '-C',
                str(self._repo_root), 'rev-parse', '--show-toplevel'
            ],
                                    check=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL,
                                    text=True)
        except OSError:
            return False
        if result.returncode != 0:
            return False
        try:
            return pathlib.Path(
                result.stdout.strip()).resolve() == self._repo_root
        except (OSError, RuntimeError):
            return False

    def _git_commit_state(self, sha: str) -> typing.Tuple[bool, bool]:
        cached = self._provenance_cache.get(sha)
        if cached is not None:
            return cached
        try:
            exists_result = subprocess.run([
                'git', '-C',
                str(self._repo_root), 'cat-file', '-e', f'{sha}^{{commit}}'
            ],
                                           check=False,
                                           stdout=subprocess.DEVNULL,
                                           stderr=subprocess.DEVNULL)
        except OSError:
            result = (False, False)
            self._provenance_cache[sha] = result
            return result
        exists = exists_result.returncode == 0
        ancestor = False
        if exists:
            ancestor_result = subprocess.run([
                'git', '-C',
                str(self._repo_root), 'merge-base', '--is-ancestor', sha, 'HEAD'
            ],
                                             check=False,
                                             stdout=subprocess.DEVNULL,
                                             stderr=subprocess.DEVNULL)
            ancestor = ancestor_result.returncode == 0
        result = (exists, ancestor)
        self._provenance_cache[sha] = result
        return result

    def _check_git_commit(self, artifact_id: str, label: str, sha: str) -> bool:
        if not self._git_available:
            return True
        exists, ancestor = self._git_commit_state(sha)
        if not exists:
            self._error(artifact_id,
                        f'{label} commit does not exist in this checkout')
        elif not ancestor:
            self._error(artifact_id,
                        f'{label} commit is not an ancestor of HEAD')
        return exists and ancestor

    def check(self, document: typing.Any) -> typing.List[str]:
        if not isinstance(document, dict):
            return ['manifest: top level must be a mapping']

        self._check_keys('manifest', document, _TOP_LEVEL_KEYS, _TOP_LEVEL_KEYS)
        if document.get('schema_version') != _SCHEMA_VERSION:
            self._error('manifest', f'schema_version must be {_SCHEMA_VERSION}')

        self._check_coverage_gaps(document.get('coverage_gaps'))

        artifacts = document.get('artifacts')
        if not isinstance(artifacts, list) or not artifacts:
            self._error('manifest', 'artifacts must be a nonempty list')
            return sorted(self._errors)

        for index, artifact in enumerate(artifacts):
            label = f'artifacts[{index}]'
            if not isinstance(artifact, dict):
                self._error(label, 'artifact must be a mapping')
                continue
            artifact_id = artifact.get('id')
            if not _is_nonempty_string(artifact_id):
                self._error(label, 'id must be a nonempty string')
                continue
            if artifact_id in self._artifacts_by_id:
                self._error(artifact_id, 'duplicate artifact id')
                continue
            self._artifacts_by_id[artifact_id] = artifact

        for artifact_id in sorted(self._artifacts_by_id):
            self._check_artifact(artifact_id,
                                 self._artifacts_by_id[artifact_id])
        self._check_references()
        self._check_history_reverse_links()
        self._check_dependency_cycles()
        return sorted(self._errors)

    def _check_coverage_gaps(self, coverage_gaps: typing.Any) -> None:
        if not isinstance(coverage_gaps, list):
            self._error('manifest', 'coverage_gaps must be a list')
            return
        seen_ids: typing.Set[str] = set()
        for index, gap in enumerate(coverage_gaps):
            label = f'coverage_gaps[{index}]'
            if not isinstance(gap, dict):
                self._error(label, 'coverage gap must be a mapping')
                continue
            self._check_keys(label, gap, _COVERAGE_GAP_KEYS, _COVERAGE_GAP_KEYS)
            gap_id = gap.get('id')
            if not isinstance(gap_id, str) or _COVERAGE_GAP_ID_RE.fullmatch(
                    gap_id) is None:
                self._error(label, 'id must match PLA-GAP-NNN')
                gap_id = label
            elif gap_id in seen_ids:
                self._error(gap_id, 'duplicate coverage gap id')
            else:
                seen_ids.add(gap_id)
                self._coverage_gap_ids.add(gap_id)
            if gap.get('milestone') not in _MILESTONES:
                self._error(gap_id, 'coverage gap milestone is invalid')
            for key in ('responsibility', 'owner', 'reason', 'closure_gate'):
                if not _is_nonempty_string(gap.get(key)):
                    self._error(gap_id, f'coverage gap {key} must be nonempty')
            locators = gap.get('candidate_locators')
            if not isinstance(locators, list) or not locators:
                self._error(gap_id,
                            'coverage gap candidate_locators must be nonempty')
            else:
                for locator_index, locator in enumerate(locators):
                    if not isinstance(locator, dict) or locator.get(
                            'kind') != 'python_symbol':
                        self._error(
                            gap_id,
                            'coverage gap candidate locators must be exact '
                            'python_symbol locators')
                    self._check_locator(gap_id, locator_index, locator, True)
            if self._phase == 'final':
                self._error(gap_id, 'final phase requires coverage gap closure')

    def _error(self, label: str, message: str) -> None:
        self._errors.append(f'{label}: {message}')

    def _check_keys(self, label: str, value: typing.Mapping[str, typing.Any],
                    required: typing.AbstractSet[str],
                    allowed: typing.AbstractSet[str]) -> None:
        missing = required - value.keys()
        unknown = value.keys() - allowed
        if missing:
            self._error(label, f'missing keys: {_format_key_set(missing)}')
        if unknown:
            self._error(label, f'unknown keys: {_format_key_set(unknown)}')

    def _check_artifact(self, artifact_id: str,
                        artifact: typing.Mapping[str, typing.Any]) -> None:
        self._check_keys(artifact_id, artifact, _ARTIFACT_REQUIRED_KEYS,
                         _ARTIFACT_REQUIRED_KEYS | _ARTIFACT_OPTIONAL_KEYS)

        match = _ARTIFACT_ID_RE.fullmatch(artifact_id)
        if match is None:
            self._error(artifact_id, 'id must match PLA-(BASE|M[0-7])-NNN')

        milestone = artifact.get('milestone')
        if milestone not in _MILESTONES:
            self._error(artifact_id,
                        f'milestone must be one of {sorted(_MILESTONES)}')
        elif match is not None and match.group(1) != milestone:
            self._error(artifact_id, 'id milestone does not match milestone')

        obligation = artifact.get('obligation')
        disposition = artifact.get('disposition')
        status = artifact.get('status')
        if obligation not in _OBLIGATIONS:
            self._error(artifact_id, 'invalid obligation')
        if disposition not in _DISPOSITIONS:
            self._error(artifact_id, 'invalid disposition')
        if status not in _STATUSES:
            self._error(artifact_id, 'invalid status')

        self._check_obligation_disposition(artifact_id, obligation, disposition)
        self._check_introducing_sha(artifact_id, artifact.get('introduced_by'),
                                    status, artifact.get('blocker'))
        self._check_scope(artifact_id, artifact.get('scope'))

        if not isinstance(artifact.get('runtime_affecting'), bool):
            self._error(artifact_id, 'runtime_affecting must be a boolean')
        if not _is_nonempty_string(artifact.get('replacement')):
            self._error(artifact_id, 'replacement must be a nonempty string')

        self._check_string_list(artifact_id, 'dependencies',
                                artifact.get('dependencies'))
        self._check_string_list(artifact_id,
                                'retained_references',
                                artifact.get('retained_references'),
                                reject_globs=True)
        self._check_status_history(artifact_id, status,
                                   artifact.get('status_history'),
                                   artifact.get('blocker'))
        gates_valid = self._check_gates(artifact_id, artifact.get('gates'))
        self._check_evidence(artifact_id, artifact, gates_valid)

        locators = artifact.get('locators')
        if not isinstance(locators, list) or not locators:
            self._error(artifact_id, 'locators must be a nonempty list')
        else:
            self._check_disposition_locators(artifact_id, disposition, locators)
            expectation = self._expected_presence(status,
                                                  artifact.get('blocker'))
            for index, locator in enumerate(locators):
                self._check_locator(artifact_id, index, locator, expectation)

        self._check_retention(artifact_id, artifact)
        if self._phase == 'final':
            if obligation in ('must_remove',
                              'must_contract') and status != 'removed':
                self._error(artifact_id, 'final phase requires status removed')
            if obligation in ('retain_history', 'retain_characterization'
                             ) and status != 'retained_verified':
                self._error(artifact_id,
                            'final phase requires status retained_verified')

    def _check_disposition_locators(
            self, artifact_id: str, disposition: typing.Any,
            locators: typing.Sequence[typing.Any]) -> None:
        if disposition != 'replace_content':
            return
        kinds = [
            locator.get('kind')
            for locator in locators
            if isinstance(locator, dict)
        ]
        invalid_kinds = sorted({
            kind for kind in kinds if kind in _LOCATOR_KINDS and
            kind not in _CONTENT_ABSENCE_LOCATOR_KINDS
        })
        if invalid_kinds:
            self._error(
                artifact_id,
                'replace_content cannot use enclosing or physical locators: '
                f'{", ".join(invalid_kinds)}')
        if not any(kind in _CONTENT_ABSENCE_LOCATOR_KINDS for kind in kinds):
            self._error(
                artifact_id,
                'replace_content requires an absence-verifiable content locator'
            )

    def _check_obligation_disposition(self, artifact_id: str,
                                      obligation: typing.Any,
                                      disposition: typing.Any) -> None:
        expected_retention = {
            'retain_history': 'retain_history',
            'retain_characterization': 'retain_characterization',
        }
        if obligation in expected_retention:
            if disposition != expected_retention[obligation]:
                self._error(artifact_id,
                            'retention obligation and disposition must match')
        elif disposition in ('retain_history', 'retain_characterization'):
            self._error(
                artifact_id,
                'removal obligation cannot use a retention disposition')
        if obligation == 'must_remove' and disposition == 'contract_live_schema':
            self._error(artifact_id,
                        'contract_live_schema requires must_contract')

    def _check_introducing_sha(self, artifact_id: str,
                               introduced_by: typing.Any, status: typing.Any,
                               blocker: typing.Any) -> None:
        effective_status = status
        if status == 'blocked' and isinstance(blocker, dict):
            effective_status = blocker.get('blocked_from_status')
        if introduced_by is None:
            if effective_status != 'planned':
                self._error(
                    artifact_id,
                    'introduced_by may be null only while effectively planned')
            return
        if not isinstance(introduced_by,
                          str) or _SHA_RE.fullmatch(introduced_by) is None:
            self._error(artifact_id,
                        'introduced_by must be a lowercase 40-hex SHA')
            return
        self._check_git_commit(artifact_id, 'introduced_by', introduced_by)

    def _check_scope(self, artifact_id: str, scope: typing.Any) -> None:
        if not isinstance(scope, dict):
            self._error(artifact_id, 'scope must be a mapping')
            return
        self._check_keys(f'{artifact_id}.scope', scope, _SCOPE_KEYS,
                         _SCOPE_KEYS)
        for key in sorted(_SCOPE_KEYS):
            value = scope.get(key)
            if not _is_nonempty_string(value):
                self._error(artifact_id,
                            f'scope.{key} must be a nonempty string')
                continue
            normalized = value.strip().lower()
            if value != normalized or _SCOPE_VALUE_RE.fullmatch(value) is None:
                self._error(artifact_id,
                            f'scope.{key} must be a canonical lowercase token')
            if _has_glob(
                    value
            ) or normalized in _BROAD_SCOPE_VALUES or normalized.startswith(
                    'all '):
                self._error(artifact_id,
                            f'scope.{key} must be exact, not {value!r}')

    def _check_string_list(self,
                           artifact_id: str,
                           field: str,
                           value: typing.Any,
                           reject_globs: bool = False) -> None:
        if not isinstance(value, list):
            self._error(artifact_id, f'{field} must be a list')
            return
        seen: typing.Set[str] = set()
        for index, item in enumerate(value):
            if not _is_nonempty_string(item):
                self._error(artifact_id,
                            f'{field}[{index}] must be a nonempty string')
                continue
            if item in seen:
                self._error(artifact_id, f'{field} contains duplicate {item!r}')
            seen.add(item)
            if reject_globs and _has_glob(item):
                self._error(artifact_id,
                            f'{field}[{index}] must not contain a wildcard')

    def _check_status_history(self, artifact_id: str, status: typing.Any,
                              history: typing.Any, blocker: typing.Any) -> None:
        if not isinstance(history, list) or not history:
            self._error(artifact_id, 'status_history must be a nonempty list')
            return
        for index, item in enumerate(history):
            if item not in _STATUSES:
                self._error(artifact_id, f'status_history[{index}] is invalid')
        if history[0] != 'planned':
            self._error(artifact_id, 'status_history must start with planned')
        if history[-1] != status:
            self._error(artifact_id,
                        'status_history must end with the current status')

        resumable_status: typing.Optional[str] = None
        for previous, current in zip(history, history[1:]):
            if previous not in _STATUSES or current not in _STATUSES:
                continue
            if current == 'blocked':
                if previous not in _INCOMPLETE_STATUSES:
                    self._error(
                        artifact_id,
                        f'invalid status transition {previous} -> blocked')
                resumable_status = previous
                continue
            if previous == 'blocked':
                if resumable_status is None or current != resumable_status:
                    self._error(
                        artifact_id,
                        'blocked status must resume to its recorded prior status'
                    )
                resumable_status = None
                continue
            allowed = _NORMAL_TRANSITIONS.get(previous, frozenset())
            if current not in allowed:
                self._error(
                    artifact_id,
                    f'invalid status transition {previous} -> {current}')

        if status == 'blocked':
            if not isinstance(blocker, dict):
                self._error(artifact_id,
                            'blocked status requires a blocker mapping')
                return
            self._check_keys(f'{artifact_id}.blocker', blocker, _BLOCKER_KEYS,
                             _BLOCKER_KEYS)
            blocked_from = blocker.get('blocked_from_status')
            expected_from = history[-2] if len(history) >= 2 else None
            if blocked_from != expected_from or blocked_from not in _INCOMPLETE_STATUSES:
                self._error(
                    artifact_id,
                    'blocker.blocked_from_status must match the prior incomplete status'
                )
            for key in ('owner', 'issue'):
                if not _is_nonempty_string(blocker.get(key)):
                    self._error(artifact_id,
                                f'blocker.{key} must be a nonempty string')
            self._check_evidence_values(artifact_id,
                                        'blocker.evidence',
                                        blocker.get('evidence'),
                                        required=True)
        elif blocker is not None:
            self._error(artifact_id,
                        'blocker must be null unless status is blocked')

    def _check_gates(self, artifact_id: str, gates: typing.Any) -> bool:
        if not isinstance(gates, dict):
            self._error(artifact_id, 'gates must be a mapping')
            return False
        self._claim_mutable_tree(artifact_id, 'gates', gates,
                                 self._mutable_node_owners)
        self._check_keys(f'{artifact_id}.gates', gates,
                         frozenset(_GATE_CATEGORIES),
                         frozenset(_GATE_CATEGORIES))
        valid = True
        for category in _GATE_CATEGORIES:
            entries = gates.get(category)
            if not isinstance(entries, list):
                self._error(artifact_id, f'gates.{category} must be a list')
                valid = False
                continue
            seen_ids: typing.Set[str] = set()
            for index, entry in enumerate(entries):
                label = f'gates.{category}[{index}]'
                if not isinstance(entry, dict):
                    self._error(artifact_id, f'{label} must be a mapping')
                    valid = False
                    continue
                self._check_keys(f'{artifact_id}.{label}', entry, _GATE_KEYS,
                                 _GATE_KEYS)
                gate_id = entry.get('id')
                if not _is_nonempty_string(gate_id) or _has_glob(gate_id):
                    self._error(artifact_id,
                                f'{label}.id must be exact and nonempty')
                    valid = False
                elif gate_id in seen_ids:
                    self._error(
                        artifact_id,
                        f'gates.{category} has duplicate id {gate_id!r}')
                    valid = False
                else:
                    seen_ids.add(gate_id)
                if not isinstance(entry.get('satisfied'), bool):
                    self._error(artifact_id,
                                f'{label}.satisfied must be a boolean')
                    valid = False
                evidence_valid = self._check_evidence_values(
                    artifact_id,
                    f'{label}.evidence',
                    entry.get('evidence'),
                    required=entry.get('satisfied') is True)
                valid = valid and evidence_valid
        return valid

    def _check_evidence_values(self, artifact_id: str, label: str,
                               value: typing.Any, required: bool) -> bool:
        values: typing.List[typing.Any]
        if isinstance(value, str):
            values = [value]
        elif isinstance(value, list):
            values = value
        else:
            self._error(artifact_id,
                        f'{label} must be a string or list of strings')
            return False
        valid = True
        if required and not values:
            self._error(artifact_id, f'{label} must not be empty')
            valid = False
        for index, item in enumerate(values):
            if not _is_nonempty_string(item):
                self._error(artifact_id,
                            f'{label}[{index}] must be a nonempty string')
                valid = False
        return valid

    def _check_evidence(self, artifact_id: str,
                        artifact: typing.Mapping[str, typing.Any],
                        gates_valid: bool) -> None:
        evidence = artifact.get('evidence')
        if not isinstance(evidence, dict):
            self._error(artifact_id, 'evidence must be a mapping')
            return
        self._claim_mutable_tree(artifact_id, 'evidence', evidence,
                                 self._mutable_node_owners)
        self._check_keys(f'{artifact_id}.evidence', evidence, frozenset(),
                         _EVIDENCE_KEYS)
        status = artifact.get('status')
        obligation = artifact.get('obligation')
        terminal = status in ('removed', 'retained_verified')
        gates = artifact.get('gates')
        if terminal and gates_valid and isinstance(gates, dict):
            for category in _GATE_CATEGORIES:
                entries = gates.get(category, [])
                for entry in entries:
                    if isinstance(entry,
                                  dict) and entry.get('satisfied') is not True:
                        self._error(
                            artifact_id,
                            f'terminal status requires gates.{category} to pass'
                        )
            if not gates.get('source'):
                self._error(artifact_id,
                            'terminal status requires a source gate')
            if obligation in ('must_contract',
                              'retain_history') and not gates.get('schema'):
                self._error(
                    artifact_id,
                    'terminal schema obligation requires a schema gate')

        if status != 'removed':
            for key in ('removal_sha', 'exact_head_ci', 'merge', 'deployment'):
                if key in evidence:
                    self._error(artifact_id,
                                f'evidence.{key} is legal only when removed')
            return

        removal_sha = evidence.get('removal_sha')
        if not isinstance(removal_sha,
                          str) or _SHA_RE.fullmatch(removal_sha) is None:
            self._error(artifact_id,
                        'removed status requires evidence.removal_sha')
        else:
            self._check_git_commit(artifact_id, 'evidence.removal_sha',
                                   removal_sha)
        exact_head_sha = self._check_proof(artifact_id,
                                           evidence,
                                           'exact_head_ci',
                                           required=True)
        if isinstance(
                removal_sha, str
        ) and exact_head_sha is not None and exact_head_sha != removal_sha:
            self._error(
                artifact_id,
                'evidence.exact_head_ci.sha must equal evidence.removal_sha')
        merge_sha = self._check_merge_proof(artifact_id, evidence, removal_sha)
        runtime_affecting = artifact.get('runtime_affecting') is True
        deployment_sha = self._check_proof(artifact_id,
                                           evidence,
                                           'deployment',
                                           required=runtime_affecting)
        if deployment_sha is not None and merge_sha is not None and deployment_sha != merge_sha:
            self._error(
                artifact_id,
                'evidence.deployment.sha must equal evidence.merge.sha')

    def _claim_mutable_tree(self, artifact_id: str, label: str,
                            value: typing.Any,
                            owners: typing.Dict[int, str]) -> None:
        if not isinstance(value, (dict, list)):
            return
        value_id = id(value)
        prior_owner = owners.get(value_id)
        if prior_owner is not None:
            if prior_owner != artifact_id:
                self._error(
                    artifact_id,
                    f'{label} must be artifact-owned, not a YAML alias shared '
                    f'with {prior_owner}')
            return
        owners[value_id] = artifact_id
        children = value.values() if isinstance(value, dict) else value
        for child in children:
            self._claim_mutable_tree(artifact_id, label, child, owners)

    def _check_merge_proof(self, artifact_id: str,
                           evidence: typing.Mapping[str, typing.Any],
                           removal_sha: typing.Any) -> typing.Optional[str]:
        proof = evidence.get('merge')
        if proof is None:
            self._error(artifact_id, 'removed status requires evidence.merge')
            return None
        if not isinstance(proof, dict):
            self._error(artifact_id, 'evidence.merge must be a mapping')
            return None
        self._check_keys(f'{artifact_id}.evidence.merge', proof,
                         _MERGE_PROOF_KEYS, _MERGE_PROOF_KEYS)
        valid_shas: typing.Dict[str, str] = {}
        verified_shas: typing.Dict[str, bool] = {}
        for key in ('sha', 'first_parent', 'second_parent'):
            value = proof.get(key)
            if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
                self._error(
                    artifact_id,
                    f'evidence.merge.{key} must be a lowercase 40-hex SHA')
            else:
                valid_shas[key] = value
                verified_shas[key] = self._check_git_commit(
                    artifact_id, f'evidence.merge.{key}', value)
        self._check_evidence_values(artifact_id,
                                    'evidence.merge.evidence',
                                    proof.get('evidence'),
                                    required=True)
        if isinstance(removal_sha,
                      str) and valid_shas.get('second_parent') != removal_sha:
            self._error(
                artifact_id,
                'evidence.merge.second_parent must equal evidence.removal_sha')
        merge_sha = valid_shas.get('sha')
        if self._git_available and merge_sha is not None and verified_shas.get(
                'sha'):
            try:
                result = subprocess.run([
                    'git', '-C',
                    str(self._repo_root), 'rev-list', '--parents', '-n', '1',
                    merge_sha
                ],
                                        check=False,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL,
                                        text=True)
            except OSError:
                result = None
            parents = [] if result is None else result.stdout.strip().split()
            if result is None or result.returncode != 0:
                self._error(artifact_id,
                            'evidence.merge parents cannot be inspected')
            elif len(parents) != 3:
                self._error(
                    artifact_id,
                    'evidence.merge.sha must be a normal two-parent merge')
            else:
                if parents[1] != valid_shas.get('first_parent'):
                    self._error(
                        artifact_id,
                        'evidence.merge.first_parent does not match Git')
                if parents[2] != valid_shas.get('second_parent'):
                    self._error(
                        artifact_id,
                        'evidence.merge.second_parent does not match Git')
        return merge_sha

    def _check_proof(self, artifact_id: str,
                     evidence: typing.Mapping[str, typing.Any], key: str,
                     required: bool) -> typing.Optional[str]:
        proof = evidence.get(key)
        if proof is None:
            if required:
                self._error(artifact_id,
                            f'removed status requires evidence.{key}')
            return None
        if not isinstance(proof, dict):
            self._error(artifact_id, f'evidence.{key} must be a mapping')
            return None
        self._check_keys(f'{artifact_id}.evidence.{key}', proof, _PROOF_KEYS,
                         _PROOF_KEYS)
        sha = proof.get('sha')
        if not isinstance(sha, str) or _SHA_RE.fullmatch(sha) is None:
            self._error(artifact_id,
                        f'evidence.{key}.sha must be a lowercase 40-hex SHA')
            sha = None
        else:
            self._check_git_commit(artifact_id, f'evidence.{key}.sha', sha)
        self._check_evidence_values(artifact_id,
                                    f'evidence.{key}.evidence',
                                    proof.get('evidence'),
                                    required=True)
        return sha

    def _expected_presence(self, status: typing.Any,
                           blocker: typing.Any) -> typing.Optional[bool]:
        effective_status = status
        if status == 'blocked' and isinstance(blocker, dict):
            effective_status = blocker.get('blocked_from_status')
        if effective_status in _PRESENT_STATUSES:
            return True
        if effective_status == 'removed':
            return False
        if effective_status == 'retained_verified':
            return True
        return None

    def _check_locator(self, artifact_id: str, index: int, locator: typing.Any,
                       expectation: typing.Optional[bool]) -> None:
        label = f'locators[{index}]'
        if not isinstance(locator, dict):
            self._error(artifact_id, f'{label} must be a mapping')
            return
        line_keys = locator.keys() & {
            'line', 'lineno', 'line_number', 'start_line', 'end_line'
        }
        if line_keys:
            self._error(
                artifact_id,
                f'{label} must not use line-number identity: {_format_key_set(line_keys)}'
            )

        kind = locator.get('kind')
        if kind not in _LOCATOR_KINDS:
            self._error(artifact_id, f'{label}.kind is invalid')
            return
        required, optional = self._locator_keys(kind)
        self._check_keys(f'{artifact_id}.{label}', locator, required,
                         required | optional)

        relative_path = locator.get('path')
        path = self._resolve_repo_path(artifact_id, label, relative_path)
        if path is None:
            return
        for key in sorted(required - {'kind', 'path'}):
            if not _is_nonempty_string(locator.get(key)):
                self._error(artifact_id,
                            f'{label}.{key} must be a nonempty string')
        if kind == 'runtime_import' and 'symbol' in locator and not _is_nonempty_string(
                locator.get('symbol')):
            self._error(artifact_id,
                        f'{label}.symbol must be a nonempty string')
        if kind == 'python_ast_pattern' and 'symbol' in locator and not _is_nonempty_string(
                locator.get('symbol')):
            self._error(artifact_id,
                        f'{label}.symbol must be a nonempty string')

        if kind == 'python_ast_pattern' and _is_nonempty_string(
                locator.get('pattern')):
            try:
                _pattern_node(locator['pattern'])
            except (SyntaxError, ValueError) as error:
                self._error(artifact_id, f'{label}.pattern is invalid: {error}')
                return
        if kind == 'file_digest':
            digest = locator.get('sha256')
            if not isinstance(digest,
                              str) or _SHA256_RE.fullmatch(digest) is None:
                self._error(
                    artifact_id,
                    f'{label}.sha256 must be a lowercase 64-hex digest')
                return
        if kind == 'sql_object':
            self._check_sql_locator(artifact_id, label, locator, path,
                                    expectation)
            return
        if expectation is None:
            return

        exists = self._locator_exists(artifact_id, label, locator, path)
        if exists is None:
            return
        if expectation and not exists:
            self._error(artifact_id,
                        f'{label} does not resolve to a present artifact')
        elif not expectation and exists:
            self._error(artifact_id,
                        f'{label} still resolves after status removed')

    def _locator_keys(
        self, kind: str
    ) -> typing.Tuple[typing.FrozenSet[str], typing.FrozenSet[str]]:
        common = {'kind', 'path'}
        extra_required: typing.Set[str] = set()
        extra_optional: typing.Set[str] = set()
        if kind == 'python_symbol':
            extra_required.add('symbol')
        elif kind == 'python_attribute':
            extra_required.update({'symbol', 'attribute'})
        elif kind == 'python_call_within':
            extra_required.update({'symbol', 'call'})
        elif kind == 'python_enum_member':
            extra_required.update({'symbol', 'member'})
        elif kind == 'python_ast_pattern':
            extra_required.add('pattern')
            extra_optional.add('symbol')
        elif kind == 'file_digest':
            extra_required.add('sha256')
        elif kind == 'sql_object':
            extra_required.update({'object_type', 'name'})
        elif kind == 'runtime_metadata':
            extra_required.add('name')
        elif kind == 'runtime_import':
            extra_required.add('module')
            extra_optional.add('symbol')
        elif kind == 'test_node':
            extra_required.add('node')
        return (frozenset(common | extra_required), frozenset(extra_optional))

    def _resolve_repo_path(self, artifact_id: str, label: str,
                           value: typing.Any) -> typing.Optional[pathlib.Path]:
        if not _is_nonempty_string(value):
            self._error(artifact_id, f'{label}.path must be a nonempty string')
            return None
        if _has_glob(value):
            self._error(artifact_id,
                        f'{label}.path must not contain a wildcard')
            return None
        if re.search(r':\d+(?::\d+)?\Z', value):
            self._error(artifact_id,
                        f'{label}.path must not contain a line number')
            return None
        relative = pathlib.PurePosixPath(value)
        if relative.is_absolute() or '..' in relative.parts:
            self._error(artifact_id,
                        f'{label}.path must be repository-relative')
            return None
        path = (self._repo_root / pathlib.Path(*relative.parts)).resolve()
        try:
            path.relative_to(self._repo_root)
        except ValueError:
            self._error(artifact_id, f'{label}.path escapes the repository')
            return None
        return path

    def _locator_exists(self, artifact_id: str, label: str,
                        locator: typing.Mapping[str, typing.Any],
                        path: pathlib.Path) -> typing.Optional[bool]:
        kind = locator['kind']
        if kind in ('path', 'packaged_path'):
            return path.exists()
        if kind == 'file_digest':
            if not path.is_file():
                return False
            try:
                actual = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError as error:
                self._error(artifact_id,
                            f'{label}.path cannot be read: {error}')
                return None
            return actual == locator['sha256']
        if not path.exists():
            return False
        tree = self._read_ast(artifact_id, label, path)
        if tree is None:
            return None
        if kind == 'python_symbol':
            return _resolve_python_symbol(tree, locator['symbol']) is not None
        if kind == 'python_attribute':
            owner = _resolve_python_symbol(tree, locator['symbol'])
            return owner is not None and _contains_attribute(
                owner, locator['attribute'])
        if kind == 'python_call_within':
            owner = _resolve_python_symbol(tree, locator['symbol'])
            return owner is not None and _contains_call(owner, locator['call'])
        if kind == 'python_enum_member':
            enum_node = _resolve_python_symbol(tree, locator['symbol'])
            return enum_node is not None and _contains_enum_member(
                enum_node, locator['member'])
        if kind == 'python_ast_pattern':
            owner = tree
            if 'symbol' in locator:
                resolved_owner = _resolve_python_symbol(tree, locator['symbol'])
                if resolved_owner is None:
                    return False
                owner = resolved_owner
            return _contains_ast_pattern(owner, locator['pattern'])
        if kind == 'runtime_metadata':
            return _contains_runtime_metadata(tree, locator['name'])
        if kind == 'runtime_import':
            return _contains_runtime_import(tree, locator['module'],
                                            locator.get('symbol'))
        if kind == 'test_node':
            node = locator['node'].split('[', 1)[0].replace('::', '.')
            return _resolve_python_symbol(tree, node) is not None
        self._error(artifact_id, f'{label} has unsupported locator kind {kind}')
        return None

    def _read_ast(self, artifact_id: str, label: str,
                  path: pathlib.Path) -> typing.Optional[ast.Module]:
        if path in self._ast_cache:
            return self._ast_cache[path]
        try:
            source = path.read_text(encoding='utf-8')
            tree = ast.parse(source, filename=str(path))
        except (OSError, UnicodeError, SyntaxError) as error:
            self._error(artifact_id,
                        f'{label}.path cannot be parsed as Python: {error}')
            self._ast_cache[path] = None
            return None
        self._ast_cache[path] = tree
        return tree

    def _check_sql_locator(self, artifact_id: str, label: str,
                           locator: typing.Mapping[str, typing.Any],
                           path: pathlib.Path,
                           expectation: typing.Optional[bool]) -> None:
        object_type = locator.get('object_type')
        name = locator.get('name')
        if not _is_nonempty_string(object_type) or _NAME_RE.fullmatch(
                object_type) is None:
            self._error(artifact_id,
                        f'{label}.object_type must be an exact identifier')
        if not _is_nonempty_string(name) or _NAME_RE.fullmatch(name) is None:
            self._error(artifact_id,
                        f'{label}.name must be an exact identifier')
            return
        # A retained historical migration can still name an object after a
        # forward contraction.  Therefore absence from PostgreSQL is proven by
        # the schema gate, not by a source-text absence assertion.
        if expectation is not True:
            return
        if not path.exists():
            self._error(
                artifact_id,
                f'{label} source path does not exist for present SQL object')
            return
        try:
            source = path.read_text(encoding='utf-8')
        except (OSError, UnicodeError) as error:
            self._error(artifact_id, f'{label}.path cannot be read: {error}')
            return
        token = re.compile(
            rf'(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])')
        if token.search(source) is None:
            self._error(artifact_id,
                        f'{label} does not name the present SQL object')

    def _check_retention(self, artifact_id: str,
                         artifact: typing.Mapping[str, typing.Any]) -> None:
        obligation = artifact.get('obligation')
        status = artifact.get('status')
        effective_status = status
        blocker = artifact.get('blocker')
        if status == 'blocked' and isinstance(blocker, dict):
            effective_status = blocker.get('blocked_from_status')
        checksum = artifact.get('checksum_sha256')
        linked = artifact.get('linked_contractions')
        if obligation != 'retain_history':
            if checksum is not None:
                self._error(artifact_id,
                            'checksum_sha256 is legal only for retain_history')
            if linked is not None:
                self._error(
                    artifact_id,
                    'linked_contractions is legal only for retain_history')
            return

        if not isinstance(linked, list) or not linked:
            self._error(artifact_id,
                        'retain_history requires linked_contractions')
        else:
            self._check_string_list(artifact_id, 'linked_contractions', linked)
        physical_paths = self._history_paths(artifact)
        if len(physical_paths) != 1:
            self._error(
                artifact_id,
                'retain_history requires exactly one physical-file locator')
            return
        path = self._resolve_repo_path(artifact_id, 'checksum',
                                       next(iter(physical_paths)))
        if path is None:
            return
        if effective_status == 'planned':
            if checksum is not None:
                self._error(
                    artifact_id,
                    'planned retain_history must not claim a checksum before '
                    'the migration exists')
            if path.exists():
                self._error(
                    artifact_id,
                    'planned retain_history path already exists; mark the '
                    'artifact present with provenance and checksum')
            return
        if not isinstance(checksum,
                          str) or _SHA256_RE.fullmatch(checksum) is None:
            self._error(artifact_id,
                        'retain_history requires a lowercase SHA-256 checksum')
            return
        if not path.is_file():
            self._error(artifact_id,
                        'retained history checksum target is not a file')
            return
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != checksum:
            self._error(
                artifact_id,
                f'retained history checksum mismatch: expected {checksum}, got {actual}'
            )

    def _check_references(self) -> None:
        for artifact_id in sorted(self._artifacts_by_id):
            artifact = self._artifacts_by_id[artifact_id]
            retained_references = artifact.get('retained_references')
            if isinstance(retained_references, list):
                for reference in sorted(item for item in retained_references
                                        if isinstance(item, str)):
                    if _COVERAGE_GAP_ID_RE.fullmatch(reference):
                        if reference not in self._coverage_gap_ids:
                            self._error(
                                artifact_id,
                                f'unknown retained coverage gap {reference!r}')
                    elif _ARTIFACT_ID_RE.fullmatch(reference):
                        if reference not in self._artifacts_by_id:
                            self._error(
                                artifact_id,
                                f'unknown retained artifact {reference!r}')
            dependencies = artifact.get('dependencies')
            if isinstance(dependencies, list):
                history = artifact.get('status_history')
                dependency_gate_status = None
                if isinstance(history, list):
                    dependency_gate_status = next(
                        (item for item in history
                         if item in ('ready_to_remove', 'removal_in_progress',
                                     'removed')), None)
                for dependency in sorted(
                        item for item in dependencies if isinstance(item, str)):
                    if dependency == artifact_id:
                        self._error(artifact_id, 'cannot depend on itself')
                    elif dependency not in self._artifacts_by_id:
                        self._error(artifact_id,
                                    f'unknown dependency {dependency!r}')
                    elif dependency_gate_status is not None:
                        dependency_artifact = self._artifacts_by_id[dependency]
                        expected_status = self._terminal_status(
                            dependency_artifact.get('obligation'))
                        if dependency_artifact.get('status') != expected_status:
                            self._error(
                                artifact_id,
                                f'dependency {dependency!r} must be '
                                f'{expected_status} before '
                                f'{dependency_gate_status}')
            linked = artifact.get('linked_contractions')
            if isinstance(linked, list):
                for linked_id in sorted(
                        item for item in linked if isinstance(item, str)):
                    linked_artifact = self._artifacts_by_id.get(linked_id)
                    if linked_artifact is None:
                        self._error(
                            artifact_id,
                            f'unknown linked contraction {linked_id!r}')
                    elif linked_artifact.get('obligation') != 'must_contract':
                        self._error(
                            artifact_id,
                            f'linked artifact {linked_id!r} is not must_contract'
                        )
                    elif not self._history_and_contraction_are_linked(
                            artifact_id, artifact, linked_artifact):
                        self._error(
                            artifact_id,
                            f'linked contraction {linked_id!r} does not own a '
                            'SQL object in the retained migration path')

    def _terminal_status(self, obligation: typing.Any) -> str:
        if obligation in ('retain_history', 'retain_characterization'):
            return 'retained_verified'
        return 'removed'

    def _history_paths(
            self, artifact: typing.Mapping[str, typing.Any]) -> typing.Set[str]:
        locators = artifact.get('locators')
        if not isinstance(locators, list):
            return set()
        return {
            locator['path']
            for locator in locators
            if isinstance(locator, dict) and locator.get('kind') in (
                'path',
                'packaged_path') and _is_nonempty_string(locator.get('path'))
        }

    def _sql_paths(
            self, artifact: typing.Mapping[str, typing.Any]) -> typing.Set[str]:
        locators = artifact.get('locators')
        if not isinstance(locators, list):
            return set()
        return {
            locator['path']
            for locator in locators
            if isinstance(locator, dict) and locator.get('kind') == 'sql_object'
            and _is_nonempty_string(locator.get('path'))
        }

    def _history_and_contraction_are_linked(
            self, history_id: str, history: typing.Mapping[str, typing.Any],
            contraction: typing.Mapping[str, typing.Any]) -> bool:
        history_paths = self._history_paths(history)
        if history_paths & self._sql_paths(contraction):
            return True
        retained_references = contraction.get('retained_references')
        if not isinstance(retained_references, list):
            return False
        return history_id in retained_references or bool(
            history_paths & set(retained_references))

    def _check_history_reverse_links(self) -> None:
        retained_by_path: typing.Dict[str, typing.List[typing.Tuple[
            str, typing.Mapping[str, typing.Any]]]] = {}
        for artifact_id, artifact in self._artifacts_by_id.items():
            if artifact.get('obligation') != 'retain_history':
                continue
            for path in self._history_paths(artifact):
                retained_by_path.setdefault(path, []).append(
                    (artifact_id, artifact))

        for path, owners in sorted(retained_by_path.items()):
            if len(owners) > 1:
                owner_ids = ', '.join(sorted(
                    owner_id for owner_id, _ in owners))
                self._error(
                    owners[0][0],
                    f'migration path {path!r} has multiple retain_history '
                    f'owners: {owner_ids}')

        for artifact_id, artifact in sorted(self._artifacts_by_id.items()):
            if artifact.get('obligation') != 'must_contract':
                continue
            if not self._sql_paths(artifact):
                continue
            linked_histories = []
            for history_id, history in self._artifacts_by_id.items():
                if history.get('obligation') != 'retain_history':
                    continue
                if artifact_id not in history.get('linked_contractions', []):
                    continue
                if self._history_and_contraction_are_linked(
                        history_id, history, artifact):
                    linked_histories.append(history_id)
            if not linked_histories:
                self._error(
                    artifact_id,
                    'SQL contraction requires a retain_history owner linked '
                    'back to this artifact and its migration provenance')
            elif len(linked_histories) > 1:
                self._error(
                    artifact_id,
                    'SQL contraction must have exactly one retain_history '
                    'owner; found ' + ', '.join(sorted(linked_histories)))

    def _check_dependency_cycles(self) -> None:
        visited: typing.Set[str] = set()
        active: typing.Set[str] = set()

        def visit(artifact_id: str, path: typing.List[str]) -> None:
            if artifact_id in active:
                cycle_start = path.index(artifact_id)
                cycle = path[cycle_start:] + [artifact_id]
                self._error(artifact_id,
                            f'dependency cycle: {" -> ".join(cycle)}')
                return
            if artifact_id in visited:
                return
            active.add(artifact_id)
            artifact = self._artifacts_by_id[artifact_id]
            dependencies = artifact.get('dependencies')
            if isinstance(dependencies, list):
                for dependency in sorted(item for item in dependencies
                                         if item in self._artifacts_by_id):
                    visit(dependency, path + [artifact_id])
            active.remove(artifact_id)
            visited.add(artifact_id)

        for artifact_id in sorted(self._artifacts_by_id):
            visit(artifact_id, [])


def check_manifest(
        manifest_path: pathlib.Path,
        phase: str,
        repo_root: typing.Optional[pathlib.Path] = None) -> typing.List[str]:
    """Return deterministic validation errors for a manifest."""
    if phase not in ('current', 'final'):
        return [f'manifest: invalid phase {phase!r}']
    if repo_root is None:
        repo_root = pathlib.Path(__file__).resolve().parents[1]
    try:
        document = yaml.load(manifest_path.read_text(encoding='utf-8'),
                             Loader=_UniqueKeyLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        return [f'manifest: cannot load {manifest_path}: {error}']
    return ManifestChecker(repo_root, phase).check(document)


def _parse_args(
        argv: typing.Optional[typing.Sequence[str]] = None
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Validate the provider lifecycle removal manifest.')
    parser.add_argument('--manifest',
                        required=True,
                        type=pathlib.Path,
                        help='Path to the YAML removal manifest.')
    parser.add_argument('--phase',
                        required=True,
                        choices=('current', 'final'),
                        help='Validation phase.')
    return parser.parse_args(argv)


def main(argv: typing.Optional[typing.Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    errors = check_manifest(args.manifest, args.phase)
    if errors:
        for error in errors:
            print(f'ERROR: {error}', file=sys.stderr)
        return 1
    print(f'Lifecycle removal manifest passes {args.phase} validation.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
