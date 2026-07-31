"""Goldens and invariants for bounded C2 evidence hashing."""

import dataclasses
import hashlib

import pytest

from sky.physical_capacity import canonical
from sky.physical_capacity import contracts
from sky.physical_capacity import hashing
from sky.physical_capacity import models


def _selectors() -> tuple[contracts.SourceSelector, ...]:
    return (
        contracts.ServeSourceSelector('default',
                                      models.ProjectionSourceKind.SERVE_SERVICE,
                                      'svc'),
        contracts.ServeSourceSelector('default',
                                      models.ProjectionSourceKind.SERVE_POOL,
                                      'pool'),
        contracts.ManagedJobTaskSelector('default', 42, 0),
        contracts.ServeSourceSelector('other',
                                      models.ProjectionSourceKind.SERVE_POOL,
                                      'other-pool'),
    )


def _evidence() -> tuple[contracts.EvidenceRecord, ...]:
    return (
        contracts.GroupEvidenceRecord(
            source_incarnation_hash='b' * 64,
            confidence=contracts.EvidenceGroupConfidence.EXACT,
            lifecycle=contracts.EvidenceLifecycle.ACTIVE,
            status_class=contracts.EvidenceStatusClass.PRESENT),
        contracts.AllocationCandidateEvidenceRecord(
            source_incarnation_hash='a' * 64,
            group_source_incarnation_hash='b' * 64,
            identity_confidence=contracts.EvidenceIdentityConfidence.LEGACY,
            association_status=contracts.EvidenceAssociationStatus.
            REGISTRY_HASH,
            desired_state=contracts.EvidenceDesiredState.PRESENT,
            observed_state=contracts.EvidenceObservedState.UP,
            scalar_placement_hash='c' * 64),
    )


def test_new_canonical_domains_are_closed_and_separated() -> None:
    payload = {'mapping_version': 1, 'value': 'same'}
    scope = canonical.canonical_hash(
        payload, domain=canonical.CanonicalDomain.SCOPE_ENTRY)
    evidence = canonical.canonical_hash(
        payload, domain=canonical.CanonicalDomain.EVIDENCE_RECORD)
    assert scope != evidence
    assert {
        canonical.CanonicalDomain.SCOPE_ENTRY.value,
        canonical.CanonicalDomain.EVIDENCE_RECORD.value
    } == {'scope_entry', 'evidence_record'}


def test_partition_hash_and_jitter_goldens() -> None:
    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.MANAGED_JOB_TASK)
    assert hashing.source_partition_hash(partition) == (
        '8bada7b072d5504b79521497e4f2fb4d2181f5722b5e9b7f683c2c713b6212a4')
    assert hashing.slot_jitter_seconds(
        hashing.source_partition_hash(partition)) == 41


def test_managed_partition_dependencies_include_only_same_workspace_pools(
) -> None:
    selectors = _selectors()
    managed = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.MANAGED_JOB_TASK)
    dependencies = hashing.dependency_selectors_for_partition(
        tuple(reversed(selectors)), managed)
    assert set(dependencies) == {selectors[1], selectors[2]}

    service = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.SERVE_SERVICE)
    assert hashing.dependency_selectors_for_partition(
        selectors, service) == (selectors[0],)


def test_scope_hash_is_order_independent_and_dependency_sensitive() -> None:
    selectors = _selectors()
    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.MANAGED_JOB_TASK)
    kwargs = {
        'owner_kinds':
            (models.OwnerKind.MANAGED_JOB_TASK, models.OwnerKind.POOL),
        'providers': ('aws', 'gcp'),
        'groups': (),
        'verbs': ('observe',),
    }
    digest = hashing.projection_scope_hash(partition, selectors,
                                           '2026-08-31T00:00:00Z', **kwargs)
    permuted = hashing.projection_scope_hash(
        partition,
        tuple(reversed(selectors)),
        '2026-08-31T00:00:00Z',
        owner_kinds=tuple(reversed(kwargs['owner_kinds'])),
        providers=tuple(reversed(kwargs['providers'])),
        groups=(),
        verbs=('observe',))
    assert digest == permuted == (
        '51537ced37bbd47ab9f48ce40d810df03e4dcb80f20d0dc5f1e497a6e87ab6c0')

    unrelated_service = selectors + (contracts.ServeSourceSelector(
        'other', models.ProjectionSourceKind.SERVE_SERVICE, 'ignored'),)
    assert hashing.projection_scope_hash(partition, unrelated_service,
                                         '2026-08-31T00:00:00Z',
                                         **kwargs) == digest
    changed_pool = tuple(
        selector for selector in selectors if selector != selectors[1]) + (
            contracts.ServeSourceSelector(
                'default', models.ProjectionSourceKind.SERVE_POOL, 'pool-v2'),)
    assert hashing.projection_scope_hash(partition, changed_pool,
                                         '2026-08-31T00:00:00Z',
                                         **kwargs) != digest


def test_scan_phase_and_persisted_error_codes_are_closed() -> None:
    assert {member.value for member in models.ProjectionScanPhase
           } == {'full_snapshot'}
    assert {member.value for member in models.ProjectionScanErrorCode} == {
        'row_limit_exceeded',
        'byte_limit_exceeded',
        'scan_timeout',
        'source_decode_failed',
        'source_conflict',
        'selector_mismatch',
        'source_index_missing',
        'non_colocated_source_store',
        'controller_fenced',
        'serialization_exhausted',
        'database_unavailable',
        'database_statement_failed',
        'stale_scan',
    }


def test_scope_payload_has_compact_component_count_and_hashes() -> None:
    selectors = _selectors()
    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.SERVE_SERVICE)
    payload = hashing.projection_scope_payload(
        partition,
        selectors,
        '2026-08-31T00:00:00Z',
        owner_kinds=(models.OwnerKind.SERVICE,))
    assert payload['source_selectors'] == {
        'count': 1,
        'hash': '52179a4c071f029552dcdf402e1167deb9747fb58e26a4b5a1f9775950f614e5',
    }
    assert payload['providers']['count'] == 0
    assert 'service_name' not in str(payload)


@pytest.mark.parametrize('value', [
    '٢٠٢٦-08-31T00:00:00Z',
    '２０２６-08-31T00:00:00Z',
])
def test_scope_hash_rejects_non_ascii_timestamp_digits(value: str) -> None:
    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.SERVE_SERVICE)
    with pytest.raises(ValueError, match='YYYY-MM-DD'):
        hashing.projection_scope_hash(partition, _selectors(), value)


def test_evidence_digest_is_stable_order_independent_and_duplicate_preserving(
) -> None:
    records = _evidence()
    digest = hashing.evidence_inventory_digest(records)
    assert digest == (
        'e093bacaa31df58019afe1a8a7a1edb64714e1fd033586697743bcd4f4366069')
    assert hashing.evidence_inventory_digest(tuple(reversed(records))) == digest
    assert hashing.evidence_inventory_digest(records + (records[0],)) != digest
    assert hashing.evidence_inventory_digest(()) == (
        '45f6da25c7dfd4e094364223fc62fe5a7b91d4df8f329c30a8271e59496ad3ee')


def test_workspace_metric_hash_uses_exact_domain_prefix() -> None:
    expected = hashlib.sha256(
        b'skypilot-capacity-workspace-metric-v1\x00default').hexdigest()
    assert hashing.workspace_metric_hash('default') == expected
    assert hashing.workspace_metric_hash('default') != hashlib.sha256(
        b'default').hexdigest()


def test_evidence_dtos_emit_only_closed_payloads() -> None:
    group, allocation = _evidence()
    assert group.to_payload() == {
        'mapping_version': 1,
        'record_type': 'group',
        'source_incarnation_hash': 'b' * 64,
        'confidence': 'exact',
        'lifecycle': 'active',
        'status_class': 'present',
    }
    assert allocation.to_payload()['association_status'] == 'registry_hash'
    with pytest.raises(ValueError, match='lowercase SHA-256'):
        dataclasses.replace(group, source_incarnation_hash='not-a-hash')


def _valid_findings() -> contracts.FindingCounts:
    return contracts.FindingCounts(source_rows=7,
                                   selectors_present=2,
                                   selectors_missing=1,
                                   groups_exact=1,
                                   groups_legacy=1,
                                   allocation_candidates=2,
                                   allocations_legacy=1,
                                   allocations_unknown=1,
                                   identity_gap=2,
                                   scalar_placement_known=1,
                                   selected_spec_gap=2,
                                   desired_present=1,
                                   desired_absent=1)


def test_finding_counts_close_all_required_arithmetic() -> None:
    findings = _valid_findings()
    findings.validate(configured_selectors=3)
    assert tuple(findings.to_dict()) == tuple(
        member.value for member in contracts.FindingKey)
    group, allocation = _evidence()
    second_group = dataclasses.replace(group, source_incarnation_hash='d' * 64)
    second_allocation = dataclasses.replace(allocation,
                                            source_incarnation_hash='e' * 64,
                                            group_source_incarnation_hash='d' *
                                            64)
    result = contracts.PartitionEvidenceResult(records=(group, second_group,
                                                        allocation,
                                                        second_allocation),
                                               findings=findings,
                                               rows_seen=9)
    result.validate(configured_selectors=3)


@pytest.mark.parametrize('field,value,match', [
    ('selectors_missing', 0, 'Selector'),
    ('groups_unknown', 1, 'Group'),
    ('allocations_exact', 1, 'Allocation'),
    ('desired_unknown', 1, 'Desired-state'),
    ('identity_gap', 1, 'Identity-gap'),
    ('selected_spec_gap', 1, 'Selected-spec-gap'),
    ('scalar_placement_known', 2, 'Scalar placement'),
])
def test_finding_counts_reject_each_broken_invariant(field: str, value: int,
                                                     match: str) -> None:
    findings = _valid_findings()
    setattr(findings, field, value)
    with pytest.raises(ValueError, match=match):
        findings.validate(configured_selectors=3)


def test_finding_counts_reject_boolean_and_unknown_increment() -> None:
    findings = contracts.FindingCounts()
    with pytest.raises(ValueError, match='integer'):
        findings.increment(contracts.FindingKey.SOURCE_ROWS, True)
    with pytest.raises(ValueError, match='Unknown finding'):
        findings.increment('not_a_finding')
