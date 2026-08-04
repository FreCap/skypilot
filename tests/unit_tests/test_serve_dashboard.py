"""Tests for bounded direct SkyServe dashboard reads."""

# pylint: disable=protected-access
import base64
import contextlib
import json
from unittest import mock

import pytest
from sqlalchemy.dialects import postgresql

from sky.serve import serve_dashboard
from sky.serve import spot_placer


def _compiled(statement) -> str:
    return str(
        statement.compile(dialect=postgresql.dialect(),
                          compile_kwargs={'literal_binds': True}))


def _replica_row(replica_id: int, status: str | None = 'READY') -> dict:
    return {
        'replica_id': replica_id,
        'status': status,
        'version': 3,
        'cluster_name': f'svc-{replica_id}',
        'created_at': 100.0,
        'is_spot': True,
        'replica_state_version': 1,
        'replica_state': {
            'planned_capacity': 4,
            'is_zero_cost': False,
            'replica_record_id': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
            'location': {
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': 'us-east-1a',
                'use_spot': True,
                'instance_type': 'g6.4xlarge',
                'accelerators': {
                    'L4': 1
                },
            },
            'status_property': {
                'first_ready_time': 130.0,
            },
        },
    }


def _catalog(*, cost: float = 1.25, num_nodes: int | None = 2) -> dict:
    location = spot_placer.Location.from_pickleable({
        'cloud': 'AWS',
        'region': 'us-east-1',
        'zone': 'us-east-1a',
        'use_spot': True,
        'instance_type': 'g6.4xlarge',
        'accelerators': {
            'L4': 1
        },
    })
    assert location is not None
    return spot_placer.PlacementCatalog(((location, cost),),
                                        num_nodes=num_nodes).to_dict()


def _catalog_metadata(catalog: dict, version: int = 3) -> dict:
    return {
        'version': version,
        'catalog_type': 'object',
        'schema_type': 'number',
        'schema_text': '1',
        'entries_type': 'array',
        'entry_count': len(catalog['entries']),
        'catalog_bytes': len(json.dumps(catalog)),
    }


def _catalog_resolver(
    catalog: dict | spot_placer.PlacementCatalog,
) -> serve_dashboard._PricingCatalogResolver:
    if isinstance(catalog, dict):
        catalog = spot_placer.PlacementCatalog.from_dict(catalog)
    return serve_dashboard._PricingCatalogResolver.from_catalog(catalog)


def _pricing_group(**overrides) -> dict:
    row = {
        'version': 3,
        'status': 'READY',
        'is_spot': True,
        'is_zero_cost': False,
        'pricing_identity_too_large': False,
        'location': _replica_row(1)['replica_state']['location'],
        'resources_override': None,
        'physical_count': 1,
    }
    row.update(overrides)
    return row


def _pricing_row(replica_id: int, **overrides) -> dict:
    replica = _replica_row(replica_id)
    state = replica['replica_state']
    row = {
        'replica_id': replica_id,
        'status': replica['status'],
        'version': replica['version'],
        'cluster_name': replica['cluster_name'],
        'created_at': replica['created_at'],
        'is_spot': replica['is_spot'],
        'is_zero_cost': state['is_zero_cost'],
        'pricing_identity_too_large': False,
        'location': state['location'],
        'resources_override': None,
        'replica_record_id': state['replica_record_id'],
    }
    row.update(overrides)
    return row


class _Result:
    """Minimal SQLAlchemy result stub."""

    def __init__(self, *, row=None, scalar=None, rows=None):
        self._row = row
        self._scalar = scalar
        self._rows = [] if rows is None else rows

    def fetchone(self):
        return self._row

    def scalar_one(self):
        return self._scalar

    def fetchall(self):
        return self._rows


class _Connection:
    """Ordered-result connection stub that records statements."""

    def __init__(self, results):
        self.results = list(results)
        self.statements = []

    def begin(self):
        return contextlib.nullcontext()

    def execute(self, statement):
        self.statements.append(statement)
        return self.results.pop(0)


def _cursor_with(payload: dict) -> str:
    raw = json.dumps(payload, separators=(',', ':')).encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip('=')
    return 'v1.' + encoded


def test_summary_query_is_one_compact_grouped_scan():
    sql = _compiled(serve_dashboard._replica_summary_query(['svc-a', 'svc-b']))

    assert 'LEFT OUTER JOIN replicas' in sql
    assert 'GROUP BY services.name' in sql
    assert "services.name IN ('svc-a', 'svc-b')" in sql
    assert 'replicas.replica_state' in sql
    assert 'replicas.replica_info' not in sql


def test_summary_counts_physical_attempts_separately_from_capacity():
    rows = [{
        'name': 'svc',
        'hash': 'hash-a',
        'logical_replica_semantics': 1,
        'status': 'READY',
        'physical_count': 2,
        'capacity_count': 8,
    }, {
        'name': 'svc',
        'hash': 'hash-a',
        'logical_replica_semantics': 1,
        'status': 'FAILED_PROVISION',
        'physical_count': 3,
        'capacity_count': 12,
    }, {
        'name': 'svc',
        'hash': 'hash-a',
        'logical_replica_semantics': 1,
        'status': None,
        'physical_count': 1,
        'capacity_count': 4,
    }]

    assert serve_dashboard._build_replica_summaries(rows) == [{
        'service_name': 'svc',
        'service_hash': 'hash-a',
        'replica_unit': 'logical_slot',
        'replica_status_counts': {
            'READY': 2,
            'FAILED_PROVISION': 3,
            'UNKNOWN': 1,
        },
        'replica_capacity_counts': {
            'READY': 8,
            'FAILED_PROVISION': 12,
            'UNKNOWN': 4,
        },
        'current_or_uncertain_count': 3,
        'past_attempt_count': 3,
    }]


def test_replica_summaries_execute_one_batched_query(monkeypatch):
    connection = _Connection([
        _Result(rows=[{
            'name': 'svc',
            'hash': 'hash-a',
            'logical_replica_semantics': 0,
            'status': 'READY',
            'physical_count': 1,
            'capacity_count': 1,
        }])
    ])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(connection))
    monkeypatch.setattr(serve_dashboard.time, 'time', lambda: 42.0)

    result = serve_dashboard.get_replica_summaries(['svc'])

    assert len(connection.statements) == 1
    assert result['observed_at'] == 42.0
    assert result['summaries'][0]['service_name'] == 'svc'


def test_scope_predicates_keep_unknown_and_future_states_current():
    current_sql = _compiled(
        serve_dashboard._scope_predicate(
            serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE))
    past_sql = _compiled(
        serve_dashboard._scope_predicate(serve_dashboard.PAST_ATTEMPTS_SCOPE))

    assert 'replicas.status IS NULL' in current_sql
    assert 'NOT IN' in current_sql
    for status in serve_dashboard.PAST_ATTEMPT_STATUSES:
        assert status in current_sql
        assert status in past_sql
    assert 'FAILED_CLEANUP' not in past_sql
    assert 'UNKNOWN' not in past_sql
    assert 'PREEMPTED' not in past_sql


def test_page_query_filters_and_bounds_before_compact_state_decode():
    query = serve_dashboard._replica_page_query(
        'svc', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 50, 120, 75)
    sql = _compiled(query)

    assert 'replicas.replica_id <= 120' in sql
    assert 'replicas.replica_id < 75' in sql
    assert 'replicas.status IS NULL' in sql
    assert 'ORDER BY replicas.replica_id DESC' in sql
    assert 'LIMIT 51' in sql
    assert 'replicas.replica_state' in sql
    assert 'replicas.replica_info' not in sql


def test_cursor_round_trip_allows_limit_one_first_page():
    cursor = serve_dashboard._encode_cursor(
        'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 10, 10)

    assert serve_dashboard._decode_cursor(
        cursor, 'hash-a',
        serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE) == (10, 10)


@pytest.mark.parametrize('payload', [{
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': 'current_or_uncertain',
}, {
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 2,
}, {
    'hash': '',
    'last': 9,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': 11,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': True,
    'max': 10,
    'scope': 'current_or_uncertain',
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': [],
    'version': 1,
}, {
    'hash': 'hash-a',
    'last': 9,
    'max': 10,
    'scope': 'invalid',
    'version': 1,
}])
def test_cursor_rejects_malformed_payload(payload):
    with pytest.raises(serve_dashboard.InvalidReplicaCursorError):
        serve_dashboard._decode_cursor(
            _cursor_with(payload), 'hash-a',
            serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE)


def test_cursor_rejects_excessively_nested_json():
    raw = ('{"hash":"hash-a","last":1,"max":1,"scope":' + '[' * 1000 + '0' +
           ']' * 1000 + ',"version":1}').encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip('=')

    with pytest.raises(serve_dashboard.InvalidReplicaCursorError):
        serve_dashboard._decode_cursor(
            f'v1.{encoded}', 'hash-a',
            serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE)


@pytest.mark.parametrize(('service_hash', 'scope'), [
    ('hash-b', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE),
    ('hash-a', serve_dashboard.PAST_ATTEMPTS_SCOPE),
])
def test_cursor_rejects_other_incarnation_or_scope(service_hash, scope):
    cursor = serve_dashboard._encode_cursor(
        'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 10, 8)

    with pytest.raises(serve_dashboard.ReplicaCursorMismatchError):
        serve_dashboard._decode_cursor(cursor, service_hash, scope)


def test_replica_serialization_is_lightweight_and_handle_free():
    replica = serve_dashboard._serialize_replica_row(_replica_row(7))

    assert replica == {
        'replica_id': 7,
        'pricing_fingerprint': serve_dashboard._pricing_fingerprint(
            _replica_row(7)),
        'status': 'READY',
        'version': 3,
        'planned_capacity': 4,
        'is_spot': True,
        'created_at': 100.0,
        'launched_at': None,
        'ready_at': 130.0,
        'time_to_ready_seconds': 30.0,
        'cloud': 'AWS',
        'region': 'us-east-1',
        'zone': 'us-east-1a',
        'infra': 'AWS (us-east-1)',
        'resources_str': 'g6.4xlarge; L4:1',
        'resources_str_full': 'g6.4xlarge; L4:1',
        'instance_type': 'g6.4xlarge',
        'accelerators': {
            'L4': 1
        },
    }
    assert not ({'handle', 'endpoint', 'hourly_cost'} & replica.keys())


def test_replica_page_uses_snapshot_max_in_followup_cursor(monkeypatch):
    first_connection = _Connection([
        _Result(row=('hash-a', 1)),
        _Result(scalar=105),
        _Result(scalar=3),
        _Result(rows=[
            _replica_row(105),
            _replica_row(100, 'FAILED_CLEANUP'),
            _replica_row(90, 'UNKNOWN'),
        ]),
    ])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(
        serve_dashboard, '_repeatable_read_connection',
        lambda _engine: contextlib.nullcontext(first_connection))

    first = serve_dashboard.get_replica_page(
        'svc', 'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 2, None)

    assert [row['replica_id'] for row in first['replicas']] == [105, 100]
    assert first['total'] == 3
    assert first['replica_unit'] == 'logical_slot'
    assert first['next_cursor'] is not None
    assert serve_dashboard._decode_cursor(
        first['next_cursor'], 'hash-a',
        serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE) == (105, 100)

    next_connection = _Connection([
        _Result(row=('hash-a', 1)),
        _Result(scalar=3),
        _Result(rows=[_replica_row(90, 'UNKNOWN')]),
    ])
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(next_connection))

    second = serve_dashboard.get_replica_page(
        'svc', 'hash-a', serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE, 2,
        first['next_cursor'])

    assert [row['replica_id'] for row in second['replicas']] == [90]
    assert second['next_cursor'] is None
    assert len(next_connection.statements) == 3
    page_sql = _compiled(next_connection.statements[-1])
    assert 'replicas.replica_id <= 105' in page_sql
    assert 'replicas.replica_id < 100' in page_sql


@pytest.mark.parametrize(('identity', 'error'), [
    (None, serve_dashboard.ServiceNotFoundError),
    (('hash-b', 0), serve_dashboard.ServiceHashMismatchError),
])
def test_replica_page_fences_service_identity(monkeypatch, identity, error):
    connection = _Connection([_Result(row=identity)])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(connection))

    with pytest.raises(error):
        serve_dashboard.get_replica_page('svc', 'hash-a',
                                         serve_dashboard.PAST_ATTEMPTS_SCOPE,
                                         50, None)

    assert len(connection.statements) == 1


def test_pricing_queries_bound_rows_groups_and_json_before_transfer():
    probe_sql = _compiled(serve_dashboard._pricing_row_probe_query('svc'))
    group_sql = _compiled(serve_dashboard._pricing_group_query('svc'))
    row_sql = _compiled(serve_dashboard._pricing_replica_query('svc', [1]))
    metadata_sql = _compiled(serve_dashboard._catalog_metadata_query(
        'svc', [3]))
    body_sql = _compiled(serve_dashboard._catalog_body_query('svc', [3]))

    assert 'SELECT replicas.replica_id' in probe_sql
    assert 'replicas.replica_state' not in probe_sql
    assert 'LIMIT 10001' in probe_sql
    assert 'LIMIT 4097' in group_sql
    assert 'octet_length' in group_sql
    assert 'CASE WHEN' in group_sql
    assert 'coalesce' in group_sql
    assert 'replicas.replica_info' not in group_sql
    assert 'PENDING' in group_sql
    assert 'PREEMPTED' in group_sql
    assert 'octet_length' in metadata_sql
    assert 'jsonb_array_length' in metadata_sql
    assert 'version_specs.placement_catalog AS placement_catalog' not in (
        metadata_sql)
    assert 'CASE WHEN' in row_sql
    assert 'coalesce' in row_sql
    assert 'CAST(version_specs.placement_catalog AS TEXT)' in body_sql


def test_explicit_zero_fingerprint_bypasses_location_identity():
    row = _replica_row(7)
    row['replica_state']['is_zero_cost'] = True
    row['replica_state']['location'] = {'oversized': 'x' * 70_000}
    first = serve_dashboard._pricing_fingerprint(row)
    row['replica_state']['location'] = {'different': 'y' * 70_000}

    assert first is not None
    assert serve_dashboard._pricing_fingerprint(row) == first


def test_pricing_identity_byte_cap_is_inclusive(monkeypatch):
    state = {
        'location': {
            'value': 'abc'
        },
        'resources_override': {
            'value': 'def'
        },
    }
    serialized_size = sum(
        len(
            json.dumps(state[key],
                       allow_nan=False,
                       ensure_ascii=False,
                       separators=(', ', ': '),
                       sort_keys=True).encode())
        for key in ('location', 'resources_override'))

    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_IDENTITY_BYTES',
                        serialized_size)
    assert not serve_dashboard._pricing_identity_is_too_large(state)

    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_IDENTITY_BYTES',
                        serialized_size - 1)
    assert serve_dashboard._pricing_identity_is_too_large(state)


def test_fingerprint_changes_for_record_version_and_price_inputs():
    row = _replica_row(7)
    baseline = serve_dashboard._pricing_fingerprint(row)
    row['version'] = 4
    assert serve_dashboard._pricing_fingerprint(row) != baseline
    row['version'] = 3
    row['is_spot'] = False
    assert serve_dashboard._pricing_fingerprint(row) != baseline
    row['is_spot'] = True
    row['replica_state']['replica_record_id'] = (
        'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb')
    assert serve_dashboard._pricing_fingerprint(row) != baseline


def test_catalog_price_uses_version_node_count_not_planned_capacity():
    catalog = spot_placer.PlacementCatalog.from_dict(
        _catalog(cost=1.25, num_nodes=2))
    row = _pricing_group(physical_count=4)

    cost, source, reason = serve_dashboard._resolve_persisted_price(
        row, {3: _catalog_resolver(catalog)})

    assert (cost, source, reason) == (2.5, 'version_catalog', None)


def test_legacy_positive_price_requires_node_count_but_zero_does_not():
    legacy_positive = spot_placer.PlacementCatalog.from_dict(
        _catalog(cost=1.25, num_nodes=None))
    legacy_zero = spot_placer.PlacementCatalog.from_dict(
        _catalog(cost=0.0, num_nodes=None))

    assert serve_dashboard._resolve_persisted_price(
        _pricing_group(),
        {3: _catalog_resolver(legacy_positive)}) == (None, None,
                                                     'unknown_node_count')
    assert serve_dashboard._resolve_persisted_price(
        _pricing_group(),
        {3: _catalog_resolver(legacy_zero)}) == (0.0, 'version_catalog', None)


def test_huge_node_count_fails_closed_in_aggregate_and_id_modes():
    catalog = _catalog(cost=1.25, num_nodes=10**1000)
    aggregate_connection = _Connection([
        _Result(rows=[{
            'replica_id': 1
        }]),
        _Result(rows=[_pricing_group()]),
        _Result(rows=[_catalog_metadata(catalog)]),
        _Result(rows=[{
            'version': 3,
            'placement_catalog_text': json.dumps(catalog),
        }]),
    ])

    aggregate = serve_dashboard._aggregate_pricing(aggregate_connection, 'svc')

    assert aggregate['coverage'] == 'none'
    assert aggregate['exclusion_reasons'] == {'catalog_price_unavailable': 1}

    row_connection = _Connection([
        _Result(rows=[_pricing_row(7)]),
        _Result(rows=[_catalog_metadata(catalog)]),
        _Result(rows=[{
            'version': 3,
            'placement_catalog_text': json.dumps(catalog),
        }]),
    ])

    rows = serve_dashboard._replica_pricing(row_connection, 'svc', [7])

    assert rows[0]['hourly_cost'] is None
    assert rows[0]['hourly_cost_exclusion_reason'] == (
        'catalog_price_unavailable')


@pytest.mark.parametrize('hostile_field', ['schema_version', 'hourly_cost'])
def test_hostile_catalog_numeric_fails_closed_without_loading_error(
        hostile_field):
    catalog = _catalog()
    metadata = _catalog_metadata(catalog)
    if hostile_field == 'schema_version':
        metadata['schema_text'] = '9' * 5000
        connection = _Connection([_Result(rows=[metadata])])
    else:
        catalog['entries'][0]['hourly_cost'] = 10**1000
        connection = _Connection([
            _Result(rows=[metadata]),
            _Result(rows=[{
                'version': 3,
                'placement_catalog_text': json.dumps(catalog),
            }]),
        ])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3], aggregate_mode=False)

    assert not too_large
    assert catalogs == {3: 'invalid_version_catalog'}


def test_explicit_zero_precedes_catalog_location_and_purchase_validation():
    row = _pricing_group(is_zero_cost=True,
                         version=None,
                         is_spot=None,
                         location=None)

    assert serve_dashboard._resolve_persisted_price(
        row, {}) == (0.0, 'zero_cost_provenance', None)


@pytest.mark.parametrize(('overrides', 'reason'), [
    ({
        'location': None,
        'resources_override': None
    }, 'missing_location'),
    ({
        'location': 'corrupt'
    }, 'invalid_location'),
    ({
        'location': {
            'cloud': 123,
            'region': 'us-east-1',
            'zone': None,
        }
    }, 'invalid_location'),
    ({
        'is_spot': False
    }, 'purchase_option_mismatch'),
    ({
        'pricing_identity_too_large': True,
        'location': None
    }, 'pricing_identity_too_large'),
])
def test_persisted_price_fails_closed_with_stable_reason(overrides, reason):
    catalog = spot_placer.PlacementCatalog.from_dict(_catalog())

    assert serve_dashboard._resolve_persisted_price(
        _pricing_group(**overrides),
        {3: _catalog_resolver(catalog)}) == (None, None, reason)


def test_malformed_row_location_shape_fails_closed():
    catalog = _catalog_resolver(_catalog())
    location = dict(_pricing_group()['location'])
    location['accelerators'] = 'malformed'

    assert serve_dashboard._resolve_persisted_price(
        _pricing_group(location=location),
        {3: catalog}) == (None, None, 'invalid_location')


def test_catalog_location_index_is_built_once_per_loaded_version(monkeypatch):
    catalog = _catalog()
    original = spot_placer.CatalogLocationIndex.from_locations
    build_count = 0

    def _tracked_build(cls, candidates):
        del cls
        nonlocal build_count
        build_count += 1
        return original(candidates)

    monkeypatch.setattr(spot_placer.CatalogLocationIndex, 'from_locations',
                        classmethod(_tracked_build))
    connection = _Connection([
        _Result(rows=[_catalog_metadata(catalog)]),
        _Result(rows=[{
            'version': 3,
            'placement_catalog_text': json.dumps(catalog),
        }]),
    ])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3], aggregate_mode=False)
    first = serve_dashboard._resolve_persisted_price(_pricing_group(), catalogs)
    second = serve_dashboard._resolve_persisted_price(_pricing_group(),
                                                      catalogs)

    assert not too_large
    assert first == second == (2.5, 'version_catalog', None)
    assert build_count == 1


def test_malformed_catalog_location_shape_fails_closed():
    catalog = _catalog()
    catalog['entries'][0]['location']['accelerators'] = 'malformed'
    connection = _Connection([
        _Result(rows=[_catalog_metadata(catalog)]),
        _Result(rows=[{
            'version': 3,
            'placement_catalog_text': json.dumps(catalog),
        }]),
    ])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3], aggregate_mode=False)

    assert not too_large
    assert catalogs == {3: 'invalid_version_catalog'}


def test_catalog_metadata_classifies_without_loading_invalid_bodies():
    connection = _Connection([
        _Result(rows=[{
            'version': 3,
            'catalog_type': 'object',
            'schema_type': 'number',
            'schema_text': '2',
            'entries_type': 'array',
            'entry_count': 1,
            'catalog_bytes': 100,
        }, {
            'version': 4,
            'catalog_type': 'array',
            'schema_type': None,
            'schema_text': None,
            'entries_type': None,
            'entry_count': None,
            'catalog_bytes': 100,
        }]),
    ])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3, 4, 5], aggregate_mode=False)

    assert not too_large
    assert catalogs == {
        3: 'unsupported_version_catalog',
        4: 'invalid_version_catalog',
        5: 'missing_version_catalog',
    }
    assert len(connection.statements) == 1


def test_catalog_byte_cap_fails_aggregate_without_loading_body(monkeypatch):
    catalog = _catalog()
    metadata = _catalog_metadata(catalog)
    metadata['catalog_bytes'] = 101
    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_CATALOG_BYTES', 100)
    connection = _Connection([_Result(rows=[metadata])])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3], aggregate_mode=True)

    assert too_large
    assert catalogs == {3: 'missing_version_catalog'}
    assert len(connection.statements) == 1


def test_catalog_entry_cap_fails_aggregate_without_loading_body(monkeypatch):
    catalog = _catalog()
    metadata = _catalog_metadata(catalog)
    metadata['entry_count'] = 2
    monkeypatch.setattr(serve_dashboard,
                        '_MAX_PRICING_CATALOG_ENTRIES_PER_CATALOG', 1)
    connection = _Connection([_Result(rows=[metadata])])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3], aggregate_mode=True)

    assert too_large
    assert catalogs == {3: 'missing_version_catalog'}
    assert len(connection.statements) == 1


def test_catalog_total_entry_cap_fails_aggregate_without_loading_bodies(
        monkeypatch):
    catalog = _catalog()
    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_CATALOG_ENTRIES', 1)
    connection = _Connection([
        _Result(rows=[
            _catalog_metadata(catalog, version=3),
            _catalog_metadata(catalog, version=4),
        ]),
    ])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3, 4], aggregate_mode=True)

    assert too_large
    assert catalogs == {
        3: 'missing_version_catalog',
        4: 'missing_version_catalog'
    }
    assert len(connection.statements) == 1


def test_catalog_total_byte_cap_fails_aggregate_without_loading_bodies(
        monkeypatch):
    catalog = _catalog()
    first = _catalog_metadata(catalog, version=3)
    second = _catalog_metadata(catalog, version=4)
    first['catalog_bytes'] = 60
    second['catalog_bytes'] = 60
    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_TOTAL_CATALOG_BYTES',
                        100)
    connection = _Connection([_Result(rows=[first, second])])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3, 4], aggregate_mode=True)

    assert too_large
    assert catalogs == {
        3: 'missing_version_catalog',
        4: 'missing_version_catalog'
    }
    assert len(connection.statements) == 1


@pytest.mark.parametrize('bound', ['entries', 'bytes'])
def test_id_catalog_bounds_settle_rows_without_loading_body(bound, monkeypatch):
    catalog = _catalog()
    metadata = _catalog_metadata(catalog)
    if bound == 'entries':
        metadata['entry_count'] = 2
        monkeypatch.setattr(serve_dashboard,
                            '_MAX_PRICING_CATALOG_ENTRIES_PER_CATALOG', 1)
    else:
        metadata['catalog_bytes'] = 101
        monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_CATALOG_BYTES', 100)
    connection = _Connection([_Result(rows=[metadata])])

    catalogs, too_large = serve_dashboard._load_version_catalogs(
        connection, 'svc', [3], aggregate_mode=False)

    assert not too_large
    assert catalogs == {3: 'catalog_too_large'}
    assert len(connection.statements) == 1


def test_aggregate_complete_multiplies_physical_groups():
    catalog = _catalog(cost=1.25, num_nodes=2)
    connection = _Connection([
        _Result(rows=[{
            'replica_id': 1
        }, {
            'replica_id': 2
        }, {
            'replica_id': 3
        }]),
        _Result(rows=[
            _pricing_group(physical_count=2),
            _pricing_group(version=99,
                           is_zero_cost=True,
                           is_spot=False,
                           location=None,
                           physical_count=1),
        ]),
        _Result(rows=[_catalog_metadata(catalog)]),
        _Result(rows=[{
            'version': 3,
            'placement_catalog_text': json.dumps(catalog),
        }]),
    ])

    aggregate = serve_dashboard._aggregate_pricing(connection, 'svc')

    assert aggregate == {
        'available': True,
        'unavailable_reason': None,
        'coverage': 'complete',
        'known_hourly_cost': 5.0,
        'spot_hourly_cost': 5.0,
        'non_spot_hourly_cost': 0.0,
        'tracked_replica_count': 3,
        'priced_replica_count': 3,
        'excluded_replica_count': 0,
        'exclusion_reasons': {},
    }


def test_aggregate_partial_zero_is_known_lower_bound():
    connection = _Connection([
        _Result(rows=[{
            'replica_id': 1
        }, {
            'replica_id': 2
        }]),
        _Result(rows=[
            _pricing_group(is_zero_cost=True, location=None, physical_count=1),
            _pricing_group(pricing_identity_too_large=True,
                           location=None,
                           physical_count=1),
        ]),
    ])

    aggregate = serve_dashboard._aggregate_pricing(connection, 'svc')

    assert aggregate['coverage'] == 'partial'
    assert aggregate['known_hourly_cost'] == 0.0
    assert aggregate['priced_replica_count'] == 1
    assert aggregate['excluded_replica_count'] == 1
    assert aggregate['exclusion_reasons'] == {'pricing_identity_too_large': 1}


def test_aggregate_none_never_displays_unknown_fleet_as_zero():
    connection = _Connection([
        _Result(rows=[{
            'replica_id': 1
        }]),
        _Result(rows=[_pricing_group()]),
        _Result(rows=[]),
    ])

    aggregate = serve_dashboard._aggregate_pricing(connection, 'svc')

    assert aggregate['coverage'] == 'none'
    assert aggregate['known_hourly_cost'] is None
    assert aggregate['spot_hourly_cost'] is None
    assert aggregate['non_spot_hourly_cost'] is None
    assert aggregate['exclusion_reasons'] == {'missing_version_catalog': 1}


def test_aggregate_row_cap_returns_only_projection_unavailable(monkeypatch):
    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_TRACKED_ROWS', 1)
    connection = _Connection(
        [_Result(rows=[{
            'replica_id': 1
        }, {
            'replica_id': 2
        }])])

    aggregate = serve_dashboard._aggregate_pricing(connection, 'svc')

    assert aggregate == serve_dashboard._unavailable_aggregate()
    assert all(value is None
               for key, value in aggregate.items()
               if key not in ('available', 'unavailable_reason'))


def test_aggregate_group_cap_returns_only_projection_unavailable(monkeypatch):
    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_GROUPS', 1)
    connection = _Connection([
        _Result(rows=[{
            'replica_id': 1
        }, {
            'replica_id': 2
        }]),
        _Result(rows=[
            _pricing_group(),
            _pricing_group(version=4),
        ]),
    ])

    aggregate = serve_dashboard._aggregate_pricing(connection, 'svc')

    assert aggregate == serve_dashboard._unavailable_aggregate()
    assert len(connection.statements) == 2


def test_aggregate_live_version_cap_includes_zero_cost_groups(monkeypatch):
    monkeypatch.setattr(serve_dashboard, '_MAX_PRICING_VERSIONS', 1)
    connection = _Connection([
        _Result(rows=[{
            'replica_id': 1
        }, {
            'replica_id': 2
        }]),
        _Result(rows=[
            _pricing_group(version=3, is_zero_cost=True, location=None),
            _pricing_group(version=4, is_zero_cost=True, location=None),
        ]),
    ])

    aggregate = serve_dashboard._aggregate_pricing(connection, 'svc')

    assert aggregate == serve_dashboard._unavailable_aggregate()
    assert len(connection.statements) == 2


def test_id_mode_deduplicates_settles_missing_and_matches_page_fingerprint(
        monkeypatch):
    catalog = _catalog(cost=1.25, num_nodes=2)
    row = _pricing_row(7)
    connection = _Connection([
        _Result(row=('hash-a', 0)),
        _Result(rows=[row]),
        _Result(rows=[_catalog_metadata(catalog)]),
        _Result(rows=[{
            'version': 3,
            'placement_catalog_text': json.dumps(catalog),
        }]),
    ])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(connection))

    result = serve_dashboard.get_service_pricing('svc', 'hash-a', [7, 7, 99])

    assert result['aggregate'] is None
    assert [item['replica_id'] for item in result['replicas']] == [7, 99]
    assert result['replicas'][0]['hourly_cost'] == 2.5
    direct_fingerprint = serve_dashboard._serialize_replica_row(
        _replica_row(7))['pricing_fingerprint']
    assert result['replicas'][0]['pricing_fingerprint'] == direct_fingerprint
    assert result['replicas'][1] == {
        'replica_id': 99,
        'pricing_fingerprint': None,
        'hourly_cost': None,
        'price_source': None,
        'hourly_cost_exclusion_reason': 'not_current_or_uncertain',
    }


def test_no_id_mode_owns_aggregate_and_returns_no_rows(monkeypatch):
    connection = _Connection([
        _Result(row=('hash-a', 0)),
        _Result(rows=[]),
    ])
    monkeypatch.setattr(serve_dashboard, '_postgres_engine',
                        mock.Mock(return_value=mock.Mock()))
    monkeypatch.setattr(serve_dashboard, '_repeatable_read_connection',
                        lambda _engine: contextlib.nullcontext(connection))

    result = serve_dashboard.get_service_pricing('svc', 'hash-a')

    assert result['price_basis'] == 'version_catalog'
    assert result['aggregate'] == serve_dashboard._empty_aggregate()
    assert not result['replicas']


@pytest.mark.parametrize(
    'replica_ids',
    [[0], [-1], [True], [2**31], list(range(101))])
def test_pricing_rejects_invalid_raw_ids_before_database(
        replica_ids, monkeypatch):
    engine = mock.Mock()
    monkeypatch.setattr(serve_dashboard, '_postgres_engine', engine)

    with pytest.raises(ValueError):
        serve_dashboard.get_service_pricing('svc', 'hash-a', replica_ids)

    engine.assert_not_called()
