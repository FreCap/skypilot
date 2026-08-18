"""Characterization tests for the durable LB cutover repository."""

import ast
import hashlib
import inspect
import subprocess
import sys
import textwrap

from sky.serve import lb_cutover_state
from sky.serve import serve_state

_EXPECTED_AST_SHA256 = {
    '_require_postgresql_lb_cutover': '16a95452c5d8c0b27633ea53e8107931a2fedfe8f7fe25c408ff44573e92b3aa',
    'get_lb_cutover_state': '2d261d61de753cd543c957fdf50bf08ca526232ba9a973529f82c37723091dc1',
    '_lb_cutover_owner_predicates': '9dd022e33b7a23128a983adc9d85f8e3364f1e10e3ca5a9aeb04fc8cfe05c4b2',
    'begin_lb_ha_migration': 'e5580aeb893f8c3a8b394b7ed129f8507c3fea5b7b549cc52748378814d176f5',
    'finish_lb_ha_migration': '69e01df36495034d050f767d77a94a2664ffcb22c8bd9e3fdbdb3b5117165624',
    'begin_lb_ha_rollback': '9670e422667eb9af9dec5babb7bc3a401c6d406a5c794f7c8feb43b426b3a444',
    'finish_lb_ha_rollback': '46754dc27c2442ecc9eb515db1a92fd25d2e396073c5eab132e40d0cc6fadad0',
    'begin_lb_cutover': 'aba05796484bc5d8ed747b8759aadaf707d4900d66331a69c8074361f463b550',
    'record_lb_active_demand_snapshot': '56c8b376cde149760ffaab445b75220a69614b1303948d5cdf0cde28f52f68a2',
    'get_lb_last_demand_snapshot': '591f0d5234686cd79010e20112bce5d8b0f0f9476eaf828b21eb2c201a5a43ea',
    'commit_lb_cutover': '419ec07ea661a3aaebce0e64f450c6dc51a000d89be2c682311def355be4926b',
    'finish_lb_cutover_drain': '7d6e8205ea712f27e05cde990053b093cc5d881b5a7df4a3ab7b28627c7ee23a',
    'get_lb_demand_handoff': 'fa130862f4995b2413b977cceb092ba5b3fa3246f570632864162d70ed2f43da',
    'mark_lb_demand_handoff_complete': 'cb984fc2e5c2c3aad8f40707b394877f044b0691ae1650859342db0f660de3c5',
    'clear_lb_demand_handoff': 'c41c6323afe6063aa26f0e165b5acfb098a36df7be90c1e30045da8b58a6cfe8',
    'lb_cutover_kubernetes_guard': '218d9795dcb695fe15f034e2f9356e4c5c3a346b8ab5515700fa39335968b490',
    'abort_lb_cutover_preparation': 'f91897f46012883a184514adc61ce542e02116eb4f5ed507d6844e1278ea7599',
}


def _ast_sha256(symbol) -> str:
    source = inspect.getsource(inspect.unwrap(symbol))
    node = ast.parse(textwrap.dedent(source)).body[0]
    # Python 3.14 stops rendering empty AST fields by default, while Python
    # 3.12+ adds an empty type_params field to functions. Render empty fields
    # when supported, then omit the non-generic parser metadata so the
    # structural fingerprint is stable across supported Python versions.
    dump_kwargs = {'include_attributes': False}
    if 'show_empty' in inspect.signature(ast.dump).parameters:
        dump_kwargs['show_empty'] = True
    normalized = ast.dump(node, **dump_kwargs).replace(', type_params=[]', '')
    return hashlib.sha256(normalized.encode()).hexdigest()


def test_cutover_repository_structure_and_historical_identity():
    for name, expected_digest in _EXPECTED_AST_SHA256.items():
        symbol = getattr(serve_state, name)
        assert symbol.__module__ == 'sky.serve.serve_state'
        assert inspect.unwrap(symbol).__module__ == 'sky.serve.serve_state'
        assert _ast_sha256(symbol) == expected_digest


def test_cutover_repository_is_a_direct_facade_over_shared_state():
    for name in _EXPECTED_AST_SHA256:
        assert getattr(serve_state, name) is getattr(lb_cutover_state, name)
    assert lb_cutover_state.services_table is serve_state.services_table
    assert getattr(lb_cutover_state,
                   '_db_manager') is getattr(serve_state, '_db_manager')
    assert lb_cutover_state.lb_ha is serve_state.lb_ha
    assert lb_cutover_state.sqlalchemy is serve_state.sqlalchemy
    assert lb_cutover_state.orm is serve_state.orm
    assert lb_cutover_state.time is serve_state.time


def test_cutover_repository_import_order_preserves_identity():
    common_assertions = """
assert facade._db_manager is implementation._db_manager
assert facade.services_table is implementation.services_table
assert facade.begin_lb_cutover is implementation.begin_lb_cutover
assert facade.lb_cutover_kubernetes_guard is (
    implementation.lb_cutover_kubernetes_guard)
"""
    programs = (
        """
from sky.serve import lb_cutover_state as implementation
from sky.serve import serve_state as facade
""",
        """
from sky.serve import serve_state as facade
from sky.serve import lb_cutover_state as implementation
""",
    )
    for imports in programs:
        subprocess.run([sys.executable, '-c', imports + common_assertions],
                       check=True)
