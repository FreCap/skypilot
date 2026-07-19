# Skip YAML rendering for service summaries

_Created: 2026-07-19_

## Problem

`get_service_status_pickled(..., summary_only=True)` omits replica details and
controller autoscaler calls, but still asks `_get_service_status()` to render
each service YAML. The dashboard list polls this path every 30 seconds and does
not consume `service_yaml`. Consequently every poll parses, redacts, and dumps
one YAML document per service. Missing or malformed YAML also adds avoidable
error handling to an otherwise metadata-only status path.

Pool summaries are different: pool lifecycle consumers parse their YAML back
into a task, so their existing YAML contract must remain intact.

## Goal

Service summaries perform zero YAML reads, parses, redactions, or dumps. Full
service status and every pool status preserve their current behavior. The
number of status/database/controller calls must not increase.

## Solution

When `summary_only` is requested for services (`pool=False`), pass
`with_yaml=False` into the existing `_get_service_status()` enrichment
boundary. Keep the default for full service status and for pools. This reuses
the existing tested opt-out rather than introducing another status renderer or
response type.

### Changed-path-to-test matrix

| Changed production path or invariant | Test file | Command |
|---|---|---|
| `sky/serve/serve_utils.py`: service summaries skip YAML work | `tests/unit_tests/test_serve_service_yaml.py` | `pytest -q tests/unit_tests/test_serve_service_yaml.py` |
| Full service status still returns redacted YAML | `tests/unit_tests/test_serve_service_yaml.py` | same command |
| Pool summaries retain launchable YAML | `tests/unit_tests/test_serve_service_yaml.py` | same command |
| Summary counts and target-fetch opt-in remain unchanged | `tests/unit_tests/test_serve_status_summary.py` | `pytest -q tests/unit_tests/test_serve_status_summary.py` |
| Full multi-service fan-out, ordering, failures, context propagation, and worker cap remain unchanged | `tests/unit_tests/test_serve_lazy_handle.py` | `pytest -q tests/unit_tests/test_serve_lazy_handle.py` |
| Performance: YAML parse count is zero for service summaries instead of one per service | `tests/unit_tests/test_serve_service_yaml.py` | focused call-count regression test in the first command |

CI collects all three files in `Python Tests - Unit Tests` via the
`tests/unit_tests` matrix entry in `.github/workflows/pytest.yml`.

## Alternatives considered

Batching every service row and replica histogram could reduce database calls,
but it is a larger query and response-contract change. Removing YAML from pool
summaries would be unsafe because pool coordination still consumes it. The
existing `with_yaml` switch is the smallest seam that removes the observed
work without widening lifecycle scope.

## Rollout and verification

No migration or compatibility gate is needed. Verify the focused suites,
format the changed Python and test files, run the broader Serve unit-test set,
and require the exact PR head to pass the full visible CI rollup before merge.

The exact base regression test observed 25 YAML parses for 25 service summary
records. The implementation records zero parses and zero redactions for the
same 25 records. The focused matrix passed 61 tests, the full Serve unit-test
glob passed 1,481 tests plus 7 subtests, and `tests/test_jobs_and_serve.py`
passed 11 tests locally.
