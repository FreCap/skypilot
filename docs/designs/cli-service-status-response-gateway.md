# CLI service status response gateway

_Created: 2026-07-31_

## Problems

`sky/client/cli/command.py` owns the root Click tree, task and cluster
lifecycle orchestration, and the complete Managed Jobs and SkyServe command
families. It also owns a distinct response gateway that consumes asynchronous
service or pool status requests, translates controller and transport failures,
selects endpoint or table output, and reports missing names.

The gateway has four callers across root status, `sky jobs pool status`, and
`sky serve status`. Its dependencies and failure modes are specific to status
response translation rather than Click registration or resource lifecycle.
Keeping it in the root command module obscures that ownership boundary.

## Goals

Move the complete status-response gateway behind the existing
`sky.client.cli.command._handle_services_request` façade. Preserve the exact
function identity exposed from the historical module, call counts, exception
behavior, formatting, controller fallback, and import cost within measurement
noise. Do not move Click command registration or any service or pool lifecycle
operation.

## Background and responsibility map

The root CLI façade owns command object registration and stable import paths.
Its callers are Click discovery, tests, extensions, and users importing command
objects. It depends on registration order and decorator metadata, owns the
command tree identity, and can fail through help or import drift. Root import
and help rendering are performance-sensitive, and the responsibility changes
with global CLI compatibility.

Task, cluster, Managed Jobs, pool, and SkyServe lifecycle orchestration is a
second responsibility. Its callers are launch, exec, status, jobs, pool, and
serve workflows. It depends on SDK requests, remote commands, confirmation,
waiting, and cleanup; it owns request-local lifecycle ordering. Its failures
include changed remote behavior, cleanup, or cancellation semantics, and its
cadence follows product lifecycle work.

The service-status response gateway is a third responsibility. Its callers are
the root aggregate status command, `sky jobs pool status`, and `sky serve
status`. It depends on SDK result retrieval, controller discovery, status table
formatting, Click usage errors, and terminal messages. It owns no durable state.
Its failures are false empty-fleet output, probing the wrong controller,
endpoint cardinality drift, missing-name drift, or changed internal-usage
attribution. It runs once per status request, so SDK call counts and local
formatting overhead are the relevant performance constraints. Its cadence
follows status API and presentation compatibility.

## Solution

Create `sky/client/cli/service_status.py` containing the existing handler as a
plain function. Import it from `command.py` and expose it through a direct alias
whose `__module__` remains the historical command module. This keeps existing
imports, monkeypatches, introspection, and pickle lookup stable without a
wrapper frame or dependency-injection layer.

Characterization tests are committed before the move. They cover the public
signature and pickle identity, internal-usage attribution, successful service
and pool table projection, missing names, endpoint cardinality, stopped and
absent controller fallbacks, and exact SDK and formatter call counts. Existing
CLI and Serve status tests remain the broader regression suite.

## Alternatives considered

Leaving the helper in place avoids a new module, but preserves a complete
transport and presentation boundary inside the already mixed root façade.
Moving only the pool command is smaller by line count but does not own behavior
end to end and would add callbacks into root lifecycle helpers. Moving all
status commands would cross cluster, Managed Jobs, and SkyServe orchestration
and repeat the previously rejected high-state split. A class, protocol,
strategy, registry, or adapter hierarchy would add carrying cost without a
second implementation.

## Verification and rollout

The changed-path matrix is:

| Changed path | Responsibility | Tests and checks |
| --- | --- | --- |
| `sky/client/cli/command.py` | Historical façade and callers | characterization tests, CLI tests, import and pickle checks |
| `sky/client/cli/service_status.py` | Response gateway | characterization tests, Serve status error tests, type and lint checks |
| `tests/unit_tests/test_cli_service_status_contract.py` | Boundary contract | focused pytest plus full relevant CLI and Serve suites |
| this design | Canonical contract | documentation review and diff checks |

Measure balanced cold imports of `sky.client.cli.command` before and after the
move. Direct aliasing must add no SDK request, formatter call, copy, retry, or
wrapper frame. The pull request remains open unless relevant CI covers these
paths and succeeds on the exact pushed SHA.
