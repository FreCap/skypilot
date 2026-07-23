# Managed Jobs Cancellation Gateway

_Created: 2026-07-21_

## Problem

`sky/jobs/server/core.py` owns managed job launch, queue projection,
cancellation, log transport, waiting, pools, and event access. The cancellation
path is a complete transport gateway embedded between the queue and log APIs.
It does not share lifecycle state with either neighbor, but it brings together
request validation, controller access, Skylet gRPC request construction,
legacy runner fallback, and response interpretation inside the main facade.

The file is 1,799 lines before this change. Its size is only a prioritization
signal. The reason to extract cancellation is the stable end to end gateway
boundary and its materially different dependencies, callers, failure modes,
and cadence.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Launch and controller lifecycle | Managed jobs launch API | Execution, controller, storage, and admin policy | Controller and submission lifecycle | Provisioning, upload, token, or submission failure | Launch calls and persistence queries | Managed job launch features |
| Queue and API response projection | Queue API, debug utilities, resource checker, and wait | Controller restart, gRPC, runners, workspaces, and users | Controller mutation during refresh | Controller availability, protocol, filtering, or compatibility errors | Polling, projection, and query counts | Queue filtering and API behavior |
| Cancellation gateway | Cancel API router | Controller access, Skylet gRPC, runner fallback, workspace, and user identity | None | Invalid selectors, inaccessible controller, RPC fallback, missing output, or ambiguous names | One controller lookup and one gRPC or legacy dispatch | Cancellation protocol and policy |
| Wait lifecycle | Wait API router | Queue projection and monotonic deadlines | Poll status and deadline | Missing jobs or tasks, timeout, or queue failure | Poll count and sleep pacing | Wait and JobGroup semantics |
| Logs, pools, and events | CLI, SDK, dashboard, pool, and event consumers | Existing transport gateways and repositories | Stream, pool, and event progress | Transport interruption or stale identity | Streaming and polling counts | Transport and observability changes |

## Solution

Move the complete `cancel()` implementation to
`sky/jobs/server/cancellation.py`. Keep `sky.jobs.server.core.cancel` as a
direct alias to the extracted function. This keeps the historical server
router and SDK type comparison paths stable and adds no forwarding call frame.

The extracted module remains a plain gateway module. It owns no class,
registry, strategy, protocol, or dependency injection layer. It continues to
use the existing `managed_job_runner` seam for the legacy implementation and
the existing `SkyletClient` seam for gRPC.

Preserve these behavior contracts:

1. The public function signature and docstring remain unchanged.
2. Exactly one selector category of job IDs, name, pool, or `all`/`all_users`
   is accepted.
3. gRPC requests retain workspace, user, graceful cancellation, and selector
   projection semantics.
4. `SkyletMethodNotImplementedError` falls back to the legacy runner with the
   same arguments and ordering.
5. Missing output and ambiguous name responses retain their exception types
   and messages.
6. The stable facade is a direct alias, so cancellation adds no wrapper call,
   controller lookup, RPC, retry, or allocation. Both the decorated callable
   and its wrapped implementation retain the historical facade module.
7. Logging retains the historical `sky.jobs.server.core` logger name.
8. Usage events retain the historical `sky.jobs.server.core.cancel`
   entrypoint attribution after the implementation moves modules.

## Alternatives considered

Keeping the implementation in `core.py` has no immediate behavioral cost, but
leaves a complete provider gateway mixed with launch, queue, wait, and log
orchestration. The direct alias extraction has low carrying cost because the
gateway already has one stable entrypoint and two concrete transport paths.

Extracting `wait()` instead was rejected. Its behavior deliberately calls the
late bound `core.queue_v2_api` seam, which tests and downstream monkeypatches
replace. Moving it alone would require a wrapper, injected callback, or a
larger queue extraction.

Extracting cancellation and wait together was rejected because their callers,
dependencies, state, failures, and change cadence differ. A generic lifecycle
service would create a new concept without a shared implementation contract.

## Milestones and validation

1. Add facade level characterization for gRPC request projection, legacy
   fallback, selector validation, missing output, and ambiguous names. Run it
   against the unsplit implementation.
2. Move cancellation unchanged, add the direct facade alias, and prove the
   moved function is normalized AST identical. Preserve the wrapped
   implementation's module because the usage decorator derives attribution
   from it at call time rather than from a later alias.
3. Run the focused managed jobs server and SDK type tests, formatting and
   static analysis, import checks, and the relevant CI matrix.
4. Merge only after the full visible exact head CI rollup and review state are
   green. Rollback is a single revert because no data, API, config, or protocol
   migration is involved.
