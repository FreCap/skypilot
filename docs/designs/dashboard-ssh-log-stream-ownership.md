# Dashboard SSH log stream ownership

## Problem

`streamSSHDeploymentLogs()` and `streamSSHOperationLogs()` duplicate the same fetch, inactivity timer, reader, decoder, cancellation, and error logic. When either five-minute inactivity timer wins, the function clears the timer and returns but leaves the losing fetch or `reader.read()` alive. Later UI work can overlap that stale stream and retain browser and API-server resources.

## Behavior contract

- Both public functions use one shared lifecycle implementation while retaining their current signatures and distinct warning text.
- Every invocation owns the cancellation signal passed to `fetch()`.
- Inactivity aborts that owned signal before the public function resolves.
- A transport that settles after timeout is not consumed; its body is cancelled, and a late failure is observed.
- Caller cancellation forwards once without mutating the caller-owned controller and without an inactivity warning.
- Inactivity is measured with a monotonic clock, so wall-clock corrections
  cannot extend or prematurely consume the five-minute budget.
- Normal EOF retains one fetch, one read per chunk plus EOF, streaming UTF-8 decoding, reader cleanup, and the five-minute inactivity budget.
- HTTP and stream failures propagate after timers and caller listeners are cleaned up.

## Solution

Create one private `streamSSHLogs()` implementation in `ssh-node-pools.js`. It owns an `AbortController`, forwards the optional caller signal for the invocation lifetime, and performs the activity-based timeout race and streaming decode using `performance.now()` for elapsed time. If inactivity wins, it aborts its controller, observes the losing promise without blocking the timeout return, and reports the wrapper-provided warning. If an already-aborted request receives a late response, it cancels the response body without opening a reader.

Keep `streamSSHDeploymentLogs()` and `streamSSHOperationLogs()` as thin delegates which supply the existing deployment and operation warning messages.

## Changed-path-to-test matrix

| Changed path or invariant                                                                                                         | Test file                                                                  | Command and CI coverage                                                                                                                                                                      |
| --------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sky/dashboard/src/data/connectors/ssh-node-pools.js`: shared implementation, owned timeout abort, bounded late transport cleanup | `sky/dashboard/src/data/connectors/ssh-node-pools-stream-timeout.test.jsx` | `npm --prefix sky/dashboard test -- --runInBand src/data/connectors/ssh-node-pools-stream-timeout.test.jsx`; Dashboard workflow `dashboard` job                                              |
| Deployment and operation wrappers retain distinct warnings and one fetch each                                                     | `sky/dashboard/src/data/connectors/ssh-node-pools-stream-timeout.test.jsx` | Same focused command and CI job                                                                                                                                                              |
| Caller cancellation forwards without warning and detaches after completion/failure                                                | `sky/dashboard/src/data/connectors/ssh-node-pools-stream-timeout.test.jsx` | Same focused command and CI job                                                                                                                                                              |
| Backward wall-clock corrections cannot extend the inactivity budget                                                               | `sky/dashboard/src/data/connectors/ssh-node-pools-stream-timeout.test.jsx` | Same focused command and CI job                                                                                                                                                              |
| Normal EOF preserves read counts, cleanup, and output order                                                                       | `sky/dashboard/src/data/connectors/ssh-node-pools-stream-timeout.test.jsx` | Same focused command and CI job                                                                                                                                                              |
| Split UTF-8 decoding remains correct for both exports                                                                             | `sky/dashboard/src/data/connectors/stream-decoders.test.jsx`               | `npm --prefix sky/dashboard test -- --runInBand src/data/connectors/ssh-node-pools-stream-timeout.test.jsx src/data/connectors/stream-decoders.test.jsx`; Dashboard workflow `dashboard` job |
| Existing SSH node-pool UI lifecycle remains compatible                                                                            | `sky/dashboard/src/components/infra.test.jsx`                              | Workflow-equivalent dashboard Jest command; Dashboard workflow `dashboard` job                                                                                                               |
| CI executes the new regression suite                                                                                              | `.github/workflows/dashboard.yml`                                          | Inspect workflow branch/path triggers and explicit test list; exact-head Dashboard check                                                                                                     |
| Bundle, lint, and formatting remain valid                                                                                         | Dashboard source and build                                                 | `npm --prefix sky/dashboard run lint`, `npm --prefix sky/dashboard run format:check`, `npm --prefix sky/dashboard run build`; Dashboard workflow `dashboard` job                             |

## Performance evidence

Focused tests assert one fetch, one timer chain, no additional read after timeout, zero pending fake timers, monotonic timeout behavior across a backward wall-clock correction, and unchanged one-read-per-chunk plus EOF behavior. Each invocation adds one local controller and at most one abort listener, while inactivity now releases the unbounded losing request. Reading the monotonic browser clock has the same constant-time shape as the prior wall-clock read. The extraction removes a full duplicated stream loop and adds no API request, polling round, retry, render, or cache lookup.

## Rollout and recovery

This is a dashboard-only change with no API or data migration. Reverting the connector, test, workflow, and design commit restores the previous behavior.
