# Bound Dashboard Log Progress State

_Created: 2026-07-21_

## Problem

The shared dashboard `useLogStreamer` hook caps ordinary buffered log lines,
but stores process-keyed progress lines in an unbounded `Map`. A stream with
many distinct process prefixes can therefore grow retained and rendered state
for the lifetime of the stream despite the hook's `maxRenderLines` budget.
Each keyed progress line also enqueues its own React progress-tick update, even
when many lines arrive in one chunk.

The untouched baseline is concrete: with `maxRenderLines=3`, one chunk
containing four distinct process progress lines renders all four. Memory and
render work are therefore O(unique process prefixes), rather than bounded by
the configured line budget.

## Goals

Keep process-keyed progress state bounded by `maxRenderLines`, including a
zero-line boundary, retain the most recent distinct processes, update an
existing process in place, and publish at most one progress tick per received
chunk. Preserve stream cancellation, refresh reset, partial-line buffering,
normal-line retention, and error handling.

## Solution

Use one small append helper for ordinary buffered lines so both progress-bar
fallbacks and normal log lines share the same trimming rule. While processing a
chunk, update the keyed progress map without publishing React state. After the
chunk is processed, evict the oldest process keys until the map is within
`maxRenderLines`, then publish one progress tick if any keyed progress line
changed. `Map` insertion order makes eviction deterministic and keeps the work
linear in the input chunk, with retained progress state O(maxRenderLines).

Refresh and unmount continue to clear the map and abort the active stream. The
change adds no request, timer, scan of prior ordinary logs, or backend call.

## Alternatives considered

Moving progress entries into the ordinary log array would simplify storage but
would lose in-place replacement by process and cause repeated progress updates
to consume history slots. Capping during every individual insertion would
bound memory too, but retaining the existing one-state-update-per-line pattern
would preserve avoidable React update-queue work.

## Changed-path-to-test matrix

| Changed path                                     | Invariant                                                                                                                                                                                                                                 | Test file                                                         | Command                                                                           |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `sky/dashboard/src/hooks/useLogStreamer.js`      | Distinct keyed progress entries retain only the newest `maxRenderLines`; repeated updates replace one process; ordinary and unkeyed progress lines remain bounded; refresh and unmount revoke the prior stream, timer, and buffered state | `sky/dashboard/src/hooks/useLogStreamer.test.js`                  | `npm --prefix sky/dashboard test -- --runInBand src/hooks/useLogStreamer.test.js` |
| `.github/workflows/dashboard.yml`                | The exact new hook regression suite runs in pull-request CI with no relevant path filter                                                                                                                                                  | workflow inspection plus local reproduction of its Jest inventory | `npm --prefix sky/dashboard test -- --runInBand <dashboard.yml Jest inventory>`   |
| `docs/designs/dashboard-bounded-log-progress.md` | Behavior, lifecycle boundaries, performance proof, and CI mapping stay synchronized with the implementation                                                                                                                               | review and diff checks                                            | `git diff --check origin/improvements...HEAD`                                     |

## Verification and performance evidence

Run the new suite first against untouched production code to record the
four-versus-three failure. After implementation, run the focused hook suite,
the exact dashboard workflow Jest inventory, ESLint, Prettier, the Next.js
production build, and `git diff --check`.

The boundary test with a deliberately tiny budget is the deterministic
performance assertion: retained keyed progress state changes from O(unique
process prefixes) to O(maxRenderLines). Chunk processing stays O(chunk lines),
ordinary log storage stays O(maxRenderLines), and no I/O or timer is added.
