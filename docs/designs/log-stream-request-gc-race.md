# Handle Request GC During Log Streaming

## Context

Request log streaming opens the log file and then polls the request row at EOF
to decide whether to keep following. Request retention deletes old terminal
logs before deleting their database rows. A stream that already holds the
opened file can therefore outlive the row.

Two unchecked optional reads can fail in this window:

- the lightweight status row can disappear before an EOF poll;
- a cancelled status can be observed, then the full request row can disappear
  before the streamer reads its retry and display-name fields.

Both paths currently raise `AttributeError` inside an already-started streaming
response.

## Behavior Contract

- If a request row disappears after log streaming starts, flush all data read
  from the opened log and end the stream cleanly.
- If a cancelled request disappears between its status and full-row reads, end
  cleanly without inventing a retry control message or request name.
- Existing pending, running, terminal, cancelled, retry, heartbeat, and log
  rollover behavior is unchanged while the request row exists.
- `sky/server/stream_utils.py` statically rejects future unchecked optional
  member access.

## Design

At an EOF status poll, treat a missing status as a terminal end-of-stream
condition. In the cancelled branch, treat a missing full row the same way.
Both paths leave the shared buffer flush at the generator exit as the single
source of truth for delivering already-read output.

Enable BasedPyright's `reportOptionalMemberAccess` diagnostic for
`sky/server/stream_utils.py` with a file-level directive. Update only the line
locations of unchanged async-lifecycle findings shifted by that directive;
their codes and messages must remain identical.

## Alternatives

1. Raise HTTP 404. Streaming response headers may already be committed, so a
   late HTTP error cannot be represented reliably.
2. Keep following after the row disappears. No producer remains for a
   retention-eligible terminal request, so this can create an endless stream.
3. Hold a database lease for every log reader. This adds distributed state and
   changes retention behavior for a case that can terminate safely.

## Milestones

1. Add regressions for a missing status row and for a cancelled request whose
   full row disappears.
2. Add the clean EOF guards and scoped BasedPyright diagnostic.
3. Refresh only shifted async-lifecycle baseline locations.
4. Run focused stream tests, BasedPyright, Ruff, async-lifecycle, and format.

## Rollout

This is a backward-compatible streaming error-path correction with no schema,
configuration, or migration changes. It can use the normal API server rollout.

## Test Plan

- Verify both retention races preserve already-read log output and terminate.
- Run the complete server stream-utils unit-test module.
- Run BasedPyright and Ruff.
- Verify async-lifecycle output exactly matches its reviewed baseline.
- Run repository formatting on the changed Python files.
