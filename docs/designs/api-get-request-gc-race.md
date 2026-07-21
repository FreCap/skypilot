# Handle Request GC During `/api/get`

## Context

`/api/get` first polls a request's lightweight status until it is terminal and
then fetches the full request row for serialization. Request retention runs in
another coroutine and deletes old terminal rows. It can delete a row after the
status poll succeeds but before the full-row fetch completes.

The full-row lookup is correctly typed as returning `Request | None`, but the
endpoint dereferences the result without checking it. In the race, clients
receive an internal `AttributeError` instead of the API's existing not-found
response.

## Behavior Contract

- A request that disappears at any point during `/api/get` returns HTTP 404.
- Existing successful, failed, cancelled, daemon, and retry responses are
  unchanged when the full request row still exists.
- Request retention and request persistence behavior are unchanged.
- `sky/server/server.py` statically rejects future unchecked optional member
  access so this class of race cannot be reintroduced in that route module.

## Design

After the terminal-status polling loop, check the result of the full-row fetch
before accessing request fields. Reuse the endpoint's existing 404 message and
status code for a request missing during the earlier status poll.

Enable BasedPyright's `reportOptionalMemberAccess` diagnostic for
`sky/server/server.py` with a file-level directive. Global enablement is not
part of this change because the repository still has many unrelated findings,
primarily from optional dependency adaptors.

## Alternatives

1. Fetch and hold the full row on every poll. This widens each polling query,
   increases database load, and still cannot pin the row after the final read.
2. Prevent GC while a client polls. This requires distributed reader leases
   and changes retention semantics for a small error-reporting race.
3. Return a synthetic terminal payload after deletion. The deleted row's
   result, error, and retry fields are unavailable, so such a payload could be
   incorrect.

## Milestones

1. Add a regression test that returns a terminal status and then simulates the
   full request row being deleted.
2. Add the missing-row guard and the scoped BasedPyright diagnostic.
3. Run the focused server tests, BasedPyright, Ruff, and repository formatting.

## Rollout

This is a backward-compatible API error-path correction with no schema,
configuration, or migration changes. It can roll out with the normal API
server release.

## Test Plan

- Verify the race returns HTTP 404 with the request ID in the response detail.
- Verify the existing successful serialization test remains green.
- Run the focused server unit-test module.
- Run BasedPyright against the existing baseline and run Ruff on `sky`.
