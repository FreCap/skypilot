# Dashboard service-version YAML copy ownership

## Context

The service version history lets an administrator view the retained submitted
or compiled YAML for any version and copy the formatted YAML to the clipboard.
Clipboard writes are asynchronous. A write may finish after the administrator
changes the displayed YAML, starts another copy, switches versions, or closes
the viewer.

The copy confirmation is therefore owned by more than a mounted component. It
is owned by one specific copy attempt for one specific viewer generation.

## Behavior contract

- The copy button writes exactly the formatted YAML displayed when the attempt
  starts.
- A completion may publish `Copied!` only while that attempt is still the
  latest attempt in the same mounted viewer.
- Changing between submitted and compiled YAML invalidates any pending write
  and clears feedback from the previous YAML kind.
- Starting another copy invalidates the older write and owns a fresh two-second
  feedback interval.
- Switching versions or closing the viewer invalidates pending writes and
  clears the owned timer during unmount.
- A stale success or failure has no user-visible effect. Clipboard failures for
  the current attempt keep the existing console error behavior.
- Viewer actions do not refetch version history or add backend work.

## Design

`VersionYamlViewer` maintains a monotonically increasing generation in a ref.
Every copy attempt first invalidates the prior generation and captures the new
one. The asynchronous completion and feedback timer compare their captured
generation with the current generation before publishing state.

The viewer owns at most one feedback timer. Generation changes clear the owned
timer before a replacement may be installed. Unmount increments the generation
and clears the timer, which fences completions that still hold the old ref.

This is deliberately local to the viewer. A shared clipboard abstraction would
broaden the change without improving the ownership boundary, and cancellation
cannot be delegated to the browser clipboard API because its promise is not
abortable.

## Alternatives considered

1. Disable YAML-kind and version controls while copying. Rejected because a
   clipboard write should not block navigation, and it still would not cancel
   a write during unmount.
2. Track only whether the component is mounted. Rejected because overlapping
   writes and YAML-kind changes occur within one mount.
3. Let every completion update the same boolean and timer. Rejected because
   completion order then determines feedback for unrelated displayed content.

## Rollout and compatibility

The change is limited to Dashboard client state. It changes no API, persisted
data, authorization rule, YAML formatting, or service election path. No
feature flag or data migration is required.

## Test plan

- On the exact PR #1170 source head, defer a submitted-YAML clipboard write,
  switch to compiled YAML, complete the old write, and prove stale `Copied!`
  feedback appears.
- Repeat on the correction and prove the old completion is ignored.
- Start a second copy while the first feedback timer exists and prove the old
  timer cannot clear the newer feedback interval.
- Run the service-version component tests, service-details integration tests,
  the full Dashboard suite, lint, formatting, and production build.
- Confirm the implementation performs constant-time generation and timer work
  per copy and adds no connector call or scale-dependent pass.
