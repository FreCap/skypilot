# Dashboard controller-log download ownership

## Problem

The managed-job controller-log download button uses rendered React state as its
only duplicate guard. A duplicate activation that reaches the handler before
that state is rendered can start a second archive preparation and download.
The two requests then race to clear the shared loading state.

## Behavior contract

- At most one fallback controller-log download owns a given job route.
- Duplicate activations reuse the current owner and do not make another
  connector call.
- A settled success or failure releases ownership so a later attempt can run.
- A download for the previous job route does not disable or clear a newer
  route's download.
- Unmounting prevents a settled request from publishing component state.
- Plugin-owned download state and the existing plugin context remain
  compatible.

## Implementation

Keep fallback download promises in a ref keyed by route for synchronous
ownership, and bump a scalar state version when the registry changes so React
renders the current route's ownership. Cleanup compares the owner identity
stored for that route before clearing the registry, so another route's request
cannot replace or release it. The existing boolean state remains dedicated to
plugin-controlled downloads.

This adds constant-time coordination only. It does not add requests, polling,
timers, scans, or cache invalidations. Under duplicate activation, connector
calls fall from two to one.

## Alternatives

- Rely on the disabled button. This does not own calls that already reached the
  handler and does not protect against programmatic duplicate activation.
- Debounce clicks. This adds a timer, delays legitimate retries, and does not
  model request lifetime.
- Use only a boolean ref. That blocks a new route behind an old route and lets
  an old completion clear a newer request.

## Rollout and tests

No migration or compatibility rollout is required. Focused dashboard tests
cover duplicate activation, success and failure release, route supersession,
an A-B-A route cycle, 100 concurrent route owners, old-completion fencing, and
the existing plugin contract. The dashboard CI workflow directly executes the
focused test file, lint, formatting, and the production build.
