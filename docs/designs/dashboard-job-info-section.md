# Dashboard job information section

## Context

`sky/dashboard/src/pages/jobs/[job].js` owns the managed-job detail page. It
coordinates connector data, page refreshes, deep-link scrolling, log viewers,
controller logs, telemetry, task selection, and the rendered page sections.
The same file also defines `JobInfoSection`, a self-contained metadata renderer
with its own copy and YAML-expansion state.

The page is 1,324 lines. Line count alone does not justify a split, but these
two responsibilities have different state, dependencies, failure modes, and
reasons to change.

## Responsibility map

### Page orchestration

- Callers: the Next.js `/jobs/[job]` route.
- Dependencies: managed-job connectors, router state, log and controller-log
  components, telemetry availability, plugin slots, timers, and DOM observers.
- State: refresh ownership, selected task and node, log loading and extracted
  links, telemetry selection, and deep-link scroll lifecycle.
- Failure modes: overlapping refreshes, stale task or node selection, missing
  deep-link targets, and inconsistent log or telemetry refreshes.
- Performance sensitivity: connector calls, stream ownership, timers, and
  rerender fan-out.
- Change cadence: page lifecycle, log streaming, telemetry, and navigation.

### Job metadata presentation

- Callers: the details card in the managed-job detail page.
- Dependencies: status and batch badges, pool-link formatting, user display,
  YAML formatting, clipboard APIs, external-link normalization, and plugins.
- State: YAML expansion, per-document expansion, full-YAML selection, and copy
  feedback.
- Failure modes: incorrect grouped status or resource projection, malformed
  YAML presentation, lost external links, and clipboard failures.
- Performance sensitivity: bounded in-memory projections and component-local
  rerenders, with no transport calls.
- Change cadence: metadata fields, badges, YAML display, and link presentation.

## Decision

Move the complete `JobInfoSection` implementation to
`sky/dashboard/src/components/job-info-section.jsx`. Keep the Next.js page as
the stable public entrypoint and import the component directly. Use a plain
React component, not an abstract interface, registry, strategy, or new package
hierarchy.

The extraction is structural. The function body and prop contract remain
unchanged. No connector ownership, page lifecycle, plugin contract, serialized
format, route, or user-visible behavior changes.

## Alternatives

- Keep the file intact: viable, but the independent 573-line presentation leaf
  obscures the page's transport and lifecycle ownership and makes focused
  metadata testing harder.
- Split individual fields or YAML helpers: rejected because it would fragment
  one cohesive presentation component and add forwarding layers.
- Introduce a view-model or component registry: rejected because there is no
  second implementation or policy variation.

## Rollout and compatibility

This is an internal dashboard extraction with no server, API, database, route,
or configuration change. The default export from `/jobs/[job]` remains
`JobDetails`. Rollback is a direct inlining of the unchanged component.

## Test plan

- Characterize grouped status, resource aggregation, badges, infrastructure
  links, extracted links, and existing log-to-metadata interaction before the
  move.
- Prove the moved `JobInfoSection` function AST is unchanged.
- Run the job-detail and adjacent component suites selected by dashboard CI.
- Run dashboard lint, Prettier verification, and the production build.
- Compare production JavaScript output size and confirm the extraction adds no
  connector calls, timers, wrappers, or render layers.
