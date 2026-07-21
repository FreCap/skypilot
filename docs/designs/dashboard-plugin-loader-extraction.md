# Dashboard plugin loader extraction

## Problem

`sky/dashboard/src/plugins/PluginProvider.jsx` owns several responsibilities in
one 1,150-line module:

1. Plugin registry state, registration validation, and conflict reporting.
2. Plugin manifest transport and browser script injection.
3. Browser history interception, navigation, analytics, and host capability
   projection through `SkyDashboardPluginAPI`.
4. React provider lifecycle, global reference publication, and nav-link cache
   persistence.
5. Consumer hooks for routes, slots, data providers, and table columns.

Manifest retrieval and script injection are a stable gateway seam. They depend
on the plugins endpoint, URL resolution, the DOM, and one module-local promise
cache. The other responsibilities depend on React state, Next.js routing,
plugin contracts, analytics, dashboard caches, and presentation callers.

## Behavior contract

- `PluginProvider` remains the only public provider entrypoint and keeps all
  existing exports and global names.
- The provider requests `/api/plugins` once per mount, tolerates HTTP, payload,
  and request failures by treating the manifest as empty, and continues
  startup.
- Each valid `js_extension_path` becomes an asynchronous script element with
  the same resolved URL and `requires_early_init` data attribute.
- Duplicate resolved URLs share one promise and inject one script per module
  lifetime.
- Script load errors remain non-fatal and settle the loading promise.
- `skydashboard:plugins-loaded` and the synchronous loaded flag are published
  only after all injected scripts settle, with cancellation behavior unchanged.
- Registry ordering, registration normalization, history interception, React
  state, local storage, routing, analytics, and consumer hook behavior do not
  change.

## Design

Move `resolveScriptUrl`, `loadPluginScript`, `fetchPluginManifest`,
`extractJsPath`, and their `pluginScriptPromises` state to
`sky/dashboard/src/plugins/plugin-loader.js`. `PluginProvider.jsx` imports the
four functions directly and remains the stable facade and lifecycle owner.

This is a plain module gateway, not a class, strategy, registry, or dependency
injection layer. The loader has one implementation and needs no variation
abstraction. Keeping the promise cache beside script injection preserves its
single-owner lifetime and avoids a forwarding wrapper.

## Alternatives considered

- Leave the file unchanged. This avoids a new file, but continues to mix an
  independently changing network and DOM gateway into registry and React
  lifecycle code.
- Extract all registration normalization. Those functions and the reducer
  share the same plugin contract and callers, so splitting them would divide a
  cohesive responsibility.
- Extract the entire bootstrap effect. The effect owns provider cancellation,
  loaded-state publication, and React dispatch ordering, so moving it would
  couple the loader back to provider lifecycle state.
- Introduce a loader class or injectable interface. There is no second loader
  implementation, and the extra construction surface would add indirection
  without buying isolation.

## Milestones

1. Add provider-level characterization tests for manifest fetch, URL
   resolution, duplicate suppression, early-init metadata, error tolerance,
   and loaded-event ordering. Run them against the unsplit implementation.
2. Extract the byte-equivalent loader gateway and update the provider import.
3. Verify the focused plugin suite, dashboard suite, lint, formatting, build,
   and diff checks.

## Test and CI plan

| Changed path              | Evidence                                                                                                                          |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `PluginProvider.jsx`      | Provider characterization pins the stable facade, bootstrap ordering, and loaded signal.                                          |
| `plugin-loader.js`        | The same characterization pins manifest transport, script attributes, deduplication, and failure tolerance through the provider.  |
| `PluginProvider.test.jsx` | Run directly before and after extraction.                                                                                         |
| `dashboard.yml`           | Add the characterization test to the existing unfiltered dashboard job, which also runs lint, Prettier, and the production build. |

The dashboard workflow has no relevant pull-request path filter. Structural
verification confirms all four moved function ASTs are unchanged, and the
characterization proves one manifest request and one script injection per
resolved URL without an added React render, dispatch, or wrapper call. A clean
production build comparison measured a 490-byte raw and 37-byte gzip increase
in the `_app` chunk (0.054% and 0.013%), plus a 2,143-byte raw and 311-byte gzip
increase across all JavaScript chunks (0.082% and 0.038%). This is a negligible
module-boundary cost with no extra runtime I/O.

## Rollout and rollback

This is an internal structural extraction with no migration or configuration
change. It rolls out with the dashboard bundle. Reverting the extraction commit
restores the single-file implementation without data cleanup.
