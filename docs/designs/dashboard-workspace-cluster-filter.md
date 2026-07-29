# Dashboard Workspace Cluster Filter

## Problem

The workspace editor loads cluster capacity for one workspace, but its cluster
request currently fetches every cluster visible to the caller and discards
other workspaces in the browser. The request does unnecessary database,
serialization, network, cache, and client-side summarization work. Its cost
grows with the caller's total visible cluster inventory instead of the active
workspace.

Moving the filter to the API boundary must not weaken workspace authorization.
A client-supplied workspace name is only a requested narrowing of the
server-derived accessible-workspace set.

## Behavior contract

- The workspace editor issues exactly one cluster status request for the active
  workspace on each route load.
- The request includes the active workspace and does not fetch other visible
  workspaces for client-side filtering.
- The server intersects every requested workspace filter with the authenticated
  caller's accessible workspaces. A request can narrow access but cannot widen
  it.
- An omitted filter preserves the existing accessible-workspace result set.
- An empty filter, or a filter containing only inaccessible workspaces, returns
  no clusters.
- Direct cluster names and globbed cluster names remain subject to the same
  effective workspace filter.
- Status refresh, credential suppression, user filtering, response shaping,
  and request scheduling retain their existing behavior.
- The default synchronous and asynchronous SDK call shapes remain unchanged
  when no workspace filter is requested.

## Solution

Thread an optional `workspaces_filter` from the dashboard cluster connector
through the synchronous and asynchronous client SDKs, the status request body,
and `sky.core.status()` into `backend_utils.get_clusters()`. At the backend
boundary, intersect the requested workspaces with the server-derived accessible
workspace set once, then reuse that effective set for glob resolution and the
single cluster-record query.

The dashboard cache key includes the workspace argument, so cached results from
one workspace cannot satisfy another workspace's request.

## Alternatives considered

Keeping browser-side filtering avoids an API change but preserves the
inventory-wide query, response, and summarization cost. Adding a
dashboard-only endpoint would duplicate status authorization, filtering, and
response-shaping behavior. Trusting the requested workspace directly is
smaller but would create an authorization bypass for callers that can name a
workspace they cannot access.

## Changed-path-to-test matrix

| Changed path | Invariant | Test and command |
| --- | --- | --- |
| `sky/dashboard/src/components/workspace-editor.jsx` | One workspace-scoped cluster read per route; route changes use distinct cache arguments | `npm --prefix sky/dashboard test -- --runInBand src/components/workspace-editor.test.jsx` |
| `sky/dashboard/src/data/connectors/clusters.jsx` | `/status` request serializes the workspace filter without adding another request | `npm --prefix sky/dashboard test -- --runInBand src/data/connectors/clusters.test.jsx` |
| `sky/client/sdk.py`, `sky/server/requests/payloads.py` | Synchronous SDK serializes the filter into the status body | `pytest -q -o addopts='' tests/unit_tests/test_sky/client/test_sdk_async_status_workspace_filter.py` |
| `sky/client/sdk_async.py` | Explicit filters reach the synchronous SDK; omitted filters preserve the default call shape | `pytest -q -o addopts='' tests/unit_tests/test_sky/client/test_sdk_async.py tests/unit_tests/test_sky/client/test_sdk_async_status_workspace_filter.py` |
| `sky/core.py` | The scheduled request filter reaches the backend unchanged | `pytest -q -o addopts='' tests/unit_tests/test_core.py` |
| `sky/backends/backend_utils.py` | Requested and accessible workspaces are intersected; omitted, empty, and inaccessible-only boundaries are closed; glob and direct-name filtering share the result | `pytest -q -o addopts='' tests/unit_tests/test_backend_utils.py` |
| Performance | One dashboard fetch, one backend cluster query, and a result set bounded by the active workspace | Exact request and call-count assertions in the focused dashboard and backend tests |

## CI and rollout

`.github/workflows/dashboard.yml` explicitly runs both changed dashboard test
files, lint, formatting, and the production build for pull requests to
`improvements`. `.github/workflows/pytest.yml` runs all changed Python tests in
`Python Tests - Unit Tests` without path exclusions. Static workflows cover
formatting, mypy, Pylint, Ruff, basedpyright, async lifecycle, import contracts,
stub runtime contracts, and the worker-floor import.

No migration is required. The field defaults to `None`, so existing callers
retain the server-derived accessible-workspace filter.
