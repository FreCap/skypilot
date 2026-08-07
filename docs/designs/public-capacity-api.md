# Public Capacity API

- Status: Implemented; deployment verification pending
- Last updated: 2026-08-07
- Owners: SkyPilot API server and infrastructure observability

## Goals

- Expose a stable, read-only HTTP endpoint that does not require SkyPilot
  authentication.
- Report GPU capacity by physical Kubernetes cluster and accelerator type as
  mutually exclusive used, preemptible, available, and unavailable buckets.
- Report the number and status distribution of active managed jobs per user.
- Keep the public response bounded and free of credentials, network addresses,
  workload definitions, commands, logs, job names, and private configuration.
- Avoid turning anonymous polling into unbounded Kubernetes or PostgreSQL load.

## Non-goals

- This endpoint does not launch, mutate, cancel, or refresh resources.
- It is not a replacement for authenticated cluster, job, workspace, or
  dashboard APIs.
- It does not expose per-node inventory, pod names, job names, YAML, logs,
  costs, cloud account details, namespaces, workspace policy, or IP addresses.
- It does not report public-cloud GPU availability outside the configured
  Kubernetes contexts.
- It does not provide historical capacity or job data.

## Public contract

The API server serves this exact route:

```text
GET /api/v1/public/capacity
```

The route requires no cookie, bearer token, basic-auth credential, or proxy
identity. Only the exact `GET` path is public; sibling paths, path prefixes,
other methods, and all existing APIs retain their current authentication
behavior. Supplying an invalid credential does not make a public request fail,
because authentication is not part of this route's contract.

The successful response is JSON with schema version 1:

```json
{
  "version": 1,
  "generated_at": "2026-08-07T12:00:00+00:00",
  "partial": false,
  "clusters": [
    {
      "name": "research-east",
      "status": "ok",
      "gpus": [
        {
          "type": "H200",
          "total": 128,
          "used": 80,
          "preemptible": 24,
          "available": 16,
          "unavailable": 8
        }
      ]
    }
  ],
  "jobs_by_user": [
    {
      "user": "user@example.com",
      "active_jobs": 3,
      "statuses": {
        "PENDING": 1,
        "RUNNING": 2
      }
    }
  ],
  "jobs_status": "ok"
}
```

`clusters` and each cluster's `gpus` list are sorted by name. `jobs_by_user` is
sorted by user. Cluster names and user display identities are intentionally
public under this contract; user hashes are not returned.

For a GPU row with known allocation data, these invariants hold:

```text
total = used + preemptible + available + unavailable
used, preemptible, available, unavailable >= 0
```

- `available` is unallocated capacity on ready, schedulable nodes.
- `preemptible` is allocated capacity held below the cluster's highest observed
  accelerator-holding priority tier and reclaimable by a higher-priority
  workload.
- `used` is allocated, non-preemptible capacity on ready, schedulable nodes.
- `unavailable` is capacity on not-ready, cordoned, or untolerably tainted
  nodes. It is not misreported as used.

If Kubernetes allocation visibility is incomplete, the affected row keeps its
known `total` and `unavailable` values but returns `null` for `used`,
`preemptible`, and `available`. The API never converts unknown allocation into
zero. A row is also treated as unknown rather than clamped if its source values
cannot satisfy the accounting invariant.

An unavailable cluster remains in `clusters` with `status` set to
`temporarily_unavailable` and an empty `gpus` list. An unavailable managed-jobs
source yields an empty `jobs_by_user` list and `jobs_status` set to
`temporarily_unavailable`. Either condition sets `partial` to `true`. The
response is also partial when any GPU row has unknown allocation. It does not
include raw exception text, paths, hosts, credentials, priority-class labels,
or provider diagnostics. Partial snapshots return HTTP 200 so callers can
retain healthy cluster data; HTTP 503 is reserved for failure to construct any
top-level snapshot.

Successful responses include:

```text
Cache-Control: public, max-age=15
```

## Architecture and invariants

The route is a direct API-server read rather than an executor request. An
anonymous caller therefore receives the result in one response and never needs
access to the authenticated `/api/get` request-result route.

The API server discovers the union of existing Kubernetes contexts allowed by
the configured workspaces. Each context is observed once per snapshot under the
lexicographically first configured workspace that admits it; that workspace's
node filters, tolerations, and context settings therefore define the public
view. This deterministic, conservative rule avoids unioning differently scoped
node inventories and accidentally publishing nodes hidden by every one of the
context's workspace views. Observation uses the existing
`get_kubernetes_node_info()` primitive, which is already the source of the
authenticated infrastructure dashboard's total, availability, node-health,
and preemptible accounting. Reusing that primitive keeps the public and
authenticated views semantically aligned.

Active managed jobs are read without refresh or controller mutation. In jobs
consolidation mode the API reads the centralized job state directly. Legacy
controller mode uses the existing read-only queue path without restarting the
controller. Multi-task rows are reduced to one active job before per-user
aggregation, so pipelines are not overcounted. A job with one active task
status uses that status; a job with multiple distinct active task statuses is
counted in a `MIXED` bucket. Terminal task rows belonging to a still-active
pipeline do not create additional status counts.

Each API-server process maintains a 15-second, single-flight in-memory cache.
The cache bounds repeated anonymous requests and concurrent refreshes within a
worker. Kubernetes contexts are queried concurrently with a fixed upper bound;
one context failure cannot cancel other observations. The cache is only a load
shield, not durable state, and restarts safely discard it.

The response projection is an allowlist. It copies only the fields defined in
the public schema. In particular, raw Kubernetes node objects and raw managed
job rows must never be serialized into the response.

## Authentication boundary

Basic-auth, bearer-token, header-auth-proxy, and OAuth2-proxy middleware share
one exact predicate for the route and method above. The predicate runs before
credential parsing, user registration, or proxy redirection and marks the
request anonymous for observability. The predicate must not use `startswith`,
a regular expression, or a caller-provided header. RBAC behavior for every
non-public route remains unchanged.

Ingress-level authentication outside the SkyPilot process may still protect
the route. Deployments that require this endpoint to be reachable before an
external ingress auth layer must configure that ingress to exempt the exact
path separately; application code cannot bypass an upstream proxy that never
forwards the request.

## Compatibility and versioning

This is an additive direct HTTP API. Existing clients and servers continue to
interoperate because no existing payload changes. The SkyPilot API version is
bumped so deployment tooling can identify the first server revision that
contains the endpoint. Callers of the public route do not need to send a
SkyPilot client-version header.

Schema `version` is independent from the SkyPilot client/server API version.
Additive fields may be introduced within version 1. Removing or changing the
meaning or type of an existing field requires a new public schema version and a
new path or an explicit compatibility period.

## Implementation phases

1. Add the checked-in contract and adversarially review its authentication,
   privacy, accounting, load, and partial-failure behavior.
2. Add typed response projection, context discovery, bounded aggregation,
   active-job reduction, and the short single-flight cache.
3. Register the exact route and the shared authentication bypass; bump the API
   version.
4. Add unit tests for accounting invariants, unknown values, partial failures,
   multi-task job deduplication, cache behavior, route registration, and every
   authentication middleware.
5. Deploy and manually compare the public result with the authenticated Infra
   and Jobs pages before announcing the endpoint.

## Deployment and rollback

The change has no durable-state migration, dual write, feature transition, or
controller protocol change. It can roll out with the normal API-server image.
Old replicas return 404 while new replicas return schema version 1 during a
rolling deployment; callers must tolerate 404 until version convergence.

Rollback consists of restoring the previous API-server image. That removes the
route and its exact auth exemption together and returns 404. No data cleanup is
required. Because no temporary compatibility or fallback path is introduced,
there is no stacked removal PR.

## Verification evidence

Implementation verification on 2026-08-07:

- The endpoint and adjacent authentication/version suites pass with 45 tests:
  `pytest tests/unit_tests/test_sky/server/test_public_capacity.py
  tests/unit_tests/test_sky/server/test_auth_middleware.py
  tests/unit_tests/test_sky/server/auth/test_oauth2_proxy.py
  tests/unit_tests/test_sky/users/test_viewer_route_coverage.py
  tests/unit_tests/test_serve_replica_api.py::test_replica_reads_have_a_distinct_api_capability_version
  -n 0`.
- `bash format.sh --files` passes for every changed Python file, including
  repository-wide mypy over 885 source files, pylint at 10.00/10, and the
  dashboard ESLint and Prettier checks.
- An in-process request through the full FastAPI middleware stack with an
  intentionally invalid `sky_` bearer token returns HTTP 200, schema version
  1, and `Cache-Control: public, max-age=15`.
- Unit coverage verifies that neighboring paths and `POST` do not match the
  authentication exemption; all four authentication middleware variants are
  covered individually.
- GPU bucket invariants, unavailable nodes, unknown allocation values,
  deterministic context deduplication, partial-source failures, multi-task job
  deduplication, terminal-task exclusion, both managed-job storage modes, and
  15-second cache reuse are covered.

## Open gates

- After deployment, run unauthenticated and invalid-credential `curl` checks
  through the production ingress.
- Compare per-cluster totals with the authenticated Infra page and per-user job
  totals with the authenticated Jobs page before announcing the endpoint.
