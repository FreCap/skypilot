# Dashboard Kubernetes Infrastructure Gateway Extraction

_Created: 2026-07-20_

## Problem

`sky/dashboard/src/data/connectors/infra.jsx` combines four reasons to
change: workspace and enabled-cloud aggregation, Kubernetes node transport
and GPU projection, job and cluster count projection, and cloud accelerator
catalog transport and formatting. The 1,062-line connector remains the
stable dashboard entrypoint, but its Kubernetes request protocol and node
projection form a provider-specific leaf with different dependencies and
failure modes from the surrounding aggregation code.

The responsibility map is:

| Responsibility                                     | Callers                                                         | Dependencies and state                                                                         | Failure modes                                                                                     | Sensitivity and cadence                                                                       |
| -------------------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Enabled-cloud and workspace aggregation            | Infrastructure page and cache preloader                         | Workspace, cluster, and managed-job connectors plus dashboard cache; cache key identity        | Partial workspace failures, stale cache projections, incorrect cloud counts                       | Request-count and startup-latency sensitive; changes with workspace and page loading behavior |
| Kubernetes node transport and GPU projection       | Per-context progressive loader and legacy workspace aggregation | `/kubernetes_node_info`, request polling, node readiness and taint metadata; no retained state | Missing request IDs, unavailable contexts, partial multi-context failure, malformed node payloads | API-call-count and projection sensitive; changes with Kubernetes node metadata                |
| Job and cluster count projection                   | Infrastructure page and legacy workspace aggregation            | Cloud and region fields plus context-key normalization; no retained state                      | Misclassified Kubernetes, SSH, or Slurm context counts                                            | Linear scan sensitive; changes with infrastructure presentation                               |
| Cloud accelerator catalog transport and formatting | Accelerator detail consumers                                    | Accelerator list APIs and catalog tuple decoding; no retained state                            | Missing request IDs, malformed catalog tuples, price ordering drift                               | Response-size and parsing sensitive; changes with catalog APIs and presentation               |

The Kubernetes gateway has materially different callers, dependencies,
failure policy, and reason to change. It also matches the provider-specific
leaf already established by `infra-slurm.jsx`.

## Goals

Move the complete Kubernetes node request and GPU projection gateway to one
plain module while preserving the historical `infra.jsx` import path and
function identities. Keep request counts, polling order, partial-failure
behavior, output ordering, cache behavior, and user-visible results unchanged.

## Solution

Add `sky/dashboard/src/data/connectors/infra-kubernetes.jsx` containing the
node-readiness predicate, single-context projection, multi-context projection,
and request/poll transport. `infra.jsx` directly re-exports
`getContextGPUData` and imports the batch helper used by the legacy
`getWorkspaceInfrastructure` path. Direct export bindings avoid a forwarding
wrapper, additional call frame, and cache-key identity change.

The extraction is a facade-first plain-module gateway. An adapter, strategy,
class hierarchy, registry, or dependency-injection layer would add contracts
without a second implementation or policy variation.

## Behavior contract

- `getContextGPUData` remains importable from `@/data/connectors/infra` and is
  the identical callable exported by the Kubernetes gateway.
- One context performs one `/kubernetes_node_info` submission and one
  `/api/get` poll after receiving a request ID.
- Multi-context loading submits every context once, retains successful results
  when another context fails, and preserves deterministic context, node, and
  GPU ordering.
- Missing or unhealthy node metadata preserves current readiness, cordon,
  taint, CPU, memory, and accelerator defaults.
- `getWorkspaceInfrastructure` keeps the same aggregation, logging, and error
  policy, with no extra cache lookup or network request.

## Alternatives considered

Leaving the file unchanged avoids one module, but retains a provider-specific
transport protocol inside an otherwise cross-provider aggregator and makes its
failure policy harder to characterize independently. Moving workspace-context
discovery too would mix workspace cache ownership into the provider gateway.
Extracting only the request helper would leave node projection split across
modules and add indirection without transferring a complete responsibility.

## Milestones and rollout

1. Add characterization tests against the unsplit facade for callable behavior,
   request counts, partial failure, readiness projection, and deterministic
   ordering.
2. Move the gateway source without behavioral edits and preserve direct facade
   exports.
3. Run the mapped dashboard suites, lint, format check, production build, and
   diff checks. Compare moved function ASTs and dashboard bundle size.
4. Ship as a structural dashboard change. Rollback is a single PR revert and
   requires no data, configuration, or API migration.

## Test plan and CI mapping

| Changed path                        | Local evidence                                                                    | CI job                                                                |
| ----------------------------------- | --------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `infra-kubernetes.jsx`, `infra.jsx` | Kubernetes connector characterization plus infrastructure page and refresh suites | Dashboard Tests                                                       |
| `infra-kubernetes.test.jsx`         | Focused Jest run                                                                  | Dashboard Tests, with the test explicitly selected in `dashboard.yml` |
| `dashboard.yml`                     | YAML inspection and exact local command selection                                 | Dashboard Tests                                                       |
| This design                         | `git diff --check`                                                                | Format and static-analysis workflows                                  |

The dashboard workflow has no pull-request path filter. It must explicitly
collect the new characterization test, then run Jest, ESLint, Prettier, and the
Next.js production build on the exact PR head.
