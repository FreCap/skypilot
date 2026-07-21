# GPU CLI Slurm Gateway Extraction

## Problem

`sky/client/cli/gpus.py` owns the stable `sky gpus` command family, but its
1,040-line `_show_gpus_impl()` mixes command validation and catalog rendering
with two independent live-capacity integrations. The Slurm integration owns a
separate request protocol, backward-compatible response decoding, partial
failure policy, partition aggregation, and verbose presentation. Keeping that
provider gateway nested inside the command renderer makes changes to Slurm
capacity reporting require editing the same closure as Kubernetes and cloud
catalog output.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| CLI validation and JSON output | `show_gpus`, `gpus_list` | Click, cloud registry, SDK JSON models | Parsed command options | Invalid option combinations or wire-shape drift | One enabled-cloud lookup and one catalog request | CLI UX and API compatibility |
| Kubernetes and SSH live capacity | Table-output generator | Kubernetes SDK endpoints, node metadata, config, readiness utilities | Per-invocation context results and hints | Missing contexts, partial permissions, malformed node data | Request count and node projection | Kubernetes and SSH metadata |
| Slurm live capacity | Table-output generator | Slurm availability and node-info SDK endpoints, response compatibility tuple, table utilities | Per-invocation cluster results, failures, and aggregate counts | Old-server tuples, unreachable clusters, malformed partition metadata | Request count, dispatch ordering, and linear aggregation | Slurm API and scheduler metadata |
| Cloud catalog sorting and presentation | Table-output generator | Accelerator catalog API, pandas, table utilities | Per-invocation sorted catalog rows | Empty or malformed catalog rows and price-order drift | Dataframe allocation and sorting | Catalog schema and presentation |
| GPU labeling command | Direct CLI caller | Kubernetes label SDK endpoint and streamed result | One request result | Labeler failure or nonzero result | One remote operation | Kubernetes labeling lifecycle |

The Slurm live-capacity family has materially different callers, dependencies,
failure policy, and reasons to change from command validation, Kubernetes node
projection, and cloud-catalog sorting. It is a complete low-state leaf: it can
own request dispatch, compatibility decoding, aggregation, error containment,
and rendering without owning command options or the outer output sequence.

## Behavior contract

- Keep `sky.client.cli.gpus` and `sky.client.cli.command` as the stable command
  facades. Command registration, help text, callback identity, pickle identity,
  and output ordering remain unchanged.
- Preserve old two-element and new three-element Slurm availability tuples.
- Preserve the existing remote-call count and ordering. In verbose mode,
  dispatch all `slurm_node_info` requests before streaming any result.
- Preserve aggregate and per-cluster tables, partial-cluster errors, warnings,
  ANSI styles, empty-result messages, and the v0.13.0 compatibility TODO.
- Keep module imports at the top level and preserve the historical logger name
  `sky.client.cli.command`.
- Add no wrapper class, abstract interface, registry, dependency injection
  layer, or behavioral optimization.

## Design

Move the complete Slurm availability and presentation family to the plain
module `sky/client/cli/gpus_slurm.py`. The module exports two internal
functions: one builds availability tables and one yields the rendered output.
The command facade imports those functions directly and retains control of
provider selection and output sequencing.

The quantity-list formatter is small shared presentation behavior, so pass it
as a callable to the table builder instead of moving Kubernetes behavior or
creating a new common abstraction. The renderer owns its verbose per-partition
lookup because request dispatch, partial failure handling, aggregation, and the
resulting warning are one invariant-complete Slurm operation.

## Alternatives considered

- Keep the file intact. This avoids movement, but leaves a complete provider
  gateway embedded in a 1,040-line closure despite independent protocol and
  failure behavior.
- Extract Kubernetes and SSH first. That family is larger, but it captures
  command flags, configuration, context filtering, readiness policy, and
  multiple formatting callbacks. Moving it cleanly would require a new context
  object or broad parameter surface and is too stateful for this pass.
- Introduce a provider strategy hierarchy. There is no proven interchangeable
  contract: cloud catalogs, Kubernetes, SSH, and Slurm return different models
  and render different sections. A strategy abstraction would add ceremony
  without a second implementation of one contract.

## Milestones

1. Add public-CLI characterization for compatibility tuples, partial failures,
   aggregate counts, verbose dispatch ordering, and unreachable clusters.
2. Move the three Slurm nested functions without changing their control flow.
3. Replace the two outer call sites with direct module-function calls.
4. Verify AST equivalence, focused and component tests, CLI identity, formatting,
   static analysis, import cost, and the exact CI job mapping.

## Test and rollout plan

The changed-path matrix is:

| Changed path | Coverage |
| --- | --- |
| `sky/client/cli/gpus_slurm.py` | New Slurm gateway characterization plus existing Slurm aggregation, filtering, verbose, and failure tests in `test_cli_gpus_list.py` |
| `sky/client/cli/gpus.py` | GPU CLI unit suite, JSON output suite, root registration and pickle-identity suite, and `tests/test_cli.py` |
| `docs/designs/cli-gpus-slurm-gateway.md` | Documentation and format checks |

`.github/workflows/pytest.yml` has no pull-request path filter. Its Unit Tests
job collects the gateway and registration suites, while CLI Tests runs
`tests/test_cli.py`. Run the focused tests before and after movement, then the
relevant component suites, `bash format.sh --files`, `git diff --check`, and an
alternating cold-import comparison. Merge only after the exact branch head has
a fully green visible CI rollup and no actionable review thread.
