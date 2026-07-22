# Controller Dependency Installation Extraction

_Created: 2026-07-22_

## Problem

`sky/utils/controller_utils.py` is 1,513 lines and currently owns four
materially different responsibility families: controller bootstrap dependency
commands, controller resource and config projection, two-hop file and storage
translation, and controller capacity and admission policy. Its size is only a
prioritization signal. The extraction is justified because dependency command
generation is a complete stateless leaf with its own provider dependencies,
remote-shell failure modes, tests, and change cadence.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Controller dependency command generation | `shared_controller_vars_to_fill` plus direct smoke-test probes | Enabled cloud and storage discovery, provider classes, dependency metadata, Skylet shell constants | None beyond local command accumulators | Missing packages, invalid shell ordering, provider CLI installation, or architecture mismatch | One credential-cache read per capability and linear work in enabled providers | Provider support, dependency pins, and controller bootstrap fixes |
| Controller resource and config projection | Jobs and Serve launch paths, core status, backend setup | Config, resources, plugins, cloud feasibility, and controller identity | Projected resource and environment dictionaries | Invalid resources, unsupported HA, infeasible clouds, or plugin rejection | Optimizer inputs and controller launch latency | Controller sizing, config, and plugin policy |
| Two-hop file and storage translation | Jobs and Serve submission | Blob storage, storage mounts, task mutation, proxy commands, and temporary files | Mutated task mounts and uploaded storage artifacts | Upload failure, cleanup ownership, path collision, or proxy rewrite error | File copies, uploads, and storage scans | File-mount and storage semantics |
| Controller capacity and admission policy | Jobs scheduler, Serve control loops, API server sizing, and service launch | Memory sizing, consolidation signals, jobs state, Serve state, and request-scoped caches | Consolidation warnings and cached capacity calculations | Stale signals, oversubscription, incorrect launch budget, or misleading limits | Hot control-loop scans and request call counts | Capacity policy and controller deployment topology |

## Solution

Move `_get_cloud_dependencies_installation_commands()` unchanged to
`sky/utils/controller_dependency_installation.py`. Keep the historical
`sky.utils.controller_utils` symbol as a direct alias and retain its module and
pickle identity. `shared_controller_vars_to_fill()` remains in the facade and
continues to perform a late-bound lookup of the facade symbol, preserving the
existing monkeypatch seam.

The extracted implementation is a plain module function. It adds no class,
protocol, registry, strategy, dependency injection layer, or package
hierarchy. Provider modules and dependency metadata remain the existing source
of truth.

Preserve these behavior contracts:

1. Provider discovery, sorting, and command ordering are unchanged.
2. GCP and Kubernetes authentication setup retains its order dependency.
3. Azure, Nebius, Cudo, Vast, IBM, and storage-only special cases are
   unchanged.
4. Package accumulation remains deduplicated and sorted, including the Ray
   compatibility pin for Click.
5. Command progress numbering and shell operator grouping remain byte
   identical for identical inputs.
6. The historical facade and pickle identity remain
   `sky.utils.controller_utils._get_cloud_dependencies_installation_commands`.
7. The direct alias adds no call frame, cache read, subprocess, or allocation.

## Alternatives considered

Keeping the function in `controller_utils.py` avoids one module, but leaves
provider bootstrap logic mixed with task mutation and controller capacity
policy. This function has had independent provider and shell-ordering fixes
and already has a focused characterization family, so the module cost is
smaller than the ownership improvement.

Moving the function to `sky/setup_files/dependencies.py` was rejected because
that module owns declarative package metadata. Controller bootstrap also owns
remote shell command construction and enabled-provider discovery, which would
reverse the dependency direction from metadata into runtime orchestration.

Extracting two-hop file translation would remove more lines, but that path
mutates tasks and storage ownership across a 331-line workflow. It is a more
stateful seam and is not needed to make this bounded extraction useful.

Extracting capacity policy was rejected for this pass because request caches,
consolidation warnings, jobs persistence, Serve state, and hot control-loop
call counts form a higher-risk cluster.

## Milestones and validation

1. Add and run facade and pickle-identity characterization on the unsplit
   implementation.
2. Move the function without behavioral edits, keep a direct facade alias,
   and prove normalized AST identity.
3. Run the full controller-utils unit suite, focused jobs and Serve callers,
   formatting, static analysis, import checks, and diff checks.
4. Measure alternating cold imports and confirm that the direct alias adds no
   runtime operation.
5. Merge only after the full visible exact-head CI rollup and review state are
   green. Rollback is a single revert because no API, config, persistence, or
   remote-command format changes.
