# Kubernetes kubeconfig exec-auth ownership

## Problem

`sky/provision/kubernetes/utils.py` is a 5,094-line compatibility facade for
several unrelated Kubernetes concerns.  Most of its kubeconfig context
resolution already lives in `sky/provision/kubernetes/context_utils.py`, but
the exec-auth rewrite and content-addressed cache still own YAML mutation,
filesystem output, credential-command normalization, and cache fallback in the
facade.  Those responsibilities change for credential transport reasons and
are independent from node observation, autoscaler discovery, placement, and
pod lifecycle behavior.

File size is only a prioritization signal here.  The bounded extraction is
justified by a complete two-operation state boundary and an existing owner,
not by moving arbitrary helpers out of a large file.

## Responsibility map

### Kubernetes API retry and provider error translation

Callers include provisioning, inventory, credential checks, storage, and pod
lifecycle helpers.  It depends on Kubernetes client exceptions, retry timing,
logging, and user-facing error translation.  Its state is per-call backoff;
its failures are missed retries, masked terminal errors, or changed exception
text.  External API latency and sleep counts are performance-sensitive, and it
changes with Kubernetes client behavior.

### Autoscaler discovery, node observation, and placement projection

Callers include the optimizer, scheduler, status, diagnostics, and provider
lifecycle capture.  It depends on provider APIs, label formatters, taints,
tolerations, resource quantity parsing, bounded response ownership, and request
caches.  It owns provider snapshots, byte and node budgets, and scheduling
projections.  Its failures include false feasibility, unbounded reads, resource
drift, or leaked responses.  Provider calls, response reads, allocations, and
hot node scans are performance-sensitive, and it changes with scheduling and
capacity policy.

### Kubeconfig exec-auth rewrite and cache

The Kubernetes cloud credential mount path and the standalone exec-kubeconfig
converter call this family through `sky.provision.kubernetes.utils`.  It
depends on YAML load/dump behavior, filesystem paths, the SkyPilot exec wrapper
constant, SHA-1 content addressing, warning projection, and Nebius profile
normalization.  It owns only output files and a content-addressed cache entry.
Its failures are recursive wrapper injection, path-sensitive cache misses,
incorrect executable or argument rewriting, corrupted YAML, lost fallback to
the original kubeconfig, or changed warnings.  File and YAML operation counts
are performance-sensitive, and it changes with credential transport and exec
plugin compatibility.

### Kubeconfig context and namespace resolution

Callers include core, resources serialization, metrics, provisioning, volumes,
and network configuration.  It already lives in
`sky/provision/kubernetes/context_utils.py` behind the historical facade.  It
depends on kubeconfig discovery, in-cluster credentials, workspace config, and
namespace fallbacks.  Its failures and cadence are authentication and context
selection concerns, making it the natural owner for the neighboring exec-auth
rewrite family.

## Solution

Move the complete exec-auth rewrite and cache implementations into the
existing `sky.provision.kubernetes.context_utils` module.  Keep the public
signatures and function definitions in `sky.provision.kubernetes.utils` as a
facade.  The cached wrapper passes the historical facade rewrite callable and
warning formatter into the implementation so monkeypatching and fallback
ordering remain late-bound without a reverse import.

Use plain functions.  No class, protocol, registry, adapter hierarchy,
strategy, factory, or new package is needed.  The implementation must preserve:

- public import paths, signatures, modules, qualified names, and callable
  identities in `sky.provision.kubernetes.utils`;
- executable basename normalization, wrapper recursion avoidance, missing
  `args` handling, Nebius `--profile sky` rewriting, and mutation order;
- output directory creation, UTF-8 YAML output, normalized content hashing,
  cache-hit short circuiting, and original-path fallback;
- facade-level rewrite monkeypatching, warning logger identity, exception
  formatting, and exact filesystem and YAML operation counts.

The facade remains the supported entrypoint.  `context_utils` becomes the sole
implementation owner, and no caller import changes are required.

## Alternatives considered

Leaving the family in place avoids a small structural change but retains a
credential-filesystem responsibility in the node and placement facade after
the neighboring context helpers have already moved.

Moving only `format_kubeconfig_exec_auth` is smaller but splits one rewrite and
cache lifecycle across modules.  Moving the high-performance-network enum is
also small, but it cannot preserve both its historical serialized module
identity and source-inspection behavior without compatibility tricks, and it
has only one production owner.  A new credential adapter or dependency
injection layer would add an unproven abstraction.

## Validation and rollout

Characterization must pass on the exact base before implementation.  It will
cover public callable contracts, wrapper and no-wrapper transforms, Nebius
profile normalization, existing-wrapper idempotence, content-addressed cache
hits and misses, facade monkeypatching, warning fallback, exact read/write and
YAML operation counts, and output bytes.

The changed-path matrix maps `context_utils.py` and the facade to the new
contract tests, Kubernetes cloud tests, Kubernetes utility tests, resource
serialization tests, workspace remote-identity tests, and converter tests.
Formatting, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall, both
import orders, smoke-test collection, and `git diff --check` are required.
Cold import and representative cache-hit and cache-miss timings will be
compared to the exact base, while characterization proves unchanged external
operation counts.

This is a structural extraction with no migration or runtime rollout.  Revert
the single extraction commit if any compatibility, performance, or CI gate
fails.  Merge only from the exact pushed SHA after all relevant checks and
review threads are green.
