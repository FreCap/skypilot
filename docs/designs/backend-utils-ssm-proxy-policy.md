# Backend SSM ProxyCommand Policy

## Problem

`sky/backends/backend_utils.py` is 5,237 lines and owns several unrelated
backend responsibilities. It builds launch configuration, manages file mounts
and credentials, probes Ray and cluster status, refreshes persistent cluster
records, renders cluster listings, resolves endpoints, and exposes process and
lock helpers.

The file also owns a cohesive AWS SSM ProxyCommand compatibility policy:
constructing an exec-safe adaptive-retry wrapper, guarding empty instance-ID
lookups, and upgrading persisted legacy commands. This policy is pure string
transformation with AWS CLI and OpenSSH compatibility failure modes. It changes
independently from the stateful cluster-status and lifecycle orchestration that
dominates the rest of the module.

The goal is not to split `backend_utils.py` by size. It is to give this
independently evolving compatibility policy one owner while retaining
`backend_utils` as the stable facade.

## Responsibility map

### SSM ProxyCommand compatibility policy

Callers are the SSM branch of `write_cluster_config`,
`ssh_credential_from_yaml`, `ssh_credentials_from_handles`, and focused SSM
tests. Dependencies are `re`, `shlex`, AWS CLI command syntax, OpenSSH percent
token expansion, and persisted legacy command forms. It owns no mutable state,
only immutable parsing and rendering constants.

Failure modes include invoking SSM with an empty target, losing the EC2 lookup
exit status, producing an `export` prefix that OpenSSH cannot execute, failing
to repair the briefly shipped unescaped `printf` form, changing profile or
port placeholders, and losing idempotence. It is called during config
generation and credential reads. Its cost must remain one bounded regular
expression match and string transformation with no I/O. It changed in four
independent AWS fixes during July 2026.

### Cluster configuration and credential assembly

Callers are launch and provisioning flows, the cloud VM backend, provisioner,
and tests. Dependencies include cloud resource deploy variables, workspace and
provider configuration, credential mounts, authentication keys, volumes,
managed images, YAML templates, and persistent cluster YAML.

It owns transient launch configuration dictionaries and credential
projections. Failure modes include invalid cloud configuration, lost
credentials, incorrect launch templates, incompatible persisted YAML, or
changed SSH behavior. Its performance depends on avoiding redundant provider,
configuration, filesystem, and database work. It changes with launch,
workspace, provider, image, volume, and identity features.

### Cluster health and persistent status orchestration

Callers are core cluster APIs, managed jobs, Serve, server daemons, dashboard
reads, and the cloud VM backend. Dependencies include cloud status APIs,
Skylet and SSH probes, persistent global state, ownership and resource locks,
autostop reconciliation, and Kubernetes diagnostics.

It owns refresh snapshots, lock-scoped state transitions, retry budgets, and
failure diagnostics. Failure modes include stale or destructive status
transitions, redundant provider calls, lock races, controller stalls, and
incorrect autostop reconciliation. It is a latency- and query-sensitive hot
path and changes with lifecycle, caching, recovery, and provider behavior.

### Presentation and miscellaneous backend helpers

Callers include CLI and dashboard cluster listings, endpoint APIs, file sync,
task rendering, and process signal handlers. Dependencies span formatting,
filtering, task resources, command runners, network checks, and locks.

This is evidence that `backend_utils.py` remains mixed after the bounded
extraction, not a claim that all remaining helpers should be split. Each
future seam requires an independent responsibility map and safety case.

## Solution

Create `sky/backends/ssm_proxy.py` as a plain module containing the unchanged
SSM constants and three pure transformation functions. Keep the historical
private constants and functions in `sky.backends.backend_utils` as direct
aliases to the exact same objects.

Production callers inside `backend_utils` continue resolving the historical
facade globals. This preserves existing monkeypatch behavior and avoids a
forwarding frame. The implementation functions retain their historical
`sky.backends.backend_utils` module identity so introspection and any
unexpected private-function serialization remain compatible.

The extraction moves the complete policy. It does not move AWS launch
selection, profile lookup, credential dictionary assembly, or SSH command
execution. Those remain owned by their existing layers.

No abstract class, protocol, registry, dependency-injection layer, adapter, or
strategy is introduced. There is one deterministic implementation and plain
functions are the smallest sufficient abstraction.

## Behavior contract

The following must be identical before and after extraction:

1. All three normalized function ASTs.
2. All facade constant values and function object identities.
3. Historical `__module__` and pickle globals for the three functions.
4. Import-order behavior for `backend_utils` and `ssm_proxy`.
5. Exact output for plain, legacy `export`-prefixed, already wrapped, guarded
   with the old percent escape, malformed quoted, unrelated, and null inputs.
6. Idempotence of upgraded commands.
7. Real `/bin/sh` and OpenSSH behavior already characterized by
   `tests/unit_tests/test_ssm_proxy_command.py`.
8. Config-writing and single/batched credential call counts and outputs.

No database, configuration, YAML, CLI, remote-command, or public API format
changes are allowed.

## Alternatives considered

Leaving the policy in place avoids one module but keeps AWS/OpenSSH
compatibility mixed into a high-fan-in cluster orchestration module. The policy
has already changed repeatedly for reasons unrelated to status or lifecycle,
and it has a complete pure boundary.

Moving only legacy upgrade or only empty-target guarding would split one
normalization protocol and leave ownership ambiguous.

Moving all SSH credential assembly would combine generic credentials,
Kubernetes control-master policy, Docker users, persisted YAML, and SSM
compatibility. That boundary is materially larger and more stateful.

Moving the code under `sky/provision/aws` would imply provider provisioning
ownership, but the policy also runs when reading persisted credentials outside
provisioning. The backend module is the narrower cross-lifecycle owner.

Forwarding wrappers would preserve facade lookup but add indirection and change
callable identity. Direct aliases preserve both behavior and performance.

## Milestones

### v0: Characterize

Add a focused contract that records the normalized ASTs, facade identities,
module identities, imports, outputs, and call counts on the unsplit base. Run
it together with the existing SSM, backend configuration, and credential tests.

### v1: Extract

Move the complete policy unchanged, install direct aliases in `backend_utils`,
and rerun the characterization and component suites. Compare normalized ASTs
and cold import timing against an untouched base.

## Test and rollout plan

The changed-path-to-test matrix is:

| Changed path | Responsibility | Tests and checks |
| --- | --- | --- |
| `sky/backends/ssm_proxy.py` | SSM parsing and rendering | new contract, `test_ssm_proxy_command.py` |
| `sky/backends/backend_utils.py` | facade and internal callers | new contract, `test_backend_utils.py`, AWS and Ray-ready focused tests |
| contract test | compatibility evidence | Python 3.11 and 3.14 focused pytest |
| this design | canonical contract | diff and documentation review |

Run focused tests before and after the move, the relevant backend unit suite,
`bash format.sh --files` for every changed Python file, `git diff --check`,
Python 3.11 pytest, Python 3.14 compile and structural probes, and an
alternating cold-import benchmark. The PR must map changed paths to
pull-request workflows and remain open unless every visible relevant check
passes on the exact pushed SHA with no actionable review thread.

Rollback is a normal revert because there is no migration, persisted-data
rewrite, public API change, or behavioral optimization.
