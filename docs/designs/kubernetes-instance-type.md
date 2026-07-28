# Kubernetes Instance Type Ownership

## Context

`sky/provision/kubernetes/utils.py` owns several unrelated Kubernetes concerns:

| Responsibility | Callers | Dependencies | State | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Virtual instance type formatting, parsing, validation, and construction | Kubernetes cloud resource projection, Kubernetes catalog, autoscaler fit policy, scheduling fit checks, and unit tests | `math`, `re`, and `common_utils.format_float` | Per-object CPU, memory, accelerator count, and accelerator type | Invalid names, changed round trips, wrong accelerator rounding, or serialized identity drift | Pure bounded string and numeric operations | Virtual resource naming and accelerator compatibility |
| Kubernetes API retry and provider error translation | Provisioning, inventory, credentials, node, pod, storage, and status helpers | Kubernetes adaptor exceptions, retry timing, logging, and user-facing errors | Per-call retry state | Misclassified errors, altered retries, or masked terminal failures | External API latency and sleep counts | Kubernetes client and error UX |
| Autoscaler discovery and capacity projection | Resource feasibility, cluster checks, instance projection, and scale-to-zero scheduling | GKE, Karpenter, CoreWeave, Nebius, provider APIs, and label formatters | Provider inventory and request-scoped caches | False feasibility or stale capacity | External provider queries and pool enumeration | Autoscaler providers and queue semantics |
| Node and pod inventory, scheduling fit, and accelerator allocation | Optimizer, provisioner, status, dashboard, GPU listing, and health checks | Kubernetes models, labels, taints, tolerations, affinity, and configuration | Projected node and pod snapshots | Oversubscription, false infeasibility, or incorrect allocation | Hot node and pod scans plus API calls | Scheduler policy and resource accounting |
| Credentials, kubeconfig projection, SSH transport facade, and pod configuration | CLI checks, provisioning, backend connection setup, and pod launch | Kubeconfig, auth plugins, filesystem, shell transport, and Kubernetes object models | Kubeconfig and generated pod specifications | Authentication failures, unreachable pods, or invalid specifications | Import path, filesystem, and external command costs | Authentication, transport, and pod features |

The instance type value object has materially different callers, dependencies,
state, failure modes, and reasons to change from the transport and orchestration
families. It is also the Kubernetes counterpart of
`sky/provision/slurm/instance_type.py`.

## Behavior contract

- Keep `sky.provision.kubernetes.utils.KubernetesInstanceType` as the stable
  public entrypoint.
- Preserve the class object, `__module__`, pickle identity, method behavior,
  exception messages, accepted name grammar, accelerator name handling, and
  rounding behavior.
- Preserve existing monkeypatch behavior for `common_utils.format_float`,
  `re.compile`, and `math.ceil`.
- Add no wrapper call frame, registry, abstract base class, or dependency
  injection layer.
- Change no Kubernetes API calls, caches, serialized formats, configuration, or
  user-visible output.

## Chosen boundary

Move the unchanged class to
`sky/provision/kubernetes/instance_type.py`. Import it into
`sky/provision/kubernetes/utils.py` as a direct alias and restore its historical
`__module__` value through the facade.

This is a facade-first plain-module extraction. The class is a complete
low-state leaf, and a direct alias preserves identity without forwarding
indirection. The new module mirrors the established Slurm ownership boundary.

## Alternatives

- Keep the class in `utils.py`: behavior is safe, but the domain value object
  remains owned by a transport and orchestration grab bag despite having
  independent callers and no Kubernetes API dependency.
- Move resource quantity parsers with it: rejected because those functions
  parse Kubernetes API quantities for inventory and allocation callers, while
  the class owns SkyPilot's virtual instance type name contract.
- Introduce a protocol, base class, or shared Kubernetes and Slurm hierarchy:
  rejected because there is no second interchangeable implementation behind
  one runtime contract, and the two providers currently differ in behavior.
- Update every caller to the new module immediately: rejected because retaining
  the utilities facade minimizes compatibility risk. Callers can migrate
  independently only when that provides a concrete import-time benefit.

## Milestones

1. Add characterization tests for formatting, parsing, facade identity,
   pickling, and dependency patch seams, and run them on the unsplit base.
2. Move the class without behavioral edits and retain a direct facade alias.
3. Prove normalized AST identity and run focused Kubernetes caller tests.
4. Run formatting, type, lint, import, diff, and representative import-time
   checks.
5. Publish only if the exact PR head has a complete green CI rollup and no
   actionable review thread.

## Rollout and compatibility

This is an internal structural extraction with no feature flag or data
migration. The historical facade remains the serialization authority. If CI or
identity probes find drift, revert the extraction as one commit.

## Changed path to test matrix

| Changed path | Local evidence | CI job |
| --- | --- | --- |
| `sky/provision/kubernetes/instance_type.py` | Instance type unit tests, normalized AST, compile, lint, and import probes | Unit Tests and static analysis jobs |
| `sky/provision/kubernetes/utils.py` | Facade identity, pickle, patch seams, autoscaler and fit-policy callers | Unit Tests and Limited Deps |
| `tests/unit_tests/kubernetes/test_instance_type.py` | Passes on the unsplit base and extracted head | Unit Tests |
| `docs/designs/kubernetes-instance-type.md` | Design review and diff checks | Format and static analysis |
