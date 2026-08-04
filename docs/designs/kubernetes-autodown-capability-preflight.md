# Kubernetes autodown capability preflight

## Problem

`sky launch -i 60 --down` is a promise: the cluster deletes itself after 60
idle minutes. The node keeps that promise from the inside. `StopEvent._run`
(`sky/skylet/events.py`) wakes every 60s, and once the idle threshold is
crossed calls `_stop_cluster` ->
`provision_lib.terminate_instances`, using whatever identity the node runs
as.

On Kubernetes that identity is the pod's ServiceAccount. SkyPilot only
provisions the pod-management Role/RoleBinding when the pod uses SkyPilot's
own default ServiceAccount. `sky/provision/kubernetes/config.py`:

```python
requested_service_account = config.node_config['spec']['serviceAccountName']
if requested_service_account == kubernetes_utils.DEFAULT_SERVICE_ACCOUNT_NAME:
    # ... set up roles and bindings ...
elif requested_service_account != 'default':
    logger.info(f'Using service account {requested_service_account!r}, '
                'skipping role and role binding setup.')
```

The comment states the contract: "If the user has requested a different
service account (via pod_config in ~/.sky/config.yaml), we assume they have
already set up the necessary roles and role bindings."

When that assumption is wrong, nothing detects it. The failure is
completely silent:

1. `set_autostop` succeeds -- storing an autostop config needs no RBAC.
2. `global_user_state.set_cluster_autostop_value` records `60 / to_down`,
   so `sky status` reports `AUTOSTOP  1h (down)` forever.
3. At the idle threshold `terminate_instances` raises 403.
4. `_stop_cluster` clears the autostopping indicator and re-raises.
5. `SkyletEvent.run` swallows the exception (`logger.error`, keep the
   skylet alive) and retries in 60s.
6. Because the indicator is cleared on every failure, the API server never
   observes `AUTOSTOPPING`; no cluster event is written; `sky status` stays
   serene.

Steps 3-6 repeat every minute for the life of the cluster.

### Observed incident

2026-08-01, `abbvie-esmfold2` on `prod_research_cluster_eks`:

```
sky launch -c abbvie-esmfold2 --config active_workspace=rescluster-k8s-prod-east1 \
    -i 60 --down -y --retry-until-up scripts/peptide_screen/esmfold2/fold_screen.sky.yaml
```

- 23:25:15 cluster provisioned (1 node, `A100:8`).
- 23:25:21 job 1 `peptide-screen` submitted; FAILED at 23:25:38 after 0.74s.
- The cluster then sat `UP` with `AUTOSTOP 1h (down)` for **48 hours**,
  holding 8 A100s on a shared research cluster, until a human tore it down
  by hand on 2026-08-04 01:03.

The deployment sets, globally:

```yaml
kubernetes:
  pod_config:
    spec:
      serviceAccountName: skypilot-pool-sa
```

`skypilot-pool-sa` has no pods RBAC in `rescluster-k8s-prod-east1`. Verified
from inside a live SkyPilot pod on that cluster:

```
delete pods -> allowed=False
get    pods -> allowed=False
list   pods -> allowed=False
```

and a direct `GET` of the pod's own object returns HTTP 403. So autodown
could never have worked for *any* cluster in this deployment, and none of
the 116 recorded autodown events across the fleet came from this namespace.

## Why the check must run on the node

The question is "may **this node** delete itself?", and only the node can
answer it cheaply and correctly:

- The API server's own kube credentials describe the API server, not the
  pod, so a `SelfSubjectAccessReview` from there answers the wrong
  question.
- `SubjectAccessReview` would let the API server ask on the pod's behalf,
  but creating one is itself a privileged verb. On the cluster in the
  incident the API server gets 403 issuing it, so a server-side check
  degrades to "unknown" exactly where it is needed.
- A `SelfSubjectAccessReview` issued *from inside the pod* describes the
  runtime identity precisely, and every Kubernetes cluster grants
  `create selfsubjectaccessreviews` to `system:authenticated` through the
  built-in `system:basic-user` ClusterRole. Verified working from a live
  SkyPilot pod on the affected cluster.

## Behavior contract

Capability and policy are split. The node answers *"could I execute this
teardown?"*; the launch layer decides *"is a no fatal?"*, because only it
knows who asked for the autodown.

### Capability: `sky/skylet/autostop_preflight.py`

Runs inside the skylet, on the `SetAutostop` path only, and only when
**all** of these hold:

1. `down` is true and `idle_minutes >= 0` (an autodown is being armed).
   Cancels (`idle_minutes < 0`) and plain autostop are unaffected. Plain
   autostop is already rejected for Kubernetes upstream of this.
2. `/var/run/secrets/kubernetes.io/serviceaccount/token` exists -- we are
   in a pod with in-cluster auth. Non-Kubernetes clusters never reach the
   check and never import the `kubernetes` package.
3. The node's cluster YAML names the `kubernetes` provider -- the same
   file `StopEvent._stop_cluster` reads to pick the cloud.

When it runs, it issues a `SelfSubjectAccessReview` for `delete` on `pods`
in the pod's own namespace.

- Explicit `allowed == False` -> `set_autostop` raises before persisting
  anything. The skylet aborts the RPC with `FAILED_PRECONDITION`, which the
  client surfaces as `exceptions.NotSupportedError` with an actionable
  message. No autostop config is stored, and
  `global_user_state.set_cluster_autostop_value` is never reached, so
  `sky status` does not display an autostop that cannot happen.
- Anything else -- review not issuable, connection error, empty status --
  is treated as "unknown" and the arm proceeds exactly as today. A probe
  failure must never cost a working cluster its autostop.

### Policy: `execution_autostop.apply_launch_autostop`

The PRE_EXEC stage arms the config through this helper, which handles the
refusal according to who asked for the autodown:

- **The user did** (`sky launch -i N --down`, `sky autostop --down`):
  fatal. The promise cannot be kept, so fail rather than hand back a
  cluster that will sit there billing under a serene `AUTOSTOP  Nm (down)`.
- **SkyPilot did**: not fatal. Managed-job clusters get
  `idle_minutes_to_autostop=10, down=True` purely as a leak backstop in
  case the controller dies (`jobs/recovery_strategy.py`), and
  `CloudVmRayBackend.set_autostop` force-converts a Kubernetes SkyServe
  controller to autodown regardless of what the user asked for. Failing
  those launches would trade a backstop that was never going to fire for
  an outage. They proceed, and the helper re-arms with `idle_minutes=-1,
  down=False` so autostop is explicitly *cleared* rather than left
  advertised -- the same hooks payload rides along, so lifecycle hooks are
  not lost.

The discriminator is `not is_managed and controller is None`, both already
computed in `_execute`.

### User-visible effect

`sky launch -i 60 --down` on such a cluster now fails at the PRE_EXEC stage
with a message naming the namespace and the missing verb, instead of
succeeding and leaking the cluster. The cluster is left provisioned (the
normal SkyPilot behavior for a failed launch stage) and the error tells the
user to `sky down` it.

`sky autostop --down <cluster>` fails immediately with the same message.

`sky jobs launch` onto the same infrastructure keeps working, with a
warning that the backstop is off.

This mirrors the existing policy in `sky/execution_autostop.py`, whose
`autostop_requested_features` docstring already frames this exact class of
bug: "the launch accepts the config and the skylet's stop attempt then
fails forever at idle time, leaving the cluster in AUTOSTOPPING while it
keeps billing". That helper catches the cloud-capability version of the
problem (AWS one-time spot cannot stop). This catches the
node-identity version, which no cloud-level feature flag can express.

## Alternatives considered

**Grant the RBAC from SkyPilot.** SkyPilot deliberately does not touch RBAC
for an operator-supplied ServiceAccount, and in general lacks permission to
create Roles in the target namespace. Rejected: this is an operator
decision, and the incident's real remedy is an operator one. The product
fix is to stop failing silently.

**Warn instead of raising.** The incident launch ran with `-y` inside an
agent session; a warning in a 400 KB provision log is indistinguishable
from silence. The whole failure mode is "nobody noticed for 48 hours".

**Report the repeated teardown failures instead of preventing the arm.**
Strictly more general -- it would also catch revoked IAM roles, deleted
contexts and transient-turned-permanent API errors, none of which this
preflight covers. It needs a new field on `IsAutostoppingResponse` (or a
new RPC) plus server-side plumbing to surface it in `sky status`, and it
only reports *after* the first idle window has already been missed. Left as
a follow-up; see below.

**Check at provision time in `bootstrap_instances`.** It has the final
merged `serviceAccountName` and the namespace, but not the autostop intent,
and it runs with the API server's credentials -- which is the identity that
cannot answer the question.

## Known limitations

- Only Kubernetes, only autodown, only an explicit denial. Every other way
  a teardown can permanently fail is still silent.
- Managed-job clusters and controllers keep launching without a backstop,
  so a dead controller still leaks them -- as it did before this change,
  except the warning now says so.
- Clusters armed *before* this change keep their stored config; a later
  re-launch that carries it forward will now fail the arm. That is the same
  broken promise surfacing, but it surfaces on an unrelated `sky launch`.
- `SKYLET_VERSION` is bumped (43 -> 44) so existing clusters pick the check
  up; their skylets restart on the next launch/start, as with any skylet
  fix.

## Follow-up

Surface permanent idle-teardown failures to the API server, so any cluster
whose autodown keeps failing shows up as such in `sky status` rather than
as a healthy `AUTOSTOP 1h (down)`. That subsumes this preflight's coverage
for causes the preflight cannot predict.

## Test plan

Unit (`tests/unit_tests/test_autostop_preflight.py`):

- denial (`allowed=False`) -> reason returned; `set_autostop` raises and
  persists nothing.
- allowed -> `None`; `set_autostop` stores the config.
- review raises -> `None` (graceful degradation), config stored.
- not in a pod (no SA token) -> `None`, and the provider probe is never
  reached.
- non-kubernetes provider in the node YAML -> skipped.
- unreadable node YAML -> skipped.
- `down=False` and `idle_minutes < 0` -> skipped.
- `apply_launch_autostop`: a refusal raises when `refusal_is_fatal`, and
  otherwise re-arms with `idle_minutes=-1, down=False`; an accepted arm
  calls `set_autostop` exactly once.
- `FAILED_PRECONDITION` from the skylet surfaces as `NotSupportedError` on
  the first attempt, with no retries.

Manual:

```bash
# On a Kubernetes context whose pod ServiceAccount lacks pods/delete:
sky launch -c t --cloud kubernetes --cpus 2 -i 60 --down -y echo hi
# expected: launch fails at PRE_EXEC naming the namespace + 'delete pods';
#           `sky status` shows no autostop for t.
sky down t

# On a context where the ServiceAccount does have pods/delete:
sky launch -c t2 --cloud kubernetes --cpus 2 -i 1 --down -y echo hi
sky status   # AUTOSTOP 1m (down); cluster is gone within ~2 minutes.
```
