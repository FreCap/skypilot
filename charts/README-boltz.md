# boltz fork of `charts/skypilot`

`charts/skypilot` in this repo is a **fork** of the upstream SkyPilot Helm chart,
carrying a small boltz-specific delta on top. We forked (rather than wrapped with a
parent/umbrella chart) so the chart versions and ships next to the fork's `sky/`
code and can be published to our private ECR OCI registry by
`.github/workflows/boltz-chart-publish.yml`.

Upstream's own chart is still published, unchanged, to `helm.skypilot.co`; we do not
touch that path. Our fork chart is pushed only to
`oci://699626303757.dkr.ecr.us-east-1.amazonaws.com/helm-charts/skypilot` and consumed
by the platform repo's terraform `helm_release`.

## The boltz delta

The entire delta is the **portable external load balancer** support for
controller-owned SkyServe LBs, gated behind `serve.externalLoadBalancer.enabled`
(default `false`, so the rendered workload is unchanged for anyone not opting in).
SkyServe itself is external-LB-only in this fork: a deployment that starts services
must enable this chart capability. The chart injects
`SKYPILOT_SERVE_EXTERNAL_LB_ENABLED=true`, which is the runtime source of truth;
operators do not need to maintain a second copy of the flag in SkyPilot config.
That capability signal also forces service-controller consolidation in the API
pod (pools retain their independent jobs-controller setting), so enabling the
chart cannot accidentally launch an obsolete dedicated controller VM.

The LB no longer connects through a shared controller Service or a preallocated
controller port range. Each per-service LB sends syncs to the existing API Service:

```text
per-service LB Deployment
  -> http://<fullname>-api-service.<namespace>.svc
  -> /api/internal/serve/<service>/controller/load_balancer_sync
  -> any API pod resolves the current controller owner from shared Serve state
  -> exactly one forward to that owner's pod IP and controller port
```

This gives the LB a stable address across API-pod/controller failover without
randomly routing to a non-owner pod or rolling the LB Deployment. The proxy does
not retry an ambiguous sync POST.

The chart delta is:

| File | Change | Why |
| --- | --- | --- |
| `charts/skypilot/templates/api-deployment.yaml` | **Modified.** When enabled, injects pod identity, the stable API-Service URL, two mandatory token-ring file paths, and an explicit data-plane-auth capability signal. The data-plane file and projection are emitted only when both optional references are set. | `lb_k8s.py` mirrors the running API image plus the LB-sync projection and, when enabled, the data-plane projection into each LB. The controller-admin projection remains API/controller-only. Projected files make overlap-token rotation live without a pod restart. |
| `charts/skypilot/templates/rbac.yaml` | **Modified.** When the workload namespace differs from the Helm release namespace, adds a least-privilege external-LB Role/Binding beside the API pod and retains it while the feature is disabled. | LB Deployments, Services, projected auth Secrets, and controller image identity all live in the release namespace; workload-namespace RBAC alone is insufficient, and disabling the feature must not remove permission before old objects are reaped. |
| `charts/skypilot/values.yaml` | **Modified.** Adds `serve.externalLoadBalancer.enabled`, per-LB resources, mandatory `lbSync`/`controllerAdmin` references, and the optional `lbDataPlane` pair. | This is the platform-facing values contract. There is no controller port-range configuration. |
| `charts/skypilot/values.schema.json` | **Generated.** Describes the external-LB flag and three auth objects. | Kept in sync with `values.yaml` by the `helm-values-schema-json` plugin; `.github/workflows/helm-values-schema.yaml` gates drift. |
| `charts/skypilot/tests/external_lb_test.yaml` | **New.** Covers disabled-mode inertness, mandatory-ring failures, optional data-plane pair validation, null-safe values, conditional projections, and injected environment variables. | Prevents the chart from silently rendering an incomplete external-LB deployment or accidentally requiring data-plane credentials. |
| `charts/skypilot/tests/external_lb_rbac_test.yaml` | **New.** Covers the split workload/release namespace Role and the same-namespace no-duplicate case. | Prevents valid `kubernetesCredentials.inclusterNamespace` configurations from passing Helm render but failing every LB create. |
| `charts/skypilot/templates/serve-controller-service.yaml` | **Deleted.** | A Service selecting all API pods is not a valid route to a child controller owned by exactly one pod. Do not restore it during an upstream sync. |

## Platform values and Secret contract

The platform must supply the four LB-sync and controller-admin Secret reference
fields when it enables the capability. Data-plane auth is opt-in: supply both
`lbDataPlane` fields to require the SkyServe-specific inference header, or leave
both empty (or omit the subtree) to accept inference requests without it. A
partial data-plane pair is rejected during Helm rendering.

```yaml
serve:
  externalLoadBalancer:
    enabled: true
    auth:
      lbSync:
        existingSecret: skypilot-serve-lb-sync
        key: tokens
      controllerAdmin:
        existingSecret: skypilot-serve-controller-admin
        key: tokens
      # Optional: omit this object or leave both strings empty to disable
      # SkyServe data-plane authentication.
      lbDataPlane:
        existingSecret: skypilot-serve-lb-data-plane
        key: tokens
```

All configured rings must be logically distinct and live in the Helm release
namespace. Prefer separate Secret objects as shown here; a platform that manages
one Secret may instead use different keys because each projected volume exposes
only its configured key. Each key contains a newline-delimited bearer-token ring:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: skypilot-serve-lb-sync
type: Opaque
stringData:
  tokens: |-
    replace-with-current-random-token
    replace-with-previous-random-token
```

Repeat that shape for the controller-admin ring and, when enabled, the data-plane
ring; never reuse a token between rings. The first line is primary (components
that present a token use it) and all lines are accepted, so rotation is: add the
new token first, allow projected volumes to update, switch callers, then remove
the old token. A final newline is allowed, but blank lines, whitespace, non-ASCII
text, and tokens containing characters outside `A-Za-z0-9._~+/=-` are rejected.

Task-level `service.tls_credential` is intentionally unsupported in this
external-only topology. Terminate TLS at the platform ingress/load balancer;
accepting a task certificate would otherwise advertise HTTPS while the LB pod
serves HTTP.

Disabling `serve.externalLoadBalancer.enabled` while services or LB objects
still exist is unsupported. First run `sky serve down --all`, wait for cleanup,
and verify `kubectl get deployment,service -n <release-namespace> -l
skypilot-serve-lb` returns no objects; only then disable the Helm capability.
This ordering is required in particular when
`kubernetesCredentials.useApiServerCluster=false`, because the disabling roll
intentionally removes the API pod's in-cluster service-account token rather
than broadening that security boundary for cleanup.

The rendered trust boundaries are:

| Ring | API volume and file environment variable | Copied into LB pods |
| --- | --- | --- |
| LB sync | `skypilot-serve-lb-sync-auth`; `SKYPILOT_SERVE_LB_SYNC_AUTH_TOKENS_FILE=/etc/skypilot/serve-auth/lb-sync/tokens` | Yes |
| Controller admin | `skypilot-serve-controller-admin-auth`; `SKYPILOT_SERVE_CONTROLLER_ADMIN_AUTH_TOKENS_FILE=/etc/skypilot/serve-auth/controller-admin/tokens` | **No** |
| LB data plane (optional) | When enabled: `skypilot-serve-lb-auth`; `SKYPILOT_SERVE_LB_AUTH_TOKENS_FILE=/etc/skypilot/serve-auth/lb-data-plane/tokens` | When enabled |

The chart also injects
`SKYPILOT_SERVE_EXTERNAL_LB_ENABLED=true`,
`SKYPILOT_SERVE_LB_DATA_PLANE_AUTH_ENABLED=true|false`,
`SKYPILOT_SERVE_API_SERVICE_URL=http://<fullname>-api-service.<namespace>.svc`
and the downward-API `SKYPILOT_POD_NAME` / `SKYPILOT_POD_NAMESPACE` values. Helm
validates that the four mandatory reference strings are non-empty and that the
LB-sync and controller-admin roles do not reference the same Secret key. The API
server also requires the parsed token rings to be content-disjoint at startup
and on every live read. Rotation overlap is supported within each ring, but a
token must never move between both rings in one step. The optional data-plane
references must be either both set or both empty. Kubernetes enforces that
referenced Secrets and keys actually exist when it mounts the API pod. Disabling
data-plane auth does not alter a model server's own
`Authorization` handling; the external LB continues to preserve that header.

Note: `Chart.yaml` `version`/`appVersion` are `0.0.0` / `"0.0"` placeholders in git;
CI (`boltz-chart-publish.yml`) stamps the real immutable version at publish time.
`Chart.lock` and `charts/*.tgz` are gitignored and rebuilt by `helm dependency build`.

## Upstream-sync procedure

Because this is a fork, upstream chart improvements do not arrive automatically —
re-pull them periodically and re-apply the boltz delta on top.

1. **Add/refresh the upstream remote** and fetch:
   ```bash
   git remote add upstream https://github.com/skypilot-org/skypilot.git   # once
   git fetch upstream
   ```

2. **Re-pull upstream's chart tree** onto a sync branch. Overwrite the whole
   `charts/skypilot` directory from upstream so you start from a clean upstream base
   (this transiently drops the boltz delta — you re-apply it in step 3):
   ```bash
   git switch -c chore/chart-upstream-sync
   git checkout upstream/master -- charts/skypilot
   ```

3. **Re-apply the boltz delta** — restore/replay exactly the files listed above:
   - `templates/api-deployment.yaml` — re-insert the
     `{{- if .Values.serve.externalLoadBalancer.enabled }}` identity/env, volume
     mount, and projected-volume blocks, including the explicit data-plane-auth
     capability and conditional data-plane projection. Upstream may have
     restructured this file, so reconcile by hand rather than blindly reverting.
   - `templates/rbac.yaml` — restore the conditional least-privilege LB
     Role/Binding in the Helm release namespace when the workload namespace is
     different.
   - `values.yaml` — re-add the complete `serve.externalLoadBalancer` flag, the
     two mandatory auth rings, and the optional data-plane pair. Do not
     reintroduce controller port-range values.
   - `values.schema.json` — regenerate, do NOT hand-edit (see step 4).
   - `tests/external_lb_test.yaml` — restore the fail-closed contract tests.
   - `tests/external_lb_rbac_test.yaml` — restore split-namespace RBAC tests.
   - Ensure `templates/serve-controller-service.yaml` remains absent.

   Tip: to see the exact boltz delta to replay, diff the chart against the upstream
   base it was forked from:
   ```bash
   git diff upstream/master -- charts/skypilot/templates/api-deployment.yaml \
                               charts/skypilot/values.yaml \
                               charts/skypilot/templates/rbac.yaml \
                               charts/skypilot/tests/external_lb_test.yaml \
                               charts/skypilot/tests/external_lb_rbac_test.yaml
   ```

4. **Regenerate the values schema** with the same plugin CI uses (never hand-edit it):
   ```bash
   helm plugin install https://github.com/losisin/helm-values-schema-json.git   # once
   cd charts/skypilot && helm schema -o values.schema.json
   ```

5. **Validate** the chart, generated schema, focused contract tests, and both
   rendering modes. Use a values file containing the complete example contract
   above for `<external-lb-values.yaml>`:
   ```bash
   helm dependency build charts/skypilot
   helm lint charts/skypilot
   helm unittest charts/skypilot
   helm unittest charts/skypilot -f 'tests/external_lb_test.yaml'
   helm unittest charts/skypilot -f 'tests/external_lb_rbac_test.yaml'
   helm template skypilot charts/skypilot --namespace skypilot \
     -f <external-lb-values.yaml> >/dev/null
   helm template charts/skypilot >/dev/null   # default (flag off) still renders
   cd charts/skypilot
   helm schema -o /tmp/values.schema.json
   cmp values.schema.json /tmp/values.schema.json
   ```

   Also verify the fail-closed guard explicitly; this command must fail on the
   first missing Secret reference:
   ```bash
   helm template charts/skypilot \
     --set serve.externalLoadBalancer.enabled=true
   ```

6. **Watch for upstream restructures.** If upstream renames/moves the api-server
   Deployment template or reworks the values layout, the `api-deployment.yaml` and
   `values.yaml` re-apply in step 3 needs manual reconciliation. Preserve the stable
   API-Service URL, the two mandatory projected rings, the optional data-plane
   projection, and the rule that the LB receives sync credentials plus configured
   data-plane credentials but never controller-admin credentials.

7. Open the sync PR to `improvements`. CI (`helm-values-schema.yaml`, now triggered on
   `improvements`) gates schema drift; `boltz-chart-publish.yml` re-publishes on merge.
