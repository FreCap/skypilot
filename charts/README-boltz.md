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
(default `false`, so the chart is byte-inert for anyone not opting in). It is three
files:

| File | Change | Why |
| --- | --- | --- |
| `charts/skypilot/templates/serve-controller-service.yaml` | **New file.** Renders a `ClusterIP` Service named exactly `skypilot-serve-controller` exposing the controller port range (`controllerPortRangeStart`..`+controllerPortRangeSize`, default `20001`–`20100`) and selecting the api-server pod by its `app: <fullname>-api` label only. | External LB pods spawned by the in-pod controller reach the controller through this Service. The name is hardcoded in `sky/serve/lb_k8s.py` (`CONTROLLER_SERVICE_NAME`), so it must NOT be a templated name. |
| `charts/skypilot/templates/api-deployment.yaml` | **Modified.** Adds a `{{- if .Values.serve.externalLoadBalancer.enabled }}` block injecting downward-API env `SKYPILOT_POD_NAME` (`metadata.name`) and `SKYPILOT_POD_NAMESPACE` (`metadata.namespace`) on the api-server container. | The controller reads these to mirror its own image/identity onto the LB pod it creates (matches `sky/serve/constants.py` `POD_NAME_ENV_VAR`). |
| `charts/skypilot/values.yaml` | **Modified.** Adds the top-level `serve.externalLoadBalancer` block (`enabled`, `controllerPortRangeStart`, `controllerPortRangeSize`). | The values contract these templates read. |

Plus the generated, machine-checked artifact that must move with `values.yaml`:

| File | Change | Why |
| --- | --- | --- |
| `charts/skypilot/values.schema.json` | **Modified.** Adds the `serve.externalLoadBalancer` object (properties `enabled`, `controllerPortRangeStart`, `controllerPortRangeSize`). | Kept in sync with `values.yaml` by the `helm-values-schema-json` plugin; `.github/workflows/helm-values-schema.yaml` fails CI on drift (now also on the `improvements` branch). |

Chart-contract summary (what the platform repo depends on): the single flag
`serve.externalLoadBalancer.enabled` renders the `skypilot-serve-controller` Service
+ the POD_NAME/_NAMESPACE env. The platform side depends only on that flag, not on
any internal detail of these templates.

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
   - `templates/serve-controller-service.yaml` — boltz-only file; upstream will not
     have it, so `git checkout upstream/... -- charts/skypilot` deletes it. Restore it:
     ```bash
     git checkout HEAD@{1} -- charts/skypilot/templates/serve-controller-service.yaml
     ```
     (or cherry-pick it back from the pre-sync commit / the `[Chart] Portable
     external-LB support` commit).
   - `templates/api-deployment.yaml` — re-insert the
     `{{- if .Values.serve.externalLoadBalancer.enabled }}` env block. Upstream may
     have restructured this file, so reconcile by hand rather than blindly reverting;
     keep the boltz block near the other downward-API env vars.
   - `values.yaml` — re-add the top-level `serve.externalLoadBalancer` block.
   - `values.schema.json` — regenerate, do NOT hand-edit (see step 4).

   Tip: to see the exact boltz delta to replay, diff the chart against the upstream
   base it was forked from:
   ```bash
   git diff upstream/master -- charts/skypilot/templates/api-deployment.yaml \
                                charts/skypilot/values.yaml
   ```

4. **Regenerate the values schema** with the same plugin CI uses (never hand-edit it):
   ```bash
   helm plugin install https://github.com/losisin/helm-values-schema-json.git   # once
   cd charts/skypilot && helm schema -o values.schema.json
   ```

5. **Validate** the chart still builds and renders both ways:
   ```bash
   helm dependency build charts/skypilot
   helm lint charts/skypilot
   helm template charts/skypilot --set serve.externalLoadBalancer.enabled=true >/dev/null
   helm template charts/skypilot >/dev/null   # default (flag off) still renders
   ```

6. **Watch for upstream restructures.** If upstream renames/moves the api-server
   Deployment template or reworks the values layout, the `api-deployment.yaml` and
   `values.yaml` re-apply in step 3 needs manual reconciliation — the boltz delta is
   small, so re-derive it from the table above rather than forcing a merge.

7. Open the sync PR to `improvements`. CI (`helm-values-schema.yaml`, now triggered on
   `improvements`) gates schema drift; `boltz-chart-publish.yml` re-publishes on merge.
