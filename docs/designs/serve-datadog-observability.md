# Datadog observability for SkyServe controllers and load balancers

- **Status:** M0 and M1 DEPLOYED and verified on 2026-07-25. M2 to M4 (the
  SkyPilot-side metrics emission) not started.
- **Last updated:** 2026-07-25

The rest of this document was written before M1 shipped and still reads as a
proposal. It is retained as the reasoning that produced the design; the
deployment record and the departures from it are below.

## Deployment record (2026-07-25)

Shipped by boltz-platform PR #7330 (ArgoCD Application `hub-datadog`, namespace
`observability-datadog`, chart `deployment/helm-addons/datadog-wrapper`).

Verified live, not assumed:

- All 3 hub nodes report to Datadog and are `up`.
- Agent reports `API key valid`; the logs agent had shipped 201,812 events with
  0 retries within minutes of rollout.
- 47 SkyPilot log sources are tailed, covering every external load balancer pod
  and the API server container. Sample LB lines are queryable in Datadog under
  `kube_cluster_name:boltz-platform-gitops-hub-rainier-eks-cluster`.
- DogStatsD is listening on host port 8125/udp, ready for M2.
- **The controller-log gap in this document is confirmed, not theoretical.**
  Searching Datadog for `Reserved-fill broker` and `Concurrency report` returns
  0 hits, because controllers write to `~/.sky/serve/<svc>/controller.log` on
  the state-volume PVC and never to stdout. M2 remains the fix.

### Departures from this design, and why

1. **Secret name.** This document specifies a new
   `skypilot/gitops-hub-rainier/datadog/credentials`. What shipped reuses the
   platform-wide convention `global/datadog/provider-tf-credentials`, seeded
   into account 255203429798, because the `datadog-wrapper` chart already
   defaults `externalSecret.awsSecretName` to it and every other account uses
   that name. The design's substantive point was kept: only `api_key` is
   replicated, not `app_key`.
2. **IAM location.** The grant went into
   `deployment/terragrunt/modules/aws-gitops-hub/irsa_argocd.tf` (the managed
   policy for the hub's ESO role) rather than the skypilot control-plane
   `eso.tf`, because the consumer is the cluster-wide agent, not the SkyPilot
   release.
3. **Chart source.** The Application uses the git chart path, not the ECR
   chart. `infra-prod` and `infra-test` pin `datadog-wrapper` 3.653.0 while the
   registry's newest tag is 3.650.6, so that pin cannot render and both apps
   read OutOfSync. Using it would have shipped broken.

Everything else landed as designed, including the two structural calls this
document argues hardest for: the agent holds the only Datadog credential, and
the load balancer PodSpec was not touched.

Primary approach: **install the Datadog node agent on ghub-skypilot for logs and
infrastructure signals, then emit SkyServe metrics from the controller process
over DogStatsD to that node-local agent. Never touch the load balancer PodSpec.**
This is staged: the agent lands first with zero SkyPilot code, metrics follow in a
routine image bump.

---

## What exists today

**ghub-skypilot has no Datadog. It has no observability stack of any kind.**

The SkyPilot control plane runs on the EKS cluster reached by kube context
`ghub-skypilot` (gitops-hub-rainier, account 255203429798). Verified live:

- No `datadog` namespace, no agent pods, no Prometheus, no Grafana, no OTel
  collector, no log shipper. The only DaemonSets on the whole cluster are
  `aws-node`, `ebs-csi-node`, `ebs-csi-node-windows` (0 desired),
  `eks-pod-identity-agent`, and `kube-proxy`.
- Namespaces are only: `argocd`, `default`, `external-dns`, `external-secrets`,
  `golink`, `kube-node-lease`, `kube-public`, `kube-system`, `skypilot`,
  `skypilot-independent-ci-fcapponi`, `skypilot-system` (empty).

By contrast, context `boltz-prod` has a full install: DaemonSet
`infra-prod-datadog` in namespace `observability-datadog`, agent image
`gcr.io/datadoghq/agent:7.70.2`, 4 containers, `hostNetwork: true`, `hostPID: true`,
with APM, logs, DogStatsD and OTLP receivers all enabled, plus an
`infra-prod-datadog-cluster-agent` Deployment.

**What that asymmetry implies.** Every mental model imported from boltz-prod is
wrong here. On boltz-prod, a pod that writes to stdout is observable for free and
a pod can talk DogStatsD to `status.hostIP` for free. On ghub-skypilot neither is
true today. So any design must state plainly whether it installs an agent, and own
that cost. This one does install an agent, and section
[Recommended approach](#recommended-approach) owns the cost.

### The topology that actually matters

- The SkyPilot API server is a single pod, `skypilot-api-server-5fdb4777d7-lggrv`,
  in namespace `skypilot`. Deployment strategy `Recreate`, `replicas: 1`,
  `terminationGracePeriodSeconds: 60`.
- **Every SkyServe controller is a process inside that pod** (consolidation mode),
  not a separate pod. 11 confirmed live: `opendde-10c200s-v4-mt-hybrid`,
  `onecut-spot-20260725-0014-r2-pool`, `opendde-builder-source-20260724-r1`,
  `b25fi-v4`, `opendde-builder-built-20260724-r1`, `opendde-10c200s-v4`,
  `protenixv2-hybrid-v1`, `boltz-l4-fleet-test`,
  `foldeverything-foldbench-rnp-8ijjix39-5x5-v1`, `b25fi-gostar-v1`,
  `boltz-l4-fleet`. Each is `python -u -m sky.serve.service --service-name ...`.
- **Controller logs do not go to stdout.** They are per-service files at
  `~/.sky/serve/<scoped>/controller.log`
  (`sky/serve/constants.py:5` `SKYSERVE_METADATA_DIR = '~/.sky/serve'`) on the
  `state-volume` PVC mounted at `/root/.sky`. This is the single most
  misunderstood fact in this topology: **container log collection does not
  observe the controllers.** It observes the LB pods only.
- The external load balancers are 17 separate Deployments named
  `skypilot-lb-<service>-<hash>` in the same namespace, fronted by 10 AWS ELB
  Services on port 30001. 7 services are HA (two Deployments sharing one
  Service), 3 are not.
- LB pods are not chart objects. Their PodSpec is built programmatically in
  `sky/serve/lb_k8s.py`, by the single builder `_build_deployment_dict`
  (`sky/serve/lb_k8s.py:1013-1181`), whose sole call site is
  `sky/serve/lb_k8s.py:1977-1982`.
- The LB runs the **same image** as the controller: `_resolve_lb_image`
  (`sky/serve/lb_k8s.py:495-541`) reads the controller container's runtime
  imageID. The pod template also pins annotation
  `skypilot.co/controller-image-digest` (`sky/serve/lb_k8s.py:49-51`, written at
  `:1119-1121`). **Consequence: every control-plane image change rolls all 17 LB
  Deployments.** Live `terminationGracePeriodSeconds` on an LB is 7245s (~2h), so
  a fleet roll is slow.
- Each LB roll changes the pod UID, which `sky/serve/lb_k8s.py:1082-1084`
  documents as "the durable session identity used to make rollout-overlap drain
  proofs fail closed". Perturbing LB pods perturbs drain proof.

### Release and delivery state

- Live release: `skypilot`, revision 286, chart `skypilot-1.1.811`, appVersion
  1.1.811, deployed 2026-07-25 13:42 BST. Eight revisions in ~13 hours that day.
- **Terragrunt is behind.** `deployment/terragrunt/environments/gitops-hub-rainier/skypilot-control-plane/skypilot-pin.json`
  reads `{"version": "1.1.805", "commit": "3bea3a14e8b0a67969d6ca499d7c44cbe93d98de"}`.
  A naive `terragrunt apply` today reconciles `helm_release.skypilot` back to chart
  and image 1.1.805, a **six-release downgrade of the control plane**. Any change
  routed through terragrunt must bump this pin in the same commit.
- Eight unrelated Helm releases share namespace `skypilot`
  (`skypilot-image-canary-diagnostic`, `-scan`, `-tags`,
  `skypilot-image-profile-association`, `-diagnostic`, `-observation`,
  `skypilot-image-qualification-prototype`, `skypilot-image-spot-config-seed`), so
  new object names in that namespace must not collide.
- ArgoCD `infrastructure` AppProject destinations on this cluster are
  `kube-system`, `external-secrets`, `karpenter`, `keda`, **`observability-datadog`**,
  `external-dns`, `mmseqs-app`, `golink`, each with `server: "*"`. The `root`
  Application runs `automated: {prune: true, selfHeal: true}`.
  **`observability-datadog` is already an allowed destination on this cluster.**
  That is a meaningful, non-obvious advantage for the agent path: it needs no
  AppProject edit, whereas a new `skypilot-telemetry` namespace would.
- The `infra-addons` ApplicationSet generator selects clusters by label
  `cluster-type: internal`; only the `prod` and `test` cluster Secrets carry it.
  The hub's own destination `https://kubernetes.default.svc` has no cluster
  Secret and is not covered, so the agent needs a **new, dedicated Application**,
  not a values flip. Do not label the hub `cluster-type: internal` as a shortcut:
  that would also pull `karpenter-provisioners` and `cluster-secret-stores` onto
  the hub, and the ApplicationSet's RollingSync steps are hardcoded
  test-then-production with no hub slot.

### Capacity and network facts that constrain the design

- 3 nodes, all `m6i.8xlarge` (32 vCPU, ~124Gi).
- Node headroom is **not** tight. Live `kubectl describe node` shows the three
  nodes at 4%, **52%**, and 7% CPU requests. The api-server's node is the 52% one:
  16680m of 32000m requested, so roughly **15 vCPU free**, not the ~4 vCPU an
  earlier draft claimed. The Datadog agent requests 200m/256Mi. This is a
  non-issue; do not redesign around it.
- **hostPorts are free.** The only hostPorts in use across all pods cluster-wide
  are 80, 2703, 8162, 61678 (`eks-pod-identity-agent`, `aws-eks-nodeagent`,
  `aws-node`). Ports 8125/udp, 8126, 4317, 4318, 5555, 6000 are all unused, so
  the agent can run `useHostNetwork: true` exactly as boltz-prod does.
- Egress to Datadog works from both workloads. `curl` from the api-server pod to
  `api.datadoghq.com`, `http-intake.logs.datadoghq.com` and
  `trace.agent.datadoghq.com` returns **403** (TLS completed, rejected for a
  missing key). Same 403 from an LB pod for the first two. No proxy, no egress
  firewall. Agentless submission is therefore network-feasible; it is rejected on
  other grounds (see [Rejected alternatives](#rejected-alternatives)).

### The secret blocker

`global/datadog/provider-tf-credentials` **does not exist** in account
255203429798 (`DescribeSecret` returns `ResourceNotFoundException`). It exists in
421498156696 (prod) and 911167932214 (management). The prod copy has **no resource
policy**, so cross-account read is impossible today.

The hub ESO role
`arn:aws:iam::255203429798:role/boltz-platform-gitops-hub-rainier-external-secrets`
grants `GetSecretValue`/`DescribeSecret` on same-account ARNs only. Both
`ClusterSecretStore`s on ghub (`aws-secrets-manager` and
`aws-secrets-manager-global`) have byte-identical specs:
`provider.aws {region: us-east-1, service: SecretsManager}` with **no** `auth`
block and **no** `roleARN`. The name "global" is nominal; both resolve through the
ESO controller's own identity in 255203429798. All 8 ExternalSecrets on the
cluster use the regional store; the "global" store has never been exercised here.

This blocks every candidate design equally and is the gating work item.

### What is already instrumented in the fork

Nothing emits to Datadog or OTLP. `git grep -ilE 'datadog|ddtrace|dogstatsd|statsd'`
finds no hits under `sky/serve/`, `sky/server/`, `charts/`, or `Dockerfile`;
`import ddtrace` and `import datadog` both raise `ImportError` inside a live LB
pod. `sky/setup_files/dependencies.py:92` carries `prometheus_client>=0.8.0` and no
Datadog package.

`apiService.metrics.enabled` is **false** in production (the live pod template has
no `prometheus.io/*` annotations and no 9090 port; `skypilot-api-service` exposes
80 only). Every metric family in `sky/metrics/utils.py:52-267` is api-server or
managed-jobs. There is not one serve, LB, autoscaler, or reserved-fill metric in
the tree.

### Corrections to prior briefings

Four claims circulating in earlier drafts are false against this worktree
(HEAD `4f33fb9a46`). They are corrected here because designs were built on them:

1. **`drain_proof_stats_snapshot` is NOT dark.** It is defined at
   `sky/serve/replica_managers.py:2312` and **is** wired into `/autoscaler/info`
   at `sky/serve/controller.py:4032`, inside a `try/except` whose comment reads
   "Diagnostics must never take down the endpoint the supervisor and the
   dashboard both read". Milestone 0 of the drain-proof design is implemented and
   readable. The real gap is that nothing polls that endpoint on a timer.
2. **`ha_observability` and the occupancy aggregates are NOT in the LB sync
   payload.** They are in the `_capacity` response
   (`sky/serve/load_balancer.py:2836-2845`), which backs the `/_lb/capacity`
   **pull** endpoint. The sync payload is built at
   `sky/serve/load_balancer.py:3323-3356` and carries `queue_depth`,
   `queue_depth_by_priority`, `rejected_in_window`, `rejected_in_recent_window`,
   both `_by_priority` variants, `in_flight`, `routing_urls`,
   `occupancy_sampled_urls`, `total_slots_by_url`, `draining_urls` and
   `lb_session_id` -- and none of `ha_observability`, `probed_replicas`,
   `busy_replicas`, `total_slots`, `running_slots`, `free_slots`, or
   `occupancy_probe_age_seconds`. Any plan that says "the controller already
   receives everything" is wrong.
3. **`_health` is not a telemetry payload.** It is at
   `sky/serve/load_balancer.py:1915-1921` and returns a **status code with an
   empty body**; it is the Kubernetes readiness handler.
4. **`demonstrated_need` is in `reserved_capacity_broker.py`, not
   `reserved_capacity_allocation.py`.** Real sites: read from the activity row at
   `sky/serve/reserved_capacity_broker.py:313-314`, the `ActivityInput` field at
   `:356`, the blind sentinel at `:363`, `_activity_input` at `:366-386`, and
   consumption at `:422`. It is defined at `sky/serve/autoscalers.py:115` and
   written by `sky/serve/reserved_capacity.py:488`.

---

## What we want to see

Grouped by the four problems this team actually debugs. "Cheap" means the value is
already computed and reachable at an existing call site, so the work is an emit
line. Tags are bounded by construction: `service` (11 values), `pool`,
`accelerator`, `lb_slot` (2), `reason` (closed 4-value set), `outcome`. **Never**
tag by replica URL, request id, job id, or pod name.

### 1. Reserved-fill broker arbitration -- CHEAP

The just-shipped utilization gate has a rollout gate that is literally "watch
`demonstrated_need` for a week", and today that is unmeasurable by any means.
Every value below is a local variable at
`sky/serve/reserved_capacity_broker.py:1242-1285`, already persisted by
`publish_reserved_fill_round` and already printed by the round log at `:1267-1271`.

| Metric | Source |
|---|---|
| `sky.serve.reserved_fill.grant`, `.raw_grant`, `.feed` | `grants`, `raw_grants`, `feeds` at `:1246-1250` |
| `sky.serve.reserved_fill.sum_holdings` | `published_sum_holdings` at `:1252` |
| `sky.serve.reserved_fill.last_observed_free`, `.last_observed_free_age` | `last_free`, `last_free_ts` at `:1253-1254` |
| `sky.serve.reserved_fill.phantom_streak`, `.shrink_baseline` | `:1255-1256` |
| `sky.serve.reserved_fill.round_id`, `.epoch` | `:1244`, `:1247`. A stalled `round_id` is itself the alarm |
| `sky.serve.reserved_fill.demand_gate_grant` | `Allocation` at `:1278-1285` |
| **`sky.serve.reserved_fill.demonstrated_need`** | `reserved_capacity_broker.py:313-314` / `:366-386`. **The rollout gate.** |
| `sky.serve.reserved_fill.utilization_gate_active` | 0/1, whether `utilization_state` was truthy this round |
| `sky.serve.reserved_fill.lease_superseded` (count) | the `if not published` branch at `:1261-1267` |
| `sky.serve.reserved_fill.blackout_round` (count) | the `service_name not in grants` branch at `:1273-1277` |

One trap: `utilization_state` is persisted only `if utilization_state else None`
(`:1258-1259`), so on rounds where the gate is inactive `demonstrated_need` is
**not** in the database row. Emit it directly from the broker, not by reading back
the persisted round. This is the single highest-value line in the whole design.

### 2. Drain proof -- CHEAP

Counters exist in `sky/serve/drain_observability.py` (closed reason set at
`:31-40`, snapshot at `:89-101`), are recorded at
`sky/serve/replica_managers.py:5837, 6037, 6058, 6060`, and are already readable
via `drain_proof_stats_snapshot` (`replica_managers.py:2312`) at
`controller.py:4032`. Only a timed export is missing.

- `sky.serve.drain.deadline_expiry_without_proof` and `sky.serve.drain.proved_drained`.
  The ratio is the headline number: there is currently no production evidence the
  7200s drain cap has ever been paid.
- `sky.serve.drain.logical_aborts{reason:target_coverage|fence_changed|idle_proof_timeout|other}`
  and `.logical_aborts_total`.
- `sky.serve.drain.bounded_completions`.
- `sky.serve.drain.blind_capacity_rounds`, `.blind_capacity_skipped_replicas`.

These are process-local monotonic ints that reset when a controller restarts
(`drain_observability.py:20-24`). Emit them as **DogStatsD counters carrying the
delta since the previous tick**, not as gauges: see
[Restart-safe counter semantics](#restart-safe-counter-semantics).

### 3. LB health -- MIXED

Already in the sync payload at `load_balancer.py:3323-3356`, so **cheap**:

- `sky.serve.lb.queue_depth`, `.queue_depth_by_priority{priority}`
- `sky.serve.lb.rejected_in_window`, `.rejected_in_recent_window`, both `_by_priority`
- `sky.serve.lb.in_flight`, `.routing_urls_count`, `.unknown_in_flight_urls_count`, `.draining_urls_count`
- `sky.serve.lb.session_changed` (count), by diffing `lb_session_id` per
  (service, slot). This is problem area 3's headline: "the LB session id changing
  is what breaks drain proof."

**NOT in the sync payload, needs a payload extension (M3):** `ha_observability`
(`lb_ha_observability.py:165-320`: role transitions `:193-221`, probe outcomes
`:268-286`, percentile histograms `:66-123`) and the occupancy aggregates
`probed_replicas`, `busy_replicas`, `total_slots`, `running_slots`, `free_slots`,
`occupancy_probe_age_seconds`. These are computed inside the async `_capacity`
handler at `load_balancer.py:2524`, specifically `:2604-2657`, so exposing them
requires extracting the computation into a reusable helper. That is a real but
bounded refactor, not a one-line dict addition.

**Genuinely new instrumentation:** a monotonic rejection counter. What exists today
is `_reject_last_seen: dict[str, tuple[float,int]]` (`load_balancer.py:192`),
a dedup/compatibility **window** keyed by job id and pruned by
`_prune_reject_window` (`:2037-2060`). `rejected_in_window` is therefore a gauge of
unique jobs currently inside the window, **not** a count of rejections. Dashboarding
it as a rejection rate produces numbers that are wrong in a plausible-looking way.

### 4. Autoscaler -- CHEAP

From `sky/serve/autoscalers.py:1377-1418` (subclass override adds `replica_unit`,
`adaptive_demand_estimation`, `in_flight_total`), already assembled for the
minute-bucketed Postgres write at `sky/serve/controller.py:2358-2375`:

- `sky.serve.autoscaler.target_num_replicas`, `.min_replicas`, `.max_replicas`
- per-accelerator: `.target_by_accelerator`, `.demand_target_by_accelerator`,
  `.warm_retention_target_by_accelerator`, `.cold_launch_authority_by_accelerator`,
  `.min_by_accelerator`, `.ready_by_accelerator`, `.provisioning_by_accelerator`
- `.ready_capacity`, `.provisioning_capacity`, `.total_capacity`,
  `.peak_in_flight`, `.peak_queue_depth` (validated at `controller.py:2350-2370`)
- `.fill_free_slots`, `.fill_snapshot_age`, `.fill_target` (`autoscalers.py:1408-1417`)
- `.requests_per_second`, `.recent_request_count` (`autoscalers.py:1402-1407`)

### 5. Self-telemetry and logs

- `sky.serve.dogstatsd.dropped` (count), `sky.serve.controller.heartbeat` (gauge
  per service per tick). A Datadog no-data monitor on the heartbeat is what
  detects a dead api-server pod.
- **Logs come from the agent, not from code.** With
  `DD_LOGS_CONFIG_CONTAINER_COLLECT_ALL=true`, all 17 LB pods' stdout is collected
  with zero SkyPilot changes, along with Kubernetes events, container restarts,
  and OOMKills. Controller logs are files on a PVC and are **not** collected; see
  [Open questions](#open-questions) for whether that gap is worth closing and why
  the obvious fix (a tailer sidecar) is dangerous.

Estimated volume: roughly 45 series x 11 services + about 20 LB series x 17 pods,
so on the order of 800-900 active custom timeseries. No histograms in v1, so no
`datadog_metric_tag_configuration` entries are needed yet (each one doubles custom
metric count for that metric).

---

## Recommended approach

**Install the Datadog node agent on ghub-skypilot. Emit SkyServe metrics from the
controller processes over DogStatsD to that agent. Never modify the load balancer
PodSpec.**

Three properties make this the right shape, and each is a deliberate choice:

1. **No SkyPilot workload ever holds the Datadog API key.** DogStatsD over UDP to a
   node-local agent is unauthenticated by design: the trust boundary is the node,
   not a credential. Only the agent holds the key. This is not a mitigation bolted
   on afterwards, it is the reason the agent path beats direct submission.
2. **The LB PodSpec is never touched, so this work adds zero LB rolls.** Earlier
   drafts wanted `DD_AGENT_HOST` env or a metrics containerPort on the LB. Both
   roll all 17 Deployments and change the pod UID that *is* the drain-proof session
   identity, which means instrumenting drain proof would perturb drain proof. That
   is avoidable: the agent collects LB **logs, restarts and OOMKills** at the node
   level, and the controller relays LB **metrics** over the sync channel. Together
   those cover problem area 3 without a single byte of LB PodSpec change.
3. **`observability-datadog` is already an allowed ArgoCD destination on this
   cluster**, so the agent needs no AppProject edit. A new telemetry namespace
   would.

### Milestone shape

Value lands before risk. M1 delivers observability with **no SkyPilot code at
all**, and is the go/no-go gate for everything after it.

| Stage | What | SkyPilot code? | Restarts api-server? | Rolls LBs? |
|---|---|---|---|---|
| M0 | Secret + IAM (Terraform) | no | no | no |
| M1 | Datadog agent DaemonSet | no | no | no |
| M2 | Controller DogStatsD metrics | yes | **yes, once** | yes (image bump, unavoidable) |
| M3 | LB sync payload extension | yes (LB, not PodSpec) | yes | yes (image bump) |
| M4 | Rejection counter, dashboards | yes | yes | yes (image bump) |

### Chart changes

**None.** This is worth stating explicitly so nobody opens a chart PR.

- `apiService.extraEnvs` is passed through verbatim: `charts/skypilot/templates/api-deployment.yaml:232-234`
  does `{{- toYaml . | nindent 8 }}`. The only validation is
  `skypilot.validatePodExtras` (`charts/skypilot/templates/_helpers.tpl:40-60`),
  which checks for a non-empty `.name` and rejects duplicates and reserved names,
  and never inspects `value` versus `valueFrom`. No `DD_*` name is reserved
  (`api-deployment.yaml:28-48`).
- The chart already renders `valueFrom` shapes in that same list:
  `SKYPILOT_INITIAL_BASIC_AUTH` (`:244-248`) and `SKYPILOT_DB_CONNECTION_URI`
  (`:270-282`), and production already carries a user-supplied one
  (`SKYPILOT_HOSTED_CATALOG_TOKEN` from secret `skypilot-catalog-token`).
- Use `apiService.extraEnvs`, **not** `global.extraEnvs` (`values.yaml:11`), which
  fans out to roughly 8 other containers.
- **Do not enable `apiService.metrics.enabled`.** See
  [Rejected alternatives](#rejected-alternatives); it is not free and it buys
  nothing here.

### Infrastructure changes (boltz-platform)

1. **AWS Secrets Manager, account 255203429798, us-east-1.** New secret
   `skypilot/gitops-hub-rainier/datadog/credentials` containing `{"api_key": "..."}`
   and nothing else. The `app_key` is a read-API credential for the compute-api
   monitor (`deployment/helm/templates/externalsecret.yaml:64-65`) and **nothing in
   this design reads from Datadog**, so it is not replicated. The api_key value
   must be supplied by an operator out of band; see [Secret access](#secret-access).
2. **IAM.** Add the new key to `local.eso_read_secret_keys` at
   `deployment/terragrunt/modules/skypilot/control_plane/skypilot_control_plane/eso.tf:12-18`.
   The existing inline policy `${var.release_name}-eso-read-secrets` at `:24-38`
   renders `Resource` from that list as
   `arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:${k}-*`,
   so this is a policy-content update, not a new IAM object. The file says so at
   `:9-11` and `:20-23`.
3. **New ArgoCD Application `datadog-hub`**, targeting
   `https://kubernetes.default.svc`, namespace `observability-datadog`, sourcing
   chart `deployment/helm-addons/datadog-wrapper` with a new `values-hub.yaml`.
   Structure it like the existing in-cluster hub addon
   (`deployment/argocd/applicationsets/golink.yaml`), and **copy the
   `ignoreDifferences` block from `deployment/argocd/applicationsets/infra-addons.yaml:89-122`
   verbatim** (kpi-telemetry configmap `install_id`/`install_time`, the
   cluster-agent Secret token, and the `checksum/clusteragent_token` annotation on
   the DaemonSet and cluster-agent Deployment). Without it, ArgoCD selfHeal rotates
   the cluster-agent token every reconcile and rolling-restarts the agent forever.
   Start with `automated.selfHeal: false` and sync manually for the first week.
4. **`values-hub.yaml`** for the wrapper chart:
   - `useHostNetwork: true` (verified safe: hostPorts 8125/8126/4317/4318/5555/6000
     are all free on all 3 nodes). **Do not carry a
     `hostNetwork: false` fallback:** on boltz-prod, DogStatsD 8125/udp is declared
     as a containerPort with **no** hostPort and is reachable only because
     `hostNetwork` is true. With `hostNetwork: false` there is no host binding for
     8125 at all, so `DD_AGENT_HOST=status.hostIP` would resolve to nothing and,
     because the emitter swallows `ECONNREFUSED` by contract, **every metric would
     be silently dropped while the install still looked healthy.**
   - `logs.enabled: true`, `containerCollectAll: true`.
   - DogStatsD: `nonLocalTraffic: true`, port 8125.
   - Host tags `["deployment_name:skypilot-control-plane", "environment:gitops-hub", "env:gitops-hub"]`
     so SkyServe series stay separable from the platform's.
   - `kubernetes_pod_labels_as_tags` mapping `skypilot-serve-lb` -> `service` and
     `skypilot-serve-lb-slot` -> `lb_slot`. Those labels already exist on every LB
     pod (`sky/serve/lb_k8s.py:60`, `:62`, applied at `:479` and `:965`), so LB
     container logs self-identify by service **with no PodSpec change**.
   - Agent image tag 7.70.2 and chart pin 3.88.0, matching boltz-prod. Explicit
     agent resources: requests 200m/256Mi, limits 400m/512Mi.
5. **`api_server_extra_envs` cannot carry these entries as typed.** The variable is
   `type = list(object({ name = string, value = string }))`
   (`.../skypilot_control_plane/variables.tf:124-141`), so a `valueFrom.fieldRef`
   entry fails Terraform type checking. Add a module-owned
   `local.datadog_envs` gated on the new variable and append it to
   `local.all_extra_envs` (`.../skypilot_control_plane/skypilot.tf:240-244`) as a
   fourth `jsondecode(jsonencode(...))` element, exactly as `local.gcp_envs`
   (`:142`) and `local.catalog_envs` (`:166`) already do. That wrapper exists
   precisely because `concat` otherwise demands one common element type
   (comment at `:233`). Duplicate-name validation at `:246` still applies.
   The entries are:
   - `DD_AGENT_HOST`, `valueFrom.fieldRef.fieldPath: status.hostIP`
   - `DD_DOGSTATSD_PORT`, value `"8125"`
   - `DD_ENV`, value `production`
   - `DD_SERVICE`, value `skypilot-serve-controller`
   - `SKYPILOT_SERVE_DOGSTATSD_ENABLED`, value `"true"` (kill switch)

   All five are non-secret. An env dump shows a private RFC1918 address and a port.

### Code changes (SkyPilot fork)

#### New: `sky/serve/dogstatsd.py` (~140 lines, stdlib only)

No new dependency. Deliberately **not** adding `datadog` or `ddtrace` to
`sky/setup_files/dependencies.py`: `ddtrace` monkeypatches at import time, which is
unacceptable in a process that hosts 11 controllers and the API server, and the LB
shares that image.

Contract:

- One `socket.socket(AF_INET, SOCK_DGRAM)`, `setblocking(False)`, `connect((host, port))`
  at init. `DD_AGENT_HOST` is an IP literal from `status.hostIP`, so **no DNS
  resolution ever happens on an emit path**.
- Every send wrapped in `except OSError: self._drops += 1; return`. This swallows
  `ECONNREFUSED` (agent down), `EAGAIN`/`ENOBUFS` (kernel buffer full), `EPERM`.
- If `DD_AGENT_HOST` is unset or `SKYPILOT_SERVE_DOGSTATSD_ENABLED` is not `"true"`,
  the module is a no-op object. Absence of config is the default state, not an error.
- Datagrams capped at 8 KiB, batched per emit call, never buffered across calls.
- One INFO line at startup saying enabled or disabled. It never prints a key,
  because no key exists in this process.
- A `/tmp/skypilot-datadog-disabled` sentinel checked per tick disables emission
  fleet-wide without a pod restart:
  `kubectl exec ... -c skypilot-api -- touch /tmp/skypilot-datadog-disabled`.

#### New: `sky/serve/serve_telemetry.py`

Pure snapshot-to-series mappers for the four families plus the shared guard:

```python
def emit(fn, *a, **kw) -> None:
    try:
        fn(*a, **kw)
    except Exception:  # pylint: disable=broad-except
        logger.debug('serve telemetry emit failed', exc_info=True)
```

Every emission goes through `emit(...)`. Pure functions so the mappers are
unit-testable without a socket.

#### Restart-safe counter semantics

Drain-proof and broker counters are process-local monotonic ints that reset to 0
when a controller restarts, and this pod restarts often (8 helm revisions in 13
hours on 2026-07-25). Emitting them as gauges produces cliffs; emitting the raw
value as a DogStatsD counter double-counts.

Rule: the telemetry module keeps a `last_value` per (metric, tagset) and emits
`max(0, current - last)` as a `|c` counter, then sets `last = current`. On a
controller restart the new process starts with `last = 0` and `current = 0`, so
deltas stay correct from that point forward. No negative rates, no double counts,
and Datadog `sum`/`rate` queries work naturally.

#### Emit call sites

All are wrapped in `emit(...)` and placed so that a raise cannot alter control
flow. Note the ordering constraint at the broker in particular.

1. **`sky/serve/controller.py:2358-2375`**, the existing minute-bucket write.
   `autoscaler_info`, `demand_target`, `fill_target`, `capacity_target`,
   `peak_in_flight`, `peak_queue_depth` and `accelerator_breakdown` are all already
   computed here for `serve_history.record_autoscaler_snapshot`. Emit the same
   values. Covers problem area 4.
2. **Same tick**, emit `self._replica_manager.drain_proof_stats_snapshot()`
   (`replica_managers.py:2312`) as deltas. Covers problem area 2 on a fixed
   cadence instead of only when someone GETs `/autoscaler/info`. Also emit the
   per-service heartbeat here. The autoscaler decision interval is 20s
   (`sky/serve/constants.py:694`).
3. **`sky/serve/reserved_capacity_broker.py`, immediately after the round log at
   `:1267-1271`.** Placement matters: it must be **after** `publish_reserved_fill_round`
   and its `if not published: return None` guard at `:1260-1266`, and **before**
   the blackout `return None` at `:1273-1277`, so every round emits exactly once
   including blackout rounds. Never between the CAS and the return. Emit
   `demonstrated_need` from `_activity_input` (`:366-386`), not from the persisted
   row. Covers problem area 1.
4. **`sky/serve/controller.py:1190-1215`**, inside `_handle_load_balancer_sync`,
   after the `authority[0]` gate at `:1194-1198`. Emit LB series from
   `request_data`. Tag by `lb_slot` (added in M3) so the two LBs of an HA service
   stay separate, and diff `lb_session_id` per (service, slot) to emit
   `sky.serve.lb.session_changed`. Covers problem area 3.

   **The HA double-report trap:** 7 of 10 services run two LB Deployments and
   **both** call `_sync_with_controller` (the loop returns only when
   `self._draining`), so both POST every 20s with different, stable
   `lb_session_id`s. A single last-seen session id per service would flip on every
   alternate sync and emit a spurious `session_changed` event roughly every 20
   seconds, forever, for 7 services -- the exact signal designated as the
   drain-proof invalidation alarm. Keying by (service, slot) fixes it. The
   controller already resolves the reporter's slot from the Kubernetes pod list
   (`_lb_report_authority` at `controller.py:797` uses
   `pod_authority.slot_by_uid`, `lb_k8s.py:141`), but that function is
   security-critical and returns `(live, demand, drain)`; **do not widen it**. Add
   `lb_slot` to the sync payload instead (M3), which is strictly lower risk.

#### M3: LB sync payload extension

Two changes in `sky/serve/load_balancer.py`, both inside the existing
`sync_payload` dict at `:3323-3356`. **No PodSpec change, no `lb_k8s.py` change.**

- Add `'lb_slot': self._lb_slot.value if self._lb_slot is not None else None`.
  `self._lb_slot` is set at `:299` and this exact expression already appears at
  `:2748` and `:3801-3802`.
- Add `'ha_observability': self._ha_stats().snapshot()` and the occupancy
  aggregates. `_ha_stats()` is lazily constructed (`:460`), so this is safe on
  non-HA LBs. The occupancy aggregates are computed inside the async `_capacity`
  handler at `:2604-2657`; extract that into a small synchronous helper that both
  `_capacity` and the sync payload call.

**Critical placement warning.** The payload dict at `:3323-3356` is built
**outside and before** the `try:` that follows, and the inner `except` catches only
`(aiohttp.ClientError, asyncio.TimeoutError)`. `request_batch = self._request_aggregator.drain()`
happens just above at `:3317`, and the `finally: if not request_batch_accepted: self._request_aggregator.restore(request_batch)`
is attached to that same `try`. **A raise while building this dict skips the
restore entirely and silently discards the drained request batch.** The outer
`_sync_with_controller` loop catches broad `Exception`, logs, and retries forever,
so the LB would never crash and never alert while the controller stopped receiving
routing specs and demand data. Every attribute referenced here must be verified to
exist. (An earlier draft proposed `self._process_start_time`, which **does not
exist anywhere in `sky/serve/`** and would have triggered exactly this failure.)

#### M4: rejection counter

`_record_rejection` (`sky/serve/load_balancer.py:2070`) has **four** call sites,
not three: `:1668`, `:1727`, `:1832`, and **`:4364` inside `_unavailable()`**
(`:4358-4372`), which is the terminal-503 factory used throughout
`_proxy_with_retries_inner` and is the dominant rejection path. Adding a required
`reason` argument while updating only the first three raises `TypeError` **on the
LB request path**, turning a 503-with-Retry-After into a 500 and skipping the
demand-retention side effect that keeps unplaced demand visible to the autoscaler.
Add the argument with a default, update all four call sites, and note that
`_record_rejection` is deduplicated by job id (docstring `:2071-2078`), so this
counts distinct rejected jobs, not raw rejected requests. Name and document it
accordingly.

---

## Secret access

### The manifest

The agent's ExternalSecret is rendered by the `datadog-wrapper` chart in namespace
`observability-datadog`, exactly as on boltz-prod. Adapted from
`deployment/helm-addons/datadog-wrapper/templates/externalsecret.yaml:4-30`, it
differs from the boltz-prod object in exactly three ways, each stated as a
deliberate deviation rather than presented as identical: the store is the
**regional** one, the remote key is the **new in-account** secret, and only
**api-key** is mapped.

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: datadog-global-secret
  namespace: observability-datadog
spec:
  refreshInterval: "1h"
  secretStoreRef:
    name: aws-secrets-manager          # regional store, not -global: see below
    kind: ClusterSecretStore
  target:
    name: datadog-api-key
    creationPolicy: Owner
    deletionPolicy: Retain
  data:
    - secretKey: api-key
      remoteRef:
        key: skypilot/gitops-hub-rainier/datadog/credentials
        property: api_key
```

**Why `aws-secrets-manager` and not `aws-secrets-manager-global`.** Their specs on
this cluster are byte-identical (`provider.aws {region: us-east-1, service: SecretsManager}`,
no `auth`, no `roleARN`), and all four working ExternalSecrets in namespace
`skypilot` plus all eight cluster-wide use the regional one. The "global" store has
never been exercised here, so its `Valid` status is only store-level validation.
Since the secret now lives in-account, using the unexercised store would add an
untested variable for zero benefit.

**Why replicate rather than federate.** Prod (421498156696), test (361913687221)
and management (911167932214) already keep separate copies of this same logical
secret, so a fourth is consistent with existing practice. The alternative is a
resource policy on the 421498156696 secret plus a cross-account `roleARN`
ClusterSecretStore: that is an IAM change in the **production** account to serve a
management-cluster addon, and it debuts a store shape never used on this cluster.
The cost of replication is honest and stated in [Risks](#risks): a fourth rotation
site.

The Terraform-side ExternalSecret precedent, if one is ever needed in namespace
`skypilot`, is the nebius block at
`.../skypilot_control_plane/eso.tf:118-146` (the JSON-property variant). This
design does **not** create one there: the Datadog secret belongs to the addon
chart's namespace.

Layering note: the IAM grant lands in the SkyPilot module's
`eso_read_secret_keys` even though the consuming ExternalSecret lives in
`observability-datadog`. That is a mild wart. It is chosen because that local is
the existing, documented extension point for hub ESO grants and adding a key there
is a policy-content update rather than a new IAM object. The alternative is to own
the grant in the host module, which the module's own comment at `eso.tf:5-7`
deliberately avoids so the host stays SkyPilot-agnostic. Flagged for review.

### How each workload reads it

- **Datadog agent DaemonSet and cluster-agent** (ns `observability-datadog`):
  `DD_API_KEY` via `secretKeyRef {name: datadog-api-key, key: api-key}`. **This is
  the only consumer of the key anywhere.**
- **api-server pod** (ns `skypilot`): reads **no** Datadog secret. It gets
  `DD_AGENT_HOST` (fieldRef `status.hostIP`), `DD_DOGSTATSD_PORT`, `DD_ENV`,
  `DD_SERVICE`, `SKYPILOT_SERVE_DOGSTATSD_ENABLED`. All non-secret.
- **11 controller processes**: inherit that env for free. `sky/server/server.py:2488`
  calls `capture_clean_server_env()`; `sky/server/clean_env.py:22-32` stores
  `dict(os.environ)`; `sky/utils/command_runner.py:1969` passes it as the subprocess
  env for consolidation-mode spawns (`sky/serve/server/impl.py:859-864`). It is a
  one-shot snapshot taken at api-server start, which is exactly right for static
  Deployment-level env.
- **17 LB pods**: read **no** Datadog secret and get **no** new env. Their PodSpec
  is unchanged.

### Keeping the key out of logs, env dumps and persisted task YAML

The guarantee here is structural, not procedural: **the key is never in the address
space of any SkyPilot process**, so there is nothing for a logger, a crash handler,
or a YAML serializer to capture.

- **Container image:** nothing added. No `datadog`/`ddtrace` dependency, so no
  credential can be baked into a build layer. Worth a CI grep assertion that
  `datadog|ddtrace|dogstatsd` never appears outside `sky/serve/dogstatsd.py`.
- **Env dumps:** the only new api-server env vars are an RFC1918 address, a port,
  and two tag strings. Note for accuracy: `sky/utils/debug_utils.py:592` iterates
  `os.environ` but emits only keys prefixed `SKYPILOT_`/`SKY_`, masking those in
  `_SENSITIVE_ENV_VARS` (`:53-62`), so `DD_*` names are excluded by prefix anyway.
  An earlier draft proposed adding `DD_API_KEY` to `redact_config`
  (`sky/utils/debug_dump_helpers.py`) and `_redact_secrets_values`
  (`sky/utils/common_utils.py:584`); **neither function can touch an env var** (the
  first walks the SkyPilot config dict, the second rewrites `--secret K=V` argv
  tokens), so that control would have been a no-op and its proposed unit test
  unimplementable. Under this design the question is moot.
- **Persisted task YAML and the serve DB:** the LB PodSpec is built by
  `_build_deployment_dict` and handed to the Kubernetes API only; it is never
  written to `serve_state`. Since no Datadog env is added to it, there is nothing
  to leak even if it were.
- **Never in git:** only the Secrets Manager **key name** appears in
  `terragrunt.hcl` and `values-hub.yaml`. The value is created out of band, exactly
  like the other four ESO-sourced SkyPilot secrets.
- **Pre-existing hazard, flagged but out of scope:**
  `scripts/datadog-credentials.sh:6-11` in boltz-platform defines
  `_DD_API_KEY_DEFAULT` and `_DD_APP_KEY_DEFAULT` as literal committed values. That
  pair should be rotated and the fallback removed independently of this work, and
  it is a strong argument for minting a **fresh, ingestion-scoped** key for the new
  secret rather than copying the terraform-provider key (whose stated purpose in
  the management account is "terraform changes to datadog monitor setups"). Do not
  extend that script pattern to SkyPilot; model any read-path tooling on
  `scripts/datadog-logs.py:15-26`, which reads from the environment with no
  committed defaults.
- **Rotation:** updating the replicated value propagates to the `datadog-api-key`
  Secret within the 1h `refreshInterval`, but the agent reads `DD_API_KEY` via
  `secretKeyRef` at container start, so the DaemonSet must be restarted to pick it
  up. Noted in the runbook.

---

## Rejected alternatives

**Prometheus bridge: enable `apiService.metrics.enabled` and scrape into an OTel
collector.** Attractive because it is Datadog-agnostic and the plumbing already
exists, but rejected on three verified grounds.

- *It is not free, contrary to the obvious reading.* The metrics server is **not** a
  separate process: `sky/server/server.py:2440-2451` schedules it on the
  supervisor's background uvloop. `/metrics` registers `_BURN_RATE_COLLECTOR`,
  `_WORKSPACE_USAGE_COLLECTOR` and `_MANAGED_JOBS_COLLECTOR`
  (`sky/server/metrics.py:620-641`), which run synchronous PostgreSQL queries
  (`get_clusters()` plus per-cluster cost, and a managed-jobs aggregate) inside the
  supervisor, which sets `db_utils.set_max_connections(1)` at
  `sky/server/server.py:2371`. With a 30s scrape and a 30s cache TTL, effectively
  every scrape recomputes and holds the single connection while the managed-job
  refresh thread in that same process needs it. This is the same pool-starvation
  class this team already fixed once.
- *Multiprocess aggregation is subtly wrong for the headline metric.*
  `run_round_if_stale` (`sky/serve/reserved_capacity_broker.py:775-812`) lets **any**
  claimant's controller drive a round, and that round publishes grants for **all**
  services in the pool, so the emitting pid rotates. `multiprocess_mode='livesum'`
  would sum the same `grant{service:B}` across every controller that has ever
  driven a round, corrupting exactly the reserved-fill numbers the project exists
  to produce. It is fixable with `livemostrecent` (verified supported: the live
  image ships `prometheus_client 0.25.0`), but `sky/setup_files/dependencies.py:92`
  pins only `>=0.8.0`, so the floor would have to be raised. DogStatsD sidesteps
  this entirely: gauges are last-value-wins per flush and rounds are serialized
  under a cross-process lock, so exactly one emitter exists per round.
- *Delivery and coverage.* A new `skypilot-telemetry` namespace is **not** in the
  `infrastructure` AppProject destination allowlist (verified), so it needs an
  AppProject edit, whereas `observability-datadog` is already allowed. And it
  yields no logs, no Kubernetes events, and no OOMKill or restart visibility.

**Direct HTTPS submission from the controller to `api/v2/series`.** Network-feasible
(the 403s prove it) and the fastest to build, but rejected: it puts a live write
credential inside the api-server process and therefore inside the clean-env snapshot
handed to all 11 controllers, it is a bespoke intake client where the org standard
is OTLP through a collector, it yields no logs, and LB telemetry would go blind
exactly during an api-server outage, which is precisely when the external LB
matters most.

**Putting a Datadog key or `DD_*` env in the LB pod.** Rejected on three counts.
It places a write credential in the pod that terminates world-facing ELB traffic on
:30001, cutting against the deliberate contract at `sky/serve/lb_k8s.py:795-857`
(whitelisted runtime fields, refuses arbitrary volumes, service account, or host
networking) and `:1135` (`automountServiceAccountToken: False`). It rolls all 17 LB
Deployments and changes the pod UID that is the drain-proof session identity
(`:1082-1084`), so instrumenting drain proof would perturb drain proof. And it is
unnecessary: the node agent collects LB stdout, restarts and OOMKills, and the
controller relays LB metrics.

**A metrics containerPort on the LB.** Same PodSpec-roll objection. Also note
`_lb_runtime_revision` (`sky/serve/lb_k8s.py:616-624`, applied at `:1123-1126`)
hashes only `controller_image_digest + termination_grace_period_seconds + service_hash`,
so an env-or-port-only change does **not** bump that annotation even though the
template diff still rolls the pod.

**A `serve-log-tailer` sidecar to collect controller logs.** Tempting, because it is
a pure values change (`apiService.sidecarContainers`, `charts/skypilot/values.yaml:230`,
rendered at `charts/skypilot/templates/api-deployment.yaml:711-713`) and it is the
only way to get controller logs into Datadog. **Rejected for now as too dangerous
for the value.** Verified coupling: `skypilot-api-service`'s selector is
`{"app":"skypilot-api","skypilot.co/ready":"true"}` and `skypilot.co/ready` is a
**static** template label (`api-deployment.yaml:175`), so pod readiness is the only
endpoint gate, and the Endpoints object has exactly one address. A sidecar in
CrashLoopBackOff makes the pod NotReady, removes the only endpoint, and every LB's
`--controller-addr http://skypilot-api-service.skypilot.svc/...` starts failing;
after `RESERVED_FILL_ACTIVITY_MAX_LAG_SECONDS` (60s,
`sky/serve/constants.py:640`) the broker's activity input goes blind for every
claimant and reserved-capacity arbitration degrades. A log shipper must not be able
to do that. Two further traps if it is ever revisited: logrotate uses `copytruncate`
at `size 10M` (`api-deployment.yaml:667-700`), so `tail -F` would re-emit up to
10 MB per service per rotation; and with 14 `controller.log` files totalling ~71 MB
and no persisted offset (the volume would be mounted read-only), a naive tailer
re-ships the entire backlog on **every** api-server restart, which at the observed
8-restarts-per-13-hours cadence is roughly 500 MB/day of duplicate into the indexed
tier. See [Open questions](#open-questions).

**`DD_ENTITY_ID` on any pod.** An earlier draft added it for origin detection.
DogStatsD origin detection via `DD_ENTITY_ID` is a **client library** feature: the
official library reads the env var and appends a `dd.internal.entity_id` tag. A
hand-rolled emitter that formats `name:value|type|#tags` does not, so the variable
would be inert. Dropped.

**Cross-account secret federation.** Covered in [Secret access](#secret-access).

---

## Failure isolation

**Invariant: serve behavior is byte-for-byte unaffected by anything Datadog does.**
No control-plane decision (scaling, drain, arbitration, routing, admission) reads
any value this design writes.

- **Datadog unreachable, or the API key is wrong or rotated.** No SkyPilot process
  ever connects to Datadog. The agent's own forwarder retries and drops; SkyPilot
  never learns. Metrics gap only.
- **The agent is down, crash-looping, or was never installed.** The emitter's
  socket is connected to the node kernel, not to Datadog. `sendto` returns
  `ECONNREFUSED`, which is swallowed and counted as a drop. Worst case per emit is
  a single non-blocking syscall. There is no code path in which telemetry latency
  can depend on Datadog availability.
- **The emitter itself throws.** Every call site is wrapped in
  `serve_telemetry.emit(...)`, which catches `Exception` and logs at debug. The
  realistic faults (socket exhaustion, a mapper bug on an unexpected `None`) become
  a debug line, not an exception inside a broker round or a drain path.
- **Ordering protection at the broker.** The emit sits after the publish and after
  the round log, and before the blackout return. It can therefore never abort a
  round between the CAS and the return, which would have fed the autoscaler zero
  free slots and produced a spurious pool-wide scale-down.
- **The LB sync handler.** The emit is a read of an already-parsed
  `request_data` dict placed after the `authority[0]` gate, and is wrapped. An
  unguarded raise there would 503 the LB sync, which would stop routing-spec
  delivery and demand accounting; this is the single most dangerous site in the
  design and is why the guard is structural rather than aspirational.
- **The LB sync payload (M3).** The additions are inside a dict built **outside**
  the `try` that owns the `restore` in its `finally`, so a raise there silently
  discards a drained request batch and the outer loop retries forever without
  alerting. Mitigation: only reference attributes verified to exist
  (`self._lb_slot` at `:299`, `self._ha_stats()` at `:460`, both confirmed), and add
  a unit test asserting the payload builds on a non-HA LB.
- **ESO cannot read the secret.** The ExternalSecret goes NotReady; because
  `deletionPolicy: Retain`, the existing `datadog-api-key` Secret survives and the
  agent keeps running on the old key. If the Secret never existed, the agent pods
  fail to start. Neither touches SkyPilot. Note the one ordering constraint: a
  `secretKeyRef` to a missing Secret blocks **agent** pod start, which is why M0
  and M1 are separate milestones with a verification gate between them.
- **Kill switch.** `touch /tmp/skypilot-datadog-disabled` in the api-server
  container silences all 11 controllers within one tick, with no restart. Full
  removal is deleting one ArgoCD Application and one namespace.
- **What is not protected.** Datadog **cost** is not isolated: an unbounded tag or a
  log-volume surprise is a billing event, not a SkyPilot event. Bound tags at
  review time and watch the first week.

---

## Deploy runbook

Nothing below has been executed. All verification performed so far was read-only.

**Restart summary, stated up front.** M0 and M1 restart nothing. **M2 restarts the
api-server pod.** Because the Deployment strategy is `Recreate` with `replicas: 1`,
that kills and respawns **all 11 SkyServe controller processes**, makes the API
unavailable for the restart window, resets every process-local drain-proof and
broker counter, and aborts any in-progress drain wave. Because the M2 image digest
changes, it also **rolls all 17 LB Deployments**, and each roll changes the LB pod
UID that is the drain-proof session identity. **Schedule M2 in a quiet window, and
check that no service is mid-update or mid-retirement first.** M3 and M4 carry the
same cost and should ride routine image bumps.

### Step 0: snapshot the release (rollback artifact)

```
helm get values skypilot -n skypilot --kube-context ghub-skypilot > /tmp/skypilot-values-rev286.yaml
helm history skypilot -n skypilot --kube-context ghub-skypilot | tail -5
kubectl --context ghub-skypilot -n skypilot get deploy skypilot-api-server \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

Expect chart `skypilot-1.1.811`, revision 286, image digest
`...skypilot-nightly-boltz:1.1.811@sha256:f794cf80f3156449937057c35718ede6757fb2a7c91bc386ae168e91c975bf90`.

### Step 1 (M0): create the secret, then grant it

Operator supplies the api_key value. Prefer a **fresh, ingestion-scoped key**
minted in the Datadog org over copying the terraform-provider key.

```
# Create secret skypilot/gitops-hub-rainier/datadog/credentials in
# account 255203429798, us-east-1, value {"api_key": "<operator-supplied>"}.
# Then land the Terraform change adding it to local.eso_read_secret_keys.
```

Verify (read-only):

```
aws --profile boltz-gitops-hub secretsmanager describe-secret \
  --secret-id skypilot/gitops-hub-rainier/datadog/credentials --region us-east-1
aws --profile boltz-gitops-hub iam get-role-policy \
  --role-name boltz-platform-gitops-hub-rainier-external-secrets \
  --policy-name skypilot-eso-read-secrets
```

Expect the ARN to resolve and the policy `Resource` list to contain
`...:secret:skypilot/gitops-hub-rainier/datadog/credentials-*`.

**Terragrunt pin warning.** If this change is applied via terragrunt against the
`skypilot-control-plane` module, bump
`environments/gitops-hub-rainier/skypilot-control-plane/skypilot-pin.json` from
`1.1.805` to the live version **in the same commit**, or the apply downgrades the
control plane six releases. Verify the plan shows no `helm_release.skypilot`
version change before applying.

No restart. No SkyPilot impact.

### Step 2 (M1): install the Datadog agent

Merge the `datadog-hub` ArgoCD Application and `values-hub.yaml`, then sync
manually (`selfHeal: false` for the first week).

Verify (read-only):

```
kubectl --context ghub-skypilot -n observability-datadog get externalsecret
kubectl --context ghub-skypilot -n observability-datadog get daemonset
kubectl --context ghub-skypilot -n observability-datadog get pods
```

Expect the ExternalSecret `SecretSynced=True`, the DaemonSet **3/3 ready**, and the
cluster-agent Deployment 1/1.

**This is the go/no-go gate.** Confirm in the Datadog UI that LB container logs are
arriving, tagged by `service` from the `skypilot-serve-lb` pod label, **with zero
SkyPilot changes**. Also confirm Kubernetes events and container restarts are
visible. If this does not work, stop: nothing downstream will either.

No restart. No SkyPilot impact. Full rollback is deleting the Application and the
namespace (the `datadog-api-key` Secret has `deletionPolicy: Retain`, so delete it
explicitly if you want a clean teardown).

### Step 3 (M2): controller metrics

Merge the fork code, build and push the image, then apply the env additions and the
new image together in **one** restart. Preferred path is terragrunt (with the pin
bumped). For an out-of-band deploy, `--reuse-values` is mandatory:

```
helm upgrade skypilot ./charts/skypilot -n skypilot --kube-context ghub-skypilot \
  --reuse-values \
  --set apiService.image=<new-repo@sha256:...>
```

`--reuse-values` preserves `dbConnectionSecretName: skypilot-postgres-auth`, the
OAuth and serve-auth secret refs, the storage and ingress blocks, the resource
requests, and the two `extraInitContainers`. Dropping it breaks the deployment.
Note that Helm **replaces** arrays rather than merging them, so the `extraEnvs`
list must be supplied complete (21 existing entries plus the 5 new ones), which is
another reason to route this through the terragrunt module rather than by hand.

Anything applied by hand out of band will be **silently reverted by the next
terragrunt apply** unless the same values land in `terragrunt.hcl`.

Verify (read-only):

```
kubectl --context ghub-skypilot -n skypilot get pods -l app=skypilot-api
kubectl --context ghub-skypilot -n skypilot exec <api-pod> -c skypilot-api -- \
  ps aux | grep -c sky.serve.service          # expect 11
kubectl --context ghub-skypilot -n skypilot exec <api-pod> -c skypilot-api -- \
  python -c "import os; print(os.environ.get('DD_AGENT_HOST'), os.environ.get('DD_DOGSTATSD_PORT'))"
kubectl --context ghub-skypilot -n skypilot get deploy -l skypilot-serve-lb --no-headers | wc -l
```

Expect the pod Ready, 11 controllers back, a private IP and `8125`, and 17 LB
Deployments rolling to the new digest. Then confirm `sky.serve.*` series in
Datadog, starting with `sky.serve.reserved_fill.demonstrated_need`.

**This step restarts the api-server pod and rolls every LB. Quiet window.**

### Step 4 (M3, M4): LB payload extension, rejection counter, dashboards

Ride routine image bumps. Same restart and roll cost as step 3, so batch them with
other planned deploys rather than taking extra restarts. After M3, verify
`sky.serve.lb.session_changed` is **not** firing every 20s on the 7 HA services;
if it is, the `lb_slot` keying is wrong.

Register dashboards and monitors in the existing two-tier convention
(`deployment/terragrunt/environments/global/datadog/README.md:5-18`): application
monitors and dashboards as exported JSON in `environments/global/datadog`,
infrastructure monitors as HCL in `modules/cloud-datadog/monitors.tf`. There is
prior art: `monitors/skypilot-low-priority-job-failure-rate.json`. Tag
`deployment_name:skypilot-control-plane`.

### Rollback, cheapest first

1. **Silence telemetry, zero risk, no restart:**
   `kubectl --context ghub-skypilot -n skypilot exec <api-pod> -c skypilot-api -- touch /tmp/skypilot-datadog-disabled`.
   All 11 controllers stop emitting within one tick.
2. **Remove the agent:** delete the `datadog-hub` Application, then the
   `observability-datadog` namespace. Metrics and logs stop. SkyPilot is
   unaffected: the emitter swallows `ECONNREFUSED`.
3. **Revert the env:** terragrunt apply with `local.datadog_envs` removed, or
   `helm rollback skypilot 286 -n skypilot --kube-context ghub-skypilot` for an
   emergency. Re-apply terragrunt afterwards or the next apply reintroduces it.
   One `Recreate` restart.
4. **Revert the code:** `--set apiService.image=<previous digest>`
   (rev 286 is `sha256:f794cf80...`). Rolls all 17 LBs again.
5. **Leave the secret and IAM in place.** They are inert with no agent.

Prefer the targeted `--set` forms over `helm rollback`, which reverts image and
values together. At no point does rollback require touching the request path.

---

## Milestones

| # | Deliverable | Effort | Independently shippable? |
|---|---|---|---|
| M0 | AWS secret in 255203429798 + `eso_read_secret_keys` grant + pin bump | 0.5d plus review latency on another team's queue | Yes. No runtime effect. |
| M1 | `datadog-hub` ArgoCD Application, `values-hub.yaml`, agent DaemonSet + cluster-agent | 1d | **Yes, and this is the value gate.** Delivers LB container logs, k8s events, restart and OOMKill visibility, node metrics, with zero SkyPilot code. |
| M2 | `dogstatsd.py`, `serve_telemetry.py`, emit sites for broker / drain proof / autoscaler / sync-payload LB fields, `local.datadog_envs` | 2d | Yes. Covers problem areas 1, 2, 4 and the cheap half of 3. Includes `demonstrated_need`, the rollout gate. |
| M3 | `lb_slot` + `ha_observability` + occupancy aggregates into the sync payload (extract the `_capacity` helper), controller-side per-slot keying | 1.5d | Yes. Closes the rest of problem area 3. |
| M4 | Rejection counter with bounded `reason` across all 4 call sites, dashboards and monitors registered in `environments/global/datadog` | 1d | Yes. |

Roughly 6 working days of work, realistically two to three weeks elapsed: M0 sits
on another team's Terraform queue, and M2 onward each want a scheduled restart
window.

If only one milestone ships, it should be **M1**: it is the only one with no
SkyPilot risk, and container logs plus restart visibility for 17 LB pods is real
value on a cluster that currently has none.

---

## Risks

1. **M0 gates everything and is not in this team's control.** It is a Terraform
   change plus a secret creation in the gitops-hub account. If the org rejects a
   replicated copy and insists on a single source of truth, scope grows to a
   resource policy on the 421498156696 secret plus a cross-account `roleARN` store
   shape that has never been exercised on this cluster.
2. **A fourth copy of the Datadog credential now exists** (prod, test, management,
   gitops-hub). Rotation must remember it. Mitigate with a description field
   pointing at the source ARN and a runbook line. Related: the committed
   `_DD_API_KEY_DEFAULT`/`_DD_APP_KEY_DEFAULT` pair in
   `scripts/datadog-credentials.sh:6-11` should be rotated independently.
3. **The M2 restart is a genuine control-plane interruption:** `Recreate`,
   `replicas: 1`, 11 controllers inside. It also resets every process-local counter,
   so the first hour of drain-proof and broker data is an artefact of the deploy
   itself, and the "watch `demonstrated_need` for a week" clock starts at deploy,
   not retroactively.
4. **Terragrunt drift will bite the deploy.** The pin says 1.1.805, live is 1.1.811,
   with 8 revisions in 13 hours. Any apply that does not bump the pin downgrades
   the control plane six releases. Conversely, any hand-run `helm upgrade` is
   silently reverted by the next apply.
5. **Helm replaces arrays.** `--reuse-values` does not merge a hand-added
   `extraEnvs` entry with a later supplied array. The IaC change is mandatory, not
   documentation.
6. **The M3 sync-payload edit sits in a `finally`-restore hazard zone.** A raise
   while building the dict discards a drained request batch with no crash and no
   alert. Only reference verified attributes; add a payload-builds unit test.
7. **HA double-reporting will produce a false alarm if slot keying is wrong.** 7 of
   10 services run two syncing LBs. Getting this wrong emits a spurious
   `session_changed` every 20s per HA service, on the exact metric designated as
   the drain-proof invalidation alarm, and double-emits every LB gauge under
   identical tags.
8. **`_record_rejection` has a fourth call site on the request path** (`:4364`,
   inside `_unavailable()`). A required-argument change that misses it raises
   `TypeError` in the 503 factory, converting 503s into 500s and skipping the
   demand-retention side effect, so the autoscaler would scale **down** under unmet
   demand. Use a default argument and update all four.
9. **`rejected_in_window` is a gauge of unique jobs in a dedup window, not a
   rejection count** (`load_balancer.py:192`, `:2037-2060`). Dashboarding it as a
   rate produces plausible-looking wrong numbers.
10. **DogStatsD is a deviation from the org's OTLP convention.** boltz-platform
    applications emit OTLP/gRPC to a collector and use neither dd-trace nor
    DogStatsD. The deviation is chosen because adding an OTel Python SDK to an image
    shared by the controller and all 17 LBs is a real cost for counters and gauges,
    and DogStatsD is a first-class agent primitive. It does mean the org has no
    existing operational precedent for a DogStatsD ingestion path, and the existing
    `datadog_metric_tag_configuration` workflow assumes OTLP histograms.
11. **Datadog cost is estimated, not measured**, on two axes: roughly 800-900 custom
    timeseries, and log volume from 17 hot-path LB pods against an existing 30-day
    index plus S3 archive posture
    (`modules/cloud-datadog/s3_logs.tf:4-18, 64-92`). Be ready to use
    `containerExcludeLogs` (`helm_generation.tf:453-456`) if LB proxy logging turns
    out to be per-request.
12. **Datadog host billing for 3 new hosts.** Small in absolute terms, but it is a
    new line item and the agent is the first observability DaemonSet this cluster
    has ever had.
13. **The `ignoreDifferences` block is mandatory, not optional.** Omitting it means
    ArgoCD selfHeal rotates the cluster-agent token every reconcile and
    rolling-restarts the agent forever.
14. **Counters reset on controller restart**, and this pod restarts often. Monitors
    must be written to tolerate rate discontinuities even with delta emission.
15. **Node headroom is fine, contrary to an earlier draft** (~15 vCPU free on the
    busiest node, not ~4). Listed here so nobody re-derives a blocker that does not
    exist. Still set explicit agent limits and re-check with
    `kubectl describe node` after install.

---

## Open questions

1. **Should a fresh ingestion-scoped Datadog key be minted?** The management-account
   copy is described as being for "terraform changes to datadog monitor setups",
   which is a different purpose from agent ingestion, even though prod already uses
   it for exactly that. Given the committed-credentials issue in
   `scripts/datadog-credentials.sh`, minting a fresh key is probably right. Needs
   the Datadog org owner.
2. **Do we ever want controller logs in Datadog, and if so how?** They are files on
   a PVC, so the node agent cannot see them. The sidecar approach is rejected above
   because a crash-looping sidecar removes the api-server from its Service. Safer
   options, none costed yet: (a) accept metrics-only for the controllers and keep
   using `sky serve logs` plus Postgres; (b) ship a deliberately tiny set of
   discrete **events** over DogStatsD or the events API (drain deadline expiry
   without proof, logical-retirement abort with reason, broker blackout round,
   controller start/stop), which is maybe 5 lines and no new container; (c) a
   sidecar with a liveness probe that always succeeds and an explicit
   `restartPolicy` posture that cannot make the pod NotReady, if Kubernetes gives us
   that guarantee. Option (b) is the current lean.
3. **Does the layering wart matter?** The IAM grant lives in the SkyPilot module
   while the consuming ExternalSecret lives in `observability-datadog`. Acceptable,
   or should the grant move to the host module?
4. **Do the two `_capacity` occupancy call sites diverge after the M3 refactor?**
   Extracting the computation into a shared helper is the plan, but `_capacity` is
   async and the sync payload is built in an async context too, so confirm there is
   no blocking work being moved onto the wrong path.
5. **Which distributions, if any, justify percentiles later?** Each
   `datadog_metric_tag_configuration` entry doubles custom-metric count for that
   metric. Candidates are drain-deadline and occupancy-probe latency. Deferred out
   of v1 deliberately.
6. **`deployment_name` value.** This design uses `skypilot-control-plane` to keep
   SkyServe series separable from the platform's. Confirm that is the desired slug,
   since boltz-platform derives it from Terraform `var.deployment_name` and an unset
   value deliberately stamps nothing.
7. **Should `apiService.metrics.enabled` be turned on separately, on its own merits?**
   It would give api-server and managed-jobs metrics that nothing currently
   collects, and the agent could scrape them via an
   `ad.datadoghq.com/...openmetrics` annotation. That is a separate decision on a
   separate day, and it carries the DB-pool coupling documented above.
