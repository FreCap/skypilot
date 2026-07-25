# Serve external load balancer: HTTPS and data-path encryption

_Status: proposed_
_Last updated: 2026-07-25_

## Problem

The SkyServe external load balancer serves plaintext HTTP, and every hop of the
serve data path is unencrypted. Two hops carry production traffic today.

`sky/serve/serve_utils.py:1385-1400` records the original intent: the external
LB "intentionally serves HTTP behind the platform ingress", and task-level
`service.tls_credential` is rejected so the control plane cannot advertise an
HTTPS endpoint that does not exist. `sky/serve/lb_k8s.py:1092-1093` repeats it:
"no TLS handling in this pass -- TLS terminates at the ingress/ALB".

That platform ingress was never built for Serve. The API server got one; the
Serve load balancers did not. This design builds the missing half and closes
the replica hop.

### Verified current state (2026-07-25, `gitops-hub-rainier`, account 255203429798)

| Hop | Path | Encrypted | Evidence |
| --- | --- | --- | --- |
| 1. `compute_api` -> serve NLB | `http://k8s-skypilot-skypilot-d260f5c163-...elb.us-east-1.amazonaws.com:30001` | **No** | `deployment/helm/values/production/values-production.yaml:317-318` |
| 2. NLB -> LB pod `:30001` | in-cluster | No | Service port 30001/TCP -> targetPort 30001 |
| 3. LB pod -> replica `:8080` | replica **public IP**, cross-cloud, multi-region | **No** | `sky/serve/replica_managers.py:1625`; server config `use_internal_ips: false` |
| 4. control plane -> replica | SSH over the SSM proxy | Yes | existing SSM `ProxyCommand` path |
| 5. browser -> API ALB `:443` | internal ALB, ACM cert | Yes | `:80` returns `HTTP_301` to `:443` |
| 6. API ALB -> API pod `:46580` | in-cluster, target-type `ip` | No | ALB target group `Protocol=HTTP` |

Hop 1 carried **2.34 GB in 24h** on the production fleet NLB
(`AWS/NetworkELB` `ProcessedBytes`) and sends `SKYPILOT_SERVE_LB_AUTH_TOKEN` as
a bearer token in cleartext on every request. It is reachable from four peered
VPCs *and the whole tailnet*, though not from the internet.

Hop 3 leaves private networking, but only for the VM tiers:

- **Kubernetes tier** (research `10.5/16`, peered): never leaves private
  networking, and the node security group already admits `:8080` only from the
  hub CIDR.
- **AWS VM tier**: NAT EIP -> IGW -> a public EC2 IP in another account.
  Internet-routed at the IP layer. AWS may keep it on its own backbone, but
  that is not attestable or auditable, so it cannot be treated as a control.
- **GCP VM tier**: AWS NAT -> IGW -> a Google public IP. Unambiguously public
  transit, which settles the question on its own.

The exposed VM tiers are **intermittent**, correlated with paid spot capacity.
A validation run that happens to land on an all-Kubernetes fleet proves nothing
about the hop this design targets.

### What is already protected (do not overstate this)

`boltz-l4-fleet` payloads are **already AES-256-GCM encrypted end to end**,
backend to model container, independent of transport, and results go to S3 over
presigned HTTPS carried inside that envelope. The load balancer also strips the
bearer token before proxying, so no credential reaches a replica.

What an on-path attacker gets on hop 3 today is therefore request and response
*metadata* -- job and request ids, priorities, sizes, timing, status -- plus the
ability to substitute a replica. Not scientific payloads.

Two caveats cut the other way: the replica decrypt path fails open when
`encrypted` is absent, so the envelope is not an access-control boundary; and
**eight other serve services have no envelope at all**, so for them the
transport is the only protection.

State this accurately. Framing the work as "protein sequences in the clear" is
false and invites a reviewer to dismiss the whole change.

### The finding that outranks this design

`sky/provision/aws/instance.py` opens **every port in `resources.ports` to
`0.0.0.0/0`**, and the fleet declares `ports: 8080`. Since the load balancer
strips the bearer token and the replica has no authentication of its own,
**anyone on the internet can POST a job to a paid AWS replica and consume the
GPU.**

The platform already fixed exactly this on GCP:
`deployment/terragrunt-gcp/modules/skypilot_pool_gcp_vm/firewall.tf` denies
tcp:8080 from `0.0.0.0/0` and allows only the control-plane NAT `/32`, with the
comment "without this, the public internet can reach the replica's
unauthenticated inference server directly". The AWS spoke module ships IAM
only. That is an oversight, not an accepted risk.

TLS does not fix it -- TLS makes it *encrypted* anonymous access. Locking down
replica ingress is cheaper and removes more risk than everything below, and it
should ship first. It is tracked as M0.

### What is *not* a problem

All 12 ELBv2 load balancers in the cluster account are `Scheme=internal`; there
are zero internet-facing load balancers. The API server edge already forces
HTTPS. So the exposure is not "a public endpoint serving plaintext"; it is
plaintext on internal and inter-account hops, plus one hop over public IPs.

## Goals

1. Terminate TLS in front of every Serve load balancer, and make the advertised
   consumer endpoint `https://`.
2. Encrypt the LB-pod -> replica hop, which is the hop that actually traverses
   public networks.
3. Change no Serve behaviour when the feature is not configured. The default
   path stays byte-identical.

### Non-goals

- Encrypting hop 2 and hop 6 (load balancer -> pod, inside the cluster). Those
  are in-VPC, terminate at a pod IP, and match how the API ALB already works.
- mTLS / client authentication. The existing bearer token remains the
  authentication mechanism; this design only stops it crossing the wire in the
  clear.
- Making the Serve LBs internet-facing. They stay `internal`.

## Constraint that drives the design

The replica hop **cannot** be solved with network topology. The cluster VPC
(`vpc-0a3e3f4817dd201b5`) has three active peerings, none to the replica
account, and `ml_models/providers/skypilot/boltz-2/boltz-l4-fleet.serve.yaml`
deliberately spreads replicas across clouds and regions ("Cross-cloud spot
pool: replicas diversify across clouds, regions"), including AWS opt-in regions
and GCP. Peering to every region of every cloud a spot replica may land in is
not viable, so `use_internal_ips: true` is rejected as the primary remedy.

Therefore hop 3 must be encrypted at the transport layer.

## Design

The work splits into two independently shippable milestones. M1 is a
self-contained fix for hop 1 and is the reimplementation of PR #367. M2 closes
hop 3 and is substantially larger because the replica's port is served by the
user's own container.

### M1: TLS at the Serve load balancer

Everything M1 needs already exists in the cluster:

- ExternalDNS `1/1`, args `--source=service --policy=upsert-only
  --domain-filter=int.boltz.bio --txt-owner-id=gitops-hub-rainier`.
- Private Route53 zone `int.boltz.bio.` (`Z01022063BKTOF5KHA0M5`).
- ACM certificate `*.int.boltz.bio`, status `ISSUED`,
  `arn:aws:acm:us-east-1:255203429798:certificate/146cc864-630f-4865-9f60-6b256e9b7902`
  — the same certificate the API ALB already serves.
- `aws-load-balancer-controller` `2/2`, which already injects
  `spec.loadBalancerClass: service.k8s.aws/nlb` on these Services.

The LB Service gains a TLS listener and a stable hostname:

```yaml
metadata:
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-ssl-cert: <acm-arn>
    service.beta.kubernetes.io/aws-load-balancer-ssl-ports: "https"
    service.beta.kubernetes.io/aws-load-balancer-ssl-negotiation-policy: ELBSecurityPolicy-TLS13-1-2-2021-06
    external-dns.alpha.kubernetes.io/hostname: <service>.int.boltz.bio
spec:
  ports:
    - name: http                 # retained during migration, removed in M1c
      port: 30001
      targetPort: 30001
      protocol: TCP
    - name: https
      port: 443
      targetPort: 30001
      protocol: TCP
```

The NLB terminates TLS on 443 and forwards TCP to the LB pod's existing
plaintext 30001. The LB pod itself is unchanged, so the SkyServe LB keeps
serving HTTP and `service.tls_credential` stays rejected — the guard in
`serve_utils.py` remains correct and is deliberately not relaxed.

Three properties make this safe against the HA-slot contract in
`lb_k8s.py:1165-1219`:

- Adding a port and annotations does **not** change `spec.selector`, so the
  selector-only cutover and its retained ClusterIP/NLB are untouched.
- `_service_has_desired_routing` (`lb_k8s.py:1245-1280`) already compares
  annotations as a **subset** (`all(annotations.get(k) == v for k, v in
  desired_annotations.items())`), so the controller-injected annotations do not
  cause reconcile churn.
- The same function compares ports by exact list equality, so adding the 443
  port correctly drives one patch and then converges.

Because a multi-port Service requires every port to be named, the existing
unnamed 30001 port gains `name: http`. This is an in-place spec update; it does
not recreate the Service or the NLB.

#### Configuration: environment variables, not `skypilot_config`

PR #367 configured this through `apiService.extraEnvs`. That was correct and is
retained. The initial instinct to move it into `skypilot_config` is **wrong**
here, for a specific structural reason.

`_build_service_dict` is called from three places, in two different processes:

- `create_lb_deployment_and_service` (`lb_k8s.py:1804`) and
  `_create_ha_lb_objects` (`lb_k8s.py:1683`) — the API server.
- `ensure_lb_objects_exist` (`lb_k8s.py:2238` and `2320`) — the **controller's
  60-second re-ensure loop**.

If those two processes disagreed about whether TLS is configured, they would
fight: one would add the 443 port and annotations, the other would reconcile
them away, every 60 seconds, forever. That is exactly the split-brain class
already hit once in this fork, where the consolidation-mode controller read a
per-service config snapshot frozen at `serve up` while the API server read live
config, leaving a permanently dangling advertised endpoint.

`is_external_load_balancer_mode()` (`serve_utils.py:1084-1095`) documents the
chosen defence: Helm "injects the same explicit capability into the API pod and
every generated LB pod. Consolidated controller children inherit the API pod's
environment. No persisted or per-service config participates, so all processes
in the topology necessarily agree."

The HTTPS settings follow that contract exactly, alongside
`SKYPILOT_SERVE_EXTERNAL_LB_ENABLED`:

```
SKYPILOT_SERVE_EXTERNAL_HTTPS_CERT_ARN     = arn:aws:acm:...
SKYPILOT_SERVE_EXTERNAL_HTTPS_DNS_SUFFIX   = int.boltz.bio
SKYPILOT_SERVE_EXTERNAL_HTTPS_SSL_POLICY   = ELBSecurityPolicy-TLS13-1-2-2021-06  (optional)
```

Both the cert ARN and the DNS suffix are required together. A partial
configuration fails closed at service creation rather than silently producing a
Service with a certificate and no hostname, or a hostname served only by a
plaintext listener.

#### Endpoint advertisement

`compute_api` does not use SkyPilot's advertised endpoint; it reads
`SKYPILOT_SERVE_LB_URL`, hardcoded to the NLB's generated
`*.elb.amazonaws.com` name in Helm values. That is brittle: the name changes if
the Service is recreated. M1 replaces it with the stable
`https://<service>.int.boltz.bio`, which is a strict improvement independent of
TLS.

The SkyPilot-advertised endpoint (`<svc>.<ns>.svc.cluster.local:30001`, a pure
function of name and namespace) is unchanged in M1b and revisited in M1c.

#### Blocking prerequisites

Adversarial review found four things that must land **before** any live
endpoint changes. Each of them turns a routine rollout into an incident.

1. **The CI byte-equality gate.**
   `.github/workflows/skypilot-test-fleet-deploy.yml` compares
   `sky serve status --endpoint` against a `yq` read of the values file and
   `exit 1`s on mismatch, *before* `sky serve update`. The production
   onboarding runbook has the same gate. Any endpoint-string change blocks
   every subsequent fleet update until the gate is updated in the **same**
   commit.
2. **TLS error codes in spill classification.**
   `skypilot.provider.ts` classifies definitive pre-accept transport errors as
   `{EAI_AGAIN, ECONNREFUSED, EHOSTUNREACH, ENETUNREACH, ENOTFOUND}`. TLS
   errors are absent, so a certificate fault becomes a **non-spillable silent
   outage** instead of a clean failover. Ship this first, on its own.
3. **`$patch: delete` port removal in both Service reconcilers.**
   `v1.ServicePort` strategic-merge keys on `port`, so a plain merge patch
   *adds* 443 and *keeps* 30001. Both directions then wedge against the
   exact-list-equality drift check, which re-runs the full LB create path every
   60 seconds forever. Without this, M1d can never converge and rollback can
   never converge either.
4. **The endpoint contract.** `sky/serve/server/status.py` and
   `lb_k8s.lb_service_endpoint_or_none` hardcode `http://` and
   `LOAD_BALANCER_PORT_START`. They must become contract-aware together, in the
   same commit as the gate update, or `sky serve status --endpoint` advertises
   a dead port.

Two further risks are not code-fixable and need an owner decision:

- **Eight serve services are untracked.** `b25fi-gostar-v1`, `b25fi-v4`,
  `foldeverything-foldbench-rnp-8i`, `opendde-10c200s-v4(-mt-hybrid)`,
  `opendde-builder-{built,source}`, `protenixv2-hybrid-v1` have no reference in
  either repository, no CI signal, and no application-layer envelope. Because
  the config is read inside `_build_service_dict`, enabling it migrates **all**
  services on the next supervision pass. Either inventory their consumers or
  add a per-service opt-in before enabling anything globally.
- **`/_lb/capacity` failures degrade silently.** The platform catches them and
  returns a conservative scaling fallback, which throttles SkyPilot admission
  and shifts traffic elsewhere without paging. Probe it explicitly after each
  cutover step; it is the canary for this whole change.

#### Rollout

- **M0** — lock down replica ingress (AWS parity with GCP). Independent of
  everything else and the highest risk reduction per unit of work.
- **M1a** — ship the code behind unset config. No live change.
- **M1b** — configure `certificate_arn` + `dns_suffix` on **test only**.
  Services reconcile to dual-listen: plaintext 30001 *and* TLS 443. Nothing
  breaks; no consumer has moved. Verify `kubectl get svc` shows both ports, the
  NLB has a TLS listener with the expected certificate and policy, the private
  zone record exists, `openssl s_client` completes, TLS 1.0/1.1 are refused,
  and plaintext 30001 still serves.
- **M1c** — flip `SKYPILOT_SERVE_LB_URL` to `https://<name>.int.boltz.bio` in
  the same PR as the gate update. Probe `/_lb/capacity` explicitly, not just
  predict. Then production, after a soak, watching spill rate as the canary.
- **M1d** — drop the plaintext 30001 port. This is what makes "HTTPS only"
  real, and prerequisite 3 is what makes it converge.

Rollback at every step is a single env var or values revert, **except** M1d,
which is why prerequisite 3 is not optional.

One property to confirm by experiment before M1b: whether the AWS Load Balancer
Controller preserves NLB identity when a Service's ports change, or recreates
the listener and drops in-flight connections. Test on `boltz-l4-fleet-test`.

### M2: encrypting the LB -> replica hop

Hop 3 is harder than hop 1 because **port 8080 is served by the user's own
model container**, not by SkyPilot. Something must terminate TLS on the replica
without requiring every workload to implement it.

Two sub-problems: terminating TLS on the replica, and letting the LB decide
whether to trust what it finds there.

#### Termination: on the existing port, not a new one

A TLS-terminating proxy runs on the replica, started from the task `setup`. It
**binds the existing service port 8080** and forwards to the model container on
loopback (`127.0.0.1:8081`). The model server itself is untouched;
`service.ports` stays `8080`.

Keeping the port is not cosmetic. An earlier draft of this design put TLS on a
*new* port, which would have been actively dangerous on GCP:
`deployment/terragrunt-gcp/modules/skypilot_pool_gcp_vm/firewall.tf` allowlists
**tcp:8080 by number**:

```
 800  skypilot-allow-ctrl-service   allow tcp:8080 from the control-plane NAT egress IP
 970  skypilot-deny-public-service  deny  tcp:8080 from 0.0.0.0/0
```

Moving the replica to, say, `:8443` would have done two bad things at once: the
priority-800 allow would no longer match, so the control plane could not reach
GCP replicas at all; and the priority-970 deny would no longer match either, so
the new TLS port would fall through to SkyPilot's auto-created
`sky-ports-<cluster>` rule (`0.0.0.0/0` at priority 65534) and be **world-open**.
The net effect would be an unreachable-but-exposed replica: strictly worse than
today, on both counts, and only on GCP.

Terminating on 8080 keeps every existing network control correct and unchanged:
GCP's two firewall rules, and the AWS security-group lockdown in M0.

#### Both clouds, one mechanism

The fleet spans **20 AWS regions and 18 GCP regions**, and both clouds are
configured `use_internal_ips: false`, so replicas on both get public IPs. The
GCP hop is the clearer violation: AWS NAT -> IGW -> a Google public IP crosses
between two providers' networks.

The mechanism is deliberately cloud-agnostic. TLS material arrives as task
env/secret and the proxy starts in `setup`, so AWS, GCP, and the Kubernetes
tier are handled identically with no per-cloud code. The only per-cloud
difference is the *network* control: GCP is already locked to the control-plane
NAT `/32`; AWS is not, which is M0.

#### Trust

A replica has no stable DNS name and its public IP is assigned at boot, so
conventional CA-issued certificates do not fit. Neither cert-manager nor an ACM
private CA exists in this deployment, and ACM's own certificates cannot be
exported to an instance, so a CA-issued replica certificate is not available at
any price short of standing up new infrastructure.

**The chosen model is: the operator mints one keypair and provisions it as
configuration, exactly like the load balancer's bearer tokens.**

1. An operator runs `replica_tls.generate_material()` once and stores the pair
   in ESO / Helm values. Nothing mints at runtime.
2. The controller injects the private key into each replica task as a **task
   secret** (redacted from task YAML and logs) and the certificate as an
   ordinary env var.
3. The controller passes the certificate -- never the key -- to the LB pod.
4. The LB proxy, the controller readiness probe, and the LB occupancy probe all
   trust that certificate and nothing else.

Verification then succeeds exactly when the peer holds that private key. This
is certificate pinning expressed through the standard TLS verifier rather than
a bespoke fingerprint check.

An earlier draft of this design proposed that the replica generate its own
keypair and the control plane read the fingerprint back over SSH/SSM. That is
recorded here because it was rejected, not deferred: it puts an extra
control-plane round trip on the critical path of every launch, and the
implementation would have had three different trust models in one change.

**Known limitation, stated plainly:** one keypair covers the whole deployment,
so compromising any replica yields a key accepted for all of them. This is a
transport control, not a replica identity system. Per-replica identity needs an
issuing CA and is out of scope.

#### The three clients that must agree

This is the part that makes M2 dangerous, and it is why the rollout sequences
them explicitly. Three independent clients dial replicas, and they fail in
three different ways:

| Client | Location | Failure mode if left plaintext |
| --- | --- | --- |
| LB proxy | `load_balancer.py` client pool | Loud. Requests fail. |
| Controller readiness probe | `replica_managers.py`, `requests` | **Every replica marked NOT_READY, controller tears down live capacity.** |
| LB occupancy probe | `load_balancer.py`, `aiohttp`, wrapped in `except Exception: return None` | **Silent.** Occupancy goes unknown and concurrency-native autoscaling quietly degrades. |

All three are configured from `sky/serve/replica_tls.py` for exactly this
reason, and a test asserts they agree in both `pinned` and `unverified` modes.

#### Scope warning

M2 changes replica endpoint construction, three HTTP client construction sites,
and the fleet YAML (which must run a TLS proxy and bind the model container to
loopback). It is **not** a small change and must not be bundled with M1.

## Alternatives

- **`use_internal_ips: true` + VPC peering.** Rejected: replicas are
  multi-region and multi-cloud by design; no peering topology reaches them.
- **Service mesh / WireGuard / Tailscale overlay between LB and replicas.**
  Rejected for now: adds a daemon and an identity system to every ephemeral
  spot replica for the same property fingerprint-pinned TLS gives.
- **AWS Private CA per-replica certificates.** Viable but ~$400/month and more
  moving parts than pinning; kept as a fallback if pinning proves awkward.
- **`type: ClusterIP` and delete the NLBs.** Rejected on evidence: the fleet
  NLB carries 2.34 GB/day, so out-of-cluster consumers are real.
- **Relaxing the `tls_credential` guard so the LB pod serves TLS directly.**
  Rejected for M1: it would require mounting certificates into every LB pod and
  rotating them, where the NLB + ACM path rotates automatically.
- **Application-layer payload encryption.** Rejected: does not protect the
  bearer token or request metadata, and pushes crypto into the model server.

## Test plan

M1, unit:
- Service dict gains the TLS port and annotations only when configured; is
  byte-identical to today when unset.
- Partial config (`certificate_arn` without `dns_suffix`, and vice versa) is
  rejected with a clear error.
- `_service_has_desired_routing` returns `False` for a live Service missing the
  TLS port or annotations, and `True` once reconciled, including when the AWS
  controller has injected extra annotations.
- The HA-slot cutover path still produces an unchanged `spec.selector` with TLS
  enabled, and `cutover_generation` handling is unaffected.
- Port names are populated for every port whenever more than one port exists.

M1, live (test service first):
- `kubectl get svc` shows both ports; the NLB shows a TLS listener on 443 with
  the expected certificate and SSL policy.
- The Route53 record for `<service>.int.boltz.bio` exists and resolves to the
  NLB.
- `openssl s_client -connect <service>.int.boltz.bio:443` completes and
  presents the `*.int.boltz.bio` certificate; TLS 1.0/1.1 are refused.
- An authenticated request over HTTPS returns the same response as over
  plaintext 30001.
- Kill an LB pod and confirm the slot cutover still works with TLS configured.

M2 is specified with its own plan when M1 has soaked.

## Open questions

Decided:

- **Remove the plaintext listener (M1d), after a soak.** Availability without
  enforcement is not the ask.
- **Trust model**: operator-minted keypair provisioned as configuration. See
  M2 above; the two rejected models are recorded there.
- **Verification is configurable**: `pinned` by default, `unverified` as a
  documented escape hatch, because no CA-issued replica certificate is
  obtainable in this deployment.

Still open:

1. **Who owns the eight untracked serve services?** They have no envelope, so
   transport is their only protection, and no CI signal when they break. This
   gates any global enablement.
2. **What terminates TLS on a replica?** `sky/serve/local_async_router.py`
   already fronts `:8080` on multi-GPU replicas and uses aiohttp's
   `web.run_app`, which accepts `ssl_context=`. Routing single-server replicas
   through it too would give one fork-owned TLS terminator for every VM replica
   with **no model-image change** -- likely cheaper than adding stunnel/nginx
   to the fleet YAML. Needs a decision before M2 ships.
3. **Should the Kubernetes tier be exempt from M2?** Its hop stays inside VPC
   peering with a restricted node security group, and exempting it avoids
   touching the in-pod path at all.
4. **Is `--policy=upsert-only` acceptable for per-service DNS?** ExternalDNS
   never deletes, so a torn-down and recreated service leaves a record pointing
   at a dead NLB -- a silent blackhole.
5. **Does "HTTPS everywhere" extend to intra-cluster credential-bearing
   traffic** (LB -> controller bearer over plain HTTP in-cluster)?
6. **Does the sync inference path use the encrypted envelope?** The fleet YAML
   guard language is async-specific. If sync is plaintext, the risk framing
   above changes materially.
