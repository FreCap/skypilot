# Multi-pool SkyServe reserved-capacity fill

Last updated: 2026-08-20

Status: source integration is complete through PR #1613 and production
qualification is in progress. PRs #1604 and #1607--#1613 are merged. Production
Helm revision 465 runs release `1.1.1390` on the exact six-writer image digest
`sha256:8560dafc8be27460fec4d6b4905cdea7579ce0b82ff006f68f6a97117a848091`;
two API, two controller, and two executor Pods are Ready with zero restarts.
Reconciliation is `SEQUENCED_ACTIVE` at generation 6 and the service uses
`DURABLE_INTENT`. Fresh broker rounds observe 163 free PHX H200s, 18 free East
A100-80GBs, and two free East A100s. They currently publish new-work feeds of
160, 12, and two because nine exact provider-present associations still debit
three H200 and six A100-80GB slots. The service is therefore `FAILED` with
0/43 routable replicas even though Kueue and both physical pools are healthy.
This change supplies the narrowly fenced reconciliation needed to settle those
nine rows; it is not yet merged or deployed. Platform PR #8652 is merged and
does not pin the SkyPilot runtime.

Projected-worker runtime-readiness PR #1618 is merged at
`6ad2407d813d04aed79de2fea62723987ee56670` and published in release
`1.1.1394`; it is not yet deployed or production-proven. Draft cleanup PR
#1619 removes the rollout-only v1/v2/v3 projection readers after its separate
production gate. The change strengthens only canonical projection protocol v4
and adds no schema, EFS/RWX, KubeRay, Terraform, Terragrunt, or platform-pin
dependency.

The 2026-08-20 canonical-birth correction is source-implemented in PR #1621
and is not yet merged, deployed, activated, or production-proven. It makes a
fresh lifecycle-fenced PostgreSQL
non-pool service commit its service/version row with one complete generic
bound, durable-route, durable-demand, and durable-intent authority tuple tied
to one controller incarnation. Startup verifies that tuple instead of
promoting a fresh legacy row. This is schema-free and does not alter pools,
SQLite/local compatibility, or retained services. Consequently the existing
`boltz-l4-fleet` must either pass its explicit retained-row promotion or be
normally torn down and recreated after its effect/quiescence evidence settles;
recreation is the cleaner cut when service interruption is acceptable.

## Convergence goal and remaining work

The goal is one PostgreSQL-authoritative path that automatically assigns every
fresh, compatible, policy-authorized free GPU to the requesting service, then
computes paid residual capacity only after that reserved supply is committed.
For `boltz-l4-fleet`, the final policy intentionally backfills zero-cost GPUs
while traffic is idle. It must still scale to zero when no compatible free
capacity exists, and must never manufacture a paid floor.

The completed system has these invariants:

- one generation-fenced transaction admits the reserved-fill replica, binds
  its immutable association, creates its executable request and retention pin,
  and commits its durable actuation materialization;
- no provider effect is possible before that transaction commits, and a lost
  acknowledgement is recovered by exact tuple hydration rather than a second
  launch;
- reserved capacity is committed before the paid residual is calculated, so
  compatible reserved capacity suppresses new Spot launches and existing paid
  replicas drain as reserved replicas become healthy;
- controllers and adopters recover exclusively from PostgreSQL. Reserved-fill
  correctness, request observation, and takeover do not open or require an
  RWX/EFS launch log;
- every fresh lifecycle-fenced PostgreSQL non-pool service is born on the same
  canonical authority tuple; it has no committed legacy/direct interval and
  performs no controller-startup promotion;
- each physical pool converges independently, stale or ambiguous historical
  rows cannot block healthy pools, and UI request/capacity totals remain
  available during provider stalls;
- the active service has `min_replicas: 0`, zero fill floor, immutable
  server-owned worker projections, no task-owned Kubernetes overrides, and
  cannot publish a projected Kubernetes provider success until the exact Pod
  UID is marker-gated `Ready` after base SSH/environment/SkyPilot/Ray
  bootstrap.

Already implemented, merged, and deployed are the PostgreSQL-backed generic
non-pool binding, atomic replica/association/request/queue/pin/intent admission,
durable demand/route/actuation authorities, sequenced pool observations,
paid-residual accounting, takeover fences, and the provider-proof
single-flight receipt. PR #1610 owns atomic admission, #1611 the durable-owner
execution boundary, #1612 passive preflight, and #1613 receipt renewal across
launch waves. Production readback, not source state, establishes the live
authority facts above.

This recovery change is intentionally smaller than another state migration. It
reconstructs the exact frozen committed-intent profile for cleanup and accepts
only the enumerated historical HTTP digest representation. Feature PR:
[#1614](https://github.com/boltz-bio/skypilot/pull/1614). Its dependent
**historical-digest verifier cleanup PR**
[#1615](https://github.com/boltz-bio/skypilot/pull/1615) removes only that
verifier and its transition-only tests after all nine associations settle and
one complete stale/quiescence horizon remains at zero. The cleanup stays draft
until that gate is captured. This stack is distinct from the pre-existing
Serve056 owner-column cleanup described below.

Remaining work, in exact order:

1. Merge provider-present recovery PR #1614 while retaining its blocked draft
   cleanup PR #1615.
2. Direct-Helm deploy the recovery fix's immutable image/chart with
   `--reuse-values`; do not update a `boltz-platform` pin.
3. Prove all nine provider-present rows enter UID-fenced teardown, reach fresh
   provider `ABSENT`, release their pins/debits, and disappear from live
   replica totals. The broker's debit gap must fall from three H200 and six
   A100-80GB slots to zero.
4. Observe the first post-cleanup H200 Workloads and East Pods. Kueue admission
   must remain immediate; classify any later failure as scheduling, bootstrap,
   readiness, or application health from exact Pod evidence.
5. Deploy the protocol-v4 projected-worker readiness boundary before declaring
   convergence production-proven: provider success requires the same Pod UID
   to be Ready after SSH/environment/SkyPilot/Ray bootstrap, while old binaries
   reject v4 before provider I/O.
6. Reapply the merged #8652 production-only service update, retaining
   `min_replicas: 0`, full zero-cost backfill, server-owned Kubernetes
   projection, and no runtime pin.
7. Prove automatic backfill across every compatible free pool and a typed paid
   residual for every uncovered slot. The production performance gate is
   80--90 H200 workloads submitted within a few minutes with concurrent
   initialization and cross-pool independence. Also prove no new paid launch
   when reserved supply covers demand, paid drain/termination, exact request UI
   totals, and no phantom historical capacity immediately, at +10, at +30, and
   through the full 180-second stale/quiescence horizon, including child and
   Pod takeover.
8. Only after the full horizon may the dependent cleanup merge and remove its
   transition-only code/tests.

Explicit exclusions: EFS/RWX is neither authority nor a correctness fallback;
KubeRay is not part of this Serve path; no Terraform or Terragrunt expansion is
required; and no `boltz-platform` SkyPilot runtime pin is added. Deployment is
fix-forward through the SkyPilot Helm release, while #8652 is used only for the
service configuration update it owns.

Completion means the code and both PRs are merged, the exact images are live,
the final authorities are durably active, `boltz-l4-fleet` automatically
materializes 100% of fresh compatible free capacity, paid residual and drain
behavior are correct, dashboard request totals are fresh and non-null, restart
takeover succeeds without RWX/EFS, and every immediate/+10/+30/full-horizon
production gate passes. Source-complete, deployed, activated, and
production-proven are distinct states; this effort is not complete until all
four are true.

The remaining production launch wave exposed two steady-state gates. First,
many simultaneous terminal launch authorizations independently repeat the
same live AWS and Kubernetes proof under separate five-second deadlines.
Isolated full-fleet proofs complete in about three seconds, but 15 or more
parallel launch proofs contend and intermittently reject the complete claim
heartbeat. An early PR #1604 iteration used a policy-instance-local in-flight
map and was replaced because every production launch executes in a separate
`DisposableExecutor` child process and the policy instance resets after fork.
The final PostgreSQL-only Serve054 provider-proof receipt from PR #1604 is
merged at `5d473147dfbaecead6b1501f923f47abf58adfe5` after three fresh
exact-head CI passes. One advisory-lock owner publishes a safe, exact
context-wide AWS/Kubernetes summary for `(policy identity, gate generation,
context)` and independent processes may reuse it only inside the existing
five-second authorization-freshness horizon. Exact launch scope is still
validated per caller and the single receipt row is revalidated on the database
clock immediately before the provider effect. The executable policy contract
advances deliberately to revision `1.1.1386`. The source merge/CI gate is
complete; direct-Helm deployment, live readback, and cross-Pod production
qualification remain open. Second, provider effects can
still race the moving allocation fence before the generalized
request/association receipt is durably bound, producing new ambiguous capacity
debits and exposing the RWX stale-file-handle dependency. The required final
path is the already-planned atomic PostgreSQL
replica/association/request admission followed by asynchronous provider
actuation; neither more retries nor EFS is authority.

The first atomic launch wave on release `1.1.1389` then exposed a receipt
renewal race inside that single-flight boundary. Replicas 55746--55755 proved
that the v2 request can reach real Kubernetes Pods, while sibling launches
failed either during policy authorization or at the final reclaim-authority
check with no gate-generation change. The Serve054 row was replacing its
random nonce on every five-second refresh, so an unchanged provider proof could
revoke a concurrently minted exact reference between policy return and the
terminal guard. A cached receipt was also reusable until the last instant of
its five-second horizon, leaving no time for the terminal PostgreSQL check. The
fix-forward below keeps the nonce for an identical canonical proof renewal,
rotates it for every schema or proof-content change, uses one nonblocking
READ-COMMITTED MVCC statement as the terminal linearization point, and
reserves the final 0.5 seconds for the terminal guard using live local elapsed
time at actual handoff. The shared fleet gate is acquired before that
five-second ticket is minted, so an unbounded gate wait cannot consume the
reserve. This changes neither the five-second maximum age nor any gate,
service, projection, physical-cluster, or provider-proof predicate.

Historical rollout record: the additive reserved-fill, exact worker-projection, generalized
non-pool binding, demand, route, executor-termination, provider-independent
route, durable actuation-intent, supply-aware paid-residual, successor-schema,
and scheduler-mode prerequisites are merged through PRs #1537, #1540, #1542,
#1547, #1548, #1549, #1552, #1553, and #1555. Production Helm revision 436 /
release `1.1.1359` runs the exact 2 API / 2 controller / 2 executor split-role
cohort on RWX storage. Production committed clean successor version 63 on
2026-08-19. Before platform PR #8652 merged, one deployment attempt durably
stored an equivalent version 64 after API acceptance but failed before
election. The merged #8652 deployment workflow has not subsequently completed,
and version 64 was not activated or elected at the last observation. Its
post-merge combined run `32281262288` validated and dry-ran successfully, then
failed closed in the separate test target because `boltz-l4-fleet-test` does
not exist and cannot be bootstrapped without an atomic Platform endpoint
update; it made no production service change. After the controller rollout,
the existing prod-only `ml-model-deploy.yml` dispatch with `models=boltz-2`,
`mode=prod`, and `provider=skypilot` targets only `boltz-l4-fleet` and remains
the canonical application path. Fresh live readback remains required. The
stored version 64 has the same three worker
projections and `utilization_gate: false`. Historical exact database readback
proves its three non-null worker
projections include a PHX H200 projection that uses
`default-scheduler`, `be`/`be-ls`, priority -1000/`Never`,
`skypilot-pool-sa`, and no Pod Identity role; both east projections retain
`gpu-binpack-scheduler`. Version 63 has `min_replicas: 0`, a zero fill floor,
`utilization_gate: true`, and the known-good `v3.682.2-boltz-2` image. Version
64 changes only that gate. It
supersedes version 62, which proved the projection shape but remains ineligible
because it pins the rejected `v5.44.1-boltz-2` image. At the 2026-08-19
inspection compatible pools published free A100, A100-80GB, and H200 GPUs but
the stored version 64 had not admitted them. The last-observed reconciliation
gate is
`SEQUENCED_ACTIVE` generation 1, authorized to the unchanged policy contract
introduced in release `1.1.1358`; the release `1.1.1359` overlay incorrectly
derived policy authority from its unrelated artifact version. That prevented
the broker from refreshing version 63's claim to stored version 64, while the
canonical activation command correctly rejected the stale claim. The clean
fix is to keep artifact versioning independent from the explicit policy
contract revision, deploy the complete writer cohort, let the unchanged
authorized policy refresh the current claim, and invoke the same generation-
fenced activation command to bind the successor writer receipt. After
durable intent activation, production intentionally changes only that clean
successor's `utilization_gate` to `false`: the service backfills every fresh,
authenticated, policy-compatible reclaimable zero-cost GPU granted to it even
while traffic is idle, while
`min_replicas: 0`, `floor_replicas: 0`, and the zero-cost-only intent contract
continue to forbid a paid floor. Test remains utilization-gated. Platform PR
#8652 records and validates that production/test distinction without changing
Terragrunt, runtime pins, images, Kueue, or Helm. Adversarial review found
that the separately exposed demand and actuation promotions leave an
observable intermediate authority pair. PR #1555 closed that cutover race with
one atomic transition and is deployed dark in revision 431. The exact
post-horizon removal of both separate transition surfaces remains stacked as
draft PR #1556.

Post-deployment takeover review found one remaining ownership gap in that
atomic pair. A replacement controller incarnation already adopts generalized
launch authority, revokes route leases, and later re-advertises zero-cost
actuation, but the promoted `demand_authority_controller_incarnation` remained
bound to the predecessor. A `DURABLE_FEED` service consequently failed closed
forever after any child or pod takeover. The fix-forward correction makes the
existing fenced service-owner transfer also rebind the demand and zero-cost
capability advertisements in the same PostgreSQL transaction, without changing
either one-way mode or epoch. It terminalizes only unmaterialized
grant-before-row intents from the predecessor; committed intents and their
replica rows remain durable. Route authority is intentionally left cold, so no
demand, reserved-fill, or paid actuation resumes until the replacement publishes
its own route generation and the load balancer replaces the stale report with a
fresh complete report naming that generation and digest.
Grant admission additionally requires the calling manager's exact durable
controller incarnation and owner epoch under the same locked service-row
transaction. This fences predecessor in-memory plans even if the operating
system reuses the same PID/IP/port transport fingerprint.

Revision 431's first atomic-activation preflight found one unrelated retained
claimant, `opendde-10c200s-v4`, whose old version 4 carried null worker
projections. A normal service update committed version 6, removed its two
task-owned Kubernetes candidates and reserved-fill policy, retained its 36
cloud candidates, and let the owner-fenced poller withdraw its authoritative
claim. It remains correctly scaled to zero. The next preflight exposed a
deployment-policy bug: activation validates the global claim scope containing
both `boltz-l4-fleet` and `boltz-l4-fleet-test`, but reused a single-service
duplicate-atom set across the whole scope. It therefore rejected the intended
case where two services share the same physical pool/card. The corrected
contract validates duplicate `(physical_cluster_uid, exact_card)` atoms within
each service while allowing the broker's documented cross-service sharing;
provider attestation still runs once for the whole immutable fleet.

`boltz-l4-fleet-test` was briefly found at scale zero but is not dormant: its
durable load-balancer report showed real demand during the preflight. A
successor version 67 restored its exact reserved-fill policy after a version 66
diagnostic update. No ready replica was removed by those updates. While the
sequenced gate remains inactive, its legacy controller correctly skips typed
protocol-v2 fill and can still request paid Spot capacity for that demand; the
observed Spot attempts failed provisioning. This is further evidence that the
activation policy correction and final sequenced cutover, rather than removing
the test claimant, are required for cost convergence.

The first revision-423 activation attempt failed closed before durable
mutation. The common typed reclaim view required non-null Kueue admission for
every strict worker projection even though schema v4 intentionally represents
east with `kueue_admission: null` and an attested `gpu-binpack-scheduler`.
That made the global claim scope impossible to construct for an east claim.
The steady-state contract below now closes that representation gap with one
explicit `ReclaimAdmissionMode`: `KUBERNETES_SCHEDULER` requires both queue
identities to be null, while `KUEUE` requires both to be nonempty. The Boltz
policy derives the expected mode from its immutable bundle and rejects any
mode or payload mismatch before provider reads.

Production qualification on 2026-08-17 found two fill-boundary failures.
First, attempts 53925--53933 created speculative H200 replica/request rows
before the physical-pool actuation lane was owned; they later converged to
`FAILED_PROVISION`. P2d in `skyserve-demand-capacity-convergence.md` now
supersedes that boundary with a durable grant-before-row actuation intent and
per-physical-pool executor; it merged in PR #1537 and is deployed dark in
revision 418. Second, every Pod was rejected from
`rescluster-k8s-prod-east1-preemptible-inference` because the launch omitted its
required Kueue queue label. The live PHX contract is LocalQueue `be`,
ClusterQueue `skypilot-be`, `mt_hybrid` WorkloadPriorityClass `be-ls`, service
account `skypilot-pool-sa`, and Pod PriorityClass
`rescluster-k8s-prod-east1-preemptible-inference-low`. Revision 407's embedded
fleet bundle still contains the retired `default`,
`skyserve-inference-borrowed`, `skyserve-inference-low`, and
`skypilot-inference-sa` identities. Bundle schema v3 and the corrected
queue/service-account identities merged in PR #1529 and are deployed, but
their PHX scheduler contract is obsolete. A 2026-08-18 audit proved that PHX
intentionally removed `gpu-binpack-scheduler` in platform PR #8527 after
enabling Kueue topology-aware scheduling in #8524 and removing the custom
scheduler pin in #8526. Reinstalling it would create a second placement
authority and is explicitly rejected. The next policy schema instead binds
PHX to `default-scheduler` plus the exact Kueue v0.19 TAS feature-gate and
ResourceFlavor topology contract; east retains its existing attested custom
scheduler.

The exact per-spoke audit IAM roles, EKS access entries, and read-only RBAC are
deployed from the already-pinned Terraform module. The exact east inference
namespace/service-account no longer owns stale Pod Identity association
`a-rsvzwdtaesxvxorkh`; the reviewed worker projection remains identity-free.
Revision 428 proves both AWS role assumptions and both Kubernetes API
authentications use the same bounded audit sessions. That successful
authentication exposed the remaining object-contract blockers: PHX still
inherits rather than explicitly pins `AssignQueueLabelsForPods: true`, and
east's provider-owned `ml.p4d.24xlarge` and `ml.p4de.24xlarge`
ResourceFlavors both carry `topologyName: hyperpod` while the embedded
inventory still expects null. The clean correction pins the PHX gate in the
owning platform module and binds the live east topology in the immutable
bundle. It does not enable Kueue admission in east or create a second
scheduler.

The steady-state authentication correction uses the same short-lived assumed
audit-role session for both EKS inventory and Kubernetes API reads. It obtains
the exact active cluster endpoint and CA from EKS, signs one bounded EKS bearer
token with the assumed credentials, constructs an isolated Kubernetes client
from an in-memory kubeconfig document, verifies the `kube-system` UID before
the object-read batch, and scrubs/closes that client after the proof. It never mutates the ambient
kubeconfig, never grants audit reads to the ordinary writer identity, and
never caches provider observations or bearer tokens.

Revision 426 deployed the isolated audit-role Kubernetes client and the exact
east association is now absent. Its first two-context preflight failed closed
without durable activation because the bounded audit session exposes botocore
`ReadOnlyCredentials`. Revision 427 avoided calling
`get_frozen_credentials()` in policy code, but production traceback proved
that `RequestSigner` itself requires and freezes a credential-provider object;
passing the read-only tuple through therefore preserved the same failure. Both
AWS inventory/identity proofs passed and no durable gate changed. The corrected
fix-forward signer passes normal refreshable `Credentials` through unchanged
and reconstructs an immutable `Credentials` provider from the bounded
read-only tuple. It neither refreshes nor selects a new identity source, and
tests cover both shapes at the exact signer seam.

Revision 428's isolated audit client reached both Kubernetes validators. PHX's
live Kueue v0.19 configuration relies on the upstream default for
`AssignQueueLabelsForPods`; the attested contract deliberately requires the
gate to be explicit. The platform correction pins it `true` independently of
TAS rather than weakening attestation or installing a second scheduler. East
initially failed only because its two provider-owned ResourceFlavors expose the
same `hyperpod` topology that the immutable inventory had left null. PR #1553
bound that exact topology and production revision 429 now passes the complete
east AWS and Kubernetes proof. Platform PRs #8631 and #8638 published the PHX
gate as immutable external bundle `v5.57.2`; PR #8639 merged the PHX-only
environment pin. The fresh two-context preflight now fails only on the still
unapplied live PHX gate: PHX AWS identity passes, while Kubernetes reports
`Kueue Pod integration or a required feature gate is disabled`.

At 2026-08-18 01:01 UTC the service had real demand (queue depth 325,
confirmed in-flight 106, and 98 recent requests in 60 seconds). Paid Spot
launches were therefore not phantom traffic: version 58's legacy controller
computed a paid residual before any reserved PHX/east supply could be durably
committed. The PR #1542 guard is deliberately narrower and cannot correct a
service that has not committed projections and promoted the durable reserved
path. The long-term correction is the one canonical sequenced path below, not
a broader legacy heuristic.

At 2026-08-18 14:06 UTC the durable protocol-v2 LB report was fresh and
complete with 21 asynchronous predictions processing, zero HTTP in-flight,
zero queued, zero recent rejected, and 368 completed predictions in the prior
ten minutes. The controller nevertheless reported `in_flight_total: null`
because the service still selects `LEGACY_CONTROLLER` demand. Route ownership
is already `DURABLE_PROJECTED` at epoch 1; it is not a remaining promotion.
This exact split explains the dashboard gap and makes an artificial
pre-promotion H200 replica the wrong gate. The clean order is: apply and attest
the worker contract, promote bound launch authority to generic, activate
sequenced reconciliation, atomically promote the durable demand report and
durable grant-before-row actuation, apply the gate-false successor, and prove
the first automatically generated zero-demand H200 backfill admission on that
final path. A temporary replica floor or direct-fill canary
would test the path being removed. Permanent production reserved backfill is
instead an explicit service policy applied only after durable intent authority
is active; it must never be approximated with `min_replicas`, a positive fill
floor, synthetic traffic, or legacy direct launch.

The atomic promotion is required, not merely operator convenience. The current
separate controller transitions each hold the actuation generation odd only
for their own request. Durable demand is a prerequisite of durable actuation,
so invoking the two endpoints in order exposes an even-generation interval in
which a reconciliation tick can observe `DURABLE_FEED` with
`DIRECT_REPLICA`. Zero-cost-first planning prevents paid spill in that interval,
but a direct replica can still become the first H200 admission and preserve the
transition path as a second happy path. The steady-state endpoint must hold one
odd actuation generation and the routing-state linearization lock while one
PostgreSQL transaction validates both capability barriers and advances both
epochs. Only after that transaction commits may the controller install the
durable manager mode and release reconciliation. A post-commit in-memory
installation failure permanently fences that child and delegates recovery to
the supervisor; restart reads the already-atomic durable modes. The separate
promotion endpoints are transition-only compatibility surfaces, are deprecated
for activation, and are removed in the stacked cleanup after the production
horizon.

The stale demand selector is also the immediate blocker for an existing paid
retirement wave, not only a dashboard presentation bug. At 2026-08-18 14:19
UTC, all 151 Spot rows were still `SHUTTING_DOWN` with
`sky_down_status=SCHEDULED`, `wait_for_idle_before_termination=true`, and an
uncommitted logical-retirement fence despite being 12.6--32.7 hours old and
having a 3,900-second drain cap. A read-only provider query through each
server-owned workspace proved that 116 GCP L4 instances were still `UP` (106
in `asia-northeast3`, 10 in `us-east4`); 35 GCP targets and the sole AWS target
were already absent. The controller reports the exact cause every 120 seconds:
the legacy autoscaler signal is stale, its exact-card target is incomplete, and
logical-retirement recovery therefore has no fresh coherent target/capacity
snapshot from which to commit teardown. This is correct fail-closed behavior
for ambiguous demand, but it makes durable-demand promotion a cost-critical
production gate. After promotion, recovery must re-fence the old-version
retirements under a newer complete snapshot, retain only the capacity needed
for the authenticated target, and drive ordinary `sky.down` for the rest.
Provider absence, not row deletion or cached SkyPilot status, is the completion
proof. If the promoted feed does not produce that snapshot, fix the canonical
durable snapshot/recovery path before any manual cleanup.

The 2026-08-18 follow-up code audit found exactly that promotion blocker in
the current durable path. `_reconcile_scale_once()` collects the complete
PostgreSQL demand report into `ConcurrencyAutoscaler`, but only the legacy LB
sync path publishes the matching capacity/occupancy snapshot to
`ReplicaManager`. Durable promotion by itself would therefore leave logical
retirement recovery without its second fence. The canonical bridge has these
invariants:

- under the same `_routing_state_lock` that ingests a validated durable report,
  treat `DurableAutoscalingSnapshot.demand_feed_generation` as the authority,
  require the embedded request-information generation and the autoscaler's
  collected-generation echo to equal it, and stage (but do not expose) a
  snapshot at that exact generation `N` with the observed-slot, in-flight, and
  unknown-replica maps;
- only after planning returns a valid logical target, rollout failure is
  excluded, and the actuation, notification, and demand generations are still
  current may the controller reacquire that routing lock and publish. One
  `ReplicaManager` operation installs target `N` and snapshot `N` through a
  single immutable reconcile-state reference under `_logical_state_lock`.
  Every manager fence captures that reference once, including scale-up's
  initial and per-launch checks, so readers see the complete old publication or
  the complete new publication even for a same-generation replay or generation
  regression;
- plan failure, target invalidation, rollout failure, or generation mismatch
  publishes neither half. The controller-local `_reconcile_generation` remains
  only an optimistic in-process race fence and never stamps durable evidence;
- replay of feed generation `N` republishes `N`, never synthetic `N + 1`.
  Recovery may adopt/re-fence an old-controller retirement at `N`, but only a
  genuinely newer durable report can provide the strict `N + 1` release.

The initial bridge published evidence only. Exact-head review of the existing
logical-retirement consumer then found that process-local freshness and the
ordinary replica-row write boundary were insufficient for irreversible
teardown: a generation could advance after the controller read its evidence but
before the queued worker was admitted. PR #1561 therefore narrows the existing
logical teardown admission to one PostgreSQL commit seam; it does not add a
second scale-down or provider-cleanup path. Its contract is:

- `get_autoscaling_snapshot()` reads the service, demand generation, exact
  report rows, route head, and immutable route snapshot in one explicit
  PostgreSQL `REPEATABLE READ`, `READ ONLY` transaction. A generation-`N` read
  can never be paired with report rows from `N + 1`;
- that read mints one immutable authority token containing the exact service
  lifecycle/version and controller owner, demand source epoch/generation and
  receipt watermark, route source/head/digest, HA slot/cutover authority,
  fresh-zero bit, and selected occupancy-sample URLs. Its deadline is computed
  once at read start as the minimum remaining route-head TTL, report TTL, and
  selected occupancy-sample lifetime after the reporters' supplied sample
  ages. The controller and manager carry that same deadline; no receive,
  publication, or retry timestamp refreshes it. A stale, incomplete, missing,
  or failed PostgreSQL read revokes both the manager target and snapshot;
- destructive admission requires a genuinely newer feed generation than the
  retirement's reversible selection generation. Under the established global
  SQL order, the transaction locks the zero-cost protocol singleton, then the
  durable lifecycle fence, service row, and exact replica row. The lifecycle
  fence precedes the service row exactly as it does during lifecycle takeover,
  preventing a retirement/takeover lock inversion. The service row is the
  shared service-local mutex with demand ingestion: if `N + 1` ingestion
  commits first, the old token is rejected; if retirement holds the row first,
  its commit is ordered before that report. No participant acquires the
  protocol row after a lifecycle or service row, preserving the common
  `protocol -> lifecycle -> service -> replica` order used by admission,
  materialization, handoff, takeover, and fill;
- while holding those locks, the commit revalidates the exact service and
  controller owner, source/feed/receipt tuple, route head and immutable digest,
  HA authority, selected fresh occupancy set, database-clock expiry, and
  replica record/precommit fence. It atomically records the confirmed
  generation, `logical_retirement_committed=true`, `sky_down_status=RUNNING`,
  and exact route-lease revocation. Only an unambiguous commit result may start
  the already-queued worker;
- a rejected transaction revokes manager authority and starts no worker. A
  failed commit call is `AMBIGUOUS`: the original worker is discarded and only
  an exact durable row readback may reconstruct cleanup. A committed row is
  detached into ordinary idempotent cleanup; an unchanged reversible row is
  requeued behind the same later authority seam. If the controller crashes
  before that process-local readback, startup recognizes the same exact
  admission-precommit shape, re-fences it under fresh generation `N` as the
  canonical strict-idle reversible precommit, and waits for genuine `N + 1`
  plus fresh idle proof before requeue. Malformed state remains off-route for
  inspection; and
- a pre-commit-bit legacy ambiguous row is never grandfathered into teardown.
  Fresh durable generation `N` first normalizes it to a current-controller,
  `committed=false`, strict-idle reversible precommit and starts no worker. A
  lost normalization acknowledgement is resolved by exact readback of that
  same reversible shape. Only a genuine `N + 1` can then traverse the normal
  PostgreSQL commit seam above.

Real-PostgreSQL tests force the generation/read interleaving, assert the
canonical protocol/lifecycle/service/replica SQL lock order, execute both
report-first and retirement-first service-row orderings, force the former
retirement/lifecycle-takeover lock-cycle interleaving, and inject a
commit-lost-ack. Manager tests prove the original worker never starts on
ambiguity, a restart recovers the exact admission-precommit row only behind
fresh `N + 1`, and legacy normalization, including its lost-ack readback,
still requires strict `N + 1`. The change requires no schema, Helm, Terragrunt,
EFS, or alternate storage path.

A read-only production audit at revision 432 found all seven live services
still on `LEGACY_CONTROLLER` plus `DIRECT_REPLICA`, no live row matching the
legacy ambiguous precommit shape, and all 151 `boltz-l4-fleet` logical
retirement rows explicitly uncommitted. The exact-head code is therefore dark
until the documented one-way authority promotion; it is generic migration and
restart safety, not authorization for manual row deletion or immediate
provider teardown.

The single-operation coherence guarantee above is specifically the durable
promotion bridge. Before promotion, legacy LB ingestion still publishes a
capacity snapshot and the later reconcile publishes its target as two
standalone operations. That path increments one process-local generation for
every accepted report, so it cannot legitimately replay a capacity half at the
same generation; `ReplicaManager` rejects duplicate/regressed standalone
snapshots and regressed standalone targets. A target at generation `N` may
still coexist intentionally with a newer legacy capacity snapshot `N + 1`:
newer capacity is stronger evidence, and the next generation-fenced reconcile
will replace the target. Each legacy operation also replaces one immutable
state reference, so readers never tear a target or snapshot object while that
intentional cross-generation state is visible.

Closed cleanup PR #1506 contained a superficially similar hunk that derived
`next_demand_generation = self._reconcile_generation + 1`. That is unsafe: the
live durable feed was already at generation 68023 while a restarted controller
counter begins at zero, and replay would manufacture false new evidence. This
focused fix superseded that controller-local-generation hunk. PRs #1506 and
#1510 are closed and superseded; they reserve no API or Serve migration number.
Any later cleanup must be re-derived from observed old-path use after the
production horizon and receive then-current heads only when its code exists.

An activation preflight on 2026-08-18 also found that the mechanical
transition still required the historical *exact* Serve047/API011 revisions,
so it rejected the valid deployed Serve052/API015 successor heads. The same
documented `python -m` command did not select server context or load the
deployment reclaim-policy plugin. The transition now requires the exact
heads supported by its deployed binary, verifies that those numeric linear
heads retain the Serve047/API011 prerequisites, and initializes the server
MAIN plugin context itself. This preserves the no-divergent-head invariant
without pinning activation forever to one historical schema pair.

All platform-pin and Terragrunt deployment sequences retained later in this
file are historical review records, not current gates or deployment authority.
The live Helm release is authoritative. Merged SkyPilot artifacts are deployed
directly with `--reuse-values`; no `boltz-platform` runtime pin is created or
updated.

Last updated: 2026-08-19 (production full reserved-backfill policy,
policy-bundle schema v5 and strict direct-Pod lifecycle authority,
release-1.1.1355 authority audit, PR #1561 durable logical-retirement
contract, and #1562 controller-takeover capacity-authority/grant fencing)

Canonical owner: this file

Rollout policy: generation-fenced fix forward, no capacity-consuming canary,
and no supported demotion after fleet activation

## Decision summary

The steady-state reserved-fill path is:

```text
concurrent physical-pool observation
  -> immutable PostgreSQL observation generations
  -> immutable per-version worker/Kueue projection and canonical digest
  -> deployment-policy authorization of the exact projected claim set
  -> broker round with exact observation provenance
  -> one authenticated service-wide allocation map
  -> the ordinary autoscaler reconciliation coordinator
  -> pure typed fill plan
  -> locked durable zero-cost actuation-intent admission
  -> one per-physical-pool executor owns its PostgreSQL lane and provider fence
  -> atomic replica-row, typed launch association, API request, queue, and
     retention-pin acceptance
  -> exact committed/deferred receipt carrying intent/association/request IDs
  -> exact projection revalidation and deployment-policy authorization at the
     terminal provider boundary
  -> one-way Pod materialization boundary and fresh authority around every
     bounded post-Pod runtime or workload effect
  -> existing asynchronous launch/provider path through the generic non-pool
     request handler
```

The target correction removes three root causes now proven in production:

1. Physical-capacity reads no longer wait behind slow replica actuation while
   their conservative freshness timestamp expires.
2. Planned fill capacity is counted only after an unexpired durable actuation
   intent commits; replica/action rows are created only after the exact pool
   lane and provider fence are owned.
3. Worker admission configuration and the deployment-policy bundle are one
   exact release contract, so a stale queue, workload priority, service
   account, or cluster attestation fails before replica materialization.

The actuation-intent insert is the capacity-admission ledger, not passive
recording. Under service/allocation locks it re-parses the authenticated map,
rejects idempotency replay, recomputes the durable service ceiling, and spends
one physical pool/card slot for a bounded interval. A delayed or concurrent
caller therefore cannot oversubscribe capacity that an in-memory planner once
observed. The later replica/association/request transaction transfers that
debit from intent to durable replica without double counting.

One PostgreSQL selector changes the whole writer fleet from the compatibility
path to the sequenced path. The transition is one way. After activation, an
unavailable or invalid allocation map withholds new fill; it never falls back
to the old speculative launch path. Ordinary demand reconciliation continues.

This is a fix-forward deployment. We do not run a GPU, service, or BCL canary
and do not retain a second operator-selectable happy path. A problem after
activation is repaired by deploying a successor image and authorizing one new
gate generation through the same command and transaction used for first
activation. The prior generation then fails closed.

The remaining rollout is one SkyPilot fix-forward stack deployed through the
existing direct-Helm split-role topology:

1. Schema-v4 PHX `be`/`skypilot-be`/`be-ls`, `skypilot-pool-sa`,
   `default-scheduler`, Kueue v0.19 TAS, H200 ResourceFlavor topology, and the
   exact per-spoke audit roles/RBAC are deployed. Merge and publish the
   successor policy fix that uses those audit roles for Kubernetes as well as
   AWS, remove the stale east inference Pod Identity association, and render a
   complete direct Helm upgrade with the corrected `mt_hybrid` context
   configuration. Use `--reuse-values`, and prove the API/LB image, ConfigMap,
   and successful two-context preflight are the reviewed tuple. No service
   authority is promoted by this binary/config deployment. The schema-v4 proof
   wire format remains version 2; both successful and failed preflight payloads
   use that version. The Boltz worker startup
   consumes the projected cache and scratch environment before any R2 access:
   it verifies exact mount target, anchored source, filesystem type, total and
   free byte/inode budgets, and `/tmp` as `tmpfs`. Merely projecting these
   fields is not qualification evidence.
2. Version 63 is the committed, elected, and controller-applied clean
   demand-gated successor. It retains the active known-good
   `v3.682.2-boltz-2` image and version 62's corrected effective config,
   `min_replicas: 0`, and exact non-null worker projections, but does not
   inherit version 62's rejected `v5.44.1` pin. With zero demand and
   `utilization_gate: true`, verify the deployed P2d path remains dark and every
   activation barrier is fresh. Promote bound launch authority to generic,
   freshly verify and, if required, generation-fenced reauthorize the
   last-recorded sequenced reconciliation gate, and atomically promote durable
   demand plus durable intent actuation. No configuration change may enable full
   backfill while legacy direct launch owns actuation.
3. Commit one more immutable service version by changing only production
   `reserved_capacity_fill.utilization_gate` from `true` to `false` relative
   to that clean successor. Preserve its exact model/runtime image and
   non-null worker projections. Fresh authenticated allocation grants derived
   from policy-compatible per-pool observations must automatically produce
   zero-cost intents and correctly admitted Pods without synthetic traffic.
   Prove the expected queue, ClusterQueue, workload priority, Pod
   priority, service account, default scheduler, TAS assignment, accelerator,
   and physical cluster identity; no paid compute may satisfy this gate.
4. Author and cross-link the required Serve056 cleanup draft before the feature
   merges. Only after the transition horizon may it merge. It removes the
   nullable owner-attestation transition, not the generalized non-pool path or
   the closed historical cleanup stacks. The offline migration is forward-only
   and is repaired by another schema-compatible Helm fix-forward if it fails
   after commit.

Historical rollout snapshot (not current live readback): the then-live
production artifact was merge
`b34661c43015c05d5bb2a6358b1d9335fbd465f1`, release `1.1.1349`, image
digest
`sha256:07579af96b42de183b404d8cb23a6452598e59f22c7a9f29810694fbe2bf08d3`,
and chart package digest
`sha256:a56b6e1c2035e0ef2b0e560060b7005d74c19d8a66ed54e1a2b409cb1e619ad7`.
The then-reviewed deployment candidate was merge
`7d075e1d378f898814f21e89126842b387610491`, release `1.1.1352`, image
digest
`sha256:58439f4a84407ca7279a3922bdfffb5ccc9ba6b4c3cb3204dac821428e898bcb`,
and chart package digest
`sha256:c3b18ff024c152efaff185be59c2cf9c9ef17abb7dafb7918dc203aaee218fb0`.
Release `1.1.1332` remains compatibility evidence, not an activation
candidate for version 58, because that version has null projections and
legacy authority and its PHX policy bundle still names the retired scheduler.
Releases 1.1.1273/1.1.1277 and the A/B/C/platform nomenclature below
are retained only to explain already-merged transition contracts; they do not
authorize rollback, pinning, or another deployment path.

## Retained pre-activation contracts

The current release inherits the following pre-activation contracts from the
historical A/B/C work. They remain useful while the generation gate is closed,
but P2d and the exact policy/config correction above define the steady-state
activation boundary.

### Authenticated request-queue capacity

The authenticated `/_lb/capacity` response is the canonical admission contract
for the platform client. When `load_balancer.request_queue` is configured it
always includes:

| Field | Contract |
|---|---|
| `request_queue_capacity` | Dynamic waiting capacity derived from the current ready/logical fleet, bounded by configured minimum and maximum. |
| `request_queue_dispatch_limit` | Dynamic backend dispatch concurrency; zero while no usable backend capacity exists. |
| `request_queue_submission_limit` | Capacity-insensitive controller HTTP concurrency, exactly `max_size + max_concurrency`. This lets a cold service accept its configured backlog before its first worker is Ready. |
| `request_queue_min_size` | Immutable configured minimum waiting capacity. |
| `request_queue_size_per_replica` | Immutable configured waiting capacity per ready/logical replica unit. |
| `request_queue_max_size` | Immutable configured waiting-capacity ceiling. |
| `request_queue_max_concurrency` | Immutable configured active-dispatch ceiling. |
| `request_queue_max_request_body_bytes` | Immutable configured per-request body ceiling. |
| `request_queue_timeout_seconds` | Immutable configured queue-wait timeout. |
| `request_queue_uses_async_occupancy` | Immutable configured occupancy mode. |

The three admission fields are zero on a non-armed/non-active LB slot; the
immutable echoes remain present so a reader can diagnose role mismatch without
mistaking it for a different service contract. All queue fields are `null` only
when the queue is disabled. Presence and exact JSON types form the compatibility
boundary; this localized response extension does not add another schema/version
switch or alternate endpoint.

The PR 14 cold-start contract is exactly `min_size: 200`,
`size_per_replica: 10`, `max_size: 2000`, `max_concurrency: 128`,
`max_request_body_bytes: 1048576`, `timeout_seconds: 3600`, and
`use_async_occupancy: true`. Before any worker is Ready, the response therefore
reports queue capacity `200`, dispatch limit `0`, and submission limit `2128`.

Timeout ownership is intentionally layered rather than duplicated:

| Owner | Setting | Seconds | Invariant |
|---|---|---:|---|
| Platform node-local model/router render | `--request-timeout-seconds` | 315 | Returns before SkyServe's upstream stream deadline. |
| SkyPilot source/service spec | SkyServe LB `stream_timeout_seconds` | 330 | Covers the 315-second model/router request with margin. |
| Platform outbound SkyServe client config | request timeout | 3960 | Covers the 3600-second queue wait plus 330-second stream window and margin. |
| Platform generated-Service annotation | NLB TCP listener idle timeout | 4000 | Strictly exceeds the 3960-second client deadline. |

Source owns and supports the 330-second LB setting and the queue contract.
Platform owns the 315-, 3960-, and 4000-second rendered values. Neither side
silently derives or rewrites the other's values.

### Generic generated-Service annotations

The only operator input is the exact string map
`serve.externalLoadBalancer.serviceAnnotations`. Helm validates only that the
value is an object with string keys and values, serializes it deterministically
as JSON, and projects the same reserved environment variable into every
`api`, `controller`, and `executor` Pod. Python is the sole semantic authority:
startup and every reconciliation reject malformed Kubernetes annotation keys,
duplicate JSON keys, non-string values, and conflicts with SkyPilot-owned
`skypilot.co/` keys or the exact third-party-domain TLS, DNS, and backend-
protocol keys managed by SkyPilot.

Every generated inference `Service` receives the map. SkyPilot records only
those operator-owned keys in the canonical durable annotation
`skypilot.co/serve-lb-operator-annotation-keys`. On update it sets current owned
keys and emits strategic-merge `null` only for retired keys in that ledger;
unrelated annotations injected by AWS Load Balancer Controller, ExternalDNS,
or another provider/controller remain untouched. A malformed ledger fails
closed. During A's bounded PR 14-to-C transition, a missing ledger is accepted
as a bootstrap and is interpreted as owning zero existing keys. A cannot prove
from a markerless Kubernetes object whether it predates A or lost its ledger
after A, so it deliberately preserves every unmarked key in either case. A
post-A missing ledger is therefore observable drift that fails the PR 14/C
receipt gates even though A can still repair the marker without deleting an
unknown provider-owned key.

PR 14 must reconcile and read back every live generated inference Service with
a canonical ledger. That receipt is cleanup C's exact removal gate: Serve049
must physically remove markerless acceptance and reject a missing ledger while
retaining the ledger and narrow merge behavior as the permanent single path.
The cleanup absence tests must prove no `require_marker=False` call or
missing-marker bootstrap remains and that a missing live ledger fails closed;
this is not a TODO or optional soak gate.

The interface is provider-neutral. The platform's current AWS contract uses:

```yaml
serve:
  externalLoadBalancer:
    serviceAnnotations:
      service.beta.kubernetes.io/aws-load-balancer-listener-attributes.TCP-30001: tcp.idle_timeout.seconds=4000
```

The key shape and TCP idle-timeout range follow the official
[AWS Load Balancer Controller Service annotation contract](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/service/annotations/)
and [AWS Network Load Balancer listener behavior](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-listeners.html).

### Audit targets independent of Kueue object creation

`reserved_fill_reclaim_audit` is an audit identity and exact read target, not
the owner of partition Kueue objects. Its required
`local_queue_name` and `inference_cluster_queue_name` fields are therefore
available even when the selected `partition.kueue` is `null`. Terraform can
stage the audit role, EKS access entry, and exact Kubernetes RBAC before a
separately owned Kueue rollout. Once that partition enables Kueue, a lifecycle
precondition requires exact equality with its `local_queue_name` and
`cluster_queue_name`; there is no alias, fallback, inferred default, or second
source of queue identity.

This decoupling changes neither ordinary workspace placement nor its
credentials. The existing ordinary `wa` and `skypilot-wa` paths remain
unchanged. A's module tests must prove both the Kueue-null staging state and the
later exact-match rejection boundary.

The deployment policy must consume that identity rather than merely attest
that it exists. One role assumption supplies short-lived credentials to both
provider domains: EKS APIs prove the cluster and Pod Identity inventory, and a
separately constructed in-memory Kubernetes client proves the exact RBAC-bound
objects. The ordinary writer kubeconfig is not a fallback. If role assumption,
EKS endpoint/CA validation, token signing, physical UID validation, or any
exact-name read fails, the complete proof fails before activation.

Only frozen historical Alembic replay code remains after C; runtime code does
not import it. There is no rollback branch, legacy publisher, topology
re-creation, or second Helm/runtime happy path to preserve.

Code rollout and gate activation are separate operations. The image may be
rolled out while the gate remains `LEGACY_ACTIVE`. Activation is withheld if
the deployment cannot prove that inference and BCL/research workloads share a
Kueue preemption domain; Kubernetes Pod priority by itself is not that proof.

A final integration audit found that the first implementation bound the
server-owned Pod PriorityClass but still resolved the Kueue LocalQueue from
mutable launch-time configuration and copied the task-owned
`resources.priority_class` into Kueue's WorkloadPriorityClass label. Claim and
launch scopes also omitted the committed service version and worker-projection
digest. That split ownership could attest Pod priority `-1000/Never` while
submitting a differently prioritized Kueue Workload, and a service update could
reuse the old claim generation. The steady-state correction is worker
placement projection protocol v2: one immutable version record owns namespace,
service account, explicit Pod Identity role or explicit identity-free state,
Pod priority, accelerator scheduling, LocalQueue, and WorkloadPriorityClass.
Its canonical digest and service version are fenced through every durable
stage. No reserved-fill-specific parallel projection is introduced.

The production inference partition intentionally has no AWS Pod Identity
association. Protocol v2 therefore treats `pod_identity_role_arn: null` as a
closed, hash-bound negative identity contract, not as a missing projection.
Protocol v1 retains its historical non-null role requirement. The deployment
policy receives the nullable value in every typed projected admission and must
attest either the exact role association or its absence for the projected
namespace/service-account pair. This keeps identity-bearing and identity-free
partitions on the same canonical projection path without inventing a sentinel
role or a deployment-specific compatibility branch.

## Historical context and incident evidence

Multi-pool protocol v2 and its physical-cluster identity fences predate this
correction. Historical protocol work includes PR
[#1261](https://github.com/boltz-bio/skypilot/pull/1261), its draft protocol-v1
cleanup PR [#1263](https://github.com/boltz-bio/skypilot/pull/1263), and the
later detached-authority correction in PR #1440 (`c964b5480`). Those changes
made multiple physical Kubernetes pools representable and fail closed, but did
not solve the lock convoy or receipt-less partial admission described here.
The ordinary-launch prerequisites for this correction merged as
[#1434](https://github.com/boltz-bio/skypilot/pull/1434) and
[#1435](https://github.com/boltz-bio/skypilot/pull/1435).

Production inspection on 2026-08-11 found server `1.1.1243` at `2d2c67efb`.
The A100 pool remained underfilled while the protocol-v2 broker repeatedly
published 34 free slots. Those snapshots reached the autoscaler 181--250
seconds after their conservative source timestamps, beyond the existing
180-second authority horizon. Physical polling and slow manager/provider work
were serialized by `_actuation_epoch_lock`.

The same investigation found that
`Autoscaler._apply_reserved_capacity_fill_v2()` reduced its in-memory feed for
every decision it emitted, while `ReplicaManager.scale_up_batch()` could accept
only a prefix and returned no accepted-prefix receipt. A deferred tail was
therefore neither durable nor eligible for immediate replanning.

The broker's successful, UID-fenced publication of free A100 capacity is
positive evidence that the live identity-read permission and reserved-cluster
module deployment were not the underfill cause. RBAC drift should still be
reconciled declaratively, but neither a permission change nor a larger timeout
fixes these two defects.

### 2026-08-15 mixed-version launch incident correction

A separate production incident corrected the launch-boundary premise shared by
the first reserved-fill plan. An old mixed-version executor claimed requests
and caused real provider effects without the current process/quiescence receipt
protocol. Later cancellation/storage could show
`execution_generation = 0`, no PID or entrypoint, and
`execution_quiescence_required = false`. Those surviving fields do not prove
that no Pod or other provider effect was created.

Therefore reserved-fill durable row acceptance may not hand work to an unbound
special launch path. It joins the one generalized non-pool Serve association
defined by `durable-serve-replica-actions.md` and
`skyserve-resource-action-provider-facet.md`. The existing Serve042 association
is extended in place; no reserved-fill association, request queue, executor,
scheduler, or dual write is added. Its immutable `RESERVED_FILL/v1` profile
digest references the locked gate, allocation, claim, observation, exact
pool/physical-UID/card, intent, zero-cost admission sequence, committed service
version, worker-projection digest, and reclaim-policy identity/ticket. The
locked replica/profile rows remain the detailed authority; both atomic
admission and the terminal provider fence revalidate them.

`FillCommitResult` remains the planner/grant accounting receipt. Atomic
materialization returns a separate `AdmissionReceipt` naming the exact durable
replica, replica-record, association, API-request, and launch-generation IDs.
Those objects, the queue row, and retention pin commit atomically under the
existing demand/reserved-fill lock order. A row cannot first become accepted
and later acquire a binding. Receipt loss retries the same stable key and
returns the same identities; a payload, profile, authority, or digest mismatch
fails closed.

Recovery is typed per association. A legacy/mixed-version request is
`LEGACY_EFFECT_AMBIGUOUS` unless exact result and provider readback prove its
disposition; no migration, reducer, repair command, or operator may fabricate
quiescence or backfill a synthetic association. Provider timeout, partial
enumeration, RBAC denial, malformed identity, and same-name/new-UID mismatch are
`UNKNOWN`, not absence. While ambiguous, the row conservatively debits its
exact pool/card and blocks only that grant/successor. Physical observations,
route publication, autoscaling, and independent pools continue.

Only a request admitted entirely by the exact current generic handler,
`RESERVED_FILL/v1` profile, capability cohort, and receipt protocol may classify
generation zero plus no claim and `NOT_STARTED` as
`PRE_EFFECT_TERMINAL`. The immediate cancel-then-rediscover repair remains a
bounded mitigation and must preserve real-effect ambiguity.

### Launch digest and executable-request invariants

The generalized binding retains its established two-proof contract. The
canonical launch digest fingerprints the exact immutable submitted/prepared
body before authenticated identity fields and client API metadata are
normalized for execution. This is the contract implemented by the original
generic HTTP admission and must remain stable across a mixed API rollout. A
retry separately reconstructs the trusted normalized executable request;
under the association/request locks PostgreSQL requires its immutable payload,
handler, tenant, queue delivery, and profile columns to equal the already
persisted request. A changed submitted body therefore conflicts through the
association digest, while a changed authenticated principal or API
normalization conflicts through the persisted request. Neither proof is a
fallback for the other.

Provider-present cleanup parses the bound context from the locked request and
requires it to equal the locked association/context, in addition to the exact
profile, terminal generation, quiescence, queue/pin, provider evidence,
service-job, replica, and paid-claim checks. It also independently requires
`requests.user_id == body.USER_ID == association.tenant_scope` and both the
request-row and body cluster names to equal the association cluster. The bound
context omits those fields, so a digest match never replaces the relational
checks.

There are temporarily two closed digest modes for this cleanup transaction:

- `EXECUTABLE_EXACT` is the permanent atomic mode. The locked durable-body
  digest equals `input_digest`, and the body owner ID/name and association
  tenant equal the immutable locked service-owner tuple.
- `LEGACY_HTTP_NORMALIZED` is a cleanup-only transition for pre-atomic HTTP
  admissions. On a deep copy, the validator replaces only `USER_ID`,
  `USER_NAME`, and `client_api_version` with the locked service-owner ID/name
  and `None`, respectively. The reconstructed digest must equal
  `input_digest`; the durable request is never rewritten. Every other body,
  context, tenant, cluster, profile, or authority change fails closed.

The legacy mode grants only entry into exact provider-present fenced teardown;
it cannot authorize launch, adoption, cancellation, paid capacity, or result
projection. The 2026-08-20 production census found exactly nine unsettled
`RESERVED_FILL` associations, all nine matched this reconstruction, and zero
had an unexplained mismatch. Once they settle and one complete
stale/quiescence horizon observes zero unsettled legacy rows, the stacked
cleanup removes this branch and its transition-only tests. No schema mode is
added for a verifier whose entire population is already enumerated and whose
steady state is direct-digest-only.

Atomically admitted `RESERVED_FILL` requires the server-local prepared body and
durable executable body to have the same digest at admission, lost-ack
hydration, and cleanup. This preserves exact corruption containment without
making an obsolete normalized representation current again.

Terminal request errors also retain their existing public source type/message
metadata. A non-builtin, non-SkyPilot source exception is deliberately encoded
as a client-safe `CloudError`; the reducer accepts that second representation
only when the decoded wrapper's nonempty provider, source type, single source
message argument, and rendered message are mutually exact. The production
census found four of the nine rows in this canonical
`concurrent.futures.CancelledError` wrapper shape. Any mismatch among the outer
type/message and decoded wrapper attributes, arguments, or rendered message,
or any extra field, remains malformed. The self-consistent provider label is
diagnostic metadata and grants no request, provider, or cleanup authority.

The current planner is similarly pre-effect authority, not teardown authority.
Allocation, observation, and reconciliation-gate generations may advance after
an exact launch. Provider-present cleanup reconstructs the admitted profile
from the immutable committed intent, original observation, and replica's
admission-time sequence and gate generation. The live sequencer must remain a
monotonic successor of those frozen values, but today's gate is never
substituted into yesterday's profile digest. This keeps cleanup exact without
requiring an obsolete grant to become current again.

### Atomic reserved-fill replica and request admission

Protocol-v2 durable reserved fill has one PostgreSQL visibility boundary.
Before SQL mutation, the controller builds a side-effect-free prepared launch
body from the selected replica task, stable submission UUID, immutable service
workspace, and current controller fence. It performs no API-endpoint discovery,
HTTP submission, controller/system authentication, or file upload. Inputs that
need controller-local paths, mutable config, or upload fail before capacity is
debited. Inside the transaction, the immutable owner ID on the durable service
resolves to exactly one extant user row under a shared row lock. That tenant
and the durable workspace -- never the current API/controller system identity
or a machine-local fallback -- own the request and generalized association.
Before invoking the shared canonical builder, atomic admission stamps the
durable tenant ID, immutable non-empty `services.owner_user_name` audit value,
workspace, and fixed server API version into that prepared body. Its canonical
pre-normalization digest consequently also equals the durable executable-body
digest and an exact lost-ACK hydration cannot change when the user's current
display name changes. At execution only, a bound request resolves the current
non-empty `users.name` by immutable ID and does not upsert the historical queued
name. New services write the audit tuple in their initial service/version
transaction. A retained
pre-Serve055 service starts with both fields `NULL` and its exact elected
controller may attest the frozen launch-time ID/name once under the complete
service, lifecycle, incarnation, and owner-epoch fence. Partial tuples,
deleted users, and a frozen identity that differs from an already attested
tuple fail closed; no migration or recovery path guesses or repairs them.
While either owner column is missing or nullable, deletion of every
non-internal user is temporarily unavailable. A per-user or `NULL` census is
not sufficient: an old in-flight writer can insert a nullable service after
that read without acquiring the owner foreign-key lock. This schema-derived
global guard is the transition's deliberately conservative behavior. Serve056
removes it only after both columns become `NOT NULL`, the zero-`NULL` and
no-old-writer gates pass, and the foreign key can serialize every future
service creation against exact user deletion.

The atomic reserved-fill admission module owns one root PostgreSQL transaction
and one nested savepoint. In the
canonical sequencer -> protocol -> broker lease -> lifecycle -> service ->
actuation intent -> capacity ledger/replica -> association -> API request ->
queue -> retention-pin order, it revalidates all current authority, inserts the
typed replica, transfers the exact intent from `ACTUATING` to `COMMITTED`,
resolves `RESERVED_FILL/v1`, and inserts or exact-matches the deterministic
association, non-retryable request, queue delivery, and retention pin. The
durable actuation lease's immutable `FillIntent` is the sole source for its
pool epoch and ordinary-admission high-water; atomic admission carries no
duplicate caller-provided copies that could disagree with that intent. The
Serve staging primitive only performs non-committing writes on the supplied
connection; the atomic layer owns savepoint release, root commit/rollback, and
lost-ack hydration. It publishes database-assigned admission state into
manager memory only after root commit. Any validation or suffix-write failure
rolls back the replica, intent transfer, association, request, queue row, and
pin together. For this internal atomic profile, request identity and its
canonical digest are computed only after those immutable owner, workspace, and
API fields have been stamped into the prepared launch body.

Only a complete commit may install an adopter for the returned request ID. It
starts immediately after commit (or immediately when the restart scanner
rediscovers the complete tuple) because queued work may already be executable.
The adopter is only a reducer/observer: it reads the existing PostgreSQL
request result directly, makes no second submission, and bypasses spot
admission budgets, provider benches, and replica-delete admission. The ordinary
generic queue executor remains the sole provider-effect path, behind the final
context-wide Serve054 proof and exact per-launch scope validation. No provider
effect, provider-launch thread, or process-local capacity publication precedes
commit.

Commit acknowledgement is a closed three-way contract. `COMMITTED` means the
complete exact tuple is durable and may be adopted. `REJECTED` means a definite
precommit rejection followed by an acknowledged rollback. `AMBIGUOUS` means
the commit or exact follow-up read was inconclusive; it preserves the intent
and stable identities and performs no cleanup, release, launch-thread
registration, or provider call. A fresh transaction may hydrate only the same
complete digest/profile/fence tuple. It never repairs a partial historical
tuple. In particular, commit-applied/ACK-lost followed by a second read failure
remains `AMBIGUOUS` and restart-adoptable.

Serve055 is the only feature schema change: it adds nullable `owner_user_id`
and `owner_user_name` columns to the existing `services` table, a
both-or-neither non-empty check, an `owner_user_id -> users.id ON DELETE
RESTRICT` foreign key, and a PostgreSQL trigger that permits the one `NULL` ->
tuple attestation but makes the tuple immutable thereafter. It is
PostgreSQL-only, forward-only, and adds no table or SQLite path. The API user
deletion guard rejects every non-internal deletion while either owner column
is missing or nullable. In the Serve056 steady state, exact-owner listing
reports owned services before deletion and the foreign key is the race-closing
authority if service creation wins concurrently.
The feature adds no scheduler, actuator, Terraform, Kueue, EFS, Helm resource,
or platform runtime pin.

The feature physically removes protocol-v2 direct/non-atomic persistence and
rejects `RESERVED_FILL` at the controller HTTP launch surface; the generic HTTP
surface remains canonical for other profiles such as `SYSTEM_OOM_RECOVERY`.
There is no reserved-fill system-identity or RWX/EFS correctness branch left
for a cleanup to remove. The immutable request body, digest, environment, and
association continue to carry the durable service owner. Execution may skip
only that owner's mutable workspace-membership check when one PostgreSQL read,
keyed by the request ID's unique current association, proves all ordinary
current-binding fences plus the canonical
`RESERVED_FILL/RESERVED_FILL_ALLOCATION` profile, association tenant equal to
the current service owner, association/frozen/current workspace equality, and
current `DURABLE_INTENT` actuation. The executor then resolves the same owner
as an existing user and rechecks that the applied workspace is exactly the
database-authorized frozen workspace; its environment, reload identity, event
actor, and event workspace remain that owner tuple. Stale, malformed, legacy,
ordinary, or caller-crafted requests fall through to ordinary owner RBAC, and
the final provider guard still revalidates the binding before provider I/O.
The request body is only a query-scope hint and never authorization authority.

Separately, the pre-existing Serve056 owner-column cleanup branch is planned as
`fix/serve-atomic-fill-admission-cleanup`, but has not yet been authored or
opened. It is not the historical-digest verifier cleanup stack introduced
above. Its only steady-state change is Serve056: after the transition gates
below, make both owner columns `NOT NULL`, remove the application
`NULL`-attestation controller branch, and retire its transition-only
observability/tests. The database must retain the existing owner-immutability
trigger or atomically replace it with a simpler permanent trigger that rejects
every non-null tuple mutation.
It does not revive closed PRs #1506/#1510 or remove generalized non-pool
machinery used by other profiles.

The cleanup PR stays draft until all of these exact gates pass on the feature
revision: the complete API/controller/executor cohort is current and no old
owner-tuple writer can restart; `SELECT count(*) FROM services WHERE
owner_user_id IS NULL OR owner_user_name IS NULL` is zero; database backups and
feature production evidence are captured; and the immediate, +10, +30,
complete 180-second authority, full stale-writer/quiescence, controller-child
restart, controller-Pod takeover, large multi-pool fill, ordinary-traffic, and
no-paid-spill gates all pass. Real-PostgreSQL savepoint, rollback, lock-order,
lost-ACK/read-failure, restart-adoption, current-user-name, owner-FK race, and
source-no-fallback tests must also pass. Serve056 then validates zero `NULL`
tuples in its migration transaction before applying `NOT NULL`; its final-state
suite must prove the application one-shot branch and transition-only artifacts
are physically absent and direct SQL cannot mutate one non-null owner tuple to
another before merge.

## Prior-plan reconciliation and intentional departures

This file remains the one canonical design. Its detailed pre-implementation
state is preserved immutably at commit
`dbaad6213b582ae1b2e3bb364d6cc5e55bd7d311` and can be inspected with:

```bash
git show dbaad6213b582ae1b2e3bb364d6cc5e55bd7d311:\
docs/designs/serve-multi-pool-reserved-capacity-fill.md
```

That revision is historical evidence, not an alternate contract or operator
runbook. The implementation audit deliberately replaced these planned
mechanisms before activation:

| Superseded plan | Canonical implemented contract | Reason |
|---|---|---|
| One composite `(physical UID, accelerator set)` pool per context | One atomic `(physical UID, exact accelerator card)` edge; authenticated aliases are bounded query routes | Heterogeneous A100/H200 supply, width, and failure must remain independent without multiplying physical capacity. |
| Slot-valued provider observations | Raw exact-card GPU counts plus an exact presence set; the broker converts once using `broker_slot_width` | Claimant width is service policy, not physical evidence; converting at observation time caused ambiguous or double conversion. |
| Advance a capacity counter when observation begins | Snapshot three non-advancing commit counters: total admission, ordinary admission, and first-success materialization | Observation start is not a capacity-consuming event. Commit order, rather than wall time, closes admission and provider-visibility races. |
| Direct single-context broker polling under the actuation path | Bounded concurrent observation cohorts, per-edge alias failover, immutable PostgreSQL results, and post-commit notification | Provider latency must not consume observation freshness or serialize unrelated physical pools. |
| Ordered-prefix receipt | A bijective sparse receipt: pool-local failures skip only their intents; global authority loss defers the remaining ordered tail | One unavailable cluster must not starve an independent cluster, while service-wide fences remain atomic. |
| A new durable intent state machine, provider scheduler, mutation arbiter, and debt path | Durable replica-row acceptance plus the existing generalized non-pool association/request/queue transaction is the receipt boundary; accepted rows use the existing asynchronous provider path | A second scheduler and actuator would create another happy path and duplicate lifecycle ownership, while an unbound special launch would repeat the incident defect. |
| Pod `PriorityClass`, task-owned Kueue priority, mutable launch-time queue resolution, or activation-time attestation as sufficient reclaim proof | The immutable worker projection is the sole admission owner; one entry-point-loaded deployment policy must prove and durably identify the shared Kueue domain, then authorize every sequenced claim set and terminal provider launch against the exact service version and projection digest | Kueue can withhold higher-priority BCL Pods before kube-scheduler priority can act, and a one-time census or Pod-only identity cannot govern later claims, service updates, or restarted executors. |
| External release supervisor, phase-0 authority reset, bootstrap/maintenance modes, capacity canary, rollback, and fixed 24-hour soak | Full immutable split-role rollout at `LEGACY_ACTIVE`, exact activation prerequisites, then generation-fenced fix forward; no capacity canary or supported demotion | The Serve045 gate/reclaim receipt plus Serve046 version/projection binding form one smaller fail-closed transition and match the current lightly used service. |
| Provider-progress status as launch authority | Provider-free `reserved_fill_reconciliation` diagnostics derived from authenticated allocation and durable rows | Observability must not perform provider I/O or become a second authorization source. |

Rejected alternatives remain rejected: increasing the 180-second TTL hides the
lock convoy; a finite `provision_timeout` cannot distinguish initialization
from capacity exhaustion; a fallback planner or actuator duplicates authority;
an external scheduler/supervisor duplicates lifecycle ownership; Pod priority
alone does not prove Kueue reclaim; and a canary or rollback protocol is not
required for this fix-forward rollout.

## Goals

- Fill every service-granted slot of idle, policy-compatible zero-cost
  Kubernetes GPU capacity in its exact accelerator width without multiplying
  the service-wide policy.
- Preserve the conservative 180-second capacity-authority horizon.
- Query independent Kubernetes contexts concurrently and outside slow
  actuation locks.
- Publish only a complete service-wide planner input authenticated against the
  current service owner, claim generation, pool rounds, physical identities,
  and exact observations.
- Use the same autoscaler decision tick for demand, scale-down shelter, and
  reserved fill, with one reconciliation coordinator and no lost wakeup.
- Debit ordinary demand and already accepted fill before producing new fill
  intents.
- Count capacity as spent and advance pool rotation only from a validated
  durable commit receipt.
- Atomically bind every accepted fill row to the exact generic non-pool API
  request and return its association/request identities in that receipt.
- Revalidate the exact intent, current service ceiling, and remaining
  aggregate and accelerator-card feed in the same transaction that inserts
  each sequenced fill row.
- Keep every fill launch pinned to a zero-cost location in the exact physical
  pool and accelerator class that authorized it.
- Preserve and mechanically respect the deployment-owned Kueue admission and
  preemption contract under which BCL work may reclaim preemptible inference
  slots.
- Make canonical worker placement projection v4 the only owner of new
  projected Kubernetes admission: task inputs cannot select Pod priority,
  LocalQueue, or
  WorkloadPriorityClass, and launch rendering cannot reread those values from
  mutable configuration.
- Bind the exact deployment reclaim-policy identity into PostgreSQL, allocation
  authentication, replica provenance, and the terminal provider launch fence,
  together with the committed service version and complete worker-projection
  digest; a missing, legacy, ambiguous, stale, or differently identified
  policy or projection fails closed.
- End with the sequenced path as the only launch path and a concrete stacked
  removal change for the compatibility code.

## Non-goals

- No new YAML, SDK, or CLI service policy field.
- No increase to the observation freshness horizon and no postdating of a slow
  provider read.
- No inference from `provision_timeout` that a Kubernetes request is either
  initializing or out of capacity.
- No paid fallback. A reserved-fill intent is zero-cost-only and is skipped if
  its exact zero-cost location cannot be proved.
- No fixed application/model-ready SLA for a 200-replica wave. Durable
  admission is bounded; provider scheduling and image pull retain their real
  latencies. Canonical projected workers separately bound only their
  in-container SSH, environment, SkyPilot, and Ray bootstrap to 30 minutes
  after container creation before kubelet startup-probe failure.
- No capacity-consuming canary, shadow planner, dual actuator, per-service
  rollout flag, or supported post-activation demotion.
- No caller-selectable Kubernetes namespace, service account, PriorityClass,
  LocalQueue, WorkloadPriorityClass, toleration, or BCL priority policy. The
  existing deployment-owned values move into one immutable projection and are
  reasserted; this feature does not choose new priority semantics.
- No status-side provider calls, mutation authority, or new provider-progress
  scheduler. Diagnostics are a read-only projection of already authenticated
  PostgreSQL authority and exact durable replica attribution.
- No reserved-fill-specific association, queue, handler, executor, effect
  receipt, or recovery lock. The common binding carries a typed reserved-fill
  profile while this design retains broker and reclaim authority.

## Public contract

The existing policy forms remain unchanged:

```yaml
service:
  replica_policy:
    reserved_capacity_fill: true
```

and:

```yaml
service:
  replica_policy:
    reserved_capacity_fill:
      floor_replicas: 10
      weight: 100
      utilization_gate: true
```

The production Boltz fleet intentionally uses this existing policy shape:

```yaml
service:
  replica_policy:
    min_replicas: 0
    reserved_capacity_fill:
      floor_replicas: 0
      weight: 100
      utilization_gate: false
```

This is full zero-cost backfill, not an always-paid minimum. A false utilization
gate removes traffic as a cap on the service's authenticated grant from fresh,
policy-compatible reclaimable supply, but a reserved-fill intent remains pinned
to its exact zero-cost pool and cannot fall through to Spot or on-demand
capacity. Research retains the deployment-owned higher priority and may reclaim
these Pods. The test fleet overrides the same field to `true` so it relinquishes
borrowed capacity when idle.

Zero-cost Kubernetes candidates are grouped by physical pool. The steady-state
atomic identity is one physical Kubernetes cluster UID plus one exact
accelerator card; the access context and replica width are carried separately.
A single context offering `A100: 1` and `H200: N` therefore produces two
independent, deterministically ordered edges. The repeated context is valid:
only an overlap on the same physical UID/card is ambiguous. Context aliases
that prove the same UID/card cannot multiply one physical pool, and aliases
that disagree on width for that exact card fail closed.
The deployment policy enforces the same atom invariant per service through one
shared claim-set validator used by both activation and every later claim
replacement. Activation groups its global scope by service before applying the
validator: it accepts multiple services claiming the same brokered pool/card,
accepts multiple edges sharing an access context within one service, attests
each context once, and rejects only a second claim by the same service on the
same `(physical_cluster_uid, exact_card)` atom before provider calls.
The typed admission for that atom also carries the normalized accelerator
scheduling tuple: label key, sorted label values, and extended-resource key.
Activation, every claim-set replacement, and every terminal launch must match
that tuple exactly against a disjoint code-owned card contract. A logical card
cannot own a flavor or scheduling tuple already owned by another logical card.

The existing service-wide meanings remain authoritative:

- `floor_replicas` is one total fill floor, not one floor per context.
- `weight` is relative between services sharing a pool. If a second service
  with the same weight joins, the broker shares eligible capacity according to
  both services' floors, weights, holdings, and caps; setting both weights to
  `1000` is equivalent to setting both to `1`.
- `utilization_gate: true` measures service demand once and bounds the same
  global fill budget. `false` removes demand as a utilization cap; the
  service's authenticated grant from fresh observed zero-cost supply becomes
  its fill budget. It does not create ordinary paid demand or weaken any pool,
  service, or admission ceiling.
- `max_replicas` is a hard service-wide ceiling across versions and pools.
- Ordinary demand has priority in the service headroom calculation and in the
  allocation-local pool/card debits.

`kubernetes.provision_timeout` is not changed by this feature. In particular,
`-1` may remain the correct choice for preemptible reserved capacity that
should wait rather than fail the service. A finite timeout such as 30 seconds
is not used as a capacity classifier. The observer records explicit success or
typed blackout evidence, while ordinary replica/provider states continue to
describe initialization and launch progress. The indefinite-wait liveness
guarantee applies to the instrumented built-in Kubernetes reserved-fill path,
whose passive scheduling/readiness waits hold no authority guard. Opaque
provisioners are not eligible for protocol-v2 reserved fill: v2 requires the
in-tree Kubernetes create/adopt/attest boundary so success has an unambiguous
one-way Pod-materialization transition.

## Architecture and invariants

### 1. Provider-free observation ledger

`sky/serve/pool_capacity_observer.py` owns physical-pool reads. For one
observation tick it:

- builds an immutable target from pool key, physical UID, every authenticated
  access-context route, and the exact accelerator card;
- rejects service configurations with more than eight resolved exact-card
  `(physical UID, card)` edges, then starts the bounded independent pool
  queries concurrently; each edge independently accepts at most eight
  authenticated access-context routes;
- passes each query an absolute deadline (45 seconds by default);
- rotates the first alias attempted, gives each remaining alias a fair share of
  the remaining root deadline, and emits a typed pool-local blackout only after
  every authenticated route fails; and
- commits either raw exact-card GPU success with the winning route or a typed
  pool-local blackout; and
- notifies reconciliation only after the completed row is durable.

The observer does not allocate, plan, mutate replicas, or hold the controller's
actuation lock during provider I/O. A slow or failed pool cannot serialize a
healthy sibling. The provider adapter must consume the same absolute batch
deadline, bound its blocking Kubernetes calls to the remaining time, and free
executor capacity after timeout; a wrapper-only timeout is insufficient.
Exact-card edges in the same context join one in-progress physical-UID capture
under that deadline rather than interpreting their shared initializer as
capacity failure. Accelerator names returned by the provider are case-folded
only after rejecting collisions, and the catalog's negative forbidden-read
sentinel becomes a typed permission blackout.

`PoolCapacityObservationRepository` is PostgreSQL-only. The canonical
`begin_observations()` boundary locks the event sequencer once, locks the
requested pool rows in sorted order, and atomically acquires every independently
due, unleased pool in a cohort. Busy or not-due members are skipped without
starving healthy siblings; their prior completed evidence remains independently
bounded by its own freshness deadline. Every returned lease shares one capture
of:

- a per-pool observation generation and lease token;
- the global all-zero-cost admission high-water;
- the global ordinary-zero-cost admission high-water;
- the global first-successful-launch materialization high-water;
- a conservative `observed_at` at query start; and
- `valid_until = observed_at + 180 seconds` under current defaults.

Starting an observation advances none of the event counters. Observation
generation identifies a provider read; admission and materialization counters
identify replica-row commits that can invalidate or contextualize that read.
Only the ordinary-admission counter must match across the old and new pool
evidence combined into a service-wide allocation. The total admission and
materialization counters remain per-pool debit boundaries, so skipped older
evidence is safe when its exact provenance is retained. Repository and target
validation both enforce the eight-edge bound; route validation separately
enforces the eight-alias-per-edge bound. There is no silent chunking that could
reintroduce staggered ordinary-admission prefixes.

Completion verifies the latest lease and identity and writes a SHA-256 over
the complete identity, sequence, payload, legacy projection, and timestamps.
A success stores physical GPU counts, never service-specific replica slots.
The exact-card count remains present even at zero; the separate canonical
presence set distinguishes a present-but-full card from a card that is absent
from the physical cluster. The winning access context must belong to the
immutable route set acquired in the lease and is persisted as observation
provenance.

A transient physical-UID discovery failure retains the last proven
context-to-UID edge instead of deleting fleet-wide capacity topology. This is
not stale launch authority: every provider observation and every admitted
launch still re-proves that UID through the captured Kubernetes client. A
retargeted context therefore fails its identity fence, while another alias for
the same physical pool may continue the observation.

A timed-out, superseded, malformed, permission-denied, identity-mismatched, or
otherwise failed query grants no capacity. A newer completed blackout prevents
fallback to an older success. A newer in-progress generation does not erase
the last completed result until it completes.

The fixed-rate poll remains 60 seconds. The correction is event-driven after a
publication: it does not wait for the former autoscaler polling interval or
for unrelated actuation, but it cannot discover capacity before the next
physical observation tick.

### 2. Commit-order sequencing for zero-cost admission and materialization

The protocol singleton has three deliberately separate counters:

- `zero_cost_admission_sequence` is the total commit order for every accepted
  zero-cost replica row. It provides row attribution and replay diagnostics.
- `ordinary_zero_cost_admission_sequence` is the cross-service invalidation
  generation for ordinary demand. Ordinary capacity is not broker-partitioned,
  so a commit by any service invalidates every allocation map based on the old
  generation. Reserved-fill commits do not advance this counter because their
  capacity is already partitioned by authenticated broker grants.
- `zero_cost_materialization_sequence` is the total commit order for the first
  persisted successful `sky.launch` of every zero-cost row. It closes the
  interval between row admission and provider-visible occupancy without using
  `created_at`, readiness, or an inferred provisioning timeout.

Each observation snapshots all three counters without advancing any of them.
An allocation publication is valid only when every included observation
captured the same ordinary high-water and that value still equals the locked
protocol singleton. A reader repeats the exact-equality check; a newer ordinary
commit makes the map stale instead of trying to repair it with application
clocks.

In `SEQUENCED_ACTIVE`, an ordinary zero-cost insert atomically advances both
counters and stores its database-assigned total sequence on `ReplicaInfo`. A
typed fill insert carries the allocation's ordinary high-water, requires it to
still equal the locked singleton, and only then advances the total counter.
The manager performs that final revalidation and the generic binding admission
while participating in the same global demand-admission lock as ordinary
placement. In the same PostgreSQL transaction it inserts the replica row,
inserts `RESERVED_FILL/v1` in the existing association table, inserts the
correlated API request, queue row and retention pin, and returns their exact
identities. Provider preflight stays outside that lock. This closes both
directions of the race:
ordinary demand cannot commit between fill revalidation and persistence, and a
fill cannot consume stale evidence while an ordinary placement transaction is
in flight.

The pure planner still debits ordinary decisions from the same reconciliation
tick before they have committed. A target-less or otherwise ambiguous decision
is conservatively debited against every compatible map-local pool, with each
debit capped by that pool's authenticated feed. The broker also performs one
complete row snapshot across claimant and nonclaimant services. For a sequenced
observation, a compatible nonterminal zero-cost row debits observed free when
its admission is newer than the observation admission high-water, its first
successful launch is newer than the observation materialization high-water, or
either marker is missing or malformed. A row whose valid admission and
materialization markers are both no newer than the observation is left to the
provider measurement and is not double-debited. This prevents two services
from spending one observed slot without making broker-disjoint fill commits
invalidate each other.

Physical placement, not economic classification, determines whether a replica
can race a pool observation. Sequenced scans therefore include every
Kubernetes row whose current access context and accelerator match the pool,
plus rows with complete immutable pool provenance and conservative same-card
fallbacks for unattributed zero-cost/fill rows on retired aliases. This rule is
durable across replica rewrites: serialization upgrades a pre-v11 row to the
latest record version but cannot reconstruct its historical `is_zero_cost`
truth. A false cost flag can therefore never make a physically matching row
disappear from the debit. Until every Kubernetes launch row carries immutable
physical-UID attribution, an unattributed same-card row on another context may
be using a retired alias and is conservatively debited from every compatible
v2 pool. A row on a non-Kubernetes cloud is excluded unless its persisted pool
provenance still makes this pool plausible. This conservative occupancy
accounting is not a second launch path and is intentionally retained by cleanup
PR #1452. A separate future stack may persist the physical UID on ordinary
placement, migrate live ordinary rows as they are authoritatively refreshed,
and remove same-card duplication only after no nonterminal zero-cost row lacks
that identity for one complete observation horizon. Neither this feature nor
#1452 claims that removal.

The complete replica snapshot is part of spendable sequenced authority. A
grouped enumeration, query, or decode failure rejects the new observation and
does not publish a successor round; the previous round remains bounded by its
original freshness deadline. The legacy callback path retains its historical
per-service fallback only while the durable selector remains in
`LEGACY_ACTIVE`.

The materialization marker is assigned only after the provider operation has
reported success, in the same PostgreSQL transaction that projects the locked
terminal request evidence and ordinarily first persists
`sky_launch_status == SUCCEEDED`. If teardown already made `INTERRUPTED`
absorbing, the reducer passes the exact provider-success bit separately so it
can stamp materialization without reviving the replica. A pre-effect
cancellation passes false and cannot stamp it. The marker is never written
before provider visibility. A provider query that overlaps the bind-to-marker interval may
already exclude the pod while the sequence rule also debits it. That is a
deliberate conservative underfill for at most the current observation round:
the next observation snapshots the committed marker and leaves the row to the
provider measurement. The opposite, oversubscribing because a launch became
visible after the query started, is not allowed.

Occupancy is reconciled in the same slot and accelerator units carried by the
provider observation. Complete pool-key/physical-UID provenance dominates a
retired access alias or a later claim-width change: a queued old row can still
bind to that physical pool, so its historical GPU count is converted to the
current width's slot equivalent. Partial, legacy, or shapeless zero-cost rows
debit every plausible same-card pool/card until their physical identity is
known. Exact-card debits are subtracted from that exact card before feed
partitioning; an A100 row can never make the broker withhold H200 while leaving
A100 launch authority. Rows proven physically off-pool are excluded; a paid
classification alone is not physical absence and cannot override an exact
placement match.

A `SHUTTING_DOWN` or `FAILED_CLEANUP` zero-cost row remains cleanup-unproven
occupancy. A successful launch status or durable materialization marker is
enough to conserve fill entitlement. Independently, every such row whose event
markers do not prove it preceded the observation debits the provider snapshot,
including an interrupted launch with a missing or malformed materialization
marker: the pod may have bound immediately before cancellation while its
success reducer lost the race. Fill rows also participate in the same-map
planner replay debit and service `max_replicas` headroom until physical cleanup
deletes the row or transitions it to a cleanup-proven terminal state. This
prevents one allocation map from re-spending a slot during graceful drain or an
ambiguous cancellation.

All PostgreSQL replica writers that may insert a zero-cost row or persist its
first successful launch use one SQL lock order:

```text
zero-cost event sequencer
  -> protocol/lifecycle/service authority
  -> sorted pool/claim authority where applicable
  -> replica row
```

The bound-request reducer takes the sequencer mutex at transaction entry,
before it can inspect the service and replica to learn whether the launch is
zero-cost. Stale whole-row updates merge the immutable database-assigned event
markers from the locked row, so a retry cannot erase or replace either one.

Replica state version 18 persists the complete typed fill attribution. The
transitional reader accepts only the six sanctioned legacy versions/eight
exact pre-v17 shapes and the two exact observed v17 shapes described in the
deployment gate below:

- allocation generation, input hash, and claim generation;
- observation generation and sequence;
- intent idempotency key; and
- reconciliation-gate generation and the exact three-part reclaim-policy
  identity for that generation;
- the SHA-256 digest of the exact protocol-v2 worker placement projection that
  owns Kubernetes and Kueue admission for this replica;
- database-assigned zero-cost admission sequence; and
- database-assigned first-success materialization sequence, once launched.

For v17, the six historical allocation/observation/intent fields are all
present or all absent. The five gate/policy/projection fields are likewise all
present or all absent and require the historical tuple. The v17 collision
branch admits only the complete current shape or the exact observed shape with
all 13 attribution fields absent. The complete successor tuple must match
current durable authority before it can authorize a sequenced launch. The
closed readable version set is therefore `3`, `6`, `7`, `12`, `13`, `14`,
`17`, and `18`; v15, worktree-only v16, every other version, and every partial
or extra-field shape fail closed. The materialization marker is null before
launch success and a positive integer afterward. Missing event attribution is
a conservative debit in sequenced rounds, and only fully attributed current
rows count as same-allocation replay debits.

### 2a. Durable reclaim authorization receipt

Serve045 is a forward-only successor to Serve044. It adds a nullable reclaim
receipt to the PostgreSQL protocol-authority singleton. The receipt contains:

- `reclaim_fleet_bundle_sha256`, `reclaim_policy_revision`, and
  `reclaim_provider_inventory_sha256`, which form the typed
  `ReclaimPolicyIdentity`;
- `reclaim_claim_scope_count` and `reclaim_claim_scope_sha256`;
- `reclaim_evidence_sha256` and `reclaim_authorized_at`; and
- the existing protocol-v2 writer proof: image digest, Deployment generation
  and UID, and Pod inventory count and digest.

The reconciliation-gate constraint is closed: reclaim fields are null in
`LEGACY_ACTIVE`; a `SEQUENCED_ACTIVE` row has one complete well-formed receipt
and protocol version 2. First activation is `LEGACY_ACTIVE ->
SEQUENCED_ACTIVE` at exactly `generation + 1`. Fix-forward reauthorization is
`SEQUENCED_ACTIVE -> SEQUENCED_ACTIVE`, also at exactly `generation + 1`, with
a different complete evidence digest. Exact receipt replay is an
application-level no-op: it changes neither generation nor timestamp.
Demotion, generation jumps, partial edits, and same-generation authority edits
are rejected by an `ENABLE ALWAYS` trigger. Serve044 remains historical
migration authority and is not rewritten.

Policy or writer rotation therefore uses no alternate protocol, feature flag,
or in-place identity edit. The operator converges the full fleet, reruns the
same `activate` authorization command, and atomically replaces the receipt in
a successor generation. The transaction clears every authenticated allocation
map. Already durable rows remain conservative occupancy, while queued requests
carrying the old generation fail the terminal provider fence. This is the one
canonical fix-forward path. An unchanged policy contract retains its explicit
policy revision across ordinary SkyPilot overlay releases, so its claim
heartbeat can first bind a concurrently committed current service version.
The activation still rotates and attests the complete writer cohort; no writer
receipt is inferred from the policy revision.

`reclaim_provider_inventory_sha256` fingerprints the immutable allowed fleet
and enforcement inventory owned by the bundle. It must not hash live Pods, the
current claim census, or another naturally changing observation; those values
are activation/authorization evidence and would make an immutable identity
drift without an explicit protocol transition.

Serve046 is the forward-only admission-binding successor to Serve045. It adds
the committed `service_version` to each authoritative claim set and the exact
closed `worker_projection_sha256_by_accelerator` mapping to every normalized
claim edge. The application
requires both for a sequenced set, locks the immutable version row, selects the
exact `(context, accelerator, count)` worker projection for every card,
validates protocol v2 with non-null Kueue admission, and recomputes every digest
before claim persistence. The mapping has exactly one case-folded accelerator
key for every edge accelerator and no extras. This remains correct for the
canonical one-card edge while safely authenticating an older composite edge;
one scalar edge digest cannot identify multiple candidates. Legacy-active
compatibility rows may keep the version and mapping null; they cannot publish a
sequenced allocation or authorize a sequenced launch.

Allocation-map schema 5 hash-binds the gate generation, all three policy
identity fields, the committed service version, and each edge's exact
accelerator-to-worker-projection-digest mapping. A `FillIntent` narrows that map
to the one selected accelerator digest, and a sequenced replica row persists
that scalar as part of its immutable fill attribution.
The protocol-v2 API launch fence carries it into the durable request row. The
fill-persistence transaction revalidates the allocation, current gate
generation, exact identity, idempotency key, service ceiling, and remaining
aggregate and per-card feed before accepting the row. This makes the
activation proof, broker claim, allocation, row, and provider effect one
traceable authority chain rather than independent checks.

### 3. Broker provenance and authenticated allocation map

In compatibility mode, the protocol-v2 broker may still query a provider
inside its old round path. In `SEQUENCED_ACTIVE`, it consumes only a fresh
completed observation and calls
`run_round_from_committed_observation()`. The committed round stores the exact
observation generation, admission sequence, materialization sequence, and
payload digest as one nullable-all-or-present tuple. Publication revalidates
that tuple against the digest-valid observation while holding the event
sequencer, and allocation reads repeat the exact match; an in-memory
provenance value that was not durably persisted cannot authorize fill.

The broker is the only raw-GPU-to-replica-slot conversion boundary. It chooses
one deterministic authenticated claim width for a physical UID/card, divides
the committed exact-card GPU count once, and persists that `broker_slot_width`
beside both the converted observation and per-service feed. Claims with a
different width remain in the authoritative claim set but receive explicit
zero launch and shelter authority for that round; they cannot reinterpret or
double-convert another claimant's slot count. Allocation publication
recomputes the conversion from the exact committed raw observation and rejects
any mismatch. Old observation payload schemas are not silently inferred as raw
GPU authority and fail closed.

During the pre-activation image rollout, legacy broker rounds retain their
existing exact-card envelope bytes and do not emit the new slot-width metadata.
The width key first appears in committed-observation rounds after gate
activation has proved exact writer convergence. This avoids mixed-binary epoch churn
without adding a second sequenced representation.

`ReservedFillAllocationRepository` is the sole durable adapter from broker
rounds to the pure planner. It publishes a map only if every claimed pool has a
complete `PoolFillSnapshot` and all of these still agree in one PostgreSQL
transaction:

- protocol v2 and the current reconciliation-gate generation;
- service hash, resource scope, and controller owner;
- service claim-set generation and ordered edge topology;
- sorted pool round identities, epochs, grants, feeds, and exact-card feeds;
- physical cluster UIDs and access contexts; and
- latest fresh completed observation provenance.

The canonical lock order is protocol, service, sorted pool rounds, claim set
and edges, then exact observations. The map hash covers the complete ordered
map. A no-op republication returns the existing generation. A semantic claim
replacement clears the old map before a successor can be published. Readers
revalidate the current rounds, claim set, owner, gate, and freshness rather
than trusting the stored JSON alone.

The first shipped allocation-map wire schema is explicitly versioned as 5;
the version is both persisted and covered by its authentication hash. Earlier
worktree-only schemas 3 and 4 and earlier shapes were never merged, deployed,
or activation-capable,
so they are rejected as unknown durable state instead of creating a permanent
compatibility decoder. This preserves one canonical map path from the first
production release.

The closed top-level schema contains exactly `schema_version = 5`,
`allocation_generation`, `allocation_input_sha256`,
`allocation_claim_generation`,
`service_version`,
`ordinary_zero_cost_admission_sequence_high_water`,
`reconciliation_gate_generation`, `reclaim_fleet_bundle_sha256`,
`reclaim_policy_revision`, `reclaim_provider_inventory_sha256`, and
`pool_snapshots`.
Each ordered snapshot contains exactly `protocol_version`, `pool_key`,
`physical_cluster_uid`, `service_generation`,
`worker_projection_sha256_by_accelerator`, `edge_cap`,
`broker_slot_width`, `free_slots`, `free_slots_by_accelerator`, `grant`,
`grant_epoch`, `observation_generation`, `observation_sequence`,
`ordinary_zero_cost_admission_sequence`, `valid_until`, and
`zero_cost_location_keys`. The hash covers the schema version, ordering, and
every authoritative input field; `allocation_input_sha256` stores that hash.
Missing or unknown fields and any schema other than 5 fail closed.
Materialization provenance is authenticated by the allocation's exact
observation/round join and is not duplicated into this map.

Allocation-map schema 5, worker placement projection protocol 2, internal
observation-authority payload schema 3, and PostgreSQL Serve schema 046 are
independent version domains. Equality of their
numbers carries no compatibility meaning.

Observation access context is query-route provenance, not physical-pool
identity. Several contexts may alias one physical UID and accelerator set. A
current physical-pool observation may authorize service edges reached through
those aliases once every edge independently proves that same physical UID; map
publication must not require the observer's chosen context string to equal
every claim's service-edge context. Each accepted intent remains pinned to its
own authenticated service-edge context.

Ordinary zero-cost rows currently persist their Kubernetes context and exact
accelerator shape, but not the context's physical-cluster UID. The sequenced
occupancy scan therefore conservatively debits such a row against every v2
pool with the same accelerator card and per-replica width, regardless of
context alias. Physically disjoint same-card pools may underfill; they cannot
oversubscribe. This compatibility duplication is not a second placement path.
The steady-state follow-up is to persist the physical UID on ordinary
placement, migrate live ordinary rows as they are authoritatively refreshed,
then remove the same-card duplication after no nonterminal zero-cost row lacks
that identity for one complete observation horizon.

Publication is all-or-nothing across a service's pools. If one edge is missing,
stale, blacked out, malformed, or concurrently replaced, no new complete map is
published. A previously published map is also rejected by `read_current()` as
soon as any of its authority moves or expires.

### 4. One reconciliation coordinator and pure planning

`ScaleReconcileCoordinator` is the single consumer for autoscaler work. It
coalesces notifications with a monotonic in-process generation, compares the
generation before waiting, and performs a bounded five-second recovery reread
even if an in-process notification is lost. The controller rereads durable
state on every pass. Provider calls and slow manager actions run without the
coordinator condition lock or the actuation-generation lock.

The controller takes a short optimistic actuation generation before planning
and revalidates it before each mutation. An update moves that generation to an
odd transition value, so stale work cannot publish into the successor runtime.

Once the durable gate is `SEQUENCED_ACTIVE`, the controller enters
`Autoscaler.sequenced_reserved_fill_planning()`. The existing autoscaler still
computes ordinary demand, scale-down shelter, and the legacy status projection,
but it emits no legacy fill launch and does not spend feed or advance rotation.
A missing or unreadable authenticated map means zero new fill for that pass;
there is no fallback.

`ReservedFillPlanner` is database- and provider-free. From one immutable map it
computes deterministic, exact pool/card intents after applying:

- service-global `max_replicas` headroom in the configured physical or logical
  capacity unit;
- ordinary demand debits;
- durable nonterminal fill rows from the same allocation map; and
- the last receipt-proven rotation anchor.

Planning mutates no feed, fairness cursor, or replica state. Its deterministic
idempotency key is correlation and replay-debit evidence; the database-assigned
replica row and returned receipt remain the commit boundary.

### 5. Concurrent multi-cluster preflight and commit receipt

`SkyPilotReplicaManager.accept_reserved_fill()` validates the typed plan and
manager/service owner before provider admission. It then acquires one fenced
provider phase and starts one physical-UID capture thread per distinct
`(Kubernetes context, physical UID)` pair. Independent contexts initialize in
parallel. A same-context initializer already in progress returns typed
backpressure instead of blocking the whole wave. The preflight deadline is 45
seconds for the whole batch, measured from one shared absolute deadline, and is
unrelated to `kubernetes.provision_timeout`. Per-context waits and thread joins
must consume only the remaining batch budget.

After all distinct-pool preflights report, one manager critical section
acquires the global demand-capacity reservation lock and revalidates service
ownership, current version, service-global headroom, pool epochs, observation
expiry, and physical identities. Intents are admitted in plan order while both
locks remain held through the existing protocol-v2 replica-row transaction.
That transaction independently revalidates the ordinary admission generation,
gate, allocation identity, round provenance, fresh observation, claim topology,
and owner before assigning the total zero-cost admission sequence. The
in-process lock order is manager then demand reservation; provider preflight
holds neither. Inside PostgreSQL, the zero-cost event sequencer is acquired
before lifecycle/service, round, claim, and replica rows, matching ordinary
admission and launch-result writers.

`FillCommitResult` is a bijective receipt that accounts for every planned
intent exactly once as accepted or deferred. A pool-local identity or preflight
failure produces a sparse receipt and does not starve healthy independent
contexts; a service-global owner, version, sequence, headroom, or provider-phase
failure defers the remaining ordered tail. Each accepted entry names its intent
hash, durable replica ID, generalized launch association ID, and exact API
request ID. All four identities were committed in the same admission
transaction. The controller advances pool rotation only from
durably accepted rows. If authority remains current while any intent is
deferred, the controller immediately coalesces another reconciliation pass.

This avoids `N * provision_timeout` cluster initialization. For a 200-intent
wave across two Kubernetes clusters, the two physical-cluster captures begin in
parallel, then the manager persists every independently admissible intent and
returns an exact sparse receipt. Receipt acceptance proves durable replica-row
admission, not provider completion or readiness. Launch workers and the existing
request machinery proceed asynchronously. Readiness may still be
gradual because Kubernetes scheduling, image pulls, setup, model loading, the
provider phase, and executor capacity are real limits; the feature does not
claim all 200 become ready at once.

### 6. BCL reclaim invariant

Reserved fill remains zero-cost-only and uses the server/workspace-owned
preemptible inference placement. Canonical worker placement projection protocol
v4 is the single owner for new admission; v1/v2/v3 are retained readers only.
Each candidate adds `projection_version: 4`, the closed `provision_timeout` and
scratch contracts, and either `kueue_admission: null` or the exact closed
mapping `{local_queue_name, workload_priority_class_name}`. Namespace, service
account, Pod PriorityClass name/value/preemption policy, accelerator scheduling,
LocalQueue, WorkloadPriorityClass, scheduling timeout, and scratch are frozen
together when the service version is committed. `require_managed` is derived
from non-null Kueue admission; it is not separately caller-selectable.

Protocol v4 also owns the scheduler and actual binding seam. The immutable
candidate freezes `scheduler_name` from only the server-owned context/workspace
Pod configuration, defaults it to `default-scheduler`, and binds it through the
candidate digest and typed reclaim-policy view. Final rendering removes any
caller/restored `spec.nodeName` and installs exactly the projected scheduler.
The create response and a still-gated Pod must remain unbound; an admitted or
post-wait bound Pod is freshly joined to its exact Node, whose projected
accelerator label key/value must match the immutable candidate. Frozen affinity
without this bound-Node proof is not sufficient reclaim or capacity evidence
because direct `nodeName` binding bypasses the scheduler.

The LocalQueue is resolved from the service workspace's server-owned
`kubernetes.kueue.local_queue_name`/`kubernetes.quota.queue`. The
WorkloadPriorityClass is resolved only from the new server-owned
`serve_worker_kueue_workload_priority_class_name`. A managed queue without that
class, a class without a queue, or request-owned `resources.priority_class`,
`kubernetes.kueue`, or `kubernetes.quota.queue` makes a projected version or
launch fail closed. The full validated candidate has one deterministic
canonical JSON SHA-256 digest. Mutable launch-time configuration is never
reread for a projected worker.

The reclaim boundary is the typed, deployment-attested admission authority,
not Pod priority alone. PHX uses `KUEUE`: a Kubernetes PriorityClass at `-1000`
with `preemptionPolicy: Never` cannot by itself make a Kueue-gated BCL gang
reclaim an unmanaged inference Pod, because Kueue may refuse to admit the
higher-priority workload. East uses `KUBERNETES_SCHEDULER`: its queue identities
must both be null and the deployment policy must instead prove the exact
reviewed `gpu-binpack-scheduler` Deployment, namespace, service account, Pod
priority/preemption tuple, accelerator selectors, and bound-Node result. A
null queue projection therefore selects a second closed admission authority;
it is not an implicit downgrade to unverified Pod priority. The PHX failure
mode remains the root cause recorded in
`docs/designs/kubernetes-kueue-fail-closed-pods.md`.

Activation therefore requires deployment evidence for every reserved
inference context that:

- the inference namespace has an active LocalQueue selected by server-owned
  SkyPilot configuration;
- its ClusterQueue admits that namespace and shares a reviewed preemption
  domain with BCL/research workloads;
- lower inference workload priority and higher BCL/research priority are
  enforced in that domain;
- SkyPilot's strict plain-Pod path can read the queue objects and fails closed
  unless the admission response attests `managed=true`, the exact queue, and
  the Kueue scheduling gate; and
- the final Pod boundary strips caller-supplied Kueue state, reasserts the
  server-owned queue and priority, and installs the admission scheduling gate
  before `CREATE`, so a missing or bypassed Kueue mutation leaves the Pod
  unschedulable and causes synchronous rejection and cleanup.

PHX's shared queue-name policy and its partition Pod policy remain
platform-owned defense for legacy and non-SkyPilot clients. They are not
reserved-fill launch authority. The strict SkyPilot path already proves the
stronger contract on the exact Pod: it checks the create response inside the
provider mutation epoch, then fresh-reads the same UID after admission and
requires the exact queue outputs, managed finalizer, PodSet, scheduler, bound
Node, and accelerator identity before publishing provisioning success. A
ValidatingAdmissionPolicy read is an earlier, weaker snapshot and cannot
strengthen that proof; making it mandatory only lets unrelated policy or RBAC
drift stop otherwise safe fill.

Policy-bundle schema v5 therefore removes admission-policy and binding names
from `kueue_enforcement`, performs no ValidatingAdmissionPolicy reads, and
retains the exact LocalQueue/ClusterQueue, controller/config, Pod webhooks, TAS
feature gates, priority, flavor, immutable projection, and per-Pod lifecycle
proofs. Because this changes provider-inventory semantics, v5 advances that
section's hash domain from `provider/v3` to `provider/v4`; the unchanged fleet
section remains on `fleet/v4`. `boltz-l4-fleet` creates direct core/v1 Pods and
has no KubeRay, HPTO, or shared research-policy runtime dependency. Platform
may evolve those defenses independently without changing fleet authority.

The evidence and enforcement boundary is one
`ReservedFillReclaimPolicy`. It is a code-owned, typed deployment extension,
not an environment variable, operator boolean, or JSON assertion. Python
entry-point group `skypilot.reserved_fill_reclaim_policy` must resolve exactly
one implementation; zero or multiple implementations fail closed. The generic
distribution intentionally installs none.

The required interface contract is
`GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2`. It supersedes the
worktree-only V1 contract, whose scope did not prove immutable Kueue admission.
V1 may remain parseable solely for an explicit historical diagnostic, but it
is rejected by activation, reauthorization, claim, and launch validation. The
activation evidence hash uses schema 2 and includes this exact V2 enum. An old
policy cannot become activation-ready merely by echoing a newly extended Python
scope: it must return V2 evidence under a correspondingly reviewed policy
identity and full-fleet bundle. Any already active experimental generation
would require normal fix-forward reauthorization, which advances the gate and
invalidates its allocation maps.

### 7. Provider-free external load-balancer route projection

Endpoint and accelerator resolution remains provider-bearing work in the
replica manager's ordinary readiness-probe round. The external load balancer
and API paths do not repeat it: they read no Kubernetes object, provider phase,
cluster handle, endpoint resolver, or process-local provider-derived route
cache.

After applying a complete probe round, the manager classifies every current
replica under its bounded manager lock as routable with exact URL/accelerator,
intentionally off-route, or fail-closed with a typed reason. A provider-phase
deferral, owner transition, incomplete fleet read, missing version/profile
contract, or unclassified row aborts publication; it never publishes a partial
or spuriously empty route set. Provider calls and waits have already completed
before this short classification/publication boundary.

The semantic projection is ordered by `(replica_id, replica_record_id)` and
binds service version, row version, normalized URL, accelerator/count,
zero-cost provenance, asynchronous occupancy, system-recovery marker, service
owner epoch, binding epoch, and capability-cohort epoch. URL collisions fail
closed for every colliding record. Canonical semantic and wire digests are
validated on every read. One transaction inserts an immutable generation,
advances the service head only for a semantic change, and prunes history to a
bounded eight generations.

Serve047 creates the projection head/generation tables in the same additive
transition that generalizes the existing launch association. It is not another
Serve revision or authority path. `read_current()` joins the current service
owner, cohort, head, and immutable generation and returns nothing on mismatch.
A replacement controller therefore starts cold and fails closed until its own
first complete publication; it does not wait for association reconciliation.
A warm LB may retain its last already-applied coherent snapshot during that
bounded gap, while a cold LB remains unready.

Each route client is bound to exact `(replica_id, replica_record_id)` identity.
Reusing a URL for a successor drains the predecessor client and creates a new
identity-bound client. Demand and occupancy reports echo projection generation
and record identity; stale evidence cannot affect a successor. Raw URLs remain
only in the existing identity-validated drain report. URL-to-replica caches are
deleted.

The HA role channel uses that same immutable projection as promotion authority.
The LB applies routing URLs, service version, route-source epoch, projection
generation, digest, and the removed-client drain overlay under one lock, then
echoes the coherent fence in both role and durable-demand reports. A replacement
controller never relies on its empty process-local occupancy cache: on every
candidate heartbeat it reads the current route source and exact fresh immutable
snapshot in one PostgreSQL transaction, derives the expected asynchronous URLs
from the snapshot, exact-matches the LB's reported routing URL set, and requires
a fresh generation-valid sample for every expected asynchronous URL. Extra
coherent samples for off-ready draining URLs are permitted; omission of a
currently routed URL is not. Legacy route mode alone retains the transitional
process-local contract. The LB treats `(service version, route generation,
route digest, route-source epoch)` as part of its occupancy observation
context. Any change advances the existing sample epoch and clears exported
proof before the new route fence is visible; a probe crossing that transition
is rejected at completion. Pending reservations remain conservative, an
identical head renewal preserves current proof, and a successor record reusing
the same URL cannot inherit old local capacity or idle evidence.

Promotion is fail closed without disturbing the healthy selected slot. Missing,
malformed, stale, corrupt, or owner-mismatched route evidence returns a normal
role heartbeat with `promotable=false`; it does not move the Service selector.
The STABLE-to-PREPARING transaction locks the service row before rechecking that
the same generation/digest is still the fresh route head. A semantic head
advance rejects the CAS and the next LB sync/role round retries; an unchanged
head renewal is accepted. The one-way route-source promotion takes the same
service-row lock and, for HA services, is permitted only in STABLE; it cannot
race a legacy decision into PREPARING or MIGRATING. Bounded rollout telemetry
exposes only the promotion
reason enum, route mode/version/epoch/generation, expected/reported/fresh/missing
backend counts, planned-upgrade decision, transition-attempted bit, and CAS
result. It never exports URLs, route digests, tokens, Pod IDs, or request IDs.

The promotion read is one fail-fast shared owner-lock statement plus one indexed
PostgreSQL head/generation join. It copies the immutable row and releases the
lock before bounded canonical decoding; the cutover CAS revalidates the exact
head. Tests pin that two-statement shape and instrument provider/Kubernetes
adapters to require zero calls, complete owner replacement, exact successor
isolation, and one poisoned launch association among hundreds without blocking
fresh route publication for healthy rows. The generous regression ceilings are
one cold 840-route read within ten seconds and 100 subsequent reads with p99
below one second; they are not permission to add another in-process cache.

### 7a. Controller takeover of promoted capacity authority

Controller incarnation is part of the durable demand, route, and zero-cost
capability fences, not process metadata. The one canonical takeover transaction
therefore locks the lifecycle and service rows, compare-and-swaps the exact
previous controller incarnation and owner epoch, and installs the replacement
incarnation on generalized launch authority. If demand is already
`DURABLE_FEED`, that same transaction re-advertises both demand and zero-cost
capability under the replacement incarnation. It does not change
`demand_source_mode`, `demand_source_epoch`,
`reserved_fill_actuation_mode`, or `reserved_fill_actuation_epoch`; takeover is
neither a second promotion nor a demotion. Takeover accepts only the complete
`LEGACY_CONTROLLER`/`DIRECT_REPLICA` or
`DURABLE_FEED`/`DURABLE_INTENT` pair. Either asymmetric state is rejected and
the owner transfer rolls back. The deprecated live repair surface may finish a
`DURABLE_FEED`/`DIRECT_REPLICA` transition only while the current controller
authority is still intact; a replacement controller never adopts or
re-advertises that partial state.

Immediately before a Helm upgrade containing this takeover contract, the
operator must read every non-pool service row in the deployment database and
prove that the count of either asymmetric pair is zero. An earlier audit is not
sufficient while the deprecated separate promotion surfaces remain deployed.
If `DURABLE_FEED`/`DIRECT_REPLICA` is found and its current controller authority
is intact, finish the existing atomic fix-forward promotion before the upgrade.
If current authority cannot be proven, recover the old binary/controller first.
Do not repair an asymmetric pair with manual row mutation. The Helm apply is
blocked until a fresh deployment-wide read proves only complete legacy or
complete durable pairs.

The takeover also marks predecessor `GRANTED`, `ACTUATING`, and `RETRYABLE`
zero-cost intents terminal before commit. Those grants have no materialized
replica row and cannot be adopted safely because their existing controller
fingerprint does not include the UUID incarnation and a PID/IP/port can be
reused. `COMMITTED` intents are not changed: their exact replica rows and any
generalized launch associations remain under the existing recovery contract.
An unpersisted predecessor plan has no row to terminalize, so durable grant
admission also supplies the calling manager's immutable controller incarnation
and owner epoch and compares both with the already locked service row. A
replacement that happens to reuse the complete transport fingerprint therefore
cannot make an old manager's in-memory plan authoritative.
Provider effects covered by the service launch-authority guard continue to
exclude takeover. An uncommitted intent-to-replica handoff separately orders on
the locked service row: if takeover wins, the intent becomes terminal and the
blocked handoff fails its authority check, rolling back the replica insert.

Route capability is deliberately not rebound by owner transfer. Existing route
leases are revoked in the same transaction, and the replacement must publish a
new owner-bound immutable route generation. Still-fresh predecessor reports
name the previous route generation/digest and remain display-only. The durable
demand reader, capacity-plan publisher, paid-claim validator, and zero-cost
planner all fail closed until a current authoritative LB session publishes a
fresh complete protocol-v2 report naming the replacement route generation,
digest, and unchanged route-source epoch. Thus the safe recovery interval is
underfill: neither paid nor zero-cost provider actuation can use stale demand.

The owner-incarnation/owner-epoch compare-and-swap decides concurrent takeover.
The winner rebinds the complete capability pair and revokes the old route in one
commit; a blocked or concurrent loser observes the advanced owner fence and
changes nothing. A later supervised restart repeats the same transaction with a
new incarnation and repairs a demand capability left stale by an older binary,
again without advancing either source epoch.

### Boltz deployment policy bundle

Boltz implements this interface in the separate
`boltz-skypilot-reserved-fill-reclaim-policy` distribution under
`boltz/reserved_fill_reclaim_policy/`. The generic SkyPilot wheel remains
entry-point-free. The Boltz overlay builds and installs the generic wheel and
the deployment-policy wheel independently, then verifies that the combined
image exposes exactly one `skypilot.reserved_fill_reclaim_policy` entry point.
The overlay release stamps only the policy distribution's artifact version.
The package owns an explicit, independently reviewed policy-contract revision,
which advances only when executable authorization or enforcement semantics
change. Ordinary SkyPilot changes therefore do not manufacture a new reclaim
authority. Any policy-code change must advance that revision; fleet and
provider-contract changes already rotate their domain-separated digests.

The package embeds one strict JSON fleet bundle. Unknown or duplicate keys are
rejected. Its normalized semantic sections are hashed independently with
domain-separated SHA-256 prefixes: the admission/reclaim contract produces
`fleet_bundle_sha256`, while physical/provider inventory produces
`provider_inventory_sha256`. Reordering contexts, flavors, or quota rows does
not rotate either identity. The package caches only temporary AssumeRole
credentials; it never caches Kubernetes or AWS attestation results.

The initial public inference contract is service-name-agnostic. A second
service, including one with traffic weight 1000, uses the same claim and launch
path if its immutable projection matches this bundle. No service allowlist or
second scheduling path exists. The exact shared object contract is:

| Contract | East | PHX |
|---|---|---|
| Context | `prod_research_cluster_eks` | `phx_research_cluster_eks` |
| Physical cluster UID | `14de98b4-cb7b-4f82-beb7-6f754a96f1dd` | `ba2dcdca-2a0d-447f-ad8a-31849a63c1d5` |
| Namespace / service account | `rescluster-k8s-prod-east1-preemptible-inference` / `skypilot-pool-sa` | same |
| Pod Identity | absent | absent |
| Kueue admission | absent; east remains ordinary Pod scheduling | LocalQueue `be` -> ClusterQueue `skypilot-be`, WorkloadPriorityClass `be-ls` for the `mt_hybrid` serving workspace |
| Pod PriorityClass (spoke-module-owned) | `rescluster-k8s-prod-east1-preemptible-inference-low`, value -1000, `Never` | same |
| Scheduler / topology authority | exact `gpu-binpack-scheduler` Deployment | Kubernetes `default-scheduler`; Kueue v0.19 TAS owns admission topology and no custom scheduler Deployment is permitted |
| GPU resource | `nvidia.com/gpu` | `nvidia.com/gpu` |
| Exact worker accelerator scheduling | `A100`: `nvidia.com/gpu.product=NVIDIA-A100-SXM4-40GB`, `nvidia.com/gpu`; `A100-80GB`: `nvidia.com/gpu.product=NVIDIA-A100-SXM4-80GB`, `nvidia.com/gpu` | `H200`: `nvidia.com/gpu.product=NVIDIA-H200`, `nvidia.com/gpu` |

The PHX best-effort ClusterQueue has zero nominal GPU quota in the same
`shared-pool` cohort as the research queue. Its exact live preemption tuple is
`borrowWithinCohort: Never`, `reclaimWithinCohort: LowerPriority`, and
`withinClusterQueue: LowerPriority`; the research queue retains
`reclaimWithinCohort: Any`. East has no Kueue admission pair and must not be
forced through a nonexistent queue. Bundle schema v5 retains schema v4's one
nullable `kueue_admission` object per fleet context and matching nullable
`kueue_enforcement` object per provider context. Both objects must be null or
both non-null. Null selects the exact custom-scheduler reclaim authority and
performs no Kueue reads; the typed projection must also carry
`KUBERNETES_SCHEDULER`, null queue identities, and the reviewed scheduler name.
It does not silently downgrade reclaim to Pod priority. The PHX pair binds
`be`, `skypilot-be`, and `be-ls` together. Schema v5 also retains schema v4's
nullable provider custom-scheduler Deployment, exact Kueue TAS feature gates,
and ResourceFlavor topology-name fields. Fleet `scheduler_name` remains a
required string because it is part of the immutable worker projection; it is
`default-scheduler` for PHX and the custom deployment name for east.
`ResourceFlavor.spec.topologyName` is exact provider inventory, not an
authority discriminator: a provider-owned flavor may retain that field while
an inference namespace remains outside Kueue. The nullable admission and
enforcement pair plus the scheduler contract select the sole placement path.
Schema v5 removes the redundant ValidatingAdmissionPolicy snapshot from that
enforcement object; strict Pod preparation and synchronous/fresh lifecycle
attestation remain the sole per-Pod runtime admission proof.
The policy proves the exact LocalQueue target and current Active ClusterQueues
when the pair is non-null, plus cohort, namespace selectors, GPU flavor quotas,
preemption policies, provider-owned ResourceFlavor instance selectors,
WorkloadPriorityClass name/value, Pod PriorityClass, and the context's one
scheduler/topology mode. For east it attests the immutable custom scheduler
Deployment and both provider-owned ResourceFlavors' live
`topologyName: hyperpod`; this inventory binding does not enable Kueue
admission or controller reads there. For PHX it rejects a custom scheduler,
requires projected `default-scheduler`, binds the H200 ResourceFlavor's
`topologyName: hyperpod`,
and attests the current Kueue controller, Pod integration,
`AssignQueueLabelsForPods: true`, `TopologyAwareScheduling: true`, and the
reviewed TAS replacement/multilayer feature gates. It does not infer Pod
admission from a webhook configuration name:
for both the mutating and validating configurations it requires exactly one
named Pod webhook with the reviewed core/v1 Pod operations, Kueue service
name/namespace/path/port, nonempty CA bundle, admission review version,
selectors, side effects, timeout, match and failure policies, plus the
mutating reinvocation policy. Any missing rule or endpoint drift fails
activation and every later policy check. HyperPod remains the sole owner of its
ResourceFlavors: the attestor proves each flavor's exact provider-owned
instance selector, then cross-binds that selector to the GPU product label and
`nvidia.com/gpu` capacity on the current physical cluster's Nodes. It requires
at least one non-deleting Node for every reviewed flavor and rejects every
non-deleting Node of that shape whose product or GPU capacity differs. Node
readiness and allocatable occupancy remain physical-observation inputs, so a
temporarily initializing Node is not misclassified as policy drift. The
inference namespace UID and physical cluster UID are immutable inventory;
replace either only by shipping a new bundle and normal fix-forward
reauthorization.

AWS absence is a positive proof, not an omitted check. For each context the
plugin uses the hub writer's Pod Identity session to assume the exact spoke
roles `skypilot-rf-b6ca6363ec70-audit` in east and
`skypilot-rf-fe7c6c421c88-audit` in PHX. The spoke module derives these
collision-resistant identities from the exact cluster and partition. Each
role is read-only and limited
to `eks:DescribeCluster`, `eks:ListPodIdentityAssociations`, and
`eks:DescribePodIdentityAssociation` on the exact cluster and its association
resources. The spoke trust names the single current hub Pod Identity writer
role, permits only `sts:AssumeRole` plus the required `sts:TagSession`, and
requires its transitive `eks-cluster-arn`, `kubernetes-namespace`, and
`kubernetes-service-account` session tags. The chart renders API, controller,
and executor writers with the same `skypilot-api-sa`; a chart test must keep
that invariant true. Every proof describes the exact active EKS cluster,
paginates the filtered association index with cycle detection, and requires
zero associations for the public inference service account. A non-null future
bundle instead requires exactly one summary plus one exact described
association, including role, null target role, ARN, and owner agreement. The
Kubernetes proof independently rejects an IRSA annotation on the service
account.

Activation runs both provider domains for both contexts concurrently under the
caller's single absolute five-second deadline. Claim authorization does the
same for every distinct requested context, and launch authorization runs AWS
and Kubernetes concurrently for its one context. Static identity, pool-key,
projection, accelerator, and admission mismatches fail before provider I/O.
All network calls use the remaining deadline, one-attempt client retries, and a
shared cancellation event. Raw provider payloads and credential material never
enter errors or proof output.

Launch authorization shares only a short-lived provider proof, never an exact
launch scope or a permission to create a replica. Serve054 adds the
PostgreSQL-only `serve_reserved_fill_reclaim_provider_proofs` table. A fresh
random 256-bit `receipt_nonce` is its primary key. The exact-authority tuple of
reconciliation-gate generation, all three immutable policy identity fields,
and Kubernetes context is unique. Its SHA-256 is computed only as the
advisory-lock ID and is not persisted. The table and API are launch-proof
specific, so they carry no fixed-value purpose or provider-domain
discriminator. Each row otherwise carries only proof schema version, one safe
JSONB object containing the exact `aws` and `kubernetes` summaries, its
canonical SHA-256, and database-clock `completed_at`. There is no persisted
generation or expiry. A new authority row or a refresh whose proof schema,
canonical digest, or JSONB content changed receives a fresh nonce. A refresh of
the identical canonical proof retains the existing nonce and advances only the
proof payload/completion fields. Freshness remains derived as
`completed_at + 5 seconds > clock_timestamp()`. Delete/reinsert of the same
authority and payload still receives a new nonce and therefore cannot match an
older ticket; an in-place identical renewal remains the same external fact and
does not revoke sibling launch references.

The application accepts contexts of at most 1,024 UTF-8 bytes and canonical
proof JSON of at most 32 KiB with at most 32 nested container levels.
PostgreSQL independently limits the rendered JSONB value to 64 KiB; that
deliberate headroom accounts for JSONB text spacing and normalization rather
than claiming its byte representation is identical to compact canonical JSON.
The database also rejects malformed identities, nonces, schema versions, and
digests. The table has no
service foreign key because the provider fact is context-wide; service,
replica, accelerator, projection, and pool authority remain outside it and are
revalidated separately. Proof payloads contain only the existing safe
summaries and never AWS credentials, EKS bearer tokens, kubeconfigs, or raw
provider responses. Central state has no SQLite implementation or fallback.

After all existing local identity, projection, admission, accelerator, and
pool-key checks pass, one deadline-watched worker constructs one repository and
derived Serve engine. Every network-capable construction, database, and
provider operation remains inside that watched work; the handler main thread
performs no proof-path network I/O. The worker opens one dedicated `NullPool`
transaction, reads the exact context-authority row plus PostgreSQL
`clock_timestamp()`, and uses a fresh canonical row immediately. On a missing,
expired, malformed, or semantically inexact row, that same transaction makes
exactly one nonblocking `pg_try_advisory_xact_lock` attempt using the authority
hash. A loser rolls back and physically closes before polling. A winner rereads
the exact authority under its transaction lock to close the read/election race.
Before that reread or any provider work, it transaction-locally sets and
verifies PostgreSQL `application_name = skypilot-reclaim-proof-owner`; failure
to tag the elected phase rolls back and fails closed. It then
captures the database-clock proof-start anchor, runs the AWS and Kubernetes
provider reads concurrently under the same outer deadline, validates the exact
complete pair, and performs one authority-tuple
`INSERT ... ON CONFLICT DO UPDATE`. The conflict update retains the incumbent
nonce only when proof schema, canonical digest, and JSONB payload all exactly
match; otherwise it installs the newly generated nonce. It decodes
`RETURNING`, exact-checks the published payload, digest, completion, and
terminal reserve, commits, and physically closes before the receipt can
authorize its caller.
Transaction commit, rollback, connection loss, or process death releases the
lock atomically; no custom session-lock lifecycle, DBAPI facade,
prior-generation CAS, or unlock query remains. Receipt waits never consume or
retain an ordinary API/Serve `QueuePool` slot.
The provider deadline reserves the final 0.5 seconds of the outer horizon for
publication and physical session close. Independently, a receipt may be reused
or published to a caller only while at least 0.5 seconds remain before its
five-second expiry. The database read maps completion conservatively to the
caller's monotonic clock, and the live reserve check runs after payload
validation, transaction commit/rollback, and physical `NullPool` connection
close at the actual return boundary. If those steps consumed the reserve, the
caller re-enters election under its unchanged absolute deadline and never sees
the near-expiry receipt. That second reserve gives the caller time to enter the
terminal PostgreSQL guard; the guard still checks the full maximum age and
fails closed if the reserve was insufficient under actual contention.

The receipt-owned connection path is PostgreSQL-only, `NullPool`, and
instrumented under the bounded `reserved-fill-reclaim-proof` metric label. It
uses the distinct transaction-local `skypilot-reclaim-proof-owner` database
phase tag only while it owns the advisory transaction, making retained-owner
observations deterministic without changing pool identity or metric cardinality.
It
retains URL connection options, performs no generic database retry, permits
one libpq connection attempt with a one-second connect timeout, applies
200-millisecond server statement/lock limits and client socket send/receive
limits, and sets `idle_in_transaction_session_timeout` to 6 seconds, explicitly
above the five-second outer horizon while bounding a wedged provider-held
transaction. These limits reduce pressure and make normal database faults
prompt; they are not the absolute survivor boundary because synchronous DNS
and a lost commit response cannot be proven bounded by libpq.
The existing per-invocation `DisposableExecutor` is that boundary. At the
five-second outer deadline the policy sets cancellation, reports failure
without joining an uncooperative proof thread, and the existing inner warden
stops, kills, reaps, and proves absence of the exact handler family before its
typed `family_drained` result becomes visible. No new warden protocol or
long-lived execution path is introduced.

A caller that loses its one election attempt never joins an exclusive-lock
handoff convoy. It waits locally and makes a
bounded number of jittered, exponentially spaced `NullPool` receipt reads,
starting at 400 milliseconds and capped at one second. A
waiter may spend its remaining outer horizon on a final client-bounded read;
the post-read deadline check rejects a late result and `DisposableExecutor`
remains the hard stalled-session boundary. Loss of the elected transaction,
provider failure, or uncertain publication denies authorization to the current
invocation. A failure before commit leaves no newly published or usable
receipt; a prior stale or malformed row may remain but cannot authorize. A lost
commit acknowledgement may leave either no row or exactly one complete
canonical row. Partial receipt rows and partial launch authority are impossible,
and a later invocation may safely read that committed row or reprove it.
Already-observed losers fail closed at their original deadline and do not
re-elect while the elected proof is outstanding. The sole exception is an
observed completed receipt whose validation or connection-close handoff
consumed the required terminal reserve; that caller begins a new nonblocking
election attempt under the same original deadline rather than returning an
unusable ticket. This cannot create a lock-handoff convoy because the prior
transaction is physically closed and the new attempt still uses
`pg_try_advisory_xact_lock`. Because failures are deliberately not
persisted, a slow contemporaneous handler that has not yet attempted the lock
may become leader after the failed leader releases it; this is a later
independent attempt, not a waiter handoff. The existing durable actuation
intent/request retry remains the sole durable retry owner. Thus the contract is
one active leader per exact context authority, exactly one AWS call and one
Kubernetes call in a successful synchronized cold wave, no cached failure, no
retained waiter sessions, and no advisory-lock handoff convoy. The synchronized
90-process gate measures the instantaneous and total physical-open bounds;
`NullPool` alone is not a global connection cap.

Each provider future captures its local monotonic completion, and the typed
context candidate carries the older of the AWS and Kubernetes completions
outside the persisted payload. The repository maps that oldest completion from
the proof-start database/local clock anchor into `completed_at`. The receipt is
therefore never newer than either fact even when domain durations are
asymmetric, without discarding the complete five-second reuse horizon after the
older fact actually completed. Loss of the transaction makes publication fail.
Provider failures are not persisted or cached, and database, lock, clock,
payload, semantic, digest, or authority uncertainty fails closed without
falling back to an independent provider read.

Receipt readers conservatively map database age back to local monotonic time by
adding the full SQL round-trip to the database-reported age. Each caller then
mints its own exact `ReclaimLaunchAuthorization` carrying the one immutable
context receipt reference and its conservative completion time. The terminal
helper independently rejects a locally stale reference even if an identical
row has since been renewed. The final PostgreSQL launch-authority transaction
requires `READ COMMITTED` isolation and performs one ordinary MVCC `SELECT` by
nonce, with `clock_timestamp()` in that same statement. It requires the exact
policy/gate/context/schema/digest, database-clock freshness, and nested
Kubernetes physical UID immediately before provider I/O. A changed refresh or
delete committed before this statement is visible and rejects the old
reference. An uncommitted update leaves the prior committed row visible, so a
passing guard linearizes before that transition. This is the same authority
ordering the former share lock provided: that lock ended with the terminal
transaction before provider I/O and therefore never protected the effect, but
it did make valid identical renewals spuriously fail or time out. The
nonblocking MVCC read removes that false conflict. An identical committed
renewal retains the nonce; any changed committed proof rotates it and rejects
the older reference. The existing shared fleet-gate advisory guard remains
held across proof minting, terminal validation, and the provider effect. A
policy rotation, gate advance, context mismatch, changed proof, receipt ABA,
expiry, deletion, malformed payload, non-READ-COMMITTED transaction, or
database loss therefore rejects the launch. Distinct context proofs remain
parallel.

This deliberately reuses completed evidence inside, but never beyond, the
same five-second horizon already accepted for one launch ticket. It does not
mint a fill-plan capability. Durable intents can live for roughly 180 seconds
and can be delayed by per-pool leasing, API queueing, retries, and Kueue; a
five-second pre-fanout capability would routinely expire, while extending it
to the intent horizon would weaken external IAM/Kueue/scheduler-drift safety.
Refreshing the shared receipt at the terminal boundary preserves that safety
without a process-local second authority path.

The deployment preflight is machine-readable:

```bash
python -m boltz_reserved_fill_reclaim_policy
```

It prints exactly one JSON object. Successful preflight and the structured
activation, claim, and launch log payloads use schema 2 with `operation`,
`success`, the V2 `contract`, all three identity fields, completion time, and
one record per attested context. Each AWS record includes
`association_count`, `expected_role_arn`, and the explicit boolean
`identity_absence_proven`; each Kubernetes record includes the physical and
namespace UIDs, exact queue names, IRSA-absence result,
`assign_queue_labels_for_pods`, and per-flavor non-deleting Node counts plus
reviewed product/capacity. Failed CLI preflight returns exit 1 and only
`{"schema_version":2,"operation":"preflight","success":false,
"error_code":"ATTESTATION_FAILED"}`. Runtime activation and authorization
fail closed with `ReclaimAttestationError`.

Rollout is fix-forward. Apply and attest the IAM, namespace/service-account,
queue, priority, Kueue configuration, and server projection first; remove the
east unmanaged inference Pods and its drifted Pod Identity association before
activation. Then build one immutable two-wheel Boltz image,
deploy it to the complete writer fleet, run the JSON preflight, and invoke the
normal activation command. A correction ships a successor bundle/image and
uses the same reauthorization command to advance the generation; it does not
reopen legacy activation or introduce a rollback-only happy path.

The one interface owns three operations:

1. activation attestation enumerates the exact current durable claim edges and
   returns the immutable fleet-bundle, policy-revision, and provider-inventory
   identity under the V2 claim/admission/launch contract;
2. every sequenced complete claim-set replacement authorizes its exact
   normalized requested edges, committed service version, and typed projected
   admissions against the stored identity; and
3. every sequenced launch authorizes its exact service, claim generation,
   physical UID, service version, complete worker-projection digest, namespace,
   service account, projected scheduler, Pod-priority contract, LocalQueue,
   WorkloadPriorityClass, accelerator, and width against the identity carried
   by the durable launch fence.

The typed policy view is derived from the worker projection; it is not another
persisted projection schema. It includes the exact nullable Pod Identity role,
so a policy must verify positive identity and identity-free admission with the
same interface. A claim edge stores only its exact closed
accelerator-to-digest map beside its existing normalized identity. The full
source remains the immutable version row. Claim replacement locks that row and
recomputes every edge map before commit. A version or admission-only change
therefore changes the semantic hash, advances the claim generation, and
invalidates the prior allocation even when pool topology and capacity policy
are unchanged.

Provider and Kubernetes reads for activation and claims complete before the
broker and PostgreSQL row locks are acquired. A terminal launch first holds
its existing service/association authority, then acquires the shared
fleet-wide reclaim guard, verifies that exact guard session, and only then
mints the short-lived launch authorization. It verifies the fleet session
again after the bounded proof and revalidates the typed result through the
atomic row and generalized binding admission before provider I/O. Reserved
fill must carry the
`RESERVED_FILL/v1` bound-launch profile and exact authorization references. It
does not carry ordinary defaults or create a second association.
For built-in Kubernetes reserved fill, every provider-mutation factory call
acquires the service-owner guard, obtains the shared fleet guard, mints a fresh
deployment-policy ticket, revalidates exact durable authority, performs one
bounded mutation, and releases all three before any passive wait. Other
bound profiles and opaque provisioners retain their existing whole-call
service guard. The deployment policy call receives a new absolute five-second
monotonic deadline only after fleet-gate acquisition and must be
cancellation-aware; a result returned after that deadline is rejected before
terminal validation or mutation. The canonical launch order is therefore
service-shared, fleet-shared, context-proof transaction advisory lock, central
authority row locks, and provider effect. No path acquires the per-service
guard after the fleet-wide reclaim guard, so the order remains acyclic. The
fleet guard already requires one dedicated session per active provider effect;
this order extends that session's lifetime only across the bounded proof and
adds no third guard session. The returned typed authorization is short-lived
and exact-scope. The
claim transaction locks and revalidates the current gate generation and
identity plus the exact normalized edges, version row, projections, and
digests before persistence. Allocation publication and replica insertion
repeat the locked version/digest comparison. At launch, the executor reloads
the exact committed version projection, verifies the protocol-v2 digest carried
by the durable fence, and enters the service and fleet provider guards before
obtaining a fresh exact authorization. Inside those guards it revalidates the typed
authorization, durable launch fence, current claim edge, current version row,
and generation-bound gate identity before yielding to provider mutation. A
restarted executor with a missing plugin, a differently identified bundle, a
stale version or digest, or a partial fence cannot launch.

Kubernetes deploy-variable generation receives the selected persisted
projection. It takes the LocalQueue and WorkloadPriorityClass only from
`kueue_admission`, sets the provider's required-managed contract, and never
uses `resources.priority_class` for a projected worker. The post-merge and
legacy-YAML-restore enforcement step reasserts both the provider fields and the
exact Pod labels/priority fields. The provisioner continues to preflight the
projected LocalQueue and attest the admitted Pod; any mutation of queue,
WorkloadPriorityClass, namespace, service account, Pod priority, or accelerator
shape fails closed before workload execution.

The terminal reclaim guard covers each bounded Kubernetes compute-mutation
window, including provider-internal create retries that can submit the
Kueue-managed workload and immediate rejection cleanup for an admitted Pod
whose projected identity changed.  The built-in Kubernetes provisioner
receives the canonical provider-effect guard from the backend and enters it at
the per-Pod create/retry boundary for reserved fill; it does not
rely on an outer guard around the opaque bulk-provision call. Every normal,
AppArmor-retry, and 409 replacement create attempt reacquires
separately. Force-remove and rejected-identity delete/read attempts also
reacquire separately, with all retry sleeps outside the guard. Every successful
create response is checked
for the exact queue, WorkloadPriorityClass, admission scheduling gate,
namespace, service account, Pod priority, and accelerator shape before that
guarded call returns. Existing Pods are reattested against their current Kueue
lifecycle state: an admitted Pod may have had the gate removed, but must retain
the exact managed/queue/WorkloadPriorityClass labels, Kueue's managed finalizer,
`podset=role-hash`, and the exact LocalQueue/ClusterQueue outputs bound at
preflight. `AssignQueueLabelsForPods` is therefore a deployment prerequisite.
A label-only Pod without either the create-response gate or the complete
post-admission binding is rejected and deleted under fresh authority.

Passive scheduling and readiness waits on this reserved-fill built-in
Kubernetes path never hold the per-service or fleet-wide advisory guard. After
the wait, every Pod is fresh-read and its complete admitted identity reattested
in one new guard epoch before provisioning can return. The fresh object must
remain `Running` and retain the exact UID captured by the all-containers-
running observation; same-name replacement and still-gated objects fail
closed. In
particular, a correctly Kueue-pending Pod
with `provision_timeout: -1` may wait indefinitely without blocking a service
version mutation, controller takeover, or reclaim-policy reauthorization.
Any later provider mutation or retry must enter a fresh guard, obtain a fresh
policy authorization, and revalidate the durable fence. If authority changed
while a Pod was pending, the stale request cannot perform another create or
destructive retry; durable controller reconciliation owns eventual cleanup of
the old replica.  A failure after entering this instrumented path does not run
opaque request-owned teardown; it returns a terminal reserved-fill fence and
the durable replica owner performs exact cleanup. This mutation/wait split is
the one canonical reserved-fill built-in Kubernetes path, including
protocol-v2-fenced requests emitted while `LEGACY_ACTIVE`. Fence-less
historical requests remain on the opaque whole-call path. An opaque
protocol-v2 provisioner is rejected before provider mutation; only the in-tree
Kubernetes provisioner can produce the exact create/adopt attestation required
to enter the materialized tail.

Canonical projection protocol v4 additionally owns one closed base-runtime
readiness contract. Final rendering, after caller and restored YAML merging,
installs exactly one downward-API `SKYPILOT_POD_UID`, `restartPolicy: Never`,
and startup/readiness exec probes on the sole `ray-node` container. Before any
workspace or resource Pod-config merge, the renderer hashes the canonical
template's exact `ray-node` `command`, `args`, and `lifecycle` into one
server-owned SHA256 expectation. Final rendering rejects any merged change to
that producer and persists only the digest through the provisioner boundary.
The create response, adoption read, finalized Pod, admitted read, and final
fresh read must all reproduce the same digest; an injected `postStart`, changed
pre-stop lifecycle, alternate command, or replacement script therefore cannot
forge the readiness marker. Both probes require the contents of
`/tmp/skypilot-serve-worker-runtime-ready` to equal the current Pod UID. Under
fail-fast shell execution, the container clears every ready, setup-completion,
setup-failure, Ray-completion, and host-network-port marker that could be
inherited from an image or writable layer both before and after the trusted
server-owned `runcmd`, then starts the bootstrap producers. It atomically
publishes the UID marker only after the asynchronous SSH, environment, and
SkyPilot installation steps succeed and the generated Ray head start or worker
join returns successfully. The Ray head initialization loop captures both
output and exit status under fail-fast shell execution, retries only the
explicit `No cluster status.` initialization response, and propagates every
other nonzero `ray status` result; a failed status pipeline cannot become a
successful marker producer. It then connects to the final local sshd port
(including the dynamically selected host-network port) and requires an SSH
banner before publication. The startup probe runs every two seconds with 900
failures, bounding this in-container base bootstrap to 30 minutes after
container creation; the readiness probe continuously withdraws Ready if the
marker no longer matches.

`Running` is therefore only an intermediate observation for v4. The
provisioner captures the exact non-empty name/UID set, requires the ordinary
all-containers-running wait to return the same set, passively waits for Pod
`Ready=True` and `ray-node.ready=True`, and then repeats Ready, UID, and the
complete immutable projection attestation in the existing final guarded read
before publishing provider success. Deletion, missing Pods, a same-name UID
replacement, bootstrap timeout, or a fresh-read readiness regression fails
closed. A reserved-fill readiness wait that cannot prove success raises the
exact provider-present fence with the captured Pod UIDs; it cannot enter
request-owned cleanup or capacity failover before the bulk call returns.
Generic Kubernetes launches and historical projection protocols v1/v2/v3
retain their existing behavior. This marker proves only the base
SSH/environment/SkyPilot/Ray bootstrap; later workdir/file synchronization,
task setup, model loading, and application health keep their existing owners.
No database schema or projection-payload-shape migration is required: v4 uses
the same closed key set as v3 but a distinct discriminator and therefore a
distinct candidate digest. Protocol v3 deliberately retains its historical
Running-only behavior. The existing placement-projection capability handshake
must report exact protocol v4 across the complete API-server/controller/
provisioner cohort before a clean service version emits v4; old binaries reject
that discriminator before provider mutation instead of silently accepting a
mixed interpretation.

Pre-Pod auxiliary bootstrap and object-storage construction cannot occupy a
reserved accelerator slot and remain outside the reclaim guard. The successful
in-tree bulk/adoption return is the single one-way materialization boundary.
It is recorded before deploy-variable generation or any other local tail can
fail. From that point, every error is normalized to a terminal reserved-fill
fence: no capacity classification, cross-placement failover, or broad
request-owned teardown is permitted. Config-hash reuse is disabled for v2, so
an existing Pod re-enters the same current Kueue/projection adoption
attestation rather than inferring identity from cached configuration.

Post-Pod runtime preparation, internal file mounting, Ray/skylet startup,
workdir and file-mount synchronization, task setup, autostop/hook mutation,
port reconciliation, and job submission each run under a fresh bounded
service/policy/fleet guard. Passive Kueue scheduling and readiness waits remain
outside every guard. A missing guard fails closed; terminal cursor restoration
and other best-effort reporting cannot replace the typed materialized result.
Ordinary bound requests retain their existing authority path.

The asynchronous request boundary has one exact execution-quiescence protocol.
Every claimed invocation retains generation, claim token, worker instance,
outer-guardian Linux PID, and `/proc/<pid>/stat` process-start ticks until the
exact process-family boundary finishes effect-bearing handler code and cleanup
and publishes its receipt. API-request schema 010 has not been deployed, so
this identity has one universal meaning from its first rollout; no historical
schema-010 handler-PID rows exist. Lease expiry, controller handoff, signal
delivery, guardian absence, and process-pool failure revoke authority but are
not quiescence proof. They cannot make the request replayable. PID signalling
uses a pidfd and repeats the guardian's process-birth check, so PID reuse cannot
target another invocation. A schema-010 guardian PID disappearing without its
exact receipt remains fail closed; local PID absence never synthesizes family
quiescence.

The parent Future monitor is the durable receipt-delivery owner. The child
wrapper never writes an execution receipt: its return only reaches an inner
warden, and cancellation, retry, or failure can still require descendant
drain after that return. Durable PostgreSQL claims use one disposable
per-invocation execution path; reusable `ProcessPoolExecutor` workers and
forgotten broken-pool shutdown threads are removed. The retained
`BurstableExecutor` capacity interface owns one finite set of these
invocations, so a full lane leaves work durably queued instead of claiming an
unbounded hidden backlog.

Each invocation has a dedicated two-level process boundary: a minimal outer
guardian and an inner warden. Both become Linux child subreapers before the
inner warden spawns the handler as leader of a new session/process group. The
outer guardian PID and process-start ticks are published before handler
admission and remain the durable claim identity. Both owners are outside the
handler group, and bidirectional lifetime pipes make
each owner drain if the owner on the other side dies. If the inner warden is
hard-killed, its complete orphaned family, including children that called
`setsid()`, reparents to the per-invocation outer guardian instead of joining a
process-global orphan set. If the outer guardian is hard-killed, the inner
warden observes EOF and drains its family. API-parent death makes the outer
guardian drain. This per-invocation kernel ancestry boundary is canonical;
the API process is not used as a shared fallback subreaper because concurrent
families would become indistinguishable after reparenting.

The handler remains alive, or remains an unreaped zombie, as the exact family
root until descendant drain is complete. The direct-child guardian is not
reaped until its authenticated completion and durable receipt are accepted.
Every outcome, including normal success, terminates and reaps every descendant
before the handler root is released. A finite API invocation may not hand a
long-lived child to durable state: runtime daemons and managed-job controller
slots are the explicit runtime-owned abstractions for long-lived work. The
inner warden repeatedly terminates and reaps adopted descendants; the outer
guardian independently requires a stable empty family before it reports
completion and permits the exact handler to be reaped. Cancellation targets
the exact direct-child guardian, which treats `SIGTERM` as a drain request
rather than exiting. An unreadable
identity, a surviving child, or a termination timeout keeps the guardian,
warden, and claim unquiesced. Best-effort psutil enumeration is never receipt
proof. Graceful shutdown requests the same guardian-owned convergence protocol
instead of killing an untracked process. The parent Future becomes complete
only after the outer boundary reports typed outcome plus exact family absence;
only then may its monitor publish the first execution receipt.

The execution result is a closed typed outcome: `SUCCEEDED`, `PRE_EFFECT`,
`CANCELLED`, `RETRYABLE`, or `FAILED`. Every outcome requires the warden's and
guardian's complete descendant-absence proof. This turns arbitrary handler
threads, double forks, and `setsid()` descendants into one ownership boundary
instead of relying on a racy process-tree snapshot or a success-only child
handoff exception.

Every outer-boundary Future outcome proves both that the submitted callable
returned and that its required family drain completed. Transported wrapper or
result-serialization exceptions are closed typed outcomes and enter one
idempotent receipt loop with bounded backoff and no terminal give-up. The loop
ends only when the exact receipt is accepted or a database read proves that the
exact generation/token/worker identity no longer requires it. Abrupt guardian
or warden loss without the surviving peer's stable-empty proof remains
ambiguous and uses local family convergence; a cancelled-before-admission
invocation uses the claimed pre-effect proof below. Result monitors are
registered before their thread starts, removed only after durable convergence,
and joined after executor shutdown. Role ownership cannot be released while
one is still delivering a receipt.

Monitor setup is transactional: registration precedes `Thread.start()`, a
start failure runs the monitor synchronously, and an outermost `finally`
removes the exact registration. Executor startup likewise returns ownership
only after the queue server and every worker have started; any partial failure
stops and joins all earlier components before propagating. No effect owner can
exist outside the runtime's returned ownership aggregate.

Receipt delivery and outcome reconciliation are one parent-owned convergence
protocol, not independent fire-and-forget callbacks. A normal return preserves
the wrapper's already-fenced terminal result. `ExecutionRetryableError`
atomically consumes the exact parent-proven family result into the request's
`RUNNING -> WAITING` transition, clears the claim, and publishes one queue
delivery with a database-clock `available_at`. The transaction deliberately
ignores lease age: the exact generation, token, worker, live origin,
uncancelled row, and claimed queue delivery are its authority. The Future
monitor acknowledges the boundary and releases finite executor capacity
immediately after that handoff; it never sleeps while retaining a process
boundary. Any other transported callable exception is terminalized as the
exact claim's failure without overwriting a terminal child result, then
receives the same durable receipt.
Each database mutation is generation/token/worker fenced and idempotent, and a
transient failure retries the incomplete convergence rather than abandoning a
RUNNING row.

A locked claimed request in `PENDING` or `WAITING` with no PID has not crossed
the guarded `RUNNING` transition; revoking that exact generation is canonical
pre-effect proof and must atomically publish its exact generation-bound
quiescence receipt before cancellation, dispatcher failure, worker loss, or
shutdown can remove or terminalize its delivery. This applies uniformly to
ordinary and provider-reserved dispatch. A `RUNNING` request with a
nullable legacy API009 process identity remains ambiguous and fails closed.
For a terminal request whose family lost all receipt publishers after result
persistence, the result remains terminal but retention stays pinned; process
absence does not close it and no terminal request is reopened. Exact boundary
receipt closure accepts every replay policy, including `NEVER` and
provider-mutating handlers, because it cannot schedule an invocation; it is
also association-agnostic, so a bound ordinary launch can close retention
after persisting its immutable result. `READ_ONLY` and `RECONCILE` policy may
record retry intent after owner loss, but any execution-quiescence-required
claim becomes replayable only after the exact boundary receipt. Rows that
never entered this protocol may retain their existing policy recovery because
they have no admitted effect family.

There is intentionally no local PID-death observer or `/proc`-absence reducer.
It would have no authorizing fact once the recorded PID names the outer
guardian: abrupt guardian absence can precede the inner warden's drain. The
outer guardian, inner warden, and parent result monitor are the only receipt
publishers, and each may publish only after authenticated stable-empty proof.

Graceful role shutdown first stops all request dispatchers from claiming and
then converges every owned disposable boundary and receipt monitor. Executor
shutdown must return explicit per-guardian reaped or absent proof after its
receipt; a kill-helper timeout or a still-live boundary is a fail-stop result
and cannot be treated as drain completion. Controller shutdown first stops
runtime-daemon supervisors and proves their process groups absent; a generic
child kill must not race or replace the subsystem-specific supervisors. The
controller may release its leadership session, and an executor/all role may
release its instance lease, only after this sequence completes. Any timed-out
or failed supervisor, guardian, monitor, or queue join enters a not-Ready
convergence loop while retaining the leadership and instance sessions. It
retries authoritative drain and never exits merely to drop the PostgreSQL
session; only complete effect-owner absence permits ownership release.

API-request schema 010 adds the process-birth identity required by this
contract. Whole-Pod hard death of a finite API request intentionally remains
fail-closed: Kubernetes 404, force deletion, or same-name replacement does not
prove that containers on a partitioned node stopped, and an invocation is not
replayed across an unattested executable-image or handler-contract change.
Cross-Pod request replay is not implemented as a parallel happy path. A future
change may recover it only with a durable claim-bound executable contract and
authoritative effect-stop fence.

Perpetual controller maintenance loops are not finite API requests and must not
depend on an invocation receipt that can disappear with their owner Pod. The
steady state therefore removes every registered internal-daemon handler and
daemon queue submission path. The daemon specifications remain in
`sky.server.daemons`, but the elected controller runtime owns them directly:

1. after controller leadership is established, it retires every request and
   queue row whose ID is in the versioned historical-daemon allowlist before
   stale-claim fencing or generic request re-enqueue can decode or deliver one;
2. it evaluates each specification's `should_skip` predicate once for that
   leadership term and starts every selected daemon as a dedicated subprocess;
3. the subprocess starts a new Linux session/process group and enters through a
   `-S`, minimal standard-library launcher. Before importing any SkyPilot or
   handler module, the launcher receives the expected parent PID and
   process-start ticks, arms `PR_SET_PDEATHSIG`, re-reads both parent
   identities, becomes non-dumpable, and forks a minimal fail-stop guardian
   that kills the complete owned process group. The guardian immediately
   closes the capability transport and every inherited descriptor except its
   private control channel, reasserts non-dumpability, independently
   revalidates both process identities, and never installs controller
   authority. Only after that race-free
   effect-admission check does it restore the startup-captured clean server
   environment, load the executor plugin context, establish the system
   execution context, write the existing per-daemon log, and run the existing
   blocking event loop;
4. one runtime supervisor restarts an unexpectedly exited subprocess with
   bounded backoff, while cancellation sends `SIGTERM` to the owned process
   group, escalates to `SIGKILL` after a bound, reaps the exact child, and does
   not return until the process group no longer exists;
5. the minimal launcher makes parent death terminate the complete owned
   process group, including daemon grandchildren. A minimal guardian outside
   the daemon group and a launcher-side guardian-liveness monitor form a
   two-way fail-stop contract: supervisor/launcher death makes the guardian
   drain the group, while guardian death makes the launcher kill its own group.
   Neither an unmonitored guardian nor parent-death signalling of only the
   launcher is sufficient; and
6. controller shutdown cancels and joins those supervisors before releasing
   the outer leadership session, so a graceful leadership handoff cannot leave
   a locally owned daemon behind. A bounded join may escalate process-group
   termination, but failure to prove the group absent keeps leadership release
   fail closed.

The split controller's `ControllerLeaderLease` remains the outer fleet fence.
It sets the generation environment and completes legacy-row retirement before
stale-claim fencing. PostgreSQL `all` mode always uses the same lease,
regardless of whether managed-job consolidation is enabled, because its mixed
request queue can claim controller-class Serve and Jobs handlers even when no
fixed managed-job slot is active. It establishes that generation, opaque
origin capability, and loss monitor before it starts either execution class;
when managed-job consolidation is enabled, it also starts the fixed slots
before either class. It is a packaging compatibility mode, not a second
authority protocol. It attaches that exact generation to every
controller-class request claim; no PostgreSQL role admits a controller claim
whose generation is null. Normal-class requests remain outside this controller
generation fence. The combined process passes its owner explicitly through
the startup-maintenance and controller-claim boundaries rather than publishing
the generic controller identity process-wide to normal request work. Under one
legacy-daemon transition session it performs generation-fenced allowlist
retirement, fences nullable and prior-generation controller claims, and then
performs generation-fenced request recovery before starting any background
daemon, managed-job slot, or request worker. SQLite `all` mode uses the same
runtime interfaces
with one private owner-only authority file bound to an exact local PID and
process-start tick. Before any request recovery, decoding, re-enqueue, or
daemon startup, each mode retires the explicit historical IDs. Failure keeps
the role from serving. Each runtime daemon then uses its existing singleton
lock; local SQLite remains single-process and retires the same allowlist before
local recovery.
SkyServe and pool refresh retain
their existing, independently probed consolidation locks because those locks
fence controller recovery effects, not merely process scheduling. The other
maintenance events are overlap-tolerant by their existing resource locks or
idempotent database/telemetry operations. A PostgreSQL session can be released
server-side before its former owner observes the failure, so a singleton lock
alone is never described as proof that arbitrary old effects stopped.

The managed-job refresh loop and controller capacity are also explicit
controller-runtime ownership. A controller generation starts one fixed set of
runtime-owned `ManagedJobControllerSlotSupervisor`s. Each numbered slot owns
one local guardian handle and one disposable `ControllerManager` process
family; it starts at runtime admission, polls for work even when the queue is
empty, and is never created by an API request, `submit_jobs()`, a PID-file
scan, or another controller process. The slot count is computed once from the
existing controller parallelism policy at generation startup. Each manager
still multiplexes the existing bounded jobs-per-worker capacity, so the
topology remains bounded at the current 2,000-job fleet ceiling rather than
creating one process or request per job.

Managed-job schema 028 adds nullable `controller_slot_id` and
`controller_slot_attempt` columns plus a non-null
`controller_slot_quiescing` column with a database default of false. The
migration rejects an already adopted nullable quiescence shape instead of
silently retaining a schema weaker than the runtime invariant. A slot attempt
is a fresh UUID for each disposable manager birth. `WAITING -> LAUNCHING`
atomically stores the exact
`(controller_instance_id, controller_generation, controller_slot_id,
controller_slot_attempt)` tuple under the existing shared leadership-row lock.
Every controller-owned state transition, cleanup decision, and reservation of
a new provider effect compares that whole tuple. Controller-originated nested
API actions carry it as internal admission metadata. Normal service-account or
loopback authentication remains mandatory; a separate 256-bit opaque
controller capability authenticates the claimed outer origin, and only its
SHA-256 digest is durable. The elected controller runtime installs the raw
value in a PID-bound process-local registry, removes every raw/path environment
representation, and becomes non-dumpable before it spawns owned work. It
captures one canonical RequestWorker environment with the generic controller
pair and every managed-job owner/job/slot field removed. Controller-class
request handlers receive their durable outer pair only for local database
fencing; without an authenticated managed origin and process-local capability,
that pair emits no controller-origin SDK headers. Trusted runtime daemons
receive the nonsecret outer pair as explicit launcher arguments and the raw
capability through a fresh one-shot inherited pipe on every restart, install
both before plugin/daemon effects, and never recover them from the neutral
RequestWorker snapshot. The managed-runtime owner published by PostgreSQL
`all` mode alone does not authorize controller-origin SDK headers, even while
the process-local capability exists; a normal combined-process coroutine
therefore remains ordinary work. A complete managed attempt context or the
generic pair installed at a trusted daemon/controller boundary is required.
The runtime also passes the value explicitly to the slot
supervisor, which transports it to
each disposable manager through one-shot inherited pipes across both guardian
owners. Transfer handles are redeemed immediately by a non-dumpable boundary
owner; raw authority is then relayed only through close-on-exec descriptors, so
pre-admission cancellation cannot strand it in a parent resource sharer. The
manager starts through a `-S`, standard-library-only bootstrap, becomes
non-dumpable, consumes and closes its descriptor, and installs the same
PID-bound registry before enabling site packages or importing SkyPilot,
plugins, or lifecycle code. It removes all transport state before it can
execute a user event callback. Every
runtime-daemon birth or restart similarly runs through a `-S`,
standard-library-only bootstrap, receives a fresh one-shot descriptor and
explicit nonsecret outer owner pair, then becomes non-dumpable, consumes and
closes the descriptor, and installs process-local authority before enabling
site packages or importing `setproctitle`, SkyPilot, or plugins. It then
installs the outer pair before the first SkyPilot import.

A disposable request handler receives a fresh descriptor only when its queue
claim carries the complete, transactionally verified five-field managed-job
origin. The handler rechecks that tuple against the durable request, installs
it as bounded request context, and resets that context after execution. A
controller-class request without that origin receives no raw authority. An
exec child has no process-local registry, and neither the runtime,
guardian/warden, daemon, manager, handler, nor callback environment or argv
contains the raw capability. Registry clearing is PID-bound and exact-owner
scoped: a fork child fails closed, and cleanup never clears unrelated process
authority. This boundary isolates the new internal controller authority; it
does not claim that historical user or service credentials are absent from
callback environments. Caller-supplied
origin headers are stripped case-insensitively before server-owned values are
installed. The API persists
the complete five-field job/outer/slot tuple and accepts creation, queue claim,
and guarded `RUNNING` admission only while the live outer generation and exact
non-quiescing slot attempt own the job. PostgreSQL always locks outer
leadership, job, request, then queue in that order. A stale process can
therefore neither mutate durable job state nor ask the API tier to begin a new
provider effect after its slot is replaced. User cancellation remains its
separate durable intent path.

Each slot uses the same two-owner shape as a finite request: a local outer
guardian and inner subreaper warden surround the manager session. The runtime
does not publish, interpret, or compact a shared PID inventory: Linux PIDs,
process-start ticks, and guardian handles are Pod-local supervision evidence,
not cross-Pod authority. If a slot manager exits unexpectedly, its local
supervisor first converges and reaps that exact process family. It then closes
nested admission by setting `controller_slot_quiescing`, terminalizes
unadmitted deliveries with pre-effect receipts, cancels admitted deliveries,
and waits for every exact API guardian receipt. Only after both proofs may it
reset every non-`INACTIVE`, non-`DONE` job carrying the dead slot attempt,
rotate the attempt UUID, and start the replacement. A row whose task family is
already terminal returns to `WAITING` as cleanup-only lifecycle work; a row
with a nonterminal task returns to the ordinary execution path. Any uncertain
local or nested family drain keeps
the controller generation not Ready and retains leadership rather than
resetting or replacing the slot. A whole-Pod or outer-leadership loss is
recovered differently: the successor generation first closes every stale
exact nested origin and waits for its receipts, then resets every non-
`INACTIVE`, non-`DONE` prior-generation row and only then admits its fixed
slots. A terminal task family is again cleanup-only, never workload recovery.
Schema 010 and slot schema 028 are first-rollout additions. A fully nullable
pre-slot row may be adopted exactly once only after the successor owns fresh
outer leadership and a locked request-store query proves that no request row
for that job carries any managed-job origin. A partial job slot identity, a
partial nested-request origin, or any nested origin associated with that
nullable job is ambiguous and fails closed. No successor invents an origin,
interprets a foreign PID, or needs a shared PID inventory to do so.

Managed-job provider mutations have one path. Launch, recovery, cancel,
cluster teardown, pool cancellation, status confirmation, and ephemeral
storage deletion enter through SDK-created API requests while the exact
per-job context is active. The fixed-slot manager is the sole cleanup owner:
after exact stale-attempt quiescence, terminal task families are claimed as
cleanup-only work and run the ordinary complete manager cleanup without
constructing `JobController`, relaunching a workload, or changing its terminal
task outcome. Cleanup failure retains the exact claim and retries by phase;
`DONE` is an exact-attempt, non-quiescing, all-tasks-terminal commit after
cleanup and token revocation succeed. The outer refresh reconciler is
observation-only for every complete fixed-slot row and performs no provider or
storage effect. It retains only the narrow pre-slot PID terminalization needed
during the first image rollout and defers its resulting terminal row to the
same cleanup-only manager path. There is no refresh-owned or operator-owned
second cleanup authority.

Startup creates the refresh owner and every fixed slot transactionally before
the controller role becomes Ready or starts controller-class request workers.
Shutdown first prevents new refresh, slot, and request claims. The exact
refresh thread reaches effect quiescence while retaining its thread-local
consolidation lock; request boundaries and slot guardians then drain and join.
The main thread finally asks the refresh owner to release its own lock, joins
it, and only then releases outer controller leadership. Any bounded join may
report current failure, but subsequent convergence iterations re-run the
authoritative liveness and cleanup checks while retaining ownership. A cached
timeout, logged best-effort kill, PID-file deletion, or one-shot process-tree
snapshot is never a handoff boundary.

This is the sole managed-job controller happy path. The historical
`JOB_CONTROLLER_PID_PATH` inventory, `get_alive_controllers()`,
`start_controller()`, request-triggered `maybe_start_controllers()`, and their
polling/cutover machinery are deleted in the feature change rather than kept as
a compatibility branch. Rows written before slot schema 028 are nullable only
for migration. A new runtime always advances the outer generation before
recovery, applies the locked no-associated-origin proof above, handles each
eligible row once as stale lifecycle ownership, and never decodes it as a
current slot claim. The stacked cleanup removes this nullable adoption rule
after one complete fleet rollout and a database proof that no non-`INACTIVE`,
non-`DONE` pre-slot row remains.

Shutdown convergence is live and retryable. A bounded background/task join can
record its current failure, but the next convergence iteration must re-run
authoritative liveness and cleanup checks; it cannot replay a cached timeout as
permanent failure after the effect has disappeared. Every retry keeps the role
not Ready and retains the outer PostgreSQL ownership sessions.

The historical allowlist comprises the six daemon IDs in the current source
plus the retired `managed-job-status-refresh-daemon`; arbitrary user request
IDs ending in `-daemon` are never deleted. This is one execution path, not a
daemon-specific replay exception. Daemon
rows are excluded from API status, cancellation, shutdown, request retention,
and execution-quiescence logic because the new runtime never creates them. A
narrow legacy-row retirement helper is the only transition artifact. The
stacked cleanup removes that helper, the historical ID allowlist, recognizer
for historical supported-handler names with the `daemon:` prefix, and pickle
compatibility stubs after one full controller-fleet rollout and a PostgreSQL
query proves zero rows for every allowlisted ID for at least one
controller-instance stale window. Request IDs are never classified by a
`-daemon` suffix. Rolling back to a binary that recreates daemon requests is
unsupported;
operational recovery is fix-forward with a successor image.

Activation and active reauthorization similarly perform external attestation
first, then predicate-lock and revalidate the exact PostgreSQL claim scope
under the same-session broker and fleet locks before atomically binding the
complete evidence and writer receipt in the gate CAS. On first activation,
every PENDING or PROVISIONING row is locked and decoded. Non-fill rows are
ignored only after a current decode; every queued fill must be an exact current
ReplicaInfo v18 record whose scalar columns agree, service version equals the
locked committed version, projection digest matches the locked claim
admission, and policy tuple names the successor generation. Worktree-only v16,
a stale version or digest, a partial policy tuple, or any decode/proof failure
blocks activation. READY legacy rows remain readable. On active rotation,
old allocation maps are invalidated and the successor generation terminally
fences queued old requests. An activation-time census without the ongoing
claim and launch methods is insufficient. The deployment must preserve the
currently authorized policy identity and its external enforcement while
requests bearing that generation can execute.

First activation may attest a legacy-null claim version/digest pair by deriving
it from the locked current protocol-v2 version. Activation does not mutate the
claim. The canonical sequenced claim heartbeat must refresh that row with
Serve046 version and digest fields before schema-5 allocation publication can
resume.

Therefore the preparatory feature image may run at `LEGACY_ACTIVE`, but it
cannot activate `SEQUENCED_ACTIVE`: `activate` fails before broker-lock
acquisition or gate mutation when no unique plugin exists. Read-only `status`
continues to work. The Boltz deployment must ship the canonical policy and its
Kueue bundle before activation. This is an explicit open gate, not an operator
bypass or a claim that the current east1 or Phoenix topology is reclaimable.

Historical worker projection v1 rows remain readable only for ordinary launch
during the pre-activation transition. They cannot participate in a sequenced
claim, allocation, fill admission, or terminal launch. Protocol v2 and v3 rows
also remain exact historical readers, but no new version builder emits them.
After all active service versions are recommitted with protocol v4 and
production has remained `SEQUENCED_ACTIVE` through the documented cleanup gate,
stacked cleanup PR #1452 removes the v1 ordinary-launch decoder and its
transition tests. New writes always use v4; no compatibility setting can create
a v1, v2, or v3 projection.

When this external contract holds, a fill intent cannot spill to a paid
candidate, Kueue can evict lower-priority inference Workloads before admitting
BCL/research work, and SkyServe's existing preemption handling reconciles the
reclaimed replica away. A subsequent physical observation reduces free supply,
so no new map can spend a slot while BCL owns it.

`provision_timeout: -1` neither creates nor weakens the reclaim contract. It
only permits a correctly Kueue-managed inference Workload to remain pending.
This implementation launches no GPU or BCL canary. Existing deployment
evidence or a separately authorized Kueue rollout must satisfy the contract
before activation. If it does not, the new image may deploy but the gate stays
`LEGACY_ACTIVE`.

## What is implemented and what is not

### Present in the current worktree

- PostgreSQL Serve044 observation, sequencing, provenance, allocation, and
  fail-closed gate columns; Serve044 follows the upstream Serve043 placement-
  projection migration without rewriting historical migration authority.
  Forward-only Serve045 adds the complete generation-bound reclaim receipt and
  exact activation/reauthorization guard. Forward-only Serve046 adds committed
  service version and closed accelerator-to-projection digest maps.
- `reserved_fill_projection_authority.py` is the canonical adapter from one
  immutable worker projection to typed reclaim admission. New writes emit
  homogeneous explicit projection protocol v4; sequenced paths require
  non-null typed Kueue admission. Protocol v4 supports both an exact AWS role
  ARN and an explicit null identity contract, and the value is hash-bound and
  exposed to the deployment policy. Protocols v1/v2/v3 remain only as exact
  historical decoders; v1 ordinary-launch decoding remains pending cleanup PR
  #1452.
- API capability 77 exposes the placement-projection capability surface and
  the exact advertised current discriminator is v4; allocation-map schema 5
  binds service version and the closed digest map; ReplicaInfo v18 persists the
  selected scalar digest. Activation successor A reads only the six sanctioned
  legacy versions/eight exact pre-v17 shapes and the two exact v17 shapes long
  enough to produce the required normalization receipt. The closed readable
  version set is `3`, `6`, `7`, `12`, `13`, `14`, `17`, and `18`; v15,
  worktree-only v16, every other version, and every partial or extra-field
  shape fail closed.
- Concurrent provider-free pool observer and typed blackouts.
- Committed-observation broker rounds and complete authenticated maps.
- Ordinary zero-cost commit sequencing and complete v18 attribution.
- Lost-wakeup-free controller coordinator and optimistic actuation generation.
- Pure planner, autoscaler adapter, manager sparse receipt, and receipt-driven
  rotation.
- The generalized non-pool association/profile/cohort and atomic binding IDs
  described above are present in source and deployed dark. The canonical-
  birth correction is implemented in the current unmerged branch: a fresh
  eligible service commits generic `bound`, `DURABLE_PROJECTED`,
  `DURABLE_FEED`, and `DURABLE_INTENT` authority at epoch 1 under one
  incarnation, then verifies it before child spawn. The last verified
  production service authority remained legacy; recreation or retained-row
  promotion and fresh live readback remain open gates.
- Fleet transition CLI requiring protocol v2, Serve046, API010, an exact stable
  split-role `api`/`controller`/`executor` writer cohort on one immutable image
  digest, and one entry-point-loaded deployment reclaim policy. The same
  command reauthorizes active fix-forward generations. The generic build has
  no policy plugin and deliberately blocks authorization before broker lock or
  CAS.
- Provider-free reconciliation diagnostics derived from current authenticated
  schema-5 allocation, its exact durable observations, and exact-version/
  projection v18 replica rows.
- An exact v18 queued-effect proof at first activation and per-mutation
  Kubernetes guards with immediate create-response attestation, guard-free
  passive waits, and durable-owner cleanup after a terminal fence.

The Serve046 base above merged in source PR #1451, and the v18 live contract
plus normalizer merged in PR #1483. The former activation-successor A/B/C
language in the validation record below is historical: its source
prerequisites subsequently merged and the applicable dark components are
tracked by the phase table. It is not current deployment authority. The
remaining service-authority promotions, full-backfill application, and fresh
live readback stay open exactly as recorded in the phase table and gates.

### Runtime audit corrections implemented and frozen for review

The runtime audits found additional correctness and bounded-progress defects.
The current worktree now:

1. conservatively debits a target-less ordinary `SCALE_UP` against every
   compatible authenticated pool/card feed, clipped independently per feed;
2. uses one absolute deadline for the complete multi-context physical preflight
   batch, including cancellation, release, and thread joins; bounds provider
   root child-drain on exit; and keeps an incompatible provider phase closed
   out until any non-cooperative child actually releases;
3. propagates one cancellation-aware absolute deadline through Kubernetes
   client admission, fence capture, RPC timeouts, retries, and parsing
   checkpoints so timed-out work releases observer capacity; and
4. treats observation access context as authenticated acquisition provenance,
   while allocation consumption joins aliases by physical UID, pool key, and
   accelerator identity and independently revalidates each service-edge launch
   context; and
5. separates all-zero-cost row attribution from the global ordinary-demand
   invalidation generation, requires exact generation equality at allocation
   read and fill commit, and joins final fill persistence to the shared demand
   lock. This prevents a service-B ordinary commit from racing a service-A fill
   while allowing broker-disjoint fill commits to proceed independently;
6. snapshots an independent global first-success materialization sequence in
   every observation, stamps each zero-cost row transactionally on its first
   successful launch persistence, and uses admission plus materialization event
   order instead of wall-clock or readiness guesses for sequenced occupancy;
7. treats the complete grouped replica snapshot as part of sequenced spendable
   authority, rejecting the new round on enumeration, query, or decode failure
   rather than optimistically skipping an unread service; and
8. includes ordinary zero-cost rows owned by nonclaimant services in sequenced
   occupancy, with conservative same-card/same-width duplication until
   ordinary placement persists physical-cluster UID attribution;
9. observes raw exact-card GPU counts independent of claimant width, converts
   them exactly once under the broker's authenticated deterministic width, and
   publishes the width as allocation and diagnostic provenance;
10. treats same-context heterogeneous cards as disjoint physical UID/card
    edges with deterministic unique positions, while still rejecting a
    duplicate exact-card edge or conflicting exact-card alias width;
11. keeps last-proven UID topology across transient discovery failures and
    tries authenticated context aliases under one fair bounded query deadline,
    persisting only the winning route; and
12. initializes distinct physical captures in parallel outside the manager
    lock, retains each successful capture through every join-only persistence
    seam, rechecks pending service versions before and at the row transaction,
    and returns sparse receipts for pool-local failures; and
13. makes same-context exact-card observers join one UID initializer, rejects
    case-folded provider-card collisions, and preserves permission-denied
    evidence instead of intermittently blacking out a sibling card as generic
    provider failure; and
14. replaces the activation-only reclaim boundary with one uniquely loaded
    policy whose immutable identity is bound by Serve045, whose current
    service version/projection is bound by Serve046, and which is enforced at
    each sequenced claim transaction and terminal provider launch, while a
    complete successor receipt supports active fix-forward reauthorization;
    and
15. makes the final sequenced replica insert the durable capacity-spend
    boundary: under the locked current service specification and sorted
    replica rows it rejects service-wide intent replay, enforces physical
    aggregate/per-card feed, and enforces the physical-or-logical
    `max_replicas` unit before advancing the admission sequence; and
16. binds Serve046 service version and projection-v2 Kueue identity through
    claims, schema-5 maps, v18 rows, activation, status, and terminal launch,
    while splitting built-in Kubernetes mutation guards from passive `-1`
    scheduling/readiness waits; and
17. retains exact finite-request generation/token/worker/guardian-PID/process-
    birth identity until real process-family quiescence, routes pre-effect
    revocation through the sole replay reducer, fences every signal against PID
    reuse, preserves terminal results, and leaves guardian or whole-Pod death
    without a receipt fail-closed instead of treating deletion or lease age as
    process-stop evidence; and
18. removes perpetual controller daemons from the API request/replay protocol,
    retires legacy daemon rows before request recovery, and supervises one
    parent-death-fenced subprocess per selected daemon under controller/runtime
    singleton ownership with bounded termination and exact child reaping; and
19. closes Kubernetes direct-binding as a parallel placement path by removing
    projected `nodeName`, freezing and attesting the exact server-owned
    scheduler, rejecting webhook or gated-Pod binding, and requiring a fresh
    exact bound-Node accelerator-label join before admitted adoption or
    post-wait success; and
20. closes finite-request liveness under graceful shutdown: every claimed
    PID-less pre-effect terminalization publishes an exact receipt, terminal
    results remain pinned until a boundary-authored receipt, and leadership or
    instance ownership remains held until guardians and durable Future-receipt
    monitors are quiescent; and
21. replaces reusable/untracked request-process ownership with one disposable
    per-invocation outer-guardian/inner-warden subreaper boundary, publishes no
    receipt until its exact family is absent, makes startup and monitor
    registration transactional, makes daemon guardians mutually fail-stop,
    and replaces managed-job PID-file spawning with fixed runtime-owned slots
    whose generation/slot-attempt fence covers every state and provider-effect
    boundary and whose exact families participate in the same
    retry-until-proven-absent ownership handoff; and
22. makes deployment attestation use the same physical-UID/card atom as the
    broker so east's A100-40 and A100-80 edges may share one context without
    duplicating capacity, binds activation and later claim replacement to one
    shared atom validator, and binds both Kueue Pod webhooks to their exact
    reviewed rules and service endpoints rather than trusting object names;
    and
23. carries the normalized accelerator label key, sorted label values, and
    resource key in every typed reclaim admission, requires their exact
    code-owned match at activation, claim, and terminal launch, and makes the
    east A100-40 and A100-80 bundle contracts disjoint.

Regression tests exist for corrections 1--23, including owner-death,
request-liveness, legacy-row retirement, runtime-daemon supervision,
scheduler/bound-Node admission, capability bootstrap, and fixed managed-job
slot ownership. The complete non-PostgreSQL matrix, changed-source lint, and
Terraform module tests pass. The exact serial PostgreSQL freeze and corrected
deployment-policy matrix pass, and all three restarted consecutive adversarial
reviews passed without findings.

The audit also proved that BCL reclaim is a deployment prerequisite rather
than a Pod-priority property. A read-only audit on 2026-08-13 found that both
east and PHX lacked the historical `default` /
`skyserve-inference-borrowed` / `skyserve-inference-low` objects. PHX has since
adopted the exact `be` -> `skypilot-be` contract and `be-ls`/`be-lt` workload
priorities; east intentionally remains outside Kueue. Both retain the exact Pod
PriorityClass at value -1000 with `preemptionPolicy: Never`, and
`skypilot-pool-sa` is the exact worker service account. PHX's `skypilot-be`
queue has zero nominal quota and borrowing limits of 512 GPUs, 12100 CPU,
120Ti memory, and 2048 EFA; its paired `hyperpod-ns-research-clusterqueue`
also has zero nominal H200 quota and a 512-GPU borrowing limit. Reclaim is
therefore proved by the shared cohort, the research queue's
`reclaimWithinCohort: Any`, and the exact priority ordering rather than by the
superseded assumption that one research ClusterQueue owns all nominal quota.
The inference service accounts have no IRSA annotation. PHX has no Pod
Identity association. The 2026-08-18 production preflight corrected an earlier
inventory mistake: east association `a-rsvzwdtaesxvxorkh` to
`research-dropzone-irsa` belongs to the exact inference namespace and
`skypilot-pool-sa`, not the separate research partition. The model path does
not consume that identity; the canonical identity-free contract removes the
stale association and continues to require zero exact matches. The plugin
filters and paginates by the exact projected namespace and service account, so
unrelated research identities remain outside this proof.

HyperPod's live ResourceFlavors are provider-owned and intentionally omit GPU
product labels. East exposes 8 non-deleting `ml.p4d.24xlarge` Nodes with 8
A100-40GB GPUs each and 33 `ml.p4de.24xlarge` Nodes with 8 A100-80GB GPUs each;
PHX exposes 64 `ml.p5e.48xlarge` Nodes with 8 H200 GPUs each. The east
ResourceFlavor selectors are the live beta/stable/HyperPod labels for p4d and
beta/HyperPod labels for p4de, and both east flavors carry
`topologyName: hyperpod`; PHX has beta/stable/HyperPod labels for p5e with the
same topology name.
The queue-name and partition Pod policies remain deployed platform defenses in
PHX but are outside fleet authority. Schema v5 performs no admission-policy
reads in either context. The unmanaged east context performs no Kueue
controller or webhook reads; its null admission/enforcement pair selects the
independently attested custom-scheduler path. PHX must pass the complete
code-owned v0.19 Kueue controller/webhook contract through deployment preflight
before activation.
The 2026-08-18 read
found two ready Kueue controller replicas, the required Pod integration and TAS
feature gates, a `hyperpod` topology on `ml.p5e.48xlarge`, and no
`gpu-binpack-scheduler` Deployment. The absence is intentional: platform PRs
#8524/#8526/#8527 made Kueue TAS plus the Kubernetes `default-scheduler` the
single PHX admission and placement path. Schema v5 retains schema v4's
prohibition on a custom scheduler Deployment in a managed TAS context, binds
the projected scheduler name to `default-scheduler`, binds the ResourceFlavor
`topologyName`, and attests the required controller feature gates. It retains
schema v3's exact custom-scheduler Deployment proof for unmanaged east. A
managed context with a custom scheduler, or an unmanaged context without its
configured scheduler, fails closed; dual placement paths are not supported.

The audit role intentionally needs no read of the `Topology` object itself.
The exact `ResourceFlavor.spec.topologyName` in every context and the exact PHX
Kueue controller feature-gate set are the durable inventory/admission
contract; node/flavor reads continue
to prove physical capacity and accelerator identity. The `hyperpod` Topology
levels are verified as a deployment preflight/readback. This keeps the
existing Terraform module pin and avoids broadening ongoing controller RBAC.
The current hub writer is forbidden from reading the required Namespace,
ServiceAccount, queue, priority, flavor, Node, scheduler, controller, and
Kueue config/webhook objects in both spokes. That is intentional. The
deployment policy must read them through the exact audit role and EKS group
instead. These remain platform IAM/RBAC and object gates; SkyPilot core does
not duplicate their ownership or widen the writer role.

### Status actually exposed

The public server API version is 77. Reserved-fill reconciliation status keeps
its independent minimum capability at 76; immutable worker placement
projection requires API 77 plus projection protocol 2. These are distinct from
the PostgreSQL API-request schema revision 010 required for activation. The
controller's `/autoscaler/info` response always contains a nested
`reserved_fill_reconciliation` object. The user-facing service status copies
the same object when `with_target_num_replicas` is requested.

The stable top-level fields are:

| Field | Meaning |
|---|---|
| `enabled` | Whether reserved-capacity fill is enabled for this service. |
| `authority_mode` | `disabled`, `legacy`, `sequenced`, or `unavailable`. Once selected, `sequenced` remains visible even if its map is missing or stale; diagnostics never imply a legacy fallback. |
| `allocation_current` | Whether a complete authenticated allocation map is current. |
| `allocation_generation` | Current map generation, or `null`. |
| `allocation_input_sha256` | Current map content hash, or `null`. |
| `allocation_claim_generation` | Claim-set generation authenticated by the current map, or `null`. |
| `reconciliation_gate_generation` | Current sequenced gate generation authenticated by the map, or `null`. |
| `reclaim_policy_identity` | The three durable reclaim-policy identity fields when sequenced, or `null`; this is metadata, not permission to launch. |
| `pools` | Mapping keyed by canonical physical pool key. |

Each pool in a current allocation exposes:

| Field | Meaning |
|---|---|
| `physical_cluster_uid` | Physical Kubernetes identity fenced by the allocation. |
| `kubernetes_context` | Access context carried separately from physical identity. |
| `service_generation` | Broker service generation in the authenticated edge. |
| `observation_generation` | Exact durable observation generation used by the map. |
| `observation_sequence` | Exact global zero-cost sequence at observation start. |
| `observation_valid_until` | Conservative authority expiry timestamp. |
| `observation_available` | Whether the exact durable observation payload was available to the diagnostic read. |
| `broker_slot_width` | Authenticated GPU width used by the broker's one and only raw-GPU-to-slot conversion. |
| `observed_free_gpus` and `observed_free_gpus_by_accelerator` | Raw physical supply from that exact observation; `null` if the optional diagnostic read fails. |
| `observed_free_slots` and `observed_free_slots_by_accelerator` | Diagnostic conversion of that raw observation using `broker_slot_width`; this is not additional authority. |
| `spendable_slots` and `spendable_slots_by_accelerator` | Broker-published feed in the current allocation, bounded and split before planner replay debits; it is neither a live provider total nor a guaranteed remaining tail. |
| `grant` and `edge_cap` | Authenticated service allocation bounds. |
| `current_allocation_admitted_replicas` | Nonterminal reserved-fill rows carrying the exact schema-5 allocation identity, service version, selected-card worker-projection digest, observation provenance, typed intent, and positive database admission sequence; `null` if the optional progress read fails. This is not total pool or service holdings. |
| `current_allocation_ready_replicas` | Ready subset of those exact current-allocation rows; `null` if the optional progress read fails. This is not total pool or service readiness. |

The projection never queries a provider and never authorizes a launch. Failure
to inspect the reconciliation selector yields `authority_mode: unavailable`.
Failure of an optional exact-observation or replica-progress read preserves
`authority_mode: sequenced` and the current allocation metadata while returning
`false`/`null` for the unavailable detail. The endpoint remains healthy in
either case.

## Implementation phases and intentional departures

| Phase | Scope | State |
|---|---|---|
| 0 | Historical multi-pool protocol v2, UID fences, claims, grants, and zero-cost-only launch seam | Already present before this correction. |
| 1 | Observation ledger, admission sequence, authenticated map, coordinator, pure planner, manager receipt, diagnostics, and Serve045/046 reclaim-policy identity | Merged in source PR #1451. Its prior freeze/reviews are historical evidence, not a pass for the current stack. |
| 1b | PR #1483 precursor: replica state v18 plus its one-shot normalizer | Merged and published as 1.1.1277, but activation-ineligible because it lacks A's pre-activation contracts. |
| 1c | Exact-shape read bridge for the live v3/v6/v7/v12/v13/v14 JSON inventory | Merged in PR #1492 and published as v1.1.1284; removable only after the v18 normalization receipt. Its rollout is `LEGACY_ACTIVE` only. |
| 1d | Generalized binding, demand/route projection, and G1S execution-termination evidence through API014/Serve050 | Merged and deployed. Route authority is live at `DURABLE_PROJECTED` epoch 1; ordinary binding and demand remain legacy. |
| 1e | Canonical birth for fresh lifecycle-fenced PostgreSQL non-pool services | Implemented on `feature/canonical-fresh-service-birth`, not merged or deployed. `add_service()` uses the existing Serve047-allowed adjacent update inside the service/version transaction, so PostgreSQL exposes only the final generic bound/route/demand/fill tuple. `_start()` verifies rather than promotes it. No schema, EFS, Helm, Terraform/Terragrunt, provider, pool, or SQLite change is included. |
| 2a | Policy-bundle schema v3 plus exact live PHX queue/service-account contract | Merged in PR #1529 and deployed in revision 418; superseded in place by schema v4 because PHX intentionally replaced its custom scheduler with Kueue TAS. |
| 2a.1 | Policy-bundle schema v4, PHX Kueue TAS/default-scheduler contract, exact spoke audit roles, and PostgreSQL-backed server-config transaction | Merged and deployed through release 1.1.1332 / platform configuration; server config is corrected, but version 61 retained the pre-correction PHX scheduler projection. |
| 2a.2 | Isolated audit-role Kubernetes authentication and exact east identity-free inventory | Merged and deployed through revision 429 / release 1.1.1336. East passes. PHX now explicitly enables `AssignQueueLabelsForPods=true`; a clean current platform plan is empty. The successful full two-context preflight remains gated by 2a.4. |
| 2a.3 | Global activation scope with per-service duplicate-pool validation | Revision 431 preflight exposed that the deployment policy incorrectly treated two services sharing one broker pool/card as a duplicate claim. The fix groups activation claims by service, retains same-service duplicate rejection, and permits the documented cross-service sharing before one fleet-wide provider attestation. |
| 2a.4 | Remove redundant admission-policy authority from reserved fill | Platform PR #8649 is superseded and must not change the shared KubeRay/HPTO policy for fleet activation. Policy-bundle schema v5 removes ValidatingAdmissionPolicy and binding reads while retaining the stronger exact Kueue controller/webhook and synchronous/fresh Pod lifecycle proof. Activation requires only the SkyPilot fix-forward deployment and a fresh full-fleet preflight; no Terraform or platform Helm change is part of this gate. |
| 2b | New immutable service version with task-owned Kubernetes overrides removed, `min_replicas: 0`, and exact non-null worker projections | Version 63 is committed, elected, and controller-applied at lifecycle epoch 82 on known-good image `v3.682.2-boltz-2`; it is the clean demand-gated activation successor. Version 62 remains rejected historical projection evidence. |
| 2b.1 | UID-bound base-runtime readiness for canonical projected Kubernetes workers | Local implementation candidate on `fix/serve-projected-worker-runtime-readiness`; the focused render, source-composition, template-owned bootstrap-digest, typed Kubernetes-model, mutation, bounded-wait, UID-replacement, SSH-listener-failure, and final fresh-read tests pass locally. It is not merged, deployed, or production-proven. Generic Kubernetes and projection v1/v2/v3 remain unchanged. |
| 2c | P2c provider-independent route leases and safe zero-demand paid retirement (Serve051/API88) | PR #1531 is merged and deployed dark. PR #1532's exact-owner fix is deployed as revision 410 / v1.1.1314. PR #1533's immutable route-contract fix is deployed as revision 411 / v1.1.1315 and removes the shared routing-lock dependency. Production then exposed synchronous per-probe PostgreSQL receipt writes on the composition event loop. The bounded batch receipt-writer fix-forward and provider-stall qualification remain open. Historical cleanup #1506 is closed/superseded and reserves no head. |
| 2d | P2d grant-before-row per-pool actuation intents (Serve052) | Merged in PR #1537 and deployed dark within revision 418. Production activation and busy-lane/no-row evidence remain gates. |
| 2e | Atomic per-service durable-demand plus durable-actuation promotion | PR #1555 is merged and deployed dark in revision 431 / release 1.1.1338: one controller fence, routing linearization lock, and PostgreSQL transaction replace the two promotion requests. Draft cleanup PR #1556 removes both deprecated separate surfaces and the unsupported demand demotion after the documented production horizon. Activation remains gated by 2a.3, 2a.4, and a successful full-fleet re-attestation. |
| 2f | Promoted capacity-authority controller takeover | The fix-forward implementation is merged in PR #1562: the existing owner-transfer transaction preserves both one-way epochs, rebinds demand and zero-cost capability together, invalidates pre-row predecessor intents, and leaves route/report admission cold until fresh replacement evidence. No schema, chart, provider, or platform change is required. Deployment and child/Pod takeover qualification remain activation prerequisites. |
| 2g | Production full reserved backfill | Platform PR #8652 is merged, but its merged deployment workflow has not successfully completed and version 64 is not elected or activated at the last observation. A pre-merge attempt durably stored an equivalent version 64 after API acceptance and then failed before election; fresh live readback is required. The PR changes only the existing service policy and its validator/docs: production uses `utilization_gate: false`; test explicitly retains `true`. Complete the live workflow only after 2e/2f activation, changing one boolean relative to the clean demand-gated successor and preserving its known-good model image and immutable worker projections. |
| 2h | Atomic reserved-fill replica/request admission | Implementation candidate on `fix/serve-atomic-fill-admission`; production gates remain open. It uses the existing durable intent, generalized association, request/queue/pin, and reducer. One atomic-admission module owns the root PostgreSQL transaction and savepoint; the manager only prepares immutable server-local input before it and starts the returned request reducer immediately after commit. Serve055 adds the owner audit tuple, user FK, and retained-row one-shot transition. The durable body/digest uses the immutable audit name; execution resolves current `users.name` by tenant ID without an upsert. A request-ID-keyed PostgreSQL proof permits only current atomic reserved fill to bypass the owner's mutable workspace-membership check while retaining owner identity and all final provider fences. This feature already removes protocol-v2 direct/non-atomic admission and reserved-fill HTTP/system-identity/RWX correctness branches; it allocates no table or infrastructure. |
| 3 | Stacked Serve055 transition cleanup after the production horizon | The required but not-yet-authored `fix/serve-atomic-fill-admission-cleanup` branch adds Serve056 `NOT NULL` owner columns and removes only the application one-shot `NULL` attestation branch, the schema-derived temporary global user-deletion guard, and transition-only observability/tests. It retains or atomically simplifies the permanent database owner-immutability trigger. Its draft PR must be cross-linked before feature merge and remains blocked on a complete capable cohort, zero `NULL` tuples, no old writers, backups, and the complete stale/HA production horizon. Closed PRs #1506/#1510 are not revived; Serve054 belongs only to the provider-proof receipt. |

Durable acceptance atomically binds rows to the existing asynchronous launch
path through the generic non-pool handler, and
status projects the same allocation/observation evidence used by
reconciliation; neither is a second source of launch authority.

## Deployment, activation, and fix-forward reauthorization

The current executable sequence is the four-step direct-Helm rollout in the
Decision summary and phase table above. The A/B/C, split-topology, publisher,
and platform-PR sequences retained in the subsections below are historical
review evidence only. They must not be executed, used as merge gates, or
treated as deployment authority after revision 418.

### Serve054 one-way rollout boundary

Serve054 is additive, and applying it does not invalidate already-running
`1.1.1385` controller children or their ordinary traffic/fill transactions.
It does deliberately advance the single Serve Alembic head. An old controller
that restarts after that advance cannot re-adopt a service: its compiled
placement-normalization recognized-head set ends at Serve053, so adoption
fails closed. An image rollback to `1.1.1385` is therefore forbidden once the
migration job commits Serve054.

Deploy the successor image and migration in one direct Helm fix-forward
operation, permit only the bounded migration-first overlap, and require the
complete successor API/controller/executor cohort plus successful service
controller adoption before any activation or `boltz-l4-fleet` update. Existing
old children may serve during that bounded overlap, but no old restarted child
is treated as healthy adoption evidence. If the rollout stalls after Serve054,
repair and deploy another successor image; do not stamp the database backward,
run the forward-only downgrade, restore `1.1.1385`, or create a precursor
schema path. This is the already-established one-way Helm/fix-forward contract,
not a new migration topology.

### Serve055 service-owner rollout boundary

Serve055 is a PostgreSQL-only, forward-only additive migration layered directly
on Serve054. Applying it installs the immutable service-owner tuple described
above. A pre-Serve055 image does not recognize the new Alembic head and must not
be restarted or rolled back after the migration commits. The supported recovery
is another fix-forward image and direct Helm deployment; never downgrade or
stamp the Serve database, and never add EFS, Terraform, Kueue, or another Helm
resource to recover this state.

The central global user-state migration must create `users(id)` before
Serve055. Serve055 checks that prerequisite before its first DDL statement and
fails with an ordering error if it is absent or malformed; the failed attempt
must leave the Alembic revision at Serve054 with no owner columns, constraint,
function, trigger, or foreign key partially installed.

New services write the exact owner ID/name tuple atomically at creation. Before
atomic reserved-fill admission is activated for a retained service whose tuple
is `NULL`/`NULL`, its exact elected controller must attest the frozen
launch-time ID/name under the current service hash, lifecycle epoch,
incarnation, controller endpoint, and owner epoch. Both columns must be `NULL`
or both non-empty. A partial tuple, deleted owner, stale controller, or mismatch
with a prior attestation fails closed and is not automatically repaired. The
complete successor API/controller/executor cohort, successful controller
adoption, and retained-row attestation are required before updating
`boltz-l4-fleet` or enabling the atomic writer.

The owner ID is runtime tenant authority and has a foreign key to `users.id`
with `ON DELETE RESTRICT`. The attested owner name is immutable audit evidence
and the stable queued/digest value. Each bound executor resolves the current
non-empty `users.name` by that ID without upserting the queued name, so a later
display-name change neither breaks exact hydration nor gets reverted by a
launch. Serve055 rejects every non-internal user deletion while the schema can
still admit a `NULL` owner tuple. After the complete capable cohort, zero
`NULL` tuples, no old writers, database backups, and the full stale/HA horizon
are proven, the stacked Serve056 cleanup makes both columns `NOT NULL` and
deletes the global deletion guard, application one-shot attestation branch,
and transition-only artifacts. In that steady state, user deletion first
reports the names of owned services and the foreign key serializes the
concurrent delete-versus-service-create race. Serve056 retains or atomically
replaces the database trigger with permanent non-null tuple immutability and
proves direct SQL owner replacement still fails.

### Generalized binding prerequisite

G1 ships API011 and Serve047 readers/schema before writers, then converges all
API acceptors, request backends, queue executors, GC participants, possible
service controllers, and profile participants on one immutable generic-handler/
profile/receipt digest. Old ready and non-ready-recent leases drain through the
maximum stale/quiescence horizon. Initial reserved-fill activation is blocked
until the `RESERVED_FILL/v1` profile is in that cohort and every accepted fill
row atomically returns its association/request IDs.

Before per-service cutover, require zero unbound active request and zero
unbound `PENDING`/`PROVISIONING` row. Settle the seven incident rows from exact
provider/result evidence without fabricating a receipt. A remaining legacy
ambiguous row conservatively debits only its pool/card and cannot be retried.
Start provider probes, the reconciliation coordinator, and route publication
without a global recovery lock; schedule association repair per row.

For these pre-binding rows, Serve047's transition-only legacy-reconciliation
evidence table preserves the exact request and replica identity, an explicit
old-executor termination attestation, and a later physical-UID-scoped provider
observation. Only a later exact `ABSENT` observation can authorize cleanup; the
record cannot authorize a retry, fabricate request quiescence, or become a
synthetic association. The audit tombstone is retained when G2 removes the
transition writer.

API012/Serve048 next implement the durable demand feed, zero-cost-before-paid
placement fence, and provider-free route projection. G2/C is authored with G1
and that convergence but blocked. It owns API013 and Serve049, including the
final permanent reserved-fill authorization and combined-role cleanup that
earlier drafts assigned to API011/Serve047. API009--012 and Serve042--048
remain immutable. G2/C removes unbound non-pool launch/recovery, old handler aliases,
global startup recovery lock/backoff, cluster-name quiescence authority,
process-map authority, demotion after the rollback window, and transition-only
telemetry. After it lands, reserved fill and all other non-pool profiles use one
binding and fix forward.

### Preconditions

Before changing the gate:

1. Activation successor A must pass its independent security/contract review,
   CI, exact Python/Helm/Terraform suites, and deterministic generated-file
   checks before merge. After PR 17's unchanged-A normalization receipt, B/PR
   18, and C's publication, freeze every final source and platform head and run
   three consecutive pragmatic adversarial rounds before PR 19's final
   deployment/completion. Any material change resets that final sequence.
2. Merge and publish A through the old publisher as that publisher's final
   release. Amend platform PR 14 with one reviewed source, version, runtime
   digest, chart digest, module pin, API version, and structural proof. The
   historical 1.1.1277 tuple is forbidden here.
3. During PR 14, apply API-request schema 010 and the managed-job slot columns
   before A Pods become Ready. In the same Helm revision, convert one-pod `all`
   directly to exact 2/2/2 `api`/`controller`/`executor` on A and physically
   delete `all`. Let ordinary controller leader handoff drain old
   claims/processes; capture live values, render, Pods, and writer leases.
4. PR 14 must prove exactly six Ready same-digest A writers and no old writer;
   API capability 77, API-request 010, Serve 046, projection protocol 2,
   allocation schema 5, replica state 18, and reserved-fill protocol v2; the
   exact authenticated cold queue-capacity response; the identical annotation
   projection on all three roles; canonical ownership ledgers on every live
   generated inference Service; and the audit module's explicit queue targets.
   Authority remains `LEGACY_ACTIVE` and PR 14 does not run normalization.
5. Platform PR 17 runs A's normalization command and archives its accepted
   exact receipt while the PR 14 source, image, chart, module, values, Pods,
   topology, and Helm revision remain unchanged. It is an operation against A,
   not an A deployment or a second release.
6. Only after that receipt, merge publisher B. Its first automatic run must
   fail closed without publishing while the canonical role is absent. Platform
   PR 18 then adopts and hardens the Rainier runtime and Como chart
   registries/roles in separate account applies and readbacks. PR 18 creates no
   Helm revision and does not deploy A or change topology.
7. Deploy and attest the separately owned Kueue inference contract and the
   unique policy-bearing A image, converge the full split-role fleet on that
   image, and repeat the pre-activation proof. The generic A image cannot
   authorize activation.
8. Perform A's initial generation-fenced activation and non-compute manual
   verification. Keep A deployed until every cleanup-C runtime/removal horizon
   below passes, including `SEQUENCED_ACTIVE`, one controller restart, and one
   ordinary service update on the sequenced path.
9. Only after both PR 18 account readbacks and all cleanup-C runtime gates pass,
   merge cleanup C. C physically removes the normalizer, v17 live decoder,
   pickle column, and source topology/transition controls, while requiring B's
   superseded publisher paths to remain absent. The canonical roles publish one
   immutable C image/chart tuple and receipt.
10. Amend platform PR 19 with that exact C tuple and publication receipt. PR 19
    upgrades the already-split release in place, enables the typed worker-cache
    contract, and physically removes platform publisher/storage/topology
    transition paths. It must not create or replace the split topology. Invoke
    the same generation-fenced command to reauthorize the C fleet; no rollback
    or second activation path exists.
11. Prove every active reserved-fill service version has exact protocol-v2
   worker projections and every queued PENDING/PROVISIONING fill row is exact
   v18 bound to its locked service version, projection digest, and successor
   policy tuple. Drain any stale or undecodable row; no legacy fallback is
   permitted.
12. Prove the deployment-owned Kueue LocalQueue, ClusterQueue namespace
   selection, shared preemption domain, workload priorities, strict SkyPilot
   configuration, RBAC, and fail-closed admission contract for every reserved
   inference context. Pod priority alone does not pass this gate. Launch no GPU
   or BCL verification workload for this rollout.
13. Prove every split-role Pod runs the one immutable C image and the unique
    Boltz policy entry point resolves from its separately packaged wheel before
    C reauthorization. Later defects replace the complete fleet with a new
    immutable tuple and use that same generation-fenced command; no rollback or
    second policy/image path exists.

The live Helm release is the deployment authority. A platform repository pin,
Terraform/Terragrunt state, or open platform PR is neither a prerequisite nor
evidence of the deployed SkyPilot version. Before applying, capture `helm get
values` and `helm get manifest`, resolve the exact `api`, `controller`, and
`executor` Deployments, and review the rendered diff. Upgrade the existing
release with the repository chart and `--reuse-values`, setting the immutable
image for every role that has an explicit override. Any unintended namespace,
ingress, PVC, database, authentication, Secret, service-account, role-topology,
or other persistent-resource change blocks the apply. Post-deploy evidence is
the live Helm revision, immutable image digest, rollout state, schema status,
and non-compute health checks—not a repository tag or PR alone.

Revision 423 also established the required RWX writer substrate with the
encrypted, backup-enabled EFS claim `skypilot-state-rwx`. Authoritative
`.sky` state (excluding rebuildable client/catalog caches and obsolete central
SQLite request files) and `.ssh` state passed a zero-delta second rsync before
the 2/2/2 rollout. The inference route remained available through cutover.
However, selecting `existingClaim` caused Helm to delete the superseded
chart-owned RWO PVC and its EBS volume before a snapshot or Retain patch could
complete. That old volume is not a rollback path. Any future persistent-volume
migration must first make the source recoverable outside the release—an
accepted snapshot or a proven `helm.sh/resource-policy: keep`/Retain
transition—before a rendered release is allowed to stop owning the source
claim. This is a migration-safety correction; production recovery remains
fix-forward from the verified RWX copy.

The separately owned Kueue contract is a different deployment change. It may
proceed through its own reviewed authority only after the east and Phoenix
inference partitions, exact RBAC, server queue configuration, fail-closed
strict Pod lifecycle admission, and the code-owned policy plugin are
implemented and reviewed. It is not smuggled into the runtime-image Helm
upgrade and does not block
deploying the combined image safely at `LEGACY_ACTIVE`; it does block
activation.

### ReplicaInfo v18 expand-and-normalize gate

Production first exposed a v17 label collision: retained rows were labelled
current while some omitted the 13 sequenced-attribution keys introduced during
the Serve046 development sequence. A later read-only transaction on
2026-08-15 established that the live JSON inventory is broader. It returned no
payload values or row/service identifiers and found this exact version and
closed-key-shape census:

| Version | Rows | Exact top-level shape | Exact status shape |
|---:|---:|---|---|
| 3 | 1 | `B` | `S11` |
| 6 | 1 | `B + C` | `S19` |
| 7 | 128 | `B` | `S11` |
| 12 | 2,888 | `B + C + E` | `S19` |
| 13 | 385 | `B + C + E + R` | `S19` |
| 13 | 32 | `B + C + E + R + I3` | `S19` |
| 13 | 515 | `B + C + E + R + I4` | `S19` |
| 14 | 5,006 | `B + C + E + R + I4` | `S19` |
| 17 | 390 | Complete v17 including all 13 collision fields | `S19` |
| 17 | 1 | Complete non-collision v17 fields with all 13 collision fields absent | `S19` |

A separate read-only bounded preflight routed pre-v17 rows through the exact
v1.1.1276 legacy semantics followed by canonical v17 serialization and routed
v17 through the exact v1.1.1283 collision-aware branch; every result then
passed canonical v18 serialization. It passed all 9,347 retained rows. Across
all 8,956 pre-v17 rows it found zero recursive present-field value or JSON-type
mismatches, including nested `location` and `resources_override`, and produced
deterministic/idempotent round trips. All 3,809 pre-v17 `reserved_fill` rows
passed with zero present-field mismatches and no recovery quarantine. The one
v17 row with all 13 attribution fields absent passed only through v1.1.1283's
existing exact v17 collision branch, as intended. The pre-v17 bridge never
handles v17.

The symbols above are exact sets, not minimum-field descriptions:

- `B` is `replica_info_version`, `replica_id`, `cluster_name`, `version`,
  `replica_port`, `created_at`, `first_not_ready_time`,
  `first_consecutive_failure_time`, `status_property`, `is_spot`, `location`,
  `resources_override`, `reserved_fill`, and
  `cost_rebalance_for_replica_id`.
- `C` is `planned_capacity`, `unknown_capacity_replacement`, and
  `logical_bridge_capacity_verified`.
- `E` is `is_zero_cost` and `paid_capacity_pool_key`.
- `R` is `replica_record_id` plus all nine
  `system_recovery_*`/association fields:
  `system_recovery_launch_intent`, `system_recovery_disposition`,
  `launch_request_id`, `service_job_id`, `candidate_ready_observed_at`,
  `ordinary_release_not_before`, `system_recovery_revision`,
  `system_recovery`, and `system_recovery_quarantine`.
- `I3` is `reserved_fill_pool_key`,
  `reserved_fill_service_generation`, and
  `reserved_fill_physical_cluster_uid`. `I4` is `I3` plus
  `reserved_fill_kubernetes_context`.
- `S11` is `sky_launch_status`, `user_app_failed`, `service_ready_now`,
  `first_ready_time`, `sky_down_status`, `is_scale_down`, `preempted`,
  `purged`, `failed_spot_availability`, `drain_cap_seconds`, and
  `wait_for_idle_before_termination`.
- `S19` is `S11` plus `drain_started_at`,
  `logical_retirement_version`, `logical_retirement_controller_epoch`,
  `logical_retirement_generation`, `logical_retirement_target_capacity`,
  `logical_retirement_confirmed_generation`,
  `logical_retirement_bounded_deadline`, and
  `logical_retirement_committed`.

The pre-v17 inventory includes historical reserved-fill and zero-cost rows;
those markers and every present protocol-v2 pool-identity field are durable
facts, not defaults to erase. The bridge therefore restores the v1.1.1276
decode semantics only behind the exact eight sanctioned pre-v17 shapes in the
table, including that reader's frozen scalar, status, and resource
canonicalization. The bounded preflight proved that this canonicalization
changes no present field value or JSON type anywhere in the censused live
inventory. The bridge materializes only the version-appropriate conservative
defaults and immediately owns a complete v18 in-memory interface. The
operator-only normalizer applies the stronger invariant: it atomically refuses
any present-field delta before rewriting a row. A subsequent ordinary
persistence writes the one canonical closed v18 shape. In particular:

- pre-v13 rows get the deterministic transition record ID and ordinary system
  recovery defaults;
- v13/v14 rows preserve and validate their complete recovery bundle; malformed
  recovery state enters the existing absorbing off-route quarantine;
- absent capacity, economic, reserved-fill identity, allocation attribution,
  observation, admission, and materialization fields become only their
  historical `1`, `false`, or `null` defaults—no allocation authority,
  sequence, or cleanup identity is synthesized;
- the historical v13 `I3` form remains cleanup-safe through the existing rule
  that derives its missing explicit context from the immutable Kubernetes
  location, while a missing or contradictory location still fails closed; and
- legacy reserved-fill rows without complete sequenced attribution remain
  readable and cleanable but continue to fail the policy-bound admission
  validator, so they cannot authorize new sequenced capacity.

This is not generic `<17` compatibility. Versions outside
`{3, 6, 7, 12, 13, 14, 17, 18}`, a sanctioned version with any other
top-level or nested key set, malformed required state, and unknown/future
versions fail closed. Exact v17/v18 behavior remains unchanged: the
transitional v17 reader accepts only complete v17 or the one collision shape
with all 13 fields absent. The collision shape becomes 13 explicit `null`
values; a complete v17 value is preserved exactly. A partially missing
attribution bundle, unknown field, missing non-collision field, or incomplete
status subdocument fails closed.

The bridge may roll only while the reconciliation gate is proven
`LEGACY_ACTIVE`. Deploying it neither runs normalization nor changes the gate,
and no `SEQUENCED_ACTIVE` activation is allowed while any pre-v18 row remains.
For the ordinary FEV2 queue prerequisite, A may first replace the current
single `all` Pod in place and retain that exact topology. This narrow rollout
exists only to restore backward reads and expose A's authenticated queue
capacity. It must not run the normalizer, create the 2/2/2 role split, change
publisher or registry ownership, migrate storage, or invoke reserved-fill
activation; all of those remain later independently reviewed gates.
The Helm rollout must keep the chart's `Recreate` writer strategy and prove
that every old writer Pod has terminated before any bridge writer becomes
Ready; a mixed old/new API/controller/executor or compatibility-`all` writer
window is forbidden. Post-rollout inventory must show only the reviewed
immutable bridge digest and no old writer process or lease before ordinary
Serve mutation resumes.
It exists solely to let the current v18 writer read the retained fleet safely
until the source-owned atomic normalizer rewrites it. The already-required
normalization receipt must report zero invalid, noncurrent, and legacy-pickle
rows before activation. The stacked cleanup change removes the bridge for the
six sanctioned legacy versions/eight observed shapes (and later the existing
v17 collision reader at its established gate); it cannot merge until that
receipt and one full current-writer rollout prove no sanctioned legacy version
remains. A also stops all live pickle dual-writes; the nullable column remains
only until Serve047 drops it.

The first successful persistence by an A writer irreversibly converts that row
to v18. From that point a rollback to v1.1.1276 is forbidden because the old
runtime cannot read the v17-collision/v18 fleet safely. Any rollout defect is
fixed forward with another complete Recreate writer rollout; restoring an old
image is not a recovery path.

Platform PR 14 establishes the split topology directly on A and deletes `all`.
Only after exactly two `api`, two `controller`, and two `executor` Pods and
their six Pod-bound writer leases are Ready on the same immutable A digest,
with no old writer remaining, PR 17 runs the source-owned internal one-shot
operation from that exact unchanged API Deployment (replace `<namespace>` and
`<api-deployment>` with the reviewed live objects):

```bash
kubectl -n <namespace> exec deploy/<api-deployment> -c skypilot-api -- \
  python -m sky.serve.replica_record_normalization --json
```

There is deliberately no public API, SDK, CLI, feature flag, or alternate
normalization path. The operation proves the token-bound exact 2/2/2
API/controller/executor writer rollout before and during the cutover, requires
PostgreSQL at exact Serve schema revision 046, and takes the shared broker lock
plus an `ACCESS EXCLUSIVE` replicas-table lock. It validates every retained row
before the first update, including exact parity between JSON and every
JSON-derived query scalar (`status`, `sky_down_status`, `version`,
`cluster_name`, `created_at`, `is_spot`, and `paid_capacity_pool_key`). It does
not repair denormalized scalar authority. It rewrites all rows atomically
through the canonical serializer and requires every present legacy JSON field,
nested value, and exact JSON type to survive unchanged. For the six sanctioned
pre-v17 versions/eight observed shapes it may add only frozen
version-and-shape defaults, deterministic pre-v13 record identity and ordinary
recovery fields, plus the 13 null attribution fields. For v17 it may change
only the version and materialize the 13 absent collision fields as null. Any
coercible present scalar, resource-representation change, recovery-quarantine
delta, or other difference aborts the whole transaction. It clears every
legacy pickle. In the same atomic
boundary it installs an enforced check requiring non-null outer version 1,
non-null v18 JSON, and a null pickle column, so neither a v17 writer, a
SQL-NULL outer version, nor stale pickle repopulation can return. Constraint
validation is a separate, safely resumable transaction because replica updates
can leave deferred foreign-key trigger events; the already-enforced check
protects the gap. Controlled failures identify only an opaque row ordinal and a
controlled reason or exception class. One outer public boundary lets the lock
and transaction contexts finish cleanup, then converts every unexpected
operation, database, or transaction exception to one generic error with raw
exception chaining suppressed. No failure can therefore expose persisted
identifiers, payloads, credentials, driver SQL, or bound parameters.
`ReplicaInfo.from_storage_dict()` is a pure decoder with no logging side
effects, and `ReplicaInfo.status` is the single pure status projection. The
ordinary Serve row-read wrapper owns operational quarantine reporting and emits
an identifier-free warning when that canonical projection is `UNKNOWN`; the
normalizer calls the same pure decoder and projection directly. Quarantined
recovery fields or an `UNKNOWN` stored status therefore cannot leak a persisted
row identity while A validates retained state.

The single stdout line is the durable deployment receipt. It contains counts
and the immutable writer digest, never row payloads, credentials, or service
names. Platform evidence must retain the exact JSON and require all of these
invariants:

```json
{
  "already_current_records": 0,
  "constraint": "ck_replicas_replica_info_version_18",
  "contract": "skyserve.replica-info-v18-normalization/v1",
  "invalid_records": 0,
  "remaining_legacy_pickle_records": 0,
  "remaining_noncurrent_records": 0,
  "rewritten_records": 1,
  "scanned_records": 1,
  "scanned_services": 1,
  "schema_version": 18,
  "serve_database_revision": "046",
  "writer_deployment_roles": ["api", "controller", "executor"],
  "writer_image_digest": "sha256:<exact-A-digest>",
  "writer_pod_inventory_count": 6,
  "writer_pod_inventory_sha256": "<exact-inventory-sha256>",
  "writer_process_count": 6
}
```

The counts are live values, not expected constants, but
`scanned_records == rewritten_records + already_current_records`, both
`remaining_*` fields and `invalid_records` are zero,
`serve_database_revision` is `046`, the role list, Pod count, process count,
and inventory hash prove the exact 2/2/2 cohort, and `writer_image_digest`
equals the digest proven on every split writer role. A
failed or absent receipt blocks Serve049. Rerunning after an interrupted
validation is safe and produces an idempotent receipt. Serve049 then asserts
the exact v18/key shape, drops the pickle column, and physically deletes the
v17 runtime decoder and this normalization module; normalization is not a
permanent happy path. Historical Alembic replay retains only the
migration-owned, executable-global-allowlisted frozen pickle converter used by
revisions 010 (maximum v7) and 026 (maximum v11). No runtime module imports it,
and it cannot be called as a live compatibility path.

### Mechanical activation

Run the transition command from the deployed control-plane environment that
has the central PostgreSQL URI and the chart's Kubernetes inventory access:

```bash
python -m sky.serve.reserved_fill_reconciliation_transition status --json
python -m sky.serve.reserved_fill_reconciliation_transition activate
python -m sky.serve.reserved_fill_reconciliation_transition status --json
```

The module entrypoint selects `IS_SKYPILOT_SERVER=true` before reading
server-sensitive state and loads the deployment's MAIN plugin context before
status or activation. Operators do not need a second wrapper or a hand-authored
plugin bootstrap.

Activation fails unless all of the following are true:

- the database is central PostgreSQL;
- reserved-fill protocol version is exactly 2;
- the deployed transition's current Serve and API-request schema heads are
  exact, non-divergent successors of generalized-binding Serve047/API011;
- Kubernetes and PostgreSQL inventory attest exactly the split roles
  `api`, `controller`, and `executor`, with no compatibility `all` writer;
- all attested writer pods are Ready and all recent process leases match that
  exact pod cohort; and
- every writer Deployment uses one immutable image digest; and
- every API/request-backend/executor/GC/controller/profile participant carries
  the exact same generic handler/profile/receipt capability digest and cohort
  epoch, with old/recent leases drained; and
- every accepted reserved-fill row commits one `RESERVED_FILL/v1` association,
  request, queue row, and retention pin atomically; and
- exactly one deployment-installed `ReservedFillReclaimPolicy` returns fresh
  typed evidence for the exact current claims and the global future-claim and
  terminal-launch enforcement contract. The activation CAS binds its exact
  fleet-bundle, policy-revision, and provider-inventory identity. The generic
  feature image always fails this check.

On one PostgreSQL session, authorization acquires the broker and fleet advisory
locks, opens the SQL transaction, predicate-locks claim scope, and performs one
generation-fenced CAS. First activation changes `LEGACY_ACTIVE` to
`SEQUENCED_ACTIVE`; subsequent fix-forward runs retain `SEQUENCED_ACTIVE` and
advance one generation. A retry with the exact receipt is idempotent. There is
no demotion command, and the PostgreSQL guard rejects a transition back to
legacy.

### Fix-forward behavior

After `SEQUENCED_ACTIVE`:

- old or mixed writers are not a supported target;
- a controller that cannot read the gate or current map suppresses new fill;
- ordinary demand and existing serving replicas continue through their
  existing paths;
- existing legacy fill rows remain readable and may be sheltered or cleaned up,
  but cannot authorize new sequenced capacity; and
- a defect is repaired by deploying a newer full-fleet image and invoking the
  same authorization command. A changed writer/policy receipt advances the
  gate exactly once and invalidates old allocation maps; an exact retry does
  nothing. Durable observation and occupancy history remain in place.

No rollback or canary protocol is promised. The safe failure mode is temporary
reserved-capacity underfill, not duplicate fill or paid spill.

## Manual verification after activation

The verification steps do not directly launch compute or manufacture demand.
After the gate-false successor is applied, the canonical controller is expected
to create zero-cost compute automatically; observing that effect is required.

1. Confirm the transition status reports protocol 2,
   `SEQUENCED_ACTIVE`, the deployed binary's current Serve/API-request heads
   (each a successor of Serve047/API011), the exact generic capability cohort,
   and the expected durable reclaim identity.
   Confirm the full fleet reports public server API capability 77 and worker
   placement projection protocol 2.
2. Confirm every claimed physical pool is producing completed `SUCCESS` or
   explicit `BLACKOUT` observation generations, with no success used past its
   `valid_until`.
3. Confirm a fill-enabled service publishes a nonzero allocation generation
   only when every current edge has matching round and observation provenance;
   confirm schema 5 carries the current gate generation, committed service
   version, exact accelerator-to-worker-projection-digest map per edge, and
   durable reclaim identity.
4. Confirm newly accepted fill replica rows carry the full allocation,
   observation, intent, reclaim identity, service version, exact worker
   projection digest, positive `zero_cost_admission_sequence`, and exact
   `RESERVED_FILL/v1` association/request IDs from `AdmissionReceipt`.
   After its first successful launch persistence, confirm the row also carries
   one immutable positive `zero_cost_materialization_sequence`.
5. Confirm an ordinary zero-cost row created after allocation publication
   advances both admission counters, makes that allocation unreadable, and
   causes any stale fill persistence attempt to write no row. Confirm a peer
   fill advances only the total counter and does not invalidate a map at the
   same ordinary-demand high-water. Confirm first launch success advances only
   the materialization counter and that retrying the same success preserves
   the row's original marker.
6. Confirm `/autoscaler/info` reports `authority_mode: sequenced`, a current
   allocation generation/hash/claim generation, and one exact-provenance pool
   record per authenticated edge. Confirm the same nested object is propagated
   through service status when target replica counts are requested.
   At zero authenticated demand, reconcile each pool/card in its exact width:
   the grant equals attributed nonterminal holdings plus admitted
   unmaterialized intents plus a typed residual. Count a materialized intent
   only as a holding.
7. For an existing service already spanning two reserved contexts, confirm
   observations for both contexts overlap in wall time and new rows remain
   pinned to their authorizing context/physical UID. Do not deploy a synthetic
   service to manufacture this evidence.
8. Confirm every new fill replica remains on a configured zero-cost location
   and no ordinary paid cloud request was created by the fill path.
9. Passively inspect the inference LocalQueue, its active ClusterQueue and
   namespace selector, shared BCL/research preemption policy, effective
   workload priorities, managed inference Workload evidence, and platform
   admission policies; confirm this rollout did not mutate them. Verify from
   logs/metrics that the unique reclaim policy authorized each new sequenced
   claim and launch under the same identity, without launching a synthetic
   workload.
10. Confirm service version convergence and ordinary request handling remain
    healthy. A missing map should stop only new fill, not fail the service.

## Verification plan and evidence

The projected-worker runtime-readiness source gate must prove all of the
following before merge:

- canonical v4 rendering composes the real guarded Kubernetes fragment and
  produces the exact UID input, startup/readiness probes, marker ordering, and
  30-minute startup bound, and a missing final SSH listener/banner prevents
  marker publication;
- caller collisions on the owned UID, probes, or restart policy fail closed;
  command, args, `postStart`, and lifecycle mutations cannot retain the
  template-owned producer digest; and a real Kubernetes client `V1PodSpec`
  round-trips the same exact producer and readiness contract;
- the Ray head status wait retries a nonzero response only when it carries the
  explicit not-yet-initialized condition and propagates every other nonzero
  response even under `set -e`;
- historical v1/v2/v3 projections and an ordinary generic Kubernetes render do
  not receive the marker or probes;
- a Running/not-Ready exact UID remains in the bounded passive wait, a Ready
  exact UID succeeds, and deletion, missing Pod, same-name replacement,
  timeout, or a final-read readiness regression cannot publish success; and
- the monolithic Kubernetes template remains byte-identical to recomposition
  from its guarded outer and node-config sources.

Production qualification is separate. After direct-Helm deployment, inspect a
new east and PHX projected worker and prove the live Pod has the exact probes,
UID field reference, `restartPolicy: Never`, and same UID from create through
Ready. A deliberately failed base bootstrap must never return provider success;
a successful bootstrap must reach Ready without affecting ordinary generic
Kubernetes launches. Activation must first prove every API-server/controller/
provisioner replica reports the capable source; an old-render/new-provisioner
launch must fail static attestation rather than publish success. These checks
do not substitute for the later model and application health gates.

The 2026-08-20 production trace showed isolated full-fleet preflights at
2.92--3.32 seconds while a 15-plus-launch wave caused overlapping
five-second attestation failures. The focused regression must use real
PostgreSQL and real OS handler processes matching the database topology inside
`DisposableExecutor`. Barrier-synchronized cohorts of 15 and 90 independently
instantiated policy objects run against a cold receipt while the elected
provider proofs remain parked for 3.2 seconds, inside the observed
2.92--3.32-second production range, and every caller retains the same
absolute five-second horizon. The production guardian and warden do not import
the policy or open proof/database sessions; the stress test therefore spawns
the 15/90 handler processes directly while the existing process-boundary suite
separately owns guardian/warden/FD-quarantine correctness. CI keeps the
15-process case in the ordinary suite but runs the marked 90-process case in
its own non-xdist 4x16 shard, so it never contends with three unrelated test
workers. The spawned pressure target uses a strictly test-only import topology:
it installs only the `sky` and `sky.serve` package search paths before importing
the real policy, repository, schemas, and database utilities. It therefore
skips unrelated CLI/API re-export initializers without stubbing any contract
under test. The ordinary pytest process retains the normal package
initializers, and every result attests that the clean spawned worker took the
narrow path. This topology proves the database and concurrency contracts; it
does not emulate production worker bootstrap or establish production handler
RSS. The separate `DisposableExecutor` suite owns that process boundary. The
shard is qualified only after repeated CI runs establish safe runner memory
headroom; otherwise it moves unchanged to a >=32 GiB profile.
The elected transaction changes its PostgreSQL `application_name` locally to
`skypilot-reclaim-proof-owner` immediately after the nonblocking advisory lock
succeeds; any failure to install that phase tag rolls back and denies the
proof. The pressure target parks every loser at the entry to receipt polling,
which is reachable only after its election transaction and NullPool connection
have physically closed. It separately parks both provider callbacks. The
parent waits for the two callbacks and all `N - 1` losing PIDs, then requires
exactly one owner session in `idle in transaction`, exactly one granted
transaction advisory lock on that owner, zero base-name proof sessions, and
zero advisory waiters. It then releases the losers into the real bounded
receipt-polling wave while keeping the provider owner parked until 3.2 seconds.
This proves both the deterministic no-retained-loser snapshot and the complete
production polling pressure; it does not suppress the pressure to obtain the
snapshot. This deterministic observation replaces backend-age thresholds,
which measure process scheduling rather than connection retention.
One additional real
`DisposableExecutor` test elects one context receipt owner, proves its AWS and
Kubernetes provider callbacks are both stalled while exactly one session holds
exactly one advisory lock, then verifies the five-second failure produces
`FAILED` plus
`family_drained` and, after that process-family proof, zero surviving proof
sessions/locks, no receipt/provider effect, and a successful later fresh-nonce
reproof. This process boundary is
the hard survivor/session guarantee; the local libpq, server-statement, and
socket timeouts above are pressure-reduction and prompt-failure layers. Each
pressure case
must produce exactly one AWS and one Kubernetes provider read, one receipt row,
one nonce, and one returned reference; exactly one tagged owner session and
one advisory-lock holder; no advisory-lock waiters; and fewer than 100
simultaneous proof sessions. Every worker executes its terminal final guard on
an ordinary Serve database connection before it reports success, and zero
proof/worker/counter sessions survive the completed wave. The complete
empirical wave, including those guards, must use fewer than eight physical
session opens per worker plus a fixed allowance of 20 over the full five-second
horizon. Tests also cover distinct authorities, database-clock expiry,
conservative SQL-round-trip age mapping, provider
failure without caching, leader/advisory-session loss before publish, the
intentional already-observed-waiter-wave fail-closed result plus one later
durable retry, waiter timeout without leader cancellation, semantic cache
mismatch, malformed exact-authority-row repair, delete/reinsert ABA, scope
mismatch despite a valid context proof, and final-guard rejection of missing,
expired, malformed, wrong-nonce, wrong-digest, wrong-gate, wrong-identity,
or wrong-context rows. It also proves that an identical expired renewal
preserves the nonce and an already minted reference, a receipt inside the final
0.5-second reserve is refreshed before return, slow payload validation and
physical connection close cannot consume that reserve at handoff, and any
proof-content change rotates the nonce and rejects the older reference. The
terminal test requires READ COMMITTED, rejects an independently stale local
reference, proves an uncommitted identical update neither blocks nor rejects a
valid MVCC guard, and proves a changed commit is visible to the next statement
and rejects the old nonce. A staggered 90-launch wave runs for ten seconds,
crosses at least two identical renewals, retains one nonce, and admits every
terminal guard. Backend tests hold a delayed fleet-gate acquisition open and
prove no policy deadline/reference is minted until the gate is entered, then
verify the gate session both after proof and after provider I/O:

```bash
pytest -n 0 -q \
  tests/unit_tests/test_reserved_fill_reclaim_proofs.py \
  tests/unit_tests/test_boltz_reserved_fill_reclaim_policy.py \
  -k 'multiprocess_launches_share_fresh_provider_receipt or \
      expired_identical_receipt_refreshes_once or \
      staggered_launches_span_two_identical_renewals or \
      failed_provider_proof_is_not_persisted or \
      lost_advisory_session_cannot_publish or \
      final_guard_rejects_stale_or_mismatched_receipt or \
      kubernetes_provider_uses_the_exact_assumed_audit_session'
```

Local PostgreSQL qualification on 2026-08-20 used one shared absolute
five-second deadline for every handler and included each handler's terminal
guard. Before deterministic phase tagging, the first fresh 4x16 CI run used
8,688,271,360 bytes at its cgroup peak and completed all 90 authorizations in
4.399 seconds, but correctly failed qualification because the old
`backend_start > 100 ms` proxy classified 48 scheduler-delayed active reads as
long lived. Pinning the same workload to four local CPUs reproduced the false
classification with 22 such reads while still showing one lock holder and no
waiters; the age proxy is therefore removed, not relaxed.

With the exact owner/parked-loser barrier, the 15-process wave completed in
3.949 seconds with a 0.002-second maximum start skew, five peak proof sessions,
and 88 physical opens. The 90-process wave pinned to four local CPUs completed
in 4.393 seconds with a 0.051-second maximum start skew, 71 peak proof sessions,
and 538 physical opens. At the deterministic observation point every non-owner
PID was parked after physical close and PostgreSQL showed exactly one tagged
`idle in transaction` owner, one advisory-lock holder, zero base-name proof
sessions, and zero advisory waiters. Both waves produced one AWS call, one
Kubernetes call, one row, one nonce, matching returned references and terminal
guards, and zero surviving proof/worker/counter sessions. Cross-pod production
qualification remains a rollout gate and is not inferred from this local
process evidence. Read-only production PostgreSQL inspection on 2026-08-20
reported `max_connections = 844`, three superuser-reserved slots, and 40
current connections. A conservative 90-launch overlap of 90 service sessions,
90 fleet sessions, the measured 71 proof-session peak, and that 40-session
baseline is 291 total, leaving 550 non-reserved slots; extending the
already-required fleet session across the bounded proof is therefore within
the present production budget. Earlier diagnostic measurement of the narrow
90-worker topology found 3,553,361 KiB aggregate worker PSS and 3,818,638 KiB including
pytest and the manager, but the shared local pod's historical cgroup peak makes
that evidence ineligible to qualify a 16-GiB runner or claim production worker
RSS. The dedicated fresh CI job sets a 12-GiB absolute cgroup limit; the
pressure test reads and reports
cgroup-v2 `memory.peak` (or samples `memory.current` when peak is unavailable)
or cgroup-v1 `memory.max_usage_in_bytes`, including pytest setup, runner, and
PostgreSQL overhead when they share that cgroup, and fails when the metric is
unavailable or exceeds the limit. Qualification requires three consecutive
exact 90-process CI passes under that absolute limit; otherwise the unchanged
test moves to a >=32-GiB profile.

### Required automated commands

```bash
# Activation successor A pre-activation contracts.
uv run --no-sync pytest -q -n 0 \
  tests/unit_tests/test_serve_lb_k8s.py \
  tests/unit_tests/test_serve_request_queue.py \
  tests/unit_tests/test_reserved_fill_reclaim_policy_unit.py \
  tests/unit_tests/test_sky/utils/test_context.py

helm lint charts/skypilot
helm unittest charts/skypilot
helm schema -f charts/skypilot/values.yaml \
  -o /tmp/skypilot-values.schema.json
cmp /tmp/skypilot-values.schema.json charts/skypilot/values.schema.json

terraform -chdir=infra/terraform/modules/skypilot-spoke-workspace-pool-eks \
  fmt -check -recursive
terraform -chdir=infra/terraform/modules/skypilot-spoke-workspace-pool-eks \
  validate
terraform -chdir=infra/terraform/modules/skypilot-spoke-workspace-pool-eks \
  test -test-directory=terraform-tests

# Existing reserved-fill protocol regression set.
uv run --no-sync pytest -q \
  tests/unit_tests/test_serve_capacity_takeover_pg.py \
  tests/unit_tests/test_pool_capacity_observation.py \
  tests/unit_tests/test_pool_capacity_observer.py \
  tests/unit_tests/test_reserved_fill_planner.py \
  tests/unit_tests/test_reserved_fill_manager_receipt.py \
  tests/unit_tests/test_reserved_fill_autoscaler_adapter.py \
  tests/unit_tests/test_reserved_fill_status.py \
  tests/unit_tests/test_reserved_fill_reclaim_attestation.py \
  tests/unit_tests/test_reserved_fill_reclaim_policy_unit.py \
  tests/unit_tests/test_reserved_fill_execution_fence.py \
  tests/unit_tests/test_reserved_fill_reconciliation_transition.py \
  tests/unit_tests/test_serve_platform_projection.py \
  tests/unit_tests/kubernetes/test_provision.py \
  tests/unit_tests/test_sky/provision/test_provision_cluster_incarnation.py \
  tests/unit_tests/test_sky/provision/test_provisioner_pause.py \
  tests/unit_tests/test_sky/test_failover_classification.py \
  tests/unit_tests/test_backend_utils.py \
  tests/unit_tests/test_sky/clouds/test_kubernetes.py \
  tests/unit_tests/test_serve_scale_reconciliation.py \
  tests/unit_tests/test_serve_controller.py \
  tests/unit_tests/test_serve_controller_event_loop.py \
  tests/unit_tests/test_reserved_capacity_fill.py \
  tests/unit_tests/test_reserved_fill_broker.py \
  tests/unit_tests/test_serve_cleanup_recovery_script_order.py \
  tests/unit_tests/test_serve_ordinary_launch_binding.py \
  tests/unit_tests/test_serve_replica_api.py \
  tests/unit_tests/test_serve_replica_managers.py \
  tests/unit_tests/test_serve_replica_record_contract.py \
  tests/unit_tests/test_serve_state.py \
  tests/unit_tests/test_serve_utils.py \
  tests/unit_tests/test_interrupt_request_for_retry.py \
  tests/unit_tests/test_sky/server/requests/test_executor.py \
  tests/unit_tests/test_sky/server/requests/test_process.py \
  tests/unit_tests/test_api_requests_postgres_schema.py \
  tests/unit_tests/test_orphaned_inflight_requests.py \
  tests/unit_tests/test_server_request_recovery.py \
  tests/unit_tests/test_sky/server/requests/test_internal_daemon_submission.py \
  tests/unit_tests/test_sky/server/test_daemons.py \
  tests/unit_tests/test_sky/server/test_runtime.py \
  tests/unit_tests/test_sky/server/test_runtime_daemons.py \
  tests/unit_tests/test_batch_recovery.py \
  tests/unit_tests/test_jobs_utils.py \
  tests/unit_tests/test_managed_job_controller_restart_race.py \
  tests/unit_tests/test_sky/jobs/test_scheduler.py \
  tests/unit_tests/test_sky/jobs/test_controller.py \
  tests/unit_tests/test_sky/jobs/test_controller_attempt_fencing.py \
  tests/unit_tests/test_sky/jobs/test_controller_ownership.py \
  tests/unit_tests/test_sky/jobs/test_controller_slots.py \
  tests/unit_tests/test_sky/jobs/test_jobs_state.py \
  tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py \
  tests/unit_tests/test_sky/client/test_service_account_auth.py \
  tests/unit_tests/test_sky/utils/test_controller_capability.py

SKYPILOT_TEST_POSTGRES_URL=postgresql:///postgres \
  uv run --no-sync pytest -q -n 0 \
  tests/unit_tests/test_api_requests_pg.py \
  tests/unit_tests/test_batch_recovery_pg.py \
  tests/unit_tests/test_pool_capacity_observation_pg.py \
  tests/unit_tests/test_reserved_fill_allocation_pg.py \
  tests/unit_tests/test_reserved_fill_broker_pg.py \
  tests/unit_tests/test_reserved_fill_terminal_fence_pg.py \
  tests/unit_tests/test_reserved_fill_multi_pool_state.py \
  tests/unit_tests/test_replica_record_normalization_pg.py \
  tests/unit_tests/test_serve_ordinary_launch_handoff_schema_041_pg.py \
  tests/unit_tests/test_serve_placement_normalization_schema_040_pg.py \
  tests/unit_tests/test_serve_resource_action_schema_033_pg.py \
  tests/unit_tests/test_serve_resource_action_schema_038_pg.py \
  tests/unit_tests/test_serve_resource_action_schema_039_pg.py \
  tests/unit_tests/test_serve_resource_action_state_pg.py \
  tests/unit_tests/test_serve_resource_actions_pg.py \
  tests/unit_tests/test_serve_system_recovery_persistence_pg.py
```

PostgreSQL tests must run against real PostgreSQL with repository-default xdist
explicitly disabled (`-n 0`); parallel schema migration fixtures can otherwise
exhaust a small server's shared lock table without exercising feature
correctness. Formatting, typing, lint, and diff integrity must also pass for
every changed file.

The regression set must include one cross-layer contract with
`min_replicas: 0`, `floor_replicas: 0`, `utilization_gate: false`, zero demand,
and a fresh authenticated grant of `N`: reconciliation emits exactly `N`
width-adjusted sequenced intents and publishes no paid residual or Spot launch
authority. The paired `utilization_gate: true` case emits zero idle fill.

The replica-record contract tests must instantiate all eight exact pre-v17
top-level/status census shapes, including all three v13 identity variants.
They prove lossless preservation of every present field, conservative
materialization of every absent field, deterministic pre-v13 record identity,
v13/v14 recovery quarantine semantics, canonical v18 output on the next
persistence, and cleanup-fence preservation for historical reserved-fill rows.
Negative cases reject every unsanctioned version, missing or extra field,
wrong status shape, and v17 partial-collision bundle. Policy-bound admission
must still reject a decoded legacy reserved-fill row without complete
sequenced attribution.

The PostgreSQL normalizer test independently freezes those same eight input
shapes, all missing-field defaults, known pre-v13 UUID5 outputs, value-bearing
protocol-v2 `I3`/`I4` cleanup identity, and exact recursive preservation of
every present value/type. It must also prove atomic rollback for a coercible
legacy value and a v13/v14 recovery-quarantine delta, preserve complete v17
attribution, clear a stale pickle from an already-v18 row, and remain
idempotent. These assertions must not derive their expected additions from the
current decoder or serializer.

The generated values schema is a required enforcement layer, not decorative
documentation: `serviceAnnotations.additionalProperties` must remain
`type: string`. The Helm helper independently rejects a non-map or non-string
entry before serializing deterministic JSON, and Python tests feed raw JSON
directly to the semantic boundary to reject numeric values, duplicate keys,
malformed keys, reserved-key conflicts, and malformed ownership ledgers. This
combination is the numeric-value evidence; a `helm-unittest --set` fixture is
not authoritative because that harness may coerce scalar input before the
template observes its original YAML type.

The automated policy tests cover zero and multiple entry points, malformed or
stale typed evidence, identity mismatch, a claim change between external proof
and locked persistence, an activation change between attestation and CAS,
missing policy after executor restart, partial/forged launch fences, and a
policy mismatch immediately before provider mutation. They also prove that
all policy/provider reads occur outside broker, service-authority, and database
locks and that `LEGACY_ACTIVE` retains its bounded compatibility behavior.
Kubernetes tests cover fresh authorization for normal/AppArmor/409 create
attempts and rejection cleanup, immediate create-response attestation,
guard-free passive `provision_timeout: -1` waits, terminal cancellation, and
durable-owner cleanup instead of opaque request teardown. PostgreSQL activation
tests prove exact v18 queued authority and reject pre-normalization v17/v16,
stale service versions, and stale projection digests. Request tests cover exact
outer-guardian
receipts, pre-effect pidless claims, ambiguous RUNNING legacy claims, PID reuse
and pidfd signalling, abrupt-boundary deferral, terminal result preservation,
and the absence of any PID-death receipt shortcut. They cover ordinary and
provider-reserved claimed `PENDING`/`WAITING` cancellation and
dispatcher-no-Future races; and prove terminal boundary-receipt closure for
`NEVER`, `READ_ONLY`, and `RECONCILE`, including a bound ordinary launch,
without reopening terminal work. They also inject PostgreSQL loss before the
parent receipt write, prove the parent monitor retries to durable convergence,
cover transported callable and result-serialization exceptions as typed
outcomes, and prove shutdown joins all receipt monitors. Disposable-boundary
tests kill the inner warden while a `setsid()` grandchild is live, kill the
outer guardian while the inner family is live, and race cancellation against
handler return; no Future or receipt becomes visible until the
surviving owner drains the exact family and the handler root is safely reaped.
Capability qualification proves fresh-FD delivery for every manager, daemon
restart, and verified managed-origin handler; no grant for an origin-less
controller-class request; non-dumpability before raw authority or plugin
access; absence of the bearer from environment and argv; exec/fork fail-closed
behavior; exact scoped cleanup; and pre-admission cancellation without a
stranded transfer-handle duplicate.
They also prove that Pod deletion,
replacement, lease age, and signal delivery do not synthesize quiescence.
Runtime-daemon tests additionally cover retirement before generic re-enqueue,
no daemon handler registration or queue delivery, exact selected-daemon
inventory, isolated child restart, parent-death setup, clean environment and
system context initialization, bounded `SIGTERM`/`SIGKILL` shutdown, and child
and grandchild group reaping before graceful controller leadership release.
Shutdown tests prove every request guardian is explicitly receipt-complete and
reaped/absent (including a simulated kill/join timeout), and make an incomplete
background/supervisor join fail closed before leadership or instance-lease
release. Real-PostgreSQL tests
seed queued, claimed, and terminal legacy daemon rows and prove that only those
rows are retired under the current controller generation while ordinary
requests survive.
Managed-job slot tests cover fixed eager slot birth, empty-queue polling,
transactional four-field claim publication, exact-slot state and nested-action
fencing, local slot crash and complete family drain before exact-attempt reset,
replacement-attempt rotation, stale outer-generation recovery after whole-Pod
loss, graceful refresh/slot/request drain ordering, and the absence of any
PID-file, request-triggered controller spawn, or shared-PID decoder.

### Evidence recorded so far

- Activation successor A's pre-activation contract suite passes locally on the
  exact current worktree: 350/350 Python tests across external-LB Kubernetes
  lifecycle, request queue, reclaim-policy, and request-context isolation;
  Helm lint plus 21/21 suites and 305/305 tests; byte-identical regenerated
  values schema; an expected-negative schema lint rejecting an integer Service
  annotation; Terraform 1.14.8 init/validate/fmt plus 51/51 module tests; and
  `format.sh` mypy/pylint/dashboard checks. These are source/render tests, not
  PR 14 deployment, generated-Service readback, route materialization, live
  capacity, or BCL-preemption evidence.
- Historical live diagnosis: repeated UID-fenced 34-slot A100 publications,
  with 181--250-second age at autoscaler consumption.
- Implementation tests exist for concurrent observation, pool-local blackout,
  generation-fenced activation/reauthorization CAS, ordinary zero-cost
  sequencing, map authentication and
  current-round revalidation, deterministic planning, same-map replay debit,
  parallel multi-context preflight, exact sparse receipt, lost wakeups, and
  sequenced-controller selection. Status tests cover exact durable-provenance
  joins, legacy/unavailable shapes, optional diagnostic failure,
  endpoint fail-closed fallback, and service-status propagation.
- Added tests prove same-tick `target=None` ordinary-demand debit, one shared
  multi-context preflight deadline, release of observer capacity after a
  timed-out Kubernetes query, access-context alias consumption with preserved
  UID/context fences, and alias de-duplication without physical-pool double
  counting.
- Real-PostgreSQL regressions prove a service-B ordinary row invalidates
  service A's published map and rejects its stale fill transaction with no row,
  while a broker-partitioned peer fill advances only total row attribution and
  remains valid input to a later observation at the unchanged ordinary
  high-water.
- Additional real-PostgreSQL regressions prove protocol-first zero-cost writes
  do not form a sequencer/service crossed-lock deadlock; a pre-observation
  unbound ordinary nonclaimant is debited; a launch materializing during an
  observation remains debited; a pre-observation materialization is not
  double-debited; and post-observation admission is ordered correctly even
  when its application timestamp is older. A grouped replica decode failure
  rejects sequenced occupancy instead of publishing optimistic capacity.
- Historical pre-projection-v2 validation on 2026-08-12 passed its focused
  policy, Serve045, broker, non-PostgreSQL, format, mypy, pylint, dashboard, and
  Prettier checks. Those counts do not certify the current Serve046 worktree.
- The final 2026-08-13 non-PostgreSQL matrix passed all 3,298 tests across the
  50 documented files in 83 seconds after formatting. Its first run exposed
  and the implementation corrected one backend integration regression: the
  historical optional planner/DAG input must remain optional for a successful
  reserved-fill Kubernetes adoption while the post-materialization authority
  guard is carried forward. The focused regression and complete rerun pass.
- After the exact accelerator-scheduling correction, the final complete
  non-PostgreSQL rerun on code/design revision
  `123c16762aea510d34db74f52c0c27e733fbb07d` passed all 3,384 tests across the
  same documented 50-file matrix with zero failures or skips in 93.733
  seconds. The retained JUnit artifact is
  `/tmp/feature-nonpg-123c16762-rerun.xml`, SHA-256
  `c0afd4330fa7d5ef500f49e52d54abd3a8033c97c1b1c656360bf07e294d6092`.
- Changed-source pylint passes at 10.00/10 and `git diff --check` is clean.
  Changed-source mypy has only the same pre-existing `backend_utils.py`
  overload diagnostic present on `origin/improvements`; the repository mypy
  target reaches six unrelated baseline/environment diagnostics in unchanged
  files. No feature-owned typing diagnostic remains.
- On the exact corrected behavior tree
  `688521ffd6cce0838b55c98fbb1196584116fc70`, Terraform 1.15.8 validates both
  changed spoke modules. The final EKS module at audit-boundary revision
  `3af32dfdcd1ca9b27985e53e990d9f9efd256d58`, including its separate
  collision-resistant role, exact transitive-tag trust, derived queue grant,
  and clean invalid-partition failures, passes all 48 tests. The RBAC module
  passes all 20 tests from its explicit `terraform-tests` directory.
- The final serial real-PostgreSQL matrix passed all 618 tests across the 15
  documented files with zero failures, errors, or skips. Repository-default
  xdist was disabled; four ordered chunks each exited zero, with an aggregate
  wall time of 1,252 seconds (20m52s) and aggregate JUnit test time of
  1,221.896 seconds. The exact code revision was
  `688521ffd6cce0838b55c98fbb1196584116fc70`; the four retained JUnit artifacts
  are `/tmp/feature-pg-chunk1-688521ffd.R57qNi.xml` (206 tests),
  `/tmp/feature-pg-chunk2-688521ffd.HiL1jE.xml` (211 tests),
  `/tmp/feature-pg-chunk3-688521ffd.MoLBSj.xml` (80 tests), and
  `/tmp/feature-pg-chunk4-688521ffd.Ax9kzJ.xml` (121 tests). Process audits
  proved one pytest owner throughout each chunk and no PostgreSQL pytest
  remained after the freeze.
- The exact post-accelerator-correction serial real-PostgreSQL rerun on
  revision `123c16762aea510d34db74f52c0c27e733fbb07d` passed the same 618 tests
  with zero failures, errors, or skips in 1,345.917 seconds. It used one pytest
  process and the required real PostgreSQL server. The retained JUnit artifact
  is `/tmp/feature-pg-123c16762.xml`, SHA-256
  `19101b79676824822e55f4fdf9c9c2299ae8792944aa59e8c673bc934a0229ac`.
- The publication CI correction changes only static contracts and preserves
  the three-times-reviewed behavior: it replaces deprecated typing/import
  forms, freezes the already-narrowed observation repository in its callback,
  records existing dynamic factory and cookie contracts for the type checker,
  moves blocking runtime-daemon path setup to one worker-thread helper, and
  narrows the existing lifecycle-removal locator without changing its
  obligation. Exact Ruff, basedpyright, mypy, lifecycle-removal, formatting,
  import-order, changed-source pylint, and focused regression checks pass on
  the corrected tree. The callback freeze also removes a latent loop
  late-binding ambiguity while retaining the reviewed repository identity.
- On integrated implementation revision
  `244cc34fbfb61ba719691b33c92f93d039ef610f`, the corrected separate Boltz
  plugin and generic policy interface pass all 113 tests in the focused
  superset across policy, packaging,
  overlay-manifest, release-version, attestation, and generic-interface
  suites. Ruff, targeted mypy, changed-source pylint at 10.00/10, JSON parsing,
  Python compilation, formatting, and `git diff --check` also pass. The
  repository-wide mypy step reaches one unrelated baseline diagnostic in
  unchanged `sky/server/common.py`; targeted feature mypy is clean. New tests
  reject a
  missing or mismatched Node inventory, selector/product/capacity drift, and a
  deleting-only flavor while accepting a non-Ready initializing Node.
- On correction revision `70cb55a2fb003a4cd9665c5f3118c2b923a1f6ea`, the
  integrated policy/interface superset passes 121/121. New tests accept the two
  east exact-card edges in one context, reject a duplicate physical UID/card
  atom before provider calls, and reject missing Pod rules or drift in webhook
  operations, endpoint, CA bundle, and namespace selection. Ruff and targeted
  mypy pass, pylint is 10.00/10, JSON and Python compilation pass, and the exact
  live east mutating and validating objects pass the same validator.
- On behavior revision `f4a8aa8d003f256e5c2b621ca29461d75f84fdcd`,
  activation and later claim replacement call the same exact-card atom
  validator. The integrated policy/interface superset passes 123/123; new
  activation tests accept east's distinct A100-40/A100-80 claims in one
  context and reject a duplicated physical UID/card atom before provider
  calls. Ruff and targeted mypy pass, pylint is 10.00/10, formatting and
  `git diff --check` pass.
- On correction revision `bfbd6cbe0a9f22487d035f9149ac673ca4dacd95`,
  the fix after failed review round 1 carries the exact Kubernetes
  accelerator scheduling atom through the typed admission and rejects drift in
  its label key, label values, or resource key before provider calls at claim,
  activation, and launch. The strict bundle rejects cross-card flavor or
  scheduling overlap, and its east A100 contract now owns only the 40GB
  product. The integrated policy/interface superset passes 132/132; Ruff and
  targeted mypy pass, pylint is 10.00/10, JSON parsing, compilation,
  formatting, and `git diff --check` pass.
- A historical post-correction local Serve047 implementation restack at
  `1efa6b284` passed 58/58
  focused final-state, cleanup-presence, manager-receipt,
  reconciliation-transition, and status tests; the combined policy superset
  passes 134/134; and its required real-PostgreSQL Serve047 schema suite passes
  12/12 with zero skips against the isolated local PostgreSQL server.
  These results describe a superseded cleanup tree. They do not validate or
  freeze current cleanup C, whose exact replacement revision and tests remain
  open.
- No A, B, or C merge/publication, platform PR 14/17/18/19 apply, activation
  result, live GPU fill, or BCL preemption result is claimed in this document.

### Historical adversarial review record

The final integrated A/B/C/platform stack has zero passing final review rounds.
Three new consecutive pragmatic reviews run only after every final head is
frozen and before final deployment/completion; any material change resets the
sequence. A's earlier merge gate is the independent security/contract review
and CI described above, not a separate three-round sequence. The table below is
retained only as historical Serve046 evidence and no row counts toward the new
sequence.

One non-counting review attempt on feature `b93db03fb` and cleanup
`1094b9ded` failed. It found that the deployment policy rejected valid
same-context exact-card edges and that webhook attestation trusted names
without proving Pod admission rules or the Kueue endpoint. Revision
`70cb55a2f` fixes both findings and adds the fail-closed coverage above. The
consecutive sequence therefore restarts from round 1.

The first restarted round on feature `9baca9dca` and cleanup `cae4abd87`
also failed and does not count. It found that the policy ticket bound only the
logical accelerator name and count, while terminal launch used an accelerator
label/resource tuple absent from that ticket. East's bundle also let logical
`a100` name both 40GB and 80GB products. The correction above binds the exact
scheduling atom end to end and makes logical card contracts disjoint. The
consecutive sequence restarts from round 1 again.

| Round | Revision reviewed | Result | Material findings/fixes |
|---|---|---|---|
| 1 | feature/design `123c16762aea510d34db74f52c0c27e733fbb07d`; stacked Serve047 cleanup `bc2725c54149d14bc4e90edb2df24af5efccd789` | pass | No material or non-material findings. The review traced the exact normalized accelerator label key, sorted values, and resource through projection, activation, claim replacement, durable receipt/scope hashing, terminal authorization, rendering, Pod admission/adoption, and bound-Node proof. It also found no oversubscription, stale-authority, paid-spill, duplicate-happy-path, or BCL reclaim regression, and confirmed Serve047 leaves one forward-only two-state authorization path. |
| 2 | feature/design `a0fe24207854cdc3f98a4d2a879cc9dce4bfa0f7`; stacked Serve047 cleanup `175e04e8376d8507c9d08428f2f2a34516df8b2e`; design SHA-256 `b6037bab7e8de936aa5d447b7547f7ea2faf012395bfb36ccc1eb8006cecf486` | pass | No material or non-material findings. Independent review reverified the terminal PostgreSQL admission ledger, exact projection-to-Node accelerator scheduling atom, disjoint physical-card contracts, fail-closed zero-cost launch, live-attested bounded Kueue borrowing and BCL/research reclaim, and Serve047's sole forward authorization path. All non-design blobs remained byte-identical to round 1. |
| 3 | feature/design `cea111a5ddcf7f84e7426d75920e23cae7d33b65`; stacked Serve047 cleanup `c4a46c2debe54a832916cd64408c8306a50dc266`; feature design SHA-256 `fb9a88be168ac3a951a51c053f054a86d97ad5f363504c005a4c1be10bd2d398`; cleanup design SHA-256 `793fc1b5000f3d2ec46da066798645c9e36fc703f4faade9ef4fbc35bc4a89e5` | pass | No material or non-material findings. Final independent review reconfirmed bounded generation-fenced observation, complete terminal admission revalidation, exact-card zero-cost-only launch, projection and deployment-policy identity, zero-nominal inference borrowing with research reclaim, and Serve047's two-state forward-only authority. Every non-design blob remained byte-identical to round 2. |

Those rows reviewed a now-superseded use of migration number Serve047 for
cleanup. Serve047 now belongs to generalized binding G1 and cleanup is
Serve049; none of the rows validates the mixed-version incident correction,
capability cohort, typed reconciliation, or atomic reserved-fill binding.

Reviews should be pragmatic and fix-forward oriented. They must reject an
oversubscription, stale-authority, duplicate-happy-path, paid-spill, or BCL
priority regression, but should not require a canary or a general rollback
system.

## Transitional code and stacked removal path

Source PR #1451 is the already-merged Serve046 base, not an open transition
PR. The current source stack is:

1. activation successor A, targeting `improvements`: the already-merged v18
   writer and one-shot normalizer, the exact-shape v3/v6/v7/v12/v13/v14
   reader bridge, plus the queue-capacity, generated-Service annotation, and
   audit-target contracts required for platform PR 14's direct split-and-roll;
2. a stacked pre-v17 reader-removal change, held until the archived v18
   normalization receipt proves that none of the six sanctioned legacy
   versions/eight observed shapes remains; it removes only the bridge added to
   A and preserves the already-existing exact v17/v18 boundary;
3. independently releasable publisher B: the exact-only publication contract
   and physical deletion of superseded publisher paths, with no runtime or
   schema behavior change;
4. generalized binding G1: forward-only API011/Serve047, in-place typed
   non-pool association profiles, exact capability cohort, atomic reserved-fill
   binding receipt, per-row typed recovery, and provider-free route projection;
   and
5. demand/route convergence: forward-only API012/Serve048 implementing the
   durable feed, ordered placement, and route projection; and
6. final cleanup C/G2: forward-only API013/Serve049 plus physical deletion of
   every live compatibility and transition path. C retains
   `reserved_fill_reconciliation_transition status/activate` as the sole
   first-authorization and reauthorization surface.

Within the reserved-fill activation stack, platform PR 14 pins A while creating
the split topology; the earlier narrow ordinary-FEV2 single-`all` rollout is
not an activation step and cannot normalize. PR 17 normalizes retained rows on
the unchanged PR 14 tuple and creates no Helm revision; PR 18 follows B and
also creates no Helm revision; PR 19 alone pins and deploys C. The remote draft
cleanup PR
[#1452](https://github.com/boltz-bio/skypilot/pull/1452) is a stale predecessor,
not current C or merge/deployment evidence. It must be replaced or updated to
the exact reviewed C revision. Historical cleanup PR #1263 is unrelated.

The cleanup uses new forward-only API013 and Serve049 migrations; it never
edits or renumbers historical Serve044, Serve045, Serve046, or generalized-
binding Serve047 and demand Serve048, nor API009--012. API013 owns the combined-
role/old-handler final cleanup that an earlier draft assigned to API011.
Serve049 preserves the
Serve045 reclaim receipt/generation and every Serve046 service-version and
projection-digest column and constraint. It replaces the Serve045 gate check,
default, and `ENABLE ALWAYS` trigger with the protocol-v2-only final two-state
domain: `UNAUTHORIZED` with a completely null authorization receipt, or
`SEQUENCED_ACTIVE` with a complete Serve045 receipt. Under the migration lock,
a well-formed null-receipt
`LEGACY_ACTIVE` bootstrap row becomes `UNAUTHORIZED` without changing its
generation; a valid active row and receipt are preserved byte-for-byte; a
partial or malformed shape aborts migration. Thus a fresh database is inert,
and a migrated but not-yet-authorized database has no legacy actuator.
`UNAUTHORIZED` permits ordinary reconciliation but suppresses every
reserved-fill provider observation, allocation, and launch effect. It still
maintains provider-free protocol-v2 service claims and immutable worker
projections, so the canonical command has a complete current claim scope to
attest on first authorization. Before the protocol-v1 decoder is removed,
migration mechanically rejects any active protocol-v1 worker projection. The
same canonical command authorizes `UNAUTHORIZED` to
`SEQUENCED_ACTIVE` at exactly `generation + 1` and reauthorizes
`SEQUENCED_ACTIVE` after a fix-forward rollout; cleanup does not introduce a
second bootstrap actuator.

The cleanup stack removes, rather than perpetuates:

- every unbound non-pool admission/recovery path, the old ordinary-only
  handler/profile alias, cluster-name quiescence as launch authority, global
  startup recovery lock/backoff, process-map authority, rollback demotion after
  its rehearsal window, and generalized-binding transition telemetry;

- first, the exact-shape v3/v6/v7/v12/v13/v14 live reader and its census tests
  after the archived receipt proves zero non-v18 rows; then, in final cleanup
  C, the `replica_info` pickle column, live v17 reader, one-shot v18
  normalizer, its command/tests, and every transition-only receipt consumer
  after the exact archived receipt passes. Frozen revisions 010/026 migration
  replay is the only historical pickle code retained;
- the one-pod/all runtime role and corresponding Helm values, templates,
  conditionals, tests, and operator knobs after the exact split-role receipt;
- transition-only acceptance of a generated inference Service without
  `skypilot.co/serve-lb-operator-annotation-keys`, after PR 14 proves every
  live Service has been reconciled to a canonical ledger; the permanent narrow
  ownership ledger and strategic-merge behavior remain;
- no superseded image/chart publisher workflow, overlay builder, moving tag,
  or release fallback; B must physically delete those source paths before C is
  eligible, C's final absence tests prevent their reintroduction, and PR 19
  later deletes the retired external publisher identity;
- the `LEGACY_ACTIVE` provider-query branch in protocol-v2 broker cycles;
- legacy fill launch emission and emission-time feed/rotation spending from
  `_apply_reserved_capacity_fill_v2()`;
- direct-call poller compatibility that lacks an actuation-generation fence
  and therefore holds the old broad lock;
- new-admission tolerance for an unattributed protocol-v2 fill tuple after the
  legacy fleet has drained;
- the worker-projection protocol-v1 ordinary-launch decoder and its transition
  tests after every active version and launch has remained on protocol v2 for
  the removal horizon;
- obsolete autoscaler/manager fill signaling methods superseded by
  `ScaleReconcileCoordinator`; and
- the nullable pre-slot managed-job adoption decoder, queries, and transition
  tests after its fleet horizon; and
- `LEGACY_REQUEST_DAEMON_IDS`, the legacy `daemon:` supported-handler census
  and transition lock, row-retirement methods and startup calls, retired
  daemon pickle symbols, and their transition tests after their stale-writer
  horizon. The fixed-slot managed-job supervisors and runtime-daemon
  subprocess supervisors remain. Historical controller PID inventory and
  cutover helpers are already deleted by the feature and are not claimed again
  by cleanup.

Cleanup explicitly retains the canonical `status` plus
authorization/reauthorization command, its `_read_stable_writer_rollout`
attestation, and `get` access to ReplicaSets. Pod -> ReplicaSet -> Deployment
identity is part of every first authorization and fix-forward reauthorization,
not transition-only code. Read-only historical row decoding may remain only
where durable terminal data still requires it; the protocol-v1 ordinary worker
projection decoder does not remain after its gate passes.

Cleanup C's merge/publish gate and platform PR 19's later apply gate are
distinct. Before evaluating the runtime gates below, the archived PR 17
normalization receipt for the unchanged A tuple must prove Serve revision 046,
v18, zero
pickle/noncurrent rows, exact 2/2/2 Pods and writer leases, and one immutable
image digest. Publisher B must be merged, its expected pre-adoption run must
have failed closed without publishing, and both platform PR 18 account applies
and readbacks must have passed without creating a Helm revision. Missing
evidence blocks C; it never permits a legacy path.

After the runtime gates pass, C may merge and the canonical roles publish its
immutable Serve049 image/chart tuple. PR 19 must not merge, plan, or apply until
that exact publication receipt and all final platform absence gates pass. PR 19
then pins C and upgrades the existing split topology in place; it does not
create or replace that topology.

Generalized-binding removal additionally requires zero legacy-capable
participants, zero active or unsettled old-handler requests, zero unbound
non-pool rows requiring recovery, and typed settlement of all seven incident
rows without fabricated receipts. It requires one controller restart and
ordinary service update, readiness/+10/+30, one full 180-second authority
horizon plus the longer stale-writer/quiescence horizon, bounded manager-lock
hold time, fresh provider-free routes, broker conservation/no paid spill, and a
rollback rehearsal. One poisoned association must not block healthy probe,
route, autoscaler, or sibling-pool progress.

1. source PR #1451 and the v18 precursor PR #1483 are merged, PR 14's exact
   split-A receipt is accepted, and PR 17's normalization receipt names the
   unchanged full split-role A fleet;
2. production reports `SEQUENCED_ACTIVE` and cannot demote;
3. at least three consecutive observation periods complete without a stale
   writer overwriting a successor, a map-authentication failure caused by
   current code, or a receipt-accounting error;
4. at least one controller restart and one ordinary service update complete on
   the sequenced path;
5. every nonterminal protocol-v2 fill row is fully attributed, or every
   remaining unattributed legacy row has become terminal and stayed absent for
   one complete 180-second authority horizon;
6. no new fill row appears without a positive database-assigned admission
   sequence after activation;
7. ordinary traffic, no-paid-spill, `max_replicas`, two-context concurrency,
   and the deployment-owned Kueue reclaim contract pass; and
8. every active service version uses worker projection protocol v4 and no
   ordinary launch has consumed the v1 decoder for one complete 180-second
   authority horizon; and
9. after one complete controller-fleet rollout, no non-`INACTIVE`, non-`DONE`
   managed-job row has a null or partial slot identity; separately, for one
   complete controller-instance stale window, no allowlisted daemon ID exists
   in request, queue, or retention state and no live/recent writer advertises
   a legacy `daemon:` handler; and
10. the cleanup branch's final-state tests pass with the compatibility code
    physically absent, while fresh-install `UNAUTHORIZED` authorization and
    active fix-forward reauthorization both pass through the same command and
    require the stable Pod -> ReplicaSet -> Deployment owner chain. Tests also
    prove chart RBAC retains `get` on ReplicaSets, the protocol-v1 worker
    projection decoder is physically absent, and schema-5 allocations plus
    exact v18 replica attribution still round-trip and fence correctly. Source-
    absence checks reject an unbound non-pool handler/recovery branch, old
    ordinary handler alias, global recovery wait, cluster-name authority,
    process-map authority, or synthetic quiescence/backfill helper.

This gate is intentionally short and fix-forward compatible. It proves the
old path is no longer needed without imposing a 24-hour soak or a GPU/BCL
canary. If it fails, fix the feature or cleanup branch forward; do not reopen
legacy activation.

## Open gates

- [x] Deploy Serve053 recognition and empty durable-intent recovery as exact
  Helm release `1.1.1377`; prove the controller advances route generations and
  the original provider-absent rows retire without manual deletion.
- [x] Merge and source-qualify PR #1604's Serve054 PostgreSQL provider-proof
  receipt at `5d473147dfbaecead6b1501f923f47abf58adfe5`; three fresh exact-head
  CI passes prove the deterministic owner/parked-loser contract.
- [ ] Deploy PR #1604's merged source by direct Helm fix-forward and complete
  live readback plus cross-Pod production qualification. The shared five-second
  operation/freshness horizon has no failure cache or fallback.
- [ ] Merge and deploy the implemented atomic PostgreSQL
  replica/association/request receipt. Its feature change already removes
  protocol-v2 direct/non-atomic admission and rejects reserved fill at the HTTP
  surface; no system-identity or RWX/EFS correctness fallback remains.
- [ ] Merge and direct-Helm deploy the canonical-v4 projected-worker
  runtime-readiness fix-forward, then prove exact-UID Ready in east and PHX and
  prove a failed base bootstrap never publishes provider success. This gate has
  no schema, EFS/RWX, KubeRay, Terraform, Terragrunt, or platform-pin change.
- [x] Open cross-linked draft projection cleanup PR #1619. Keep it draft until
  a complete protocol-v4-capable cohort, retained-row and in-flight/provider
  evidence, east/PHX/generic/no-paid proofs, and the full 180-second
  stale-authority/quiescence horizon are complete; then remove the v1/v2/v3
  projection readers through that stacked cleanup.
- [ ] Open the cross-linked Serve056 cleanup and, only after a complete capable
  cohort, zero nullable owner tuples, no old writers, backups, and the full
  stale/HA production horizon, make service owner columns `NOT NULL` and remove
  the one-shot attestation transition.
- [ ] Adjudicate every exact PRESENT/quiesced launch through the existing
  generation/UID-fenced cleanup graph; UNKNOWN or replaced identities remain
  quarantined rather than inferred absent.
- [ ] Prove a large grant assigns every compatible free GPU within the bounded
  convergence target, with zero paid spill and no new ambiguous capacity debit.

- [x] Deploy and qualify the generalized binding, demand/route projections,
  G1S precursor, provider-independent routes, P2d durable actuation intents,
  and the supply-aware paid residual as direct Helm revision 418 / release
  1.1.1325 without promoting service authority.
- [x] Record exact terminal failure evidence for H200 attempts 53925--53933:
  every request failed closed on the missing PHX Kueue label and no attempt
  remains phantom capacity.
- [x] Implement bundle schema v3 with nullable per-context Kueue admission,
  `skypilot-pool-sa`, unmanaged east, and the exact PHX
  `be`/`skypilot-be`/`be-ls` contract in PR #1529.
- [x] Dark-verify PR #1540: repeated zero-demand H200 broker rounds created no
  legacy replica rows, launch threads, or `sky.launch` requests and introduced
  no paid claims. Later paid launches coincided with authenticated nonzero
  demand and therefore do not invalidate this gate.
- [x] Implement and deploy schema v4's single-path scheduler contract: east retains the
  exact `gpu-binpack-scheduler` Deployment; PHX uses `default-scheduler` and
  must attest Kueue TAS feature gates plus ResourceFlavor `topologyName`.
  Bundle, policy, live-snapshot, digest, negative-drift, audit-cache, and PHX
  diagnostic admission proofs passed before revision 423.
- [x] Create and apply the exact east and PHX audit IAM roles, EKS access
  entries, and RBAC using the already-pinned Terraform module. The namespace
  ServiceAccount and exact read contracts are proven in both spokes.
- [x] Snapshot/hash the PostgreSQL-backed API-server config and update it only
  through the audited workspace-config transaction. Add the complete
  server-owned `mt_hybrid` worker identity/projection inputs; do not patch the
  database or create a platform runtime pin. Exact cache/scratch, priority,
  timeout, identity, and hostPath inputs are committed and applied.
- [x] Remove task-owned Kubernetes Pod config, identity, priority, scratch, and
  provisioning overrides from the canonical service YAML and set
  `min_replicas: 0`. Version 61 captured the retired PHX scheduler and remains
  ineligible; version 62 proves the exact non-null east and PHX projection
  shape required by the clean activation successor.
- [x] Deploy the RequestSigner credential-provider fix after revision 427's
  fail-closed preflight without widening the writer role. Revision 428 proves
  both AWS and Kubernetes reads use the exact assumed audit sessions; exact
  east Pod Identity association `a-rsvzwdtaesxvxorkh` is absent.
- [x] Close superseded Platform PR #8649 without deploying it; the shared and
  partition admission policies remain unchanged.
- [ ] Deploy the schema-v5 SkyPilot attester that removes admission-policy and
  binding reads and obtain one fresh successful two-context preflight. No
  Terraform, platform Helm change, runtime pin, Kueue release, or second
  scheduler is part of this gate.
- [x] Commit and elect version 62 whose non-null immutable east and PHX worker
  projections exactly match the corrected server configuration. It proves the
  projection generator but is not an activation candidate because its
  `v5.44.1-boltz-2` model image was intentionally rolled back by Platform PR
  #8635.
- [x] Commit and elect clean demand-gated activation successor version 63 on
  the reviewed `v3.682.2-boltz-2` source with the corrected config and exact
  three non-null projections. Exact readback proves `min_replicas: 0`, fill
  floor 0, `utilization_gate: true`, and controller-applied lifecycle epoch 82.
  It differs from version 62 by the intentional model rollback and from the
  final backfill version only by `utilization_gate`.
- [ ] After the generic-binding, sequenced, durable-demand, and
  durable-actuation promotions below, apply the full-backfill policy and prove
  one automatically
  generated zero-cost PHX H200 Pod is admitted
  through `be` -> `skypilot-be` with `be-ls`, the -1000/`Never` Pod priority,
  `skypilot-pool-sa`, `default-scheduler`, a Kueue TAS assignment, and exact
  H200/cluster identity. Do not create a temporary floor, manufacture traffic,
  use direct fill, or launch paid compute for this gate.
- [x] Implement P2d Serve052 grant-before-row actuation in PR #1537 and deploy
  it dark. Under a held physical pool lane, require one bounded intent, zero
  replica/request rows, and sibling pool progress; production evidence remains
  part of activation qualification.
- [x] Resolve PR #1524 by semantic comparison rather than merging its
  conflicting 109-file branch. G1 recovery contracts shipped through
  #1519/#1526/#1527/#1528; #1524 is closed as superseded.
- [x] Implement the surgical durable logical-snapshot bridge without schema,
  Helm or Terragrunt changes. Focused tests prove exact feed/target
  generation equality under both locks, feed/request/autoscaler mismatch
  rejection, immutable target/snapshot publication during a forced
  same-generation interleaving, stale and plan-failure fail-closed behavior,
  publication-rejection and final-currentness actuation fences, and absence of
  a new teardown path. Legacy half-publication tests additionally reject a
  duplicate/regressed capacity snapshot and a regressed target while retaining
  the intentional newer-capacity/older-target contract. This supersedes only
  #1506's controller-local `+ 1` generation hunk.
- [x] Implement PR #1561's exact-head hardening: one read-only repeatable-read
  source snapshot, its unchanged bounded authority token, the canonical
  protocol/lifecycle/service/replica lock order, a service-row-serialized
  PostgreSQL logical-retirement commit, explicit rejected/ambiguous outcomes,
  and legacy normalization through the same strict `N + 1` seam. Controller
  startup also adopts an exact admission-precommit crash row as the canonical
  strict-idle reversible precommit; it cannot reconstruct a worker until a
  genuine `N + 1` and fresh idle proof. Real-PostgreSQL retirement/takeover
  races, live-update handoff, restart recovery, and manager lost-ack tests pass;
  no schema, storage, Helm, Terragrunt, or provider path is added.
- [x] Merge the capacity-authority takeover fix in PR #1562. PostgreSQL tests
  prove stale predecessor reports
  and pre-row intents cannot actuate, a fresh complete report bound to the
  replacement route restores planning, stale persisted demand ownership is
  repaired without epoch changes, and a concurrent loser changes nothing.
- [ ] Deploy and qualify the merged durable-route HA promotion source with the
  bridge, PR #1561 exact-head hardening, and #1562 takeover fix before authority
  activation. Verify feed generation `N` publishes both
  logical target and manager evidence at `N`, a repeated `N` cannot release
  adopted retirements, and a fresh `N + 1` lets the existing manager path
  commit through the exact database seam and resume normal evidence-backed
  convergence. Confirm stale/unavailable reads revoke the full manager pair
  and no ambiguous outcome starts its original worker. Then exercise one
  controller-child restart and one controller-Pod takeover after promotion.
  Each replacement must preserve bound planning and reconstruct HA
  promotability from the exact durable route generation with no
  controller-sync side effect; a mismatched generation or routing URL set must
  leave the selected slot healthy, and bounded role telemetry must record the
  closed/open gate and cutover CAS result. These production failover exercises
  remain required before the cleanup horizon opens.
- [ ] Merge and deploy canonical fresh-service birth, then move
  `boltz-l4-fleet` onto it. The preferred clean cut, because interruption is
  acceptable, is evidence-backed normal teardown followed by recreation; do
  not manually delete rows or bypass quiescence/provider proof. Read back one
  service row at generic `bound`, `DURABLE_PROJECTED`, `DURABLE_FEED`, and
  `DURABLE_INTENT` epoch 1, with one birth incarnation and no committed legacy
  interval, then prove the claimed controller has rebound generic/demand/fill
  authority and published fresh route evidence. If recreation is not possible,
  the retained-row path must explicitly promote ordinary binding and atomically
  promote demand plus actuation; no reconciliation tick may observe an
  intermediate `DURABLE_FEED`/`DIRECT_REPLICA` pair. Resource-action authority
  remains gated by its separate shadow horizon. Either activation is one way
  and fix-forward only.
- [x] Merge Platform PR #8652 after CI and review. It is a service-spec change
  only: no Terraform/Terragrunt or platform runtime pin is part of this path.
- [ ] After the successor controller rollout, apply #8652's production
  full-backfill service update from the clean demand-gated activation version.
  Prove the known-good model image, secrets, worker projections, endpoint, and
  every other service field are unchanged. Test remains demand-gated.
- [ ] With zero authenticated demand, prove every fresh, authenticated,
  policy-compatible reclaimable zero-cost slot granted to this service is
  covered by a durable intent or a correctly attributed admitted,
  provisioning, or ready replica in the exact accelerator-width unit. Reconcile
  the count from physical observations through broker grants, intents, Kueue
  reservations, Pods, and ready replicas. Every residual must carry one typed
  fresh reason; ordinary paid claims and new Spot launches must remain zero.
- [ ] With authenticated live demand, prove 80--90 eligible H200 workloads are
  durably submitted within a few minutes, multiple Pods initialize
  concurrently, and a held pool lane does not block another pool.
- [ ] Prove reserved supply is committed before the paid residual, no new Spot
  launches occur while compatible reserved supply covers demand, and the 151
  existing Spot retirements converge from a fresh exact-card durable-demand
  snapshot. Re-query provider truth until the 116 observed live GCP instances
  are absent, let the normal cleanup path remove the 36 already-absent targets,
  and confirm `SHUTTING_DOWN` resources cease billing without manual row
  deletion.
- [ ] Adjudicate orphaned/ambiguous historical rows only from durable
  quiescence and provider evidence. Keep historical failed rows out of current
  capacity, placement, and UI totals without deleting evidence-bearing rows.
- [ ] Promote the fresh durable demand report and verify the dashboard exposes
  confirmed processing, queued, in-flight, rejected, completed, and freshness
  independently of provider/controller stalls. The underlying LB report and
  prediction-history rows are already fresh; the live gap is the selected
  legacy demand source. Also separate paid, reserved, provisioning,
  shutting-down, and historical-failed replica classes.
- [ ] Pass typed provider present/absent/unknown/replaced,
  legacy-real-effect, lost-ACK, poisoned-row progress, broker conservation,
  no-paid-spill, and full restart/adoption tests.
- [ ] After the complete capability, stale-writer, route, demand, and
  actuation horizon proves zero old-path use and zero unsettled unbound work,
  complete the deletion-only
  `cleanup/remove-legacy-serve-authority-transitions` branch from current
  source. Its additional gate is that every live central-PostgreSQL non-pool
  service was canonically recreated or evidence-backed retired, and no
  production registration omits a lifecycle epoch. Closed/superseded PRs
  #1506/#1510 reserve no schema or API heads and must not be revived.
- [ ] Keep atomic-authority cleanup #1556 stacked on feature PR #1555 until
  the immediate, +10 minute, +30 minute, and complete stale/quiescence horizon
  proves no partial authority pair, paid spill, or ordinary-traffic regression;
  then merge it before declaring phase 2e complete.
