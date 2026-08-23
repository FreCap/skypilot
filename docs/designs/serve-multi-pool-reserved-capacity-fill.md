# Multi-pool SkyServe reserved-capacity fill

Last updated: 2026-08-23

Status: the canonical PostgreSQL-authoritative reserved-capacity admission and
assignment path is source-complete and its primary occupancy contract was
production-proven. The protocol-v7/cohort-7 bootstrap and bounded rolling-
cleanup correction is source-complete and focused unit/real-PostgreSQL-
qualified in the current branch. It is not yet merged, published, Helm-
deployed, activated, or production-proven.

The current live incident boundary is later than the occupancy evidence below.
The supported v6 `serve down --purge` removed the East and PHX provider
workloads, and both direct provider inventories are now zero, but
`boltz-l4-fleet` lifecycle 93 is `FAILED_CLEANUP` with nine retained replicas in
`UNKNOWN`. V6 cannot finish that database finalization. The v7 N-1 typed
finalizer is required; operators must not delete those rows or their evidence
manually. Reconciliation remains `SEQUENCED_ACTIVE` at generation 34 until the
exact rollout below advances it once.

Before that supported teardown, lifecycle 93/version 1 had
`min_replicas: 0`, zero fill floor, `utilization_gate: false`, immutable
server-owned worker projections, no task-owned Kubernetes overrides, and no
EFS/PV/PVC correctness dependency. PRs #1676 and #1677 had supplied bounded
historical teardown authority and retired lifecycle 91 through normal
evidence-backed cleanup.

The synchronized pre-PR-#1678 census proved 100% occupancy of the capacity the
two existing scheduler domains made available to this service: East had 58 of
58 healthy compatible scheduler-fit GPUs Ready (13 A100 and 45 A100-80GB), and
PHX had 75 of 75 Kueue-admitted H200 Workloads Running and Ready. SkyPilot had
submitted 78 PHX Workloads; the remaining three were visibly queued because
the unchanged Kueue/topology scheduler did not admit them, so they were not in
the policy-admissible denominator. All 133 Ready replicas were reserved,
zero-cost, and non-Spot. PostgreSQL reported zero paid claims and zero paid
waiters, and direct provider inventory reported zero fleet Spot clusters. The
service stayed on lifecycle 93/version 1 with immutable service hash
`3f767fce-a4ec-4869-bbcb-3f7f77456f89`.

PR #1678 is merged at
`38ec2434245c2286d36c81d371834f97bde4f43c`. It closes two runtime defects
found while preparing the final HA proof: a rejected duplicate `serve up`
could advance the durable lifecycle epoch, and Kueue observer publication
took exclusive protocol/lifecycle/service locks in an order that could
deadlock the autoscaler. The correction preserves writer exclusivity but uses
the canonical protocol `FOR SHARE` -> lifecycle `FOR SHARE` -> service
`FOR SHARE` read prefix before the observer-specific exclusive suffix. Focused
unit tests, 24 real-PostgreSQL observer/writer tests, and a forced former-cycle
interleaving passed. It changes only SkyPilot Serve source and tests; it adds no
schema, migration, Kueue object, Terraform/Terragrunt resource, EFS/PVC,
KubeRay path, platform runtime pin, or task-placement override.

The immutable `1.1.1448` image is published as
`255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz:1.1.1448@sha256:ba7602e63363baaa35210b8aafd429b7050c1a3fa5861a0bc9c58939c9fecb3c`.
The `1.1.1448` chart digest is
`sha256:658276a5dcbcf4aae2438c03d9217b8adcef02b4725f9cacba3866bb2312e49b`;
its rendered source delta from `1.1.1447` contains no Kueue object. Direct Helm
revision 572 completed with `--reuse-values`: all two API, two controller, and
three executor Pods are Ready on the exact image digest above. Reconciliation
is `SEQUENCED_ACTIVE` at generation 34 and binds that complete seven-writer
inventory. The control plane remains PostgreSQL-authoritative with Helm storage
disabled.

The rollout forced a real controller-Pod replacement. Durable ownership moved
from controller IP `10.30.1.190`, owner epoch 3, and the predecessor controller
incarnation to controller IP `10.30.0.98`, owner epoch 4, and a new durable
incarnation. Lifecycle 93, version 1, the immutable service hash, and the
endpoint remained unchanged. A live duplicate same-name `serve up` then
rejected without advancing lifecycle, changing version/hash, or rotating owner
epoch 4. This proves both PR-#1678 runtime corrections at their production
boundaries.

| Slice | Source | Deployed | Activated | Production proof |
|---|---|---|---|---|
| Bounded v5 teardown and clean recreation (#1676/#1677) | Complete | Complete in `1.1.1447` | Complete at gate 33 | Complete: lifecycle 91 purged normally; lifecycle 93 created |
| Scheduler-authorized reserved fill | Complete | Complete | Complete | Complete: 133/133 Ready; three additional PHX Workloads queued by unchanged Kueue |
| Current-request telemetry | Complete | Complete | Complete | Complete through cross-Pod takeover with fresh two-reporter projection and stable LB HA |
| PR #1678 duplicate-up and observer lock-order correction | Complete at `38ec24342` | Complete in Helm 572 | Complete at gate 34 | Duplicate-up, takeover, 180-second stale horizon, and +30 control-plane/HA/error checks passed; the coincident eight-card v6 readiness wave was superseded by supported provider teardown |
| Kueue non-mutation | No source change | No rendered chart change | Not applicable | Complete: both normalized PRE and POST hashes are equal |
| Paid residual | Complete | Complete | Complete | Source/real-PostgreSQL tests qualify commit-before-residual and drain; idle plus harmless-request production proof created no paid capacity |
| Protocol-v7 bootstrap and N-1 terminal-cleanup correction | Source complete; focused unit/real-PostgreSQL qualification complete | Not deployed | Not activated | Not proven; required to finish lifecycle-93 cleanup and recreate the service |

At approximately 03:07 UTC, after takeover and gate-34 activation,
`sky serve status` reported 133 Ready of 136 current reserved units after
23 minutes 37 seconds of lifecycle uptime, with the endpoint unchanged. East
remained 58/58 Ready (13 A100 and 45 A100-80GB). PHX remained 75
Kueue-admitted H200 Workloads Ready and three additional submitted H200
Workloads queued/pending under unchanged Kueue. All 136 units were attributed
reserved, zero-cost, and non-Spot; paid claims and paid waiters were both zero.

The fresh approximately 03:10 UTC allocation readback kept claim/service
generation 7778 current at allocation generation 17. Exact grants and edge caps
were A100 13, A100-80GB 45, and H200 78. Raw observed free was respectively 0,
0, and 3, while spendable free was zero for every card because the three PHX
units were already debited by queued intents. `fill_target` and
`fill_free_slots` were both zero. The corresponding service census was A100 13
Ready, A100-80GB 45 Ready, H200 75 Ready plus three provisioning, with observed
Ready 133 fresh at age 7.85 seconds.

The later +30 readback captured a genuine live capacity-release event rather
than an accounting discrepancy. East had 328 physical GPUs: research owned 262
Running GPU requests, and the fleet immediately expanded from 58 to 66 GPU
Pods by creating replicas 150--157 on the newly free eight-GPU A100-80GB node.
At the sampled boundary 60 fleet Pods were Running and six of the new Pods were
Pending bootstrap, so 262 research plus 66 fleet assignments covered all 328
GPUs. The observer correctly reported raw A100-80GB free moving from zero to
eight while the canonical spendable value remained zero: all eight slots had
already been durably debited before provider work. This is direct production
proof that a newly released compatible reserved slot is discovered and
refilled automatically without a Kueue or infrastructure change.

The eight-worker wave did not then make normal readiness progress. Inspection
of replica 150 found the 90-second wrapper timeout killed `apt-get` while its
child dpkg PID 2654 retained the package lock, leaving interrupted dpkg state.
OpenSSH was skipped, `/etc/ssh/sshd_config` was absent, and the following `sed`
failed. The outer background wrapper inherited `set -e` and exited before
writing `/tmp/apt-ssh-setup.failed`; PID 1 therefore waited indefinitely while
the runtime/Ray marker was complete and both apt success/failure markers were
absent. The other new workers showed the same stuck boundary. This is a
SkyPilot worker-bootstrap bug, not Kueue admission, provider capacity, node
pressure, or infrastructure policy.

The canonical correction is one fresh protocol-v7/cohort-7 writer with a
unique v7 marker. The projection authenticates the complete command, args,
lifecycle environment, and bootstrap environment through the existing full-
script SHA. It never hard-interrupts an apt/dpkg critical section, retains
apt's bounded Acquire network deadlines, and uses the existing 30-minute whole-
Pod startup deadline as the safe ephemeral transaction boundary. After an
ordinary nonzero install result it runs `dpkg --configure -a` before
`apt-get -f install` and retry. It also guarantees a typed apt/SSH failure
marker even when a step fails under errexit. PID 1 scans every step's failure
marker on each poll, so a runtime/env failure cannot hide behind unresolved
apt setup, and terminates rather than waiting forever when setup cannot
complete. Protocol v6
becomes cleanup/settlement-only through the existing historical validator. A
silent v6 script edit is forbidden because it would reproduce the earlier v5
discriminator collision. Rollout therefore requires a homogeneous v7 SkyPilot
Helm cohort, supported v7 finalization of lifecycle 93, fresh same-name
recreation onto the immutable v7 projection, reauthorization, and a new
refill/readiness proof. It changes no Kueue, Terraform/Terragrunt, EFS, KubeRay,
or other platform object. The former eight v6 bootstrap-hung rows/Pods supplied
the incident evidence. After supported provider-side teardown, lifecycle 93
remains `FAILED_CLEANUP` with nine `UNKNOWN` rows. They must retire through the
ordinary typed provider-absence and quiescence protocol. No row, request,
association, Workload, or Pod may be manually deleted to accelerate the
rollout.

Request telemetry remained fresh and complete through takeover: recent request
count 0 over 60 seconds, requests/second 0, in-flight 0, confirmed in-flight 0,
unknown replica reporters 0, queue depth 0, recent rejects 0, and two reporters.
The approximately 03:10 UTC sample was fresh/complete at age 3.23 seconds.
Load-balancer HA reported true with active slot `b`, cutover generation 2, and
state `STABLE`.
At 03:06:23 UTC all 66 PHX nodes were Ready and no node reported
`DiskPressure`, `MemoryPressure`, or `PIDPressure`. By 03:06 UTC the complete
180-second stale/quiescence interval had elapsed; current API, controller,
executor, and load-balancer logs since 02:43 UTC contained zero
`deadlock detected`, `Provider evidence expired`, `POST_EFFECT_AMBIGUOUS`, or
`KueueAdmissionConflict` signatures. The 03:13:53 UTC +30 core readback also
kept lifecycle 93/version 1, service hash, controller owner tuple, gate 34,
claim generation 7778, all seven writers and both load-balancer slots on the
exact digest with zero restarts, Kueue admission, telemetry, and zero targeted
error counts stable. Its newly released A100-80GB capacity was synchronously
and durably claimed as described above.

The lifecycle-91 account below is retained as incident and qualification
history; it is not current service state.

That wave exposed one remaining SkyPilot identity error. Runtime pool budgets
were included in the complete claim set's semantic hash. Query-to-row-scan
materializations changed `effective_cap` from one heartbeat to the next even
though service version, topology, projections, reclaim policy, and configured
fill semantics were unchanged. Production claim generation advanced
7764→7765→7766→7767 while valid batches were still dispatching. Each
advance cleared the allocation head, created publication gaps, and rejected or
reauthorized stale pre-commit snapshots. Already committed intents retain their
independent immutable authority; this evidence does not attribute historical
cleanup to claim-generation movement. The allocation churn is avoidable
controller work, not a Kueue admission or capacity limit.

The steady-state identity split is explicit: claim generation names immutable
claim and launch topology. It covers protocol, service incarnation and
version, worker projections, ordered pool membership and position, access
context, physical UID, accelerator cards and width, configured service maximum,
fill floor, weight, and utilization-gate mode. `holdings_fill`, `launchable`,
`global_headroom`, `utilization_ceiling`, utilization state, partitioned edge
floor, and `effective_cap` are runtime allocation inputs. They update
atomically in place without rotating claim generation. An authority-affecting
edge-cap change makes the old allocation unreadable; a changed broker grant or
feed advances its pool epoch; and a newly published complete map gets its own
allocation generation. Holdings, headroom, utilization state, and a derived
floor need not independently invalidate an otherwise identical map: current
row and pending-intent debits remain mandatory at grant time, and a changed
floor takes effect through the next bounded broker round/epoch. The configured
service maximum is hashed independently from dynamic headroom.
Already committed intents retain their immutable post-commit authority and
remain occupancy debits until settlement. Topology, service version,
projection, or configured-policy changes still rotate claim generation.

The same correction removes the input noise instead of merely tolerating it.
The broker already computes exact-card spendable free after debiting occupancy
that the raw observation cannot prove it saw: admission or first-success
materialization beyond the captured high-water, malformed/missing sequence
markers, live pending intents, and cleanup-unproven ambiguous rows. Pods proved
materialized before the observation are already excluded by the provider/Kueue
measurement and are not debited again. Committed protocol-v2 rounds publish
that slot mapping additively as `$skypilot-spendable-free-v1` beside the
unchanged authenticated `$skypilot-observed-free-v1` slot mapping. The
committed observation ledger owns raw GPUs; the broker converts both envelope
mappings exactly once with the authenticated `broker_slot_width`. The round-
consistent pool-cap hint is exactly
`round sum_holdings + sum(spendable free slots)`, not observed free plus current
or post-query holdings. `allocate_fill_pool_budgets()` separately retains
`max(current local holdings, round-consistent hint)`, so a replica admitted
after round publication cannot be added to stale free a second time. Exact-card
clipping may make this a conservative lower bound on aggregate entitlement for
ambiguous composite-card debits; it is exact for unambiguous and single-card
pools and never fabricates a card.

The additive metadata is emitted only by committed sequenced rounds after the
new homogeneous writer gate is active. V1 and legacy round bytes remain
unchanged. The new key is stripped from the service-feed payload before
pool-epoch comparison and never replaces raw observation provenance. Readers
require committed protocol-v2 provenance, matching slot width and timestamp,
closed canonical observed-slot and spendable-slot mappings over exactly the
pool cards,
non-boolean nonnegative integers, and pointwise
`spendable_slots[card] <= observed_slots[card]`. A valid all-zero mapping is
authoritative zero. An absent key from an N-1 committed round or blackout is a
non-widening `max(current holdings, previous cap)` compatibility carry; a
present malformed key withholds to current holdings. Claimant-specific feeds
are never reverse-inferred into pool free. Because `1.1.1443` does not strip
the new key from epoch comparison, only an explicitly reauthorized homogeneous
successor may emit it after this correction is deployed.

The supported teardown of test-only lifecycle 91 began after the v6 rollout
and reached `FAILED_CLEANUP` with no endpoint. Releases `1.1.1440` and
`1.1.1442` emitted different bootstrap semantics under the same protocol-v5
discriminator, so every v5 graph remains historical cleanup-only after the
protocol-v6 cutover. PR #1674 accidentally applied the fresh-admission
`require_current_protocol=True` check while reconstructing the immutable Kueue
identity of those retained v5 rows. The check fails before the existing
uncached, physical-cluster-UID-fenced provider absence probe can run, so normal
retirement conservatively retains the graph.

The fix is an explicit teardown-versus-act boundary, not a fallback. Fresh
admission, capacity accounting, and every provider-effect start continue to
require exact-current protocol v6. A separately named classifier is callable
only by the two teardown/retirement readers. It accepts the exact strict
projection already bound to a retained row only within the current and two
immediately preceding projection protocols; N-3, future, mixed, malformed, or
digest-divergent bytes fail closed. It returns only scheduler-lane mode and the
existing immutable Kueue identity, never fresh admission or provider authority.
Historical decoding still validates the canonical projection digest, service
lifecycle/version, protocol-v2 pool identity, physical cluster UID, allowed
location, replica/intent/association graph, terminal execution quiescence, and
the uncached provider-absence receipt before deletion. `PRESENT`, `UNPROVEN`,
malformed, or digest-mismatched state remains retained. Teardown, not manual row
deletion, remains the only authority that adjudicates these rows. The final
proof uses one clean same-name recreation only after the cleanup correction is
homogeneous, the old lifecycle is normally purged, and the v6 cohort is
explicitly reauthorized.

The 2026-08-23 pre-fix read-only census confirms that this is retained state,
not live provider capacity. Lifecycle 91 has 160 replica rows, 160 matching
committed intents, and 160 matching launch associations, all non-Spot, with
zero paid claims. Both exact Kubernetes namespaces have zero fleet Pods, all 75
durably named PHX Workloads are absent, and the central cluster table has zero
fleet cluster records. The purge must therefore obtain and persist the existing
fenced absence receipts; it must not infer absence from this operator census or
delete rows directly.

Source qualification for the cleanup correction passed seven focused tests
against real PostgreSQL. They cover default v5 rejection, explicit exact-digest
historical decode, digest-tamper rejection, end-to-end v5 absence publication
and whole-service retirement, unchanged current-protocol retirement, and the
capacity reader retaining v5 as exact-shape unknown. Independent adversarial
review found no permission path from historical decode into fresh admission,
capacity authority, or provider effects. The follow-up capability hardening
adds dynamic N/N-1/N-2 and N-3 rejection tests and removes the reusable boolean
escape hatch; it changes no cleanup evidence or provider behavior.

Normal evidence-backed teardown removed lifecycle 89. Lifecycle 90's 22
protocol-v5/cohort-4 launch graphs were retained until durable quiescence and
fresh provider-absence evidence allowed the supported cohort-5 cleanup path;
no row was manually deleted. Lifecycle 91 was then recreated normally.
Production observations show PHX fleet Pods admitted on Simone's unchanged
`be -> skypilot-be`, `be-lt=11`, Pod priority `-1000`/`Never` lane, while East
uses its existing scheduler-fit boundary. No ClusterQueue, LocalQueue, Kueue
Cohort, flavor, quota, borrowing, preemption, priority, scheduler, research
workload, Terraform/Terragrunt, KubeRay, or platform pin change is required.
The obsolete unmounted `skypilot-state-rwx` PVC, retained PV, and sole EFS
access point remain absent; `storage.enabled=false` remains.

PR #1670's bounded four-item manager handoff is retained in PR #1671 and the
successor. The staged SkyPilot-only resource proof keeps each API request at
56 GiB with a 110 GiB limit and each of three executor requests equal to its
48 GiB limit. All executors previously reported exact downward-API
`51539607552`-byte input and startup with 64 long workers. Those reservations
change no service placement, Kueue policy, or GPU authority.

The preceding clean lifecycle proved three source corrections. PR #1667 makes
the controller's one frozen Kubernetes resource the only protocol-v2 placement
authority and disables both initial and retry optimization for that exact
request. PR #1668 parks intent before materialization whenever the exact
provider proof lacks its 20-second handoff reserve, while malformed proof,
authority drift, and every post-effect ambiguity remain terminal. PR #1669
keeps Kubernetes manifest deserialization inside the already selected physical
context; it removed the `KubernetesPhysicalClusterIdentityError` that formerly
failed existing Service/RBAC object decoding under a valid launch fence.

PRs #1670/#1671 removed the preceding controller manager head-of-line and
worker-bootstrap blockers. Before PR #1673, the next measured blocker was a
PostgreSQL lock convoy inside proof publication. Provider reads themselves
completed quickly (PHX approximately 2.6--3.7 seconds and East approximately
0.6--1.0 seconds), but publication took `FOR SHARE` on the singleton protocol
row after those reads. Large launch waves continuously held or queued
`FOR UPDATE` on the same row. The deployment proof owner therefore joined
18--20 launch/reducer waiters, hit its 200 ms statement deadline, let both
exact receipts expire, withheld the whole claim set, and prevented a fresh
broker round. Three-second launch retry polling sustained the convoy. Small
waves appeared only during brief receipt-fresh windows.

The deployed PR #1673 correction is SkyPilot-only: proof publication and
negative invalidation perform a lock-free READ COMMITTED revalidation of the
exact live generation and policy identity instead of joining the global zero-cost writer
mutex. Proof rows, consumers, and terminal guards remain bound to generation,
identity, context, and current live gate. A publication that races a gate
rotation may leave an old-generation historical row, but it is immediately
inert and cannot authorize admission or a provider effect; an old-generation
invalidation cannot delete its successor. The correction adds no fallback,
retry path, timeout increase, schema, migration, provider path, or
infrastructure change.

PR #1673 deployed that correction in release `1.1.1443`. Under the subsequent
148-Ready/eight-booting pressure wave both exact proof receipts renewed
continuously with sampled ages below ten seconds, and PostgreSQL showed no
proof-publication waiter on the protocol writer mutex. Proof publication is
therefore a closed production gate, not the current convergence blocker.

Dark worker startup then exposed an independent SkyPilot rootfs amplification:
each projected replica wrote approximately 598 MB of SkyPilot runtime, 649 MB
of uv cache, and 95 MB of uv-managed Python beneath `/root`. Seven replicas
therefore consumed roughly 10.5 GB of node rootfs and pushed two nodes to about
81% nodefs use even though the service already owned a bounded 20 GiB
memory-backed `/tmp`. This is a SkyPilot bootstrap-placement bug, not a Kueue
capacity, quota, or admission failure.

Projection protocol v6 is the narrow correction required before recreation.
Releases 1.1.1440 and 1.1.1442 assigned different bootstrap renderings to the
same protocol-v5 discriminator. Cohort 4 versus 5 keeps historical settlement
safe, but the projection alone is ambiguous and therefore every v5 row is
cleanup-only. For memory-scratch projected workers v6 owns exactly
`SKY_RUNTIME_DIR`, `UV_CACHE_DIR`, and
`UV_PYTHON_INSTALL_DIR` under `/tmp/.skypilot-runtime`, installs them as literal
Pod env so every fresh `kubectl exec` inherits them, and re-exports the same
values after trusted `runcmd` in the authenticated bootstrap. The existing
bootstrap SHA now covers those exact owned env entries as well as the script;
the existing render/create/adopt/admit/final-read checks reject drift. The uv
executable remains under `$HOME/.local/bin` (approximately 55 MB), and v1-v5,
`scratch.kind: none`, generic Kubernetes, and non-Serve paths keep their exact
historical read/settle behavior. Capability cohort epoch 6 fences new v6
effects while adjacent epoch 5 may settle already-owned work. Exact-current
projection v6 is also required at admission and every provider-effect start,
so decodability never grants replay authority. The correction adds no EFS/PVC,
schema migration, KubeRay, platform pin, Terraform/Terragrunt resource, task
resource, or Kueue object/change.

The protocol-v6/cohort-6 source patch is merged and deployed. Focused tests
cover fresh-write rejection for v1-v5, exact v5 version retry, adjacent-cohort
cleanup, mixed-fleet refusal, exact-current render/create/adopt fencing, and
the v5/v6 bootstrap identities. PR #1676 merged the historical teardown
decoder regression fix, and PR #1677 merged its bounded N/N-1/N-2 capability
hardening. Normal evidence-backed purge and clean lifecycle-93 recreation are
complete. The exact-current writer rule remains unchanged: historical
decodability authorizes terminal cleanup only, never fresh admission, capacity
authority, or provider effects.

PHX success is defined exclusively by Simone's unchanged Kueue policy. SkyPilot
must submit every fresh reserved grant; every Workload that Kueue marks
`QuotaReserved=True` and `Admitted=True` must map one to one to a durable
intent, replica, request, and provisioning or Ready runtime. A raw idle GPU is
not a failure when Kueue withholds the submitted Workload or the immutable task
is not scheduler-fit. SkyPilot must not change ClusterQueues, LocalQueues,
Cohorts, ResourceFlavors, quotas, borrowing, preemption, priorities, scheduler,
or research workload specifications to improve occupancy. East has no Kueue
boundary and uses its existing scheduler-fit compatible-capacity denominator.
Two reproducible normalized PRE/POST snapshots prove that the PR-#1678 rollout
did not mutate PHX Kueue policy. Both cover exactly 40 objects: seven
ClusterQueues, seven LocalQueues, three ResourceFlavors, eight
WorkloadPriorityClasses, thirteen Pod PriorityClasses, one Kueue Deployment,
and one Kueue ConfigMap.

The canonical rollout comparison selects exactly those 40 named objects and
maps each one to the flat object
`{apiVersion,kind,name:.metadata.name,namespace:.metadata.namespace,spec,data}`.
It deletes null-valued keys only at that top level, sorts the array by
`(apiVersion, kind, namespace-or-empty, name)`, serializes the complete array
with `jq -cS`, and hashes the emitted bytes including `jq`'s trailing newline.
Its PRE and POST `sha256sum` is
`fd5af31d5d1570701e2a7b636691fa4efbf15d6efdba3ee8277d7f5b982d170d`.
A nested-metadata, recursive-null alternate over those same current objects
produces the distinct corroborating `1013f6df...` value; that difference is a
normalization choice, not policy drift. The former `2c8b2d9089f0f92d...`
stable-object recipe is retired historical evidence and must not be used as a
current PRE/POST rollout comparison. The older `b231f36e...` value likewise
had no reproducible current recipe and remains historical only.

Stable claim identity, unchanged-policy occupancy, zero-demand paid
suppression, fresh request telemetry, homogeneous `1.1.1448` deployment,
gate-34 activation, duplicate-up epoch preservation, cross-Pod takeover,
Kueue non-mutation, node-pressure health, and the full 180-second
stale/quiescence interval are production-proven on the clean lifecycle. The
+30 control-plane, HA, error, exact-writer, claim, and policy checks passed. A
coincident fresh eight-card A100-80GB release was fully claimed and submitted;
overall final convergence remains open until the source-qualified protocol-v7
correction is merged and Helm-deployed, finishes lifecycle-93 cleanup, is
activated once at generation 35 on a fresh immutable recreation, and proves
the new scheduler-authorized assignments settle Ready or to a typed terminal
result while the final paid census remains zero. The
rollout audit must then accept the result. Deliberately creating a billable uncovered-demand
case is not required for this closeout: commit-before-paid-residual, exact-shape
suppression, and paid drain are source- and real-PostgreSQL-test-qualified,
while idle production and the harmless authenticated request both created zero
paid capacity.
Mixed writer cohorts continue to fail the existing fleet barrier. Adjacent
cohorts may read, settle, recover, or clean already-owned work but cannot admit
a new request or enter provider I/O. N-2 never receives admission, adoption,
provider reconciliation/evidence-write, pre-admission redrive, PRESENT
teardown, or provider-effect authority. Its only operational boundary is final
replica-row retirement from an already canonical `PROJECTED`/`ABSENT`, zero-
paid, terminal, quiescent, pin-released frozen reserved-fill graph.

The permanent rolling-compatibility floor is exact-current for new admission
and provider-effect start, plus guaranteed N-1 read, route, settlement,
recovery, and teardown. Projection decoders for older versions may remain, but
they do not by themselves authorize an older capability cohort. The cleanup
classifier bounds immutable projection decoding to N/N-1/N-2, but the generic
capability fence still authorizes broad operation only for N/N-1 cohorts. A
separate N/N-1/N-2 terminal-cleanup predicate runs only after complete frozen-
profile, terminal/quiescence, projected pin-release, zero-paid, and canonical
post-quiescence ABSENT proof. Cohort rotation and either capability-tuple
demotion refuse to proceed while any association-backed replica row remains,
including one already in that exact terminal shape. The physical replica row
must retire while the service still advertises the association's old cohort
tuple; only retained association history with no matching replica record may
cross rotation or demotion. Such replica-free history is not trusted merely
because its replica disappeared: every row must independently be in a closed
`PROJECTED` or `PRE_EFFECT_TERMINAL` resolution with exact terminal evidence,
durable projection/tombstone markers, and a released pin. Required quiescence
and generic protocol/evidence fields must be internally consistent; an active,
unpinned, partial, or malformed association-only row fails closed.
Operators must not infer N-2 recovery, evidence acquisition, PRESENT teardown,
or effect authority from projection decodability.
Controller takeover may still rotate the service owner while terminal N-2
retirement is pending, but only after every retained replica has exactly one
current association and independently passes the exact already-
`PROJECTED`/`ABSENT` retirement classifier, or no retained replica rows remain.
Every retained graph must also carry the durable immediate-cleanup marker that
authorizes its physical replica-row retirement. The service must have zero
paid-capacity claims globally, and every replica-free association row must pass
the exact inert-history predicate above; an association-less replica, dangling
association pointer, orphan claim, active/unpinned association-only row,
drifted cleanup marker, malformed graph, or multiple association is a closed
failure. This N-2 branch changes only the
service controller owner and non-pool controller incarnation. Capacity demand/fill
authority, route leases, pending intents, and the complete historical
association row remain byte-stable. Normal takeover continues to rewrite
unsettled ordinary protocol-v1 and exact N/N-1 generic associations; malformed,
unknown, N-3, active, ambiguous, or nonquiescent N-2 state fails before the
service-owner CAS.

The following lifecycle-84 cleanup account is retained as incident history,
not current service state. Before the `1.1.1436` correction, normal fenced down
had removed every provider object, central cluster row, load balancer object,
request executor, queue row, retention pin, and paid-capacity claim but left the
service in `FAILED_CLEANUP`: 84 of 140 retained historical replicas had the
pre-job protocol-v2 shape that the then-current finalizer did not classify.
Seventy-three had no Kueue admission; eleven had one exact `INTENT_PENDING`
admission with no Pod identity or receipt. All 84 were PHX rows with one exact
committed intent/replica/association edge, terminal and quiesced request
history, canonical post-quiescence provider `ABSENT`, zero paid authority, and
zero queue or pin authority. The old exact-Pod loader rejected the pending
shape, while final retirement required a mutable immediate-cleanup marker that
was never persisted after the launch stopped pre-job.

The deployed corrective contract does not add a cleanup fallback. It routes only an
exact `INTENT_PENDING`/no-Pod row through the existing typed exact-Pod
`NOT_APPLICABLE` result into the provider-free lineage validator. That validator
accepts only a terminal whole-service `PROJECTED` pre-job association in
`NOT_STARTED` or `PROVIDER_IO`, with no service job and canonical durable
`ABSENT`; the admission may be absent or may be the one exact matching pending
row. It revalidates service lifecycle, committed intent, replica UUID,
generation, association, frozen profile, terminal request, and zero
paid/queue/pin authority before atomic admission/replica/intent deletion. It
performs no provider read on durable replay. `POD_WAITING`, `POLICY_ADMITTED`,
foreign or multiple admissions, later effect phases, stale/forged evidence,
and every live authority remain fail closed. Ordinary replica cleanup and the
normal admitted-Pod path are unchanged. Release `1.1.1436` exercised this path
through normal lifecycle-84 purge; the later normal lifecycle-85 purge confirms
the obsolete retained graph is no longer the live blocker.

The observer correction makes every explicit-context realtime Kubernetes
accelerator query use the immutable snapshot's policy-only allowed-cloud gate
followed by the existing exact-context credential/RBAC and uncached provider
reads. A policy-disabled context is a successful zero-capacity result, while a
failed credential, RBAC, transport, or measurement probe raises into the
observer's `BLACKOUT` path; it can never masquerade as authoritative zero and
withdraw confirmed reserved holdings. Non-realtime and implicit-context
catalog discovery retain the central cached credential path. The correction
does not stamp a central PostgreSQL identity onto derived child bytes, weaken
provider/physical-UID fences, or introduce a Serve-only catalog branch.

The allocation-generation-2 2026-08-22 read-only PHX census found all 64 H200
nodes Ready, 512 allocatable H200 GPUs, 402 consumed by research, and 110 raw
candidate slots; CPU and memory were not tighter constraints. The active
service submitted only a bounded prefix before proof expiry. Every submitted
Workload was immediately Kueue-admitted and its Pod scheduled; zero fleet
Workloads remained pending. East has no Kueue admission boundary. The
production denominator is evaluated continuously: every non-finished
SkyPilot Workload with `QuotaReserved=True` and `Admitted=True` must map one to
one to a durable intent, replica, request, and provisioning or Ready runtime.
SkyPilot must submit enough work to cover every broker grant; any remaining raw
idle PHX GPU is acceptable only when an exact submitted Workload is visibly
withheld by Kueue's unchanged policy. The convergence target is every healthy
compatible GPU this existing policy admits, not raw occupancy obtained by
changing Simone's ClusterQueues, LocalQueues, cohort, flavors, quotas,
priorities, borrowing, preemption, or scheduler behavior.
SkyPilot PR #1650's provider proof and flat-PHX attester, PR #1651 and its
teardown successors, and PR #1655's projected-worker finalization are all in
that deployed image. Live readback proves Platform PR #8824 released both
SkyPilot queues at generation 7. Its `skypilot-be` release is required and
effective for boltz-l4-fleet; its independent `skypilot-wa` release restores
the pre-existing general PHX SkyPilot workspace after this initiative's
temporary hold and is not a fleet dependency. The final path preserves the
implicit `shared-pool`, zero
explicit Cohort objects, and the existing `be-lt=11` WorkloadPriorityClass.
SkyPilot creates no Cohort, ClusterQueue, WorkloadPriorityClass, or second
scheduler.

Platform PR #8824 (`220edaf1`) is narrow. The fleet-required delta is
`stopPolicy: Hold -> None` on the pre-existing Simone-owned `skypilot-be`
ClusterQueue used by boltz-l4-fleet's PHX projection. Its identical
`skypilot-wa` delta is not used by this service, but correctly restores the
pre-#8797 active state of Simone's existing `rescluster-k8s-phx` SkyPilot
workspace after the abandoned topology migration's explicitly temporary hold.
Reverting the whole PR would stop fleet admission; re-holding only
`skypilot-wa` would disable that independent SkyPilot entry point and requires
its owner's explicit policy decision. Neither half adds HCL/Terraform
resources, and the PR changes no research queue, cohort, quota, preemption
policy, priority, IAM identity, namespace, or workload. The broader #8820
research-cohort experiment was fully removed by #8822 and is not part of this
design.

Release `1.1.1427` / Helm revisions 498--499 exposed the former ownership gap:
receipt renewal lived in each fill service controller and stopped across its
lifecycle boundaries. PRs #1656 and #1657 removed that coupling. Release
`1.1.1429` now has one PostgreSQL-singleton deployment daemon that remains
active during service-controller holds, updates, and restarts, with no
per-service renewal thread. Revision 501 proved that ownership correction dark
through more than two receipt lifetimes; revision 502 then released the hold.
The deployed `1.1.1430` correction was narrower: it made that one owner
continuously prove every context under deadline-bounded provider calls and a
timing contract with enough measured and formal publication headroom. It did
not lengthen the 30-second receipt lifetime, perform provider I/O in a launch
handler, or weaken any terminal guard.

SkyPilot PR #1651's bounded failed-service teardown and its delete-order/JIT
successors are deployed in release `1.1.1427`. They removed the former
teardown blockers through the normal evidence-backed paths; manual row deletion
remains unauthorized. The old version-64 `SHUTTING_DOWN` graph and its 71-row
foreign-key collision are incident history, not current service state. The
canonical service was subsequently recreated first as lifecycle 84 and then as
lifecycle 85 on PostgreSQL authority with no EFS correctness or runtime
dependency. Both lifecycles were removed normally; lifecycle 91 was later
created and also removed normally. Lifecycle 93/version 1 later supplied the
historical convergence proof and is now `FAILED_CLEANUP` after supported
provider teardown. Historical lifecycles do not contribute to a fresh
service's allocation or occupancy.

The following census is the historical lifecycle-84 baseline, not current
state. A read-only PostgreSQL census at 2026-08-22 13:00 UTC found 266 retained,
entirely non-Spot replicas: 150
`FAILED_PROVISION`, 46 `FAILED_CLEANUP`, 55 `READY`, and 15 `SHUTTING_DOWN`.
The exact protocol-v2 associations comprise 197 projected provider-`ABSENT`
pre-job rows, 14 ambiguous provider-`PRESENT` pre-job rows, and 55 projected
post-job rows whose provider evidence is `NOT_QUERIED`. Every row retains its
frozen profile, positive admission sequence, and exact successful capacity
observation; the 55 post-job rows also retain their independent materialization
sequence. The intent ledger has 355 `COMMITTED` and 1,988 `TERMINAL` rows,
while Kueue admissions and paid-capacity claims are both zero. The released
controller reduced the earlier 280-row census, after which release `1.1.1436`
closed the pre-Serve057 missing-admission boundary and normal fenced purge
removed the lifecycle. Lifecycle 85 then supplied the clean-fill evidence
above and was also normally purged. The next recreated lifecycle after the
protocol-v6 fix-forward deployment is the production-proof subject; these
retained counts are incident evidence, not accepted steady state.

Revision 501 dark verification proved the corrected deployment singleton was
unique and renewed east and PHX repeatedly for more than two receipt
lifetimes while the controller hold remained active. Revision 502 released
that hold. Consolidated HA recovered `boltz-l4-fleet` on the new controller
Pod, broker generations advanced, and all new replica rows remained non-Spot
with zero paid-capacity claims. The launch wave then exposed an undersized
provider-renewal timing contract: successful publications were normally
7--15 seconds apart, but five-second provider deadlines intermittently failed
under ordinary launch load. The sampled daemon produced 47 successful and 33
failed rounds, with ten inter-publication gaps above the usable 25-second
horizon and a maximum of 57.84 seconds. Of 84 reserved-fill launches sampled
after revision 502, 39 were rejected at the same stale exact-receipt guard;
some failed before admission/effect, while later-checkpoint failures entered
`POST_EFFECT_AMBIGUOUS` evidence-backed cleanup. A
fresh three-sample full-fleet attestation measured 3.076--4.336 seconds;
per-context samples measured 0.805--1.303 seconds for east and 2.442--3.129
seconds for PHX. The five-second budget therefore had less than one second of
normal headroom after jitter, database publication, and load variance.

That expiry was the blocker corrected and dark-qualified by the deployed
`1.1.1430` writer described above; it is not an open Serve057 prerequisite.
Fresh PHX readback also resolves the apparent free-H200 discrepancy. All seven
ClusterQueues are current and Active in the unchanged flat `shared-pool`.
Readback on 2026-08-22 showed all 64 H200 nodes Ready with 512 allocatable GPUs;
Kueue reserves 503 GPUs, including 37 admitted and Running `skypilot-be`
Workloads. Every LocalQueue reports zero pending Workloads, including
`research-ma`, and both SkyPilot ClusterQueues are Active/Ready. The remaining
nine GPUs were therefore **policy-admissible free capacity at that readback**.
The earlier revision-502 and revision-551 controller holds were released for
their respective lifecycle proofs. Lifecycle 85 proved Serve057 birth and
exact PHX admission, but its executor churn prevented full convergence and it
was normally removed. Lifecycle 91 later supplied the pre-v6 observation and
was also normally purged. The final synchronized capacity/Workload census now
belongs to clean lifecycle 93: 58 of 58 East scheduler-fit GPUs and 75 of 75
PHX Kueue-admitted GPUs were Ready, while three additional submitted PHX
Workloads remained explicitly queued. No stale nine-GPU or 55-GPU snapshot is
accepted as final proof, and no lack of occupancy is attributed to Kueue
without that exact admission-denominator comparison. The earlier
32-GPU `research-ma` head-of-line gang was a transient production observation
and is no longer present. A later higher-priority research request may again
make some raw free GPUs policy-withheld under the unchanged
`LowerPriority`/`StrictFIFO` contract. The live fleet Pod contract is
LocalQueue `be`, WorkloadPriorityClass `be-lt` (numeric 11), Pod priority
`-1000`, `preemptionPolicy: Never`, `default-scheduler`, and
`skypilot-pool-sa`; no new queue or priority tranche is involved.

That evidence does **not** authorize a shared Kueue change. The steady-state
occupancy contract is 100% of healthy, compatible, **policy-admissible** reserved
capacity; PHX had nine such free GPUs in the cited historical readback, and
lifecycle 93 subsequently proved every currently admitted unit covered. Raw
physical free and
`physically_free_but_policy_withheld` must be reported separately, and the
first associated gated Workload must prevent misleading additional assignable
capacity without implementing a second SkyPilot-side Kueue simulator. Making
raw occupancy work-conserving in this state requires research-owner approval to
change borrowing/preemption semantics and is outside this design. Automatic
reserved backfill and live current-request telemetry were production-proven on
lifecycle 93 before supported teardown. The `1.1.1448` homogeneous rollout,
cross-Pod takeover,
duplicate-up rejection, Kueue non-mutation, node-pressure readback, and the
complete 180-second stale/quiescence horizon are production-proven. The live
gate is now v7 finalization of lifecycle 93, fresh same-name recreation,
generation-35 activation, new scheduler-authorized convergence, a zero-paid
final census, and rollout-audit acceptance. A
deliberately billable nonzero-demand
exercise is not required for closeout: the paid residual and drain contracts
are source- and real-PostgreSQL-test-qualified, while idle and harmless-request
production checks created no paid capacity. Serve057, exact replay, typed proof
pause, pre-materialization proof, and the admissionless-retirement contract are
merged, deployed, and exercised.
Exact completed logical requests
require the separate PostgreSQL idempotency/completeness contract described
below.

## Policy-admission feedback and bounded fill

Status: this revised contract replaces the source-only seven-state
paid-handoff/reprobe proposal, which was never deployed. Its exact three-state,
bounded-batch, and exact-card surge contract is merged in PR #1659 and deployed
through release `1.1.1436` / Helm revision 523. Required cleanup PR #1660
remains draft, stacked, cross-linked, and blocked on the production gates.
Lifecycle 85 exercised birth with this contract and exact PHX admission before
normal purge; lifecycle 93 exercised it after clean recreation and closed the
historical primary backfill gate before supported teardown. The PR-#1678
takeover and complete
stale/quiescence horizon are production-proven. The scheduled +30
control-plane/HA/error checks also passed; the eight coincident fresh
A100-80GB assignments exposed the protocol-v6 apt/SSH marker hang. The
source-qualified protocol-v7/cohort-7 correction must now finish lifecycle-93
cleanup before a fresh recreation, generation-35 activation, readiness
settlement, and final audit. Paid behavior is source- and real-PostgreSQL-test-
qualified and does not require deliberately purchasing synthetic capacity.
The steady state
deliberately has one narrow
PostgreSQL admission relation, three states, the existing durable request retry
mechanism, and the ordinary READY-aware paid-retirement path. The non-authoritative HTTP
wakeup was deleted, and one frozen provider-owned runtime object now carries
the complete Kueue Pod identity, accelerator, observer, and optional persisted
Pod identity through provisioning instead of four parallel optional values.

### Scope and invariants

Kueue remains the sole policy-admission authority for PHX. SkyPilot observes
the exact Pod it owns; it neither simulates Kueue nor modifies shared queue
topology. East has no Kueue admission configuration and keeps the existing
concurrent reserved-fill path unchanged.

A Kueue unresolved domain is the checked tuple of service name, service
lifecycle epoch, pool physical UID, canonical accelerator card and per-Pod GPU
count. The immutable worker projection and its version are verified separately
on every transition. A mismatch, missing row, stale observation, replaced Pod,
or incomplete graph is UNKNOWN and fails closed. Raw physically free GPUs are
not policy-admissible capacity.

Version election is held while an outgoing version has `INTENT_PENDING` or
`POD_WAITING` authority. The update may proceed only after that exact probe is
evidence-cleaned, or after it becomes `POLICY_ADMITTED`. A live admitted
old-version row remains positively charged across a normal update and is
validated against its own immutable version and projection; merely advancing
`current_version` never makes it UNKNOWN. New-version successors use the new
immutable projection, but require the same fresh predecessor proof and
capacity checks as every other successor. Lifecycle replacement remains
stricter: all old-lifecycle provider graphs must be evidence-clean before a new
lifecycle probes the same domain.

The final implementation introduces no EFS correctness dependency, KubeRay,
Terraform/Terragrunt resource, Cohort, ClusterQueue, LocalQueue, quota,
WorkloadPriorityClass, scheduler, application-admin permission, or research
policy change. The only platform state this service requires is the already
effective release of Simone's `skypilot-be` queue from Hold. `skypilot-wa` is
unrelated to this service; its current unheld state is recorded but neither
depended on nor changed by this implementation.

### One durable admission relation

Serve057's three-state PostgreSQL table `serve_kueue_admissions` is deployed.
The superseded seven-state draft was replaced before PR #1659 merged and was
never deployed.
There is no legacy backfill: boltz-l4-fleet is test-only and is normally
fenced, torn down with provider evidence, and recreated. The additive central
schema still deploys through the normal PostgreSQL migration mechanism.

There is one row per Kueue reserved-fill intent, created before provider work.
Its primary identity is the intent idempotency key. It stores the exact service
and lifecycle, unresolved-domain digest and checked columns, pool key/epoch and
physical UID, accelerator/card count, service version, worker projection
digest, and timestamps. Materialization columns are initially null and later
bind the exact replica ID, replica record UUID, provider cluster generation,
reserved-fill launch association, and Pod namespace/name/UID plus immutable
receipt hashes and observations.

One admission row may also carry a service replacement-surge lease:
`replacement_surge_units > 0` and an immutable
`replacement_compatibility_sha256`. The units are expressed in the service's
immutable configured ceiling unit: `PHYSICAL` counts one per Pod, while
`LOGICAL` counts the Pod's planned slots/accelerator count. The lease always
represents exactly one physical reserved Pod even when its recorded overflow is
multiple logical units. The compatibility digest binds the exact normalized
accelerator card/count, immutable worker-projection evidence, service
incarnation/version, and configured ceiling unit. The current service contract
has no immutable directional proof that every request eligible for paid L4 is
also eligible for reserved H200 or A100; a configured accelerator catalog and
multiple runnable worker projections do not imply that dominance. Serve057
therefore permits the above-ceiling replacement lease only for the exact same
card/count. Cross-card reserved supply still fills ordinary headroom and may
replace paid capacity through ordinary demand-aware rebalance once headroom is
available, but it cannot use the surge exception. A future cross-card lease
would require an explicit server-owned immutable directional capability
contract in the service version; mutable demand telemetry and an empty-profile
default are never sufficient authority.

Only these states exist:

- `INTENT_PENDING`: durable intent exists; no complete exact Pod receipt is
  asserted.
- `POD_WAITING`: the exact live Pod still carries Kueue's scheduling gate and
  its PostgreSQL-clock receipt is fresh.
- `POLICY_ADMITTED`: the same exact Pod was observed without the gate. This
  fact is monotonic for that Pod UID and does not expire.

There are no lane generations, current/history cursor, paid handoff, paid
occupancy, reprobe, backoff, or paid-victim lifecycle columns. PostgreSQL
constraints make materialization and Pod-receipt shapes all-or-none and reject
invalid state transitions. Foreign keys to intent, replica, and association
authority are restrictive, never cascading. Multiple pre-admission rows may
coexist in one unresolved domain so one authenticated grant can durably submit
a bounded batch to Kueue. Intent idempotency prevents replay duplication; the
sequenced grant locks and debits every unresolved admission plus every live
non-Kueue pending intent before allocating the remaining freshly observed
capacity. A partial unique index on service name
where `replacement_surge_units > 0` permits exactly one service-level surge
lease. Admitted rows coexist while their replicas are live.

### Grant and materialization

Every grant transaction acquires locks in one order: global zero-cost
sequencer, protocol/service, sorted pool and claim rows, sorted intents,
replicas and associations, then sorted admission rows. No provider or
Kubernetes call occurs while a SQL row or advisory lock is held.

`grant_plan()` inserts each reserved-fill intent and its `INTENT_PENDING`
admission row atomically. This is the admission linearization point; inserting
only at later materialization would let concurrent grants race. A plan may
create a same-domain batch, but only up to the exact freshly observed grant,
service ceiling/surge bound, and authenticated allocation. Every committed
intent immediately becomes a physical-capacity debit, so the next sequenced
round subtracts the whole batch before computing residual capacity. Replay is
idempotent by intent key and cannot add another debit. An exact live
`POLICY_ADMITTED` predecessor may coexist with successors only when capacity
accounting permits them and its complete live graph is freshly proven. Before
SQL locking, the reconciliation provider phase samples a PostgreSQL-clock
token through one unlocked query, then reads exact Pod/provider identity and
produces short-lived positive receipts. The grant transaction revalidates the
original token under PostgreSQL time after every lock wait against the linked
COMMITTED intents, replicas, associations, admission rows, lifecycle,
immutable versions/projections, Pod UIDs, and provider generations. Missing,
stale, replaced, or partial evidence is UNKNOWN. Replica status and non-expiring
admitted history are never successor authority by themselves. Provider I/O
remains outside every SQL and advisory lock. `INTENT_PENDING`, `POD_WAITING`,
and bounded UNKNOWN rows consume their own planned-capacity debits but do not
serialize otherwise available slots in the same domain. Replay and new intents
in one plan obey the same locked aggregate bounds. The planner validates the
full graph and never relies on replica status or a process-local cache.

An expired, never-materialized `INTENT_PENDING` row whose intent is terminal
may be removed without a provider read only after one locked transaction proves
the exact admission, intent, replica, association, request, queue, and
retention-pin graph contains no materialization or provider-effect path. The
terminal intent remains as history. The next normal sequenced grant uses a
fresh pool observation and the remaining durable debits; cleanup does not
create a privileged successor or bypass capacity accounting. This independent
maintenance may commit during replay-only or capacity-blocked reconciliation;
its complete provider-free proof, not the presence of a successor, is deletion
authority. Any materialized or ambiguous row requires normal evidence-backed
provider cleanup.

Durable submission uses one bounded executor per physical pool, not one
provider-preflight round trip per intent and not an unbounded thread per Pod.
The repository retains a hard safety limit of 32 leases, but the manager takes
an actuation quantum of four oldest actionable intents per mutex turn. Every
intent keeps its own owner, generation, and expiry. The executor opens one V2
provider-phase admission and one deduplicated physical-UID capture for that
quantum, then commits each exact intent/replica/request graph independently
under the existing manager serialization. Each committed graph immediately
starts the ordinary bound-request adopter, but the adopter does not execute
the launch: actual provider work begins only when the generic request executor
leases that durable request. The held rollout therefore supplies the matching
generic long-worker capacity described below. A full four-item turn releases
its exact pool lane before re-signalling durable work, which prevents a
completed-but-still-live thread from consuming the wakeup and imposing the
one-second dispatcher poll. This bounds cross-pool head-of-line blocking
without changing the repository safety ceiling or introducing another
scheduler. A definite failure releases or terminalizes
only that intent; an ambiguous commit preserves only that exact graph and does
not cancel later members. The capture is released after the last member is
staged, without waiting for Pod scheduling or readiness. Different pools
retain independent executor threads and compatible V2 provider phases.

Service-version YAML is immutable and can be large. The manager therefore
parses it once per active/recovery version, retains only the current version
plus two recently used recovery templates, and deep-copies the template for
each executable launch. The display-only original YAML remains in the durable
service-version row but is omitted from the internal launch request. The
server workspace is injected during the first request freeze instead of
decoding and re-encoding the frozen body. These are preparation-only
optimizations: they change no task resource, worker projection, Kueue identity,
provider proof, capacity debit, or paid-launch authority. An evicted old
version remains recoverable by reparsing its PostgreSQL-authoritative YAML and
specification.

The executable protocol-v2 request contains exactly one controller-selected,
launchable Kubernetes resource. That serialized singleton and the durable
fence are the complete placement authority: the executor reconstructs
`best_resources` from them before considering cluster existence or optimizer
stages, on both the first attempt and every replay. It never re-optimizes a
reserved-fill request and never trusts process-local optimizer residue. This
keeps retries byte-equivalent in placement semantics, prevents a partial
cluster record from changing execution behavior, and avoids multiplying
Kubernetes discovery load by the replica count.

This invariant uses the already-bound provider policy mode
`absent_controller_and_executor`. Protocol-v2 controller preparation rejects a
configured `admin_policy` before freezing request bytes, and executor replay
rejects one before policy application. A policy-mutated resource cannot become
unjournaled placement authority. Supporting an admin policy in this path would
require a separately reviewed durable post-policy projection; it must not be
added as a retry-time compatibility branch. The early INIT cluster record is a
write-ahead execution snapshot of the same singleton, persisted before Pod
creation and reused under the cluster lock; it is not a second placement
authority and needs no additional schema.

The existing intent materialization transaction CAS-binds the admission row to
the exact replica ID, record UUID, provider cluster generation, and launch
association. The Pod carries server-owned intent key, record UUID, pool
physical UID, and projection digest annotations; caller collisions fail.
Dynamic identity annotations are excluded from the static projection digest,
whose expected scheduling specification remains hash-bound.

### One Pod and a non-resident observer

Every Kueue-bound reserved-fill replica is exactly one node and one Pod.
Grant, materialization, execution, and `bulk_provision()` independently require
`placement_catalog.num_nodes == 1` and runtime `ProvisionConfig.count == 1`
before provider work. Accelerator count continues to mean GPUs per Pod and is
never used as Pod count.

The Kubernetes layer exposes a pure typed CoreV1 Pod classifier. It does not
import Serve or PostgreSQL. The Serve launch/adoption boundary validates the
exact namespace/name/UID, annotations, queue, workload priority, Pod priority,
scheduler, ServiceAccount, card/count, pool UID, and projection digest, then
performs the PostgreSQL CAS.

There is no resident lane poller, launch thread waiting for quota, or 24-hour
Kueue timeout. After creating or exactly adopting the Pod and committing a
fresh `POD_WAITING` receipt, the handler raises a typed
`ExecutionPausedError` with a short approximately five-second durable retry.
The existing request executor atomically changes the durable request to its
delayed retry state and releases its worker claim, while leaving the COMMITTED
intent and unsettled launch association nonterminal. It does not run teardown.
The retry re-enters the same launch, and exact UID adoption reattests the same
object. Once the gate is absent, it commits `POLICY_ADMITTED` and resumes an
idempotent, restart-recoverable bootstrap. Provider Pod create remains
single-object/adopted; a crash may replay bootstrap, so bootstrap must tolerate
re-entry rather than claim exactly-once execution. A name-only or replaced-UID
object has no authority.

Each exact observation first samples PostgreSQL `clock_timestamp()` without
holding a row or advisory lock, then performs the exact Kubernetes read. The
later admission-row CAS persists that original sample as `observed_at`, sets
`valid_until = observed_at + 15 seconds`, and rejects the receipt when the
database clock has already reached that deadline. A provider read or lock wait
therefore consumes freshness; commit time can never mint age back. Each retry
renews only through a new token, successful exact Pod read, and admission-row
CAS.
A missing object, read/identity failure, database failure, or dead handler
cannot renew. An expired waiting receipt is UNKNOWN for accounting, but a
later retry that proves the exact same Pod UID and immutable graph may renew it;
expiry is not terminal. Cancellation and update never discard the Pod or row
directly and use only evidence-backed cleanup. Admitted replays may refresh
Pending-to-Running and optional valid workload/topology audit output without
changing `admitted_at`.

Observer materialization and admission-snapshot publication share one explicit
SQL hierarchy with every service writer. Their common prefix is protocol
`FOR SHARE`, lifecycle `FOR SHARE`, then service `FOR SHARE`; writers take the
same prefix exclusively when mutation requires it. The exact observer suffix
then locks intent, replica, association, and admission rows exclusively. A
snapshot locks all selected intents `FOR UPDATE` before reading admissions
`FOR SHARE`. No path upgrades a shared prefix row to exclusive inside the same
transaction. These read prefixes protect current generation/lifecycle/service
identity without turning observation into a writer and remove the former
observer-versus-autoscaler cycle. Provider/Kubernetes I/O remains outside these
locks, and a lock wait consumes rather than refreshes observation validity.

### Capacity, paid residual, and bounded replacement surge

The admission projection has three accounting outcomes:

- Fresh exact `POD_WAITING` owns a physical zero-cost Pod but contributes zero
  demand-serving supply and zero assigned-GPU debit for the paid/serving
  ceiling. It remains a conserved physical-capacity debit; a bounded batch may
  wait in the same domain while Kueue chooses which Pods are policy-admissible.
- Exact `POLICY_ADMITTED` is future reserved supply. It immediately contributes
  assigned-GPU debit and suppresses matching new paid capacity. It is not
  serving supply until the ordinary replica becomes `READY`.
- `INTENT_PENDING`, expired waiting, missing/malformed lineage, and any identity
  mismatch are UNKNOWN. Each bounded row conservatively contributes its own
  assigned and conserved-capacity debit, preventing replacement of that
  capacity and suppressing exact-shape paid authority without serializing other
  freshly observed capacity in the same domain.

The planner, claim transaction, and final provider-effect gate all reconstruct
this projection under PostgreSQL time and locks. A plan-to-claim transition,
expiry, or Pod-identity race rejects the paid effect. No timer or in-memory
cache is paid authority.

UNKNOWN is scoped by immutable identity evidence. In Serve057, a row with a
complete checked domain debits its exact planned units and suppresses only the
exact normalized accelerator card/count accounting class; other freshly
observed capacity in that domain and unrelated pools/cards/services continue.
Planner, claim, and final provider-effect
validation use that identical exact-shape scope and explicitly prohibit
widening from the configured catalog, mutable demand profiles, or an empty
profile default. If the identity is so malformed or incomplete that its exact
shape cannot be bounded, all paid residual for that service fails closed,
because incompatibility cannot be proven, while independent services still
progress.

The paid/serving `max_replicas` ceiling remains unchanged. To avoid deadlock
when compatible paid capacity occupies its final slots, the broker has one
explicit zero-cost replacement exception. In the sequenced grant transaction,
it locks and counts **all cleanup-unproven capacity plus every live
unmaterialized intent** across every version and replica status, in the
service's immutable configured ceiling unit. It then computes
`overflow = max(0, conserved_before + candidate_capacity - max)`. This hard
debit is deliberately independent of paid-residual assigned supply: fresh
waiting Pods are non-serving but are still conserved capacity, so concurrent
ordinary-headroom waits in different domains cannot chain beyond the ceiling.
The same transaction proves non-retiring paid capacity with the exact same
normalized card/count, expressed in the same immutable unit, covers every
overflow unit. If overflow is
positive, the new `INTENT_PENDING` row acquires the one service-level surge
lease with that unit count and compatibility digest. This may add exactly one
physical reserved Pod above the normal ceiling, never a paid Pod. Normal
headroom may admit multiple domains only while the locked conserved total
remains at or below the ceiling; only one admission row per service may hold
positive surge units. Grant rejects atomically when either bound cannot be
proven; materialization revalidation is defense in depth, not the first hard
limit.

The lease starts at grant and persists through `INTENT_PENDING`,
`POD_WAITING`, `POLICY_ADMITTED`, initialization, READY, and any paid
SHUTTING_DOWN state. No replica status transition releases it, and the
service-wide partial unique index plus locked aggregate conservation prevents
another row or domain from chaining a surge.
Materialization revalidates the exact lease, compatibility witness, frozen
ceiling unit, and `cleanup-unproven units + live unmaterialized intent units <=
max + surge_units`.

Fresh `POD_WAITING` is non-serving/non-assigned for paid residual calculation
but remains part of cleanup-unproven conservation in the configured ceiling
unit.
`POLICY_ADMITTED` counts its full planned capacity immediately. Once the
reserved replica is READY, ordinary READY-aware cost-descending retirement
selects the exact-card/count paid replica that covered the overflow. Under
`PHYSICAL`, both candidate and victim cost one; under `LOGICAL`, both have the
same accelerator count, so one exact-shape victim always covers
`overflow <= candidate_capacity`. Cross-card retirement remains on the ordinary
demand-aware rebalance path and is not lease authority. The surge lease is
cleared only in a
transaction that proves an exact provider-clean capacity reduction has reduced
all cleanup-unproven capacity plus live unmaterialized intent capacity to the
frozen ceiling or below. Paid drain is the preferred reduction, but an
independently proved reserved cleanup may also restore the invariant; tying
release only to a paid victim would strand a dead lease. A crash during the
victim drain retains the lease and resumes safely. Cleanup of the
reserved probe itself may remove the lease only with that probe's exact
provider-absence proof. Thus total physical overshoot is at most one Pod while
configured-unit overflow is explicit and bounded.
Busy paid traffic is not interrupted merely to probe, and no special paid
state machine is introduced.

A continuously retrying waiting Pod is the reprobe. There is no destructive
paid-to-reserved handoff token: fresh waiting proves that Kueue has not assigned
a GPU, admitted suppresses new paid launch, and READY authorizes ordinary paid
drain. This preserves traffic coverage and removes the dead seven-state
implementation.

### Cleanup and garbage collection

Replica status or `sky_down_status=SUCCEEDED` is never provider-absence proof.
A materialized admission accepts exactly the existing normalized interrupted /
ambiguous launch absence authority, or a fresh exact physical-UID-scoped
observation proving the immutable `(namespace, name, UID)` is ABSENT after
ordinary successful teardown. A same-name different-UID Pod is REPLACED;
provider, context, identity, or read failure is UNKNOWN.

Either authority must be combined with the ordinary launch association's
copied terminal and execution-quiescence envelope, the absence of a live queue
or retention pin, exact projection, and provider-generation predicates. Once
projection commits, that association is the durable lifetime receipt; the API
request row may be garbage-collected before the replica is retired. If the
request row is still present, every terminal, quiescence, and capability field
must exactly match the association. A missing request is accepted only after
its queue and pins are also absent. The accepted post-job terminal outcomes are
`SUCCEEDED`, `FAILED`, and `CANCELLED`; each requires a positive service job ID
and matching copied terminal/quiescence evidence. `ReplicaInfo` does not copy
that job ID: its `launch_request_id` and `service_job_id` fields remain owned by
the separate system-OOM recovery subdocument, while the generalized launch
association is the sole ordinary-launch receipt.

The transaction first settles the association and clears the replica's
association pointer. It then deletes exactly one admission row, deletes exactly
one replica row, and deletes the COMMITTED reserved-fill intent when that
cleanup path owns its removal. Exact row counts and every predicate are
rechecked in the same lock order. A crash commits all of these operations or
none.

Whole-service teardown uses the same evidence and delete order. Generic
replica deletion is allowed only when immutable projection positively proves
the non-Kueue/East path. A Kueue replica with missing admission state remains
UNKNOWN and fails closed for live admission, materialization, ordinary
scale-down, paid-capacity accounting, and provider-present cleanup.

Kubernetes accepting `core.down` is not yet provider absence: even a zero-grace
delete can leave the Pod visible while API deletion and Kueue finalization
propagate. For a protocol-v2 cleanup fence only, successful down therefore
enters one bounded post-delete observation loop before either the bound
association projector or exact Kueue-Pod projector runs. Every iteration
rechecks the lifecycle owner, acquires a fresh `V2_FENCED` provider phase,
performs an uncached physical-UID-fenced Pod inventory, releases the provider
phase, and only then sleeps. Ownership is rechecked after the provider read so
a stale owner cannot consume a concurrent `ABSENT` result. Only `ABSENT`
continues to durable projection. `PRESENT`, `UNPROVEN` (including provider
failure or a retargeted/replaced physical cluster), provider-phase timeout, or
ownership loss retains the cleanup graph and capacity debit. The deadline does
not reissue the already successful down, and replay of an already committed
absence remains idempotent through the existing durable projector. Legacy and
non-Kubernetes cleanup are unchanged.

The lifecycle-89 production teardown on 2026-08-22 exposed this propagation
window: all provider Pods and Kueue Workloads disappeared, but ten East
replicas that completed `INIT -> UP` during teardown and the three admitted PHX
replicas reached the one-shot projector before their deletes were observable.
They were correctly retained but forced a later supported purge. The bounded
read-after-delete wait removes that false `FAILED_CLEANUP` classification
without weakening absence evidence or changing Kueue policy.

There is one permanent provider-free exception at the irreversible whole-
service teardown boundary. Only while the exact current lifecycle is
`SHUTTING_DOWN` or retrying `FAILED_CLEANUP`, a missing admission may be retired
after all of the following are re-proved: the service hash/lifecycle/owner;
the immutable COMMITTED intent, replica-record UUID, provider generation, and
single protocol-v2 association; a terminal launch generation and copied
execution-quiescence receipt; no request queue row, retention pin, or paid
claim; and a fresh uncached physical-UID-fenced provider read that began after
quiescence and found the exact cluster name ABSENT. The provider read occurs
outside database locks. Its PostgreSQL-clock start token and canonical physical
absence envelope are then persisted on the association, and the complete graph
is locked and revalidated before the replica and intent are deleted atomically.
`PRESENT`, `UNPROVEN`, expired evidence, a concurrent materialization or
successor, or any identity mismatch retains the graph. No admission row is
synthesized or backfilled.

The missing-admission proof authenticates the complete immutable
`RESERVED_FILL/v1` association profile; the association cannot authenticate
itself with an arbitrary digest. The committed intent stores the observation's
all-zero-cost admission high-water, while atomic admission assigns a later
replica sequence, so a canonical typed-fill row requires
`replica.zero_cost_admission_sequence > intent.observation_sequence`. Admission
and first-successful materialization are independent PostgreSQL event streams:
each positive marker is validated only against its own durable singleton
high-water and the two markers are never ordered against each other. The
association profile was frozen before provider I/O with
`materialization_sequence = null`. Cleanup therefore first validates any
current non-null materialization receipt against the materialization
high-water, then reconstructs the immutable profile with only that field reset
to null before recomputing its digest. This preserves a valid row such as the
second of two admissions when only that second launch materializes, whose
persisted event tuple is `(admission=2, materialization=1)`.

The execution-quiescence receipt is also the provider-effect linearization
fence: it proves every execution generation authorized for that association is
terminal and no launch handler survives the sampled absence. Entering whole-
service teardown has already irrevocably revoked new grants and materialization
for the monotonic, never-reused `service_lifecycle_fences.epoch`;
`FAILED_CLEANUP` is a retry state inside that same revocation fence and cannot
return to live admission. Every admission or provider writer revalidates the
unchanged lifecycle and non-teardown service state immediately before provider
effect and is bound to that epoch, service hash, intent, association launch
generation, and replica-record UUID. No later provider operation may reuse
those values.
The final transaction locks the service and intent parents before checking the
replica, association, request/queue/pin, and admission children. Admission and
successor writers use the same parent lock order, and restrictive foreign keys
prevent a phantom child from committing while its locked parent is retired.
Immediately before consuming the admissionless proof, the transaction queries
and locks that exact admission key again and requires the result to contain
zero rows.

The canonical physical-absence envelope binds the association UUID, replica-
record UUID, exact cluster name, Kubernetes context, physical-cluster UID,
reserved-fill profile digest, and `ABSENT` result; the locked surrounding graph
additionally binds the provider launch generation. The provider observer holds
the physical-UID fence and accepts only the Kubernetes provider's authoritative,
complete read-after-delete inventory contract; a provider without equivalent
consistency cannot use this path. An auth failure, timeout, partial or non-
authoritative Pod inventory, missing ownership annotation, or ambiguous/
replaced identity is `UNPROVEN`, never absence. The PostgreSQL timestamp records
when the uncached read began; it must be no earlier than quiescence, and the
provider read, locked graph revalidation, and evidence-publication statement
must finish before that start token's bounded deadline. The publication update
tests the deadline in PostgreSQL and rechecks the database clock after the
write. A commit may finish later only while that same transaction continues to
hold the complete lifecycle/service/intent/replica/association graph locks; no
provider or admission writer can cross that interval, so commit latency cannot
invalidate the observation or create new launch authority.
Whole-service teardown claims a fresh restricted reducer incarnation before
cleanup. Settled `PROJECTED` associations intentionally do not move during the
generic takeover, so either ABSENT publication atomically adopts the locked
current service owner tuple and advances the association owner revision in the
same evidence statement. The Serve047 trigger therefore sees one exact current
owner; there is no ownerless evidence write or separate rebind window.
Once stamped under the unchanged terminal lifecycle fence, ABSENT is monotonic
and does not expire: the temporary freshness bound applies before publication,
not to the resulting terminal receipt. If a request row still exists it must be
the unique terminal, finished, execution-quiesced row for the same association,
generation, and capability tuple; duplicates, a surviving lease, or any field
mismatch retain the graph.
Both exact-Pod and admissionless loaders return the same closed decision set:
`NOT_APPLICABLE`, `NEEDS_PROBE`, or `ALREADY_PROVEN`. A restart or lost
acknowledgement that reads canonical durable ABSENT returns `ALREADY_PROVEN`
without another provider call; malformed or conflicting retained evidence
still fails closed. This keeps a temporary provider outage after the committed
receipt from reopening terminal uncertainty.

Normal down of an existing incarnation retains that incarnation's lifecycle
epoch while holding the name-scoped PostgreSQL advisory lock and atomically
publishing `SHUTTING_DOWN`; it does not rewrite immutable admission-time
association provenance. Fresh service birth or same-name rebirth advances the
durable lifecycle fence. Controller takeover is orthogonal: it rotates the
controller incarnation and owner epoch and transfers unresolved association
ownership without changing their lifecycle epoch.

The lifecycle increment belongs inside the successful birth transaction. A
same-name `serve up` against an existing nonterminal service must reject before
advancing the fence; rejection cannot rotate the active epoch, revoke its
controller, or make the existing service look like a predecessor. A lost
acknowledgement after a genuinely committed birth is reconciled from the
service row and lifecycle fence rather than by allocating another epoch.

Deletion retains the association tombstone for its ordinary 60-day audit
period and retains the monotonically advanced service lifecycle fence after the
service row is gone. Those durable tombstones, non-reused UUIDs/generations,
and the same-name lifecycle increment prevent import, recovery, or an ABA
successor from recreating the retired authority.

This is not a gate-9, lifecycle-84, or service-name exception. Once exact
provider absence and executor quiescence are durable, a missing admission no
longer owns capacity or mutation authority; the same recovery invariant safely
handles a future corrupted missing-lineage row without baking incident history
into teardown. Every path before that terminal boundary remains strict.
Structural restrictive foreign keys plus a schema-presence-aware
association-GC selection predicate prevent deletion while an admission row
references the graph. GC excludes those association IDs before forming its
bounded delete batch, so one protected Kueue association cannot roll back
collection of unrelated eligible tombstones; the predicate remains valid when
API and Serve migrations install in either order.
Association tombstones retain their ordinary 60-day audit lifetime after
cleanup; admission state is not a second historical archive.

### Observability, rollout, and proof

The service and infrastructure dashboards distinguish raw physical free,
policy-admissible free, assigned, fresh waiting, admitted, READY, paid,
provisioning, shutting down, policy-withheld, and UNKNOWN units with
freshness. A fresh waiting receipt proves only that its exact Pod is
policy-gated; it does not invent a numeric count for all raw free GPUs.

PostgreSQL-backed processing, queued, and in-flight request telemetry plus
freshness is already source-complete. The existing history path also reports
aggregate requests in the last hour and terminal prediction observations; it
is not an exact completed-logical-request ledger. Exact completed request
idempotency/completeness remains the separate contract called out in this
document and is not required to answer how many requests are processing now.
PR #1671 preserved the deployed telemetry path and added no second counter
path. Lifecycle 93 closed the healthy-controller production freshness and
request-count proof. The projection remained fresh and complete through the
PR-#1678 cross-Pod takeover and complete 180-second stale/quiescence interval,
with two reporters and stable load-balancer HA. Artificial provider-stall
injection remains source/test-qualified and is not a production closeout gate.

PR #1671 added no schema, migration, Kueue, Terraform/Terragrunt, storage, or
platform-runtime-pin change. Its exact API/controller/executor image was live
as one direct-Helm fix-forward cohort, and lifecycle 91 was recreated from the
canonical `min_replicas: 0`, zero-floor, no-EFS projection before normal purge
and lifecycle-93 recreation. PR #1678 uses the same held fix-forward protocol.
Repair a failed qualification or deployment with another compatible Helm
fix-forward; do not downgrade.

Required real-PostgreSQL and focused unit/integration tests cover concurrent
same-domain and same-plan grants, including a full authenticated same-domain
batch with no duplicate next-round debit; a freshly re-proven admitted successor;
outgoing waiting update rejection; old-version admitted charging and
new-version successors; projection and lifecycle mismatch; replay-plus-new
plans; expired never-materialized provider-free cleanup followed by an ordinary
freshly observed grant; one-node enforcement at
all four boundaries; observer pause atomically releasing the executor claim
without terminating intent/association; restart/adoption and expired-receipt
renewal for the same UID; TTL expiry; replaced/missing UID and database
failure; idempotent admitted bootstrap replay; plan/claim/provider-effect
races; compatibility-scoped UNKNOWN and malformed unbounded identity; exact
card/count surge binding and rejection of configured-catalog, mutable-profile,
empty-profile, or cross-card dominance guesses;
fresh-waiting supply exclusion; admitted future supply; one-physical-Pod
service surge in both `PHYSICAL` and `LOGICAL` ceiling units; no paid use of
surge; a logical-unit `current=18,max=20,candidate=H200:8` case in which one
non-retiring exact H200:8 paid row plus ten other units covers the six-unit
overflow; insufficient coverage and no-exact-shape rejection; a concurrent
cross-domain chain where fresh waits already consume the remaining conserved
headroom; exact-victim READY drain and crash/restart; busy paid traffic
preservation; both exact
absence authorities; association settlement/delete order; restrictive-FK and
60-day GC behavior; update and service teardown; unchanged East batch fill;
and dashboard/request-telemetry freshness. The provider-free missing-admission
contract additionally proves that whole-service teardown accepts only a fresh,
uncached, physical-UID-fenced ABSENT observation begun after exact execution
quiescence. Negative tests retain the complete graph for a live service,
ordinary scale-down, `PRESENT`/`UNPROVEN`, expired or pre-quiescence evidence,
queue/pin/paid authority, wrong service hash/lifecycle/replica-record/provider
generation, a malformed frozen profile, and a concurrent admission or
successor. A production-shaped atomic regression commits two typed fills,
materializes only the second, and proves that `(admission=2,
materialization=1)` still authenticates the unchanged admission-time cleanup
profile.

Production proof runs immediately, at +10 and +30 minutes, and through a full
stale/quiescence interval. It must show every healthy compatible
policy-admissible free unit assigned, gated units represented without launch
thread accumulation, no paid launch when admitted/READY reserved capacity
covers demand, paid residual only for genuinely uncovered authenticated
demand, paid drain after reserved READY, no phantom rows, bounded controller
latency, restart recovery, and fresh request counts.

## Qualification history

The current production truth at this design revision is the supported-down
incident boundary: lifecycle 93 is `FAILED_CLEANUP` with nine retained
`UNKNOWN` rows, while direct East and PHX provider inventories both report
zero workloads. The v6 control plane cannot finalize those rows. Helm revision
572 remains deployed at reconciliation generation 34, but the service is not
active and no occupancy result below should be read as current provider state.

Immediately before that supported teardown, production used release `1.1.1448`, exact merge commit
`38ec2434245c2286d36c81d371834f97bde4f43c`, and image digest
`sha256:ba7602e63363baaa35210b8aafd429b7050c1a3fa5861a0bc9c58939c9fecb3c`
on all two API, two controller, and three executor Pods. Storage is disabled.
Reconciliation is `SEQUENCED_ACTIVE` at generation 34 with seven exact writers.
Lifecycle 93/version 1 retains service hash
`3f767fce-a4ec-4869-bbcb-3f7f77456f89` and the same endpoint after durable
ownership transferred from `10.30.1.190`/epoch 3 to
`10.30.0.98`/epoch 4 and a new controller incarnation. A duplicate same-name
`serve up` rejected without changing lifecycle, version, hash, owner epoch, or
endpoint.

The historical approximately 03:10 UTC census was 133 Ready of 136 reserved, zero-cost,
non-Spot units: East 13 A100 plus 45 A100-80GB Ready, and PHX 75 of 78 H200
Ready plus three provisioning/queued. Claim and service generation 7778 were
current at allocation generation 17. Grants and edge caps were 13, 45, and 78;
raw observed free was 0, 0, and 3, but spendable free was zero on every card
because the three PHX units were already durable queued-intent debits.
`fill_target` and `fill_free_slots` were zero. Paid-attributed rows, paid
claims, paid waiters, association ambiguities, and provider Spot clusters were
zero.

At the historical 03:13:53 UTC +30 boundary, the control-plane, HA, exact-writer, error,
Kueue, telemetry, lifecycle, service-hash, owner, gate-34, and claim-7778
checks all remained healthy. All seven writers and both load-balancer slots
used the exact digest with zero restarts. That read coincided with a real eight-card
A100-80GB capacity release. East inventory reconciled exactly: research held
262 Running GPU requests, while SkyPilot immediately created eight new fleet
replicas for a total of 66 assigned fleet GPU Pods, covering all 328 physical
GPUs. Sixty fleet Pods were Running and six new Pods were Pending bootstrap.
Raw observed free therefore moved from zero to eight, but spendable stayed zero
because all eight slots were already durable debits. That sample was not
unassigned capacity. Its subsequent v6 bootstrap failure and supported teardown
supersede the readiness-settlement wording that applied at that instant.

The request projection remained fresh and complete through takeover with two
reporters. At the latest idle census it reported recent requests 0 over 60
seconds, requests/second 0, in-flight 0, confirmed in-flight 0, unknown
reporters 0, queue depth 0, and recent rejects 0, at age 3.23 seconds. Observed
Ready capacity was 133 at age 7.85 seconds. Load-balancer HA remained true on
active slot `b`, cutover generation 2, state `STABLE`. An authenticated harmless
`GET /v1/models/model` returned HTTP 200 without creating paid capacity. The
service dashboard returned HTTP 200 and rendered processing, confirmed,
queued, and freshness fields from that PostgreSQL-backed projection. This
closes the honest current-request-count gate; it does not claim an exact
completed-logical-request ledger.

Both normalized Kueue POST snapshots equal their PRE values. At 03:06:23 UTC
all 66 PHX nodes were Ready with no DiskPressure, MemoryPressure, or PIDPressure.
The complete 180-second stale/quiescence interval passed with zero named error
signatures across every current API, controller, executor, and load-balancer
log since 02:43 UTC. The scheduled +30 readback at or after 03:13:10 UTC is the
only time-based production checkpoint; its control-plane/HA/error portion
passed at 03:13:53 UTC. Overall acceptance remains pending while the coincident
eight-card refill settles and the rollout audit verifies the final paid census.

Helm revision 489 / release `1.1.1423` deployed merged PR #1651 at
`e883af27b986aee2bb5ae715dec99f308298356a` on the complete control-plane
writer cohort. It includes PR #1650's release-`1.1.1422` flat-PHX attester and
independent receipt renewal. The subsequent normal `boltz-l4-fleet` purge
proved all matching provider Pods/Workloads, external-load-balancer objects,
and central cluster rows absent, then retained exactly 71 database replica
rows at the deterministic intent foreign-key delete-order boundary described
above. This qualifies the deployed bounded teardown through physical absence;
it does not qualify final database deletion, recreation, no-EFS operation, or
reserved-capacity convergence.

Helm revision 475 / release `1.1.1404` ran image digest
`sha256:8047b32d5703eb280b621c5f7f404daadb2bb98738327e32d0f44355cbfe603e`
on all two API, two controller, and two executor Pods. It included the
application-only PR #1632 correction that recognizes additive Serve revision
056 as a valid descendant of placement-normalization authority 040. Controller
generation 110 became ready at 08:28 UTC and the `boltz-l4-fleet` service child
then adopted successfully, clearing revision 474's deterministic controller
boot failure and controller-dependent 503s. Release `1.1.1404` did **not**
contain corrective PR #1630's complete committed-handoff contract, so it was a
service-availability precursor rather than the final fix-forward image.

Revision 473 / release `1.1.1401` included merged PR #1626 at
`218aaaefb63d655f5513430f974198ff0c8aa93f`; its durable global-capacity
accounting is under horizon, while the independent postcommit provider handoff
still fails under successor-state churn. At 05:57 UTC, revision 472 represented
H200 30/30, A100-80GB 12/12, and A100 9/9 as 51 READY non-Spot replicas with
zero paid claims. At 06:03 UTC it immediately admitted eight newly free
A100-80GB slots, but seven of those eight provider operations lost the mutable
postcommit fence. Revision 473 subsequently proved cleanup-unproven accounting
is bounded, while the cleanup-and-retry sequence extended through replica
56146 and returned five A100-80GB slots to the free inventory. Direct provider
inventory reports zero instances in the service's GCP project, so the former
116 Spot L4 VMs are no longer a live cost gate.

Revision 472 included PR #1625's single pre-demand admission ordering at
`2704c25c148373e9521b3f7671a64094275ea2da`; revision 471 included PR #1624's
retirement-shelter fix at
`6a3605d7b11ab27eb3cb9006213055616ed781f8`. The preceding revision 470 /
release `1.1.1397` incident from PR #1623 at
`39fe1268d7b5bc9c6fa6a3ac6d8c44718d612075`
caused service version 64 to enter `FAILED` after a controller child restart
generated 45 demand-zero logical retirements. Forty-four physical worker Pods
survived; one additional Pod was independently evicted for node disk pressure.
All 45 retirement rows remained before the irreversible provider-down commit
seam and were restart-recoverable. Kueue and both physical pools were healthy.
This was a SkyServe controller correctness incident, not a Kueue condition.

The root cause is a volatile authority gap. The restarted controller had a
fresh PostgreSQL `AuthenticatedAllocationMap`, but its process-local legacy
fill-shelter cache was empty until the pool poller completed. The autoscaler
therefore treated fresh zero traffic as a complete target of zero and admitted
retirement before consuming the durable reserved-fill allocation. A rollback
does not undo the already persisted logical retirements and can reproduce the
same cold-start race.

Revision 471 exposed a second controller-ordering defect. At about 04:06 UTC,
one transiently aligned route/demand report allowed 11 fill intents to be
accepted: nine H200, one A100, and one A100-80GB. A later current H200
allocation reported grant 30, free 4, feed 2,
and 28 represented slots (26 ready plus two pending), but the expected
two-intent admission did not converge; the one visible replacement row did not
close that deficit. The authenticated allocation and free physical capacity
were present. The controller instead read load-balancer demand before fill
admission and returned when route/report churn made that independent snapshot
unavailable. This proved that blocker was controller ordering, not GPU
availability or Kueue admission. PR #1625 is merged on `improvements` at
`2704c25c148373e9521b3f7671a64094275ea2da`: the single canonical admission
site now spends an independently authenticated zero-cost allocation before
reading load-balancer demand. It does not add a degraded-demand path.

Adversarial review of that ordering fix found a narrower accounting race. An
ACTUATING intent can become COMMITTED and insert its replica atomically between
two controller reads. The controller must therefore read pending intent state
first and a fresh replica ledger second: it may conservatively count both
sides, but can never miss both. The earlier implementation treated normalized
`sky_down_status=SUCCEEDED` as provider cleanup. Serve057 tightens that rule:
status alone is never absence evidence, and capacity remains conserved until
the exact provider-clean graph transaction removes the replica/admission (or a
never-materialized terminal admission passes its provider-free graph proof).
Every live pending intent across service incarnations and allocation
generations is also normalized into the current physical or logical unit.

This fix-forward contract is merged, deployed, and source/real-PostgreSQL-
qualified; lifecycle 93 supplies its live zero-paid and refill evidence. Every
sequenced decision tick
must derive one frozen retirement shelter from the current authenticated
PostgreSQL allocation and materialized/restart-recoverable holdings *before*
planning. Ordinary demand remains the action target; the exact-card retirement
floor is a distinct value used only to admit teardown and protects only
matching materialized or restart-recoverable reserved-fill holdings, capped by
the current grant. Unmaterialized FEED remains launch authority only; treating
it as a retirement floor would deadlock a paid replica at `max_replicas` and
prevent its free replacement. Missing, stale,
malformed, or composition-incomplete allocation authority blocks fill launch,
paid launch, and destructive retirement while preserving materialized fill.
An explicit current authenticated grant of zero is different and permits
retirement. Final logical retirement must lock the protocol singleton first and
revalidate the complete allocation identity, rounds, claims, projections,
ordinary-admission high-water, reclaim identity, and database-clock freshness
inside the same PostgreSQL transaction before making a replica off-route.

Platform PR #8652 is merged and does not pin the SkyPilot runtime. EFS/RWX and
KubeRay are not correctness dependencies, and no Terraform/Terragrunt expansion
is part of this recovery.

The 06:02--06:22 UTC production horizon exposed the separate postcommit
provider-handoff defect directly. In the original eight-row A100-80GB wave,
replicas 56096--56103, only 56098 crossed the provider boundary; the other
seven failed the mutable provider fence after durable admission. A subsequent
replacement chain eventually produced READY replicas 56113--56121 and
56126--56127, while 56122--56125 again failed the provider fence and 56128
became post-effect ambiguous before evidence-backed cleanup. PR #1626 merged
at `218aaaefb63d655f5513430f974198ff0c8aa93f` and was deployed as Helm
revision 473 during this horizon. At 06:24:51 UTC the service had 62 READY and
three STARTING replicas (56129--56131), zero Spot/paid capacity, and an
A100-80GB protocol-v2 round reporting free 2, grant/holdings 26, and feed 0.
Revision 473's global pending/cleanup-unproven accounting is deployed and under
its production horizon; these observations are not yet full proof of that
change. The seven-of-eight rejection and later mixed success/churn are the
qualification evidence for Serve056: mutable successor allocation, claim,
gate, observation, or deployment-policy state must stop revoking an already
committed zero-cost launch.

The 06:29:46 UTC observation cleanly separated those two contracts. The
A100-80GB round reported 24 holdings plus cleanup-unproven replica 56136 and
feed 2, for grant 27 with free 4; fresh rows 56137 and 56138 were exactly that
feed. Revision 473 therefore counted the ambiguous row instead of creating an
extra replacement. The preceding 56132--56135 batch had nevertheless failed
the mutable provider fence and 56136 was ambiguous. Overall capacity was 63
READY, zero Spot/paid, and direct node inventory showed zero schedulable free
slots across East A100/A100-80GB and PHX H200 because pending/admitted Pods had
consumed the remaining physical slots. The accounting fix is behaving as
designed, while committed provider handoff remains the independent blocker.
By 06:35 UTC the replacement sequence had extended through 56146 without a new
launch surviving; provider-fence failures dominated, with 56136's intermediate
post-effect ambiguity cleaned by evidence. After cleanup proved those effects
absent, direct A100-80GB free capacity rose back to 5 and the broker correctly
retried. This is a liveness loop, not an accounting overcommit: Serve056 must
let each exact committed handoff cross the provider boundary.

Historical 2026-08-20 qualification context follows. Helm revision 465 ran
release `1.1.1390` on the exact six-writer image digest
`sha256:8560dafc8be27460fec4d6b4905cdea7579ce0b82ff006f68f6a97117a848091`.
At that point nine exact provider-present associations still debited three H200
and six A100-80GB slots while Kueue and both physical pools were healthy.

Projected-worker runtime-readiness PR #1618 is merged at
`6ad2407d813d04aed79de2fea62723987ee56670` and published in release
`1.1.1394`; revision 473 includes it, but its production gate is not yet
complete. Draft cleanup PR
#1619 removes the rollout-only v1/v2/v3 projection readers after its separate
production gate. The change strengthens only canonical projection protocol v4
and adds no schema, EFS/RWX, KubeRay, Terraform, Terragrunt, or platform-pin
dependency.

The 2026-08-20 canonical-birth correction is merged in PR #1621 at
`bb81d16f1c2d194ec5bf488c1e1d87c8f44ee391`; revision 473 includes it, but it
is not activated or production-proven for a fresh service birth. It makes a
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
- external reclaim inventory is observed by a supervised, deployment-owned
  PostgreSQL-singleton renewal daemon, not by a service controller or launch
  handler. The observation is published as one
  PostgreSQL receipt bound to the exact policy identity, reconciliation-gate
  generation, Kubernetes context, and canonical provider payload. Claim/grant
  admission and every provider checkpoint consume only a fresh durable
  receipt. Missing, stale, mismatched, or invalidated evidence fails closed;
- a claim edge, allocation, observation, and current gate authorize admission
  only before commit. After commit, the exact committed intent is the
  immutable handoff identity; a monotonic successor may replace those planner
  inputs without retroactively revoking it. Every provider effect still needs
  a fresh external proof against the frozen identity plus current
  service/association, projection, no-paid, and physical-UID authority.
  Missing, uncommitted, corrupt, or freshly unattestable state fails closed;
- each reserved-fill provider-effect epoch atomically validates that complete
  authority together with the exact live request execution generation, claim
  token, worker instance, claimed queue delivery, retention pin, association,
  and effect phase. That short transaction commits the durable `PROVIDER_IO`
  boundary and closes before Kubernetes or other provider I/O. The execution
  claim and its guardian-authored quiescence receipt, not a PostgreSQL session
  or advisory lock held across the call, are the in-flight barrier. A service
  transition may commit after that linearization point, but it must cancel and
  await the exact execution receipt before replay or settlement. A fresh
  post-effect transaction that observes changed authority fails closed and
  leaves the already-started effect ambiguous for normal exact-provider
  reconciliation;
- reserved capacity is committed before the paid residual is calculated. A
  policy-admitted reserved row is demand-committed future supply and therefore
  suppresses compatible new Spot residual; a freshly policy-waiting row is
  only physical debit, not demand supply. The ordinary ordered-capacity plan
  may cover the remaining demand with paid capacity; no separate paid-handoff
  state or protocol exists. Unknown admission evidence revokes paid authority
  rather than guessing either supply or absence. Existing paid replicas drain
  as admitted reserved replicas become healthy;
- sequenced reserved-fill admission uses only the current authenticated
  PostgreSQL allocation plus durable row/pending debits and runs before the
  independent load-balancer demand read. It may commit fill intents only; it
  cannot publish a target, paid authority, provider effect, or retirement;
- that admission reads all live pending intents before the fresh replica
  ledger. The atomic pending-to-committed handoff can be counted twice during
  the read boundary, but never missed on both sides;
- the service ceiling includes every cleanup-unproven replica/admission across
  service versions until an exact provider-absence, execution-quiescence,
  association/request/queue/pin transaction removes that graph; normalized
  provider-down status alone is not evidence. It also includes all unexpired
  pending intents across service incarnations and allocation generations
  projected into the current capacity unit. A supported physical-to-logical update
  derives historical row width from the exact normalized accelerator shape;
  historical `planned_capacity=1` is not interpreted as a unit tag, and
  missing or conflicting persisted shapes fail closed. Only exact
  current-allocation rows become pool/card replay debits;
- the admission transaction locks only capacity-owning replica/admission rows,
  live pending intents, and exact current-plan replay keys. Provider-clean
  replica rows have already been removed atomically; unrelated terminal or
  committed intent history remains queryable but cannot make the hot
  transaction grow with retained history;
- missing or stale demand remains unknown, never zero: after a no-progress fill
  admission it still blocks paid launch, target publication, and destructive
  retirement;
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
paid-residual accounting, takeover fences, and the provider-proof receipt.
PR #1610 owns atomic admission, #1611 the durable-owner execution boundary,
#1612 passive preflight, and #1613 nonce-stable receipt reuse. PR #1650 moved
provider renewal out of launch waves and made the immutable PHX proof match the
existing implicit-flat/seven-ClusterQueue topology; it is deployed in release
`1.1.1422`. PR #1651, its teardown successors, and PR #1655 are deployed in
release `1.1.1427`. PRs #1656/#1657 are deployed in release `1.1.1429` / Helm
revision 502, the hold is released, and the canonical lifecycle-84/version-1
service recovered on the consolidated HA controller. The SkyPilot queues are
active through Platform PR #8824, and no EFS/PV/PVC is a runtime or correctness
dependency. Policy revision `1.1.1430` is merged, deployed as Helm revision
505, reauthorized at generation 10, and qualified through the deployment-owned
renewal/takeover horizon. Serve057 policy-admission feedback is merged and
deployed through PR #1659, release `1.1.1431`, and Helm revision 508.
PRs #1663--#1669 subsequently closed the provider-free whole-service
retirement gap, exact-request replay, proof-readiness, and selected-context
deserialization defects. Production readback, not source state, establishes
the live authority facts above. Lifecycles 89--91 were normally purged and the
clean service was recreated as lifecycle 93/version 1; lifecycle 93 is now at
the supported-down `FAILED_CLEANUP` boundary described above. PR #1670 and fix-forward PR
#1671 changed only SkyPilot manager, request-executor, and projected-worker
runtime behavior; they did not change admission policy, task placement, Kueue,
paid-residual accounting, shared infrastructure, or database schema.

Remaining work, in exact order:

1. Publish the already source-qualified protocol-v7/cohort-7 correction.
2. Direct-Helm deploy one homogeneous v7 API/controller/executor cohort with
   `--reuse-values` while lifecycle 93 remains cohort 6 and reconciliation stays
   `SEQUENCED_ACTIVE` generation 34. V7 exact-current launch predicates must
   fail every fresh effect closed during this bounded overlap.
3. Ensure the existing controller hold is false. Retry the supported
   `sky serve down boltz-l4-fleet --purge -y` under v7 until the N-1 typed
   finalizer retires all nine rows and proves total database/provider absence.
   Do not advance the gate first, demote, run `serve update`, mutate PostgreSQL,
   or delete a Pod/Workload manually.
4. Recreate the same service name from the canonical v7 definition with
   `min_replicas: 0`, zero fill floor, `utilization_gate: false`, and immutable
   server-owned projections only. A fresh `serve up`, not migration of the
   failed lifecycle, is the one supported birth path.
5. Run the complete non-compute preflight, exact-writer inventory, database
   census, and unchanged-Kueue snapshot gates. Re-read `sky serve status
   boltz-l4-fleet --endpoint`: down/up may allocate a different load-balancer
   hostname. Record and directly probe the new load balancer with only its
   local health read and authenticated `/_lb/capacity` read. Do not update
   Platform or roll compute-api yet. Keep Platform on the dead/old endpoint or
   prove an explicit upstream dispatch hold. If the hostname did not change,
   require that dispatch hold or a continuous zero-request census from
   recreation through reserved readiness. Do not send `/v1/models/model` or
   any catch-all proxy request before activation: on a fresh scale-to-zero
   service it is demand and can create a paid residual.
6. Perform exactly one active-to-active reconciliation CAS from
   `SEQUENCED_ACTIVE` generation 34 to generation 35, binding the complete v7
   writer inventory. Never reopen `LEGACY_ACTIVE` and never advance the gate
   before lifecycle-93 cleanup and fresh-service preflight both pass. Before
   reconnecting Platform, require every currently compatible scheduler-
   authorized reserved unit to have a durable assigned intent, at least one
   compatible reserved replica Ready, and zero paid claims, waiters, and
   provider Spot capacity.
7. If the endpoint changed, update only Platform's
   `SKYPILOT_SERVE_LB_URL` value and roll compute-api now; otherwise release
   the explicit dispatch hold, if used. This narrow endpoint repair does not
   authorize a SkyPilot runtime pin, Terraform/Terragrunt, or Kueue change.
   Prove that every compatible East scheduler-fit GPU and every PHX GPU
   admitted by Simone's unchanged Kueue policy has one durable intent and a
   provisioning or Ready replica; all admitted replacements must settle Ready
   or to typed terminal results. Require zero paid claims/waiters/provider Spot
   capacity when reserved capacity covers demand. Send the authenticated
   harmless `GET /v1/models/model`; immediately prove paid claims, waiters, and
   provider Spot capacity remain zero. Repeat immediate/+10/+30/full-stale-
   horizon checks, obtain rollout-audit acceptance, and record the exact
   result here.

Only then may a separate deletion-only compatibility cleanup proceed, and only
after that cleanup's own zero-legacy/null-writer predicates pass. Exact
completed HTTP exchanges or model jobs still require the separate PostgreSQL
idempotency/completeness contract and are not a blocker for honest current
processing/queued/in-flight telemetry.

A deliberately billable uncovered-demand production exercise is not a
remaining closeout requirement. The canonical commit-before-paid-residual,
exact-shape suppression, and paid-retirement contracts are covered by focused
unit and real-PostgreSQL tests. Production proved zero paid capacity at idle and
after one harmless authenticated request. Any future genuine demand not covered
by reserved capacity may still use the configured paid alternatives and will
provide ordinary operational drain evidence without synthetic spend.

Historical recovery record: PR #1614 reconstructed the exact frozen
committed-intent profile for nine provider-present associations, and draft
cleanup PR #1615 removed its enumerated historical-digest verifier after the
documented horizon. Those rows are not the cause of the revision-470 incident;
the former cold-controller retirement shelter is deployed and exercised. The
current gate is publishing and executing the protocol-v7 purge/recreate rollout
above, followed by final settlement and audit acceptance of the new scheduler-
authorized refill. Homogeneous `1.1.1448`
deployment, gate-34 reauthorization, live duplicate-up/takeover, Kueue
non-mutation, node health, fresh telemetry, the complete 180-second stale
horizon, and the primary 133/133 occupancy proof are complete.

Explicit exclusions: EFS/RWX is neither authority nor a correctness fallback;
KubeRay is not part of this Serve path; no Terraform or Terragrunt expansion is
required; and no `boltz-platform` SkyPilot runtime pin is added. Deployment is
fix-forward through the SkyPilot Helm release, while #8652 is used only for the
service configuration update it owns.

`storage.enabled=false`; active control-plane Pods and the former lifecycle-93
fleet workers have no EFS or PVC mount. The retired PVC, retained PV, and sole access
point are already absent. The Terraform-managed shared
filesystem and any historical scaled-zero ReplicaSets that still mention the
retired claim are optional deletion-only platform hygiene after a fresh
readback; they are not read by the live path and are not a rollout gate.

Completion means Serve057 and the corrective retirement contract remain
deployed, the PR-#1678 cohort is homogeneous and reauthorized,
`boltz-l4-fleet` continues to materialize 100% of fresh compatible capacity
that East scheduling and unchanged PHX Kueue admit, paid residual and drain
behavior remain source- and real-PostgreSQL-qualified, dashboard request totals
remain fresh and non-null through takeover, restart takeover succeeds without
RWX/EFS, and every immediate/+10/+30/full-horizon production gate passes.
Source-complete, deployed, activated, and production-proven are distinct
states. The +30 historical control-plane/HA/error boundary passed; final
overall production proof remains open until source-qualified protocol v7 is
published, deployed, used to finish supported lifecycle-93 cleanup, followed
by a fresh service recreation and the single generation-35 activation. Its new
refill must settle to Ready or a typed terminal result, and the final zero-paid
census must be accepted.

### Controller-independent dashboard read boundary

Status: PR #1637 is merged at `75debe4e3`, and its mixed-version scope repair
PR #1641 is merged at `77b653d42`; both are present in the deployed source.
Lifecycle 93 has shown fresh, non-null current-request activity from two
reporters through cross-Pod takeover, stable LB HA, and the full stale interval;
the dashboard has rendered it. Artificial provider-stall injection was not
required for this production closeout; the independence contract remains
source/test-qualified. The deployed contract uses scope-less
public wire bodies, retained legacy durable bodies with a required nullable
owner, and distinct authorized handler identities that fence older workers.

The services list and one-service detail identity load from the existing
PostgreSQL-backed `GET /serve/replica-summaries` projection. Persisted replica,
demand, history, and pricing reads therefore start as soon as that immutable
service hash lands. The detail bootstrap is the single owner of that summary;
the bounded replica hook consumes the landed snapshot and owns only replica
pages plus optional controller enrichment. Controller status remains optional
enrichment for endpoint and live autoscaler fields. It owns a separate
single-flight boundary, so a hung controller request cannot hold the persisted
refresh owner, suppress a manual/visibility refresh, or hide confirmed
processing, queued, rejected, and freshness fields. A freshness boundary
fences a stale enrichment response but does not accumulate a second controller
request while the first is hung. If a rolling old-server or non-consolidated
response revokes the direct capability, the sole fenced request must settle
before exactly one compatibility successor starts. A modern direct `not_found`
response is authoritative absence and never falls back to controller identity.

A successful modern direct response is authoritative for the complete service
identity set, including an empty set. Controller rows may enrich only an exact
name/hash match; they cannot retain a controller-only row, resurrect a removed
service, replace PostgreSQL lifecycle/count fields, or merge two service
incarnations. A direct transport failure after capability was proved preserves
the last authoritative identity and schedules a later direct retry; it does not
silently restore controller identity authority. Policy display is derived only
from the elected version's immutable `version_specs.spec`. The mutable legacy
`services.policy` field is never a fallback because it may describe an older
stored or previously elected version. A missing or unreadable elected
projection renders policy unavailable.

Request activity has three distinct honesty levels. In-flight is exact only
when every currently relevant backend reporter is covered; otherwise the UI
shows the confirmed lower bound and the number of backends whose occupancy is
unknown. Queue depth, recent arrivals, rejects, and their observation age come
from the controller-independent PostgreSQL demand projection. The prediction
histogram is not an exact completed-logical-request ledger: it contains bounded
per-load-balancer terminal prediction observations, an async terminal may be
missed before polling, and retries or load-balancer failover may duplicate a
logical request. The UI therefore labels the sum as terminal prediction
observations, not completed requests. Its latest-hour report timestamp is the
most recent report containing nonzero terminal observations; equal cumulative
retries can advance it without a new completion. It describes report recency,
not event time, unique-request completeness, reporter coverage, or proof that a
displayed zero means no logical request completed. If a demand refresh fails,
retained values are labeled `last reported`/`last confirmed`, their frozen
server age is hidden, and the UI identifies the value as the last persisted
snapshot. A controller enrichment refresh similarly returns endpoint/target
cells to Loading and then Unavailable on failure instead of presenting the
previous controller value as fresh alongside a newer PostgreSQL timestamp.

All direct Serve dashboard reads use the existing canonical request-access
resolver. The resulting owner predicate is pushed into the service identity or
grouped summary SQL, rather than filtering returned rows in application code.
Queued status and placement reads carry a server-derived execution scope; the
route overwrites any caller-supplied value and execution rechecks the same
owner predicate, closing same-name replacement races. Versions endpoints stay
unchanged because they are already administrator-only. This slice adds no
schema, EFS/RWX, KubeRay, Terraform, Terragrunt, service-version, or platform-pin
dependency.

### Historical provider-proof and launch incident record

The earlier production launch wave exposed two steady-state gates. First,
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
advanced deliberately to revision `1.1.1386`. At that historical point, source
merge/CI was complete while deployment and production qualification remained
open; later releases deployed and superseded that launch-owned refresh path.
Second, provider effects could
race the moving allocation fence before the generalized
request/association receipt is durably bound, producing new ambiguous capacity
debits and exposing the RWX stale-file-handle dependency. The required final
path was the planned atomic PostgreSQL replica/association/request admission
followed by asynchronous provider actuation. That path is now deployed;
neither retries nor EFS became authority.

The first atomic launch wave on release `1.1.1389` then exposed a receipt
renewal race inside that single-flight boundary. Replicas 55746--55755 proved
that the v2 request can reach real Kubernetes Pods, while sibling launches
failed either during policy authorization or at the final reclaim-authority
check with no gate-generation change. The Serve054 row was replacing its
random nonce on every five-second refresh, so an unchanged provider proof could
revoke a concurrently minted exact reference between policy return and the
terminal guard. A cached receipt was also reusable until the last instant of
its five-second horizon, leaving no time for the terminal PostgreSQL check. The
initial fix-forward kept the nonce for an identical canonical proof renewal,
rotated it for every schema or proof-content change, used one nonblocking
READ-COMMITTED MVCC statement as the terminal linearization point, and
reserved the final 0.5 seconds for the terminal guard using live local elapsed
time at actual handoff. The shared fleet gate was acquired before that
five-second ticket was minted, so an unbounded gate wait could not consume the
reserve. It changed neither the five-second maximum age nor any gate, service,
projection, physical-cluster, or provider-proof predicate. That statement
describes the superseded launch-owned ticket path; deployment-renewal
publication must never acquire the protocol row.

The first larger protocol-v2 activation wave at gate generation 7 then showed
that the 0.5-second launch handoff reserve was not a sufficient contract.
Replicas 55930 and later often reused one receipt with less than a second left;
policy decoding, caller validation, and the multi-statement terminal PostgreSQL
read consumed that remainder under ten-way launch concurrency. An exact
committed-intent snapshot with the newer broker generation passes every durable
claim, service, projection, and policy predicate when supplied a freshly
authorized receipt, isolating that sampled failure to the final provider-proof
freshness predicate. The fix-forward retains the strict five-second terminal
expiry and all exact nonce/digest/gate/content checks, but requires at least two
seconds of freshness at both repository selection and final policy-to-provider
handoff.
A receipt inside that bounded entry budget is single-flight refreshed before it
is returned; there is no generic authority retry and no provider effect on an
expired or indeterminate proof.

PR #1622's freshness correction exposed a separate successor-edge race at the
same terminal boundary. A launch fenced and atomically committed under service
generation G could reach the provider guard after the broker had published
G+1. The guard correctly required the current claim set to be an authoritative,
monotonic successor, but it also required G+1's mutable edge set to contain and
restate G's exact pool edge. A legitimate G+1 replacement or removal therefore
revoked an already committed G launch even though its immutable intent,
replica, service version, policy ticket, projection, and provider proof all
still matched.

The terminal contract distinguishes pre-commit authority from post-commit
provider authority. Before atomic admission commits, the current allocation,
claim edge, observation, ticket, and reconciliation gate must all authorize
the intent. The same root transaction then writes the replica's immutable
`reserved_fill_intent_idempotency_key` scalar, creates its generalized
association/request/queue/pin tuple, and transitions that exact intent from
ACTUATING to COMMITTED. After commit, that scalar-linked intent and frozen
launch profile are the sole reserved-fill handoff authority; a successor may
advance or replace the allocation, claim set/edge, observation, ticket receipt,
or gate generation without retroactively revoking the accepted launch.

The post-commit guard still requires the current service lifecycle,
incarnation, resource scope, elected version, generic controller capability,
exact immutable worker projection, replica record UUID, physical cluster/Pod
identity, fresh provider proof for that immutable scope, and absence of any
paid claim. A null or malformed scalar, non-COMMITTED/missing intent,
scalar/JSON mismatch, association mismatch, version/record/projection mismatch,
paid claim, or failed physical proof rejects provider effect. There is no
fallback to the mutable pre-commit chain or JSON-only identity. PR #1623's
successor-edge exception established the need for this split but did not
complete it; the additive PostgreSQL-only Serve056 link is the canonical
fix-forward.

Historical rollout record: the additive reserved-fill, exact worker-projection, generalized
non-pool binding, demand, route, executor-termination, provider-independent
route, durable actuation-intent, supply-aware paid-residual, successor-schema,
and scheduler-mode prerequisites are merged through PRs #1537, #1540, #1542,
#1547, #1548, #1549, #1552, #1553, and #1555. Production Helm revision 436 /
release `1.1.1359` runs the exact 2 API / 2 controller / 2 executor split-role
cohort on RWX storage. Production committed clean successor version 63 on
2026-08-19. Before platform PR #8652 merged, one deployment attempt durably
stored an equivalent version 64 after API acceptance but failed before
election. Its initial post-merge combined run `32281262288` validated and
dry-ran successfully, then failed closed in the separate test target because
`boltz-l4-fleet-test` did not exist; that historical run made no production
service change. The workflow subsequently completed: revision 472 ran the
uniform successor cohort and elected version 64, which later admitted
provider-free capacity before the teardown recorded at the top of this design.
The existing prod-only
`ml-model-deploy.yml` dispatch with `models=boltz-2`,
`mode=prod`, and `provider=skypilot` targets only `boltz-l4-fleet` and remains
the canonical application path. Version 64 has the same three worker
projections and `utilization_gate: false`. Historical exact database readback
proves its three non-null worker
projections include a PHX H200 projection that uses
`default-scheduler`, `be`/`be-ls`, priority -1000/`Never`,
`skypilot-pool-sa`, and no Pod Identity role; both east projections retain
`gpu-binpack-scheduler`. Version 63 has `min_replicas: 0`, a zero fill floor,
`utilization_gate: true`, and the known-good `v3.682.2-boltz-2` image. Version
64 changes only that gate. It
supersedes version 62, which proved the projection shape but remains ineligible
because it pins the rejected `v5.44.1-boltz-2` image. At the historical
2026-08-19 inspection, compatible pools published free A100, A100-80GB, and
H200 GPUs but the then-unelected version 64 had not admitted them. The
reconciliation gate at that inspection was
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

The historical direct-Helm split-role rollout and generation-34 qualification
completed, but the supported v6 teardown left lifecycle 93 in
`FAILED_CLEANUP` with nine `UNKNOWN` rows after both providers reached zero.
The current executable sequence is exactly:

1. Publish the source-qualified v7 correction.
2. Roll the complete API/controller/executor fleet homogeneously to v7 by
   direct Helm with `--reuse-values`, leaving the service on cohort 6 and the
   gate on generation 34 so all fresh effects fail closed.
3. Ensure the controller hold is false and retry supported
   `sky serve down boltz-l4-fleet --purge -y` until v7's N-1 typed finalizer
   proves total absence and removes all nine rows.
4. Recreate a fresh same-name canonical v7 service; do not update or migrate
   the failed lifecycle.
5. Pass the full preflight, exact-writer/database census, immutable projection,
   endpoint, and byte-exact unchanged-Kueue gates. Directly probe only the new
   load balancer's local health and authenticated `/_lb/capacity`; keep
   Platform on the dead/old endpoint or prove an upstream dispatch hold. If
   the hostname is unchanged, require that hold or a continuous zero-request
   census through reserved readiness.
6. Run exactly one active-to-active CAS from `SEQUENCED_ACTIVE` generation 34
   to generation 35. Do not demote, reopen `LEGACY_ACTIVE`, or advance the gate
   before purge and recreation complete. Before reconnecting Platform, prove
   all currently scheduler-authorized reserved units have durable assigned
   intents, at least one compatible reserved replica is Ready, and paid
   claims, waiters, and provider Spot capacity are zero.
7. Only then update `SKYPILOT_SERVE_LB_URL` and roll compute-api if the endpoint
   changed, or release the explicit dispatch hold if it did not. Fill and prove
   100% of compatible capacity that East's scheduler and Simone's unchanged
   PHX Kueue configuration make available, plus zero paid residual whenever
   reserved capacity covers demand, through the complete production horizon.

Down/up can allocate a new load-balancer hostname. After recreation, obtain it
again with `sky serve status boltz-l4-fleet --endpoint`, but do not reconnect
Platform before generation 35, complete reserved-intent assignment, one
compatible reserved Ready replica, and a zero-paid census. Before that gate,
only direct load-balancer-local health and authenticated `/_lb/capacity` reads
are permitted; Platform remains on the dead/old endpoint or behind an explicit
dispatch hold. An unchanged hostname instead requires that dispatch hold or a
continuous zero-request census from recreation through reserved readiness.
After those gates, the only permitted changed-endpoint correction is the
surgical `SKYPILOT_SERVE_LB_URL` value and compute-api rollout. Then send the
authenticated harmless `GET /v1/models/model` and immediately repeat the zero-
paid census. This does not authorize a runtime pin, Terraform/Terragrunt, or
Kueue change. Keep required cleanup PR #1660 and every deletion-only
compatibility cleanup blocked until this sequence and its own zero-legacy/null-
writer gates pass.

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

Historical initial-activation record (non-executable): code rollout and gate
activation were separate operations. The image could be
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

Provider-present cleanup now has one digest mode. The locked durable-body
digest must equal `input_digest`, and the body owner ID/name and association
tenant must equal the immutable locked service-owner tuple. Every body,
context, tenant, cluster, profile, or authority change fails closed. The
temporary `LEGACY_HTTP_NORMALIZED` verifier was scoped only to the nine
enumerated pre-atomic rows and is removed after those rows reach exact provider
`ABSENT`, release their pins and debits, and the zero-legacy census remains
zero at T0 and T+180 seconds. No schema mode was added for that verifier.

The removal gate was captured on 2026-08-21. Production censuses at
02:18:15 UTC and 02:21:53 UTC (218 seconds apart) each found zero unsettled
`RESERVED_FILL` associations, zero associated request-retention pins, and zero
`boltz-l4-fleet` paid-capacity claims. All two API, two controller, and two
executor Pods were Ready with zero restarts on image digest
`sha256:a61cc5ecf391ed5dfc9861d3ecebbbc55f5c7bf9c3ba0089ec3691bbe0618e3a`.
The obsolete verifier therefore has no remaining provider-present cleanup
population, and cleanup PR #1615 is eligible to merge independently of the
broader fill-convergence qualification.

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

### Reclaim-safe failed-service bulk teardown

Normal finalization and failed-service purge use one shared preparation and
cleanup dispatch for retained bound launches. An already `AMBIGUOUS`, terminal,
execution-quiesced protocol-v2 `RESERVED_FILL` association never re-enters the
generic request-cancel or name-only teardown paths. Protocol v1, ordinary
profiles, malformed identity, or missing exact authority remain quarantined.

The closed provider outcomes are:

- `ABSENT`: record the exact physical-UID/context/replica observation after
  executor quiescence, atomically project the association, clear its replica
  pointer, and release its request-retention pin. A restart consumes that
  PostgreSQL history only when the SkyPilot cluster record is also absent; it
  authorizes replica-row removal and no new provider operation.
- `PRESENT`: atomically persist the existing immediate-cleanup marker while
  retaining the association and pin, then use the existing exact
  `terminate_bound_non_pool_provider_present_cluster` path. Successful fenced
  down is not completion until a forced fresh `ABSENT` observation projects
  the association. If the SkyPilot cluster record disappeared before cleanup,
  the same exact reconciliation path performs a fresh provider read instead of
  falling through to generic teardown.
- `UNKNOWN` or `REPLACED`: retain the association, pin, replica row, and
  capacity debit. Neither result is absence or cleanup authority.

Provider reads occur after the readiness/authority transaction releases all
PostgreSQL row locks. The service lifecycle/name lock remains held across bulk
preparation and cleanup. While bound cleanup still owns either an unresolved
`PRESENT` association or an already-projected `ABSENT` replica row, normal
finalization retains the current lifecycle epoch rather than advancing it;
otherwise the immutable admission epoch would conflict with the service row
before exact cleanup can consume the row. After acquiring that lock and
immediately before scheduling destructive down, the cleanup owner revalidates
the exact
association/controller incarnation and owner epoch in PostgreSQL. A takeover,
even with reused parent PID/IP, therefore fails before provider deletion.
Failed-purge owner rotation propagates the newly claimed authority and reloads
the persisted marker before cleanup.

This correction changes no schema, migration, provider, Kueue, Helm,
Terraform/Terragrunt, EFS, KubeRay, or platform-pin contract. It merged as PR
#1651 and is deployed in release `1.1.1423` / Helm revision 489. Production
proved that it removed all matching `boltz-l4-fleet` provider Workloads/Pods,
external-load-balancer objects, and central cluster rows. Final PostgreSQL
deletion then exposed a narrower deterministic ordering defect: 71 replica
rows still reference zero-cost intents when service deletion cascades into
those intents. The successor replica-before-service delete-order fix is merged
and deployed in release `1.1.1427`; normal lifecycle removed the old graph and
recreated `boltz-l4-fleet` as lifecycle 84/version 1 without manual deletion.
Later normal retirement of lifecycles 85 and 89--91, clean lifecycle-93
recreation, and the complete stale/quiescence interval qualified the evidence-
backed release boundary without generic cancel/down or manual SQL deletion.
Deletion-only cleanup remains separately gated on its own zero-legacy/null-
writer census and the final +30 refill audit.

### Atomic reserved-fill replica and request admission

#### Serve056 committed provider handoff

Implementation PR: corrective PR #1630
(`fix/serve056-complete-committed-handoff`) on top of merged PR #1629.

Operational correction (2026-08-21): the automated release path deployed the
incomplete PR #1629 source as Helm revision 474 / release `1.1.1403`, immutable
digest `sha256:8bd915cb851b108420687f580dc87ec9de2d900d42545a8a61cf70d18f9e042a`,
before corrective PR #1630 merged. Although its six writer Pods were ready, a
restarted service controller deterministically rejected the newly installed
Serve056 head because its compiled additive-revision set ended at Serve055.
Revision 475 / release `1.1.1404`, immutable digest
`sha256:8047b32d5703eb280b621c5f7f404daadb2bb98738327e32d0f44355cbfe603e`,
deployed PR #1632's one-line application-authority correction and restored
service-controller adoption. It did not contain #1630's fresh provider proof,
scalar-`NULL` cleanup, or cohort-rotation contracts. That precursor was later
superseded: corrective PR #1630 merged and deployed in release `1.1.1410`, and
Helm revision 489 inherits its committed-handoff contract. Schema 056 remains
forward-only; no schema/application rollback or manual state rewrite is
authorized.

Serve056 closes the remaining mutable-authority gap between atomic admission
and the first provider effect. It adds one nullable, initial-insert-only scalar
to `replicas`, `reserved_fill_intent_idempotency_key`. For a new protocol-v2
reserved-fill row, that scalar is the authority edge; the same value in
`ReplicaInfo` JSON is only a checked projection. A composite foreign key binds
the row to exactly one `(service_name, intent_idempotency_key)` actuation
intent, and uniqueness permits one replica handoff per intent. The migration
does not backfill historical rows. Existing scalar-`NULL` rows remain visible
for evidence-based cleanup but cannot authorize a new launch effect; after the
migration, a rolling old writer that attempts a new JSON-only protocol-v2
insert is rejected.

The transaction deliberately changes the exact leased intent from `ACTUATING`
to `COMMITTED` before inserting its scalar-linked replica, then creates the
association, executable request, queue delivery, and retention pin and updates
the replica pointer before the outer commit. All operations share one root
transaction. Immediate PostgreSQL guards require an exact zero-cost, non-Spot,
non-paid replica and exact service, record UUID, version, pool, allocation,
frozen gate/policy identity, observation, physical UID, Kubernetes context,
accelerator/card count, planned capacity, worker-projection digest, and first
allowed-location coordinates. The replica location and resources override
must agree. The guard intentionally does not compare the whole serialized
location object: the durable intent represents `image_id` as a JSON object
while `ReplicaInfo` losslessly represents the same mapping as key/value pairs.
Deferred constraint triggers prove both directions of the committed
intent/replica/association graph at transaction commit. The scalar link and a
committed intent are update-immutable, and the foreign key prevents removal of
a committed intent while its replica remains linked.

Before commit, admission continues to use the complete mutable chain: the
current authenticated allocation and claim set, current observation and
reconciliation gate, current deployment-policy authorization and provider
inventory, exact controller/service ownership, and headroom. After commit,
none of those planner inputs can retroactively revoke the accepted launch.
The scalar-linked immutable `COMMITTED` intent is the sole postcommit
*admission identity*, together with the still-current service
lifecycle/incarnation, elected version, controller capability, immutable
worker projection, association/request execution generation and digest,
record UUID, and frozen physical UID/context/card/count. A newer allocation,
replacement claim edge, current gate, or newer observation is not reread at
this boundary. Runtime provider/admission/no-paid facts are different: every
bounded provider effect asks the installed deployment policy for a fresh
five-second proof against the intent's frozen policy identity, gate generation,
scope, and projection. The terminal PostgreSQL read validates that exact proof
receipt before the physical-UID fence and provider mutation. The current
mutable gate or policy-identity row is never substituted. If the installed
plugin no longer implements the frozen identity, that old committed intent
fails closed and must settle before cohort rotation; there is no fallback to a
new identity. Lifecycle, elected-version, controller-capability,
association/request-execution, projection, live provider fact, or physical-UID
drift still fails closed. No paid or Spot launch can use this handoff.

The nullable link creates one deliberately isolated rolling-compatibility
case. A scalar-`NULL` retained protocol-v2 row can never launch, but an exact
provider-present row must remain cleanable. Only after the existing
`POST_EFFECT_AMBIGUOUS` + `PRESENT`, copied terminal/quiescence, zero-cost,
non-Spot, no-paid, unmaterialized cleanup boundary passes may a cleanup-only
resolver treat the JSON intent key as a candidate. It rereads the exact
replica, requires a scalar `NULL`, and exact-matches the immutable `COMMITTED`
intent, record UUID, all attribution/projection fields, original observation,
and persisted association profile. It grants teardown only: admission,
workspace, lease, materialization, and provider launch remain scalar-only.
There is no backfill. A stacked cleanup removes this resolver and its
transition tests after there are zero scalar-`NULL` protocol-v2 replicas, zero
unsettled scalar-`NULL` provider-effect associations, and zero scalar-`NULL`
cleanup-unproven markers through one complete stale, execution-quiescence, and
provider-reprobe horizon. That cleanup is draft PR #1633, stacked directly on
#1630; it replaces the automatically closed draft #1631 and cannot merge with
the feature or before those production gates hold.

The first in-tree Kubernetes `create_namespaced_pod` guard cannot require a
Pod UID because no Pod exists yet. It pins and freshly proves only the frozen
Kubernetes context and physical cluster UID before permitting the create. The
returned or exactly adopted Pod UID is then captured and fully attested by the
existing Kubernetes path; every later guarded effect epoch fresh-reads and
reattests that same Pod UID. A context retarget therefore fails before the
first effect, while same-name Pod replacement fails at the post-create or
later-epoch attestation.

Serve056 is PostgreSQL-only and forward-only. It rotates the existing generic
non-pool capability cohort from 1 to 2 because executor-side provider semantics
change. Mixed cohorts fail the existing fleet capability barrier. An existing
service may advance its binding epoch and publish cohort 2 only after every
live API/controller/executor reports cohort 2 for the full 70-second
quiescence horizon and all cohort-1 associations are settled, quiescent,
unpinned, and dequeued, and every association-backed replica row is physically
retired. New scalar-linked admissions remain closed until that transaction
commits. The fleet read and service CAS share one transaction that
holds `api_server_instances` in `SHARE` mode, so a cohort-1 registration or
heartbeat cannot appear between the barrier and rotation. While the durable
service still advertises cohort 1, a
cohort-2 controller has a narrowly named adjacent-cohort settlement authority.
It may adopt an already-admitted request, record provider evidence, drive exact
PRESENT/ABSENT cleanup, or retire a pointerless pre-admission replica after
proving that no request or unsettled association exists. It cannot admit a
request or enter provider I/O: both operations independently require the
current code cohort. Interrupted old reserved-fill rows retain the established
quiesce-and-teardown path rather than being re-driven. Cohort 0, a non-adjacent
cohort, a malformed capability tuple, or a changed association tuple grants
nothing.

A failure is repaired with another schema-compatible SkyPilot image and direct
Helm fix-forward. There is no alternate provider path, historical backfill,
timeout increase, retry authority, EFS/RWX state, KubeRay component,
Terraform/Terragrunt change, or `boltz-platform` runtime pin. The source in
this change is not deployment, activation, or production-proof evidence.
Qualification must include real-PostgreSQL 055→056 upgrade and retained
provider-present cleanup, old-writer rejection, commit-graph and rollback
faults, nullable-field adversarial cases, live `image_id` encoding,
current-service revocation, mutable-successor non-revocation with a newly fresh
proof at repeated provider epochs, mixed-cohort admission/effect closure,
previous-cohort pre-admission retirement and provider-evidence cleanup, and
physical-UID retarget rejection before the effect body runs.

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
global guard is the transition's deliberately conservative behavior. Serve058
removes it only after both columns become `NOT NULL`, the zero-`NULL` and
no-old-writer gates pass, and the foreign key can serialize every future
service creation against exact user deletion.

The atomic reserved-fill admission module owns one root PostgreSQL transaction
and one nested savepoint. In the
canonical sequencer -> protocol -> broker lease -> lifecycle -> service ->
actuation intent commit -> capacity ledger/replica -> association -> API
request -> queue -> retention-pin order, it revalidates all current authority,
transfers the exact intent from `ACTUATING` to `COMMITTED`, inserts the typed
replica, resolves `RESERVED_FILL/v1`, and inserts or exact-matches the
deterministic association, non-retryable request, queue delivery, and retention
pin. The durable actuation lease's immutable `FillIntent` is the sole source
for its pool epoch and ordinary-admission high-water; atomic admission carries
no duplicate caller-provided copies that could disagree with that intent. The
Serve staging primitive only performs non-committing writes on the supplied
connection; the atomic layer owns savepoint release, root commit/rollback, and
lost-ack hydration. It publishes database-assigned admission state into
manager memory only after root commit. Any validation or suffix-write failure
rolls back the replica, intent transfer, association, request, queue row, and
pin together. For this internal atomic profile, request identity and its
canonical digest are computed only after those immutable owner, workspace, and
API fields have been stamped into the prepared launch body.

The authoritative Serve056 schema and rollout contract is the subsection
above. Its additive migration is
`sky/schemas/db/serve_state/056_committed_reserved_fill_handoff.py`. It performs
no backfill and the admission transaction is intent-first, then replica,
association, request, queue, and pin. Deferred constraints validate the final
bidirectional graph; they do not make a historical JSON key authoritative or
permit a second write order. The cleanup-only scalar-`NULL` resolver is the
single isolated transition branch and is removed at its documented horizon
gate. Serve058 remains the distinct, horizon-gated owner-column `NOT NULL`
cleanup; the migrations are not combined.

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

Serve055 adds nullable `owner_user_id`
and `owner_user_name` columns to the existing `services` table, a
both-or-neither non-empty check, an `owner_user_id -> users.id ON DELETE
RESTRICT` foreign key, and a PostgreSQL trigger that permits the one `NULL` ->
tuple attestation but makes the tuple immutable thereafter. It is
PostgreSQL-only, forward-only, and adds no table or SQLite path. The API user
deletion guard rejects every non-internal deletion while either owner column
is missing or nullable. In the later Serve058 owner-cleanup steady state,
exact-owner listing
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

Separately, the Serve058 owner-column cleanup is authored on
`fix/serve-atomic-fill-admission-cleanup` and open as cross-linked draft PR
#1660, stacked on #1659. It is not the historical-digest verifier cleanup stack
introduced above. Its only steady-state change is Serve058: after the
transition gates below, make both owner columns `NOT NULL`, remove the application
`NULL`-attestation controller branch, and retire its transition-only
observability/tests. This is a PostgreSQL-only constraint change: the
dialect-neutral SQLAlchemy model remains nullable for controller-local SQLite
at its retained Serve037 head. Serve058 must verify or reinstall the owner FK
and existing owner-immutability trigger (or atomically replace the trigger with
a simpler permanent equivalent) before applying `NOT NULL`; every non-null
tuple mutation remains rejected.
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
source-no-fallback tests must also pass. Serve058 then validates zero `NULL`
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
- Use one reconciliation coordinator with a single reserved-fill admission
  site before the independent demand, target, paid, and retirement phases.
- Debit all already represented service capacity and accepted fill from durable
  PostgreSQL state before producing new fill intents; never reserve capacity
  for an ordinary-demand action that has not committed.
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
- Make canonical worker placement projection v7 the only owner of new
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
- Ordinary zero-cost admission and sequenced fill serialize through the same
  PostgreSQL admission order. A committed ordinary row consumes service
  headroom and advances the allocation high-water; speculative demand does not
  debit a grant. Paid residual is computed only after accepted fill is visible.

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

### Mixed-release trust and compatibility

Durable state is not authority merely because another SkyPilot version wrote
it. A runtime advertises the bounded protocol versions it can decode and the
single actuation version it can write. The current release and its two
immediate predecessors are the long-term target for read, status, recovery, and
normal teardown support inside the declared window. The current generic
capability fence guarantees broad recovery and settlement only for the
immediately adjacent cohort. N-2 supports only already-proven terminal
`PROJECTED`/`ABSENT` replica-row retirement; the N-1-to-N-2 rotation gate
requires every association-backed replica row to be physically retired first,
even if it has reached that canonical state. N-2 retirement therefore runs
only while the service retains the association's exact old cohort tuple; the
remaining association history is harmless after its replica is gone only when
each row is settled, terminal, projected, pin-released, and internally
consistent. Active, unpinned, partial, or malformed association-only history
fails closed before takeover. Only a runtime whose actuation
capability exactly covers the durable gate may allocate or call a provider;
unknown newer state and malformed older state fail closed without making
teardown unavailable. New durable shapes therefore land additively, keep their
older readers for two releases, and remove them only after the live-row and
stale-writer gates prove the compatibility window empty.

An immutable Serve child projection carries no ambient central PostgreSQL
config identity from its parent. Provider observations re-prove their exact
Kubernetes context, physical UID, credentials, RBAC, and deadline in the child;
they do not treat a parent version's config receipt or process environment as
delegated central authority. The current realtime-catalog correction changes
no durable encoding or capability number, so every reader already supported by
its declared cohort retains its existing behavior while the fully converged
writer cohort adopts the correct provider-read boundary. This statement does
not expand broad settlement beyond N-1 or the exact terminal-ABSENT N-2
retirement boundary. The current rollout still
requires one exact writer digest before actuation is reauthorized;
mixed-release read compatibility is not permission for mixed writers.

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

Claim-set generation and allocation generation are deliberately separate
identity domains. `replace_claim_set()` is the sole canonical claim-identity
builder; persistence never accepts a second caller-defined interpretation of
the hash. Its closed semantic payload contains protocol, exact service
incarnation hash, committed service version, configured `max_replicas`, fill
floor, weight, utilization-gate mode, and the ordered edges. Each edge contains
the pool and legacy pool keys, position, access context, physical UID, canonical
accelerator names, replica slot width, and the closed per-accelerator worker-
projection digest map. Recreating a same-named service therefore rotates the
claim identity, as does a configured maximum, version, projection, topology,
or configured fill-policy change.

Heartbeat-owned runtime values (`holdings_fill`, `launchable`,
`global_headroom`, `utilization_ceiling`, utilization state, the partitioned
edge floor, and `effective_cap`) are persisted atomically under that generation
but excluded from its semantic hash. Per-edge runtime `weight` is only the
persisted copy of the one configured service weight and is not hashed a second
time. This does not make runtime budgets advisory: an edge-cap mismatch makes
`read_current()` reject the old allocation immediately; a transition to
unlaunchable rejects a nonzero allocation; and a changed broker grant or feed
advances its pool epoch. Grant-time code locks and recomputes row and pending-
intent occupancy. Other nonbinding holdings, headroom, utilization, derived-
floor, or launchability changes take effect in the next bounded broker round
without rotating the whole service claim. Post-commit provider authority is
already the immutable intent/replica/profile contract described above, so an
already committed intent remains materializable and an occupancy debit across
such a runtime update.

Protocol-v2 capacity discovery uses two distinct exact-card replica-slot
mappings in the existing round envelope. The committed observation ledger is
the immutable raw provider/Kueue GPU result.
`$skypilot-observed-free-v1` is that exact observation converted once to slots
with the authenticated `broker_slot_width`; `$skypilot-spendable-free-v1` is
the broker's same-width slot result after applying the sequenced occupancy
debit. Both mappings are validation inputs; the spendable mapping is discovery
metadata rather than per-service launch authority and is removed before
service-feed epoch comparison. The pool-cap hint is the round's conserved
`sum_holdings + sum(spendable)`. The allocator then takes the maximum of that
round-consistent hint and current local holdings.
Exact-card clipping can make the hint a conservative lower bound on aggregate
entitlement when a debit is ambiguous across cards; it is exact for an
unambiguous or single-card pool and never moves capacity between cards. This
changes no database schema or allocation-map wire schema.

The decoder is tri-state and pointwise. A closed all-zero spendable mapping
with one zero entry for every pool card is authoritative zero; literal `{}` is
malformed for a nonempty pool. Counts must be non-boolean nonnegative integers
in closed, casefold-canonical observed-slot and spendable-slot mappings over
exactly the pool cards, with
`spendable_slots[card] <= observed_slots[card]` for every card. Validity
also requires protocol v2, the expected slot width, and the exact committed
observation row named by generation, sequence, materialization sequence, and
payload digest. The reader recomputes the observed slot map from that row with
the authenticated width, requires exact equality with
`$skypilot-observed-free-v1`, and requires its sum to equal
`last_observed_free`. Separately, `last_observed_free_ts`, round
`snapshot_time`, and committed observation `observed_at` must be exactly equal.
A present
malformed, duplicate-after-casefold, out-of-pool, composition-invalid, or
provenance-inexact map withholds to current holdings. A committed N-1 round
that otherwise has valid raw provenance but lacks the additive key uses only
the non-widening maximum of current holdings and the previous cap. A blackout
or stale sequenced round likewise carries that bounded prior hint and never
reinterprets stale raw free. Non-sequenced and protocol-v1 rounds preserve
their historical bytes and discovery behavior exactly. The new key is emitted
only after the homogeneous successor writer gate is active; the `1.1.1443`
writer must never share an emitting cohort.

Qualification must prove with real PostgreSQL and focused pure tests that:

- each immutable field above rotates claim generation, including same-name
  service recreation and configured-maximum change, while every classified
  runtime field preserves it;
- a same-generation cap mismatch immediately invalidates the old map and the
  successor allocation advances independently, while a committed intent
  remains materializable and debited;
- publication followed by intent commit, row materialization, and the next
  heartbeat conserves one slot without adding post-publication local holdings
  to stale free;
- closed-all-zero, literal-empty, absent-N-1, stale/blackout, and malformed
  spendable metadata follow the fail-closed rules above, including forged raw
  metadata versus the exact committed observation, pointwise same-total card
  fabrication, zero-free-card, ambiguous composite-card, and multi-GPU slot-
  width cases;
- v1 and legacy envelope bytes remain identical, and a spendable-only metadata
  change neither bumps a pool epoch nor delays the N-1 settle/read path.

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

Once the durable gate is `SEQUENCED_ACTIVE`, the controller first reads the
current authenticated allocation and row-represented capacity, then invokes the
single reserved-fill admission site. That provider-free phase can commit only
durable intents. If it commits any, the controller notifies reconciliation and
returns so the next pass replans from those PostgreSQL debits before reading
demand or publishing paid authority. It never invokes a provider path. The
later demand/planning phase enters
`Autoscaler.sequenced_reserved_fill_planning()` only to compute demand, target,
paid residual, and the PostgreSQL-derived retirement shelter; it has no second
fill-admission site. A missing or unreadable authenticated map means zero new
fill for that pass; there is no fallback.

`ReservedFillPlanner` is database- and provider-free. From one immutable map it
computes deterministic, exact pool/card intents after applying:

- service-global `max_replicas` headroom in the configured physical or logical
  capacity unit;
- all nonterminal row-represented service capacity, without an inferred demand
  target;
- durable nonterminal fill rows from the same allocation map; and
- the last receipt-proven rotation anchor.

Planning mutates no feed, fairness cursor, or replica state. Its deterministic
idempotency key is correlation and replay-debit evidence; the PostgreSQL intent
and returned receipt are the pre-provider grant boundary.

### 5. Atomic intent grant and asynchronous pool dispatch

`SkyPilotReplicaManager.accept_reserved_fill()` validates the typed plan and
manager/service owner, rereads the exact current allocation under the broker
serialization lock, and calls the PostgreSQL `grant_plan()` transaction. That
transaction locks the service, every live pending intent, exact current-plan
replay keys, and only replicas without provider-down success; retires expired
grants; recomputes row plus pending-intent headroom; and persists accepted
intents in plan order. Cleanup-proven replicas and unrelated terminal or
committed intent history are not selected or locked. This call owns no provider
phase, physical-cluster read, replica ID, API request, or worker thread.

`FillCommitResult` accounts for every planned intent exactly once as accepted
or deferred. Replaying the same idempotency key returns the existing pending or
committed intent, and the controller advances rotation only from this durable
receipt. A changed allocation, owner, version, ceiling, or actuation authority
fails closed before provider work.

The existing durable-intent dispatcher subsequently leases each physical pool
on an independent lane. It performs the bounded physical preflight and then
uses the shared generalized non-pool transaction to materialize the replica,
association, executable request, queue/pin, and capacity debit atomically. Only
that committed materialization can reach the ordinary asynchronous provider
path. Independent pools therefore progress concurrently, while a busy pool
cannot delay PostgreSQL admission or another pool's lane. Receipt acceptance
proves a durable pre-provider intent, not provider completion or readiness;
Kubernetes scheduling, image pulls, setup, model loading, and executor capacity
retain their real latencies.

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
- PHX fill uses the existing lowest Kueue WorkloadPriorityClass, `be-lt` at
  value 11, while the worker Pod independently uses the server-owned
  -1000/`Never` PriorityClass; SkyPilot does not manufacture a lower Kueue
  class or alter research queue policy;
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

Policy-bundle schema v6 performs no ValidatingAdmissionPolicy reads and retains
the exact controller/config, Pod webhooks, TAS feature gates, priority, flavor,
immutable projection, and per-Pod lifecycle proofs. It additionally closes the
fleet-side PHX inventory over zero explicit Cohort objects and all seven exact
ClusterQueues, their namespace selectors, queueing/fungibility/fair-sharing
settings, preemption tuples, and complete quota profiles. It proves that every
queue remains in Simone's implicit flat `shared-pool`, both fill queues have
zero nominal quota, and research nominal ownership sums to the physical ceiling
exactly. It separately proves the existing `be-lt` WorkloadPriorityClass at 11
and the server-owned worker Pod PriorityClass at -1000/`Never`; their numeric
values need not match, and the existence of a Pod PriorityClass named `be-lt`
is neither forbidden nor used as worker authority. Schema v5's removal of admission-policy authority
advanced provider inventory from `provider/v3` to `provider/v4`; v6 leaves
that provider domain unchanged and advances the changed fleet domain from
`fleet/v4` to `fleet/v5`. `boltz-l4-fleet` creates direct core/v1 Pods and has
no KubeRay, HPTO, or shared research-policy runtime dependency. Platform may
evolve those unrelated defenses independently without changing fleet
authority.

Schema v6 obtains the Cohort and ClusterQueue facts from exactly two bounded,
paginated, cluster-wide LISTs made by the per-spoke audit role. Those LIST
payloads are the sole source for both the configured-object shape checks and
membership closure; there are no per-name Cohort or ClusterQueue GETs. Every
reviewed object must be present and retain its exact spec. No unexpected
ClusterQueue may name the governed implicit `shared-pool`. The expected
explicit Cohort inventory for that governed name is empty: an explicit
`shared-pool` Cohort or an unexpected Cohort whose parent is `shared-pool`
fails closed. This closes every foreign edge while allowing unrelated roots
and subtrees elsewhere in the shared cluster. Duplicate or missing names,
malformed membership specs,
invalid or repeated continuation tokens, list errors, and pagination-cap
exhaustion are indeterminate and therefore cannot publish or renew authority.

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
| Kueue admission | absent; east remains ordinary Pod scheduling | LocalQueue `be` -> ClusterQueue `skypilot-be`; existing WorkloadPriorityClass `be-lt` at 11; exact implicit flat `shared-pool` and seven-queue inventory |
| Pod PriorityClass (spoke-module-owned) | `rescluster-k8s-prod-east1-preemptible-inference-low`, value -1000, `Never` | same |
| Scheduler / topology authority | exact `gpu-binpack-scheduler` Deployment | Kubernetes `default-scheduler`; Kueue v0.19 TAS owns admission topology and no custom scheduler Deployment is permitted |
| GPU resource | `nvidia.com/gpu` | `nvidia.com/gpu` |
| Exact worker accelerator scheduling | `A100`: `nvidia.com/gpu.product=NVIDIA-A100-SXM4-40GB`, `nvidia.com/gpu`; `A100-80GB`: `nvidia.com/gpu.product=NVIDIA-A100-SXM4-80GB`, `nvidia.com/gpu` | `H200`: `nvidia.com/gpu.product=NVIDIA-H200`, `nvidia.com/gpu` |

PHX retains the implicit flat Cohort name `shared-pool`; there is no Cohort
object or hierarchy. ClusterQueues `skypilot-be`, `skypilot-wa`,
`hyperpod-ns-research-clusterqueue`, `research-ha`, `research-ma`,
`research-wa`, and `research-be` retain the exact specs introduced by Simone's
Platform PRs #8407 and #8517. The two SkyPilot queues have zero nominal quota
and use `Never`/`LowerPriority`/`LowerPriority` for borrow/reclaim/within-queue
preemption. The legacy queue retains
`LowerPriority`/`Any`/`Never`; the four class queues retain
`Never`/`LowerPriority`/`LowerPriority`. The MA
and WA nominal profiles sum to the physical 512 H200 / 12100 CPU / 120Ti memory
/ 2048 EFA ceiling (plus the exact m6i CPU/memory atoms). East
has no Kueue admission pair and must not be forced through a nonexistent
queue. Bundle schema v6 retains one nullable `kueue_admission` object per fleet
context and matching nullable `kueue_enforcement` object per provider context.

This choice preserves a real platform boundary rather than claiming a stronger
one: Kueue `LowerPriority` reclaim does not select an equal-priority `be-lt`
Workload as its victim. The independently injected -1000 Pod priority keeps an
admitted research Pod above a fill Pod at kube-scheduler, while SkyPilot's
zero-nominal grant is revoked when fresh provider capacity is no longer free.
If the existing Kueue contract later needs stronger equal-tranche reclaim, that
is a separately reviewed research-scheduling change owned in boltz-platform;
SkyPilot must not synthesize it with a new priority or Cohort hierarchy.

Both objects must be null or both non-null. Null selects the exact
custom-scheduler reclaim authority and performs no Kueue reads; the typed
projection must also carry `KUBERNETES_SCHEDULER`, null queue identities, and
the reviewed scheduler name. It does not silently downgrade reclaim to Pod
priority. The PHX pair binds `be`, `skypilot-be`,
`be-lt`, the absence of an explicit governed Cohort object, and all seven
ClusterQueues as one
closed contract. Schema v6 retains schema v5's nullable provider
custom-scheduler Deployment, exact Kueue TAS feature gates, and ResourceFlavor
topology-name fields. Fleet `scheduler_name` remains a required string because
it is part of the immutable worker projection; it is `default-scheduler` for
PHX and the custom deployment name for east.
`ResourceFlavor.spec.topologyName` is exact provider inventory, not an
authority discriminator: a provider-owned flavor may retain that field while
an inference namespace remains outside Kueue. The nullable admission and
enforcement pair plus the scheduler contract select the sole placement path.
Schema v6 excludes the redundant ValidatingAdmissionPolicy snapshot from that
enforcement object; strict Pod preparation and synchronous/fresh lifecycle
attestation remain the sole per-Pod runtime admission proof.
The policy proves the exact LocalQueue target, complete Cohort inventory, and
all seven current Active ClusterQueues when the pair is non-null, plus flat
membership,
namespace selectors, complete flavor/resource quotas,
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
instance selector and topology as the complete allowed `spec`: unmodeled spec
fields and extra `nodeLabels` are nonconformance. Node inventory is listed with
all labels in that exact selector, rather than one representative label, and
the attestor cross-binds the complete selector to the GPU product label and
`nvidia.com/gpu` capacity on the current physical cluster's Nodes. It requires
at least one non-deleting Node matching the complete selector for every
reviewed flavor and rejects every matching non-deleting Node whose product or
GPU capacity differs. Node readiness and allocatable occupancy remain
physical-observation inputs, so a temporarily initializing Node is not
misclassified as policy drift. The inference namespace UID and physical
cluster UID are immutable inventory; replace either only by shipping a new
bundle and normal fix-forward reauthorization.

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

Activation runs both provider domains for both contexts concurrently under its
caller-owned bounded deadline. Static identity, pool-key, projection,
accelerator, and admission mismatches fail before provider I/O. Raw provider
payloads and credential material never enter errors or proof output. Activation
is a one-way transition operation; it is not the recurring launch observation
path.

#### Independently renewed provider receipts

The steady-state provider observation owner is one deployment runtime daemon
under the API server's PostgreSQL singleton, not an API request, service
controller, broker critical section, launch handler, or provider checkpoint.
It stays active while Serve controller recovery is held and while individual
service controllers update, stop, or restart. Each attempt executes inside the
existing disposable guardian/warden process boundary. A provider SDK call
that ignores its thread deadline is killed and its complete process family is
proven absent before another attempt begins. The runtime daemon keeps one
finite executor across bounded event ticks; returning after each tick preserves
the runtime framework's log rotation, annotation cleanup, timeline flush, and
memory release. Non-PostgreSQL deployments skip the daemon. A PostgreSQL
deployment with no policy entry point also skips it; one or more installed
entries select it, and discovery uncertainty selects it fail closed. Exact
uniqueness and identity are enforced inside every active child tick. Runtime-
daemon selection is once per controller leadership term and the installed
package set is immutable for that term. Installing the policy or activating a
previously policy-free deployment therefore requires a normal controller
rollout, which reevaluates selection before activation. An already-active gate
whose package is missing cannot renew; its receipts expire and every consumer
denies authorization. No
process-local cache, thread identity, child result, EFS file, or per-service
lifecycle is authority. Every selected tick rechecks the current reconciliation
gate and exact installed policy identity inside the child, then renews every
context in the immutable deployment bundle when the gate is active.
Publication and negative invalidation take no row lock on the protocol
singleton. Immediately before their exact proof-row DML they evaluate the live
generation and policy identity in a fresh READ COMMITTED statement snapshot.
This is an eligibility predicate, not a linearization fence: a rotation
committed after that statement may allow old-generation DML to commit. Safety
comes from the generation/identity/context-qualified proof row and from every
`get_fresh`, admission-readiness check, and terminal reference guard
independently joining it to the currently live gate. Thus a late positive is
inert, and an old-generation delete cannot touch a successor row. A rotation
committed before the post-read revalidation rejects publication; a rotation
committed after revalidation may leave only inert old-generation history. The
loop is supervised; an ambiguous process-family lifetime
poisons that owner rather than allowing overlapping provider readers. In
particular, cancellation without an authenticated drain result constructs a
typed boundary failure and permanently poisons the single executor lane. The
executor has no poison callback that could strand its boundary monitor; that
monitor remains free to publish a typed failure and close local records
independently. The deployment daemon's main event owner parks without
returning, retains the same poisoned executor, and leaves its supervisor
awaiting the same process under the retained PostgreSQL singleton session. It
never exits into the generic daemon restart loop. The runtime-daemon metadata
also marks this owner fail-stop: after its child process has been admitted, any
unexpected exit, SIGKILL/OOM, wait error, or drain error parks the supervisor
coroutine under the same distributed singleton instead of spawning a same-Pod
successor. Only a pre-spawn failure may use ordinary retry. A direct daemon
restart is not a supported operator recovery because its process group excludes
the inner warden and handler sessions and cannot prove their absence. Recovery
requires cancellation of the complete controller owner and recycling its
Pod/container PID namespace. Until then receipts age out and claim/launch
authorization fails closed; the critical daemon/supervisor log names that
required recovery explicitly.

Loss of the singleton's PostgreSQL session or complete controller-Pod ownership
is different: the database may release authority before the old process
observes local loss. Cross-owner safety deliberately does not assume physical
zero overlap. Each context's nonblocking transaction advisory lock elects at
most one publish-capable transaction. If session loss releases that lock while
an old provider SDK call is still returning, a successor may perform a
duplicate external read; those reads are side-effect-free, and publication
revalidates the exact live gate and policy identity, so the obsolete process
cannot publish usable authority: loss of its advisory transaction prevents
commit, while a gate rotation either rejects revalidation or leaves only an
inert old-generation row. `test_lost_advisory_session_cannot_publish` and
`test_lost_leader_fails_wave_and_later_call_recovers` own this crash/session-
loss contract; the parked ambiguity tests own the no-successor path while the
daemon itself remains alive. Lock-convoy tests additionally hold the protocol
row for update and require positive publication and negative invalidation to
complete promptly, while gate-rotation tests prove late old-generation writes
are inert. Receipt renewal does not create a second
service-controller wake path: service pollers retain their one fixed-rate
observation loop, and a cold proof withholds admission until the next ordinary
tick.

Three time domains are deliberately separate:

- the fast launch/claim receipt read has a two-second absolute deadline,
  started inside the final isolated handler after process bootstrap, and
  performs PostgreSQL I/O only;
- one elected provider refresh has its own eight-second timeout; and
- a positive receipt has a 30-second maximum age on the PostgreSQL clock.

The observer targets a three-second fixed rate between round starts and
forcibly refreshes every context in every round; provider-operation time is
deducted from the next delay, and an overrun yields without adding another
three-second gap. It never lets a still-fresh cached context skip behind a
slower peer. East and PHX provider proofs start concurrently, use independent
context transactions/receipts/cancellation events, and share only the one
authenticated process-family survivor boundary. A successful publication must
retain the existing 20-second renewal handoff reserve. An identical proof keeps
its nonce but advances `completed_at`; a losing election waiter therefore
requires a database completion strictly newer than the row it first observed.
The same newer-completion rule applies when another owner commits between the
contender's first READ COMMITTED read and advisory-lock acquisition.

The parent starts one absolute ten-second containment deadline before process
admission. A successful round must complete startup, provider work, result
transport, guardian reap, and executor-lane release inside it, without
extending the child's eight-second provider/DB deadline. The strict 30-second
provider-fact lifetime and five-second consumer reserve leave a 25-second
publication horizon. Conservatively, an earlier publication may occur at the
start of one otherwise successful ten-second boundary; after the three-second
daemon interval, its successor may consume another complete ten-second
boundary. One second remains as the monitored daemon/OS scheduling allowance:
`2 * 10 + 3 + 1 = 24 < 25`. Import-time validation enforces the configured
budgets, while the dark production horizon verifies that the operational
scheduling allowance actually holds. This gives the measured
3.076--4.336-second full-fleet operation normal tail-latency headroom without a
retry, provider read on the launch path, second renewal owner, or longer-lived
authority. Every raw Kubernetes RPC, Kueue version fallback/page, and AWS Pod
Identity page derives a fresh at-most-one-second socket timeout from the same
absolute provider deadline. Kubernetes uses an explicit `(connect, read)`
tuple and disables generated-client retries; a float or default retry policy
would silently escape this contract. A failed
boundary plus its separate drain can exceed the remaining receipt lifetime;
the design does not promise uninterrupted validity across provider failure.
Existing evidence expires and authorization fails closed until a later
successful renewal, or the parked-owner recovery above if family absence is
ambiguous.
Launch and claim reads require at least five seconds remaining. A freshly minted launch
authorization has its own five-second handoff age; it does not pretend that the
older provider observation completed at authorization time. The terminal
database predicate independently enforces both ages.

Claim-set authorization consumes the same fresh exact receipt for every
requested context before a claim or grant can become authoritative. A cold or
restarted observer therefore withholds admission until it has published a
positive observation; it cannot commit a wave of intents that are guaranteed
to fail at the provider boundary. The controller poller owns one finite,
single-lane `DisposableExecutor` for the complete claim authorization call.
The parent waits for the authenticated drained result and for release of that
same lane before another claim can begin. A timeout cancels then drains; an
unproven family or an unreleased lane poisons the executor, synchronously
invokes the controller's existing fail-stop callback, and propagates past the
broker and poller broad failure handlers. The returned authorization is
revalidated in the parent before the broker lock and PostgreSQL claim commit.
The launch path instead uses its already-existing API-request disposable
boundary; it does not nest another executor. Both paths read receipts only and
start the two-second database deadline in the final handler, so spawn/import
latency cannot silently consume the database operation horizon. Launch
authorization validates its exact
service, pool, projection, accelerator, and immutable admission scope locally,
then performs only the bounded PostgreSQL receipt read. AWS, EKS, Kubernetes,
and Kueue calls never execute on the launch path. Repeated provider checkpoints
re-read durable evidence and terminally revalidate it; they do not initiate a
refresh.

Renewal is deliberately outside launch/request recovery. It reads and updates
only the exact context-wide receipt and does not drive a service poller. It
does not enumerate Workloads, infer authority from a Pod, create or
delete replica/request rows, claim a queued launch, or rewrite a RUNNING launch
request. Current committed-intent PENDING rows therefore remain owned by the
normal PostgreSQL request and recovery graph.

Each disposable renewal starts its absolute eight-second provider/DB deadline
inside the final handler, after process startup and invocation deserialization.
The parent independently starts its ten-second deadline before submitting the
guardian and admits no handler after it. Success requires the authenticated
process-family drain, guardian reap, and executor-lane release before that same
deadline; this containment budget does not extend provider publication
authority or receipt freshness. At eight seconds of provider work, or ten
seconds of total boundary life, the parent requests cancellation and retains
the existing separate
ten-second drain-proof window. Failure to prove the family absent still poisons
the renewal lane and fails the controller closed.

Serve054's existing PostgreSQL-only
`serve_reserved_fill_reclaim_provider_proofs` table remains the sole receipt
store. A fresh random 256-bit `receipt_nonce` is its primary key. The unique
exact-authority tuple is reconciliation-gate generation, all three immutable
policy identity fields, and Kubernetes context. Each row additionally carries
only proof schema version, one safe JSONB object containing the exact `aws` and
`kubernetes` summaries, its canonical SHA-256, and database-clock
`completed_at`. There is no persisted expiry or second observer table;
freshness is derived from the database clock.

An identical positive renewal retains the nonce and advances `completed_at`,
so concurrent launch references survive normal refresh. A positive observation
whose schema, canonical digest, or JSONB content changed atomically rotates the
nonce, immediately revoking older references. A provider response that
successfully proves nonconformance atomically invalidates the exact-authority
row before returning failure. Both the per-context provider-domain fanout and
the outer all-context renewal fanout inspect completed work as it arrives. One
typed complete negative dominates and returns its committed invalidation
immediately even when a peer domain or context SDK call never returns; generic
failures remain pending evidence so a later completed negative still wins. The
enclosing renewal boundary then cancels, kills, and proves absence of that peer
family. An
indeterminate refresh -- timeout, transport
failure, cancellation, lost database acknowledgement, or malformed provider
response -- does not rewrite or delete a previously committed positive fact;
that fact may remain usable only until its bounded 30-second expiry. Provider
adapters surface observed nonconformance as a typed result distinct from
indeterminate failure. Missing, malformed, stale, mismatched, or explicitly
invalidated evidence fails closed, with no launch-time fallback proof.

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

#### Superseded launch-driven Serve054 behavior

The following paragraphs record the deployed Serve054 implementation and its
earlier qualification rationale. They are retained as incident history, not as
the steady-state contract. The independently renewed receipt design above
replaces launch-time election, launch-time provider calls, five-second
provider-fact freshness, and launch-handler waiter polling.

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
or published to a caller only while at least two seconds remain before its
five-second expiry. The same public minimum-remaining value is rechecked after
policy decoding at the final policy-to-provider handoff. The database read maps
completion conservatively to the caller's monotonic clock, and the live reserve
check runs after payload validation, transaction commit/rollback, and physical
`NullPool` connection close at the actual return boundary. If those steps
consume the reserve, the caller re-enters election under its unchanged absolute
deadline and never sees the near-expiry receipt. The terminal PostgreSQL guard
still checks the full maximum age and fails closed if the bounded entry budget
is consumed under actual contention.

The receipt-owned connection path is PostgreSQL-only, `NullPool`, and
instrumented under the bounded `reserved-fill-reclaim-proof` metric label. It
uses the distinct transaction-local `skypilot-reclaim-proof-owner` database
phase tag only while it owns the advisory transaction, making retained-owner
observations deterministic without changing pool identity or metric cardinality.
It
retains URL connection options, performs no generic database retry, permits
one libpq connection attempt with a one-second connect timeout, applies
200-millisecond server statement/lock limits and client socket send/receive
limits, and sets `idle_in_transaction_session_timeout` to 9 seconds, explicitly
above the eight-second provider-refresh horizon while bounding a wedged provider-held
transaction. These limits reduce pressure and make normal database faults
prompt; they are not the absolute survivor boundary because synchronous DNS
and a lost commit response cannot be proven bounded by libpq.
The existing per-invocation `DisposableExecutor` is that boundary. At the
eight-second provider deadline the policy sets cancellation, reports failure
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
the older reference. Serve056 does not hold the mutable shared fleet-gate guard
across a committed provider effect and does not reread today's allocation,
claim edge, observation, or gate. Instead, the installed policy must freshly
attest the exact frozen policy identity, gate generation, scope, projection,
and no-paid facts carried by the committed intent. The terminal transaction
validates that exact short-lived receipt together with the immutable committed
graph and current service/association authority. A changed installed policy
identity, context mismatch, changed proof, receipt ABA, expiry, deletion,
malformed payload, non-`READ COMMITTED` transaction, database loss, current
service/elected-version revocation, or physical-UID retarget rejects the
launch. A later mutable planner generation alone does not retroactively revoke
the accepted intent. Distinct context proofs remain parallel.

For `RESERVED_FILL/v1`, that terminal validation participates in one short
effect-claim epoch with the exact live request claim and association. All
PostgreSQL transactions and advisory-lock sessions close before the physical
UID fence or provider call begins. Committing that claim linearizes the bounded
effect before any later service/version/controller transition. Such a
transition may proceed concurrently, but cannot replay, settle, or replace the
old request until its exact guardian-authored process-family quiescence receipt
exists. When the provider call returns, a new short transaction revalidates the
same service/lifecycle/controller/intent/association/generation and request
claim. If it changed, provider success is not published: the execution remains
post-effect ambiguous and the existing exact-presence/absence reconciler owns
the outcome. Generic paid and system-recovery profiles retain their existing
shared launch-authority guard in this feature; the lock-free boundary is
deliberately restricted to the scalar-linked reserved-fill profile.

This deliberately reuses completed evidence inside, but never beyond, the
same five-second horizon already accepted for one launch ticket. It does not
mint a fill-plan capability. Durable intents can live for roughly 180 seconds
and can be delayed by per-pool leasing, API queueing, retries, and Kueue; a
five-second pre-fanout capability would routinely expire, while extending it
to the intent horizon would weaken external IAM/Kueue/scheduler-drift safety.
Refreshing the shared receipt at the terminal boundary preserves that safety
without a process-local second authority path.

#### Deployment preflight and policy surface

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

Historical policy-bring-up record (non-executable): that rollout was fix-
forward and applied and attested IAM, namespace/service-account, queue,
priority, Kueue configuration, and server projection first; it removed the
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
broker and PostgreSQL row locks are acquired. Claim replacement, allocation,
and precommit admission continue to lock and revalidate the current gate,
identity, normalized edges, version projection, observation, and digests. A
reserved-fill request carries the `RESERVED_FILL/v1` bound profile and exact
authorization references; it does not carry ordinary defaults or create a
second association.

After Serve056 commit, each built-in Kubernetes provider-mutation attempt
enters the exact request/association effect-claim epoch, reconstructs the frozen
scope and policy identity from the durable launch fence and immutable worker
projection, and asks the installed policy for a fresh bounded authorization
against that frozen gate generation. The terminal PostgreSQL transaction then
validates the exact receipt, scalar-linked `COMMITTED` intent graph,
service/lifecycle/elected-version/controller authority, execution generation,
profile, projection, no-paid shape, record UUID, and physical context, commits,
and closes. Only then does the nested physical-cluster-UID fence yield to
provider mutation, with no PostgreSQL transaction, session advisory lock, or
connection retained as authority across Kubernetes I/O.
The canonical committed-effect order is therefore association effect
claim, fresh context-proof election/read, terminal committed-graph and receipt
validation plus commit/connection close, physical-UID fence, provider effect,
and fresh post-effect validation. There is no current mutable fleet guard,
claim-edge read, allocation read, or observation read in this postcommit path.

The policy operation has a new absolute five-second monotonic deadline for
each bounded effect and is cancellation-aware; a late or near-expiry result is
rejected before mutation. Passive Kubernetes/Kueue waits and retry sleeps are
outside the guard. A restarted executor with a missing plugin, a differently
identified installed policy, stale service version or projection, partial
fence, invalid receipt, or retargeted physical cluster cannot launch. A
successor planner generation does not require an obsolete grant to become
current and does not create another provider path.

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
rely on an outer guard around the opaque bulk-provision call. Before the first
durable Pod receipt, every normal, AppArmor-retry, and 409 replacement create
attempt reacquires separately. Force-remove and rejected-identity delete/read
attempts also reacquire separately, with all retry sleeps outside the guard.
Once PostgreSQL binds an exact Pod namespace/name/UID, every replay loads that
identity before Kubernetes provisioning and becomes adoption-only. It reads
and verifies that exact object before any Pod-create boundary; an absent,
same-name replacement, terminating, `Failed`, `Succeeded`, or 409-conflicting
object performs zero create, patch, finalizer removal, or delete and returns the
provider-present fence to canonical teardown/reconciliation. Every successful
create response is checked
for the exact queue, WorkloadPriorityClass, admission scheduling gate,
namespace, service account, Pod priority, and accelerator shape before that
guarded call returns. Existing Pods are reattested against their current Kueue
lifecycle state: an admitted Pod may have had the gate removed, but must retain
the exact managed/queue/WorkloadPriorityClass labels, Kueue's managed finalizer,
`podset=role-hash`, and the exact LocalQueue/ClusterQueue outputs bound at
preflight. `AssignQueueLabelsForPods` is therefore a deployment prerequisite.
A label-only Pod without either the create-response gate or the complete
post-admission binding is rejected and deleted under fresh authority only
before a durable Pod receipt exists. After that receipt, the same rejection is
observation-only and canonical reconciliation retains exact cleanup authority.

Passive scheduling and readiness waits on this reserved-fill built-in
Kubernetes path never hold a PostgreSQL transaction, session, or per-service or
fleet-wide advisory guard. After the wait, every Pod is fresh-read and its
complete admitted identity reattested in one new short effect-claim epoch
before provisioning can return. The fresh object must
remain `Running` and retain the exact UID captured by the all-containers-
running observation; same-name replacement and still-gated objects fail
closed. In
particular, a correctly Kueue-pending Pod
with `provision_timeout: -1` may wait indefinitely without blocking a service
version mutation, controller takeover, or reclaim-policy reauthorization.
Any later provider mutation or retry must enter a fresh short claim
transaction, obtain a fresh policy authorization, and revalidate the durable
fence before I/O without retaining PostgreSQL authority across that I/O. If
authority changed while a Pod was pending, the stale request cannot perform
another create or destructive retry; durable controller reconciliation owns
eventual cleanup of the old replica. A transition that commits after an exact
effect claim cannot declare that execution absent or replay it until the
guardian receipt proves quiescence. A failure after entering this instrumented
path does not run
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
transient server-owned SHA256 expectation and rejects any merged change to that
producer. Kubernetes SSH authentication is the one later trusted rendering
step: it materializes the server key in the canonical bootstrap placeholder.
Immediately after that step, while the complete projection remains available,
the renderer requires every projected node type to share one exact
authenticated producer, reasserts the projection, and persists that final
digest through the provisioner boundary. No untrusted merge occurs between
authentication and this final freeze. The finalized Pod, create response,
adoption read, admitted read, and final fresh read must all reproduce the final
digest; an injected `postStart`, changed pre-stop lifecycle, alternate command,
or replacement script therefore cannot forge the readiness marker. Both probes
require the contents of
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

Protocol v5 is historical and cleanup-only because releases 1.1.1440 and
1.1.1442 persisted the same discriminator while rendering different bootstrap
environments. Their cohort-4/cohort-5 lineage remains sufficient for bounded
settlement, but neither representation may authorize fresh provider work.

Protocol v6 retains the complete v4 readiness contract and uniquely binds
large SkyPilot/uv bootstrap writes to a memory-scratch worker's already
authenticated `/tmp`. The three exact server-owned paths are present both as
literal Pod env, which every new `kubectl exec` process inherits, and as
post-`runcmd` exports in the canonical bootstrap. Their Pod env entries join
command, script, lifecycle, and the unique v6 marker in the final bootstrap
SHA; every existing provider attestation point therefore rejects mutation. V4
bootstrap hashes remain unchanged, and v5 concrete bootstrap identities remain
recognizable for audit without granting replay. Protocol v6 rotates the
generic non-pool capability cohort to epoch 6: a complete epoch-6 cohort may
write, epoch 5 is settlement-only, and epoch 4 remains terminal historical
evidence rather than an N-2 compatibility claim. This extension changes no
projection payload shape, database schema, Kueue object, task resource, or
platform configuration.

Protocol v7 is the sole fresh-writer successor to v6. The +30 eight-card refill
proved v6 can durably assign released capacity, but also exposed that its
background apt/SSH wrapper inherits `set -e`: the 90-second wrapper timeout
killed `apt-get` while child dpkg PID 2654 retained the package lock, leaving
the transaction interrupted. Bootstrap then skipped OpenSSH, failed the
missing-configuration edit, and exited before publishing either the success or
failure marker. PID 1 then waited forever even though the independent
runtime/Ray bootstrap completed. This is an immutable worker-contract defect,
not a reason to add a scheduler or infrastructure fallback.

V7 uses a unique discriminator and bootstrap marker, and the existing full-
script digest continues to bind command, args, lifecycle/environment, and
bootstrap content. Its apt/SSH phase must:

- never wrap package installation, `dpkg --configure -a`, or
  `apt-get -f install` in a hard shell timeout; apt's Acquire HTTP/HTTPS
  deadlines bound network operations, while the existing 30-minute whole-Pod
  startup deadline is the safe boundary for the ephemeral package transaction;
- after an ordinary nonzero install result, run `dpkg --configure -a` before
  `apt-get -f install` and only then retry;
- emit exactly one terminal success or typed failure marker on every exit,
  including package failure, absent `sshd_config`, and edit/start failure under
  errexit;
- make the PID-1 wait scan every step's failure marker on every poll, so a
  later runtime/env failure is consumed even while apt remains unresolved, and
  exit with a typed provider-present failure instead of waiting indefinitely;
- publish success only after OpenSSH installation, required configuration,
  and the complete authenticated readiness contract pass.

Protocol v6 becomes settlement/cleanup-only through the existing historical
validator; it cannot authorize a fresh provider effect after cohort 7 is
activated. Editing v6's script in place is forbidden because the same
discriminator would then name two concrete bootstrap identities, repeating the
v5 collision. Focused tests must prove package install and both repair commands
have no hard timeout, Acquire network deadlines remain, dpkg repair precedes
apt repair and retry, and a forced inner failure publishes a terminal failure
marker with bounded PID-1 exit while the success marker cannot publish. A
later-step failure must terminate PID 1 even when an earlier completion marker
is intentionally absent. They must also verify the v7 digest/marker is unique
and prove v6 rows remain
recoverable but cannot write. The production rollout must use one homogeneous
SkyPilot Helm cohort,
cleanly update/recreate the service onto the immutable v7 projection,
reauthorize that exact writer inventory, and repeat assignment, readiness,
zero-paid, telemetry, takeover, node-pressure, and error-signature checks. It
must not alter Kueue or any non-SkyPilot infrastructure object.

The memory `emptyDir` size is a hard per-Pod limit, not a reservation; actual
tmpfs pages count as Pod memory. Moving the approximately 598 MB runtime, 649
MB uv cache, and 95 MB uv-managed Python tree there trades unbounded duplicated
nodefs consumption for bounded, reclaimable Pod memory and leaves only the
approximately 55 MB uv executable beneath `$HOME/.local/bin`. Bootstrap does
not delete the uv cache because later exact-wheel setup consumes it; early
deletion would increase retry work without lowering concurrent-install peak
memory. The production gate therefore observes Pod/node memory and pressure at
one-Pod and full-wave scale.

Lifecycle 89 also exposed a control-plane execution ceiling after PostgreSQL
had already authorized durable zero-cost intents. A 48 GiB executor limit with
an 8 GiB request made the chart's downward API publish only eight long-request
workers per executor. Held revisions 550 and 551 staged the SkyPilot-only
resource correction: API requests are 56 GiB with 110 GiB limits, and three
executors now request and limit 48 GiB with four CPUs and
`SKYPILOT_LONG_WORKER_CPU_MULTIPLIER=16`. The intended result is 64 long workers
per executor and 192 aggregate slots under the existing Serve launch ceiling.
This is only an execution bound after PostgreSQL admission; it cannot grant a
GPU, bypass Kueue, or introduce a second fill path. Pre-activation dark
verification covered the exact downward-API input, runtime/startup counts,
node scheduling headroom, cgroup `pids.current`/`pids.max`, memory/OOM and
`PIDPressure`, PostgreSQL connections, and bounded queue latency. Helm revision
572 now runs the exact seven-writer topology with zero restarts; the live node
readback found no DiskPressure, MemoryPressure, or PIDPressure. A later
steady-state improvement may publish executor capacity dynamically, but must
preserve the single PostgreSQL-authorized launch path.

The two-step order was required because simultaneous API and executor surge
Pods could exceed the only initially roomy hub node even though the final
aggregate reservation fits. At the 2026-08-22 21:07 UTC pre-rollout
observation, each API Pod used about 3.8 GiB; the 56 GiB request left about 6.5
GiB of request headroom for the first topology-constrained executor
replacement. The target process topology is 64 long plus 68 short workers per
executor before launch subprocesses. The PIDs, memory/OOM, PostgreSQL-
connection, and queue-latency checks were activation qualification rather than
new capacity authority and are not an open post-rollout gate.

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
association/request effect claim, fresh current-service validation, fresh
provider proof against the frozen policy scope, scalar-linked committed-intent
validation, and exact physical-cluster UID fence. The database claim commits
and closes before each external call; a fresh transaction validates the result
afterward. They do not reacquire mutable claim,
allocation, observation, or fleet-gate authority. Passive Kueue scheduling and
readiness waits remain outside every guard. A missing guard fails closed;
terminal cursor restoration and other best-effort reporting cannot replace the
typed materialized result. Ordinary bound requests retain their existing
authority path.

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
4. ordinary runtime supervisors restart an unexpectedly exited subprocess with
   bounded backoff. The provider-proof renewer is the explicit exception: its
   `fail_stop_on_unexpected_exit` metadata parks the supervisor and retains the
   outer singleton after any post-admission exit or supervisor/drain
   uncertainty, because group absence cannot prove its separate-session
   executor family absent. Cancellation from complete controller-owner loss is
   still allowed. For every daemon, cancellation sends `SIGTERM` to the owned
   process group, escalates to `SIGKILL` after a bound, reaps the exact child,
   and does not return until the process group no longer exists;
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

Historical worker projection bytes remain readable only through their bounded
decoders and terminal-cleanup classifiers. They cannot participate in a new
claim, allocation, fill admission, replica/request admission or replay,
`NOT_STARTED -> PROVIDER_IO` transition, or provider materialization. Exact
deletion of an already-owned provider object remains allowed only when the
separate generic capability fence also authorizes settlement; projection
decodability alone is never authority. After all active service versions are
committed with protocol v7 and production has remained `SEQUENCED_ACTIVE`
through the documented cleanup gate, stacked cleanup PR #1619 must remove only
the v1-v4 decoders and transition tests. It must preserve exact v5/v6
projection decoding for the N-2/N-1 read window. Generic operational
settlement remains N/N-1; cohort-5 N-2 may only retire an already canonical
terminal `PROJECTED`/`ABSENT` graph through the separate terminal-only
predicate described above. New writes always use v7; no
compatibility setting can create an older projection or let retained cohort 5
or 6 state admit new provider effects.

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
  homogeneous explicit projection protocol v7; sequenced paths require
  non-null typed Kueue admission. Protocol v7 supports both an exact AWS role
  ARN and an explicit null identity contract, and the value is hash-bound and
  exposed to the deployment policy. Protocols v5/v6 remain as exact N-2/N-1
  projection decoders and terminal-cleanup identity classifiers. Generic
  operational settlement remains N/N-1; v5/cohort-5 N-2 can only retire an
  already terminal, quiescent, pin-released, zero-paid, canonical
  `PROJECTED`/`ABSENT` graph. V1-v4 removal remains pending the retargeted
  cleanup PR #1619.
- API capability 77 exposes the placement-projection capability surface and
  the exact advertised current discriminator is v7; allocation-map schema 5
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
  described above are deployed. The canonical-birth correction is now
  exercised historically by lifecycle 84/version 1 and lifecycle 85/version 1:
  a fresh eligible service commits generic `bound`, `DURABLE_PROJECTED`,
  `DURABLE_FEED`, and `DURABLE_INTENT` authority at epoch 1 under one
  incarnation, then verifies it before child spawn. Both lifecycles were
  normally purged; lifecycle 93 supplied the historical proven backfill and
  production-proven PR-#1678 cross-Pod takeover before supported teardown.
  Final acceptance requires v7 cleanup, fresh recreation, and a new scheduler-
  authorized refill/audit.
- Deployed PR #1667 binds protocol-v2 to one frozen, already-launchable request
  resource and an absent configured admin-policy mode at both controller
  preparation and executor replay. Execution reconstructs `best_resources`
  from that singleton before cluster-existence/initial optimization, and the
  provisioner returns an exact-candidate failure before retry optimization or
  failover. This contract is deployed in release `1.1.1442`.
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
service-authority promotions and full-backfill configuration were exercised by
lifecycle 85. PRs #1667/#1671, normal lifecycle-91 purge, and clean
lifecycle-93 recreation are complete; full backfill and fresh live request
readback are proven exactly as recorded in the phase table and gates.

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

The audit also proved that research-over-fill reclaim is a deployment
prerequisite rather than a Pod-priority property. A read-only audit on
2026-08-13 found that both east and PHX lacked the historical `default` /
`skyserve-inference-borrowed` / `skyserve-inference-low` objects. PHX later
adopted the transitional `be` -> `skypilot-be` and `be-ls`/`be-lt` priorities;
east intentionally remains outside Kueue. Both retain the exact Pod
PriorityClass at value -1000 with `preemptionPolicy: Never`, and
`skypilot-pool-sa` is the exact worker service account. The reviewed successor
does not rely on `be-ls` outranking another workload. It proves the unchanged
implicit flat `shared-pool`, both zero-nominal SkyPilot queues, all five
research queues, their complete quotas and exact #8407/#8517 preemption tuples,
and existing `be-lt=11` WorkloadPriorityClass. Research nominal ownership sums
to the 512-GPU physical ceiling. SkyPilot deliberately does not claim or create
a stronger Cohort hierarchy; the worker Pod remains independently lower at
-1000/`Never`.
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
Platform PR #8822 was merged and deployed as platform revision 29 on
2026-08-21. Its readback preserves the exact flat implicit `shared-pool`, zero
explicit Cohort objects, the existing `be-lt=11` WorkloadPriorityClass, and the
list-only audit-role boundary; the PostgreSQL worker projection also names
`be-lt`. Platform PR #8824 subsequently activated the SkyPilot queues without
changing that shared scheduler topology. This qualifies the platform shape
expected by deployed schema v6 and proves the queues are no longer held; it is
not by itself reserved-fill convergence evidence.
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
levels are verified as a deployment preflight/readback. The PHX chart's
supplemental audit ClusterRole is the sole owner of read-only cluster-wide
`list` on Kueue Cohorts and ClusterQueues; the spoke module retains its
existing minimal object-read contract. No permission is granted to the API
writer, controller, or worker identity.
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
| 1d | Generalized binding, demand/route projection, and G1S execution-termination evidence through API014/Serve050 | Merged and deployed. Lifecycle 84/version 1 and lifecycle 85/version 1 both exercised ordinary `bound`, route `DURABLE_PROJECTED`, demand `DURABLE_FEED`, and fill `DURABLE_INTENT` epoch 1. Both are now normally purged; resource-action cleanup remains a separate horizon. |
| 1e | Canonical birth for fresh lifecycle-fenced PostgreSQL non-pool services | Merged in PR #1621 at `bb81d16f1c2d194ec5bf488c1e1d87c8f44ee391` and exercised by lifecycles 84, 85, 91, and 93/version 1. `add_service()` uses the existing Serve047-allowed adjacent update inside the service/version transaction, so PostgreSQL exposes only the final generic bound/route/demand/fill tuple. `_start()` verifies rather than promotes it. No schema, EFS, Helm, Terraform/Terragrunt, provider, pool, or SQLite change is included. Historical backfill, cross-Pod takeover, and stale/quiescence proof passed. Lifecycle 93 is now `FAILED_CLEANUP`; v7 must finalize it before this same canonical birth path creates the fresh service. |
| 2a | Policy-bundle schema v3 plus exact live PHX queue/service-account contract | Merged in PR #1529 and deployed in revision 418; superseded in place by schema v4 because PHX intentionally replaced its custom scheduler with Kueue TAS. |
| 2a.1 | Policy-bundle schema v4, PHX Kueue TAS/default-scheduler contract, exact spoke audit roles, and PostgreSQL-backed server-config transaction | Merged and deployed through release 1.1.1332 / platform configuration; server config is corrected, but version 61 retained the pre-correction PHX scheduler projection. |
| 2a.2 | Isolated audit-role Kubernetes authentication and exact east identity-free inventory | Merged and deployed through revision 429 / release 1.1.1336. East passes. PHX explicitly enables `AssignQueueLabelsForPods=true`; a clean current platform plan is empty. Release `1.1.1429` passed fresh full two-context preflights in 3.076--4.336 seconds. Policy revision `1.1.1430` completed the generation-10 proof on Helm revision 505, including eight renewal samples over 93 seconds with receipt ages of 2--11 seconds. |
| 2a.3 | Global activation scope with per-service duplicate-pool validation | Revision 431 preflight exposed that the deployment policy incorrectly treated two services sharing one broker pool/card as a duplicate claim. The fix groups activation claims by service, retains same-service duplicate rejection, and permits the documented cross-service sharing before one fleet-wide provider attestation. |
| 2a.4 | Remove redundant admission-policy authority from reserved fill | Platform PR #8649 is superseded and must not change the shared KubeRay/HPTO policy for fleet activation. Policy-bundle schema v5 removes ValidatingAdmissionPolicy and binding reads while retaining the stronger exact Kueue controller/webhook and synchronous/fresh Pod lifecycle proof. Activation requires only the SkyPilot fix-forward deployment and a fresh full-fleet preflight; no Terraform or platform Helm change is part of this gate. |
| 2a.5 | Preserve Simone's flat PHX Kueue contract and attest its complete inventory | Platform PR #8822 is merged and deployed as platform revision 29: zero explicit Cohort objects, no new priority class, existing `be-lt=11`, list-only audit access, and PostgreSQL projection updated to `be-lt`. Platform PR #8824 later activated only the SkyPilot queues without changing that topology. The matching SkyPilot schema-v6 correction is merged in PR #1650 and deployed in release `1.1.1422`. Lifecycle 93 proved 75 submitted, admitted, Running, and Ready H200 Workloads plus three separately queued Workloads under this unchanged policy. |
| 2b | New immutable service version with task-owned Kubernetes overrides removed, `min_replicas: 0`, and exact non-null worker projections | Lifecycle 93/version 1 historically proved the canonical no-EFS projection and synchronized 133/133 Ready plus three PHX queued census after normal cleanup of lifecycles 89--91. It is now `FAILED_CLEANUP` after supported teardown. V7 must finish its nine retained rows and recreate the same-name service rather than update that failed lifecycle. |
| 2b.1 | UID-bound base-runtime readiness for canonical projected Kubernetes workers | Merged in PR #1618 at `6ad2407d813d04aed79de2fea62723987ee56670`; fix-forward PR #1655 retains the pre-merge digest check, then freezes and reasserts the exact authenticated producer before restore, hashing, and persistence. PR #1655 is deployed through release `1.1.1429` / Helm revision 502 and inherited by `1.1.1448`. Exact projected Pods remained Ready through the complete stale/quiescence interval, and all 66 PHX nodes were Ready without pressure. Deliberately inducing a failed base bootstrap is source/test-qualified rather than a required production fault injection. Generic Kubernetes and projection v1/v2/v3 remain unchanged. |
| 2b.2 | Protocol-v7/cohort-7 apt/SSH terminal-marker, interrupted-dpkg, and bounded rolling-cleanup correction | **Source complete and focused unit/real-PostgreSQL-qualified; not merged, published, deployed, activated, or production-proven.** V7 is the sole fresh writer and uses a unique marker plus the existing full-script SHA over command/args/lifecycle/environment. It never hard-interrupts apt/dpkg, retains bounded Acquire network deadlines, uses the 30-minute whole-Pod startup deadline as the safe ephemeral transaction boundary, repairs dpkg before apt after ordinary nonzero failure, guarantees a terminal success/failure marker and PID-1 exit, makes v6 settlement/cleanup-only, and forbids an in-place v6 script edit. Its N-1 typed finalizer and marker-gated N-2 takeover retire only canonical terminal absence graphs. Rollout is homogeneous v7 Helm at gate 34, supported lifecycle-93 purge, fresh same-name recreation, full preflight, and exactly one active-to-active generation-35 CAS. No manual row/Pod deletion and no Kueue or infrastructure change are allowed. |
| 2c | P2c provider-independent route leases and safe zero-demand paid retirement (Serve051/API88) | PR #1531 is merged and deployed dark. PR #1532's exact-owner fix is deployed as revision 410 / v1.1.1314. PR #1533's immutable route-contract fix is deployed as revision 411 / v1.1.1315 and removes the shared routing-lock dependency. Later fix-forwards removed the synchronous per-probe receipt-write bottleneck. Current-request telemetry remained fresh through cross-Pod takeover and the stale interval; artificial provider-stall behavior is source/test-qualified. Historical cleanup #1506 is closed/superseded and reserves no head. |
| 2d | P2d grant-before-row per-pool actuation intents (Serve052) | Merged in PR #1537 and deployed. Lifecycle 84/version 1 and lifecycle 85/version 1 both promoted `DURABLE_INTENT` epoch 1 before normal purge. Full busy-lane/no-row and throughput production evidence remains part of phase 2g. |
| 2e | Atomic per-service durable-demand plus durable-actuation promotion | PR #1555 is merged and deployed. Lifecycle 93/version 1 historically exercised the single controller fence, routing linearization lock, and PostgreSQL transaction owning the `DURABLE_FEED`/`DURABLE_INTENT` pair before supported teardown. Draft cleanup PR #1556 removes deprecated separate surfaces and unsupported demand demotion only after the documented horizon. |
| 2f | Promoted capacity-authority controller takeover | PR #1562 is merged and deployed. Revision 502 proved one consolidated-HA recovery with the bound/demand/fill pair preserved and fresh route evidence restored from PostgreSQL; revision 505 then proved takeover of the deployment-owned renewal singleton. Helm revision 572 then forced a true PR-#1678 cross-Pod lifecycle-93 service-controller takeover from `10.30.1.190` / owner epoch 3 to `10.30.0.98` / owner epoch 4 with a new incarnation, while lifecycle, version, service hash, Ready state, endpoint, and replicas remained stable. The complete stale interval and +30 control-plane/HA/error checks also passed. No schema, provider, platform, or storage change was required. |
| 2g | Production full reserved backfill | **Historical primary occupancy, idle cost, rollout, takeover, stale-horizon, and automatic-refill submission gates passed; the fresh v7 recreation proof remains open.** PRs #1675--#1678 are deployed with storage disabled in homogeneous Helm revision 572 / release `1.1.1448`, and gate generation 34 authorized all seven writers. Lifecycle 93 initially reached 133 Ready of 136 reserved units: East 58 scheduler-fit GPUs and PHX 75 Kueue-admitted H200s, with the other three submitted PHX Workloads queued by unchanged Kueue/topology policy and already debited. At +30, SkyPilot immediately durably claimed a fresh eight-card East release, but the new v6 workers then hit the apt/marker bootstrap defect. Supported teardown removed all provider workloads and left lifecycle 93 `FAILED_CLEANUP` with nine `UNKNOWN` rows. Final proof requires homogeneous v7 Helm at gate 34, N-1 finalization, fresh recreation, full preflight, one generation-35 CAS, Ready or typed-terminal convergence for every newly scheduler-authorized unit, and audit acceptance of the final zero-paid census. Paid residual and drain are source- and real-PostgreSQL-test-qualified without deliberately billable production load. Exact completed logical requests are a separate PostgreSQL idempotency/completeness feature. |
| 2h | Atomic reserved-fill replica/request admission | Merged in PR #1626 and deployed on Helm revision 473 / release `1.1.1401`. One atomic-admission module owns the root PostgreSQL transaction and savepoint; the manager only prepares immutable server-local input before it and starts the returned request reducer after commit. Serve055 adds the owner audit tuple, user FK, and retained-row one-shot transition. The deployed pending-first/global-pending/cleanup-unproven accounting correctly avoided duplicate replacement capacity. The remaining postcommit mutable-authority rejection is owned by phase 2i, not by another admission path or infrastructure change. |
| 2i | Serve056 committed reserved-fill provider handoff and cohort rotation | PR #1629 merged at `1642ca2e3` as the scalar-schema precursor; PR #1632 restored adoption and corrective PR #1630 supplied the complete committed-handoff contract. That source is deployed through release `1.1.1410` and inherited by revision 489. Draft cleanup PR #1633 remains gated on the final zero-legacy census and production horizon. No EFS, KubeRay, Terraform/Terragrunt, platform runtime pin, or alternate provider path is added. |
| 2j | Exact protocol-v2 execution capsule and no-failover retry boundary | PR #1667 is merged and deployed in release `1.1.1437`. Under the explicitly absent configured admin-policy mode, the server controller freezes one already-launchable Kubernetes resource in the hashed request, the executor reconstructs `best_resources` from it before cluster-existence/initial optimization, and the provisioner exits an exact-candidate failure before retry optimization or failover. Ordinary launches are unchanged. Lifecycle 93 historically proved clean recreation and full scheduler-authorized occupancy before supported teardown. |
| 3a | Stacked Serve055 owner-transition cleanup after the production horizon | The required `fix/serve-atomic-fill-admission-cleanup` branch adds PostgreSQL-only Serve058 `NOT NULL` owner columns and removes only the application one-shot `NULL` attestation branch, the schema-derived temporary global user-deletion guard, and transition-only observability/tests. The dialect-neutral SQLAlchemy model remains nullable for the separately supported controller-local SQLite/Serve037 path. Serve058 verifies or reinstalls the permanent PostgreSQL owner FK and owner-immutability trigger in the same migration. Draft PR #1660 is stacked on and cross-linked from #1659; it remains blocked on a complete capable cohort, zero `NULL` tuples, no old writers, backups, and the complete stale/HA production horizon. |
| 3b | Stacked Serve056 scalar-`NULL` cleanup bridge removal | Draft PR #1633 is stacked on and cross-linked from #1630, replacing automatically closed draft #1631. It removes only the cleanup-only JSON resolver and its transition tests after zero scalar-`NULL` protocol-v2 replicas, zero unsettled scalar-`NULL` provider-effect associations, and zero scalar-`NULL` cleanup-unproven markers persist through the complete stale/quiescence/provider-reprobe horizon. It cannot merge earlier. Closed PRs #1506/#1510 are not revived. |

Durable acceptance atomically binds rows to the existing asynchronous launch
path through the generic non-pool handler, and
status projects the same allocation/observation evidence used by
reconciliation; neither is a second source of launch authority.

## Deployment, activation, and fix-forward reauthorization

The current executable sequence is the seven-step direct-Helm purge/recreate
rollout in the Decision summary and phase table above. The A/B/C, split-topology, publisher,
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
are proven, the stacked Serve058 cleanup makes both columns `NOT NULL` and
deletes the global deletion guard, application one-shot attestation branch,
and transition-only artifacts. In that steady state, user deletion first
reports the names of owned services and the foreign key serializes the
concurrent delete-versus-service-create race. Serve058 retains or atomically
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

### Historical rollout preconditions (non-executable)

The current v7 executable runbook in the Decision summary supersedes every
A/B/C, publisher, Terraform/Terragrunt, and separately owned Kueue instruction
in this subsection. The steps below are retained only as review history. They
must not be applied to the current fleet and do not authorize changing Simone's
Kueue configuration.

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

Revision 423's RWX/EFS work is superseded historical migration evidence, not
the current topology or a rollback path. At that time, the encrypted,
backup-enabled `skypilot-state-rwx` claim received `.sky` and `.ssh` state and
passed a zero-delta second rsync before the 2/2/2 rollout; the inference route
remained available through cutover. The later fresh role-split control plane
removed that dependency. The live release now has `storage.enabled=false`:
API, controller, and executor Pods use bounded `emptyDir` only for ephemeral
artifacts, PostgreSQL is the sole durable authority for configuration,
service, replica, request, allocation, and recovery state, and no PV, PVC, or
EFS mount is a runtime or correctness prerequisite.

The historical cutover also showed why retained volume ownership is not a
safe fallback: selecting `existingClaim` caused Helm to delete the superseded
chart-owned RWO PVC and its EBS volume before a snapshot or Retain patch could
complete. That old volume and the later RWX copy are not rollback authorities.
Any retained EFS/PV/PVC or ReplicaSet cleanup is hygiene only and must not
reintroduce storage authority. If a future, unrelated persistent-volume
migration is ever proposed, it must first make its source recoverable outside
the release before Helm stops owning the source claim.

Historical note (non-executable): the separately owned Kueue contract was a
different deployment change. It could
proceed through its own reviewed authority only after the east and Phoenix
inference partitions, exact RBAC, server queue configuration, fail-closed
strict Pod lifecycle admission, and the code-owned policy plugin are
implemented and reviewed. It is not smuggled into the runtime-image Helm
upgrade and does not block
deploying the combined image safely at `LEGACY_ACTIVE`; it does block
activation.

### ReplicaInfo v18 expand-and-normalize gate

This subsection is retained historical migration evidence. SkyPilot is
currently test-only and the v7 service is recreated on the latest schema, so
pre-v17 schema replay, retained-row census, and one-shot normalization matrices
are not gates for this rollout. Fresh-head PostgreSQL bootstrap and atomicity,
current fences, exact-current provider-effect rejection, guaranteed N/N-1
rolling operation, and the bounded N-2 terminal cleanup classifier remain
mandatory. This waiver does not permit an old row to be decoded loosely or
deleted without typed authority.

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

The reserved-fill ordering source gate uses a fresh authenticated allocation
with unavailable load-balancer demand. A non-Kueue/East lane must accept its
exact fill deficit concurrently. A Kueue/PHX unresolved domain must accept the
full authenticated batch within the locked free-capacity/ceiling bound; every
intent is an immediate durable debit, and a later replay adds none. Each accepted
case returns and notifies before attempting the demand read and invokes no paid,
target, retirement, scale-up/down, or provider path. A case with no new intent
continues to the unavailable demand read and proves that all paid and
destructive actuation remains blocked. Source inspection must show one
sequenced fill-admission call site and one injected Pod-observation callback,
not a second controller-side admission observer.

The production rollout gate repeats revision 471's route/report churn while a
fresh allocation has positive feed. East/non-Kueue materializes every granted
compatible slot as a durable intent or correctly attributed row even when
demand is temporarily unavailable. PHX/Kueue materializes one unresolved
row per bounded granted Pod, allowing Kueue to admit the batch concurrently;
the next broker round subtracts all N durable physical debits.
Replay and controller restart create no duplicate intent. Only after those
debits and admission facts are visible may a fresh demand snapshot publish a
paid residual. Verify immediately, at +10, +30, and through the complete
stale/quiescence horizon that no paid claim, provider effect, or retirement was
authorized by an unknown demand snapshot.

The projected-worker runtime-readiness source gate must prove all of the
following before merge:

- a real-PostgreSQL reserved-fill provider-effect test parks simulated
  Kubernetes I/O after the exact effect claim commits and observes zero granted
  or waiting per-service advisory locks and zero `idle in transaction` effect
  session. A concurrent same-service version/owner transition must commit
  without waiting; the parked effect's fresh post-I/O validation must then
  reject the stale authority, while its execution remains effectful and cannot
  be replayed without the existing exact quiescence/provider evidence;

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

Source qualification on 2026-08-22 passed the complete seven-file real-
PostgreSQL admission/lineage/capacity suite and a separate 69-case atomic-
admission PostgreSQL run after correcting its fixture to the production
`DURABLE_FEED`/`DURABLE_INTENT` authority pair. The latter includes a real
concurrent controller transfer while simulated Kubernetes I/O is parked: the
transfer commits without waiting, advances the owner revision, and makes the
fresh post-effect validation reject while retaining `PROVIDER_IO`, active-
delivery adoption, and quiescence-before-reconciliation behavior. A 164-case
integrated non-PostgreSQL execution/observer/Kubernetes/provisioning/transport
suite and 148 dashboard request-telemetry tests passed. Changed source compiles
under Python 3.14.3, mypy passes all 975 configured source files, pylint rates
the changed source 10.00/10, and `git diff --check` passes. Two refreshed
adversarial reviews found no remaining P0/P1 issue. The scope review also
confirmed that the remaining create/adoption attester and exact-UID receipt
classifier are distinct fail-closed lifecycle boundaries, not competing
admission authorities; sharing their pure predicates is a post-production P2
cleanup, not a rollout gate.

The 2026-08-20 production trace showed isolated full-fleet preflights at
2.92--3.32 seconds while a 15-plus-launch wave caused overlapping
five-second attestation failures. The focused regression must use real
PostgreSQL and real OS processes matching the renewal database topology inside
`DisposableExecutor`. Barrier-synchronized cohorts of 15 and 90 independent
renewal observers run against a cold receipt while the elected provider proofs
remain parked for 3.2 seconds, inside the observed
2.92--3.32-second production range, and every caller retains the same
absolute eight-second provider-refresh horizon inside the deployment's
ten-second process-family containment boundary. After publication, every observer also
executes one launch-style receipt read and terminal guard without provider I/O.
The production guardian and warden do not import
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
exactly one advisory lock, then verifies that bounded containment failure
produces `FAILED` plus `family_drained` and, after that process-family proof,
zero surviving proof sessions/locks, no receipt/provider effect, and a
successful later fresh-nonce reproof. This process boundary is
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
session opens per worker plus a fixed allowance of 20 over the complete
eight-second provider-refresh horizon. Tests also cover distinct authorities,
database-clock expiry, conservative SQL-round-trip age mapping, provider
failure without caching, leader/advisory-session loss before publish, the
intentional already-observed-waiter-wave fail-closed result plus one later
durable retry, waiter timeout without leader cancellation, semantic cache
mismatch, malformed exact-authority-row repair, delete/reinsert ABA, scope
mismatch despite a valid context proof, and final-guard rejection of missing,
expired, malformed, wrong-nonce, wrong-digest, wrong-gate, wrong-identity,
or wrong-context rows. It also proves that an identical proactive renewal
preserves the nonce and an already minted reference, a receipt below the
renewer's required remaining horizon is refreshed before return, slow payload
validation and physical connection close cannot consume the requested
consumer/renewal reserve at handoff, and any
proof-content change rotates the nonce and rejects the older reference. The
terminal test requires READ COMMITTED, rejects an independently stale local
reference, proves an uncommitted identical update neither blocks nor rejects a
valid MVCC guard, and proves a changed commit is visible to the next statement
and rejects the old nonce. One renewal is held in progress while 100 concurrent
launch-style consumers read the prior committed positive fact; the identical
renewal retains its nonce and all 100 consumers complete without provider I/O.
A cold consumer creates no row or provider work. Changed positive evidence
rotates the nonce, a typed complete negative deletes the exact positive row,
including while another provider domain remains stuck; an indeterminate
refresh preserves it only until expiry, and a gate rotation
visible before proof work prevents provider I/O. A rotation committed during an
already-running side-effect-free read rejects publication or makes its late
old-generation row inert. The focused
`test_publication_does_not_join_zero_cost_protocol_writer_convoy` and
`test_negative_invalidation_does_not_join_protocol_writer_convoy` cases require
both positive publication and proven-negative invalidation to finish while
another transaction holds the protocol row `FOR UPDATE`.
`test_late_old_gate_publication_is_inert_and_successor_renews`,
`test_gate_rotation_during_provider_read_rejects_old_publication`, and
`test_late_old_gate_invalidation_cannot_delete_successor` own all three gate-
rotation races. Separate process-boundary tests repeat a blackholed libpq claim read
on the same finite lane, prove each handler/socket family absent before reuse,
then complete a healthy invocation. Deterministic poller tests prove the single
fixed-rate loop preserves the one-interval claim-withdrawal horizon.
`test_lost_advisory_session_cannot_publish` and
`test_lost_leader_fails_wave_and_later_call_recovers` prove that a released
owner session cannot publish and that a later elected transaction recovers
without treating process overlap as publication authority.
`test_async_ambiguity_keeps_poisoned_owner_and_blocks_successor` hard-kills
both invocation owners while a handler and its separate-session child remain
live; it proves monitor cleanup completes, the retained lane stays poisoned,
and the deployment event parks without constructing a successor.
`test_post_result_reap_ambiguity_parks_immediately` covers the later window
where a successful typed Future result is already visible but guardian-reap
proof becomes ambiguous; the same boundary call receives that exact ambiguity,
parks the lane immediately, and admits no replacement.
`test_fail_stop_daemon_exit_never_admits_same_pod_successor` kills the runtime
daemon while a stubborn separate-session child survives and proves the
supervisor admits no replacement before its own cancellation.
`test_fail_stop_daemon_supervisor_exception_never_restarts` proves the same
fail-stop branch covers post-admission wait and group-drain failures, while the
unchanged generic-daemon test retains bounded restart behavior.
The reserved-fill provider-effect PostgreSQL tests park simulated Kubernetes
I/O only after the durable `PROVIDER_IO` compare-and-swap commits. They prove
that no PostgreSQL transaction, advisory lock, or proof session remains open
across that I/O; a same-service shutdown or controller-owner transfer commits
without waiting, and the fresh post-I/O validation rejects the stale effect.
The association remains effectful, the active delivery is adopted rather than
replayed, and a later terminal delivery must wait for its exact guardian
quiescence receipt before provider reconciliation. Fleet-proof tests remain a
separate pre-effect authorization gate and never lend their PostgreSQL session
to the provider call:

```bash
pytest -n 0 -q \
  tests/unit_tests/test_reserved_fill_atomic_admission_pg.py \
  tests/unit_tests/test_reserved_fill_reclaim_proofs.py \
  tests/unit_tests/test_boltz_reserved_fill_reclaim_policy.py \
  -k 'reserved_fill_provider_io_holds_no_postgres_authority_session or \
      reserved_fill_owner_transfer_fences_lock_free_provider_effect or \
      multiprocess_renewers_share_receipt_and_launch_reads or \
      one_renewal_serves_100_concurrent_consumers or \
      changed_positive_rotates_and_proven_negative_revokes or \
      gate_rotation_rejects_old_receipt or \
      publication_does_not_join_zero_cost_protocol_writer_convoy or \
      negative_invalidation_does_not_join_protocol_writer_convoy or \
      late_old_gate_publication_is_inert_and_successor_renews or \
      gate_rotation_during_provider_read_rejects_old_publication or \
      late_old_gate_invalidation_cannot_delete_successor or \
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

The 2026-08-21 successor-source qualification passed the complete real-
PostgreSQL proof suite, including both pressure cohorts. The 15- and 90-process
waves completed in approximately 4.03 and 4.04 seconds, peaked at 4 and 30
simultaneous proof sessions, and used 103 and 628 physical session opens,
respectively. After the final schema-v6 bundle-only correction, the focused
cold-read, 100-consumer renewal, changed-positive, completed-negative,
indeterminate-expiry, gate-rotation, nonce-preservation, and terminal-guard
cases passed again against real PostgreSQL. The complete policy/packaging and
focused controller/reclaim suites also passed. This paragraph records source
evidence; the later `1.1.1448` / Helm-572 deployment and production evidence
are recorded at the top of this document. The scheduled +30 readback has been
collected and its changed raw observation was adjudicated as a real eight-card
release that SkyPilot fully claimed and submitted. Final acceptance waits for
those new assignments to settle and for the final zero-paid audit.

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

The regression set must include one cross-layer non-Kueue/East contract with
`min_replicas: 0`, `floor_replicas: 0`, `utilization_gate: false`, zero demand,
and a fresh authenticated grant of `N`: reconciliation emits exactly `N`
width-adjusted sequenced intents and publishes no paid residual or Spot launch
authority. Its Kueue/PHX counterpart also emits exactly `N` bounded durable
intents/lineage rows and never duplicates them on replay. The paired
`utilization_gate: true` cases emit zero idle fill.

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
- Added tests prove one shared multi-context preflight deadline, release of
  observer capacity after a
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
- The historical A/B/C no-deployment statement is superseded by the
  release-`1.1.1436`, lifecycle-85, and lifecycle-93 evidence at the top of
  this document. Full scheduler-authorized GPU convergence is proven. The
  final PR-#1678 cross-Pod takeover and complete stale/quiescence interval are
  production-proven. Paid residual and drain are source- and real-PostgreSQL-
  test-qualified; deliberately billable production load is not a closeout
  requirement. The +30 control-plane/HA/error horizon passed and its coincident
  eight-card release was fully claimed/submitted; final worker-readiness
  settlement and the zero-paid audit remain open.

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
8. every active service version uses worker projection protocol v7 and no
   ordinary launch has consumed a v1-v4 decoder for one complete 180-second
   authority horizon, while exact v5/v6 projection decoding remains source-
   qualified for the N-2/N-1 read window and cohort-5 can perform only exact
   terminal `PROJECTED`/`ABSENT` replica-row retirement; and
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

- [x] Merge and deploy PRs #1671--#1677. They establish protocol v6,
  lock-free proof renewal, stable claim/conserved-capacity-hint identity, and
  bounded historical teardown. Lifecycles 89--91 retired through normal
  evidence-backed cleanup with no manual SQL deletion.
- [x] Recreate the canonical no-EFS service as lifecycle 93/version 1 with
  `min_replicas: 0`, zero fill floor, `utilization_gate: false`, and only
  immutable server-owned worker projections. Gate generation 33 authorized the
  complete pre-PR-#1678 cohort.
- [x] At one synchronized denominator, prove a submitted worker for every East
  scheduler-fit slot and every PHX slot Kueue admits. East was 58/58 Ready;
  PHX was 75/75 admitted, Running, and Ready, with three additional submitted
  Workloads visibly queued by unchanged Kueue/topology policy. All 133 Ready
  replicas were reserved/non-Spot and paid claims, paid waiters, and provider
  Spot clusters were zero.
- [x] Prove current request telemetry on lifecycle 93. Two reporters produced a
  fresh complete projection; the idle sample showed zero recent, processing,
  in-flight, unknown, queued, and rejected requests, and the dashboard rendered
  those values. An authenticated harmless endpoint request returned HTTP 200
  without creating paid capacity.
- [x] Merge PR #1678 at `38ec2434245c2286d36c81d371834f97bde4f43c`,
  qualify the duplicate-up and observer lock-order corrections, and publish the
  exact `1.1.1448` image and chart artifacts.
- [x] Finish Helm revision 572 and prove all seven writer Pods use image digest
  `sha256:ba7602e63363baaa35210b8aafd429b7050c1a3fa5861a0bc9c58939c9fecb3c`.
  All two API, two controller, and three executor Pods were Ready on that exact
  digest; only that homogeneous inventory was reauthorized at gate generation
  34.
- [x] Live-test rejected duplicate `serve up`: lifecycle 93, version 1,
  immutable service hash, and owner epoch 4 remained unchanged.
- [x] Prove cross-Pod owner takeover onto a new PR-#1678 controller Pod.
  Durable ownership advanced from `10.30.1.190` / epoch 3 to `10.30.0.98` /
  epoch 4 with a new incarnation, while service Ready state, endpoint,
  lifecycle/version/hash, and replicas remained stable.
- [x] Recompute the canonical exact-40-object Kueue POST snapshot by mapping
  each object to
  `{apiVersion,kind,name:.metadata.name,namespace:.metadata.namespace,spec,data}`;
  deleting top-level nulls only; sorting by
  `(apiVersion,kind,namespace-or-empty,name)`; serializing the complete array
  with `jq -cS`; and hashing the bytes including the trailing newline. Require
  equality with PRE hash
  `fd5af31d5d1570701e2a7b636691fa4efbf15d6efdba3ee8277d7f5b982d170d`.
  The normalized PRE and POST values matched. The nested-metadata,
  recursive-null alternate also matched at `1013f6df...`; the former
  `2c8b2d...` recipe is historical and is not a rollout comparator.
- [x] Repeat the stable-claim/conserved-hint and synchronized occupancy census
  after the rollout. Require 100% of the then-current scheduler-authorized
  denominator; use 133/133 only as the pre-rollout reference. Claim generation
  must not rotate on runtime-only budget heartbeats; query-to-row-scan movement
  must conserve capacity; mixed/N-1 readers must fail closed without epoch
  churn. Post-rollout reads retained current claim generation 7778 and
  allocation generation 17 with grants/edge caps of 13 A100, 45 A100-80GB,
  and 78 H200. The census was 133 Ready plus three PHX queued/provisioning, all
  136 reserved/zero-cost/non-Spot; paid claims and waiters were zero. The three
  observed-free H200s were already debited by queued intents, so spendable
  capacity was zero. At +30, a new eight-card A100-80GB release moved raw free
  to eight without rotating claim generation; SkyPilot immediately created
  replicas 150--157, leaving spendable zero and reconciling 262 research plus
  66 fleet assignments to all 328 East GPUs. Mixed/N-1 failure behavior remains
  source/test-qualified.
- [x] Prove the v6 runtime/cache/Python trees remain on bounded memory scratch,
  rootfs stays bounded, and no fleet worker causes node DiskPressure or
  MemoryPressure. At 03:06:23 UTC all 66 PHX nodes were Ready with no
  DiskPressure, MemoryPressure, or PIDPressure, and the normalized Kueue PRE and
  POST objects were identical. No shared Kueue or platform object changed.
- [x] Implement and focused-source-qualify protocol v7/cohort 7: terminal
  apt/SSH markers, no hard apt/dpkg interruption, interrupted-dpkg repair,
  bounded PID-1 failure detection, exact-current fresh effects, N-1 typed
  finalization, and durable-cleanup-marker-gated N-2 terminal takeover.
- [ ] Merge and publish the immutable v7 source/image/chart tuple.
- [ ] Direct-Helm deploy every API/controller/executor writer to that exact v7
  tuple with `--reuse-values`, leaving lifecycle 93 at cohort 6 and
  `SEQUENCED_ACTIVE` generation 34. Prove mixed overlap permits cleanup only
  and rejects fresh effects.
- [ ] With controller hold false, retry supported
  `sky serve down boltz-l4-fleet --purge -y` until all nine lifecycle-93
  `UNKNOWN` rows retire and both PostgreSQL and the East/PHX providers prove
  total absence. Do not demote, update, advance the gate, mutate rows, or
  manually delete Pods/Workloads.
- [ ] Recreate a fresh same-name v7 service from the canonical zero-minimum,
  zero-floor, immutable-projection definition. Pass full preflight, exact
  writer/database census, and the byte-exact unchanged-Kueue snapshot. Re-read
  `sky serve status boltz-l4-fleet --endpoint`, but directly probe only the
  load-balancer-local health read and authenticated `/_lb/capacity`. Keep
  Platform on the dead/old endpoint or behind an explicit dispatch hold. For
  an unchanged hostname, require that hold or a continuous zero-request census
  through reserved readiness. Do not send a model/catch-all request before
  activation.
- [ ] Perform exactly one active-to-active CAS from `SEQUENCED_ACTIVE`
  generation 34 to generation 35. Never reopen `LEGACY_ACTIVE`. Before
  reconnecting Platform, prove all currently scheduler-authorized reserved
  intents assigned, at least one compatible reserved replica Ready, and zero
  paid claims, waiters, and provider Spot capacity.
- [ ] Prove 100% assignment of compatible East scheduler-fit capacity and PHX
  capacity admitted by Simone's unchanged Kueue policy; Ready or typed-terminal
  convergence; zero paid residual while reserved covers demand; and the
  immediate/+10/+30/full stale-horizon gates. Only after the preceding gates,
  update `SKYPILOT_SERVE_LB_URL` and roll compute-api if the endpoint changed,
  or release the dispatch hold if it did not. Send the authenticated harmless
  `GET /v1/models/model` and immediately re-prove zero paid. Obtain rollout-
  audit acceptance.

- [x] Merge and deploy the deployment-owned provider-proof singleton in PRs
  #1656/#1657 and release `1.1.1429`; revision 501 proved it dark through more
  than two receipt lifetimes and revision 502 released the hold.

- [x] Merge and direct-Helm deploy the forced-renewal horizon correction as
  policy revision `1.1.1430` under the Serve controller hold. Helm revision 505
  runs the exact release, generation 10 is reauthorized, and repeated east/PHX
  renewal plus deployment-proof-daemon takeover passed through more than two
  receipt lifetimes.

- [x] Merge and direct-Helm deploy the typed pre-job provider-absence
  retirement correction in release `1.1.1436` / Helm revision 523. Normal
  fenced purge completed lifecycle 84 without manual database deletion;
  lifecycle 85 was then created, exercised, and also normally purged.
  Lifecycles 89--91 later retired normally. Lifecycle 93 owns the historical
  convergence evidence and the current typed `FAILED_CLEANUP` incident.

- [x] Review, merge, and direct-Helm deploy PR #1667's exact execution-capsule
  cohort in release `1.1.1437`.
- [x] Merge and direct-Helm deploy PR #1671's exact complete writer cohort,
  then recreate `boltz-l4-fleet` from the canonical
  `min_replicas: 0`, zero-floor, no-EFS YAML. Configured admin policy is
  absent at controller freeze and executor replay, one frozen launchable
  singleton supplies `best_resources`, and protocol v2 invokes neither initial
  nor retry optimization/failover.
- [x] Merge and deploy lock-free provider-proof publication. Both context
  receipts remained continuously fresh under the 148-Ready/eight-booting wave,
  with sampled ages below ten seconds and no proof-publication protocol-lock
  waiter.
- [x] Finish normal teardown of lifecycle 91 through the supported
  whole-service ABSENT path and recreate lifecycle 93 from the same canonical
  YAML. No manual SQL deletion or v5 fresh-launch authority was used.
- [x] Prove the deployed stable claim/conserved-hint path can converge to all
  current scheduler-authorized capacity: lifecycle 93 reached 133/133 Ready
  with three additional PHX Workloads separately queued by Kueue.
- [x] Repeat claim-generation stability after gate-34 activation. Claim
  generation 7778 and allocation generation 17 remained current across the
  post-rollout reads while runtime observations changed without epoch churn;
  mixed/N-1 failure behavior is covered by the source/test qualification.

- [x] Merge and direct-Helm deploy the single pre-demand PostgreSQL fill
  admission. Its final end-to-end no-paid/no-provider/no-retirement horizon is
  tracked by the clean-service convergence gate below.

- [x] Deploy Serve053 recognition and empty durable-intent recovery as exact
  Helm release `1.1.1377`; prove the controller advances route generations and
  the original provider-absent rows retire without manual deletion.
- [x] Merge and source-qualify PR #1604's Serve054 PostgreSQL provider-proof
  receipt at `5d473147dfbaecead6b1501f923f47abf58adfe5`; three fresh exact-head
  CI passes prove the deterministic owner/parked-loser contract.
- [x] Deploy PR #1604's merged source by direct Helm fix-forward. Its
  launch-wave liveness limitation was superseded by the independently renewed
  receipt in PR #1650.
- [x] Merge and direct-Helm deploy the successor renewable receipt source in
  PR #1650 / release `1.1.1422`. Source implementation uses the existing
  Serve054 table,
  a supervised three-second observer, five-second provider refreshes,
  30-second database-clock freshness, and two-second consumer reads. Complete
  negative observations revoke immediately and malformed or
  indeterminate responses retain prior authority only until expiry. A typed
  negative must win without waiting for a hung peer; claim reads must use one
  controller-owned finite disposable lane with irreversible ambiguity
  fail-stop; ResourceFlavor specs, node labels, and live Node selectors must
  match the complete reviewed contract. Its per-service renewal owner is
  superseded by the deployment singleton described above. Clean lifecycle-93
  occupancy qualification, PR-#1678 cross-Pod takeover, and the complete
  stale/quiescence interval are production-proven.
- [x] Merge and deploy the implemented atomic PostgreSQL
  replica/association/request receipt. Its feature change already removes
  protocol-v2 direct/non-atomic admission and rejects reserved fill at the HTTP
  surface; no system-identity or RWX/EFS correctness fallback remains.
- [x] Merge and direct-Helm deploy the canonical-v4 projected-worker
  runtime-readiness precursor. East and PHX exact projected workers remained
  Ready through the full stale interval. The negative failed-bootstrap
  boundary is source/test-qualified; deliberately faulting a production base
  bootstrap is not a closeout requirement.
- [x] Merge and direct-Helm deploy protocol v5/cohort 5 so memory-scratch
  workers place SkyPilot runtime, uv cache, and uv-managed Python under the
  authenticated `/tmp` contract; a fresh `kubectl exec` inherited all three
  exact paths and the sampled worker used about 1.3 GiB of its 20 GiB `/tmp`.
- [x] Merge and direct-Helm deploy protocol v6/cohort 6 with the same scratch
  paths under one unambiguous discriminator. Admitted-object drift fails
  closed and v4/v5 settlement remained intact. Post-PR-#1678 node-rootfs
  readback passed through the full stale horizon: all 66 PHX nodes were Ready
  with no DiskPressure, MemoryPressure, or PIDPressure. This change has no
  schema, EFS/RWX, KubeRay, Terraform, Terragrunt, platform-pin, task-resource,
  or Kueue-policy change.
- [x] Open cross-linked draft projection cleanup PR #1619. Keep it draft until
  a complete protocol-v7/cohort-7 deployment, retained-row and
  in-flight/provider evidence, east/PHX/generic/no-paid proofs, nodefs proof,
  and the full 180-second stale-authority/quiescence horizon are complete. The
  earlier v6 gates passed, but the v7 rollout and +30 eight-card refill
  settlement remain open. Retarget the cleanup to remove only the v1-v4
  projection readers after its retained-row predicates pass; preserve exact
  v5/v6 projection decoding for the N-2/N-1 read window. Broad settlement
  remains N/N-1; cohort-5 N-2 may only retire already canonical terminal
  `PROJECTED`/`ABSENT` state while the service retains the exact cohort-5 tuple.
  Rotation into N-2 must fail closed until every association-backed replica row
  is physically retired; only exact settled, terminal, projected, pin-released,
  internally consistent replica-free association history may remain.
- [x] Open cross-linked draft Serve058 cleanup PR #1660. Keep it draft and,
  only after a complete capable cohort, zero nullable owner tuples, no old
  writers, backups, and the full stale/HA production horizon, make service
  owner columns `NOT NULL` and remove the one-shot attestation transition. The
  capable-cohort and stale/HA gates are complete; final settlement/audit of the
  +30 eight-card refill and the cleanup-specific nullable-owner/stale-writer
  census remain mandatory.
- [x] Merge and direct-Helm deploy reclaim-safe failed-service teardown PR
  #1651 in release `1.1.1423` / Helm revision 489. Production proved zero
  matching provider Workloads/Pods, load-balancer objects, and central cluster
  rows before exposing the final database delete-order blocker.
- [x] Merge and deploy the replica-before-service intent-FK correction and the
  independent JIT recovery snapshot-receipt correction in release `1.1.1427`.
  The normal evidence-backed purge and lifecycle-84/version-1 recreation
  removed the old live teardown blockers without manual row deletion.
- [x] After protocol-v6 deployment, prove a fresh large grant assigns every
  compatible policy-admissible free GPU within the bounded convergence target,
  with zero paid spill and no ambiguous capacity debit. Lifecycle 93 submitted
  78 PHX Workloads, reached 75 admitted/Running/Ready, left three visibly
  Kueue-queued, and concurrently kept all 58 East scheduler-fit GPUs Ready.

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
- [x] Merge and deploy Platform PR #8822 as platform revision 29. Readback
  proves Simone's flat implicit `shared-pool`, zero explicit Cohort objects,
  no new priority class, existing `be-lt=11`, list-only audit permissions, and
  a PostgreSQL projection naming `be-lt`. Platform PR #8824 subsequently
  activated the SkyPilot queues without changing that topology. This is
  platform-shape/admission evidence, not reserved-backfill convergence proof.
- [x] Deploy the schema-v6 SkyPilot attester merged in PR #1650 as release
  `1.1.1422`: retain schema-v5's removal of admission-policy/binding reads, add
  the exact implicit-flat-cohort/seven-ClusterQueue PHX topology, governed
  membership closure from two complete cluster-wide LISTs, and independent
  renewable provider receipts.
- [x] Obtain fresh successful two-context preflights for the clean recreated
  service on release `1.1.1429`; three samples completed in
  3.076--4.336 seconds. Policy revision `1.1.1430` repeated the proof as part
  of generation-10 reauthorization, including eight renewal samples over 93
  seconds with receipt ages of 2--11 seconds. No runtime pin, KubeRay path,
  second scheduler, or EFS authority is part of this gate.
- [x] Commit and elect historical version 62 whose non-null immutable east and PHX worker
  projections exactly match the corrected server configuration. It proves the
  projection generator but is not an activation candidate because its
  `v5.44.1-boltz-2` model image was intentionally rolled back by Platform PR
  #8635.
- [x] Commit and elect historical demand-gated successor version 63 on
  the reviewed `v3.682.2-boltz-2` source with the corrected config and exact
  three non-null projections. Exact readback proves `min_replicas: 0`, fill
  floor 0, `utilization_gate: true`, and controller-applied lifecycle epoch 82.
  It differs from version 62 by the intentional model rollback and from the
  final backfill version only by `utilization_gate`. Normal teardown and
  canonical recreation later replaced it with lifecycle 84/version 1.
- [x] Lifecycle 85 proved automatically generated zero-cost PHX H200 Pods are
  admitted through `be` -> `skypilot-be` with existing
  `be-lt=11` WorkloadPriorityClass, the independent -1000/`Never` Pod priority,
  `skypilot-pool-sa`, `default-scheduler`, a Kueue TAS assignment, and exact
  H200/cluster identity. Every submitted Workload was admitted and no paid
  claim was created. Lifecycle 93 later supplied the separate full-capacity
  convergence proof.
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
- [x] Complete qualification of the already-deployed durable-route HA source,
  PR #1561 exact-head hardening, and #1562 takeover fix. Revision 502 proved one
  consolidated-HA recovery. Source and real-PostgreSQL tests verify feed
  generation `N` publishes both
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
  closed/open gate and cutover CAS result. Helm revision 572 supplied the true
  cross-Pod production exercise: ownership advanced from epoch 3 to epoch 4 on
  a new Pod/incarnation while route, service identity, endpoint, and replica
  set stayed healthy, and telemetry reported stable LB HA.
- [x] Exercise the canonical fresh-service birth and move `boltz-l4-fleet` onto
  it by evidence-backed normal teardown and recreation. Lifecycle 84/version 1
  and lifecycle 85/version 1 read back at generic `bound`,
  `DURABLE_PROJECTED`, `DURABLE_FEED`, and `DURABLE_INTENT` epoch 1 with one
  birth incarnation and no committed legacy interval. Both were normally
  purged; lifecycle 91 was also normally retired and lifecycle 93 was the clean
  post-#1677 recreation. Its PR-#1678 cross-Pod takeover and complete stale
  interval and +30 control-plane/HA/error boundary are production-proven.
  Lifecycle 93 is now `FAILED_CLEANUP`; the fresh-v7 recreation proof remains
  open.
  Activation is one way and fix-forward only.
- [x] Merge Platform PR #8652 after CI and review. It is a service-spec change
  only: no Terraform/Terragrunt or platform runtime pin is part of this path.
- [x] Apply #8652's production full-backfill service update from the clean
  demand-gated activation version. Version 64 was elected with the known-good
  model image and immutable worker projections before historical teardown.
  Canonical recreations subsequently completed as lifecycle 84/version 1,
  lifecycle 85/version 1, lifecycle 91/version 1, and former lifecycle
  93/version 1. Scheduler-authorized occupancy convergence was proven; the
  restart/takeover and stale horizons are complete. Paid residual/drain is
  source- and real-PostgreSQL-test-qualified without deliberately billable
  production load.
- [x] Merge and deploy corrective Serve056 PR #1630's complete committed
  provider handoff; release `1.1.1410` and every successor through revision 489
  contain it.
- [ ] Keep cross-linked draft cleanup PR #1633 blocked until the clean recreated
  service passes the zero-scalar-`NULL`, stale/quiescence, no-paid, and provider
  proof horizons. Do not deploy EFS, KubeRay, Terraform/Terragrunt, or a
  platform runtime pin. The stale, no-paid-at-observed-demand, and provider
  historical production horizons are complete; accept the fresh v7 recreation,
  refill, and zero-paid audit, and prove the cleanup-specific zero-scalar-
  `NULL` census before unblocking it.
- [x] With zero authenticated demand, prove every fresh, authenticated,
  policy-compatible reclaimable zero-cost slot granted to this service is
  covered by a durable intent or a correctly attributed admitted,
  provisioning, or ready replica in the exact accelerator-width unit. Reconcile
  the count from physical observations through broker grants, intents, Kueue
  reservations, Pods, and ready replicas. Every residual must carry one typed
  fresh reason; ordinary paid claims and new Spot launches must remain zero.
  Lifecycle 93 closed this at 133/133 Ready, zero paid claims/waiters, and zero
  provider Spot clusters; the three non-Ready PHX rows were visibly queued or
  pending and consumed the remaining policy-assignable slots.
- [x] Prove the large-fill submission path is not an artificial serial prefix.
  SkyPilot durably submitted all 78 PHX candidates, 75 became concurrently
  admitted/Running/Ready, and East independently converged to 58/58. The old
  80--90 target is superseded by the exact currently available denominator;
  SkyPilot must not manufacture additional Kueue admission to meet a nominal
  count.
- [x] Prove reserved supply is committed before the paid residual, no new Spot
  launches occur while compatible reserved supply covers demand, and any future
  paid capacity drains from a fresh exact-card durable-demand snapshot. Current
  source and real-PostgreSQL tests cover commit-before-residual, exact-shape
  suppression, and ordinary READY-aware paid retirement. Production direct GCP
  provider inventory, paid-capacity claims, and paid waiters were zero at idle
  and after one harmless authenticated request. Deliberately purchasing
  uncovered capacity is not a closeout gate; future genuine uncovered demand
  will exercise the same qualified path and provide ordinary billing-cessation
  evidence.
- [x] Adjudicate orphaned/ambiguous historical rows only from durable
  quiescence and provider evidence. Keep historical failed rows out of current
  capacity, placement, and UI totals without deleting evidence-bearing rows.
  Lifecycles 89--91 retired through normal evidence-backed cleanup, and the
  historical active census reported zero association ambiguities. The current
  lifecycle-93 nine-row cleanup incident must complete through v7 typed
  authority before a fresh lifecycle supplies allocation or dashboard totals.
- [ ] After the production horizon, decide separately whether to delete the
  unused Terraform-managed shared filesystem and any freshly confirmed
  scaled-zero ReplicaSets that still mention the retired claim. The PVC, PV,
  and sole access point are already absent. This is optional deletion-only
  platform hygiene, not a convergence gate; no live authority, Pod, or
  controller may consume the retired claim.
- [x] Verify the active `DURABLE_FEED` dashboard exposes confirmed processing,
  queued, in-flight, rejected, and freshness under a healthy controller. The
  lifecycle-93 projection was fresh/complete with two reporters and the page
  rendered those fields. It remained fresh/complete through PR-#1678 cross-Pod
  controller takeover and the complete stale interval; artificial provider-
  stall injection is source/test-qualified rather than a production gate. Add
  a PostgreSQL idempotency ledger and explicit
  completeness contract before labeling any value exact completed HTTP
  exchanges; exact completed model jobs additionally require an at-least-once
  worker callback. Separately expose economic provenance (reserved fill, other
  zero-cost, paid Spot, paid non-Spot, unknown) and lifecycle (ready,
  provisioning, shutting down, cleanup-uncertain, historical) axes.
- [x] Pass typed provider present/absent/unknown/replaced,
  legacy-real-effect, lost-ACK, poisoned-row progress, broker conservation,
  no-paid-spill, and full restart/adoption tests. Focused unit and real-
  PostgreSQL qualification covers these negative and concurrency boundaries;
  the production rollout separately proved cross-Pod takeover and zero paid
  spill at the observed demand boundary.
- [ ] After the complete capability, stale-writer, route, demand, and
  actuation horizon proves zero old-path use and zero unsettled unbound work,
  complete the deletion-only
  `cleanup/remove-legacy-serve-authority-transitions` branch from current
  source. Its additional gate is that every live central-PostgreSQL non-pool
  service was canonically recreated or evidence-backed retired, and no
  production registration omits a lifecycle epoch. Closed/superseded PRs
  #1506/#1510 reserve no schema or API heads and must not be revived. The live
  historical capability/horizon evidence remains valid, but fresh-v7
  purge/recreation and refill acceptance are open; cleanup-specific zero-old-
  path predicates still apply.
- [ ] Keep atomic-authority cleanup #1556 stacked on feature PR #1555 until
  the immediate, +10 minute, +30 minute, and complete stale/quiescence horizon
  proves no partial authority pair, paid spill, or ordinary-traffic regression;
  then merge it before declaring phase 2e complete. The historical +30
  control-plane boundary passed, but the fresh-v7 refill must settle and retain
  a zero-paid census before this cleanup gate is accepted.
