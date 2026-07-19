# SkyServe exact-accelerator compatibility, priority, and per-card capacity plan

_Created: 2026-07-19_

## Decision summary

Add one compatibility-aware SkyServe queue. Each request may carry a subset of the exact accelerator identifiers configured by the service. The subset constrains where the request may run; it is not a preemption instruction and it is not a hard preference for the first card in the list.

Scheduling and scaling follow these rules:

1. Never preempt or migrate an admitted request.
2. Numeric request priority remains the primary queue order (`high = 50`, `low = 20` in boltz-platform).
3. At equal numeric priority, make a supply-aware assignment that maximizes immediate admissions and protects the request with the worst realistic fallback; FIFO breaks a true fallback tie. Raw compatibility-set size is not a sufficient ordering rule.
4. A request uses already-ready compatible capacity before causing a scale-up, even if that ready capacity is a larger card. Among otherwise-valid ready assignments, prefer reserved/zero-cost replicas before paid replicas so paid capacity can become idle and scale down.
5. A healthy provisioning replica is committed future capacity, not a routable slot: count it against the target to avoid a duplicate launch. For demand still unmet after ready and committed capacity, launch into a free compatible reserved-capacity slot, then cold-start the cheapest compatible paid card.
6. A missing compatibility field means every exact accelerator configured for the active SkyServe service version is compatible.
7. Global demand target, hard per-card serving-replica floors, and optional reserved-fill targets remain separate control-plane signals. The UI shows all three.
8. `A100` and `A100-80GB` are distinct identifiers in validation, queue indexes, metrics, APIs, placement, tests, and UI. Matching may be case-insensitive, but it must never use family, prefix, regex, or memory-suffix normalization.

The priority rule deliberately means that a flexible priority-50 request remains ahead of a constrained priority-20 request. Within the same numeric priority, however, an `A100`-only request has no fallback and therefore gets the next A100 slot ahead of older flexible `L4/A100/H100` work. This preserves the existing strict-priority contract while protecting scarce-card access among peers.

## Baseline and scope

- SkyPilot design baseline: `boltz-bio/skypilot` `origin/improvements` at `a6dd3a0def00461da5f8bb5af6f15a7f3680b329`.
- boltz-platform integration baseline: `boltz-bio/my-full-stack` PR branch `feat/skyserve-request-priority-header` at `3d6df7a48d68f90cc603f585b9bb1537c8a17fa3`.
- Existing SkyServe request priority, process-local admission queue, instance-aware least-load policy, exact `replica_info.gpu_type`, targeted resource override support, and reserved-capacity broker are extended rather than replaced.
- Existing HA behavior remains: one active load-balancer authority owns queue/admission state; clients retry across an authority change. Queue durability across an LB failover is not introduced by this project.
- This plan covers SkyServe and the boltz-platform request path. It does not add preemption, priority aging, a persistent distributed queue, or per-card maximums.

## Behavioral contract

### Request compatibility wire contract

Add the data-plane header:

```text
X-SkyServe-Compatible-Accelerators: L4,A100,A100-80GB,H100
```

- One header field is allowed. Reject repeated fields, an empty value, empty tokens, duplicate exact cards, unknown cards, excessive token count, or a value longer than a small fixed limit (for example 512 bytes) with HTTP 400 before queue admission.
- Trim optional HTTP whitespace around comma-separated tokens.
- Resolve each token case-insensitively against the active service version's configured exact accelerator IDs, then retain the service's canonical display spelling.
- Never apply `HardwareGroup` regex matching or accelerator-family canonicalization. `A100` does not match `A100-80GB` in either direction.
- Treat the supplied sequence as a compatibility set for queueing and demand aggregation. Preserve its order for observability and deterministic equal-cost tie-breaking, but do not let list order force a cold paid launch when a cheaper compatible card exists.
- When the header is missing, synthesize the full set of exact accelerators configured by the active service version. This behavior lives in SkyServe, so older and non-platform clients automatically get the safe default.
- Strip the SkyServe-only header before proxying to user replicas.
- Snapshot the active service version and canonical compatibility set at admission. On an in-place service update, intersect queued waiters with the new exact set: re-index surviving waiters without changing priority/sequence; fail a waiter whose set becomes empty with a retryable 503 rather than silently widening it.

The capacity endpoint advertises a versioned capability, for example:

```json
{
  "request_accelerator_compatibility_version": 1,
  "configured_accelerators": ["L4", "A100", "A100-80GB", "H100"]
}
```

SkyServe advertises version 1 only when it has complete exact-card telemetry and at most `MAX_COMPATIBILITY_ACCELERATORS = 8` configured cards. Services outside that bound keep their legacy behavior and do not advertise the feature, so this addition does not invalidate an existing larger `resources.any_of` service. boltz-platform sends the header only after seeing version 1. An omitted platform field may safely remain omitted against an old LB because both sides mean all configured cards. An explicit subset must either use another exact-compatible provider candidate or fail closed with an unsupported-capability/no-capacity result when version 1 is absent; it must never be silently widened by omitting the header. This provides a safe SkyPilot-first rollout and prevents a new platform binary from assuming filtering on an old load balancer.

### Queue and dispatch order

Keep one authoritative waiter object and one ownership/state transition path. Per-card indexes contain references to those waiters; they are not separate queues.

Process numeric-priority tiers from highest to lowest. Within one tier, group authoritative FIFO waiters by compatibility bitmap and solve a bounded profile-to-ready-card assignment (at most 255 profiles by 8 exact card types) with these lexicographic objectives:

1. maximize the number of requests admitted immediately;
2. give a scarce ready slot to the compatibility profile whose best non-selected fallback is worse;
3. preserve FIFO sequence when fallback quality is equal.

Define fallback quality from the same exact-card supply snapshot as an ordered tuple, not from compatibility count:

```text
ready reserved/zero-cost alternative
  < ready paid alternative
  < healthy provisioning alternative within startup SLA
  < free reserved-capacity alternative
  < paid cold alternative (cheapest/fastest first)
  < unavailable alternative
```

A ready card is an admission edge only when it is in the waiter's exact compatibility set. Provisioning/reserved/paid alternatives influence which waiter most needs a scarce ready slot, but they do not become routable until ready. A provisioning attempt that exceeds its startup SLA or enters failure stops counting as a healthy fallback and triggers replanning.

Reserved-first is a replica-assignment tie-break after numeric priority, maximum immediate admission, and scarce-card protection. It must not let a flexible request take the only ready reserved A100 from an equal-priority A100-only request when a paid L4 can serve the flexible request. Within an equivalent exact-card assignment, route to a healthy reserved replica with a free concurrency slot before a paid replica; never overload reserved capacity merely to preserve the cost preference.

Example for equal numeric priority:

- Request A is compatible with `{L4, A100}`; request B with `{A100, H100}`.
- If L4 is unavailable and A100 plus H100 are ready, maximize admissions by assigning A to A100 and B to H100.
- If only A100 is ready and neither request has a viable alternative, both fallbacks are equally unavailable and FIFO decides.
- If only A100 is ready but A's alternative is a cheap paid L4 while B's alternative is a more expensive paid H100, B gets A100 and the scale planner targets L4 for A.

This matching rule subsumes the simple A100-only-versus-flexible case without making the incorrect assumption that all two-card sets are equally flexible.

The scheduler atomically grants a waiter a card-specific capacity reservation/eligible replica set. It removes all secondary references in the same lock, so a flexible request cannot be granted once from L4 and again from A100. New requests always enter the authoritative waiter registry before dispatch; they cannot bypass already-eligible waiters.

If a granted replica fails during proxying, the request may retry on another compatible ready card. The scheduler atomically transfers the reservation. If no compatible slot remains, it requeues the same waiter with its original priority, FIFO sequence, deadline, and compatibility set. It does not become a newer request and does not hold a phantom card slot.

Strict numeric priority and fallback-aware ordering can starve lower-priority requests or requests with consistently better alternatives. This is intentional and matches the accepted priority policy; existing queue timeout/cancellation remains the bound.

### Demand allocation and scale-up choice

Compatibility demand is counted once. It must never be copied into every compatible card's backlog.

The active LB reports a bounded histogram keyed by `(numeric priority, compatibility bitmap)` over the service's exact configured cards. List order is ignored for the histogram, so equivalent sets coalesce. `MAX_COMPATIBILITY_ACCELERATORS = 8` bounds compatibility masks at 255; numeric priority is already bounded to 0..100, and the sparse payload is additionally bounded by the configured request-queue size. Priority partitions demand for ordering but never duplicates a request or multiplies desired capacity. In-flight work is attributed to the exact card actually holding the slot.

The autoscaler allocates aggregate demand by numeric priority descending using the same exact-card marginal-supply model, with stable assignments and existing up/down hysteresis to prevent oscillation. Below `max_replicas`, all priority partitions still contribute demand; when the cap forces a choice, the per-card target allocation mirrors queue precedence instead of reserving scarce capacity for work that cannot yet be admitted. For each compatibility profile:

1. subtract ready capacity already assigned to its demand, consuming reserved/zero-cost ready slots before paid ready slots within an otherwise-valid compatibility assignment;
2. subtract healthy compatible replicas already provisioning from the still-needed target—they are committed capacity, not a place to dispatch;
3. for residual scale-out, claim a free exact-card slot on reserved/zero-cost infrastructure;
4. for any remaining scale-out, choose the cheapest cold paid compatible resource, using request/service order only as a deterministic equal-cost tie-break.

The controller recomputes after each supply transition. It may launch reserved and paid capacity in the same control cycle when demand exceeds already-ready, provisioning, and reserved capacity; the list above is allocation accounting, not a requirement to wait serially for one tier to finish.

This is why an already-ready reserved A100 may serve flexible L4/A100/H100 work, while an empty fleet normally cold-starts the cheaper L4. When an A100-only request later arrives, no running flexible request is interrupted. At equal priority it owns the next A100 admission opportunity, and its demand increases the A100 target if capacity is otherwise occupied.

### Global target, per-card target, and floors

Add an exact-card floor map to `replica_policy`:

```yaml
service:
  replica_policy:
    min_replicas: 0
    max_replicas: 100
    min_replicas_by_accelerator:
      L4: 0
      A100: 0
      A100-80GB: 0
      H100: 0
```

- Keys must resolve to distinct exact accelerators present in the service task resources. Unknown/family/regex keys are invalid.
- Missing keys have floor zero. The whole map may be omitted for backward compatibility.
- Values are non-negative serving-replica counts. Reject `sum(per-card floors) > max_replicas`.
- Existing `min_replicas` remains an independent aggregate floor. If it exceeds the sum of card floors, allocate the remainder with the same ready/reserved/cheapest policy.
- `demand_target_by_accelerator` includes the hard per-card floor and its entries sum to the existing aggregate `target_num_replicas`.
- The aggregate demand target is `max(calculated demand, min_replicas, sum(per-card floors))`, capped by `max_replicas`. When demand exceeds the cap, requests remain queued; compatibility is never widened.
- Scale-up decisions carry an exact accelerator resource override. Scale-down selects an exact card whose current serving replicas exceed that card's target and floor, observes the existing graceful/idleness delay, and never terminates active work.
- For this first version, require one GPU-count shape per exact accelerator ID in a multi-card service. Reject ambiguous configurations such as both `A100:1` and `A100:8` under one `A100` floor until the public identity is extended to an exact card-plus-count shape.

The control loop exposes three related but different values:

```text
global demand target = sum(demand target per exact card)
effective desired per card = max(demand target, reserved-fill target)
actual replicas = ready + provisioning + other live states
```

This keeps global and per-card targets independently understandable without allowing them to contradict each other.

### Reserved capacity

Reserved capacity is supply, not accelerator identity and not hidden demand.

- Observe, claim, and report reserved slots by exact `(cluster/context, accelerator_id)` pool. Lowercasing for case-insensitive equality is allowed; collapsing `A100` with `A100-80GB` is forbidden.
- Split any current multi-accelerator broker/fill round into exact-card grants before applying per-card targets. A claim for A100 cannot satisfy an A100-80GB decision.
- A free compatible reserved slot has zero incremental infrastructure cost and therefore wins before a paid cold start, including when it is a larger card.
- A healthy ready replica already running on reserved infrastructure wins before an otherwise-equivalent ready paid replica. This makes paid replicas idle sooner so the normal graceful scale-down can remove them; request priority, compatibility matching, and concurrency safety still take precedence.
- Keep `reserved_capacity_fill` as an optional overlay, reported as `fill_target_by_accelerator` and `free_reserved_slots_by_accelerator`, not folded into demand target.
- With fill enabled, zero-cost serving replicas may intentionally remain above demand/floors; the UI labels them as fill capacity. With fill disabled, idle serving replicas gracefully drain to demand/floors while the underlying reserved physical machines may remain up and appear as free reserved supply. This is expected extra capacity, not a failed scale-down.
- When demand and fill both want the same exact-card replica, count it once via `max(demand_target, fill_target)`, not by adding both targets.

## Architecture flow

```text
boltz-platform user request
  compatibility?: [exact Hardware values]   priority: high|low
                 |                                  |
                 +-------------+--------------------+
                               v
                 one SkyServe service request
                 compatibility header + 50/20 priority
                               |
                               v
        one authoritative LB waiter registry / admission scheduler
          exact-card secondary indexes; no per-card duplicate queues
                               |
             ready exact card? +---- yes ----> grant and proxy
                               |
                               no
                               v
        compatibility bitmap demand histogram to active controller
                               |
          priority-first, supply-aware per-card demand allocation
          floor map + reserved observations + cost
                               |
       exact-card scale-up/down decisions and UI/status breakdown
```

## Implementation milestones

### Milestone 1 - Exact accelerator identity and control-plane schema

SkyPilot changes:

- In `sky/serve/service_spec.py` and `sky/utils/service_schema.py`, add `min_replicas_by_accelerator`, canonical exact-card validation, the one-GPU-count-shape-per-card guard, and serialization/backward-compatible defaults.
- Add a shared SkyServe exact accelerator registry derived from the active task resources. It maps case-insensitive wire tokens to canonical display IDs but exposes no family/prefix matcher.
- Persist explicit zero-cost/reserved-supply provenance on every replica placed on a reserved zero-cost location, whether it was launched for ordinary demand or proactive fill. Do not infer this from `reserved_fill`, which describes why the replica was launched rather than where it runs.
- Extend `sky/serve/constants.py` with the compatibility header name, version, and size/count bounds.
- Extend autoscaler/controller status types in `sky/serve/autoscalers.py`, `sky/serve/controller.py`, `sky/serve/serve_utils.py`, and API schemas with additive per-card maps while preserving existing aggregate fields for old clients.
- Add unit tests proving `A100`, `A100-80GB`, and differently cased spellings have the intended equality boundaries; test invalid maps, floor/max conflicts, serialization, and old service YAML.

Acceptance gate:

- Existing single-card services produce identical behavior and status.
- A configuration containing both A100 variants preserves two exact registry entries, two floor keys, and two status rows.

### Milestone 2 - Compatibility-aware admission and routing

SkyPilot changes:

- In `sky/serve/load_balancer.py`, parse/default/validate the header at admission, store canonical exact-card compatibility on `_RequestQueueWaiter`, and strip it before forwarding.
- Replace the single global-head grant loop with one authoritative waiter registry plus exact-card secondary indexes. Run the bounded priority-tier/profile-to-card matcher from a consistent supply snapshot and keep atomic cross-index removal/cancellation.
- Use existing `LoadBalancingPolicy.select_replica(..., eligible=...)` support to restrict dispatch to URLs whose `replica_info.gpu_type` is an exact compatible card.
- In `sky/serve/load_balancing_policies.py`, centralize exact-card URL lookup while retaining instance-aware least-load selection among eligible replicas.
- Include the persisted zero-cost provenance in controller-to-LB `replica_info` and use it only as the final ready-replica cost preference after the compatibility matcher has protected constrained demand.
- Add card-specific reservation transfer/requeue behavior for proxy failures and cancellation cleanup.
- Add bounded compatibility-set queue metrics and the capability/configured-card fields to the LB capacity endpoint.

Tests:

- Extend `tests/unit_tests/test_serve_request_queue.py` for default-all, invalid headers, exact A100 separation, no incompatible dispatch, no new-arrival bypass, cancellation cleanup, and one-grant-only behavior for flexible waiters.
- Add deterministic concurrency cases: 1000 same-priority flexible L4/A100/H100 waiters, then an A100-only waiter; the constrained waiter gets the next A100 slot, L4 continues serving flexible work, and no running request is interrupted.
- Test numeric dominance separately: priority-50 flexible remains ahead of priority-20 A100-only for an A100 slot.
- Test the crossed two-card case: `{L4,A100}` and `{A100,H100}` use A100/H100 when L4 is unavailable and H100 is ready; with only A100 and equally unavailable alternatives FIFO wins; with paid L4 versus paid H100 fallback, assign A100 to the request avoiding the worse fallback and target the cheaper cold card for the other.
- Test replica failure requeue preserves original FIFO sequence and never leaks occupancy.

Acceptance gate:

- Every proxied request reaches only an exact compatible replica.
- The queue remains one ownership domain, and flexible requests are neither duplicated nor lost under concurrent card releases.

### Milestone 3 - Per-card autoscaling and reserved-capacity allocation

SkyPilot changes:

- Extend LB-to-controller request history with the bounded `(numeric priority, compatibility bitmap)` histogram and exact-card in-flight counts. Keep the payload sparse and make the active authority the sole reporter.
- In `sky/serve/autoscalers.py`, add a deterministic, sticky priority-first supply-aware allocator that converts demand profiles into `demand_target_by_accelerator` using each exact card's request-rate target and the same marginal fallback ranking as admission.
- Update autoscaler decisions to carry exact accelerator resource overrides on ordinary demand scale-up, not only reserved-fill scale-up.
- Make scale-down exact-card-aware and enforce both aggregate and per-card hard floors under the existing graceful delays.
- In `sky/serve/reserved_capacity.py`, `sky/serve/reserved_capacity_broker.py`, and `sky/serve/replica_managers.py`, expose exact-card free supply, split grants by exact pool, prefer zero-incremental-cost compatible supply, and keep fill targets separate from demand targets.
- Mark both demand-launched and fill-launched replicas as zero-cost when their selected exact location is in the current reserved-capacity set, persist that marker across controller restarts, and clear/recompute it only from authoritative placement metadata—not a stale fill reason.
- Preserve sticky assignments to ready/provisioning cards across control loops and add hysteresis around card reassignment so transient snapshots do not churn L4/A100/H100 targets.

Tests:

- Extend `tests/unit_tests/test_instance_aware_autoscaler.py` and `tests/unit_tests/test_reserved_capacity_fill.py` with empty-fleet cheapest selection, already-ready larger-card selection, healthy-provisioning capacity preventing duplicate launch, timed-out provisioning triggering replanning, free-reserved-before-paid residual scale-out, constrained demand reserving/scaling its exact card, crossed-set fallback-cost allocation, and no double-count of flexible demand.
- Cover global min greater than floor sum, floor sum greater than calculated demand, max-replica saturation, per-card graceful scale-down, optional fill enabled/disabled, and reserved physical machines remaining after serving replicas drain.
- Preserve and expand the existing A100/A100-80GB reserved-pool separation tests.
- Test that demand and fill replicas on reserved infrastructure both advertise zero-cost provenance to the LB, while a paid replica and a replica with unknown/stale provenance do not receive reserved-first preference.
- Add controller/LB synchronization tests for service-version changes and stale reserved observations.

Acceptance gate:

- `A100 min=0` can still use an already-ready or free-reserved A100 without forcing a paid L4 launch.
- When compatible demand disappears, paid serving replicas drain toward per-card floors; any remaining fill replicas/physical reserved capacity are explicitly attributable to the fill overlay.

### Milestone 4 - boltz-platform exact compatibility propagation

boltz-platform changes:

- In `packages/common-backend/src/compute/compute.types.ts`, add `a100-80gb` as its own closed `Hardware` leaf and update cost/latency/VRAM catalogs and `MODEL_HARDWARE_LADDER` entries where that card is actually offered. Never express compatibility through `HardwareGroup` regexes.
- Add optional `compatibleHardware?: readonly Hardware[]` to the user-owned Boltz prediction/compute submit contract and thread it through `Boltz2SubmitInput`, workflow/retry state, `ComputeJobOptions`, and provider dispatch without replacing the existing concrete deployment identity.
- Validate the user-owned value globally as a non-empty, duplicate-free list of exact closed `Hardware` leaves. Omission stays omission so each selected backend applies its established default.
- Add the exact allowlist to platform placement resolution. Before priority-affinity/cost/load ordering, `resolveDispatchCandidates` removes every concrete non-SkyPilot deployment whose declared `hardware` is not in the set. For each SkyPilot fleet candidate, intersect the global set with that service's advertised exact cards; remove the candidate only when the intersection is empty, and attach the non-empty candidate-local intersection to that dispatch attempt. Thus global `[A100,H100]` can use an A100-only fleet without sending it an unknown H100 token. Capacity-shed retries reuse the validated global allowlist and recompute the candidate-local intersection; they may change provider but never escape to a disallowed card.
- If an existing explicit `capability?: ComputePool` selects a truly concrete provider/card outside `compatibleHardware`, reject the request as contradictory. If the capability's cell is only a legacy alias for one multi-card SkyPilot service endpoint, treat it as selecting that service, not as pinning the cell's card; require a non-empty advertised intersection and let the compatibility allowlist govern exact replicas behind the endpoint.
- In `packages/common-backend/src/compute/providers/skypilot.provider.ts`, map the selected candidate's exact intersection to `X-SkyServe-Compatible-Accelerators`, preserve the global set across idempotent retries/capacity shedding, and send a freshly resolved candidate-local set only when the capacity capability version is 1. If the array is explicit and capability version 1 is absent, fail closed as unsupported/no-capacity for that fleet instead of retrying without the constraint. Continue mapping high/low to 50/20.
- Treat a multi-card SkyServe service as one provider deployment and one DAFQ/admission resource. Do not manufacture several platform pools pointing at the same LB URL; card choice belongs to the compatibility-aware SkyServe scheduler, which can see ready and reserved supply.
- Keep upstream platform admission aggregate and priority-aware only; it must not create a second per-card queue or make card-specific grants for the SkyPilot fleet. SkyServe is the sole card-level compatibility queue. For a SkyPilot multi-card deployment, the resolved platform pool selects the service, while `compatibleHardware` constrains exact replicas inside that service. Other providers retain concrete-card placement but are eligible only when that exact card is allowed.

Tests:

- Update `compute.types`, placement/catalog, deployment-validation, router, and SkyPilot provider tests for the distinct `a100` and `a100-80gb` leaves.
- Verify header omission, exact serialization, validation, priority plus compatibility together, retry preservation, global-to-candidate intersection, default-all behavior against an old LB, exact-compatible spill to another provider, explicit-subset fail-closed behavior when no capable candidate remains, true concrete-capability conflicts, multi-card SkyPilot cell aliases, and no duplicate per-card platform admission queues.
- Add integration-shaped tests whose capacity payload advertises L4/A100/A100-80GB/H100 and proves each in-service subset is passed unchanged and exact, plus a partial-overlap case that sends only the exact candidate-local intersection.

Acceptance gate:

- A platform caller can constrain one request to A100/H100 and another to L4/A100/H100 while both use the same SkyServe service and priority system; no provider retry may leave those exact sets.
- An old SkyServe LB never receives a compatibility header it cannot enforce, and an explicit subset is never widened to default-all.

### Milestone 5 - Status, metrics, and dashboard visualization

SkyPilot changes:

- Extend `/autoscaler/info`, LB capacity hints, service status, and Prometheus metrics additively with:
  - `demand_target_by_accelerator`
  - `min_replicas_by_accelerator`
  - ready/provisioning/live counts by accelerator
  - `fill_target_by_accelerator`
  - `free_reserved_slots_by_accelerator`
  - queued/in-flight counts by exact compatibility set (bounded labels; do not emit unbounded raw header values)
- Keep existing aggregate `target_num_replicas`, current/max/in-flight capacity, and old dashboard fields intact.
- In `sky/dashboard/src/data/connectors/services.jsx` and `sky/dashboard/src/pages/services/[service].js`, retain the global `ready / total (target: N)` summary and add an exact-card table:

| Card | Ready | Provisioning | Demand target | Hard floor | Fill target | Free reserved slots |
|---|---:|---:|---:|---:|---:|---:|
| L4 | ... | ... | ... | ... | ... | ... |
| A100 | ... | ... | ... | ... | ... | ... |
| A100-80GB | ... | ... | ... | ... | ... | ... |
| H100 | ... | ... | ... | ... | ... | ... |

- Add tooltips explaining that demand target sizes traffic, floor is a hard serving minimum, fill is optional zero-cost extra serving capacity, and free reserved slots are physical supply not yet represented by a serving replica.

Tests:

- Add status/API schema tests and dashboard connector/component tests for missing additive fields, totals, fill overlays, and separate A100 rows.
- Assert that the displayed global demand target equals the per-card demand-target sum, while actual/fill capacity may be larger.

Acceptance gate:

- An operator can explain every replica above the demand target as either a hard floor, provisioning lag, or reserved fill; A100 variants are never visually combined.

### Milestone 6 - Rollout and production validation

1. Land and deploy SkyPilot schema/status changes with behavior disabled by absence of the new header/map.
2. Land compatibility queue and per-card autoscaling behind a service flag; deploy to a test multi-card fleet.
3. Verify the capacity endpoint advertises compatibility version 1 and distinct configured cards.
4. Enable per-card floors and reserved integration in test, then run cold, warm, reserved, saturation, retry, and service-update scenarios.
5. Deploy the SkyPilot release to production and verify its exact release/health/capability response.
6. Land/deploy boltz-platform propagation only after production advertises version 1.
7. Enable platform caller/UI selection gradually, starting with omission/default-all and then explicit subsets.

Production checks:

- Cold fleet + flexible request starts the cheapest compatible paid card.
- Ready reserved A100 + flexible L4/A100/H100 request uses A100 without launching L4.
- Ready reserved A100 + ready paid L4 + flexible L4/A100 request uses A100 when no constrained peer needs it, allowing L4 to become idle; with an equal-priority A100-only peer, match that peer to A100 and the flexible request to L4.
- Large flexible backlog + same-priority A100-only request gives the next A100 slot to the constrained request with no preemption.
- High flexible versus low constrained demonstrates numeric priority dominance.
- `A100` traffic never reaches `A100-80GB`, and vice versa, unless both are explicitly in the compatibility set.
- Card-specific queue depth, target, floor, fill, free reserved slots, and actual replica counts reconcile in API metrics and UI.
- Removing demand causes graceful per-card scale-down; reserved physical capacity or configured fill remains clearly reported as extra supply.

## Failure handling and invariants

- Fail closed on unknown or empty explicit compatibility. Never silently widen an invalid subset to all cards.
- Missing is the only path that means all configured cards.
- A waiter has exactly one lifecycle state and at most one granted card reservation.
- Sum of compatibility-group demand is total demand; it is not multiplied by compatible-card count.
- Sum of demand targets by card equals the aggregate demand target. Fill is an overlapping overlay, not added demand.
- Per-card floors are hard for serving replicas; reserved physical machines are supply and do not satisfy a serving floor until a replica is launched.
- All comparisons use exact canonical IDs. No code path may use `startswith`, hardware regex groups, generic `A100*`, or suffix stripping for compatibility.
- If exact-card telemetry is missing or stale, do not route or scale on a guessed family. Mark the replica/card unknown, exclude it from compatibility grants, and surface degraded status.
- If no compatible resource can be provisioned before queue timeout, return the existing retryable no-capacity result with the requested exact set in structured diagnostics.

## Alternatives rejected

1. **One queue/service per card.** This duplicates flexible requests, creates cross-queue races, and prevents the scheduler from using live/reserved supply coherently.
2. **Platform chooses one card before SkyServe.** The platform cannot see replica occupancy or exact reserved slots at dispatch time, so it would cold-start cheaper hardware while compatible larger hardware is already paid for and ready.
3. **Always choose the first compatible card.** List order is a weak proxy for marginal cost and ignores already-ready/reserved supply.
4. **Always choose cheapest, including warm capacity.** This causes unnecessary cold launches and latency while compatible capacity is idle.
5. **Card-family matching.** This violates the explicit A100/A100-80GB isolation requirement and makes floors, capacity, and billing irreconcilable.
6. **Treat reserved physical machines as hard serving floors.** A physical slot without a ready SkyServe replica cannot immediately serve a request; conflating the layers hides provisioning work and scale-down behavior.
7. **Preempt flexible work when constrained work arrives.** Explicitly out of scope; priority affects the next grant and future scaling only.

## Manual test plan

Use a test SkyServe service configured with exact L4, A100, A100-80GB, and H100 resources and distinct per-card QPS targets.

1. Start with zero serving replicas and no free reserved capacity. Submit a default/missing-field request and confirm the cheapest compatible paid card is targeted.
2. Expose a free reserved A100 slot (and no ready replica), submit the same request, and confirm the exact A100 resource override is selected before paid L4.
3. Keep a reserved A100 replica and a paid L4 replica ready with spare concurrency. Submit only a flexible L4/A100 request and confirm reserved A100 dispatch. Then add an equal-priority A100-only request and confirm the matcher assigns A100-only to reserved A100 and flexible to paid L4.
4. Fill all A100 slots with flexible requests, queue an older same-priority flexible request and then an A100-only request, release one A100 slot, and confirm A100-only runs next. Confirm existing work was not interrupted.
5. Repeat with flexible priority 50 and A100-only priority 20; confirm the priority-50 request runs first.
6. Queue equal-priority `{L4,A100}` and `{A100,H100}` requests. With L4 unavailable and A100/H100 ready, confirm the maximum matching uses A100/H100. With only A100 and no viable alternatives, confirm FIFO. Then expose paid L4 versus paid H100 fallbacks and confirm the ready A100 avoids the worse fallback while L4 is the cold target.
7. Submit explicit `A100` and `A100-80GB` requests and confirm exact isolation. Submit both together and confirm either exact type is allowed.
8. Start a compatible replica provisioning and confirm it prevents a duplicate launch without receiving traffic; exceed/fail its startup SLA and confirm reserved/paid residual capacity is replanned.
9. Set A100 floor zero. Remove A100 demand and confirm paid replicas drain after grace; then repeat with reserved fill enabled and confirm any retained replica is shown as fill, while physical reserved capacity remains separately visible.
10. Set distinct floors for all cards, drive mixed demand, and reconcile global target, per-card targets, actual states, fill targets, and free reserved slots through API, metrics, and dashboard.
11. Change the active service version while requests are queued; confirm compatible waiters retain sequence and an emptied intersection receives retryable 503.
12. Point the platform test client at a pre-capability LB: confirm an omitted field keeps default-all behavior, an explicit subset may spill only to an exact-compatible concrete provider, and otherwise fails closed. Point it at version 1 and confirm each selected fleet receives only its exact non-empty intersection plus the numeric priority header on every retry.

## Completion criteria

- All milestone acceptance gates and the manual test plan pass.
- SkyPilot unit suites for queueing, instance-aware autoscaling, reserved fill, service schema/status, and dashboard pass after `bash format.sh` on changed files.
- boltz-platform typecheck/lint/unit suites for compute types, routing, placement, SkyPilot provider, and retries pass using the repository's `mise` environment.
- The exact deployed SkyPilot release advertises capability version 1 before platform explicit subsets are enabled.
- Production observability can distinguish demand, hard floors, active reserved fill, and unconsumed reserved supply per exact accelerator.
