# Drain proof across load balancer restarts

- **Status:** designed, not implemented
- **Last updated:** 2026-07-25
- **Milestones:** M0 observability, M1 watchlist + explicit-idle hardening,
  M2 blind capacity view, M3 docs, M4 (separate) reduce LB roll frequency
- **Blocks:** `serve-reserved-fill-utilization-gate.md`. The gate's release
  progress is repeatedly reverted on this cluster until M1 and M2 land.

Canonical location: `docs/designs/serve-drain-proof-across-lb-restarts.md`. All line numbers are at the deployed SHA `a0028d62c7be576a97937d8fe7471bfa7c019849` (SkyPilot 1.1.807); read with `git show a0028d62c7be576a97937d8fe7471bfa7c019849:sky/serve/<file>.py`.

## Problem

A retiring replica is taken off the route, then must prove it is drained before the controller tears it down. The proof is `_ReplicaDrainTracker` (`replica_managers.py:700-801`), which requires SEEN-THEN-CLEAN within one load-balancer incarnation:

```
# replica_managers.py:777-801
if session != self._session:
    self._session = session
    self._seen = False
    self._unknown_tainted = False
if (url in routing_urls or url in unknown_urls or
        url in draining_urls or url in in_flight):
    self._seen = True
...
return self._seen and not blocked
```

`session` is `lb_session_id`, which `_get_lb_session_id()` (`load_balancer.py:1897-1907`) reads from `constants.LB_POD_UID_ENV_VAR` (`constants.py:115`), injected by the Downward API `fieldRef: metadata.uid` (`lb_k8s.py:1072-1078`). A new LB Pod means a new session, which resets `_seen`.

**A new session can never re-acknowledge an off-route url.** All four acknowledgement sets are structurally unreachable:

- `routing_urls = list(policy.ready_replicas)` (`load_balancer.py:1966`), fed from `replica_info` (`load_balancer.py:3413, 3507-3512`), which `_get_lb_replica_info` filters to `status == READY and version in active_versions` (`controller.py:531-535`). A retiring replica is `SHUTTING_DOWN`.
- `draining_urls = list(self._draining_clients or {})` (`load_balancer.py:3328`), populated only when *this* process pruned a url it was proxying. Cold: empty.
- `in_flight` is `policy.snapshot_in_flight()` (READY-scoped, `load_balancing_policies.py:233-242`) plus the draining fold (`load_balancer.py:1994-1998`) plus this round's occupancy fold (`2004-2008`). Cold: empty.
- `unknown_urls` is derived from `capable = self._occupancy_capable` (`load_balancer.py:1956, 2019-2028`). `_occupancy_capable` grows from `declared_async_urls` (`3464-3500`, sourced from `replica_info`, excludes the replica), from probe inference (`3112-3119`, but `probe_urls = set(ready_urls) | _occupancy_capable` at `2958-2960`, so it is circular), and from dispatch (`2280-2287`, impossible off-route).

So `_seen` stays `False` forever and the drain runs to its full `graceful_drain_seconds`. For `protenixv2-hybrid-v1` that is 7200 s, the schema maximum, coupled to `LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS = 7200` (`constants.py:300`).

### The termination decision, and the gate that fires before it

`_refresh_wait_for_idle` (`replica_managers.py:5839-5995`) has two paths.

Physical (`5975-5995`): `if not drained and not deadline_expired: continue`, then `drain_cap = 0` if drained else `status_property.drain_cap_seconds`, then `_terminate_replica(..., in_flight_drain_cap_seconds=drain_cap)`. A proven drain terminates immediately; an unproven one waits out the cap inside `_wait_for_drain` (`621-656`, polled every `_DRAIN_POLL_SECONDS = 2`).

Logical (`5912-5974`), which is protenix's path (`spot_placer: dynamic_fallback_per_gpu` implies `uses_logical_replicas`): **the drain proof is consulted only after a capacity-coverage gate.**

```
# replica_managers.py:5928-5934
with self._logical_state_lock:
    retirement_state = self._logical_retirement_state(info)
    if retirement_state == 'abort':
        self._abort_logical_retirement(info, ...)
        continue
```

`_logical_retirement_state` (`5062-5134`) aborts at `5126-5130` when `not ready_covers_target`, and `_logical_ready_capacity` (`5136-5161`) skips every candidate where

```
# replica_managers.py:5154-5158
observed = snapshot.observed_slots_by_replica_id.get(candidate.replica_id)
if (observed is None or candidate.replica_id in snapshot.unknown_replica_ids):
    continue
```

A restarted LB zeroes that sum two independent ways:

1. `observed_slots = self._translate_observed_slots(request_data['total_slots_by_url'])` (`controller.py:693-703, 1223`). `_replica_total_slots` is only written by a probe round (`load_balancer.py:3089-3093`), so a fresh LB ships `{}` for its first one or two syncs.
2. During the maxSurge overlap of a non-HA rollout, `force_all_live_unknown=(not drain_authoritative and not ha_enabled)` (`controller.py:1073`) is unconditionally `True`, and `_unknown_async_replica_ids` then marks **every live replica** unknown (`controller.py:783-788`). protenix's LB Deployment is `RollingUpdate {maxSurge: 1, maxUnavailable: 0}` and it is non-HA, so this fires on every roll.

The blind snapshot is still published (`controller.py:1132-1139`) because the new Pod is `demand_authoritative` (sole Ready non-terminating) while the old Pod is still `live`. `_abort_logical_retirement` (`5757-5782`) then clears `sky_down_status`, `is_scale_down`, `drain_cap_seconds`, `drain_started_at`, `wait_for_idle_before_termination` and every `logical_retirement_*` field, and returns the replica to routing. The same conflation drives `_reconcile_recovering_logical_retirements`, which reactivates up to `_LOGICAL_RETIREMENT_RECOVERY_MAX_REACTIVATIONS_PER_GENERATION = 20` victims per snapshot generation on `ready_capacity < current_target` (`5615-5688`).

### Blast radius

Measured 2026-07-25:

- protenix: 78/78 READY; autoscaler target `{'A100-80GB': 1, 'L4': 1}`. About 76 replicas are reclaim candidates.
- protenix's LB Deployment was created `2026-07-23T18:04:10Z` and was at `deployment.kubernetes.io/revision: 47` at `2026-07-25T11:59Z`: **46 rollouts in 41.9 h, mean interval 55 min.** (Counting visible ReplicaSets undercounts ~20x, because `spec.revisionHistoryLimit: 10` prunes them.) boltz-l4-fleet's two slots were at revisions 110/111 since `2026-07-18T06:22`, roughly 15 rolls/day/slot.
- The API server Pod was recreated at `11:57:47Z` and every LB Pod followed at `11:59:28Z-12:02:12Z`. The LB pod template pins `skypilot.co/controller-image-digest`, so **every control-plane deploy rolls every LB and restarts every controller**.

Two distinct costs, and it matters which is which:

| path | what happens on an LB roll today | cost |
|---|---|---|
| logical (protenix reclaim) | blind window ~15-40 s vs a 20 s `_thread_pool_refresher` tick (`replica_managers.py:61, 7208-7221`): at least one tick lands inside with near certainty, so `_logical_retirement_state` returns `abort` for every non-committed, non-recovering retirement | the **whole wave is undone**; each replica loses its elapsed drain and must re-select and re-drain from a fresh 7200 s budget. A 76-replica batch retired in one tick has all 76 drains open, so one roll reverts all 76. |
| physical (`_terminate_replica` with a positive cap, tracker at `4854`), and any logical retirement whose roll misses the blind ticks | `_seen` never returns; `_wait_for_drain` sleeps to the deadline | 7200 s per replica = 2 A100-h each |
| co-restart (the observed event) | `_logical_controller_epoch = uuid.uuid4().hex` is regenerated (`1962`, `4508`), so every uncommitted logical retirement enters `_recovering_logical_retirement_ids` (`2949-2952`); `_refresh_wait_for_idle` `continue`s at `5921-5927` and `_reconcile_recovering_logical_retirements` decides on the same blind snapshot | reactivation of up to 20 per generation until the wave is undone |

At a 55 min mean roll interval, any reclaim wave lasting more than about an hour is expected to span at least one roll. That is the sense in which the reclaim feature is "effectively inert": not because it waits 7200 s, but because **its progress is repeatedly reverted**, and the minority of drains that survive the revert do wait 7200 s.

No `Strict idle wait ... deadline` or `reported drained after` line exists in the retained protenix controller logs (`controller.log` + `.log.1` cover `09:16Z-11:55Z`, a window with no retirements), so this model is derived from code, not calibrated against production counters. See Milestone 0.

## Why it is not just conservatism

Two commits made the current behaviour, and they were right about the property and wrong about the cost.

`7d69d531664bd987cd66e1b7202dfba9660676d2` (`[Serve] In-flight-aware graceful drain for replica retirement (#116)`, 2026-07-09) states the safety property explicitly: "a cold-restarted LB can prove nothing", "Every degraded condition (old LB, no report, staleness, unknowns) falls back to the deadline -- never to an early kill." At that time the cap was "validated 0..3600 ... default 120", so the worst case of the accepted fallback was one hour and the normal case two minutes.

`cd3b82100f` (`[Serve] Crash-durable drain policy; lift the drain cap for hour-scale jobs`) raised the ceiling and `LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS` from 3600 to 7200, reasoning that "The retention cost is only probing non-answering off-ready urls longer." That analysed the retention, not the fallback, and doubled the fallback's price without revisiting it.

`b36b15626505f235bb63669fc12a061d66af717a` (`[Serve] Avoid idle drain deadline stalls`, 2026-07-20) added `_seed_from_existing_report` and deliberately preserved the reset, documenting in `docs/designs/serve-demand-aware-scaling-ramp.md` that "a session change clears the seed". That phrasing implies a later same-session report can still arrive. It cannot: after a session change, the url is unreachable in all four acknowledgement sets **permanently**.

The distinction that matters:

- **Deliberate and correct:** never let a cold LB falsely prove a drain. Async work may genuinely still be running; a false proof kills real user jobs. This design does not relax it by one term.
- **Accidental:** the cold LB is *structurally prevented from obtaining* proof it could legitimately get. The proof is a first-party HTTP fact, not a memory of the dead process. `_fetch_replica_occupancy` (`load_balancer.py:2916-2934`) POSTs `{"action": "async_capacity"}` to `{replica_url}/v1/models/model:predict` and reads `running_count` straight from the replica's own router (`local_async_router.py:568, 634`). A fresh LB can obtain that answer in one probe interval. It just never learns which url to ask, because `_occupancy_capable` is rebuilt only from `replica_info`, which by construction excludes the replica being drained. The LB already probes off-ready capable urls on purpose (`load_balancer.py:2945-2960`, `3128-3147`). We are restoring that machinery's input, not adding a mechanism.
- **Also accidental:** treating "I cannot prove this replica is idle" as "this replica provides no capacity". `unknown_replica_ids` is a drain-proof predicate (its comment at `controller.py:1092-1099` says so: "Keep those backends drain-busy"), and `_logical_ready_capacity` reuses it as a capacity predicate. A READY replica behind a blind LB is still in the Kubernetes Service and still serving; it is unobservable, not absent. Reading unobservable as absent turns a 30 s observability gap into a fleet-wide capacity collapse, and the response to a capacity collapse (reactivate every victim) is exactly wrong when the collapse is an artifact of observation.

The asymmetry that licenses the whole design, verified in both halves:

- **Async occupancy is recoverable.** It lives in the replica, is exposed over HTTP, and is answerable to any process that knows the url.
- **Synchronous proxied in-flight is not recoverable, and does not need to be.** It lives in per-replica `httpx.AsyncClient` objects refcounted via `_INFLIGHT_ATTR` (`load_balancer.py:48, 4064-4086`). The clients and the client-facing sockets die with the process, so after a restart there are by construction zero surviving proxied requests. And while a warm peer is still alive, the controller refuses to publish anything the fresh Pod claims: non-HA drain authority requires `pod_authority.live_uids == {session_id}` (`controller.py:856`), and a merely-Ready overlap reporter publishes the deliberately blocking `({}, None, [], [])` (`controller.py:1165-1170`), whose `routing_urls=None` makes `__call__` return `False` at `replica_managers.py:771-772`.

## Behavior contract

Numbered, testable.

1. **Cold-incarnation invariant.** A report whose `lb_session_id` differs from the one that acknowledged the url can complete a drain only via an *explicit idle entry*: `url in in_flight and in_flight[url] == 0`, where that zero was produced by this LB process's own occupancy fold (`load_balancer.py:2004-2008`) from a current-round, generation-valid, role-valid, off-ready sample. Absence of the url from every set never completes a drain under a new session.
2. **Async replicas: absence is never proof.** For a replica whose version declares `graceful_drain_async_occupancy: true`, `_ReplicaDrainTracker` returns `True` only on an explicit idle entry, never on absence, in any session, warm or cold.
3. **Sync-only replicas are unchanged.** For a replica whose version does not declare async occupancy, the predicate is byte-identical to today, including the absence path.
4. **Advertisement scope.** The controller advertises a url in `drain_watchlist` if and only if: it is absent from `replica_info` in the same response; its replica row shows an open drain with `_remaining_drain_seconds(drain_started_at, drain_cap) > 0`; and it is async-occupancy-capable (spec flag `true` for the replica's version, or the LB has reported it in `occupancy_sampled_urls` during this controller incarnation).
5. **Advertisement never routes.** A watchlisted url is never added to `ready_replicas`, `_replica_info_by_url`, `_replica_total_slots` or `_replica_free_slots`, and never becomes an async-capability *declaration* (`_occupancy_declared_urls`, `_occupancy_explicitly_disabled_urls`, `_occupancy_disable_pending` are untouched).
6. **Monotonicity.** Adding a url to the watchlist can only (a) cause more probing, (b) cause the url to appear in `unknown_in_flight_urls`, which is blocking and taint-setting, or (c) admit a first-party probe answer. It can never convert a previously blocked drain into a proof by absence.
7. **Bounded advertisement.** The controller stops advertising when the drain budget is exhausted or the tracker is gone; independently, the LB drops the retention pin at the advertised remaining-seconds horizon, so a silent controller cannot pin a url forever. Advertisement is capped at `LB_DRAIN_WATCHLIST_MAX_URLS = 512`, ordered by soonest deadline, and truncation is logged.
8. **Blind capacity is not evidence.** A logical capacity view is *blind* when the controller applied `force_all_live_unknown`, or when at least one eligible ready candidate exists and none of them contributed observed capacity. A blind view does not produce `abort` from the coverage check (`replica_managers.py:5126-5130`) and does not trigger recovery reactivation (`5626-5688`) for at most `_LOGICAL_CAPACITY_BLIND_GRACE_SECONDS`; past that grace, today's behaviour resumes exactly.
9. **Blindness suppression never withholds capacity growth.** It suppresses `abort` and reactivation only. No launch, scale-up or target computation is changed.
10. **Deadline is still the outer bound.** Nothing here extends any drain past `graceful_drain_seconds`; the deadline branches at `replica_managers.py:5935-5965` and `5975-5995` are untouched.
11. **Additive wire.** A new controller with an old LB, and a new LB with an old controller, both behave exactly as the deployed system. No `API_VERSION` bump (this is the internal controller-to-LB channel).

## Mechanism

Three parts. **A** is the fix; **B** is a hardening that makes A safe to rely on and closes two latent false-proof holes; **C** is the repair without which A is inert for protenix.

### A. Controller-advertised drain watchlist

**A1. Manager keeps a url cache for open drains.** `_ReplicaDrainTracker.__init__` (`replica_managers.py:726-734`) already stores the resolved url. Add a read-only `replica_url` property, and route both construction sites through one factory `_new_drain_tracker(info, replica_url, drain_started)` that also writes `self._drain_proof_urls[replica_id] = replica_url`. The two sites are `4854-4855` (physical drain, tracker handed to `terminate_cluster`) and `5012` (`_register_wait_for_idle`, the strict/logical path). A factory is required so a third site cannot be added without registering.

The cache is a *url memo only*; it is never the authority for whether to advertise. That matters because a missed removal must not produce a stale advertisement.

**A2. Manager computes the advertisement from durable row state.**

```python
def drain_proof_watchlist(
        self,
        replica_infos: list[ReplicaInfo],
        async_occupancy_by_version: dict[int, bool | None],
        routable_urls: set[str]) -> dict[str, str]:
```

It walks the `replica_infos` list **the sync handler already loaded** (`controller.py:1201-1203`, via `_snapshot_replica_occupancy` at `2456-2481`), so there is no new DB read and no url re-resolution (`info.url` is a cluster-record read plus a YAML parse per replica, which the recovery path already batches at `2909` precisely to avoid this). For each row it requires:

- `_is_valid_drain_started_at(status.drain_started_at)` (`replica_managers.py:659-661`) and `_remaining_drain_seconds(drain_started_at, drain_cap) > 0` (`690-697`);
- an open drain marker: `wait_for_idle_before_termination is True`, or `logical_retirement_version is not None`, or `sky_down_status is not None`;
- a url from `self._drain_proof_urls`; if absent, skip (never resolve here);
- `url not in routable_urls`;
- async capability: `async_occupancy_by_version.get(info.version) is True`, **or** `url in self._lb_observed_occupancy_urls`.

Returns `{url: str(int(remaining_seconds))}`, ordered by soonest deadline, truncated to `LB_DRAIN_WATCHLIST_MAX_URLS`.

The async filter is load-bearing, not decoration. Putting a sync-only url into `_occupancy_capable` makes every probe miss read as `unknown` (`load_balancer.py:2019-2028`), which is permanently blocking (`replica_managers.py:787-800`). That would make sync-only drains *slower* than today. Default to exclusion whenever the flag is `None` (spec row missing, or simply unset).

`_lb_observed_occupancy_urls` closes the coverage gap for replicas that are async-capable in reality but do not declare it: the LB discovers such urls by probe inference (`load_balancer.py:3112-3119`). The controller records `url -> monotonic` for every url the LB reports in `occupancy_sampled_urls` on an authoritative sync, and prunes it to `replica_info` keys plus the current watchlist. **Only `occupancy_sampled_urls`**, never `unknown_in_flight_urls`: a sampled url has actually answered a probe, which proves the image implements the action; an unknown url has not, and `newly_disabled` sync urls also appear there (`load_balancer.py:3494-3496`). This map is in-memory and dies with the controller, so in the co-restart case only spec-declared replicas are covered. That is the honest limit, and the operator-facing recommendation is to declare the flag.

**A3. Controller ships one additive response key.** In `_handle_load_balancer_sync`'s `response_content` (`controller.py:1361-1373`):

```python
try:
    response_content['drain_watchlist'] = (
        self._replica_manager.drain_proof_watchlist(
            replica_infos, async_occupancy_by_version,
            set(lb_replica_info)))
except Exception as e:
    logger.warning(...)   # omit the key; never fail the sync
```

Shape mirrors `replica_info`: `dict[str, dict|str]` with string values (`controller.py:604-625`). The seconds are **relative**, so LB/controller clock skew cannot lengthen a pin.

Concurrency: `drain_proof_watchlist` reads `self._drain_proof_urls` and the passed-in list. `_wait_for_idle_trackers` is a plain `dict` mutated under `@with_lock` by the refresher (`replica_managers.py:5857, 5883, 5903, 5990, 5532, 5782, 5837`), so any iteration over manager dicts from the sync handler thread must snapshot first: `dict(self._drain_proof_urls)`, the same defensive idiom already used at `replica_managers.py:5846` and `6674`. Do **not** take `self.lock`: `_refresh_thread_pool` holds it across per-replica DB writes and an inline SSH log sync, which would block the LB sync handler for a full refresh pass.

**A4. LB seeds `_occupancy_capable`.** Inside the existing `with self._client_pool_lock` block, immediately after the `declared_async_urls` union at `load_balancer.py:3496-3500`:

```python
raw = response_json.get('drain_watchlist')      # parsed at 3392 with replica_info
if raw is not None:
    now = time.monotonic()
    pins = {}
    for url, remaining in raw.items():
        if url in ready_replica_urls:
            continue
        if url in set(self._occupancy_explicitly_disabled_urls or ()):
            continue
        pins[url] = now + min(float(remaining),
                              constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS)
    self._drain_watchlist_until = pins           # empty map == release
    if pins:
        self._occupancy_capable = set(self._occupancy_capable or ()) | set(pins)
        off_ready = dict(self._occupancy_off_ready_since or {})
        for url in pins:
            off_ready.setdefault(url, now)
        self._occupancy_off_ready_since = off_ready
```

Absent key means *no information*: keep existing pins until they expire on their own. An empty map means *release now*. Nothing else is written; in particular `_occupancy_declared_urls`, `_occupancy_explicitly_disabled_urls` and `_occupancy_disable_pending` are untouched, so a watchlist entry can never resurrect a url mid `true -> false` two-phase disable, and `set_ready_replicas` (`3512`) is never called with it.

This must run **before** the `routing_spec is None` early return (`3420-3432`) and before the spurious-empty-sync return (`3433-3451`): seeding is monotone-conservative (contract 6), so it must not be skipped on degraded rounds. Both early returns happen before the lock is taken today, so the seed goes in its own short `with self._client_pool_lock` block placed just after `replica_info` is parsed.

**A5. LB pins retention.** At the probe-round pruner (`load_balancer.py:3148`):

```python
pinned = {u for u, until in (self._drain_watchlist_until or {}).items()
          if until > now}
keep = confirmed | set(self._draining_clients or {}) | retained | pinned
```

and drop expired entries from `_drain_watchlist_until` in the same pass, plus clear the map in the empty-`probe_urls` reset branch (`2966-2985`). The existing `LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS` TTL stays as the outer failsafe.

**A6. Everything downstream is existing machinery.** `probe_urls = set(ready_urls) | (self._occupancy_capable or set())` (`2958-2960`) now contains the url, so `_fetch_replica_occupancy` (`2916-2934`) probes it every `LB_OCCUPANCY_PROBE_INTERVAL_SECONDS = 10` with a 2 s timeout. A 200 whose body parses (`_parse_replica_occupancy`, `2889-2914`: `status in {READY, DRAINING}`, non-negative non-bool ints) and whose `running_count == 0` passes the off-ready fence (`3079-3087`: off-ready at round start *and* write time, which a fresh LB satisfies trivially because the url was never in its ready set), lands in `_occupancy_sampled_off_ready`, survives the current-round/dispatch-generation/role-epoch filters (`715-740`), and is folded in as an explicit `in_flight[url] = 0` (`2004-2008`). The tracker sees `explicit_idle`, clears the taint, `blocked = False`, `_seen = True`.

**No settle window is needed.** Design-1 proposed a 15 s `LB_DRAIN_SEED_SETTLE_SECONDS` fence against a probe zero on a never-before-seen url. It is unnecessary and it introduced a re-retirement staleness bug. The ordering already fences it: the only process that could dispatch to the url is one that still routes it, the url is off-route in every LB that has applied its removal, and while any *other* LB Pod is live the controller refuses to publish a proof at all (`controller.py:856, 1165-1170`). A request the dead process had on the wire either arrived (and is counted as a router reservation, `local_async_router.py:592-635`) or was reset when its socket closed.

### B. Absence is never proof for an async-declared replica

`_register_wait_for_idle` already resolves the replica's spec on this path via `_resolve_drain_cap_seconds` (`replica_managers.py:4982-4986` -> `4929-4956` -> `_get_version_spec`, `7894-7902`). Resolve `graceful_drain_async_occupancy` at the same place, with the same try/except-and-default shape, and pass it to the tracker as `requires_explicit_idle: bool` (default `False` on any resolution failure, i.e. today's semantics). Then in `__call__` (`replica_managers.py:793-801`):

```python
explicit_idle = url in in_flight and in_flight[url] == 0
...
blocked = (url in routing_urls or url in unknown_urls or
           in_flight.get(url, 0) != 0 or
           (self._unknown_tainted and not explicit_idle) or
           (self._requires_explicit_idle and not explicit_idle))
```

One added conjunct. It closes two real holes without touching session identity, `lb_ha.py`, or the HA state machine:

- **In-place container restart, same Pod UID.** `restartPolicy: Always` with an unchanged `metadata.uid` produces a cold process shipping empty overlays under an *unchanged* session, so `777` does not reset `_seen`. If the last warm report carried a positive `in_flight[url]` the taint stays `False` (only an explicit zero clears it, `794-797`), so `blocked` evaluates `False` and the replica is terminated with `in_flight_drain_cap_seconds=0` while async work runs. All 17 live LB Pods show `restartCount: 0`, so this has probably never fired, but the tracker's docstring claims a defence it does not have.
- **HA cutover narrowing.** `_publish_ha_drain_view` (`controller.py:1540-1590`) publishes `f'ha-generation-{state.generation}'` and aggregates over `stream_owner_ids` (`1554-1579`). When the DRAINING phase ends the owner set narrows from `{old warm, new cold}` to `{new cold}` with the generation unchanged, so `_seen` is not reset; the cold owner does not mention the url; `blocked` is `False`; the drain completes even though the warm owner's last report carried `running_count > 0`.

**Why this and not a per-process session nonce.** Making the session finer-grained (a `uuid4` per process, or folding `restart_count`, or an HA owner fingerprint) resets `_seen` on every process boundary. That over-corrects: for a **sync-only** replica the process boundary *destroys* the work, so absence after it is genuinely sound, and a sync-only url can never re-acknowledge (contract 3's population is exactly the population that has no recovery path). A finer session would therefore convert working sync-only drains into 7200 s deadlines, in HA on every cutover phase change, to close a hole that only exists for async work. Conjunct B closes the async hole precisely and leaves the sync-only path alone. And with A in place, an async-declared url is always in `_occupancy_capable` on every live LB, so it is always either explicitly idle or `unknown`; absence does not occur, and requiring explicit idle costs nothing.

### C. A blind capacity view is not evidence of a shortfall

**C1. Snapshot carries the fact the controller already knows.** `LogicalReconcileSnapshot` (`replica_managers.py:156-164`) gains `forced_all_live_unknown: bool = False` (frozen dataclass, defaulted, so all existing construction sites and tests are unaffected). `update_logical_reconcile_snapshot` (`2014-2030`) gains the matching keyword, passed at `controller.py:1134-1139` from the same expression already computed at `1073`.

**C2. Manager derives blindness from the walk it already does.**

```python
@staticmethod
def _logical_capacity_view_is_blind(replica_infos, snapshot, version,
                                    excluded_replica_ids) -> bool:
    if snapshot.forced_all_live_unknown:
        return True
    eligible = countable = 0
    for candidate in replica_infos:
        # identical eligibility filter to _logical_ready_capacity:5143-5153
        ...
        eligible += 1
        observed = snapshot.observed_slots_by_replica_id.get(candidate.replica_id)
        if observed is not None and candidate.replica_id not in snapshot.unknown_replica_ids:
            countable += 1
    return eligible > 0 and countable == 0
```

Called **only** when `not ready_covers_target`, so there is zero added cost on the common path.

**C3. Bounded grace.** New `_LOGICAL_CAPACITY_BLIND_GRACE_SECONDS = 6 * serve_constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS` (= 120 s), declared beside `_LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS` (`replica_managers.py:85`) whose value it matches, because it encodes the same idea ("evidence unavailable for this long stops being a blip"). `self._logical_last_sighted_capacity_at` is initialised to `time.monotonic()` in `__init__` (so a freshly restarted controller starts with a full grace, which is exactly what the co-restart case needs) and refreshed whenever a non-blind view is evaluated.

In `_logical_retirement_state` (`5126-5130`):

```python
if not ready_covers_target:
    if self._logical_capacity_view_is_blind(replica_infos, snapshot, version,
                                            excluded_ids):
        if (time.monotonic() - self._logical_last_sighted_capacity_at
                <= _LOGICAL_CAPACITY_BLIND_GRACE_SECONDS):
            return 'wait'
    else:
        self._logical_last_sighted_capacity_at = time.monotonic()
    return 'abort'
```

In `_reconcile_recovering_logical_retirements`, immediately before the reactivation branch at `5626`, apply the same test and, when blind-within-grace, renew the diagnostic deadline and `return` - the exact shape the function already uses for "evidence unavailable" at `5556-5567` and `5572-5582`.

**Why 'wait' is safe here.** `wait` keeps the victim off-route and alive; it withholds nothing that the autoscaler cannot supply, because contract 9 leaves every launch path untouched. It is bounded three ways: by the grace (120 s), by snapshot staleness (`_logical_snapshot_is_fresh`, 60 s, `2126-2128`), and by the drain deadline at `5935-5965`, after which today's abort fires anyway. The cost of the grace is that a genuine demand rebound that coincides with an LB roll cannot be served by instant victim reactivation for up to 120 s. The benefit is that the far more common case, a 15-40 s observability gap, stops reverting entire reclaim waves. That trade is stated here so it can be revisited with a smaller value (60 s = `3 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS`) if reactivation latency turns out to matter.

### What is not changed

`_ReplicaDrainTracker`'s four acknowledgement sets and the `_unknown_tainted` logic; `_seed_from_existing_report` (`736-761`); `update_lb_in_flight` and its tuple (`1956-1958`, `1986-2012`); `_lb_report_authority` (`797-857`); `_apply_load_balancer_drain_report`'s authority gates (`1142-1174`); `_publish_ha_drain_view` (`1540-1590`); every file in `lb_ha.py` and `lb_k8s.py`; `_get_lb_session_id`; the termination call sites; every service-spec schema field; the HA cutover saga.

## Latency budget

LB Deployment rolls at T=0 (new Pod created). Replica R is off-route, genuinely idle, drain open, drain age already > 0. Constants: sync loop sleeps 5 s before its first attempt (`load_balancer.py:3669-3670`) then runs every `LB_CONTROLLER_SYNC_INTERVAL_SECONDS = 20`; probe every `LB_OCCUPANCY_PROBE_INTERVAL_SECONDS = 10`; `_refresh_wait_for_idle` runs inside `_thread_pool_refresher`'s `_PROCESS_POOL_REFRESH_INTERVAL = 20` s loop (`7208-7221`, called at `6672`); `LB_DRAIN_GRACE_SECONDS = 15` before the old server exits (`load_balancer_http.py:85-93`).

### Non-HA (protenix), logical retirement

| t | event | before | after |
|---|---|---|---|
| +10-15 s | new Pod's uvicorn up; probe round 1 runs with an empty probe set | - | - |
| +15-25 s | first sync succeeds; `_ready = True` | R absent from every set forever | **watchlist arrives; R enters `_occupancy_capable`** |
| +20-30 s | k8s marks the new Pod Ready; Deployment scales the old RS down; old Pod gets a deletionTimestamp | blind snapshot published (`force_all_live_unknown`); a refresher tick in this window **aborts the retirement** | blind view detected; `_logical_retirement_state` returns `wait`; retirement survives |
| +25-35 s | probe round picks R up; 200 with `running_count: 0`; R joins `_occupancy_sampled_off_ready` | - | first-party proof held locally |
| +35-55 s | old Pod's server exits (`+15 s` after SIGTERM) and its Pod object is deleted -> `live_uids == {new}` -> `drain_authoritative` | - | - |
| +40-70 s | first sole-live sync publishes `in_flight[R] = 0` under the new session | - | tracker: `_seen = True`, `explicit_idle`, `blocked = False` -> **drained** |
| +0-20 s after that | `_refresh_wait_for_idle` tick; capacity view is no longer blind (probe rounds have repopulated `total_slots_by_url` since ~+30 s) | - | `_logical_retirement_state` -> `safe` -> `_finish_logical_retirement` -> `_terminate_replica(in_flight_drain_cap_seconds=0)` |

**After: ~60 s typical, ~110 s worst case.**
**Before: the retirement is aborted at ~25-45 s with zero net progress**, its `drain_started_at` cleared, requiring re-selection and a fresh 7200 s budget; or, on the minority of rolls whose blind window misses every 20 s tick, 7200 s and then `_abort_logical_retirement` at `5962-5964` anyway.

### Non-HA, physical drain (`_terminate_replica` with a positive cap)

Same arrival of proof at +40-70 s; `_wait_for_drain` polls the predicate every 2 s (`replica_managers.py:640-655`), so termination proceeds at **+42-72 s** instead of at `graceful_drain_seconds`. For a service with the 7200 s cap that is a ~100x reduction; for the `_DEFAULT_DRAIN_SECONDS = 120` default it is roughly break-even, which is fine.

### Co-restart (API server Pod + all LB Pods, the measured 11:57/11:59 event)

Add the recovery gate. `_logical_controller_epoch` is new, so every uncommitted logical retirement is in `_recovering_logical_retirement_ids` and `_refresh_wait_for_idle` `continue`s at `5921-5927`. Ownership is `_reconcile_recovering_logical_retirements`:

- with C, the reactivation branch is suppressed while the view is blind, so the wave is not undone;
- once a fresh, version-matching, generation-coherent snapshot arrives (~+40-70 s from LB Pod creation), the pass **adopts**: it re-fences the row to the new epoch and sets `logical_retirement_generation = snapshot.generation` (`5705-5718`);
- release requires a **strictly newer** snapshot generation (`5606-5609`), i.e. one more authoritative sync, +20 s;
- then the next `_refresh_wait_for_idle` tick, +0-20 s, sees `drained` already `True` (A delivered the proof during the wait) and finishes.

**After: ~100-150 s.** Before: the wave is reactivated 20 victims per generation until it is fully undone.

### HA (boltz-l4-fleet)

The promoted slot has been receiving `drain_watchlist` on its own unconditional 20 s sync loop and probing R for the entire drain, so it is warm at cutover. `_ha_role_payload` sources the same `_in_flight_with_draining()` (`load_balancer.py:3704-3708`) on the `LB_ROLE_HEARTBEAT_INTERVAL_SECONDS = 2` channel, so the report leg is <=2 s instead of <=20 s. Dominated by the `_refresh_wait_for_idle` cadence: **~20-40 s** after the last warm stream owner leaves. Note `force_all_live_unknown` is already `False` under HA (`controller.py:1073`), so HA's only blindness source is empty `observed_slots`, which C still covers.

### Steady state, no restart

Unchanged: probe (<=10 s) + sync (<=20 s, or <=2 s in HA) + refresher tick (<=20 s) = ~20-50 s. A warm LB that already has the url in `_occupancy_capable` pays nothing new; the watchlist union is a no-op for it.

## Rejected alternatives

**Rejected: controller-side occupancy probing behind a Kubernetes pod-generation fence** (design 2). The insight is right (the controller can obtain first-party occupancy in 3-5 ms from inside the API server Pod, over the same network path `ReplicaInfo.probe` already uses at `replica_managers.py:1812-1822`), but the construction fails on three counts. (i) The safety argument turns on a persisted `pre_retirement_lb_pod_ids` fence, and its specified write site, `_register_wait_for_idle`, has four callers of which only `5060` is a drain start; `2947` and `2958` are the controller-restart recovery walk (`_register_wait_for_idle` early-returns only on an already-present tracker, `4980-4981`, and the tracker map is empty after a restart). Writing always makes the fence name the *current* Pods in the co-restart case, so it never clears and the fix is inert; writing only-if-absent leaves the fence behind on `_abort_logical_retirement` (`5757-5782` clears every other drain field but would not clear this one), and a stale fence naming long-dead Pods produces a genuine false proof: `live_ids & pre_retirement_ids` is empty, the "no contradiction" condition is satisfied vacuously by the blocking publish `({}, None, [], [])`, and a replica with *synchronous* work on a warm Pod is torn down with `in_flight_drain_cap_seconds=0`. (ii) It promotes `get_lb_pod_authority` from a gate that withholds proof into an input that grants it, which is a strictly larger trust than today's. (iii) It puts a Kubernetes LIST inside `tracker()`, which `_logical_retirement_victim_is_idle` (`5241-5250`) calls from inside `self._logical_state_lock`, on a fleet path whose history already includes serial probe waves starving scale-up (`replica_managers.py:7473-7477`). The watchlist gets the same first-party evidence with no new trust, no new durable field, no new thread, and no new lock interaction, because the LB is already the process that owns the probe.

**Rejected: HA warm-standby continuity as the fix** (design 3's part (a)). The standby genuinely is warm - `_sync_with_controller` and `_probe_occupancy_loop` are both started unconditionally (`load_balancer.py:4557-4575`) with no role gate - but its report is discarded in the STABLE phase because `_publish_ha_drain_view` restricts owners to `{state.active_slot}` (`controller.py:1554-1560`), and the dominant event rolls the standby *first*: `planned_upgrade` fires exactly when the target carries `desired_runtime_revision` and the active does not (`controller.py:1963-1981`), so the Pod promoted to ACTIVE is the cold one, and `begin_lb_cutover` bumps the generation (`serve_state.py:2199`), changing the published `ha-generation-{N}` and resetting `_seen`. HA converts "never proves because `_seen` is False" into "never proves because the taint is set". Same 7200 s, more moving parts, and it requires a `sky serve update`.

**Rejected: LB-to-LB pre-stop overlay handoff** (design 3's part (b), before it was reshaped). What the dying process holds that its successor lacks is `_draining_clients` (live sockets, which cease to exist at the moment of handoff), `_replica_occupancy` (stale numbers that must *not* be inherited, since a stale count is exactly what could false-prove), and `_occupancy_off_ready_since` (hygiene). Strip those and the residue is a list of urls, which the controller already knows better and more durably. A pre-stop handoff also silently skips on `kill -9`, OOMKill, node loss and eviction, which are precisely the cases where you need it.

**Rejected: a per-process session nonce, `restart_count` folding, or an HA owner fingerprint.** Analysed under Mechanism B: it resets `_seen` on process boundaries where the un-provable dimension (synchronous in-flight) has provably been destroyed, and for sync-only replicas a reset is unrecoverable, so it would convert working drains into deadlines, in HA on every cutover phase change. Conjunct B closes the actual holes precisely.

**Rejected: `LB_DRAIN_SEED_SETTLE_SECONDS`** (design 1's D2 fence). Unnecessary given the sole-live authority gate at `controller.py:856`, and its `setdefault`-based bookkeeping silently disappears on a re-retirement of the same url within one LB process lifetime, so the invariant it advertises does not hold where it is claimed.

**Rejected: extending `_lb_expected_occupancy_urls` to gate HA promotion on drain-proof coverage** (design 3's M5). It touches the cutover state machine, the most intricate code in this area, and a bug there wedges LB rollouts. With A in place the promoted slot is already warm for off-route replicas, so M5 converts "warm in practice" into "provably warm", which is not worth the risk. Reconsider only if HA cutovers are measured losing drains.

## Interim mitigation, no code

**Plain verdict: no configuration change on `protenixv2-hybrid-v1` covers a meaningful fraction of the exposure. The code change is the substantive win, not a marginal one.** Each candidate and why:

**`load_balancer.high_availability: true` - do not do this.** It looks attractive because HA does remove one of the two blinding paths (`force_all_live_unknown` is `False` when `ha_enabled`, `controller.py:1073`). But (i) the LB pod template pins the controller image digest, so a control-plane deploy rolls **both** slots and bumps the cutover generation, resetting `_seen` exactly as a non-HA Pod replacement does; (ii) the promoted slot is the cold one (`planned_upgrade` at `controller.py:1963-1981`); (iii) it does not touch the second blinding path, an empty `total_slots_by_url` from a cold demand reporter; (iv) it costs a `sky serve update`, a second LB Pod, and puts the service on the cutover saga; (v) it exposes the HA-narrowing false-proof edge that conjunct B exists to close. Partial at best, and it adds a correctness edge.

**Lowering `graceful_drain_seconds` (7200 -> e.g. 900) - do not do this.** The same spec value feeds `in_flight_drain_cap_seconds` on the physical and bounded rolling-update termination paths (`replica_managers.py:5952-5960, 5975-5995`), where the cap bounds a real graceful drain and force-kills async work when it elapses. `cd3b82100f` raised the ceiling to 7200 precisely because "a job admitted the instant retirement starts runs its full duration into the drain". Shortening it trades correctness for latency, which the safety bar forbids. Keep it in the operator's pocket for an emergency only, and only with the understanding that it can kill hour-scale protenix jobs during a version rollout.

**Nothing else in the service spec helps.** `graceful_drain_async_occupancy` is already forced `true` by `spot_placer: dynamic_fallback_per_gpu` (`service_spec.py:439-444`) and cannot be turned off. Raising `terminationGracePeriodSeconds` or `LB_DRAIN_GRACE_SECONDS` is counterproductive: it makes the sole-live authority arrive *later*.

**Two levers that do help, neither of them a service config change:**

1. **Stop stamping the controller image digest onto the LB pod template** (chart/platform change). This attacks the frequency directly: measured 46 protenix LB rollouts in 41.9 h, mean 55 min, essentially all of them side effects of control-plane deploys. Decoupling would reduce that to LB-image changes only. It fixes nothing - a single deploy still permanently strands every open drain and reverts the wave - but it cuts the number of such events by roughly an order of magnitude. Worth doing independently.
2. **Make the reclaim workstream LB-aware** (operational rule in the reclaim scheduler, not in SkyPilot). Skip a reclaim tick while any of the service's LB Deployments has a rollout in progress (`kubectl rollout status`), and stagger retirements so only a small number of drains are open at once rather than retiring ~76 in a single `scale_down_logically_batch`. This reduces how many replicas a roll can catch; it does not stop a roll from catching them.

## Risks

**Residual paths to a false proof.**

- *The replica lies.* The entire proof rests on `running_count` from `local_async_router.py:568, 634`. A replica whose async router is wedged and reports 0 while jobs run will be terminated. This is unchanged from today (a warm LB's proof rests on the same number), and the router is itself conservative: `_capacity_response` (`local_async_router.py:592-635`) counts reservations as running, adds `max(1, child.running, reserved)` for an unknown child, and returns `status: UNKNOWN` if any child is unknown, which `_parse_replica_occupancy` rejects outright. But this design makes the system *rely on it more often*, because proofs now happen where deadlines used to expire.
- *Direct client access bypassing the LB.* Out of scope and equally a hole today.
- *IP reuse / stale url.* Both today and after: the tracker matches by url, and `_refresh_wait_for_idle` already treats a vanished cluster record as drained (`5898-5899`). Unchanged.
- *A degenerate watchlist entry.* Cannot produce a proof. A stale, duplicated or dangling url names a replica that is torn down or unreachable, so the probe returns `None`, the url is not in `sampled_set`, and `_in_flight_with_draining` appends it to `unknown_urls` (`2019-2028`), which sets `_unknown_tainted` and `blocked` (`787-800`). It stays blocked afterwards, because only an explicit idle entry clears the taint. There is no branch in which a bad entry yields a proof.

**The co-restart case is only partially solved, by design.** With C, the wave survives; but `_refresh_wait_for_idle` still refuses authority for recovering ids (`5921-5927`) and `_reconcile_recovering_logical_retirements` still requires adoption plus a strictly newer snapshot generation before releasing (`5606-5609`). That costs ~40 s on top of the LB warm-up. This design does not shorten that fence: it is an existing, documented safety property ("Recovery reconciliation exclusively owns route readmission"), and shortening it is a separate change with its own argument. If the co-restart path proves to be the dominant cost in practice, that fence is the next thing to look at, not the drain proof.

**Blind-grace tuning.** 120 s is a judgement call. Too long and a genuine demand rebound coinciding with an LB roll cannot be served by instant victim reactivation; too short and a slow LB warm-up still reverts the wave. It is bounded by the drain deadline in every direction, and it never suppresses a launch, but it is the parameter most likely to need adjustment after the Milestone 0 counters exist.

**Probe fan-out.** During a 76-replica wave the LB's probe set grows from ~2 ready urls to ~78. `TCPConnector(limit=len(probe_urls))` (`load_balancer.py:3005`) gives full parallel fan-out with a 2 s per-request timeout, so round wall time stays ~2 s, but concurrent sockets roughly double. A *warm* LB already does exactly this today via off-ready retention; the change only makes it start earlier after a restart. Watch `record_probe` telemetry (`3225-3229`) and the LB container's fd limit if the fleet grows an order of magnitude.

**Coverage is narrower than "every service".** Only replicas whose version declares `graceful_drain_async_occupancy: true`, or whose url the LB has been observed successfully sampling in this controller incarnation, are advertised. Sync-only drains keep today's behaviour (correctly: there is no first-party evidence to recover, and their normal cap is `_DEFAULT_DRAIN_SECONDS = 120`). A service running async work without declaring the flag gets nothing after a co-restart. Log the eligible set at controller start so this is visible rather than assumed.

**Conjunct B is a small conservatism increase during version skew.** A new controller with an old LB advertises nothing, so an async-declared url is absent from the report, and B refuses the absence proof that today's code would have accepted. That window is one LB rollout (minutes, per the digest coupling), and refusing it was always the correct answer.

**Registry hygiene.** `_drain_proof_urls` is a url memo keyed by `replica_id`. A missed removal cannot cause a stale advertisement, because the advertisement is gated on durable row state (A2), but it can grow. Prune it in the same pass that snapshots it, dropping ids that are not in the current `replica_infos`.

**Two files on the "handle with care" list.** `load_balancer.py`'s seed sits inside `_client_pool_lock`, which every proxied request contends: it must stay `O(len(watchlist))` and perform no I/O. `controller.py`'s watchlist call is on the 20 s sync path and must never raise out of its try/except.

## Milestones

Ordered by dependency. **The sequencing constraint is real: C must not ship before A.** With C alone, a caught logical retirement stops being aborted at ~30 s and instead waits its full `graceful_drain_seconds` and *then* aborts, which is worse for the affected replicas. With A alone, protenix sees no change (the capacity gate still aborts first) but the physical drain path and non-logical services get the full win immediately, and nothing regresses.

**Milestone 0 - observability. ~0.5 day.** Add counters, no behaviour change: drain-deadline-expiry-without-proof (per replica, at `replica_managers.py:5984` and `5962`), logical-retirement aborts by reason, blind-snapshot occurrences, and watchlist truncation. Ship first so the model in this document is calibrated against production before and after. Today there is no production evidence that the 7200 s cost has ever actually been paid on the logical path, and the code says it usually is not.

**Milestone 1 - A + B: watchlist and explicit-idle hardening. ~3 days.** `sky/serve/constants.py` (one constant), `sky/serve/replica_managers.py` (tracker factory + url memo, `drain_proof_watchlist`, `requires_explicit_idle` conjunct), `sky/serve/controller.py` (one response key, `_lb_observed_occupancy_urls`), `sky/serve/load_balancer.py` (parse, seed, retention pin, reset). Independently shippable and independently valuable: it fixes the physical drain path for every service and closes both latent false-proof holes. Inert but harmless for protenix's logical retirements.

**Milestone 2 - C: blind capacity view. ~2 days.** `sky/serve/replica_managers.py` (snapshot field, blindness helper, grace, two call sites) and one keyword at `controller.py:1134-1139`. This is what makes Milestone 1 deliver for protenix. Ship after Milestone 1 is deployed and its counters are clean.

**Milestone 3 - design doc + operator note. ~0.5 day.** Update `docs/designs/serve-demand-aware-scaling-ramp.md`'s "Drain-proof handoff and recovery" section, whose current text ("a session change clears the seed") implies a later same-session report can still arrive. State that after a session change the url is permanently unreachable in all four sets, and document the watchlist, the explicit-idle rule, and the blind-capacity grace. Add an operator note listing which services are eligible for watchlist coverage and why.

**Milestone 4 (optional, separate review) - reduce LB roll frequency.** Chart change to stop pinning the controller image digest on the LB pod template. Not part of this design; tracked here because it multiplies the value of Milestones 1-2.

## Test plan

Logic only. No assertions on log or error message text. Extend these existing files.

**`tests/unit_tests/test_serve_graceful_drain.py`** (`TestReplicaDrainTracker`, line 211; `_manager()` at 201):
- New session, url absent from all four sets: `tracker()` is `False`. (Regression pin for today's behaviour, contract 1.)
- New session, `in_flight[url] == 0` present: `tracker()` is `True`.
- `requires_explicit_idle=True`, url absent from all sets, `_seen` carried from a prior report under the same session: `tracker()` is `False`. (Contract 2; covers the container-restart and HA-narrowing holes.)
- `requires_explicit_idle=True`, `in_flight[url] == 0`: `tracker()` is `True`.
- `requires_explicit_idle=False` (sync-only), url absent, `_seen` set: `tracker()` is `True`. (Contract 3: no regression for the population that cannot re-acknowledge.)
- `requires_explicit_idle=True`, `url in unknown_urls`: `tracker()` is `False`, and a later report with the url merely absent stays `False`.

**`tests/unit_tests/test_lb_occupancy.py`** (`TestProbeRound`, line 175; `_make_balancer()` at 161):
- A `drain_watchlist` entry unions into `_occupancy_capable`, seeds `_occupancy_off_ready_since`, and does **not** appear in `ready_replicas`, `_replica_info_by_url`, `_replica_total_slots` or `_replica_free_slots`. (Contract 5.)
- A watchlisted url whose probe misses appears in `unknown_in_flight_urls` and has no `in_flight` entry.
- A watchlisted url whose probe returns `running_count: 0` yields `in_flight[url] == 0` and membership in `occupancy_sampled_urls`.
- A watchlisted url that is also in `_occupancy_explicitly_disabled_urls` is not seeded, and `_occupancy_declared_urls` / `_occupancy_disable_pending` are unchanged by seeding.
- Absent `drain_watchlist` key leaves existing pins intact; an empty map releases them; an expired pin is dropped by the next probe round and the url is no longer in `keep`.

**`tests/unit_tests/test_serve_controller.py`** (already exercises `update_lb_in_flight`, `_apply_load_balancer_drain_report` and `update_logical_reconcile_snapshot`):
- `drain_watchlist` excludes a url present in `replica_info`, excludes a replica whose remaining drain budget is `<= 0`, and excludes a replica whose version flag is `None` or `False`.
- `drain_watchlist` includes a url the LB previously reported in `occupancy_sampled_urls`, and excludes one seen only in `unknown_in_flight_urls`.
- Truncation at `LB_DRAIN_WATCHLIST_MAX_URLS` keeps the soonest deadlines.
- An exception inside the watchlist computation omits the key and the sync still returns 200.
- The snapshot carries `forced_all_live_unknown=True` exactly when `not drain_authoritative and not ha_enabled`.

**`tests/unit_tests/test_serve_replica_managers.py`** (already exercises `_logical_retirement_state` / `_logical_ready_capacity`):
- `forced_all_live_unknown=True` with `ready_capacity < target` returns `'wait'` within the grace and `'abort'` past it.
- Empty `observed_slots` with at least one eligible ready candidate returns `'wait'` within the grace.
- Partial observation that still covers the target returns `'safe'`/`'wait'` per the victim-idle check and never consults the blindness helper.
- A genuine shortfall with a fully observed fleet still returns `'abort'` (no regression).
- `_logical_last_sighted_capacity_at` advances on a non-blind evaluation.

**`tests/unit_tests/test_serve_restart_bounded_drain_resume.py`** (already has `test_probe3_shortfall_reactivation_still_works` at line 148 and `_restart_manager` at 66):
- A blind snapshot does not reactivate recovering candidates within the grace, and the recovery deadline is renewed instead.
- Past the grace, the existing reactivation behaviour is unchanged (extend the existing shortfall test with a non-blind snapshot so it keeps asserting today's semantics).
- After recovery re-registers trackers, `drain_proof_watchlist` includes the recovered urls (co-restart re-arming).

**`tests/unit_tests/test_serve_lb_ha.py`**: assert the HA aggregate and `_publish_ha_drain_view` are byte-unchanged by this design (no new fields, session string still `ha-generation-{N}`), so the cutover saga is provably untouched.

**Version skew**, in `tests/unit_tests/test_serve_load_balancer_sync_spec.py`: a response without `drain_watchlist` leaves all LB occupancy state exactly as today; a request payload with an unexpected extra key is ignored by the controller handler.

**Smoke**: no new smoke test. `tests/smoke_tests/test_sky_serve.py` already covers update/scale-down; the drain-proof path needs a live LB roll, which is not reproducible there. Validate on the fleet instead, using the Milestone 0 counters before and after.

## Open questions

1. **Is 120 s the right blind grace?** It is currently justified by symmetry with `_LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS` (`replica_managers.py:85`). The measured blind window is 15-40 s, so 60 s (`3 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS`) may be enough and would halve the worst-case reactivation delay. Decide with Milestone 0 data.
2. **Does `boltz-l4-fleet` declare `graceful_drain_async_occupancy`?** Not verified. It determines whether HA services get watchlist coverage at all, and whether conjunct B's explicit-idle requirement applies to them. Check the live spec before Milestone 1 ships.
3. **Should the recovery release fence (`replica_managers.py:5606-5609`, requiring a strictly newer snapshot generation after adoption) be shortened?** It costs ~20 s on the co-restart path, which is the dominant production event. It exists for a documented reason ("Do not admit that shutdown from the same LB generation whose pre-adoption view authorized the re-fence"), so shortening it needs its own argument, not a side effect of this change.
4. **Should `_lb_observed_occupancy_urls` be made durable?** In-memory it dies with the controller, so it contributes nothing in the co-restart case. Persisting it would widen coverage to undeclared-but-async services across restarts, at the cost of a new durable field. Probably not worth it; the correct answer for those services is to declare the flag.
5. **Should the watchlist also carry the physical-drain replicas of *pool* services?** They have no LB (`replica_managers.py:5010`, `4850-4852` skip tracker construction for pools), so the answer is almost certainly no, but it should be stated in the code rather than inferred.
6. **Is there a cheap way to detect a wedged async router** (one that reports `running_count: 0` while jobs run)? Everything here inherits that trust. A cross-check against the replica's own job-count endpoint, or against the controller's own view of dispatched job ids, would close the last residual false-proof path. Out of scope for this change; worth a follow-up if the reclaim workstream starts terminating at scale.