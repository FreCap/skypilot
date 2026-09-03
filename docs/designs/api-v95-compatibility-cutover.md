# API v95 compatibility cutover

- **Status:** In progress
- **Last updated:** 2026-09-03
- **Transition PR:** [boltz-bio/skypilot#1930](https://github.com/boltz-bio/skypilot/pull/1930);
  merged as `c114eff9fc9fcc5e50553bd13a21920bc1a08feb` without a
  production rollout.
- **Cleanup PR:** [boltz-bio/skypilot#1920](https://github.com/boltz-bio/skypilot/pull/1920);
  rebased on the transition merge, with exact-head CI pending.
- **Protocol PR:** [boltz-bio/skypilot#1917](https://github.com/boltz-bio/skypilot/pull/1917)

## Context

API v95 replaced ambiguous request-result replay authorization with an exact
request-ID response header.  The protocol shipped in Boltz artifact
`v1.1.1653`; production currently runs `v1.1.1660` on every API, controller,
and executor pod.  All of those binaries therefore speak API v95, but the
source compatibility floor remained at API v24, inherited from SkyPilot's
0.11-to-0.12 release transition.

The Boltz artifact publisher creates an immutable patch release for each merge.
It does not advance `MIN_COMPATIBLE_API_VERSION`; that value is a deliberate
peer-support policy, not a record of the last Helm deployment.  Transition PR
#1930 has now raised the source floor and bundled dashboard to API v95.
Production still runs `v1.1.1660`, whose bundled dashboard advertises API v84,
until the one combined rollout below.

The operating decision for this cutover is one fix-forward production rollout.
The floor/dashboard transition is merged first as a source prerequisite, then
the obsolete pre-v95 authorization and body-decoder code is removed in the
cleanup PR, and only the final combined artifact is deployed.  `boltz-platform`
has no authority over the application Helm release and is outside this rollout.

## Goals

- Make API v95 the oldest supported versioned client/server protocol.
- Keep the bundled dashboard usable by advertising API v95 before the floor is
  enforced in the same final artifact.
- Make pre-v95 request-result detail-based replay authorization unreachable,
  then remove its redundant body decoder in the already-open cleanup PR.
- Retarget backward-compatibility validation to the first supported Boltz v95
  artifact rather than unsupported public PyPI releases.
- Deploy API, controller, and executor from one immutable image digest through
  one direct Helm fix-forward revision.

## Non-goals

- No new request-result protocol; API v95 remains unchanged.
- No database schema or durable-state migration.
- No `boltz-platform` edit or infrastructure apply.
- No promise that public SkyPilot API v24-v94 clients remain compatible.
- No conversion of the compatibility middleware into a network access-control
  boundary.

## Public contract

- A peer that sends both SkyPilot version headers with API version below 95 is
  rejected by client/server compatibility checks.
- A peer at API v95 or later is accepted by the global compatibility check.
- `MIN_COMPATIBLE_VERSION` is `1.1.1653`, the first immutable Boltz artifact
  containing API v95.  The semantic version is explanatory; API-version
  headers remain the enforcement key.
- Dashboard calls routed through the shared API client, plus the explicitly
  versioned queued mutation paths, send API v95 and the readable dashboard
  identifier.  A few direct same-origin read/stream paths remain headerless and
  use the documented middleware bypass; they do not execute the Python
  request-result parser changed by this cutover.
- Existing callers that omit either version header continue through the
  historical middleware bypass and are treated as having unknown client API
  version.  This exception is required by unauthenticated probes and non-SDK
  HTTP paths today.  Removing it requires a separate route-aware design and is
  not implied by this cutover.
- External API v24-v94 SDK/CLI installations are unsupported after rollout and
  receive the normal version-mismatch response when they send version headers.

## Architecture and invariants

1. `API_VERSION` and the exact retry-marker introduction version both remain
   95.
2. The bundled dashboard API version must be at least
   `MIN_COMPATIBLE_API_VERSION`; a source-level test guards this cross-language
   invariant.
3. The cleanup removes the legacy detail parser from both synchronous and
   asynchronous result clients.  No 503 body is JSON-decoded or parsed for
   replay authorization; tests assert that the synchronous JSON method is not
   called and the asynchronous JSON method is not awaited.  Async transport
   draining via `response.read()` remains part of lost-body/ACK detection.
4. Production has no live pre-v95 control-plane pod before deployment.  During
   the final rolling update, old v95 Python peers accept new v95 peers and new
   peers accept old v95 Python peers.
5. A browser can briefly retain or receive dashboard-v84 assets while new
   floor-v95 pods enter service.  The authorized single-rollout policy accepts
   that transient UI failure; a refresh after convergence loads the v95 bundle.
   It must not affect Python control-plane reconciliation.
6. API, controller, and executor image values move together to the same digest.
7. PostgreSQL remains the central request store.  The rollout does not add a
   SQLite compatibility path.
8. The direct single-rollout procedure assumes controlled ingress has no
   unsupported pre-v95 or unknown/headerless mutating writer during the final
   inventory and rolling update.  The point-in-time PostgreSQL query detects
   existing work but cannot itself freeze a new headerless submission.  If
   that operational assumption cannot be established, the rollout is blocked
   pending an explicit admission-maintenance window.

## Implementation phases

### 1. Source transition (complete)

- Set `MIN_COMPATIBLE_API_VERSION` from 24 to 95.
- Set `MIN_COMPATIBLE_VERSION` to `1.1.1653`.
- Set the dashboard `CLIENT_API_VERSION` from 84 to 95.
- Replace public-PyPI compatibility mismatch remediation: rejected old clients
  receive an exact `boltz-bio/skypilot` server-commit install command, while an
  old server requires an operator fix-forward rather than a client downgrade.
- Change v94 synchronous and asynchronous request-result tests from legacy
  replay authorization to bounded fail-closed observation.
- Keep and characterize the now-unreachable legacy authorization branch and
  still-executed response-detail decoder so the subsequent deletion has an
  explicit red/green seam.
- Make the backward-compatibility harness clone the Boltz repository and use
  `v1.1.1653` as its default supported baseline.
- Replace scheduled public-PyPI compatibility inputs with the Boltz v95 floor;
  public API v56 and earlier are intentionally outside this contract.
- Merged into `improvements` as
  `c114eff9fc9fcc5e50553bd13a21920bc1a08feb`; it was not deployed by itself.

### 2. Cleanup (implementation complete, exact-head CI pending)

- Rebased PR #1920 on the merged transition.
- Removed legacy sync/async response-detail parsing and the obsolete function
  parameter.
- Asserted that v94 503 bodies are never decoded and cannot authorize replay.
- Completed local focused tests plus formatting/static checks.  Run required
  CI and the PostgreSQL Managed Jobs Unpaid E2E on the exact published head
  before merge.

### 3. One production rollout

- Wait for the immutable image corresponding to the cleanup merge and resolve
  its ECR digest and source commit.
- Confirm no paid E2E lifecycle is attached to a rollout pod, its test service
  has been torn down, and no nonterminal mutating request or live remote
  controller is pre-v95 or has unknown/headerless version evidence.
- Render the exact checked-in chart with the current Helm user values and all
  three role image overrides.
- Upgrade release `skypilot` in namespace `skypilot` with `--reuse-values`,
  `--wait`, `--wait-for-jobs`, and a bounded timeout.
- Verify migration job completion, all role deployments and pods on the exact
  digest, API health/version/commit, the dashboard bundle, and API 94/95
  boundary behavior.

## Deployment and recovery

Production application runtime is owned by direct Helm.  The pre-cutover
baseline is Helm revision 765, artifact `v1.1.1660`, commit
`9a13d9189ac09087adea5452ff03e44eb10d2bfa`, and image digest
`sha256:391ae1ee680abddd45e97be8527911c0e6cca832cf6d5e0bad3e14c9f6d78d5f`.
The canonical JSON user-values SHA-256 (`helm get values -o json | jq -S -c`)
is `ecb316107998d30ae9f02c822eb2d5112444723bb2b5352c21f89aeaaff76e93`;
the raw stored-manifest SHA-256 (`helm get manifest`) is
`54e60d69fd805d16d12ad4b8d3eb175d8e4226630a104f38d8c2c29d4465b270`.
Both are re-captured with the same commands and compared immediately before
mutation.

The normal recovery path is another fix-forward Helm revision.  `--atomic` and
an unqualified native Helm rollback are not used because production database
migrations are forward-only in the general case.  These two source changes add
no migration; reusing the recorded v1.1.1660 digest is allowed only after the
post-deploy schema head is proven readable by that binary.  Otherwise recovery
uses a newly published schema-compatible fix.

## Verification evidence

- TDD red: the source transition tests fail on `v1.1.1660` because the floor is
  24 and the dashboard advertises 84.
- TDD green: 33 version-contract tests, 76 sync/async request-result tests, and
  15 dashboard client tests pass locally.  The two mismatch-remediation tests
  were also observed failing before the Boltz-specific messages were added.
- Cleanup RED/GREEN: on the transition tree, the two new v94 no-JSON
  expectations fail because the legacy parser calls/awaits JSON three times.
  On the cleanup tree, all 109 focused sync/async/version tests pass; the same
  responses exhaust three same-ID observations and never call/await JSON.
- The cleanup tree passes `format.sh` (YAPF, isort, mypy over 995 files,
  Pylint 10/10, dashboard ESLint/Prettier), the exact pinned async-lifecycle
  baseline, and `basedpyright` in an isolated Python 3.14 environment.
- The generator regression test passes.  A real Kubernetes-filtered generation
  contains two current quick-core steps plus twelve backward-compatibility
  steps; only the latter carry `--base-branch v1.1.1653`.
- Live preflight: Helm revision 765 is healthy; 2 API, 2 controller, and 7
  executor pods are Ready on one v1.1.1660 digest with zero restarts; API health
  reports protocol 95 and the exact source commit.
- Recent request inventory includes API 50, 84, 87, 93, and 94 callers as well
  as API 95.  Those older versioned callers are deliberately retired by this
  decision.  Dashboard v84 accounts for most of that traffic and is updated in
  the final artifact.
- At 21:55 UTC, active pre-v95 work consisted only of two read-only dashboard
  API-v84 `sky.jobs.queue_v2` polls.  Active mutations were three API-v95
  managed-job launches.  This is evidence, not a substitute for repeating the
  gate immediately before Helm.
- At 22:51 UTC, the paid E2E lifecycle and service had no active process,
  service, replica, claim, waiter, controller, cluster, queued request, managed
  job, Kubernetes object, EC2 instance, Spot request, EBS volume, ENI, or EIP.
  Three unattached non-billable security groups and database audit/history rows
  remain intentionally; they are not active resource blockers.

## Open gates

- [x] Transition focused unit and dashboard tests are green locally.
- [x] Backward-compatibility generator/harness selects the Boltz v95 baseline.
- [x] Transition PR required CI is green and merged without a production
      rollout.
- [x] PR #1920 is rebased and its no-body-decode TDD proof is green locally.
- [ ] PR #1920 required CI and Managed Jobs Unpaid E2E are green on exact head.
- [ ] Final image provenance and digest are verified.
- [x] The active paid-capacity E2E exits and its service teardown is verified;
      do not interrupt the lifecycle process by rolling its API pod.
- [ ] The immediate pre-Helm request/controller inventory has no nonterminal
      pre-v95 or unknown/headerless mutator or controller.
- [ ] Helm dry-run differs only by the intended image/provenance inputs.
- [ ] Final rollout and post-rollout API/dashboard/reconciliation checks pass.
