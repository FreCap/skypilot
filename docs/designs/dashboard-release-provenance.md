# Dashboard release provenance

_Status: implemented; production rollout follows merge_

## Problem

The desktop dashboard header uses scarce space for four community shortcuts while the release indicator exposes only a version and build number. Operators cannot tell when the displayed code was checked in or when the currently serving API process came online, so confirming that a merge reached production requires separate Git and Kubernetes inspection.

## Behavior contract

The desktop header shows a compact `v<version> · deployed <relative age>` label when deployment time is available. Its details show the full version, build, commit, exact check-in time, and exact deployment time. Missing timestamps are omitted, and an older server falls back to the existing version/build label.

“Checked in” is the committer timestamp of the exact commit stamped into the release. “Deployed” is the UTC initialization time of the API process currently serving the response. The detailed label states that server-start semantic explicitly, because a process restart advances it even when the image does not change.

Docs, GitHub, Slack, and feedback shortcuts are removed from the desktop header. Mobile community links, plugin navigation, request activity, notifications, theme, settings, and user controls remain unchanged.

## Data flow

```text
git commit time -> package/overlay stamp -> sky.__commit_timestamp__
API process start -----------------------> deployment_timestamp
                                                    |
                                                    v
authenticated /api/health -> VersionProvider -> compact header + tooltip
```

## API and compatibility

`APIHealthResponse` adds nullable ISO 8601 `commit_timestamp` and `deployment_timestamp` fields. The authenticated `/api/health` response populates them; anonymous orchestration probes continue to receive status only. This is an additive response change, so existing clients ignore it and no API-version bump is required.

Source and editable installs derive the commit timestamp with `git show -s --format=%cI HEAD`. Wheel builds replace a source placeholder through the existing setup lifecycle. The Boltz overlay builder stamps the same exact release commit timestamp into its isolated context before Git metadata is removed.

Malformed or unavailable timestamps never produce guessed dates. The dashboard ignores them and retains the old label. A newly cached dashboard bundle used briefly against an older server therefore remains functional.

## Alternatives considered

Querying Kubernetes or Helm from `/api/health` could expose an orchestration-specific rollout time, but it would add permissions, latency, and a failure dependency to a health-critical endpoint. Stamping image build time would incorrectly present publication as deployment. API-process initialization is portable and directly identifies the running code instance.

Showing all exact values inline would recreate header crowding. Relative deployment age keeps the header scannable while the existing tooltip carries exact provenance.

## Implementation milestones

1. Add commit-timestamp stamping and the optional health-response fields.
2. Extend the version provider, compact header, and exact tooltip details; remove the four desktop shortcuts.
3. Add package, schema, component, and compatibility tests; run formatting and the production dashboard build.
4. Merge the exact green PR head, wait for immutable Boltz image and chart artifacts, deploy with existing Helm values, and verify the live header and release metadata.

## Test plan

Automated coverage verifies ISO 8601 build metadata, health-response serialization, timestamp propagation, relative-age rendering, exact details, malformed/missing fallback, and the unchanged mobile link surface. Existing formatter, Python unit, dashboard, chart, and production-build checks must pass.

Manual verification at desktop width confirms the four shortcuts are absent, the compact label reports deployment age, and the details show the exact check-in and server-start times. Production verification records the reviewed SHA, merge SHA, immutable image and chart versions, Helm revision, rollout completion, authenticated health response, and rendered UI.

## Rollout and rollback

The change ships in the same API-server image and dashboard bundle. Deploy with `helm upgrade ... --reuse-values` and the immutable matching chart version. Roll back to the immediately preceding Helm revision if health, rollout, or UI smoke checks fail. No database migration or state rollback is required.
