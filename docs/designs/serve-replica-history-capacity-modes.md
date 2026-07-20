# SkyServe replica history capacity modes

_Created: 2026-07-20_

## Problem

The service dashboard graphs only physical replica rows. Services that use
logical replicas can place multiple schedulable slots on one backend, so the
physical graph cannot be compared directly with logical autoscaler targets.
The graph also combines ordinary ready capacity and ready capacity launched by
the free reserved-capacity fill policy.

## Behavior contract

- Show Logical and Physical modes, with Logical selected by default.
- Logical counts weight each durable replica row by its positive
  `planned_capacity`; invalid, missing, or legacy values have width one.
- Physical counts weight each durable replica row as one backend.
- In both modes, split ready capacity into ordinary Ready and Ready free
  reserved series.
- Attribute free reserved capacity from the durable `reserved_fill` launch
  origin flag. This does not claim that an ordinary replica is billed or that
  a fill replica is currently assigned to demand.
- Preserve missing minutes as gaps. For history recorded before this schema,
  preserve the physical ready total and omit the unavailable reserved split;
  do not fabricate logical or reserved zeroes.
- Keep per-version tooltip rows in the selected unit.

## Data design

Extend `serve_replica_status_history` with a nullable physical
`ready_reserved_count`, nullable logical counts for each existing status
bucket, and a nullable logical total. New snapshots populate all fields in the
same PostgreSQL query already used once per minute. The query reads
`replica_state.planned_capacity` and `replica_state.reserved_fill`, then groups
by service, version, and status. It adds no provider call, controller call,
poller, or replica-info deserialization.

The new fields remain nullable so an upgraded dashboard can distinguish old
rows from genuine zeroes. Existing physical status and total columns remain
unchanged for old clients. Check constraints keep reserved-ready counts within
ready totals and require logical counts to be either complete or wholly null.

## Dashboard design

The selected mode maps the same status colors to physical or logical fields.
Ready is rendered as the ready total minus its reserved subset, with the free
reserved subset as a separate stacked series. When a historical physical row
lacks the split, the total remains in Ready and the reserved dataset has a gap.
Logical rows with any missing logical field are gaps because summing only the
upgraded versions would undercount the minute.

Summary labels, the y-axis title, integral error and stopping minutes, and
per-version tooltips use the selected unit. Changing modes does not change the
shared time selection.

## Compatibility and rollout

Central history remains PostgreSQL-only. The migration is idempotent for fresh
databases because an earlier migration creates the table from current module
metadata. Old API servers ignore the new columns until they are deployed; old
dashboards ignore the additional response keys.

After deployment, the first new minute has both modes. Earlier physical
minutes remain visible without a reserved split, while earlier logical minutes
remain gaps. Verify the migration, one fresh sample, both toggle states, and
the live API and dashboard bundle before declaring the rollout complete.

## Test plan

- Unit-test status aggregation across versions, logical widths, free reserved
  attribution, zero capacity, and generated PostgreSQL SQL.
- Run the PostgreSQL migration chain and snapshot integration tests.
- Test response normalization, including nullable old-server fields.
- Test both chart modes, default selection, ready splitting, missing-field
  gaps, statistics, per-version tooltips, and shared-range behavior.
- Run formatting, focused Python and dashboard suites, dashboard lint and
  production build, then require the full visible CI rollup on the exact head.
