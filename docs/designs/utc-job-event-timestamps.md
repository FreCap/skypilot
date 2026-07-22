# UTC-aware managed job event timestamps

## Problem

Managed job events use a SQLAlchemy `DateTime(timezone=True)` column. PostgreSQL
therefore stores them as `TIMESTAMP WITH TIME ZONE`, and migration 010 defines
legacy naive timestamps as UTC. The event writers and retention cutoff still use
naive `datetime.now()` values.

When a controller process runs outside UTC while its PostgreSQL session uses
UTC, the driver sends the local wall-clock value without an offset. PostgreSQL
interprets that value in the session timezone. For example, a controller in
Europe/London during summer stores an event one hour in the future. This can
misorder event timelines and delay retention.

## Behavior contract

- Every newly generated managed job event timestamp is timezone-aware UTC.
- An explicit aware timestamp preserves its instant and is normalized to UTC.
- An explicit naive timestamp remains backward compatible with migration 010:
  it is interpreted as UTC, not as the controller's local timezone.
- The retention cutoff is timezone-aware UTC.
- SQLite-backed controller databases keep working. SQLite may return naive
  datetimes, but the value written represents the same UTC wall-clock instant.
- This change does not rewrite historical rows or alter the database schema.

## Design

Add a small timestamp-normalization helper in the managed job event repository.
The synchronous and asynchronous event writers call it for both default and
explicit timestamps. Batch lifecycle transitions, which insert event rows in
their owner-fenced transaction, generate aware UTC timestamps directly. The
retention query also computes its cutoff from aware UTC now.

Keeping Batch event insertion in the existing transaction preserves atomicity
with the lifecycle transition. Moving those writes through the event repository
would split the transaction and is therefore rejected.

## Alternatives

- Setting the process or PostgreSQL session timezone to UTC is insufficient:
  library code and deployments can run with other process timezones, and a
  naive value still carries no proof of its intended timezone.
- Using PostgreSQL `CURRENT_TIMESTAMP` would fix server-generated values but
  would not normalize explicit timestamps and would diverge from SQLite.
- Rewriting historical rows is out of scope because their originating process
  timezone is not reliably knowable.

## Rollout and compatibility

The change is application-only and can roll out across mixed controller
versions. New writers become correct immediately. Existing shifted rows remain
unchanged and naturally age out under retention.

## Test plan

- Unit-test naive and offset-aware timestamp normalization.
- Exercise synchronous and asynchronous event writers and retention on the
  existing SQLite test database.
- Exercise owner-fenced Batch terminal transitions.
- Run Ruff's DTZ rules on the touched Jobs modules.
- Against a disposable PostgreSQL 16 database with a UTC session and an
  Asia/Kolkata process, verify that default sync, async, and Batch event writes
  are stored at the actual UTC instant. Also verify that retention preserves an
  explicit recent event and removes an explicit two-hour-old event.
- Run the focused managed-job state tests, formatting, type checking, Ruff,
  BasedPyright, and the async lifecycle baseline.
