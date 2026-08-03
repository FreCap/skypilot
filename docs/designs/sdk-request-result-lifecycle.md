# SDK request-result lifecycle decomposition

## Problem

`sky/client/sdk.py` is a 3,228-line public facade that currently owns endpoint
request construction, prepared-launch serialization, request-result decoding
and streaming, local API-server process management, SSH node-pool setup, and
debug-dump submission.  Line count alone does not justify a split, but the
request-result lifecycle has a stable boundary and changes for different
reasons from the endpoint-specific submission APIs around it.

The bounded candidate consists of `stream_response`, `get`, and
`stream_and_get`.  Together they own authenticated result retrieval, decoding a
serialized `Request`, remote-error projection, cancellation projection,
stream-request identity checks, interactive stream handling, and retry progress
bookkeeping.  Their public locations and their internal calls through
`sky.client.sdk.get` and `sky.client.sdk.stream_response` are historical test
and extension seams.

## Responsibility map

### Endpoint request submission

Callers include the CLI, the synchronous and asynchronous SDKs, SkyServe, and
managed jobs.  Dependencies are endpoint payload models, API-version gates,
admin policy, DAG and resource serialization, and `server_common`.  It owns no
durable state, but it constructs endpoint-specific request bodies and request
IDs.  Failures are validation, compatibility, serialization, and HTTP errors.
Its performance is dominated by validation, serialization, and one network
submission.  It changes with individual product APIs.

### Prepared-launch serialization

Callers are `launch`, `prepare_launch_request`, and
`submit_prepared_launch_request`.  Dependencies include DAG copying, canonical
JSON, file-mount upload state, client context, and API-version compatibility.
It owns frozen request bytes and context fingerprints.  Failures are stale or
mismatched prepared requests and upload or policy errors.  Copy and hashing
costs are performance-sensitive.  It changes with launch semantics.

### Request-result lifecycle

Callers include the top-level SDK, the CLI, managed jobs, SkyServe, load tests,
and smoke tests.  Dependencies are `server_common`, `requests_lib`, request
payload models, retry state, rich-status decoding, interactive authentication,
client error projection, and the shared SDK logger.  It owns only per-call
stream counters and the retry context supplied by `rest`; it owns no process or
durable state.  Failures include malformed result payloads, remote exceptions,
cancelled requests, HTTP stream errors, mismatched request IDs, and interrupted
streams.  It is latency-sensitive for stream handling and must not add network,
decode, copy, or retry operations.  Its history changes with retry, streaming,
and request protocol behavior rather than endpoint schemas.

### API request administration and local server management

`api_cancel` and `api_status` construct administration requests.  The API
server functions additionally inspect and terminate local processes, coordinate
managed-job controllers, acquire a file lock, remove the server socket, and
tail local logs.  Their callers are the API CLI, scheduler recovery, and the
async SDK.  Their state and failures are process, filesystem, and lifecycle
oriented, and they change independently from result decoding.

### SSH node-pool and debug-dump helpers

These functions coordinate local files, subprocesses, upload APIs, and
platform metadata for their respective product APIs.  Their failures and
change cadence are unrelated to request-result decoding.

## Solution

Add one plain implementation module, `sky/client/request_results.py`, to own the
request-result lifecycle.  Keep the public functions, overloads, decorators,
docstrings, signatures, module identities, and import paths in
`sky.client.sdk`.  The facade functions delegate once to implementation
functions.

The implementation receives callbacks only where current behavior resolves a
name dynamically from the SDK module:

- `stream_response` receives the current `sdk.get`, preserving tests and
  extensions that patch result retrieval.
- `stream_and_get` receives the current `sdk.get` and `sdk.stream_response`,
  preserving the fallback and stream-dispatch patch seams.
- `get` receives the current `_raise_exception_object_on_client`, preserving
  the shared remote-error projection used by both `get` and `validate`.

All other dependencies remain ordinary module-scope imports.  There is no new
class, protocol, registry, factory, or dependency-injection layer.  The facade
continues to own the public API contract; the new module owns transport/result
implementation.

## Behavior contract

- Public import paths and `sky` re-exports remain unchanged.
- Public names, signatures, decorator order, documentation, and function module
  identities remain unchanged.
- Existing patches of `sky.client.sdk.get`, `stream_response`, and
  `_raise_exception_object_on_client` continue to affect the same internal
  calls.
- HTTP method, path, parameters, timeout, retry, and streaming flags remain
  unchanged.
- Request decoding, error precedence, cancellation messages, and returned
  values remain unchanged.
- Streaming output, interactive-auth handling, retry progress, resume high-water
  marks, fallback behavior, and request-ID validation remain unchanged.
- No additional authenticated request, JSON decode, `Request.decode`, rich
  status decode, copy, retry, or stream iteration is introduced.

## Alternatives considered

Leaving the code in place avoids one module and one facade call, but retains a
high-fan-in transport/result subsystem inside an endpoint-submission module and
makes unrelated protocol changes continue to expand the facade.

Directly importing moved public functions back into `sdk.py` is smaller, but it
changes function module and pickle identity and causes internal calls to bypass
historical SDK monkeypatch seams.

Extracting local API-server process control instead was rejected for this run.
That family shares shutdown ordering with managed-job controller records,
filesystem locks, process discovery, and scheduler recovery, so its ownership
boundary is more stateful and riskier.

Splitting streaming, result decoding, and remote-error projection into separate
modules was rejected because they form one request-result lifecycle and share
the same callers, retry/error contract, and reasons to change.

## Milestones

1. Add and run characterization tests against the unsplit implementation.
2. Move the three implementation bodies behind the stable SDK facade without a
   behavior change.
3. Run the changed-path test matrix, formatting, type and lint gates, import and
   CLI checks, and exact-base performance comparisons.
4. Push one PR and require all relevant CI and review gates on its exact head
   before merge.

## Test and rollout plan

Characterization covers public module and signature identity, top-level
re-exports, and each dynamic SDK patch seam.  Existing request-result tests in
`tests/unit_tests/test_sky/server/test_sdk.py` cover HTTP fallbacks, mismatch
rejection, retry progress, resume behavior, interactive authentication, remote
errors, and cancellation.  SDK type and async suites cover the public and async
callers.

The changed-path matrix maps both production files to the new contract test,
the server SDK suite, SDK type and async suites, CLI API tests, managed-jobs
client tests, and relevant smoke-test collection.  Formatting, full mypy,
Pylint, Ruff, BasedPyright, import-linter, both import orders, compileall,
dashboard checks, and `git diff --check` are required.  Performance evidence
will compare alternating exact-base and head imports plus representative empty
stream and decoded-result paths while proving identical operation counts.

This is a structural extraction only.  Rollback is a normal revert of the
single PR; there is no schema, configuration, serialized-data, or server rollout.
