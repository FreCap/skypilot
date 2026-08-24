"""Fail-closed load-balancer client contracts for async ledger receipts."""
# pylint: disable=protected-access

import asyncio
import dataclasses
import hashlib
import json
from unittest import mock
import uuid

import fastapi
import pytest

from sky.serve import async_request_ledger_client as ledger_client
from sky.serve import constants

_REQUEST_ID = 'stable-request-1'
_JOB_ID = 'durable-job-9'
_INTENT = 'a' * 64
_SERVICE_INCARNATION = '11111111-1111-4111-8111-111111111111'
_BODY = json.dumps(
    {
        'action': 'async_predict',
        'payload': {
            'input': 's3://bucket/input'
        },
        'request_id': _REQUEST_ID,
    },
    sort_keys=True,
    separators=(',', ':')).encode()


def _request(*headers: tuple[bytes, bytes], method: str = 'POST'):
    scope = {
        'type': 'http',
        'http_version': '1.1',
        'method': method,
        'scheme': 'https',
        'path': '/predict',
        'raw_path': b'/predict',
        'query_string': b'',
        'headers': list(headers),
        'client': ('127.0.0.1', 1234),
        'server': ('test', 443),
    }
    return fastapi.Request(scope)


def _identity_request(
    *extra_headers: tuple[bytes, bytes],
    service_incarnation: str | None = _SERVICE_INCARNATION,
):
    incarnation_headers = (() if service_incarnation is None else ((
        constants.LB_ASYNC_SERVICE_INCARNATION_HEADER.lower().encode(),
        service_incarnation.encode()),))
    return _request(
        (constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER.lower().encode(), b'1'),
        (constants.LB_ASYNC_INTENT_SHA256_HEADER.lower().encode(),
         _INTENT.encode()),
        (constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER.lower().encode(),
         _REQUEST_ID.encode()),
        (constants.LB_JOB_ID_HEADER.lower().encode(), _JOB_ID.encode()),
        (b'content-type', b'application/json'), *incarnation_headers,
        *extra_headers)


def _receipt(*,
             attempt_no=1,
             state='DISPATCH_MAY_HAVE_OCCURRED',
             revision=1,
             duplicate=False,
             dispatch_authorized=True,
             attempt_id=None):
    return ledger_client.AsyncLedgerReceipt(
        request_key_sha256=hashlib.sha256(_REQUEST_ID.encode()).hexdigest(),
        attempt_id=attempt_id or str(uuid.uuid4()),
        attempt_no=attempt_no,
        state=state,
        revision=revision,
        duplicate=duplicate,
        dispatch_authorized=dispatch_authorized)


def test_parse_identity_requires_explicit_complete_protocol() -> None:
    assert ledger_client.parse_identity(_request(), 1,
                                        _SERVICE_INCARNATION) is None
    identity = ledger_client.parse_identity(_identity_request(), 1,
                                            _SERVICE_INCARNATION, _BODY)
    assert identity == ledger_client.AsyncLedgerIdentity(
        _REQUEST_ID, _INTENT, _JOB_ID)

    with pytest.raises(fastapi.HTTPException) as error:
        ledger_client.parse_identity(
            _identity_request(
                (constants.LB_ASYNC_ATTEMPT_ID_HEADER.lower().encode(),
                 str(uuid.uuid4()).encode())), 1, _SERVICE_INCARNATION, _BODY)
    assert error.value.status_code == 400


def test_parse_identity_fails_closed_without_server_advertisement() -> None:
    with pytest.raises(fastapi.HTTPException) as error:
        ledger_client.parse_identity(_identity_request(), None,
                                     _SERVICE_INCARNATION, _BODY)
    assert error.value.status_code == 503


def test_parse_identity_requires_matching_service_incarnation() -> None:
    with pytest.raises(fastapi.HTTPException) as missing:
        ledger_client.parse_identity(
            _identity_request(service_incarnation=None), 1,
            _SERVICE_INCARNATION, _BODY)
    assert missing.value.status_code == 400

    with pytest.raises(fastapi.HTTPException) as mismatch:
        ledger_client.parse_identity(
            _identity_request(service_incarnation='different-incarnation'), 1,
            _SERVICE_INCARNATION, _BODY)
    assert mismatch.value.status_code == 409

    with pytest.raises(fastapi.HTTPException) as duplicate:
        ledger_client.parse_identity(
            _identity_request(
                (constants.LB_ASYNC_SERVICE_INCARNATION_HEADER.lower().encode(),
                 _SERVICE_INCARNATION.encode())), 1, _SERVICE_INCARNATION,
            _BODY)
    assert duplicate.value.status_code == 400


def test_incarnation_only_declaration_never_downgrades_to_legacy() -> None:
    request = _request(
        (constants.LB_ASYNC_SERVICE_INCARNATION_HEADER.lower().encode(),
         _SERVICE_INCARNATION.encode()), (b'content-type', b'application/json'))

    assert ledger_client.has_identity_declaration(request)
    with pytest.raises(fastapi.HTTPException) as error:
        ledger_client.validate_identity_declaration(request, 1,
                                                    _SERVICE_INCARNATION)
    assert error.value.status_code == 400
    with pytest.raises(fastapi.HTTPException) as parsed:
        ledger_client.parse_identity(request, 1, _SERVICE_INCARNATION, _BODY)
    assert parsed.value.status_code == 400


@pytest.mark.parametrize('body', [
    (b'{"action":"async_status","payload":{},'
     b'"request_id":"stable-request-1"}'),
    b'{"action":"async_predict","payload":{},"request_id":"other"}',
    (b'{"action":"async_predict", "payload":{},'
     b'"request_id":"stable-request-1"}'),
    b'{"action":"async_predict","request_id":"stable-request-1"}',
    (b'{"action":"async_predict","extra":true,"payload":{},'
     b'"request_id":"stable-request-1"}'),
    (b'{"action":"async_predict","request_id":"stable-request-1",'
     b'"request_id":"stable-request-1"}'),
    (b'{"action":"async_predict","payload":{"input":"a","input":"b"},'
     b'"request_id":"stable-request-1"}'),
])
def test_parse_identity_rejects_non_dispatch_or_inexact_payload(
        body: bytes) -> None:
    request = _identity_request()
    with pytest.raises(fastapi.HTTPException) as error:
        ledger_client.parse_identity(request, 1, _SERVICE_INCARNATION, body)
    assert error.value.status_code == 400
    assert ledger_client.get_identity(request) is None


def test_parse_identity_uses_configured_request_queue_body_ceiling() -> None:
    body = json.dumps(
        {
            'action': 'async_predict',
            'payload': {
                'input': 'x' * (constants.LB_ASYNC_ACTION_BODY_MAX_BYTES + 1)
            },
            'request_id': _REQUEST_ID,
        },
        sort_keys=True,
        separators=(',', ':')).encode()
    assert len(body) > constants.LB_ASYNC_ACTION_BODY_MAX_BYTES

    identity = ledger_client.parse_identity(_identity_request(),
                                            1,
                                            _SERVICE_INCARNATION,
                                            body,
                                            max_body_bytes=len(body))
    assert identity == ledger_client.AsyncLedgerIdentity(
        _REQUEST_ID, _INTENT, _JOB_ID)

    with pytest.raises(fastapi.HTTPException) as error:
        ledger_client.parse_identity(_identity_request(),
                                     1,
                                     _SERVICE_INCARNATION,
                                     body,
                                     max_body_bytes=len(body) - 1)
    assert error.value.status_code == 413


def test_parse_identity_uses_rfc8785_body_canonicalization() -> None:
    canonical = (
        '{"action":"async_predict","payload":{"label":"€","small":1e-7},'
        '"request_id":"stable-request-1"}').encode()
    identity = ledger_client.parse_identity(_identity_request(), 1,
                                            _SERVICE_INCARNATION, canonical)
    assert identity == ledger_client.AsyncLedgerIdentity(
        _REQUEST_ID, _INTENT, _JOB_ID)

    python_spelling = json.dumps(
        {
            'action': 'async_predict',
            'payload': {
                'label': '€',
                'small': 1e-7,
            },
            'request_id': _REQUEST_ID,
        },
        sort_keys=True,
        separators=(',', ':')).encode()
    assert python_spelling != canonical
    with pytest.raises(fastapi.HTTPException) as error:
        ledger_client.parse_identity(_identity_request(), 1,
                                     _SERVICE_INCARNATION, python_spelling)
    assert error.value.status_code == 400


def test_parse_identity_matches_caller_jcs_interoperability_vector() -> None:
    canonical = (
        '{"action":"async_predict","payload":{"payload":{"exponent":1e-7,'
        '"unicode":"€"},"submitted_at":"2026-07-07T00:00:00.000Z"},'
        '"request_id":"req-1"}').encode()
    request = _request(
        (constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER.lower().encode(), b'1'),
        (constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER.lower().encode(),
         b'req-1'), (constants.LB_ASYNC_INTENT_SHA256_HEADER.lower().encode(),
                     _INTENT.encode()),
        (constants.LB_ASYNC_SERVICE_INCARNATION_HEADER.lower().encode(),
         _SERVICE_INCARNATION.encode()),
        (constants.LB_JOB_ID_HEADER.lower().encode(), _JOB_ID.encode()),
        (b'content-type', b'application/json'))

    assert ledger_client.parse_identity(
        request, 1, _SERVICE_INCARNATION,
        canonical) == ledger_client.AsyncLedgerIdentity('req-1', _INTENT,
                                                        _JOB_ID)


def test_new_http_request_accepts_postgres_authorized_later_attempt() -> None:
    identity = ledger_client.AsyncLedgerIdentity(_REQUEST_ID, _INTENT, _JOB_ID)
    # A client retry uses a new ASGI Request and therefore has no in-memory
    # prior receipt. PostgreSQL, not LB memory, proves attempt continuity.
    receipt = _receipt(attempt_no=3)
    assert ledger_client.validate_bind_receipt(identity, receipt,
                                               None) is receipt


def test_same_request_retry_requires_exact_attempt_continuity() -> None:
    identity = ledger_client.AsyncLedgerIdentity(_REQUEST_ID, _INTENT, _JOB_ID)
    prior = _receipt(state='REJECTED_PRE_DISPATCH',
                     revision=2,
                     dispatch_authorized=False)
    successor = _receipt(attempt_no=2)
    assert ledger_client.validate_bind_receipt(identity, successor,
                                               prior) is successor

    with pytest.raises(ledger_client.AsyncLedgerTransportError):
        ledger_client.validate_bind_receipt(identity, _receipt(attempt_no=3),
                                            prior)


def test_transition_receipt_cannot_move_between_attempts() -> None:
    identity = ledger_client.AsyncLedgerIdentity(_REQUEST_ID, _INTENT, _JOB_ID)
    prior = _receipt()
    accepted = _receipt(attempt_id=prior.attempt_id,
                        state='ACCEPTED',
                        revision=2,
                        dispatch_authorized=False)
    assert ledger_client.validate_transition_receipt(identity, prior, accepted,
                                                     'accepted') is accepted

    with pytest.raises(ledger_client.AsyncLedgerTransportError):
        ledger_client.validate_transition_receipt(
            identity, prior,
            _receipt(state='ACCEPTED', revision=2, dispatch_authorized=False),
            'accepted')


def test_terminal_observation_receipt_is_exact_and_idempotent() -> None:
    attempt_id = str(uuid.uuid4())
    committed = _receipt(attempt_id=attempt_id,
                         state='SUCCEEDED',
                         revision=8,
                         dispatch_authorized=False)
    assert ledger_client.validate_terminal_observation_receipt(
        _REQUEST_ID, attempt_id, 1, 7, 'SUCCEEDED', committed) is committed

    duplicate = _receipt(attempt_id=attempt_id,
                         state='SUCCEEDED',
                         revision=8,
                         duplicate=True,
                         dispatch_authorized=False)
    assert ledger_client.validate_terminal_observation_receipt(
        _REQUEST_ID, attempt_id, 1, 7, 'SUCCEEDED', duplicate) is duplicate

    # The completion reporter only knows the bind revision.  An intervening
    # ACCEPTED transition may advance the authoritative row more than once.
    advanced = dataclasses.replace(committed, revision=9)
    assert ledger_client.validate_terminal_observation_receipt(
        _REQUEST_ID, attempt_id, 1, 7, 'SUCCEEDED', advanced) is advanced


def test_terminal_observation_rejects_inexact_receipt() -> None:
    attempt_id = str(uuid.uuid4())
    receipt = _receipt(attempt_id=attempt_id,
                       state='SUCCEEDED',
                       revision=8,
                       dispatch_authorized=False)
    invalid_receipts = (
        dataclasses.replace(receipt, request_key_sha256='b' * 64),
        dataclasses.replace(receipt, attempt_id=str(uuid.uuid4())),
        dataclasses.replace(receipt, attempt_no=2),
        dataclasses.replace(receipt, state='FAILED'),
        dataclasses.replace(receipt, revision=7),
        dataclasses.replace(receipt, dispatch_authorized=True),
    )
    for invalid in invalid_receipts:
        with pytest.raises(ledger_client.AsyncLedgerTransportError):
            ledger_client.validate_terminal_observation_receipt(
                _REQUEST_ID, attempt_id, 1, 7, 'SUCCEEDED', invalid)


def test_terminal_lookup_accepts_only_same_attempt_at_newer_revision() -> None:
    attempt_id = str(uuid.uuid4())
    current = _receipt(attempt_id=attempt_id,
                       state='ACCEPTED',
                       revision=2,
                       duplicate=True,
                       dispatch_authorized=False)
    assert ledger_client.validate_terminal_lookup_receipt(
        _REQUEST_ID, attempt_id, 1, 1, current) is current

    for invalid in (
            dataclasses.replace(current, attempt_id=str(uuid.uuid4())),
            dataclasses.replace(current, attempt_no=2),
            dataclasses.replace(current, revision=0),
            dataclasses.replace(current, state='REJECTED_PRE_DISPATCH'),
            dataclasses.replace(current, duplicate=False),
    ):
        with pytest.raises(ledger_client.AsyncLedgerTransportError):
            ledger_client.validate_terminal_lookup_receipt(
                _REQUEST_ID, attempt_id, 1, 1, invalid)


def test_read_only_lookup_maps_only_404_to_absent() -> None:
    client = ledger_client.AsyncRequestLedgerClient('http://controller')
    client._post_raw = mock.AsyncMock(return_value=(404, {
        'detail': 'No durable request attempt exists.'
    }))
    try:
        assert asyncio.run(client.lookup({'operation': 'bind'}, 'hash')) is None
    finally:
        asyncio.run(client.close())

    client = ledger_client.AsyncRequestLedgerClient('http://controller')
    client._post_raw = mock.AsyncMock(return_value=(503, {
        'detail': 'database unavailable'
    }))
    try:
        with pytest.raises(ledger_client.AsyncLedgerTransportError,
                           match='database unavailable'):
            asyncio.run(client.lookup({'operation': 'bind'}, 'hash'))
    finally:
        asyncio.run(client.close())


def test_transport_bounds_total_work_and_reserves_slots_for_mutations(
        monkeypatch) -> None:

    class _Response:
        status_code = 200
        content = b'{}'

        @staticmethod
        def json():
            return {}

    class _BlockingClient:
        """Fake transport that exposes active-call concurrency."""

        def __init__(self) -> None:
            self.release = asyncio.Event()
            self.active = 0
            self.active_lookups = 0
            self.max_active = 0
            self.mutation_started = asyncio.Event()

        async def post(self, unused_url, **kwargs):
            payload = kwargs['json']
            headers = kwargs['headers']
            del unused_url, headers
            is_lookup = (payload.get('operation') == 'bind' and
                         payload.get('allow_new_attempt') is False)
            self.active += 1
            self.active_lookups += int(is_lookup)
            self.max_active = max(self.max_active, self.active)
            if not is_lookup:
                self.mutation_started.set()
            try:
                await self.release.wait()
                return _Response()
            finally:
                self.active -= 1
                self.active_lookups -= int(is_lookup)

        async def aclose(self):
            pass

    transport = _BlockingClient()
    monkeypatch.setattr(ledger_client.httpx, 'AsyncClient',
                        lambda **unused_kwargs: transport)
    monkeypatch.setattr(ledger_client.serve_utils, 'get_lb_sync_auth_tokens',
                        lambda required: ())
    client = ledger_client.AsyncRequestLedgerClient('http://controller')

    async def _run() -> None:
        lookup_payload = {
            'operation': 'bind',
            'allow_new_attempt': False,
        }
        mutation_payload = {'operation': 'accepted'}
        lookups = [
            asyncio.create_task(client._post_raw(lookup_payload, 'hash'))
            for _ in range(32)
        ]
        for _ in range(100):
            if transport.active_lookups == (
                    constants.LB_ASYNC_REQUEST_LEDGER_MAX_LOOKUP_CONCURRENCY):
                break
            await asyncio.sleep(0)
        assert transport.active_lookups == (
            constants.LB_ASYNC_REQUEST_LEDGER_MAX_LOOKUP_CONCURRENCY)

        mutations = [
            asyncio.create_task(client._post_raw(mutation_payload, 'hash'))
            for _ in range(32)
        ]
        await asyncio.wait_for(transport.mutation_started.wait(), timeout=1)
        for _ in range(100):
            if transport.active == constants.LB_ASYNC_REQUEST_LEDGER_MAX_CONCURRENCY:
                break
            await asyncio.sleep(0)
        assert transport.active == constants.LB_ASYNC_REQUEST_LEDGER_MAX_CONCURRENCY
        assert transport.max_active == (
            constants.LB_ASYNC_REQUEST_LEDGER_MAX_CONCURRENCY)

        # Hundreds of callers may await the passive gates without monopolizing
        # the loop that also serves the Kubernetes liveness route.
        await asyncio.wait_for(asyncio.sleep(0), timeout=0.1)
        transport.release.set()
        await asyncio.gather(*lookups, *mutations)
        await client.close()

    asyncio.run(_run())
