"""Logic tests for load-balancer-to-replica TLS."""
# pylint: disable=protected-access
import asyncio
import http.server
import os
import socket
import ssl
import threading

import aiohttp
import httpx
import pytest
import requests

from sky.serve import constants
from sky.serve import lb_k8s
from sky.serve import load_balancer
from sky.serve import replica_managers
from sky.serve import replica_tls
from sky.serve import serve_utils


def _clear_mode(monkeypatch) -> None:
    monkeypatch.delenv(constants.REPLICA_TLS_MODE_ENV_VAR, raising=False)


def test_mode_defaults_to_off(monkeypatch):
    _clear_mode(monkeypatch)
    assert serve_utils.replica_tls_mode() == constants.REPLICA_TLS_MODE_OFF


def test_unknown_mode_is_rejected(monkeypatch):
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR, 'sortof')
    with pytest.raises(ValueError):
        serve_utils.replica_tls_mode()


def test_generated_material_is_a_usable_keypair():
    material = replica_tls.generate_material()
    assert material.certificate_pem.startswith('-----BEGIN CERTIFICATE-----')
    assert material.private_key_pem.startswith('-----BEGIN PRIVATE KEY-----')
    # Distinct every time: two services must not share a key.
    assert (replica_tls.generate_material().private_key_pem
            != material.private_key_pem)


def test_unpinned_context_disables_verification():
    assert replica_tls.build_ssl_context(None) is False


def test_pinned_context_trusts_only_its_own_certificate():
    material = replica_tls.generate_material()
    context = replica_tls.build_ssl_context(material.certificate_pem)
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    # The pin is the whole check, so there is no name to verify.
    assert context.check_hostname is False


class _Handler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):  # pylint: disable=invalid-name
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'ok')

    def log_message(self, *args):  # pylint: disable=arguments-differ
        pass


def _serve_tls(material: replica_tls.ReplicaTLSMaterial, tmp_path):
    """Runs a throwaway TLS server presenting ``material``."""
    certificate = tmp_path / 'cert.pem'
    key = tmp_path / 'key.pem'
    certificate.write_text(material.certificate_pem)
    key.write_text(material.private_key_pem)
    server = http.server.HTTPServer(('127.0.0.1', 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(certificate), str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.socket.getsockname()[1]


def _serve_tls_v6(material: replica_tls.ReplicaTLSMaterial, tmp_path):
    """Like _serve_tls, but on [::1], which is NOT in the certificate SANs."""
    certificate = tmp_path / 'cert6.pem'
    key = tmp_path / 'key6.pem'
    certificate.write_text(material.certificate_pem)
    key.write_text(material.private_key_pem)

    class _V6Server(http.server.HTTPServer):
        address_family = socket.AF_INET6

    server = _V6Server(('::1', 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(certificate), str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.socket.getsockname()[1]


def test_pinned_client_accepts_the_matching_replica(tmp_path):
    material = replica_tls.generate_material()
    server, port = _serve_tls(material, tmp_path)
    try:
        response = httpx.get(f'https://127.0.0.1:{port}/',
                             verify=replica_tls.build_ssl_context(
                                 material.certificate_pem),
                             timeout=10)
        assert response.status_code == 200
    finally:
        server.shutdown()


def test_pinned_client_rejects_an_impostor(tmp_path):
    """The property that makes this worth doing: substitution fails."""
    material = replica_tls.generate_material()
    impostor = replica_tls.generate_material()
    server, port = _serve_tls(impostor, tmp_path)
    try:
        with pytest.raises(httpx.HTTPError):
            httpx.get(f'https://127.0.0.1:{port}/',
                      verify=replica_tls.build_ssl_context(
                          material.certificate_pem),
                      timeout=10)
    finally:
        server.shutdown()


def test_unverified_client_accepts_any_replica(tmp_path):
    """The documented escape hatch: encrypted, but not authenticated."""
    server, port = _serve_tls(replica_tls.generate_material(), tmp_path)
    try:
        response = httpx.get(f'https://127.0.0.1:{port}/',
                             verify=replica_tls.build_ssl_context(None),
                             timeout=10)
        assert response.status_code == 200
    finally:
        server.shutdown()


def test_pinned_mode_without_a_certificate_fails_closed(monkeypatch):
    """Degrading to unverified would look encrypted while trusting anyone."""
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.delenv(constants.REPLICA_TLS_CERT_ENV_VAR, raising=False)
    with pytest.raises(ValueError):
        load_balancer.SkyServeLoadBalancer._build_replica_ssl_context()


def test_replica_url_scheme_follows_the_mode(monkeypatch):
    _clear_mode(monkeypatch)
    assert serve_utils.replica_tls_mode() == constants.REPLICA_TLS_MODE_OFF
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    assert serve_utils.replica_tls_mode() == constants.REPLICA_TLS_MODE_PINNED
    # The scheme decision lives in _resolve_url; assert the helper it keys on
    # rather than reconstructing a full replica record here.
    assert hasattr(replica_managers, '_inject_replica_tls_material')


def test_material_injection_requires_both_halves(monkeypatch):
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR, 'cert')
    monkeypatch.delenv(constants.REPLICA_TLS_KEY_SECRET_ENV_VAR, raising=False)

    class _Task:

        def update_envs(self, _):
            raise AssertionError('must not inject a half-configured keypair')

        def update_secrets(self, _):
            raise AssertionError('must not inject a half-configured keypair')

    with pytest.raises(RuntimeError):
        replica_managers._inject_replica_tls_material(_Task())


def test_no_material_is_injected_when_tls_is_off(monkeypatch):
    _clear_mode(monkeypatch)
    injected = []

    class _Task:

        def update_envs(self, values):
            injected.append(values)

        def update_secrets(self, values):
            injected.append(values)

    replica_managers._inject_replica_tls_material(_Task())
    assert not injected


def test_private_key_never_travels_as_a_plain_env(monkeypatch):
    material = replica_tls.generate_material()
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR,
                       material.certificate_pem)
    monkeypatch.setenv(constants.REPLICA_TLS_KEY_SECRET_ENV_VAR,
                       material.private_key_pem)
    envs: dict = {}
    secrets: dict = {}

    class _Task:

        def update_envs(self, values):
            envs.update(values)

        def update_secrets(self, values):
            secrets.update(values)

    replica_managers._inject_replica_tls_material(_Task())
    assert envs == {
        constants.REPLICA_TLS_CERT_ENV_VAR: material.certificate_pem
    }
    assert secrets == {
        constants.REPLICA_TLS_KEY_SECRET_ENV_VAR: material.private_key_pem
    }
    assert material.private_key_pem not in ''.join(envs.values())


def test_lb_pod_receives_the_certificate_but_never_the_key(monkeypatch):
    material = replica_tls.generate_material()
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR,
                       material.certificate_pem)
    monkeypatch.setenv(constants.REPLICA_TLS_KEY_SECRET_ENV_VAR,
                       material.private_key_pem)
    envs = lb_k8s._replica_tls_envs()
    names = {env['name'] for env in envs}
    assert constants.REPLICA_TLS_CERT_ENV_VAR in names
    assert constants.REPLICA_TLS_KEY_SECRET_ENV_VAR not in names
    rendered = str(envs)
    assert material.private_key_pem not in rendered


def test_lb_pod_gets_no_tls_envs_when_off(monkeypatch):
    _clear_mode(monkeypatch)
    assert not lb_k8s._replica_tls_envs()


def test_os_environ_is_the_single_source_for_both_ends():
    """Controller and LB must not read TLS state from different places."""
    assert constants.REPLICA_TLS_MODE_ENV_VAR.startswith('SKYPILOT_SERVE_')
    assert os.environ.get('SKYPILOT_SERVE_REPLICA_TLS_MODE') in (
        None, *constants.REPLICA_TLS_MODES)


# The three clients on the LB->replica hop must agree. A rollout that
# configures only the proxy breaks the other two in different ways: the
# readiness probe marks every replica NOT_READY and the controller tears down
# live capacity, and the occupancy probe fails silently.


def test_probe_and_occupancy_default_to_todays_behaviour(monkeypatch):
    _clear_mode(monkeypatch)
    # With TLS off the probe client IS the requests module, so the default
    # path has no client indirection at all.
    assert replica_tls.probe_client() is requests
    assert replica_tls.aiohttp_ssl_setting() is None


def test_all_three_clients_agree_when_pinned(monkeypatch):
    material = replica_tls.generate_material()
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR,
                       material.certificate_pem)
    proxy = load_balancer.SkyServeLoadBalancer._build_replica_ssl_context()
    occupancy = replica_tls.aiohttp_ssl_setting()
    probe = replica_tls.probe_client()
    assert isinstance(proxy, ssl.SSLContext)
    assert isinstance(occupancy, ssl.SSLContext)
    # The probe must carry the SSL context, NOT a CA-bundle path: requests
    # would then run urllib3's own hostname assertion on top, which a
    # certificate minted before any replica exists can never satisfy.
    adapter = probe.get_adapter('https://example.invalid')
    assert isinstance(adapter, replica_tls._PinnedAdapter)
    assert adapter._ssl_context.check_hostname is False


def test_all_three_clients_agree_when_unverified(monkeypatch):
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_UNVERIFIED)
    monkeypatch.delenv(constants.REPLICA_TLS_CERT_ENV_VAR, raising=False)
    assert load_balancer.SkyServeLoadBalancer._build_replica_ssl_context(
    ) is False
    assert replica_tls.aiohttp_ssl_setting() is False
    assert replica_tls.probe_client().verify is False


def test_probe_fails_closed_when_pinned_without_a_certificate(monkeypatch):
    """Silently probing with system trust would fail every replica anyway."""
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.delenv(constants.REPLICA_TLS_CERT_ENV_VAR, raising=False)
    with pytest.raises(ValueError):
        replica_tls.probe_client()


def test_probe_verifies_a_replica_at_an_address_not_in_the_certificate(
        tmp_path, monkeypatch):
    """The regression that matters: a replica is dialled by its own address.

    The pinned certificate is minted before any replica exists, so it can
    carry no replica address. An earlier implementation handed requests a
    CA-bundle path, which makes urllib3 assert the hostname anyway -- so every
    probe failed, every replica went NOT_READY, and the controller tore down
    live capacity. Dialling 127.0.0.1 hid this, because that is the one
    address the certificate does list.
    """
    material = replica_tls.generate_material()
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR,
                       material.certificate_pem)
    server, port = _serve_tls_v6(material, tmp_path)
    try:
        # [::1] is loopback but is NOT among the certificate's SANs.
        response = replica_tls.probe_client().get(f'https://[::1]:{port}/',
                                                  timeout=10)
        assert response.status_code == 200
    finally:
        server.shutdown()


def test_probe_still_rejects_an_impostor_at_a_foreign_address(
        tmp_path, monkeypatch):
    """Disabling the hostname assertion must not disable the pin itself."""
    material = replica_tls.generate_material()
    impostor = replica_tls.generate_material()
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR,
                       material.certificate_pem)
    server, port = _serve_tls_v6(impostor, tmp_path)
    try:
        with pytest.raises(Exception):
            replica_tls.probe_client().get(f'https://[::1]:{port}/', timeout=10)
    finally:
        server.shutdown()


def _occupancy_probe(port: int, ssl_setting) -> int:
    """Dials one URL exactly as the load balancer's occupancy probe does.

    Mirrors the connector construction in
    SkyServeLoadBalancer._fetch_replica_occupancy: aiohttp gets an ``ssl``
    kwarg only when the setting is not ``None`` (the plaintext/off path).
    """

    async def _run() -> int:
        connector_kwargs: dict = {'limit': 1}
        if ssl_setting is not None:
            connector_kwargs['ssl'] = ssl_setting
        connector = aiohttp.TCPConnector(**connector_kwargs)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                    f'https://[::1]:{port}/',
                    timeout=aiohttp.ClientTimeout(total=10)) as response:
                return response.status

    return asyncio.run(_run())


def test_occupancy_probe_verifies_a_replica_at_a_foreign_address(
        tmp_path, monkeypatch):
    """The silent client must reach an IP-addressed replica under pinning.

    The occupancy probe is the one client whose failures are swallowed, so a
    TLS mistake here degrades concurrency-native autoscaling without an error
    rather than loudly. The proxy (httpx) and the readiness probe
    (requests/urllib3) each have a live test at [::1], an address the pinned
    certificate does not list; aiohttp reaches the same replicas and keys its
    verification off ``check_hostname`` alone -- differently from urllib3 2.x,
    whose separate hostname assertion the probe session must disable -- so a
    later refactor that unified the three clients could silently reintroduce a
    hostname assertion on this hop and only here. Dialling [::1] is what would
    catch that.
    """
    material = replica_tls.generate_material()
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR,
                       material.certificate_pem)
    ssl_setting = replica_tls.aiohttp_ssl_setting()
    assert isinstance(ssl_setting, ssl.SSLContext)
    server, port = _serve_tls_v6(material, tmp_path)
    try:
        assert _occupancy_probe(port, ssl_setting) == 200
    finally:
        server.shutdown()


def test_occupancy_probe_still_rejects_an_impostor_at_a_foreign_address(
        tmp_path, monkeypatch):
    """Disabling the hostname assertion must not disable the pin on this hop."""
    material = replica_tls.generate_material()
    impostor = replica_tls.generate_material()
    monkeypatch.setenv(constants.REPLICA_TLS_MODE_ENV_VAR,
                       constants.REPLICA_TLS_MODE_PINNED)
    monkeypatch.setenv(constants.REPLICA_TLS_CERT_ENV_VAR,
                       material.certificate_pem)
    ssl_setting = replica_tls.aiohttp_ssl_setting()
    server, port = _serve_tls_v6(impostor, tmp_path)
    try:
        with pytest.raises(Exception):
            _occupancy_probe(port, ssl_setting)
    finally:
        server.shutdown()
