"""TLS material for the load-balancer-to-replica hop.

The load balancer reaches replicas over their public IPs, across clouds and
regions, so that hop is the one that genuinely leaves a private network. It
cannot be secured by network topology: replicas are a cross-cloud spot pool and
land wherever capacity exists, so no peering arrangement reaches them.

Conventional certificates do not fit either. A replica has no stable DNS name,
its address is assigned at boot, and it lives for minutes. Public CAs cannot
issue for it, and the deployment has neither cert-manager nor an ACM private CA
(ACM's own certificates cannot be exported to an instance).

So the trust anchor is a keypair the operator mints once and provisions as
configuration, exactly like the load balancer's bearer tokens. ``generate_material``
is that minting utility; nothing calls it at runtime. The controller then hands
the private key to replicas over the provisioning channel as a task secret, and
gives the load balancer and both probes the certificate as their *only* trust
root. Verification succeeds exactly when the peer holds that private key, which
is the property we want; it is certificate pinning expressed through the
standard TLS verifier rather than a bespoke fingerprint check.

Hostname verification is disabled deliberately: the certificate is pinned to a
single trusted key, and no replica address is known when it is minted. Binding
to a name we would then have to ignore only invites a false sense of what is
being checked.

Scope: one keypair for the deployment, not one per replica or per service. A
per-replica keypair would put issuance and propagation on the critical path of
every launch, and there is no mechanism to mint one at provisioning time. The
cost of the shared key is that compromising any replica yields a key accepted
for all of them; that is a real limitation, and it is why this is a transport
control rather than a replica identity system.

Three clients share this hop and must agree, or a rollout breaks in three
different ways: the load balancer's proxy (fails loudly), the controller's
readiness probe (marks every replica NOT_READY and tears down capacity), and
the load balancer's occupancy probe (fails silently and degrades autoscaling).
Every one of them is configured from this module for that reason.
"""
import datetime
import functools
import ipaddress
import os
import ssl
import tempfile
from typing import Any, NamedTuple

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
import requests

from sky.serve import constants
from sky.serve import serve_utils

# Long enough that rotation is never on a replica's critical path, short enough
# that a leaked key is not valid forever. Rotation happens by restarting the
# service, which re-mints and re-propagates.
_CERTIFICATE_LIFETIME_DAYS = 365

# Placeholder subject. Nothing verifies it (see module docstring), so it exists
# only to make the certificate well-formed and recognisable in a packet capture.
_SUBJECT_COMMON_NAME = 'skypilot-serve-replica'


class ReplicaTLSMaterial(NamedTuple):
    """A service's replica-facing TLS keypair.

    ``certificate_pem`` is safe to log or persist. ``private_key_pem`` is not,
    and must only ever travel as a task secret.
    """
    certificate_pem: str
    private_key_pem: str


def generate_material() -> ReplicaTLSMaterial:
    """Mints one self-signed P-256 keypair for a service's replicas."""
    private_key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, _SUBJECT_COMMON_NAME)])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder().subject_name(subject).issuer_name(
            subject).public_key(private_key.public_key()).
        serial_number(x509.random_serial_number()).not_valid_before(
            # Tolerate modest clock skew between the control plane and
            # a freshly booted replica, which would otherwise reject a
            # certificate that is not yet valid.
            now - datetime.timedelta(hours=1)).not_valid_after(
                now + datetime.timedelta(days=_CERTIFICATE_LIFETIME_DAYS)).
        add_extension(
            # A replica's address is unknown at minting time and changes on
            # every launch, so SANs cannot be meaningful here. Loopback is
            # recorded because the TLS proxy and its health probe share the
            # instance.
            x509.SubjectAlternativeName([
                x509.IPAddress(ipaddress.ip_address('127.0.0.1')),
                x509.DNSName('localhost'),
            ]),
            critical=False,
        ).add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        ).sign(private_key, hashes.SHA256()))
    return ReplicaTLSMaterial(
        certificate_pem=certificate.public_bytes(
            serialization.Encoding.PEM).decode(),
        private_key_pem=private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()).decode())


@functools.lru_cache(maxsize=1)
def _self_signed_serving_files() -> tuple[str, str]:
    """Mints this process's own throwaway serving keypair, returning paths.

    Cached so a restart is the only thing that rotates it, which is exactly the
    right lifetime: the certificate is never distributed and never trusted by
    name. Written 0600 because unlike the pinned certificate, this pair
    includes a private key.
    """
    material = generate_material()
    directory = tempfile.mkdtemp(prefix='skypilot-lb-tls-')
    certificate_path = os.path.join(directory, 'cert.pem')
    key_path = os.path.join(directory, 'key.pem')
    for path, contents in ((certificate_path, material.certificate_pem),
                           (key_path, material.private_key_pem)):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, 'w') as handle:
            handle.write(contents)
    return certificate_path, key_path


def uvicorn_tls_kwargs() -> dict[str, str]:
    """``ssl_*`` kwargs so the load balancer serves HTTPS on its own port.

    Empty unless HTTPS_ONLY is set, so the plaintext path is untouched during
    the migration window and by default.

    The certificate is self-signed and minted per process. Nothing validates
    it: an NLB TLS target group does not verify its backend, and kubelet does
    not verify HTTPS probe certificates. That is the point -- this hop needs
    encryption, not a second identity system, and a distributed certificate
    here would add rotation and trust plumbing for no gain.
    """
    if os.environ.get(constants.EXTERNAL_LB_HTTPS_ONLY_ENV_VAR,
                      '').strip().lower() != 'true':
        return {}
    certificate_path, key_path = _self_signed_serving_files()
    return {'ssl_certfile': certificate_path, 'ssl_keyfile': key_path}


def _configured_certificate_pem() -> str:
    return os.environ.get(constants.REPLICA_TLS_CERT_ENV_VAR, '').strip()


class _PinnedAdapter(requests.adapters.HTTPAdapter):
    """Applies the pinned SSL context to a ``requests`` connection pool.

    Passing a CA bundle path to ``verify=`` is NOT equivalent: urllib3 then
    runs its own hostname assertion on top of the certificate check, and the
    pinned certificate deliberately carries no replica address (none exists
    when it is minted). Every probe would fail on an IP mismatch, which is the
    exact fleet-teardown this module exists to prevent. The context must be
    injected into the pool manager, and urllib3 2.x's separate assertion
    disabled alongside it.
    """

    def __init__(self, ssl_context: ssl.SSLContext, **kwargs) -> None:
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self._ssl_context
        kwargs['assert_hostname'] = False
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs['ssl_context'] = self._ssl_context
        kwargs['assert_hostname'] = False
        return super().proxy_manager_for(*args, **kwargs)


@functools.lru_cache(maxsize=1)
def _pinned_probe_session(certificate_pem: str) -> requests.Session:
    context = build_ssl_context(certificate_pem)
    # build_ssl_context only returns a bool on the unverified path, which is
    # handled by its own session and never reaches here.
    assert isinstance(context, ssl.SSLContext), context
    session = requests.Session()
    session.mount('https://', _PinnedAdapter(context))
    return session


@functools.lru_cache(maxsize=1)
def _unverified_probe_session() -> requests.Session:
    session = requests.Session()
    session.verify = False
    return session


def probe_client() -> Any:
    """HTTP client for the controller's readiness probe.

    The readiness probe is a separate client from the load balancer's proxy,
    in a separate process, and it decides whether a replica lives. If it
    trusted less than the proxy, a TLS rollout would mark every healthy replica
    NOT_READY and the controller would tear down live capacity; if it trusted
    more, readiness would not mean reachable.

    With TLS off this returns the ``requests`` module itself, whose ``get`` and
    ``post`` have the same signature as a session's, so the default path is
    byte-identical to having no client indirection at all.
    """
    mode = serve_utils.replica_tls_mode()
    if mode == constants.REPLICA_TLS_MODE_OFF:
        return requests
    if mode == constants.REPLICA_TLS_MODE_UNVERIFIED:
        return _unverified_probe_session()
    certificate_pem = _configured_certificate_pem()
    if not certificate_pem:
        raise ValueError(
            f'{constants.REPLICA_TLS_MODE_ENV_VAR}='
            f'{constants.REPLICA_TLS_MODE_PINNED} requires '
            f'{constants.REPLICA_TLS_CERT_ENV_VAR} in the controller '
            'environment; probing would otherwise fail every replica.')
    return _pinned_probe_session(certificate_pem)


def aiohttp_ssl_setting() -> 'ssl.SSLContext | bool | None':
    """``ssl=`` for the load balancer's occupancy probe.

    A third client on the same hop. Its failures are swallowed by the caller,
    so a TLS mistake here degrades concurrency-native autoscaling silently
    instead of erroring; it must therefore be configured, not left to default.

    ``None`` means "leave aiohttp's default alone", used when TLS is off.
    """
    mode = serve_utils.replica_tls_mode()
    if mode == constants.REPLICA_TLS_MODE_OFF:
        return None
    if mode == constants.REPLICA_TLS_MODE_UNVERIFIED:
        return False
    return build_ssl_context(_configured_certificate_pem() or None)


def build_ssl_context(certificate_pem: str | None) -> 'ssl.SSLContext | bool':
    """SSL verification setting for the load balancer's replica client.

    Returns a value suitable for ``httpx``'s ``verify=``.

    With a certificate, the returned context trusts that certificate and
    nothing else, which pins the connection to the holder of its private key.
    Without one, verification is disabled: that still defeats passive
    interception but NOT an active man-in-the-middle, so it is only for
    deployments that cannot distribute the material.
    """
    if not certificate_pem:
        return False
    context = ssl.create_default_context(cadata=certificate_pem)
    # See module docstring: the pin is the whole check; there is no name to
    # verify. Order matters -- check_hostname must be cleared before the
    # verify_mode assignment is meaningful.
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    return context
