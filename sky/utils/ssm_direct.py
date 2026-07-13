"""Opportunistic direct SSH for clusters configured with SSM proxy commands.

With ``aws.use_ssm``, every SSH connection spawns ``aws ssm start-session``,
which is throttled account-wide at a low TPS (default 3). When the target is
also directly reachable (public IP, or an in-VPC route), the proxy adds no
reachability -- only quota pressure. This module lets SkyPilot drop the proxy
for targets that have been *verified* directly reachable, while keeping the
SSM proxy as the recorded transport in the cluster YAML and as the runtime
fallback, so nothing depends on the network staying open.

Design constraints (from adversarial review):
- A bare TCP accept is NOT proof that SSH works through the direct path, so
  only a full proxy-less SSH handshake (performed by ``wait_for_ssh`` during
  provisioning) may mark a target direct-ok.
- ``SSHCommandRunner`` construction sits on hot paths, so the bypass decision
  there is cache-only and never blocks on the network.
- The bypass self-heals in both directions: a transport failure (ssh exit
  255) on a bypassed runner restores the proxy for that runner and poisons
  the cache entry, so an IP whose instance was replaced or whose security
  group was tightened falls back to SSM within one failed attempt.
"""
import socket
import threading
import time

from sky import sky_logging

logger = sky_logging.init_logger(__name__)

_TCP_PROBE_TIMEOUT_SECONDS = 1.5
_DIRECT_OK_TTL_SECONDS = 30 * 60
_DIRECT_FAILED_TTL_SECONDS = 10 * 60

_cache_lock = threading.Lock()
# (ip, port) -> (direct_ok, recorded_at). Entries expire by state-specific
# TTL: a stale positive is dangerous (IP reuse), a stale negative only delays
# re-enabling the optimization.
_cache: dict[tuple[str, int], tuple[bool, float]] = {}


def is_skypilot_ssm_proxy(proxy_command: str | None) -> bool:
    """Whether the proxy command is a SkyPilot-generated SSM session.

    Matches both the current form (with the adaptive-retry export prefix)
    and legacy YAML-recorded forms. Deliberately does not match arbitrary
    user-supplied SSM invocations with a different shape: bypassing a
    custom proxy could break auth paths we know nothing about.
    """
    if proxy_command is None:
        return False
    return ('aws ssm start-session --target' in proxy_command and
            'AWS-StartSSHSession' in proxy_command)


def is_enabled() -> bool:
    """Whether the direct-SSH bypass is enabled (aws.ssm_direct_fallback).

    Note: skypilot_config is context/thread-local; callers that fan out to
    worker threads must read this once in the parent thread.
    """
    # Deferred import: this module is imported by command_runner, whose
    # import graph (exceptions -> backends -> cluster_utils) is entangled
    # with skypilot_config's; importing it at module level risks a cycle.
    from sky import skypilot_config  # pylint: disable=import-outside-toplevel
    try:
        return bool(
            skypilot_config.get_nested(('aws', 'ssm_direct_fallback'), True))
    except Exception:  # pylint: disable=broad-except
        # Config not loadable in this context (e.g. on-cluster processes):
        # allow the bypass -- it only ever activates on cache entries that
        # were recorded by a successful direct handshake.
        return True


def tcp_reachable(ip: str,
                  port: int,
                  timeout: float = _TCP_PROBE_TIMEOUT_SECONDS) -> bool:
    """Blocking TCP probe. Only a pre-filter: callers must still verify
    with a real SSH handshake before calling mark_direct_ok()."""
    try:
        socket.create_connection((ip, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def mark_direct_ok(ip: str, port: int) -> None:
    """Record that a full proxy-less SSH handshake to (ip, port) succeeded."""
    with _cache_lock:
        _cache[(ip, port)] = (True, time.time())


def mark_direct_failed(ip: str, port: int) -> None:
    """Record that the direct path to (ip, port) is not usable for SSH."""
    with _cache_lock:
        _cache[(ip, port)] = (False, time.time())


def _cached_direct_ok(ip: str, port: int) -> bool:
    now = time.time()
    with _cache_lock:
        entry = _cache.get((ip, port))
        if entry is None:
            return False
        direct_ok, recorded_at = entry
        ttl = (_DIRECT_OK_TTL_SECONDS
               if direct_ok else _DIRECT_FAILED_TTL_SECONDS)
        if now - recorded_at > ttl:
            del _cache[(ip, port)]
            return False
        return direct_ok


def maybe_bypass_proxy(ip: str, port: int,
                       proxy_command: str | None) -> str | None:
    """Returns None (drop the proxy -> direct SSH) for verified targets.

    Cache-only and non-blocking: never touches the network, so it is safe
    on runner-construction hot paths. Returns the proxy command unchanged
    for non-SkyPilot-SSM proxies, unverified targets, or when disabled.
    """
    if not is_skypilot_ssm_proxy(proxy_command):
        return proxy_command
    if not _cached_direct_ok(ip, port):
        return proxy_command
    if not is_enabled():
        return proxy_command
    logger.debug(f'Bypassing SSM proxy for {ip}:{port} '
                 '(verified directly reachable).')
    return None
