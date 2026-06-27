"""Regression test: a dead serve controller child must be respawned in place.

``sky/serve/service.py::_start`` runs the controller server (autoscaler +
replica-manager threads + FastAPI app) as a child process, but its supervision
loop historically only checked DB row ownership -- never the controller child's
liveness. HA recovery only re-creates a controller on parent ``controller_pid``
loss / pod move; it does not cover the controller child dying while the parent
stays alive, and in VM (non-consolidation) mode nothing does. Autoscaling,
probing and replica reconciliation then stop permanently.

``_respawn_controller_if_dead`` lets the (alive) parent restart the controller
child in place on the SAME port. These tests pin its contract, including the
critical safety property that a respawn which fails to bind must NOT raise (that
would reach ``_start``'s finally and trigger destructive ``_cleanup``).
"""
from sky.serve import service
from sky.utils import subprocess_utils


class _FakeProc:

    def __init__(self, alive: bool, pid: int = 100, exitcode=None):
        self._alive = alive
        self.pid = pid
        self.exitcode = exitcode

    def is_alive(self) -> bool:
        return self._alive


def test_alive_controller_not_respawned(monkeypatch):
    alive = _FakeProc(alive=True)
    spawned = []
    monkeypatch.setattr(service, '_spawn_controller',
                        lambda *a, **k: spawned.append(1))

    out = service._respawn_controller_if_dead(alive, 'svc', object(), 1,
                                              '127.0.0.1', 20001)

    assert out is alive
    assert spawned == [], 'a live controller must not be respawned'


def test_none_controller_passes_through():
    assert service._respawn_controller_if_dead(None, 'svc', object(), 1, 'h',
                                               1) is None


def test_dead_controller_respawned_when_it_binds(monkeypatch):
    dead = _FakeProc(alive=False, pid=111, exitcode=1)
    new = _FakeProc(alive=True, pid=222)
    monkeypatch.setattr(service, '_spawn_controller', lambda *a, **k: new)
    monkeypatch.setattr(service, '_wait_for_controller_ready',
                        lambda *a, **k: None)
    monkeypatch.setattr(subprocess_utils, 'kill_children_processes',
                        lambda *a, **k: None)

    out = service._respawn_controller_if_dead(dead, 'svc', object(), 1,
                                              '127.0.0.1', 20001)

    assert out is new, 'a dead controller that re-binds must be replaced'


def test_respawn_bind_failure_retries_without_raising(monkeypatch):
    """If the respawn fails to bind, return the (dead) handle so the next tick
    retries, kill the not-bound respawn, and crucially do NOT raise."""
    dead = _FakeProc(alive=False, pid=111, exitcode=1)
    new = _FakeProc(alive=True, pid=222)
    killed = []
    monkeypatch.setattr(service, '_spawn_controller', lambda *a, **k: new)

    def _fail_to_bind(*a, **k):
        raise RuntimeError('controller did not become ready')

    monkeypatch.setattr(service, '_wait_for_controller_ready', _fail_to_bind)
    monkeypatch.setattr(
        subprocess_utils, 'kill_children_processes',
        lambda parent_pids, force=False: killed.extend(parent_pids))

    out = service._respawn_controller_if_dead(dead, 'svc', object(), 1,
                                              '127.0.0.1', 20001)

    assert out is dead, 'a failed respawn must keep the dead handle for retry'
    assert new.pid in killed, 'the not-bound respawn must be reaped'
