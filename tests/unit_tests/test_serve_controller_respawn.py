"""Regression test: a dead serve controller child must be respawned in place.

``sky/serve/service.py::_start`` runs the controller server (autoscaler +
replica-manager threads + FastAPI app) as a child process, but its supervision
loop historically only checked DB row ownership -- never the controller child's
liveness. HA recovery only re-creates a controller on parent ``controller_pid``
loss / pod move; it does not cover the controller child dying while the parent
stays alive, and in VM (non-consolidation) mode nothing does. Autoscaling,
probing and replica reconciliation then stop permanently.

``_respawn_controller_if_dead`` lets the (alive) parent restart the controller
child in place on the SAME port. These tests pin its contract:
  - respawn a dead controller; leave a live one / None alone;
  - reload the latest version + spec from the DB (so a respawn after an update
    does not regress the controller to the stale boot-time spec);
  - fully contain spawn AND readiness failures -- they must NOT raise (that
    would reach ``_start``'s finally and trigger destructive ``_cleanup``).
"""
from sky.serve import serve_state
from sky.serve import service
from sky.utils import subprocess_utils


class _FakeProc:

    def __init__(self, alive: bool, pid: int = 100, exitcode=None):
        self._alive = alive
        self.pid = pid
        self.exitcode = exitcode

    def is_alive(self) -> bool:
        return self._alive


def _mute_db_reload(monkeypatch, version=None, spec=None):
    """Make the latest-version/spec reload a no-op (keep captured values)
    unless explicit version/spec are provided."""
    monkeypatch.setattr(serve_state, 'get_latest_version', lambda name: version)
    monkeypatch.setattr(serve_state, 'get_spec', lambda name, ver: spec)


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
    _mute_db_reload(monkeypatch)

    out = service._respawn_controller_if_dead(dead, 'svc', object(), 1,
                                              '127.0.0.1', 20001)

    assert out is new, 'a dead controller that re-binds must be replaced'


def test_respawn_reloads_latest_version_and_spec(monkeypatch):
    """A respawn must use the LATEST version/spec from the DB, not the stale
    values captured at boot (which go stale after a /update_service)."""
    dead = _FakeProc(alive=False, pid=111, exitcode=1)
    new = _FakeProc(alive=True, pid=222)
    latest_spec = object()
    spawn_args = {}

    def _spawn(service_name, spec, version, host, port):
        spawn_args.update(spec=spec, version=version)
        return new

    monkeypatch.setattr(service, '_spawn_controller', _spawn)
    monkeypatch.setattr(service, '_wait_for_controller_ready',
                        lambda *a, **k: None)
    monkeypatch.setattr(subprocess_utils, 'kill_children_processes',
                        lambda *a, **k: None)
    _mute_db_reload(monkeypatch, version=7, spec=latest_spec)

    service._respawn_controller_if_dead(dead, 'svc', object(), 1, '127.0.0.1',
                                        20001)

    assert spawn_args['version'] == 7, 'must respawn at the latest DB version'
    assert spawn_args['spec'] is latest_spec, 'must use the latest spec'


def test_spawn_failure_is_contained_without_raising(monkeypatch):
    """If spawning the replacement controller fails, return the (dead) handle
    so the next tick retries, and crucially do NOT raise."""
    dead = _FakeProc(alive=False, pid=111, exitcode=1)
    _mute_db_reload(monkeypatch)

    def _spawn_boom(*a, **k):
        raise OSError('cannot allocate memory for new process')

    monkeypatch.setattr(service, '_spawn_controller', _spawn_boom)
    monkeypatch.setattr(subprocess_utils, 'kill_children_processes',
                        lambda *a, **k: None)

    out = service._respawn_controller_if_dead(dead, 'svc', object(), 1,
                                              '127.0.0.1', 20001)

    assert out is dead, 'a failed spawn must keep the dead handle for retry'


def test_readiness_failure_retries_without_raising(monkeypatch):
    """If the respawn fails to become ready, return the (dead) handle, kill the
    not-ready respawn, and do NOT raise."""
    dead = _FakeProc(alive=False, pid=111, exitcode=1)
    new = _FakeProc(alive=True, pid=222)
    killed = []
    monkeypatch.setattr(service, '_spawn_controller', lambda *a, **k: new)
    _mute_db_reload(monkeypatch)

    def _fail_ready(*a, **k):
        raise RuntimeError('controller did not become ready')

    monkeypatch.setattr(service, '_wait_for_controller_ready', _fail_ready)
    monkeypatch.setattr(
        subprocess_utils, 'kill_children_processes',
        lambda parent_pids, force=False: killed.extend(parent_pids))

    out = service._respawn_controller_if_dead(dead, 'svc', object(), 1,
                                              '127.0.0.1', 20001)

    assert out is dead, 'a not-ready respawn keeps the dead handle to retry'
    assert new.pid in killed, 'the not-ready respawn must be reaped'
