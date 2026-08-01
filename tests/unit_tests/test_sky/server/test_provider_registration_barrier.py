"""Tests for the process-local provider registration barrier."""

import contextlib
import dataclasses
import os
import select
import signal
import threading
import types

import pytest

from sky.server import plugins
from sky.utils import provider_registration
from sky.utils import registry


def _capture_context(
    receipt: provider_registration.ProviderRegistrationBarrierV1,) -> str:
    with provider_registration.provider_registration_capture(receipt) as value:
        return value


def _assert_receipt_failure(
    receipt: object,
    expected: provider_registration.ProviderRegistrationReceiptFailureV1,
) -> None:
    with pytest.raises(
            provider_registration.ProviderRegistrationReceiptError) as exc:
        with provider_registration.provider_registration_capture(receipt):
            pass
    assert exc.value.reason is expected


def test_empty_plugin_load_returns_current_context_receipt(monkeypatch):
    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: None)
    monkeypatch.setattr(plugins, '_plugins_loaded', False)

    receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.MAIN))

    assert plugins.plugins_loaded()
    assert receipt.context == plugins.PluginContext.MAIN.value
    assert _capture_context(receipt) == plugins.PluginContext.MAIN.value


def test_plugin_import_and_install_registrations_are_in_same_receipt(
        monkeypatch):
    monkeypatch.setattr(plugins, '_PLUGINS', {})
    cloud_registry: registry._Registry[object] = (  # pylint: disable=protected-access
        registry._Registry(  # pylint: disable=protected-access
            registry_name='cloud',
            exclude=None))
    monkeypatch.setattr(registry, 'CLOUD_REGISTRY', cloud_registry)

    class ImportedCloud:
        pass

    class InstalledCloud:
        pass

    class RegisteringPlugin(plugins.BasePlugin):
        """Plugin that registers from a synchronous child thread."""

        def install(self, extension_context):
            del extension_context

            def _register_cloud():
                cloud_registry.register(aliases=['installed'])(InstalledCloud)

            worker = threading.Thread(target=_register_cloud)
            worker.start()
            worker.join(timeout=2)
            assert not worker.is_alive()
            assert 'installedcloud' in cloud_registry

    plugin_config = {
        'plugins': [{
            'class': 'test_plugin.RegisteringPlugin',
        }]
    }
    plugin_module = types.SimpleNamespace(RegisteringPlugin=RegisteringPlugin)

    def _import_plugin(unused_path):
        cloud_registry.register(aliases=['imported'])(ImportedCloud)
        return plugin_module

    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: plugin_config)
    monkeypatch.setattr(plugins.importlib, 'import_module', _import_plugin)

    receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.UVICORN))

    assert isinstance(cloud_registry['importedcloud'], ImportedCloud)
    assert isinstance(cloud_registry['installedcloud'], InstalledCloud)
    assert cloud_registry._aliases == {  # pylint: disable=protected-access
        'imported': 'importedcloud',
        'installed': 'installedcloud',
    }
    assert _capture_context(receipt) == plugins.PluginContext.UVICORN.value


def test_failed_plugin_load_invalidates_receipt_without_resetting_loaded(
        monkeypatch):
    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: None)
    old_receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.MAIN))
    assert plugins.plugins_loaded()

    class FailingPlugin(plugins.BasePlugin):

        def install(self, extension_context):
            del extension_context
            raise RuntimeError('install failed')

    plugin_config = {'plugins': [{'class': 'test_plugin.FailingPlugin'}]}
    plugin_module = types.SimpleNamespace(FailingPlugin=FailingPlugin)
    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: plugin_config)
    monkeypatch.setattr(plugins.importlib, 'import_module',
                        lambda unused_path: plugin_module)

    with pytest.raises(RuntimeError, match='install failed'):
        plugins.load_plugins(
            plugins.ExtensionContext(context=plugins.PluginContext.UVICORN))

    assert plugins.plugins_loaded()
    _assert_receipt_failure(
        old_receipt,
        provider_registration.ProviderRegistrationReceiptFailureV1.STALE_EPOCH)


def test_later_context_makes_older_receipt_stale(monkeypatch):
    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: None)
    main_receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.MAIN))
    executor_receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.EXECUTOR))

    _assert_receipt_failure(
        main_receipt,
        provider_registration.ProviderRegistrationReceiptFailureV1.STALE_EPOCH)
    assert _capture_context(
        executor_receipt) == plugins.PluginContext.EXECUTOR.value


def test_active_session_rejects_capture(monkeypatch):
    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: None)
    receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.MAIN))

    with provider_registration.provider_registration_session(
            'later') as session:
        _assert_receipt_failure(
            receipt, provider_registration.ProviderRegistrationReceiptFailureV1.
            ACTIVE_SESSION)
        session.complete()


def test_wrong_process_and_invalid_receipts(monkeypatch):
    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: None)
    receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.MAIN))
    wrong_process = dataclasses.replace(receipt,
                                        process_id=receipt.process_id + 1)

    _assert_receipt_failure(
        wrong_process, provider_registration.
        ProviderRegistrationReceiptFailureV1.WRONG_PROCESS)
    _assert_receipt_failure(
        object(), provider_registration.ProviderRegistrationReceiptFailureV1.
        INVALID_RECEIPT)
    _assert_receipt_failure(
        None, provider_registration.ProviderRegistrationReceiptFailureV1.
        MISSING_RECEIPT)


def test_supported_cloud_registration_invalidates_current_receipt(monkeypatch):
    monkeypatch.setattr(plugins, '_load_plugin_config', lambda: None)
    receipt = plugins.load_plugins(
        plugins.ExtensionContext(context=plugins.PluginContext.MAIN))
    cloud_registry: registry._Registry[object] = (  # pylint: disable=protected-access
        registry._Registry(  # pylint: disable=protected-access
            registry_name='cloud',
            exclude=None))
    monkeypatch.setattr(registry, 'CLOUD_REGISTRY', cloud_registry)

    @cloud_registry.register
    class LateCloud:
        pass

    assert isinstance(cloud_registry['latecloud'], LateCloud)
    _assert_receipt_failure(
        receipt,
        provider_registration.ProviderRegistrationReceiptFailureV1.STALE_EPOCH)


def test_cloud_construction_precedes_atomic_registration_mutation(monkeypatch):
    cloud_registry: registry._Registry[object] = (  # pylint: disable=protected-access
        registry._Registry(  # pylint: disable=protected-access
            registry_name='cloud',
            exclude=None))
    monkeypatch.setattr(registry, 'CLOUD_REGISTRY', cloud_registry)
    original_mutation = provider_registration.provider_registration_mutation
    main_thread_id = threading.get_ident()
    mutation_windows = []
    aliases = cloud_registry._aliases  # pylint: disable=protected-access

    @contextlib.contextmanager
    def _recording_mutation():
        with original_mutation():
            before = (tuple(cloud_registry), dict(aliases))
            yield
            after = (tuple(cloud_registry), dict(aliases))
        mutation_windows.append((threading.get_ident(), before, after))

    monkeypatch.setattr(provider_registration, 'provider_registration_mutation',
                        _recording_mutation)

    class ChildCloud:
        pass

    class ParentCloud:
        """Cloud whose constructor waits for a child registration."""

        def __init__(self):
            assert 'parentcloud' not in cloud_registry
            assert 'parent' not in aliases
            worker = threading.Thread(target=lambda: cloud_registry.register(
                aliases=['child'])(ChildCloud))
            worker.start()
            worker.join(timeout=2)
            assert not worker.is_alive()
            assert isinstance(cloud_registry['childcloud'], ChildCloud)

    cloud_registry.register(aliases=['parent'])(ParentCloud)

    main_windows = [(before, after)
                    for thread_id, before, after in mutation_windows
                    if thread_id == main_thread_id]
    assert len(main_windows) == 1
    before, after = main_windows[0]
    assert 'parentcloud' not in before[0]
    assert 'parent' not in before[1]
    assert 'parentcloud' in after[0]
    assert after[1]['parent'] == 'parentcloud'
    assert isinstance(cloud_registry['parentcloud'], ParentCloud)
    assert cloud_registry.from_str('parent') is cloud_registry['parentcloud']


def test_duplicate_cloud_name_is_rejected_before_construction(monkeypatch):
    cloud_registry: registry._Registry[object] = (  # pylint: disable=protected-access
        registry._Registry(  # pylint: disable=protected-access
            registry_name='cloud',
            exclude=None))
    monkeypatch.setattr(registry, 'CLOUD_REGISTRY', cloud_registry)

    class DuplicateCloud:
        pass

    cloud_registry.register(DuplicateCloud)
    construction_calls = []

    def _duplicate_init(unused_self):
        construction_calls.append('constructed')

    duplicate_type = type('DuplicateCloud', (), {'__init__': _duplicate_init})

    with pytest.raises(AssertionError, match='already registered'):
        cloud_registry.register(duplicate_type)

    assert not construction_calls


@pytest.mark.skipif(
    not hasattr(os, 'fork') or not hasattr(signal, 'SIGALRM'),
    reason='requires fork and SIGALRM',
)
@pytest.mark.parametrize('lock_kind', ('load', 'mutation'))
def test_forked_child_does_not_inherit_locked_registration_mutation(lock_kind):
    coordinator_locked = threading.Event()
    release_coordinator = threading.Event()

    def _hold_coordinator_lock() -> None:
        if lock_kind == 'mutation':
            with provider_registration.provider_registration_mutation():
                coordinator_locked.set()
                release_coordinator.wait(timeout=10)
            return
        with provider_registration.provider_registration_session(
                'parent-lock-holder') as session:
            coordinator_locked.set()
            release_coordinator.wait(timeout=10)
            session.complete()

    holder = threading.Thread(target=_hold_coordinator_lock)
    holder.start()
    assert coordinator_locked.wait(timeout=2)

    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(read_fd)

        def _alarm_handler(unused_signum, unused_frame) -> None:
            del unused_signum, unused_frame
            os.write(write_fd, b'timeout')
            os._exit(2)  # pylint: disable=protected-access

        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(2)
        try:
            if lock_kind == 'mutation':
                with provider_registration.provider_registration_mutation():
                    pass
            else:
                with provider_registration.provider_registration_session(
                        'child-lock-check') as session:
                    session.complete()
        except Exception:  # pylint: disable=broad-exception-caught
            signal.alarm(0)
            os.write(write_fd, b'error')
            os._exit(3)  # pylint: disable=protected-access
        signal.alarm(0)
        os.write(write_fd, b'ok')
        os._exit(0)  # pylint: disable=protected-access

    os.close(write_fd)
    payload = b'parent-timeout'
    try:
        readable, _, _ = select.select([read_fd], [], [], 4)
        if readable:
            payload = os.read(read_fd, 64)
        else:
            os.kill(child_pid, signal.SIGKILL)
        _, child_status = os.waitpid(child_pid, 0)
    finally:
        os.close(read_fd)
        release_coordinator.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    assert payload == b'ok'
    assert os.WIFEXITED(child_status)
    assert os.WEXITSTATUS(child_status) == 0
