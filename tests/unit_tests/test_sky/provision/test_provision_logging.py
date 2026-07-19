"""Unit tests for sky.provision.logging."""

import pytest

from sky.provision import logging as provision_logging


def test_setup_provision_logging_preserves_file_handler_error(
        monkeypatch, tmp_path):

    def raise_file_handler_error(*args, **kwargs):
        del args, kwargs
        raise OSError('disk full')

    monkeypatch.setattr(provision_logging.logging, 'FileHandler',
                        raise_file_handler_error)

    with pytest.raises(OSError, match='disk full'):
        with provision_logging.setup_provision_logging(str(tmp_path)):
            pass
