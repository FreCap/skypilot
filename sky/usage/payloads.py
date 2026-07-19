"""Prepare privacy-safe configuration payloads for usage reporting."""

import typing
from typing import Any

from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.usage import constants
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:
    import inspect
else:
    # inspect costs ~100ms to load, which can be postponed to collection phase
    # or skipped if the user disables collection.
    inspect = adaptors_common.LazyImport('inspect')

logger = sky_logging.init_logger(__name__)


def _clean_yaml(yaml_info: dict[str, str | None]):
    """Remove sensitive information from user YAML."""
    cleaned_yaml_info = yaml_info.copy()
    for redact_type in constants.USAGE_MESSAGE_REDACT_KEYS:
        if redact_type in cleaned_yaml_info:
            contents = cleaned_yaml_info[redact_type]
            if not contents:
                cleaned_yaml_info[redact_type] = None
                continue

            message = None
            try:
                if callable(contents):
                    contents = inspect.getsource(contents)

                if type(contents) in constants.USAGE_MESSAGE_REDACT_TYPES:
                    lines = yaml_utils.dump_yaml_str({
                        redact_type: contents
                    }).strip().split('\n')
                    message = (f'{len(lines)} lines {redact_type.upper()}'
                               ' redacted')
                else:
                    message = (f'Error: Unexpected type for {redact_type}: '
                               f'{type(contents)}')
                    logger.debug(message)
            except Exception:  # pylint: disable=broad-except
                message = (
                    f'Error: Failed to dump lines for {redact_type.upper()}')
                logger.debug(message)

            cleaned_yaml_info[redact_type] = message

    return cleaned_yaml_info


def prepare_json_from_yaml_config(
        yaml_config_or_path: dict | str) -> list[dict[str, Any]]:
    """Upload safe contents of YAML file to Loki."""
    if isinstance(yaml_config_or_path, dict):
        yaml_info = [yaml_config_or_path]
        comment_lines = []
    else:
        with open(yaml_config_or_path, encoding='utf-8') as f:
            lines = f.readlines()
            comment_lines = [line for line in lines if line.startswith('#')]
        yaml_info = yaml_utils.read_yaml_all(yaml_config_or_path)

    for i in range(len(yaml_info)):
        if yaml_info[i] is None:
            yaml_info[i] = {}
        yaml_info[i] = _clean_yaml(yaml_info[i])
        yaml_info[i]['__redacted_comment_lines'] = len(comment_lines)
    return yaml_info
