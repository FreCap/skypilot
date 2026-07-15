"""Best-effort recording for low-cardinality operator notifications."""
import enum
import threading

from sky import global_user_state
from sky import sky_logging
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

MAX_MESSAGE_LENGTH = 4096
INSUFFICIENT_QUOTA_DEDUPE_WINDOW_SECONDS = 60 * 60


class OperatorNotificationCategory(str, enum.Enum):
    """Explicit category registry that keeps the notification DB bounded."""

    INSUFFICIENT_QUOTA = 'insufficient_quota'


_failure_lock = threading.Lock()
_failure_reported = False


def record_notification(category: OperatorNotificationCategory, message: str,
                        dedupe_window_seconds: int) -> bool:
    """Record a notification without affecting the caller on failure.

    Returns whether persistence succeeded. Categories are an enum on purpose:
    callers must add a reviewed, low-cardinality category instead of deriving
    one from resource or workload identifiers.
    """
    global _failure_reported
    try:
        if not isinstance(category, OperatorNotificationCategory):
            raise TypeError('category must be an OperatorNotificationCategory')
        if dedupe_window_seconds < 0:
            raise ValueError('dedupe_window_seconds must be non-negative')
        normalized_message = message.strip()
        if not normalized_message:
            raise ValueError('message must be non-empty')
        normalized_message = normalized_message[:MAX_MESSAGE_LENGTH]
        global_user_state.record_operator_notification(
            category.value,
            normalized_message,
            dedupe_window_seconds=dedupe_window_seconds)
    except Exception as e:  # pylint: disable=broad-except
        with _failure_lock:
            log = logger.warning if not _failure_reported else logger.debug
            _failure_reported = True
        log('Failed to record operator notification: '
            f'{common_utils.format_exception(e)}')
        return False

    with _failure_lock:
        _failure_reported = False
    return True
