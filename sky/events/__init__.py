"""Public operational event models and lazy client surface."""
# pylint: disable=undefined-all-variable

from typing import Any

from sky.events.api_models import EventActor
from sky.events.api_models import EventActorType
from sky.events.api_models import EventCause
from sky.events.api_models import EventKind
from sky.events.api_models import EventOutcome
from sky.events.api_models import EventPhase
from sky.events.api_models import EventsPage
from sky.events.api_models import EventTarget
from sky.events.api_models import EventTargetType
from sky.events.api_models import OperationalEvent
from sky.events.api_models import TraversalDirection

__all__ = [
    'EventActor',
    'EventActorType',
    'EventCause',
    'EventKind',
    'EventOutcome',
    'EventPhase',
    'EventsPage',
    'EventTarget',
    'EventTargetType',
    'OperationalEvent',
    'TraversalDirection',
    'list',
]


def __getattr__(name: str) -> Any:
    if name != 'list':
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    # Server-side model imports must not eagerly pull in client dependencies.
    # pylint: disable=import-outside-toplevel
    from sky.events.client import list_events
    return list_events
