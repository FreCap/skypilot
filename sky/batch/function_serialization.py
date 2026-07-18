"""Source-based serialization for Sky Batch remote functions."""

import base64
from collections.abc import Callable
import inspect
import json
import textwrap


def serialize_function(
        fn: Callable,
        source_getter: Callable[[Callable], str] = inspect.getsource) -> str:
    """Serialize a function to a base64-encoded source payload."""
    try:
        source = source_getter(fn)
    except (TypeError, OSError) as e:
        raise TypeError(
            f'Cannot serialize function {fn.__name__}: unable to retrieve '
            f'source code. Make sure the function is defined in a file '
            f'(not interactively) and is accessible to inspect.getsource(). '
            f'Error: {e}') from e

    source = textwrap.dedent(source)
    payload = {
        'type': 'source',
        'source': source,
        'name': fn.__name__,
        'version': '1.0',
    }
    serialized = json.dumps(payload)
    return base64.b64encode(serialized.encode('utf-8')).decode('utf-8')


def deserialize_function(serialized: str) -> Callable:
    """Deserialize a function from a base64-encoded source payload."""
    decoded = base64.b64decode(serialized.encode('utf-8'))
    payload = json.loads(decoded)

    if payload.get('type') != 'source':
        raise ValueError('Unknown or missing serialization type: '
                         f'{payload.get("type")}. Expected "source".')

    source = payload['source']
    fn_name = payload['name']
    namespace = {
        '__builtins__': __builtins__,
    }

    # Importing here avoids the sky.batch package initialization cycle while
    # still exposing sky.batch.load() and save_results() to remote functions.
    try:
        import sky.batch  # pylint: disable=unused-import,import-outside-toplevel
        namespace['sky'] = __import__('sky')
    except ImportError:
        # Functions with explicit imports can still be reconstructed if the
        # SkyPilot package is unavailable in the deserialization environment.
        pass

    try:
        exec(source, namespace)  # pylint: disable=exec-used
    except Exception as e:
        raise ValueError(
            f'Failed to execute function source code for {fn_name}. '
            f'Error: {e}\n\nSource:\n{source}') from e

    if fn_name not in namespace:
        raise ValueError(
            f'Function {fn_name} not found in namespace after executing '
            f'source code. Available names: {list(namespace.keys())}')

    fn = namespace[fn_name]
    if not callable(fn):
        raise ValueError(
            f'Expected {fn_name} to be a callable function, but got '
            f'{type(fn).__name__}')

    return fn
