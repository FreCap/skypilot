"""YAML utilities."""
import io
from typing import Any, TYPE_CHECKING

from sky.adaptors import common

if TYPE_CHECKING:
    import yaml
else:
    yaml = common.LazyImport('yaml')

_c_extension_unavailable = False
_MAX_YAML_GRAPH_EDGES = 100_000


def check_no_duplicate_keys(yaml_str: str) -> None:
    """Raise ValueError if any mapping in the YAML has duplicate keys.

    PyYAML's default behavior is to silently drop the earlier value on
    duplicate keys, which masks real user typos (e.g. two `name:` lines
    in a task YAML or two mounts with the same remote destination).
    This function walks the YAML node graph and raises a value-free error for
    the first duplicate.  The traversal is cycle-safe and bounded because YAML
    aliases can otherwise turn a small document into an unbounded recursive
    graph walk.
    """
    stream = io.StringIO(yaml_str)
    try:
        nodes = list(yaml.compose_all(stream))
    except RecursionError:
        raise ValueError('YAML document is too complex.') from None
    except yaml.YAMLError:
        # Let the regular `safe_load` path produce the user-facing parse
        # error; this function's job is only to catch silent duplicates.
        return

    visited: set[int] = set()
    visiting: set[int] = set()
    edge_count = 0
    stack: list[tuple['yaml.Node', bool]] = [
        (node, False) for node in reversed(nodes) if node is not None
    ]
    while stack:
        node, exiting = stack.pop()
        node_id = id(node)
        if exiting:
            visiting.remove(node_id)
            visited.add(node_id)
            continue
        if node_id in visiting:
            raise ValueError('YAML alias graph contains a cycle.') from None
        if node_id in visited:
            continue
        visiting.add(node_id)
        stack.append((node, True))

        children: list['yaml.Node'] = []
        if isinstance(node, yaml.MappingNode):
            seen: dict[Any, int] = {}
            for key_node, value_node in node.value:
                if isinstance(key_node, yaml.ScalarNode):
                    key = key_node.value
                    if key in seen:
                        previous_line = seen[key] + 1
                        line = key_node.start_mark.line + 1
                        raise ValueError('Duplicate key name in YAML at '
                                         f'line {line} (also defined at line '
                                         f'{previous_line}).') from None
                    seen[key] = key_node.start_mark.line
                children.extend((key_node, value_node))
        elif isinstance(node, yaml.SequenceNode):
            children.extend(node.value)

        edge_count += len(children)
        if edge_count > _MAX_YAML_GRAPH_EDGES:
            raise ValueError('YAML document is too complex.') from None
        stack.extend((child, False) for child in reversed(children))


def safe_load(stream) -> Any:
    global _c_extension_unavailable
    if _c_extension_unavailable:
        return yaml.load(stream, Loader=yaml.SafeLoader)

    try:
        return yaml.load(stream, Loader=yaml.CSafeLoader)
    except AttributeError:
        _c_extension_unavailable = True
        return yaml.load(stream, Loader=yaml.SafeLoader)


def safe_load_value_free(stream) -> Any:
    """Loads one YAML document without retaining hostile parser values.

    PyYAML exceptions include source excerpts and tag names.  Public task and
    config boundaries may contain credentials, so callers that surface parse
    failures must receive a fixed message with no exception cause chain.
    """
    try:
        return safe_load(stream)
    except yaml.YAMLError:
        raise ValueError('Invalid YAML syntax.') from None


def safe_load_all(stream) -> Any:
    global _c_extension_unavailable
    if _c_extension_unavailable:
        return yaml.load_all(stream, Loader=yaml.SafeLoader)

    try:
        return yaml.load_all(stream, Loader=yaml.CSafeLoader)
    except AttributeError:
        _c_extension_unavailable = True
        return yaml.load_all(stream, Loader=yaml.SafeLoader)


def read_yaml(path: str | None,
              *,
              reject_duplicate_keys: bool = False) -> dict[str, Any]:
    if path is None:
        raise ValueError('Attempted to read a None YAML.')
    with open(path, encoding='utf-8') as f:
        return read_yaml_str(f.read(),
                             reject_duplicate_keys=reject_duplicate_keys)


def read_yaml_str(yaml_str: str,
                  *,
                  reject_duplicate_keys: bool = False) -> dict[str, Any]:
    if reject_duplicate_keys:
        check_no_duplicate_keys(yaml_str)
    stream = io.StringIO(yaml_str)
    parsed_yaml = safe_load_value_free(stream)
    if not parsed_yaml:
        # Empty dict
        return {}
    return parsed_yaml


def read_yaml_all_str(
        yaml_str: str,
        *,
        reject_duplicate_keys: bool = False) -> list[dict[str, Any]]:
    if reject_duplicate_keys:
        check_no_duplicate_keys(yaml_str)
    stream = io.StringIO(yaml_str)
    try:
        configs = list(safe_load_all(stream))
    except yaml.YAMLError:
        raise ValueError('Invalid YAML syntax.') from None
    if not configs:
        # Empty YAML file.
        return [{}]
    return configs


def read_yaml_all(path: str,
                  *,
                  reject_duplicate_keys: bool = False) -> list[dict[str, Any]]:
    with open(path, encoding='utf-8') as f:
        return read_yaml_all_str(f.read(),
                                 reject_duplicate_keys=reject_duplicate_keys)


def dump_yaml(path: str,
              config: list[dict[str, Any]] | dict[str, Any],
              blank: bool = False) -> None:
    """Dumps a YAML file.

    Args:
        path: the path to the YAML file.
        config: the configuration to dump.
    """
    with open(path, 'w', encoding='utf-8') as f:
        contents = dump_yaml_str(config)
        if blank and isinstance(config, dict) and len(config) == 0:
            # when dumping to yaml, an empty dict will go in as {}.
            contents = ''
        f.write(contents)


def dump_yaml_str(config: list[dict[str, Any]] | dict[str, Any]) -> str:
    """Dumps a YAML string.
    Args:
        config: the configuration to dump.
    Returns:
        The YAML string.
    """

    # https://github.com/yaml/pyyaml/issues/127
    class LineBreakDumper(yaml.SafeDumper):

        def write_line_break(self, data=None):
            super().write_line_break(data)
            if len(self.indents) == 1:
                super().write_line_break()

    if isinstance(config, list):
        dump_func = yaml.dump_all
    else:
        dump_func = yaml.dump  # type: ignore
    return dump_func(config,
                     Dumper=LineBreakDumper,
                     sort_keys=False,
                     default_flow_style=False)
