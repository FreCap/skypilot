"""Leaf JSON type definitions and immutable JSON containers."""

from __future__ import annotations

import collections.abc
import typing

JSONScalar: typing.TypeAlias = str | int | float | bool | None
JSONValue: typing.TypeAlias = (JSONScalar | list['JSONValue'] |
                               dict[str, 'JSONValue'])
JSONObject: typing.TypeAlias = dict[str, JSONValue]

FrozenJSONScalar: typing.TypeAlias = JSONScalar


class FrozenJSONDict(collections.abc.Mapping[str, 'FrozenJSONValue']):
    """An immutable, hashable, recursively detached JSON object."""

    __slots__ = ('_items', '_hash')
    _items: tuple[tuple[str, FrozenJSONValue], ...]
    _hash: int

    def __init__(
        self,
        items: collections.abc.Iterable[tuple[str, FrozenJSONValue]],
    ) -> None:
        supplied_items = tuple(items)
        keys = tuple(key for key, _ in supplied_items)
        if not all(type(key) is str for key in keys):
            raise ValueError('FrozenJSONDict keys must be strings.')
        if len(keys) != len(set(keys)):
            raise ValueError('FrozenJSONDict keys must be unique.')
        frozen_items = tuple(sorted(supplied_items, key=lambda item: item[0]))
        object.__setattr__(self, '_items', frozen_items)
        object.__setattr__(self, '_hash', hash(frozen_items))

    def __setattr__(self, name: str, value: object) -> typing.NoReturn:
        del name, value
        raise AttributeError(f'{type(self).__name__} is immutable.')

    def __getitem__(self, key: str) -> FrozenJSONValue:
        for candidate, value in self._items:
            if candidate == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> collections.abc.Iterator[str]:
        return (key for key, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return self._hash

    def __repr__(self) -> str:
        contents = ', '.join(
            f'{key!r}: {value!r}' for key, value in self._items)
        return f'{type(self).__name__}({{{contents}}})'


FrozenJSONValue: typing.TypeAlias = (FrozenJSONScalar |
                                     tuple['FrozenJSONValue', ...] |
                                     FrozenJSONDict)


def freeze_json(value: JSONValue) -> FrozenJSONValue:
    """Recursively detach a JSON value into immutable containers."""
    if isinstance(value, dict):
        return FrozenJSONDict(
            (key, freeze_json(child)) for key, child in value.items())
    if isinstance(value, list):
        return tuple(freeze_json(child) for child in value)
    return value


def thaw_json(value: FrozenJSONValue) -> JSONValue:
    """Return a fresh tree containing only mutable JSON built-ins."""
    if isinstance(value, FrozenJSONDict):
        return {key: thaw_json(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(child) for child in value]
    return value
