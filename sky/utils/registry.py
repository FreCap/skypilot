"""Registry for classes to be discovered"""

from collections.abc import Callable
import contextlib
import difflib
import typing

from sky.utils import provider_registration
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    # Used only inside string generics like _Registry['cloud.Cloud'] below;
    # static linters do not see those as usages, so keep F401 suppressed.
    from sky.backends import backend  # noqa: F401
    from sky.batch import io_formats  # noqa: F401
    from sky.clouds import cloud  # noqa: F401
    from sky.jobs import recovery_strategy  # noqa: F401

T = typing.TypeVar('T')


class _Registry(dict, typing.Generic[T]):
    """Registry."""

    def __init__(self,
                 registry_name: str,
                 exclude: set[str] | None,
                 type_register: bool = False):
        super().__init__()
        self._registry_name = registry_name
        self._exclude = exclude or set()
        self._default: str | None = None
        self._type_register: bool = type_register
        self._aliases: dict[str, str] = {}

    def from_str(self, name: str | None) -> T | None:
        """Returns the cloud instance from the canonical name or alias."""
        if name is None:
            return None

        search_name = name.lower()
        if search_name in self._exclude:
            return None

        if search_name in self:
            return self[search_name]

        if search_name in self._aliases:
            return self[self._aliases[search_name]]

        known_names = [*self.keys(), *self._aliases.keys()]
        suggestion = difflib.get_close_matches(search_name,
                                               known_names,
                                               n=1,
                                               cutoff=0.6)
        suggestion_msg = (f' Did you mean {suggestion[0]!r}?'
                          if suggestion else '')
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'{self._registry_name.capitalize()} {name!r} is not a '
                f'valid {self._registry_name} among '
                f'{known_names}.{suggestion_msg}')

    def type_register(self,
                      name: str,
                      default: bool = False) -> Callable[[type[T]], type[T]]:

        name = name.lower()

        def decorator(cls: type[T]) -> type[T]:
            assert self._type_register, ('type_register can only be used '
                                         'when type_register is True')
            assert name not in self, f'{name} already registered'
            self[name] = cls
            if default:
                self._default = name
            return cls

        return decorator

    @typing.overload
    def register(self, cls: type[T]) -> type[T]:
        ...

    @typing.overload
    def register(
            self,
            cls: None = None,
            aliases: list[str] | None = None) -> Callable[[type[T]], type[T]]:
        ...

    def register(self,
                 cls: type[T] | None = None,
                 aliases: list[str] | None = None
                ) -> type[T] | Callable[[type[T]], type[T]]:
        assert not self._type_register, ('register can only be used when '
                                         'type_register is False')

        def _register(cls: type[T]) -> type[T]:
            name = cls.__name__.lower()
            # Preserve rejection-before-construction for an already registered
            # name. The locked check below remains authoritative for races.
            assert name not in self, f'{name} already registered'
            instance = cls()
            normalized_aliases = tuple(alias.lower() for alias in aliases or ())
            mutation_context = (
                provider_registration.provider_registration_mutation()
                if self is CLOUD_REGISTRY else contextlib.nullcontext())
            with mutation_context:
                assert name not in self, f'{name} already registered'
                seen_aliases: set[str] = set()
                for alias in normalized_aliases:
                    assert alias not in self._aliases, (
                        f'{alias} already registered')
                    assert alias not in seen_aliases, (
                        f'{alias} already registered')
                    seen_aliases.add(alias)
                self[name] = instance
                for alias in normalized_aliases:
                    self._aliases[alias] = name
            return cls

        if cls is not None:
            # Invocation without parentheses (e.g. @register)
            return _register(cls)

        # Invocation with parentheses (e.g. @register(aliases=['alias']))
        return _register

    @property
    def default(self) -> str:
        assert self._default is not None, ('default is not set', self)
        return self._default


# Backward compatibility. global_user_state's DB may have recorded
# Local cloud, and we've just removed it from the registry, and
# global_user_state.get_enabled_clouds() would call into this func
# and fail.

CLOUD_REGISTRY: _Registry = _Registry['cloud.Cloud'](registry_name='cloud',
                                                     exclude={'local'})

BACKEND_REGISTRY: _Registry = _Registry['backend.Backend'](
    registry_name='backend', type_register=True, exclude=None)

JOBS_RECOVERY_STRATEGY_REGISTRY: _Registry = (
    _Registry['recovery_strategy.StrategyExecutor'](
        registry_name='jobs recovery strategy',
        exclude=None,
        type_register=True))

INPUT_READER_REGISTRY: _Registry = _Registry['io_formats.InputReader'](
    registry_name='input reader', exclude=None, type_register=True)

OUTPUT_WRITER_REGISTRY: _Registry = _Registry['io_formats.OutputWriter'](
    registry_name='output writer', exclude=None, type_register=True)

# TODO(tian): Add a registry for spot placer.
