"""Typed identity for placement-normalization run manifests.

This module intentionally has no SkyPilot dependencies.  Both the offline
normalizer and the runtime receipt reader must accept exactly the same bounded
set of historical protocols without importing one another.
"""

import dataclasses
import re

PROTOCOL_V1 = 1
PROTOCOL_V2 = 2
PROTOCOL_V3 = 3
PROTOCOL_V4 = 4
CURRENT_PROTOCOL = PROTOCOL_V4
_HISTORICAL_PROTOCOLS = frozenset({PROTOCOL_V1, PROTOCOL_V2, PROTOCOL_V3})
_SUPPORTED_PROTOCOLS = frozenset({*_HISTORICAL_PROTOCOLS, PROTOCOL_V4})
APPLY_SUPPORTED_MODE = 'apply_supported'
RETIRE_TERMINAL_HISTORICAL_MODE = 'retire_terminal_historical'
_SUPPORTED_MODES = frozenset({
    APPLY_SUPPORTED_MODE,
    RETIRE_TERMINAL_HISTORICAL_MODE,
})
_IDENTITY_PATTERN = re.compile(r'^(1|2|3|4):([0-9a-f]{40})$')
_COMMIT_PATTERN = re.compile(r'[0-9a-f]{40}')


class PlacementNormalizationIdentityError(ValueError):
    """A normalizer identity is outside the persisted manifest contract."""


@dataclasses.dataclass(frozen=True)
class PlacementNormalizationIdentity:
    """Parsed protocol and source-image commit for one normalizer run."""

    protocol: int
    image_commit: str


_FROZEN_APPLY_OUTCOMES = frozenset({
    ('placeholder', 'unchanged'),
    ('explicit_v1', 'changed'),
    ('explicit_v2', 'unchanged'),
    ('fieldless_supported', 'changed'),
    ('historical_physical_per_gpu', 'unchanged'),
    ('retired', 'unchanged'),
})
_FROZEN_RETIREMENT_OUTCOMES = frozenset({
    ('placeholder', 'unchanged'),
    ('explicit_v2', 'unchanged'),
    ('historical_physical_per_gpu', 'retired'),
    ('retired', 'unchanged'),
})
_PROTOCOL_V4_RETIREMENT_OUTCOMES = frozenset({
    *_FROZEN_RETIREMENT_OUTCOMES,
    ('stale_placeholder', 'unchanged'),
})
_OUTCOMES_BY_PROTOCOL_MODE = {
    (protocol, APPLY_SUPPORTED_MODE): _FROZEN_APPLY_OUTCOMES
    for protocol in _SUPPORTED_PROTOCOLS
}
_OUTCOMES_BY_PROTOCOL_MODE.update({
    (protocol, RETIRE_TERMINAL_HISTORICAL_MODE): _FROZEN_RETIREMENT_OUTCOMES
    for protocol in _HISTORICAL_PROTOCOLS
})
_OUTCOMES_BY_PROTOCOL_MODE[(
    PROTOCOL_V4,
    RETIRE_TERMINAL_HISTORICAL_MODE,
)] = _PROTOCOL_V4_RETIREMENT_OUTCOMES
_LOADABLE_OUTCOMES_BY_MODE = {
    APPLY_SUPPORTED_MODE: frozenset({
        ('fieldless_supported', 'changed'),
        ('explicit_v1', 'changed'),
        ('explicit_v2', 'unchanged'),
    }),
    RETIRE_TERMINAL_HISTORICAL_MODE: frozenset({('explicit_v2', 'unchanged')}),
}
_FILLABLE_OUTCOMES_BY_MODE = {
    APPLY_SUPPORTED_MODE: frozenset({('placeholder', 'unchanged')}),
    RETIRE_TERMINAL_HISTORICAL_MODE: frozenset({('placeholder', 'unchanged')}),
}


def parse_normalizer_identity(value: object) -> PlacementNormalizationIdentity:
    """Parse an exact persisted identity without aliases or coercion."""
    if type(value) is not str:
        raise PlacementNormalizationIdentityError(
            'Placement-normalizer identity must be a string.')
    match = _IDENTITY_PATTERN.fullmatch(value)
    if match is None:
        raise PlacementNormalizationIdentityError(
            'Placement-normalizer identity must match '
            '`^(1|2|3|4):[0-9a-f]{40}$`.')
    protocol = int(match.group(1))
    if protocol not in _SUPPORTED_PROTOCOLS:
        # The regular expression makes this unreachable today, but retaining
        # the explicit typed protocol fence keeps future grammar changes safe.
        raise PlacementNormalizationIdentityError(
            f'Unsupported placement-normalizer protocol: {protocol}.')
    return PlacementNormalizationIdentity(protocol=protocol,
                                          image_commit=match.group(2))


def format_normalizer_identity(image_commit: str) -> str:
    """Format the identity emitted by the current protocol-v4 writer."""
    if (type(image_commit) is not str or
            _COMMIT_PATTERN.fullmatch(image_commit) is None):
        raise PlacementNormalizationIdentityError(
            'Placement-normalizer image commit must be 40 lowercase hex '
            'characters.')
    return f'{CURRENT_PROTOCOL}:{image_commit}'


def parse_manifest_mode(value: object) -> str:
    """Parse an exact persisted operator mode without aliases."""
    if type(value) is not str or value not in _SUPPORTED_MODES:
        raise PlacementNormalizationIdentityError(
            'Placement-normalizer mode is invalid.')
    return value


def allowed_manifest_outcomes(identity: PlacementNormalizationIdentity,
                              mode: str) -> frozenset[tuple[str, str]]:
    """Return the frozen full-ledger outcome matrix for one protocol/mode."""
    parsed_mode = parse_manifest_mode(mode)
    try:
        return _OUTCOMES_BY_PROTOCOL_MODE[(identity.protocol, parsed_mode)]
    except KeyError:
        raise PlacementNormalizationIdentityError(
            'Placement-normalizer protocol/mode combination is invalid.') \
            from None


def is_loadable_manifest_outcome(identity: PlacementNormalizationIdentity,
                                 mode: str, classification: object,
                                 outcome: object) -> bool:
    """Whether selected bytes are a loadable result for this exact run."""
    allowed_manifest_outcomes(identity, mode)
    return (classification, outcome) in _LOADABLE_OUTCOMES_BY_MODE[mode]


def is_fillable_manifest_outcome(identity: PlacementNormalizationIdentity,
                                 mode: str, classification: object,
                                 outcome: object) -> bool:
    """Whether an ordinary manifested placeholder may be filled.

    Protocol-v4 ``stale_placeholder`` rows have a separate durable outcome and
    are never part of this fillable set.
    """
    allowed_manifest_outcomes(identity, mode)
    return (classification, outcome) in _FILLABLE_OUTCOMES_BY_MODE[mode]
