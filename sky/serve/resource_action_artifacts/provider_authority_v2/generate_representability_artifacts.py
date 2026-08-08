"""Regenerate the closed provider-authority representability artifacts."""
# pylint: disable=protected-access

from __future__ import annotations

import hashlib
import pathlib

from sky.serve import resource_action_provider_inventory_v2 as inventory_v2
from sky.serve import resource_action_representability as representability
from sky.serve import resource_actions as actions

_DIRECTORY = pathlib.Path(__file__).resolve().parent


def _write(path: pathlib.Path, canonical_bytes: bytes) -> bytes:
    contents = canonical_bytes + b'\n'
    if len(contents) > 65_536:
        raise ValueError(f'{path} exceeds the 65536-byte artifact bound.')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)
    return contents


def main() -> None:
    cases = (
        representability.PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASES_V2)
    split = (len(cases) + 1) // 2
    case_groups = (cases[:split], cases[split:])
    descriptors = []
    for ordinal, shard_cases in enumerate(case_groups):
        shard = (representability.
                 ProviderResourceActionRepresentabilityCaseInventoryShardV2(
                     version=2,
                     contract=(representability._CASE_INVENTORY_SHARD_CONTRACT),
                     profile=representability._CASE_INVENTORY_PROFILE,
                     ordinal=ordinal,
                     cases=shard_cases))
        relative = representability._CASE_INVENTORY_SHARD_PATHS[ordinal]
        path = (_DIRECTORY / 'representability_case_inventory' /
                f'{ordinal:03d}.json')
        contents = _write(path, shard.canonical_bytes)
        descriptors.append(
            representability.
            ProviderResourceActionRepresentabilityShardDescriptorV2(
                ordinal=ordinal,
                first_case_sequence=shard_cases[0].sequence,
                last_case_sequence=shard_cases[-1].sequence,
                case_count=len(shard_cases),
                artifact=actions.ProviderRepoArtifactRefV1(
                    repo_path=relative,
                    byte_size=len(contents),
                    sha256=hashlib.sha256(contents).hexdigest())))

    index = (representability.
             ProviderResourceActionRepresentabilityCaseInventoryIndexV2(
                 version=2,
                 contract=representability._CASE_INVENTORY_CONTRACT,
                 profile=representability._CASE_INVENTORY_PROFILE,
                 shards=tuple(descriptors)))
    _write(_DIRECTORY / 'representability_case_inventory.json',
           index.canonical_bytes)

    artifact_inventory = (
        inventory_v2.project_provider_authority_artifact_inventory_v2())
    _write(_DIRECTORY / 'artifact_inventory.json',
           artifact_inventory.canonical_bytes)
    callable_inventory = (
        inventory_v2.project_provider_authority_callable_inventory_v2())
    _write(_DIRECTORY / 'callable_inventory.json',
           callable_inventory.canonical_bytes)


if __name__ == '__main__':
    main()
