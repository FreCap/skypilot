"""Pure DigitalOcean instance-tag projection."""

import copy
import hashlib

from sky.provision import constants as provision_constants

TAG_SKYPILOT_CLUSTER_INCARNATION = 'skypilot-cluster-incarnation'

_INCARNATION_DOMAIN_SEPARATOR = b'skypilot-do-cluster-incarnation-v1\0'
_INCARNATION_TAG_PREFIX = f'{TAG_SKYPILOT_CLUSTER_INCARNATION}:'


def project_instance_tags(
    cluster_name_on_cloud: str,
    user_tags: dict[str, str],
    cluster_incarnation: object,
) -> list[str]:
    """Projects legacy tags and an optional cluster-incarnation marker."""
    # Preserve the existing sorting, copying, and managed-tag override behavior.
    sorted_user_tags = dict(sorted(copy.deepcopy(user_tags).items()))
    projected_tags = {
        'Name': cluster_name_on_cloud,
        provision_constants.TAG_RAY_CLUSTER_NAME: cluster_name_on_cloud,
        provision_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud,
        **sorted_user_tags,
    }
    formatted_tags = [f'{key}:{value}' for key, value in projected_tags.items()]

    # Only an exact string opts into the new marker. All other runtime values
    # retain the legacy request exactly, including marker-like user tags.
    if type(cluster_incarnation) is not str:
        return formatted_tags

    reserved_prefix = _INCARNATION_TAG_PREFIX.casefold()
    formatted_tags = [
        tag for tag in formatted_tags
        if not tag.casefold().startswith(reserved_prefix)
    ]
    digest = hashlib.sha256(_INCARNATION_DOMAIN_SEPARATOR)
    digest.update(cluster_incarnation.encode('utf-8', errors='surrogatepass'))
    formatted_tags.append(f'{_INCARNATION_TAG_PREFIX}v1-{digest.hexdigest()}')
    return formatted_tags
