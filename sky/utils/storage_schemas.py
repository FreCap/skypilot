"""JSON schema construction for storage and volume definitions."""

from sky.skylet import constants

_LABELS_SCHEMA = {
    'labels': {
        'type': 'object',
        'required': [],
        'additionalProperties': {
            'type': 'string',
        },
    }
}


# Note: This is similar to schemas._get_infra_pattern()
# but without the wildcard patterns.
def _get_volume_infra_pattern():
    # Building the regex pattern for the infra field
    # Format: cloud[/region[/zone]] or wildcards or kubernetes context
    # Match any cloud name (case insensitive)
    all_clouds = list(constants.ALL_CLOUDS)
    all_clouds.remove('kubernetes')
    cloud_pattern = f'(?i:({"|".join(all_clouds)}))'

    # Optional /region followed by optional /zone
    # /[^/]+ matches a slash followed by any characters except slash (region or
    # zone name)
    # The outer (?:...)? makes the entire region/zone part optional
    region_zone_pattern = '(?:/[^/]+(?:/[^/]+)?)?'

    # Kubernetes specific pattern - matches:
    # 1. Just the word "kubernetes" or "k8s" by itself
    # 2. "k8s/" or "kubernetes/" followed by any context name (which may
    # contain slashes)
    kubernetes_pattern = '(?i:kubernetes|k8s)(?:/.+)?'

    # Combine all patterns with alternation (|)
    # ^ marks start of string, $ marks end of string
    infra_pattern = (f'^(?:{cloud_pattern}{region_zone_pattern}|'
                     f'{kubernetes_pattern})$')
    return infra_pattern


def get_volume_schema():
    # pylint: disable=import-outside-toplevel
    from sky.utils import volume

    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': ['name', 'type'],
        'additionalProperties': False,
        'properties': {
            'name': {
                'type': 'string',
            },
            'type': {
                'type': 'string',
                'case_sensitive_enum': [
                    type.value for type in volume.VolumeType
                ],
            },
            'infra': {
                'type': 'string',
                'description': ('Infrastructure specification in format: '
                                'cloud[/region[/zone]].'),
                # Pattern validates:
                # 1. cloud[/region[/zone]] - e.g. "aws", "aws/us-east-1",
                #    "aws/us-east-1/us-east-1a"
                # 2. Kubernetes patterns - e.g. "kubernetes/my-context",
                #    "k8s/context-name",
                #    "k8s/aws:eks:us-east-1:123456789012:cluster/my-cluster"
                'pattern': _get_volume_infra_pattern(),
            },
            'size': {
                'type': 'string',
                'pattern': constants.MEMORY_SIZE_PATTERN,
            },
            'use_existing': {
                'type': 'boolean',
            },
            'config': {
                'type': 'object',
                'required': [],
                'properties': {
                    'storage_class_name': {
                        'type': 'string',
                    },
                    'access_mode': {
                        'type': 'string',
                        'case_sensitive_enum': [
                            type.value for type in volume.VolumeAccessMode
                        ],
                    },
                    'namespace': {
                        'type': 'string',
                    },
                    'host_path': {
                        'type': 'string',
                    },
                    'cleanup_on_deletion': {
                        'type': 'boolean',
                    },
                },
            },
            **_LABELS_SCHEMA,
        }
    }


def get_storage_schema():
    # pylint: disable=import-outside-toplevel
    from sky.data import storage

    # Refer to https://rclone.org/docs/#options for more information
    # on rclone-specific nomenclature.
    rclone_memory_units = ('B', 'K', 'M', 'G', 'T', 'P')
    rclone_memory_pattern = (
        '^[0-9]+('
        f'{"|".join([unit.lower() for unit in rclone_memory_units])}|'
        f'{"|".join([unit.upper() for unit in rclone_memory_units])})?$')
    rclone_duration_pattern = (
        r'^(?:(?:[-+]?(?:\d+(?:\.\d+)?|\.\d+)'
        r'(?:ms|[smhdwMy]))+|([-+]?(?:\d+(?:\.\d+)?|\.\d+)))$')

    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'name': {
                'type': 'string',
            },
            'source': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'array',
                    'minItems': 1,
                    'items': {
                        'type': 'string'
                    }
                }]
            },
            'store': {
                'type': 'string',
                'case_insensitive_enum': [
                    type.value for type in storage.StoreType
                ]
            },
            'persistent': {
                'type': 'boolean',
            },
            'mode': {
                'type': 'string',
                'case_insensitive_enum': [
                    mode.value for mode in storage.StorageMode
                ]
            },
            'type': {
                'type': 'string',
                'case_insensitive_enum': [
                    t.value for t in storage.FileMountType
                ]
            },
            'config': {
                'type': 'object',
                'properties': {
                    'disk_size': {
                        'anyOf': [{
                            'type': 'string',
                            'pattern': constants.MEMORY_SIZE_PATTERN,
                        }, {
                            'type': 'integer',
                        }],
                    },
                    'disk_tier': {
                        'type': 'string',
                    },
                    'storage_type': {
                        'type': 'string',
                    },
                    'attach_mode': {
                        'type': 'string',
                    },
                    'mount_cached': {
                        'type': 'object',
                        'additionalProperties': False,
                        'properties': {
                            'transfers': {
                                'type': 'integer',
                                'minimum': 1,
                            },
                            'buffer_size': {
                                'type': 'string',
                                'pattern': rclone_memory_pattern,
                            },
                            'vfs_cache_max_size': {
                                'type': 'string',
                                'pattern': rclone_memory_pattern,
                            },
                            'vfs_cache_max_age': {
                                'type': 'string',
                                'pattern': rclone_duration_pattern,
                            },
                            'vfs_read_ahead': {
                                'type': 'string',
                                'pattern': rclone_memory_pattern,
                            },
                            'vfs_read_chunk_size': {
                                'type': 'string',
                                'pattern': rclone_memory_pattern,
                            },
                            'vfs_read_chunk_streams': {
                                'type': 'integer',
                                'minimum': 0,
                            },
                            'vfs_write_back': {
                                'type': 'string',
                                'pattern': rclone_duration_pattern,
                            },
                            'read_only': {
                                'type': 'boolean',
                            },
                        },
                    },
                    'mount': {
                        'type': 'object',
                        'additionalProperties': False,
                        'properties': {
                            'read_only': {
                                'type': 'boolean',
                            },
                            # Hugging Face stores only: extra ``hf-mount``
                            # flags forwarded verbatim to the daemon. Each
                            # element is one shell token.
                            'hf_mount_args': {
                                'type': 'array',
                                'items': {
                                    'type': 'string',
                                },
                            },
                        },
                    },
                },
            },
            '_is_sky_managed': {
                'type': 'boolean',
            },
            '_bucket_sub_path': {
                'type': 'string',
            },
            '_store_region': {
                'type': 'string',
            },
            '_force_delete': {
                'type': 'boolean',
            }
        }
    }


def get_volume_mount_schema():
    """Schema for volume mount object in task config (internal use only)."""
    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'path': {
                'type': 'string',
            },
            'volume_name': {
                'type': 'string',
            },
            'is_ephemeral': {
                'type': 'boolean',
            },
            'sub_path': {
                'type': 'string',
                'pattern': constants.SUB_PATH_PATTERN,
            },
            'volume_config': {
                'type': 'object',
                'required': [],
                'additionalProperties': True,
                'properties': {
                    'cloud': {
                        'type': 'string',
                        'case_insensitive_enum': list(constants.ALL_CLOUDS)
                    },
                    'region': {
                        'anyOf': [{
                            'type': 'string'
                        }, {
                            'type': 'null'
                        }]
                    },
                    'zone': {
                        'anyOf': [{
                            'type': 'string'
                        }, {
                            'type': 'null'
                        }]
                    },
                },
            }
        }
    }
