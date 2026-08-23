"""This module contains schemas used to validate objects.

Schemas conform to the JSON Schema specification as defined at
https://json-schema.org/
"""
import enum
import os
from typing import Any

from sky.skylet import autostop_lib
from sky.skylet import constants
from sky.utils import kubernetes_enums
from sky.utils import storage_schemas as _storage_schemas
from sky.utils.service_schema import get_service_schema
from sky.utils.storage_schemas import get_storage_schema
from sky.utils.storage_schemas import get_volume_mount_schema
from sky.utils.storage_schemas import get_volume_schema

# Preserve historical private aliases used by the facade.
_get_volume_infra_pattern = (  # pylint: disable=protected-access
    _storage_schemas._get_volume_infra_pattern)  # pylint: disable=protected-access
_LABELS_SCHEMA = _storage_schemas._LABELS_SCHEMA  # pylint: disable=protected-access

# Preserve the established public and serialized function identity.
get_service_schema.__module__ = __name__
get_storage_schema.__module__ = __name__
get_volume_mount_schema.__module__ = __name__
get_volume_schema.__module__ = __name__

# Registry for plugin-provided job_recovery schema properties.
# Plugins call register_job_recovery_property() to add strategy-specific
# config fields. On the server, once plugins have loaded, their properties
# are registered so additionalProperties is False. On the client (or
# before plugins load), additionalProperties is True to let plugin
# config pass through for server-side validation.
_extra_job_recovery_properties: dict[str, Any] = {}


def register_job_recovery_property(name: str, schema: dict[str, Any]) -> None:
    """Register an additional property for the job_recovery schema.

    This allows plugins to extend the job_recovery dict schema with
    strategy-specific configuration fields. The property is merged into
    the schema's properties dict, so it passes JSON schema validation
    even with additionalProperties: False.

    Args:
        name: The property name.
        schema: The JSON Schema for the property
            (e.g., {'type': 'integer'}).
    """
    _extra_job_recovery_properties[name] = schema


_extra_jobs_properties: dict[str, Any] = {}

_extra_kubernetes_properties: dict[str, Any] = {}

# Registry for plugin-provided properties under the top-level
# `plugins:` config section. Keyed by plugin name.
_extra_plugin_properties: dict[str, Any] = {}


def register_plugin_property(name: str, schema: dict[str, Any]) -> None:
    """Register a sub-property of the top-level `plugins:` config section."""
    if name in _extra_plugin_properties:
        raise ValueError(f'Plugin property {name!r} is already registered.')
    _extra_plugin_properties[name] = schema


def _allow_additional_properties() -> bool:
    """Return True if schemas should allow additional properties.

    On the client (ENV_VAR_IS_SKYPILOT_SERVER not set), always allow
    additional properties so they pass through for server-side validation.
    On the server, allow additional properties only until plugins have
    been loaded — after that, enforce strict validation.
    """
    if os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is None:
        return True
    # Import here to avoid circular imports (plugins imports from sky.utils).
    from sky.server import plugins  # pylint: disable=import-outside-toplevel
    return not plugins.plugins_loaded()


def register_jobs_property(name: str, schema: dict[str, Any]) -> None:
    """Register an additional property for the jobs controller schema.

    This allows plugins to extend the ``jobs`` config section with
    custom configuration fields.  The property is merged into the
    schema's properties dict, so it passes JSON schema validation
    even with additionalProperties: False.

    Args:
        name: The property name.
        schema: The JSON Schema for the property
            (e.g., {'type': 'boolean'}).
    """
    if name in _extra_jobs_properties:
        raise ValueError(f'Jobs property {name!r} is already registered.')
    _extra_jobs_properties[name] = schema


def register_kubernetes_property(name: str, schema: dict[str, Any]) -> None:
    """Register an additional property for the kubernetes schema.

    This allows plugins to extend the kubernetes dict schema with
    kubernetes-specific configuration fields. The property is merged into
    the schema's properties dict, so it passes JSON schema validation
    even with additionalProperties: False.

    Args:
        name: The property name.
        schema: The JSON Schema for the property
            (e.g., {'type': 'string'}).
    """
    _extra_kubernetes_properties[name] = schema


def _check_not_both_fields_present(field1: str, field2: str):
    return {
        'oneOf': [{
            'required': [field1],
            'not': {
                'required': [field2]
            }
        }, {
            'required': [field2],
            'not': {
                'required': [field1]
            }
        }, {
            'not': {
                'anyOf': [{
                    'required': [field1]
                }, {
                    'required': [field2]
                }]
            }
        }]
    }


_AUTOSTOP_SCHEMA = {
    'anyOf': [
        {
            # Use boolean to disable autostop completely, e.g.
            #   autostop: false
            'type': 'boolean',
        },
        {
            # Shorthand to set idle_minutes by directly specifying, e.g.
            #   autostop: 5
            'anyOf': [{
                'type': 'string',
                'pattern': constants.TIME_PATTERN,
                'minimum': 0,
            }, {
                'type': 'integer',
            }]
        },
        {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {
                # TODO(luca): update field to use time units as well.
                'idle_minutes': {
                    'type': 'integer',
                    'minimum': 0,
                },
                'down': {
                    'type': 'boolean',
                },
                'wait_for': {
                    'type': 'string',
                    'case_insensitive_enum':
                        autostop_lib.AutostopWaitFor.supported_modes(),
                },
                # TODO(zpoint): remove after v0.15.0 — routed into
                # top-level resources.hooks for backward compatibility.
                'hook': {
                    'type': 'string',
                },
                'hook_timeout': {
                    'type': 'integer',
                    'minimum': 1,
                }
            },
        },
    ],
}

# Supported events in config.hooks[*].events.
_HOOK_EVENTS = ['stop', 'preemption', 'down']

_HOOKS_SCHEMA = {
    'type': 'array',
    # Bound the array so the SetAutostop request payload can't grow
    # past gRPC's default max_receive_message_length (4 MB). 32 entries
    # × 16 KiB per `run` plus event/timeout overhead leaves comfortable
    # headroom.
    'maxItems': 32,
    'items': {
        'type': 'object',
        'required': ['run'],
        'additionalProperties': False,
        'properties': {
            'run': {
                'type': 'string',
                'minLength': 1,
                # 16 KiB cap. Matches typical shell-script size limits
                # and keeps gRPC payloads tractable. Users with large
                # bodies should put the script under workdir/ and call
                # it from `run:` instead.
                'maxLength': 16 * 1024,
            },
            # `events` is optional. When absent, Resources fills the
            # default list (all three events) at load time.
            'events': {
                'type': 'array',
                'minItems': 1,
                'uniqueItems': True,
                'items': {
                    'type': 'string',
                    'enum': _HOOK_EVENTS,
                },
            },
            'timeout': {
                'type': 'integer',
                'minimum': 1,
            },
        },
    },
}


def _get_infra_pattern():
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

    # Wildcard patterns:
    # 1. * - any cloud
    # 2. */region - any cloud with specific region
    # 3. */*/zone - any cloud, any region, specific zone
    wildcard_cloud = '\\*'  # Wildcard for cloud
    wildcard_with_region = '(?:/[^/]+(?:/[^/]+)?)?'

    # Kubernetes specific pattern - matches:
    # 1. Just the word "kubernetes" or "k8s" by itself
    # 2. "k8s/" or "kubernetes/" followed by any context name (which may contain
    # slashes)
    kubernetes_pattern = '(?i:kubernetes|k8s)(?:/.+)?'

    # Combine all patterns with alternation (|)
    # ^ marks start of string, $ marks end of string
    infra_pattern = (f'^(?:{cloud_pattern}{region_zone_pattern}|'
                     f'{wildcard_cloud}{wildcard_with_region}|'
                     f'{kubernetes_pattern})$')
    return infra_pattern


def _get_single_resources_schema():
    """Schema for a single resource in a resources list."""
    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'cloud': {
                'type': 'string',
                'case_insensitive_enum': list(constants.ALL_CLOUDS)
            },
            'region': {
                'type': 'string',
            },
            'zone': {
                'type': 'string',
            },
            'infra': {
                'type': 'string',
                'description':
                    ('Infrastructure specification in format: '
                     'cloud[/region[/zone]]. Use "*" as a wildcard.'),
                # Pattern validates:
                # 1. cloud[/region[/zone]] - e.g. "aws", "aws/us-east-1",
                #    "aws/us-east-1/us-east-1a"
                # 2. Wildcard patterns - e.g. "*", "*/us-east-1",
                #    "*/*/us-east-1a", "aws/*/us-east-1a"
                # 3. Kubernetes patterns - e.g. "kubernetes/my-context",
                #    "k8s/context-name",
                #    "k8s/aws:eks:us-east-1:123456789012:cluster/my-cluster"
                'pattern': _get_infra_pattern(),
            },
            'cpus': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'number',
                }],
            },
            'memory': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'number',
                }],
            },
            'accelerators': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'object',
                    'required': [],
                    'minProperties': 1,
                    'maxProperties': 1,
                    'additionalProperties': {
                        'type': 'number'
                    }
                }]
            },
            'instance_type': {
                'type': 'string',
            },
            'use_spot': {
                'type': 'boolean',
            },
            'job_recovery': {
                # Either a string or a dict.
                'anyOf': [
                    {
                        'type': 'string',
                    },
                    {
                        'type': 'object',
                        'required': [],
                        # On the server, plugins have registered
                        # their properties via
                        # register_job_recovery_property(), so we
                        # can be strict. On the client we allow
                        # unknown properties to pass through for
                        # server-side validation.
                        'additionalProperties': _allow_additional_properties(),
                        'properties': {
                            'strategy': {
                                'anyOf': [{
                                    'type': 'string',
                                }, {
                                    'type': 'null',
                                }],
                            },
                            'max_restarts_on_errors': {
                                'type': 'integer',
                                'minimum': 0,
                            },
                            'recover_on_exit_codes': {
                                'anyOf': [
                                    {
                                        # Single exit code
                                        'type': 'integer',
                                        'minimum': 0,
                                        'maximum': 255,
                                    },
                                    {
                                        # List of exit codes
                                        'type': 'array',
                                        'items': {
                                            'type': 'integer',
                                            'minimum': 0,
                                            'maximum': 255,
                                        },
                                        'uniqueItems': True,
                                    },
                                ],
                            },
                            # Plugin-registered strategy-specific
                            # properties (validated on server side
                            # where plugins are loaded).
                            **_extra_job_recovery_properties,
                        }
                    }
                ],
            },
            'volumes': {
                'type': 'array',
                'items': {
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
                        'path': {
                            'type': 'string',
                        },
                        'auto_delete': {
                            'type': 'boolean',
                        },
                        'storage_type': {
                            'type': 'string',
                        },
                        'name': {
                            'type': 'string',
                        },
                        'attach_mode': {
                            'type': 'string',
                        },
                    },
                },
            },
            'disk_size': {
                'anyOf': [{
                    'type': 'string',
                    'pattern': constants.MEMORY_SIZE_PATTERN,
                }, {
                    'type': 'integer',
                }],
            },
            'ephemeral_storage': {
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
            'network_tier': {
                'type': 'string',
            },
            'local_disk': {
                'type': 'string',
            },
            'max_hourly_cost': {
                'type': 'number',
                'exclusiveMinimum': 0,
            },
            'ports': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'integer',
                }, {
                    'type': 'array',
                    'items': {
                        'anyOf': [{
                            'type': 'string',
                        }, {
                            'type': 'integer',
                        }]
                    }
                }, {
                    'type': 'null',
                }],
            },
            'labels': {
                'type': 'object',
                'additionalProperties': {
                    'type': 'string'
                }
            },
            'accelerator_args': {
                'type': 'object',
                'required': [],
                'additionalProperties': False,
                'properties': {
                    'runtime_version': {
                        'type': 'string',
                    },
                    'tpu_name': {
                        'type': 'string',
                    },
                    'tpu_vm': {
                        'type': 'boolean',
                    },
                    'gcp_queued_resource': {
                        'type': 'boolean',
                    },
                }
            },
            '_no_missing_accel_warnings': {
                'type': 'boolean'
            },
            'image_id': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'object',
                    'required': [],
                }, {
                    'type': 'null',
                }]
            },
            'container_image': {
                'anyOf': [
                    {
                        'type': 'string',
                        'minLength': 1,
                        'maxLength': 1024,
                    },
                    {
                        'type': 'object',
                        'anyOf': [{
                            'required': ['ref'],
                        }, {
                            'required': ['release'],
                        }, {
                            'required': ['version'],
                        }, {
                            'required': ['artifact_id'],
                        }],
                        'additionalProperties': False,
                        'properties': {
                            'ref': {
                                'type': 'string',
                                'minLength': 1,
                                'maxLength': 1024,
                            },
                            'distribution': {
                                'type': 'string',
                                'minLength': 1,
                            },
                            'release': {
                                'type': 'string',
                            },
                            'artifact_id': {
                                'type': 'string',
                                'minLength': 1,
                            },
                            # Pre-release compatibility aliases.
                            'profile': {
                                'type': 'string',
                                'minLength': 1,
                            },
                            'version': {
                                'type': 'string',
                            },
                        },
                    },
                    {
                        'type': 'null',
                    }
                ]
            },
            'autostop': _AUTOSTOP_SCHEMA,
            'priority': {
                'type': 'integer',
                'minimum': constants.MIN_PRIORITY,
                'maximum': constants.MAX_PRIORITY,
            },
            'priority_class': {
                'type': 'string',
            },
            # The following fields are for internal use only. Should not be
            # specified in the task config.
            '_docker_login_config': {
                'type': 'object',
                'required': ['username', 'password', 'server'],
                'additionalProperties': False,
                'properties': {
                    'username': {
                        'type': 'string',
                    },
                    'password': {
                        'type': 'string',
                    },
                    'server': {
                        'type': 'string',
                    }
                }
            },
            '_resolved_container_image': {
                'type': 'object',
                'required': [
                    'image_id', 'reference', 'target_id', 'digest',
                    'auth_strategy'
                ],
                'additionalProperties': False,
                'properties': {
                    'image_id': {
                        'type': 'string',
                    },
                    'reference': {
                        'type': 'string',
                    },
                    'target_id': {
                        'type': 'string',
                    },
                    'digest': {
                        'type': 'string',
                    },
                    'auth_strategy': {
                        'type': 'string',
                    },
                    'location_id': {
                        'anyOf': [{
                            'type': 'string',
                        }, {
                            'type': 'null',
                        }],
                    },
                    'distribution': {
                        'anyOf': [{
                            'type': 'string',
                            'minLength': 1,
                        }, {
                            'type': 'null',
                        }],
                    },
                    'profile_revision': {
                        'anyOf': [{
                            'type': 'integer',
                            'minimum': 1,
                        }, {
                            'type': 'null',
                        }],
                    },
                    'policy_fingerprint': {
                        'anyOf': [{
                            'type': 'string',
                            'pattern': '^[0-9a-f]{64}$',
                        }, {
                            'type': 'null',
                        }],
                    },
                    'status': {
                        'type': 'string',
                        'enum': ['READY', 'WARMING'],
                    },
                    'fallback_reason': {
                        'anyOf': [{
                            'type': 'string',
                        }, {
                            'type': 'null',
                        }],
                    },
                },
            },
            '_is_image_managed': {
                'type': 'boolean',
            },
            '_requires_fuse': {
                'type': 'boolean',
            },
            '_cluster_config_overrides': {
                'type': 'object',
            },
        }
    }


def _get_multi_resources_schema():
    multi_resources_schema = {
        k: v
        for k, v in _get_single_resources_schema().items()
        # Validation may fail if $schema is included.
        if k != '$schema'
    }
    return multi_resources_schema


def get_resources_schema():
    """Resource schema in task config."""
    single_resources_schema = _get_single_resources_schema()['properties']
    single_resources_schema.pop('accelerators')
    multi_resources_schema = _get_multi_resources_schema()
    return {
        '$schema': 'http://json-schema.org/draft-07/schema#',
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            **single_resources_schema,
            # We redefine the 'accelerators' field to allow one line list or
            # a set of accelerators.
            'accelerators': {
                # {'V100:1', 'A100:1'} will be
                # read as a string and converted to dict.
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'object',
                    'required': [],
                    'minProperties': 1,
                    'additionalProperties': {
                        'anyOf': [{
                            'type': 'null',
                        }, {
                            'type': 'number',
                        }]
                    }
                }, {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    }
                }]
            },
            'any_of': {
                'type': 'array',
                'items': multi_resources_schema,
            },
            'ordered': {
                'type': 'array',
                'items': multi_resources_schema,
            }
        },
    }


def _filter_schema(schema: dict, keys_to_keep: list[tuple[str, ...]]) -> dict:
    """Recursively filter a schema to include only certain keys.

    Args:
        schema: The original schema dictionary.
        keys_to_keep: List of tuples with the path of keys to retain.

    Returns:
        The filtered schema.
    """
    # Convert list of tuples to a dictionary for easier access
    paths_dict: dict[str, Any] = {}
    for path in keys_to_keep:
        current = paths_dict
        for step in path:
            if step not in current:
                current[step] = {}
            current = current[step]

    def keep_keys(current_schema: dict, current_path_dict: dict,
                  new_schema: dict) -> dict:
        # Base case: if we reach a leaf in the path_dict, we stop.
        if (not current_path_dict or not isinstance(current_schema, dict) or
                not current_schema.get('properties')):
            return current_schema

        if 'properties' not in new_schema:
            new_schema = {
                key: current_schema[key]
                for key in current_schema
                # We do not support the handling of `oneOf`, `anyOf`, `allOf`,
                # `required` for now.
                if key not in
                {'properties', 'oneOf', 'anyOf', 'allOf', 'required'}
            }
            new_schema['properties'] = {}
        for key, sub_schema in current_schema['properties'].items():
            if key in current_path_dict:
                # Recursively keep keys if further path dict exists
                new_schema['properties'][key] = {}
                current_path_value = current_path_dict.pop(key)
                new_schema['properties'][key] = keep_keys(
                    sub_schema, current_path_value,
                    new_schema['properties'][key])

        return new_schema

    # Start the recursive filtering
    new_schema = keep_keys(schema, paths_dict, {})
    assert not paths_dict, f'Unprocessed keys: {paths_dict}'
    return new_schema


def _task_config_schema():
    """Schema for task-YAML's `config:` block.

    Hand-merged from the global config schema (filtered to overrideable
    keys) plus the task-only `hooks` property. `hooks` is intentionally
    NOT exposed in the global ``~/.sky/config.yaml`` schema — lifecycle
    hooks are task-scoped (preserving the original ``resources.hooks:``
    placement).
    """
    overrideable = _filter_schema(
        get_config_schema(),
        constants.OVERRIDEABLE_CONFIG_KEYS_IN_TASK)['properties']
    return {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            **overrideable,
            'hooks': _HOOKS_SCHEMA,
        },
    }


def get_task_schema():
    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'name': {
                'type': 'string',
            },
            'workdir': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'object',
                    'required': ['url'],
                    'additionalProperties': False,
                    'properties': {
                        'url': {
                            'type': 'string',
                        },
                        'ref': {
                            'type': 'string',
                        },
                    },
                }],
            },
            'event_callback': {
                'type': 'string',
            },
            'num_nodes': {
                'type': 'integer',
            },
            # resources config is validated separately using RESOURCES_SCHEMA
            'resources': {
                'type': 'object',
            },
            # storage config is validated separately using STORAGE_SCHEMA
            'file_mounts': {
                'type': 'object',
            },
            # service config is validated separately using SERVICE_SCHEMA
            'service': {
                'type': 'object',
            },
            'pool': {
                'type': 'object',
            },
            'setup': {
                'type': 'string',
            },
            'run': {
                'type': 'string',
            },
            'envs': {
                'type': 'object',
                'required': [],
                'patternProperties': {
                    # Checks env keys are valid env var names.
                    '^[a-zA-Z_][a-zA-Z0-9_]*$': {
                        'type': ['string', 'null']
                    }
                },
                'additionalProperties': False,
            },
            'secrets': {
                'oneOf': [
                    {
                        'type': 'object',
                        # Dict form: inline secrets + managed refs
                        'additionalProperties': {
                            'type': ['string', 'null']
                        },
                    },
                    {
                        'type': 'array',
                        # Array form: managed secret refs only
                        'items': {
                            'type': 'string'
                        },
                    },
                ],
            },
            'managed_secrets': {
                'type': 'array',
                'items': {
                    'oneOf': [
                        {
                            'type': 'string'
                        },
                        {
                            'type': 'object',
                            'maxProperties': 1,
                            'additionalProperties': {
                                'type': 'object',
                                'properties': {
                                    'mount_path': {
                                        'type': 'string'
                                    },
                                },
                                'additionalProperties': False,
                            },
                        },
                    ],
                },
            },
            # inputs and outputs are experimental
            'inputs': {
                'type': 'object',
                'required': [],
                'maxProperties': 1,
                'additionalProperties': {
                    'type': 'number'
                }
            },
            'outputs': {
                'type': 'object',
                'required': [],
                'maxProperties': 1,
                'additionalProperties': {
                    'type': 'number'
                }
            },
            'file_mounts_mapping': {
                'type': 'object',
            },
            # Per-task config block. Hand-merged so we can add a
            # task-only `hooks:` key that is intentionally NOT part of
            # the global `~/.sky/config.yaml` schema (lifecycle hooks
            # are task-scoped, not workspace/operator-scoped).
            'config': _task_config_schema(),
            # volumes config is validated separately using get_volume_schema
            'volumes': {
                'type': 'object',
            },
            'volume_mounts': {
                'type': 'array',
                'items': get_volume_mount_schema(),
            },
            'api_server_access': {
                'type': 'boolean',
            },
            '_metadata': {
                'type': 'object',
            },
        }
    }


def get_cluster_schema():
    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': ['cluster', 'auth'],
        'additionalProperties': False,
        'properties': {
            'cluster': {
                'type': 'object',
                'required': ['ips', 'name'],
                'additionalProperties': False,
                'properties': {
                    'ips': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                        }
                    },
                    'name': {
                        'type': 'string',
                    },
                }
            },
            'auth': {
                'type': 'object',
                'required': ['ssh_user', 'ssh_private_key'],
                'additionalProperties': False,
                'properties': {
                    'ssh_user': {
                        'type': 'string',
                    },
                    'ssh_private_key': {
                        'type': 'string',
                    },
                }
            },
            'python': {
                'type': 'string',
            },
        }
    }


_NETWORK_CONFIG_SCHEMA = {
    'use_internal_ips': {
        'type': 'boolean',
    },
    'ssh_proxy_command': {
        'oneOf': [{
            'type': 'string',
        }, {
            'type': 'null',
        }, {
            'type': 'object',
            'required': [],
            'additionalProperties': {
                'anyOf': [
                    {
                        'type': 'string'
                    },
                    {
                        'type': 'null'
                    },
                ]
            }
        }]
    },
}

_PROPERTY_NAME_OR_CLUSTER_NAME_TO_PROPERTY = {
    'oneOf': [
        {
            'type': 'string',
            'minLength': 1,
        },
        {
            # A list of single-element dict to pretain the
            # order.
            # Example:
            #  property_name:
            #    - my-cluster1-*: my-property-1
            #    - my-cluster2-*: my-property-2
            #    - "*"": my-property-3
            'type': 'array',
            'items': {
                'type': 'object',
                'additionalProperties': {
                    'type': 'string',
                    'minLength': 1,
                },
                'maxProperties': 1,
                'minProperties': 1,
            },
        }
    ]
}


class RemoteIdentityOptions(enum.Enum):
    """Enum for remote identity types.

    Some clouds (e.g., AWS, Kubernetes) also allow string values for remote
    identity, which map to the service account/role to use. Those are not
    included in this enum.
    """
    LOCAL_CREDENTIALS = 'LOCAL_CREDENTIALS'
    SERVICE_ACCOUNT = 'SERVICE_ACCOUNT'
    NO_UPLOAD = 'NO_UPLOAD'


def get_default_remote_identity(cloud: str) -> str:
    """Get the default remote identity for the specified cloud."""
    if cloud in ('kubernetes', 'ssh'):
        return RemoteIdentityOptions.SERVICE_ACCOUNT.value
    return RemoteIdentityOptions.LOCAL_CREDENTIALS.value


_CAPABILITIES_SCHEMA = {
    'capabilities': {
        'type': 'array',
        'items': {
            'type': 'string',
            'case_insensitive_enum': ['compute', 'storage']
        },
    }
}

_REMOTE_IDENTITY_SCHEMA = {
    'remote_identity': {
        'type': 'string',
        'case_insensitive_enum': [
            option.value for option in RemoteIdentityOptions
        ]
    }
}

_REMOTE_IDENTITY_SCHEMA_KUBERNETES = {
    'remote_identity': {
        'anyOf': [{
            'type': 'string'
        }, {
            'type': 'object',
            'additionalProperties': {
                'type': 'string'
            }
        }]
    },
}

_SBATCH_OPTIONS_SCHEMA = {
    'type': 'object',
    'required': [],
    'additionalProperties': {
        'oneOf': [
            {
                'type': 'string',
                # Disallow newlines to prevent script injection in
                # #SBATCH directives.
                'pattern': r'^[^\n]*$'
            },
            {
                'type': 'number'
            },
            {
                'type': 'boolean'
            },
            {
                'type': 'null'
            },
        ]
    },
}

_GPU_PARTITION_MAP_SCHEMA = {
    'type': 'object',
    'required': [],
    'additionalProperties': {
        'anyOf': [{
            'type': 'string',
        }, {
            'type': 'array',
            'items': {
                'type': 'string',
            },
        }],
    },
}

_PRICING_SCHEMA = {
    'type': 'object',
    'required': [],
    'additionalProperties': False,
    'properties': {
        'cpu': {
            'type': 'number',
            'minimum': 0
        },
        'memory': {
            'type': 'number',
            'minimum': 0
        },
        'accelerators': {
            'type': 'object',
            'required': [],
            'additionalProperties': {
                'type': 'number',
                'minimum': 0
            },
        },
    },
}

_CONTEXT_CONFIG_SCHEMA_MINIMAL = {
    'pod_config': {
        'type': 'object',
        'required': [],
        # Allow arbitrary keys since validating pod spec is hard
        'additionalProperties': True,
    },
    'provision_timeout': {
        'type': 'integer',
    },
    'custom_metadata': {
        'type': 'object',
        'required': [],
        # Allow arbitrary keys since validating metadata is hard
        'additionalProperties': True,
        # Disallow 'name' and 'namespace' keys in this dict
        'not': {
            'anyOf': [{
                'required': ['name']
            }, {
                'required': ['namespace']
            }]
        },
    },
}

_SERVE_CACHE_ATTESTATION_SCHEMA = {
    'type': 'object',
    'required': [
        'attestation_id', 'device_source_pattern', 'filesystem_type',
        'required_bytes_per_replica', 'required_inodes_per_replica',
        'max_replicas_per_node', 'reserved_bytes_per_node',
        'reserved_inodes_per_node', 'usable_bytes_per_node',
        'usable_inodes_per_node'
    ],
    'additionalProperties': False,
    'properties': {
        'attestation_id': {
            'type': 'string',
            'minLength': 1,
        },
        'device_source_pattern': {
            'type': 'string',
            'minLength': 3,
        },
        'filesystem_type': {
            'type': 'string',
            'minLength': 1,
        },
        'required_bytes_per_replica': {
            'type': 'integer',
            'minimum': 1,
        },
        'required_inodes_per_replica': {
            'type': 'integer',
            'minimum': 1,
        },
        'max_replicas_per_node': {
            'type': 'integer',
            'minimum': 1,
        },
        'reserved_bytes_per_node': {
            'type': 'integer',
            'minimum': 0,
        },
        'reserved_inodes_per_node': {
            'type': 'integer',
            'minimum': 0,
        },
        'usable_bytes_per_node': {
            'type': 'integer',
            'minimum': 1,
        },
        'usable_inodes_per_node': {
            'type': 'integer',
            'minimum': 1,
        },
    },
}

_SERVE_WORKER_CACHE_SCHEMA = {
    'oneOf': [{
        'type': 'object',
        'required': ['kind'],
        'additionalProperties': False,
        'properties': {
            'kind': {
                'const': 'none',
            },
        },
    }, {
        'type': 'object',
        'required': ['kind', 'mount_path', 'volume_name', 'attestation'],
        'additionalProperties': False,
        'properties': {
            'kind': {
                'const': 'node_local',
            },
            'mount_path': {
                'type': 'string',
                'pattern': '^/',
            },
            'volume_name': {
                'type': 'string',
                'minLength': 1,
            },
            'host_mount_path': {
                'type': 'string',
                'pattern': '^/',
            },
            'bootstrap_image': {
                'type': 'string',
                'pattern': r'^[^\s@]+@sha256:[0-9a-f]{64}$',
            },
            'attestation': _SERVE_CACHE_ATTESTATION_SCHEMA,
        },
    }],
}

_SERVE_WORKER_SCRATCH_SCHEMA = {
    'oneOf': [{
        'type': 'object',
        'required': ['kind'],
        'additionalProperties': False,
        'properties': {
            'kind': {
                'const': 'none',
            },
        },
    }, {
        'type': 'object',
        'required': ['kind', 'size_limit_bytes'],
        'additionalProperties': False,
        'properties': {
            'kind': {
                'const': 'memory',
            },
            'size_limit_bytes': {
                'type': 'integer',
                'minimum': 1,
                'maximum': 9223372036854775807,
            },
        },
    }],
}

_SERVE_CONTROLLER_WORK_CACHE_SCHEMA = {
    'oneOf': [{
        'type': 'object',
        'required': [
            'kind', 'mount_path', 'required_bytes', 'required_inodes',
            'size_limit_bytes'
        ],
        'additionalProperties': False,
        'properties': {
            'kind': {
                'const': 'empty_dir',
            },
            'mount_path': {
                'type': 'string',
                'pattern': '^/',
            },
            **{
                key: {
                    'type': 'integer',
                    'minimum': 1,
                } for key in ('required_bytes', 'required_inodes', 'size_limit_bytes')
            }
        },
    }, {
        'type': 'object',
        'required': [
            'kind', 'mount_path', 'volume_name', 'required_bytes',
            'required_inodes', 'attestation'
        ],
        'additionalProperties': False,
        'properties': {
            'kind': {
                'const': 'node_local',
            },
            'mount_path': {
                'type': 'string',
                'pattern': '^/',
            },
            'volume_name': {
                'type': 'string',
                'minLength': 1,
            },
            'required_bytes': {
                'type': 'integer',
                'minimum': 1,
            },
            'required_inodes': {
                'type': 'integer',
                'minimum': 1,
            },
            'attestation': _SERVE_CACHE_ATTESTATION_SCHEMA,
        },
    }],
}

_SERVE_CONTROLLER_CONTEXT_SCHEMA = {
    'type': 'string',
    'minLength': 1,
}

_SERVE_CONTROLLER_WORKSPACE_SCHEMA = {
    'type': 'string',
    'minLength': 1,
}

_SERVE_CONTROLLER_PRIORITY_CLASS_NAME_SCHEMA = {
    'type': 'string',
    'minLength': 1,
}

_SERVE_WORKER_PRIORITY_CLASS_NAME_SCHEMA = {
    'oneOf': [{
        'type': 'string',
        'minLength': 1,
    }, {
        'type': 'null',
    }],
}

_SERVE_WORKER_KUEUE_WORKLOAD_PRIORITY_CLASS_NAME_SCHEMA = {
    'oneOf': [{
        'type': 'string',
        'minLength': 1,
    }, {
        'type': 'null',
    }],
}

_SERVE_WORKER_PRIORITY_VALUE_SCHEMA = {
    'oneOf': [{
        'type': 'integer',
        'minimum': -2147483648,
        'maximum': 1000000000,
    }, {
        'type': 'null',
    }],
}

_SERVE_WORKER_ACCELERATOR_SCHEDULING_SCHEMA = {
    'type': 'object',
    'minProperties': 1,
    'additionalProperties': {
        'type': 'object',
        'required': ['label_key', 'label_values', 'resource_key'],
        'additionalProperties': False,
        'properties': {
            'label_key': {
                'type': 'string',
                'minLength': 1,
            },
            'label_values': {
                'type': 'array',
                'minItems': 1,
                'maxItems': 16,
                'uniqueItems': True,
                'items': {
                    'type': 'string',
                    'minLength': 1,
                },
            },
            'resource_key': {
                'type': 'string',
                'minLength': 1,
            },
        },
    },
}

_SERVE_WORKER_PREEMPTION_POLICY_SCHEMA = {
    'oneOf': [{
        'enum': ['Never', 'PreemptLowerPriority'],
    }, {
        'type': 'null',
    }],
}

_SERVE_CONTROLLER_LB_DATA_PLANE_AUTH_SCHEMA = {
    'type': 'object',
    'required': ['secret_name', 'secret_key'],
    'additionalProperties': False,
    'properties': {
        'secret_name': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 253,
            'pattern': '^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$',
        },
        'secret_key': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 253,
            'pattern': '^[-._A-Za-z0-9]+$',
        },
    },
}

_SERVE_WORKER_POD_IDENTITY_ROLE_ARN_SCHEMA = {
    'oneOf': [{
        'type': 'string',
        'pattern': ('^arn:(?:aws|aws-us-gov|aws-cn):iam::[0-9]{12}:'
                    'role/[A-Za-z0-9+=,.@_/-]+$'),
    }, {
        'type': 'null',
    }],
}

_CONTEXT_CONFIG_SCHEMA_KUBERNETES = {
    'allowed_nodes': {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'label_selector': {
                # Each key-value pair is OR'd: a node matches if ANY
                # label matches. This differs from K8s label selectors
                # which are AND'd.
                'type': 'object',
                'additionalProperties': {
                    'type': 'string'
                },
            },
            'names': {
                'type': 'array',
                'items': {
                    'type': 'string'
                },
            },
            'ips': {
                'type': 'array',
                'items': {
                    'type': 'string'
                },
            },
        },
    },
    'serve_controller_work_cache': _SERVE_CONTROLLER_WORK_CACHE_SCHEMA,
    'serve_controller_lb_data_plane_auth': _SERVE_CONTROLLER_LB_DATA_PLANE_AUTH_SCHEMA,
    'serve_controller_priority_class_name': _SERVE_CONTROLLER_PRIORITY_CLASS_NAME_SCHEMA,
    'serve_worker_cache': _SERVE_WORKER_CACHE_SCHEMA,
    'serve_worker_scratch': _SERVE_WORKER_SCRATCH_SCHEMA,
    'serve_worker_priority_class_name': _SERVE_WORKER_PRIORITY_CLASS_NAME_SCHEMA,
    'serve_worker_kueue_workload_priority_class_name': _SERVE_WORKER_KUEUE_WORKLOAD_PRIORITY_CLASS_NAME_SCHEMA,
    'serve_worker_priority_value': _SERVE_WORKER_PRIORITY_VALUE_SCHEMA,
    'serve_worker_preemption_policy': _SERVE_WORKER_PREEMPTION_POLICY_SCHEMA,
    'serve_worker_accelerator_scheduling': _SERVE_WORKER_ACCELERATOR_SCHEDULING_SCHEMA,
    'serve_worker_pod_identity_role_arn': _SERVE_WORKER_POD_IDENTITY_ROLE_ARN_SCHEMA,
    # TODO(kevin): Remove 'networking' in v0.13.0.
    'networking': {
        'type': 'string',
        'case_insensitive_enum': [
            type.value for type in kubernetes_enums.KubernetesNetworkingMode
        ],
    },
    'ports': {
        'type': 'string',
        'case_insensitive_enum': [
            type.value for type in kubernetes_enums.KubernetesPortMode
        ],
    },
    **_CONTEXT_CONFIG_SCHEMA_MINIMAL,
    'namespace': {
        'type': 'string',
    },
    'autoscaler': {
        'type': 'string',
        'case_insensitive_enum': [
            type.value for type in kubernetes_enums.KubernetesAutoscalerType
        ],
    },
    'high_availability': {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'storage_class_name': {
                'type': 'string',
            }
        },
    },
    'kueue': {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'local_queue_name': {
                'type': 'string',
            },
            'require_managed': {
                'type': 'boolean',
            },
        },
    },
    # Alias of `kueue.local_queue_name`; `quota.queue` takes precedence
    # when both are set. Permissive so external schedulers (registered
    # via plugins) can layer their own sub-fields under `quota` without
    # requiring per-key OSS schema updates; sub-field validation is the
    # consumer's responsibility.
    'quota': {
        'type': 'object',
        'required': [],
        'additionalProperties': True,
        'properties': {
            'queue': {
                'type': 'string',
            },
        },
    },
    'dws': {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'enabled': {
                'type': 'boolean',
            },
            # Only used when Kueue is enabled.
            'max_run_duration': {
                'anyOf': [{
                    'type': 'string',
                    'pattern': constants.TIME_PATTERN,
                }, {
                    'type': 'integer',
                }]
            },
        },
    },
    'remote_identity': {
        'type': 'string',
    },
    'post_provision_runcmd': {
        'type': 'array',
        'items': {
            'type': 'string'
        },
    },
    'apt_mirrors': {
        # List of APT mirror hostnames (or empty list to disable fallback
        # mirrors entirely) to try in order when installing packages on a
        # provisioned pod. When unset, SkyPilot uses a built-in default list.
        'type': 'array',
        'items': {
            'type': 'string',
            'pattern': '^[a-zA-Z0-9.-]+$',
        },
    },
    'set_pod_resource_limits': {
        # Can be:
        # - false: do not set limits (default)
        # - true: set limits equal to requests (multiplier of 1)
        # - number: set limits to requests * multiplier
        'oneOf': [{
            'type': 'boolean',
        }, {
            'type': 'number',
            'minimum': 1,
        }],
    },
    'pricing': _PRICING_SCHEMA,
    'auto_mounts': {
        'type': 'array',
        'items': {
            'type': 'object',
            'required': ['volume_name', 'mount_paths'],
            'additionalProperties': False,
            'properties': {
                'volume_name': {
                    'type': 'string',
                },
                'mount_paths': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                        'pattern': '^(/|~/|~$)',
                    },
                    'minItems': 1,
                },
            },
        },
    },
    'enable_docker': {
        'oneOf': [
            # Simple form: enable_docker: true / false
            {
                'type': 'boolean'
            },
            # Simple form: enable_docker: "ALL" / "BUILD"
            {
                'type': 'string',
                'enum': ['ALL', 'BUILD'],
            },
            # Detailed form with optional cache volume.
            {
                'type': 'object',
                'required': ['mode'],
                'additionalProperties': False,
                'properties': {
                    'mode': {
                        'type': 'string',
                        'enum': ['ALL', 'BUILD'],
                    },
                    # SkyPilot volume name for the Docker/BuildKit cache.
                    # Omit to use an ephemeral emptyDir volume instead.
                    'cache_volume': {
                        'type': 'string',
                    },
                },
            },
        ],
    },
}


def get_config_schema():
    # pylint: disable=import-outside-toplevel
    from sky.server import daemons

    resources_schema = {
        k: v
        for k, v in get_resources_schema().items()
        # Validation may fail if $schema is included.
        if k != '$schema'
    }
    resources_schema['properties'].pop('ports')

    registry_target_properties = {
        'region': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 64,
        },
        'registry': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 253,
        },
        'repository_prefix': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 255,
        },
        'shard_count': {
            'type': 'integer',
            'minimum': 1,
            'maximum': 256,
        },
        'max_manifests_per_shard': {
            'type': 'integer',
            'minimum': 1,
        },
        'max_declared_bytes_per_shard': {
            'type': 'integer',
            'minimum': 1,
        },
        'max_in_flight': {
            'type': 'integer',
            'minimum': 1,
        },
        'write_authority': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 128,
        },
        'delete_authority': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 128,
        },
        'qualification_delete_authority': {
            'type': 'string',
            'minLength': 1,
            'maxLength': 128,
        },
        'qualification_repository_generation': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 255,
        },
        'runtime_pull': {
            'type': 'object',
            'minProperties': 1,
            'maxProperties': 2,
            'additionalProperties': False,
            'properties': {
                'aws_vm': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 128,
                },
                'aws_eks': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 128,
                },
            },
        },
    }
    canonical_registry_target_schema = {
        'type': 'object',
        'required': [
            key for key in registry_target_properties
            if key != 'qualification_repository_generation'
        ],
        'additionalProperties': False,
        'properties': registry_target_properties,
    }
    regional_registry_target_schema = {
        'type': 'object',
        'required': [
            'name', *[
                key for key in registry_target_properties
                if key != 'qualification_repository_generation'
            ]
        ],
        'additionalProperties': False,
        'properties': {
            'name': {
                'type': 'string',
                'minLength': 1,
                'maxLength': 128,
            },
            **registry_target_properties,
        },
    }
    binding_purposes_schema = {
        'type': 'array',
        'minItems': 1,
        'maxItems': 6,
        'uniqueItems': True,
        'items': {
            'type': 'string',
            'enum': [
                'source_read', 'destination_write', 'verify', 'runtime_pull',
                'lifecycle_delete', 'canary_launch'
            ],
        },
    }
    access_binding_schema = {
        'oneOf': [{
            'type': 'object',
            'required': ['kind', 'authority', 'purposes'],
            'additionalProperties': False,
            'properties': {
                'kind': {
                    'const': 'aws_assume_role',
                },
                'authority': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 2048,
                },
                'external_id': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 1024,
                },
                'purposes': binding_purposes_schema,
            },
        }, {
            'type': 'object',
            'required': [
                'kind', 'principals', 'credential_helper',
                'qualified_node_images', 'instance_profile', 'canary_authority',
                'canary_instance_type', 'canary_subnets', 'purposes'
            ],
            'additionalProperties': False,
            'properties': {
                'kind': {
                    'const': 'aws_ec2_instance_identity',
                },
                'principals': {
                    'type': 'array',
                    'minItems': 1,
                    'maxItems': 256,
                    'uniqueItems': True,
                    'items': {
                        'type': 'string',
                        'minLength': 1,
                        'maxLength': 2048,
                    },
                },
                'credential_helper': {
                    'const': 'amazon-ecr-credential-helper',
                },
                'instance_profile': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 128,
                },
                'qualified_node_images': {
                    'type': 'object',
                    'minProperties': 1,
                    'maxProperties': 64,
                    'additionalProperties': {
                        'type': 'string',
                        'minLength': 1,
                        'maxLength': 128,
                    },
                },
                'canary_authority': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 128,
                },
                'canary_instance_type': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 128,
                },
                'canary_use_spot': {
                    'type': 'boolean',
                    'default': True,
                },
                'canary_subnets': {
                    'type': 'object',
                    'minProperties': 1,
                    'maxProperties': 64,
                    'additionalProperties': {
                        'type': 'array',
                        'minItems': 1,
                        'maxItems': 32,
                        'uniqueItems': True,
                        'items': {
                            'type': 'string',
                            'pattern': '^subnet-[A-Za-z0-9]+$',
                        },
                    },
                },
                'canary_security_groups': {
                    'type': 'object',
                    'maxProperties': 64,
                    'additionalProperties': {
                        'type': 'array',
                        'maxItems': 32,
                        'uniqueItems': True,
                        'items': {
                            'type': 'string',
                            'pattern': '^sg-[A-Za-z0-9]+$',
                        },
                    },
                },
                'purposes': binding_purposes_schema,
            },
        }, {
            'type': 'object',
            'required': [
                'kind', 'qualified_clusters', 'canary_authority', 'purposes'
            ],
            'additionalProperties': False,
            'properties': {
                'kind': {
                    'const': 'aws_eks_kubelet_identity',
                },
                'canary_authority': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 128,
                },
                'qualified_clusters': {
                    'type': 'array',
                    'minItems': 1,
                    'maxItems': 256,
                    'items': {
                        'type': 'object',
                        'required': [
                            'context', 'cluster_arn', 'node_role', 'namespace',
                            'node_selector'
                        ],
                        'additionalProperties': False,
                        'properties': {
                            'context': {
                                'type': 'string',
                                'minLength': 1,
                                'maxLength': 128,
                            },
                            'cluster_arn': {
                                'type': 'string',
                                'minLength': 1,
                                'maxLength': 2048,
                            },
                            'node_role': {
                                'type': 'string',
                                'minLength': 1,
                                'maxLength': 2048,
                            },
                            'namespace': {
                                'type': 'string',
                                'minLength': 1,
                                'maxLength': 253,
                            },
                            'node_selector': {
                                'type': 'object',
                                'minProperties': 1,
                                'maxProperties': 16,
                                'required': ['kubernetes.io/arch'],
                                'properties': {
                                    'kubernetes.io/arch': {
                                        'const': 'amd64',
                                    },
                                },
                                'propertyNames': {
                                    'type': 'string',
                                    'minLength': 1,
                                    'maxLength': 317,
                                },
                                'additionalProperties': {
                                    'type': 'string',
                                    'minLength': 1,
                                    'maxLength': 63,
                                },
                            },
                        },
                    },
                },
                'purposes': binding_purposes_schema,
            },
        }, {
            'type': 'object',
            'required': ['kind', 'reference', 'purposes'],
            'additionalProperties': False,
            'properties': {
                'kind': {
                    'const': 'kubernetes_dockerconfig_secret',
                },
                'reference': {
                    'type': 'object',
                    'required': ['namespace', 'name', 'key'],
                    'additionalProperties': False,
                    'properties': {
                        field: {
                            'type': 'string',
                            'minLength': 1,
                            'maxLength': 253,
                        } for field in ('namespace', 'name', 'key')
                    },
                },
                'purposes': binding_purposes_schema,
            },
        }],
    }
    container_registries_schema = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'default_profile': {
                'type': 'string',
                'minLength': 1,
                'maxLength': 128,
            },
            'access_bindings': {
                'type': 'object',
                'maxProperties': 256,
                'additionalProperties': access_binding_schema,
            },
            'profiles': {
                'type': 'object',
                'maxProperties': 128,
                'additionalProperties': {
                    'type': 'object',
                    'required': [
                        'revision', 'ownership', 'provider', 'partition',
                        'registry_account', 'realm', 'limits', 'qualification',
                        'canonical', 'targets'
                    ],
                    'additionalProperties': False,
                    'properties': {
                        'revision': {
                            'type': 'integer',
                            'minimum': 1,
                        },
                        'ownership': {
                            'const': 'managed',
                        },
                        'provider': {
                            'const': 'aws',
                        },
                        'partition': {
                            'type': 'string',
                            'minLength': 1,
                            'maxLength': 64,
                        },
                        'registry_account': {
                            'type': 'string',
                            'pattern': '^[0-9]{12}$',
                        },
                        'realm': {
                            'type': 'string',
                            'minLength': 1,
                            'maxLength': 128,
                        },
                        'limits': {
                            'type': 'object',
                            'required': [
                                'max_artifact_bytes',
                                'max_releases_per_artifact',
                                'max_regional_locations_per_artifact'
                            ],
                            'additionalProperties': False,
                            'properties': {
                                field: {
                                    'type': 'integer',
                                    'minimum': 1,
                                } for field in (
                                    'max_artifact_bytes',
                                    'max_releases_per_artifact',
                                    'max_regional_locations_per_artifact')
                            },
                        },
                        'qualification': {
                            'type': 'object',
                            'required': [
                                'runtime_attestation_max_age_seconds',
                                'automatic_canaries',
                                'max_daily_canary_cost_usd',
                                'canary_worst_case_cost_usd',
                                'canary_timeout_seconds', 'canary_ref',
                                'canary_platform'
                            ],
                            'additionalProperties': False,
                            'properties': {
                                'runtime_attestation_max_age_seconds': {
                                    'type': 'integer',
                                    'minimum': 1,
                                },
                                'automatic_canaries': {
                                    'type': 'boolean',
                                },
                                'max_daily_canary_cost_usd': {
                                    'type': 'number',
                                    'minimum': 0,
                                },
                                'canary_worst_case_cost_usd': {
                                    'type': 'number',
                                    'exclusiveMinimum': 0,
                                },
                                'canary_timeout_seconds': {
                                    'type': 'integer',
                                    'minimum': 60,
                                    'maximum': 3600,
                                },
                                'canary_ref': {
                                    'type': 'string',
                                    'minLength': 1,
                                    'maxLength': 2048,
                                },
                                'canary_platform': {
                                    'const': 'linux/amd64',
                                },
                            },
                        },
                        'canonical': canonical_registry_target_schema,
                        'targets': {
                            'type': 'array',
                            'maxItems': 255,
                            'items': regional_registry_target_schema,
                        },
                    },
                },
            },
        },
    }

    def _get_controller_schema(
        extra_properties: dict[str, Any] | None = None,
        extra_controller_properties: dict[str, Any] | None = None,
    ):
        controller_properties = {
            'resources': resources_schema,
            'high_availability': {
                'type': 'boolean',
                'default': False,
            },
            'autostop': _AUTOSTOP_SCHEMA,
            'consolidation_mode': {
                'type': 'boolean',
                # When unset, automatically enabled for deploy-mode servers
                # (--deploy) if no existing controller clusters are found.
            },
            'controller_logs_gc_retention_hours': {
                'type': 'integer',
            },
            'task_logs_gc_retention_hours': {
                'type': 'integer',
            },
        }
        if extra_controller_properties:
            controller_properties.update(extra_controller_properties)
        props: dict[str, Any] = {
            'controller': {
                'type': 'object',
                'required': [],
                'additionalProperties': False,
                'properties': controller_properties,
            },
            'bucket': {
                'type': 'string',
                'pattern': '^(https|s3|gs|r2|cos)://.+',
                'required': [],
            },
            'force_disable_cloud_bucket': {
                'type': 'boolean',
                'default': False,
            },
        }
        if extra_properties:
            props.update(extra_properties)
        return {
            'type': 'object',
            'required': [],
            'additionalProperties': _allow_additional_properties(),
            'properties': props,
        }

    cloud_configs = {
        'aws': {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {
                'prioritize_reservations': {
                    'type': 'boolean',
                },
                'specific_reservations': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    },
                },
                'disk_encrypted': {
                    'type': 'boolean',
                },
                'ssh_user': {
                    'type': 'string',
                },
                'security_group_name':
                    (_PROPERTY_NAME_OR_CLUSTER_NAME_TO_PROPERTY),
                # Source CIDRs allowed to reach a cluster's SSH port and any
                # ports declared by `resources.ports`. Defaults to
                # ['0.0.0.0/0'], which is the historical behaviour: SkyPilot
                # opens requested ports to the whole internet. Narrow this to
                # the control plane's egress address when the workload behind
                # those ports has no authentication of its own.
                'ingress_source_ranges': {
                    'type': 'array',
                    'minItems': 1,
                    'items': {
                        'type': 'string',
                        'minLength': 1,
                    },
                },
                'vpc_name': {
                    'oneOf': [{
                        'type': 'string',
                    }, {
                        'type': 'null',
                    }]
                },
                'vpc_names': {
                    'oneOf': [{
                        'type': 'string',
                    }, {
                        'type': 'null',
                    }, {
                        'type': 'array',
                        'items': {
                            'type': 'string'
                        }
                    }],
                },
                'subnet_names': {
                    'oneOf': [{
                        'type': 'string',
                    }, {
                        'type': 'null',
                    }, {
                        'type': 'array',
                        'items': {
                            'type': 'string'
                        }
                    }],
                },
                'use_ssm': {
                    'type': 'boolean',
                },
                'ssm_profile': {
                    'type': 'string',
                },
                'ssm_direct_fallback': {
                    'type': 'boolean',
                },
                'post_provision_runcmd': {
                    'type': 'array',
                    'items': {
                        'oneOf': [{
                            'type': 'string'
                        }, {
                            'type': 'array',
                            'items': {
                                'type': 'string'
                            }
                        }]
                    },
                },
                **_CAPABILITIES_SCHEMA,
                **_LABELS_SCHEMA,
                **_NETWORK_CONFIG_SCHEMA,
            },
            **_check_not_both_fields_present('instance_tags', 'labels')
        },
        'gcp': {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {
                'prioritize_reservations': {
                    'type': 'boolean',
                },
                'specific_reservations': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    },
                },
                'managed_instance_group': {
                    'type': 'object',
                    'required': ['run_duration'],
                    'additionalProperties': False,
                    'properties': {
                        'run_duration': {
                            'type': 'integer',
                        },
                        'provision_timeout': {
                            'type': 'integer',
                        }
                    }
                },
                'force_enable_external_ips': {
                    'type': 'boolean'
                },
                'enable_gvnic': {
                    'type': 'boolean'
                },
                'enable_gpu_direct': {
                    'type': 'boolean'
                },
                'placement_policy': {
                    'type': 'string',
                },
                'vpc_name': {
                    'oneOf': [
                        {
                            'type': 'string',
                            # vpc-name or project-id/vpc-name
                            # VPC name and Project ID have -, a-z, and 0-9.
                            'pattern': '^(?:[-a-z0-9]+/)?[-a-z0-9]+$'
                        },
                        {
                            'type': 'null',
                        }
                    ],
                },
                'subnet_names': {
                    'oneOf': [{
                        'type': 'string',
                    }, {
                        'type': 'null',
                    }, {
                        'type': 'array',
                        'items': {
                            'type': 'string'
                        }
                    }],
                },
                **_CAPABILITIES_SCHEMA,
                **_LABELS_SCHEMA,
                **_NETWORK_CONFIG_SCHEMA,
            },
            **_check_not_both_fields_present('instance_tags', 'labels')
        },
        'azure': {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {
                'storage_account': {
                    'type': 'string',
                },
                'resource_group_vm': {
                    'type': 'string',
                },
                'vpc_name': {
                    'oneOf': [{
                        'type': 'string',
                    }, {
                        'type': 'null',
                    }]
                },
                **_LABELS_SCHEMA,
                **_CAPABILITIES_SCHEMA,
                **_NETWORK_CONFIG_SCHEMA,
            },
            **_check_not_both_fields_present('instance_tags', 'labels')
        },
        'kubernetes': {
            'type': 'object',
            'required': [],
            # On the server, plugins have registered
            # their properties via
            # register_kubernetes_property(), so we
            # can be strict. On the client we allow
            # unknown properties to pass through for
            # server-side validation.
            'additionalProperties': _allow_additional_properties(),
            'properties': {
                'allowed_contexts': {
                    'oneOf': [{
                        'type': 'array',
                        'items': {
                            'type': 'string',
                        },
                    }, {
                        'type': 'string',
                        'pattern': '^all$'
                    }]
                },
                'serve_controller_workspace': _SERVE_CONTROLLER_WORKSPACE_SCHEMA,
                'serve_controller_context': _SERVE_CONTROLLER_CONTEXT_SCHEMA,
                'context_configs': {
                    'type': 'object',
                    'required': [],
                    'properties': {},
                    # Properties are kubernetes context names.
                    'additionalProperties': {
                        'type': 'object',
                        'required': [],
                        # On the server, plugins have registered
                        # their properties via
                        # register_kubernetes_property(), so we
                        # can be strict. On the client we allow
                        # unknown properties to pass through for
                        # server-side validation.
                        'additionalProperties': _allow_additional_properties(),
                        'properties': {
                            **_CONTEXT_CONFIG_SCHEMA_KUBERNETES,
                            **_extra_kubernetes_properties,
                        },
                    },
                },
                **_CONTEXT_CONFIG_SCHEMA_KUBERNETES,
                **_extra_kubernetes_properties,
            }
        },
        'ssh': {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {
                'allowed_node_pools': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    },
                },
                'context_configs': {
                    'type': 'object',
                    'required': [],
                    'properties': {},
                    # Properties are ssh cluster names, which are the
                    # kubernetes context names without `ssh-` prefix.
                    'additionalProperties': {
                        'type': 'object',
                        'required': [],
                        'additionalProperties': False,
                        'properties': {
                            **_CONTEXT_CONFIG_SCHEMA_MINIMAL,
                        },
                    },
                },
                **_CONTEXT_CONFIG_SCHEMA_MINIMAL,
            }
        },
        'slurm': {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {
                'allowed_clusters': {
                    'oneOf': [{
                        'type': 'array',
                        'items': {
                            'type': 'string',
                        },
                    }, {
                        'type': 'string',
                        'pattern': '^all$'
                    }]
                },
                'provision_timeout': {
                    'type': 'integer',
                },
                'enable_ports': {
                    'type': 'boolean',
                },
                'pricing': _PRICING_SCHEMA,
                'sbatch_options': _SBATCH_OPTIONS_SCHEMA,
                'gpu_partition_map': _GPU_PARTITION_MAP_SCHEMA,
                'cpu_partition': {
                    'type': 'string',
                },
                'cluster_configs': {
                    'type': 'object',
                    'required': [],
                    'properties': {},
                    'additionalProperties': {
                        'type': 'object',
                        'required': [],
                        'additionalProperties': False,
                        'properties': {
                            'workdir': {
                                'type': 'string',
                            },
                            'tmpdir': {
                                'type': 'string',
                            },
                            'enable_ports': {
                                'type': 'boolean',
                            },
                            'pricing': _PRICING_SCHEMA,
                            'sbatch_options': _SBATCH_OPTIONS_SCHEMA,
                            'gpu_partition_map': _GPU_PARTITION_MAP_SCHEMA,
                            'cpu_partition': {
                                'type': 'string',
                            },
                            'partition_configs': {
                                'type': 'object',
                                'required': [],
                                'properties': {},
                                'additionalProperties': {
                                    'type': 'object',
                                    'required': [],
                                    'additionalProperties': False,
                                    'properties': {
                                        'pricing': _PRICING_SCHEMA,
                                        'sbatch_options': _SBATCH_OPTIONS_SCHEMA,  # pylint: disable=line-too-long
                                    },
                                },
                            },
                        },
                    },
                },
            }
        },
        'oci': {
            'type': 'object',
            'required': [],
            'properties': {
                'region_configs': {
                    'type': 'object',
                    'required': [],
                    'properties': {},
                    # Properties are either 'default' or a region
                    # name.
                    'additionalProperties': {
                        'type': 'object',
                        'required': [],
                        'additionalProperties': False,
                        'properties': {
                            'compartment_ocid': {
                                'type': 'string',
                            },
                            'image_tag_general': {
                                'type': 'string',
                            },
                            'image_tag_gpu': {
                                'type': 'string',
                            },
                            'vcn_ocid': {
                                'type': 'string',
                            },
                            'vcn_subnet': {
                                'type': 'string',
                            },
                        }
                    },
                }
            },
        },
        'vast': {
            'type': 'object',
            'required': [],
            'additionalProperties': False,
            'properties': {
                'datacenter_only': {
                    'type': 'boolean',
                },
                'create_instance_kwargs': {
                    'type': 'object',
                },
            }
        },
        'nebius': {
            'type': 'object',
            'required': [],
            'properties': {
                **_NETWORK_CONFIG_SCHEMA, 'use_static_ip_address': {
                    'type': 'boolean',
                },
                'tenant_id': {
                    'type': 'string',
                },
                'domain': {
                    'type': 'string',
                },
                'security_group_name':
                    (_PROPERTY_NAME_OR_CLUSTER_NAME_TO_PROPERTY),
                'region_configs': {
                    'type': 'object',
                    'required': [],
                    'properties': {},
                    'additionalProperties': {
                        'type': 'object',
                        'required': [],
                        'additionalProperties': False,
                        'properties': {
                            'project_id': {
                                'type': 'string',
                            },
                            'subnet_id': {
                                'type': 'string',
                            },
                            'fabric': {
                                'type': 'string',
                            },
                            'filesystems': {
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'additionalProperties': False,
                                    'properties': {
                                        'filesystem_id': {
                                            'type': 'string',
                                        },
                                        'attach_mode': {
                                            'type': 'string',
                                            'case_sensitive_enum': [
                                                'READ_WRITE', 'READ_ONLY'
                                            ]
                                        },
                                        'mount_path': {
                                            'type': 'string',
                                        }
                                    }
                                }
                            },
                        },
                    }
                }
            },
        }
    }

    admin_policy_schema = {
        'type': 'string',
        'anyOf': [
            {
                # Check regex to be a valid python module path
                'pattern': (r'^[a-zA-Z_][a-zA-Z0-9_]*'
                            r'(\.[a-zA-Z_][a-zA-Z0-9_]*)+$'),
            },
            {
                # Check for valid HTTP/HTTPS URL
                'pattern': r'^https?://.*$',
            }
        ]
    }

    allowed_clouds = {
        # A list of cloud names that are allowed to be used
        'type': 'array',
        'items': {
            'type': 'string',
            'case_insensitive_enum':
                (list(constants.ALL_CLOUDS) + constants.STORAGE_ONLY_CLOUDS)
        }
    }

    docker_configs = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'run_options': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    }
                }]
            }
        }
    }
    gpu_configs = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'disable_ecc': {
                'type': 'boolean',
            },
        }
    }

    daemon_config = {
        'type': 'object',
        'required': [],
        'properties': {
            'log_level': {
                'type': 'string',
                'case_insensitive_enum': ['DEBUG', 'INFO', 'WARNING'],
            },
            # Only honored by daemons that opt in to reading this; see the
            # per-daemon event functions in sky/server/daemons.py for support.
            'interval_seconds': {
                'type': 'integer',
                'minimum': 1,
            },
        }
    }

    daemon_schema: dict[str, Any] = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {}
    }

    for daemon in daemons.RUNTIME_DAEMONS:
        daemon_schema['properties'][daemon.id] = daemon_config

    api_server = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'endpoint': {
                'type': 'string',
                # Apply validation for URL
                'pattern': r'^https?://.*$',
            },
            'service_account_token': {
                'anyOf': [
                    {
                        'type': 'string',
                        # Validate that token starts with sky_ prefix
                        'pattern': r'^sky_.+$',
                    },
                    {
                        'type': 'null',
                    }
                ]
            },
            'requests_retention_hours': {
                'type': 'integer',
            },
            'cluster_event_retention_hours': {
                'type': 'number',
            },
            'cluster_debug_event_retention_hours': {
                'type': 'number',
            },
            'cluster_terminal_event_retention_hours': {
                'type': 'number',
            },
            'operational_event_retention_hours': {
                'type': 'number',
            },
            'daemon_log_max_bytes': {
                'type': 'integer',
                'minimum': 0,
            },
        }
    }

    rbac_schema = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'default_role': {
                'type': 'string',
                'case_insensitive_enum': ['admin', 'user', 'viewer']
            },
            # Per-role permission overrides. Schema is intentionally
            # permissive (additionalProperties: True on
            # `permissions`) because admin/user use `blocklist`
            # entries while `viewer` uses `allowlist`; both shapes
            # are `[{path, method}, ...]`.
            'roles': {
                'type': 'object',
                'additionalProperties': {
                    'type': 'object',
                    'properties': {
                        'permissions': {
                            'type': 'object',
                            'additionalProperties': True,
                        },
                    },
                },
            },
        },
    }

    workspace_schema = {'type': 'string'}

    allowed_workspace_cloud_names = list(
        constants.ALL_CLOUDS) + constants.STORAGE_ONLY_CLOUDS
    # Create pattern for not supported clouds, i.e.
    # all clouds except aws, gcp, kubernetes, ssh, nebius
    not_supported_clouds = [
        cloud for cloud in allowed_workspace_cloud_names
        if cloud.lower() not in ['aws', 'gcp', 'kubernetes', 'ssh', 'nebius']
    ]
    not_supported_cloud_regex = '|'.join(not_supported_clouds)
    workspaces_schema = {
        'type': 'object',
        'required': [],
        # each key is a workspace name
        'additionalProperties': {
            'type': 'object',
            'additionalProperties': False,
            'patternProperties': {
                # Pattern for clouds with no workspace-specific config -
                # only allow 'disabled' property.
                f'^({not_supported_cloud_regex})$': {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'disabled': {
                            'type': 'boolean'
                        }
                    },
                },
            },
            'properties': {
                # Explicit definition for GCP allows both project_id and
                # disabled
                'private': {
                    'type': 'boolean',
                },
                'allowed_users': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    },
                },
                'container_images': {
                    'type': 'object',
                    'required': [],
                    'additionalProperties': False,
                    'properties': {
                        'mode': {
                            'type': 'string',
                            'enum': [
                                'direct', 'managed_required',
                                'managed_preferred'
                            ],
                        },
                        'default_profile': {
                            'type': 'string',
                            'minLength': 1,
                        },
                        'allowed_profiles': {
                            'type': 'array',
                            'maxItems': 128,
                            'items': {
                                'type': 'string',
                                'minLength': 1,
                            },
                            'uniqueItems': True,
                        },
                        'publishers': {
                            'type': 'array',
                            'maxItems': 256,
                            'items': {
                                'type': 'string',
                                'minLength': 1,
                                'maxLength': 256,
                                'pattern': '^\\S+$',
                            },
                            'uniqueItems': True,
                        },
                        'locality': {
                            'type': 'string',
                            'enum': ['prefer', 'require', 'canonical'],
                        },
                        'regional_cache_retention_weeks': {
                            'anyOf': [{
                                'type': 'integer',
                                'minimum': 1,
                            }, {
                                'type': 'null',
                            }],
                            'default': 8,
                        },
                    },
                },
                'gcp': {
                    'type': 'object',
                    'properties': {
                        'project_id': {
                            'type': 'string'
                        },
                        'disabled': {
                            'type': 'boolean'
                        },
                        **_CAPABILITIES_SCHEMA,
                        **_REMOTE_IDENTITY_SCHEMA,
                    },
                    'additionalProperties': False,
                },
                'aws': {
                    'type': 'object',
                    'properties': {
                        'profile': {
                            'type': 'string'
                        },
                        'disabled': {
                            'type': 'boolean'
                        },
                        **_CAPABILITIES_SCHEMA,
                        'remote_identity':
                            (_PROPERTY_NAME_OR_CLUSTER_NAME_TO_PROPERTY),
                    },
                    'additionalProperties': False,
                },
                'ssh': {
                    'type': 'object',
                    'required': [],
                    'properties': {
                        'allowed_node_pools': {
                            'type': 'array',
                            'items': {
                                'type': 'string',
                            },
                        },
                        'disabled': {
                            'type': 'boolean'
                        },
                    },
                    'additionalProperties': False,
                },
                'kubernetes': {
                    'type': 'object',
                    'required': [],
                    'properties': {
                        'allowed_contexts': {
                            'oneOf': [{
                                'type': 'array',
                                'items': {
                                    'type': 'string',
                                },
                            }, {
                                'type': 'string',
                                'pattern': '^all$'
                            }]
                        },
                        'disabled': {
                            'type': 'boolean'
                        },
                        'serve_controller_workspace': _SERVE_CONTROLLER_WORKSPACE_SCHEMA,
                        'serve_controller_context': _SERVE_CONTROLLER_CONTEXT_SCHEMA,
                        # Workspace Kubernetes overrides use the same regional
                        # property contract as the global Kubernetes config.
                        # Keep this as the single source of truth so new
                        # properties cannot validate globally but fail when
                        # scoped to a workspace.
                        **_CONTEXT_CONFIG_SCHEMA_KUBERNETES,
                        'context_configs': {
                            'type': 'object',
                            'required': [],
                            'properties': {},
                            # Properties are kubernetes context names.
                            'additionalProperties': {
                                'type': 'object',
                                'required': [],
                                # On the server, plugins have registered
                                # their properties via
                                # register_kubernetes_property(), so we
                                # can be strict. On the client we allow
                                # unknown properties to pass through for
                                # server-side validation.
                                'additionalProperties':
                                    _allow_additional_properties(),
                                'properties': {
                                    **_CONTEXT_CONFIG_SCHEMA_KUBERNETES,
                                    **_extra_kubernetes_properties,
                                    **_REMOTE_IDENTITY_SCHEMA_KUBERNETES,
                                },
                            },
                        },
                        **_extra_kubernetes_properties,
                        **_REMOTE_IDENTITY_SCHEMA_KUBERNETES,
                    },
                    # On the server, plugins have registered
                    # their properties via
                    # register_kubernetes_property(), so we
                    # can be strict. On the client we allow
                    # unknown properties to pass through for
                    # server-side validation.
                    'additionalProperties': _allow_additional_properties(),
                },
                'nebius': {
                    'type': 'object',
                    'required': [],
                    'properties': {
                        'credentials_file_path': {
                            'type': 'string',
                        },
                        'tenant_id': {
                            'type': 'string',
                        },
                        'domain': {
                            'type': 'string',
                        },
                        'disabled': {
                            'type': 'boolean'
                        },
                    },
                    'additionalProperties': False,
                },
            },
        },
    }

    provision_configs = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'ssh_timeout': {
                'type': 'integer',
                'minimum': 1,
            },
            'install_conda': {
                'type': 'boolean',
            },
            # Enabled by default. Set to false to stop GCP from writing or
            # reading capacity and quota hints, returning provisioning to its
            # pre-cache behavior.
            'gcp_capacity_cache': {
                'type': 'boolean',
            },
        }
    }

    logs_schema = {
        'type': 'object',
        'required': ['store'],
        'additionalProperties': False,
        'properties': {
            'store': {
                'type': 'string',
                'case_insensitive_enum': ['gcp', 'aws'],
            },
            'gcp': {
                'type': 'object',
                'properties': {
                    'project_id': {
                        'type': 'string',
                    },
                    'credentials_file': {
                        'type': 'string',
                    },
                    'additional_labels': {
                        'type': 'object',
                        'additionalProperties': {
                            'type': 'string',
                        },
                    },
                },
            },
            'aws': {
                'type': 'object',
                'properties': {
                    'region': {
                        'type': 'string',
                    },
                    'credentials_file': {
                        'type': 'string',
                    },
                    'log_group_name': {
                        'type': 'string',
                    },
                    'log_stream_prefix': {
                        'type': 'string',
                    },
                    'auto_create_group': {
                        'type': 'boolean',
                    },
                    'additional_tags': {
                        'type': 'object',
                        'additionalProperties': {
                            'type': 'string',
                        },
                    },
                },
            },
        },
    }

    for cloud, config in cloud_configs.items():
        if cloud in ('aws', 'azure'):
            config['properties'].update(
                {'remote_identity': _PROPERTY_NAME_OR_CLUSTER_NAME_TO_PROPERTY})
        elif cloud == 'kubernetes':
            config['properties'].update(_REMOTE_IDENTITY_SCHEMA_KUBERNETES)
        else:
            config['properties'].update(_REMOTE_IDENTITY_SCHEMA)

    # TODO (kyuds): deprecated; remove v0.13.0
    data_schema = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'mount_cached': {
                'type': 'object',
                'required': [],
                'additionalProperties': False,
                'properties': {
                    'sequential_upload': {
                        'type': 'boolean',
                    },
                },
            },
        },
    }

    dashboard_schema = {
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            'external_links': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'required': ['label', 'regex'],
                    'additionalProperties': False,
                    'properties': {
                        'label': {
                            'type': 'string',
                            'minLength': 1,
                        },
                        'regex': {
                            'type': 'string',
                            'minLength': 1,
                        },
                    },
                },
            },
        },
    }

    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': [],
        'additionalProperties': False,
        'properties': {
            # TODO Replace this with whatever syang cooks up
            'workspace': {
                'type': 'string',
            },
            'db': {
                'type': 'string',
            },
            'jobs': _get_controller_schema(
                extra_properties=_extra_jobs_properties,),
            'serve': _get_controller_schema(
                extra_controller_properties={
                    # Deprecated compatibility input. Runtime capability comes
                    # exclusively from SKYPILOT_SERVE_EXTERNAL_LB_ENABLED; keep
                    # accepting old server configs during rolling upgrades.
                    'external_load_balancer': {
                        'type': 'boolean',
                        'default': False,
                    },
                }),
            'allowed_clouds': allowed_clouds,
            'admin_policy': admin_policy_schema,
            'docker': docker_configs,
            'nvidia_gpus': gpu_configs,
            'api_server': api_server,
            'active_workspace': workspace_schema,
            'workspaces': workspaces_schema,
            'container_registries': container_registries_schema,
            'provision': provision_configs,
            'rbac': rbac_schema,
            'logs': logs_schema,
            'daemons': daemon_schema,
            'data': data_schema,
            'dashboard': dashboard_schema,
            **cloud_configs,
            # For plugin-specific config.
            'plugins': {
                'type': 'object',
                'required': [],
                # Allow unknown properties since a plugin can be turned off
                # and the previously valid config should not block server
                # from reading the config file.
                'additionalProperties': True,
                'properties': _extra_plugin_properties,
            },
        },
    }
