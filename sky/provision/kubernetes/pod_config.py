"""Kubernetes pod configuration validation and composition."""

import copy
import datetime
import re
import typing
from typing import Any

from sky import clouds
from sky import exceptions
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.utils import config_utils

if typing.TYPE_CHECKING:
    from dateutil import parser as dateutil_parser
    from kubernetes.client import models as kubernetes_models
else:
    dateutil_parser = adaptors_common.LazyImport('dateutil.parser')
    kubernetes_models = adaptors_common.LazyImport('kubernetes.client.models')


class PodValidator:
    """Validates Kubernetes pod configs against the OpenAPI spec.

    Adapted from kubernetes.client.ApiClient:
    https://github.com/kubernetes-client/python/blob/0c56ef1c8c4b50087bc7b803f6af896fb973309e/kubernetes/client/api_client.py#L33

    We needed to adapt it because the original implementation ignores
    unknown fields, whereas we want to raise an error so that users
    are aware of the issue.
    """
    PRIMITIVE_TYPES = (int, float, bool, str)
    NATIVE_TYPES_MAPPING = {
        'int': int,
        'float': float,
        'str': str,
        'bool': bool,
        'date': datetime.date,
        'datetime': datetime.datetime,
        'object': object,
    }

    @classmethod
    def validate(cls, data):
        return cls.__validate(data, kubernetes_models.V1Pod)

    @classmethod
    def __validate(cls, data, klass):
        """Deserializes dict, list, str into an object.

        :param data: dict, list or str.
        :param klass: class literal, or string of class name.

        :return: object.
        """
        if data is None:
            return None

        if isinstance(klass, str):
            if klass.startswith('list['):
                match = re.match(r'list\[(.*)\]', klass)
                if match is None:
                    raise ValueError(f'Invalid list type format: {klass}')
                sub_kls = match.group(1)
                return [cls.__validate(sub_data, sub_kls) for sub_data in data]

            if klass.startswith('dict('):
                match = re.match(r'dict\(([^,]*), (.*)\)', klass)
                if match is None:
                    raise ValueError(f'Invalid dict type format: {klass}')
                sub_kls = match.group(2)
                return {k: cls.__validate(v, sub_kls) for k, v in data.items()}

            # convert str to class
            if klass in cls.NATIVE_TYPES_MAPPING:
                klass = cls.NATIVE_TYPES_MAPPING[klass]
            else:
                klass = getattr(kubernetes_models, klass)

        if klass in cls.PRIMITIVE_TYPES:
            return cls.__validate_primitive(data, klass)
        elif klass == object:
            return cls.__validate_object(data)
        elif klass == datetime.date:
            return cls.__validate_date(data)
        elif klass == datetime.datetime:
            return cls.__validate_datetime(data)
        else:
            return cls.__validate_model(data, klass)

    @classmethod
    def __validate_primitive(cls, data, klass):
        """Deserializes string to primitive type.

        :param data: str.
        :param klass: class literal.

        :return: int, long, float, str, bool.
        """
        try:
            return klass(data)
        except UnicodeEncodeError:
            return str(data)
        except TypeError:
            return data

    @classmethod
    def __validate_object(cls, value):
        """Return an original value.

        :return: object.
        """
        return value

    @classmethod
    def __validate_date(cls, string):
        """Deserializes string to date.

        :param string: str.
        :return: date.
        """
        try:
            return dateutil_parser.parse(string).date()
        except ValueError as exc:
            raise ValueError(
                f'Failed to parse `{string}` as date object') from exc

    @classmethod
    def __validate_datetime(cls, string):
        """Deserializes string to datetime.

        The string should be in iso8601 datetime format.

        :param string: str.
        :return: datetime.
        """
        try:
            return dateutil_parser.parse(string)
        except ValueError as exc:
            raise ValueError(
                f'Failed to parse `{string}` as datetime object') from exc

    @classmethod
    def __validate_model(cls, data, klass):
        """Deserializes list or dict to model.

        :param data: dict, list.
        :param klass: class literal.
        :return: model object.
        """

        if not klass.openapi_types and not hasattr(klass,
                                                   'get_real_child_model'):
            return data

        kwargs = {}
        try:
            if (data is not None and klass.openapi_types is not None and
                    isinstance(data, (list, dict))):
                # attribute_map is a dict that maps field names in snake_case
                # to camelCase.
                reverse_attribute_map = {
                    v: k for k, v in klass.attribute_map.items()
                }
                for k, v in data.items():
                    field_name = reverse_attribute_map.get(k, None)
                    if field_name is None:
                        raise ValueError(
                            f'Unknown field `{k}`. Please ensure '
                            'pod_config follows the Kubernetes '
                            'Pod schema: '
                            'https://github.com/kubernetes/kubernetes/blob/master/api/openapi-spec/v3/api__v1_openapi.json'
                        )
                    kwargs[field_name] = cls.__validate(
                        v, klass.openapi_types[field_name])
        except exceptions.KubernetesValidationError as e:
            raise exceptions.KubernetesValidationError([k] + e.path,
                                                       str(e)) from e
        except Exception as e:
            raise exceptions.KubernetesValidationError([k], str(e)) from e

        instance = klass(**kwargs)

        if hasattr(instance, 'get_real_child_model'):
            klass_name = instance.get_real_child_model(data)
            if klass_name:
                instance = cls.__validate(data, klass_name)
        return instance


def check_pod_config(pod_config: dict) -> tuple[bool, str | None]:
    """Check if the pod_config is a valid pod config.

    Uses the deserialize API from the kubernetes client library.

    This is a client-side validation, meant to catch common errors like
    unknown/misspelled fields, and missing required fields.

    The full validation however is done later on by the Kubernetes API server
    when the pod creation request is sent.

    Returns:
        bool: True if pod_config is valid.
        str: Error message about why the pod_config is invalid, None otherwise.
    """
    try:
        PodValidator.validate(pod_config)
    except exceptions.KubernetesValidationError as e:
        return False, f'Validation error in {".".join(e.path)}: {str(e)}'
    except Exception as e:  # pylint: disable=broad-except
        return False, f'Unexpected error: {str(e)}'
    return True, None


def resolve_effective_pod_config(
    cluster_config_overrides: dict[str, Any],
    cloud: clouds.Cloud | None = None,
    context: str | None = None,
) -> dict[str, Any]:
    """Resolves the effective ``kubernetes.pod_config`` (global + overrides).

    This is the same pod_config that combine_pod_config_fields() folds into
    the rendered cluster YAML. make_deploy_resources_variables() needs it
    before the template is rendered (to detect ``hostNetwork``), so both
    resolve it here to stay in agreement on the SSH cloud/context handling.
    """
    # We don't use override_configs in `get_effective_region_config`, as
    # merging the pod config requires special handling.
    cloud_str = 'ssh' if isinstance(cloud, clouds.SSH) else 'kubernetes'
    context_str = context
    if isinstance(cloud, clouds.SSH) and context is not None:
        assert context.startswith('ssh-'), 'SSH context must start with "ssh-"'
        context_str = context[len('ssh-'):]
    kubernetes_config = skypilot_config.get_effective_region_config(
        cloud=cloud_str,
        region=context_str,
        keys=('pod_config',),
        default_value={})
    override_pod_config = config_utils.get_cloud_config_value_from_dict(
        dict_config=cluster_config_overrides,
        cloud=cloud_str,
        region=context_str,
        keys=('pod_config',),
        default_value={})
    config_utils.merge_k8s_configs(kubernetes_config, override_pod_config)
    return kubernetes_config


def combine_pod_config_fields(
    cluster_yaml_obj: dict[str, Any],
    cluster_config_overrides: dict[str, Any],
    cloud: clouds.Cloud | None = None,
    context: str | None = None,
) -> dict[str, Any]:
    """Adds or updates fields in the YAML with configured pod fields.

    Nested objects are merged and configured lists are appended to the
    destination list. The input cluster YAML is not mutated.
    """
    merged_cluster_yaml_obj = copy.deepcopy(cluster_yaml_obj)
    kubernetes_config = resolve_effective_pod_config(cluster_config_overrides,
                                                     cloud, context)

    # Merge the kubernetes config into the YAML for both head and worker nodes.
    config_utils.merge_k8s_configs(
        merged_cluster_yaml_obj['available_node_types']['ray_head_default']
        ['node_config'], kubernetes_config)
    return merged_cluster_yaml_obj


def combine_metadata_fields(cluster_yaml_obj: dict[str, Any],
                            cluster_config_overrides: dict[str, Any],
                            context: str | None = None) -> dict[str, Any]:
    """Apply configured metadata to all Kubernetes objects SkyPilot creates."""
    merged_cluster_yaml_obj = copy.deepcopy(cluster_yaml_obj)
    context, cloud_str = get_cleaned_context_and_cloud_str(context)

    custom_metadata = skypilot_config.get_effective_region_config(
        cloud=cloud_str,
        region=context,
        keys=('custom_metadata',),
        default_value={})
    override_custom_metadata = config_utils.get_cloud_config_value_from_dict(
        dict_config=cluster_config_overrides,
        cloud=cloud_str,
        region=context,
        keys=('custom_metadata',),
        default_value={})
    config_utils.merge_k8s_configs(custom_metadata, override_custom_metadata)

    combination_destinations = [
        merged_cluster_yaml_obj['provider']['autoscaler_service_account']
        ['metadata'],
        merged_cluster_yaml_obj['provider']['autoscaler_role']['metadata'],
        merged_cluster_yaml_obj['provider']['autoscaler_role_binding']
        ['metadata'], merged_cluster_yaml_obj['provider']
        ['autoscaler_service_account']['metadata'],
        merged_cluster_yaml_obj['available_node_types']['ray_head_default']
        ['node_config']['metadata'], *[
            svc['metadata']
            for svc in merged_cluster_yaml_obj['provider']['services']
        ]
    ]

    for destination in combination_destinations:
        config_utils.merge_k8s_configs(destination, custom_metadata)

    return merged_cluster_yaml_obj


def combine_pod_config_fields_and_metadata(
        cluster_yaml_obj: dict[str, Any],
        cluster_config_overrides: dict[str, Any],
        cloud: clouds.Cloud | None = None,
        context: str | None = None) -> dict[str, Any]:
    """Combine configured pod fields and metadata into cluster YAML."""
    combined_yaml_obj = combine_pod_config_fields(cluster_yaml_obj,
                                                  cluster_config_overrides,
                                                  cloud, context)
    combined_yaml_obj = combine_metadata_fields(combined_yaml_obj,
                                                cluster_config_overrides,
                                                context)
    return combined_yaml_obj


def merge_custom_metadata(
        original_metadata: dict[str, Any],
        context: str | None = None,
        cluster_config_overrides: dict[str, Any] | None = None) -> None:
    """Merge configured custom metadata into the supplied mapping in place."""
    context, cloud_str = get_cleaned_context_and_cloud_str(context)

    custom_metadata = skypilot_config.get_effective_region_config(
        cloud=cloud_str,
        region=context,
        keys=('custom_metadata',),
        default_value={})

    if cluster_config_overrides is not None:
        override_custom_metadata = config_utils.get_cloud_config_value_from_dict(
            dict_config=cluster_config_overrides,
            cloud=cloud_str,
            region=context,
            keys=('custom_metadata',),
            default_value={})
        config_utils.merge_k8s_configs(custom_metadata,
                                       override_custom_metadata)

    config_utils.merge_k8s_configs(original_metadata, custom_metadata)


def get_cleaned_context_and_cloud_str(
        context: str | None) -> tuple[str | None, str]:
    """Return the cleaned context and relevant cloud string from a context."""
    cloud_str = 'kubernetes'
    if context is not None and context.startswith('ssh-'):
        cloud_str = 'ssh'
        context = context[len('ssh-'):]
    return context, cloud_str
