"""Tests for instance type in Kubernetes.

Tests verify correct instance type parsing and formatting.
"""
import pickle

import pytest

from sky.clouds import kubernetes as kubernetes_cloud
from sky.provision.kubernetes import instance_type as instance_type_lib
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.provision.kubernetes.utils import KubernetesInstanceType


# Unit test for KubernetesInstanceType
def test_kubernetes_instance_type():
    test_cases = [
        (4, 16, None, None, "4CPU--16GB"),
        (0.5, 1.5, None, None, "0.5CPU--1.5GB"),
        (4, 16, 1, "V100", "4CPU--16GB--V100:1"),
        (4, 16, 2, "Atx100", "4CPU--16GB--Atx100:2"),
        (4, 16, 4, "4090", "4CPU--16GB--4090:4"),
        (4, 16, 1, "H100-80GB", "4CPU--16GB--H100-80GB:1"),
        (1, 6, 1, "K80", "1CPU--6GB--K80:1"),
        # Test underscore-based GPU names (CoreWeave format)
        (2, 8, 1, "H100_NVLINK_80GB", "2CPU--8GB--H100_NVLINK_80GB:1"),
        (8, 32, 4, "A100_SXM4_80GB", "8CPU--32GB--A100_SXM4_80GB:4"),
    ]

    for cpus, memory, accelerator_count, accelerator_type, expected in test_cases:
        instance_type = KubernetesInstanceType(
            cpus=cpus,
            memory=memory,
            accelerator_count=accelerator_count,
            accelerator_type=accelerator_type)

        assert instance_type.name == expected, f'Failed name check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'

        assert KubernetesInstanceType.is_valid_instance_type(
            instance_type.name
        ), f'Failed valid instance type check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'

        cpus, memory, accelerator_count, accelerator_type = KubernetesInstanceType._parse_instance_type(
            instance_type.name)
        assert cpus == cpus, f'Failed parse check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
        assert memory == memory, f'Failed parse check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
        assert accelerator_count == accelerator_count, f'Failed parse check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
        assert accelerator_type == accelerator_type, f'Failed parse check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'

        instance_type_from_name = KubernetesInstanceType.from_instance_type(
            instance_type.name)
        assert instance_type_from_name.cpus == cpus, f'Failed from instance type check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
        assert instance_type_from_name.memory == memory, f'Failed from instance type check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
        assert instance_type_from_name.accelerator_count == accelerator_count, f'Failed from instance type check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
        assert instance_type_from_name.accelerator_type == accelerator_type, f'Failed from instance type check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'

        if accelerator_count is not None:
            instance_type_from_resources = KubernetesInstanceType.from_resources(
                cpus, memory, accelerator_count, accelerator_type)
            assert instance_type_from_resources.cpus == cpus, f'Failed from resources check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
            assert instance_type_from_resources.memory == memory, f'Failed from resources check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
            assert instance_type_from_resources.accelerator_count == accelerator_count, f'Failed from resources check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'
            assert instance_type_from_resources.accelerator_type == accelerator_type, f'Failed from resources check for {cpus}, {memory}, {accelerator_count}, {accelerator_type}'


def test_gpu_name_underscore_preservation():
    """Test that GPU names with underscores are preserved exactly as-is.

    This specifically tests the fix for CoreWeave H100_NVLINK_80GB support
    where underscores were incorrectly converted to spaces during parsing.
    """
    test_cases = [
        # (accelerator_name, expected_preserved_name)
        ("H100_NVLINK_80GB", "H100_NVLINK_80GB"),
        ("A100_SXM4_80GB", "A100_SXM4_80GB"),
        ("RTX4090", "RTX4090"),
        # Also test hyphen-based names continue to work
        ("H100-80GB", "H100-80GB"),
        ("A100-40GB", "A100-40GB"),
    ]

    for original_name, expected_name in test_cases:
        # Create instance type with accelerator name
        instance = KubernetesInstanceType.from_resources(
            cpus=4,
            memory=16,
            accelerator_count=1,
            accelerator_type=original_name)

        # Parse it back from the instance type string
        parsed_instance = KubernetesInstanceType.from_instance_type(
            instance.name)

        # Verify the accelerator name is preserved exactly
        assert parsed_instance.accelerator_type == expected_name, (
            f"Expected accelerator name '{expected_name}' but got "
            f"'{parsed_instance.accelerator_type}' after round-trip parsing")

        # Verify the full instance type string contains the original name
        assert original_name in instance.name, (
            f"Instance type string '{instance.name}' should contain "
            f"original accelerator name '{original_name}'")


def test_instance_type_facade_and_pickle_identity():
    assert KubernetesInstanceType is instance_type_lib.KubernetesInstanceType
    assert KubernetesInstanceType is kubernetes_utils.KubernetesInstanceType
    assert KubernetesInstanceType.__module__ == (
        'sky.provision.kubernetes.utils')

    instance_type = KubernetesInstanceType.from_resources(cpus=2,
                                                          memory=8,
                                                          accelerator_count=1,
                                                          accelerator_type='L4')
    restored = pickle.loads(pickle.dumps(instance_type))

    assert type(restored) is KubernetesInstanceType
    assert restored.name == '2CPU--8GB--L4:1'


def test_instance_type_dependency_patch_seams(monkeypatch):
    format_calls = []
    ceil_calls = []
    compile_calls = []
    original_compile = kubernetes_utils.re.compile

    def record_format(value):
        format_calls.append(value)
        return f'formatted-{value}'

    def record_ceil(value):
        ceil_calls.append(value)
        return 3

    def record_compile(pattern, *args, **kwargs):
        compile_calls.append(pattern)
        return original_compile(pattern, *args, **kwargs)

    monkeypatch.setattr(kubernetes_utils.common_utils, 'format_float',
                        record_format)
    monkeypatch.setattr(kubernetes_utils.math, 'ceil', record_ceil)
    monkeypatch.setattr(kubernetes_utils.re, 'compile', record_compile)

    instance_type = KubernetesInstanceType(cpus=2,
                                           memory=8,
                                           accelerator_count=1,
                                           accelerator_type='L4')
    assert instance_type.name == ('formatted-2CPU--formatted-8GB--L4:1')
    from_resources = KubernetesInstanceType.from_resources(
        cpus=2, memory=8, accelerator_count=1.2, accelerator_type='L4')
    assert from_resources.accelerator_count == 3
    assert KubernetesInstanceType.is_valid_instance_type('2CPU--8GB')

    assert format_calls == [2, 8]
    assert ceil_calls == [1.2]
    assert len(compile_calls) == 1


def test_get_vcpus_mem_suppresses_internal_traceback():
    """A non-Kubernetes instance name should raise a clean ValueError.

    Previously, passing e.g. an AWS instance_type to a Kubernetes cloud
    (or hitting this path during dryrun YAML serialization) leaked the
    internal KubernetesInstanceType regex-mismatch traceback. The
    message should name the offending value and be raised without a
    chained internal exception.
    """
    with pytest.raises(ValueError) as e:
        kubernetes_cloud.Kubernetes.get_vcpus_mem_from_instance_type('m5.large')
    # Must clearly mention the offending input.
    assert 'm5.large' in str(e.value)
    # No chained 'during handling of another exception' context.
    assert e.value.__suppress_context__ is True
