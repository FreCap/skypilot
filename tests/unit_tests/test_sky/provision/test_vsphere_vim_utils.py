"""Tests for vSphere inventory conversion helpers."""

from types import SimpleNamespace

from sky.provision.vsphere.common import vim_utils


def _pci_device(device_id: str, *, class_id: int, name: str):
    return SimpleNamespace(id=device_id,
                           classId=class_id,
                           vendorId=0x10DE,
                           vendorName='NVIDIA',
                           deviceId=0x2235,
                           deviceName=name,
                           subVendorId=0x10DE,
                           subDeviceId=0x145A)


def test_pci_passthrough_state_is_matched_by_device_id():
    gpu = _pci_device('0000:65:00.0', class_id=0x0302, name='NVIDIA A100')
    network_card = _pci_device('0000:17:00.0',
                               class_id=0x0200,
                               name='Network Adapter')
    # vSphere exposes independent device and passthrough arrays. Return the
    # passthrough states in a different order to ensure the stable PCI IDs,
    # rather than array positions, determine the GPU status.
    passthrough_info = [
        SimpleNamespace(id=network_card.id, passthruActive=False),
        SimpleNamespace(id=gpu.id, passthruActive=True),
    ]
    hardware = SimpleNamespace(
        systemInfo=SimpleNamespace(model='model', vendor='vendor', uuid='uuid'),
        cpuInfo=SimpleNamespace(numCpuCores=8,
                                numCpuPackages=1,
                                numCpuThreads=16),
        memorySize=16 * 1024**3,
        pciDevice=[gpu, network_card],
    )
    summary = SimpleNamespace(
        hardware=SimpleNamespace(cpuMhz=2000),
        quickStats=SimpleNamespace(overallCpuUsage=100,
                                   overallMemoryUsage=1024),
    )
    host = SimpleNamespace(
        hardware=hardware,
        summary=summary,
        vm=[],
        name='host-1',
        config=SimpleNamespace(pciPassthruInfo=passthrough_info),
        _moId='host-1')

    hosts = vim_utils.list_hosts_with_devices_info([host], 'vcenter',
                                                   'datacenter', 'cluster', [{
                                                       'Model': 'NVIDIA A100',
                                                       'MemoryMB': 40960,
                                                   }])

    assert hosts[0]['Accelerators'] == [{
        'ID': gpu.id,
        'ClassID': '0302',
        'VendorID': '10de',
        'VendorName': 'NVIDIA',
        'DeviceID': '2235',
        'DeviceName': 'NVIDIA A100',
        'SubVendorID': '10de',
        'SubDeviceID': '145a',
        'Status': 'Available',
        'MemorySizeMB': 40960,
    }]
