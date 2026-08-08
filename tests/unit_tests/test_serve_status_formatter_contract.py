"""Characterization tests for the SkyServe status-table facade."""
# pylint: disable=protected-access
import copy
import pickle
import re
from unittest import mock

from sky.serve import serve_state
from sky.serve import serve_status_formatter
from sky.serve import serve_utils

_ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')


def _record() -> dict:
    return {
        'name': 'svc',
        'active_versions': [1, 2],
        'uptime': 65,
        'status': serve_state.ServiceStatus.READY,
        'ready_replicas': 1,
        'total_replicas': 2,
        'endpoint': 'https://svc.example',
        'policy': 'autoscale',
        'requested_resources_str': '1x A100',
        'load_balancing_policy': 'round_robin',
        'replica_info': [
            {
                'replica_id': 1,
                'version': 1,
                'endpoint': 'http://r1',
                'launched_at': 1,
                'infra': 'aws/us-east-1',
                'resources_str': 'A100:1',
                'resources_str_full': 'A100:1, 8CPU',
                'status': serve_state.ReplicaStatus.READY,
                'used_by': None,
                'handle': None,
            },
            {
                'replica_id': 2,
                'version': 2,
                'endpoint': None,
                'launched_at': 2,
                'infra': 'gcp/us-central1',
                'resources_str': 'L4:1',
                'resources_str_full': 'L4:1, 4CPU',
                'status': serve_state.ReplicaStatus.PROVISIONING,
                'used_by': ['job-a', 'job-b', 'job-c'],
                'handle': None,
            },
        ],
    }


def _normalized_lines(rendered: str) -> list[str]:
    return [
        _ANSI_ESCAPE.sub('', line).rstrip() for line in rendered.splitlines()
    ]


def test_service_table_contract_and_input_mutation():
    record = _record()
    with mock.patch.object(serve_utils.log_utils,
                           'readable_time_duration',
                           return_value='1m'):
        rendered = serve_utils.format_service_table([record],
                                                    show_all=False,
                                                    pool=False)

    assert _normalized_lines(rendered) == [
        'NAME  VERSION  UPTIME  STATUS  REPLICAS  ENDPOINT',
        'svc   1,2      1m      READY   1/2       https://svc.example',
        '',
        'Service Replicas',
        ('SERVICE_NAME  ID  VERSION  ENDPOINT   LAUNCHED  INFRA'
         '            RESOURCES  STATUS'),
        ('svc           1   1        http://r1  1m        aws/us-east-1'
         '    A100:1     READY'),
        ('svc           2   2        -          1m        gcp/us-central1'
         '  L4:1       PROVISIONING'),
    ]
    assert [replica['service_name'] for replica in record['replica_info']
           ] == ['svc', 'svc']


def test_pool_table_contract_with_full_columns():
    record = _record()
    with mock.patch.object(serve_utils.log_utils,
                           'readable_time_duration',
                           return_value='1m'):
        rendered = serve_utils.format_service_table([record],
                                                    show_all=True,
                                                    pool=True)

    assert _normalized_lines(rendered) == [
        ('NAME  VERSION  UPTIME  STATUS  WORKERS  AUTOSCALING_POLICY'
         '  REQUESTED_RESOURCES'),
        ('svc   1,2      1m      READY   1/2      autoscale'
         '           1x A100'),
        '',
        'Pool Workers',
        ('POOL_NAME  ID  VERSION  LAUNCHED  INFRA            RESOURCES'
         '     STATUS        USED_BY'),
        ('svc        1   1        1m        aws/us-east-1    A100:1, 8CPU'
         '  READY         -'),
        ('svc        2   2        1m        gcp/us-central1  L4:1, 4CPU'
         '    PROVISIONING  job-a, job-b, +1 more'),
    ]


def test_empty_and_replica_truncation_contracts():
    assert serve_utils.format_service_table([], False,
                                            False) == 'No existing services.'
    assert serve_utils.format_service_table([], False,
                                            True) == 'No existing pools.'

    replicas = []
    for replica_id in range(serve_utils._REPLICA_TRUNC_NUM + 1):
        replica = copy.deepcopy(_record()['replica_info'][0])
        replica['service_name'] = 'svc'
        replica['replica_id'] = replica_id
        replicas.append(replica)
    with mock.patch.object(serve_utils.log_utils,
                           'readable_time_duration',
                           return_value='1m'):
        rendered = serve_utils._format_replica_table(replicas,
                                                     show_all=False,
                                                     pool=False)
    plain_rendered = _ANSI_ESCAPE.sub('', rendered)
    last_included = serve_utils._REPLICA_TRUNC_NUM - 1
    first_excluded = serve_utils._REPLICA_TRUNC_NUM
    assert re.search(rf'^svc\s+{last_included}\s', plain_rendered, re.MULTILINE)
    assert not re.search(rf'^svc\s+{first_excluded}\s', plain_rendered,
                         re.MULTILINE)
    # `sky serve status` has no --all; show_all comes from --verbose there.
    assert '... (use -v to show all replicas)' in rendered


def test_facade_function_metadata_and_pickle_contract():
    assert (serve_utils._REPLICA_TRUNC_NUM ==
            serve_status_formatter._REPLICA_TRUNC_NUM)
    for function in (serve_utils._get_replicas,
                     serve_utils.format_service_table,
                     serve_utils._format_replica_table):
        assert function.__module__ == 'sky.serve.serve_utils'
        assert pickle.loads(pickle.dumps(function)) is function
    assert (serve_utils._get_replicas is serve_status_formatter._get_replicas)
    assert (serve_utils.format_service_table
            is serve_status_formatter.format_service_table)
    assert (serve_utils._format_replica_table
            is serve_status_formatter._format_replica_table)
