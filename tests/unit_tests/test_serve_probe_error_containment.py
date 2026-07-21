"""ReplicaInfo.probe() must never let an exception escape.

The replica prober drains probe futures with future.get(); an exception
escaping probe() aborts the entire probe round before any status writes,
and since probe inputs (readiness path, headers, post data) come from the
user's service YAML, a bad static value (e.g. a non-latin-1 header) would
re-raise on every 10s tick, permanently stalling status/uptime updates
for every replica. probe() therefore contains all errors and reports the
replica as not ready, mirroring probe_pool().
"""
# pylint: disable=protected-access
import unittest
from unittest import mock

import requests

from sky.serve import replica_managers


def _replica_info():
    info = object.__new__(replica_managers.ReplicaInfo)
    info.replica_id = 1
    return info


class TestProbeErrorContainment(unittest.TestCase):
    """probe() contains all exception types and reports not-ready."""

    def _probe(self, info, post_data=None):
        return info.probe(
            readiness_path='/health',
            post_data=post_data,
            timeout=5,
            headers={'Authorization': 'token'},
            resolved_url='http://10.0.0.1:8080',
        )

    def test_get_non_request_exception_reports_not_ready(self):
        info = _replica_info()
        err = UnicodeEncodeError('latin-1', 'café', 3, 4,
                                 'ordinal not in range(256)')
        with mock.patch.object(requests, 'get', side_effect=err):
            returned, is_ready, probe_time = self._probe(info)
        self.assertIs(returned, info)
        self.assertFalse(is_ready)
        self.assertIsInstance(probe_time, float)

    def test_post_non_request_exception_reports_not_ready(self):
        info = _replica_info()
        with mock.patch.object(requests,
                               'post',
                               side_effect=TypeError('bad post data')):
            returned, is_ready, _ = self._probe(info, post_data={'k': object()})
        self.assertIs(returned, info)
        self.assertFalse(is_ready)

    def test_request_exception_still_reports_not_ready(self):
        info = _replica_info()
        with mock.patch.object(
                requests,
                'get',
                side_effect=requests.exceptions.ConnectionError('refused')):
            returned, is_ready, _ = self._probe(info)
        self.assertIs(returned, info)
        self.assertFalse(is_ready)


if __name__ == '__main__':
    unittest.main()
