"""Instance-aware LB must weight load by GPU count, not just type.

A replica on a 4xL4 machine (running one model server per GPU behind a
local fan-out) absorbs 4x the concurrent requests of a 1xL4 replica; the
normalized-load comparison must reflect that or the big node is treated
as saturated after one request.
"""
from sky.serve import load_balancing_policies as lb_policies


def _make_policy(replica_info, target_qps):
    policy = lb_policies.InstanceAwareLeastLoadPolicy()
    policy.set_ready_replicas(list(replica_info.keys()))
    policy.set_replica_info(replica_info)
    policy.set_target_qps_per_accelerator(target_qps)
    return policy


class TestGpuCountNormalization:
    """_get_normalized_load with gpu_count."""

    def test_four_gpu_replica_absorbs_four_requests(self):
        policy = _make_policy(
            {
                'small': {
                    'gpu_type': 'L4',
                    'gpu_count': '1'
                },
                'big': {
                    'gpu_type': 'L4',
                    'gpu_count': '4'
                },
            },
            {'L4': 0.1},
        )
        # One request on each: the big node must look 4x less loaded.
        policy.load_map['small'] = 1
        policy.load_map['big'] = 1
        small = policy._get_normalized_load('small')  # pylint: disable=protected-access
        big = policy._get_normalized_load('big')  # pylint: disable=protected-access
        assert small / big == 4

        # With 3 in flight on big and 1 on small, big is STILL less
        # loaded (3/0.4 < 1/0.1 is false: 7.5 vs 10 -> big wins).
        policy.load_map['big'] = 3
        assert (policy._get_normalized_load('big') <  # pylint: disable=protected-access
                policy._get_normalized_load('small'))  # pylint: disable=protected-access

    def test_exact_shape_key_is_per_replica(self):
        policy = _make_policy(
            {'big': {
                'gpu_type': 'L4',
                'gpu_count': '4'
            }},
            {
                'L4': 0.1,
                'L4:4': 0.3
            },
        )
        policy.load_map['big'] = 3
        # 3 / 0.3 = 10, not 3 / (0.1 * 4) = 7.5.
        assert policy._get_normalized_load('big') == 10  # pylint: disable=protected-access

    def test_count_suffixed_key_normalized_to_per_gpu(self):
        policy = _make_policy(
            {'big': {
                'gpu_type': 'L4',
                'gpu_count': '4'
            }},
            {'L4:2': 0.2},
        )
        policy.load_map['big'] = 1
        # per-GPU = 0.2 / 2 = 0.1; per-replica = 0.4.
        assert policy._get_normalized_load('big') == 1 / 0.4  # pylint: disable=protected-access

    def test_missing_count_defaults_to_one(self):
        # Old controllers don't send gpu_count: behave exactly as before.
        policy = _make_policy(
            {'r': {
                'gpu_type': 'L4'
            }},
            {'L4': 0.1},
        )
        policy.load_map['r'] = 1
        assert policy._get_normalized_load('r') == 10  # pylint: disable=protected-access
