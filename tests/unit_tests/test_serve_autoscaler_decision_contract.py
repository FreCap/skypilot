"""Characterization tests for the public autoscaler decision contract."""

import pickle

import pytest

from sky.serve import autoscaler_decisions
from sky.serve import autoscalers

_CONTRACT_TYPES = (
    autoscalers.AutoscalerDecisionOperator,
    autoscalers.AutoscalerDecisionReason,
    autoscalers.LogicalScaleTarget,
    autoscalers.LogicalScaleDownTarget,
    autoscalers.UnrecoverableRolloutFailure,
    autoscalers.FillDemandSample,
    autoscalers.AutoscalerDecision,
)


@pytest.mark.parametrize('contract_type', _CONTRACT_TYPES)
def test_decision_contract_preserves_historical_module_and_pickle(
        contract_type):
    assert contract_type.__module__ == 'sky.serve.autoscalers'
    assert pickle.loads(pickle.dumps(contract_type)) is contract_type


def test_autoscaler_decision_validation_and_repr():
    target = autoscalers.LogicalScaleTarget(version=3,
                                            reconcile_generation=7,
                                            target_capacity=11)
    decision = autoscalers.AutoscalerDecision(
        autoscalers.AutoscalerDecisionOperator.SCALE_UP, target)

    assert repr(decision) == (
        'AutoscalerDecision(AutoscalerDecisionOperator.SCALE_UP, '
        'LogicalScaleTarget(version=3, reconcile_generation=7, '
        'target_capacity=11, target_capacity_by_accelerator=(), '
        'accelerator_shapes=(), replace_unknown_replica_ids=(), '
        'launch_budget=None, launch_priority=0, '
        'launch_priority_by_accelerator=(), '
        'cold_launch_authority_by_accelerator=None), reason=None)')

    with pytest.raises(AssertionError):
        autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, target)


def test_logical_paid_launch_authority_distinguishes_legacy_and_empty():
    legacy = autoscalers.LogicalScaleTarget(version=3,
                                            reconcile_generation=7,
                                            target_capacity=11)
    explicit_empty = autoscalers.LogicalScaleTarget(
        version=3,
        reconcile_generation=7,
        target_capacity=11,
        cold_launch_authority_by_accelerator=())

    assert legacy.cold_launch_authority_by_accelerator is None
    assert explicit_empty.cold_launch_authority_by_accelerator == ()
    assert pickle.loads(pickle.dumps(explicit_empty)) == explicit_empty


def test_fill_demand_sample_contract():
    sample = autoscalers.FillDemandSample(outstanding_work=5.1,
                                          busy_fill_holdings=1,
                                          pre_ready_fill_holdings=2,
                                          upscale_pending=False,
                                          work_per_replica=2.0)

    assert sample.demonstrated_need() == 3
    assert sample.boot_hold()


@pytest.mark.parametrize('contract_type', _CONTRACT_TYPES)
def test_decision_contract_facade_is_direct_alias(contract_type):
    assert contract_type is getattr(autoscaler_decisions,
                                    contract_type.__name__)
