# SkyServe autoscaler decision contract

_Created: 2026-07-29_

## Problem

`sky/serve/autoscalers.py` is 7,805 lines and owns both the data contract
exchanged with the Serve controller and the stateful algorithms that compute
that data. The decision enums and dataclasses are consumed by the controller,
replica orchestration, and focused tests, while the policy implementations own
request histories, hysteresis, capacity observations, cost caches, locks, and
durable dynamic state.

The shared contract changes for controller and reconciliation reasons. Keeping
it embedded in the policy implementation makes callers depend on a module whose
main responsibility is mutable autoscaling policy and makes contract review
compete with changes in several large algorithms.

## Goals

Move only the low-state decision contract into a focused module. Preserve
`sky.serve.autoscalers` as the public facade, including direct object identity,
historical `__module__` values, pickle behavior, constructor validation,
representations, and controller behavior. Do not change any autoscaling
algorithm, state transition, lock boundary, call count, or persistence format.

## Background

The module currently has four responsibility families:

1. The decision contract defines scale operators, reasons, logical targets,
   rollout failures, reserved-fill samples, and decision validation. Its callers
   are the Serve controller, replica reconciliation, reserved-capacity logic,
   and tests. It depends only on dataclasses, enums, typing, and Serve constants.
   Its state is immutable value data except for the historically mutable
   `AutoscalerDecision`. Its failures are incompatible targets, broken runtime
   type checks, or serialization drift. Its cadence follows controller and
   reconciliation protocols.
2. The base autoscaler owns version updates, reserved-fill allocation,
   cost-rebalance state, rollout replacement, scaling decision assembly, and
   dynamic-state persistence. Its callers are the controller and concrete
   policies. It depends on service state, global cluster state, spot placement,
   reserved capacity, and operator notifications. Its failures include
   overlaunch, unsafe retirement, stale cost decisions, and persistence drift.
3. Request-rate and instance-aware policies own QPS windows, accelerator
   compatibility, GPU-shape resolution, and hysteresis. Their state includes
   request histories, configured card shapes, provider-handle caches, and
   per-card targets. Their failure modes are incompatible-card launches, stale
   demand, provider calls on hot paths, and oscillation.
4. The concurrency policy owns load-balancer generations, outstanding-work
   accounting, adaptive request durations and provisioning leads, logical
   capacity budgets, downscale vetoes, and retirement selection. Its state is
   protected by a lock and is consumed on every controller decision tick. Its
   failures include stale-generation actuation, double-counted work, capacity
   overshoot, unsafe downscale, and lock races.

The first family has materially different callers, dependencies, state, failure
modes, and reasons to change from the other three. The policy families remain
coupled through inheritance and shared mutable state and are not split here.

## Solution

Add `sky/serve/autoscaler_decisions.py` containing the seven existing enums and
dataclasses without behavioral edits. Set each moved class's `__module__` to
`sky.serve.autoscalers`, then bind direct aliases from the historical module.
The facade therefore adds no wrapper frame or allocation and old pickle globals
continue to resolve.

Keep pure calculation helpers and every policy class in `autoscalers.py`.
Callers continue importing only `sky.serve.autoscalers`; the new module is an
implementation boundary, not a second public API.

## Alternatives considered

Leaving the contract in place avoids one module but preserves mixed ownership
and forces controller-facing reviews through the stateful policy file.
Extracting a complete autoscaling policy would remove more lines but would move
large mutable state, inheritance hooks, and hot-path calculations with a much
higher regression and conflict risk. An abstract base class, registry, strategy,
or dependency-injection layer is unnecessary because the existing policy
classes already provide the variation seam.

## Rollout and rollback

This is a source-only structural extraction with no data or configuration
migration. Rollback is the inverse move because the historical facade remains
the only supported import path.

## Test plan

Before moving code, add characterization coverage for constructors, validation,
representations, dataclass behavior, historical module names, and pickle
round-trips. After extraction, prove direct alias identity between the facade and
implementation module and compare serialized bytes.

Run the focused decision contract, autoscaler, concurrency autoscaler,
cost-rebalance, reserved-fill, and controller tests. Run repository formatting
and static checks for changed files, `git diff --check`, import and pickle
probes, and an alternating cold-import timing comparison. Confirm the unfiltered
unit-test workflow covers the changed production and test paths.
