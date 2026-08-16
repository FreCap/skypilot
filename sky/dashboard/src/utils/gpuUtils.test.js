import {
  emptySchedulingBreakdown,
  mergeSchedulingBreakdown,
  summarizeGpusByType,
} from '@/utils/gpuUtils';

describe('summarizeGpusByType', () => {
  it('carries preemptible accounting into the top-level summary', () => {
    // Two contexts reporting the same GPU type, as the Infra page's
    // Kubernetes section sees them. Dropping the preemptible fields here
    // collapses the bar back into a single "used" block even though the
    // per-context rows carry the split.
    const summary = summarizeGpusByType([
      {
        gpu_name: 'A100',
        gpu_total: 328,
        gpu_free: 0,
        gpu_not_ready: 0,
        gpu_preemptible: 70,
        gpu_allocation_breakdown: {
          'ma-lt (31)': 258,
          'wa-eval (20)': 70,
        },
        gpu_allocation_workload_breakdown: {
          'ma-lt (31)': ['prod/train-a'],
          'wa-eval (20)': ['prod/eval-a', 'prod/eval-b'],
        },
        gpu_preemptible_breakdown: { 'inference-low (-1000)': 70 },
        gpu_preemptible_services: 60,
        gpu_preemptible_service_breakdown: { 'boltz-l4-fleet': 60 },
        gpu_preemptible_service_priority_breakdown: {
          'boltz-l4-fleet': { 'inference-low (-1000)': 60 },
        },
        gpu_preemptible_service_priority_workload_breakdown: {
          'boltz-l4-fleet': {
            'inference-low (-1000)': ['prod/replica-a', 'prod/replica-b'],
          },
        },
        context: 'prod',
      },
      {
        gpu_name: 'A100',
        gpu_total: 16,
        gpu_free: 4,
        gpu_not_ready: 0,
        gpu_preemptible: 2,
        gpu_allocation_breakdown: {
          'ma-lt (31)': 10,
          'inference-low (-1000)': 1,
          'drill (-500)': 1,
        },
        gpu_allocation_workload_breakdown: {
          'ma-lt (31)': ['staging/train-a'],
          'inference-low (-1000)': ['staging/replica-a'],
          'drill (-500)': ['staging/drill-a'],
        },
        gpu_preemptible_breakdown: {
          'inference-low (-1000)': 1,
          'drill (-500)': 1,
        },
        gpu_preemptible_services: 1,
        gpu_preemptible_service_breakdown: { 'boltz-l4-fleet': 1 },
        gpu_preemptible_service_priority_breakdown: {
          'boltz-l4-fleet': { 'inference-low (-1000)': 1 },
        },
        gpu_preemptible_service_priority_workload_breakdown: {
          'boltz-l4-fleet': {
            'inference-low (-1000)': ['staging/replica-a'],
          },
        },
        context: 'staging',
      },
    ]);

    expect(summary).toHaveLength(1);
    expect(summary[0]).toEqual({
      gpu_name: 'A100',
      gpu_total: 344,
      gpu_free: 4,
      gpu_not_ready: 0,
      gpu_allocation_breakdown: {
        'ma-lt (31)': 268,
        'wa-eval (20)': 70,
        'inference-low (-1000)': 1,
        'drill (-500)': 1,
      },
      gpu_allocation_workload_breakdown: {
        'ma-lt (31)': ['prod/train-a', 'staging/train-a'],
        'wa-eval (20)': ['prod/eval-a', 'prod/eval-b'],
        'inference-low (-1000)': ['staging/replica-a'],
        'drill (-500)': ['staging/drill-a'],
      },
      gpu_preemptible: 72,
      gpu_preemptible_breakdown: {
        'inference-low (-1000)': 71,
        'drill (-500)': 1,
      },
      gpu_preemptible_services: 61,
      gpu_preemptible_service_breakdown: { 'boltz-l4-fleet': 61 },
      gpu_preemptible_service_priority_breakdown: {
        'boltz-l4-fleet': { 'inference-low (-1000)': 61 },
      },
      gpu_preemptible_service_priority_workload_breakdown: {
        'boltz-l4-fleet': {
          'inference-low (-1000)': [
            'prod/replica-a',
            'prod/replica-b',
            'staging/replica-a',
          ],
        },
      },
    });
  });

  it('groups by canonical GPU name across raw label spellings', () => {
    const summary = summarizeGpusByType([
      {
        gpu_name: 'NVIDIA-A100-SXM4-80GB',
        gpu_total: 8,
        gpu_free: 0,
        gpu_preemptible: 3,
        gpu_preemptible_breakdown: { 'inference-low (-1000)': 3 },
      },
      {
        gpu_name: 'A100',
        gpu_total: 8,
        gpu_free: 1,
        gpu_preemptible: 1,
        gpu_preemptible_breakdown: { 'inference-low (-1000)': 1 },
      },
    ]);

    expect(summary).toHaveLength(1);
    expect(summary[0].gpu_total).toBe(16);
    expect(summary[0].gpu_preemptible).toBe(4);
  });

  it('defaults rows that predate the preemptible fields to zero', () => {
    // An older server, or a Slurm/SSH row, reports no preemptible data. The
    // summary must still be well-formed so the bar renders as it did before.
    const summary = summarizeGpusByType([
      { gpu_name: 'H200', gpu_total: 512, gpu_free: 22, gpu_not_ready: 0 },
    ]);

    expect(summary[0].gpu_preemptible).toBe(0);
    expect(summary[0].gpu_preemptible_breakdown).toEqual({});
    expect(summary[0].gpu_preemptible_services).toBe(0);
    expect(summary[0].gpu_preemptible_service_breakdown).toEqual({});
    expect(summary[0].gpu_allocation_breakdown).toEqual({});
    expect(summary[0].gpu_allocation_workload_breakdown).toEqual({});
    expect(summary[0].gpu_preemptible_service_priority_breakdown).toEqual({});
    expect(
      summary[0].gpu_preemptible_service_priority_workload_breakdown
    ).toEqual({});
  });

  it('returns nothing for empty or missing input', () => {
    expect(summarizeGpusByType([])).toEqual([]);
    expect(summarizeGpusByType(undefined)).toEqual([]);
  });
});

describe('mergeSchedulingBreakdown', () => {
  it('sums counts and unions the per-class breakdown', () => {
    const target = emptySchedulingBreakdown();
    mergeSchedulingBreakdown(target, {
      gpu_allocation_breakdown: { top: 3, a: 4, b: 1 },
      gpu_allocation_workload_breakdown: {
        top: ['job-shared'],
        a: ['job-a'],
        b: ['job-b'],
      },
      gpu_preemptible: 5,
      gpu_preemptible_breakdown: { a: 4, b: 1 },
      gpu_preemptible_services: 4,
      gpu_preemptible_service_breakdown: { fleet: 4 },
      gpu_preemptible_service_priority_breakdown: { fleet: { a: 4 } },
      gpu_preemptible_service_priority_workload_breakdown: {
        fleet: { a: ['job-a'] },
      },
    });
    mergeSchedulingBreakdown(target, {
      gpu_allocation_breakdown: { top: 2, b: 2 },
      gpu_allocation_workload_breakdown: {
        top: ['job-shared'],
        b: ['job-b', 'job-c'],
      },
      gpu_preemptible: 2,
      gpu_preemptible_breakdown: { b: 2 },
      gpu_preemptible_services: 1,
      gpu_preemptible_service_breakdown: { fleet: 1 },
      gpu_preemptible_service_priority_breakdown: { fleet: { b: 1 } },
      gpu_preemptible_service_priority_workload_breakdown: {
        fleet: { b: ['job-b'] },
      },
    });

    expect(target).toEqual({
      gpu_allocation_breakdown: { top: 5, a: 4, b: 3 },
      gpu_allocation_workload_breakdown: {
        top: ['job-shared'],
        a: ['job-a'],
        b: ['job-b', 'job-c'],
      },
      gpu_preemptible: 7,
      gpu_preemptible_breakdown: { a: 4, b: 3 },
      gpu_preemptible_services: 5,
      gpu_preemptible_service_breakdown: { fleet: 5 },
      gpu_preemptible_service_priority_breakdown: {
        fleet: { a: 4, b: 1 },
      },
      gpu_preemptible_service_priority_workload_breakdown: {
        fleet: { a: ['job-a'], b: ['job-b'] },
      },
    });
  });

  it('tolerates sources with no preemptible fields', () => {
    const target = emptySchedulingBreakdown();
    mergeSchedulingBreakdown(target, { gpu_total: 8 });
    mergeSchedulingBreakdown(target, undefined);

    expect(target).toEqual(emptySchedulingBreakdown());
  });

  it('propagates unknown SkyServe attribution instead of summing it as zero', () => {
    const target = emptySchedulingBreakdown();
    mergeSchedulingBreakdown(target, {
      gpu_preemptible: 432,
      gpu_preemptible_breakdown: { 'ma-lt (31)': 176, 'priority 0': 256 },
      gpu_preemptible_services: null,
      gpu_preemptible_service_breakdown: null,
      gpu_preemptible_service_priority_breakdown: null,
    });
    mergeSchedulingBreakdown(target, {
      gpu_preemptible: 4,
      gpu_preemptible_services: 4,
      gpu_preemptible_service_breakdown: { fleet: 4 },
      gpu_preemptible_service_priority_breakdown: { fleet: { be: 4 } },
    });

    expect(target.gpu_preemptible).toBe(436);
    expect(target.gpu_preemptible_services).toBeNull();
    expect(target.gpu_preemptible_service_breakdown).toBeNull();
    expect(target.gpu_preemptible_service_priority_breakdown).toBeNull();
  });
});
