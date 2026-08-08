import {
  emptyPreemptible,
  mergePreemptible,
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
        gpu_preemptible_breakdown: { 'inference-low (-1000)': 70 },
        context: 'prod',
      },
      {
        gpu_name: 'A100',
        gpu_total: 16,
        gpu_free: 4,
        gpu_not_ready: 0,
        gpu_preemptible: 2,
        gpu_preemptible_breakdown: {
          'inference-low (-1000)': 1,
          'drill (-500)': 1,
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
      gpu_preemptible: 72,
      gpu_preemptible_breakdown: {
        'inference-low (-1000)': 71,
        'drill (-500)': 1,
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
  });

  it('returns nothing for empty or missing input', () => {
    expect(summarizeGpusByType([])).toEqual([]);
    expect(summarizeGpusByType(undefined)).toEqual([]);
  });
});

describe('mergePreemptible', () => {
  it('sums counts and unions the per-class breakdown', () => {
    const target = emptyPreemptible();
    mergePreemptible(target, {
      gpu_preemptible: 5,
      gpu_preemptible_breakdown: { a: 4, b: 1 },
    });
    mergePreemptible(target, {
      gpu_preemptible: 2,
      gpu_preemptible_breakdown: { b: 2 },
    });

    expect(target).toEqual({
      gpu_preemptible: 7,
      gpu_preemptible_breakdown: { a: 4, b: 3 },
    });
  });

  it('tolerates sources with no preemptible fields', () => {
    const target = emptyPreemptible();
    mergePreemptible(target, { gpu_total: 8 });
    mergePreemptible(target, undefined);

    expect(target).toEqual(emptyPreemptible());
  });
});
