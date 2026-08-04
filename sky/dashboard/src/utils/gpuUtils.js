/**
 * GPU name canonicalization utilities.
 *
 * Mirrors the logic from SkyPilot's Python backend
 * (sky.provision.kubernetes.utils.GFDLabelFormatter) so the dashboard
 * displays clean, consistent GPU names regardless of the raw label value
 * reported by different Kubernetes GPU discovery plugins.
 */

/**
 * Canonical GPU model names, ordered so that more-specific names are checked
 * before less-specific ones (e.g. H100-80GB before H100).
 * Keep in sync with sky/utils/gpu_names.py.
 */
export const CANONICAL_GPU_NAMES = [
  'GB300',
  'GB200',
  'B300',
  'B200',
  'B100',
  'GH200',
  'H200',
  'H100-MEGA',
  'H100',
  'A100',
  'A10G',
  'A10',
  'A16',
  'A30',
  'A40',
  'RTX6000-Ada',
  'L40S',
  'L40',
  'L4',
  'A6000',
  'A5000',
  'A4000',
  'V100',
  'P100',
  'P40',
  'P4000',
  'P4',
  'T4g',
  'T4',
  'K80',
  'M60',
];

/**
 * Canonicalize a raw GPU model string to a clean display name.
 *
 * @param {string} rawName - The raw GPU name (e.g. "NVIDIA A100-SXM4-80GB")
 * @returns {string} A canonical display name (e.g. "A100-80GB")
 */
export function canonicalizeGpuName(rawName) {
  if (!rawName) return 'Unknown';
  const value = rawName.trim();
  if (!value) return 'Unknown';

  for (const canonical of CANONICAL_GPU_NAMES) {
    // Word-boundary matching to prevent substring matches (e.g. L4 vs L40)
    const re = new RegExp(
      `\\b${canonical.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`,
      'i'
    );
    if (re.test(value)) {
      return canonical;
    }
  }

  // Fallback: clean up the name
  return (
    value
      .toUpperCase()
      .replace(/^NVIDIA[\s-]*/i, '')
      .replace(/^GEFORCE[\s-]*/i, '')
      .replace(/RTX[\s-]/i, 'RTX')
      .replace(/-SXM[\d]*/, '')
      .replace(/-PCIE/, '')
      .trim() || 'Unknown'
  );
}

/**
 * Check whether a cluster or managed-job task has any accelerator requested.
 *
 * Accepts the structured `accelerators` field SkyPilot exposes on both
 * clusters and managed jobs. The value can be:
 *   - An object: {"A100": 4}, {"H100-80GB": 8}, or {} for none
 *   - A Python-repr string: "{'A100': 4}" or "None" or ""
 *   - null / undefined
 *
 * Used to decide whether to render GPU telemetry panels; CPU-only resources
 * should suppress GPU panels to avoid empty charts.
 *
 * @param {Object|string|null} accelerators - The accelerators field
 * @returns {boolean} True if any accelerator is requested.
 */
export function hasAccelerator(accelerators) {
  if (accelerators == null) return false;
  let parsed = accelerators;
  // Handle Python-repr strings like "{'A100': 4}" or "None".
  if (typeof accelerators === 'string') {
    const trimmed = accelerators.trim();
    if (!trimmed || trimmed === 'None' || trimmed === 'null') return false;
    try {
      parsed = JSON.parse(trimmed.replace(/'/g, '"').replace(/None/g, 'null'));
    } catch {
      return false;
    }
  }
  if (typeof parsed === 'object' && parsed !== null) {
    // Any entry with a non-zero count means an accelerator is requested.
    return Object.values(parsed).some((v) => Number(v) > 0);
  }
  return false;
}

/**
 * Zero-valued preemptible accounting for a fresh per-GPU-type aggregate.
 *
 * "Preemptible" accelerators are those held by pods below the cluster's top
 * scheduling priority tier, i.e. reclaimable by a higher-priority workload.
 * The Infra page aggregates GPU rows at three levels (per node into a context,
 * per context into a workspace view, and per GPU type across contexts); every
 * one of them must carry these two fields or the split silently collapses back
 * into a single "used" block.
 */
export function emptyPreemptible() {
  return { gpu_preemptible: 0, gpu_preemptible_breakdown: {} };
}

/**
 * Fold one per-GPU-type aggregate's preemptible accounting into another.
 */
export function mergePreemptible(target, source) {
  target.gpu_preemptible += source?.gpu_preemptible || 0;
  for (const [label, qty] of Object.entries(
    source?.gpu_preemptible_breakdown || {}
  )) {
    target.gpu_preemptible_breakdown[label] =
      (target.gpu_preemptible_breakdown[label] || 0) + qty;
  }
}

/**
 * Aggregate per-context GPU rows into one row per canonical GPU type.
 *
 * This is what feeds the top-level Kubernetes utilization bars, so it must
 * carry every field the bar reads. Exported (rather than inlined in the
 * component effect that consumes it) so the aggregation is unit-testable
 * without rendering the page.
 */
export function summarizeGpusByType(perContextGPUs) {
  const gpuSummary = {};
  (perContextGPUs || []).forEach((gpu) => {
    const gpuName = canonicalizeGpuName(gpu.gpu_name);
    if (!(gpuName in gpuSummary)) {
      gpuSummary[gpuName] = {
        gpu_name: gpuName,
        gpu_total: 0,
        gpu_free: 0,
        gpu_not_ready: 0,
        ...emptyPreemptible(),
      };
    }
    gpuSummary[gpuName].gpu_total += gpu.gpu_total || 0;
    gpuSummary[gpuName].gpu_free += gpu.gpu_free || 0;
    gpuSummary[gpuName].gpu_not_ready += gpu.gpu_not_ready || 0;
    mergePreemptible(gpuSummary[gpuName], gpu);
  });
  return Object.values(gpuSummary);
}
