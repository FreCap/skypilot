'use client';

import { statusGroups } from '@/components/jobs';
import { getClusters } from '@/data/connectors/clusters';
import { getManagedJobs } from '@/data/connectors/jobs';
import dashboardCache from '@/lib/cache';

export const ACTIVE_JOB_STATUSES = new Set(statusGroups.active);

// Statuses for which a managed job actually occupies GPUs and should be
// counted toward a user's GPU usage. A job's accelerators are derived from its
// live cluster handle, so a STARTING job (cluster still provisioning, e.g. a
// k8s pod sitting Pending) can report accelerators it has not yet been
// allocated, inflating per-user GPU totals above physical cluster capacity.
// Only count jobs that have actually acquired GPUs: RUNNING, RECOVERING (a
// previously-running job re-acquiring the same resources), and CANCELLING (the
// cluster still holds GPUs until teardown completes). PENDING/SUBMITTED jobs
// have no cluster handle and already report no accelerators.
const GPU_CONSUMING_JOB_STATUSES = new Set([
  'RUNNING',
  'RECOVERING',
  'CANCELLING',
]);

// Helper function to get GPU count with validation
export const getGPUCount = (accelerators, source) => {
  if (!accelerators) return 0;

  let parsed = accelerators;

  // Handle string format (from clusters): "{'V100': 4}"
  if (typeof accelerators === 'string') {
    try {
      const jsonStr = accelerators.replace(/'/g, '"').replace(/None/g, 'null');
      parsed = JSON.parse(jsonStr);
    } catch (e) {
      console.error('Failed to parse accelerators string:', accelerators, e);
      return 0;
    }
  }

  // Validate and extract GPU count
  if (typeof parsed === 'object' && parsed !== null) {
    const entries = Object.entries(parsed);

    if (entries.length === 0) {
      return 0;
    }

    if (entries.length > 1) {
      console.warn(
        `${source} has ${entries.length} accelerator entries:`,
        parsed
      );
    }

    // Return the first (and ideally only) GPU count
    return Number(entries[0][1]) || 0;
  }

  return 0;
};

// Extract num_nodes from a cluster_resources_full string (e.g. "3x(...)").
// Defaults to 1 when the count cannot be determined.
const extractNumNodes = (clusterResourcesFull) => {
  if (!clusterResourcesFull || typeof clusterResourcesFull !== 'string') {
    return 1;
  }
  const match = clusterResourcesFull.match(/^(\d+)x/);
  return match ? parseInt(match[1], 10) : 1;
};

// Total GPUs a managed job currently occupies, across all of its nodes.
// Returns 0 for jobs that have not actually been allocated GPUs yet (see
// GPU_CONSUMING_JOB_STATUSES) so per-user GPU totals never exceed the physical
// cluster capacity.
export const getJobGpuCount = (job) => {
  if (!job || !GPU_CONSUMING_JOB_STATUSES.has(job.status)) {
    return 0;
  }
  const gpuCountPerNode = getGPUCount(
    job.accelerators,
    `Job ${job.job_name || job.job_id}`
  );
  const numNodes = extractNumNodes(job.resources_str_full);
  return gpuCountPerNode * numNodes;
};

// Helper function to fetch clusters and managed jobs data with independent error handling
// Uses Promise.allSettled so one failure doesn't affect the other
export const fetchClustersAndJobs = async () => {
  const [clustersResult, jobsResult] = await Promise.allSettled([
    dashboardCache.get(getClusters),
    // Use shared cache key (no field filtering) - preloader uses same args
    dashboardCache.get(getManagedJobs, [
      { allUsers: true, skipFinished: true },
    ]),
  ]);

  const clustersData =
    (clustersResult.status === 'fulfilled' && clustersResult.value) || [];
  const jobsResponse = (jobsResult.status === 'fulfilled' &&
    jobsResult.value) || { jobs: [] };

  if (clustersResult.status === 'rejected') {
    console.error('Error fetching clusters:', clustersResult.reason);
  }
  if (jobsResult.status === 'rejected') {
    console.error('Error fetching managed jobs:', jobsResult.reason);
  }

  return { clustersData, jobsResponse };
};
