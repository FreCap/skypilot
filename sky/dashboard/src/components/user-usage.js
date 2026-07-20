'use client';

import { statusGroups } from '@/components/job-domain';
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

// Build the unfiltered resource totals shared by user-facing tables. Each
// resource snapshot is visited once, so adding users does not multiply the
// cost of processing large cluster and managed-job fleets.
export const aggregateUserUsage = (clusters = [], jobs = []) => {
  const usageByUser = new Map();

  const getUsage = (userId) => {
    let usage = usageByUser.get(userId);
    if (!usage) {
      usage = { clusterCount: 0, jobCount: 0, gpuCount: 0 };
      usageByUser.set(userId, usage);
    }
    return usage;
  };

  for (const cluster of clusters) {
    const userId = cluster.user_hash;
    if (!userId) continue;

    const usage = getUsage(userId);
    usage.clusterCount += 1;
    if (cluster.status !== 'STOPPED' && cluster.status !== 'TERMINATED') {
      const gpuCountPerNode = getGPUCount(
        cluster.gpus,
        `Cluster ${cluster.cluster}`
      );
      usage.gpuCount += gpuCountPerNode * (cluster.num_nodes || 1);
    }
  }

  for (const job of jobs) {
    if (!job.user_hash || !ACTIVE_JOB_STATUSES.has(job.status)) continue;

    const usage = getUsage(job.user_hash);
    usage.jobCount += 1;
    usage.gpuCount += getJobGpuCount(job);
  }

  return usageByUser;
};

const extractGPUType = (accelerators) => {
  if (!accelerators) return null;

  let parsed = accelerators;
  if (typeof accelerators === 'string') {
    try {
      const jsonStr = accelerators.replace(/'/g, '"').replace(/None/g, 'null');
      parsed = JSON.parse(jsonStr);
    } catch (e) {
      return null;
    }
  }

  if (typeof parsed === 'object' && parsed !== null) {
    const entries = Object.entries(parsed);
    if (entries.length > 0) {
      return entries[0][0];
    }
  }
  return null;
};

// Build the per-user resource dimensions used by GPU and infrastructure
// filters. Each snapshot is visited once; aggregate entries support filtering
// by either dimension without rescanning clusters or jobs during rendering.
export const buildUsageFilterLookup = (clusters = [], jobs = []) => {
  const lookup = {};

  const updateLookup = (
    userId,
    infra,
    gpuType,
    clusterDelta,
    jobDelta,
    gpuDelta
  ) => {
    if (!userId || !infra) return;

    if (!lookup[userId]) {
      lookup[userId] = {};
    }
    if (!lookup[userId][infra]) {
      lookup[userId][infra] = {};
    }
    if (!lookup[userId]['Total']) {
      lookup[userId]['Total'] = {};
    }

    if (!lookup[userId][infra]['Total']) {
      lookup[userId][infra]['Total'] = {
        clusterCount: 0,
        jobCount: 0,
        gpuCount: 0,
      };
    }
    lookup[userId][infra]['Total'].clusterCount += clusterDelta;
    lookup[userId][infra]['Total'].jobCount += jobDelta;
    lookup[userId][infra]['Total'].gpuCount += gpuDelta;

    if (gpuType) {
      if (!lookup[userId][infra][gpuType]) {
        lookup[userId][infra][gpuType] = {
          clusterCount: 0,
          jobCount: 0,
          gpuCount: 0,
        };
      }
      lookup[userId][infra][gpuType].clusterCount += clusterDelta;
      lookup[userId][infra][gpuType].jobCount += jobDelta;
      lookup[userId][infra][gpuType].gpuCount += gpuDelta;

      if (!lookup[userId]['Total'][gpuType]) {
        lookup[userId]['Total'][gpuType] = {
          clusterCount: 0,
          jobCount: 0,
          gpuCount: 0,
        };
      }
      lookup[userId]['Total'][gpuType].clusterCount += clusterDelta;
      lookup[userId]['Total'][gpuType].jobCount += jobDelta;
      lookup[userId]['Total'][gpuType].gpuCount += gpuDelta;
    }
  };

  for (const cluster of clusters) {
    const userId = cluster.user_hash;
    if (!userId) continue;

    const gpuType = extractGPUType(cluster.gpus);
    let gpuCount = 0;
    if (cluster.status !== 'STOPPED' && cluster.status !== 'TERMINATED') {
      const gpuCountPerNode = getGPUCount(
        cluster.gpus,
        `Cluster ${cluster.cluster}`
      );
      gpuCount = gpuCountPerNode * (cluster.num_nodes || 1);
    }
    updateLookup(userId, cluster.infra, gpuType, 1, 0, gpuCount);
  }

  for (const job of jobs) {
    if (!ACTIVE_JOB_STATUSES.has(job.status)) continue;

    const userId = job.user_hash;
    if (!userId) continue;

    updateLookup(
      userId,
      job.infra,
      extractGPUType(job.accelerators),
      0,
      1,
      getJobGpuCount(job)
    );
  }

  return lookup;
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
