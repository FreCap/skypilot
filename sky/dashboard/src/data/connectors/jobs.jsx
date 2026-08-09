import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { showToast } from '@/data/connectors/toast';
import {
  CLUSTER_NOT_UP_ERROR,
  CLUSTER_DOES_NOT_EXIST,
  NOT_SUPPORTED_ERROR,
} from '@/data/connectors/constants';
import dashboardCache from '@/lib/cache';
import jobsCacheManager from '@/lib/jobs-cache-manager';
import { apiClient } from './client';
import { applyEnhancements } from '@/plugins/dataEnhancement';

// ============ Pagination Plugin Integration ============

/**
 * Check if the jobs pagination plugin is available.
 * The plugin sets window.__skyJobsPaginationFetch when loaded.
 * With requires_early_init=True, the plugin is guaranteed to be
 * loaded before any API calls complete.
 */
function isJobsPaginationPluginAvailable() {
  return (
    typeof window !== 'undefined' &&
    typeof window.__skyJobsPaginationFetch === 'function'
  );
}

/**
 * Get the jobs pagination plugin fetch function
 */
function getJobsPaginationFetch() {
  return typeof window !== 'undefined' ? window.__skyJobsPaginationFetch : null;
}

// Configuration
const DEFAULT_FIELDS = [
  'job_id',
  '_job_id',
  'job_name',
  'user_name',
  'user_hash',
  'workspace',
  'submitted_at',
  'job_duration',
  'status',
  'resources',
  'cloud',
  'region',
  'accelerators',
  'cluster_resources',
  'cluster_resources_full',
  'recovery_count',
  'pool',
  'pool_hash',
  'details',
  'failure_reason',
  'user_yaml',
  'entrypoint',
  'is_job_group',
  'execution',
  'is_primary_in_job_group',
  'links',
  'is_batch',
  'batch_total_batches',
  'batch_completed_batches',
  'node_names',
  'priority_class',
];

/**
 * Compute the job group status based on primary tasks.
 * For job groups with primary/auxiliary tasks, the job status is determined
 * only by the primary tasks. If all primary tasks succeed, the job is
 * considered successful even if auxiliary tasks were cancelled.
 *
 * Uses is_primary_in_job_group per task:
 * - null: Non-job-group task (counts for status)
 * - true: Primary task in job group (counts for status)
 * - false: Auxiliary task in job group (does not count for status)
 *
 * @param {Array} tasks - Array of task objects with status and is_primary_in_job_group fields
 * @returns {string} - The computed job group status
 */
export function computeJobGroupStatus(tasks) {
  if (!tasks || tasks.length === 0) {
    return null;
  }

  // Filter to only primary tasks for status determination.
  // is_primary_in_job_group: true/false for job groups, null for non-groups.
  // For non-job-groups (null), all tasks count for status.
  // For job groups, only tasks with is_primary_in_job_group=true count.
  const primaryTasks = tasks.filter(
    (t) =>
      t.is_primary_in_job_group === null ||
      t.is_primary_in_job_group === undefined ||
      t.is_primary_in_job_group === true
  );

  // Use primary tasks for status; fall back to all tasks if none match
  const tasksForStatus = primaryTasks.length > 0 ? primaryTasks : tasks;

  // Return the first non-SUCCEEDED status, or SUCCEEDED if all succeeded
  for (const task of tasksForStatus) {
    if (task.status !== 'SUCCEEDED') {
      return task.status;
    }
  }
  return 'SUCCEEDED';
}

export async function getManagedJobs(options = {}) {
  try {
    const {
      allUsers = true,
      skipFinished = false,
      allFields = false,
      jobIdMatch,
      nameMatch,
      userMatch,
      workspaceMatch,
      poolMatch,
      page,
      limit,
      statuses,
      fields,
      jobIDs,
    } = options;

    const body = {
      all_users: allUsers,
      verbose: true,
      skip_finished: skipFinished,
    };
    if (nameMatch !== undefined) body.name_match = nameMatch;
    if (userMatch !== undefined) body.user_match = userMatch;
    if (workspaceMatch !== undefined) body.workspace_match = workspaceMatch;
    if (poolMatch !== undefined) body.pool_match = poolMatch;
    if (page !== undefined) body.page = page;
    if (limit !== undefined) body.limit = limit;
    if (statuses !== undefined && statuses.length > 0) body.statuses = statuses;
    // Support both jobIdMatch (from filter UI) and jobIDs (direct usage)
    const resolvedJobIDs = jobIdMatch ? [jobIdMatch] : jobIDs;
    if (resolvedJobIDs !== undefined && resolvedJobIDs.length > 0)
      body.job_ids = resolvedJobIDs;
    if (!allFields) {
      if (fields && fields.length > 0) {
        body.fields = fields;
      } else {
        body.fields = DEFAULT_FIELDS;
      }
    }

    const response = await apiClient.post(`/jobs/queue/v2`, body);
    if (!response.ok) {
      const msg = `Failed to get managed jobs with status ${response.status}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    // Handle empty request ID
    if (!id) {
      const msg = 'No request ID received from server for managed jobs';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    let errorMessage = fetchedData.statusText;
    if (fetchedData.status === 500) {
      try {
        const data = await fetchedData.json();
        if (data.detail && data.detail.error) {
          try {
            const error = JSON.parse(data.detail.error);
            // Handle specific error types
            if (error.type && error.type === CLUSTER_NOT_UP_ERROR) {
              return { jobs: [], total: 0, controllerStopped: true };
            } else {
              errorMessage = error.message || String(data.detail.error);
            }
          } catch (jsonError) {
            console.error(
              'Error parsing JSON from data.detail.error:',
              jsonError
            );
            errorMessage = String(data.detail.error);
          }
        }
      } catch (parseError) {
        console.error('Error parsing response JSON:', parseError);
        errorMessage = String(parseError);
      }
    }
    // Handle all error status codes (4xx, 5xx, etc.)
    if (!fetchedData.ok) {
      const msg = `API request to get managed jobs result failed with status ${fetchedData.status}, error: ${errorMessage}`;
      throw new Error(msg);
    }
    // print out the response for debugging
    const data = await fetchedData.json();
    const parsed = data.return_value ? JSON.parse(data.return_value) : [];
    const managedJobs = Array.isArray(parsed) ? parsed : parsed?.jobs || [];
    const total = Array.isArray(parsed)
      ? managedJobs.length
      : (parsed?.total ?? managedJobs.length);
    const totalNoFilter = parsed?.total_no_filter || total;
    const statusCounts = parsed?.status_counts || {};

    // Process jobs data
    const jobData = managedJobs.map((job) => {
      let total_duration = 0;
      if (job.end_at && job.submitted_at) {
        total_duration = job.end_at - job.submitted_at;
      } else if (job.submitted_at) {
        total_duration = Date.now() / 1000 - job.submitted_at;
      }

      const events = [];
      if (job.submitted_at) {
        events.push({
          type: 'PENDING',
          timestamp: job.submitted_at,
        });
      }
      if (job.start_at) {
        events.push({
          type: 'RUNNING',
          timestamp: job.start_at,
        });
      }
      if (job.end_at) {
        events.push({
          type: job.status,
          timestamp: job.end_at,
        });
      }

      let cloud = '';
      let region = '';
      let cluster_resources = '';
      let infra = '';
      let full_infra = '';

      try {
        cloud = job.cloud || '';
        cluster_resources = job.cluster_resources;
        region = job.region || '';
        if (region === '-') {
          region = '';
        }

        if (cloud) {
          infra = cloud;
          if (region) {
            infra += ` (${region})`;
          }
        }

        full_infra = infra;
        if (job.accelerators) {
          const accel_str = Object.entries(job.accelerators)
            .map(([key, value]) => `${value}x${key}`)
            .join(', ');
          if (accel_str) {
            full_infra += ` (${accel_str})`;
          }
        }
      } catch (e) {
        cluster_resources = job.cluster_resources;
      }

      return {
        id: job.job_id,
        task_job_id: job._job_id,
        task: job.task_name,
        name: job.job_name,
        job_duration: job.job_duration,
        total_duration: total_duration,
        workspace: job.workspace,
        status: job.status,
        requested_resources: job.resources,
        resources_str: cluster_resources,
        resources_str_full: job.cluster_resources_full || cluster_resources,
        cloud: cloud,
        region: job.region,
        infra: infra,
        full_infra: full_infra,
        recoveries: job.recovery_count,
        details: job.details || job.failure_reason,
        user: job.user_name,
        user_hash: job.user_hash,
        submitted_at: job.submitted_at
          ? new Date(job.submitted_at * 1000)
          : null,
        events: events,
        dag_yaml: job.user_yaml,
        entrypoint: job.entrypoint,
        git_commit: job.metadata?.git_commit || '-',
        links: job.links || {},
        pool: job.pool,
        pool_hash: job.pool_hash,
        schedule_state: job.schedule_state,
        current_cluster_name: job.current_cluster_name,
        cluster_name_on_cloud: job.cluster_name_on_cloud,
        job_id_on_pool_cluster: job.job_id_on_pool_cluster,
        accelerators: job.accelerators, // Include accelerators field
        labels: job.labels || {}, // Include labels field
        node_names: job.node_names, // Node names for dashboard display
        // JobGroup fields
        is_job_group: job.is_job_group,
        execution: job.execution,
        is_primary_in_job_group: job.is_primary_in_job_group,
        // Batch progress
        batch_total_batches: job.batch_total_batches,
        batch_completed_batches: job.batch_completed_batches,
      };
    });

    // Apply plugin data enhancements
    // Pass raw backend data so enhancements can extract fields directly
    const enhancedJobs = await applyEnhancements(jobData, 'jobs', {
      dashboardCache,
      rawData: managedJobs, // Raw backend response for field extraction
    });

    return {
      jobs: enhancedJobs,
      total,
      totalNoFilter,
      controllerStopped: false,
      statusCounts,
    };
  } catch (error) {
    console.error('Error fetching managed job data:', error);
    // Signal to the cache to not overwrite previously cached data
    throw error;
  }
}

/**
 * Enhanced getManagedJobs function that supports client-side pagination
 * This function fetches all jobs data once and caches it, then performs filtering and pagination on the client side
 * @param {Object} options - Query options
 * @param {boolean} options.allUsers - Whether to fetch jobs for all users
 * @param {string} options.nameMatch - Filter by job name
 * @param {string} options.userMatch - Filter by user
 * @param {string} options.workspaceMatch - Filter by workspace
 * @param {string} options.poolMatch - Filter by pool
 * @param {Array} options.jobIDs - Filter by job IDs
 * @param {number} options.page - Page page (1-based)
 * @param {number} options.limit - Page size
 * @param {Array} options.fields - Fields to return
 * @param {boolean} options.allFields - Whether to return all fields (default: false)
 * @param {boolean} options.useClientPagination - Whether to use client-side pagination (default: true)
 * @returns {Promise<{jobs: Array, total: number, controllerStopped: boolean, __skipCache?: boolean}>}
 */
export async function getManagedJobsWithClientPagination(options) {
  const {
    allUsers = true,
    nameMatch,
    userMatch,
    workspaceMatch,
    poolMatch,
    page = 1,
    limit = 10,
    jobIDs,
    fields,
    allFields = false,
    useClientPagination = true,
  } = options || {};

  try {
    // If client pagination is disabled, fall back to server-side pagination
    if (!useClientPagination) {
      return await getManagedJobs(options);
    }

    // Create cache key for full dataset (without pagination params)
    const cacheKey = {
      allUsers,
      nameMatch,
      userMatch,
      workspaceMatch,
      poolMatch,
      jobIDs,
      fields,
      allFields,
    };

    // Fetch all data without pagination parameters
    const fullDataResponse = await getManagedJobs(cacheKey);

    if (fullDataResponse.controllerStopped || !fullDataResponse.jobs) {
      return fullDataResponse;
    }

    const allJobs = fullDataResponse.jobs;
    const total = allJobs.length;

    // Apply client-side pagination
    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    const paginatedJobs = allJobs.slice(startIndex, endIndex);

    return {
      jobs: paginatedJobs,
      total: total,
      controllerStopped: false,
    };
  } catch (error) {
    console.error(
      'Error fetching managed job data with client pagination:',
      error
    );
    throw error;
  }
}

export async function getPoolStatus({
  poolNames = null,
  includeJobCounts = true,
} = {}) {
  try {
    const response = await apiClient.post(`/jobs/pool_status`, {
      pool_names: poolNames,
    });
    if (!response.ok) {
      const msg = `Initial API request to get pool status failed with status ${response.status}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for getting pool status';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    let errorMessage = fetchedData.statusText;
    if (fetchedData.status === 500) {
      try {
        const data = await fetchedData.json();
        if (data.detail && data.detail.error) {
          try {
            const error = JSON.parse(data.detail.error);
            if (error.type && error.type === CLUSTER_NOT_UP_ERROR) {
              return { pools: [], controllerStopped: true };
            } else {
              errorMessage = error.message || String(data.detail.error);
            }
          } catch (jsonError) {
            console.error(
              'Error parsing JSON from data.detail.error:',
              jsonError
            );
            errorMessage = String(data.detail.error);
          }
        }
      } catch (dataError) {
        console.error('Error parsing response JSON:', dataError);
        errorMessage = String(dataError);
      }
    }

    if (!fetchedData.ok) {
      const msg = `API request to get pool status result failed with status ${fetchedData.status}, error: ${errorMessage}`;
      throw new Error(msg);
    }

    // Parse the pools data from the response
    const data = await fetchedData.json();
    const poolData = data.return_value ? JSON.parse(data.return_value) : [];

    // Skip the active-jobs fetch entirely when there are no pools — the
    // job counts it computes have nothing to attach to.
    if (poolData.length === 0) {
      return { pools: [], controllerStopped: false };
    }

    const pools = poolData.map((pool) => ({
      ...pool,
      jobCounts:
        includeJobCounts &&
        pool.job_status_counts &&
        typeof pool.job_status_counts === 'object' &&
        !Array.isArray(pool.job_status_counts)
          ? pool.job_status_counts
          : {},
    }));

    return { pools, controllerStopped: false };
  } catch (error) {
    console.error('Error fetching pools:', error);
    throw error;
  }
}

// Read only the pool rows needed to validate links on a managed-job detail
// route. The stable name key prevents unrelated job polling updates from
// repeating the request, while effect cleanup keeps a superseded route from
// publishing its result or error.
export function useManagedJobPools(jobs, jobId) {
  const poolNamesKey = useMemo(() => {
    const poolNames = new Set();
    for (const job of jobs || []) {
      if (String(job.id) === String(jobId) && job.pool) {
        poolNames.add(job.pool);
      }
    }
    return JSON.stringify(Array.from(poolNames).sort());
  }, [jobs, jobId]);
  const [snapshot, setSnapshot] = useState({ key: null, pools: [] });

  useEffect(() => {
    let isCurrentRequest = true;
    const poolNames = JSON.parse(poolNamesKey);
    if (poolNames.length === 0) {
      return () => {
        isCurrentRequest = false;
      };
    }

    async function fetchPoolsData() {
      try {
        const poolsResponse = await dashboardCache.get(getPoolStatus, [
          { poolNames, includeJobCounts: false },
        ]);
        if (isCurrentRequest) {
          setSnapshot({ key: poolNamesKey, pools: poolsResponse.pools || [] });
        }
      } catch (error) {
        if (isCurrentRequest) {
          console.error('Error fetching pools data:', error);
          setSnapshot({ key: poolNamesKey, pools: [] });
        }
      }
    }
    fetchPoolsData();

    return () => {
      isCurrentRequest = false;
    };
  }, [poolNamesKey]);

  return snapshot.key === poolNamesKey ? snapshot.pools : [];
}

// Hook for individual job details that reuses the main jobs cache.
// Returns all tasks for a given job_id (supports multi-task jobs) and one
// promise-returning refresh owner for the current route.
export function useSingleManagedJob(jobId) {
  const [jobData, setJobData] = useState(null);
  const [loadingJobData, setLoadingJobData] = useState(true);
  const requestVersionRef = useRef(0);
  const activeJobIdRef = useRef(jobId);
  const refreshInFlightRef = useRef(null);
  const visibleJobDataRef = useRef(null);

  useEffect(() => {
    visibleJobDataRef.current = jobData;
  }, [jobData]);

  const fetchJobData = useCallback(
    async ({
      forceRefresh = false,
      source = 'refresh',
      supersede = false,
    } = {}) => {
      if (!jobId) return;
      const inFlight = refreshInFlightRef.current;
      const visibleCurrentJobData = visibleJobDataRef.current;
      const hasVisibleCurrentJobData = Boolean(
        visibleCurrentJobData?.jobs?.some(
          (job) => String(job.id) === String(jobId)
        )
      );
      const shouldReuseInFlight =
        inFlight?.jobId === jobId &&
        (!supersede ||
          !hasVisibleCurrentJobData ||
          inFlight.source === 'manual');
      if (shouldReuseInFlight) {
        return inFlight.promise;
      }

      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion;

      try {
        setLoadingJobData(true);

        // Fetch the specific job by ID with all fields for complete data.
        const cacheArgs = [
          { allUsers: true, allFields: true, jobIDs: [jobId] },
        ];
        if (forceRefresh) {
          dashboardCache.invalidate(getManagedJobs, cacheArgs);
        }
        const allJobsData = await dashboardCache.get(getManagedJobs, cacheArgs);
        if (!isCurrentRequest()) {
          return;
        }

        // Filter for ALL tasks matching this job_id (supports multi-task jobs)
        const matchingJobs =
          allJobsData?.jobs?.filter((j) => String(j.id) === String(jobId)) ||
          [];

        if (matchingJobs.length > 0) {
          setJobData({
            jobs: matchingJobs,
            controllerStopped: allJobsData.controllerStopped || false,
          });
        } else {
          // Job not found in the results
          setJobData({
            jobs: [],
            controllerStopped: allJobsData.controllerStopped || false,
          });
        }
      } catch (error) {
        if (!isCurrentRequest()) {
          return;
        }
        console.error('Error fetching single managed job data:', error);
        if (!hasVisibleCurrentJobData) {
          setJobData({ jobs: [], controllerStopped: false });
        }
      } finally {
        if (isCurrentRequest()) {
          setLoadingJobData(false);
        }
      }
    },
    [jobId]
  );

  const refreshJobData = useCallback(() => {
    const refreshPromise = fetchJobData({
      forceRefresh: true,
      source: 'manual',
      supersede: true,
    }).finally(() => {
      if (refreshInFlightRef.current?.promise === refreshPromise) {
        refreshInFlightRef.current = null;
      }
    });
    refreshInFlightRef.current = {
      jobId,
      promise: refreshPromise,
      source: 'manual',
    };
    return refreshPromise;
  }, [fetchJobData, jobId]);

  useEffect(() => {
    if (activeJobIdRef.current !== jobId) {
      activeJobIdRef.current = jobId;
      setJobData(null);
    }
    const loadPromise = fetchJobData({
      source: 'initial',
    }).finally(() => {
      if (refreshInFlightRef.current?.promise === loadPromise) {
        refreshInFlightRef.current = null;
      }
    });
    refreshInFlightRef.current = {
      jobId,
      promise: loadPromise,
      source: 'initial',
    };
    return () => {
      requestVersionRef.current += 1;
      if (refreshInFlightRef.current?.jobId === jobId) {
        refreshInFlightRef.current = null;
      }
    };
  }, [fetchJobData, jobId]);

  // Effects run after render. On a route change, do not expose the previous
  // job's tasks or settled loading state before the effect advances ownership.
  const ownsRouteState = activeJobIdRef.current === jobId;

  return {
    jobData: ownsRouteState ? jobData : null,
    loading: !ownsRouteState || loadingJobData,
    refreshJobData,
  };
}

export {
  streamManagedJobLogs,
  downloadManagedJobLogs,
} from './managed-job-logs';

export async function handleJobAction(action, jobId, cluster) {
  let logStarter = '';
  let logMiddle = '';
  let apiPath = '';
  let requestBody = {};
  switch (action) {
    case 'restartcontroller':
      logStarter = 'Restarting';
      logMiddle = 'restarted';
      apiPath = 'jobs/queue/v2';
      requestBody = {
        all_users: true,
        refresh: true,
        skip_finished: true,
        fields: ['status'],
      };
      jobId = 'controller';
      break;
    default:
      throw new Error(`Invalid action: ${action}`);
  }

  // Show initial notification
  showToast(`${logStarter} job ${jobId}...`, 'info');

  try {
    try {
      const response = await apiClient.fetchImmediate(
        `/${apiPath}`,
        requestBody
      );
      if (!response.ok) {
        console.error(
          `Initial API request ${apiPath} failed with status ${response.status}`
        );
        showToast(
          `${logStarter} job ${jobId} failed with status ${response.status}.`,
          'error'
        );
        return;
      }

      const id = response.headers.get('X-Skypilot-Request-ID');
      if (!id) {
        console.error(`No request ID received from server for ${apiPath}`);
        showToast(
          `${logStarter} job ${jobId} failed with no request ID.`,
          'error'
        );
        return;
      }
      const finalResponse = await apiClient.fetchImmediate(
        `/api/get?request_id=${id}`,
        undefined,
        'GET'
      );

      // Check the status code of the final response
      if (finalResponse.status === 200) {
        showToast(`Job ${jobId} ${logMiddle} successfully.`, 'success');
      } else {
        if (finalResponse.status === 500) {
          try {
            const data = await finalResponse.json();

            if (data.detail && data.detail.error) {
              try {
                const error = JSON.parse(data.detail.error);

                // Handle specific error types
                if (error.type && error.type === NOT_SUPPORTED_ERROR) {
                  showToast(
                    `${logStarter} job ${jobId} is not supported!`,
                    'error',
                    10000
                  );
                } else if (
                  error.type &&
                  error.type === CLUSTER_DOES_NOT_EXIST
                ) {
                  showToast(`Cluster ${cluster} does not exist.`, 'error');
                } else if (error.type && error.type === CLUSTER_NOT_UP_ERROR) {
                  showToast(`Cluster ${cluster} is not up.`, 'error');
                } else {
                  showToast(
                    `${logStarter} job ${jobId} failed: ${error.type}`,
                    'error'
                  );
                }
              } catch (jsonError) {
                showToast(
                  `${logStarter} job ${jobId} failed: ${data.detail.error}`,
                  'error'
                );
              }
            } else {
              showToast(
                `${logStarter} job ${jobId} failed with no details.`,
                'error'
              );
            }
          } catch (parseError) {
            showToast(
              `${logStarter} job ${jobId} failed with parse error.`,
              'error'
            );
          }
        } else {
          showToast(
            `${logStarter} job ${jobId} failed with status ${finalResponse.status}.`,
            'error'
          );
        }
      }
    } catch (fetchError) {
      console.error('Fetch error:', fetchError);
      showToast(
        `Network error ${logStarter} job ${jobId}: ${fetchError.message}`,
        'error'
      );
    }
  } catch (outerError) {
    console.error('Error in handleStop:', outerError);
    showToast(
      `Critical error ${logStarter} job ${jobId}: ${outerError.message}`,
      'error'
    );
  }
}
