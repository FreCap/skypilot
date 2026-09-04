'use client';

import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { showToast } from '@/data/connectors/toast';
import { apiClient } from '@/data/connectors/client';
import { ENDPOINT } from '@/data/connectors/constants';
import dashboardCache from '@/lib/cache';
import { applyEnhancements } from '@/plugins/dataEnhancement';
import { trackClusterAction } from '@/lib/analytics';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';

// ============ Pagination Plugin Integration ============

/**
 * Check if the pagination plugin is available.
 * The plugin sets window.__skyPaginationFetch when loaded.
 * With requires_early_init=True, the plugin is guaranteed to be
 * loaded before any API calls complete.
 */
function isPaginationPluginAvailable() {
  return (
    typeof window !== 'undefined' &&
    typeof window.__skyPaginationFetch === 'function'
  );
}

/**
 * Get the pagination plugin fetch function
 */
function getPaginationFetch() {
  return typeof window !== 'undefined' ? window.__skyPaginationFetch : null;
}

const DEFAULT_TAIL_LINES = 5000;

/**
 * Truncates a string in the middle, preserving parts from beginning and end.
 * @param {string} str - The string to truncate
 * @param {number} maxLength - Maximum length of the truncated string
 * @return {string} - Truncated string
 */
function truncateMiddle(str, maxLength = 15) {
  if (!str || str.length <= maxLength) return str;

  // Reserve 3 characters for '...'
  if (maxLength <= 3) return '...';

  // Calculate how many characters to keep from beginning and end
  const halfLength = Math.floor((maxLength - 3) / 2);
  const remainder = (maxLength - 3) % 2;

  // Keep one more character at the beginning if maxLength - 3 is odd
  const startLength = halfLength + remainder;
  const endLength = halfLength;

  // When endLength is 0, just show the start part and '...'
  if (endLength === 0) {
    return str.substring(0, startLength) + '...';
  }

  return (
    str.substring(0, startLength) +
    '...' +
    str.substring(str.length - endLength)
  );
}

const clusterStatusMap = {
  UP: 'RUNNING',
  STOPPED: 'STOPPED',
  INIT: 'LAUNCHING',
  // Cluster is executing pre-stop hooks and about to stop/tear down
  // (sky/utils/status_lib.py ClusterStatus.AUTOSTOPPING). Without this
  // entry the badge rendered with an undefined label (a bare dot).
  AUTOSTOPPING: 'AUTOSTOPPING',
  PENDING: 'PENDING',
  null: 'TERMINATED',
};

export async function getClusters({
  clusterNames = null,
  workspaces = null,
} = {}) {
  try {
    const clusters = await apiClient.fetch('/status', {
      cluster_names: clusterNames,
      workspaces_filter: workspaces,
      all_users: true,
      include_credentials: false,
      include_handle: false,
      summary_response: clusterNames == null,
    });

    const clusterData = clusters.map((cluster) => {
      // Use cluster_hash for lookup, assuming it's directly in cluster.cluster_hash
      let region_or_zone = '';
      if (cluster.zone) {
        region_or_zone = cluster.zone;
      } else {
        region_or_zone = cluster.region;
      }
      // For SSH Node Pools, strip the 'ssh-' prefix from region display
      // to avoid redundant "SSH (ssh-poolname)" showing as "SSH (poolname)"
      if (cluster.cloud === 'SSH' && region_or_zone?.startsWith('ssh-')) {
        region_or_zone = region_or_zone.substring(4);
      }
      // Store the full value before truncation
      const full_region_or_zone = region_or_zone;
      // Truncate region_or_zone in the middle if it's too long
      if (region_or_zone && region_or_zone.length > 25) {
        region_or_zone = truncateMiddle(region_or_zone, 25);
      }
      return {
        // Fall back to the raw status so an unmapped enum value still
        // renders a labeled badge instead of a bare dot.
        status: clusterStatusMap[cluster.status] || cluster.status,
        cluster: cluster.name,
        user: cluster.user_name,
        user_hash: cluster.user_hash,
        cluster_hash: cluster.cluster_hash,
        cloud: cluster.cloud,
        region: cluster.region,
        infra: region_or_zone
          ? cluster.cloud + ' (' + region_or_zone + ')'
          : cluster.cloud,
        full_infra: full_region_or_zone
          ? `${cluster.cloud} (${full_region_or_zone})`
          : cluster.cloud,
        cpus: cluster.cpus,
        mem: cluster.memory,
        gpus: cluster.accelerators,
        resources_str: cluster.resources_str,
        resources_str_full: cluster.resources_str_full,
        time: new Date(cluster.launched_at * 1000),
        num_nodes: cluster.nodes,
        workspace: cluster.workspace,
        autostop: cluster.autostop,
        last_event: cluster.last_event,
        statusTooltip:
          cluster.status === 'INIT' ? cluster.launch_status_reason : null,
        to_down: cluster.to_down,
        cluster_name_on_cloud: cluster.cluster_name_on_cloud,
        labels: cluster.labels || {},
        node_names: cluster.node_names || null,
        // Persisted external links from the cluster row (currently
        // populated with cloud-provider instance console URLs at launch
        // time). Shape: {label: url}.
        links: cluster.links || {},
        jobs: [],
        command: cluster.last_creation_command || cluster.last_use,
        task_yaml: cluster.last_creation_yaml || '{}',
        events: [
          {
            time: new Date(cluster.launched_at * 1000),
            event: 'Cluster created.',
          },
        ],
      };
    });

    // Apply plugin data enhancements
    // Pass raw backend data so enhancements can extract fields directly
    const enhancedClusters = await applyEnhancements(clusterData, 'clusters', {
      dashboardCache,
      rawData: clusters, // Raw backend response for field extraction
    });

    return enhancedClusters;
  } catch (error) {
    console.error('Error fetching clusters:', error);
    throw error;
  }
}

export async function getClusterHistory(
  clusterHash = null,
  days = 30,
  clusterName = null
) {
  try {
    const requestBody = {
      days: days,
      dashboard_summary_response: true,
      // Hide clusters that back managed jobs/services from the history view.
      // These controller-launched clusters are already excluded from the
      // active cluster list (sky.core.status), so excluding them here keeps
      // the "Show history" view consistent.
      exclude_managed_clusters: true,
    };

    // If a specific cluster hash is provided, include it in the request
    if (clusterHash) {
      requestBody.cluster_hashes = [clusterHash];
    }
    // The server filters on hash OR name when both are set, which lets the
    // dashboard look up a cluster by either identifier in a single call.
    // This avoids fetching the entire history (potentially tens of
    // thousands of rows) just to resolve a name-keyed URL.
    if (clusterName) {
      requestBody.cluster_names = [clusterName];
    }

    const history = await apiClient.fetch('/cost_report', requestBody);

    const historyData = history.map((cluster) => {
      // Get cloud name from resources if available
      let cloud = 'Unknown';
      if (cluster.cloud) {
        cloud = cluster.cloud;
      } else if (cluster.resources && cluster.resources.cloud) {
        cloud = cluster.resources.cloud;
      }

      // Get user name - need to look up from user_hash if needed
      let user_name = cluster.user_name || '-';

      // Extract resource info

      return {
        status: cluster.status
          ? clusterStatusMap[cluster.status] || cluster.status
          : 'TERMINATED',
        cluster: cluster.name,
        user: user_name,
        user_hash: cluster.user_hash,
        cluster_hash: cluster.cluster_hash,
        cloud: cloud,
        region: '',
        infra: cloud,
        full_infra: cloud,
        resources_str: cluster.resources_str,
        resources_str_full: cluster.resources_str_full,
        time: cluster.launched_at ? new Date(cluster.launched_at * 1000) : null,
        num_nodes: cluster.num_nodes || 1,
        duration: cluster.duration,
        total_cost: cluster.total_cost,
        workspace: cluster.workspace || 'default',
        autostop: -1,
        last_event: cluster.last_event,
        to_down: false,
        cluster_name_on_cloud: null,
        node_names: cluster.node_names || null,
        usage_intervals: cluster.usage_intervals,
        command: cluster.last_creation_command || '',
        task_yaml: cluster.last_creation_yaml || '{}',
        events: [
          {
            time: cluster.launched_at
              ? new Date(cluster.launched_at * 1000)
              : new Date(),
            event: 'Cluster created.',
          },
        ],
      };
    });

    // Apply plugin data enhancements
    // Pass raw backend data so enhancements can extract fields directly
    const enhancedHistory = await applyEnhancements(historyData, 'clusters', {
      dashboardCache,
      rawData: history, // Raw backend response for field extraction
    });
    return enhancedHistory;
  } catch (error) {
    console.error('Error fetching cluster history:', error);
    throw error;
  }
}

export async function streamClusterJobLogs({
  clusterName,
  jobId,
  onNewLog,
  workspace,
  signal,
  tail = DEFAULT_TAIL_LINES,
}) {
  try {
    await apiClient.stream(
      '/logs',
      {
        follow: false,
        cluster_name: clusterName,
        job_id: jobId,
        tail,
        override_skypilot_config: {
          active_workspace: workspace || 'default',
        },
      },
      onNewLog,
      { signal }
    );
  } catch (error) {
    // Abort is an expected control path (e.g., user refresh/navigation).
    if (error?.name === 'AbortError') {
      return;
    }
    console.error('Error in streamClusterJobLogs:', error);
    showToast(`Error in streamClusterJobLogs: ${error.message}`, 'error');
  }
}

export async function streamClusterProvisionLogs({
  clusterName,
  worker = null,
  onNewLog,
  signal,
}) {
  try {
    // provision_logs takes follow and tail as query params, not body fields.
    const params = `follow=false&tail=${DEFAULT_TAIL_LINES}`;
    const body = { cluster_name: clusterName };
    if (worker !== null) {
      body.worker = worker;
    }
    await apiClient.stream(`/provision_logs?${params}`, body, onNewLog, {
      signal,
    });
  } catch (error) {
    if (error?.name === 'AbortError') {
      return;
    }
    console.error('Error in streamClusterProvisionLogs:', error);
    showToast(`Error fetching provision logs: ${error.message}`, 'error');
  }
}

/**
 * Downloads job logs as a zip via the API server.
 * Flow:
 * 1) POST /download_logs to fetch logs from the remote cluster to API server
 * 2) POST /download to stream a zip back to the browser and trigger download
 */
export async function downloadJobLogs({
  clusterName,
  jobIds = null,
  workspace,
}) {
  try {
    // Step 1: schedule server-side download; result is a mapping job_id -> folder path on API server
    const mapping = await apiClient.fetch('/download_logs', {
      cluster_name: clusterName,
      job_ids: jobIds ? jobIds.map(String) : null, // Convert to strings as expected by server
      override_skypilot_config: {
        active_workspace: workspace || 'default',
      },
    });

    const folderPaths = Object.values(mapping || {});
    if (!folderPaths.length) {
      showToast('No logs found to download.', 'warning');
      return;
    }

    // Step 2: request the zip and trigger browser download
    const resp = await apiClient.fetchImmediate('/download?relative=items', {
      folder_paths: folderPaths,
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Download failed: ${resp.status} ${text}`);
    }
    const blob = await resp.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const namePart =
      jobIds && jobIds.length === 1 ? `job-${jobIds[0]}` : 'jobs';
    a.href = url;
    const filename = `${clusterName}-${namePart}-logs-${ts}.zip`;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
    trackClusterAction('download_logs', {
      job_count: jobIds?.length ?? 0,
    });
  } catch (error) {
    console.error('Error downloading logs:', error);
    showToast(`Error downloading logs: ${error.message}`, 'error');
  }
}

export async function getClusterJobs({ clusterName, workspace }) {
  try {
    const jobs = await apiClient.fetch('/queue', {
      cluster_name: clusterName,
      all_users: true,
      override_skypilot_config: {
        active_workspace: workspace,
      },
    });

    const jobData = jobs.map((job) => {
      let endTime = job.end_at ? job.end_at : Date.now() / 1000;
      let total_duration = 0;
      let job_duration = 0;
      if (job.submitted_at) {
        total_duration = endTime - job.submitted_at;
      }
      if (job.start_at) {
        job_duration = endTime - job.start_at;
      }

      return {
        id: job.job_id,
        status: job.status,
        job: job.job_name,
        user: job.username,
        user_hash: job.user_hash,
        gpus: job.accelerators || {},
        submitted_at: job.submitted_at
          ? new Date(job.submitted_at * 1000)
          : null,
        resources: job.resources,
        cluster: clusterName,
        total_duration: total_duration,
        job_duration: job_duration,
        infra: '',
        logs: '',
        workspace: workspace || 'default',
        git_commit: job.metadata?.git_commit || '-',
      };
    });
    return jobData;
  } catch (error) {
    console.error('Error fetching cluster jobs:', error);
    throw error;
  }
}

export function useClusterDetails({ cluster }) {
  const [clusterData, setClusterData] = useState(null);
  const [clusterJobData, setClusterJobData] = useState(null);
  const [loadingClusterData, setLoadingClusterData] = useState(true);
  const [loadingClusterJobData, setLoadingClusterJobData] = useState(true);
  const clusterRequestVersionRef = useRef(0);
  const clusterJobsRequestVersionRef = useRef(0);
  const activeClusterRef = useRef(cluster);
  const refreshInFlightRef = useRef(null);
  const clusterJobsRefreshInFlightRef = useRef(null);

  // Separate loading states - cluster details vs cluster jobs
  const clusterDetailsLoading = loadingClusterData;
  const clusterJobsLoading = loadingClusterJobData;

  const fetchClusterData = useCallback(
    async (requestVersion) => {
      const isCurrentRequest = () =>
        clusterRequestVersionRef.current === requestVersion;
      try {
        setLoadingClusterData(true);
        // Use dashboard cache for cluster data
        const data = await dashboardCache.get(getClusters, [
          { clusterNames: [cluster] },
        ]);
        if (!isCurrentRequest()) {
          return null;
        }
        if (data.length > 0) {
          setClusterData(data[0]); // Assuming getClusters returns an array
          return {
            kind: 'found',
            cluster: data[0],
          };
        }
        console.error('No cluster data found for cluster:', cluster);
        return { kind: 'missing' };
      } catch (error) {
        if (isCurrentRequest()) {
          console.error('Error fetching cluster data:', error);
        }
        return { kind: 'error' };
      } finally {
        if (isCurrentRequest()) {
          setLoadingClusterData(false);
        }
      }
    },
    [cluster]
  );

  const fetchClusterJobData = useCallback(
    async (workspace, requestVersion) => {
      const isCurrentRequest = () =>
        clusterJobsRequestVersionRef.current === requestVersion;
      if (!isCurrentRequest()) {
        return;
      }
      try {
        setLoadingClusterJobData(true);
        // Use dashboard cache for cluster jobs
        const data = await dashboardCache.get(getClusterJobs, [
          {
            clusterName: cluster,
            workspace: workspace || 'default',
          },
        ]);
        if (isCurrentRequest()) {
          setClusterJobData(data);
        }
      } catch (error) {
        if (isCurrentRequest()) {
          console.error('Error fetching cluster job data:', error);
        }
      } finally {
        if (isCurrentRequest()) {
          setLoadingClusterJobData(false);
        }
      }
    },
    [cluster]
  );

  const startFetch = useCallback(
    (kind, { invalidateJobs = false } = {}) => {
      const clusterRequestVersion = clusterRequestVersionRef.current + 1;
      clusterRequestVersionRef.current = clusterRequestVersion;
      const clusterJobsRequestVersion =
        clusterJobsRequestVersionRef.current + 1;
      clusterJobsRequestVersionRef.current = clusterJobsRequestVersion;

      if (clusterJobsRefreshInFlightRef.current?.cluster === cluster) {
        clusterJobsRefreshInFlightRef.current = null;
      }

      if (!cluster) {
        setClusterData(null);
        setClusterJobData(null);
        setLoadingClusterData(false);
        setLoadingClusterJobData(false);
        return Promise.resolve();
      }

      if (kind === 'manual') {
        dashboardCache.invalidate(getClusters, [{ clusterNames: [cluster] }]);
      }

      // The jobs request cannot start until the cluster workspace is known, but
      // its loading state belongs to this request chain immediately.
      setLoadingClusterJobData(true);
      let refreshPromise;
      refreshPromise = (async () => {
        const clusterRead = await fetchClusterData(clusterRequestVersion);
        if (
          clusterJobsRequestVersionRef.current !== clusterJobsRequestVersion
        ) {
          return;
        }
        if (clusterRead?.kind === 'found') {
          if (invalidateJobs) {
            dashboardCache.invalidate(getClusterJobs, [
              {
                clusterName: cluster,
                workspace: clusterRead.cluster.workspace || 'default',
              },
            ]);
          }
          await fetchClusterJobData(
            clusterRead.cluster.workspace,
            clusterJobsRequestVersion
          );
        } else if (clusterRead?.kind === 'missing') {
          setClusterData(null);
          setClusterJobData(null);
          setLoadingClusterJobData(false);
        } else if (
          clusterJobsRequestVersionRef.current === clusterJobsRequestVersion
        ) {
          setLoadingClusterJobData(false);
        }
      })().finally(() => {
        if (refreshInFlightRef.current?.promise === refreshPromise) {
          refreshInFlightRef.current = null;
        }
      });
      refreshInFlightRef.current = {
        cluster,
        kind,
        promise: refreshPromise,
      };
      return refreshPromise;
    },
    [cluster, fetchClusterData, fetchClusterJobData]
  );

  const fetchData = useCallback(() => {
    const inFlight = refreshInFlightRef.current;
    if (inFlight?.cluster === cluster) {
      return inFlight.promise;
    }
    return startFetch('automatic');
  }, [cluster, startFetch]);

  const refreshData = useCallback(() => {
    const inFlight = refreshInFlightRef.current;
    if (inFlight?.cluster === cluster && inFlight.kind === 'manual') {
      return inFlight.promise;
    }
    return startFetch('manual', { invalidateJobs: true });
  }, [cluster, startFetch]);

  const refreshClusterJobsOnly = useCallback(() => {
    if (!clusterData) {
      return Promise.resolve();
    }

    const inFlight = clusterJobsRefreshInFlightRef.current;
    if (inFlight?.cluster === cluster) {
      return inFlight.promise;
    }

    const requestVersion = clusterJobsRequestVersionRef.current + 1;
    clusterJobsRequestVersionRef.current = requestVersion;
    dashboardCache.invalidate(getClusterJobs, [
      {
        clusterName: cluster,
        workspace: clusterData.workspace || 'default',
      },
    ]);
    let refreshPromise;
    refreshPromise = fetchClusterJobData(
      clusterData.workspace,
      requestVersion
    ).finally(() => {
      if (clusterJobsRefreshInFlightRef.current?.promise === refreshPromise) {
        clusterJobsRefreshInFlightRef.current = null;
      }
    });
    clusterJobsRefreshInFlightRef.current = {
      cluster,
      promise: refreshPromise,
    };
    return refreshPromise;
  }, [fetchClusterJobData, clusterData, cluster]);

  useEffect(() => {
    if (activeClusterRef.current !== cluster) {
      activeClusterRef.current = cluster;
      setClusterData(null);
      setClusterJobData(null);
    }
    fetchData();
    return () => {
      clusterRequestVersionRef.current += 1;
      clusterJobsRequestVersionRef.current += 1;
      if (refreshInFlightRef.current?.cluster === cluster) {
        refreshInFlightRef.current = null;
      }
      if (clusterJobsRefreshInFlightRef.current?.cluster === cluster) {
        clusterJobsRefreshInFlightRef.current = null;
      }
    };
  }, [cluster, fetchData]);

  // Effects run after render. On a route change, do not expose the previous
  // route's details, jobs, or idle loading flags during the render before the
  // effect above advances ownership and clears its state.
  const ownsRouteState = activeClusterRef.current === cluster;

  return {
    clusterData: ownsRouteState ? clusterData : null,
    clusterJobData: ownsRouteState ? clusterJobData : null,
    // Only cluster details loading for initial page render.
    loading: !ownsRouteState || clusterDetailsLoading,
    clusterDetailsLoading: !ownsRouteState || clusterDetailsLoading,
    clusterJobsLoading: !ownsRouteState || clusterJobsLoading,
    refreshData,
    refreshClusterJobsOnly,
  };
}

// ============ useClusterData Hook ============

/**
 * Hook for cluster data with pagination support.
 * If the pagination plugin is available, uses server-side pagination.
 * Otherwise, falls back to client-side pagination with getClusters.
 *
 * With requires_early_init=True, the plugin is guaranteed to be loaded
 * before the first API call completes, so we just need a simple check.
 *
 * @param {Object} options - Hook options
 * @param {boolean} options.showHistory - Whether to include historical clusters
 * @param {number} options.historyDays - Number of days of history to fetch
 * @param {number} options.refreshInterval - Auto-refresh interval in ms
 * @returns {Object} Cluster data with pagination state and actions
 */
export function useClusterData(options = {}) {
  const {
    showHistory = false,
    historyDays = 1,
    refreshInterval = null,
    sortConfig = { key: null, direction: 'ascending' },
    filters = [],
  } = options;

  // Convert sortConfig to API format
  // Default to launched_at desc (newest first) when no sort is specified
  const sortBy = sortConfig.key || 'launched_at';
  const sortOrder = sortConfig.key
    ? sortConfig.direction === 'ascending'
      ? 'asc'
      : 'desc'
    : 'desc'; // Default to desc when no sort key selected

  // Serialize filters for stable dependency comparison
  const filtersKey = JSON.stringify(filters);

  const { initialPage = 1, initialLimit = 10 } = options;

  const [data, setData] = useState([]);
  const [fullData, setFullData] = useState([]); // Full dataset for client-side filtering
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(initialPage);
  const [limit, setLimit] = useState(initialLimit);
  const [total, setTotal] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [hasNext, setHasNext] = useState(false);
  const [hasPrev, setHasPrev] = useState(false);
  const [error, setError] = useState(null);
  const [isServerPagination, setIsServerPagination] = useState(false);
  const isInitialMount = useRef(true);
  const previousFiltersKeyRef = useRef(filtersKey);
  const requestVersionRef = useRef(0);
  const refreshInFlightRef = useRef(null);
  const paginationPluginAvailable = isPaginationPluginAvailable();
  const filtersChanged = previousFiltersKeyRef.current !== filtersKey;
  const fetchEffectPageToken = paginationPluginAvailable ? page : null;
  const serverFiltersChanged = paginationPluginAvailable
    ? filtersChanged
    : false;
  const shouldDelayServerFetchForPageReset =
    serverFiltersChanged && fetchEffectPageToken !== 1;
  const serverRequestArgs = useMemo(
    () => ({
      page,
      limit,
      showHistory,
      historyDays,
      sortBy,
      sortOrder,
      filters,
    }),
    [page, limit, showHistory, historyDays, sortBy, sortOrder, filters]
  );
  const requestContext = useMemo(
    () =>
      paginationPluginAvailable
        ? JSON.stringify(serverRequestArgs)
        : JSON.stringify({
            showHistory,
            historyDays,
          }),
    [historyDays, paginationPluginAvailable, serverRequestArgs, showHistory]
  );

  // Reset to page 1 when filters change, but skip on initial mount
  // so the page number read from the URL isn't overwritten.
  useEffect(() => {
    if (isInitialMount.current) {
      isInitialMount.current = false;
      previousFiltersKeyRef.current = filtersKey;
      return;
    }
    if (filtersChanged) {
      previousFiltersKeyRef.current = filtersKey;
      setPage(1);
    }
  }, [filtersChanged, filtersKey]);

  /**
   * Fetch clusters using server-side pagination (plugin path)
   */
  const fetchServerSide = useCallback(
    async (isCurrentRequest) => {
      console.log('[useClusterData] Using server-side pagination');
      const pluginFetch = getPaginationFetch();

      const result = await dashboardCache.get(pluginFetch, [serverRequestArgs]);
      if (!isCurrentRequest()) {
        return;
      }

      const resultTotal = result.total || 0;
      const resultTotalPages = result.totalPages || result.total_pages || 1;
      const resultHasNext = result.hasNext || result.has_next || false;
      const resultHasPrev = result.hasPrev || result.has_prev || false;
      const resultData = result.items || result.data || [];

      setData(resultData);
      setFullData(resultData);
      setTotal(resultTotal);
      setTotalPages(resultTotalPages);
      setHasNext(resultHasNext);
      setHasPrev(resultHasPrev);
      setIsServerPagination(true);

      // Prefetch next page in background if there is one
      if (resultHasNext) {
        const nextPageOptions = {
          ...serverRequestArgs,
          page: page + 1,
        };
        dashboardCache
          .get(pluginFetch, [nextPageOptions], { ttl: 30000 })
          .then(() => console.log('[useClusterData] Prefetched page', page + 1))
          .catch((err) =>
            console.warn('[useClusterData] Prefetch failed:', err)
          );
      }
    },
    [page, serverRequestArgs]
  );

  /**
   * Fetch clusters using client-side pagination (default path)
   */
  const fetchClientSide = useCallback(
    async (isCurrentRequest) => {
      console.log('[useClusterData] Using client-side pagination');

      let allClusters;
      if (showHistory) {
        let historyClusters = [];
        try {
          historyClusters = await dashboardCache.get(getClusterHistory, [
            null,
            historyDays,
          ]);
        } catch (historyError) {
          if (isCurrentRequest()) {
            console.error('Error fetching cluster history:', historyError);
          }
        }

        // "Show history" surfaces only truly terminated clusters within the
        // selected time window. cost_report also returns active clusters, so
        // drop anything still present in cluster_table (status !== TERMINATED).
        allClusters = historyClusters
          .filter((c) => c.status === 'TERMINATED')
          .map((c) => ({ ...c, isHistorical: true }));
      } else {
        const activeClusters = await dashboardCache.get(getClusters);
        allClusters = activeClusters.map((c) => ({
          ...c,
          isHistorical: false,
        }));
      }
      if (!isCurrentRequest()) {
        return;
      }

      setFullData(allClusters);
      setIsServerPagination(false);
    },
    [showHistory, historyDays]
  );

  const fetchCurrentMode = useMemo(
    () => (paginationPluginAvailable ? fetchServerSide : fetchClientSide),
    [fetchClientSide, fetchServerSide, paginationPluginAvailable]
  );
  const invalidateServerContext = useCallback(() => {
    const pluginFetch = getPaginationFetch();
    if (pluginFetch) {
      dashboardCache.invalidate(pluginFetch, [serverRequestArgs]);
    }
  }, [serverRequestArgs]);
  const invalidateClientContext = useCallback(() => {
    if (showHistory) {
      dashboardCache.invalidate(getClusterHistory, [null, historyDays]);
      return;
    }
    dashboardCache.invalidate(getClusters, []);
  }, [historyDays, showHistory]);
  const invalidateCurrentContext = useMemo(
    () =>
      paginationPluginAvailable
        ? invalidateServerContext
        : invalidateClientContext,
    [
      invalidateClientContext,
      invalidateServerContext,
      paginationPluginAvailable,
    ]
  );

  const startFetch = useCallback(
    (kind) => {
      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion;
      setLoading(true);
      setError(null);
      if (kind === 'manual') {
        invalidateCurrentContext();
      }

      let refreshPromise;
      refreshPromise = (async () => {
        try {
          await fetchCurrentMode(isCurrentRequest);
        } catch (fetchError) {
          if (isCurrentRequest()) {
            console.error(
              '[useClusterData] Error fetching clusters:',
              fetchError
            );
            setError(fetchError);
            setData([]);
            setFullData([]);
          }
        } finally {
          if (isCurrentRequest()) {
            setLoading(false);
          }
        }
      })().finally(() => {
        if (refreshInFlightRef.current?.promise === refreshPromise) {
          refreshInFlightRef.current = null;
        }
      });
      refreshInFlightRef.current = {
        context: requestContext,
        kind,
        promise: refreshPromise,
      };
      return refreshPromise;
    },
    [fetchCurrentMode, invalidateCurrentContext, requestContext]
  );

  /**
   * Automatic loads and interval ticks never supersede current work for the
   * same context. This bounds pending hook continuations during slow reads.
   */
  const fetchData = useCallback(() => {
    const inFlight = refreshInFlightRef.current;
    if (inFlight?.context === requestContext) {
      return inFlight.promise;
    }
    return startFetch('automatic');
  }, [requestContext, startFetch]);

  /**
   * The first explicit refresh supersedes an automatic load, preserving the
   * user's request for a newer snapshot. Further explicit callers reuse that
   * manual owner until it settles.
   */
  const refreshData = useCallback(() => {
    const inFlight = refreshInFlightRef.current;
    if (inFlight?.context === requestContext && inFlight.kind === 'manual') {
      return inFlight.promise;
    }
    return startFetch('manual');
  }, [requestContext, startFetch]);

  // Fetch data on mount and when dependencies change
  useEffect(() => {
    if (shouldDelayServerFetchForPageReset) {
      return;
    }
    fetchData();
    return () => {
      requestVersionRef.current += 1;
      if (refreshInFlightRef.current?.context === requestContext) {
        refreshInFlightRef.current = null;
      }
    };
  }, [
    fetchData,
    fetchEffectPageToken,
    requestContext,
    shouldDelayServerFetchForPageReset,
  ]);

  const refreshWhenVisible = useCallback(
    (source) => {
      if (source === 'visibilitychange') {
        void refreshData();
        return;
      }
      void fetchData();
    },
    [fetchData, refreshData]
  );
  useVisibleRefreshInterval(
    Boolean(refreshInterval),
    refreshInterval,
    refreshWhenVisible
  );

  // Handle limit change - reset to page 1
  const handleSetLimit = useCallback((newLimit) => {
    setLimit(newLimit);
    setPage(1);
  }, []);

  const clientTotalPages = Math.ceil(fullData.length / limit) || 1;
  const clientPage = Math.min(page, clientTotalPages);
  const clientStartIndex = (clientPage - 1) * limit;
  const clientData = fullData.slice(clientStartIndex, clientStartIndex + limit);
  const visibleClientPage = loading ? page : clientPage;

  useEffect(() => {
    if (!paginationPluginAvailable && !loading && page !== clientPage) {
      setPage(clientPage);
    }
  }, [clientPage, loading, page, paginationPluginAvailable]);

  return {
    // Data - current page slice (paginated)
    data: paginationPluginAvailable ? data : clientData,
    // allData - full dataset for client-side filtering (in server mode, same as data)
    allData: fullData,
    total: paginationPluginAvailable ? total : fullData.length,

    // Pagination state
    page: paginationPluginAvailable ? page : visibleClientPage,
    limit,
    totalPages: paginationPluginAvailable ? totalPages : clientTotalPages,
    hasNext: paginationPluginAvailable
      ? hasNext
      : visibleClientPage < clientTotalPages,
    hasPrev: paginationPluginAvailable ? hasPrev : visibleClientPage > 1,

    // Pagination actions
    setPage,
    setLimit: handleSetLimit,

    // Other
    loading,
    error,
    refresh: refreshData,
    isServerPagination: paginationPluginAvailable,
  };
}
