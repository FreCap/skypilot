'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import { useRouter } from 'next/router';
import {
  getWorkspaces,
  getEnabledCloudsBatch,
  deleteWorkspace,
} from '@/data/connectors/workspaces';
import { Button } from '@/components/ui/button';
import { CircularProgress } from '@mui/material';
import yaml from 'js-yaml';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import {
  ServerIcon,
  BriefcaseIcon,
  BookDocIcon,
  TickIcon,
} from '@/components/elements/icons';
import { ErrorDisplay } from '@/components/elements/ErrorDisplay';
import { RotateCwIcon } from 'lucide-react';
import { LastUpdatedTimestamp } from '@/components/utils';
import { useMobile } from '@/hooks/useMobile';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';
import { statusGroups } from './job-domain';
import dashboardCache from '@/lib/cache';
import { REFRESH_INTERVALS } from '@/lib/config';
import cachePreloader from '@/lib/cache-preloader';
import { apiClient, getCurrentUserRole } from '@/data/connectors/client';
import { trackWorkspaceAction } from '@/lib/analytics';
import { getClusters } from '@/data/connectors/clusters';
import { getManagedJobs } from '@/data/connectors/jobs';
import { WorkspacesTable } from './workspaces-table';

// Workspace-aware API functions - use cached global data and filter by workspace
// This avoids making separate API calls per workspace
export async function getWorkspaceClusters(workspaceName) {
  try {
    // Use cached global clusters data and filter by workspace
    const allClusters = await dashboardCache.get(getClusters);

    // Filter clusters to only include those that belong to the requested workspace
    const filteredClusters = (allClusters || []).filter(
      (cluster) => cluster.workspace === workspaceName
    );
    return filteredClusters;
  } catch (error) {
    const msg = `Error fetching clusters for workspace ${workspaceName}: ${error}`;
    console.error(msg);
    throw new Error(msg);
  }
}

export async function getWorkspaceManagedJobs(workspaceName) {
  try {
    // Use cached global managed jobs data and filter by workspace
    // This avoids making separate API calls per workspace
    const allJobsData = await dashboardCache.get(getManagedJobs, [
      { allUsers: true, skipFinished: true },
    ]);

    const allJobs = allJobsData?.jobs || [];

    // Filter jobs to only include those that belong to the requested workspace
    const filteredJobs = allJobs.filter(
      (job) => job.workspace === workspaceName
    );

    return { jobs: filteredJobs };
  } catch (error) {
    const msg = `Error fetching managed jobs for workspace ${workspaceName}: ${error}`;
    console.error(msg);
    throw new Error(msg);
  }
}

// Workspace configuration description component
const WorkspaceConfigDescription = ({ workspaceName, config }) => {
  if (!config) return null;

  const isDefault = workspaceName === 'default';
  const isEmptyConfig = Object.keys(config).length === 0;

  if (isDefault && isEmptyConfig) {
    return (
      <div className="text-sm text-gray-500 mb-3 italic p-3 bg-sky-50 rounded border border-sky-200">
        Workspace &apos;default&apos; can use all accessible infrastructure.
      </div>
    );
  }

  const enabledDescriptions = [];
  const disabledClouds = [];

  Object.entries(config).forEach(([cloud, cloudConfig]) => {
    const cloudNameUpper = cloud.toUpperCase();

    if (cloudConfig?.disabled === true) {
      disabledClouds.push(cloudNameUpper);
    } else if (cloudConfig && Object.keys(cloudConfig).length > 0) {
      let detail = '';
      if (cloud.toLowerCase() === 'gcp' && cloudConfig.project_id) {
        detail = ` (Project ID: ${cloudConfig.project_id})`;
      } else if (cloud.toLowerCase() === 'aws' && cloudConfig.region) {
        detail = ` (Region: ${cloudConfig.region})`;
      }
      enabledDescriptions.push(
        <span key={`${cloud}-enabled`} className="block">
          {cloudNameUpper}
          {detail} is enabled.
        </span>
      );
    } else {
      enabledDescriptions.push(
        <span key={`${cloud}-default-enabled`} className="block">
          {cloudNameUpper} is enabled (using default settings).
        </span>
      );
    }
  });

  const finalDescriptions = [];
  if (disabledClouds.length > 0) {
    const disabledString = disabledClouds.join(' and ');
    finalDescriptions.push(
      <span key="disabled-clouds" className="block">
        {disabledString} {disabledClouds.length === 1 ? 'is' : 'are'} explicitly
        disabled.
      </span>
    );
  }
  finalDescriptions.push(...enabledDescriptions);

  if (finalDescriptions.length > 0) {
    return (
      <div className="text-sm text-gray-700 mb-3 p-3 bg-sky-50 rounded border border-sky-200">
        {finalDescriptions}
        <p className="mt-2 text-gray-500">
          Other accessible infrastructure are enabled. See{' '}
          <code className="text-sky-blue">Enabled Infra</code>.
        </p>
      </div>
    );
  }

  if (!isDefault && isEmptyConfig) {
    return (
      <div className="text-sm text-gray-500 mb-3 italic p-3 bg-sky-50 rounded border border-sky-200">
        This workspace has no specific cloud resource configurations and can use
        all accessible infrastructure.
      </div>
    );
  }
  return null;
};

// Statistics summary component
const StatsSummary = ({
  workspaceCount,
  runningClusters,
  totalClusters,
  managedJobs,
  router,
}) => (
  <div className="bg-sky-50 p-4 rounded-lg shadow mb-6">
    <div className="flex flex-col sm:flex-row justify-around items-center">
      <div className="p-2">
        <div className="flex items-center">
          <BookDocIcon className="w-5 h-5 mr-2 text-sky-600" />
          <span className="text-sm text-gray-600">Workspaces:</span>
          <span className="ml-1 text-xl font-semibold text-sky-700">
            {workspaceCount}
          </span>
        </div>
      </div>
      <div className="p-2">
        <div className="flex items-center">
          <ServerIcon className="w-5 h-5 mr-2 text-sky-600" />
          <span className="text-sm text-gray-600">
            Clusters (Running / Total):
          </span>
          <button
            onClick={() => router.push('/clusters')}
            className="ml-1 text-xl font-semibold text-blue-600 hover:text-blue-800 hover:underline cursor-pointer"
          >
            {runningClusters} / {totalClusters}
          </button>
        </div>
      </div>
      <div className="p-2">
        <div className="flex items-center">
          <BriefcaseIcon className="w-5 h-5 mr-2 text-sky-600" />
          <span className="text-sm text-gray-600">Managed Jobs:</span>
          <button
            onClick={() => router.push('/jobs')}
            className="ml-1 text-xl font-semibold text-blue-600 hover:text-blue-800 hover:underline cursor-pointer"
          >
            {managedJobs}
          </button>
        </div>
      </div>
    </div>
  </div>
);

export function Workspaces() {
  const [workspaceDetails, setWorkspaceDetails] = useState([]);
  const [globalStats, setGlobalStats] = useState({
    runningClusters: 0,
    totalClusters: 0,
    managedJobs: 0,
  });
  const [clustersLoading, setClustersLoading] = useState(true);
  const [jobsLoading, setJobsLoading] = useState(true);
  const [rawWorkspacesData, setRawWorkspacesData] = useState(null);
  const [lastFetchedTime, setLastFetchedTime] = useState(null);

  // Track if this is the initial load (controls panel-level vs cell-level spinners)
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const isInitialLoadRef = useRef(true);
  const requestVersionRef = useRef(0);
  const activeRequestRef = useRef(null);
  const manualRefreshOwnerRef = useRef(null);

  // Modal states
  const [isAllWorkspacesModalOpen, setIsAllWorkspacesModalOpen] =
    useState(false);

  // Delete confirmation states
  const [deleteState, setDeleteState] = useState({
    confirmOpen: false,
    workspaceToDelete: null,
    deleting: false,
    error: null,
  });

  // Permission denial dialog state
  const [permissionDenialState, setPermissionDenialState] = useState({
    open: false,
    message: '',
    userName: '',
  });

  const [roleLoading, setRoleLoading] = useState(false);

  // Top-level error and success states
  const [topLevelError, setTopLevelError] = useState(null);
  const [topLevelSuccess, setTopLevelSuccess] = useState(null);

  const router = useRouter();
  const isMobile = useMobile();

  const getUserRole = useCallback(async () => {
    setRoleLoading(true);
    try {
      const roleData = await getCurrentUserRole();
      if (roleData.roleFetchFailed) {
        throw new Error('Failed to get user role');
      }
      return roleData;
    } finally {
      setRoleLoading(false);
    }
  }, []);

  const checkPermissionAndAct = useCallback(
    async (action, actionCallback) => {
      try {
        const roleData = await getUserRole();

        if (roleData.role !== 'admin') {
          setPermissionDenialState({
            open: true,
            message: action,
            userName: roleData.name.toLowerCase(),
          });
          return false;
        }

        actionCallback();
        return true;
      } catch (error) {
        console.error('Failed to check user role:', error);
        setPermissionDenialState({
          open: true,
          message: `Error: ${error.message}`,
          userName: '',
        });
        return false;
      }
    },
    [getUserRole]
  );

  // Fetch clusters independently and update state progressively
  const fetchClustersData = useCallback(
    async (workspaceNames, isCurrentRequest) => {
      try {
        const allClusters = await dashboardCache.get(getClusters);
        if (!isCurrentRequest()) return;

        // Calculate per-workspace cluster stats
        const workspaceClusterStats = {};
        let totalRunningClusters = 0;

        workspaceNames.forEach((wsName) => {
          workspaceClusterStats[wsName] = {
            totalClusterCount: 0,
            runningClusterCount: 0,
          };
        });

        (allClusters || []).forEach((cluster) => {
          const wsName = cluster.workspace || 'default';
          if (!workspaceClusterStats[wsName]) {
            workspaceClusterStats[wsName] = {
              totalClusterCount: 0,
              runningClusterCount: 0,
            };
          }
          workspaceClusterStats[wsName].totalClusterCount++;
          if (cluster.status === 'RUNNING' || cluster.status === 'LAUNCHING') {
            workspaceClusterStats[wsName].runningClusterCount++;
            totalRunningClusters++;
          }
        });

        // Update workspaceDetails with cluster data
        setWorkspaceDetails((prev) => {
          return prev.map((ws) => ({
            ...ws,
            totalClusterCount:
              workspaceClusterStats[ws.name]?.totalClusterCount || 0,
            runningClusterCount:
              workspaceClusterStats[ws.name]?.runningClusterCount || 0,
          }));
        });

        // Update global stats for clusters
        setGlobalStats((prev) => ({
          ...prev,
          runningClusters: totalRunningClusters,
          totalClusters: (allClusters || []).length,
        }));
      } catch (error) {
        if (isCurrentRequest()) {
          console.error('Error fetching clusters:', error);
        }
      } finally {
        if (isCurrentRequest()) {
          setClustersLoading(false);
        }
      }
    },
    []
  );

  // Fetch jobs independently and update state progressively
  const fetchJobsData = useCallback(
    async (workspaceNames, isCurrentRequest) => {
      try {
        const allJobsData = await dashboardCache.get(getManagedJobs, [
          { allUsers: true, skipFinished: true },
        ]);
        if (!isCurrentRequest()) return;
        const jobs = allJobsData?.jobs || [];

        // Calculate per-workspace job stats
        const workspaceJobStats = {};
        const activeJobStatuses = new Set(statusGroups.active);
        let activeGlobalManagedJobs = 0;

        workspaceNames.forEach((wsName) => {
          workspaceJobStats[wsName] = { managedJobsCount: 0 };
        });

        jobs.forEach((job) => {
          const wsName = job.workspace || 'default';
          if (!workspaceJobStats[wsName]) {
            workspaceJobStats[wsName] = { managedJobsCount: 0 };
          }
          if (activeJobStatuses.has(job.status)) {
            workspaceJobStats[wsName].managedJobsCount++;
            activeGlobalManagedJobs++;
          }
        });

        // Update workspaceDetails with job data
        setWorkspaceDetails((prev) => {
          return prev.map((ws) => ({
            ...ws,
            managedJobsCount: workspaceJobStats[ws.name]?.managedJobsCount || 0,
          }));
        });

        // Update global stats for jobs
        setGlobalStats((prev) => ({
          ...prev,
          managedJobs: activeGlobalManagedJobs,
        }));
      } catch (error) {
        if (isCurrentRequest()) {
          console.error('Error fetching jobs:', error);
        }
      } finally {
        if (isCurrentRequest()) {
          setJobsLoading(false);
        }
      }
    },
    []
  );

  const fetchData = useCallback(
    async (options = {}) => {
      const { showLoadingIndicators = true, supersede = false } = options;
      if (!supersede && activeRequestRef.current) {
        return activeRequestRef.current;
      }
      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      const isCurrentRequest = () => {
        return requestVersionRef.current === requestVersion;
      };
      const wasInitialLoad = isInitialLoadRef.current;

      const request = (async () => {
        if (showLoadingIndicators) {
          setClustersLoading(true);
          setJobsLoading(true);
        }

        try {
          // First, get the list of workspaces the user has access to
          const fetchedWorkspacesConfig =
            await dashboardCache.get(getWorkspaces);
          if (!isCurrentRequest()) return false;
          setRawWorkspacesData(fetchedWorkspacesConfig);
          const configuredWorkspaceNames = Object.keys(fetchedWorkspacesConfig);

          // Fetch enabledClouds for all workspaces in a single batch request
          let enabledCloudsMap = {};
          try {
            enabledCloudsMap = await dashboardCache.get(getEnabledCloudsBatch, [
              configuredWorkspaceNames,
            ]);
          } catch (error) {
            if (isCurrentRequest()) {
              console.error('Error fetching enabled clouds batch:', error);
            }
          }

          if (!isCurrentRequest()) return false;

          // Initialize workspace details with zeros, UI will show spinners for counts.
          const initialWorkspaceDetails = configuredWorkspaceNames
            .map((wsName) => ({
              name: wsName,
              totalClusterCount: 0,
              runningClusterCount: 0,
              managedJobsCount: 0,
              clouds: Array.isArray(enabledCloudsMap[wsName])
                ? enabledCloudsMap[wsName]
                : [],
            }))
            .sort((a, b) => a.name.localeCompare(b.name));

          setWorkspaceDetails(initialWorkspaceDetails);

          // Mark initial loading as complete so the table renders.
          if (wasInitialLoad && showLoadingIndicators) {
            isInitialLoadRef.current = false;
            setIsInitialLoad(false);
          }

          // Launch clusters and jobs fetches in parallel.
          const clustersPromise = fetchClustersData(
            configuredWorkspaceNames,
            isCurrentRequest
          );
          const jobsPromise = fetchJobsData(
            configuredWorkspaceNames,
            isCurrentRequest
          );

          // Wait for both to complete (errors are handled inside each function).
          await Promise.all([clustersPromise, jobsPromise]);
          if (!isCurrentRequest()) return false;
          setLastFetchedTime(new Date());
          return true;
        } catch (error) {
          if (!isCurrentRequest()) return false;
          console.error('Error fetching workspace data:', error);
          // Don't clear data on error during refresh, keep showing stale data.
          if (wasInitialLoad) {
            setWorkspaceDetails([]);
            setGlobalStats({
              runningClusters: 0,
              totalClusters: 0,
              managedJobs: 0,
            });
          }
          if (showLoadingIndicators) {
            setClustersLoading(false);
            setJobsLoading(false);
          }
          if (wasInitialLoad && showLoadingIndicators) {
            isInitialLoadRef.current = false;
            setIsInitialLoad(false);
          }
          return false;
        }
      })();

      activeRequestRef.current = request;
      return request.finally(() => {
        if (activeRequestRef.current === request) {
          activeRequestRef.current = null;
        }
      });
    },
    [fetchClustersData, fetchJobsData]
  );

  useEffect(() => {
    let isCurrent = true;
    const initializeData = async () => {
      // Trigger cache preloading for workspaces page and background preload other pages
      await cachePreloader.preloadForPage('workspaces');
      if (!isCurrent) return;

      await fetchData({ showLoadingIndicators: true });
    };

    initializeData();

    return () => {
      isCurrent = false;
      requestVersionRef.current += 1;
      activeRequestRef.current = null;
      manualRefreshOwnerRef.current = null;
    };
  }, [fetchData]);

  const refreshWhenVisible = useCallback(
    (refreshSource) => {
      if (manualRefreshOwnerRef.current !== null) {
        return;
      }
      void fetchData({
        showLoadingIndicators: false,
        supersede: refreshSource === 'visibilitychange',
      });
    },
    [fetchData]
  );

  useVisibleRefreshInterval(
    true,
    REFRESH_INTERVALS.REFRESH_INTERVAL,
    refreshWhenVisible
  );

  const handleRefresh = useCallback(async () => {
    trackWorkspaceAction('refresh');
    const refreshOwner = {};
    manualRefreshOwnerRef.current = refreshOwner;
    const refreshVersion = requestVersionRef.current + 1;
    requestVersionRef.current = refreshVersion;
    const isCurrentRefresh = () => {
      return requestVersionRef.current === refreshVersion;
    };
    // Set loading states immediately for responsive UI
    setClustersLoading(true);
    setJobsLoading(true);

    // Invalidate cache to ensure fresh data is fetched
    dashboardCache.invalidate(getWorkspaces);
    dashboardCache.invalidateFunction(getEnabledCloudsBatch);

    // Invalidate cluster and job caches
    dashboardCache.invalidate(getClusters);
    dashboardCache.invalidateFunction(getManagedJobs);

    try {
      await apiClient.fetch('/check', {}, 'POST');
      if (!isCurrentRefresh()) return;
      await fetchData({
        showLoadingIndicators: true,
        supersede: true,
      });
    } catch (error) {
      if (isCurrentRefresh()) {
        console.error('Error during sky check refresh:', error);
        setClustersLoading(false);
        setJobsLoading(false);
      }
    } finally {
      if (manualRefreshOwnerRef.current === refreshOwner) {
        manualRefreshOwnerRef.current = null;
      }
    }
  }, [fetchData]);

  // Intercept Cmd+R / Ctrl+R to trigger in-app refresh instead of browser reload
  useEffect(() => {
    const handleKeyDown = (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === 'r') {
        event.preventDefault();
        handleRefresh();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleRefresh]);

  const handleDeleteWorkspace = (workspaceName) => {
    trackWorkspaceAction('delete');
    checkPermissionAndAct('cannot delete workspace', () => {
      setDeleteState({
        confirmOpen: true,
        workspaceToDelete: workspaceName,
        deleting: false,
        error: null,
      });
    });
  };

  const handleConfirmDelete = async () => {
    if (!deleteState.workspaceToDelete) return;

    setDeleteState((prev) => ({ ...prev, deleting: true, error: null }));
    try {
      await deleteWorkspace(deleteState.workspaceToDelete);

      // Show success message at top level
      setTopLevelSuccess(
        `Workspace "${deleteState.workspaceToDelete}" deleted successfully!`
      );

      setDeleteState({
        confirmOpen: false,
        workspaceToDelete: null,
        deleting: false,
        error: null,
      });

      // Invalidate cache to ensure fresh data is fetched (same as manual refresh)
      dashboardCache.invalidate(getWorkspaces);
      dashboardCache.invalidate(getClusters);
      dashboardCache.invalidateFunction(getManagedJobs);

      await fetchData({
        showLoadingIndicators: true,
        supersede: true,
      });
    } catch (error) {
      console.error('Error deleting workspace:', error);

      // Keep dialog open and show error at top level for better UX
      setDeleteState((prev) => ({
        ...prev,
        deleting: false,
        error: null,
      }));
      setTopLevelError(error);
    }
  };

  const handleCancelDelete = () => {
    setDeleteState({
      confirmOpen: false,
      workspaceToDelete: null,
      deleting: false,
      error: null,
    });
  };

  const handleCreateWorkspace = () => {
    trackWorkspaceAction('create');
    checkPermissionAndAct('cannot create workspace', () => {
      router.push('/workspace/new');
    });
  };

  const handleEditWorkspace = (workspaceName) => {
    trackWorkspaceAction('edit');
    checkPermissionAndAct('cannot edit workspace', () => {
      router.push(`/workspaces/${workspaceName}`);
    });
  };

  const preStyle = {
    backgroundColor: '#f5f5f5',
    padding: '16px',
    borderRadius: '8px',
    overflowX: 'auto',
    whiteSpace: 'pre',
    wordBreak: 'normal',
  };

  // Only show full-page loading spinner during initial load
  if (isInitialLoad && workspaceDetails.length === 0) {
    return (
      <div className="flex justify-center items-center h-64">
        <CircularProgress />
        <span className="ml-2 text-gray-500">Loading workspaces...</span>
      </div>
    );
  }

  return (
    <div>
      {/* Error/Success messages positioned at top right, below navigation bar */}
      <div className="fixed top-20 right-4 z-[9999] max-w-md">
        {topLevelSuccess && (
          <div className="bg-green-50 border border-green-200 rounded p-4 mb-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <div className="flex-shrink-0">
                  <svg
                    className="h-5 w-5 text-green-400"
                    viewBox="0 0 20 20"
                    fill="currentColor"
                  >
                    <path
                      fillRule="evenodd"
                      d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                      clipRule="evenodd"
                    />
                  </svg>
                </div>
                <div className="ml-3">
                  <p className="text-sm font-medium text-green-800">
                    {topLevelSuccess}
                  </p>
                </div>
              </div>
              <div className="ml-auto pl-3">
                <button
                  type="button"
                  onClick={() => setTopLevelSuccess(null)}
                  className="inline-flex rounded-md bg-green-50 p-1.5 text-green-500 hover:bg-green-100"
                >
                  <span className="sr-only">Dismiss</span>
                  <svg
                    className="h-5 w-5"
                    viewBox="0 0 20 20"
                    fill="currentColor"
                  >
                    <path
                      fillRule="evenodd"
                      d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
                      clipRule="evenodd"
                    />
                  </svg>
                </button>
              </div>
            </div>
          </div>
        )}
        <ErrorDisplay
          error={topLevelError}
          title="Error"
          onDismiss={() => setTopLevelError(null)}
        />
      </div>

      {/* Header */}
      <div className="flex items-center justify-between mb-2 h-5">
        <div className="text-base flex items-center">
          <span className="text-sky-blue leading-none">Workspaces</span>
        </div>
        <div className="flex items-center">
          {(clustersLoading || jobsLoading) && (
            <div className="flex items-center mr-2">
              <CircularProgress size={15} className="mt-0" />
              <span className="ml-2 text-gray-500 text-xs">Loading...</span>
            </div>
          )}
          {!clustersLoading && !jobsLoading && lastFetchedTime && (
            <LastUpdatedTimestamp
              timestamp={lastFetchedTime}
              className="mr-2"
            />
          )}
          <button
            onClick={handleRefresh}
            disabled={clustersLoading || jobsLoading}
            className="text-sky-blue hover:text-sky-blue-bright flex items-center"
          >
            <RotateCwIcon className="h-4 w-4 mr-1.5" />
            {!isMobile && <span>Refresh</span>}
          </button>
        </div>
      </div>

      <WorkspacesTable
        workspaceDetails={workspaceDetails}
        rawWorkspacesData={rawWorkspacesData}
        clustersLoading={clustersLoading}
        jobsLoading={jobsLoading}
        isInitialLoad={isInitialLoad}
        roleLoading={roleLoading}
        onCreateWorkspace={handleCreateWorkspace}
        onEditWorkspace={handleEditWorkspace}
        onDeleteWorkspace={handleDeleteWorkspace}
        router={router}
      />
      {/* All Workspaces Config Modal */}
      {rawWorkspacesData && (
        <Dialog
          open={isAllWorkspacesModalOpen}
          onOpenChange={setIsAllWorkspacesModalOpen}
        >
          <DialogContent className="sm:max-w-md md:max-w-lg lg:max-w-xl xl:max-w-2xl w-full max-h-[90vh] flex flex-col">
            <DialogHeader>
              <DialogTitle className="pr-10">
                All Workspaces Configuration
              </DialogTitle>
            </DialogHeader>
            <div className="flex-grow overflow-y-auto py-4">
              <pre style={preStyle}>
                {yaml.dump(rawWorkspacesData, { indent: 2 })}
              </pre>
            </div>
          </DialogContent>
        </Dialog>
      )}

      {/* Permission Denial Dialog */}
      <Dialog
        open={permissionDenialState.open}
        onOpenChange={(open) => {
          setPermissionDenialState((prev) => ({ ...prev, open }));
          if (!open) {
            setTopLevelError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-md transition-all duration-200 ease-in-out">
          <DialogHeader>
            <DialogTitle>Permission Denied</DialogTitle>
            <DialogDescription>
              {roleLoading ? (
                <div className="flex items-center py-2">
                  <CircularProgress size={16} className="mr-2" />
                  <span>Checking permissions...</span>
                </div>
              ) : (
                <>
                  {permissionDenialState.userName ? (
                    <>
                      {permissionDenialState.userName} is logged in as non-admin
                      and {permissionDenialState.message}.
                    </>
                  ) : (
                    permissionDenialState.message
                  )}
                </>
              )}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() =>
                setPermissionDenialState((prev) => ({ ...prev, open: false }))
              }
              disabled={roleLoading}
            >
              OK
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog
        open={deleteState.confirmOpen}
        onOpenChange={(open) => {
          if (open) return;
          handleCancelDelete();
          setTopLevelError(null);
        }}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Delete Workspace</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete workspace &quot;
              {deleteState.workspaceToDelete}&quot;? This action cannot be
              undone.
            </DialogDescription>
          </DialogHeader>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={handleCancelDelete}
              disabled={deleteState.deleting}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleConfirmDelete}
              disabled={deleteState.deleting}
            >
              {deleteState.deleting ? 'Deleting...' : 'Delete'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
