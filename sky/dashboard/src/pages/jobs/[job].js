import React, { useState, useEffect, useMemo, useRef } from 'react';
import { CircularProgress } from '@mui/material';
import { useRouter } from 'next/router';
import { Card } from '@/components/ui/card';
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from '@/components/ui/table';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  useSingleManagedJob,
  useManagedJobPools,
} from '@/data/connectors/jobs';
import Link from 'next/link';
import { RotateCwIcon, Download } from 'lucide-react';
import { CustomTooltip as Tooltip, formatDuration } from '@/components/utils';
import { downloadManagedJobLogs } from '@/data/connectors/jobs';
import { StatusBadge } from '@/components/elements/StatusBadge';
import { PrimaryBadge } from '@/components/elements/PrimaryBadge';
import { BatchBadge } from '@/components/elements/BatchBadge';
import { useMobile } from '@/hooks/useMobile';
import Head from 'next/head';
import { NonCapitalizedTooltip } from '@/components/utils';
import { PluginSlot } from '@/plugins/PluginSlot';
import { usePluginComponents } from '@/plugins/PluginProvider';
import { checkGrafanaAvailability } from '@/utils/grafana';
import { TelemetrySection } from '@/components/TelemetrySection';
import { hasAccelerator } from '@/utils/gpuUtils';
import { JobLogViewer } from '@/components/job-log-viewer';
import { ControllerLogsSection } from '@/components/controller-logs-section';
import { JobInfoSection } from '@/components/job-info-section';

function clampRouteIndex(index, itemCount) {
  if (!Number.isInteger(index) || index < 0 || index >= itemCount) {
    return 0;
  }
  return index;
}

function JobDetails() {
  const router = useRouter();
  const { job: jobId, tab } = router.query;
  const jobRouteKey = Array.isArray(jobId) ? jobId[0] : jobId;
  const { jobData, loading, refreshJobData } = useSingleManagedJob(jobId);
  const poolsData = useManagedJobPools(jobData?.jobs, jobId);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [isLoadingControllerLogs, setIsLoadingControllerLogs] = useState(false);
  const [scrollExecuted, setScrollExecuted] = useState(false);
  const [pageLoaded, setPageLoaded] = useState(false);
  const [domReady, setDomReady] = useState(false);
  const [refreshLogsFlag, setRefreshLogsFlag] = useState(0);
  const [refreshControllerLogsFlag, setRefreshControllerLogsFlag] = useState(0);
  const [selectedTaskIndex, setSelectedTaskIndex] = useState(0);
  const [selectedNode, setSelectedNode] = useState('all');
  const [logNodes, setLogNodes] = useState([]);
  // If a plugin owns the logs slot, the OSS "(Logs are not streaming;
  // click refresh ...)" hint is misleading — the plugin's component
  // streams live. Hide it. (ControllerLogsSection makes the same check
  // independently for the controller-logs heading.)
  const logsSlotHasPlugin = usePluginComponents('jobs.detail.logs').length > 0;
  const [logExtractedLinks, setLogExtractedLinks] = useState({});
  // Track download-in-flight per kind ('logs' / 'controller' / per-task)
  // so we can disable the button + spin the icon while the zip is being
  // assembled on the server. Without feedback, users click and assume
  // nothing is happening because the browser only shows the file in the
  // download bar a second or two later.
  const [logsDownloading, setLogsDownloading] = useState(false);
  const downloadLogsZip = async () => {
    if (logsDownloading) return;
    setLogsDownloading(true);
    try {
      const detail = jobData?.jobs?.find((j) => String(j.id) === String(jobId));
      await downloadManagedJobLogs({
        jobId: parseInt(Array.isArray(jobId) ? jobId[0] : jobId),
        controller: false,
        jobStatus: detail?.status,
      });
    } finally {
      setLogsDownloading(false);
    }
  };
  const isMobile = useMobile();

  // Telemetry state
  const [isGrafanaAvailable, setIsGrafanaAvailable] = useState(false);
  const [telemetryRefreshTrigger, setTelemetryRefreshTrigger] = useState(0);
  // Telemetry task selection for job groups
  const [telemetryTaskIndex, setTelemetryTaskIndex] = useState(0);
  const TELEMETRY_EXPANDED_KEY = 'skypilot-jobs-telemetry-expanded';
  const activeJobIdRef = useRef(jobRouteKey);

  // Check Grafana availability on mount
  useEffect(() => {
    const checkGrafana = async () => {
      const available = await checkGrafanaAvailability();
      setIsGrafanaAvailable(available);
    };
    checkGrafana();
  }, []);

  // Function to scroll to a specific section
  const scrollToSection = (sectionId) => {
    const element = document.getElementById(sectionId);
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' });
    }
  };

  // Set pageLoaded to true when the component mounts
  useEffect(() => {
    setPageLoaded(true);
  }, []);

  // Use MutationObserver to detect when the DOM is fully rendered
  useEffect(() => {
    if (!domReady) {
      const observer = new MutationObserver(() => {
        // Check if the sections we want to scroll to exist in the DOM
        const logsSection = document.getElementById('logs-section');
        const controllerLogsSection = document.getElementById(
          'controller-logs-section'
        );

        if (
          (tab === 'logs' && logsSection) ||
          (tab === 'controllerlogs' && controllerLogsSection)
        ) {
          setDomReady(true);
          observer.disconnect();
        }
      });

      observer.observe(document.body, {
        childList: true,
        subtree: true,
      });

      return () => observer.disconnect();
    }
  }, [domReady, tab]);

  // Scroll to the appropriate section when the page loads with a tab parameter
  useEffect(() => {
    if (router.isReady && pageLoaded && domReady && !scrollExecuted) {
      // Add a small delay to ensure the DOM is fully rendered
      const timer = setTimeout(() => {
        if (tab === 'logs') {
          scrollToSection('logs-section');
          setScrollExecuted(true);
        } else if (tab === 'controllerlogs') {
          scrollToSection('controller-logs-section');
          setScrollExecuted(true);
        }
      }, 800);

      return () => clearTimeout(timer);
    }
  }, [router.isReady, tab, scrollExecuted, pageLoaded, domReady]);

  // Reset scrollExecuted when tab changes
  useEffect(() => {
    setScrollExecuted(false);
    setDomReady(false);
  }, [tab]);

  // Handle manual refresh of everything
  const handleManualRefresh = async () => {
    setIsRefreshing(true);
    try {
      // Trigger logs refresh
      setRefreshLogsFlag((prev) => prev + 1);
      // Trigger controller logs refresh
      setRefreshControllerLogsFlag((prev) => prev + 1);
      // Trigger telemetry refresh
      setTelemetryRefreshTrigger((prev) => prev + 1);
      await refreshJobData();
    } catch (error) {
      console.error('Error refreshing data:', error);
    } finally {
      setIsRefreshing(false);
    }
  };

  // Individual refresh handlers for logs
  const handleLogsRefresh = () => {
    setRefreshLogsFlag((prev) => prev + 1);
  };

  const handleControllerLogsRefresh = () => {
    setRefreshControllerLogsFlag((prev) => prev + 1);
  };

  // Keep route-owned UI state scoped to the current job. The render path uses
  // default selections immediately on a route change, while the effects below
  // synchronize the backing state after the new route commits.
  useEffect(() => {
    if (activeJobIdRef.current !== jobRouteKey) {
      activeJobIdRef.current = jobRouteKey;
      setSelectedTaskIndex(0);
      setTelemetryTaskIndex(0);
      setSelectedNode('all');
      setLogNodes([]);
      setLogExtractedLinks({});
    }
  }, [jobRouteKey]);

  // Get all tasks for this job (supports multi-task jobs)
  const allTasks = useMemo(() => {
    return (
      jobData?.jobs?.filter((item) => String(item.id) === String(jobId)) || []
    );
  }, [jobData, jobId]);

  // Determine which tasks have telemetry (Kubernetes, not pool, has cluster_name_on_cloud)
  const tasksWithTelemetry = useMemo(() => {
    return allTasks.map((task, index) => ({
      index,
      task,
      hasMetrics:
        task.full_infra?.toLowerCase().includes('kubernetes') &&
        !task.pool &&
        task.cluster_name_on_cloud,
    }));
  }, [allTasks]);

  const hasAnyTaskWithTelemetry = tasksWithTelemetry.some((t) => t.hasMetrics);
  const ownsRouteState = activeJobIdRef.current === jobRouteKey;
  const currentSelectedTaskIndex = clampRouteIndex(
    ownsRouteState ? selectedTaskIndex : 0,
    allTasks.length
  );
  const currentTelemetryTaskIndex = clampRouteIndex(
    ownsRouteState ? telemetryTaskIndex : 0,
    allTasks.length
  );
  const currentSelectedNode = ownsRouteState ? selectedNode : 'all';
  const currentLogNodes = useMemo(
    () => (ownsRouteState ? logNodes : []),
    [ownsRouteState, logNodes]
  );
  const currentLogExtractedLinks = useMemo(
    () => (ownsRouteState ? logExtractedLinks : {}),
    [ownsRouteState, logExtractedLinks]
  );

  useEffect(() => {
    if (selectedTaskIndex !== currentSelectedTaskIndex) {
      setSelectedTaskIndex(currentSelectedTaskIndex);
    }
  }, [currentSelectedTaskIndex, selectedTaskIndex]);

  useEffect(() => {
    if (telemetryTaskIndex !== currentTelemetryTaskIndex) {
      setTelemetryTaskIndex(currentTelemetryTaskIndex);
    }
  }, [currentTelemetryTaskIndex, telemetryTaskIndex]);

  useEffect(() => {
    if (
      currentSelectedNode !== 'all' &&
      !currentLogNodes.includes(currentSelectedNode)
    ) {
      setSelectedNode('all');
    }
  }, [currentLogNodes, currentSelectedNode]);

  // Get the currently selected task for telemetry
  const telemetryTask = allTasks[currentTelemetryTaskIndex] || allTasks[0];

  // Get cluster name for telemetry from selected task
  const telemetryClusterName =
    telemetryTask?.cluster_name_on_cloud || allTasks[0]?.cluster_name_on_cloud;

  if (!router.isReady) {
    return <div>Loading...</div>;
  }

  // Use the first task for main details display
  const detailJobData = allTasks.length > 0 ? allTasks[0] : null;
  const isMultiTask = allTasks.length > 1;
  const isRouteLoading = loading && detailJobData === null;

  // For multi-task jobs, find fields from any task (they may only be on one task)
  const jobYaml =
    allTasks.find((t) => t.dag_yaml)?.dag_yaml || detailJobData?.dag_yaml;
  const jobEntrypoint =
    allTasks.find((t) => t.entrypoint)?.entrypoint || detailJobData?.entrypoint;
  const jobIsJobGroup =
    allTasks.find((t) => t.is_job_group)?.is_job_group ||
    detailJobData?.is_job_group ||
    allTasks.length > 1;

  // For execution, check stored values first, then apply defaults for multi-task jobs
  // Older jobs may not have these fields stored, so provide sensible defaults
  const storedExecution =
    allTasks.find((t) => t.execution)?.execution || detailJobData?.execution;
  // Default execution to 'parallel' for multi-task jobs without stored value
  const jobExecution = storedExecution || (isMultiTask ? 'parallel' : null);

  // Enhanced job data with fields from any task
  const enhancedJobData = detailJobData
    ? {
        ...detailJobData,
        dag_yaml: jobYaml,
        entrypoint: jobEntrypoint,
        execution: jobExecution,
        is_job_group: jobIsJobGroup,
      }
    : null;

  const title = jobId
    ? `Job: ${jobId} | SkyPilot Dashboard`
    : 'Job Details | SkyPilot Dashboard';

  return (
    <>
      <Head>
        <title>{title}</title>
      </Head>
      <>
        <div className="flex items-center justify-between mb-4">
          <div className="text-base flex items-center">
            <Link href="/jobs" className="text-sky-blue hover:underline">
              Managed Jobs
            </Link>
            <span className="mx-2 text-gray-500">›</span>
            <Link
              href={`/jobs/${jobId}`}
              className="text-sky-blue hover:underline"
            >
              {jobId} {detailJobData?.name ? `(${detailJobData.name})` : ''}
            </Link>
            {(detailJobData?.is_batch === true ||
              detailJobData?.batch_total_batches != null) && (
              <BatchBadge className="ml-2" />
            )}
            {isMultiTask && (
              <span className="ml-2 text-xs text-gray-500 bg-gray-200 px-1.5 py-0.5 rounded">
                {allTasks.length} tasks
              </span>
            )}
          </div>

          <div className="text-sm flex items-center">
            {(loading ||
              isRefreshing ||
              isLoadingLogs ||
              isLoadingControllerLogs) && (
              <div className="flex items-center mr-4">
                <CircularProgress size={15} className="mt-0" />
                <span className="ml-2 text-gray-500">Loading...</span>
              </div>
            )}
            <Tooltip content="Refresh" className="text-muted-foreground">
              <button
                onClick={handleManualRefresh}
                disabled={loading || isRefreshing}
                className="text-sky-blue hover:text-sky-blue-bright font-medium inline-flex items-center h-8"
              >
                <RotateCwIcon className="w-4 h-4 mr-1.5" />
                {!isMobile && <span>Refresh</span>}
              </button>
            </Tooltip>
          </div>
        </div>

        {isRouteLoading ? (
          <div className="flex items-center justify-center py-32">
            <CircularProgress size={20} className="mr-2" />
            <span>Loading...</span>
          </div>
        ) : detailJobData ? (
          <div className="space-y-8">
            {/* Details Section */}
            <div id="details-section">
              <Card>
                <div className="flex items-center justify-between px-4 pt-4">
                  <h3 className="text-lg font-semibold">Details</h3>
                </div>
                <div className="p-4">
                  <JobInfoSection
                    jobData={enhancedJobData}
                    allTasks={allTasks}
                    poolsData={poolsData}
                    links={enhancedJobData?.links}
                    logExtractedLinks={currentLogExtractedLinks}
                  />
                </div>
              </Card>
            </div>

            {/* Tasks Section - only show for multi-task jobs */}
            {isMultiTask && (
              <div id="tasks-section" className="mt-6">
                <Card>
                  <div className="flex items-center justify-between px-4 pt-4">
                    <h3 className="text-lg font-semibold flex items-center">
                      Tasks
                      <span className="ml-2 text-sm font-normal text-gray-500">
                        ({allTasks.length} tasks)
                      </span>
                    </h3>
                  </div>
                  <div className="p-4">
                    <div className="overflow-x-auto rounded-lg border">
                      <Table className="min-w-full">
                        <TableHeader>
                          <TableRow>
                            <TableHead className="whitespace-nowrap">
                              ID
                            </TableHead>
                            <TableHead className="whitespace-nowrap">
                              Name
                            </TableHead>
                            <TableHead className="whitespace-nowrap">
                              Status
                            </TableHead>
                            <TableHead className="whitespace-nowrap">
                              Duration
                            </TableHead>
                            <TableHead className="whitespace-nowrap">
                              Infra
                            </TableHead>
                            <TableHead className="whitespace-nowrap">
                              Resources
                            </TableHead>
                            <TableHead className="whitespace-nowrap">
                              Recoveries
                            </TableHead>
                            <TableHead className="whitespace-nowrap">
                              Logs
                            </TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {allTasks.map((task, index) => (
                            <TableRow
                              key={task.task_job_id}
                              className="hover:bg-gray-50"
                            >
                              <TableCell>
                                <Link
                                  href={`/jobs/${jobId}/${index}`}
                                  className="text-blue-600 hover:underline"
                                >
                                  {index}
                                </Link>
                              </TableCell>
                              <TableCell>
                                <Link
                                  href={`/jobs/${jobId}/${index}`}
                                  className="text-blue-600 hover:underline"
                                >
                                  {task.task || `Job ${index}`}
                                  {/* Show Primary badge for primary tasks in job groups with auxiliaries */}
                                  {allTasks.some(
                                    (t) => t.is_primary_in_job_group === false
                                  ) &&
                                    task.is_primary_in_job_group === true && (
                                      <span className="ml-1.5">
                                        <PrimaryBadge />
                                      </span>
                                    )}
                                </Link>
                              </TableCell>
                              <TableCell>
                                <StatusBadge status={task.status} />
                              </TableCell>
                              <TableCell>
                                {formatDuration(task.job_duration)}
                              </TableCell>
                              <TableCell>
                                {task.infra && task.infra !== '-' ? (
                                  <NonCapitalizedTooltip
                                    content={task.full_infra || task.infra}
                                    className="text-sm text-muted-foreground"
                                  >
                                    <span>
                                      {task.cloud ||
                                        task.infra.split('(')[0].trim()}
                                      {task.infra.includes('(') && (
                                        <span className="text-gray-500">
                                          {' ' +
                                            task.infra.substring(
                                              task.infra.indexOf('(')
                                            )}
                                        </span>
                                      )}
                                    </span>
                                  </NonCapitalizedTooltip>
                                ) : (
                                  <span>-</span>
                                )}
                              </TableCell>
                              <TableCell>
                                <NonCapitalizedTooltip
                                  content={
                                    task.requested_resources ||
                                    task.resources_str_full ||
                                    task.resources_str ||
                                    '-'
                                  }
                                  className="text-sm text-muted-foreground"
                                >
                                  <span>
                                    {task.requested_resources ||
                                      task.resources_str ||
                                      '-'}
                                  </span>
                                </NonCapitalizedTooltip>
                              </TableCell>
                              <TableCell>{task.recoveries || 0}</TableCell>
                              <TableCell>
                                <Tooltip
                                  content="Download job logs"
                                  className="text-muted-foreground"
                                >
                                  <button
                                    onClick={() =>
                                      downloadManagedJobLogs({
                                        jobId: parseInt(jobId),
                                        controller: false,
                                        jobStatus: task?.status,
                                      })
                                    }
                                    className="text-sky-blue hover:text-sky-blue-bright"
                                  >
                                    <Download className="w-4 h-4" />
                                  </button>
                                </Tooltip>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </div>
                </Card>
              </div>
            )}

            {/* Logs Section — moved up so the live tail is visible
                 right under the job summary instead of below
                 Telemetry / Infra Nodes panels. */}
            <div id="logs-section" className="mt-6">
              <Card>
                <div className="flex items-center justify-between px-4 pt-4">
                  <div className="flex items-center gap-4">
                    <h3 className="text-lg font-semibold">Logs</h3>
                    {isMultiTask && (
                      <Select
                        onValueChange={(value) =>
                          setSelectedTaskIndex(parseInt(value, 10))
                        }
                        value={String(currentSelectedTaskIndex)}
                      >
                        <SelectTrigger
                          aria-label="Task"
                          className="focus:ring-0 focus:ring-offset-0 h-8 w-auto min-w-[160px] text-sm"
                        >
                          <SelectValue placeholder="Select Task" />
                        </SelectTrigger>
                        <SelectContent>
                          {allTasks.map((task, index) => (
                            <SelectItem
                              key={task.task_job_id || index}
                              value={String(index)}
                            >
                              Task {index}
                              {task.task ? `: ${task.task}` : ''}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    )}
                    <Select
                      onValueChange={(value) => setSelectedNode(value)}
                      value={currentSelectedNode}
                    >
                      <SelectTrigger
                        aria-label="Node"
                        className="focus:ring-0 focus:ring-offset-0 h-8 w-auto min-w-[120px] text-sm"
                      >
                        <SelectValue placeholder="All Nodes" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All Nodes</SelectItem>
                        {currentLogNodes.map((node) => (
                          <SelectItem key={node} value={node}>
                            {node.charAt(0).toUpperCase() + node.slice(1)}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    {!logsSlotHasPlugin && (
                      <span className="text-xs text-gray-500">
                        (Logs are not streaming; click refresh to fetch the
                        latest logs.)
                      </span>
                    )}
                  </div>
                  <div className="flex items-center space-x-3">
                    <PluginSlot
                      name="jobs.detail.downloadbutton"
                      context={{
                        jobId: parseInt(
                          Array.isArray(jobId) ? jobId[0] : jobId
                        ),
                        controller: false,
                        jobStatus: detailJobData?.status,
                        downloading: logsDownloading,
                        onDownloadingChange: setLogsDownloading,
                      }}
                      fallback={
                        <Tooltip
                          content={
                            logsDownloading
                              ? 'Preparing zip… download will start shortly'
                              : 'Download all job logs (zip)'
                          }
                          className="text-muted-foreground"
                        >
                          <button
                            onClick={downloadLogsZip}
                            disabled={logsDownloading}
                            className="text-sky-blue hover:text-sky-blue-bright disabled:opacity-50 disabled:cursor-wait flex items-center"
                          >
                            {logsDownloading ? (
                              <CircularProgress size={16} />
                            ) : (
                              <Download className="w-4 h-4" />
                            )}
                          </button>
                        </Tooltip>
                      }
                    />
                    <Tooltip
                      content="Refresh logs"
                      className="text-muted-foreground"
                    >
                      <button
                        onClick={handleLogsRefresh}
                        disabled={isLoadingLogs}
                        className="text-sky-blue hover:text-sky-blue-bright flex items-center"
                      >
                        <RotateCwIcon
                          className={`w-4 h-4 ${isLoadingLogs ? 'animate-spin' : ''}`}
                        />
                      </button>
                    </Tooltip>
                  </div>
                </div>
                <div className="p-4">
                  <JobLogViewer
                    key={jobRouteKey || 'managed-job-logs'}
                    jobData={
                      isMultiTask
                        ? allTasks[currentSelectedTaskIndex]
                        : detailJobData
                    }
                    activeTab="logs"
                    setIsLoadingLogs={setIsLoadingLogs}
                    setIsLoadingControllerLogs={setIsLoadingControllerLogs}
                    isLoadingLogs={isLoadingLogs}
                    isLoadingControllerLogs={isLoadingControllerLogs}
                    refreshFlag={refreshLogsFlag}
                    selectedTaskIndex={
                      isMultiTask ? currentSelectedTaskIndex : null
                    }
                    selectedNode={currentSelectedNode}
                    onNodesExtracted={setLogNodes}
                    onLinksExtracted={setLogExtractedLinks}
                  />
                </div>
              </Card>
            </div>

            {/* Telemetry Section (GPU + CPU/Memory) - Show for Kubernetes managed jobs with cluster_name_on_cloud */}
            {isGrafanaAvailable && hasAnyTaskWithTelemetry && (
              <TelemetrySection
                clusterNameOnCloud={telemetryClusterName}
                displayName={
                  isMultiTask
                    ? `${telemetryTask?.task || telemetryTask?.name || detailJobData.name} (Task ${currentTelemetryTaskIndex})`
                    : telemetryTask?.task ||
                      telemetryTask?.name ||
                      detailJobData.name
                }
                storageKey={TELEMETRY_EXPANDED_KEY}
                refreshTrigger={telemetryRefreshTrigger}
                hasGpu={hasAccelerator(telemetryTask?.accelerators)}
                noMetricsMessage={
                  telemetryTask?.pool
                    ? 'Telemetry is not available for pool jobs.'
                    : !telemetryTask?.full_infra?.includes('Kubernetes')
                      ? 'Telemetry is only available for Kubernetes tasks.'
                      : 'No telemetry available for this task.'
                }
                headerExtra={
                  isMultiTask && (
                    <Select
                      onValueChange={(value) =>
                        setTelemetryTaskIndex(parseInt(value, 10))
                      }
                      value={String(currentTelemetryTaskIndex)}
                    >
                      <SelectTrigger
                        onClick={(e) => e.stopPropagation()}
                        aria-label="Task"
                        className="focus:ring-0 focus:ring-offset-0 h-8 w-auto min-w-[160px] text-sm ml-4"
                      >
                        <SelectValue placeholder="Select Task" />
                      </SelectTrigger>
                      <SelectContent>
                        {tasksWithTelemetry.map(
                          ({ index, task, hasMetrics }) => (
                            <SelectItem
                              key={index}
                              value={String(index)}
                              disabled={!hasMetrics}
                            >
                              Task {index}
                              {task.task ? `: ${task.task}` : ''}
                              {!hasMetrics ? ' (no metrics)' : ''}
                            </SelectItem>
                          )
                        )}
                      </SelectContent>
                    </Select>
                  )
                }
              />
            )}

            {/* Plugin Slot: Job Infra Nodes */}
            <PluginSlot
              name="jobs.detail.nodes"
              context={{
                clusterName: detailJobData.current_cluster_name,
                clusterNameOnCloud: detailJobData.cluster_name_on_cloud,
                nodeNames: detailJobData.node_names,
                infra: detailJobData.full_infra,
                status: detailJobData.status,
              }}
              wrapperClassName="mt-6"
            />

            {/* Plugin Slot: Job Detail Events */}
            <PluginSlot
              name="jobs.detail.events"
              context={{
                jobId: detailJobData.id,
              }}
              wrapperClassName="mt-6"
            />

            {/* Controller Logs Section - Collapsible */}
            <ControllerLogsSection
              jobId={jobId}
              detailJobData={detailJobData}
              isLoadingControllerLogs={isLoadingControllerLogs}
              handleControllerLogsRefresh={handleControllerLogsRefresh}
              setIsLoadingControllerLogs={setIsLoadingControllerLogs}
              setIsLoadingLogs={setIsLoadingLogs}
              refreshControllerLogsFlag={refreshControllerLogsFlag}
            />
          </div>
        ) : (
          <div className="flex items-center justify-center py-32">
            <span>Job not found</span>
          </div>
        )}
      </>
    </>
  );
}

export default JobDetails;
