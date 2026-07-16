import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import { CircularProgress } from '@mui/material';
import PropTypes from 'prop-types';

import { LogFilter, extractNodeTypes } from '@/components/utils';
import { streamManagedJobLogs } from '@/data/connectors/jobs';
import { useLogStreamer } from '@/hooks/useLogStreamer';
import { PluginSlot } from '@/plugins/PluginSlot';
import { usePluginComponents } from '@/plugins/PluginProvider';
import { useLogLinkExtractor } from '@/utils/externalLinks';

export function JobLogViewer({
  jobData,
  activeTab,
  setIsLoadingLogs,
  setIsLoadingControllerLogs,
  isLoadingLogs,
  isLoadingControllerLogs,
  refreshFlag,
  onLinksExtracted,
  selectedTaskIndex = null,
  selectedNode = 'all',
  onNodesExtracted = null,
}) {
  // Auto-scroll refs
  const logsContainerRef = useRef(null);
  const controllerLogsContainerRef = useRef(null);

  // Custom hook for auto-scrolling
  const scrollToBottom = useCallback((logType) => {
    const containerRef =
      logType === 'logs' ? logsContainerRef : controllerLogsContainerRef;

    if (!containerRef.current) return;

    // Try multiple ways to find the scrollable container
    const attempts = [
      () => containerRef.current.querySelector('.logs-container'),
      () => containerRef.current.querySelector('[class*="logs-container"]'),
      () => containerRef.current.querySelector('div[style*="overflow"]'),
      () => containerRef.current, // Fallback to the ref itself
    ];

    for (const attempt of attempts) {
      const container = attempt();
      if (container && container.scrollHeight > container.clientHeight) {
        container.scrollTop = container.scrollHeight;
        console.log(`Auto-scrolled ${logType} to bottom`); // Debug log
        break;
      }
    }
  }, []);

  const PENDING_STATUSES = ['PENDING', 'SUBMITTED', 'STARTING'];
  const PRE_START_STATUSES = ['PENDING', 'SUBMITTED'];
  const RECOVERING_STATUSES = ['RECOVERING'];

  const isPending = PENDING_STATUSES.includes(jobData.status);
  // After priority-based scheduling (#5682), a job can be PENDING while its
  // controller is already running. Show controller logs when schedule_state
  // indicates the controller has been claimed (anything other than
  // INACTIVE/WAITING/null).
  const isControllerRunning =
    jobData.schedule_state != null &&
    jobData.schedule_state !== 'INACTIVE' &&
    jobData.schedule_state !== 'WAITING';
  const isPreStart =
    PRE_START_STATUSES.includes(jobData.status) && !isControllerRunning;
  const isRecovering = RECOVERING_STATUSES.includes(jobData.status);
  const logStreamArgs = useMemo(
    () => ({
      jobId: jobData.id,
      controller: false,
      // Pass task index (as int) when viewing a specific task in a multi-task job
      task: selectedTaskIndex,
    }),
    [jobData.id, selectedTaskIndex]
  );

  const controllerStreamArgs = useMemo(
    () => ({
      jobId: jobData.id,
      controller: true,
    }),
    [jobData.id]
  );

  const handleLogsError = useCallback((error) => {
    console.error('Error streaming logs:', error);
  }, []);

  const handleControllerLogsError = useCallback((error) => {
    console.error('Error streaming controller logs:', error);
  }, []);

  // If a plugin registers a component for the logs slot, it owns the
  // entire log panel (its own streamer, its own rendering). Skip the
  // OSS streamer to avoid double-fetching.
  const logsSlotPluginComponents = usePluginComponents('jobs.detail.logs');
  const controllerLogsSlotPluginComponents = usePluginComponents(
    'jobs.detail.controllerlogs'
  );
  const logsSlotOverridden = logsSlotPluginComponents.length > 0;
  const controllerLogsSlotOverridden =
    controllerLogsSlotPluginComponents.length > 0;

  const {
    lines: logs,
    isLoading: streamingLogsLoading,
    hasReceivedFirstChunk: hasReceivedLogChunk,
  } = useLogStreamer({
    streamFn: streamManagedJobLogs,
    streamArgs: logStreamArgs,
    enabled:
      activeTab === 'logs' &&
      !isPending &&
      !isRecovering &&
      !logsSlotOverridden,
    refreshTrigger: activeTab === 'logs' ? refreshFlag : 0,
    onError: handleLogsError,
  });

  const {
    lines: controllerLogs,
    isLoading: streamingControllerLogsLoading,
    hasReceivedFirstChunk: hasReceivedControllerChunk,
  } = useLogStreamer({
    streamFn: streamManagedJobLogs,
    streamArgs: controllerStreamArgs,
    enabled:
      activeTab === 'controllerlogs' &&
      !isPreStart &&
      !controllerLogsSlotOverridden,
    refreshTrigger: activeTab === 'controllerlogs' ? refreshFlag : 0,
    onError: handleControllerLogsError,
  });

  useEffect(() => {
    setIsLoadingLogs(streamingLogsLoading);
  }, [streamingLogsLoading, setIsLoadingLogs]);

  useEffect(() => {
    setIsLoadingControllerLogs(streamingControllerLogsLoading);
  }, [streamingControllerLogsLoading, setIsLoadingControllerLogs]);

  // Extract node types from logs and pass them to parent
  useEffect(() => {
    if (onNodesExtracted && logs.length > 0) {
      const logsText = logs.join('\n');
      const nodes = extractNodeTypes(logsText);
      onNodesExtracted(nodes);
    }
  }, [logs, onNodesExtracted]);

  // External-link extraction from log lines. Matches accumulate inside
  // the hook so they survive tab switches and re-renders. `scanLines` is
  // a stable callback: besides feeding it from the OSS streamer below,
  // it is handed to a plugin that owns the logs slot — the OSS streamer
  // does not run in that case, so the plugin forwards its own lines.
  const { extractedLinks: logExtractedLinks, scanLines } =
    useLogLinkExtractor();

  useEffect(() => {
    scanLines(logs);
  }, [logs, scanLines]);

  // Notify parent when links are extracted (for cross-component sharing)
  useEffect(() => {
    if (onLinksExtracted && Object.keys(logExtractedLinks).length > 0) {
      onLinksExtracted(logExtractedLinks);
    }
  }, [logExtractedLinks, onLinksExtracted]);
  // Auto-scroll to bottom when logs change or tab changes
  useEffect(() => {
    const performScroll = () => {
      if (
        (activeTab === 'logs' && logs.length) ||
        (activeTab === 'controllerlogs' && controllerLogs.length)
      ) {
        scrollToBottom(activeTab === 'logs' ? 'logs' : 'controllerlogs');
      }
    };

    // Use requestAnimationFrame for better timing after DOM updates
    requestAnimationFrame(() => {
      requestAnimationFrame(performScroll); // Double RAF to ensure DOM is updated
    });
  }, [activeTab, logs, controllerLogs, scrollToBottom]);

  if (activeTab === 'logs') {
    const defaultLogsContent = (
      <div className="max-h-96 overflow-y-auto" ref={logsContainerRef}>
        {isPending ? (
          <div className="bg-[#f7f7f7] flex items-center justify-center py-4 text-gray-500">
            <span>Waiting for the job to start; refresh in a few moments.</span>
          </div>
        ) : isRecovering ? (
          <div className="bg-[#f7f7f7] flex items-center justify-center py-4 text-gray-500">
            <span>
              Waiting for the job to recover; refresh in a few moments.
            </span>
          </div>
        ) : (
          <LogFilter
            logs={logs}
            isLoading={isLoadingLogs && !hasReceivedLogChunk && !logs.length}
            selectedNode={selectedNode}
          />
        )}
      </div>
    );
    // Plugin override: a registered plugin component owns the entire log
    // panel (its own streamer, its own rendering). We pass enough context
    // (jobId, taskId, status) for the plugin to drive `/jobs/logs` itself.
    // Pass `onNodesExtracted` too so the plugin can populate the
    // node-filter dropdown (the OSS `useLogStreamer` no longer runs to
    // discover node names when the plugin is in charge).
    return (
      <PluginSlot
        name="jobs.detail.logs"
        context={{
          jobId: jobData.id,
          taskId: selectedTaskIndex,
          status: jobData.status,
          isPending,
          isRecovering,
          selectedNode,
          isController: false,
          onNodesExtracted,
          // The OSS streamer that feeds External Links extraction does
          // not run when a plugin owns this slot. The plugin should
          // forward its visible log lines (raw buffer string or array of
          // lines) through this callback so extraction keeps working.
          onLogLines: scanLines,
          // Forward the refresh-button signal so a plugin owning this slot
          // can re-fetch on refresh (the OSS streamer it replaces consumes
          // the same flag). Without this the refresh button is a no-op for
          // plugin-owned log panels.
          refreshTrigger: refreshFlag,
        }}
        fallback={defaultLogsContent}
      />
    );
  }

  if (activeTab === 'controllerlogs') {
    const defaultControllerLogsContent = (
      <div
        className="max-h-96 overflow-y-auto"
        ref={controllerLogsContainerRef}
      >
        {isPreStart ? (
          <div className="bg-[#f7f7f7] flex items-center justify-center py-4 text-gray-500">
            <span>
              Waiting for the job controller process to start; refresh in a few
              moments.
            </span>
          </div>
        ) : hasReceivedControllerChunk || controllerLogs.length ? (
          <LogFilter logs={controllerLogs} controller={true} />
        ) : isLoadingControllerLogs ? (
          <div className="flex items-center justify-center py-4">
            <CircularProgress size={20} className="mr-2" />
            <span>Loading logs...</span>
          </div>
        ) : (
          <LogFilter logs={controllerLogs} controller={true} />
        )}
      </div>
    );
    return (
      <PluginSlot
        name="jobs.detail.controllerlogs"
        context={{
          jobId: jobData.id,
          status: jobData.status,
          isPreStart,
          isController: true,
          // Forward the refresh-button signal (see jobs.detail.logs slot).
          refreshTrigger: refreshFlag,
        }}
        fallback={defaultControllerLogsContent}
      />
    );
  }
}

JobLogViewer.propTypes = {
  jobData: PropTypes.shape({
    id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    status: PropTypes.string,
    schedule_state: PropTypes.string,
  }).isRequired,
  activeTab: PropTypes.oneOf(['logs', 'controllerlogs']).isRequired,
  setIsLoadingLogs: PropTypes.func.isRequired,
  setIsLoadingControllerLogs: PropTypes.func.isRequired,
  isLoadingLogs: PropTypes.bool,
  isLoadingControllerLogs: PropTypes.bool,
  refreshFlag: PropTypes.number,
  onLinksExtracted: PropTypes.func,
  selectedTaskIndex: PropTypes.number,
  selectedNode: PropTypes.string,
  onNodesExtracted: PropTypes.func,
};
