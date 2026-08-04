import React, { useEffect, useRef, useState } from 'react';
import { CircularProgress } from '@mui/material';
import {
  ChevronDownIcon,
  ChevronRightIcon,
  Download,
  RotateCwIcon,
} from 'lucide-react';

import { JobLogViewer } from '@/components/job-log-viewer';
import { Card } from '@/components/ui/card';
import { CustomTooltip as Tooltip } from '@/components/utils';
import { downloadManagedJobLogs } from '@/data/connectors/jobs';
import { PluginSlot } from '@/plugins/PluginSlot';
import { usePluginComponents } from '@/plugins/PluginProvider';

export function ControllerLogsSection({
  jobId,
  detailJobData,
  isLoadingControllerLogs,
  handleControllerLogsRefresh,
  setIsLoadingControllerLogs,
  setIsLoadingLogs,
  refreshControllerLogsFlag,
}) {
  const CONTROLLER_LOGS_EXPANDED_KEY = 'skypilot-controller-logs-expanded';
  const controllerLogsSlotHasPlugin =
    usePluginComponents('jobs.detail.controllerlogs').length > 0;
  const normalizedJobId = Array.isArray(jobId) ? jobId[0] : jobId;
  const jobRouteKey = String(normalizedJobId);
  const [pluginDownloading, setPluginDownloading] = useState(false);
  const [, setDownloadOwnerVersion] = useState(0);
  const downloadOwnersRef = useRef(new Map());
  const downloading =
    pluginDownloading || downloadOwnersRef.current.has(jobRouteKey);

  useEffect(
    () => () => {
      downloadOwnersRef.current.clear();
    },
    []
  );

  const downloadControllerZip = () => {
    const currentOwner = downloadOwnersRef.current.get(jobRouteKey);
    if (currentOwner != null) {
      return currentOwner.promise;
    }

    const promise = downloadManagedJobLogs({
      jobId: parseInt(normalizedJobId),
      controller: true,
      jobStatus: detailJobData?.status,
    });
    const owner = { routeKey: jobRouteKey, promise };
    downloadOwnersRef.current.set(jobRouteKey, owner);
    setDownloadOwnerVersion((version) => version + 1);

    const releaseOwner = () => {
      if (downloadOwnersRef.current.get(jobRouteKey) === owner) {
        downloadOwnersRef.current.delete(jobRouteKey);
        setDownloadOwnerVersion((version) => version + 1);
      }
    };
    void promise.then(releaseOwner, releaseOwner);
    return promise;
  };

  // Initialize state from localStorage
  const [isExpanded, setIsExpanded] = useState(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem(CONTROLLER_LOGS_EXPANDED_KEY);
      return saved === 'true';
    }
    return false;
  });

  // Persist state to localStorage when it changes
  const toggleExpanded = () => {
    const newValue = !isExpanded;
    setIsExpanded(newValue);
    if (typeof window !== 'undefined') {
      localStorage.setItem(CONTROLLER_LOGS_EXPANDED_KEY, String(newValue));
    }
  };

  return (
    <div id="controller-logs-section" className="mt-6">
      <Card>
        <div
          className={`flex items-center justify-between px-4 ${isExpanded ? 'pt-4' : 'py-4'}`}
        >
          <button
            onClick={toggleExpanded}
            className="flex items-center text-left focus:outline-none hover:text-gray-700 transition-colors duration-200"
          >
            {isExpanded ? (
              <ChevronDownIcon className="w-5 h-5 mr-2" />
            ) : (
              <ChevronRightIcon className="w-5 h-5 mr-2" />
            )}
            <h3 className="text-lg font-semibold">Controller Logs</h3>
            {!controllerLogsSlotHasPlugin && (
              <span className="ml-2 text-xs text-gray-500">
                (Logs are not streaming; click refresh to fetch the latest
                logs.)
              </span>
            )}
          </button>
          {isExpanded && (
            <div className="flex items-center space-x-3">
              <PluginSlot
                name="jobs.detail.downloadbutton"
                context={{
                  jobId: parseInt(Array.isArray(jobId) ? jobId[0] : jobId),
                  controller: true,
                  jobStatus: detailJobData?.status,
                  downloading,
                  onDownloadingChange: setPluginDownloading,
                }}
                fallback={
                  <Tooltip
                    content={
                      downloading
                        ? 'Preparing zip… download will start shortly'
                        : 'Download full controller logs'
                    }
                    className="text-muted-foreground"
                  >
                    <button
                      onClick={downloadControllerZip}
                      disabled={downloading}
                      className="text-sky-blue hover:text-sky-blue-bright disabled:opacity-50 disabled:cursor-wait flex items-center"
                    >
                      {downloading ? (
                        <CircularProgress size={16} />
                      ) : (
                        <Download className="w-4 h-4" />
                      )}
                    </button>
                  </Tooltip>
                }
              />
              <Tooltip
                content="Refresh controller logs"
                className="text-muted-foreground"
              >
                <button
                  onClick={handleControllerLogsRefresh}
                  disabled={isLoadingControllerLogs}
                  className="text-sky-blue hover:text-sky-blue-bright flex items-center"
                >
                  <RotateCwIcon
                    className={`w-4 h-4 ${isLoadingControllerLogs ? 'animate-spin' : ''}`}
                  />
                </button>
              </Tooltip>
            </div>
          )}
        </div>
        {isExpanded && (
          <div className="p-4">
            <JobLogViewer
              jobData={detailJobData}
              activeTab="controllerlogs"
              setIsLoadingLogs={setIsLoadingLogs}
              setIsLoadingControllerLogs={setIsLoadingControllerLogs}
              isLoadingLogs={false}
              isLoadingControllerLogs={isLoadingControllerLogs}
              refreshFlag={refreshControllerLogsFlag}
            />
          </div>
        )}
      </Card>
    </div>
  );
}
