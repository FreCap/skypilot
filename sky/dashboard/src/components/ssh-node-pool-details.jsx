import React, { useEffect, useState } from 'react';
import { CircularProgress } from '@mui/material';
import { PlayIcon, TrashIcon } from 'lucide-react';

import { ContextDetails } from '@/components/infra-context-details';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  getSSHNodePoolStatus,
  sshDownNodePool,
  streamSSHDeploymentLogs,
  streamSSHOperationLogs,
} from '@/data/connectors/ssh-node-pools';
import { PluginSlot } from '@/plugins/PluginSlot';

export function SSHNodePoolDetails({
  poolName,
  gpusInContext,
  nodesInContext,
  handleDeploySSHPool,
  handleEditSSHPool,
  handleDeleteSSHPool,
  poolConfig,
}) {
  const [statusData, setStatusData] = useState(null);
  const [statusLoading, setStatusLoading] = useState(true);

  // Confirmation dialog state
  const [confirmDialog, setConfirmDialog] = useState({
    isOpen: false,
    action: null, // 'deploy', 'repair', or 'delete'
    loading: false,
  });

  // Deployment streaming dialog state
  const [streamingDialog, setStreamingDialog] = useState({
    isOpen: false,
    logs: '',
    isStreaming: false,
    deploymentComplete: false,
    deploymentSuccess: false,
    requestId: null,
  });

  // Fetch status when component mounts
  useEffect(() => {
    const fetchStatus = async () => {
      try {
        setStatusLoading(true);
        const status = await getSSHNodePoolStatus(poolName);
        setStatusData(status);
      } catch (error) {
        console.error('Failed to fetch SSH Node Pool status:', error);
        setStatusData({
          pool_name: poolName,
          status: 'Error',
          reason: 'Failed to fetch status',
        });
      } finally {
        setStatusLoading(false);
      }
    };

    fetchStatus();
  }, [poolName]);

  const StatusBadge = ({ status, reason }) => {
    const isReady = status === 'Ready';
    const isNotReady = status === 'Not Ready';
    const bgColor = isReady ? 'bg-green-100' : 'bg-red-100';
    const textColor = isReady ? 'text-green-800' : 'text-red-800';

    // Show helpful hint for "Not Ready" status
    const displayReason = isNotReady
      ? 'Click Deploy to set up this node pool'
      : reason;

    return (
      <div className="flex items-center space-x-2">
        <span
          className={`px-2 py-0.5 rounded text-xs font-medium ${bgColor} ${textColor}`}
        >
          {status}
        </span>
        {!isReady && displayReason && (
          <span className="text-sm text-gray-600">({displayReason})</span>
        )}
      </div>
    );
  };

  // Clean up SSH deployment logs
  const cleanSSHDeploymentLogs = (logs) => {
    if (!logs) return '';

    return logs
      .split('\n')
      .map((line) => {
        // Remove ANSI escape codes
        line = line.replace(/\x1b\[[0-9;]*m/g, '');

        // Skip debug lines (starting with D)
        if (line.match(/^D \d{2}-\d{2} \d{2}:\d{2}:\d{2}/)) {
          return null;
        }

        // Clean up tree characters and extra formatting
        line = line.replace(/├──/g, '├─');
        line = line.replace(/└──/g, '└─');

        return line;
      })
      .filter((line) => line !== null && line.trim() !== '')
      .join('\n');
  };

  const handleEditClick = () => {
    console.log('Edit button clicked for pool:', poolName);
    console.log('Pool config:', poolConfig);
    console.log('handleEditSSHPool function:', handleEditSSHPool);
    if (handleEditSSHPool) {
      handleEditSSHPool(poolName, poolConfig);
    } else {
      console.error('handleEditSSHPool function not provided');
    }
  };

  // Determine button states based on status
  const getButtonStates = () => {
    if (!statusData) {
      return {
        deployDisabled: true,
      };
    }

    const status = statusData.status;

    if (status === 'Ready') {
      return {
        deployDisabled: true,
      };
    } else if (status === 'Error') {
      return {
        deployDisabled: true,
      };
    } else {
      // Not Ready or other status
      return {
        deployDisabled: false,
      };
    }
  };

  const { deployDisabled } = getButtonStates();

  const handleDeployClick = () => {
    setConfirmDialog({
      isOpen: true,
      action: 'deploy',
      loading: false,
    });
  };

  const handleDeleteClick = () => {
    setConfirmDialog({
      isOpen: true,
      action: 'delete',
      loading: false,
    });
  };

  const handleConfirmAction = async () => {
    setConfirmDialog({ ...confirmDialog, loading: true });

    try {
      if (confirmDialog.action === 'deploy') {
        // Hide confirmation dialog and show streaming dialog
        setConfirmDialog({ isOpen: false, action: null, loading: false });
        setStreamingDialog({
          isOpen: true,
          logs: '',
          isStreaming: true,
          deploymentComplete: false,
          deploymentSuccess: false,
          requestId: null,
        });

        try {
          const response = await handleDeploySSHPool(poolName);
          const requestId = response.request_id;

          setStreamingDialog((prev) => ({ ...prev, requestId }));

          // Create an AbortController for this streaming session
          const abortController = new AbortController();

          await streamSSHDeploymentLogs({
            requestId,
            signal: abortController.signal,
            onNewLog: (log) => {
              setStreamingDialog((prev) => ({
                ...prev,
                logs: prev.logs + log,
              }));
            },
          });

          // Deployment completed successfully
          setStreamingDialog((prev) => ({
            ...prev,
            isStreaming: false,
            deploymentComplete: true,
            deploymentSuccess: true,
          }));

          // Refresh status after successful deployment
          setTimeout(async () => {
            const fetchStatus = async () => {
              try {
                const status = await getSSHNodePoolStatus(poolName);
                setStatusData(status);
              } catch (error) {
                console.error(
                  'Failed to fetch SSH Node Pool status after deployment:',
                  error
                );
              }
            };
            fetchStatus();
          }, 1000);
        } catch (error) {
          console.error('Deployment failed:', error);
          setStreamingDialog((prev) => ({
            ...prev,
            isStreaming: false,
            deploymentComplete: true,
            deploymentSuccess: false,
            logs: prev.logs + `\nDeployment failed: ${error.message}`,
          }));
        }
      } else if (confirmDialog.action === 'delete') {
        // Hide confirmation dialog and show streaming dialog
        setConfirmDialog({ isOpen: false, action: null, loading: false });
        setStreamingDialog({
          isOpen: true,
          logs: '',
          isStreaming: true,
          deploymentComplete: false,
          deploymentSuccess: false,
          requestId: null,
        });

        try {
          // Step 1: Call sshDownNodePool to get request_id for streaming
          const downResponse = await sshDownNodePool(poolName);
          const requestId = downResponse.request_id;

          setStreamingDialog((prev) => ({ ...prev, requestId }));

          if (requestId) {
            // Stream the down operation logs
            await streamSSHOperationLogs({
              requestId,
              signal: null, // No abort signal for now
              onNewLog: (log) => {
                setStreamingDialog((prev) => ({
                  ...prev,
                  logs: prev.logs + log,
                }));
              },
              operationType: 'down',
            });
          }

          // Step 2: After streaming completes, call the parent's delete handler
          // which will handle the actual deletion and navigation
          await handleDeleteSSHPool(poolName);

          // Down operation completed successfully
          setStreamingDialog((prev) => ({
            ...prev,
            isStreaming: false,
            deploymentComplete: true,
            deploymentSuccess: true,
            logs:
              prev.logs + '\nSSH Node Pool teardown completed successfully.',
          }));
        } catch (error) {
          console.error('Down operation failed:', error);
          setStreamingDialog((prev) => ({
            ...prev,
            isStreaming: false,
            deploymentComplete: true,
            deploymentSuccess: false,
            logs: prev.logs + `\nTeardown failed: ${error.message}`,
          }));
        }
      }
    } catch (error) {
      console.error('Action failed:', error);
      setConfirmDialog({ ...confirmDialog, loading: false });
    }
  };

  const handleCancelAction = () => {
    setConfirmDialog({ isOpen: false, action: null, loading: false });
  };

  const handleCloseStreamingDialog = () => {
    setStreamingDialog({
      isOpen: false,
      logs: '',
      isStreaming: false,
      deploymentComplete: false,
      deploymentSuccess: false,
      requestId: null,
    });

    // Refresh status after deployment
    if (streamingDialog.deploymentComplete) {
      setTimeout(() => {
        const fetchStatus = async () => {
          try {
            const status = await getSSHNodePoolStatus(poolName);
            setStatusData(status);
          } catch (error) {
            console.error('Failed to refresh status:', error);
          }
        };
        fetchStatus();
      }, 1000);
    }
  };

  const getDialogContent = () => {
    if (confirmDialog.action === 'deploy') {
      return {
        title: 'Deploy SSH Node Pool',
        description: `Are you sure you want to deploy SSH Node Pool "${poolName}"?`,
        details: [
          '• Set up SkyPilot runtime on the configured SSH hosts',
          '• Install required components and dependencies',
          '• Make the node pool available for workloads',
          '',
          'This process may take a few minutes to complete.',
        ],
      };
    } else {
      return {
        title: 'Delete SSH Node Pool',
        description: `Are you sure you want to delete SSH Node Pool "${poolName}"?`,
        details: [
          '• Clean up any deployed resources',
          '• Remove the SSH Node Pool configuration',
        ],
      };
    }
  };

  const dialogContent = getDialogContent();

  return (
    <div>
      <PluginSlot name="infra.sshDetail.statusPanel" context={{ poolName }} />
      {/* SSH Node Pool Info Card */}
      <div className="mb-6">
        <div className="rounded-lg border bg-card text-card-foreground shadow-sm">
          <div className="flex items-center justify-between px-4 pt-4">
            <h3 className="text-lg font-semibold">SSH Node Pool Details</h3>
            <div className="flex items-center space-x-2">
              <button
                className={`px-3 py-1 text-sm border rounded flex items-center ${
                  deployDisabled
                    ? 'border-gray-300 bg-gray-100 text-gray-400 cursor-not-allowed'
                    : 'border-green-300 bg-green-50 text-green-700 hover:bg-green-100'
                }`}
                onClick={deployDisabled ? undefined : handleDeployClick}
                disabled={deployDisabled}
              >
                <PlayIcon className="w-4 h-4 mr-2" />
                Deploy
              </button>
              <button
                className="px-3 py-1 text-sm border border-gray-300 rounded hover:bg-gray-50 flex items-center text-red-600 hover:text-red-700"
                onClick={handleDeleteClick}
              >
                <TrashIcon className="w-4 h-4 mr-2" />
                Delete
              </button>
            </div>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-2 gap-6">
              <div>
                <div className="text-gray-600 font-medium text-base">
                  Pool Name
                </div>
                <div className="text-base mt-1">{poolName}</div>
              </div>
              <div>
                <div className="text-gray-600 font-medium text-base">Nodes</div>
                <div className="text-base mt-1">
                  {nodesInContext ? nodesInContext.length : 0}
                </div>
              </div>
              <div>
                <div className="text-gray-600 font-medium text-base">
                  Status
                </div>
                <div className="text-base mt-1">
                  {statusLoading ? (
                    <div className="flex items-center">
                      <CircularProgress size={16} className="mr-2" />
                      <span className="text-gray-500">Loading...</span>
                    </div>
                  ) : statusData ? (
                    <StatusBadge
                      status={statusData.status}
                      reason={statusData.reason}
                    />
                  ) : (
                    <span className="text-gray-500">Unknown</span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* GPU and Node Details */}
      <ContextDetails
        contextName={`ssh-${poolName}`}
        gpusInContext={gpusInContext}
        nodesInContext={nodesInContext}
      />

      {/* Confirmation Dialog */}
      <Dialog open={confirmDialog.isOpen} onOpenChange={handleCancelAction}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader className="">
            <DialogTitle className="">{dialogContent.title}</DialogTitle>
            <DialogDescription className="">
              {dialogContent.description}
            </DialogDescription>
          </DialogHeader>

          <div className="py-4">
            <div className="text-sm text-gray-600 space-y-1">
              <p className="font-medium mb-2">This will:</p>
              {dialogContent.details.map((detail, index) => (
                <p key={index} className={detail === '' ? 'pt-2' : ''}>
                  {detail}
                </p>
              ))}
            </div>
          </div>

          <DialogFooter className="">
            <Button
              variant="outline"
              onClick={handleCancelAction}
              disabled={confirmDialog.loading}
              className=""
            >
              Cancel
            </Button>
            <Button
              onClick={handleConfirmAction}
              disabled={confirmDialog.loading}
              className={
                confirmDialog.action === 'deploy'
                  ? 'bg-green-600 hover:bg-green-700 text-white'
                  : 'bg-red-600 hover:bg-red-700 text-white'
              }
            >
              {confirmDialog.loading ? (
                <>
                  <CircularProgress size={16} className="mr-2" />
                  {confirmDialog.action === 'deploy'
                    ? 'Deploying...'
                    : 'Deleting...'}
                </>
              ) : confirmDialog.action === 'deploy' ? (
                'Deploy'
              ) : (
                'Delete'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Deployment Streaming Dialog */}
      <Dialog
        open={streamingDialog.isOpen}
        onOpenChange={
          !streamingDialog.isStreaming ? handleCloseStreamingDialog : undefined
        }
      >
        <DialogContent className="sm:max-w-4xl max-h-[80vh]">
          <DialogHeader className="">
            <DialogTitle className="">
              Deploying SSH Node Pool: {poolName}
            </DialogTitle>
            <DialogDescription className="">
              {streamingDialog.isStreaming
                ? 'Deployment in progress. Do not close this dialog.'
                : streamingDialog.deploymentSuccess
                  ? 'Deployment completed successfully!'
                  : 'Deployment completed with errors.'}
            </DialogDescription>
          </DialogHeader>

          <div className="py-4">
            <div className="bg-black text-green-400 p-4 rounded-md font-mono text-sm max-h-96 overflow-y-auto">
              <pre className="whitespace-pre-wrap">
                {cleanSSHDeploymentLogs(streamingDialog.logs)}
              </pre>
              {streamingDialog.isStreaming && (
                <div className="flex items-center mt-2">
                  <CircularProgress size={16} className="mr-2 text-green-400" />
                  <span className="text-green-400">Streaming logs...</span>
                </div>
              )}
            </div>
          </div>

          <DialogFooter className="">
            <Button
              onClick={handleCloseStreamingDialog}
              disabled={streamingDialog.isStreaming}
              className={
                streamingDialog.deploymentSuccess
                  ? 'bg-green-600 hover:bg-green-700 text-white'
                  : streamingDialog.deploymentComplete &&
                      !streamingDialog.deploymentSuccess
                    ? 'bg-red-600 hover:bg-red-700 text-white'
                    : 'bg-gray-600 hover:bg-gray-700 text-white'
              }
            >
              {streamingDialog.isStreaming ? (
                <>
                  <CircularProgress size={16} className="mr-2" />
                  Deploying...
                </>
              ) : (
                'Close'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

// SSH Node Pool Table component with status fetching
