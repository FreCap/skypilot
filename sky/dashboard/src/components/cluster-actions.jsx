'use client';

import React from 'react';
import { SquareCode, Terminal } from 'lucide-react';

import { CustomTooltip as Tooltip } from '@/components/utils';
import { useMobile } from '@/hooks/useMobile';
import { trackClusterAction } from '@/lib/analytics';

export const handleVSCodeConnection = (cluster, onOpenVSCodeModal) => {
  if (onOpenVSCodeModal) {
    onOpenVSCodeModal(cluster);
  }
};

const handleConnect = (cluster, onOpenSSHModal) => {
  if (onOpenSSHModal) {
    onOpenSSHModal(cluster);
  } else {
    const uri = `ssh://${cluster}`;
    window.open(uri);
  }
};

// TODO(hailong): The enabled actions are also related to the `cloud` of the cluster
export const enabledActions = (status) => {
  switch (status) {
    case 'RUNNING':
      return ['connect', 'VSCode'];
    default:
      return [];
  }
};

const actionIcons = {
  connect: <Terminal className="w-4 h-4 text-gray-500 inline-block" />,
  VSCode: <SquareCode className="w-4 h-4 text-gray-500 inline-block" />,
};

export function Status2Actions({
  withLabel = false,
  cluster,
  status,
  onOpenSSHModal,
  onOpenVSCodeModal,
}) {
  const actions = enabledActions(status);
  const isMobile = useMobile();

  const handleActionClick = (actionName) => {
    trackClusterAction(actionName, { status });
    switch (actionName) {
      case 'connect':
        handleConnect(cluster, onOpenSSHModal);
        break;
      case 'VSCode':
        handleVSCodeConnection(cluster, onOpenVSCodeModal);
        break;
      default:
        return;
    }
  };

  return (
    <>
      <div className="flex items-center space-x-4">
        {Object.entries(actionIcons).map(([actionName, actionIcon]) => {
          let label, tooltipText;
          switch (actionName) {
            case 'connect':
              label = 'Connect';
              tooltipText = 'Connect with SSH';
              break;
            case 'VSCode':
              label = 'VSCode';
              tooltipText = 'Open in VS Code';
              break;
            default:
              break;
          }
          if (!withLabel) {
            label = '';
          }
          if (actions.includes(actionName)) {
            return (
              <Tooltip
                key={actionName}
                content={tooltipText}
                className="capitalize text-sm text-muted-foreground"
              >
                <button
                  onClick={() => handleActionClick(actionName)}
                  className="text-sky-blue hover:text-sky-blue-bright font-medium inline-flex items-center"
                >
                  {actionIcon}
                  {!isMobile && <span className="ml-1.5">{label}</span>}
                </button>
              </Tooltip>
            );
          }
          return (
            <Tooltip
              key={actionName}
              content={tooltipText}
              className="capitalize text-sm text-muted-foreground"
            >
              <span
                className="opacity-30 flex items-center cursor-not-allowed text-sm"
                title={actionName}
              >
                {actionIcon}
                {!isMobile && <span className="ml-1.5">{label}</span>}
              </span>
            </Tooltip>
          );
        })}
      </div>
    </>
  );
}
