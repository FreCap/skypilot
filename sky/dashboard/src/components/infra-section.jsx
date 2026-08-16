import React from 'react';
import { CircularProgress } from '@mui/material';
import { AlertTriangleIcon } from 'lucide-react';

import { GpuUtilizationBar } from '@/components/infra-context-details';
import { NonCapitalizedTooltip } from '@/components/utils';
import { PluginSlot } from '@/plugins/PluginSlot';
import { UI_CONFIG } from '@/lib/config';
import {
  calculateAggregatedResource,
  formatCpu,
  formatMemory,
} from '@/utils/resourceUtils';
import { buildContextStatsKey } from '@/utils/infraUtils';
import { canonicalizeGpuName } from '@/utils/gpuUtils';

const NAME_TRUNCATE_LENGTH = UI_CONFIG.NAME_TRUNCATE_LENGTH;
const TABLE_MAX_ROWS_BEFORE_SCROLL = 5;

export const SkeletonBadge = () => (
  <span className="px-2 py-0.5 bg-muted rounded text-xs font-medium inline-flex items-center">
    <span
      className="infra-skeleton-text"
      style={{ width: '20px', height: '12px' }}
    />
  </span>
);

// Reusable component for infrastructure sections (SSH Node Pool or Kubernetes)
export function InfrastructureSection({
  title,
  isLoading,
  isDataLoaded,
  contexts,
  gpus,
  groupedPerContextGPUs,
  groupedPerNodeGPUs,
  handleContextClick,
  contextStats = {},
  jobsData = {},
  isJobsDataLoading = true,
  isClusterDataLoading = true, // Loading state for cluster data
  isSSH = false, // To differentiate between SSH and Kubernetes
  isSlurm = false, // To differentiate Slurm clusters
  actionButton = null, // Optional action button for the header
  contextWorkspaceMap = {}, // Mapping of contexts to workspaces
  contextErrors = {}, // Mapping of contexts to error messages
  gpuMetricsRefreshTrigger = 0, // Counter for forcing iframe refresh
  loadedContexts = new Set(), // Set of contexts that have had their GPU data loaded
  isInitialLoad = true, // Controls panel-level loading spinner (not cell spinners)
  statusByKey = null, // Map<`${kind}:${id}`, Status> from plugin data providers
}) {
  // Add defensive check for contexts
  const safeContexts = contexts || [];

  // Only show "no data" message after data has been loaded and confirmed empty
  // Check this FIRST so that during refresh, we keep showing the message instead of a spinner
  if (isDataLoaded && safeContexts.length === 0) {
    return (
      <div className="rounded-lg border bg-card text-card-foreground shadow-sm mb-6">
        <div className="p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold">{title}</h3>
            {actionButton}
          </div>
          <p className="text-sm text-gray-500">
            No {title} found or {title} is not configured.
          </p>
        </div>
      </div>
    );
  }

  // Show panel-level loading spinner ONLY during initial load
  // On subsequent refreshes, show the table with cell-level spinners instead
  if (
    isInitialLoad &&
    isLoading &&
    !isDataLoaded &&
    safeContexts.length === 0
  ) {
    return (
      <div className="rounded-lg border bg-card text-card-foreground shadow-sm mb-6">
        <div className="p-5">
          <h3 className="text-lg font-semibold mb-4">{title}</h3>
          <div className="flex items-center justify-center py-6">
            <CircularProgress size={24} className="mr-3" />
            <span className="text-gray-500">Loading {title}...</span>
          </div>
        </div>
      </div>
    );
  }

  // Determine if table should show refreshing state
  // For K8s: show during loading or when contexts haven't all loaded yet
  // For SSH/Slurm: only show during loading
  const isTableRefreshing =
    !isInitialLoad &&
    (isLoading ||
      (!(isSlurm || isSSH) &&
        safeContexts.length > 0 &&
        !safeContexts.every((c) => loadedContexts.has(c))));

  // Show table if we have contexts to display, even if some data is still loading
  if (safeContexts.length > 0) {
    return (
      <div className="rounded-lg border bg-card text-card-foreground shadow-sm mb-6">
        <div className="p-5">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center">
              <h3 className="text-lg font-semibold">{title}</h3>
              <span className="ml-2 px-2 py-0.5 bg-blue-100 text-blue-800 rounded-full text-xs font-medium">
                {safeContexts.length}{' '}
                {safeContexts.length === 1
                  ? isSSH
                    ? 'pool'
                    : isSlurm
                      ? 'cluster'
                      : 'context'
                  : isSSH
                    ? 'pools'
                    : isSlurm
                      ? 'clusters'
                      : 'contexts'}
              </span>
            </div>
            {actionButton}
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <div
                className={`overflow-x-auto rounded-md border shadow-sm bg-card ${
                  isTableRefreshing ? 'infra-table-refreshing' : ''
                } ${safeContexts.length > TABLE_MAX_ROWS_BEFORE_SCROLL ? 'max-h-[300px] overflow-y-auto' : ''}`}
              >
                <table className="min-w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0 z-10">
                    <tr>
                      <th className="p-3 text-left font-medium text-gray-600 w-1/4">
                        Name
                      </th>
                      <th className="p-3 text-left font-medium text-gray-600 w-1/8">
                        Clusters
                      </th>
                      <th className="p-3 text-left font-medium text-gray-600 w-1/8">
                        Jobs
                      </th>
                      <th className="p-3 text-left font-medium text-gray-600 w-1/8">
                        Nodes
                      </th>
                      {!isSlurm && (
                        <th className="p-3 text-left font-medium text-gray-600 w-1/8">
                          CPU
                        </th>
                      )}
                      {!isSlurm && (
                        <th className="p-3 text-left font-medium text-gray-600 w-1/6">
                          Memory
                        </th>
                      )}
                      <th className="p-3 text-left font-medium text-gray-600 w-1/6">
                        GPU Types
                      </th>
                      <th className="p-3 text-left font-medium text-gray-600 w-1/8">
                        GPUs
                      </th>
                      <th className="p-3 text-right font-medium text-gray-600 w-12" />
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {safeContexts.map((context) => {
                      const gpus = groupedPerContextGPUs[context] || [];
                      const nodes = groupedPerNodeGPUs[context] || [];
                      const totalGpus = gpus.reduce(
                        (sum, gpu) => sum + (gpu.gpu_total || 0),
                        0
                      );

                      // Get cluster and job counts for this context
                      const contextStatsKey = buildContextStatsKey(context, {
                        isSSH,
                        isSlurm,
                      });
                      const stats = contextStats[contextStatsKey] || {
                        clusters: 0,
                        jobs: 0,
                      };

                      // Check if GPU/Node data is available for THIS specific context
                      // For Kubernetes: use progressive loading (check loadedContexts)
                      // For Slurm/SSH: data is fetched all at once, so use isLoading
                      const hasGpuData =
                        isSlurm || isSSH
                          ? !isLoading
                          : loadedContexts.has(context);
                      const hasNodeData =
                        isSlurm || isSSH
                          ? !isLoading
                          : loadedContexts.has(context);

                      // Format GPU types based on context type
                      // Always calculate from available data (show stale values during refresh)
                      const gpuTypes = (() => {
                        if (gpus.length === 0) return null;
                        const typeCounts = gpus.reduce((acc, gpu) => {
                          const name = canonicalizeGpuName(gpu.gpu_name);
                          acc[name] = (acc[name] || 0) + (gpu.gpu_total || 0);
                          return acc;
                        }, {});

                        return Object.keys(typeCounts).join(', ');
                      })();

                      // Calculate aggregated CPU and memory for this context
                      // Always calculate from available data (show stale values during refresh)
                      const aggregatedCpu = calculateAggregatedResource(
                        nodes,
                        'cpu_count',
                        true // Always calculate if nodes exist
                      );
                      const aggregatedMemory = calculateAggregatedResource(
                        nodes,
                        'memory_gb',
                        true // Always calculate if nodes exist
                      );

                      // Format display name for SSH contexts
                      const displayName = isSSH
                        ? context.replace(/^ssh-/, '')
                        : context;

                      // Stable id/kind for both row PluginSlots below.
                      const rowId = displayName;
                      const rowKind = isSSH ? 'ssh' : isSlurm ? 'slurm' : 'k8s';

                      // Get workspace information for this context
                      const workspaces = contextWorkspaceMap[context] || [];
                      const workspaceDisplay =
                        workspaces.length > 1
                          ? ` (workspaces: ${workspaces.join(', ')})`
                          : '';

                      return (
                        <tr
                          key={context}
                          className={`hover:bg-muted/50 ${
                            !hasGpuData && !isInitialLoad
                              ? 'infra-loading-row'
                              : ''
                          }`}
                        >
                          <td className="p-3">
                            <div className="flex items-center gap-1.5">
                              <PluginSlot
                                name="infra.row.namePrefix"
                                context={{
                                  id: rowId,
                                  kind: rowKind,
                                  status: statusByKey?.get(
                                    `${rowKind}:${rowId}`
                                  ),
                                }}
                              />
                              <NonCapitalizedTooltip
                                content={`${displayName}${workspaceDisplay}`}
                                className="text-sm text-muted-foreground"
                              >
                                <span
                                  className="text-blue-600 hover:underline cursor-pointer"
                                  onClick={() => handleContextClick(context)}
                                >
                                  {displayName.length > NAME_TRUNCATE_LENGTH
                                    ? `${displayName.substring(0, Math.floor((NAME_TRUNCATE_LENGTH - 3) / 2))}...${displayName.substring(displayName.length - Math.ceil((NAME_TRUNCATE_LENGTH - 3) / 2))}`
                                    : displayName}
                                  {workspaceDisplay && (
                                    <span className="text-xs text-gray-500 ml-1">
                                      {workspaceDisplay}
                                    </span>
                                  )}
                                </span>
                              </NonCapitalizedTooltip>
                              {contextErrors[context] && !statusByKey && (
                                // Hidden when a plugin is contributing status data
                                // via the infra.row.namePrefix slot — the plugin's
                                // status dot + per-context detail page cover this
                                // information already.
                                <NonCapitalizedTooltip
                                  content={`Context unreachable: ${contextErrors[context]}`}
                                  className="text-sm text-muted-foreground"
                                >
                                  <AlertTriangleIcon className="w-4 h-4 text-yellow-500 flex-shrink-0" />
                                </NonCapitalizedTooltip>
                              )}
                            </div>
                          </td>
                          <td className="p-3">
                            {isClusterDataLoading ? (
                              <SkeletonBadge />
                            ) : (
                              <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                                {stats.clusters}
                              </span>
                            )}
                          </td>
                          <td className="p-3">
                            {isJobsDataLoading ? (
                              <SkeletonBadge />
                            ) : (
                              <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                                {jobsData[contextStatsKey]?.jobs || 0}
                              </span>
                            )}
                          </td>
                          <td className="p-3">
                            {!hasNodeData ? (
                              <SkeletonBadge />
                            ) : (
                              <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                                {nodes.length}
                              </span>
                            )}
                          </td>
                          {!isSlurm && (
                            <td className="p-3">
                              {!hasNodeData ? (
                                <SkeletonBadge />
                              ) : (
                                <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                                  {formatCpu(aggregatedCpu)}
                                </span>
                              )}
                            </td>
                          )}
                          {!isSlurm && (
                            <td className="p-3">
                              {!hasNodeData ? (
                                <SkeletonBadge />
                              ) : (
                                <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                                  {formatMemory(aggregatedMemory)}
                                </span>
                              )}
                            </td>
                          )}
                          <td className="p-3">
                            {!hasGpuData ? (
                              <SkeletonBadge />
                            ) : (
                              <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                                {gpuTypes || '-'}
                              </span>
                            )}
                          </td>
                          <td className="p-3">
                            {!hasGpuData ? (
                              <SkeletonBadge />
                            ) : (
                              <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                                {totalGpus}
                              </span>
                            )}
                          </td>
                          <td className="p-3 text-right">
                            <PluginSlot
                              name="infra.row.actions"
                              context={{ id: rowId, kind: rowKind }}
                            />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
            {gpus && gpus.length > 0 && (
              <div>
                <div
                  className={`overflow-x-auto rounded-md border shadow-sm bg-card ${
                    isTableRefreshing ? 'infra-table-refreshing' : ''
                  } ${gpus.length > TABLE_MAX_ROWS_BEFORE_SCROLL ? 'max-h-[300px] overflow-y-auto' : ''}`}
                >
                  <table className="min-w-full text-sm">
                    <thead className="bg-gray-50 sticky top-0 z-10">
                      <tr>
                        <th className="p-3 text-left font-medium text-gray-600 w-1/4 whitespace-nowrap">
                          GPU
                          <span className="ml-2 px-2 py-0.5 bg-green-100 text-green-800 rounded-full text-xs font-medium whitespace-nowrap">
                            {gpus.reduce((sum, gpu) => sum + gpu.gpu_free, 0)}{' '}
                            of{' '}
                            {gpus.reduce((sum, gpu) => sum + gpu.gpu_total, 0)}{' '}
                            free
                          </span>
                        </th>
                        <th className="p-3 text-left font-medium text-gray-600 w-1/4">
                          Requestable
                        </th>
                        <th className="p-3 text-left font-medium text-gray-600 w-1/2">
                          <div className="flex items-center">
                            <span>Utilization</span>
                          </div>
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {gpus.map((gpu) => {
                        // Find the requestable quantities from contexts
                        const requestableQtys = groupedPerContextGPUs
                          ? Object.values(groupedPerContextGPUs)
                              .flat()
                              .filter((g) => {
                                if (
                                  canonicalizeGpuName(g.gpu_name) !==
                                  gpu.gpu_name
                                )
                                  return false;
                                if (isSlurm) return true; // For Slurm, include all
                                // For Kubernetes/SSH, filter by context type
                                const contextKey = g.context || g.cluster;
                                if (!contextKey) return false;
                                return isSSH
                                  ? contextKey.startsWith('ssh-')
                                  : !contextKey.startsWith('ssh-');
                              })
                              .map((g) => g.gpu_requestable_qty_per_node)
                              .filter((qty, i, arr) => arr.indexOf(qty) === i) // Unique values
                              .join(', ')
                          : '-';

                        return (
                          <tr key={gpu.gpu_name}>
                            <td className="p-3 font-medium w-24 whitespace-nowrap">
                              {canonicalizeGpuName(gpu.gpu_name)}
                            </td>
                            <td className="p-3 text-xs text-gray-600">
                              {requestableQtys || '-'} / node
                            </td>
                            <td className="p-3 w-2/3">
                              <div className="flex items-center gap-3">
                                <GpuUtilizationBar
                                  gpu={gpu}
                                  heightClass="h-5"
                                  wrapperClassName="flex-1 min-w-[100px] w-full"
                                  showPriorityPolicy={!isSSH && !isSlurm}
                                />
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  return null;
}
