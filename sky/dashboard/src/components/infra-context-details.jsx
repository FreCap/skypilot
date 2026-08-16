import React, { useState, useEffect, useCallback } from 'react';
import { CircularProgress } from '@mui/material';

import { checkGrafanaAvailability, getGrafanaUrl } from '@/utils/grafana';
import { formatCpu } from '@/utils/resourceUtils';
import { canonicalizeGpuName } from '@/utils/gpuUtils';
import { trackInfraAction } from '@/lib/analytics';
import { PluginSlot } from '@/plugins/PluginSlot';

// Tooltip text for the preemptible segment: the priority classes holding
// those accelerators, largest first, e.g.
//   "66 preemptible attributed to non-SkyServe pods
//    inference-low (-1000): 60
//    drill (-500): 6"
const buildPreemptibleTitle = (preemptible, breakdown) => {
  const header =
    `${preemptible} preemptible attributed to non-SkyServe pods ` +
    `(reclaimable by higher-priority workloads)`;
  const entries = Object.entries(breakdown || {}).sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) {
    return header;
  }
  return [header, ...entries.map(([label, qty]) => `${label}: ${qty}`)].join(
    '\n'
  );
};

const buildServicePreemptibleTitle = (preemptible, breakdown) => {
  const header =
    `${preemptible} preemptible attributed to SkyServe pods ` +
    `(reclaimable by higher-priority workloads)`;
  const entries = Object.entries(breakdown || {}).sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) {
    return header;
  }
  return [
    header,
    ...entries.map(([service, qty]) => `${service}: ${qty}`),
  ].join('\n');
};

const buildUnknownPreemptibleTitle = (preemptible, breakdown) => {
  const header =
    `${preemptible} preemptible; SkyServe pod attribution unavailable ` +
    `(pods lack durable SkyPilot workload identity)`;
  const entries = Object.entries(breakdown || {}).sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) {
    return header;
  }
  return [header, ...entries.map(([label, qty]) => `${label}: ${qty}`)].join(
    '\n'
  );
};

const GpuUtilizationBar = ({
  gpu,
  heightClass = 'h-4',
  wrapperClassName = '',
}) => {
  const total = gpu?.gpu_total || 0;
  const notReady = gpu?.gpu_not_ready || 0;
  const free = gpu?.gpu_free || 0;
  const allUsed = Math.max(0, total - free - notReady);
  // Accelerators held below the cluster's top priority tier. Clamped to what
  // is actually in use so a stale or partial reading can never render a
  // segment wider than the used block it splits.
  const preemptible = Math.min(Math.max(0, gpu?.gpu_preemptible || 0), allUsed);
  const serviceAttributionKnown = gpu?.gpu_preemptible_services != null;
  const servicePreemptible = Math.min(
    Math.max(
      0,
      serviceAttributionKnown ? gpu?.gpu_preemptible_services || 0 : 0
    ),
    preemptible
  );
  const otherPreemptible = serviceAttributionKnown
    ? preemptible - servicePreemptible
    : 0;
  const unknownPreemptible = serviceAttributionKnown ? 0 : preemptible;
  const used = allUsed - preemptible;
  const notReadyLabel = `${notReady} not ready`;
  const usedLabel = `${used} used`;
  const servicePreemptibleLabel = `${servicePreemptible} SkyServe pods`;
  const otherPreemptibleLabel = `${otherPreemptible} confirmed other`;
  const unknownPreemptibleLabel = `${unknownPreemptible} attribution unknown`;
  const freeLabel = `${free} free`;
  const toPercentage = total > 0 ? (value) => (value / total) * 100 : () => 0;
  const notReadyPercentage = toPercentage(notReady);
  const usedPercentage = toPercentage(used);
  const servicePreemptiblePercentage = toPercentage(servicePreemptible);
  const otherPreemptiblePercentage = toPercentage(otherPreemptible);
  const unknownPreemptiblePercentage = toPercentage(unknownPreemptible);
  const freePercentage = toPercentage(free);

  return (
    <div
      className={`bg-gray-100 rounded-md flex overflow-hidden shadow-sm ${heightClass} ${wrapperClassName}`.trim()}
    >
      {notReadyPercentage > 0 && (
        <div
          style={{
            width: `${notReadyPercentage}%`,
            fontSize: 'clamp(8px, 1.2vw, 12px)',
          }}
          title={notReadyLabel}
          className="bg-gray-400 h-full flex items-center justify-center text-white font-medium overflow-hidden whitespace-nowrap px-1"
        >
          {notReadyPercentage > 15 && notReadyLabel}
        </div>
      )}
      {usedPercentage > 0 && (
        <div
          style={{
            width: `${usedPercentage}%`,
            fontSize: 'clamp(8px, 1.2vw, 12px)',
          }}
          title={usedLabel}
          className="bg-yellow-500 h-full flex items-center justify-center text-white font-medium overflow-hidden whitespace-nowrap px-1"
        >
          {usedPercentage > 15 && usedLabel}
        </div>
      )}
      {servicePreemptiblePercentage > 0 && (
        <div
          style={{
            width: `${servicePreemptiblePercentage}%`,
            fontSize: 'clamp(8px, 1.2vw, 12px)',
          }}
          title={buildServicePreemptibleTitle(
            servicePreemptible,
            gpu?.gpu_preemptible_service_breakdown
          )}
          className="bg-violet-300 h-full flex items-center justify-center text-violet-950 font-medium overflow-hidden whitespace-nowrap px-1"
        >
          {servicePreemptiblePercentage > 15 && servicePreemptibleLabel}
        </div>
      )}
      {otherPreemptiblePercentage > 0 && (
        <div
          style={{
            width: `${otherPreemptiblePercentage}%`,
            fontSize: 'clamp(8px, 1.2vw, 12px)',
          }}
          title={buildPreemptibleTitle(
            otherPreemptible,
            servicePreemptible === 0 ? gpu?.gpu_preemptible_breakdown : null
          )}
          className="bg-amber-300 h-full flex items-center justify-center text-amber-900 font-medium overflow-hidden whitespace-nowrap px-1"
        >
          {otherPreemptiblePercentage > 15 && otherPreemptibleLabel}
        </div>
      )}
      {unknownPreemptiblePercentage > 0 && (
        <div
          style={{
            width: `${unknownPreemptiblePercentage}%`,
            fontSize: 'clamp(8px, 1.2vw, 12px)',
          }}
          title={buildUnknownPreemptibleTitle(
            unknownPreemptible,
            gpu?.gpu_preemptible_breakdown
          )}
          className="bg-orange-300 h-full flex items-center justify-center text-orange-950 font-medium overflow-hidden whitespace-nowrap px-1"
        >
          {unknownPreemptiblePercentage > 15 && unknownPreemptibleLabel}
        </div>
      )}
      {freePercentage > 0 && (
        <div
          style={{
            width: `${freePercentage}%`,
            fontSize: 'clamp(8px, 1.2vw, 12px)',
          }}
          title={freeLabel}
          className="bg-green-700 h-full flex items-center justify-center text-white font-medium overflow-hidden whitespace-nowrap px-1"
        >
          {freePercentage > 15 && freeLabel}
        </div>
      )}
    </div>
  );
};

export function ContextDetails({
  contextName,
  gpusInContext,
  nodesInContext,
  gpuMetricsRefreshTrigger = 0,
  isSlurm = false,
}) {
  // Determine if this is an SSH context
  const isSSHContext = contextName.startsWith('ssh-');
  const displayTitle = isSSHContext ? 'Node Pool' : 'Context';

  // State for filtering controls
  const [availableHosts, setAvailableHosts] = useState([]);
  const [selectedHosts, setSelectedHosts] = useState('$__all');
  const [timeRange, setTimeRange] = useState({
    from: 'now-1h',
    to: 'now',
  });
  const [isLoadingHosts, setIsLoadingHosts] = useState(false);
  const [isGrafanaAvailable, setIsGrafanaAvailable] = useState(false);

  // Check Grafana availability on mount
  useEffect(() => {
    const checkGrafana = async () => {
      const available = await checkGrafanaAvailability();
      setIsGrafanaAvailable(available);
    };

    if (typeof window !== 'undefined') {
      checkGrafana();
    }
  }, []);

  // Function to fetch available hosts from Prometheus for the specific cluster
  const fetchAvailableHosts = useCallback(async () => {
    if (!isGrafanaAvailable) return;

    setIsLoadingHosts(true);
    try {
      const grafanaUrl = getGrafanaUrl();
      const clusterParam = contextName === 'in-cluster' ? '^$' : contextName;

      // Build query to get nodes for specific cluster
      const query =
        'query=' +
        encodeURIComponent(
          `group by (node) (DCGM_FI_DEV_GPU_TEMP{cluster=~"${clusterParam}"} or label_replace(amd_gpu_gfx_activity{cluster=~"${clusterParam}"}, "node", "$1", "hostname", "(.*)"))`
        );

      const endpoint = `/api/datasources/proxy/1/api/v1/query?${query}`;

      try {
        const response = await fetch(`${grafanaUrl}${endpoint}`, {
          method: 'GET',
          credentials: 'include',
          headers: {
            Accept: 'application/json',
          },
        });

        if (response.ok) {
          const data = await response.json();
          if (data.data && data.data.result && data.data.result.length > 0) {
            const nodes = data.data.result
              .map((result) => result.metric.node)
              .filter(Boolean)
              .sort();
            setAvailableHosts(nodes);
            console.log(
              `Successfully fetched hosts for cluster ${clusterParam || 'in-cluster'}:`,
              nodes
            );
          } else {
            console.log('No nodes found for this cluster');
            setAvailableHosts([]);
          }
        } else {
          console.log(
            `HTTP ${response.status} from ${endpoint}: ${response.statusText}`
          );
          setAvailableHosts([]);
        }
      } catch (error) {
        console.log(`Failed to fetch from ${endpoint}:`, error);
        setAvailableHosts([]);
      }
    } catch (error) {
      console.error('Error fetching available hosts:', error);
      setAvailableHosts([]);
    } finally {
      setIsLoadingHosts(false);
    }
  }, [isGrafanaAvailable, contextName]);

  // Fetch hosts when component mounts and Grafana is available
  useEffect(() => {
    if (isGrafanaAvailable && nodesInContext && nodesInContext.length > 0) {
      fetchAvailableHosts();
    }
  }, [nodesInContext, isGrafanaAvailable, fetchAvailableHosts]);

  // Function to build Grafana panel URL with filters
  const buildGrafanaUrlForContext = (panelId) => {
    const grafanaUrl = getGrafanaUrl();
    // When "All Nodes" is selected (.*), pass .* directly to match all nodes
    const hostParam = selectedHosts;

    // Cluster parameter logic for k8s contexts only
    // For in-cluster: regex to match only missing/empty cluster labels
    // For external clusters: exact cluster name
    const clusterParam = contextName === 'in-cluster' ? '^$' : contextName;

    return `${grafanaUrl}/d-solo/skypilot-dcgm-cluster-dashboard/skypilot-dcgm-kubernetes-cluster-dashboard?orgId=1&timezone=browser&var-datasource=prometheus&var-host=${encodeURIComponent(hostParam)}&var-gpu=$__all&var-cluster=${encodeURIComponent(clusterParam)}&refresh=5s&theme=light&from=${encodeURIComponent(timeRange.from)}&to=${encodeURIComponent(timeRange.to)}&panelId=${panelId}&__feature.dashboardSceneSolo`;
  };

  // Handle host selection change
  const handleHostChange = (event) => {
    setSelectedHosts(event.target.value);
  };

  // Handle time range preset change
  const handleTimeRangePreset = (preset) => {
    trackInfraAction('time_range_change', { range: preset });
    const presets = {
      '15m': { from: 'now-15m', to: 'now' },
      '1h': { from: 'now-1h', to: 'now' },
      '6h': { from: 'now-6h', to: 'now' },
      '24h': { from: 'now-24h', to: 'now' },
      '7d': { from: 'now-7d', to: 'now' },
    };
    setTimeRange(presets[preset]);
  };

  return (
    <div className="mb-4">
      {/* infra.contextDetail.headerActions used to render here, but the
          actions look right next to the page heading rather than above
          the panels. The slot is now rendered alongside the h1 in the
          GPUs component's return. */}
      <PluginSlot
        name="infra.contextDetail.statusPanel"
        context={{ contextName, isSlurm }}
      />
      <div className="rounded-lg border bg-card text-card-foreground shadow-sm h-full">
        <div className="p-5">
          <div className="flex items-center justify-between mb-4">
            <h4 className="text-lg font-semibold">Nodes</h4>
          </div>
          {gpusInContext.length > 0 && (
            <div className="mb-6">
              <h4 className="text-base font-semibold mb-3">Available GPUs</h4>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {gpusInContext.map((gpu) => {
                  return (
                    <div
                      key={gpu.gpu_name}
                      className="p-3 bg-gray-50 rounded-md border border-gray-200 shadow-sm"
                    >
                      <div className="flex justify-between items-center mb-1.5 flex-wrap">
                        <div className="font-medium text-gray-800 text-sm">
                          {canonicalizeGpuName(gpu.gpu_name)}
                          <span className="text-xs text-gray-500 ml-2">
                            (Requestable: {gpu.gpu_requestable_qty_per_node} /
                            node)
                          </span>
                        </div>
                        <span className="text-xs font-medium">
                          {gpu.gpu_free} free / {gpu.gpu_total} total
                        </span>
                      </div>
                      <div className="w-full">
                        <GpuUtilizationBar
                          gpu={gpu}
                          heightClass="h-4"
                          wrapperClassName="w-full"
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {nodesInContext.length > 0 && (
            <div className="overflow-x-auto rounded-md border border-gray-200 shadow-sm">
              <table className="min-w-full text-sm">
                <thead className="bg-gray-100">
                  <tr>
                    <th className="p-3 text-left font-medium text-gray-600">
                      Node
                    </th>
                    {!isSlurm && (
                      <>
                        <th className="p-3 text-left font-medium text-gray-600">
                          IP Address
                        </th>
                        <th className="p-3 text-left font-medium text-gray-600">
                          vCPU
                        </th>
                        <th className="p-3 text-left font-medium text-gray-600">
                          Memory (GB)
                        </th>
                      </>
                    )}
                    <th className="p-3 text-left font-medium text-gray-600">
                      GPU
                    </th>
                    <th className="p-3 text-left font-medium text-gray-600">
                      GPU Utilization
                    </th>
                    <th className="p-3 text-left font-medium text-gray-600">
                      Node Status
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {nodesInContext.map((node, index) => {
                    // Format CPU display: "X of Y free" or just "Y" if free is unknown
                    let cpuDisplay = '-';
                    if (
                      node.cpu_count !== null &&
                      node.cpu_count !== undefined
                    ) {
                      const cpuTotal = formatCpu(node.cpu_count);
                      if (
                        node.cpu_free !== null &&
                        node.cpu_free !== undefined
                      ) {
                        const cpuFree = formatCpu(node.cpu_free);
                        cpuDisplay = `${cpuFree} of ${cpuTotal} free`;
                      } else {
                        cpuDisplay = cpuTotal;
                      }
                    }

                    // Format memory display: "X of Y free" or just "Y" if free is unknown
                    // (GB is in column header, so don't include it in values)
                    let memoryDisplay = '-';
                    if (
                      node.memory_gb !== null &&
                      node.memory_gb !== undefined
                    ) {
                      const memoryTotal = node.memory_gb.toFixed(1);
                      if (
                        node.memory_free_gb !== null &&
                        node.memory_free_gb !== undefined
                      ) {
                        const memoryFree = node.memory_free_gb.toFixed(1);
                        memoryDisplay = `${memoryFree} of ${memoryTotal} free`;
                      } else {
                        memoryDisplay = memoryTotal;
                      }
                    }

                    // Build utilization string
                    const utilizationStr = `${node.gpu_free} of ${node.gpu_total} free`;

                    // Build node status string
                    const statusInfo = [];

                    // Add not ready info
                    if (node.is_ready === false) {
                      statusInfo.push('NotReady');
                    }

                    // Add cordoned info
                    if (node.is_cordoned === true) {
                      statusInfo.push('Cordoned');
                    }

                    // Build taint info separately. Taints whose
                    // `tolerated` flag is set by the backend (i.e. matched
                    // by `kubernetes.pod_config.spec.tolerations`) do not
                    // count against node health on the Infra page — they're
                    // surfaced in the GPU Manager drawer instead.
                    const taints = node.taints || [];
                    const untoleratedTaints = taints.filter(
                      (t) => t && t.tolerated !== true
                    );
                    let taintInfo = null;
                    if (untoleratedTaints.length > 0) {
                      const taintsByEffect = {};
                      for (const taint of untoleratedTaints) {
                        const effect = taint.effect;
                        const key = taint.key;
                        if (!taintsByEffect[effect]) {
                          taintsByEffect[effect] = [];
                        }
                        taintsByEffect[effect].push(key);
                      }
                      const taintStrs = Object.entries(taintsByEffect).map(
                        ([effect, keys]) =>
                          `${effect} Taint [${keys.join(', ')}]`
                      );
                      if (taintStrs.length > 0) {
                        taintInfo = taintStrs.join(', ');
                      }
                    }

                    const nodeStatusStr =
                      statusInfo.length > 0 || taintInfo
                        ? statusInfo.join(', ')
                        : 'Healthy';
                    const isNodeHealthy = statusInfo.length === 0 && !taintInfo;

                    return (
                      <tr
                        key={`${node.node_name}-${index}`}
                        className="hover:bg-gray-50"
                      >
                        <td className="p-3 whitespace-nowrap text-gray-700">
                          {node.node_name}
                        </td>
                        {!isSlurm && (
                          <>
                            <td className="p-3 whitespace-nowrap text-gray-700">
                              {node.ip_address || '-'}
                            </td>
                            <td className="p-3 whitespace-nowrap text-gray-700">
                              {cpuDisplay}
                            </td>
                            <td className="p-3 whitespace-nowrap text-gray-700">
                              {memoryDisplay}
                            </td>
                          </>
                        )}
                        <td className="p-3 whitespace-nowrap text-gray-700">
                          {canonicalizeGpuName(node.gpu_name)}
                        </td>
                        <td className="p-3 whitespace-nowrap text-gray-700">
                          {utilizationStr}
                        </td>
                        <td className="p-3 max-w-xs">
                          <div className="flex flex-col gap-1.5">
                            {nodeStatusStr && (
                              <span
                                className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium w-fit ${
                                  isNodeHealthy
                                    ? 'bg-emerald-50 text-emerald-700 ring-1 ring-inset ring-emerald-600/20'
                                    : 'bg-amber-50 text-amber-700 ring-1 ring-inset ring-amber-600/20'
                                }`}
                              >
                                {nodeStatusStr}
                              </span>
                            )}
                            {taintInfo && (
                              <span className="inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium w-fit bg-gray-50 text-gray-700 ring-1 ring-inset ring-gray-600/20">
                                {taintInfo}
                              </span>
                            )}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {/* GPU Metrics Section - only show for k8s contexts, not SSH node pools or Slurm */}
          {isGrafanaAvailable &&
            gpusInContext &&
            gpusInContext.length > 0 &&
            !isSSHContext &&
            !isSlurm && (
              <>
                <h4 className="text-lg font-semibold mb-4 mt-6">GPU Metrics</h4>

                {/* Filtering Controls */}
                <div className="mb-4 p-4 bg-gray-50 rounded-md border border-gray-200">
                  <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center">
                    {/* Host Selection - only show if we have node info */}
                    {nodesInContext && nodesInContext.length > 0 && (
                      <div className="flex items-center gap-2">
                        <label
                          htmlFor="host-select"
                          className="text-sm font-medium text-gray-700 whitespace-nowrap"
                        >
                          Node:
                        </label>
                        <select
                          id="host-select"
                          value={selectedHosts}
                          onChange={handleHostChange}
                          disabled={isLoadingHosts}
                          className="px-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-sky-blue focus:border-transparent"
                        >
                          <option value="$__all">All Nodes</option>
                          {availableHosts.map((host) => (
                            <option key={host} value={host}>
                              {host}
                            </option>
                          ))}
                        </select>
                        {isLoadingHosts && (
                          <div className="ml-2">
                            <CircularProgress size={16} />
                          </div>
                        )}
                      </div>
                    )}

                    {/* Time Range Selection */}
                    <div className="flex items-center gap-2">
                      <label className="text-sm font-medium text-gray-700 whitespace-nowrap">
                        Time Range:
                      </label>
                      <div className="flex gap-1">
                        {[
                          { label: '15m', value: '15m' },
                          { label: '1h', value: '1h' },
                          { label: '6h', value: '6h' },
                          { label: '24h', value: '24h' },
                          { label: '7d', value: '7d' },
                        ].map((preset) => (
                          <button
                            key={preset.value}
                            onClick={() => handleTimeRangePreset(preset.value)}
                            className={`px-2 py-1 text-xs font-medium rounded border transition-colors ${
                              timeRange.from === `now-${preset.value}` &&
                              timeRange.to === 'now'
                                ? 'bg-sky-blue text-white border-sky-blue'
                                : 'bg-white text-gray-600 border-gray-300 hover:bg-gray-50'
                            }`}
                          >
                            {preset.label}
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>

                  {/* Show current selection info */}
                  <div className="mt-2 text-xs text-gray-500">
                    {nodesInContext && nodesInContext.length > 0 ? (
                      <>
                        Showing:{' '}
                        {selectedHosts === '$__all'
                          ? 'All nodes'
                          : selectedHosts}{' '}
                        • Time: {timeRange.from} to {timeRange.to}
                        {availableHosts.length > 0 && (
                          <span>
                            {' '}
                            • {availableHosts.length} nodes available
                          </span>
                        )}
                      </>
                    ) : (
                      <>
                        Cluster:{' '}
                        {isSSHContext
                          ? contextName.replace(/^ssh-/, '')
                          : contextName}{' '}
                        • Time: {timeRange.from} to {timeRange.to} • Showing
                        metrics for all nodes in cluster
                      </>
                    )}
                  </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                  {/* GPU Utilization */}
                  <div className="bg-white rounded-md border border-gray-200 shadow-sm">
                    <div className="p-2">
                      <iframe
                        src={buildGrafanaUrlForContext('6')}
                        width="100%"
                        height="400"
                        frameBorder="0"
                        title="GPU Utilization"
                        className="rounded"
                        key={`gpu-util-${selectedHosts}-${timeRange.from}-${timeRange.to}-${gpuMetricsRefreshTrigger || 0}`}
                      />
                    </div>
                  </div>

                  {/* GPU Memory */}
                  <div className="bg-white rounded-md border border-gray-200 shadow-sm">
                    <div className="p-2">
                      <iframe
                        src={buildGrafanaUrlForContext('18')}
                        width="100%"
                        height="400"
                        frameBorder="0"
                        title="GPU Memory"
                        className="rounded"
                        key={`gpu-memory-${selectedHosts}-${timeRange.from}-${timeRange.to}-${gpuMetricsRefreshTrigger || 0}`}
                      />
                    </div>
                  </div>

                  {/* GPU Power Consumption */}
                  <div className="bg-white rounded-md border border-gray-200 shadow-sm">
                    <div className="p-2">
                      <iframe
                        src={buildGrafanaUrlForContext('10')}
                        width="100%"
                        height="400"
                        frameBorder="0"
                        title="GPU Power Consumption"
                        className="rounded"
                        key={`gpu-power-${selectedHosts}-${timeRange.from}-${timeRange.to}-${gpuMetricsRefreshTrigger || 0}`}
                      />
                    </div>
                  </div>

                  {/* GPU Temperature */}
                  <div className="bg-white rounded-md border border-gray-200 shadow-sm">
                    <div className="p-2">
                      <iframe
                        src={buildGrafanaUrlForContext('12')}
                        width="100%"
                        height="400"
                        frameBorder="0"
                        title="GPU Temperature"
                        className="rounded"
                        key={`gpu-temp-${selectedHosts}-${timeRange.from}-${timeRange.to}-${gpuMetricsRefreshTrigger || 0}`}
                      />
                    </div>
                  </div>

                  {/* CPU Utilization */}
                  <div className="bg-white rounded-md border border-gray-200 shadow-sm">
                    <div className="p-2">
                      <iframe
                        src={buildGrafanaUrlForContext('22')}
                        width="100%"
                        height="400"
                        frameBorder="0"
                        title="CPU Utilization"
                        className="rounded"
                        key={`cpu-util-${selectedHosts}-${timeRange.from}-${timeRange.to}-${gpuMetricsRefreshTrigger || 0}`}
                      />
                    </div>
                  </div>

                  {/* Memory Utilization */}
                  <div className="bg-white rounded-md border border-gray-200 shadow-sm">
                    <div className="p-2">
                      <iframe
                        src={buildGrafanaUrlForContext('21')}
                        width="100%"
                        height="400"
                        frameBorder="0"
                        title="Memory Utilization"
                        className="rounded"
                        key={`memory-util-${selectedHosts}-${timeRange.from}-${timeRange.to}-${gpuMetricsRefreshTrigger || 0}`}
                      />
                    </div>
                  </div>
                </div>
              </>
            )}
        </div>
      </div>
    </div>
  );
}

export { GpuUtilizationBar };
