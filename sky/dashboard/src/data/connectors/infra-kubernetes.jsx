import { apiClient } from '@/data/connectors/client';
import { getErrorMessageFromResponse } from '@/data/utils';
import {
  emptySchedulingBreakdown,
  mergeSchedulingBreakdown,
} from '@/utils/gpuUtils';

/**
 * Returns true iff `nodeData` (a `KubernetesNodeInfo`-shaped object from
 * the API) should be excluded from free-GPU availability counts — i.e.,
 * it's not ready, cordoned, or carries at least one taint that the
 * configured `kubernetes.pod_config.spec.tolerations` does NOT tolerate.
 * Taints with `tolerated: true` (set by the backend for matching
 * tolerations) don't suppress availability.
 */
function isNodeNotReadyForGpus(nodeData) {
  const isReady = nodeData['is_ready'] !== false;
  const isCordoned = nodeData['is_cordoned'] === true;
  const isTainted = (nodeData['taints'] || []).some(
    (t) => t && t.tolerated !== true
  );
  return !isReady || isCordoned || isTainted;
}

/**
 * Fold a node's active scheduling breakdown into a per-GPU-type aggregate.
 *
 * Not-ready nodes are skipped: their whole capacity is already reported as
 * `gpu_not_ready`, so it is not part of the used block that the preemptible
 * segment subdivides, and counting it here would overdraw the bar.
 */
function addSchedulingFromNode(aggregate, nodeData, isNodeNotReady) {
  if (isNodeNotReady) return;
  mergeSchedulingBreakdown(aggregate, {
    gpu_allocation_breakdown: nodeData['allocation_breakdown'] || {},
    gpu_allocation_workload_breakdown:
      nodeData['allocation_workload_breakdown'] ?? null,
    gpu_preemptible: nodeData['accelerators_preemptible'] || 0,
    gpu_preemptible_breakdown: nodeData['preemptible_breakdown'] || {},
    gpu_preemptible_services: nodeData['accelerators_preemptible_services'],
    gpu_preemptible_service_breakdown:
      nodeData['preemptible_service_breakdown'],
    gpu_preemptible_service_priority_breakdown:
      nodeData['preemptible_service_priority_breakdown'],
    gpu_preemptible_service_priority_workload_breakdown:
      nodeData['preemptible_service_priority_workload_breakdown'] ?? null,
  });
}

// Fetch GPU data for a single context - used for progressive loading
// Returns processed GPU data for one context that can be merged into state
export async function getContextGPUData(context) {
  try {
    const nodeInfoDict = await getKubernetesPerNodeGPUs(context);

    // Process node info into GPU summaries
    const gpuToData = {};
    const perNodeGPUs = [];

    if (nodeInfoDict && Object.keys(nodeInfoDict).length > 0) {
      for (const nodeName in nodeInfoDict) {
        const nodeData = nodeInfoDict[nodeName];
        if (!nodeData) continue;

        const gpuName = nodeData['accelerator_type'] || '-';
        const totalCount = nodeData['total']?.['accelerator_count'] || 0;
        const freeCount = nodeData['free']?.['accelerators_available'] || 0;
        const isNodeNotReady = isNodeNotReadyForGpus(nodeData);

        // Per-node data - use same field names as original getKubernetesGPUsFromContexts
        perNodeGPUs.push({
          node_name: nodeData['name'] || nodeName,
          gpu_name: gpuName,
          gpu_total: totalCount,
          gpu_free: freeCount,
          is_ready: nodeData['is_ready'] !== false,
          is_cordoned: nodeData['is_cordoned'] === true,
          taints: nodeData['taints'] || [],
          context: context,
          ip_address: nodeData['ip_address'] || null,
          cpu_count: nodeData['cpu_count'] ?? null,
          memory_gb: nodeData['memory_gb'] ?? null,
          cpu_free: nodeData['cpu_free'] ?? null,
          memory_free_gb: nodeData['memory_free_gb'] ?? null,
          gpu_preemptible: nodeData['accelerators_preemptible'] ?? null,
          gpu_preemptible_breakdown: nodeData['preemptible_breakdown'] ?? null,
          gpu_allocation_breakdown: nodeData['allocation_breakdown'] ?? null,
          gpu_allocation_workload_breakdown:
            nodeData['allocation_workload_breakdown'] ?? null,
          gpu_preemptible_services:
            nodeData['accelerators_preemptible_services'] ?? null,
          gpu_preemptible_service_breakdown:
            nodeData['preemptible_service_breakdown'] ?? null,
          gpu_preemptible_service_priority_breakdown:
            nodeData['preemptible_service_priority_breakdown'] ?? null,
          gpu_preemptible_service_priority_workload_breakdown:
            nodeData['preemptible_service_priority_workload_breakdown'] ?? null,
        });

        // Aggregate GPU data per context
        if (totalCount > 0) {
          if (!gpuToData[gpuName]) {
            gpuToData[gpuName] = {
              gpu_name: gpuName,
              gpu_requestable_qty_per_node: 0,
              gpu_total: 0,
              gpu_free: 0,
              gpu_not_ready: 0,
              context: context,
              ...emptySchedulingBreakdown(),
            };
          }
          gpuToData[gpuName].gpu_total += totalCount;
          gpuToData[gpuName].gpu_free += freeCount;
          if (isNodeNotReady) {
            gpuToData[gpuName].gpu_not_ready += totalCount;
          }
          addSchedulingFromNode(gpuToData[gpuName], nodeData, isNodeNotReady);
          gpuToData[gpuName].gpu_requestable_qty_per_node = totalCount;
        }
      }
    }

    return {
      context,
      perContextGPUs: Object.values(gpuToData),
      perNodeGPUs: perNodeGPUs,
      error: null,
    };
  } catch (error) {
    const errorMessage =
      error?.message ||
      (typeof error === 'string' && error) ||
      'Context may be unavailable or timed out';
    console.warn(
      `Failed to get GPU data for context ${context}:`,
      errorMessage
    );
    return {
      context,
      perContextGPUs: [],
      perNodeGPUs: [],
      error: errorMessage,
    };
  }
}

// Helper function to get GPU data for specific contexts
export async function getKubernetesGPUsFromContexts(contextNames) {
  try {
    if (!contextNames || contextNames.length === 0) {
      return {
        allGPUs: [],
        perContextGPUs: [],
        perNodeGPUs: [],
        contextErrors: {},
      };
    }

    const allGPUsSummary = {};
    const perContextGPUsData = {};
    const perNodeGPUs_dict = {};
    const contextErrors = {};

    // Get all of the node info for all contexts in parallel and put them
    // in a dictionary keyed by context name.
    // Use Promise.allSettled to handle partial failures gracefully
    const contextNodeInfoResults = await Promise.allSettled(
      contextNames.map((context) => getKubernetesPerNodeGPUs(context))
    );
    const contextToNodeInfo = {};
    for (let i = 0; i < contextNames.length; i++) {
      const result = contextNodeInfoResults[i];
      if (result.status === 'fulfilled') {
        contextToNodeInfo[contextNames[i]] = result.value;
        console.log(
          '[CONTEXT_DEBUG] Context node info result:',
          contextNames[i],
          result.value
        );
      } else {
        // Log the error but continue with other contexts
        const errorMessage =
          result.reason?.message ||
          (typeof result.reason === 'string' && result.reason) ||
          'Context may be unavailable or timed out';
        console.warn(
          `Failed to get node info for context ${contextNames[i]}:`,
          errorMessage
        );
        contextToNodeInfo[contextNames[i]] = {};
        contextErrors[contextNames[i]] = errorMessage;
      }
    }

    // Populate the gpuToData map for each context.
    for (const context of contextNames) {
      const nodeInfoForContext = contextToNodeInfo[context] || {};
      if (nodeInfoForContext && Object.keys(nodeInfoForContext).length > 0) {
        const gpuToData = {};
        for (const nodeName in nodeInfoForContext) {
          const nodeData = nodeInfoForContext[nodeName];
          if (!nodeData) {
            console.warn(
              `No node data for node ${nodeName} in context ${context}`
            );
            continue;
          }

          const gpuName = nodeData['accelerator_type'] || '-';
          const totalCount = nodeData['total']?.['accelerator_count'] || 0;
          const freeCount = nodeData['free']?.['accelerators_available'] || 0;
          const isNodeNotReady = isNodeNotReadyForGpus(nodeData);

          if (totalCount > 0) {
            if (!gpuToData[gpuName]) {
              gpuToData[gpuName] = {
                gpu_name: gpuName,
                gpu_requestable_qty_per_node: 0,
                gpu_total: 0,
                gpu_free: 0,
                gpu_not_ready: 0,
                context: context,
                ...emptySchedulingBreakdown(),
              };
            }
            gpuToData[gpuName].gpu_total += totalCount;
            gpuToData[gpuName].gpu_free += freeCount;
            if (isNodeNotReady) {
              gpuToData[gpuName].gpu_not_ready += totalCount;
            }
            addSchedulingFromNode(gpuToData[gpuName], nodeData, isNodeNotReady);
            gpuToData[gpuName].gpu_requestable_qty_per_node = totalCount;
          }
        }
        perContextGPUsData[context] = Object.values(gpuToData);
        for (const gpuName in gpuToData) {
          if (gpuName in allGPUsSummary) {
            allGPUsSummary[gpuName].gpu_total += gpuToData[gpuName].gpu_total;
            allGPUsSummary[gpuName].gpu_free += gpuToData[gpuName].gpu_free;
            allGPUsSummary[gpuName].gpu_not_ready +=
              gpuToData[gpuName].gpu_not_ready;
            mergeSchedulingBreakdown(
              allGPUsSummary[gpuName],
              gpuToData[gpuName]
            );
          } else {
            const schedulingBreakdown = emptySchedulingBreakdown();
            mergeSchedulingBreakdown(schedulingBreakdown, gpuToData[gpuName]);
            allGPUsSummary[gpuName] = {
              gpu_total: gpuToData[gpuName].gpu_total,
              gpu_free: gpuToData[gpuName].gpu_free,
              gpu_not_ready: gpuToData[gpuName].gpu_not_ready,
              gpu_name: gpuName,
              ...schedulingBreakdown,
            };
          }
        }
      } else {
        // Initialize empty array for contexts that don't have node info
        perContextGPUsData[context] = [];
      }
    }

    // Populate the perNodeGPUs_dict map for each context.
    for (const context of contextNames) {
      const nodeInfoForContext = contextToNodeInfo[context];
      if (nodeInfoForContext && Object.keys(nodeInfoForContext).length > 0) {
        for (const nodeName in nodeInfoForContext) {
          const nodeData = nodeInfoForContext[nodeName];
          if (!nodeData) {
            console.warn(
              `No node data for node ${nodeName} in context ${context}`
            );
            continue;
          }

          // Ensure accelerator_type, total, and free fields exist or provide defaults
          const acceleratorType = nodeData['accelerator_type'] || '-';
          const totalAccelerators =
            nodeData['total']?.['accelerator_count'] ?? 0;
          const freeAccelerators =
            nodeData['free']?.['accelerators_available'] ?? 0;
          // Check if node is ready (defaults to true for backward compatibility)
          const nodeIsReady = nodeData['is_ready'] !== false;
          // Check if node is cordoned (defaults to false for backward compatibility)
          const nodeIsCordoned = nodeData['is_cordoned'] === true;
          // Get taints (defaults to empty for backward compatibility)
          const nodeTaints = nodeData['taints'] || [];

          // Extract CPU and memory information
          const cpuCount = nodeData['cpu_count'] ?? null;
          const memoryGb = nodeData['memory_gb'] ?? null;
          const cpuFree = nodeData['cpu_free'] ?? null;
          const memoryFreeGb = nodeData['memory_free_gb'] ?? null;

          perNodeGPUs_dict[`${context}/${nodeName}`] = {
            node_name: nodeData['name'] || nodeName,
            gpu_name: acceleratorType,
            gpu_total: totalAccelerators,
            gpu_free: freeAccelerators,
            ip_address: nodeData['ip_address'] || null,
            context: context,
            cpu_count: cpuCount,
            memory_gb: memoryGb,
            cpu_free: cpuFree,
            memory_free_gb: memoryFreeGb,
            is_ready: nodeIsReady,
            is_cordoned: nodeIsCordoned,
            taints: nodeTaints,
            gpu_preemptible: nodeData['accelerators_preemptible'] ?? null,
            gpu_preemptible_breakdown:
              nodeData['preemptible_breakdown'] ?? null,
            gpu_allocation_breakdown: nodeData['allocation_breakdown'] ?? null,
            gpu_allocation_workload_breakdown:
              nodeData['allocation_workload_breakdown'] ?? null,
            gpu_preemptible_services:
              nodeData['accelerators_preemptible_services'] ?? null,
            gpu_preemptible_service_breakdown:
              nodeData['preemptible_service_breakdown'] ?? null,
            gpu_preemptible_service_priority_breakdown:
              nodeData['preemptible_service_priority_breakdown'] ?? null,
            gpu_preemptible_service_priority_workload_breakdown:
              nodeData['preemptible_service_priority_workload_breakdown'] ??
              null,
          };

          // If this node provides a GPU type not found via GPU availability,
          // add it to perContextGPUsData with 0/0 counts if it's not already there.
          if (
            acceleratorType !== '-' &&
            perContextGPUsData[context] &&
            !perContextGPUsData[context].some(
              (gpu) => gpu.gpu_name === acceleratorType
            )
          ) {
            if (!(acceleratorType in allGPUsSummary)) {
              allGPUsSummary[acceleratorType] = {
                gpu_total: 0,
                gpu_free: 0,
                gpu_not_ready: 0,
                gpu_name: acceleratorType,
                ...emptySchedulingBreakdown(),
              };
            }
            const existingGpuEntry = perContextGPUsData[context].find(
              (gpu) => gpu.gpu_name === acceleratorType
            );
            if (!existingGpuEntry) {
              perContextGPUsData[context].push({
                gpu_name: acceleratorType,
                gpu_not_ready: 0,
                gpu_requestable_qty_per_node: '-',
                gpu_total: 0,
                gpu_free: 0,
                context: context,
                ...emptySchedulingBreakdown(),
              });
            }
          }
        }
      }
    }

    console.log('[CONTEXT_DEBUG] All GPUs summary:', allGPUsSummary);
    console.log('[CONTEXT_DEBUG] Per context GPUs data:', perContextGPUsData);
    console.log('[CONTEXT_DEBUG] Per node GPUs data:', perNodeGPUs_dict);
    console.log('[CONTEXT_DEBUG] Context errors:', contextErrors);
    return {
      allGPUs: Object.values(allGPUsSummary).sort((a, b) =>
        (a.gpu_name || '').localeCompare(b.gpu_name || '')
      ),
      perContextGPUs: Object.values(perContextGPUsData)
        .flat()
        .sort(
          (a, b) =>
            (a.context || '').localeCompare(b.context || '') ||
            (a.gpu_name || '').localeCompare(b.gpu_name || '')
        ),
      perNodeGPUs: Object.values(perNodeGPUs_dict).sort(
        (a, b) =>
          (a.context || '').localeCompare(b.context || '') ||
          (a.node_name || '').localeCompare(b.node_name || '') ||
          (a.gpu_name || '').localeCompare(b.gpu_name || '')
      ),
      contextErrors: contextErrors,
    };
  } catch (error) {
    console.error('[infra.jsx] Error in getKubernetesGPUsFromContexts:', error);
    throw error;
  }
}

async function getKubernetesPerNodeGPUs(context) {
  try {
    const response = await apiClient.post(`/kubernetes_node_info`, {
      context: context,
    });
    if (!response.ok) {
      const msg = `Failed to get kubernetes node info for context ${context} with status ${response.status}, error: ${response.statusText}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for kubernetes node info';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    if (!fetchedData.ok) {
      const errorMessage = await getErrorMessageFromResponse(fetchedData);
      const msg = `Failed to get kubernetes node info result for context ${context} with status ${fetchedData.status}, error: ${errorMessage}`;
      throw new Error(msg);
    }
    const data = await fetchedData.json();
    const nodeInfo = data.return_value ? JSON.parse(data.return_value) : {};
    const nodeInfoDict = nodeInfo['node_info_dict'] || {};
    return nodeInfoDict;
  } catch (error) {
    console.warn(
      `[infra.jsx] Context ${context} unavailable or timed out:`,
      error.message
    );
    throw error;
  }
}
