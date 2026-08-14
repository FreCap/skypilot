import { CLOUDS_LIST, COMMON_GPUS } from '@/data/connectors/constants';

// Importing from the same directory
import { apiClient } from '@/data/connectors/client';
import { getKubernetesGPUsFromContexts } from '@/data/connectors/infra-kubernetes';
import { getErrorMessageFromResponse } from '@/data/utils';
import dashboardCache from '@/lib/cache';
import { buildContextStatsKeyFromCloud } from '@/utils/infraUtils';

export { getSlurmInfrastructure } from '@/data/connectors/infra-slurm';
export { getContextGPUData } from '@/data/connectors/infra-kubernetes';

const INFRA_SUMMARY_VERSION = 1;

/**
 * Fetch the workspace/infra identity needed for first paint directly from the
 * API process. Older servers return 404 and keep using the scheduled reads.
 */
export async function getInfraSummary() {
  try {
    const response = await apiClient.get('/infra_summary');
    if (response.status === 404) {
      return { available: false, reason: 'unsupported' };
    }
    if (!response.ok) {
      throw new Error(`Infra summary request failed with ${response.status}`);
    }

    const payload = await response.json();
    if (
      payload?.version !== INFRA_SUMMARY_VERSION ||
      !Array.isArray(payload.workspaces)
    ) {
      throw new Error('Infra summary response was malformed');
    }

    const workspaces = {};
    for (const workspace of payload.workspaces) {
      if (
        !workspace ||
        typeof workspace.name !== 'string' ||
        !Array.isArray(workspace.infrastructure)
      ) {
        throw new Error('Infra summary workspace was malformed');
      }
      workspaces[workspace.name] = workspace.infrastructure.filter(
        (item) => typeof item === 'string' && item.length > 0
      );
    }
    return { available: true, workspaces };
  } catch (error) {
    console.warn(
      'Direct infra summary unavailable; using legacy reads:',
      error
    );
    return {
      available: false,
      reason: 'unavailable',
      // A transient direct-read failure should be retried on the next refresh.
      __skipCache: true,
    };
  }
}

function enabledCloudRows(workspaceInfrastructure) {
  const enabledCloudsSet = new Set();
  Object.values(workspaceInfrastructure || {}).forEach((infrastructure) => {
    (infrastructure || []).forEach((item) => {
      enabledCloudsSet.add(item.toLowerCase().split('/')[0]);
    });
  });

  const clouds = CLOUDS_LIST.filter((cloud) =>
    enabledCloudsSet.has(cloud.toLowerCase())
  )
    .map((name) => ({ name, enabled: true }))
    .sort((a, b) => a.name.localeCompare(b.name));
  return {
    clouds,
    totalClouds: CLOUDS_LIST.length,
    enabledClouds: clouds.length,
  };
}

export function buildWorkspaceContexts(
  workspaceInfrastructure,
  workspaceConfigs = {}
) {
  const workspaces = {};
  const allContextNames = new Set();
  const contextWorkspaceMap = {};

  Object.entries(workspaceInfrastructure || {}).forEach(
    ([workspaceName, infrastructure]) => {
      workspaces[workspaceName] = {
        config: workspaceConfigs[workspaceName] || {},
        clouds: infrastructure,
        contexts: [],
      };

      (infrastructure || []).forEach((infraItem) => {
        const normalized = infraItem.toLowerCase();
        let context = null;
        if (normalized.startsWith('kubernetes/')) {
          context = infraItem.replace(/^kubernetes\//i, '');
        } else if (normalized.startsWith('ssh/')) {
          context = `ssh-${infraItem.replace(/^ssh\//i, '')}`;
        }
        if (!context) return;

        allContextNames.add(context);
        if (!workspaces[workspaceName].contexts.includes(context)) {
          workspaces[workspaceName].contexts.push(context);
        }
        if (!contextWorkspaceMap[context]) {
          contextWorkspaceMap[context] = [];
        }
        if (!contextWorkspaceMap[context].includes(workspaceName)) {
          contextWorkspaceMap[context].push(workspaceName);
        }
      });
    }
  );

  return {
    workspaces,
    allContextNames: [...allContextNames].sort(),
    contextWorkspaceMap,
  };
}

/**
 * Fast function to get just the list of enabled clouds (without counts).
 * Used for progressive loading - display cloud rows immediately, then overlay counts.
 */
export async function getEnabledCloudsList() {
  const { getWorkspaces, getEnabledCloudsBatch } = await import(
    '@/data/connectors/workspaces'
  );

  try {
    const summary = await dashboardCache.get(getInfraSummary);
    if (summary.available) {
      return enabledCloudRows(summary.workspaces);
    }

    // Get workspaces (fast - cached)
    const workspacesData = await dashboardCache
      .get(getWorkspaces)
      .catch(() => ({}));
    const workspaceNames = Object.keys(workspacesData || {});

    if (workspaceNames.length === 0) {
      return { clouds: [], totalClouds: CLOUDS_LIST.length, enabledClouds: 0 };
    }

    // Fetch enabled clouds for all workspaces in a single batch call
    const batchResult = await dashboardCache.get(getEnabledCloudsBatch, [
      workspaceNames,
      false,
    ]);

    const enabledCloudsSet = new Set();
    Object.values(batchResult || {}).forEach((workspaceClouds) => {
      if (Array.isArray(workspaceClouds)) {
        workspaceClouds.forEach((cloud) => {
          if (cloud) {
            enabledCloudsSet.add(cloud.toLowerCase());
          }
        });
      }
    });

    // Build cloud objects with just name and enabled status (no counts)
    const enabledCloudsList = Array.from(enabledCloudsSet);
    const clouds = CLOUDS_LIST.filter((cloud) =>
      enabledCloudsList.includes(cloud.toLowerCase())
    )
      .map((name) => ({ name, enabled: true }))
      .sort((a, b) => a.name.localeCompare(b.name));

    return {
      clouds,
      totalClouds: CLOUDS_LIST.length,
      enabledClouds: clouds.length,
    };
  } catch (error) {
    console.error('Error fetching enabled clouds list:', error);
    return { clouds: [], totalClouds: CLOUDS_LIST.length, enabledClouds: 0 };
  }
}

export async function getCloudInfrastructure(forceRefresh = false) {
  const { getClusters } = await import('@/data/connectors/clusters');
  const { getManagedJobs } = await import('@/data/connectors/jobs');
  const { getWorkspaces, getEnabledCloudsBatch } = await import(
    '@/data/connectors/workspaces'
  );

  try {
    // Fetch jobs, clusters, and workspaces in parallel for better performance
    const [jobsResult, clustersResult, workspacesData] = await Promise.all([
      // Use shared cache key (no field filtering) - preloader uses same args
      dashboardCache
        .get(getManagedJobs, [{ allUsers: true, skipFinished: true }])
        .catch((error) => {
          console.error('Error fetching managed jobs:', error);
          return { jobs: [] };
        }),
      dashboardCache.get(getClusters).catch((error) => {
        console.error('Error fetching clusters:', error);
        return [];
      }),
      dashboardCache.get(getWorkspaces).catch((error) => {
        console.error('Error fetching workspaces:', error);
        return {};
      }),
    ]);

    const jobs = jobsResult?.jobs || [];
    const clusters = clustersResult || [];

    // Get enabled clouds by aggregating across all workspaces
    let enabledCloudsList = [];
    const workspaceNames = Object.keys(workspacesData || {});

    if (workspaceNames.length === 0) {
      console.warn('No accessible workspaces found');
      enabledCloudsList = [];
    } else {
      // Fetch enabled clouds for all workspaces in a single batch call
      const batchResult = await dashboardCache
        .get(getEnabledCloudsBatch, [workspaceNames, false])
        .catch(() => ({}));

      const enabledCloudsSet = new Set();
      Object.values(batchResult || {}).forEach((workspaceClouds) => {
        if (Array.isArray(workspaceClouds)) {
          workspaceClouds.forEach((cloud) => {
            if (cloud) {
              enabledCloudsSet.add(cloud.toLowerCase());
            }
          });
        }
      });

      enabledCloudsList = Array.from(enabledCloudsSet);
      console.log(
        'Aggregated enabled clouds across all workspaces:',
        enabledCloudsList
      );
    }

    // Create a map to store cloud data
    const cloudsData = {};

    // Initialize with all clouds from CLOUDS_LIST
    CLOUDS_LIST.forEach((cloud) => {
      // Check if the cloud is in the enabled clouds list
      const isEnabled = enabledCloudsList.includes(cloud.toLowerCase());

      cloudsData[cloud] = {
        name: cloud,
        clusters: 0,
        jobs: 0,
        enabled: isEnabled,
      };
    });

    // Count clusters per cloud
    clusters.forEach((cluster) => {
      if (cluster.cloud) {
        const cloudName = cluster.cloud;
        if (cloudsData[cloudName]) {
          cloudsData[cloudName].clusters += 1;
          // If we have clusters in a cloud, it must be enabled
          cloudsData[cloudName].enabled = true;
        }
      }
    });

    // Count jobs per cloud
    jobs.forEach((job) => {
      if (job.cloud) {
        const cloudName = job.cloud;
        if (cloudsData[cloudName]) {
          cloudsData[cloudName].jobs += 1;
          // If we have jobs in a cloud, it must be enabled
          cloudsData[cloudName].enabled = true;
        }
      }
    });

    // Get total and enabled counts for the UI
    const totalClouds = CLOUDS_LIST.length;
    const enabledClouds = Object.values(cloudsData).filter(
      (c) => c.enabled
    ).length;

    // Convert to array, filter to only enabled clouds, and sort by name
    const result = Object.values(cloudsData)
      .filter((cloud) => cloud.enabled)
      .sort((a, b) => a.name.localeCompare(b.name));

    return {
      clouds: result,
      totalClouds,
      enabledClouds,
    };
  } catch (error) {
    console.error('Error fetching cloud infrastructure:', error);
    throw error;
  }
}

export async function getGPUs() {
  // Legacy function - now redirects to workspace-aware infrastructure
  return await getWorkspaceInfrastructure();
}

// New workspace-aware infrastructure fetching function
export async function getWorkspaceInfrastructure() {
  try {
    console.log('[DEBUG] Starting workspace-aware infrastructure fetch');

    // Step 1: Get all accessible workspaces for the user (use cache for performance)
    const { getWorkspaces } = await import('@/data/connectors/workspaces');
    console.log('[DEBUG] About to call getWorkspaces() via cache');
    const workspacesData = await dashboardCache.get(getWorkspaces);
    console.log('[DEBUG] Workspaces data received:', workspacesData);
    console.log(
      '[DEBUG] Number of accessible workspaces:',
      Object.keys(workspacesData || {}).length
    );
    console.log('[DEBUG] Workspace names:', Object.keys(workspacesData || {}));

    if (!workspacesData || Object.keys(workspacesData).length === 0) {
      console.log(
        '[DEBUG] No accessible workspaces found - returning empty result'
      );
      return {
        workspaces: {},
        allContextNames: [],
        allGPUs: [],
        perContextGPUs: [],
        perNodeGPUs: [],
        allSlurmGPUs: [],
        perClusterSlurmGPUs: [],
        perNodeSlurmGPUs: [],
        contextStats: {},
        contextWorkspaceMap: {},
        contextErrors: {},
      };
    }

    // Step 2: Fetch expanded clouds for all workspaces in a single batch call
    const { getEnabledCloudsBatch } = await import(
      '@/data/connectors/workspaces'
    );
    const workspaceNames = Object.keys(workspacesData);
    const batchResult = await dashboardCache
      .get(getEnabledCloudsBatch, [workspaceNames, true])
      .catch(() => ({}));

    const workspaceInfraData = {};
    const allContextsAcrossWorkspaces = [];
    const contextWorkspaceMap = {};

    Object.entries(workspacesData).forEach(
      ([workspaceName, workspaceConfig]) => {
        const expandedClouds = batchResult[workspaceName] || [];
        workspaceInfraData[workspaceName] = {
          config: workspaceConfig,
          clouds: expandedClouds,
          contexts: [],
        };

        expandedClouds.forEach((infraItem) => {
          if (infraItem.toLowerCase().startsWith('kubernetes/')) {
            const context = infraItem.replace(/^kubernetes\//i, '');
            allContextsAcrossWorkspaces.push(context);
            if (!contextWorkspaceMap[context]) {
              contextWorkspaceMap[context] = [];
            }
            if (!contextWorkspaceMap[context].includes(workspaceName)) {
              contextWorkspaceMap[context].push(workspaceName);
            }
            workspaceInfraData[workspaceName].contexts.push(context);
          } else if (infraItem.toLowerCase().startsWith('ssh/')) {
            const poolName = infraItem.replace(/^ssh\//i, '');
            const sshContextName = `ssh-${poolName}`;
            allContextsAcrossWorkspaces.push(sshContextName);
            if (!contextWorkspaceMap[sshContextName]) {
              contextWorkspaceMap[sshContextName] = [];
            }
            if (!contextWorkspaceMap[sshContextName].includes(workspaceName)) {
              contextWorkspaceMap[sshContextName].push(workspaceName);
            }
            workspaceInfraData[workspaceName].contexts.push(sshContextName);
          }
        });
      }
    );

    // Step 3: Get detailed GPU information for all contexts
    const { getClusters } = await import('@/data/connectors/clusters');
    let clustersData = [];
    try {
      clustersData = await dashboardCache.get(getClusters);
    } catch (error) {
      console.error('Error fetching clusters:', error);
    }
    const clusters = clustersData || [];

    // Get context stats (cluster counts)
    let contextStats = {};
    try {
      contextStats = await getContextClusters(clusters);
    } catch (error) {
      console.error('Error fetching context clusters:', error);
    }

    // Get GPU data for all contexts (filter out any undefined contexts)
    const validContexts = [...new Set(allContextsAcrossWorkspaces)].filter(
      (context) => context && typeof context === 'string'
    );
    let gpuData = {
      allGPUs: [],
      perContextGPUs: [],
      perNodeGPUs: [],
      contextErrors: {},
    };
    try {
      gpuData = await getKubernetesGPUsFromContexts(validContexts);
    } catch (error) {
      console.error('Error fetching Kubernetes GPUs:', error);
    }

    // Note: Slurm GPU data is now fetched separately via getSlurmInfrastructure()
    // This allows Slurm to load in parallel with Kubernetes/SSH data

    const finalResult = {
      workspaces: workspaceInfraData,
      allContextNames: [...new Set(allContextsAcrossWorkspaces)].sort(),
      allGPUs: gpuData.allGPUs || [],
      perContextGPUs: gpuData.perContextGPUs || [],
      perNodeGPUs: gpuData.perNodeGPUs || [],
      contextStats: contextStats,
      contextWorkspaceMap: contextWorkspaceMap,
      contextErrors: gpuData.contextErrors || {},
    };

    console.log('[DEBUG] Final result:', finalResult);
    console.log('[DEBUG] All contexts found:', allContextsAcrossWorkspaces);
    console.log('[DEBUG] Context workspace map:', contextWorkspaceMap);

    return finalResult;
  } catch (error) {
    console.error('[DEBUG] Failed to fetch workspace infrastructure:', error);
    console.error('[DEBUG] Error stack:', error.stack);
    throw error;
  }
}

// Lightweight function to get just context names quickly (without GPU data)
// This allows the UI to show contexts immediately while GPU data loads progressively
export async function getWorkspaceContexts() {
  try {
    const summary = await dashboardCache.get(getInfraSummary);
    if (summary.available) {
      return buildWorkspaceContexts(summary.workspaces);
    }

    // Step 1: Get all accessible workspaces for the user (use cache for performance)
    const { getWorkspaces } = await import('@/data/connectors/workspaces');
    const workspacesData = await dashboardCache.get(getWorkspaces);

    if (!workspacesData || Object.keys(workspacesData).length === 0) {
      return {
        workspaces: {},
        allContextNames: [],
        contextWorkspaceMap: {},
      };
    }

    // Step 2: Fetch expanded clouds for all workspaces in a single batch call
    const { getEnabledCloudsBatch } = await import(
      '@/data/connectors/workspaces'
    );
    const workspaceNames = Object.keys(workspacesData);
    const batchResult = await dashboardCache
      .get(getEnabledCloudsBatch, [workspaceNames, true])
      .catch(() => ({}));

    const workspaceInfrastructure = Object.fromEntries(
      Object.keys(workspacesData).map((workspaceName) => [
        workspaceName,
        batchResult[workspaceName] || [],
      ])
    );
    return buildWorkspaceContexts(workspaceInfrastructure, workspacesData);
  } catch (error) {
    console.error('Failed to fetch workspace contexts:', error);
    throw error;
  }
}

export async function getContextJobs(jobs) {
  try {
    // Count jobs per k8s context/ssh node pool/slurm cluster
    const contextStats = {};

    // Process jobs
    jobs.forEach((job) => {
      const contextKey = buildContextStatsKeyFromCloud(job.cloud, job.region);

      if (contextKey) {
        if (!contextStats[contextKey]) {
          contextStats[contextKey] = { clusters: 0, jobs: 0 };
        }
        contextStats[contextKey].jobs += 1;
      }
    });

    return contextStats;
  } catch (error) {
    console.error('=== Error in getContextJobs ===', error);
    throw error;
  }
}

export async function getContextClusters(clusters) {
  try {
    // Count clusters per k8s context/ssh node pool/slurm cluster
    const contextStats = {};
    clusters.forEach((cluster) => {
      const contextKey = buildContextStatsKeyFromCloud(
        cluster.cloud,
        cluster.region
      );

      if (contextKey) {
        if (!contextStats[contextKey]) {
          contextStats[contextKey] = { clusters: 0, jobs: 0 };
        }
        contextStats[contextKey].clusters += 1;
      }
    });

    return contextStats;
  } catch (error) {
    console.error('=== Error in getContextClusters ===', error);
    throw error;
  }
}

export async function getCloudGPUs() {
  try {
    const response = await apiClient.post(`/list_accelerator_counts`, {
      clouds: CLOUDS_LIST,
      gpus_only: true,
    });
    if (!response.ok) {
      const msg = `Failed to get cloud GPUs with status ${response.status}, error: ${response.statusText}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for cloud GPUs';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    if (!fetchedData.ok) {
      const errorMessage = await getErrorMessageFromResponse(fetchedData);
      const msg = `Failed to get cloud GPUs result with status ${fetchedData.status}, error: ${errorMessage}`;
      throw new Error(msg);
    }
    const data = await fetchedData.json();
    const allGPUs = data.return_value ? JSON.parse(data.return_value) : {};
    const commonGPUs = Object.keys(allGPUs)
      .filter((gpu) => COMMON_GPUS.includes(gpu))
      .map((gpu) => ({
        gpu_name: gpu,
        gpu_quantities: allGPUs[gpu].join(', '),
      }))
      .sort((a, b) => a.gpu_name.localeCompare(b.gpu_name));
    const tpus = Object.keys(allGPUs)
      .filter((gpu) => gpu.startsWith('tpu-'))
      .map((gpu) => ({
        gpu_name: gpu,
        gpu_quantities: allGPUs[gpu].join(', '),
      }))
      .sort((a, b) => a.gpu_name.localeCompare(b.gpu_name));
    const otherGPUs = Object.keys(allGPUs)
      .filter((gpu) => !COMMON_GPUS.includes(gpu) && !gpu.startsWith('tpu-'))
      .map((gpu) => ({
        gpu_name: gpu,
        gpu_quantities: allGPUs[gpu].join(', '),
      }))
      .sort((a, b) => a.gpu_name.localeCompare(b.gpu_name));
    return {
      commonGPUs,
      tpus,
      otherGPUs,
    };
  } catch (error) {
    console.error('Error fetching cloud GPUs:', error);
    throw error;
  }
}

export async function getDetailedGpuInfo(filter) {
  try {
    let gpuName = filter;
    let gpuCount = null;

    if (filter.includes(':')) {
      const [name, countStr] = filter.split(':');
      gpuName = name.trim();
      const parsedCount = parseInt(countStr.trim());
      if (!isNaN(parsedCount) && parsedCount > 0) {
        gpuCount = parsedCount;
      }
    }

    console.log(
      `Searching for GPU: ${gpuName}${gpuCount !== null ? ', effective count: ${gpuCount}' : ''}`
    );

    const response = await apiClient.post(`/list_accelerators`, {
      gpus_only: true,
      name_filter: gpuName,
      quantity_filter: gpuCount,
      clouds: CLOUDS_LIST,
      case_sensitive: false,
      all_regions: true,
    });
    if (!response.ok) {
      const msg = `Failed to get detailed GPU info with status ${response.status}, error: ${response.statusText}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for detailed GPU info';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    if (!fetchedData.ok) {
      const errorMessage = await getErrorMessageFromResponse(fetchedData);
      const msg = `Failed to get detailed GPU info result with status ${fetchedData.status}, error: ${errorMessage}`;
      throw new Error(msg);
    }

    const data = await fetchedData.json();

    if (!data.return_value) {
      console.log('No return_value in API response for detailed GPU info.');
      return [];
    }

    let rawData;
    try {
      const jsonStr = data.return_value;
      const processedStr = jsonStr
        .replace(/NaN/g, 'null')
        .replace(/Infinity/g, 'null')
        .replace(/-Infinity/g, 'null')
        .replace(/undefined/g, 'null');

      rawData = JSON.parse(processedStr);
      console.log(
        'Successfully parsed GPU data. Top-level keys:',
        Object.keys(rawData)
      );
    } catch (parseError) {
      console.error('Error parsing GPU data:', parseError);
      throw parseError;
    }

    const formattedData = [];
    const expectedArrayLength = 10;

    for (const [gpuNameKey, instances] of Object.entries(rawData)) {
      if (!Array.isArray(instances)) {
        console.log(`Value for key ${gpuNameKey} is not an array:`, instances);
        continue;
      }
      console.log(`Processing ${instances.length} instances for ${gpuNameKey}`);
      if (instances.length > 0 && Array.isArray(instances[0])) {
        console.log(
          'First instance array being processed:',
          JSON.stringify(instances[0], null, 2)
        );
      } else if (instances.length > 0) {
        console.log(
          'First instance (not an array as expected):',
          JSON.stringify(instances[0], null, 2)
        );
      }

      instances.forEach((instanceArray) => {
        if (
          !Array.isArray(instanceArray) ||
          instanceArray.length < expectedArrayLength
        ) {
          if (!Array.isArray(instanceArray)) {
            console.warn(
              `Expected an array for instance under ${gpuNameKey}, but got:`,
              instanceArray
            );
            return;
          } else {
            console.warn(
              `Instance array for ${gpuNameKey} has unexpected length ${instanceArray.length} (expected ${expectedArrayLength}):`,
              instanceArray
            );
          }
        }

        const cloud = instanceArray[0];
        const instance_type = instanceArray[1];
        const acc_count = instanceArray[3];
        const cpu_val = instanceArray[4];
        const dev_mem_val = instanceArray[5];
        const mem_val = instanceArray[6];
        const price_val = instanceArray[7];
        const spot_val = instanceArray[8];
        const region_val = instanceArray[9];

        let display_count = acc_count;
        if (
          gpuCount !== null &&
          (display_count === null ||
            display_count === undefined ||
            display_count === 0)
        ) {
          display_count = gpuCount;
        }
        display_count =
          display_count === null ||
          display_count === undefined ||
          isNaN(parseInt(display_count))
            ? 0
            : parseInt(display_count);

        const instanceType = instance_type || '(attachable)';
        const deviceMemory =
          dev_mem_val !== null && !isNaN(dev_mem_val)
            ? `${Math.floor(dev_mem_val)}GB`
            : '-';
        const cpuCount =
          cpu_val !== null && !isNaN(cpu_val)
            ? Number.isInteger(cpu_val)
              ? cpu_val
              : parseFloat(cpu_val).toFixed(1)
            : '-';
        const memory =
          mem_val !== null && !isNaN(mem_val)
            ? `${Math.floor(mem_val)}GB`
            : '-';
        const price =
          price_val !== null && !isNaN(price_val)
            ? `$${parseFloat(price_val).toFixed(3)}`
            : '-';
        const spotPrice =
          spot_val !== null && !isNaN(spot_val)
            ? `$${parseFloat(spot_val).toFixed(3)}`
            : '-';
        const region = region_val || '-';

        formattedData.push({
          accelerator_name: gpuNameKey,
          accelerator_count: display_count,
          cloud: cloud || '',
          instance_type: instanceType,
          device_memory: deviceMemory,
          cpu_count: cpuCount,
          memory: memory,
          price: price,
          spot_price: spotPrice,
          region: region,
          raw_price:
            price_val !== null && !isNaN(price_val)
              ? parseFloat(price_val)
              : Infinity,
          raw_spot_price:
            spot_val !== null && !isNaN(spot_val)
              ? parseFloat(spot_val)
              : Infinity,
        });
      });
    }

    return formattedData.sort((a, b) => {
      if (a.raw_price !== b.raw_price) return a.raw_price - b.raw_price;
      return a.raw_spot_price - b.raw_spot_price;
    });
  } catch (error) {
    console.error('Outer error in getDetailedGpuInfo:', error);
    throw error;
  }
}
