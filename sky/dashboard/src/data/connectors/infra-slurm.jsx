import { apiClient } from '@/data/connectors/client';

async function getSlurmClusterGPUs() {
  try {
    const response = await apiClient.post(`/slurm_gpu_availability`, {});
    if (!response.ok) {
      const msg = `Failed to get slurm cluster GPUs with status ${response.status}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for slurm cluster GPUs';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    if (fetchedData.status === 500) {
      try {
        const data = await fetchedData.json();
        if (data.detail && data.detail.error) {
          try {
            const error = JSON.parse(data.detail.error);
            console.error('Error fetching Slurm cluster GPUs:', error.message);
          } catch (jsonError) {
            console.error('Error parsing JSON for Slurm error:', jsonError);
          }
        }
      } catch (parseError) {
        console.error('Error parsing JSON for Slurm 500 response:', parseError);
      }
      return [];
    }
    if (!fetchedData.ok) {
      const msg = `Failed to get slurm cluster GPUs result with status ${fetchedData.status}`;
      throw new Error(msg);
    }
    const data = await fetchedData.json();
    const clusterGPUs = data.return_value ? JSON.parse(data.return_value) : [];
    return clusterGPUs;
  } catch (error) {
    console.error('Error fetching Slurm cluster GPUs:', error);
    return [];
  }
}

async function getSlurmPerNodeGPUs() {
  try {
    const response = await apiClient.post(`/slurm_node_info`, {});
    if (!response.ok) {
      const msg = `Failed to get slurm node info with status ${response.status}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for slurm node info';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    if (fetchedData.status === 500) {
      try {
        const data = await fetchedData.json();
        if (data.detail && data.detail.error) {
          try {
            const error = JSON.parse(data.detail.error);
            console.error('Error fetching Slurm per node GPUs:', error.message);
          } catch (jsonError) {
            console.error(
              'Error parsing JSON for Slurm node error:',
              jsonError
            );
          }
        }
      } catch (parseError) {
        console.error(
          'Error parsing JSON for Slurm node 500 response:',
          parseError
        );
      }
      return [];
    }
    if (!fetchedData.ok) {
      const msg = `Failed to get slurm node info result with status ${fetchedData.status}`;
      throw new Error(msg);
    }
    const data = await fetchedData.json();
    const nodeInfo = data.return_value ? JSON.parse(data.return_value) : [];
    return nodeInfo;
  } catch (error) {
    console.error('Error fetching Slurm per node GPUs:', error);
    return [];
  }
}

// Export Slurm infrastructure fetching for parallel loading
export async function getSlurmInfrastructure() {
  return await getSlurmServiceGPUs();
}

async function getSlurmServiceGPUs() {
  try {
    // Fetch cluster GPUs and node GPUs in parallel for better performance
    const [clusterGPUsRaw, nodeGPUsRaw] = await Promise.all([
      getSlurmClusterGPUs(),
      getSlurmPerNodeGPUs(),
    ]);

    const allSlurmGPUs = {};
    const perClusterSlurmGPUs = {}; // Similar to perContextGPUs for Kubernetes
    const perNodeSlurmGPUs = {}; // { 'cluster/node_name': { ... } }

    // Process cluster GPUs (similar to Kubernetes context GPUs)
    // clusterGPUsRaw is expected to be like: [ [cluster_name, [ [gpu_name, counts, capacity, available], ... ] ], ... ]
    for (const clusterData of clusterGPUsRaw) {
      const clusterName = clusterData[0];
      const gpusInCluster = clusterData[1];

      for (const gpuRaw of gpusInCluster) {
        const gpuName = gpuRaw[0];
        // gpuRaw[1] is counts (list of requestable quantities), e.g., [1, 2, 4]
        const gpuRequestableQtyPerNode = gpuRaw[1].join(', ');
        const gpuTotal = gpuRaw[2]; // capacity
        const gpuFree = gpuRaw[3]; // available

        // Aggregate for allSlurmGPUs
        if (gpuName in allSlurmGPUs) {
          allSlurmGPUs[gpuName].gpu_total += gpuTotal;
          allSlurmGPUs[gpuName].gpu_free += gpuFree;
        } else {
          allSlurmGPUs[gpuName] = {
            gpu_total: gpuTotal,
            gpu_free: gpuFree,
            gpu_name: gpuName,
          };
        }

        // Store for perClusterSlurmGPUs (similar to perContextGPUs)
        const clusterGpuKey = `${clusterName}#${gpuName}`; // Unique key for cluster-gpu combo
        perClusterSlurmGPUs[clusterGpuKey] = {
          gpu_name: gpuName,
          gpu_requestable_qty_per_node: gpuRequestableQtyPerNode,
          gpu_total: gpuTotal,
          gpu_free: gpuFree,
          cluster: clusterName,
        };
      }
    }

    // Process node GPUs
    // nodeGPUsRaw is expected to be like: [ {node_name, slurm_cluster_name, partition, gpu_type, total_gpus, free_gpus}, ... ]
    for (const node of nodeGPUsRaw) {
      const clusterName = node.slurm_cluster_name || 'default';
      const key = `${clusterName}/${node.node_name}/${node.gpu_type || '-'}`;
      perNodeSlurmGPUs[key] = {
        node_name: node.node_name,
        gpu_name: node.gpu_type || '-', // gpu_type might be null
        gpu_total: node.total_gpus || 0,
        gpu_free: node.free_gpus || 0,
        cluster: clusterName,
        partition: node.partition || 'default', // partition might be null
      };
    }

    return {
      allSlurmGPUs: Object.values(allSlurmGPUs).sort((a, b) =>
        a.gpu_name.localeCompare(b.gpu_name)
      ),
      perClusterSlurmGPUs: Object.values(perClusterSlurmGPUs).sort(
        (a, b) =>
          a.cluster.localeCompare(b.cluster) ||
          a.gpu_name.localeCompare(b.gpu_name)
      ),
      perNodeSlurmGPUs: Object.values(perNodeSlurmGPUs).sort(
        (a, b) =>
          (a.cluster || '').localeCompare(b.cluster || '') ||
          (a.node_name || '').localeCompare(b.node_name || '') ||
          (a.gpu_name || '').localeCompare(b.gpu_name || '')
      ),
    };
  } catch (error) {
    console.error('Error fetching Slurm GPUs:', error);
    return {
      allSlurmGPUs: [],
      perClusterSlurmGPUs: [],
      perNodeSlurmGPUs: [],
    };
  }
}
