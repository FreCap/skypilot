import { apiClient } from './client';
import { CLUSTER_NOT_UP_ERROR } from '@/data/connectors/constants';

// Normalize a raw replica_info entry from the /serve/status response.
// The REST encoder (`encode_serve_status`) serializes replica statuses to
// their plain string values (sky/serve/serve_state.py ReplicaStatus) and
// leaves the pickled `handle` as an opaque blob, which we ignore.
export function normalizeReplica(replica) {
  return {
    id: replica.replica_id,
    status: replica.status,
    version: replica.version,
    endpoint: replica.endpoint || null,
    is_spot: replica.is_spot,
    launched_at: replica.launched_at || null,
    cloud: replica.cloud || null,
    region: replica.region || null,
    infra: replica.infra || null,
    resources_str: replica.resources_str || null,
    resources_str_full:
      replica.resources_str_full || replica.resources_str || null,
  };
}

// Normalize a raw service record from the /serve/status response into the
// shape consumed by the services pages. Statuses arrive as plain strings
// (sky/serve/serve_state.py ServiceStatus values).
export function normalizeService(record) {
  const replicaInfo = Array.isArray(record.replica_info)
    ? record.replica_info
    : [];
  const replicas = replicaInfo.map(normalizeReplica);
  const replicasReady = replicas.filter((r) => r.status === 'READY').length;

  return {
    name: record.name,
    status: record.status,
    // Epoch timestamp of when the service first became ready (see
    // serve_state.set_service_uptime); the UI renders `now - uptime`.
    uptime: record.uptime ?? null,
    endpoint: record.endpoint || null,
    replicasReady,
    replicasTotal: replicas.length,
    targetReplicas: record.target_num_replicas ?? null,
    policy: record.policy || null,
    loadBalancingPolicy: record.load_balancing_policy || null,
    requestedResources: record.requested_resources_str || null,
    activeVersions: record.active_versions || [],
    version: record.version ?? null,
    tlsEncrypted: Boolean(record.tls_encrypted),
    replicas,
  };
}

export async function getServices() {
  try {
    const response = await apiClient.post(`/serve/status`, {
      service_names: null, // null means get all services
    });
    if (!response.ok) {
      const msg = `Initial API request to get services failed with status ${response.status}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for getting services';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    let errorMessage = fetchedData.statusText;
    if (fetchedData.status === 500) {
      try {
        const data = await fetchedData.json();
        if (data.detail && data.detail.error) {
          try {
            const error = JSON.parse(data.detail.error);
            if (error.type && error.type === CLUSTER_NOT_UP_ERROR) {
              // The serve controller is not up (e.g. no services have ever
              // been launched); treat as "no services".
              return { services: [], controllerStopped: true };
            } else {
              errorMessage = error.message || String(data.detail.error);
            }
          } catch (jsonError) {
            console.error(
              'Error parsing JSON from data.detail.error:',
              jsonError
            );
            errorMessage = String(data.detail.error);
          }
        }
      } catch (dataError) {
        console.error('Error parsing response JSON:', dataError);
        errorMessage = String(dataError);
      }
    }

    if (!fetchedData.ok) {
      const msg = `API request to get services result failed with status ${fetchedData.status}, error: ${errorMessage}`;
      throw new Error(msg);
    }

    const data = await fetchedData.json();
    const serviceData = data.return_value ? JSON.parse(data.return_value) : [];
    const services = (Array.isArray(serviceData) ? serviceData : []).map(
      normalizeService
    );
    return { services, controllerStopped: false };
  } catch (error) {
    console.error('Error fetching services:', error);
    throw error;
  }
}
