import { apiClient } from '@/data/connectors/client';

export const OPERATIONAL_EVENTS_UNAVAILABLE = 'OPERATIONAL_EVENTS_UNAVAILABLE';
export const STALE_OPERATIONAL_EVENT_CURSOR = 'STALE_OPERATIONAL_EVENT_CURSOR';
export const OPERATIONAL_EVENTS_UPGRADE_REQUIRED =
  'OPERATIONAL_EVENTS_UPGRADE_REQUIRED';

export class OperationalEventApiError extends Error {
  constructor(code, status) {
    super(code || `OPERATIONAL_EVENTS_HTTP_${status}`);
    this.name = 'OperationalEventApiError';
    this.code = code || `OPERATIONAL_EVENTS_HTTP_${status}`;
    this.status = status;
  }
}

async function decode(response) {
  if (response.ok) {
    return await response.json();
  }
  let code = null;
  try {
    const payload = await response.json();
    code = payload?.detail?.code || payload?.code || null;
  } catch {
    // The panel intentionally does not surface arbitrary response text.
  }
  if (response.status === 404 && !code) {
    code = OPERATIONAL_EVENTS_UPGRADE_REQUIRED;
  }
  throw new OperationalEventApiError(code, response.status);
}

function eventQuery(options) {
  const params = new URLSearchParams();
  if (options.workspace) params.set('workspace', options.workspace);
  params.set('target_type', 'cluster');
  if (options.clusterHash) {
    params.set('target_id', options.clusterHash);
  } else if (options.clusterName) {
    params.set('target_name', options.clusterName);
  }
  params.set('limit', String(options.limit || 20));
  if (options.cursor) params.set('cursor', options.cursor);
  return params.toString();
}

export async function getOperationalEvents(options, signal) {
  const query = eventQuery(options);
  const response = await apiClient.get(`/events?${query}`, { signal });
  return decode(response);
}
