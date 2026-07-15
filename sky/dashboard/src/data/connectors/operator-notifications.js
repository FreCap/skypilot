import { apiClient } from '@/data/connectors/client';

const listRequests = new Map();
const acknowledgeRequests = new Map();

async function parseResponse(response, fallback) {
  if (response.ok) return response.json();
  let detail = '';
  try {
    const payload = await response.json();
    detail = payload?.detail || '';
  } catch (_) {
    // Use the fallback below.
  }
  const error = new Error(detail || `${fallback} (${response.status})`);
  error.status = response.status;
  throw error;
}

export function getOperatorNotifications(days = 7) {
  const key = String(days);
  const existing = listRequests.get(key);
  if (existing) return existing;

  const request = (async () => {
    const response = await apiClient.get(`/notifications?days=${days}`);
    return parseResponse(response, 'Failed to fetch operator notifications');
  })().finally(() => listRequests.delete(key));
  listRequests.set(key, request);
  return request;
}

export function acknowledgeOperatorNotifications(throughSequence) {
  const key = String(throughSequence);
  const existing = acknowledgeRequests.get(key);
  if (existing) return existing;

  const request = (async () => {
    const response = await apiClient.post('/notifications/read', {
      through_sequence: throughSequence,
    });
    return parseResponse(response, 'Failed to acknowledge notifications');
  })().finally(() => acknowledgeRequests.delete(key));
  acknowledgeRequests.set(key, request);
  return request;
}

export function resetOperatorNotificationRequestsForTests() {
  listRequests.clear();
  acknowledgeRequests.clear();
}
