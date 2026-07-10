import { apiClient } from '@/data/connectors/client';

export async function getEstimatedSpend(days = 30) {
  const response = await apiClient.get(`/estimated_spend?days=${days}`);
  if (!response.ok) {
    let detail = '';
    try {
      const payload = await response.json();
      detail = payload?.detail || '';
    } catch (_) {
      // Use the status-based fallback below.
    }
    const error = new Error(
      detail || `Failed to fetch estimated spend (${response.status})`
    );
    error.status = response.status;
    throw error;
  }
  return response.json();
}

export async function getCurrentRole() {
  const response = await apiClient.get('/users/role');
  if (!response.ok) {
    throw new Error(`Failed to fetch current role (${response.status})`);
  }
  return response.json();
}
