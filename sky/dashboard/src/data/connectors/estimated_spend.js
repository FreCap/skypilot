import { apiClient } from '@/data/connectors/client';

export async function getEstimatedSpend(
  days = 30,
  groupBy = 'job',
  dateRange = null
) {
  const query = new URLSearchParams({
    days: String(days),
    group_by: groupBy,
  });
  if (dateRange?.startDate && dateRange?.endDate) {
    query.set('start_date', dateRange.startDate);
    query.set('end_date', dateRange.endDate);
  }
  const response = await apiClient.get(`/estimated_spend?${query.toString()}`);
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
