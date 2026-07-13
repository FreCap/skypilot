jest.mock('@/data/connectors/client', () => ({
  apiClient: { get: jest.fn() },
}));

import { apiClient } from '@/data/connectors/client';
import { getEstimatedSpend } from '@/data/connectors/estimated_spend';

beforeEach(() => {
  apiClient.get.mockReset();
  apiClient.get.mockResolvedValue({
    ok: true,
    json: async () => ({ totals: {} }),
  });
});

test('sends an exact inclusive UTC date range', async () => {
  await getEstimatedSpend(1, 'job', {
    startDate: '2026-07-12',
    endDate: '2026-07-12',
  });

  expect(apiClient.get).toHaveBeenCalledWith(
    '/estimated_spend?days=1&group_by=job&start_date=2026-07-12&end_date=2026-07-12'
  );
});

test('keeps the legacy days query when no exact range is supplied', async () => {
  await getEstimatedSpend(30, 'user');

  expect(apiClient.get).toHaveBeenCalledWith(
    '/estimated_spend?days=30&group_by=user'
  );
});
