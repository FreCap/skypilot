import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

jest.mock('chart.js', () => ({
  BarElement: {},
  CategoryScale: {},
  Chart: { register: jest.fn() },
  Legend: {},
  LinearScale: {},
  Tooltip: {},
}));
jest.mock('react-chartjs-2', () => ({
  Bar: () => <div data-testid="chart" />,
}));
jest.mock('@/data/connectors/estimated_spend', () => ({
  getCurrentRole: jest.fn(),
  getEstimatedSpend: jest.fn(),
}));

import {
  getCurrentRole,
  getEstimatedSpend,
} from '@/data/connectors/estimated_spend';
import { EstimatedSpend } from '@/components/estimated-spend';

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function response(days, estimatedCost) {
  return {
    as_of: 1_700_006_400,
    last_successful_refresh_at: 1_700_006_400,
    backfill_complete: true,
    stale: false,
    totals: {
      estimated_cost: estimatedCost,
      priced_machine_seconds: 3600,
      excluded_machine_seconds: 0,
    },
    days: [{ date: '2023-11-15', estimated_cost: estimatedCost }],
    workloads: [],
    clouds: [],
    excluded_by_reason: {},
    requested_days: days,
  };
}

test('keeps the newest range when an older request finishes last', async () => {
  const requests = new Map();
  getCurrentRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockImplementation((days) => {
    const request = deferred();
    requests.set(days, request);
    return request.promise;
  });

  render(<EstimatedSpend />);
  await waitFor(() => expect(requests.has(30)).toBe(true));

  fireEvent.click(screen.getByRole('button', { name: '90d' }));
  await waitFor(() => expect(requests.has(90)).toBe(true));

  await act(async () => {
    requests.get(90).resolve(response(90, 90));
  });
  expect(await screen.findAllByText('$90.00')).toHaveLength(2);

  await act(async () => {
    requests.get(30).resolve(response(30, 30));
  });
  expect(screen.getAllByText('$90.00')).toHaveLength(2);
  expect(screen.queryAllByText('$30.00')).toHaveLength(0);
  expect(getCurrentRole).toHaveBeenCalledTimes(2);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(2);
});
