import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
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
  Bar: ({ data, options }) => (
    <div
      data-testid="chart"
      data-x-stacked={String(options.scales.x.stacked)}
      data-y-stacked={String(options.scales.y.stacked)}
    >
      {data.datasets
        .map((dataset) => `${dataset.label}:${dataset.data.join(',')}`)
        .join('|')}
    </div>
  ),
}));
jest.mock('@/data/connectors/client', () => ({
  getCurrentUserRole: jest.fn(),
}));
jest.mock('@/data/connectors/estimated_spend', () => ({
  getEstimatedSpend: jest.fn(),
}));

import { getCurrentUserRole } from '@/data/connectors/client';
import { getEstimatedSpend } from '@/data/connectors/estimated_spend';
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
    group_by: 'job',
    groups: [],
    series: [],
    excluded_by_reason: {},
    requested_days: days,
  };
}

test('keeps the newest range when an older request finishes last', async () => {
  const requests = new Map();
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockImplementation((days, groupBy) => {
    const request = deferred();
    requests.set(`${days}:${groupBy}`, request);
    return request.promise;
  });

  render(<EstimatedSpend />);
  await waitFor(() => expect(requests.has('30:job')).toBe(true));

  fireEvent.click(screen.getByRole('button', { name: '90d' }));
  await waitFor(() => expect(requests.has('90:job')).toBe(true));

  await act(async () => {
    requests.get('90:job').resolve(response(90, 90));
  });
  expect(await screen.findAllByText('$90.00')).toHaveLength(2);

  await act(async () => {
    requests.get('30:job').resolve(response(30, 30));
  });
  expect(screen.getAllByText('$90.00')).toHaveLength(2);
  expect(screen.queryAllByText('$30.00')).toHaveLength(0);
  expect(getCurrentUserRole).toHaveBeenCalledTimes(2);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(2);
});

test('groups the chart and table by user with purchase-option costs', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockImplementation(async (days, groupBy) => {
    if (groupBy === 'user') {
      return {
        ...response(days, 10),
        group_by: 'user',
        groups: [
          {
            user_hash: 'user-1',
            user_name: 'Alice',
            estimated_cost: 10,
            spot_estimated_cost: 4,
            on_demand_estimated_cost: 6,
            priced_machine_seconds: 7200,
            excluded_machine_seconds: 0,
          },
        ],
        series: [
          {
            user_hash: 'user-1',
            user_name: 'Alice',
            estimated_cost_by_day: [10],
          },
        ],
      };
    }
    return response(days, 10);
  });

  render(<EstimatedSpend />);
  await waitFor(() =>
    expect(getEstimatedSpend).toHaveBeenCalledWith(30, 'job')
  );

  fireEvent.change(screen.getByLabelText('Group spend by'), {
    target: { value: 'user' },
  });
  await waitFor(() =>
    expect(getEstimatedSpend).toHaveBeenCalledWith(30, 'user')
  );

  const aliceRow = (await screen.findByText('Alice')).closest('tr');
  expect(within(aliceRow).getByText('$10.00')).toBeTruthy();
  expect(within(aliceRow).getByText('$4.00')).toBeTruthy();
  expect(within(aliceRow).getByText('$6.00')).toBeTruthy();
  expect(screen.getByRole('columnheader', { name: 'Spot' })).toBeTruthy();
  expect(screen.getByRole('columnheader', { name: 'On-demand' })).toBeTruthy();
  expect(screen.getByTestId('chart')).toHaveTextContent('Alice:10');
  expect(screen.getByTestId('chart')).toHaveAttribute('data-x-stacked', 'true');
  expect(screen.getByTestId('chart')).toHaveAttribute('data-y-stacked', 'true');
});

test('shows spot and on-demand as purchase-option groups', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockImplementation(async (days, groupBy) => {
    if (groupBy === 'purchase_option') {
      return {
        ...response(days, 10),
        group_by: 'purchase_option',
        groups: [
          {
            purchase_option: 'spot',
            estimated_cost: 4,
            spot_estimated_cost: 4,
            on_demand_estimated_cost: 0,
            priced_machine_seconds: 3600,
            excluded_machine_seconds: 0,
          },
          {
            purchase_option: 'on_demand',
            estimated_cost: 6,
            spot_estimated_cost: 0,
            on_demand_estimated_cost: 6,
            priced_machine_seconds: 3600,
            excluded_machine_seconds: 0,
          },
        ],
        series: [
          { purchase_option: 'spot', estimated_cost_by_day: [4] },
          { purchase_option: 'on_demand', estimated_cost_by_day: [6] },
        ],
      };
    }
    return response(days, 10);
  });

  render(<EstimatedSpend />);
  fireEvent.change(await screen.findByLabelText('Group spend by'), {
    target: { value: 'purchase_option' },
  });

  await waitFor(() =>
    expect(getEstimatedSpend).toHaveBeenCalledWith(30, 'purchase_option')
  );
  expect(await screen.findByText('Spend by purchase option')).toBeTruthy();
  expect(screen.getByTestId('chart')).toHaveTextContent('Spot:4');
  expect(screen.getByTestId('chart')).toHaveTextContent('On-demand:6');
  expect(screen.queryByRole('columnheader', { name: 'Spot' })).not.toBeTruthy();
  expect(
    screen.queryByRole('columnheader', { name: 'On-demand' })
  ).not.toBeTruthy();
});

test('does not invent purchase-option costs for a legacy server response', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const legacyResponse = response(30, 10);
  delete legacyResponse.group_by;
  delete legacyResponse.groups;
  delete legacyResponse.series;
  legacyResponse.workloads = [
    {
      workload_type: 'managed_job',
      workload_id: '42',
      estimated_cost: 10,
      priced_machine_seconds: 3600,
      excluded_machine_seconds: 0,
    },
  ];
  getEstimatedSpend.mockResolvedValue(legacyResponse);

  render(<EstimatedSpend />);

  expect(await screen.findByText('Managed job #42')).toBeTruthy();
  expect(screen.queryByLabelText('Group spend by')).not.toBeTruthy();
  expect(screen.queryByRole('columnheader', { name: 'Spot' })).not.toBeTruthy();
  expect(
    screen.queryByRole('columnheader', { name: 'On-demand' })
  ).not.toBeTruthy();
});
