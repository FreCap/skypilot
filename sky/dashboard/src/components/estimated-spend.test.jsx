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
  Bar: ({ data, options }) => {
    const serviceDataset = data.datasets.find((dataset) =>
      Object.prototype.hasOwnProperty.call(dataset, 'estimatedCostByDay')
    );
    const positiveContext = serviceDataset
      ? {
          dataset: serviceDataset,
          dataIndex: 0,
          parsed: { y: serviceDataset.data[0] },
        }
      : {
          dataset: { label: 'Cluster alpha' },
          dataIndex: 0,
          parsed: { y: 1.25 },
        };
    const zeroContext = {
      dataset: { label: 'Cluster zero' },
      parsed: { y: 0 },
    };
    return (
      <div
        data-testid="chart"
        data-x-stacked={String(options.scales.x.stacked)}
        data-y-stacked={String(options.scales.y.stacked)}
        data-tooltip-label={options.plugins.tooltip.callbacks.label(
          positiveContext
        )}
        data-tooltip-shows-zero={String(
          options.plugins.tooltip.filter(zeroContext)
        )}
      >
        {data.datasets
          .map((dataset) => `${dataset.label}:${dataset.data.join(',')}`)
          .join('|')}
      </div>
    );
  },
}));
jest.mock('@/data/connectors/client', () => ({
  getCurrentUserRole: jest.fn(),
}));
jest.mock('@/data/connectors/estimated_spend', () => ({
  getEstimatedSpend: jest.fn(),
  getEstimatedSpendDrilldown: jest.fn(),
}));

import { getCurrentUserRole } from '@/data/connectors/client';
import {
  getEstimatedSpend,
  getEstimatedSpendDrilldown,
} from '@/data/connectors/estimated_spend';
import {
  EstimatedSpend,
  formatCostPerRequest,
  shiftUtcDate,
  utcDateString,
  workloadLabel,
} from '@/components/estimated-spend';

beforeEach(() => {
  getEstimatedSpendDrilldown.mockResolvedValue({
    level: 'owner',
    rows: [],
    total: 0,
    offset: 0,
    limit: 50,
    has_more: false,
  });
});

afterEach(() => {
  jest.clearAllMocks();
});

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
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
    group_by: 'user',
    groups: [],
    series: [],
    excluded_by_reason: {},
    requested_days: days,
  };
}

test('keeps legacy flat workload identities distinguishable', () => {
  expect(
    workloadLabel({ workload_type: 'managed', workload_id: 'legacy-a' })
  ).toBe('Legacy managed attempt · legacy-a');
  expect(
    workloadLabel({ workload_type: 'managed_unattributed', workload_id: null })
  ).toBe('Legacy managed, parent unknown');
});

function rollingRange(days, endOffset = 0) {
  const endDate = shiftUtcDate(utcDateString(), endOffset);
  return {
    startDate: shiftUtcDate(endDate, -(days - 1)),
    endDate,
  };
}

test('formats compute cost per request with four decimal places', () => {
  expect(formatCostPerRequest(0)).toBe('$0.0000');
  expect(formatCostPerRequest(0.00321)).toBe('$0.0032');
  expect(formatCostPerRequest(0.00001)).toBe('<$0.0001');
  expect(formatCostPerRequest(0.25)).toBe('$0.2500');
  expect(formatCostPerRequest(null)).toBe('N/A');
});

test('renders reserved zero-cost service capacity as available', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const estimate = response(1, 0);
  estimate.service_requests = {
    available: true,
    non_rejected: {
      available: true,
      definition: 'non_rejected_inbound_requests',
      coverage_start_utc: Date.parse('2023-11-15T00:00:00Z') / 1000,
      coverage: 'complete',
      complete_by_day: [true],
      total_request_count: 4,
      services: [
        {
          service_name: 'reserved-service',
          request_count: 4,
          estimated_cost: 0,
          estimated_cost_per_request: 0,
          ratio_request_count: 4,
          ratio_coverage_start_utc: Date.parse('2023-11-15T00:00:00Z') / 1000,
          priced_machine_seconds: 3600,
          excluded_machine_seconds: 0,
          cost_coverage: 'complete',
        },
      ],
      series: [
        {
          service_name: 'reserved-service',
          request_count_by_day: [4],
          estimated_cost_by_day: [0],
          estimated_cost_per_request_by_day: [0],
        },
      ],
    },
  };
  getEstimatedSpend.mockResolvedValue(estimate);

  render(<EstimatedSpend />);

  expect(await screen.findByText('reserved-service')).toBeTruthy();
  expect(screen.getByText('$0.0000')).toBeTruthy();
  expect(screen.queryByText('unpriced capacity')).not.toBeTruthy();
  expect(screen.getAllByTestId('chart')[1]).toHaveAttribute(
    'data-tooltip-label',
    'reserved-service: 4 non-rejected requests,Est. compute: $0.00,Est. compute cost / non-rejected request: $0.0000'
  );
});

test('keeps the newest range when an older request finishes last', async () => {
  const requests = new Map();
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockImplementation((days, groupBy) => {
    const request = deferred();
    requests.set(`${days}:${groupBy}`, request);
    return request.promise;
  });

  render(<EstimatedSpend />);
  await waitFor(() => expect(requests.has('30:user')).toBe(true));

  fireEvent.click(screen.getByRole('button', { name: '90d' }));
  await waitFor(() => expect(requests.has('90:user')).toBe(true));

  await act(async () => {
    requests.get('90:user').resolve(response(90, 90));
  });
  expect(await screen.findAllByText('$90.00')).toHaveLength(2);

  await act(async () => {
    requests.get('30:user').resolve(response(30, 30));
  });
  expect(screen.getAllByText('$90.00')).toHaveLength(2);
  expect(screen.queryAllByText('$30.00')).toHaveLength(0);
  expect(getCurrentUserRole).toHaveBeenCalledTimes(2);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(2);
});

test('coalesces interval refreshes while the same request is pending', async () => {
  jest.useFakeTimers();
  const pendingEstimate = deferred();
  getCurrentUserRole.mockClear();
  getEstimatedSpend.mockClear();
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockReturnValue(pendingEstimate.promise);

  const { unmount } = render(<EstimatedSpend />);
  await act(async () => {});
  expect(getCurrentUserRole).toHaveBeenCalledTimes(1);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(1);

  await act(async () => {
    jest.advanceTimersByTime(90_000);
  });
  expect(getCurrentUserRole).toHaveBeenCalledTimes(1);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(1);

  await act(async () => {
    pendingEstimate.resolve(response(30, 30));
  });
  await act(async () => {
    jest.advanceTimersByTime(30_000);
  });
  expect(getCurrentUserRole).toHaveBeenCalledTimes(2);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(2);

  unmount();
  await act(async () => {
    jest.advanceTimersByTime(90_000);
  });
  expect(getCurrentUserRole).toHaveBeenCalledTimes(2);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(2);
  jest.useRealTimers();
});

test('manual refresh supersedes a slow automatic refresh', async () => {
  jest.useFakeTimers();
  const automaticRefresh = deferred();
  const manualRefresh = deferred();
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend
    .mockResolvedValueOnce(response(30, 30))
    .mockImplementationOnce(() => automaticRefresh.promise)
    .mockImplementationOnce(() => manualRefresh.promise);

  render(<EstimatedSpend />);

  try {
    expect(await screen.findAllByText('$30.00')).toHaveLength(2);

    await act(async () => {
      jest.advanceTimersByTime(60_000);
      await Promise.resolve();
    });
    expect(getEstimatedSpend).toHaveBeenCalledTimes(2);

    const refreshButton = screen.getByRole('button', { name: /refresh/i });
    expect(refreshButton).toBeEnabled();
    fireEvent.click(refreshButton);
    await waitFor(() => expect(getEstimatedSpend).toHaveBeenCalledTimes(3));

    await act(async () => {
      manualRefresh.resolve(response(30, 90));
      await manualRefresh.promise;
    });
    expect(await screen.findAllByText('$90.00')).toHaveLength(2);

    await act(async () => {
      automaticRefresh.resolve(response(30, 60));
      await automaticRefresh.promise;
    });
    expect(screen.getAllByText('$90.00')).toHaveLength(2);
    expect(screen.queryAllByText('$60.00')).toHaveLength(0);
  } finally {
    jest.useRealTimers();
  }
});

test('pauses hidden refreshes and refreshes once on visibility restore', async () => {
  const visibilityDescriptor = Object.getOwnPropertyDescriptor(
    window.document,
    'visibilityState'
  );
  jest.useFakeTimers();
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockResolvedValue(response(30, 30));
  setDocumentVisibility('hidden');
  const { unmount } = render(<EstimatedSpend />);
  let mounted = true;

  try {
    await act(async () => {});
    expect(getCurrentUserRole).toHaveBeenCalledTimes(1);
    expect(getEstimatedSpend).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(120_000 - 1);
    });
    expect(getCurrentUserRole).toHaveBeenCalledTimes(1);
    expect(getEstimatedSpend).toHaveBeenCalledTimes(1);

    setDocumentVisibility('visible');
    await act(async () => {
      window.document.dispatchEvent(new Event('visibilitychange'));
      await Promise.resolve();
    });
    expect(getCurrentUserRole).toHaveBeenCalledTimes(2);
    expect(getEstimatedSpend).toHaveBeenCalledTimes(2);

    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getCurrentUserRole).toHaveBeenCalledTimes(2);
    expect(getEstimatedSpend).toHaveBeenCalledTimes(2);

    await act(async () => {
      jest.advanceTimersByTime(60_000);
    });
    expect(getCurrentUserRole).toHaveBeenCalledTimes(3);
    expect(getEstimatedSpend).toHaveBeenCalledTimes(3);

    unmount();
    mounted = false;
    window.document.dispatchEvent(new Event('visibilitychange'));
    await act(async () => {
      jest.advanceTimersByTime(60_000);
    });
    expect(getCurrentUserRole).toHaveBeenCalledTimes(3);
    expect(getEstimatedSpend).toHaveBeenCalledTimes(3);
  } finally {
    if (mounted) unmount();
    if (visibilityDescriptor) {
      Object.defineProperty(
        window.document,
        'visibilityState',
        visibilityDescriptor
      );
    } else {
      delete window.document.visibilityState;
    }
    jest.useRealTimers();
  }
});

test('defaults to owner hierarchy with purchase-option costs', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpendDrilldown.mockResolvedValue({
    level: 'owner',
    rows: [
      {
        user_hash: 'user-1',
        user_name: 'Alice',
        owner_unknown: false,
        workload_count: 2,
        cluster_count: 3,
        estimated_cost: 10,
        spot_estimated_cost: 4,
        on_demand_estimated_cost: 6,
        priced_machine_seconds: 7200,
        excluded_machine_seconds: 0,
      },
    ],
    total: 1,
    offset: 0,
    limit: 50,
    has_more: false,
  });
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
    expect(getEstimatedSpend).toHaveBeenCalledWith(30, 'user', rollingRange(30))
  );

  const aliceRow = (await screen.findByText('Alice')).closest('tr');
  expect(within(aliceRow).getByText('$10.00')).toBeTruthy();
  expect(within(aliceRow).getByText('$4.00')).toBeTruthy();
  expect(within(aliceRow).getByText('$6.00')).toBeTruthy();
  expect(screen.getByRole('columnheader', { name: 'Spot' })).toBeTruthy();
  expect(screen.getByRole('columnheader', { name: 'On-demand' })).toBeTruthy();
  expect(screen.getByText('2 workloads · 3 attempts')).toBeTruthy();
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
    expect(getEstimatedSpend).toHaveBeenCalledWith(
      30,
      'purchase_option',
      rollingRange(30)
    )
  );
  expect(await screen.findByText('Spend by purchase option')).toBeTruthy();
  expect(screen.getByTestId('chart')).toHaveTextContent('Spot:4');
  expect(screen.getByTestId('chart')).toHaveTextContent('On-demand:6');
  expect(screen.queryByRole('columnheader', { name: 'Spot' })).not.toBeTruthy();
  expect(
    screen.queryByRole('columnheader', { name: 'On-demand' })
  ).not.toBeTruthy();
});

test('selects today and yesterday as exact UTC ranges', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockResolvedValue(response(1, 10));

  render(<EstimatedSpend />);
  await waitFor(() =>
    expect(getEstimatedSpend).toHaveBeenCalledWith(30, 'user', rollingRange(30))
  );

  fireEvent.click(screen.getByRole('button', { name: 'Today' }));
  await waitFor(() =>
    expect(getEstimatedSpend).toHaveBeenCalledWith(1, 'user', rollingRange(1))
  );

  fireEvent.click(screen.getByRole('button', { name: 'Yesterday' }));
  await waitFor(() =>
    expect(getEstimatedSpend).toHaveBeenCalledWith(
      1,
      'user',
      rollingRange(1, -1)
    )
  );
  expect(await screen.findByText('Yesterday estimate (UTC)')).toBeTruthy();
});

test('applies an arbitrary inclusive UTC date range for the owner view', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockResolvedValue(response(3, 10));
  const startDate = shiftUtcDate(utcDateString(), -4);
  const endDate = shiftUtcDate(utcDateString(), -2);

  render(<EstimatedSpend />);
  await waitFor(() =>
    expect(screen.getByRole('button', { name: 'Apply' })).toBeEnabled()
  );
  fireEvent.change(screen.getByLabelText('Start date (UTC)'), {
    target: { value: startDate },
  });
  fireEvent.change(screen.getByLabelText('End date (UTC)'), {
    target: { value: endDate },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Apply' }));

  await waitFor(() =>
    expect(getEstimatedSpend).toHaveBeenCalledWith(3, 'user', {
      startDate,
      endDate,
    })
  );
  expect(await screen.findByText('Selected range estimate')).toBeTruthy();
});

test('rejects an invalid custom range without fetching', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockResolvedValue(response(30, 10));

  render(<EstimatedSpend />);
  await waitFor(() =>
    expect(screen.getByRole('button', { name: 'Apply' })).toBeEnabled()
  );
  getEstimatedSpend.mockClear();
  fireEvent.change(screen.getByLabelText('Start date (UTC)'), {
    target: { value: utcDateString() },
  });
  fireEvent.change(screen.getByLabelText('End date (UTC)'), {
    target: { value: shiftUtcDate(utcDateString(), -1) },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Apply' }));

  expect(
    await screen.findByText('The start date must be on or before the end date.')
  ).toBeTruthy();
  expect(getEstimatedSpend).not.toHaveBeenCalled();
});

test('names tooltip entries and hides zero-valued series', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getEstimatedSpend.mockResolvedValue(response(30, 10));

  render(<EstimatedSpend />);

  const chart = await screen.findByTestId('chart');
  expect(chart).toHaveAttribute('data-tooltip-label', 'Cluster alpha: $1.25');
  expect(chart).toHaveAttribute('data-tooltip-shows-zero', 'false');
});

test('makes the chart-only Other remainder inspectable', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const estimate = response(30, 10);
  estimate.series = [
    {
      user_hash: 'user-1',
      user_name: 'Alice',
      estimated_cost_by_day: [6],
    },
    {
      is_other: true,
      estimated_cost_by_day: [4],
    },
  ];
  getEstimatedSpend.mockResolvedValue(estimate);

  render(<EstimatedSpend />);

  expect(
    await screen.findByRole('button', { name: 'Inspect Other' })
  ).toBeTruthy();
  expect(
    screen.getByText('Other is the chart-only remainder', { exact: false })
  ).toBeTruthy();
  fireEvent.click(screen.getByRole('button', { name: 'Inspect Other' }));
  expect(screen.getByLabelText('Group spend by')).toHaveValue('user');
});

test('treats a failed role lookup as an error, not a permission denial', async () => {
  getCurrentUserRole.mockClear();
  getEstimatedSpend.mockClear();
  getCurrentUserRole.mockResolvedValue({
    id: 'local',
    name: 'local',
    role: null,
    roleFetchFailed: true,
  });
  getEstimatedSpend.mockResolvedValue(response(30, 10));

  render(<EstimatedSpend />);

  await waitFor(() => expect(getCurrentUserRole).toHaveBeenCalledTimes(1));
  await waitFor(() =>
    expect(screen.getByRole('button', { name: /refresh/i })).toBeEnabled()
  );
  expect(screen.queryByText('Admin access required')).not.toBeTruthy();
  expect(getEstimatedSpend).not.toHaveBeenCalled();
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
  expect(getCurrentUserRole).toHaveBeenCalledTimes(1);
  expect(getEstimatedSpend).toHaveBeenCalledTimes(1);
  expect(screen.queryByLabelText('Group spend by')).not.toBeTruthy();
  expect(screen.queryByRole('columnheader', { name: 'Spot' })).not.toBeTruthy();
  expect(
    screen.queryByRole('columnheader', { name: 'On-demand' })
  ).not.toBeTruthy();
});

test('replaces a stale forbidden banner with a later role-fetch failure', async () => {
  jest.useFakeTimers();
  getCurrentUserRole
    .mockResolvedValueOnce({ role: 'user' })
    .mockResolvedValueOnce({
      id: 'local',
      name: 'local',
      role: null,
      roleFetchFailed: true,
    });
  getEstimatedSpend.mockResolvedValue(response(30, 10));

  render(<EstimatedSpend />);

  try {
    expect(await screen.findByText('Admin access required')).toBeTruthy();
    expect(getEstimatedSpend).not.toHaveBeenCalled();

    await act(async () => {
      jest.advanceTimersByTime(60_000);
      await Promise.resolve();
    });

    expect(
      await screen.findByText('Failed to fetch current role')
    ).toBeTruthy();
    expect(screen.queryByText('Admin access required')).not.toBeTruthy();
    expect(getEstimatedSpend).not.toHaveBeenCalled();
  } finally {
    jest.useRealTimers();
  }
});

test('renders daily per-service request volume and counting semantics', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const estimate = response(2, 10);
  estimate.days = [
    { date: '2023-11-14', estimated_cost: 4 },
    { date: '2023-11-15', estimated_cost: 6 },
  ];
  estimate.service_requests = {
    available: true,
    non_rejected: {
      available: true,
      definition: 'non_rejected_inbound_requests',
      coverage_start_utc: Date.parse('2023-11-14T00:01:00Z') / 1000,
      coverage: 'partial',
      complete_by_day: [false, true],
      total_request_count: 20,
      services: [
        {
          service_name: 'service-a',
          request_count: 15,
          estimated_cost: 0.0288,
          estimated_cost_per_request: 0.0032,
          ratio_request_count: 9,
          ratio_coverage_start_utc: Date.parse('2023-11-15T00:00:00Z') / 1000,
          priced_machine_seconds: 3600,
          excluded_machine_seconds: 0,
          cost_coverage: 'complete',
        },
        {
          service_name: 'service-b',
          request_count: 5,
          estimated_cost: 0.01,
          estimated_cost_per_request: null,
          ratio_request_count: 3,
          ratio_coverage_start_utc: Date.parse('2023-11-15T00:00:00Z') / 1000,
          priced_machine_seconds: 3600,
          excluded_machine_seconds: 1800,
          cost_coverage: 'partial',
        },
      ],
      series: [
        {
          service_name: 'service-a',
          request_count_by_day: [6, 9],
          estimated_cost_by_day: [0.0192, 0.0288],
          estimated_cost_per_request_by_day: [0.0032, 0.0032],
        },
        {
          service_name: 'service-b',
          request_count_by_day: [2, 3],
          estimated_cost_by_day: [0, 0.01],
          estimated_cost_per_request_by_day: [null, null],
        },
      ],
    },
  };
  getEstimatedSpend.mockResolvedValue(estimate);

  render(<EstimatedSpend />);

  expect(
    await screen.findByText('Daily non-rejected requests by service')
  ).toBeTruthy();
  expect(
    screen.getByText('Known non-rejected requests in selected range')
  ).toBeTruthy();
  expect(
    screen.getByText('Requests rejected by the load balancer are excluded.', {
      exact: false,
    })
  ).toBeTruthy();
  expect(
    screen.getByText('Days with legacy or mixed load balancers are omitted.', {
      exact: false,
    })
  ).toBeTruthy();
  expect(screen.getByText('service-a')).toBeTruthy();
  expect(screen.queryByText('75.0%')).not.toBeTruthy();
  expect(
    screen.getByRole('columnheader', {
      name: 'Est. compute cost / non-rejected request',
    })
  ).toBeTruthy();
  expect(screen.getByText('$0.0032')).toBeTruthy();
  expect(screen.getByText('unpriced capacity')).toBeTruthy();
  expect(screen.getByText('based on 9 requests')).toBeTruthy();
  expect(screen.getAllByTestId('chart')[1].textContent).toContain(
    'service-a:6,9|service-b:2,3'
  );
  expect(screen.getAllByTestId('chart')[1]).toHaveAttribute(
    'data-tooltip-label',
    'service-a: 6 non-rejected requests,Est. compute: $0.02,Est. compute cost / non-rejected request: $0.0032'
  );
});

test('shows each service request denominator coverage independently', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const estimate = response(30, 1);
  const { startDate } = rollingRange(30);
  const firstServiceStart = shiftUtcDate(startDate, 1);
  const secondServiceStart = shiftUtcDate(startDate, 2);
  estimate.service_requests = {
    available: true,
    non_rejected: {
      available: true,
      definition: 'non_rejected_inbound_requests',
      coverage_start_utc: Date.parse(`${startDate}T00:00:00Z`) / 1000,
      coverage: 'complete',
      complete_by_day: [true],
      total_request_count: 3,
      services: [
        {
          service_name: 'service-a',
          coverage: 'complete',
          request_count: 2,
          estimated_cost_per_request: 0.5,
          ratio_request_count: 2,
          ratio_coverage_start_utc:
            Date.parse(`${firstServiceStart}T00:00:00Z`) / 1000,
          cost_coverage: 'complete',
        },
        {
          service_name: 'service-b',
          coverage: 'complete',
          request_count: 1,
          estimated_cost_per_request: 1,
          ratio_request_count: 1,
          ratio_coverage_start_utc:
            Date.parse(`${secondServiceStart}T00:00:00Z`) / 1000,
          cost_coverage: 'complete',
        },
      ],
      series: [],
    },
  };
  getEstimatedSpend.mockResolvedValue(estimate);

  render(<EstimatedSpend />);

  expect(await screen.findByText('service-a')).toBeTruthy();
  expect(
    screen.getByText(`request denominator since ${firstServiceStart} UTC`)
  ).toBeTruthy();
  expect(
    screen.getByText(`request denominator since ${secondServiceStart} UTC`)
  ).toBeTruthy();
  expect(
    screen.queryByText('Cost/request uses complete request-history days', {
      exact: false,
    })
  ).not.toBeTruthy();
});

test('renders unavailable service request coverage as N/A, not zero', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const estimate = response(1, 1);
  estimate.service_requests = {
    available: true,
    non_rejected: {
      available: true,
      definition: 'non_rejected_inbound_requests',
      coverage_start_utc: Date.parse('2023-11-15T00:00:00Z') / 1000,
      coverage: 'partial',
      complete_by_day: [false],
      total_request_count: 0,
      services: [
        {
          service_name: 'legacy-service',
          coverage: 'unavailable',
          request_count: 0,
          estimated_cost_per_request: null,
          ratio_request_count: 0,
          ratio_coverage_start_utc: null,
          cost_coverage: 'complete',
        },
      ],
      series: [
        {
          service_name: 'legacy-service',
          request_count_by_day: [null],
        },
      ],
    },
  };
  getEstimatedSpend.mockResolvedValue(estimate);

  render(<EstimatedSpend />);

  const serviceName = await screen.findByText('legacy-service');
  const row = serviceName.closest('tr');
  expect(within(row).getByText('request history unavailable')).toBeTruthy();
  expect(within(row).getAllByText('N/A')).toHaveLength(3);
  expect(within(row).queryByText('0')).not.toBeTruthy();
});

test('shows request volume while the first spend estimate is pending', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const estimate = response(1, 0);
  estimate.as_of = null;
  estimate.last_successful_refresh_at = null;
  estimate.service_requests = {
    available: true,
    non_rejected: {
      available: true,
      definition: 'non_rejected_inbound_requests',
      coverage_start_utc: Date.parse('2023-11-15T00:01:00Z') / 1000,
      coverage: 'partial',
      complete_by_day: [false],
      total_request_count: 3,
      services: [{ service_name: 'service-a', request_count: 3 }],
      series: [{ service_name: 'service-a', request_count_by_day: [3] }],
    },
  };
  getEstimatedSpend.mockResolvedValue(estimate);

  render(<EstimatedSpend />);

  expect(await screen.findByText('Preparing the first estimate')).toBeTruthy();
  expect(
    screen.getByText('Daily non-rejected requests by service')
  ).toBeTruthy();
  expect(screen.getByText('service-a')).toBeTruthy();
});

test('does not fall back to legacy attempt counts for cost per request', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  const estimate = response(1, 1);
  estimate.service_requests = {
    available: true,
    definition: 'admitted_inbound_requests',
    total_request_count: 100,
    services: [
      {
        service_name: 'legacy-attempts',
        request_count: 100,
        estimated_cost_per_request: 0.0001,
      },
    ],
    series: [],
  };
  getEstimatedSpend.mockResolvedValue(estimate);

  render(<EstimatedSpend />);

  expect(
    await screen.findByText(
      'Exact non-rejected request history is unavailable on this server or for this range.',
      { exact: false }
    )
  ).toBeTruthy();
  expect(screen.queryByText('legacy-attempts')).not.toBeTruthy();
  expect(screen.queryByText('$0.0001')).not.toBeTruthy();
});
