import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

jest.mock('@/data/connectors/estimated_spend', () => ({
  getEstimatedSpendDrilldown: jest.fn(),
}));

import { SpendAttributionTable } from '@/components/spend-attribution-table';
import { getEstimatedSpendDrilldown } from '@/data/connectors/estimated_spend';

const DATE_RANGE = {
  startDate: '2026-07-01',
  endDate: '2026-07-30',
  days: 30,
};

function page(level, rows, hasMore = false, offset = 0, total = rows.length) {
  return {
    level,
    rows,
    total,
    offset,
    limit: 50,
    has_more: hasMore,
  };
}

function owner(overrides = {}) {
  return {
    user_hash: 'alice',
    user_name: 'Alice',
    owner_unknown: false,
    workload_count: 1,
    cluster_count: 2,
    estimated_cost: 10,
    spot_estimated_cost: 4,
    on_demand_estimated_cost: 6,
    priced_machine_seconds: 7200,
    excluded_machine_seconds: 0,
    ...overrides,
  };
}

function workload(overrides = {}) {
  return {
    workload_type: 'managed_job',
    workload_id: '42',
    task_count: 2,
    cluster_count: 2,
    estimated_cost: 10,
    spot_estimated_cost: 4,
    on_demand_estimated_cost: 6,
    priced_machine_seconds: 7200,
    excluded_machine_seconds: 0,
    ...overrides,
  };
}

function tableElement(fallbackGroups = [], snapshotAt = 123) {
  return (
    <SpendAttributionTable
      dateRange={DATE_RANGE}
      snapshotAt={snapshotAt}
      totalCost={10}
      fallbackGroups={fallbackGroups}
      formatCurrency={(value) => `$${Number(value || 0).toFixed(2)}`}
      formatHours={(value) => `${Number(value || 0) / 3600} h`}
      workloadLabel={(row) =>
        row.workload_type === 'managed_unattributed'
          ? 'Legacy managed, parent unknown'
          : `Managed job #${row.workload_id}`
      }
    />
  );
}

function renderTable(fallbackGroups = [], snapshotAt = 123) {
  return render(tableElement(fallbackGroups, snapshotAt));
}

afterEach(() => {
  jest.clearAllMocks();
});

test('drills from owner through a multi-task managed job to attempts', async () => {
  getEstimatedSpendDrilldown.mockImplementation(async ({ level, scope }) => {
    if (level === 'owner') return page('owner', [owner()]);
    if (level === 'workload') return page('workload', [workload()]);
    if (level === 'task') {
      return page('task', [
        {
          workload_task_id: 0,
          cluster_count: 1,
          estimated_cost: 4,
          spot_estimated_cost: 4,
          on_demand_estimated_cost: 0,
          priced_machine_seconds: 3600,
          excluded_machine_seconds: 0,
        },
        {
          workload_task_id: 1,
          cluster_count: 1,
          estimated_cost: 6,
          spot_estimated_cost: 0,
          on_demand_estimated_cost: 6,
          priced_machine_seconds: 3600,
          excluded_machine_seconds: 0,
        },
      ]);
    }
    expect(scope.workload_task_id).toBe(0);
    return page('cluster', [
      {
        cluster_hash: 'attempt-a',
        cluster_name: 'job-42-recovery-a',
        workspace: 'default',
        estimated_cost: 4,
        spot_estimated_cost: 4,
        on_demand_estimated_cost: 0,
        priced_machine_seconds: 3600,
        excluded_machine_seconds: 0,
      },
    ]);
  });

  renderTable();

  fireEvent.click(await screen.findByRole('button', { name: 'Expand Alice' }));
  fireEvent.click(
    await screen.findByRole('button', { name: 'Expand Managed job #42' })
  );
  expect(await screen.findByText('Task 0')).toBeTruthy();
  expect(screen.getByText('Task 1')).toBeTruthy();
  fireEvent.click(
    screen.getByRole('button', {
      name: 'Expand Task 0',
    })
  );

  expect(await screen.findByText('job-42-recovery-a')).toBeTruthy();
  expect(getEstimatedSpendDrilldown).toHaveBeenCalledWith(
    expect.objectContaining({
      level: 'cluster',
      scope: {
        owner_user_hash: 'alice',
        workload_type: 'managed_job',
        workload_id: '42',
        workload_task_id: 0,
      },
    })
  );
});

test('single-task and legacy workloads expand directly to attempts', async () => {
  getEstimatedSpendDrilldown.mockImplementation(async ({ level, scope }) => {
    if (level === 'owner') return page('owner', [owner()]);
    if (level === 'workload') {
      return page('workload', [
        workload({
          workload_type: 'managed_unattributed',
          workload_id: null,
          task_count: 0,
          cluster_count: 2,
        }),
      ]);
    }
    expect(level).toBe('cluster');
    expect(scope).toEqual({
      owner_user_hash: 'alice',
      workload_type: 'managed_unattributed',
    });
    return page('cluster', [
      {
        cluster_hash: 'legacy-a',
        cluster_name: 'legacy-a',
        workspace: 'default',
        estimated_cost: 10,
        spot_estimated_cost: 4,
        on_demand_estimated_cost: 6,
        priced_machine_seconds: 7200,
        excluded_machine_seconds: 0,
      },
    ]);
  });

  renderTable();

  fireEvent.click(await screen.findByRole('button', { name: 'Expand Alice' }));
  fireEvent.click(
    await screen.findByRole('button', {
      name: 'Expand Legacy managed, parent unknown',
    })
  );

  expect(await screen.findByText('legacy-a')).toBeTruthy();
  expect(screen.queryByText('Task 0')).not.toBeTruthy();
});

test('does not hide null-task attempts behind a partial task hierarchy', async () => {
  getEstimatedSpendDrilldown.mockImplementation(async ({ level, scope }) => {
    if (level === 'owner') return page('owner', [owner()]);
    if (level === 'workload') {
      return page('workload', [
        workload({
          task_count: 2,
          unknown_task_cluster_count: 1,
          cluster_count: 3,
        }),
      ]);
    }
    expect(level).toBe('cluster');
    expect(scope.workload_task_id).toBeUndefined();
    return page('cluster', [
      {
        cluster_hash: 'unknown-task-attempt',
        cluster_name: 'unknown-task-attempt',
        workspace: 'default',
        estimated_cost: 1,
        spot_estimated_cost: 0,
        on_demand_estimated_cost: 1,
        priced_machine_seconds: 3600,
        excluded_machine_seconds: 0,
      },
    ]);
  });

  renderTable();
  fireEvent.click(await screen.findByRole('button', { name: 'Expand Alice' }));
  expect(
    await screen.findByText('3 attempts · task grouping incomplete')
  ).toBeTruthy();
  fireEvent.click(
    screen.getByRole('button', { name: 'Expand Managed job #42' })
  );

  expect(await screen.findByText('unknown-task-attempt')).toBeTruthy();
  expect(screen.queryByText('Task 0')).not.toBeTruthy();
});

test('appends paginated owners without replacing the first page', async () => {
  getEstimatedSpendDrilldown.mockImplementation(async ({ level, offset }) => {
    expect(level).toBe('owner');
    if (offset === 0) return page('owner', [owner()], true, 0, 2);
    return page(
      'owner',
      [
        owner({
          user_hash: 'bob',
          user_name: 'Bob',
          estimated_cost: 2,
          workload_count: 1,
          cluster_count: 1,
        }),
      ],
      false,
      1,
      2
    );
  });

  renderTable();

  fireEvent.click(
    await screen.findByRole('button', { name: 'Load more owners' })
  );

  expect(await screen.findByText('Bob')).toBeTruthy();
  expect(screen.getByText('Alice')).toBeTruthy();
  expect(getEstimatedSpendDrilldown).toHaveBeenLastCalledWith(
    expect.objectContaining({ level: 'owner', offset: 1 })
  );
});

test('refreshes the hierarchy when the materialized snapshot advances', async () => {
  getEstimatedSpendDrilldown.mockResolvedValue(page('owner', []));
  const { rerender } = renderTable();
  await waitFor(() =>
    expect(getEstimatedSpendDrilldown).toHaveBeenCalledTimes(1)
  );

  rerender(tableElement([], 124));

  await waitFor(() =>
    expect(getEstimatedSpendDrilldown).toHaveBeenCalledTimes(2)
  );
});

test('falls back to the flat owner summary against an older server', async () => {
  const unsupported = new Error('Not found');
  unsupported.status = 404;
  getEstimatedSpendDrilldown.mockRejectedValue(unsupported);

  renderTable([
    {
      user_hash: 'alice',
      user_name: 'Alice',
      estimated_cost: 10,
      spot_estimated_cost: 4,
      on_demand_estimated_cost: 6,
      priced_machine_seconds: 7200,
      excluded_machine_seconds: 0,
    },
  ]);

  expect(await screen.findByText('Alice')).toBeTruthy();
  expect(
    screen.getByText('Detailed attribution is unavailable on this server.', {
      exact: false,
    })
  ).toBeTruthy();
  expect(
    screen.queryByRole('button', { name: 'Expand Alice' })
  ).not.toBeTruthy();
});

test('keeps a child failure local and retryable', async () => {
  getEstimatedSpendDrilldown.mockImplementation(async ({ level }) => {
    if (level === 'owner') return page('owner', [owner()]);
    throw new Error('Child query failed');
  });

  renderTable();
  fireEvent.click(await screen.findByRole('button', { name: 'Expand Alice' }));

  expect(await screen.findByText('Child query failed')).toBeTruthy();
  expect(screen.getByText('Alice')).toBeTruthy();
  expect(screen.getByRole('button', { name: 'Retry' })).toBeTruthy();
});
