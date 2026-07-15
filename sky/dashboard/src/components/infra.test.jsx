import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';

import { ContextDetails } from '@/components/infra';
import { ContextDetails as ExtractedContextDetails } from '@/components/infra-context-details';
import { SSHNodePoolDetails } from '@/components/ssh-node-pool-details';
import {
  getSSHNodePoolStatus,
  sshDownNodePool,
  streamSSHDeploymentLogs,
  streamSSHOperationLogs,
} from '@/data/connectors/ssh-node-pools';
import { checkGrafanaAvailability, getGrafanaUrl } from '@/utils/grafana';
import { trackInfraAction } from '@/lib/analytics';

jest.mock('@/utils/grafana', () => ({
  checkGrafanaAvailability: jest.fn(),
  getGrafanaUrl: jest.fn(),
  buildGrafanaUrl: jest.fn(),
  openGrafana: jest.fn(),
}));

jest.mock('@/lib/analytics', () => ({
  trackInfraAction: jest.fn(),
}));

jest.mock('@/data/connectors/ssh-node-pools', () => ({
  getSSHNodePools: jest.fn(),
  updateSSHNodePools: jest.fn(),
  deleteSSHNodePool: jest.fn(),
  deploySSHNodePool: jest.fn(),
  sshDownNodePool: jest.fn(),
  getSSHNodePoolStatus: jest.fn(),
  streamSSHDeploymentLogs: jest.fn(),
  streamSSHOperationLogs: jest.fn(),
}));

jest.mock('@/plugins/PluginSlot', () => ({
  PluginSlot: () => null,
}));

const gpu = {
  gpu_name: 'H100',
  gpu_requestable_qty_per_node: 4,
  gpu_total: 4,
  gpu_free: 1,
  gpu_not_ready: 1,
};

const node = {
  node_name: 'worker-1',
  ip_address: '10.0.0.1',
  cpu_count: 8,
  cpu_free: 3,
  memory_gb: 32,
  memory_free_gb: 10,
  gpu_name: 'H100',
  gpu_total: 4,
  gpu_free: 1,
  is_ready: false,
  is_cordoned: true,
  taints: [
    { key: 'dedicated', effect: 'NoSchedule' },
    { key: 'accepted', effect: 'NoSchedule', tolerated: true },
  ],
};

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function sshNodePoolDetails({
  poolName = 'gpu-pool',
  handleDeploySSHPool = jest.fn(),
  handleDeleteSSHPool = jest.fn(),
} = {}) {
  return (
    <SSHNodePoolDetails
      poolName={poolName}
      gpusInContext={[]}
      nodesInContext={[]}
      handleDeploySSHPool={handleDeploySSHPool}
      handleDeleteSSHPool={handleDeleteSSHPool}
    />
  );
}

async function completeSSHNodePoolDeployment() {
  expect(await screen.findByText('Not Ready')).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'Deploy' }));
  fireEvent.click(
    within(screen.getByRole('dialog')).getByRole('button', {
      name: 'Deploy',
    })
  );
  expect(
    await screen.findByText('Deployment completed successfully!')
  ).toBeVisible();
}

describe('ContextDetails', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    checkGrafanaAvailability.mockResolvedValue(false);
    getGrafanaUrl.mockReturnValue('https://grafana.example');
    global.fetch = jest.fn();
  });

  afterEach(() => {
    delete global.fetch;
  });

  it('preserves the infra facade export as a direct alias', () => {
    expect(ContextDetails).toBe(ExtractedContextDetails);
  });

  it('preserves GPU utilization and node health presentation', async () => {
    render(
      <ContextDetails
        contextName="dev-cluster"
        gpusInContext={[gpu]}
        nodesInContext={[
          node,
          {
            ...node,
            node_name: 'worker-2',
            is_ready: true,
            is_cordoned: false,
            taints: [
              { key: 'accepted', effect: 'NoSchedule', tolerated: true },
            ],
          },
        ]}
      />
    );

    expect(screen.getByTitle('1 not ready')).toBeInTheDocument();
    expect(screen.getByTitle('2 used')).toBeInTheDocument();
    expect(screen.getByTitle('1 free')).toBeInTheDocument();

    const unhealthyRow = screen.getByText('worker-1').closest('tr');
    expect(unhealthyRow).not.toBeNull();
    expect(within(unhealthyRow).getByText('NotReady, Cordoned')).toBeVisible();
    expect(
      within(unhealthyRow).getByText('NoSchedule Taint [dedicated]')
    ).toBeVisible();
    expect(
      within(unhealthyRow).queryByText(/accepted/)
    ).not.toBeInTheDocument();

    const healthyRow = screen.getByText('worker-2').closest('tr');
    expect(healthyRow).not.toBeNull();
    expect(within(healthyRow).getByText('Healthy')).toBeVisible();

    await waitFor(() =>
      expect(checkGrafanaAvailability).toHaveBeenCalledTimes(1)
    );
    expect(global.fetch).not.toHaveBeenCalled();
  });

  it('keeps Grafana host discovery and filter updates bounded', async () => {
    checkGrafanaAvailability.mockResolvedValue(true);
    global.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        data: {
          result: [
            { metric: { node: 'worker-b' } },
            { metric: { node: 'worker-a' } },
          ],
        },
      }),
    });

    render(
      <ContextDetails
        contextName="dev-cluster"
        gpusInContext={[gpu]}
        nodesInContext={[node]}
        gpuMetricsRefreshTrigger={3}
      />
    );

    const hostSelect = await screen.findByLabelText('Node:');
    await waitFor(() => expect(global.fetch).toHaveBeenCalledTimes(1));
    expect(checkGrafanaAvailability).toHaveBeenCalledTimes(1);
    expect(global.fetch.mock.calls[0][0]).toContain(
      'cluster%3D~%22dev-cluster%22'
    );
    expect(
      within(hostSelect)
        .getAllByRole('option')
        .map((option) => option.value)
    ).toEqual(['$__all', 'worker-a', 'worker-b']);

    expect(screen.getAllByTitle(/GPU|CPU|Memory/)).toHaveLength(6);
    expect(screen.getByTitle('GPU Utilization')).toHaveAttribute(
      'src',
      expect.stringContaining('panelId=6')
    );

    fireEvent.click(screen.getByRole('button', { name: '15m' }));
    expect(trackInfraAction).toHaveBeenCalledWith('time_range_change', {
      range: '15m',
    });
    expect(screen.getByTitle('GPU Utilization')).toHaveAttribute(
      'src',
      expect.stringContaining('from=now-15m')
    );
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });
});

describe('SSHNodePoolDetails', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    getSSHNodePoolStatus.mockResolvedValue({
      pool_name: 'gpu-pool',
      status: 'Not Ready',
      reason: 'runtime missing',
    });
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it('loads status and exposes the deploy action for a non-ready pool', async () => {
    render(sshNodePoolDetails());

    expect(await screen.findByText('Not Ready')).toBeVisible();
    expect(
      screen.getByText('(Click Deploy to set up this node pool)')
    ).toBeVisible();
    expect(screen.getByRole('button', { name: 'Deploy' })).toBeEnabled();
    expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(1);
    expect(getSSHNodePoolStatus).toHaveBeenCalledWith('gpu-pool');
  });

  it('keeps the new pool loading when an old status request resolves', async () => {
    const oldStatus = deferred();
    const newStatus = deferred();
    getSSHNodePoolStatus.mockImplementation((poolName) =>
      poolName === 'old-pool' ? oldStatus.promise : newStatus.promise
    );

    const { rerender } = render(sshNodePoolDetails({ poolName: 'old-pool' }));

    await waitFor(() => expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(1));
    rerender(sshNodePoolDetails({ poolName: 'new-pool' }));
    await waitFor(() => expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(2));

    await act(async () => {
      oldStatus.resolve({
        pool_name: 'old-pool',
        status: 'Ready',
      });
      await Promise.resolve();
    });

    expect(screen.getByText('Loading...')).toBeVisible();
    expect(screen.queryByText('Ready')).not.toBeInTheDocument();

    await act(async () => {
      newStatus.resolve({
        pool_name: 'new-pool',
        status: 'Not Ready',
      });
      await Promise.resolve();
    });
    expect(screen.getByText('Not Ready')).toBeVisible();
  });

  it('ignores an old pool error after the new status is visible', async () => {
    const oldStatus = deferred();
    const newStatus = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    getSSHNodePoolStatus.mockImplementation((poolName) =>
      poolName === 'old-pool' ? oldStatus.promise : newStatus.promise
    );

    const { rerender } = render(sshNodePoolDetails({ poolName: 'old-pool' }));
    await waitFor(() => expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(1));
    rerender(sshNodePoolDetails({ poolName: 'new-pool' }));
    await waitFor(() => expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(2));

    await act(async () => {
      newStatus.resolve({
        pool_name: 'new-pool',
        status: 'Ready',
      });
      await Promise.resolve();
    });
    expect(screen.getByText('Ready')).toBeVisible();

    await act(async () => {
      oldStatus.reject(new Error('old pool unavailable'));
      await Promise.resolve();
    });

    expect(screen.getByText('Ready')).toBeVisible();
    expect(screen.queryByText('Error')).not.toBeInTheDocument();
    consoleError.mockRestore();
  });

  it('streams deployment logs and reports successful completion', async () => {
    const handleDeploySSHPool = jest
      .fn()
      .mockResolvedValue({ request_id: 'request-123' });
    streamSSHDeploymentLogs.mockImplementation(
      async ({ requestId, onNewLog }) => {
        expect(requestId).toBe('request-123');
        onNewLog('\u001b[32mInstalling\u001b[0m\n');
        onNewLog('D 07-15 12:34:56 hidden debug line\n└── ready\n');
      }
    );

    render(sshNodePoolDetails({ handleDeploySSHPool }));

    await screen.findByText('Not Ready');
    fireEvent.click(screen.getByRole('button', { name: 'Deploy' }));
    const confirmDialog = screen.getByRole('dialog');
    fireEvent.click(
      within(confirmDialog).getByRole('button', { name: 'Deploy' })
    );

    expect(
      await screen.findByText('Deployment completed successfully!')
    ).toBeVisible();
    expect(handleDeploySSHPool).toHaveBeenCalledWith('gpu-pool');
    expect(streamSSHDeploymentLogs).toHaveBeenCalledTimes(1);
    expect(screen.getByText(/Installing/)).toHaveTextContent('Installing');
    expect(screen.getByText(/└─ ready/)).toBeVisible();
    expect(screen.queryByText(/hidden debug line/)).not.toBeInTheDocument();
  });

  it('refreshes status once after a completed deployment', async () => {
    jest.useFakeTimers();
    const handleDeploySSHPool = jest
      .fn()
      .mockResolvedValue({ request_id: 'request-123' });
    streamSSHDeploymentLogs.mockResolvedValue(undefined);

    render(sshNodePoolDetails({ handleDeploySSHPool }));
    await completeSSHNodePoolDeployment();
    fireEvent.click(
      within(screen.getByRole('dialog')).getAllByRole('button', {
        name: 'Close',
      })[0]
    );

    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });

    expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(2);
    expect(getSSHNodePoolStatus).toHaveBeenLastCalledWith('gpu-pool');
  });

  it('cancels a pending deployment refresh when the pool changes', async () => {
    jest.useFakeTimers();
    const handleDeploySSHPool = jest
      .fn()
      .mockResolvedValue({ request_id: 'request-123' });
    streamSSHDeploymentLogs.mockResolvedValue(undefined);

    const { rerender } = render(sshNodePoolDetails({ handleDeploySSHPool }));
    await completeSSHNodePoolDeployment();

    rerender(
      sshNodePoolDetails({
        poolName: 'next-pool',
        handleDeploySSHPool,
      })
    );
    await waitFor(() => expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(2));

    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });

    expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(2);
    expect(getSSHNodePoolStatus.mock.calls).toEqual([
      ['gpu-pool'],
      ['next-pool'],
    ]);
  });

  it('cancels a pending deployment refresh when the view unmounts', async () => {
    jest.useFakeTimers();
    const handleDeploySSHPool = jest
      .fn()
      .mockResolvedValue({ request_id: 'request-123' });
    streamSSHDeploymentLogs.mockResolvedValue(undefined);

    const { unmount } = render(sshNodePoolDetails({ handleDeploySSHPool }));
    await completeSSHNodePoolDeployment();

    unmount();
    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });

    expect(getSSHNodePoolStatus).toHaveBeenCalledTimes(1);
  });

  it('tears down the pool before deleting its configuration', async () => {
    const handleDeleteSSHPool = jest.fn().mockResolvedValue(undefined);
    const teardownStream = deferred();
    sshDownNodePool.mockResolvedValue({ request_id: 'down-123' });
    streamSSHOperationLogs.mockImplementation(
      async ({ requestId, operationType, onNewLog }) => {
        expect(requestId).toBe('down-123');
        expect(operationType).toBe('down');
        onNewLog('Stopped workers\n');
        await teardownStream.promise;
      }
    );

    render(sshNodePoolDetails({ handleDeleteSSHPool }));

    await screen.findByText('Not Ready');
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    const confirmDialog = screen.getByRole('dialog');
    fireEvent.click(
      within(confirmDialog).getByRole('button', { name: 'Delete' })
    );

    await waitFor(() =>
      expect(streamSSHOperationLogs).toHaveBeenCalledTimes(1)
    );
    expect(handleDeleteSSHPool).not.toHaveBeenCalled();

    await act(async () => {
      teardownStream.resolve();
      await teardownStream.promise;
    });

    expect(
      await screen.findByText('Deployment completed successfully!')
    ).toBeVisible();
    expect(sshDownNodePool).toHaveBeenCalledWith('gpu-pool');
    expect(streamSSHOperationLogs).toHaveBeenCalledTimes(1);
    expect(handleDeleteSSHPool).toHaveBeenCalledWith('gpu-pool');
    expect(
      screen.getByText(/SSH Node Pool teardown completed successfully/)
    ).toBeVisible();
  });
});
