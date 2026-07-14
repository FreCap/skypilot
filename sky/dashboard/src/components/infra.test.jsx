import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';

import { ContextDetails } from '@/components/infra';
import { ContextDetails as ExtractedContextDetails } from '@/components/infra-context-details';
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
