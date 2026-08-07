import { act, render, screen, waitFor } from '@testing-library/react';

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
    setPreloader: jest.fn(),
    getCached: jest.fn(),
    clear: jest.fn(),
  },
}));

const routerState = {
  isReady: true,
  query: { cluster: 'cluster-a' },
};

jest.mock('next/router', () => ({
  useRouter: () => routerState,
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/components/jobs', () => ({
  ClusterJobs: () => <div data-testid="cluster-jobs" />,
}));

jest.mock('@/components/cluster-actions', () => ({
  Status2Actions: () => <div data-testid="cluster-actions" />,
}));

jest.mock('@/data/connectors/clusters', () => {
  const actual = jest.requireActual('@/data/connectors/clusters');
  return {
    ...actual,
    streamClusterProvisionLogs: jest.fn().mockResolvedValue(undefined),
    streamClusterJobLogs: jest.fn().mockResolvedValue(undefined),
  };
});

jest.mock('@/components/utils', () => ({
  CustomTooltip: ({ children }) => children,
  NonCapitalizedTooltip: ({ children }) => children,
  formatFullTimestamp: () => 'formatted-time',
  formatAutostop: () => 'autostop',
  LogFilter: () => <div data-testid="log-filter" />,
}));

jest.mock('@/utils/grafana', () => ({
  checkGrafanaAvailability: jest.fn().mockResolvedValue(false),
}));

jest.mock('@/utils/externalLinks', () => ({
  extractLinksFromLogs: jest.fn(() => ({})),
  normalizeUrl: (url) => url,
  useCustomUrlPatterns: () => [],
  useLogLinkExtractor: () => ({
    extractedLinks: {},
    scanLines: jest.fn(),
  }),
}));

jest.mock('@/components/elements/modals', () => ({
  SSHInstructionsModal: () => null,
  VSCodeInstructionsModal: () => null,
}));

jest.mock('@/components/elements/UserDisplay', () => ({
  UserDisplay: () => <span>user</span>,
}));

jest.mock('@/components/ui/yaml-code-block', () => ({
  YamlCodeBlock: () => <div data-testid="yaml-code-block" />,
}));

jest.mock('@/plugins/PluginSlot', () => ({
  PluginSlot: ({ fallback = null }) => fallback,
}));

jest.mock('@/components/TelemetrySection', () => ({
  TelemetrySection: () => <div data-testid="telemetry" />,
}));

jest.mock('@/components/cluster-operational-events', () => ({
  ClusterOperationalEvents: () => <div data-testid="cluster-events" />,
}));

jest.mock('@/hooks/useLogStreamer', () => ({
  useLogStreamer: () => ({
    lines: [],
    isLoading: false,
  }),
}));

import dashboardCache from '@/lib/cache';
import ClusterDetails from '@/pages/clusters/[cluster]';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('ClusterDetails route ownership rendering', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    routerState.isReady = true;
    routerState.query = { cluster: 'cluster-a' };
  });

  it('shows route loading instead of flashing not found while a new cluster route is in flight', async () => {
    const nextCluster = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([
        {
          cluster: 'cluster-a',
          status: 'UP',
          workspace: 'workspace-a',
          resources_str: '1 CPU',
          autostop: 0,
          to_down: false,
        },
      ])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockImplementationOnce(() => nextCluster.promise)
      .mockResolvedValueOnce([{ id: 2, cluster: 'cluster-b' }]);

    const { rerender } = render(<ClusterDetails />);

    await waitFor(() =>
      expect(screen.getAllByText('cluster-a')).not.toHaveLength(0)
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    routerState.query = { cluster: 'cluster-b' };
    rerender(<ClusterDetails />);

    expect(screen.getByText('Loading cluster details...')).toBeInTheDocument();
    expect(
      screen.queryByText('Cluster not found in active clusters or history.')
    ).not.toBeInTheDocument();
    expect(screen.queryByText('cluster-a')).not.toBeInTheDocument();

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    await act(async () => {
      nextCluster.resolve([
        {
          cluster: 'cluster-b',
          status: 'STOPPED',
          workspace: 'workspace-b',
          resources_str: '2 CPU',
          autostop: 0,
          to_down: false,
        },
      ]);
      await nextCluster.promise;
    });

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));
    await waitFor(() =>
      expect(screen.getAllByText('cluster-b')).not.toHaveLength(0)
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
  });
});
