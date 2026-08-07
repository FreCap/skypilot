import { act, fireEvent, render, screen } from '@testing-library/react';

const mockUseClusterDetails = jest.fn();
const mockRefreshData = jest.fn();

const routerState = {
  isReady: true,
  query: { cluster: 'cluster-a', job: '1' },
};

jest.mock('next/router', () => ({
  useRouter: () => routerState,
}));

jest.mock('next/head', () => ({
  __esModule: true,
  default: ({ children }) => <>{children}</>,
}));

jest.mock('next/link', () => ({
  __esModule: true,
  default: ({ children, href }) => <a href={href}>{children}</a>,
}));

jest.mock('@mui/material', () => ({
  CircularProgress: () => <div data-testid="spinner" />,
}));

jest.mock('@/components/ui/card', () => ({
  Card: ({ children }) => <div>{children}</div>,
}));

jest.mock('@/data/connectors/clusters', () => ({
  useClusterDetails: (...args) => mockUseClusterDetails(...args),
  streamClusterJobLogs: jest.fn(),
  downloadJobLogs: jest.fn(),
}));

jest.mock('@/components/utils', () => ({
  CustomTooltip: ({ children }) => children,
  formatFullTimestamp: () => 'formatted-time',
  LogFilter: () => <div data-testid="log-filter" />,
}));

jest.mock('@/components/elements/StatusBadge', () => ({
  StatusBadge: ({ status }) => <span>{status}</span>,
}));

jest.mock('@/components/elements/UserDisplay', () => ({
  UserDisplay: ({ username }) => <span>{username}</span>,
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/hooks/useLogStreamer', () => ({
  useLogStreamer: () => ({
    lines: [],
    isLoading: false,
  }),
}));

jest.mock('@/utils/externalLinks', () => ({
  normalizeUrl: (url) => url,
  useLogLinkExtractor: () => ({
    extractedLinks: {},
    scanLines: jest.fn(),
  }),
}));

import { JobDetailPage } from '@/pages/clusters/[cluster]/[job]';

function clusterData(cluster) {
  return {
    cluster,
    workspace: `${cluster}-workspace`,
    infra: 'AWS',
    user: 'alice',
    user_hash: 'hash',
  };
}

function clusterJob(id, overrides = {}) {
  return {
    id,
    job: `job-${id}`,
    status: 'RUNNING',
    user: 'alice',
    user_hash: 'hash',
    total_duration: 1,
    resources: '1xA100',
    cluster: 'cluster-a',
    workspace: 'cluster-a-workspace',
    ...overrides,
  };
}

describe('JobDetailPage initial-load refresh ownership', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    routerState.isReady = true;
    routerState.query = { cluster: 'cluster-a', job: '1' };
    mockRefreshData.mockResolvedValue(undefined);
  });

  it('keeps refresh disabled while the dependent job read is still loading', () => {
    mockUseClusterDetails.mockReturnValue({
      clusterData: clusterData('cluster-a'),
      clusterJobData: null,
      loading: false,
      clusterJobsLoading: true,
      refreshData: mockRefreshData,
    });

    render(<JobDetailPage />);

    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    expect(refreshButton).toBeDisabled();
    fireEvent.click(refreshButton);
    expect(mockRefreshData).not.toHaveBeenCalled();
    expect(screen.getAllByText('Loading...').length).toBeGreaterThan(0);
  });

  it('re-enables refresh after the current route owns job data', async () => {
    mockUseClusterDetails.mockReturnValue({
      clusterData: clusterData('cluster-a'),
      clusterJobData: [clusterJob('1')],
      loading: false,
      clusterJobsLoading: false,
      refreshData: mockRefreshData,
    });

    render(<JobDetailPage />);

    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    expect(refreshButton).toBeEnabled();

    await act(async () => {
      fireEvent.click(refreshButton);
      await Promise.resolve();
    });

    expect(mockRefreshData).toHaveBeenCalledTimes(1);
  });
});
