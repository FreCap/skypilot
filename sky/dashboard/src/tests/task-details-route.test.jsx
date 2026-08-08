import { fireEvent, render, screen, waitFor } from '@testing-library/react';

const mockUseSingleManagedJob = jest.fn();
const mockUseManagedJobPools = jest.fn();
const mockUseLogStreamer = jest.fn();
const mockRefreshJobData = jest.fn();

const routerState = {
  isReady: true,
  query: { job: '1', task: '0' },
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

jest.mock('@/data/connectors/jobs', () => ({
  useSingleManagedJob: (...args) => mockUseSingleManagedJob(...args),
  useManagedJobPools: (...args) => mockUseManagedJobPools(...args),
  streamManagedJobLogs: jest.fn(),
  downloadManagedJobLogs: jest.fn(),
}));

jest.mock('@/components/utils', () => ({
  CustomTooltip: ({ children }) => children,
  NonCapitalizedTooltip: ({ children }) => children,
  LogFilter: () => <div data-testid="log-filter" />,
  formatFullTimestamp: () => 'formatted-time',
  formatDuration: () => 'duration',
  renderPoolLink: () => <span>pool-link</span>,
}));

jest.mock('@/components/elements/StatusBadge', () => ({
  StatusBadge: ({ status }) => <span>{status}</span>,
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/components/elements/UserDisplay', () => ({
  UserDisplay: ({ username }) => <span>{username}</span>,
}));

jest.mock('@/hooks/useLogStreamer', () => ({
  useLogStreamer: (...args) => mockUseLogStreamer(...args),
}));

jest.mock('@/utils/grafana', () => ({
  checkGrafanaAvailability: jest.fn(() => new Promise(() => {})),
}));

jest.mock('@/components/TelemetrySection', () => ({
  TelemetrySection: () => <div data-testid="telemetry" />,
}));

jest.mock('@/utils/gpuUtils', () => ({
  hasAccelerator: () => false,
}));

jest.mock('@/lib/analytics', () => ({
  trackJobAction: jest.fn(),
}));

import TaskDetails from '@/pages/jobs/[job]/[task]';
import { checkGrafanaAvailability } from '@/utils/grafana';

function task(taskIndex) {
  return {
    id: '1',
    task: `task-${taskIndex}`,
    name: 'example-job',
    status: 'RUNNING',
    user: 'alice',
    user_hash: 'hash',
    workspace: 'default',
    job_duration: 10,
    requested_resources: '1xA100',
    resources_str: '1xA100',
    infra: 'AWS',
    full_infra: 'AWS',
    recoveries: 0,
    pool: null,
  };
}

describe('TaskDetails route ownership rendering', () => {
  let currentLogLoading = false;

  beforeEach(() => {
    jest.resetAllMocks();
    routerState.isReady = true;
    routerState.query = { job: '1', task: '0' };
    currentLogLoading = false;
    checkGrafanaAvailability.mockImplementation(() => new Promise(() => {}));

    mockUseSingleManagedJob.mockReturnValue({
      jobData: { jobs: [task(0), task(1)] },
      loading: false,
      refreshJobData: mockRefreshJobData,
    });
    mockUseManagedJobPools.mockReturnValue([]);
    mockUseLogStreamer.mockImplementation(() => ({
      lines: [],
      isLoading: currentLogLoading,
      hasReceivedFirstChunk: false,
    }));
  });

  it('re-expands the logs section when the route target changes', () => {
    const view = render(<TaskDetails />);

    expect(screen.getByTestId('log-filter')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /logs/i }));
    expect(screen.queryByTestId('log-filter')).not.toBeInTheDocument();

    routerState.query = { job: '1', task: '1' };
    view.rerender(<TaskDetails />);

    expect(screen.getByTestId('log-filter')).toBeInTheDocument();
    expect(screen.getByText('(Task 1 logs)')).toBeInTheDocument();
  });

  it('drops stale log-loading state on the first render of a new task route', async () => {
    currentLogLoading = true;
    const view = render(<TaskDetails />);

    await waitFor(() =>
      expect(screen.getByText('Loading logs...')).toBeInTheDocument()
    );

    currentLogLoading = false;
    routerState.query = { job: '1', task: '1' };
    view.rerender(<TaskDetails />);

    expect(screen.queryByText('Loading logs...')).not.toBeInTheDocument();
    expect(screen.getByText('(Task 1 logs)')).toBeInTheDocument();
  });

  it('does not trigger a managed-job refresh when switching tasks in the same job', () => {
    const view = render(<TaskDetails />);

    routerState.query = { job: '1', task: '1' };
    view.rerender(<TaskDetails />);

    expect(mockRefreshJobData).not.toHaveBeenCalled();
    expect(mockUseSingleManagedJob).toHaveBeenCalledTimes(2);
  });
});
