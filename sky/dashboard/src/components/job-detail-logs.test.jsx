import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';

import JobDetails from '@/pages/jobs/[job]';
import TaskDetails from '@/pages/jobs/[job]/[task]';
import ClusterJobDetails from '@/pages/clusters/[cluster]/[job]';
import {
  computeJobGroupStatus,
  downloadManagedJobLogs,
  streamManagedJobLogs,
  useManagedJobPools,
  useSingleManagedJob,
} from '@/data/connectors/jobs';
import {
  streamClusterJobLogs,
  useClusterDetails,
} from '@/data/connectors/clusters';
import { useLogStreamer } from '@/hooks/useLogStreamer';
import { PluginSlot } from '@/plugins/PluginSlot';
import { usePluginComponents } from '@/plugins/PluginProvider';
import { useLogLinkExtractor } from '@/utils/externalLinks';
import { checkGrafanaAvailability } from '@/utils/grafana';
import { TelemetrySection } from '@/components/TelemetrySection';

const router = {
  isReady: true,
  query: { job: '42' },
};

jest.mock('next/router', () => ({
  useRouter: () => router,
}));

jest.mock('@/data/connectors/jobs', () => ({
  useSingleManagedJob: jest.fn(),
  getPoolStatus: jest.fn(),
  computeJobGroupStatus: jest.fn((tasks) => tasks[0]?.status),
  streamManagedJobLogs: jest.fn(),
  downloadManagedJobLogs: jest.fn(),
  useManagedJobPools: jest.fn(() => []),
}));

jest.mock('@/data/connectors/clusters', () => ({
  downloadJobLogs: jest.fn(),
  streamClusterJobLogs: jest.fn(),
  useClusterDetails: jest.fn(),
}));

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn().mockResolvedValue({ pools: [] }),
  },
}));

jest.mock('@/hooks/useLogStreamer', () => ({
  useLogStreamer: jest.fn(),
}));

jest.mock('@/plugins/PluginProvider', () => ({
  usePluginComponents: jest.fn(() => []),
}));

jest.mock('@/plugins/PluginSlot', () => ({
  PluginSlot: jest.fn(({ fallback = null }) => fallback),
}));

jest.mock('@/components/ui/select', () => ({
  Select: ({ value, onValueChange, children }) => (
    <select
      value={value}
      onChange={(event) => onValueChange?.(event.target.value)}
    >
      {children}
    </select>
  ),
  SelectContent: ({ children }) => <>{children}</>,
  SelectItem: ({ children, value, disabled = false }) => (
    <option value={value} disabled={disabled}>
      {children}
    </option>
  ),
  SelectTrigger: () => null,
  SelectValue: () => null,
}));

jest.mock('@/components/elements/UserDisplay', () => ({
  UserDisplay: () => null,
}));

jest.mock('@/utils/grafana', () => ({
  checkGrafanaAvailability: jest.fn().mockResolvedValue(false),
}));

jest.mock('@/components/TelemetrySection', () => ({
  TelemetrySection: jest.fn(({ displayName, headerExtra = null }) => (
    <div data-testid="telemetry-section">
      <span>{displayName}</span>
      {headerExtra}
    </div>
  )),
}));

const mockScanLines = jest.fn();
jest.mock('@/utils/externalLinks', () => ({
  normalizeUrl: jest.fn((url) => url),
  useLogLinkExtractor: jest.fn(() => ({
    extractedLinks: {},
    scanLines: mockScanLines,
  })),
}));

const job = {
  id: 42,
  name: 'training',
  status: 'RUNNING',
  schedule_state: 'ALIVE',
  user: 'alice',
  workspace: 'default',
  requested_resources: '1x A100',
  resources_str: '1x A100',
  infra: 'AWS (us-east-1)',
  full_infra: 'AWS (us-east-1)',
  cloud: 'AWS',
  links: {},
};

const workerLines = ['worker output'];
const controllerLines = ['controller output'];

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function enabledStreamCall(controller) {
  return useLogStreamer.mock.calls.find(
    ([options]) =>
      options.enabled === true && options.streamArgs.controller === controller
  )?.[0];
}

function latestEnabledStreamCall(controller) {
  return [...useLogStreamer.mock.calls]
    .reverse()
    .find(
      ([options]) =>
        options.enabled === true && options.streamArgs.controller === controller
    )?.[0];
}

function latestClusterStreamCall() {
  return [...useLogStreamer.mock.calls]
    .reverse()
    .find(([options]) => options.streamFn === streamClusterJobLogs)?.[0];
}

beforeEach(() => {
  jest.clearAllMocks();
  router.query = { job: '42' };
  window.localStorage.clear();
  global.requestAnimationFrame = (callback) => callback();
  usePluginComponents.mockImplementation(() => []);
  useLogLinkExtractor.mockReturnValue({
    extractedLinks: {},
    scanLines: mockScanLines,
  });
  useSingleManagedJob.mockReturnValue({
    jobData: { jobs: [job] },
    loading: false,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  });
  useManagedJobPools.mockReturnValue([]);
  useClusterDetails.mockReturnValue({
    clusterData: null,
    clusterJobData: null,
    loading: true,
    clusterJobsLoading: true,
    refreshData: jest.fn().mockResolvedValue(undefined),
  });
  useLogStreamer.mockImplementation(({ streamArgs }) => ({
    lines: streamArgs.controller ? controllerLines : workerLines,
    isLoading: false,
    hasReceivedFirstChunk: true,
  }));
});

it('streams one confirmed runnable cluster job but not a pending job', () => {
  router.query = { cluster: 'cluster-a', job: '7' };
  const refreshData = jest.fn().mockResolvedValue(undefined);
  useClusterDetails.mockReturnValue({
    clusterData: { cluster: 'cluster-a', workspace: 'workspace-a' },
    clusterJobData: [{ id: 7, status: 'PENDING', job: 'queued' }],
    loading: false,
    clusterJobsLoading: false,
    refreshData,
  });

  const view = render(<ClusterJobDetails />);
  expect(latestClusterStreamCall().enabled).toBe(false);

  useClusterDetails.mockReturnValue({
    clusterData: { cluster: 'cluster-a', workspace: 'workspace-a' },
    clusterJobData: [{ id: 7, status: 'RUNNING', job: 'training' }],
    loading: false,
    clusterJobsLoading: false,
    refreshData,
  });
  view.rerender(<ClusterJobDetails />);

  expect(latestClusterStreamCall()).toMatchObject({
    streamFn: streamClusterJobLogs,
    streamArgs: {
      clusterName: 'cluster-a',
      jobId: '7',
      workspace: 'workspace-a',
    },
    enabled: true,
  });
});

it('does not start a mismatched stream across cluster-job route changes', () => {
  router.query = { cluster: 'cluster-a', job: '7' };
  useClusterDetails.mockReturnValue({
    clusterData: { cluster: 'cluster-a', workspace: 'workspace-a' },
    clusterJobData: [{ id: 7, status: 'RUNNING', job: 'training' }],
    loading: false,
    clusterJobsLoading: false,
    refreshData: jest.fn().mockResolvedValue(undefined),
  });
  const view = render(<ClusterJobDetails />);
  expect(latestClusterStreamCall().enabled).toBe(true);

  router.query = { cluster: 'cluster-b', job: '8' };
  useClusterDetails.mockReturnValue({
    clusterData: { cluster: 'cluster-b', workspace: 'workspace-b' },
    clusterJobData: [],
    loading: false,
    clusterJobsLoading: false,
    refreshData: jest.fn().mockResolvedValue(undefined),
  });
  view.rerender(<ClusterJobDetails />);

  expect(latestClusterStreamCall()).toMatchObject({
    streamArgs: {
      clusterName: 'cluster-b',
      jobId: '8',
      workspace: 'workspace-b',
    },
    enabled: false,
  });
  expect(
    useLogStreamer.mock.calls.some(
      ([options]) =>
        options.enabled === true &&
        options.streamArgs.clusterName === 'cluster-b'
    )
  ).toBe(false);
});

it('uses the current job rows for its pool-link snapshot', async () => {
  render(<JobDetails />);

  await screen.findByText('Managed Jobs');
  expect(useManagedJobPools).toHaveBeenCalledWith([job], '42');
});

it('uses the current task job rows for its pool-link snapshot', async () => {
  router.query = { job: '42', task: '0' };
  render(<TaskDetails />);

  await screen.findByText('Task 0');
  expect(useManagedJobPools).toHaveBeenCalledWith([job], '42');
});

it('owns the job-detail refresh button until the data refresh settles', async () => {
  const refresh = deferred();
  const refreshJobData = jest.fn(() => refresh.promise);
  useSingleManagedJob.mockReturnValue({
    jobData: { jobs: [job] },
    loading: false,
    refreshJobData,
  });
  render(<JobDetails />);

  const refreshButton = screen.getAllByRole('button', { name: 'Refresh' })[0];
  fireEvent.click(refreshButton);

  expect(refreshJobData).toHaveBeenCalledTimes(1);
  expect(refreshButton).toBeDisabled();
  expect(screen.getByText('Loading...')).toBeInTheDocument();
  await waitFor(() =>
    expect(latestEnabledStreamCall(false).refreshTrigger).toBe(1)
  );

  refresh.resolve();
  await waitFor(() => expect(refreshButton).toBeEnabled());
});

it('owns the task-detail refresh button until the data refresh settles', async () => {
  router.query = { job: '42', task: '0' };
  const refresh = deferred();
  const refreshJobData = jest.fn(() => refresh.promise);
  useSingleManagedJob.mockReturnValue({
    jobData: { jobs: [job] },
    loading: false,
    refreshJobData,
  });
  render(<TaskDetails />);

  const refreshButton = screen.getAllByRole('button', { name: 'Refresh' })[0];
  fireEvent.click(refreshButton);

  expect(refreshJobData).toHaveBeenCalledTimes(1);
  expect(refreshButton).toBeDisabled();
  expect(screen.getByText('Loading...')).toBeInTheDocument();
  await waitFor(() =>
    expect(latestEnabledStreamCall(false).refreshTrigger).toBe(1)
  );

  refresh.resolve();
  await waitFor(() => expect(refreshButton).toBeEnabled());
});

it('keeps the job route in loading instead of not-found while a new route is fetching', async () => {
  const refresh42 = jest.fn().mockResolvedValue(undefined);
  const refresh43 = jest.fn().mockResolvedValue(undefined);
  let job43Loading = true;
  useSingleManagedJob.mockImplementation((jobId) => {
    if (String(jobId) === '42') {
      return {
        jobData: { jobs: [job] },
        loading: false,
        refreshJobData: refresh42,
      };
    }
    return {
      jobData: null,
      loading: job43Loading,
      refreshJobData: refresh43,
    };
  });

  const { rerender } = render(<JobDetails />);
  await screen.findByText('Managed Jobs');

  router.query = { job: '43' };
  rerender(<JobDetails />);

  expect(screen.queryByText('Job not found')).not.toBeInTheDocument();
  expect(screen.getAllByRole('progressbar')).toHaveLength(2);
  expect(refresh42).not.toHaveBeenCalled();
  expect(refresh43).not.toHaveBeenCalled();

  job43Loading = false;
  rerender(<JobDetails />);
  await screen.findByText('Job not found');
});

it('keeps the task route in loading instead of not-found while a new route is fetching', async () => {
  const refresh42 = jest.fn().mockResolvedValue(undefined);
  const refresh43 = jest.fn().mockResolvedValue(undefined);
  let job43Loading = true;
  useSingleManagedJob.mockImplementation((jobId) => {
    if (String(jobId) === '42') {
      return {
        jobData: { jobs: [job] },
        loading: false,
        refreshJobData: refresh42,
      };
    }
    return {
      jobData: null,
      loading: job43Loading,
      refreshJobData: refresh43,
    };
  });

  router.query = { job: '42', task: '0' };
  const { rerender } = render(<TaskDetails />);
  await screen.findByText('Task 0');

  router.query = { job: '43', task: '0' };
  rerender(<TaskDetails />);

  expect(screen.queryByText('Task not found')).not.toBeInTheDocument();
  expect(screen.getAllByRole('progressbar')).toHaveLength(2);
  expect(refresh42).not.toHaveBeenCalled();
  expect(refresh43).not.toHaveBeenCalled();

  job43Loading = false;
  rerender(<TaskDetails />);
  await screen.findByText('Task not found');
});

it('keeps matching job detail visible during background loading', async () => {
  useSingleManagedJob.mockReturnValue({
    jobData: { jobs: [job] },
    loading: true,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  });

  render(<JobDetails />);

  expect(await screen.findByText('Job ID (Name)')).toBeInTheDocument();
  expect(screen.queryByText('Job not found')).not.toBeInTheDocument();
  expect(screen.getAllByRole('progressbar')).toHaveLength(1);
});

it('keeps matching task detail visible during background loading', async () => {
  router.query = { job: '42', task: '0' };
  useSingleManagedJob.mockReturnValue({
    jobData: { jobs: [job] },
    loading: true,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  });

  render(<TaskDetails />);

  expect(await screen.findByText('Task Details')).toBeInTheDocument();
  expect(screen.queryByText('Task not found')).not.toBeInTheDocument();
  expect(screen.getAllByRole('progressbar')).toHaveLength(1);
});

it('settles an invalid task index to not-found', async () => {
  router.query = { job: '42', task: '1' };

  render(<TaskDetails />);

  expect(await screen.findByText('Task not found')).toBeInTheDocument();
});

it('advances job telemetry once for an accepted manual refresh', async () => {
  const refresh = deferred();
  const refreshJobData = jest.fn(() => refresh.promise);
  const kubeJob = {
    ...job,
    full_infra: 'Kubernetes (context-a)',
    cluster_name_on_cloud: 'job-42',
  };
  checkGrafanaAvailability.mockResolvedValue(true);
  useSingleManagedJob.mockReturnValue({
    jobData: { jobs: [kubeJob] },
    loading: false,
    refreshJobData,
  });
  render(<JobDetails />);

  await waitFor(() => expect(TelemetrySection).toHaveBeenCalled());
  expect(TelemetrySection.mock.calls.at(-1)[0].refreshTrigger).toBe(0);

  const refreshButton = screen.getAllByRole('button', { name: 'Refresh' })[0];
  fireEvent.click(refreshButton);

  await waitFor(() =>
    expect(TelemetrySection.mock.calls.at(-1)[0].refreshTrigger).toBe(1)
  );
  fireEvent.click(refreshButton);
  expect(refreshJobData).toHaveBeenCalledTimes(1);
  expect(TelemetrySection.mock.calls.at(-1)[0].refreshTrigger).toBe(1);

  refresh.resolve();
  await waitFor(() => expect(refreshButton).toBeEnabled());
});

it('preserves the managed-job log stream and plugin contract', async () => {
  render(<JobDetails />);

  expect(await screen.findByText('worker output')).toBeInTheDocument();

  // The metadata renderer no longer owns two disabled stream configurations.
  // Count memoized argument identities so React rerenders do not inflate the
  // number of live stream owners.
  const streamOwners = new Set(
    useLogStreamer.mock.calls.map(([options]) => options.streamArgs)
  );
  expect(streamOwners.size).toBe(2);
  const streamCall = enabledStreamCall(false);
  expect(streamCall).toMatchObject({
    streamFn: streamManagedJobLogs,
    streamArgs: { jobId: 42, controller: false, task: null },
    refreshTrigger: 0,
  });
  expect(
    PluginSlot.mock.calls.find(
      ([props]) => props.name === 'jobs.detail.logs'
    )?.[0].context
  ).toMatchObject({
    jobId: 42,
    taskId: null,
    status: 'RUNNING',
    selectedNode: 'all',
    isController: false,
    refreshTrigger: 0,
  });
  expect(mockScanLines).toHaveBeenCalledWith(workerLines);
});

it('resets route-owned job log, telemetry, and extracted-link state', async () => {
  const makeTask = (jobId, index, nodeSuffix) => ({
    ...job,
    id: Number(jobId),
    task_job_id: Number(jobId) * 10 + index,
    task: `task-${jobId}-${index}`,
    name: `job-${jobId}`,
    full_infra: 'Kubernetes (context-a)',
    cluster_name_on_cloud: `cluster-${jobId}-${index}`,
    node_names: ['head', `worker${nodeSuffix}`],
  });
  const jobsById = {
    42: [
      makeTask('42', 0, '1'),
      makeTask('42', 1, '1'),
      makeTask('42', 2, '1'),
    ],
    43: [
      makeTask('43', 0, '2'),
      makeTask('43', 1, '2'),
      makeTask('43', 2, '2'),
    ],
  };
  const logsByJobId = {
    42: ['(head, 0) job-42', '(worker1, 0) task-2'],
    43: ['(head, 0) job-43', '(worker2, 0) task-0'],
  };
  const extractedLinksByJobId = {
    42: { 'W&B Run': 'https://wandb.ai/acme/project/runs/run-42' },
    43: {},
  };
  checkGrafanaAvailability.mockResolvedValue(true);
  useSingleManagedJob.mockImplementation((jobId) => ({
    jobData: { jobs: jobsById[String(jobId)] },
    loading: false,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  }));
  useLogStreamer.mockImplementation(({ streamArgs }) => ({
    lines: streamArgs.controller
      ? controllerLines
      : logsByJobId[String(streamArgs.jobId)],
    isLoading: false,
    hasReceivedFirstChunk: true,
  }));
  useLogLinkExtractor.mockImplementation(() => ({
    extractedLinks: extractedLinksByJobId[String(router.query.job)],
    scanLines: mockScanLines,
  }));

  const { rerender } = render(<JobDetails />);
  await screen.findByRole('link', { name: 'W&B Run' });
  await waitFor(() =>
    expect(screen.getByTestId('telemetry-section')).toBeInTheDocument()
  );

  let logsSection = document.querySelector('#logs-section');
  let [taskSelect, nodeSelect] = within(logsSection).getAllByRole('combobox');
  fireEvent.change(taskSelect, { target: { value: '2' } });
  fireEvent.change(nodeSelect, { target: { value: 'worker1' } });
  fireEvent.change(
    within(screen.getByTestId('telemetry-section')).getByRole('combobox'),
    {
      target: { value: '2' },
    }
  );

  await waitFor(() =>
    expect(latestEnabledStreamCall(false)).toMatchObject({
      streamArgs: { jobId: 42, controller: false, task: 2 },
    })
  );

  router.query = { job: '43' };
  rerender(<JobDetails />);

  logsSection = document.querySelector('#logs-section');
  [taskSelect, nodeSelect] = within(logsSection).getAllByRole('combobox');
  expect(taskSelect).toHaveValue('0');
  expect(nodeSelect).toHaveValue('all');
  expect(
    within(screen.getByTestId('telemetry-section')).getByRole('combobox')
  ).toHaveValue('0');
  expect(
    screen.queryByRole('link', { name: 'W&B Run' })
  ).not.toBeInTheDocument();
  expect(
    screen.queryByRole('option', { name: 'Task 2: task-42-2' })
  ).not.toBeInTheDocument();
  expect(
    screen.queryByRole('option', { name: 'worker1' })
  ).not.toBeInTheDocument();

  await waitFor(() =>
    expect(latestEnabledStreamCall(false)).toMatchObject({
      streamArgs: { jobId: 43, controller: false, task: 0 },
    })
  );
  expect(
    useLogStreamer.mock.calls.some(
      ([options]) =>
        options.enabled === true &&
        options.streamArgs.jobId === 43 &&
        options.streamArgs.task === 2
    )
  ).toBe(false);
});

it('clamps a stale task selection before a shorter destination route owns logs', async () => {
  const makeTask = (jobId, index) => ({
    ...job,
    id: Number(jobId),
    task_job_id: Number(jobId) * 10 + index,
    task: `task-${jobId}-${index}`,
    name: `job-${jobId}`,
  });
  const jobsById = {
    42: [makeTask('42', 0), makeTask('42', 1), makeTask('42', 2)],
    43: [makeTask('43', 0), makeTask('43', 1)],
  };
  const workerRouteLogs = ['(head, 0) log line'];
  useSingleManagedJob.mockImplementation((jobId) => ({
    jobData: { jobs: jobsById[String(jobId)] },
    loading: false,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  }));
  useLogStreamer.mockImplementation(({ streamArgs }) => ({
    lines: streamArgs.controller ? controllerLines : workerRouteLogs,
    isLoading: false,
    hasReceivedFirstChunk: true,
  }));

  const { rerender } = render(<JobDetails />);
  let logsSection = document.querySelector('#logs-section');
  let [taskSelect] = within(logsSection).getAllByRole('combobox');
  fireEvent.change(taskSelect, { target: { value: '2' } });

  await waitFor(() =>
    expect(latestEnabledStreamCall(false)).toMatchObject({
      streamArgs: { jobId: 42, controller: false, task: 2 },
    })
  );

  router.query = { job: '43' };
  rerender(<JobDetails />);

  logsSection = document.querySelector('#logs-section');
  [taskSelect] = within(logsSection).getAllByRole('combobox');
  expect(taskSelect).toHaveValue('0');
  await waitFor(() =>
    expect(latestEnabledStreamCall(false)).toMatchObject({
      streamArgs: { jobId: 43, controller: false, task: 0 },
    })
  );
  expect(screen.queryByText('Job not found')).not.toBeInTheDocument();
});

it('preserves expanded controller-log streaming and plugin context', async () => {
  window.localStorage.setItem('skypilot-controller-logs-expanded', 'true');
  render(<JobDetails />);

  expect(await screen.findByText('controller output')).toBeInTheDocument();

  const streamCall = enabledStreamCall(true);
  expect(streamCall).toMatchObject({
    streamFn: streamManagedJobLogs,
    streamArgs: { jobId: 42, controller: true },
    refreshTrigger: 0,
  });
  await waitFor(() => {
    expect(
      PluginSlot.mock.calls.find(
        ([props]) => props.name === 'jobs.detail.controllerlogs'
      )?.[0].context
    ).toMatchObject({
      jobId: 42,
      status: 'RUNNING',
      isController: true,
      refreshTrigger: 0,
    });
  });
});

it('preserves controller-log expansion and download behavior', async () => {
  downloadManagedJobLogs.mockResolvedValue(undefined);
  render(<JobDetails />);

  const section = document.querySelector('#controller-logs-section');
  const toggle = within(section).getByRole('button', {
    name: /Controller Logs/,
  });
  expect(screen.queryByText('controller output')).not.toBeInTheDocument();

  fireEvent.click(toggle);

  expect(window.localStorage.getItem('skypilot-controller-logs-expanded')).toBe(
    'true'
  );
  expect(await screen.findByText('controller output')).toBeInTheDocument();

  const [, downloadButton] = within(section).getAllByRole('button');
  fireEvent.click(downloadButton);
  await waitFor(() => {
    expect(downloadManagedJobLogs).toHaveBeenCalledWith({
      jobId: 42,
      controller: true,
      jobStatus: 'RUNNING',
    });
  });
});

it('owns duplicate controller-log downloads until the request settles', async () => {
  const firstDownload = deferred();
  downloadManagedJobLogs
    .mockImplementationOnce(() => firstDownload.promise)
    .mockResolvedValueOnce(undefined);
  render(<JobDetails />);

  const section = document.querySelector('#controller-logs-section');
  fireEvent.click(
    within(section).getByRole('button', { name: /Controller Logs/ })
  );
  const [, downloadButton] = within(section).getAllByRole('button');

  act(() => {
    fireEvent.click(downloadButton);
    fireEvent.click(downloadButton);
  });
  expect(downloadManagedJobLogs).toHaveBeenCalledTimes(1);

  firstDownload.resolve();
  await waitFor(() => expect(downloadButton).toBeEnabled());

  fireEvent.click(downloadButton);
  await waitFor(() => expect(downloadManagedJobLogs).toHaveBeenCalledTimes(2));
});

it('releases controller-log download ownership after failure', async () => {
  const failedDownload = deferred();
  downloadManagedJobLogs
    .mockImplementationOnce(() => failedDownload.promise)
    .mockResolvedValueOnce(undefined);
  render(<JobDetails />);

  const section = document.querySelector('#controller-logs-section');
  fireEvent.click(
    within(section).getByRole('button', { name: /Controller Logs/ })
  );
  const [, downloadButton] = within(section).getAllByRole('button');

  fireEvent.click(downloadButton);
  expect(downloadButton).toBeDisabled();

  failedDownload.reject(new Error('archive failed'));
  await waitFor(() => expect(downloadButton).toBeEnabled());

  fireEvent.click(downloadButton);
  await waitFor(() => expect(downloadManagedJobLogs).toHaveBeenCalledTimes(2));
});

it('fences controller-log download cleanup across job routes', async () => {
  const firstDownload = deferred();
  const secondDownload = deferred();
  downloadManagedJobLogs
    .mockImplementationOnce(() => firstDownload.promise)
    .mockImplementationOnce(() => secondDownload.promise);
  useSingleManagedJob.mockImplementation((jobId) => ({
    jobData: {
      jobs: [
        {
          ...job,
          id: Number(jobId),
          name: `training-${jobId}`,
        },
      ],
    },
    loading: false,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  }));

  const { rerender } = render(<JobDetails />);
  let section = document.querySelector('#controller-logs-section');
  fireEvent.click(
    within(section).getByRole('button', { name: /Controller Logs/ })
  );
  let [, downloadButton] = within(section).getAllByRole('button');
  fireEvent.click(downloadButton);
  expect(downloadButton).toBeDisabled();

  router.query = { job: '43' };
  rerender(<JobDetails />);
  section = document.querySelector('#controller-logs-section');
  [, downloadButton] = within(section).getAllByRole('button');
  expect(downloadButton).toBeEnabled();
  fireEvent.click(downloadButton);
  expect(downloadManagedJobLogs).toHaveBeenNthCalledWith(2, {
    jobId: 43,
    controller: true,
    jobStatus: 'RUNNING',
  });
  expect(downloadButton).toBeDisabled();

  firstDownload.resolve();
  await waitFor(() => expect(downloadButton).toBeDisabled());

  secondDownload.resolve();
  await waitFor(() => expect(downloadButton).toBeEnabled());
});

it('retains controller-log ownership across an A-B-A route cycle', async () => {
  const firstJobDownload = deferred();
  const secondJobDownload = deferred();
  downloadManagedJobLogs
    .mockImplementationOnce(() => firstJobDownload.promise)
    .mockImplementationOnce(() => secondJobDownload.promise);
  useSingleManagedJob.mockImplementation((jobId) => ({
    jobData: {
      jobs: [
        {
          ...job,
          id: Number(jobId),
          name: `training-${jobId}`,
        },
      ],
    },
    loading: false,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  }));

  const { rerender } = render(<JobDetails />);
  let section = document.querySelector('#controller-logs-section');
  fireEvent.click(
    within(section).getByRole('button', { name: /Controller Logs/ })
  );
  let [, downloadButton] = within(section).getAllByRole('button');
  fireEvent.click(downloadButton);

  router.query = { job: '43' };
  rerender(<JobDetails />);
  section = document.querySelector('#controller-logs-section');
  [, downloadButton] = within(section).getAllByRole('button');
  expect(downloadButton).toBeEnabled();
  fireEvent.click(downloadButton);

  router.query = { job: '42' };
  rerender(<JobDetails />);
  section = document.querySelector('#controller-logs-section');
  [, downloadButton] = within(section).getAllByRole('button');
  expect(downloadButton).toBeDisabled();
  fireEvent.click(downloadButton);
  expect(downloadManagedJobLogs).toHaveBeenCalledTimes(2);

  secondJobDownload.resolve();
  await act(async () => {});
  expect(downloadButton).toBeDisabled();

  firstJobDownload.resolve();
  await waitFor(() => expect(downloadButton).toBeEnabled());
});

it('retains 100 pending route owners without duplicate downloads', async () => {
  const routeCount = 100;
  const firstJobId = 100;
  const downloads = Array.from({ length: routeCount }, deferred);
  downloadManagedJobLogs.mockImplementation(({ jobId }) => {
    return downloads[jobId - firstJobId].promise;
  });
  useSingleManagedJob.mockImplementation((jobId) => ({
    jobData: {
      jobs: [
        {
          ...job,
          id: Number(jobId),
          name: `training-${jobId}`,
        },
      ],
    },
    loading: false,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  }));

  router.query = { job: String(firstJobId) };
  const { rerender } = render(<JobDetails />);
  let section = document.querySelector('#controller-logs-section');
  fireEvent.click(
    within(section).getByRole('button', { name: /Controller Logs/ })
  );

  for (let offset = 0; offset < routeCount; offset += 1) {
    router.query = { job: String(firstJobId + offset) };
    rerender(<JobDetails />);
    section = document.querySelector('#controller-logs-section');
    const [, downloadButton] = within(section).getAllByRole('button');
    expect(downloadButton).toBeEnabled();
    fireEvent.click(downloadButton);
  }
  expect(downloadManagedJobLogs).toHaveBeenCalledTimes(routeCount);

  for (let offset = routeCount - 1; offset >= 0; offset -= 1) {
    router.query = { job: String(firstJobId + offset) };
    rerender(<JobDetails />);
    section = document.querySelector('#controller-logs-section');
    const [, downloadButton] = within(section).getAllByRole('button');
    expect(downloadButton).toBeDisabled();
    fireEvent.click(downloadButton);
  }
  expect(downloadManagedJobLogs).toHaveBeenCalledTimes(routeCount);

  await act(async () => {
    downloads.forEach(({ resolve }) => resolve());
    await Promise.all(downloads.map(({ promise }) => promise));
  });
  const [, downloadButton] = within(section).getAllByRole('button');
  expect(downloadButton).toBeEnabled();
});

it('leaves streaming and log-line extraction to a logs-slot plugin', async () => {
  usePluginComponents.mockImplementation((slot) =>
    slot === 'jobs.detail.logs' ? [{ id: 'custom-logs' }] : []
  );

  render(<JobDetails />);

  await screen.findByText('Managed Jobs');
  expect(
    useLogStreamer.mock.calls.some(
      ([options]) =>
        options.streamArgs.controller === false && options.enabled === true
    )
  ).toBe(false);
  expect(
    PluginSlot.mock.calls.find(
      ([props]) => props.name === 'jobs.detail.logs'
    )?.[0].context
  ).toMatchObject({
    jobId: 42,
    isController: false,
    onLogLines: mockScanLines,
    refreshTrigger: 0,
  });
});

it('shares links extracted by the log viewer with job metadata', async () => {
  useLogLinkExtractor.mockReturnValue({
    extractedLinks: {
      'W&B Run': 'https://wandb.ai/acme/project/runs/run-42',
    },
    scanLines: mockScanLines,
  });

  render(<JobDetails />);

  expect(await screen.findByRole('link', { name: 'W&B Run' })).toHaveAttribute(
    'href',
    'https://wandb.ai/acme/project/runs/run-42'
  );
});

it('projects grouped job metadata through the details section', async () => {
  const groupedJobs = [
    {
      ...job,
      task_job_id: 100,
      task: 'trainer',
      is_job_group: true,
      requested_resources: '1x A100',
    },
    {
      ...job,
      task_job_id: 101,
      task: 'evaluator',
      status: 'SUCCEEDED',
      requested_resources: '1x A100',
    },
  ];
  computeJobGroupStatus.mockReturnValue('RUNNING');
  useSingleManagedJob.mockReturnValue({
    jobData: { jobs: groupedJobs },
    loading: false,
    refreshJobData: jest.fn().mockResolvedValue(undefined),
  });

  render(<JobDetails />);

  await screen.findByText('Job ID (Name)');
  expect(computeJobGroupStatus).toHaveBeenCalledWith(groupedJobs);
  expect(screen.getAllByText('JobGroup')).toHaveLength(1);
  expect(screen.getByText('1x A100 (x2 tasks)')).toBeInTheDocument();
  expect(screen.getByRole('link', { name: 'AWS' })).toHaveAttribute(
    'href',
    '/infra'
  );
});
