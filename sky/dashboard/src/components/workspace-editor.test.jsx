import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import WorkspacePage from '@/pages/workspaces/[name]';
import {
  deleteWorkspace,
  getEnabledClouds,
  getWorkspaces,
} from '@/data/connectors/workspaces';
import { getUsers } from '@/data/connectors/users';
import { getClusters } from '@/data/connectors/clusters';
import { getManagedJobs } from '@/data/connectors/jobs';
import { dashboardCache } from '@/lib/cache';
import { summarizeWorkspaceStats } from '@/components/workspace-editor';

let mockRouter;

jest.mock('next/router', () => ({
  useRouter: () => mockRouter,
}));

jest.mock('@/components/ui/yaml-editor', () => ({
  YamlEditor: ({ value }) => <div data-testid="yaml-editor">{value}</div>,
}));

jest.mock('@/components/elements/layout', () => ({
  Layout: ({ children }) => <>{children}</>,
}));

jest.mock('@/data/connectors/workspaces', () => ({
  getWorkspaces: jest.fn(),
  updateWorkspace: jest.fn(),
  createWorkspace: jest.fn(),
  deleteWorkspace: jest.fn(),
  getEnabledClouds: jest.fn(),
}));

jest.mock('@/data/connectors/users', () => ({
  getUsers: jest.fn(),
}));

jest.mock('@/data/connectors/clusters', () => ({
  getClusters: jest.fn(),
}));

jest.mock('@/data/connectors/jobs', () => ({
  getManagedJobs: jest.fn(),
}));

jest.mock('@/data/connectors/client', () => ({
  apiClient: { fetch: jest.fn() },
}));

jest.mock('@/lib/cache', () => {
  const cache = {
    get: jest.fn(),
    setPreloader: jest.fn(),
  };
  return {
    __esModule: true,
    default: cache,
    dashboardCache: cache,
  };
});

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

describe('WorkspacePage request lifecycle', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockRouter = {
      isReady: true,
      query: { name: 'alpha' },
      push: jest.fn(),
    };
    getUsers.mockResolvedValue([]);
  });

  it('keeps late configuration and statistics on their original route', async () => {
    const alphaConfig = deferred();
    const alphaClusters = deferred();
    let workspaceCalls = 0;
    let clusterCalls = 0;

    getWorkspaces.mockImplementation(() => {
      workspaceCalls += 1;
      if (workspaceCalls === 1) return alphaConfig.promise;
      return Promise.resolve({
        beta: { gcp: { project_id: 'beta-project' } },
      });
    });
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getClusters) {
        clusterCalls += 1;
        if (clusterCalls === 1) return alphaClusters.promise;
        return Promise.resolve([
          { cluster: 'beta-cluster', workspace: 'beta', status: 'RUNNING' },
        ]);
      }
      if (fetcher === getManagedJobs) {
        const workspace = args[0].workspaceMatch;
        return Promise.resolve({
          jobs: [{ workspace, status: 'RUNNING' }],
        });
      }
      if (fetcher === getEnabledClouds) {
        return Promise.resolve([args[0] === 'beta' ? 'gcp' : 'aws']);
      }
      throw new Error('Unexpected dashboard cache fetcher');
    });

    const { rerender } = render(<WorkspacePage />);
    await waitFor(() => expect(getWorkspaces).toHaveBeenCalledTimes(1));

    mockRouter = { ...mockRouter, query: { name: 'beta' } };
    rerender(<WorkspacePage />);

    await waitFor(() =>
      expect(screen.getByTestId('yaml-editor')).toHaveTextContent(
        'beta-project'
      )
    );
    expect(screen.getByText('1 / 1')).toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenNthCalledWith(1, getClusters, [
      { workspaces: ['alpha'] },
    ]);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(4, getClusters, [
      { workspaces: ['beta'] },
    ]);

    await act(async () => {
      alphaConfig.resolve({
        alpha: { aws: { region: 'us-east-1' } },
      });
      alphaClusters.resolve([
        { cluster: 'alpha-1', workspace: 'alpha', status: 'RUNNING' },
        { cluster: 'alpha-2', workspace: 'alpha', status: 'STOPPED' },
      ]);
      await Promise.all([alphaConfig.promise, alphaClusters.promise]);
    });

    expect(screen.getByTestId('yaml-editor')).toHaveTextContent('beta-project');
    expect(screen.getByTestId('yaml-editor')).not.toHaveTextContent(
      'us-east-1'
    );
    expect(screen.getByText('1 / 1')).toBeInTheDocument();
    expect(getWorkspaces).toHaveBeenCalledTimes(2);
    expect(getUsers).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenCalledTimes(6);
  });

  it('keeps failures from an unmounted route out of the current editor', async () => {
    const alphaConfig = deferred();
    const alphaClusters = deferred();
    let clusterCalls = 0;
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    getWorkspaces
      .mockImplementationOnce(() => alphaConfig.promise)
      .mockResolvedValue({ beta: {} });
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getClusters) {
        clusterCalls += 1;
        if (clusterCalls === 1) return alphaClusters.promise;
        return Promise.resolve([]);
      }
      if (fetcher === getManagedJobs) return Promise.resolve({ jobs: [] });
      if (fetcher === getEnabledClouds) return Promise.resolve([]);
      throw new Error('Unexpected dashboard cache fetcher');
    });

    const { rerender } = render(<WorkspacePage />);
    await waitFor(() => expect(getWorkspaces).toHaveBeenCalledTimes(1));
    mockRouter = { ...mockRouter, query: { name: 'beta' } };
    rerender(<WorkspacePage />);
    await waitFor(() =>
      expect(screen.getByTestId('yaml-editor')).toHaveTextContent('beta:')
    );

    await act(async () => {
      alphaConfig.reject(new Error('stale config failure'));
      alphaClusters.reject(new Error('stale stats failure'));
      await Promise.allSettled([alphaConfig.promise, alphaClusters.promise]);
    });

    expect(screen.queryByText('Error')).not.toBeInTheDocument();
    expect(screen.getByTestId('yaml-editor')).toHaveTextContent('beta:');
    consoleError.mockRestore();
  });

  it('cancels delayed navigation after leaving a deleted workspace', async () => {
    const push = jest.fn();
    mockRouter = { ...mockRouter, push };
    getWorkspaces.mockResolvedValue({ alpha: {}, beta: {} });
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getClusters) return Promise.resolve([]);
      if (fetcher === getManagedJobs) return Promise.resolve({ jobs: [] });
      if (fetcher === getEnabledClouds) return Promise.resolve([]);
      throw new Error('Unexpected dashboard cache fetcher');
    });
    deleteWorkspace.mockResolvedValue(undefined);

    const { unmount } = render(<WorkspacePage />);
    await waitFor(() =>
      expect(screen.getByTestId('yaml-editor')).toHaveTextContent('alpha:')
    );

    jest.useFakeTimers();
    try {
      fireEvent.click(screen.getAllByRole('button', { name: 'Delete' })[0]);
      fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
      await act(async () => {
        await Promise.resolve();
      });
      expect(deleteWorkspace).toHaveBeenCalledWith('alpha');

      unmount();
      act(() => {
        jest.advanceTimersByTime(1500);
      });

      expect(push).not.toHaveBeenCalled();
    } finally {
      jest.useRealTimers();
    }
  });

  it('does not schedule navigation when deletion finishes after leaving', async () => {
    const deletion = deferred();
    const push = jest.fn();
    mockRouter = { ...mockRouter, push };
    getWorkspaces.mockResolvedValue({ alpha: {} });
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getClusters) return Promise.resolve([]);
      if (fetcher === getManagedJobs) return Promise.resolve({ jobs: [] });
      if (fetcher === getEnabledClouds) return Promise.resolve([]);
      throw new Error('Unexpected dashboard cache fetcher');
    });
    deleteWorkspace.mockReturnValue(deletion.promise);

    const { unmount } = render(<WorkspacePage />);
    await waitFor(() =>
      expect(screen.getByTestId('yaml-editor')).toHaveTextContent('alpha:')
    );

    jest.useFakeTimers();
    try {
      fireEvent.click(screen.getAllByRole('button', { name: 'Delete' })[0]);
      fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
      expect(deleteWorkspace).toHaveBeenCalledWith('alpha');

      unmount();
      await act(async () => {
        deletion.resolve();
        await deletion.promise;
      });
      act(() => {
        jest.advanceTimersByTime(1500);
      });

      expect(push).not.toHaveBeenCalled();
    } finally {
      jest.useRealTimers();
    }
  });
});

describe('summarizeWorkspaceStats', () => {
  it('visits each source row once and preserves active-state counts', () => {
    let clusterWorkspaceReads = 0;
    let clusterStatusReads = 0;
    let jobWorkspaceReads = 0;
    let jobStatusReads = 0;
    const cluster = (workspace, status) => ({
      get workspace() {
        clusterWorkspaceReads += 1;
        return workspace;
      },
      get status() {
        clusterStatusReads += 1;
        return status;
      },
    });
    const job = (workspace, status) => ({
      get workspace() {
        jobWorkspaceReads += 1;
        return workspace;
      },
      get status() {
        jobStatusReads += 1;
        return status;
      },
    });

    expect(
      summarizeWorkspaceStats(
        'target',
        [
          cluster('target', 'RUNNING'),
          cluster('target', 'STOPPED'),
          cluster('target', 'LAUNCHING'),
          cluster('other', 'RUNNING'),
        ],
        [
          job('target', 'RUNNING'),
          job('target', 'SUCCEEDED'),
          job('other', 'RUNNING'),
        ]
      )
    ).toEqual({
      totalClusterCount: 3,
      runningClusterCount: 2,
      managedJobsCount: 1,
    });
    expect(clusterWorkspaceReads).toBe(4);
    expect(clusterStatusReads).toBe(3);
    expect(jobWorkspaceReads).toBe(3);
    expect(jobStatusReads).toBe(2);
  });

  it('preserves the implicit default workspace', () => {
    expect(
      summarizeWorkspaceStats(
        'default',
        [{ status: 'RUNNING' }],
        [{ workspace: 'default', status: 'PENDING' }]
      )
    ).toEqual({
      totalClusterCount: 1,
      runningClusterCount: 1,
      managedJobsCount: 1,
    });
  });
});
