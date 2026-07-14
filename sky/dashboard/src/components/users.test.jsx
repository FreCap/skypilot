import { act, render, screen, waitFor, within } from '@testing-library/react';
import dashboardCache from '@/lib/cache';
import {
  getUsers,
  getServiceAccountTokens,
  getServiceAccountTokensPaginated,
  isServiceAccountTokensPaginationAvailable,
} from '@/data/connectors/users';
import { getClusters } from '@/data/connectors/clusters';
import { getManagedJobs } from '@/data/connectors/jobs';
import { ServiceAccountTokensView } from '@/components/service-account-tokens';
import {
  aggregateUserUsage,
  getJobGpuCount as extractedGetJobGpuCount,
} from '@/components/user-usage';
import {
  buildUsersWithUsage,
  getJobGpuCount,
  UsersTable,
} from '@/components/users';

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    setPreloader: jest.fn(),
  },
}));

jest.mock('@/lib/cache-preloader', () => ({
  __esModule: true,
  default: {
    preloadForPage: jest.fn(() => Promise.resolve()),
  },
}));

jest.mock('@/data/connectors/users', () => ({
  getUsers: jest.fn(),
  getServiceAccountTokens: jest.fn(),
  getServiceAccountTokensPaginated: jest.fn(),
  isServiceAccountTokensPaginationAvailable: jest.fn(),
}));

jest.mock('@/data/connectors/clusters', () => ({
  getClusters: jest.fn(),
}));

jest.mock('@/data/connectors/jobs', () => ({
  getManagedJobs: jest.fn(),
}));

jest.mock('@/data/connectors/client', () => ({
  apiClient: {
    get: jest.fn(),
    post: jest.fn(),
  },
}));

const deferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
};

describe('UsersTable refresh lifecycle', () => {
  const user = {
    userId: 'alice-id',
    username: 'alice@example.com',
    role: 'user',
    userType: 'sso',
  };
  const cluster = {
    cluster: 'alice-cluster',
    user_hash: 'alice-id',
    status: 'RUNNING',
    gpus: { H100: 1 },
    num_nodes: 1,
    infra: 'aws/us-east-1',
  };
  const job = {
    job_id: 1,
    user_hash: 'alice-id',
    status: 'RUNNING',
    accelerators: { H100: 2 },
    resources_str_full: '1x(H100:2)',
    infra: 'aws/us-east-1',
  };

  const renderTable = (overrides = {}) => {
    const props = {
      refreshInterval: 60 * 60 * 1000,
      setLoading: jest.fn(),
      refreshDataRef: { current: null },
      checkPermissionAndAct: jest.fn(async (_message, action) => action()),
      roleLoading: false,
      onResetPassword: jest.fn(),
      onDeleteUser: jest.fn(),
      basicAuthEnabled: false,
      ingressBasicAuthEnabled: false,
      externalProxyAuthEnabled: false,
      currentUserRole: 'admin',
      currentUserId: 'admin-id',
      filters: [],
      setValueList: jest.fn(),
      deduplicateUsers: false,
      setLastFetchedTime: jest.fn(),
      setCreateError: jest.fn(),
      ...overrides,
    };
    return { ...render(<UsersTable {...props} />), props };
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('coalesces background polls while manual refresh takes ownership', async () => {
    jest.useFakeTimers();
    const backgroundUsers = deferred();
    const manualUsers = deferred();
    const setLoading = jest.fn();
    let userCalls = 0;
    let clusterCalls = 0;
    let jobCalls = 0;
    const currentUser = {
      ...user,
      userId: 'bob-id',
      username: 'bob@example.com',
    };
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getUsers) {
        userCalls += 1;
        if (userCalls === 1) return Promise.resolve([user]);
        if (userCalls === 2) return backgroundUsers.promise;
        if (userCalls === 3) return manualUsers.promise;
        return Promise.resolve([currentUser]);
      }
      if (fetcher === getClusters) {
        clusterCalls += 1;
        return Promise.resolve(
          clusterCalls === 1
            ? []
            : [{ ...cluster, user_hash: currentUser.userId }]
        );
      }
      if (fetcher === getManagedJobs) {
        jobCalls += 1;
        return Promise.resolve({
          jobs:
            jobCalls === 1 ? [] : [{ ...job, user_hash: currentUser.userId }],
        });
      }
      throw new Error('Unexpected cache fetcher');
    });

    const { props, unmount } = renderTable({
      refreshInterval: 1000,
      setLoading,
    });
    await act(async () => {
      for (let i = 0; i < 5; i++) await Promise.resolve();
    });
    expect(screen.getByText('alice')).toBeInTheDocument();
    setLoading.mockClear();

    act(() => {
      jest.advanceTimersByTime(2000);
    });
    expect(userCalls).toBe(2);
    expect(setLoading).not.toHaveBeenCalled();

    act(() => {
      props.refreshDataRef.current();
    });
    expect(userCalls).toBe(3);
    expect(setLoading).toHaveBeenCalledWith(true);

    await act(async () => {
      manualUsers.resolve([currentUser]);
      for (let i = 0; i < 5; i++) await Promise.resolve();
    });
    expect(screen.getByText('bob')).toBeInTheDocument();
    expect(screen.queryByText('alice')).not.toBeInTheDocument();
    expect(screen.getByTitle('Total GPUs: 3')).toHaveTextContent('3');
    expect(screen.getByTitle('View 1 cluster for bob')).toHaveTextContent('1');
    expect(screen.getByTitle('View 1 active job for bob')).toHaveTextContent(
      '1'
    );
    expect(clusterCalls).toBe(2);
    expect(jobCalls).toBe(2);

    await act(async () => {
      backgroundUsers.resolve([user]);
      await backgroundUsers.promise;
    });
    expect(screen.getByText('bob')).toBeInTheDocument();
    expect(screen.queryByText('alice')).not.toBeInTheDocument();
    expect(userCalls).toBe(3);

    await act(async () => {
      jest.advanceTimersByTime(1000);
      for (let i = 0; i < 5; i++) await Promise.resolve();
    });
    expect(userCalls).toBe(4);

    unmount();
  });

  it('revokes state ownership and the refresh callback on unmount', async () => {
    const usersRequest = deferred();
    const setLoading = jest.fn();
    const setLastFetchedTime = jest.fn();
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getUsers) return usersRequest.promise;
      throw new Error('A revoked refresh must not fetch resource snapshots');
    });

    const { props, unmount } = renderTable({
      setLoading,
      setLastFetchedTime,
    });
    await waitFor(() => {
      expect(dashboardCache.get).toHaveBeenCalledWith(getUsers);
      expect(props.refreshDataRef.current).toEqual(expect.any(Function));
    });
    setLoading.mockClear();

    unmount();
    await act(async () => {
      usersRequest.resolve([user]);
      await usersRequest.promise;
    });

    expect(props.refreshDataRef.current).toBeNull();
    expect(setLoading).not.toHaveBeenCalled();
    expect(setLastFetchedTime).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });
});

// getJobGpuCount decides how many GPUs a managed job contributes to a user's
// GPU total on the Users page. The critical regression it guards against:
// jobs that are STARTING/PENDING (cluster still provisioning, e.g. a k8s pod
// sitting Pending) must NOT be counted, otherwise per-user GPU totals can
// exceed the physical cluster capacity (SKY-5730).
describe('getJobGpuCount', () => {
  it('preserves the users module export as a direct alias', () => {
    expect(getJobGpuCount).toBe(extractedGetJobGpuCount);
  });

  const makeJob = (overrides) => ({
    status: 'RUNNING',
    accelerators: { H100: 8 },
    resources_str_full: '1x(H100:8)',
    job_id: 1,
    ...overrides,
  });

  it('counts GPUs for a single-node RUNNING job', () => {
    expect(getJobGpuCount(makeJob({ status: 'RUNNING' }))).toBe(8);
  });

  it('multiplies per-node GPUs by the number of nodes', () => {
    expect(
      getJobGpuCount(
        makeJob({ status: 'RUNNING', resources_str_full: '4x(H100:8)' })
      )
    ).toBe(32);
  });

  // Regression: these transient states reported their requested accelerators
  // before the GPUs were actually allocated, inflating the total above
  // cluster capacity.
  it.each(['STARTING', 'PENDING', 'SUBMITTED'])(
    'does not count GPUs for a %s job',
    (status) => {
      expect(getJobGpuCount(makeJob({ status }))).toBe(0);
    }
  );

  // A RECOVERING job is re-acquiring the same resources, and a CANCELLING job
  // still holds GPUs until its cluster is torn down, so both are counted.
  it.each(['RECOVERING', 'CANCELLING'])(
    'counts GPUs for a %s job',
    (status) => {
      expect(
        getJobGpuCount(makeJob({ status, accelerators: { A100: 4 } }))
      ).toBe(4);
    }
  );

  it('returns 0 for a RUNNING job with no accelerators (CPU-only)', () => {
    expect(
      getJobGpuCount(
        makeJob({
          status: 'RUNNING',
          accelerators: null,
          resources_str_full: '1x(vcpu=4)',
        })
      )
    ).toBe(0);
  });

  it('parses accelerators provided as a string', () => {
    expect(
      getJobGpuCount(
        makeJob({ status: 'RUNNING', accelerators: "{'V100': 4}" })
      )
    ).toBe(4);
  });

  it('defaults to a single node when resources_str_full is missing', () => {
    expect(
      getJobGpuCount(
        makeJob({ status: 'RUNNING', resources_str_full: undefined })
      )
    ).toBe(8);
  });

  it('returns 0 for a null or undefined job', () => {
    expect(getJobGpuCount(null)).toBe(0);
    expect(getJobGpuCount(undefined)).toBe(0);
  });
});

describe('aggregateUserUsage', () => {
  it('visits each snapshot once and preserves lifecycle boundaries', () => {
    let clusterVisits = 0;
    let jobVisits = 0;
    const trackVisits = (items, increment) => ({
      *[Symbol.iterator]() {
        for (const item of items) {
          increment();
          yield item;
        }
      },
    });
    const clusters = trackVisits(
      [
        {
          user_hash: 'service-id',
          status: 'UP',
          gpus: { H100: 2 },
          num_nodes: 3,
          cluster: 'active',
        },
        {
          user_hash: 'service-id',
          status: 'TERMINATED',
          gpus: { H100: 8 },
          num_nodes: 4,
          cluster: 'terminal',
        },
      ],
      () => {
        clusterVisits += 1;
      }
    );
    const jobs = trackVisits(
      [
        {
          user_hash: 'service-id',
          status: 'RUNNING',
          accelerators: { H100: 4 },
          resources_str_full: '2x(H100:4)',
          job_id: 1,
        },
        {
          user_hash: 'service-id',
          status: 'SUCCEEDED',
          accelerators: { H100: 8 },
          resources_str_full: '1x(H100:8)',
          job_id: 2,
        },
      ],
      () => {
        jobVisits += 1;
      }
    );

    const usage = aggregateUserUsage(clusters, jobs);

    expect(usage.get('service-id')).toEqual({
      clusterCount: 2,
      jobCount: 1,
      gpuCount: 14,
    });
    expect(usage.has('missing-id')).toBe(false);
    expect(clusterVisits).toBe(2);
    expect(jobVisits).toBe(2);
  });
});

describe('buildUsersWithUsage', () => {
  it('preserves an empty users response', () => {
    expect(buildUsersWithUsage(null)).toEqual([]);
  });

  it('indexes resource snapshots once and defaults users without usage', () => {
    let clusterVisits = 0;
    let jobVisits = 0;
    const trackVisits = (items, increment) => ({
      *[Symbol.iterator]() {
        for (const item of items) {
          increment();
          yield item;
        }
      },
    });
    const users = [
      { userId: 'alice-id', username: 'alice@example.com' },
      { userId: 'bob-id', username: 'bob' },
    ];
    const clusters = trackVisits(
      [
        {
          user_hash: 'alice-id',
          status: 'UP',
          gpus: { H100: 2 },
          num_nodes: 3,
          cluster: 'active',
        },
        {
          user_hash: 'alice-id',
          status: 'STOPPED',
          gpus: { H100: 8 },
          num_nodes: 4,
          cluster: 'stopped',
        },
      ],
      () => {
        clusterVisits += 1;
      }
    );
    const jobs = trackVisits(
      [
        {
          user_hash: 'alice-id',
          status: 'RUNNING',
          accelerators: { H100: 4 },
          resources_str_full: '2x(H100:4)',
          job_id: 1,
        },
        {
          user_hash: 'alice-id',
          status: 'SUCCEEDED',
          accelerators: { H100: 8 },
          resources_str_full: '1x(H100:8)',
          job_id: 2,
        },
      ],
      () => {
        jobVisits += 1;
      }
    );

    expect(buildUsersWithUsage(users, clusters, jobs)).toEqual([
      {
        userId: 'alice-id',
        username: 'alice@example.com',
        usernameDisplay: 'alice',
        fullEmailID: 'alice@example.com',
        clusterCount: 2,
        jobCount: 1,
        gpuCount: 14,
      },
      {
        userId: 'bob-id',
        username: 'bob',
        usernameDisplay: 'bob',
        fullEmailID: 'bob-id',
        clusterCount: 0,
        jobCount: 0,
        gpuCount: 0,
      },
    ]);
    expect(clusterVisits).toBe(2);
    expect(jobVisits).toBe(2);
  });
});

describe('ServiceAccountTokensView', () => {
  const baseToken = {
    token_id: 'token-1',
    token_name: 'ci-bot',
    creator_name: 'Alice',
    creator_user_hash: 'alice-id',
    service_account_name: 'ci-bot-user',
    service_account_user_id: 'service-id',
    service_account_roles: ['user'],
    created_at: 1783987200,
    last_used_at: null,
    expires_at: null,
  };

  const renderView = (overrides = {}) =>
    render(
      <ServiceAccountTokensView
        checkPermissionAndAct={jest.fn(async (_message, action) => action())}
        userRoleCache={{ id: 'alice-id', role: 'admin' }}
        setCreateSuccess={jest.fn()}
        setCreateError={jest.fn()}
        showCreateDialog={false}
        setShowCreateDialog={jest.fn()}
        showRotateDialog={false}
        setShowRotateDialog={jest.fn()}
        tokenToRotate={null}
        setTokenToRotate={jest.fn()}
        rotating={false}
        setRotating={jest.fn()}
        searchQuery=""
        setSearchQuery={jest.fn()}
        {...overrides}
      />
    );

  beforeEach(() => {
    jest.clearAllMocks();
    isServiceAccountTokensPaginationAvailable.mockReturnValue(false);
  });

  it('loads client-side tokens and preserves resource usage aggregation', async () => {
    dashboardCache.get.mockImplementation(async (fetcher) => {
      if (fetcher === getServiceAccountTokens) return [baseToken];
      if (fetcher === getClusters) {
        return [
          {
            user_hash: 'service-id',
            status: 'UP',
            gpus: { H100: 2 },
            num_nodes: 3,
            cluster: 'cluster-1',
          },
          {
            user_hash: 'service-id',
            status: 'STOPPED',
            gpus: { H100: 8 },
            num_nodes: 4,
            cluster: 'cluster-2',
          },
        ];
      }
      if (fetcher === getManagedJobs) {
        return {
          jobs: [
            {
              user_hash: 'service-id',
              status: 'RUNNING',
              accelerators: { H100: 4 },
              resources_str_full: '1x(H100:4)',
              job_id: 1,
            },
            {
              user_hash: 'service-id',
              status: 'PENDING',
              accelerators: { H100: 8 },
              resources_str_full: '1x(H100:8)',
              job_id: 2,
            },
          ],
        };
      }
      throw new Error('Unexpected cache fetcher');
    });

    renderView();

    const row = (await screen.findByText('ci-bot')).closest('tr');
    expect(row).not.toBeNull();
    expect(
      within(row).getByTitle('View 2 clusters for ci-bot')
    ).toHaveTextContent('2');
    expect(
      within(row).getByTitle('View 2 active jobs for ci-bot')
    ).toHaveTextContent('2');
    expect(within(row).getByText('10')).toBeInTheDocument();

    const requestedFetchers = dashboardCache.get.mock.calls.map(
      ([fetcher]) => fetcher
    );
    expect(requestedFetchers).toEqual([
      getServiceAccountTokens,
      getClusters,
      getManagedJobs,
    ]);
  });

  it('switches to the server-paginated token projection without changing call counts', async () => {
    isServiceAccountTokensPaginationAvailable.mockReturnValue(true);
    const pagedToken = { ...baseToken, token_name: 'paged-bot' };
    dashboardCache.get.mockImplementation(async (fetcher) => {
      if (fetcher === getServiceAccountTokens) return [];
      if (fetcher === getClusters) return [];
      if (fetcher === getManagedJobs) return { jobs: [] };
      if (fetcher === getServiceAccountTokensPaginated) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        return {
          items: [pagedToken],
          total: 1,
          total_pages: 1,
          has_next: false,
          has_prev: false,
        };
      }
      throw new Error('Unexpected cache fetcher');
    });

    renderView();

    const row = (await screen.findByText('paged-bot')).closest('tr');
    expect(row).not.toBeNull();
    expect(
      within(row).getAllByTitle('Counts hidden in server-paginated view')
    ).toHaveLength(3);

    await waitFor(() => {
      const requestedFetchers = dashboardCache.get.mock.calls.map(
        ([fetcher]) => fetcher
      );
      for (const fetcher of [
        getServiceAccountTokens,
        getClusters,
        getManagedJobs,
        getServiceAccountTokensPaginated,
      ]) {
        expect(
          requestedFetchers.filter((item) => item === fetcher)
        ).toHaveLength(1);
      }
    });
  });
});
