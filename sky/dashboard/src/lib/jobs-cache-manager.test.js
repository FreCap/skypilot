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

jest.mock('@/data/connectors/jobs', () => ({
  __esModule: true,
  getManagedJobs: jest.fn(),
}));

import dashboardCache from '@/lib/cache';
import { JobsCacheManager } from '@/lib/jobs-cache-manager';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function makePageResponse(id, total = 1) {
  return {
    jobs: [{ id, status: 'RUNNING' }],
    total,
    totalNoFilter: total,
    statusCounts: { RUNNING: total },
    controllerStopped: false,
  };
}

describe('JobsCacheManager invalidation fencing', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    delete window.__skyJobsPaginationFetch;
  });

  it('keeps a stale in-flight page response from repopulating cache after invalidation', async () => {
    const manager = new JobsCacheManager();
    jest
      .spyOn(manager, '_kickOffBackgroundPrefetch')
      .mockImplementation(() => {});
    const firstFetch = deferred();
    const secondFetch = deferred();
    const options = { page: 1, limit: 10 };

    dashboardCache.get
      .mockImplementationOnce(() => firstFetch.promise)
      .mockImplementationOnce(() => secondFetch.promise);

    const staleResultPromise = manager.getPaginatedJobs(options);
    manager.invalidateCache();
    const freshResultPromise = manager.getPaginatedJobs(options);

    secondFetch.resolve(makePageResponse('fresh-job'));
    const freshResult = await freshResultPromise;
    expect(freshResult.jobs[0].id).toBe('fresh-job');
    expect(freshResult.fromCache).toBe(false);

    firstFetch.resolve(makePageResponse('stale-job'));
    const staleResult = await staleResultPromise;
    expect(staleResult.jobs[0].id).toBe('stale-job');

    const cachedResult = await manager.getPaginatedJobs(options);
    expect(cachedResult.fromCache).toBe(true);
    expect(cachedResult.jobs[0].id).toBe('fresh-job');
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('drops a stale background prefetch after invalidation', async () => {
    const manager = new JobsCacheManager();
    const staleFullDataset = deferred();

    dashboardCache.get
      .mockResolvedValueOnce(makePageResponse('page-1-job', 2))
      .mockImplementationOnce(() => staleFullDataset.promise);

    await manager.getPaginatedJobs({ page: 1, limit: 1 });

    manager.invalidateCache();
    staleFullDataset.resolve({
      jobs: [
        { id: 'stale-page-1', status: 'RUNNING' },
        { id: 'stale-page-2', status: 'RUNNING' },
      ],
      total: 2,
      totalNoFilter: 2,
      statusCounts: { RUNNING: 2 },
      controllerStopped: false,
    });
    await Promise.resolve();

    expect(manager.isDataCached({ page: 2, limit: 1 })).toBeFalsy();
  });

  it('still dedupes identical concurrent page fetches to one network call', async () => {
    const manager = new JobsCacheManager();
    jest
      .spyOn(manager, '_kickOffBackgroundPrefetch')
      .mockImplementation(() => {});
    dashboardCache.get.mockResolvedValue(makePageResponse('only-job'));

    const [first, second] = await Promise.all([
      manager.getPaginatedJobs({ page: 1, limit: 10 }),
      manager.getPaginatedJobs({ page: 1, limit: 10 }),
    ]);

    expect(first.jobs[0].id).toBe('only-job');
    expect(second.jobs[0].id).toBe('only-job');
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });
});
