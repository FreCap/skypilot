import {
  getRequestActivitySnapshot,
  REQUEST_ACTIVITY_STORAGE_KEY,
  resetRequestActivityForTests,
  trackDashboardRequest,
} from './request-activity';

describe('dashboard request activity', () => {
  let nowSpy;
  const T0 = new Date('2026-07-12T12:01:00Z').getTime();

  beforeEach(() => {
    resetRequestActivityForTests();
    nowSpy = jest.spyOn(Date, 'now');
    nowSpy.mockReturnValue(T0);
  });

  afterEach(() => {
    resetRequestActivityForTests();
    nowSpy.mockRestore();
    jest.useRealTimers();
  });

  it('tracks overlapping requests and cleans up after failures', async () => {
    let resolveFirst;
    let rejectSecond;
    const first = trackDashboardRequest(
      () =>
        new Promise((resolve) => {
          resolveFirst = resolve;
        })
    );
    const second = trackDashboardRequest(
      () =>
        new Promise((_resolve, reject) => {
          rejectSecond = reject;
        })
    );

    expect(getRequestActivitySnapshot().inFlight).toBe(2);
    expect(getRequestActivitySnapshot().history.at(-1).count).toBe(2);

    resolveFirst('done');
    await expect(first).resolves.toBe('done');
    expect(getRequestActivitySnapshot().inFlight).toBe(1);

    rejectSecond(new Error('cancelled'));
    await expect(second).rejects.toThrow('cancelled');
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('groups request starts into five-minute buckets', async () => {
    await trackDashboardRequest(async () => 'first');

    nowSpy.mockReturnValue(new Date('2026-07-12T12:04:59Z').getTime());
    await trackDashboardRequest(async () => 'second');

    nowSpy.mockReturnValue(new Date('2026-07-12T12:05:00Z').getTime());
    await trackDashboardRequest(async () => 'third');

    const history = getRequestActivitySnapshot().history;
    expect(history.at(-2).count).toBe(2);
    expect(history.at(-1).count).toBe(1);
  });

  it('persists the bounded history without blocking requests', async () => {
    jest.useFakeTimers({ now: T0 });

    await trackDashboardRequest(async () => 'done');
    expect(
      window.localStorage.getItem(REQUEST_ACTIVITY_STORAGE_KEY)
    ).toBeNull();

    jest.runOnlyPendingTimers();
    const stored = JSON.parse(
      window.localStorage.getItem(REQUEST_ACTIVITY_STORAGE_KEY)
    );
    expect(stored).toHaveLength(12);
    expect(stored.at(-1).count).toBe(1);
  });
});
