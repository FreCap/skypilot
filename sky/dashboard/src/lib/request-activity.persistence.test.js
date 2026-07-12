/**
 * Persistence and resilience coverage for the request activity store.
 * Exercises the reload round trip and hostile localStorage contents via
 * fresh module instances, which resetRequestActivityForTests cannot
 * simulate because it clears storage.
 */

const KEY = 'skypilot.dashboard.request-activity.v1';
const BUCKET_MS = 5 * 60 * 1000;

function freshModule() {
  let mod;
  jest.isolateModules(() => {
    mod = require('./request-activity');
  });
  return mod;
}

describe('request activity persistence', () => {
  let nowSpy;
  const T0 = new Date('2026-07-12T12:02:00Z').getTime();

  beforeEach(() => {
    window.localStorage.clear();
    jest.useFakeTimers({ now: T0 });
    nowSpy = jest.spyOn(Date, 'now').mockReturnValue(T0);
  });

  afterEach(() => {
    jest.useRealTimers();
    nowSpy.mockRestore();
    window.localStorage.clear();
  });

  it('persisted history survives a simulated reload', async () => {
    const first = freshModule();
    await first.trackDashboardRequest(async () => 'a');
    await first.trackDashboardRequest(async () => 'b');
    jest.runOnlyPendingTimers();
    expect(window.localStorage.getItem(KEY)).not.toBeNull();

    // Simulated reload: new module instance, same localStorage.
    const second = freshModule();
    const history = second.getRequestActivitySnapshot().history;
    expect(history).toHaveLength(12);
    expect(history.at(-1).count).toBe(2);
  });

  it('drops stored buckets older than the one-hour window on reload', () => {
    const staleBucket =
      Math.floor((T0 - 2 * 60 * 60 * 1000) / BUCKET_MS) * BUCKET_MS;
    const liveBucket = Math.floor(T0 / BUCKET_MS) * BUCKET_MS;
    window.localStorage.setItem(
      KEY,
      JSON.stringify([
        { timestamp: staleBucket, count: 7 },
        { timestamp: liveBucket, count: 3 },
      ])
    );

    const mod = freshModule();
    const history = mod.getRequestActivitySnapshot().history;
    expect(history.at(-1).count).toBe(3);
    expect(history.reduce((s, b) => s + b.count, 0)).toBe(3);
  });

  it('future-dated and malformed stored entries cannot poison the window', async () => {
    const futureBucket =
      Math.floor((T0 + 60 * 60 * 1000) / BUCKET_MS) * BUCKET_MS;
    window.localStorage.setItem(
      KEY,
      JSON.stringify([
        { timestamp: futureBucket, count: 99 },
        { timestamp: 'garbage', count: 5 },
        { timestamp: T0, count: -4 },
        null,
        { timestamp: T0, count: Infinity },
      ])
    );

    const mod = freshModule();
    const history = mod.getRequestActivitySnapshot().history;
    expect(history.reduce((s, b) => s + b.count, 0)).toBe(0);

    // Tracking still works after hostile data.
    await mod.trackDashboardRequest(async () => 'ok');
    expect(mod.getRequestActivitySnapshot().history.at(-1).count).toBe(1);
  });

  it('corrupt JSON in storage never breaks or blocks a request', async () => {
    window.localStorage.setItem(KEY, '{not json');
    const mod = freshModule();
    await expect(mod.trackDashboardRequest(async () => 42)).resolves.toBe(42);
    expect(mod.getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('a throwing localStorage neither breaks tracking nor persistence flush', async () => {
    const getItem = jest
      .spyOn(Storage.prototype, 'getItem')
      .mockImplementation(() => {
        throw new Error('storage disabled');
      });
    const setItem = jest
      .spyOn(Storage.prototype, 'setItem')
      .mockImplementation(() => {
        throw new Error('storage disabled');
      });

    const mod = freshModule();
    await expect(mod.trackDashboardRequest(async () => 'ok')).resolves.toBe(
      'ok'
    );
    expect(() => jest.runOnlyPendingTimers()).not.toThrow();
    expect(mod.getRequestActivitySnapshot().history.at(-1).count).toBe(1);

    getItem.mockRestore();
    setItem.mockRestore();
  });

  it('in-flight count never underflows even on unbalanced failures', async () => {
    const mod = freshModule();
    await expect(
      mod.trackDashboardRequest(() => {
        throw new Error('sync throw');
      })
    ).rejects.toThrow('sync throw');
    expect(mod.getRequestActivitySnapshot().inFlight).toBe(0);

    await mod.trackDashboardRequest(async () => 'ok');
    expect(mod.getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('coalesces a burst of starts into a single storage write', async () => {
    const mod = freshModule();
    const setItem = jest.spyOn(Storage.prototype, 'setItem');
    await Promise.all(
      Array.from({ length: 25 }, () => mod.trackDashboardRequest(async () => 1))
    );
    expect(setItem).not.toHaveBeenCalled();
    jest.runOnlyPendingTimers();
    expect(setItem).toHaveBeenCalledTimes(1);
    expect(mod.getRequestActivitySnapshot().history.at(-1).count).toBe(25);
    setItem.mockRestore();
  });
});
