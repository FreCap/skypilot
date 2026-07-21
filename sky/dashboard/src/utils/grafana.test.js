import {
  checkGrafanaAvailability,
  resetGrafanaAvailabilityCache,
} from '@/utils/grafana';

describe('checkGrafanaAvailability', () => {
  beforeAll(() => {
    // jsdom's AbortSignal lacks the static timeout() browsers provide.
    if (typeof AbortSignal.timeout !== 'function') {
      AbortSignal.timeout = () => new AbortController().signal;
    }
  });

  beforeEach(() => {
    resetGrafanaAvailabilityCache();
    global.fetch = jest.fn();
  });

  afterEach(() => {
    resetGrafanaAvailabilityCache();
    jest.restoreAllMocks();
  });

  it('caches a successful (available) check across sequential calls', async () => {
    global.fetch.mockResolvedValue({ status: 200 });

    await expect(checkGrafanaAvailability()).resolves.toBe(true);
    await expect(checkGrafanaAvailability()).resolves.toBe(true);

    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('caches an unavailable (non-200) check across sequential calls', async () => {
    global.fetch.mockResolvedValue({ status: 502 });

    await expect(checkGrafanaAvailability()).resolves.toBe(false);
    await expect(checkGrafanaAvailability()).resolves.toBe(false);

    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('caches a failed (network error) check across sequential calls', async () => {
    global.fetch.mockRejectedValue(new Error('network down'));

    await expect(checkGrafanaAvailability()).resolves.toBe(false);
    await expect(checkGrafanaAvailability()).resolves.toBe(false);

    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('deduplicates concurrent in-flight checks into one request', async () => {
    let resolveFetch;
    global.fetch.mockReturnValue(
      new Promise((resolve) => {
        resolveFetch = resolve;
      })
    );

    const first = checkGrafanaAvailability();
    const second = checkGrafanaAvailability();
    resolveFetch({ status: 200 });

    await expect(first).resolves.toBe(true);
    await expect(second).resolves.toBe(true);
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('re-checks after the cache is reset', async () => {
    global.fetch.mockResolvedValue({ status: 200 });

    await checkGrafanaAvailability();
    resetGrafanaAvailabilityCache();
    await checkGrafanaAvailability();

    expect(global.fetch).toHaveBeenCalledTimes(2);
  });
});
