/**
 * Tests for DashboardCache request deduplication and caching behavior
 */

import { DashboardCache } from './cache';

// Helper to create a mock async function that tracks calls
function createMockFetch(returnValue, delay = 10) {
  const calls = [];
  const fn = jest.fn(async (...args) => {
    calls.push({ args, timestamp: Date.now() });
    await new Promise((resolve) => setTimeout(resolve, delay));
    return typeof returnValue === 'function' ? returnValue() : returnValue;
  });
  fn.calls = calls;
  return fn;
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function createSlowBackgroundFetch(thirdData = 'duplicate') {
  const background = createDeferred();
  let call = 0;
  const fetch = jest.fn(() => {
    call += 1;
    if (call === 1) {
      return Promise.resolve({ data: 'seeded' });
    }
    if (call === 2) {
      return background.promise;
    }
    return Promise.resolve({ data: thirdData });
  });
  return { background, fetch };
}

function createSameSourceFetcher(value) {
  const fetch = async (...args) => {
    fetch.calls.push(args);
    return { value };
  };
  fetch.calls = [];
  return fetch;
}

describe('DashboardCache', () => {
  let cache;

  beforeEach(() => {
    cache = new DashboardCache();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('Request Deduplication', () => {
    test('should deduplicate concurrent identical requests', async () => {
      const mockFetch = createMockFetch({ data: 'test' }, 100);

      // Make 3 concurrent identical requests
      const promises = [
        cache.get(mockFetch, ['arg1']),
        cache.get(mockFetch, ['arg1']),
        cache.get(mockFetch, ['arg1']),
      ];

      // Fast-forward time to complete the request
      jest.advanceTimersByTime(100);

      const results = await Promise.all(promises);

      // All should return the same data
      expect(results[0]).toEqual({ data: 'test' });
      expect(results[1]).toEqual({ data: 'test' });
      expect(results[2]).toEqual({ data: 'test' });

      // But the fetch function should only be called once
      expect(mockFetch).toHaveBeenCalledTimes(1);
      expect(mockFetch.calls.length).toBe(1);
    });

    test('should not deduplicate requests with different arguments', async () => {
      const mockFetch = createMockFetch({ data: 'test' }, 100);

      // Make concurrent requests with different arguments
      const promises = [
        cache.get(mockFetch, ['arg1']),
        cache.get(mockFetch, ['arg2']),
        cache.get(mockFetch, ['arg3']),
      ];

      jest.advanceTimersByTime(100);
      await Promise.all(promises);

      // Should call fetch 3 times (once for each unique argument set)
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    test('should handle sequential requests using cache', async () => {
      const mockFetch = createMockFetch({ data: 'test' }, 50);

      // First request
      const promise1 = cache.get(mockFetch, ['arg1']);
      jest.advanceTimersByTime(50);
      await promise1;

      // Second request after first completes (should use cache)
      const promise2 = cache.get(mockFetch, ['arg1']);
      await promise2;

      // Should use cache for second request, but cache also triggers
      // a background refresh, so we expect 2 calls total
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    test('should cleanup pending requests after completion', async () => {
      const mockFetch = createMockFetch({ data: 'test' }, 100);

      const promise = cache.get(mockFetch, ['arg1']);

      // Before completion, pending request should exist
      expect(cache.pendingRequests.size).toBe(1);

      jest.advanceTimersByTime(100);
      await promise;

      // After completion, pending request should be cleaned up
      expect(cache.pendingRequests.size).toBe(0);
    });

    test('should cleanup pending requests even on error', async () => {
      const mockFetch = jest.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        throw new Error('Test error');
      });

      const promise = cache.get(mockFetch, ['arg1']);

      expect(cache.pendingRequests.size).toBe(1);

      jest.advanceTimersByTime(100);

      try {
        await promise;
      } catch (error) {
        // Expected error
      }

      // Should cleanup even on error
      expect(cache.pendingRequests.size).toBe(0);
    });
  });

  describe('invalidateFunction', () => {
    test('distinguishes same-source closures with different captured values', async () => {
      const fetchA = createSameSourceFetcher('A');
      const fetchB = createSameSourceFetcher('B');

      expect(fetchA.toString()).toBe(fetchB.toString());

      await expect(cache.get(fetchA, ['x'])).resolves.toEqual({ value: 'A' });
      await expect(cache.get(fetchB, ['x'])).resolves.toEqual({ value: 'B' });

      expect(cache.getCached(fetchA, ['x'])).toEqual({ value: 'A' });
      expect(cache.getCached(fetchB, ['x'])).toEqual({ value: 'B' });
      expect(fetchA.calls).toHaveLength(1);
      expect(fetchB.calls).toHaveLength(1);
    });

    test('invalidateFunction only clears the exact fetch function', async () => {
      const fetchA = createSameSourceFetcher('A');
      const fetchB = createSameSourceFetcher('B');

      await cache.get(fetchA, ['x']);
      await cache.get(fetchB, ['x']);

      cache.invalidateFunction(fetchA);

      expect(cache.getCached(fetchA, ['x'])).toBeNull();
      expect(cache.getCached(fetchB, ['x'])).toEqual({ value: 'B' });

      await expect(cache.get(fetchA, ['x'])).resolves.toEqual({ value: 'A' });
      expect(fetchA.calls).toHaveLength(2);
      expect(fetchB.calls).toHaveLength(1);
    });

    test('memoizes function identity key generation', async () => {
      let toStringCalls = 0;
      const fetch = async () => ({ data: 'seeded' });
      fetch.toString = () => {
        toStringCalls += 1;
        return 'expensive';
      };

      await cache.get(fetch, ['x']);
      cache.getCached(fetch, ['x']);
      cache.getCached(fetch, ['x']);
      cache.invalidateFunction(fetch);
      await cache.get(fetch, ['x']);

      expect(toStringCalls).toBe(0);
    });

    test('drops in-flight pending requests, not just cached entries', async () => {
      const mockFetch = createMockFetch({ data: 'v1' }, 100);

      // Start a first fetch; it is in pendingRequests but NOT yet in
      // the cache map.
      const inflight = cache.get(mockFetch, [{ summaryOnly: false }]);

      // Invalidate mid-flight: a subsequent get() must start a fresh
      // request instead of reusing the pre-invalidate one.
      cache.invalidateFunction(mockFetch);
      const fresh = cache.get(mockFetch, [{ summaryOnly: false }]);

      jest.advanceTimersByTime(100);
      await Promise.all([inflight, fresh]);

      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    test('an invalidated in-flight request cannot write stale data to the cache', async () => {
      let firstResolve;
      const slowFirst = new Promise((resolve) => {
        firstResolve = resolve;
      });
      let call = 0;
      const mockFetch = jest.fn(async () => {
        call += 1;
        if (call === 1) {
          await slowFirst;
          return { data: 'stale' };
        }
        return { data: 'fresh' };
      });

      // Request A in flight (slow), then invalidate, then request B
      // completes with fresh data.
      const a = cache.get(mockFetch, ['x']);
      cache.invalidateFunction(mockFetch);
      const b = cache.get(mockFetch, ['x']);
      await b;

      // A resolves last — it must NOT overwrite B's fresh cache entry
      // nor delete B's bookkeeping.
      firstResolve();
      await a;

      // A resolved after the invalidate: it must not have overwritten
      // B's fresh entry (getCached is side-effect free — no background
      // refresh).
      expect(cache.getCached(mockFetch, ['x'])).toEqual({ data: 'fresh' });
    });

    test('synchronously-throwing fetch still falls back to stale cache', async () => {
      // Seed the cache, expire it, then use a fetchFunction that
      // throws BEFORE returning a promise: the catch/finally in get()
      // run during the call itself and must not hit a TDZ on the
      // pending-promise guard.
      let shouldThrow = false;
      const mockFetch = jest.fn((...args) => {
        if (shouldThrow) {
          throw new Error('sync boom');
        }
        return Promise.resolve({ data: 'seeded' });
      });

      await cache.get(mockFetch, ['x']);
      // Age the entry past the TTL so the next get() refetches.
      jest.advanceTimersByTime(6 * 60 * 1000);

      shouldThrow = true;
      const result = await cache.get(mockFetch, ['x']);
      expect(result).toEqual({ data: 'seeded' });

      // The failed request must not poison the key: once the fetch
      // works again, a stale get() must retry it (a leftover settled
      // promise in pendingRequests would short-circuit it forever).
      shouldThrow = false;
      jest.advanceTimersByTime(6 * 60 * 1000);
      const recovered = await cache.get(mockFetch, ['x']);
      expect(recovered).toEqual({ data: 'seeded' });
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    test('sync-throw with no cached fallback rejects with the original error and does not poison the key', async () => {
      let shouldThrow = true;
      const mockFetch = jest.fn((...args) => {
        if (shouldThrow) {
          throw new Error('sync boom');
        }
        return Promise.resolve({ data: 'ok' });
      });

      await expect(cache.get(mockFetch, ['y'])).rejects.toThrow('sync boom');

      // Next call must retry the fetch, not reuse the rejected promise.
      shouldThrow = false;
      const result = await cache.get(mockFetch, ['y']);
      expect(result).toEqual({ data: 'ok' });
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    test('drops every args variant of the function', async () => {
      const mockFetch = createMockFetch({ data: 'v1' }, 10);

      const p1 = cache.get(mockFetch, [{ summaryOnly: true }]);
      const p2 = cache.get(mockFetch, [{ summaryOnly: false }]);
      jest.advanceTimersByTime(10);
      await Promise.all([p1, p2]);
      expect(mockFetch).toHaveBeenCalledTimes(2);

      cache.invalidateFunction(mockFetch);

      const p3 = cache.get(mockFetch, [{ summaryOnly: true }]);
      const p4 = cache.get(mockFetch, [{ summaryOnly: false }]);
      jest.advanceTimersByTime(10);
      await Promise.all([p3, p4]);
      expect(mockFetch).toHaveBeenCalledTimes(4);
    });
  });

  describe('Cache Behavior', () => {
    test('stale read reuses an in-flight background refresh', async () => {
      const { background, fetch } = createSlowBackgroundFetch();
      const options = { ttl: 100, refreshOnAccess: false };

      await cache.get(fetch, ['x'], options);
      await cache.get(fetch, ['x'], options);
      jest.advanceTimersByTime(101);

      const staleReads = [
        cache.get(fetch, ['x'], options),
        cache.get(fetch, ['x'], options),
      ];

      await expect(Promise.all(staleReads)).resolves.toEqual([
        { data: 'seeded' },
        { data: 'seeded' },
      ]);
      expect(fetch).toHaveBeenCalledTimes(2);

      background.resolve({ data: 'fresh' });
      await Promise.resolve();
      await Promise.resolve();
      expect(cache.getCached(fetch, ['x'], options)).toEqual({
        data: 'fresh',
      });
    });

    test('stale read falls back when its background owner fails', async () => {
      const { background, fetch } = createSlowBackgroundFetch();
      const consoleWarn = jest
        .spyOn(console, 'warn')
        .mockImplementation(() => {});
      const options = { ttl: 100, refreshOnAccess: false };

      await cache.get(fetch, ['x'], options);
      await cache.get(fetch, ['x'], options);
      jest.advanceTimersByTime(101);

      const staleRead = cache.get(fetch, ['x'], options);
      await expect(staleRead).resolves.toEqual({ data: 'seeded' });
      expect(fetch).toHaveBeenCalledTimes(2);

      background.reject(new Error('background failed'));
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
      expect(cache.backgroundJobs.size).toBe(0);
      await expect(cache.get(fetch, ['x'], options)).resolves.toEqual({
        data: 'duplicate',
      });
      expect(fetch).toHaveBeenCalledTimes(3);
      consoleWarn.mockRestore();
    });

    test('stale read falls back when its background owner skips caching', async () => {
      const { background, fetch } = createSlowBackgroundFetch();
      const options = { ttl: 100, refreshOnAccess: false };

      await cache.get(fetch, ['x'], options);
      await cache.get(fetch, ['x'], options);
      jest.advanceTimersByTime(101);

      const staleRead = cache.get(fetch, ['x'], options);
      await expect(staleRead).resolves.toEqual({ data: 'seeded' });
      expect(fetch).toHaveBeenCalledTimes(2);

      background.resolve({ data: 'fallback', __skipCache: true });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
      expect(cache.backgroundJobs.size).toBe(0);
      await expect(cache.get(fetch, ['x'], options)).resolves.toEqual({
        data: 'duplicate',
      });
      expect(fetch).toHaveBeenCalledTimes(3);
    });

    test('invalidating a background owner starts and preserves a new generation', async () => {
      const { background, fetch } = createSlowBackgroundFetch('new generation');

      await cache.get(fetch, ['x']);
      await cache.get(fetch, ['x']);
      cache.invalidate(fetch, ['x']);

      await expect(cache.get(fetch, ['x'])).resolves.toEqual({
        data: 'new generation',
      });
      background.resolve({ data: 'revoked' });
      await Promise.resolve();
      await Promise.resolve();

      expect(fetch).toHaveBeenCalledTimes(3);
      expect(cache.getCached(fetch, ['x'])).toEqual({
        data: 'new generation',
      });
    });

    test('should return cached data when available and fresh', async () => {
      jest.useRealTimers(); // Use real timers for this test
      const mockFetch = createMockFetch({ data: 'test' }, 10);

      // First request
      const result1 = await cache.get(mockFetch, ['arg1']);

      // Second request (should use cache)
      const result2 = await cache.get(mockFetch, ['arg1']);

      expect(result1).toEqual({ data: 'test' });
      expect(result2).toEqual({ data: 'test' });
      // First call is the initial fetch, second is background refresh
      expect(mockFetch.mock.calls.length).toBeGreaterThanOrEqual(1);
    });

    test('should fetch fresh data when cache is stale', async () => {
      jest.useRealTimers(); // Use real timers for this test
      const mockFetch = createMockFetch({ data: 'test' }, 10);
      const ttl = 100; // Short TTL for test

      // First request
      await cache.get(mockFetch, ['arg1'], { ttl });

      // Wait for cache to become stale
      await new Promise((resolve) => setTimeout(resolve, ttl + 50));

      // Second request (cache is now stale, should fetch fresh data)
      await cache.get(mockFetch, ['arg1'], { ttl });

      expect(mockFetch.mock.calls.length).toBeGreaterThanOrEqual(2);
    });

    test('should invalidate cache correctly', async () => {
      jest.useRealTimers(); // Use real timers for this test
      const mockFetch = createMockFetch({ data: 'test' }, 10);

      // First request
      await cache.get(mockFetch, ['arg1']);

      // Invalidate cache
      cache.invalidate(mockFetch, ['arg1']);

      // Second request (should fetch fresh data)
      await cache.get(mockFetch, ['arg1']);

      expect(mockFetch.mock.calls.length).toBeGreaterThanOrEqual(2);
    });

    test('synchronous background failure preserves the cache hit and retries later', async () => {
      let shouldThrow = false;
      const mockFetch = jest.fn(() => {
        if (shouldThrow) {
          throw new Error('sync background failure');
        }
        return Promise.resolve({ data: 'seeded' });
      });
      const consoleWarn = jest
        .spyOn(console, 'warn')
        .mockImplementation(() => {});

      await cache.get(mockFetch, ['x']);
      shouldThrow = true;

      // A fresh cache hit must not fail just because its best-effort
      // background revalidation throws before returning a promise.
      await expect(cache.get(mockFetch, ['x'])).resolves.toEqual({
        data: 'seeded',
      });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();

      expect(mockFetch).toHaveBeenCalledTimes(2);
      expect(cache.backgroundJobs.size).toBe(0);

      // Cleanup must leave the key live: the next eligible hit starts exactly
      // one new background refresh instead of being suppressed forever.
      shouldThrow = false;
      await cache.get(mockFetch, ['x']);
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();

      expect(mockFetch).toHaveBeenCalledTimes(3);
      expect(cache.backgroundJobs.size).toBe(0);
      consoleWarn.mockRestore();
    });
  });

  describe('Real-world scenario: Jobs page load', () => {
    test('should handle multiple simultaneous calls from useEffect hooks', async () => {
      // Simulate getManagedJobs function
      const getManagedJobs = createMockFetch(
        {
          jobs: [
            { id: 1, name: 'job1' },
            { id: 2, name: 'job2' },
          ],
          total: 2,
        },
        200
      );

      // Simulate 4 concurrent calls (like the 4 useEffect hooks)
      const params = { allUsers: true, page: 1, limit: 10 };
      const promises = [
        cache.get(getManagedJobs, [params]), // Initial load
        cache.get(getManagedJobs, [params]), // Filters effect
        cache.get(getManagedJobs, [params]), // Status filter effect
        cache.get(getManagedJobs, [params]), // Page effect
      ];

      // All should be pending
      expect(cache.pendingRequests.size).toBe(1);

      jest.advanceTimersByTime(200);
      const results = await Promise.all(promises);

      // All should get the same data
      results.forEach((result) => {
        expect(result.jobs).toHaveLength(2);
        expect(result.total).toBe(2);
      });

      // But only one actual API call should be made
      expect(getManagedJobs).toHaveBeenCalledTimes(1);

      // Pending requests should be cleaned up
      expect(cache.pendingRequests.size).toBe(0);
    });

    test('should handle concurrent requests with different parameters', async () => {
      const getManagedJobs = createMockFetch({ jobs: [], total: 0 }, 200);

      // Simulate different filter combinations being requested concurrently
      const promises = [
        cache.get(getManagedJobs, [{ allUsers: true, page: 1 }]),
        cache.get(getManagedJobs, [{ allUsers: true, page: 2 }]),
        cache.get(getManagedJobs, [{ allUsers: false, page: 1 }]),
      ];

      expect(cache.pendingRequests.size).toBe(3);

      jest.advanceTimersByTime(200);
      await Promise.all(promises);

      // Should make 3 separate calls (different parameters)
      expect(getManagedJobs).toHaveBeenCalledTimes(3);
    });
  });

  describe('Statistics', () => {
    test('should track pending requests in stats', async () => {
      const mockFetch = createMockFetch({ data: 'test' }, 100);

      const promise = cache.get(mockFetch, ['arg1']);

      const stats = cache.getStats();
      expect(stats.pendingRequests).toBe(1);

      jest.advanceTimersByTime(100);
      await promise;

      const statsAfter = cache.getStats();
      expect(statsAfter.pendingRequests).toBe(0);
    });

    test('should clear pending requests on cache.clear()', async () => {
      const mockFetch = createMockFetch({ data: 'test' }, 100);

      cache.get(mockFetch, ['arg1']);

      expect(cache.pendingRequests.size).toBe(1);

      cache.clear();

      expect(cache.pendingRequests.size).toBe(0);
    });
  });
});
