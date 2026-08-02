// Generalized caching mechanism for dashboard API calls
// This cache can be used across all pages to store and retrieve API responses

import { CACHE_CONFIG } from './config';

// Configurable cache TTL duration (in milliseconds)
// Default value configured in config.js but can be overridden per function or globally
const DEFAULT_CACHE_TTL = CACHE_CONFIG.DEFAULT_TTL;

function invokeAsPromise(fetchFunction, args) {
  try {
    return Promise.resolve(fetchFunction(...args));
  } catch (error) {
    return Promise.reject(error);
  }
}

class DashboardCache {
  constructor() {
    this.cache = new Map();
    this.backgroundJobs = new Map(); // Track ongoing background refresh jobs
    this.pendingRequests = new Map(); // Track in-flight requests to deduplicate concurrent calls
    this.functionIds = new WeakMap();
    this.nextFunctionId = 0;
    this.debugMode = false; // Added for debug mode
    this.preloader = null; // Reference to cache preloader for coordination
  }

  /**
   * Set the cache preloader instance for coordination
   * @param {Object} preloader - The cache preloader instance
   */
  setPreloader(preloader) {
    this.preloader = preloader;
  }

  /**
   * Get cached data or fetch fresh data
   * @param {Function} fetchFunction - The function to call to fetch data
   * @param {Array} [args=[]] - Arguments to pass to the fetch function
   * @param {Object} [options={}] - Cache options
   * @param {number} [options.ttl] - Time to live in milliseconds
   * @param {boolean} [options.refreshOnAccess] - Whether to refresh TTL on cache access (default: true)
   * @returns {Promise} - The cached or fresh data
   */
  async get(fetchFunction, args = [], options = {}) {
    const ttl = options.ttl || DEFAULT_CACHE_TTL;
    const refreshOnAccess = options.refreshOnAccess !== false; // Default to true
    const key = this._generateKey(fetchFunction, args);
    const functionName = fetchFunction.name || 'anonymous';

    const cachedItem = this.cache.get(key);
    const now = Date.now();

    // If we have cached data and it's not stale, return it and refresh in background
    if (cachedItem && now - cachedItem.lastUpdated < ttl) {
      const age = Math.round((now - cachedItem.lastUpdated) / 1000);
      this._debug(
        `Cache HIT for ${functionName} (age: ${age}s, TTL: ${Math.round(ttl / 1000)}s)`
      );

      // Update the lastUpdated timestamp to extend the cache life on access
      if (refreshOnAccess) {
        this.cache.set(key, {
          data: cachedItem.data,
          lastUpdated: now,
        });
        this._debug(`Cache TTL refreshed for ${functionName}`);
      }

      // Launch background refresh if we're not already refreshing
      // and if the data wasn't recently preloaded
      if (!this.backgroundJobs.has(key)) {
        const wasRecentlyPreloaded =
          this.preloader?.wasRecentlyPreloaded(fetchFunction, args) || false;
        if (!wasRecentlyPreloaded) {
          this._refreshInBackground(fetchFunction, args, key);
        } else {
          this._debug(
            `Skipping background refresh for ${functionName} - recently preloaded`
          );
        }
      }

      return cachedItem.data;
    }

    // Check if there's already a pending request for this key
    // If so, wait for it to complete instead of making a duplicate request
    if (this.pendingRequests.has(key)) {
      this._debug(
        `Request deduplication: Waiting for pending request for ${functionName}`
      );
      return this.pendingRequests.get(key);
    }

    // A background refresh can outlive the cache entry's TTL. Reuse that
    // generation's owner instead of starting a second connector call while
    // the first is still in flight.
    if (this.backgroundJobs.has(key)) {
      this._debug(
        `Request deduplication: Reusing background refresh for ${functionName}`
      );
      return cachedItem.data;
    }

    // Keep connector invocation eager while normalizing a synchronous throw
    // into the same rejected-promise path as an asynchronous failure.
    const fetchResultPromise = invokeAsPromise(fetchFunction, args);
    const requestPromise = (async () => {
      try {
        const freshData = await fetchResultPromise;

        // If the fetch function indicates the result should not be cached
        // (e.g., transient error fallback), then skip cache update and
        // return stale data if available.
        if (freshData && freshData.__skipCache) {
          this._debug(
            `Skip caching for ${functionName} due to __skipCache flag on result`
          );
          if (cachedItem) {
            return cachedItem.data;
          }
          return freshData;
        }

        // Update cache with fresh data — but only if this request is
        // still the current one for the key. If invalidate()/
        // invalidateFunction() ran while we were in flight, a newer
        // request may already be pending (or resolved); writing our
        // result would resurrect pre-invalidate data.
        if (this.pendingRequests.get(key) === requestPromise) {
          this.cache.set(key, {
            data: freshData,
            lastUpdated: Date.now(),
          });
        }

        return freshData;
      } catch (error) {
        // If fetch fails and we have stale data, return stale data
        if (cachedItem) {
          console.warn(
            `Failed to fetch fresh data for ${key}/${functionName}, returning stale data:`,
            error
          );
          return cachedItem.data;
        }

        // If no cached data and fetch fails, re-throw the error
        throw error;
      } finally {
        // Remove the pending request marker — only our own. After an
        // invalidate, a newer request may occupy this key; deleting it
        // would break that request's deduplication. (The normalized
        // promise above guarantees this never runs before the marker is
        // set, so no TDZ/undefined handling is needed.)
        if (this.pendingRequests.get(key) === requestPromise) {
          this.pendingRequests.delete(key);
        }
      }
    })();

    // Store the promise so concurrent requests can reuse it
    this.pendingRequests.set(key, requestPromise);

    return requestPromise;
  }

  /**
   * Invalidate a specific cache entry
   * @param {Function} fetchFunction - The function used to generate the cache key
   * @param {Array} [args=[]] - Arguments used to generate the cache key
   */
  invalidate(fetchFunction, args = []) {
    const key = this._generateKey(fetchFunction, args);
    this.cache.delete(key);
    // Also cancel any ongoing background job for this key
    this.backgroundJobs.delete(key);
    // Also remove any pending requests
    this.pendingRequests.delete(key);
  }

  /**
   * Invalidate all cache entries for a given function (regardless of arguments)
   * @param {Function} fetchFunction - The function to invalidate all entries for
   */
  invalidateFunction(fetchFunction) {
    const functionId = this.functionIds.get(fetchFunction);
    if (functionId === undefined) {
      return;
    }
    const keysToDelete = new Set();

    // Find all keys that start with the function hash. Sweep the
    // pending/background maps too: an in-flight first fetch has no
    // cache entry yet, and leaving its pendingRequests entry behind
    // would let a post-invalidate get() reuse the pre-invalidate
    // request instead of refetching.
    for (const map of [this.cache, this.pendingRequests, this.backgroundJobs]) {
      for (const key of map.keys()) {
        if (key.startsWith(`${functionId}_`)) {
          keysToDelete.add(key);
        }
      }
    }

    // Delete all matching entries
    keysToDelete.forEach((key) => {
      this.cache.delete(key);
      this.backgroundJobs.delete(key);
      this.pendingRequests.delete(key);
    });
  }

  /**
   * Clear all cache entries
   */
  clear() {
    this.cache.clear();
    this.backgroundJobs.clear();
    this.pendingRequests.clear();
  }

  /**
   * Synchronously return cached data without triggering a fetch.
   * Returns null on cache miss or stale data.
   * @param {Function} fetchFunction - The function used to generate the cache key
   * @param {Array} [args=[]] - Arguments used to generate the cache key
   * @param {Object} [options={}] - Options
   * @param {number} [options.ttl] - Time to live in milliseconds (default: DEFAULT_CACHE_TTL)
   * @returns {*|null} - The cached data or null
   */
  getCached(fetchFunction, args = [], options = {}) {
    const ttl = options.ttl || DEFAULT_CACHE_TTL;
    const key = this._generateKey(fetchFunction, args);
    const cachedItem = this.cache.get(key);
    if (cachedItem && Date.now() - cachedItem.lastUpdated < ttl) {
      return cachedItem.data;
    }
    return null;
  }

  /**
   * Get cache statistics for debugging
   */
  getStats() {
    return {
      cacheSize: this.cache.size,
      backgroundJobs: this.backgroundJobs.size,
      pendingRequests: this.pendingRequests.size,
      keys: Array.from(this.cache.keys()),
    };
  }

  /**
   * Get detailed cache information for debugging
   */
  getDetailedStats() {
    const now = Date.now();
    const entries = [];

    for (const [key, item] of this.cache.entries()) {
      const age = now - item.lastUpdated;
      entries.push({
        key,
        age: Math.round(age / 1000), // Age in seconds
        lastUpdated: new Date(item.lastUpdated).toISOString(),
        hasBackgroundJob: this.backgroundJobs.has(key),
        hasPendingRequest: this.pendingRequests.has(key),
      });
    }

    return {
      cacheSize: this.cache.size,
      backgroundJobs: this.backgroundJobs.size,
      pendingRequests: this.pendingRequests.size,
      entries: entries.sort((a, b) => a.age - b.age),
    };
  }

  /**
   * Enable or disable debug logging
   */
  setDebugMode(enabled) {
    this.debugMode = enabled;
  }

  /**
   * Log debug information if debug mode is enabled
   * @private
   */
  _debug(message, ...args) {
    if (this.debugMode) {
      console.log(`[DashboardCache] ${message}`, ...args);
    }
  }

  /**
   * Refresh data in the background without blocking the current request
   * @private
   */
  _refreshInBackground(fetchFunction, args, key) {
    // Mark that we have a background job running for this key. The
    // token identifies THIS job: if invalidate()/invalidateFunction()
    // removes it mid-flight, the stale result must neither be written
    // to the cache nor clobber a newer job's marker.
    const jobToken = {};
    this.backgroundJobs.set(key, jobToken);

    // Normalization keeps a synchronous connector failure inside this
    // best-effort refresh path so the valid cache hit still succeeds.
    invokeAsPromise(fetchFunction, args)
      .then((freshData) => {
        if (this.backgroundJobs.get(key) !== jobToken) {
          return; // invalidated while in flight
        }
        // Respect __skipCache signal from fetch function
        if (freshData && freshData.__skipCache) {
          return; // do not update cache
        }
        // Update cache with fresh data
        this.cache.set(key, {
          data: freshData,
          lastUpdated: Date.now(),
        });
      })
      .catch((error) => {
        console.warn(`Background refresh failed for ${key}:`, error);
      })
      .finally(() => {
        // Remove the background job marker — only our own.
        if (this.backgroundJobs.get(key) === jobToken) {
          this.backgroundJobs.delete(key);
        }
      });
  }

  /**
   * Generate a cache key based on function name and arguments
   * @private
   */
  _generateKey(fetchFunction, args) {
    const functionHash = this._getFunctionHash(fetchFunction);
    const argsHash = args.length > 0 ? JSON.stringify(args) : '';
    return `${functionHash}_${argsHash}`;
  }

  _getFunctionHash(fetchFunction) {
    let functionHash = this.functionIds.get(fetchFunction);
    if (functionHash === undefined) {
      // Stable per-function ids avoid cross-cache collisions for distinct
      // closures that share identical source text and remove repeated
      // stringification from the hot path.
      this.nextFunctionId += 1;
      functionHash = this.nextFunctionId;
      this.functionIds.set(fetchFunction, functionHash);
    }
    return functionHash;
  }
}

// Create a singleton instance to be shared across the application
const dashboardCache = new DashboardCache();

// Export both the class and the singleton instance
export { DashboardCache, dashboardCache };
export default dashboardCache;
