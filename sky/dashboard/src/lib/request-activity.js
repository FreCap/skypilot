'use client';

export const REQUEST_ACTIVITY_BUCKET_MS = 5 * 60 * 1000;
export const REQUEST_ACTIVITY_HISTORY_BUCKETS = 12;
export const REQUEST_ACTIVITY_STORAGE_KEY =
  'skypilot.dashboard.request-activity.v1';

const listeners = new Set();
const bucketCounts = new Map();

let initialized = false;
let inFlight = 0;
let persistHandle = null;
let persistWithIdleCallback = false;
let snapshot = {
  inFlight: 0,
  history: [],
};
const serverSnapshot = {
  inFlight: 0,
  history: [],
};

function bucketStart(timestamp) {
  return (
    Math.floor(timestamp / REQUEST_ACTIVITY_BUCKET_MS) *
    REQUEST_ACTIVITY_BUCKET_MS
  );
}

function oldestBucketStart(now) {
  return (
    bucketStart(now) -
    (REQUEST_ACTIVITY_HISTORY_BUCKETS - 1) * REQUEST_ACTIVITY_BUCKET_MS
  );
}

function pruneBuckets(now) {
  const oldest = oldestBucketStart(now);
  const current = bucketStart(now);
  for (const timestamp of bucketCounts.keys()) {
    if (timestamp < oldest || timestamp > current) {
      bucketCounts.delete(timestamp);
    }
  }
}

function buildHistory(now) {
  pruneBuckets(now);
  const oldest = oldestBucketStart(now);
  return Array.from(
    { length: REQUEST_ACTIVITY_HISTORY_BUCKETS },
    (_, index) => {
      const timestamp = oldest + index * REQUEST_ACTIVITY_BUCKET_MS;
      return {
        timestamp,
        count: bucketCounts.get(timestamp) || 0,
      };
    }
  );
}

function rebuildSnapshot(now = Date.now()) {
  snapshot = {
    inFlight,
    history: buildHistory(now),
  };
}

function readStoredHistory() {
  try {
    const stored = window.localStorage.getItem(REQUEST_ACTIVITY_STORAGE_KEY);
    if (!stored) return;

    const parsed = JSON.parse(stored);
    if (!Array.isArray(parsed)) return;

    for (const entry of parsed) {
      const timestamp = Number(entry?.timestamp);
      const count = Number(entry?.count);
      if (Number.isFinite(timestamp) && Number.isFinite(count) && count >= 0) {
        bucketCounts.set(bucketStart(timestamp), Math.floor(count));
      }
    }
  } catch (_error) {
    // Activity history is best effort. A disabled or corrupt localStorage
    // should never interfere with dashboard requests.
  }
}

function ensureInitialized() {
  if (initialized || typeof window === 'undefined') return;

  initialized = true;
  readStoredHistory();
  rebuildSnapshot();
}

function flushHistory() {
  persistHandle = null;
  persistWithIdleCallback = false;
  try {
    const history = buildHistory(Date.now());
    window.localStorage.setItem(
      REQUEST_ACTIVITY_STORAGE_KEY,
      JSON.stringify(history)
    );
  } catch (_error) {
    // Persistence is deliberately best effort and must not affect requests.
  }
}

function scheduleHistoryPersistence() {
  if (typeof window === 'undefined' || persistHandle !== null) return;

  if (typeof window.requestIdleCallback === 'function') {
    persistWithIdleCallback = true;
    persistHandle = window.requestIdleCallback(flushHistory, {
      timeout: 1000,
    });
    return;
  }

  // Safari does not expose requestIdleCallback. Delay the fallback so a burst
  // of dashboard calls still produces a single small storage write.
  persistHandle = window.setTimeout(flushHistory, 1000);
}

function publish(now = Date.now()) {
  rebuildSnapshot(now);
  for (const listener of listeners) {
    listener();
  }
}

function requestStarted() {
  ensureInitialized();
  const now = Date.now();
  const timestamp = bucketStart(now);
  inFlight += 1;
  bucketCounts.set(timestamp, (bucketCounts.get(timestamp) || 0) + 1);
  publish(now);
  scheduleHistoryPersistence();
}

function requestFinished() {
  inFlight = Math.max(0, inFlight - 1);
  publish();
}

export async function trackDashboardRequest(operation) {
  requestStarted();
  try {
    return await operation();
  } finally {
    requestFinished();
  }
}

export function subscribeRequestActivity(listener) {
  ensureInitialized();
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getRequestActivitySnapshot() {
  ensureInitialized();
  return snapshot;
}

export function getServerRequestActivitySnapshot() {
  return serverSnapshot;
}

export function refreshRequestActivity() {
  ensureInitialized();
  publish();
}

export function resetRequestActivityForTests() {
  if (typeof window !== 'undefined' && persistHandle !== null) {
    if (
      persistWithIdleCallback &&
      typeof window.cancelIdleCallback === 'function'
    ) {
      window.cancelIdleCallback(persistHandle);
    } else {
      window.clearTimeout(persistHandle);
    }
  }

  persistHandle = null;
  persistWithIdleCallback = false;
  listeners.clear();
  bucketCounts.clear();
  inFlight = 0;
  initialized = false;
  snapshot = {
    inFlight: 0,
    history: [],
  };

  if (typeof window !== 'undefined') {
    try {
      window.localStorage.removeItem(REQUEST_ACTIVITY_STORAGE_KEY);
    } catch (_error) {
      // Tests may run with storage disabled.
    }
  }
}
