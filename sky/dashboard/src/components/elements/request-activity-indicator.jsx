import React, {
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from 'react';
import { Activity } from 'lucide-react';
import PropTypes from 'prop-types';

import {
  getRequestActivitySnapshot,
  getServerRequestActivitySnapshot,
  refreshRequestActivity,
  REQUEST_ACTIVITY_BUCKET_MS,
  subscribeRequestActivity,
} from '@/lib/request-activity';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';

function formatBucketTime(timestamp) {
  return new Date(timestamp).toLocaleTimeString(undefined, {
    hour: 'numeric',
    minute: '2-digit',
  });
}

function useRequestActivity() {
  const activity = useSyncExternalStore(
    subscribeRequestActivity,
    getRequestActivitySnapshot,
    getServerRequestActivitySnapshot
  );
  const initialDelayRef = useRef(null);

  if (initialDelayRef.current === null) {
    const now = Date.now();
    initialDelayRef.current =
      REQUEST_ACTIVITY_BUCKET_MS - (now % REQUEST_ACTIVITY_BUCKET_MS);
  }

  useVisibleRefreshInterval(
    true,
    REQUEST_ACTIVITY_BUCKET_MS,
    (source) => {
      const currentBucket =
        Math.floor(Date.now() / REQUEST_ACTIVITY_BUCKET_MS) *
        REQUEST_ACTIVITY_BUCKET_MS;
      const publishedBucket =
        activity.history[activity.history.length - 1]?.timestamp ?? null;
      if (source === 'visibilitychange' && publishedBucket === currentBucket) {
        return false;
      }
      refreshRequestActivity();
    },
    {
      initialDelayMs: initialDelayRef.current,
    }
  );

  return activity;
}

function requestLabel(count) {
  return `${count} ${count === 1 ? 'request' : 'requests'}`;
}

export function RequestActivityIndicator({ compact = false }) {
  const { inFlight, history } = useRequestActivity();
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef(null);

  useEffect(() => {
    if (!isOpen) return undefined;

    const closeOnOutsideClick = (event) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target)
      ) {
        setIsOpen(false);
      }
    };
    const closeOnEscape = (event) => {
      if (event.key === 'Escape') setIsOpen(false);
    };

    document.addEventListener('mousedown', closeOnOutsideClick);
    document.addEventListener('keydown', closeOnEscape);
    return () => {
      document.removeEventListener('mousedown', closeOnOutsideClick);
      document.removeEventListener('keydown', closeOnEscape);
    };
  }, [isOpen]);

  const maxCount = Math.max(1, ...history.map((bucket) => bucket.count));
  const total = history.reduce((sum, bucket) => sum + bucket.count, 0);
  const liveLabel = inFlight === 0 ? 'Idle' : `${inFlight} active`;

  return (
    <div className="relative" ref={containerRef}>
      <button
        type="button"
        onClick={() => setIsOpen((open) => !open)}
        className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-1 text-xs font-medium transition-colors ${
          inFlight > 0
            ? 'border-blue-200 bg-blue-50 text-blue-700'
            : 'border-gray-200 bg-gray-50 text-gray-600 hover:bg-gray-100'
        }`}
        aria-label={`Dashboard request activity: ${liveLabel}`}
        aria-expanded={isOpen}
      >
        <span
          className={`h-2 w-2 rounded-full ${
            inFlight > 0 ? 'bg-blue-500 animate-pulse' : 'bg-gray-400'
          }`}
          aria-hidden="true"
        />
        {!compact && <span>{liveLabel}</span>}
        {compact && inFlight > 0 && <span>{inFlight}</span>}
      </button>

      {isOpen && (
        <div
          className="absolute right-0 z-50 mt-2 w-80 rounded-lg border border-gray-200 bg-white p-4 text-gray-900 shadow-lg"
          role="dialog"
          aria-label="Dashboard request activity details"
        >
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="flex items-center gap-2 text-sm font-semibold">
                <Activity className="h-4 w-4 text-blue-600" />
                Dashboard request activity
              </div>
              <p className="mt-1 text-xs leading-4 text-gray-500">
                Best-effort API calls observed by this browser. This is not
                server-wide traffic.
              </p>
            </div>
            <span className="whitespace-nowrap text-sm font-semibold text-blue-700">
              {liveLabel}
            </span>
          </div>

          <div className="mt-4 flex items-baseline justify-between">
            <span className="text-xs font-medium text-gray-600">
              Requests started per 5 minutes
            </span>
            <span className="text-xs text-gray-500">
              {requestLabel(total)} / hour
            </span>
          </div>

          <div
            className="mt-2 grid h-20 grid-cols-12 items-end gap-1"
            aria-label="Request counts for the last hour"
          >
            {history.map((bucket) => {
              const height =
                bucket.count === 0
                  ? 2
                  : Math.max(6, Math.round((bucket.count / maxCount) * 48));
              const label = `${formatBucketTime(bucket.timestamp)}: ${requestLabel(
                bucket.count
              )}`;
              return (
                <div
                  key={bucket.timestamp}
                  className="flex h-full min-w-0 flex-col items-center justify-end"
                  title={label}
                  aria-label={label}
                >
                  <span className="mb-1 text-[9px] leading-none text-gray-500">
                    {bucket.count}
                  </span>
                  <span
                    className={`w-full rounded-sm ${
                      bucket.count > 0 ? 'bg-blue-500' : 'bg-gray-200'
                    }`}
                    style={{ height: `${height}px` }}
                    aria-hidden="true"
                  />
                </div>
              );
            })}
          </div>

          {history.length > 0 && (
            <div className="mt-1 flex justify-between text-[10px] text-gray-400">
              <span>{formatBucketTime(history[0].timestamp)}</span>
              <span>Now</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

RequestActivityIndicator.propTypes = {
  compact: PropTypes.bool,
};
