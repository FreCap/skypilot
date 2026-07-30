import { useEffect, useRef } from 'react';

export function useVisibleRefreshInterval(enabled, intervalMs, onRefresh) {
  const onRefreshRef = useRef(onRefresh);
  const lastVisibilityRefreshAtRef = useRef(null);

  useEffect(() => {
    onRefreshRef.current = onRefresh;
  }, [onRefresh]);

  useEffect(() => {
    if (!enabled || !intervalMs) {
      lastVisibilityRefreshAtRef.current = null;
      return undefined;
    }

    const maybeRefresh = (source) => {
      if (window.document.visibilityState !== 'visible') {
        return;
      }

      const now = Date.now();
      if (
        source === 'interval' &&
        lastVisibilityRefreshAtRef.current !== null &&
        now - lastVisibilityRefreshAtRef.current < intervalMs
      ) {
        return;
      }

      if (source === 'visibilitychange') {
        lastVisibilityRefreshAtRef.current = now;
      }
      onRefreshRef.current(source);
    };

    const handleVisibilityChange = () => {
      if (window.document.visibilityState === 'visible') {
        maybeRefresh('visibilitychange');
      }
    };

    const interval = setInterval(() => {
      maybeRefresh('interval');
    }, intervalMs);
    window.document.addEventListener(
      'visibilitychange',
      handleVisibilityChange
    );

    return () => {
      lastVisibilityRefreshAtRef.current = null;
      window.document.removeEventListener(
        'visibilitychange',
        handleVisibilityChange
      );
      clearInterval(interval);
    };
  }, [enabled, intervalMs]);
}
