import { useEffect, useRef } from 'react';

export function useVisibleRefreshInterval(
  enabled,
  intervalMs,
  onRefresh,
  options = {}
) {
  const { initialDelayMs = intervalMs, catchUpOnlyWhenOverdue = false } =
    options;
  const onRefreshRef = useRef(onRefresh);

  useEffect(() => {
    onRefreshRef.current = onRefresh;
  }, [onRefresh]);

  useEffect(() => {
    if (!enabled || !Number.isFinite(intervalMs) || intervalMs <= 0) {
      return undefined;
    }

    let timeoutId = null;
    let nextIntervalDueAt = null;
    const initialDelay = Math.max(
      0,
      Number.isFinite(initialDelayMs) ? initialDelayMs : intervalMs
    );
    const cadenceAnchor = window.performance.now() + initialDelay;

    const clearScheduledRefresh = () => {
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    const nextCadenceTickAfter = (referenceTime) => {
      if (referenceTime < cadenceAnchor) {
        return cadenceAnchor;
      }

      const elapsed = referenceTime - cadenceAnchor;
      const completedIntervals = Math.floor(elapsed / intervalMs) + 1;
      return cadenceAnchor + completedIntervals * intervalMs;
    };

    const scheduleNextRefresh = (dueAt) => {
      nextIntervalDueAt = dueAt;
      clearScheduledRefresh();
      if (window.document.visibilityState !== 'visible') {
        return;
      }

      timeoutId = window.setTimeout(
        () => {
          timeoutId = null;
          const now = window.performance.now();
          if (window.document.visibilityState !== 'visible') {
            scheduleNextRefresh(nextCadenceTickAfter(now));
            return;
          }
          const resumedDueAt = nextCadenceTickAfter(now);
          try {
            void onRefreshRef.current('interval');
          } finally {
            scheduleNextRefresh(resumedDueAt);
          }
        },
        Math.max(0, dueAt - window.performance.now())
      );
    };

    const handleVisibilityChange = () => {
      if (window.document.visibilityState !== 'visible') {
        clearScheduledRefresh();
        return;
      }

      const now = window.performance.now();
      const nextDueAt = nextIntervalDueAt;
      if (catchUpOnlyWhenOverdue && nextDueAt !== null && now < nextDueAt) {
        scheduleNextRefresh(nextDueAt);
        return;
      }

      let resumedDueAt = nextCadenceTickAfter(now);
      try {
        const handled = onRefreshRef.current('visibilitychange');
        resumedDueAt =
          handled === false
            ? nextCadenceTickAfter(now)
            : nextCadenceTickAfter(now + intervalMs - 1);
      } finally {
        scheduleNextRefresh(resumedDueAt);
      }
    };

    scheduleNextRefresh(cadenceAnchor);
    window.document.addEventListener(
      'visibilitychange',
      handleVisibilityChange
    );

    return () => {
      clearScheduledRefresh();
      window.document.removeEventListener(
        'visibilitychange',
        handleVisibilityChange
      );
    };
  }, [catchUpOnlyWhenOverdue, enabled, initialDelayMs, intervalMs]);
}
