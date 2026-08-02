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
  const cadenceAnchorRef = useRef(null);
  const nextIntervalDueAtRef = useRef(null);

  useEffect(() => {
    onRefreshRef.current = onRefresh;
  }, [onRefresh]);

  useEffect(() => {
    if (!enabled || !intervalMs) {
      nextIntervalDueAtRef.current = null;
      return undefined;
    }

    let timeoutId = null;

    const clearScheduledRefresh = () => {
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    const nextCadenceTickAfter = (referenceTime) => {
      const anchor = cadenceAnchorRef.current;
      if (anchor === null || referenceTime < anchor) {
        return anchor;
      }

      const elapsed = referenceTime - anchor;
      const completedIntervals = Math.floor(elapsed / intervalMs) + 1;
      return anchor + completedIntervals * intervalMs;
    };

    const scheduleNextRefresh = (dueAt) => {
      nextIntervalDueAtRef.current = dueAt;
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
          void onRefreshRef.current('interval');
          scheduleNextRefresh(nextCadenceTickAfter(now));
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
      const nextDueAt = nextIntervalDueAtRef.current;
      if (catchUpOnlyWhenOverdue && nextDueAt !== null && now < nextDueAt) {
        scheduleNextRefresh(nextDueAt);
        return;
      }

      const handled = onRefreshRef.current('visibilitychange');
      const resumedDueAt =
        handled === false
          ? nextCadenceTickAfter(now)
          : nextCadenceTickAfter(now + intervalMs - 1);
      scheduleNextRefresh(resumedDueAt);
    };

    const initialDelay = Math.max(
      0,
      Number.isFinite(initialDelayMs) ? initialDelayMs : intervalMs
    );
    cadenceAnchorRef.current = window.performance.now() + initialDelay;
    scheduleNextRefresh(cadenceAnchorRef.current);
    window.document.addEventListener(
      'visibilitychange',
      handleVisibilityChange
    );

    return () => {
      cadenceAnchorRef.current = null;
      nextIntervalDueAtRef.current = null;
      clearScheduledRefresh();
      window.document.removeEventListener(
        'visibilitychange',
        handleVisibilityChange
      );
    };
  }, [catchUpOnlyWhenOverdue, enabled, initialDelayMs, intervalMs]);
}
