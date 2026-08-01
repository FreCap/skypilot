import React, { useCallback, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { AlertTriangle, Bell } from 'lucide-react';
import PropTypes from 'prop-types';

import {
  acknowledgeOperatorNotifications,
  getOperatorNotifications,
} from '@/data/connectors/operator-notifications';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';

export const OPERATOR_NOTIFICATION_POLL_MS = 60 * 1000;

function formatCategory(category) {
  return category
    .split('_')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function formatTimestamp(timestamp) {
  return new Date(timestamp * 1000).toLocaleString();
}

function reconcileAcknowledgedNotifications(snapshot, acknowledgedThrough) {
  const lastSeenSequence = Math.max(
    snapshot.last_seen_sequence || 0,
    acknowledgedThrough
  );
  let notifications = snapshot.notifications || [];
  let notificationsChanged = false;
  let unreadCount = 0;

  notifications.forEach((notification, index) => {
    const shouldMarkRead =
      notification.unread && notification.sequence <= lastSeenSequence;
    if (shouldMarkRead) {
      if (!notificationsChanged) {
        notifications = [...notifications];
        notificationsChanged = true;
      }
      notifications[index] = { ...notification, unread: false };
    }
    if (!shouldMarkRead && notification.unread) {
      unreadCount += 1;
    }
  });

  if (
    !notificationsChanged &&
    lastSeenSequence === (snapshot.last_seen_sequence || 0) &&
    unreadCount === (snapshot.unread_count || 0)
  ) {
    return snapshot;
  }

  return {
    ...snapshot,
    notifications,
    unread_count: unreadCount,
    last_seen_sequence: lastSeenSequence,
  };
}

export function OperatorNotificationBell({ role, compact = false }) {
  const [data, setData] = useState({
    notifications: [],
    unread_count: 0,
    latest_sequence: 0,
    last_seen_sequence: 0,
  });
  const [isOpen, setIsOpen] = useState(false);
  const [openNotifications, setOpenNotifications] = useState([]);
  const [error, setError] = useState(null);
  const containerRef = useRef(null);
  const lifecycleVersionRef = useRef(0);
  const refreshInFlightRef = useRef(null);
  const acknowledgedThroughRef = useRef(0);

  const refresh = useCallback((lifecycleVersion) => {
    const inFlight = refreshInFlightRef.current;
    if (inFlight?.lifecycleVersion === lifecycleVersion) {
      return inFlight.promise;
    }
    const refreshPromise = (async () => {
      try {
        const next = reconcileAcknowledgedNotifications(
          await getOperatorNotifications(7),
          acknowledgedThroughRef.current
        );
        if (lifecycleVersion !== lifecycleVersionRef.current) return;
        acknowledgedThroughRef.current = next.last_seen_sequence || 0;
        setData(next);
        setError(null);
      } catch (fetchError) {
        if (lifecycleVersion === lifecycleVersionRef.current) {
          setError(fetchError.message);
        }
      }
    })().finally(() => {
      if (refreshInFlightRef.current?.promise === refreshPromise) {
        refreshInFlightRef.current = null;
      }
    });
    refreshInFlightRef.current = {
      lifecycleVersion,
      promise: refreshPromise,
    };
    return refreshPromise;
  }, []);

  useVisibleRefreshInterval(
    role === 'admin',
    OPERATOR_NOTIFICATION_POLL_MS,
    () => void refresh(lifecycleVersionRef.current)
  );

  useEffect(() => {
    const lifecycleVersion = lifecycleVersionRef.current + 1;
    lifecycleVersionRef.current = lifecycleVersion;
    acknowledgedThroughRef.current = 0;
    const revokeLifecycle = () => {
      if (lifecycleVersionRef.current === lifecycleVersion) {
        lifecycleVersionRef.current += 1;
      }
      if (refreshInFlightRef.current?.lifecycleVersion === lifecycleVersion) {
        refreshInFlightRef.current = null;
      }
      acknowledgedThroughRef.current = 0;
    };
    if (role !== 'admin') {
      return revokeLifecycle;
    }
    void refresh(lifecycleVersion);
    return revokeLifecycle;
  }, [refresh, role]);

  useEffect(() => {
    if (!isOpen) return undefined;
    const close = (event) => {
      if (
        event.key === 'Escape' ||
        (containerRef.current && !containerRef.current.contains(event.target))
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', close);
    document.addEventListener('keydown', close);
    return () => {
      document.removeEventListener('mousedown', close);
      document.removeEventListener('keydown', close);
    };
  }, [isOpen]);

  if (role !== 'admin') return null;

  const unreadCount = data.unread_count || 0;
  const badge = unreadCount > 9 ? '9+' : String(unreadCount);

  const acknowledge = async (throughSequence) => {
    const lifecycleVersion = lifecycleVersionRef.current;
    try {
      const result = await acknowledgeOperatorNotifications(throughSequence);
      if (lifecycleVersion !== lifecycleVersionRef.current) return;
      acknowledgedThroughRef.current = Math.max(
        acknowledgedThroughRef.current,
        result.last_seen_sequence || 0
      );
      setData((current) =>
        reconcileAcknowledgedNotifications(
          current,
          acknowledgedThroughRef.current
        )
      );
    } catch (acknowledgeError) {
      if (lifecycleVersion === lifecycleVersionRef.current) {
        setError(acknowledgeError.message);
      }
    }
  };

  const toggle = () => {
    const opening = !isOpen;
    setIsOpen(opening);
    if (!opening) return;

    const unread = data.notifications.filter((item) => item.unread);
    setOpenNotifications(unread);
    const lastSeenSequence = Math.max(
      data.last_seen_sequence || 0,
      acknowledgedThroughRef.current
    );
    if (data.latest_sequence > lastSeenSequence) {
      acknowledge(data.latest_sequence);
    }
  };

  return (
    <div className="relative" ref={containerRef}>
      <button
        type="button"
        onClick={toggle}
        className="relative inline-flex items-center justify-center rounded-full p-2 text-gray-600 transition-colors hover:bg-gray-100 hover:text-blue-600"
        aria-label={`Operator notifications: ${unreadCount} unread`}
        aria-expanded={isOpen}
        title="Operator notifications"
      >
        <Bell className={compact ? 'h-4 w-4' : 'h-5 w-5'} />
        {unreadCount > 0 && (
          <span className="absolute -right-1 -top-1 min-w-4 rounded-full bg-red-600 px-1 text-center text-[10px] font-semibold leading-4 text-white">
            {badge}
          </span>
        )}
      </button>

      {isOpen && (
        <div
          className="absolute right-0 z-50 mt-2 w-[calc(100vw-1rem)] max-w-96 rounded-lg border border-gray-200 bg-white text-gray-900 shadow-lg"
          role="dialog"
          aria-label="Operator notification details"
        >
          <div className="border-b border-gray-200 px-4 py-3">
            <div className="flex items-center gap-2 text-sm font-semibold">
              <Bell className="h-4 w-4 text-blue-600" />
              Operator notifications
            </div>
            <p className="mt-1 text-xs text-gray-500">
              New low-cardinality alerts since you last checked.
            </p>
          </div>

          <div className="max-h-80 overflow-y-auto">
            {error && (
              <div className="m-3 rounded-md bg-red-50 p-3 text-xs text-red-700">
                {error}
              </div>
            )}
            {!error && openNotifications.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-gray-500">
                No new notifications.
              </div>
            )}
            {openNotifications.map((notification) => (
              <div
                key={`${notification.category}-${notification.sequence}`}
                className="border-b border-gray-100 px-4 py-3 last:border-b-0"
              >
                <div className="flex items-start gap-2">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-500" />
                  <div className="min-w-0">
                    <div className="text-xs font-semibold text-gray-700">
                      {formatCategory(notification.category)}
                    </div>
                    <p className="mt-1 text-sm leading-5 text-gray-800">
                      {notification.message}
                    </p>
                    <p className="mt-1 text-xs text-gray-500">
                      Last seen {formatTimestamp(notification.last_seen_at)}
                      {notification.occurrence_count > 1 &&
                        ` · ${notification.occurrence_count} occurrences`}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>

          <div className="border-t border-gray-200 px-4 py-3 text-right">
            <Link
              href="/notifications"
              className="text-sm font-medium text-blue-600 hover:text-blue-700"
              onClick={() => setIsOpen(false)}
              prefetch={false}
            >
              View the last 7 days
            </Link>
          </div>
        </div>
      )}
    </div>
  );
}

OperatorNotificationBell.propTypes = {
  role: PropTypes.string,
  compact: PropTypes.bool,
};
