'use client';

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { AlertTriangle, Bell, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { getCurrentUserRole } from '@/data/connectors/client';
import {
  acknowledgeOperatorNotifications,
  getOperatorNotifications,
} from '@/data/connectors/operator-notifications';

function formatCategory(category) {
  return category
    .split('_')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function formatTimestamp(timestamp) {
  return new Date(timestamp * 1000).toLocaleString();
}

export function OperatorNotifications() {
  const [notifications, setNotifications] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [accessDenied, setAccessDenied] = useState(false);
  const [error, setError] = useState(null);
  const mountedRef = useRef(false);

  const load = useCallback(async (manual = false) => {
    if (manual) setRefreshing(true);
    try {
      const data = await getOperatorNotifications(7);
      if (!mountedRef.current) return;
      setNotifications(data.notifications);
      setError(null);
      if (data.latest_sequence > data.last_seen_sequence) {
        await acknowledgeOperatorNotifications(data.latest_sequence);
      }
    } catch (loadError) {
      if (mountedRef.current) setError(loadError.message);
    } finally {
      if (mountedRef.current) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    (async () => {
      const user = await getCurrentUserRole();
      if (!mountedRef.current) return;
      if (user.role !== 'admin') {
        setAccessDenied(true);
        setLoading(false);
        return;
      }
      await load();
    })();
    return () => {
      mountedRef.current = false;
    };
  }, [load]);

  if (loading) {
    return (
      <div className="p-6 text-sm text-gray-500">Loading notifications…</div>
    );
  }
  if (accessDenied) {
    return (
      <div className="p-6 text-sm text-gray-600">
        Admin access is required to view operator notifications.
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-7xl space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <Bell className="h-6 w-6 text-blue-600" />
            <h1 className="text-2xl font-semibold text-gray-900">
              Operator notifications
            </h1>
          </div>
          <p className="mt-1 text-sm text-gray-500">
            Coalesced operational alerts observed during the last 7 days.
          </p>
        </div>
        <Button
          variant="outline"
          onClick={() => load(true)}
          disabled={refreshing}
        >
          <RefreshCw
            className={`mr-2 h-4 w-4 ${refreshing ? 'animate-spin' : ''}`}
          />
          Refresh
        </Button>
      </div>

      {error && (
        <div className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-700">
          {error}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Recent categories</CardTitle>
          <CardDescription>
            Repeated occurrences update one category row and count.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {!error && notifications.length === 0 ? (
            <div className="py-12 text-center text-sm text-gray-500">
              No operator notifications were recorded in the last 7 days.
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Category</TableHead>
                  <TableHead>Message</TableHead>
                  <TableHead>First seen</TableHead>
                  <TableHead>Last seen</TableHead>
                  <TableHead className="text-right">Occurrences</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {notifications.map((notification) => (
                  <TableRow key={notification.category}>
                    <TableCell>
                      <div className="flex items-center gap-2 font-medium">
                        <AlertTriangle className="h-4 w-4 text-amber-500" />
                        {formatCategory(notification.category)}
                      </div>
                    </TableCell>
                    <TableCell className="max-w-xl whitespace-normal leading-5">
                      {notification.message}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-gray-600">
                      {formatTimestamp(notification.first_seen_at)}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-gray-600">
                      {formatTimestamp(notification.last_seen_at)}
                    </TableCell>
                    <TableCell className="text-right font-medium">
                      {notification.occurrence_count}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
