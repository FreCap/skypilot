import React, { useCallback, useEffect, useRef, useState } from 'react';
import { CircularProgress } from '@mui/material';

import { formatFullTimestamp } from '@/components/utils';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Card } from '@/components/ui/card';
import {
  getOperationalEvents,
  OperationalEventApiError,
  OPERATIONAL_EVENTS_UNAVAILABLE,
  OPERATIONAL_EVENTS_UPGRADE_REQUIRED,
} from '@/data/connectors/events';

function actionName(kind) {
  return kind?.split('.').slice(1).join('.') || kind || '-';
}

function errorMessage(error) {
  if (
    error instanceof OperationalEventApiError &&
    error.code === OPERATIONAL_EVENTS_UNAVAILABLE
  ) {
    return 'Operational history requires a PostgreSQL-backed API server.';
  }
  if (
    error instanceof OperationalEventApiError &&
    error.code === OPERATIONAL_EVENTS_UPGRADE_REQUIRED
  ) {
    return 'Reload this dashboard after the API server upgrade completes.';
  }
  return 'Operational history could not be loaded.';
}

export function ClusterOperationalEvents({
  clusterHash,
  clusterName,
  workspace,
}) {
  const [items, setItems] = useState([]);
  const [nextCursor, setNextCursor] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState(null);
  const controllerRef = useRef(null);
  const requestVersionRef = useRef(0);
  const hasItems = items.length > 0;

  useEffect(() => {
    const requestVersion = requestVersionRef.current + 1;
    requestVersionRef.current = requestVersion;
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    setItems([]);
    setNextCursor(null);
    setError(null);
    setLoading(true);
    setLoadingMore(false);

    getOperationalEvents(
      { clusterHash, clusterName, workspace, limit: 20 },
      controller.signal
    )
      .then((page) => {
        if (
          !controller.signal.aborted &&
          requestVersionRef.current === requestVersion
        ) {
          setItems(page.items || []);
          setNextCursor(page.next_cursor || null);
        }
      })
      .catch((requestError) => {
        if (
          requestError?.name !== 'AbortError' &&
          requestVersionRef.current === requestVersion
        ) {
          setError(requestError);
        }
      })
      .finally(() => {
        if (
          !controller.signal.aborted &&
          requestVersionRef.current === requestVersion
        ) {
          setLoading(false);
        }
      });

    return () => controller.abort();
  }, [clusterHash, clusterName, workspace]);

  const loadMore = useCallback(async () => {
    if (!nextCursor || loadingMore) return;
    const requestVersion = requestVersionRef.current;
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    setError(null);
    setLoadingMore(true);
    try {
      const page = await getOperationalEvents(
        {
          clusterHash,
          clusterName,
          workspace,
          limit: 20,
          cursor: nextCursor,
        },
        controller.signal
      );
      if (
        !controller.signal.aborted &&
        requestVersionRef.current === requestVersion
      ) {
        setItems((current) => [...current, ...(page.items || [])]);
        setNextCursor(page.next_cursor || null);
      }
    } catch (requestError) {
      if (
        requestError?.name !== 'AbortError' &&
        requestVersionRef.current === requestVersion
      ) {
        setError(requestError);
      }
    } finally {
      if (
        !controller.signal.aborted &&
        requestVersionRef.current === requestVersion
      ) {
        setLoadingMore(false);
      }
    }
  }, [clusterHash, clusterName, loadingMore, nextCursor, workspace]);

  return (
    <Card className="mb-8">
      <div className="flex items-center justify-between px-4 pt-4 pb-3">
        <h2 className="text-lg font-semibold">Operational history</h2>
      </div>
      {loading ? (
        <div className="flex items-center justify-center py-8 text-gray-500">
          <CircularProgress size={20} className="mr-2" />
          <span>Loading operational history...</span>
        </div>
      ) : error && !hasItems ? (
        <div className="px-4 pb-6 text-sm text-gray-500">
          {errorMessage(error)}
        </div>
      ) : !hasItems ? (
        <div className="px-4 pb-6 text-sm text-gray-500">
          No operational events recorded for this cluster.
        </div>
      ) : (
        <>
          {error && (
            <div className="px-4 pb-3 text-sm text-gray-500">
              Operational history refresh failed. Showing the last available
              page.
            </div>
          )}
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Time</TableHead>
                <TableHead>Action</TableHead>
                <TableHead>Outcome</TableHead>
                <TableHead>Actor</TableHead>
                <TableHead>Request</TableHead>
                <TableHead>Message</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((event) => (
                <TableRow key={event.id}>
                  <TableCell className="whitespace-nowrap">
                    {formatFullTimestamp(new Date(event.occurred_at))}
                  </TableCell>
                  <TableCell>{actionName(event.kind)}</TableCell>
                  <TableCell>{event.outcome}</TableCell>
                  <TableCell>{event.actor?.name || event.actor?.id}</TableCell>
                  <TableCell className="font-mono text-xs break-all">
                    {event.request_id}
                  </TableCell>
                  <TableCell>{event.message}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {nextCursor && (
            <div className="flex justify-center px-4 py-3 border-t">
              <button
                type="button"
                onClick={loadMore}
                disabled={loadingMore}
                className="text-sky-blue hover:text-sky-blue-bright disabled:opacity-50"
              >
                {loadingMore ? 'Loading...' : 'Load more'}
              </button>
            </div>
          )}
        </>
      )}
    </Card>
  );
}
