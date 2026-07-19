import React, { useCallback, useEffect, useRef, useState } from 'react';
import { CircularProgress } from '@mui/material';
import { ChevronDownIcon, ChevronRightIcon, RotateCwIcon } from 'lucide-react';

import { Card } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { formatFullTimestamp } from '@/components/utils';
import { getServicePlacement } from '@/data/connectors/services';

function timestamp(value) {
  return value ? formatFullTimestamp(new Date(value * 1000)) : '-';
}

function relativeExpiry(value) {
  if (!value) return '-';
  const seconds = Math.max(0, Math.ceil(value - Date.now() / 1000));
  if (seconds < 60) return `${seconds}s`;
  return `${Math.ceil(seconds / 60)}m`;
}

export function formatAccelerators(accelerators) {
  if (!accelerators || typeof accelerators !== 'object') return '-';
  return Object.entries(accelerators)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, count]) => `${name}:${count}`)
    .join(', ');
}

export function locationDisplayStatus(location) {
  if (location.probeEligible) return 'Probe eligible';
  if (location.storedStatus === 'PREEMPTED') return 'Benched';
  return 'Active';
}

function StatusPill({ children, tone = 'neutral' }) {
  const tones = {
    active: 'bg-green-100 text-green-800',
    warning: 'bg-amber-100 text-amber-800',
    error: 'bg-red-100 text-red-800',
    neutral: 'bg-gray-100 text-gray-700',
  };
  return (
    <span
      className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${tones[tone]}`}
    >
      {children}
    </span>
  );
}

function SectionUnavailable({ label }) {
  return (
    <div className="p-5 text-sm text-gray-500">
      {label} is currently unavailable.
    </div>
  );
}

function PlacerStateCard({ state }) {
  return (
    <Card>
      <div className="border-b px-4 py-3">
        <h3 className="font-semibold">Service fallback locations</h3>
        <p className="mt-1 text-sm text-gray-500">
          A benched location becomes eligible for one probe after the retry
          window.
        </p>
      </div>
      {!state.available ? (
        <SectionUnavailable label="Live placer state" />
      ) : !state.enabled ? (
        <div className="p-5 text-sm text-gray-500">
          This service does not use the spot placer.
        </div>
      ) : state.locations.length === 0 ? (
        <div className="p-5 text-sm text-gray-500">
          No placement locations are configured.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Provider</TableHead>
                <TableHead>Region / zone</TableHead>
                <TableHead>Shape</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Next probe</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {state.locations.map((location, index) => {
                const status = locationDisplayStatus(location);
                return (
                  <TableRow
                    key={`${location.cloud}-${location.region}-${location.zone}-${index}`}
                  >
                    <TableCell className="font-medium">
                      {location.cloud}
                    </TableCell>
                    <TableCell>
                      {location.region || '-'}
                      {location.zone ? ` / ${location.zone}` : ''}
                    </TableCell>
                    <TableCell>
                      {formatAccelerators(location.accelerators)} ·{' '}
                      {location.useSpot ? 'Spot' : 'On-demand'}
                    </TableCell>
                    <TableCell>
                      <StatusPill
                        tone={
                          status === 'Active'
                            ? 'active'
                            : status === 'Benched'
                              ? 'error'
                              : 'warning'
                        }
                      >
                        {status}
                      </StatusPill>
                      <div className="mt-1 text-xs text-gray-500">
                        Stored {location.storedStatus || '-'} · Effective{' '}
                        {location.effectiveStatus || '-'}
                      </div>
                    </TableCell>
                    <TableCell>
                      {location.benchedAt ? (
                        <>
                          <div>Benched {timestamp(location.benchedAt)}</div>
                          <div className="text-xs text-gray-500">
                            Probe {timestamp(location.nextProbeAt)} (
                            {relativeExpiry(location.nextProbeAt)})
                          </div>
                        </>
                      ) : (
                        '-'
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}
      {state.truncated && (
        <div className="border-t px-4 py-2 text-xs text-amber-700">
          Additional locations were omitted by the response limit.
        </div>
      )}
    </Card>
  );
}

function CapacityHintsCard({ state }) {
  return (
    <Card>
      <div className="border-b px-4 py-3">
        <h3 className="font-semibold">AWS launch suppression</h3>
        <p className="mt-1 text-sm text-gray-500">
          These short-lived hints suppress only the exact instance demand
          shown—not every instance type in the zone or region.
        </p>
      </div>
      {!state.available ? (
        <SectionUnavailable label="AWS capacity hints" />
      ) : state.hints.length === 0 ? (
        <div className="p-5 text-sm text-gray-500">
          No active AWS capacity or quota hints for this service.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Scope</TableHead>
                <TableHead>Region / zone</TableHead>
                <TableHead>Instance type</TableHead>
                <TableHead>Nodes</TableHead>
                <TableHead>Expires</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {state.hints.map((hint, index) => (
                <TableRow
                  key={`${hint.kind}-${hint.region}-${hint.zone}-${hint.instanceType}-${index}`}
                >
                  <TableCell>
                    <StatusPill tone="warning">
                      {hint.kind === 'quota' ? 'Regional quota' : 'AZ capacity'}
                    </StatusPill>
                  </TableCell>
                  <TableCell>
                    {hint.region || '-'}
                    {hint.zone ? ` / ${hint.zone}` : ''}
                  </TableCell>
                  <TableCell className="font-medium">
                    {hint.instanceType || '-'}
                  </TableCell>
                  <TableCell>{hint.numNodes ?? '-'}</TableCell>
                  <TableCell>
                    {timestamp(hint.expiresAt)} (
                    {relativeExpiry(hint.expiresAt)})
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </Card>
  );
}

function outcomeTone(outcome) {
  if (outcome === 'succeeded') return 'active';
  if (outcome === 'capacity_failed' || outcome === 'quota_failed') {
    return 'warning';
  }
  return 'error';
}

function HistoryCard({ history, loadingMore, onLoadMore }) {
  const [expanded, setExpanded] = useState(new Set());
  const toggle = (eventId) => {
    setExpanded((current) => {
      const next = new Set(current);
      if (next.has(eventId)) next.delete(eventId);
      else next.add(eventId);
      return next;
    });
  };
  return (
    <Card>
      <div className="border-b px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h3 className="font-semibold">Placement decisions (24h)</h3>
            <p className="mt-1 text-sm text-gray-500">
              Prices are snapshots from the exact resource at decision time.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            {Object.entries(history.outcomeCounts).map(([outcome, count]) => (
              <StatusPill key={outcome} tone={outcomeTone(outcome)}>
                {outcome.replaceAll('_', ' ')}: {count}
              </StatusPill>
            ))}
          </div>
        </div>
      </div>
      {!history.available ? (
        <SectionUnavailable label="Placement history" />
      ) : history.events.length === 0 ? (
        <div className="p-5 text-sm text-gray-500">
          No placement decisions have been recorded in this window.
        </div>
      ) : (
        <div className="divide-y">
          {history.events.map((event) => {
            const isExpanded = expanded.has(event.eventId);
            const location = [event.provider, event.region, event.zone]
              .filter(Boolean)
              .join(' / ');
            return (
              <div key={event.eventId} className="px-4 py-3">
                <button
                  type="button"
                  onClick={() => toggle(event.eventId)}
                  className="flex w-full items-start gap-2 text-left"
                >
                  {isExpanded ? (
                    <ChevronDownIcon className="mt-0.5 h-4 w-4 shrink-0" />
                  ) : (
                    <ChevronRightIcon className="mt-0.5 h-4 w-4 shrink-0" />
                  )}
                  <div className="grid flex-1 gap-2 md:grid-cols-[180px_1fr_160px]">
                    <div>{timestamp(event.observedAt)}</div>
                    <div>
                      <div className="font-medium">
                        {event.clusterName || `Replica ${event.replicaId}`}
                      </div>
                      <div className="text-sm text-gray-500">
                        {location || '-'} · {event.instanceType || 'unresolved'}
                      </div>
                    </div>
                    <div className="flex items-start justify-between gap-2 md:justify-end">
                      <div className="text-right text-sm">
                        {event.hourlyPrice != null
                          ? `$${event.hourlyPrice.toFixed(4)}/hr`
                          : '-'}
                      </div>
                      <StatusPill tone={outcomeTone(event.outcome)}>
                        {(event.outcome || 'unknown').replaceAll('_', ' ')}
                      </StatusPill>
                    </div>
                  </div>
                </button>
                {isExpanded && (
                  <div className="ml-6 mt-3 rounded bg-gray-50 p-3 text-sm">
                    <div>Request: {event.requestId || '-'}</div>
                    <div>
                      Demand: {event.numNodes ?? '-'} node(s),{' '}
                      {event.useSpot ? 'Spot' : 'On-demand'}
                    </div>
                    <div>
                      Accelerators: {formatAccelerators(event.accelerators)}
                    </div>
                    {event.errorCode && (
                      <div>Error code: {event.errorCode}</div>
                    )}
                    {event.errorSummary && (
                      <div className="mt-1 text-red-700">
                        {event.errorSummary}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
      {history.nextCursor && (
        <div className="border-t p-3 text-center">
          <button
            type="button"
            onClick={onLoadMore}
            disabled={loadingMore}
            className="text-sm font-medium text-sky-blue hover:text-sky-blue-bright disabled:text-gray-400"
          >
            {loadingMore ? 'Loading…' : 'Load older decisions'}
          </button>
        </div>
      )}
    </Card>
  );
}

export function ServicePlacement({ serviceName }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState(null);
  const requestVersionRef = useRef(0);

  const fetchData = useCallback(
    async ({ cursor = null, append = false } = {}) => {
      if (!serviceName) return;
      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion;
      append ? setLoadingMore(true) : setLoading(true);
      setError(null);
      try {
        const next = await getServicePlacement({ serviceName, cursor });
        if (!isCurrentRequest()) return;
        setData((current) => {
          if (!append || !current) return next;
          return {
            ...next,
            history: {
              ...next.history,
              events: [...current.history.events, ...next.history.events],
            },
          };
        });
      } catch (requestError) {
        if (!isCurrentRequest()) return;
        setError(
          requestError.status === 404
            ? 'Placement data is unavailable on this server version.'
            : 'Failed to load placement data.'
        );
      } finally {
        if (isCurrentRequest()) {
          setLoading(false);
          setLoadingMore(false);
        }
      }
    },
    [serviceName]
  );

  useEffect(() => {
    setData(null);
    setError(null);
    setLoading(true);
    fetchData();
    return () => {
      requestVersionRef.current += 1;
    };
  }, [fetchData]);

  if (loading && !data) {
    return (
      <div className="flex items-center justify-center py-12 text-gray-500">
        <CircularProgress size={20} className="mr-2" />
        Loading placement data…
      </div>
    );
  }
  if (error && !data) {
    return <div className="py-12 text-center text-gray-500">{error}</div>;
  }
  if (!data) return null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">Current retry state</h2>
          <p className="text-sm text-gray-500">
            Read-only control-plane state; opening this page does not probe a
            provider or consume a retry.
          </p>
        </div>
        <button
          type="button"
          onClick={() => fetchData()}
          disabled={loading}
          className="inline-flex items-center text-sm font-medium text-sky-blue hover:text-sky-blue-bright disabled:text-gray-400"
        >
          <RotateCwIcon className="mr-1.5 h-4 w-4" />
          Refresh
        </button>
      </div>
      {error && <div className="text-sm text-red-700">{error}</div>}
      <PlacerStateCard state={data.placerState} />
      <CapacityHintsCard state={data.capacityHints} />
      <HistoryCard
        history={data.history}
        loadingMore={loadingMore}
        onLoadMore={() =>
          fetchData({ cursor: data.history.nextCursor, append: true })
        }
      />
    </div>
  );
}
