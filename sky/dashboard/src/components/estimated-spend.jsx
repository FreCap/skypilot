'use client';

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import PropTypes from 'prop-types';
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  Tooltip,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';
import {
  AlertTriangle,
  Clock3,
  DollarSign,
  RefreshCw,
  Server,
} from 'lucide-react';

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
import {
  getCurrentRole,
  getEstimatedSpend,
} from '@/data/connectors/estimated_spend';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

const RANGE_OPTIONS = [7, 30, 90];
const AUTO_REFRESH_MS = 60 * 1000;

export function formatCurrency(value) {
  const number = Number(value || 0);
  if (number >= 10000) {
    return `$${number.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  }
  return `$${number.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

export function formatHours(seconds) {
  const hours = Number(seconds || 0) / 3600;
  return `${hours.toLocaleString(undefined, {
    maximumFractionDigits: hours >= 100 ? 0 : 1,
  })} h`;
}

export function workloadLabel(workload) {
  const type = workload.workload_type || 'cluster';
  const id = workload.workload_id || 'unattributed';
  if (type === 'managed_job') return `Managed job #${id}`;
  if (type === 'service') return `Service · ${id}`;
  if (type === 'pool') return `Pool · ${id}`;
  if (type === 'controller') return `Platform · ${id}`;
  if (type === 'managed') return `Managed workload · ${id}`;
  return `Cluster · ${id}`;
}

function MetricCard({ title, value, detail, icon: Icon }) {
  return (
    <Card>
      <CardContent className="p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-sm font-medium text-muted-foreground">{title}</p>
            <p className="mt-2 text-2xl font-semibold tracking-tight">
              {value}
            </p>
            <p className="mt-1 text-xs text-muted-foreground">{detail}</p>
          </div>
          <div className="rounded-lg bg-blue-50 p-2.5 text-blue-600 dark:bg-blue-950/40 dark:text-blue-300">
            <Icon className="h-5 w-5" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

MetricCard.propTypes = {
  title: PropTypes.string.isRequired,
  value: PropTypes.string.isRequired,
  detail: PropTypes.string.isRequired,
  icon: PropTypes.elementType.isRequired,
};

function EmptyEstimate() {
  return (
    <Card className="border-dashed">
      <CardContent className="flex min-h-64 flex-col items-center justify-center p-8 text-center">
        <Clock3 className="mb-4 h-9 w-9 text-muted-foreground" />
        <h2 className="text-lg font-semibold">Preparing the first estimate</h2>
        <p className="mt-2 max-w-lg text-sm text-muted-foreground">
          The background rollup is reading cluster history in bounded batches.
          This page will populate without refreshing or blocking live workloads.
        </p>
      </CardContent>
    </Card>
  );
}

export function EstimatedSpend() {
  const [rangeDays, setRangeDays] = useState(30);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [forbidden, setForbidden] = useState(false);
  const [lastFetchedAt, setLastFetchedAt] = useState(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const role = await getCurrentRole();
      if (role.role !== 'admin') {
        setForbidden(true);
        setData(null);
        return;
      }
      setForbidden(false);
      const estimate = await getEstimatedSpend(rangeDays);
      setData(estimate);
      setLastFetchedAt(new Date());
    } catch (fetchError) {
      if (fetchError.status === 403) {
        setForbidden(true);
      } else {
        setError(fetchError);
      }
    } finally {
      setLoading(false);
    }
  }, [rangeDays]);

  useEffect(() => {
    fetchData();
    const timer = setInterval(fetchData, AUTO_REFRESH_MS);
    return () => clearInterval(timer);
  }, [fetchData]);

  const chartData = useMemo(() => {
    const days = data?.days || [];
    return {
      labels: days.map((day) =>
        new Date(`${day.date}T00:00:00Z`).toLocaleDateString(undefined, {
          month: 'short',
          day: 'numeric',
          timeZone: 'UTC',
        })
      ),
      datasets: [
        {
          label: 'Estimated compute cost',
          data: days.map((day) => day.estimated_cost),
          backgroundColor: 'rgba(14, 165, 233, 0.75)',
          borderColor: 'rgb(2, 132, 199)',
          borderWidth: 1,
          borderRadius: 4,
          maxBarThickness: 36,
        },
      ],
    };
  }, [data]);

  const chartOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context) => formatCurrency(context.parsed.y),
          },
        },
      },
      scales: {
        x: { grid: { display: false } },
        y: {
          beginAtZero: true,
          ticks: { callback: (value) => `$${value}` },
        },
      },
    }),
    []
  );

  if (forbidden) {
    return (
      <div className="mx-auto max-w-3xl py-16">
        <Card>
          <CardContent className="flex flex-col items-center p-10 text-center">
            <AlertTriangle className="mb-4 h-9 w-9 text-amber-500" />
            <h1 className="text-xl font-semibold">Admin access required</h1>
            <p className="mt-2 text-sm text-muted-foreground">
              Estimated spend contains organization-wide workload and workspace
              information and is available only to admins.
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  const totals = data?.totals || {};
  const today = data?.days?.[data.days.length - 1];
  const kubernetesSeconds = data?.excluded_by_reason?.kubernetes || 0;
  const lastSuccess = data?.last_successful_refresh_at
    ? new Date(data.last_successful_refresh_at * 1000)
    : null;
  const workloads = (data?.workloads || []).slice(0, 20);
  const clouds = data?.clouds || [];
  const totalCost = Number(totals.estimated_cost || 0);

  return (
    <div className="mx-auto max-w-7xl space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-2xl font-semibold tracking-tight">
              Estimated compute cost
            </h1>
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-950/50 dark:text-amber-200">
              Estimate
            </span>
          </div>
          <p className="mt-1 text-sm text-muted-foreground">
            Pay-as-you-go-equivalent compute cost from SkyPilot machine uptime,
            grouped by UTC day.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="inline-flex rounded-md border bg-background p-1">
            {RANGE_OPTIONS.map((days) => (
              <button
                key={days}
                type="button"
                onClick={() => setRangeDays(days)}
                className={`rounded px-3 py-1.5 text-sm transition-colors ${
                  rangeDays === days
                    ? 'bg-blue-600 text-white'
                    : 'text-muted-foreground hover:bg-muted'
                }`}
              >
                {days}d
              </button>
            ))}
          </div>
          <Button variant="outline" onClick={fetchData} disabled={loading}>
            <RefreshCw
              className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`}
            />
            Refresh
          </Button>
        </div>
      </div>

      <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-100">
        Kubernetes usage and reservation, Savings Plan, or committed-use
        adjustments are not included. Storage, network, credits, and taxes are
        also outside this estimate.
      </div>

      {error && (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/30 dark:text-red-200">
          {error.message}
        </div>
      )}

      {data?.stale && data?.as_of && (
        <div className="rounded-lg border border-orange-200 bg-orange-50 px-4 py-3 text-sm text-orange-900 dark:border-orange-900 dark:bg-orange-950/30 dark:text-orange-100">
          The estimator is behind. Showing the last completed snapshot from{' '}
          {lastSuccess?.toLocaleString()}.
        </div>
      )}

      {!loading && data && !data.as_of ? (
        <EmptyEstimate />
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard
              title={`${rangeDays}-day estimate`}
              value={formatCurrency(totalCost)}
              detail="Compute catalog rates only"
              icon={DollarSign}
            />
            <MetricCard
              title="Today (UTC)"
              value={formatCurrency(today?.estimated_cost)}
              detail="Updates about every five minutes"
              icon={Clock3}
            />
            <MetricCard
              title="Priced machine time"
              value={formatHours(totals.priced_machine_seconds)}
              detail="Node count × observed uptime"
              icon={Server}
            />
            <MetricCard
              title="Excluded / unpriced"
              value={formatHours(totals.excluded_machine_seconds)}
              detail={`${formatHours(kubernetesSeconds)} Kubernetes`}
              icon={AlertTriangle}
            />
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Daily estimate</CardTitle>
              <CardDescription>
                Machine uptime split at UTC midnight. The current day is
                partial.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-80">
                <Bar data={chartData} options={chartOptions} />
              </div>
            </CardContent>
          </Card>

          <div className="grid gap-6 xl:grid-cols-[2fr_1fr]">
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Top workloads</CardTitle>
                <CardDescription>
                  Dedicated managed jobs include provisioning and recovery
                  uptime. Shared machines remain attributed to their pool.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Workload</TableHead>
                        <TableHead className="text-right">
                          Machine time
                        </TableHead>
                        <TableHead className="text-right">Estimate</TableHead>
                        <TableHead className="text-right">Share</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {workloads.length === 0 ? (
                        <TableRow>
                          <TableCell
                            colSpan={4}
                            className="py-8 text-center text-muted-foreground"
                          >
                            No priced workloads in this range.
                          </TableCell>
                        </TableRow>
                      ) : (
                        workloads.map((workload) => {
                          const cost = Number(workload.estimated_cost || 0);
                          const share =
                            totalCost > 0 ? (cost / totalCost) * 100 : 0;
                          return (
                            <TableRow
                              key={`${workload.workload_type}:${workload.workload_id}`}
                            >
                              <TableCell className="max-w-md truncate font-medium">
                                {workloadLabel(workload)}
                              </TableCell>
                              <TableCell className="text-right text-muted-foreground">
                                <div>
                                  {formatHours(workload.priced_machine_seconds)}
                                </div>
                                {workload.excluded_machine_seconds > 0 && (
                                  <div className="text-xs">
                                    {formatHours(
                                      workload.excluded_machine_seconds
                                    )}{' '}
                                    excluded
                                  </div>
                                )}
                              </TableCell>
                              <TableCell className="text-right font-medium">
                                {formatCurrency(cost)}
                              </TableCell>
                              <TableCell className="text-right text-muted-foreground">
                                {share.toFixed(1)}%
                              </TableCell>
                            </TableRow>
                          );
                        })
                      )}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-lg">By cloud</CardTitle>
                <CardDescription>
                  Catalog-priced compute in the selected range.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {clouds.length === 0 ? (
                  <p className="py-8 text-center text-sm text-muted-foreground">
                    No cloud breakdown yet.
                  </p>
                ) : (
                  clouds.map((cloud) => {
                    const cost = Number(cloud.estimated_cost || 0);
                    const width = totalCost > 0 ? (cost / totalCost) * 100 : 0;
                    return (
                      <div key={cloud.cloud || 'unknown'}>
                        <div className="mb-1.5 flex items-center justify-between gap-4 text-sm">
                          <span className="font-medium">
                            {cloud.cloud || 'Unknown'}
                          </span>
                          <span>{formatCurrency(cost)}</span>
                        </div>
                        <div className="h-2 overflow-hidden rounded-full bg-muted">
                          <div
                            className="h-full rounded-full bg-sky-500"
                            style={{
                              width: `${Math.max(width, cost > 0 ? 2 : 0)}%`,
                            }}
                          />
                        </div>
                        {cloud.excluded_machine_seconds > 0 && (
                          <p className="mt-1 text-xs text-muted-foreground">
                            {formatHours(cloud.excluded_machine_seconds)}{' '}
                            excluded
                          </p>
                        )}
                      </div>
                    );
                  })
                )}
              </CardContent>
            </Card>
          </div>
        </>
      )}

      <div className="flex flex-col gap-1 border-t pt-4 text-xs text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
        <span>
          Snapshot:{' '}
          {lastSuccess ? lastSuccess.toLocaleString() : 'waiting for rollup'}
          {!data?.backfill_complete && ' · historical backfill in progress'}
        </span>
        <span>
          Page refreshed:{' '}
          {lastFetchedAt ? lastFetchedAt.toLocaleTimeString() : '—'}
        </span>
      </div>
    </div>
  );
}
