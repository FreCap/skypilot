'use client';

import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
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
  Activity,
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
import { SpendAttributionTable } from '@/components/spend-attribution-table';
import { getEstimatedSpend } from '@/data/connectors/estimated_spend';
import { getCurrentUserRole } from '@/data/connectors/client';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

const MAX_RANGE_DAYS = 90;
const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;
const RANGE_OPTIONS = [
  { key: 'today', label: 'Today', days: 1, endOffset: 0 },
  { key: 'yesterday', label: 'Yesterday', days: 1, endOffset: -1 },
  { key: '7d', label: '7d', days: 7, endOffset: 0 },
  { key: '30d', label: '30d', days: 30, endOffset: 0 },
  { key: '90d', label: '90d', days: 90, endOffset: 0 },
];
const AUTO_REFRESH_MS = 60 * 1000;
const GROUP_OPTIONS = [
  { value: 'job', label: 'Job / workload' },
  { value: 'user', label: 'User' },
  { value: 'purchase_option', label: 'Purchase option' },
];
const SERIES_COLORS = [
  ['rgba(14, 165, 233, 0.78)', 'rgb(2, 132, 199)'],
  ['rgba(99, 102, 241, 0.78)', 'rgb(79, 70, 229)'],
  ['rgba(168, 85, 247, 0.78)', 'rgb(147, 51, 234)'],
  ['rgba(236, 72, 153, 0.78)', 'rgb(219, 39, 119)'],
  ['rgba(249, 115, 22, 0.78)', 'rgb(234, 88, 12)'],
  ['rgba(234, 179, 8, 0.78)', 'rgb(202, 138, 4)'],
  ['rgba(34, 197, 94, 0.78)', 'rgb(22, 163, 74)'],
  ['rgba(20, 184, 166, 0.78)', 'rgb(13, 148, 136)'],
];
const PURCHASE_OPTION_COLORS = {
  spot: ['rgba(34, 197, 94, 0.78)', 'rgb(22, 163, 74)'],
  on_demand: ['rgba(14, 165, 233, 0.78)', 'rgb(2, 132, 199)'],
  unknown: ['rgba(148, 163, 184, 0.78)', 'rgb(100, 116, 139)'],
};
const OTHER_COLOR = ['rgba(148, 163, 184, 0.62)', 'rgb(100, 116, 139)'];

export function formatCurrency(value) {
  const number = Number(value || 0);
  if (number > 0 && number < 0.01) return '<$0.01';
  if (number >= 10000) {
    return `$${number.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  }
  return `$${number.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

export function formatCostPerRequest(value) {
  if (value === null || value === undefined) return 'N/A';
  const number = Number(value);
  if (!Number.isFinite(number)) return 'N/A';
  if (number > 0 && number < 0.0001) return '<$0.0001';
  return `$${number.toLocaleString(undefined, {
    minimumFractionDigits: 4,
    maximumFractionDigits: 4,
  })}`;
}

export function utcDateString(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

export function shiftUtcDate(dateString, days) {
  const date = new Date(`${dateString}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return utcDateString(date);
}

function inclusiveRangeDays(startDate, endDate) {
  const start = Date.parse(`${startDate}T00:00:00Z`);
  const end = Date.parse(`${endDate}T00:00:00Z`);
  return Math.floor((end - start) / MILLISECONDS_PER_DAY) + 1;
}

export function validateDateRange(startDate, endDate, today = utcDateString()) {
  if (!startDate || !endDate) return 'Choose both a start and end date.';
  if (startDate > endDate) {
    return 'The start date must be on or before the end date.';
  }
  if (endDate > today) return 'The end date cannot be in the future.';
  const earliestDate = shiftUtcDate(today, -(MAX_RANGE_DAYS - 1));
  if (startDate < earliestDate) {
    return `Choose a date within the last ${MAX_RANGE_DAYS} UTC days.`;
  }
  if (inclusiveRangeDays(startDate, endDate) > MAX_RANGE_DAYS) {
    return `The selected range cannot exceed ${MAX_RANGE_DAYS} UTC days.`;
  }
  return null;
}

function rangeForPreset(option, today = utcDateString()) {
  const endDate = shiftUtcDate(today, option.endOffset);
  return {
    startDate: shiftUtcDate(endDate, -(option.days - 1)),
    endDate,
    days: option.days,
    preset: option.key,
  };
}

function formatUtcDate(dateString, options) {
  return new Date(`${dateString}T00:00:00Z`).toLocaleDateString(undefined, {
    timeZone: 'UTC',
    ...options,
  });
}

function estimateTitle(dateRange) {
  if (dateRange.preset === 'today') return 'Today estimate (UTC)';
  if (dateRange.preset === 'yesterday') return 'Yesterday estimate (UTC)';
  if (dateRange.preset?.endsWith('d')) {
    return `${dateRange.days}-day estimate`;
  }
  if (dateRange.startDate === dateRange.endDate) {
    return `${formatUtcDate(dateRange.startDate, {
      month: 'short',
      day: 'numeric',
    })} estimate (UTC)`;
  }
  return 'Selected range estimate';
}

export function formatHours(seconds) {
  const hours = Number(seconds || 0) / 3600;
  return `${hours.toLocaleString(undefined, {
    maximumFractionDigits: hours >= 100 ? 0 : 1,
  })} h`;
}

export function formatRequestCount(value) {
  return Number(value || 0).toLocaleString(undefined, {
    maximumFractionDigits: 0,
  });
}

export function workloadLabel(workload) {
  const type = workload.workload_type || 'cluster';
  const id = workload.workload_id || 'unattributed';
  if (type === 'managed_job') return `Managed job #${id}`;
  if (type === 'service') return `Service · ${id}`;
  if (type === 'pool') return `Pool · ${id}`;
  if (type === 'controller') return `Platform · ${id}`;
  if (type === 'managed' || type === 'managed_unattributed') {
    return 'Legacy managed, parent unknown';
  }
  return `Cluster · ${id}`;
}

export function breakdownLabel(groupBy, group) {
  if (group.is_other) return 'Other';
  if (groupBy === 'job') return workloadLabel(group);
  if (groupBy === 'user') {
    return group.user_name || group.user_hash || 'Unknown';
  }
  if (group.purchase_option === 'spot') return 'Spot';
  if (group.purchase_option === 'on_demand') return 'On-demand';
  return 'Unknown / unpriced';
}

function breakdownKey(groupBy, group) {
  if (group.is_other) return 'other';
  if (groupBy === 'job') {
    return `${group.workload_type || 'cluster'}:${group.workload_id || ''}`;
  }
  if (groupBy === 'user') return `user:${group.user_hash || 'unknown'}`;
  return group.purchase_option || 'unknown';
}

function seriesColor(groupBy, series, index) {
  if (series.is_other) return OTHER_COLOR;
  if (groupBy === 'purchase_option') {
    return PURCHASE_OPTION_COLORS[series.purchase_option] || OTHER_COLOR;
  }
  return SERIES_COLORS[index % SERIES_COLORS.length];
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

function ServiceRequestsCard({
  serviceRequests,
  days,
  startDate,
  endDate,
  todayUtc,
}) {
  const totalRequestCount = Number(serviceRequests?.total_request_count || 0);
  const coveragePartial =
    serviceRequests?.coverage_start_utc &&
    serviceRequests.coverage_start_utc >
      Date.parse(`${startDate}T00:00:00Z`) / 1000;
  const ratioCoverageStartUtc =
    serviceRequests?.services?.[0]?.ratio_coverage_start_utc;
  const ratioCoveragePartial =
    ratioCoverageStartUtc &&
    ratioCoverageStartUtc > Date.parse(`${startDate}T00:00:00Z`) / 1000;
  const chartData = useMemo(() => {
    const series = serviceRequests?.series || [];
    return {
      labels: days.map((day) =>
        new Date(`${day.date}T00:00:00Z`).toLocaleDateString(undefined, {
          month: 'short',
          day: 'numeric',
          timeZone: 'UTC',
        })
      ),
      datasets: series.map((service, index) => {
        const [backgroundColor, borderColor] = service.is_other
          ? OTHER_COLOR
          : SERIES_COLORS[index % SERIES_COLORS.length];
        return {
          label: service.is_other ? 'Other' : service.service_name,
          data: service.request_count_by_day || [],
          isOther: Boolean(service.is_other),
          estimatedCostByDay: service.estimated_cost_by_day || [],
          estimatedCostPerRequestByDay:
            service.estimated_cost_per_request_by_day || [],
          backgroundColor,
          borderColor,
          borderWidth: 1,
          borderRadius: 3,
          maxBarThickness: 36,
          stack: 'service-requests',
        };
      }),
    };
  }, [days, serviceRequests]);
  const chartOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: {
          display: chartData.datasets.length > 1,
          position: 'bottom',
        },
        tooltip: {
          filter: (context) => Number(context.parsed.y) > 0,
          callbacks: {
            label: (context) => {
              const labels = [
                `${context.dataset.label}: ${formatRequestCount(
                  context.parsed.y
                )} requests`,
              ];
              if (context.dataset.isOther) return labels;
              const estimatedCost =
                context.dataset.estimatedCostByDay?.[context.dataIndex];
              const costPerRequest =
                context.dataset.estimatedCostPerRequestByDay?.[
                  context.dataIndex
                ];
              labels.push(
                `Est. compute: ${
                  costPerRequest !== null && costPerRequest !== undefined
                    ? formatCurrency(estimatedCost)
                    : 'N/A'
                }`
              );
              labels.push(
                `Est. compute cost / request: ${formatCostPerRequest(
                  costPerRequest
                )}`
              );
              return labels;
            },
          },
        },
      },
      scales: {
        x: { grid: { display: false }, stacked: true },
        y: {
          beginAtZero: true,
          stacked: true,
          ticks: {
            precision: 0,
            callback: (value) => formatRequestCount(value),
          },
        },
      },
    }),
    [chartData.datasets.length]
  );

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <CardTitle className="flex items-center gap-2 text-lg">
              <Activity className="h-5 w-5 text-blue-600" />
              Daily requests by service
            </CardTitle>
            <CardDescription className="mt-1">
              One admitted or capacity-rejected inbound request counts once.
              Internal replica retries do not add requests; client retries are
              separate requests. Cost/request is estimated replica compute, with
              reserved Kubernetes capacity valued at zero; genuinely unknown
              pricing remains unavailable.{' '}
              {endDate === todayUtc && 'The current UTC day is partial.'}
            </CardDescription>
          </div>
          {serviceRequests.available && (
            <div className="sm:text-right">
              <p className="text-2xl font-semibold tracking-tight">
                {formatRequestCount(totalRequestCount)}
              </p>
              <p className="text-xs text-muted-foreground">
                Requests in selected range
              </p>
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {!serviceRequests.available ? (
          <div className="rounded-lg border border-dashed p-8 text-center text-sm text-muted-foreground">
            Daily service request history is unavailable on this server.
          </div>
        ) : (
          <>
            {coveragePartial && (
              <div className="rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-900 dark:border-blue-900 dark:bg-blue-950/30 dark:text-blue-100">
                Request history begins{' '}
                {new Date(
                  serviceRequests.coverage_start_utc * 1000
                ).toLocaleString(undefined, {
                  timeZone: 'UTC',
                  timeZoneName: 'short',
                })}
                . The earlier portion of this range has no retained request
                data.
              </div>
            )}
            {ratioCoveragePartial && (
              <div className="rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-900 dark:border-blue-900 dark:bg-blue-950/30 dark:text-blue-100">
                Cost/request uses complete request-history days beginning{' '}
                {new Date(ratioCoverageStartUtc * 1000).toLocaleDateString(
                  undefined,
                  {
                    timeZone: 'UTC',
                    timeZoneName: 'short',
                  }
                )}
                . Its request denominator can be smaller than the selected-range
                request total.
              </div>
            )}
            {totalRequestCount === 0 ? (
              <div className="rounded-lg border border-dashed p-8 text-center text-sm text-muted-foreground">
                No service requests recorded in this range.
              </div>
            ) : (
              <div className="h-80">
                <Bar data={chartData} options={chartOptions} />
              </div>
            )}
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Service</TableHead>
                    <TableHead className="text-right">Requests</TableHead>
                    <TableHead className="text-right">
                      Est. compute cost / request
                    </TableHead>
                    <TableHead className="text-right">Share</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(serviceRequests.services || []).length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={4}
                        className="py-8 text-center text-muted-foreground"
                      >
                        No services with request activity in this range.
                      </TableCell>
                    </TableRow>
                  ) : (
                    serviceRequests.services.map((service) => {
                      const count = Number(service.request_count || 0);
                      const share =
                        totalRequestCount > 0
                          ? (count / totalRequestCount) * 100
                          : 0;
                      return (
                        <TableRow key={service.service_name}>
                          <TableCell className="font-medium">
                            {service.service_name}
                          </TableCell>
                          <TableCell className="text-right font-medium">
                            {formatRequestCount(count)}
                          </TableCell>
                          <TableCell className="text-right font-medium">
                            <div>
                              {formatCostPerRequest(
                                service.estimated_cost_per_request
                              )}
                            </div>
                            {service.cost_coverage === 'partial' && (
                              <div className="text-xs font-normal text-muted-foreground">
                                unpriced capacity
                              </div>
                            )}
                            {Number(service.ratio_request_count) > 0 &&
                              Number(service.ratio_request_count) !== count && (
                                <div className="text-xs font-normal text-muted-foreground">
                                  based on{' '}
                                  {formatRequestCount(
                                    service.ratio_request_count
                                  )}{' '}
                                  requests
                                </div>
                              )}
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
          </>
        )}
      </CardContent>
    </Card>
  );
}

ServiceRequestsCard.propTypes = {
  serviceRequests: PropTypes.object.isRequired,
  days: PropTypes.arrayOf(PropTypes.object).isRequired,
  startDate: PropTypes.string.isRequired,
  endDate: PropTypes.string.isRequired,
  todayUtc: PropTypes.string.isRequired,
};

export function EstimatedSpend() {
  const [dateRange, setDateRange] = useState(() =>
    rangeForPreset(RANGE_OPTIONS.find((option) => option.key === '30d'))
  );
  const [draftStartDate, setDraftStartDate] = useState(
    () => rangeForPreset(RANGE_OPTIONS[3]).startDate
  );
  const [draftEndDate, setDraftEndDate] = useState(
    () => rangeForPreset(RANGE_OPTIONS[3]).endDate
  );
  const [dateRangeError, setDateRangeError] = useState(null);
  const [groupBy, setGroupBy] = useState('user');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [forbidden, setForbidden] = useState(false);
  const [lastFetchedAt, setLastFetchedAt] = useState(null);
  const requestState = useRef({ generation: 0, active: null });

  const fetchData = useCallback(async () => {
    const requestKey = JSON.stringify([
      dateRange.startDate,
      dateRange.endDate,
      dateRange.days,
      groupBy,
    ]);
    if (requestState.current.active?.key === requestKey) return;

    const generation = ++requestState.current.generation;
    requestState.current.active = { key: requestKey, generation };
    setLoading(true);
    setError(null);
    try {
      const role = await getCurrentUserRole();
      if (generation !== requestState.current.generation) return;
      if (role.roleFetchFailed) {
        // A failed role lookup is an error, not a permission denial: keep
        // the error UI so the next refresh cycle retries.
        throw new Error('Failed to fetch current role');
      }
      if (role.role !== 'admin') {
        setForbidden(true);
        setData(null);
        return;
      }
      setForbidden(false);
      const estimate = await getEstimatedSpend(dateRange.days, groupBy, {
        startDate: dateRange.startDate,
        endDate: dateRange.endDate,
      });
      if (generation !== requestState.current.generation) return;
      if (!estimate.group_by && groupBy !== 'job') {
        setGroupBy('job');
      }
      setData(estimate);
      setLastFetchedAt(new Date());
    } catch (fetchError) {
      if (generation !== requestState.current.generation) return;
      if (fetchError.status === 403) {
        setForbidden(true);
      } else {
        setError(fetchError);
      }
    } finally {
      if (generation === requestState.current.generation) {
        setLoading(false);
      }
      if (requestState.current.active?.generation === generation) {
        requestState.current.active = null;
      }
    }
  }, [dateRange.days, dateRange.endDate, dateRange.startDate, groupBy]);

  const selectPreset = useCallback((option) => {
    const nextRange = rangeForPreset(option);
    setDateRange(nextRange);
    setDraftStartDate(nextRange.startDate);
    setDraftEndDate(nextRange.endDate);
    setDateRangeError(null);
  }, []);

  const applyCustomRange = useCallback(
    (event) => {
      event.preventDefault();
      const validationError = validateDateRange(draftStartDate, draftEndDate);
      if (validationError) {
        setDateRangeError(validationError);
        return;
      }
      setDateRange({
        startDate: draftStartDate,
        endDate: draftEndDate,
        days: inclusiveRangeDays(draftStartDate, draftEndDate),
        preset: null,
      });
      setDateRangeError(null);
    },
    [draftEndDate, draftStartDate]
  );

  useEffect(() => {
    fetchData();
    const timer = setInterval(fetchData, AUTO_REFRESH_MS);
    const state = requestState.current;
    return () => {
      clearInterval(timer);
      state.generation += 1;
      state.active = null;
    };
  }, [fetchData]);

  const chartData = useMemo(() => {
    const days = data?.days || [];
    const selectedGroupBy = data?.group_by || 'job';
    const groupedSeries = data?.series || [];
    const datasets = groupedSeries.length
      ? groupedSeries.map((series, index) => {
          const [backgroundColor, borderColor] = seriesColor(
            selectedGroupBy,
            series,
            index
          );
          return {
            label: breakdownLabel(selectedGroupBy, series),
            data: series.estimated_cost_by_day || [],
            backgroundColor,
            borderColor,
            borderWidth: 1,
            borderRadius: 3,
            maxBarThickness: 36,
            stack: 'estimated-spend',
          };
        })
      : [
          {
            label: 'Estimated compute cost',
            data: days.map((day) => day.estimated_cost),
            backgroundColor: 'rgba(14, 165, 233, 0.75)',
            borderColor: 'rgb(2, 132, 199)',
            borderWidth: 1,
            borderRadius: 4,
            maxBarThickness: 36,
          },
        ];
    return {
      labels: days.map((day) =>
        new Date(`${day.date}T00:00:00Z`).toLocaleDateString(undefined, {
          month: 'short',
          day: 'numeric',
          timeZone: 'UTC',
        })
      ),
      datasets,
    };
  }, [data]);

  const chartOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: { display: chartData.datasets.length > 1, position: 'bottom' },
        tooltip: {
          filter: (context) => Number(context.parsed.y) > 0,
          callbacks: {
            label: (context) =>
              `${context.dataset.label}: ${formatCurrency(context.parsed.y)}`,
          },
        },
      },
      scales: {
        x: { grid: { display: false }, stacked: true },
        y: {
          beginAtZero: true,
          stacked: true,
          ticks: { callback: (value) => `$${value}` },
        },
      },
    }),
    [chartData.datasets.length]
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
  const latestDay = data?.days?.[data.days.length - 1];
  const kubernetesSeconds = data?.excluded_by_reason?.kubernetes || 0;
  const lastSuccess = data?.last_successful_refresh_at
    ? new Date(data.last_successful_refresh_at * 1000)
    : null;
  const displayedGroupBy = data?.group_by || 'job';
  const supportsBreakdowns = Boolean(
    data?.group_by && Array.isArray(data?.groups)
  );
  const groups = supportsBreakdowns
    ? data.groups
    : displayedGroupBy === 'job'
      ? data?.workloads || []
      : [];
  const groupOption = GROUP_OPTIONS.find(
    (option) => option.value === displayedGroupBy
  );
  const showPurchaseColumns =
    supportsBreakdowns && displayedGroupBy !== 'purchase_option';
  const clouds = data?.clouds || [];
  const totalCost = Number(totals.estimated_cost || 0);
  const serviceRequests = data?.service_requests;
  const hasOtherSeries = (data?.series || []).some((series) => series.is_other);
  const todayUtc = utcDateString();
  const earliestDate = shiftUtcDate(todayUtc, -(MAX_RANGE_DAYS - 1));
  const selectedRangeDetail = `${formatUtcDate(dateRange.startDate, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })} to ${formatUtcDate(dateRange.endDate, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })}`;
  const latestDayTitle = latestDay
    ? `${formatUtcDate(latestDay.date, {
        month: 'short',
        day: 'numeric',
      })} (UTC)`
    : 'Latest selected day';

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
          {(!data || supportsBreakdowns) && (
            <label className="flex items-center gap-2 text-sm text-muted-foreground">
              <span>Group by</span>
              <select
                aria-label="Group spend by"
                value={groupBy}
                onChange={(event) => setGroupBy(event.target.value)}
                className="h-9 rounded-md border border-input bg-background px-3 text-sm text-foreground shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
              >
                {GROUP_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          )}
          <div className="inline-flex rounded-md border bg-background p-1">
            {RANGE_OPTIONS.map((option) => (
              <button
                key={option.key}
                type="button"
                onClick={() => selectPreset(option)}
                className={`rounded px-3 py-1.5 text-sm transition-colors ${
                  dateRange.preset === option.key
                    ? 'bg-blue-600 text-white'
                    : 'text-muted-foreground hover:bg-muted'
                }`}
              >
                {option.label}
              </button>
            ))}
          </div>
          <form
            className="flex flex-wrap items-center gap-2"
            onSubmit={applyCustomRange}
          >
            <label>
              <span className="sr-only">Start date (UTC)</span>
              <input
                aria-label="Start date (UTC)"
                type="date"
                min={earliestDate}
                max={todayUtc}
                value={draftStartDate}
                onChange={(event) => setDraftStartDate(event.target.value)}
                className="h-9 rounded-md border border-input bg-background px-2 text-sm text-foreground shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </label>
            <span className="text-sm text-muted-foreground">to</span>
            <label>
              <span className="sr-only">End date (UTC)</span>
              <input
                aria-label="End date (UTC)"
                type="date"
                min={earliestDate}
                max={todayUtc}
                value={draftEndDate}
                onChange={(event) => setDraftEndDate(event.target.value)}
                className="h-9 rounded-md border border-input bg-background px-2 text-sm text-foreground shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </label>
            <Button type="submit" variant="outline" disabled={loading}>
              Apply
            </Button>
          </form>
          <Button variant="outline" onClick={fetchData} disabled={loading}>
            <RefreshCw
              className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`}
            />
            Refresh
          </Button>
        </div>
      </div>

      {dateRangeError && (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/30 dark:text-red-200">
          {dateRangeError}
        </div>
      )}

      <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-100">
        Kubernetes usage is not included in spend totals. Service cost/request
        separately values reserved Kubernetes capacity at zero. Other
        reservation, Savings Plan, or committed-use adjustments, plus storage,
        network, credits, and taxes, are outside this estimate.
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
              title={estimateTitle(dateRange)}
              value={formatCurrency(totalCost)}
              detail={`${selectedRangeDetail} UTC`}
              icon={DollarSign}
            />
            <MetricCard
              title={latestDayTitle}
              value={formatCurrency(latestDay?.estimated_cost)}
              detail={
                dateRange.endDate === todayUtc
                  ? 'Updates about every five minutes'
                  : 'Latest day in the selected range'
              }
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
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div>
                  <CardTitle className="text-lg">
                    Daily estimate by{' '}
                    {groupOption?.label.toLowerCase() || 'group'}
                  </CardTitle>
                  <CardDescription className="mt-1">
                    Stacked catalog-priced cost split at UTC midnight.{' '}
                    {dateRange.endDate === todayUtc &&
                      'The current day is partial; '}
                    {hasOtherSeries
                      ? `the top 8 groups are charted individually. Other is the chart-only remainder; ${
                          displayedGroupBy === 'user'
                            ? 'use the owner hierarchy below to inspect every paginated workload and attempt.'
                            : 'switch to User to inspect every paginated workload and attempt.'
                        }`
                      : 'all groups in this range fit in the chart.'}
                  </CardDescription>
                </div>
                {hasOtherSeries && (
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      setGroupBy('user');
                      setTimeout(() => {
                        document
                          .getElementById('spend-ownership')
                          ?.scrollIntoView?.({ behavior: 'smooth' });
                      }, 0);
                    }}
                  >
                    Inspect Other
                  </Button>
                )}
              </div>
            </CardHeader>
            <CardContent>
              <div className="h-80">
                <Bar data={chartData} options={chartOptions} />
              </div>
            </CardContent>
          </Card>

          <div className="grid gap-6 xl:grid-cols-[2fr_1fr]">
            {displayedGroupBy === 'user' && supportsBreakdowns ? (
              <SpendAttributionTable
                dateRange={dateRange}
                snapshotAt={Number(data?.as_of || 0)}
                totalCost={totalCost}
                fallbackGroups={groups}
                formatCurrency={formatCurrency}
                formatHours={formatHours}
                workloadLabel={workloadLabel}
              />
            ) : (
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">
                    {displayedGroupBy === 'purchase_option'
                      ? 'Spend'
                      : 'Top spend'}{' '}
                    by {groupOption?.label.toLowerCase() || 'group'}
                  </CardTitle>
                  <CardDescription>
                    {displayedGroupBy === 'job'
                      ? 'Up to 50 workloads. Managed jobs include provisioning and recovery uptime; shared machines remain attributed to their pool.'
                      : displayedGroupBy === 'user'
                        ? 'Up to 50 users. Ownership follows the user recorded when each cluster was launched.'
                        : 'Catalog-priced compute split between spot and on-demand capacity.'}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>
                            {displayedGroupBy === 'job'
                              ? 'Job / workload'
                              : displayedGroupBy === 'user'
                                ? 'User'
                                : 'Purchase option'}
                          </TableHead>
                          <TableHead className="text-right">
                            Machine time
                          </TableHead>
                          <TableHead className="text-right">Total</TableHead>
                          {showPurchaseColumns && (
                            <>
                              <TableHead className="text-right">Spot</TableHead>
                              <TableHead className="text-right">
                                On-demand
                              </TableHead>
                            </>
                          )}
                          <TableHead className="text-right">Share</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {groups.length === 0 ? (
                          <TableRow>
                            <TableCell
                              colSpan={showPurchaseColumns ? 6 : 4}
                              className="py-8 text-center text-muted-foreground"
                            >
                              No priced groups in this range.
                            </TableCell>
                          </TableRow>
                        ) : (
                          groups.map((group) => {
                            const cost = Number(group.estimated_cost || 0);
                            const share =
                              totalCost > 0 ? (cost / totalCost) * 100 : 0;
                            return (
                              <TableRow
                                key={breakdownKey(displayedGroupBy, group)}
                              >
                                <TableCell className="max-w-md truncate font-medium">
                                  {breakdownLabel(displayedGroupBy, group)}
                                </TableCell>
                                <TableCell className="text-right text-muted-foreground">
                                  <div>
                                    {formatHours(group.priced_machine_seconds)}
                                  </div>
                                  {group.excluded_machine_seconds > 0 && (
                                    <div className="text-xs">
                                      {formatHours(
                                        group.excluded_machine_seconds
                                      )}{' '}
                                      excluded
                                    </div>
                                  )}
                                </TableCell>
                                <TableCell className="text-right font-medium">
                                  {formatCurrency(cost)}
                                </TableCell>
                                {showPurchaseColumns && (
                                  <>
                                    <TableCell className="text-right text-emerald-700 dark:text-emerald-300">
                                      {formatCurrency(
                                        group.spot_estimated_cost
                                      )}
                                    </TableCell>
                                    <TableCell className="text-right text-sky-700 dark:text-sky-300">
                                      {formatCurrency(
                                        group.on_demand_estimated_cost
                                      )}
                                    </TableCell>
                                  </>
                                )}
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
            )}

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

      {serviceRequests && (
        <ServiceRequestsCard
          serviceRequests={serviceRequests}
          days={data?.days || []}
          startDate={dateRange.startDate}
          endDate={dateRange.endDate}
          todayUtc={todayUtc}
        />
      )}

      <div className="flex flex-col gap-1 border-t pt-4 text-xs text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
        <span>
          Snapshot:{' '}
          {lastSuccess ? lastSuccess.toLocaleString() : 'waiting for rollup'}
          {!data?.backfill_complete && ' · historical backfill in progress'}
        </span>
        <span>
          Page refreshed:{' '}
          {lastFetchedAt ? lastFetchedAt.toLocaleTimeString() : 'not yet'}
        </span>
      </div>
    </div>
  );
}
