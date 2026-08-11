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
  ChevronDown,
  ChevronRight,
  LoaderCircle,
  RefreshCw,
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
import { getEstimatedSpendDrilldown } from '@/data/connectors/estimated_spend';

const PAGE_LIMIT = 50;
const ROOT_KEY = 'root';

function ownerLabel(row) {
  return row.user_name || row.user_hash || 'Unknown owner';
}

function nodeLabel(level, row, workloadLabel) {
  if (level === 'owner') return ownerLabel(row);
  if (level === 'workload') return workloadLabel(row);
  if (level === 'task') return `Task ${row.workload_task_id}`;
  return row.cluster_name || row.cluster_hash || 'Unknown attempt';
}

function nodeKey(level, row, parentKey = ROOT_KEY) {
  if (level === 'owner') {
    return `${parentKey}/owner:${
      row.owner_unknown ? 'unknown' : row.user_hash
    }`;
  }
  if (level === 'workload') {
    return `${parentKey}/workload:${row.workload_type}:${
      row.workload_id ?? ''
    }`;
  }
  if (level === 'task') {
    return `${parentKey}/task:${row.workload_task_id}`;
  }
  return `${parentKey}/cluster:${row.cluster_hash}`;
}

function ownerScope(row) {
  if (row.owner_unknown) return { owner_unknown: true };
  return { owner_user_hash: row.user_hash };
}

function workloadScope(scope, row) {
  const nextScope = {
    ...scope,
    workload_type: row.workload_type,
  };
  if (row.workload_id !== null && row.workload_id !== undefined) {
    nextScope.workload_id = row.workload_id;
  }
  return nextScope;
}

function childDescriptor(level, row, scope) {
  if (level === 'owner') {
    return { level: 'workload', scope: ownerScope(row) };
  }
  if (level === 'workload') {
    const hasCompleteTaskAttribution =
      Number(row.unknown_task_cluster_count || 0) === 0;
    return {
      level:
        Number(row.task_count || 0) > 1 && hasCompleteTaskAttribution
          ? 'task'
          : 'cluster',
      scope: workloadScope(scope, row),
    };
  }
  if (level === 'task') {
    return {
      level: 'cluster',
      scope: {
        ...scope,
        workload_task_id: row.workload_task_id,
      },
    };
  }
  return null;
}

function childSummary(level, row) {
  if (level === 'owner') {
    return `${Number(row.workload_count || 0).toLocaleString()} workloads · ${Number(
      row.cluster_count || 0
    ).toLocaleString()} attempts`;
  }
  if (level === 'workload') {
    const taskCount = Number(row.task_count || 0);
    const attemptCount = Number(row.cluster_count || 0);
    const unknownTaskCount = Number(row.unknown_task_cluster_count || 0);
    if (taskCount > 1 && unknownTaskCount === 0) {
      return `${taskCount.toLocaleString()} tasks · ${attemptCount.toLocaleString()} attempts`;
    }
    if (unknownTaskCount > 0 && taskCount > 1) {
      return `${attemptCount.toLocaleString()} attempts · task grouping incomplete`;
    }
    return `${attemptCount.toLocaleString()} attempts`;
  }
  if (level === 'task') {
    return `${Number(row.cluster_count || 0).toLocaleString()} attempts`;
  }
  return row.workspace ? `Workspace · ${row.workspace}` : 'Physical attempt';
}

function initialPage() {
  return {
    rows: [],
    total: 0,
    hasMore: false,
    loading: false,
    error: null,
  };
}

export function SpendAttributionTable({
  dateRange,
  snapshotAt,
  totalCost,
  fallbackGroups,
  formatCurrency,
  formatHours,
  workloadLabel,
}) {
  const [pages, setPages] = useState({});
  const [expanded, setExpanded] = useState(new Set());
  const [unsupported, setUnsupported] = useState(false);
  const rangeKey = `${dateRange.startDate}:${dateRange.endDate}:${dateRange.days}:${snapshotAt}`;
  const activeRangeKey = useRef(rangeKey);
  const activeRequests = useRef(new Map());

  const loadPage = useCallback(
    async ({ pageKey, level, scope, offset = 0 }) => {
      const requestKey = `${rangeKey}:${pageKey}:${offset}`;
      if (activeRequests.current.has(requestKey)) return;
      const requestToken = {};
      activeRequests.current.set(requestKey, requestToken);
      setPages((current) => ({
        ...current,
        [pageKey]: {
          ...(current[pageKey] || initialPage()),
          loading: true,
          error: null,
        },
      }));
      try {
        const result = await getEstimatedSpendDrilldown({
          level,
          days: dateRange.days,
          dateRange,
          scope,
          offset,
          limit: PAGE_LIMIT,
        });
        if (
          activeRangeKey.current !== rangeKey ||
          activeRequests.current.get(requestKey) !== requestToken
        ) {
          return;
        }
        setPages((current) => {
          const existingRows = offset > 0 ? current[pageKey]?.rows || [] : [];
          return {
            ...current,
            [pageKey]: {
              rows: [...existingRows, ...(result.rows || [])],
              total: Number(result.total || 0),
              hasMore: Boolean(result.has_more),
              loading: false,
              error: null,
            },
          };
        });
      } catch (error) {
        if (
          activeRangeKey.current !== rangeKey ||
          activeRequests.current.get(requestKey) !== requestToken
        ) {
          return;
        }
        if (pageKey === ROOT_KEY && [404, 405].includes(error.status)) {
          setUnsupported(true);
          setPages((current) => ({
            ...current,
            [pageKey]: {
              ...(current[pageKey] || initialPage()),
              loading: false,
              error: null,
            },
          }));
        } else {
          setPages((current) => ({
            ...current,
            [pageKey]: {
              ...(current[pageKey] || initialPage()),
              loading: false,
              error,
            },
          }));
        }
      } finally {
        if (activeRequests.current.get(requestKey) === requestToken) {
          activeRequests.current.delete(requestKey);
        }
      }
    },
    [dateRange, rangeKey]
  );

  useEffect(() => {
    const requests = activeRequests.current;
    activeRangeKey.current = rangeKey;
    requests.clear();
    setPages({});
    setExpanded(new Set());
    setUnsupported(false);
    loadPage({ pageKey: ROOT_KEY, level: 'owner', scope: {} });
    return () => {
      requests.clear();
    };
  }, [loadPage, rangeKey]);

  const toggleRow = useCallback(
    ({ key, descriptor }) => {
      const willExpand = !expanded.has(key);
      setExpanded((current) => {
        const next = new Set(current);
        if (next.has(key)) {
          next.delete(key);
        } else {
          next.add(key);
        }
        return next;
      });
      if (willExpand && !pages[key] && descriptor) {
        loadPage({
          pageKey: key,
          level: descriptor.level,
          scope: descriptor.scope,
        });
      }
    },
    [expanded, loadPage, pages]
  );

  const renderRows = useCallback(
    (rows, level, scope = {}, depth = 0, parentKey = ROOT_KEY) =>
      rows.flatMap((row) => {
        const key = nodeKey(level, row, parentKey);
        const descriptor = childDescriptor(level, row, scope);
        const hasChildren =
          descriptor !== null && Number(row.cluster_count || 0) > 0;
        const isExpanded = expanded.has(key);
        const label = nodeLabel(level, row, workloadLabel);
        const childPage = pages[key] || initialPage();
        const cost = Number(row.estimated_cost || 0);
        const share = totalCost > 0 ? (cost / totalCost) * 100 : 0;
        const rendered = [
          <TableRow key={key} data-level={level}>
            <TableCell className="max-w-md font-medium">
              <div
                className="flex min-w-0 items-start gap-2"
                style={{ paddingLeft: `${depth * 24}px` }}
              >
                {hasChildren ? (
                  <button
                    type="button"
                    aria-label={`${isExpanded ? 'Collapse' : 'Expand'} ${label}`}
                    aria-expanded={isExpanded}
                    className="mt-0.5 rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                    onClick={() => toggleRow({ key, descriptor })}
                  >
                    {isExpanded ? (
                      <ChevronDown className="h-4 w-4" />
                    ) : (
                      <ChevronRight className="h-4 w-4" />
                    )}
                  </button>
                ) : (
                  <span className="h-5 w-5 shrink-0" />
                )}
                <div className="min-w-0">
                  <div className="truncate">{label}</div>
                  <div className="text-xs font-normal text-muted-foreground">
                    {childSummary(level, row)}
                  </div>
                </div>
              </div>
            </TableCell>
            <TableCell className="text-right text-muted-foreground">
              <div>{formatHours(row.priced_machine_seconds)}</div>
              {Number(row.excluded_machine_seconds || 0) > 0 && (
                <div className="text-xs">
                  {formatHours(row.excluded_machine_seconds)} excluded
                </div>
              )}
            </TableCell>
            <TableCell className="text-right font-medium">
              {formatCurrency(cost)}
            </TableCell>
            <TableCell className="text-right text-emerald-700 dark:text-emerald-300">
              {formatCurrency(row.spot_estimated_cost)}
            </TableCell>
            <TableCell className="text-right text-sky-700 dark:text-sky-300">
              {formatCurrency(row.on_demand_estimated_cost)}
            </TableCell>
            <TableCell className="text-right text-muted-foreground">
              {share.toFixed(1)}%
            </TableCell>
          </TableRow>,
        ];
        if (!isExpanded) return rendered;

        if (childPage.loading && childPage.rows.length === 0) {
          rendered.push(
            <TableRow key={`${key}:loading`}>
              <TableCell colSpan={6}>
                <div
                  className="flex items-center gap-2 py-2 text-sm text-muted-foreground"
                  style={{ paddingLeft: `${(depth + 1) * 24}px` }}
                >
                  <LoaderCircle className="h-4 w-4 animate-spin" />
                  Loading details...
                </div>
              </TableCell>
            </TableRow>
          );
        } else {
          rendered.push(
            ...renderRows(
              childPage.rows,
              descriptor.level,
              descriptor.scope,
              depth + 1,
              key
            )
          );
          if (childPage.error) {
            rendered.push(
              <TableRow key={`${key}:error`}>
                <TableCell colSpan={6}>
                  <div
                    className="flex items-center gap-3 py-2 text-sm text-red-700 dark:text-red-300"
                    style={{ paddingLeft: `${(depth + 1) * 24}px` }}
                  >
                    <span>{childPage.error.message}</span>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        loadPage({
                          pageKey: key,
                          level: descriptor.level,
                          scope: descriptor.scope,
                          offset: childPage.rows.length,
                        })
                      }
                    >
                      <RefreshCw className="mr-1 h-3.5 w-3.5" />
                      Retry
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            );
          } else if (childPage.hasMore) {
            rendered.push(
              <TableRow key={`${key}:more`}>
                <TableCell colSpan={6}>
                  <div style={{ paddingLeft: `${(depth + 1) * 24}px` }}>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      disabled={childPage.loading}
                      onClick={() =>
                        loadPage({
                          pageKey: key,
                          level: descriptor.level,
                          scope: descriptor.scope,
                          offset: childPage.rows.length,
                        })
                      }
                    >
                      {childPage.loading && (
                        <LoaderCircle className="mr-2 h-4 w-4 animate-spin" />
                      )}
                      Load more
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            );
          }
        }
        return rendered;
      }),
    [
      expanded,
      formatCurrency,
      formatHours,
      loadPage,
      pages,
      toggleRow,
      totalCost,
      workloadLabel,
    ]
  );

  const rootPage = pages[ROOT_KEY] || initialPage();
  const fallbackRows = useMemo(
    () =>
      fallbackGroups.map((group) => ({
        ...group,
        owner_unknown: !group.user_hash,
      })),
    [fallbackGroups]
  );
  const displayedRows = unsupported ? fallbackRows : rootPage.rows;

  return (
    <Card id="spend-ownership">
      <CardHeader>
        <CardTitle className="text-lg">Spend ownership</CardTitle>
        <CardDescription>
          Expand an owner into logical workloads, managed-job tasks when
          present, and the physical attempts that incurred cost. Legacy managed
          rows are grouped only when their parent job cannot be proven.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {unsupported && (
          <div className="mb-4 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-100">
            Detailed attribution is unavailable on this server. Showing the flat
            owner summary.
          </div>
        )}
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Owner / workload / attempt</TableHead>
                <TableHead className="text-right">Machine time</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead className="text-right">Spot</TableHead>
                <TableHead className="text-right">On-demand</TableHead>
                <TableHead className="text-right">Share</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rootPage.loading && displayedRows.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={6}
                    className="py-8 text-center text-muted-foreground"
                  >
                    <div className="flex items-center justify-center gap-2">
                      <LoaderCircle className="h-4 w-4 animate-spin" />
                      Loading owners...
                    </div>
                  </TableCell>
                </TableRow>
              ) : rootPage.error ? (
                <TableRow>
                  <TableCell colSpan={6} className="py-8 text-center">
                    <div className="space-y-3">
                      <p className="text-sm text-red-700 dark:text-red-300">
                        {rootPage.error.message}
                      </p>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() =>
                          loadPage({
                            pageKey: ROOT_KEY,
                            level: 'owner',
                            scope: {},
                          })
                        }
                      >
                        <RefreshCw className="mr-2 h-4 w-4" />
                        Retry
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ) : displayedRows.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={6}
                    className="py-8 text-center text-muted-foreground"
                  >
                    No priced owners in this range.
                  </TableCell>
                </TableRow>
              ) : unsupported ? (
                displayedRows.map((row) => {
                  const cost = Number(row.estimated_cost || 0);
                  const share = totalCost > 0 ? (cost / totalCost) * 100 : 0;
                  return (
                    <TableRow key={row.user_hash || 'unknown'}>
                      <TableCell className="font-medium">
                        {ownerLabel(row)}
                      </TableCell>
                      <TableCell className="text-right text-muted-foreground">
                        {formatHours(row.priced_machine_seconds)}
                      </TableCell>
                      <TableCell className="text-right font-medium">
                        {formatCurrency(cost)}
                      </TableCell>
                      <TableCell className="text-right text-emerald-700 dark:text-emerald-300">
                        {formatCurrency(row.spot_estimated_cost)}
                      </TableCell>
                      <TableCell className="text-right text-sky-700 dark:text-sky-300">
                        {formatCurrency(row.on_demand_estimated_cost)}
                      </TableCell>
                      <TableCell className="text-right text-muted-foreground">
                        {share.toFixed(1)}%
                      </TableCell>
                    </TableRow>
                  );
                })
              ) : (
                renderRows(displayedRows, 'owner')
              )}
              {!unsupported && rootPage.hasMore && (
                <TableRow>
                  <TableCell colSpan={6}>
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      disabled={rootPage.loading}
                      onClick={() =>
                        loadPage({
                          pageKey: ROOT_KEY,
                          level: 'owner',
                          scope: {},
                          offset: rootPage.rows.length,
                        })
                      }
                    >
                      {rootPage.loading && (
                        <LoaderCircle className="mr-2 h-4 w-4 animate-spin" />
                      )}
                      Load more owners
                    </Button>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

SpendAttributionTable.propTypes = {
  dateRange: PropTypes.shape({
    startDate: PropTypes.string.isRequired,
    endDate: PropTypes.string.isRequired,
    days: PropTypes.number.isRequired,
  }).isRequired,
  snapshotAt: PropTypes.number.isRequired,
  totalCost: PropTypes.number.isRequired,
  fallbackGroups: PropTypes.arrayOf(PropTypes.object).isRequired,
  formatCurrency: PropTypes.func.isRequired,
  formatHours: PropTypes.func.isRequired,
  workloadLabel: PropTypes.func.isRequired,
};
