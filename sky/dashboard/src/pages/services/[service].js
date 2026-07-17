import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
} from 'react';
import { CircularProgress } from '@mui/material';
import { useRouter } from 'next/router';
import Link from 'next/link';
import Head from 'next/head';
import {
  CheckIcon,
  ChevronDownIcon,
  ChevronRightIcon,
  CopyIcon,
  RotateCwIcon,
} from 'lucide-react';
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from '@/components/ui/table';
import { Card } from '@/components/ui/card';
import { StatusBadge } from '@/components/elements/StatusBadge';
import { getServices } from '@/data/connectors/services';
import dashboardCache from '@/lib/cache';
import {
  CustomTooltip as Tooltip,
  NonCapitalizedTooltip,
  formatDuration,
  formatFullTimestamp,
} from '@/components/utils';
import { EndpointCell, formatUptime } from '@/components/services';
import { ServeHistorySection } from '@/components/serve-history';
import { ServiceVersionHistory } from '@/components/service-version-history';
import { useMobile } from '@/hooks/useMobile';
import { formatYaml } from '@/lib/yamlUtils';
import { YamlCodeBlock } from '@/components/ui/yaml-code-block';
import { CLOUD_CANONICALIZATIONS } from '@/data/connectors/constants';

const REPLICA_PLACEMENT_COLUMNS = [
  { key: 'pending', label: 'Pending' },
  { key: 'provisioning', label: 'Provisioning' },
  { key: 'initializing', label: 'Initializing' },
  { key: 'ready', label: 'Ready' },
  { key: 'notReady', label: 'Not ready' },
  { key: 'stopping', label: 'Stopping' },
  { key: 'error', label: 'Error' },
  { key: 'other', label: 'Other' },
];

const REPLICA_ERROR_STATUSES = new Set([
  'FAILED',
  'FAILED_CLEANUP',
  'FAILED_INITIAL_DELAY',
  'FAILED_PROBING',
  'FAILED_PROVISION',
  'UNKNOWN',
]);

const SERVICE_HISTORY_HOURS = 24;

function getReplicaPlacementStatusBucket(status) {
  if (REPLICA_ERROR_STATUSES.has(status)) return 'error';
  switch (status) {
    case 'PENDING':
      return 'pending';
    case 'PROVISIONING':
      return 'provisioning';
    case 'STARTING':
      return 'initializing';
    case 'READY':
      return 'ready';
    case 'NOT_READY':
      return 'notReady';
    case 'SHUTTING_DOWN':
    case 'PREEMPTED':
      return 'stopping';
    default:
      return 'other';
  }
}

export function getReplicaPlacementBreakdown(replicas) {
  const replicaList = Array.isArray(replicas) ? replicas : [];
  const rowsByPlacement = new Map();

  replicaList.forEach((replica) => {
    const rawCloud = replica.cloud?.trim();
    const cloud = rawCloud
      ? CLOUD_CANONICALIZATIONS[rawCloud.toLowerCase()] || rawCloud
      : 'Unknown';
    const region = replica.region?.trim() || 'Pending placement';
    const placementKey = `${cloud}\u0000${region}`;
    let row = rowsByPlacement.get(placementKey);
    if (!row) {
      row = {
        cloud,
        region,
        pending: 0,
        provisioning: 0,
        initializing: 0,
        ready: 0,
        notReady: 0,
        stopping: 0,
        error: 0,
        other: 0,
        total: 0,
      };
      rowsByPlacement.set(placementKey, row);
    }
    row[getReplicaPlacementStatusBucket(replica.status)] += 1;
    row.total += 1;
  });

  return Array.from(rowsByPlacement.values()).sort(
    (left, right) =>
      left.cloud.localeCompare(right.cloud) ||
      left.region.localeCompare(right.region)
  );
}

export function useServiceDetails({ serviceName }) {
  const [serviceData, setServiceData] = useState(null);
  const [replicaHistory, setReplicaHistory] = useState(null);
  const [loading, setLoading] = useState(true);
  const [replicasLoading, setReplicasLoading] = useState(true);
  const [historyLoading, setHistoryLoading] = useState(true);
  const requestVersionRef = useRef(0);

  // Two-phase load, both scoped to THIS service (the old implementation
  // fetched every service with full replica info just to display one):
  //   1. summary_only — near-instant; renders the header/summary card.
  //   2. full — per-replica table; takes tens of seconds at fleet scale,
  //      fills in when it lands.
  const fetchData = useCallback(async () => {
    if (!serviceName) return;
    const requestVersion = requestVersionRef.current + 1;
    requestVersionRef.current = requestVersion;
    setLoading(true);
    setReplicasLoading(true);
    setHistoryLoading(true);
    setReplicaHistory(null);
    const isCurrentRequest = () => requestVersionRef.current === requestVersion;
    // Ordering within THIS invocation only: once the full result has
    // landed, a later-resolving summary must not overwrite it — but a
    // fresh summary must still replace whatever an earlier invocation
    // left behind.
    let fullLanded = false;
    const summaryPromise = dashboardCache
      .get(getServices, [
        {
          serviceNames: [serviceName],
          summaryOnly: true,
          includeTargetReplicas: true,
          historyHours: SERVICE_HISTORY_HOURS,
        },
      ])
      .then(({ services }) => {
        if (!isCurrentRequest()) return;
        const found = (services || []).find((s) => s.name === serviceName);
        setReplicaHistory(found?.replicaHistory || null);
        if (fullLanded) return;
        setServiceData(found || null);
      })
      .catch((error) => {
        console.error('Failed to fetch service summary:', error);
      })
      .finally(() => {
        if (isCurrentRequest()) {
          setLoading(false);
          setHistoryLoading(false);
        }
      });
    const fullPromise = dashboardCache
      .get(getServices, [{ serviceNames: [serviceName] }])
      .then(({ services }) => {
        if (!isCurrentRequest()) return;
        const found = (services || []).find((s) => s.name === serviceName);
        fullLanded = true;
        setServiceData(found || null);
      })
      .catch((error) => {
        console.error('Failed to fetch service replicas:', error);
      })
      .finally(() => {
        if (isCurrentRequest()) {
          setReplicasLoading(false);
        }
      });
    await Promise.allSettled([summaryPromise, fullPromise]);
  }, [serviceName]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    if (!serviceName) return undefined;
    let active = true;
    let refreshInFlight = false;
    const historyArgs = [
      {
        serviceNames: [serviceName],
        summaryOnly: true,
        includeTargetReplicas: false,
        historyHours: SERVICE_HISTORY_HOURS,
      },
    ];
    const refreshHistory = async () => {
      if (refreshInFlight) return;
      refreshInFlight = true;
      const requestVersion = requestVersionRef.current;
      const isCurrentRequest = () =>
        active && requestVersionRef.current === requestVersion;
      setHistoryLoading(true);
      dashboardCache.invalidate(getServices, historyArgs);
      try {
        const { services } = await dashboardCache.get(getServices, historyArgs);
        if (!isCurrentRequest()) return;
        const found = (services || []).find((s) => s.name === serviceName);
        setReplicaHistory(found?.replicaHistory || null);
      } catch (error) {
        if (isCurrentRequest()) {
          console.error('Failed to refresh service history:', error);
        }
      } finally {
        refreshInFlight = false;
        if (isCurrentRequest()) {
          setHistoryLoading(false);
        }
      }
    };
    const interval = setInterval(refreshHistory, 60 * 1000);
    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [serviceName]);

  const refreshData = useCallback(async () => {
    // Drop every args-keyed getServices variant (summary and full).
    dashboardCache.invalidateFunction(getServices);
    await fetchData();
  }, [fetchData]);

  return {
    serviceData,
    replicaHistory,
    loading,
    replicasLoading,
    historyLoading,
    refreshData,
  };
}

function ServiceDetails() {
  const router = useRouter();
  const { service: serviceName } = router.query;

  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const isMobile = useMobile();
  const {
    serviceData,
    replicaHistory,
    loading,
    replicasLoading,
    historyLoading,
    refreshData,
  } = useServiceDetails({ serviceName });

  useEffect(() => {
    if (!loading && isInitialLoad) {
      setIsInitialLoad(false);
    }
  }, [loading, isInitialLoad]);

  const handleManualRefresh = async () => {
    setIsRefreshing(true);
    await refreshData();
    setIsRefreshing(false);
  };

  if (!router.isReady) {
    return <div>Loading...</div>;
  }

  const title = serviceName
    ? `Service: ${serviceName} | SkyPilot Dashboard`
    : 'Service Details | SkyPilot Dashboard';

  return (
    <>
      <Head>
        <title>{title}</title>
      </Head>
      <>
        <div className="flex items-center justify-between mb-4 h-5">
          <div className="text-base flex items-center">
            <Link href="/services" className="text-sky-blue hover:underline">
              Services
            </Link>
            <span className="mx-2 text-gray-500">&rsaquo;</span>
            <Link
              href={`/services/${encodeURIComponent(serviceName)}`}
              className="text-sky-blue hover:underline"
            >
              {serviceName}
            </Link>
          </div>

          <div className="text-sm flex items-center">
            {(loading || isRefreshing) && (
              <div className="flex items-center mr-4">
                <CircularProgress size={15} className="mt-0" />
                <span className="ml-2 text-gray-500">Loading...</span>
              </div>
            )}
            {serviceData && (
              <Tooltip
                content="Refresh"
                className="text-sm text-muted-foreground"
              >
                <button
                  onClick={handleManualRefresh}
                  disabled={loading || isRefreshing}
                  className="text-sky-blue hover:text-sky-blue-bright font-medium inline-flex items-center"
                >
                  <RotateCwIcon className="w-4 h-4 mr-1.5" />
                  {!isMobile && <span>Refresh</span>}
                </button>
              </Tooltip>
            )}
          </div>
        </div>

        {loading && isInitialLoad ? (
          <div className="flex justify-center items-center py-12">
            <CircularProgress size={24} className="mr-2" />
            <span className="text-gray-500">Loading service details...</span>
          </div>
        ) : serviceData ? (
          <>
            <ServiceDetailCard
              serviceData={serviceData}
              requestHistory={replicaHistory}
              pricingLoading={replicasLoading && serviceData.summaryOnly}
            />
            <ServiceVersionHistory
              serviceName={serviceName}
              onElectionComplete={refreshData}
            />
            <ServeHistorySection
              key={serviceName}
              history={replicaHistory}
              loading={historyLoading}
            />
            <ReplicaPlacementCard
              replicas={serviceData.replicas}
              loading={replicasLoading && serviceData.summaryOnly}
            />
            <ReplicasCard
              replicas={serviceData.replicas}
              loading={replicasLoading && serviceData.summaryOnly}
            />
          </>
        ) : (
          <div className="flex justify-center items-center py-12">
            <span className="text-gray-500">Service not found.</span>
          </div>
        )}
      </>
    </>
  );
}

function formatUsd(value) {
  return Number(value).toLocaleString(undefined, {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function formatRequestRate(value) {
  return `${Number(value).toLocaleString(undefined, {
    minimumFractionDigits: value < 1 ? 2 : 1,
    maximumFractionDigits: value < 1 ? 3 : 1,
  })} req/s`;
}

export function ServiceDetailCard({
  serviceData,
  requestHistory = null,
  pricingLoading = false,
}) {
  const hourlyCostDetails = [];
  if (serviceData.spotHourlyCost > 0) {
    hourlyCostDetails.push(`Spot ${formatUsd(serviceData.spotHourlyCost)}/hr`);
  }
  if (serviceData.onDemandHourlyCost > 0) {
    hourlyCostDetails.push(
      `On-demand ${formatUsd(serviceData.onDemandHourlyCost)}/hr`
    );
  }
  if (serviceData.hourlyCostExcludedReplicaCount > 0) {
    hourlyCostDetails.push(
      `${serviceData.hourlyCostExcludedReplicaCount} unpriced replica${
        serviceData.hourlyCostExcludedReplicaCount === 1 ? '' : 's'
      }`
    );
  }
  if (serviceData.estimatedHourlyCost != null) {
    hourlyCostDetails.push('Current catalog, compute only');
  }

  const requestDetails = [];
  const usesLogicalReplicas = serviceData.replicaUnit === 'logical';
  if (
    serviceData.recentRequestCount != null &&
    serviceData.requestWindowSeconds != null
  ) {
    requestDetails.push(
      `${serviceData.recentRequestCount.toLocaleString()} requests in ${serviceData.requestWindowSeconds}s`
    );
  }
  if (
    requestHistory?.available !== false &&
    requestHistory?.requestsLastHour != null
  ) {
    requestDetails.push(
      `${requestHistory.requestsLastHour.toLocaleString()} requests in last hour`
    );
  }
  if (serviceData.inFlightRequests != null) {
    requestDetails.push(`${serviceData.inFlightRequests} in flight`);
  }
  if (serviceData.requestQueueDepth != null) {
    requestDetails.push(`${serviceData.requestQueueDepth} queued`);
  }
  if (serviceData.rejectedRequests != null) {
    requestDetails.push(`${serviceData.rejectedRequests} rejected`);
  }
  if (serviceData.requestStatsAgeSeconds != null) {
    requestDetails.push(
      `activity report ${Math.round(serviceData.requestStatsAgeSeconds)}s old`
    );
  }

  return (
    <div className="mb-6">
      <div className="rounded-lg border bg-card text-card-foreground shadow-sm">
        <div className="flex items-center justify-between px-4 pt-4">
          <h3 className="text-lg font-semibold">Details</h3>
        </div>
        <div className="p-4">
          <div className="grid grid-cols-2 gap-6">
            <div>
              <div className="text-gray-600 font-medium text-base">Status</div>
              <div className="text-base mt-1">
                <StatusBadge status={serviceData.status} />
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">Name</div>
              <div className="text-base mt-1">{serviceData.name}</div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">Uptime</div>
              <div className="text-base mt-1">
                {formatUptime(serviceData.uptime)}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                {usesLogicalReplicas
                  ? 'Logical replicas (ready/total)'
                  : 'Replicas (ready/total)'}
              </div>
              <div className="text-base mt-1">
                {serviceData.replicasReady}/{serviceData.replicasTotal}
                {serviceData.replicasFailed > 0 && (
                  <span className="text-red-700">
                    {' '}
                    (+{serviceData.replicasFailed} failed)
                  </span>
                )}
                {serviceData.targetReplicas != null && (
                  <span className="text-gray-500">
                    {' '}
                    (target: {serviceData.targetReplicas})
                  </span>
                )}
              </div>
              {usesLogicalReplicas && (
                <div className="text-sm text-gray-500 mt-1">
                  {serviceData.physicalReplicasReady}/
                  {serviceData.physicalReplicasTotal} physical backends ready
                  {serviceData.physicalReplicasFailed > 0 && (
                    <span className="text-red-700">
                      {' '}
                      (+{serviceData.physicalReplicasFailed} failed)
                    </span>
                  )}
                </div>
              )}
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Endpoint
              </div>
              <div className="text-base mt-1">
                <EndpointCell endpoint={serviceData.endpoint} />
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">Policy</div>
              <div className="text-base mt-1">{serviceData.policy || '-'}</div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Load Balancing Policy
              </div>
              <div className="text-base mt-1">
                {serviceData.loadBalancingPolicy || '-'}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Requested Resources
              </div>
              <div className="text-base mt-1">
                {serviceData.requestedResources || '-'}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Estimated compute cost
              </div>
              <div className="text-base mt-1">
                {serviceData.estimatedHourlyCost != null
                  ? `${formatUsd(serviceData.estimatedHourlyCost)}/hr`
                  : pricingLoading
                    ? 'Loading replica prices...'
                    : '-'}
              </div>
              {hourlyCostDetails.length > 0 && (
                <div className="text-xs text-gray-500 mt-1">
                  {hourlyCostDetails.join(' · ')}
                </div>
              )}
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Recent request rate
              </div>
              <div className="text-base mt-1">
                {serviceData.requestRate != null
                  ? formatRequestRate(serviceData.requestRate)
                  : '-'}
              </div>
              {requestDetails.length > 0 && (
                <div className="text-xs text-gray-500 mt-1">
                  {requestDetails.join(' · ')}
                </div>
              )}
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Estimated compute / 1K requests
              </div>
              <div className="text-base mt-1">
                {serviceData.costPerThousandRequests != null
                  ? formatUsd(serviceData.costPerThousandRequests)
                  : '-'}
              </div>
              <div className="text-xs text-gray-500 mt-1">
                Current fleet cost at the recent request rate
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Elected Version
              </div>
              <div className="text-base mt-1">
                {serviceData.electedVersion ?? '-'}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Active Versions
              </div>
              <div className="text-base mt-1">
                {serviceData.activeVersions &&
                serviceData.activeVersions.length > 0
                  ? serviceData.activeVersions.join(', ')
                  : '-'}
              </div>
            </div>
            {serviceData.serviceYaml && (
              <ServiceYamlSection serviceYaml={serviceData.serviceYaml} />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// Collapsible YAML viewer, mirroring the managed-jobs detail page. The
// YAML arrives already redacted from the server (secrets masked); this
// only handles display formatting and copy.
function ServiceYamlSection({ serviceYaml }) {
  const [isYamlExpanded, setIsYamlExpanded] = useState(false);
  const [isCopied, setIsCopied] = useState(false);

  const formattedYaml = formatYaml(serviceYaml);

  const copyYamlToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(formattedYaml);
      setIsCopied(true);
      setTimeout(() => setIsCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy YAML to clipboard:', err);
    }
  };

  return (
    <div className="col-span-2">
      <div className="flex items-center">
        <button
          onClick={() => setIsYamlExpanded(!isYamlExpanded)}
          className="flex items-center text-left focus:outline-none text-gray-700 hover:text-gray-900 transition-colors duration-200"
        >
          {isYamlExpanded ? (
            <ChevronDownIcon className="w-4 h-4 mr-1" />
          ) : (
            <ChevronRightIcon className="w-4 h-4 mr-1" />
          )}
          <span className="text-base">Show SkyPilot YAML</span>
        </button>

        <Tooltip
          content={isCopied ? 'Copied!' : 'Copy YAML'}
          className="text-muted-foreground"
        >
          <button
            onClick={copyYamlToClipboard}
            className="flex items-center text-gray-500 hover:text-gray-700 transition-colors duration-200 p-1 ml-2"
          >
            {isCopied ? (
              <CheckIcon className="w-4 h-4 text-green-600" />
            ) : (
              <CopyIcon className="w-4 h-4" />
            )}
          </button>
        </Tooltip>
      </div>

      {isYamlExpanded && (
        <div className="mt-2">
          <YamlCodeBlock value={formattedYaml} readOnly />
        </div>
      )}
    </div>
  );
}

export function ReplicaPlacementCard({ replicas, loading }) {
  const rows = getReplicaPlacementBreakdown(replicas);

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h3 className="text-lg font-semibold">Machines by region</h3>
          <p className="text-sm text-gray-500">
            Physical backends by Kubernetes context or cloud region, grouped by
            lifecycle state.
          </p>
        </div>
        {loading && (
          <span className="text-sm text-gray-500 whitespace-nowrap">
            <CircularProgress size={14} className="mr-2" />
            Loading machines…
          </span>
        )}
      </div>
      <Card>
        <div className="overflow-x-auto rounded-lg">
          <Table className="min-w-full">
            <TableHeader>
              <TableRow>
                <TableHead className="whitespace-nowrap">Provider</TableHead>
                <TableHead className="whitespace-nowrap">
                  Region / context
                </TableHead>
                {REPLICA_PLACEMENT_COLUMNS.map((column) => (
                  <TableHead
                    key={column.key}
                    className="whitespace-nowrap text-right"
                  >
                    {column.label}
                  </TableHead>
                ))}
                <TableHead className="whitespace-nowrap text-right">
                  Total
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.length > 0 ? (
                rows.map((row) => (
                  <TableRow key={`${row.cloud}/${row.region}`}>
                    <TableCell className="font-medium">{row.cloud}</TableCell>
                    <TableCell>{row.region}</TableCell>
                    {REPLICA_PLACEMENT_COLUMNS.map((column) => (
                      <TableCell
                        key={column.key}
                        className={
                          row[column.key] > 0
                            ? 'text-right tabular-nums'
                            : 'text-right tabular-nums text-gray-400'
                        }
                      >
                        {row[column.key]}
                      </TableCell>
                    ))}
                    <TableCell className="text-right tabular-nums font-semibold">
                      {row.total}
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell
                    colSpan={REPLICA_PLACEMENT_COLUMNS.length + 3}
                    className="text-center py-6 text-gray-500"
                  >
                    {loading ? 'Loading machine placement…' : 'No replicas.'}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}

const REPLICA_SORT_VALUE = {
  id: (replica) => replica.id,
  status: (replica) => replica.status,
  version: (replica) => replica.version,
  resources: (replica) => replica.resources_str_full || replica.resources_str,
  hourlyCost: (replica) => replica.hourlyCost,
  region: (replica) => replica.region,
  endpoint: (replica) => replica.endpoint,
  timeToReadySeconds: (replica) => replica.timeToReadySeconds,
  launched_at: (replica) => replica.launched_at,
};

const replicaSortCollator = new Intl.Collator(undefined, {
  numeric: true,
  sensitivity: 'base',
});

export function sortReplicas(replicas, sortConfig) {
  const replicaList = Array.isArray(replicas) ? replicas : [];
  const getValue = REPLICA_SORT_VALUE[sortConfig.key];
  if (!getValue) return replicaList;

  return replicaList
    .map((replica, index) => ({ replica, index }))
    .sort((left, right) => {
      const leftValue = getValue(left.replica);
      const rightValue = getValue(right.replica);
      const leftMissing = leftValue === null || leftValue === undefined;
      const rightMissing = rightValue === null || rightValue === undefined;
      if (leftMissing || rightMissing) {
        if (leftMissing !== rightMissing) return leftMissing ? 1 : -1;
        return left.index - right.index;
      }

      const comparison =
        typeof leftValue === 'number' && typeof rightValue === 'number'
          ? leftValue - rightValue
          : replicaSortCollator.compare(String(leftValue), String(rightValue));
      if (comparison === 0) return left.index - right.index;
      return sortConfig.direction === 'ascending' ? comparison : -comparison;
    })
    .map(({ replica }) => replica);
}

export function ReplicasCard({ replicas, loading }) {
  const [sortConfig, setSortConfig] = useState({
    key: 'id',
    direction: 'ascending',
  });
  const sortedReplicas = useMemo(
    () => sortReplicas(replicas, sortConfig),
    [replicas, sortConfig]
  );

  const requestSort = (key) => {
    setSortConfig((current) => ({
      key,
      direction:
        current.key === key && current.direction === 'ascending'
          ? 'descending'
          : 'ascending',
    }));
  };

  const sortableHeader = (label, key) => {
    const active = sortConfig.key === key;
    const direction = active ? sortConfig.direction : null;
    return (
      <TableHead className="whitespace-nowrap" aria-sort={direction || 'none'}>
        <button
          type="button"
          className="inline-flex w-full items-center gap-1 text-left hover:text-sky-blue"
          onClick={() => requestSort(key)}
        >
          {label}
          {active && (
            <span aria-hidden="true">
              {direction === 'ascending' ? '↑' : '↓'}
            </span>
          )}
        </button>
      </TableHead>
    );
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-lg font-semibold">Replicas</h3>
        {loading && (
          <span className="text-sm text-gray-500">
            <CircularProgress size={14} className="mr-2" />
            Loading replicas…
          </span>
        )}
      </div>
      <Card>
        <div className="overflow-x-auto rounded-lg">
          <Table className="min-w-full">
            <TableHeader>
              <TableRow>
                {sortableHeader('ID', 'id')}
                {sortableHeader('Status', 'status')}
                {sortableHeader('Version', 'version')}
                {sortableHeader('Resources', 'resources')}
                {sortableHeader('Est. $/hr', 'hourlyCost')}
                {sortableHeader('Region', 'region')}
                {sortableHeader('Endpoint', 'endpoint')}
                {sortableHeader('Ready in', 'timeToReadySeconds')}
                {sortableHeader('Launched', 'launched_at')}
              </TableRow>
            </TableHeader>
            <TableBody>
              {sortedReplicas.length > 0 ? (
                sortedReplicas.map((replica) => (
                  <TableRow key={replica.id}>
                    <TableCell>{replica.id}</TableCell>
                    <TableCell>
                      <StatusBadge status={replica.status} />
                    </TableCell>
                    <TableCell>{replica.version ?? '-'}</TableCell>
                    <TableCell>
                      {replica.resources_str ? (
                        <NonCapitalizedTooltip
                          content={
                            replica.resources_str_full || replica.resources_str
                          }
                          className="text-sm text-muted-foreground"
                        >
                          <span>
                            {replica.infra
                              ? `${replica.infra} (${replica.resources_str})`
                              : replica.resources_str}
                            {replica.is_spot ? ' [spot]' : ''}
                          </span>
                        </NonCapitalizedTooltip>
                      ) : (
                        '-'
                      )}
                    </TableCell>
                    <TableCell>
                      {replica.hourlyCost != null
                        ? formatUsd(replica.hourlyCost)
                        : '-'}
                    </TableCell>
                    <TableCell>{replica.region || '-'}</TableCell>
                    <TableCell>
                      <EndpointCell endpoint={replica.endpoint} />
                    </TableCell>
                    <TableCell>
                      {replica.timeToReadySeconds != null ? (
                        <NonCapitalizedTooltip
                          content={`Ready at ${formatFullTimestamp(
                            new Date(replica.ready_at * 1000)
                          )}`}
                        >
                          <span className="border-b border-dotted border-gray-400 cursor-help">
                            {formatDuration(replica.timeToReadySeconds)}
                          </span>
                        </NonCapitalizedTooltip>
                      ) : (
                        '-'
                      )}
                    </TableCell>
                    <TableCell>
                      {replica.launched_at
                        ? formatFullTimestamp(
                            new Date(replica.launched_at * 1000)
                          )
                        : '-'}
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell
                    colSpan={9}
                    className="text-center py-6 text-gray-500"
                  >
                    No replicas.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}

export default ServiceDetails;
