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
import { ServicePlacement } from '@/components/service-placement';
import { useMobile } from '@/hooks/useMobile';
import { formatYaml } from '@/lib/yamlUtils';
import { YamlCodeBlock } from '@/components/ui/yaml-code-block';
import { CLOUD_CANONICALIZATIONS } from '@/data/connectors/constants';

const REPLICA_PLACEMENT_COLUMNS = [
  { key: 'queuedIntent', label: 'Queued intent' },
  { key: 'providerSetup', label: 'Provider / setup' },
  { key: 'initializingNotReady', label: 'Initializing / not ready' },
  { key: 'ready', label: 'Ready' },
  { key: 'stopping', label: 'Stopping' },
  { key: 'cleanupUncertain', label: 'Cleanup uncertain' },
  { key: 'historicalFailure', label: 'Historical failure' },
  { key: 'other', label: 'Other' },
];

const REPLICA_HISTORICAL_FAILURE_STATUSES = new Set([
  'FAILED',
  'FAILED_INITIAL_DELAY',
  'FAILED_PROBING',
  'FAILED_PROVISION',
  'UNKNOWN',
]);

const SERVICE_HISTORY_HOURS = 24;

function getReplicaPlacementStatusBucket(replica) {
  const { status } = replica;
  if (REPLICA_HISTORICAL_FAILURE_STATUSES.has(status)) {
    return 'historicalFailure';
  }
  switch (status) {
    case 'PENDING':
      return 'queuedIntent';
    case 'PROVISIONING':
      return replica.launched_at === null || replica.launched_at === undefined
        ? 'queuedIntent'
        : 'providerSetup';
    case 'STARTING':
    case 'NOT_READY':
      return 'initializingNotReady';
    case 'READY':
      return 'ready';
    case 'SHUTTING_DOWN':
    case 'PREEMPTED':
      return 'stopping';
    case 'FAILED_CLEANUP':
      return 'cleanupUncertain';
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
        queuedIntent: 0,
        providerSetup: 0,
        initializingNotReady: 0,
        ready: 0,
        stopping: 0,
        cleanupUncertain: 0,
        historicalFailure: 0,
        other: 0,
        currentOrUncertain: 0,
        trackedAttempts: 0,
      };
      rowsByPlacement.set(placementKey, row);
    }
    const bucket = getReplicaPlacementStatusBucket(replica);
    row[bucket] += 1;
    if (bucket !== 'historicalFailure') {
      row.currentOrUncertain += 1;
    }
    row.trackedAttempts += 1;
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
  const refreshInFlightRef = useRef(null);
  const visibleServiceDataRef = useRef(null);
  const summaryArgs = useMemo(
    () => [
      {
        serviceNames: [serviceName],
        summaryOnly: true,
        includeTargetReplicas: true,
        historyHours: SERVICE_HISTORY_HOURS,
      },
    ],
    [serviceName]
  );
  const fullArgs = useMemo(
    () => [{ serviceNames: [serviceName] }],
    [serviceName]
  );

  useEffect(() => {
    visibleServiceDataRef.current = serviceData;
  }, [serviceData]);

  // Two-phase load, both scoped to THIS service (the old implementation
  // fetched every service with full replica info just to display one):
  //   1. summary_only — near-instant; renders the header/summary card.
  //   2. full — per-replica table; takes tens of seconds at fleet scale,
  //      fills in when it lands.
  const fetchData = useCallback(
    ({
      invalidate = false,
      resetHistory = false,
      source = 'refresh',
      supersede = false,
    } = {}) => {
      if (!serviceName) return Promise.resolve();
      const inFlight = refreshInFlightRef.current;
      const hasVisibleCurrentServiceData =
        visibleServiceDataRef.current?.name === serviceName;
      const shouldReuseInFlight =
        inFlight?.serviceName === serviceName &&
        (!supersede ||
          (!hasVisibleCurrentServiceData && inFlight.summaryPending) ||
          inFlight.source === 'manual');
      if (shouldReuseInFlight) {
        return inFlight.promise;
      }
      if (invalidate) {
        dashboardCache.invalidate(getServices, summaryArgs);
        dashboardCache.invalidate(getServices, fullArgs);
      }

      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      setLoading(true);
      setReplicasLoading(true);
      setHistoryLoading(true);
      if (resetHistory) {
        setReplicaHistory(null);
      }
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion;
      let fullLanded = false;
      let refreshPromise;
      refreshPromise = (async () => {
        const summaryPromise = dashboardCache
          .get(getServices, summaryArgs)
          .then(({ services }) => {
            if (!isCurrentRequest()) return;
            const found = (services || []).find((s) => s.name === serviceName);
            setReplicaHistory(found?.replicaHistory || null);
            if (fullLanded) return;
            setServiceData((previous) => {
              if (!found) return null;
              if (
                previous?.name !== serviceName ||
                previous.summaryOnly === true ||
                !Array.isArray(previous.replicas)
              ) {
                return found;
              }
              // Summary mode is the cheap, current view, but it omits the full
              // replica list. Keep the last complete snapshot visible while
              // the corresponding full request is still in flight.
              return {
                ...previous,
                ...found,
                replicas: previous.replicas,
                summaryOnly: false,
              };
            });
          })
          .catch((error) => {
            if (isCurrentRequest()) {
              console.error('Failed to fetch service summary:', error);
            }
          })
          .finally(() => {
            if (refreshInFlightRef.current?.promise === refreshPromise) {
              refreshInFlightRef.current.summaryPending = false;
            }
            if (isCurrentRequest()) {
              setLoading(false);
              setHistoryLoading(false);
            }
          });
        const fullPromise = dashboardCache
          .get(getServices, fullArgs)
          .then(({ services }) => {
            if (!isCurrentRequest()) return;
            const found = (services || []).find((s) => s.name === serviceName);
            fullLanded = true;
            setServiceData(found || null);
          })
          .catch((error) => {
            if (isCurrentRequest()) {
              console.error('Failed to fetch service replicas:', error);
            }
          })
          .finally(() => {
            if (isCurrentRequest()) {
              setReplicasLoading(false);
            }
          });
        await Promise.allSettled([summaryPromise, fullPromise]);
      })().finally(() => {
        if (refreshInFlightRef.current?.promise === refreshPromise) {
          refreshInFlightRef.current = null;
        }
      });
      refreshInFlightRef.current = {
        serviceName,
        promise: refreshPromise,
        source,
        summaryPending: true,
      };
      return refreshPromise;
    },
    [fullArgs, serviceName, summaryArgs]
  );

  useEffect(() => {
    fetchData({ resetHistory: true, source: 'initial' });
    return () => {
      requestVersionRef.current += 1;
      if (refreshInFlightRef.current?.serviceName === serviceName) {
        refreshInFlightRef.current = null;
      }
    };
  }, [fetchData, serviceName]);

  const refreshData = useCallback(
    () => fetchData({ invalidate: true, source: 'manual', supersede: true }),
    [fetchData]
  );

  useEffect(() => {
    if (!serviceName) return undefined;
    const interval = setInterval(() => {
      fetchData({ invalidate: true, source: 'poll' });
    }, 60 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [fetchData, serviceName]);

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
  const activeTab = ['versions', 'placement'].includes(router.query.tab)
    ? router.query.tab
    : 'overview';

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
            {serviceData && activeTab !== 'placement' && (
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

        {serviceData && (
          <div className="mb-4 flex border-b text-sm" role="tablist">
            {[
              { id: 'overview', label: 'Overview', suffix: '' },
              { id: 'versions', label: 'Versions', suffix: '?tab=versions' },
              {
                id: 'placement',
                label: 'Placement',
                suffix: '?tab=placement',
              },
            ].map((tab) => (
              <Link
                key={tab.id}
                role="tab"
                aria-selected={activeTab === tab.id}
                href={`/services/${encodeURIComponent(serviceName)}${tab.suffix}`}
                shallow
                className={`border-b-2 px-4 py-2 font-medium ${
                  activeTab === tab.id
                    ? 'border-sky-blue text-sky-blue'
                    : 'border-transparent text-gray-500 hover:text-gray-800'
                }`}
              >
                {tab.label}
              </Link>
            ))}
          </div>
        )}

        {loading && isInitialLoad ? (
          <div className="flex justify-center items-center py-12">
            <CircularProgress size={24} className="mr-2" />
            <span className="text-gray-500">Loading service details...</span>
          </div>
        ) : serviceData ? (
          activeTab === 'versions' ? (
            <ServiceVersionHistory
              serviceName={serviceName}
              onElectionComplete={refreshData}
            />
          ) : activeTab === 'placement' ? (
            <ServicePlacement serviceName={serviceName} />
          ) : (
            <>
              <ServiceDetailCard
                serviceData={serviceData}
                requestHistory={replicaHistory}
                pricingLoading={replicasLoading && serviceData.summaryOnly}
              />
              <AcceleratorCapacityCard serviceData={serviceData} />
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
          )
        ) : (
          <div className="flex justify-center items-center py-12">
            <span className="text-gray-500">Service not found.</span>
          </div>
        )}
      </>
    </>
  );
}

export function AcceleratorCapacityCard({ serviceData }) {
  const rows = serviceData.acceleratorCapacity || [];
  if (!rows.length) return null;
  return (
    <Card className="mb-6 overflow-hidden">
      <div className="flex items-start justify-between border-b px-4 py-3">
        <div>
          <h3 className="text-lg font-semibold">Capacity by exact card</h3>
          <p className="mt-1 text-sm text-gray-500">
            Demand target assigns flexible work to the cheapest compatible card.
            Warm retention shows work staying on its current card. Only cold
            launch authority requests incremental exact card capacity. Reserved
            fill capacity remains independent. Committed / unready is the
            controller-reported non-ready capacity already assigned to the card;
            it includes queued, provider-launching, initializing, and not-ready
            work.
          </p>
        </div>
        {(serviceData.fillTarget != null ||
          serviceData.freeReservedSlots != null) && (
          <div className="text-right text-xs text-gray-500">
            {serviceData.fillTarget != null && (
              <div>Aggregate fill target: {serviceData.fillTarget}</div>
            )}
            {serviceData.freeReservedSlots != null && (
              <div>
                Aggregate free reserved slots: {serviceData.freeReservedSlots}
              </div>
            )}
          </div>
        )}
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Card</TableHead>
            <TableHead className="text-right">Ready</TableHead>
            <TableHead className="text-right">Committed / unready</TableHead>
            <TableHead className="text-right">Total</TableHead>
            <TableHead className="text-right">Demand target</TableHead>
            <TableHead className="text-right">Warm retention</TableHead>
            <TableHead className="text-right">Cold-launch authority</TableHead>
            <TableHead className="text-right">Hard floor</TableHead>
            <TableHead className="text-right">Zero-cost ready</TableHead>
            <TableHead className="text-right">Fill target</TableHead>
            <TableHead className="text-right">Free reserved</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.card}>
              <TableCell className="font-medium">{row.card}</TableCell>
              <TableCell className="text-right">{row.ready}</TableCell>
              <TableCell className="text-right">{row.provisioning}</TableCell>
              <TableCell className="text-right">{row.total}</TableCell>
              <TableCell className="text-right">{row.demandTarget}</TableCell>
              <TableCell className="text-right">
                {row.warmRetentionTarget ?? 'n/a'}
              </TableCell>
              <TableCell className="text-right">
                {row.coldLaunchAuthority ?? 'n/a'}
              </TableCell>
              <TableCell className="text-right">{row.hardFloor}</TableCell>
              <TableCell className="text-right">{row.zeroCostReady}</TableCell>
              <TableCell className="text-right">
                {row.fillTarget ?? '—'}
              </TableCell>
              <TableCell className="text-right">
                {row.freeReserved ?? '—'}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Card>
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
  if (serviceData.costTrackedReplicaCount > 0) {
    hourlyCostDetails.push(
      `${serviceData.costTrackedReplicaCount} active, stopping, or cleanup-uncertain replica${
        serviceData.costTrackedReplicaCount === 1 ? '' : 's'
      }`
    );
  }
  if (serviceData.estimatedHourlyCost != null) {
    hourlyCostDetails.push(
      'Current catalog, compute only, not a provider bill'
    );
  }

  const excludedCostDetails = Object.entries(
    serviceData.hourlyCostExclusionReasons || {}
  ).map(([reason, count]) => {
    const label = reason === 'kubernetes' ? 'Kubernetes' : reason;
    return `${count} ${label} replica${count === 1 ? '' : 's'} excluded`;
  });

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
                  ? 'Logical capacity (ready/non-failed)'
                  : 'Replicas (ready/non-failed)'}
              </div>
              <div className="text-base mt-1">
                {serviceData.replicasReady}/{serviceData.replicasTotal}
                {serviceData.replicasFailed > 0 && (
                  <span className="text-red-700">
                    {' '}
                    (+{serviceData.replicasFailed}{' '}
                    {usesLogicalReplicas
                      ? 'failed or cleanup-uncertain slots, including history'
                      : 'failed or cleanup-uncertain replicas, including history'}
                    )
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
                  {serviceData.physicalReplicasTotal} physical backends
                  {' (ready/non-failed)'}
                  {serviceData.physicalReplicasFailed > 0 && (
                    <span className="text-red-700">
                      {' '}
                      (+{serviceData.physicalReplicasFailed} failed or
                      cleanup-uncertain{' '}
                      {serviceData.physicalReplicasFailed === 1
                        ? 'backend'
                        : 'backends'}
                      , including history)
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
                Estimated tracked compute cost
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
                Known cloud compute / 1K requests
              </div>
              <div className="text-base mt-1">
                {serviceData.costPerThousandRequests != null
                  ? `${formatUsd(serviceData.costPerThousandRequests)}${
                      serviceData.hourlyCostExcludedReplicaCount > 0 ? '+' : ''
                    }`
                  : serviceData.hourlyCostExcludedReplicaCount > 0
                    ? 'Unknown'
                    : '-'}
              </div>
              <div className="text-xs text-gray-500 mt-1">
                {excludedCostDetails.length > 0
                  ? serviceData.costPerThousandRequests != null
                    ? `Known lower bound at the recent request rate · ${excludedCostDetails.join(' · ')}`
                    : serviceData.pricedReplicaCount > 0
                      ? `No recent request rate · ${excludedCostDetails.join(' · ')}`
                      : `No pricing available · ${excludedCostDetails.join(' · ')}`
                  : 'Current fleet cost at the recent request rate'}
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
          <h3 className="text-lg font-semibold">
            Replica attempts by placement
          </h3>
          <p className="text-sm text-gray-500">
            Selected or confirmed placement for every tracked attempt. Queued
            intent and retained failure history are not live-machine counts.
          </p>
        </div>
        {loading && (
          <span className="text-sm text-gray-500 whitespace-nowrap">
            <CircularProgress size={14} className="mr-2" />
            Loading attempts…
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
                  Current / uncertain
                </TableHead>
                <TableHead className="whitespace-nowrap text-right">
                  Tracked attempts
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
                      {row.currentOrUncertain}
                    </TableCell>
                    <TableCell className="text-right tabular-nums font-semibold">
                      {row.trackedAttempts}
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell
                    colSpan={REPLICA_PLACEMENT_COLUMNS.length + 4}
                    className="text-center py-6 text-gray-500"
                  >
                    {loading ? 'Loading attempt placement…' : 'No replicas.'}
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
