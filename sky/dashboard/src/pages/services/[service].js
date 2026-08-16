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
import {
  getServiceDemand,
  getServiceHistory,
  getServicePricing,
  getServiceReplicaSummaries,
  getServiceReplicas,
  getServices,
} from '@/data/connectors/services';
import dashboardCache from '@/lib/cache';
import {
  CustomTooltip as Tooltip,
  NonCapitalizedTooltip,
  formatDuration,
  formatFullTimestamp,
} from '@/components/utils';
import {
  EndpointCell,
  ServiceHealthBadge,
  formatUptime,
  getPastAttemptCount,
} from '@/components/services';
import { ServeHistorySection } from '@/components/serve-history';
import { ServiceVersionHistory } from '@/components/service-version-history';
import { ServicePlacement } from '@/components/service-placement';
import { useMobile } from '@/hooks/useMobile';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';
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
]);

const DEFAULT_SERVICE_HISTORY_HOURS = 1;

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

export function useServiceDetails({ serviceName, loadFull = true }) {
  const [serviceData, setServiceData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [replicasLoading, setReplicasLoading] = useState(true);
  const requestVersionRef = useRef(0);
  const refreshInFlightRef = useRef(null);
  const visibleServiceDataRef = useRef(null);
  const initialLoadServiceNameRef = useRef(null);
  const summaryArgs = useMemo(
    () => [
      {
        serviceNames: [serviceName],
        metadataOnly: true,
      },
    ],
    [serviceName]
  );
  const fullArgs = useMemo(
    () => [
      {
        serviceNames: [serviceName],
        includeTargetReplicas: true,
      },
    ],
    [serviceName]
  );

  useEffect(() => {
    visibleServiceDataRef.current = serviceData;
  }, [serviceData]);

  // Two-phase load, both scoped to THIS service (the old implementation
  // fetched every service with full replica info just to display one):
  //   1. metadata_only - near-instant; renders the header/summary card.
  //   2. full — per-replica table; takes tens of seconds at fleet scale,
  //      fills in when it lands.
  const fetchData = useCallback(
    ({
      invalidate = false,
      source = 'refresh',
      supersede = false,
      loadFullRequest = loadFull,
      requireFreshSummary = false,
    } = {}) => {
      if (!serviceName) return Promise.resolve();
      const inFlight = refreshInFlightRef.current;
      const hasVisibleCurrentServiceData =
        visibleServiceDataRef.current?.name === serviceName;
      const hasVisibleCurrentFullServiceData =
        hasVisibleCurrentServiceData &&
        visibleServiceDataRef.current?.summaryOnly !== true &&
        visibleServiceDataRef.current?.metadataOnly !== true &&
        Array.isArray(visibleServiceDataRef.current?.replicas);
      if (
        source === 'initial' &&
        !loadFullRequest &&
        !requireFreshSummary &&
        hasVisibleCurrentServiceData
      ) {
        setLoading(false);
        setReplicasLoading(false);
        return Promise.resolve();
      }
      const loadSummary =
        requireFreshSummary ||
        !loadFullRequest ||
        source !== 'initial' ||
        !hasVisibleCurrentServiceData;
      const shouldReuseInFlight =
        inFlight?.serviceName === serviceName &&
        (!loadSummary || inFlight.loadSummary) &&
        (!loadFullRequest || inFlight.loadFull) &&
        (!supersede ||
          (!hasVisibleCurrentServiceData && inFlight.summaryPending) ||
          inFlight.source === 'manual');
      if (shouldReuseInFlight) {
        return inFlight.promise;
      }
      if (
        source === 'initial' &&
        loadFullRequest &&
        !loadSummary &&
        hasVisibleCurrentFullServiceData
      ) {
        setLoading(false);
        setReplicasLoading(false);
        return Promise.resolve();
      }
      if (invalidate) {
        if (loadSummary) {
          dashboardCache.invalidate(getServices, summaryArgs);
        }
        if (loadFullRequest) {
          dashboardCache.invalidate(getServices, fullArgs);
        }
      }

      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      setLoading(true);
      setReplicasLoading(loadFullRequest);
      if (loadFullRequest) {
        setServiceData((previous) =>
          previous?.name === serviceName && previous.enrichmentUnavailable
            ? { ...previous, enrichmentUnavailable: false }
            : previous
        );
      }
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion;
      let summarySettled = !loadSummary;
      let summaryServiceLanded = false;
      let fullServiceLanded = false;
      let fullSettled = !loadFullRequest;
      const finishLoadingIfReady = (hasRenderableData = false) => {
        if (!isCurrentRequest()) return;
        if (
          hasRenderableData ||
          visibleServiceDataRef.current?.name === serviceName ||
          (summarySettled && fullSettled)
        ) {
          setLoading(false);
        }
      };
      let refreshPromise;
      refreshPromise = (async () => {
        const promises = [];
        let metadataPromise = Promise.resolve();
        if (loadSummary) {
          const summaryPromise = dashboardCache
            .get(getServices, summaryArgs)
            .then(({ services }) => {
              if (!isCurrentRequest()) return;
              const found = (services || []).find(
                (s) => s.name === serviceName
              );
              summaryServiceLanded = Boolean(found);
              if (fullServiceLanded) return;
              setServiceData((previous) => {
                if (!found) return null;
                if (
                  previous?.name !== serviceName ||
                  previous.summaryOnly === true ||
                  !Array.isArray(previous.replicas)
                ) {
                  return found;
                }
                // Summary mode is the cheap, current view, but it omits the
                // full replica list. Keep the last complete snapshot visible
                // while the corresponding full request is still in flight.
                return {
                  ...previous,
                  ...found,
                  replicas: previous.replicas,
                  summaryOnly: false,
                  metadataOnly: false,
                };
              });
              finishLoadingIfReady(Boolean(found));
            })
            .catch((error) => {
              if (isCurrentRequest()) {
                console.error('Failed to fetch service summary:', error);
              }
            })
            .finally(() => {
              summarySettled = true;
              if (refreshInFlightRef.current?.promise === refreshPromise) {
                refreshInFlightRef.current.summaryPending = false;
              }
              if (isCurrentRequest()) {
                finishLoadingIfReady();
              }
            });
          metadataPromise = summaryPromise;
          promises.push(summaryPromise);
        }
        if (loadFullRequest) {
          const fullPromise = metadataPromise
            .then(() => {
              if (!isCurrentRequest()) return null;
              return dashboardCache.get(getServices, fullArgs);
            })
            .then((response) => {
              if (response === null) return;
              const { services } = response;
              if (!isCurrentRequest()) return;
              const found = (services || []).find(
                (s) => s.name === serviceName
              );
              fullServiceLanded = Boolean(found);
              if (found || !summaryServiceLanded) {
                setServiceData(found || null);
              } else {
                setServiceData((previous) =>
                  previous?.name === serviceName &&
                  (previous.metadataOnly || previous.summaryOnly)
                    ? { ...previous, enrichmentUnavailable: true }
                    : previous
                );
              }
              finishLoadingIfReady(Boolean(found));
            })
            .catch((error) => {
              if (isCurrentRequest()) {
                console.error('Failed to fetch service replicas:', error);
                setServiceData((previous) =>
                  previous?.name === serviceName &&
                  (previous.metadataOnly || previous.summaryOnly)
                    ? { ...previous, enrichmentUnavailable: true }
                    : previous
                );
              }
            })
            .finally(() => {
              fullSettled = true;
              if (isCurrentRequest()) {
                finishLoadingIfReady();
                setReplicasLoading(false);
              }
            });
          promises.push(fullPromise);
        }
        await Promise.allSettled(promises);
      })().finally(() => {
        if (refreshInFlightRef.current?.promise === refreshPromise) {
          refreshInFlightRef.current = null;
        }
      });
      refreshInFlightRef.current = {
        serviceName,
        promise: refreshPromise,
        source,
        summaryPending: loadSummary,
        loadSummary,
        loadFull: loadFullRequest,
      };
      return refreshPromise;
    },
    [fullArgs, loadFull, serviceName, summaryArgs]
  );

  useEffect(() => {
    const requireFreshSummary =
      initialLoadServiceNameRef.current !== serviceName;
    initialLoadServiceNameRef.current = serviceName;
    fetchData({
      source: 'initial',
      requireFreshSummary,
    });
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

  const refreshWhenVisible = useCallback(
    (refreshSource) => {
      void fetchData({
        invalidate: true,
        source: refreshSource === 'visibilitychange' ? 'visibility' : 'poll',
        supersede: refreshSource === 'visibilitychange',
      });
    },
    [fetchData]
  );

  useVisibleRefreshInterval(
    Boolean(serviceName),
    60 * 1000,
    refreshWhenVisible
  );

  return {
    serviceData,
    loading,
    replicasLoading,
    refreshData,
  };
}

export function useServiceDemand({
  serviceName,
  serviceHash,
  metadataReady,
  enabled = true,
  onServiceHashMismatch,
}) {
  const hasMetadata = metadataReady ?? Boolean(serviceHash);
  const [demandData, setDemandData] = useState(null);
  const [demandLoading, setDemandLoading] = useState(
    Boolean(enabled && serviceName)
  );
  const identityRef = useRef(null);
  const visibleDemandRef = useRef(null);
  const requestVersionRef = useRef(0);
  const activeRequestRef = useRef(null);

  useEffect(() => {
    visibleDemandRef.current = demandData;
  }, [demandData]);

  const fetchDemand = useCallback(() => {
    if (!enabled || !serviceName || !hasMetadata || !serviceHash) {
      setDemandLoading(Boolean(enabled && serviceName && !hasMetadata));
      return Promise.resolve();
    }
    const identity = `${serviceName}:${serviceHash}`;
    const active = activeRequestRef.current;
    if (active?.identity === identity) return active.promise;
    const requestVersion = requestVersionRef.current + 1;
    requestVersionRef.current = requestVersion;
    setDemandLoading(true);
    const isCurrentRequest = () =>
      requestVersionRef.current === requestVersion &&
      identityRef.current === identity;
    let requestPromise;
    requestPromise = getServiceDemand({ serviceName, serviceHash })
      .then((demand) => {
        if (!isCurrentRequest()) return;
        if (demand.requestTelemetryReason === 'not_found') {
          const error = new Error('The service incarnation changed.');
          error.code = 'SERVICE_HASH_MISMATCH';
          throw error;
        }
        const ownedDemand = { ...demand, serviceHash };
        visibleDemandRef.current = ownedDemand;
        setDemandData(ownedDemand);
      })
      .catch((error) => {
        if (!isCurrentRequest()) return;
        if (error?.code === 'SERVICE_HASH_MISMATCH') {
          const unavailable = {
            serviceHash,
            requestTelemetryState: 'unavailable',
            requestTelemetryReason: 'service_changed',
          };
          visibleDemandRef.current = unavailable;
          setDemandData(unavailable);
          void onServiceHashMismatch?.();
          return;
        }
        console.error('Failed to fetch service demand:', error);
        const previous = visibleDemandRef.current;
        const unavailable =
          previous?.serviceHash === serviceHash
            ? {
                ...previous,
                requestTelemetryState: 'stale',
                requestTelemetryReason: 'dashboard_refresh_failed',
                refreshUnavailable: true,
              }
            : {
                serviceHash,
                requestTelemetryState: 'unavailable',
                requestTelemetryReason: 'temporarily_unavailable',
                refreshUnavailable: true,
              };
        visibleDemandRef.current = unavailable;
        setDemandData(unavailable);
      })
      .finally(() => {
        if (isCurrentRequest()) setDemandLoading(false);
        if (activeRequestRef.current?.promise === requestPromise) {
          activeRequestRef.current = null;
        }
      });
    activeRequestRef.current = { identity, promise: requestPromise };
    return requestPromise;
  }, [enabled, hasMetadata, onServiceHashMismatch, serviceHash, serviceName]);

  useEffect(() => {
    const identity =
      enabled && serviceName && hasMetadata && serviceHash
        ? `${serviceName}:${serviceHash}`
        : null;
    if (identityRef.current !== identity) {
      identityRef.current = identity;
      visibleDemandRef.current = null;
      setDemandData(null);
    }
    requestVersionRef.current += 1;
    activeRequestRef.current = null;
    if (!identity) {
      setDemandLoading(Boolean(enabled && serviceName && !hasMetadata));
      return undefined;
    }
    void fetchDemand();
    return () => {
      requestVersionRef.current += 1;
      activeRequestRef.current = null;
    };
  }, [enabled, fetchDemand, hasMetadata, serviceHash, serviceName]);

  const refreshDemand = useCallback(() => fetchDemand(), [fetchDemand]);
  useVisibleRefreshInterval(
    Boolean(enabled && serviceName && hasMetadata && serviceHash),
    10 * 1000,
    refreshDemand
  );

  return { demandData, demandLoading, refreshDemand };
}

export function useServiceHistory({
  serviceName,
  serviceHash,
  metadataReady,
  enabled = true,
  onServiceHashMismatch,
}) {
  const hasMetadata = metadataReady ?? Boolean(serviceHash);
  const [replicaHistory, setReplicaHistory] = useState(null);
  const [historyLoading, setHistoryLoading] = useState(
    Boolean(enabled && serviceName)
  );
  const visibleHistoryRef = useRef(null);
  const identityRef = useRef(null);
  const loadedHoursRef = useRef(0);
  const desiredHoursRef = useRef(DEFAULT_SERVICE_HISTORY_HOURS);
  const requestVersionRef = useRef(0);
  const activeRequestRef = useRef(null);

  useEffect(() => {
    visibleHistoryRef.current = replicaHistory;
  }, [replicaHistory]);

  const fetchHistory = useCallback(
    ({ hours, force = false, supersede = false } = {}) => {
      const requestedHours = Math.max(
        1,
        Math.min(24, Number(hours) || desiredHoursRef.current)
      );
      desiredHoursRef.current = requestedHours;
      if (!enabled || !serviceName || !hasMetadata) {
        setHistoryLoading(Boolean(enabled && serviceName));
        return Promise.resolve();
      }
      const identity = `${serviceName}:${serviceHash ?? '<legacy>'}`;
      if (
        !force &&
        identityRef.current === identity &&
        loadedHoursRef.current >= requestedHours &&
        visibleHistoryRef.current?.serviceHash === serviceHash
      ) {
        return Promise.resolve();
      }
      const active = activeRequestRef.current;
      if (
        active?.identity === identity &&
        active.hours >= requestedHours &&
        !force &&
        !supersede
      ) {
        return active.promise;
      }

      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      setHistoryLoading(true);
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion &&
        identityRef.current === identity;

      let requestPromise;
      requestPromise = (async () => {
        try {
          let history = serviceHash
            ? await getServiceHistory({
                serviceName,
                serviceHash,
                hours: requestedHours,
              })
            : { legacyFallback: true };
          if (history.legacyFallback) {
            const legacyArgs = [
              {
                serviceNames: [serviceName],
                summaryOnly: true,
                historyHours: requestedHours,
              },
            ];
            if (force) {
              dashboardCache.invalidate(getServices, legacyArgs);
            }
            const { services } = await dashboardCache.get(
              getServices,
              legacyArgs
            );
            const service = (services || []).find(
              (candidate) => candidate.name === serviceName
            );
            if (service?.serviceHash && service.serviceHash !== serviceHash) {
              const error = new Error('The service incarnation changed.');
              error.code = 'SERVICE_HASH_MISMATCH';
              throw error;
            }
            history = service?.replicaHistory || {
              available: false,
              reason: 'legacy_history_unavailable',
            };
          }
          if (history.serviceHash && history.serviceHash !== serviceHash) {
            const error = new Error('The service incarnation changed.');
            error.code = 'SERVICE_HASH_MISMATCH';
            throw error;
          }
          if (!isCurrentRequest()) return;
          const ownedHistory = { ...history, serviceHash };
          visibleHistoryRef.current = ownedHistory;
          loadedHoursRef.current = requestedHours;
          setReplicaHistory(ownedHistory);
        } catch (error) {
          if (!isCurrentRequest()) return;
          if (error?.code === 'SERVICE_HASH_MISMATCH') {
            const unavailable = {
              available: false,
              reason: 'service_changed',
              serviceHash,
            };
            visibleHistoryRef.current = unavailable;
            loadedHoursRef.current = 0;
            setReplicaHistory(unavailable);
            void onServiceHashMismatch?.();
            return;
          }
          console.error('Failed to fetch service history:', error);
          if (visibleHistoryRef.current?.serviceHash === serviceHash) {
            const staleHistory = {
              ...visibleHistoryRef.current,
              refreshUnavailable: true,
            };
            visibleHistoryRef.current = staleHistory;
            setReplicaHistory(staleHistory);
          } else {
            const unavailable = {
              available: false,
              reason: 'temporarily_unavailable',
              serviceHash,
            };
            visibleHistoryRef.current = unavailable;
            setReplicaHistory(unavailable);
          }
        } finally {
          if (isCurrentRequest()) {
            setHistoryLoading(false);
          }
          if (activeRequestRef.current?.promise === requestPromise) {
            activeRequestRef.current = null;
          }
        }
      })();
      activeRequestRef.current = {
        identity,
        hours: requestedHours,
        promise: requestPromise,
      };
      return requestPromise;
    },
    [enabled, hasMetadata, onServiceHashMismatch, serviceHash, serviceName]
  );

  useEffect(() => {
    const identity =
      serviceName && hasMetadata
        ? `${serviceName}:${serviceHash ?? '<legacy>'}`
        : null;
    if (identityRef.current !== identity) {
      identityRef.current = identity;
      loadedHoursRef.current = 0;
      desiredHoursRef.current = DEFAULT_SERVICE_HISTORY_HOURS;
      visibleHistoryRef.current = null;
      setReplicaHistory(null);
    }
    requestVersionRef.current += 1;
    activeRequestRef.current = null;
    if (!enabled || !identity) {
      setHistoryLoading(Boolean(enabled && serviceName));
      return undefined;
    }
    void fetchHistory({
      hours: desiredHoursRef.current,
      force: loadedHoursRef.current === 0,
    });
    return () => {
      requestVersionRef.current += 1;
      activeRequestRef.current = null;
    };
  }, [enabled, fetchHistory, hasMetadata, serviceHash, serviceName]);

  const loadHistoryHours = useCallback(
    (hours) => fetchHistory({ hours }),
    [fetchHistory]
  );
  const refreshHistory = useCallback(
    () =>
      fetchHistory({
        hours: desiredHoursRef.current,
        force: true,
        supersede: true,
      }),
    [fetchHistory]
  );
  const refreshWhenVisible = useCallback(() => {
    void fetchHistory({
      hours: desiredHoursRef.current,
      force: true,
    });
  }, [fetchHistory]);

  useVisibleRefreshInterval(
    Boolean(enabled && serviceName && hasMetadata),
    60 * 1000,
    refreshWhenVisible
  );

  return {
    replicaHistory,
    historyLoading,
    loadHistoryHours,
    refreshHistory,
  };
}

const REPLICA_PAGE_SIZE = 50;
const CURRENT_REPLICA_SCOPE = 'current_or_uncertain';
const PAST_REPLICA_SCOPE = 'past_attempts';

function emptyReplicaPage() {
  return {
    serviceHash: null,
    replicas: [],
    total: null,
    nextCursor: null,
    observedAt: null,
    loading: false,
    loadingMore: false,
    unavailable: false,
    refreshUnavailable: false,
  };
}

function dedupeReplicas(previous, incoming) {
  const rows = new Map();
  [...(previous || []), ...(incoming || [])].forEach((replica) => {
    rows.set(String(replica.id), replica);
  });
  return Array.from(rows.values());
}

// Owns the v1c replica projections. The controller summary stays independent
// because it remains the fresh authority for targets and the endpoint.
// Durable demand and persisted replica counts/pages are hash-anchored and
// never wait for it.
export function useServiceReplicaData({
  serviceName,
  serviceHash,
  metadataReady,
  enabled = true,
  onServiceHashMismatch,
}) {
  const hasMetadata = metadataReady ?? Boolean(serviceHash);
  const [liveService, setLiveService] = useState(null);
  const [liveSummaryLoading, setLiveSummaryLoading] = useState(false);
  const [liveSummaryUnavailable, setLiveSummaryUnavailable] = useState(false);
  const [replicaSummary, setReplicaSummary] = useState(null);
  const [replicaSummaryLoading, setReplicaSummaryLoading] = useState(false);
  const [replicaSummaryUnavailable, setReplicaSummaryUnavailable] =
    useState(false);
  const [currentPage, setCurrentPage] = useState(emptyReplicaPage);
  const [pastPage, setPastPage] = useState(emptyReplicaPage);
  const [legacyService, setLegacyService] = useState(null);
  const identityRef = useRef(null);
  const generationRef = useRef(0);
  const modeRef = useRef('direct');
  const liveRequestRef = useRef(0);
  const summaryRequestRef = useRef(0);
  const currentRequestRef = useRef(0);
  const pastRequestRef = useRef(0);
  const legacyRequestRef = useRef(0);
  const fallbackRequestRef = useRef(null);
  const pastRequestedRef = useRef(false);
  const currentPageRef = useRef(currentPage);
  const pastPageRef = useRef(pastPage);

  useEffect(() => {
    currentPageRef.current = currentPage;
  }, [currentPage]);
  useEffect(() => {
    pastPageRef.current = pastPage;
  }, [pastPage]);

  const identityIsCurrent = useCallback(
    (identity, generation) =>
      identityRef.current === identity && generationRef.current === generation,
    []
  );

  const reportHashMismatch = useCallback(
    (identity, generation) => {
      if (!identityIsCurrent(identity, generation)) return;
      generationRef.current += 1;
      setReplicaSummaryUnavailable(true);
      setCurrentPage((previous) => ({
        ...previous,
        loading: false,
        loadingMore: false,
        unavailable: previous.replicas.length === 0,
        refreshUnavailable: previous.replicas.length > 0,
      }));
      setPastPage((previous) => ({
        ...previous,
        loading: false,
        loadingMore: false,
        unavailable: previous.replicas.length === 0,
        refreshUnavailable: previous.replicas.length > 0,
      }));
      void onServiceHashMismatch?.();
    },
    [identityIsCurrent, onServiceHashMismatch]
  );

  const fetchLegacyFull = useCallback(
    ({ identity, generation, force = false }) => {
      if (!identityIsCurrent(identity, generation)) return Promise.resolve();
      const existing = fallbackRequestRef.current;
      if (existing?.identity === identity && !force) return existing.promise;
      modeRef.current = 'legacy';
      const requestVersion = legacyRequestRef.current + 1;
      legacyRequestRef.current = requestVersion;
      summaryRequestRef.current += 1;
      currentRequestRef.current += 1;
      pastRequestRef.current += 1;
      const args = [
        {
          serviceNames: [serviceName],
          includeTargetReplicas: true,
        },
      ];
      if (force) dashboardCache.invalidate(getServices, args);
      setReplicaSummaryLoading(true);
      setCurrentPage((previous) => ({
        ...previous,
        loading: true,
        refreshUnavailable: false,
      }));
      let promise;
      promise = dashboardCache
        .get(getServices, args)
        .then(({ services }) => {
          if (
            !identityIsCurrent(identity, generation) ||
            legacyRequestRef.current !== requestVersion
          ) {
            return;
          }
          const service = (services || []).find(
            (candidate) => candidate.name === serviceName
          );
          if (
            service?.serviceHash &&
            serviceHash &&
            service.serviceHash !== serviceHash
          ) {
            reportHashMismatch(identity, generation);
            return;
          }
          if (!service) throw new Error('Service not found');
          const current = (service.replicas || []).filter(
            (replica) =>
              !REPLICA_HISTORICAL_FAILURE_STATUSES.has(replica.status)
          );
          const past = (service.replicas || []).filter((replica) =>
            REPLICA_HISTORICAL_FAILURE_STATUSES.has(replica.status)
          );
          setLegacyService(service);
          setReplicaSummary(service);
          setReplicaSummaryUnavailable(false);
          setCurrentPage({
            serviceHash: serviceHash || null,
            replicas: current,
            total: current.length,
            nextCursor: null,
            observedAt: null,
            loading: false,
            loadingMore: false,
            unavailable: false,
            refreshUnavailable: false,
          });
          setPastPage({
            serviceHash: serviceHash || null,
            replicas: past,
            total: past.length,
            nextCursor: null,
            observedAt: null,
            loading: false,
            loadingMore: false,
            unavailable: false,
            refreshUnavailable: false,
          });
        })
        .catch((error) => {
          if (
            !identityIsCurrent(identity, generation) ||
            legacyRequestRef.current !== requestVersion
          ) {
            return;
          }
          console.error('Failed to fetch legacy service replicas:', error);
          setReplicaSummaryUnavailable(true);
          setCurrentPage((previous) => ({
            ...previous,
            loading: false,
            unavailable: previous.replicas.length === 0,
            refreshUnavailable: previous.replicas.length > 0,
          }));
          setPastPage((previous) => ({
            ...previous,
            loading: false,
            unavailable: previous.replicas.length === 0,
            refreshUnavailable: previous.replicas.length > 0,
          }));
        })
        .finally(() => {
          if (
            identityIsCurrent(identity, generation) &&
            legacyRequestRef.current === requestVersion
          ) {
            setReplicaSummaryLoading(false);
          }
          if (fallbackRequestRef.current?.promise === promise) {
            fallbackRequestRef.current = null;
          }
        });
      fallbackRequestRef.current = { identity, promise };
      return promise;
    },
    [identityIsCurrent, reportHashMismatch, serviceHash, serviceName]
  );

  const fetchLiveSummary = useCallback(
    async ({ identity, generation, force = false }) => {
      if (!identityIsCurrent(identity, generation)) return;
      const requestVersion = liveRequestRef.current + 1;
      liveRequestRef.current = requestVersion;
      const args = [
        {
          serviceNames: [serviceName],
          summaryOnly: true,
          includeTargetReplicas: true,
          includeEndpoints: true,
        },
      ];
      if (force) dashboardCache.invalidate(getServices, args);
      setLiveSummaryLoading(true);
      setLiveSummaryUnavailable(false);
      try {
        const { services } = await dashboardCache.get(getServices, args);
        if (
          !identityIsCurrent(identity, generation) ||
          liveRequestRef.current !== requestVersion
        ) {
          return;
        }
        const service = (services || []).find(
          (candidate) => candidate.name === serviceName
        );
        if (
          service?.serviceHash &&
          serviceHash &&
          service.serviceHash !== serviceHash
        ) {
          reportHashMismatch(identity, generation);
          return;
        }
        if (!service) throw new Error('Service not found');
        setLiveService(service);
      } catch (error) {
        if (
          !identityIsCurrent(identity, generation) ||
          liveRequestRef.current !== requestVersion
        ) {
          return;
        }
        console.error('Failed to fetch live service summary:', error);
        setLiveSummaryUnavailable(true);
      } finally {
        if (
          identityIsCurrent(identity, generation) &&
          liveRequestRef.current === requestVersion
        ) {
          setLiveSummaryLoading(false);
        }
      }
    },
    [identityIsCurrent, reportHashMismatch, serviceHash, serviceName]
  );

  const fetchReplicaSummary = useCallback(
    async ({ identity, generation }) => {
      if (!identityIsCurrent(identity, generation) || !serviceHash) return;
      const requestVersion = summaryRequestRef.current + 1;
      summaryRequestRef.current = requestVersion;
      setReplicaSummaryLoading(true);
      setReplicaSummaryUnavailable(false);
      try {
        const response = await getServiceReplicaSummaries({
          serviceNames: [serviceName],
        });
        if (
          !identityIsCurrent(identity, generation) ||
          summaryRequestRef.current !== requestVersion ||
          modeRef.current === 'legacy'
        ) {
          return;
        }
        if (response.legacyFallback) {
          await fetchLegacyFull({ identity, generation });
          return;
        }
        if (!response.available) {
          if (response.reason === 'not_found') {
            reportHashMismatch(identity, generation);
            return;
          }
          throw new Error(
            `Persisted replica summary unavailable: ${response.reason || 'unknown'}`
          );
        }
        const summary = (response.summaries || []).find(
          (candidate) => candidate.name === serviceName
        );
        if (!summary) {
          throw new Error('Persisted replica summary was not found');
        }
        if (summary.serviceHash !== serviceHash) {
          reportHashMismatch(identity, generation);
          return;
        }
        setReplicaSummary(summary);
      } catch (error) {
        if (
          !identityIsCurrent(identity, generation) ||
          summaryRequestRef.current !== requestVersion
        ) {
          return;
        }
        if (error?.code === 'SERVICE_HASH_MISMATCH') {
          reportHashMismatch(identity, generation);
          return;
        }
        console.error('Failed to fetch persisted replica summary:', error);
        setReplicaSummaryUnavailable(true);
      } finally {
        if (
          identityIsCurrent(identity, generation) &&
          summaryRequestRef.current === requestVersion
        ) {
          setReplicaSummaryLoading(false);
        }
      }
    },
    [
      fetchLegacyFull,
      identityIsCurrent,
      reportHashMismatch,
      serviceHash,
      serviceName,
    ]
  );

  const fetchReplicaPage = useCallback(
    async ({ identity, generation, scope, cursor = null, append = false }) => {
      if (!identityIsCurrent(identity, generation) || !serviceHash) return;
      const isPast = scope === PAST_REPLICA_SCOPE;
      const requestRef = isPast ? pastRequestRef : currentRequestRef;
      const setPage = isPast ? setPastPage : setCurrentPage;
      const requestVersion = requestRef.current + 1;
      requestRef.current = requestVersion;
      setPage((previous) => ({
        ...previous,
        loading: !append,
        loadingMore: append,
        refreshUnavailable: false,
      }));
      try {
        const response = await getServiceReplicas({
          serviceName,
          serviceHash,
          scope,
          limit: REPLICA_PAGE_SIZE,
          cursor,
        });
        if (
          !identityIsCurrent(identity, generation) ||
          requestRef.current !== requestVersion ||
          modeRef.current === 'legacy'
        ) {
          return;
        }
        if (response.legacyFallback) {
          await fetchLegacyFull({ identity, generation });
          return;
        }
        if (!response.available) {
          if (response.reason === 'not_found') {
            reportHashMismatch(identity, generation);
            return;
          }
          throw new Error(
            `Persisted replicas unavailable: ${response.reason || 'unknown'}`
          );
        }
        if (response.serviceHash !== serviceHash) {
          reportHashMismatch(identity, generation);
          return;
        }
        const incomingIds = new Set(
          response.replicas.map((replica) => String(replica.id))
        );
        const setOtherPage = isPast ? setCurrentPage : setPastPage;
        setOtherPage((previous) => {
          const replicas = previous.replicas.filter(
            (replica) => !incomingIds.has(String(replica.id))
          );
          const removedCount = previous.replicas.length - replicas.length;
          const total =
            previous.total == null
              ? null
              : Math.max(replicas.length, previous.total - removedCount);
          const next = {
            ...previous,
            replicas,
            total,
          };
          if (isPast) currentPageRef.current = next;
          else pastPageRef.current = next;
          return next;
        });
        setPage((previous) => {
          const next = {
            serviceHash: response.serviceHash,
            replicas: append
              ? dedupeReplicas(previous.replicas, response.replicas)
              : response.replicas,
            total: response.total,
            nextCursor: response.nextCursor,
            observedAt: response.observedAt,
            loading: false,
            loadingMore: false,
            unavailable: false,
            refreshUnavailable: false,
          };
          if (isPast) pastPageRef.current = next;
          else currentPageRef.current = next;
          return next;
        });
      } catch (error) {
        if (
          !identityIsCurrent(identity, generation) ||
          requestRef.current !== requestVersion
        ) {
          return;
        }
        if (error?.code === 'SERVICE_HASH_MISMATCH') {
          reportHashMismatch(identity, generation);
          return;
        }
        console.error(`Failed to fetch ${scope} replicas:`, error);
        setPage((previous) => ({
          ...previous,
          loading: false,
          loadingMore: false,
          unavailable: previous.replicas.length === 0,
          refreshUnavailable: previous.replicas.length > 0,
        }));
      }
    },
    [
      fetchLegacyFull,
      identityIsCurrent,
      reportHashMismatch,
      serviceHash,
      serviceName,
    ]
  );

  useEffect(() => {
    const identity =
      serviceName && hasMetadata
        ? `${serviceName}:${serviceHash ?? '<legacy>'}`
        : null;
    generationRef.current += 1;
    const generation = generationRef.current;
    identityRef.current = identity;
    modeRef.current = serviceHash ? 'direct' : 'legacy';
    fallbackRequestRef.current = null;
    pastRequestedRef.current = false;
    setLiveService(null);
    setLiveSummaryUnavailable(false);
    setReplicaSummary(null);
    setReplicaSummaryUnavailable(false);
    setLegacyService(null);
    setCurrentPage(emptyReplicaPage());
    setPastPage(emptyReplicaPage());
    if (!enabled || !identity) {
      setLiveSummaryLoading(false);
      setReplicaSummaryLoading(false);
      return undefined;
    }
    void fetchLiveSummary({ identity, generation });
    if (!serviceHash) {
      void fetchLegacyFull({ identity, generation });
    } else {
      void fetchReplicaSummary({ identity, generation });
      void fetchReplicaPage({
        identity,
        generation,
        scope: CURRENT_REPLICA_SCOPE,
      });
    }
    return () => {
      generationRef.current += 1;
      fallbackRequestRef.current = null;
    };
  }, [
    enabled,
    fetchLegacyFull,
    fetchLiveSummary,
    fetchReplicaPage,
    fetchReplicaSummary,
    hasMetadata,
    serviceHash,
    serviceName,
  ]);

  const refreshReplicas = useCallback(() => {
    const identity = identityRef.current;
    const generation = generationRef.current;
    if (!enabled || !identity) return Promise.resolve();
    const requests = [fetchLiveSummary({ identity, generation, force: true })];
    if (modeRef.current === 'legacy' || !serviceHash) {
      requests.push(fetchLegacyFull({ identity, generation, force: true }));
    } else {
      requests.push(
        fetchReplicaSummary({ identity, generation }),
        fetchReplicaPage({
          identity,
          generation,
          scope: CURRENT_REPLICA_SCOPE,
        })
      );
      if (pastRequestedRef.current) {
        requests.push(
          fetchReplicaPage({
            identity,
            generation,
            scope: PAST_REPLICA_SCOPE,
          })
        );
      }
    }
    return Promise.allSettled(requests);
  }, [
    enabled,
    fetchLegacyFull,
    fetchLiveSummary,
    fetchReplicaPage,
    fetchReplicaSummary,
    serviceHash,
  ]);

  const refreshCurrentPage = useCallback(() => {
    const identity = identityRef.current;
    const generation = generationRef.current;
    if (!enabled || !identity || modeRef.current === 'legacy' || !serviceHash) {
      return Promise.resolve();
    }
    return fetchReplicaPage({
      identity,
      generation,
      scope: CURRENT_REPLICA_SCOPE,
    });
  }, [enabled, fetchReplicaPage, serviceHash]);

  const openPastAttempts = useCallback(() => {
    pastRequestedRef.current = true;
    if (
      modeRef.current === 'legacy' ||
      !serviceHash ||
      pastPageRef.current.total !== null ||
      pastPageRef.current.loading
    ) {
      return Promise.resolve();
    }
    return fetchReplicaPage({
      identity: identityRef.current,
      generation: generationRef.current,
      scope: PAST_REPLICA_SCOPE,
    });
  }, [fetchReplicaPage, serviceHash]);

  const loadMoreCurrent = useCallback(() => {
    const cursor = currentPageRef.current.nextCursor;
    if (
      !cursor ||
      currentPageRef.current.loading ||
      currentPageRef.current.loadingMore
    ) {
      return Promise.resolve();
    }
    return fetchReplicaPage({
      identity: identityRef.current,
      generation: generationRef.current,
      scope: CURRENT_REPLICA_SCOPE,
      cursor,
      append: true,
    });
  }, [fetchReplicaPage]);

  const loadMorePast = useCallback(() => {
    const cursor = pastPageRef.current.nextCursor;
    if (
      !cursor ||
      pastPageRef.current.loading ||
      pastPageRef.current.loadingMore
    ) {
      return Promise.resolve();
    }
    return fetchReplicaPage({
      identity: identityRef.current,
      generation: generationRef.current,
      scope: PAST_REPLICA_SCOPE,
      cursor,
      append: true,
    });
  }, [fetchReplicaPage]);

  const refreshWhenVisible = useCallback(() => {
    void refreshReplicas();
  }, [refreshReplicas]);
  useVisibleRefreshInterval(
    Boolean(enabled && serviceName && hasMetadata),
    60 * 1000,
    refreshWhenVisible
  );

  return {
    liveService,
    liveSummaryLoading,
    liveSummaryUnavailable,
    replicaSummary,
    replicaSummaryLoading,
    replicaSummaryUnavailable,
    currentPage,
    pastPage,
    legacyService,
    refreshReplicas,
    refreshCurrentPage,
    openPastAttempts,
    loadMoreCurrent,
    loadMorePast,
  };
}

function replicaPricingKey(replica) {
  return JSON.stringify([
    Number(replica.id),
    replica.pricingFingerprint ?? null,
  ]);
}

function pricingAggregateIsGood(aggregate) {
  return aggregate?.available === true;
}

export function useServicePricing({
  serviceName,
  serviceHash,
  metadataReady,
  currentPage,
  enabled = true,
  onServiceHashMismatch,
  onRefreshCurrentPage,
}) {
  const hasMetadata = metadataReady ?? Boolean(serviceHash);
  const requestedIdentity =
    serviceName && hasMetadata
      ? `${serviceName}:${serviceHash ?? '<legacy>'}`
      : null;
  const [aggregate, setAggregate] = useState(null);
  const [priceBasis, setPriceBasis] = useState(null);
  const [aggregateLoading, setAggregateLoading] = useState(false);
  const [aggregateRefreshing, setAggregateRefreshing] = useState(false);
  const [aggregateUnavailable, setAggregateUnavailable] = useState(false);
  const [aggregateRefreshUnavailable, setAggregateRefreshUnavailable] =
    useState(false);
  const [aggregateUnavailableReason, setAggregateUnavailableReason] =
    useState(null);
  const [rowPricing, setRowPricing] = useState({});
  const [rowLoading, setRowLoading] = useState({});
  const [rowUnavailable, setRowUnavailable] = useState({});
  const [rowCapabilityUnavailable, setRowCapabilityUnavailable] =
    useState(false);
  const identityRef = useRef(null);
  const generationRef = useRef(0);
  const aggregateRequestRef = useRef(0);
  const rowEpochRef = useRef(0);
  const aggregateRef = useRef(null);
  const rowPricingRef = useRef({});
  const rowLoadingRef = useRef({});
  const rowUnavailableRef = useRef({});
  const rowCapabilityUnavailableRef = useRef(false);
  const rowDeferredRef = useRef(new Set());
  const currentPageRef = useRef(currentPage);

  useEffect(() => {
    currentPageRef.current = currentPage;
  }, [currentPage]);

  const replaceRowPricing = useCallback((next) => {
    rowPricingRef.current = next;
    setRowPricing(next);
  }, []);
  const replaceRowLoading = useCallback((next) => {
    rowLoadingRef.current = next;
    setRowLoading(next);
  }, []);
  const replaceRowUnavailable = useCallback((next) => {
    rowUnavailableRef.current = next;
    setRowUnavailable(next);
  }, []);

  const identityIsCurrent = useCallback(
    (identity, generation) =>
      identityRef.current === identity && generationRef.current === generation,
    []
  );

  const reportHashMismatch = useCallback(
    (identity, generation) => {
      if (!identityIsCurrent(identity, generation)) return;
      generationRef.current += 1;
      rowEpochRef.current += 1;
      setAggregateLoading(false);
      setAggregateRefreshing(false);
      setAggregateUnavailable(true);
      replaceRowLoading({});
      void onServiceHashMismatch?.();
    },
    [identityIsCurrent, onServiceHashMismatch, replaceRowLoading]
  );

  const fetchAggregate = useCallback(
    async ({ identity, generation }) => {
      if (
        !identityIsCurrent(identity, generation) ||
        !serviceHash ||
        !enabled
      ) {
        return;
      }
      const requestVersion = aggregateRequestRef.current + 1;
      aggregateRequestRef.current = requestVersion;
      const hadLastGood = pricingAggregateIsGood(aggregateRef.current);
      setAggregateLoading(!hadLastGood);
      setAggregateRefreshing(hadLastGood);
      setAggregateUnavailable(false);
      setAggregateRefreshUnavailable(false);
      setAggregateUnavailableReason(null);
      try {
        const response = await getServicePricing({
          serviceName,
          serviceHash,
        });
        if (
          !identityIsCurrent(identity, generation) ||
          aggregateRequestRef.current !== requestVersion
        ) {
          return;
        }
        if (!response.available) {
          if (response.reason === 'not_found') {
            reportHashMismatch(identity, generation);
            return;
          }
          if (hadLastGood) {
            setAggregateRefreshUnavailable(true);
          } else {
            setAggregateUnavailable(true);
          }
          setAggregateUnavailableReason(response.reason || 'unavailable');
          return;
        }
        setPriceBasis(response.priceBasis);
        if (!response.aggregate?.available) {
          if (hadLastGood) {
            setAggregateRefreshUnavailable(true);
          } else {
            aggregateRef.current = response.aggregate;
            setAggregate(response.aggregate);
            setAggregateUnavailable(true);
          }
          setAggregateUnavailableReason(
            response.aggregate?.unavailableReason || 'unavailable'
          );
          return;
        }
        aggregateRef.current = response.aggregate;
        setAggregate(response.aggregate);
      } catch (error) {
        if (
          !identityIsCurrent(identity, generation) ||
          aggregateRequestRef.current !== requestVersion
        ) {
          return;
        }
        if (error?.code === 'SERVICE_HASH_MISMATCH') {
          reportHashMismatch(identity, generation);
          return;
        }
        console.error('Failed to fetch service pricing aggregate:', error);
        if (hadLastGood) {
          setAggregateRefreshUnavailable(true);
        } else {
          setAggregateUnavailable(true);
        }
        setAggregateUnavailableReason('request_failed');
      } finally {
        if (
          identityIsCurrent(identity, generation) &&
          aggregateRequestRef.current === requestVersion
        ) {
          setAggregateLoading(false);
          setAggregateRefreshing(false);
        }
      }
    },
    [enabled, identityIsCurrent, reportHashMismatch, serviceHash, serviceName]
  );

  const fetchRowChunk = useCallback(
    async ({ identity, generation, epoch, replicas }) => {
      if (
        !identityIsCurrent(identity, generation) ||
        rowEpochRef.current !== epoch ||
        !serviceHash ||
        !enabled ||
        replicas.length === 0
      ) {
        return;
      }
      const expectedById = new Map(
        replicas.map((replica) => [Number(replica.id), replica])
      );
      const keys = replicas.map(replicaPricingKey);
      const nextLoading = { ...rowLoadingRef.current };
      const nextUnavailable = { ...rowUnavailableRef.current };
      keys.forEach((key) => {
        nextLoading[key] = epoch;
        delete nextUnavailable[key];
      });
      replaceRowLoading(nextLoading);
      replaceRowUnavailable(nextUnavailable);
      let refreshCurrentPage = false;
      try {
        const response = await getServicePricing({
          serviceName,
          serviceHash,
          replicaIds: replicas.map((replica) => replica.id),
        });
        if (
          !identityIsCurrent(identity, generation) ||
          rowEpochRef.current !== epoch
        ) {
          return;
        }
        if (!response.available) {
          if (response.reason === 'not_found') {
            reportHashMismatch(identity, generation);
            return;
          }
          rowCapabilityUnavailableRef.current = true;
          setRowCapabilityUnavailable(true);
          const unavailable = { ...rowUnavailableRef.current };
          keys.forEach((key) => {
            unavailable[key] = epoch;
          });
          replaceRowUnavailable(unavailable);
          return;
        }
        setPriceBasis((previous) => previous || response.priceBasis);
        const currentRows = new Map(
          (currentPageRef.current?.serviceHash === serviceHash
            ? currentPageRef.current.replicas
            : []
          ).map((replica) => [Number(replica.id), replica])
        );
        const nextPricing = { ...rowPricingRef.current };
        const unavailable = { ...rowUnavailableRef.current };
        response.replicas.forEach((pricedReplica) => {
          const expected = expectedById.get(Number(pricedReplica.id));
          const current = currentRows.get(Number(pricedReplica.id));
          if (!expected || !current) return;
          const expectedKey = replicaPricingKey(expected);
          if (replicaPricingKey(current) !== expectedKey) return;
          if (
            pricedReplica.hourlyCostExclusionReason ===
            'not_current_or_uncertain'
          ) {
            rowDeferredRef.current.add(expectedKey);
            unavailable[expectedKey] = epoch;
            refreshCurrentPage = true;
            return;
          }
          if (
            pricedReplica.pricingFingerprint !==
            (expected.pricingFingerprint ?? null)
          ) {
            rowDeferredRef.current.add(expectedKey);
            unavailable[expectedKey] = epoch;
            refreshCurrentPage = true;
            return;
          }
          nextPricing[expectedKey] = pricedReplica;
          delete unavailable[expectedKey];
        });
        replaceRowPricing(nextPricing);
        replaceRowUnavailable(unavailable);
      } catch (error) {
        if (
          !identityIsCurrent(identity, generation) ||
          rowEpochRef.current !== epoch
        ) {
          return;
        }
        if (error?.code === 'SERVICE_HASH_MISMATCH') {
          reportHashMismatch(identity, generation);
          return;
        }
        console.error('Failed to fetch service replica prices:', error);
        const unavailable = { ...rowUnavailableRef.current };
        keys.forEach((key) => {
          unavailable[key] = epoch;
        });
        replaceRowUnavailable(unavailable);
      } finally {
        if (
          identityIsCurrent(identity, generation) &&
          rowEpochRef.current === epoch
        ) {
          const loading = { ...rowLoadingRef.current };
          keys.forEach((key) => {
            if (loading[key] === epoch) delete loading[key];
          });
          replaceRowLoading(loading);
          if (refreshCurrentPage) void onRefreshCurrentPage?.();
        }
      }
    },
    [
      enabled,
      identityIsCurrent,
      onRefreshCurrentPage,
      replaceRowLoading,
      replaceRowPricing,
      replaceRowUnavailable,
      reportHashMismatch,
      serviceHash,
      serviceName,
    ]
  );

  const fetchMissingRows = useCallback(
    ({ identity, generation, epoch }) => {
      if (
        !identityIsCurrent(identity, generation) ||
        rowEpochRef.current !== epoch ||
        rowCapabilityUnavailableRef.current ||
        currentPageRef.current?.serviceHash !== serviceHash
      ) {
        return Promise.resolve([]);
      }
      const missing = (currentPageRef.current.replicas || []).filter(
        (replica) => {
          const key = replicaPricingKey(replica);
          return (
            !Object.prototype.hasOwnProperty.call(rowPricingRef.current, key) &&
            !Object.prototype.hasOwnProperty.call(rowLoadingRef.current, key) &&
            !Object.prototype.hasOwnProperty.call(
              rowUnavailableRef.current,
              key
            ) &&
            !rowDeferredRef.current.has(key)
          );
        }
      );
      const requests = [];
      for (let index = 0; index < missing.length; index += 100) {
        requests.push(
          fetchRowChunk({
            identity,
            generation,
            epoch,
            replicas: missing.slice(index, index + 100),
          })
        );
      }
      return Promise.allSettled(requests);
    },
    [fetchRowChunk, identityIsCurrent, serviceHash]
  );

  useEffect(() => {
    const identity = requestedIdentity;
    const identityChanged = identityRef.current !== identity;
    generationRef.current += 1;
    rowEpochRef.current += 1;
    const generation = generationRef.current;
    const epoch = rowEpochRef.current;
    identityRef.current = identity;
    // Every effect restart invalidates the prior row epoch.  Drop its
    // in-flight markers even when the service identity is unchanged (for
    // example, when leaving and returning to Overview), since the stale
    // request is intentionally unable to clear them in its finally block.
    replaceRowLoading({});
    if (identityChanged) {
      aggregateRef.current = null;
      rowPricingRef.current = {};
      rowUnavailableRef.current = {};
      rowCapabilityUnavailableRef.current = false;
      rowDeferredRef.current = new Set();
      setAggregate(null);
      setPriceBasis(null);
      setAggregateUnavailable(false);
      setAggregateRefreshUnavailable(false);
      setAggregateUnavailableReason(null);
      replaceRowPricing({});
      replaceRowUnavailable({});
      setRowCapabilityUnavailable(false);
    }
    if (!enabled || !identity || !serviceHash) {
      setAggregateLoading(false);
      setAggregateRefreshing(false);
      return undefined;
    }
    void fetchAggregate({ identity, generation });
    void fetchMissingRows({ identity, generation, epoch });
    return () => {
      generationRef.current += 1;
      rowEpochRef.current += 1;
    };
  }, [
    enabled,
    fetchAggregate,
    fetchMissingRows,
    hasMetadata,
    replaceRowLoading,
    replaceRowPricing,
    replaceRowUnavailable,
    requestedIdentity,
    serviceHash,
  ]);

  useEffect(() => {
    const identity = identityRef.current;
    if (!enabled || !identity || !serviceHash) return;
    void fetchMissingRows({
      identity,
      generation: generationRef.current,
      epoch: rowEpochRef.current,
    });
  }, [currentPage, enabled, fetchMissingRows, serviceHash]);

  const refreshPricing = useCallback(() => {
    const identity = identityRef.current;
    const generation = generationRef.current;
    if (!enabled || !identity || !serviceHash) return Promise.resolve();
    rowEpochRef.current += 1;
    const epoch = rowEpochRef.current;
    const positivePricing = Object.fromEntries(
      Object.entries(rowPricingRef.current).filter(
        ([, pricedReplica]) => pricedReplica.hourlyCost !== null
      )
    );
    replaceRowPricing(positivePricing);
    replaceRowLoading({});
    replaceRowUnavailable({});
    rowDeferredRef.current = new Set();
    rowCapabilityUnavailableRef.current = false;
    setRowCapabilityUnavailable(false);
    return Promise.allSettled([
      fetchAggregate({ identity, generation }),
      fetchMissingRows({ identity, generation, epoch }),
    ]);
  }, [
    enabled,
    fetchAggregate,
    fetchMissingRows,
    replaceRowLoading,
    replaceRowPricing,
    replaceRowUnavailable,
    serviceHash,
  ]);

  const refreshWhenVisible = useCallback(() => {
    void refreshPricing();
  }, [refreshPricing]);
  useVisibleRefreshInterval(
    Boolean(enabled && serviceName && serviceHash),
    60 * 1000,
    refreshWhenVisible
  );

  // Effects reset state after a prop change. Fence the render that observes a
  // new same-name service hash before that reset effect has run, too.
  const ownsPricingIdentity =
    requestedIdentity !== null && identityRef.current === requestedIdentity;

  const getReplicaPricing = useCallback(
    (replica) => {
      if (!ownsPricingIdentity) return { state: 'loading' };
      const key = replicaPricingKey(replica);
      const pricedReplica = rowPricing[key];
      if (pricedReplica) {
        return {
          state: pricedReplica.hourlyCost === null ? 'excluded' : 'available',
          ...pricedReplica,
        };
      }
      if (Object.prototype.hasOwnProperty.call(rowLoading, key)) {
        return { state: 'loading' };
      }
      if (
        rowCapabilityUnavailable ||
        Object.prototype.hasOwnProperty.call(rowUnavailable, key)
      ) {
        return { state: 'unavailable' };
      }
      return { state: 'loading' };
    },
    [
      ownsPricingIdentity,
      rowCapabilityUnavailable,
      rowLoading,
      rowPricing,
      rowUnavailable,
    ]
  );

  return {
    aggregate: ownsPricingIdentity ? aggregate : null,
    priceBasis: ownsPricingIdentity ? priceBasis : null,
    aggregateLoading: ownsPricingIdentity
      ? aggregateLoading
      : Boolean(enabled && requestedIdentity && serviceHash),
    aggregateRefreshing: ownsPricingIdentity ? aggregateRefreshing : false,
    aggregateUnavailable: ownsPricingIdentity ? aggregateUnavailable : false,
    aggregateRefreshUnavailable: ownsPricingIdentity
      ? aggregateRefreshUnavailable
      : false,
    aggregateUnavailableReason: ownsPricingIdentity
      ? aggregateUnavailableReason
      : null,
    getReplicaPricing,
    refreshPricing,
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
  const activeServiceNameRef = useRef(serviceName);
  const isMobile = useMobile();
  const { serviceData, loading, refreshData } = useServiceDetails({
    serviceName,
    // Overview uses the bounded direct replica projections below. The full
    // controller status path is reserved for capability/topology fallback.
    loadFull: false,
  });
  const replicaData = useServiceReplicaData({
    serviceName,
    serviceHash:
      serviceData?.name === serviceName ? serviceData.serviceHash : null,
    metadataReady:
      serviceData?.name === serviceName &&
      Object.prototype.hasOwnProperty.call(serviceData, 'serviceHash'),
    enabled: activeTab === 'overview',
    onServiceHashMismatch: refreshData,
  });
  const demand = useServiceDemand({
    serviceName,
    serviceHash:
      serviceData?.name === serviceName ? serviceData.serviceHash : null,
    metadataReady:
      serviceData?.name === serviceName &&
      Object.prototype.hasOwnProperty.call(serviceData, 'serviceHash'),
    enabled: activeTab === 'overview',
    onServiceHashMismatch: refreshData,
  });
  const pricingData = useServicePricing({
    serviceName,
    serviceHash:
      serviceData?.name === serviceName ? serviceData.serviceHash : null,
    metadataReady:
      serviceData?.name === serviceName &&
      Object.prototype.hasOwnProperty.call(serviceData, 'serviceHash'),
    currentPage: replicaData.currentPage,
    enabled: activeTab === 'overview' && !replicaData.legacyService,
    onServiceHashMismatch: refreshData,
    onRefreshCurrentPage: replicaData.refreshCurrentPage,
  });
  const { replicaHistory, historyLoading, loadHistoryHours, refreshHistory } =
    useServiceHistory({
      serviceName,
      serviceHash:
        serviceData?.name === serviceName ? serviceData.serviceHash : null,
      metadataReady:
        serviceData?.name === serviceName &&
        Object.prototype.hasOwnProperty.call(serviceData, 'serviceHash'),
      enabled: activeTab === 'overview',
      onServiceHashMismatch: refreshData,
    });

  useEffect(() => {
    if (activeServiceNameRef.current !== serviceName) {
      activeServiceNameRef.current = serviceName;
      setIsInitialLoad(true);
    }
  }, [serviceName]);

  useEffect(() => {
    if (!loading && isInitialLoad) {
      setIsInitialLoad(false);
    }
  }, [loading, isInitialLoad]);

  // Effects run after render. On a route change, do not expose the previous
  // service's snapshot or a false settled state before the new route owns the
  // page and lands its first matching response.
  const ownsRouteState = activeServiceNameRef.current === serviceName;
  const currentServiceData = useMemo(() => {
    if (!ownsRouteState || serviceData?.name !== serviceName) return null;
    const anchoredHash = serviceData.serviceHash;
    const ownsIdentity = (candidate) =>
      candidate?.name === serviceName &&
      (!anchoredHash ||
        !candidate.serviceHash ||
        candidate.serviceHash === anchoredHash);
    const persistedSummary = ownsIdentity(replicaData.replicaSummary)
      ? replicaData.replicaSummary
      : null;
    const liveSummary = ownsIdentity(replicaData.liveService)
      ? replicaData.liveService
      : null;
    const legacy = ownsIdentity(replicaData.legacyService)
      ? replicaData.legacyService
      : null;
    const currentReplicas = legacy
      ? replicaData.currentPage.replicas
      : replicaData.currentPage.replicas.map((replica) => {
          const pricing = pricingData.getReplicaPricing(replica);
          return {
            ...replica,
            pricingState: pricing.state,
            ...(pricing.state === 'available' || pricing.state === 'excluded'
              ? {
                  hourlyCost: pricing.hourlyCost,
                  hourlyCostExclusionReason: pricing.hourlyCostExclusionReason,
                  priceSource: pricing.priceSource,
                }
              : {}),
          };
        });
    const persistedPricing =
      !legacy && pricingAggregateIsGood(pricingData.aggregate)
        ? {
            ...pricingData.aggregate,
            pricingCoverage: pricingData.aggregate.coverage,
            priceBasis: pricingData.priceBasis,
          }
        : {};
    const directOnlyFields = persistedSummary
      ? {
          currentOrUncertainCount: persistedSummary.currentOrUncertainCount,
          pastAttemptCount: persistedSummary.pastAttemptCount,
          replicaSummaryObservedAt: persistedSummary.observedAt,
        }
      : {};
    const directDemand =
      demand.demandData?.serviceHash === anchoredHash &&
      demand.demandData?.legacyFallback !== true
        ? demand.demandData
        : null;
    const directDemandMetadata = directDemand
      ? {
          requestTelemetryState: directDemand.requestTelemetryState,
          requestTelemetryReason: directDemand.requestTelemetryReason,
          requestTelemetryGeneration:
            directDemand.requestTelemetryGeneration ?? null,
          requestTelemetryCompatibilityComplete:
            directDemand.requestTelemetryCompatibilityComplete ?? null,
          requestReporterCount: directDemand.requestReporterCount ?? null,
          requestStatsAgeSeconds: directDemand.requestStatsAgeSeconds ?? null,
        }
      : {};
    const directDemandMetrics =
      directDemand &&
      [
        directDemand.recentRequestCount,
        directDemand.requestRate,
        directDemand.inFlightRequests,
        directDemand.requestQueueDepth,
        directDemand.rejectedRequests,
      ].some((value) => value != null)
        ? {
            recentRequestCount: directDemand.recentRequestCount ?? null,
            requestWindowSeconds: directDemand.requestWindowSeconds ?? null,
            requestRate: directDemand.requestRate ?? null,
            inFlightRequests: directDemand.inFlightRequests ?? null,
            requestQueueDepth: directDemand.requestQueueDepth ?? null,
            rejectedRequests: directDemand.rejectedRequests ?? null,
            recentRejectedRequests: directDemand.recentRejectedRequests ?? null,
          }
        : {};
    const enriched = {
      ...serviceData,
      ...(persistedSummary || {}),
      ...(liveSummary || {}),
      ...directOnlyFields,
      ...directDemandMetadata,
      ...directDemandMetrics,
      ...persistedPricing,
      ...(legacy || {}),
      replicas: currentReplicas,
      enrichmentUnavailable: replicaData.liveSummaryUnavailable,
      replicaSummaryUnavailable: replicaData.replicaSummaryUnavailable,
      pricingUnavailable:
        !legacy &&
        pricingData.aggregateUnavailable &&
        !pricingAggregateIsGood(pricingData.aggregate),
      pricingRefreshUnavailable:
        !legacy && pricingData.aggregateRefreshUnavailable,
      pricingUnavailableReason:
        !legacy && pricingData.aggregateUnavailableReason,
      serviceYamlUnavailable: !legacy && Boolean(anchoredHash),
    };
    if (persistedPricing.estimatedHourlyCost != null) {
      enriched.costPerThousandRequests =
        enriched.requestRate > 0
          ? (persistedPricing.estimatedHourlyCost * 1000) /
            (enriched.requestRate * 3600)
          : null;
    } else if (!legacy && pricingData.aggregate) {
      enriched.costPerThousandRequests = null;
    }
    // A live controller summary settles metadata-dependent cells but cannot
    // erase an independently landed replica summary or page.
    if (liveSummary || legacy) enriched.metadataOnly = false;
    return enriched;
  }, [
    demand.demandData,
    ownsRouteState,
    pricingData,
    replicaData,
    serviceData,
    serviceName,
  ]);
  const isRouteLoading = !router.isReady || !ownsRouteState || isInitialLoad;

  const handleManualRefresh = async () => {
    setIsRefreshing(true);
    await Promise.all([
      refreshData(),
      demand.refreshDemand(),
      refreshHistory(),
      replicaData.refreshReplicas(),
      pricingData.refreshPricing(),
    ]);
    setIsRefreshing(false);
  };

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
            {currentServiceData && activeTab !== 'placement' && (
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

        {currentServiceData && (
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

        {isRouteLoading ? (
          <div className="flex justify-center items-center py-12">
            <CircularProgress size={24} className="mr-2" />
            <span className="text-gray-500">Loading service details...</span>
          </div>
        ) : currentServiceData ? (
          activeTab === 'versions' ? (
            <ServiceVersionHistory
              serviceName={serviceName}
              onElectionComplete={refreshData}
            />
          ) : activeTab === 'placement' ? (
            <ServicePlacement serviceName={serviceName} />
          ) : (
            <>
              {currentServiceData.enrichmentUnavailable && (
                <div
                  role="alert"
                  className="mb-4 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900"
                >
                  Live target, request, or endpoint details could not be loaded.
                  Persisted replica state and independently loaded history
                  remain available. Refresh to retry.
                </div>
              )}
              <ServiceDetailCard
                serviceData={currentServiceData}
                requestHistory={replicaHistory}
                pricingLoading={pricingData.aggregateLoading}
              />
              <AcceleratorCapacityCard serviceData={currentServiceData} />
              <ServeHistorySection
                key={serviceName}
                history={replicaHistory}
                loading={historyLoading}
                onHoursChange={loadHistoryHours}
              />
              <ReplicaPlacementCard
                replicas={currentServiceData.replicas}
                unavailable={replicaData.currentPage.unavailable}
                loading={replicaData.currentPage.loading}
                currentOnly
                partial={Boolean(replicaData.currentPage.nextCursor)}
              />
              <ReplicasCard
                replicas={
                  replicaData.legacyService?.replicas ??
                  currentServiceData.replicas
                }
                loading={replicaData.currentPage.loading}
                unavailable={replicaData.currentPage.unavailable}
                refreshUnavailable={replicaData.currentPage.refreshUnavailable}
                currentTotal={
                  replicaData.legacyService
                    ? undefined
                    : replicaData.currentPage.total
                }
                currentNextCursor={
                  replicaData.legacyService
                    ? undefined
                    : replicaData.currentPage.nextCursor
                }
                currentLoadingMore={
                  !replicaData.legacyService &&
                  replicaData.currentPage.loadingMore
                }
                onLoadMoreCurrent={
                  replicaData.legacyService
                    ? undefined
                    : replicaData.loadMoreCurrent
                }
                pastReplicas={
                  replicaData.legacyService
                    ? undefined
                    : replicaData.pastPage.replicas
                }
                pastTotal={
                  replicaData.legacyService
                    ? undefined
                    : (replicaData.pastPage.total ??
                      replicaData.replicaSummary?.pastAttemptCount ??
                      (replicaData.liveService
                        ? getPastAttemptCount(currentServiceData)
                        : null))
                }
                pastLoading={
                  !replicaData.legacyService && replicaData.pastPage.loading
                }
                pastLoadingMore={
                  !replicaData.legacyService && replicaData.pastPage.loadingMore
                }
                pastUnavailable={
                  !replicaData.legacyService && replicaData.pastPage.unavailable
                }
                pastRefreshUnavailable={
                  !replicaData.legacyService &&
                  replicaData.pastPage.refreshUnavailable
                }
                pastNextCursor={
                  replicaData.legacyService
                    ? undefined
                    : replicaData.pastPage.nextCursor
                }
                onOpenPast={
                  replicaData.legacyService
                    ? undefined
                    : replicaData.openPastAttempts
                }
                onLoadMorePast={
                  replicaData.legacyService
                    ? undefined
                    : replicaData.loadMorePast
                }
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

const PRICING_EXCLUSION_LABELS = {
  missing_version_catalog: 'missing version catalog',
  unsupported_version_catalog: 'unsupported version catalog',
  invalid_version_catalog: 'invalid version catalog',
  catalog_too_large: 'catalog too large',
  missing_location: 'missing placement location',
  invalid_location: 'invalid placement location',
  location_not_in_version_catalog: 'location absent from version catalog',
  ambiguous_legacy_location: 'ambiguous legacy location',
  catalog_price_unavailable: 'catalog price unavailable',
  purchase_option_mismatch: 'purchase option mismatch',
  unknown_node_count: 'unknown node count',
  pricing_identity_too_large: 'pricing identity too large',
  not_current_or_uncertain: 'replica no longer current',
  kubernetes: 'Kubernetes',
};

function pricingReasonLabel(reason) {
  if (!reason) return 'unknown price';
  return PRICING_EXCLUSION_LABELS[reason] || reason.replaceAll('_', ' ');
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
  const pastAttemptCount = getPastAttemptCount(serviceData);
  const metadataDeferred = serviceData.metadataOnly === true;
  const metadataUnavailable = serviceData.enrichmentUnavailable === true;
  const deferredValue = (
    <span className="text-gray-400">
      {metadataUnavailable ? 'Unavailable' : 'Loading...'}
    </span>
  );
  const hourlyCostDetails = [];
  const usesVersionCatalog = serviceData.priceBasis === 'version_catalog';
  const nonSpotHourlyCost = usesVersionCatalog
    ? serviceData.nonSpotHourlyCost
    : serviceData.onDemandHourlyCost;
  if (serviceData.spotHourlyCost > 0) {
    hourlyCostDetails.push(`Spot ${formatUsd(serviceData.spotHourlyCost)}/hr`);
  }
  if (nonSpotHourlyCost > 0) {
    hourlyCostDetails.push(
      `${usesVersionCatalog ? 'Non-Spot' : 'On-demand'} ${formatUsd(
        nonSpotHourlyCost
      )}/hr`
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
  if (usesVersionCatalog) {
    hourlyCostDetails.push(
      "Each replica version's deployment catalog; reserved $0 from persisted placement provenance; compute estimate, not a provider bill"
    );
  } else if (serviceData.estimatedHourlyCost != null) {
    hourlyCostDetails.push(
      'Current catalog, compute only, not a provider bill'
    );
  }

  const excludedCostDetails = Object.entries(
    serviceData.hourlyCostExclusionReasons || {}
  ).map(([reason, count]) => {
    const label = pricingReasonLabel(reason);
    return `${count} ${label} replica${count === 1 ? '' : 's'} excluded`;
  });

  const requestDetails = [];
  const usesLogicalReplicas = serviceData.replicaUnit === 'logical';
  const logicalCapacityUnverified =
    usesLogicalReplicas &&
    serviceData.replicasReady != null &&
    serviceData.status !== 'READY';
  const staleLogicalCapacityObservation =
    usesLogicalReplicas &&
    serviceData.observedReadyReplicas != null &&
    serviceData.observedReadyReplicasFresh === false;
  if (serviceData.inFlightRequests != null && serviceData.requestRate != null) {
    requestDetails.push(`${formatRequestRate(serviceData.requestRate)} recent`);
  }
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
  if (
    serviceData.requestTelemetryState != null &&
    serviceData.requestTelemetryState !== 'fresh'
  ) {
    const legacySuffix =
      serviceData.recentRequestCount != null
        ? '; showing controller snapshot'
        : '';
    requestDetails.push(
      `request telemetry ${serviceData.requestTelemetryState}${legacySuffix}`
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
              <div className="mt-1 flex flex-wrap items-center gap-2 text-base">
                <ServiceHealthBadge service={serviceData} />
                <span className="text-xs text-gray-500">SkyServe state:</span>
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
                {metadataDeferred && serviceData.uptime == null
                  ? deferredValue
                  : formatUptime(serviceData.uptime)}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                {usesLogicalReplicas
                  ? 'Fleet-wide logical slots (ready/non-failed)'
                  : 'Replicas (ready/non-failed)'}
              </div>
              <div className="text-base mt-1">
                {serviceData.replicasReady == null ? (
                  <span className="text-gray-400">
                    {metadataUnavailable
                      ? 'Replica health unavailable.'
                      : 'Loading replica health...'}
                  </span>
                ) : (
                  <>
                    {serviceData.replicasReady}/{serviceData.replicasTotal}
                    {serviceData.targetReplicas != null && (
                      <span className="text-gray-500">
                        {' '}
                        (target: {serviceData.targetReplicas})
                      </span>
                    )}
                  </>
                )}
              </div>
              {pastAttemptCount > 0 && (
                <div className="mt-1 text-sm text-gray-500">
                  {pastAttemptCount} past{' '}
                  {pastAttemptCount === 1 ? 'attempt' : 'attempts'} retained for
                  operational history. SkyServe replaced them automatically, so
                  no action is required while the serving target remains met.
                </div>
              )}
              {usesLogicalReplicas &&
                serviceData.physicalReplicasReady != null && (
                  <div className="text-sm text-gray-500 mt-1">
                    {serviceData.physicalReplicasReady}/
                    {serviceData.physicalReplicasTotal} physical backends
                    {' (ready/non-failed)'}
                  </div>
                )}
              {usesLogicalReplicas && (
                <div className="text-sm text-gray-500 mt-1">
                  Logical slots span every cloud and Kubernetes context in this
                  service; they are not a GPU count for the current cluster.
                </div>
              )}
              {staleLogicalCapacityObservation && (
                <div
                  role="alert"
                  className="mt-2 rounded-md border border-amber-200 bg-amber-50 px-2 py-1.5 text-sm text-amber-900"
                >
                  The last load-balancer observation (
                  {serviceData.observedReadyReplicas} logical slots) is stale
                  {serviceData.requestStatsAgeSeconds != null
                    ? ` (${Math.round(
                        serviceData.requestStatsAgeSeconds
                      )}s old)`
                    : ''}
                  . The count above comes from current replica records.
                </div>
              )}
              {logicalCapacityUnverified && (
                <div
                  role="alert"
                  className="mt-2 rounded-md border border-amber-200 bg-amber-50 px-2 py-1.5 text-sm text-amber-900"
                >
                  Routing readiness is unverified while SkyServe state is{' '}
                  {serviceData.status}. These are controller-recorded slots, not
                  confirmed live GPU endpoints.
                </div>
              )}
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Endpoint
              </div>
              <div className="text-base mt-1">
                {metadataDeferred ? (
                  deferredValue
                ) : (
                  <EndpointCell endpoint={serviceData.endpoint} />
                )}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">Policy</div>
              <div className="text-base mt-1">
                {metadataDeferred && !serviceData.policy
                  ? deferredValue
                  : serviceData.policy || '-'}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Load Balancing Policy
              </div>
              <div className="text-base mt-1">
                {metadataDeferred && !serviceData.loadBalancingPolicy
                  ? deferredValue
                  : serviceData.loadBalancingPolicy || '-'}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Requested Resources
              </div>
              <div className="text-base mt-1">
                {metadataDeferred && !serviceData.requestedResources
                  ? deferredValue
                  : serviceData.requestedResources || '-'}
              </div>
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Estimated tracked compute cost
              </div>
              <div className="text-base mt-1">
                {serviceData.estimatedHourlyCost != null
                  ? `${formatUsd(serviceData.estimatedHourlyCost)}${
                      serviceData.pricingCoverage === 'partial' ? '+' : ''
                    }/hr`
                  : metadataDeferred
                    ? deferredValue
                    : pricingLoading
                      ? 'Loading replica prices...'
                      : serviceData.pricingUnavailable
                        ? 'Pricing unavailable'
                        : serviceData.pricingCoverage === 'none'
                          ? 'Unknown'
                          : '-'}
              </div>
              {serviceData.pricingRefreshUnavailable && (
                <div className="text-xs text-amber-700 mt-1">
                  Pricing refresh failed. Showing the last available estimate.
                </div>
              )}
              {hourlyCostDetails.length > 0 && (
                <div className="text-xs text-gray-500 mt-1">
                  {hourlyCostDetails.join(' · ')}
                </div>
              )}
            </div>
            <div>
              <div className="text-gray-600 font-medium text-base">
                Requests now
              </div>
              <div className="text-base mt-1">
                {serviceData.inFlightRequests != null
                  ? `${serviceData.inFlightRequests.toLocaleString()} processing`
                  : serviceData.requestRate != null
                    ? formatRequestRate(serviceData.requestRate)
                    : metadataDeferred
                      ? deferredValue
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
                  : metadataDeferred
                    ? deferredValue
                    : serviceData.hourlyCostExcludedReplicaCount > 0
                      ? 'Unknown'
                      : serviceData.pricingUnavailable
                        ? 'Pricing unavailable'
                        : '-'}
              </div>
              <div className="text-xs text-gray-500 mt-1">
                {metadataDeferred
                  ? metadataUnavailable
                    ? 'Request and pricing data are unavailable. Refresh to retry.'
                    : 'Loading request and pricing data.'
                  : serviceData.pricingUnavailable
                    ? `Pricing could not be loaded${
                        serviceData.pricingUnavailableReason
                          ? ` (${pricingReasonLabel(
                              serviceData.pricingUnavailableReason
                            )})`
                          : ''
                      }. Refresh to retry.`
                    : serviceData.pricingCoverage === 'empty'
                      ? 'No cost-tracked replicas; current tracked compute cost is $0.'
                      : serviceData.pricingCoverage === 'none'
                        ? `No tracked replica price could be resolved${
                            excludedCostDetails.length
                              ? ` · ${excludedCostDetails.join(' · ')}`
                              : ''
                          }`
                        : excludedCostDetails.length > 0
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
                {metadataDeferred && serviceData.electedVersion == null
                  ? deferredValue
                  : (serviceData.electedVersion ?? '-')}
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
                  : metadataDeferred
                    ? deferredValue
                    : '-'}
              </div>
            </div>
            {!serviceData.serviceYaml && serviceData.serviceYamlUnavailable && (
              <div>
                <div className="text-gray-600 font-medium text-base">
                  SkyPilot YAML
                </div>
                <div className="text-base mt-1 text-gray-500">
                  Not loaded in the bounded replica view.
                </div>
              </div>
            )}
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

export function ReplicaPlacementCard({
  replicas,
  loading,
  unavailable = false,
  currentOnly = false,
  partial = false,
}) {
  const rows = getReplicaPlacementBreakdown(replicas);
  const placementColumns = currentOnly
    ? REPLICA_PLACEMENT_COLUMNS.filter(
        (column) => column.key !== 'historicalFailure'
      )
    : REPLICA_PLACEMENT_COLUMNS;

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h3 className="text-lg font-semibold">
            Replica attempts by placement
          </h3>
          <p className="text-sm text-gray-500">
            {currentOnly
              ? `Selected or confirmed placement for the ${
                  partial ? 'loaded page of ' : 'loaded '
                }current or uncertain replicas. These are loaded-row counts, not fleet totals.`
              : 'Selected or confirmed placement for every tracked attempt. Queued intent and retained failure history are not live-machine counts.'}
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
                {placementColumns.map((column) => (
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
                  {currentOnly ? 'Loaded rows' : 'Tracked attempts'}
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.length > 0 ? (
                rows.map((row) => (
                  <TableRow key={`${row.cloud}/${row.region}`}>
                    <TableCell className="font-medium">{row.cloud}</TableCell>
                    <TableCell>{row.region}</TableCell>
                    {placementColumns.map((column) => (
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
                    colSpan={placementColumns.length + 4}
                    className="text-center py-6 text-gray-500"
                  >
                    {loading
                      ? 'Loading attempt placement…'
                      : unavailable
                        ? 'Replica placement unavailable. Refresh to retry.'
                        : 'No replicas.'}
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
  createdAt: (replica) => replica.createdAt,
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

export function ReplicasCard({
  replicas,
  loading,
  unavailable = false,
  refreshUnavailable = false,
  currentTotal,
  currentNextCursor = null,
  currentLoadingMore = false,
  onLoadMoreCurrent,
  pastReplicas,
  pastTotal,
  pastLoading = false,
  pastLoadingMore = false,
  pastUnavailable = false,
  pastRefreshUnavailable = false,
  pastNextCursor = null,
  onOpenPast,
  onLoadMorePast,
}) {
  const [sortConfig, setSortConfig] = useState({
    key: 'id',
    direction: 'ascending',
  });
  const sortedReplicas = useMemo(
    () => sortReplicas(replicas, sortConfig),
    [replicas, sortConfig]
  );
  const paginated = pastReplicas !== undefined || currentTotal !== undefined;
  const currentReplicas = useMemo(() => {
    if (paginated) return sortedReplicas;
    return sortedReplicas.filter(
      (replica) => !REPLICA_HISTORICAL_FAILURE_STATUSES.has(replica.status)
    );
  }, [paginated, sortedReplicas]);
  const historicalReplicas = useMemo(() => {
    if (paginated) return Array.isArray(pastReplicas) ? pastReplicas : [];
    return (Array.isArray(replicas) ? replicas : []).filter((replica) =>
      REPLICA_HISTORICAL_FAILURE_STATUSES.has(replica.status)
    );
  }, [paginated, pastReplicas, replicas]);
  const boundedHistoricalReplicas = useMemo(() => {
    const mostRecent = [...historicalReplicas].sort(
      (left, right) => Number(right.id) - Number(left.id)
    );
    if (!paginated) mostRecent.splice(50);
    return sortReplicas(mostRecent, sortConfig);
  }, [historicalReplicas, paginated, sortConfig]);
  const resolvedPastTotal = paginated ? pastTotal : historicalReplicas.length;

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

  const renderReplicaTable = (rows, emptyMessage, pastAttempt = false) => (
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
              {sortableHeader('Created', 'createdAt')}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length > 0 ? (
              rows.map((replica) => (
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
                    ) : replica.directProjection ? (
                      <span className="text-gray-400">Not loaded</span>
                    ) : (
                      '-'
                    )}
                  </TableCell>
                  <TableCell>
                    {pastAttempt && replica.directProjection ? (
                      <span className="text-gray-400">Not available</span>
                    ) : replica.hourlyCost != null ? (
                      <NonCapitalizedTooltip
                        content={
                          replica.priceSource === 'zero_cost_provenance'
                            ? 'Reserved zero-cost placement provenance'
                            : replica.priceSource === 'version_catalog'
                              ? "This replica version's deployment catalog"
                              : 'Current catalog estimate'
                        }
                      >
                        <span>
                          {formatUsd(replica.hourlyCost)}
                          {replica.priceSource === 'zero_cost_provenance'
                            ? ' reserved'
                            : ''}
                        </span>
                      </NonCapitalizedTooltip>
                    ) : replica.pricingState === 'loading' ? (
                      <span className="text-gray-400">Loading...</span>
                    ) : replica.pricingState === 'excluded' ? (
                      <NonCapitalizedTooltip
                        content={pricingReasonLabel(
                          replica.hourlyCostExclusionReason
                        )}
                      >
                        <span>Excluded</span>
                      </NonCapitalizedTooltip>
                    ) : replica.pricingState === 'unavailable' ? (
                      <span className="text-gray-400">Unavailable</span>
                    ) : replica.directProjection ? (
                      'Not loaded'
                    ) : (
                      '-'
                    )}
                  </TableCell>
                  <TableCell>{replica.region || '-'}</TableCell>
                  <TableCell>
                    {replica.directProjection && !replica.endpoint ? (
                      <span className="text-gray-400">Not loaded</span>
                    ) : (
                      <EndpointCell endpoint={replica.endpoint} />
                    )}
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
                      : replica.directProjection
                        ? 'Not loaded'
                        : '-'}
                  </TableCell>
                  <TableCell>
                    {replica.createdAt
                      ? formatFullTimestamp(new Date(replica.createdAt * 1000))
                      : '-'}
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={10}
                  className="text-center py-6 text-gray-500"
                >
                  {emptyMessage}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </Card>
  );

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-lg font-semibold">Replicas</h3>
        <div className="text-sm text-gray-500">
          {currentTotal != null && (
            <span className="mr-3">
              Showing {currentReplicas.length} of {currentTotal} current or
              uncertain
            </span>
          )}
          {loading && (
            <span className="text-sm text-gray-500">
              <CircularProgress size={14} className="mr-2" />
              Loading replicas…
            </span>
          )}
        </div>
      </div>
      {refreshUnavailable && currentReplicas.length > 0 && (
        <p className="mb-2 text-sm text-amber-700">
          Replica refresh failed. Showing the last available page.
        </p>
      )}
      {renderReplicaTable(
        currentReplicas,
        loading
          ? 'Replica details are loading.'
          : unavailable
            ? 'Replica details unavailable. Refresh to retry.'
            : 'No current replicas.'
      )}
      {currentNextCursor && (
        <button
          type="button"
          onClick={onLoadMoreCurrent}
          disabled={loading || currentLoadingMore}
          className="mt-2 text-sm font-medium text-sky-blue hover:text-sky-blue-bright disabled:text-gray-400"
        >
          {currentLoadingMore ? 'Loading more replicas…' : 'Load more replicas'}
        </button>
      )}
      {(resolvedPastTotal == null ||
        resolvedPastTotal > 0 ||
        historicalReplicas.length > 0) && (
        <details
          className="mt-3 rounded-lg border bg-white px-4 py-3"
          onToggle={(event) => {
            if (event.currentTarget.open) void onOpenPast?.();
          }}
        >
          <summary className="cursor-pointer font-medium text-gray-700">
            {`Past attempts (${
              resolvedPastTotal == null ? 'count loading' : resolvedPastTotal
            })`}
          </summary>
          <p className="mb-3 mt-2 text-sm text-gray-500">
            These replaced attempts are retained as diagnostic history. They do
            not indicate a current incident while the service is meeting its
            serving target, and no action is normally required.
          </p>
          {pastRefreshUnavailable && historicalReplicas.length > 0 && (
            <p className="mb-2 text-sm text-amber-700">
              Past-attempt refresh failed. Showing the last available page.
            </p>
          )}
          {renderReplicaTable(
            boundedHistoricalReplicas,
            pastLoading
              ? 'Past attempts are loading.'
              : pastUnavailable
                ? 'Past attempts unavailable. Refresh to retry.'
                : 'No past attempts.',
            true
          )}
          {!paginated &&
            historicalReplicas.length > boundedHistoricalReplicas.length && (
              <p className="mt-2 text-sm text-gray-500">
                Showing the 50 most recent attempts.
              </p>
            )}
          {pastNextCursor && (
            <button
              type="button"
              onClick={onLoadMorePast}
              disabled={pastLoading || pastLoadingMore}
              className="mt-2 text-sm font-medium text-sky-blue hover:text-sky-blue-bright disabled:text-gray-400"
            >
              {pastLoadingMore
                ? 'Loading more past attempts…'
                : 'Load more past attempts'}
            </button>
          )}
        </details>
      )}
    </div>
  );
}

export default ServiceDetails;
