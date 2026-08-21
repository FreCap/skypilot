'use client';

import React, {
  useState,
  useEffect,
  useMemo,
  useCallback,
  useRef,
} from 'react';
import PropTypes from 'prop-types';
import { CircularProgress } from '@mui/material';
import { Button } from '@/components/ui/button';
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from '@/components/ui/table';
import {
  getServiceReplicaSummaries,
  getServices,
} from '@/data/connectors/services';
import { REFRESH_INTERVALS } from '@/lib/config';
import { sortData } from '@/data/utils';
import { RotateCwIcon, CopyIcon, CheckIcon } from 'lucide-react';
import { useMobile } from '@/hooks/useMobile';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';
import { Card } from '@/components/ui/card';
import Link from 'next/link';
import {
  CustomTooltip as Tooltip,
  LastUpdatedTimestamp,
  formatDuration,
} from '@/components/utils';
import dashboardCache from '@/lib/cache';

const REFRESH_INTERVAL = REFRESH_INTERVALS.REFRESH_INTERVAL;
const SERVICE_METADATA_ARGS = [{ metadataOnly: true }];
const SERVICE_SUMMARY_ARGS = [{ summaryOnly: true, includeEndpoints: true }];
const SERVICE_REPLICA_SUMMARY_ARGS = [{}];

const PAST_ATTEMPT_STATUSES = new Set([
  'FAILED',
  'FAILED_INITIAL_DELAY',
  'FAILED_PROBING',
  'FAILED_PROVISION',
]);

const ACTIVE_RECOVERY_STATUSES = new Set([
  'PENDING',
  'PROVISIONING',
  'STARTING',
  'NOT_READY',
  'PREEMPTED',
  'SHUTTING_DOWN',
]);

const CLEANUP_UNCERTAIN_STATUSES = new Set(['FAILED_CLEANUP']);
const UNKNOWN_REPLICA_STATUSES = new Set(['UNKNOWN']);

function countStatuses(service, statuses) {
  if (service.replicaStatusCounts) {
    return Object.entries(service.replicaStatusCounts)
      .filter(([status]) => statuses.has(status))
      .reduce((total, [, count]) => total + Number(count || 0), 0);
  }
  return (service.replicas || []).filter((replica) =>
    statuses.has(replica.status)
  ).length;
}

export function getPastAttemptCount(service) {
  if (Number.isInteger(service.pastAttemptCount)) {
    return service.pastAttemptCount;
  }
  return countStatuses(service, PAST_ATTEMPT_STATUSES);
}

export function getServiceOperationalState(service) {
  const cleanupCount = countStatuses(service, CLEANUP_UNCERTAIN_STATUSES);
  const unknownCount = countStatuses(service, UNKNOWN_REPLICA_STATUSES);
  const rawStatus = service.status || 'UNKNOWN';
  if (rawStatus === 'FAILED_CLEANUP' || cleanupCount > 0) {
    const cleanupSubject =
      cleanupCount > 0
        ? `${cleanupCount} replica cleanup ${
            cleanupCount === 1 ? 'record needs' : 'records need'
          }`
        : 'Service cleanup needs';
    return {
      label: 'Cleanup needs verification',
      tone: 'warning',
      detail: `${cleanupSubject} verification. Cloud resources may require manual cleanup.`,
    };
  }
  if (unknownCount > 0) {
    return {
      label: 'Replica state needs verification',
      tone: 'warning',
      detail: `${unknownCount} replica ${
        unknownCount === 1 ? 'has' : 'have'
      } an unknown current state. Inspect replica and provider state.`,
    };
  }
  if (['CONTROLLER_FAILED', 'FAILED'].includes(rawStatus)) {
    return {
      label: 'Needs attention',
      tone: 'danger',
      detail:
        'The service controller is in a terminal failure state. Inspect the service logs and placement details.',
    };
  }
  if (service.replicasReady == null) {
    const enrichmentUnavailable = service.enrichmentUnavailable === true;
    if (rawStatus === 'READY') {
      return {
        label: 'Serving',
        tone: 'success',
        detail: enrichmentUnavailable
          ? 'The service is serving. Replica health is temporarily unavailable. Refresh to retry.'
          : 'The service is serving. Target and replica health are still loading.',
      };
    }
    return {
      label: rawStatus === 'CONTROLLER_INIT' ? 'Starting' : rawStatus,
      tone: 'neutral',
      detail: enrichmentUnavailable
        ? 'Replica health is temporarily unavailable. Refresh to retry.'
        : 'Replica health is still loading.',
    };
  }
  if (
    service.replicaUnit === 'logical' &&
    rawStatus !== 'READY' &&
    service.replicasReady > 0
  ) {
    return {
      label: 'Routing unverified',
      tone: 'warning',
      detail: `The controller records ${service.replicasReady}/${service.replicasTotal} logical slots, but SkyServe state is ${rawStatus}, not READY. Do not treat this snapshot as verified routable capacity.`,
    };
  }
  if (
    rawStatus === 'READY' &&
    service.targetReplicas != null &&
    service.replicasReady >= service.targetReplicas
  ) {
    const pastAttemptCount = getPastAttemptCount(service);
    return {
      label: 'Healthy',
      tone: 'success',
      detail: `${service.replicasReady}/${service.targetReplicas} target replicas are ready.${
        pastAttemptCount > 0
          ? ` ${pastAttemptCount} past ${
              pastAttemptCount === 1 ? 'attempt was' : 'attempts were'
            } replaced automatically.`
          : ''
      } No action is required.`,
    };
  }
  const activeRecoveryCount = countStatuses(service, ACTIVE_RECOVERY_STATUSES);
  if (activeRecoveryCount > 0) {
    return {
      label: 'Scaling automatically',
      tone: 'info',
      detail: `${activeRecoveryCount} replica ${
        activeRecoveryCount === 1 ? 'is' : 'are'
      } pending, starting, or being replaced. No action is normally required.`,
    };
  }
  if (rawStatus === 'READY') {
    return {
      label:
        service.targetReplicas == null ? 'Serving' : 'Recovery not yet visible',
      tone: service.targetReplicas == null ? 'success' : 'warning',
      detail:
        service.targetReplicas == null
          ? 'The service is serving. No autoscaler target was included in this snapshot.'
          : 'Ready capacity is below target, but this single snapshot does not prove recovery is stalled. Refresh or inspect placement if it persists.',
    };
  }
  return {
    label: rawStatus,
    tone: 'neutral',
    detail: 'The service is operating in this lifecycle state.',
  };
}

export function ServiceHealthBadge({ service }) {
  const health = getServiceOperationalState(service);
  const toneClasses = {
    success: 'bg-green-100 text-green-800',
    info: 'bg-blue-100 text-blue-800',
    warning: 'bg-amber-100 text-amber-900',
    danger: 'bg-red-100 text-red-800',
    neutral: 'bg-gray-100 text-gray-800',
  };
  return (
    <Tooltip content={`${health.detail} SkyServe state: ${service.status}.`}>
      <span
        className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${toneClasses[health.tone]}`}
      >
        {health.label}
      </span>
    </Tooltip>
  );
}

ServiceHealthBadge.propTypes = {
  service: PropTypes.object.isRequired,
};

function mergeMetadataWithPrevious(metadataRows, previousRows) {
  const previousByName = new Map(
    (previousRows || []).map((service) => [service.name, service])
  );
  return (metadataRows || []).map((metadata) => {
    const previous = previousByName.get(metadata.name);
    if (
      previous?.serviceHash &&
      metadata.serviceHash &&
      previous.serviceHash !== metadata.serviceHash
    ) {
      return metadata;
    }
    if (
      !previous ||
      (previous.metadataOnly && !previous.replicaSummaryLoaded)
    ) {
      return metadata;
    }
    return {
      ...metadata,
      ...previous,
      name: metadata.name,
      status: metadata.status,
      uptime: metadata.uptime,
      policy: metadata.policy,
      loadBalancingPolicy: metadata.loadBalancingPolicy,
      requestedResources: metadata.requestedResources,
      activeVersions: metadata.activeVersions,
      version: metadata.version,
      electedVersion: metadata.electedVersion,
      tlsEncrypted: metadata.tlsEncrypted,
      metadataOnly: previous.controllerSummaryLoaded !== true,
    };
  });
}

function mergeServiceRows(baseRows, enrichedRows) {
  const merged = new Map((baseRows || []).map((row) => [row.name, row]));
  (enrichedRows || []).forEach((row) => {
    const previous = merged.get(row.name);
    const identityChanged =
      previous?.serviceHash &&
      row.serviceHash &&
      previous.serviceHash !== row.serviceHash;
    merged.set(row.name, {
      ...(identityChanged ? {} : previous || {}),
      ...row,
      metadataOnly: false,
      controllerSummaryLoaded: true,
      enrichmentUnavailable: false,
    });
  });
  return Array.from(merged.values());
}

function mergeReplicaSummaryRows(
  baseRows,
  summaries,
  identityAuthoritative = false
) {
  const previousByName = new Map(
    (baseRows || []).map((row) => [row.name, row])
  );
  const merged = identityAuthoritative ? new Map() : new Map(previousByName);
  (summaries || []).forEach((summary) => {
    let previous = previousByName.get(summary.name);
    // New servers attach persisted lifecycle metadata to this direct
    // PostgreSQL projection, so it can own first paint without waiting for
    // controller transport. Older servers remain replica-only and are still
    // buffered until an identity-bearing controller row exists.
    if (!previous && !summary.persistedMetadataLoaded) return;
    if (identityAuthoritative && !summary.serviceHash) return;
    if (
      previous &&
      (identityAuthoritative
        ? previous.serviceHash !== summary.serviceHash
        : previous.serviceHash &&
          summary.serviceHash &&
          previous.serviceHash !== summary.serviceHash)
    ) {
      if (!identityAuthoritative) return;
      previous = undefined;
    }
    const liveReplicaFields =
      !identityAuthoritative && previous?.controllerSummaryLoaded
        ? {
            replicaUnit: previous.replicaUnit,
            replicaStatusCounts: previous.replicaStatusCounts,
            replicasReady: previous.replicasReady,
            replicasTotal: previous.replicasTotal,
            replicasFailed: previous.replicasFailed,
            physicalReplicasReady: previous.physicalReplicasReady,
            physicalReplicasTotal: previous.physicalReplicasTotal,
            physicalReplicasFailed: previous.physicalReplicasFailed,
          }
        : {};
    merged.set(summary.name, {
      ...(previous || {}),
      ...summary,
      ...liveReplicaFields,
      name: summary.name,
      serviceHash: previous?.serviceHash || summary.serviceHash,
      metadataOnly: previous?.metadataOnly ?? true,
      replicaSummaryLoaded: true,
      replicaSummaryUnavailable: false,
    });
  });
  return Array.from(merged.values());
}

function clearDirectReplicaSummary(rows) {
  return (rows || []).map((row) => {
    if (!row.replicaSummaryLoaded) return row;
    const cleared = {
      ...row,
      replicaSummaryLoaded: false,
      replicaSummaryUnavailable: false,
    };
    delete cleared.currentOrUncertainCount;
    delete cleared.pastAttemptCount;
    delete cleared.replicaCapacityCounts;
    delete cleared.observedAt;
    if (!row.controllerSummaryLoaded) {
      Object.assign(cleared, {
        replicaStatusCounts: null,
        replicasReady: null,
        replicasTotal: null,
        replicasFailed: null,
        physicalReplicasReady: null,
        physicalReplicasTotal: null,
        physicalReplicasFailed: null,
      });
    }
    return cleared;
  });
}

function markControllerEnrichmentPending(rows) {
  return (rows || []).map((row) => {
    const pending = {
      ...row,
      metadataOnly: true,
      controllerSummaryLoaded: false,
      enrichmentUnavailable: false,
    };
    // These fields have no PostgreSQL authority in the compact summary. Do
    // not present a prior controller observation as fresh while its successor
    // is pending or after it fails.
    delete pending.endpoint;
    delete pending.targetReplicas;
    delete pending.loadBalancingPolicy;
    return pending;
  });
}

function LoadingValue({ label, unavailable = false }) {
  return (
    <span className="text-gray-400" aria-label={label}>
      {unavailable ? 'Unavailable' : 'Loading...'}
    </span>
  );
}

LoadingValue.propTypes = {
  label: PropTypes.string.isRequired,
  unavailable: PropTypes.bool,
};

export function formatUptime(uptime) {
  // `uptime` is the epoch timestamp of when the service first became
  // ready (see sky/serve/serve_state.py set_service_uptime).
  if (!uptime || uptime <= 0) return '-';
  return formatDuration(Math.max(0, Date.now() / 1000 - uptime));
}

export function EndpointCell({ endpoint }) {
  const [isCopied, setIsCopied] = useState(false);

  if (!endpoint) {
    return <span>-</span>;
  }

  const copyEndpoint = async () => {
    try {
      await navigator.clipboard.writeText(endpoint);
      setIsCopied(true);
      setTimeout(() => setIsCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy endpoint to clipboard:', err);
    }
  };

  return (
    <span className="inline-flex items-center">
      <a
        href={endpoint}
        target="_blank"
        rel="noopener noreferrer"
        className="text-blue-600 hover:underline"
      >
        {endpoint}
      </a>
      <Tooltip
        content={isCopied ? 'Copied!' : 'Copy endpoint'}
        className="text-muted-foreground"
      >
        <button
          onClick={copyEndpoint}
          className="flex items-center text-gray-500 hover:text-gray-700 transition-colors duration-200 p-1 ml-1"
        >
          {isCopied ? (
            <CheckIcon className="w-4 h-4 text-green-600" />
          ) : (
            <CopyIcon className="w-4 h-4" />
          )}
        </button>
      </Tooltip>
    </span>
  );
}

EndpointCell.propTypes = {
  endpoint: PropTypes.string,
};

export function Services() {
  const [loading, setLoading] = useState(false);
  const refreshDataRef = useRef(null);
  const isMobile = useMobile();
  const [lastFetchedTime, setLastFetchedTime] = useState(null);

  const handleRefresh = () => {
    // The cache is args-keyed; drop every getServices variant
    // (summary and full) so refresh always refetches.
    dashboardCache.invalidateFunction(getServices);
    dashboardCache.invalidateFunction(getServiceReplicaSummaries);
    if (refreshDataRef.current) {
      refreshDataRef.current();
    }
  };

  return (
    <>
      <div className="flex items-center justify-between mb-4 h-5">
        <div className="text-base flex items-center">
          <Link
            href="/services"
            className="text-sky-blue hover:underline leading-none"
          >
            Services
          </Link>
        </div>
        <div className="flex items-center gap-3">
          {loading && (
            <div className="flex items-center">
              <CircularProgress size={15} className="mt-0" />
              <span className="ml-2 text-gray-500 text-sm">Loading...</span>
            </div>
          )}
          {!loading && lastFetchedTime && (
            <LastUpdatedTimestamp timestamp={lastFetchedTime} />
          )}
          <button
            onClick={handleRefresh}
            disabled={loading}
            className="text-sky-blue hover:text-sky-blue-bright flex items-center"
          >
            <RotateCwIcon className="h-4 w-4 mr-1.5" />
            {!isMobile && <span>Refresh</span>}
          </button>
        </div>
      </div>

      {/* Pass the state setter itself (stable identity) rather than an
          inline arrow: onFetched is a dependency of the table's fetchData
          callback, and a per-render closure would recreate fetchData on
          every parent render, re-triggering the fetch effect in a loop
          outside the 30s interval/visibility gate. */}
      <ServicesTable
        refreshInterval={REFRESH_INTERVAL}
        loading={loading}
        setLoading={setLoading}
        refreshDataRef={refreshDataRef}
        onFetched={setLastFetchedTime}
      />
    </>
  );
}

export function ServicesTable({
  refreshInterval,
  loading,
  setLoading,
  refreshDataRef,
  onFetched,
}) {
  const [data, setData] = useState([]);
  const [controllerStopped, setControllerStopped] = useState(false);
  const [sortConfig, setSortConfig] = useState({
    key: null,
    direction: 'ascending',
  });
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const requestVersionRef = useRef(0);
  const activeRequestRef = useRef(null);
  const controllerVersionRef = useRef(0);
  const controllerRequestRef = useRef(null);
  // null means the direct endpoint has not yet proven whether it can carry
  // persisted service identity. Old servers need the controller request to
  // own the compatibility paint, while current servers can refresh direct
  // state independently and fence stale controller enrichment.
  const directIdentityCapabilityRef = useRef(null);
  const replicaSummariesRef = useRef([]);
  const replicaSummaryIdentityAuthoritativeRef = useRef(false);

  const startControllerEnrichment = useCallback(() => {
    if (controllerRequestRef.current !== null) {
      return controllerRequestRef.current;
    }
    const controllerVersion = controllerVersionRef.current + 1;
    controllerVersionRef.current = controllerVersion;
    const owner = {
      version: controllerVersion,
      promise: null,
      summaryObservedAt: null,
    };
    const isCurrentControllerRequest = () =>
      controllerVersionRef.current === controllerVersion;
    const metadataPromise = dashboardCache
      .get(getServices, SERVICE_METADATA_ARGS)
      .then((metadataResponse) => {
        if (!isCurrentControllerRequest()) return;
        setData((previous) => {
          const metadataRows = mergeMetadataWithPrevious(
            metadataResponse.services || [],
            previous
          );
          return mergeReplicaSummaryRows(
            metadataRows,
            replicaSummariesRef.current,
            replicaSummaryIdentityAuthoritativeRef.current
          );
        });
        setControllerStopped(metadataResponse.controllerStopped || false);
        setIsInitialLoad(false);
      })
      .catch((error) => {
        if (isCurrentControllerRequest()) {
          console.error('Failed to fetch service metadata:', error);
        }
      });
    const summaryPromise = dashboardCache
      .get(getServices, SERVICE_SUMMARY_ARGS)
      .then((servicesResponse) => {
        if (!isCurrentControllerRequest()) return;
        setData((previous) => {
          const liveRows = mergeServiceRows(
            previous,
            servicesResponse.services || []
          );
          return mergeReplicaSummaryRows(
            liveRows,
            replicaSummariesRef.current,
            replicaSummaryIdentityAuthoritativeRef.current
          );
        });
        setControllerStopped(servicesResponse.controllerStopped || false);
        setIsInitialLoad(false);
        owner.summaryObservedAt = Date.now();
      })
      .catch((error) => {
        if (!isCurrentControllerRequest()) return;
        console.error('Failed to fetch service summaries:', error);
        setData((previous) =>
          previous.map((service) =>
            service.metadataOnly
              ? { ...service, enrichmentUnavailable: true }
              : service
          )
        );
      });
    let controllerPromise;
    controllerPromise = Promise.allSettled([
      metadataPromise,
      summaryPromise,
    ]).finally(() => {
      if (controllerRequestRef.current === owner) {
        controllerRequestRef.current = null;
      }
    });
    owner.promise = controllerPromise;
    controllerRequestRef.current = owner;
    return owner;
  }, []);

  const runFetch = useCallback(
    (requestVersion, kind) => {
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion;

      setLoading(true);
      // Persisted state owns first paint and refresh completion. Controller
      // metadata and endpoint/target fields are optional enrichment with a
      // separate singleflight, so a stalled controller cannot suppress direct
      // refreshes.
      if (directIdentityCapabilityRef.current === true) {
        setData(markControllerEnrichmentPending);
      }
      if (
        (kind === 'manual' || kind === 'visibility') &&
        directIdentityCapabilityRef.current === true
      ) {
        // A freshness boundary invalidates the old enrichment response, but
        // does not start a second controller request while the first is hung.
        controllerVersionRef.current += 1;
      }
      const controllerRequest = startControllerEnrichment();
      // Persisted identity is the authoritative freshness boundary. Bypass
      // stale-while-revalidate semantics so a failed read is surfaced instead
      // of repeatedly relabeling an old snapshot as current.
      dashboardCache.invalidate(
        getServiceReplicaSummaries,
        SERVICE_REPLICA_SUMMARY_ARGS
      );
      const replicaSummaryPromise = dashboardCache
        .get(getServiceReplicaSummaries, SERVICE_REPLICA_SUMMARY_ARGS)
        .then((response) => {
          if (!isCurrentRequest()) return true;
          if (response.legacyFallback) {
            directIdentityCapabilityRef.current = false;
            replicaSummaryIdentityAuthoritativeRef.current = false;
            replicaSummariesRef.current = [];
            setData(clearDirectReplicaSummary);
            return false;
          }
          replicaSummariesRef.current = response.summaries || [];
          if (response.available && response.serviceMetadataIncluded) {
            directIdentityCapabilityRef.current = true;
            replicaSummaryIdentityAuthoritativeRef.current = true;
            setData((previous) =>
              mergeReplicaSummaryRows(previous, response.summaries || [], true)
            );
            // The direct snapshot is the first useful paint. Controller-backed
            // endpoints and live autoscaler fields continue loading behind
            // their per-cell placeholders.
            setLoading(false);
            setIsInitialLoad(false);
            if (onFetched && Number.isFinite(response.observedAt)) {
              onFetched(new Date(response.observedAt * 1000));
            }
            return true;
          }
          directIdentityCapabilityRef.current = false;
          replicaSummaryIdentityAuthoritativeRef.current = false;
          setData((previous) =>
            mergeReplicaSummaryRows(previous, response.summaries || [])
          );
          return false;
        })
        .catch((error) => {
          if (!isCurrentRequest()) return true;
          const directIdentityWasProven =
            directIdentityCapabilityRef.current === true;
          if (!directIdentityWasProven) {
            directIdentityCapabilityRef.current = false;
          }
          console.error('Failed to fetch persisted replica summaries:', error);
          setData((previous) =>
            previous.map((service) => ({
              ...service,
              replicaSummaryUnavailable: true,
            }))
          );
          // A transport failure cannot revoke a capability already proved by
          // this mounted server. Keep the last authoritative identity and let
          // the persisted refresh owner settle so a later retry remains live.
          return directIdentityWasProven;
        })
        .then(async (directOwnsFirstPaint) => {
          // An old/non-consolidated server cannot supply persisted service
          // identity. Keep the initial/refresh owner pending until the
          // controller compatibility projection settles instead of briefly
          // painting an authoritative-looking empty list.
          if (!directOwnsFirstPaint) {
            let compatibilityControllerRequest = controllerRequest;
            await compatibilityControllerRequest.promise;
            if (
              isCurrentRequest() &&
              directIdentityCapabilityRef.current !== true &&
              compatibilityControllerRequest.version !==
                controllerVersionRef.current
            ) {
              // A rolling old-server/non-consolidated response may arrive
              // after a modern direct refresh fenced the prior controller
              // enrichment. Once that sole stale request settles, start
              // exactly one compatibility successor so the list can paint.
              compatibilityControllerRequest = startControllerEnrichment();
              await compatibilityControllerRequest.promise;
            }
            if (
              isCurrentRequest() &&
              directIdentityCapabilityRef.current !== true &&
              Number.isFinite(
                compatibilityControllerRequest.summaryObservedAt
              ) &&
              onFetched
            ) {
              // Only the compatibility path may use controller completion as
              // its freshness boundary. A modern persisted snapshot carries
              // its own server observation time and cannot be relabelled by a
              // late controller response.
              onFetched(
                new Date(compatibilityControllerRequest.summaryObservedAt)
              );
            }
          }
        })
        .finally(() => {
          if (!isCurrentRequest()) return;
          setLoading(false);
          setIsInitialLoad(false);
        });
      return replicaSummaryPromise;
    },
    [setLoading, onFetched, startControllerEnrichment]
  );

  const fetchData = useCallback(
    ({ kind = 'automatic' } = {}) => {
      const activeRequest = activeRequestRef.current;
      if (activeRequest !== null) {
        // Until the direct projection has proved it carries service identity,
        // the active request may be the old-server compatibility owner. Reuse
        // it even for a manual/visibility refresh so we do not fence the only
        // response capable of painting the service list.
        const compatibilityReadOwnsRefresh =
          directIdentityCapabilityRef.current !== true;
        const shouldReuse =
          compatibilityReadOwnsRefresh ||
          kind === 'automatic' ||
          activeRequest.kind === kind ||
          (kind === 'visibility' && activeRequest.kind === 'manual');
        if (shouldReuse) {
          return activeRequest.promise;
        }
      }

      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      const request = runFetch(requestVersion, kind);
      const owner = { kind, promise: request };
      activeRequestRef.current = owner;
      return request.finally(() => {
        if (activeRequestRef.current === owner) {
          activeRequestRef.current = null;
        }
      });
    },
    [runFetch]
  );

  const sortedData = useMemo(() => {
    return sortData(data, sortConfig.key, sortConfig.direction);
  }, [data, sortConfig]);

  // Expose fetchData to parent component
  useEffect(() => {
    if (refreshDataRef) {
      const refresh = () => fetchData({ kind: 'manual' });
      refreshDataRef.current = refresh;
      return () => {
        if (refreshDataRef.current === refresh) {
          refreshDataRef.current = null;
        }
      };
    }
    return undefined;
  }, [refreshDataRef, fetchData]);

  const refreshWhenVisible = useCallback(
    (source) => {
      if (source === 'visibilitychange') {
        const activeKind = activeRequestRef.current?.kind;
        if (activeKind === 'manual' || activeKind === 'visibility') {
          return;
        }
        dashboardCache.invalidate(getServices, SERVICE_SUMMARY_ARGS);
        dashboardCache.invalidate(getServices, SERVICE_METADATA_ARGS);
        dashboardCache.invalidate(
          getServiceReplicaSummaries,
          SERVICE_REPLICA_SUMMARY_ARGS
        );
        void fetchData({ kind: 'visibility' });
        return;
      }
      void fetchData();
    },
    [fetchData]
  );

  useVisibleRefreshInterval(
    Boolean(refreshInterval),
    refreshInterval,
    refreshWhenVisible
  );

  useEffect(() => {
    void fetchData();
    return () => {
      requestVersionRef.current += 1;
      controllerVersionRef.current += 1;
      activeRequestRef.current = null;
      controllerRequestRef.current = null;
      directIdentityCapabilityRef.current = null;
      replicaSummaryIdentityAuthoritativeRef.current = false;
    };
  }, [fetchData]);

  // Reset to first page when data changes
  useEffect(() => {
    setCurrentPage(1);
  }, [data.length]);

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const getSortDirection = (key) => {
    if (sortConfig.key === key) {
      return sortConfig.direction === 'ascending' ? ' ↑' : ' ↓';
    }
    return '';
  };

  const totalPages = Math.ceil(sortedData.length / pageSize);
  const startIndex = (currentPage - 1) * pageSize;
  const endIndex = startIndex + pageSize;
  const paginatedData = sortedData.slice(startIndex, endIndex);

  const goToPreviousPage = () => {
    setCurrentPage((page) => Math.max(page - 1, 1));
  };

  const goToNextPage = () => {
    setCurrentPage((page) => Math.min(page + 1, totalPages));
  };

  const handlePageSizeChange = (e) => {
    const newSize = parseInt(e.target.value, 10);
    setPageSize(newSize);
    setCurrentPage(1);
  };

  const sortableHeader = (label, sortKey) => (
    <TableHead
      className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50"
      onClick={() => requestSort(sortKey)}
    >
      {label}
      {getSortDirection(sortKey)}
    </TableHead>
  );

  const totalColSpan = 7;

  return (
    <div>
      <Card>
        <div className="overflow-x-auto rounded-lg">
          <Table className="min-w-full">
            <TableHeader>
              <TableRow>
                {sortableHeader('Name', 'name')}
                {sortableHeader('Status', 'status')}
                {sortableHeader('Replicas', 'replicasReady')}
                <TableHead className="whitespace-nowrap">Endpoint</TableHead>
                {sortableHeader('Uptime', 'uptime')}
                {sortableHeader('Policy', 'policy')}
                {sortableHeader('Resources', 'requestedResources')}
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading && isInitialLoad ? (
                <TableRow>
                  <TableCell
                    colSpan={totalColSpan}
                    className="text-center py-6 text-gray-500"
                  >
                    <div className="flex justify-center items-center">
                      <CircularProgress size={20} className="mr-2" />
                      <span>Loading...</span>
                    </div>
                  </TableCell>
                </TableRow>
              ) : paginatedData.length > 0 ? (
                paginatedData.map((service) => (
                  <TableRow key={service.name}>
                    <TableCell>
                      <Link
                        href={`/services/${encodeURIComponent(service.name)}`}
                        className="text-blue-600"
                      >
                        {service.name}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <ServiceHealthBadge service={service} />
                    </TableCell>
                    <TableCell>
                      {service.replicasReady == null ? (
                        <LoadingValue
                          label="Replica summary"
                          unavailable={service.enrichmentUnavailable}
                        />
                      ) : (
                        <>
                          {service.replicasReady}/{service.replicasTotal}
                          {getPastAttemptCount(service) > 0 && (
                            <Tooltip content="Past attempts are retained autoscaling and provisioning history. SkyServe replaced them automatically, so no action is required while the serving target remains met.">
                              <span className="ml-1 text-gray-500">
                                {getPastAttemptCount(service)} past attempts
                              </span>
                            </Tooltip>
                          )}
                        </>
                      )}
                    </TableCell>
                    <TableCell>
                      {service.metadataOnly ? (
                        <LoadingValue
                          label="Endpoint"
                          unavailable={service.enrichmentUnavailable}
                        />
                      ) : (
                        <EndpointCell endpoint={service.endpoint} />
                      )}
                    </TableCell>
                    <TableCell>
                      {service.metadataOnly && service.uptime == null ? (
                        <LoadingValue
                          label="Uptime"
                          unavailable={service.enrichmentUnavailable}
                        />
                      ) : (
                        formatUptime(service.uptime)
                      )}
                    </TableCell>
                    <TableCell>
                      {service.metadataOnly && !service.policy ? (
                        <LoadingValue
                          label="Policy"
                          unavailable={service.enrichmentUnavailable}
                        />
                      ) : (
                        service.policy || '-'
                      )}
                    </TableCell>
                    <TableCell>
                      {service.metadataOnly && !service.requestedResources ? (
                        <LoadingValue
                          label="Resources"
                          unavailable={service.enrichmentUnavailable}
                        />
                      ) : (
                        service.requestedResources || '-'
                      )}
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell
                    colSpan={totalColSpan}
                    className="text-center py-6 text-gray-500"
                  >
                    {controllerStopped
                      ? 'No services (the SkyServe controller is not up).'
                      : 'No services found. Launch one with `sky serve up`.'}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </Card>

      {/* Pagination controls */}
      {data.length > 0 && (
        <div className="flex justify-end items-center py-2 px-4 text-sm text-gray-700">
          <div className="flex items-center space-x-4">
            <div className="flex items-center">
              <span className="mr-2">Rows per page:</span>
              <div className="relative inline-block">
                <select
                  value={pageSize}
                  onChange={handlePageSizeChange}
                  className="py-1 pl-2 pr-6 appearance-none outline-none cursor-pointer border-none bg-transparent"
                  style={{ minWidth: '40px' }}
                >
                  <option value={10}>10</option>
                  <option value={30}>30</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                  <option value={200}>200</option>
                </select>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="h-4 w-4 text-gray-500 absolute right-0 top-1/2 transform -translate-y-1/2 pointer-events-none"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M19 9l-7 7-7-7"
                  />
                </svg>
              </div>
            </div>
            <div>
              {startIndex + 1} – {Math.min(endIndex, data.length)} of{' '}
              {data.length}
            </div>
            <div className="flex items-center space-x-2">
              <Button
                variant="ghost"
                size="icon"
                onClick={goToPreviousPage}
                disabled={currentPage === 1}
                className="text-gray-500 h-8 w-8 p-0"
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="chevron-left"
                >
                  <path d="M15 18l-6-6 6-6" />
                </svg>
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={goToNextPage}
                disabled={currentPage === totalPages || totalPages === 0}
                className="text-gray-500 h-8 w-8 p-0"
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="chevron-right"
                >
                  <path d="M9 18l6-6-6-6" />
                </svg>
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

ServicesTable.propTypes = {
  refreshInterval: PropTypes.number.isRequired,
  loading: PropTypes.bool.isRequired,
  setLoading: PropTypes.func.isRequired,
  refreshDataRef: PropTypes.shape({
    current: PropTypes.func,
  }).isRequired,
  onFetched: PropTypes.func,
};
