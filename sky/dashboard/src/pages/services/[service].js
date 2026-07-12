import React, { useState, useEffect, useCallback, useRef } from 'react';
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
  formatFullTimestamp,
} from '@/components/utils';
import { EndpointCell, formatUptime } from '@/components/services';
import { useMobile } from '@/hooks/useMobile';
import { formatYaml } from '@/lib/yamlUtils';
import { YamlCodeBlock } from '@/components/ui/yaml-code-block';

export function useServiceDetails({ serviceName }) {
  const [serviceData, setServiceData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [replicasLoading, setReplicasLoading] = useState(true);
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
        },
      ])
      .then(({ services }) => {
        if (!isCurrentRequest() || fullLanded) return;
        const found = (services || []).find((s) => s.name === serviceName);
        setServiceData(found || null);
      })
      .catch((error) => {
        console.error('Failed to fetch service summary:', error);
      })
      .finally(() => {
        if (isCurrentRequest()) {
          setLoading(false);
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

  const refreshData = useCallback(async () => {
    // Drop every args-keyed getServices variant (summary and full).
    dashboardCache.invalidateFunction(getServices);
    await fetchData();
  }, [fetchData]);

  return { serviceData, loading, replicasLoading, refreshData };
}

function ServiceDetails() {
  const router = useRouter();
  const { service: serviceName } = router.query;

  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const isMobile = useMobile();
  const { serviceData, loading, replicasLoading, refreshData } =
    useServiceDetails({
      serviceName,
    });

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
            <ServiceDetailCard serviceData={serviceData} />
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

function ServiceDetailCard({ serviceData }) {
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
                Replicas (ready/total)
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

function ReplicasCard({ replicas, loading }) {
  const replicaList = Array.isArray(replicas) ? replicas : [];

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
                <TableHead className="whitespace-nowrap">ID</TableHead>
                <TableHead className="whitespace-nowrap">Status</TableHead>
                <TableHead className="whitespace-nowrap">Version</TableHead>
                <TableHead className="whitespace-nowrap">Resources</TableHead>
                <TableHead className="whitespace-nowrap">Region</TableHead>
                <TableHead className="whitespace-nowrap">Endpoint</TableHead>
                <TableHead className="whitespace-nowrap">Launched</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {replicaList.length > 0 ? (
                replicaList.map((replica) => (
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
                    <TableCell>{replica.region || '-'}</TableCell>
                    <TableCell>
                      <EndpointCell endpoint={replica.endpoint} />
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
                    colSpan={7}
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
