import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
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

const ALL_FILTER_VALUE = 'all';

export const LOCATION_AVAILABILITY = {
  AVAILABLE_SPOT: 'available-spot',
  AVAILABLE_ON_DEMAND: 'available-on-demand',
  UNAVAILABLE: 'unavailable',
};

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
  const formatted = Object.entries(accelerators)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, count]) => `${name}:${count}`)
    .join(', ');
  return formatted || '-';
}

export function locationDisplayStatus(location) {
  if (location.probeEligible) return 'Probe eligible';
  if (location.storedStatus === 'PREEMPTED') return 'Benched';
  return 'Active';
}

export function locationAvailability(location) {
  const effectiveStatus = location.effectiveStatus || location.storedStatus;
  if (effectiveStatus !== 'ACTIVE') {
    return LOCATION_AVAILABILITY.UNAVAILABLE;
  }
  return location.useSpot
    ? LOCATION_AVAILABILITY.AVAILABLE_SPOT
    : LOCATION_AVAILABILITY.AVAILABLE_ON_DEMAND;
}

export function formatHourlyPrice(price) {
  return Number.isFinite(price)
    ? `$${price.toFixed(4)}/hr`
    : 'Price unavailable';
}

export function formatPlacementHourlyPrice(price, costUnit) {
  if (!Number.isFinite(price)) return 'Order price unavailable';
  const suffix =
    costUnit === 'gpu_slot_hour'
      ? 'GPU-hr'
      : costUnit === 'machine_hour'
        ? 'machine-hr'
        : 'ordering-unit';
  return `$${price.toFixed(4)}/${suffix}`;
}

function placementPrice(location) {
  return Number.isFinite(location.normalizedHourlyCost)
    ? location.normalizedHourlyCost
    : location.cachedHourlyCost;
}

function locationPageContractMatches(expected, actual, requestedOffset) {
  return (
    expected !== null &&
    expected.paginationVersion === actual.paginationVersion &&
    expected.orderSemantics === actual.orderSemantics &&
    expected.costUnit === actual.costUnit &&
    expected.orderGeneration === actual.orderGeneration &&
    expected.totalLocations === actual.totalLocations &&
    actual.pageOffset === requestedOffset
  );
}

function locationCards(location) {
  if (!location.accelerators || typeof location.accelerators !== 'object') {
    return [];
  }
  return Object.keys(location.accelerators).sort((left, right) =>
    left.localeCompare(right)
  );
}

export function filterPlacementLocations(locations, filters) {
  const maxPrice =
    filters.maxPrice === '' ? null : Number.parseFloat(filters.maxPrice);
  return locations.filter((location) => {
    if (
      filters.provider !== ALL_FILTER_VALUE &&
      location.cloud !== filters.provider
    ) {
      return false;
    }
    if (
      filters.region !== ALL_FILTER_VALUE &&
      (location.region || '-') !== filters.region
    ) {
      return false;
    }
    if (
      filters.card !== ALL_FILTER_VALUE &&
      !locationCards(location).includes(filters.card)
    ) {
      return false;
    }
    if (
      filters.availability !== ALL_FILTER_VALUE &&
      locationAvailability(location) !== filters.availability
    ) {
      return false;
    }
    if (maxPrice !== null && Number.isFinite(maxPrice) && maxPrice >= 0) {
      const price = placementPrice(location);
      return Number.isFinite(price) && price <= maxPrice;
    }
    return true;
  });
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

function FilterSelect({ label, value, options, onChange }) {
  return (
    <label className="min-w-36 text-xs font-medium text-gray-600">
      <span className="mb-1 block">{label}</span>
      <select
        aria-label={`${label} filter`}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="h-8 w-full rounded-md border border-gray-200 bg-white px-2 text-sm font-normal text-gray-800 focus:border-sky-blue focus:outline-none"
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function locationTooltip(location, costUnit) {
  const availability = locationAvailability(location);
  const availabilityLabel = {
    [LOCATION_AVAILABILITY.AVAILABLE_SPOT]: 'Eligible spot',
    [LOCATION_AVAILABILITY.AVAILABLE_ON_DEMAND]: 'Eligible on-demand',
    [LOCATION_AVAILABILITY.UNAVAILABLE]: 'Ineligible',
  }[availability];
  const details = [
    `Zone: ${location.zone || '-'}`,
    `Eligibility: ${availabilityLabel}`,
    `Instance type: ${location.instanceType || '-'}`,
    `Card: ${formatAccelerators(location.accelerators)}`,
    `Placement price: ${formatPlacementHourlyPrice(
      location.normalizedHourlyCost,
      costUnit
    )}`,
    `Machine price: ${formatHourlyPrice(location.cachedHourlyCost)}`,
    `Status: ${locationDisplayStatus(location)}`,
    `Stored: ${location.storedStatus || '-'} · Effective: ${location.effectiveStatus || '-'}`,
  ];
  if (location.benchReason) {
    details.push(`Bench reason: ${location.benchReason}`);
  }
  if (location.paidAdmission) {
    details.push(
      `Paid admission: ${location.paidAdmission.state || '-'} · pool remaining: ${location.paidAdmission.poolRemaining ?? '-'} · service remaining: ${location.paidAdmission.serviceRemaining ?? '-'}`
    );
  }
  if (location.nextProbeAt) {
    details.push(
      location.probeEligible
        ? `Probe eligible since ${timestamp(location.nextProbeAt)}`
        : `Next probe: ${timestamp(location.nextProbeAt)} (${relativeExpiry(location.nextProbeAt)})`
    );
  }
  return details.join('\n');
}

function LocationStatus({ location }) {
  const availability = locationAvailability(location);
  const label = {
    [LOCATION_AVAILABILITY.AVAILABLE_SPOT]: 'Eligible spot',
    [LOCATION_AVAILABILITY.AVAILABLE_ON_DEMAND]: 'Eligible on-demand',
    [LOCATION_AVAILABILITY.UNAVAILABLE]: 'Ineligible',
  }[availability];
  const tone =
    availability === LOCATION_AVAILABILITY.UNAVAILABLE ? 'error' : 'active';
  return (
    <div>
      <StatusPill tone={tone}>{label}</StatusPill>
      <div className="mt-1 text-xs text-gray-500">
        {locationDisplayStatus(location)}
        {location.paidAdmission?.state
          ? ` · paid ${location.paidAdmission.state}`
          : ''}
      </div>
    </div>
  );
}

function PlacerStateCard({ state, loadingMore, requestPending, onLoadMore }) {
  const [filters, setFilters] = useState({
    provider: ALL_FILTER_VALUE,
    region: ALL_FILTER_VALUE,
    card: ALL_FILTER_VALUE,
    availability: ALL_FILTER_VALUE,
    maxPrice: '',
  });
  const locations = useMemo(() => state.locations || [], [state.locations]);
  const optionValues = useMemo(() => {
    const providers = new Set();
    const regions = new Set();
    const cards = new Set();
    locations.forEach((location) => {
      providers.add(location.cloud || 'Unknown');
      regions.add(location.region || '-');
      locationCards(location).forEach((card) => cards.add(card));
    });
    return {
      providers: Array.from(providers).sort(),
      regions: Array.from(regions).sort(),
      cards: Array.from(cards).sort(),
    };
  }, [locations]);
  const filteredLocations = useMemo(
    () => filterPlacementLocations(locations, filters),
    [locations, filters]
  );
  const setFilter = (name, value) =>
    setFilters((current) => ({ ...current, [name]: value }));
  const hasActiveFilters =
    filters.provider !== ALL_FILTER_VALUE ||
    filters.region !== ALL_FILTER_VALUE ||
    filters.card !== ALL_FILTER_VALUE ||
    filters.availability !== ALL_FILTER_VALUE ||
    filters.maxPrice !== '';
  const hasTieredCatalogCostOrder =
    state.orderSemantics ===
    'catalog_normalized_cost_then_exact_backend_market_then_location_identity';
  const hasPreviousCatalogCostOrder =
    state.orderSemantics === 'catalog_normalized_cost_then_location_identity';
  const hasCatalogCostOrder =
    hasTieredCatalogCostOrder || hasPreviousCatalogCostOrder;

  return (
    <Card>
      <div className="border-b px-4 py-3">
        <h3 className="font-semibold">Candidate catalog — not launches</h3>
        <p className="mt-1 text-sm text-gray-500">
          {hasTieredCatalogCostOrder
            ? 'Catalog sorted by normalized cost; equal-price entries are grouped by exact accelerator shape and purchase market, then location identity. Actual selection applies ACTIVE status, requested accelerator card, zero-cost preference, paid admission, and frontier gates. '
            : hasPreviousCatalogCostOrder
              ? 'Catalog sorted by normalized cost; equal-price display order uses location identity. Actual selection applies ACTIVE status, requested accelerator card, zero-cost preference, paid admission, and frontier gates. '
              : 'This controller version does not report a normalized catalog-cost order. '}
          Eligible means the controller may attempt a launch; it does not
          promise live provider inventory. Benched and otherwise ineligible
          candidates remain visible.
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
        <>
          <div className="flex flex-wrap items-end gap-2 border-b bg-gray-50/50 px-4 py-3">
            <FilterSelect
              label="Provider"
              value={filters.provider}
              options={[
                { value: ALL_FILTER_VALUE, label: 'All providers' },
                ...optionValues.providers.map((provider) => ({
                  value: provider,
                  label: provider,
                })),
              ]}
              onChange={(value) => setFilter('provider', value)}
            />
            <FilterSelect
              label="Region"
              value={filters.region}
              options={[
                { value: ALL_FILTER_VALUE, label: 'All regions' },
                ...optionValues.regions.map((region) => ({
                  value: region,
                  label: region,
                })),
              ]}
              onChange={(value) => setFilter('region', value)}
            />
            <FilterSelect
              label="Card"
              value={filters.card}
              options={[
                { value: ALL_FILTER_VALUE, label: 'All cards' },
                ...optionValues.cards.map((card) => ({
                  value: card,
                  label: card,
                })),
              ]}
              onChange={(value) => setFilter('card', value)}
            />
            <FilterSelect
              label="Eligibility"
              value={filters.availability}
              options={[
                { value: ALL_FILTER_VALUE, label: 'All eligibility' },
                {
                  value: LOCATION_AVAILABILITY.AVAILABLE_SPOT,
                  label: 'Eligible spot',
                },
                {
                  value: LOCATION_AVAILABILITY.AVAILABLE_ON_DEMAND,
                  label: 'Eligible on-demand',
                },
                {
                  value: LOCATION_AVAILABILITY.UNAVAILABLE,
                  label: 'Ineligible',
                },
              ]}
              onChange={(value) => setFilter('availability', value)}
            />
            <label className="w-36 text-xs font-medium text-gray-600">
              <span className="mb-1 block">
                {hasCatalogCostOrder
                  ? 'Maximum placement price'
                  : 'Maximum machine price'}
              </span>
              <input
                type="number"
                min="0"
                step="0.0001"
                inputMode="decimal"
                aria-label="Maximum price filter"
                placeholder="$/hr"
                value={filters.maxPrice}
                onChange={(event) => setFilter('maxPrice', event.target.value)}
                className="h-8 w-full rounded-md border border-gray-200 bg-white px-2 text-sm font-normal text-gray-800 focus:border-sky-blue focus:outline-none"
              />
            </label>
            {hasActiveFilters && (
              <button
                type="button"
                onClick={() =>
                  setFilters({
                    provider: ALL_FILTER_VALUE,
                    region: ALL_FILTER_VALUE,
                    card: ALL_FILTER_VALUE,
                    availability: ALL_FILTER_VALUE,
                    maxPrice: '',
                  })
                }
                className="h-8 px-2 text-xs font-medium text-sky-blue hover:text-sky-blue-bright"
              >
                Clear filters
              </button>
            )}
          </div>
          {filteredLocations.length === 0 ? (
            <div className="p-5 text-sm text-gray-500">
              No placement locations match these filters.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Provider / location</TableHead>
                    <TableHead>Shape</TableHead>
                    <TableHead>Eligibility</TableHead>
                    <TableHead>Placement price</TableHead>
                    <TableHead>Machine price</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredLocations.map((location, index) => {
                    const details = locationTooltip(location, state.costUnit);
                    return (
                      <TableRow
                        key={`${location.cloud}-${location.region}-${location.zone}-${location.instanceType}-${formatAccelerators(location.accelerators)}-${location.useSpot}-${index}`}
                        title={details}
                        aria-label={details}
                        tabIndex={0}
                        className="cursor-help"
                      >
                        <TableCell>
                          <div className="font-medium">{location.cloud}</div>
                          <div className="text-xs text-gray-500">
                            {location.region || '-'}
                            {location.zone ? ` / ${location.zone}` : ''}
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="font-medium">
                            {location.instanceType || '-'}
                          </div>
                          <div className="text-xs text-gray-500">
                            {formatAccelerators(location.accelerators)}
                          </div>
                        </TableCell>
                        <TableCell>
                          <LocationStatus location={location} />
                        </TableCell>
                        <TableCell className="font-medium">
                          {formatPlacementHourlyPrice(
                            location.normalizedHourlyCost,
                            state.costUnit
                          )}
                        </TableCell>
                        <TableCell>
                          {Number.isFinite(location.cachedHourlyCost)
                            ? `$${location.cachedHourlyCost.toFixed(4)}/machine-hr`
                            : 'Price unavailable'}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </>
      )}
      {state.nextOffset !== null && state.nextOffset !== undefined ? (
        <div className="border-t p-3 text-center">
          <button
            type="button"
            onClick={onLoadMore}
            disabled={requestPending}
            className="text-sm font-medium text-sky-blue hover:text-sky-blue-bright disabled:text-gray-400"
          >
            {loadingMore ? 'Loading…' : 'Load more locations'}
          </button>
          {Number.isFinite(state.totalLocations) && (
            <div className="mt-1 text-xs text-gray-500">
              Showing {state.locations.length} of {state.totalLocations}
            </div>
          )}
        </div>
      ) : state.truncated ? (
        <div className="border-t px-4 py-2 text-xs text-amber-700">
          Additional locations were omitted by the response limit.
        </div>
      ) : null}
    </Card>
  );
}

function CapacityHintsCard({ state }) {
  return (
    <Card>
      <div className="border-b px-4 py-3">
        <h3 className="font-semibold">Launch suppression</h3>
        <p className="mt-1 text-sm text-gray-500">
          These short-lived hints suppress only the exact instance demand
          shown—not every instance type in the zone or region.
        </p>
      </div>
      {!state.available ? (
        <SectionUnavailable label="Capacity hints" />
      ) : state.hints.length === 0 ? (
        <div className="p-5 text-sm text-gray-500">
          No active capacity or quota hints for this service.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Scope</TableHead>
                <TableHead>Provider</TableHead>
                <TableHead>Region / zone</TableHead>
                <TableHead>Instance type</TableHead>
                <TableHead>Nodes</TableHead>
                <TableHead>Expires</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {state.hints.map((hint, index) => (
                <TableRow
                  key={`${hint.kind}-${hint.cloud}-${hint.region}-${hint.zone}-${hint.instanceType}-${index}`}
                >
                  <TableCell>
                    <StatusPill tone="warning">
                      {hint.kind === 'quota'
                        ? 'Regional quota'
                        : 'Zonal capacity'}
                    </StatusPill>
                  </TableCell>
                  <TableCell className="uppercase">
                    {hint.cloud || '-'}
                  </TableCell>
                  <TableCell>
                    {hint.region || '-'}
                    {hint.zone ? ` / ${hint.zone}` : ''}
                  </TableCell>
                  <TableCell className="font-medium">
                    {hint.instanceType || '-'}
                    {hint.accelerators ? ` (${hint.accelerators})` : ''}
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

function historyPriceSource(priceSource) {
  if (priceSource === 'catalog_at_decision') {
    return 'Catalog estimate at decision';
  }
  return priceSource
    ? priceSource.replaceAll('_', ' ')
    : 'Price source unavailable';
}

function HistoryCard({ history, loadingMore, requestPending, onLoadMore }) {
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
            <h3 className="font-semibold">Actual placement attempts (24h)</h3>
            <p className="mt-1 text-sm text-gray-500">
              Only provider launch attempts appear here. Displayed prices are
              catalog machine-price estimates captured at decision time, not
              provider billing.
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
          No provider launch attempts have been recorded in this window.
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
                        <div>
                          {event.hourlyPrice != null
                            ? `$${event.hourlyPrice.toFixed(4)}/machine-hr`
                            : '-'}
                        </div>
                        {event.hourlyPrice != null && (
                          <div className="text-xs text-gray-500">
                            {historyPriceSource(event.priceSource)}
                          </div>
                        )}
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
                    <div>
                      Price source: {historyPriceSource(event.priceSource)}
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
            disabled={requestPending}
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
  const [pendingAction, setPendingAction] = useState('refresh');
  const [error, setError] = useState(null);
  const requestVersionRef = useRef(0);
  const loading = pendingAction === 'refresh';
  const loadingMoreHistory = pendingAction === 'append-history';
  const loadingMoreLocations = pendingAction === 'append-locations';
  const requestPending = pendingAction !== null;

  const fetchData = useCallback(
    async ({
      cursor = null,
      locationOffset = 0,
      append = null,
      expectedLocationPage = null,
    } = {}) => {
      if (!serviceName) return;
      const requestVersion = requestVersionRef.current + 1;
      requestVersionRef.current = requestVersion;
      const isCurrentRequest = () =>
        requestVersionRef.current === requestVersion;
      setPendingAction(append || 'refresh');
      setError(null);
      try {
        const placementRequest = {
          serviceName,
          cursor,
          locationOffset,
        };
        if (expectedLocationPage?.orderGeneration) {
          placementRequest.locationOrderGeneration =
            expectedLocationPage.orderGeneration;
        }
        const next = await getServicePlacement(placementRequest);
        if (!isCurrentRequest()) return;
        if (
          append === 'append-locations' &&
          !next.placerState.available &&
          next.placerState.reason !== 'catalog_order_changed'
        ) {
          setError('Failed to load more placement locations.');
          return;
        }
        if (
          append === 'append-locations' &&
          (next.placerState.reason === 'catalog_order_changed' ||
            !locationPageContractMatches(
              expectedLocationPage,
              next.placerState,
              locationOffset
            ))
        ) {
          // A controller/API rolling upgrade or catalog-policy change can
          // change page ordering between requests. Never append across that
          // boundary: atomically replace the view with a fresh first page.
          const reset = await getServicePlacement({ serviceName });
          if (!isCurrentRequest()) return;
          if (!reset.placerState.available) {
            setError('Failed to reload the changed placement catalog.');
            return;
          }
          setData(reset);
          return;
        }
        if (append === 'append-history' && !next.history.available) {
          setError('Failed to load more placement history.');
          return;
        }
        setData((current) => {
          if (!append || !current) return next;
          if (append === 'append-history') {
            return {
              ...current,
              history: {
                ...next.history,
                events: [...current.history.events, ...next.history.events],
              },
            };
          }
          if (append === 'append-locations') {
            return {
              ...current,
              placerState: {
                ...next.placerState,
                locations: [
                  ...current.placerState.locations,
                  ...next.placerState.locations,
                ],
              },
            };
          }
          return next;
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
          setPendingAction(null);
        }
      }
    },
    [serviceName]
  );

  useEffect(() => {
    setData(null);
    setError(null);
    setPendingAction('refresh');
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
          disabled={requestPending}
          className="inline-flex items-center text-sm font-medium text-sky-blue hover:text-sky-blue-bright disabled:text-gray-400"
        >
          <RotateCwIcon className="mr-1.5 h-4 w-4" />
          Refresh
        </button>
      </div>
      {error && <div className="text-sm text-red-700">{error}</div>}
      <HistoryCard
        history={data.history}
        loadingMore={loadingMoreHistory}
        requestPending={requestPending}
        onLoadMore={() =>
          fetchData({
            cursor: data.history.nextCursor,
            append: 'append-history',
          })
        }
      />
      <PlacerStateCard
        state={data.placerState}
        loadingMore={loadingMoreLocations}
        requestPending={requestPending}
        onLoadMore={() =>
          fetchData({
            locationOffset: data.placerState.nextOffset,
            append: 'append-locations',
            expectedLocationPage: {
              paginationVersion: data.placerState.paginationVersion,
              orderSemantics: data.placerState.orderSemantics,
              costUnit: data.placerState.costUnit,
              orderGeneration: data.placerState.orderGeneration,
              totalLocations: data.placerState.totalLocations,
            },
          })
        }
      />
      <CapacityHintsCard state={data.capacityHints} />
    </div>
  );
}
