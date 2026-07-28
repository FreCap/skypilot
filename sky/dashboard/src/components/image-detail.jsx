import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import { ArrowLeft, Copy, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { StatusBadge } from '@/components/elements/StatusBadge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  IMAGE_REMEDIATIONS,
  PrepareImageDialog,
  RetryImageDialog,
} from '@/components/image-action-dialogs';
import {
  getImageArtifactCollection,
  getImageArtifactDetail,
  getImageCapabilities,
} from '@/data/connectors/images';
import {
  advanceImageCursorHistory,
  currentImageCursorEntry,
  firstImageCursorHistory,
  retreatImageCursorHistory,
} from '@/data/image-cursor-history';

const ARTIFACT_COLLECTIONS = [
  'releases',
  'sources',
  'locations',
  'publications',
  'demands',
];
const IMAGE_DETAIL_POLL_MS = 5000;

function initialCollectionCursorStacks() {
  return Object.fromEntries(
    ARTIFACT_COLLECTIONS.map((collection) => [
      collection,
      firstImageCursorHistory(),
    ])
  );
}

function timestamp(value) {
  return value ? new Date(value * 1000).toLocaleString() : 'Never';
}

function bytes(value) {
  if (!Number.isFinite(value)) return 'Unknown';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let current = value;
  let index = 0;
  while (current >= 1024 && index < units.length - 1) {
    current /= 1024;
    index += 1;
  }
  return `${current.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function CopyCode({ value }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex min-w-0 items-center gap-2">
      <code className="min-w-0 break-all text-xs text-gray-700">{value}</code>
      <button
        type="button"
        aria-label="Copy value"
        className="shrink-0 rounded p-1 text-gray-500 hover:bg-gray-100"
        onClick={async () => {
          await navigator.clipboard.writeText(value);
          setCopied(true);
          setTimeout(() => setCopied(false), 1200);
        }}
      >
        <Copy className="h-4 w-4" />
      </button>
      {copied && <span className="text-xs text-green-700">Copied</span>}
    </div>
  );
}

function Definition({ label, children }) {
  return (
    <div>
      <dt className="text-xs font-medium uppercase tracking-wide text-gray-500">
        {label}
      </dt>
      <dd className="mt-1 text-sm text-gray-900">{children}</dd>
    </div>
  );
}

function CollectionPager({
  collection,
  page,
  nextCursor,
  loading,
  error,
  notice,
  canPrevious,
  onFirst,
  onPrevious,
  onNext,
}) {
  const label = collection.replace('_', ' ');
  if (page === 1 && !nextCursor && !loading && !error && !notice) return null;
  return (
    <div className="mt-4 border-t border-gray-200 pt-3">
      {notice && (
        <div className="mb-3 rounded-md border border-blue-200 bg-blue-50 p-2 text-sm text-blue-800">
          {notice}
        </div>
      )}
      {error && (
        <div
          role="alert"
          className="mb-3 rounded-md border border-red-200 bg-red-50 p-2 text-sm text-red-800"
        >
          {error}
        </div>
      )}
      <div className="flex items-center justify-between">
        <span className="text-sm text-gray-500">Page {page}</span>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            aria-label={`First ${label} page`}
            disabled={page === 1 || loading}
            onClick={onFirst}
          >
            First
          </Button>
          <Button
            variant="outline"
            size="sm"
            aria-label={`Previous ${label} page`}
            disabled={!canPrevious || loading}
            onClick={onPrevious}
          >
            Previous
          </Button>
          <Button
            variant="outline"
            size="sm"
            aria-label={`Next ${label} page`}
            disabled={!nextCursor || loading}
            onClick={onNext}
          >
            Next
          </Button>
        </div>
      </div>
    </div>
  );
}

export function ImageDetail() {
  const router = useRouter();
  const imageId =
    typeof router.query.image === 'string' ? router.query.image : null;
  const requestedWorkspace =
    typeof router.query.workspace === 'string' ? router.query.workspace : null;
  const requestScope = JSON.stringify([requestedWorkspace, imageId]);
  const [requestState, setRequestState] = useState(() => ({
    scope: requestScope,
    workspace: requestedWorkspace,
    detail: null,
    capabilities: null,
    loading: true,
    error: null,
    oldServer: false,
  }));
  const [activeTab, setActiveTab] = useState('overview');
  const [prepareOpen, setPrepareOpen] = useState(false);
  const [retry, setRetry] = useState(null);
  const [collectionCursorStacks, setCollectionCursorStacks] = useState(
    initialCollectionCursorStacks
  );
  const [collectionLoading, setCollectionLoading] = useState({});
  const [collectionErrors, setCollectionErrors] = useState({});
  const [collectionNotices, setCollectionNotices] = useState({});
  const generation = useRef(0);
  const requestOwner = useRef(null);
  const lastRequestStart = useRef(null);
  const collectionControllers = useRef({});
  const loadedScope = useRef(null);
  const requestScopeRef = useRef(requestScope);
  requestScopeRef.current = requestScope;
  const requestStateIsCurrent = requestState.scope === requestScope;
  const workspace = requestStateIsCurrent
    ? requestState.workspace
    : requestedWorkspace;
  const detail = requestStateIsCurrent ? requestState.detail : null;
  const capabilities = requestStateIsCurrent ? requestState.capabilities : null;
  const loading = requestStateIsCurrent ? requestState.loading : true;
  const error = requestStateIsCurrent ? requestState.error : null;
  const oldServer = requestStateIsCurrent ? requestState.oldServer : false;

  const startLoad = useCallback(
    (source) => {
      if (!imageId) return Promise.resolve();
      const scope = requestScope;
      const scopeChanged = loadedScope.current !== scope;
      loadedScope.current = scope;
      const previousOwner = requestOwner.current;
      if (previousOwner !== null) {
        previousOwner.revoked = true;
        previousOwner.controller.abort();
      }
      Object.values(collectionControllers.current).forEach((controller) =>
        controller.abort()
      );
      collectionControllers.current = {};
      setCollectionLoading({});
      if (scopeChanged) {
        setActiveTab('overview');
        setPrepareOpen(false);
        setRetry(null);
        setCollectionCursorStacks(initialCollectionCursorStacks());
        setCollectionErrors({});
        setCollectionNotices({});
      }
      const controller = new AbortController();
      const currentGeneration = ++generation.current;
      const owner = {
        scope,
        source,
        startedAt: performance.now(),
        controller,
        promise: null,
        revoked: false,
      };
      requestOwner.current = owner;
      lastRequestStart.current = {
        scope,
        startedAt: owner.startedAt,
      };
      const isCurrentRequest = () =>
        !owner.revoked &&
        requestOwner.current === owner &&
        generation.current === currentGeneration &&
        requestScopeRef.current === scope;
      setRequestState((current) => ({
        scope,
        workspace:
          current.scope === scope ? current.workspace : requestedWorkspace,
        detail: current.scope === scope ? current.detail : null,
        capabilities: current.scope === scope ? current.capabilities : null,
        loading: true,
        error: null,
        oldServer: false,
      }));
      let capabilitiesLoaded = false;
      owner.promise = (async () => {
        try {
          const nextCapabilities = await getImageCapabilities(
            requestedWorkspace,
            controller.signal
          );
          capabilitiesLoaded = true;
          if (!isCurrentRequest()) return;
          const nextDetail = await getImageArtifactDetail(
            imageId,
            nextCapabilities.workspace,
            controller.signal
          );
          if (!isCurrentRequest()) return;
          setRequestState({
            scope,
            workspace: nextCapabilities.workspace,
            detail: nextDetail,
            capabilities: nextCapabilities,
            loading: false,
            error: null,
            oldServer: false,
          });
          setCollectionCursorStacks(initialCollectionCursorStacks());
          setCollectionErrors({});
          setCollectionNotices({});
        } catch (requestError) {
          if (requestError.name !== 'AbortError' && isCurrentRequest()) {
            const isOldServer =
              !capabilitiesLoaded &&
              (requestError.status === 404 || requestError.status === 426);
            setRequestState((current) =>
              current.scope === scope
                ? {
                    ...current,
                    loading: false,
                    error: requestError.code || requestError.message,
                    oldServer: isOldServer,
                  }
                : current
            );
          }
        } finally {
          if (
            requestOwner.current === owner &&
            generation.current === currentGeneration &&
            requestScopeRef.current === scope
          ) {
            setRequestState((current) =>
              current.scope === scope ? { ...current, loading: false } : current
            );
            requestOwner.current = null;
          }
        }
      })();
      return owner.promise;
    },
    [imageId, requestedWorkspace, requestScope]
  );

  const load = useCallback(() => startLoad('manual'), [startLoad]);

  useEffect(() => {
    void startLoad('initial');
    return () => {
      generation.current += 1;
      const owner = requestOwner.current;
      if (owner !== null) {
        owner.revoked = true;
        owner.controller.abort();
        requestOwner.current = null;
      }
      Object.values(collectionControllers.current).forEach((controller) =>
        controller.abort()
      );
      collectionControllers.current = {};
    };
  }, [startLoad]);

  const pageArtifactCollection = useCallback(
    async (collection, direction) => {
      if (!requestStateIsCurrent || !imageId || !workspace || !detail) return;
      const scope = requestScope;
      const currentStack =
        collectionCursorStacks[collection] || firstImageCursorHistory();
      let nextStack;
      if (direction === 'next') {
        const nextCursor = detail.next_cursors?.[collection];
        if (!nextCursor) return;
        nextStack = advanceImageCursorHistory(currentStack, nextCursor);
      } else if (direction === 'first') {
        nextStack = firstImageCursorHistory();
      } else {
        if (currentStack.length === 1) return;
        nextStack = retreatImageCursorHistory(currentStack);
      }
      const requestedCursor = currentImageCursorEntry(nextStack).cursor;

      collectionControllers.current[collection]?.abort();
      const controller = new AbortController();
      collectionControllers.current[collection] = controller;
      const currentGeneration = generation.current;
      setCollectionLoading((current) => ({
        ...current,
        [collection]: true,
      }));
      setCollectionErrors((current) => {
        const next = { ...current };
        delete next[collection];
        return next;
      });
      setCollectionNotices((current) => {
        const next = { ...current };
        delete next[collection];
        return next;
      });

      try {
        let page;
        let recoveredStaleCursor = false;
        try {
          page = await getImageArtifactCollection(
            imageId,
            collection,
            {
              workspace,
              limit: 100,
              cursor: requestedCursor,
            },
            controller.signal
          );
        } catch (requestError) {
          if (
            requestError.code !== 'STALE_IMAGE_CURSOR' ||
            requestedCursor === null
          ) {
            throw requestError;
          }
          nextStack = firstImageCursorHistory();
          page = await getImageArtifactCollection(
            imageId,
            collection,
            { workspace, limit: 100, cursor: null },
            controller.signal
          );
          recoveredStaleCursor = true;
        }

        if (
          generation.current !== currentGeneration ||
          requestScopeRef.current !== scope ||
          collectionControllers.current[collection] !== controller
        )
          return;
        if (recoveredStaleCursor) {
          setCollectionNotices((current) => ({
            ...current,
            [collection]: `The ${collection} collection changed while paging. Reloaded the first page.`,
          }));
        }
        setRequestState((current) => {
          if (
            current.scope !== scope ||
            !current.detail ||
            current.detail.artifact.id !== detail.artifact.id
          )
            return current;
          const nextCursors = {
            ...(current.detail.next_cursors || {}),
            [collection]: page.next_cursor || null,
          };
          return {
            ...current,
            detail: {
              ...current.detail,
              [collection]: page.items,
              next_cursors: nextCursors,
              truncated: Object.values(nextCursors).some(Boolean),
            },
          };
        });
        setCollectionCursorStacks((current) => ({
          ...current,
          [collection]: nextStack,
        }));
      } catch (requestError) {
        if (
          requestError.name !== 'AbortError' &&
          generation.current === currentGeneration &&
          requestScopeRef.current === scope &&
          collectionControllers.current[collection] === controller
        ) {
          setCollectionErrors((current) => ({
            ...current,
            [collection]: requestError.code || requestError.message,
          }));
        }
      } finally {
        if (collectionControllers.current[collection] === controller) {
          delete collectionControllers.current[collection];
          if (requestScopeRef.current === scope) {
            setCollectionLoading((current) => ({
              ...current,
              [collection]: false,
            }));
          }
        }
      }
    },
    [
      collectionCursorStacks,
      detail,
      imageId,
      requestScope,
      requestStateIsCurrent,
      workspace,
    ]
  );

  const hasNonterminal = useMemo(
    () =>
      Boolean(
        detail?.publications.some((item) =>
          ['PENDING', 'INSPECTING'].includes(item.state)
        ) ||
          detail?.locations.some((item) =>
            ['PENDING', 'COPYING', 'VERIFYING', 'MISSING', 'EVICTING'].includes(
              item.state
            )
          )
      ),
    [detail]
  );
  const viewingFirstCollectionPages = useMemo(
    () =>
      ARTIFACT_COLLECTIONS.every(
        (collection) =>
          currentImageCursorEntry(collectionCursorStacks[collection]).page === 1
      ),
    [collectionCursorStacks]
  );

  useEffect(() => {
    if (!hasNonterminal || !viewingFirstCollectionPages) return undefined;
    let active = true;
    let timer = null;

    const schedule = () => {
      if (!active) return;
      const lastStart = lastRequestStart.current;
      const elapsed =
        lastStart?.scope === requestScope
          ? performance.now() - lastStart.startedAt
          : IMAGE_DETAIL_POLL_MS;
      timer = setTimeout(run, Math.max(0, IMAGE_DETAIL_POLL_MS - elapsed));
    };

    const run = async () => {
      if (!active) return;
      const lastStart = lastRequestStart.current;
      if (lastStart?.scope === requestScope) {
        const remaining =
          IMAGE_DETAIL_POLL_MS - (performance.now() - lastStart.startedAt);
        if (remaining > 0) {
          timer = setTimeout(run, remaining);
          return;
        }
      }
      const owner = requestOwner.current;
      const request =
        owner?.scope === requestScope ? owner.promise : startLoad('poll');
      await request;
      schedule();
    };

    timer = setTimeout(run, IMAGE_DETAIL_POLL_MS);
    return () => {
      active = false;
      if (timer !== null) clearTimeout(timer);
      const owner = requestOwner.current;
      if (owner?.scope === requestScope && owner.source === 'poll') {
        owner.revoked = true;
        owner.controller.abort();
      }
    };
  }, [hasNonterminal, requestScope, startLoad, viewingFirstCollectionPages]);

  if (loading && !detail) {
    return (
      <div className="py-20 text-center text-gray-500">Loading artifact…</div>
    );
  }
  if (oldServer && !detail) {
    return (
      <div className="space-y-4">
        <Link
          href={{
            pathname: '/images',
            query: requestedWorkspace ? { workspace: requestedWorkspace } : {},
          }}
          className="inline-flex items-center text-sm text-blue-600 hover:underline"
        >
          <ArrowLeft className="mr-2 h-4 w-4" /> Back to Images
        </Link>
        <div className="rounded-lg border border-blue-200 bg-blue-50 p-5 text-blue-900">
          <h1 className="font-semibold">
            Managed Images requires API version 62
          </h1>
          <p className="mt-1 text-sm">
            The connected API server does not expose managed image artifact
            details yet.
          </p>
        </div>
      </div>
    );
  }
  if (error && !detail) {
    return (
      <div className="space-y-4">
        <Link
          href={{
            pathname: '/images',
            query: requestedWorkspace ? { workspace: requestedWorkspace } : {},
          }}
          className="inline-flex items-center text-sm text-blue-600 hover:underline"
        >
          <ArrowLeft className="mr-2 h-4 w-4" /> Back to Images
        </Link>
        <div
          role="alert"
          className="rounded-md border border-red-200 bg-red-50 p-4 text-red-800"
        >
          {error}
        </div>
      </div>
    );
  }
  if (!detail || !capabilities) return null;

  const artifact = detail.artifact;
  const stale = loading || Boolean(error) || oldServer;
  return (
    <div className="mx-auto max-w-[1500px] space-y-6">
      {(error || oldServer) && (
        <div
          role="alert"
          className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900"
        >
          Latest refresh failed ({error || 'UPGRADE'}). Cached artifact data is
          read-only until refresh succeeds.
        </div>
      )}
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <Link
            href={{
              pathname: '/images',
              query: workspace ? { workspace } : {},
            }}
            className="inline-flex items-center text-sm text-blue-600 hover:underline"
          >
            <ArrowLeft className="mr-2 h-4 w-4" /> Back to Images
          </Link>
          <h1 className="mt-3 text-2xl font-semibold text-gray-900">
            Image artifact
          </h1>
          <div className="mt-2 max-w-4xl">
            <CopyCode value={artifact.runtime_digest} />
          </div>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={load} disabled={loading}>
            <RefreshCw
              className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`}
            />
            Refresh
          </Button>
          {capabilities.publish && (
            <Button
              onClick={() => setPrepareOpen(true)}
              disabled={stale}
              title={
                stale
                  ? 'Refresh artifact data before changing state'
                  : undefined
              }
            >
              Prepare target
            </Button>
          )}
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-4">
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <div className="text-xs text-gray-500">Releases on page</div>
          <div className="mt-1 text-2xl font-semibold">
            {detail.releases.length}
          </div>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <div className="text-xs text-gray-500">Locations on page</div>
          <div className="mt-1 text-2xl font-semibold">
            {detail.locations.length}
          </div>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <div className="text-xs text-gray-500">Active demands on page</div>
          <div className="mt-1 text-2xl font-semibold">
            {
              detail.demands.filter((item) =>
                ['WARMING', 'READY'].includes(item.state)
              ).length
            }
          </div>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <div className="text-xs text-gray-500">Declared size</div>
          <div className="mt-1 text-2xl font-semibold">
            {bytes(artifact.declared_size_bytes)}
          </div>
        </div>
      </div>

      <Tabs>
        <TabsList aria-label="Artifact detail sections">
          {['overview', 'locations', 'publications', 'demands'].map((tab) => (
            <TabsTrigger
              key={tab}
              active={activeTab === tab}
              onClick={() => setActiveTab(tab)}
            >
              {tab[0].toUpperCase() + tab.slice(1)}
            </TabsTrigger>
          ))}
        </TabsList>

        <TabsContent active={activeTab === 'overview'}>
          <div className="grid gap-6 lg:grid-cols-2">
            <section className="rounded-lg border border-gray-200 bg-white p-5">
              <h2 className="font-semibold text-gray-900">Artifact identity</h2>
              <dl className="mt-4 grid gap-5 sm:grid-cols-2">
                <Definition label="Artifact ID">
                  <CopyCode value={artifact.id} />
                </Definition>
                <Definition label="Workspace">{artifact.workspace}</Definition>
                <Definition label="Platform">{artifact.platform}</Definition>
                <Definition label="Producer">
                  {artifact.producer_kind}
                </Definition>
                <Definition label="Config digest">
                  <CopyCode value={artifact.config_digest} />
                </Definition>
                <Definition label="Manifest size">
                  {bytes(artifact.manifest_size_bytes)}
                </Definition>
                <Definition label="Created">
                  {timestamp(artifact.created_at)}
                </Definition>
                <Definition label="Updated">
                  {timestamp(artifact.updated_at)}
                </Definition>
              </dl>
            </section>
            <section className="rounded-lg border border-gray-200 bg-white p-5">
              <h2 className="font-semibold text-gray-900">
                Immutable releases
              </h2>
              <div className="mt-4 space-y-3">
                {detail.releases.map((release) => (
                  <div
                    key={release.publication_id}
                    className="rounded-md border border-gray-200 p-3"
                  >
                    <div className="font-medium">{release.release}</div>
                    <div className="mt-1 text-xs text-gray-500">
                      Published {timestamp(release.published_at)}
                    </div>
                  </div>
                ))}
                {detail.releases.length === 0 && (
                  <p className="text-sm text-gray-500">No READY release.</p>
                )}
              </div>
              <CollectionPager
                collection="releases"
                page={
                  currentImageCursorEntry(collectionCursorStacks.releases).page
                }
                nextCursor={detail.next_cursors?.releases}
                loading={Boolean(collectionLoading.releases)}
                error={collectionErrors.releases}
                notice={collectionNotices.releases}
                canPrevious={collectionCursorStacks.releases.length > 1}
                onFirst={() => pageArtifactCollection('releases', 'first')}
                onPrevious={() =>
                  pageArtifactCollection('releases', 'previous')
                }
                onNext={() => pageArtifactCollection('releases', 'next')}
              />
            </section>
            <section className="rounded-lg border border-gray-200 bg-white p-5 lg:col-span-2">
              <h2 className="font-semibold text-gray-900">Retained sources</h2>
              <div className="mt-4 grid gap-3 lg:grid-cols-2">
                {detail.sources.map((source) => (
                  <div
                    key={source.id}
                    className="rounded-md border border-gray-200 p-3"
                  >
                    <CopyCode value={source.source_ref} />
                    <div className="mt-2 text-xs text-gray-500">
                      {source.requested_platform}
                      {source.source_auth_binding_id && (
                        <> · binding {source.source_auth_binding_id}</>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              <CollectionPager
                collection="sources"
                page={
                  currentImageCursorEntry(collectionCursorStacks.sources).page
                }
                nextCursor={detail.next_cursors?.sources}
                loading={Boolean(collectionLoading.sources)}
                error={collectionErrors.sources}
                notice={collectionNotices.sources}
                canPrevious={collectionCursorStacks.sources.length > 1}
                onFirst={() => pageArtifactCollection('sources', 'first')}
                onPrevious={() => pageArtifactCollection('sources', 'previous')}
                onNext={() => pageArtifactCollection('sources', 'next')}
              />
            </section>
          </div>
        </TabsContent>

        <TabsContent active={activeTab === 'locations'}>
          <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
            <div className="border-b border-gray-200 px-5 py-4">
              <h2 className="font-semibold text-gray-900">
                Registry materialization
              </h2>
              <p className="text-sm text-gray-500">
                This is registry state only. Node pulls and Serve replica health
                are separate.
              </p>
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Distribution / target</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Reference</TableHead>
                  <TableHead>Verified</TableHead>
                  <TableHead>Attempts</TableHead>
                  <TableHead>Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {detail.locations.map((location) => (
                  <TableRow key={location.id}>
                    <TableCell>
                      <div className="font-medium">{location.distribution}</div>
                      <div className="text-xs text-gray-500">
                        {location.target_id}
                        {location.canonical ? ' · canonical' : ''}
                      </div>
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={location.state} />
                      <div className="mt-1 text-xs text-red-700">
                        {location.error_code}
                      </div>
                      {IMAGE_REMEDIATIONS?.[location.error_code] && (
                        <div className="mt-1 max-w-xs text-xs text-gray-600">
                          {IMAGE_REMEDIATIONS[location.error_code]}
                        </div>
                      )}
                    </TableCell>
                    <TableCell className="max-w-md">
                      <CopyCode value={location.target_ref} />
                    </TableCell>
                    <TableCell>
                      {timestamp(location.last_verified_at)}
                    </TableCell>
                    <TableCell>{location.attempt_count}</TableCell>
                    <TableCell>
                      {capabilities.publish &&
                        ['FAILED', 'MISSING', 'EVICTED'].includes(
                          location.state
                        ) && (
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={stale}
                            onClick={() =>
                              setRetry({ kind: 'location', id: location.id })
                            }
                          >
                            Retry
                          </Button>
                        )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <div className="px-5 pb-4">
              <CollectionPager
                collection="locations"
                page={
                  currentImageCursorEntry(collectionCursorStacks.locations).page
                }
                nextCursor={detail.next_cursors?.locations}
                loading={Boolean(collectionLoading.locations)}
                error={collectionErrors.locations}
                notice={collectionNotices.locations}
                canPrevious={collectionCursorStacks.locations.length > 1}
                onFirst={() => pageArtifactCollection('locations', 'first')}
                onPrevious={() =>
                  pageArtifactCollection('locations', 'previous')
                }
                onNext={() => pageArtifactCollection('locations', 'next')}
              />
            </div>
          </section>
        </TabsContent>

        <TabsContent active={activeTab === 'publications'}>
          <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Release</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead>Attempts</TableHead>
                  <TableHead>Updated</TableHead>
                  <TableHead>Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {detail.publications.map((publication) => (
                  <TableRow key={publication.id}>
                    <TableCell className="font-medium">
                      {publication.requested_release}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={publication.state} />
                      <div className="mt-1 text-xs text-red-700">
                        {publication.error_code}
                      </div>
                    </TableCell>
                    <TableCell className="max-w-md">
                      <CopyCode value={publication.source_ref} />
                    </TableCell>
                    <TableCell>{publication.attempt_count}</TableCell>
                    <TableCell>{timestamp(publication.updated_at)}</TableCell>
                    <TableCell>
                      {capabilities.publish &&
                        publication.state === 'FAILED' && (
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={stale}
                            onClick={() =>
                              setRetry({
                                kind: 'publication',
                                id: publication.id,
                              })
                            }
                          >
                            Retry
                          </Button>
                        )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <div className="px-5 pb-4">
              <CollectionPager
                collection="publications"
                page={
                  currentImageCursorEntry(collectionCursorStacks.publications)
                    .page
                }
                nextCursor={detail.next_cursors?.publications}
                loading={Boolean(collectionLoading.publications)}
                error={collectionErrors.publications}
                notice={collectionNotices.publications}
                canPrevious={collectionCursorStacks.publications.length > 1}
                onFirst={() => pageArtifactCollection('publications', 'first')}
                onPrevious={() =>
                  pageArtifactCollection('publications', 'previous')
                }
                onNext={() => pageArtifactCollection('publications', 'next')}
              />
            </div>
          </section>
        </TabsContent>

        <TabsContent active={activeTab === 'demands'}>
          <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Consumer</TableHead>
                  <TableHead>Generation</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Target</TableHead>
                  <TableHead>Attached</TableHead>
                  <TableHead>Updated</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {detail.demands.map((demand) => (
                  <TableRow key={demand.id}>
                    <TableCell>
                      <div className="font-medium">{demand.consumer_kind}</div>
                      <div className="text-xs text-gray-500">
                        {demand.consumer_owner}
                      </div>
                    </TableCell>
                    <TableCell>{demand.consumer_generation}</TableCell>
                    <TableCell>
                      <StatusBadge status={demand.state} />
                      <div className="mt-1 text-xs text-red-700">
                        {demand.error_code}
                      </div>
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {demand.target_key}
                    </TableCell>
                    <TableCell>
                      {demand.consumer_attached ? 'Yes' : 'No'}
                    </TableCell>
                    <TableCell>{timestamp(demand.updated_at)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <div className="px-5 pb-4">
              <CollectionPager
                collection="demands"
                page={
                  currentImageCursorEntry(collectionCursorStacks.demands).page
                }
                nextCursor={detail.next_cursors?.demands}
                loading={Boolean(collectionLoading.demands)}
                error={collectionErrors.demands}
                notice={collectionNotices.demands}
                canPrevious={collectionCursorStacks.demands.length > 1}
                onFirst={() => pageArtifactCollection('demands', 'first')}
                onPrevious={() => pageArtifactCollection('demands', 'previous')}
                onNext={() => pageArtifactCollection('demands', 'next')}
              />
            </div>
          </section>
        </TabsContent>
      </Tabs>

      <PrepareImageDialog
        open={prepareOpen}
        onOpenChange={(open) => {
          setPrepareOpen(open);
          if (!open) load();
        }}
        workspace={workspace}
        artifact={artifact}
        capabilities={capabilities}
        onChanged={load}
      />
      <RetryImageDialog
        open={Boolean(retry)}
        onOpenChange={(open) => {
          if (!open) {
            setRetry(null);
            load();
          }
        }}
        workspace={workspace}
        kind={retry?.kind}
        recordId={retry?.id}
        onChanged={load}
      />
    </div>
  );
}
