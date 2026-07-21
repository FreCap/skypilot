import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import { Box, RefreshCw, Search, Upload } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
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
  PublishImageDialog,
  RetryImageDialog,
} from '@/components/image-action-dialogs';
import { ImageReadiness } from '@/components/image-readiness';
import {
  getImageCapabilities,
  getImageCatalog,
  getImagePublications,
  getImageReadiness,
} from '@/data/connectors/images';
import { getWorkspaces } from '@/data/connectors/workspaces';

const EMPTY_FILTERS = {
  release: '',
  digest: '',
  source_ref: '',
  distribution: '',
  target: '',
  state: '',
};

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

function locationSummary(states) {
  const entries = Object.entries(states || {});
  if (!entries.length) return <span className="text-gray-400">None</span>;
  return (
    <div className="flex flex-wrap gap-1">
      {entries.map(([state, count]) => (
        <span key={state} className="inline-flex items-center gap-1">
          <StatusBadge status={state} />
          <span className="text-xs text-gray-500">×{count}</span>
        </span>
      ))}
    </div>
  );
}

function parseWorkspaceNames(value) {
  if (!value || typeof value !== 'object') return [];
  if (Array.isArray(value)) {
    return value.map((item) => item?.name || item).filter(Boolean);
  }
  const candidate = value.workspaces || value;
  if (Array.isArray(candidate)) {
    return candidate.map((item) => item?.name || item).filter(Boolean);
  }
  return Object.keys(candidate);
}

export function Images() {
  const router = useRouter();
  const requestedWorkspace =
    typeof router.query.workspace === 'string' ? router.query.workspace : '';
  const initialTab = router.query.tab === 'readiness' ? 'readiness' : 'catalog';
  const [activeTab, setActiveTab] = useState(initialTab);
  const [workspaceInput, setWorkspaceInput] = useState(requestedWorkspace);
  const [workspaceNames, setWorkspaceNames] = useState([]);
  const [capabilities, setCapabilities] = useState(null);
  const [capabilityError, setCapabilityError] = useState(null);
  const [oldServer, setOldServer] = useState(false);
  const [filters, setFilters] = useState(EMPTY_FILTERS);
  const [draftFilters, setDraftFilters] = useState(EMPTY_FILTERS);
  const [cursorStack, setCursorStack] = useState([null]);
  const [catalog, setCatalog] = useState(null);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogError, setCatalogError] = useState(null);
  const [cursorNotice, setCursorNotice] = useState(null);
  const [failedPublications, setFailedPublications] = useState([]);
  const [readiness, setReadiness] = useState(null);
  const [readinessLoading, setReadinessLoading] = useState(false);
  const [readinessError, setReadinessError] = useState(null);
  const [publishOpen, setPublishOpen] = useState(false);
  const [retryPublication, setRetryPublication] = useState(null);
  const capabilityGeneration = useRef(0);
  const catalogGeneration = useRef(0);
  const readinessGeneration = useRef(0);
  const catalogController = useRef(null);
  const readinessController = useRef(null);
  const cursor = cursorStack[cursorStack.length - 1];

  useEffect(() => {
    getWorkspaces()
      .then((value) => setWorkspaceNames(parseWorkspaceNames(value)))
      .catch(() => setWorkspaceNames([]));
  }, []);

  const selectWorkspace = useCallback(
    (nextWorkspace) => {
      const query = { ...router.query };
      if (nextWorkspace) query.workspace = nextWorkspace;
      else delete query.workspace;
      delete query.image;
      router.replace({ pathname: '/images', query }, undefined, {
        shallow: true,
      });
      setCursorStack([null]);
      setCatalog(null);
      setReadiness(null);
    },
    [router]
  );

  useEffect(() => {
    setWorkspaceInput(requestedWorkspace);
    const generation = ++capabilityGeneration.current;
    const controller = new AbortController();
    setCapabilityError(null);
    setOldServer(false);
    getImageCapabilities(requestedWorkspace || null, controller.signal)
      .then((value) => {
        if (capabilityGeneration.current !== generation) return;
        setCapabilities(value);
        if (!requestedWorkspace) setWorkspaceInput(value.workspace);
      })
      .catch((error) => {
        if (
          error.name === 'AbortError' ||
          capabilityGeneration.current !== generation
        )
          return;
        if (error.status === 404 || error.status === 426) setOldServer(true);
        else setCapabilityError(error.code || error.message);
      });
    return () => {
      capabilityGeneration.current += 1;
      controller.abort();
    };
  }, [requestedWorkspace]);

  const loadCatalog = useCallback(async () => {
    if (!capabilities) return;
    catalogController.current?.abort();
    const generation = ++catalogGeneration.current;
    const controller = new AbortController();
    catalogController.current = controller;
    setCatalogLoading(true);
    setCatalogError(null);
    try {
      const [page, publications] = await Promise.all([
        getImageCatalog(
          {
            workspace: capabilities.workspace,
            limit: 30,
            cursor,
            ...filters,
          },
          controller.signal
        ),
        capabilities.publish
          ? getImagePublications(
              {
                workspace: capabilities.workspace,
                state: 'FAILED',
                limit: 10,
              },
              controller.signal
            )
          : Promise.resolve({ items: [] }),
      ]);
      if (catalogGeneration.current !== generation) return;
      setCatalog(page);
      setFailedPublications(publications.items);
      setCursorNotice(null);
    } catch (error) {
      if (
        error.name === 'AbortError' ||
        catalogGeneration.current !== generation
      )
        return;
      if (error.code === 'STALE_IMAGE_CURSOR' && cursor) {
        setCursorStack([null]);
        setCursorNotice(
          'The catalog changed while paging. Reloaded the first page.'
        );
      } else {
        setCatalogError(error.code || error.message);
      }
    } finally {
      if (catalogGeneration.current === generation) setCatalogLoading(false);
      if (catalogController.current === controller) {
        catalogController.current = null;
      }
    }
  }, [capabilities, cursor, filters]);

  useEffect(() => {
    loadCatalog();
    return () => {
      catalogGeneration.current += 1;
      catalogController.current?.abort();
      catalogController.current = null;
    };
  }, [loadCatalog]);

  const loadReadiness = useCallback(async () => {
    if (!capabilities?.admin) return;
    readinessController.current?.abort();
    const generation = ++readinessGeneration.current;
    const controller = new AbortController();
    readinessController.current = controller;
    setReadinessLoading(true);
    setReadinessError(null);
    try {
      const value = await getImageReadiness(
        capabilities.workspace,
        controller.signal
      );
      if (readinessGeneration.current === generation) setReadiness(value);
    } catch (error) {
      if (
        error.name !== 'AbortError' &&
        readinessGeneration.current === generation
      ) {
        setReadinessError(error.code || error.message);
      }
    } finally {
      if (readinessGeneration.current === generation)
        setReadinessLoading(false);
      if (readinessController.current === controller) {
        readinessController.current = null;
      }
    }
  }, [capabilities]);

  useEffect(() => {
    if (activeTab !== 'readiness') return undefined;
    loadReadiness();
    return () => {
      readinessGeneration.current += 1;
      readinessController.current?.abort();
      readinessController.current = null;
    };
  }, [activeTab, loadReadiness]);

  const distributionTargets = useMemo(
    () =>
      capabilities?.distributions.flatMap((distribution) =>
        distribution.targets.map((target) => ({
          distribution: distribution.name,
          target: target.name,
        }))
      ) || [],
    [capabilities]
  );

  const setTab = (tab) => {
    setActiveTab(tab);
    const query = { ...router.query };
    if (tab === 'readiness') query.tab = tab;
    else delete query.tab;
    router.replace({ pathname: '/images', query }, undefined, {
      shallow: true,
    });
  };

  if (oldServer) {
    return (
      <div className="mx-auto max-w-5xl space-y-4">
        <h1 className="text-2xl font-semibold text-gray-900">Images</h1>
        <div className="rounded-lg border border-blue-200 bg-blue-50 p-5 text-blue-900">
          <h2 className="font-semibold">
            Managed Images requires API version 62
          </h2>
          <p className="mt-1 text-sm">
            This Dashboard can continue to manage existing workloads, but the
            connected API server does not expose the managed image catalog yet.
          </p>
        </div>
      </div>
    );
  }

  if (capabilityError && !capabilities) {
    return (
      <div
        role="alert"
        className="rounded-md border border-red-200 bg-red-50 p-4 text-red-800"
      >
        {capabilityError}
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-[1600px] space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">Images</h1>
          <p className="mt-1 text-sm text-gray-500">
            Digest-pinned artifacts, verified registry locations, and
            distribution readiness.
          </p>
        </div>
        <div className="flex flex-wrap items-end gap-2">
          <div>
            <label
              htmlFor="images-workspace"
              className="mb-1 block text-xs font-medium text-gray-500"
            >
              Workspace
            </label>
            <Input
              id="images-workspace"
              list="images-workspaces"
              className="w-56"
              value={workspaceInput}
              onChange={(event) => setWorkspaceInput(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter')
                  selectWorkspace(workspaceInput.trim());
              }}
            />
            <datalist id="images-workspaces">
              {workspaceNames.map((name) => (
                <option key={name} value={name} />
              ))}
            </datalist>
          </div>
          <Button
            variant="outline"
            onClick={() => selectWorkspace(workspaceInput.trim())}
          >
            Apply
          </Button>
          {capabilities?.publish && (
            <Button onClick={() => setPublishOpen(true)}>
              <Upload className="mr-2 h-4 w-4" /> Publish
            </Button>
          )}
        </div>
      </div>

      {capabilities && (
        <div className="flex flex-wrap items-center gap-2 text-xs text-gray-600">
          <span className="rounded-full bg-gray-100 px-2 py-1">
            {capabilities.workspace_mode}
          </span>
          <span>
            Workspace: <strong>{capabilities.workspace}</strong>
          </span>
          {capabilities.default_distribution && (
            <span>
              Default: <strong>{capabilities.default_distribution}</strong>
            </span>
          )}
          {!capabilities.publish && (
            <span className="rounded-full bg-amber-50 px-2 py-1 text-amber-800">
              Read-only
            </span>
          )}
        </div>
      )}

      <Tabs>
        <TabsList aria-label="Images sections">
          <TabsTrigger
            active={activeTab === 'catalog'}
            onClick={() => setTab('catalog')}
          >
            Catalog
          </TabsTrigger>
          {capabilities?.admin && (
            <TabsTrigger
              active={activeTab === 'readiness'}
              onClick={() => setTab('readiness')}
            >
              Readiness
            </TabsTrigger>
          )}
        </TabsList>

        <TabsContent active={activeTab === 'catalog'}>
          <div className="space-y-4">
            <section className="rounded-lg border border-gray-200 bg-white p-4">
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
                <Input
                  placeholder="Release"
                  value={draftFilters.release}
                  onChange={(event) =>
                    setDraftFilters({
                      ...draftFilters,
                      release: event.target.value,
                    })
                  }
                />
                <Input
                  placeholder="sha256 digest"
                  value={draftFilters.digest}
                  onChange={(event) =>
                    setDraftFilters({
                      ...draftFilters,
                      digest: event.target.value,
                    })
                  }
                />
                <Input
                  placeholder="Source reference"
                  value={draftFilters.source_ref}
                  onChange={(event) =>
                    setDraftFilters({
                      ...draftFilters,
                      source_ref: event.target.value,
                    })
                  }
                />
                <select
                  className="h-10 rounded-md border border-gray-300 bg-white px-3 text-sm"
                  value={draftFilters.distribution}
                  onChange={(event) =>
                    setDraftFilters({
                      ...draftFilters,
                      distribution: event.target.value,
                    })
                  }
                >
                  <option value="">All distributions</option>
                  {capabilities?.distributions.map((item) => (
                    <option key={item.name} value={item.name}>
                      {item.name}
                    </option>
                  ))}
                </select>
                <select
                  className="h-10 rounded-md border border-gray-300 bg-white px-3 text-sm"
                  value={draftFilters.target}
                  onChange={(event) =>
                    setDraftFilters({
                      ...draftFilters,
                      target: event.target.value,
                    })
                  }
                >
                  <option value="">All targets</option>
                  {distributionTargets.map((item) => (
                    <option
                      key={`${item.distribution}:${item.target}`}
                      value={item.target}
                    >
                      {item.distribution} / {item.target}
                    </option>
                  ))}
                </select>
                <select
                  className="h-10 rounded-md border border-gray-300 bg-white px-3 text-sm"
                  value={draftFilters.state}
                  onChange={(event) =>
                    setDraftFilters({
                      ...draftFilters,
                      state: event.target.value,
                    })
                  }
                >
                  <option value="">All location states</option>
                  {[
                    'PENDING',
                    'COPYING',
                    'VERIFYING',
                    'READY',
                    'FAILED',
                    'MISSING',
                    'EVICTING',
                    'EVICTED',
                    'QUARANTINED',
                  ].map((state) => (
                    <option key={state} value={state}>
                      {state}
                    </option>
                  ))}
                </select>
              </div>
              <div className="mt-3 flex justify-end gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setDraftFilters(EMPTY_FILTERS);
                    setFilters(EMPTY_FILTERS);
                    setCursorStack([null]);
                  }}
                >
                  Clear
                </Button>
                <Button
                  size="sm"
                  onClick={() => {
                    setFilters(draftFilters);
                    setCursorStack([null]);
                  }}
                >
                  <Search className="mr-2 h-4 w-4" /> Filter
                </Button>
              </div>
            </section>

            {cursorNotice && (
              <div className="rounded-md border border-blue-200 bg-blue-50 p-3 text-sm text-blue-800">
                {cursorNotice}
              </div>
            )}
            {catalogError && (
              <div
                role="alert"
                className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800"
              >
                {catalogError}
              </div>
            )}

            {failedPublications.length > 0 && capabilities?.publish && (
              <section className="rounded-lg border border-red-200 bg-red-50 p-4">
                <h2 className="font-semibold text-red-900">
                  Failed publication reservations
                </h2>
                <p className="text-sm text-red-700">
                  Failures before inspection may not have an artifact row yet.
                </p>
                <div className="mt-3 space-y-2">
                  {failedPublications.map((publication) => (
                    <div
                      key={publication.id}
                      className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-red-200 bg-white p-3"
                    >
                      <div className="min-w-0">
                        <div className="font-medium">
                          {publication.requested_release}
                        </div>
                        <div className="truncate font-mono text-xs text-gray-500">
                          {publication.source_ref}
                        </div>
                        <div className="text-xs text-red-700">
                          {publication.error_code}
                        </div>
                      </div>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setRetryPublication(publication.id)}
                      >
                        Retry
                      </Button>
                    </div>
                  ))}
                </div>
              </section>
            )}

            <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
              <div className="flex items-center justify-between border-b border-gray-200 px-5 py-4">
                <div>
                  <h2 className="font-semibold text-gray-900">
                    Artifact catalog
                  </h2>
                  <p className="text-sm text-gray-500">
                    Immutable runtime identities and READY release projections.
                  </p>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={catalogLoading}
                  onClick={loadCatalog}
                >
                  <RefreshCw
                    className={`mr-2 h-4 w-4 ${catalogLoading ? 'animate-spin' : ''}`}
                  />
                  Refresh
                </Button>
              </div>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Releases</TableHead>
                    <TableHead>Digest / platform</TableHead>
                    <TableHead>Source</TableHead>
                    <TableHead>Size</TableHead>
                    <TableHead>Locations</TableHead>
                    <TableHead>Updated</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {catalog?.items.map((artifact) => (
                    <TableRow key={artifact.id} className="cursor-pointer">
                      <TableCell>
                        <Link
                          href={{
                            pathname: '/images/[image]',
                            query: {
                              image: artifact.id,
                              workspace: capabilities.workspace,
                            },
                          }}
                          className="block font-medium text-blue-700 hover:underline"
                        >
                          {artifact.releases.length
                            ? artifact.releases.join(', ')
                            : 'Unreleased'}
                        </Link>
                        <div className="mt-1 text-xs text-gray-500">
                          {artifact.distributions.join(', ')}
                        </div>
                      </TableCell>
                      <TableCell className="max-w-sm">
                        <Link
                          href={{
                            pathname: '/images/[image]',
                            query: {
                              image: artifact.id,
                              workspace: capabilities.workspace,
                            },
                          }}
                          className="block"
                        >
                          <code
                            className="block truncate text-xs"
                            title={artifact.runtime_digest}
                          >
                            {artifact.runtime_digest}
                          </code>
                          <span className="mt-1 block text-xs text-gray-500">
                            {artifact.platform}
                          </span>
                        </Link>
                      </TableCell>
                      <TableCell className="max-w-xs">
                        <div
                          className="truncate font-mono text-xs"
                          title={artifact.source_refs[0]}
                        >
                          {artifact.source_refs[0] || 'Unknown'}
                        </div>
                      </TableCell>
                      <TableCell>
                        {bytes(artifact.declared_size_bytes)}
                      </TableCell>
                      <TableCell>
                        {locationSummary(artifact.location_states)}
                      </TableCell>
                      <TableCell className="text-sm text-gray-500">
                        {timestamp(artifact.updated_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                  {!catalogLoading && catalog?.items.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={6} className="py-16 text-center">
                        <Box className="mx-auto mb-3 h-8 w-8 text-gray-300" />
                        <div className="font-medium text-gray-700">
                          No matching artifacts
                        </div>
                        <div className="mt-1 text-sm text-gray-500">
                          Publish a digest-pinned source or clear the filters.
                        </div>
                      </TableCell>
                    </TableRow>
                  )}
                  {catalogLoading && !catalog && (
                    <TableRow>
                      <TableCell
                        colSpan={6}
                        className="py-16 text-center text-gray-500"
                      >
                        Loading catalog…
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
              <div className="flex items-center justify-between border-t border-gray-200 px-5 py-3">
                <span className="text-sm text-gray-500">
                  Page {cursorStack.length}
                </span>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={cursorStack.length === 1 || catalogLoading}
                    onClick={() =>
                      setCursorStack((stack) => stack.slice(0, -1))
                    }
                  >
                    Previous
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={!catalog?.next_cursor || catalogLoading}
                    onClick={() =>
                      setCursorStack((stack) => [...stack, catalog.next_cursor])
                    }
                  >
                    Next
                  </Button>
                </div>
              </div>
            </section>
          </div>
        </TabsContent>

        <TabsContent active={activeTab === 'readiness'}>
          {capabilities?.admin && (
            <ImageReadiness
              readiness={readiness}
              capabilities={capabilities}
              loading={readinessLoading}
              error={readinessError}
              onRefresh={loadReadiness}
            />
          )}
        </TabsContent>
      </Tabs>

      {capabilities && (
        <PublishImageDialog
          open={publishOpen}
          onOpenChange={(open) => {
            setPublishOpen(open);
            if (!open) loadCatalog();
          }}
          workspace={capabilities.workspace}
          capabilities={capabilities}
          onChanged={loadCatalog}
        />
      )}
      <RetryImageDialog
        open={Boolean(retryPublication)}
        onOpenChange={(open) => {
          if (!open) {
            setRetryPublication(null);
            loadCatalog();
          }
        }}
        workspace={capabilities?.workspace}
        kind="publication"
        recordId={retryPublication}
        onChanged={loadCatalog}
      />
    </div>
  );
}
