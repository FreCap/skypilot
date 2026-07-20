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
  PrepareImageDialog,
  RetryImageDialog,
} from '@/components/image-action-dialogs';
import {
  getImageArtifactDetail,
  getImageCapabilities,
} from '@/data/connectors/images';

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

export function ImageDetail() {
  const router = useRouter();
  const imageId =
    typeof router.query.image === 'string' ? router.query.image : null;
  const requestedWorkspace =
    typeof router.query.workspace === 'string' ? router.query.workspace : null;
  const [workspace, setWorkspace] = useState(requestedWorkspace);
  const [detail, setDetail] = useState(null);
  const [capabilities, setCapabilities] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [oldServer, setOldServer] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');
  const [prepareOpen, setPrepareOpen] = useState(false);
  const [retry, setRetry] = useState(null);
  const generation = useRef(0);
  const requestController = useRef(null);

  const load = useCallback(async () => {
    if (!imageId) return undefined;
    requestController.current?.abort();
    const controller = new AbortController();
    requestController.current = controller;
    const currentGeneration = ++generation.current;
    setLoading(true);
    setError(null);
    setOldServer(false);
    let capabilitiesLoaded = false;
    try {
      const nextCapabilities = await getImageCapabilities(
        requestedWorkspace,
        controller.signal
      );
      capabilitiesLoaded = true;
      const nextDetail = await getImageArtifactDetail(
        imageId,
        nextCapabilities.workspace,
        controller.signal
      );
      if (generation.current !== currentGeneration) return;
      setWorkspace(nextCapabilities.workspace);
      setCapabilities(nextCapabilities);
      setDetail(nextDetail);
    } catch (requestError) {
      if (
        requestError.name !== 'AbortError' &&
        generation.current === currentGeneration
      ) {
        if (
          !capabilitiesLoaded &&
          (requestError.status === 404 || requestError.status === 426)
        ) {
          setOldServer(true);
        } else {
          setError(requestError.code || requestError.message);
        }
      }
    } finally {
      if (generation.current === currentGeneration) setLoading(false);
      if (requestController.current === controller) {
        requestController.current = null;
      }
    }
  }, [imageId, requestedWorkspace]);

  useEffect(() => {
    load();
    return () => {
      generation.current += 1;
      requestController.current?.abort();
      requestController.current = null;
    };
  }, [load]);

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

  useEffect(() => {
    if (!hasNonterminal) return undefined;
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [hasNonterminal, load]);

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
  return (
    <div className="mx-auto max-w-[1500px] space-y-6">
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
            <Button onClick={() => setPrepareOpen(true)}>Prepare target</Button>
          )}
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-4">
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <div className="text-xs text-gray-500">Releases</div>
          <div className="mt-1 text-2xl font-semibold">
            {detail.releases.length}
          </div>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <div className="text-xs text-gray-500">Registry locations</div>
          <div className="mt-1 text-2xl font-semibold">
            {detail.locations.length}
          </div>
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <div className="text-xs text-gray-500">Active demands</div>
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

      {detail.truncated && (
        <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
          One detail collection exceeded 100 rows. Use the paginated API for the
          complete history.
        </div>
      )}

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
                      {source.requested_platform} · binding{' '}
                      {source.source_auth_binding_id || 'public'}
                    </div>
                  </div>
                ))}
              </div>
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
