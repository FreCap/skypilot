import React, { useMemo, useState } from 'react';
import { Copy, RefreshCw, ShieldCheck } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { StatusBadge } from '@/components/elements/StatusBadge';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  CanaryProfileDialog,
  QualifyProfileDialog,
} from '@/components/image-action-dialogs';

const READINESS_CLOCK_REFRESH_MS = 5000;

function duration(seconds) {
  if (seconds === null || seconds === undefined) return 'Unknown';
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.ceil(seconds / 60)}m`;
  return `${Math.ceil(seconds / 3600)}h`;
}

function age(now, timestamp) {
  if (!timestamp) return 'Never';
  return `${duration(Math.max(0, now - timestamp))} ago`;
}

function bounded(value, atLeast) {
  return `${Number(value || 0).toLocaleString()}${atLeast ? '+' : ''}`;
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

function SummaryCard({ label, value, detail, warning }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-4">
      <div className="text-xs font-medium uppercase tracking-wide text-gray-500">
        {label}
      </div>
      <div
        className={
          warning
            ? 'mt-1 text-2xl font-semibold text-amber-700'
            : 'mt-1 text-2xl font-semibold text-gray-900'
        }
      >
        {value}
      </div>
      <div className="mt-1 text-xs text-gray-500">{detail}</div>
    </div>
  );
}

function CopyValue({ label, value }) {
  const [copied, setCopied] = useState(false);
  return (
    <div>
      <div className="text-xs font-medium text-gray-500">{label}</div>
      <div className="mt-1 flex items-center gap-2 rounded-md bg-gray-50 px-3 py-2">
        <code className="min-w-0 flex-1 break-all text-xs">{value}</code>
        <button
          type="button"
          className="rounded p-1 text-gray-500 hover:bg-gray-200"
          aria-label={`Copy ${label}`}
          onClick={async () => {
            await navigator.clipboard.writeText(value);
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
          }}
        >
          <Copy className="h-4 w-4" />
        </button>
      </div>
      {copied && <div className="mt-1 text-xs text-green-700">Copied</div>}
    </div>
  );
}

export function ImageReadiness({
  readiness,
  capabilities,
  loading,
  error,
  onRefresh,
}) {
  const [qualifyOpen, setQualifyOpen] = useState(false);
  const [canaryOpen, setCanaryOpen] = useState(false);
  const [now, setNow] = useState(() => Math.floor(Date.now() / 1000));
  useVisibleRefreshInterval(true, READINESS_CLOCK_REFRESH_MS, () => {
    setNow(Math.floor(Date.now() / 1000));
  });
  const snapshotAge = readiness?.generated_at
    ? Math.max(0, now - readiness.generated_at)
    : null;
  const stale = Boolean(error) || snapshotAge === null || snapshotAge > 60;
  const healthyWorkers =
    readiness?.workers.filter((worker) => now - worker.heartbeat_at <= 30) ||
    [];
  const staleWorkers =
    readiness?.workers.filter((worker) => now - worker.heartbeat_at > 30) || [];
  const activeProfiles =
    readiness?.profiles.filter((profile) => profile.state === 'ACTIVE') || [];
  const desiredProfiles =
    readiness?.profiles.filter((profile) => profile.state === 'QUALIFYING') ||
    [];
  const profileNames = useMemo(
    () => [...new Set(capabilities.distributions.map((item) => item.name))],
    [capabilities]
  );
  const queueDepth =
    readiness?.queues.reduce((sum, queue) => sum + queue.queue_depth, 0) || 0;
  const queueDepthAtLeast = Boolean(
    readiness?.queues_truncated ||
      readiness?.queues.some((queue) => queue.queue_depth_at_least)
  );
  const failedCount =
    readiness?.queues.reduce((sum, queue) => sum + queue.failed_count, 0) || 0;
  const failedCountAtLeast = Boolean(
    readiness?.queues_truncated ||
      readiness?.queues.some((queue) => queue.failed_count_at_least)
  );
  const allQualificationTargets =
    readiness?.profiles.flatMap((profile) =>
      (profile.qualification_targets || []).map((target) => ({
        ...target,
        profile: profile.profile,
      }))
    ) || [];
  const safeGenerationByTarget = new Map();
  allQualificationTargets.forEach((target) => {
    if (!target.repository_attested || target.repository_quarantined) return;
    const key = JSON.stringify([
      target.profile,
      target.target,
      target.target_fingerprint,
    ]);
    safeGenerationByTarget.set(
      key,
      Math.max(
        safeGenerationByTarget.get(key) ?? -1,
        target.repository_generation
      )
    );
  });
  const quarantinedTargets = allQualificationTargets
    .filter((target) => target.repository_quarantined)
    .map((target) => {
      const replacementGeneration =
        safeGenerationByTarget.get(
          JSON.stringify([
            target.profile,
            target.target,
            target.target_fingerprint,
          ])
        ) ?? -1;
      return {
        ...target,
        replacement_generation:
          replacementGeneration > target.repository_generation
            ? replacementGeneration
            : null,
      };
    });
  const unresolvedQuarantinedTargets = quarantinedTargets.filter(
    (target) => target.replacement_generation === null
  );
  const repositoryQuarantineCount =
    readiness?.qualification_repository_quarantines?.length ||
    quarantinedTargets.length;
  const qualificationMutation = readiness?.qualification_mutation;
  const qualificationMutationDetail =
    qualificationMutation?.state === 'RESTORING'
      ? 'Only the exact owner copy may restore the digest. Other copy, canary, staging and activation work remains fenced until restoration clears.'
      : qualificationMutation?.state === 'QUARANTINED'
        ? 'The old repository is permanently excluded. The same logical profile may stage a higher generation and ingest its fresh Terraform handoff.'
        : 'Provider deletion and all copy, canary, staging and activation work remain fenced until readback completes.';
  const handoffBlocked = Boolean(
    qualificationMutation && qualificationMutation.state !== 'QUARANTINED'
  );
  // A completed quarantine cutover intentionally leaves the previous ACTIVE
  // row pointing at its permanent tombstone while the fresh QUALIFYING row is
  // canaried. The catalog mutation is the authoritative global fence; target
  // tombstones remain visible remediation/history and are enforced again by
  // the server for the exact selected revision.
  const canaryBlocked = Boolean(qualificationMutation);

  if (loading && !readiness) {
    return (
      <div className="py-16 text-center text-gray-500">Loading readiness…</div>
    );
  }
  if (error && !readiness) {
    return (
      <div
        role="alert"
        className="rounded-md border border-red-200 bg-red-50 p-4 text-red-800"
      >
        {error}
      </div>
    );
  }
  if (!readiness) return null;

  return (
    <div className="space-y-6">
      {stale && (
        <div
          role="alert"
          className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900"
        >
          {error
            ? `Latest refresh failed (${error}). The cached snapshot is read-only until refresh succeeds.`
            : `This snapshot is ${age(now, readiness.generated_at)} old. Mutations are disabled until refresh succeeds.`}
        </div>
      )}
      {qualificationMutation && (
        <div
          role="status"
          className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900"
        >
          Qualification mutation <strong>{qualificationMutation.state}</strong>{' '}
          for {qualificationMutation.owner_target}.{' '}
          {qualificationMutationDetail}
        </div>
      )}
      {quarantinedTargets.length > 0 && (
        <div
          role="alert"
          className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-900"
        >
          <div className="font-semibold">
            {unresolvedQuarantinedTargets.length > 0
              ? 'Qualification repository cutover required'
              : 'Qualification repository quarantine retained'}
          </div>
          <div className="mt-1">
            {unresolvedQuarantinedTargets.length > 0
              ? 'An ambiguous delete permanently tombstoned a shared physical repository. Retain the old generation, provision a higher generation with Terraform, update the profile YAML, then ingest the new handoff.'
              : 'The old physical repository remains permanently tombstoned while its fresh qualification generation completes canary and activation.'}
          </div>
          <ul className="mt-2 list-disc space-y-1 pl-5">
            {quarantinedTargets.slice(0, 5).map((target) => (
              <li key={`${target.profile}:${target.target}`}>
                {target.profile} / {target.target}: generation{' '}
                {target.repository_generation}
                {target.replacement_generation !== null
                  ? `, fresh generation ${target.replacement_generation} is qualifying`
                  : target.required_generation !== null
                    ? `, use generation ${target.required_generation} or higher`
                    : ', generation space exhausted'}
              </li>
            ))}
          </ul>
          {quarantinedTargets.length > 5 && (
            <div className="mt-2">
              Plus {quarantinedTargets.length - 5} more affected targets.
            </div>
          )}
        </div>
      )}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">
            Distribution readiness
          </h2>
          <p className="text-sm text-gray-500">
            PostgreSQL projections only. This page performs no registry or cloud
            calls.
          </p>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={onRefresh}
            disabled={loading}
          >
            <RefreshCw
              className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`}
            />
            Refresh
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setQualifyOpen(true)}
            disabled={stale || loading || handoffBlocked}
            title={
              stale
                ? 'Refresh readiness before changing state'
                : handoffBlocked
                  ? 'Only a quarantined mutation accepts a fresh handoff'
                  : undefined
            }
          >
            Ingest handoff
          </Button>
          <Button
            size="sm"
            onClick={() => setCanaryOpen(true)}
            disabled={stale || loading || canaryBlocked}
            title={
              stale
                ? 'Refresh readiness before changing state'
                : canaryBlocked
                  ? 'Resolve qualification mutation or repository quarantine first'
                  : undefined
            }
          >
            <ShieldCheck className="mr-2 h-4 w-4" />
            Run canary
          </Button>
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <SummaryCard
          label="Active profiles"
          value={activeProfiles.length}
          detail={`${desiredProfiles.length} desired revisions qualifying`}
          warning={desiredProfiles.length > 0}
        />
        <SummaryCard
          label="Healthy workers"
          value={healthyWorkers.length}
          detail={`${staleWorkers.length} stale heartbeats`}
          warning={staleWorkers.length > 0}
        />
        <SummaryCard
          label="Queued locations"
          value={bounded(queueDepth, queueDepthAtLeast)}
          detail={`${bounded(failedCount, failedCountAtLeast)} terminal failures`}
          warning={failedCount > 0}
        />
        <SummaryCard
          label="Provider backoffs"
          value={
            readiness.provider_budgets.filter(
              (budget) => budget.blocked_until && budget.blocked_until > now
            ).length
          }
          detail="Shared account-region budgets"
          warning={readiness.provider_budgets.some(
            (budget) => budget.blocked_until && budget.blocked_until > now
          )}
        />
        <SummaryCard
          label="Repo quarantines"
          value={`${repositoryQuarantineCount}${
            readiness.qualification_repository_quarantines_truncated ? '+' : ''
          }`}
          detail={
            unresolvedQuarantinedTargets.length > 0
              ? `${unresolvedQuarantinedTargets.length} target${
                  unresolvedQuarantinedTargets.length === 1 ? '' : 's'
                } require${
                  unresolvedQuarantinedTargets.length === 1 ? 's' : ''
                } a new generation`
              : quarantinedTargets.length > 0
                ? `${quarantinedTargets.length} replaced target${
                    quarantinedTargets.length === 1 ? '' : 's'
                  } reference${
                    quarantinedTargets.length === 1 ? 's' : ''
                  } a retained tombstone`
                : repositoryQuarantineCount > 0
                  ? 'No selected target uses a tombstoned repository'
                  : 'No physical repository tombstones'
          }
          warning={repositoryQuarantineCount > 0}
        />
      </div>

      <section className="rounded-lg border border-gray-200 bg-white p-5">
        <h3 className="font-semibold text-gray-900">
          Terraform handoff identity
        </h3>
        <p className="mt-1 text-sm text-gray-500">
          These stable values bind repositories to this exact catalog. They
          contain no credentials.
        </p>
        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          <CopyValue
            label="Catalog authority"
            value={readiness.catalog_authority}
          />
          <CopyValue
            label="Catalog authority base32"
            value={readiness.catalog_authority_base32}
          />
        </div>
      </section>

      <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
        <div className="border-b border-gray-200 px-5 py-4">
          <h3 className="font-semibold text-gray-900">
            Profiles and attestations
          </h3>
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Profile</TableHead>
              <TableHead>Revision</TableHead>
              <TableHead>Generation</TableHead>
              <TableHead>State</TableHead>
              <TableHead>Qualification repo</TableHead>
              <TableHead>Evidence</TableHead>
              <TableHead>Updated</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {readiness.profiles.map((profile) => (
              <TableRow key={profile.id}>
                <TableCell className="font-medium">{profile.profile}</TableCell>
                <TableCell>{profile.revision}</TableCell>
                <TableCell>{profile.desired_generation}</TableCell>
                <TableCell>
                  <StatusBadge status={profile.state} />
                </TableCell>
                <TableCell>
                  <div className="space-y-1">
                    {(profile.qualification_targets || []).map((target) => (
                      <div
                        key={target.target}
                        className={
                          target.repository_quarantined
                            ? 'text-xs font-medium text-red-700'
                            : !target.repository_attested
                              ? 'text-xs text-gray-400'
                              : 'text-xs text-gray-600'
                        }
                      >
                        {target.target}: g{target.repository_generation}{' '}
                        {target.repository_quarantined
                          ? target.required_generation === null
                            ? '(quarantined, generation space exhausted)'
                            : `(quarantined, use g${target.required_generation}+)`
                          : target.repository_attested
                            ? 'attested'
                            : 'not attested'}
                      </div>
                    ))}
                    {(profile.qualification_targets || []).length === 0 && (
                      <span className="text-xs text-gray-400">
                        Not attested
                      </span>
                    )}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex max-w-md flex-wrap gap-1">
                    {Object.entries(profile.attestations).map(
                      ([kind, evidence]) => (
                        <span
                          key={kind}
                          title={`${kind}: ${age(now, evidence?.observed_at)}`}
                          className={`rounded-full px-2 py-1 text-xs ${
                            evidence?.status === 'READY'
                              ? 'bg-green-50 text-green-700'
                              : 'bg-amber-50 text-amber-800'
                          }`}
                        >
                          {kind}: {evidence?.status || 'UNKNOWN'}
                        </span>
                      )
                    )}
                    {Object.keys(profile.attestations).length === 0 && (
                      <span className="text-sm text-gray-400">No evidence</span>
                    )}
                  </div>
                </TableCell>
                <TableCell className="text-sm text-gray-500">
                  {age(now, profile.updated_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </section>

      <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
        <div className="border-b border-gray-200 px-5 py-4">
          <h3 className="font-semibold text-gray-900">
            Target queues and capacity
          </h3>
          <p className="text-sm text-gray-500">
            ETA is a provider-quota lower bound. It does not predict node cache
            fill or replica health.
          </p>
          {readiness.queues_truncated && (
            <p className="mt-1 text-sm font-medium text-amber-700">
              Showing the first 100 target groups. Counts are lower bounds.
            </p>
          )}
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Profile / target</TableHead>
              <TableHead>Queue</TableHead>
              <TableHead>Oldest</TableHead>
              <TableHead>Quota-bound ETA</TableHead>
              <TableHead>Manifest headroom</TableHead>
              <TableHead>Byte headroom</TableHead>
              <TableHead>In flight</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {readiness.queues.map((queue) => (
              <TableRow key={`${queue.profile}:${queue.target}`}>
                <TableCell>
                  <div className="font-medium">{queue.profile}</div>
                  <div className="text-xs text-gray-500">
                    {queue.target} · {queue.region}
                  </div>
                </TableCell>
                <TableCell>
                  {bounded(queue.queue_depth, queue.queue_depth_at_least)}
                  {queue.failed_count > 0 && (
                    <div className="text-xs text-red-700">
                      {bounded(queue.failed_count, queue.failed_count_at_least)}{' '}
                      failed
                    </div>
                  )}
                  {queue.quarantined_count > 0 && (
                    <div className="text-xs text-red-700">
                      {bounded(
                        queue.quarantined_count,
                        queue.quarantined_count_at_least
                      )}{' '}
                      quarantined ·{' '}
                      {queue.quarantined_reserved_declared_bytes_at_least
                        ? '≥'
                        : ''}
                      {bytes(queue.quarantined_reserved_declared_bytes)}{' '}
                      retained
                    </div>
                  )}
                </TableCell>
                <TableCell>{age(now, queue.oldest_queued_at)}</TableCell>
                <TableCell>
                  {queue.quota_blocked_until > now
                    ? `Backoff ${duration(queue.quota_blocked_until - now)}`
                    : `${queue.quota_bound_eta_at_least ? '≥' : ''}${duration(
                        queue.quota_bound_eta_seconds
                      )}`}
                  <div className="text-xs text-gray-500">
                    {queue.quota_rate_per_second
                      ? `${queue.quota_rate_per_second}/s shared`
                      : 'Budget unavailable'}
                  </div>
                </TableCell>
                <TableCell>
                  {(
                    queue.max_manifests - queue.reserved_manifests
                  ).toLocaleString()}
                  <div className="text-xs text-gray-500">
                    of {queue.max_manifests.toLocaleString()}
                  </div>
                </TableCell>
                <TableCell>
                  {bytes(
                    queue.max_declared_bytes - queue.reserved_declared_bytes
                  )}
                </TableCell>
                <TableCell>
                  {queue.in_flight} / {queue.max_in_flight}
                </TableCell>
              </TableRow>
            ))}
            {readiness.queues.length === 0 && (
              <TableRow>
                <TableCell
                  colSpan={7}
                  className="py-10 text-center text-gray-500"
                >
                  No qualified target shards exist for this workspace.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </section>

      <div className="grid gap-6 xl:grid-cols-2">
        <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
          <div className="border-b border-gray-200 px-5 py-4">
            <h3 className="font-semibold text-gray-900">Worker health</h3>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Kind</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Heartbeat</TableHead>
                <TableHead>In flight</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {readiness.workers.map((worker) => {
                const stale = now - worker.heartbeat_at > 30;
                return (
                  <TableRow key={worker.id}>
                    <TableCell className="font-medium">{worker.kind}</TableCell>
                    <TableCell className="font-mono text-xs">
                      {worker.version}
                    </TableCell>
                    <TableCell
                      className={stale ? 'text-amber-700' : 'text-green-700'}
                    >
                      {age(now, worker.heartbeat_at)}
                    </TableCell>
                    <TableCell>
                      {worker.in_flight} / {worker.max_in_flight}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </section>

        <section className="overflow-hidden rounded-lg border border-gray-200 bg-white">
          <div className="border-b border-gray-200 px-5 py-4">
            <h3 className="font-semibold text-gray-900">Provider budgets</h3>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Region</TableHead>
                <TableHead>Rate / burst</TableHead>
                <TableHead>Backoff</TableHead>
                <TableHead>Throttles</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {readiness.provider_budgets.map((budget) => (
                <TableRow
                  key={`${budget.provider}:${budget.account}:${budget.region}:${budget.api_family}`}
                >
                  <TableCell>
                    <div className="font-medium">{budget.region}</div>
                    <div className="text-xs text-gray-500">
                      {budget.api_family}
                    </div>
                  </TableCell>
                  <TableCell>
                    {budget.applied_rate_per_second}/s · {budget.burst}
                  </TableCell>
                  <TableCell>
                    {budget.blocked_until > now
                      ? duration(budget.blocked_until - now)
                      : 'None'}
                  </TableCell>
                  <TableCell>{budget.throttle_count}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </section>
      </div>

      {(readiness.profiles_truncated ||
        readiness.shards_truncated ||
        readiness.workers_truncated ||
        readiness.provider_budgets_truncated ||
        readiness.queues_truncated) && (
        <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
          This readiness projection reached a safety bound. Narrow the workspace
          or use paginated APIs.
        </div>
      )}

      <QualifyProfileDialog
        open={qualifyOpen}
        onOpenChange={setQualifyOpen}
        workspace={readiness.workspace}
        profiles={profileNames}
        onChanged={onRefresh}
      />
      <CanaryProfileDialog
        open={canaryOpen}
        onOpenChange={setCanaryOpen}
        workspace={readiness.workspace}
        capabilities={capabilities}
        onChanged={onRefresh}
      />
    </div>
  );
}
