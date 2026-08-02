import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';

import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { StatusBadge } from '@/components/elements/StatusBadge';
import {
  canaryImageProfile,
  getImageOperation,
  newIdempotencyKey,
  prepareImage,
  publishImage,
  qualifyImageProfile,
  retryImageLocation,
  retryImagePublication,
} from '@/data/connectors/images';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';

export const IMAGE_REMEDIATIONS = {
  ARTIFACT_NOT_READY: 'Wait for canonical verification, then retry prepare.',
  AUTH_BINDING_UNAVAILABLE:
    'Restore the configured source binding without changing its identity.',
  CANARY_DAILY_COST_LIMIT:
    'Wait for the next UTC budget window or raise the reviewed cap.',
  CANARY_FAILED: 'Inspect the target attestation and launch evidence.',
  IMAGE_LIMIT_EXCEEDED: 'Retire releases or raise the reviewed profile limit.',
  IMAGE_LOCALITY_UNSUPPORTED:
    'Choose a qualified target and runtime backend for this placement.',
  IMAGE_PREPARATION_FAILED: 'Inspect the failed location, then retry it.',
  PLATFORM_UNSUPPORTED: 'Publish a supported OCI platform child.',
  PROFILE_NOT_ACTIVE:
    'Apply Terraform evidence and complete profile qualification first.',
  PROVIDER_OUTCOME_AMBIGUOUS:
    'Activate a qualified target with a new repository ring. This physical reference is quarantined.',
  PROVIDER_THROTTLED: 'The shared provider budget will retry automatically.',
  QUALIFICATION_FAILED:
    'Compare the profile revision, Terraform handoff, and attestations.',
  REGISTRY_CAPACITY_EXHAUSTED:
    'Provision and qualify a new fixed shard generation before retrying.',
  REGISTRY_LOCATION_QUARANTINED:
    'Activate a qualified target with a new repository ring before preparing again.',
  REGISTRY_SHARD_UNAVAILABLE:
    'Repair shard drift or activate a qualified revision before retrying.',
  RELEASE_CONFLICT: 'Use a new release or the existing immutable artifact.',
};

const OPERATION_POLL_INTERVAL_MS = 2000;

export function ImageError({ code }) {
  if (!code) return null;
  return (
    <div
      role="alert"
      className="rounded-md border border-red-200 bg-red-50 p-3"
    >
      <div className="font-mono text-sm font-semibold text-red-800">{code}</div>
      <div className="mt-1 text-sm text-red-700">
        {IMAGE_REMEDIATIONS[code] ||
          'Retry after checking the bounded Images readiness evidence.'}
      </div>
    </div>
  );
}

function OperationProgress({ mutation, workspace, onTerminal }) {
  const [operation, setOperation] = useState(mutation.operation);
  const [pollError, setPollError] = useState(null);
  const generation = useRef(0);
  const terminalNotification = useRef(null);
  const pollControllerRef = useRef(null);
  const pollOwnerRef = useRef(null);
  const catchUpPollRef = useRef(false);
  const nextPollDueAtRef = useRef(null);
  const operationId = operation?.id;
  const operationTerminal =
    operation && ['SUCCEEDED', 'FAILED'].includes(operation.state);

  useEffect(() => {
    if (!operation || !['SUCCEEDED', 'FAILED'].includes(operation.state))
      return;
    const notificationKey = `${operation.id}:${operation.state}`;
    if (terminalNotification.current === notificationKey) return;
    terminalNotification.current = notificationKey;
    onTerminal?.(operation);
  }, [operation, onTerminal]);

  const startPoll = useCallback(
    (refreshSource = 'poll') => {
      if (!operationId || operationTerminal) {
        return false;
      }
      if (
        refreshSource === 'visibilitychange' &&
        nextPollDueAtRef.current !== null &&
        performance.now() < nextPollDueAtRef.current
      ) {
        return false;
      }
      if (pollOwnerRef.current !== null) {
        catchUpPollRef.current = true;
        return false;
      }

      const currentGeneration = generation.current;
      nextPollDueAtRef.current = performance.now() + OPERATION_POLL_INTERVAL_MS;
      const controller = pollControllerRef.current;
      if (controller === null) {
        return false;
      }
      let terminal = false;
      const pollPromise = (async () => {
        try {
          const next = await getImageOperation(
            operationId,
            workspace,
            controller.signal
          );
          if (generation.current !== currentGeneration) return;
          setOperation(next);
          setPollError(null);
          terminal = ['SUCCEEDED', 'FAILED'].includes(next.state);
        } catch (error) {
          if (
            generation.current === currentGeneration &&
            error.name !== 'AbortError'
          ) {
            setPollError(error.code || error.message);
          }
        }
      })().finally(() => {
        if (pollOwnerRef.current?.promise === pollPromise) {
          pollOwnerRef.current = null;
        }
        if (generation.current !== currentGeneration || terminal) {
          return;
        }
        if (
          catchUpPollRef.current &&
          window.document.visibilityState === 'visible'
        ) {
          catchUpPollRef.current = false;
          void startPoll();
        }
      });
      pollOwnerRef.current = {
        promise: pollPromise,
      };
      return true;
    },
    [operationId, operationTerminal, workspace]
  );

  useEffect(() => {
    generation.current += 1;
    catchUpPollRef.current = false;
    nextPollDueAtRef.current = null;
    pollControllerRef.current?.abort();
    pollControllerRef.current = new AbortController();
    pollOwnerRef.current = null;

    if (!operationId || operationTerminal) {
      return undefined;
    }
    nextPollDueAtRef.current = performance.now() + OPERATION_POLL_INTERVAL_MS;
    void startPoll('initial');

    return () => {
      generation.current += 1;
      catchUpPollRef.current = false;
      nextPollDueAtRef.current = null;
      pollControllerRef.current?.abort();
      pollControllerRef.current = null;
      pollOwnerRef.current = null;
    };
  }, [operationId, operationTerminal, workspace, startPoll]);

  useVisibleRefreshInterval(
    Boolean(operationId && !operationTerminal),
    OPERATION_POLL_INTERVAL_MS,
    startPoll
  );

  return (
    <div className="space-y-3 rounded-md border border-gray-200 bg-gray-50 p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="text-sm font-medium">Background operation</span>
        <StatusBadge status={operation.state} />
      </div>
      <code className="block break-all text-xs text-gray-600">
        {operation.id}
      </code>
      <ImageError code={operation.error_code || pollError} />
      {!['SUCCEEDED', 'FAILED'].includes(operation.state) && (
        <p className="text-xs text-gray-500">
          Closing this dialog detaches the browser. It does not cancel committed
          provider work.
        </p>
      )}
    </div>
  );
}

function ActionDialog({
  open,
  onOpenChange,
  title,
  description,
  children,
  mutation,
  workspace,
  submitting,
  canSubmit,
  submitLabel,
  error,
  onSubmit,
  onTerminal,
}) {
  const terminal =
    mutation && ['SUCCEEDED', 'FAILED'].includes(mutation.operation.state);
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          {!mutation && children}
          <ImageError code={error} />
          {mutation && (
            <OperationProgress
              mutation={mutation}
              workspace={workspace}
              onTerminal={onTerminal}
            />
          )}
        </div>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => onOpenChange(false)}
          >
            {mutation && !terminal ? 'Detach' : 'Close'}
          </Button>
          {!mutation && (
            <Button
              type="button"
              disabled={submitting || !canSubmit}
              onClick={onSubmit}
            >
              {submitting ? 'Submitting…' : submitLabel}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function useMutationForm(open) {
  const [idempotencyKey, setIdempotencyKey] = useState(null);
  const [mutation, setMutation] = useState(null);
  const [error, setError] = useState(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (open && idempotencyKey === null) setIdempotencyKey(newIdempotencyKey());
    if (!open) {
      setIdempotencyKey(null);
      setMutation(null);
      setError(null);
      setSubmitting(false);
    }
  }, [open, idempotencyKey]);

  const run = async (fn) => {
    setSubmitting(true);
    setError(null);
    try {
      const result = await fn(idempotencyKey);
      setMutation(result);
      return result;
    } catch (requestError) {
      setError(requestError.code || requestError.message);
      return null;
    } finally {
      setSubmitting(false);
    }
  };
  return { mutation, error, submitting, run };
}

export function PublishImageDialog({
  open,
  onOpenChange,
  workspace,
  capabilities,
  onChanged,
}) {
  const activeDistributions = useMemo(
    () => capabilities.distributions.filter((item) => item.active),
    [capabilities]
  );
  const [form, setForm] = useState({
    source_ref: '',
    release: '',
    platform: 'linux/amd64',
    distribution: '',
    source_auth: '',
  });
  const state = useMutationForm(open);

  useEffect(() => {
    if (open && !form.distribution) {
      setForm((current) => ({
        ...current,
        distribution:
          capabilities.default_distribution ||
          activeDistributions[0]?.name ||
          '',
      }));
    }
  }, [open, form.distribution, capabilities, activeDistributions]);

  const digestPinned = /@sha256:[0-9a-f]{64}$/.test(form.source_ref.trim());
  const canSubmit =
    digestPinned &&
    form.release.trim().length > 0 &&
    form.distribution.length > 0 &&
    form.platform.includes('/');

  const submit = () =>
    state.run((key) =>
      publishImage(
        {
          ...form,
          source_ref: form.source_ref.trim(),
          release: form.release.trim(),
          source_auth: form.source_auth || null,
          workspace,
        },
        key
      )
    );

  return (
    <ActionDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Publish a digest-pinned image"
      description="Reserve one immutable release and inspect it asynchronously."
      mutation={state.mutation}
      workspace={workspace}
      submitting={state.submitting}
      canSubmit={canSubmit}
      submitLabel="Publish"
      error={state.error}
      onSubmit={submit}
      onTerminal={onChanged}
    >
      <div className="space-y-2">
        <Label htmlFor="image-source">OCI source digest</Label>
        <Input
          id="image-source"
          value={form.source_ref}
          onChange={(event) =>
            setForm({ ...form, source_ref: event.target.value })
          }
          placeholder="registry.example/model@sha256:…"
          aria-invalid={form.source_ref.length > 0 && !digestPinned}
        />
        <p className="text-xs text-gray-500">Mutable tags are not accepted.</p>
      </div>
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="space-y-2">
          <Label htmlFor="image-release">Release</Label>
          <Input
            id="image-release"
            value={form.release}
            onChange={(event) =>
              setForm({ ...form, release: event.target.value })
            }
            placeholder="boltz-l4-2026-07-20"
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="image-platform">Platform</Label>
          <Input
            id="image-platform"
            value={form.platform}
            onChange={(event) =>
              setForm({ ...form, platform: event.target.value })
            }
          />
        </div>
      </div>
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="space-y-2">
          <Label htmlFor="image-distribution">Distribution</Label>
          <select
            id="image-distribution"
            className="h-10 w-full rounded-md border border-gray-300 bg-white px-3 text-sm"
            value={form.distribution}
            onChange={(event) =>
              setForm({ ...form, distribution: event.target.value })
            }
          >
            <option value="">Select a profile</option>
            {activeDistributions.map((item) => (
              <option key={item.name} value={item.name}>
                {item.name}
              </option>
            ))}
          </select>
        </div>
        <div className="space-y-2">
          <Label htmlFor="image-source-auth">Source binding</Label>
          <select
            id="image-source-auth"
            className="h-10 w-full rounded-md border border-gray-300 bg-white px-3 text-sm"
            value={form.source_auth}
            onChange={(event) =>
              setForm({ ...form, source_auth: event.target.value })
            }
          >
            <option value="">Public source</option>
            {capabilities.source_bindings.map((binding) => (
              <option key={binding} value={binding}>
                {binding}
              </option>
            ))}
          </select>
        </div>
      </div>
    </ActionDialog>
  );
}

export function PrepareImageDialog({
  open,
  onOpenChange,
  workspace,
  artifact,
  capabilities,
  onChanged,
}) {
  const options = useMemo(
    () =>
      capabilities.distributions
        .filter((distribution) => distribution.active)
        .flatMap((distribution) =>
          distribution.targets.map((target) => ({
            value: `${distribution.name}:${target.name}`,
            distribution: distribution.name,
            target: target.name,
            label: `${distribution.name} / ${target.name} (${target.region})`,
          }))
        ),
    [capabilities]
  );
  const [selection, setSelection] = useState('');
  const state = useMutationForm(open);
  useEffect(() => {
    if (open && !selection && options[0]) setSelection(options[0].value);
  }, [open, selection, options]);
  const selected = options.find((item) => item.value === selection);
  return (
    <ActionDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Prepare a regional location"
      description="Create one durable target intent. Deployment never performs this copy inline."
      mutation={state.mutation}
      workspace={workspace}
      submitting={state.submitting}
      canSubmit={Boolean(selected)}
      submitLabel="Prepare"
      error={state.error}
      onSubmit={() =>
        state.run((key) =>
          prepareImage(
            artifact.id,
            {
              workspace,
              distribution: selected.distribution,
              target: selected.target,
            },
            key
          )
        )
      }
      onTerminal={onChanged}
    >
      <div className="space-y-2">
        <Label htmlFor="prepare-target">Qualified target</Label>
        <select
          id="prepare-target"
          className="h-10 w-full rounded-md border border-gray-300 bg-white px-3 text-sm"
          value={selection}
          onChange={(event) => setSelection(event.target.value)}
        >
          {options.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </select>
      </div>
    </ActionDialog>
  );
}

export function RetryImageDialog({
  open,
  onOpenChange,
  workspace,
  kind,
  recordId,
  onChanged,
}) {
  const state = useMutationForm(open);
  const label = kind === 'publication' ? 'publication' : 'location';
  return (
    <ActionDialog
      open={open}
      onOpenChange={onOpenChange}
      title={`Retry failed ${label}`}
      description="Retry keeps the original immutable identity and creates a new idempotent operation."
      mutation={state.mutation}
      workspace={workspace}
      submitting={state.submitting}
      canSubmit={Boolean(recordId)}
      submitLabel="Retry"
      error={state.error}
      onSubmit={() =>
        state.run((key) =>
          kind === 'publication'
            ? retryImagePublication(recordId, workspace, key)
            : retryImageLocation(recordId, workspace, key)
        )
      }
      onTerminal={onChanged}
    >
      <p className="break-all font-mono text-sm text-gray-600">{recordId}</p>
    </ActionDialog>
  );
}

export function QualifyProfileDialog({
  open,
  onOpenChange,
  workspace,
  profiles,
  onChanged,
}) {
  const [profile, setProfile] = useState('');
  const [manifestText, setManifestText] = useState('');
  const [parseError, setParseError] = useState(null);
  const state = useMutationForm(open);
  useEffect(() => {
    if (open && !profile && profiles[0]) setProfile(profiles[0]);
  }, [open, profile, profiles]);
  const parse = () => {
    try {
      const manifest = JSON.parse(manifestText);
      setParseError(null);
      return manifest;
    } catch {
      setParseError('INVALID_QUALIFICATION_MANIFEST_JSON');
      return null;
    }
  };
  return (
    <ActionDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Ingest Terraform qualification evidence"
      description="This uploads a bounded secret-free manifest. It never runs Terraform or assumes a role in the browser."
      mutation={state.mutation}
      workspace={workspace}
      submitting={state.submitting}
      canSubmit={profile.length > 0 && manifestText.trim().length > 0}
      submitLabel="Ingest manifest"
      error={state.error || parseError}
      onSubmit={() => {
        const manifest = parse();
        if (manifest) {
          state.run((key) => qualifyImageProfile(profile, manifest, key));
        }
      }}
      onTerminal={onChanged}
    >
      <div className="space-y-2">
        <Label htmlFor="qualification-profile">Profile</Label>
        <select
          id="qualification-profile"
          className="h-10 w-full rounded-md border border-gray-300 bg-white px-3 text-sm"
          value={profile}
          onChange={(event) => setProfile(event.target.value)}
        >
          {profiles.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
      </div>
      <div className="space-y-2">
        <Label htmlFor="qualification-manifest">Terraform JSON manifest</Label>
        <Textarea
          id="qualification-manifest"
          className="min-h-64 font-mono text-xs"
          value={manifestText}
          onChange={(event) => setManifestText(event.target.value)}
          spellCheck={false}
        />
      </div>
    </ActionDialog>
  );
}

export function CanaryProfileDialog({
  open,
  onOpenChange,
  workspace,
  capabilities,
  onChanged,
}) {
  const options = useMemo(
    () =>
      capabilities.distributions.flatMap((distribution) =>
        distribution.targets.flatMap((target) =>
          target.runtime_backends.flatMap((backend) =>
            (target.runtime_ids?.[backend] || []).map((runtimeId) => ({
              value: `${distribution.name}:${target.name}:${backend}:${runtimeId}`,
              profile: distribution.name,
              target: target.name,
              backend,
              runtimeId,
              label: `${distribution.name} / ${target.name} / ${backend} / ${runtimeId}`,
            }))
          )
        )
      ),
    [capabilities]
  );
  const [selection, setSelection] = useState('');
  const [confirmed, setConfirmed] = useState(false);
  const state = useMutationForm(open);
  useEffect(() => {
    if (open && !selection && options[0]) setSelection(options[0].value);
  }, [open, selection, options]);
  const selected = options.find((item) => item.value === selection);
  return (
    <ActionDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Run an actual-principal canary"
      description="The background runner uses the declared EC2 instance profile or EKS node role and enforces the daily cost cap."
      mutation={state.mutation}
      workspace={workspace}
      submitting={state.submitting}
      canSubmit={Boolean(selected && confirmed)}
      submitLabel="Run canary"
      error={state.error}
      onSubmit={() =>
        state.run((key) =>
          canaryImageProfile(
            selected.profile,
            {
              workspace,
              target: selected.target,
              backend: selected.backend,
              runtime_id: selected.runtimeId,
              confirm_cost: true,
            },
            key
          )
        )
      }
      onTerminal={onChanged}
    >
      <div className="space-y-2">
        <Label htmlFor="canary-target">Target and runtime principal</Label>
        <select
          id="canary-target"
          className="h-10 w-full rounded-md border border-gray-300 bg-white px-3 text-sm"
          value={selection}
          onChange={(event) => setSelection(event.target.value)}
        >
          {options.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </select>
      </div>
      <label className="flex items-start gap-3 rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
        <input
          type="checkbox"
          checked={confirmed}
          onChange={(event) => setConfirmed(event.target.checked)}
          className="mt-1"
        />
        <span>
          I understand this may launch temporary compute and consume the
          reviewed daily canary budget.
        </span>
      </label>
    </ActionDialog>
  );
}
