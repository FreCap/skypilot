'use client';

import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { CircularProgress } from '@mui/material';
import { diffLines, diffWordsWithSpace } from 'diff';

import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { getCurrentUserRole } from '@/data/connectors/client';
import {
  electServiceVersion,
  getServiceVersions,
} from '@/data/connectors/services';
import { YamlCodeBlock } from '@/components/ui/yaml-code-block';
import { formatYaml } from '@/lib/yamlUtils';
import { TimestampWithTooltip } from '@/components/utils';

const DIFF_CONTEXT_LINES = 3;

function splitLines(value) {
  const lines = String(value ?? '')
    .replace(/\r\n/g, '\n')
    .split('\n');
  if (lines.at(-1) === '') lines.pop();
  return lines;
}

function collapseContext(rows) {
  const compact = [];
  let index = 0;
  while (index < rows.length) {
    if (rows[index].type !== 'context') {
      compact.push(rows[index]);
      index += 1;
      continue;
    }

    let end = index;
    while (end < rows.length && rows[end].type === 'context') end += 1;
    const run = rows.slice(index, end);
    const keepBefore = index === 0 ? 0 : DIFF_CONTEXT_LINES;
    const keepAfter = end === rows.length ? 0 : DIFF_CONTEXT_LINES;
    if (run.length <= keepBefore + keepAfter + 1) {
      compact.push(...run);
    } else {
      compact.push(...run.slice(0, keepBefore));
      compact.push({
        type: 'gap',
        count: run.length - keepBefore - keepAfter,
      });
      if (keepAfter > 0) compact.push(...run.slice(-keepAfter));
    }
    index = end;
  }
  return compact;
}

export function buildSplitDiffRows(base, comparison) {
  const parts = diffLines(base, comparison);
  const rows = [];
  let baseLine = 1;
  let comparisonLine = 1;

  for (let index = 0; index < parts.length; ) {
    const part = parts[index];
    if (!part.added && !part.removed) {
      splitLines(part.value).forEach((line) => {
        rows.push({
          type: 'context',
          baseLine: baseLine++,
          comparisonLine: comparisonLine++,
          baseText: line,
          comparisonText: line,
        });
      });
      index += 1;
      continue;
    }

    const removed = [];
    const added = [];
    while (
      index < parts.length &&
      (parts[index].added || parts[index].removed)
    ) {
      const changedPart = parts[index];
      const lines = splitLines(changedPart.value);
      if (changedPart.removed) removed.push(...lines);
      if (changedPart.added) added.push(...lines);
      index += 1;
    }
    const changedLineCount = Math.max(removed.length, added.length);
    for (let offset = 0; offset < changedLineCount; offset += 1) {
      const hasRemoved = offset < removed.length;
      const hasAdded = offset < added.length;
      rows.push({
        type:
          hasRemoved && hasAdded ? 'changed' : hasRemoved ? 'removed' : 'added',
        baseLine: hasRemoved ? baseLine++ : null,
        comparisonLine: hasAdded ? comparisonLine++ : null,
        baseText: hasRemoved ? removed[offset] : '',
        comparisonText: hasAdded ? added[offset] : '',
      });
    }
  }

  return collapseContext(rows);
}

function InlineDiff({ baseText, comparisonText, side, wordDiff }) {
  const text = side === 'base' ? baseText : comparisonText;
  if (wordDiff === null) return text || ' ';
  const visibleParts = wordDiff.filter((part) =>
    side === 'base' ? !part.added : !part.removed
  );
  if (visibleParts.length === 0) return ' ';
  return visibleParts.map((part, index) => {
    const highlighted = side === 'base' ? part.removed : part.added;
    return (
      <span
        key={`${index}-${part.value}`}
        className={
          highlighted
            ? side === 'base'
              ? 'bg-red-300'
              : 'bg-green-300'
            : undefined
        }
      >
        {part.value}
      </span>
    );
  });
}

function SplitYamlDiff({ elected, selected, onClose }) {
  const [yamlKind, setYamlKind] = useState('submitted');
  const yamlField =
    yamlKind === 'submitted'
      ? 'submitted_yaml_content'
      : 'compiled_yaml_content';
  const yamlAvailable = Boolean(elected[yamlField] && selected[yamlField]);
  const base = useMemo(
    () => (yamlAvailable ? formatYaml(elected[yamlField]) : ''),
    [elected, yamlAvailable, yamlField]
  );
  const comparison = useMemo(
    () => (yamlAvailable ? formatYaml(selected[yamlField]) : ''),
    [selected, yamlAvailable, yamlField]
  );
  const rows = useMemo(() => {
    return buildSplitDiffRows(base, comparison).map((row) => ({
      ...row,
      wordDiff:
        row.type === 'changed'
          ? diffWordsWithSpace(row.baseText, row.comparisonText)
          : null,
    }));
  }, [base, comparison]);
  const changedRows = rows.filter(
    (row) => row.type !== 'context' && row.type !== 'gap'
  );
  const additions = changedRows.filter(
    (row) => row.comparisonLine !== null
  ).length;
  const deletions = changedRows.filter((row) => row.baseLine !== null).length;

  return (
    <div className="border-t px-3 py-3">
      <div className="mb-2 flex items-center justify-between gap-3 text-sm">
        <div className="flex items-center gap-3">
          <div>
            <span className="font-medium">
              Changes from elected v{elected.version}
            </span>
            <span className="ml-2 text-gray-500">to v{selected.version}</span>
            {yamlAvailable && (
              <>
                <span className="ml-3 text-green-700">+{additions}</span>
                <span className="ml-2 text-red-700">-{deletions}</span>
              </>
            )}
          </div>
          <div className="flex rounded-md border p-0.5">
            {['submitted', 'compiled'].map((kind) => (
              <Button
                key={kind}
                variant={yamlKind === kind ? 'secondary' : 'ghost'}
                size="sm"
                className="h-6 px-2 text-xs capitalize"
                onClick={() => setYamlKind(kind)}
              >
                {kind}
              </Button>
            ))}
          </div>
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="h-7 px-2 text-xs"
          onClick={onClose}
        >
          Close
        </Button>
      </div>

      {!yamlAvailable ? (
        <div className="rounded-md border bg-gray-50 p-4 text-center text-sm text-gray-500">
          {yamlKind === 'submitted'
            ? 'Submitted YAML was not retained for one of these versions. Compare the compiled YAML instead.'
            : 'Compiled YAML is unavailable for one of these versions.'}
        </div>
      ) : changedRows.length === 0 ? (
        <div className="rounded-md border bg-gray-50 p-4 text-center text-sm text-gray-500">
          These versions have identical YAML.
        </div>
      ) : (
        <div className="max-h-[65vh] overflow-auto rounded-md border border-gray-300">
          <table className="w-full min-w-[900px] border-collapse font-mono text-xs leading-5">
            <thead className="sticky top-0 z-10 bg-gray-100 text-left font-sans text-gray-600">
              <tr>
                <th
                  colSpan={2}
                  className="w-1/2 border-r px-2 py-1.5 font-medium"
                >
                  Elected v{elected.version}
                </th>
                <th colSpan={2} className="w-1/2 px-2 py-1.5 font-medium">
                  Version {selected.version}
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => {
                if (row.type === 'gap') {
                  return (
                    <tr
                      key={`gap-${index}`}
                      className="bg-blue-50 text-blue-700"
                    >
                      <td colSpan={4} className="border-y px-3 py-1 font-sans">
                        {row.count} unchanged lines hidden
                      </td>
                    </tr>
                  );
                }
                const baseChanged =
                  row.type === 'removed' || row.type === 'changed';
                const comparisonChanged =
                  row.type === 'added' || row.type === 'changed';
                return (
                  <tr
                    key={`${row.baseLine}-${row.comparisonLine}-${index}`}
                    data-testid={`diff-${row.type}-row`}
                  >
                    <td
                      className={`w-10 select-none border-r px-2 text-right text-gray-400 ${baseChanged ? 'bg-red-100' : 'bg-gray-50'}`}
                    >
                      {row.baseLine ?? ''}
                    </td>
                    <td
                      className={`w-[calc(50%-2.5rem)] border-r px-2 whitespace-pre ${baseChanged ? 'bg-red-50' : ''}`}
                    >
                      <InlineDiff {...row} side="base" />
                    </td>
                    <td
                      className={`w-10 select-none border-r px-2 text-right text-gray-400 ${comparisonChanged ? 'bg-green-100' : 'bg-gray-50'}`}
                    >
                      {row.comparisonLine ?? ''}
                    </td>
                    <td
                      className={`w-[calc(50%-2.5rem)] px-2 whitespace-pre ${comparisonChanged ? 'bg-green-50' : ''}`}
                    >
                      <InlineDiff {...row} side="comparison" />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function VersionYamlViewer({ version, onClose }) {
  const [yamlKind, setYamlKind] = useState('submitted');
  const [isCopied, setIsCopied] = useState(false);
  const copyGeneration = useRef(0);
  const copyResetTimer = useRef(null);
  const yamlField =
    yamlKind === 'submitted'
      ? 'submitted_yaml_content'
      : 'compiled_yaml_content';
  const yamlContent = version[yamlField];
  const yamlAvailable = Boolean(
    typeof yamlContent === 'string' && yamlContent.trim()
  );
  const formattedYaml = useMemo(
    () => (yamlAvailable ? formatYaml(yamlContent) : ''),
    [yamlAvailable, yamlContent]
  );

  useEffect(() => {
    return () => {
      copyGeneration.current += 1;
      if (copyResetTimer.current !== null) {
        clearTimeout(copyResetTimer.current);
      }
    };
  }, []);

  const resetCopyStatus = () => {
    copyGeneration.current += 1;
    if (copyResetTimer.current !== null) {
      clearTimeout(copyResetTimer.current);
      copyResetTimer.current = null;
    }
    setIsCopied(false);
  };

  const selectYamlKind = (kind) => {
    resetCopyStatus();
    setYamlKind(kind);
  };

  const copyYaml = async () => {
    resetCopyStatus();
    const generation = copyGeneration.current;
    try {
      await navigator.clipboard.writeText(formattedYaml);
      if (generation !== copyGeneration.current) return;
      setIsCopied(true);
      copyResetTimer.current = setTimeout(() => {
        if (generation !== copyGeneration.current) return;
        setIsCopied(false);
        copyResetTimer.current = null;
      }, 2000);
    } catch (copyError) {
      if (generation !== copyGeneration.current) return;
      console.error('Failed to copy version YAML to clipboard:', copyError);
    }
  };

  return (
    <div className="border-t px-3 py-3">
      <div className="mb-2 flex items-center justify-between gap-3 text-sm">
        <div className="flex items-center gap-3">
          <span className="font-medium">Version {version.version} YAML</span>
          <div className="flex rounded-md border p-0.5">
            {['submitted', 'compiled'].map((kind) => (
              <Button
                key={kind}
                variant={yamlKind === kind ? 'secondary' : 'ghost'}
                size="sm"
                className="h-6 px-2 text-xs capitalize"
                onClick={() => selectYamlKind(kind)}
              >
                {kind}
              </Button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="outline"
            size="sm"
            className="h-7 px-2 text-xs"
            onClick={copyYaml}
            disabled={!yamlAvailable}
          >
            {isCopied ? 'Copied!' : 'Copy YAML'}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 px-2 text-xs"
            onClick={onClose}
          >
            Close
          </Button>
        </div>
      </div>

      {yamlAvailable ? (
        <YamlCodeBlock value={formattedYaml} maxHeight="65vh" readOnly />
      ) : (
        <div className="rounded-md border bg-gray-50 p-4 text-center text-sm text-gray-500">
          {yamlKind === 'submitted'
            ? 'Submitted YAML was not retained for this version.'
            : 'Compiled YAML is unavailable for this version.'}
        </div>
      )}
    </div>
  );
}

export function ServiceVersionHistory({ serviceName, onElectionComplete }) {
  const [isAdmin, setIsAdmin] = useState(false);
  const [loading, setLoading] = useState(true);
  const [history, setHistory] = useState(null);
  const [selectedVersion, setSelectedVersion] = useState(null);
  const [viewedVersion, setViewedVersion] = useState(null);
  const [electingVersion, setElectingVersion] = useState(null);
  const [error, setError] = useState(null);
  const loadGeneration = useRef(0);

  const loadHistory = useCallback(async () => {
    if (!serviceName) {
      setLoading(false);
      return;
    }
    const generation = ++loadGeneration.current;
    setLoading(true);
    try {
      const data = await getServiceVersions(serviceName);
      if (generation !== loadGeneration.current) return;
      setHistory(data);
      setError(null);
    } catch (loadError) {
      if (generation !== loadGeneration.current) return;
      setError(loadError.message);
    } finally {
      if (generation === loadGeneration.current) setLoading(false);
    }
  }, [serviceName]);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setHistory(null);
    setSelectedVersion(null);
    setViewedVersion(null);
    setError(null);
    (async () => {
      try {
        const user = await getCurrentUserRole();
        if (!mounted) return;
        if (user.role !== 'admin') {
          setIsAdmin(false);
          setLoading(false);
          return;
        }
        setIsAdmin(true);
        await loadHistory();
      } catch (roleError) {
        if (!mounted) return;
        setIsAdmin(false);
        setError(roleError.message);
        setLoading(false);
      }
    })();
    return () => {
      mounted = false;
      loadGeneration.current += 1;
    };
  }, [loadHistory]);

  const elected = useMemo(
    () => history?.versions?.find((version) => version.elected) || null,
    [history]
  );
  const selected = useMemo(
    () =>
      history?.versions?.find(
        (version) => version.version === selectedVersion
      ) || null,
    [history, selectedVersion]
  );
  const viewed = useMemo(
    () =>
      history?.versions?.find((version) => version.version === viewedVersion) ||
      null,
    [history, viewedVersion]
  );

  if (!isAdmin && !loading) {
    return (
      <Card className="p-6 text-center text-sm text-gray-500">
        {error || 'Version history is available to administrators.'}
      </Card>
    );
  }

  const elect = async (version) => {
    const confirmed = window.confirm(
      `Create a new rolling deployment generation from version ${version}?`
    );
    if (!confirmed) return;
    setElectingVersion(version);
    setError(null);
    try {
      await electServiceVersion(serviceName, version);
      await loadHistory();
      setSelectedVersion(null);
      setViewedVersion(null);
      if (onElectionComplete) await onElectionComplete();
    } catch (electionError) {
      setError(electionError.message);
    } finally {
      setElectingVersion(null);
    }
  };

  return (
    <Card>
      <div className="flex items-center justify-between border-b px-3 py-2">
        <div>
          <h3 className="text-base font-semibold">Version history</h3>
          <p className="text-xs text-gray-500">
            Elected {history?.elected_version ?? '-'} · Active{' '}
            {history?.active_versions?.join(', ') || '-'}
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          className="h-8"
          onClick={loadHistory}
          disabled={loading}
        >
          Refresh
        </Button>
      </div>

      {error && (
        <div className="mx-3 mt-3 rounded-md border border-red-200 bg-red-50 p-2 text-sm text-red-700">
          {error}
        </div>
      )}

      {loading && !history ? (
        <div className="flex items-center justify-center p-6 text-sm text-gray-500">
          <CircularProgress size={18} className="mr-2" /> Loading versions...
        </div>
      ) : (
        <div className="overflow-x-auto px-3 py-2">
          <Table className="text-sm">
            <TableHeader>
              <TableRow>
                <TableHead className="h-8 px-2">Version</TableHead>
                <TableHead className="h-8 px-2">Committed</TableHead>
                <TableHead className="h-8 px-2">Deployed by</TableHead>
                <TableHead className="h-8 px-2">Scaling</TableHead>
                <TableHead className="h-8 px-2">State</TableHead>
                <TableHead className="h-8 px-2 text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(history?.versions || []).map((version) => (
                <TableRow
                  key={version.version}
                  data-state={
                    selectedVersion === version.version ||
                    viewedVersion === version.version
                      ? 'selected'
                      : undefined
                  }
                >
                  <TableCell className="px-2 py-1.5 font-medium">
                    {version.version}
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-2 py-1.5">
                    {version.created_at ? (
                      <TimestampWithTooltip
                        date={new Date(version.created_at * 1000)}
                      />
                    ) : (
                      <span className="text-gray-400">Unknown</span>
                    )}
                  </TableCell>
                  <TableCell className="px-2 py-1.5">
                    {version.created_by || (
                      <span className="text-gray-400">Unknown</span>
                    )}
                  </TableCell>
                  <TableCell
                    className="max-w-72 truncate px-2 py-1.5 text-xs text-gray-600"
                    title={version.policy || undefined}
                  >
                    {version.policy || '-'}
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-2 py-1.5">
                    {version.elected && (
                      <span className="mr-1 rounded bg-blue-100 px-1.5 py-0.5 text-xs text-blue-800">
                        Elected
                      </span>
                    )}
                    {version.active && (
                      <span className="rounded bg-green-100 px-1.5 py-0.5 text-xs text-green-800">
                        Active
                      </span>
                    )}
                    {!version.elected && !version.active && '-'}
                  </TableCell>
                  <TableCell className="px-2 py-1.5">
                    <div className="flex justify-end gap-1.5">
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-7 px-2 text-xs"
                        aria-label={`View YAML for version ${version.version}`}
                        aria-pressed={viewedVersion === version.version}
                        onClick={() => {
                          setViewedVersion(version.version);
                          setSelectedVersion(null);
                        }}
                      >
                        View YAML
                      </Button>
                      {!version.elected && elected && (
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-7 px-2 text-xs"
                          onClick={() => {
                            setSelectedVersion(version.version);
                            setViewedVersion(null);
                          }}
                        >
                          Compare
                        </Button>
                      )}
                      {!version.elected && (
                        <Button
                          size="sm"
                          className="h-7 px-2 text-xs"
                          onClick={() => elect(version.version)}
                          disabled={electingVersion !== null}
                        >
                          {electingVersion === version.version
                            ? 'Electing...'
                            : 'Elect'}
                        </Button>
                      )}
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {viewed && (
        <VersionYamlViewer
          key={viewed.version}
          version={viewed}
          onClose={() => setViewedVersion(null)}
        />
      )}

      {selected && elected && (
        <SplitYamlDiff
          key={`${elected.version}-${selected.version}`}
          elected={elected}
          selected={selected}
          onClose={() => setSelectedVersion(null)}
        />
      )}
    </Card>
  );
}
