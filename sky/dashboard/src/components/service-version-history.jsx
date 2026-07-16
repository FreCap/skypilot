'use client';

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { CircularProgress } from '@mui/material';

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
import { YamlCodeBlock } from '@/components/ui/yaml-code-block';
import { getCurrentUserRole } from '@/data/connectors/client';
import {
  electServiceVersion,
  getServiceVersions,
} from '@/data/connectors/services';
import { formatYaml } from '@/lib/yamlUtils';

export function ServiceVersionHistory({ serviceName, onElectionComplete }) {
  const [isAdmin, setIsAdmin] = useState(false);
  const [loading, setLoading] = useState(true);
  const [history, setHistory] = useState(null);
  const [selectedVersion, setSelectedVersion] = useState(null);
  const [electingVersion, setElectingVersion] = useState(null);
  const [error, setError] = useState(null);

  const loadHistory = useCallback(async () => {
    if (!serviceName) return;
    setLoading(true);
    try {
      const data = await getServiceVersions(serviceName);
      setHistory(data);
      setError(null);
    } catch (loadError) {
      setError(loadError.message);
    } finally {
      setLoading(false);
    }
  }, [serviceName]);

  useEffect(() => {
    let mounted = true;
    (async () => {
      const user = await getCurrentUserRole();
      if (!mounted) return;
      if (user.role !== 'admin') {
        setLoading(false);
        return;
      }
      setIsAdmin(true);
      await loadHistory();
    })();
    return () => {
      mounted = false;
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

  if (!isAdmin && !loading) return null;

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
      if (onElectionComplete) await onElectionComplete();
    } catch (electionError) {
      setError(electionError.message);
    } finally {
      setElectingVersion(null);
    }
  };

  return (
    <Card className="mb-6">
      <div className="flex items-center justify-between border-b px-4 py-3">
        <div>
          <h3 className="text-lg font-semibold">Service versions</h3>
          <p className="text-sm text-gray-500">
            Elected generation: {history?.elected_version ?? '-'} · Active:{' '}
            {history?.active_versions?.join(', ') || '-'}
          </p>
        </div>
        <Button variant="outline" onClick={loadHistory} disabled={loading}>
          Refresh
        </Button>
      </div>

      {error && (
        <div className="mx-4 mt-4 rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {loading && !history ? (
        <div className="flex items-center justify-center p-8 text-sm text-gray-500">
          <CircularProgress size={18} className="mr-2" /> Loading versions...
        </div>
      ) : (
        <div className="overflow-x-auto p-4">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Version</TableHead>
                <TableHead>State</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(history?.versions || []).map((version) => (
                <TableRow key={version.version}>
                  <TableCell className="font-medium">
                    {version.version}
                  </TableCell>
                  <TableCell>
                    {version.elected && (
                      <span className="mr-2 rounded bg-blue-100 px-2 py-1 text-xs text-blue-800">
                        Elected
                      </span>
                    )}
                    {version.active && (
                      <span className="rounded bg-green-100 px-2 py-1 text-xs text-green-800">
                        Active
                      </span>
                    )}
                    {!version.elected && !version.active && '-'}
                  </TableCell>
                  <TableCell>
                    <div className="flex justify-end gap-2">
                      {!version.elected && (
                        <Button
                          variant="outline"
                          onClick={() => setSelectedVersion(version.version)}
                        >
                          Compare
                        </Button>
                      )}
                      {!version.elected && (
                        <Button
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

      {selected && elected && (
        <div className="border-t p-4">
          <h4 className="mb-3 font-medium">
            Version {selected.version} compared with elected version{' '}
            {elected.version}
          </h4>
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <div>
              <div className="mb-2 text-sm font-medium text-gray-600">
                Version {selected.version}
              </div>
              <YamlCodeBlock
                value={formatYaml(selected.yaml_content)}
                readOnly
                height="420px"
              />
            </div>
            <div>
              <div className="mb-2 text-sm font-medium text-gray-600">
                Elected version {elected.version}
              </div>
              <YamlCodeBlock
                value={formatYaml(elected.yaml_content)}
                readOnly
                height="420px"
              />
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}
