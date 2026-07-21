'use client';

import React, { useMemo, useState } from 'react';
import { CircularProgress } from '@mui/material';

import { Card } from '@/components/ui/card';
import {
  SelectableHistoryLine,
  historyLinearScale,
} from '@/components/serve-history-range';

const SERIES = [
  {
    key: 'readyOrdinaryCount',
    label: 'Ready',
    borderColor: 'rgb(22, 163, 74)',
    backgroundColor: 'rgba(34, 197, 94, 0.42)',
  },
  {
    key: 'readyReservedCount',
    label: 'Ready free reserved',
    borderColor: 'rgb(13, 148, 136)',
    backgroundColor: 'rgba(20, 184, 166, 0.45)',
  },
  {
    key: 'provisioningCount',
    label: 'Provisioning',
    borderColor: 'rgb(2, 132, 199)',
    backgroundColor: 'rgba(14, 165, 233, 0.38)',
  },
  {
    key: 'notReadyCount',
    label: 'Not ready',
    borderColor: 'rgb(202, 138, 4)',
    backgroundColor: 'rgba(234, 179, 8, 0.38)',
  },
  {
    key: 'erroredCount',
    label: 'Errored',
    borderColor: 'rgb(220, 38, 38)',
    backgroundColor: 'rgba(239, 68, 68, 0.4)',
  },
  {
    key: 'preemptedCount',
    label: 'Preempted',
    borderColor: 'rgb(126, 34, 206)',
    backgroundColor: 'rgba(168, 85, 247, 0.36)',
  },
  {
    key: 'stoppingCount',
    label: 'Stopping',
    borderColor: 'rgb(234, 88, 12)',
    backgroundColor: 'rgba(249, 115, 22, 0.36)',
  },
];

const MODE_CONFIG = {
  logical: {
    label: 'Logical',
    singular: 'logical slot',
    plural: 'logical slots',
    axisTitle: 'Logical slots',
    fields: {
      readyCount: 'logicalReadyCount',
      readyReservedCount: 'logicalReadyReservedCount',
      provisioningCount: 'logicalProvisioningCount',
      notReadyCount: 'logicalNotReadyCount',
      erroredCount: 'logicalErroredCount',
      preemptedCount: 'logicalPreemptedCount',
      stoppingCount: 'logicalStoppingCount',
      totalCount: 'logicalTotalCount',
    },
  },
  physical: {
    label: 'Physical',
    singular: 'physical backend',
    plural: 'physical backends',
    axisTitle: 'Physical backends',
    fields: {
      readyCount: 'readyCount',
      readyReservedCount: 'readyReservedCount',
      provisioningCount: 'provisioningCount',
      notReadyCount: 'notReadyCount',
      erroredCount: 'erroredCount',
      preemptedCount: 'preemptedCount',
      stoppingCount: 'stoppingCount',
      totalCount: 'totalCount',
    },
  },
};

const COUNT_KEYS = [
  'readyCount',
  'provisioningCount',
  'notReadyCount',
  'erroredCount',
  'preemptedCount',
  'stoppingCount',
  'totalCount',
];

function validCount(value) {
  return Number.isInteger(value) && value >= 0;
}

function sampleCounts(sample, mode) {
  const fields = MODE_CONFIG[mode].fields;
  const counts = {};
  for (const key of COUNT_KEYS) {
    const value = sample[fields[key]];
    if (!validCount(value)) return null;
    counts[key] = value;
  }
  const reserved = sample[fields.readyReservedCount];
  counts.readyReservedCount = validCount(reserved) ? reserved : null;
  return counts;
}

function aggregateRows(rows, mode) {
  const selected = rows.map((row) => sampleCounts(row, mode));
  if (selected.some((counts) => counts === null)) return null;

  const counts = Object.fromEntries(COUNT_KEYS.map((key) => [key, 0]));
  selected.forEach((row) => {
    COUNT_KEYS.forEach((key) => {
      counts[key] += row[key];
    });
  });
  const reservedKnown = selected.every(
    (row) => row.readyReservedCount !== null
  );
  counts.readyReservedCount = reservedKnown
    ? selected.reduce((sum, row) => sum + row.readyReservedCount, 0)
    : null;
  counts.readyOrdinaryCount =
    counts.readyCount - (counts.readyReservedCount || 0);
  return counts;
}

export function buildReplicaHistoryView(
  history,
  range = null,
  mode = 'logical'
) {
  const samples = history?.available ? history.samples || [] : [];
  const config = MODE_CONFIG[mode] || MODE_CONFIG.logical;
  if (!samples.length || !range) {
    return {
      mode,
      config,
      timestamps: [],
      versionBreakdowns: [],
      datasets: SERIES.map((series) => ({ ...series, data: [] })),
      stats: null,
    };
  }

  const bucketSeconds = history.bucketSeconds || 60;
  const versions = new Map();
  samples.forEach((sample) => {
    const timestamp = sample.timestamp;
    if (timestamp < range.start || timestamp > range.end) return;
    const versionRows = versions.get(timestamp) || [];
    versionRows.push(sample);
    versions.set(timestamp, versionRows);
  });

  const timestamps = [];
  const versionBreakdowns = [];
  const observed = [];
  for (
    let timestamp = range.start;
    timestamp <= range.end;
    timestamp += bucketSeconds
  ) {
    timestamps.push(timestamp);
    const versionRows = (versions.get(timestamp) || []).sort(
      (left, right) => left.version - right.version
    );
    versionBreakdowns.push(versionRows);
    observed.push(versionRows.length ? aggregateRows(versionRows, mode) : null);
  }

  const observedCounts = observed.filter(Boolean);
  const latest = observedCounts.at(-1);
  const readyValues = observedCounts.map((counts) => counts.readyCount);
  const minuteMultiplier = bucketSeconds / 60;
  const stats = latest
    ? {
        current: latest,
        averageReady:
          readyValues.reduce((sum, value) => sum + value, 0) /
          readyValues.length,
        minimumReady: Math.min(...readyValues),
        peakErrored: Math.max(
          ...observedCounts.map((counts) => counts.erroredCount)
        ),
        erroredMinutes: observedCounts.reduce(
          (sum, counts) => sum + counts.erroredCount * minuteMultiplier,
          0
        ),
        stoppingMinutes: observedCounts.reduce(
          (sum, counts) => sum + counts.stoppingCount * minuteMultiplier,
          0
        ),
      }
    : null;
  const visibleSeries = SERIES.filter(
    ({ key }) =>
      key !== 'readyReservedCount' ||
      observedCounts.some((counts) => counts.readyReservedCount !== null)
  );
  return {
    mode,
    config,
    timestamps,
    versionBreakdowns,
    datasets: visibleSeries.map((series) => ({
      ...series,
      data: observed.map((counts) => (counts ? counts[series.key] : null)),
      fill: true,
      pointRadius: 0,
      borderWidth: 1.5,
      tension: 0,
      spanGaps: false,
    })),
    stats,
  };
}

function versionFooter(versionRows, mode) {
  return versionRows.map((row) => {
    const counts = sampleCounts(row, mode);
    if (!counts)
      return `v${row.version}: ${MODE_CONFIG[mode].plural} unavailable`;
    const reserved =
      counts.readyReservedCount === null
        ? ''
        : ` (${counts.readyReservedCount} free reserved)`;
    return `v${row.version}: ${counts.readyCount} ready${reserved}, ${counts.provisioningCount} provisioning, ${counts.erroredCount} errored, ${counts.stoppingCount} stopping`;
  });
}

function ModeToggle({ mode, onChange }) {
  return (
    <div className="inline-flex rounded-md border border-gray-200 p-0.5">
      {Object.entries(MODE_CONFIG).map(([key, config]) => {
        const active = key === mode;
        return (
          <button
            key={key}
            type="button"
            onClick={() => onChange(key)}
            aria-pressed={active}
            className={`rounded px-2.5 py-1 text-xs font-medium ${
              active
                ? 'bg-sky-50 text-sky-700'
                : 'text-gray-600 hover:bg-gray-50'
            }`}
          >
            {config.label}
          </button>
        );
      })}
    </div>
  );
}

export function ReplicaHistoryCard({
  history,
  range,
  onRangeSelect,
  loading = false,
}) {
  const [mode, setMode] = useState('logical');
  const view = useMemo(
    () => buildReplicaHistoryView(history, range, mode),
    [history, range, mode]
  );
  if (!history) return null;
  if (history?.available === false) return null;

  const chartData = {
    datasets: view.datasets.map((dataset) => ({
      ...dataset,
      parsing: false,
      data: view.timestamps.map((timestamp, index) => ({
        x: timestamp,
        y: dataset.data[index],
      })),
    })),
  };
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: historyLinearScale(range, {
      stacked: true,
      beginAtZero: true,
      ticks: { precision: 0 },
      title: { display: true, text: view.config.axisTitle },
    }),
    plugins: {
      legend: { position: 'bottom' },
      tooltip: {
        callbacks: {
          footer: (items) => {
            const index = items?.[0]?.dataIndex;
            return Number.isInteger(index)
              ? versionFooter(view.versionBreakdowns[index], mode)
              : [];
          },
        },
      },
    },
  };
  options.scales.x.stacked = true;
  options.scales.y = {
    ...options.scales.y,
    stacked: true,
  };

  return (
    <div className="mb-6">
      <div className="flex flex-wrap items-center justify-between gap-2 mb-2">
        <div>
          <h3 className="text-lg font-semibold">Replica history</h3>
          <div className="text-sm text-gray-500">
            Status by {view.config.singular} in the selected range
          </div>
        </div>
        <div className="flex items-center gap-2">
          <ModeToggle mode={mode} onChange={setMode} />
          {loading && <CircularProgress size={16} />}
        </div>
      </div>
      <Card>
        {!view.timestamps.length ? (
          <div className="text-center py-12 text-gray-500">
            No history yet. The first sample appears within one minute.
          </div>
        ) : (
          <div className="p-4">
            {view.stats ? (
              <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-4 text-sm">
                <div>
                  <div className="text-gray-500">Latest ready</div>
                  <div className="font-semibold">
                    {view.stats.current.readyCount}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500">Latest free reserved</div>
                  <div className="font-semibold">
                    {view.stats.current.readyReservedCount ?? 'Unavailable'}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500">Average ready</div>
                  <div className="font-semibold">
                    {view.stats.averageReady.toFixed(1)}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500">Minimum ready</div>
                  <div className="font-semibold">{view.stats.minimumReady}</div>
                </div>
                <div>
                  <div className="text-gray-500">Peak errored</div>
                  <div className="font-semibold">{view.stats.peakErrored}</div>
                </div>
                <div>
                  <div className="text-gray-500">
                    Error / stopping {view.config.singular}-minutes
                  </div>
                  <div className="font-semibold">
                    {view.stats.erroredMinutes} / {view.stats.stoppingMinutes}
                  </div>
                </div>
              </div>
            ) : (
              <div className="mb-4 text-sm text-gray-500">
                No {view.config.plural} samples in the selected range.
              </div>
            )}
            <SelectableHistoryLine
              data={chartData}
              options={options}
              range={range}
              bucketSeconds={history.bucketSeconds || 60}
              onRangeSelect={onRangeSelect}
              ariaLabel="Machine history chart"
            />
          </div>
        )}
      </Card>
    </div>
  );
}
