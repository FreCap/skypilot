'use client';

import React, { useMemo } from 'react';
import { CircularProgress } from '@mui/material';
import {
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
} from 'chart.js';
import { Line } from 'react-chartjs-2';

import { Card } from '@/components/ui/card';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend
);

const SERIES = [
  {
    key: 'readyCount',
    label: 'Ready',
    borderColor: 'rgb(22, 163, 74)',
    backgroundColor: 'rgba(34, 197, 94, 0.42)',
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

function emptyCounts() {
  return Object.fromEntries(SERIES.map(({ key }) => [key, 0]));
}

export function buildReplicaHistoryView(history) {
  const samples = history?.available ? history.samples || [] : [];
  if (!samples.length) {
    return {
      timestamps: [],
      versionBreakdowns: [],
      datasets: SERIES.map((series) => ({ ...series, data: [] })),
      stats: null,
    };
  }

  const bucketSeconds = history.bucketSeconds || 60;
  const firstTimestamp = Math.min(...samples.map((sample) => sample.timestamp));
  const lastTimestamp = Math.max(...samples.map((sample) => sample.timestamp));
  const windowStart = Number.isFinite(history.windowStart)
    ? Math.ceil(history.windowStart / bucketSeconds) * bucketSeconds
    : firstTimestamp;
  const windowEnd = Number.isFinite(history.windowEnd)
    ? Math.floor(history.windowEnd / bucketSeconds) * bucketSeconds
    : lastTimestamp;
  const aggregates = new Map();
  const versions = new Map();
  samples.forEach((sample) => {
    const timestamp = sample.timestamp;
    const aggregate = aggregates.get(timestamp) || emptyCounts();
    SERIES.forEach(({ key }) => {
      aggregate[key] += sample[key] || 0;
    });
    aggregates.set(timestamp, aggregate);
    const versionRows = versions.get(timestamp) || [];
    versionRows.push(sample);
    versions.set(timestamp, versionRows);
  });

  const timestamps = [];
  const versionBreakdowns = [];
  const observed = [];
  for (
    let timestamp = windowStart;
    timestamp <= windowEnd;
    timestamp += bucketSeconds
  ) {
    timestamps.push(timestamp);
    const aggregate = aggregates.get(timestamp) || null;
    observed.push(aggregate);
    versionBreakdowns.push(
      (versions.get(timestamp) || []).sort(
        (left, right) => left.version - right.version
      )
    );
  }

  const observedCounts = observed.filter(Boolean);
  const latest = observedCounts.at(-1);
  const readyValues = observedCounts.map((counts) => counts.readyCount);
  const machineMinuteMultiplier = bucketSeconds / 60;
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
        erroredMachineMinutes: observedCounts.reduce(
          (sum, counts) => sum + counts.erroredCount * machineMinuteMultiplier,
          0
        ),
        stoppingMachineMinutes: observedCounts.reduce(
          (sum, counts) => sum + counts.stoppingCount * machineMinuteMultiplier,
          0
        ),
      }
    : null;
  return {
    timestamps,
    versionBreakdowns,
    datasets: SERIES.map((series) => ({
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

function timestampLabel(timestamp) {
  return new Date(timestamp * 1000).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function versionFooter(versionRows) {
  return versionRows.map(
    (row) =>
      `v${row.version}: ${row.readyCount} ready, ${row.provisioningCount} provisioning, ${row.erroredCount} errored, ${row.stoppingCount} stopping`
  );
}

export function ReplicaHistoryCard({ history, loading = false }) {
  const view = useMemo(() => buildReplicaHistoryView(history), [history]);
  if (!history) return null;
  if (history?.available === false) return null;

  const chartData = {
    labels: view.timestamps.map(timestampLabel),
    datasets: view.datasets,
  };
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: { stacked: true, ticks: { maxTicksLimit: 12 } },
      y: {
        stacked: true,
        beginAtZero: true,
        ticks: { precision: 0 },
        title: { display: true, text: 'Physical machines' },
      },
    },
    plugins: {
      legend: { position: 'bottom' },
      tooltip: {
        callbacks: {
          footer: (items) => {
            const index = items?.[0]?.dataIndex;
            return Number.isInteger(index)
              ? versionFooter(view.versionBreakdowns[index])
              : [];
          },
        },
      },
    },
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h3 className="text-lg font-semibold">Machine history</h3>
          <div className="text-sm text-gray-500">
            Physical replica status over the last 12 hours
          </div>
        </div>
        {loading && <CircularProgress size={16} />}
      </div>
      <Card>
        {!view.timestamps.length ? (
          <div className="text-center py-12 text-gray-500">
            No history yet. The first sample appears within one minute.
          </div>
        ) : (
          <div className="p-4">
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4 text-sm">
              <div>
                <div className="text-gray-500">Current ready</div>
                <div className="font-semibold">
                  {view.stats.current.readyCount}
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
                <div className="text-gray-500">Error / stopping minutes</div>
                <div className="font-semibold">
                  {view.stats.erroredMachineMinutes} /{' '}
                  {view.stats.stoppingMachineMinutes}
                </div>
              </div>
            </div>
            <div className="h-80">
              <Line data={chartData} options={options} />
            </div>
          </div>
        )}
      </Card>
    </div>
  );
}
