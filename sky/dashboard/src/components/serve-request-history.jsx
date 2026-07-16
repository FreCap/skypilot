'use client';

import React, { useMemo } from 'react';

import { Card } from '@/components/ui/card';
import {
  SelectableHistoryLine,
  historyLinearScale,
} from '@/components/serve-history-range';

export function buildRequestHistoryView(history, range) {
  if (!history?.available || !range) {
    return { timestamps: [], counts: [], stats: null };
  }
  const bucketSeconds = history.bucketSeconds || 60;
  const requestCounts = new Map(
    (history.requestSamples || []).map((sample) => [
      sample.timestamp,
      sample.requestCount,
    ])
  );
  const timestamps = [];
  const counts = [];
  for (
    let timestamp = range.start;
    timestamp <= range.end;
    timestamp += bucketSeconds
  ) {
    timestamps.push(timestamp);
    counts.push(requestCounts.get(timestamp) || 0);
  }
  const total = counts.reduce((sum, count) => sum + count, 0);
  return {
    timestamps,
    counts,
    stats: {
      total,
      averagePerMinute: counts.length ? total / counts.length : 0,
      peakPerMinute: counts.length ? Math.max(...counts) : 0,
    },
  };
}

export function RequestHistoryCard({
  history,
  range,
  onRangeSelect,
  loading = false,
}) {
  const view = useMemo(
    () => buildRequestHistoryView(history, range),
    [history, range]
  );
  if (!history || history.available === false) return null;
  const chartData = {
    datasets: [
      {
        label: 'Requests',
        data: view.timestamps.map((timestamp, index) => ({
          x: timestamp,
          y: view.counts[index],
        })),
        parsing: false,
        borderColor: 'rgb(2, 132, 199)',
        backgroundColor: 'rgba(14, 165, 233, 0.18)',
        fill: true,
        pointRadius: 0,
        borderWidth: 1.75,
        stepped: 'middle',
        tension: 0,
      },
    ],
  };
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: historyLinearScale(range, {
      beginAtZero: true,
      ticks: { precision: 0 },
      title: { display: true, text: 'Recorded requests / minute' },
    }),
    plugins: { legend: { display: false } },
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h3 className="text-lg font-semibold">Request history</h3>
          <div className="text-sm text-gray-500">
            Recorded requests per minute
            {loading ? ' · Refreshing…' : ''}
          </div>
        </div>
      </div>
      <Card>
        {!view.timestamps.length ? (
          <div className="text-center py-12 text-gray-500">
            No request history is available for this range.
          </div>
        ) : (
          <div className="p-4">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-4 text-sm">
              <div>
                <div className="text-gray-500">Requests in range</div>
                <div className="font-semibold">
                  {view.stats.total.toLocaleString()}
                </div>
              </div>
              <div>
                <div className="text-gray-500">Average / minute</div>
                <div className="font-semibold">
                  {view.stats.averagePerMinute.toLocaleString(undefined, {
                    maximumFractionDigits: 1,
                  })}
                </div>
              </div>
              <div>
                <div className="text-gray-500">Peak / minute</div>
                <div className="font-semibold">
                  {view.stats.peakPerMinute.toLocaleString()}
                </div>
              </div>
            </div>
            <SelectableHistoryLine
              data={chartData}
              options={options}
              range={range}
              bucketSeconds={history.bucketSeconds || 60}
              onRangeSelect={onRangeSelect}
              ariaLabel="Request history chart"
            />
          </div>
        )}
      </Card>
    </div>
  );
}
