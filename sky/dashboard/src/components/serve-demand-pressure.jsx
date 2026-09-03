'use client';

import React, { useMemo } from 'react';
import { CircularProgress } from '@mui/material';

import { Card } from '@/components/ui/card';
import {
  SelectableHistoryLine,
  historyLinearScale,
} from '@/components/serve-history-range';

function peak(values) {
  const observed = values.filter((value) => value !== null);
  return observed.length ? Math.max(...observed) : null;
}

export function buildDemandPressureView(history, range) {
  if (!history?.available || !range) {
    return {
      supported: false,
      timestamps: [],
      inFlight: [],
      queued: [],
      rejected: [],
      stats: null,
    };
  }
  const bucketSeconds = history.bucketSeconds || 60;
  const autoscalerSamples = new Map(
    (history.autoscalerSamples || []).map((sample) => [
      sample.timestamp,
      sample,
    ])
  );
  const requestSamples = new Map(
    (history.requestSamples || []).map((sample) => [sample.timestamp, sample])
  );
  const gaugeSupported = (history.autoscalerSamples || []).some(
    (sample) => sample.peakInFlight !== null || sample.peakQueueDepth !== null
  );
  const rejectionSupported = history.rejectionHistoryAvailable === true;
  const supported = gaugeSupported || rejectionSupported;
  if (!supported) {
    return {
      supported,
      timestamps: [],
      inFlight: [],
      queued: [],
      rejected: [],
      stats: null,
    };
  }

  const timestamps = [];
  const inFlight = [];
  const queued = [];
  const rejected = [];
  for (
    let timestamp = range.start;
    timestamp <= range.end;
    timestamp += bucketSeconds
  ) {
    timestamps.push(timestamp);
    const autoscalerSample = autoscalerSamples.get(timestamp);
    const requestSample = requestSamples.get(timestamp);
    inFlight.push(autoscalerSample?.peakInFlight ?? null);
    queued.push(autoscalerSample?.peakQueueDepth ?? null);
    // Missing rows remain unknown. New active load balancers publish explicit
    // zero rows only after observing a complete minute.
    rejected.push(
      rejectionSupported ? (requestSample?.rejectedCount ?? null) : null
    );
  }
  const rejectionHistoryComplete =
    rejectionSupported &&
    rejected.length > 0 &&
    rejected.every((value) => value !== null);
  const rejectedObserved = rejected.filter((value) => value !== null);
  return {
    supported,
    timestamps,
    inFlight,
    queued,
    rejected,
    stats: {
      peakInFlight: peak(inFlight),
      peakQueued: peak(queued),
      totalRejected: rejectionHistoryComplete
        ? rejectedObserved.reduce((sum, value) => sum + value, 0)
        : null,
      peakRejected: peak(rejected),
    },
  };
}

export function DemandPressureCard({
  history,
  range,
  onRangeSelect,
  loading = false,
}) {
  const view = useMemo(
    () => buildDemandPressureView(history, range),
    [history, range]
  );
  if (!view.supported) return null;

  const series = [
    {
      label: 'Peak in flight',
      values: view.inFlight,
      color: 'rgb(14, 116, 144)',
    },
    {
      label: 'Peak queued',
      values: view.queued,
      color: 'rgb(202, 138, 4)',
    },
    {
      label: 'Rejected',
      values: view.rejected,
      color: 'rgb(220, 38, 38)',
    },
  ].filter(({ values }) => values.some((value) => value !== null));
  const chartData = {
    datasets: series.map(({ label, values, color }) => ({
      label,
      data: view.timestamps.map((timestamp, index) => ({
        x: timestamp,
        y: values[index],
      })),
      parsing: false,
      borderColor: color,
      backgroundColor: color,
      fill: false,
      pointRadius: 0,
      borderWidth: 1.75,
      stepped: 'middle',
      spanGaps: false,
      tension: 0,
    })),
  };
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: historyLinearScale(range, {
      beginAtZero: true,
      ticks: { precision: 0 },
      title: { display: true, text: 'Requests' },
    }),
    plugins: { legend: { position: 'bottom' } },
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h3 className="text-lg font-semibold">Demand pressure</h3>
          <div className="text-sm text-gray-500">
            Peak concurrent demand and recorded load-balancer rejections; gaps
            mean coverage is unknown
          </div>
        </div>
        {loading && <CircularProgress size={16} />}
      </div>
      <Card>
        <div className="p-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4 text-sm">
            <div>
              <div className="text-gray-500">Peak in flight</div>
              <div className="font-semibold">
                {view.stats.peakInFlight ?? 'N/A'}
              </div>
            </div>
            <div>
              <div className="text-gray-500">Peak queued</div>
              <div className="font-semibold">
                {view.stats.peakQueued ?? 'N/A'}
              </div>
            </div>
            <div>
              <div className="text-gray-500">Rejected in range</div>
              <div className="font-semibold">
                {view.stats.totalRejected ?? 'N/A'}
              </div>
            </div>
            <div>
              <div className="text-gray-500">Peak rejected / minute</div>
              <div className="font-semibold">
                {view.stats.peakRejected ?? 'N/A'}
              </div>
            </div>
          </div>
          <SelectableHistoryLine
            data={chartData}
            options={options}
            range={range}
            bucketSeconds={history.bucketSeconds || 60}
            onRangeSelect={onRangeSelect}
            ariaLabel="Demand pressure chart"
          />
        </div>
      </Card>
    </div>
  );
}
