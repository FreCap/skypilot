'use client';

import React, { useMemo } from 'react';

import { Card } from '@/components/ui/card';
import {
  SelectableHistoryLine,
  historyLinearScale,
} from '@/components/serve-history-range';

export function buildRequestHistoryView(history, range) {
  if (!history?.available || !range) {
    return {
      timestamps: [],
      counts: [],
      demandTargets: [],
      capacityTargets: [],
      readyCapacities: [],
      provisioningCapacities: [],
      totalCapacities: [],
      events: [],
      stats: null,
      capacityStats: null,
    };
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
  const autoscalerSamples = new Map(
    (history.autoscalerSamples || []).map((sample) => [
      sample.timestamp,
      sample,
    ])
  );
  const demandTargets = [];
  const capacityTargets = [];
  const readyCapacities = [];
  const provisioningCapacities = [];
  const totalCapacities = [];
  for (
    let timestamp = range.start;
    timestamp <= range.end;
    timestamp += bucketSeconds
  ) {
    timestamps.push(timestamp);
    counts.push(
      requestCounts.has(timestamp) ? requestCounts.get(timestamp) : null
    );
    const sample = autoscalerSamples.get(timestamp);
    demandTargets.push(sample?.demandTarget ?? null);
    capacityTargets.push(sample?.capacityTarget ?? null);
    readyCapacities.push(sample?.readyCapacity ?? null);
    provisioningCapacities.push(sample?.provisioningCapacity ?? null);
    totalCapacities.push(sample?.totalCapacity ?? null);
  }
  const observedCounts = counts.filter((count) => count !== null);
  const requestHistoryComplete =
    counts.length > 0 && counts.every((count) => count !== null);
  const total = observedCounts.reduce((sum, count) => sum + count, 0);
  const observedCapacity = timestamps
    .map((timestamp, index) => ({
      timestamp,
      demandTarget: demandTargets[index],
      capacityTarget: capacityTargets[index],
      readyCapacity: readyCapacities[index],
    }))
    .filter(
      (sample) =>
        sample.demandTarget !== null &&
        sample.capacityTarget !== null &&
        sample.readyCapacity !== null
    );
  let longestBelowTargetBuckets = 0;
  let currentBelowTargetBuckets = 0;
  capacityTargets.forEach((capacityTarget, index) => {
    const readyCapacity = readyCapacities[index];
    if (capacityTarget === null || readyCapacity === null) {
      currentBelowTargetBuckets = 0;
    } else if (readyCapacity < capacityTarget) {
      currentBelowTargetBuckets += 1;
      longestBelowTargetBuckets = Math.max(
        longestBelowTargetBuckets,
        currentBelowTargetBuckets
      );
    } else {
      currentBelowTargetBuckets = 0;
    }
  });
  const minuteMultiplier = bucketSeconds / 60;
  const capacityStats = observedCapacity.length
    ? {
        peakDemandTarget: Math.max(
          ...observedCapacity.map((sample) => sample.demandTarget)
        ),
        peakCapacityTarget: Math.max(
          ...observedCapacity.map((sample) => sample.capacityTarget)
        ),
        peakDeficit: Math.max(
          ...observedCapacity.map((sample) =>
            Math.max(0, sample.capacityTarget - sample.readyCapacity)
          )
        ),
        belowTargetMinutes:
          observedCapacity.filter(
            (sample) => sample.readyCapacity < sample.capacityTarget
          ).length * minuteMultiplier,
        longestBelowTargetMinutes: longestBelowTargetBuckets * minuteMultiplier,
      }
    : null;
  const orderedSamples = (history.autoscalerSamples || [])
    .filter(
      (sample) =>
        sample.timestamp >= range.start && sample.timestamp <= range.end
    )
    .sort((left, right) => left.timestamp - right.timestamp);
  const events = [];
  orderedSamples.forEach((sample, index) => {
    if (index === 0) return;
    const previous = orderedSamples[index - 1];
    const y = sample.capacityTarget ?? sample.readyCapacity ?? 0;
    if (
      sample.controllerSessionId &&
      previous.controllerSessionId &&
      sample.controllerSessionId !== previous.controllerSessionId
    ) {
      events.push({
        timestamp: sample.timestamp,
        y,
        kind: 'restart',
        label: 'Controller restarted',
      });
    }
    if (sample.version !== previous.version) {
      events.push({
        timestamp: sample.timestamp,
        y,
        kind: 'update',
        label: `Service updated to v${sample.version}`,
      });
    }
  });
  return {
    timestamps,
    counts,
    demandTargets,
    capacityTargets,
    readyCapacities,
    provisioningCapacities,
    totalCapacities,
    events,
    stats: {
      total: requestHistoryComplete ? total : null,
      averagePerMinute: requestHistoryComplete ? total / counts.length : null,
      peakPerMinute: requestHistoryComplete ? Math.max(...counts) : null,
    },
    capacityStats,
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
  const capacityData = (values) =>
    view.timestamps.map((timestamp, index) => ({
      x: timestamp,
      y: values[index],
    }));
  const capacityLine = (label, values, color, borderDash = []) => ({
    label,
    data: capacityData(values),
    parsing: false,
    yAxisID: 'yCapacity',
    borderColor: color,
    backgroundColor: color,
    fill: false,
    pointRadius: 0,
    borderWidth: 1.75,
    borderDash,
    stepped: 'middle',
    spanGaps: false,
    tension: 0,
  });
  const hasCapacity = view.demandTargets.some((value) => value !== null);
  const hasDistinctCapacityTarget = view.capacityTargets.some(
    (value, index) =>
      value !== null &&
      view.demandTargets[index] !== null &&
      value !== view.demandTargets[index]
  );
  const chartData = {
    datasets: [
      {
        label: 'Recorded request attempts',
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
        spanGaps: false,
        tension: 0,
      },
      ...(hasCapacity
        ? [
            capacityLine(
              'Traffic target (with hysteresis)',
              view.demandTargets,
              'rgb(234, 88, 12)',
              [7, 4]
            ),
            ...(hasDistinctCapacityTarget
              ? [
                  capacityLine(
                    'Traffic or reservation target',
                    view.capacityTargets,
                    'rgb(147, 51, 234)',
                    [3, 3]
                  ),
                ]
              : []),
            capacityLine(
              'Ready capacity',
              view.readyCapacities,
              'rgb(22, 163, 74)'
            ),
            capacityLine(
              'Committed / unready capacity',
              view.provisioningCapacities,
              'rgb(8, 145, 178)',
              [2, 3]
            ),
            capacityLine(
              'Non-failed tracked capacity',
              view.totalCapacities,
              'rgb(100, 116, 139)',
              [1, 3]
            ),
          ]
        : []),
      ...['restart', 'update']
        .filter((kind) => view.events.some((event) => event.kind === kind))
        .map((kind) => ({
          label: kind === 'restart' ? 'Controller restart' : 'Service update',
          data: view.events
            .filter((event) => event.kind === kind)
            .map((event) => ({
              x: event.timestamp,
              y: event.y,
              eventLabel: event.label,
            })),
          parsing: false,
          yAxisID: 'yCapacity',
          showLine: false,
          pointRadius: 5,
          pointHoverRadius: 7,
          pointStyle: kind === 'restart' ? 'triangle' : 'rectRot',
          borderColor:
            kind === 'restart' ? 'rgb(220, 38, 38)' : 'rgb(79, 70, 229)',
          backgroundColor:
            kind === 'restart' ? 'rgb(220, 38, 38)' : 'rgb(79, 70, 229)',
          clip: false,
        })),
    ],
  };
  const scales = historyLinearScale(range, {
    beginAtZero: true,
    ticks: { precision: 0 },
    title: { display: true, text: 'Recorded request attempts / minute' },
  });
  scales.yCapacity = {
    position: 'right',
    beginAtZero: true,
    ticks: { precision: 0 },
    grid: { drawOnChartArea: false },
    title: {
      display: hasCapacity,
      text:
        history.autoscalerSamples?.find((sample) => sample.replicaUnit)
          ?.replicaUnit === 'logical_slot'
          ? 'Capacity slots'
          : 'Tracked backend capacity',
    },
  };
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales,
    plugins: {
      legend: { display: hasCapacity, position: 'bottom' },
      tooltip: {
        callbacks: {
          label: (context) =>
            context.raw?.eventLabel ||
            `${context.dataset.label}: ${context.parsed.y}`,
        },
      },
    },
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h3 className="text-lg font-semibold">
            Recorded request and capacity history
          </h3>
          <div className="text-sm text-gray-500">
            Recorded attempts include admitted requests and explicit queue-full
            or queue-timeout rejections; attempts canceled or disconnected while
            awaiting admission are excluded. Missing minutes are unknown; an
            explicit zero means the active load balancer observed the complete
            minute. Traffic target includes autoscaler hysteresis. Traffic or
            reservation target is the larger of traffic and reserved-capacity
            fill. Committed / unready capacity combines queued,
            provider-launching, application-starting, and not-ready work.
            Non-failed tracked capacity also includes stopping and preempted
            rows until cleanup finishes
            {loading ? ' · Refreshing…' : ''}
          </div>
        </div>
      </div>
      <Card>
        {!view.timestamps.length ? (
          <div className="text-center py-12 text-gray-500">
            No recorded request history is available for this range.
          </div>
        ) : (
          <div className="p-4">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-4 text-sm">
              <div>
                <div className="text-gray-500">Recorded attempts in range</div>
                <div className="font-semibold">
                  {view.stats.total === null
                    ? 'N/A'
                    : view.stats.total.toLocaleString()}
                </div>
              </div>
              <div>
                <div className="text-gray-500">Average / minute</div>
                <div className="font-semibold">
                  {view.stats.averagePerMinute === null
                    ? 'N/A'
                    : view.stats.averagePerMinute.toLocaleString(undefined, {
                        maximumFractionDigits: 1,
                      })}
                </div>
              </div>
              <div>
                <div className="text-gray-500">Peak / minute</div>
                <div className="font-semibold">
                  {view.stats.peakPerMinute === null
                    ? 'N/A'
                    : view.stats.peakPerMinute.toLocaleString()}
                </div>
              </div>
            </div>
            {view.capacityStats && (
              <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4 text-sm">
                <div>
                  <div className="text-gray-500">
                    Peak traffic target (with hysteresis)
                  </div>
                  <div className="font-semibold">
                    {view.capacityStats.peakDemandTarget}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500">
                    Peak traffic or reservation target
                  </div>
                  <div className="font-semibold">
                    {view.capacityStats.peakCapacityTarget}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500">Peak target deficit</div>
                  <div className="font-semibold">
                    {view.capacityStats.peakDeficit}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500">Minutes below target</div>
                  <div className="font-semibold">
                    {view.capacityStats.belowTargetMinutes}
                  </div>
                </div>
                <div>
                  <div className="text-gray-500">Longest target gap</div>
                  <div className="font-semibold">
                    {view.capacityStats.longestBelowTargetMinutes} min
                  </div>
                </div>
              </div>
            )}
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
