'use client';

import React, { useMemo, useState } from 'react';

import { Card } from '@/components/ui/card';
import {
  SelectableHistoryLine,
  historyLinearScale,
} from '@/components/serve-history-range';

const SERVING_SERIES = [
  ['Demand target by card', 'demandTarget', 'rgb(234, 88, 12)', [7, 4]],
  ['Warm retention', 'warmRetentionTarget', 'rgb(168, 85, 247)', [4, 3]],
  ['Cold-launch authority', 'coldLaunchAuthority', 'rgb(220, 38, 38)', [2, 2]],
  ['Ready capacity', 'readyCapacity', 'rgb(22, 163, 74)', []],
  [
    'Committed / unready capacity',
    'provisioningCapacity',
    'rgb(8, 145, 178)',
    [2, 3],
  ],
  [
    'Non-failed tracked capacity',
    'totalCapacity',
    'rgb(100, 116, 139)',
    [1, 3],
  ],
];

const RESERVED_SERIES = [
  ['Reserved-fill target', 'fillTarget', 'rgb(147, 51, 234)', [3, 3]],
  ['Zero-cost ready capacity', 'zeroCostReadyCapacity', 'rgb(22, 163, 74)', []],
  ['Free reserved slots', 'freeReservedSlots', 'rgb(2, 132, 199)', [5, 3]],
  ['Hard serving floor', 'minReplicas', 'rgb(220, 38, 38)', [2, 2]],
];

export function buildAcceleratorHistoryView(history, range) {
  if (!history?.available || !range) return null;
  const samples = (history.autoscalerSamples || []).filter(
    (sample) => sample.acceleratorBreakdown
  );
  if (!samples.length) return null;
  const cards = [];
  const seen = new Set();
  samples.forEach((sample) => {
    sample.acceleratorBreakdown.configuredAccelerators.forEach((card) => {
      if (!seen.has(card)) {
        seen.add(card);
        cards.push(card);
      }
    });
  });
  const byTimestamp = new Map(
    samples.map((sample) => [sample.timestamp, sample])
  );
  const bucketSeconds = history.bucketSeconds || 60;
  const timestamps = [];
  for (
    let timestamp = range.start;
    timestamp <= range.end;
    timestamp += bucketSeconds
  ) {
    timestamps.push(timestamp);
  }
  const hasLegacyCommittedCapacityGaps = samples.some(
    (sample) =>
      sample.timestamp >= range.start &&
      sample.timestamp <= range.end &&
      sample.acceleratorBreakdown.capacitySemanticsVersion !== 2
  );
  return {
    cards,
    timestamps,
    hasLegacyCommittedCapacityGaps,
    valuesByCard: Object.fromEntries(
      cards.map((card) => [
        card,
        Object.fromEntries(
          [
            ...SERVING_SERIES.map((series) => series[1]),
            ...RESERVED_SERIES.map((series) => series[1]),
          ].map((field) => [
            field,
            timestamps.map((timestamp) => {
              const breakdown =
                byTimestamp.get(timestamp)?.acceleratorBreakdown;
              if (!breakdown?.configuredAccelerators.includes(card)) {
                return null;
              }
              if (
                field === 'provisioningCapacity' &&
                breakdown.capacitySemanticsVersion !== 2
              ) {
                return null;
              }
              return breakdown[field]?.[card] ?? null;
            }),
          ])
        ),
      ])
    ),
  };
}

export function AcceleratorHistoryCard({
  history,
  range,
  onRangeSelect,
  loading = false,
}) {
  const [viewKind, setViewKind] = useState('serving');
  const view = useMemo(
    () => buildAcceleratorHistoryView(history, range),
    [history, range]
  );
  if (!view) return null;
  const series = viewKind === 'serving' ? SERVING_SERIES : RESERVED_SERIES;
  const unit =
    history.autoscalerSamples?.find(
      (sample) => sample.acceleratorBreakdown && sample.replicaUnit
    )?.replicaUnit === 'logical_slot'
      ? 'Tracked capacity slots'
      : 'Tracked backend capacity';

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2 gap-4">
        <div>
          <h3 className="text-lg font-semibold">Exact accelerator history</h3>
          <div className="text-sm text-gray-500">
            Demand target assigns flexible work to the cheapest compatible card.
            Warm retention shows work staying on its current card. Cold launch
            authority is the incremental shortage allowed to request new
            capacity. Committed / unready capacity is the controller-reported
            non-ready work already assigned to that card.
            {view.hasLegacyCommittedCapacityGaps &&
              ' Older samples appear as gaps in that series because they predate capacity semantics v2.'}
            {loading ? ' · Refreshing…' : ''}
          </div>
        </div>
        <div className="inline-flex rounded-md border border-gray-200 p-1">
          {[
            ['serving', 'Serving capacity'],
            ['reserved', 'Reserved capacity'],
          ].map(([kind, label]) => (
            <button
              key={kind}
              type="button"
              aria-pressed={viewKind === kind}
              className={`rounded px-3 py-1 text-sm ${
                viewKind === kind ? 'bg-sky-100 text-sky-800' : 'text-gray-600'
              }`}
              onClick={() => setViewKind(kind)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        {view.cards.map((card) => {
          const values = view.valuesByCard[card];
          const data = {
            datasets: series.map(([label, field, color, borderDash]) => ({
              label,
              data: view.timestamps.map((timestamp, index) => ({
                x: timestamp,
                y: values[field][index],
              })),
              parsing: false,
              borderColor: color,
              backgroundColor: color,
              borderDash,
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
              title: { display: true, text: unit },
            }),
            plugins: { legend: { display: true, position: 'bottom' } },
          };
          return (
            <Card key={card}>
              <div className="p-4">
                <h4 className="font-semibold mb-2">{card}</h4>
                <SelectableHistoryLine
                  data={data}
                  options={options}
                  range={range}
                  bucketSeconds={history.bucketSeconds || 60}
                  onRangeSelect={onRangeSelect}
                  ariaLabel={`${card} accelerator history chart`}
                />
              </div>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
