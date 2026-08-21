'use client';

import React, { useMemo, useState } from 'react';
import { CircularProgress } from '@mui/material';
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LinearScale,
  Tooltip,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';

import { Card } from '@/components/ui/card';
import {
  SelectableHistoryLine,
  historyLinearScale,
} from '@/components/serve-history-range';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

const OUTCOME_OPTIONS = ['all', 'succeeded', 'failed'];

function addCounts(target, source) {
  source.forEach((count, index) => {
    target[index] += count;
  });
}

function sampleCounts(sample, outcome, bucketCount) {
  const counts = Array(bucketCount).fill(0);
  if (outcome === 'all') {
    Object.values(sample.outcomeCounts || {}).forEach((values) =>
      addCounts(counts, values)
    );
  } else if (sample.outcomeCounts?.[outcome]) {
    addCounts(counts, sample.outcomeCounts[outcome]);
  }
  return counts;
}

function histogramQuantile(counts, upperBounds, quantile) {
  const total = counts.reduce((sum, count) => sum + count, 0);
  if (!total) return null;
  const rank = Math.ceil(total * quantile);
  let cumulative = 0;
  for (let index = 0; index < counts.length; index += 1) {
    cumulative += counts[index];
    if (cumulative >= rank) {
      return {
        value: upperBounds[Math.min(index, upperBounds.length - 1)],
        overflow: index >= upperBounds.length,
      };
    }
  }
  return null;
}

export function formatPredictionTime(seconds) {
  if (!Number.isFinite(seconds)) return 'N/A';
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 60) return `${Number(seconds.toFixed(1))} s`;
  return `${Number((seconds / 60).toFixed(1))} min`;
}

export function buildPredictionTimeHistoryView(
  history,
  range,
  outcome = 'all'
) {
  const upperBounds = history?.predictionTimeBucketUpperBoundsSeconds || [];
  if (
    !history?.available ||
    !range ||
    history.predictionTimeHistogramVersion !== 1 ||
    !upperBounds.length ||
    !OUTCOME_OPTIONS.includes(outcome)
  ) {
    return { supported: false, samples: 0 };
  }
  const bucketCount = upperBounds.length + 1;
  const bucketSeconds = history.bucketSeconds || 60;
  const selectedSamples = (history.predictionTimeSamples || []).filter(
    (sample) => sample.timestamp >= range.start && sample.timestamp <= range.end
  );
  const samplesByTimestamp = new Map(
    selectedSamples.map((sample) => [sample.timestamp, sample])
  );
  const timestamps = [];
  const p50 = [];
  const p95 = [];
  const p99 = [];
  const p50Overflow = [];
  const p95Overflow = [];
  const p99Overflow = [];
  const aggregateCounts = Array(bucketCount).fill(0);
  for (
    let timestamp = range.start;
    timestamp <= range.end;
    timestamp += bucketSeconds
  ) {
    timestamps.push(timestamp);
    const sample = samplesByTimestamp.get(timestamp);
    const counts = sample
      ? sampleCounts(sample, outcome, bucketCount)
      : Array(bucketCount).fill(0);
    addCounts(aggregateCounts, counts);
    const estimates = [0.5, 0.95, 0.99].map((quantile) =>
      histogramQuantile(counts, upperBounds, quantile)
    );
    p50.push(estimates[0]?.value ?? null);
    p95.push(estimates[1]?.value ?? null);
    p99.push(estimates[2]?.value ?? null);
    p50Overflow.push(estimates[0]?.overflow === true);
    p95Overflow.push(estimates[1]?.overflow === true);
    p99Overflow.push(estimates[2]?.overflow === true);
  }
  const samples = aggregateCounts.reduce((sum, count) => sum + count, 0);
  const selectedEstimates = [0.5, 0.95, 0.99].map((quantile) =>
    histogramQuantile(aggregateCounts, upperBounds, quantile)
  );
  return {
    supported: true,
    timestamps,
    p50,
    p95,
    p99,
    p50Overflow,
    p95Overflow,
    p99Overflow,
    aggregateCounts,
    upperBounds,
    samples,
    selectedP50: selectedEstimates[0]?.value ?? null,
    selectedP95: selectedEstimates[1]?.value ?? null,
    selectedP99: selectedEstimates[2]?.value ?? null,
    selectedP50Overflow: selectedEstimates[0]?.overflow === true,
    selectedP95Overflow: selectedEstimates[1]?.overflow === true,
    selectedP99Overflow: selectedEstimates[2]?.overflow === true,
  };
}

function bucketLabel(upperBounds, index) {
  if (index >= upperBounds.length) {
    return `> ${formatPredictionTime(upperBounds[upperBounds.length - 1])}`;
  }
  return `<= ${formatPredictionTime(upperBounds[index])}`;
}

export function PredictionTimeHistoryCard({
  history,
  range,
  onRangeSelect,
  loading = false,
}) {
  const [outcome, setOutcome] = useState('all');
  const view = useMemo(
    () => buildPredictionTimeHistoryView(history, range, outcome),
    [history, range, outcome]
  );
  if (!view.supported) return null;

  const trendData = {
    datasets: [
      ['p50', view.p50, view.p50Overflow, 'rgb(2, 132, 199)'],
      ['p95', view.p95, view.p95Overflow, 'rgb(234, 88, 12)'],
      ['p99', view.p99, view.p99Overflow, 'rgb(190, 24, 93)'],
    ].map(([label, values, overflows, color]) => ({
      label,
      data: view.timestamps.map((timestamp, index) => ({
        x: timestamp,
        y: values[index],
        overflow: overflows[index],
      })),
      parsing: false,
      borderColor: color,
      backgroundColor: color,
      fill: false,
      pointRadius: 0,
      borderWidth: 1.75,
      spanGaps: false,
      tension: 0,
    })),
  };
  const trendOptions = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: historyLinearScale(range, {
      beginAtZero: true,
      title: { display: true, text: 'Approximate prediction time (seconds)' },
    }),
    plugins: {
      legend: { position: 'bottom' },
      tooltip: {
        callbacks: {
          label: (context) =>
            `${context.dataset.label}: ${
              context.raw?.overflow ? '> ' : ''
            }${formatPredictionTime(context.raw?.y)}`,
        },
      },
    },
  };
  const histogramData = {
    labels: view.aggregateCounts.map((_, index) =>
      bucketLabel(view.upperBounds, index)
    ),
    datasets: [
      {
        label: 'Recorded terminal prediction observations',
        data: view.aggregateCounts,
        backgroundColor: 'rgba(2, 132, 199, 0.65)',
      },
    ],
  };
  const histogramOptions = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      x: {
        ticks: { maxRotation: 65, minRotation: 45 },
        title: { display: true, text: 'Prediction-time bucket' },
      },
      y: {
        beginAtZero: true,
        ticks: { precision: 0 },
        title: {
          display: true,
          text: 'Recorded terminal prediction observations',
        },
      },
    },
    plugins: { legend: { display: false } },
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between gap-3 mb-2">
        <div>
          <h3 className="text-lg font-semibold">Prediction time</h3>
          <div className="text-sm text-gray-500">
            Replica execution time, excluding SkyServe queueing and retries.
            Async jobs use model-reported processing time.
          </div>
        </div>
        {loading && <CircularProgress size={16} />}
      </div>
      <Card>
        <div className="p-4">
          <div className="flex flex-wrap gap-2 mb-4">
            {OUTCOME_OPTIONS.map((option) => (
              <button
                key={option}
                type="button"
                aria-pressed={outcome === option}
                onClick={() => setOutcome(option)}
                className={`px-3 py-1 rounded border text-sm ${
                  outcome === option
                    ? 'bg-sky-600 text-white border-sky-600'
                    : 'bg-white text-gray-700 border-gray-300'
                }`}
              >
                {option === 'all'
                  ? 'All'
                  : option.charAt(0).toUpperCase() + option.slice(1)}
              </button>
            ))}
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4 text-sm">
            <div>
              <div className="text-gray-500">
                Recorded terminal observations in range
              </div>
              <div className="font-semibold">{view.samples}</div>
            </div>
            {[
              ['Approx. p50', view.selectedP50, view.selectedP50Overflow],
              ['Approx. p95', view.selectedP95, view.selectedP95Overflow],
              ['Approx. p99', view.selectedP99, view.selectedP99Overflow],
            ].map(([label, value, overflow]) => (
              <div key={label}>
                <div className="text-gray-500">{label}</div>
                <div className="font-semibold">
                  {overflow ? '> ' : ''}
                  {formatPredictionTime(value)}
                </div>
              </div>
            ))}
          </div>
          {view.samples === 0 ? (
            <div className="py-8 text-sm text-gray-500 text-center">
              No terminal prediction observations were recorded in the selected
              range.
            </div>
          ) : (
            <>
              <SelectableHistoryLine
                data={trendData}
                options={trendOptions}
                range={range}
                bucketSeconds={history.bucketSeconds || 60}
                onRangeSelect={onRangeSelect}
                ariaLabel="Prediction time trend chart"
              />
              <div className="h-72 mt-6" aria-label="Prediction time histogram">
                <Bar data={histogramData} options={histogramOptions} />
              </div>
            </>
          )}
        </div>
      </Card>
    </div>
  );
}
