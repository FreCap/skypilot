'use client';

import React, { useMemo, useRef, useState } from 'react';
import {
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend
);

export const HISTORY_PRESETS = [
  { label: '1h', seconds: 60 * 60 },
  { label: '12h', seconds: 12 * 60 * 60 },
  { label: '24h', seconds: 24 * 60 * 60 },
];

const DEFAULT_HISTORY_SELECTION = {
  kind: 'preset',
  seconds: HISTORY_PRESETS[0].seconds,
};

export function getHistoryBounds(history) {
  if (!history?.available) return null;
  const bucketSeconds = history.bucketSeconds || 60;
  const timestamps = [
    ...(history.samples || []).map((sample) => sample.timestamp),
    ...(history.requestSamples || []).map((sample) => sample.timestamp),
    ...(history.responseTimeSamples || []).map((sample) => sample.timestamp),
    ...(history.autoscalerSamples || []).map((sample) => sample.timestamp),
  ].filter(Number.isFinite);
  const fallbackStart = timestamps.length ? Math.min(...timestamps) : null;
  const fallbackEnd = timestamps.length ? Math.max(...timestamps) : null;
  const rawStart = Number.isFinite(history.windowStart)
    ? history.windowStart
    : fallbackStart;
  const rawEnd = Number.isFinite(history.windowEnd)
    ? history.windowEnd
    : fallbackEnd;
  if (!Number.isFinite(rawStart) || !Number.isFinite(rawEnd)) return null;
  const start = Math.ceil(rawStart / bucketSeconds) * bucketSeconds;
  const end = Math.floor(rawEnd / bucketSeconds) * bucketSeconds;
  if (end < start) return null;
  return { start, end, bucketSeconds };
}

export function getEffectiveHistorySelection(history, selection) {
  if (selection?.kind !== 'custom') {
    return selection?.kind === 'preset' ? selection : DEFAULT_HISTORY_SELECTION;
  }
  const bounds = getHistoryBounds(history);
  if (!bounds) return DEFAULT_HISTORY_SELECTION;
  const start = Math.max(bounds.start, Number(selection.start));
  const end = Math.min(bounds.end, Number(selection.end));
  return Number.isFinite(start) && Number.isFinite(end) && end > start
    ? selection
    : DEFAULT_HISTORY_SELECTION;
}

export function resolveHistoryRange(history, selection) {
  const bounds = getHistoryBounds(history);
  if (!bounds) return null;
  const effectiveSelection = getEffectiveHistorySelection(history, selection);
  if (effectiveSelection.kind === 'custom') {
    const start = Math.max(bounds.start, Number(effectiveSelection.start));
    const end = Math.min(bounds.end, Number(effectiveSelection.end));
    if (Number.isFinite(start) && Number.isFinite(end) && end > start) {
      return { start, end };
    }
  }
  const presetSeconds =
    effectiveSelection.kind === 'preset' &&
    Number(effectiveSelection.seconds) > 0
      ? Number(effectiveSelection.seconds)
      : HISTORY_PRESETS[0].seconds;
  return {
    start: Math.max(
      bounds.start,
      bounds.end - presetSeconds + bounds.bucketSeconds
    ),
    end: bounds.end,
  };
}

export function normalizeDraggedRange(start, end, bounds, bucketSeconds = 60) {
  if (
    !Number.isFinite(start) ||
    !Number.isFinite(end) ||
    !bounds ||
    !Number.isFinite(bounds.start) ||
    !Number.isFinite(bounds.end)
  ) {
    return null;
  }
  let normalizedStart =
    Math.floor(Math.min(start, end) / bucketSeconds) * bucketSeconds;
  let normalizedEnd =
    Math.ceil(Math.max(start, end) / bucketSeconds) * bucketSeconds;
  normalizedStart = Math.max(bounds.start, normalizedStart);
  normalizedEnd = Math.min(bounds.end, normalizedEnd);
  if (normalizedEnd - normalizedStart < bucketSeconds) {
    if (normalizedStart + bucketSeconds <= bounds.end) {
      normalizedEnd = normalizedStart + bucketSeconds;
    } else if (normalizedEnd - bucketSeconds >= bounds.start) {
      normalizedStart = normalizedEnd - bucketSeconds;
    } else {
      return null;
    }
  }
  return { start: normalizedStart, end: normalizedEnd };
}

export function timestampLabel(timestamp) {
  return new Date(timestamp * 1000).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

export function historyLinearScale(range, yOptions) {
  return {
    x: {
      type: 'linear',
      min: range?.start,
      max: range?.end,
      ticks: {
        maxTicksLimit: 10,
        callback: (value) => timestampLabel(Number(value)),
      },
    },
    y: yOptions,
  };
}

export function HistoryRangeToolbar({ selection, range, onPreset }) {
  const custom = selection?.kind === 'custom';
  return (
    <div className="flex flex-wrap items-center justify-end gap-2 mb-3">
      <span className="text-xs text-gray-500 mr-1">
        Drag any chart to select a shared range
      </span>
      {HISTORY_PRESETS.map((preset) => {
        const active =
          selection?.kind === 'preset' && selection.seconds === preset.seconds;
        return (
          <button
            key={preset.seconds}
            type="button"
            onClick={() => onPreset(preset.seconds)}
            aria-pressed={active}
            className={`rounded-md border px-2.5 py-1 text-xs font-medium ${
              active
                ? 'border-sky-500 bg-sky-50 text-sky-700'
                : 'border-gray-200 text-gray-600 hover:bg-gray-50'
            }`}
          >
            {preset.label}
          </button>
        );
      })}
      {custom && range && (
        <span className="rounded-md bg-gray-100 px-2.5 py-1 text-xs text-gray-600">
          {timestampLabel(range.start)} to {timestampLabel(range.end)}
        </span>
      )}
    </div>
  );
}

function chartPosition(chart, wrapper, clientX) {
  if (!chart?.canvas || !chart?.chartArea || !chart?.scales?.x) return null;
  const canvasRect = chart.canvas.getBoundingClientRect();
  const wrapperRect = wrapper.getBoundingClientRect();
  const canvasPixel = clientX - canvasRect.left;
  const clampedPixel = Math.min(
    chart.chartArea.right,
    Math.max(chart.chartArea.left, canvasPixel)
  );
  return {
    timestamp: chart.scales.x.getValueForPixel(clampedPixel),
    wrapperPixel: canvasRect.left - wrapperRect.left + clampedPixel,
    top: canvasRect.top - wrapperRect.top + chart.chartArea.top,
    height: chart.chartArea.bottom - chart.chartArea.top,
  };
}

function releasePointerCapture(target, pointerId) {
  if (
    typeof target.hasPointerCapture === 'function' &&
    !target.hasPointerCapture(pointerId)
  ) {
    return;
  }
  target.releasePointerCapture?.(pointerId);
}

export function SelectableHistoryLine({
  data,
  options,
  range,
  bucketSeconds,
  onRangeSelect,
  ariaLabel,
}) {
  const chartRef = useRef(null);
  const [drag, setDrag] = useState(null);
  const mergedOptions = useMemo(
    () => ({
      ...options,
      plugins: {
        ...options.plugins,
        tooltip: {
          ...options.plugins?.tooltip,
          callbacks: {
            ...options.plugins?.tooltip?.callbacks,
            title: (items) => {
              const timestamp = items?.[0]?.parsed?.x;
              return Number.isFinite(timestamp)
                ? timestampLabel(timestamp)
                : '';
            },
          },
        },
      },
    }),
    [options]
  );

  const handlePointerDown = (event) => {
    if (event.button !== 0 || !range || !onRangeSelect) return;
    const position = chartPosition(
      chartRef.current,
      event.currentTarget,
      event.clientX
    );
    if (!position) return;
    event.currentTarget.setPointerCapture?.(event.pointerId);
    setDrag({
      pointerId: event.pointerId,
      startTimestamp: position.timestamp,
      currentTimestamp: position.timestamp,
      startPixel: position.wrapperPixel,
      currentPixel: position.wrapperPixel,
      top: position.top,
      height: position.height,
    });
  };

  const handlePointerMove = (event) => {
    if (!drag || drag.pointerId !== event.pointerId) return;
    const position = chartPosition(
      chartRef.current,
      event.currentTarget,
      event.clientX
    );
    if (!position) return;
    setDrag((current) => ({
      ...current,
      currentTimestamp: position.timestamp,
      currentPixel: position.wrapperPixel,
    }));
  };

  const finishDrag = (event) => {
    if (!drag || drag.pointerId !== event.pointerId) return;
    const position = chartPosition(
      chartRef.current,
      event.currentTarget,
      event.clientX
    );
    const endTimestamp = position?.timestamp ?? drag.currentTimestamp;
    const endPixel = position?.wrapperPixel ?? drag.currentPixel;
    const pixelDistance = Math.abs(endPixel - drag.startPixel);
    if (pixelDistance >= 6) {
      const selected = normalizeDraggedRange(
        drag.startTimestamp,
        endTimestamp,
        range,
        bucketSeconds
      );
      if (selected) onRangeSelect(selected);
    }
    releasePointerCapture(event.currentTarget, event.pointerId);
    setDrag(null);
  };

  const cancelDrag = (event) => {
    if (!drag || drag.pointerId !== event.pointerId) return;
    releasePointerCapture(event.currentTarget, event.pointerId);
    setDrag(null);
  };

  return (
    <div
      className="relative h-80 select-none"
      style={{ touchAction: 'pan-y' }}
      onPointerDown={handlePointerDown}
      onPointerMove={handlePointerMove}
      onPointerUp={finishDrag}
      onPointerCancel={cancelDrag}
      role="img"
      aria-label={ariaLabel}
    >
      <Line ref={chartRef} data={data} options={mergedOptions} />
      {drag && (
        <div
          className="pointer-events-none absolute border border-sky-500 bg-sky-400/20"
          style={{
            left: Math.min(drag.startPixel, drag.currentPixel),
            width: Math.abs(drag.currentPixel - drag.startPixel),
            top: drag.top,
            height: drag.height,
          }}
        />
      )}
    </div>
  );
}
