'use client';

import React, { useEffect, useMemo, useState } from 'react';

import {
  HISTORY_PRESETS,
  HistoryRangeToolbar,
  getEffectiveHistorySelection,
  resolveHistoryRange,
} from '@/components/serve-history-range';
import { DemandPressureCard } from '@/components/serve-demand-pressure';
import { AcceleratorHistoryCard } from '@/components/serve-accelerator-history';
import { ReplicaHistoryCard } from '@/components/serve-replica-history';
import { RequestHistoryCard } from '@/components/serve-request-history';
import { PredictionTimeHistoryCard } from '@/components/serve-prediction-time-history';

export function ServeHistorySection({
  history,
  loading = false,
  onHoursChange,
}) {
  const [selection, setSelection] = useState({
    kind: 'preset',
    seconds: HISTORY_PRESETS[0].seconds,
  });
  useEffect(() => {
    setSelection({
      kind: 'preset',
      seconds: HISTORY_PRESETS[0].seconds,
    });
  }, [history?.serviceHash]);
  const effectiveSelection = useMemo(
    () => getEffectiveHistorySelection(history, selection),
    [history, selection]
  );
  const range = useMemo(
    () => resolveHistoryRange(history, effectiveSelection),
    [history, effectiveSelection]
  );
  if (!history) {
    return loading ? (
      <div className="mb-4 rounded-lg border border-gray-200 bg-white px-4 py-8 text-center text-sm text-gray-500">
        Loading request and capacity history...
      </div>
    ) : null;
  }
  if (history.available === false) {
    return (
      <div className="mb-4 rounded-lg border border-gray-200 bg-gray-50 px-4 py-3 text-sm text-gray-600">
        Request and capacity history is temporarily unavailable. Other service
        data is still available. Refresh to retry.
      </div>
    );
  }
  if (!range) return null;

  const selectPreset = (seconds) => {
    setSelection({ kind: 'preset', seconds });
    onHoursChange?.(Math.ceil(seconds / (60 * 60)));
  };

  return (
    <>
      {history.refreshUnavailable && (
        <div className="mb-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-900">
          The latest history refresh failed. The last good range remains
          visible. Refresh to retry.
        </div>
      )}
      <HistoryRangeToolbar
        selection={effectiveSelection}
        range={range}
        onPreset={selectPreset}
      />
      <RequestHistoryCard
        history={history}
        range={range}
        onRangeSelect={({ start, end }) =>
          setSelection({ kind: 'custom', start, end })
        }
        loading={loading}
      />
      <PredictionTimeHistoryCard
        history={history}
        range={range}
        onRangeSelect={({ start, end }) =>
          setSelection({ kind: 'custom', start, end })
        }
        loading={loading}
      />
      <DemandPressureCard
        history={history}
        range={range}
        onRangeSelect={({ start, end }) =>
          setSelection({ kind: 'custom', start, end })
        }
        loading={loading}
      />
      <AcceleratorHistoryCard
        history={history}
        range={range}
        onRangeSelect={({ start, end }) =>
          setSelection({ kind: 'custom', start, end })
        }
        loading={loading}
      />
      <ReplicaHistoryCard
        history={history}
        range={range}
        onRangeSelect={({ start, end }) =>
          setSelection({ kind: 'custom', start, end })
        }
        loading={loading}
      />
    </>
  );
}
