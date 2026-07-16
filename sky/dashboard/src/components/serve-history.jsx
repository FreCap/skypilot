'use client';

import React, { useMemo, useState } from 'react';

import {
  HISTORY_PRESETS,
  HistoryRangeToolbar,
  getEffectiveHistorySelection,
  resolveHistoryRange,
} from '@/components/serve-history-range';
import { ReplicaHistoryCard } from '@/components/serve-replica-history';
import { RequestHistoryCard } from '@/components/serve-request-history';

export function ServeHistorySection({ history, loading = false }) {
  const [selection, setSelection] = useState({
    kind: 'preset',
    seconds: HISTORY_PRESETS[0].seconds,
  });
  const effectiveSelection = useMemo(
    () => getEffectiveHistorySelection(history, selection),
    [history, selection]
  );
  const range = useMemo(
    () => resolveHistoryRange(history, effectiveSelection),
    [history, effectiveSelection]
  );
  if (!history || history.available === false || !range) return null;

  return (
    <>
      <HistoryRangeToolbar
        selection={effectiveSelection}
        range={range}
        onPreset={(seconds) => setSelection({ kind: 'preset', seconds })}
      />
      <RequestHistoryCard
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
