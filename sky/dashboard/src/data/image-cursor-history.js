export const IMAGE_CURSOR_HISTORY_LIMIT = 20;

export function firstImageCursorHistory() {
  return [{ cursor: null, page: 1 }];
}

export function currentImageCursorEntry(history) {
  return history[history.length - 1];
}

export function advanceImageCursorHistory(history, nextCursor) {
  if (!nextCursor) return history;
  const current = currentImageCursorEntry(history);
  return [...history, { cursor: nextCursor, page: current.page + 1 }].slice(
    -IMAGE_CURSOR_HISTORY_LIMIT
  );
}

export function retreatImageCursorHistory(history) {
  if (history.length <= 1) return history;
  return history.slice(0, -1);
}
