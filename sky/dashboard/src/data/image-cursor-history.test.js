import {
  advanceImageCursorHistory,
  currentImageCursorEntry,
  firstImageCursorHistory,
  IMAGE_CURSOR_HISTORY_LIMIT,
  retreatImageCursorHistory,
} from '@/data/image-cursor-history';

describe('managed image cursor history', () => {
  it('retains a fixed window while preserving the absolute page', () => {
    let history = firstImageCursorHistory();
    for (let page = 2; page <= 1000; page += 1) {
      history = advanceImageCursorHistory(history, `cursor-${page}`);
    }

    expect(history).toHaveLength(IMAGE_CURSOR_HISTORY_LIMIT);
    expect(history[0]).toEqual({ cursor: 'cursor-981', page: 981 });
    expect(currentImageCursorEntry(history)).toEqual({
      cursor: 'cursor-1000',
      page: 1000,
    });
  });

  it('retreats within the retained window and resets with a fresh first page', () => {
    let history = firstImageCursorHistory();
    history = advanceImageCursorHistory(history, 'cursor-2');
    history = advanceImageCursorHistory(history, 'cursor-3');

    history = retreatImageCursorHistory(history);
    expect(currentImageCursorEntry(history)).toEqual({
      cursor: 'cursor-2',
      page: 2,
    });
    expect(currentImageCursorEntry(firstImageCursorHistory())).toEqual({
      cursor: null,
      page: 1,
    });
  });
});
