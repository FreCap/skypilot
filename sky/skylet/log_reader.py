"""Low-level readers and marker policy for SkyPilot log files."""

from collections.abc import Iterable
import os
from typing import TextIO

# Peek the head of the lines to check if we need to start streaming when
# tail > 0.
PEEK_HEAD_LINES_FOR_START_STREAM = 20

# Block size for the backward-seek tail reader. ~64 KB is large enough to fit
# the tail of typical log files in a single read while keeping memory bounded
# for very long lines.
_TAIL_BLOCK_SIZE = 64 * 1024


def tail_lines_from_end(path: str,
                        tail: int,
                        offset: int = 0) -> tuple[list[str], int]:
    """Return the last ``tail`` lines from ``path``, skipping ``offset``.

    Reads backwards in fixed-size blocks from EOF so cost is O(tail *
    line-length) rather than O(file-size). For multi-GB log files this
    is the difference between ~10 s and ~1 ms per call.

    Args:
        path: File path to read.
        tail: Number of lines to return (must be > 0).
        offset: Number of lines from EOF to skip before taking ``tail``.

    Returns:
        ``(lines, end_pos)`` — lines (each with trailing newline if
        present in source) and the byte position at file EOF when the
        scan started. Callers that follow the file should seek to
        ``end_pos`` to avoid re-emitting bytes that were already
        returned. If ``offset`` is at or past the start of the file,
        returns ``([], end_pos)``.
    """
    assert tail > 0
    needed = tail + max(offset, 0)
    chunks: list[bytes] = []
    line_count = 0
    pos = 0
    end_pos = 0
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        end_pos = f.tell()
        pos = end_pos
        while pos > 0 and line_count <= needed:
            read_size = min(_TAIL_BLOCK_SIZE, pos)
            pos -= read_size
            f.seek(pos)
            chunk = f.read(read_size)
            chunks.append(chunk)
            line_count += chunk.count(b'\n')
    data = b''.join(reversed(chunks))
    text = data.decode('utf-8', errors='replace')
    lines = text.splitlines(keepends=True)
    # If we stopped before reaching offset 0, the first decoded line is
    # almost certainly partial (we landed mid-line). Drop it so callers see
    # only complete lines.
    if pos > 0 and lines:
        lines = lines[1:]
    if offset > 0:
        if offset >= len(lines):
            return [], end_pos
        # pylint: disable=invalid-unary-operand-type
        lines = lines[:-offset]
    return lines[-tail:], end_pos


def peek_head_lines(log_file: TextIO) -> list[str]:
    """Peek the head of the file."""
    lines = [
        log_file.readline() for _ in range(PEEK_HEAD_LINES_FOR_START_STREAM)
    ]
    # Reset the file pointer to the beginning
    log_file.seek(0, os.SEEK_SET)
    return [line for line in lines if line]


def should_stream_the_whole_tail_lines(head_lines_of_log_file: list[str],
                                       tail_lines: Iterable[str],
                                       start_stream_at: str) -> bool:
    """Check if the entire tail lines should be streamed."""
    # See comment:
    # https://github.com/skypilot-org/skypilot/pull/4241#discussion_r1833611567
    # for more details.
    # Case 1: If start_stream_at is found at the head of the tail lines,
    # we should not stream the whole tail lines.
    for index, line in enumerate(tail_lines):
        if index >= PEEK_HEAD_LINES_FOR_START_STREAM:
            break
        if start_stream_at in line:
            return False
    # Case 2: If start_stream_at is found at the head of log file, but not at
    # the tail lines, we need to stream the whole tail lines.
    for line in head_lines_of_log_file:
        if start_stream_at in line:
            return True
    # Case 3: If start_stream_at is not at the head, and not found at the tail
    # lines, we should not stream the whole tail lines.
    return False
