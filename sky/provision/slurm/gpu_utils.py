"""Slurm GPU identifier translation helpers."""

import re

from sky.utils import gpu_names

# Regex pattern for parsing GPU GRES strings.
# Format: 'gpu[:acc_type]:acc_count(optional_extra_info)'
# Examples: 'gpu:8', 'gpu:H100:8', 'gpu:nvidia_h100_80gb_hbm3:8(S:0-1)'
_GRES_GPU_PATTERN = re.compile(r'\bgpu:(?:(?P<type>[^:(]+):)?(?P<count>\d+)',
                               re.IGNORECASE)

# Vendor prefixes stripped during normalization for matching purposes.
_GPU_VENDOR_PREFIXES = ('nvidia', 'amd', 'intel', 'tesla')


def get_gpu_type_and_count(gres_str: str) -> tuple[str | None, int]:
    """Parses GPU type and count from a GRES string.

    Returns:
        A tuple of (GPU type, GPU count). If no GPU is found, returns (None, 0).
    """
    match = _GRES_GPU_PATTERN.search(gres_str)
    if not match:
        return None, 0
    return match.group('type'), int(match.group('count'))


def _normalize_gpu_name(name: str) -> str:
    """Normalize a GPU name for fuzzy comparison.

    Strips vendor prefixes, normalizes separators, and lowercases. Used only
    for matching, never for submission.

    Examples:
        'nvidia_h100_80gb_hbm3' -> 'h100-80gb-hbm3'
        'H100'                -> 'h100'
        'A100-SXM-80GB'       -> 'a100-sxm-80gb'
    """
    result = name.lower().replace('_', '-')
    for prefix in _GPU_VENDOR_PREFIXES:
        if result.startswith(prefix + '-'):
            result = result[len(prefix) + 1:]
            break
    return result


def _is_segment_subsequence(segments_a: list[str],
                            segments_b: list[str]) -> bool:
    """Check if segments_a appears as an ordered subsequence of segments_b.

    Each segment must match exactly (preventing e.g. 'l4' matching 'l40').

    Examples:
        (['h100'], ['h100', '80gb', 's'])          -> True
        (['a100', '80gb'], ['a100', 'sxm4', '80gb']) -> True
        (['v100', '32gb'], ['v100', 'pcie', '16gb']) -> False
        (['l4'], ['l40'])                           -> False
    """
    # The iterator is stateful: once an element is consumed it won't be
    # revisited, so matches are always found in left-to-right order.
    b_iter = iter(segments_b)
    for seg in segments_a:
        # Scan forward through b_iter for a matching segment.
        for b_seg in b_iter:
            if seg == b_seg:
                break
        else:
            # for...else: runs when b_iter is exhausted without finding
            # seg, meaning segments_a is not a subsequence.
            return False
    return True


def _accelerator_name_matches_slurm(requested_acc: str,
                                    candidate_raw: str) -> bool:
    """Check if a requested accelerator name matches a Slurm GRES raw type.

    Matching rules (checked in order):
    1. Case-insensitive exact match of raw strings.
    2. Normalized forms are equal (vendor-prefix stripped, separators unified).
    3. Segment subsequence: the shorter name's dash-segments appear in order
       within the longer name's segments. Handles both prefix cases
       (H100 ~ H100-80GB-S) and non-contiguous memory variants
       (A100-80GB ~ A100-SXM4-80GB, V100-32GB ~ V100-PCIE-32GB).
       Exact segment matching prevents false positives (L4 ≠ L40).

    Args:
        requested_acc: The accelerator name requested by the user
            (e.g. 'H100', 'A100-80GB').
        candidate_raw: A raw GRES GPU type string from Slurm node metadata
            (e.g. 'NVIDIA_H100_80GB_HBM3').

    Returns:
        True if the names are considered matching.
    """
    # 1. Exact case-insensitive match.
    if requested_acc.lower() == candidate_raw.lower():
        return True

    # 2. Normalized equality.
    req_norm = _normalize_gpu_name(requested_acc)
    cand_norm = _normalize_gpu_name(candidate_raw)
    if req_norm == cand_norm:
        return True

    # 3. Segment subsequence (bidirectional): either side's segments may
    #    be a subsequence of the other (e.g. user says 'A100-80GB' and
    #    cluster has 'a100-sxm4-80gb', or vice-versa).
    req_segments = req_norm.split('-')
    cand_segments = cand_norm.split('-')
    if len(req_segments) < len(cand_segments):
        return _is_segment_subsequence(req_segments, cand_segments)
    if len(cand_segments) < len(req_segments):
        return _is_segment_subsequence(cand_segments, req_segments)
    return False


def canonicalize_raw_gpu_name(raw_name: str) -> str:
    """Convert a raw Slurm GRES GPU type to a canonical display name.

    Iterates CANONICAL_GPU_NAMES (most-specific first) and returns the
    first canonical name whose normalized form matches the raw string.
    Falls back to uppercasing.

    Matching rules (checked in order for each canonical name):
    1. Normalized equality (vendor-prefix stripped, separators unified).
    2. Segment subsequence: canonical's dash-segments appear in order within
       the raw name's segments. One-directional only (canonical into raw)
       because the list is ordered most-specific first.

    Examples:
        'nvidia_h100_80gb_hbm3'  -> 'H100-80GB'
        'nvidia_a100_sxm4_80gb'  -> 'A100-80GB'
        'nvidia_l40s'            -> 'L40S'
        'H100'                   -> 'H100'
        'unknown_custom_gpu'     -> 'UNKNOWN_CUSTOM_GPU'
    """
    raw_norm = _normalize_gpu_name(raw_name)
    raw_segments = raw_norm.split('-')

    for canonical in gpu_names.CANONICAL_GPU_NAMES:
        can_norm = _normalize_gpu_name(canonical)

        # 1. Normalized equality (also covers exact case-insensitive matches
        #    since normalization lowercases and unifies separators).
        if can_norm == raw_norm:
            return canonical

        # 2. Canonical segments are a subsequence of raw segments.
        # One-directional: only check canonical-into-raw, not the reverse,
        # because the list is ordered most-specific first (e.g. 'H100-80GB'
        # before 'H100') and a reverse match would cause 'H100' to
        # incorrectly resolve to 'H100-80GB'.
        can_segments = can_norm.split('-')
        if len(can_segments) < len(raw_segments):
            if _is_segment_subsequence(can_segments, raw_segments):
                return canonical

    return raw_name.upper()
