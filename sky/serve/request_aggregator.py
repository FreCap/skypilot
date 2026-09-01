"""In-memory request accounting for the SkyServe load balancer."""
import bisect
import collections
import math
import time
import typing
from typing import Any

from sky.serve import constants

if typing.TYPE_CHECKING:
    import fastapi


class RequestsAggregator:
    """Base class for request aggregator."""

    def add(self, request: 'fastapi.Request') -> None:
        """Add a request to the request aggregator."""
        raise NotImplementedError

    def add_rejection(self) -> None:
        """Record one terminal load-balancer rejection."""
        raise NotImplementedError

    def add_request_classification(self, *, rejected: bool) -> None:
        """Record one terminal classification for an eligible request."""
        raise NotImplementedError

    def clear(self) -> None:
        """Clear all current request aggregator."""
        raise NotImplementedError

    def drain(self) -> dict[str, Any]:
        """Atomically take the current report batch out of the aggregator.

        New samples added after this method returns belong to the next batch.
        The caller must restore the returned batch if delivery fails.
        """
        raise NotImplementedError

    def restore(self, batch: dict[str, Any]) -> None:
        """Restore a previously drained batch after failed delivery."""
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        """Convert the aggregator to a dict."""
        raise NotImplementedError

    def request_history_snapshot(self) -> dict[str, Any] | None:
        """Return request-history counters awaiting acknowledgement."""
        raise NotImplementedError

    def demand_window_snapshot(self) -> dict[str, Any]:
        """Return the complete bounded autoscaling window without draining."""
        raise NotImplementedError

    def mark_request_history_accepted(self,
                                      snapshot: dict[str, Any] | None) -> None:
        """Mark a request-history snapshot as durably accepted."""
        raise NotImplementedError

    def request_classification_history_snapshot(self) -> dict[str, Any]:
        """Return independently acknowledged terminal classifications."""
        raise NotImplementedError

    def mark_request_classification_history_accepted(
            self, snapshot: dict[str, Any] | None) -> None:
        """Mark a classification snapshot as durably accepted."""
        raise NotImplementedError

    def add_prediction_time(self, duration_seconds: float,
                            outcome: str) -> None:
        """Record one completed prediction."""
        raise NotImplementedError

    def prediction_time_history_snapshot(self) -> dict[str, Any] | None:
        """Return prediction-time counters awaiting acknowledgement."""
        raise NotImplementedError

    def mark_prediction_time_history_accepted(
            self, snapshot: dict[str, Any] | None) -> None:
        """Mark a prediction-time snapshot as durably accepted."""
        raise NotImplementedError

    def __repr__(self) -> str:
        raise NotImplementedError


class RequestTimestamp(RequestsAggregator):
    """RequestTimestamp: Aggregates request timestamps.

    This is useful for QPS-based autoscaling.
    """

    def __init__(self) -> None:
        # A fresh process cannot prove that the preceding autoscaling window
        # was empty. This durable-report watermark becomes authoritative only
        # after the recorder has observed one complete window itself.
        self._demand_window_coverage_started_at = time.time()
        # Bounded: the batch is retained across a failed controller sync (so
        # load signal is not dropped), but a persistent failure must not grow it
        # without limit -- maxlen keeps only the most recent samples (ample for
        # QPS autoscaling). See constants.LB_REQUEST_TIMESTAMP_CAP.
        self.timestamps: collections.deque[float] = collections.deque(
            maxlen=constants.LB_REQUEST_TIMESTAMP_CAP)
        self.compatibility_profiles: collections.deque[dict[str, Any]] = (
            collections.deque(maxlen=constants.LB_REQUEST_TIMESTAMP_CAP))
        # The legacy controller channel drains ``timestamps`` on every sync.
        # Keep a separate rolling snapshot for the durable demand feed so a
        # successful controller round cannot erase the central feed's view.
        self._demand_window_timestamps: collections.deque[float] = (
            collections.deque(maxlen=constants.LB_REQUEST_TIMESTAMP_CAP))
        self._demand_window_profiles: collections.deque[dict[str, Any]] = (
            collections.deque(maxlen=constants.LB_REQUEST_TIMESTAMP_CAP))
        # Exact arrival counters are reported independently from the lossy,
        # bounded raw timestamp batch used by autoscaling. Counts remain in
        # memory through the current hour so another request in an already
        # acknowledged minute advances the same cumulative counter.
        self._request_history: dict[int, int] = {}
        self._acknowledged_request_history: dict[int, int] = {}
        self._rejection_history: dict[int, int] = {}
        self._acknowledged_rejection_history: dict[int, int] = {}
        # Terminal classifications intentionally use a separate cumulative
        # report and acknowledgement from arrival history. A new LB talking to
        # an old controller must retain these counters when only the legacy
        # request-history snapshot is acknowledged.
        self._classified_request_history: dict[int, int] = {}
        self._counted_rejection_history: dict[int, int] = {}
        self._acknowledged_classified_request_history: dict[int, int] = {}
        self._acknowledged_counted_rejection_history: dict[int, int] = {}
        self._prediction_time_history: dict[int, dict[str, list[int]]] = {}
        self._acknowledged_prediction_time_history: dict[int,
                                                         dict[str,
                                                              list[int]]] = {}
        # Pruning rebuilds both bounded history dictionaries. Keep that work on
        # minute boundaries (and controller snapshots), never on every request.
        self._last_pruned_request_history_bucket: int | None = None

    def add(self, request: 'fastapi.Request') -> None:
        """Add a request to the request aggregator."""
        timestamp = time.time()
        self.timestamps.append(timestamp)
        compatible = getattr(request, '_skyserve_compatible_accelerators', None)
        self.compatibility_profiles.append({
            'timestamp': timestamp,
            'priority': int(
                getattr(request, '_skyserve_request_priority',
                        constants.LB_REQUEST_PRIORITY_MIN)),
            # None distinguishes a legacy omitted-catalog request from an
            # explicit canonical set; an empty list is never valid.
            'compatible_accelerators':
                (list(compatible) if compatible is not None else None),
        })
        self._demand_window_timestamps.append(timestamp)
        self._demand_window_profiles.append({
            'timestamp': timestamp,
            'priority': int(
                getattr(request, '_skyserve_request_priority',
                        constants.LB_REQUEST_PRIORITY_MIN)),
            'compatible_accelerators':
                (list(compatible) if compatible is not None else None),
        })
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(timestamp // bucket_seconds) * bucket_seconds
        self._request_history[bucket_start] = (
            self._request_history.get(bucket_start, 0) + 1)
        if bucket_start != self._last_pruned_request_history_bucket:
            self._prune_request_history(bucket_start)

    def add_rejection(self) -> None:
        """Record one terminal 503 in its completion-minute bucket."""
        timestamp = time.time()
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(timestamp // bucket_seconds) * bucket_seconds
        self._rejection_history[bucket_start] = (
            self._rejection_history.get(bucket_start, 0) + 1)
        if bucket_start != self._last_pruned_request_history_bucket:
            self._prune_request_history(bucket_start)

    def add_request_classification(self, *, rejected: bool) -> None:
        """Record one eligible request outcome in its terminal minute.

        A rejected outcome advances both components in one synchronous
        operation. Their difference is therefore the exact, monotonic count of
        non-rejected requests even when snapshots are retried or arrive out of
        order at the controller.
        """
        timestamp = time.time()
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(timestamp // bucket_seconds) * bucket_seconds
        self._classified_request_history[bucket_start] = (
            self._classified_request_history.get(bucket_start, 0) + 1)
        if rejected:
            self._counted_rejection_history[bucket_start] = (
                self._counted_rejection_history.get(bucket_start, 0) + 1)
        if bucket_start != self._last_pruned_request_history_bucket:
            self._prune_request_history(bucket_start)

    def add_prediction_time(self, duration_seconds: float,
                            outcome: str) -> None:
        """Record one completed prediction in its observation minute."""
        timestamp = time.time()
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(timestamp // bucket_seconds) * bucket_seconds
        if outcome not in constants.LB_PREDICTION_TIME_OUTCOMES:
            raise ValueError(f'Unsupported prediction outcome: {outcome!r}.')
        if (not isinstance(duration_seconds, (int, float)) or
                isinstance(duration_seconds, bool) or
                not math.isfinite(duration_seconds)):
            raise ValueError('Prediction duration must be finite.')
        duration_seconds = max(0.0, float(duration_seconds))
        duration_bucket = bisect.bisect_left(
            constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS,
            duration_seconds)
        outcome_counts = self._prediction_time_history.setdefault(
            bucket_start, {})
        counts = outcome_counts.setdefault(
            outcome, [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT)
        counts[duration_bucket] += 1
        if bucket_start != self._last_pruned_request_history_bucket:
            self._prune_request_history(bucket_start)

    def clear(self) -> None:
        """Clear all current request aggregator."""
        self.timestamps.clear()
        self.compatibility_profiles.clear()
        self._demand_window_timestamps.clear()
        self._demand_window_profiles.clear()
        self._request_history.clear()
        self._acknowledged_request_history.clear()
        self._rejection_history.clear()
        self._acknowledged_rejection_history.clear()
        self._classified_request_history.clear()
        self._counted_rejection_history.clear()
        self._acknowledged_classified_request_history.clear()
        self._acknowledged_counted_rejection_history.clear()
        self._prediction_time_history.clear()
        self._acknowledged_prediction_time_history.clear()
        self._last_pruned_request_history_bucket = None

    def _prune_request_history(self, newest_bucket: int) -> None:
        oldest_bucket = (newest_bucket -
                         (constants.LB_REQUEST_HISTORY_MAX_BUCKETS - 1) *
                         constants.LB_REQUEST_HISTORY_BUCKET_SECONDS)
        self._request_history = {
            bucket: count
            for bucket, count in self._request_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_request_history = {
            bucket: count
            for bucket, count in self._acknowledged_request_history.items()
            if bucket >= oldest_bucket
        }
        self._rejection_history = {
            bucket: count
            for bucket, count in self._rejection_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_rejection_history = {
            bucket: count
            for bucket, count in self._acknowledged_rejection_history.items()
            if bucket >= oldest_bucket
        }
        self._classified_request_history = {
            bucket: count
            for bucket, count in self._classified_request_history.items()
            if bucket >= oldest_bucket
        }
        self._counted_rejection_history = {
            bucket: count
            for bucket, count in self._counted_rejection_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_classified_request_history = {
            bucket: count
            for bucket, count in
            self._acknowledged_classified_request_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_counted_rejection_history = {
            bucket: count
            for bucket, count in
            self._acknowledged_counted_rejection_history.items()
            if bucket >= oldest_bucket
        }
        self._prediction_time_history = {
            bucket: counts
            for bucket, counts in self._prediction_time_history.items()
            if bucket >= oldest_bucket
        }
        self._acknowledged_prediction_time_history = {
            bucket: counts
            for bucket, counts in
            self._acknowledged_prediction_time_history.items()
            if bucket >= oldest_bucket
        }
        self._last_pruned_request_history_bucket = newest_bucket

    def request_history_snapshot(self) -> dict[str, Any] | None:
        """Return counters changed since their last durable acknowledgement."""
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        newest_bucket = int(time.time() // bucket_seconds) * bucket_seconds
        self._prune_request_history(newest_bucket)
        bucket_starts = sorted(
            set(self._request_history) | set(self._rejection_history))
        buckets = []
        for bucket in bucket_starts:
            request_count = self._request_history.get(bucket, 0)
            rejected_count = self._rejection_history.get(bucket, 0)
            if (request_count <= self._acknowledged_request_history.get(
                    bucket, 0) and
                    rejected_count <= self._acknowledged_rejection_history.get(
                        bucket, 0)):
                continue
            bucket_payload = {
                'bucket_start': bucket,
                'request_count': request_count,
                'rejected_count': rejected_count,
            }
            buckets.append(bucket_payload)
        if not buckets:
            return None
        return {
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'buckets': buckets,
        }

    def demand_window_snapshot(self) -> dict[str, Any]:
        """Return one non-destructive, bounded rolling demand snapshot."""
        window_seconds = constants.LB_DEMAND_WINDOW_SECONDS
        bucket_seconds = constants.LB_DEMAND_WINDOW_BUCKET_SECONDS
        cutoff = time.time() - window_seconds
        while (self._demand_window_timestamps and
               self._demand_window_timestamps[0] < cutoff):
            self._demand_window_timestamps.popleft()
        while (self._demand_window_profiles and
               self._demand_window_profiles[0]['timestamp'] < cutoff):
            self._demand_window_profiles.popleft()
        counts_by_bucket: dict[int, int] = {}
        grouped: dict[tuple[int, int, frozenset[str]], dict[str, Any]] = {}
        compatibility_complete = True
        for timestamp in self._demand_window_timestamps:
            bucket = int(timestamp // bucket_seconds) * bucket_seconds
            counts_by_bucket[bucket] = counts_by_bucket.get(bucket, 0) + 1
        for profile in self._demand_window_profiles:
            accelerators = profile.get('compatible_accelerators')
            if not isinstance(accelerators, list) or not accelerators:
                compatibility_complete = False
                continue
            priority = int(profile['priority'])
            bucket = int(
                profile['timestamp'] // bucket_seconds) * bucket_seconds
            key = (bucket, priority, frozenset(accelerators))
            grouped_profile = grouped.get(key)
            if grouped_profile is None:
                grouped[key] = {
                    'priority': priority,
                    'compatible_accelerators': list(accelerators),
                    'count': 1,
                }
            else:
                grouped_profile['count'] += 1
        saturated = (len(self._demand_window_timestamps) ==
                     constants.LB_REQUEST_TIMESTAMP_CAP)
        return {
            'bucket_seconds': bucket_seconds,
            'window_seconds': window_seconds,
            'coverage_started_at': self._demand_window_coverage_started_at,
            'buckets': [{
                'bucket_start': bucket,
                'request_count': count,
                'compatibility_profiles': [
                    profile
                    for (profile_bucket, _, _), profile in grouped.items()
                    if profile_bucket == bucket
                ],
            }
                        for bucket, count in sorted(counts_by_bucket.items())],
            'compatibility_complete': compatibility_complete and not saturated,
            'saturated': saturated,
        }

    def mark_request_history_accepted(self,
                                      snapshot: dict[str, Any] | None) -> None:
        """Acknowledge only counts present in an accepted snapshot.

        Requests arriving while the snapshot is in flight increment the live
        counter beyond the acknowledged value and are therefore sent on the
        next sync.
        """
        if snapshot is None:
            return
        for bucket in snapshot.get('buckets', []):
            bucket_start = bucket.get('bucket_start')
            request_count = bucket.get('request_count')
            rejected_count = bucket.get('rejected_count', 0)
            current_count = self._request_history.get(bucket_start)
            if current_count is not None:
                accepted_count = min(current_count, request_count)
                self._acknowledged_request_history[bucket_start] = max(
                    accepted_count,
                    self._acknowledged_request_history.get(bucket_start, 0))
            current_rejected = self._rejection_history.get(bucket_start)
            if current_rejected is not None:
                accepted_rejected = min(current_rejected, rejected_count)
                self._acknowledged_rejection_history[bucket_start] = max(
                    accepted_rejected,
                    self._acknowledged_rejection_history.get(bucket_start, 0))

    def request_classification_history_snapshot(self) -> dict[str, Any]:
        """Return terminal counters changed since durable acceptance.

        Unlike legacy request history, this always returns the versioned
        envelope. An empty v1 snapshot is capability evidence during a rolling
        upgrade and lets the controller mark request-history rows with a valid
        zero pair instead of mistaking the reporter for a legacy LB.
        """
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        newest_bucket = int(time.time() // bucket_seconds) * bucket_seconds
        self._prune_request_history(newest_bucket)
        bucket_starts = sorted(
            set(self._classified_request_history) |
            set(self._counted_rejection_history))
        buckets = []
        for bucket_start in bucket_starts:
            classified_count = self._classified_request_history.get(
                bucket_start, 0)
            rejected_count = self._counted_rejection_history.get(
                bucket_start, 0)
            if (classified_count
                    <= self._acknowledged_classified_request_history.get(
                        bucket_start, 0) and rejected_count
                    <= self._acknowledged_counted_rejection_history.get(
                        bucket_start, 0)):
                continue
            buckets.append({
                'bucket_start': bucket_start,
                'classified_request_count': classified_count,
                'counted_rejected_count': rejected_count,
            })
        return {
            'classification_version': 1,
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'buckets': buckets,
        }

    def mark_request_classification_history_accepted(
            self, snapshot: dict[str, Any] | None) -> None:
        """Acknowledge only terminal counters in one accepted snapshot."""
        if snapshot is None or snapshot.get('classification_version') != 1:
            return
        for bucket in snapshot.get('buckets', []):
            bucket_start = bucket.get('bucket_start')
            classified_count = bucket.get('classified_request_count')
            rejected_count = bucket.get('counted_rejected_count')
            current_classified = self._classified_request_history.get(
                bucket_start)
            if current_classified is not None:
                accepted_classified = min(current_classified, classified_count)
                self._acknowledged_classified_request_history[bucket_start] = (
                    max(
                        accepted_classified,
                        self._acknowledged_classified_request_history.get(
                            bucket_start, 0)))
            current_rejected = self._counted_rejection_history.get(bucket_start)
            if current_rejected is not None:
                accepted_rejected = min(current_rejected, rejected_count)
                self._acknowledged_counted_rejection_history[bucket_start] = (
                    max(
                        accepted_rejected,
                        self._acknowledged_counted_rejection_history.get(
                            bucket_start, 0)))

    @staticmethod
    def _prediction_counts_advance(
            current: dict[str, list[int]],
            acknowledged: dict[str, list[int]] | None) -> bool:
        if acknowledged is None:
            return any(sum(counts) for counts in current.values())
        for outcome, counts in current.items():
            accepted = acknowledged.get(outcome, [])
            if any(count > (accepted[index] if index < len(accepted) else 0)
                   for index, count in enumerate(counts)):
                return True
        return False

    def prediction_time_history_snapshot(self) -> dict[str, Any] | None:
        """Return prediction histograms changed since durable acceptance."""
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        newest_bucket = int(time.time() // bucket_seconds) * bucket_seconds
        self._prune_request_history(newest_bucket)
        buckets = []
        for bucket_start in sorted(self._prediction_time_history):
            outcome_counts = self._prediction_time_history[bucket_start]
            acknowledged = self._acknowledged_prediction_time_history.get(
                bucket_start)
            if not self._prediction_counts_advance(outcome_counts,
                                                   acknowledged):
                continue
            buckets.append({
                'bucket_start': bucket_start,
                'outcome_counts': {
                    outcome: list(counts)
                    for outcome, counts in outcome_counts.items()
                    if any(counts)
                },
            })
        if not buckets:
            return None
        return {
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'histogram_version': constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION,
            'buckets': buckets,
        }

    def mark_prediction_time_history_accepted(
            self, snapshot: dict[str, Any] | None) -> None:
        """Acknowledge only histogram counts present in one accepted report."""
        if snapshot is None:
            return
        for bucket in snapshot.get('buckets', []):
            bucket_start = bucket.get('bucket_start')
            live = self._prediction_time_history.get(bucket_start)
            reported = bucket.get('outcome_counts')
            if live is None or not isinstance(reported, dict):
                continue
            acknowledged = self._acknowledged_prediction_time_history.setdefault(
                bucket_start, {})
            for outcome, reported_counts in reported.items():
                live_counts = live.get(outcome)
                if live_counts is None or not isinstance(reported_counts, list):
                    continue
                accepted = acknowledged.setdefault(
                    outcome, [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT)
                for index, reported_count in enumerate(reported_counts):
                    if index >= len(live_counts) or index >= len(accepted):
                        break
                    accepted[index] = max(
                        accepted[index], min(live_counts[index],
                                             reported_count))

    def drain(self) -> dict[str, Any]:
        """Take the current timestamps, leaving later arrivals untouched."""
        batch = self.to_dict()
        self.timestamps.clear()
        self.compatibility_profiles.clear()
        return batch

    def restore(self, batch: dict[str, Any]) -> None:
        """Merge a failed batch back ahead of any arrivals made in-flight.

        Extending oldest-to-newest also preserves the deque's bounded behavior:
        if the combined batches exceed the cap, only the newest timestamps are
        retained.
        """
        drained = batch.get('timestamps', [])
        drained_profiles = batch.get('compatibility_profiles', [])
        if not drained and not drained_profiles:
            return
        current = list(self.timestamps)
        current_profiles = list(self.compatibility_profiles)
        self.timestamps.clear()
        self.compatibility_profiles.clear()
        self.timestamps.extend(drained)
        self.timestamps.extend(current)
        self.compatibility_profiles.extend(drained_profiles)
        self.compatibility_profiles.extend(current_profiles)

    def to_dict(self) -> dict[str, Any]:
        """Convert the aggregator to a dict."""
        grouped_profiles: dict[tuple[int, frozenset[str]], dict[str, Any]] = {}
        for profile in self.compatibility_profiles:
            accelerators = profile.get('compatible_accelerators')
            priority = profile.get('priority')
            timestamp = profile.get('timestamp')
            count = profile.get('count', 1)
            if (not isinstance(accelerators, list) or not accelerators or
                    not isinstance(priority, int) or
                    not isinstance(timestamp, (int, float)) or
                    not isinstance(count, int) or count < 1):
                # Legacy omitted-catalog samples remain visible to aggregate
                # timestamp scaling but cannot be safely assigned to a card.
                continue
            key = (priority, frozenset(accelerators))
            grouped = grouped_profiles.get(key)
            if grouped is None:
                grouped_profiles[key] = {
                    'timestamp': timestamp,
                    'priority': priority,
                    'compatible_accelerators': list(accelerators),
                    'count': count,
                }
            else:
                grouped['timestamp'] = max(grouped['timestamp'], timestamp)
                grouped['count'] += count
        return {
            'timestamps': list(self.timestamps),
            'compatibility_profiles': list(grouped_profiles.values()),
        }

    def __repr__(self) -> str:
        return f'RequestTimestamp(timestamps={list(self.timestamps)})'
