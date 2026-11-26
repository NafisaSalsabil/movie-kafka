"""Event-time window and grace-period rules for the streaming sink."""

WINDOW_SECONDS = 60 * 60
GRACE_SECONDS = 5 * 60


def window_bounds(event_timestamp):
    """Return the UTC tumbling-hour window containing an epoch timestamp."""
    start = (int(event_timestamp) // WINDOW_SECONDS) * WINDOW_SECONDS
    return start, start + WINDOW_SECONDS


def is_beyond_grace(event_timestamp, max_event_time_seen_before):
    """True only after the event's window end has passed the 5-minute grace.

    The exact boundary is inclusive: when the previous event-time high-water
    mark equals window_end + 300 seconds, the row is still accepted.
    """
    if max_event_time_seen_before is None:
        return False
    _, end = window_bounds(event_timestamp)
    return int(max_event_time_seen_before) > end + GRACE_SECONDS


def watermark_epoch(max_event_time):
    """The event-time watermark is maximum observed event time minus grace."""
    if max_event_time is None:
        return None
    return int(max_event_time) - GRACE_SECONDS
