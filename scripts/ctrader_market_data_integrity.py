"""
ctrader_market_data_integrity.py
----------------------------------
Pure logic, no network: decides whether one month of M1 bars looks
damaged (blank/zero bars, holes in the middle of a trading session, or
a month that is almost empty) so the monthly audit can re-pull it.

It is a HEURISTIC. Markets legitimately have gaps (weekends, holidays,
daily maintenance breaks), and those must not be flagged. When the audit
re-pulls a flagged month and the broker returns the same thin data, the
runner records that result (see runner's audit state) and stops retrying,
so a legitimately sparse month is only re-pulled once.
"""

import hashlib
import math
from datetime import datetime, timedelta, timezone

MAX_GAP_MINUTES = 120      # longer than a normal daily maintenance break
MIN_BARS_FULL_MONTH = 5000  # a real month of M1 data is 20,000+; below this is almost certainly damaged
MAX_REPORTED_GAPS = 50
MAX_WEEKEND_GAP_HOURS = 100  # Fri close -> Mon open, plus a long holiday weekend


def _utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _gap_is_expected(start_ts, end_ts):
    """True for gaps that are normal: weekends (including long ones) and the year-end holiday window."""
    start, end = _utc(start_ts), _utc(end_ts)

    # Year-end thin trading / closures: Dec 24 - Jan 2.
    if (start.month == 12 and start.day >= 24) or (start.month == 1 and start.day <= 2):
        return True

    # A weekend: the gap spans a Saturday and is not absurdly long.
    if end_ts - start_ts <= MAX_WEEKEND_GAP_HOURS * 3600:
        day = start.date()
        while day <= end.date():
            if day.weekday() == 5:  # Saturday
                return True
            day += timedelta(days=1)
    return False


def _is_bad_bar(bar):
    try:
        values = [bar["open"], bar["high"], bar["low"], bar["close"]]
    except (KeyError, TypeError):
        return True
    for v in values:
        if v is None or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0:
            return True
    return bar["high"] < bar["low"]


def find_suspects(bars, max_gap_minutes=MAX_GAP_MINUTES, min_bars=MIN_BARS_FULL_MONTH):
    """
    Returns a list of short descriptor strings, empty if the month looks
    fine. `bars` is the month file's list of {time, open, high, low, close}.
    """
    if not bars:
        return ["empty"]

    suspects = []
    bad = sum(1 for b in bars if _is_bad_bar(b))
    if bad:
        suspects.append(f"bad:{bad}")
    if len(bars) < min_bars:
        suspects.append("sparse")

    times = sorted(b["time"] for b in bars if isinstance(b.get("time"), (int, float)))
    gaps = 0
    for prev, cur in zip(times, times[1:]):
        if (cur - prev) / 60 > max_gap_minutes and not _gap_is_expected(prev, cur):
            gaps += 1
            if gaps <= MAX_REPORTED_GAPS:
                suspects.append(f"gap:{int(prev)}:{int(cur)}")
    return suspects


def signature(suspects):
    """Short stable fingerprint of a suspect list, so 'same problem as last time' is detectable."""
    if not suspects:
        return ""
    return hashlib.sha1("|".join(sorted(suspects)).encode()).hexdigest()[:12]
