"""
ctrader_market_data_current.py
--------------------------------
Pure helpers for the CURRENT, still-growing month: merging newly pulled
bars into the stored partial file, working out what range still needs
pulling, and never storing the minute bar that is still forming.

All timestamps are UTC. Bars are {time (epoch seconds), open, high, low, close}.
"""

from datetime import datetime, timezone

from ctrader_market_data_puller import month_bounds_utc

# Re-pull this much before the last stored bar, so a bar that was still
# forming (or revised) at the previous run gets replaced by its final form.
OVERLAP_SECONDS = 15 * 60


def data_path(instrument, year, month):
    return f"{instrument}/{instrument}_{year}_{month:02d}.json"


def merge_bars(existing, new):
    """Union by bar time; on a clash the NEW bar wins. Returns a time-sorted list. Never drops existing bars."""
    by_time = {b["time"]: b for b in (existing or [])}
    for b in (new or []):
        by_time[b["time"]] = b
    return [by_time[t] for t in sorted(by_time)]


def drop_forming_bar(bars, now_ts):
    """Removes the bar for the minute that has not finished yet."""
    current_minute = (int(now_ts) // 60) * 60
    return [b for b in bars if b["time"] < current_minute]


def plan_range(existing_bars, year, month, now):
    """
    (from_dt, to_dt) still to pull for the given month as of `now` (aware
    UTC datetime), or None if already up to date. Never reaches past the
    end of the month or the last finished minute.
    """
    start, month_end = month_bounds_utc(year, month)
    floor_now = datetime.fromtimestamp((int(now.timestamp()) // 60) * 60, tz=timezone.utc)
    to_dt = min(month_end, floor_now)
    from_dt = start
    if existing_bars:
        last = existing_bars[-1]["time"]
        from_dt = max(start, datetime.fromtimestamp(last - OVERLAP_SECONDS, tz=timezone.utc))
    if from_dt >= to_dt:
        return None
    return from_dt, to_dt
