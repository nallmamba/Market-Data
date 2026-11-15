"""
ctrader_market_data_gaps.py
-----------------------------
Figures out which (instrument, year, month) files are missing from the
GitHub market-data repo, so the scheduled puller (Thread C from the
roadmap - the trendbar-fetching and GitHub-commit pieces come in a
later step) knows exactly what to fetch and nothing more.

Pure logic, no network calls, no cTrader connection - this only needs
manifest.json's already-fetched content and today's date, which is
exactly why it's built and tested as its own piece before anything
that touches a live connection.

THE DECISION THIS ENCODES (from the roadmap): extend forward and fill
real gaps, never touch or overwrite an existing month, and never
decide on its own to start tracking a brand new instrument or to
backfill further into the past than what's already there.

  - "Extend forward": once an instrument has at least one month in the
    repo, every completed month after the latest one gets picked up
    automatically, forever - this is the actual "no admin task ever
    again" goal.
  - "Fill real gaps": a genuine hole INSIDE the existing range (e.g.
    Jan and Mar exist but Feb doesn't - a month that failed to upload,
    or was skipped by hand once) also gets picked up, not just
    whatever's past the latest month. One unified walk from the
    earliest existing month through the last fully-completed month
    catches both cases at once - see missing_months_for_instrument().
  - "Never before the earliest existing month": an instrument with NO
    months at all yet returns nothing to fetch, and gaps are never
    hunted for earlier than whatever's already the earliest present
    month. Deciding how far into the past to backfill a given
    instrument (does the broker even have data that far back? how far
    is worth the request budget?) is a real, deliberate choice - not
    something a nightly job should make unattended. That's a manual,
    explicit action for later, not this function's job.
  - "Never the current month": the most recent CALENDAR month that has
    fully ended is the newest thing this ever returns. The current,
    still-in-progress month is deliberately out of scope here - that's
    the separate on-demand "current month" path from Thread B, which
    needs to keep re-fetching a PARTIAL month as it grows; this module
    only ever deals in settled, final months.
"""

import re

MONTHLY_FILE_PATTERN = re.compile(r"^(?P<instrument>.+)_(?P<year>\d{4})_(?P<month>\d{2})\.json$", re.IGNORECASE)


def parse_manifest_files(files):
    """
    `files`: manifest.json's "files" array (see worker.js's
    updateManifest()) - paths like "eurusd/eurusd_2024_01.json".
    Returns {instrument_code: set of (year, month) tuples}. Instrument
    codes are lowercased, matching the frontend's own case-insensitive
    folder-code convention (index.html's discoverAvailableInstruments()
    keys everything by lowercase folder name for the same reason).
    """
    months_by_instrument = {}
    for path in files:
        if "/" not in path:
            continue  # root-level file (manifest.json itself, README, etc.) - same skip worker.js's rebuildManifestFromTree() does
        folder, filename = path.split("/", 1)
        match = MONTHLY_FILE_PATTERN.match(filename)
        if not match:
            continue
        instrument = folder.lower()
        year = int(match.group("year"))
        month = int(match.group("month"))
        months_by_instrument.setdefault(instrument, set()).add((year, month))
    return months_by_instrument


def _next_month(year, month):
    return (year, month + 1) if month < 12 else (year + 1, 1)


def last_fully_completed_month(today):
    """
    The most recent calendar month that has FULLY ended as of `today` -
    e.g. any day in August 2026 gives (2026, 7), not (2026, 8). See
    module docstring's "never the current month" point for why.
    """
    if today.month == 1:
        return (today.year - 1, 12)
    return (today.year, today.month - 1)


def missing_months_for_instrument(existing_months, today):
    """
    `existing_months`: set of (year, month) tuples already in the repo
    for one instrument (from parse_manifest_files() above).

    Returns a sorted list of (year, month) tuples to fetch - every
    month from the EARLIEST existing one through the last fully
    completed month, that ISN'T already present. Catches both a true
    mid-history gap and forward extension past the latest month in one
    pass. Returns an empty list for an instrument with no existing
    months at all - see module docstring's "never before the earliest"
    and "never a brand new instrument" points for why that's correct,
    not a bug.
    """
    if not existing_months:
        return []

    earliest = min(existing_months)
    target = last_fully_completed_month(today)
    if earliest > target:
        return []  # only existing data is somehow in the future relative to `today` - defensive, shouldn't happen in practice

    missing = []
    cursor = earliest
    while cursor <= target:
        if cursor not in existing_months:
            missing.append(cursor)
        cursor = _next_month(*cursor)
    return missing


def missing_months_by_instrument(manifest_files, today):
    """
    Top-level entry point. manifest.json's "files" array in,
    {instrument_code: [(year, month), ...]} of everything that needs
    fetching out. Instruments with nothing missing are omitted
    entirely (not present with an empty list), so a caller can just do
    `for instrument, months in missing_months_by_instrument(...).items()`
    without an extra "skip if empty" check.
    """
    result = {}
    months_by_instrument = parse_manifest_files(manifest_files)
    for instrument, existing in months_by_instrument.items():
        missing = missing_months_for_instrument(existing, today)
        if missing:
            result[instrument] = missing
    return result
