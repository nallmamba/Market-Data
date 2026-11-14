"""
ctrader_market_data_puller.py
-------------------------------
Turns one (instrument, year, month) gap - identified by
ctrader_market_data_gaps.py - into an actual fetched,
frontend-ready monthly bar file.

WHY THIS IS MOSTLY WIRING, NOT NEW LOGIC: the frontend's monthly JSON
files are already just a plain array of {time, open, high, low, close}
dicts - confirmed against fetchMonthBarsCached()'s own
`Array.isArray(fetched)` check in index.html, not assumed - which is
EXACTLY what fetch_trendbars_as_bars() (ctrader_candles.py) already
returns. So most of what this module adds is: matching a manifest
instrument code (always lowercase, e.g. "eurusd") against cTrader's
own symbol names (typically uppercase, e.g. "EURUSD"), computing a
full calendar month's UTC boundaries, and handling the "nothing to
pull" cases gracefully rather than erroring.

ONE GENUINE SIDE EFFECT WORTH NAMING: a full calendar month of M1 bars
is comfortably over the ~14,000-bar single-request cap (a month is
~40,000-44,000 minutes even accounting for forex's closed weekends) -
so calling this for real is the first thing in this project that
actually EXERCISES fetch_trendbars_as_bars()'s chunking logic against
more than a synthetic test scenario, not just a narrow trade-replay
window that always fits in one request.

WHAT'S NOT HERE: committing the result to GitHub, or updating
manifest.json - that's the next piece, once this one's output (a
{repo_path: bars} dict) has somewhere to go.
"""

from datetime import datetime, timezone

from ctrader_candles import fetch_trendbars_as_bars


def build_symbol_lookup(symbol_map):
    """
    `symbol_map`: {symbolId: symbolName} from build_symbol_map()
    (ctrader_history.py). Returns {lowercased_symbol_name: symbolId},
    for matching a manifest instrument code against cTrader's symbol
    names case-insensitively - manifest codes are always lowercase
    (index.html's discoverAvailableInstruments() keys everything by
    lowercase folder name), cTrader's own symbol names typically
    aren't.
    """
    return {name.lower(): symbol_id for symbol_id, name in symbol_map.items()}


def symbol_ids_needed(missing_by_instrument, symbol_lookup):
    """
    Every symbolId actually needed to pull everything
    missing_months_by_instrument() found - meant to be batch-fetched
    through fetch_symbol_digits() ONCE (ctrader_candles.py), the same
    "batch, not per-item" pattern already used elsewhere in this
    project, rather than one digits lookup per month pulled.
    """
    return [
        symbol_lookup[code]
        for code in missing_by_instrument
        if code in symbol_lookup
    ]


def month_bounds_utc(year, month):
    """
    Returns (from_dt, to_dt) spanning one WHOLE calendar month as the
    half-open range [start of month, start of NEXT month) - the same
    boundary convention fetchMonthBarsCached() in index.html uses
    (Date.UTC(year, month-1, 1) through just before
    Date.UTC(nextYear, nextMonth-1, 1)).
    """
    # Timezone-aware UTC: a naive datetime's .timestamp() would use the
    # machine's LOCAL zone, silently shifting every request on a non-UTC PC.
    from_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    to_dt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return from_dt, to_dt


def pull_month(session, ctid_trader_account_id, instrument_code, year, month, symbol_lookup, digits_by_symbol):
    """
    Fetches one full calendar month of M1 bars for `instrument_code`
    and returns them in the exact shape the frontend's monthly JSON
    files already use - see module docstring.

    Returns None (never an empty list) if the instrument code doesn't
    match any known cTrader symbol, or if cTrader genuinely has no
    bars for that month (e.g. requested further back than the broker's
    own price history goes) - both are "nothing to write", not errors,
    and callers should treat them that way rather than writing an
    empty file.
    """
    symbol_id = symbol_lookup.get(instrument_code.lower())
    if symbol_id is None:
        return None

    digits = digits_by_symbol.get(symbol_id, 5)  # 5 is the common FX fallback - see fetch_symbol_digits()'s own docstring
    from_dt, to_dt = month_bounds_utc(year, month)
    bars = fetch_trendbars_as_bars(session, ctid_trader_account_id, symbol_id, digits, from_dt, to_dt)
    return bars if bars else None


def pull_all_missing(session, ctid_trader_account_id, missing_by_instrument, symbol_lookup, digits_by_symbol, log=print):
    """
    Top-level entry point: `missing_by_instrument` is
    ctrader_market_data_gaps.missing_months_by_instrument()'s output.
    Returns {repo_path: bars_list} for every month successfully
    pulled - repo_path matches the exact
    "<instrument>/<instrument>_<year>_<mm>.json" convention worker.js's
    MANIFEST_FILE_PATTERN and updateManifest() expect, so this dict's
    keys are ready to hand straight to a commit step once one exists.

    An instrument code with no matching cTrader symbol, or a month
    that comes back with no bars, is logged and skipped rather than
    silently dropped or treated as an error - see pull_month()'s
    docstring for why both are legitimate "nothing to write" outcomes.
    """
    results = {}
    for instrument_code, months in missing_by_instrument.items():
        for year, month in months:
            bars = pull_month(session, ctid_trader_account_id, instrument_code, year, month, symbol_lookup, digits_by_symbol)
            mm = f"{month:02d}"
            repo_path = f"{instrument_code}/{instrument_code}_{year}_{mm}.json"
            if bars is None:
                log(f"Skipping {repo_path} - no bars available (unmatched symbol or no data for this month).")
                continue
            results[repo_path] = bars
            log(f"Pulled {repo_path}: {len(bars)} bars.")
    return results
