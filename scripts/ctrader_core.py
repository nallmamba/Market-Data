"""
ctrader_core.py
-----------------
Shared, boring plumbing used by every other ctrader_*.py file:
atomic JSON reads/writes for the trades file, the candles file, and
the live-event timeline file.

WHY THIS FILE EXISTS NOW, WHEN IT DIDN'T BEFORE:

Each of ctrader_history.py, ctrader_candles.py, and
ctrader_sync_bridge.py grew its own near-identical
read-json-tolerate-corruption / write-json-to-a-.tmp-then-os.replace()
pair, written separately as each file was built one step at a time.
They'd drifted apart slightly by the time all three existed side by
side - not in a way that caused a bug, but in a way that would have,
eventually, if one got a fix the others didn't. This file is that
logic written once, imported everywhere.

THIS IS SCOPED TO THE CTRADER SIDE ONLY. mt5_sync_bridge.py has its
own, separately-evolved versions of the same idea and hasn't been
touched here - unifying across BOTH platforms into one truly shared
core (the architecture proposed early in this project) is a bigger
change than "consolidate cTrader's own internal duplication," and
would mean editing mt5_sync_bridge.py itself, which wasn't the ask for
this step. What's here removes the duplication that had built up
within the cTrader codebase specifically.

WHAT DIDN'T MOVE, ON PURPOSE:

log() / acquire_lock() / release_lock() stay in ctrader_sync_bridge.py
- they're only ever used by that one process (the smoke-test
`__main__` blocks in ctrader_history.py / ctrader_candles.py print
straight to stdout, deliberately, since they're meant to be run
standalone by hand, not as a background service). Moving them here
would just relocate code with exactly one caller, not remove any
duplication - not worth the churn.
"""

import json
import os


def load_json(path, default=None):
    """
    Reads a JSON file, tolerating "doesn't exist yet" and "got
    corrupted mid-write somehow" the same way every caller here needs
    to on a bridge that's meant to survive being killed mid-write.
    Returns `default` (falls back to None) in either case.
    """
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return default


def save_json_atomic(path, data, **json_kwargs):
    """
    Writes to `path` in a way that can never leave a half-written,
    corrupt file behind - even if the process is killed mid-write, the
    original file is untouched until the new one is fully flushed to
    disk (write to a .tmp file, then os.replace(), which is atomic on
    both Windows and POSIX). Every save_* helper below goes through
    this rather than writing `path` directly.
    """
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, **json_kwargs)
    os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# Trades file: saved as a JSON LIST (sorted by close time) on disk, but
# every caller wants to work with it as a {ticket: trade} DICT in memory
# - these two functions are the only place that translation happens.
# ---------------------------------------------------------------------------

def load_trades(trades_file):
    data = load_json(trades_file, default=[])
    return {str(t["ticket"]): t for t in data if "ticket" in t}


def save_trades(trades_file, trades_by_ticket):
    trades = sorted(trades_by_ticket.values(), key=lambda t: t.get("closeTime", ""))
    save_json_atomic(trades_file, trades, indent=2)


# ---------------------------------------------------------------------------
# Candles file: {ticket: candle_record}, written compact (no indent) -
# these files can get large (a full OHLC bar list per trade), so unlike
# trades.json there's no reason to spend bytes on pretty-printing.
# ---------------------------------------------------------------------------

def load_candles(candles_file):
    return load_json(candles_file, default={})


def save_candles(candles_file, candles_by_ticket):
    save_json_atomic(candles_file, candles_by_ticket)  # no indent - see note above


# ---------------------------------------------------------------------------
# Timeline file: {ticket_or_pending_key: [event, ...]} - the live
# event log poll_live_events() builds up in ctrader_live.py.
# ---------------------------------------------------------------------------

def load_timeline(timeline_file):
    return load_json(timeline_file, default={})


def save_timeline(timeline_file, timeline):
    save_json_atomic(timeline_file, timeline)
