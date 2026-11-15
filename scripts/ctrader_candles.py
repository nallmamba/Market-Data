"""
ctrader_candles.py
-------------------
Pulls M1 candles around one trade's entry/exit window and attaches its
event timeline, for Trade Replay - the cTrader equivalent of
fetch_candles_for_trade() in mt5_sync_bridge.py.

WHY THIS LOOKS DIFFERENT FROM fetch_candles_for_trade() IN THE MT5 SCRIPT:

MT5's copy_rates_range() hands back plain OHLC doubles, ready to use.
cTrader's ProtoOATrendbar does NOT - each bar gives you an absolute
`low` plus three DELTAS (deltaOpen, deltaClose, deltaHigh), all as
scaled integers, not an open/high/low/close quadruple of real prices:

    open  = (low + deltaOpen)  / 100000, rounded to the symbol's digits
    high  = (low + deltaHigh)  / 100000, rounded to the symbol's digits
    low   =  low               / 100000, rounded to the symbol's digits
    close = (low + deltaClose) / 100000, rounded to the symbol's digits

The `/ 100000` is a FIXED divisor (not 10**digits) per cTrader's own
"Attain symbol data" tutorial - `digits` only controls the rounding
afterward, e.g. 2 decimal places for an index, 5 for EURUSD. Verified
against Spotware's own worked example (low=257175000, deltaOpen=120000
-> open=2572.95) before trusting this in fetch_trendbars_as_bars()
below - see the test alongside this file.

`digits` itself isn't in the lightweight symbol list (build_symbol_map
in ctrader_history.py) - it's only on the FULL ProtoOASymbol, fetched
here via ProtoOASymbolByIdReq. That request takes a batch of symbol
IDs at once, so fetch_symbol_digits() below is meant to be called once
per sync cycle for whichever symbols actually need candles that cycle,
not once per trade.
"""

import os
import time
from datetime import datetime, timedelta

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAGetTrendbarsReq,
    ProtoOASymbolByIdReq,
)
from ctrader_core import load_candles as load_existing_candles, save_candles, load_timeline

REPLAY_MINUTES_BEFORE = 60   # candle buffer before entry, for replay context - matches mt5_sync_bridge.py
REPLAY_MINUTES_AFTER = 30    # candle buffer after exit
TRENDBAR_PERIOD_M1 = 1       # ProtoOATrendbarPeriod.M1

# cTrader hard-caps ProtoOAGetTrendbarsReq at 14,000 bars per request
# (confirmed by Spotware staff on their own forum - not in the official
# docs). For M1 that's 14,000 minutes of a fully 24/7-open market;
# forex isn't open 24/7, so the real number of TRADING minutes in any
# given calendar window is always somewhat less than the raw minute
# count - meaning this margin is conservative in the right direction
# (understating how much one request can safely cover, never
# overstating it). 9 days * 1440 min/day = 12,960 - comfortably under
# the cap even for an instrument that never closes.
MAX_BARS_PER_REQUEST = 14000
M1_CHUNK_DAYS = 9
# Small pause between chunks on a multi-chunk pull, purely as a
# courtesy toward the 5 req/sec historical-data rate limit on a pull
# that's making several requests back to back - single-chunk pulls
# (the normal case, one trade's own narrow replay window) never hit
# this at all.
INTER_CHUNK_PAUSE_SECONDS = 0.25


def fetch_symbol_digits(session, ctid_trader_account_id, symbol_ids):
    """
    Returns {symbolId: digits} for the given symbol IDs (batched into
    one request). `digits` is how many decimal places that symbol's
    prices display with (e.g. 5 for EURUSD, 2 for most indices) - needed
    to round trendbar prices correctly, see module docstring.
    """
    if not symbol_ids:
        return {}
    conn = session.connection_for_account(ctid_trader_account_id)
    request = ProtoOASymbolByIdReq()
    request.ctidTraderAccountId = ctid_trader_account_id
    request.symbolId.extend(symbol_ids)
    response = conn.call(request)
    return {s.symbolId: s.digits for s in response.symbol}


def _bar_prices(trendbar, digits):
    """Converts one raw trendbar's low+deltas into real, rounded OHLC prices - see module docstring formula."""
    low = trendbar.low / 100000
    open_ = (trendbar.low + trendbar.deltaOpen) / 100000
    high = (trendbar.low + trendbar.deltaHigh) / 100000
    close = (trendbar.low + trendbar.deltaClose) / 100000
    return (round(open_, digits), round(high, digits), round(low, digits), round(close, digits))


def _fetch_one_trendbar_chunk(session, ctid_trader_account_id, symbol_id, digits, from_dt, to_dt):
    """
    One ProtoOAGetTrendbarsReq call, no chunking - the actual network
    request. fetch_trendbars_as_bars() below is the public entry point;
    it calls this once for a normal-sized window (the common case) or
    several times for a wide one (see MAX_BARS_PER_REQUEST above).
    """
    conn = session.connection_for_account(ctid_trader_account_id)
    request = ProtoOAGetTrendbarsReq()
    request.ctidTraderAccountId = ctid_trader_account_id
    request.symbolId = symbol_id
    request.period = TRENDBAR_PERIOD_M1
    request.fromTimestamp = int(from_dt.timestamp() * 1000)
    request.toTimestamp = int(to_dt.timestamp() * 1000)
    response = conn.call(request)

    bars = []
    for tb in response.trendbar:
        open_, high, low, close = _bar_prices(tb, digits)
        bars.append({
            "time": int(tb.utcTimestampInMinutes * 60),  # epoch seconds - matches what lightweight-charts expects
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
        })
    return bars


def fetch_trendbars_as_bars(session, ctid_trader_account_id, symbol_id, digits, from_dt, to_dt):
    """
    Pulls M1 trendbars for [from_dt, to_dt] and returns them in the same
    plain-dict shape MT5's copy_rates_range() output gets turned into:
    [{"time": epoch_seconds, "open":, "high":, "low":, "close":}, ...].

    Transparently splits the request into several chunks of at most
    M1_CHUNK_DAYS each if the window is wide enough to risk the
    14,000-bar cap (see MAX_BARS_PER_REQUEST above) - a trade's own
    narrow replay buffer (REPLAY_MINUTES_BEFORE/AFTER) never comes
    close to needing this; it exists for a position held open many
    days, or for pulling a full month of context (the Thread B
    extension discussed but not yet built). Chunks are fetched
    sequentially (not in parallel) to respect the rate limit, and
    de-duplicated at the boundaries before returning, since two
    adjacent chunks can both legitimately include the bar sitting
    exactly on the cut point.
    """
    total_span = to_dt - from_dt
    if total_span <= timedelta(days=M1_CHUNK_DAYS):
        bars = _fetch_one_trendbar_chunk(session, ctid_trader_account_id, symbol_id, digits, from_dt, to_dt)
        bars.sort(key=lambda b: b["time"])
        return bars

    bars_by_time = {}  # dict, not a list, so an overlapping boundary bar naturally de-dupes by key
    chunk_start = from_dt
    while chunk_start < to_dt:
        chunk_end = min(chunk_start + timedelta(days=M1_CHUNK_DAYS), to_dt)
        chunk_bars = _fetch_one_trendbar_chunk(session, ctid_trader_account_id, symbol_id, digits, chunk_start, chunk_end)
        for bar in chunk_bars:
            bars_by_time[bar["time"]] = bar
        chunk_start = chunk_end
        if chunk_start < to_dt:
            time.sleep(INTER_CHUNK_PAUSE_SECONDS)  # only pauses between chunks - never on the common single-chunk path

    bars = list(bars_by_time.values())
    bars.sort(key=lambda b: b["time"])
    return bars


def fetch_candles_for_trade(session, ctid_trader_account_id, trade, timeline, symbol_id, digits):
    """
    Pulls candles covering [entry - buffer, exit + buffer] for one trade
    and attaches its event timeline, exactly matching the record shape
    fetch_candles_for_trade() produces in the MT5 script - so Trade
    Replay's front end reads either platform's candle file identically.
    Returns None if cTrader has no bars for this window.
    """
    if not trade.get("openTimestamp") or not trade.get("closeTimestamp"):
        return None

    from_dt = datetime.fromtimestamp(trade["openTimestamp"]) - timedelta(minutes=REPLAY_MINUTES_BEFORE)
    to_dt = datetime.fromtimestamp(trade["closeTimestamp"]) + timedelta(minutes=REPLAY_MINUTES_AFTER)

    bars = fetch_trendbars_as_bars(session, ctid_trader_account_id, symbol_id, digits, from_dt, to_dt)
    if not bars:
        return None

    ticket_key = str(trade["ticket"])
    events = list(timeline.get(ticket_key, []))
    # Always make sure a "closed" event is present and exact - the live
    # poller never sees a position disappear WITH a "closed" flag (it
    # just vanishes from the reconcile response), so it's added here
    # once the trade is confirmed closed via history, same as MT5.
    if not any(e.get("type") == "closed" for e in events):
        events.append({
            "time": trade["closeTimestamp"],
            "type": "closed",
            "price": trade.get("exitPrice"),
            "accountLogin": trade.get("accountLogin"),
        })
    events.sort(key=lambda e: e["time"])

    return {
        "symbol": trade["symbol"],
        "timeframe": "M1",
        "entryTime": trade["openTimestamp"],
        "exitTime": trade["closeTimestamp"],
        "entryPrice": trade.get("entryPrice"),
        "exitPrice": trade.get("exitPrice"),
        "sl": trade.get("sl"),
        "tp": trade.get("tp"),
        "direction": trade.get("direction"),
        "accountLogin": trade.get("accountLogin"),
        "bars": bars,
        "events": events,
    }


if __name__ == "__main__":
    # Smoke test: run ctrader_history.py first (or reuse its saved
    # trades file) then fetch replay candles for whichever trades don't
    # have them yet - mirrors the CLOSED_TRADE_CHECK_EVERY candle block
    # in mt5_sync_bridge.py's main(), as a one-shot run.
    from ctrader_auth import TokenStore
    from ctrader_backend import CTraderSession
    from ctrader_history import build_symbol_map, load_existing as load_trades

    sync_dir = os.path.dirname(os.path.abspath(__file__))
    token_store = TokenStore(sync_dir)
    access_token = token_store.ensure_fresh()

    session = CTraderSession(access_token)
    accounts = session.list_accounts()
    if not accounts:
        raise SystemExit("No cTrader accounts found for this login.")

    account = accounts[0]
    ctid = account["ctidTraderAccountId"]
    account_login = account["traderLogin"]

    trades_file = os.path.join(sync_dir, f"ctrader_trades_{account_login}.json")
    candles_file = os.path.join(sync_dir, f"ctrader_candles_{account_login}.json")
    timeline_file = os.path.join(sync_dir, f"ctrader_timeline_{account_login}.json")

    all_trades = load_trades(trades_file)
    if not all_trades:
        raise SystemExit(f"No trades found in {trades_file} - run ctrader_history.py first.")

    timeline = load_timeline(timeline_file)

    symbol_name_to_id = {name: sid for sid, name in build_symbol_map(session, ctid).items()}
    all_candles = load_existing_candles(candles_file)

    needed_symbol_ids = {
        symbol_name_to_id[t["symbol"]]
        for t in all_trades.values()
        if t["ticket"] not in all_candles and t["symbol"] in symbol_name_to_id
    }
    digits_by_symbol = fetch_symbol_digits(session, ctid, list(needed_symbol_ids))

    added = 0
    for ticket, trade in all_trades.items():
        if ticket in all_candles:
            continue
        symbol_id = symbol_name_to_id.get(trade["symbol"])
        if symbol_id is None:
            continue
        candle_data = fetch_candles_for_trade(session, ctid, trade, timeline, symbol_id, digits_by_symbol.get(symbol_id, 5))
        if candle_data:
            all_candles[ticket] = candle_data
            added += 1

    if added:
        save_candles(candles_file, all_candles)
    print(f"+{added} trade(s) with replay data. Total with candles: {len(all_candles)}. Saved to {candles_file}")
