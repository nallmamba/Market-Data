"""
ctrader_history.py
-------------------
Pulls closed-trade history from cTrader and turns it into the same
trade-record shape mt5_sync_bridge.py's fetch_closed_trades() produces,
so the journal's front end can read either platform's output without
caring which one it came from.

WHY THIS LOOKS DIFFERENT FROM fetch_closed_trades() IN THE MT5 SCRIPT:

MT5 hands you ready-made "deals" that already say IN or OUT (opening or
closing) via `deal.entry`. cTrader doesn't - instead, a ProtoOADeal that
CLOSED (all or part of) a position carries a populated
`closePositionDetail` field; a deal that OPENED one doesn't. So instead
of grouping by `d.entry == mt5.DEAL_ENTRY_IN/OUT`, this groups by
"does closePositionDetail exist".

Two more real differences worth knowing about, so nothing here looks
like a bug later:

  - MONEY FIELDS ARE SCALED INTEGERS. grossProfit/swap/commission/
    balance on closePositionDetail are int64, stored as the real value
    times 10**moneyDigits (moneyDigits is usually 2, i.e. cents) - NOT
    already-decimal like MT5's deal.profit. Every money value here is
    divided by 10**moneyDigits before use. Price fields (executionPrice,
    entryPrice) are plain doubles already, no scaling needed.

  - VOLUME AND LOT SIZE ARE BOTH IN HUNDREDTHS OF A UNIT, AND THE /100
    CANCELS OUT. deal.volume and ProtoOASymbol.lotSize are both scaled
    the same way (cTrader's own forum confirms this - "Open API
    returns Symbol min/max/step volumes in cents"), so
    lots = volume_cents / lotSize_cents gives the platform's own "lots"
    number directly, with no need to separately divide either side by
    100 first - that division would cancel out anyway, and skipping it
    sidesteps a real edge case: some instruments (e.g. index CFDs) have
    a minVolume that looks wrong if you divide it by 100 in isolation
    (100 cents reading as "1.00" when the platform shows a minimum
    trade size of 1 whole contract, not 0.01) - dividing by lotSize
    directly instead of by a hardcoded 100 avoids inheriting that trap.
    fetch_symbol_lot_sizes() below fetches the real lotSize per symbol
    via ProtoOASymbolByIdReq (same call ctrader_candles.py already
    makes for `digits` - a small duplicated request for now, worth
    merging into one call whenever the shared-core refactor happens).
    If a symbol's lot size wasn't fetched (caller passed nothing, or
    this particular symbol was missing from the batch), lots falls
    back to the FX-standard assumption and is very likely wrong for
    non-FX instruments - this fallback existing at all is the one
    remaining rough edge here, not the calculation itself.

WHAT'S DELIBERATELY NOT HERE YET (matches this being one step, not the
whole bridge):
  - sl / tp on each trade record are always None. MT5 can reconstruct
    these after the fact from order history; cTrader's deal history has
    no SL/TP fields at all - those only exist on the LIVE position
    object (ProtoOAPosition.stopLoss/takeProfit) or in execution
    events. Getting them means the live poll_live_events()-equivalent,
    which watches positions in real time the same way the MT5 script
    does - that's a separate, later step.
  - comment is always "" for the same reason (ProtoOADeal has no
    comment field; it lives on the order, not the deal).
  - No candle/replay fetching yet (ProtoOAGetTrendbarsReq) - that
    mirrors fetch_candles_for_trade() and comes after live events.
"""

import os
from datetime import datetime, timedelta

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOADealListReq,
    ProtoOASymbolsListReq,
    ProtoOASymbolByIdReq,
)
from ctrader_core import load_trades as load_existing, save_trades

MAX_ROWS_PER_REQUEST = 1000  # cTrader caps rows per ProtoOADealListReq; page with fromTimestamp if you hit this
DEFAULT_LOT_SIZE_CENTS = 100_000 * 100  # FX-standard fallback (100,000 units) - see module docstring caveat


def fetch_symbol_lot_sizes(session, ctid_trader_account_id, symbol_ids):
    """
    Returns {symbolId: lotSize_in_cents} for the given symbol IDs
    (batched into one request) - the real per-symbol contract size, so
    `lots` in fetch_closed_trades() is correct for indices/crypto/
    metals too, not just FX. See module docstring for why this stays
    in raw "cents" units rather than pre-dividing by 100.
    """
    if not symbol_ids:
        return {}
    conn = session.connection_for_account(ctid_trader_account_id)
    request = ProtoOASymbolByIdReq()
    request.ctidTraderAccountId = ctid_trader_account_id
    request.symbolId.extend(symbol_ids)
    response = conn.call(request)
    return {s.symbolId: s.lotSize for s in response.symbol}


def build_symbol_map(session, ctid_trader_account_id):
    """
    Returns {symbolId: symbolName} for every symbol on this account, so
    trade records can show "EURUSD" instead of a bare numeric ID.
    Cheap enough to call once per sync cycle - the symbol list rarely
    changes.
    """
    conn = session.connection_for_account(ctid_trader_account_id)
    request = ProtoOASymbolsListReq()
    request.ctidTraderAccountId = ctid_trader_account_id
    response = conn.call(request)
    return {s.symbolId: s.symbolName for s in response.symbol}


def _money(raw_value, money_digits):
    """Converts a scaled integer money field (see module docstring) to a real decimal amount."""
    return raw_value / (10 ** money_digits) if money_digits else float(raw_value)


def fetch_closed_trades(session, ctid_trader_account_id, account_login, from_date, symbol_map, lot_size_map=None):
    """
    Pulls closing deals since `from_date` and turns each into one trade
    record, keyed by position ID (as a string) - same shape and same
    key convention as MT5's fetch_closed_trades(), so downstream code
    doesn't need a platform-specific branch.

    `account_login` is the human-facing account number (cTrader's
    traderLogin) to stamp onto each record - kept separate from
    ctid_trader_account_id, which is only the API's internal handle for
    routing requests to the right connection.

    `lot_size_map` is optional {symbolId: lotSize_in_cents} from
    fetch_symbol_lot_sizes() - pass it whenever you have it so `lots`
    is exact instead of the FX-only fallback (see module docstring).
    """
    conn = session.connection_for_account(ctid_trader_account_id)

    request = ProtoOADealListReq()
    request.ctidTraderAccountId = ctid_trader_account_id
    request.fromTimestamp = int(from_date.timestamp() * 1000)
    request.toTimestamp = int((datetime.now() + timedelta(days=1)).timestamp() * 1000)
    request.maxRows = MAX_ROWS_PER_REQUEST
    response = conn.call(request)

    # Group by position: deals WITHOUT closePositionDetail are opens,
    # deals WITH it are closes (see module docstring - cTrader has no
    # MT5-style entry IN/OUT flag, this field is the equivalent signal).
    opens_by_position = {}
    closes_by_position = {}
    for deal in response.deal:
        if deal.dealStatus != 2:  # 2 == FILLED; skip rejected/missed/partially-filled-in-progress deals
            continue
        if deal.HasField("closePositionDetail"):
            closes_by_position.setdefault(deal.positionId, []).append(deal)
        else:
            opens_by_position.setdefault(deal.positionId, deal)

    results = {}
    for position_id, close_deals in closes_by_position.items():
        last_close = max(close_deals, key=lambda d: d.executionTimestamp)
        open_deal = opens_by_position.get(position_id)
        detail = last_close.closePositionDetail
        money_digits = detail.moneyDigits

        total_pnl = sum(
            _money(d.closePositionDetail.grossProfit, d.closePositionDetail.moneyDigits)
            + _money(d.closePositionDetail.swap, d.closePositionDetail.moneyDigits)
            + _money(d.closePositionDetail.commission, d.closePositionDetail.moneyDigits)
            for d in close_deals
        )
        total_volume_cents = sum(d.filledVolume for d in close_deals)

        # tradeSide: 1=BUY, 2=SELL (see ProtoOATradeSide). If we saw the
        # opening deal, its side IS the direction. Otherwise (position
        # opened before this sync window) infer from the closing side,
        # same logic as the MT5 script's fallback: a closing BUY means
        # the position was originally SELL, and vice versa.
        if open_deal is not None:
            direction = "Long" if open_deal.tradeSide == 1 else "Short"
        else:
            direction = "Short" if last_close.tradeSide == 1 else "Long"

        symbol_id = open_deal.symbolId if open_deal is not None else last_close.symbolId
        lot_size_cents = (lot_size_map or {}).get(symbol_id, DEFAULT_LOT_SIZE_CENTS)

        results[str(position_id)] = {
            "ticket": position_id,
            "accountLogin": account_login,
            "symbol": symbol_map.get(symbol_id, f"symbol#{symbol_id}"),
            "direction": direction,
            "lots": round(total_volume_cents / lot_size_cents, 2),
            "openTime": (
                datetime.fromtimestamp(open_deal.executionTimestamp / 1000).strftime("%Y-%m-%d")
                if open_deal else ""
            ),
            "closeTime": datetime.fromtimestamp(last_close.executionTimestamp / 1000).strftime("%Y-%m-%d"),
            "openTimestamp": int(open_deal.executionTimestamp / 1000) if open_deal else None,
            "closeTimestamp": int(last_close.executionTimestamp / 1000),
            "entryPrice": detail.entryPrice if detail.entryPrice else (open_deal.executionPrice if open_deal else None),
            "exitPrice": last_close.executionPrice,
            "sl": None,  # not available from closed-deal history - see module docstring
            "tp": None,  # not available from closed-deal history - see module docstring
            "pnl": round(total_pnl, 2),
            "comment": "",  # ProtoOADeal has no comment field - see module docstring
        }

    return results


if __name__ == "__main__":
    # Smoke test: log in, pick the first account, pull 30 days of closed
    # trades, merge with whatever's already on disk, and save -
    # mirroring what main()'s startup + CLOSED_TRADE_CHECK_EVERY block
    # do in mt5_sync_bridge.py, just as a one-shot run instead of a
    # polling loop (the live loop is the next step).
    from ctrader_auth import TokenStore
    from ctrader_backend import CTraderSession

    LOOKBACK_DAYS_ON_START = 30

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
    print(f"Syncing account #{account_login} (ctid {ctid})...")

    symbol_map = build_symbol_map(session, ctid)
    lot_size_map = fetch_symbol_lot_sizes(session, ctid, list(symbol_map.keys()))
    from_date = datetime.now() - timedelta(days=LOOKBACK_DAYS_ON_START)
    fresh = fetch_closed_trades(session, ctid, account_login, from_date, symbol_map, lot_size_map)

    trades_file = os.path.join(sync_dir, f"ctrader_trades_{account_login}.json")
    all_trades = load_existing(trades_file)
    new_tickets = [t for t in fresh if t not in all_trades]
    all_trades.update(fresh)
    save_trades(trades_file, all_trades)

    print(f"+{len(new_tickets)} new closed trade(s). Total tracked: {len(all_trades)}. Saved to {trades_file}")
