"""Verify cTrader demo authorization and fetch a small recent M1 sample."""

import os
import sys
from datetime import datetime, timedelta, timezone
from math import isfinite

from ctrader_auth import TokenStore
from ctrader_backend import CTraderSession
from ctrader_candles import fetch_symbol_digits, fetch_trendbars_as_bars
from ctrader_history import build_symbol_map
from ctrader_market_data_current import drop_forming_bar


def main():
    sync_dir = os.path.dirname(os.path.abspath(__file__))
    access_token = TokenStore(sync_dir).ensure_fresh()
    session = CTraderSession(access_token)

    demo_accounts = [account for account in session.list_accounts() if account.get("isLive") is False]
    if not demo_accounts:
        raise SystemExit("No cTrader demo account is available for this authorization.")

    account = demo_accounts[0]
    account_id = account["ctidTraderAccountId"]
    symbols = build_symbol_map(session, account_id)
    preferred_symbol = next(
        (name for name in symbols.values() if "EURUSD" in name.upper()),
        None,
    )
    requested_symbol = sys.argv[1] if len(sys.argv) > 1 else preferred_symbol
    symbol_ids = {name.casefold(): symbol_id for symbol_id, name in symbols.items()}
    symbol_id = symbol_ids.get(requested_symbol.casefold()) if requested_symbol else None
    if symbol_id is None:
        available = ", ".join(sorted(symbols.values())[:30])
        raise SystemExit(
            "Couldn't find the requested symbol{} on the demo account. "
            "Available symbols include: {}. Re-run with a symbol name as the argument.".format(
                f" {requested_symbol!r}" if requested_symbol else "",
                available or "(none)",
            )
        )

    digits = fetch_symbol_digits(session, account_id, [symbol_id])[symbol_id]
    now = datetime.now(timezone.utc)
    bars = fetch_trendbars_as_bars(
        session,
        account_id,
        symbol_id,
        digits,
        now - timedelta(days=3),
        now,
    )
    bars = drop_forming_bar(bars, now.timestamp())
    if not bars:
        raise SystemExit(
            f"cTrader returned no completed M1 candles for {requested_symbol}; "
            "check the symbol name and the demo account's history availability."
        )

    if any(
        not isinstance(bar, dict)
        or not all(
            isinstance(bar.get(key), (int, float)) and isfinite(bar[key])
            for key in ("time", "open", "high", "low", "close")
        )
        or any(bar[key] <= 0 for key in ("open", "high", "low", "close"))
        for bar in bars
    ):
        raise SystemExit("cTrader returned a candle with an invalid OHLC field.")
    if any(bars[index]["time"] >= bars[index + 1]["time"] for index in range(len(bars) - 1)):
        raise SystemExit("cTrader returned candles that are not strictly time-ordered.")

    start = datetime.fromtimestamp(bars[0]["time"], timezone.utc).isoformat()
    end = datetime.fromtimestamp(bars[-1]["time"], timezone.utc).isoformat()
    print("Demo account verified: isLive=false")
    print(f"Symbol: {symbols[symbol_id]}")
    print(f"Completed M1 candles fetched: {len(bars)}")
    print(f"UTC range: {start} .. {end}")
    print(f"Latest OHLC: {bars[-1]['open']}, {bars[-1]['high']}, {bars[-1]['low']}, {bars[-1]['close']}")


if __name__ == "__main__":
    main()
