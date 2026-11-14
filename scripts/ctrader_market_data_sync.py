"""
ctrader_market_data_sync.py
------------------------------
Entry point, run by GitHub Actions (see .github/workflows/ctrader-market-data-sync.yml).

SYNC_MODE (env):
  nightly   (default) fill missing months, finalize ended months, top up the current
            month for "live" instruments, drain the replay queue.
  ondemand  only the instruments in the replay queue (started by the Cloudflare Worker).
  audit     nightly + a damage scan of every past month file (needs DATA_DIR = a
            full checkout of the data repo).

REQUIRED ENV: GITHUB_DATA_OWNER, GITHUB_DATA_REPO, GITHUB_TOKEN, CTRADER_ACCOUNT_ID,
              CTRADER_REFRESH_TOKEN (+ CTRADER_CLIENT_ID/SECRET, read by ctrader_auth.py).
OPTIONAL ENV: GITHUB_DATA_BRANCH (main), DATA_DIR, SYMBOL_ALIASES (JSON, e.g.
              {"us30":"US30Cash"}), CURRENT_MONTH_INSTRUMENTS (comma list always kept live),
              ALLOW_NEW_INSTRUMENTS (1/0), LIVE_RETENTION_DAYS (45), MAX_WAIT_SECONDS (420).

ONE-TIME SETUP: see README.md (the first login is done by hand once to get a refresh token;
the workflow then saves each rotated token back into the CTRADER_REFRESH_TOKEN secret).

Every timestamp this script reads or writes is UTC.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

from ctrader_auth import TokenStore
from ctrader_backend import CTraderSession
from ctrader_candles import fetch_symbol_digits, fetch_trendbars_as_bars
from ctrader_history import build_symbol_map
from ctrader_market_data_committer import commit_files_and_update_manifest, fetch_manifest
from ctrader_market_data_current import drop_forming_bar
from ctrader_market_data_gaps import parse_manifest_files
from ctrader_market_data_puller import build_symbol_lookup
from ctrader_market_data_repoio import STATE_PATH, read_json, read_queue, read_state
from ctrader_market_data_runner import run


def _required_env(name):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _flag(name, default=False):
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().lower() in ("1", "true", "yes")


def require_demo_account(session, account_id):
    account = next(
        (a for a in session.list_accounts() if a["ctidTraderAccountId"] == account_id),
        None,
    )
    if account is None:
        raise SystemExit(f"Configured cTrader account #{account_id} is not available to this token.")
    if account.get("isLive") is not False:
        raise SystemExit(
            f"Configured cTrader account #{account_id} is not confirmed as a demo account. "
            "The market-data puller only accepts demo accounts."
        )
    return account


def _wait_for_queue(queue, max_wait):
    """
    A replay request asks for data up to a few minutes AFTER the trade closed.
    If that moment hasn't happened yet, wait for it (bounded) instead of
    pulling a window that can't be complete.
    """
    needs = [int(q["needUntil"]) for q in queue if str(q.get("needUntil", "")).lstrip("-").isdigit()]
    if not needs:
        return
    wait = min(max(needs) + 60 - time.time(), max_wait)
    if wait > 0:
        print(f"Waiting {int(wait)}s for the requested window to finish...")
        time.sleep(wait)


def main():
    mode = os.environ.get("SYNC_MODE", "nightly").strip().lower()
    if mode not in ("nightly", "ondemand", "audit"):
        raise SystemExit(f"Unknown SYNC_MODE {mode!r}")
    owner = _required_env("GITHUB_DATA_OWNER")
    repo = _required_env("GITHUB_DATA_REPO")
    branch = os.environ.get("GITHUB_DATA_BRANCH", "main")
    gh_token = _required_env("GITHUB_TOKEN")
    account_id = int(_required_env("CTRADER_ACCOUNT_ID"))
    refresh_token = _required_env("CTRADER_REFRESH_TOKEN")
    data_dir = os.environ.get("DATA_DIR")
    print(f"Mode: {mode}")

    existing_files, _ = fetch_manifest(owner, repo, branch, gh_token)
    state = read_state(owner, repo, branch, gh_token)
    queue = read_queue(owner, repo, branch, gh_token)
    print(f"{len(existing_files)} file(s) in manifest, {len(queue)} queued request(s).")

    if mode == "ondemand":
        if not queue:
            print("Queue is empty; nothing to do.")
            return
        _wait_for_queue(queue, int(os.environ.get("MAX_WAIT_SECONDS", "420")))

    token_store = TokenStore(os.environ.get("RUNNER_TEMP", "."), seed_refresh_token=refresh_token, allow_interactive_login=False)
    session = CTraderSession(token_store.ensure_fresh())

    require_demo_account(session, account_id)
    symbol_map = build_symbol_map(session, account_id)
    lookup = build_symbol_lookup(symbol_map)
    for code, broker_name in json.loads(os.environ.get("SYMBOL_ALIASES", "{}")).items():
        if broker_name.lower() in lookup:
            lookup[code.lower()] = lookup[broker_name.lower()]
        else:
            print(f"  Warning: alias {code!r} -> {broker_name!r}: broker has no such symbol.")

    forced = tuple(c.strip().lower() for c in os.environ.get("CURRENT_MONTH_INSTRUMENTS", "").split(",") if c.strip())
    wanted = set(parse_manifest_files(existing_files)) | set(forced) | {str(q.get("instrument", "")).lower() for q in queue}
    ids = [lookup[c] for c in wanted if c in lookup]
    digits = fetch_symbol_digits(session, account_id, ids)

    def fetch_bars(instrument, from_dt, to_dt):
        symbol_id = lookup[instrument]
        bars = fetch_trendbars_as_bars(session, account_id, symbol_id, digits.get(symbol_id, 5), from_dt, to_dt)
        return drop_forming_bar(bars, time.time())

    def read_bars(path):
        if data_dir:
            local = os.path.join(data_dir, path)
            if os.path.exists(local):
                with open(local, "r", encoding="utf-8") as f:
                    return json.load(f)
            return None
        return read_json(owner, repo, branch, gh_token, path)

    config = {
        "allow_new_instruments": _flag("ALLOW_NEW_INSTRUMENTS"),
        "live_retention_days": int(os.environ.get("LIVE_RETENTION_DAYS", "45")),
        "forced_live": forced,
    }
    result = run(mode, datetime.now(timezone.utc), existing_files, state, queue,
                 read_bars, fetch_bars, lambda i: i in lookup, config)

    new_state_text = json.dumps(result["state"], indent=1, sort_keys=True)
    extra = {STATE_PATH: new_state_text} if result["state"] != _norm(state) else {}
    sha = commit_files_and_update_manifest(
        owner, repo, branch, gh_token, result["files"],
        commit_message=f"Auto-pull ({mode}): {len(result['files'])} file(s)",
        extra_files=extra, delete_paths=sorted(result["done_queue_ids"]),
    )
    print(f"Done. Commit: {sha}")
    if result["errors"]:
        print(f"{len(result['errors'])} error(s) during the run:")
        for e in result["errors"]:
            print("  -", e)
        sys.exit(1)


def _norm(state):
    out = dict(state or {})
    for key in ("live", "partial", "audit", "unknown"):
        out[key] = dict(out.get(key) or {})
    return out


if __name__ == "__main__":
    main()
