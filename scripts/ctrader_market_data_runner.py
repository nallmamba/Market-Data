"""
ctrader_market_data_runner.py
-------------------------------
The decision logic for all three run modes, with NO network or cTrader
code inside it: everything it needs from the outside world is passed in
as plain callables, so it can be tested with fakes.

MODES
  nightly   Fill missing completed months; finalize any partial month
            that has now ended; top up the CURRENT month for every
            "live" instrument; drain the on-demand queue.
  ondemand  Same, but only for instruments named in the queue (fast).
  audit     Everything nightly does, PLUS scan every past month file for
            damage (see ctrader_market_data_integrity.py) and re-pull
            the damaged ones.

STATE (state.json in the data repo, committed with the data)
  live     {instrument: last_requested_epoch}  instruments whose current
           month is kept growing. Added when a replay request arrives;
           dropped after LIVE_RETENTION_DAYS without a new request.
  partial  {repo_path: {through, updatedAt}}   month files that are not
           final yet. When the month ends they are re-pulled in full once.
  audit    {repo_path: {sig, repulled, checkedAt}}  so a month that is
           legitimately thin is re-pulled once, not every month forever.
  unknown  {instrument: {count, last}}  requests for instruments the repo
           doesn't track (and ALLOW_NEW_INSTRUMENTS is off) - for you to
           review.

QUEUE REQUESTS  {id, instrument, needUntil (UTC epoch s), requestedAt}
A request is removed only once data has been pulled up to needUntil (or it
is malformed/stale/unknown). Otherwise it stays and the next run retries.
An explicit `backfillFrom` request starts a separately capped, resumable
history fill. Other on-demand runs fill only the requested historical month.
"""

import re
from datetime import datetime, timezone

from ctrader_market_data_current import data_path, merge_bars, plan_range
from ctrader_market_data_gaps import last_fully_completed_month, missing_months_by_instrument, parse_manifest_files, MONTHLY_FILE_PATTERN
from ctrader_market_data_integrity import find_suspects, signature
from ctrader_market_data_puller import month_bounds_utc

INSTRUMENT_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,31}$")

DEFAULT_CONFIG = {
    "live_retention_days": 45,
    "allow_new_instruments": False,
    "new_instrument_prior_months": 1,
    "audit_max_repulls": 40,
    "max_request_age_days": 3,
    "max_request_future_seconds": 15 * 60,
    "max_requested_at_future_seconds": 5 * 60,
    "max_backfill_months_per_run": 6,
    "max_gap_months_per_run": 24,
    "forced_live": (),
}

BACKFILL_START_RE = re.compile(r"^(\d{4})-(\d{2})-01$")


def _parse_backfill_month(value):
    if not isinstance(value, str):
        return None
    match = BACKFILL_START_RE.match(value)
    if not match:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    if month < 1 or month > 12 or (year, month) < (2020, 1):
        return None
    return year, month


def _month_string(year, month):
    return f"{year:04d}-{month:02d}-01"


def _next_month(year, month):
    return (year, month + 1) if month < 12 else (year + 1, 1)


def split_path(path):
    """'eurusd/eurusd_2026_11.json' -> ('eurusd', 2026, 11), or None."""
    if "/" not in path:
        return None
    folder, filename = path.split("/", 1)
    m = MONTHLY_FILE_PATTERN.match(filename)
    if not m:
        return None
    return folder.lower(), int(m.group("year")), int(m.group("month"))


def _normalise_state(state):
    state = dict(state or {})
    for key in ("live", "partial", "audit", "unknown", "backfills"):
        state[key] = dict(state.get(key) or {})
    return state


def run(mode, now, existing_files, state, queue, read_bars, fetch_bars, has_symbol, config=None, log=print):
    """
    Returns {"files": {repo_path: bars}, "state": new_state,
             "done_queue_ids": set, "errors": [str]}.
    `now` must be an aware UTC datetime.
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    state = _normalise_state(state)
    now_ts = int(now.timestamp())
    cur = (now.year, now.month)
    files, errors, done_ids = {}, [], set()
    known = parse_manifest_files(existing_files)

    def safe_fetch(instrument, from_dt, to_dt):
        try:
            return fetch_bars(instrument, from_dt, to_dt)
        except Exception as exc:  # one bad instrument/month must not kill the whole run
            errors.append(f"{instrument} {from_dt:%Y-%m-%d}..{to_dt:%Y-%m-%d}: {exc!r}")
            log(f"  ERROR pulling {instrument}: {exc!r}")
            return None

    def month_bars(instrument, year, month):
        path = data_path(instrument, year, month)
        if path in files:
            return files[path]
        if path in existing_files:
            return read_bars(path) or []
        return []

    def covers_time(bars, timestamp):
        return any(
            isinstance(bar, dict)
            and isinstance(bar.get("time"), (int, float))
            and bar["time"] + 60 >= timestamp
            for bar in bars
        )

    # --- 0. read the queue --------------------------------------------------
    req_by_inst = {}
    live_request_instruments = set()
    for q in queue:
        inst = str(q.get("instrument", "")).lower().strip()
        try:
            need = int(q.get("needUntil", 0))
            requested = int(q.get("requestedAt", need))
        except (TypeError, ValueError):
            need = 0
            requested = 0
        is_backfill = "backfillFrom" in q
        backfill_start = _parse_backfill_month(q.get("backfillFrom")) if is_backfill else None
        if not INSTRUMENT_RE.match(inst) or need <= 0:
            log(f"  Dropping malformed queue request {q.get('id')}")
            done_ids.add(q["id"])
            continue
        if is_backfill and (backfill_start is None or need < int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())):
            log(f"  Dropping invalid or pre-2020 backfill request {q.get('id')}")
            done_ids.add(q["id"])
            continue
        if need > now_ts + cfg["max_request_future_seconds"]:
            log(f"  Dropping queue request too far in the future {q.get('id')}")
            done_ids.add(q["id"])
            continue
        if requested > now_ts + cfg["max_requested_at_future_seconds"]:
            log(f"  Dropping queue request with a future requestedAt {q.get('id')}")
            done_ids.add(q["id"])
            continue
        if now_ts - requested > cfg["max_request_age_days"] * 86400:
            log(f"  Dropping stale queue request {q.get('id')}")
            done_ids.add(q["id"])
            continue
        if not is_backfill and now_ts - need > cfg["max_request_age_days"] * 86400:
            log(f"  Dropping queue request with stale needUntil {q.get('id')}")
            done_ids.add(q["id"])
            continue
        if (inst not in known and not cfg["allow_new_instruments"] and not is_backfill) or not has_symbol(inst):
            entry = state["unknown"].setdefault(inst, {"count": 0, "last": 0})
            entry["count"] += 1
            entry["last"] = now_ts
            log(f"  Request for untracked/unmatched instrument {inst!r} - recorded in state.unknown, not pulled.")
            done_ids.add(q["id"])
            continue
        req_by_inst.setdefault(inst, []).append(q)
        if need >= now_ts - cfg["live_retention_days"] * 86400:
            state["live"][inst] = now_ts
            live_request_instruments.add(inst)
        if is_backfill:
            existing_backfill = state["backfills"].get(inst)
            existing_backfill = existing_backfill if isinstance(existing_backfill, dict) else {}
            existing_start = _parse_backfill_month(existing_backfill.get("startMonth"))
            existing_next = _parse_backfill_month(existing_backfill.get("nextMonth"))
            start_month = min(backfill_start, existing_start) if existing_start else backfill_start
            next_month = min(backfill_start, existing_next) if existing_next else backfill_start
            state["backfills"][inst] = {
                **existing_backfill,
                "startMonth": _month_string(*start_month),
                "nextMonth": _month_string(*next_month),
                "requestedAt": max(now_ts, int(existing_backfill.get("requestedAt", 0) or 0)),
                "status": "pending",
                "noDataMonths": list(existing_backfill.get("noDataMonths") or []),
            }

    # --- 1. missing completed months ---------------------------------------
    if mode != "ondemand":
        gap_budget = max(0, int(cfg["max_gap_months_per_run"]))
        for inst, months in missing_months_by_instrument(existing_files, now.date()).items():
            backfill = state["backfills"].get(inst, {})
            if isinstance(backfill, dict) and backfill.get("status") == "pending":
                continue
            no_data_months = set(backfill.get("noDataMonths", [])) if isinstance(backfill, dict) else set()
            months = [month for month in months if _month_string(*month) not in no_data_months]
            if not months:
                continue
            if not has_symbol(inst):
                log(f"  No cTrader symbol for {inst} - skipping its {len(months)} missing month(s).")
                continue
            for y, m in months:
                if gap_budget <= 0:
                    log("  Completed-month gap cap reached; remaining gaps wait for the next run.")
                    break
                gap_budget -= 1
                f, t = month_bounds_utc(y, m)
                bars = safe_fetch(inst, f, t)
                if bars:
                    files[data_path(inst, y, m)] = bars
                    log(f"  Filled {data_path(inst, y, m)}: {len(bars)} bars.")
            if gap_budget <= 0:
                break

    if mode == "ondemand":
        for inst, reqs in req_by_inst.items():
            for q in reqs:
                need = int(q["needUntil"])
                requested_month = datetime.fromtimestamp(need, timezone.utc)
                if (requested_month.year, requested_month.month) >= cur:
                    continue
                bars = month_bars(inst, requested_month.year, requested_month.month)
                if covers_time(bars, need):
                    continue
                f, t = month_bounds_utc(requested_month.year, requested_month.month)
                fresh = safe_fetch(inst, f, t)
                if fresh is not None:
                    path = data_path(inst, requested_month.year, requested_month.month)
                    merged = merge_bars(bars, fresh)
                    if merged and merged != bars:
                        files[path] = merged
                        log(f"  Requested historical month {path}: {len(merged)} bars.")

    # --- 1b. resumable, bounded instrument backfills ----------------------
    backfill_budget = max(0, int(cfg["max_backfill_months_per_run"]))
    last_completed = last_fully_completed_month(now.date())
    backfill_order = sorted(
        state["backfills"].items(),
        key=lambda item: (int(item[1].get("requestedAt", 0) or 0), item[0])
        if isinstance(item[1], dict) else (0, item[0]),
    )
    for inst, raw_backfill in backfill_order:
        if not isinstance(raw_backfill, dict) or raw_backfill.get("status") != "pending":
            continue
        if not has_symbol(inst):
            log(f"  Backfill for {inst} is paused because no cTrader symbol mapping is available.")
            continue
        backfill = dict(raw_backfill)
        cursor = _parse_backfill_month(backfill.get("nextMonth")) or _parse_backfill_month(backfill.get("startMonth"))
        if cursor is None:
            log(f"  Backfill for {inst} has an invalid cursor; leaving it paused for review.")
            continue
        no_data = set(backfill.get("noDataMonths") or [])
        while cursor <= last_completed and backfill_budget > 0:
            year, month = cursor
            path = data_path(inst, year, month)
            if path not in existing_files and path not in files and _month_string(year, month) not in no_data:
                from_dt, to_dt = month_bounds_utc(year, month)
                bars = safe_fetch(inst, from_dt, to_dt)
                backfill_budget -= 1
                if bars is None:
                    break
                if bars:
                    files[path] = bars
                    log(f"  Backfill pulled {path}: {len(bars)} bars.")
                else:
                    no_data.add(_month_string(year, month))
                    log(f"  Backfill found no bars for {path}; recording the empty month and continuing.")
            cursor = _next_month(year, month)
            backfill["nextMonth"] = _month_string(*cursor)
            backfill["noDataMonths"] = sorted(no_data)
            state["backfills"][inst] = backfill

        backfill["status"] = "complete" if cursor > last_completed else "pending"
        backfill["nextMonth"] = _month_string(*cursor)
        backfill["noDataMonths"] = sorted(no_data)
        state["backfills"][inst] = backfill
        if backfill["status"] == "complete":
            log(f"  Historical backfill for {inst} is complete through {last_completed[0]}-{last_completed[1]:02d}.")
        elif backfill_budget <= 0:
            log("  Backfill month cap reached; remaining history will resume in a later run.")

    # --- 2. finalize partial months that have now ended --------------------
    partial_paths = [] if mode == "ondemand" else list(state["partial"])
    for path in partial_paths:
        parsed = split_path(path)
        if not parsed:
            state["partial"].pop(path)
            continue
        inst, y, m = parsed
        if (y, m) >= cur or not has_symbol(inst):
            continue
        f, t = month_bounds_utc(y, m)
        bars = safe_fetch(inst, f, t)
        if bars is None:
            continue  # keep it partial; try again next run
        files[path] = merge_bars(read_bars(path), bars)
        state["partial"].pop(path)
        log(f"  Finalized {path}: {len(files[path])} bars.")

    # --- 3. current-month top-up -------------------------------------------
    live_cutoff = now_ts - cfg["live_retention_days"] * 86400
    if mode == "ondemand":
        targets = live_request_instruments
    else:
        targets = {i for i, ts in state["live"].items() if ts >= live_cutoff}
        targets |= {i.lower() for i in cfg["forced_live"]}
        targets |= set(req_by_inst)
    state["live"] = {i: ts for i, ts in state["live"].items() if ts >= live_cutoff or i in req_by_inst}

    for inst in sorted(targets):
        backfill = state["backfills"].get(inst, {})
        backfill_is_active = isinstance(backfill, dict) and backfill.get("status") == "pending"
        if (inst not in known and not cfg["allow_new_instruments"] and not backfill_is_active) or not has_symbol(inst):
            continue
        if inst not in known and cfg["allow_new_instruments"]:  # explicitly allowed: a bounded initial sample
            y, m = cur
            for _ in range(cfg["new_instrument_prior_months"]):
                y, m = (y - 1, 12) if m == 1 else (y, m - 1)
                f, t = month_bounds_utc(y, m)
                bars = safe_fetch(inst, f, t)
                if bars:
                    files[data_path(inst, y, m)] = bars
        path = data_path(inst, *cur)
        existing = (read_bars(path) or []) if path in existing_files else []
        rng = plan_range(existing, cur[0], cur[1], now)
        merged = existing
        if rng:
            new = safe_fetch(inst, rng[0], rng[1])
            if new is None:
                continue
            merged = merge_bars(existing, new)
        if merged and merged != existing:
            files[path] = merged
            log(f"  Current month {path}: {len(merged)} bars (+{len(merged) - len(existing)}).")
        if merged:
            state["partial"][path] = {"through": merged[-1]["time"], "updatedAt": now_ts}

    # --- 4. resolve queue requests -----------------------------------------
    for inst, reqs in req_by_inst.items():
        for q in reqs:
            need = int(q["needUntil"])
            requested_month = datetime.fromtimestamp(need, timezone.utc)
            bars = month_bars(inst, requested_month.year, requested_month.month)
            if covers_time(bars, need):
                done_ids.add(q["id"])
            else:
                log(f"  Request {q['id']} not yet satisfied (needs data to {need}); keeping it queued.")

    # --- 5. monthly audit ---------------------------------------------------
    if mode == "audit":
        repulls = 0
        for inst, months in sorted(parse_manifest_files(existing_files).items()):
            for y, m in sorted(months):
                if (y, m) >= cur:
                    continue
                path = data_path(inst, y, m)
                if path in files or path in state["partial"]:
                    continue
                bars = read_bars(path)
                suspects = find_suspects(bars)
                sig = signature(suspects)
                prev = state["audit"].get(path)
                if not suspects:
                    state["audit"].pop(path, None)
                    continue
                if prev and prev.get("sig") == sig and prev.get("repulled"):
                    continue  # already tried; the broker's data is genuinely like this
                if repulls >= cfg["audit_max_repulls"]:
                    log("  Audit re-pull cap reached; the rest waits for the next audit.")
                    break
                if not has_symbol(inst):
                    continue
                repulls += 1
                log(f"  Audit: {path} looks damaged {suspects[:3]}; re-pulling.")
                f, t = month_bounds_utc(y, m)
                fresh = safe_fetch(inst, f, t)
                if fresh is None:
                    continue
                merged = merge_bars(bars, fresh)
                if merged != bars:
                    files[path] = merged
                state["audit"][path] = {
                    "sig": signature(find_suspects(merged)),
                    "repulled": True,
                    "checkedAt": now_ts,
                }
            else:
                continue
            break

    return {"files": files, "state": state, "done_queue_ids": done_ids, "errors": errors}
