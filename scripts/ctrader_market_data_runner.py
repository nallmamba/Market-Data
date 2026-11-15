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
On-demand runs fill only the requested historical month when needed, not
every gap in the instrument's full history.
"""

import re
from datetime import datetime, timezone

from ctrader_market_data_current import data_path, merge_bars, plan_range
from ctrader_market_data_gaps import missing_months_by_instrument, parse_manifest_files, MONTHLY_FILE_PATTERN
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
    "forced_live": (),
}


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
    for key in ("live", "partial", "audit", "unknown"):
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
    for q in queue:
        inst = str(q.get("instrument", "")).lower().strip()
        try:
            need = int(q.get("needUntil", 0))
            requested = int(q.get("requestedAt", need))
        except (TypeError, ValueError):
            need = 0
            requested = 0
        if not INSTRUMENT_RE.match(inst) or need <= 0:
            log(f"  Dropping malformed queue request {q.get('id')}")
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
        if now_ts - need > cfg["max_request_age_days"] * 86400:
            log(f"  Dropping queue request with stale needUntil {q.get('id')}")
            done_ids.add(q["id"])
            continue
        if (inst not in known and not cfg["allow_new_instruments"]) or not has_symbol(inst):
            entry = state["unknown"].setdefault(inst, {"count": 0, "last": 0})
            entry["count"] += 1
            entry["last"] = now_ts
            log(f"  Request for untracked/unmatched instrument {inst!r} - recorded in state.unknown, not pulled.")
            done_ids.add(q["id"])
            continue
        req_by_inst.setdefault(inst, []).append(q)
        state["live"][inst] = now_ts

    # --- 1. missing completed months ---------------------------------------
    if mode != "ondemand":
        for inst, months in missing_months_by_instrument(existing_files, now.date()).items():
            if not has_symbol(inst):
                log(f"  No cTrader symbol for {inst} - skipping its {len(months)} missing month(s).")
                continue
            for y, m in months:
                f, t = month_bounds_utc(y, m)
                bars = safe_fetch(inst, f, t)
                if bars:
                    files[data_path(inst, y, m)] = bars
                    log(f"  Filled {data_path(inst, y, m)}: {len(bars)} bars.")

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
        targets = set(req_by_inst)
    else:
        targets = {i for i, ts in state["live"].items() if ts >= live_cutoff}
        targets |= {i.lower() for i in cfg["forced_live"]}
        targets |= set(req_by_inst)
    state["live"] = {i: ts for i, ts in state["live"].items() if ts >= live_cutoff or i in req_by_inst}

    for inst in sorted(targets):
        if (inst not in known and not cfg["allow_new_instruments"]) or not has_symbol(inst):
            continue
        if inst not in known:  # brand-new instrument, explicitly allowed: a bounded backfill
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
