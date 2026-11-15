"""
ctrader_market_data_repoio.py
-------------------------------
Small GitHub read helpers for the data repo: the on-demand queue
(queue/*.json), state.json, and reading one existing month file.
Uses the same retrying request helper as the committer.
"""

import json

from ctrader_market_data_committer import _request_with_retry, GITHUB_API_BASE

QUEUE_DIR = "queue"
STATE_PATH = "state.json"
RAW = {"Accept": "application/vnd.github.raw+json"}


def read_text(owner, repo, branch, token, path, log=print):
    """File contents as text, or None if the file doesn't exist."""
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{path}"
    res = _request_with_retry("GET", url, token, log, params={"ref": branch}, headers=RAW)
    if res.status_code == 404:
        return None
    res.raise_for_status()
    return res.text


def read_json(owner, repo, branch, token, path, log=print):
    text = read_text(owner, repo, branch, token, path, log)
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        log(f"  Warning: {path} is not valid JSON; treating as missing.")
        return None


def read_state(owner, repo, branch, token, log=print):
    state = read_json(owner, repo, branch, token, STATE_PATH, log)
    return state if isinstance(state, dict) else {}


def read_queue(owner, repo, branch, token, log=print):
    """
    Every request file in queue/, as dicts with an added "id" (the file's
    repo path - also what gets deleted once the request is satisfied).
    Unreadable files are returned as {"id": path} (no fields) so the runner
    drops them as malformed instead of retrying them forever.
    """
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{QUEUE_DIR}"
    res = _request_with_retry("GET", url, token, log, params={"ref": branch})
    if res.status_code == 404:
        return []
    res.raise_for_status()
    listing = res.json()
    if not isinstance(listing, list):
        return []
    requests_out = []
    for item in listing:
        if item.get("type") != "file" or not item["name"].endswith(".json"):
            continue
        path = item["path"]
        data = read_json(owner, repo, branch, token, path, log)
        entry = dict(data) if isinstance(data, dict) else {}
        entry["id"] = path
        requests_out.append(entry)
    return requests_out
