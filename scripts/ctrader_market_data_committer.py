"""
ctrader_market_data_committer.py
-----------------------------------
Commits the puller's {repo_path: bars} output to GitHub and updates
manifest.json - the last piece of Thread C's scheduled market-data
puller.

WHY THIS DOESN'T LITERALLY MIRROR worker.js's commitFileViaGitData():

That function commits ONE file per call, via GitHub's lower-level Git
Data API (blob -> tree -> commit -> ref update, 6 requests) rather
than the simpler Contents API - specifically to avoid Cloudflare
Workers' CPU-time limit when base64-encoding a large file in
JavaScript (GitHub does the encoding server-side via encoding:
"utf-8" instead, sidestepping that limit entirely). That constraint
doesn't apply here - this runs as a plain Python script (a GitHub
Actions job), with no CPU-time cap, and Python's base64 module
encodes instantly even for a large file.

What DOES carry over, and matters even more here: a scheduled backfill
run can easily produce dozens of files across several instruments in
one run (a new gap spanning several months, several pairs needing
catch-up at once). Using the Git Data API's tree step to bundle ALL of
them into ONE commit - rather than one Contents-API commit per file,
which is what updateManifest() does for its own single incremental
add - is a genuine improvement for THIS use case specifically, not
just a port with the numbers changed: one commit instead of dozens
means one entry in the repo's history for "today's backfill run"
instead of a flood of individual single-file commits, and roughly 5
total GitHub API calls for the whole batch rather than 2-3 PER FILE.

manifest.json is updated as part of the SAME commit (one more entry in
the same tree), not a separate commit afterward - so the data files
and the manifest describing them are never observably out of sync,
not even for the instant between two separate commits.
"""

import base64
import json
import time

import requests

GITHUB_API_BASE = "https://api.github.com"
DEFAULT_TIMEOUT_SECONDS = 60  # month files can be several MB
MAX_RETRIES = 3
MAX_REF_RETRIES = 3


def _github_headers(github_token):
    return {
        "Authorization": f"Bearer {github_token}",
        "User-Agent": "ctrader-market-data-puller",
        "Accept": "application/vnd.github+json",
    }


def _request_with_retry(method, url, github_token, log=print, **kwargs):
    """
    Same retry shape as worker.js's githubRawRequest(): retries a
    403/429 (rate limit) with backoff (honoring a Retry-After header
    when GitHub sends one), and retries a genuine network error, up to
    MAX_RETRIES times. Any other non-2xx status is returned as-is for
    the caller to raise_for_status() on - not every failure here is
    transient or worth retrying blindly.
    """
    headers = _github_headers(github_token)
    headers.update(kwargs.pop("headers", {}))
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS, **kwargs)
        except requests.RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise
            log(f"GitHub request error ({exc}), retrying...")
            time.sleep(2 ** attempt)
            continue

        if response.status_code in (403, 429) and attempt < MAX_RETRIES:
            retry_after = response.headers.get("retry-after")
            wait_seconds = float(retry_after) if retry_after else float(2 ** attempt)
            log(f"GitHub rate-limited (HTTP {response.status_code}), waiting {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        return response
    raise RuntimeError(f"Exhausted retries for {method} {url}")  # unreachable - the loop above always returns or raises first


def fetch_manifest(owner, repo, branch, github_token, log=print):
    """
    Mirrors worker.js's fetchManifest(): returns (files, sha). A 404
    means no upload has ever happened - treated as an empty list with
    no sha, same as the Worker does, so the very first commit this
    makes creates manifest.json rather than erroring.
    """
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/manifest.json"
    response = _request_with_retry("GET", url, github_token, log, params={"ref": branch})
    if response.status_code == 404:
        return [], None
    response.raise_for_status()
    data = response.json()
    content = json.loads(base64.b64decode(data["content"]).decode("utf-8"))
    files = content.get("files")
    if not isinstance(files, list):
        raise ValueError("manifest.json's content is missing a 'files' array.")
    return files, data["sha"]


def commit_files_and_update_manifest(owner, repo, branch, github_token, files_by_path, commit_message, log=print,
                                     extra_files=None, delete_paths=None):
    """
    `files_by_path`: {repo_path: bars_list} - data files; also added to manifest.json.
    `extra_files`: {repo_path: text} - other files committed as-is (e.g. state.json), NOT added to the manifest.
    `delete_paths`: repo paths removed in the same commit (e.g. processed queue files).

    Everything lands in ONE commit, manifest.json included, so data and
    manifest are never observably out of sync. Returns the new commit sha,
    or None if there was nothing to commit.

    If the branch moved while we were working (e.g. the Worker added a
    queue file), the ref update is rejected with 422; we then rebuild the
    tree on the new tip and try again (up to MAX_REF_RETRIES times).
    """
    extra_files = extra_files or {}
    delete_paths = list(delete_paths or [])
    if not files_by_path and not extra_files and not delete_paths:
        log("Nothing to commit.")
        return None

    base_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"

    # Blobs are created once, up front; they don't depend on the branch tip.
    entries = []
    for repo_path, bars in files_by_path.items():
        blob_res = _request_with_retry(
            "POST", f"{base_url}/git/blobs", github_token, log,
            json={"content": json.dumps(bars, separators=(",", ":")), "encoding": "utf-8"},
        )
        blob_res.raise_for_status()
        entries.append({"path": repo_path, "mode": "100644", "type": "blob", "sha": blob_res.json()["sha"]})
        log(f"Created blob for {repo_path} ({len(bars)} bars).")
    for repo_path, text in extra_files.items():
        blob_res = _request_with_retry(
            "POST", f"{base_url}/git/blobs", github_token, log,
            json={"content": text, "encoding": "utf-8"},
        )
        blob_res.raise_for_status()
        entries.append({"path": repo_path, "mode": "100644", "type": "blob", "sha": blob_res.json()["sha"]})
    for repo_path in delete_paths:
        entries.append({"path": repo_path, "mode": "100644", "type": "blob", "sha": None})

    for attempt in range(MAX_REF_RETRIES + 1):
        ref_res = _request_with_retry("GET", f"{base_url}/git/ref/heads/{branch}", github_token, log)
        ref_res.raise_for_status()
        parent_commit_sha = ref_res.json()["object"]["sha"]

        commit_res = _request_with_retry("GET", f"{base_url}/git/commits/{parent_commit_sha}", github_token, log)
        commit_res.raise_for_status()
        base_tree_sha = commit_res.json()["tree"]["sha"]

        tree_entries = list(entries)
        if files_by_path:
            # Fresh manifest on every attempt, so a concurrent commit's entries are never lost.
            existing_files, _ = fetch_manifest(owner, repo, branch, github_token, log)
            updated_files = sorted(set(existing_files) | set(files_by_path.keys()))
            manifest_content = json.dumps(
                {"files": updated_files, "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                indent=2,
            )
            m_res = _request_with_retry(
                "POST", f"{base_url}/git/blobs", github_token, log,
                json={"content": manifest_content, "encoding": "utf-8"},
            )
            m_res.raise_for_status()
            tree_entries.append({"path": "manifest.json", "mode": "100644", "type": "blob", "sha": m_res.json()["sha"]})

        tree_res = _request_with_retry(
            "POST", f"{base_url}/git/trees", github_token, log,
            json={"base_tree": base_tree_sha, "tree": tree_entries},
        )
        tree_res.raise_for_status()

        new_commit_res = _request_with_retry(
            "POST", f"{base_url}/git/commits", github_token, log,
            json={"message": commit_message, "tree": tree_res.json()["sha"], "parents": [parent_commit_sha]},
        )
        new_commit_res.raise_for_status()
        new_commit_sha = new_commit_res.json()["sha"]

        update_ref_res = _request_with_retry(
            "PATCH", f"{base_url}/git/refs/heads/{branch}", github_token, log,
            json={"sha": new_commit_sha},
        )
        if update_ref_res.status_code == 422 and attempt < MAX_REF_RETRIES:
            log("Branch moved while committing; rebuilding on the new tip...")
            continue
        update_ref_res.raise_for_status()

        log(f"Committed {len(files_by_path)} data file(s), {len(extra_files)} extra, {len(delete_paths)} deletion(s) ({new_commit_sha[:7]}).")
        return new_commit_sha
    raise RuntimeError("Could not update the branch after retries.")  # unreachable
