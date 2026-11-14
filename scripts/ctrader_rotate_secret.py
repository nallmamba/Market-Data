"""
Writes the (rotated) cTrader refresh token back into the repo's
CTRADER_REFRESH_TOKEN Actions secret.

WHY: cTrader invalidates the old refresh token the moment a refresh
succeeds. The sync run saves the new one to $RUNNER_TEMP/ctrader_token.json,
but that file dies with the runner - so without this step the secret goes
stale after the first run and every later run fails.

Needs env: SECRETS_PAT (fine-grained PAT, this repo only, permission
"Secrets: read and write"), GITHUB_REPOSITORY, RUNNER_TEMP.
Safe no-op if the token file doesn't exist or the token is unchanged.
"""
import base64, json, os, sys
import requests
from nacl import encoding, public

SECRET_NAME = "CTRADER_REFRESH_TOKEN"


def main():
    path = os.path.join(os.environ.get("RUNNER_TEMP", "."), "ctrader_token.json")
    if not os.path.exists(path):
        print("No token file written this run; nothing to rotate.")
        return 0
    with open(path, "r", encoding="utf-8") as f:
        new_token = json.load(f).get("refresh_token")
    if not new_token:
        print("Token file has no refresh_token; nothing to rotate.")
        return 0
    if new_token == os.environ.get("CTRADER_REFRESH_TOKEN"):
        print("Refresh token unchanged; nothing to rotate.")
        return 0

    repo = os.environ["GITHUB_REPOSITORY"]
    headers = {
        "Authorization": f"Bearer {os.environ['SECRETS_PAT']}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    base = f"https://api.github.com/repos/{repo}/actions/secrets"
    r = requests.get(f"{base}/public-key", headers=headers, timeout=30)
    r.raise_for_status()
    key = r.json()
    box = public.SealedBox(public.PublicKey(key["key"].encode(), encoding.Base64Encoder()))
    encrypted = base64.b64encode(box.encrypt(new_token.encode())).decode()
    r = requests.put(
        f"{base}/{SECRET_NAME}",
        headers=headers,
        json={"encrypted_value": encrypted, "key_id": key["key_id"]},
        timeout=30,
    )
    r.raise_for_status()
    print(f"Updated {SECRET_NAME} secret with the rotated refresh token.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
