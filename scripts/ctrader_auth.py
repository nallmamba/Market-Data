"""
ctrader_auth.py
----------------
Handles the one-time OAuth login and ongoing token refresh for the
cTrader Open API. This is the piece that replaces "MT5 terminal is
already logged in, just attach to it" - cTrader has no local terminal
to attach to, so the bridge has to get its own access token via a
browser-based login once, then keep it refreshed forever after.

THE FLOW, END TO END:

  1. First run (no saved token yet):
       - We start a tiny local HTTP server on 127.0.0.1:<REDIRECT_PORT>
         whose only job is to catch the redirect cTrader sends back
         after login.
       - We open the user's default browser to cTrader's own login/
         consent page (open_browser_for_login()). The user logs in
         with their cTID **on cTrader's site**, not ours - we never
         see their password.
       - cTrader redirects the browser back to
         http://127.0.0.1:<REDIRECT_PORT>/callback?code=... - our local
         server (CallbackServer) grabs that one-time `code` and shuts
         itself down.
       - We exchange that code for an access_token + refresh_token
         and save both to disk (ctrader_token.json in the configured
         sync directory). That file is plain text: keep it private and
         never commit or share it.

  2. Every run after that:
       - We load the saved refresh_token and use it to mint a fresh
         access_token (TokenStore.ensure_fresh) WITHOUT involving the
         browser at all. Access tokens are short-lived; refresh tokens
         are long-lived and are themselves rotated (cTrader returns a
         new refresh_token on every refresh - we always save the new
         one, never keep reusing an old one).
       - If cTrader rejects the refresh token (for example, it was
         revoked), we fall back to step 1 and ask them to log in again.
         Network errors stop with a retry message instead of opening a
         misleading login prompt.

Nothing in this file talks to the trading API itself (positions, deals,
symbols) - that's ctrader_backend.py. This file's only job is producing
a valid access token for that module to use.

BUGFIX (this version): the success/failure check on a token response
used to be `if "errorCode" in token_response:` - checking only whether
the KEY "errorCode" was present in the dict, not whether it actually
carried a real error value. cTrader's own client library includes an
"errorCode" key on EVERY response, successful ones included - it's just
set to None when nothing went wrong. That meant a fully successful
login or refresh was being treated as a failure every single time,
purely because the label existed, never because there was an actual
error - the real tokens would print to the console (inside the "failed"
error message, confusingly) but never get saved to ctrader_token.json.
Fixed by checking `token_response.get("errorCode")` instead, which is
only truthy when errorCode is a real, non-None error value - exactly
the "is this ACTUALLY an error" question the code meant to ask.
"""

import getpass
import json
import os
import threading
import time
import webbrowser
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import requests
from ctrader_open_api import Auth, EndPoints

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# App credentials must be provided at runtime. Never embed a shared app
# secret in source code or a distributed executable.
_APP_CREDENTIALS = None

# Where cTrader redirects the browser back to after login. Must be
# registered EXACTLY (including port) as a redirect URI on the app's
# Spotware developer-portal page, or the auth request will be rejected.
REDIRECT_PORT = 53987
REDIRECT_URI = f"http://127.0.0.1:{REDIRECT_PORT}/callback"

# Read-only scope: history + live trade data, no order placement. Keeps
# the consent screen the user sees as unalarming as possible, and means
# a leaked/stale token can't be used to place or modify trades.
OAUTH_SCOPE = "accounts"

# How long to wait for the user to finish the browser login before giving
# up and telling them to try again.
LOGIN_TIMEOUT_SECONDS = 300


def get_app_credentials(allow_prompt=False):
    """Gets app credentials from the environment or a masked local prompt."""
    global _APP_CREDENTIALS
    if _APP_CREDENTIALS is not None:
        return _APP_CREDENTIALS

    client_id = os.environ.get("CTRADER_CLIENT_ID", "").strip()
    client_secret = os.environ.get("CTRADER_CLIENT_SECRET", "")
    if allow_prompt and not client_id:
        client_id = input("cTrader app client ID: ").strip()
    if allow_prompt and not client_secret:
        client_secret = getpass.getpass("cTrader app client secret: ")

    if not client_id or not client_secret:
        raise RuntimeError(
            "cTrader app credentials are missing. Set CTRADER_CLIENT_ID and "
            "CTRADER_CLIENT_SECRET, or run interactively to enter them."
        )

    _APP_CREDENTIALS = (client_id, client_secret)
    return _APP_CREDENTIALS


def _new_auth(allow_prompt=False):
    client_id, client_secret = get_app_credentials(allow_prompt=allow_prompt)
    return Auth(client_id, client_secret, REDIRECT_URI)


def _token_path(sync_dir):
    return os.path.join(sync_dir, "ctrader_token.json")


class _CallbackServer(BaseHTTPRequestHandler):
    """
    Handles exactly one GET to /callback, pulls the `code` query param
    out of it, stashes it on the server instance, and tells the browser
    tab it's safe to close. Everything else is a 404 - this is not a
    general-purpose web server, it exists for one redirect and then dies.
    """
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return

        params = parse_qs(parsed.query)
        code = params.get("code", [None])[0]
        error = params.get("error", [None])[0]
        self.server.auth_code = code
        self.server.auth_error = error

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if code:
            body = "<html><body><h2>Connected. You can close this tab and return to the app.</h2></body></html>"
        else:
            body = f"<html><body><h2>Login failed: {escape(error or 'unknown error')}. Close this tab and try again.</h2></body></html>"
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format, *args):
        pass  # silence BaseHTTPRequestHandler's default stderr access log


def _wait_for_callback(timeout_seconds):
    """
    Runs the local callback server on its own thread until it's caught
    one request (or the timeout passes), then returns (code, error).
    """
    server = HTTPServer(("127.0.0.1", REDIRECT_PORT), _CallbackServer)
    server.auth_code = None
    server.auth_error = None
    server.timeout = 1  # so handle_request() below doesn't block forever per call

    thread_deadline = time.time() + timeout_seconds
    while time.time() < thread_deadline:
        server.handle_request()  # blocks up to `timeout` seconds for one request
        if server.auth_code or server.auth_error:
            break
    server.server_close()
    return server.auth_code, server.auth_error


def open_browser_for_login(log=print):
    """
    Kicks off the full first-time login: opens the consent page in the
    user's browser, waits for the redirect, and returns the raw
    authorization code (or raises SystemExit on failure/timeout).
    """
    auth = _new_auth(allow_prompt=True)
    auth_uri = auth.getAuthUri(scope=OAUTH_SCOPE)

    log("Opening browser for cTrader login - please log in and click Authorize...")
    webbrowser.open(auth_uri)

    code, error = _wait_for_callback(LOGIN_TIMEOUT_SECONDS)
    if error:
        raise SystemExit(f"cTrader login was denied or failed: {error}")
    if not code:
        raise SystemExit(
            f"Timed out waiting for cTrader login (no response after {LOGIN_TIMEOUT_SECONDS}s).\n"
            "Run the bridge again when you're ready to log in."
        )
    return code


class TokenStore:
    """
    Owns the on-disk token file for one SYNC_DIR and knows how to turn
    "whatever's on disk" into "a definitely-fresh access token", doing
    a browser login only when there's truly no other way.
    """

    def __init__(self, sync_dir, log=print, seed_refresh_token=None, allow_interactive_login=True):
        self.path = _token_path(sync_dir)
        self.log = log
        self.allow_interactive_login = allow_interactive_login
        self.auth = _new_auth(allow_prompt=allow_interactive_login)
        self._data = self._load()

        # Headless/CI seeding: if nothing's on disk yet but a refresh
        # token was supplied directly (e.g. from a GitHub Actions
        # secret - see ctrader_market_data_sync.py), seed it here with
        # expires_at=0 so ensure_fresh() below treats it as already
        # expired and goes straight through the NORMAL refresh path
        # (self.auth.refreshToken(...)) on its first call - no special
        # casing needed anywhere else, this is the only place headless
        # bootstrapping touches. A refresh token already on disk always
        # wins over a seed value, so this never clobbers a real
        # previous login.
        if seed_refresh_token and not self._data.get("refresh_token"):
            self._data = {"refresh_token": seed_refresh_token, "access_token": None, "expires_at": 0}

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
        return {}

    def _save(self, token_response):
        """
        Persists the token response as-is (access_token, refresh_token,
        expires_in, etc). We store expires_in as an absolute
        expires_at instead, since "seconds from now" goes stale the
        moment it's written - only the absolute time is still correct
        when we read it back later.
        """
        now = int(time.time())
        self._data = {
            "access_token": token_response["accessToken"],
            "refresh_token": token_response["refreshToken"],
            "expires_at": now + int(token_response.get("expiresIn", 0)),
            "token_type": token_response.get("tokenType", "bearer"),
        }
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        os.replace(tmp_path, self.path)

    def _login_and_save(self):
        if not self.allow_interactive_login:
            # Headless context (a GitHub Actions runner, any environment
            # with no browser/display) - opening a browser here would
            # just hang until LOGIN_TIMEOUT_SECONDS expires (5 minutes)
            # before failing anyway, since nothing will ever complete
            # the callback. Fail immediately and clearly instead - this
            # means either the seeded refresh token was never provided,
            # or it's been revoked/expired and needs a real interactive
            # re-login (see ctrader_market_data_sync.py's docstring for
            # the "how to get a fresh one" steps).
            raise SystemExit(
                "cTrader login requires a browser, but this is running headless "
                "(allow_interactive_login=False) and no valid refresh token was "
                "available. Run the interactive login locally once to get a fresh "
                "refresh token, then update the CTRADER_REFRESH_TOKEN secret."
            )
        code = open_browser_for_login(self.log)
        try:
            token_response = self.auth.getToken(code)
        except requests.RequestException:
            raise SystemExit("cTrader token exchange failed due to a network error; retry the login.") from None
        if not isinstance(token_response, dict):
            raise SystemExit("cTrader returned an invalid token response; no token was saved.")
        # See module docstring's BUGFIX note: "errorCode" is present on
        # EVERY response (success or failure) - only a non-None value
        # means something actually went wrong. Checking `"errorCode" in
        # token_response` instead of this would treat every successful
        # login as a failure and silently discard the real tokens.
        if token_response.get("errorCode"):
            raise SystemExit(
                "cTrader token exchange was rejected. Check the app credentials, "
                "redirect URI, and authorization, then retry."
            )
        if not token_response.get("accessToken") or not token_response.get("refreshToken"):
            raise SystemExit("cTrader returned an incomplete token response; no token was saved.")
        self._save(token_response)
        self.log("cTrader login successful - token saved.")

    def ensure_fresh(self, refresh_margin_seconds=120):
        """
        Returns a valid access token, refreshing or re-logging-in as
        needed. This is the one method the rest of the bridge should
        call - it never returns a token that's about to expire.
        """
        if not self._data.get("refresh_token"):
            self._login_and_save()
            return self._data["access_token"]

        expires_at = self._data.get("expires_at", 0)
        if time.time() < expires_at - refresh_margin_seconds:
            return self._data["access_token"]  # still comfortably valid

        try:
            token_response = self.auth.refreshToken(self._data["refresh_token"])
        except requests.RequestException:
            raise SystemExit("cTrader token refresh failed due to a network error; retry later.") from None
        if not isinstance(token_response, dict):
            raise SystemExit("cTrader returned an invalid token response; no token was saved.")
        if token_response.get("errorCode"):
            self.log("cTrader refresh token was rejected; requesting a fresh authorization.")
            self._login_and_save()
        else:
            if not token_response.get("accessToken") or not token_response.get("refreshToken"):
                raise SystemExit("cTrader returned an incomplete token response; no token was saved.")
            self._save(token_response)
            self.log("cTrader access token refreshed.")

        return self._data["access_token"]
