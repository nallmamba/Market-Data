"""
ctrader_backend.py
-------------------
Talks to the cTrader Open API itself, once ctrader_auth.py has produced
a valid access token. This is the cTrader equivalent of the `mt5.*`
calls in mt5_sync_bridge.py - but because there's no local terminal to
attach to, "connecting" here means opening a TCP/TLS socket to
Spotware's servers and running an authentication handshake, not calling
mt5.initialize().

WHY THIS FILE LOOKS DIFFERENT FROM THE MT5 SCRIPT:

The `MetaTrader5` package is synchronous - call a function, get an
answer, done. The `ctrader-open-api` package is built on Twisted and is
entirely callback/Deferred-based - you send a request, and some time
later a callback fires with the response, on Twisted's own event loop
("the reactor"), which has to be running continuously in the
background for any of this to work.

Rather than rewrite the whole bridge's poll loop in Twisted's style,
CTraderConnection below runs the reactor on a single background daemon
thread for the life of the program, and exposes a plain, blocking
`.call(request)` method - so the rest of the bridge (poll loop, JSON
saving, etc, all still to be ported from mt5_sync_bridge.py) can stay
written in the same simple synchronous style as the MT5 version.

BUGFIX (this version): every message that comes back over the wire
arrives wrapped in a generic ProtoMessage envelope first - just a
payloadType number plus raw, still-serialized bytes - not the actual,
specific response object (ProtoOAGetAccountListByAccessTokenRes,
ProtoOADealListRes, etc). `.call()` was returning that raw envelope
directly instead of unwrapping it, so any caller that tried to read a
real field off the result (response.ctidTraderAccount, response.deal,
response.symbol, response.trendbar, response.order/position, ...)
failed with an AttributeError - because that field genuinely only
exists on the unwrapped message, not the envelope. Confirmed directly
against the installed ctrader-open-api package: its own tcpProtocol.py
always parses incoming bytes into a bare ProtoMessage first, and the
package's own Protobuf.extract() helper is what turns that into the
real, typed message (it looks up the right message class from
payloadType, then parses the envelope's raw payload bytes into it).
Since every other cTrader script (ctrader_history.py, ctrader_candles.py,
ctrader_live.py, ...) reads its results through this one shared
`.call()` method, this single fix - extracting the real message here,
once, before returning it - fixes every one of those call sites at
once, with no changes needed anywhere else.

DEMO VS LIVE - READ BEFORE ADDING ACCOUNTS:

cTrader's demo and live accounts are NOT reachable from the same
connection - they're separate hosts, and Spotware's servers reject
account-auth if you use the wrong one for a given account. The account
list response tells you which is which per account (`isLive`), but you
only find that out AFTER connecting once. So the flow here is:

  1. Open one connection to the DEMO host, app-auth, and ask for the
     account list. (The list itself includes both demo and live
     accounts tied to this login - only the per-account trading
     calls are host-restricted, not the list call.)
  2. Any account with isLive=False can be authenticated and used right
     on that same demo connection.
  3. Any account with isLive=True needs a SECOND connection, to the
     LIVE host, app-authed separately, before it can be account-authed.

CTraderSession (bottom of this file) manages that split so callers
don't have to think about it - ask it for a connection for a specific
ctidTraderAccountId and it hands back the right one, opening the live
connection lazily only if/when a live account is actually requested.
"""

import threading

from twisted.internet import reactor
from ctrader_open_api import Client, TcpProtocol, EndPoints, Protobuf
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq,
    ProtoOAGetAccountListByAccessTokenReq,
)

from ctrader_auth import get_app_credentials

DEFAULT_CALL_TIMEOUT = 10
CONNECT_TIMEOUT = 15


class CTraderConnection:
    """
    One TCP/TLS connection to one cTrader Open API host (demo OR live -
    never both; see module docstring). Handles the low-level
    connect -> app-auth handshake and exposes a blocking `.call()` for
    everything after that.
    """

    def __init__(self, use_live, log=print):
        self.use_live = use_live
        self.log = log
        host = EndPoints.PROTOBUF_LIVE_HOST if use_live else EndPoints.PROTOBUF_DEMO_HOST
        self.client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self._connected_event = threading.Event()
        self.client.setConnectedCallback(lambda c: self._connected_event.set())
        self.client.setDisconnectedCallback(self._on_disconnected)
        self._app_authed = False
        self._authed_account_ids = set()

    def _on_disconnected(self, client, reason):
        self._connected_event.clear()
        self._app_authed = False
        self._authed_account_ids.clear()
        self.log(f"cTrader {'live' if self.use_live else 'demo'} connection lost: {reason}")

    @staticmethod
    def _ensure_reactor_running():
        # The reactor is a process-wide singleton - only start its thread
        # once, no matter how many CTraderConnection instances exist
        # (one for demo, maybe a second for live).
        if not reactor.running:
            t = threading.Thread(
                target=reactor.run, kwargs={"installSignalHandlers": False}, daemon=True
            )
            t.start()

    def connect(self, timeout=CONNECT_TIMEOUT):
        self._ensure_reactor_running()
        reactor.callFromThread(self.client.startService)
        if not self._connected_event.wait(timeout):
            raise SystemExit(
                f"Couldn't reach cTrader's {'live' if self.use_live else 'demo'} "
                "servers (timed out). Check your internet connection and try again."
            )
        self.log(f"Connected to cTrader ({'live' if self.use_live else 'demo'}).")

    def call(self, request, timeout=DEFAULT_CALL_TIMEOUT):
        """
        Sends one request and blocks the CALLING thread (not the
        reactor thread) until the matching response arrives, or raises
        on error/timeout. This is the bridge between Twisted's
        callback world and the rest of the bridge's plain synchronous
        code.

        Returns the real, unwrapped response message (e.g. a
        ProtoOAGetAccountListByAccessTokenRes with a real
        ctidTraderAccount field) - NOT the raw ProtoMessage envelope
        it arrives in over the wire. See module docstring's BUGFIX
        note for why this extraction step has to happen here.
        """
        result = {}
        done = threading.Event()

        def _on_success(response):
            result["response"] = response
            done.set()

        def _on_error(failure):
            result["error"] = failure
            done.set()

        def _do_send():
            deferred = self.client.send(request, responseTimeoutInSeconds=timeout)
            deferred.addCallbacks(_on_success, _on_error)

        reactor.callFromThread(_do_send)

        if not done.wait(timeout + 5):
            raise TimeoutError(f"cTrader request timed out: {type(request).__name__}")
        if "error" in result:
            raise RuntimeError(f"cTrader request failed: {result['error']}")

        # Unwrap the raw envelope into the real, typed message - see
        # module docstring's BUGFIX note. Protobuf.extract() reads the
        # envelope's payloadType, looks up the matching message class,
        # and parses the envelope's raw payload bytes into it.
        return Protobuf.extract(result["response"])

    def authenticate_app(self):
        if self._app_authed:
            return
        client_id, client_secret = get_app_credentials(allow_prompt=True)
        request = ProtoOAApplicationAuthReq()
        request.clientId = client_id
        request.clientSecret = client_secret
        self.call(request)
        self._app_authed = True
        self.log(f"cTrader app authorized on {'live' if self.use_live else 'demo'} connection.")

    def authenticate_account(self, ctid_trader_account_id, access_token):
        if ctid_trader_account_id in self._authed_account_ids:
            return
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = ctid_trader_account_id
        request.accessToken = access_token
        self.call(request)
        self._authed_account_ids.add(ctid_trader_account_id)
        self.log(f"cTrader account #{ctid_trader_account_id} authorized.")


class CTraderSession:
    """
    Top-level entry point for the rest of the bridge. Wraps the
    demo/live connection split described in the module docstring so
    callers just say "give me a connection for this account" and get
    back something already app-authed and account-authed, without
    needing to know or care whether that account happens to be demo or
    live.
    """

    def __init__(self, access_token, log=print):
        self.access_token = access_token
        self.log = log
        self._demo_conn = None
        self._live_conn = None
        self._accounts = None  # populated by list_accounts()

    def _demo_connection(self):
        if self._demo_conn is None:
            self._demo_conn = CTraderConnection(use_live=False, log=self.log)
            self._demo_conn.connect()
            self._demo_conn.authenticate_app()
        return self._demo_conn

    def _live_connection(self):
        if self._live_conn is None:
            self._live_conn = CTraderConnection(use_live=True, log=self.log)
            self._live_conn.connect()
            self._live_conn.authenticate_app()
        return self._live_conn

    def list_accounts(self, force_refresh=False):
        """
        Returns the list of trading accounts (demo and live) reachable
        with the current access token, as a list of dicts:
        {"ctidTraderAccountId": int, "isLive": bool, "traderLogin": int}.
        Cached after the first call within this session.
        """
        if self._accounts is not None and not force_refresh:
            return self._accounts

        conn = self._demo_connection()  # the list call itself works from either host
        request = ProtoOAGetAccountListByAccessTokenReq()
        request.accessToken = self.access_token
        response = conn.call(request)

        self._accounts = [
            {
                "ctidTraderAccountId": acc.ctidTraderAccountId,
                "isLive": acc.isLive,
                "traderLogin": acc.traderLogin,
            }
            for acc in response.ctidTraderAccount
        ]
        return self._accounts

    def refresh_access_token(self, new_access_token):
        """
        Swaps in a freshly-refreshed access token (see
        ctrader_auth.TokenStore.ensure_fresh(), which the sync bridge's
        main loop calls every cycle) WITHOUT tearing down and
        reconnecting the underlying TCP connections - app auth doesn't
        depend on the user's access token so it stays valid, but each
        account's account-auth DOES use it, so any already-authed
        accounts need to be re-authed with the new token before their
        next call. Cheap and rare in practice (access tokens are
        typically valid for hours), but necessary for a bridge that's
        meant to run for days at a time.
        """
        if new_access_token == self.access_token:
            return
        self.access_token = new_access_token
        for conn in (self._demo_conn, self._live_conn):
            if conn is not None:
                conn._authed_account_ids.clear()  # forces re-authenticate_account() on next use, with the new token

    def connection_for_account(self, ctid_trader_account_id):
        """
        Returns a connection that's already app-authed AND account-authed
        for the given account, opening the (demo or live) connection
        lazily on first use and reusing it after that.
        """
        accounts = self.list_accounts()
        match = next(
            (a for a in accounts if a["ctidTraderAccountId"] == ctid_trader_account_id), None
        )
        if match is None:
            raise ValueError(
                f"Account #{ctid_trader_account_id} isn't in this token's account list. "
                "Call list_accounts() to see what's available."
            )

        conn = self._live_connection() if match["isLive"] else self._demo_connection()
        conn.authenticate_account(ctid_trader_account_id, self.access_token)
        return conn


if __name__ == "__main__":
    # Smoke test: login (browser flow if needed), list accounts, and
    # confirm we can account-auth against the first one. Doesn't pull
    # trade history yet - that's the next piece to build, matching
    # fetch_closed_trades() / poll_live_events() from mt5_sync_bridge.py
    # but using ProtoOADealListReq / ProtoOAReconcileReq instead of
    # mt5.history_deals_get() / mt5.positions_get().
    import os
    from ctrader_auth import TokenStore

    sync_dir = os.path.dirname(os.path.abspath(__file__))
    token_store = TokenStore(sync_dir)
    access_token = token_store.ensure_fresh()

    session = CTraderSession(access_token)
    accounts = session.list_accounts()
    print(f"Found {len(accounts)} account(s):")
    for acc in accounts:
        print(f"  #{acc['ctidTraderAccountId']} login={acc['traderLogin']} live={acc['isLive']}")

    if accounts:
        first = accounts[0]["ctidTraderAccountId"]
        session.connection_for_account(first)
        print(f"Successfully authenticated account #{first}.")
