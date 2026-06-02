"""
STAGE 1 — Single Test Order (manual, read+write proof)
=============================================================================
Goal: prove we can PLACE an order on the Exness MT5 demo via MetaApi, see it,
and cancel it. This is the first time code WRITES to the trading account.

Safety design:
  - XAUUSDm only (the confirmed gold symbol)
  - Fixed tiny volume (0.01 lots) — NO risk-based sizing yet (that's Stage 2)
  - The limit is placed FAR BELOW current price so it CANNOT fill during the
    test (a buy limit only triggers if price drops to it; we put it ~12% below)
  - 24h expiry attached, so even if forgotten it auto-cancels
  - Nothing runs on a loop; every action is a manual URL hit

Endpoints:
  GET /                  -> info
  GET /price             -> current XAUUSDm price (read-only sanity check)
  GET /place_test_order  -> place ONE safe buy-limit, returns order id + details
  GET /orders            -> list current pending orders
  GET /cancel/<order_id> -> cancel a specific pending order

Environment variables (set in Railway, never in code):
  METAAPI_TOKEN, METAAPI_ACCOUNT_ID
"""

import os
import asyncio
import time
import uuid
import logging
import threading
import traceback
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, request

from sizing import compute_lot_size

app = Flask(__name__)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("jp-exec")

# --- duplicate-order diagnosis instrumentation ---------------------------- #
# Every call to a placing endpoint logs an ENTER line with a unique id + the
# time since the previous call. An in-memory lock blocks a second placement
# within COOLDOWN seconds, so retries are revealed AND prevented.
_place_lock = threading.Lock()
_last_place_ts = [0.0]          # mutable holder
_place_call_count = [0]
COOLDOWN_SECONDS = 30           # no two test placements within 30s

# Detector bot that serves /signals (the source of truth for setups)
BOT_URL = os.getenv("BOT_URL", "https://tv-telegram-bot-bhuc.onrender.com").rstrip("/")

# Signal ids we've already placed this process (id-level duplicate guard).
_placed_signal_ids = set()


def fetch_detector_signals():
    """Read the detector bot's /signals. Returns (signals_list, error_str)."""
    import requests
    try:
        # Render free tier can cold-start; allow time.
        r = requests.get(f"{BOT_URL}/signals", timeout=70)
        r.raise_for_status()
        data = r.json()
        return data.get("signals", []), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def assess_signal(sig, spec, price, balance):
    """Pure logic: given a signal + symbol spec + current price + balance, work
    out the order type, validate the limit against market, and size it. Places
    nothing. Returns a dict describing exactly what WOULD be placed (or why not)."""
    direction = (sig.get("direction") or "").lower()
    entry = sig.get("entry")
    stop = sig.get("stop")
    target = sig.get("target")
    risk_percent = sig.get("risk_percent") or 0.5
    bid = price.get("bid")
    ask = price.get("ask")

    out = {
        "id": sig.get("id"), "symbol": sig.get("symbol"),
        "direction": direction, "entry": entry, "stop": stop, "target": target,
        "risk_percent": risk_percent, "market": {"bid": bid, "ask": ask},
    }

    if entry is None or stop is None or target is None:
        out.update(placeable=False, reason="signal missing entry/stop/target")
        return out

    # direction -> order type + structural sanity check
    if direction == "bullish":
        out["order_type"] = "buy_limit"
        if not (stop < entry < target):
            out.update(placeable=False, reason="bullish stop<entry<target check failed")
            return out
        if entry >= ask:   # buy limit must sit BELOW market
            out.update(placeable=False,
                       reason=f"buy-limit entry {entry} not below market ask {ask} "
                              f"(price moved; signal stale)")
            return out
    elif direction == "bearish":
        out["order_type"] = "sell_limit"
        if not (target < entry < stop):
            out.update(placeable=False, reason="bearish target<entry<stop check failed")
            return out
        if entry <= bid:   # sell limit must sit ABOVE market
            out.update(placeable=False,
                       reason=f"sell-limit entry {entry} not above market bid {bid} "
                              f"(price moved; signal stale)")
            return out
    else:
        out.update(placeable=False, reason=f"unknown direction '{direction}'")
        return out

    sizing = compute_lot_size(balance, risk_percent, entry, stop, spec)
    out["sizing"] = sizing
    if not sizing.get("ok") or not sizing.get("tradeable"):
        out.update(placeable=False,
                   reason="sizing not tradeable: " + str(sizing.get("flag") or sizing.get("error")))
        return out

    out.update(placeable=True, lot=sizing["lot"], actual_risk=sizing["actual_risk"])
    return out

TOKEN = os.getenv("METAAPI_TOKEN", "")
ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")

# =========================================================================== #
# STAGE 5a — READ-ONLY WATCHER LOOP
# =========================================================================== #
# A background thread polls the detector on a timer and LOGS what it WOULD
# auto-place for each fresh, valid signal. It places NOTHING. This is the safe
# first step toward autonomy: it proves the loop sees new signals and that the
# selection/validity logic behaves, with zero broker risk.
#
# Controls (Railway env vars):
#   WATCHER_ENABLED   "true" to run the loop at all (default false -> off)
#   WATCHER_INTERVAL  seconds between polls (default 120)
#
# IMPORTANT: there is no placement here. Arming actual auto-placement is a
# separate, later step (5b) and will use the DURABLE order_placed flag in
# Supabase for idempotency — NOT the in-memory set, which a restart would wipe.
# =========================================================================== #

WATCHER_ENABLED = os.getenv("WATCHER_ENABLED", "false").lower() == "true"
try:
    WATCHER_INTERVAL = max(30, int(os.getenv("WATCHER_INTERVAL", "120")))
except ValueError:
    WATCHER_INTERVAL = 120

# --- Stage 5b placement controls (all conservative defaults) -------------- #
# AUTO_PLACE gates real placement. Ships OFF: deploying 5b changes nothing
# until you explicitly set AUTO_PLACE=true. Mirrors how WATCHER_ENABLED works.
AUTO_PLACE = os.getenv("AUTO_PLACE", "false").lower() == "true"
try:
    MAX_OPEN_AUTO = max(1, int(os.getenv("MAX_OPEN_AUTO", "1")))   # concurrent cap
except ValueError:
    MAX_OPEN_AUTO = 1
try:
    MAX_PLACES_PER_DAY = max(1, int(os.getenv("MAX_PLACES_PER_DAY", "5")))
except ValueError:
    MAX_PLACES_PER_DAY = 5

# Runtime kill-switch (toggled via /watcher/pause and /watcher/resume — no
# redeploy needed). When paused, the loop reverts to read-only evaluation.
_place_paused = [False]
# Backlog snapshot: ids present when the loop first armed -> never auto-placed.
_backlog_ids = set()
_backlog_captured = [False]
# Daily placement ledger: {"date": "YYYY-MM-DD", "count": n}
_places_today = {"date": None, "count": 0}

# Last evaluation snapshot, surfaced via /watcher_status (no log digging needed).
_watcher_state = {
    "enabled": WATCHER_ENABLED,
    "interval": WATCHER_INTERVAL,
    "auto_place": AUTO_PLACE,
    "max_open_auto": MAX_OPEN_AUTO,
    "max_places_per_day": MAX_PLACES_PER_DAY,
    "running": False,
    "paused": False,
    "last_run": None,
    "last_error": None,
    "runs": 0,
    "backlog_count": 0,
    "places_today": 0,
    "last_placed": None,        # {id, orderId, at}
    "last_evaluation": [],      # list of {id, would_place, reason/lot}
}
_watcher_started = False


def _mark_placed_durably(signal_id, order_id):
    """Persist order_placed=true to the detector (Supabase) so a restart never
    re-places this signal. Reuses the Stage 4b worker-token write path."""
    if not DETECTOR_URL or not WORKER_TOKEN:
        log.warning("[watcher] cannot mark placed: DETECTOR_URL/WORKER_TOKEN unset")
        return
    try:
        requests.post(
            f"{DETECTOR_URL}/update_signal",
            headers={"Content-Type": "application/json", "X-Worker-Token": WORKER_TOKEN},
            json={"id": signal_id, "order_placed": True}, timeout=20)
    except Exception as e:
        log.warning(f"[watcher] mark-placed failed for {signal_id}: {e}")


def _outstanding_auto_count(c_positions, c_orders):
    """Count JP_SIG_ items currently OUTSTANDING — open positions PLUS pending
    limit orders. 'Outstanding' is the right unit for the concurrency cap: a
    pending limit will become exposure when it fills, so it counts too."""
    n = 0
    for p in (c_positions or []):
        if (p.get("comment") or "").startswith("JP_SIG_"):
            n += 1
    for o in (c_orders or []):
        if (o.get("comment") or "").startswith("JP_SIG_"):
            n += 1
    return n


def _today_str():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _places_today_count():
    if _places_today["date"] != _today_str():
        _places_today["date"] = _today_str()
        _places_today["count"] = 0
    return _places_today["count"]


def _watcher_tick():
    """One pass. ALWAYS evaluates read-only. PLACES only when armed (AUTO_PLACE),
    not paused, the signal is genuinely new (not backlog, not already placed),
    and every cap/guard passes. Places at most ONE signal per tick."""
    from datetime import datetime, timezone, timedelta
    signals, ferr = fetch_detector_signals()
    if ferr:
        _watcher_state["last_error"] = f"fetch: {ferr}"
        log.warning(f"[watcher] could not reach detector: {ferr}")
        return
    signals = signals or []

    # Capture the backlog ONCE, the first time we run while armed. Everything
    # that exists at that moment is backlog -> never auto-placed (JP's rule).
    if AUTO_PLACE and not _backlog_captured[0]:
        for s in signals:
            if s.get("id"):
                _backlog_ids.add(s["id"])
        _backlog_captured[0] = True
        _watcher_state["backlog_count"] = len(_backlog_ids)
        log.info(f"[watcher] backlog snapshot captured: {len(_backlog_ids)} signals will NOT be auto-placed")

    ranked = sorted(signals, key=lambda s: s.get("timestamp", ""), reverse=True)[:8]

    async def do(c):
        info = await c.get_account_information()
        balance = info.get("balance")
        positions = await c.get_positions()
        pending = await c.get_orders()
        outstanding = _outstanding_auto_count(positions, pending)

        evals = []
        placement = None
        for sig in ranked:
            sym = sig.get("symbol") or SYMBOL
            sid = sig.get("id")
            try:
                spec_raw = await c.get_symbol_specification(symbol=sym)
                price = await c.get_symbol_price(symbol=sym)
                spec = {
                    "tick_size": spec_raw.get("tickSize"),
                    "tick_value": price.get("lossTickValue"),
                    "contract_size": spec_raw.get("contractSize"),
                    "min_volume": spec_raw.get("minVolume"),
                    "max_volume": spec_raw.get("maxVolume"),
                    "volume_step": spec_raw.get("volumeStep"),
                }
                a = assess_signal(sig, spec, price, balance)
                is_backlog = sid in _backlog_ids
                already = bool(sig.get("order_placed")) or (sid in _placed_signal_ids)
                eligible = bool(a.get("placeable")) and not already and not is_backlog
                evals.append({
                    "id": sid, "would_place": eligible,
                    "already_placed": already, "backlog": is_backlog,
                    "placeable": a.get("placeable"), "reason": a.get("reason"),
                    "lot": a.get("lot"), "order_type": a.get("order_type"),
                })
                # pick the FIRST (freshest) eligible signal as the placement candidate
                if eligible and placement is None:
                    placement = (sig, a, sym)
            except Exception as e:
                evals.append({"id": sid, "would_place": False,
                              "reason": f"assess error: {type(e).__name__}: {e}"})

        result = {"evals": evals, "outstanding": outstanding, "placed": None, "skipped": None}

        # ---- placement decision (only if armed + not paused) ----
        if not (AUTO_PLACE and not _place_paused[0]):
            result["skipped"] = "read-only (not armed or paused)"
            return result
        if placement is None:
            result["skipped"] = "nothing new+valid to place"
            return result
        if outstanding >= MAX_OPEN_AUTO:
            result["skipped"] = f"concurrency cap: {outstanding} outstanding >= {MAX_OPEN_AUTO}"
            return result
        if _places_today_count() >= MAX_PLACES_PER_DAY:
            result["skipped"] = f"daily cap reached ({MAX_PLACES_PER_DAY})"
            return result

        sig, a, sym = placement
        sid = sig.get("id")
        # serialize with the same lock the manual endpoint uses
        if not _place_lock.acquire(blocking=False):
            result["skipped"] = "another placement in progress"
            return result
        try:
            expiry_hours = sig.get("order_expiry_hours") or 24
            options = {
                "comment": f"JP_SIG_{sid}"[:31],
                "expiration": {
                    "type": "ORDER_TIME_SPECIFIED",
                    "time": datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
                },
            }
            if a["order_type"] == "buy_limit":
                res = await c.create_limit_buy_order(
                    symbol=sym, volume=a["lot"], open_price=a["entry"],
                    stop_loss=a["stop"], take_profit=a["target"], options=options)
            else:
                res = await c.create_limit_sell_order(
                    symbol=sym, volume=a["lot"], open_price=a["entry"],
                    stop_loss=a["stop"], take_profit=a["target"], options=options)
            order_id = res.get("orderId")
            _placed_signal_ids.add(sid)
            _last_place_ts[0] = time.time()
            _places_today["count"] = _places_today_count() + 1
            result["placed"] = {"id": sid, "orderId": order_id, "lot": a["lot"],
                                "order_type": a["order_type"]}
            log.info(f"[watcher] AUTO-PLACED {sid} order={order_id} lot={a['lot']}")
        finally:
            _place_lock.release()
        return result

    try:
        out = asyncio.run(_with_connection(do))
        _watcher_state["last_evaluation"] = out["evals"]
        _watcher_state["last_error"] = None
        _watcher_state["last_run"] = datetime.now(timezone.utc).isoformat()
        _watcher_state["runs"] += 1
        _watcher_state["places_today"] = _places_today_count()
        if out.get("placed"):
            p = out["placed"]
            p["at"] = datetime.now(timezone.utc).isoformat()
            _watcher_state["last_placed"] = p
            # mark durably OUTSIDE the connection so a failure here can't block the loop
            _mark_placed_durably(p["id"], p["orderId"])
        elif out.get("skipped"):
            log.info(f"[watcher] no placement: {out['skipped']}")
    except Exception as e:
        _watcher_state["last_error"] = f"{type(e).__name__}: {e}"
        log.warning(f"[watcher] tick error: {e}")


def _watcher_loop():
    mode = "ARMED (will place)" if AUTO_PLACE else "read-only (no placement)"
    log.info(f"[watcher] loop started — {mode}, interval {WATCHER_INTERVAL}s, "
             f"max_open={MAX_OPEN_AUTO}, max/day={MAX_PLACES_PER_DAY}")
    _watcher_state["running"] = True
    while True:
        try:
            _watcher_tick()
        except Exception as e:
            log.warning(f"[watcher] loop guard caught: {e}")
        _watcher_state["paused"] = _place_paused[0]
        time.sleep(WATCHER_INTERVAL)


def _maybe_start_watcher():
    """Start the loop once, only if enabled. Safe under gunicorn --workers 1."""
    global _watcher_started
    if _watcher_started or not WATCHER_ENABLED:
        return
    _watcher_started = True
    t = threading.Thread(target=_watcher_loop, daemon=True)
    t.start()

# Where to write computed outcomes back (the detector = source of truth) and
# the shared secret that authorizes broker-field writes. Both set in Railway.
DETECTOR_URL = os.getenv("DETECTOR_URL", "https://tv-telegram-bot-bhuc.onrender.com").rstrip("/")
WORKER_TOKEN = os.getenv("WORKER_TOKEN", "")

# Test symbol is configurable so we can use a 24/7 instrument (e.g. BTCUSDm)
# when forex/metals markets are closed on weekends, then switch back to
# XAUUSDm during market hours. Set TEST_SYMBOL in Railway to override.
SYMBOL = os.getenv("TEST_SYMBOL", "XAUUSDm")
TEST_VOLUME = float(os.getenv("TEST_VOLUME", "0.01"))   # smallest lot
SAFE_DISTANCE_PCT = 0.12    # place limit 12% below price so it can't fill
TEST_RR = 3.0               # 3R target, matching the strategy


async def _with_connection(do):
    """Open an RPC connection, run `do(connection)`, always clean up."""
    from metaapi_cloud_sdk import MetaApi
    api = MetaApi(TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
    if account.state != "DEPLOYED":
        await account.deploy()
    await account.wait_connected()
    connection = account.get_rpc_connection()
    await connection.connect()
    await connection.wait_synchronized(120)
    try:
        return await do(connection)
    finally:
        try:
            await connection.close()
        except Exception:
            pass


def _check_env():
    if not TOKEN or not ACCOUNT_ID:
        return {"success": False, "error": "METAAPI_TOKEN or METAAPI_ACCOUNT_ID not set"}
    return None


# --------------------------------------------------------------------------- #
@app.route("/")
def home():
    return jsonify({
        "service": "Stage 1 - single test order",
        "configured": bool(TOKEN and ACCOUNT_ID),
        "symbol": SYMBOL,
        "endpoints": {
            "/symbols?q=BTC": "search available symbols by substring (find exact name)",
            "/symbol_spec": "read real broker spec + tick value + demo sizing (read-only)",
            "/preview_signals": "DRY RUN: what we'd place for each real detector signal (no orders)",
            "/place_signal/<id>": "place ONE order from a specific detector signal (guarded)",
            "/price": "current price for the configured TEST_SYMBOL",
            "/place_test_order": "place ONE safe buy-limit (won't fill), 24h expiry",
            "/orders": "list pending orders",
            "/cancel/<order_id>": "cancel a pending order",
        },
        "note": f"Test symbol is {SYMBOL} (set TEST_SYMBOL env var to change, "
                f"e.g. a 24/7 crypto symbol on weekends).",
    })


@app.route("/symbols")
def symbols():
    """Search available symbols by substring, e.g. /symbols?q=BTC
    Use this to find the EXACT crypto symbol name before testing on weekends."""
    err = _check_env()
    if err:
        return jsonify(err), 400
    from flask import request
    q = (request.args.get("q") or "").upper()

    async def do(c):
        syms = await c.get_symbols()
        matches = [s for s in syms if q in s.upper()] if q else syms
        return {"query": q, "match_count": len(matches), "matches": matches[:50],
                "total_symbols": len(syms)}

    try:
        result = asyncio.run(_with_connection(do))
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1200:]}), 500


@app.route("/symbol_spec")
def symbol_spec():
    """Read the REAL broker symbol spec + price (raw, no guessing), extract the
    fields sizing needs, and demo a sizing calc against live numbers.
    Read-only — places no orders.
    Optional query params to shape the demo: balance, risk, entry, stop.
    """
    err = _check_env()
    if err:
        return jsonify(err), 400
    from flask import request
    balance = float(request.args.get("balance", 10000))
    risk = float(request.args.get("risk", 0.5))

    async def do(c):
        spec_raw = await c.get_symbol_specification(symbol=SYMBOL)
        price = await c.get_symbol_price(symbol=SYMBOL)

        # Extract the fields sizing needs. We try common key names but also
        # return the FULL raw objects so we can see the truth if a name differs.
        def g(d, *names):
            for n in names:
                if isinstance(d, dict) and d.get(n) is not None:
                    return d.get(n)
            return None

        tick_size = g(spec_raw, "tickSize")
        contract_size = g(spec_raw, "contractSize")
        min_v = g(spec_raw, "minVolume", "volumeMin")
        max_v = g(spec_raw, "maxVolume", "volumeMax")
        step = g(spec_raw, "volumeStep", "volumeStepSize")
        # tick value usually lives on the price object
        loss_tick_value = g(price, "lossTickValue")
        profit_tick_value = g(price, "profitTickValue")

        extracted = {
            "tick_size": tick_size,
            "tick_value": loss_tick_value,        # use LOSS tick value for risk
            "contract_size": contract_size,
            "min_volume": min_v,
            "max_volume": max_v,
            "volume_step": step,
        }

        result = {
            "symbol": SYMBOL,
            "extracted_for_sizing": extracted,
            "loss_tick_value": loss_tick_value,
            "profit_tick_value": profit_tick_value,
            "price": {"bid": price.get("bid"), "ask": price.get("ask")},
            "raw_specification": spec_raw,
            "raw_price": price,
        }

        # If we have enough to size, demo it against live price.
        entry = float(request.args.get("entry", price.get("bid") or 0))
        # default stop = 0.3% below entry (just a realistic demo distance)
        default_stop = round(entry * 0.997, 5) if entry else 0
        stop = float(request.args.get("stop", default_stop))
        if tick_size and step and min_v is not None and max_v is not None:
            sizing = compute_lot_size(balance, risk, entry, stop, extracted)
            result["sizing_demo"] = sizing
        else:
            result["sizing_demo"] = {"ok": False,
                                     "error": "missing spec fields; inspect raw_specification"}
        return result

    try:
        result = asyncio.run(_with_connection(do))
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1500:]}), 500


@app.route("/price")
def price():
    err = _check_env()
    if err:
        return jsonify(err), 400

    async def do(c):
        p = await c.get_symbol_price(symbol=SYMBOL)
        return {"symbol": SYMBOL, "bid": p.get("bid"), "ask": p.get("ask")}

    try:
        result = asyncio.run(_with_connection(do))
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1200:]}), 500


@app.route("/place_test_order")
def place_test_order():
    err = _check_env()
    if err:
        return jsonify(err), 400

    # --- diagnosis: log every entry with a unique id and gap since last call
    req_id = uuid.uuid4().hex[:8]
    now = time.time()
    gap = now - _last_place_ts[0]
    _place_call_count[0] += 1
    log.info(f"ENTER place_test_order req={req_id} "
             f"call#={_place_call_count[0]} gap_since_last={gap:.2f}s")

    # --- idempotency guard: block a second placement within the cooldown.
    # Reveals AND prevents duplicates (returns 'blocked' instead of placing).
    if not _place_lock.acquire(blocking=False):
        log.warning(f"BLOCKED req={req_id}: another placement in progress")
        return jsonify({"success": False, "blocked": True, "req_id": req_id,
                        "reason": "another placement already in progress"}), 429
    try:
        if _last_place_ts[0] > 0 and gap < COOLDOWN_SECONDS:
            log.warning(f"BLOCKED req={req_id}: only {gap:.2f}s since last "
                        f"(cooldown {COOLDOWN_SECONDS}s)")
            return jsonify({"success": False, "blocked": True, "req_id": req_id,
                            "reason": f"cooldown: {gap:.1f}s since last placement, "
                                      f"need {COOLDOWN_SECONDS}s",
                            "hint": "if you only clicked once, a RETRY fired this "
                                    "request again -> that is the duplicate cause"}), 429
        _last_place_ts[0] = now

        async def do(c):
            p = await c.get_symbol_price(symbol=SYMBOL)
            ask = p.get("ask")
            bid = p.get("bid")

            entry = round(bid * (1 - SAFE_DISTANCE_PCT), 2)   # ~12% below market
            risk = round(entry * 0.005, 2)
            if risk < 1:
                risk = 1.0
            stop = round(entry - risk, 2)
            target = round(entry + TEST_RR * risk, 2)

            options = {
                "comment": "JP_STAGE1_TEST",
                "expiration": {
                    "type": "ORDER_TIME_SPECIFIED",
                    "time": datetime.now(timezone.utc) + timedelta(hours=24),
                },
            }
            result = await c.create_limit_buy_order(
                symbol=SYMBOL, volume=TEST_VOLUME, open_price=entry,
                stop_loss=stop, take_profit=target, options=options)

            return {
                "placed": True,
                "req_id": req_id,
                "live_price": {"bid": bid, "ask": ask},
                "order": {
                    "symbol": SYMBOL, "volume": TEST_VOLUME, "entry_limit": entry,
                    "stop_loss": stop, "take_profit": target,
                    "distance_below_market_pct": SAFE_DISTANCE_PCT * 100,
                },
                "result": {
                    "orderId": result.get("orderId"),
                    "stringCode": result.get("stringCode"),
                    "numericCode": result.get("numericCode"),
                },
            }

        try:
            result = asyncio.run(_with_connection(do))
            log.info(f"PLACED req={req_id} order={result.get('result',{}).get('orderId')}")
            return jsonify({"success": True, **result})
        except Exception as e:
            log.error(f"ERROR req={req_id}: {e}")
            return jsonify({"success": False, "req_id": req_id,
                            "error": f"{type(e).__name__}: {e}",
                            "traceback": traceback.format_exc()[-1500:]}), 500
    finally:
        _place_lock.release()


@app.route("/preview_signals")
def preview_signals():
    """DRY RUN: read the detector's real signals and show exactly what we WOULD
    place for each — order type, sized lot, risk, and validity. Places nothing."""
    err = _check_env()
    if err:
        return jsonify(err), 400

    signals, ferr = fetch_detector_signals()
    if ferr:
        return jsonify({"success": False, "error": f"could not reach detector: {ferr}"}), 502
    if not signals:
        return jsonify({"success": True, "count": 0, "previews": [],
                        "note": "detector returned no signals"}), 200

    # newest first, look at the most recent few
    signals = sorted(signals, key=lambda s: s.get("timestamp", ""), reverse=True)[:8]

    async def do(c):
        info = await c.get_account_information()
        balance = info.get("balance")
        previews = []
        for sig in signals:
            sym = sig.get("symbol") or SYMBOL
            try:
                spec_raw = await c.get_symbol_specification(symbol=sym)
                price = await c.get_symbol_price(symbol=sym)
                spec = {
                    "tick_size": spec_raw.get("tickSize"),
                    "tick_value": price.get("lossTickValue"),
                    "contract_size": spec_raw.get("contractSize"),
                    "min_volume": spec_raw.get("minVolume"),
                    "max_volume": spec_raw.get("maxVolume"),
                    "volume_step": spec_raw.get("volumeStep"),
                }
                a = assess_signal(sig, spec, price, balance)
                a["already_placed"] = sig.get("id") in _placed_signal_ids
                previews.append(a)
            except Exception as e:
                previews.append({"id": sig.get("id"), "placeable": False,
                                 "reason": f"assess error: {type(e).__name__}: {e}"})
        return {"balance": balance, "count": len(previews), "previews": previews}

    try:
        result = asyncio.run(_with_connection(do))
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1500:]}), 500


@app.route("/place_signal/<signal_id>")
def place_signal(signal_id):
    """Place ONE order from a specific detector signal (manual trigger).
    Guards: 30s cooldown, signal-id dedup, limit-validity, sizing tradeable."""
    err = _check_env()
    if err:
        return jsonify(err), 400

    req_id = uuid.uuid4().hex[:8]
    now = time.time()
    gap = now - _last_place_ts[0]
    log.info(f"ENTER place_signal id={signal_id} req={req_id} gap={gap:.2f}s")

    # id-level duplicate guard
    if signal_id in _placed_signal_ids:
        return jsonify({"success": False, "blocked": True, "req_id": req_id,
                        "reason": f"signal {signal_id} already placed this session"}), 409

    if not _place_lock.acquire(blocking=False):
        return jsonify({"success": False, "blocked": True, "req_id": req_id,
                        "reason": "another placement in progress"}), 429
    try:
        if _last_place_ts[0] > 0 and gap < COOLDOWN_SECONDS:
            return jsonify({"success": False, "blocked": True, "req_id": req_id,
                            "reason": f"cooldown: {gap:.1f}s since last, need {COOLDOWN_SECONDS}s"}), 429

        signals, ferr = fetch_detector_signals()
        if ferr:
            return jsonify({"success": False, "error": f"could not reach detector: {ferr}"}), 502
        sig = next((s for s in (signals or []) if s.get("id") == signal_id), None)
        if not sig:
            return jsonify({"success": False, "error": f"signal {signal_id} not found"}), 404

        async def do(c):
            info = await c.get_account_information()
            balance = info.get("balance")
            sym = sig.get("symbol") or SYMBOL
            spec_raw = await c.get_symbol_specification(symbol=sym)
            price = await c.get_symbol_price(symbol=sym)
            spec = {
                "tick_size": spec_raw.get("tickSize"),
                "tick_value": price.get("lossTickValue"),
                "contract_size": spec_raw.get("contractSize"),
                "min_volume": spec_raw.get("minVolume"),
                "max_volume": spec_raw.get("maxVolume"),
                "volume_step": spec_raw.get("volumeStep"),
            }
            a = assess_signal(sig, spec, price, balance)
            if not a.get("placeable"):
                return {"placed": False, "assessment": a,
                        "reason": a.get("reason")}

            expiry_hours = sig.get("order_expiry_hours") or 24
            options = {
                "comment": f"JP_SIG_{signal_id}"[:31],
                "expiration": {
                    "type": "ORDER_TIME_SPECIFIED",
                    "time": datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
                },
            }
            if a["order_type"] == "buy_limit":
                res = await c.create_limit_buy_order(
                    symbol=sym, volume=a["lot"], open_price=a["entry"],
                    stop_loss=a["stop"], take_profit=a["target"], options=options)
            else:
                res = await c.create_limit_sell_order(
                    symbol=sym, volume=a["lot"], open_price=a["entry"],
                    stop_loss=a["stop"], take_profit=a["target"], options=options)

            return {"placed": True, "assessment": a,
                    "result": {"orderId": res.get("orderId"),
                               "stringCode": res.get("stringCode"),
                               "numericCode": res.get("numericCode")}}

        result = asyncio.run(_with_connection(do))
        if result.get("placed"):
            _last_place_ts[0] = now
            _placed_signal_ids.add(signal_id)
            log.info(f"PLACED signal {signal_id} order={result.get('result',{}).get('orderId')}")
        return jsonify({"success": True, "req_id": req_id, **result})
    except Exception as e:
        log.error(f"ERROR place_signal {signal_id}: {e}")
        return jsonify({"success": False, "req_id": req_id,
                        "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1500:]}), 500
    finally:
        _place_lock.release()


@app.route("/orders")
def orders():
    err = _check_env()
    if err:
        return jsonify(err), 400

    async def do(c):
        o = await c.get_orders()
        slim = [{
            "id": x.get("id"),
            "type": x.get("type"),
            "symbol": x.get("symbol"),
            "openPrice": x.get("openPrice"),
            "stopLoss": x.get("stopLoss"),
            "takeProfit": x.get("takeProfit"),
            "volume": x.get("volume"),
            "comment": x.get("comment"),
        } for x in o]
        return {"count": len(slim), "orders": slim}

    try:
        result = asyncio.run(_with_connection(do))
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1200:]}), 500


@app.route("/cancel_all")
def cancel_all():
    """Cancel every pending order tagged JP_STAGE1_TEST. Only touches our test
    orders (matched by comment), never anything else on the account."""
    err = _check_env()
    if err:
        return jsonify(err), 400

    async def do(c):
        o = await c.get_orders()
        ours = [x for x in o if (x.get("comment") or "").startswith("JP_STAGE1_TEST")]
        results = []
        for x in ours:
            oid = x.get("id")
            try:
                r = await c.cancel_order(order_id=oid)
                results.append({"id": oid, "stringCode": r.get("stringCode")})
            except Exception as ce:
                results.append({"id": oid, "error": str(ce)})
        return {"found": len(ours), "cancelled": results}

    try:
        result = asyncio.run(_with_connection(do))
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1200:]}), 500


@app.route("/cancel/<order_id>")
def cancel(order_id):
    err = _check_env()
    if err:
        return jsonify(err), 400

    async def do(c):
        result = await c.cancel_order(order_id=order_id)
        return {"cancelled": order_id,
                "stringCode": result.get("stringCode"),
                "numericCode": result.get("numericCode")}

    try:
        result = asyncio.run(_with_connection(do))
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1200:]}), 500


# =========================================================================== #
# STAGE 4a — OUTCOME READ-BACK: PROBE (read-only)
# =========================================================================== #
# Purpose: SEE the real shape MetaApi returns for closed trades, so Stage 4b
# can be written against verified field names instead of guesses. This endpoint
# is strictly READ-ONLY: it places nothing, cancels nothing, writes nothing.
#
#   GET /trade_history            -> last 7 days of deals + history orders (raw)
#   GET /trade_history?days=30    -> custom lookback window
#   GET /trade_history?raw=1      -> return the FULL untouched payload (verbose)
#
# What to look for in the output (this is what Stage 4b needs to confirm):
#   - Does the "comment" field carry "JP_SIG_<id>"? On which deal(s)?
#   - For a CLOSED position: the entry deal (entryType DEAL_ENTRY_IN) and the
#     exit deal (DEAL_ENTRY_OUT) — both share the same positionId.
#   - The "reason" code on the exit deal for an SL-hit vs a TP-hit.
#   - Where profit / price / volume / time actually live.
# =========================================================================== #
@app.route("/trade_history")
def trade_history():
    err = _check_env()
    if err:
        return jsonify(err), 400

    try:
        days = int(request.args.get("days", "7"))
    except ValueError:
        days = 7
    days = max(1, min(days, 365))
    full_raw = request.args.get("raw") in ("1", "true", "yes")

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    async def do(c):
        deals_resp = await c.get_deals_by_time_range(start, end)
        orders_resp = await c.get_history_orders_by_time_range(start, end)

        # These come back as dict-like models: {"deals":[...], "synchronizing":bool}
        deals = (deals_resp or {}).get("deals", []) if isinstance(deals_resp, dict) else getattr(deals_resp, "deals", [])
        hist_orders = (orders_resp or {}).get("historyOrders", []) if isinstance(orders_resp, dict) else getattr(orders_resp, "historyOrders", [])

        # Slim, readable view focused on the fields Stage 4b will use.
        slim_deals = [{
            "id": d.get("id"),
            "type": d.get("type"),
            "entryType": d.get("entryType"),
            "symbol": d.get("symbol"),
            "volume": d.get("volume"),
            "price": d.get("price"),
            "profit": d.get("profit"),
            "commission": d.get("commission"),
            "swap": d.get("swap"),
            "positionId": d.get("positionId"),
            "orderId": d.get("orderId"),
            "comment": d.get("comment"),
            "brokerComment": d.get("brokerComment"),
            "reason": d.get("reason"),
            "brokerTime": d.get("brokerTime"),
            "stopLoss": d.get("stopLoss"),
            "takeProfit": d.get("takeProfit"),
        } for d in deals]

        # Highlight anything carrying our signal tag, for an at-a-glance check.
        jp_tagged = [d for d in slim_deals if (d.get("comment") or "").startswith("JP_SIG_")]

        out = {
            "window": {"from": start.isoformat(), "to": end.isoformat(), "days": days},
            "counts": {"deals": len(slim_deals), "history_orders": len(hist_orders),
                       "jp_tagged_deals": len(jp_tagged)},
            "jp_tagged_deals": jp_tagged,
            "deals": slim_deals,
        }
        if full_raw:
            # Untouched payloads so we can inspect EVERY field, not just the slim set.
            out["raw_deals"] = deals
            out["raw_history_orders"] = hist_orders
        return out

    try:
        result = asyncio.run(_with_connection(do))
        # default=str so datetimes serialize cleanly
        return app.response_class(
            response=__import__("json").dumps({"success": True, **result}, default=str, indent=2),
            mimetype="application/json",
        )
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1500:]}), 500


# =========================================================================== #
# STAGE 4b (DRY) — OUTCOME COMPUTATION (read-only, writes nothing)
# =========================================================================== #
# Reads closed JP_SIG positions and COMPUTES what each signal's outcome record
# should be — outcome, r_result, fill/exit price+time, pnl, lot_size — and
# returns them. It does NOT write anything anywhere yet. Once you confirm the
# numbers match reality (the known SL trade should read loss / -1.0R / -49.2),
# 4b-commit will extend the detector's /update_signal and flip this to write.
#
#   GET /sync_outcomes          -> compute outcomes for last 30 days (dry run)
#   GET /sync_outcomes?days=60  -> custom lookback
#
# R math (verified by hand against position 2797990653):
#   risk_per_unit = |entry_fill - stopLoss|
#   favorable     = (entry_fill - exit) for SELL, (exit - entry_fill) for BUY
#   r_result      = favorable / risk_per_unit
# All values come from the deals themselves; no signal fetch needed to compute.
# =========================================================================== #
def _sig_id_from_comment(comment):
    """'JP_SIG_20260601_164500_b' -> '20260601_164500_b' (None if not ours)."""
    if comment and comment.startswith("JP_SIG_"):
        return comment[len("JP_SIG_"):]
    return None


def _vwap(deals, price_key="price", vol_key="volume"):
    """Volume-weighted average price across a set of deals (handles partials)."""
    tv = sum((d.get(vol_key) or 0) for d in deals)
    if tv <= 0:
        return None, 0.0
    p = sum((d.get(price_key) or 0) * (d.get(vol_key) or 0) for d in deals) / tv
    return p, tv


def _build_outcomes(deals, hist_orders):
    """Pure function: turn raw deals/orders into per-signal outcome records."""
    EPS = 0.10  # |r| below this is treated as breakeven

    # group our deals by positionId
    by_pos = {}
    for d in deals:
        if not _sig_id_from_comment(d.get("comment")):
            continue
        pid = d.get("positionId")
        if pid is None:
            continue
        by_pos.setdefault(pid, []).append(d)

    records, skipped_open = [], []
    seen_positions = set()

    for pid, dl in by_pos.items():
        seen_positions.add(pid)
        ins = [d for d in dl if d.get("entryType") == "DEAL_ENTRY_IN"]
        outs = [d for d in dl if d.get("entryType") == "DEAL_ENTRY_OUT"]
        sig_id = _sig_id_from_comment((ins or dl)[0].get("comment"))

        if not ins:
            continue  # no entry deal — nothing to do
        if not outs:
            skipped_open.append({"signal_id": sig_id, "positionId": pid,
                                  "note": "position still open — not settled"})
            continue

        is_sell = (ins[0].get("type") == "DEAL_TYPE_SELL")
        entry_px, _ = _vwap(ins)
        exit_px, exit_vol = _vwap(outs)
        stop = ins[0].get("stopLoss")
        lot = sum((d.get("volume") or 0) for d in ins)

        pnl = sum((d.get("profit") or 0) + (d.get("commission") or 0) + (d.get("swap") or 0) for d in dl)
        pnl = round(pnl, 2)

        # r_result from prices (broker truth)
        r_result = None
        if entry_px is not None and exit_px is not None and stop not in (None, 0):
            risk = abs(entry_px - stop)
            if risk > 0:
                favorable = (entry_px - exit_px) if is_sell else (exit_px - entry_px)
                r_result = round(favorable / risk, 3)

        if r_result is None:
            outcome = "win" if pnl > 0 else "loss" if pnl < 0 else "breakeven"
        elif r_result >= EPS:
            outcome = "win"
        elif r_result <= -EPS:
            outcome = "loss"
        else:
            outcome = "breakeven"

        exit_reason = (outs[-1].get("reason") or "")
        records.append({
            "signal_id": sig_id,
            "positionId": pid,
            "order_placed": True,
            "fill_status": "filled",
            "fill_price": entry_px,
            "fill_time": ins[0].get("brokerTime"),
            "exit_price": exit_px,
            "exit_time": outs[-1].get("brokerTime"),
            "lot_size": round(lot, 2),
            "outcome": outcome,
            "r_result": r_result,
            "pnl": pnl,
            "_exit_reason": exit_reason,  # cross-check only (leading _ = not persisted)
        })

    # no-fill detection: our orders that ended without ever opening a position
    DEAD = {"ORDER_STATE_CANCELED", "ORDER_STATE_EXPIRED", "ORDER_STATE_REJECTED"}
    for o in hist_orders:
        sig_id = _sig_id_from_comment(o.get("comment"))
        if not sig_id:
            continue
        if o.get("positionId") in seen_positions:
            continue  # it filled — handled above
        if o.get("state") in DEAD:
            records.append({
                "signal_id": sig_id,
                "positionId": o.get("positionId"),
                "order_placed": True,
                "fill_status": "no-fill",
                "fill_price": None, "fill_time": None,
                "exit_price": None, "exit_time": None,
                "lot_size": None,
                "outcome": "no-fill",
                "r_result": 0.0,
                "pnl": 0.0,
                "_order_state": o.get("state"),
            })

    return records, skipped_open


@app.route("/sync_outcomes")
def sync_outcomes():
    err = _check_env()
    if err:
        return jsonify(err), 400

    try:
        days = int(request.args.get("days", "30"))
    except ValueError:
        days = 30
    days = max(1, min(days, 365))
    commit = request.args.get("commit") in ("1", "true", "yes")

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    async def do(c):
        deals_resp = await c.get_deals_by_time_range(start, end)
        orders_resp = await c.get_history_orders_by_time_range(start, end)
        deals = (deals_resp or {}).get("deals", []) if isinstance(deals_resp, dict) else getattr(deals_resp, "deals", [])
        hist = (orders_resp or {}).get("historyOrders", []) if isinstance(orders_resp, dict) else getattr(orders_resp, "historyOrders", [])
        records, open_positions = _build_outcomes(deals, hist)
        return records, open_positions

    try:
        records, open_positions = asyncio.run(_with_connection(do))

        write_results = None
        if commit:
            write_results = _commit_outcomes(records)

        payload = {
            "success": True,
            "dry_run": not commit,
            "note": ("written to detector" if commit else "computed only — nothing was written"),
            "window": {"days": days, "from": start.isoformat(), "to": end.isoformat()},
            "settled_count": len(records),
            "open_unsettled": open_positions,
            "outcomes": records,
        }
        if write_results is not None:
            payload["writes"] = write_results
        return app.response_class(
            response=__import__("json").dumps(payload, default=str, indent=2),
            mimetype="application/json",
        )
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1500:]}), 500


def _commit_outcomes(records):
    """POST each computed outcome to the detector's /update_signal. Only the
    persisted execution fields are sent (keys starting with _ are dropped).
    Idempotent: re-sending the same outcome merely re-writes identical values."""
    if not DETECTOR_URL or not WORKER_TOKEN:
        return [{"error": "DETECTOR_URL or WORKER_TOKEN not set on the worker"}]
    PERSIST = {"order_placed", "fill_status", "fill_price", "fill_time",
               "outcome", "exit_price", "exit_time", "r_result", "lot_size", "pnl"}
    results = []
    for rec in records:
        sid = rec.get("signal_id")
        body = {"id": sid}
        body.update({k: v for k, v in rec.items() if k in PERSIST})
        try:
            r = requests.post(
                f"{DETECTOR_URL}/update_signal",
                headers={"Content-Type": "application/json", "X-Worker-Token": WORKER_TOKEN},
                json=body, timeout=20)
            results.append({"signal_id": sid, "status": r.status_code,
                            "ok": r.ok, "resp": r.text[:200]})
        except Exception as e:
            results.append({"signal_id": sid, "ok": False, "error": f"{type(e).__name__}: {e}"})
    return results


@app.route("/watcher_status")
def watcher_status():
    """Read-only view of the watcher: armed/paused state, caps, last run, last
    evaluation (what it would/did place), and the last auto-placement."""
    return jsonify({
        "success": True,
        "stage": "5b (autonomous placement, capped)" if AUTO_PLACE else "5b code, read-only (AUTO_PLACE off)",
        "places_orders": bool(AUTO_PLACE and not _place_paused[0]),
        **_watcher_state,
        "paused": _place_paused[0],
        "backlog_captured": _backlog_captured[0],
    })


@app.route("/watcher/pause")
def watcher_pause():
    """KILL-SWITCH: stop auto-placement immediately (no redeploy). The loop keeps
    evaluating read-only; it just won't place until /watcher/resume."""
    _place_paused[0] = True
    _watcher_state["paused"] = True
    log.info("[watcher] PAUSED via kill-switch — placement halted")
    return jsonify({"success": True, "paused": True,
                    "note": "auto-placement halted; loop still evaluates read-only"})


@app.route("/watcher/resume")
def watcher_resume():
    """Re-arm auto-placement after a pause (only matters if AUTO_PLACE=true)."""
    _place_paused[0] = False
    _watcher_state["paused"] = False
    log.info("[watcher] RESUMED via kill-switch")
    return jsonify({"success": True, "paused": False,
                    "armed": bool(AUTO_PLACE)})


# Start the watcher at import time so it runs under gunicorn.
# No-op unless WATCHER_ENABLED=true. Placement additionally requires AUTO_PLACE=true.
_maybe_start_watcher()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
