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
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify

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

TOKEN = os.getenv("METAAPI_TOKEN", "")
ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")

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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
