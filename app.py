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
import traceback
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify

app = Flask(__name__)

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

    async def do(c):
        # 1. Read live price
        p = await c.get_symbol_price(symbol=SYMBOL)
        ask = p.get("ask")
        bid = p.get("bid")

        # 2. Compute a SAFE buy-limit far below market (cannot fill)
        entry = round(bid * (1 - SAFE_DISTANCE_PCT), 2)   # ~12% below
        risk = round(entry * 0.005, 2)                    # small risk band for the test
        if risk < 1:
            risk = 1.0
        stop = round(entry - risk, 2)                     # below entry
        target = round(entry + TEST_RR * risk, 2)         # 3R above entry

        # 3. Place the buy-limit with 24h expiry
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
            "live_price": {"bid": bid, "ask": ask},
            "order": {
                "symbol": SYMBOL,
                "volume": TEST_VOLUME,
                "entry_limit": entry,
                "stop_loss": stop,
                "take_profit": target,
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
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1500:]}), 500


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
