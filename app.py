"""
MetaApi Connection Test — read-only.
A tiny web service for Railway. Hitting /test runs a one-shot connection check:
connect to the account, read balance, list gold symbols, read a price.
NO orders are placed. This only proves the websocket holds from this host.

Environment variables required (set in Railway, never in code):
  METAAPI_TOKEN       - your MetaApi access token
  METAAPI_ACCOUNT_ID  - your account id (4e2294fd-...)
"""

import os
import asyncio
import traceback
from flask import Flask, jsonify

app = Flask(__name__)

TOKEN = os.getenv("METAAPI_TOKEN", "")
ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")


async def run_connection_test():
    from metaapi_cloud_sdk import MetaApi
    result = {"steps": []}

    def step(name, ok, detail=""):
        result["steps"].append({"step": name, "ok": ok, "detail": str(detail)})

    if not TOKEN or not ACCOUNT_ID:
        step("env_vars", False, "METAAPI_TOKEN or METAAPI_ACCOUNT_ID not set")
        result["success"] = False
        return result

    api = None
    connection = None
    try:
        api = MetaApi(TOKEN)
        step("sdk_init", True, "MetaApi client created")

        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
        step("get_account", True, f"state={account.state} conn={account.connection_status}")

        # Ensure deployed (no-op if already deployed)
        try:
            if account.state != "DEPLOYED":
                await account.deploy()
                step("deploy", True, "deploy requested")
        except Exception as e:
            step("deploy", True, f"skip ({e})")

        # Wait for the account to be connected to the broker
        await account.wait_connected()
        step("wait_connected", True, "account connected to broker")

        connection = account.get_rpc_connection()
        await connection.connect()
        step("rpc_connect", True, "rpc connection opened")

        # The make-or-break step: full terminal sync over websocket
        await connection.wait_synchronized({"timeoutInSeconds": 120})
        step("wait_synchronized", True, "terminal synchronized")

        info = await connection.get_account_information()
        result["account"] = {
            "balance": info.get("balance"),
            "currency": info.get("currency"),
            "equity": info.get("equity"),
            "leverage": info.get("leverage"),
            "broker": info.get("broker"),
        }
        step("account_information", True, f"balance={info.get('balance')} {info.get('currency')}")

        symbols = await connection.get_symbols()
        golds = [s for s in symbols if "XAU" in s.upper()]
        result["gold_symbols"] = golds
        step("get_symbols", True, f"{len(symbols)} symbols, gold={golds}")

        if golds:
            sym = golds[0]
            price = await connection.get_symbol_price(sym)
            result["gold_price"] = {"symbol": sym, "bid": price.get("bid"), "ask": price.get("ask")}
            step("get_symbol_price", True, f"{sym} bid={price.get('bid')} ask={price.get('ask')}")

        result["success"] = True
    except Exception as e:
        step("ERROR", False, f"{type(e).__name__}: {e}")
        result["success"] = False
        result["traceback"] = traceback.format_exc()[-1500:]
    finally:
        try:
            if connection:
                await connection.close()
        except Exception:
            pass
    return result


@app.route("/")
def home():
    return jsonify({
        "service": "MetaApi connection test (read-only)",
        "configured": bool(TOKEN and ACCOUNT_ID),
        "usage": "GET /test to run the one-shot connection check",
    })


@app.route("/test")
def test():
    try:
        result = asyncio.run(run_connection_test())
        code = 200 if result.get("success") else 500
        return jsonify(result), code
    except Exception as e:
        return jsonify({"success": False, "fatal": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-1500:]}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
