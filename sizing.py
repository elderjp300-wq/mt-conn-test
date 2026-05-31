"""
Position sizing — pure math, no network, fully testable.

Given account balance, risk %, entry, stop, and the broker's symbol spec,
compute the lot size so that IF the stop is hit, the loss equals risk% of
balance. This module places no orders; it only calculates.

SAFETY PRINCIPLES baked in:
  - Lot is rounded DOWN to the broker's volume step, so actual risk never
    EXCEEDS the target (rounding up would over-risk — never do that).
  - If the smallest allowed lot (min_volume) would risk MORE than target,
    the trade is flagged NOT tradeable (lot 0) rather than silently
    over-risking. The caller decides what to do.
  - Everything intermediate is returned, so the math is always auditable.
"""

import math
from typing import Optional, Dict, Any

EPS = 1e-9


def _step_decimals(step: float) -> int:
    """Number of decimal places implied by a volume step (0.01 -> 2)."""
    s = f"{step:.10f}".rstrip("0")
    if "." in s:
        return len(s.split(".")[1])
    return 0


def compute_lot_size(
    balance: float,
    risk_percent: float,
    entry: float,
    stop: float,
    spec: Dict[str, Any],
) -> Dict[str, Any]:
    """
    spec keys:
      tick_size    (float, required)  minimum price increment, e.g. 0.01
      tick_value   (float, optional)  account-ccy loss value of ONE tick per 1.0 lot
      contract_size(float, optional)  units per lot — fallback if tick_value missing
      min_volume   (float, required)
      max_volume   (float, required)
      volume_step  (float, required)

    Returns a dict with the computed lot and every intermediate value.
    """
    out: Dict[str, Any] = {
        "inputs": {
            "balance": balance, "risk_percent": risk_percent,
            "entry": entry, "stop": stop, "spec": spec,
        }
    }

    # --- guard rails ---
    if balance <= 0 or risk_percent <= 0:
        out.update(ok=False, error="balance and risk_percent must be positive")
        return out
    price_distance = abs(entry - stop)
    if price_distance <= 0:
        out.update(ok=False, error="entry and stop cannot be equal")
        return out

    tick_size = spec.get("tick_size")
    if not tick_size or tick_size <= 0:
        out.update(ok=False, error="spec.tick_size missing or invalid")
        return out
    try:
        min_v = float(spec["min_volume"])
        max_v = float(spec["max_volume"])
        step = float(spec["volume_step"])
    except (KeyError, TypeError, ValueError):
        out.update(ok=False, error="spec needs min_volume, max_volume, volume_step")
        return out
    if step <= 0:
        out.update(ok=False, error="spec.volume_step must be positive")
        return out

    risk_amount = balance * (risk_percent / 100.0)

    # --- loss per 1.0 lot if the stop is hit ---
    tick_value = spec.get("tick_value")
    if tick_value and tick_value > 0:
        ticks = price_distance / tick_size
        loss_per_lot = ticks * tick_value
        method = "tick_value"
    elif spec.get("contract_size"):
        # valid when the quote currency equals the account currency (USD here)
        loss_per_lot = price_distance * float(spec["contract_size"])
        method = "contract_size"
    else:
        out.update(ok=False, error="spec needs tick_value or contract_size")
        return out
    if loss_per_lot <= 0:
        out.update(ok=False, error="computed loss_per_lot <= 0")
        return out

    raw_lot = risk_amount / loss_per_lot

    # --- round DOWN to the volume step (epsilon guards float error) ---
    decimals = _step_decimals(step)
    stepped = math.floor(raw_lot / step + EPS) * step
    stepped = round(stepped, decimals)

    tradeable = True
    flag = None
    lot = stepped

    if lot > max_v:
        lot = round(max_v, decimals)
        flag = f"capped at max_volume {max_v}"
    if lot < min_v:
        # smallest allowed lot would over-risk -> refuse, don't silently bump
        tradeable = False
        min_risk = loss_per_lot * min_v
        flag = (f"computed lot {stepped} below min_volume {min_v}; "
                f"min lot would risk {min_risk:.2f} "
                f"({min_risk / balance * 100:.2f}% > target {risk_percent}%)")
        lot = 0.0

    actual_risk = loss_per_lot * lot
    out.update(
        ok=True,
        tradeable=tradeable,
        lot=lot,
        method=method,
        risk_amount_target=round(risk_amount, 2),
        price_distance=round(price_distance, 5),
        loss_per_lot=round(loss_per_lot, 4),
        raw_lot=round(raw_lot, 6),
        actual_risk=round(actual_risk, 2),
        actual_risk_percent=round(actual_risk / balance * 100, 4) if balance else None,
        flag=flag,
    )
    return out
