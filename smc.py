from typing import Dict, List, Optional


# ── UTILS ─────────────────────────────────────────────────────────────────────

def pip_size(instrument: str) -> float:
    if "JPY" in instrument:
        return 0.01
    if "XAU" in instrument or "GOLD" in instrument:
        return 0.1
    if "BTC" in instrument:
        return 1.0
    return 0.0001


def fmt_price(price: float, instrument: str) -> str:
    if "XAU" in instrument:
        return f"{price:.2f}"
    if "BTC" in instrument:
        return f"{price:.0f}"
    if "JPY" in instrument:
        return f"{price:.3f}"
    return f"{price:.5f}"


# ── STRUCTURE ─────────────────────────────────────────────────────────────────

def find_swing_highs_lows(candles: list, lookback: int = 3) -> dict:
    """Find swing highs and lows."""
    highs, lows = [], []
    for i in range(lookback, len(candles) - lookback):
        c = candles[i]
        # Swing high
        if all(c["h"] >= candles[i-j]["h"] for j in range(1, lookback+1)) and \
           all(c["h"] >= candles[i+j]["h"] for j in range(1, lookback+1)):
            highs.append({"price": c["h"], "index": i, "time": c["time"]})
        # Swing low
        if all(c["l"] <= candles[i-j]["l"] for j in range(1, lookback+1)) and \
           all(c["l"] <= candles[i+j]["l"] for j in range(1, lookback+1)):
            lows.append({"price": c["l"], "index": i, "time": c["time"]})
    return {"highs": highs[-5:], "lows": lows[-5:]}


def detect_market_structure(candles: list) -> dict:
    """Detect BOS and CHoCH."""
    if len(candles) < 10:
        return {"trend": "unknown", "last_bos": None, "last_choch": None}

    swings = find_swing_highs_lows(candles)
    highs = swings["highs"]
    lows = swings["lows"]

    trend = "unknown"
    last_bos = None
    last_choch = None

    if len(highs) >= 2 and len(lows) >= 2:
        # Higher highs + higher lows = bullish
        if highs[-1]["price"] > highs[-2]["price"] and lows[-1]["price"] > lows[-2]["price"]:
            trend = "bullish"
        # Lower highs + lower lows = bearish
        elif highs[-1]["price"] < highs[-2]["price"] and lows[-1]["price"] < lows[-2]["price"]:
            trend = "bearish"
        else:
            trend = "ranging"

        # BOS: break above last high (bullish) or below last low (bearish)
        last_close = candles[-1]["c"]
        if trend == "bullish" and last_close > highs[-2]["price"]:
            last_bos = {"type": "bullish_bos", "level": highs[-2]["price"]}
        elif trend == "bearish" and last_close < lows[-2]["price"]:
            last_bos = {"type": "bearish_bos", "level": lows[-2]["price"]}

        # CHoCH: if bullish trend breaks last low, or bearish breaks last high
        if trend == "bullish" and last_close < lows[-1]["price"]:
            last_choch = {"type": "bearish_choch", "level": lows[-1]["price"]}
        elif trend == "bearish" and last_close > highs[-1]["price"]:
            last_choch = {"type": "bullish_choch", "level": highs[-1]["price"]}

    return {"trend": trend, "last_bos": last_bos, "last_choch": last_choch,
            "swing_highs": highs, "swing_lows": lows}


# ── ORDER BLOCKS ──────────────────────────────────────────────────────────────

def find_order_blocks(candles: list, structure: dict) -> list:
    """Find Order Blocks — last bearish candle before bullish move and vice versa."""
    obs = []
    if len(candles) < 5:
        return obs

    trend = structure.get("trend", "unknown")

    for i in range(2, len(candles) - 2):
        c = candles[i]
        next_c = candles[i + 1]
        next2_c = candles[i + 2]

        body_size = abs(c["c"] - c["o"])
        if body_size == 0:
            continue

        # Bullish OB: bearish candle followed by strong bullish move
        if c["c"] < c["o"]:  # bearish candle
            move_up = (next2_c["h"] - c["l"]) / body_size if body_size > 0 else 0
            if next_c["c"] > c["h"] and move_up > 1.5:
                obs.append({
                    "type": "bullish_ob",
                    "top": c["o"],
                    "bottom": c["l"],
                    "index": i,
                    "time": c["time"],
                    "strength": min(move_up / 2, 3.0)
                })

        # Bearish OB: bullish candle followed by strong bearish move
        elif c["c"] > c["o"]:  # bullish candle
            move_down = (c["h"] - next2_c["l"]) / body_size if body_size > 0 else 0
            if next_c["c"] < c["l"] and move_down > 1.5:
                obs.append({
                    "type": "bearish_ob",
                    "top": c["h"],
                    "bottom": c["o"],
                    "index": i,
                    "time": c["time"],
                    "strength": min(move_down / 2, 3.0)
                })

    # Return last 3 most relevant
    bullish_obs = [o for o in obs if o["type"] == "bullish_ob"][-2:]
    bearish_obs = [o for o in obs if o["type"] == "bearish_ob"][-2:]
    return bullish_obs + bearish_obs


# ── FAIR VALUE GAPS ───────────────────────────────────────────────────────────

def find_fvg(candles: list) -> list:
    """Find Fair Value Gaps (Imbalances)."""
    fvgs = []
    if len(candles) < 3:
        return fvgs

    for i in range(1, len(candles) - 1):
        prev = candles[i - 1]
        curr = candles[i]
        nxt = candles[i + 1]

        # Bullish FVG: gap between prev high and next low
        if nxt["l"] > prev["h"]:
            fvgs.append({
                "type": "bullish_fvg",
                "top": nxt["l"],
                "bottom": prev["h"],
                "mid": (nxt["l"] + prev["h"]) / 2,
                "index": i,
                "time": curr["time"],
                "filled": False
            })

        # Bearish FVG: gap between prev low and next high
        elif nxt["h"] < prev["l"]:
            fvgs.append({
                "type": "bearish_fvg",
                "top": prev["l"],
                "bottom": nxt["h"],
                "mid": (prev["l"] + nxt["h"]) / 2,
                "index": i,
                "time": curr["time"],
                "filled": False
            })

    # Mark filled FVGs
    last_close = candles[-1]["c"] if candles else 0
    for fvg in fvgs:
        if fvg["type"] == "bullish_fvg" and last_close < fvg["bottom"]:
            fvg["filled"] = True
        elif fvg["type"] == "bearish_fvg" and last_close > fvg["top"]:
            fvg["filled"] = True

    # Return only unfilled, last 4
    unfilled = [f for f in fvgs if not f["filled"]]
    return unfilled[-4:]


# ── LIQUIDITY ─────────────────────────────────────────────────────────────────

def find_liquidity_levels(candles: list) -> dict:
    """Find liquidity pools: equal highs/lows, swing points."""
    if len(candles) < 10:
        return {"buy_side": [], "sell_side": []}

    # Recent swing highs = sell-side liquidity (stops above)
    # Recent swing lows = buy-side liquidity (stops below)
    swings = find_swing_highs_lows(candles, lookback=4)

    buy_side = [{"price": l["price"], "time": l["time"]} for l in swings["lows"][-3:]]
    sell_side = [{"price": h["price"], "time": h["time"]} for h in swings["highs"][-3:]]

    return {"buy_side": buy_side, "sell_side": sell_side}


def detect_liquidity_sweep(candles: list, liquidity: dict) -> Optional[dict]:
    """Detect if price recently swept liquidity."""
    if len(candles) < 3:
        return None

    last = candles[-1]
    prev = candles[-2]

    # Sweep of buy-side liquidity (wick below swing low then close above)
    for lvl in liquidity["buy_side"]:
        if last["l"] < lvl["price"] and last["c"] > lvl["price"]:
            return {
                "type": "sweep_buy_side",
                "level": lvl["price"],
                "direction": "bullish",
                "desc": f"Sweep BSL (стопи під {lvl['price']:.5f})"
            }

    # Sweep of sell-side liquidity (wick above swing high then close below)
    for lvl in liquidity["sell_side"]:
        if last["h"] > lvl["price"] and last["c"] < lvl["price"]:
            return {
                "type": "sweep_sell_side",
                "level": lvl["price"],
                "direction": "bearish",
                "desc": f"Sweep SSL (стопи над {lvl['price']:.5f})"
            }

    return None


# ── KEY LEVELS ────────────────────────────────────────────────────────────────

def get_key_levels(candles_by_tf: dict) -> dict:
    """Extract PDH/PDL/PWH/PWL/PMH/PML."""
    levels = {}

    # Previous Day High/Low (from H4 or D)
    d_candles = candles_by_tf.get("D", [])
    if len(d_candles) >= 3:
        pd = d_candles[-2]
        ppd = d_candles[-3]
        levels["PDH"] = pd["h"]
        levels["PDL"] = pd["l"]
        levels["PPD_H"] = ppd["h"]
        levels["PPD_L"] = ppd["l"]

    # Previous Week High/Low
    w_candles = candles_by_tf.get("W", [])
    if len(w_candles) >= 2:
        pw = w_candles[-2]
        levels["PWH"] = pw["h"]
        levels["PWL"] = pw["l"]

    # Previous Month High/Low
    m_candles = candles_by_tf.get("M", [])
    if len(m_candles) >= 2:
        pm = m_candles[-2]
        levels["PMH"] = pm["h"]
        levels["PML"] = pm["l"]

    return levels


# ── PREMIUM / DISCOUNT ────────────────────────────────────────────────────────

def get_premium_discount(candles: list) -> dict:
    """Determine if price is in premium or discount zone."""
    if len(candles) < 20:
        return {"zone": "unknown", "equilibrium": None, "percent": None}

    recent = candles[-50:] if len(candles) >= 50 else candles
    high = max(c["h"] for c in recent)
    low = min(c["l"] for c in recent)
    equilibrium = (high + low) / 2
    current = candles[-1]["c"]

    percent = (current - low) / (high - low) * 100 if high != low else 50

    if percent > 62:
        zone = "premium"
    elif percent < 38:
        zone = "discount"
    else:
        zone = "equilibrium"

    return {"zone": zone, "equilibrium": equilibrium, "percent": round(percent, 1),
            "range_high": high, "range_low": low}


# ── SETUP DETECTION ───────────────────────────────────────────────────────────

def score_setup(structure_1h: dict, fvg_15m: list, ob_1h: list,
                liquidity_sweep: Optional[dict], pd_zone: dict) -> int:
    """Score setup quality 0-5."""
    score = 0

    # Trend alignment
    if structure_1h.get("trend") in ("bullish", "bearish"):
        score += 1

    # FVG present on 15m
    if fvg_15m:
        score += 1

    # OB present on 1H
    if ob_1h:
        score += 1

    # Liquidity sweep
    if liquidity_sweep:
        score += 1

    # Price in discount (for buys) or premium (for sells)
    trend = structure_1h.get("trend", "unknown")
    zone = pd_zone.get("zone", "unknown")
    if (trend == "bullish" and zone == "discount") or (trend == "bearish" and zone == "premium"):
        score += 1

    return score


# ── MAIN ANALYSIS ─────────────────────────────────────────────────────────────

def analyze_smc(candles_by_tf: dict) -> dict:
    """Full SMC analysis across all timeframes."""
    result = {}

    # Per-timeframe structure
    for tf in ["M", "W", "D", "H4", "H1", "M15"]:
        c = candles_by_tf.get(tf, [])
        if c:
            result[f"structure_{tf}"] = detect_market_structure(c)
            result[f"ob_{tf}"] = find_order_blocks(c, result[f"structure_{tf}"])
            result[f"fvg_{tf}"] = find_fvg(c)
            result[f"liquidity_{tf}"] = find_liquidity_levels(c)
            result[f"sweep_{tf}"] = detect_liquidity_sweep(c, result[f"liquidity_{tf}"])
            result[f"pd_zone_{tf}"] = get_premium_discount(c)

            # Current price
            if c:
                result["current_price"] = c[-1]["c"]

    # Key levels from higher TFs
    result["key_levels"] = get_key_levels(candles_by_tf)

    # Setup quality score
    structure_1h = result.get("structure_H1", {})
    fvg_15m = result.get("fvg_M15", [])
    ob_1h = result.get("ob_H1", [])
    sweep_15m = result.get("sweep_M15")
    pd_zone = result.get("pd_zone_H1", {})

    score = score_setup(structure_1h, fvg_15m, ob_1h, sweep_15m, pd_zone)
    result["setup_quality"] = score
    result["has_setup"] = score >= 3

    return result
