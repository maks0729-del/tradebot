import os
import asyncio
import logging
import aiohttp
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
TWELVEDATA_API_KEY = os.environ["TWELVEDATA_API_KEY"]

TWELVEDATA_BASE = "https://api.twelvedata.com"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
INSTRUMENTS = ["EUR_USD", "GBP_USD", "XAU_USD", "BTC_USD"]
ALERT_USERS = set()
SENT_ALERTS = {}
MIN_PRICE_CHANGE_PIPS = 20

SYMBOL_MAP = {
    "EUR_USD": "EUR/USD",
    "GBP_USD": "GBP/USD",
    "XAU_USD": "XAU/USD",
    "BTC_USD": "BTC/USD",
}

TIMEFRAMES = {
    "M":   {"interval": "1month",  "count": 12},
    "W":   {"interval": "1week",   "count": 12},
    "D":   {"interval": "1day",    "count": 30},
    "H4":  {"interval": "4h",      "count": 60},
    "H1":  {"interval": "1h",      "count": 100},
    "M15": {"interval": "15min",   "count": 96},
    "M5":  {"interval": "5min",    "count": 60},
}

# 5M only for forex and gold (BTC is too noisy on 5M)
TIMEFRAMES_NO_5M = ["BTC_USD"]


def is_killzone():
    hour = datetime.now(timezone.utc).hour
    return (8 <= hour < 12) or (13 <= hour < 17)


def is_asian_session():
    hour = datetime.now(timezone.utc).hour
    return 22 <= hour or hour < 8


# ── DATA ──────────────────────────────────────────────────────────────────────

async def fetch_twelvedata(session, api_key, symbol, interval, count):
    url = (
        TWELVEDATA_BASE + "/time_series"
        "?symbol=" + symbol +
        "&interval=" + interval +
        "&outputsize=" + str(count) +
        "&apikey=" + api_key +
        "&format=JSON"
    )
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        if resp.status != 200:
            raise Exception("TwelveData HTTP error: " + str(resp.status))
        data = await resp.json()
    if data.get("status") == "error":
        raise Exception("TwelveData error: " + data.get("message", "Unknown"))
    values = data.get("values", [])
    if not values:
        return []
    values.reverse()
    candles = []
    for v in values:
        try:
            candles.append({
                "time": v["datetime"],
                "o": float(v["open"]),
                "h": float(v["high"]),
                "l": float(v["low"]),
                "c": float(v["close"]),
                "volume": int(float(v.get("volume", 0))),
            })
        except (KeyError, ValueError):
            continue
    return candles


async def fetch_candles(instrument, api_key):
    symbol = SYMBOL_MAP.get(instrument)
    if not symbol:
        raise Exception("Невідомий інструмент: " + instrument)
    result = {}
    async with aiohttp.ClientSession() as session:
        for tf_label, tf_cfg in TIMEFRAMES.items():
            # Skip 5M for BTC - too noisy
            if tf_label == "M5" and instrument in TIMEFRAMES_NO_5M:
                result[tf_label] = []
                continue
            try:
                candles = await fetch_twelvedata(session, api_key, symbol, tf_cfg["interval"], tf_cfg["count"])
                result[tf_label] = candles
            except Exception as e:
                logger.error("TwelveData error " + tf_label + ": " + str(e))
                result[tf_label] = []
            await asyncio.sleep(0.5)
    return result


def get_session_info():
    now = datetime.now(timezone.utc)
    hour = now.hour
    if 22 <= hour or hour < 8:
        return {"name": "Азійська", "emoji": "🌏", "active": True}
    elif 8 <= hour < 12:
        return {"name": "Лондонська", "emoji": "🇬🇧", "active": True}
    elif 12 <= hour < 17:
        return {"name": "Нью-Йорк", "emoji": "🗽", "active": True}
    else:
        return {"name": "Між сесіями", "emoji": "😴", "active": False}


# ── SMC ───────────────────────────────────────────────────────────────────────

def find_swing_highs_lows(candles, lookback=3):
    highs, lows = [], []
    for i in range(lookback, len(candles) - lookback):
        c = candles[i]
        if all(c["h"] >= candles[i-j]["h"] for j in range(1, lookback+1)) and \
           all(c["h"] >= candles[i+j]["h"] for j in range(1, lookback+1)):
            highs.append({"price": c["h"], "index": i, "time": c["time"]})
        if all(c["l"] <= candles[i-j]["l"] for j in range(1, lookback+1)) and \
           all(c["l"] <= candles[i+j]["l"] for j in range(1, lookback+1)):
            lows.append({"price": c["l"], "index": i, "time": c["time"]})
    return {"highs": highs[-5:], "lows": lows[-5:]}


def detect_5m_trigger(candles_5m, bias):
    """Detect BOS/CHoCH on 5M as entry trigger in direction of bias."""
    if not candles_5m or len(candles_5m) < 10:
        return {"trigger": None, "desc": "немає даних"}

    structure = detect_market_structure(candles_5m)
    trend = structure.get("trend", "unknown")
    last_bos = structure.get("last_bos")
    last_choch = structure.get("last_choch")

    # Bullish bias — look for bullish BOS or CHoCH on 5M
    if bias == "bullish":
        if last_choch and "bullish" in last_choch.get("type", ""):
            return {
                "trigger": "bullish_choch",
                "level": last_choch["level"],
                "desc": "✅ CHoCH вгору на 5M — тригер підтверджено",
                "confirmed": True
            }
        if last_bos and "bullish" in last_bos.get("type", ""):
            return {
                "trigger": "bullish_bos",
                "level": last_bos["level"],
                "desc": "✅ BOS вгору на 5M — тригер підтверджено",
                "confirmed": True
            }
        if trend == "bullish":
            return {
                "trigger": "bullish_structure",
                "level": None,
                "desc": "⏳ 5M структура бичача — чекай CHoCH для входу",
                "confirmed": False
            }
        return {
            "trigger": None,
            "desc": "❌ 5M ще не підтвердив bullish — не входити",
            "confirmed": False
        }

    # Bearish bias — look for bearish BOS or CHoCH on 5M
    elif bias == "bearish":
        if last_choch and "bearish" in last_choch.get("type", ""):
            return {
                "trigger": "bearish_choch",
                "level": last_choch["level"],
                "desc": "✅ CHoCH вниз на 5M — тригер підтверджено",
                "confirmed": True
            }
        if last_bos and "bearish" in last_bos.get("type", ""):
            return {
                "trigger": "bearish_bos",
                "level": last_bos["level"],
                "desc": "✅ BOS вниз на 5M — тригер підтверджено",
                "confirmed": True
            }
        if trend == "bearish":
            return {
                "trigger": "bearish_structure",
                "level": None,
                "desc": "⏳ 5M структура ведмежа — чекай CHoCH для входу",
                "confirmed": False
            }
        return {
            "trigger": None,
            "desc": "❌ 5M ще не підтвердив bearish — не входити",
            "confirmed": False
        }

    return {"trigger": None, "desc": "⏳ Bias не визначено — чекай", "confirmed": False}


def detect_market_structure(candles):
    if len(candles) < 10:
        return {"trend": "unknown", "last_bos": None, "last_choch": None, "swing_highs": [], "swing_lows": []}
    swings = find_swing_highs_lows(candles)
    highs = swings["highs"]
    lows = swings["lows"]
    trend = "unknown"
    last_bos = None
    last_choch = None
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1]["price"] > highs[-2]["price"] and lows[-1]["price"] > lows[-2]["price"]:
            trend = "bullish"
        elif highs[-1]["price"] < highs[-2]["price"] and lows[-1]["price"] < lows[-2]["price"]:
            trend = "bearish"
        else:
            trend = "ranging"
        last_close = candles[-1]["c"]
        if trend == "bullish" and last_close > highs[-2]["price"]:
            last_bos = {"type": "bullish_bos", "level": highs[-2]["price"]}
        elif trend == "bearish" and last_close < lows[-2]["price"]:
            last_bos = {"type": "bearish_bos", "level": lows[-2]["price"]}
        if trend == "bullish" and last_close < lows[-1]["price"]:
            last_choch = {"type": "bearish_choch", "level": lows[-1]["price"]}
        elif trend == "bearish" and last_close > highs[-1]["price"]:
            last_choch = {"type": "bullish_choch", "level": highs[-1]["price"]}
    return {"trend": trend, "last_bos": last_bos, "last_choch": last_choch,
            "swing_highs": highs, "swing_lows": lows}


def find_order_blocks(candles, structure):
    obs = []
    if len(candles) < 5:
        return obs
    for i in range(2, len(candles) - 2):
        c = candles[i]
        next_c = candles[i + 1]
        next2_c = candles[i + 2]
        body_size = abs(c["c"] - c["o"])
        if body_size == 0:
            continue
        if c["c"] < c["o"]:
            move_up = (next2_c["h"] - c["l"]) / body_size if body_size > 0 else 0
            if next_c["c"] > c["h"] and move_up > 1.5:
                obs.append({"type": "bullish_ob", "top": c["o"], "bottom": c["l"],
                            "index": i, "strength": min(move_up / 2, 3.0)})
        elif c["c"] > c["o"]:
            move_down = (c["h"] - next2_c["l"]) / body_size if body_size > 0 else 0
            if next_c["c"] < c["l"] and move_down > 1.5:
                obs.append({"type": "bearish_ob", "top": c["h"], "bottom": c["o"],
                            "index": i, "strength": min(move_down / 2, 3.0)})
    bullish_obs = [o for o in obs if o["type"] == "bullish_ob"][-2:]
    bearish_obs = [o for o in obs if o["type"] == "bearish_ob"][-2:]
    return bullish_obs + bearish_obs


def find_fvg(candles):
    fvgs = []
    if len(candles) < 3:
        return fvgs
    for i in range(1, len(candles) - 1):
        prev = candles[i - 1]
        nxt = candles[i + 1]
        if nxt["l"] > prev["h"]:
            fvgs.append({"type": "bullish_fvg", "top": nxt["l"], "bottom": prev["h"],
                         "mid": (nxt["l"] + prev["h"]) / 2, "filled": False, "inverted": False})
        elif nxt["h"] < prev["l"]:
            fvgs.append({"type": "bearish_fvg", "top": prev["l"], "bottom": nxt["h"],
                         "mid": (prev["l"] + nxt["h"]) / 2, "filled": False, "inverted": False})

    last_low = candles[-1]["l"] if candles else 0
    last_high = candles[-1]["h"] if candles else 0
    last_close = candles[-1]["c"] if candles else 0

    for fvg in fvgs:
        if fvg["type"] == "bullish_fvg":
            if last_close < fvg["bottom"]:
                # Full fill — invert to bearish IFVG
                fvg["filled"] = True
                fvg["inverted"] = True
                fvg["type"] = "bearish_ifvg"
            fvg["partial"] = last_low < fvg["top"] and last_close > fvg["bottom"]
        elif fvg["type"] == "bearish_fvg":
            if last_close > fvg["top"]:
                # Full fill — invert to bullish IFVG
                fvg["filled"] = True
                fvg["inverted"] = True
                fvg["type"] = "bullish_ifvg"
            fvg["partial"] = last_high > fvg["bottom"] and last_close < fvg["top"]

    # Keep unfilled FVGs + inverted FVGs (IFVG) — remove only plain filled
    active = [f for f in fvgs if not f["filled"] or f["inverted"]]
    return active[-6:]


def find_bpr(candles):
    """Find Balanced Price Range — overlap between bullish and bearish FVG."""
    fvgs = find_fvg(candles)
    bullish = [f for f in fvgs if f["type"] == "bullish_fvg"]
    bearish = [f for f in fvgs if f["type"] == "bearish_fvg"]
    bprs = []
    for b in bullish:
        for bear in bearish:
            # Check overlap
            overlap_top = min(b["top"], bear["top"])
            overlap_bottom = max(b["bottom"], bear["bottom"])
            if overlap_top > overlap_bottom:
                mid = (overlap_top + overlap_bottom) / 2
                bprs.append({
                    "type": "bpr",
                    "top": round(overlap_top, 5),
                    "bottom": round(overlap_bottom, 5),
                    "mid": round(mid, 5),
                    "size": round(overlap_top - overlap_bottom, 5),
                })
    return bprs[-3:]


def find_liquidity_levels(candles):
    if len(candles) < 10:
        return {"buy_side": [], "sell_side": []}
    swings = find_swing_highs_lows(candles, lookback=4)
    buy_side = [{"price": l["price"]} for l in swings["lows"][-3:]]
    sell_side = [{"price": h["price"]} for h in swings["highs"][-3:]]
    return {"buy_side": buy_side, "sell_side": sell_side}


def detect_liquidity_sweep(candles, liquidity):
    if len(candles) < 3:
        return None
    last = candles[-1]
    for lvl in liquidity["buy_side"]:
        if last["l"] < lvl["price"] and last["c"] > lvl["price"]:
            return {"type": "sweep_buy_side", "level": lvl["price"], "direction": "bullish",
                    "desc": "Sweep BSL " + "{:.5f}".format(lvl["price"])}
    for lvl in liquidity["sell_side"]:
        if last["h"] > lvl["price"] and last["c"] < lvl["price"]:
            return {"type": "sweep_sell_side", "level": lvl["price"], "direction": "bearish",
                    "desc": "Sweep SSL " + "{:.5f}".format(lvl["price"])}
    return None


def get_premium_discount(candles):
    if len(candles) < 20:
        return {"zone": "unknown", "equilibrium": None, "percent": 50}
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



def find_eqh_eql(candles, instrument, lookback=50):
    """Find Equal Highs and Equal Lows (liquidity pools)."""
    if len(candles) < 10:
        return {"eqh": [], "eql": []}

    # Tolerance: how close prices must be to be "equal"
    if "BTC" in instrument:
        tolerance = 50      # $50 for BTC
    elif "XAU" in instrument:
        tolerance = 0.5     # $0.50 for Gold
    elif "JPY" in instrument:
        tolerance = 0.05
    else:
        tolerance = 0.00050  # 5 pips for forex

    recent = candles[-lookback:]
    eqh = []
    eql = []

    highs = [c["h"] for c in recent]
    lows = [c["l"] for c in recent]

    # Find equal highs
    for i in range(len(highs)):
        matches = []
        for j in range(i + 1, len(highs)):
            if abs(highs[i] - highs[j]) <= tolerance:
                matches.append(j)
        if matches:
            level = round(sum([highs[i]] + [highs[m] for m in matches]) / (len(matches) + 1), 5)
            # Avoid duplicates
            if not any(abs(e["price"] - level) <= tolerance for e in eqh):
                eqh.append({
                    "price": level,
                    "count": len(matches) + 1,
                    "strength": min(len(matches) + 1, 5)
                })

    # Find equal lows
    for i in range(len(lows)):
        matches = []
        for j in range(i + 1, len(lows)):
            if abs(lows[i] - lows[j]) <= tolerance:
                matches.append(j)
        if matches:
            level = round(sum([lows[i]] + [lows[m] for m in matches]) / (len(matches) + 1), 5)
            if not any(abs(e["price"] - level) <= tolerance for e in eql):
                eql.append({
                    "price": level,
                    "count": len(matches) + 1,
                    "strength": min(len(matches) + 1, 5)
                })

    # Sort by strength and return top 3
    eqh = sorted(eqh, key=lambda x: x["strength"], reverse=True)[:3]
    eql = sorted(eql, key=lambda x: x["strength"], reverse=True)[:3]

    return {"eqh": eqh, "eql": eql}

def calculate_fibonacci(candles):
    if len(candles) < 20:
        return {}
    recent = candles[-50:] if len(candles) >= 50 else candles
    swing_high = max(c["h"] for c in recent)
    swing_low = min(c["l"] for c in recent)
    diff = swing_high - swing_low
    if diff == 0:
        return {}
    return {
        "swing_high": round(swing_high, 5),
        "swing_low":  round(swing_low, 5),
        "fib_0_5":    round(swing_high - diff * 0.500, 5),
        "fib_0_618":  round(swing_high - diff * 0.618, 5),
        "fib_0_705":  round(swing_high - diff * 0.705, 5),
        "fib_0_79":   round(swing_high - diff * 0.790, 5),
    }


def get_key_levels(candles_by_tf):
    levels = {}
    d_candles = candles_by_tf.get("D", [])
    if len(d_candles) >= 3:
        pd = d_candles[-2]
        levels["PDH"] = pd["h"]
        levels["PDL"] = pd["l"]
    w_candles = candles_by_tf.get("W", [])
    if len(w_candles) >= 2:
        pw = w_candles[-2]
        levels["PWH"] = pw["h"]
        levels["PWL"] = pw["l"]
    m_candles = candles_by_tf.get("M", [])
    if len(m_candles) >= 2:
        pm = m_candles[-2]
        levels["PMH"] = pm["h"]
        levels["PML"] = pm["l"]
    return levels


def analyze_smc(candles_by_tf, instrument="EUR_USD"):
    result = {}
    for tf in ["M", "W", "D", "H4", "H1", "M15", "M5"]:
        c = candles_by_tf.get(tf, [])
        if c:
            result["structure_" + tf] = detect_market_structure(c)

            # Filter OB by minimum size
            raw_obs = find_order_blocks(c, result["structure_" + tf])
            min_ob = min_ob_size(instrument, tf)
            pv = pip_value(instrument)
            if "BTC" in instrument or "XAU" in instrument:
                result["ob_" + tf] = [o for o in raw_obs if (o["top"] - o["bottom"]) >= min_ob]
            else:
                result["ob_" + tf] = [o for o in raw_obs if (o["top"] - o["bottom"]) / pv >= min_ob]

            # Filter FVG by minimum size
            raw_fvgs = find_fvg(c)
            min_fvg = min_fvg_size(instrument, tf)
            if "BTC" in instrument or "XAU" in instrument:
                result["fvg_" + tf] = [f for f in raw_fvgs if (f["top"] - f["bottom"]) >= min_fvg]
            else:
                result["fvg_" + tf] = [f for f in raw_fvgs if (f["top"] - f["bottom"]) / pv >= min_fvg]

            result["bpr_" + tf] = find_bpr(c)
            result["liquidity_" + tf] = find_liquidity_levels(c)
            result["sweep_" + tf] = detect_liquidity_sweep(c, result["liquidity_" + tf])
            result["pd_zone_" + tf] = get_premium_discount(c)
            result["current_price"] = c[-1]["c"]
    result["key_levels"] = get_key_levels(candles_by_tf)
    score = 0
    if result.get("structure_H1", {}).get("trend") in ("bullish", "bearish"):
        score += 1
    if result.get("fvg_M15"):
        score += 1
    if result.get("ob_H1"):
        score += 1
    if result.get("sweep_M15"):
        score += 1
    zone = result.get("pd_zone_H1", {}).get("zone", "")
    trend = result.get("structure_H1", {}).get("trend", "")
    if (trend == "bullish" and zone == "discount") or (trend == "bearish" and zone == "premium"):
        score += 1
    result["setup_quality"] = score
    result["has_setup"] = score >= 3
    # Will be boosted after setups calculated if confluence found
    result["fibonacci"] = calculate_fibonacci(candles_by_tf.get("H1", []))

    # EQH/EQL on 1H and 4H
    h1_candles = candles_by_tf.get("H1", [])
    h4_candles = candles_by_tf.get("H4", [])
    # instrument not available here, use generic tolerance via key
    result["eqh_eql_H1"] = find_eqh_eql(h1_candles, instrument)
    result["eqh_eql_H4"] = find_eqh_eql(h4_candles, instrument)

    # 5M trigger (only for forex and gold)
    bias_1h = result.get("structure_H1", {}).get("trend", "unknown")
    candles_5m = candles_by_tf.get("M5", [])
    result["trigger_5m"] = detect_5m_trigger(candles_5m, bias_1h)

    # Boost score if 5M confirms
    if result["trigger_5m"].get("confirmed"):
        result["setup_quality"] = min(result["setup_quality"] + 1, 5)
        result["has_setup"] = result["setup_quality"] >= 3

    # Structure confirmation — continuation vs reversal
    candles_h1 = candles_by_tf.get("H1", [])
    candles_m15 = candles_by_tf.get("M15", [])
    candles_h4 = candles_by_tf.get("H4", [])
    struct_confirm = analyze_structure_confirmation(candles_h1, candles_m15, candles_h4, result)
    result["struct_confirm"] = struct_confirm

    # Boost score for high confidence confirmation
    if struct_confirm["confidence"] >= 4:
        result["setup_quality"] = min(result["setup_quality"] + 1, 5)
        result["has_setup"] = result["setup_quality"] >= 3

    return result


# ── CALCULATIONS ──────────────────────────────────────────────────────────────

def pip_value(instrument):
    if "JPY" in instrument:
        return 0.01
    if "XAU" in instrument:
        return 0.01
    if "BTC" in instrument:
        return 1.0
    return 0.0001


def sl_buffer(instrument):
    if "BTC" in instrument:
        return 150
    if "XAU" in instrument:
        return 1.5
    if "JPY" in instrument:
        return 0.15
    return 0.00100



def min_fvg_size(instrument, tf):
    """Minimum FVG size to be considered valid."""
    sizes = {
        "BTC": {"M5": 80, "M15": 80, "H1": 150, "H4": 350, "D": 700, "W": 1500, "M": 1500},
        "XAU": {"M5": 0.50, "M15": 0.50, "H1": 0.80, "H4": 2.00, "D": 4.00, "W": 8.00, "M": 8.00},
    }
    forex = {"M5": 2, "M15": 2, "H1": 3, "H4": 6, "D": 12, "W": 25, "M": 25}
    if "BTC" in instrument:
        return sizes["BTC"].get(tf, 80)
    if "XAU" in instrument:
        return sizes["XAU"].get(tf, 0.50)
    return forex.get(tf, 2)


def min_ob_size(instrument, tf):
    """Minimum OB size to be considered valid."""
    sizes = {
        "BTC": {"M5": 100, "M15": 100, "H1": 200, "H4": 500, "D": 1000, "W": 2500, "M": 2500},
        "XAU": {"M5": 0.80, "M15": 0.80, "H1": 1.20, "H4": 3.00, "D": 6.00, "W": 12.00, "M": 12.00},
    }
    forex = {"M5": 3, "M15": 3, "H1": 4, "H4": 8, "D": 16, "W": 32, "M": 32}
    if "BTC" in instrument:
        return sizes["BTC"].get(tf, 100)
    if "XAU" in instrument:
        return sizes["XAU"].get(tf, 0.80)
    return forex.get(tf, 3)

def pips(price_diff, instrument):
    pv = pip_value(instrument)
    return round(abs(price_diff) / pv)


def calc_rr(entry, sl, tp):
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    if risk == 0:
        return 0
    return round(reward / risk, 1)


def check_body_close(candle, level, direction):
    """True if candle BODY closed beyond level (not just wick)."""
    body_top = max(candle["o"], candle["c"])
    body_bottom = min(candle["o"], candle["c"])
    if direction == "below":
        return body_top < level
    else:
        return body_bottom > level


def analyze_structure_confirmation(candles_h1, candles_m15, candles_h4, smc_data):
    """
    Determines continuation vs reversal based on body close vs wick sweep.
    CONTINUATION: body close beyond swing high/low + FVG + unswept pools
    REVERSAL: wick sweep only + body held + pools in opposite direction
    """
    result = {
        "scenario": "neutral", "direction": None,
        "entry_type": None, "correction_zone": None,
        "has_unswept_pools": False, "confidence": 0,
    }
    if not candles_h1 or len(candles_h1) < 5:
        return result

    last = candles_h1[-1]
    prev = candles_h1[-2]
    price = smc_data.get("current_price", 0)
    structure_h1 = smc_data.get("structure_H1", {})
    swing_highs = structure_h1.get("swing_highs", [])
    swing_lows = structure_h1.get("swing_lows", [])
    if not swing_lows or not swing_highs:
        return result

    prev_low = swing_lows[-1]["price"]
    prev_high = swing_highs[-1]["price"]
    fvg_h1 = smc_data.get("fvg_H1", [])
    bpr_h1 = smc_data.get("bpr_H1", [])
    liq_h1 = smc_data.get("liquidity_H1", {})
    liq_h4 = smc_data.get("liquidity_H4", {})
    pools_below = [l for l in liq_h1.get("buy_side", []) if l["price"] < price]
    pools_below += [l for l in liq_h4.get("buy_side", []) if l["price"] < price]
    pools_above = [l for l in liq_h1.get("sell_side", []) if l["price"] > price]
    pools_above += [l for l in liq_h4.get("sell_side", []) if l["price"] > price]

    # ── BEARISH CONTINUATION ──
    body_broke_low = check_body_close(last, prev_low, "below") or check_body_close(prev, prev_low, "below")
    zones_above = [f for f in fvg_h1 if f["type"] in ("bearish_fvg", "bearish_ifvg", "bullish_ifvg") and f["top"] > price]
    zones_above += [b for b in bpr_h1 if b["mid"] > price]
    if body_broke_low and zones_above and pools_below:
        cz_list = []
        for f in [z for z in zones_above if "fvg" in z.get("type","")]:
            cz_list.append((f["type"], f["bottom"], f["top"], f["mid"]))
        for b in [z for z in zones_above if "mid" in z]:
            cz_list.append(("bpr", b["bottom"], b["top"], b["mid"]))
        cz_list.sort(key=lambda x: x[1])
        result.update({"scenario": "continuation", "direction": "sell", "entry_type": "correction",
                       "correction_zone": cz_list[0] if cz_list else None,
                       "has_unswept_pools": True, "confidence": 4 if len(pools_below) >= 2 else 3})
        return result

    # ── BEARISH REVERSAL → potential BUY ──
    if last["l"] < prev_low and check_body_close(last, prev_low, "above") and pools_above:
        result.update({"scenario": "reversal", "direction": "buy", "entry_type": "choch",
                       "has_unswept_pools": True, "confidence": 4})
        return result

    # ── BULLISH CONTINUATION ──
    body_broke_high = check_body_close(last, prev_high, "above") or check_body_close(prev, prev_high, "above")
    zones_below = [f for f in fvg_h1 if f["type"] in ("bullish_fvg", "bullish_ifvg", "bearish_ifvg") and f["bottom"] < price]
    zones_below += [b for b in bpr_h1 if b["mid"] < price]
    if body_broke_high and zones_below and pools_above:
        cz_list = []
        for f in [z for z in zones_below if "fvg" in z.get("type","")]:
            cz_list.append((f["type"], f["bottom"], f["top"], f["mid"]))
        for b in [z for z in zones_below if "mid" in z]:
            cz_list.append(("bpr", b["bottom"], b["top"], b["mid"]))
        cz_list.sort(key=lambda x: x[2], reverse=True)
        result.update({"scenario": "continuation", "direction": "buy", "entry_type": "correction",
                       "correction_zone": cz_list[0] if cz_list else None,
                       "has_unswept_pools": True, "confidence": 4 if len(pools_above) >= 2 else 3})
        return result

    # ── BULLISH REVERSAL → potential SELL ──
    if last["h"] > prev_high and check_body_close(last, prev_high, "below") and pools_below:
        result.update({"scenario": "reversal", "direction": "sell", "entry_type": "choch",
                       "has_unswept_pools": True, "confidence": 4})
        return result

    return result


def find_entry_zone(ob_list, fvg_list, bpr_list, price, direction):
    """
    Confluence-based entry zone hierarchy:
    1. OB + FVG confluence ★★★★★
    2. OB + IFVG confluence ★★★★☆
    3. BPR ★★★★☆
    4. OB alone ★★★☆☆
    5. FVG alone ★★☆☆☆
    6. IFVG alone ★★☆☆☆
    """
    if direction == "buy":
        obs = [o for o in ob_list if o["type"] == "bullish_ob" and o["bottom"] < price]
        fvgs = [f for f in fvg_list if f["type"] == "bullish_fvg" and f["bottom"] < price]
        ifvgs = [f for f in fvg_list if f["type"] == "bullish_ifvg" and f["bottom"] < price]
        bprs = [b for b in bpr_list if b["mid"] < price]
    else:
        obs = [o for o in ob_list if o["type"] == "bearish_ob" and o["top"] > price]
        fvgs = [f for f in fvg_list if f["type"] == "bearish_fvg" and f["top"] > price]
        ifvgs = [f for f in fvg_list if f["type"] == "bearish_ifvg" and f["top"] > price]
        bprs = [b for b in bpr_list if b["mid"] > price]

    best = None

    # 1. OB + FVG confluence
    for ob in obs:
        for fvg in fvgs:
            ot = min(ob["top"], fvg["top"])
            ob_ = max(ob["bottom"], fvg["bottom"])
            if ot > ob_:
                mid = (ot + ob_) / 2
                best = (round(mid, 5), round(ob_, 5), round(ot, 5), "OB+FVG confluence", 5)
                break
        if best:
            break

    # 2. OB + IFVG confluence
    if not best:
        for ob in obs:
            for ifvg in ifvgs:
                ot = min(ob["top"], ifvg["top"])
                ob_ = max(ob["bottom"], ifvg["bottom"])
                if ot > ob_:
                    mid = (ot + ob_) / 2
                    best = (round(mid, 5), round(ob_, 5), round(ot, 5), "OB+IFVG confluence", 4)
                    break
            if best:
                break

    # 3. BPR
    if not best and bprs:
        b = bprs[-1] if direction == "buy" else bprs[0]
        best = (round(b["mid"], 5), round(b["bottom"], 5), round(b["top"], 5), "BPR", 4)

    # 4. OB alone
    if not best and obs:
        ob = obs[-1] if direction == "buy" else obs[0]
        entry = round(ob["top"], 5) if direction == "buy" else round(ob["bottom"], 5)
        best = (entry, round(ob["bottom"], 5), round(ob["top"], 5), "Order Block", 3)

    # 5. FVG alone
    if not best and fvgs:
        fvg = fvgs[-1] if direction == "buy" else fvgs[0]
        entry = round(fvg["top"], 5) if direction == "buy" else round(fvg["bottom"], 5)
        best = (entry, round(fvg["bottom"], 5), round(fvg["top"], 5), "FVG", 2)

    # 6. IFVG alone
    if not best and ifvgs:
        ifvg = ifvgs[-1] if direction == "buy" else ifvgs[0]
        entry = round(ifvg["top"], 5) if direction == "buy" else round(ifvg["bottom"], 5)
        best = (entry, round(ifvg["bottom"], 5), round(ifvg["top"], 5), "IFVG", 2)

    return best


def calculate_setups(instrument, smc_data):
    price = smc_data.get("current_price", 0)
    key = smc_data.get("key_levels", {})
    ob_h1 = smc_data.get("ob_H1", [])
    fvg_h1 = smc_data.get("fvg_H1", [])
    structure_h1 = smc_data.get("structure_H1", {})
    structure_m15 = smc_data.get("structure_M15", {})
    swing_highs_h1 = structure_h1.get("swing_highs", [])
    swing_lows_h1 = structure_h1.get("swing_lows", [])
    swing_highs_m15 = structure_m15.get("swing_highs", [])
    swing_lows_m15 = structure_m15.get("swing_lows", [])
    bpr_h1 = smc_data.get("bpr_H1", [])
    buf = sl_buffer(instrument)
    setups = {}

    struct_confirm = smc_data.get("struct_confirm", {})
    sc_scenario = struct_confirm.get("scenario", "neutral")
    sc_direction = struct_confirm.get("direction")

    # ── BUY SETUP ──
    buy_zone = find_entry_zone(ob_h1, fvg_h1, bpr_h1, price, "buy")

    # Continuation BUY — override with correction zone below price
    if sc_scenario == "continuation" and sc_direction == "buy":
        cz = struct_confirm.get("correction_zone")
        if cz:
            buy_zone = (round(cz[3], 5), round(cz[1], 5), round(cz[2], 5), "Correction " + cz[0].upper(), 5)

    # Skip buy if bearish continuation confirmed
    if sc_scenario == "continuation" and sc_direction == "sell":
        buy_zone = None

    buy_entry = None
    buy_sl = None
    buy_label = ""
    buy_strength = 0

    if buy_zone:
        buy_entry, zone_bottom, zone_top, buy_label, buy_strength = buy_zone
        lows_below = [l["price"] for l in swing_lows_m15 if l["price"] < buy_entry]
        if lows_below:
            buy_sl = round(min(lows_below) - buf, 5)
        else:
            lows_h1_below = [l["price"] for l in swing_lows_h1 if l["price"] < buy_entry]
            if lows_h1_below:
                buy_sl = round(min(lows_h1_below) - buf, 5)
            else:
                buy_sl = round(zone_bottom - buf, 5)

    # ── LIQUIDITY TARGETS ──
    # Collect all potential TP targets sorted by distance from entry
    liq_h1 = smc_data.get("liquidity_H1", {})
    liq_h4 = smc_data.get("liquidity_H4", {})
    fvg_h4 = smc_data.get("fvg_H4", [])
    eqh_eql_h1 = smc_data.get("eqh_eql_H1", {})
    eqh_eql_h4 = smc_data.get("eqh_eql_H4", {})

    if buy_entry and buy_sl:
        risk = abs(buy_entry - buy_sl)
        if risk <= 0:
            risk = buf * 3
            buy_sl = round(buy_entry - risk, 5)

        # Collect BUY targets above entry (SSL = sell-side liquidity above = targets for buy)
        buy_targets = []

        # 1. SSL on 1H above entry (stops of sellers = magnet for buys)
        for lvl in liq_h1.get("sell_side", []):
            if lvl["price"] > buy_entry:
                buy_targets.append(("SSL 1H", round(lvl["price"], 5)))

        # 2. SSL on 4H above entry
        for lvl in liq_h4.get("sell_side", []):
            if lvl["price"] > buy_entry:
                buy_targets.append(("SSL 4H", round(lvl["price"], 5)))

        # 3. FVG on 4H above entry (imbalance as magnet)
        for fvg in fvg_h4:
            if fvg["type"] == "bearish_fvg" and fvg["bottom"] > buy_entry:
                buy_targets.append(("FVG 4H", round(fvg["bottom"], 5)))

        # 4. EQH on 1H above entry (equal highs = liquidity pool)
        for eq in eqh_eql_h1.get("eqh", []):
            if eq["price"] > buy_entry:
                buy_targets.append(("EQH 1H", round(eq["price"], 5)))

        # 5. EQH on 4H above entry
        for eq in eqh_eql_h4.get("eqh", []):
            if eq["price"] > buy_entry:
                buy_targets.append(("EQH 4H", round(eq["price"], 5)))

        # 6. Key levels above entry
        for label in ["PDH", "PWH", "PMH"]:
            val = key.get(label)
            if val and val > buy_entry:
                buy_targets.append((label, round(val, 5)))

        # Sort by distance (closest first)
        buy_targets.sort(key=lambda x: x[1])

        # Filter: must give at least RR 1:1.5
        min_tp = buy_entry + risk * 1.5
        valid_targets = [(l, p) for l, p in buy_targets if p >= min_tp]

        if len(valid_targets) >= 2:
            buy_tp1_label, buy_tp1 = valid_targets[0]
            buy_tp2_label, buy_tp2 = valid_targets[1]
        elif len(valid_targets) == 1:
            buy_tp1_label, buy_tp1 = valid_targets[0]
            buy_tp2_label = "RR 1:3"
            buy_tp2 = round(buy_entry + risk * 3, 5)
        else:
            buy_tp1_label = "RR 1:2"
            buy_tp1 = round(buy_entry + risk * 2, 5)
            buy_tp2_label = "RR 1:3"
            buy_tp2 = round(buy_entry + risk * 3, 5)

        setups["buy"] = {
            "entry": buy_entry, "sl": buy_sl,
            "tp1": buy_tp1, "tp2": buy_tp2,
            "tp1_label": buy_tp1_label, "tp2_label": buy_tp2_label,
            "zone_label": buy_label, "zone_strength": buy_strength,
            "sl_pips": pips(buy_entry - buy_sl, instrument),
            "tp1_pips": pips(buy_tp1 - buy_entry, instrument),
            "tp2_pips": pips(buy_tp2 - buy_entry, instrument),
            "rr1": calc_rr(buy_entry, buy_sl, buy_tp1),
            "rr2": calc_rr(buy_entry, buy_sl, buy_tp2),
        }

    # ── SELL SETUP ──
    sell_zone = find_entry_zone(ob_h1, fvg_h1, bpr_h1, price, "sell")

    # Override: if continuation SELL — enter from correction zone above price
    if sc_scenario == "continuation" and sc_direction == "sell":
        cz = struct_confirm.get("correction_zone")
        if cz:
            sell_zone = (round(cz[3], 5), round(cz[1], 5), round(cz[2], 5), "Correction " + cz[0].upper(), 5)

    # Skip sell if continuation is in buy direction
    if sc_scenario == "continuation" and sc_direction == "buy":
        sell_zone = None

    sell_entry = None
    sell_sl = None

    if sell_zone:
        sell_entry, zone_bottom, zone_top, sell_label, sell_strength = sell_zone
        highs_above = [h["price"] for h in swing_highs_m15 if h["price"] > sell_entry]
        if highs_above:
            sell_sl = round(max(highs_above) + buf, 5)
        else:
            highs_h1_above = [h["price"] for h in swing_highs_h1 if h["price"] > sell_entry]
            if highs_h1_above:
                sell_sl = round(max(highs_h1_above) + buf, 5)
            else:
                sell_sl = round(zone_top + buf, 5)

    if sell_entry and sell_sl:
        risk = abs(sell_entry - sell_sl)
        if risk <= 0:
            risk = buf * 3
            sell_sl = round(sell_entry + risk, 5)

        # Collect SELL targets below entry (BSL = buy-side liquidity below = targets for sells)
        sell_targets = []

        # 1. BSL on 1H below entry
        for lvl in liq_h1.get("buy_side", []):
            if lvl["price"] < sell_entry:
                sell_targets.append(("BSL 1H", round(lvl["price"], 5)))

        # 2. BSL on 4H below entry
        for lvl in liq_h4.get("buy_side", []):
            if lvl["price"] < sell_entry:
                sell_targets.append(("BSL 4H", round(lvl["price"], 5)))

        # 3. Bullish FVG / Bullish IFVG on 4H below entry
        for fvg in fvg_h4:
            if fvg["type"] == "bullish_fvg" and fvg["top"] < sell_entry:
                sell_targets.append(("FVG 4H", round(fvg["top"], 5)))
            elif fvg["type"] == "bullish_ifvg" and fvg["top"] < sell_entry:
                sell_targets.append(("IFVG 4H", round(fvg["mid"], 5)))

        # BPR on 1H below entry
        for bpr in smc_data.get("bpr_H1", []):
            if bpr["mid"] < sell_entry:
                sell_targets.append(("BPR 1H", round(bpr["mid"], 5)))

        # 4. EQL on 1H below entry (equal lows = liquidity pool)
        for eq in eqh_eql_h1.get("eql", []):
            if eq["price"] < sell_entry:
                sell_targets.append(("EQL 1H", round(eq["price"], 5)))

        # 5. EQL on 4H below entry
        for eq in eqh_eql_h4.get("eql", []):
            if eq["price"] < sell_entry:
                sell_targets.append(("EQL 4H", round(eq["price"], 5)))

        # 6. Key levels below entry
        for label in ["PDL", "PWL", "PML"]:
            val = key.get(label)
            if val and val < sell_entry:
                sell_targets.append((label, round(val, 5)))

        # Sort by distance (closest first = highest price below entry)
        sell_targets.sort(key=lambda x: x[1], reverse=True)

        # Filter: must give at least RR 1:1.5
        min_tp = sell_entry - risk * 1.5
        valid_targets = [(l, p) for l, p in sell_targets if p <= min_tp]

        if len(valid_targets) >= 2:
            sell_tp1_label, sell_tp1 = valid_targets[0]
            sell_tp2_label, sell_tp2 = valid_targets[1]
        elif len(valid_targets) == 1:
            sell_tp1_label, sell_tp1 = valid_targets[0]
            sell_tp2_label = "RR 1:3"
            sell_tp2 = round(sell_entry - risk * 3, 5)
        else:
            sell_tp1_label = "RR 1:2"
            sell_tp1 = round(sell_entry - risk * 2, 5)
            sell_tp2_label = "RR 1:3"
            sell_tp2 = round(sell_entry - risk * 3, 5)

        setups["sell"] = {
            "entry": sell_entry, "sl": sell_sl,
            "tp1": sell_tp1, "tp2": sell_tp2,
            "tp1_label": sell_tp1_label, "tp2_label": sell_tp2_label,
            "zone_label": sell_label, "zone_strength": sell_strength,
            "sl_pips": pips(sell_entry - sell_sl, instrument),
            "tp1_pips": pips(sell_entry - sell_tp1, instrument),
            "tp2_pips": pips(sell_entry - sell_tp2, instrument),
            "rr1": calc_rr(sell_entry, sell_sl, sell_tp1),
            "rr2": calc_rr(sell_entry, sell_sl, sell_tp2),
        }

    return setups


def format_distance(pips_val, instrument):
    """Show pips for forex, dollars for XAU/BTC."""
    if "XAU" in instrument:
        return "$" + "{:.2f}".format(pips_val * 0.01)
    if "BTC" in instrument:
        return "$" + "{:.0f}".format(pips_val)
    return str(pips_val) + " pips"


def format_setup(setup, direction, instrument=""):
    if not setup:
        return "зона не визначена"
    arrow = "BUY" if direction == "buy" else "SELL"
    sl_dist = format_distance(setup["sl_pips"], instrument)
    tp1_dist = format_distance(setup["tp1_pips"], instrument)
    tp2_dist = format_distance(setup["tp2_pips"], instrument)
    tp1_label = setup.get("tp1_label", "")
    tp2_label = setup.get("tp2_label", "")
    zone_label = setup.get("zone_label", "")
    zone_strength = setup.get("zone_strength", 0)
    stars = "★" * zone_strength + "☆" * (5 - zone_strength)
    tp1_tag = " [" + tp1_label + "]" if tp1_label else ""
    tp2_tag = " [" + tp2_label + "]" if tp2_label else ""
    zone_tag = " | Zone: " + zone_label + " " + stars if zone_label else ""
    return (
        arrow + " Entry: " + "{:.5f}".format(setup["entry"]) + zone_tag +
        " | SL: " + "{:.5f}".format(setup["sl"]) + " (" + sl_dist + ")" +
        " | TP1: " + "{:.5f}".format(setup["tp1"]) + tp1_tag + " (" + tp1_dist + ", RR 1:" + str(setup["rr1"]) + ")" +
        " | TP2: " + "{:.5f}".format(setup["tp2"]) + tp2_tag + " (" + tp2_dist + ", RR 1:" + str(setup["rr2"]) + ")"
    )


# ── AI ────────────────────────────────────────────────────────────────────────

async def get_ai_analysis(instrument, smc_data, session_info, alert_mode=False):
    price = smc_data.get("current_price", 0)
    key = smc_data.get("key_levels", {})

    def trend_arrow(t):
        return {"bullish": "UP Bullish", "bearish": "DOWN Bearish", "ranging": "Ranging"}.get(t, "Unknown")

    def ob_str(obs):
        if not obs:
            return "  не знайдено"
        lines = []
        for o in obs[-2:]:
            d = "Bullish" if "bullish" in o["type"] else "Bearish"
            lines.append("  " + d + " OB: " + "{:.5f}".format(o["bottom"]) + " - " + "{:.5f}".format(o["top"]))
        return "\n".join(lines)

    def fvg_str(fvgs):
        if not fvgs:
            return "  не знайдено"
        lines = []
        for f in fvgs[-3:]:
            t = f["type"]
            if "ifvg" in t:
                d = "🔄 Bullish IFVG" if "bullish" in t else "🔄 Bearish IFVG"
                status = ""
            else:
                d = "Bullish FVG" if "bullish" in t else "Bearish FVG"
                status = " [PARTIAL]" if f.get("partial") else " [FRESH]"
            lines.append("  " + d + status + ": " + "{:.5f}".format(f["bottom"]) + " - " + "{:.5f}".format(f["top"]))
        return "\n".join(lines)

    def liq_str(liq):
        bsl = liq.get("buy_side", [])
        ssl = liq.get("sell_side", [])
        parts = []
        if bsl:
            bsl_prices = ", ".join("{:.5f}".format(l["price"]) for l in bsl[-2:])
            parts.append("  BSL: " + bsl_prices)
        if ssl:
            ssl_prices = ", ".join("{:.5f}".format(l["price"]) for l in ssl[-2:])
            parts.append("  SSL: " + ssl_prices)
        return "\n".join(parts) if parts else "  не визначено"

    def sweep_str(sweep):
        if not sweep:
            return "немає"
        return "CONFIRMED: " + sweep["desc"] + " -> " + sweep["direction"]

    def pd_str(pd):
        zone = pd.get("zone", "unknown")
        pct = pd.get("percent", 0)
        eq = pd.get("equilibrium", 0)
        labels = {"premium": "Premium", "discount": "Discount", "equilibrium": "Equilibrium"}
        return labels.get(zone, zone) + " (" + str(pct) + "%, EQ: " + "{:.5f}".format(eq if eq else 0) + ")"

    setups = calculate_setups(instrument, smc_data)
    fib = smc_data.get("fibonacci", {})

    prompt = (
        "Ти досвідчений SMC трейдер. Проведи повний топ-даун аналіз.\n\n"
        "ІНСТРУМЕНТ: " + instrument.replace("_", "/") + "\n"
        "ЦІНА: " + "{:.5f}".format(price) + "\n"
        "СЕСІЯ: " + session_info["name"] + "\n\n"
        "=== СТРУКТУРА ===\n"
        "Monthly: " + trend_arrow(smc_data.get("structure_M", {}).get("trend", "unknown")) + "\n"
        "Weekly: " + trend_arrow(smc_data.get("structure_W", {}).get("trend", "unknown")) + "\n"
        "Daily: " + trend_arrow(smc_data.get("structure_D", {}).get("trend", "unknown")) + "\n"
        "4H: " + trend_arrow(smc_data.get("structure_H4", {}).get("trend", "unknown")) + "\n"
        "1H: " + trend_arrow(smc_data.get("structure_H1", {}).get("trend", "unknown")) + "\n"
        "15M: " + trend_arrow(smc_data.get("structure_M15", {}).get("trend", "unknown")) + "\n\n"
        "=== КЛЮЧОВІ РІВНІ ===\n"
        "PMH/PML: " + str(key.get("PMH", "N/A")) + " / " + str(key.get("PML", "N/A")) + "\n"
        "PWH/PWL: " + str(key.get("PWH", "N/A")) + " / " + str(key.get("PWL", "N/A")) + "\n"
        "PDH/PDL: " + str(key.get("PDH", "N/A")) + " / " + str(key.get("PDL", "N/A")) + "\n\n"
        "=== ORDER BLOCKS ===\n"
        "4H:\n" + ob_str(smc_data.get("ob_H4", [])) + "\n"
        "1H:\n" + ob_str(smc_data.get("ob_H1", [])) + "\n"
        "15M:\n" + ob_str(smc_data.get("ob_M15", [])) + "\n\n"
        "=== FVG / IFVG ===\n"
        "4H:\n" + fvg_str(smc_data.get("fvg_H4", [])) + "\n"
        "1H:\n" + fvg_str(smc_data.get("fvg_H1", [])) + "\n"
        "15M:\n" + fvg_str(smc_data.get("fvg_M15", [])) + "\n\n"
        "=== BPR (Balanced Price Range) ===\n"
        + ("\n".join("  BPR: {:.5f} - {:.5f} | mid: {:.5f}".format(b["bottom"], b["top"], b["mid"]) for b in smc_data.get("bpr_H1", [])) or "  не знайдено") + "\n\n"
        "=== ЛІКВІДНІСТЬ ===\n"
        "1H:\n" + liq_str(smc_data.get("liquidity_H1", {})) + "\n"
        "15M:\n" + liq_str(smc_data.get("liquidity_M15", {})) + "\n\n"
        "=== EQH / EQL (рівні хаї/лої) ===\n"
        "1H EQH: " + ", ".join("{:.5f}({}x)".format(e["price"], e["count"]) for e in smc_data.get("eqh_eql_H1", {}).get("eqh", [])) + "\n"
        "1H EQL: " + ", ".join("{:.5f}({}x)".format(e["price"], e["count"]) for e in smc_data.get("eqh_eql_H1", {}).get("eql", [])) + "\n"
        "4H EQH: " + ", ".join("{:.5f}({}x)".format(e["price"], e["count"]) for e in smc_data.get("eqh_eql_H4", {}).get("eqh", [])) + "\n"
        "4H EQL: " + ", ".join("{:.5f}({}x)".format(e["price"], e["count"]) for e in smc_data.get("eqh_eql_H4", {}).get("eql", [])) + "\n\n"
        "=== СВІПИ ===\n"
        "4H: " + sweep_str(smc_data.get("sweep_H4")) + "\n"
        "1H: " + sweep_str(smc_data.get("sweep_H1")) + "\n"
        "15M: " + sweep_str(smc_data.get("sweep_M15")) + "\n\n"
        "=== FIBONACCI (1H) ===\n"
        "High: " + str(fib.get("swing_high", "N/A")) + " | Low: " + str(fib.get("swing_low", "N/A")) + "\n"
        "0.5:   " + str(fib.get("fib_0_5", "N/A")) + "\n"
        "0.618: " + str(fib.get("fib_0_618", "N/A")) + "\n"
        "0.705: " + str(fib.get("fib_0_705", "N/A")) + "\n"
        "0.79:  " + str(fib.get("fib_0_79", "N/A")) + "\n\n"
        "=== PREMIUM/DISCOUNT ===\n"
        "4H: " + pd_str(smc_data.get("pd_zone_H4", {})) + "\n"
        "1H: " + pd_str(smc_data.get("pd_zone_H1", {})) + "\n\n"
        "=== 5M ТРИГЕР ===\n"
        + smc_data.get("trigger_5m", {}).get("desc", "немає даних") + "\n\n"
        "SCORE: " + str(smc_data.get("setup_quality", 0)) + "/5\n\n"
        "ПРАВИЛА: RR мін 1:2, ціль 1:3+, ризик 0.5-1%, prop challenge +8%\n\n"
        "=== РОЗРАХОВАНІ РІВНІ (використовуй ТІЛЬКИ ЦІ цифри!) ===\n"
        "BUY: " + format_setup(setups.get("buy"), "buy", instrument) + "\n"
        "SELL: " + format_setup(setups.get("sell"), "sell") + "\n\n"
        "ВАЖЛИВО: НЕ придумуй свої рівні! Використовуй тільки цифри вище.\n\n"
        "Дай відповідь УКРАЇНСЬКОЮ, формат Telegram Markdown:\n"
        "1. BIAS\n2. СИТУАЦІЯ\n"
        "3. BUY сценарій (використай розраховані рівні вище)\n"
        "4. SELL сценарій (використай розраховані рівні вище)\n"
        "5. ТРИГЕР на 15M\n6. ВИСНОВОК"
    )

    if alert_mode:
        prompt += "\n\nКОРОТКО: починай з ALERT, тільки головне — напрям/вхід/SL/TP."

    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1500,
        "messages": [{"role": "user", "content": prompt}],
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(ANTHROPIC_API, headers=headers, json=body,
                                timeout=aiohttp.ClientTimeout(total=45)) as resp:
            if resp.status != 200:
                err = await resp.text()
                raise Exception("Anthropic error " + str(resp.status) + ": " + err[:200])
            data = await resp.json()
            text = "".join(b.get("text", "") for b in data.get("content", []))

    emoji_map = {"EUR_USD": "🇪🇺", "GBP_USD": "🇬🇧", "XAU_USD": "🥇", "BTC_USD": "₿"}
    emoji = emoji_map.get(instrument, "📊")
    display = instrument.replace("_", "/")
    score = smc_data.get("setup_quality", 0)
    stars = "★" * score + "☆" * (5 - score)
    header = (
        emoji + " *" + display + "* | `" + "{:.5f}".format(price) + "`\n"
        + session_info["name"] + " " + session_info["emoji"] + " | " + stars + "\n"
        + "─" * 30 + "\n\n"
    )
    return header + text


# ── TELEGRAM HANDLERS ─────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *SMC Trading Bot*\n\n"
        "Аналізую ринок за Smart Money концепцією:\n"
        "BOS/CHoCH · OB · FVG · Liquidity Sweeps · Fibonacci\n\n"
        "*Команди:*\n"
        "`/xauusd` — Аналіз Gold\n"
        "`/eurusd` — Аналіз EUR/USD\n"
        "`/gbpusd` — Аналіз GBP/USD\n"
        "`/btcusd` — Аналіз Bitcoin\n\n"
        "`/alerts on` — автоматичні алерти\n"
        "`/alerts off` — вимкнути\n"
        "`/status` — статус бота\n\n"
        "_Killzones: Лондон 08-12 UTC · Нью-Йорк 13-17 UTC_"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Вкажи інструмент: `/analyze XAUUSD`", parse_mode=ParseMode.MARKDOWN)
        return
    raw = args[0].upper().replace("/", "_")
    aliases = {
        "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD",
        "XAUUSD": "XAU_USD", "BTCUSD": "BTC_USD",
        "GOLD": "XAU_USD", "BTC": "BTC_USD",
        "EUR_USD": "EUR_USD", "GBP_USD": "GBP_USD",
        "XAU_USD": "XAU_USD", "BTC_USD": "BTC_USD",
    }
    instrument = aliases.get(raw)
    if not instrument:
        await update.message.reply_text("Невідомий інструмент. Доступні: EURUSD, GBPUSD, XAUUSD, BTCUSD")
        return
    emoji_map = {"EUR_USD": "🇪🇺", "GBP_USD": "🇬🇧", "XAU_USD": "🥇", "BTC_USD": "₿"}
    emoji = emoji_map.get(instrument, "📊")
    display = instrument.replace("_", "/")
    msg = await update.message.reply_text(
        emoji + " *" + display + "* — завантажую дані...", parse_mode=ParseMode.MARKDOWN
    )
    try:
        await msg.edit_text(emoji + " *" + display + "* — збираю свічки (6 TF)...", parse_mode=ParseMode.MARKDOWN)
        candles = await fetch_candles(instrument, TWELVEDATA_API_KEY)
        await msg.edit_text(emoji + " *" + display + "* — аналізую SMC структуру...", parse_mode=ParseMode.MARKDOWN)
        smc_data = analyze_smc(candles, instrument)
        session_info = get_session_info()
        await msg.edit_text(emoji + " *" + display + "* — AI генерує аналіз...", parse_mode=ParseMode.MARKDOWN)
        analysis = await get_ai_analysis(instrument, smc_data, session_info)
        await msg.edit_text(analysis, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception as e:
        logger.error("Analysis error: " + str(e), exc_info=True)
        await msg.edit_text("Помилка аналізу " + display + ":\n`" + str(e)[:200] + "`", parse_mode=ParseMode.MARKDOWN)


async def cmd_xauusd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.args = ["XAUUSD"]
    await cmd_analyze(update, context)

async def cmd_eurusd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.args = ["EURUSD"]
    await cmd_analyze(update, context)

async def cmd_gbpusd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.args = ["GBPUSD"]
    await cmd_analyze(update, context)

async def cmd_btcusd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.args = ["BTCUSD"]
    await cmd_analyze(update, context)


async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    if not args or args[0].lower() not in ("on", "off"):
        await update.message.reply_text("Usage: `/alerts on` або `/alerts off`", parse_mode=ParseMode.MARKDOWN)
        return
    if args[0].lower() == "on":
        ALERT_USERS.add(chat_id)
        await update.message.reply_text(
            "✅ *Алерти увімкнено!*\nБот перевіряє ринок кожні 15 хвилин.\nПК можна вимикати ☁️",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        ALERT_USERS.discard(chat_id)
        await update.message.reply_text("🔕 Алерти вимкнено.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    alert_status = "✅ Увімкнені" if chat_id in ALERT_USERS else "🔕 Вимкнені"
    session = get_session_info()
    now = datetime.now(timezone.utc)
    kz = "✅ Активна" if is_killzone() else "⏸ Не активна"
    text = (
        "🤖 *SMC Bot*\n\n"
        "UTC: `" + now.strftime("%H:%M %d.%m.%Y") + "`\n"
        "Сесія: *" + session["name"] + "* " + session["emoji"] + "\n"
        "Killzone: " + kz + "\n"
        "Алерти: " + alert_status + "\n\n"
        "Інструменти: EUR/USD · GBP/USD · XAU/USD · BTC/USD"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ── ALERT LOOP ────────────────────────────────────────────────────────────────

async def alert_loop(app):
    await asyncio.sleep(60)
    while True:
        try:
            if ALERT_USERS:
                if is_asian_session():
                    logger.info("Asian session — skipping alerts")
                    await asyncio.sleep(15 * 60)
                    continue
                if not is_killzone():
                    logger.info("Not in killzone — skipping alerts")
                    await asyncio.sleep(15 * 60)
                    continue
                for instrument in INSTRUMENTS:
                    try:
                        candles = await fetch_candles(instrument, TWELVEDATA_API_KEY)
                        smc_data = analyze_smc(candles, instrument)
                        if smc_data.get("has_setup") and smc_data.get("setup_quality", 0) >= 3:
                            current_price = smc_data.get("current_price", 0)
                            pv = pip_value(instrument)
                            last_price = SENT_ALERTS.get(instrument, 0)
                            price_change_pips = abs(current_price - last_price) / pv if pv > 0 else 999
                            if price_change_pips < MIN_PRICE_CHANGE_PIPS:
                                logger.info("Skipping " + instrument + " — price unchanged (" + str(round(price_change_pips)) + " pips)")
                                await asyncio.sleep(3)
                                continue
                            session_info = get_session_info()
                            analysis = await get_ai_analysis(instrument, smc_data, session_info, alert_mode=True)
                            sent_ok = False
                            for chat_id in list(ALERT_USERS):
                                try:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=analysis,
                                        parse_mode=ParseMode.MARKDOWN,
                                        disable_web_page_preview=True
                                    )
                                    sent_ok = True
                                except Exception as e:
                                    logger.error("Alert send error: " + str(e))
                            if sent_ok:
                                SENT_ALERTS[instrument] = current_price
                                logger.info("Alert sent for " + instrument + " at " + str(current_price))
                        await asyncio.sleep(3)
                    except Exception as e:
                        logger.error("Alert scan error " + instrument + ": " + str(e))
        except Exception as e:
            logger.error("Alert loop error: " + str(e))
        await asyncio.sleep(15 * 60)


async def post_init(app):
    asyncio.create_task(alert_loop(app))


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("analyze", cmd_analyze))
    app.add_handler(CommandHandler("xauusd", cmd_xauusd))
    app.add_handler(CommandHandler("eurusd", cmd_eurusd))
    app.add_handler(CommandHandler("gbpusd", cmd_gbpusd))
    app.add_handler(CommandHandler("btcusd", cmd_btcusd))
    app.add_handler(CommandHandler("alerts", cmd_alerts))
    app.add_handler(CommandHandler("status", cmd_status))
    logger.info("SMC Trading Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
