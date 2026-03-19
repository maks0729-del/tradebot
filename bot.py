import os
import time
import asyncio
import logging
import aiohttp
import json
from datetime import datetime, timezone
import pytz
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
MIN_PRICE_CHANGE_PIPS = 20  # default for forex

# Active signal tracking — {instrument: {entry, sl, tp1, tp2, direction, time, label, notified_entry, notified_tp1}}
ACTIVE_SIGNALS = {}
SIGNAL_TTL = 86400  # 24 hours

def min_price_change(instrument):
    """Minimum price change before sending repeat alert."""
    if "BTC" in instrument:
        return 500   # $500 for BTC
    if "XAU" in instrument:
        return 150   # 150 * 0.01 = $1.50 for Gold
    if "GBP" in instrument:
        return 10    # 10 pips for GBP
    return 8         # 8 pips for EUR



# Cache for higher timeframes to save API requests
TF_CACHE = {}       # {instrument: {tf: candles}}
TF_CACHE_TIME = {}  # {instrument: {tf: last_update_timestamp}}

# Cache TTL per timeframe (seconds)
TF_CACHE_TTL = {
    "M":   86400,   # 24 hours
    "W":   86400,   # 24 hours
    "D":   14400,   # 4 hours
    "H4":  3600,    # 1 hour
    "H1":  900,     # 15 min
    "M15": 300,     # 5 min
    "M5":  300,     # 5 min
}

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

DUBLIN_TZ = pytz.timezone("Europe/Dublin")
NY_TZ = pytz.timezone("America/New_York")


def get_dublin_time():
    return datetime.now(DUBLIN_TZ)


def is_london_open():
    t = get_dublin_time()
    return t.hour == 8


def is_ny_open():
    ny_time = datetime.now(NY_TZ)
    return 9 <= ny_time.hour < 12


def is_5min_period():
    return is_london_open() or is_ny_open()


def is_killzone():
    t = get_dublin_time()
    return (8 <= t.hour < 12) or is_ny_open()


def is_asian_session():
    t = get_dublin_time()
    return 0 <= t.hour < 7


def is_weekend():
    """Saturday and Sunday — forex/gold markets closed."""
    t = get_dublin_time()
    return t.weekday() >= 5  # 5=Saturday, 6=Sunday


def get_active_instruments():
    """Returns active instruments based on day of week."""
    if is_weekend():
        return ["BTC_USD"]  # only crypto on weekends
    return INSTRUMENTS      # all instruments on weekdays


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


async def fetch_candles_cached(instrument, api_key):
    """
    Smart fetch with caching.
    TTL per TF:
    - M, W  → 24h
    - D, H4 → 1h
    - H1    → 15min
    - M15   → 5min
    - M5    → 5min
    Sleep 8s between requests = max 7.5/min, never exceeds TwelveData 8/min limit.
    """
    import time

    if instrument not in TF_CACHE:
        TF_CACHE[instrument] = {}
        TF_CACHE_TIME[instrument] = {}

    symbol = SYMBOL_MAP.get(instrument, instrument)  # TwelveData needs EUR/USD with slash

    tfs_to_fetch = []
    for tf in TIMEFRAMES:
        if tf == "M5" and instrument in TIMEFRAMES_NO_5M:
            continue
        last_update = TF_CACHE_TIME[instrument].get(tf, 0)
        ttl = TF_CACHE_TTL.get(tf, 300)
        # Use fresh time() for each check
        if (time.time() - last_update) > ttl or tf not in TF_CACHE[instrument]:
            tfs_to_fetch.append(tf)

    if tfs_to_fetch:
        logger.info(f"[CACHE] {instrument} fetching {tfs_to_fetch} ({len(tfs_to_fetch)} requests)")
        async with aiohttp.ClientSession() as session:
            for tf in tfs_to_fetch:
                cfg = TIMEFRAMES[tf]
                try:
                    candles = await fetch_twelvedata(session, api_key, symbol, cfg["interval"], cfg["count"])
                    if candles:
                        TF_CACHE[instrument][tf] = candles
                        TF_CACHE_TIME[instrument][tf] = time.time()  # fresh time after each request
                except Exception as e:
                    logger.warning(f"Cache fetch error {instrument} {tf}: {e}")
                await asyncio.sleep(8)  # 8s between requests — OUTSIDE try/except, always executes
    else:
        logger.info(f"[CACHE] {instrument} — all cached, 0 requests")

    return {tf: TF_CACHE[instrument].get(tf, []) for tf in TIMEFRAMES if tf in TF_CACHE[instrument]}


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
            await asyncio.sleep(8)  # 8s between requests — always executes
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


def detect_5m_trigger(candles_5m, bias, candles_15m=None, instrument="EUR_USD"):
    """
    Forex/XAU: small BOS on 5M + confirmation on 15M
    BTC: BOS on 15M only
    Returns trigger info with aggression score
    """
    result = {"confirmed": False, "type": None, "direction": None, "aggression": 0}

    is_btc = "BTC" in instrument

    # For BTC use 15M as trigger TF
    trigger_candles = candles_15m if is_btc else candles_5m
    confirm_candles = candles_15m if not is_btc else None

    if not trigger_candles or len(trigger_candles) < 10:
        return result

    recent = trigger_candles[-15:]
    highs = [c["h"] for c in recent]
    lows = [c["l"] for c in recent]
    closes = [c["c"] for c in recent]

    last = recent[-1]
    prev_high = max(highs[:-3]) if len(highs) > 3 else highs[0]
    prev_low = min(lows[:-3]) if len(lows) > 3 else lows[0]

    # Count FVGs for aggression score
    fvg_count = count_fvg_on_5m(trigger_candles[-20:]) if not is_btc else 0

    # BOS bullish: close above recent swing high
    if bias in ("bullish", "buy") and last["c"] > prev_high:
        result["confirmed"] = True
        result["type"] = "BOS"
        result["direction"] = "buy"
        result["aggression"] = min(fvg_count, 3)

        # For forex: check 15M confirmation
        if not is_btc and confirm_candles and len(confirm_candles) >= 5:
            recent_15m = confirm_candles[-5:]
            highs_15m = [c["h"] for c in recent_15m]
            if recent_15m[-1]["c"] > max(highs_15m[:-1]):
                result["type"] = "BOS+15M"
                result["aggression"] = min(result["aggression"] + 1, 3)

    # BOS bearish: close below recent swing low
    elif bias in ("bearish", "sell") and last["c"] < prev_low:
        result["confirmed"] = True
        result["type"] = "BOS"
        result["direction"] = "sell"
        result["aggression"] = min(fvg_count, 3)

        if not is_btc and confirm_candles and len(confirm_candles) >= 5:
            recent_15m = confirm_candles[-5:]
            lows_15m = [c["l"] for c in recent_15m]
            if recent_15m[-1]["c"] < min(lows_15m[:-1]):
                result["type"] = "BOS+15M"
                result["aggression"] = min(result["aggression"] + 1, 3)

    # CHoCH — opposite direction BOS
    elif bias in ("bullish", "buy") and last["c"] < prev_low:
        result["confirmed"] = True
        result["type"] = "CHoCH"
        result["direction"] = "sell"
        result["aggression"] = min(fvg_count, 3)

    elif bias in ("bearish", "sell") and last["c"] > prev_high:
        result["confirmed"] = True
        result["type"] = "CHoCH"
        result["direction"] = "buy"
        result["aggression"] = min(fvg_count, 3)

    return result


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
    """
    Valid OB requires:
    1. Liquidity sweep before the move (wick beyond prev swing high/low)
    2. Aggressive impulse FROM the OB (move >= 2x body size)
    3. FVG formed during the impulse (gap between candles = aggression proof)
    4. BOS after the impulse (closes beyond structure)
    """
    obs = []
    if len(candles) < 6:
        return obs

    swing_highs = [h["price"] for h in structure.get("swing_highs", [])]
    swing_lows = [l["price"] for l in structure.get("swing_lows", [])]

    for i in range(2, len(candles) - 3):
        c = candles[i]           # potential OB candle
        prev = candles[i - 1]    # candle before OB
        next1 = candles[i + 1]   # first impulse candle
        next2 = candles[i + 2]   # second impulse candle
        next3 = candles[i + 3] if i + 3 < len(candles) else next2

        body_size = abs(c["c"] - c["o"])
        if body_size == 0:
            continue

        # ── BULLISH OB ──
        # OB candle must be bearish (last down candle before up move)
        if c["c"] < c["o"]:
            # 1. Liquidity sweep: wick below recent swing low before OB
            recent_lows = [l for l in swing_lows if l < c["l"]]
            sweep_happened = c["l"] < prev["l"] or (recent_lows and c["l"] <= min(recent_lows) * 1.001)

            # 2. Aggressive impulse: next candle closes above OB high
            bos_confirmed = next1["c"] > c["h"]

            # 3. Impulse strength: move at least 2x body size
            impulse_size = next2["h"] - c["l"]
            strong_impulse = (impulse_size / body_size) >= 2.0 if body_size > 0 else False

            # 4. FVG formed during impulse (gap between OB candle and next2)
            fvg_formed = next2["l"] > c["h"]  # gap = imbalance = aggression proof

            if sweep_happened and bos_confirmed and strong_impulse and fvg_formed:
                strength = min(impulse_size / body_size / 2, 3.0)
                # Check if HH confirmed by BODY (not just wick)
                body_confirmed_hh = next1["c"] > max(c["h"], prev["h"])
                obs.append({
                    "type": "bullish_ob",
                    "top": c["o"],
                    "bottom": c["l"],
                    "index": i,
                    "strength": round(strength, 2),
                    "has_fvg": True,
                    "swept_liquidity": True,
                    "body_confirmed": body_confirmed_hh,
                })

        # ── BEARISH OB ──
        # OB candle must be bullish (last up candle before down move)
        elif c["c"] > c["o"]:
            # 1. Liquidity sweep: wick above recent swing high before OB
            recent_highs = [h for h in swing_highs if h > c["h"]]
            sweep_happened = c["h"] > prev["h"] or (recent_highs and c["h"] >= max(recent_highs) * 0.999)

            # 2. Aggressive impulse: next candle closes below OB low
            bos_confirmed = next1["c"] < c["l"]

            # 3. Impulse strength: move at least 2x body size
            impulse_size = c["h"] - next2["l"]
            strong_impulse = (impulse_size / body_size) >= 2.0 if body_size > 0 else False

            # 4. FVG formed during impulse (gap between OB candle and next2)
            fvg_formed = next2["h"] < c["l"]  # gap below OB = imbalance

            if sweep_happened and bos_confirmed and strong_impulse and fvg_formed:
                strength = min(impulse_size / body_size / 2, 3.0)
                # Check if LL confirmed by BODY (not just wick)
                body_confirmed_ll = next1["c"] < min(c["l"], prev["l"])
                obs.append({
                    "type": "bearish_ob",
                    "top": c["h"],
                    "bottom": c["o"],
                    "index": i,
                    "strength": round(strength, 2),
                    "has_fvg": True,
                    "swept_liquidity": True,
                    "body_confirmed": body_confirmed_ll,
                })

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
    # Store raw candles for aggression detection in calculate_setups
    result["candles_h1"] = candles_by_tf.get("H1", [])
    result["candles_h4"] = candles_by_tf.get("H4", [])
    result["candles_5m"] = candles_by_tf.get("M5", [])
    result["candles_d"]  = candles_by_tf.get("D", [])

    # ── ORIGIN LIQUIDITY ── (BSL/SSL that originated the current move)
    result["origin_liquidity"] = find_origin_liquidity(
        candles_by_tf.get("H1", []),
        candles_by_tf.get("H4", []),
        candles_by_tf.get("D", []),
        result
    )

    # ── SIGNAL CLASSIFICATION ── reversal vs continuation
    result["signal"] = classify_signal(result, instrument)
    # ── EQH/EQL ──
    h1_candles = candles_by_tf.get("H1", [])
    h4_candles = candles_by_tf.get("H4", [])
    result["eqh_eql_H1"] = find_eqh_eql(h1_candles, instrument)
    result["eqh_eql_H4"] = find_eqh_eql(h4_candles, instrument)
    # Fibonacci — H1 for forex/XAU, H4 for BTC
    if "BTC" in instrument:
        result["fibonacci"] = calculate_fibonacci(candles_by_tf.get("H4", []))
        result["fibonacci_h1"] = calculate_fibonacci(h1_candles)
    else:
        result["fibonacci"] = calculate_fibonacci(h1_candles)
        result["fibonacci_h1"] = result["fibonacci"]

    # ── 5M/15M TRIGGER ──
    bias_1h = result.get("structure_H1", {}).get("trend", "unknown")
    candles_5m = candles_by_tf.get("M5", [])
    candles_15m = candles_by_tf.get("M15", [])
    result["trigger_5m"] = detect_5m_trigger(candles_5m, bias_1h, candles_15m, instrument)

    # ── STRUCTURE CONFIRMATION ──
    candles_h1 = candles_by_tf.get("H1", [])
    candles_m15 = candles_by_tf.get("M15", [])
    candles_h4 = candles_by_tf.get("H4", [])
    struct_confirm = analyze_structure_confirmation(candles_h1, candles_m15, candles_h4, result)
    result["struct_confirm"] = struct_confirm

    # ── SCORE SYSTEM ──
    # Base: market structure
    score = 0
    trend = result.get("structure_H1", {}).get("trend", "")
    trend_h4 = result.get("structure_H4", {}).get("trend", "")

    # 1. H1 структура визначена
    if trend in ("bullish", "bearish"):
        score += 1

    # 2. H4 підтверджує H1 напрямок
    if trend and trend == trend_h4:
        score += 1

    # 3. OB або FVG на H1 як зона входу
    if result.get("ob_H1") or result.get("fvg_H1"):
        score += 1

    # 4. Ліквідність знята (sweep) — ціна почала рух
    sweep_h1 = result.get("sweep_H1") or {}
    sweep_m15 = result.get("sweep_M15") or {}
    sweep_h4 = result.get("sweep_H4") or {}
    sweep_d = result.get("sweep_D") or {}
    if sweep_h1.get("swept") or sweep_m15.get("swept") or sweep_h4.get("swept"):
        score += 1

    # 5b. Origin liquidity знайдена — підтверджує напрямок руху
    origin = result.get("origin_liquidity") or {}
    if origin.get("origin"):
        score += 1

    # 5. Premium/Discount відповідає напрямку
    zone = (result.get("pd_zone_H1") or {}).get("zone", "")
    if (trend == "bullish" and zone == "discount") or (trend == "bearish" and zone == "premium"):
        score += 1

    result["setup_quality"] = min(score, 5)
    result["has_setup"] = score >= 3
    result["origin_score"] = score  # save before boosts

    # ── SCORE БУСТЕРИ (максимум +2) ──
    boosts = 0

    # Буст 1: 5M/15M тригер підтверджує bias
    trigger_5m = result.get("trigger_5m") or {}
    if trigger_5m.get("confirmed") and trigger_5m.get("direction") == trend:
        boosts += 1
        # Додатковий буст якщо BOS+15M (повне підтвердження)
        if trigger_5m.get("type") == "BOS+15M":
            boosts += 1

    # Буст 2: Агресія після sweep (2+ FVG на 5M або wick sweep на H1/H4)
    aggression = detect_aggressive_reversal(
        candles_h1, candles_h4, candles_5m, result,
        "buy" if trend == "bullish" else "sell"
    )
    if aggression["aggressive"] and aggression["confidence"] >= 2:
        boosts += 1

    # Буст 3: Continuation з корекцією до FVG/IFVG/BPR
    if (result.get("struct_confirm") or {}).get("scenario") == "continuation" and (result.get("struct_confirm") or {}).get("confidence", 0) >= 4:
        boosts += 1

    # Буст 4: Reversal сигнал підтверджено (wick sweep + агресія + BOS)
    signal = result.get("signal") or {}
    if signal.get("type") == "reversal" and signal.get("confidence", 0) >= 3:
        boosts += 1

    # Буст 6: OTE зона (FVG або BPR в золотій Fibo зоні)
    fib_data = result.get("fibonacci") or {}
    price_now = result.get("current_price", 0)
    if fib_data and price_now:
        f705 = fib_data.get("fib_0_705", 0)
        f79  = fib_data.get("fib_0_79", 0)
        f618 = fib_data.get("fib_0_618", 0)
        f50  = fib_data.get("fib_0_5", 0)
        trend = (result.get("structure_H1") or {}).get("trend", "")
        if trend == "bullish" and f79 and f50:
            if f79 <= price_now <= f50:
                boosts += 1  # Price in OTE buy zone
        elif trend == "bearish" and f50 and f79:
            if f50 <= price_now <= f79:
                boosts += 1  # Price in OTE sell zone

    # Буст 5: Reversal на старшому TF (H4 або D) — сильніший сигнал
    if signal.get("type") == "reversal" and signal.get("sl_tf") in ("H4", "D"):
        boosts += 1

    # Застосовуємо бустери (максимум 5/5)
    result["setup_quality"] = min(result["setup_quality"] + boosts, 5)
    result["has_setup"] = result["setup_quality"] >= 3
    result["aggression"] = aggression

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


def find_best_sl(instrument, direction, entry_price, smc_data, buf):
    """
    SL always behind swept liquidity level.
    Priority:
    1. Swept liquidity level from sweep_H1 / sweep_H4 / sweep_M15
    2. Liquidity pool (BSL/SSL) from liquidity_H1 / H4 / M15
    3. Nearest swing from structure (last resort)
    """
    # ── 1. Swept liquidity levels (most important) ──
    swept_candidates = []
    for tf in ["H1", "H4", "M15"]:
        sweep = smc_data.get("sweep_" + tf) or {}
        if not sweep or not sweep.get("type"):
            continue
        swept_level = sweep.get("level", 0)
        if not swept_level:
            continue
        # For buy: swept level should be below entry (swept BSL = support)
        if direction == "buy" and swept_level < entry_price:
            swept_candidates.append(round(swept_level - buf, 5))
        # For sell: swept level should be above entry (swept SSL = resistance)
        elif direction == "sell" and swept_level > entry_price:
            swept_candidates.append(round(swept_level + buf, 5))

    if swept_candidates:
        # Return closest to entry (best RR)
        if direction == "buy":
            return max(swept_candidates)   # highest = closest to entry
        else:
            return min(swept_candidates)   # lowest = closest to entry

    # ── 2. Liquidity pools between entry and current price ──
    liq_candidates = []
    for tf in ["M15", "H1", "H4"]:
        liq = smc_data.get("liquidity_" + tf) or {}
        if direction == "buy":
            for lvl in liq.get("buy_side", []):
                p = lvl["price"]
                if p < entry_price:
                    liq_candidates.append(round(p - buf, 5))
        else:
            for lvl in liq.get("sell_side", []):
                p = lvl["price"]
                if p > entry_price:
                    liq_candidates.append(round(p + buf, 5))

    if liq_candidates:
        if direction == "buy":
            return max(liq_candidates)
        else:
            return min(liq_candidates)

    # ── 3. Nearest swing (last resort) ──
    structure_m15 = smc_data.get("structure_M15") or {}
    structure_h1  = smc_data.get("structure_H1") or {}
    structure_h4  = smc_data.get("structure_H4") or {}

    if direction == "buy":
        for struct in [structure_m15, structure_h1, structure_h4]:
            lows = sorted(
                [l["price"] for l in struct.get("swing_lows", []) if l["price"] < entry_price],
                reverse=True
            )
            if lows:
                return round(lows[0] - buf, 5)
    else:
        for struct in [structure_m15, structure_h1, structure_h4]:
            highs = sorted(
                [h["price"] for h in struct.get("swing_highs", []) if h["price"] > entry_price]
            )
            if highs:
                return round(highs[0] + buf, 5)

    return None

    return None



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



def count_fvg_on_5m(candles_5m):
    """Count FVGs on 5M — multiple FVGs = aggressive move."""
    if not candles_5m or len(candles_5m) < 10:
        return 0
    recent = candles_5m[-20:]
    count = 0
    for i in range(1, len(recent) - 1):
        prev = recent[i - 1]
        nxt = recent[i + 1]
        # Bullish FVG
        if nxt["l"] > prev["h"]:
            count += 1
        # Bearish FVG
        if nxt["h"] < prev["l"]:
            count += 1
    return count


def detect_aggressive_reversal(candles_h1, candles_h4, candles_5m, smc_data, direction):
    """
    Detect aggressive reversal after liquidity sweep.
    Signs of aggression:
    1. Sweep by wick on 1H or 4H (body didn't close beyond pool)
    2. Multiple FVGs on 5M during reversal move (3+ = aggressive)
    Returns: {"aggressive": bool, "confidence": 0-3, "reason": str}
    """
    result = {"aggressive": False, "confidence": 0, "reason": ""}
    score = 0
    reasons = []

    # Check 1: wick sweep on 1H (body held)
    if candles_h1 and len(candles_h1) >= 2:
        last = candles_h1[-1]
        body_top = max(last["o"], last["c"])
        body_bot = min(last["o"], last["c"])
        structure = smc_data.get("structure_H1") or {}
        swing_highs = structure.get("swing_highs", [])
        swing_lows = structure.get("swing_lows", [])

        if direction == "sell" and swing_highs:
            prev_high = swing_highs[-1]["price"]
            wick_swept = last["h"] > prev_high
            body_held = body_top < prev_high
            if wick_swept and body_held:
                score += 1
                reasons.append("wick sweep 1H (тіло утримало)")

        if direction == "buy" and swing_lows:
            prev_low = swing_lows[-1]["price"]
            wick_swept = last["l"] < prev_low
            body_held = body_bot > prev_low
            if wick_swept and body_held:
                score += 1
                reasons.append("wick sweep 1H (тіло утримало)")

    # Check 2: wick sweep on 4H
    if candles_h4 and len(candles_h4) >= 2:
        last4 = candles_h4[-1]
        body_top4 = max(last4["o"], last4["c"])
        body_bot4 = min(last4["o"], last4["c"])
        structure4 = smc_data.get("structure_H4") or {}
        highs4 = structure4.get("swing_highs", [])
        lows4 = structure4.get("swing_lows", [])

        if direction == "sell" and highs4:
            prev_high4 = highs4[-1]["price"]
            if last4["h"] > prev_high4 and body_top4 < prev_high4:
                score += 1
                reasons.append("wick sweep 4H (тіло утримало)")

        if direction == "buy" and lows4:
            prev_low4 = lows4[-1]["price"]
            if last4["l"] < prev_low4 and body_bot4 > prev_low4:
                score += 1
                reasons.append("wick sweep 4H (тіло утримало)")

    # Check 3: multiple FVGs on 5M = aggressive move
    fvg_count = count_fvg_on_5m(candles_5m)
    if fvg_count >= 2:
        score += 1
        reasons.append(f"агресія на 5M ({fvg_count} FVG)")

    result["aggressive"] = score >= 1
    result["confidence"] = score
    result["reason"] = " + ".join(reasons) if reasons else "немає агресії"
    return result

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


def is_in_ote(price_level, fib, direction):
    """Check if price level is in OTE zone (0.5 - 0.786 Fibonacci)."""
    if not fib:
        return False, 0
    f50  = fib.get("fib_0_5")
    f618 = fib.get("fib_0_618")
    f705 = fib.get("fib_0_705")
    f79  = fib.get("fib_0_79")
    if not all([f50, f618, f705, f79]):
        return False, 0

    if direction == "buy":
        # For buy: OTE is between swing_low corrected to 0.5-0.786
        ote_high = f50
        ote_low  = f79
        in_ote = ote_low <= price_level <= ote_high
        # Score bonus based on zone quality
        if f705 <= price_level <= f79:
            return in_ote, 2   # Golden OTE zone
        elif f618 <= price_level <= f705:
            return in_ote, 1   # Good OTE zone
        elif f50 <= price_level <= f618:
            return in_ote, 0   # Basic 0.5 zone
    else:
        # For sell: OTE is between swing_high corrected to 0.5-0.786
        ote_low  = f50
        ote_high = f79
        in_ote = ote_low <= price_level <= ote_high
        if f705 <= price_level <= f79:
            return in_ote, 2
        elif f618 <= price_level <= f705:
            return in_ote, 1
        elif f50 <= price_level <= f618:
            return in_ote, 0
    return False, 0


def find_entry_zone(ob_list, fvg_list, bpr_list, price, direction, fib=None):
    """
    OTE-aware entry zone hierarchy:

    ★★★★★  FVG в Golden OTE (0.705-0.786)
    ★★★★★  BPR в OTE зоні
    ★★★★☆  FVG в OTE (0.618-0.705)
    ★★★★☆  Валідний OB (body_confirmed) в OTE
    ★★★★☆  FVG в 0.5 зоні
    ★★★☆☆  BPR поза OTE
    ★★★☆☆  Валідний OB в OTE (без body confirm)
    ★★★☆☆  OB + FVG confluence
    ★★☆☆☆  FVG поза Fibo
    ★★☆☆☆  Слабкий OB
    """
    if direction == "buy":
        obs   = [o for o in ob_list if o["type"] == "bullish_ob"  and o["bottom"] < price]
        fvgs  = [f for f in fvg_list if f["type"] == "bullish_fvg" and f["bottom"] < price]
        ifvgs = [f for f in fvg_list if f["type"] == "bullish_ifvg" and f["bottom"] < price]
        bprs  = [b for b in bpr_list if b["mid"] < price]
    else:
        obs   = [o for o in ob_list if o["type"] == "bearish_ob"  and o["top"] > price]
        fvgs  = [f for f in fvg_list if f["type"] == "bearish_fvg" and f["top"] > price]
        ifvgs = [f for f in fvg_list if f["type"] == "bearish_ifvg" and f["top"] > price]
        bprs  = [b for b in bpr_list if b["mid"] > price]

    scored = []  # list of (score, entry, bottom, top, label)

    # ── Score FVGs ──
    for fvg in fvgs + ifvgs:
        mid = fvg.get("mid", (fvg["top"] + fvg["bottom"]) / 2)
        in_ote, ote_bonus = is_in_ote(mid, fib, direction)
        is_ifvg = "ifvg" in fvg["type"]
        base = 3 if not is_ifvg else 2
        score = base + ote_bonus + (1 if in_ote else 0)
        if direction == "buy":
            entry = round(fvg["top"], 5)
        else:
            entry = round(fvg["bottom"], 5)
        label = ("FVG" if not is_ifvg else "IFVG")
        if in_ote:
            label += f" OTE({0.705 if ote_bonus==2 else 0.618 if ote_bonus==1 else 0.5})"
        scored.append((score, entry, round(fvg["bottom"],5), round(fvg["top"],5), label))

    # ── Score BPRs ──
    for bpr in bprs:
        mid = bpr["mid"]
        in_ote, ote_bonus = is_in_ote(mid, fib, direction)
        score = 4 + ote_bonus + (1 if in_ote else 0)
        label = "BPR" + (" OTE" if in_ote else "")
        scored.append((score, round(mid,5), round(bpr["bottom"],5), round(bpr["top"],5), label))

    # ── Score OBs ──
    for ob in obs:
        mid = (ob["top"] + ob["bottom"]) / 2
        in_ote, ote_bonus = is_in_ote(mid, fib, direction)
        body_ok = ob.get("body_confirmed", False)
        swept   = ob.get("swept_liquidity", False)
        has_fvg = ob.get("has_fvg", False)

        # Base score
        if swept and has_fvg and body_ok:
            base = 4   # fully valid OB
        elif swept and has_fvg:
            base = 3   # valid OB (wick confirmed)
        elif swept:
            base = 2   # partial
        else:
            base = 1   # weak

        score = base + ote_bonus + (1 if in_ote else 0)
        if direction == "buy":
            entry = round(ob["bottom"], 5)  # enter at bottom of OB
        else:
            entry = round(ob["top"], 5)     # enter at top of OB
        label = "OB"
        if body_ok and swept:
            label = "OB★"
        if in_ote:
            label += " OTE"
        scored.append((score, entry, round(ob["bottom"],5), round(ob["top"],5), label))

    if not scored:
        return None

    # Return highest scored zone
    scored.sort(key=lambda x: x[0], reverse=True)
    score, entry, bottom, top, label = scored[0]
    return (entry, bottom, top, label, min(score, 5))




def find_internal_liquidity(smc_data, direction, entry_price, origin_price):
    """
    Find internal liquidity between entry and origin.
    These are targets between BSL and SSL:
    EQH/EQL, PDH/PDL, FVG mids, BPR mids
    Only returns levels NOT already swept (still valid targets).
    """
    candidates = []
    price = smc_data.get("current_price", 0)
    key = smc_data.get("key_levels", {})

    for tf in ["H1", "H4", "D"]:
        liq = smc_data.get("liquidity_" + tf) or {}
        sweep = smc_data.get("sweep_" + tf) or {}
        swept_level = sweep.get("level", 0) if sweep else 0

        if direction == "sell":
            # Internal lows between entry and origin
            for lvl in liq.get("buy_side", []):
                p = lvl["price"]
                # Must be below entry, above origin, NOT already swept
                if origin_price < p < entry_price and abs(p - swept_level) > 0.0001:
                    candidates.append({"price": round(p, 5), "label": f"BSL {tf}", "tf": tf})
        else:
            # Internal highs between entry and origin
            for lvl in liq.get("sell_side", []):
                p = lvl["price"]
                if entry_price < p < origin_price and abs(p - swept_level) > 0.0001:
                    candidates.append({"price": round(p, 5), "label": f"SSL {tf}", "tf": tf})

    # EQH/EQL
    for tf in ["H1", "H4"]:
        eqh_eql = smc_data.get("eqh_eql_" + tf) or {}
        if direction == "sell":
            for eq in eqh_eql.get("eql", []):
                p = eq["price"]
                if origin_price < p < entry_price:
                    candidates.append({"price": round(p, 5), "label": f"EQL {tf}", "tf": tf})
        else:
            for eq in eqh_eql.get("eqh", []):
                p = eq["price"]
                if entry_price < p < origin_price:
                    candidates.append({"price": round(p, 5), "label": f"EQH {tf}", "tf": tf})

    # Key levels PDH/PDL
    if direction == "sell":
        for lbl in ["PDL", "PWL"]:
            val = key.get(lbl)
            if val and origin_price < val < entry_price:
                candidates.append({"price": round(val, 5), "label": lbl, "tf": "D"})
    else:
        for lbl in ["PDH", "PWH"]:
            val = key.get(lbl)
            if val and entry_price < val < origin_price:
                candidates.append({"price": round(val, 5), "label": lbl, "tf": "D"})

    # FVG mids on H1/H4
    for tf in ["H1", "H4"]:
        fvgs = smc_data.get("fvg_" + tf) or []
        for fvg in fvgs:
            mid = fvg.get("mid", (fvg["top"] + fvg["bottom"]) / 2)
            if direction == "sell" and origin_price < mid < entry_price:
                candidates.append({"price": round(mid, 5), "label": f"FVG {tf}", "tf": tf})
            elif direction == "buy" and entry_price < mid < origin_price:
                candidates.append({"price": round(mid, 5), "label": f"FVG {tf}", "tf": tf})

    # Sort by proximity to entry
    if direction == "sell":
        candidates.sort(key=lambda x: x["price"], reverse=True)
    else:
        candidates.sort(key=lambda x: x["price"])

    return candidates[:3]


def classify_signal(smc_data, instrument):
    """
    Classify signal as REVERSAL or CONTINUATION.

    REVERSAL (high quality):
    - Wick sweep on H1/H4/D (body held)
    - 2+ FVG on 5M after sweep (aggression)
    - Small BOS on 5M
    - Large BOS on 15M

    CONTINUATION (medium quality):
    - Sweep but weak reaction (no aggression)
    - Price corrected to Fibo 0.5/0.618/0.705/0.79
    - BOS on 5M or 15M in trend direction

    Returns: {"type": "reversal"/"continuation"/"none",
              "direction": "buy"/"sell",
              "confidence": 0-5,
              "sl_level": price,  # liquidity level for SL
              "sl_tf": "H1"/"H4"/"D"}
    """
    result = {"type": "none", "direction": None, "confidence": 0,
              "sl_level": None, "sl_tf": None}

    price = smc_data.get("current_price", 0)
    if not price:
        return result

    trigger = smc_data.get("trigger_5m") or {}
    aggression = smc_data.get("aggression") or {}
    struct_confirm = smc_data.get("struct_confirm") or {}
    fib = smc_data.get("fibonacci") or {}

    direction = trigger.get("direction")
    if not direction:
        # Use H1 trend as fallback
        trend = (smc_data.get("structure_H1") or {}).get("trend", "")
        direction = "buy" if trend == "bullish" else "sell" if trend == "bearish" else None
    if not direction:
        return result

    # ── Check for wick sweep on H1/H4/D ──
    wick_sweep_tf = None
    sl_level = None
    for tf in ["H1", "H4", "D"]:
        sweep = smc_data.get("sweep_" + tf) or {}
        if not sweep or not sweep.get("type"):
            continue
        structure = smc_data.get("structure_" + tf) or {}
        highs = structure.get("swing_highs", [])
        lows = structure.get("swing_lows", [])
        candles = smc_data.get("candles_" + tf.lower().replace("h", "h").replace("d", "d"), [])

        # Get last candle for that TF from smc_data
        if tf == "H1":
            candles = smc_data.get("candles_h1", [])
        elif tf == "H4":
            candles = smc_data.get("candles_h4", [])
        elif tf == "D":
            candles = smc_data.get("candles_d", [])

        if not candles:
            continue
        last = candles[-1]
        body_top = max(last["o"], last["c"])
        body_bot = min(last["o"], last["c"])

        if direction == "buy" and lows:
            prev_low = lows[-1]["price"]
            if last["l"] < prev_low and body_bot > prev_low:
                wick_sweep_tf = tf
                sl_level = round(prev_low, 5)
                break
        elif direction == "sell" and highs:
            prev_high = highs[-1]["price"]
            if last["h"] > prev_high and body_top < prev_high:
                wick_sweep_tf = tf
                sl_level = round(prev_high, 5)
                break

    # ── REVERSAL check ──
    is_aggressive = aggression.get("aggressive", False) and aggression.get("confidence", 0) >= 2
    has_5m_bos = trigger.get("confirmed") and trigger.get("type") in ("BOS", "BOS+15M", "CHoCH")
    has_15m_confirm = trigger.get("type") == "BOS+15M"

    if wick_sweep_tf and is_aggressive and has_5m_bos:
        confidence = 3
        if has_15m_confirm:
            confidence += 1
        if wick_sweep_tf in ("H4", "D"):
            confidence += 1
        result.update({
            "type": "reversal",
            "direction": direction,
            "confidence": min(confidence, 5),
            "sl_level": sl_level,
            "sl_tf": wick_sweep_tf
        })
        return result

    # ── CONTINUATION check ──
    sc_scenario = struct_confirm.get("scenario", "")
    fib_50 = fib.get("fib_0_5")
    fib_618 = fib.get("fib_0_618")
    fib_705 = fib.get("fib_0_705")
    fib_79 = fib.get("fib_0_79")

    at_fibo = False
    if fib_50 and fib_79:
        fib_low = min(fib_50, fib_618 or fib_50, fib_705 or fib_50, fib_79)
        fib_high = max(fib_50, fib_618 or fib_50, fib_705 or fib_50, fib_79)
        at_fibo = fib_low <= price <= fib_high

    if sc_scenario == "continuation" and has_5m_bos:
        confidence = 2
        if at_fibo:
            confidence += 1
        if has_15m_confirm:
            confidence += 1

        # SL = nearest liquidity level for continuation
        for tf in ["H1", "H4", "D"]:
            liq = smc_data.get("liquidity_" + tf) or {}
            if direction == "buy":
                lows_below = [l["price"] for l in liq.get("buy_side", []) if l["price"] < price]
                if lows_below:
                    sl_level = round(max(lows_below), 5)
                    result["sl_tf"] = tf
                    break
            else:
                highs_above = [l["price"] for l in liq.get("sell_side", []) if l["price"] > price]
                if highs_above:
                    sl_level = round(min(highs_above), 5)
                    result["sl_tf"] = tf
                    break

        result.update({
            "type": "continuation",
            "direction": direction,
            "confidence": min(confidence, 5),
            "sl_level": sl_level,
        })
        return result

    return result

def find_origin_liquidity(candles_h1, candles_h4, candles_d, smc_data):
    """
    Find the BSL/SSL that ORIGINATED the current move.
    Logic: after sweep of SSL → find BSL that caused the rally to SSL
           after sweep of BSL → find SSL that caused the drop to BSL
    Works on H1, H4, D timeframes.
    Returns: {"origin": price, "type": "bsl"/"ssl", "tf": "H1"/"H4"/"D", "desc": str}
    """
    result = {"origin": None, "type": None, "tf": None, "desc": ""}

    for tf, candles in [("D", candles_d), ("H4", candles_h4), ("H1", candles_h1)]:
        if not candles or len(candles) < 20:
            continue

        liq = smc_data.get("liquidity_" + tf) or {}
        sweep = smc_data.get("sweep_" + tf) or {}
        price = smc_data.get("current_price", 0)

        if not sweep or not sweep.get("type"):
            continue

        buy_side = liq.get("buy_side", [])
        sell_side = liq.get("sell_side", [])

        # SSL swept → find BSL below that originated the up move
        if sweep["type"] == "sweep_sell_side":
            swept_level = sweep["level"]
            # Origin = highest BSL below swept SSL
            candidates = [l["price"] for l in buy_side if l["price"] < swept_level]
            if candidates:
                origin = min(candidates)  # lowest BSL = origin of the whole move up
                result = {
                    "origin": round(origin, 5),
                    "type": "bsl",
                    "tf": tf,
                    "desc": f"BSL {tf} {origin:.5f} — утворив рух до SSL"
                }
                return result  # Return first match (highest TF priority)

        # BSL swept → find SSL above that originated the down move
        elif sweep["type"] == "sweep_buy_side":
            swept_level = sweep["level"]
            candidates = [l["price"] for l in sell_side if l["price"] > swept_level]
            if candidates:
                origin = max(candidates)  # highest SSL = origin of the whole move down
                result = {
                    "origin": round(origin, 5),
                    "type": "ssl",
                    "tf": tf,
                    "desc": f"SSL {tf} {origin:.5f} — утворив рух до BSL"
                }
                return result

    return result


def find_intermediate_fvg(smc_data, direction, entry_price, target_price):
    """
    Find FVGs between entry and target — these are balance zones where
    price may pause/react on its way to target.
    Works on H1, H4, D timeframes.
    Returns: list of {"price": float, "tf": str, "type": str}
    """
    intermediate = []

    for tf in ["H1", "H4", "D"]:
        fvgs = smc_data.get("fvg_" + tf) or []
        for fvg in fvgs:
            mid = fvg.get("mid", (fvg["top"] + fvg["bottom"]) / 2)

            if direction == "sell":
                # Looking for FVGs between entry (high) and target (low)
                if target_price < mid < entry_price:
                    intermediate.append({
                        "price": round(mid, 5),
                        "top": round(fvg["top"], 5),
                        "bottom": round(fvg["bottom"], 5),
                        "tf": tf,
                        "type": fvg["type"]
                    })
            else:
                # Looking for FVGs between entry (low) and target (high)
                if entry_price < mid < target_price:
                    intermediate.append({
                        "price": round(mid, 5),
                        "top": round(fvg["top"], 5),
                        "bottom": round(fvg["bottom"], 5),
                        "tf": tf,
                        "type": fvg["type"]
                    })

    # Sort by proximity to entry
    if direction == "sell":
        intermediate.sort(key=lambda x: x["price"], reverse=True)
    else:
        intermediate.sort(key=lambda x: x["price"])

    return intermediate[:3]  # max 3 intermediate zones

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

    # ── SIGNAL CLASSIFICATION ──
    signal = smc_data.get("signal") or {}
    sig_type = signal.get("type", "none")
    sig_direction = signal.get("direction")
    sig_sl_level = signal.get("sl_level")
    sig_sl_tf = signal.get("sl_tf")

    # ── BUY SETUP ──
    fib = smc_data.get("fibonacci") or {}
    buy_zone = find_entry_zone(ob_h1, fvg_h1, bpr_h1, price, "buy", fib)

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
        # Use find_best_sl — tightest valid SL within max intraday distance
        buy_sl = find_best_sl(instrument, "buy", buy_entry, smc_data, buf)
        if buy_sl is None:
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

        # Collect BUY targets — must be ABOVE current price (not just above entry)
        buy_targets = []

        # Check aggressive reversal — affects TP direction
        candles_5m_data = smc_data.get("candles_5m", [])
        aggression = detect_aggressive_reversal(
            smc_data.get("candles_h1", []), smc_data.get("candles_h4", []),
            candles_5m_data, smc_data, "buy"
        )

        # 1. SSL on 1H ABOVE current price
        for lvl in liq_h1.get("sell_side", []):
            if lvl["price"] > price:
                buy_targets.append(("SSL 1H", round(lvl["price"], 5)))

        # 2. SSL on 4H ABOVE current price
        for lvl in liq_h4.get("sell_side", []):
            if lvl["price"] > price:
                buy_targets.append(("SSL 4H", round(lvl["price"], 5)))

        # 3. FVG on 4H above current price
        for fvg in fvg_h4:
            if fvg["type"] == "bearish_fvg" and fvg["bottom"] > price:
                buy_targets.append(("FVG 4H", round(fvg["bottom"], 5)))

        # 4. EQH on 1H above current price
        for eq in eqh_eql_h1.get("eqh", []):
            if eq["price"] > price:
                buy_targets.append(("EQH 1H", round(eq["price"], 5)))

        # 5. EQH on 4H above current price
        for eq in eqh_eql_h4.get("eqh", []):
            if eq["price"] > price:
                buy_targets.append(("EQH 4H", round(eq["price"], 5)))

        # 6. Key levels above current price
        for label in ["PDH", "PWH", "PMH"]:
            val = key.get(label)
            if val and val > price:
                buy_targets.append((label, round(val, 5)))

        # Sort by distance from current price (closest first)
        buy_targets.sort(key=lambda x: x[1])

        # Filter: must give at least RR 1:1.5
        min_tp = buy_entry + risk * 1.5
        valid_targets = [(l, p) for l, p in buy_targets if p >= min_tp]

        # Check origin liquidity as final target (TP2)
        origin = smc_data.get("origin_liquidity") or {}
        origin_price = origin.get("origin")
        origin_label = origin.get("desc", "")

        if origin_price and origin_price > price and sig_type in ("reversal", "continuation"):
            # Use origin as TP2
            buy_tp2 = round(origin_price, 5)
            buy_tp2_label = origin_label or "Origin BSL"

            # Find internal liquidity as TP1
            internal = find_internal_liquidity(smc_data, "buy", buy_entry, buy_tp2)
            if internal:
                tp1_candidate = internal[0]["price"]
                if tp1_candidate >= min_tp:
                    buy_tp1 = round(tp1_candidate, 5)
                    buy_tp1_label = internal[0]["label"]
                else:
                    buy_tp1_label = "RR 1:2"
                    buy_tp1 = round(buy_entry + risk * 2, 5)
            else:
                # Fallback: intermediate FVG
                intermediates = find_intermediate_fvg(smc_data, "buy", buy_entry, buy_tp2)
                if intermediates and intermediates[0]["price"] >= min_tp:
                    buy_tp1 = round(intermediates[0]["price"], 5)
                    buy_tp1_label = f"IMB {intermediates[0]['tf']}"
                elif valid_targets:
                    buy_tp1_label, buy_tp1 = valid_targets[0]
                else:
                    buy_tp1_label = "RR 1:2"
                    buy_tp1 = round(buy_entry + risk * 2, 5)

            # Validate RR >= 2 for both TPs
            if calc_rr(buy_entry, buy_sl, buy_tp1) < 2.0:
                buy_tp1 = round(buy_entry + risk * 2, 5)
                buy_tp1_label = "RR 1:2"
            if calc_rr(buy_entry, buy_sl, buy_tp2) < 3.0:
                buy_tp2 = round(buy_entry + risk * 3, 5)
                buy_tp2_label = "RR 1:3"

        elif len(valid_targets) >= 2:
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
    sell_zone = find_entry_zone(ob_h1, fvg_h1, bpr_h1, price, "sell", fib)

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
        # Use find_best_sl — tightest valid SL within max intraday distance
        sell_sl = find_best_sl(instrument, "sell", sell_entry, smc_data, buf)
        if sell_sl is None:
            sell_sl = round(zone_top + buf, 5)

    if sell_entry and sell_sl:
        risk = abs(sell_entry - sell_sl)
        if risk <= 0:
            risk = buf * 3
            sell_sl = round(sell_entry + risk, 5)

        # Collect SELL targets — must be BELOW current price
        sell_targets = []

        # Check aggressive reversal
        aggression_sell = detect_aggressive_reversal(
            smc_data.get("candles_h1", []), smc_data.get("candles_h4", []),
            smc_data.get("candles_5m", []), smc_data, "sell"
        )

        # 1. BSL on 1H BELOW current price
        for lvl in liq_h1.get("buy_side", []):
            if lvl["price"] < price:
                sell_targets.append(("BSL 1H", round(lvl["price"], 5)))

        # 2. BSL on 4H BELOW current price
        for lvl in liq_h4.get("buy_side", []):
            if lvl["price"] < price:
                sell_targets.append(("BSL 4H", round(lvl["price"], 5)))

        # 3. FVG on 4H below current price
        for fvg in fvg_h4:
            if fvg["type"] == "bullish_fvg" and fvg["top"] < price:
                sell_targets.append(("FVG 4H", round(fvg["top"], 5)))
            elif fvg["type"] == "bullish_ifvg" and fvg["top"] < price:
                sell_targets.append(("IFVG 4H", round(fvg["mid"], 5)))

        # BPR on 1H below current price
        for bpr in smc_data.get("bpr_H1", []):
            if bpr["mid"] < price:
                sell_targets.append(("BPR 1H", round(bpr["mid"], 5)))

        # 4. EQL on 1H below current price
        for eq in eqh_eql_h1.get("eql", []):
            if eq["price"] < price:
                sell_targets.append(("EQL 1H", round(eq["price"], 5)))

        # 5. EQL on 4H below current price
        for eq in eqh_eql_h4.get("eql", []):
            if eq["price"] < price:
                sell_targets.append(("EQL 4H", round(eq["price"], 5)))

        # 6. Key levels below current price
        for label in ["PDL", "PWL", "PML"]:
            val = key.get(label)
            if val and val < price:
                sell_targets.append((label, round(val, 5)))

        # Sort by distance from current price (closest first = highest below price)
        sell_targets.sort(key=lambda x: x[1], reverse=True)

        # Filter: must give at least RR 1:1.5
        min_tp = sell_entry - risk * 1.5
        valid_targets = [(l, p) for l, p in sell_targets if p <= min_tp]

        # Check origin liquidity as final target (TP2)
        origin = smc_data.get("origin_liquidity") or {}
        origin_price = origin.get("origin")
        origin_label = origin.get("desc", "")

        if origin_price and origin_price < price and sig_type in ("reversal", "continuation"):
            sell_tp2 = round(origin_price, 5)
            sell_tp2_label = origin_label or "Origin BSL"

            # Find internal liquidity as TP1
            internal = find_internal_liquidity(smc_data, "sell", sell_entry, sell_tp2)
            if internal and internal[0]["price"] <= min_tp:
                sell_tp1 = round(internal[0]["price"], 5)
                sell_tp1_label = internal[0]["label"]
            else:
                intermediates = find_intermediate_fvg(smc_data, "sell", sell_entry, sell_tp2)
                if intermediates and intermediates[0]["price"] <= min_tp:
                    sell_tp1 = round(intermediates[0]["price"], 5)
                    sell_tp1_label = f"IMB {intermediates[0]['tf']}"
                elif valid_targets:
                    sell_tp1_label, sell_tp1 = valid_targets[0]
                else:
                    sell_tp1_label = "RR 1:2"
                    sell_tp1 = round(sell_entry - risk * 2, 5)

            # Validate RR >= 2 for TP1, >= 3 for TP2
            if calc_rr(sell_entry, sell_sl, sell_tp1) < 2.0:
                sell_tp1 = round(sell_entry - risk * 2, 5)
                sell_tp1_label = "RR 1:2"
            if calc_rr(sell_entry, sell_sl, sell_tp2) < 3.0:
                sell_tp2 = round(sell_entry - risk * 3, 5)
                sell_tp2_label = "RR 1:3"

        elif len(valid_targets) >= 2:
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


async def cmd_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle free-form messages — chat with the bot about market analysis."""
    user_msg = update.message.text.strip()
    chat_id = update.effective_chat.id

    # Build context about active signals
    signals_context = ""
    for inst, sig in ACTIVE_SIGNALS.items():
        if not sig.get("closed"):
            import time as _t
            age_h = (_t.time() - sig["time"]) / 3600
            signals_context += (
                f"\n{inst}: {sig['direction'].upper()} entry={sig['entry']} "
                f"sl={sig['sl']} tp1={sig['tp1']} tp2={sig['tp2']} "
                f"(sent {age_h:.1f}h ago)"
            )

    if not signals_context:
        signals_context = "Немає активних сигналів"

    # Build prompt for Claude
    prompt = f"""Ти SMC торговий асистент. Відповідай коротко і по суті українською мовою.

Активні сигнали:
{signals_context}

Запитання трейдера: {user_msg}

Якщо питання про ціну чи сигнал — відповідай на основі активних сигналів вище.
Якщо питання загальне про ринок — відповідай коротко своїми знаннями про SMC.
Відповідь максимум 3-4 речення. Без зайвих слів."""

    try:
        thinking_msg = await update.message.reply_text("🤔 Думаю...")

        headers = {
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}]
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(ANTHROPIC_API, headers=headers, json=payload) as resp:
                data = await resp.json()
                reply = data.get("content", [{}])[0].get("text", "Не вдалось відповісти")

        await thinking_msg.edit_text(reply)

    except Exception as e:
        logger.error(f"Chat error: {e}")
        await update.message.reply_text("Помилка. Спробуй ще раз.")

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

        # Validate price — if 0 means API returned no data
        if not smc_data.get("current_price") or smc_data.get("current_price") == 0:
            await msg.edit_text(
                "⚠️ *" + display + "* — немає даних від API\n\n"
                "Можливі причини:\n"
                "• Вичерпано ліміт TwelveData (800/день)\n"
                "• Ринок закритий\n"
                "• Проблема з підключенням\n\n"
                "Спробуй пізніше або перевір: twelvedata.com/account",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        session_info = get_session_info()
        await msg.edit_text(emoji + " *" + display + "* — AI генерує аналіз...", parse_mode=ParseMode.MARKDOWN)
        analysis = await get_ai_analysis(instrument, smc_data, session_info)
        try:
            await msg.edit_text(analysis, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        except Exception:
            clean = analysis.replace("*", "").replace("`", "").replace("_", "").replace("#", "")
            await msg.edit_text(clean, disable_web_page_preview=True)
    except Exception as e:
        logger.error("Analysis error: " + str(e), exc_info=True)
        await msg.edit_text("Помилка аналізу " + display + ":\n" + str(e)[:200])


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


def save_active_signal(instrument, setup, direction, price):
    """Save signal to ACTIVE_SIGNALS for tracking."""
    import time
    entry = setup["entry"]
    # Check if price already at/past entry zone at signal time
    if direction == "buy":
        entry_reached = price <= entry * 1.0005  # price at or below entry
    else:
        entry_reached = price >= entry * 0.9995  # price at or above entry

    ACTIVE_SIGNALS[instrument] = {
        "direction": direction,
        "entry": entry,
        "sl": setup["sl"],
        "tp1": setup["tp1"],
        "tp2": setup["tp2"],
        "tp1_label": setup.get("tp1_label", "TP1"),
        "tp2_label": setup.get("tp2_label", "TP2"),
        "zone_label": setup.get("zone_label", ""),
        "price_at_signal": price,
        "time": time.time(),
        "entry_reached": entry_reached,  # True if price already at entry
        "notified_entry": entry_reached, # skip near_entry if already there
        "notified_tp1": False,
        "closed": False,
    }



def detect_trigger_confirmation(candles_5m, candles_15m, direction):
    """
    Detect BOS with body close + at least 1 FVG formed.
    For active signal — confirms entry timing.
    
    BUY trigger:  body close ABOVE swing high + FVG formed upward
    SELL trigger: body close BELOW swing low  + FVG formed downward
    
    Returns: {"confirmed": bool, "type": "5M"/"5M+15M", "fvg_count": int}
    """
    result = {"confirmed": False, "type": None, "fvg_count": 0}

    if not candles_5m or len(candles_5m) < 5:
        return result

    recent_5m = candles_5m[-10:]
    last = recent_5m[-1]
    prev = recent_5m[-2]

    # Find recent swing high/low on 5M
    highs = [c["h"] for c in recent_5m[:-1]]
    lows  = [c["l"] for c in recent_5m[:-1]]
    if not highs or not lows:
        return result

    prev_high = max(highs[-5:]) if len(highs) >= 5 else max(highs)
    prev_low  = min(lows[-5:])  if len(lows)  >= 5 else min(lows)

    # Check BOS with BODY (not just wick)
    body_top = max(last["o"], last["c"])
    body_bot = min(last["o"], last["c"])

    bos_confirmed = False
    if direction == "buy" and body_top > prev_high:
        bos_confirmed = True
    elif direction == "sell" and body_bot < prev_low:
        bos_confirmed = True

    if not bos_confirmed:
        return result

    # Count FVGs formed in last 3 candles (strength proof)
    fvg_count = 0
    check_candles = recent_5m[-4:]
    for i in range(1, len(check_candles) - 1):
        p = check_candles[i - 1]
        n = check_candles[i + 1]
        if direction == "buy" and n["l"] > p["h"]:
            fvg_count += 1
        elif direction == "sell" and n["h"] < p["l"]:
            fvg_count += 1

    if fvg_count < 1:
        return result  # Need at least 1 FVG

    result["confirmed"] = True
    result["fvg_count"] = fvg_count
    result["type"] = "5M"

    # Check 15M confirmation — body close in same direction
    if candles_15m and len(candles_15m) >= 3:
        last_15 = candles_15m[-1]
        prev_15 = candles_15m[-2]
        body_top_15 = max(last_15["o"], last_15["c"])
        body_bot_15 = min(last_15["o"], last_15["c"])
        highs_15 = [c["h"] for c in candles_15m[-5:-1]]
        lows_15  = [c["l"] for c in candles_15m[-5:-1]]

        if highs_15 and lows_15:
            if direction == "buy" and body_top_15 > max(highs_15):
                result["type"] = "5M+15M"
            elif direction == "sell" and body_bot_15 < min(lows_15):
                result["type"] = "5M+15M"

    return result

def check_active_signal(instrument, current_price, pv):
    """
    Check status of active signal.
    Returns: {"status": "near_entry"/"tp1_hit"/"sl_hit"/"active"/"expired"/"none", "msg": str}
    """
    import time
    signal = ACTIVE_SIGNALS.get(instrument)
    if not signal or signal.get("closed"):
        return {"status": "none"}

    # Check expiry (24 hours)
    age = time.time() - signal["time"]
    if age > SIGNAL_TTL:
        ACTIVE_SIGNALS[instrument]["closed"] = True
        return {"status": "expired"}

    entry = signal["entry"]
    sl = signal["sl"]
    tp1 = signal["tp1"]
    tp2 = signal["tp2"]
    direction = signal["direction"]

    # Near entry check — notify when price approaches entry zone
    if not signal["notified_entry"]:
        near_threshold = pv * 10
        if "BTC" in instrument:
            near_threshold = 100
        elif "XAU" in instrument:
            near_threshold = 1.5

        distance = abs(current_price - entry)
        if distance <= near_threshold:
            ACTIVE_SIGNALS[instrument]["notified_entry"] = True
            ACTIVE_SIGNALS[instrument]["entry_reached"] = True
            return {"status": "near_entry", "entry": entry, "direction": direction}

    # Only check SL/TP after entry was reached
    entry_reached = signal.get("entry_reached", False)

    # Mark entry as reached if price crossed entry level
    if not entry_reached:
        if direction == "buy" and current_price <= entry:
            ACTIVE_SIGNALS[instrument]["entry_reached"] = True
            entry_reached = True
        elif direction == "sell" and current_price >= entry:
            ACTIVE_SIGNALS[instrument]["entry_reached"] = True
            entry_reached = True

    # Trigger check — BOS+FVG on 5M even before entry reached
    if not entry_reached and not signal.get("notified_trigger"):
        # We need candles to check trigger — passed via smc_data in alert loop
        trigger = signal.get("last_trigger_check") or {}
        if trigger.get("confirmed"):
            ACTIVE_SIGNALS[instrument]["notified_trigger"] = True
            return {
                "status": "trigger_confirmed",
                "direction": direction,
                "entry": entry,
                "type": trigger.get("type", "5M"),
                "fvg_count": trigger.get("fvg_count", 1),
            }

    if not entry_reached:
        return {"status": "active"}

    # SL hit — only after entry reached
    if direction == "buy" and current_price <= sl:
        ACTIVE_SIGNALS[instrument]["closed"] = True
        return {"status": "sl_hit", "level": sl}
    if direction == "sell" and current_price >= sl:
        ACTIVE_SIGNALS[instrument]["closed"] = True
        return {"status": "sl_hit", "level": sl}

    # TP1 hit — only after entry reached
    if not signal["notified_tp1"]:
        if direction == "buy" and current_price >= tp1:
            ACTIVE_SIGNALS[instrument]["notified_tp1"] = True
            return {"status": "tp1_hit", "level": tp1, "label": signal["tp1_label"]}
        if direction == "sell" and current_price <= tp1:
            ACTIVE_SIGNALS[instrument]["notified_tp1"] = True
            return {"status": "tp1_hit", "level": tp1, "label": signal["tp1_label"]}

    return {"status": "active"}


def format_signal_update(instrument, status_info, current_price):
    """Format notification message for signal update."""
    display = instrument.replace("_", "/")
    emoji = "📈" if status_info.get("direction") == "buy" else "📉"
    status = status_info["status"]

    if status == "trigger_confirmed":
        direction_text = "КУПІВЛЯ" if status_info.get("direction") == "buy" else "ПРОДАЖ"
        sig_type = status_info.get("type", "5M")
        fvg_count = status_info.get("fvg_count", 1)
        confirm_emoji = "🔥" if sig_type == "5M+15M" else "⚡"
        return (
            f"{confirm_emoji} *{display}* — ТРИГЕР ПІДТВЕРДЖЕНО!\n\n"
            f"{emoji} Напрямок: *{direction_text}*\n"
            f"🎯 Точка входу: `{status_info['entry']}`\n"
            f"📊 Підтвердження: *{sig_type}* (злам тілом + {fvg_count} FVG)\n"
            f"💰 Поточна ціна: `{current_price}`\n\n"
            f"{'✅ Повне підтвердження 5M+15M — сильний сигнал!' if sig_type == '5M+15M' else '⚠️ Підтвердження тільки 5M — чекай 15M для впевненості'}"
        )

    if status == "near_entry":
        direction_text = "КУПІВЛЯ" if status_info["direction"] == "buy" else "ПРОДАЖ"
        return (
            f"⚡ *{display}* — ЦІНА БІЛЯ ТОЧКИ ВХОДУ\n\n"
            f"{emoji} Напрямок: *{direction_text}*\n"
            f"🎯 Точка входу: `{status_info['entry']}`\n"
            f"💰 Поточна ціна: `{current_price}`\n\n"
            f"👀 Придивляйся за ціною та чекай підтвердження на 5M"
        )
    elif status == "tp1_hit":
        return (
            f"🎯 *{display}* — TP1 ДОСЯГНУТО!\n\n"
            f"✅ {status_info['label']}: `{status_info['level']}`\n"
            f"💰 Поточна ціна: `{current_price}`\n\n"
            f"💡 Розглянь часткове закриття та перемісти SL в беззбиток"
        )
    elif status == "sl_hit":
        return (
            f"🛑 *{display}* — СТОП ЛОС АКТИВОВАНО\n\n"
            f"❌ SL: `{status_info['level']}`\n"
            f"💰 Поточна ціна: `{current_price}`\n\n"
            f"📊 Сигнал закрито. Чекаємо наступний сетап"
        )
    return ""

async def alert_loop(app):
    await asyncio.sleep(60)
    while True:
        try:
            if ALERT_USERS:
                dublin = get_dublin_time()

                # Skip asian session entirely
                if is_asian_session():
                    logger.info(f"Asian session ({dublin.strftime('%H:%M')} Dublin) — sleeping 15 min")
                    await asyncio.sleep(15 * 60)
                    continue

                # Skip if not in any killzone
                if not is_killzone():
                    logger.info(f"Not in killzone ({dublin.strftime('%H:%M')} Dublin) — sleeping 15 min")
                    await asyncio.sleep(15 * 60)
                    continue

                # Determine scan frequency
                high_freq = is_5min_period()
                period_label = "5min scan" if high_freq else "15min scan"
                logger.info(f"[{dublin.strftime('%H:%M')} Dublin] {period_label}")

                active = get_active_instruments()
                if is_weekend():
                    logger.info(f"[{dublin.strftime('%H:%M')} Dublin] Weekend — forex/gold off, BTC only")
                for instrument in active:
                    try:
                        # Use cached fetch — only refreshes stale TFs
                        await asyncio.sleep(2)  # small pause between instruments
                        candles = await fetch_candles_cached(instrument, TWELVEDATA_API_KEY)
                        smc_data = analyze_smc(candles, instrument)
                        current_price = smc_data.get("current_price", 0)
                        score = smc_data.get("setup_quality", 0)
                        logger.info(f"[ALERT SCAN] {instrument} | price={current_price} | score={score}/5")
                        if not current_price or current_price == 0:
                            logger.warning("No price data for " + instrument + " — skipping")
                            await asyncio.sleep(3)
                            continue

                        # Check real setups exist (entry/SL/TP)
                        setups = calculate_setups(instrument, smc_data)
                        has_real_setup = bool(setups.get("buy") or setups.get("sell"))
                        logger.info(f"[ALERT SCAN] {instrument} | real_setup={has_real_setup} | setups={list(setups.keys())}")

                        # Check active signal status (near entry / TP1 / SL / trigger)
                        pv = pip_value(instrument)
                        # Update trigger check in active signal before checking status
                        if ACTIVE_SIGNALS.get(instrument) and not ACTIVE_SIGNALS[instrument].get("closed"):
                            sig = ACTIVE_SIGNALS[instrument]
                            if not sig.get("notified_trigger"):
                                candles_5m_data = candles.get("M5", [])
                                candles_15m_data = candles.get("M15", [])
                                trig = detect_trigger_confirmation(
                                    candles_5m_data, candles_15m_data,
                                    sig.get("direction", "buy")
                                )
                                ACTIVE_SIGNALS[instrument]["last_trigger_check"] = trig
                        sig_status = check_active_signal(instrument, current_price, pv)
                        if sig_status["status"] in ("near_entry", "tp1_hit", "sl_hit"):
                            update_msg = format_signal_update(instrument, sig_status, current_price)
                            if update_msg:
                                for chat_id in list(ALERT_USERS):
                                    try:
                                        await app.bot.send_message(
                                            chat_id=chat_id,
                                            text=update_msg,
                                            parse_mode=ParseMode.MARKDOWN
                                        )
                                    except Exception as e:
                                        logger.error(f"Signal update error: {e}")
                            await asyncio.sleep(3)
                            continue

                        if score >= 3 and has_real_setup:
                            pv = pip_value(instrument)
                            last_price = SENT_ALERTS.get(instrument, 0)
                            price_change_pips = abs(current_price - last_price) / pv if pv > 0 else 999
                            min_change = min_price_change(instrument)
                            logger.info(f"[ALERT] {instrument} | price_change={round(price_change_pips)} | min={min_change}")
                            if price_change_pips < min_change:
                                logger.info("Skipping " + instrument + " — price unchanged (" + str(round(price_change_pips)) + ")")
                                await asyncio.sleep(3)
                                continue
                            # Check if there's already an active signal
                            existing = ACTIVE_SIGNALS.get(instrument, {})
                            if existing and not existing.get("closed") and (time.time() - existing.get("time", 0)) < SIGNAL_TTL:
                                logger.info(f"[ALERT] {instrument} — active signal exists, skipping new")
                                await asyncio.sleep(3)
                                continue

                            # Send alert
                            session_info = get_session_info()
                            analysis = await get_ai_analysis(instrument, smc_data, session_info, alert_mode=True)
                            sent_ok = False
                            for chat_id in list(ALERT_USERS):
                                try:
                                    try:
                                        await app.bot.send_message(
                                            chat_id=chat_id,
                                            text=analysis,
                                            parse_mode=ParseMode.MARKDOWN,
                                            disable_web_page_preview=True
                                        )
                                    except Exception:
                                        clean = analysis.replace("*", "").replace("`", "").replace("_", "").replace("#", "")
                                        await app.bot.send_message(
                                            chat_id=chat_id,
                                            text=clean,
                                            disable_web_page_preview=True
                                        )
                                    sent_ok = True
                                except Exception as e:
                                    logger.error("Alert send error: " + str(e))
                            if sent_ok:
                                SENT_ALERTS[instrument] = current_price
                                # Save to active signals tracking
                                direction = "buy" if setups.get("buy") else "sell"
                                active_setup = setups.get("buy") or setups.get("sell")
                                if active_setup:
                                    save_active_signal(instrument, active_setup, direction, current_price)
                                logger.info("Alert sent for " + instrument + " at " + str(current_price))
                        else:
                            logger.info(f"[ALERT] {instrument} — skipping (score={score}/5, real_setup={has_real_setup})")
                        await asyncio.sleep(3)
                    except Exception as e:
                        logger.error("Alert scan error " + instrument + ": " + str(e))

                # Sleep based on current period
                if is_5min_period():
                    logger.info(f"[{get_dublin_time().strftime('%H:%M')} Dublin] Next scan in 5 min")
                    await asyncio.sleep(5 * 60)
                else:
                    logger.info(f"[{get_dublin_time().strftime('%H:%M')} Dublin] Next scan in 15 min")
                    await asyncio.sleep(15 * 60)

            else:
                await asyncio.sleep(60)
                continue

        except Exception as e:
            logger.error("Alert loop error: " + str(e))
            await asyncio.sleep(60)


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
    # Chat handler — responds to any non-command message
    from telegram.ext import MessageHandler, filters
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_chat))
    logger.info("SMC Trading Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
