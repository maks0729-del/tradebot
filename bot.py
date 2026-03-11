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
}


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


def detect_market_structure(candles):
    if len(candles) < 10:
        return {"trend": "unknown", "last_bos": None, "last_choch": None}
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
                         "mid": (nxt["l"] + prev["h"]) / 2, "filled": False})
        elif nxt["h"] < prev["l"]:
            fvgs.append({"type": "bearish_fvg", "top": prev["l"], "bottom": nxt["h"],
                         "mid": (prev["l"] + nxt["h"]) / 2, "filled": False})
    last_close = candles[-1]["c"] if candles else 0
    for fvg in fvgs:
        if fvg["type"] == "bullish_fvg" and last_close < fvg["bottom"]:
            fvg["filled"] = True
        elif fvg["type"] == "bearish_fvg" and last_close > fvg["top"]:
            fvg["filled"] = True
    return [f for f in fvgs if not f["filled"]][-4:]


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


def analyze_smc(candles_by_tf):
    result = {}
    for tf in ["M", "W", "D", "H4", "H1", "M15"]:
        c = candles_by_tf.get(tf, [])
        if c:
            result["structure_" + tf] = detect_market_structure(c)
            result["ob_" + tf] = find_order_blocks(c, result["structure_" + tf])
            result["fvg_" + tf] = find_fvg(c)
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
    return result




# ── CALCULATIONS ──────────────────────────────────────────────────────────────

def pip_value(instrument):
    """Return pip size for instrument."""
    if "JPY" in instrument:
        return 0.01
    if "XAU" in instrument:
        return 0.01
    if "BTC" in instrument:
        return 1.0
    return 0.0001


def pips(price_diff, instrument):
    """Convert price difference to pips."""
    pv = pip_value(instrument)
    return round(abs(price_diff) / pv)


def calc_rr(entry, sl, tp):
    """Calculate Risk:Reward ratio."""
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    if risk == 0:
        return 0
    return round(reward / risk, 1)


def calculate_setups(instrument, smc_data):
    """Calculate BUY and SELL setups with exact SL/TP/RR/pips."""
    price = smc_data.get("current_price", 0)
    key = smc_data.get("key_levels", {})
    ob_h1 = smc_data.get("ob_H1", [])
    ob_m15 = smc_data.get("ob_M15", [])
    fvg_h1 = smc_data.get("fvg_H1", [])
    fvg_m15 = smc_data.get("fvg_M15", [])
    pd_h1 = smc_data.get("pd_zone_H1", {})
    structure_h1 = smc_data.get("structure_H1", {})
    trend = structure_h1.get("trend", "unknown")

    pv = pip_value(instrument)
    setups = {}

    # ── BUY SETUP ──
    # Entry: bottom of nearest bullish OB or FVG below price
    buy_obs = [o for o in ob_h1 if o["type"] == "bullish_ob" and o["bottom"] < price]
    buy_fvgs = [f for f in fvg_h1 if f["type"] == "bullish_fvg" and f["bottom"] < price]

    buy_entry = None
    buy_sl = None
    buy_tp1 = None
    buy_tp2 = None

    if buy_obs:
        ob = buy_obs[-1]
        buy_entry = round((ob["top"] + ob["bottom"]) / 2, 5)
        buy_sl = round(ob["bottom"] - pv * 5, 5)  # 5 pips below OB
    elif buy_fvgs:
        fvg = buy_fvgs[-1]
        buy_entry = round(fvg["mid"], 5)
        buy_sl = round(fvg["bottom"] - pv * 5, 5)

    if buy_entry and buy_sl:
        risk = abs(buy_entry - buy_sl)
        buy_tp1 = round(buy_entry + risk * 2, 5)  # RR 1:2
        buy_tp2 = round(buy_entry + risk * 3, 5)  # RR 1:3
        # Override TP with key levels if closer
        pdh = key.get("PDH")
        pwh = key.get("PWH")
        if pdh and pdh > buy_entry:
            buy_tp1 = round(pdh, 5)
        if pwh and pwh > buy_entry:
            buy_tp2 = round(pwh, 5)

        setups["buy"] = {
            "entry": buy_entry,
            "sl": buy_sl,
            "tp1": buy_tp1,
            "tp2": buy_tp2,
            "sl_pips": pips(buy_entry - buy_sl, instrument),
            "tp1_pips": pips(buy_tp1 - buy_entry, instrument),
            "tp2_pips": pips(buy_tp2 - buy_entry, instrument),
            "rr1": calc_rr(buy_entry, buy_sl, buy_tp1),
            "rr2": calc_rr(buy_entry, buy_sl, buy_tp2),
        }

    # ── SELL SETUP ──
    sell_obs = [o for o in ob_h1 if o["type"] == "bearish_ob" and o["top"] > price]
    sell_fvgs = [f for f in fvg_h1 if f["type"] == "bearish_fvg" and f["top"] > price]

    sell_entry = None
    sell_sl = None
    sell_tp1 = None
    sell_tp2 = None

    if sell_obs:
        ob = sell_obs[0]
        sell_entry = round((ob["top"] + ob["bottom"]) / 2, 5)
        sell_sl = round(ob["top"] + pv * 5, 5)
    elif sell_fvgs:
        fvg = sell_fvgs[0]
        sell_entry = round(fvg["mid"], 5)
        sell_sl = round(fvg["top"] + pv * 5, 5)

    if sell_entry and sell_sl:
        risk = abs(sell_entry - sell_sl)
        sell_tp1 = round(sell_entry - risk * 2, 5)
        sell_tp2 = round(sell_entry - risk * 3, 5)
        pdl = key.get("PDL")
        pwl = key.get("PWL")
        if pdl and pdl < sell_entry:
            sell_tp1 = round(pdl, 5)
        if pwl and pwl < sell_entry:
            sell_tp2 = round(pwl, 5)

        setups["sell"] = {
            "entry": sell_entry,
            "sl": sell_sl,
            "tp1": sell_tp1,
            "tp2": sell_tp2,
            "sl_pips": pips(sell_entry - sell_sl, instrument),
            "tp1_pips": pips(sell_entry - sell_tp1, instrument),
            "tp2_pips": pips(sell_entry - sell_tp2, instrument),
            "rr1": calc_rr(sell_entry, sell_sl, sell_tp1),
            "rr2": calc_rr(sell_entry, sell_sl, sell_tp2),
        }

    return setups


def format_setup(setup, direction):
    """Format setup as readable string for AI prompt."""
    if not setup:
        return "зона не визначена"
    arrow = "BUY" if direction == "buy" else "SELL"
    return (
        arrow + " Entry: " + "{:.5f}".format(setup["entry"]) +
        " | SL: " + "{:.5f}".format(setup["sl"]) + " (" + str(setup["sl_pips"]) + " pips)" +
        " | TP1: " + "{:.5f}".format(setup["tp1"]) + " (" + str(setup["tp1_pips"]) + " pips, RR 1:" + str(setup["rr1"]) + ")" +
        " | TP2: " + "{:.5f}".format(setup["tp2"]) + " (" + str(setup["tp2_pips"]) + " pips, RR 1:" + str(setup["rr2"]) + ")"
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
        for f in fvgs[-2:]:
            d = "Bullish" if "bullish" in f["type"] else "Bearish"
            lines.append("  " + d + " FVG: " + "{:.5f}".format(f["bottom"]) + " - " + "{:.5f}".format(f["top"]))
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
        "=== РІВНІ ===\n"
        "PMH/PML: " + str(key.get("PMH", "N/A")) + " / " + str(key.get("PML", "N/A")) + "\n"
        "PWH/PWL: " + str(key.get("PWH", "N/A")) + " / " + str(key.get("PWL", "N/A")) + "\n"
        "PDH/PDL: " + str(key.get("PDH", "N/A")) + " / " + str(key.get("PDL", "N/A")) + "\n\n"
        "=== ORDER BLOCKS ===\n"
        "4H:\n" + ob_str(smc_data.get("ob_H4", [])) + "\n"
        "1H:\n" + ob_str(smc_data.get("ob_H1", [])) + "\n"
        "15M:\n" + ob_str(smc_data.get("ob_M15", [])) + "\n\n"
        "=== FVG ===\n"
        "4H:\n" + fvg_str(smc_data.get("fvg_H4", [])) + "\n"
        "1H:\n" + fvg_str(smc_data.get("fvg_H1", [])) + "\n"
        "15M:\n" + fvg_str(smc_data.get("fvg_M15", [])) + "\n\n"
        "=== ЛІКВІДНІСТЬ ===\n"
        "1H:\n" + liq_str(smc_data.get("liquidity_H1", {})) + "\n"
        "15M:\n" + liq_str(smc_data.get("liquidity_M15", {})) + "\n\n"
        "=== СВІПИ ===\n"
        "4H: " + sweep_str(smc_data.get("sweep_H4")) + "\n"
        "1H: " + sweep_str(smc_data.get("sweep_H1")) + "\n"
        "15M: " + sweep_str(smc_data.get("sweep_M15")) + "\n\n"
        "=== PREMIUM/DISCOUNT ===\n"
        "4H: " + pd_str(smc_data.get("pd_zone_H4", {})) + "\n"
        "1H: " + pd_str(smc_data.get("pd_zone_H1", {})) + "\n\n"
        "SCORE: " + str(smc_data.get("setup_quality", 0)) + "/5\n\n"
        "ПРАВИЛА: RR мін 1:2, ціль 1:3+, ризик 0.5-1%, prop challenge +8%\n\n"
        "=== РОЗРАХОВАНІ РІВНІ (використовуй ТІЛЬКИ ЦІ цифри!) ===\n"
        "BUY: " + format_setup(setups.get("buy"), "buy") + "\n"
        "SELL: " + format_setup(setups.get("sell"), "sell") + "\n\n"
        "ВАЖЛИВО: НЕ придумуй свої рівні! Використовуй тільки цифри вище.\n\n"
        "Дай відповідь УКРАЇНСЬКОЮ, формат Telegram Markdown:\n"
        "1. BIAS\n2. СИТУАЦІЯ\n"
        "3. BUY сценарій (використай розраховані рівні вище)\n"
        "4. SELL сценарій (використай розраховані рівні вище)\n"
        "5. ТРИГЕР на 15M\n6. ВИСНОВОК"
    )
    setups = calculate_setups(instrument, smc_data)

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
        "BOS/CHoCH · OB · FVG · Liquidity Sweeps\n\n"
        "*Команди:*\n"
        "`/analyze XAUUSD`\n"
        "`/analyze EURUSD`\n"
        "`/analyze GBPUSD`\n"
        "`/analyze BTCUSD`\n\n"
        "`/alerts on` — автоматичні алерти\n"
        "`/alerts off` — вимкнути\n"
        "`/status` — статус бота"
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
        emoji + " *" + display + "* — завантажую дані...",
        parse_mode=ParseMode.MARKDOWN
    )

    try:
        await msg.edit_text(emoji + " *" + display + "* — збираю свічки (6 TF)...", parse_mode=ParseMode.MARKDOWN)
        candles = await fetch_candles(instrument, TWELVEDATA_API_KEY)

        await msg.edit_text(emoji + " *" + display + "* — аналізую SMC структуру...", parse_mode=ParseMode.MARKDOWN)
        smc_data = analyze_smc(candles)
        session_info = get_session_info()

        await msg.edit_text(emoji + " *" + display + "* — AI генерує аналіз...", parse_mode=ParseMode.MARKDOWN)
        analysis = await get_ai_analysis(instrument, smc_data, session_info)

        await msg.edit_text(analysis, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

    except Exception as e:
        logger.error("Analysis error: " + str(e), exc_info=True)
        await msg.edit_text("Помилка аналізу " + display + ":\n`" + str(e)[:200] + "`", parse_mode=ParseMode.MARKDOWN)


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
    text = (
        "🤖 *SMC Bot*\n\n"
        "UTC: `" + now.strftime("%H:%M %d.%m.%Y") + "`\n"
        "Сесія: *" + session["name"] + "* " + session["emoji"] + "\n"
        "Алерти: " + alert_status + "\n"
        "Інструменти: EUR/USD · GBP/USD · XAU/USD · BTC/USD"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ── ALERT LOOP ────────────────────────────────────────────────────────────────

async def alert_loop(app):
    await asyncio.sleep(60)
    while True:
        try:
            if ALERT_USERS:
                for instrument in INSTRUMENTS:
                    try:
                        candles = await fetch_candles(instrument, TWELVEDATA_API_KEY)
                        smc_data = analyze_smc(candles)
                        if smc_data.get("has_setup") and smc_data.get("setup_quality", 0) >= 3:
                            session_info = get_session_info()
                            analysis = await get_ai_analysis(instrument, smc_data, session_info, alert_mode=True)
                            for chat_id in list(ALERT_USERS):
                                try:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=analysis,
                                        parse_mode=ParseMode.MARKDOWN,
                                        disable_web_page_preview=True
                                    )
                                except Exception as e:
                                    logger.error("Alert send error: " + str(e))
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
    app.add_handler(CommandHandler("alerts", cmd_alerts))
    app.add_handler(CommandHandler("status", cmd_status))
    logger.info("SMC Trading Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
