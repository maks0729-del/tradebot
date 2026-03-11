import aiohttp
import asyncio
from datetime import datetime, timezone
from typing import Dict, List

TWELVEDATA_BASE = "https://api.twelvedata.com"

# Instrument mapping: internal name -> TwelveData symbol
SYMBOL_MAP = {
    "EUR_USD": "EUR/USD",
    "GBP_USD": "GBP/USD",
    "XAU_USD": "XAU/USD",
    "BTC_USD": "BTC/USD",
}

# Timeframe mapping: internal label -> TwelveData interval, count of candles
TIMEFRAMES = {
    "M":   {"interval": "1month",  "count": 12},
    "W":   {"interval": "1week",   "count": 12},
    "D":   {"interval": "1day",    "count": 30},
    "H4":  {"interval": "4h",      "count": 60},
    "H1":  {"interval": "1h",      "count": 100},
    "M15": {"interval": "15min",   "count": 96},
}


async def fetch_twelvedata(
    session: aiohttp.ClientSession,
    api_key: str,
    symbol: str,
    interval: str,
    count: int
) -> list:
    """Fetch candles from TwelveData API."""
    url = (
        f"{TWELVEDATA_BASE}/time_series"
        f"?symbol={symbol}"
        f"&interval={interval}"
        f"&outputsize={count}"
        f"&apikey={api_key}"
        f"&format=JSON"
    )

    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        if resp.status != 200:
            raise Exception(f"TwelveData HTTP error: {resp.status}")
        data = await resp.json()

    if data.get("status") == "error":
        raise Exception(f"TwelveData error: {data.get('message', 'Unknown error')}")

    values = data.get("values", [])
    if not values:
        return []

    # TwelveData returns newest first — reverse to oldest first
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


async def fetch_candles(instrument: str, api_key: str) -> Dict[str, List[dict]]:
    """Fetch candles for all timeframes for a given instrument."""
    symbol = SYMBOL_MAP.get(instrument)
    if not symbol:
        raise Exception(f"Невідомий інструмент: {instrument}")

    result = {}

    async with aiohttp.ClientSession() as session:
        for tf_label, tf_cfg in TIMEFRAMES.items():
            try:
                candles = await fetch_twelvedata(
                    session, api_key, symbol,
                    tf_cfg["interval"], tf_cfg["count"]
                )
                result[tf_label] = candles
            except Exception as e:
                result[tf_label] = []

            await asyncio.sleep(0.5)

    return result


def get_session_info() -> dict:
    """Determine current trading session based on UTC time."""
    now = datetime.now(timezone.utc)
    hour = now.hour

    if 22 <= hour or hour < 8:
        return {
            "name": "Азійська", "emoji": "🌏", "active": True, "slug": "asian",
            "desc": "Низька волатильність. Формується рейндж."
        }
    elif 8 <= hour < 12:
        return {
            "name": "Лондонська", "emoji": "🇬🇧", "active": True, "slug": "london",
            "desc": "Висока волатильність. Часто свіп азійського рейнджу."
        }
    elif 12 <= hour < 17:
        return {
            "name": "Нью-Йорк", "emoji": "🗽", "active": True, "slug": "newyork",
            "desc": "Найвища волатильність. Основні рухи дня."
        }
    else:
        return {
            "name": "Між сесіями", "emoji": "😴", "active": False, "slug": "off",
            "desc": "Низька активність. Краще чекати наступної сесії."
        }
