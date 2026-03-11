import aiohttp
import json
from typing import Optional


ANTHROPIC_API = "https://api.anthropic.com/v1/messages"


def format_smc_for_prompt(instrument: str, smc: dict, session: dict) -> str:
    price = smc.get("current_price", 0)
    key = smc.get("key_levels", {})

    def trend_arrow(t):
        arrows = {"bullish": "UP Bullish", "bearish": "DOWN Bearish", "ranging": "Ranging"}
        return arrows.get(t, "Unknown")

    def ob_str(obs):
        if not obs:
            return "  не знайдено"
        lines = []
        for o in obs[-2:]:
            direction = "Bullish" if "bullish" in o["type"] else "Bearish"
            bottom = "{:.5f}".format(o["bottom"])
            top = "{:.5f}".format(o["top"])
            lines.append("  " + direction + " OB: " + bottom + " - " + top)
        return "\n".join(lines)

    def fvg_str(fvgs):
        if not fvgs:
            return "  не знайдено"
        lines = []
        for f in fvgs[-2:]:
            direction = "Bullish" if "bullish" in f["type"] else "Bearish"
            bottom = "{:.5f}".format(f["bottom"])
            top = "{:.5f}".format(f["top"])
            lines.append("  " + direction + " FVG: " + bottom + " - " + top)
        return "\n".join(lines)

    def liq_str(liq):
        bsl = liq.get("buy_side", [])
        ssl = liq.get("sell_side", [])
        parts = []
        if bsl:
            bsl_prices = ", ".join("{:.5f}".format(l["price"]) for l in bsl[-2:])
            parts.append("  BSL (стопи під): " + bsl_prices)
        if ssl:
            ssl_prices = ", ".join("{:.5f}".format(l["price"]) for l in ssl[-2:])
            parts.append("  SSL (стопи над): " + ssl_prices)
        return "\n".join(parts) if parts else "  не визначено"

    def sweep_str(sweep):
        if not sweep:
            return "немає"
        return "CONFIRMED: " + sweep["desc"] + " -> напрям: " + sweep["direction"]

    def pd_str(pd):
        zone = pd.get("zone", "unknown")
        pct = pd.get("percent", 0)
        eq = pd.get("equilibrium", 0)
        icons = {"premium": "Premium", "discount": "Discount", "equilibrium": "Equilibrium"}
        zone_label = icons.get(zone, zone)
        return zone_label + " (" + str(pct) + "% від рейнджу, EQ: " + "{:.5f}".format(eq) + ")"

    m_s = smc.get("structure_M", {})
    w_s = smc.get("structure_W", {})
    d_s = smc.get("structure_D", {})
    h4_s = smc.get("structure_H4", {})
    h1_s = smc.get("structure_H1", {})
    m15_s = smc.get("structure_M15", {})

    pmh = str(key.get("PMH", "N/A"))
    pml = str(key.get("PML", "N/A"))
    pwh = str(key.get("PWH", "N/A"))
    pwl = str(key.get("PWL", "N/A"))
    pdh = str(key.get("PDH", "N/A"))
    pdl = str(key.get("PDL", "N/A"))

    score = smc.get("setup_quality", 0)
    has_setup = "TAK" if smc.get("has_setup") else "NI"

    prompt = (
        "Ти досвідчений SMC трейдер. Проведи повний топ-даун аналіз та дай торговий план.\n\n"
        "ІНСТРУМЕНТ: " + instrument.replace("_", "/") + "\n"
        "ПОТОЧНА ЦІНА: " + "{:.5f}".format(price) + "\n"
        "СЕСІЯ: " + session["name"] + " " + session["emoji"] + "\n\n"
        "=== СТРУКТУРА РИНКУ ===\n"
        "Monthly : " + trend_arrow(m_s.get("trend", "unknown")) + "\n"
        "Weekly  : " + trend_arrow(w_s.get("trend", "unknown")) + "\n"
        "Daily   : " + trend_arrow(d_s.get("trend", "unknown")) + "\n"
        "4H      : " + trend_arrow(h4_s.get("trend", "unknown")) + "\n"
        "1H      : " + trend_arrow(h1_s.get("trend", "unknown")) + "\n"
        "15M     : " + trend_arrow(m15_s.get("trend", "unknown")) + "\n\n"
        "=== КЛЮЧОВІ РІВНІ ===\n"
        "PMH: " + pmh + "  |  PML: " + pml + "\n"
        "PWH: " + pwh + "  |  PWL: " + pwl + "\n"
        "PDH: " + pdh + "  |  PDL: " + pdl + "\n\n"
        "=== ORDER BLOCKS ===\n"
        "4H OB:\n" + ob_str(smc.get("ob_H4", [])) + "\n"
        "1H OB:\n" + ob_str(smc.get("ob_H1", [])) + "\n"
        "15M OB:\n" + ob_str(smc.get("ob_M15", [])) + "\n\n"
        "=== FAIR VALUE GAPS ===\n"
        "4H FVG:\n" + fvg_str(smc.get("fvg_H4", [])) + "\n"
        "1H FVG:\n" + fvg_str(smc.get("fvg_H1", [])) + "\n"
        "15M FVG:\n" + fvg_str(smc.get("fvg_M15", [])) + "\n\n"
        "=== ЛІКВІДНІСТЬ ===\n"
        "1H:\n" + liq_str(smc.get("liquidity_H1", {})) + "\n"
        "15M:\n" + liq_str(smc.get("liquidity_M15", {})) + "\n\n"
        "=== СВІПИ ЛІКВІДНОСТІ ===\n"
        "4H: " + sweep_str(smc.get("sweep_H4")) + "\n"
        "1H: " + sweep_str(smc.get("sweep_H1")) + "\n"
        "15M: " + sweep_str(smc.get("sweep_M15")) + "\n\n"
        "=== PREMIUM / DISCOUNT ===\n"
        "4H: " + pd_str(smc.get("pd_zone_H4", {})) + "\n"
        "1H: " + pd_str(smc.get("pd_zone_H1", {})) + "\n\n"
        "=== ЯКІСТЬ СЕТАПУ ===\n"
        "Score: " + str(score) + "/5\n"
        "Has Setup: " + has_setup + "\n\n"
        "ПРАВИЛА:\n"
        "- Топ-даун: Monthly -> Weekly -> Daily -> 4H -> 1H -> 15M\n"
        "- RR мінімум 1:2, ціль 1:3+\n"
        "- Ризик на угоду: 0.5-1% від депозиту\n"
        "- Ціль: prop challenge +8%\n\n"
        "Дай відповідь УКРАЇНСЬКОЮ у форматі Telegram (емодзі + Markdown):\n"
        "1. BIAS (загальний напрям)\n"
        "2. ПОТОЧНА СИТУАЦІЯ\n"
        "3. СЦЕНАРІЙ BUY: зона входу, SL, TP1, TP2, RR\n"
        "4. СЦЕНАРІЙ SELL: зона входу, SL, TP1, TP2, RR\n"
        "5. ЩО ЧЕКАТИ (тригер на 15M)\n"
        "6. ВИСНОВОК\n\n"
        "Будь конкретним — давай точні рівні цін."
    )
    return prompt


async def get_ai_analysis(
    instrument: str,
    smc_data: dict,
    session_info: dict,
    api_key: str,
    alert_mode: bool = False
) -> str:
    prompt = format_smc_for_prompt(instrument, smc_data, session_info)

    if alert_mode:
        prompt += "\n\nВАЖЛИВО: Це АВТОМАТИЧНИЙ АЛЕРТ. Починай з 'ALERT:' і будь коротким — тільки вхід/SL/TP."

    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }

    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1500,
        "messages": [{"role": "user", "content": prompt}],
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            ANTHROPIC_API,
            headers=headers,
            json=body,
            timeout=aiohttp.ClientTimeout(total=45)
        ) as resp:
            if resp.status != 200:
                err = await resp.text()
                raise Exception("Anthropic API error " + str(resp.status) + ": " + err[:200])
            data = await resp.json()
            text = "".join(b.get("text", "") for b in data.get("content", []))

    emoji_map = {"EUR_USD": "EU", "GBP_USD": "GB", "XAU_USD": "GOLD", "BTC_USD": "BTC"}
    emoji = emoji_map.get(instrument, "")
    display = instrument.replace("_", "/")
    price = smc_data.get("current_price", 0)
    score = smc_data.get("setup_quality", 0)
    stars = "★" * score + "☆" * (5 - score)

    header = (
        "*" + emoji + " " + display + "* | `" + "{:.5f}".format(price) + "`\n"
        + session_info["name"] + " " + session_info["emoji"] + " | " + stars + "\n"
        + "─" * 30 + "\n\n"
    )

    return header + text
