import os
import asyncio
import logging
from datetime import datetime, timezone
import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

from data import fetch_candles, get_session_info
from smc import analyze_smc
from ai import get_ai_analysis

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
TWELVEDATA_API_KEY = os.environ["TWELVEDATA_API_KEY"]


# Instruments to monitor
INSTRUMENTS = ["EUR_USD", "GBP_USD", "XAU_USD", "BTC_USD"]

# Store user chat IDs for alerts
ALERT_USERS = set()
ALERT_TASK = None


# ── HELPERS ──────────────────────────────────────────────────────────────────

def instrument_display(inst: str) -> str:
    return inst.replace("_", "/")

def instrument_emoji(inst: str) -> str:
    return {"EUR_USD": "🇪🇺", "GBP_USD": "🇬🇧", "XAU_USD": "🥇", "BTC_USD": "₿"}.get(inst, "📊")


# ── COMMANDS ─────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *SMC Trading Bot*\n\n"
        "Аналізую ринок за концепцією Smart Money:\n"
        "BOS/CHoCH · OB · FVG · Liquidity Sweeps · Sessions\n\n"
        "*Команди:*\n"
        "`/analyze EURUSD` — повний топ-даун аналіз\n"
        "`/analyze GBPUSD`\n"
        "`/analyze XAUUSD`\n"
        "`/analyze BTCUSD`\n\n"
        "`/alerts on` — увімкнути автоматичні алерти\n"
        "`/alerts off` — вимкнути алерти\n"
        "`/status` — статус бота\n\n"
        "_RR мінімум 1:2 · Ризик 0.5-1% · Prop challenge mode_ 🎯"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "❓ Вкажи інструмент:\n`/analyze XAUUSD`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    raw = args[0].upper().replace("/", "_")
    # Normalize
    aliases = {
        "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD",
        "XAUUSD": "XAU_USD", "BTCUSD": "BTC_USD",
        "GOLD": "XAU_USD", "BTC": "BTC_USD",
        "EUR_USD": "EUR_USD", "GBP_USD": "GBP_USD",
        "XAU_USD": "XAU_USD", "BTC_USD": "BTC_USD",
    }
    instrument = aliases.get(raw)
    if not instrument:
        await update.message.reply_text(
            "❌ Невідомий інструмент. Доступні: EURUSD, GBPUSD, XAUUSD, BTCUSD"
        )
        return

    emoji = instrument_emoji(instrument)
    display = instrument_display(instrument)

    msg = await update.message.reply_text(
        f"{emoji} *{display}* — аналізую...\n⏳ Завантажую дані по всіх таймфреймах",
        parse_mode=ParseMode.MARKDOWN
    )

    try:
        await msg.edit_text(
            f"{emoji} *{display}* — аналізую...\n⏳ Збираю свічки (1M/1W/1D/4H/1H/15M)",
            parse_mode=ParseMode.MARKDOWN
        )

        # Fetch candles for all timeframes
        candles = await fetch_candles(instrument, TWELVEDATA_API_KEY)

        await msg.edit_text(
            f"{emoji} *{display}* — аналізую...\n⏳ Обчислюю SMC структуру",
            parse_mode=ParseMode.MARKDOWN
        )

        # SMC analysis
        smc_data = analyze_smc(candles)
        session_info = get_session_info()

        await msg.edit_text(
            f"{emoji} *{display}* — аналізую...\n⏳ AI генерує топ-даун аналіз",
            parse_mode=ParseMode.MARKDOWN
        )

        # AI analysis
        analysis = await get_ai_analysis(instrument, smc_data, session_info, ANTHROPIC_API_KEY)

        await msg.edit_text(analysis, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Analysis error for {instrument}: {e}", exc_info=True)
        await msg.edit_text(f"❌ Помилка аналізу {display}:\n`{str(e)[:200]}`", parse_mode=ParseMode.MARKDOWN)


async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ALERT_TASK
    chat_id = update.effective_chat.id
    args = context.args

    if not args or args[0].lower() not in ("on", "off"):
        await update.message.reply_text("Usage: `/alerts on` або `/alerts off`", parse_mode=ParseMode.MARKDOWN)
        return

    if args[0].lower() == "on":
        ALERT_USERS.add(chat_id)
        await update.message.reply_text(
            "✅ *Алерти увімкнено!*\n\n"
            "Бот перевіряє ринок кожні 15 хвилин.\n"
            "Отримаєш повідомлення коли знайдеться якісний сетап:\n"
            "• Sweep ліквідності\n"
            "• Вхід в OB/FVG зону\n"
            "• BOS/CHoCH на старшому TF\n\n"
            "_ПК можна вимикати — бот працює в хмарі 24/7_ ☁️",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        ALERT_USERS.discard(chat_id)
        await update.message.reply_text("🔕 Алерти вимкнено.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    alert_status = "✅ Увімкнені" if chat_id in ALERT_USERS else "🔕 Вимкнені"
    now = datetime.now(timezone.utc)
    session = get_session_info()

    text = (
        f"🤖 *SMC Bot Status*\n\n"
        f"⏰ UTC: `{now.strftime('%H:%M %d.%m.%Y')}`\n"
        f"📍 Сесія: *{session['name']}* {session['emoji']}\n"
        f"🔔 Алерти: {alert_status}\n"
        f"👥 Активних користувачів: {len(ALERT_USERS)}\n\n"
        f"📊 Інструменти: EUR/USD · GBP/USD · XAU/USD · BTC/USD"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ── ALERT LOOP ────────────────────────────────────────────────────────────────

async def alert_loop(app: Application):
    """Runs every 15 minutes, scans all instruments, sends alerts for quality setups."""
    await asyncio.sleep(30)  # Initial delay on startup

    while True:
        try:
            if ALERT_USERS:
                logger.info(f"Alert scan started for {len(ALERT_USERS)} users")

                for instrument in INSTRUMENTS:
                    try:
                        candles = await fetch_candles(instrument, TWELVEDATA_API_KEY)
                        smc_data = analyze_smc(candles)

                        # Only alert if there's a quality setup
                        if smc_data.get("has_setup") and smc_data.get("setup_quality", 0) >= 2:
                            session_info = get_session_info()
                            analysis = await get_ai_analysis(
                                instrument, smc_data, session_info,
                                ANTHROPIC_API_KEY, alert_mode=True
                            )

                            for chat_id in list(ALERT_USERS):
                                try:
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text=analysis,
                                        parse_mode=ParseMode.MARKDOWN,
                                        disable_web_page_preview=True
                                    )
                                except Exception as e:
                                    logger.error(f"Failed to send alert to {chat_id}: {e}")

                        await asyncio.sleep(2)  # Small delay between instruments

                    except Exception as e:
                        logger.error(f"Alert scan error for {instrument}: {e}")

        except Exception as e:
            logger.error(f"Alert loop error: {e}")

        # Wait 15 minutes
        await asyncio.sleep(15 * 60)


# ── MAIN ──────────────────────────────────────────────────────────────────────

async def post_init(app: Application):
    asyncio.create_task(alert_loop(app))


def main():
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("analyze", cmd_analyze))
    app.add_handler(CommandHandler("alerts", cmd_alerts))
    app.add_handler(CommandHandler("status", cmd_status))

    logger.info("SMC Trading Bot started...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
