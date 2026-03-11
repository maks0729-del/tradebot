# SMC Trading Bot 🤖

Telegram бот для аналізу валютного ринку за концепцією Smart Money (SMC).

## Що аналізує:
- **BOS / CHoCH** — структура ринку на всіх TF
- **Order Blocks** — 4H, 1H, 15M
- **Fair Value Gaps** — імбаланси
- **Liquidity Sweeps** — свіпи BSL/SSL
- **Premium / Discount zones**
- **PDH/PDL/PWH/PWL/PMH/PML** — ключові рівні
- **Торгові сесії** — Азія / Лондон / Нью-Йорк

## Команди:
- `/analyze XAUUSD` — повний топ-даун аналіз
- `/alerts on` — автоматичні алерти кожні 15 хв
- `/alerts off` — вимкнути алерти
- `/status` — статус бота

## Змінні середовища (Variables на Railway):

| Назва | Де взяти |
|-------|---------|
| `TELEGRAM_TOKEN` | @BotFather → /newbot |
| `ANTHROPIC_API_KEY` | console.anthropic.com → API Keys |
| `OANDA_API_KEY` | oanda.com → My Services → API |
| `OANDA_ACCOUNT_ID` | oanda.com → My Account (необов'язково) |

## Деплой на Railway:
1. Завантаж всі файли на GitHub
2. railway.app → New Project → Deploy from GitHub
3. Variables → додай всі 4 змінні
4. Deploy

⚠️ *Не фінансова порада. DYOR.*
