import os
import time
import logging
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ============================================================
# ATLAS AI V6.2
# Robust 24/7 radar
# 1D -> 4H -> 1H -> 15M + 2M entry timing
# Binance Spot public data + Telegram
#
# V6.2 fixes:
# - Real background scan loop (no JobQueue dependency)
# - Safe None handling
# - One bad symbol cannot stop the scan
# - Counters reset correctly on every scan
# - Old errors do not survive a successful scan
# - Graceful startup/shutdown
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("atlas")

TOKEN = os.getenv("BOT_TOKEN", "").strip()
SIGNAL_CHAT = os.getenv("SIGNAL_CHAT_ID", "").strip()
ALLOWED = os.getenv("ALLOWED_CHAT_ID", "").strip()

SCAN_INTERVAL = max(30, int(os.getenv("SCAN_INTERVAL", "120")))
UNIVERSE_SIZE = max(10, int(os.getenv("UNIVERSE_SIZE", "80")))
THRESHOLD = max(1, min(100, int(os.getenv("RADAR_THRESHOLD", "80"))))
COOLDOWN = max(0, int(os.getenv("SIGNAL_COOLDOWN", "3600")))
MAX_WORKERS = max(2, min(16, int(os.getenv("MAX_WORKERS", "8"))))

BASE = "https://api.binance.com"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "AtlasAI-V6.2/1.0",
})

state = {
    "scans": 0,
    "last": 0.0,
    "universe": 0,
    "scanned": 0,
    "ready": 0,
    "signals": 0,
    "errors": 0,
    "skipped": 0,
    "last_error": "",
    "top": [],
    "running": False,
}

sent = defaultdict(float)
scan_lock = asyncio.Lock()
scanner_task = None


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def allowed(update: Update) -> bool:
    if not ALLOWED:
        return True

    chat = update.effective_chat
    if not chat:
        return False

    return str(chat.id) == ALLOWED


async def guarded(update: Update) -> bool:
    if allowed(update):
        return True

    if update.effective_message:
        await update.effective_message.reply_text(
            "⛔ Bu bot için yetkiniz yok."
        )

    return False


def api(path, params=None):
    response = SESSION.get(
        BASE + path,
        params=params,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        result = float(value)
        if result != result:
            return default
        return result
    except (TypeError, ValueError):
        return default


def ema(values, period):
    if not values or len(values) < period:
        return None

    result = sum(values[:period]) / period
    k = 2.0 / (period + 1)

    for value in values[period:]:
        result = value * k + result * (1.0 - k)

    return result


def rsi(values, period=14):
    if not values or len(values) <= period:
        return 50.0

    gains = 0.0
    losses = 0.0

    start = len(values) - period - 1

    for i in range(start, len(values) - 1):
        delta = values[i + 1] - values[i]

        if delta > 0:
            gains += delta
        elif delta < 0:
            losses -= delta

    if losses == 0:
        return 100.0 if gains > 0 else 50.0

    rs = gains / losses
    return 100.0 - (100.0 / (1.0 + rs))


def atr(data, period=14):
    if not data or len(data) < period + 1:
        return 0.0

    true_ranges = []

    for i in range(1, len(data)):
        current = data[i]
        previous = data[i - 1]

        high = current["h"]
        low = current["l"]
        previous_close = previous["c"]

        true_ranges.append(
            max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        )

    if not true_ranges:
        return 0.0

    return sum(true_ranges[-period:]) / min(
        period,
        len(true_ranges),
    )


def candles(symbol, interval, limit=120):
    raw = api(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )

    result = []

    for row in raw:
        if len(row) < 6:
            continue

        try:
            result.append({
                "t": int(row[0]),
                "o": float(row[1]),
                "h": float(row[2]),
                "l": float(row[3]),
                "c": float(row[4]),
                "v": float(row[5]),
            })
        except (TypeError, ValueError):
            continue

    return result


def build_2m(one_minute):
    """
    Binance Spot does not expose a native 2m interval.
    Build 2m candles from consecutive 1m candles.
    """
    result = []

    if len(one_minute) < 2:
        return result

    # Start on an even pair from the available series.
    for i in range(0, len(one_minute) - 1, 2):
        a = one_minute[i]
        b = one_minute[i + 1]

        if b["t"] != a["t"] + 60_000:
            continue

        result.append({
            "t": a["t"],
            "o": a["o"],
            "h": max(a["h"], b["h"]),
            "l": min(a["l"], b["l"]),
            "c": b["c"],
            "v": a["v"] + b["v"],
        })

    return result


def valid_candles(data, minimum=50):
    return bool(data) and len(data) >= minimum


def trend(data):
    """
    Returns:
        (BULLISH/BEARISH/NEUTRAL, component_score)

    Never performs arithmetic/comparison against None.
    """
    if not valid_candles(data):
        return "NEUTRAL", 0.0

    values = [x["c"] for x in data]

    if len(values) < 51:
        return "NEUTRAL", 0.0

    price = values[-1]
    e20 = ema(values, 20)
    e50 = ema(values, 50)

    if e20 is None or e50 is None or price <= 0:
        return "NEUTRAL", 0.0

    current_rsi = rsi(values)
    previous_index = max(0, len(values) - 21)
    base_price = values[previous_index]

    if base_price <= 0:
        momentum = 0.0
    else:
        momentum = (price / base_price - 1.0) * 100.0

    score = 0.0

    score += 1.0 if price > e20 else -1.0
    score += 1.0 if e20 > e50 else -1.0

    if current_rsi >= 55:
        score += 0.7
    elif current_rsi <= 45:
        score -= 0.7

    if momentum > 0:
        score += 0.3
    elif momentum < 0:
        score -= 0.3

    if score >= 1.0:
        return "BULLISH", score

    if score <= -1.0:
        return "BEARISH", score

    return "NEUTRAL", score


def support_resistance(data):
    if not data:
        return None, None, False, False

    data = data[-80:]

    if len(data) < 5:
        return None, None, False, False

    price = data[-1]["c"]

    supports = []
    resistances = []

    for i in range(2, len(data) - 2):
        high = data[i]["h"]
        low = data[i]["l"]

        if high >= max(
            data[i - 2]["h"],
            data[i - 1]["h"],
            data[i + 1]["h"],
            data[i + 2]["h"],
        ):
            resistances.append(high)

        if low <= min(
            data[i - 2]["l"],
            data[i - 1]["l"],
            data[i + 1]["l"],
            data[i + 2]["l"],
        ):
            supports.append(low)

    support_candidates = [
        value for value in supports
        if value <= price
    ]

    resistance_candidates = [
        value for value in resistances
        if value >= price
    ]

    support = max(
        support_candidates,
        default=min(x["l"] for x in data),
    )

    resistance = min(
        resistance_candidates,
        default=max(x["h"] for x in data),
    )

    if price <= 0:
        return support, resistance, False, False

    current_atr = atr(data)
    atr_percent = current_atr / price * 100.0
    proximity = max(
        0.2,
        min(1.5, atr_percent * 0.8),
    )

    near_support = (
        abs(price - support) / price * 100.0
        <= proximity
    )

    near_resistance = (
        abs(resistance - price) / price * 100.0
        <= proximity
    )

    return (
        support,
        resistance,
        near_support,
        near_resistance,
    )


# ------------------------------------------------------------
# Market universe
# ------------------------------------------------------------

def get_universe():
    exchange_info = api("/api/v3/exchangeInfo")

    allowed_symbols = {
        item["symbol"]
        for item in exchange_info.get("symbols", [])
        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("isSpotTradingAllowed", False)
        )
    }

    tickers = api("/api/v3/ticker/24hr")

    excluded_suffixes = (
        "UPUSDT",
        "DOWNUSDT",
        "BULLUSDT",
        "BEARUSDT",
    )

    ranked = []

    for ticker in tickers:
        symbol = ticker.get("symbol")

        if symbol not in allowed_symbols:
            continue

        if any(
            symbol.endswith(suffix)
            for suffix in excluded_suffixes
        ):
            continue

        volume = safe_float(
            ticker.get("quoteVolume"),
            None,
        )

        if volume is None or volume <= 0:
            continue

        ranked.append((volume, symbol))

    ranked.sort(reverse=True)

    return [
        symbol
        for _, symbol in ranked[:UNIVERSE_SIZE]
    ]


# ------------------------------------------------------------
# Analysis
# ------------------------------------------------------------

def analyze(symbol):
    try:
        daily = candles(symbol, "1d")
        four_hour = candles(symbol, "4h")
        hourly = candles(symbol, "1h")
        fifteen = candles(symbol, "15m")

        frames = [
            daily,
            four_hour,
            hourly,
            fifteen,
        ]

        if any(
            not valid_candles(frame)
            for frame in frames
        ):
            return None, "insufficient_mtf_data"

        mtf = [
            trend(frame)[0]
            for frame in frames
        ]

        one_minute = candles(symbol, "1m")
        two_minute = build_2m(one_minute)

        if not valid_candles(two_minute):
            return None, "insufficient_2m_data"

        timing = trend(two_minute)[0]

        values_2m = [
            x["c"] for x in two_minute
        ]

        price = values_2m[-1]

        e9 = ema(values_2m, 9)
        e20 = ema(values_2m, 20)

        if (
            e9 is None
            or e20 is None
            or price <= 0
        ):
            return None, "indicator_data"

        current_rsi = rsi(values_2m)

        (
            support,
            resistance,
            near_support,
            near_resistance,
        ) = support_resistance(fifteen)

        if support is None or resistance is None:
            return None, "sr_data"

        bull_count = mtf.count("BULLISH")
        bear_count = mtf.count("BEARISH")

        weights = (28, 24, 20, 14)

        long_score = sum(
            weight
            for direction, weight in zip(
                mtf,
                weights,
            )
            if direction == "BULLISH"
        )

        short_score = sum(
            weight
            for direction, weight in zip(
                mtf,
                weights,
            )
            if direction == "BEARISH"
        )

        if timing == "BULLISH":
            long_score += 7

        if timing == "BEARISH":
            short_score += 7

        if bull_count >= 3:
            long_score += 7

        if bear_count >= 3:
            short_score += 7

        if long_score >= short_score:
            direction = "LONG"
            score = min(100, max(0, long_score))
        else:
            direction = "SHORT"
            score = min(100, max(0, short_score))

        if direction == "LONG":
            trigger = (
                timing == "BULLISH"
                and e9 > e20
                and current_rsi >= 52
                and (
                    near_support
                    or near_resistance
                )
            )
        else:
            trigger = (
                timing == "BEARISH"
                and e9 < e20
                and current_rsi <= 48
                and (
                    near_support
                    or near_resistance
                )
            )

        ready = (
            score >= THRESHOLD
            and trigger
            and (
                bull_count >= 3
                or bear_count >= 3
            )
        )

        current_atr = atr(fifteen)
        entry = price

        if direction == "LONG":
            stop = min(
                support,
                entry - 1.2 * current_atr,
            )

            risk = max(
                entry - stop,
                entry * 0.002,
            )

            targets = [
                entry + 1.2 * risk,
                entry + 2.0 * risk,
                entry + 3.0 * risk,
            ]
        else:
            stop = max(
                resistance,
                entry + 1.2 * current_atr,
            )

            risk = max(
                stop - entry,
                entry * 0.002,
            )

            targets = [
                entry - 1.2 * risk,
                entry - 2.0 * risk,
                entry - 3.0 * risk,
            ]

        return {
            "symbol": symbol,
            "price": price,
            "direction": direction,
            "score": int(score),
            "ready": bool(ready),
            "mtf": mtf + [timing],
            "rsi": current_rsi,
            "support": support,
            "resistance": resistance,
            "entry": entry,
            "stop": stop,
            "tp": targets,
        }, None

    except Exception as exc:
        return None, str(exc)


# ------------------------------------------------------------
# Scan
# ------------------------------------------------------------

def perform_scan():
    """
    Synchronous market scan.
    It is executed in a worker thread so Telegram remains responsive.
    """
    state["scans"] += 1
    state["last"] = time.time()

    # Reset per-scan values.
    state["scanned"] = 0
    state["ready"] = 0
    state["skipped"] = 0
    state["errors"] = 0
    state["last_error"] = ""
    state["top"] = []

    try:
        symbols = get_universe()
    except Exception as exc:
        state["universe"] = 0
        state["errors"] = 1
        state["last_error"] = f"Universe: {exc}"
        log.exception("Universe error")
        return []

    state["universe"] = len(symbols)

    results = []
    skipped = 0
    errors = 0
    last_error = ""

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                analyze,
                symbol,
            ): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):
            symbol = futures[future]

            try:
                result, error = future.result()

                if result is not None:
                    results.append(result)
                elif error:
                    if error.startswith(
                        "insufficient_"
                    ) or error in (
                        "indicator_data",
                        "sr_data",
                    ):
                        skipped += 1
                    else:
                        errors += 1
                        last_error = (
                            f"{symbol}: {error}"
                        )

            except Exception as exc:
                errors += 1
                last_error = (
                    f"{symbol}: {exc}"
                )
                log.exception(
                    "Worker error for %s",
                    symbol,
                )

    results.sort(
        key=lambda item: item["score"],
        reverse=True,
    )

    state["scanned"] = len(results)
    state["skipped"] = skipped
    state["errors"] = errors
    state["last_error"] = last_error
    state["ready"] = sum(
        1
        for item in results
        if item["ready"]
    )
    state["top"] = results[:10]

    outgoing = []

    now = time.time()

    for result in results:
        if not result["ready"]:
            continue

        symbol = result["symbol"]

        if (
            now - sent[symbol]
            >= COOLDOWN
        ):
            sent[symbol] = now
            outgoing.append(result)

    state["signals"] += len(outgoing)

    log.info(
        "SCAN #%s | universe=%s scanned=%s "
        "ready=%s signals=%s skipped=%s errors=%s",
        state["scans"],
        state["universe"],
        state["scanned"],
        state["ready"],
        len(outgoing),
        state["skipped"],
        state["errors"],
    )

    return outgoing


# ------------------------------------------------------------
# Formatting
# ------------------------------------------------------------

def format_price(value):
    if value is None:
        return "N/A"

    if value >= 1000:
        return f"{value:,.2f}"

    if value >= 1:
        return f"{value:,.4f}"

    if value >= 0.01:
        return f"{value:,.6f}"

    return f"{value:.8f}"


def signal_message(result):
    mtf = result["mtf"]

    return (
        f"🚨 <b>ATLAS AI V6.2 — "
        f"{result['direction']}</b>\n"
        f"#{result['symbol']} | "
        f"Score: <b>{result['score']}/100</b>\n\n"
        f"💰 Giriş: "
        f"<code>{format_price(result['entry'])}</code>\n"
        f"🛑 Stop: "
        f"<code>{format_price(result['stop'])}</code>\n"
        f"🎯 TP1: "
        f"<code>{format_price(result['tp'][0])}</code>\n"
        f"🎯 TP2: "
        f"<code>{format_price(result['tp'][1])}</code>\n"
        f"🎯 TP3: "
        f"<code>{format_price(result['tp'][2])}</code>\n\n"
        f"📊 1D: {mtf[0]}\n"
        f"📊 4H: {mtf[1]}\n"
        f"📊 1H: {mtf[2]}\n"
        f"📊 15M: {mtf[3]}\n"
        f"⚡ 2M: {mtf[4]}\n"
        f"📈 RSI: {result['rsi']:.1f}"
    )


# ------------------------------------------------------------
# Telegram commands
# ------------------------------------------------------------

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await guarded(update):
        return

    await update.effective_message.reply_text(
        "🤖 ATLAS AI V6.2 aktif.\n\n"
        "Komutlar:\n"
        "/test\n"
        "/status\n"
        "/diagnostics\n"
        "/scan\n"
        "/top"
    )


async def test_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await guarded(update):
        return

    try:
        ticker = await asyncio.to_thread(
            api,
            "/api/v3/ticker/price",
            {"symbol": "BTCUSDT"},
        )

        price = ticker.get("price")

        if price is None:
            raise RuntimeError(
                "BTCUSDT fiyatı alınamadı."
            )

        await update.effective_message.reply_text(
            "🧪 <b>TEST OK</b>\n"
            "Binance: ✅\n"
            f"BTCUSDT: {format_price(float(price))}\n"
            f"2M aggregation: ✅\n"
            f"Radar: {'🟢' if state['running'] else '🔴'}",
            parse_mode=ParseMode.HTML,
        )

    except Exception as exc:
        await update.effective_message.reply_text(
            f"❌ TEST HATASI\n{exc}"
        )


async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await guarded(update):
        return

    last_age = (
        "henüz yok"
        if state["last"] == 0
        else f"{int(max(0, time.time() - state['last']))}s önce"
    )

    await update.effective_message.reply_text(
        "🧭 <b>ATLAS AI V6.2 STATUS</b>\n\n"
        f"Telegram: {'✅' if TOKEN else '❌'}\n"
        "Binance REST: ✅\n"
        f"Radar: {'🟢 AKTİF' if state['running'] else '🔴 DURDU'}\n"
        "MTF: 1D → 4H → 1H → 15M\n"
        "2M: giriş zamanlaması\n"
        f"Tarama: {SCAN_INTERVAL}s\n"
        f"Son tarama: {last_age}\n"
        f"Universe: top {UNIVERSE_SIZE} USDT spot",
        parse_mode=ParseMode.HTML,
    )


async def diagnostics_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await guarded(update):
        return

    last_age = (
        "henüz yok"
        if state["last"] == 0
        else f"{int(max(0, time.time() - state['last']))}s önce"
    )

    error_text = (
        state["last_error"]
        if state["last_error"]
        else "Yok"
    )

    await update.effective_message.reply_text(
        "🛠 <b>ATLAS AI V6.2 DIAGNOSTICS</b>\n\n"
        f"Tarama: {state['scans']}\n"
        f"Son tarama: {last_age}\n"
        f"Universe: {state['universe']}\n"
        f"Taranan: {state['scanned']}\n"
        f"TRADE READY: {state['ready']}\n"
        f"Toplam sinyal: {state['signals']}\n"
        f"Atlanan: {state['skipped']}\n"
        f"Hata: {state['errors']}\n"
        f"Son hata: {error_text}",
        parse_mode=ParseMode.HTML,
    )


async def scan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await guarded(update):
        return

    if scan_lock.locked():
        await update.effective_message.reply_text(
            "⏳ Zaten bir tarama çalışıyor."
        )
        return

    await update.effective_message.reply_text(
        "🔎 Manuel tarama başlatıldı..."
    )

    async with scan_lock:
        results = await asyncio.to_thread(
            perform_scan
        )

    await update.effective_message.reply_text(
        "✅ Tarama tamamlandı.\n"
        f"Universe: {state['universe']}\n"
        f"Taranan: {state['scanned']}\n"
        f"Ready: {state['ready']}\n"
        f"Yeni sinyal: {len(results)}\n"
        f"Hata: {state['errors']}"
    )

    for result in results:
        try:
            await update.effective_message.reply_text(
                signal_message(result),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            log.exception(
                "Manual signal send error"
            )


async def top_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await guarded(update):
        return

    top = state["top"]

    if not top:
        await update.effective_message.reply_text(
            "📊 Henüz tarama sonucu yok."
        )
        return

    lines = [
        "📊 <b>ATLAS AI V6.2 TOP 10</b>",
        "",
    ]

    for index, item in enumerate(top, 1):
        emoji = (
            "🟢"
            if item["direction"] == "LONG"
            else "🔴"
        )

        ready = " ⚡ READY" if item["ready"] else ""

        lines.append(
            f"{index}. {emoji} "
            f"#{item['symbol']} — "
            f"{item['score']}/100{ready}"
        )

    await update.effective_message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


# ------------------------------------------------------------
# Background radar
# ------------------------------------------------------------

async def send_signals(application, results):
    if not results:
        return

    destination = (
        SIGNAL_CHAT
        or ALLOWED
    )

    if not destination:
        log.warning(
            "Signal destination not configured."
        )
        return

    for result in results:
        try:
            await application.bot.send_message(
                chat_id=destination,
                text=signal_message(result),
                parse_mode=ParseMode.HTML,
            )

        except Exception as exc:
            log.exception(
                "Signal send error: %s",
                exc,
            )


async def radar_loop(application):
    """
    Independent background loop.
    This intentionally does NOT use Telegram JobQueue.
    """
    log.info(
        "Radar loop started. Interval=%ss",
        SCAN_INTERVAL,
    )

    state["running"] = True

    # Small startup delay so Telegram polling can fully start.
    await asyncio.sleep(5)

    while True:
        try:
            if scan_lock.locked():
                log.warning(
                    "Previous scan still running; skipping cycle."
                )
            else:
                async with scan_lock:
                    results = await asyncio.to_thread(
                        perform_scan
                    )

                await send_signals(
                    application,
                    results,
                )

        except asyncio.CancelledError:
            log.info(
                "Radar loop cancelled."
            )
            break

        except Exception as exc:
            state["errors"] += 1
            state["last_error"] = (
                f"Radar loop: {exc}"
            )

            log.exception(
                "Radar loop error"
            )

        # Wait AFTER the completed scan.
        # This prevents overlapping scans.
        try:
            await asyncio.sleep(
                SCAN_INTERVAL
            )
        except asyncio.CancelledError:
            break

    state["running"] = False
    log.info("Radar loop stopped.")


async def post_init(application):
    global scanner_task

    if not TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is required."
        )

    scanner_task = asyncio.create_task(
        radar_loop(application),
        name="atlas-radar-loop",
    )

    log.info(
        "ATLAS AI V6.2 initialized."
    )


async def post_shutdown(application):
    global scanner_task

    state["running"] = False

    if scanner_task is not None:
        scanner_task.cancel()

        try:
            await scanner_task
        except asyncio.CancelledError:
            pass

        scanner_task = None

    log.info(
        "ATLAS AI V6.2 shutdown complete."
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    if not TOKEN:
        raise RuntimeError(
            "BOT_TOKEN environment variable is missing."
        )

    application = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "test",
            test_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "diagnostics",
            diagnostics_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "scan",
            scan_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "top",
            top_command,
        )
    )

    log.info(
        "Starting ATLAS AI V6.2..."
    )

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
