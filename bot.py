import os
import time
import logging
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ============================================================
# ATLAS AI V6.1
# Robust radar: 1D -> 4H -> 1H -> 15M, 2M entry timing
# Binance public market data + Telegram
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("atlas")

TOKEN = os.getenv("BOT_TOKEN", "").strip()
SIGNAL_CHAT = os.getenv("SIGNAL_CHAT_ID", "").strip()
ALLOWED = os.getenv("ALLOWED_CHAT_ID", "").strip()

INTERVAL = int(os.getenv("SCAN_INTERVAL", "120"))
UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "80"))
THRESHOLD = int(os.getenv("RADAR_THRESHOLD", "80"))
COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", "3600"))

BASE = "https://api.binance.com"
S = requests.Session()
S.headers["User-Agent"] = "AtlasAI-V6.1/1.0"

state = {
    "scans": 0,
    "last": 0,
    "universe": 0,
    "scanned": 0,
    "ready": 0,
    "signals": 0,
    "errors": 0,
    "skipped": 0,
    "last_error": "",
    "top": [],
}
sent = defaultdict(float)


def ok(update: Update) -> bool:
    return not ALLOWED or str(update.effective_chat.id) == ALLOWED


def api(path, params=None):
    r = S.get(BASE + path, params=params, timeout=12)
    r.raise_for_status()
    return r.json()


def ema(values, n):
    if not values or len(values) < n:
        return None

    x = sum(values[:n]) / n
    k = 2 / (n + 1)

    for v in values[n:]:
        x = v * k + x * (1 - k)

    return x


def rsi(values, n=14):
    if len(values) <= n:
        return 50.0

    gains = 0.0
    losses = 0.0

    for x, y in zip(values[-n - 1:-1], values[-n:]):
        d = y - x
        gains += max(d, 0)
        losses += max(-d, 0)

    if losses == 0:
        return 100.0

    return 100.0 - 100.0 / (1.0 + gains / losses)


def atr(candles_data, n=14):
    if len(candles_data) < n + 1:
        return 0.0

    tr = []

    for i in range(1, len(candles_data)):
        cur = candles_data[i]
        prev = candles_data[i - 1]

        tr.append(
            max(
                cur["h"] - cur["l"],
                abs(cur["h"] - prev["c"]),
                abs(cur["l"] - prev["c"]),
            )
        )

    return sum(tr[-n:]) / n


def candles(sym, interval, limit=120):
    data = api(
        "/api/v3/klines",
        {
            "symbol": sym,
            "interval": interval,
            "limit": limit,
        },
    )

    return [
        {
            "t": x[0],
            "o": float(x[1]),
            "h": float(x[2]),
            "l": float(x[3]),
            "c": float(x[4]),
            "v": float(x[5]),
        }
        for x in data
    ]


def two_min(one_minute):
    """
    Binance Spot does not provide a native 2m interval.
    Build 2m candles from consecutive 1m candles.
    """
    out = []

    for i in range(0, len(one_minute) - 1, 2):
        a = one_minute[i]
        b = one_minute[i + 1]

        if b["t"] != a["t"] + 60000:
            continue

        out.append(
            {
                "t": a["t"],
                "o": a["o"],
                "h": max(a["h"], b["h"]),
                "l": min(a["l"], b["l"]),
                "c": b["c"],
                "v": a["v"] + b["v"],
            }
        )

    return out


def valid_candles(c):
    return bool(c) and len(c) >= 50


def trend(c):
    """
    Returns (direction, score_component).
    Never compares against None.
    """
    if not valid_candles(c):
        return "NEUTRAL", 0.0

    values = [x["c"] for x in c]
    price = values[-1]

    e20 = ema(values, 20)
    e50 = ema(values, 50)

    if e20 is None or e50 is None:
        return "NEUTRAL", 0.0

    q = rsi(values)
    momentum = (price / values[-21] - 1) * 100

    z = 0.0
    z += 1 if price > e20 else -1
    z += 1 if e20 > e50 else -1
    z += 0.7 if q >= 55 else -0.7 if q <= 45 else 0
    z += 0.3 if momentum > 0 else -0.3 if momentum < 0 else 0

    if z >= 1:
        return "BULLISH", z
    if z <= -1:
        return "BEARISH", z

    return "NEUTRAL", z


def sr(c):
    if not c:
        return None, None, False, False

    c = c[-80:]
    price = c[-1]["c"]

    supports = []
    resistances = []

    if len(c) < 5:
        return None, None, False, False

    for i in range(2, len(c) - 2):
        if c[i]["h"] >= max(
            c[i - 2]["h"],
            c[i - 1]["h"],
            c[i + 1]["h"],
            c[i + 2]["h"],
        ):
            resistances.append(c[i]["h"])

        if c[i]["l"] <= min(
            c[i - 2]["l"],
            c[i - 1]["l"],
            c[i + 1]["l"],
            c[i + 2]["l"],
        ):
            supports.append(c[i]["l"])

    support = max(
        [x for x in supports if x <= price],
        default=min(x["l"] for x in c),
    )

    resistance = min(
        [x for x in resistances if x >= price],
        default=max(x["h"] for x in c),
    )

    if price <= 0:
        return support, resistance, False, False

    ap = atr(c) / price * 100
    near = max(0.2, min(1.5, ap * 0.8))

    near_support = abs(price - support) / price * 100 <= near
    near_resistance = abs(resistance - price) / price * 100 <= near

    return support, resistance, near_support, near_resistance


def analyze(sym):
    """
    Analyze one symbol.
    Any bad/incomplete symbol is safely skipped instead of
    breaking the complete scan.
    """
    try:
        d1 = candles(sym, "1d", 120)
        d4 = candles(sym, "4h", 120)
        h1 = candles(sym, "1h", 120)
        m15 = candles(sym, "15m", 120)

        frames = [d1, d4, h1, m15]

        if any(not valid_candles(x) for x in frames):
            state["skipped"] += 1
            return None

        mtf = [trend(x)[0] for x in frames]

        m1 = candles(sym, "1m", 120)
        m2 = two_min(m1)

        if not valid_candles(m2):
            state["skipped"] += 1
            return None

        timing = trend(m2)[0]

        values2 = [x["c"] for x in m2]
        price = values2[-1]

        e9 = ema(values2, 9)
        e20 = ema(values2, 20)

        if e9 is None or e20 is None or price <= 0:
            state["skipped"] += 1
            return None

        q = rsi(values2)

        support, resistance, near_support, near_resistance = sr(m15)

        if support is None or resistance is None:
            state["skipped"] += 1
            return None

        bull = mtf.count("BULLISH")
        bear = mtf.count("BEARISH")

        long_score = sum(
            weight
            for direction, weight in zip(
                mtf,
                (28, 24, 20, 14),
            )
            if direction == "BULLISH"
        )

        short_score = sum(
            weight
            for direction, weight in zip(
                mtf,
                (28, 24, 20, 14),
            )
            if direction == "BEARISH"
        )

        if timing == "BULLISH":
            long_score += 7

        if timing == "BEARISH":
            short_score += 7

        if bull >= 3:
            long_score += 7

        if bear >= 3:
            short_score += 7

        direction = "LONG" if long_score >= short_score else "SHORT"
        score = min(100, max(long_score, short_score))

        if direction == "LONG":
            trigger = (
                timing == "BULLISH"
                and e9 > e20
                and q >= 52
                and (near_support or near_resistance)
            )
        else:
            trigger = (
                timing == "BEARISH"
                and e9 < e20
                and q <= 48
                and (near_support or near_resistance)
            )

        ready = (
            score >= THRESHOLD
            and trigger
            and (bull >= 3 or bear >= 3)
        )

        atr15 = atr(m15)
        entry = price

        if direction == "LONG":
            stop = min(
                support,
                entry - 1.2 * atr15,
            )
            risk = max(
                entry - stop,
                entry * 0.002,
            )
            tp = [
                entry + 1.2 * risk,
                entry + 2.0 * risk,
                entry + 3.0 * risk,
            ]
        else:
            stop = max(
                resistance,
                entry + 1.2 * atr15,
            )
            risk = max(
                stop - entry,
                entry * 0.002,
            )
            tp = [
                entry - 1.2 * risk,
                entry - 2.0 * risk,
                entry - 3.0 * risk,
            ]

        return {
            "symbol": sym,
            "price": price,
            "direction": direction,
            "score": int(score),
            "ready": ready,
            "mtf": mtf + [timing],
            "rsi": q,
            "support": support,
            "resistance": resistance,
            "entry": entry,
            "stop": stop,
            "tp": tp,
        }

    except Exception as exc:
        # A single broken symbol must never break the radar.
        state["errors"] += 1
        state["last_error"] = f"{sym}: {exc}"
        log.warning("Skipped %s: %s", sym, exc)
        return None


def universe():
    info = api("/api/v3/exchangeInfo")

    allowed = {
        x["symbol"]
        for x in info["symbols"]
        if (
            x["status"] == "TRADING"
            and x["quoteAsset"] == "USDT"
            and x.get("isSpotTradingAllowed", False)
        )
    }

    tickers = api("/api/v3/ticker/24hr")
    ranked = []

    for x in tickers:
        symbol = x["symbol"]

        if symbol not in allowed:
            continue

        if any(
            symbol.endswith(v)
            for v in (
                "UPUSDT",
                "DOWNUSDT",
                "BULLUSDT",
                "BEARUSDT",
            )
        ):
            continue

        try:
            ranked.append(
                (
                    float(x["quoteVolume"]),
                    symbol,
                )
            )
        except (TypeError, ValueError):
            continue

    ranked.sort(reverse=True)

    return [
        symbol
        for _, symbol in ranked[:UNIVERSE_SIZE]
    ]


def scan():
    state["scans"] += 1
    state["last"] = time.time()
    state["last_error"] = ""

    try:
        symbols = universe()
    except Exception as exc:
        state["errors"] += 1
        state["last_error"] = f"Universe: {exc}"
        log.exception("Universe error")
        return []

    state["universe"] = len(symbols)

    results = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(analyze, symbol)
            for symbol in symbols
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                state["errors"] += 1
                state["last_error"] = str(exc)
                log.exception("Worker error")

    results.sort(
        key=lambda x: x["score"],
        reverse=True,
    )

    state["scanned"] = len(results)
    state["ready"] = sum(
        1 for x in results if x["ready"]
    )
    state["top"] = results[:10]

    outgoing = []

    for result in results:
        if not result["ready"]:
            continue

        if (
            time.time() - sent[result["symbol"]]
            >= COOLDOWN
        ):
            sent[result["symbol"]] = time.time()
            outgoing.append(result)

    state["signals"] += len(outgoing)

    return outgoing


def fp(value):
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:,.4f}"
    if value >= 0.01:
        return f"{value:,.6f}"
    return f"{value:.8f}"


def signal_message(x):
    return f"""🚨 <b>ATLAS AI V6.1 — {x["direction"]}</b>
#{x["symbol"]} | Score: <b>{x["score"]}/100</b>

💰 Giriş: <code>{fp(x["entry"])}</code>
🛑 Stop: <code>{fp(x["stop"])}</code>
🎯 TP1: <code>{fp(x["tp"][0])}</code>
🎯 TP2: <code>{fp(x["tp"][1])}</code>
🎯 TP3: <code>{fp(x["tp"][2])}</code>

🧭 1D {x["mtf"][0]} → 4H {x["mtf"][1]} → 1H {x["mtf"][2]} → 15M {x["mtf"][3]}
⚡ 2M {x["mtf"][4]} | RSI {x["rsi"]:.1f}
📍 S {fp(x["support"])} | R {fp(x["resistance"])}

⚠️ Otomatik teknik analiz uyarısıdır; garanti kâr/satış tavsiyesi değildir."""


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ok(update):
        return

    await update.message.reply_text(
        "🧭 <b>ATLAS AI V6.1</b>\n\n"
        "1D → 4H → 1H → 15M = yön\n"
        "S/R = teyit\n"
        "2M = giriş zamanlaması\n\n"
        "/status /diagnostics /top /test",
        parse_mode="HTML",
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ok(update):
        return

    await update.message.reply_text(
        f"🧭 <b>ATLAS AI V6.1 STATUS</b>\n\n"
        "Telegram: ✅\n"
        "Binance REST: ✅\n"
        "MTF: 1D → 4H → 1H → 15M\n"
        "S/R: ACTIVE\n"
        "2M: giriş zamanlaması\n"
        f"Tarama: {INTERVAL}s\n"
        f"Universe: top {UNIVERSE_SIZE} USDT spot",
        parse_mode="HTML",
    )


async def diagnostics(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not ok(update):
        return

    if not state["last"]:
        age = "yok"
    else:
        age = f"{int(time.time() - state['last'])}s önce"

    await update.message.reply_text(
        "🔧 <b>ATLAS AI V6.1 DIAGNOSTICS</b>\n\n"
        f"Tarama: {state['scans']}\n"
        f"Son tarama: {age}\n"
        f"Universe: {state['universe']}\n"
        f"Taranan: {state['scanned']}\n"
        f"Atlanan: {state['skipped']}\n"
        f"TRADE READY: {state['ready']}\n"
        f"Sinyal: {state['signals']}\n"
        f"Hata: {state['errors']}\n"
        f"Son hata: {state['last_error'] or 'yok'}",
        parse_mode="HTML",
    )


async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ok(update):
        return

    if not state["top"]:
        await update.message.reply_text(
            "Henüz tarama sonucu yok."
        )
        return

    lines = []

    for i, x in enumerate(state["top"], 1):
        lines.append(
            f"{i}. {x['symbol']} — "
            f"{x['direction']} {x['score']}/100 | "
            f"1D {x['mtf'][0]} "
            f"4H {x['mtf'][1]} "
            f"1H {x['mtf'][2]} "
            f"15M {x['mtf'][3]} "
            f"2M {x['mtf'][4]}"
        )

    await update.message.reply_text(
        "🏆 <b>TOP RADAR</b>\n\n"
        + "\n".join(lines),
        parse_mode="HTML",
    )


async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ok(update):
        return

    try:
        one_min = candles(
            "BTCUSDT",
            "1m",
            4,
        )
        two = two_min(one_min)

        await update.message.reply_text(
            "🧪 <b>TEST OK</b>\n"
            "Binance: ✅\n"
            f"BTCUSDT 1m: {len(one_min)}\n"
            f"2m aggregation: "
            f"{'✅' if two else '⚠️'}",
            parse_mode="HTML",
        )

    except Exception as exc:
        await update.message.reply_text(
            f"❌ TEST ERROR\n{exc}"
        )


async def worker(app: Application):
    while True:
        started = time.time()

        try:
            signals = scan()

            if SIGNAL_CHAT:
                for signal in signals:
                    try:
                        await app.bot.send_message(
                            SIGNAL_CHAT,
                            signal_message(signal),
                            parse_mode="HTML",
                        )
                    except Exception as exc:
                        state["errors"] += 1
                        state["last_error"] = (
                            f"Telegram: {exc}"
                        )
                        log.exception(
                            "Telegram send error"
                        )

        except Exception as exc:
            state["errors"] += 1
            state["last_error"] = str(exc)
            log.exception("Scan error")

        elapsed = time.time() - started
        await asyncio.sleep(
            max(5, INTERVAL - elapsed)
        )


async def post_init(application: Application):
    application.create_task(
        worker(application),
        name="atlas-radar-worker",
    )


def main():
    if not TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is required"
        )

    application = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )

    handlers = [
        ("start", start),
        ("status", status),
        ("diagnostics", diagnostics),
        ("top", top),
        ("test", test),
    ]

    for command, handler in handlers:
        application.add_handler(
            CommandHandler(command, handler)
        )

    log.info("ATLAS AI V6.1 starting...")
    log.info(
        "MTF: 1D -> 4H -> 1H -> 15M | "
        "2M timing | Scan: %ss | Universe: %s",
        INTERVAL,
        UNIVERSE_SIZE,
    )

    application.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
