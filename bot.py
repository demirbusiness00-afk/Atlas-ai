# ATLAS AI V8
# Self-checking research/paper signal engine.
# S/R + MTF + liquidity + structure + VWAP + volume + RR + performance journal.
# NO automatic order execution.

import os
import time
import asyncio
import logging
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("atlas")

TOKEN = os.getenv("BOT_TOKEN", "").strip()
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "").strip()
SIGNAL_CHAT_ID = os.getenv("SIGNAL_CHAT_ID", "@ATLASRADAR").strip()

SCAN_INTERVAL = max(60, int(os.getenv("SCAN_INTERVAL", "120")))
UNIVERSE_SIZE = max(20, min(100, int(os.getenv("UNIVERSE_SIZE", "80"))))
THRESHOLD = max(70, min(100, int(os.getenv("RADAR_THRESHOLD", "82"))))
COOLDOWN = max(900, int(os.getenv("SIGNAL_COOLDOWN", "3600")))
WORKERS = max(4, min(12, int(os.getenv("MAX_WORKERS", "8"))))

# Long-horizon quality filters.
MIN_RR = float(os.getenv("MIN_RR", "2.5"))
MIN_TP2_PCT = float(os.getenv("MIN_TP2_PCT", "0.025"))  # 2.5%
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "0.035")) # 3.5%
TRACK_HOURS = max(6, int(os.getenv("TRACK_HOURS", "48")))

BASE = "https://api.binance.com"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "AtlasAI-V8"})

DB_PATH = os.getenv("ATLAS_DB", "atlas_v8.db")

state = {
    "scans": 0, "last": 0.0, "universe": 0, "scanned": 0,
    "ready": 0, "signals": 0, "errors": 0, "skipped": 0,
    "last_error": "", "top": [], "running": False,
    "blocked_quality": 0, "tracked": 0
}
sent = defaultdict(float)
scan_lock = asyncio.Lock()
scan_task = None
tracker_task = None


def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            score INTEGER NOT NULL,
            entry REAL NOT NULL,
            stop REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            support REAL,
            resistance REAL,
            reason TEXT,
            status TEXT DEFAULT 'OPEN',
            closed_ts INTEGER,
            close_price REAL
        )
    """)
    con.commit()
    return con


def allowed(update):
    if not ALLOWED_CHAT_ID:
        return True
    chat = update.effective_chat
    return bool(chat and str(chat.id) == ALLOWED_CHAT_ID)


async def guard(update):
    if allowed(update):
        return True
    if update.effective_message:
        await update.effective_message.reply_text("⛔ Yetkiniz yok.")
    return False


def api(path, params=None):
    r = SESSION.get(BASE + path, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def candles(symbol, timeframe, limit=180):
    raw = api("/api/v3/klines", {
        "symbol": symbol, "interval": timeframe, "limit": limit
    })
    out = []
    for x in raw:
        try:
            out.append({
                "t": int(x[0]), "o": float(x[1]), "h": float(x[2]),
                "l": float(x[3]), "c": float(x[4]), "v": float(x[5])
            })
        except (ValueError, TypeError, IndexError):
            continue
    return out


def ema(v, n):
    if len(v) < n:
        return None
    x = sum(v[:n]) / n
    k = 2 / (n + 1)
    for z in v[n:]:
        x = z * k + x * (1 - k)
    return x


def rsi(v, n=14):
    if len(v) <= n:
        return 50.0
    gains = losses = 0.0
    for i in range(len(v) - n, len(v)):
        d = v[i] - v[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    if losses == 0:
        return 100.0 if gains else 50.0
    rs = gains / losses
    return 100.0 - 100.0 / (1.0 + rs)


def atr(c, n=14):
    if len(c) < n + 1:
        return 0.0
    tr = []
    for i in range(1, len(c)):
        h, low, pc = c[i]["h"], c[i]["l"], c[i - 1]["c"]
        tr.append(max(h - low, abs(h - pc), abs(low - pc)))
    return sum(tr[-n:]) / n


def vwap(c):
    q = c[-60:]
    vol = sum(x["v"] for x in q)
    return sum(x["c"] * x["v"] for x in q) / vol if vol else q[-1]["c"]


def volume_ok(c):
    if len(c) < 22:
        return False
    avg = sum(x["v"] for x in c[-21:-1]) / 20
    return bool(avg and c[-1]["v"] >= avg * 1.15)


def structure(c):
    if len(c) < 24:
        return "NEUTRAL"
    h = [x["h"] for x in c[-24:]]
    l = [x["l"] for x in c[-24:]]
    h_old, h_new = max(h[:12]), max(h[12:])
    l_old, l_new = min(l[:12]), min(l[12:])
    if h_new > h_old and l_new > l_old:
        return "BULLISH"
    if h_new < h_old and l_new < l_old:
        return "BEARISH"
    return "RANGE"


def pivots(c):
    s, r = [], []
    for i in range(2, len(c) - 2):
        if c[i]["h"] >= max(c[i-2]["h"], c[i-1]["h"],
                             c[i+1]["h"], c[i+2]["h"]):
            r.append((c[i]["h"], c[i]["t"]))
        if c[i]["l"] <= min(c[i-2]["l"], c[i-1]["l"],
                             c[i+1]["l"], c[i+2]["l"]):
            s.append((c[i]["l"], c[i]["t"]))
    return s, r


def clusters(levels, tol):
    z = []
    for p, t, w in sorted(levels):
        if z and abs(p - z[-1]["p"]) <= tol:
            q = z[-1]
            q["p"] = (q["p"] * q["n"] + p) / (q["n"] + 1)
            q["n"] += 1
            q["w"] += w
            q["t"] = max(q["t"], t)
        else:
            z.append({"p": p, "n": 1, "w": w, "t": t})
    return z


def sr_map(frames):
    weights = {"1d": 4, "4h": 3, "1h": 2, "15m": 1}
    sup, res = [], []
    for tf, c in frames.items():
        if len(c) < 60:
            continue
        ps, pr = pivots(c)
        sup += [(p, t, weights[tf]) for p, t in ps]
        res += [(p, t, weights[tf]) for p, t in pr]

    price = frames["15m"][-1]["c"]
    tol = max(atr(frames["15m"]) * 0.55, price * 0.0018)
    S = clusters(sup, tol)
    R = clusters(res, tol)

    S = sorted(
        [x for x in S if x["p"] <= price],
        key=lambda x: (-x["w"], -x["p"])
    )[:8]
    R = sorted(
        [x for x in R if x["p"] >= price],
        key=lambda x: (-x["w"], x["p"])
    )[:8]
    return S, R


def near(price, z, a):
    return bool(
        z and abs(price - z["p"]) <= max(a * 0.65, price * 0.002)
    )


def rejection(c, side):
    x = c[-1]
    body = abs(x["c"] - x["o"])
    rng = max(x["h"] - x["l"], 1e-12)
    lo = min(x["o"], x["c"]) - x["l"]
    hi = x["h"] - max(x["o"], x["c"])
    if side == "LONG":
        return lo >= body * 1.15 and lo / rng >= 0.30 and x["c"] >= x["o"]
    return hi >= body * 1.15 and hi / rng >= 0.30 and x["c"] <= x["o"]


def sweep(c, side, level):
    if level is None:
        return False
    x = c[-1]
    if side == "LONG":
        return x["l"] < level and x["c"] > level
    return x["h"] > level and x["c"] < level


def retest(c, side, level):
    if level is None or len(c) < 5:
        return False
    a, x = c[-4], c[-1]
    if side == "LONG":
        return a["c"] > level and x["l"] <= level * 1.003 and x["c"] > level
    return a["c"] < level and x["h"] >= level * 0.997 and x["c"] < level


def bos(c, side):
    if len(c) < 12:
        return False
    x = c[-1]
    prior = c[-7:-1]
    if side == "LONG":
        return x["c"] > max(z["h"] for z in prior)
    return x["c"] < min(z["l"] for z in prior)


def fvg(c, side):
    if len(c) < 4:
        return False
    a, b, x = c[-3], c[-2], c[-1]
    if side == "LONG":
        return x["l"] > a["h"] and b["c"] > b["o"]
    return x["h"] < a["l"] and b["c"] < b["o"]


def two_min(m):
    out = []
    for i in range(0, len(m) - 1, 2):
        a, b = m[i], m[i + 1]
        if b["t"] != a["t"] + 60000:
            continue
        out.append({
            "t": a["t"], "o": a["o"], "h": max(a["h"], b["h"]),
            "l": min(a["l"], b["l"]), "c": b["c"], "v": a["v"] + b["v"]
        })
    return out


def analyze(sym):
    try:
        F = {
            tf: candles(sym, tf)
            for tf in ("1d", "4h", "1h", "15m")
        }
        if any(len(c) < 70 for c in F.values()):
            return None, "MTF"

        m = two_min(candles(sym, "1m", 140))
        if len(m) < 45:
            return None, "2m"

        price = F["15m"][-1]["c"]
        a = atr(F["15m"])
        if not a:
            return None, "ATR"

        S, R = sr_map(F)
        s = S[0] if S else None
        r = R[0] if R else None

        regs = {tf: structure(c) for tf, c in F.items()}
        mtf_bull = sum(regs[tf] == "BULLISH" for tf in ("1d", "4h", "1h"))
        mtf_bear = sum(regs[tf] == "BEARISH" for tf in ("1d", "4h", "1h"))

        mv = [x["c"] for x in m]
        e9, e20 = ema(mv, 9), ema(mv, 20)
        rsi2 = rsi(mv)
        vw = vwap(F["15m"])
        vol = volume_ok(F["15m"])

        lp, sp = 0, 0
        lr, sr = [], []

        # HTF: strong alignment matters more than oscillator score.
        if mtf_bull == 3:
            lp += 25; lr.append("HTF aligned bullish")
        elif mtf_bull == 2 and mtf_bear == 0:
            lp += 15; lr.append("HTF mostly bullish")

        if mtf_bear == 3:
            sp += 25; sr.append("HTF aligned bearish")
        elif mtf_bear == 2 and mtf_bull == 0:
            sp += 15; sr.append("HTF mostly bearish")

        ns = near(price, s, a)
        nr = near(price, r, a)

        if ns:
            lp += 24; lr.append("SUPPORT ZONE")
        if nr:
            sp += 24; sr.append("RESISTANCE ZONE")

        if s and rejection(F["15m"], "LONG"):
            lp += 10; lr.append("support rejection")
        if r and rejection(F["15m"], "SHORT"):
            sp += 10; sr.append("resistance rejection")

        if s and sweep(F["15m"], "LONG", s["p"]):
            lp += 12; lr.append("liquidity sweep")
        if r and sweep(F["15m"], "SHORT", r["p"]):
            sp += 12; sr.append("liquidity sweep")

        if s and retest(F["15m"], "LONG", s["p"]):
            lp += 10; lr.append("support retest")
        if r and retest(F["15m"], "SHORT", r["p"]):
            sp += 10; sr.append("resistance retest")

        if bos(m, "LONG"):
            lp += 8; lr.append("2m BOS")
        if bos(m, "SHORT"):
            sp += 8; sr.append("2m BOS")

        if fvg(m, "LONG"):
            lp += 4; lr.append("2m FVG")
        if fvg(m, "SHORT"):
            sp += 4; sr.append("2m FVG")

        if e9 and e20 and e9 > e20 and rsi2 >= 52:
            lp += 6; lr.append("2m momentum")
        if e9 and e20 and e9 < e20 and rsi2 <= 48:
            sp += 6; sr.append("2m momentum")

        if price > vw:
            lp += 4; lr.append("above VWAP")
        elif price < vw:
            sp += 4; sr.append("below VWAP")

        if vol:
            if lp >= sp:
                lp += 4; lr.append("volume confirmation")
            else:
                sp += 4; sr.append("volume confirmation")

        if lp >= sp:
            side, score, reasons, zone = "LONG", min(100, int(lp)), lr, s
        else:
            side, score, reasons, zone = "SHORT", min(100, int(sp)), sr, r

        # Hard contradiction gate.
        if side == "LONG":
            contradiction = mtf_bear >= 2
        else:
            contradiction = mtf_bull >= 2

        if contradiction:
            return {
                "symbol": sym, "side": side, "score": score, "ready": False,
                "entry": price, "stop": price, "tp1": price, "tp2": price,
                "support": s["p"] if s else None,
                "resistance": r["p"] if r else None,
                "regimes": regs, "reasons": reasons + ["HTF CONTRADICTION"],
                "volume": vol, "blocked": "HTF contradiction"
            }, None

        if zone is None:
            return {
                "symbol": sym, "side": side, "score": score, "ready": False,
                "entry": price, "stop": price, "tp1": price, "tp2": price,
                "support": s["p"] if s else None,
                "resistance": r["p"] if r else None,
                "regimes": regs, "reasons": reasons,
                "volume": vol, "blocked": "No S/R zone"
            }, None

        # Require a real location + price-action confirmation.
        location_ok = near(price, zone, a)
        event_ok = (
            rejection(F["15m"], side)
            or sweep(F["15m"], side, zone["p"])
            or retest(F["15m"], side, zone["p"])
            or bos(m, side)
        )

        # Avoid buying directly into resistance / selling directly into support.
        if side == "LONG" and nr and not ns:
            event_ok = False
        if side == "SHORT" and ns and not nr:
            event_ok = False

        if not (score >= THRESHOLD and location_ok and event_ok):
            return {
                "symbol": sym, "side": side, "score": score, "ready": False,
                "entry": price, "stop": price, "tp1": price, "tp2": price,
                "support": s["p"] if s else None,
                "resistance": r["p"] if r else None,
                "regimes": regs, "reasons": reasons,
                "volume": vol, "blocked": "Confluence incomplete"
            }, None

        # ATR-based stop. Targets are intentionally wider for multi-hour/day holds.
        entry = price
        if side == "LONG":
            stop = min(zone["p"] - 0.45 * a, entry - 1.05 * a)
            risk = max(entry - stop, entry * 0.003)
            tp1 = entry + 1.75 * risk
            tp2 = entry + 3.0 * risk
        else:
            stop = max(zone["p"] + 0.45 * a, entry + 1.05 * a)
            risk = max(stop - entry, entry * 0.003)
            tp1 = entry - 1.75 * risk
            tp2 = entry - 3.0 * risk

        stop_pct = abs(entry - stop) / entry
        tp2_pct = abs(tp2 - entry) / entry
        rr = tp2_pct / stop_pct if stop_pct else 0.0

        if stop_pct > MAX_STOP_PCT:
            return {
                "symbol": sym, "side": side, "score": score, "ready": False,
                "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                "support": s["p"], "resistance": r["p"],
                "regimes": regs, "reasons": reasons,
                "volume": vol, "blocked": "Stop too wide"
            }, None

        if rr < MIN_RR or tp2_pct < MIN_TP2_PCT:
            return {
                "symbol": sym, "side": side, "score": score, "ready": False,
                "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
                "support": s["p"], "resistance": r["p"],
                "regimes": regs, "reasons": reasons,
                "volume": vol, "blocked": "Expected move too small"
            }, None

        reasons.append("PRO CONFLUENCE")
        reasons.append(f"TP2 potential {tp2_pct * 100:.1f}%")

        return {
            "symbol": sym, "side": side, "score": score, "ready": True,
            "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
            "support": s["p"], "resistance": r["p"],
            "regimes": regs, "reasons": reasons[:9], "volume": vol,
            "rr": rr, "tp2_pct": tp2_pct, "blocked": ""
        }, None

    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def universe():
    info = api("/api/v3/exchangeInfo")
    allowed_symbols = {
        x["symbol"] for x in info.get("symbols", [])
        if x.get("status") == "TRADING"
        and x.get("quoteAsset") == "USDT"
        and x.get("isSpotTradingAllowed", False)
    }

    banned_words = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
    rows = []

    for t in api("/api/v3/ticker/24hr"):
        s = t.get("symbol")
        try:
            q = float(t.get("quoteVolume") or 0)
        except (ValueError, TypeError):
            continue

        if (
            s in allowed_symbols and q > 0
            and not any(s.endswith(x) for x in banned_words)
        ):
            rows.append((q, s))

    rows.sort(reverse=True)
    return [s for _, s in rows[:UNIVERSE_SIZE]]


def scan():
    U, out, errors = universe(), [], 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(analyze, s): s for s in U}
        for f in as_completed(futures):
            d, e = f.result()
            if d:
                out.append(d)
            else:
                errors += 1

    out.sort(key=lambda x: (x["ready"], x["score"]), reverse=True)
    return U, out, errors


def fmt_price(value):
    if value is None:
        return "-"
    av = abs(value)
    if av >= 1000:
        return f"{value:,.2f}"
    if av >= 1:
        return f"{value:.5f}"
    if av >= 0.01:
        return f"{value:.6f}"
    if av >= 0.0001:
        return f"{value:.8f}"
    return f"{value:.12f}".rstrip("0").rstrip(".")


def signal_text(x):
    side = "🟢 LONG / AL" if x["side"] == "LONG" else "🔴 SHORT / SAT"
    return (
        "🎯 ATLAS AI V8 PRO SIGNAL\n\n"
        f"{x['symbol']} — {side}\n"
        f"Skor: {x['score']}/100 | RR: 1:{x['rr']:.1f}\n"
        f"TP2 potansiyeli: {x['tp2_pct'] * 100:.1f}%\n\n"
        f"📍 Entry: {fmt_price(x['entry'])}\n"
        f"🛑 Stop: {fmt_price(x['stop'])}\n"
        f"🎯 TP1: {fmt_price(x['tp1'])}\n"
        f"🎯 TP2: {fmt_price(x['tp2'])}\n\n"
        f"🟢 Destek: {fmt_price(x['support'])}\n"
        f"🔴 Direnç: {fmt_price(x['resistance'])}\n\n"
        f"MTF: 1D {x['regimes']['1d']} | "
        f"4H {x['regimes']['4h']} | "
        f"1H {x['regimes']['1h']} | "
        f"15M {x['regimes']['15m']}\n"
        "2M giriş: teyitli\n"
        f"Volume: {'YES' if x['volume'] else 'NO'}\n"
        f"Neden: {', '.join(x['reasons'])}\n\n"
        "🧠 ATLAS kalite filtresinden geçti.\n"
        "⚠️ Araştırma/paper sinyali. Kâr garantisi yoktur."
    )


def save_signal(x):
    con = db()
    con.execute("""
        INSERT INTO signals
        (ts, symbol, side, score, entry, stop, tp1, tp2,
         support, resistance, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        int(time.time()), x["symbol"], x["side"], x["score"],
        x["entry"], x["stop"], x["tp1"], x["tp2"],
        x["support"], x["resistance"], ", ".join(x["reasons"])
    ))
    con.commit()
    con.close()


async def send_signal(app, x):
    if not SIGNAL_CHAT_ID:
        state["errors"] += 1
        state["last_error"] = "SIGNAL_CHAT_ID boş."
        return False

    key = f"{x['symbol']}:{x['side']}"
    now = time.time()
    if now - sent[key] < COOLDOWN:
        return False

    try:
        tv_url = (
            "https://www.tradingview.com/chart/"
            f"?symbol=BINANCE:{x['symbol']}&interval=15"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📈 TradingView'de Aç", url=tv_url)
        ]])

        await app.bot.send_message(
            chat_id=SIGNAL_CHAT_ID,
            text=signal_text(x),
            reply_markup=keyboard
        )

        save_signal(x)
        sent[key] = now
        state["signals"] += 1
        log.info("Signal sent to %s: %s %s",
                 SIGNAL_CHAT_ID, x["symbol"], x["side"])
        return True

    except Exception as e:
        state["errors"] += 1
        state["last_error"] = f"SIGNAL: {type(e).__name__}: {e}"
        log.exception("Signal send failed")
        return False


def current_price(symbol):
    data = api("/api/v3/ticker/price", {"symbol": symbol})
    return float(data["price"])


def evaluate_open_signals():
    con = db()
    rows = con.execute("""
        SELECT id, ts, symbol, side, entry, stop, tp1, tp2
        FROM signals
        WHERE status = 'OPEN'
        ORDER BY id
    """).fetchall()

    closed = 0
    for row in rows:
        sid, ts, symbol, side, entry, stop, tp1, tp2 = row

        if time.time() - ts > TRACK_HOURS * 3600:
            con.execute(
                "UPDATE signals SET status='EXPIRED', closed_ts=?, close_price=? WHERE id=?",
                (int(time.time()), current_price(symbol), sid)
            )
            closed += 1
            continue

        try:
            p = current_price(symbol)
        except Exception:
            continue

        if side == "LONG":
            if p <= stop:
                status = "STOP"
            elif p >= tp2:
                status = "TP2"
            elif p >= tp1:
                status = "TP1"
            else:
                continue
        else:
            if p >= stop:
                status = "STOP"
            elif p <= tp2:
                status = "TP2"
            elif p <= tp1:
                status = "TP1"
            else:
                continue

        con.execute(
            "UPDATE signals SET status=?, closed_ts=?, close_price=? WHERE id=?",
            (status, int(time.time()), p, sid)
        )
        closed += 1

    con.commit()
    con.close()
    return closed


async def tracker():
    while True:
        try:
            await asyncio.to_thread(evaluate_open_signals)
        except Exception as e:
            state["errors"] += 1
            state["last_error"] = f"TRACKER: {type(e).__name__}: {e}"
        await asyncio.sleep(max(60, SCAN_INTERVAL))


async def do_scan(app):
    if scan_lock.locked():
        return

    async with scan_lock:
        state["running"] = True
        try:
            U, R, E = await asyncio.to_thread(scan)

            ready = [x for x in R if x["ready"]]
            blocked = [x for x in R if not x["ready"]]

            state.update(
                scans=state["scans"] + 1,
                last=time.time(),
                universe=len(U),
                scanned=len(R),
                ready=len(ready),
                errors=E,
                skipped=len(U) - len(R),
                blocked_quality=len(blocked),
                last_error="",
                top=R[:5]
            )

            for x in ready:
                await send_signal(app, x)

        except Exception as e:
            state["errors"] += 1
            state["last_error"] = f"{type(e).__name__}: {e}"
            log.exception("Scan failed")
        finally:
            state["running"] = False


async def scanner(app):
    await asyncio.sleep(3)
    while True:
        await do_scan(app)
        await asyncio.sleep(SCAN_INTERVAL)


async def start(update: Update, context):
    if not await guard(update):
        return
    await update.effective_message.reply_text(
        "🚀 ATLAS AI V8\n"
        "Self-checking PRO radar aktif.\n"
        f"Tarama: {SCAN_INTERVAL}s | Universe: {UNIVERSE_SIZE}\n"
        f"Threshold: {THRESHOLD} | Min RR: 1:{MIN_RR}\n"
        f"Min TP2: {MIN_TP2_PCT * 100:.1f}% | Max stop: {MAX_STOP_PCT * 100:.1f}%\n"
        f"📡 Kanal: {SIGNAL_CHAT_ID or 'AYARLANMADI'}"
    )


async def status(update: Update, context):
    if not await guard(update):
        return

    last = "yok" if not state["last"] else f"{int(time.time()-state['last'])}s önce"
    await update.effective_message.reply_text(
        "🧭 ATLAS AI V8 STATUS\n"
        f"Tarama: {state['scans']}\n"
        f"Son tarama: {last}\n"
        f"Universe: {state['universe']}\n"
        f"Taranan: {state['scanned']}\n"
        f"TRADE READY: {state['ready']}\n"
        f"Toplam sinyal: {state['signals']}\n"
        f"Açık takip: {open_count()}\n"
        f"Hata: {state['errors']}\n"
        f"Atlanan: {state['skipped']}\n"
        f"Son hata: {state['last_error'] or 'Yok'}"
    )


def open_count():
    con = db()
    n = con.execute(
        "SELECT COUNT(*) FROM signals WHERE status='OPEN'"
    ).fetchone()[0]
    con.close()
    return n


async def diagnostics(update: Update, context):
    if not await guard(update):
        return

    last = "yok" if not state["last"] else f"{int(time.time()-state['last'])}s önce"
    lines = [
        "🛠 ATLAS AI V8 DIAGNOSTICS",
        "",
        f"Tarama: {state['scans']}",
        f"Son tarama: {last}",
        f"Universe: {state['universe']}",
        f"Taranan: {state['scanned']}",
        f"TRADE READY: {state['ready']}",
        f"Kalite filtresinden kalan WATCH: {state['blocked_quality']}",
        f"Toplam sinyal: {state['signals']}",
        f"Açık takip: {open_count()}",
        f"Hata: {state['errors']}",
        f"Atlanan: {state['skipped']}",
        f"Durum: {'SCANNING' if state['running'] else 'IDLE'}",
        f"SIGNAL CHAT: {SIGNAL_CHAT_ID or 'AYARLANMADI'}",
        f"Min RR: 1:{MIN_RR}",
        f"Min TP2: {MIN_TP2_PCT*100:.1f}%",
        "",
        "🏆 TOP 5"
    ]

    for i, x in enumerate(state["top"], 1):
        tag = "READY" if x["ready"] else f"WATCH — {x.get('blocked','')}"
        lines.append(
            f"{i}. {x['symbol']} {x['side']} {x['score']}/100 {tag}"
        )

    await update.effective_message.reply_text("\n".join(lines))


async def performance(update: Update, context):
    if not await guard(update):
        return

    con = db()
    total = con.execute(
        "SELECT COUNT(*) FROM signals WHERE status!='OPEN'"
    ).fetchone()[0]
    tp2 = con.execute(
        "SELECT COUNT(*) FROM signals WHERE status='TP2'"
    ).fetchone()[0]
    tp1 = con.execute(
        "SELECT COUNT(*) FROM signals WHERE status='TP1'"
    ).fetchone()[0]
    stop = con.execute(
        "SELECT COUNT(*) FROM signals WHERE status='STOP'"
    ).fetchone()[0]
    expired = con.execute(
        "SELECT COUNT(*) FROM signals WHERE status='EXPIRED'"
    ).fetchone()[0]
    open_n = con.execute(
        "SELECT COUNT(*) FROM signals WHERE status='OPEN'"
    ).fetchone()[0]
    con.close()

    decisive = tp2 + tp1 + stop
    win_rate = ((tp2 + tp1) / decisive * 100) if decisive else 0.0

    await update.effective_message.reply_text(
        "📊 ATLAS AI V8 PERFORMANCE\n\n"
        f"Sonuçlanan: {total}\n"
        f"TP2: {tp2}\n"
        f"TP1: {tp1}\n"
        f"STOP: {stop}\n"
        f"EXPIRED: {expired}\n"
        f"Açık: {open_n}\n"
        f"Win rate* : {win_rate:.1f}%\n\n"
        "*Paper-trade takip oranı; gerçek kâr garantisi değildir."
    )


async def test(update: Update, context):
    if not await guard(update):
        return

    try:
        d = candles("BTCUSDT", "1m", 3)
        await update.effective_message.reply_text(
            "🧪 TEST OK\n"
            f"Binance: {'✅' if len(d) == 3 else '⚠️'}\n"
            f"BTCUSDT 1m: {len(d)}\n"
            f"Signal channel: {SIGNAL_CHAT_ID or 'AYARLANMADI'}\n"
            f"Database: {'✅' if Path(DB_PATH).exists() else '⚠️'}"
        )
    except Exception as e:
        await update.effective_message.reply_text(
            f"❌ TEST ERROR: {type(e).__name__}: {e}"
        )


async def post_init(app):
    global scan_task, tracker_task
    db().close()
    scan_task = asyncio.create_task(scanner(app))
    tracker_task = asyncio.create_task(tracker())


async def post_shutdown(app):
    global scan_task, tracker_task

    for task in (scan_task, tracker_task):
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


def main():
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN is required")

    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("diagnostics", diagnostics))
    app.add_handler(CommandHandler("performance", performance))
    app.add_handler(CommandHandler("test", test))

    log.info(
        "ATLAS AI V8 starting. Signal channel=%s",
        SIGNAL_CHAT_ID or "NOT SET"
    )
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
