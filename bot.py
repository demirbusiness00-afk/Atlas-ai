"""
ATLAS AI V9
Paper-signal / research bot for Binance spot markets.

Architecture:
1D -> 4H -> 1H -> 15M
2M is ONLY an entry/anti-miss trigger (built from Binance 1m candles).
Core concepts:
- market structure / BOS
- support & resistance zones
- liquidity sweep
- retest
- FVG
- VWAP
- volume profile approximation (POC / VAH / VAL)
- ATR based stop/targets
- minimum RR + minimum TP2 distance
- paper trade tracking and performance
- Telegram channel signals with TradingView buttons

No order execution. No profit guarantee.
"""

import os
import math
import time
import json
import sqlite3
import asyncio
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ---------------- CONFIG ----------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SIGNAL_CHAT = os.getenv("SIGNAL_CHAT", os.getenv("SIGNAL_CHANNEL", "@ATLASRADAR")).strip()

BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com").rstrip("/")
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "120"))
UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "80"))

# Keep signal quality high, but don't make V8's "everything must agree" mistake.
READY_SCORE = int(os.getenv("READY_SCORE", "80"))
WATCH_SCORE = int(os.getenv("WATCH_SCORE", "72"))
MIN_RR = float(os.getenv("MIN_RR", "2.5"))
MIN_TP2_PCT = float(os.getenv("MIN_TP2_PCT", "2.5"))
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "3.5"))

# 1D/4H/1H/15M are the actual decision stack.
TF_LIMITS = {"1d": 160, "4h": 220, "1h": 260, "15m": 260, "1m": 300}

DB_PATH = os.getenv("DB_PATH", "atlas_v9.sqlite3")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("ATLAS-V9")

# ---------------- DATA ----------------

@dataclass
class Candle:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float

@dataclass
class Analysis:
    symbol: str
    direction: str
    score: int
    status: str
    reason: str
    entry: float
    stop: float
    tp1: float
    tp2: float
    rr: float
    support: float
    resistance: float
    vwap: float
    poc: float
    vah: float
    val: float
    atr: float
    htf: str
    structure_4h: str
    setup_1h: str
    trigger_15m: str
    trigger_2m: str
    tv_url: str

# ---------------- DB ----------------

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            score INTEGER NOT NULL,
            entry REAL NOT NULL,
            stop REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            close_price REAL,
            close_reason TEXT,
            closed_at INTEGER
        )
    """)
    conn.commit()
    return conn

def save_signal(a: Analysis):
    conn = db()
    conn.execute("""
        INSERT INTO signals
        (created_at,symbol,direction,score,entry,stop,tp1,tp2,status)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        int(time.time()), a.symbol, a.direction, a.score,
        a.entry, a.stop, a.tp1, a.tp2, "OPEN"
    ))
    conn.commit()
    conn.close()

def performance():
    conn = db()
    rows = conn.execute("""
        SELECT status, COUNT(*) FROM signals
        GROUP BY status
    """).fetchall()
    open_count = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE status='OPEN'"
    ).fetchone()[0]
    total = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE status IN ('TP1','TP2','STOP','EXPIRED')"
    ).fetchone()[0]
    tp2 = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE status='TP2'"
    ).fetchone()[0]
    tp1 = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE status='TP1'"
    ).fetchone()[0]
    stop = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE status='STOP'"
    ).fetchone()[0]
    expired = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE status='EXPIRED'"
    ).fetchone()[0]
    conn.close()
    wins = tp1 + tp2
    wr = (wins / total * 100) if total else 0.0
    return total, tp2, tp1, stop, expired, open_count, wr

# ---------------- MATH / INDICATORS ----------------

def ema(values: List[float], n: int) -> List[float]:
    if not values:
        return []
    k = 2 / (n + 1)
    out = [values[0]]
    for x in values[1:]:
        out.append(x * k + out[-1] * (1 - k))
    return out

def atr(cs: List[Candle], n: int = 14) -> float:
    if len(cs) < n + 1:
        return 0.0
    trs = []
    prev = cs[0].c
    for x in cs[1:]:
        trs.append(max(x.h - x.l, abs(x.h - prev), abs(x.l - prev)))
        prev = x.c
    return sum(trs[-n:]) / n

def vwap(cs: List[Candle]) -> float:
    pv = sum(((x.h + x.l + x.c) / 3) * x.v for x in cs)
    vol = sum(x.v for x in cs)
    return pv / vol if vol else cs[-1].c

def slope(values: List[float], n: int = 8) -> float:
    if len(values) < n + 1:
        return 0.0
    a = sum(values[-n:]) / n
    b = sum(values[-2*n:-n]) / n
    return (a - b) / (abs(b) or 1e-12)

def trend(cs: List[Candle]) -> str:
    closes = [x.c for x in cs]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    sl = slope(closes, 10)
    if e20 > e50 and sl > 0:
        return "BULLISH"
    if e20 < e50 and sl < 0:
        return "BEARISH"
    return "RANGE"

def pivot_levels(cs: List[Candle], left: int = 3, right: int = 3):
    highs, lows = [], []
    for i in range(left, len(cs)-right):
        h = cs[i].h
        l = cs[i].l
        if all(h > cs[j].h for j in range(i-left, i)) and all(h >= cs[j].h for j in range(i+1, i+right+1)):
            highs.append(h)
        if all(l < cs[j].l for j in range(i-left, i)) and all(l <= cs[j].l for j in range(i+1, i+right+1)):
            lows.append(l)
    return highs[-20:], lows[-20:]

def nearest_sr(cs: List[Candle]) -> Tuple[float, float]:
    price = cs[-1].c
    highs, lows = pivot_levels(cs)
    # Add recent extremes as fallback.
    resistance_candidates = [x for x in highs if x > price * 1.001]
    support_candidates = [x for x in lows if x < price * 0.999]
    resistance = min(resistance_candidates, key=lambda x: x-price) if resistance_candidates else max(x.h for x in cs[-60:])
    support = max(support_candidates, key=lambda x: price-x) if support_candidates else min(x.l for x in cs[-60:])
    return support, resistance

def volume_profile(cs: List[Candle], rows: int = 40):
    lo = min(x.l for x in cs)
    hi = max(x.h for x in cs)
    if hi <= lo:
        return cs[-1].c, hi, lo
    step = (hi - lo) / rows
    bins = [0.0] * rows
    for x in cs:
        typical = (x.h + x.l + x.c) / 3
        idx = int((typical - lo) / step)
        idx = max(0, min(rows-1, idx))
        bins[idx] += x.v
    poc_i = max(range(rows), key=lambda i: bins[i])
    total = sum(bins)
    target = total * 0.70
    included = {poc_i}
    acc = bins[poc_i]
    up, down = poc_i + 1, poc_i - 1
    while acc < target and (up < rows or down >= 0):
        uv = bins[up] if up < rows else -1
        dv = bins[down] if down >= 0 else -1
        if uv >= dv:
            if up < rows:
                included.add(up); acc += bins[up]; up += 1
            else:
                included.add(down); acc += bins[down]; down -= 1
        else:
            if down >= 0:
                included.add(down); acc += bins[down]; down -= 1
            else:
                included.add(up); acc += bins[up]; up += 1
    poc = lo + (poc_i + 0.5) * step
    vah = lo + (max(included) + 1) * step
    val = lo + min(included) * step
    return poc, vah, val

def bos_state(cs: List[Candle], lookback: int = 8) -> str:
    if len(cs) < lookback + 3:
        return "NONE"
    highs, lows = pivot_levels(cs[-min(len(cs), 80):], 2, 2)
    price = cs[-1].c
    if highs and price > highs[-1]:
        return "BULLISH_BOS"
    if lows and price < lows[-1]:
        return "BEARISH_BOS"
    return "NONE"

def liquidity_sweep(cs: List[Candle]) -> str:
    if len(cs) < 10:
        return "NONE"
    prior_high = max(x.h for x in cs[-10:-2])
    prior_low = min(x.l for x in cs[-10:-2])
    last = cs[-1]
    # Sweep above high then close back below = bearish.
    if last.h > prior_high and last.c < prior_high:
        return "BEARISH_SWEEP"
    # Sweep below low then close back above = bullish.
    if last.l < prior_low and last.c > prior_low:
        return "BULLISH_SWEEP"
    return "NONE"

def fvg_state(cs: List[Candle]) -> str:
    if len(cs) < 3:
        return "NONE"
    a, b, c = cs[-3], cs[-2], cs[-1]
    if c.l > a.h:
        return "BULLISH_FVG"
    if c.h < a.l:
        return "BEARISH_FVG"
    return "NONE"

def retest_state(cs: List[Candle], direction: str) -> bool:
    if len(cs) < 8:
        return False
    e20 = ema([x.c for x in cs], 20)
    recent = cs[-5:]
    if direction == "LONG":
        return any(x.l <= e20[-1] * 1.002 and x.c > e20[-1] for x in recent)
    return any(x.h >= e20[-1] * 0.998 and x.c < e20[-1] for x in recent)

def vol_confirmation(cs: List[Candle]) -> bool:
    if len(cs) < 25:
        return False
    avg = sum(x.v for x in cs[-21:-1]) / 20
    return cs[-1].v >= avg * 1.15

def aggregate_2m(one_minute: List[Candle]) -> List[Candle]:
    out = []
    i = 0
    while i + 1 < len(one_minute):
        a, b = one_minute[i], one_minute[i+1]
        if b.ts - a.ts > 70_000:
            i += 1
            continue
        out.append(Candle(
            ts=a.ts,
            o=a.o,
            h=max(a.h, b.h),
            l=min(a.l, b.l),
            c=b.c,
            v=a.v + b.v
        ))
        i += 2
    return out

# ---------------- BINANCE ----------------

class Binance:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.sem = asyncio.Semaphore(8)

    async def start(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"User-Agent": "ATLAS-AI-V9/1.0"}
        )

    async def close(self):
        if self.session:
            await self.session.close()

    async def get_json(self, path, params=None):
        assert self.session
        url = BINANCE_BASE + path
        async with self.sem:
            for attempt in range(3):
                try:
                    async with self.session.get(url, params=params) as r:
                        if r.status == 429:
                            await asyncio.sleep(2 + attempt * 2)
                            continue
                        r.raise_for_status()
                        return await r.json()
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1 + attempt)
        return None

    async def symbols(self, limit=80):
        data = await self.get_json("/api/v3/ticker/24hr")
        if not isinstance(data, list):
            return []
        candidates = []
        for x in data:
            s = x.get("symbol", "")
            if not s.endswith("USDT"):
                continue
            if any(s.endswith(z) for z in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
                continue
            try:
                qv = float(x.get("quoteVolume", 0))
                price = float(x.get("lastPrice", 0))
                if qv > 0 and price > 0:
                    candidates.append((qv, s))
            except Exception:
                pass
        candidates.sort(reverse=True)
        return [s for _, s in candidates[:limit]]

    async def klines(self, symbol, interval, limit):
        data = await self.get_json(
            "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": limit}
        )
        out = []
        for x in data or []:
            out.append(Candle(
                ts=int(x[0]),
                o=float(x[1]),
                h=float(x[2]),
                l=float(x[3]),
                c=float(x[4]),
                v=float(x[5]),
            ))
        return out

# ---------------- V9 DECISION ENGINE ----------------

def directional_votes(csets: Dict[str, List[Candle]]):
    dirs = {}
    for tf in ("1d", "4h", "1h", "15m"):
        dirs[tf] = trend(csets[tf])
    return dirs

def analyze(symbol: str, csets: Dict[str, List[Candle]]) -> Optional[Analysis]:
    d = directional_votes(csets)
    d1, h4, h1, m15 = d["1d"], d["4h"], d["1h"], d["15m"]

    # 1D is the regime. We do not trade directly against it.
    if d1 == "RANGE":
        regime = "RANGE"
    else:
        regime = d1

    votes = {"LONG": 0, "SHORT": 0}
    if d1 == "BULLISH": votes["LONG"] += 22
    if d1 == "BEARISH": votes["SHORT"] += 22
    if h4 == "BULLISH": votes["LONG"] += 18
    if h4 == "BEARISH": votes["SHORT"] += 18
    if h1 == "BULLISH": votes["LONG"] += 14
    if h1 == "BEARISH": votes["SHORT"] += 14
    if m15 == "BULLISH": votes["LONG"] += 10
    if m15 == "BEARISH": votes["SHORT"] += 10

    direction = "LONG" if votes["LONG"] >= votes["SHORT"] else "SHORT"

    # If 1D is clear, block direct opposition.
    if regime == "BULLISH" and direction == "SHORT":
        direction = "LONG"
    if regime == "BEARISH" and direction == "LONG":
        direction = "SHORT"

    c4, c1, c15 = csets["4h"], csets["1h"], csets["15m"]
    c2 = csets.get("2m", [])

    support, resistance = nearest_sr(c4 + c1[-80:])
    poc, vah, val = volume_profile(c4[-160:])
    vw = vwap(c15[-80:])
    a = atr(c15, 14)
    price = c15[-1].c

    structure = bos_state(c4)
    setup = bos_state(c1)
    sweep = liquidity_sweep(c15)
    fvg = fvg_state(c15)
    vol_ok = vol_confirmation(c15)
    retest = retest_state(c1, direction)

    score = 0
    reasons = []

    # Directional alignment
    aligned = 0
    for x in (d1, h4, h1, m15):
        if (direction == "LONG" and x == "BULLISH") or (direction == "SHORT" and x == "BEARISH"):
            aligned += 1
    score += aligned * 9
    if aligned >= 3:
        reasons.append("MTF alignment")

    if direction == "LONG":
        if structure == "BULLISH_BOS": score += 10; reasons.append("4H BOS")
        if setup == "BULLISH_BOS": score += 9; reasons.append("1H BOS")
        if sweep == "BULLISH_SWEEP": score += 12; reasons.append("15M liquidity sweep")
        if fvg == "BULLISH_FVG": score += 7; reasons.append("bullish FVG")
        if price >= vw: score += 6; reasons.append("above VWAP")
        if retest: score += 7; reasons.append("retest")
        if vol_ok: score += 5; reasons.append("volume")
        if price >= val and price <= poc * 1.003: score += 5; reasons.append("value-area support")
    else:
        if structure == "BEARISH_BOS": score += 10; reasons.append("4H BOS")
        if setup == "BEARISH_BOS": score += 9; reasons.append("1H BOS")
        if sweep == "BEARISH_SWEEP": score += 12; reasons.append("15M liquidity sweep")
        if fvg == "BEARISH_FVG": score += 7; reasons.append("bearish FVG")
        if price <= vw: score += 6; reasons.append("below VWAP")
        if retest: score += 7; reasons.append("retest")
        if vol_ok: score += 5; reasons.append("volume")
        if price <= vah and price >= poc * 0.997: score += 5; reasons.append("value-area resistance")

    # 2M is not allowed to override 15M. It can only add a small trigger bonus.
    trigger2 = trend(c2) if c2 else "NONE"
    if direction == "LONG" and trigger2 == "BULLISH":
        score += 3
    if direction == "SHORT" and trigger2 == "BEARISH":
        score += 3

    # Entry is current 15M close. Stop is structure/ATR based.
    if direction == "LONG":
        structural_stop = min(support, price - 1.15 * a)
        stop = min(price - 0.8 * a, structural_stop)
        risk = price - stop
        tp1 = price + max(1.6 * risk, 0.012 * price)
        tp2 = price + max(2.5 * risk, 0.025 * price)
        next_res = resistance
        if next_res > price and next_res < tp2:
            tp2 = next_res * 0.997
    else:
        structural_stop = max(resistance, price + 1.15 * a)
        stop = max(price + 0.8 * a, structural_stop)
        risk = stop - price
        tp1 = price - max(1.6 * risk, 0.012 * price)
        tp2 = price - max(2.5 * risk, 0.025 * price)
        next_sup = support
        if next_sup < price and next_sup > tp2:
            tp2 = next_sup * 1.003

    if risk <= 0:
        return None

    rr = abs(tp2 - price) / risk
    stop_pct = risk / price * 100
    tp2_pct = abs(tp2 - price) / price * 100

    if stop_pct > MAX_STOP_PCT:
        score -= 8
        reasons.append("wide stop")
    if rr >= MIN_RR:
        score += 6
        reasons.append(f"RR {rr:.1f}")
    else:
        score -= 10
        reasons.append("RR weak")
    if tp2_pct >= MIN_TP2_PCT:
        score += 5
        reasons.append(f"TP2 {tp2_pct:.1f}%")
    else:
        score -= 10
        reasons.append("TP2 too close")

    # Hard contradiction: 1D vs trade direction.
    contradiction = (
        (direction == "LONG" and d1 == "BEARISH") or
        (direction == "SHORT" and d1 == "BULLISH")
    )
    if contradiction:
        score -= 30
        reasons.append("HTF contradiction")

    score = max(0, min(100, int(score)))

    if score >= READY_SCORE and not contradiction and rr >= MIN_RR and tp2_pct >= MIN_TP2_PCT:
        status = "READY"
    elif score >= WATCH_SCORE:
        status = "WATCH"
    else:
        status = "IGNORE"

    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}"

    return Analysis(
        symbol=symbol,
        direction=direction,
        score=score,
        status=status,
        reason=", ".join(reasons[:8]) or "Confluence incomplete",
        entry=price,
        stop=stop,
        tp1=tp1,
        tp2=tp2,
        rr=rr,
        support=support,
        resistance=resistance,
        vwap=vw,
        poc=poc,
        vah=vah,
        val=val,
        atr=a,
        htf=f"1D {d1} | 4H {h4} | 1H {h1} | 15M {m15}",
        structure_4h=structure,
        setup_1h=setup,
        trigger_15m=f"{sweep} / {fvg}",
        trigger_2m=trigger2,
        tv_url=tv,
    )

# ---------------- TELEGRAM ----------------

def fmt_price(x: float) -> str:
    if x == 0:
        return "0"
    if abs(x) >= 100:
        return f"{x:.3f}"
    if abs(x) >= 1:
        return f"{x:.5f}"
    if abs(x) >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8g}"

def signal_text(a: Analysis):
    icon = "🟢" if a.direction == "LONG" else "🔴"
    side = "LONG / AL" if a.direction == "LONG" else "SHORT / SAT"
    return (
        f"🎯 ATLAS AI V9 SIGNAL\n\n"
        f"{a.symbol} — {icon} {side}\n"
        f"Skor: {a.score}/100\n\n"
        f"Entry: {fmt_price(a.entry)}\n"
        f"Stop: {fmt_price(a.stop)}\n"
        f"TP1: {fmt_price(a.tp1)}\n"
        f"TP2: {fmt_price(a.tp2)}\n\n"
        f"🟢 Destek: {fmt_price(a.support)}\n"
        f"🔴 Direnç: {fmt_price(a.resistance)}\n"
        f"POC: {fmt_price(a.poc)} | VAH: {fmt_price(a.vah)} | VAL: {fmt_price(a.val)}\n"
        f"VWAP: {fmt_price(a.vwap)}\n\n"
        f"MTF: {a.htf}\n"
        f"4H yapı: {a.structure_4h}\n"
        f"1H setup: {a.setup_1h}\n"
        f"15M trigger: {a.trigger_15m}\n"
        f"2M kaçırmama: {a.trigger_2m}\n\n"
        f"RR: 1:{a.rr:.1f}\n"
        f"Neden: {a.reason}\n\n"
        f"⚠️ Araştırma/paper sinyali. Kâr garantisi yoktur."
    )

async def send_signal(bot, a: Analysis):
    keyboard = [[InlineKeyboardButton("📈 TradingView'de Aç", url=a.tv_url)]]
    await bot.send_message(
        chat_id=SIGNAL_CHAT,
        text=signal_text(a),
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧪 ATLAS AI V9 TEST OK\n"
        "Binance REST: hazır\n"
        f"Signal chat: {SIGNAL_CHAT}\n"
        "Timeframes: 1D → 4H → 1H → 15M | 2M anti-miss\n"
        "Mode: paper signals only"
    )

async def cmd_performance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total, tp2, tp1, stop, expired, opened, wr = performance()
    await update.message.reply_text(
        "📊 ATLAS AI V9 PERFORMANCE\n\n"
        f"Sonuçlanan: {total}\n"
        f"TP2: {tp2}\n"
        f"TP1: {tp1}\n"
        f"STOP: {stop}\n"
        f"EXPIRED: {expired}\n"
        f"Açık: {opened}\n"
        f"Win rate*: {wr:.1f}%\n\n"
        "*Paper-trade istatistiğidir; gerçek kâr garantisi değildir."
    )

async def cmd_diagnostics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛠️ ATLAS AI V9 DIAGNOSTICS\n\n"
        f"Universe: {UNIVERSE_SIZE} USDT pairs\n"
        "Decision TF: 1D / 4H / 1H / 15M\n"
        "2M: entry / anti-miss only\n"
        f"Ready threshold: {READY_SCORE}\n"
        f"Watch threshold: {WATCH_SCORE}\n"
        f"Min RR: 1:{MIN_RR}\n"
        f"Min TP2: {MIN_TP2_PCT}%\n"
        f"Signal chat: {SIGNAL_CHAT}\n"
        "Execution: OFF (paper only)"
    )

# ---------------- SCANNER ----------------

class Scanner:
    def __init__(self, bn: Binance, app: Application):
        self.bn = bn
        self.app = app
        self.last_sent = {}  # symbol -> timestamp

    async def load_symbol(self, symbol):
        # Actual decision stack.
        tasks = [
            self.bn.klines(symbol, "1d", TF_LIMITS["1d"]),
            self.bn.klines(symbol, "4h", TF_LIMITS["4h"]),
            self.bn.klines(symbol, "1h", TF_LIMITS["1h"]),
            self.bn.klines(symbol, "15m", TF_LIMITS["15m"]),
        ]
        d, h4, h1, m15 = await asyncio.gather(*tasks, return_exceptions=True)
        if any(isinstance(x, Exception) or not x for x in (d, h4, h1, m15)):
            return None
        # Only fetch 1m for candidates after initial analysis.
        return {"1d": d, "4h": h4, "1h": h1, "15m": m15}

    async def scan_once(self):
        symbols = await self.bn.symbols(UNIVERSE_SIZE)
        if not symbols:
            log.warning("No symbols received.")
            return

        results = []
        for i in range(0, len(symbols), 8):
            batch = symbols[i:i+8]
            loaded = await asyncio.gather(
                *(self.load_symbol(s) for s in batch),
                return_exceptions=True
            )
            for s, csets in zip(batch, loaded):
                if isinstance(csets, Exception) or not csets:
                    continue
                try:
                    # First pass without 2M.
                    a = analyze(s, csets)
                    if a:
                        results.append((s, a, csets))
                except Exception as e:
                    log.exception("Analyze %s failed: %s", s, e)

        # Only top candidates get the 2M trigger fetch. This keeps the API load sane.
        candidates = sorted(
            [x for x in results if x[1].status in ("READY", "WATCH")],
            key=lambda x: x[1].score,
            reverse=True
        )[:12]

        final = []
        for s, a, csets in candidates:
            try:
                one = await self.bn.klines(s, "1m", TF_LIMITS["1m"])
                csets["2m"] = aggregate_2m(one)
                a2 = analyze(s, csets)
                if a2:
                    final.append(a2)
            except Exception as e:
                log.exception("2M %s failed: %s", s, e)

        # Send only READY. WATCH remains visible in diagnostics/logs.
        ready = [a for a in final if a.status == "READY"]
        ready.sort(key=lambda x: x.score, reverse=True)

        for a in ready[:3]:
            now = time.time()
            # Avoid repeating the same pair too often.
            if now - self.last_sent.get(a.symbol, 0) < 60 * 60:
                continue
            self.last_sent[a.symbol] = now
            try:
                save_signal(a)
                await send_signal(self.app.bot, a)
                log.info("SIGNAL %s %s %s", a.symbol, a.direction, a.score)
            except Exception as e:
                log.exception("Send signal failed: %s", e)

        top = sorted(final, key=lambda x: x.score, reverse=True)[:5]
        log.info(
            "SCAN complete | universe=%d | candidates=%d | ready=%d | top=%s",
            len(symbols), len(final), len(ready),
            [(x.symbol, x.direction, x.score, x.status) for x in top]
        )

    async def loop(self):
        await self.bn.start()
        while True:
            try:
                await self.scan_once()
            except Exception:
                log.exception("Scanner loop error")
            await asyncio.sleep(SCAN_SECONDS)

# ---------------- MAIN ----------------

async def post_init(app: Application):
    bn = Binance()
    scanner = Scanner(bn, app)
    app.bot_data["scanner"] = scanner
    asyncio.create_task(scanner.loop())

async def post_shutdown(app: Application):
    scanner = app.bot_data.get("scanner")
    if scanner:
        await scanner.bn.close()

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required")
    db()
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.add_handler(CommandHandler("test", cmd_test))
    app.add_handler(CommandHandler("diagnostics", cmd_diagnostics))
    app.add_handler(CommandHandler("performance", cmd_performance))
    log.info("ATLAS AI V9 starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
