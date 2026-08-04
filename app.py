
import asyncio
import html
import logging
import math
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse


# =========================================================
# الإعدادات
# =========================================================

BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TZ = ZoneInfo(os.getenv("TZ", "Asia/Riyadh"))
PORT = int(os.getenv("PORT", "8080"))

TIMEFRAMES = tuple(
    tf.strip() for tf in os.getenv("TIMEFRAMES", "1h,4h").split(",")
    if tf.strip() in {"1h", "4h"}
) or ("1h", "4h")

SCAN_SECONDS = max(20, int(os.getenv("SCAN_SECONDS", "60")))
MAX_CONCURRENCY = max(2, int(os.getenv("MAX_CONCURRENCY", "12")))
RADAR_POOL = max(20, int(os.getenv("RADAR_POOL", "140")))
DEEP_CANDIDATES = max(10, int(os.getenv("DEEP_CANDIDATES", "45")))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "750000"))
MAX_ALERTS_PER_SCAN = max(1, int(os.getenv("MAX_ALERTS_PER_SCAN", "6")))
COOLDOWN_MINUTES = max(15, int(os.getenv("COOLDOWN_MINUTES", "180")))

EARLY_THRESHOLD = float(os.getenv("EARLY_THRESHOLD", "60"))
ENTRY_THRESHOLD = float(os.getenv("ENTRY_THRESHOLD", "75"))
EXPLOSION_THRESHOLD = float(os.getenv("EXPLOSION_THRESHOLD", "90"))
WEAKENING_DROP = float(os.getenv("WEAKENING_DROP", "14"))
MIN_DIRECTION_GAP = float(os.getenv("MIN_DIRECTION_GAP", "10"))
FIRST_CANDLE_ENTRY_THRESHOLD = float(os.getenv("FIRST_CANDLE_ENTRY_THRESHOLD", "70"))
REQUIRE_TF_AGREEMENT_FOR_ENTRY = os.getenv(
    "REQUIRE_TF_AGREEMENT_FOR_ENTRY", "true"
).lower() == "true"
ONE_HOUR_MAX_ENTRY_EXTENSION_ATR = float(
    os.getenv("ONE_HOUR_MAX_ENTRY_EXTENSION_ATR", "0.70")
)
FOUR_HOUR_MAX_ENTRY_EXTENSION_ATR = float(
    os.getenv("FOUR_HOUR_MAX_ENTRY_EXTENSION_ATR", "0.45")
)

SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_WEAKENING_ALERTS = os.getenv("SEND_WEAKENING_ALERTS", "true").lower() == "true"
ENABLE_TEST_ENDPOINT = os.getenv("ENABLE_TEST_ENDPOINT", "true").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "data/early_explosion.db")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))
BINANCE_RETRIES = max(1, int(os.getenv("BINANCE_RETRIES", "3")))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("early-explosion")


# =========================================================
# أدوات
# =========================================================

def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def now_local() -> datetime:
    return datetime.now(TZ)


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.5f}".rstrip("0").rstrip(".")
    if value >= 0.01:
        return f"{value:.7f}".rstrip("0").rstrip(".")
    return f"{value:.10f}".rstrip("0").rstrip(".")


def pct(new: float, old: float) -> float:
    return safe_div(new - old, abs(old), 0.0) * 100.0


def ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (length + 1.0)
    value = values[0]
    for item in values[1:]:
        value = alpha * item + (1.0 - alpha) * value
    return value


def atr(rows: list[list[Any]], length: int = 14) -> float:
    if len(rows) < 2:
        return 0.0
    trs = []
    for i in range(1, len(rows)):
        high = float(rows[i][2])
        low = float(rows[i][3])
        prev_close = float(rows[i - 1][4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    window = trs[-length:]
    return sum(window) / max(1, len(window))


def stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((x - mean) ** 2 for x in values) / len(values))


def stage_title(stage: str, direction: str) -> str:
    side = "شراء" if direction == "BUY" else "بيع"
    return {
        "EARLY": f"🟡 استعداد مبكر جدًا — {side}",
        "ENTRY": f"🟠 دخول الآن — {side}",
        "EXPLOSION": f"🔥 بداية الانفجار — {side}",
        "WEAKENING": f"⚠️ ضعف الزخم — {side}",
    }[stage]


def stage_rank(stage: str) -> int:
    return {"EARLY": 1, "ENTRY": 2, "EXPLOSION": 3, "WEAKENING": 4}.get(stage, 0)


# =========================================================
# نموذج الإشارة
# =========================================================

@dataclass
class Signal:
    symbol: str
    timeframe: str
    direction: str
    stage: str
    price: float
    score: float
    opposite_score: float
    score_change: float
    active_count: int
    pressure_score: float
    volume_acceleration: float
    price_acceleration: float
    oi_change: float
    cvd_bias: float
    real_delta: float
    orderbook_imbalance: float
    absorption: float
    iceberg: float
    spoofing_risk: float
    liquidation_pressure: float
    breakout: bool
    compression: float
    ema200_context: float
    entry_low: float
    entry_high: float
    stop: float
    tp1: float
    tp2: float
    tp3: float
    reasons: list[str]
    created_at: str


# =========================================================
# قاعدة البيانات
# =========================================================

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    direction TEXT NOT NULL,
    stage TEXT NOT NULL,
    price REAL NOT NULL,
    score REAL NOT NULL,
    opposite_score REAL NOT NULL,
    score_change REAL NOT NULL,
    active_count INTEGER NOT NULL,
    pressure_score REAL,
    volume_acceleration REAL,
    price_acceleration REAL,
    oi_change REAL,
    cvd_bias REAL,
    real_delta REAL,
    orderbook_imbalance REAL,
    absorption REAL,
    iceberg REAL,
    spoofing_risk REAL,
    liquidation_pressure REAL,
    breakout INTEGER,
    compression REAL,
    ema200_context REAL,
    entry_low REAL,
    entry_high REAL,
    stop REAL,
    tp1 REAL,
    tp2 REAL,
    tp3 REAL,
    reasons TEXT,
    created_at TEXT NOT NULL,
    entered_at TEXT,
    tp1_at TEXT,
    tp2_at TEXT,
    tp3_at TEXT,
    stop_at TEXT,
    best_price REAL,
    worst_price REAL,
    mfe_pct REAL DEFAULT 0,
    mae_pct REAL DEFAULT 0,
    outcome TEXT,
    status TEXT DEFAULT 'OPEN'
);

CREATE INDEX IF NOT EXISTS idx_signal_key
ON signals(symbol, timeframe, direction, stage, created_at);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_no INTEGER,
    symbols INTEGER,
    candidates INTEGER,
    analyzed INTEGER,
    alerts INTEGER,
    seconds REAL,
    error TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS rejected_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    direction TEXT,
    stage TEXT,
    score REAL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rejected_created
ON rejected_signals(created_at);

CREATE TABLE IF NOT EXISTS alert_states (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    direction TEXT NOT NULL,
    sent_early INTEGER DEFAULT 0,
    sent_entry INTEGER DEFAULT 0,
    sent_explosion INTEGER DEFAULT 0,
    sent_weakening INTEGER DEFAULT 0,
    below_count INTEGER DEFAULT 0,
    last_score REAL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(symbol, timeframe)
);
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()


async def save_signal(sig: Signal) -> None:
    values = asdict(sig)
    values["reasons"] = " | ".join(sig.reasons)
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"INSERT INTO signals ({columns}) VALUES ({placeholders})",
            tuple(values.values()),
        )
        await db.commit()


async def save_checkpoint(scan_no: int, symbols: int, candidates: int,
                          analyzed: int, alerts: int, seconds: float,
                          error: str | None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO checkpoints
               (scan_no,symbols,candidates,analyzed,alerts,seconds,error,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (scan_no, symbols, candidates, analyzed, alerts, seconds, error,
             now_local().isoformat()),
        )
        await db.commit()


async def claim_stage_once(
    symbol: str,
    timeframe: str,
    direction: str,
    stage: str,
    score: float,
) -> bool:
    """
    يمنع تكرار المرحلة نفسها لنفس الحركة، ويستمر المنع حتى بعد إعادة تشغيل Railway.
    تعاد تهيئة الحالة فقط بعد هدوء الدرجة تحت الحد المبكر لعدة فحوص.
    """
    column = {
        "EARLY": "sent_early",
        "ENTRY": "sent_entry",
        "EXPLOSION": "sent_explosion",
        "WEAKENING": "sent_weakening",
    }[stage]

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        row = await (
            await db.execute(
                "SELECT * FROM alert_states WHERE symbol=? AND timeframe=?",
                (symbol, timeframe),
            )
        ).fetchone()

        ts = now_local().isoformat()
        if row is None:
            values = {
                "sent_early": 0,
                "sent_entry": 0,
                "sent_explosion": 0,
                "sent_weakening": 0,
            }
            values[column] = 1
            await db.execute(
                """INSERT INTO alert_states
                   (symbol,timeframe,direction,sent_early,sent_entry,
                    sent_explosion,sent_weakening,below_count,last_score,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    symbol, timeframe, direction,
                    values["sent_early"], values["sent_entry"],
                    values["sent_explosion"], values["sent_weakening"],
                    0, score, ts,
                ),
            )
            await db.commit()
            return True

        # اتجاه جديد قوي يبدأ دورة جديدة بدل خلطه مع الحركة السابقة.
        if row["direction"] != direction:
            await db.execute(
                f"""UPDATE alert_states
                    SET direction=?, sent_early=0, sent_entry=0,
                        sent_explosion=0, sent_weakening=0,
                        below_count=0, last_score=?, updated_at=?,
                        {column}=1
                    WHERE symbol=? AND timeframe=?""",
                (direction, score, ts, symbol, timeframe),
            )
            await db.commit()
            return True

        if int(row[column] or 0) == 1:
            await db.execute(
                """UPDATE alert_states
                   SET last_score=?,below_count=0,updated_at=?
                   WHERE symbol=? AND timeframe=?""",
                (score, ts, symbol, timeframe),
            )
            await db.commit()
            return False

        # لا نرجع إلى مرحلة أقل بعد دخول أو انفجار.
        if stage == "EARLY" and (row["sent_entry"] or row["sent_explosion"]):
            await db.commit()
            return False
        if stage == "ENTRY" and row["sent_explosion"]:
            await db.commit()
            return False

        await db.execute(
            f"""UPDATE alert_states
                SET {column}=1,last_score=?,below_count=0,updated_at=?
                WHERE symbol=? AND timeframe=?""",
            (score, ts, symbol, timeframe),
        )
        await db.commit()
        return True


async def update_quiet_state(
    symbol: str,
    timeframe: str,
    max_score: float,
) -> None:
    """
    بعد ثلاثة فحوص هادئة متتالية تحت الحد المبكر بـ8 نقاط،
    يسمح بدورة جديدة. هذا يمنع إعادة التنبيه بسبب اهتزاز الدرجة.
    """
    quiet_limit = EARLY_THRESHOLD - 8.0
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                "SELECT below_count FROM alert_states WHERE symbol=? AND timeframe=?",
                (symbol, timeframe),
            )
        ).fetchone()
        if row is None:
            return

        if max_score < quiet_limit:
            count = int(row["below_count"] or 0) + 1
            if count >= 3:
                await db.execute(
                    """DELETE FROM alert_states
                       WHERE symbol=? AND timeframe=?""",
                    (symbol, timeframe),
                )
            else:
                await db.execute(
                    """UPDATE alert_states
                       SET below_count=?,last_score=?,updated_at=?
                       WHERE symbol=? AND timeframe=?""",
                    (count, max_score, now_local().isoformat(), symbol, timeframe),
                )
        else:
            await db.execute(
                """UPDATE alert_states
                   SET below_count=0,last_score=?,updated_at=?
                   WHERE symbol=? AND timeframe=?""",
                (max_score, now_local().isoformat(), symbol, timeframe),
            )
        await db.commit()


async def record_rejection(
    symbol: str,
    timeframe: str,
    direction: str | None,
    stage: str | None,
    score: float,
    reason: str,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO rejected_signals
               (symbol,timeframe,direction,stage,score,reason,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (
                symbol, timeframe, direction, stage, score,
                reason, now_local().isoformat(),
            ),
        )
        await db.commit()


# =========================================================
# Binance
# =========================================================

class BinanceClient:
    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None
        self.exchange_cache: tuple[float, list[str]] | None = None

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.session:
            raise RuntimeError("Binance client not started")
        last_error: Exception | None = None
        for attempt in range(BINANCE_RETRIES):
            try:
                async with self.session.get(
                    f"{BINANCE_BASE}{path}", params=params
                ) as response:
                    text = await response.text()
                    if response.status != 200:
                        raise RuntimeError(f"Binance {response.status}: {text[:200]}")
                    return await response.json()
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.6 * (attempt + 1))
        raise RuntimeError(f"Binance request failed: {last_error}")

    async def symbols(self) -> list[str]:
        now = time.monotonic()
        if self.exchange_cache and now - self.exchange_cache[0] < 3600:
            return self.exchange_cache[1]
        info = await self.get("/fapi/v1/exchangeInfo")
        symbols = [
            item["symbol"] for item in info.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
        ]
        self.exchange_cache = (now, symbols)
        return symbols

    async def tickers(self) -> list[dict[str, Any]]:
        return await self.get("/fapi/v1/ticker/24hr")

    async def klines(self, symbol: str, interval: str, limit: int = 220):
        return await self.get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )

    async def open_interest_history(self, symbol: str, period: str):
        return await self.get(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": 8},
        )

    async def depth(self, symbol: str):
        return await self.get(
            "/fapi/v1/depth", {"symbol": symbol, "limit": 100}
        )

    async def agg_trades(self, symbol: str):
        return await self.get(
            "/fapi/v1/aggTrades", {"symbol": symbol, "limit": 500}
        )

    async def force_orders(self, symbol: str):
        # Public endpoint may be unavailable in some regions/accounts.
        try:
            return await self.get(
                "/fapi/v1/allForceOrders", {"symbol": symbol, "limit": 50}
            )
        except Exception:
            return []


# =========================================================
# التحليل
# =========================================================

def candle_features(rows: list[list[Any]]) -> dict[str, float | bool]:
    opens = [float(x[1]) for x in rows]
    highs = [float(x[2]) for x in rows]
    lows = [float(x[3]) for x in rows]
    closes = [float(x[4]) for x in rows]
    volumes = [float(x[5]) for x in rows]
    quote_volumes = [float(x[7]) for x in rows]
    taker_buy_quote = [float(x[10]) for x in rows]

    price = closes[-1]
    atr_value = atr(rows, 14)
    atr_pct = safe_div(atr_value, price) * 100

    recent_returns = [pct(closes[i], closes[i - 1]) for i in range(len(closes)-5, len(closes))]
    prior_returns = [pct(closes[i], closes[i - 1]) for i in range(len(closes)-10, len(closes)-5)]
    recent_speed = sum(recent_returns[-3:])
    prior_speed = sum(prior_returns[-3:])
    price_accel = recent_speed - prior_speed

    avg_volume = sum(volumes[-21:-1]) / 20
    v1 = safe_div(volumes[-1], avg_volume, 1)
    v2 = safe_div(volumes[-2], avg_volume, 1)
    v3 = safe_div(volumes[-3], avg_volume, 1)
    volume_accel = (v1 - v2) + 0.5 * (v2 - v3)

    ranges = [highs[i] - lows[i] for i in range(-20, 0)]
    range_pct = safe_div(sum(ranges) / len(ranges), price) * 100
    compression = clamp(100 - safe_div(range_pct, max(atr_pct, 1e-9)) * 55)

    breakout_age_up = 99
    breakout_age_down = 99
    breakout_level_up = 0.0
    breakout_level_down = 0.0

    for age in range(0, 6):
        idx = len(closes) - 1 - age
        if idx < 12:
            break
        prior_high = max(highs[idx-10:idx])
        prior_low = min(lows[idx-10:idx])

        if breakout_age_up == 99 and closes[idx] > prior_high:
            breakout_age_up = age
            breakout_level_up = prior_high

        if breakout_age_down == 99 and closes[idx] < prior_low:
            breakout_age_down = age
            breakout_level_down = prior_low

    breakout_up = breakout_age_up == 0
    breakout_down = breakout_age_down == 0

    extension_up_atr = (
        safe_div(price - breakout_level_up, atr_value)
        if breakout_level_up > 0 else 0.0
    )
    extension_down_atr = (
        safe_div(breakout_level_down - price, atr_value)
        if breakout_level_down > 0 else 0.0
    )
    move_3_atr = safe_div(closes[-1] - closes[-4], atr_value) if len(closes) >= 4 else 0.0

    ema200 = ema(closes[-210:], 200)
    ema_context = safe_div(price - ema200, max(atr_value, 1e-12))

    buy_quote = sum(taker_buy_quote[-5:])
    total_quote = sum(quote_volumes[-5:])
    sell_quote = max(total_quote - buy_quote, 0)
    real_delta = safe_div(buy_quote - sell_quote, max(total_quote, 1)) * 100

    cvd_now = 0.0
    cvd_prev = 0.0
    for i in range(-10, 0):
        delta = 2 * taker_buy_quote[i] - quote_volumes[i]
        cvd_now += delta
        if i < -5:
            cvd_prev += delta
    cvd_bias = safe_div(cvd_now, max(sum(quote_volumes[-10:]), 1)) * 100
    cvd_accel = safe_div(cvd_now - 2 * cvd_prev, max(sum(quote_volumes[-10:]), 1)) * 100

    body = closes[-1] - opens[-1]
    lower_wick = min(opens[-1], closes[-1]) - lows[-1]
    upper_wick = highs[-1] - max(opens[-1], closes[-1])

    return {
        "price": price,
        "atr": atr_value,
        "atr_pct": atr_pct,
        "price_accel": price_accel,
        "volume_ratio": v1,
        "volume_accel": volume_accel,
        "compression": compression,
        "breakout_up": breakout_up,
        "breakout_down": breakout_down,
        "breakout_age_up": float(breakout_age_up),
        "breakout_age_down": float(breakout_age_down),
        "extension_up_atr": extension_up_atr,
        "extension_down_atr": extension_down_atr,
        "move_3_atr": move_3_atr,
        "ema_context": ema_context,
        "real_delta": real_delta,
        "cvd_bias": cvd_bias,
        "cvd_accel": cvd_accel,
        "body_atr": safe_div(body, atr_value),
        "lower_wick_atr": safe_div(lower_wick, atr_value),
        "upper_wick_atr": safe_div(upper_wick, atr_value),
    }

def orderbook_features(depth: dict[str, Any]) -> dict[str, float]:
    bids = [(float(p), float(q)) for p, q in depth.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in depth.get("asks", [])]
    bid_notional = sum(p * q for p, q in bids[:30])
    ask_notional = sum(p * q for p, q in asks[:30])
    imbalance = safe_div(bid_notional - ask_notional, bid_notional + ask_notional) * 100

    bid_sizes = [p * q for p, q in bids[:50]]
    ask_sizes = [p * q for p, q in asks[:50]]
    bid_med = sorted(bid_sizes)[len(bid_sizes)//2] if bid_sizes else 0
    ask_med = sorted(ask_sizes)[len(ask_sizes)//2] if ask_sizes else 0
    bid_walls = sum(1 for x in bid_sizes if bid_med and x > bid_med * 6)
    ask_walls = sum(1 for x in ask_sizes if ask_med and x > ask_med * 6)

    # تقدير محافظ، وليس كشفًا يقينيًا للأوامر الوهمية.
    spoofing_risk = clamp((bid_walls + ask_walls) * 8)
    iceberg_bias = clamp((bid_walls - ask_walls) * 12, -100, 100)
    return {
        "imbalance": imbalance,
        "spoofing_risk": spoofing_risk,
        "iceberg_bias": iceberg_bias,
    }


def trade_flow_features(trades: list[dict[str, Any]]) -> dict[str, float]:
    buy = sell = 0.0
    largest_buy = largest_sell = 0.0
    for trade in trades:
        notional = float(trade.get("p", 0)) * float(trade.get("q", 0))
        # m=True means buyer was maker => aggressive sell.
        if trade.get("m"):
            sell += notional
            largest_sell = max(largest_sell, notional)
        else:
            buy += notional
            largest_buy = max(largest_buy, notional)
    total = buy + sell
    delta = safe_div(buy - sell, total) * 100
    absorption = clamp(
        safe_div(min(buy, sell), max(buy, sell, 1)) * 100
    )
    iceberg = clamp(
        safe_div(largest_buy - largest_sell, max(largest_buy, largest_sell, 1)) * 100,
        -100, 100,
    )
    return {"trade_delta": delta, "absorption": absorption, "iceberg": iceberg}


def liquidation_features(items: list[dict[str, Any]]) -> float:
    buy_liq = sell_liq = 0.0
    for item in items:
        order = item.get("o", item)
        notional = float(order.get("ap", order.get("p", 0)) or 0) * float(order.get("q", 0) or 0)
        # SELL liquidation usually closes a long; BUY closes a short.
        if order.get("S") == "BUY":
            buy_liq += notional
        else:
            sell_liq += notional
    return safe_div(buy_liq - sell_liq, buy_liq + sell_liq) * 100


def weighted_score(direction: str, f: dict[str, float | bool]) -> tuple[float, list[str], int]:
    sign = 1 if direction == "BUY" else -1
    reasons: list[str] = []
    active = 0
    score = 0.0

    def add(points: float, condition: bool, reason: str) -> None:
        nonlocal score, active
        if condition:
            score += points
            active += 1
            reasons.append(reason)

    # لا RSI ولا MACD ولا Stoch RSI ولا KDJ.
    add(12, float(f["compression"]) >= 55, "ضغط سعري وضيق في النطاق")
    add(16, sign * float(f["volume_accel"]) >= 0.15 or float(f["volume_ratio"]) >= 1.25,
        "تسارع واضح في الحجم")
    add(13, sign * float(f["price_accel"]) >= 0.10, "تسارع السعر في اتجاه الإشارة")
    add(15, sign * float(f["oi_change"]) >= 0.10, "ارتفاع داعم في العقود المفتوحة")
    add(11, sign * float(f["cvd_bias"]) >= 2.0, "تدفق CVD يدعم الاتجاه")
    add(10, sign * float(f["real_delta"]) >= 3.0, "الدلتا الحقيقية تؤكد السيطرة")
    add(8, sign * float(f["orderbook"]) >= 5.0, "اختلال دفتر الأوامر لصالح الاتجاه")
    add(5, sign * float(f["trade_delta"]) >= 4.0, "تدفق الصفقات السوقية داعم")
    add(4, float(f["absorption"]) >= 45 and sign * float(f["trade_delta"]) >= 0,
        "امتصاص معاكس يدعم استمرار الحركة")
    add(3, sign * float(f["iceberg"]) >= 10, "نشاط أوامر مخفية محتمل")
    add(3, sign * float(f["liquidations"]) >= 8, "التصفيات تدعم التسارع")
    breakout = bool(f["breakout_up"] if direction == "BUY" else f["breakout_down"])
    add(8, breakout, "كسر قمة/قاع البنية الصغيرة")
    add(2, sign * float(f["ema_context"]) >= -0.35, "السعر في سياق مقبول قرب EMA200")

    # خصم مخاطر الأوامر الوهمية، دون تحويله إلى شرط تعطيل جامد.
    score -= min(8.0, float(f["spoofing_risk"]) * 0.08)
    return clamp(score), reasons, active


def choose_stage(score: float, previous_score: float, previous_stage: str | None) -> str | None:
    change = score - previous_score

    # ضعف الزخم لا يعني انعكاس الاتجاه. لا نرسله لمجرد هبوط الدرجة
    # بينما الإشارة ما زالت قوية؛ يجب أن تهبط دون مستوى "دخول الآن".
    if (
        previous_stage in {"ENTRY", "EXPLOSION"}
        and change <= -WEAKENING_DROP
        and score < ENTRY_THRESHOLD
    ):
        return "WEAKENING"

    if score >= EXPLOSION_THRESHOLD:
        return "EXPLOSION"
    if score >= ENTRY_THRESHOLD:
        return "ENTRY"
    if score >= EARLY_THRESHOLD:
        return "EARLY"
    return None




def timing_gate(
    direction: str,
    stage: str,
    timeframe: str,
    f: dict[str, float | bool],
) -> tuple[bool, str]:
    """
    دخول الآن = أول شمعة كسر فقط.
    مرحلة الانفجار تأكيد وليست دعوة دخول جديدة.
    """
    if direction == "BUY":
        age = int(float(f["breakout_age_up"]))
        extension = float(f["extension_up_atr"])
        move_3 = float(f["move_3_atr"])
        body_atr = float(f["body_atr"])
    else:
        age = int(float(f["breakout_age_down"]))
        extension = float(f["extension_down_atr"])
        move_3 = -float(f["move_3_atr"])
        body_atr = -float(f["body_atr"])

    if stage in {"EARLY", "WEAKENING"}:
        return True, ""

    if stage == "ENTRY":
        if age != 0:
            return False, f"دخول الآن مسموح في أول شمعة فقط؛ عمر الكسر {age}"

        max_extension = (
            ONE_HOUR_MAX_ENTRY_EXTENSION_ATR
            if timeframe == "1h"
            else FOUR_HOUR_MAX_ENTRY_EXTENSION_ATR
        )
        if extension > max_extension:
            return False, (
                f"فات الدخول: امتداد {timeframe.upper()} "
                f"{extension:.2f} ATR أكبر من {max_extension:.2f}"
            )

        max_body = 0.95 if timeframe == "1h" else 0.72
        if body_atr > max_body:
            return False, f"شمعة الكسر ممتدة بالفعل: جسمها {body_atr:.2f} ATR"

        max_move = 1.90 if timeframe == "1h" else 1.40
        if move_3 > max_move:
            return False, f"الحركة السابقة استهلكت {move_3:.2f} ATR"
        return True, ""

    if stage == "EXPLOSION":
        max_age = 1
        max_extension = 1.35 if timeframe == "1h" else 0.95
        if age > max_age:
            return False, f"تأكيد الانفجار متأخر؛ عمر الكسر {age}"
        if extension > max_extension:
            return False, f"الحركة ممتدة {extension:.2f} ATR"
        return True, ""

    return False, "مرحلة أو فريم غير مدعوم"


def validation_gate(
    direction: str,
    stage: str,
    f: dict[str, float | bool],
) -> tuple[bool, str]:
    """
    فحص آخر لحظة لمنع إرسال بيع بعد انقلاب الشمعة إلى شراء، أو العكس.
    لا يستخدم RSI/MACD؛ يعتمد على السعر والتدفقات الحالية.
    """
    if stage not in {"ENTRY", "EXPLOSION"}:
        return True, ""

    sign = 1 if direction == "BUY" else -1
    body = sign * float(f["body_atr"])
    price_accel = sign * float(f["price_accel"])
    cvd = sign * float(f["cvd_bias"])
    delta = sign * float(f["real_delta"])
    trades = sign * float(f["trade_delta"])
    book = sign * float(f["orderbook"])

    # شمعة معاكسة واضحة تلغي الإشارة فورًا.
    if body <= -0.30:
        return False, f"انعكاس لحظي: جسم الشمعة المعاكسة {abs(body):.2f} ATR"

    contradictions = 0
    labels: list[str] = []
    for condition, label in (
        (price_accel < -0.05, "تسارع السعر انعكس"),
        (cvd < -1.0, "CVD أصبح معاكسًا"),
        (delta < -1.5, "الدلتا الحقيقية أصبحت معاكسة"),
        (trades < -2.0, "الصفقات السوقية انعكست"),
        (book < -4.0, "دفتر الأوامر أصبح معاكسًا"),
    ):
        if condition:
            contradictions += 1
            labels.append(label)

    if contradictions >= 2:
        return False, "انعكاس قبل الإرسال: " + " + ".join(labels[:3])

    return True, ""


def timeframe_alignment(
    analyses: dict[str, dict[str, Any]],
    target_tf: str,
    direction: str,
    stage: str,
) -> tuple[bool, str]:
    """
    يمنع شراء وبيع متعارضين لنفس العملة.
    دخول الآن وبداية الانفجار يحتاجان توافق 1H و4H.
    الاستعداد يسمح إذا كان الفريم الآخر محايدًا، لكنه يمنع إذا كان معاكسًا.
    """
    other_tf = "4h" if target_tf == "1h" else "1h"
    other = analyses.get(other_tf)
    if not other:
        return False, "بيانات الفريم الآخر غير متوفرة"

    other_direction = other["direction"]
    other_score = float(other["score"])
    other_gap = float(other["score"] - other["opposite"])

    if other_direction != direction and other_score >= EARLY_THRESHOLD and other_gap >= MIN_DIRECTION_GAP:
        return False, (
            f"تعارض الفريمات: {target_tf.upper()} {direction} "
            f"مقابل {other_tf.upper()} {other_direction}"
        )

    if stage in {"ENTRY", "EXPLOSION"} and REQUIRE_TF_AGREEMENT_FOR_ENTRY:
        if other_direction != direction:
            return False, f"لا يوجد توافق {target_tf.upper()} و{other_tf.upper()}"
        # يكفي أن يكون الفريم الآخر داعمًا، ولا يلزم أن يصل لمرحلة دخول.
        same_direction_support = (
            float(other["buy_score"]) if direction == "BUY"
            else float(other["sell_score"])
        )
        if same_direction_support < 48:
            return False, (
                f"دعم الفريم الآخر ضعيف: {other_tf.upper()} "
                f"{same_direction_support:.1f}%"
            )

    return True, f"توافق {target_tf.upper()} و{other_tf.upper()} على الاتجاه"

def weakening_reasons(direction: str, f: dict[str, float | bool], score_change: float) -> list[str]:
    sign = 1 if direction == "BUY" else -1
    reasons: list[str] = []

    if score_change <= -WEAKENING_DROP:
        reasons.append(f"هبوط درجة الزخم بمقدار {abs(score_change):.1f} نقطة")
    if sign * float(f["price_accel"]) <= 0:
        reasons.append("توقف أو انعكاس تسارع السعر")
    if float(f["volume_accel"]) <= 0:
        reasons.append("تباطؤ تسارع الحجم")
    if sign * float(f["cvd_bias"]) < 2:
        reasons.append("ضعف دعم CVD للاتجاه")
    if sign * float(f["real_delta"]) < 3:
        reasons.append("تراجع سيطرة الدلتا الحقيقية")
    if sign * float(f["oi_change"]) <= 0:
        reasons.append("العقود المفتوحة لم تعد تدعم الحركة")
    if sign * float(f["orderbook"]) <= 0:
        reasons.append("اختلال دفتر الأوامر لم يعد داعمًا")
    if not bool(f["breakout_up"] if direction == "BUY" else f["breakout_down"]):
        reasons.append("فشل استمرار كسر البنية")

    return reasons[:7] or ["تراجع واضح في زخم الإشارة السابقة"]

def trade_plan(direction: str, price: float, atr_value: float,
               recent_high: float, recent_low: float) -> tuple[float, ...]:
    buffer = max(atr_value * 0.10, price * 0.0005)
    max_risk = max(atr_value * 1.25, price * 0.003)

    if direction == "BUY":
        entry_low = price - buffer
        entry_high = price + buffer * 0.30
        stop = max(recent_low - buffer, price - max_risk)
        risk = max(price - stop, atr_value * 0.45)
        return entry_low, entry_high, stop, price + risk, price + 2*risk, price + 3*risk

    entry_low = price - buffer * 0.30
    entry_high = price + buffer
    stop = min(recent_high + buffer, price + max_risk)
    risk = max(stop - price, atr_value * 0.45)
    return entry_low, entry_high, stop, price - risk, price - 2*risk, price - 3*risk


# =========================================================
# تيليجرام
# =========================================================

async def send_telegram(session: aiohttp.ClientSession, message: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    async with session.post(url, json=payload) as response:
        body = await response.text()
        if response.status != 200:
            raise RuntimeError(f"Telegram {response.status}: {body[:300]}")



def opportunity_quality(sig: Signal) -> tuple[str, float]:
    timing_bonus = 18.0 if any("عمر الكسر 0" in x for x in sig.reasons) else 8.0
    alignment_bonus = 14.0 if any("توافق" in x for x in sig.reasons) else 0.0
    flow = clamp(
        (
            abs(sig.cvd_bias)
            + abs(sig.real_delta)
            + abs(sig.orderbook_imbalance) * 0.6
        ) / 3.0
    )
    risk = abs(sig.price - sig.stop)
    risk_pct = safe_div(risk, sig.price) * 100
    risk_bonus = 12.0 if risk_pct <= 3.0 else 7.0 if risk_pct <= 5.0 else 2.0
    value = clamp(sig.score * 0.52 + timing_bonus + alignment_bonus + flow * 0.12 + risk_bonus)
    if value >= 92:
        return "S+", value
    if value >= 86:
        return "A+", value
    if value >= 80:
        return "A", value
    if value >= 72:
        return "B", value
    return "C", value

def build_message(sig: Signal) -> str:
    side_pressure = sig.score
    opposite = sig.opposite_score
    stars = max(1, min(5, math.ceil(sig.score / 20)))
    quality = "⭐" * stars
    quality_grade, quality_score = opportunity_quality(sig)
    reasons = "\n".join(f"✅ {html.escape(x)}" for x in sig.reasons[:7])
    bn = f"https://www.binance.com/en/futures/{sig.symbol}"
    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{sig.symbol}.P"
    time_text = datetime.fromisoformat(sig.created_at).strftime("%d-%m-%Y %H:%M:%S")

    plan = ""
    weakening_note = ""
    if sig.stage == "WEAKENING":
        weakening_note = "\nℹ️ هذا تنبيه ضعف للاتجاه السابق، وليس إشارة انعكاس أو دخول معاكس.\n"
    if sig.stage in {"ENTRY", "EXPLOSION"}:
        plan = f"""
🎯 الدخول: <b>{fmt_price(sig.entry_low)} – {fmt_price(sig.entry_high)}</b>
🛑 وقف الخسارة: <b>{fmt_price(sig.stop)}</b>
✅ TP1: <b>{fmt_price(sig.tp1)}</b> (1R)
✅ TP2: <b>{fmt_price(sig.tp2)}</b> (2R)
✅ TP3: <b>{fmt_price(sig.tp3)}</b> (3R)
"""

    return f"""<b>{stage_title(sig.stage, sig.direction)}</b>

💰 العملة: <b>#{sig.symbol}.P</b>
⏰ الفريم: <b>{sig.timeframe.upper()}</b>
💵 السعر: <b>{fmt_price(sig.price)}</b>

💥 احتمال الانفجار: <b>{sig.score:.1f}%</b>
🟢 ضغط الشراء: <b>{side_pressure if sig.direction == 'BUY' else opposite:.1f}%</b>
🔴 ضغط البيع: <b>{opposite if sig.direction == 'BUY' else side_pressure:.1f}%</b>
📈 تغير الدرجة: <b>{sig.score_change:+.1f}</b>
🎯 توافق العوامل: <b>{sig.active_count}/13</b>
🏆 جودة الفرصة: <b>{quality_grade}</b> ({quality_score:.1f}%)
{quality}
{weakening_note}{plan}
📊 أسباب التنبيه
{reasons}

🕒 {time_text} (السعودية)
🔗 <a href="{bn}">Binance</a> | <a href="{tv}">TradingView</a>

⚠️ خطة إحصائية وليست ضمانًا أو تنفيذًا تلقائيًا."""


# =========================================================
# المحرك
# =========================================================

class Engine:
    def __init__(self) -> None:
        self.client = BinanceClient()
        self.telegram: aiohttp.ClientSession | None = None
        self.running = False
        self.task: asyncio.Task | None = None
        self.tracker_task: asyncio.Task | None = None
        self.scan_no = 0
        self.last_scan: str | None = None
        self.last_error: str | None = None
        self.symbol_count = 0
        self.candidate_count = 0
        self.alert_count = 0
        self.state: dict[tuple[str, str, str], dict[str, Any]] = {}

    async def start(self) -> None:
        await init_db()
        await self.client.start()
        self.telegram = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        )
        self.running = True
        if SEND_STARTUP_MESSAGE and BOT_TOKEN and CHAT_ID:
            await send_telegram(
                self.telegram,
                "✅ <b>Ahmed Early Explosion Trader v2.0 FIRST-CANDLE بدأ العمل</b>\n\n"
                f"⏰ الفريمات: {' / '.join(x.upper() for x in TIMEFRAMES)}\n"
                "🟡 استعداد مبكر جدًا\n🟠 دخول الآن\n🔥 بداية الانفجار\n⚠️ ضعف الزخم\n\n"
                "❌ لا يستخدم RSI أو MACD أو Stoch RSI أو KDJ.",
            )
        self.task = asyncio.create_task(self.loop())
        self.tracker_task = asyncio.create_task(self.track_positions())

    async def close(self) -> None:
        self.running = False
        for task in (self.task, self.tracker_task):
            if task:
                task.cancel()
        await self.client.close()
        if self.telegram:
            await self.telegram.close()

    async def loop(self) -> None:
        while self.running:
            started = time.monotonic()
            self.scan_no += 1
            alerts = analyzed = 0
            error = None
            try:
                alerts, analyzed = await asyncio.wait_for(self.scan(), timeout=max(50, SCAN_SECONDS))
                self.last_error = None
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error = repr(exc)
                self.last_error = error
                log.exception("Scan failed")
            elapsed = time.monotonic() - started
            self.last_scan = now_local().isoformat()
            await save_checkpoint(
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, alerts, elapsed, error,
            )
            log.info(
                "scan=%s symbols=%s candidates=%s analyzed=%s alerts=%s seconds=%.1f",
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, alerts, elapsed,
            )
            await asyncio.sleep(max(5, SCAN_SECONDS - elapsed))

    async def scan(self) -> tuple[int, int]:
        symbols = await self.client.symbols()
        self.symbol_count = len(symbols)
        allowed = set(symbols)
        tickers = await self.client.tickers()

        ranked = []
        for item in tickers:
            symbol = item.get("symbol")
            if symbol not in allowed:
                continue
            quote_volume = float(item.get("quoteVolume", 0) or 0)
            if quote_volume >= MIN_QUOTE_VOLUME:
                ranked.append((quote_volume, symbol))
        ranked.sort(reverse=True)
        radar = [symbol for _, symbol in ranked[:RADAR_POOL]]

        # رادار خفيف: يختار العملات ذات الحجم/الحركة النشطة.
        candidates = []
        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        async def radar_one(symbol: str):
            async with sem:
                try:
                    rows = await self.client.klines(symbol, "1h", 35)
                    f = candle_features(rows)
                    heat = (
                        abs(float(f["price_accel"])) * 12
                        + max(0, float(f["volume_ratio"]) - 1) * 18
                        + max(0, float(f["volume_accel"])) * 15
                        + float(f["compression"]) * 0.15
                    )
                    return heat, symbol
                except Exception:
                    return -1, symbol

        results = await asyncio.gather(*(radar_one(s) for s in radar))
        results.sort(reverse=True)
        candidates = [s for heat, s in results if heat >= 0][:DEEP_CANDIDATES]
        self.candidate_count = len(candidates)

        alerts: list[Signal] = []

        async def analyze_symbol(symbol: str) -> list[Signal]:
            output: list[Signal] = []
            async with sem:
                try:
                    depth_task = asyncio.create_task(self.client.depth(symbol))
                    trades_task = asyncio.create_task(self.client.agg_trades(symbol))
                    liq_task = asyncio.create_task(self.client.force_orders(symbol))
                    kline_tasks = {
                        tf: asyncio.create_task(self.client.klines(symbol, tf, 220))
                        for tf in TIMEFRAMES
                    }
                    oi_tasks = {
                        tf: asyncio.create_task(self.client.open_interest_history(symbol, tf))
                        for tf in TIMEFRAMES
                    }

                    depth, trades, liquidations = await asyncio.gather(
                        depth_task, trades_task, liq_task
                    )
                    ob = orderbook_features(depth)
                    flow = trade_flow_features(trades)
                    liq = liquidation_features(liquidations)

                    analyses: dict[str, dict[str, Any]] = {}

                    # أولًا: نحسب الفريمين كاملين قبل اتخاذ أي قرار.
                    for tf in TIMEFRAMES:
                        rows = await kline_tasks[tf]
                        oi_rows = await oi_tasks[tf]
                        if len(rows) < 210:
                            continue

                        cf = candle_features(rows)
                        oi_values = [
                            float(x.get("sumOpenInterestValue", 0) or 0)
                            for x in oi_rows
                        ]
                        oi_change = (
                            pct(oi_values[-1], oi_values[-3])
                            if len(oi_values) >= 3 else 0.0
                        )

                        common = {
                            **cf,
                            "oi_change": oi_change,
                            "orderbook": ob["imbalance"],
                            "spoofing_risk": ob["spoofing_risk"],
                            "trade_delta": flow["trade_delta"],
                            "absorption": flow["absorption"],
                            "iceberg": (flow["iceberg"] + ob["iceberg_bias"]) / 2,
                            "liquidations": liq,
                        }

                        buy_score, buy_reasons, buy_active = weighted_score("BUY", common)
                        sell_score, sell_reasons, sell_active = weighted_score("SELL", common)
                        direction = "BUY" if buy_score >= sell_score else "SELL"
                        score = max(buy_score, sell_score)
                        opposite = min(buy_score, sell_score)

                        analyses[tf] = {
                            "rows": rows,
                            "cf": cf,
                            "common": common,
                            "oi_change": oi_change,
                            "buy_score": buy_score,
                            "sell_score": sell_score,
                            "buy_reasons": buy_reasons,
                            "sell_reasons": sell_reasons,
                            "buy_active": buy_active,
                            "sell_active": sell_active,
                            "direction": direction,
                            "score": score,
                            "opposite": opposite,
                        }

                    if set(TIMEFRAMES) - set(analyses):
                        return output

                    # إذا كان الفريمان يعطيان اتجاهين قويين متعاكسين، لا نرسل شيئًا.
                    a1 = analyses.get("1h")
                    a4 = analyses.get("4h")
                    if a1 and a4:
                        strong_1h = (
                            a1["score"] >= EARLY_THRESHOLD
                            and a1["score"] - a1["opposite"] >= MIN_DIRECTION_GAP
                        )
                        strong_4h = (
                            a4["score"] >= EARLY_THRESHOLD
                            and a4["score"] - a4["opposite"] >= MIN_DIRECTION_GAP
                        )
                        if strong_1h and strong_4h and a1["direction"] != a4["direction"]:
                            reason = (
                                f"تعارض قوي: 1H {a1['direction']} {a1['score']:.1f}% "
                                f"مقابل 4H {a4['direction']} {a4['score']:.1f}%"
                            )
                            await record_rejection(
                                symbol, "1h/4h", None, None,
                                max(a1["score"], a4["score"]), reason,
                            )
                            return output

                    for tf in TIMEFRAMES:
                        item = analyses[tf]
                        cf = item["cf"]
                        common = item["common"]
                        direction = item["direction"]
                        score = float(item["score"])
                        opposite = float(item["opposite"])
                        reasons = list(
                            item["buy_reasons"]
                            if direction == "BUY" else item["sell_reasons"]
                        )
                        active = int(
                            item["buy_active"]
                            if direction == "BUY" else item["sell_active"]
                        )

                        await update_quiet_state(symbol, tf, score)

                        if score - opposite < MIN_DIRECTION_GAP:
                            await record_rejection(
                                symbol, tf, direction, None, score,
                                "فرق الشراء والبيع غير واضح",
                            )
                            continue

                        key = (symbol, tf, direction)
                        old = self.state.get(key, {})
                        previous_score = float(old.get("score", 0))
                        previous_stage = old.get("stage")

                        stage = choose_stage(score, previous_score, previous_stage)

                        # الوصول إلى دخول الآن في أول شمعة لا ينتظر عتبة 75 إذا
                        # كانت بقية الأدلة قوية ووصلت الدرجة للحد المبكر الخاص.
                        first_breakout = bool(
                            cf["breakout_up"] if direction == "BUY"
                            else cf["breakout_down"]
                        )
                        if (
                            first_breakout
                            and score >= FIRST_CANDLE_ENTRY_THRESHOLD
                            and stage in {None, "EARLY", "ENTRY"}
                        ):
                            stage = "ENTRY"

                        score_change = score - previous_score
                        if stage == "WEAKENING":
                            reasons = weakening_reasons(direction, common, score_change)
                            active = len(reasons)

                        self.state[key] = {
                            "score": score,
                            "stage": stage or previous_stage,
                            "updated": now_local(),
                        }
                        if not stage:
                            continue
                        if stage == "WEAKENING" and not SEND_WEAKENING_ALERTS:
                            continue

                        aligned, alignment_reason = timeframe_alignment(
                            analyses, tf, direction, stage
                        )
                        if not aligned:
                            await record_rejection(
                                symbol, tf, direction, stage, score,
                                alignment_reason,
                            )
                            continue

                        allowed_timing, timing_reason = timing_gate(
                            direction, stage, tf, common
                        )
                        if not allowed_timing:
                            await record_rejection(
                                symbol, tf, direction, stage, score,
                                timing_reason,
                            )
                            log.info(
                                "blocked late signal %s %s %s: %s",
                                symbol, tf, direction, timing_reason,
                            )
                            continue

                        valid, validation_reason = validation_gate(
                            direction, stage, common
                        )
                        if not valid:
                            await record_rejection(
                                symbol, tf, direction, stage, score,
                                validation_reason,
                            )
                            continue

                        if not await claim_stage_once(
                            symbol, tf, direction, stage, score
                        ):
                            await record_rejection(
                                symbol, tf, direction, stage, score,
                                "رسالة مكررة لنفس المرحلة والحركة",
                            )
                            continue

                        price = float(cf["price"])
                        rows = item["rows"]
                        recent_high = max(float(x[2]) for x in rows[-4:])
                        recent_low = min(float(x[3]) for x in rows[-4:])
                        plan = trade_plan(
                            direction, price, float(cf["atr"]),
                            recent_high, recent_low,
                        )

                        if stage in {"ENTRY", "EXPLOSION"}:
                            age = int(float(
                                cf["breakout_age_up"] if direction == "BUY"
                                else cf["breakout_age_down"]
                            ))
                            extension = float(
                                cf["extension_up_atr"] if direction == "BUY"
                                else cf["extension_down_atr"]
                            )
                            reasons = [
                                alignment_reason,
                                f"الإشارة في أول شمعة: عمر الكسر {age}",
                                f"الامتداد بعد الكسر {extension:.2f} ATR",
                                *reasons,
                            ][:7]
                        else:
                            reasons = [alignment_reason, *reasons][:7]

                        sig = Signal(
                            symbol=symbol,
                            timeframe=tf,
                            direction=direction,
                            stage=stage,
                            price=price,
                            score=score,
                            opposite_score=opposite,
                            score_change=score_change,
                            active_count=active,
                            pressure_score=float(cf["compression"]),
                            volume_acceleration=float(cf["volume_accel"]),
                            price_acceleration=float(cf["price_accel"]),
                            oi_change=float(item["oi_change"]),
                            cvd_bias=float(cf["cvd_bias"]),
                            real_delta=float(cf["real_delta"]),
                            orderbook_imbalance=float(ob["imbalance"]),
                            absorption=float(flow["absorption"]),
                            iceberg=float(common["iceberg"]),
                            spoofing_risk=float(ob["spoofing_risk"]),
                            liquidation_pressure=liq,
                            breakout=bool(first_breakout),
                            compression=float(cf["compression"]),
                            ema200_context=float(cf["ema_context"]),
                            entry_low=plan[0],
                            entry_high=plan[1],
                            stop=plan[2],
                            tp1=plan[3],
                            tp2=plan[4],
                            tp3=plan[5],
                            reasons=reasons,
                            created_at=now_local().isoformat(),
                        )
                        old["stage"] = stage
                        self.state[key] = old
                        output.append(sig)

                except Exception as exc:
                    log.warning("analysis failed %s: %s", symbol, exc)
            return output

        batches = await asyncio.gather(*(analyze_symbol(s) for s in candidates))
        for batch in batches:
            alerts.extend(batch)

        alerts.sort(
            key=lambda x: (
                stage_rank(x.stage),
                opportunity_quality(x)[1],
                x.score,
                x.score_change,
            ),
            reverse=True,
        )
        sent = 0
        for sig in alerts[:MAX_ALERTS_PER_SCAN]:
            await save_signal(sig)
            if self.telegram and BOT_TOKEN and CHAT_ID:
                await send_telegram(self.telegram, build_message(sig))
            sent += 1
            self.alert_count += 1
        return sent, len(candidates) * len(TIMEFRAMES)

    async def track_positions(self) -> None:
        while self.running:
            try:
                tickers = await self.client.get("/fapi/v1/ticker/price")
                prices = {x["symbol"]: float(x["price"]) for x in tickers}
                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    rows = await (
                        await db.execute(
                            "SELECT * FROM signals WHERE status='OPEN' AND stage IN ('ENTRY','EXPLOSION')"
                        )
                    ).fetchall()
                    for row in rows:
                        p = prices.get(row["symbol"])
                        if not p:
                            continue
                        direction = row["direction"]
                        mid = (row["entry_low"] + row["entry_high"]) / 2
                        entered = row["entered_at"] is not None
                        in_zone = row["entry_low"] <= p <= row["entry_high"]
                        ts = now_local().isoformat()
                        if not entered and in_zone:
                            await db.execute(
                                """UPDATE signals SET entered_at=?,best_price=?,worst_price=?
                                   WHERE id=?""",
                                (ts, p, p, row["id"]),
                            )
                            entered = True
                        if not entered:
                            continue

                        best = row["best_price"] if row["best_price"] is not None else p
                        worst = row["worst_price"] if row["worst_price"] is not None else p
                        best = max(best, p) if direction == "BUY" else min(best, p)
                        worst = min(worst, p) if direction == "BUY" else max(worst, p)
                        mfe = pct(best, mid) * (1 if direction == "BUY" else -1)
                        mae = pct(worst, mid) * (-1 if direction == "BUY" else 1)

                        hit_stop = p <= row["stop"] if direction == "BUY" else p >= row["stop"]
                        hit1 = p >= row["tp1"] if direction == "BUY" else p <= row["tp1"]
                        hit2 = p >= row["tp2"] if direction == "BUY" else p <= row["tp2"]
                        hit3 = p >= row["tp3"] if direction == "BUY" else p <= row["tp3"]

                        updates = {
                            "best_price": best,
                            "worst_price": worst,
                            "mfe_pct": max(0, mfe),
                            "mae_pct": max(0, mae),
                        }
                        if hit1 and not row["tp1_at"]:
                            updates["tp1_at"] = ts
                        if hit2 and not row["tp2_at"]:
                            updates["tp2_at"] = ts
                        if hit3 and not row["tp3_at"]:
                            updates.update({"tp3_at": ts, "outcome": "TP3", "status": "CLOSED"})
                        elif hit_stop and not row["stop_at"]:
                            outcome = "SL_AFTER_TP" if row["tp1_at"] or hit1 else "SL"
                            updates.update({"stop_at": ts, "outcome": outcome, "status": "CLOSED"})

                        sql = ", ".join(f"{k}=?" for k in updates)
                        await db.execute(
                            f"UPDATE signals SET {sql} WHERE id=?",
                            (*updates.values(), row["id"]),
                        )
                    await db.commit()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("position tracker failed")
            await asyncio.sleep(60)


engine = Engine()


# =========================================================
# واجهة الويب
# =========================================================

@asynccontextmanager
async def lifespan(_: FastAPI):
    await engine.start()
    yield
    await engine.close()


app = FastAPI(title="Ahmed Early Explosion Trader", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "ok": engine.last_error is None,
        "service": "Ahmed Early Explosion Trader v2.0 FIRST-CANDLE",
        "timeframes": TIMEFRAMES,
        "scan_number": engine.scan_no,
        "last_scan": engine.last_scan,
        "last_error": engine.last_error,
        "symbols": engine.symbol_count,
        "candidates": engine.candidate_count,
        "alerts_since_start": engine.alert_count,
        "time": now_local().isoformat(),
    }


@app.get("/test-telegram")
async def test_telegram():
    if not ENABLE_TEST_ENDPOINT:
        raise HTTPException(404)
    if not engine.telegram:
        raise HTTPException(503, "Telegram session is not ready")
    await send_telegram(
        engine.telegram,
        "✅ <b>رسالة اختبار ناجحة</b>\n\n"
        "Ahmed Early Explosion Trader متصل بتيليجرام.\n"
        f"⏰ الفريمات: {' / '.join(x.upper() for x in TIMEFRAMES)}",
    )
    return {"ok": True}


@app.get("/signals")
async def signals(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,))
        ).fetchall()
    return [dict(x) for x in rows]


@app.get("/stats")
async def stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        overall = await (
            await db.execute(
                """SELECT COUNT(*) total,
                          SUM(status='OPEN') open_count,
                          SUM(outcome='TP3') tp3,
                          SUM(outcome='SL') sl,
                          SUM(outcome='SL_AFTER_TP') sl_after_tp,
                          AVG(mfe_pct) avg_mfe,
                          AVG(mae_pct) avg_mae
                   FROM signals"""
            )
        ).fetchone()
        groups = await (
            await db.execute(
                """SELECT timeframe,direction,stage,COUNT(*) cases,
                          SUM(outcome='TP3') tp3,
                          SUM(outcome='SL') sl,
                          AVG(mfe_pct) avg_mfe,
                          AVG(mae_pct) avg_mae
                   FROM signals
                   GROUP BY timeframe,direction,stage
                   ORDER BY cases DESC"""
            )
        ).fetchall()
    return {"overall": dict(overall), "groups": [dict(x) for x in groups]}


@app.get("/rejections")
async def rejections(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM rejected_signals ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        ).fetchall()
    return [dict(x) for x in rows]


@app.get("/checkpoints")
async def checkpoints(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?", (limit,)
            )
        ).fetchall()
    return [dict(x) for x in rows]


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    h = await health()
    s = await stats()
    overall = s["overall"]
    status = "يعمل ✅" if h["ok"] else "يوجد خطأ ⚠️"
    return f"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ahmed Early Explosion Trader</title>
<style>
body{{font-family:Arial;background:#0a1020;color:#eef2ff;margin:0;padding:22px}}
.wrap{{max-width:1050px;margin:auto}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}}
.card{{background:#151d34;border:1px solid #293657;border-radius:16px;padding:17px;margin:12px 0}}
.k{{color:#aab5d8;font-size:13px}} .v{{font-size:25px;font-weight:bold;margin-top:7px}}
a{{color:#8ebaff}} code{{color:#ffd27a}}
</style></head>
<body><div class="wrap">
<h1>Ahmed Early Explosion Trader v2.0 FIRST-CANDLE</h1>
<div class="card"><b>الحالة: {status}</b><br>الفريمات: {' / '.join(x.upper() for x in TIMEFRAMES)}<br>
آخر فحص: {h['last_scan'] or 'لم يبدأ'}<br>الخطأ: {html.escape(str(h['last_error'] or 'لا يوجد'))}</div>
<div class="grid">
<div class="card"><div class="k">رقم الفحص</div><div class="v">{h['scan_number']}</div></div>
<div class="card"><div class="k">عقود Binance</div><div class="v">{h['symbols']}</div></div>
<div class="card"><div class="k">المرشحون</div><div class="v">{h['candidates']}</div></div>
<div class="card"><div class="k">التنبيهات</div><div class="v">{h['alerts_since_start']}</div></div>
<div class="card"><div class="k">كل الحالات</div><div class="v">{overall.get('total') or 0}</div></div>
<div class="card"><div class="k">المفتوحة</div><div class="v">{overall.get('open_count') or 0}</div></div>
</div>
<div class="card">
<a href="/health">health</a> · <a href="/signals">signals</a> ·
<a href="/stats">stats</a> · <a href="/rejections">rejections</a> · <a href="/checkpoints">checkpoints</a> ·
<a href="/test-telegram">test-telegram</a>
</div>
<div class="card">لا يستخدم RSI أو MACD أو Stoch RSI أو KDJ. يعتمد على الضغط،
تسارع الحجم والسعر، OI، CVD، Real Delta، دفتر الأوامر، الامتصاص،
الأوامر المخفية المحتملة، فلتر الأوامر الوهمية، التصفيات، والبنية.</div>
</div></body></html>"""


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
