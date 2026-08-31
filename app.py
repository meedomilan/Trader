import os
import time
import json
import threading
import queue
import sqlite3
import html
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
import websocket
from flask import Flask, jsonify

REST_BASE = "https://fapi.binance.com"
WS_MARKET = "wss://fstream.binance.com/market/stream"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
MIN_QV = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "500000"))
RADAR_POOL = int(os.getenv("RADAR_POOL", "240"))
MAX_ALERTS = int(os.getenv("MAX_ALERTS_PER_SCAN", "6"))
COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))
SYMBOL_COOLDOWN_MIN = int(os.getenv("SYMBOL_COOLDOWN_MINUTES", "60"))
STRATEGY_COOLDOWN_MIN = int(os.getenv("STRATEGY_COOLDOWN_MINUTES", "120"))
COMBO_COOLDOWN_MIN = int(os.getenv("COMBO_COOLDOWN_MINUTES", "90"))
MTF_TREND_MIN_ALIGNED = int(os.getenv("MTF_TREND_MIN_ALIGNED", "3"))
VOL_OF_ENABLED = os.getenv("VOL_OF_ENABLED", "true").lower()=="true"
VOL_OF_MIN_SCORE = float(os.getenv("VOL_OF_MIN_SCORE", "72"))
VOL_OF_NEAR_ATR = float(os.getenv("VOL_OF_NEAR_ATR", "0.55"))
FOOTPRINT_BIN_BPS = float(os.getenv("FOOTPRINT_BIN_BPS", "2.0"))
FOOTPRINT_IMBALANCE_RATIO = float(os.getenv("FOOTPRINT_IMBALANCE_RATIO", "2.5"))
FOOTPRINT_MIN_TRADES = int(os.getenv("FOOTPRINT_MIN_TRADES", "40"))
FOOTPRINT_MIN_LEVEL_SHARE = float(os.getenv("FOOTPRINT_MIN_LEVEL_SHARE", "0.025"))
FOOTPRINT_HISTORY_BLOCKS = int(os.getenv("FOOTPRINT_HISTORY_BLOCKS", "20"))
FOOTPRINT_STREAM_ENABLED = os.getenv("FOOTPRINT_STREAM_ENABLED", "true").lower()=="true"
FOOTPRINT_REQUIRE_FOR_EARLY = os.getenv("FOOTPRINT_REQUIRE_FOR_EARLY", "false").lower()=="true"
FOOTPRINT_SEEN_IDS_MAX = int(os.getenv("FOOTPRINT_SEEN_IDS_MAX", "50000"))
FOOTPRINT_TFS = ("15m","1h","4h","1d","1w")
FOOTPRINT_TF_SECONDS = {"15m":900,"1h":3600,"4h":14400,"1d":86400,"1w":604800}
SIGNAL_CHECK_SECONDS = float(os.getenv("SIGNAL_CHECK_SECONDS", "2"))
RADAR_REFRESH_SECONDS = int(os.getenv("RADAR_REFRESH_SECONDS", "60"))
REST_MIN_INTERVAL = float(os.getenv("REST_MIN_INTERVAL_MS", "450")) / 1000.0
REST_418_BACKOFF = int(os.getenv("REST_418_DEFAULT_BACKOFF_SECONDS", "300"))
BOOTSTRAP_LIMIT = int(os.getenv("BOOTSTRAP_KLINES_LIMIT", "220"))
WS_RECONNECT_SECONDS = int(os.getenv("WS_RECONNECT_SECONDS", "5"))
DB_PATH = os.getenv("DB_PATH", "/data/bot_stats.db").strip() or "bot_stats.db"
TRACK_TP_AFTER_TP1 = os.getenv("TRACK_TP_AFTER_TP1", "true").lower()=="true"
RESULT_TIMEOUT_15M_HOURS = float(os.getenv("RESULT_TIMEOUT_15M_HOURS", "12"))
RESULT_TIMEOUT_1H_HOURS = float(os.getenv("RESULT_TIMEOUT_1H_HOURS", "48"))
RESULT_TIMEOUT_4H_HOURS = float(os.getenv("RESULT_TIMEOUT_4H_HOURS", "120"))
RESULT_PERSIST_SECONDS = float(os.getenv("RESULT_PERSIST_SECONDS", "10"))
ENABLED_TFS = [tf for tf, key in [("15m","ENABLE_15M"),("1h","ENABLE_1H"),("4h","ENABLE_4H")] if os.getenv(key,"true").lower()=="true"]
INTERNAL_TFS = list(dict.fromkeys(ENABLED_TFS + ["5m", "15m"]))
KSA = timezone(timedelta(hours=3))

session = requests.Session()
session.headers.update({"User-Agent":"AhmedStrategyFusionBot/1.9-WS"})
app = Flask(__name__)

state = {
    "version": "1.18 EARLY BLOCK REVERSAL + NO CHASE",
    "started": datetime.now(KSA).isoformat(),
    "last_signal_check": None,
    "last_error": None,
    "alerts": 0,
    "by_strategy": defaultdict(int),
    "by_side": defaultdict(int),
    "last_alerts": {},
    "ws_connected": False,
    "ws_last_event": None,
    "radar_count": 0,
    "bootstrapped": 0,
    "bootstrap_total": 0,
    "rest_blocked_until": 0.0,
    "rest_418_count": 0,
    "rest_429_count": 0,
    "waiting_confirmation": 0,
     "performance_suppressed": 0,
    "setups_detected": 0,
    "setups_waiting_entry": 0,
    "entry_gates_opened": 0,
    "live_confirmed": 0,
    "rest_skipped_backoff": 0,
    "bootstrap_paused": False,
    "aggtrade_ws_connected": False,
    "aggtrade_last_event": None,
    "aggtrade_events": 0,
    "footprint_blocks_completed": 0,
}
state_lock = threading.RLock()
data_lock = threading.RLock()
rest_lock = threading.RLock()
ws_send_lock = threading.RLock()

# symbol -> latest 24h quote volume from !miniTicker@arr
mini_tickers = {}
# symbol -> funding from !markPrice@arr@1s
funding_cache = {}
# symbol -> latest mark price
mark_price_cache = {}
# symbol -> timestamp, value
open_interest_cache = {}
# active radar symbols
radar_symbols = []
radar_set = set()
# (symbol, tf) -> deque of raw candles
candle_store = {}
# symbols with a live candle change waiting for local strategy evaluation
dirty_symbols = set()
# symbols currently bootstrapped
bootstrapped = set()

ws_app = None
ws_request_id = 1000
subscribed_streams = set(["!miniTicker@arr", "!markPrice@arr@1s"])

# Dedicated Binance aggTrade footprint stream. Kept on a separate websocket so
# the main market socket stays under Binance stream-count limits.
agg_ws_app = None
agg_ws_lock = threading.RLock()
agg_subscribed = set()
footprint_lock = threading.RLock()
footprint_live = {}
footprint_history = defaultdict(lambda: deque(maxlen=FOOTPRINT_HISTORY_BLOCKS))
# Binance aggTrade IDs are used for duplicate protection across reconnects.
aggtrade_seen_order = deque()
aggtrade_seen_set = set()


# Internal confirmation gate: strategy setups are NOT sent to Telegram.
# Only a later structural confirmation becomes a real alert.
CONFIRM_TF = os.getenv("CONFIRM_TIMEFRAME", "auto").strip().lower()
CONFIRM_TF_15M = os.getenv("CONFIRM_TF_15M", "5m").strip().lower()
CONFIRM_TF_1H = os.getenv("CONFIRM_TF_1H", "15m").strip().lower()
CONFIRM_TF_4H = os.getenv("CONFIRM_TF_4H", "15m").strip().lower()
CONFIRM_SWING_BARS = int(os.getenv("CONFIRM_SWING_BARS", "4"))
CONFIRM_RETEST_ATR = float(os.getenv("CONFIRM_RETEST_ATR", "0.22"))
CONFIRM_RETEST_TOUCH_ATR = float(os.getenv("CONFIRM_RETEST_TOUCH_ATR", "0.10"))
CONFIRM_MAX_EXTENSION_ATR = float(os.getenv("CONFIRM_MAX_EXTENSION_ATR", "0.70"))
CONFIRM_LIVE_BREAK_ATR = float(os.getenv("CONFIRM_LIVE_BREAK_ATR", "0.04"))
CONFIRM_LIVE_RECLAIM_ATR = float(os.getenv("CONFIRM_LIVE_RECLAIM_ATR", "0.03"))
CONFIRM_MOMENTUM_ATR = float(os.getenv("CONFIRM_MOMENTUM_ATR", "0.12"))
SETUP_INVALIDATION_BUFFER_ATR = float(os.getenv("SETUP_INVALIDATION_BUFFER_ATR", "0.35"))
STOP_SWING_BARS = int(os.getenv("STOP_SWING_BARS", "5"))
STOP_BUFFER_ATR = float(os.getenv("STOP_BUFFER_ATR", "0.20"))
MIN_STOP_RISK_ATR = float(os.getenv("MIN_STOP_RISK_ATR", "0.75"))
MAX_STOP_RISK_ATR = float(os.getenv("MAX_STOP_RISK_ATR", "2.40"))
EARLY_REVERSAL_ENABLED = os.getenv("EARLY_REVERSAL_ENABLED", "true").lower()=="true"
EARLY_TRIGGER_TF = os.getenv("EARLY_TRIGGER_TF", "15m").lower()
EARLY_BLOCK_NEAR_ATR = float(os.getenv("EARLY_BLOCK_NEAR_ATR", "0.50"))
EARLY_NO_CHASE_ATR = float(os.getenv("EARLY_NO_CHASE_ATR", "0.55"))
EARLY_MIN_PATTERN_SCORE = int(os.getenv("EARLY_MIN_PATTERN_SCORE", "2"))
EARLY_STOP_BUFFER_ATR = float(os.getenv("EARLY_STOP_BUFFER_ATR", "0.16"))
EARLY_MIN_RR = float(os.getenv("EARLY_MIN_RR", "1.0"))
CONFIRM_TTL_15M_MIN = int(os.getenv("CONFIRM_TTL_15M_MINUTES", "90"))
CONFIRM_TTL_1H_MIN = int(os.getenv("CONFIRM_TTL_1H_MINUTES", "240"))
CONFIRM_TTL_4H_MIN = int(os.getenv("CONFIRM_TTL_4H_MINUTES", "720"))

# v1.11 — TRADER FIRST architecture. The coin context is calculated continuously from
# existing WebSocket candles BEFORE strategy matching. It is deliberately permissive:
# context guides direction/location and prevents bad entries, but does not wait for
# extra candle closes. First live micro confirmation can trigger immediately.
COIN_CONTEXT_MIN_SCORE = float(os.getenv("COIN_CONTEXT_MIN_SCORE", "46"))
COIN_CONTEXT_STRONG_SCORE = float(os.getenv("COIN_CONTEXT_STRONG_SCORE", "60"))
COIN_CONTEXT_MAX_OPPOSITION = float(os.getenv("COIN_CONTEXT_MAX_OPPOSITION", "64"))
FAST_CONTEXT_CONFIRM = os.getenv("FAST_CONTEXT_CONFIRM", "true").lower()=="true"
FAST_CONFIRM_MIN_SCORE = float(os.getenv("FAST_CONFIRM_MIN_SCORE", "58"))
FAST_CONFIRM_MAX_DISTANCE_ATR = float(os.getenv("FAST_CONFIRM_MAX_DISTANCE_ATR", "0.55"))
COIN_STOP_BUFFER_ATR = float(os.getenv("COIN_STOP_BUFFER_ATR", "0.22"))
COIN_STOP_MAX_ATR = float(os.getenv("COIN_STOP_MAX_ATR", "2.60"))
TRADER_MIN_CONVICTION = float(os.getenv("TRADER_MIN_CONVICTION", "58"))
TRADER_MAX_CHASE_ATR = float(os.getenv("TRADER_MAX_CHASE_ATR", "1.05"))
TRADER_ENTRY_ZONE_ATR = float(os.getenv("TRADER_ENTRY_ZONE_ATR", "0.85"))
TRADER_MIN_RR_SPACE = float(os.getenv("TRADER_MIN_RR_SPACE", "1.15"))
TRADER_SWING_LOOKBACK = int(os.getenv("TRADER_SWING_LOOKBACK", "80"))
TRADER_PIVOT_SPAN = int(os.getenv("TRADER_PIVOT_SPAN", "2"))
TRADER_NEAR_ZONE_ATR = float(os.getenv("TRADER_NEAR_ZONE_ATR", "0.65"))
TRADER_MAX_IMPULSE_ATR = float(os.getenv("TRADER_MAX_IMPULSE_ATR", "2.40"))
TRADER_MIN_PLAN_SCORE = float(os.getenv("TRADER_MIN_PLAN_SCORE", "50"))
TRADER_ZONE_BUFFER_ATR = float(os.getenv("TRADER_ZONE_BUFFER_ATR", "0.28"))
TRADER_BREAKOUT_BUFFER_ATR = float(os.getenv("TRADER_BREAKOUT_BUFFER_ATR", "0.08"))
# Independent Trend Compression Breakout strategy
TCB_ENABLED = os.getenv("TCB_ENABLED", "true").lower()=="true"
TCB_MIN_COMPRESSION_BARS = int(os.getenv("TCB_MIN_COMPRESSION_BARS", "8"))
TCB_BB_PERCENTILE = float(os.getenv("TCB_BB_PERCENTILE", "0.25"))
TCB_VOLUME_MULTIPLIER = float(os.getenv("TCB_VOLUME_MULTIPLIER", "1.35"))
TCB_EDGE_ATR = float(os.getenv("TCB_EDGE_ATR", "0.30"))
TCB_RANGE_RATIO = float(os.getenv("TCB_RANGE_RATIO", "0.90"))
TCB_PREARM_ENABLED = os.getenv("TCB_PREARM_ENABLED", "true").lower()=="true"
TCB_PREARM_MAX_DISTANCE_ATR = float(os.getenv("TCB_PREARM_MAX_DISTANCE_ATR", "0.30"))

# Performance-aware priority from stats (24).
# This does NOT change setup timing or structural confirmation.
PERFORMANCE_FILTER_ENABLED = os.getenv("PERFORMANCE_FILTER_ENABLED", "true").lower()=="true"
PERFORMANCE_MIN_EVALUATED = int(os.getenv("PERFORMANCE_MIN_EVALUATED", "40"))
PERFORMANCE_MIN_WIN_RATE = float(os.getenv("PERFORMANCE_MIN_WIN_RATE", "40"))
PERFORMANCE_WEIGHT = float(os.getenv("PERFORMANCE_WEIGHT", "0.35"))

# key=(strategy, timeframe, side): (evaluated, wins_closed, tp1)
# Source: stats (24), 2026-08-26. Only individual-strategy groups are used here.
PERFORMANCE_STATS = {
    ('Order Block + Sweep + BOS','15m','BUY'):(2850,1421,1446),
    ('Order Block + Sweep + BOS','15m','SELL'):(2839,1369,1411),
    ('VWAP Reclaim','15m','BUY'):(1570,796,819),
    ('Liquidity Sweep + Reclaim','15m','SELL'):(1293,598,626),
    ('MTF 4H→1H→15M','15m','BUY'):(1210,594,599),
    ('Break & Retest','15m','BUY'):(1104,565,568),
    ('Order Block + Sweep + BOS','1h','BUY'):(1025,506,534),
    ('Break & Retest','15m','SELL'):(896,423,440),
    ('Liquidity Sweep + Reclaim','15m','BUY'):(884,518,538),
    ('Order Block + Sweep + BOS','4h','BUY'):(748,344,371),
    ('VWAP Reclaim','1h','BUY'):(665,284,299),
    ('Compression → Expansion','15m','SELL'):(660,314,332),
    ('Compression → Expansion','15m','BUY'):(605,330,332),
    ('Break & Retest','1h','SELL'):(392,183,222),
    ('Break & Retest','1h','BUY'):(391,183,186),
    ('Break & Retest','4h','BUY'):(251,139,142),
    ('Liquidity Sweep + Reclaim','1h','BUY'):(217,124,136),
    ('VWAP Reclaim','4h','BUY'):(210,91,109),
    ('Compression → Expansion','1h','SELL'):(207,88,106),
    ('Compression → Expansion','1h','BUY'):(194,87,91),
    ('Break & Retest','4h','SELL'):(105,58,77),
    ('Compression → Expansion','4h','SELL'):(79,39,48),
    ('Compression → Expansion','4h','BUY'):(67,27,28),
    ('Liquidity Sweep + Reclaim','4h','BUY'):(66,22,28),
}

def performance_profile(strategy, tf, side):
    row=PERFORMANCE_STATS.get((strategy,tf,side))
    if not row:
        return {"grade":"LEARN","evaluated":0,"win_rate":None,"tp1_rate":None,"boost":0.0,"send":True}
    evaluated,wins,tp1=row
    wr=(wins/evaluated*100.0) if evaluated else 0.0
    t1=(tp1/evaluated*100.0) if evaluated else 0.0
    if evaluated < PERFORMANCE_MIN_EVALUATED:
        grade="LEARN"
    elif wr>=70: grade="S+"
    elif wr>=60: grade="A"
    elif wr>=52: grade="B"
    elif wr>=45: grade="C"
    elif wr>=40: grade="C-"
    else: grade="D"
    # Confidence-scaled historical score adjustment; capped so live structure remains dominant.
    confidence=min(1.0, evaluated/120.0)
    boost=max(-8.0,min(10.0,(wr-50.0)*PERFORMANCE_WEIGHT*confidence))
    allowed=(not PERFORMANCE_FILTER_ENABLED) or evaluated<PERFORMANCE_MIN_EVALUATED or wr>=PERFORMANCE_MIN_WIN_RATE
    return {"grade":grade,"evaluated":evaluated,"win_rate":wr,"tp1_rate":t1,"boost":boost,"send":allowed}

def attach_performance(s):
    prof=performance_profile(s.get("strategy"),s.get("tf"),s.get("side"))
    s["performance"]=prof
    s["historical_boost"]=float(prof.get("boost",0.0))
    return s

def performance_allows_send(s):
    prof=s.get("performance") or performance_profile(s.get("strategy"),s.get("tf"),s.get("side"))
    s["performance"]=prof
    return bool(prof.get("send",True))

pending_confirmations = {}
pending_lock = threading.RLock()
coin_context_cache = {}
coin_context_lock = threading.RLock()

def _confirm_ttl_seconds(tf):
    return 60 * (CONFIRM_TTL_15M_MIN if tf=="15m" else CONFIRM_TTL_1H_MIN if tf=="1h" else CONFIRM_TTL_4H_MIN)

def _pending_key(symbol, s):
    return (symbol, s["side"], s["strategy"], s["tf"])

def _setup_invalidation_hit(p, price):
    level=float(p.get("setup_cancel_sl", p["sl"]))
    return price <= level if p["side"]=="BUY" else price >= level

def _confirmation_tf_for_setup(tf):
    if CONFIRM_TF and CONFIRM_TF != "auto":
        return CONFIRM_TF
    return CONFIRM_TF_15M if tf=="15m" else CONFIRM_TF_1H if tf=="1h" else CONFIRM_TF_4H

def register_waiting_setups(symbol, signals):
    """Store setups silently. Never sends Telegram here."""
    now=time.time()
    with pending_lock:
        for s in signals:
            key=_pending_key(symbol,s)
            old=pending_confirmations.get(key)
            # Keep the earliest setup unless the new detector has materially better quality.
            if old and now < old["expires_ts"] and old.get("quality",0) >= s.get("quality",0):
                continue
            atr=max(float(s.get("setup_atr") or 0), abs(float(s["entry"])-float(s["sl"])), 1e-12)
            cancel_sl=(float(s["sl"])-SETUP_INVALIDATION_BUFFER_ATR*atr) if s["side"]=="BUY" else (float(s["sl"])+SETUP_INVALIDATION_BUFFER_ATR*atr)
            pending_confirmations[key]={
                "symbol":symbol, "side":s["side"], "strategy":s["strategy"], "tf":s["tf"],
                "setup":dict(s), "created_ts":now, "expires_ts":now+_confirm_ttl_seconds(s["tf"]),
                "setup_entry":float(s["entry"]), "sl":float(s["sl"]), "setup_cancel_sl":cancel_sl,
                "confirm_tf":_confirmation_tf_for_setup(s["tf"]), "phase":"WAIT_BREAK",
                "entry_gate_open":bool(s.get("entry_gate_open",False)),
                "waiting_reason":None if s.get("entry_gate_open") else ((s.get("coin_context") or {}).get("location") or "انتظار منطقة دخول مناسبة"),
                "break_level":None, "break_bar_open_time":None, "break_seen_ts":None, "post_break_extreme":None,
            }

def _confirmation_df(symbol, p):
    tf=p.get("confirm_tf") or _confirmation_tf_for_setup(p.get("tf","15m"))
    df=df_from_store(symbol,tf)
    if df is None or len(df)<max(20,CONFIRM_SWING_BARS+8):
        # 15m is the safe fallback when 5m history is not ready yet.
        tf="15m"
        df=df_from_store(symbol,tf)
    return df,tf

def _structure_confirmation(p, df, live_price=None):
    """Fast event-sequenced confirmation for ALL strategies.

    No forced extra candle. The state machine records the order of events across live
    websocket updates: micro break -> pullback/retest OR clean momentum hold -> entry.
    This prevents both same-snapshot guessing and the old 5m/15m candle delay.
    """
    if df is None or len(df)<max(20,CONFIRM_SWING_BARS+8): return None
    x=df.iloc[-1]
    if pd.isna(x.atr): return None
    side=p["side"]; atr=max(float(x.atr),1e-12); bar_open=int(x.open_time)
    price=float(live_price if live_price is not None else x.close)

    # Use only candles BEFORE the live confirmation candle to define micro structure.
    prev=df.iloc[-(CONFIRM_SWING_BARS+1):-1]
    if prev.empty: return None

    if p.get("phase","WAIT_BREAK")=="WAIT_BREAK":
        if p.get("strategy")=="Trend Compression Breakout" and (p.get("setup") or {}).get("tcb_prearm"):
            level=float((p.get("setup") or {}).get("tcb_break_level", p.get("setup_entry", price)))
        else:
            level=float(prev.high.max()) if side=="BUY" else float(prev.low.min())
        crossed = price >= level + CONFIRM_LIVE_BREAK_ATR*atr if side=="BUY" else price <= level - CONFIRM_LIVE_BREAK_ATR*atr
        extension = (price-level)/atr if side=="BUY" else (level-price)/atr
        if crossed and extension<=CONFIRM_MAX_EXTENSION_ATR:
            return {"arm":True,"level":level,"bar_open_time":bar_open,"price":price,"ts":time.time()}
        if crossed and extension>CONFIRM_MAX_EXTENSION_ATR:
            return {"cancel":True,"reason":"micro break already overextended"}
        return None

    level=float(p.get("break_level"))
    if side=="BUY":
        invalid=price<=float(p.get("setup_cancel_sl",p["sl"]))
        extreme=min(float(p.get("post_break_extreme") or price),price)
        touched=extreme<=level+CONFIRM_RETEST_TOUCH_ATR*atr
        held=extreme>=level-CONFIRM_RETEST_ATR*atr
        reclaimed=price>=level+CONFIRM_LIVE_RECLAIM_ATR*atr
        extension=(price-level)/atr
        momentum_hold=(not touched) and extension>=CONFIRM_MOMENTUM_ATR and float(x.close)>=float(x.open)
    else:
        invalid=price>=float(p.get("setup_cancel_sl",p["sl"]))
        extreme=max(float(p.get("post_break_extreme") or price),price)
        touched=extreme>=level-CONFIRM_RETEST_TOUCH_ATR*atr
        held=extreme<=level+CONFIRM_RETEST_ATR*atr
        reclaimed=price<=level-CONFIRM_LIVE_RECLAIM_ATR*atr
        extension=(level-price)/atr
        momentum_hold=(not touched) and extension>=CONFIRM_MOMENTUM_ATR and float(x.close)<=float(x.open)

    if invalid: return {"cancel":True,"reason":"setup invalidated during live confirmation"}
    if extension>CONFIRM_MAX_EXTENSION_ATR: return {"cancel":True,"reason":"entry became overextended before confirmation"}
    if (touched and held and reclaimed) or momentum_hold:
        mode="micro retest + reclaim" if (touched and held and reclaimed) else "live momentum hold"
        return {"confirmed":True,"level":level,"entry":price,"atr":atr,"bar_open_time":bar_open,"mode":mode,"extreme":extreme}
    return {"track":True,"extreme":extreme}


def _bullish_reversal_pattern(df):
    """Closed/live candle sequence only; returns evidence, not a standalone trade."""
    if df is None or len(df)<4:
        return 0,[]
    a,b,x=df.iloc[-3],df.iloc[-2],df.iloc[-1]
    atr=float(x.atr) if "atr" in df.columns and pd.notna(x.atr) else float((df.high-df.low).tail(14).mean())
    atr=max(atr,1e-12)
    score=0; names=[]
    body=abs(float(x.close-x.open)); lower=min(float(x.open),float(x.close))-float(x.low)
    # Hammer / bullish pin
    if x.close>x.open and lower>=max(1.6*body,0.22*atr) and float(x.close)>=float(x.low)+0.58*float(x.high-x.low):
        score+=1; names.append("Hammer/Pin Bar شرائي")
    # Bullish engulfing
    if b.close<b.open and x.close>x.open and x.open<=b.close and x.close>=b.open:
        score+=2; names.append("Bullish Engulfing")
    # Morning-star style 3-candle sequence (geometry, not name-only guessing)
    if a.close<a.open and abs(float(b.close-b.open))<=0.45*abs(float(a.close-a.open)) and x.close>x.open and x.close>=float(a.open+a.close)/2:
        score+=2; names.append("Morning Star")
    # Tweezer / failed lower-low reclaim
    if abs(float(x.low-b.low))<=0.10*atr and x.close>x.open and b.close<b.open:
        score+=1; names.append("Tweezer/رفض قاع")
    # Failed breakdown -> reclaim: the sequence visible in the PENDLE reference.
    prior_low=float(df.low.iloc[-8:-2].min()) if len(df)>=8 else float(df.low.iloc[:-2].min())
    if float(b.low)<=prior_low+0.08*atr and x.close>max(float(b.open),float(b.close))+0.06*atr:
        score+=2; names.append("فشل هبوط ثم استرداد شرائي")
    return score,list(dict.fromkeys(names))


def _bearish_reversal_pattern(df):
    if df is None or len(df)<4:
        return 0,[]
    a,b,x=df.iloc[-3],df.iloc[-2],df.iloc[-1]
    atr=float(x.atr) if "atr" in df.columns and pd.notna(x.atr) else float((df.high-df.low).tail(14).mean())
    atr=max(atr,1e-12)
    score=0; names=[]
    body=abs(float(x.close-x.open)); upper=float(x.high)-max(float(x.open),float(x.close))
    if x.close<x.open and upper>=max(1.6*body,0.22*atr) and float(x.close)<=float(x.low)+0.42*float(x.high-x.low):
        score+=1; names.append("Shooting Star/Pin Bar بيعي")
    if b.close>b.open and x.close<x.open and x.open>=b.close and x.close<=b.open:
        score+=2; names.append("Bearish Engulfing")
    if a.close>a.open and abs(float(b.close-b.open))<=0.45*abs(float(a.close-a.open)) and x.close<x.open and x.close<=float(a.open+a.close)/2:
        score+=2; names.append("Evening Star")
    if abs(float(x.high-b.high))<=0.10*atr and x.close<x.open and b.close>b.open:
        score+=1; names.append("Tweezer/رفض قمة")
    prior_high=float(df.high.iloc[-8:-2].max()) if len(df)>=8 else float(df.high.iloc[:-2].max())
    if float(b.high)>=prior_high-0.08*atr and x.close<min(float(b.open),float(b.close))-0.06*atr:
        score+=2; names.append("فشل صعود ثم استرداد بيعي")
    return score,list(dict.fromkeys(names))


def _early_block_reversal_confirmation(symbol,p,df,price):
    """Early timing: real setup already exists. Trigger from block + reversal, before a late BOS chase."""
    if not EARLY_REVERSAL_ENABLED or df is None or len(df)<10 or price is None:
        return None
    setup=p.get("setup") or {}; side=p.get("side")
    atr=float(df.iloc[-1].atr) if "atr" in df.columns and pd.notna(df.iloc[-1].atr) else float((df.high-df.low).tail(14).mean())
    atr=max(atr,1e-12)
    blocks=setup.get("vol_of_blocks") or []
    # Prefer REAL aggTrade block. If footprint has not matured yet, use setup structural zone;
    # never invent a separate order-flow block.
    block=None
    for b in blocks:
        if b.get("side")==side:
            block=b; break
    if block:
        lo=float(block["low"]); hi=float(block["high"]); block_label=f"REAL Footprint {block.get('tf','').upper()}"
    elif FOOTPRINT_REQUIRE_FOR_EARLY:
        return None
    else:
        lo=(setup.get("coin_context") or {}).get("entry_zone_low")
        hi=(setup.get("coin_context") or {}).get("entry_zone_high")
        if lo is None or hi is None:
            return None
        lo=float(lo); hi=float(hi); block_label="منطقة الخطة البنيوية"
    if lo>hi: lo,hi=hi,lo

    # Must be in/near block; this is the anti-late-entry rule.
    dist=max(lo-float(price),float(price)-hi,0.0)/atr
    if dist>EARLY_BLOCK_NEAR_ATR:
        return None

    if side=="BUY":
        pscore,names=_bullish_reversal_pattern(df)
        delta_ok=float(df.iloc[-1].delta)>0 if "delta" in df.columns else False
        cvd_ok=float(df.iloc[-1].cvd)>=float(df.iloc[-2].cvd) if "cvd" in df.columns else False
        if pscore<EARLY_MIN_PATTERN_SCORE or not (delta_ok or cvd_ok):
            return None
        # structural stop: below actual reversal/sweep low and block low.
        reversal_low=float(df.low.iloc[-4:].min())
        stop=min(reversal_low,lo)-EARLY_STOP_BUFFER_ATR*atr
        anchor_price=max(lo,reversal_low)
        extension=(float(price)-anchor_price)/atr
    else:
        pscore,names=_bearish_reversal_pattern(df)
        delta_ok=float(df.iloc[-1].delta)<0 if "delta" in df.columns else False
        cvd_ok=float(df.iloc[-1].cvd)<=float(df.iloc[-2].cvd) if "cvd" in df.columns else False
        if pscore<EARLY_MIN_PATTERN_SCORE or not (delta_ok or cvd_ok):
            return None
        reversal_high=float(df.high.iloc[-4:].max())
        stop=max(reversal_high,hi)+EARLY_STOP_BUFFER_ATR*atr
        anchor_price=min(hi,reversal_high)
        extension=(anchor_price-float(price))/atr

    if extension>EARLY_NO_CHASE_ATR:
        return {"late":True,"reason":"فات الدخول — انتظار Retest","extension_atr":round(extension,2)}

    risk=abs(float(price)-stop)
    if risk<0.30*atr or risk>MAX_STOP_RISK_ATR*atr:
        return None
    return {
        "confirmed":True,"entry":float(price),"atr":atr,"early":True,
        "mode":"block reversal","pattern_names":names,"pattern_score":pscore,
        "block_label":block_label,"early_stop":float(stop)
    }


def _structural_stop(side, entry, df, atr, coin_invalidation=None):
    """SL is the thesis invalidation first; confirmation swing is fallback, never an arbitrary close stop."""
    recent=df.iloc[-max(3,STOP_SWING_BARS+1):]
    structural=(float(recent.low.min())-STOP_BUFFER_ATR*atr) if side=='BUY' else (float(recent.high.max())+STOP_BUFFER_ATR*atr)
    stop=structural
    if coin_invalidation is not None:
        ci=float(coin_invalidation)
        if (side=='BUY' and ci<entry) or (side=='SELL' and ci>entry): stop=ci
    risk=abs(entry-stop)
    if risk<MIN_STOP_RISK_ATR*atr:
        stop=entry-MIN_STOP_RISK_ATR*atr if side=='BUY' else entry+MIN_STOP_RISK_ATR*atr
        risk=abs(entry-stop)
    if risk<=0 or risk>min(MAX_STOP_RISK_ATR,COIN_STOP_MAX_ATR)*atr: return None
    return float(stop)

def evaluate_waiting_confirmations(symbol):
    now=time.time(); price=mark_price_cache.get(symbol)
    confirmed=[]; remove=[]

    # Latest trader analysis for this coin. It is refreshed by analyze_symbol on every dirty update.
    with coin_context_lock:
        latest_ctx=dict(coin_context_cache.get(symbol) or {})

    with pending_lock:
        items=[(k,dict(v)) for k,v in pending_confirmations.items() if v["symbol"]==symbol]

    for key,p in items:
        if now>=p["expires_ts"]:
            remove.append(key); continue
        if price and _setup_invalidation_hit(p,float(price)):
            remove.append(key); continue

        side=p["side"]

        # ENTRY GATE: a setup may exist while the trader says WAIT.
        # Keep it silently armed until price/context becomes tradeable.
        setup_snapshot=dict(p.get("setup") or {})
        gate_open,gate_reason=_setup_entry_gate(latest_ctx,setup_snapshot) if setup_snapshot else (False,"لا يوجد Setup")
        if latest_ctx.get("tradeable") and latest_ctx.get("decision")==side:
            gate_open=True
            gate_reason="تحليل العملة والاستراتيجية متوافقان"
        if not gate_open:
            with pending_lock:
                live=pending_confirmations.get(key)
                if live is not None:
                    live["entry_gate_open"]=False
                    live["waiting_reason"]=gate_reason
                    # Keep the setup current with the latest market analysis.
                    setup=dict(live.get("setup") or {})
                    if setup:
                        attach_coin_context(setup,latest_ctx)
                        setup["entry_gate_open"]=False
                        setup["setup_status"]="SETUP_FOUND_WAIT_ENTRY"
                        live["setup"]=setup
            continue

        # Gate opened: refresh setup context/invalidation BEFORE any live trigger.
        with pending_lock:
            live=pending_confirmations.get(key)
            if live is not None:
                setup=dict(live.get("setup") or {})
                if setup:
                    attach_coin_context(setup,latest_ctx)
                    setup["entry_gate_open"]=True
                    setup["setup_status"]="READY_FOR_LIVE_TRIGGER"
                    live["setup"]=setup
                    p["setup"]=dict(setup)
                live["entry_gate_open"]=True
                live["waiting_reason"]=None

        df,confirm_tf=_confirmation_df(symbol,p)

        # TCB pre-arms before the breakout; it must use the dedicated break state machine,
        # not a reversal trigger that could fire before the range boundary breaks.
        if p.get("strategy")=="Trend Compression Breakout" and (p.get("setup") or {}).get("tcb_prearm"):
            res=_structure_confirmation(p,df,float(price) if price else None)
        else:
            # FIRST choice: early reversal inside/near the real block/plan zone.
            # This prevents waiting for a large BOS after the move already happened.
            res=_early_block_reversal_confirmation(symbol,p,df,float(price) if price else None)
        if res and res.get("late"):
            with pending_lock:
                live=pending_confirmations.get(key)
                if live is not None:
                    live["waiting_reason"]=res.get("reason")
            continue
        # Fallback only if no valid early reversal exists.
        if res is None:
            res=_fast_live_context_confirmation(p,df,float(price) if price else None)
        if res is None:
            res=_structure_confirmation(p,df,float(price) if price else None)

        if res and res.get("cancel"):
            remove.append(key); continue
        if res and res.get("arm"):
            with pending_lock:
                live=pending_confirmations.get(key)
                if live is not None:
                    live["phase"]="WAIT_RETEST"
                    live["break_level"]=float(res["level"])
                    live["break_bar_open_time"]=int(res["bar_open_time"])
                    live["break_seen_ts"]=float(res.get("ts") or time.time())
                    live["post_break_extreme"]=float(res.get("price") or live["break_level"])
                    live["confirm_tf"]=confirm_tf
            continue
        if res and res.get("track"):
            with pending_lock:
                live=pending_confirmations.get(key)
                if live is not None:
                    live["post_break_extreme"]=float(res["extreme"])
            continue
        if res and res.get("confirmed"):
            s=dict(p["setup"]); entry=float(res["entry"]); atr=float(res["atr"])

            # Use the latest trader invalidation, not stale setup-time context.
            coin_inv=latest_ctx.get("invalid_buy" if s["side"]=="BUY" else "invalid_sell")
            if coin_inv is not None:
                s["coin_invalidation"]=float(coin_inv)

            if res.get("early") and res.get("early_stop") is not None:
                final_sl=float(res["early_stop"])
            else:
                final_sl=_structural_stop(s["side"],entry,df,atr,s.get("coin_invalidation"))
            if final_sl is None:
                # Bad risk/location: do NOT kill the setup immediately; return it to waiting state
                # so it can trigger later from a better price while still valid.
                with pending_lock:
                    live=pending_confirmations.get(key)
                    if live is not None:
                        live["phase"]="WAIT_BREAK"
                        live["break_level"]=None
                        live["break_bar_open_time"]=None
                        live["break_seen_ts"]=None
                        live["post_break_extreme"]=None
                        live["waiting_reason"]="الوقف البنيوي غير مناسب عند هذا السعر — انتظار دخول أفضل"
                continue

            s["entry"]=entry; s["sl"]=final_sl
            risk=abs(entry-final_sl)
            s["tp"]=[entry+(risk*r if s["side"]=="BUY" else -risk*r) for r in (1,2,3)]
            s["quality"]=min(99,float(s.get("quality",0))+6)
            if res.get("early"):
                pats=" + ".join(res.get("pattern_names") or ["انعكاس شرائي/بيعي"])
                s["trigger_summary"]=f"{pats} | {res.get('block_label','منطقة بلوك')}"
                s["reasons"]=list(s.get("reasons",[]))+[
                    f"تأكيد مبكر داخل البلوك: {pats}",
                    "Delta/CVD متوافق مع الانعكاس",
                    "NO CHASE: الدخول ما زال قريبًا من منطقة الانعكاس",
                    "الوقف خلف قاع/قمة الانعكاس والبلوك"
                ]
            else:
                s["trigger_summary"]=f"تأكيد حي {confirm_tf.upper()}"
                s["reasons"]=list(s.get("reasons",[]))+[
                    f"تأكيد حي على {confirm_tf.upper()}",
                    "الوقف خلف نقطة الإبطال البنيوية"
                ]
            s["helpers"]=helpers(df,s["side"]) if df is not None else list(s.get("helpers",[]))
            attach_performance(s)
            s["score"]=s["quality"]+min(10,len(s["helpers"])*1.5)+float(s.get("historical_boost",0.0))
            confirmed.append(s); remove.append(key)
            with state_lock:
                state["live_confirmed"] += 1

    if remove:
        with pending_lock:
            for k in remove: pending_confirmations.pop(k,None)
    return confirmed


# Persistent signal-result tracking
db_lock = threading.RLock()
active_lock = threading.RLock()
active_trades = {}           # alert_id -> dict
active_by_symbol = defaultdict(set)
# Shadow study: trades that hit SL first are followed virtually without changing live trade logic.
post_sl_trades = {}
post_sl_by_symbol = defaultdict(set)



def ensure_db_dir():
    try:
        parent=os.path.dirname(DB_PATH)
        if parent:
            os.makedirs(parent, exist_ok=True)
    except Exception:
        pass

def db_conn():
    ensure_db_dir()
    c=sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    c.row_factory=sqlite3.Row
    return c

def init_db():
    with db_lock:
        c=db_conn()
        c.execute("""CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_ts REAL NOT NULL, created_at TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
            timeframe TEXT NOT NULL, strategy TEXT NOT NULL, is_combo INTEGER NOT NULL DEFAULT 0,
            combo_strategies TEXT, entry REAL NOT NULL, sl REAL NOT NULL, tp1 REAL NOT NULL, tp2 REAL NOT NULL, tp3 REAL NOT NULL,
            quality REAL, helpers TEXT, reasons TEXT, funding REAL, oi REAL,
            status TEXT NOT NULL DEFAULT 'OPEN', result TEXT,
            tp1_hit_ts REAL, tp2_hit_ts REAL, tp3_hit_ts REAL, sl_hit_ts REAL, closed_ts REAL,
            first_hit TEXT, first_hit_ts REAL, max_favorable REAL NOT NULL DEFAULT 0, max_adverse REAL NOT NULL DEFAULT 0,
            best_r REAL NOT NULL DEFAULT 0, worst_r REAL NOT NULL DEFAULT 0, last_price REAL, timeout_hours REAL NOT NULL DEFAULT 48
        )""")
        # Schema migration for v1.3.1 SL STUDY. Existing v1.3 databases are preserved.
        existing={r[1] for r in c.execute("PRAGMA table_info(alerts)").fetchall()}
        additions={
            "post_sl_tracking_started_ts":"REAL",
            "post_sl_tracking_end_ts":"REAL",
            "post_sl_tp1_hit_ts":"REAL",
            "post_sl_best_r":"REAL",
            "post_sl_worst_r":"REAL",
            "post_sl_max_adverse":"REAL",
            "post_sl_last_price":"REAL",
            "post_sl_done":"INTEGER NOT NULL DEFAULT 0",
            "post_sl_outcome":"TEXT"
        }
        for col,decl in additions.items():
            if col not in existing:
                c.execute(f"ALTER TABLE alerts ADD COLUMN {col} {decl}")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_symbol_status ON alerts(symbol,status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_ts)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_post_sl ON alerts(symbol,post_sl_done,post_sl_tracking_started_ts)")
        c.execute("""CREATE TABLE IF NOT EXISTS footprint_blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            start_ts REAL NOT NULL,
            end_ts REAL NOT NULL,
            side TEXT NOT NULL,
            score REAL NOT NULL,
            zone_low REAL NOT NULL,
            zone_high REAL NOT NULL,
            poc REAL NOT NULL,
            delta_pct REAL NOT NULL,
            total_buy_quote REAL NOT NULL,
            total_sell_quote REAL NOT NULL,
            stacked_imbalance INTEGER NOT NULL DEFAULT 0,
            trades INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_fp_symbol_tf ON footprint_blocks(symbol,timeframe,end_ts)")
        c.commit(); c.close()

def timeout_hours_for_tf(tf):
    return RESULT_TIMEOUT_15M_HOURS if tf=='15m' else RESULT_TIMEOUT_4H_HOURS if tf=='4h' else RESULT_TIMEOUT_1H_HOURS

def load_active_trades():
    with db_lock:
        c=db_conn(); rows=c.execute("SELECT * FROM alerts WHERE status='OPEN'").fetchall(); c.close()
    with active_lock:
        active_trades.clear(); active_by_symbol.clear()
        for r in rows:
            d=dict(r); active_trades[d['id']]=d; active_by_symbol[d['symbol']].add(d['id'])

def load_post_sl_trades():
    # Only rows that were explicitly started by v1.3.1 are studied. Historical v1.3 SLs are not guessed.
    with db_lock:
        c=db_conn(); rows=c.execute("""SELECT * FROM alerts
            WHERE post_sl_tracking_started_ts IS NOT NULL AND COALESCE(post_sl_done,0)=0""").fetchall(); c.close()
    with active_lock:
        post_sl_trades.clear(); post_sl_by_symbol.clear()
        for r in rows:
            d=dict(r); post_sl_trades[d['id']]=d; post_sl_by_symbol[d['symbol']].add(d['id'])

def persist_alert(symbol,s,helpers_list,oi,funding,combo=None):
    strat=' + '.join(combo) if combo else s['strategy']
    now=time.time(); tf=s['tf']; tout=timeout_hours_for_tf(tf)
    with db_lock:
        c=db_conn(); cur=c.execute("""INSERT INTO alerts(
            created_ts,created_at,symbol,side,timeframe,strategy,is_combo,combo_strategies,entry,sl,tp1,tp2,tp3,quality,helpers,reasons,funding,oi,timeout_hours,last_price
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
            now, now_ksa().isoformat(), symbol,s['side'],tf,strat,1 if combo else 0,json.dumps(combo or [],ensure_ascii=False),
            float(s['entry']),float(s['sl']),float(s['tp'][0]),float(s['tp'][1]),float(s['tp'][2]),float(s.get('quality',0)),
            json.dumps(helpers_list or [],ensure_ascii=False),json.dumps(s.get('reasons') or [],ensure_ascii=False),funding,oi,tout,float(s['entry'])
        )); alert_id=cur.lastrowid; c.commit(); row=c.execute("SELECT * FROM alerts WHERE id=?",(alert_id,)).fetchone(); c.close()
    with active_lock:
        d=dict(row); active_trades[alert_id]=d; active_by_symbol[symbol].add(alert_id)
    return alert_id

def _crossed(side, price, level, kind):
    if kind.startswith('TP'):
        return price >= level if side=='BUY' else price <= level
    return price <= level if side=='BUY' else price >= level

def evaluate_post_sl_trades(symbol, price, ts=None):
    """Virtual follow-up after SL-first. Does not alter the original CLOSED result.
    Stops when the original TP1 is reached or when the original timeframe timeout expires.
    post_sl_worst_r is the maximum adverse excursion from entry before recovery/timeout.
    """
    ts=float(ts or time.time()); price=float(price)
    with active_lock:
        ids=list(post_sl_by_symbol.get(symbol,set()))
    if not ids:
        return
    persist=[]; finishes=[]
    for aid in ids:
        with active_lock:
            original=post_sl_trades.get(aid)
            if not original: continue
            tr=dict(original)
        entry=float(tr['entry']); sl=float(tr['sl']); risk=abs(entry-sl) or 1e-12; side=tr['side']
        fav=((price-entry) if side=='BUY' else (entry-price))
        adv=((entry-price) if side=='BUY' else (price-entry))
        fields={'post_sl_last_price':price}
        fields['post_sl_best_r']=max(float(tr.get('post_sl_best_r') or tr.get('best_r') or 0),fav/risk)
        fields['post_sl_worst_r']=max(float(tr.get('post_sl_worst_r') or tr.get('worst_r') or 0),adv/risk)
        fields['post_sl_max_adverse']=max(float(tr.get('post_sl_max_adverse') or tr.get('max_adverse') or 0),adv)
        milestone=False
        if not tr.get('post_sl_tp1_hit_ts') and _crossed(side,price,float(tr['tp1']),'TP1'):
            fields['post_sl_tp1_hit_ts']=ts
            fields['post_sl_done']=1
            fields['post_sl_outcome']='RECOVERED_TP1'
            milestone=True; finishes.append(aid)
        else:
            end_ts=float(tr.get('post_sl_tracking_end_ts') or (float(tr['created_ts'])+float(tr.get('timeout_hours') or 48)*3600))
            if ts>=end_ts:
                fields['post_sl_done']=1
                fields['post_sl_outcome']='NO_RECOVERY_TIMEOUT'
                milestone=True; finishes.append(aid)
        last_persist=float(tr.get('_post_last_persist_ts') or 0)
        should_persist=milestone or (ts-last_persist>=RESULT_PERSIST_SECONDS)
        with active_lock:
            if aid in post_sl_trades:
                post_sl_trades[aid].update(fields)
                if should_persist: post_sl_trades[aid]['_post_last_persist_ts']=ts
        if should_persist: persist.append((aid,fields))
    if persist:
        with db_lock:
            c=db_conn()
            for aid,fields in persist:
                sets=','.join(f"{k}=?" for k in fields); vals=list(fields.values())+[aid]
                c.execute(f"UPDATE alerts SET {sets} WHERE id=?",vals)
            c.commit(); c.close()
    if finishes:
        with active_lock:
            for aid in set(finishes):
                tr=post_sl_trades.pop(aid,None)
                if tr: post_sl_by_symbol[tr['symbol']].discard(aid)

def evaluate_symbol_trades(symbol, price, ts=None):
    ts=float(ts or time.time()); price=float(price)
    with active_lock:
        ids=list(active_by_symbol.get(symbol,set()))
    if not ids:
        evaluate_post_sl_trades(symbol,price,ts)
        return
    persist=[]; closes=[]; post_sl_starts=[]
    for aid in ids:
        with active_lock:
            original=active_trades.get(aid)
            if not original: continue
            tr=dict(original)
        entry=float(tr['entry']); sl=float(tr['sl']); risk=abs(entry-sl) or 1e-12; side=tr['side']
        fav=((price-entry) if side=='BUY' else (entry-price)); adv=((entry-price) if side=='BUY' else (price-entry))
        fields={'last_price':price}
        new_best=max(float(tr.get('best_r') or 0),fav/risk); new_worst=max(float(tr.get('worst_r') or 0),adv/risk)
        fields['max_favorable']=max(float(tr.get('max_favorable') or 0),fav)
        fields['max_adverse']=max(float(tr.get('max_adverse') or 0),adv)
        fields['best_r']=new_best; fields['worst_r']=new_worst
        first_hit=tr.get('first_hit'); milestone=False
        if not tr.get('tp1_hit_ts') and _crossed(side,price,float(tr['tp1']),'TP1'):
            fields['tp1_hit_ts']=ts; milestone=True
            if not first_hit: fields['first_hit']='TP1'; fields['first_hit_ts']=ts; first_hit='TP1'
        if not tr.get('tp2_hit_ts') and _crossed(side,price,float(tr['tp2']),'TP2'):
            fields['tp2_hit_ts']=ts; milestone=True
            if not first_hit: fields['first_hit']='TP2'; fields['first_hit_ts']=ts; first_hit='TP2'
        if not tr.get('tp3_hit_ts') and _crossed(side,price,float(tr['tp3']),'TP3'):
            fields['tp3_hit_ts']=ts; milestone=True
            if not first_hit: fields['first_hit']='TP3'; fields['first_hit_ts']=ts; first_hit='TP3'
            fields['status']='CLOSED'; fields['result']='TP3'; fields['closed_ts']=ts; closes.append(aid)
        if not tr.get('sl_hit_ts') and _crossed(side,price,sl,'SL'):
            fields['sl_hit_ts']=ts; milestone=True
            was_first_hit=not bool(first_hit)
            if was_first_hit: fields['first_hit']='SL'; fields['first_hit_ts']=ts
            prior_tp2=bool(tr.get('tp2_hit_ts') or fields.get('tp2_hit_ts')); prior_tp1=bool(tr.get('tp1_hit_ts') or fields.get('tp1_hit_ts'))
            fields['status']='CLOSED'; fields['result']='TP2_THEN_SL' if prior_tp2 else 'TP1_THEN_SL' if prior_tp1 else 'SL'; fields['closed_ts']=ts; closes.append(aid)
            # Start the shadow study only when SL was the first outcome. Live result remains SL.
            if was_first_hit and not prior_tp1:
                fields['post_sl_tracking_started_ts']=ts
                fields['post_sl_tracking_end_ts']=float(tr['created_ts'])+float(tr.get('timeout_hours') or 48)*3600
                fields['post_sl_best_r']=new_best
                fields['post_sl_worst_r']=new_worst
                fields['post_sl_max_adverse']=fields['max_adverse']
                fields['post_sl_last_price']=price
                fields['post_sl_done']=0
                fields['post_sl_outcome']='TRACKING'
                post_sl_starts.append((aid,tr,dict(fields)))
        if ts-float(tr['created_ts']) >= float(tr.get('timeout_hours') or 48)*3600 and fields.get('status','OPEN')=='OPEN':
            prior_tp2=bool(tr.get('tp2_hit_ts') or fields.get('tp2_hit_ts')); prior_tp1=bool(tr.get('tp1_hit_ts') or fields.get('tp1_hit_ts'))
            fields['status']='CLOSED'; fields['result']='TP2_TIMEOUT' if prior_tp2 else 'TP1_TIMEOUT' if prior_tp1 else 'TIMEOUT'; fields['closed_ts']=ts; milestone=True; closes.append(aid)
        last_persist=float(tr.get('_last_persist_ts') or 0)
        should_persist=milestone or (ts-last_persist>=RESULT_PERSIST_SECONDS)
        # Always update the in-memory tracker; persist periodically or on milestones.
        with active_lock:
            if aid in active_trades:
                active_trades[aid].update(fields)
                if should_persist: active_trades[aid]['_last_persist_ts']=ts
        if should_persist: persist.append((aid,fields))
    if persist:
        with db_lock:
            c=db_conn()
            for aid,fields in persist:
                sets=','.join(f"{k}=?" for k in fields); vals=list(fields.values())+[aid]
                c.execute(f"UPDATE alerts SET {sets} WHERE id=?",vals)
            c.commit(); c.close()
    if post_sl_starts:
        with active_lock:
            for aid,tr,fields in post_sl_starts:
                shadow=dict(tr); shadow.update(fields)
                post_sl_trades[aid]=shadow; post_sl_by_symbol[shadow['symbol']].add(aid)
    if closes:
        with active_lock:
            for aid in set(closes):
                tr=active_trades.pop(aid,None)
                if tr: active_by_symbol[tr['symbol']].discard(aid)
    # Same live tick also updates the shadow tracker, without changing the original trade result.
    evaluate_post_sl_trades(symbol,price,ts)


def result_stats():
    with db_lock:
        c=db_conn()
        total=c.execute('SELECT COUNT(*) n FROM alerts').fetchone()['n']
        open_n=c.execute("SELECT COUNT(*) n FROM alerts WHERE status='OPEN'").fetchone()['n']
        evaluated=c.execute("SELECT COUNT(*) n FROM alerts WHERE status='CLOSED'").fetchone()['n']
        wins=c.execute("SELECT COUNT(*) n FROM alerts WHERE status='CLOSED' AND (tp1_hit_ts IS NOT NULL)").fetchone()['n']
        sl_first=c.execute("SELECT COUNT(*) n FROM alerts WHERE status='CLOSED' AND first_hit='SL'").fetchone()['n']
        tp1=c.execute('SELECT COUNT(*) n FROM alerts WHERE tp1_hit_ts IS NOT NULL').fetchone()['n']
        tp2=c.execute('SELECT COUNT(*) n FROM alerts WHERE tp2_hit_ts IS NOT NULL').fetchone()['n']
        tp3=c.execute('SELECT COUNT(*) n FROM alerts WHERE tp3_hit_ts IS NOT NULL').fetchone()['n']
        strategy_rows=c.execute("""SELECT strategy,is_combo,COUNT(*) total,
            SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) evaluated,
            SUM(CASE WHEN status='CLOSED' AND tp1_hit_ts IS NOT NULL THEN 1 ELSE 0 END) wins_closed,
            SUM(CASE WHEN tp1_hit_ts IS NOT NULL THEN 1 ELSE 0 END) tp1,
            SUM(CASE WHEN tp2_hit_ts IS NOT NULL THEN 1 ELSE 0 END) tp2,
            SUM(CASE WHEN tp3_hit_ts IS NOT NULL THEN 1 ELSE 0 END) tp3,
            SUM(CASE WHEN first_hit='SL' THEN 1 ELSE 0 END) sl_first,
            AVG(CASE WHEN tp1_hit_ts IS NOT NULL THEN (tp1_hit_ts-created_ts)/60.0 END) avg_tp1_minutes,
            AVG(best_r) avg_best_r, AVG(worst_r) avg_worst_r
            FROM alerts GROUP BY strategy,is_combo
            ORDER BY CASE WHEN SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END)>=5
              THEN 1.0*SUM(CASE WHEN status='CLOSED' AND tp1_hit_ts IS NOT NULL THEN 1 ELSE 0 END)/SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) ELSE -1 END DESC, evaluated DESC""").fetchall()
        rows=c.execute("""SELECT strategy,timeframe,side,is_combo,COUNT(*) total,
            SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) evaluated,
            SUM(CASE WHEN status='CLOSED' AND tp1_hit_ts IS NOT NULL THEN 1 ELSE 0 END) wins_closed,
            SUM(CASE WHEN tp1_hit_ts IS NOT NULL THEN 1 ELSE 0 END) tp1,
            SUM(CASE WHEN tp2_hit_ts IS NOT NULL THEN 1 ELSE 0 END) tp2,
            SUM(CASE WHEN tp3_hit_ts IS NOT NULL THEN 1 ELSE 0 END) tp3,
            SUM(CASE WHEN first_hit='SL' THEN 1 ELSE 0 END) sl_first,
            AVG(CASE WHEN tp1_hit_ts IS NOT NULL THEN (tp1_hit_ts-created_ts)/60.0 END) avg_tp1_minutes,
            AVG(best_r) avg_best_r, AVG(worst_r) avg_worst_r
            FROM alerts GROUP BY strategy,timeframe,side,is_combo ORDER BY evaluated DESC,total DESC""").fetchall()
        post_sl_global=c.execute("""SELECT
            COUNT(*) tracked,
            SUM(CASE WHEN COALESCE(post_sl_done,0)=1 THEN 1 ELSE 0 END) evaluated,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' THEN 1 ELSE 0 END) recovered,
            AVG(CASE WHEN post_sl_outcome='RECOVERED_TP1' THEN post_sl_worst_r END) avg_recovery_worst_r,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.05 THEN 1 ELSE 0 END) recovered_105,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.10 THEN 1 ELSE 0 END) recovered_110,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.15 THEN 1 ELSE 0 END) recovered_115,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.20 THEN 1 ELSE 0 END) recovered_120,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.25 THEN 1 ELSE 0 END) recovered_125,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.30 THEN 1 ELSE 0 END) recovered_130,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.50 THEN 1 ELSE 0 END) recovered_150
            FROM alerts WHERE post_sl_tracking_started_ts IS NOT NULL""").fetchone()
        post_sl_rows=c.execute("""SELECT strategy,timeframe,side,is_combo,
            COUNT(*) tracked,
            SUM(CASE WHEN COALESCE(post_sl_done,0)=1 THEN 1 ELSE 0 END) evaluated,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' THEN 1 ELSE 0 END) recovered,
            AVG(CASE WHEN post_sl_outcome='RECOVERED_TP1' THEN post_sl_worst_r END) avg_recovery_worst_r,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.05 THEN 1 ELSE 0 END) recovered_105,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.10 THEN 1 ELSE 0 END) recovered_110,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.15 THEN 1 ELSE 0 END) recovered_115,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.20 THEN 1 ELSE 0 END) recovered_120,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.25 THEN 1 ELSE 0 END) recovered_125,
            SUM(CASE WHEN post_sl_outcome='RECOVERED_TP1' AND post_sl_worst_r<=1.50 THEN 1 ELSE 0 END) recovered_150
            FROM alerts WHERE post_sl_tracking_started_ts IS NOT NULL
            GROUP BY strategy,timeframe,side,is_combo ORDER BY evaluated DESC,tracked DESC""").fetchall()
        recent=c.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 60").fetchall(); c.close()
    psg=dict(post_sl_global) if post_sl_global else {}
    pe=psg.get('evaluated') or 0; pr=psg.get('recovered') or 0
    psg['recovery_rate']=(pr/pe*100 if pe else 0)
    return {'total':total,'open':open_n,'evaluated':evaluated,'wins':wins,'sl_first':sl_first,'tp1':tp1,'tp2':tp2,'tp3':tp3,'win_rate':(wins/evaluated*100 if evaluated else 0),'strategies':[dict(r) for r in strategy_rows],'groups':[dict(r) for r in rows],'post_sl':psg,'post_sl_groups':[dict(r) for r in post_sl_rows],'recent':[dict(r) for r in recent]}

def stats_html():
    st=result_stats()
    def pct(a,b): return f"{(100*a/b):.1f}%" if b else '—'
    cards=f"""<div class='cards'><div><b>{st['total']}</b><span>كل التنبيهات</span></div><div><b>{st['evaluated']}</b><span>تم تقييمها</span></div><div><b>{st['open']}</b><span>قيد المتابعة</span></div><div><b>{st['win_rate']:.1f}%</b><span>نجاح TP1 قبل الإغلاق</span></div><div><b>{st['tp2']}</b><span>وصلت TP2</span></div><div><b>{st['tp3']}</b><span>وصلت TP3</span></div></div>"""
    top=[]
    for g in st['strategies']:
        ev=g['evaluated'] or 0; win=g['wins_closed'] or 0
        top.append(f"<tr><td>{html.escape(g['strategy'])}</td><td>{'🔥 COMBO' if g['is_combo'] else 'فردية'}</td><td>{g['total']}</td><td>{ev}</td><td><b>{pct(win,ev)}</b></td><td>{g['tp1'] or 0}</td><td>{g['tp2'] or 0}</td><td>{g['tp3'] or 0}</td><td>{g['sl_first'] or 0}</td><td>{float(g['avg_tp1_minutes'] or 0):.1f} د</td><td>{float(g['avg_best_r'] or 0):.2f}R</td><td>{float(g['avg_worst_r'] or 0):.2f}R</td></tr>")
    rows=[]
    for g in st['groups']:
        ev=g['evaluated'] or 0; win=g['wins_closed'] or 0
        rows.append(f"<tr><td>{html.escape(g['strategy'])}</td><td>{g['timeframe'].upper()}</td><td class='{g['side'].lower()}'>{'شراء' if g['side']=='BUY' else 'بيع'}</td><td>{g['total']}</td><td>{ev}</td><td><b>{pct(win,ev)}</b></td><td>{g['tp1'] or 0}</td><td>{g['tp2'] or 0}</td><td>{g['tp3'] or 0}</td><td>{g['sl_first'] or 0}</td><td>{float(g['avg_tp1_minutes'] or 0):.1f} د</td><td>{float(g['avg_best_r'] or 0):.2f}R</td><td>{float(g['avg_worst_r'] or 0):.2f}R</td></tr>")
    ps=st.get('post_sl') or {}
    pe=ps.get('evaluated') or 0; pr=ps.get('recovered') or 0
    post_cards=f"""<div class='cards'><div><b>{ps.get('tracked') or 0}</b><span>SL تحت الدراسة</span></div><div><b>{pe}</b><span>اكتملت متابعتها</span></div><div><b>{pct(pr,pe)}</b><span>رجعت إلى TP1 بعد SL</span></div><div><b>{float(ps.get('avg_recovery_worst_r') or 0):.2f}R</b><span>متوسط أقصى انعكاس للمتعافية</span></div></div>"""
    post_rows=[]
    for g in st.get('post_sl_groups') or []:
        ev=g['evaluated'] or 0; rc=g['recovered'] or 0
        post_rows.append(f"<tr><td>{html.escape(g['strategy'])}</td><td>{g['timeframe'].upper()}</td><td class='{g['side'].lower()}'>{'شراء' if g['side']=='BUY' else 'بيع'}</td><td>{g['tracked']}</td><td>{ev}</td><td><b>{pct(rc,ev)}</b></td><td>{float(g['avg_recovery_worst_r'] or 0):.2f}R</td><td>{g['recovered_105'] or 0}</td><td>{g['recovered_110'] or 0}</td><td>{g['recovered_115'] or 0}</td><td>{g['recovered_120'] or 0}</td><td>{g['recovered_125'] or 0}</td><td>{g['recovered_150'] or 0}</td></tr>")
    rec=[]
    for r in st['recent']:
        status=r['result'] or ('LIVE_TP2' if r['tp2_hit_ts'] else 'LIVE_TP1' if r['tp1_hit_ts'] else 'OPEN'); cls='win' if r['tp1_hit_ts'] else 'loss' if r['first_hit']=='SL' else 'open'
        rec.append(f"<tr><td>#{r['id']}</td><td>{r['symbol']}</td><td>{html.escape(r['strategy'])}</td><td>{r['timeframe'].upper()}</td><td class='{r['side'].lower()}'>{r['side']}</td><td>{r['entry']:.8g}</td><td class='{cls}'>{status}</td><td>{float(r['best_r'] or 0):.2f}R</td><td>{float(r['worst_r'] or 0):.2f}R</td></tr>")
    return f"""<!doctype html><html lang='ar' dir='rtl'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Ahmed Strategy Fusion Stats</title><style>body{{font-family:Arial;background:#0b0f14;color:#e9eef5;margin:0;padding:22px}}h1{{margin:0 0 6px}}.sub{{color:#94a3b8;margin-bottom:18px}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:22px}}.cards div{{background:#121922;border:1px solid #263244;border-radius:12px;padding:15px}}.cards b{{font-size:25px;display:block}}.cards span{{color:#9fb0c3}}table{{width:100%;border-collapse:collapse;background:#111820;margin-bottom:26px}}th,td{{padding:10px;border-bottom:1px solid #263244;text-align:right;font-size:13px}}th{{position:sticky;top:0;background:#182231}}.buy,.win{{color:#32d583}}.sell,.loss{{color:#ff6b6b}}.open{{color:#fdb022}}.wrap{{overflow:auto;border-radius:12px;border:1px solid #263244}}a{{color:#7dd3fc}}</style></head><body><h1>📊 Ahmed Strategy Fusion Bot</h1><div class='sub'>v1.3.1 SL STUDY — نفس الاستراتيجيات والوقف والأهداف؛ تمت إضافة دراسة افتراضية بعد SL فقط</div>{cards}<h2>🏆 ترتيب الاستراتيجيات حسب النجاح</h2><div class='wrap'><table><thead><tr><th>الاستراتيجية</th><th>النوع</th><th>الإشارات</th><th>المقيّمة</th><th>نجاح TP1</th><th>TP1</th><th>TP2</th><th>TP3</th><th>SL أولاً</th><th>متوسط TP1</th><th>Avg MFE</th><th>Avg MAE</th></tr></thead><tbody>{''.join(top)}</tbody></table></div><h2>تفصيل الفريم والاتجاه</h2><div class='wrap'><table><thead><tr><th>الاستراتيجية</th><th>الفريم</th><th>الاتجاه</th><th>الإشارات</th><th>المقيّمة</th><th>نجاح TP1</th><th>TP1</th><th>TP2</th><th>TP3</th><th>SL أولاً</th><th>متوسط TP1</th><th>Avg MFE</th><th>Avg MAE</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div><h2>🧪 دراسة ما بعد وقف الخسارة</h2><div class='sub'>هذه الدراسة لا تغيّر الوقف ولا نتيجة الصفقة الأصلية. تُحسب فقط للصفقات التي ضربت SL أولًا بعد تشغيل v1.3.1، ثم نراقب هل رجعت إلى TP1 وما أقصى انعكاس قبل الرجوع.</div>{post_cards}<div class='wrap'><table><thead><tr><th>الاستراتيجية</th><th>الفريم</th><th>الاتجاه</th><th>SL متتبعة</th><th>مكتملة</th><th>رجعت TP1</th><th>متوسط أقصى انعكاس</th><th>≤1.05R</th><th>≤1.10R</th><th>≤1.15R</th><th>≤1.20R</th><th>≤1.25R</th><th>≤1.50R</th></tr></thead><tbody>{''.join(post_rows)}</tbody></table></div><h2>آخر 60 تنبيه</h2><div class='wrap'><table><thead><tr><th>ID</th><th>العملة</th><th>الاستراتيجية</th><th>الفريم</th><th>النوع</th><th>الدخول</th><th>النتيجة</th><th>MFE</th><th>MAE</th></tr></thead><tbody>{''.join(rec)}</tbody></table></div><p><a href='/stats.json'>JSON</a> · <a href='/health'>Health</a></p></body></html>"""

def now_ksa():
    return datetime.now(KSA)


def _retry_after_seconds(response):
    raw = response.headers.get("Retry-After")
    if raw:
        try:
            return max(1, int(float(raw)))
        except Exception:
            pass
    return REST_418_BACKOFF


def get_json(path, params=None, timeout=10, allow_wait=False):
    """Rate-limited REST with 418/429 protection. Never hammers Binance while blocked."""
    with rest_lock:
        now = time.time()
        blocked = float(state.get("rest_blocked_until", 0) or 0)
        if now < blocked:
            if not allow_wait:
                with state_lock:
                    state["rest_skipped_backoff"] += 1
                raise RuntimeError(f"REST_BACKOFF {int(blocked-now)}s")
            # Never hold the global REST lock for a multi-minute/hour Binance ban.
            raise RuntimeError(f"REST_BACKOFF {int(blocked-now)}s")

        last = getattr(get_json, "_last_call", 0.0)
        wait = REST_MIN_INTERVAL - (time.time() - last)
        if wait > 0:
            time.sleep(wait)

        r = session.get(REST_BASE + path, params=params, timeout=timeout)
        get_json._last_call = time.time()

        if r.status_code in (418, 429):
            backoff = _retry_after_seconds(r)
            with state_lock:
                state["rest_blocked_until"] = time.time() + backoff
                if r.status_code == 418:
                    state["rest_418_count"] += 1
                else:
                    state["rest_429_count"] += 1
                state["last_error"] = f"Binance REST {r.status_code}; backoff {backoff}s"
            raise RuntimeError(f"Binance REST {r.status_code}; backoff {backoff}s")

        r.raise_for_status()
        return r.json()


def raw_klines(symbol, interval, limit=None):
    limit = limit or BOOTSTRAP_LIMIT
    raw = get_json("/fapi/v1/klines", {"symbol":symbol,"interval":interval,"limit":limit}, allow_wait=False)
    out = []
    for x in raw:
        out.append({
            "open_time": int(x[0]), "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
            "close": float(x[4]), "volume": float(x[5]), "close_time": int(x[6]),
            "quote_volume": float(x[7]), "trades": int(x[8]),
            "taker_buy_base": float(x[9]), "taker_buy_quote": float(x[10]), "ignore": x[11]
        })
    return out


def df_from_store(symbol, tf):
    with data_lock:
        rows = list(candle_store.get((symbol, tf), []))
    if len(rows) < 40:
        return None
    return enrich(pd.DataFrame(rows))


def enrich(df):
    prev = df["close"].shift(1)
    tr = pd.concat([(df.high-df.low).abs(), (df.high-prev).abs(), (df.low-prev).abs()], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()
    df["ema20"] = df.close.ewm(span=20, adjust=False).mean()
    df["ema50"] = df.close.ewm(span=50, adjust=False).mean()
    df["ema200"] = df.close.ewm(span=200, adjust=False).mean()
    mid = df.close.rolling(20).mean(); std = df.close.rolling(20).std()
    df["bb_mid"] = mid; df["bb_up"] = mid + 2*std; df["bb_dn"] = mid - 2*std
    tp = (df.high+df.low+df.close)/3
    df["vwap"] = (tp*df.volume).rolling(60).sum() / df.volume.rolling(60).sum().replace(0,np.nan)
    delta = df["taker_buy_base"] - (df["volume"]-df["taker_buy_base"])
    df["delta"] = delta
    df["cvd"] = delta.cumsum()
    df["vol_ma"] = df.volume.rolling(20).mean()
    df["range"] = df.high-df.low
    df["body"] = (df.close-df.open).abs()
    d = df.close.diff(); gain=d.clip(lower=0).rolling(14).mean(); loss=(-d.clip(upper=0)).rolling(14).mean()
    rs = gain/loss.replace(0,np.nan); df["rsi"] = 100-(100/(1+rs))
    fast=df.close.ewm(span=12,adjust=False).mean(); slow=df.close.ewm(span=26,adjust=False).mean()
    df["macd"] = fast-slow; df["macd_signal"] = df.macd.ewm(span=9,adjust=False).mean()
    return df



def _aggregate_ohlcv(df, bars_per_group):
    if df is None or len(df) < bars_per_group*3:
        return None
    d=df.iloc[:-1].copy().reset_index(drop=True) if len(df)>1 else df.copy().reset_index(drop=True)
    rows=[]
    for start in range(0,len(d),bars_per_group):
        g=d.iloc[start:start+bars_per_group]
        if len(g)<max(2,bars_per_group//2):
            continue
        vol=float(g.volume.sum())
        buy=float(g.taker_buy_base.sum()) if "taker_buy_base" in g.columns else vol/2
        rows.append({"open":float(g.open.iloc[0]),"high":float(g.high.max()),"low":float(g.low.min()),
                     "close":float(g.close.iloc[-1]),"volume":vol,"taker_buy_base":buy,
                     "delta":float(2*buy-vol),"range":float(g.high.max()-g.low.min())})
    return pd.DataFrame(rows) if rows else None


def _simple_htf_direction(df):
    if df is None or len(df)<4:
        return "NEUTRAL"
    d=df.tail(min(30,len(df)))
    c=d.close.astype(float)
    fast=float(c.tail(min(5,len(c))).mean()); slow=float(c.tail(min(12,len(c))).mean()); last=float(c.iloc[-1])
    if fast>slow and last>=slow: return "UP"
    if fast<slow and last<=slow: return "DOWN"
    return "NEUTRAL"


def _direction_ar(tf, direction):
    names={"1h":"1H","4h":"4H","1d":"Daily","1w":"Weekly"}
    labels={"UP":"صاعد ↑","DOWN":"هابط ↓","NEUTRAL":"محايد →"}
    return f"{names.get(tf,tf.upper())}: {labels.get(direction,'محايد →')}"


def _higher_tf_views(dfs):
    d4=dfs.get("4h")
    return (_aggregate_ohlcv(d4,6) if d4 is not None else None,
            _aggregate_ohlcv(d4,42) if d4 is not None else None)



def _price_bucket(price):
    width=max(abs(float(price))*FOOTPRINT_BIN_BPS/10000.0, 1e-12)
    return round(float(price)/width)*width


def _fp_new_bucket(start_ts):
    return {
        "start_ts":float(start_ts),
        "levels":{},  # price -> [buy_quote, sell_quote, trades]
        "buy_quote":0.0,
        "sell_quote":0.0,
        "trades":0,
        "last_price":None,
    }


def _fp_finalize(symbol, tf, bucket):
    if not bucket or bucket.get("trades",0) < FOOTPRINT_MIN_TRADES:
        return None
    levels=bucket.get("levels") or {}
    if not levels:
        return None
    total_buy=float(bucket.get("buy_quote",0))
    total_sell=float(bucket.get("sell_quote",0))
    total=total_buy+total_sell
    if total<=0:
        return None

    ordered=[]
    for price,vals in levels.items():
        bq,sq,tr=vals
        lvl_total=bq+sq
        if lvl_total<=0:
            continue
        ordered.append((float(price),float(bq),float(sq),int(tr),float(lvl_total)))
    if not ordered:
        return None
    ordered.sort(key=lambda x:x[0])

    poc=max(ordered,key=lambda x:x[4])[0]
    delta_pct=100.0*(total_buy-total_sell)/max(total,1e-12)

    buy_imb=[]; sell_imb=[]
    for price,bq,sq,tr,lvl_total in ordered:
        share=lvl_total/total
        if share < FOOTPRINT_MIN_LEVEL_SHARE:
            continue
        if bq/max(sq,1e-12) >= FOOTPRINT_IMBALANCE_RATIO:
            buy_imb.append(price)
        if sq/max(bq,1e-12) >= FOOTPRINT_IMBALANCE_RATIO:
            sell_imb.append(price)

    def longest_stack(prices):
        if not prices:
            return 0
        ps=sorted(prices)
        # Native price spacing can vary slightly with adaptive bps bins.
        # Count neighboring imbalance levels as a stack when spacing is small.
        best=cur=1
        for a,b in zip(ps,ps[1:]):
            if (b-a)/max(abs(a),1e-12) <= (FOOTPRINT_BIN_BPS/10000.0)*2.5:
                cur+=1; best=max(best,cur)
            else:
                cur=1
        return best

    buy_stack=longest_stack(buy_imb)
    sell_stack=longest_stack(sell_imb)

    if delta_pct > 0:
        side="BUY"; imb=buy_imb; stack=buy_stack
    elif delta_pct < 0:
        side="SELL"; imb=sell_imb; stack=sell_stack
    else:
        return None

    dominant=abs(delta_pct)
    poc_row=min(ordered,key=lambda x:abs(x[0]-poc))
    poc_share=poc_row[4]/total
    score=45.0 + min(25.0,dominant*0.55) + min(18.0,stack*6.0) + min(11.0,poc_share*90.0)
    score=min(99.0,score)

    # Strong blocks are centered on actual aggressive-volume imbalance levels.
    if imb:
        # Use nearest imbalance cluster around POC, max 5 levels.
        chosen=sorted(imb,key=lambda x:abs(x-poc))[:5]
        zone_low=min(chosen); zone_high=max(chosen)
    else:
        width=max(abs(poc)*FOOTPRINT_BIN_BPS/10000.0,1e-12)
        zone_low=poc-width; zone_high=poc+width

    summary={
        "symbol":symbol,"tf":tf,"side":side,"score":round(score,1),
        "low":float(zone_low),"high":float(zone_high),"poc":float(poc),
        "delta_pct":round(delta_pct,2),
        "buy_quote":round(total_buy,2),"sell_quote":round(total_sell,2),
        "stacked_imbalance":int(stack),"trades":int(bucket.get("trades",0)),
        "start_ts":float(bucket["start_ts"]),
        "end_ts":float(bucket["start_ts"]+FOOTPRINT_TF_SECONDS[tf]),
        "source":"Binance aggTrade"
    }
    return summary


def _persist_fp(summary):
    try:
        with db_lock:
            c=db_conn()
            c.execute("""INSERT INTO footprint_blocks(
                symbol,timeframe,start_ts,end_ts,side,score,zone_low,zone_high,poc,delta_pct,
                total_buy_quote,total_sell_quote,stacked_imbalance,trades,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
                summary["symbol"],summary["tf"],summary["start_ts"],summary["end_ts"],summary["side"],
                summary["score"],summary["low"],summary["high"],summary["poc"],summary["delta_pct"],
                summary["buy_quote"],summary["sell_quote"],summary["stacked_imbalance"],summary["trades"],
                now_ksa().isoformat()
            ))
            c.commit(); c.close()
    except Exception as e:
        print("persist footprint",e)


def load_footprint_history():
    try:
        with db_lock:
            c=db_conn()
            rows=c.execute("""SELECT * FROM footprint_blocks
                ORDER BY end_ts DESC LIMIT 5000""").fetchall()
            c.close()
        with footprint_lock:
            footprint_history.clear()
            for r in reversed(rows):
                d=dict(r)
                key=(d["symbol"],d["timeframe"])
                footprint_history[key].append({
                    "symbol":d["symbol"],"tf":d["timeframe"],"side":d["side"],"score":float(d["score"]),
                    "low":float(d["zone_low"]),"high":float(d["zone_high"]),"poc":float(d["poc"]),
                    "delta_pct":float(d["delta_pct"]),"buy_quote":float(d["total_buy_quote"]),
                    "sell_quote":float(d["total_sell_quote"]),"stacked_imbalance":int(d["stacked_imbalance"]),
                    "trades":int(d["trades"]),"start_ts":float(d["start_ts"]),"end_ts":float(d["end_ts"]),
                    "source":"Binance aggTrade"
                })
    except Exception as e:
        print("load footprint history",e)


def process_aggtrade_event(data):
    symbol=data.get("s")
    if not symbol or symbol not in radar_set:
        return
    try:
        price=float(data["p"]); qty=float(data["q"])
        event_ts=float(data.get("T") or data.get("E") or int(time.time()*1000))/1000.0
        buyer_is_maker=bool(data.get("m"))
    except Exception:
        return
    trade_id=data.get("a")
    quote=price*qty
    aggressor="SELL" if buyer_is_maker else "BUY"
    pb=_price_bucket(price)

    completed=[]
    with footprint_lock:
        # Ignore a duplicate aggregate-trade event, which can occur around reconnects.
        if trade_id is not None:
            dedup_key=(symbol,str(trade_id))
            if dedup_key in aggtrade_seen_set:
                return
            if len(aggtrade_seen_order) >= FOOTPRINT_SEEN_IDS_MAX:
                aggtrade_seen_set.discard(aggtrade_seen_order.popleft())
            aggtrade_seen_order.append(dedup_key); aggtrade_seen_set.add(dedup_key)
        for tf in FOOTPRINT_TFS:
            secs=FOOTPRINT_TF_SECONDS[tf]
            start_ts=(int(event_ts)//secs)*secs
            key=(symbol,tf)
            bucket=footprint_live.get(key)
            if bucket is not None and start_ts < int(float(bucket.get("start_ts",-1))):
                # Late packet: never rewind or overwrite the active chronological bucket.
                continue
            if bucket is None or float(bucket.get("start_ts",-1)) != float(start_ts):
                if bucket is not None:
                    sm=_fp_finalize(symbol,tf,bucket)
                    if sm:
                        footprint_history[key].append(sm)
                        completed.append(sm)
                bucket=_fp_new_bucket(start_ts)
                footprint_live[key]=bucket
            lvl=bucket["levels"].setdefault(pb,[0.0,0.0,0])
            if aggressor=="BUY":
                lvl[0]+=quote; bucket["buy_quote"]+=quote
            else:
                lvl[1]+=quote; bucket["sell_quote"]+=quote
            lvl[2]+=1
            bucket["trades"]+=1
            bucket["last_price"]=price

    for sm in completed:
        _persist_fp(sm)
        with state_lock:
            state["footprint_blocks_completed"] += 1
    with state_lock:
        state["aggtrade_events"] += 1
        state["aggtrade_last_event"]=now_ksa().isoformat()


def _current_fp_summary(symbol,tf):
    with footprint_lock:
        bucket=footprint_live.get((symbol,tf))
        if not bucket:
            return None
        # Snapshot so finalize cannot mutate shared structures.
        snap={
            "start_ts":bucket["start_ts"],
            "levels":{k:list(v) for k,v in bucket["levels"].items()},
            "buy_quote":bucket["buy_quote"],"sell_quote":bucket["sell_quote"],
            "trades":bucket["trades"],"last_price":bucket["last_price"]
        }
    return _fp_finalize(symbol,tf,snap)


def _aggtrade_volumetric_confluence(symbol, side, entry, dfs):
    """Use only REAL Binance aggTrade footprint summaries; no kline proxy."""
    found=[]
    frames=("15m","1h","4h","1d","1w")
    for tf in frames:
        candidates=[]
        live=_current_fp_summary(symbol,tf)
        if live:
            candidates.append(live)
        with footprint_lock:
            candidates.extend(list(footprint_history.get((symbol,tf),[]))[-6:])
        # ATR/proximity reference. For Daily/Weekly fall back to 4H ATR scaled.
        refdf=dfs.get(tf)
        if refdf is None:
            refdf=dfs.get("4h")
        if refdf is None or len(refdf)<3:
            continue
        atr=float((refdf.high-refdf.low).tail(min(14,len(refdf))).mean())
        if tf=="1d":
            atr*=2.2
        elif tf=="1w":
            atr*=5.0
        if atr<=0:
            continue

        for b in candidates:
            if b.get("side")!=side or float(b.get("score",0))<VOL_OF_MIN_SCORE:
                continue
            dist=max(float(b["low"])-entry,entry-float(b["high"]),0.0)/atr
            if dist<=VOL_OF_NEAR_ATR:
                bb=dict(b); bb["entry_distance_atr"]=round(dist,2); found.append(bb)

    # Deduplicate nearly identical current/history summaries.
    uniq=[]
    seen=set()
    for b in sorted(found,key=lambda x:(x.get("score",0),x.get("end_ts",0)),reverse=True):
        k=(b.get("tf"),round(float(b.get("poc",0)),8),b.get("side"))
        if k not in seen:
            seen.add(k); uniq.append(b)
    return uniq


def on_agg_ws_message(ws,message):
    try:
        obj=json.loads(message)
        data=obj.get("data",obj)
        if isinstance(data,dict) and data.get("e")=="aggTrade":
            process_aggtrade_event(data)
    except Exception as e:
        print("aggTrade WS message error",e)


def on_agg_ws_open(ws):
    global agg_ws_app
    agg_ws_app=ws
    with state_lock:
        state["aggtrade_ws_connected"]=True
    streams=[f"{x.lower()}@aggTrade" for x in list(radar_symbols)]
    for i in range(0,len(streams),100):
        ws.send(json.dumps({"method":"SUBSCRIBE","params":streams[i:i+100],"id":5000+i}))
        time.sleep(0.12)
    with agg_ws_lock:
        agg_subscribed.update(streams)
    print("aggTrade footprint WebSocket connected")


def on_agg_ws_close(ws,code,msg):
    with state_lock:
        state["aggtrade_ws_connected"]=False
    print("aggTrade WebSocket closed",code,msg)


def on_agg_ws_error(ws,error):
    print("aggTrade WebSocket error",error)


def subscribe_aggtrade_streams(symbols):
    global agg_ws_app
    if not FOOTPRINT_STREAM_ENABLED:
        return
    wanted=[f"{x.lower()}@aggTrade" for x in symbols]
    with agg_ws_lock:
        fresh=[x for x in wanted if x not in agg_subscribed]
        app_ref=agg_ws_app
    if not app_ref or not fresh:
        return
    for i in range(0,len(fresh),100):
        try:
            app_ref.send(json.dumps({"method":"SUBSCRIBE","params":fresh[i:i+100],"id":7000+i}))
            with agg_ws_lock:
                agg_subscribed.update(fresh[i:i+100])
            time.sleep(0.08)
        except Exception as e:
            print("aggTrade subscribe",e)
            return


def aggtrade_websocket_worker():
    global agg_ws_app
    if not FOOTPRINT_STREAM_ENABLED:
        return
    while True:
        try:
            agg_ws_app=websocket.WebSocketApp(
                WS_MARKET,on_open=on_agg_ws_open,on_message=on_agg_ws_message,
                on_close=on_agg_ws_close,on_error=on_agg_ws_error
            )
            agg_ws_app.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            print("aggTrade WS worker exception",e)
        with state_lock:
            state["aggtrade_ws_connected"]=False
        time.sleep(WS_RECONNECT_SECONDS)



def helpers(df, side):
    x=df.iloc[-1]; p=df.iloc[-2]; bull = side=="BUY"
    checks = {
        "VolumeSpike": x.volume > 1.35*x.vol_ma if pd.notna(x.vol_ma) else False,
        "Delta": x.delta > 0 if bull else x.delta < 0,
        "CVD": x.cvd > p.cvd if bull else x.cvd < p.cvd,
        "VWAP": x.close >= x.vwap if bull else x.close <= x.vwap,
        "EMA": x.close >= x.ema50 if bull else x.close <= x.ema50,
        "MACD": x.macd >= x.macd_signal if bull else x.macd <= x.macd_signal,
        "BollingerLocation": x.close > x.bb_mid if bull else x.close < x.bb_mid,
    }
    return [k for k,v in checks.items() if bool(v)]


def _pivot_points(df, span=None, lookback=None):
    """Confirmed swing highs/lows from CLOSED bars only; current live bar never defines structure."""
    span=max(1,int(span or TRADER_PIVOT_SPAN)); lookback=max(25,int(lookback or TRADER_SWING_LOOKBACK))
    d=df.iloc[:-1].tail(lookback).reset_index(drop=True)
    highs=[]; lows=[]
    if len(d)<2*span+5: return highs,lows
    for i in range(span,len(d)-span):
        h=float(d.high.iloc[i]); l=float(d.low.iloc[i])
        if h>=float(d.high.iloc[i-span:i+span+1].max()): highs.append((i,h))
        if l<=float(d.low.iloc[i-span:i+span+1].min()): lows.append((i,l))
    return highs[-8:],lows[-8:]


def _structure_state(highs,lows):
    if len(highs)<2 or len(lows)<2: return "MIXED",0.0
    h1,h2=highs[-2][1],highs[-1][1]; l1,l2=lows[-2][1],lows[-1][1]
    if h2>h1 and l2>l1: return "HH_HL",1.0
    if h2<h1 and l2<l1: return "LH_LL",-1.0
    if h2>h1 and l2<=l1: return "EXPANDING",0.0
    return "RANGE",0.0


def _regime_state(df, atr):
    d=df.iloc[-21:-1]
    if len(d)<12: return "UNKNOWN",0.0
    path=float(d.close.diff().abs().sum())
    eff=abs(float(d.close.iloc[-1]-d.close.iloc[0]))/max(path,1e-12)
    rng=float(d.high.max()-d.low.min())/max(atr,1e-12)
    widths=((d.bb_up-d.bb_dn)/d.bb_mid.abs().replace(0,np.nan)).dropna() if 'bb_up' in d else pd.Series(dtype=float)
    comp=False
    if len(widths)>=8:
        hist=((df.bb_up-df.bb_dn)/df.bb_mid.abs().replace(0,np.nan)).dropna().iloc[-80:-1]
        if len(hist)>=20: comp=float(widths.iloc[-1])<=float(hist.quantile(0.30))
    if comp and rng<=8.0: return "COMPRESSION",eff
    if eff>=0.42: return "TREND",eff
    if eff<=0.24: return "RANGE",eff
    return "TRANSITION",eff


def _latest_impulse(df, atr):
    """Describe the active leg and whether current price is a pullback or already extended."""
    d=df.iloc[-13:]
    if len(d)<8: return {"dir":"FLAT","size_atr":0,"retracement":0,"move3_atr":0}
    c=float(d.close.iloc[-1]); base=float(d.close.iloc[0]); net=c-base
    direction="UP" if net>0.35*atr else "DOWN" if net<-0.35*atr else "FLAT"
    size=abs(net)/max(atr,1e-12)
    move3=abs(float(d.close.iloc[-1]-d.close.iloc[-4]))/max(atr,1e-12)
    hi=float(d.high.max()); lo=float(d.low.min())
    retr=0.0
    if direction=="UP" and hi>lo: retr=max(0.0,min(1.5,(hi-c)/(hi-lo)))
    if direction=="DOWN" and hi>lo: retr=max(0.0,min(1.5,(c-lo)/(hi-lo)))
    return {"dir":direction,"size_atr":size,"retracement":retr,"move3_atr":move3,"hi":hi,"lo":lo}


def _tf_trade_read(df, tf):
    """Full market read of one timeframe, independent from all named strategies."""
    if df is None or len(df)<55: return None
    x=df.iloc[-1]; p=df.iloc[-2]
    if any(pd.isna(v) for v in [x.atr,x.ema20,x.ema50,x.vwap,x.bb_up,x.bb_dn]): return None
    atr=max(float(x.atr),1e-12); price=float(x.close)
    highs,lows=_pivot_points(df); structure,struct_dir=_structure_state(highs,lows)
    regime,eff=_regime_state(df,atr); impulse=_latest_impulse(df,atr)
    last_high=highs[-1][1] if highs else float(df.iloc[-21:-1].high.max())
    last_low=lows[-1][1] if lows else float(df.iloc[-21:-1].low.min())
    prev_high=highs[-2][1] if len(highs)>1 else last_high
    prev_low=lows[-2][1] if len(lows)>1 else last_low

    bull=bear=0.0; bn=[]; sn=[]
    # Market structure is dominant.
    if struct_dir>0: bull+=28; bn.append(f"{tf.upper()} HH/HL")
    elif struct_dir<0: bear+=28; sn.append(f"{tf.upper()} LH/LL")
    elif structure=="RANGE": bn.append(f"{tf.upper()} نطاق"); sn.append(f"{tf.upper()} نطاق")
    # Trend/value relation.
    if price>float(x.ema20)>float(x.ema50): bull+=16; bn.append(f"{tf.upper()} فوق EMA20/50")
    elif price<float(x.ema20)<float(x.ema50): bear+=16; sn.append(f"{tf.upper()} تحت EMA20/50")
    elif float(x.ema20)>float(x.ema50): bull+=7
    elif float(x.ema20)<float(x.ema50): bear+=7
    # Active wave.
    if impulse['dir']=='UP': bull+=min(12,4+impulse['size_atr']*2)
    elif impulse['dir']=='DOWN': bear+=min(12,4+impulse['size_atr']*2)
    # Flow only supports the analysis; never creates it.
    if float(x.delta)>0 and float(x.cvd)>=float(p.cvd): bull+=7
    elif float(x.delta)<0 and float(x.cvd)<=float(p.cvd): bear+=7
    if pd.notna(x.macd) and pd.notna(x.macd_signal):
        if float(x.macd)>=float(x.macd_signal): bull+=3
        else: bear+=3
    if pd.notna(x.vol_ma) and float(x.vol_ma)>0 and float(x.volume)>1.30*float(x.vol_ma):
        if price>float(x.open): bull+=4
        elif price<float(x.open): bear+=4

    # Real trade locations: structural swing, VWAP/EMA value, and range edges.
    supports=[v for v in [last_low,prev_low,float(x.ema20),float(x.vwap)] if v<price]
    resistances=[v for v in [last_high,prev_high,float(x.ema20),float(x.vwap)] if v>price]
    support=max(supports) if supports else last_low
    resistance=min(resistances) if resistances else last_high
    dist_support=(price-support)/atr; dist_resist=(resistance-price)/atr
    near_support=dist_support<=TRADER_NEAR_ZONE_ATR
    near_resistance=dist_resist<=TRADER_NEAR_ZONE_ATR

    # Liquidity facts are part of market analysis, not an invocation of a strategy.
    closed=df.iloc[-22:-1]; prior_lo=float(closed.low.min()); prior_hi=float(closed.high.max())
    sweep_low=float(x.low)<prior_lo-0.03*atr and price>=prior_lo-0.10*atr
    sweep_high=float(x.high)>prior_hi+0.03*atr and price<=prior_hi+0.10*atr
    if sweep_low: bull+=10; bn.append(f"{tf.upper()} سحب سيولة قاع + استرداد")
    if sweep_high: bear+=10; sn.append(f"{tf.upper()} سحب سيولة قمة + رفض")

    # Exhaustion/chase: three-bar move, distance from value, and current large body.
    value=(float(x.ema20)+float(x.vwap))/2.0
    dist_value=(price-value)/atr
    body=abs(price-float(x.open))/atr
    chase_buy=max(0.0,dist_value,impulse['move3_atr']-0.65)
    chase_sell=max(0.0,-dist_value,impulse['move3_atr']-0.65)
    exhausted_buy=(chase_buy>TRADER_MAX_CHASE_ATR or (impulse['dir']=='UP' and impulse['move3_atr']>TRADER_MAX_IMPULSE_ATR) or (body>1.15 and price>float(x.bb_up)))
    exhausted_sell=(chase_sell>TRADER_MAX_CHASE_ATR or (impulse['dir']=='DOWN' and impulse['move3_atr']>TRADER_MAX_IMPULSE_ATR) or (body>1.15 and price<float(x.bb_dn)))

    # Compression at a structural edge can be a legitimate prospective trade area, without naming a strategy.
    breakout_buy=regime=='COMPRESSION' and abs(price-last_high)/atr<=0.40 and bull>=bear
    breakout_sell=regime=='COMPRESSION' and abs(price-last_low)/atr<=0.40 and bear>=bull

    return {"tf":tf,"bull":bull,"bear":bear,"atr":atr,"close":price,"structure":structure,"regime":regime,"efficiency":eff,
            "impulse":impulse,"last_high":last_high,"last_low":last_low,"support":support,"resistance":resistance,
            "near_support":near_support,"near_resistance":near_resistance,"sweep_low":sweep_low,"sweep_high":sweep_high,
            "breakout_buy":breakout_buy,"breakout_sell":breakout_sell,"chase_buy":chase_buy,"chase_sell":chase_sell,
            "exhausted_buy":exhausted_buy,"exhausted_sell":exhausted_sell,"bull_notes":bn,"bear_notes":sn}


def _plan_zone(side, r15, r1):
    atr=r15['atr']; price=r15['close']
    if side=='BUY':
        candidates=[v for v in [r15['support'],r15['last_low'],r1['support']] if v<price]
        center=max(candidates) if candidates else r15['support']
        low=center-TRADER_ZONE_BUFFER_ATR*atr; high=center+TRADER_ZONE_BUFFER_ATR*atr
        invalid=min(r15['last_low'],center)-COIN_STOP_BUFFER_ATR*atr
        target_candidates=[v for v in [r15['resistance'],r15['last_high'],r1['resistance'],r1['last_high']] if v>price]
        first_target=min(target_candidates) if target_candidates else price+2*atr
    else:
        candidates=[v for v in [r15['resistance'],r15['last_high'],r1['resistance']] if v>price]
        center=min(candidates) if candidates else r15['resistance']
        low=center-TRADER_ZONE_BUFFER_ATR*atr; high=center+TRADER_ZONE_BUFFER_ATR*atr
        invalid=max(r15['last_high'],center)+COIN_STOP_BUFFER_ATR*atr
        target_candidates=[v for v in [r15['support'],r15['last_low'],r1['support'],r1['last_low']] if v<price]
        first_target=max(target_candidates) if target_candidates else price-2*atr
    return float(low),float(high),float(invalid),float(first_target)


def build_coin_context(symbol, dfs):
    """Trader-first analysis: decide if WE would enter this coin before any strategy detector runs."""
    reads={tf:_tf_trade_read(dfs.get(tf),tf) for tf in ('4h','1h','15m')}
    if not all(reads.values()):
        ctx={"symbol":symbol,"ts":time.time(),"decision":"WAIT","preferred":"NEUTRAL","buy_score":0,"sell_score":0,"tradeable":False,"location":"بيانات غير كافية","reasons_buy":[],"reasons_sell":[],
             "trend_1h":"NEUTRAL","trend_4h":"NEUTRAL","trend_1d":"NEUTRAL","trend_1w":"NEUTRAL"}
        with coin_context_lock: coin_context_cache[symbol]=ctx
        return ctx
    r4,r1,r15=reads['4h'],reads['1h'],reads['15m']; price=r15['close']; atr=r15['atr']
    daily_df,weekly_df=_higher_tf_views(dfs)
    trend_1h=_simple_htf_direction(dfs.get("1h"))
    trend_4h=_simple_htf_direction(dfs.get("4h"))
    trend_1d=_simple_htf_direction(daily_df)
    trend_1w=_simple_htf_direction(weekly_df)
    # 4H thesis, 1H active swing, 15M execution. 15M can be countertrend while pulling back.
    buy=0.48*r4['bull']+0.34*r1['bull']+0.18*r15['bull']
    sell=0.48*r4['bear']+0.34*r1['bear']+0.18*r15['bear']
    buy=min(100.0,buy); sell=min(100.0,sell)
    bull_thesis=(r4['bull']>r4['bear']+5 and r1['bull']>=r1['bear']-3)
    bear_thesis=(r4['bear']>r4['bull']+5 and r1['bear']>=r1['bull']-3)
    side='BUY' if bull_thesis and buy>=TRADER_MIN_PLAN_SCORE and buy>sell+7 else 'SELL' if bear_thesis and sell>=TRADER_MIN_PLAN_SCORE and sell>buy+7 else None

    decision='WAIT'; tradeable=False; location='لا توجد صفقة مناسبة الآن'; entry_low=entry_high=invalid=first_target=None; rr_space=0.0
    if side:
        entry_low,entry_high,invalid,first_target=_plan_zone(side,r15,r1)
        risk=abs(price-invalid); room=abs(first_target-price); rr_space=room/max(risk,1e-12); plan_risk_atr=risk/max(atr,1e-12)
        inside=entry_low-0.12*atr<=price<=entry_high+0.12*atr
        near=(price-entry_high)/atr<=TRADER_NEAR_ZONE_ATR if side=='BUY' and price>entry_high else (entry_low-price)/atr<=TRADER_NEAR_ZONE_ATR if side=='SELL' and price<entry_low else inside
        market_event=(r15['sweep_low'] or r15['near_support'] or r15['breakout_buy']) if side=='BUY' else (r15['sweep_high'] or r15['near_resistance'] or r15['breakout_sell'])
        exhausted=r15['exhausted_buy'] if side=='BUY' else r15['exhausted_sell']
        # If trend is strong but price already ran away, WAIT. No strategy can override this.
        risk_ok=0.45<=plan_risk_atr<=COIN_STOP_MAX_ATR
        tradeable=bool(near and market_event and not exhausted and risk_ok and (rr_space>=0.85 or (r15['breakout_buy'] if side=='BUY' else r15['breakout_sell'])))
        if tradeable:
            decision=side; location='داخل/قرب منطقة دخول منطقية بعد تحليل العملة'
        elif exhausted:
            location=('الاتجاه صاعد لكن الحركة ممتدة — انتظار تصحيح' if side=='BUY' else 'الاتجاه هابط لكن الحركة ممتدة — انتظار ارتداد')
        elif not near:
            location=('الرأي شراء لكن السعر لم يصل منطقة الشراء' if side=='BUY' else 'الرأي بيع لكن السعر لم يصل منطقة البيع')
        else:
            location='الرأي موجود لكن مساحة الهدف/الموقع غير مناسبة الآن'

    ctx={"symbol":symbol,"ts":time.time(),"decision":decision,"preferred":side or 'NEUTRAL',"tradeable":tradeable,
         "buy_score":round(buy,1),"sell_score":round(sell,1),"location":location,"entry_zone_low":entry_low,"entry_zone_high":entry_high,
         "invalid_buy":invalid if side=='BUY' else None,"invalid_sell":invalid if side=='SELL' else None,"first_target":first_target,"rr_space":round(rr_space,2),"plan_risk_atr":round(plan_risk_atr,2) if side else None,
         "market_4h":f"{r4['structure']} / {r4['regime']}","market_1h":f"{r1['structure']} / {r1['regime']}","market_15m":f"{r15['structure']} / {r15['regime']}",
         "reasons_buy":(r4['bull_notes']+r1['bull_notes']+r15['bull_notes'])[-7:],"reasons_sell":(r4['bear_notes']+r1['bear_notes']+r15['bear_notes'])[-7:]}
    ctx["trend_1h"]=trend_1h
    ctx["trend_4h"]=trend_4h
    ctx["trend_1d"]=trend_1d
    ctx["trend_1w"]=trend_1w
    with coin_context_lock: coin_context_cache[symbol]=ctx
    return ctx


def context_allows_signal(ctx,s):
    side=s['side']; own=float(ctx.get('buy_score' if side=='BUY' else 'sell_score',0)); opp=float(ctx.get('sell_score' if side=='BUY' else 'buy_score',0))
    if not (ctx.get('tradeable') and ctx.get('decision')==side): return False,own,opp
    lo=ctx.get('entry_zone_low'); hi=ctx.get('entry_zone_high'); entry=float(s.get('entry',0) or 0)
    # Strategy setup must occur around the trade plan; it cannot create a trade elsewhere.
    if lo is not None and hi is not None:
        dfatr=max(float(s.get('setup_atr') or 0),1e-12)
        if entry < float(lo)-0.35*dfatr or entry > float(hi)+0.35*dfatr: return False,own,opp
    return True,own,opp


def _base_strategy_reasons(s):
    """Keep only the detector/confirmation reasons. Context reasons are regenerated, never accumulated."""
    if "_strategy_reasons" in s:
        return list(s["_strategy_reasons"])
    prefixes=(
        "قرار المحلل قبل الاستراتيجية:",
        "خطة التداول:",
        "الاستراتيجية موجودة فعليًا لكن الدخول غير مناسب الآن",
        "بوابة الدخول:",
    )
    base=[]
    for r in list(s.get("reasons",[]) or []):
        if not any(str(r).startswith(p) for p in prefixes):
            base.append(r)
    # stable de-duplication
    base=list(dict.fromkeys(base))
    s["_strategy_reasons"]=list(base)
    return base


def attach_coin_context(s,ctx):
    side=s['side']
    own=float(ctx.get('buy_score' if side=='BUY' else 'sell_score',0))
    opp=float(ctx.get('sell_score' if side=='BUY' else 'buy_score',0))
    s['coin_context']={
        "score":own,"opposition":opp,"preferred":ctx.get('preferred'),
        "decision":ctx.get('decision'),"location":ctx.get('location'),
        "entry_zone_low":ctx.get('entry_zone_low'),"entry_zone_high":ctx.get('entry_zone_high'),
        "rr_space":ctx.get('rr_space'),"plan_risk_atr":ctx.get('plan_risk_atr'),
        "market_4h":ctx.get('market_4h'),"market_1h":ctx.get('market_1h'),
        "market_15m":ctx.get('market_15m'),
        "trend_1h":ctx.get('trend_1h'),"trend_4h":ctx.get('trend_4h'),
        "trend_1d":ctx.get('trend_1d'),"trend_1w":ctx.get('trend_1w')
    }
    s['context_score']=own
    inv=ctx.get('invalid_buy' if side=='BUY' else 'invalid_sell')
    if inv is not None:
        s['coin_invalidation']=float(inv)

    rs=ctx.get('reasons_buy' if side=='BUY' else 'reasons_sell',[]) or []
    base=_base_strategy_reasons(s)
    context_reasons=[
        f"قرار المحلل قبل الاستراتيجية: {ctx.get('decision','WAIT')} ({own:.0f}%)",
        f"خطة التداول: {ctx.get('location','-')}",
    ] + list(rs[:3])
    s['reasons']=list(dict.fromkeys(base + context_reasons))
    return s

def _fast_live_context_confirmation(p,df,price):
    """Timing only. It cannot invent thesis or strategy; both must already exist."""
    if not FAST_CONTEXT_CONFIRM or df is None or len(df)<4 or price is None:return None
    s=p.get('setup',{}); ctx=s.get('coin_context') or {}
    if ctx.get('decision')!=p.get('side'):return None
    x=df.iloc[-1]; prev=df.iloc[-2]
    if pd.isna(x.atr):return None
    atr=max(float(x.atr),1e-12); entry0=float(p.get('setup_entry',price))
    if abs(float(price)-entry0)/atr>FAST_CONFIRM_MAX_DISTANCE_ATR:return None
    bull=p['side']=='BUY'; votes=0
    votes+=int((price>=x.open) if bull else (price<=x.open)); votes+=int((x.delta>0) if bull else (x.delta<0)); votes+=int((x.cvd>=prev.cvd) if bull else (x.cvd<=prev.cvd))
    if pd.notna(x.vwap): votes+=int((price>=x.vwap) if bull else (price<=x.vwap))
    if votes>=2:return {"confirmed":True,"entry":float(price),"atr":atr,"bar_open_time":int(x.open_time),"mode":"first live trigger after trader plan","fast":True}
    return None

def signal(strategy, side, tf, df, entry=None, invalid=None, quality=0, reasons=None):
    """Create a validated alert setup; never return geometrically invalid levels."""
    if side not in ("BUY", "SELL") or df is None or len(df) < 2:
        return None
    x=df.iloc[-1]
    atr=float(x.atr) if pd.notna(x.atr) and float(x.atr) > 0 else float(x.close)*0.01
    entry=float(entry if entry is not None else x.close)
    if not np.isfinite(entry) or entry <= 0 or not np.isfinite(atr) or atr <= 0:
        return None
    if invalid is None:
        invalid = entry-1.2*atr if side=="BUY" else entry+1.2*atr
    invalid=float(invalid)
    if not np.isfinite(invalid):
        return None
    # BUY must have SL below entry; SELL must have SL above entry.
    if (side=="BUY" and invalid >= entry) or (side=="SELL" and invalid <= entry):
        return None
    risk=abs(entry-invalid)
    if risk < 0.25*atr or risk > max(MAX_STOP_RISK_ATR, COIN_STOP_MAX_ATR)*atr:
        return None
    tps=[entry+risk*r if side=="BUY" else entry-risk*r for r in (1,2,3)]
    if not all(np.isfinite(tp) for tp in tps):
        return None
    if (side=="BUY" and not (entry < tps[0] < tps[1] < tps[2])) or (side=="SELL" and not (entry > tps[0] > tps[1] > tps[2])):
        return None
    return {"strategy":strategy,"side":side,"tf":tf,"entry":entry,"sl":invalid,"tp":tps,"quality":round(float(quality),1),"reasons":reasons or [],"setup_atr":atr}


def break_retest(df, tf):
    """REAL Break & Retest: a break must already exist; current live candle must actually revisit that level."""
    if len(df)<30:return []
    x=df.iloc[-1]; atr=float(x.atr) if pd.notna(x.atr) else None
    if not atr:return []
    out=[]
    # Search recent CLOSED bars for the first genuine break of structure.
    for j in range(2,7):
        b=df.iloc[-j]; prior=df.iloc[max(0,len(df)-j-22):len(df)-j-1]
        if len(prior)<12:continue
        res=float(prior.high.max()); sup=float(prior.low.min())
        broke_buy=float(b.close)>res+0.04*atr and float(b.high)>res
        broke_sell=float(b.close)<sup-0.04*atr and float(b.low)<sup
        if broke_buy:
            touched=float(x.low)<=res+CONFIRM_RETEST_ATR*atr and float(x.high)>=res-CONFIRM_RETEST_ATR*atr
            held=float(x.close)>=res-0.12*atr
            if touched and held and (x.close>x.open or x.delta>0):
                out.append(signal('Break & Retest','BUY',tf,df,x.close,min(float(x.low),res-0.25*atr),78,['كسر حقيقي سابق للمقاومة','عودة فعلية لمستوى الكسر','الثبات/الزناد يتم حيًا']))
                break
        if broke_sell:
            touched=float(x.high)>=sup-CONFIRM_RETEST_ATR*atr and float(x.low)<=sup+CONFIRM_RETEST_ATR*atr
            held=float(x.close)<=sup+0.12*atr
            if touched and held and (x.close<x.open or x.delta<0):
                out.append(signal('Break & Retest','SELL',tf,df,x.close,max(float(x.high),sup+0.25*atr),78,['كسر حقيقي سابق للدعم','عودة فعلية لمستوى الكسر','الثبات/الزناد يتم حيًا']))
                break
    return [z for z in out if z]

def liquidity_sweep(df, tf):
    """Sweep itself is the early event; reclaim can happen inside the live candle."""
    if len(df)<20:return []
    x=df.iloc[-1]; atr=x.atr; w=df.iloc[-18:-1]; out=[]
    if pd.isna(atr): return []
    lo=float(w.low.min()); hi=float(w.high.max())
    buy_sweep=x.low < lo-0.03*atr
    sell_sweep=x.high > hi+0.03*atr
    # A partial reclaim is enough to arm; the live confirmation gate decides the actual entry.
    if buy_sweep and x.close >= lo-0.12*atr and (x.close>x.open or x.delta>0):
        out.append(signal("Liquidity Sweep + Reclaim","BUY",tf,df,x.close,float(x.low)-0.18*atr,76,["سحب سيولة أسفل القاع","استرداد مبكر/رفض من منطقة الشراء"]))
    if sell_sweep and x.close <= hi+0.12*atr and (x.close<x.open or x.delta<0):
        out.append(signal("Liquidity Sweep + Reclaim","SELL",tf,df,x.close,float(x.high)+0.18*atr,76,["سحب سيولة أعلى القمة","استرداد مبكر/رفض من منطقة البيع"]))
    return out


def orderblock_sweep_bos(df, tf):
    """A valid OB must have caused displacement/structure break before its retest."""
    if len(df)<45:return []
    x=df.iloc[-1]; atr=float(x.atr) if pd.notna(x.atr) else None
    if not atr:return []
    out=[]; hist=df.iloc[-32:-3]
    # Bullish OB: last bearish candle followed by >=0.8 ATR displacement and a close above preceding local high.
    bears=hist[hist.close<hist.open]
    for idx,ob in bears.tail(6).iloc[::-1].iterrows():
        pos=df.index.get_loc(idx); after=df.iloc[pos+1:min(pos+7,len(df)-1)]; before=df.iloc[max(0,pos-8):pos]
        if after.empty or before.empty:continue
        displacement=float(after.high.max())-float(ob.low)
        bos=float(after.close.max())>float(before.high.max())+0.03*atr
        if displacement>=0.8*atr and bos:
            zl=float(min(ob.open,ob.close)); zh=float(ob.high)
            touched=float(x.low)<=zh+0.08*atr and float(x.high)>=zl-0.08*atr
            if touched and float(x.close)>=zl and (x.close>x.open or x.delta>0):
                out.append(signal('Order Block + Sweep + BOS','BUY',tf,df,x.close,min(float(x.low),zl)-0.18*atr,82,['Order Block سبّب اندفاعًا فعليًا','BOS سابق مثبت','إعادة اختبار حية للبلوك']))
                break
    bulls=hist[hist.close>hist.open]
    for idx,ob in bulls.tail(6).iloc[::-1].iterrows():
        pos=df.index.get_loc(idx); after=df.iloc[pos+1:min(pos+7,len(df)-1)]; before=df.iloc[max(0,pos-8):pos]
        if after.empty or before.empty:continue
        displacement=float(ob.high)-float(after.low.min())
        bos=float(after.close.min())<float(before.low.min())-0.03*atr
        if displacement>=0.8*atr and bos:
            zh=float(max(ob.open,ob.close)); zl=float(ob.low)
            touched=float(x.high)>=zl-0.08*atr and float(x.low)<=zh+0.08*atr
            if touched and float(x.close)<=zh and (x.close<x.open or x.delta<0):
                out.append(signal('Order Block + Sweep + BOS','SELL',tf,df,x.close,max(float(x.high),zh)+0.18*atr,82,['Order Block سبّب اندفاعًا فعليًا','BOS سابق مثبت','إعادة اختبار حية للبلوك']))
                break
    return out

def vwap_reclaim(df, tf):
    """EARLY setup around VWAP; no need to wait for a fully closed cross."""
    if len(df)<4:return []
    x,p=df.iloc[-1],df.iloc[-2]; out=[]
    if pd.isna(x.vwap) or pd.isna(x.atr):return []
    atr=float(x.atr); dist=(float(x.close)-float(x.vwap))/max(atr,1e-12)
    buy_zone = dist>=-0.18 and dist<=0.22 and (x.close>x.open or x.delta>0 or x.close>p.close)
    sell_zone = dist<=0.18 and dist>=-0.22 and (x.close<x.open or x.delta<0 or x.close<p.close)
    if buy_zone and (p.close<=p.vwap+0.15*atr or x.low<=x.vwap+0.08*atr):
        out.append(signal("VWAP Reclaim","BUY",tf,df,x.close,min(float(x.low),float(x.vwap)-0.40*atr),74,["منطقة VWAP الشرائية","بدء استرداد حي"]))
    if sell_zone and (p.close>=p.vwap-0.15*atr or x.high>=x.vwap-0.08*atr):
        out.append(signal("VWAP Rejection","SELL",tf,df,x.close,max(float(x.high),float(x.vwap)+0.40*atr),74,["منطقة VWAP البيعية","بدء رفض حي"]))
    return out


def compression_expansion(df, tf):
    """EARLY setup while compression is ending; live gate confirms the first micro expansion."""
    if len(df)<32:return []
    x=df.iloc[-1]; prev=df.iloc[-11:-1]; older=df.iloc[-31:-11]; out=[]
    if pd.isna(x.atr) or pd.isna(x.vol_ma) or len(older)<10:return []
    atr=float(x.atr); top=float(prev.high.max()); bot=float(prev.low.min())
    compressed=prev["range"].mean() < 0.88*older["range"].mean()
    vol_rise=float(x.volume) > 1.15*float(x.vol_ma)
    near_top=float(x.close)>=top-0.22*atr
    near_bot=float(x.close)<=bot+0.22*atr
    if compressed and vol_rise and near_top and (x.delta>0 or x.close>x.open):
        out.append(signal("Compression → Expansion","BUY",tf,df,x.close,float(prev.low.tail(5).min())-0.18*atr,78,["ضغط سابق","بداية توسع قرب الحد العلوي","التأكيد على أول كسر حي"]))
    if compressed and vol_rise and near_bot and (x.delta<0 or x.close<x.open):
        out.append(signal("Compression → Expansion","SELL",tf,df,x.close,float(prev.high.tail(5).max())+0.18*atr,78,["ضغط سابق","بداية توسع قرب الحد السفلي","التأكيد على أول كسر حي"]))
    return out


def trend_compression_breakout(df, tf):
    """Compression is context; TCB exists only after the LIVE price actually breaks the compressed boundary."""
    if not TCB_ENABLED or tf not in ('15m','1h','4h') or len(df)<55:return []
    x=df.iloc[-1]
    if any(pd.isna(v) for v in [x.atr,x.vol_ma,x.bb_up,x.bb_dn,x.vwap]) or float(x.atr)<=0:return []
    atr=float(x.atr); lookback=max(TCB_MIN_COMPRESSION_BARS,8); recent=df.iloc[-lookback-1:-1]; prior=df.iloc[-2*lookback-1:-lookback-1]
    if len(recent)<lookback or len(prior)<lookback:return []
    widths=((df.bb_up-df.bb_dn)/df.bb_mid.abs().replace(0,np.nan)).dropna(); hist=widths.iloc[-30:-1]
    if len(hist)<20:return []
    compressed=float(widths.iloc[-2])<=float(hist.quantile(min(.75,max(.05,TCB_BB_PERCENTILE)))) and float(recent['range'].mean())<=TCB_RANGE_RATIO*float(prior['range'].mean())
    if not compressed:return []
    top=float(recent.high.max()); bot=float(recent.low.min()); volume_ok=float(x.volume)>=TCB_VOLUME_MULTIPLIER*float(x.vol_ma); out=[]
    # x.close is the live websocket price of the current candle, so this does NOT wait for candle close.
    # Pre-arm near the compressed edge; final Telegram delivery still requires a live break.
    if TCB_PREARM_ENABLED and volume_ok and float(x.close)<top+TRADER_BREAKOUT_BUFFER_ATR*atr and float(x.close)>=top-TCB_PREARM_MAX_DISTANCE_ATR*atr and (float(x.delta)>0 or float(x.close)>float(x.open)):
        z=signal('Trend Compression Breakout','BUY',tf,df,top,bot-0.18*atr,84,['ضغط حقيقي سابق','تسليح مبكر قرب الحد العلوي','انتظار كسر حي قبل التنبيه'])
        if z:
            z['tcb_prearm']=True; z['tcb_break_level']=top; out.append(z)
    if TCB_PREARM_ENABLED and volume_ok and float(x.close)>bot-TRADER_BREAKOUT_BUFFER_ATR*atr and float(x.close)<=bot+TCB_PREARM_MAX_DISTANCE_ATR*atr and (float(x.delta)<0 or float(x.close)<float(x.open)):
        z=signal('Trend Compression Breakout','SELL',tf,df,bot,top+0.18*atr,84,['ضغط حقيقي سابق','تسليح مبكر قرب الحد السفلي','انتظار كسر حي قبل التنبيه'])
        if z:
            z['tcb_prearm']=True; z['tcb_break_level']=bot; out.append(z)
    if volume_ok and float(x.close)>=top+TRADER_BREAKOUT_BUFFER_ATR*atr and (float(x.delta)>0 or float(x.close)>float(x.open)):
        out.append(signal('Trend Compression Breakout','BUY',tf,df,float(x.close),bot-0.18*atr,86,['ضغط حقيقي سابق','كسر حي فعلي لحد النطاق','الحجم والتدفق يدعمان التوسع']))
    if volume_ok and float(x.close)<=bot-TRADER_BREAKOUT_BUFFER_ATR*atr and (float(x.delta)<0 or float(x.close)<float(x.open)):
        out.append(signal('Trend Compression Breakout','SELL',tf,df,float(x.close),top+0.18*atr,86,['ضغط حقيقي سابق','كسر حي فعلي لحد النطاق','الحجم والتدفق يدعمان التوسع']))
    return [z for z in out if z]

def mtf_signal(d15,d1,d4):
    """MTF direction is context only. Arm at the 15m reversal zone; do NOT wait for a 15m BOS."""
    out=[]; x15=d15.iloc[-1]; p15=d15.iloc[-2]; x1=d1.iloc[-1]; x4=d4.iloc[-1]
    if any(pd.isna(v) for v in [x15.atr,x1.atr,x4.ema50,x15.ema20,x15.vwap]): return []
    a15=float(x15.atr)
    bull4=x4.close>x4.ema50 and x4.ema20>x4.ema50
    bear4=x4.close<x4.ema50 and x4.ema20<x4.ema50
    # 1H is a pullback/context filter, not the trigger.
    pull1_buy=x1.low<=x1.ema20+0.55*x1.atr and x1.close>=x1.ema20-0.18*x1.atr
    pull1_sell=x1.high>=x1.ema20-0.55*x1.atr and x1.close<=x1.ema20+0.18*x1.atr
    prev15=d15.iloc[-8:-1]
    local_low=float(prev15.low.min()); local_high=float(prev15.high.max())
    # Early 15m reversal zone: EMA20/VWAP/local-liquidity area + first directional response.
    near_buy_zone = (x15.low<=x15.ema20+0.45*a15) or (x15.low<=x15.vwap+0.35*a15) or (x15.low<=local_low+0.30*a15)
    near_sell_zone = (x15.high>=x15.ema20-0.45*a15) or (x15.high>=x15.vwap-0.35*a15) or (x15.high>=local_high-0.30*a15)
    buy_response = (x15.close>x15.open) or (x15.delta>0) or (x15.close>p15.close)
    sell_response = (x15.close<x15.open) or (x15.delta<0) or (x15.close<p15.close)
    if bull4 and pull1_buy and near_buy_zone and buy_response:
        out.append(signal("MTF 4H→1H→15M","BUY","15m",d15,x15.close,min(float(x15.low),local_low)-0.18*a15,82,["4H صاعد","1H داخل تصحيح مناسب","15M عند منطقة انعكاس مبكرة","لا ننتظر BOS كامل 15M"]))
    if bear4 and pull1_sell and near_sell_zone and sell_response:
        out.append(signal("MTF 4H→1H→15M","SELL","15m",d15,x15.close,max(float(x15.high),local_high)+0.18*a15,82,["4H هابط","1H داخل تصحيح مناسب","15M عند منطقة انعكاس مبكرة","لا ننتظر BOS كامل 15M"]))
    return out

def fmt_price(x):
    if x>=1000:return f"{x:.2f}"
    if x>=1:return f"{x:.5f}".rstrip('0').rstrip('.')
    return f"{x:.8f}".rstrip('0').rstrip('.')


# Telegram delivery queue. Each item carries an Event so the caller only records an alert
# after Telegram confirms HTTP success. Retries are bounded to avoid a stuck signal worker.
telegram_queue = queue.Queue()
telegram_worker_started = False
telegram_worker_lock = threading.RLock()
TELEGRAM_MIN_SEND_INTERVAL = float(os.getenv("TELEGRAM_MIN_SEND_INTERVAL_SECONDS", "1.10"))
TELEGRAM_MAX_RETRIES = int(os.getenv("TELEGRAM_MAX_RETRIES", "3"))
TELEGRAM_DELIVERY_TIMEOUT = float(os.getenv("TELEGRAM_DELIVERY_TIMEOUT_SECONDS", "25"))

def _telegram_worker():
    url=f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    last_send_at=0.0
    while True:
        item=telegram_queue.get()
        try:
            msg, done, result = item
            delivered=False
            for attempt in range(max(1, TELEGRAM_MAX_RETRIES)):
                wait=max(0.0, TELEGRAM_MIN_SEND_INTERVAL-(time.time()-last_send_at))
                if wait > 0: time.sleep(wait)
                try:
                    r=requests.post(url, json={"chat_id":CHAT_ID,"text":msg,"parse_mode":"HTML","disable_web_page_preview":True}, timeout=15)
                    last_send_at=time.time()
                    if r.ok:
                        delivered=True; break
                    if r.status_code==429:
                        try: retry_after=(r.json().get("parameters") or {}).get("retry_after",5)
                        except Exception: retry_after=5
                        time.sleep(min(60.0, max(1.0,float(retry_after))+0.5))
                    elif r.status_code >= 500:
                        time.sleep(2.0*(attempt+1))
                    else:
                        print("Telegram non-retryable error",r.status_code,r.text[:300]); break
                except Exception as e:
                    print("Telegram exception",e)
                    if attempt+1 < max(1, TELEGRAM_MAX_RETRIES): time.sleep(2.0*(attempt+1))
            result["ok"]=delivered
            done.set()
        finally:
            telegram_queue.task_done()

def _ensure_telegram_worker():
    global telegram_worker_started
    if telegram_worker_started: return
    with telegram_worker_lock:
        if telegram_worker_started: return
        threading.Thread(target=_telegram_worker, name="telegram-delivery", daemon=True).start()
        telegram_worker_started=True

def tg(msg):
    if not TOKEN or not CHAT_ID:
        print(msg); return False
    _ensure_telegram_worker()
    done=threading.Event(); result={"ok":False}
    telegram_queue.put((msg,done,result))
    done.wait(max(1.0, TELEGRAM_DELIVERY_TIMEOUT))
    return bool(done.is_set() and result.get("ok"))


def get_oi_soft(symbol):
    cached=open_interest_cache.get(symbol)
    if cached and time.time()-cached[0] < 300:
        return cached[1]
    try:
        oi=float(get_json("/fapi/v1/openInterest", {"symbol":symbol}, allow_wait=False).get("openInterest",0))
        open_interest_cache[symbol]=(time.time(),oi)
        return oi
    except Exception:
        return None


def alert_text(symbol,s,helpers_list,oi,funding,combo=None):
    buy=s["side"]=="BUY"
    head=("💎🔥🟢 توافق استراتيجيات مؤكد — شراء" if buy else "💎🔥🔴 توافق استراتيجيات مؤكد — بيع") if combo else ("🔵🟢 دخول شراء مؤكد" if buy else "🔵🔴 دخول بيع مؤكد")
    strat=" + ".join(combo) if combo else s["strategy"]
    ctx=s.get("coin_context") or {}
    t1=_direction_ar("1h",ctx.get("trend_1h","NEUTRAL"))
    t4=_direction_ar("4h",ctx.get("trend_4h","NEUTRAL"))
    td=_direction_ar("1d",ctx.get("trend_1d","NEUTRAL"))
    tw=_direction_ar("1w",ctx.get("trend_1w","NEUTRAL"))

    blocks=s.get("vol_of_blocks") or []
    if blocks:
        b=blocks[0]
        block_type="🟩 بلوك شرائي قوي" if buy else "🟥 بلوك بيعي قوي"
        block_txt=f"{block_type}: <b>{fmt_price(float(b['low']))} – {fmt_price(float(b['high']))}</b> | {b.get('tf','').upper()} | Δ {float(b.get('delta_pct',0)):+.1f}%"
    else:
        block_txt="▫️ Footprint: يتجمع — الاعتماد على منطقة الخطة البنيوية"

    trigger=s.get("trigger_summary") or "تأكيد حي"
    return f"""<b>{head}</b>

💰 <b>#{symbol}.P</b> | ⏰ <b>{s['tf'].upper()}</b>
🧩 <b>{strat}</b> | جودة <b>{s['quality']}%</b>
🧭 {t1} | {t4} | {td} | {tw}
{block_txt}
🕯 التأكيد: <b>{trigger}</b>

🎯 الدخول: <b>{fmt_price(s['entry'])}</b>
🛑 الوقف: <b>{fmt_price(s['sl'])}</b>
✅ TP1: <b>{fmt_price(s['tp'][0])}</b> | TP2: <b>{fmt_price(s['tp'][1])}</b> | TP3: <b>{fmt_price(s['tp'][2])}</b>

🕒 {now_ksa().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)
🔗 <a href="https://www.binance.com/en/futures/{symbol}">Binance</a> | <a href="https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P">TradingView</a>

⚠️ تنبيه إحصائي وليس ضمانًا للربح"""




def cooldown_key(symbol,s=None,combo=None):
    if combo:
        return f"COMBO|{symbol}|{s['side']}|{'+'.join(sorted(combo))}"
    return f"STRATEGY|{symbol}|{s['side']}|{s['tf']}|{s['strategy']}"


def can_send(key, combo=False):
    mins=COMBO_COOLDOWN_MIN if combo else STRATEGY_COOLDOWN_MIN
    return time.time()-state["last_alerts"].get(key,0) >= mins*60


def record(key,s,combo=None):
    with state_lock:
        state["last_alerts"][key]=time.time()
        state["alerts"]+=1
        state["by_side"][s["side"]]+=1
        state["by_strategy"]["COMBO" if combo else s["strategy"]]+=1


def _setup_entry_gate(ctx, sig):
    """Strategy-assisted ENTRY gate.

    Architecture remains:
    1) coin analysis decides directional thesis/plan,
    2) a real strategy is detected,
    3) the strategy may satisfy the missing 'market event',
    4) entry still requires good location, no chase, and valid structural risk.
    """
    side=sig["side"]
    preferred=ctx.get("preferred")
    if preferred != side:
        return False, "الاستراتيجية عكس رأي تحليل العملة"

    # 4H/1H define context; Daily/Weekly are context only, not late mandatory gates.
    want="UP" if side=="BUY" else "DOWN"
    opposite="DOWN" if want=="UP" else "UP"
    t1=ctx.get("trend_1h","NEUTRAL"); t4=ctx.get("trend_4h","NEUTRAL")
    if t1==opposite and t4==opposite:
        return False, "1H و4H كلاهما عكس الصفقة"

    own=float(ctx.get("buy_score" if side=="BUY" else "sell_score",0))
    opp=float(ctx.get("sell_score" if side=="BUY" else "buy_score",0))
    if own < TRADER_MIN_PLAN_SCORE or own <= opp+5:
        return False, "ثقة اتجاه العملة غير كافية"

    entry=float(sig.get("entry",0) or 0)
    atr=max(float(sig.get("setup_atr") or 0),1e-12)
    lo=ctx.get("entry_zone_low"); hi=ctx.get("entry_zone_high")
    if lo is None or hi is None:
        return False, "لا توجد منطقة خطة واضحة"

    lo=float(lo); hi=float(hi)
    # Strategy can be slightly outside the plan zone; it must still be close enough to trade.
    near_zone=(lo-0.45*atr) <= entry <= (hi+0.45*atr)
    if not near_zone:
        return False, "الاستراتيجية موجودة لكن بعيدة عن منطقة الخطة"

    # Do not chase. Use the current analysis wording plus plan risk.
    loc=str(ctx.get("location") or "")
    if "ممتدة" in loc or "مطاردة" in loc:
        return False, "الحركة ممتدة — لا نطارد السعر"

    plan_risk=ctx.get("plan_risk_atr")
    if plan_risk is not None and (float(plan_risk) < 0.40 or float(plan_risk) > COIN_STOP_MAX_ATR):
        return False, "الوقف البنيوي غير مناسب"

    rr=float(ctx.get("rr_space") or 0)
    # A real strategy can compensate for the generic 15m market-event requirement,
    # but cannot compensate for extremely poor room.
    if rr < EARLY_MIN_RR:
        return False, "مساحة الهدف أقل من الحد المطلوب — لا دخول"

    return True, "استراتيجية حقيقية متوافقة مع خطة العملة"

def analyze_symbol(symbol):
    dfs={}
    for tf in ENABLED_TFS:
        df=df_from_store(symbol,tf)
        if df is not None: dfs[tf]=df
    if not dfs: return []

    # 1) Analyze the coin first, independently.
    ctx=build_coin_context(symbol,dfs)

    # 2) Always detect genuine strategies.
    candidates=[]
    for tf,df in dfs.items():
        for fn in (break_retest, liquidity_sweep, orderblock_sweep_bos, vwap_reclaim, compression_expansion, trend_compression_breakout):
            try:
                candidates += fn(df,tf)
            except Exception as e:
                print(symbol,tf,fn.__name__,e)
    if all(k in dfs for k in ("15m","1h","4h")):
        try:
            candidates += mtf_signal(dfs["15m"],dfs["1h"],dfs["4h"])
        except Exception as e:
            print(symbol,"MTF",e)

    if candidates:
        with state_lock:
            state["setups_detected"] += len([x for x in candidates if x is not None])

    # 3) A real strategy can open the entry gate if the coin thesis agrees
    # and price/risk are suitable. The generic 15m market_event is no longer mandatory.
    signals=[]; funding=funding_cache.get(symbol)
    for sig in candidates:
        if sig is None: continue

        side=sig["side"]
        own=float(ctx.get("buy_score" if side=="BUY" else "sell_score",0))
        attach_coin_context(sig,ctx)
        sig["vol_of_blocks"]=_aggtrade_volumetric_confluence(symbol,side,float(sig.get("entry",0) or 0),dfs)

        gate_open,gate_reason=_setup_entry_gate(ctx,sig)
        # TCB pre-arm is an earlier, still-strict gate: permit arming near the compressed edge
        # when the higher-timeframe thesis agrees, then require the dedicated live break gate.
        if sig.get("strategy")=="Trend Compression Breakout" and sig.get("tcb_prearm"):
            t1=ctx.get("trend_1h","NEUTRAL"); t4=ctx.get("trend_4h","NEUTRAL")
            opposite="DOWN" if side=="BUY" else "UP"
            own=float(ctx.get("buy_score" if side=="BUY" else "sell_score",0))
            opp=float(ctx.get("sell_score" if side=="BUY" else "buy_score",0))
            thesis_ok=(ctx.get("preferred")==side or (own>=TRADER_MIN_PLAN_SCORE and own>opp+7)) and not (t1==opposite and t4==opposite)
            gate_open=bool(thesis_ok)
            gate_reason="TCB pre-arm: اتجاه الفريمات متوافق، انتظار الكسر الحي" if gate_open else "TCB pre-arm: اتجاه الفريمات غير كافٍ"
        # If the trader analyzer itself already says tradeable on the same side, also open.
        if ctx.get("tradeable") and ctx.get("decision")==side:
            gate_open=True
            gate_reason="تحليل العملة والاستراتيجية متوافقان"

        sig["entry_gate_open"]=bool(gate_open)
        sig["setup_status"]="READY_FOR_LIVE_TRIGGER" if gate_open else "SETUP_FOUND_WAIT_ENTRY"
        sig["gate_reason"]=gate_reason
        sig["helpers"]=helpers(dfs[sig["tf"]],side) if sig["tf"] in dfs else []
        if sig["vol_of_blocks"]:
            b=sig["vol_of_blocks"][0]
            label="بلوك شرائي قوي" if side=="BUY" else "بلوك بيعي قوي"
            sig["reasons"]=list(dict.fromkeys(list(sig.get("reasons",[]))+[f"{label} — REAL aggTrade Footprint — {b['tf'].upper()} — قوة {b['score']:.0f}% — Δ {b.get('delta_pct',0):+.1f}%"]))
            sig["helpers"]=list(dict.fromkeys(sig["helpers"]+["VolumetricOF"]))
        attach_performance(sig)

        if gate_open:
            sig["reasons"]=list(dict.fromkeys(list(sig.get("reasons",[]))+[
                f"بوابة الدخول مفتوحة: {gate_reason}"
            ]))
            with state_lock:
                state["entry_gates_opened"] += 1
        else:
            sig["reasons"]=list(dict.fromkeys(list(sig.get("reasons",[]))+[
                "الاستراتيجية موجودة — انتظار دخول مناسب",
                f"سبب الانتظار: {gate_reason}"
            ]))
            with state_lock:
                state["setups_waiting_entry"] += 1

        context_bonus=min(8,max(0,(own-50))*0.20) if gate_open else 0
        sig["score"]=sig["quality"]+min(10,len(sig["helpers"])*1.5)+float(sig.get("historical_boost",0.0))+context_bonus
        sig["funding"]=funding
        signals.append(sig)

    return signals

def process_signals(symbol, signals, budget):
    if not signals or budget<=0:return 0
    allowed=[]
    for sig in signals:
        attach_performance(sig)
        if performance_allows_send(sig):
            sig["score"]=float(sig.get("quality",0))+min(10,len(sig.get("helpers",[]))*1.5)+float(sig.get("historical_boost",0.0))+min(8,max(0,float(sig.get("context_score",50))-50)*0.20)
            allowed.append(sig)
        else:
            with state_lock: state["performance_suppressed"]+=1
    if not allowed:return 0
    sent=0; oi=None; by_side={}
    for side in ("BUY","SELL"):
        g=sorted([x for x in allowed if x["side"]==side],key=lambda x:x.get("score",0),reverse=True)
        uniq=[]; seen=set()
        for x in g:
            k=(x["strategy"],x["tf"])
            if k not in seen: uniq.append(x); seen.add(k)
        if uniq: by_side[side]=uniq
    for side,uniq in by_side.items():
        for sig in uniq:
            if sent>=budget: break
            key=cooldown_key(symbol,sig)
            if not can_send(key): continue
            if oi is None: oi=get_oi_soft(symbol)
            if tg(alert_text(symbol,sig,sig.get("helpers",[]),oi,sig.get("funding"))):
                persist_alert(symbol,sig,sig.get("helpers",[]),oi,sig.get("funding")); record(key,sig); sent+=1
    for side,uniq in by_side.items():
        best={}
        for x in uniq: best.setdefault(x["strategy"],x)
        # Keep dedicated strategies out of COMBO alerts to avoid duplicate/conflicting messages.
        combo_sigs=[x for x in best.values() if x.get("strategy") not in {"Trend Compression Breakout"}]
        if len(combo_sigs)<2 or sent>=budget: continue
        combo=[x["strategy"] for x in combo_sigs]; lead=max(combo_sigs,key=lambda x:x.get("score",0)).copy(); key=cooldown_key(symbol,lead,combo)
        if not can_send(key,combo=True): continue
        lead["quality"]=min(99,max(x.get("quality",0) for x in combo_sigs)+min(12,3*(len(combo_sigs)-1)))
        lead["reasons"]=[f"🔥 توافق مميز: {len(combo_sigs)} استراتيجيات مؤكدة على نفس الاتجاه"]+list(dict.fromkeys(r for x in combo_sigs for r in x.get("reasons",[])))[:10]
        merged=[]
        for x in combo_sigs: merged.extend(x.get("vol_of_blocks") or [])
        lead["vol_of_blocks"]=sorted(merged,key=lambda b:b.get("score",0),reverse=True)[:5]
        helpers_list=list(dict.fromkeys(h for x in combo_sigs for h in x.get("helpers",[])))
        if oi is None: oi=get_oi_soft(symbol)
        if tg(alert_text(symbol,lead,helpers_list,oi,lead.get("funding"),combo)):
            persist_alert(symbol,lead,helpers_list,oi,lead.get("funding"),combo); record(key,lead,combo); sent+=1
    return sent

def _ws_send(payload):
    global ws_app
    with ws_send_lock:
        if ws_app and state.get("ws_connected"):
            try: ws_app.send(json.dumps(payload)); return True
            except Exception as e: print("WS send error",e)
    return False


def subscribe_streams(streams):
    global ws_request_id
    fresh=[x for x in streams if x not in subscribed_streams]
    if not fresh:return
    for i in range(0,len(fresh),100):
        batch=fresh[i:i+100]; ws_request_id+=1
        if _ws_send({"method":"SUBSCRIBE","params":batch,"id":ws_request_id}):
            subscribed_streams.update(batch)
            time.sleep(0.15)


def update_kline_from_ws(data):
    k=data.get("k") or {}; symbol=data.get("s") or k.get("s"); tf=k.get("i")
    if not symbol or tf not in INTERNAL_TFS or symbol not in radar_set:return
    row={
        "open_time":int(k["t"]),"open":float(k["o"]),"high":float(k["h"]),"low":float(k["l"]),"close":float(k["c"]),
        "volume":float(k["v"]),"close_time":int(k["T"]),"quote_volume":float(k["q"]),"trades":int(k["n"]),
        "taker_buy_base":float(k["V"]),"taker_buy_quote":float(k["Q"]),"ignore":"0"
    }
    with data_lock:
        dq=candle_store.get((symbol,tf))
        if dq is None:return
        if dq and dq[-1]["open_time"]==row["open_time"]: dq[-1]=row
        else: dq.append(row)
        dirty_symbols.add(symbol)


def on_ws_message(ws, message):
    try:
        obj=json.loads(message); data=obj.get("data",obj)
        with state_lock: state["ws_last_event"]=now_ksa().isoformat()
        if isinstance(data,list):
            for x in data:
                st=x.get("st")
                if st is not None and int(st)!=1: continue
                s=x.get("s")
                if not s or not s.endswith("USDT"): continue
                if x.get("e")=="24hrMiniTicker": mini_tickers[s]=float(x.get("q",0) or 0)
                elif x.get("e")=="markPriceUpdate":
                    funding_cache[s]=float(x.get("r",0) or 0)
                    mp=float(x.get("p",0) or 0)
                    if mp>0:
                        mark_price_cache[s]=mp
                        evaluate_symbol_trades(s,mp,(float(x.get("E",0) or 0)/1000.0) or time.time())
        elif isinstance(data,dict):
            if data.get("e")=="kline": update_kline_from_ws(data)
    except Exception as e:
        print("WS message error",e)


def on_ws_open(ws):
    global ws_app
    ws_app=ws
    with state_lock: state["ws_connected"]=True; state["last_error"]=None
    # Base feeds first. They are not added here because already marked in subscribed_streams.
    ws.send(json.dumps({"method":"SUBSCRIBE","params":["!miniTicker@arr","!markPrice@arr@1s"],"id":1}))
    # re-subscribe klines after reconnect
    current=[]
    with data_lock:
        for s in list(radar_symbols):
            for tf in INTERNAL_TFS: current.append(f"{s.lower()}@kline_{tf}")
    for i in range(0,len(current),100):
        ws.send(json.dumps({"method":"SUBSCRIBE","params":current[i:i+100],"id":10+i}))
        time.sleep(0.15)
    print("WebSocket connected")


def on_ws_close(ws, code, msg):
    with state_lock: state["ws_connected"]=False; state["last_error"]=f"WS closed {code}: {msg}"
    print("WebSocket closed",code,msg)


def on_ws_error(ws, error):
    with state_lock: state["last_error"]=f"WS error: {error}"
    print("WebSocket error",error)


def websocket_worker():
    global ws_app
    while True:
        try:
            ws_app=websocket.WebSocketApp(WS_MARKET,on_open=on_ws_open,on_message=on_ws_message,on_close=on_ws_close,on_error=on_ws_error)
            ws_app.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            print("WS worker exception",e)
        with state_lock: state["ws_connected"]=False
        time.sleep(WS_RECONNECT_SECONDS)


def bootstrap_symbol(symbol):
    if symbol in bootstrapped:return True
    try:
        for tf in INTERNAL_TFS:
            rows=raw_klines(symbol,tf,BOOTSTRAP_LIMIT)
            with data_lock: candle_store[(symbol,tf)] = deque(rows,maxlen=max(BOOTSTRAP_LIMIT+5,230))
        bootstrapped.add(symbol)
        with state_lock: state["bootstrapped"]=len(bootstrapped)
        subscribe_streams([f"{symbol.lower()}@kline_{tf}" for tf in INTERNAL_TFS])
        return True
    except Exception as e:
        print("bootstrap",symbol,e)
        return False


def radar_worker():
    global radar_symbols, radar_set
    # wait until miniTicker stream has market data; no exchangeInfo/ticker REST required
    while True:
        try:
            if mini_tickers:
                ranked=[(s,q) for s,q in mini_tickers.items() if s.endswith("USDT") and q>=MIN_QV]
                ranked.sort(key=lambda z:z[1],reverse=True)
                target=[s for s,_ in ranked[:RADAR_POOL]]
                with data_lock:
                    radar_symbols=target; radar_set=set(target)
                with state_lock:
                    state["radar_count"]=len(target); state["bootstrap_total"]=len(target)
                # Subscribe FIRST: live market data keeps flowing even if Binance REST is temporarily blocked.
                subscribe_streams([f"{s.lower()}@kline_{tf}" for s in target for tf in INTERNAL_TFS])
                subscribe_aggtrade_streams(target)

                # Bootstrap history conservatively. Never sit inside a 418/429 sleep loop.
                blocked_until = float(state.get("rest_blocked_until", 0) or 0)
                if time.time() < blocked_until:
                    with state_lock:
                        state["bootstrap_paused"] = True
                else:
                    with state_lock:
                        state["bootstrap_paused"] = False
                    for symbol in target:
                        if symbol in bootstrapped:
                            continue
                        # If any request triggers Binance backoff, stop REST bootstrap immediately.
                        if time.time() < float(state.get("rest_blocked_until", 0) or 0):
                            with state_lock:
                                state["bootstrap_paused"] = True
                            break
                        ok = bootstrap_symbol(symbol)
                        if not ok and time.time() < float(state.get("rest_blocked_until", 0) or 0):
                            with state_lock:
                                state["bootstrap_paused"] = True
                            break
            time.sleep(RADAR_REFRESH_SECONDS)
        except Exception as e:
            print("radar worker",e); time.sleep(10)


def signal_worker():
    while True:
        time.sleep(max(0.5,SIGNAL_CHECK_SECONDS))
        with data_lock:
            batch=list(dirty_symbols); dirty_symbols.clear()
        if not batch: continue

        confirmed_found=[]
        for symbol in batch:
            try:
                # 1) Detect all six strategy setups and store them internally only.
                setups=analyze_symbol(symbol)
                if setups:
                    register_waiting_setups(symbol,setups)

                # 2) Evaluate existing waiting setups against live 5m/15m structure.
                confirmed=evaluate_waiting_confirmations(symbol)
                if confirmed:
                    best=max((x.get("score",x.get("quality",0)) for x in confirmed),default=0)
                    distinct=len({(x["side"],x["strategy"]) for x in confirmed})
                    confirmed_found.append((best+min(12,distinct*2),symbol,confirmed))
            except Exception as e:
                print("signal/confirmation",symbol,e)

        # Telegram budget applies ONLY to confirmed entries, never to waiting setups.
        confirmed_found.sort(key=lambda z:z[0],reverse=True)
        budget=MAX_ALERTS
        for _,symbol,sigs in confirmed_found:
            if budget<=0: break
            sent=process_signals(symbol,sigs,budget)
            budget-=sent

        with state_lock:
            state["last_signal_check"]=now_ksa().isoformat()
            state["waiting_confirmation"]=len(pending_confirmations)


@app.get("/")
def home():
    return jsonify({"name":"Ahmed Strategy Fusion Bot","version":state["version"],"status":"running","transport":"Main WebSocket + dedicated Binance aggTrade footprint WebSocket + throttled REST bootstrap","strategies":7,"timeframes":ENABLED_TFS,"stats":"/stats","health":"/health"})

@app.get("/health")
def health():
    blocked=max(0,int(state["rest_blocked_until"]-time.time()))
    return jsonify({"ok":True,"version":state["version"],"ws_connected":state["ws_connected"],"ws_last_event":state["ws_last_event"],"radar_count":state["radar_count"],"bootstrapped":state["bootstrapped"],"bootstrap_total":state["bootstrap_total"],"active_trades":len(active_trades),"waiting_confirmation":len(pending_confirmations),"post_sl_tracking":len(post_sl_trades),"database":DB_PATH,"rest_backoff_seconds":blocked,"setups_detected":state.get("setups_detected",0),"setups_waiting_entry":state.get("setups_waiting_entry",0),"entry_gates_opened":state.get("entry_gates_opened",0),"live_confirmed":state.get("live_confirmed",0),"aggtrade_ws_connected":state.get("aggtrade_ws_connected",False),"aggtrade_last_event":state.get("aggtrade_last_event"),"aggtrade_events":state.get("aggtrade_events",0),"footprint_blocks_completed":state.get("footprint_blocks_completed",0),"last_error":state["last_error"]})

@app.get("/stats")
def stats():
    return stats_html(),200,{"Content-Type":"text/html; charset=utf-8"}

@app.get("/stats.json")
def stats_json():
    blocked=max(0,int(state["rest_blocked_until"]-time.time()))
    rs=result_stats()
    return jsonify({"started":state["started"],"version":state["version"],"ws_connected":state["ws_connected"],"ws_last_event":state["ws_last_event"],"last_signal_check":state["last_signal_check"],"radar_count":state["radar_count"],"bootstrapped":state["bootstrapped"],"bootstrap_total":state["bootstrap_total"],"session_alerts":state["alerts"],"session_by_strategy":dict(state["by_strategy"]),"session_by_side":dict(state["by_side"]),"results":rs,"rest_backoff_seconds":blocked,"rest_418_count":state["rest_418_count"],"rest_429_count":state["rest_429_count"],"performance_suppressed":state.get("performance_suppressed",0),"setups_detected":state.get("setups_detected",0),"setups_waiting_entry":state.get("setups_waiting_entry",0),"entry_gates_opened":state.get("entry_gates_opened",0),"live_confirmed":state.get("live_confirmed",0),"performance_policy":{"enabled":PERFORMANCE_FILTER_ENABLED,"min_evaluated":PERFORMANCE_MIN_EVALUATED,"min_win_rate":PERFORMANCE_MIN_WIN_RATE,"weight":PERFORMANCE_WEIGHT},"aggtrade_ws_connected":state.get("aggtrade_ws_connected",False),"aggtrade_events":state.get("aggtrade_events",0),"footprint_blocks_completed":state.get("footprint_blocks_completed",0),"last_error":state["last_error"]})


@app.get("/footprint.json")
def footprint_json():
    symbol=(__import__("flask").request.args.get("symbol") or "").upper().replace(".P","")
    if not symbol:
        return jsonify({"ok":False,"error":"use ?symbol=BTCUSDT"}),400
    out={}
    for tf in FOOTPRINT_TFS:
        live=_current_fp_summary(symbol,tf)
        with footprint_lock:
            hist=list(footprint_history.get((symbol,tf),[]))[-5:]
        out[tf]={"live":live,"history":hist}
    return jsonify({"ok":True,"symbol":symbol,"source":"Binance Futures aggTrade","frames":out})



init_db()
load_footprint_history()
load_active_trades()
load_post_sl_trades()
threading.Thread(target=websocket_worker,daemon=True).start()
threading.Thread(target=aggtrade_websocket_worker,daemon=True).start()
threading.Thread(target=radar_worker,daemon=True).start()
threading.Thread(target=signal_worker,daemon=True).start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")))
