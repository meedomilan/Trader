import os
import time
import json
import threading
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
MIN_QV = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "1000000"))
RADAR_POOL = int(os.getenv("RADAR_POOL", "180"))
MAX_ALERTS = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))
SIGNAL_CHECK_SECONDS = float(os.getenv("SIGNAL_CHECK_SECONDS", "2"))
RADAR_REFRESH_SECONDS = int(os.getenv("RADAR_REFRESH_SECONDS", "60"))
REST_MIN_INTERVAL = float(os.getenv("REST_MIN_INTERVAL_MS", "160")) / 1000.0
REST_418_BACKOFF = int(os.getenv("REST_418_DEFAULT_BACKOFF_SECONDS", "300"))
BOOTSTRAP_LIMIT = int(os.getenv("BOOTSTRAP_KLINES_LIMIT", "220"))
WS_RECONNECT_SECONDS = int(os.getenv("WS_RECONNECT_SECONDS", "5"))
ENABLED_TFS = [tf for tf, key in [("15m","ENABLE_15M"),("1h","ENABLE_1H"),("4h","ENABLE_4H")] if os.getenv(key,"true").lower()=="true"]
KSA = timezone(timedelta(hours=3))

session = requests.Session()
session.headers.update({"User-Agent":"AhmedStrategyFusionBot/1.2-WS"})
app = Flask(__name__)

state = {
    "version": "1.2 WebSocket FAST",
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
}
state_lock = threading.RLock()
data_lock = threading.RLock()
rest_lock = threading.RLock()
ws_send_lock = threading.RLock()

# symbol -> latest 24h quote volume from !miniTicker@arr
mini_tickers = {}
# symbol -> funding from !markPrice@arr@1s
funding_cache = {}
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
                raise RuntimeError(f"REST_BACKOFF {int(blocked-now)}s")
            time.sleep(max(0.5, blocked-now))

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
    raw = get_json("/fapi/v1/klines", {"symbol":symbol,"interval":interval,"limit":limit}, allow_wait=True)
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


def signal(strategy, side, tf, df, entry=None, invalid=None, quality=0, reasons=None):
    x=df.iloc[-1]; atr=float(x.atr) if pd.notna(x.atr) else float(x.close)*0.01
    entry=float(entry if entry is not None else x.close)
    if invalid is None: invalid = entry-1.2*atr if side=="BUY" else entry+1.2*atr
    invalid=float(invalid); risk=abs(entry-invalid)
    if risk <= 0: return None
    tps=[entry+risk*r if side=="BUY" else entry-risk*r for r in (1,2,3)]
    return {"strategy":strategy,"side":side,"tf":tf,"entry":entry,"sl":invalid,"tp":tps,"quality":round(float(quality),1),"reasons":reasons or []}


def break_retest(df, tf):
    x,p=df.iloc[-1],df.iloc[-2]; atr=x.atr
    if pd.isna(atr): return []
    window=df.iloc[-22:-2]; resistance=window.high.max(); support=window.low.min(); out=[]
    if p.close>resistance and x.low<=resistance+0.25*atr and x.close>resistance and x.close>x.open:
        q=70+min(20,max(0,(x.volume/x.vol_ma-1)*20)) if pd.notna(x.vol_ma) else 70
        out.append(signal("Break & Retest","BUY",tf,df,x.close,min(x.low,resistance-0.35*atr),q,["كسر مقاومة","إعادة اختبار ناجحة"]))
    if p.close<support and x.high>=support-0.25*atr and x.close<support and x.close<x.open:
        q=70+min(20,max(0,(x.volume/x.vol_ma-1)*20)) if pd.notna(x.vol_ma) else 70
        out.append(signal("Break & Retest","SELL",tf,df,x.close,max(x.high,support+0.35*atr),q,["كسر دعم","إعادة اختبار ناجحة"]))
    return [s for s in out if s]


def liquidity_sweep(df, tf):
    x=df.iloc[-1]; atr=x.atr; w=df.iloc[-18:-1]; out=[]
    if pd.isna(atr): return []
    lo=w.low.min(); hi=w.high.max()
    if x.low < lo-0.05*atr and x.close>lo and x.close>x.open:
        out.append(signal("Liquidity Sweep + Reclaim","BUY",tf,df,x.close,x.low-0.15*atr,78,["سحب سيولة أسفل القاع","استرداد المستوى"]))
    if x.high > hi+0.05*atr and x.close<hi and x.close<x.open:
        out.append(signal("Liquidity Sweep + Reclaim","SELL",tf,df,x.close,x.high+0.15*atr,78,["سحب سيولة أعلى القمة","رفض واسترداد هابط"]))
    return out


def orderblock_sweep_bos(df, tf):
    if len(df)<40:return []
    x=df.iloc[-1]; atr=x.atr; out=[]
    if pd.isna(atr): return []
    recent=df.iloc[-15:-1]; prevs=df.iloc[-25:-4]
    bear=prevs[prevs.close<prevs.open]; bull=prevs[prevs.close>prevs.open]
    if not bear.empty:
        ob=bear.iloc[-1]; zone_low=min(ob.open,ob.close); zone_high=ob.high
        bos=recent.high.iloc[-6:-1].max() if len(recent)>=6 else recent.high.max()
        if x.low<=zone_high and x.low>=zone_low-0.5*atr and x.close>bos and x.close>x.open:
            out.append(signal("Order Block + Sweep + BOS","BUY",tf,df,x.close,min(x.low,zone_low)-0.15*atr,82,["عودة إلى Bullish OB","BOS صاعد"]))
    if not bull.empty:
        ob=bull.iloc[-1]; zone_high=max(ob.open,ob.close); zone_low=ob.low
        bos=recent.low.iloc[-6:-1].min() if len(recent)>=6 else recent.low.min()
        if x.high>=zone_low and x.high<=zone_high+0.5*atr and x.close<bos and x.close<x.open:
            out.append(signal("Order Block + Sweep + BOS","SELL",tf,df,x.close,max(x.high,zone_high)+0.15*atr,82,["عودة إلى Bearish OB","BOS هابط"]))
    return out


def vwap_reclaim(df, tf):
    x,p=df.iloc[-1],df.iloc[-2]; out=[]
    if pd.isna(x.vwap) or pd.isna(x.atr):return []
    if p.close<p.vwap and x.close>x.vwap and x.close>x.open and x.delta>0:
        out.append(signal("VWAP Reclaim","BUY",tf,df,x.close,min(x.low,x.vwap-0.35*x.atr),76,["استرداد VWAP","Delta شرائي"]))
    if p.close>p.vwap and x.close<x.vwap and x.close<x.open and x.delta<0:
        out.append(signal("VWAP Rejection","SELL",tf,df,x.close,max(x.high,x.vwap+0.35*x.atr),76,["فقدان VWAP","Delta بيعي"]))
    return out


def compression_expansion(df, tf):
    x=df.iloc[-1]; prev=df.iloc[-11:-1]; out=[]
    if pd.isna(x.atr) or pd.isna(x.vol_ma): return []
    older=df.iloc[-31:-11]
    if len(older)<10:return []
    compressed=prev["range"].mean() < 0.85*older["range"].mean(); vol=x.volume>1.6*x.vol_ma
    if compressed and vol and x.close>prev.high.max() and x.delta>0:
        out.append(signal("Compression → Expansion","BUY",tf,df,x.close,prev.low.tail(5).min()-0.15*x.atr,84,["ضغط سابق","اختراق بحجم مرتفع","Delta شرائي"]))
    if compressed and vol and x.close<prev.low.min() and x.delta<0:
        out.append(signal("Compression → Expansion","SELL",tf,df,x.close,prev.high.tail(5).max()+0.15*x.atr,84,["ضغط سابق","كسر بحجم مرتفع","Delta بيعي"]))
    return out


def mtf_signal(d15,d1,d4):
    out=[]; x15=d15.iloc[-1]; x1=d1.iloc[-1]; x4=d4.iloc[-1]
    if any(pd.isna(v) for v in [x15.atr,x1.atr,x4.ema50]): return []
    bull4=x4.close>x4.ema50 and x4.ema20>x4.ema50; bear4=x4.close<x4.ema50 and x4.ema20<x4.ema50
    pull1_buy=x1.low<=x1.ema20+0.4*x1.atr and x1.close>x1.ema20; pull1_sell=x1.high>=x1.ema20-0.4*x1.atr and x1.close<x1.ema20
    prev15=d15.iloc[-8:-1]
    if bull4 and pull1_buy and x15.close>prev15.high.max() and x15.delta>0:
        out.append(signal("MTF 4H→1H→15M","BUY","15m",d15,x15.close,prev15.low.min()-0.15*x15.atr,86,["4H صاعد","1H تصحيح ناجح","15M BOS صاعد"]))
    if bear4 and pull1_sell and x15.close<prev15.low.min() and x15.delta<0:
        out.append(signal("MTF 4H→1H→15M","SELL","15m",d15,x15.close,prev15.high.max()+0.15*x15.atr,86,["4H هابط","1H تصحيح ناجح","15M BOS هابط"]))
    return out


def fmt_price(x):
    if x>=1000:return f"{x:.2f}"
    if x>=1:return f"{x:.5f}".rstrip('0').rstrip('.')
    return f"{x:.8f}".rstrip('0').rstrip('.')


def tg(msg):
    if not TOKEN or not CHAT_ID:
        print(msg); return False
    url=f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        r=requests.post(url,json={"chat_id":CHAT_ID,"text":msg,"parse_mode":"HTML","disable_web_page_preview":True},timeout=12)
        if not r.ok: print("Telegram error",r.text)
        return r.ok
    except Exception as e:
        print("Telegram exception",e); return False


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
    buy=s["side"]=="BUY"; head="🟢🟢 دخول شراء" if buy else "🔴🔴 دخول بيع"
    if combo: head="🔥🟢 توافق استراتيجيات قوي — شراء" if buy else "🔥🔴 توافق استراتيجيات قوي — بيع"
    strat = " + ".join(combo) if combo else s["strategy"]
    reasons="\n".join("✅ "+r for r in s["reasons"]); assists="، ".join(helpers_list) if helpers_list else "بدون تأكيدات إضافية"
    oi_txt=f"{oi:,.2f}" if oi is not None else "غير متاح مؤقتًا"
    funding_txt=f"{funding*100:+.5f}%" if funding is not None else "غير متاح"
    return f'''<b>{head}</b>\n\n💰 العملة: <b>#{symbol}.P</b>\n⏰ فريم الدخول: <b>{s['tf'].upper()}</b>\n🧩 الاستراتيجية: <b>{strat}</b>\n🏆 جودة الإعداد: <b>{s['quality']}%</b>\n\n🎯 الدخول: <b>{fmt_price(s['entry'])}</b>\n🛑 وقف الخسارة: <b>{fmt_price(s['sl'])}</b>\n✅ TP1: <b>{fmt_price(s['tp'][0])}</b> (1R)\n✅ TP2: <b>{fmt_price(s['tp'][1])}</b> (2R)\n✅ TP3: <b>{fmt_price(s['tp'][2])}</b> (3R)\n\n{reasons}\n\n🔎 عوامل مساعدة: {assists}\n📈 OI: {oi_txt}\n💸 Funding: {funding_txt}\n\n🕒 {now_ksa().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)\n🔗 <a href="https://www.binance.com/en/futures/{symbol}">Binance</a> | <a href="https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P">TradingView</a>\n\n⚠️ تنبيه إحصائي وليس ضمانًا للربح'''


def cooldown_key(symbol,s,combo=None):
    name="COMBO:"+"+".join(combo) if combo else s["strategy"]
    return f"{symbol}|{s['side']}|{s['tf']}|{name}"


def can_send(key): return time.time()-state["last_alerts"].get(key,0) >= COOLDOWN_MIN*60


def record(key,s,combo=None):
    with state_lock:
        state["last_alerts"][key]=time.time(); state["alerts"]+=1; state["by_side"][s["side"]]+=1
        state["by_strategy"]["COMBO" if combo else s["strategy"]]+=1


def analyze_symbol(symbol):
    dfs={}
    for tf in ENABLED_TFS:
        df=df_from_store(symbol,tf)
        if df is not None: dfs[tf]=df
    if not dfs: return []
    signals=[]
    for tf,df in dfs.items():
        for fn in (break_retest, liquidity_sweep, orderblock_sweep_bos, vwap_reclaim, compression_expansion):
            try: signals += fn(df,tf)
            except Exception as e: print(symbol,tf,fn.__name__,e)
    if all(k in dfs for k in ("15m","1h","4h")): signals += mtf_signal(dfs["15m"],dfs["1h"],dfs["4h"])
    funding=funding_cache.get(symbol)
    for s in signals:
        s["helpers"]=helpers(dfs[s["tf"]],s["side"]) if s["tf"] in dfs else []
        s["score"] = s["quality"] + min(10,len(s["helpers"])*1.5)
        s["funding"]=funding
    return signals


def process_signals(symbol, signals, budget):
    if not signals or budget<=0:return 0
    sent=0; oi=None
    for side in ("BUY","SELL"):
        g=[s for s in signals if s["side"]==side]; uniq=[]; seen=set()
        for s in sorted(g,key=lambda x:x["score"],reverse=True):
            if s["strategy"] not in seen: uniq.append(s); seen.add(s["strategy"])
        if len(uniq)>=2 and sent<budget:
            lead=uniq[0].copy(); combo=[x["strategy"] for x in uniq]
            lead["sl"] = min(x["sl"] for x in uniq) if side=="BUY" else max(x["sl"] for x in uniq)
            risk=abs(lead["entry"]-lead["sl"]); lead["tp"]=[lead["entry"]+(risk*r if side=="BUY" else -risk*r) for r in (1,2,3)]
            lead["quality"]=min(99, max(x["quality"] for x in uniq)+4*(len(uniq)-1)); lead["reasons"]=[f"توافق {len(uniq)} استراتيجيات"]
            key=cooldown_key(symbol,lead,combo)
            if can_send(key):
                if oi is None: oi=get_oi_soft(symbol)
                if tg(alert_text(symbol,lead,lead["helpers"],oi,lead.get("funding"),combo)):
                    record(key,lead,combo); sent+=1
        for s in uniq:
            if sent>=budget:break
            key=cooldown_key(symbol,s)
            if can_send(key):
                if oi is None: oi=get_oi_soft(symbol)
                if tg(alert_text(symbol,s,s["helpers"],oi,s.get("funding"))):
                    record(key,s); sent+=1
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
    if not symbol or tf not in ENABLED_TFS or symbol not in radar_set:return
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
                elif x.get("e")=="markPriceUpdate": funding_cache[s]=float(x.get("r",0) or 0)
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
            for tf in ENABLED_TFS: current.append(f"{s.lower()}@kline_{tf}")
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
        for tf in ENABLED_TFS:
            rows=raw_klines(symbol,tf,BOOTSTRAP_LIMIT)
            with data_lock: candle_store[(symbol,tf)] = deque(rows,maxlen=max(BOOTSTRAP_LIMIT+5,230))
        bootstrapped.add(symbol)
        with state_lock: state["bootstrapped"]=len(bootstrapped)
        subscribe_streams([f"{symbol.lower()}@kline_{tf}" for tf in ENABLED_TFS])
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
                # Bootstrap only newly selected symbols. REST is intentionally single-file + rate limited.
                for symbol in target:
                    if symbol not in bootstrapped:
                        bootstrap_symbol(symbol)
                # ensure live subscriptions after rank changes/reconnects
                subscribe_streams([f"{s.lower()}@kline_{tf}" for s in target for tf in ENABLED_TFS])
            time.sleep(RADAR_REFRESH_SECONDS)
        except Exception as e:
            print("radar worker",e); time.sleep(10)


def signal_worker():
    while True:
        time.sleep(max(0.5,SIGNAL_CHECK_SECONDS))
        with data_lock:
            batch=list(dirty_symbols); dirty_symbols.clear()
        if not batch: continue
        found=[]
        for symbol in batch:
            try:
                sigs=analyze_symbol(symbol)
                if sigs:
                    best=max((x.get("score",x.get("quality",0)) for x in sigs),default=0)
                    distinct=len({(x["side"],x["strategy"]) for x in sigs})
                    found.append((best+min(12,distinct*2),symbol,sigs))
            except Exception as e: print("signal analyze",symbol,e)
        found.sort(key=lambda z:z[0],reverse=True)
        budget=MAX_ALERTS
        for _,symbol,sigs in found:
            if budget<=0:break
            sent=process_signals(symbol,sigs,budget); budget-=sent
        with state_lock: state["last_signal_check"]=now_ksa().isoformat()


@app.get("/")
def home():
    return jsonify({"name":"Ahmed Strategy Fusion Bot","version":state["version"],"status":"running","transport":"WebSocket live + throttled REST bootstrap","strategies":6,"timeframes":ENABLED_TFS,"stats":"/stats","health":"/health"})

@app.get("/health")
def health():
    blocked=max(0,int(state["rest_blocked_until"]-time.time()))
    return jsonify({"ok":True,"version":state["version"],"ws_connected":state["ws_connected"],"ws_last_event":state["ws_last_event"],"radar_count":state["radar_count"],"bootstrapped":state["bootstrapped"],"bootstrap_total":state["bootstrap_total"],"rest_backoff_seconds":blocked,"last_error":state["last_error"]})

@app.get("/stats")
def stats():
    blocked=max(0,int(state["rest_blocked_until"]-time.time()))
    return jsonify({"started":state["started"],"version":state["version"],"ws_connected":state["ws_connected"],"ws_last_event":state["ws_last_event"],"last_signal_check":state["last_signal_check"],"radar_count":state["radar_count"],"bootstrapped":state["bootstrapped"],"bootstrap_total":state["bootstrap_total"],"alerts":state["alerts"],"by_strategy":dict(state["by_strategy"]),"by_side":dict(state["by_side"]),"rest_backoff_seconds":blocked,"rest_418_count":state["rest_418_count"],"rest_429_count":state["rest_429_count"],"last_error":state["last_error"]})


threading.Thread(target=websocket_worker,daemon=True).start()
threading.Thread(target=radar_worker,daemon=True).start()
threading.Thread(target=signal_worker,daemon=True).start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")))
