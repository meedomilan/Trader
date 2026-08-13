import os
import time
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify

BASE = "https://fapi.binance.com"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "30"))
MIN_QV = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "1000000"))
RADAR_POOL = int(os.getenv("RADAR_POOL", "120"))
MAX_ALERTS = int(os.getenv("MAX_ALERTS_PER_SCAN", "5"))
COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MINUTES", "180"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "20"))
SYMBOL_CACHE_SECONDS = int(os.getenv("SYMBOL_CACHE_SECONDS", "300"))
ENABLED_TFS = [tf for tf, key in [("15m","ENABLE_15M"),("1h","ENABLE_1H"),("4h","ENABLE_4H")] if os.getenv(key,"true").lower()=="true"]
KSA = timezone(timedelta(hours=3))

session = requests.Session()
session.headers.update({"User-Agent":"AhmedStrategyFusionBot/1.0"})
app = Flask(__name__)

state = {
    "started": datetime.now(KSA).isoformat(),
    "last_scan": None,
    "last_error": None,
    "scans": 0,
    "alerts": 0,
    "by_strategy": defaultdict(int),
    "by_side": defaultdict(int),
    "last_alerts": {},
}
lock = threading.Lock()
symbol_cache = {"ts": 0.0, "items": []}


def now_ksa():
    return datetime.now(KSA)


def get_json(path, params=None, timeout=10):
    r = session.get(BASE + path, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def symbols():
    now=time.time()
    if symbol_cache["items"] and now-symbol_cache["ts"] < SYMBOL_CACHE_SECONDS:
        return list(symbol_cache["items"])
    info = get_json("/fapi/v1/exchangeInfo")
    valid = {s["symbol"] for s in info["symbols"] if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING"}
    tick = get_json("/fapi/v1/ticker/24hr")
    ranked = []
    for x in tick:
        if x["symbol"] in valid:
            qv = float(x.get("quoteVolume",0))
            if qv >= MIN_QV:
                ranked.append((x["symbol"], qv))
    ranked.sort(key=lambda z:z[1], reverse=True)
    items=[s for s,_ in ranked[:RADAR_POOL]]
    symbol_cache["ts"]=now; symbol_cache["items"]=items
    return items


def klines(symbol, interval, limit=220):
    raw = get_json("/fapi/v1/klines", {"symbol":symbol,"interval":interval,"limit":limit})
    cols = ["open_time","open","high","low","close","volume","close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open","high","low","close","volume","quote_volume","taker_buy_base"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return enrich(df)


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
    # RSI
    d = df.close.diff(); gain=d.clip(lower=0).rolling(14).mean(); loss=(-d.clip(upper=0)).rolling(14).mean()
    rs = gain/loss.replace(0,np.nan); df["rsi"] = 100-(100/(1+rs))
    # MACD helper
    fast=df.close.ewm(span=12,adjust=False).mean(); slow=df.close.ewm(span=26,adjust=False).mean()
    df["macd"] = fast-slow; df["macd_signal"] = df.macd.ewm(span=9,adjust=False).mean()
    return df


def oi_funding(symbol):
    oi = float(get_json("/fapi/v1/openInterest", {"symbol":symbol}).get("openInterest",0))
    prem = get_json("/fapi/v1/premiumIndex", {"symbol":symbol})
    funding = float(prem.get("lastFundingRate",0))
    return oi, funding


def helpers(df, side):
    x=df.iloc[-1]; p=df.iloc[-2]
    bull = side=="BUY"
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
    if invalid is None:
        invalid = entry-1.2*atr if side=="BUY" else entry+1.2*atr
    invalid=float(invalid)
    risk=abs(entry-invalid)
    if risk <= 0: return None
    tps=[entry+risk*r if side=="BUY" else entry-risk*r for r in (1,2,3)]
    return {"strategy":strategy,"side":side,"tf":tf,"entry":entry,"sl":invalid,"tp":tps,"quality":round(float(quality),1),"reasons":reasons or []}


def break_retest(df, tf):
    x,p=df.iloc[-1],df.iloc[-2]; atr=x.atr
    if pd.isna(atr): return []
    window=df.iloc[-22:-2]
    resistance=window.high.max(); support=window.low.min()
    out=[]
    # Previous candle breaks, current retests and closes back in breakout direction
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
    recent=df.iloc[-15:-1]
    # proxy OB: last opposite impulse candle among previous 20
    prevs=df.iloc[-25:-4]
    bear=prevs[prevs.close<prevs.open]
    bull=prevs[prevs.close>prevs.open]
    if not bear.empty:
        ob=bear.iloc[-1]; zone_low=min(ob.open,ob.close); zone_high=ob.high
        swing=recent.low.min()
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
    prev_width=((prev.bb_up-prev.bb_dn)/prev.bb_mid).mean()
    cur_width=(x.bb_up-x.bb_dn)/x.bb_mid if x.bb_mid else np.nan
    compressed=prev["range"].mean() < 0.85*df.iloc[-31:-11]["range"].mean()
    vol=x.volume>1.6*x.vol_ma
    if compressed and vol and x.close>prev.high.max() and x.delta>0:
        out.append(signal("Compression → Expansion","BUY",tf,df,x.close,prev.low.tail(5).min()-0.15*x.atr,84,["ضغط سابق","اختراق بحجم مرتفع","Delta شرائي"]))
    if compressed and vol and x.close<prev.low.min() and x.delta<0:
        out.append(signal("Compression → Expansion","SELL",tf,df,x.close,prev.high.tail(5).max()+0.15*x.atr,84,["ضغط سابق","كسر بحجم مرتفع","Delta بيعي"]))
    return out


def mtf_signal(d15,d1,d4):
    out=[]; x15=d15.iloc[-1]; x1=d1.iloc[-1]; x4=d4.iloc[-1]
    bull4=x4.close>x4.ema50 and x4.ema20>x4.ema50
    bear4=x4.close<x4.ema50 and x4.ema20<x4.ema50
    pull1_buy=x1.low<=x1.ema20+0.4*x1.atr and x1.close>x1.ema20
    pull1_sell=x1.high>=x1.ema20-0.4*x1.atr and x1.close<x1.ema20
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
    r=requests.post(url,json={"chat_id":CHAT_ID,"text":msg,"parse_mode":"HTML","disable_web_page_preview":True},timeout=12)
    if not r.ok: print("Telegram error",r.text)
    return r.ok


def alert_text(symbol,s,helpers_list,oi,funding,combo=None):
    buy=s["side"]=="BUY"
    head="🟢🟢 دخول شراء" if buy else "🔴🔴 دخول بيع"
    if combo:
        head="🔥🟢 توافق استراتيجيات قوي — شراء" if buy else "🔥🔴 توافق استراتيجيات قوي — بيع"
    strat = " + ".join(combo) if combo else s["strategy"]
    reasons="\n".join("✅ "+r for r in s["reasons"])
    assists="، ".join(helpers_list) if helpers_list else "بدون تأكيدات إضافية"
    return f'''<b>{head}</b>\n\n💰 العملة: <b>#{symbol}.P</b>\n⏰ فريم الدخول: <b>{s['tf'].upper()}</b>\n🧩 الاستراتيجية: <b>{strat}</b>\n🏆 جودة الإعداد: <b>{s['quality']}%</b>\n\n🎯 الدخول: <b>{fmt_price(s['entry'])}</b>\n🛑 وقف الخسارة: <b>{fmt_price(s['sl'])}</b>\n✅ TP1: <b>{fmt_price(s['tp'][0])}</b> (1R)\n✅ TP2: <b>{fmt_price(s['tp'][1])}</b> (2R)\n✅ TP3: <b>{fmt_price(s['tp'][2])}</b> (3R)\n\n{reasons}\n\n🔎 عوامل مساعدة: {assists}\n📈 OI: {oi:,.2f}\n💸 Funding: {funding*100:+.5f}%\n\n🕒 {now_ksa().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)\n🔗 <a href="https://www.binance.com/en/futures/{symbol}">Binance</a> | <a href="https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}.P">TradingView</a>\n\n⚠️ تنبيه إحصائي وليس ضمانًا للربح'''


def cooldown_key(symbol,s,combo=None):
    name="COMBO:"+"+".join(combo) if combo else s["strategy"]
    return f"{symbol}|{s['side']}|{s['tf']}|{name}"


def can_send(key):
    t=state["last_alerts"].get(key,0)
    return time.time()-t >= COOLDOWN_MIN*60


def record(key,s,combo=None):
    state["last_alerts"][key]=time.time(); state["alerts"]+=1; state["by_side"][s["side"]]+=1
    state["by_strategy"]["COMBO" if combo else s["strategy"]]+=1


def analyze_symbol(symbol):
    dfs={tf:klines(symbol,tf) for tf in ENABLED_TFS}
    signals=[]
    for tf,df in dfs.items():
        for fn in (break_retest, liquidity_sweep, orderblock_sweep_bos, vwap_reclaim, compression_expansion):
            try: signals += fn(df,tf)
            except Exception as e: print(symbol,tf,fn.__name__,e)
    if all(k in dfs for k in ("15m","1h","4h")):
        signals += mtf_signal(dfs["15m"],dfs["1h"],dfs["4h"])
    oi,funding=oi_funding(symbol)
    # enrich signals with helper confirmations
    for s in signals:
        s["helpers"]=helpers(dfs[s["tf"]],s["side"]) if s["tf"] in dfs else []
        s["score"] = s["quality"] + min(10,len(s["helpers"])*1.5)
        s["oi"]=oi; s["funding"]=funding
    return signals


def process_signals(symbol, signals, budget):
    if not signals or budget<=0:return 0
    sent=0
    # group same side; a combination is 2+ distinct strategies firing now
    for side in ("BUY","SELL"):
        g=[s for s in signals if s["side"]==side]
        uniq=[]; seen=set()
        for s in sorted(g,key=lambda x:x["score"],reverse=True):
            if s["strategy"] not in seen:
                uniq.append(s); seen.add(s["strategy"])
        if len(uniq)>=2 and sent<budget:
            lead=uniq[0].copy(); combo=[x["strategy"] for x in uniq]
            # consensus risk: conservative SL furthest invalidation
            lead["sl"] = min(x["sl"] for x in uniq) if side=="BUY" else max(x["sl"] for x in uniq)
            risk=abs(lead["entry"]-lead["sl"]); lead["tp"]=[lead["entry"]+(risk*r if side=="BUY" else -risk*r) for r in (1,2,3)]
            lead["quality"]=min(99, max(x["quality"] for x in uniq)+4*(len(uniq)-1))
            lead["reasons"]=[f"توافق {len(uniq)} استراتيجيات"]
            key=cooldown_key(symbol,lead,combo)
            if can_send(key):
                if tg(alert_text(symbol,lead,lead["helpers"],lead["oi"],lead["funding"],combo)):
                    record(key,lead,combo); sent+=1
        # independent alerts remain independent
        for s in uniq:
            if sent>=budget:break
            key=cooldown_key(symbol,s)
            if can_send(key):
                if tg(alert_text(symbol,s,s["helpers"],s["oi"],s["funding"])):
                    record(key,s); sent+=1
    return sent


def scan_once():
    syms=symbols()
    found=[]

    # FAST: analyze symbols concurrently instead of serial HTTP requests.
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as ex:
        futures={ex.submit(analyze_symbol,sym): sym for sym in syms}
        for fut in as_completed(futures):
            sym=futures[fut]
            try:
                sig=fut.result()
                if sig:
                    found.append((sym,sig))
            except Exception as e:
                print("scan symbol error",sym,e)

    # Scan EVERY symbol first. MAX_ALERTS limits Telegram messages only; it no longer stops market scanning.
    ranked=[]
    for sym,sigs in found:
        best=max((x.get("score",x.get("quality",0)) for x in sigs), default=0)
        distinct=len({(x["side"],x["strategy"]) for x in sigs})
        ranked.append((best + min(12, distinct*2), sym, sigs))
    ranked.sort(key=lambda z:z[0], reverse=True)

    n=0
    for _,sym,sigs in ranked:
        if n>=MAX_ALERTS:
            break
        n+=process_signals(sym,sigs,MAX_ALERTS-n)

    with lock:
        state["scans"]+=1; state["last_scan"]=now_ksa().isoformat(); state["last_error"]=None
    return n


def worker():
    time.sleep(4)
    while True:
        try: scan_once()
        except Exception as e:
            state["last_error"]=str(e); print("worker error",e)
        time.sleep(max(3, SCAN_SECONDS))

@app.get("/")
def home():
    return jsonify({"name":"Ahmed Strategy Fusion Bot","status":"running","strategies":6,"timeframes":ENABLED_TFS,"stats":"/stats","health":"/health"})

@app.get("/health")
def health():
    return jsonify({"ok":True,"last_scan":state["last_scan"],"last_error":state["last_error"]})

@app.get("/stats")
def stats():
    return jsonify({"started":state["started"],"last_scan":state["last_scan"],"scans":state["scans"],"alerts":state["alerts"],"by_strategy":dict(state["by_strategy"]),"by_side":dict(state["by_side"]),"last_error":state["last_error"]})

@app.get("/scan-now")
def scan_now():
    try:
        n=scan_once(); return jsonify({"ok":True,"alerts_sent":n})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}),500

threading.Thread(target=worker,daemon=True).start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")))
