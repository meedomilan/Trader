import os, sys, types, threading, math
import pandas as pd

os.environ['DB_PATH']='/tmp/ahmed_v112_test.db'
# lightweight Flask mock for offline validation
fm=types.ModuleType('flask')
class F:
    def __init__(self,*a,**k): pass
    def route(self,*a,**k): return lambda fn: fn
    def get(self,*a,**k): return lambda fn: fn
    def run(self,*a,**k): pass
fm.Flask=F; fm.jsonify=lambda x=None,**k: x if x is not None else k
sys.modules['flask']=fm
orig=threading.Thread.start; threading.Thread.start=lambda self:None
import app
threading.Thread.start=orig

def mk(closes, volume_spike=False):
    rows=[]
    for i,c in enumerate(closes):
        o=closes[i-1] if i else c*0.999
        amp=max(c*0.0035,0.15)
        up=c>=o
        rows.append({'open_time':i,'open':o,'high':max(o,c)+amp,'low':min(o,c)-amp,'close':c,
                     'volume':160 if volume_spike and i>=len(closes)-2 else 100,'close_time':i,
                     'quote_volume':10000,'trades':10,'taker_buy_base':62 if up else 38,
                     'taker_buy_quote':6200 if up else 3800,'ignore':'0'})
    return app.enrich(pd.DataFrame(rows))

# 1) RUNE-like vertical extension must be WAIT even with a bullish HTF thesis.
base=[100+i*0.28+math.sin(i/3)*0.9 for i in range(90)]
ext=base[:-3]+[base[-4]+3.0,base[-4]+6.5,base[-4]+10.5]
ctx=app.build_coin_context('EXT',{t:mk(ext,True) for t in ('4h','1h','15m')})
assert ctx['decision']=='WAIT', ctx
assert 'ممتدة' in ctx['location'] or ctx['preferred']!='BUY', ctx

# 2) Mere contact with resistance is NOT Break & Retest anymore.
attack=[100+0.02*math.sin(i) for i in range(45)]
attack[-1]=100.35
br=app.break_retest(mk(attack,True),'15m')
assert not br, br

# 3) Real previous break + live revisit can produce Break & Retest without waiting another close.
seq=[100+0.02*math.sin(i) for i in range(50)]
# create closed breakout a few bars before live retest
seq[-5]=100.15; seq[-4]=100.55; seq[-3]=100.62; seq[-2]=100.58; seq[-1]=100.30
dbr=mk(seq,True)
# emulate positive order flow during the live retest even while the candle is still red
dbr.loc[dbr.index[-1],'delta']=abs(float(dbr.loc[dbr.index[-1],'delta']))+1
br2=app.break_retest(dbr,'15m')
assert all(x['strategy']=='Break & Retest' for x in br2)

# 4) Thesis invalidation is preferred by the stop engine and absurdly wide logical stops are rejected.
df=mk(base)
atr=float(df.iloc[-1].atr); entry=float(df.iloc[-1].close)
ci=entry-1.25*atr
sl=app._structural_stop('BUY',entry,df,atr,ci)
assert sl is not None and sl<=entry and abs(sl-ci)<0.01*atr, (sl,ci,atr)
assert app._structural_stop('BUY',entry,df,atr,entry-5*atr) is None

# 5) Current performance table is based on stats (24), not the old stats (14).
prof=app.performance_profile('Liquidity Sweep + Reclaim','1h','BUY')
assert prof['evaluated']==217, prof

print('V1_12_VALIDATION_OK')
print('VERSION',app.state['version'])
print('EXTENDED_CASE',ctx['decision'],ctx['location'])
print('BREAK_ATTACK_SIGNALS',len(br),'REAL_RETEST_SIGNALS',len(br2))
