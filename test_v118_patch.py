import os, sys, types, threading, pandas as pd
os.environ['DB_PATH']='/tmp/ahmed_v118_patch.db'
f=types.ModuleType('flask')
class F:
    def __init__(self,*a,**k): pass
    def get(self,*a,**k): return lambda fn: fn
    def route(self,*a,**k): return lambda fn: fn
    def run(self,*a,**k): pass
f.Flask=F; f.jsonify=lambda x=None,**k:x if x is not None else k; sys.modules['flask']=f
w=types.ModuleType('websocket'); w.WebSocketApp=object; sys.modules['websocket']=w
orig=threading.Thread.start; threading.Thread.start=lambda self:None
import app
threading.Thread.start=orig
app.radar_set.add('TESTUSDT')
base=1800000000000
# Same ID twice must count once.
e={'e':'aggTrade','s':'TESTUSDT','p':'100.0','q':'1','T':base+1000,'m':False,'a':123}
app.process_aggtrade_event(e); app.process_aggtrade_event(dict(e))
assert app.state['aggtrade_events']==1, app.state['aggtrade_events']
# Fill the active 15m bucket and move forward.
for i in range(45):
    app.process_aggtrade_event({'e':'aggTrade','s':'TESTUSDT','p':'100.0','q':'1','T':base+2000+i*1000,'m':False,'a':1000+i})
app.process_aggtrade_event({'e':'aggTrade','s':'TESTUSDT','p':'100.2','q':'1','T':base+901000,'m':True,'a':2000})
start=app.footprint_live[('TESTUSDT','15m')]['start_ts']
# Late event must not rewind the active bucket.
app.process_aggtrade_event({'e':'aggTrade','s':'TESTUSDT','p':'99.9','q':'1','T':base+3000,'m':True,'a':3000})
assert app.footprint_live[('TESTUSDT','15m')]['start_ts']==start
print('V118_PATCH_OK')
