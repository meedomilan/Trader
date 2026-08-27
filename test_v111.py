import os, sys, types, threading
import pandas as pd

os.environ['DB_PATH'] = '/tmp/ahmed_bot_test.db'
fake = types.ModuleType('websocket'); fake.WebSocketApp = object; sys.modules['websocket'] = fake
orig_start = threading.Thread.start; threading.Thread.start = lambda self: None
import app
threading.Thread.start = orig_start

rows = []
for i in range(60):
    p = 100 + i * 0.1
    rows.append({'open_time':i,'open':p,'high':p+1,'low':p-1,'close':p+0.2,'volume':100,'close_time':i,'quote_volume':10000,'trades':10,'taker_buy_base':55,'taker_buy_quote':5500,'ignore':'0'})
df = app.enrich(pd.DataFrame(rows))

buy = app.signal('T','BUY','15m',df,entry=110,invalid=108,quality=70)
sell = app.signal('T','SELL','15m',df,entry=110,invalid=112,quality=70)
invalid_buy = app.signal('T','BUY','15m',df,entry=110,invalid=111,quality=70)
invalid_sell = app.signal('T','SELL','15m',df,entry=110,invalid=109,quality=70)
assert buy and buy['sl'] < buy['entry'] < buy['tp'][0] < buy['tp'][1] < buy['tp'][2]
assert sell and sell['sl'] > sell['entry'] > sell['tp'][0] > sell['tp'][1] > sell['tp'][2]
assert invalid_buy is None and invalid_sell is None
print('LEVEL_VALIDATION_OK')
print('VERSION', app.state['version'])
