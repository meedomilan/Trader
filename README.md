# Ahmed Strategy Fusion Bot v1.2 — WebSocket FAST

هذه النسخة صُممت لعلاج تأخير التنبيهات وحظر Binance 418/429 الذي ظهر مع polling المتكرر.

## ما تغير

- أُلغي طلب `exchangeInfo` و`ticker/24hr` من دورة الفحص.
- اختيار العملات الأعلى سيولة يأتي من WebSocket `!miniTicker@arr`.
- الشموع الحية 15M / 1H / 4H تأتي من WebSocket kline streams.
- Funding يأتي من `!markPrice@arr@1s` عبر WebSocket.
- REST يستخدم فقط لتهيئة تاريخ الشموع مرة واحدة لكل عملة/فريم، وبمحدد سرعة محافظ.
- OI يُطلب فقط عند وجود إشارة فعلية، مع كاش 5 دقائق.
- عند 418 أو 429 يدخل REST في Backoff تلقائي بدل تكرار الطلبات وإطالة الحظر.
- تحليل الاستراتيجيات محلي كل ثانيتين تقريبًا على العملات التي تغيرت فقط.

## الاستراتيجيات

1. Break & Retest
2. Liquidity Sweep + Reclaim
3. MTF 4H → 1H → 15M
4. Order Block + Sweep + BOS
5. VWAP Reclaim / Rejection
6. Compression → Expansion

Bollinger / EMA / MACD / Volume / Delta / CVD عوامل مساعدة وليست استراتيجيات مستقلة.

## Railway Variables

```env
TELEGRAM_BOT_TOKEN=YOUR_TOKEN
TELEGRAM_CHAT_ID=YOUR_CHAT_ID
MIN_QUOTE_VOLUME_USDT=1000000
RADAR_POOL=180
MAX_ALERTS_PER_SCAN=5
ALERT_COOLDOWN_MINUTES=180
ENABLE_15M=true
ENABLE_1H=true
ENABLE_4H=true
SIGNAL_CHECK_SECONDS=2
RADAR_REFRESH_SECONDS=60
REST_MIN_INTERVAL_MS=160
REST_418_DEFAULT_BACKOFF_SECONDS=300
BOOTSTRAP_KLINES_LIMIT=220
WS_RECONNECT_SECONDS=5
PORT=8080
```

> لا تحتاج `SCAN_SECONDS` ولا `MAX_CONCURRENCY` ولا `SYMBOL_CACHE_SECONDS` في هذه النسخة؛ الفحص أصبح Event-driven عبر WebSocket.

## التشغيل الأول

في أول تشغيل، سيبدأ WebSocket فورًا ثم يحدد أعلى العملات من السيولة. بعدها يحمّل تاريخ 220 شمعة لكل فريم مرة واحدة وبشكل متدرج حتى لا يضغط REST API. أثناء هذه المرحلة راقب `/health`:

- `ws_connected: true` يعني البث الحي شغال.
- `bootstrapped / bootstrap_total` يوضح تقدم تهيئة العملات.
- `rest_backoff_seconds` يظهر مدة الانتظار إذا كان IP ما زال تحت 418/429.

إذا كان Railway IP ما زال محظورًا من النسخة القديمة، لا تعِد تشغيل الخدمة باستمرار. اترك النسخة الجديدة تعمل؛ ستنتظر Backoff بدل ضرب Binance.

## الصفحات

- `/health`
- `/stats`

## ملاحظة

البوت يرسل تنبيهات فقط ولا ينفذ صفقات. لا توجد استراتيجية مضمونة الربح.
