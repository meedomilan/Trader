# Ahmed Strategy Fusion Bot v1.3.1 SL STUDY

بوت تنبيهات Binance USDT Futures يعتمد على WebSocket للبيانات الحية ويستخدم REST بشكل محدود ومحمي من 418/429.

## الاستراتيجيات
1. Break & Retest
2. Liquidity Sweep + Reclaim
3. MTF 4H→1H→15M
4. Order Block + Sweep + BOS
5. VWAP Reclaim / Rejection
6. Compression → Expansion

كل استراتيجية ترسل منفصلة. إذا اجتمعت استراتيجيتان أو أكثر في نفس الاتجاه يصدر تنبيه COMBO خاص بالإضافة إلى الإشارات الفردية حسب حد التنبيهات.

## الموجود من v1.3
- تسجيل كل تنبيه في SQLite بعد نجاح إرساله إلى Telegram.
- متابعة السعر الحي عبر markPrice WebSocket بعد التنبيه.
- تسجيل TP1 وTP2 وTP3 وSL حسب أول وصول فعلي يُرصد.
- تسجيل MFE وMAE بوحدة R.
- تسجيل متوسط الزمن للوصول إلى TP1.
- إغلاق النتيجة عند TP3 أو SL أو انتهاء المهلة الخاصة بالفريم.
- صفحة `/stats` عربية مرتبة تعرض أفضل الاستراتيجيات ونسبة النجاح والتفصيل حسب الفريم والاتجاه.
- صفحة `/stats.json` للبيانات الخام.
- صفحة `/health` لحالة WebSocket وREST وقاعدة البيانات.

## تعريف النجاح
نسبة النجاح = عدد الإشارات المغلقة التي وصلت TP1 قبل الإغلاق / عدد الإشارات المغلقة.
إذا ضرب السعر SL قبل TP1 تسجل SL. إذا وصل TP1 أو TP2 ثم عاد للـSL تبقى الأهداف التي تحققت محفوظة وتظهر النتيجة TP1_THEN_SL أو TP2_THEN_SL.

## Railway
1. ارفع ملفات المشروع أو ZIP إلى GitHub ثم اربطه بـ Railway.
2. أضف Variables الموجودة في `.env.example`.
3. أضف Railway Volume واجعل Mount Path هو `/data` حتى لا تضيع الإحصائيات عند إعادة النشر أو التشغيل.
4. اجعل `DB_PATH=/data/bot_stats.db`.
5. لا تستخدم أكثر من Worker واحد؛ `railway.json` و`Procfile` مجهزان بـ `--workers 1` حتى لا تتكرر التنبيهات أو متابعات الصفقات.

## الإعدادات المقترحة
```
MIN_QUOTE_VOLUME_USDT=1000000
RADAR_POOL=180
MAX_ALERTS_PER_SCAN=5
ALERT_COOLDOWN_MINUTES=180
SIGNAL_CHECK_SECONDS=2
RADAR_REFRESH_SECONDS=60
REST_MIN_INTERVAL_MS=160
REST_418_DEFAULT_BACKOFF_SECONDS=300
BOOTSTRAP_KLINES_LIMIT=220
WS_RECONNECT_SECONDS=5
DB_PATH=/data/bot_stats.db
RESULT_PERSIST_SECONDS=10
RESULT_TIMEOUT_15M_HOURS=12
RESULT_TIMEOUT_1H_HOURS=48
RESULT_TIMEOUT_4H_HOURS=120
PORT=8080
```

## الصفحات
- `/health` حالة البوت والاتصال وعدد الصفقات المفتوحة في المتابعة.
- `/stats` لوحة الإحصائيات الكاملة.
- `/stats.json` الإحصائيات بصيغة JSON.

ملاحظة: تنبيهات v1.2 القديمة لا يمكن تقييم TP/SL لها تلقائيًا لأن النسخة القديمة لم تكن تخزن مستويات كل تنبيه في قاعدة بيانات. يبدأ سجل النتائج الكامل من أول تشغيل v1.3.


## إضافة v1.3.1 — دراسة ما بعد وقف الخسارة فقط
لم يتم تعديل أي استراتيجية أو شرط دخول أو وقف خسارة أو هدف أو فريم أو تنبيه.

عندما تكون نتيجة الصفقة الأصلية `SL` وكان SL هو أول مستوى يتم لمسه، يبدأ تتبع افتراضي منفصل من نفس السعر الحي. النتيجة الأصلية تبقى `SL` ولا تتغير. يتوقف التتبع الافتراضي عند أحد أمرين:
- وصول السعر لاحقًا إلى TP1 الأصلي → `RECOVERED_TP1`.
- انتهاء مهلة الفريم الأصلية → `NO_RECOVERY_TIMEOUT`.

يتم حفظ أقصى انعكاس بوحدة R قبل الرجوع إلى TP1 أو انتهاء المهلة، وتعرض `/stats` عدد الصفقات التي كانت ستنجو لو كان الوقف عند 1.05R و1.10R و1.15R و1.20R و1.25R و1.50R.

مهم: الصفقات القديمة التي ضربت SL قبل تشغيل v1.3.1 لا يتم تخمين مسارها التاريخي، لذلك دراسة ما بعد SL تبدأ من أول SL جديد بعد نشر هذه النسخة.

## v1.3.2 — Telegram Queue only
This revision changes Telegram delivery only. Signals, entries, SL/TP, timeframes, strategies, statistics, and post-SL study are unchanged.
Alerts are queued and sent sequentially. If Telegram returns HTTP 429, the bot reads `retry_after`, waits, and retries the same message instead of dropping it.
Optional variable: `TELEGRAM_MIN_SEND_INTERVAL_SECONDS=1.10` (default already 1.10; no Railway change is required).


## v1.4 Confirmed Entry
All six strategy detectors now create an internal WAIT_CONFIRMATION setup only.
No Telegram message is sent for the setup stage.

A real entry alert is sent only after live micro-structure confirmation:
- BUY: break the opposing lower-high / bearish micro block, close above it, and hold it.
- SELL: break the opposing higher-low / bullish micro block, close below it, and hold it.
- 5m is used internally for confirmation (15m fallback).
- If the original invalidation/SL is hit before confirmation, the setup is silently cancelled.
- If confirmation arrives after excessive extension, it is silently cancelled to avoid chasing.
- Existing strategy logic, SL logic, TP logic, result statistics, post-SL study, WebSocket transport and Telegram queue are unchanged.
