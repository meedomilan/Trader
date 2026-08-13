# Ahmed Strategy Fusion Bot v1.3 COMPLETE STATS

بوت تنبيهات Binance USDT Futures يعتمد على WebSocket للبيانات الحية ويستخدم REST بشكل محدود ومحمي من 418/429.

## الاستراتيجيات
1. Break & Retest
2. Liquidity Sweep + Reclaim
3. MTF 4H→1H→15M
4. Order Block + Sweep + BOS
5. VWAP Reclaim / Rejection
6. Compression → Expansion

كل استراتيجية ترسل منفصلة. إذا اجتمعت استراتيجيتان أو أكثر في نفس الاتجاه يصدر تنبيه COMBO خاص بالإضافة إلى الإشارات الفردية حسب حد التنبيهات.

## الجديد في v1.3
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
