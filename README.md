# Ahmed Strategy Fusion Bot v1.8 — PERFORMANCE PRIORITY

## ما تغير في هذه النسخة
- لم يتم تغيير توقيت v1.7 أو منطق التأكيد الحي.
- تمت إضافة أولوية تاريخية مبنية على `stats (14)` لكل استراتيجية + فريم + اتجاه.
- الاستراتيجية القوية تحصل على Boost بسيط في ترتيب الفرص، بدون أن يتغلب التاريخ على التأكيد الحي.
- إذا كانت العينة >= 40 والنجاح التاريخي أقل من 40%، يمنع إرسالها افتراضيًا.
- العينات الأقل من 40 تبقى بحالة `يتعلم` ولا يتم حظرها بسبب عينة صغيرة.
- تنبيه Telegram يعرض: التصنيف التاريخي، نسبة النجاح، نسبة TP1، وحجم العينة.
- `/stats.json` يعرض إعدادات سياسة الأداء وعدد الإشارات التي تم منعها تاريخيًا.

## التصنيف
- S+: نجاح تاريخي >= 70%
- A: >= 60%
- B: >= 52%
- C: >= 45%
- C-: >= 40%
- D: أقل من 40% — يمنع الإرسال فقط إذا كانت العينة كافية
- LEARN: العينة أقل من الحد المطلوب

## المجموعات الأقوى المستخدمة من stats (14)
- Break & Retest BUY 4H — S+
- Break & Retest BUY 1H — A
- MTF 4H→1H→15M BUY 15M — A
- VWAP Reclaim BUY 15M — A
- Liquidity Sweep + Reclaim BUY 15M — B
- Order Block + Sweep + BOS BUY 15M — B

> ملاحظة: لم يتم إصلاح Binance REST 418/429 في هذه النسخة بناءً على قرار تأجيله.

# Ahmed Strategy Fusion Bot v1.6 — LIVE EVENT CONFIRMATION

## تعديل التوقيت على جميع الاستراتيجيات

تم تطبيق نفس منطق التوقيت السريع على الاستراتيجيات الست كلها:
- Break & Retest
- Liquidity Sweep + Reclaim
- MTF 4H→1H→15M
- Order Block + Sweep + BOS
- VWAP Reclaim / Rejection
- Compression → Expansion

### منطق الدخول الجديد
1. تظهر الاستراتيجية داخليًا فقط.
2. يراقب البوت micro structure على فريم التأكيد.
3. عند الكسر يتم تسجيل الحدث فورًا من السعر الحي WebSocket.
4. لا ينتظر إغلاق شمعة 5M/15M ولا يشترط شمعة إضافية.
5. الدخول يرسل فور تحقق أحد المسارين بعد الكسر:
   - micro retest + reclaim، أو
   - live momentum hold واضح بدون مطاردة السعر.
6. إذا امتد السعر أكثر من الحد المسموح قبل التأكيد تلغى الفرصة.
7. SL يبقى خلف آخر swing حقيقي + ATR buffer، وليس وقفًا قريبًا ثابتًا.

15M يستخدم 5M داخليًا، و1H/4H يستخدمان 15M داخليًا. فحص الإشارات الافتراضي كل ثانيتين.

# Ahmed Strategy Fusion Bot v1.5 TWO-STAGE CONFIRMED ENTRY

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


## v1.5 — Two-Stage Confirmed Entry + Structural Stop
بناءً على نتائج الإحصائيات ودراسة ما بعد الوقف، تم تعديل الدخول والوقف بدل توسيع SL عشوائيًا.

- ظهور الاستراتيجية لا يرسل دخولًا؛ تتحول داخليًا إلى WAIT_BREAK.
- المرحلة الأولى: يجب كسر البنية/البلوك المعاكس وإغلاق شمعة التأكيد خلف المستوى.
- المرحلة الثانية: يمنع الدخول في شمعة الكسر نفسها. يجب انتظار شمعة لاحقة تعيد اختبار المستوى وتثبت/تسترده.
- 15M يستخدم 5M للتأكيد افتراضيًا. 1H و4H يستخدمان 15M للتأكيد افتراضيًا.
- إذا امتد السعر أكثر من اللازم قبل التأكيد تلغى الفرصة بدل مطاردة السعر.
- إبطال الإعداد قبل التأكيد أصبح أبعد قليلًا من SL القديم بهامش ATR حتى لا تلغى الفرصة بسبب ضوضاء بسيطة.
- بعد التأكيد يعاد حساب وقف الخسارة خلف آخر قاع/قمة في فريم التأكيد + هامش ATR.
- يوجد حد أدنى لمسافة الوقف 0.75 ATR حتى لا يعود الوقف شديد القرب.
- إذا احتاج الوقف أكثر من 2.40 ATR على فريم التأكيد تلغى الصفقة بدل قبول مخاطرة مبالغ فيها.
- TP1/TP2/TP3 يعاد حسابها من الدخول المؤكد والوقف الهيكلي الجديد عند 1R/2R/3R.
- إحصائيات TP/SL وMFE/MAE ودراسة ما بعد الوقف تبقى فعالة كما هي.

## v1.7 — Early Setup + Live Confirm
- اكتشاف الـ Setup أصبح مبكرًا في جميع الاستراتيجيات الست، عند منطقة الشراء/البيع وليس بعد اكتمال الحركة.
- MTF: 4H/1H سياق فقط؛ لا ينتظر BOS كامل على 15M. يبدأ من منطقة انعكاس 15M ثم يؤكد حيًا على 5M.
- Break & Retest: يبدأ عند أول هجوم/كسر للمستوى ثم ينتظر Retest مصغر حي.
- Order Block: يبدأ عند لمس/رفض البلوك، والـ BOS ينتقل لمرحلة التأكيد الحي.
- VWAP: يبدأ حول منطقة VWAP عند أول استرداد/رفض، بدون انتظار إغلاق تقاطع كامل.
- Compression: يبدأ عند نهاية الضغط واقتراب الحد مع ارتفاع الحجم؛ أول كسر مصغر يؤكد الدخول.
- Liquidity Sweep: يبدأ من السحب والاسترداد الجزئي داخل الشمعة، ثم تأكيد حي.
- لا يتم إرسال Setup المبدئي إلى Telegram؛ الرسالة تخرج فقط بعد التأكيد الحي.
