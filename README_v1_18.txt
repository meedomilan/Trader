v1.18 EARLY BLOCK REVERSAL + NO CHASE

- دخول مبكر داخل/قرب البلوك أو منطقة الخطة، بدل انتظار BOS متأخر.
- نماذج انعكاس: Hammer/Pin, Engulfing, Morning/Evening Star, Tweezer, Failed breakdown/breakout reclaim.
- لا يعتمد نموذج الشمعة وحده: يلزم Delta أو CVD متوافق.
- REAL Binance aggTrade Footprint يستخدم إذا كان متوفرًا؛ لا يتم اختراع Footprint.
- NO CHASE بعد 0.55 ATR من منطقة الانعكاس: ينتظر Retest ولا يرسل دخولًا متأخرًا.
- لا دخول إذا مساحة الهدف أقل من 1R.
- الوقف المبكر خلف قاع/قمة الانعكاس والبلوك + هامش 0.16 ATR.
- Daily/Weekly سياق فقط؛ 1H/4H يمنعان الصفقة إذا كانا كلاهما عكسها.
- رسالة Telegram مختصرة، والبنية الداخلية مخفية.
- الاستراتيجيات الأصلية والدمج المميز باقية.


## v1.18.1 corrective patch
- نماذج الانعكاس مطبقة فعليًا كدوال مستقلة، ولا تكفي الشمعة وحدها؛ يجب توافق Delta أو CVD.
- يمكن فرض وجود بلوك REAL aggTrade قبل التأكيد المبكر عبر `FOOTPRINT_REQUIRE_FOR_EARLY=true`.
- تمت إضافة حماية من تكرار `aggTrade id` وتجاهل الحزم المتأخرة التي تعيد الزمن إلى الخلف.
- تم استبعاد `Trend Compression Breakout` من تنبيهات COMBO لتجنب التكرار.
- `EARLY_MIN_RR=1.0` هو الحد الافتراضي لمساحة الهدف في مسار الانعكاس المبكر.
