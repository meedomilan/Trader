# Ahmed Strategy Fusion Bot

بوت تنبيهات مستقل لعقود Binance USDⓈ-M Futures.

## الاستراتيجيات
1. Break & Retest
2. Liquidity Sweep + Reclaim
3. Multi-Timeframe 4H → 1H → 15M
4. Order Block + Sweep + BOS
5. VWAP Reclaim / Rejection
6. Compression → Expansion

كل استراتيجية يمكن أن ترسل تنبيهًا مستقلًا. إذا اجتمعت استراتيجيتان أو أكثر على نفس العملة والاتجاه، يرسل البوت تنبيه توافق خاص.

## Railway Variables
انسخ القيم من `.env.example` إلى Variables في Railway. لا تضع التوكن داخل الكود.

## الروابط
- `/` حالة الخدمة
- `/health` فحص الصحة
- `/stats` إحصائيات التنبيهات
- `/scan-now` تشغيل فحص يدوي سريع من المتصفح

## ملاحظات
- البوت تنبيهات فقط ولا يفتح صفقات.
- يستخدم بيانات Binance العامة.
- المؤشرات المساعدة (Bollinger/EMA/RSI/MACD/Volume/CVD proxy/Taker Delta/OI/Funding) لا ترسل تنبيهات مستقلة.

## شكل التنبيهات
- 🟢🟢 دخول شراء
- 🔴🔴 دخول بيع
- 🔥🟢 توافق استراتيجيات قوي — شراء
- 🔥🔴 توافق استراتيجيات قوي — بيع

وقف الخسارة مبني على منطقة الإبطال/القاع أو القمة المرتبطة بالإعداد، والأهداف 1R و2R و3R. ويمكن تطوير TP3 لاحقًا ليعتمد على أقرب سيولة.
