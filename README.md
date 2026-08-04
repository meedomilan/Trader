# Ahmed Early Explosion Trader v1

بوت مستقل يراقب جميع عقود Binance USDT-M الدائمة على فريمَي 1H و4H فقط.

## مراحل التنبيه
- 🟡 استعداد مبكر جدًا
- 🟠 دخول الآن
- 🔥 بداية الانفجار
- ⚠️ ضعف الزخم

## لا يستخدم
RSI أو MACD أو Stoch RSI أو KDJ.

## ملفات GitHub
ارفع الملفات كما هي إلى مستودع مستقل:
- app.py
- requirements.txt
- Procfile
- runtime.txt
- .env.example

## Railway Variables الضرورية
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

ثم أضف بقية المتغيرات من `.env.example` عند الحاجة. لا تضع التوكن داخل الكود.

## الاختبار
بعد النشر افتح:
- `/health`
- `/test-telegram`
- `/signals`
- `/stats`
- `/checkpoints`

## ملاحظات مهمة
- OI وCVD وReal Delta بيانات حقيقية مشتقة من Binance.
- Iceberg وSpoofing وAbsorption تقديرات احتمالية وليست كشفًا مؤكدًا.
- نقطة الدخول والوقف والأهداف خطة إحصائية، ولا ينفذ البوت صفقات.
- قاعدة SQLite داخل Railway قد تُفقد عند إعادة النشر إن لم تستخدم Volume دائم. اربط Railway Volume إلى مجلد `/app/data`.
