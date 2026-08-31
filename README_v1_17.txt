Ahmed Strategy Fusion Bot v1.17 — REAL AGGTRADE FOOTPRINT

هذا الإصدار يستبدل تقدير Volumetric المبني على Kline ببيانات Binance Futures aggTrade الحقيقية.

المصدر:
- Binance Futures aggTrade WebSocket
- كل صفقة منفذة تحمل السعر والكمية و buyer-is-maker
- buyer-is-maker=false => aggressive BUY
- buyer-is-maker=true  => aggressive SELL

المحرك يبني Price-Level Footprint مباشر على:
15M / 1H / 4H / Daily / Weekly

ويحسب:
- Buy Quote / Sell Quote عند مستوى السعر
- Delta %
- POC من التداولات المنفذة
- Buy/Sell Imbalance
- Stacked Imbalance
- Strong Buy/Sell Footprint Block

مهم:
- بعد أول تشغيل يحتاج المحرك وقتًا ليجمع بيانات فعلية.
- 15M يتعلم أولًا، ثم 1H، ثم 4H، ثم Daily/Weekly مع استمرار التشغيل.
- البلوكات المكتملة تحفظ في SQLite، لذلك تبقى بعد إعادة التشغيل إذا كان /data دائمًا.
- لا يتم اختراع بلوك Daily/Weekly من الشموع؛ كلها مبنية من aggTrade الحقيقي الذي جمعه البوت.
- يوجد endpoint: /footprint.json?symbol=BTCUSDT
- الاستراتيجيات مستقلة، والدمج المميز الإضافي ما زال موجودًا.
- تحليل الاتجاه 1H/4H/Daily/Weekly والدخول/الوقف البنيوي محفوظ كما في v1.16.
