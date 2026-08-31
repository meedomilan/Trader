Ahmed Strategy Fusion Bot v1.9 — Binance Rate Limit Fixed

التعديلات:
- WebSocket subscription يبدأ قبل REST bootstrap.
- REST الافتراضي أبطأ: طلب كل 450ms بدل 160ms لتقليل 429/418.
- عند 418/429 يتم احترام Retry-After من Binance بدون تجميد WebSocket أو signal worker.
- bootstrap يتوقف فورًا أثناء الحظر ويستأنف تلقائيًا بعد انتهاء المدة.
- لا يوجد sleep لساعات داخل rest_lock.
- OI يبقى soft/cached ولا يعطل الإشارة إذا REST محظور.
- إضافات health/stats: bootstrap_paused و rest_skipped_backoff.

مهم: إذا Binance أرسل Retry-After طويلًا فلا يمكن تجاوز الحظر نفسه بأمان؛ الإصلاح يمنع البوت من زيادة الحظر ويحافظ على WebSocket أثناءه.
