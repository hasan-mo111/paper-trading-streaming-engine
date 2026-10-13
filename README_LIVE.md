# Aetheron Live Trading Engine

تطبيق ويب حقيقي للتداول باستخدام استراتيجية p.py مع واجهة متقدمة تعرض الأزواج العشرة في بطاقات مع تحديث مباشر.

## المزايا

- **تداول مباشر (Real-Time)**: يستخدم FastAPI مع WebSockets للتحديث اللحظي
- **عزل رأس المال**: كل زوج من 10 أزواج له رصيد مستقل $100.00
- **واجهة متقدمة**: تصميم احترافي مع Bootstrap 5 وChart.js للرسوم البيانية المصغرة
- **استراتيجية مستوردة**: يعتمد بالكامل على منطق ملف `p.py` الأصلي
- **جاهز للنشر على Render**: مع requirements.txt و render.yaml

## التشغيل المحلي

```bash
pip install -r requirements.txt
python app.py
```

سيفتح التطبيق على: http://localhost:8000

## النشر على Render

1. رفع المشروع إلى GitHub
2. ربط المشروع مع Render
3. سيتم استخدام render.yaml تلقائياً للتكوين
4. Build Command: `pip install -r requirements.txt`
5. Start Command: `gunicorn app:app --host 0.0.0.0 --port $PORT --workers 1 --timeout 120`

## API Endpoints

- `GET /` - لوحة التحكم الرئيسية
- `GET /api/state` - حالة النظام كامل (JSON)
- `WS /ws` - WebSocket للتحديثات اللحظية

## المتطلبات

- Python 3.9+
- المكتبات في requirements.txt
- يعمل بدون تبعيات خارجية (يولد بيانات تجريبية تلقائياً إذا لم يتوفر اتصال بـ Exchange)