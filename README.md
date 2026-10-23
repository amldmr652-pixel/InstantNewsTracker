# 📡 InstantNewsTracker

Türkiye ve Dünya gündemini anlık takip eden, kritik haberleri Telegram üzerinden bildiren hibrit haber takip sistemi.

## 🚀 Özellikler
- 11 farklı haber kaynağından eşzamanlı (asenkron) RSS tarama
- Akıllı anahtar kelime tabanlı kritik haber sınıflandırması
- Çapraz kaynak birleştirme — aynı haber tek bildirimde, en güvenilir kaynak birincil
- Telegram anlık bildirim (Türkçe, Markdown formatında)
- Streamlit canlı web paneli (3 saniyede bir otomatik yenileme)
- Disk destekli mükerrer engelleme (uygulama kapansa bile korunur)

## 📋 Kurulum

1. Python 3.10+ yükleyin
2. Bağımlılıkları kurun:
   ```bash
   pip install -r requirements.txt
   ```
3. `.env.example` dosyasını `.env` olarak kopyalayın ve Telegram bilgilerinizi girin:
   ```bash
   cp .env.example .env
   ```
4. `.env` dosyasını düzenleyin:
   ```
   TELEGRAM_BOT_TOKEN=your_bot_token_here
   TELEGRAM_CHAT_ID=your_chat_id_here
   ```

## 🤖 Telegram Bot Oluşturma
1. Telegram'da @BotFather'a gidin
2. `/newbot` komutu ile yeni bot oluşturun
3. Bot token'ını kopyalayın → `.env` dosyasına yapıştırın
4. Chat ID'nizi öğrenmek için @userinfobot'a mesaj atın
5. Chat ID'yi `.env` dosyasına yapıştırın

## ▶️ Çalıştırma
```bash
streamlit run app.py
```

## 📡 Haber Kaynakları
| Kaynak | Tür |
|--------|-----|
| Anadolu Ajansı | Güncel + Son Dakika |
| TRT Haber | Son Dakika |
| NTV | Gündem + Dünya |
| Habertürk | Manşet |
| Sözcü | Son Dakika |
| BBC World | Dünya |
| CNN Türk | Genel |
| Hürriyet | Genel |
| Reuters | Uluslararası |
