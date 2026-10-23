# pip install streamlit aiohttp feedparser python-dotenv requests

import asyncio
import aiohttp
import feedparser
import requests
import threading
import time
import os
import re
import json
import logging
import random
from datetime import datetime, timedelta
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv
import streamlit as st

# Çevre değişkenlerini yükle (.env)
load_dotenv()

# Loglama yapılandırması
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ==========================================
# BÖLÜM 1: SABİTLER VE KONFİGÜRASYON
# ==========================================

RSS_FEEDS = [
    ("Anadolu Ajansı (Güncel)", "https://www.aa.com.tr/tr/rss/default?cat=guncel"),
    ("Anadolu Ajansı (Son Dakika)", "https://www.aa.com.tr/tr/rss/default?cat=live"),
    ("TRT Haber", "https://www.trthaber.com/sondakika.rss"),
    ("NTV Gündem", "https://www.ntv.com.tr/gundem.rss"),
    ("NTV Dünya", "https://www.ntv.com.tr/dunya.rss"),
    ("Habertürk", "https://www.haberturk.com/rss/manset.xml"),
    ("Sözcü", "https://www.sozcu.com.tr/feeds-son-dakika"),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("CNN Türk", "https://www.cnnturk.com/feed/rss/all/news"),
    ("Hürriyet", "https://www.hurriyet.com.tr/rss/anasayfa"),
    ("Reuters", "https://www.reutersagency.com/feed/?taxonomy=best-sectors&post_type=best"),
]

SOURCE_RELIABILITY = {
    "Reuters": 1,
    "BBC World": 2,
    "Anadolu Ajansı (Güncel)": 3,
    "Anadolu Ajansı (Son Dakika)": 3,
    "TRT Haber": 4,
    "NTV Gündem": 5,
    "NTV Dünya": 5,
    "Habertürk": 6,
    "CNN Türk": 7,
    "Hürriyet": 8,
    "Sözcü": 9,
}

CRITICAL_KEYWORDS_TR = [
    "son dakika", "deprem", "acil", "savaş", "kriz", "patlama",
    "sel", "tsunami", "saldırı", "bomba", "terör", "ölüm",
    "yangın", "fırtına", "çığ", "heyelan", "olağanüstü hal",
    "seçim sonuçları", "istifa", "darbe", "sıkıyönetim"
]

CRITICAL_KEYWORDS_EN = [
    "breaking", "urgent", "earthquake", "war", "crisis",
    "explosion", "attack", "emergency", "tsunami", "coup"
]

ALL_KEYWORDS = CRITICAL_KEYWORDS_TR + CRITICAL_KEYWORDS_EN

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive",
}

SCAN_INTERVAL_SECONDS = 30          # RSS tarama aralığı (saniye)
UI_REFRESH_SECONDS = 3              # Streamlit UI yenileme aralığı (saniye)
DEDUP_WINDOW_SECONDS = 5            # Çapraz kaynak birleştirme bekleme penceresi (saniye)
MAX_NEWS_DISPLAY = 200              # Web panelinde gösterilecek maksimum haber sayısı
SEEN_SET_MAX_SIZE = 15_000          # Seen set kapasitesi (~1 aylık, ~2.2 MB)
SEEN_SET_FILE = "seen_news.json"    # Disk kayıt dosyası
REQUEST_TIMEOUT_SECONDS = 10        # HTTP istek zaman aşımı (saniye)
TELEGRAM_MAX_RETRY = 3             # Telegram bildirim yeniden deneme sayısı


# ==========================================
# BÖLÜM 2: VERİ MODELİ
# ==========================================

@dataclass
class NewsItem:
    title: str              # Haber başlığı
    link: str               # Haber bağlantısı
    source: str             # Kaynak adı (ör: "TRT Haber")
    published: Optional[str]  # RSS'teki yayın tarihi
    summary: str            # Haber özeti
    is_critical: bool       # Kritik haber mi?
    detected_at: datetime = field(default_factory=datetime.now)
    guid: str = ""          # Mükerrer engelleme için benzersiz ID


@dataclass
class NewsGroup:
    """Aynı haberin farklı kaynaklardan gelen versiyonlarını gruplar."""
    primary: NewsItem           # En güvenilir kaynaktan gelen birincil haber
    sources: list[str] = field(default_factory=list)  # Haberi bildiren tüm kaynaklar
    created_at: datetime = field(default_factory=datetime.now)
    telegram_sent: bool = False  # Telegram bildirimi gönderildi mi?

    @property
    def is_critical(self) -> bool:
        return self.primary.is_critical

    @property
    def source_count(self) -> int:
        return len(self.sources)


# ==========================================
# BÖLÜM 3: SEEN SET — MÜKERRER ENGELLEME (DİSK DESTEKLİ)
# ==========================================

class SeenSet:
    """Thread-safe, GUID/link bazlı mükerrer engelleme mekanizması.
    Disk desteği ile uygulama yeniden başlatıldığında korunur.
    FIFO eviction ile maksimum 15.000 kayıt tutar (~1 ay, ~2.2 MB)."""

    def __init__(self, filepath: str = SEEN_SET_FILE, max_size: int = SEEN_SET_MAX_SIZE):
        self._filepath = Path(filepath)
        self._max_size = max_size
        self._lock = threading.Lock()
        self._seen: set[str] = set()
        self._order: deque[str] = deque(maxlen=max_size)
        self._load_from_disk()

    def _load_from_disk(self):
        """Uygulama başlangıcında disk dosyasından kayıtları yükler."""
        if self._filepath.exists():
            try:
                with open(self._filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for item in data[-self._max_size:]:
                        self._seen.add(item)
                        self._order.append(item)
                logger.info(f"Seen set diskten başarıyla yüklendi: {len(self._seen)} kayıt")
            except Exception as e:
                logger.warning(f"Seen set dosyası okunamadı veya bozuk: {e}")

    def _save_to_disk(self):
        """Mevcut seen set verilerini diske yazar."""
        try:
            with open(self._filepath, "w", encoding="utf-8") as f:
                json.dump(list(self._order), f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Seen set diske kaydedilemedi: {e}")

    def is_new(self, identifier: str) -> bool:
        """True döner ve kaydeder eğer identifier daha önce görülmediyse."""
        if not identifier:
            return False
        with self._lock:
            if identifier in self._seen:
                return False
            self._seen.add(identifier)
            if len(self._order) == self._order.maxlen:
                oldest = self._order[0]
                self._seen.discard(oldest)
            self._order.append(identifier)
            self._save_to_disk()
            return True


# ==========================================
# BÖLÜM 4: ÇAPRAZ KAYNAK BİRLEŞTİRME (CROSS-SOURCE DEDUP)
# ==========================================

class NewsMerger:
    """Aynı haberin farklı kaynaklardan gelen versiyonlarını birleştirir.
    Jaccard benzerliği ile başlık karşılaştırması yapar.
    En güvenilir kaynağı birincil (primary) olarak belirler."""

    SIMILARITY_THRESHOLD = 0.5  # Benzerlik eşiği (0.5 ve üzeri aynı haber kabul edilir)

    def __init__(self):
        self._groups: list[NewsGroup] = []
        self._lock = threading.Lock()

    @staticmethod
    def _normalize_title(title: str) -> set[str]:
        """Başlığı temizler, küçük harfe çevirir, ön ekleri atar ve kelime kümesi döndürür."""
        prefixes_to_remove = [
            "son dakika:", "son dakika -", "son dakika!", "son dakika",
            "flaş:", "flaş!", "flaş", "breaking:", "breaking news:",
            "breaking", "just in:", "urgent:", "flash:"
        ]
        normalized = title.lower().strip()
        for prefix in prefixes_to_remove:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):].strip()
        # Noktalama ve sembolleri kaldır
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        # 2 harften uzun kelimeleri küme yap
        words = {w for w in normalized.split() if len(w) > 2}
        return words

    @staticmethod
    def _jaccard_similarity(set_a: set[str], set_b: set[str]) -> float:
        """İki kelime kümesi arasındaki Jaccard benzerlik katsayısını hesaplar."""
        if not set_a or not set_b:
            return 0.0
        intersection = set_a & set_b
        union = set_a | set_b
        return len(intersection) / len(union)

    def _find_matching_group(self, item: NewsItem) -> Optional[NewsGroup]:
        """Son 30 dakika içinde benzer başlığa sahip mevcut bir grup olup olmadığını kontrol eder."""
        item_words = self._normalize_title(item.title)
        cutoff = datetime.now() - timedelta(minutes=30)
        for group in self._groups:
            if group.created_at < cutoff:
                continue
            group_words = self._normalize_title(group.primary.title)
            similarity = self._jaccard_similarity(item_words, group_words)
            if similarity >= self.SIMILARITY_THRESHOLD:
                return group
        return None

    def add_news(self, item: NewsItem) -> Optional[NewsGroup]:
        """Yeni haberi ekler. Benzer haber varsa gruba dahil eder, yoksa yeni grup oluşturur."""
        with self._lock:
            existing_group = self._find_matching_group(item)
            if existing_group:
                # Kaynağı listeye ekle
                if item.source not in existing_group.sources:
                    existing_group.sources.append(item.source)
                # Yeni kaynak daha güvenilirse birincil haberi güncelle
                current_rank = SOURCE_RELIABILITY.get(existing_group.primary.source, 99)
                new_rank = SOURCE_RELIABILITY.get(item.source, 99)
                if new_rank < current_rank:
                    existing_group.primary = item
                return None
            else:
                group = NewsGroup(
                    primary=item,
                    sources=[item.source],
                    created_at=datetime.now()
                )
                self._groups.append(group)
                return group

    def get_pending_groups(self) -> list[NewsGroup]:
        """5 saniyelik bekleme penceresi dolmuş, henüz Telegram bildirimi atılmamış grupları döndürür."""
        with self._lock:
            cutoff = datetime.now() - timedelta(seconds=DEDUP_WINDOW_SECONDS)
            ready = []
            for group in self._groups:
                if not group.telegram_sent and group.created_at <= cutoff:
                    ready.append(group)
            return ready

    def mark_sent(self, group: NewsGroup):
        """Grubun Telegram bildiriminin başarıyla gönderildiğini işaretler."""
        with self._lock:
            group.telegram_sent = True

    def get_all_groups(self) -> list[NewsGroup]:
        """Tüm grupları en yeniden en eskiye sıralayarak döndürür."""
        with self._lock:
            return sorted(self._groups, key=lambda g: g.created_at, reverse=True)[:MAX_NEWS_DISPLAY]

    def cleanup_old_groups(self):
        """1 saatten eski grupları bellekten temizler."""
        with self._lock:
            cutoff = datetime.now() - timedelta(hours=1)
            self._groups = [g for g in self._groups if g.created_at >= cutoff]


# ==========================================
# BÖLÜM 5: ASENKRON RSS TARAYICI
# ==========================================

class AsyncNewsScanner:
    """aiohttp ile eşzamanlı RSS tarama motoru.
    ETag ve If-Modified-Since desteği ile gereksiz veri transferini engeller."""

    def __init__(self):
        self._etags: dict[str, str] = {}
        self._last_modified: dict[str, str] = {}

    async def fetch_feed(self, session: aiohttp.ClientSession, 
                         feed_url: str, source_name: str) -> list[NewsItem]:
        """Tek bir RSS kaynağını asenkron olarak çeker ve parse eder."""
        items = []
        try:
            # Sitelerin eşzamanlı yük altında engelleme yapmaması için rastgele 0-0.3s jitter
            await asyncio.sleep(random.uniform(0, 0.3))

            headers = {}
            if feed_url in self._etags:
                headers["If-None-Match"] = self._etags[feed_url]
            if feed_url in self._last_modified:
                headers["If-Modified-Since"] = self._last_modified[feed_url]

            async with session.get(feed_url, headers=headers, 
                                   timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)) as response:
                
                # 304 Not Modified: İçerik değişmedi, ağ bant genişliği tasarrufu
                if response.status == 304:
                    logger.debug(f"304 Not Modified: {source_name}")
                    return []
                
                # 429 Too Many Requests
                if response.status == 429:
                    logger.warning(f"429 Rate Limited: {source_name}")
                    return []
                
                if response.status >= 400:
                    logger.warning(f"HTTP {response.status} Hatası: {source_name}")
                    return []

                # ETag ve Last-Modified değerlerini güncelle
                if "ETag" in response.headers:
                    self._etags[feed_url] = response.headers["ETag"]
                if "Last-Modified" in response.headers:
                    self._last_modified[feed_url] = response.headers["Last-Modified"]

                content = await response.text()
                feed = feedparser.parse(content)

                for entry in feed.entries:
                    title = entry.get("title", "").strip()
                    link = entry.get("link", "").strip()
                    guid = entry.get("id", link)  # id yoksa link kullan
                    summary = entry.get("summary", entry.get("description", "")).strip()
                    
                    # HTML etiketlerini temizle
                    summary = re.sub(r'<[^>]+>', '', summary)
                    published = entry.get("published", entry.get("updated", ""))
                    
                    if not title or not link:
                        continue

                    is_critical = self.classify(title, summary)

                    items.append(NewsItem(
                        title=title,
                        link=link,
                        source=source_name,
                        published=published,
                        summary=summary[:300],
                        is_critical=is_critical,
                        guid=guid,
                    ))

        except asyncio.TimeoutError:
            logger.warning(f"Zaman aşımı (Timeout): {source_name}")
        except Exception as e:
            logger.error(f"RSS Ayrıştırma hatası ({source_name}): {e}")

        return items

    async def scan_all_feeds(self) -> list[NewsItem]:
        """Tüm kaynakları asyncio.gather ile eşzamanlı olarak tarar."""
        connector = aiohttp.TCPConnector(limit=15, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(
            headers=REQUEST_HEADERS, connector=connector, timeout=timeout
        ) as session:
            tasks = [
                self.fetch_feed(session, url, name) 
                for name, url in RSS_FEEDS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            all_items = []
            for i, result in enumerate(results):
                if isinstance(result, list):
                    all_items.extend(result)
                elif isinstance(result, Exception):
                    logger.warning(f"Feed istisnası ({RSS_FEEDS[i][0]}): {result}")
            
            return all_items

    @staticmethod
    def classify(title: str, summary: str) -> bool:
        """Haber başlığı ve özetinde kritik anahtar kelimeleri arar."""
        text = f"{title} {summary}".lower()
        return any(kw in text for kw in ALL_KEYWORDS)


# ==========================================
# BÖLÜM 6: TELEGRAM BİLDİRİM SERVİSİ
# ==========================================

class TelegramNotifier:
    """Telegram Bot API üzerinden Türkçe anlık bildirim gönderimi.
    Token veya Chat ID belirtilmemişse sessizce devre dışı kalır."""

    API_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self):
        # Çevre değişkenlerinden (.env) veya Streamlit Cloud Secrets üzerinden oku
        token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        try:
            if not token and hasattr(st, "secrets") and "TELEGRAM_BOT_TOKEN" in st.secrets:
                token = st.secrets["TELEGRAM_BOT_TOKEN"]
            if not chat_id and hasattr(st, "secrets") and "TELEGRAM_CHAT_ID" in st.secrets:
                chat_id = str(st.secrets["TELEGRAM_CHAT_ID"])
        except Exception:
            pass

        self.token = str(token).strip()
        self.chat_id = str(chat_id).strip()
        self.enabled = bool(self.token and self.chat_id and self.token != "your_bot_token_here")

    def send_alert(self, group: NewsGroup) -> bool:
        """Kritik haber grubu için Telegram bildirimi gönderir.
        Birincil kaynak ve doğrulayan diğer kaynakları dipnot olarak ekler."""
        if not self.enabled:
            return False

        primary = group.primary
        
        # Güvenilir birincil kaynak ve diğer doğrulayan kaynaklar
        source_text = f"📌 Kaynak: {primary.source}"
        if len(group.sources) > 1:
            other_sources = [s for s in group.sources if s != primary.source]
            source_text += f"\n📎 Diğer kaynaklar: {', '.join(other_sources)}"

        # Markdown V1 biçiminde mesaj metni
        clean_title = primary.title.replace("*", "").replace("_", "")
        message = (
            f"🚨 *KRİTİK HABER*\n\n"
            f"📰 *{clean_title}*\n\n"
            f"{source_text}\n"
            f"⏰ {primary.detected_at.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"🔗 [Haberin Tamamı]({primary.link})"
        )

        url = self.API_URL.format(token=self.token)
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        }

        for attempt in range(TELEGRAM_MAX_RETRY):
            try:
                response = requests.post(url, json=payload, timeout=10)
                if response.status_code == 200:
                    logger.info(f"Telegram bildirimi iletildi: {primary.title[:50]}...")
                    return True
                else:
                    logger.warning(f"Telegram API hatası (deneme {attempt+1}): {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Telegram bağlantı istisnası (deneme {attempt+1}): {e}")

            if attempt < TELEGRAM_MAX_RETRY - 1:
                time.sleep(1)

        return False


# ==========================================
# BÖLÜM 7: ARKA PLAN ORKESTRATÖRÜ
# ==========================================

class BackgroundOrchestrator:
    """Arka plan iş parçacığında (thread) asyncio döngüsünü yönetir.
    RSS taramayı 30 saniyede bir çalıştırır.
    Çapraz kaynak birleştirme ve 5 saniyelik bekleme penceresini koordine eder."""

    def __init__(self, scanner: AsyncNewsScanner, notifier: TelegramNotifier,
                 seen_set: SeenSet, merger: NewsMerger, stats: dict):
        self.scanner = scanner
        self.notifier = notifier
        self.seen_set = seen_set
        self.merger = merger
        self.stats = stats
        self.running = True

    async def _check_pending_notifications(self):
        """5 saniyelik bekleme penceresi dolmuş kritik haber gruplarını Telegram'a gönderir."""
        pending = self.merger.get_pending_groups()
        for group in pending:
            if group.is_critical and not group.telegram_sent:
                if self.notifier.send_alert(group):
                    self.stats["telegram_sent"] += 1
                self.merger.mark_sent(group)

    async def _run_loop(self):
        """Ana arka plan tarama döngüsü."""
        last_scan = 0
        
        while self.running:
            now = asyncio.get_event_loop().time()
            
            # 30 saniyede bir RSS taraması
            if now - last_scan >= SCAN_INTERVAL_SECONDS:
                last_scan = now
                try:
                    new_items = await self.scanner.scan_all_feeds()
                    for item in new_items:
                        if self.seen_set.is_new(item.guid):
                            self.stats["total_scanned"] += 1
                            self.merger.add_news(item)
                            if item.is_critical:
                                self.stats["critical_found"] += 1
                except Exception as e:
                    logger.error(f"Tarama döngüsü beklenmeyen hata: {e}")

            # Bekleme penceresi dolan bildirimleri kontrol et
            await self._check_pending_notifications()
            
            # Eski haber gruplarını temizle
            self.merger.cleanup_old_groups()

            # 1 saniye bekle
            await asyncio.sleep(1)

    def start_in_thread(self):
        """Yeni bir daemon thread içerisinde asyncio event loop başlatır."""
        def _thread_target():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._run_loop())
            except Exception as e:
                logger.error(f"Arka plan thread hatası: {e}")

        thread = threading.Thread(target=_thread_target, daemon=True, name="news-scanner")
        thread.start()
        logger.info("Arka plan haber tarama thread'i başarıyla başlatıldı.")
        return thread


# ==========================================
# BÖLÜM 8: STREAMLIT CANLI WEB PANELİ
# ==========================================

@st.cache_resource
def get_shared_state():
    """Tüm Streamlit rerun döngülerinde korunan tekil (singleton) durum.
    cache_resource sayesinde sadece ilk yüklemede bir kez oluşturulur."""
    scanner = AsyncNewsScanner()
    notifier = TelegramNotifier()
    seen_set = SeenSet()
    merger = NewsMerger()
    stats = {"total_scanned": 0, "critical_found": 0, "telegram_sent": 0}

    orchestrator = BackgroundOrchestrator(scanner, notifier, seen_set, merger, stats)
    orchestrator.start_in_thread()

    return {
        "merger": merger,
        "stats": stats,
        "notifier": notifier,
        "seen_set": seen_set,
    }


def main():
    st.set_page_config(
        page_title="📡 InstantNewsTracker",
        page_icon="📡",
        layout="wide",
    )

    state = get_shared_state()
    merger = state["merger"]
    stats = state["stats"]
    notifier = state["notifier"]

    # ============ YAN PANEL (SIDEBAR) ============
    with st.sidebar:
        st.header("📡 InstantNewsTracker")
        st.caption("Türkiye ve Dünya Gündemi Takip Sistemi")
        
        st.divider()

        # Telegram entegrasyon durumu
        if notifier.enabled:
            st.success("🟢 Telegram: Bağlı")
        else:
            st.error("🔴 Telegram: Yapılandırılmamış")
            st.caption("`.env` dosyasına geçerli TELEGRAM_BOT_TOKEN ve TELEGRAM_CHAT_ID ekleyin.")

        st.divider()

        # Canlı metrikler
        st.subheader("📊 Canlı İstatistikler")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Taranan", stats["total_scanned"])
            st.metric("Kritik", stats["critical_found"])
        with col2:
            st.metric("Telegram", stats["telegram_sent"])
            st.metric("Kaynaklar", len(RSS_FEEDS))

        st.divider()

        # Aktif RSS Kaynakları listesi
        st.subheader("📋 Aktif Kaynaklar")
        for name, _ in RSS_FEEDS:
            st.caption(f"• {name}")

        st.divider()
        st.caption(f"⏱️ RSS tarama: {SCAN_INTERVAL_SECONDS}s | UI yenileme: {UI_REFRESH_SECONDS}s")

    # ============ ANA PANEL ============
    st.title("📡 InstantNewsTracker")
    st.caption("Türkiye ve Dünya gündemini anlık takip edin • Kritik haberler anında Telegram'a bildirilir")

    # Canlı haber akışı — st.fragment ile sadece bu blok her 3 saniyede yenilenir
    @st.fragment(run_every=UI_REFRESH_SECONDS)
    def live_news_feed():
        groups = merger.get_all_groups()

        if not groups:
            st.info("⏳ Haberler taranıyor... İlk akış 30 saniye içinde burada listelenecektir.")
            return

        for group in groups:
            primary = group.primary
            
            # Kaynak metni (birincil + diğer doğrulayan kaynaklar)
            source_line = f"📌 **{primary.source}**"
            if group.source_count > 1:
                other = [s for s in group.sources if s != primary.source]
                source_line += f" + 📎 {', '.join(other)}"

            time_str = primary.detected_at.strftime("%H:%M:%S")

            if group.is_critical:
                st.error(
                    f"🚨 **{primary.title}**\n\n"
                    f"{source_line} | ⏰ {time_str}\n\n"
                    f"🔗 [Haberi Oku]({primary.link})"
                )
            else:
                st.info(
                    f"📰 **{primary.title}**\n\n"
                    f"{source_line} | ⏰ {time_str}\n\n"
                    f"🔗 [Haberi Oku]({primary.link})"
                )

    live_news_feed()


if __name__ == "__main__":
    main()
