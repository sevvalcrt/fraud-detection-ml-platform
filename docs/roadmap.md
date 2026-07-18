# Uçtan Uca Ölçeklenebilir ML Platformu — Proje Yol Haritası

**Senaryo:** Gerçek zamanlı işlem verisi üzerinden sahtekarlık (fraud) tespiti
**Tema:** Dağıtık sistemler + veri mühendisliği + ölçeklenebilirlik + gözlemlenebilirlik
**Tahmini süre:** ~9 hafta (haftada 10-12 saat varsayımıyla)
**Önceki proje ile bağlantı:** Network security monitor'de "anomali tespiti" yapmıştın; burada aynı fikri (anormal davranışı yakalama) çok daha büyük ve üretim kalitesinde bir sistemde uyguluyorsun.

> Not: Süre veya kapsam sana uymuyorsa haftaları birleştirip sıkıştırmak ya da genişletmek kolay — yapı modüler.

---

## Genel Mimari

```
[Olay Üreteci] → [Kafka] → [Stream Processor] → [Feature Store]
                                                        ↓
                                              [Model Eğitim + MLflow]
                                                        ↓
[İstemci] → [FastAPI Serving] ← [Model Registry]
        ↓
[Prometheus + Grafana] (tüm katmanları izler)
        ↓
[Docker + Kubernetes] (tüm katmanları barındırır)
        ↓
[CI/CD Pipeline] (kod/model her değiştiğinde otomatik test + deploy)
```

Her katman ayrı bir hafta/hedef olarak ilerleyecek. Amaç: her aşamanın sonunda **çalışan, gösterilebilir bir demo** olması — bu hem motivasyonu korur hem de CV/mülakat için ara kilometre taşları verir.

---

## Faz 1: Temel ve Veri Akışı (Hafta 1-2)

**Hedef:** Gerçekçi, sürekli akan işlem verisi üreten ve Kafka'ya yazan bir sistem kurmak.

- Kafka'yı Docker Compose ile ayağa kaldır (tek broker yeterli, cluster'a gerek yok başta)
- Sahte ama gerçekçi işlem verisi üreten bir script yaz (Python + Faker kütüphanesi): kullanıcı ID, tutar, konum, cihaz, zaman damgası
- İçine kasıtlı olarak "fraud pattern"leri gömüle: aynı kullanıcının kısa sürede farklı ülkelerden işlem yapması, anormal yüksek tutar, gece yarısı ani aktivite artışı gibi
- Bu veriyi sürekli Kafka topic'ine akıt (örn. saniyede 5-10 işlem)

**Öğrenilecek kavramlar:** Kafka producer/consumer, topic/partition mantığı, event-driven mimari

**Kilometre taşı:** `kafka-console-consumer` ile topic'e akan veriyi canlı izleyebiliyor olman.

---

## Faz 2: Stream Processing ve Feature Engineering (Hafta 3-4)

**Hedef:** Ham işlem verisini modelin anlayacağı özelliklere (feature) dönüştürmek.

- Kafka'dan veriyi okuyan bir consumer/processor yaz (başta basit Python consumer yeterli, istersen sonra Spark Structured Streaming'e geç)
- Anlamlı feature'lar türet: kullanıcının son 1 saatteki işlem sayısı, ortalama tutardan sapma, son işlemle bu işlem arası mesafe/süre oranı (imkansız hız tespiti)
- Bu feature'ları basit bir "feature store"da tut (başta Redis yeterli — hız kritik olduğu için gerçek sistemlerde de böyle yapılır)

**Öğrenilecek kavramlar:** Stream processing, sliding window hesaplamaları, feature store kavramı, neden batch değil stream gerektiği

**Kilometre taşı:** Kafka'ya bir işlem geldiğinde, o kullanıcı için feature'ların Redis'te otomatik güncellendiğini gösterebiliyor olman.

---

## Faz 3: Model Eğitimi ve Versiyonlama (Hafta 5)

**Hedef:** Bir sahtekarlık tespit modeli eğitmek — ama asıl odak modelin kendisi değil, **nasıl yönetildiği**.

- Ürettiğin sentetik veriyle basit bir sınıflandırma modeli eğit (XGBoost veya Random Forest — burada karmaşık bir model şart değil)
- MLflow kur: her eğitim denemesini (hyperparameter, metrik, veri versiyonu) otomatik logla
- Model registry mantığını kur: "hangi model versiyonu şu an production'da" sorusuna cevap verebilen bir sistem

**Öğrenilecek kavramlar:** Deney takibi (experiment tracking), model versiyonlama, reproducibility (aynı sonucu tekrar üretebilme)

**Kilometre taşı:** MLflow arayüzünde birden fazla model denemesini yan yana karşılaştırabiliyor olman.

**Not:** Bu proje bir "ML araştırma projesi" değil, "ML mühendisliği projesi" — model doğruluğu %95 mi %89 mu çok önemli değil, sistemin bunu nasıl yönettiği önemli.

---

## Faz 4: Serving ve Ölçeklenebilirlik (Hafta 6-7)

**Hedef:** Modeli gerçek zamanlı tahmin yapan, yatayda ölçeklenen bir API haline getirmek.

- FastAPI ile modeli bir REST/gRPC endpoint arkasında sun (`/predict` gibi)
- Endpoint'i Dockerize et
- Kubernetes'e deploy et (yerelde Minikube veya Kind yeterli, bulut gerekmez)
- **Yatay ölçekleme testi yap:** Aynı anda 2, sonra 5, sonra 10 pod ile sistemin nasıl davrandığını ölç
- Load testing (Locust veya k6) ile sisteme yük bindir, saniyede kaç tahmin işleyebildiğini ölç

**Öğrenilecek kavramlar:** Konteynerleştirme, orkestrasyon, yatay ölçekleme, load balancing, latency vs throughput trade-off'u

**Kilometre taşı:** "Sistemim saniyede X tahmin yapabiliyor, Y pod ile bu Z'ye çıkıyor" gibi **somut, ölçülmüş bir sayı** elde etmen. Bu CV'de en güçlü cümlelerden biri olacak.

---

## Faz 5: Gözlemlenebilirlik (Hafta 8)

**Hedef:** Sistemi "kara kutu" olmaktan çıkarıp izlenebilir hale getirmek.

- Prometheus ile sistem metriklerini topla: latency, hata oranı, throughput
- **Model drift** metriği ekle: zamanla gelen verinin dağılımı değiştiğinde (örn. fraud pattern'leri değiştiğinde) bunu tespit eden bir mekanizma — bu, gerçek ML platformlarının en zor ve en değerli parçalarından biri
- Grafana'da canlı dashboard kur: hem sistem sağlığını hem model performansını tek ekranda göster

**Öğrenilecek kavramlar:** Observability üçlüsü (metrics/logs/traces), model drift kavramı, alerting

**Kilometre taşı:** Dashboard'a bakarak "sistem şu an sağlıklı mı, model kötüleşiyor mu" sorusuna saniyeler içinde cevap verebiliyor olman.

---

## Faz 6: CI/CD ve Cilalama (Hafta 9)

**Hedef:** Sistemi "elle çalıştırılan demo"dan "gerçek yazılım mühendisliği ürünü"ne taşımak.

- GitHub Actions ile: kod push edildiğinde otomatik test çalıştır, yeni model versiyonu geldiğinde otomatik deploy pipeline'ı kur
- README'yi profesyonelleştir: mimari diyagramı, kurulum adımları, ölçüm sonuçları (Faz 4'teki sayılar), ekran görüntüleri/GIF
- Kısa bir demo videosu/GIF çek (mülakatlarda ve LinkedIn'de paylaşmak için altın değerinde)

**Kilometre taşı:** Bir yabancının repo'yu klonlayıp README'yi takip ederek sistemi kendi bilgisayarında ayağa kaldırabilmesi.

---

## Kritik Tavsiyeler

1. **Her fazın sonunda dur ve ölç.** "Çalışıyor" yetmez, "şu sayıda saniyede şu kadar işlem işliyor" gibi somut metrikler CV ve mülakatta seni diğer adaylardan ayırır.
2. **Basitten başla, karmaşıklaştır.** Spark yerine önce basit Python consumer, sonra gerek görürsen Spark'a geç. Erken karmaşıklık, projeyi bitirmemenin en büyük sebebi.
3. **Zorlandığın yerleri not al.** Mülakatlarda en çok sorulan soru "en çok nerede zorlandın, nasıl çözdün" — bu notlar altın değerinde olacak.
4. **Haftalık commit disiplini kur.** Küçük ve sık commit'ler, GitHub geçmişinin de bir CV parçası olduğunu unutma.

## CV / LinkedIn için Taslak Cümle

*"Designed and built an end-to-end real-time fraud detection platform processing streaming transaction data via Kafka, with feature engineering, model versioning (MLflow), and horizontally scalable serving on Kubernetes — achieving [X] predictions/sec across [Y] replicas, with full observability via Prometheus/Grafana and automated CI/CD deployment."*

(Köşeli parantezleri Faz 4'teki gerçek ölçümlerinle dolduracaksın.)

---

## Teknoloji Özeti

| Katman | Teknoloji | Alternatif (daha hafif) |
|---|---|---|
| Mesajlaşma | Apache Kafka | Redis Streams |
| İşleme | Spark Structured Streaming | Python consumer + threading |
| Feature Store | Redis | PostgreSQL |
| Model Yönetimi | MLflow | Weights & Biases |
| Serving | FastAPI | Flask |
| Konteynerleştirme | Docker | — |
| Orkestrasyon | Kubernetes (Minikube/Kind) | Docker Compose (daha basit ama daha az etkileyici) |
| İzleme | Prometheus + Grafana | — |
| CI/CD | GitHub Actions | GitLab CI |
| Yük testi | Locust / k6 | — |
