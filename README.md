# Real-Time Fraud Detection ML Platform

Uçtan uca, ölçeklenebilir bir makine öğrenmesi platformu: gerçek zamanlı işlem
verisinin alınmasından (ingestion), özellik mühendisliğine, model
versiyonlamaya ve ölçeklenebilir servis edilmeye kadar tüm boru hattını kapsar.

Bu proje, önceki [network security monitor](#) projesinin doğal devamı olarak
tasarlandı: burada da temel problem "anormal davranışı gerçek zamanlı
yakalamak", ancak bu kez odak tek bir tespit algoritması değil, **production
kalitesinde, ölçeklenen bir sistem mimarisi** kurmak.

> **Durum:** 🚧 Aktif geliştirme — Faz 1/6 tamamlandı (bkz. [Yol Haritası](#yol-haritası))

---

## Neden Bu Proje?

Çoğu "fraud detection" projesi statik bir CSV üzerinde model eğitip
%95 accuracy raporlamakla sınırlı kalıyor. Bu proje bilinçli olarak farklı bir
soruyu merkeze alıyor: **"Bu model production'da, saniyede yüzlerce işlemle,
nasıl güvenilir şekilde çalışır?"**

Bu yüzden model doğruluğu değil, aşağıdakiler önceliklidir:
- Gerçek zamanlı veri akışı (Kafka)
- Tutarlı özellik yönetimi (feature store)
- Model versiyonlama ve tekrar üretilebilirlik (MLflow)
- Yatay ölçeklenebilirlik (Kubernetes)
- Sistem ve model sağlığının izlenebilirliği (Prometheus/Grafana)
- Otomatik test ve deploy (CI/CD)

---

## Mimari

```
┌─────────────┐     ┌───────┐     ┌──────────────────┐     ┌───────────────┐
│  Transaction │────▶│ Kafka │────▶│ Stream Processor  │────▶│ Feature Store │
│  Producer    │     │       │     │ (feature eng.)    │     │ (Redis)       │
└─────────────┘     └───────┘     └──────────────────┘     └───────┬───────┘
                                                                     │
                                                                     ▼
┌───────────────┐     ┌──────────────┐     ┌──────────────────────────┐
│  Client/API   │────▶│ FastAPI      │◀────│ Model Registry (MLflow)  │
│  Request      │     │ Serving      │     └──────────────────────────┘
└───────────────┘     └──────┬───────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Prometheus/Grafana │  (tüm katmanları izler)
                    └───────────────────┘

Tüm servisler Docker + Kubernetes üzerinde çalışır.
CI/CD (GitHub Actions) her commit'te test + deploy tetikler.
```

---

## Proje Yapısı

```
fraud-detection-ml-platform/
├── infra/              # docker-compose, k8s manifest'leri
├── producer/           # Kafka'ya sentetik işlem verisi üreten servis
├── processor/          # Stream processing + feature engineering (Faz 2)
├── serving/            # FastAPI model serving katmanı (Faz 4)
├── notebooks/          # Model eğitimi / deney defterleri (Faz 3)
├── docs/               # Yol haritası, mimari notları, ölçüm sonuçları
└── README.md
```

---

## Yol Haritası

| Faz | Konu | Durum |
|---|---|---|
| 1 | Veri akışı — sentetik işlem üreteci + Kafka | ✅ Tamamlandı |
| 2 | Stream processing + feature store (Redis) | ⬜ Planlandı |
| 3 | Model eğitimi + versiyonlama (MLflow) | ⬜ Planlandı |
| 4 | FastAPI serving + Kubernetes ölçekleme + load test | ⬜ Planlandı |
| 5 | İzlenebilirlik (Prometheus/Grafana) + model drift tespiti | ⬜ Planlandı |
| 6 | CI/CD + dokümantasyon cilalama | ⬜ Planlandı |

Detaylı haftalık plan için: [`docs/roadmap.md`](docs/roadmap.md)

---

## Faz 1: Veri Akışı (Şu An Burada)

**Ne yapıyor:** `producer/transaction_producer.py`, Faker kütüphanesiyle
gerçekçi işlem verisi üretir (kullanıcı, tutar, konum, cihaz, zaman damgası)
ve bunu sürekli Kafka'nın `transactions` topic'ine yazar. Verinin ~%3'üne
bilinçli olarak iki fraud pattern'i gömülür:

- **`high_amount`** — kullanıcının normal davranışına göre anormal yüksek tutar
- **`impossible_velocity`** — kısa sürede farklı coğrafi konumdan işlem (fiziksel olarak imkansız hız)

### Kurulum ve Çalıştırma

```bash
# 1. Kafka + Redis + Kafka UI'ı ayağa kaldır
cd infra
docker compose up -d

# 2. Python bağımlılıklarını kur
cd ../producer
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Üreteci başlat
python transaction_producer.py
```

Kafka UI üzerinden akan veriyi canlı izleyebilirsin: **http://localhost:8080**

### Doğrulama

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning
```

---

## Kullanılan Teknolojiler

| Katman | Teknoloji |
|---|---|
| Mesajlaşma | Apache Kafka |
| Feature Store | Redis |
| Model Yönetimi | MLflow |
| Serving | FastAPI |
| Konteynerleştirme | Docker |
| Orkestrasyon | Kubernetes |
| İzleme | Prometheus + Grafana |
| CI/CD | GitHub Actions |
| Yük Testi | Locust |

---

## Ölçüm Sonuçları

*(Faz 4 tamamlandığında buraya gerçek throughput/latency sayıları eklenecek.)*

---

## Lisans

MIT
