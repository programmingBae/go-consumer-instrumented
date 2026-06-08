# Solace Go — Distributed Tracing Implementation Guide

## Struktur Project

```
solace-dt/
├── go.mod
├── otel-collector-config.yaml
├── publisher/
│   └── main.go                  # Publisher tanpa tracing
├── consumer/
│   └── main.go                  # Consumer tanpa tracing
└── consumer_instrumented/
    └── main.go                  # Consumer WITH distributed tracing ⭐
```

---

## 0. Setup Awal (Clone & Config)

```bash
# Clone repo
git clone https://github.com/programmingBae/go-consumer-instrumented.git
cd go-consumer-instrumented

# Salin file contoh jadi file asli, lalu isi credential lo
cp .env.example .env
cp otel-collector-config.example.yaml otel-collector-config.yaml

# Install dependency
go mod download
```

> ⚠️ **Secrets / Keamanan**
> File `.env` dan `otel-collector-config.yaml` berisi kredensial (broker, user telemetry, token Grafana Tempo) dan **di-ignore oleh git** — jangan pernah di-commit. Yang masuk repo hanya versi `*.example`. Kalau credential pernah ter-push, segera **rotate/ganti** token & password tersebut.

---

## 1. Cara Cek Versi Solace Go API

### A. Cek `go.mod` (paling umum)
```bash
cat go.mod | grep solace
# Output: solace.dev/go/messaging v1.10.0
```

### B. Via `go list`
```bash
go list -m solace.dev/go/messaging
# Output: solace.dev/go/messaging v1.10.0
```

### C. Cek `go.sum`
```bash
grep "solace.dev/go/messaging " go.sum | head -1
```

### D. Dari binary yang sudah di-build
```bash
go version -m ./nama-binary | grep solace
```

---

## 2. Prasyarat

### Di Solace Broker
Buat queue yang subscribe ke topic:
- Queue name: `demo-queue`
- Topic subscription: `demo/tracing/topic`

Untuk distributed tracing dari broker, pastikan:
- **Telemetry Profile** sudah dikonfigurasi di VPN
- **Receiver queue** `#telemetry-trace` sudah dibuat
- User `trace` dengan password `trace` sudah ada

### Dependencies
```bash
go get solace.dev/go/messaging@v1.10.0
go get solace.dev/go/messaging-trace/opentelemetry@v1.10.0
go get go.opentelemetry.io/otel@v1.21.0
go get go.opentelemetry.io/otel/sdk@v1.21.0
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v0.44.0
go get go.opentelemetry.io/otel/exporters/stdout/stdouttrace@v0.44.0
```

---

## 3. Cara Jalankan

```bash
# Terminal 1: OTEL Collector
docker run --rm \
  -p 4317:4317 -p 4318:4318 \
  -v $(pwd)/otel-collector-config.yaml:/etc/otelcol/config.yaml \
  otel/opentelemetry-collector-contrib:latest

# Terminal 2: Consumer biasa (tanpa tracing)
cd consumer && go run main.go

# Terminal 3: Consumer dengan tracing ⭐
cd consumer_instrumented && go run main.go

# Terminal 4: Publisher (kirim pesan)
cd publisher && go run main.go
```

---

## 4. Penjelasan Tracing Steps (Consumer)

### Overview Alur
```
[Solace Broker]
      │  pesan berisi "traceparent" header
      │  (trace context dari broker span)
      ▼
[Consumer App]
  Step 3: NewInboundMessageCarrier(msg)   ← baca traceparent dari pesan
  Step 4: Extract(ctx, carrier)           ← ambil SpanContext dari traceparent
  Step 5: Define attributes               ← metadata untuk span
  Step 6: tracer.Start(parentCtx, ...)   ← buat child span (TraceID sama!)
  Step 7: defer span.End()               ← tutup span setelah handler selesai
      │
      ▼
[OTEL Collector :4318]
      │
      ▼
[Grafana Tempo]  ← trace broker + consumer terhubung via TraceID yang sama
```

### Step-by-step Detail

| Step | Kode | Tujuan |
|------|------|--------|
| 1 | `sdktrace.NewTracerProvider(...)` | Factory untuk Tracer + konfigurasi exporter & sampler |
| 2 | `otel.SetTracerProvider(tp)` + `otel.SetTextMapPropagator(...)` | Register global: semua kode di app pakai setting ini |
| 3 | `solaceotel.NewInboundMessageCarrier(msg)` | Bridge antara Solace message dan OTel propagation interface |
| 4 | `otel.GetTextMapPropagator().Extract(ctx, carrier)` | Baca `traceparent` dari pesan → dapatkan SpanContext upstream |
| 5 | `[]attribute.KeyValue{...}` | Metadata span: system, topic, operation, dll |
| 6 | `tracer.Start(parentCtx, ...)` | Buat span baru sebagai CHILD dari upstream (TraceID sama) |
| 7 | `defer span.End()` | Tutup span, flush ke exporter, trigger ACK ke broker |

### Kenapa `defer span.End()` Penting?

```go
// ✅ BENAR — pakai defer
_, span := tracer.Start(parentCtx, "topic receive", opts...)
defer span.End()
// ... proses pesan ...
// span.End() dipanggil otomatis saat handler return, bahkan kalau panic

// ❌ SALAH — tidak pakai defer
_, span := tracer.Start(parentCtx, "topic receive", opts...)
processMessage(msg)
span.End() // TIDAK dipanggil kalau processMessage() panic!
```

### Koneksi TraceID antara Broker dan Consumer

```
Broker span:   TraceID=abc123, SpanID=111, ParentSpanID=nil  (root)
Consumer span: TraceID=abc123, SpanID=222, ParentSpanID=111  (child)
                      ↑
               TraceID SAMA → Grafana Tempo bisa link keduanya!
```

---

## 5. OTEL Collector Config

Config `otel-collector-config.yaml` ini sudah dikonfigurasi dengan dua pipeline:

| Pipeline | Receiver | Tujuan |
|----------|----------|--------|
| `traces/solace` | `solace` (port 5674) | Span dari Solace broker → Tempo |
| `traces/app` | `otlp` (port 4318) | Span dari aplikasi Go → Tempo |

Kedua pipeline export ke Grafana Tempo yang sama, sehingga span dari broker dan consumer bisa terhubung via TraceID di UI Grafana.

---

## 6. Troubleshooting

### Span tidak muncul di Grafana Tempo
1. Pastikan OTEL Collector running dan port 4318 bisa diakses
2. Cek log collector untuk error koneksi ke Tempo
3. Verifikasi `otelCollectorEndpoint` di kode sudah benar
4. Aktifkan stdout exporter untuk debug lokal (sudah ada di kode)

### TraceID consumer tidak terhubung ke span broker
1. Pastikan Telemetry Profile di Solace broker sudah aktif
2. Pastikan pesan dari broker memiliki `traceparent` property
3. Cek log consumer: `IsRemote: true` artinya SpanContext berhasil di-extract

### `span.SpanContext().IsRemote()` selalu `false`
Artinya Extract() tidak berhasil menemukan trace context di pesan.
Kemungkinan penyebab:
- Broker belum dikonfigurasi untuk telemetry
- Publisher mengirim pesan tanpa inject trace context
- Format propagator tidak cocok (pastikan pakai W3C TraceContext)
