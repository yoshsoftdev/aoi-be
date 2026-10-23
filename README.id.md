# AOI Backend

[English](README.md) · [Bahasa Indonesia](README.id.md)

API HTTP dan WebSocket untuk sistem akuisisi data multi-sumber AOI. Proses ini mem-polling perangkat lapangan, menghitung formula, menjalankan workflow dan bypass, lalu mengirim nilai live ke dashboard.

Frontend dilayani terpisah (Nginx di port 80 saat produksi, Vite saat development). Proses ini hanya melayani API.

## Fitur

- Koneksi perangkat: Modbus RTU, Modbus TCP, SNMP, dan HTTP
- Polling parameter. Modbus pada satu koneksi dijalankan bergiliran (round-robin)
- Parameter turunan lewat formula engine yang reaktif
- Workflow engine (aturan datar dan pohon) untuk menulis register, menunggu, dan memunculkan alarm
- Aturan bypass yang membandingkan lalu menulis set-point atau parameter
- Set-point yang diatur dari UI, termasuk jalan acak di dalam batas min/max
- Ambang batas beserta status alarm dan riwayat pelanggaran
- Pembaruan live lewat WebSocket
- Log audit penulisan di database SQLite terpisah
- Pemetaan kartu IMC ke parameter atau set-point

## Kebutuhan

- Go 1.21 atau lebih baru
- Compiler C (`CGO` dipakai driver SQLite)
- Opsional: Redis, jika `REDIS_ENABLED=true`

## Menjalankan

Dari direktori ini:

```bash
go run ./cmd/server
```

Server mendengar di `0.0.0.0:8071`. Berkas SQLite dibuat di `data/` pada start pertama. Skema diterapkan dengan GORM AutoMigrate, ditambah beberapa alter kompatibilitas di `cmd/server/main.go`. Berkas SQL di `migrations/` menyimpan riwayat skema.

### Flag

| Flag | Efek |
| --- | --- |
| `--dev` | Di macOS, path serial Linux seperti `/dev/ttyUSB0` dipetakan ke perangkat USB serial lokal |
| `--poof` | API tetap jalan, polling dimatikan. Tanpa flag ini, polling aktif |

```bash
go run ./cmd/server --dev
go run ./cmd/server --poof
```

Hentikan proses dengan `Ctrl+C`. Scheduler dan updater acak set-point berhenti pada `SIGINT` / `SIGTERM`.

## Konfigurasi

Pengaturan dibaca dari environment variable. Berkas `.env` di direktori kerja bersifat opsional. Jika tidak ada, nilai default di bawah yang dipakai.

| Variabel | Default | Keterangan |
| --- | --- | --- |
| `SERVER_HOST` | `0.0.0.0` | Alamat bind |
| `SERVER_PORT` | `8071` | Port HTTP |
| `DB_PATH` | `data/aoi-bgn.db` | Database SQLite utama (mode WAL) |
| `DB_LOG_PATH` | `data/aoi-write-log.db` | Database log aksi tulis |
| `REDIS_ENABLED` | `false` | `true` memakai Redis; `false` memakai cache di memori |
| `REDIS_HOST` | `localhost` | Host Redis |
| `REDIS_PORT` | `6379` | Port Redis |
| `REDIS_PASSWORD` | kosong | Password Redis |
| `REDIS_DB` | `0` | Indeks database Redis |
| `LOG_LEVEL` | `debug` | Level Logrus. `silent` membuang semua log |
| `LOG_FORMAT` | `text` | `text` atau `json` |
| `APP_NAME` | `AOI System` | Nama aplikasi |
| `APP_ENV` | `development` | Nama environment |
| `APP_DEV` | `false` | Efek yang sama dengan `--dev` |

Contoh:

```env
SERVER_PORT=8071
SERVER_HOST=0.0.0.0
DB_PATH=data/aoi-bgn.db
DB_LOG_PATH=data/aoi-write-log.db
REDIS_ENABLED=false
LOG_LEVEL=info
LOG_FORMAT=text
APP_ENV=development
```

Dev server frontend mem-proxy `/api` ke `http://localhost:8071`.

## Alur data

1. Scheduler memuat parameter READ dan READ_WRITE, lalu menjalankan worker.
2. Job Modbus pada satu koneksi berjalan satu per satu (round-robin). Job SNMP berjalan paralel, dibatasi 50 permintaan bersamaan. Job HTTP di-polling sesuai interval masing-masing.
3. Pembacaan yang berhasil di-decode, di-scale, disimpan di cache, dicek terhadap ambang batas, lalu di-broadcast.
4. Formula engine menghitung ulang setiap formula yang bergantung pada parameter itu (batas kedalaman 10), kemudian memberi tahu workflow engine.
5. Workflow yang cocok bisa menulis register, menunggu, mengirim event, atau membuka alarm. Perubahan set-point juga bisa memicu workflow.
6. Bypass engine mengevaluasi aturannya sendiri saat set-point atau parameter yang diawasi berubah, lalu menulis parameter atau set-point lain.

Parameter `SOURCE` tetap internal. Nilai `DIRECT`, `CLEAN`, dan `FORMULA` di-broadcast. Parameter `WRITE` adalah perintah dan tidak di-polling.

## WebSocket

Sambungkan ke `GET /api/v1/ws`.

| `type` | Arti |
| --- | --- |
| `data_update` | Satu nilai live |
| `data_update_batch` | Beberapa nilai live dalam satu frame |
| `error` | Error baca atau proses pada suatu parameter |
| `status_update` | Status gaya online / offline |
| `alarm` | Alarm workflow dibuka |
| `alarm_removed` | Alarm workflow ditutup |

Alarm workflow dan ambang batas juga dikirim sebagai `{"message":"..."}` ke `http://localhost:8085/send`. Panggilan itu gagal diam-diam jika endpoint tidak hidup.

## HTTP API

Base path: `/api/v1`. Respons JSON memakai `success` plus `data` atau `error`.

| Area | Path | Catatan |
| --- | --- | --- |
| Perangkat | `/devices` | CRUD. `GET /devices/type` memfilter menurut tipe. `POST /devices/:id/test-connection` menguji koneksi |
| Parameter | `/parameters` | CRUD, ganti nama (`PATCH /:id/name`), dan `POST /:id/write`. `POST /parameters/test-connection` menguji Modbus |
| Modbus RTU | `/modbus/rtu` | Peta register: function code, alamat, quantity, byte order, tipe data, scaling |
| Modbus TCP | `/modbus/tcp` | Peta register yang sama lewat TCP |
| SNMP | `/snmp` | OID, community, versi, pemetaan string ke angka |
| HTTP | `/http` | URL, auth, header, pemetaan field JSON. `POST /http/test-fetch` mencoba request |
| Realtime | `/realtime`, `/realtime/:parameter_id` | Nilai cache terbaru |
| Snapshot | `/snaps` | Semua nilai parameter di cache |
| Mapping | `POST /mapping/save` | Simpan mapping parameter dan segarkan engine |
| Write | `POST /write/execute`, `POST /write/test` | Commit atau uji penulisan |
| Set-point | `/set-points` | Nilai state UI (`int`, `float`, `bool`) dengan min, max, dan jalan acak opsional |
| Bypass | `/bypasses` | Aturan IF/THEN ringan di atas set-point dan parameter |
| IMC | `/imc-config` | Petakan card key ke parameter atau set-point |
| Parameter internal | `/internal-parameters` | Buat, set, reset, dan baca nilai untuk merangkai workflow |
| Workflow | `/workflows` | Versi 1 adalah daftar kondisi/aksi datar. Versi 2 menyimpan pohon di `tree_data` |
| Ambang batas | `/tracehold` | Buat atau ubah menurut parameter. Ejaan path-nya memang `tracehold` |
| Log ambang batas | `/threshold-logs/parameter/:parameter_id` | Riwayat pelanggaran dan pemulihan. Query `limit` |
| Alarm | `/alarms` | Alarm workflow yang masih aktif. Hapus menurut id atau workflow id |
| Log tulis | `/write-logs` | Filter menurut id, parameter, workflow, atau `start_date` / `end_date` |

### Mode parameter

| Field | Nilai |
| --- | --- |
| `type` | `modbus_rtu`, `modbus_tcp`, `snmp`, `analog_input`, `analog_output`, `digital_input`, `digital_output`, `formula`, `internal` |
| `direction` / `read_write` | `READ` (`0`), `WRITE` (`1`), `READ_WRITE` (`2`) |
| `parameter_mode` | `SOURCE`, `DIRECT`, `CLEAN`, `FORMULA` |
| `write_mode` | `PRESET` (nilai tersimpan) atau `DYNAMIC` (nilai diisi saat menulis) |

### Workflow versi 2

`tree_data` adalah pohon JSON:

- Pemicu: `START`, `PARAMETER_CHANGED`, `SETPOINT_CHANGED`
- Node: `if`, `action`, `timer`
- Kondisi: perbandingan (`parameter`, `setpoint`, atau `static`), jendela waktu (`HH:MM`, hari opsional), atau status parameter (`online` / `offline`)
- Aksi: `ALARM_NOTIFICATION`, `WRITE_DYNAMIC`, `WRITE_PRESET`

Aksi versi 1 adalah `TRIGGER_WRITE`, `WRITE_WITH_VALUE`, `event`, `delay`, dan `alarm`, dijalankan sesuai `action_order`.

### Bypass

Kondisi rule adalah `always` atau `compare`. Sisi kiri berupa set-point atau parameter. Sisi kanan berupa angka statis atau set-point lain. Setiap aksi menulis parameter atau set-point, dengan sumber nilai statis atau set-point.

## Struktur proyek

```
aoi-backend/
├── cmd/server/            # Entrypoint, wiring, migrasi saat start
├── api/
│   ├── handlers/          # Handler HTTP dan WebSocket
│   ├── middleware/        # CORS dan log request
│   └── routes/            # Tabel route /api/v1
├── internal/
│   ├── core/              # Config, SQLite, cache, logger, errors
│   ├── domain/            # Model GORM
│   ├── repository/        # Akses data
│   ├── service/           # Formula, workflow, bypass, write, threshold, set-point
│   ├── worker/            # Poller Modbus, SNMP, HTTP, dan hub WebSocket
│   ├── scheduler/         # Antrian polling dan cron
│   ├── modbus/            # Klien RTU/TCP, scaling, byte order
│   └── snmp/              # Klien SNMP
└── migrations/            # SQL historis; runtime memakai AutoMigrate
```

## Tech stack

- Go 1.21
- Gin
- GORM dengan SQLite (WAL, foreign key aktif)
- Redis, opsional
- Gorilla WebSocket
- goburrow/modbus dan gosnmp
- govaluate untuk ekspresi formula
- robfig/cron
- Logrus dan Viper
