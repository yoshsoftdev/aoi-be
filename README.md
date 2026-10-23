# AOI Backend

[English](README.md) · [Bahasa Indonesia](README.id.md)

HTTP and WebSocket API for the AOI multi-source data acquisition system. It polls field devices, evaluates formulas, runs workflow and bypass rules, and streams live values to the dashboard.

The frontend is served separately (Nginx on port 80 in production, Vite in development). This process only serves the API.

## Features

- Device connections: Modbus RTU, Modbus TCP, SNMP, and HTTP
- Parameter polling with per-connection round-robin for Modbus
- Derived parameters through a reactive formula engine
- Workflow engine (flat rules and tree rules) that can write, delay, and raise alarms
- Bypass rules that compare and write set-points or parameters
- User-defined set-points, including an optional random walk inside min/max
- Thresholds with alarm status and threshold history
- Live updates over WebSocket
- Write audit log in a separate SQLite database
- IMC card mapping to a parameter or a set-point

## Requirements

- Go 1.21 or newer
- A C compiler (`CGO` is required by the SQLite driver)
- Optional: Redis, when `REDIS_ENABLED=true`

## Run

From this directory:

```bash
go run ./cmd/server
```

The server listens on `0.0.0.0:8071` by default. SQLite files are created under `data/` on first start. Schema is applied with GORM AutoMigrate, plus a few compatibility alters in `cmd/server/main.go`. The SQL files in `migrations/` document the schema history.

### Flags

| Flag | Effect |
| --- | --- |
| `--dev` | On macOS, map Linux serial paths such as `/dev/ttyUSB0` to a local USB serial device |
| `--poof` | Start the API and skip polling. Polling is on when this flag is omitted |

```bash
go run ./cmd/server --dev
go run ./cmd/server --poof
```

Stop the process with `Ctrl+C`. The scheduler and the set-point random updater shut down on `SIGINT` / `SIGTERM`.

## Configuration

Settings come from environment variables. A `.env` file in the working directory is optional. If it is missing, the defaults below are used.

| Variable | Default | Description |
| --- | --- | --- |
| `SERVER_HOST` | `0.0.0.0` | Bind address |
| `SERVER_PORT` | `8071` | HTTP port |
| `DB_PATH` | `data/aoi-bgn.db` | Main SQLite database (WAL mode) |
| `DB_LOG_PATH` | `data/aoi-write-log.db` | Write-action log database |
| `REDIS_ENABLED` | `false` | `true` uses Redis; `false` uses an in-memory cache |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | empty | Redis password |
| `REDIS_DB` | `0` | Redis database index |
| `LOG_LEVEL` | `debug` | Logrus level. `silent` discards all logs |
| `LOG_FORMAT` | `text` | `text` or `json` |
| `APP_NAME` | `AOI System` | Application name |
| `APP_ENV` | `development` | Environment name |
| `APP_DEV` | `false` | Same effect as `--dev` |

Example:

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

The frontend dev server proxies `/api` to `http://localhost:8071`.

## Data flow

1. The scheduler loads READ and READ_WRITE parameters and starts workers.
2. Modbus jobs for one connection run one at a time (round-robin). SNMP jobs run in parallel, capped at 50 concurrent requests. HTTP jobs poll on their own interval.
3. A successful read is decoded, scaled, stored in cache, checked against the parameter threshold, and broadcast.
4. The formula engine re-evaluates every formula that depends on the updated parameter (depth limit 10), then notifies the workflow engine.
5. A matching workflow can write a register, wait, emit an event, or open an alarm. Set-point changes can also trigger a workflow.
6. The bypass engine evaluates its own rules when a set-point or parameter it watches changes, then writes a parameter or another set-point.

`SOURCE` parameters stay internal. `DIRECT`, `CLEAN`, and `FORMULA` values are broadcast. `WRITE` parameters are commands and are not polled.

## WebSocket

Connect to `GET /api/v1/ws`.

| `type` | Meaning |
| --- | --- |
| `data_update` | One live value |
| `data_update_batch` | Several live values in one frame |
| `error` | Read or processing error for a parameter |
| `status_update` | Online / offline style status |
| `alarm` | Workflow alarm opened |
| `alarm_removed` | Workflow alarm cleared |

Workflow and threshold alarms are also posted as `{"message":"..."}` to `http://localhost:8085/send`. That call fails silently when the endpoint is down.

## HTTP API

Base path: `/api/v1`. JSON responses use `success` plus `data` or `error`.

| Area | Path | Notes |
| --- | --- | --- |
| Devices | `/devices` | CRUD. `GET /devices/type` lists by type. `POST /devices/:id/test-connection` checks the link |
| Parameters | `/parameters` | CRUD, rename (`PATCH /:id/name`), and `POST /:id/write`. `POST /parameters/test-connection` tests Modbus |
| Modbus RTU | `/modbus/rtu` | Register map: function code, address, quantity, byte order, data type, scaling |
| Modbus TCP | `/modbus/tcp` | Same register map over TCP |
| SNMP | `/snmp` | OID, community, version, string-to-number mapping |
| HTTP | `/http` | URL, auth, headers, JSON field mapping. `POST /http/test-fetch` tries a request |
| Realtime | `/realtime`, `/realtime/:parameter_id` | Latest cached values |
| Snapshot | `/snaps` | All cached parameter values |
| Mapping | `POST /mapping/save` | Save a parameter mapping and refresh engines |
| Write | `POST /write/execute`, `POST /write/test` | Commit or test a write |
| Set-points | `/set-points` | UI state values (`int`, `float`, `bool`) with optional min, max, and random walk |
| Bypasses | `/bypasses` | Lightweight IF/THEN rules over set-points and parameters |
| IMC | `/imc-config` | Map a card key to a parameter or a set-point |
| Internal parameters | `/internal-parameters` | Create, set, reset, and read values used to chain workflows |
| Workflows | `/workflows` | Version 1 is a flat condition/action list. Version 2 stores a tree in `tree_data` |
| Thresholds | `/tracehold` | Create or update by parameter. Path spelling is `tracehold` |
| Threshold logs | `/threshold-logs/parameter/:parameter_id` | Breach and resolve history. Query `limit` |
| Alarms | `/alarms` | Active workflow alarms. Delete by id or by workflow id |
| Write logs | `/write-logs` | Filter by id, parameter, workflow, or `start_date` / `end_date` |

### Parameter modes

| Field | Values |
| --- | --- |
| `type` | `modbus_rtu`, `modbus_tcp`, `snmp`, `analog_input`, `analog_output`, `digital_input`, `digital_output`, `formula`, `internal` |
| `direction` / `read_write` | `READ` (`0`), `WRITE` (`1`), `READ_WRITE` (`2`) |
| `parameter_mode` | `SOURCE`, `DIRECT`, `CLEAN`, `FORMULA` |
| `write_mode` | `PRESET` (stored value) or `DYNAMIC` (value supplied at write time) |

### Workflow version 2

`tree_data` is a JSON tree:

- Triggers: `START`, `PARAMETER_CHANGED`, `SETPOINT_CHANGED`
- Nodes: `if`, `action`, `timer`
- Conditions: comparison (`parameter`, `setpoint`, or `static`), time window (`HH:MM`, optional weekdays), or parameter status (`online` / `offline`)
- Actions: `ALARM_NOTIFICATION`, `WRITE_DYNAMIC`, `WRITE_PRESET`

Version 1 actions are `TRIGGER_WRITE`, `WRITE_WITH_VALUE`, `event`, `delay`, and `alarm`, executed in `action_order`.

### Bypass

A rule condition is `always` or `compare`. The left side is a set-point or a parameter. The right side is a static number or another set-point. Each action writes either a parameter or a set-point, using a static value or a set-point as the source.

## Project layout

```
aoi-backend/
├── cmd/server/            # Process entrypoint, wiring, startup migrations
├── api/
│   ├── handlers/          # HTTP and WebSocket handlers
│   ├── middleware/        # CORS and request logging
│   └── routes/            # /api/v1 route table
├── internal/
│   ├── core/              # Config, SQLite, cache, logger, errors
│   ├── domain/            # GORM models
│   ├── repository/        # Data access
│   ├── service/           # Formula, workflow, bypass, write, threshold, set-point
│   ├── worker/            # Modbus, SNMP, HTTP pollers and the WebSocket hub
│   ├── scheduler/         # Polling queues and cron
│   ├── modbus/            # RTU/TCP clients, scaling, byte order
│   └── snmp/              # SNMP client
└── migrations/            # Historical SQL; runtime uses AutoMigrate
```

## Tech stack

- Go 1.21
- Gin
- GORM with SQLite (WAL, foreign keys on)
- Redis, optional
- Gorilla WebSocket
- goburrow/modbus and gosnmp
- govaluate for formula expressions
- robfig/cron
- Logrus and Viper
