# rabbitMQRest

`rabbitMQRest` is a FastAPI facade over RabbitMQ for PingOne AIC/OpenICF Scripted REST flows.

It supports:
- Publishing messages (`/v1/publish`)
- Polling messages with lease reservation (`/v1/events/users`)
- Acknowledging by batch or by id (`/v1/events/acks`, `/v1/events/ack-by-id`)
- Negative acknowledgements (`/v1/events/nacks`)

## Current architecture

- A background consumer thread pulls from `QUEUE_NAME` with manual ack and configurable prefetch.
- Messages are normalized into a bus envelope with fields:
  `Action`, `DateTime`, `Key`, `Name`, `Application`, `Sender`, `AccountId`, `Content`, `ExceptionMessage`
- `/v1/events/users` returns OpenICF-friendly objects including `_id`, `__NAME__`, `batchId`, and envelope attributes.
- Leases are in-memory only (process restart clears pending/lease state).

## Requirements

- Python 3.9+
- Reachable RabbitMQ broker (CloudAMQP or self-managed)

## Build

### macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows (PowerShell)
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Runtime configuration (`.env`)

Create env file:

```bash
cp .env.example .env
```

Required:
- `RABBITMQ_HOST`
- `RABBITMQ_VHOST`
- `RABBITMQ_USERNAME`
- `RABBITMQ_PASSWORD`
- `QUEUE_NAME`
- `FACADE_API_KEY`

Optional (with defaults from `rabbitMQRestFacade.py`):
- `PORT` default `8080` (set to `8000` to match examples below)
- `RABBITMQ_PORT` default `5671`
- `RABBITMQ_USE_TLS` default `true`
- `PREFETCH` default `200`
- `DEFAULT_LEASE_SECONDS` default `5`

## Run

```bash
uvicorn rabbitMQRestFacade:app --host 0.0.0.0 --port 8000
```

Health check:

```bash
curl http://localhost:8000/health
```

## API usage

All protected endpoints require:

```http
X-API-Key: <FACADE_API_KEY>
```

### Publish

`POST /v1/publish`

```bash
curl -X POST "http://localhost:8000/v1/publish" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{
    "exchange": "",
    "routingKey": "cs-baseline",
    "persistent": true,
    "contentType": "application/json",
    "payload": {
      "Name": "CuentaModificada",
      "Action": "UPDATE",
      "Application": "PingOneAIC",
      "Sender": "PingOneAIC",
      "AccountId": "24525",
      "Content": "{\"idAlumno\":\"24525\",\"status\":\"ACTIVE\"}"
    }
  }'
```

### Poll (lease + filter)

`GET /v1/events/users?limit=100&leaseSeconds=10&eventName=CuentaModificada&includeData=true`

```bash
curl "http://localhost:8000/v1/events/users?limit=10&leaseSeconds=10&eventName=CuentaModificada&includeData=true" \
  -H "X-API-Key: replace_with_api_key"
```

Notes:
- `eventName` defaults to `CuentaModificada`
- `includeData=true` adds parsed JSON from `Content` into `data`
- returns `204` when no events are available

### ACK batch

`POST /v1/events/acks`

```bash
curl -X POST "http://localhost:8000/v1/events/acks" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"batchId":"b_12345678","eventIds":["event-id-1","event-id-2"]}'
```

### ACK by id

`POST /v1/events/ack-by-id`

```bash
curl -X POST "http://localhost:8000/v1/events/ack-by-id" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"eventId":"event-id-1"}'
```

### NACK batch

`POST /v1/events/nacks`

```bash
curl -X POST "http://localhost:8000/v1/events/nacks" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"batchId":"b_12345678","eventIds":["event-id-1"],"requeue":true}'
```

## OpenICF / PingOne AIC configuration

Sample connector config:
- `provisioner.openicf-rabbitmqFacade-config.json`

Important fields:
- `serviceAddress`: base URL for facade (sample currently uses `http://localhost:8000`)
- `password`: GuardedString that stores `FACADE_API_KEY`
- `propertyBag.queueName`: default routing key used by `CreateScript.groovy`
- `propertyBag.eventName`: poll filter used by `SearchScript.groovy` (`CuentaModificada`)
- `propertyBag.limit`, `propertyBag.leaseSeconds`, `propertyBag.includeData`

Scripts in use:
- `CreateScript.groovy` publish + transform to legacy bus payload shape
- `SearchScript.groovy` poll + emit connector attributes
- `DeleteScript.groovy` ACK-by-id
- `ReadScript.groovy` read-by-id script
- `TestScript.groovy` health check
- `CustomizerScript.groovy` HTTP client customization hooks
- `SchemaScript.groovy` schema script stub

## Important compatibility note

- `ReadScript.groovy` references `GET /v1/user-events/{id}`.
- That endpoint is not currently implemented in `rabbitMQRestFacade.py`.
- Current supported read pattern is polling via `GET /v1/events/users` and ACKing with `POST /v1/events/ack-by-id`.

## Testing helpers

- `RabbitMQ_Facade.postman_collection.json`
- `make_script_payload.py`

## Logs

Rotating application log file:
- `facade.log`
