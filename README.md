# rabbitMQRest

`rabbitMQRest` is a FastAPI facade for RabbitMQ used by PingOne AIC/OpenICF Scripted REST connectors.

Current version behavior is **multi-queue polling with lease + ACK/NACK + audit publishing**.

## What changed

- Polling is queue-driven via `GET /v1/events`.
- Legacy compatibility endpoint `GET /v1/events/users` is still available.
- ACK success and processing failures publish audit records to `AUDIT_QUEUE`.
- Consumers are long-lived per queue with auto-reconnect and connection tuning.

## Requirements

- Python 3.9+
- Reachable RabbitMQ broker

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

## Configuration

Create `.env` from `.env.example`:

```bash
cp .env.example .env
```

### Required for runtime

- `RABBITMQ_HOST`
- `RABBITMQ_PORT`
- `RABBITMQ_VHOST`
- `RABBITMQ_USERNAME`
- `RABBITMQ_PASSWORD`
- `RABBITMQ_USE_TLS`
- `FACADE_API_KEY`

### Commonly used defaults

- `PORT=8000`
- `PREFETCH=200`
- `DEFAULT_LEASE_SECONDS=10`
- `RABBITMQ_HEARTBEAT=60`
- `RABBITMQ_BLOCKED_CONNECTION_TIMEOUT=30`
- `RABBITMQ_SOCKET_TIMEOUT=10`
- `CONSUMER_RECONNECT_DELAY=5`
- `AUDIT_QUEUE=Unir.Audit.ReceivedBusMessages`
- `SUBSCRIBER_APPLICATION=PingOneAIC`
- `DEFAULT_QUEUE_USERS=Ping.CuentaModificadaSubscriptor`
- `DEFAULT_EVENTTYPE_USERS=CuentaModificada`

### Legacy variable

- `QUEUE_NAME` exists in some environments but is not used by the current multi-queue runtime.

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

### 1) Poll events (new endpoint)

`GET /v1/events`

Required query params:
- `queue`
- `eventType`

Optional query params:
- `limit` (default `100`)
- `leaseSeconds` (default from env)
- `includeData` (default `true`)

```bash
curl "http://localhost:8000/v1/events?queue=Ping.ClienteModificadoSubscriptor&eventType=ClienteModificado&limit=10&leaseSeconds=10&includeData=true" \
  -H "X-API-Key: replace_with_api_key"
```

### 2) Poll events (legacy compatibility endpoint)

`GET /v1/events/users`

```bash
curl "http://localhost:8000/v1/events/users?queue=Ping.ClienteModificadoSubscriptor&eventName=ClienteModificado&limit=10&leaseSeconds=10&includeData=true" \
  -H "X-API-Key: replace_with_api_key"
```

### 3) Publish message

`POST /v1/publish`

```bash
curl -X POST "http://localhost:8000/v1/publish" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{
    "exchange": "",
    "routingKey": "Ping.ClienteModificadoSubscriptor",
    "persistent": true,
    "contentType": "application/json",
    "headers": {
      "key": "evt-1001",
      "application": "PingOneAIC",
      "accountId": "24525",
      "action": "UPDATE"
    },
    "payload": {
      "IdCliente": "24525",
      "Estado": "ACTIVO"
    }
  }'
```

### 4) ACK by id

`POST /v1/events/ack-by-id`

```bash
curl -X POST "http://localhost:8000/v1/events/ack-by-id" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"eventId":"replace-with-event-id"}'
```

### 5) Fail by id (audit + NACK)

`POST /v1/events/fail-by-id`

```bash
curl -X POST "http://localhost:8000/v1/events/fail-by-id" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{
    "eventId":"replace-with-event-id",
    "exceptionMessage":"Business validation failed",
    "requeue":true
  }'
```

### 6) Batch ACK

`POST /v1/events/acks`

```bash
curl -X POST "http://localhost:8000/v1/events/acks" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"batchId":"b_12345678","eventIds":["event-id-1","event-id-2"]}'
```

### 7) Batch NACK

`POST /v1/events/nacks`

```bash
curl -X POST "http://localhost:8000/v1/events/nacks" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"batchId":"b_12345678","eventIds":["event-id-1"],"requeue":true}'
```

## OpenICF / Scripted REST connector

Sample config file:
- `provisioner.openicf-rabbitmqFacade-config.json`

Current connector scripts:
- `SearchScript.groovy`
- `DeleteScript.groovy`
- `CreateScript.groovy`
- `ReadScript.groovy`
- `CustomizerScript.groovy`

Important notes:
- `SearchScript.groovy` now uses `customConfiguration` with keys like `queueName`, `eventName`, `limit`, `leaseSeconds`, `includeData`.
- `DeleteScript.groovy` uses `POST /v1/events/ack-by-id`.
- `ReadScript.groovy` still references `GET /v1/user-events/{id}`, which is not implemented in the current FastAPI service.

## Postman

Use `RabbitMQ_Facade.postman_collection.json`.

Collection variables include:
- `facadeHost`
- `facadePort`
- `facadeAPIVersion`
- `facadeAPIKey`
- `queueName`
- `eventType`
- `eventName`

## Logs

Rotating log file:
- `facade.log`
