# rabbitMQRest

`rabbitMQRest` is a FastAPI facade that exposes REST endpoints to:
- Publish messages to RabbitMQ
- Poll queued messages in batches with a lease
- ACK or NACK messages after downstream processing

It is designed to work with PingOne AIC / Scripted REST connector flows (Groovy and JS script examples are included in this repo).

## Requirements

- Python 3.9+
- Access to a RabbitMQ instance (CloudAMQP or self-managed)

## Build / Install

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

Create a `.env` from the example:

```bash
cp .env.example .env
```

Set the following values in `.env`:

- `RABBITMQ_HOST`
- `RABBITMQ_PORT` (default `5671`)
- `RABBITMQ_VHOST`
- `RABBITMQ_USERNAME`
- `RABBITMQ_PASSWORD`
- `RABBITMQ_USE_TLS` (`true` for `amqps`, `false` for `amqp`)
- `QUEUE_NAME` (queue consumed by `/v1/events/users`)
- `FACADE_API_KEY` (required in `X-API-Key` header)
- `PREFETCH` (optional, default `200`)
- `DEFAULT_LEASE_SECONDS` (optional, default `60`)

Optional:
- `PORT` (default `8080`; use this value when starting `uvicorn`)

## Run

```bash
uvicorn rabbitMQRestFacade:app --host 0.0.0.0 --port 8080
```

Health check:

```bash
curl http://localhost:8080/health
```

## API Usage

All protected endpoints require:

```http
X-API-Key: <your key>
```

### 1) Publish message

`POST /v1/publish`

```bash
curl -X POST "http://localhost:8080/v1/publish" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{
    "exchange": "",
    "routingKey": "new-users",
    "persistent": true,
    "contentType": "application/json",
    "payload": {
      "eventId": "test-001",
      "eventType": "USER_CREATED",
      "externalId": "EMP-1001",
      "payload": {
        "givenName": "Jane",
        "sn": "Doe"
      }
    }
  }'
```

### 2) Poll messages (reserve with lease)

`GET /v1/events/users?limit=100&leaseSeconds=60`

```bash
curl "http://localhost:8080/v1/events/users?limit=10&leaseSeconds=60" \
  -H "X-API-Key: replace_with_api_key"
```

Response contains:
- `batchId`
- `leaseExpiresAt`
- `events` as `[{"id":"...","data":{...}}]`

If no messages are available, the API returns `204 No Content`.

### 3) ACK by batch

`POST /v1/events/acks`

```bash
curl -X POST "http://localhost:8080/v1/events/acks" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"batchId":"b_12345678","eventIds":["event-id-1","event-id-2"]}'
```

### 4) ACK single event

`POST /v1/events/ack-by-id`

```bash
curl -X POST "http://localhost:8080/v1/events/ack-by-id" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"eventId":"event-id-1"}'
```

### 5) NACK by batch

`POST /v1/events/nacks`

```bash
curl -X POST "http://localhost:8080/v1/events/nacks" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: replace_with_api_key" \
  -d '{"batchId":"b_12345678","eventIds":["event-id-1"],"requeue":true}'
```

## Included Connector Scripts

This repo includes Scripted REST connector scripts for PingOne AIC/OpenICF:
- `SearchScript.groovy` / `rabbitMQFacadeSearch.js` (poll)
- `DeleteScript.groovy` / `rabbitMQFacadeAckEventsById.js` (ack)
- `CreateScript.groovy` (publish)
- `SchemaScript.groovy`
- `CustomizerScript.groovy`

Default sample connector patch:
- `provisioner.openicf-rabbitmqFacade-config.json`

## Testing Helpers

- Postman collection: `RabbitMQ_Facade.postman_collection.json`
- Script payload helper: `make_script_payload.py`

## Logs

The service writes rotating logs to:
- `facade.log`
