# RabbitMQ REST Facade

A small FastAPI service that consumes messages from RabbitMQ and exposes a polling + ack/nack REST interface. It also supports publishing messages to RabbitMQ.

## Requirements

- Python 3.9+
- RabbitMQ (or CloudAMQP)

## Setup

1) Copy the env template and fill in values:
```
cp .env.example .env
```

2) Create a virtual environment and install dependencies.

### macOS / Linux
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows (PowerShell)
```
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

### macOS / Linux
```
uvicorn rabbitMQRestFacade:app --host 0.0.0.0 --port 8080
```

### Windows (PowerShell)
```
uvicorn rabbitMQRestFacade:app --host 0.0.0.0 --port 8080
```

The service reads config from `.env` and listens on `PORT` (default `8080`).

## API Endpoints

Base URL: `http://localhost:8080`

All `/v1/*` endpoints require the `X-API-Key` header set to your `FACADE_API_KEY`.

## API Flow (Poll → Ack/Nack)

1) Poll for events:
```
GET /v1/events/users?limit=100&leaseSeconds=60
```

2) Process each event in `events[]`.

3) Acknowledge success (recommended):
```
POST /v1/events/acks
{
  "batchId": "b_1234abcd",
  "eventIds": ["event-id-1", "event-id-2"]
}
```

4) If processing fails, NACK to requeue (or drop):
```
POST /v1/events/nacks
{
  "batchId": "b_1234abcd",
  "eventIds": ["event-id-1", "event-id-2"],
  "requeue": true
}
```

### GET /health
Simple health check.

```
curl http://localhost:8080/health
```

### GET /v1/events/users
Polls for available events. Returns `204` if no events are available.

Query params:
- `limit` (1-500, default 100)
- `leaseSeconds` (5-300, default `DEFAULT_LEASE_SECONDS`)

```
curl "http://localhost:8080/v1/events/users?limit=50&leaseSeconds=60" \
  -H "X-API-Key: your_api_key"
```

Response (when events exist):
```
{
  "batchId": "b_1234abcd",
  "leaseExpiresAt": "2025-01-01T12:00:00Z",
  "nextCursor": null,
  "events": [
    { "id": "event-id-1", "data": { "any": "payload" } }
  ]
}
```

### POST /v1/publish
Publishes a message to RabbitMQ.

```
curl -X POST http://localhost:8080/v1/publish \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "exchange": "",
    "routingKey": "your-queue",
    "payload": { "hello": "world" },
    "contentType": "application/json",
    "persistent": true,
    "messageId": "optional-message-id",
    "headers": { "source": "facade" }
  }'
```

### POST /v1/events/acks
Acknowledges a batch of events by `batchId` and `eventIds`.

```
curl -X POST http://localhost:8080/v1/events/acks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "batchId": "b_1234abcd",
    "eventIds": ["event-id-1", "event-id-2"]
  }'
```

### POST /v1/events/ack-by-id
Acknowledges a single event by id.

```
curl -X POST http://localhost:8080/v1/events/ack-by-id \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "eventId": "event-id-1"
  }'
```

### POST /v1/events/nacks
Negatively acknowledges a batch of events, with optional requeue.

```
curl -X POST http://localhost:8080/v1/events/nacks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_api_key" \
  -d '{
    "batchId": "b_1234abcd",
    "eventIds": ["event-id-1", "event-id-2"],
    "requeue": true,
    "reason": "optional reason"
  }'
```
