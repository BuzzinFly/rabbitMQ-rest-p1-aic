import json
import os
import threading
import time
import urllib
import uuid

import logging
from logging.handlers import RotatingFileHandler

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from fastapi import Query

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from dotenv import load_dotenv
load_dotenv()

from fastapi import Depends, Header

required = ["RABBITMQ_VHOST", "RABBITMQ_USERNAME", "RABBITMQ_PASSWORD", "QUEUE_NAME", "FACADE_API_KEY"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing required env vars: {missing}")

# ----------------------------
# Config
# ----------------------------
PORT = int(os.getenv("PORT", "8080"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5671"))
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_USE_TLS = os.getenv("RABBITMQ_USE_TLS", "true").lower() == "true"
FACADE_API_KEY = os.getenv("FACADE_API_KEY")
QUEUE_NAME = os.getenv("QUEUE_NAME")
PREFETCH = int(os.getenv("PREFETCH", "200"))
DEFAULT_LEASE_SECONDS = int(os.getenv("DEFAULT_LEASE_SECONDS", "60"))


publish_conn = None
publish_ch = None
publish_lock = threading.RLock()

channel_ref = {"connection": None, "channel": None}

# ----------------------------
# Build AMQP URL safely
# ----------------------------
scheme = "amqps" if RABBITMQ_USE_TLS else "amqp"

user = urllib.parse.quote(RABBITMQ_USERNAME, safe="")
password = urllib.parse.quote(RABBITMQ_PASSWORD, safe="")
vhost = urllib.parse.quote(RABBITMQ_VHOST, safe="")

RABBIT_URL = (
    f"{scheme}://{user}:{password}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/{vhost}"
)

# ----------------------------
# Logging configuration
# ----------------------------

LOG_FILE = os.path.join(os.path.dirname(__file__), "facade.log")

logger = logging.getLogger("facade")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s %(levelname)s [%(threadName)s] %(name)s - %(message)s"
)

# File handler (rotates at 10MB, keeps 5 backups)
file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5
)
file_handler.setFormatter(formatter)

# Console handler (so you still see logs in terminal)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Avoid duplicate handlers on reload
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# ----------------------------
# Data structures
# ----------------------------

@dataclass
class PendingRecord:
    event: Dict[str, Any]
    delivery_tag: int
    reserved_until_ms: Optional[int] = None
    batch_id: Optional[str] = None


# eventId -> PendingRecord
pending: Dict[str, PendingRecord] = {}

# batchId -> set(eventId)
batches: Dict[str, Set[str]] = {}

# Protect pending/batches
lock = threading.RLock()

# Rabbit channel reference used by HTTP handlers for ack/nack
channel_ref = {"channel": None}  # type: ignore


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def purge_expired_leases() -> None:
    """Release reservations after lease expiry (does NOT ack)."""
    t = now_ms()
    with lock:
        for rec in pending.values():
            if rec.reserved_until_ms is not None and rec.reserved_until_ms <= t:
                rec.reserved_until_ms = None
                rec.batch_id = None

def require_api_key(x_api_key: str = Header(None, alias="X-API-Key")):
    if x_api_key != FACADE_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

def parse_event(body: bytes, properties: pika.BasicProperties) -> Dict[str, Any]:
    """
    Normalize message into an event the AIC Scripted REST connector can consume.
    Expects JSON message, but will tolerate non-JSON by wrapping.
    """
    content_type = (properties.content_type or "application/json").lower()

    payload: Dict[str, Any]
    body_str = body.decode("utf-8", errors="replace")

    if "json" in content_type:
        payload = json.loads(body_str)
    else:
        payload = {"raw": body_str}

    event_id = (
        payload.get("eventId")
        or (properties.message_id if properties.message_id else None)
        or str(uuid.uuid4())
    )

    event_type = payload.get("eventType", "USER_UPDATED")
    occurred_at = payload.get("occurredAt", iso_now())

    # Correlation key: pick whichever your publisher uses consistently
    external_id = payload.get("externalId") or payload.get("employeeId") or payload.get("userId")

    # If publisher wraps attributes under "payload", use that; otherwise use entire payload
    event_payload = payload.get("payload", payload)

    return {
        "eventId": event_id,
        "eventType": event_type,
        "occurredAt": occurred_at,
        "externalId": external_id,
        "source": payload.get("source", "rabbitmq"),
        "payload": event_payload,
        "traceId": payload.get("traceId"),
    }


# ----------------------------
# Rabbit consumer
# ----------------------------

def get_publish_channel():
    global publish_conn, publish_ch
    with publish_lock:
        if publish_conn and publish_ch and publish_conn.is_open and publish_ch.is_open:
            return publish_ch

        logger.info("Creating separate RabbitMQ publish connection")
        params = pika.URLParameters(RABBIT_URL)
        publish_conn = pika.BlockingConnection(params)
        publish_ch = publish_conn.channel()
        return publish_ch

def rabbit_consumer_thread() -> None:
    """
    Long-running AMQP consumer. Receives messages and stores them in `pending`
    without ACKing. ACK/NACK is scheduled thread-safely via add_callback_threadsafe.
    """
    logger.info("Starting RabbitMQ consumer thread")

    params = pika.URLParameters(RABBIT_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Store BOTH so HTTP handlers can schedule callbacks safely
    channel_ref["connection"] = connection
    channel_ref["channel"] = channel

    logger.info("Connected to RabbitMQ, queue=%s", QUEUE_NAME)

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH)

    def on_message(ch, method, properties, body):
        try:
            evt = parse_event(body, properties)
            event_id = evt["eventId"]

            with lock:
                pending[event_id] = PendingRecord(
                    event=evt,
                    delivery_tag=method.delivery_tag,
                    reserved_until_ms=None,
                    batch_id=None,
                )

            logger.info("Received message eventId=%s deliveryTag=%s", event_id, method.delivery_tag)

        except Exception:
            logger.exception("Failed to parse message; sending to DLQ (nack requeue=false)")
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)

    try:
        logger.info("Starting consume loop")
        channel.start_consuming()
    except Exception:
        logger.exception("Consumer loop crashed")
    finally:
        try:
            if channel.is_open:
                channel.close()
        except Exception:
            pass
        try:
            if connection.is_open:
                connection.close()
        except Exception:
            pass
        logger.info("Consumer thread stopped")

def schedule_on_consumer_thread(fn):
    conn = channel_ref.get("connection")
    if conn is None or not getattr(conn, "is_open", False):
        raise HTTPException(status_code=503, detail="RabbitMQ consumer connection not ready")
    conn.add_callback_threadsafe(fn)


# ----------------------------
# REST API models
# ----------------------------

app = FastAPI(title="RabbitMQ User Events Facade", version="0.1.0")

class PublishRequest(BaseModel):
    exchange: str = ""
    routingKey: str
    payload: Dict
    contentType: str = "application/json"
    persistent: bool = True

class AckRequest(BaseModel):
    batchId: str
    eventIds: List[str]

class AckByIdRequest(BaseModel):
    eventId: str

class AckResponse(BaseModel):
    acked: List[str]
    ignored: List[str]


class NackRequest(BaseModel):
    batchId: str
    eventIds: List[str]
    requeue: bool = True
    reason: Optional[str] = None


class EventBatch(BaseModel):
    batchId: str
    leaseExpiresAt: str
    nextCursor: Optional[str] = None
    events: List[Dict[str, Any]]


# ----------------------------
# REST endpoints
# ----------------------------

@app.get("/health")
def health():
    with lock:
        return {"ok": True, "pending": len(pending)}


@app.get("/v1/events/users", response_model=None)

def get_user_events(
    limit: int = Query(default=100, ge=1, le=500),
    leaseSeconds: int = Query(default=DEFAULT_LEASE_SECONDS, ge=5, le=300),
    _: None = Depends(require_api_key)
):
    purge_expired_leases()

    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    limit = min(limit, 500)

    batch_id = "b_" + uuid.uuid4().hex[:8]
    lease_expires_ms = now_ms() + leaseSeconds * 1000
    lease_expires_at = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(lease_expires_ms / 1000.0)
    )

    events: List[Dict[str, Any]] = []

    with lock:
        for event_id, rec in pending.items():
            if len(events) >= limit:
                break
            if rec.reserved_until_ms is not None:
                continue  # already reserved

            rec.reserved_until_ms = lease_expires_ms
            rec.batch_id = batch_id

            batches.setdefault(batch_id, set()).add(event_id)
            events.append(rec.event)

    # If no events, return 204 like the earlier sketch
    if not events:
        # FastAPI doesn't have a simple decorator for 204 with no body; do it manually
        from fastapi.responses import Response
        return Response(status_code=204)

    return {
        "batchId": batch_id,
        "leaseExpiresAt": lease_expires_at,
        "nextCursor": None,
        "events": events,
    }


@app.post("/v1/publish")
def publish_message(req: PublishRequest, _: None = Depends(require_api_key)):
    try:
        ch = get_publish_channel()

        body = json.dumps(req.payload).encode("utf-8")
        props = pika.BasicProperties(
            content_type=req.contentType,
            delivery_mode=2 if req.persistent else 1
        )

        # Optional convenience for tests: ensure queue exists when publishing to default exchange
        if req.exchange == "":
            ch.queue_declare(queue=req.routingKey, durable=True)

        ch.basic_publish(
            exchange=req.exchange,
            routing_key=req.routingKey,
            body=body,
            properties=props,
        )

        logger.info("Published message exchange=%s routingKey=%s", req.exchange, req.routingKey)
        return {"ok": True}
    except Exception:
        logger.exception("Publish failed")
        raise

@app.post("/v1/events/acks", response_model=AckResponse)
def ack_events(req: AckRequest, _: None = Depends(require_api_key)):
    ch = channel_ref.get("channel")
    if ch is None:
        raise HTTPException(status_code=503, detail="RabbitMQ consumer channel not ready")

    acked, ignored = [], []

    with lock:
        for event_id in req.eventIds:
            rec = pending.get(event_id)
            if rec is None or rec.batch_id != req.batchId:
                ignored.append(event_id)
                continue

            delivery_tag = rec.delivery_tag

            # Schedule ACK on the consumer connection thread
            schedule_on_consumer_thread(lambda dt=delivery_tag: ch.basic_ack(delivery_tag=dt, multiple=False))

            pending.pop(event_id, None)
            acked.append(event_id)

        batches.pop(req.batchId, None)

    logger.info("ACK batch=%s acked=%d ignored=%d", req.batchId, len(acked), len(ignored))
    return AckResponse(acked=acked, ignored=ignored)

@app.post("/v1/events/ack-by-id",)
def ack_event_by_id(req: AckByIdRequest, _: None = Depends(require_api_key)):
    ch = channel_ref.get("channel")
    conn = channel_ref.get("connection")

    if ch is None or conn is None:
        raise HTTPException(status_code=503, detail="RabbitMQ consumer not ready")

    with lock:
        rec = pending.get(req.eventId)
        if rec is None:
            raise HTTPException(status_code=404, detail="Event not found or already ACKed")

        delivery_tag = rec.delivery_tag
        batch_id = rec.batch_id

        # Schedule ACK safely on consumer thread
        conn.add_callback_threadsafe(
            lambda dt=delivery_tag: ch.basic_ack(delivery_tag=dt, multiple=False)
        )

        # Cleanup
        pending.pop(req.eventId, None)
        if batch_id and batch_id in batches:
            batches[batch_id].discard(req.eventId)
            if not batches[batch_id]:
                batches.pop(batch_id, None)

    logger.info("ACK by eventId=%s", req.eventId)
    return {"acked": req.eventId}

@app.on_event("shutdown")
def shutdown():
    global publish_conn, publish_ch
    with publish_lock:
        try:
            if publish_ch and publish_ch.is_open:
                publish_ch.close()
        except Exception:
            pass
        try:
            if publish_conn and publish_conn.is_open:
                publish_conn.close()
        except Exception:
            pass

@app.post("/v1/events/nacks")
def nack_events(req: NackRequest, _: None = Depends(require_api_key)):
    ch = channel_ref.get("channel")
    if ch is None:
        raise HTTPException(status_code=503, detail="RabbitMQ consumer channel not ready")

    with lock:
        for event_id in req.eventIds:
            rec = pending.get(event_id)
            if rec is None or rec.batch_id != req.batchId:
                continue

            delivery_tag = rec.delivery_tag

            schedule_on_consumer_thread(
                lambda dt=delivery_tag, rq=req.requeue: ch.basic_nack(delivery_tag=dt, multiple=False, requeue=rq)
            )

            pending.pop(event_id, None)

        batches.pop(req.batchId, None)

    logger.warning("NACK batch=%s events=%d requeue=%s", req.batchId, len(req.eventIds), req.requeue)
    return {"ok": True}


# ----------------------------
# Startup
# ----------------------------

def start_consumer():
    t = threading.Thread(target=rabbit_consumer_thread, daemon=True)
    t.start()


start_consumer()

# Run with:
#   uvicorn facade:app --host 0.0.0.0 --port 8080
