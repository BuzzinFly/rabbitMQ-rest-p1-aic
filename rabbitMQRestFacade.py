import json
import os
import threading
import time
import uuid
import hmac
import urllib.parse
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import pika
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.responses import Response
from pydantic import BaseModel

# ----------------------------
# Load .env
# ----------------------------
load_dotenv()

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

# ----------------------------
# Logging (file + console)
# ----------------------------
LOG_FILE = os.path.join(os.path.dirname(__file__), "facade.log")

logger = logging.getLogger("facade")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s %(levelname)s [%(threadName)s] %(name)s - %(message)s"
)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# ----------------------------
# Config (from .env)
# ----------------------------
PORT = int(os.getenv("PORT", "8080"))

RABBITMQ_HOST = require_env("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5671"))
RABBITMQ_VHOST = require_env("RABBITMQ_VHOST")
RABBITMQ_USERNAME = require_env("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = require_env("RABBITMQ_PASSWORD")
RABBITMQ_USE_TLS = os.getenv("RABBITMQ_USE_TLS", "true").lower() == "true"

QUEUE_NAME = require_env("QUEUE_NAME")
PREFETCH = int(os.getenv("PREFETCH", "200"))
DEFAULT_LEASE_SECONDS = int(os.getenv("DEFAULT_LEASE_SECONDS", "5"))

FACADE_API_KEY = require_env("FACADE_API_KEY")

def build_rabbit_url() -> str:
    scheme = "amqps" if RABBITMQ_USE_TLS else "amqp"
    user = urllib.parse.quote(RABBITMQ_USERNAME, safe="")
    password = urllib.parse.quote(RABBITMQ_PASSWORD, safe="")
    vhost = urllib.parse.quote(RABBITMQ_VHOST, safe="")
    return f"{scheme}://{user}:{password}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/{vhost}"

RABBIT_URL = build_rabbit_url()

# ----------------------------
# API Key auth dependency
# ----------------------------
def require_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    if not x_api_key or not hmac.compare_digest(x_api_key, FACADE_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

# ----------------------------
# RabbitMQ consumer thread objects
# ----------------------------
channel_ref = {"connection": None, "channel": None}  # consumer connection/channel

def schedule_on_consumer_thread(fn):
    conn = channel_ref.get("connection")
    if conn is None or not getattr(conn, "is_open", False):
        raise HTTPException(status_code=503, detail="RabbitMQ consumer connection not ready")
    conn.add_callback_threadsafe(fn)

# ----------------------------
# Publish connection (separate from consumer; thread-safe usage)
# ----------------------------
publish_conn = None
publish_ch = None
publish_lock = threading.RLock()

def get_publish_channel():
    global publish_conn, publish_ch
    with publish_lock:
        if publish_conn and publish_ch and publish_conn.is_open and publish_ch.is_open:
            return publish_ch

        logger.info("Creating (or recreating) publish connection")
        params = pika.URLParameters(RABBIT_URL)
        publish_conn = pika.BlockingConnection(params)
        publish_ch = publish_conn.channel()
        return publish_ch

# ----------------------------
# Helpers
# ----------------------------
def now_ms() -> int:
    return int(time.time() * 1000)

def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def safe_get_header(properties: pika.BasicProperties, key: str) -> Optional[Any]:
    try:
        if properties.headers and key in properties.headers:
            return properties.headers.get(key)
    except Exception:
        return None
    return None

def coerce_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, str):
        return x
    try:
        return str(x)
    except Exception:
        return None

# ----------------------------
# Envelope normalization
# ----------------------------
ENVELOPE_FIELDS = {
    "Action",
    "DateTime",
    "Key",
    "Name",
    "Application",
    "Sender",
    "AccountId",
    "Content",
    "ExceptionMessage",
}

def normalize_to_bus_envelope(payload: Dict[str, Any], properties: pika.BasicProperties) -> Dict[str, Any]:
    """
    Normalize inbound payload to your bus envelope schema:

      Action, DateTime, Key, Name, Application, Sender, AccountId, Content, ExceptionMessage

    - If payload already looks like the envelope (has Name and Content), keep it.
    - If payload is a legacy "data" style event (e.g., includes TipoEvento), wrap it into the envelope.
    - Content is always a STRING (JSON text) per your schema.
    """
    # Case 1: already in envelope form
    if isinstance(payload, dict) and ("Name" in payload and "Content" in payload):
        env = {k: payload.get(k) for k in ENVELOPE_FIELDS if k in payload}
        # Ensure Content is string
        if not isinstance(env.get("Content"), str):
            env["Content"] = json.dumps(env.get("Content"), ensure_ascii=False)
        # Best-effort defaults
        env.setdefault("Action", payload.get("Action") or "REGISTER")
        env.setdefault("DateTime", payload.get("DateTime") or utc_now_iso())
        env.setdefault("Key", payload.get("Key") or coerce_str(properties.correlation_id) or coerce_str(properties.message_id) or str(uuid.uuid4()))
        return env

    # Case 2: legacy/non-envelope JSON payload -> wrap
    tipo_evento = payload.get("TipoEvento") if isinstance(payload, dict) else None
    name = coerce_str(tipo_evento) or coerce_str(payload.get("Name")) or "UnknownEvent"

    # Try to source some metadata from AMQP props / headers, if present.
    # (These are optional; keep empty if unknown.)
    application = coerce_str(safe_get_header(properties, "Application")) or coerce_str(payload.get("Application"))
    sender = coerce_str(safe_get_header(properties, "Sender")) or coerce_str(payload.get("Sender"))
    account_id = coerce_str(safe_get_header(properties, "AccountId")) or coerce_str(payload.get("AccountId"))
    key = coerce_str(safe_get_header(properties, "Key")) or coerce_str(properties.correlation_id) or coerce_str(properties.message_id) or str(uuid.uuid4())
    action = coerce_str(safe_get_header(properties, "Action")) or coerce_str(payload.get("Action")) or "REGISTER"
    dt = coerce_str(safe_get_header(properties, "DateTime")) or coerce_str(payload.get("DateTime")) or utc_now_iso()

    return {
        "Action": action,
        "DateTime": dt,
        "Key": key,
        "Name": name,
        "Application": application,
        "Sender": sender,
        "AccountId": account_id,
        "Content": json.dumps(payload, ensure_ascii=False),
        "ExceptionMessage": coerce_str(payload.get("ExceptionMessage")),
    }

def extract_event_type(envelope: Dict[str, Any]) -> str:
    return coerce_str(envelope.get("Name")) or "UnknownEvent"

def parse_content_json(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse envelope["Content"] (string) into dict when it's valid JSON.
    If not JSON, return {"raw": <string>}.
    """
    c = envelope.get("Content")
    if not isinstance(c, str):
        return {"raw": c}
    try:
        return json.loads(c)
    except Exception:
        return {"raw": c}

def parse_message(body: bytes, properties: pika.BasicProperties) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (msg_id, envelope_dict).

    msg_id is the facade _id used for ACK-by-id.
    """
    content_type = (properties.content_type or "application/json").lower()
    body_str = body.decode("utf-8", errors="replace")

    if "json" in content_type:
        try:
            payload = json.loads(body_str)
        except Exception:
            payload = {"raw": body_str}
    else:
        payload = {"raw": body_str}

    # Prefer AMQP message_id if producer set it; else generate one.
    msg_id = properties.message_id or str(uuid.uuid4())

    # Normalize to bus envelope schema (supports future event types cleanly)
    if isinstance(payload, dict):
        envelope = normalize_to_bus_envelope(payload, properties)
    else:
        envelope = normalize_to_bus_envelope({"raw": payload}, properties)

    return msg_id, envelope

# ----------------------------
# Pending store
# ----------------------------
@dataclass
class PendingRecord:
    delivery_tag: int
    envelope: Dict[str, Any]                # normalized bus envelope
    reserved_until_ms: Optional[int] = None
    batch_id: Optional[str] = None
    event_type: str = "UnknownEvent"

pending: Dict[str, PendingRecord] = {}     # _id -> record
batches: Dict[str, Set[str]] = {}          # batchId -> set(_id)
lock = threading.RLock()

def purge_expired_leases() -> None:
    t = now_ms()
    with lock:
        for rec in pending.values():
            if rec.reserved_until_ms is not None and rec.reserved_until_ms <= t:
                rec.reserved_until_ms = None
                rec.batch_id = None

# ----------------------------
# Rabbit consumer thread
# ----------------------------
def rabbit_consumer_thread() -> None:
    logger.info("Starting RabbitMQ consumer thread")
    params = pika.URLParameters(RABBIT_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel_ref["connection"] = connection
    channel_ref["channel"] = channel

    logger.info("Connected to RabbitMQ, queue=%s", QUEUE_NAME)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH)

    def on_message(ch, method, properties, body):
        try:
            msg_id, envelope = parse_message(body, properties)
            event_type = extract_event_type(envelope)

            with lock:
                pending[msg_id] = PendingRecord(
                    delivery_tag=method.delivery_tag,
                    envelope=envelope,
                    reserved_until_ms=None,
                    batch_id=None,
                    event_type=event_type,
                )

            logger.info("Received message _id=%s type=%s deliveryTag=%s", msg_id, event_type, method.delivery_tag)
        except Exception:
            logger.exception("Failed to parse message; nack requeue=false (DLQ if configured)")
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

def start_consumer():
    t = threading.Thread(target=rabbit_consumer_thread, daemon=True, name="rabbit-consumer")
    t.start()

# ----------------------------
# REST models
# ----------------------------
class PublishRequest(BaseModel):
    exchange: str = ""
    routingKey: str
    payload: Dict[str, Any]
    contentType: str = "application/json"
    persistent: bool = True
    messageId: Optional[str] = None
    headers: Optional[Dict[str, Any]] = None

class AckRequest(BaseModel):
    batchId: str
    eventIds: List[str]

class AckResponse(BaseModel):
    acked: List[str]
    ignored: List[str]

class AckByIdRequest(BaseModel):
    eventId: str

class NackRequest(BaseModel):
    batchId: str
    eventIds: List[str]
    requeue: bool = True
    reason: Optional[str] = None

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="RabbitMQ Facade (poll/ack)", version="1.1.0")

@app.on_event("startup")
def startup():
    start_consumer()
    logger.info("Facade startup complete. Queue=%s", QUEUE_NAME)

# ----------------------------
# REST endpoints
# ----------------------------
@app.get("/health")
def health():
    with lock:
        # Include counts by event type (helps operationally)
        by_type: Dict[str, int] = {}
        for rec in pending.values():
            by_type[rec.event_type] = by_type.get(rec.event_type, 0) + 1
        return {"ok": True, "pending": len(pending), "byType": by_type}

@app.get("/v1/events/users", response_model=None)
def get_user_events(
    limit: int = Query(default=100, ge=1, le=500),
    leaseSeconds: int = Query(default=DEFAULT_LEASE_SECONDS, ge=5, le=300),

    # Focus on CuentaModificada now, but extensible: pass eventName later for other types
    eventName: Optional[str] = Query(default="CuentaModificada"),

    # Backward compatibility / convenience:
    # - includeData=true returns parsed Content as "data" (object) alongside the envelope.
    includeData: bool = Query(default=True),

    _: None = Depends(require_api_key),
):
    purge_expired_leases()

    batch_id = "b_" + uuid.uuid4().hex[:8]
    lease_expires_ms = now_ms() + leaseSeconds * 1000

    results: List[Dict[str, Any]] = []

    with lock:
        for event_id, rec in pending.items():
            if len(results) >= limit:
                break
            if rec.reserved_until_ms is not None:
                continue  # already reserved
            if eventName and rec.event_type != eventName:
                continue  # filter by type

            rec.reserved_until_ms = lease_expires_ms
            rec.batch_id = batch_id
            batches.setdefault(batch_id, set()).add(event_id)

            item: Dict[str, Any] = {
                "_id": event_id,          # keep _id for ACK-by-id
                "__NAME__": event_id,     # keeps older connector expectations happy
                "batchId": batch_id,
                # Bus envelope fields:
                **rec.envelope,
            }

            # Convenience: parsed JSON version of Content (matches your earlier "data" style)
            if includeData:
                item["data"] = parse_content_json(rec.envelope)

            results.append(item)

    if not results:
        return Response(status_code=204)

    logger.info("Issued batch=%s results=%d leaseSeconds=%d eventName=%s", batch_id, len(results), leaseSeconds, eventName)

    # Return in the same top-level shape you previously showed (result/resultCount/etc.)
    return {
        "result": results,
        "resultCount": len(results),
        "pagedResultsCookie": None,
        "totalPagedResultsPolicy": "NONE",
        "totalPagedResults": -1,
        "remainingPagedResults": -1,
    }

@app.post("/v1/publish")
def publish_message(req: PublishRequest, _: None = Depends(require_api_key)):
    try:
        ch = get_publish_channel()

        body = json.dumps(req.payload, ensure_ascii=False).encode("utf-8")
        props = pika.BasicProperties(
            content_type=req.contentType,
            delivery_mode=2 if req.persistent else 1,
            message_id=req.messageId,
            headers=req.headers,
        )

        if req.exchange == "":
            ch.queue_declare(queue=req.routingKey, durable=True)

        ch.basic_publish(
            exchange=req.exchange,
            routing_key=req.routingKey,
            body=body,
            properties=props,
        )

        logger.info("Published message exchange=%s routingKey=%s messageId=%s", req.exchange, req.routingKey, req.messageId)
        return {"ok": True}
    except Exception:
        logger.exception("Publish failed")
        raise

@app.post("/v1/events/acks", response_model=AckResponse)
def ack_events(req: AckRequest, _: None = Depends(require_api_key)):
    ch = channel_ref.get("channel")
    if ch is None:
        raise HTTPException(status_code=503, detail="RabbitMQ consumer channel not ready")

    acked: List[str] = []
    ignored: List[str] = []

    with lock:
        for event_id in req.eventIds:
            rec = pending.get(event_id)
            if rec is None or rec.batch_id != req.batchId:
                ignored.append(event_id)
                continue

            delivery_tag = rec.delivery_tag
            schedule_on_consumer_thread(lambda dt=delivery_tag: ch.basic_ack(delivery_tag=dt, multiple=False))

            pending.pop(event_id, None)
            acked.append(event_id)

        batches.pop(req.batchId, None)

    logger.info("ACK batch=%s acked=%d ignored=%d", req.batchId, len(acked), len(ignored))
    return AckResponse(acked=acked, ignored=ignored)

@app.post("/v1/events/ack-by-id")
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

        conn.add_callback_threadsafe(lambda dt=delivery_tag: ch.basic_ack(delivery_tag=dt, multiple=False))

        pending.pop(req.eventId, None)

        if batch_id and batch_id in batches:
            batches[batch_id].discard(req.eventId)
            if not batches[batch_id]:
                batches.pop(batch_id, None)

    logger.info("ACK by id=%s", req.eventId)
    return {"acked": req.eventId}

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
    logger.info("Shutdown complete")
