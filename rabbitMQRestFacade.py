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

# Connection tuning / resilience
RABBITMQ_HEARTBEAT = int(os.getenv("RABBITMQ_HEARTBEAT", "60"))
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT = int(os.getenv("RABBITMQ_BLOCKED_CONNECTION_TIMEOUT", "30"))
RABBITMQ_SOCKET_TIMEOUT = int(os.getenv("RABBITMQ_SOCKET_TIMEOUT", "10"))
CONSUMER_RECONNECT_DELAY = int(os.getenv("CONSUMER_RECONNECT_DELAY", "5"))

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
# epoch increments on each successful consumer reconnect; delivery_tag is only valid within the epoch/channel it was received.
channel_ref = {"connection": None, "channel": None, "epoch": 0}


def schedule_on_consumer_thread(fn):
    conn = channel_ref.get("connection")
    if conn is None or not getattr(conn, "is_open", False):
        raise HTTPException(status_code=503, detail="RabbitMQ consumer connection not ready")
    try:
        conn.add_callback_threadsafe(fn)
    except pika.exceptions.ConnectionWrongStateError:
        # Treat as lease invalid / retryable rather than 500
        raise HTTPException(status_code=409, detail="Lease channel closed; event will be redelivered")


# ----------------------------
# Publish connection (separate from consumer; guarded with a lock)
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
        params.heartbeat = RABBITMQ_HEARTBEAT
        params.blocked_connection_timeout = RABBITMQ_BLOCKED_CONNECTION_TIMEOUT
        params.socket_timeout = RABBITMQ_SOCKET_TIMEOUT

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
    Normalize inbound payload to bus envelope schema:

      Action, DateTime, Key, Name, Application, Sender, AccountId, Content, ExceptionMessage

    - If payload already looks like envelope (has Name and Content), keep it.
    - If payload is legacy/non-envelope, wrap it.
    - Content is always a STRING (JSON text).
    """
    # Case 1: already in envelope form
    if isinstance(payload, dict) and ("Name" in payload and "Content" in payload):
        env = {k: payload.get(k) for k in ENVELOPE_FIELDS if k in payload}
        # Ensure Content is string
        if not isinstance(env.get("Content"), str):
            env["Content"] = json.dumps(env.get("Content"), ensure_ascii=False)
        # Defaults
        env.setdefault("Action", payload.get("Action") or "REGISTER")
        env.setdefault("DateTime", payload.get("DateTime") or utc_now_iso())
        env.setdefault(
            "Key",
            payload.get("Key")
            or coerce_str(properties.correlation_id)
            or coerce_str(properties.message_id)
            or str(uuid.uuid4()),
        )
        return env

    # Case 2: legacy/non-envelope JSON payload -> wrap
    tipo_evento = payload.get("TipoEvento") if isinstance(payload, dict) else None
    name = coerce_str(tipo_evento) or coerce_str(payload.get("Name")) or "UnknownEvent"

    application = coerce_str(safe_get_header(properties, "Application")) or coerce_str(payload.get("Application"))
    sender = coerce_str(safe_get_header(properties, "Sender")) or coerce_str(payload.get("Sender"))
    account_id = coerce_str(safe_get_header(properties, "AccountId")) or coerce_str(payload.get("AccountId"))
    key = (
        coerce_str(safe_get_header(properties, "Key"))
        or coerce_str(properties.correlation_id)
        or coerce_str(properties.message_id)
        or str(uuid.uuid4())
    )
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

    # Normalize to bus envelope schema
    if isinstance(payload, dict):
        envelope = normalize_to_bus_envelope(payload, properties)
    else:
        envelope = normalize_to_bus_envelope({"raw": payload}, properties)

    return msg_id, envelope


# ----------------------------
# Pending store (leased events)
# ----------------------------
@dataclass
class PendingRecord:
    delivery_tag: int
    envelope: Dict[str, Any]
    reserved_until_ms: Optional[int] = None
    batch_id: Optional[str] = None
    event_type: str = "UnknownEvent"
    epoch: int = 0  # consumer connection epoch at the time message was received


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
# Rabbit consumer thread (reconnecting)
# ----------------------------
def rabbit_consumer_thread() -> None:
    logger.info("Starting RabbitMQ consumer thread (reconnecting loop)")

    while True:
        connection = None
        channel = None

        try:
            params = pika.URLParameters(RABBIT_URL)
            params.heartbeat = RABBITMQ_HEARTBEAT
            params.blocked_connection_timeout = RABBITMQ_BLOCKED_CONNECTION_TIMEOUT
            params.socket_timeout = RABBITMQ_SOCKET_TIMEOUT

            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            # bump epoch on each successful connect
            with lock:
                channel_ref["epoch"] = int(channel_ref.get("epoch", 0)) + 1
                epoch = channel_ref["epoch"]
                channel_ref["connection"] = connection
                channel_ref["channel"] = channel

                # delivery_tags from previous channel are invalid after reconnect
                pending.clear()
                batches.clear()

            logger.info("Connected to RabbitMQ (epoch=%s), queue=%s", epoch, QUEUE_NAME)

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
                            epoch=epoch,
                        )

                    logger.info(
                        "Received message _id=%s type=%s deliveryTag=%s epoch=%s",
                        msg_id, event_type, method.delivery_tag, epoch
                    )
                except Exception:
                    logger.exception("Failed to parse message; nack requeue=false (DLQ if configured)")
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=False)
                    except Exception:
                        pass

            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)

            logger.info("Starting consume loop (epoch=%s)", epoch)
            channel.start_consuming()

        except Exception:
            logger.exception("Consumer loop crashed; will reconnect in %ss", CONSUMER_RECONNECT_DELAY)

        finally:
            # clear refs if they still point to this connection/channel
            with lock:
                if channel_ref.get("connection") is connection:
                    channel_ref["connection"] = None
                if channel_ref.get("channel") is channel:
                    channel_ref["channel"] = None

            try:
                if channel and channel.is_open:
                    channel.close()
            except Exception:
                pass
            try:
                if connection and connection.is_open:
                    connection.close()
            except Exception:
                pass

            time.sleep(CONSUMER_RECONNECT_DELAY)


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
app = FastAPI(title="RabbitMQ Facade (poll/ack)", version="1.2.0")


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
        by_type: Dict[str, int] = {}
        for rec in pending.values():
            by_type[rec.event_type] = by_type.get(rec.event_type, 0) + 1

        conn = channel_ref.get("connection")
        ch = channel_ref.get("channel")
        epoch = channel_ref.get("epoch", 0)
        consumer_state = {
            "epoch": epoch,
            "connectionOpen": bool(conn and getattr(conn, "is_open", False)),
            "channelOpen": bool(ch and getattr(ch, "is_open", False)),
        }

        return {"ok": True, "pending": len(pending), "byType": by_type, "consumer": consumer_state}


@app.get("/v1/events/users", response_model=None)
def get_user_events(
    limit: int = Query(default=100, ge=1, le=500),
    leaseSeconds: int = Query(default=DEFAULT_LEASE_SECONDS, ge=5, le=300),
    eventName: Optional[str] = Query(default="CuentaModificada"),
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
                continue
            if eventName and rec.event_type != eventName:
                continue

            rec.reserved_until_ms = lease_expires_ms
            rec.batch_id = batch_id
            batches.setdefault(batch_id, set()).add(event_id)

            item: Dict[str, Any] = {
                "_id": event_id,
                "__NAME__": event_id,
                "batchId": batch_id,
                **rec.envelope,
            }

            if includeData:
                item["data"] = parse_content_json(rec.envelope)

            results.append(item)

    if not results:
        return Response(status_code=204)

    logger.info("Issued batch=%s results=%d leaseSeconds=%d eventName=%s", batch_id, len(results), leaseSeconds, eventName)

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

        # Default exchange: routingKey must be the queue name; declare for convenience
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
    conn = channel_ref.get("connection")
    current_epoch = channel_ref.get("epoch", 0)

    if ch is None or conn is None or not conn.is_open or not ch.is_open:
        raise HTTPException(status_code=503, detail="RabbitMQ consumer not ready")

    acked: List[str] = []
    ignored: List[str] = []
    to_ack: List[int] = []

    with lock:
        for event_id in req.eventIds:
            rec = pending.get(event_id)
            if rec is None or rec.batch_id != req.batchId:
                ignored.append(event_id)
                continue
            if rec.epoch != current_epoch:
                pending.pop(event_id, None)
                ignored.append(event_id)
                continue

            to_ack.append(rec.delivery_tag)
            pending.pop(event_id, None)
            acked.append(event_id)

        batches.pop(req.batchId, None)

    for dt in to_ack:
        schedule_on_consumer_thread(lambda d=dt: ch.basic_ack(delivery_tag=d, multiple=False))

    logger.info("ACK batch=%s acked=%d ignored=%d", req.batchId, len(acked), len(ignored))
    return AckResponse(acked=acked, ignored=ignored)


@app.post("/v1/events/ack-by-id")
def ack_event_by_id(req: AckByIdRequest, _: None = Depends(require_api_key)):
    ch = channel_ref.get("channel")
    conn = channel_ref.get("connection")
    current_epoch = channel_ref.get("epoch", 0)

    if ch is None or conn is None or not conn.is_open or not ch.is_open:
        raise HTTPException(status_code=503, detail="RabbitMQ consumer not ready")

    with lock:
        rec = pending.get(req.eventId)
        if rec is None:
            raise HTTPException(status_code=404, detail="Event not found or already ACKed")

        # Can't ack stale delivery_tag after reconnect
        if rec.epoch != current_epoch:
            pending.pop(req.eventId, None)
            raise HTTPException(status_code=409, detail="Stale lease (consumer reconnected); event will be redelivered")

        delivery_tag = rec.delivery_tag
        batch_id = rec.batch_id

    # Schedule ack outside lock; ensure open state first
    if not conn.is_open or not ch.is_open:
        raise HTTPException(status_code=409, detail="Lease channel closed; event will be redelivered")

    schedule_on_consumer_thread(lambda dt=delivery_tag: ch.basic_ack(delivery_tag=dt, multiple=False))

    with lock:
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
    conn = channel_ref.get("connection")
    current_epoch = channel_ref.get("epoch", 0)

    if ch is None or conn is None or not conn.is_open or not ch.is_open:
        raise HTTPException(status_code=503, detail="RabbitMQ consumer not ready")

    to_nack: List[Tuple[int, bool]] = []

    with lock:
        for event_id in req.eventIds:
            rec = pending.get(event_id)
            if rec is None or rec.batch_id != req.batchId:
                continue
            if rec.epoch != current_epoch:
                pending.pop(event_id, None)
                continue

            to_nack.append((rec.delivery_tag, req.requeue))
            pending.pop(event_id, None)

        batches.pop(req.batchId, None)

    for dt, rq in to_nack:
        schedule_on_consumer_thread(lambda d=dt, r=rq: ch.basic_nack(delivery_tag=d, multiple=False, requeue=r))

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
