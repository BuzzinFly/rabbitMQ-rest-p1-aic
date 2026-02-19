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


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default


def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


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
PORT = env_int("PORT", 8000)

RABBITMQ_HOST = require_env("RABBITMQ_HOST")
RABBITMQ_PORT = env_int("RABBITMQ_PORT", 5672)
RABBITMQ_VHOST = require_env("RABBITMQ_VHOST")
RABBITMQ_USERNAME = require_env("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = require_env("RABBITMQ_PASSWORD")
RABBITMQ_USE_TLS = env_bool("RABBITMQ_USE_TLS", False)

PREFETCH = env_int("PREFETCH", 200)
DEFAULT_LEASE_SECONDS = env_int("DEFAULT_LEASE_SECONDS", 10)

# Connection tuning / resilience
RABBITMQ_HEARTBEAT = env_int("RABBITMQ_HEARTBEAT", 60)
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT = env_int("RABBITMQ_BLOCKED_CONNECTION_TIMEOUT", 30)
RABBITMQ_SOCKET_TIMEOUT = env_int("RABBITMQ_SOCKET_TIMEOUT", 10)
CONSUMER_RECONNECT_DELAY = env_int("CONSUMER_RECONNECT_DELAY", 5)

FACADE_API_KEY = require_env("FACADE_API_KEY")

# Audit
AUDIT_QUEUE = os.getenv("AUDIT_QUEUE", "Unir.Audit.ReceivedBusMessages")
SUBSCRIBER_APPLICATION = os.getenv("SUBSCRIBER_APPLICATION", "PingOneAIC")

# Backwards-compatible defaults for /v1/events/users
DEFAULT_QUEUE_USERS = os.getenv("DEFAULT_QUEUE_USERS", "Ping.CuentaModificadaSubscriptor")
DEFAULT_EVENTTYPE_USERS = os.getenv("DEFAULT_EVENTTYPE_USERS", "CuentaModificada")

# READ-by-id cache (supports IDM console detail view)
READ_CACHE_TTL_SECONDS = env_int("READ_CACHE_TTL_SECONDS", 600)  # 10 minutes
READ_CACHE_MAX_ITEMS = env_int("READ_CACHE_MAX_ITEMS", 5000)


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
# Helpers
# ----------------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_headers(properties: pika.BasicProperties) -> Dict[str, Any]:
    """
    Client doc expects headers: application, key, action, accountId (optional).
    We expose canonical keys regardless of original casing.
    """
    raw = dict(properties.headers or {})

    def pick(k: str) -> Any:
        if k in raw:
            return raw.get(k)
        if k.lower() in raw:
            return raw.get(k.lower())
        if k.upper() in raw:
            return raw.get(k.upper())
        return None

    return {
        **raw,
        "application": pick("application"),
        "key": pick("key"),
        "action": pick("action"),
        "accountId": pick("accountId"),
    }


def compute_event_id(
    *,
    event_type: str,
    headers: Dict[str, Any],
    payload: Optional[Dict[str, Any]],
    received_at_utc: str,
    properties: pika.BasicProperties,
) -> str:
    # 1) header key (client-preferred)
    if headers.get("key"):
        return str(headers["key"])

    # 2) message_id
    if getattr(properties, "message_id", None):
        return str(properties.message_id)

    # 3) best-effort deterministic fallbacks
    try:
        if event_type == "ClienteModificado" and payload and "IdCliente" in payload:
            return f"BUSKEY:{event_type}:{payload['IdCliente']}:{received_at_utc}"
        if (
            event_type == "MatriculaGestorRealizada"
            and payload
            and "IdMatricula" in payload
            and "FechaCambioEstado" in payload
        ):
            return f"BUSKEY:{event_type}:{payload['IdMatricula']}:{payload['FechaCambioEstado']}"
    except Exception:
        pass

    return str(uuid.uuid4())


# ----------------------------
# RabbitMQ publish connection (separate from consumers; guarded with a lock)
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


def publish_audit_message(
    *,
    key: str,
    name: str,
    sender: str,
    account_id: str,
    content: str,
    exception_message: Optional[str] = None,
) -> None:
    """
    Publish to Unir.Audit.ReceivedBusMessages directly to the queue (default exchange).
    """
    audit_obj: Dict[str, Any] = {
        "Action": "REGISTER",
        "DateTime": utc_now_iso(),
        "Key": key,
        "Name": name,
        "Application": SUBSCRIBER_APPLICATION,
        "Sender": sender or "UNKNOWN",
        "AccountId": account_id or "",
        "Content": content,
    }
    if exception_message:
        audit_obj["ExceptionMessage"] = exception_message

    ch = get_publish_channel()
    body = json.dumps(audit_obj, ensure_ascii=False).encode("utf-8")

    # Publish direct-to-queue using default exchange.
    ch.basic_publish(
        exchange="",
        routing_key=AUDIT_QUEUE,
        body=body,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
    )


# ----------------------------
# Multi-queue consumer workers
# ----------------------------
@dataclass
class BufferedMessage:
    delivery_tag: int
    properties: pika.BasicProperties
    body: bytes
    received_at_utc: str


class ConsumerWorker:
    """
    One long-lived consumer per queue.
    Buffers messages in memory and supports thread-safe ack/nack.
    """

    def __init__(self, queue_name: str):
        self.queue = queue_name
        self._buffer_lock = threading.Lock()
        self._buffer: List[BufferedMessage] = []
        self._ready = threading.Event()
        self._stop = threading.Event()

        self._conn: Optional[pika.BlockingConnection] = None
        self._ch: Optional[pika.channel.Channel] = None

        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"rabbit-consumer:{queue_name}",
        )

    def start(self):
        if not self._thread.is_alive():
            self._thread.start()
        self._ready.wait(timeout=10)

    def pop_batch(self, limit: int) -> List[BufferedMessage]:
        with self._buffer_lock:
            batch = self._buffer[:limit]
            self._buffer = self._buffer[limit:]
            return batch

    def ack(self, delivery_tag: int):
        if not (self._conn and self._ch and self._conn.is_open and self._ch.is_open):
            raise HTTPException(status_code=503, detail="RabbitMQ worker not ready")
        try:
            self._conn.add_callback_threadsafe(
                lambda: self._ch.basic_ack(delivery_tag=delivery_tag, multiple=False)
            )
        except pika.exceptions.ConnectionWrongStateError:
            raise HTTPException(status_code=409, detail="Lease channel closed; event will be redelivered")

    def nack(self, delivery_tag: int, requeue: bool = True):
        if not (self._conn and self._ch and self._conn.is_open and self._ch.is_open):
            raise HTTPException(status_code=503, detail="RabbitMQ worker not ready")
        try:
            self._conn.add_callback_threadsafe(
                lambda: self._ch.basic_nack(delivery_tag=delivery_tag, multiple=False, requeue=requeue)
            )
        except pika.exceptions.ConnectionWrongStateError:
            raise HTTPException(status_code=409, detail="Lease channel closed; event will be redelivered")

    def _on_message(self, ch, method, properties, body):
        try:
            received_at_utc = utc_now_iso()
            bm = BufferedMessage(
                delivery_tag=method.delivery_tag,
                properties=properties,
                body=body,
                received_at_utc=received_at_utc,
            )
            with self._buffer_lock:
                self._buffer.append(bm)

            logger.info("Buffered message queue=%s deliveryTag=%s", self.queue, method.delivery_tag)
        except Exception:
            logger.exception("Failed buffering message; nack requeue=true")
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)
            except Exception:
                pass

    def _run(self):
        logger.info("Starting consumer worker queue=%s", self.queue)
        while not self._stop.is_set():
            connection = None
            channel = None
            try:
                params = pika.URLParameters(RABBIT_URL)
                params.heartbeat = RABBITMQ_HEARTBEAT
                params.blocked_connection_timeout = RABBITMQ_BLOCKED_CONNECTION_TIMEOUT
                params.socket_timeout = RABBITMQ_SOCKET_TIMEOUT

                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.basic_qos(prefetch_count=PREFETCH)

                # Passive declare: fail fast if queue not present in client env
                channel.queue_declare(queue=self.queue, passive=True)

                self._conn = connection
                self._ch = channel
                self._ready.set()

                channel.basic_consume(queue=self.queue, on_message_callback=self._on_message, auto_ack=False)
                logger.info("Consuming queue=%s", self.queue)
                channel.start_consuming()

            except Exception:
                logger.exception("Worker crashed queue=%s; reconnect in %ss", self.queue, CONSUMER_RECONNECT_DELAY)
                self._ready.set()
            finally:
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
                self._conn = None
                self._ch = None
                time.sleep(CONSUMER_RECONNECT_DELAY)


WORKERS_LOCK = threading.RLock()
WORKERS: Dict[str, ConsumerWorker] = {}


def get_worker(queue: str) -> ConsumerWorker:
    with WORKERS_LOCK:
        w = WORKERS.get(queue)
        if w:
            return w
        w = ConsumerWorker(queue)
        WORKERS[queue] = w
        w.start()
        return w


# ----------------------------
# Pending store (leased events)
# ----------------------------
@dataclass
class PendingRecord:
    queue: str
    event_type: str
    delivery_tag: int
    headers: Dict[str, Any]
    raw_content: str
    payload: Optional[Dict[str, Any]]
    received_at_utc: str
    reserved_until_ms: Optional[int] = None
    batch_id: Optional[str] = None


pending: Dict[str, PendingRecord] = {}  # _id -> record
batches: Dict[str, Set[str]] = {}  # batchId -> set(_id)
lock = threading.RLock()


# ----------------------------
# READ-by-id cache (post-ack/fail support)
# ----------------------------
@dataclass
class CachedEvent:
    event: Dict[str, Any]
    expires_at_ms: int


recent: Dict[str, CachedEvent] = {}  # eventId -> CachedEvent


def purge_recent_locked() -> None:
    t = now_ms()
    for k, v in list(recent.items()):
        if v.expires_at_ms <= t:
            recent.pop(k, None)


def cache_put(event_id: str, event_obj: Dict[str, Any]) -> None:
    """Cache event for read-by-id after it leaves pending (ACK/FAIL)."""
    if READ_CACHE_TTL_SECONDS <= 0:
        return
    expires = now_ms() + READ_CACHE_TTL_SECONDS * 1000
    with lock:
        # Size bound: purge expired, then drop arbitrary keys until under limit.
        purge_recent_locked()
        while len(recent) >= READ_CACHE_MAX_ITEMS and recent:
            recent.pop(next(iter(recent.keys())), None)
        recent[event_id] = CachedEvent(event=event_obj, expires_at_ms=expires)


def purge_expired_leases() -> None:
    t = now_ms()
    expired: List[Tuple[str, PendingRecord]] = []
    with lock:
        for event_id, rec in list(pending.items()):
            if rec.reserved_until_ms is not None and rec.reserved_until_ms <= t:
                expired.append((event_id, rec))
                # remove from pending; will be requeued
                pending.pop(event_id, None)
                if rec.batch_id and rec.batch_id in batches:
                    batches[rec.batch_id].discard(event_id)

        # drop empty batches
        for bid in list(batches.keys()):
            if not batches[bid]:
                batches.pop(bid, None)

    # Requeue expired leases (nack requeue=true)
    for event_id, rec in expired:
        try:
            worker = get_worker(rec.queue)
            worker.nack(rec.delivery_tag, requeue=True)
        except Exception:
            # if cannot nack, it will eventually requeue on connection close
            pass


def format_event_for_api(event_id: str, rec: PendingRecord) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "_id": event_id,
        "__UID__": event_id,
        "__NAME__": event_id,
        "batchId": rec.batch_id,
        "eventType": rec.event_type,
        "sourceQueue": rec.queue,
        "receivedAt": rec.received_at_utc,
        "headers": {
            "key": rec.headers.get("key"),
            "application": rec.headers.get("application"),
            "accountId": rec.headers.get("accountId"),
            "action": rec.headers.get("action"),
        },
        "rawContent": rec.raw_content,
    }
    if rec.payload is not None:
        item["data"] = rec.payload
    return item


# Lease janitor (also purges read cache)
def lease_janitor_thread() -> None:
    while True:
        try:
            purge_expired_leases()
            with lock:
                purge_recent_locked()
        except Exception:
            logger.exception("Lease janitor error")
        time.sleep(1)


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


class FailByIdRequest(BaseModel):
    eventId: str
    exceptionMessage: str
    requeue: bool = True


# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI(title="RabbitMQ Facade (multi-queue poll/ack + audit)", version="2.1.0")


@app.on_event("startup")
def startup():
    # Start lease janitor
    t = threading.Thread(target=lease_janitor_thread, daemon=True, name="lease-janitor")
    t.start()
    logger.info("Facade startup complete. Multi-queue mode enabled.")


# ----------------------------
# REST endpoints
# ----------------------------
@app.get("/health")
def health():
    with lock:
        by_queue: Dict[str, int] = {}
        by_type: Dict[str, int] = {}
        for rec in pending.values():
            by_queue[rec.queue] = by_queue.get(rec.queue, 0) + 1
            by_type[rec.event_type] = by_type.get(rec.event_type, 0) + 1

        recent_count = len(recent)

    with WORKERS_LOCK:
        workers_state = {
            q: {
                "threadAlive": w._thread.is_alive(),
                "ready": w._ready.is_set(),
                "buffered": len(w._buffer),
            }
            for q, w in WORKERS.items()
        }

    return {
        "ok": True,
        "pending": len(pending),
        "recentCache": recent_count,
        "byQueue": by_queue,
        "byType": by_type,
        "workers": workers_state,
        "readCacheTtlSeconds": READ_CACHE_TTL_SECONDS,
        "readCacheMaxItems": READ_CACHE_MAX_ITEMS,
    }


@app.get("/v1/events/by-id/{eventId}")
def get_event_by_id(eventId: str, _: None = Depends(require_api_key)):
    # 1) pending first
    with lock:
        rec = pending.get(eventId)
        if rec is not None:
            return format_event_for_api(eventId, rec)

        # 2) recent cache (post-ack/fail)
        ce = recent.get(eventId)
        if ce is not None and ce.expires_at_ms > now_ms():
            return ce.event

    raise HTTPException(status_code=404, detail="Event not found")


@app.get("/v1/events")
def get_events(
    queue: str = Query(..., description="RabbitMQ queue name to consume from"),
    eventType: str = Query(..., description="Logical event type name (used for audit Name)"),
    limit: int = Query(default=100, ge=1, le=500),
    leaseSeconds: int = Query(default=DEFAULT_LEASE_SECONDS, ge=1, le=3600),
    includeData: bool = Query(default=True),
    _: None = Depends(require_api_key),
):
    """
    Queue-driven polling endpoint (new).
    Returns a list of normalized items:
      - _id for ack-by-id
      - eventType, sourceQueue
      - headers (canonical fields)
      - data (deserialized JSON object)
      - rawContent (string)
    """
    purge_expired_leases()

    worker = get_worker(queue)
    lease_expires_ms = now_ms() + leaseSeconds * 1000

    results: List[Dict[str, Any]] = []
    batch_id = "b_" + uuid.uuid4().hex[:8]

    buffered = worker.pop_batch(limit)

    for bm in buffered:
        raw_text = bm.body.decode("utf-8", errors="replace")
        headers = safe_headers(bm.properties)

        payload: Optional[Dict[str, Any]] = None
        try:
            payload = json.loads(raw_text)
            if not isinstance(payload, dict):
                raise ValueError("Message JSON is not an object")
        except Exception as e:
            # Client requirement: audit deserialization error
            key_for_audit = str(headers.get("key") or getattr(bm.properties, "message_id", None) or str(uuid.uuid4()))
            try:
                publish_audit_message(
                    key=key_for_audit,
                    name=eventType,
                    sender=str(headers.get("application") or "UNKNOWN"),
                    account_id=str(headers.get("accountId") or ""),
                    content=raw_text,
                    exception_message=f"JSON deserialization error: {e}",
                )
            except Exception:
                logger.exception("Failed to publish audit (deserialization error)")

            # Requeue by default (safe)
            try:
                worker.nack(bm.delivery_tag, requeue=True)
            except Exception:
                pass
            continue

        event_id = compute_event_id(
            event_type=eventType,
            headers=headers,
            payload=payload,
            received_at_utc=bm.received_at_utc,
            properties=bm.properties,
        )

        rec = PendingRecord(
            queue=queue,
            event_type=eventType,
            delivery_tag=bm.delivery_tag,
            headers=headers,
            raw_content=raw_text,
            payload=payload,
            received_at_utc=bm.received_at_utc,
            reserved_until_ms=lease_expires_ms,
            batch_id=batch_id,
        )

        with lock:
            # Avoid clobber on duplicate event_id
            if event_id in pending:
                event_id = f"{event_id}:{uuid.uuid4()}"
            pending[event_id] = rec
            batches.setdefault(batch_id, set()).add(event_id)

        item: Dict[str, Any] = {
            "_id": event_id,
            "__UID__": event_id,
            "__NAME__": event_id,
            "batchId": batch_id,
            "eventType": eventType,
            "sourceQueue": queue,
            "receivedAt": bm.received_at_utc,
            "headers": {
                "key": headers.get("key"),
                "application": headers.get("application"),
                "accountId": headers.get("accountId"),
                "action": headers.get("action"),
            },
            "rawContent": raw_text,
        }
        if includeData:
            item["data"] = payload

        results.append(item)

    if not results:
        return Response(status_code=204)

    logger.info(
        "Issued batch=%s results=%d queue=%s eventType=%s leaseSeconds=%d",
        batch_id,
        len(results),
        queue,
        eventType,
        leaseSeconds,
    )

    # Keep same “paging wrapper” style as /v1/events/users for connector compatibility
    return {
        "result": results,
        "resultCount": len(results),
        "pagedResultsCookie": None,
        "totalPagedResultsPolicy": "NONE",
        "totalPagedResults": -1,
        "remainingPagedResults": -1,
    }


@app.get("/v1/events/users", response_model=None)
def get_user_events(
    limit: int = Query(default=100, ge=1, le=500),
    leaseSeconds: int = Query(default=DEFAULT_LEASE_SECONDS, ge=1, le=3600),
    # old param kept for backward compatibility; used as eventType now
    eventName: Optional[str] = Query(default=DEFAULT_EVENTTYPE_USERS),
    includeData: bool = Query(default=True),
    # NEW: allow specifying queue; default to CuentaModificada subscriber queue
    queue: str = Query(default=DEFAULT_QUEUE_USERS),
    _: None = Depends(require_api_key),
):
    """
    Backwards compatible alias used by the existing connector.
    In the new client model, selection is by queue, not filtering inside a shared queue.
    """
    return get_events(
        queue=queue,
        eventType=eventName or DEFAULT_EVENTTYPE_USERS,
        limit=limit,
        leaseSeconds=leaseSeconds,
        includeData=includeData,
        _=None,
    )


@app.post("/v1/publish")
def publish_message(req: PublishRequest, _: None = Depends(require_api_key)):
    """
    Business publish helper (unchanged semantics):
    - If exchange == "", publishes direct-to-queue (routingKey must be queue name)
    - Otherwise publishes to exchange with routingKey
    """
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

        logger.info(
            "Published message exchange=%s routingKey=%s messageId=%s",
            req.exchange,
            req.routingKey,
            req.messageId,
        )
        return {"ok": True}
    except Exception:
        logger.exception("Publish failed")
        raise


@app.post("/v1/events/ack-by-id")
def ack_event_by_id(req: AckByIdRequest, _: None = Depends(require_api_key)):
    """
    ACK a single event by its _id, and publish success audit record.
    Idempotent: if not found, return 404 (same as previous) OR you can change to ok.
    """
    with lock:
        rec = pending.get(req.eventId)
        if rec is None:
            raise HTTPException(status_code=404, detail="Event not found or already ACKed")

        # Cache for read-by-id UX before removing from pending
        cache_put(req.eventId, format_event_for_api(req.eventId, rec))

        # remove from pending first (so concurrent requests don’t double-ack)
        pending.pop(req.eventId, None)
        if rec.batch_id and rec.batch_id in batches:
            batches[rec.batch_id].discard(req.eventId)
            if not batches[rec.batch_id]:
                batches.pop(rec.batch_id, None)

    worker = get_worker(rec.queue)
    worker.ack(rec.delivery_tag)

    # Publish success audit
    try:
        key_for_audit = str(rec.headers.get("key") or req.eventId)
        publish_audit_message(
            key=key_for_audit,
            name=rec.event_type,
            sender=str(rec.headers.get("application") or "UNKNOWN"),
            account_id=str(rec.headers.get("accountId") or ""),
            content=rec.raw_content,
            exception_message=None,
        )
    except Exception:
        logger.exception("Failed to publish audit (success)")

    logger.info("ACK by id=%s queue=%s type=%s", req.eventId, rec.queue, rec.event_type)
    return {"acked": req.eventId}


@app.post("/v1/events/fail-by-id")
def fail_event_by_id(req: FailByIdRequest, _: None = Depends(require_api_key)):
    """
    Mark processing failure:
    - Publish audit with ExceptionMessage
    - NACK the original message (requeue configurable)
    """
    with lock:
        rec = pending.get(req.eventId)
        if rec is None:
            raise HTTPException(status_code=404, detail="Event not found or already completed")

        # Cache for read-by-id UX before removing from pending
        cache_put(req.eventId, format_event_for_api(req.eventId, rec))

        pending.pop(req.eventId, None)
        if rec.batch_id and rec.batch_id in batches:
            batches[rec.batch_id].discard(req.eventId)
            if not batches[rec.batch_id]:
                batches.pop(rec.batch_id, None)

    # Audit failure
    try:
        key_for_audit = str(rec.headers.get("key") or req.eventId)
        publish_audit_message(
            key=key_for_audit,
            name=rec.event_type,
            sender=str(rec.headers.get("application") or "UNKNOWN"),
            account_id=str(rec.headers.get("accountId") or ""),
            content=rec.raw_content,
            exception_message=req.exceptionMessage,
        )
    except Exception:
        logger.exception("Failed to publish audit (failure)")

    worker = get_worker(rec.queue)
    worker.nack(rec.delivery_tag, requeue=req.requeue)

    logger.warning(
        "FAIL by id=%s queue=%s type=%s requeue=%s",
        req.eventId,
        rec.queue,
        rec.event_type,
        req.requeue,
    )
    return {"failed": req.eventId, "requeued": req.requeue}


@app.post("/v1/events/acks", response_model=AckResponse)
def ack_events(req: AckRequest, _: None = Depends(require_api_key)):
    """
    Batch ACK endpoint kept for compatibility.
    Will ACK only those events that belong to the provided batchId.
    """
    acked: List[str] = []
    ignored: List[str] = []

    # Group by queue for efficient ack calls
    by_queue: Dict[str, List[Tuple[str, PendingRecord]]] = {}

    with lock:
        for event_id in req.eventIds:
            rec = pending.get(event_id)
            if rec is None or rec.batch_id != req.batchId:
                ignored.append(event_id)
                continue

            # Cache for read-by-id UX before removing from pending
            cache_put(event_id, format_event_for_api(event_id, rec))

            pending.pop(event_id, None)
            acked.append(event_id)
            by_queue.setdefault(rec.queue, []).append((event_id, rec))

        batches.pop(req.batchId, None)

    for queue, items in by_queue.items():
        worker = get_worker(queue)
        for event_id, rec in items:
            worker.ack(rec.delivery_tag)
            # success audit
            try:
                key_for_audit = str(rec.headers.get("key") or event_id)
                publish_audit_message(
                    key=key_for_audit,
                    name=rec.event_type,
                    sender=str(rec.headers.get("application") or "UNKNOWN"),
                    account_id=str(rec.headers.get("accountId") or ""),
                    content=rec.raw_content,
                    exception_message=None,
                )
            except Exception:
                logger.exception("Failed to publish audit (success) batch item")

    logger.info("ACK batch=%s acked=%d ignored=%d", req.batchId, len(acked), len(ignored))
    return AckResponse(acked=acked, ignored=ignored)


@app.post("/v1/events/nacks")
def nack_events(req: NackRequest, _: None = Depends(require_api_key)):
    """
    Batch NACK endpoint kept for compatibility.
    Does NOT publish audit (use /fail-by-id if you want ExceptionMessage).
    """
    to_nack: Dict[str, List[int]] = {}

    with lock:
        for event_id in req.eventIds:
            rec = pending.get(event_id)
            if rec is None or rec.batch_id != req.batchId:
                continue
            pending.pop(event_id, None)
            to_nack.setdefault(rec.queue, []).append(rec.delivery_tag)

        batches.pop(req.batchId, None)

    for queue, delivery_tags in to_nack.items():
        worker = get_worker(queue)
        for dt in delivery_tags:
            worker.nack(dt, requeue=req.requeue)

    logger.warning("NACK batch=%s events=%d requeue=%s", req.batchId, len(req.eventIds), req.requeue)
    return {"ok": True}


@app.on_event("shutdown")
def shutdown():
    global publish_conn, publish_ch
    # Stop workers (best effort)
    with WORKERS_LOCK:
        for w in WORKERS.values():
            try:
                w._stop.set()
            except Exception:
                pass

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


# Optional: convenience for `python facade.py`
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
