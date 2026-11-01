"""PulseDB: a compact database change streaming platform demo.

The source row and its change event are committed together. A background consumer
processes events in sequence, stores an idempotency key, and advances a durable
checkpoint in the same transaction as the downstream read model.
"""
from __future__ import annotations

import json
import os
import sqlite3
import statistics
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).parent
DATABASE = os.getenv("DATABASE_PATH", str(ROOT / "pulsedb.db"))
DB_LOCK = threading.RLock()
WORKER_PAUSED = threading.Event()
WORKER_STOP = threading.Event()


@contextmanager
def db():
    connection = sqlite3.connect(DATABASE, timeout=20)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def initialize_database():
    with DB_LOCK, db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_products (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL,
                price REAL NOT NULL, stock INTEGER NOT NULL, version INTEGER NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS change_log (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT UNIQUE NOT NULL,
                event_id TEXT NOT NULL, operation TEXT NOT NULL, entity_id TEXT NOT NULL,
                version INTEGER NOT NULL, payload TEXT, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS read_products (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL,
                price REAL NOT NULL, stock INTEGER NOT NULL, version INTEGER NOT NULL,
                indexed_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL,
                latency_ms REAL NOT NULL, processed_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS consumer_checkpoint (
                consumer TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL,
                processed INTEGER NOT NULL, duplicates INTEGER NOT NULL,
                restarted INTEGER NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS activity (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, message TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            INSERT INTO consumer_checkpoint
                (consumer, last_sequence, processed, duplicates, restarted, updated_at)
            VALUES ('catalog-indexer', 0, 0, 0, 0, 0)
            ON CONFLICT(consumer) DO NOTHING;
            """
        )


def record_activity(connection, kind: str, message: str):
    connection.execute(
        "INSERT INTO activity VALUES (?, ?, ?, ?)",
        (str(uuid.uuid4()), kind, message, time.time()),
    )


def append_event(connection, operation: str, entity_id: str, version: int, payload):
    event_id = str(uuid.uuid4())
    connection.execute(
        """INSERT INTO change_log
           (delivery_id, event_id, operation, entity_id, version, payload, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            str(uuid.uuid4()), event_id, operation, entity_id, version,
            json.dumps(payload) if payload is not None else None, time.time(),
        ),
    )
    record_activity(connection, operation.lower(), f"{operation} captured for {entity_id[:8]} · v{version}")
    return event_id


class ProductCreate(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    category: str = Field(min_length=2, max_length=50)
    price: float = Field(gt=0, le=1_000_000)
    stock: int = Field(ge=0, le=1_000_000)


class ProductUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=80)
    category: str | None = Field(default=None, min_length=2, max_length=50)
    price: float | None = Field(default=None, gt=0, le=1_000_000)
    stock: int | None = Field(default=None, ge=0, le=1_000_000)


def consume_one() -> bool:
    if WORKER_PAUSED.is_set():
        return False
    with DB_LOCK, db() as connection:
        checkpoint = connection.execute(
            "SELECT * FROM consumer_checkpoint WHERE consumer='catalog-indexer'"
        ).fetchone()
        event = connection.execute(
            "SELECT * FROM change_log WHERE sequence>? ORDER BY sequence LIMIT 1",
            (checkpoint["last_sequence"],),
        ).fetchone()
        if event is None:
            return False

        existing = connection.execute(
            "SELECT 1 FROM processed_events WHERE event_id=?", (event["event_id"],)
        ).fetchone()
        if existing:
            connection.execute(
                """UPDATE consumer_checkpoint SET last_sequence=?, duplicates=duplicates+1,
                   updated_at=? WHERE consumer='catalog-indexer'""",
                (event["sequence"], time.time()),
            )
            record_activity(connection, "duplicate", f"Duplicate delivery skipped · seq {event['sequence']}")
            return True

        payload = json.loads(event["payload"]) if event["payload"] else None
        if event["operation"] == "DELETE":
            connection.execute("DELETE FROM read_products WHERE id=?", (event["entity_id"],))
        else:
            connection.execute(
                """INSERT INTO read_products VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET name=excluded.name,
                   category=excluded.category, price=excluded.price, stock=excluded.stock,
                   version=excluded.version, indexed_at=excluded.indexed_at""",
                (
                    payload["id"], payload["name"], payload["category"], payload["price"],
                    payload["stock"], payload["version"], time.time(),
                ),
            )
        latency_ms = max(0, (time.time() - event["created_at"]) * 1000)
        connection.execute(
            "INSERT INTO processed_events VALUES (?, ?, ?, ?)",
            (event["event_id"], event["sequence"], latency_ms, time.time()),
        )
        connection.execute(
            """UPDATE consumer_checkpoint SET last_sequence=?, processed=processed+1,
               updated_at=? WHERE consumer='catalog-indexer'""",
            (event["sequence"], time.time()),
        )
        record_activity(connection, "processed", f"Sequence {event['sequence']} applied to catalog-indexer")
        return True


def worker_loop():
    while not WORKER_STOP.wait(0.45):
        try:
            consume_one()
        except Exception:
            # A failed transaction leaves the checkpoint unchanged, so the event is retried.
            time.sleep(0.5)


@asynccontextmanager
async def lifespan(_app):
    initialize_database()
    WORKER_STOP.clear()
    thread = threading.Thread(target=worker_loop, name="catalog-indexer", daemon=True)
    thread.start()
    yield
    WORKER_STOP.set()
    thread.join(timeout=5)


app = FastAPI(title="PulseDB Change Streaming Platform", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")


@app.get("/")
def dashboard():
    return FileResponse(ROOT / "static" / "index.html")


@app.get("/health")
def health():
    with db() as connection:
        connection.execute("SELECT 1")
    return {"status": "healthy", "consumer": "paused" if WORKER_PAUSED.is_set() else "running"}


@app.post("/api/products", status_code=201)
def create_product(body: ProductCreate):
    product_id = str(uuid.uuid4())
    now = time.time()
    with DB_LOCK, db() as connection:
        connection.execute(
            "INSERT INTO source_products VALUES (?, ?, ?, ?, ?, 1, ?)",
            (product_id, body.name, body.category, body.price, body.stock, now),
        )
        row = dict(connection.execute("SELECT * FROM source_products WHERE id=?", (product_id,)).fetchone())
        append_event(connection, "INSERT", product_id, 1, row)
    return row


@app.patch("/api/products/{product_id}")
def update_product(product_id: str, body: ProductUpdate):
    values = body.model_dump(exclude_none=True)
    if not values:
        raise HTTPException(400, "Provide at least one field to update")
    with DB_LOCK, db() as connection:
        current = connection.execute("SELECT * FROM source_products WHERE id=?", (product_id,)).fetchone()
        if current is None:
            raise HTTPException(404, "Product not found")
        merged = dict(current)
        merged.update(values)
        merged["version"] += 1
        merged["updated_at"] = time.time()
        connection.execute(
            """UPDATE source_products SET name=?, category=?, price=?, stock=?,
               version=?, updated_at=? WHERE id=?""",
            (
                merged["name"], merged["category"], merged["price"], merged["stock"],
                merged["version"], merged["updated_at"], product_id,
            ),
        )
        append_event(connection, "UPDATE", product_id, merged["version"], merged)
    return merged


@app.delete("/api/products/{product_id}", status_code=204)
def delete_product(product_id: str):
    with DB_LOCK, db() as connection:
        current = connection.execute("SELECT * FROM source_products WHERE id=?", (product_id,)).fetchone()
        if current is None:
            raise HTTPException(404, "Product not found")
        version = current["version"] + 1
        connection.execute("DELETE FROM source_products WHERE id=?", (product_id,))
        append_event(connection, "DELETE", product_id, version, None)


@app.post("/api/consumer/{action}")
def consumer_action(action: str):
    if action == "pause":
        WORKER_PAUSED.set()
        with db() as connection:
            record_activity(connection, "system", "Consumer paused; incoming events will accumulate")
    elif action == "resume":
        WORKER_PAUSED.clear()
        with db() as connection:
            record_activity(connection, "system", "Consumer resumed from its durable checkpoint")
    elif action == "restart":
        WORKER_PAUSED.clear()
        with DB_LOCK, db() as connection:
            connection.execute(
                """UPDATE consumer_checkpoint SET restarted=restarted+1, updated_at=?
                   WHERE consumer='catalog-indexer'""", (time.time(),)
            )
            record_activity(connection, "system", "Consumer restart simulated; checkpoint recovered")
    elif action == "duplicate":
        with DB_LOCK, db() as connection:
            latest = connection.execute("SELECT * FROM change_log ORDER BY sequence DESC LIMIT 1").fetchone()
            if latest is None:
                raise HTTPException(409, "Create a change event first")
            connection.execute(
                """INSERT INTO change_log
                   (delivery_id, event_id, operation, entity_id, version, payload, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(uuid.uuid4()), latest["event_id"], latest["operation"],
                    latest["entity_id"], latest["version"], latest["payload"], time.time(),
                ),
            )
            record_activity(connection, "duplicate", "Duplicate delivery injected for idempotency test")
    elif action == "rebuild":
        with DB_LOCK, db() as connection:
            connection.execute("DELETE FROM read_products")
            connection.execute("DELETE FROM processed_events")
            connection.execute(
                """UPDATE consumer_checkpoint SET last_sequence=0, processed=0,
                   duplicates=0, updated_at=? WHERE consumer='catalog-indexer'""", (time.time(),)
            )
            record_activity(connection, "system", "Read model cleared; snapshot replay started")
        WORKER_PAUSED.clear()
    else:
        raise HTTPException(400, "Unsupported consumer action")
    return {"action": action, "paused": WORKER_PAUSED.is_set()}


@app.get("/api/state")
def state():
    with db() as connection:
        checkpoint = dict(connection.execute("SELECT * FROM consumer_checkpoint").fetchone())
        max_sequence = connection.execute("SELECT COALESCE(MAX(sequence), 0) AS n FROM change_log").fetchone()["n"]
        latencies = [row["latency_ms"] for row in connection.execute("SELECT latency_ms FROM processed_events")]
        p95 = 0
        if latencies:
            ordered = sorted(latencies)
            p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
        throughput = connection.execute(
            "SELECT COUNT(*) AS n FROM processed_events WHERE processed_at>?", (time.time() - 60,)
        ).fetchone()["n"]
        return {
            "paused": WORKER_PAUSED.is_set(),
            "checkpoint": checkpoint,
            "lag": max(0, max_sequence - checkpoint["last_sequence"]),
            "max_sequence": max_sequence,
            "p95_ms": round(p95, 1),
            "throughput": throughput,
            "source": [dict(row) for row in connection.execute("SELECT * FROM source_products ORDER BY updated_at DESC")],
            "read_model": [dict(row) for row in connection.execute("SELECT * FROM read_products ORDER BY indexed_at DESC")],
            "events": [dict(row) for row in connection.execute("SELECT * FROM change_log ORDER BY sequence DESC LIMIT 20")],
            "activity": [dict(row) for row in connection.execute("SELECT * FROM activity ORDER BY created_at DESC LIMIT 20")],
        }
