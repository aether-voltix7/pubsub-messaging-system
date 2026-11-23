# M2: Durable Queue Storage — SQLite Schema Design

**Author:** William Labrador Jr.  
**Module:** M2 — Durable Queue Storage Layer (`storage/queue_store.py`)  
**Interface Contract:** `storage/interface.py` (`QueueStore`)

## 1. Overview
This document defines the SQLite database schema backing `SQLiteQueueStore(db_path)`. Its purpose is to persist published messages, durable topic subscriptions, and per-subscriber delivery states on disk. By committing messages to durable storage, the broker achieves **time uncoupling** (Coulouris et al., Ch. 6.1, pp. 230–232) and reliable **at-least-once delivery** (Ch. 6.4.1, p. 255), ensuring offline subscribers receive their missed backlog in order upon reconnection.

## 2. Table Definitions

### 2.1 `messages`
Stores each published message once to avoid duplicating payloads across multiple subscribers.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `seq` | `INTEGER` | `PRIMARY KEY AUTOINCREMENT` | Monotonically increasing insertion order used for FIFO catch-up. |
| `msg_id` | `TEXT` | `UNIQUE NOT NULL` | Publisher-generated UUID. Enforces idempotency on retried `PUBLISH` requests. |
| `topic` | `TEXT` | `NOT NULL` | Topic name (e.g., `stocks.tech`, `stocks.banking`, `crypto`). |
| `payload` | `TEXT` | `NOT NULL` | JSON-encoded string (`json.dumps`) so Python `dict`, `int`, and `None` payloads round-trip cleanly. |
| `publisher_id` | `TEXT` | `NOT NULL` | Identifier of the publishing client. |
| `timestamp` | `INTEGER` | `NOT NULL` | Integer timestamp supplied by the publisher/broker. |

### 2.2 `subscriptions`
Persists durable client subscriptions so they survive a broker restart (Task L4).

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `client_id` | `TEXT` | `NOT NULL` | Persistent subscriber identifier. |
| `pattern` | `TEXT` | `NOT NULL` | Subscribed topic or pattern. |

* **Primary Key:** `(client_id, pattern)` — guarantees idempotency if a client subscribes to the same pattern twice.

### 2.3 `deliveries`
Tracks the delivery and acknowledgement status of each message for each matched subscriber.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `client_id` | `TEXT` | `NOT NULL` | Target subscriber's persistent `client_id`. |
| `msg_id` | `TEXT` | `NOT NULL`, `FOREIGN KEY (msg_id) REFERENCES messages(msg_id)` | Reference to the stored message. |
| `acked` | `INTEGER` | `NOT NULL DEFAULT 0` | `0` = pending/unacknowledged; `1` = acknowledged. |

* **Primary Key:** `(client_id, msg_id)` — ensures at most one delivery state row per subscriber per message.

---

## 3. Entity Relationships

```text
+-------------------+          +-------------------+
|     messages      | 1      * |    deliveries     |
|-------------------|----------|-------------------|
| PK seq            |          | PK client_id      |
| UQ msg_id         |<---------| PK,FK msg_id      |
|    topic          |          |    acked (0 or 1) |
|    payload (JSON) |          +-------------------+
|    publisher_id   |
|    timestamp      |          +-------------------+
+-------------------+          |   subscriptions   |
                               |-------------------|
                               | PK client_id      |
                               | PK pattern        |
                               +-------------------+
```

---

## 4. Indexes, Pragmas & Verification

* **Index:** `idx_deliveries_pending ON deliveries(client_id, acked)` — speeds up `get_pending(client_id)` queries for offline catch-up.
* **Foreign Keys:** `PRAGMA foreign_keys = ON` — enforces referential integrity between `deliveries.msg_id` and `messages.msg_id`.
* **Write-Ahead Logging:** `PRAGMA journal_mode = WAL` — improves read/write concurrency and crash safety.
* **Thread Safety & Atomicity:** All operations are serialized with a `threading.Lock`, and multi-statement writes (`save_message`) run inside atomic SQLite transactions (`with self._conn:`).
* **Verification:** Validated by `tests/test_storage_contract.py` (17 tests) and `tests/test_storage.py` (3 tests — ordering, ACK removal, and restart survival).