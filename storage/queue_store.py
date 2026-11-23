"""
SQLite-backed durable queue storage layer (M2, Labrador).

Implements the `QueueStore` interface defined in storage/interface.py as
`SQLiteQueueStore(db_path)` and is verified by tests/test_storage_contract.py
and tests/test_storage.py. Replaces the in-memory reference store so queued
messages, acknowledgements, and subscriptions survive broker restarts.

Concurrency & durability:
    - Uses SQLite WAL (Write-Ahead Logging) mode with foreign keys enabled.
    - Serializes all database operations with a `threading.Lock` so multiple
      broker threads can safely share a single connection.
    - Wraps writes in atomic transactions (`with self._conn:`), ensuring each
      message and its per-subscriber delivery rows commit together.

Schema:
    messages(seq, msg_id UNIQUE, topic, payload, publisher_id, timestamp)
    subscriptions(client_id, pattern)            PRIMARY KEY (client_id, pattern)
    deliveries(client_id, msg_id, acked)         PRIMARY KEY (client_id, msg_id)

Messages are returned in strict FIFO order via `messages.seq`. Duplicate
`msg_id` saves and duplicate ACKs are handled idempotently.
"""

from __future__ import annotations  # Enables postponed evaluation of type hints to match interface.py

import json  # Serializes/deserializes message payloads to/from JSON strings
import sqlite3  # Built-in SQLite database driver for persistent storage
import threading  # Provides thread-safe locking for concurrent broker threads
from collections import defaultdict  # Auto-creates empty sets when grouping subscriptions by client
from typing import Any, Dict, Iterable, List, Set  # Type annotations for method signatures
from storage.interface import QueueStore, StoredMessage  # Base interface and message data structure


class SQLiteQueueStore(QueueStore):  # Persistent SQLite implementation of the QueueStore interface
    def __init__(self, db_path: str) -> None:
        self._lock = threading.Lock()  # Serializes DB access so concurrent threads don't collide
        self._conn = sqlite3.connect(db_path, check_same_thread=False)  # Opens DB that allows cross-thread use under self._lock
        self._conn.execute("PRAGMA foreign_keys = ON;")  # Enforces foreign key constraints between tables
        self._conn.execute("PRAGMA journal_mode = WAL;")  # Enables Write-Ahead Logging for better concurrency and crash safety

        # Creates tables and indexes atomically if they don't already exist
        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,  -- Monotonic sequence number for strict FIFO ordering
                    msg_id TEXT UNIQUE NOT NULL,            -- Unique message identifier (enforces idempotency)
                    topic TEXT NOT NULL,                    -- Topic the message was published to
                    payload TEXT NOT NULL,                  -- JSON-encoded message payload
                    publisher_id TEXT NOT NULL,             -- ID of the publishing client
                    timestamp INTEGER NOT NULL              -- Publish timestamp
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    client_id TEXT NOT NULL,                -- Subscribing client ID
                    pattern TEXT NOT NULL,                  -- Topic filter/pattern (supports wildcards)
                    PRIMARY KEY (client_id, pattern)        -- Prevents duplicate subscriptions for the same client
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    client_id TEXT NOT NULL,                -- Target subscriber ID
                    msg_id TEXT NOT NULL,                   -- Message ID to be delivered
                    acked INTEGER NOT NULL DEFAULT 0,       -- 0 = pending, 1 = acknowledged
                    PRIMARY KEY (client_id, msg_id),        -- One delivery record per subscriber per message
                    FOREIGN KEY (msg_id) REFERENCES messages(msg_id)  -- Links delivery to its stored message
                );
                CREATE INDEX IF NOT EXISTS idx_deliveries_pending
                    ON deliveries(client_id, acked);        -- Speeds up get_pending() lookups per client
            """)

    def save_message(
        self,
        msg_id: str,
        topic: str,
        payload: Any,
        publisher_id: str,
        timestamp: int,
        subscriber_ids: Iterable[str],
    ) -> bool:
        payload_json = json.dumps(payload)  # Converts arbitrary Python payload into a JSON string for SQLite

        with self._lock:  # Acquires lock for thread safety
            with self._conn:  # Atomic transaction: both inserts commit together or roll back together
                # Inserts message; ignores if msg_id already exists (idempotent)
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO messages (msg_id, topic, payload, publisher_id, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (msg_id, topic, payload_json, publisher_id, timestamp),
                )
                if cur.rowcount == 0:  # Duplicate msg_id was ignored
                    return False  # Signals to broker that this message was already stored

                # Queues a pending delivery (acked = 0) for every matching subscriber
                self._conn.executemany(
                    "INSERT OR IGNORE INTO deliveries (client_id, msg_id, acked) VALUES (?, ?, 0)",
                    [(cid, msg_id) for cid in subscriber_ids],
                )
                return True  # Signals that the new message and its deliveries are durably saved

    def register_subscription(self, client_id: str, pattern: str) -> None:
        with self._lock:  # Acquires lock for thread safety
            with self._conn:  # Commits subscription to disk
                # Saves subscription; ignores if client is already subscribed to this pattern
                self._conn.execute(
                    "INSERT OR IGNORE INTO subscriptions (client_id, pattern) VALUES (?, ?)",
                    (client_id, pattern),
                )

    def remove_subscription(self, client_id: str, pattern: str) -> None:
        with self._lock:  # Acquires lock for thread safety
            with self._conn:  # Commits deletion to disk
                # Removes the specific client-pattern subscription
                self._conn.execute(
                    "DELETE FROM subscriptions WHERE client_id = ? AND pattern = ?",
                    (client_id, pattern),
                )

    def load_subscriptions(self) -> Dict[str, Set[str]]:
        subs: Dict[str, Set[str]] = defaultdict(set)  # Maps each client_id to a set of its topic patterns

        with self._lock:  # Acquires lock for a consistent read
            rows = self._conn.execute("SELECT client_id, pattern FROM subscriptions").fetchall()  # Fetches all stored subscriptions

            for client_id, pattern in rows:  # Groups patterns under each client_id
                subs[client_id].add(pattern)

        return dict(subs)  # Returns a plain dict matching the interface contract

    def get_pending(self, client_id: str) -> List[StoredMessage]:
        with self._lock:  # Acquires lock for a consistent read
            # Joins messages and deliveries to fetch unacked messages for this client in FIFO order
            rows = self._conn.execute(
                """
                SELECT m.msg_id, m.topic, m.payload, m.publisher_id, m.timestamp, m.seq
                FROM messages m
                JOIN deliveries d ON m.msg_id = d.msg_id
                WHERE d.client_id = ? AND d.acked = 0
                ORDER BY m.seq ASC
                """,
                (client_id,),
            ).fetchall()

        result: List[StoredMessage] = []  # Holds reconstructed StoredMessage objects
        for msg_id, topic, payload_json, publisher_id, timestamp, seq in rows:
            # Deserializes JSON payload back to Python and appends as a StoredMessage
            result.append(StoredMessage(msg_id, topic, json.loads(payload_json), publisher_id, timestamp, seq))
        return result  # Returns ordered list of pending messages

    def mark_acked(self, client_id: str, msg_id: str) -> bool:
        with self._lock:  # Acquires lock for thread safety
            with self._conn:  # Commits ACK state to disk
                # Marks delivery as acknowledged only if it is currently pending (acked = 0)
                cur = self._conn.execute(
                    "UPDATE deliveries SET acked = 1 WHERE client_id = ? AND msg_id = ? AND acked = 0",
                    (client_id, msg_id),
                )
                return cur.rowcount > 0  # True if newly acked, False if already acked or missing

    def close(self) -> None:
        with self._lock:  # Waits for any in-progress DB operation to finish
            self._conn.close()  # Cleanly closes the SQLite connection