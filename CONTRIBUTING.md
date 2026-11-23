# Contributing, Integration Contracts & Merge Review (J2 / J7 / J8)

Team lead and integrator: **Kristopher Edz Jainar**. Every merge into `main` needs Jainar's review sign-off.

## Repository layout

| Path | Module | Owner | Status |
|---|---|---|---|
| `PROTOCOL.md`, `common/protocol.py` | M1 protocol spec + shared framing and validation | Jainar | done, frozen v1 |
| `broker/server.py` | M1 multi-client TCP server (J3) | Jainar | done |
| `broker/router.py` | M1 subscription registry + topic routing (J4) | Jainar | done |
| `broker/delivery.py` | M1 delivery/ACK/retransmit engine + catch-up (J5, J6) | Jainar | done |
| `storage/interface.py` | M1↔M2 storage contract (J2) | Jainar | done |
| `storage/memory_store.py` | In-memory reference store, used for tests and as a stand-in | Jainar | done |
| `storage/queue_store.py` | **M2 SQLite durable store** (`SQLiteQueueStore(db_path)`) | Labrador | done |
| `subscriber/client.py` | **M3 subscriber client** | Tortuya | done |
| `publisher/client.py` | **M4 publisher client** | Jurquina | done |
| `demo/` | **M4 feeds + demo launcher** | Jurquina | done |
| `tests/test_broker.py` | Broker tests over real sockets | Jainar | done (27 tests) |
| `tests/test_storage_contract.py` | Tests every QueueStore must pass | Jainar | done (runs on SQLite automatically once it exists) |
| `tests/TEST_PLAN.md`, `test_routing.py`, `test_faults.py` | **M5 QA** (end-to-end, real M2/M3/M4 clients; uses `tests/qa_helpers.py`) | Gaitano | done (13 tests) |

Run everything from the repository root (imports are absolute, such as `from common import protocol`):

```
python -m unittest discover -s tests -t . -v
python -m broker --store memory          # volatile: queues lost on restart
python -m broker --store sqlite --db broker.db
```

## What each module must do to integrate

### M2 Storage (Labrador)
* Create `storage/queue_store.py` with `class SQLiteQueueStore(QueueStore)` and constructor `SQLiteQueueStore(db_path: str)`.
* Implement **every** abstract method in `storage/interface.py`. The tracker lists four; the broker also needs `remove_subscription`, `load_subscriptions` and `close`.
* `save_message` must be one transaction that inserts the message and one delivery row per subscriber, committed before it returns. It must be **idempotent on msg_id** (`INSERT OR IGNORE`, then return `cursor.rowcount == 1`).
* `get_pending` must return rows ordered by `seq` (use `INTEGER PRIMARY KEY AUTOINCREMENT` on `messages`), with `seq` filled in.
* Thread safety: the broker calls the store from many threads. Either use one `sqlite3` connection with `check_same_thread=False` plus a `threading.Lock`, or one connection per thread. Enabling `PRAGMA journal_mode=WAL` is recommended.
* **Done when** `python -m unittest tests.test_storage_contract -v` shows the `SQLiteStoreContract` tests passing (not skipped), including `test_survives_restart`.

### M3 Subscriber (Tortuya)
* Reuse `common.protocol.FrameStream` for framing, and call `recv()` from one reader thread only.
* CONNECT with a stable `--id`. If `CONNACK.session_present` is true, subscriptions are already restored, though sending SUBSCRIBE again is harmless.
* ACK **after** displaying or logging each DELIVER. Keep a seen-`msg_id` file. If a msg_id was already seen, **ACK it but do not display it again**.
* Show `backlog` and `attempt` in the alert line, for example `[BACKLOG]` or `[RETRY 2]`. It makes the demo much clearer.

### M4 Publisher (Jurquina)
* `msg_id = common.protocol.new_msg_id()` **once per message**. Every retry reuses it.
* Wait for a `PUBACK` with a matching `msg_id`. If the timeout passes or the socket errors, reconnect and resend the **same** frame. `PUBACK.duplicate: true` is normal after a retry.
* Topics: `stocks.tech` and `stocks.banking` (Publisher A, `stock-exchange`), `crypto` (Publisher B, `crypto-feed`).
* Suggested subscriber ids: `tech-investor`, `bank-analyst`, `crypto-trader`.

### M5 QA (Gaitano)
* Use the broker's log lines as fault-report evidence. They are deliberately readable: `PUBLISH … -> recipients`, `DETACH … remain pending`, `CATCH-UP … n pending`, `RETRANSMIT … attempt=n`, `TAKEOVER`.
* To simulate a lost ACK, run the broker with `--ack-timeout 1` and use a subscriber that skips the ACK. To test the broker restart stretch goal, use `--store sqlite` and restart the broker.

## Merge review checklist (J8). Reviewer: Jainar

Copy this into the PR or merge-request description and tick each item.

- [ ] Runs from the repository root with **standard library only** (nothing new in `requirements.txt`).
- [ ] `python -m unittest discover -s tests -t .` passes with no new skips other than ones that are expected.
- [ ] Wire format matches **PROTOCOL.md** exactly: field names, types, and the `\n` framing. No new message types unless Jainar approves them.
- [ ] Publisher: same `msg_id` on retry; waits for a matching PUBACK; handles ERROR frames.
- [ ] Subscriber: ACK after processing; duplicate msg_ids are ACKed but not reprocessed; reconnects with the same id.
- [ ] Storage: commits before returning; thread-safe; contract tests pass, including restart.
- [ ] Sockets and threads are cleaned up on exit (Ctrl+C), with no traceback spam.
- [ ] Code is commented (who, what, and why for non-obvious logic). Any borrowed code is cited in the README.
- [ ] Logs are readable for the demo (one line per event, includes client_id/msg_id).
- [ ] Integration checklist items touched by this change are re-verified live.
