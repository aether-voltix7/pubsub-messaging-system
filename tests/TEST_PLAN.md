# TEST_PLAN.md — M5 QA plan (Gaitano)

**Status:** done · **Owner:** Gaitano (M5 QA) · **System:** pub/sub messaging system (M1 broker + M2 SQLite store + M3 subscriber + M4 publisher/demo, all merged)

Everything runs from the repository root with **standard library only**:

```
python -m unittest tests.test_routing tests.test_faults -v   # M5 suites
python -m unittest discover -s tests -t . -v                 # full suite
python -m unittest tests.test_storage_contract -v            # M2 contract tests
```

## Scope

M5 verifies the delivery guarantees of PROTOCOL.md §7 end-to-end, over real
TCP sockets, with the real merged components: `broker.server.Broker`,
`storage.queue_store.SQLiteQueueStore` (M2), `subscriber.client.SubscriberClient`
(M3) and `publisher.client.PublisherClient` (M4). `tests/qa_helpers.py`
provides `RecordingSubscriber`: the real M3 client with the alert display
replaced by a recorded list, keeping the seen-ID dedup file (T5) and
ACK-after-processing (T4) behaviour.

## Test matrix

| # | Guarantee / requirement (PROTOCOL.md) | Test | Evidence in broker log |
|---|---|---|---|
| R1 | Topic filtering, no cross-topic leakage (§3) | `test_routing.test_stock_alert_scenario_end_to_end` | `PUBLISH stock-exchange [stocks.tech] msg_id=… -> tech-investor` |
| R2 | `*` matches one segment, `#` zero or more trailing (§3) | `test_routing.test_wildcard_star_and_hash` | `SUBSCRIBE etf-bot -> stocks.*` |
| R3 | UNSUBSCRIBE stops delivery; queued messages unaffected (§4) | `test_routing.test_unsubscribe_stops_delivery` | `UNSUBSCRIBE s -x crypto` |
| R4 | New subscription gets only later messages (§4) | `test_routing.test_late_subscriber_gets_only_new_messages` | `PUBLISH … -> (no subscribers)` |
| R5 | Wildcards rejected in PUBLISH; 0-recipient publish OK (§4, §5 402) | `test_routing.test_invalid_topics_rejected_no_subscribers_ok` | `ERROR to …: INVALID_TOPIC` |
| R6 | Live messages from one publisher arrive in publish order (§7.3) | `test_routing.test_ordering_per_publisher` | one `PUBLISH` line per message |
| R7 | Redelivery when no ACK within `ack_timeout` (§7.2) | `test_faults.test_lost_ack_triggers_retransmit_then_stops` | `RETRANSMIT s msg_id=m1 attempt=2 (no ACK within timeout)` |
| R8 | Catch-up: offline subscriber gets un-ACKed messages in seq order (§7.1–7.3) | `test_faults.test_offline_subscriber_catch_up_in_order` | `DETACH bank-analyst: … remain pending` → `CATCH-UP bank-analyst: 10 pending message(s)` |
| R9 | Durability: subscriptions + pending survive a broker restart (§7.6, stretch goal) | `test_faults.test_broker_restart_durability_sqlite` | `broker listening on … (1 durable subscriber(s) restored; s={'crypto'})` |
| R10 | Duplicate `msg_id` de-duplicated on retry (§4 PUBLISH retry rule) | `test_faults.test_duplicate_publish_delivered_once` | `PUBLISH p dup msg_id=dup-id (already stored, not re-routed)` |
| R11 | Publisher reconnects and re-publishes after broker restart (M4) | `test_faults.test_publisher_client_reconnects_after_broker_restart` | `CONNECT stock-exchange as publisher …` |
| R12 | Session takeover: newest connection wins (§2.3) | `test_faults.test_session_takeover_same_client_id` | `TAKEOVER s: closing previous connection s@…` |
| R13 | Flow control: ≤ `max_inflight` un-ACKed DELIVERs (§7.5) | `test_faults.test_flow_control_window` | (queue wait is silent; observable via DELIVER count) |

Existing suites that M5 builds on (Jainar): `tests/test_broker.py` (27 broker
tests over raw sockets) and `tests/test_storage_contract.py` (contract every
`QueueStore` must pass, now running on the merged SQLite store).

## Fault-injection recipes (as shown in the demo)

* **Lost ACK:** run the broker with a short timeout (`python -m broker --store sqlite --ack-timeout 1`) and a subscriber that skips the ACK; observe `RETRANSMIT … attempt=n` and the doubling backoff.
* **Offline catch-up:** kill `bank-analyst` (Ctrl+C), keep both feeds running, restart it with the same `--id`; observe `DETACH … remain pending` then `CATCH-UP … n pending` and `[BACKLOG]` tags in the subscriber.
* **Broker restart (stretch goal):** run with `--store sqlite`, stop the broker, start it again on the same db; durable subscriptions are restored and the backlog is flushed on reconnect.
* **Duplicate publish:** resend a PUBLISH with the same `msg_id` (e.g. after a lost PUBACK); the broker replies `PUBACK duplicate:true` and the subscriber sees the alert once.

## Known limitations / risks

* Tests run against `localhost` only; no network-partition simulation beyond socket kills.
* Retransmit timing depends on wall clocks (0.5 s base timeout in tests); generous wait windows keep them stable, but a heavily loaded CI box could need longer timeouts.
* `logging` is set to `CRITICAL` in `test_faults.py` to keep output clean; set it to `INFO` to reproduce the evidence lines for the fault report.
