"""
Broker (M1) tests over real TCP sockets, using the raw protocol client in
tests/helpers.py and the in-memory store. Covers J3-J6 + the integration
checklist items that belong to the broker.

    python -m unittest tests.test_broker -v

(Gaitano's tests/test_routing.py and tests/test_faults.py test the whole
system end-to-end with the real clients; these are the broker's unit/integration tests.)
"""
import logging
import time
import unittest

from broker.router import SubscriptionRegistry, topic_matches
from broker.server import Broker
from common import protocol as P
from storage.memory_store import MemoryQueueStore
from tests.helpers import RawClient

logging.basicConfig(level=logging.CRITICAL)


class TopicMatchTests(unittest.TestCase):
    def test_exact(self):
        self.assertTrue(topic_matches("stocks.tech", "stocks.tech"))
        self.assertFalse(topic_matches("stocks.tech", "stocks.banking"))
        self.assertFalse(topic_matches("stocks", "stocks.tech"))
        self.assertFalse(topic_matches("stocks.tech", "stocks"))

    def test_single_wildcard(self):
        self.assertTrue(topic_matches("stocks.*", "stocks.tech"))
        self.assertTrue(topic_matches("*.tech", "stocks.tech"))
        self.assertFalse(topic_matches("stocks.*", "stocks"))
        self.assertFalse(topic_matches("stocks.*", "stocks.tech.us"))

    def test_multi_wildcard(self):
        self.assertTrue(topic_matches("stocks.#", "stocks"))
        self.assertTrue(topic_matches("stocks.#", "stocks.tech.us"))
        self.assertTrue(topic_matches("#", "crypto"))
        self.assertFalse(topic_matches("stocks.#", "crypto"))

    def test_registry_match_dedups_overlapping_patterns(self):
        r = SubscriptionRegistry()
        r.add("s1", "stocks.tech")
        r.add("s1", "stocks.*")
        r.add("s2", "crypto")
        self.assertEqual(r.match("stocks.tech"), ["s1"])
        self.assertEqual(r.match("crypto"), ["s2"])
        self.assertEqual(r.match("stocks.banking"), ["s1"])

    def test_pattern_validation(self):
        for bad in ["", "stocks..tech", "stocks.#.x", "stocks tech", "a.b+"]:
            with self.assertRaises(P.ProtocolError, msg=bad):
                P.validate_pattern(bad)
        for bad in ["stocks.*", "#", ""]:
            with self.assertRaises(P.ProtocolError, msg=bad):
                P.validate_topic(bad)


class BrokerTestCase(unittest.TestCase):
    ACK_TIMEOUT = 0.5

    def setUp(self):
        self.store = MemoryQueueStore()
        self.broker = Broker(self.store, "127.0.0.1", 0, ack_timeout=self.ACK_TIMEOUT)
        _, self.port = self.broker.start()
        self.clients = []

    def tearDown(self):
        for c in self.clients:
            try:
                c.kill()
            except OSError:
                pass
        self.broker.stop()

    def client(self, cid, role, **kw):
        c = RawClient(self.port, cid, role, **kw)
        self.clients.append(c)
        return c

    def sub(self, cid, *topics, **kw):
        c = self.client(cid, P.ROLE_SUBSCRIBER, **kw)
        c.connect()
        c.subscribe(*topics)
        return c

    def pub(self, cid):
        c = self.client(cid, P.ROLE_PUBLISHER)
        c.connect()
        return c

    def scenario(self):
        """The stock price alert scenario from the project brief."""
        tech = self.sub("tech-investor", "stocks.tech")
        bank = self.sub("bank-analyst", "stocks.banking", "crypto")
        crypto = self.sub("crypto-trader", "crypto")
        exch = self.pub("stock-exchange")
        cfeed = self.pub("crypto-feed")
        return tech, bank, crypto, exch, cfeed


class RoutingTests(BrokerTestCase):
    def test_topic_filtering_no_cross_topic_leakage(self):
        tech, bank, crypto, exch, cfeed = self.scenario()
        exch.publish("stocks.tech", {"symbol": "AAPL", "price": 190.1})
        exch.publish("stocks.banking", {"symbol": "BDO", "price": 140.5})
        cfeed.publish("crypto", {"symbol": "BTC", "price": 97000})
        self.assertEqual(len(tech.wait_for(1)), 1)
        self.assertEqual(len(bank.wait_for(2)), 2)
        self.assertEqual(len(crypto.wait_for(1)), 1)
        time.sleep(0.3)                                      # give leaks a chance to show
        self.assertEqual({d["topic"] for d in tech.delivered}, {"stocks.tech"})
        self.assertEqual({d["topic"] for d in bank.delivered}, {"stocks.banking", "crypto"})
        self.assertEqual({d["topic"] for d in crypto.delivered}, {"crypto"})
        self.assertEqual(len(tech.delivered) + len(bank.delivered) + len(crypto.delivered), 4)

    def test_deliver_fields(self):
        tech = self.sub("s1", "stocks.tech")
        exch = self.pub("p1")
        ack = exch.publish("stocks.tech", {"symbol": "NVDA", "price": 1.5}, msg_id="fixed-id")
        self.assertEqual(ack["recipients"], 1)
        self.assertFalse(ack["duplicate"])
        d = tech.wait_for(1)[0]
        self.assertEqual(d["msg_id"], "fixed-id")
        self.assertEqual(d["payload"], {"symbol": "NVDA", "price": 1.5})
        self.assertEqual(d["publisher_id"], "p1")
        self.assertEqual(d["attempt"], 1)
        self.assertFalse(d["backlog"])

    def test_wildcard_subscription(self):
        allstocks = self.sub("s", "stocks.*")
        exch = self.pub("p")
        exch.publish("stocks.tech", 1)
        exch.publish("stocks.banking", 2)
        exch.publish("crypto", 3)
        self.assertEqual([d["payload"] for d in allstocks.wait_for(2)], [1, 2])
        time.sleep(0.2)
        self.assertEqual(len(allstocks.delivered), 2)

    def test_unsubscribe(self):
        s = self.sub("s", "crypto", "stocks.tech")
        s.send({"type": P.UNSUBSCRIBE, "topics": ["crypto"]})
        self.assertEqual(s.expect(P.UNSUBACK)["subscriptions"], ["stocks.tech"])
        p = self.pub("p")
        p.publish("crypto", "x")
        p.publish("stocks.tech", "y")
        self.assertEqual([d["payload"] for d in s.wait_for(1)], ["y"])
        time.sleep(0.2)
        self.assertEqual(len(s.delivered), 1)

    def test_ordering_per_publisher(self):
        s = self.sub("s", "crypto")
        p = self.pub("p")
        for i in range(50):
            p.publish("crypto", i)
        self.assertEqual([d["payload"] for d in s.wait_for(50)], list(range(50)))

    def test_concurrent_two_publishers_three_subscribers(self):
        import threading
        tech, bank, crypto, exch, cfeed = self.scenario()
        n = 100

        def run(pubc, topics):
            for i in range(n):
                pubc.publish(topics[i % len(topics)], i)
        t1 = threading.Thread(target=run, args=(exch, ["stocks.tech", "stocks.banking"]))
        t2 = threading.Thread(target=run, args=(cfeed, ["crypto"]))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(tech.wait_for(50)), 50)
        self.assertEqual(len(bank.wait_for(150)), 150)
        self.assertEqual(len(crypto.wait_for(100)), 100)
        for c in (tech, bank, crypto):
            ids = [d["msg_id"] for d in c.delivered]
            self.assertEqual(len(ids), len(set(ids)), "no duplicates expected when ACKs flow normally")


class DeliveryGuaranteeTests(BrokerTestCase):
    def test_publish_persisted_before_puback(self):
        self.sub("s", "crypto").kill()                 # durable subscriber, now offline
        time.sleep(0.1)
        p = self.pub("p")
        p.publish("crypto", "x", msg_id="m1")
        # PUBACK received => already in storage
        self.assertEqual([m.msg_id for m in self.store.get_pending("s")], ["m1"])

    def test_ack_marks_delivered(self):
        s = self.sub("s", "crypto")
        p = self.pub("p")
        p.publish("crypto", "x", msg_id="m1")
        s.wait_for(1)
        deadline = time.time() + 2
        while self.store.get_pending("s") and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(self.store.get_pending("s"), [])

    def test_missing_ack_triggers_redelivery(self):
        s = self.sub("s", "crypto", auto_ack=False)
        p = self.pub("p")
        p.publish("crypto", "x", msg_id="m1")
        first = s.wait_for(1)[0]
        self.assertEqual(first["attempt"], 1)
        again = s.wait_for(2, timeout=self.ACK_TIMEOUT * 4)
        self.assertGreaterEqual(len(again), 2)
        self.assertEqual(again[1]["msg_id"], "m1")
        self.assertEqual(again[1]["attempt"], 2)
        s.send({"type": P.ACK, "msg_id": "m1"})        # now ACK -> retransmits stop
        time.sleep(self.ACK_TIMEOUT * 0.3)
        count = len(s.delivered)
        time.sleep(self.ACK_TIMEOUT * 3)
        self.assertEqual(len(s.delivered), count)
        self.assertEqual(self.store.get_pending("s"), [])

    def test_duplicate_publish_is_idempotent(self):
        s = self.sub("s", "crypto")
        p = self.pub("p")
        self.assertFalse(p.publish("crypto", "x", msg_id="dup")["duplicate"])
        self.assertTrue(p.publish("crypto", "x", msg_id="dup")["duplicate"])   # publisher retry
        s.wait_for(1)
        time.sleep(0.3)
        self.assertEqual(len(s.delivered), 1)

    def test_flow_control_window(self):
        self.broker.delivery.max_inflight = 3
        s = self.sub("s", "crypto", auto_ack=False)
        p = self.pub("p")
        for i in range(10):
            p.publish("crypto", i)
        time.sleep(0.2)
        self.assertEqual(len(s.delivered), 3)          # window full, rest queued
        for d in list(s.delivered):
            s.send({"type": P.ACK, "msg_id": d["msg_id"]})
        s.wait_for(6)
        self.assertGreaterEqual(len(s.delivered), 6)


class CatchUpTests(BrokerTestCase):
    def test_offline_subscriber_gets_backlog_in_order(self):
        """The core demo: kill Bank Analyst, keep publishing, restart with same id."""
        tech, bank, crypto, exch, cfeed = self.scenario()
        exch.publish("stocks.banking", "before")
        bank.wait_for(1)
        deadline = time.time() + 2                     # let the ACK reach the broker before the "crash"
        while self.store.get_pending("bank-analyst") and time.time() < deadline:
            time.sleep(0.01)
        bank.kill()
        time.sleep(0.2)
        self.assertFalse(self.broker.delivery.is_online("bank-analyst"))
        sent = []
        for i in range(10):
            sent.append(exch.publish("stocks.banking", f"bank-{i}")["msg_id"])
            sent.append(cfeed.publish("crypto", f"crypto-{i}")["msg_id"])
        exch.publish("stocks.tech", "not-for-bank")
        self.assertEqual([m.msg_id for m in self.store.get_pending("bank-analyst")], sent)

        bank2 = self.client("bank-analyst", P.ROLE_SUBSCRIBER)
        connack = bank2.connect()                          # NO re-subscribe needed
        self.assertTrue(connack["session_present"])
        self.assertEqual(connack["subscriptions"], ["crypto", "stocks.banking"])
        got = bank2.wait_for(20)
        self.assertEqual([d["msg_id"] for d in got], sent)  # all, in publish order
        self.assertTrue(all(d["backlog"] for d in got))
        exch.publish("stocks.banking", "live")              # live traffic continues after
        self.assertEqual(bank2.wait_for(21)[-1]["payload"], "live")
        self.assertFalse(bank2.delivered[-1]["backlog"])

    def test_unacked_inflight_redelivered_after_reconnect(self):
        s = self.sub("s", "crypto", auto_ack=False)
        p = self.pub("p")
        p.publish("crypto", "x", msg_id="m1")
        s.wait_for(1)
        s.kill()                                          # crashed before ACKing
        time.sleep(0.2)
        s2 = self.client("s", P.ROLE_SUBSCRIBER)
        s2.connect()
        d = s2.wait_for(1)[0]
        self.assertEqual(d["msg_id"], "m1")
        self.assertTrue(d["backlog"])

    def test_session_takeover_same_client_id(self):
        s = self.sub("s", "crypto")
        s2 = self.client("s", P.ROLE_SUBSCRIBER)        # old socket still open
        s2.connect()
        s.expect("_CLOSED")                               # broker closed the old one
        self.pub("p").publish("crypto", "x")
        self.assertEqual(len(s2.wait_for(1)), 1)

    def test_durable_subscriptions_restored_on_broker_restart(self):
        self.sub("s", "crypto").kill()
        time.sleep(0.1)
        self.pub("p").publish("crypto", "x", msg_id="m1")
        self.broker.stop()
        # New broker process, same (durable) store
        self.broker = Broker(self.store, "127.0.0.1", 0, ack_timeout=self.ACK_TIMEOUT)
        _, self.port = self.broker.start()
        p = self.pub("p")
        p.publish("crypto", "y", msg_id="m2")              # routed via restored subscription
        s = self.client("s", P.ROLE_SUBSCRIBER)
        self.assertTrue(s.connect()["session_present"])
        self.assertEqual([d["msg_id"] for d in s.wait_for(2)], ["m1", "m2"])


class ProtocolErrorTests(BrokerTestCase):
    def test_malformed_json(self):
        c = self.client("x", P.ROLE_PUBLISHER)
        c.send_raw(b"{not json}\n")
        self.assertEqual(c.expect(P.ERROR)["error"], "MALFORMED_FRAME")
        c.connect()                                       # connection still usable

    def test_must_connect_first(self):
        c = self.client("x", P.ROLE_PUBLISHER)
        c.send({"type": P.PUBLISH, "msg_id": "m", "topic": "crypto", "payload": 1})
        self.assertEqual(c.expect(P.ERROR)["error"], "NOT_CONNECTED")

    def test_role_violation(self):
        p = self.pub("p")
        p.send({"type": P.SUBSCRIBE, "topics": ["crypto"]})
        self.assertEqual(p.expect(P.ERROR)["error"], "ROLE_VIOLATION")
        s = self.sub("s", "crypto")
        s.send({"type": P.PUBLISH, "msg_id": "m", "topic": "crypto", "payload": 1})
        self.assertEqual(s.expect(P.ERROR)["error"], "ROLE_VIOLATION")

    def test_invalid_topic_and_unknown_type(self):
        p = self.pub("p")
        p.send({"type": P.PUBLISH, "msg_id": "m", "topic": "stocks.*", "payload": 1, "req_id": 7})
        err = p.expect(P.ERROR)
        self.assertEqual((err["error"], err["req_id"]), ("INVALID_TOPIC", 7))
        p.send({"type": "FOO"})
        self.assertEqual(p.expect(P.ERROR)["error"], "UNKNOWN_TYPE")

    def test_invalid_client_id_and_double_connect(self):
        c = self.client("bad id!", P.ROLE_SUBSCRIBER)
        c.send({"type": P.CONNECT, "client_id": "bad id!", "role": "subscriber"})
        self.assertEqual(c.expect(P.ERROR)["error"], "INVALID_CLIENT_ID")
        p = self.pub("p")
        p.send({"type": P.CONNECT, "client_id": "p", "role": "publisher"})
        self.assertEqual(p.expect(P.ERROR)["error"], "ALREADY_CONNECTED")

    def test_ping(self):
        c = self.client("x", P.ROLE_PUBLISHER)
        c.send({"type": P.PING, "req_id": "a"})
        self.assertEqual(c.expect(P.PONG)["req_id"], "a")

    def test_oversized_frame_closes_connection(self):
        c = self.client("x", P.ROLE_PUBLISHER)
        c.send_raw(b"x" * (P.MAX_FRAME_BYTES + 10) + b"\n")
        self.assertEqual(c.expect(P.ERROR)["error"], "FRAME_TOO_LARGE")
        c.expect("_CLOSED")


if __name__ == "__main__":
    unittest.main()
