"""
M5 QA - routing tests (Gaitano).

End-to-end over real TCP sockets with the REAL merged components:
M1 broker, M2 SQLiteQueueStore, M4 PublisherClient and the M3 SubscriberClient
(via tests.qa_helpers.RecordingSubscriber).

    python -m unittest tests.test_routing -v

Covers the topic-routing guarantees of PROTOCOL.md sec 3: filtering with no
leakage, wildcards, unsubscribe, late-subscriber semantics and per-publisher
ordering.
"""
import os
import tempfile
import time
import unittest
import uuid

from broker.server import Broker
from publisher.client import PublisherClient
from storage.queue_store import SQLiteQueueStore
from tests.qa_helpers import RecordingSubscriber


class RoutingEndToEnd(unittest.TestCase):
    ACK_TIMEOUT = 0.5

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls._tmp.cleanup)
        cls._old_cwd = os.getcwd()
        os.chdir(cls._tmp.name)               # subscriber seen-ID files go to the temp dir
        cls.addClassCleanup(os.chdir, cls._old_cwd)

    def setUp(self):
        self.db = os.path.join(self._tmp.name, f"{uuid.uuid4().hex}.db")
        self.store = SQLiteQueueStore(self.db)
        self.broker = Broker(self.store, "127.0.0.1", 0, ack_timeout=self.ACK_TIMEOUT)
        _, self.port = self.broker.start()
        self.subs = []
        self.pubs = []

    def tearDown(self):
        for s in self.subs:
            s.stop()
        for p in self.pubs:
            p.disconnect()
        self.broker.stop()
        self.store.close()

    # ------------------------------------------------------------ helpers
    def real_sub(self, cid, *topics):
        s = RecordingSubscriber(cid, list(topics))
        s.start_background("127.0.0.1", self.port)
        self.assertTrue(s._connected.wait(5), f"{cid} did not connect")
        self.subs.append(s)
        return s

    def real_pub(self, cid):
        p = PublisherClient(cid, "127.0.0.1", self.port, ack_timeout=3.0)
        self.assertTrue(p.connect(), f"{cid} could not connect")
        self.pubs.append(p)
        return p

    def wait_subscribed(self, cid, pattern, timeout=3.0):
        """Deterministic wait: a subscription is persisted BEFORE the SUBACK is
        sent, so durable storage is the source of truth."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pattern in self.store.load_subscriptions().get(cid, set()):
                return
            time.sleep(0.01)
        self.fail(f"{cid} never subscribed to {pattern}")

    def wait_unsubscribed(self, cid, pattern, timeout=3.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pattern not in self.store.load_subscriptions().get(cid, set()):
                return
            time.sleep(0.01)
        self.fail(f"{cid} never unsubscribed from {pattern}")

    # ------------------------------------------------------------ tests
    def test_stock_alert_scenario_end_to_end(self):
        """The exact demo scenario: 2 publishers, 3 subscribers, no leakage."""
        tech = self.real_sub("tech-investor", "stocks.tech")
        bank = self.real_sub("bank-analyst", "stocks.banking", "crypto")
        crypto = self.real_sub("crypto-trader", "crypto")
        for cid, pat in (("tech-investor", "stocks.tech"),
                         ("bank-analyst", "stocks.banking"), ("bank-analyst", "crypto"),
                         ("crypto-trader", "crypto")):
            self.wait_subscribed(cid, pat)
        exch = self.real_pub("stock-exchange")
        cfeed = self.real_pub("crypto-feed")

        self.assertTrue(exch.publish("stocks.tech", {"symbol": "AAPL", "price": 190.1, "change_pct": 0.42}))
        self.assertTrue(exch.publish("stocks.banking", {"symbol": "BDO", "price": 140.5, "change_pct": -0.10}))
        self.assertTrue(cfeed.publish("crypto", {"symbol": "BTC", "price": 97000.0, "change_pct": 0.30}))

        self.assertEqual({d["topic"] for d in tech.wait_alerts(1)}, {"stocks.tech"})
        self.assertEqual({d["topic"] for d in bank.wait_alerts(2)}, {"stocks.banking", "crypto"})
        self.assertEqual({d["topic"] for d in crypto.wait_alerts(1)}, {"crypto"})
        time.sleep(0.3)                       # give cross-topic leaks a chance to appear
        self.assertEqual(len(tech.alerts) + len(bank.alerts) + len(crypto.alerts), 4)
        btc = [a for a in bank.alerts if a["topic"] == "crypto"][0]
        self.assertEqual(btc["publisher_id"], "crypto-feed")
        self.assertEqual(btc["payload"]["symbol"], "BTC")

    def test_wildcard_star_and_hash(self):
        star = self.real_sub("etf-bot", "stocks.*")           # exactly one segment
        everything = self.real_sub("index-fund", "#")         # zero or more segments
        self.wait_subscribed("etf-bot", "stocks.*")
        self.wait_subscribed("index-fund", "#")
        exch = self.real_pub("stock-exchange")
        cfeed = self.real_pub("crypto-feed")

        exch.publish("stocks.tech", "t")
        exch.publish("stocks.banking", "b")
        cfeed.publish("crypto", "c")

        self.assertEqual({d["topic"] for d in star.wait_alerts(2)}, {"stocks.tech", "stocks.banking"})
        self.assertEqual({d["topic"] for d in everything.wait_alerts(3)},
                         {"stocks.tech", "stocks.banking", "crypto"})
        time.sleep(0.3)
        self.assertEqual(len(star.alerts), 2)                 # no crypto leak
        self.assertEqual(len(everything.alerts), 3)

    def test_unsubscribe_stops_delivery(self):
        s = self.real_sub("s", "crypto", "stocks.tech")
        self.wait_subscribed("s", "crypto")
        s.unsubscribe(["crypto"])
        self.wait_unsubscribed("s", "crypto")
        exch = self.real_pub("stock-exchange")
        exch.publish("crypto", "dropped")
        exch.publish("stocks.tech", "kept")
        self.assertEqual([d["payload"] for d in s.wait_alerts(1)], ["kept"])
        time.sleep(0.3)
        self.assertEqual(len(s.alerts), 1)

    def test_late_subscriber_gets_only_new_messages(self):
        """A subscription only receives messages published AFTER it is registered."""
        exch = self.real_pub("stock-exchange")
        self.assertTrue(exch.publish("stocks.tech", "before"))
        s = self.real_sub("late-comer", "stocks.tech")
        self.wait_subscribed("late-comer", "stocks.tech")
        exch.publish("stocks.tech", "after")
        self.assertEqual([d["payload"] for d in s.wait_alerts(1)], ["after"])
        time.sleep(0.3)
        self.assertEqual(len(s.alerts), 1)

    def test_invalid_topics_rejected_no_subscribers_ok(self):
        exch = self.real_pub("stock-exchange")
        # No subscriber matched: publish still succeeds (recipients can be 0).
        self.assertTrue(exch.publish("quiet.topic", {"x": 1}))
        # Wildcards/syntax errors are not allowed in PUBLISH -> ERROR, client returns False.
        self.assertFalse(exch.publish("stocks.*", {"x": 1}))
        self.assertFalse(exch.publish("bad topic!", {"x": 1}))
        self.assertTrue(exch.publish("stocks.tech", {"x": 2}))  # connection still usable

    def test_ordering_per_publisher(self):
        s = self.real_sub("s", "crypto")
        self.wait_subscribed("s", "crypto")
        p = self.real_pub("crypto-feed")
        for i in range(30):
            self.assertTrue(p.publish("crypto", i))
        self.assertEqual([d["payload"] for d in s.wait_alerts(30)], list(range(30)))


if __name__ == "__main__":
    unittest.main()
