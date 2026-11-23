"""
M5 QA - fault-injection tests (Gaitano).

End-to-end with the real broker and the real M2 SQLite store; faults are
injected the same way they will be shown in the demo (per CONTRIBUTING.md):
  * lost ACK          -> short ack-timeout, subscriber skips the ACK
  * offline/crashed   -> subscriber stopped, catch-up on reconnect with same id
  * broker restart    -> --store sqlite, Broker restarted on the same db file
  * duplicate PUBLISH -> identical msg_id resent (publisher retry rule)
  * takeover          -> same client_id connects twice

    python -m unittest tests.test_faults -v

Evidence for the fault report comes from the broker log lines:
  PUBLISH ... -> recipients / DETACH ... remain pending /
  CATCH-UP ... n pending / RETRANSMIT ... attempt=n / TAKEOVER
(set logging to INFO below to capture them).
"""
import logging
import os
import tempfile
import time
import unittest
import uuid

from broker.server import Broker
from common import protocol as P
from publisher.client import PublisherClient
from storage.queue_store import SQLiteQueueStore
from tests.helpers import RawClient
from tests.qa_helpers import RecordingSubscriber

logging.basicConfig(level=logging.CRITICAL)   # set to INFO to capture demo evidence


class FaultTestCase(unittest.TestCase):
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
        self.clients = []                     # RawClient or RecordingSubscriber
        self.pubs = []                        # PublisherClient instances


    def tearDown(self):
        for s in self.pubs:
            s.disconnect()
        for c in self.clients:
            if isinstance(c, RecordingSubscriber):
                c.stop()
            else:
                try:
                    c.kill()
                except OSError:
                    pass
        self.broker.stop()
        self.store.close()

    # ------------------------------------------------------------ helpers
    def raw_sub(self, cid, *topics, auto_ack=True):
        c = RawClient(self.port, cid, P.ROLE_SUBSCRIBER, auto_ack=auto_ack)
        c.connack = c.connect()               # kept for assertions; never connect twice
        if topics:
            c.subscribe(*topics)
        self.clients.append(c)
        return c

    def raw_pub(self, cid):
        c = RawClient(self.port, cid, P.ROLE_PUBLISHER)
        c.connect()
        self.clients.append(c)
        return c

    def real_sub(self, cid, *topics):
        s = RecordingSubscriber(cid, list(topics))
        s.start_background("127.0.0.1", self.port)
        self.assertTrue(s._connected.wait(5), f"{cid} did not connect")
        self.clients.append(s)
        return s

    def wait_subscribed(self, cid, pattern, timeout=3.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pattern in self.store.load_subscriptions().get(cid, set()):
                return
            time.sleep(0.01)
        self.fail(f"{cid} never subscribed to {pattern}")

    def wait_offline(self, cid, timeout=3.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.broker.delivery.is_online(cid):
                return
            time.sleep(0.01)
        self.fail(f"{cid} still online")

    def wait_pending_clear(self, cid, timeout=3.0):
        deadline = time.time() + timeout
        while self.store.get_pending(cid) and time.time() < deadline:
            time.sleep(0.01)

    def restart_broker(self):
        """Stop the broker and start a NEW one on the SAME port and db file
        (equivalent to killing the process and running python -m broker again
        with the same --port; the publisher's port is fixed at construction)."""
        self.broker.stop()
        self.store.close()
        self.store = SQLiteQueueStore(self.db)
        self.broker = Broker(self.store, "127.0.0.1", self.port, ack_timeout=self.ACK_TIMEOUT)
        _, self.port = self.broker.start()

    # ------------------------------------------------------------ tests
    def test_lost_ack_triggers_retransmit_then_stops(self):
        """Lost-ACK demo: subscriber never ACKs; broker resends with attempt+1;
        a late ACK stops the retransmits and clears storage."""
        s = self.raw_sub("s", "crypto", auto_ack=False)
        p = self.raw_pub("p")
        p.publish("crypto", "x", msg_id="m1")
        first = s.wait_for(1)[0]
        self.assertEqual(first["attempt"], 1)
        self.assertFalse(first["backlog"])

        again = s.wait_for(2, timeout=self.ACK_TIMEOUT * 6)   # retransmit scan fires
        self.assertGreaterEqual(len(again), 2)
        self.assertEqual(again[1]["msg_id"], "m1")
        self.assertEqual(again[1]["attempt"], 2)

        s.send({"type": P.ACK, "msg_id": "m1"})
        self.wait_pending_clear("s")
        count = len(s.delivered)
        time.sleep(self.ACK_TIMEOUT * 3)
        self.assertEqual(len(s.delivered), count)             # retransmits stopped
        self.assertEqual(self.store.get_pending("s"), [])

    def test_offline_subscriber_catch_up_in_order(self):
        """Crash-demo: bank-analyst goes offline, messages keep queuing, the
        reconnect with the same id flushes the backlog in publish order."""
        bank = self.real_sub("bank-analyst", "stocks.banking", "crypto")
        self.wait_subscribed("bank-analyst", "stocks.banking")
        exch = PublisherClient("stock-exchange", "127.0.0.1", self.port, ack_timeout=3.0)
        self.assertTrue(exch.connect())
        cfeed = PublisherClient("crypto-feed", "127.0.0.1", self.port, ack_timeout=3.0)
        self.assertTrue(cfeed.connect())
        self.pubs = [exch, cfeed]

        self.assertTrue(exch.publish("stocks.banking", "before"))
        self.assertEqual(len(bank.wait_alerts(1)), 1)
        self.wait_pending_clear("bank-analyst")               # first msg ACKed

        bank.stop()                                           # subscriber goes offline
        self.wait_offline("bank-analyst")

        for i in range(5):
            self.assertTrue(exch.publish("stocks.banking", f"bank-{i}"))
            self.assertTrue(cfeed.publish("crypto", f"crypto-{i}"))
        self.assertTrue(exch.publish("stocks.tech", "not-for-bank"))
        # get_pending / catch-up order is storage seq order = the order the
        # broker received the messages (bank-i and crypto-i alternate here).
        expected_order = [p for pair in zip([f"bank-{i}" for i in range(5)],
                                            [f"crypto-{i}" for i in range(5)]) for p in pair]
        pending = self.store.get_pending("bank-analyst")
        self.assertEqual([m.payload for m in pending], expected_order)

        bank2 = self.real_sub("bank-analyst")                 # reconnect, NO re-subscribe
        got = bank2.wait_alerts(10)
        self.assertEqual([d["payload"] for d in got], expected_order)
        self.assertTrue(all(d["backlog"] for d in got))       # everything is backlog
        self.assertTrue(exch.publish("stocks.banking", "live"))
        last = bank2.wait_alerts(11)[-1]
        self.assertEqual(last["payload"], "live")
        self.assertFalse(last["backlog"])                     # live traffic is not backlog

    def test_broker_restart_durability_sqlite(self):
        """Stretch goal: with --store sqlite, subscriptions AND pending messages
        survive a broker restart."""
        self.raw_sub("s", "crypto").kill()                    # durable subscriber, offline
        self.wait_offline("s")
        self.raw_pub("p").publish("crypto", "x", msg_id="m1")
        self.assertEqual([m.msg_id for m in self.store.get_pending("s")], ["m1"])

        self.restart_broker()
        p = self.raw_pub("p")
        p.publish("crypto", "y", msg_id="m2")                 # routed via restored subscription
        s = self.raw_sub("s")                                 # reconnect, no SUBSCRIBE
        self.assertTrue(s.connack["session_present"])
        self.assertEqual(s.connack["subscriptions"], ["crypto"])
        self.assertEqual([d["msg_id"] for d in s.wait_for(2)], ["m1", "m2"])
        self.assertTrue(all(d["backlog"] for d in s.delivered))

    def test_duplicate_publish_delivered_once(self):
        """Publisher retry rule: identical msg_id resent -> PUBACK duplicate=true,
        subscriber gets exactly one DELIVER."""
        s = self.raw_sub("s", "crypto")
        p = self.raw_pub("p")
        ack1 = p.publish("crypto", "x", msg_id="dup-id")
        ack2 = p.publish("crypto", "x", msg_id="dup-id")      # e.g. the first PUBACK was lost
        self.assertFalse(ack1["duplicate"])
        self.assertTrue(ack2["duplicate"])
        self.assertEqual(len(s.wait_for(1)), 1)
        time.sleep(0.3)
        self.assertEqual(len(s.delivered), 1)

    def test_publisher_client_reconnects_after_broker_restart(self):
        """M4 publisher survives a broker restart: its next publish reconnects
        automatically and reaches the durable subscriber again."""
        s = self.real_sub("s", "stocks.tech")
        self.wait_subscribed("s", "stocks.tech")
        p = PublisherClient("stock-exchange", "127.0.0.1", self.port, ack_timeout=3.0)
        self.assertTrue(p.connect())
        self.pubs.append(p)
        self.assertTrue(p.publish("stocks.tech", "before-restart"))
        self.assertEqual(len(s.wait_alerts(1)), 1)

        s.stop()                                    # stop its auto-reconnect loop first
        self.wait_offline("s")
        self.restart_broker()                       # every open socket dies with it
        self.assertTrue(p.publish("stocks.tech", "after-restart"))
        s2 = self.real_sub("s")                     # reconnects with the SAME id
        got = s2.wait_alerts(1)
        self.assertEqual([d["payload"] for d in got], ["after-restart"])
        self.assertTrue(got[0]["backlog"])

    def test_session_takeover_same_client_id(self):
        """Newest connection wins; the old half-open socket is closed (TAKEOVER)."""
        s1 = self.raw_sub("s", "crypto")
        s2 = self.raw_sub("s", "crypto")                      # same id, old socket still open
        s1.expect("_CLOSED")                                  # broker closed the old one
        p = self.raw_pub("p")
        p.publish("crypto", "x")
        self.assertEqual(len(s2.wait_for(1)), 1)
        self.assertEqual(len(s1.delivered), 0)

    def test_flow_control_window(self):
        """At most max_inflight un-ACKed DELIVERs per subscriber; the rest wait."""
        self.broker.delivery.max_inflight = 3
        s = self.raw_sub("s", "crypto", auto_ack=False)
        p = self.raw_pub("p")
        for i in range(6):
            p.publish("crypto", i)
        deadline = time.time() + 2
        while len(s.delivered) < 3 and time.time() < deadline:
            time.sleep(0.01)
        time.sleep(0.3)
        self.assertEqual(len(s.delivered), 3)                 # window full, rest queued
        for d in list(s.delivered):
            s.send({"type": P.ACK, "msg_id": d["msg_id"]})
        self.assertEqual(len(s.wait_for(6, timeout=5)), 6)


if __name__ == "__main__":
    unittest.main()
