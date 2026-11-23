"""
Unit tests for the SQLite durable queue storage layer (M2, Labrador — L5).

Verifies `SQLiteQueueStore` (storage/queue_store.py) using isolated temporary
SQLite databases created fresh for each test case and cleaned up automatically
on teardown. Complements the shared contract suite in tests/test_storage_contract.py.

Test coverage:
    - test_ordering: Confirms pending messages are returned in strict FIFO
      order with monotonically increasing `seq` values.
    - test_ack_removal: Confirms `mark_acked` removes a message only for the
      acknowledging subscriber and handles duplicate ACKs idempotently.
    - test_restart_survival: Confirms subscriptions, unacknowledged messages,
      and ACK states persist across closing and reopening the database file.

Run from the project root:
    python -m unittest tests.test_storage -v
"""

import os, tempfile, unittest  # File paths, isolated temp folders, and Python's unit test framework
from storage.queue_store import SQLiteQueueStore  # The persistent queue store class being tested


class TestSQLiteQueueStore(unittest.TestCase):  # Unit test suite for SQLiteQueueStore
    def setUp(self):  # Runs automatically before each test to create a fresh, isolated database
        self._tmp = tempfile.TemporaryDirectory()  # Creates a temporary folder for the test DB
        self.db_path = os.path.join(self._tmp.name, "test_queue.db")  # Full path to the temporary SQLite file
        self.store = SQLiteQueueStore(self.db_path)  # Initializes a new store instance

    def tearDown(self):  # Runs automatically after each test to clean up resources
        self.store.close()  # Closes the SQLite connection so the file can be deleted
        self._tmp.cleanup()  # Removes the temporary folder and its DB files

    def test_ordering(self):  # Verifies pending messages are returned in strict FIFO order
        # Saves three messages sequentially for subscriber "sub1"
        self.store.save_message("m1", "sensors/temp", {"v": 1}, "pub1", 100, ["sub1"])
        self.store.save_message("m2", "sensors/temp", {"v": 2}, "pub1", 200, ["sub1"])
        self.store.save_message("m3", "sensors/temp", {"v": 3}, "pub1", 300, ["sub1"])

        pending = self.store.get_pending("sub1")  # Fetches all unacknowledged messages for "sub1"
        self.assertEqual([m.msg_id for m in pending], ["m1", "m2", "m3"])  # Confirms exact insertion order
        self.assertTrue(pending[0].seq < pending[1].seq < pending[2].seq)  # Confirms strictly increasing sequence numbers

    def test_ack_removal(self):  # Verifies ACKs remove messages per subscriber and are idempotent
        # Saves two messages addressed to both "sub1" and "sub2"
        self.store.save_message("m1", "sensors/temp", {"v": 1}, "pub1", 100, ["sub1", "sub2"])
        self.store.save_message("m2", "sensors/temp", {"v": 2}, "pub1", 200, ["sub1", "sub2"])

        self.assertTrue(self.store.mark_acked("sub1", "m1"))  # First ACK for "sub1" on "m1" should succeed
        self.assertFalse(self.store.mark_acked("sub1", "m1"))  # Duplicate ACK should return False (idempotent)
        self.assertEqual([m.msg_id for m in self.store.get_pending("sub1")], ["m2"])  # "m1" is removed from "sub1"
        self.assertEqual([m.msg_id for m in self.store.get_pending("sub2")], ["m1", "m2"])  # "sub2" is unaffected

    def test_restart_survival(self):  # Verifies subscriptions, pending messages, and ACKs survive a restart
        self.store.register_subscription("sub1", "sensors/#")  # Persists a wildcard subscription
        self.store.save_message("m1", "sensors/temp", {"v": 1}, "pub1", 100, ["sub1"])  # Saves first message
        self.store.save_message("m2", "sensors/temp", {"v": 2}, "pub1", 200, ["sub1"])  # Saves second message
        self.store.mark_acked("sub1", "m1")  # Acknowledges only "m1" before restarting

        # Simulates a broker restart by closing and reopening the same DB file
        self.store.close()
        self.store = SQLiteQueueStore(self.db_path)

        self.assertEqual(self.store.load_subscriptions(), {"sub1": {"sensors/#"}})  # Confirms subscription survived
        self.assertEqual([m.msg_id for m in self.store.get_pending("sub1")], ["m2"])  # Confirms only unacked "m2" remains

    def test_cleanup_acked(self):  # Verifies only fully-acknowledged messages are purged
        self.store.save_message("m1", "sensors/temp", {"v": 1}, "pub1", 100, ["sub1", "sub2"])
        self.store.save_message("m2", "sensors/temp", {"v": 2}, "pub1", 200, ["sub1", "sub2"])

        # Fully acknowledge m1 (both sub1 and sub2)
        self.store.mark_acked("sub1", "m1")
        self.store.mark_acked("sub2", "m1")

        # Partially acknowledge m2 (only sub1)
        self.store.mark_acked("sub1", "m2")

        removed = self.store.cleanup_acked()
        self.assertEqual(removed, 1)  # Only m1 should be deleted

        # m2 must still be pending for sub2
        self.assertEqual([m.msg_id for m in self.store.get_pending("sub2")], ["m2"])

if __name__ == "__main__":  # Allows running this test file directly
    unittest.main()  # Discovers and executes all test methods in this file