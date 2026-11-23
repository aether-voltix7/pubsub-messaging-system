"""
QA helpers for the M5 test suites (Gaitano).

These wrap the REAL merged clients (M3 subscriber, M4 publisher) so the M5
end-to-end tests exercise exactly what ships in the demo, not a
re-implementation. Standard library only, like the rest of the project.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List

from common import protocol as P
from subscriber.client import SubscriberClient


class RecordingSubscriber(SubscriberClient):
    """The real M3 SubscriberClient, with the alert display replaced by an
    in-memory list so tests can assert on DELIVER frames.

    Everything else is production behaviour: FrameStream framing, CONNECT with
    a persistent client_id, durable subscriptions, auto-reconnect (T6), the
    seen-ID dedup file (T5) and ACK-after-processing (T4). The seen file uses
    a relative path, so M5 test bases chdir() into a temp directory first.
    """

    def __init__(self, client_id: str, topics: List[str]):
        super().__init__(client_id, topics)
        self.alerts: List[Dict[str, Any]] = []

    def _on_deliver(self, frame: Dict[str, Any], stream) -> None:
        try:
            msg_id = P.validate_msg_id(frame.get("msg_id"))
        except P.ProtocolError:
            return
        if msg_id in self.seen:
            pass                                # T5: duplicate -> ACK only, no re-show
        else:
            self.alerts.append(frame)           # "process" = record instead of print
            self.seen.add(msg_id)
        stream.send({"type": P.ACK, "msg_id": msg_id})   # T4: ACK after processing

    def wait_alerts(self, n: int, timeout: float = 5.0) -> List[Dict[str, Any]]:
        deadline = time.time() + timeout
        while len(self.alerts) < n and time.time() < deadline:
            time.sleep(0.01)
        return list(self.alerts)
