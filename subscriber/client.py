"""
Subscriber client for the pub/sub broker.
python -m subscriber.client --id bank-analyst --topics stocks.banking crypto

  T1  Socket connection + CONNECT with a persistent client_id. TCP is a byte
      stream, so framing is newline-delimited JSON via common.protocol.FrameStream.
  T2  SUBSCRIBE / UNSUBSCRIBE; CLI args --id and --topics.
  T3  Background receive thread; formatted alert display.
  T4  ACK sent only AFTER the alert has been displayed.
  T5  Duplicate detection by msg_id using a local seen-ID file.
  T6  Auto-reconnect with exponential backoff; the catch-up burst is handled
      by the same receive loop.
  T7  Demo: Ctrl+C / kill, then start again with the same --id.

Standard library only.
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import socket
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set

from common import protocol as P
from common.protocol import FrameStream, ProtocolError

log = logging.getLogger("subscriber")



# T5 - seen-ID file

class SeenStore:
    """msg_ids already processed, kept in a text file (one id per line) so
    duplicate detection survives a restart. The id is written right after the
    alert is shown and BEFORE the ACK is sent."""

    def __init__(self, path: str):
        self._ids: Set[str] = set()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                self._ids = {line.strip() for line in fh if line.strip()}
        self._fh = open(path, "a", encoding="utf-8")

    def __contains__(self, msg_id: str) -> bool:
        return msg_id in self._ids

    def add(self, msg_id: str) -> None:
        if msg_id in self._ids:
            return
        self._ids.add(msg_id)
        self._fh.write(msg_id + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        self._fh.close()


# T3 - alert line

def format_alert(frame: Dict[str, Any]) -> str:
    """e.g. [14:02:11] [BACKLOG] [RETRY 2] crypto  BTC 97100 (+0.30%)  from=crypto-feed  id=86578292"""
    ts = frame.get("timestamp")
    clock = time.strftime("%H:%M:%S", time.localtime(ts / 1000 if isinstance(ts, int) else None))
    tags = ""
    if frame.get("backlog"):
        tags += "[BACKLOG] "
    attempt = frame.get("attempt", 1)
    if isinstance(attempt, int) and attempt > 1:
        tags += f"[RETRY {attempt}] "
    payload = frame.get("payload")
    if isinstance(payload, dict) and "symbol" in payload:
        body = str(payload["symbol"])
        if "price" in payload:
            body += f" {payload['price']}"
        if isinstance(payload.get("change_pct"), (int, float)):
            body += f" ({payload['change_pct']:+.2f}%)"
    else:
        body = str(payload)
    return (f"[{clock}] {tags}{frame.get('topic', '?')}  {body}  "
            f"from={frame.get('publisher_id', '?')}  id={str(frame.get('msg_id', ''))[:8]}")



# Client

class SubscriberClient:
    def __init__(self, client_id: str, topics: List[str]):
        P.validate_client_id(client_id)
        self.client_id = client_id
        self.topics: List[str] = P.validate_patterns(topics) if topics else []
        self.seen = SeenStore(f".seen_{client_id}.txt")
        self._stop = threading.Event()
        self._stream: Optional[FrameStream] = None
        self._connected = threading.Event()
        self._fatal = False

    # ---- T2
    def subscribe(self, topics: List[str]) -> None:
        topics = P.validate_patterns(topics)
        self.topics += [t for t in topics if t not in self.topics]
        self._send({"type": P.SUBSCRIBE, "topics": topics})

    def unsubscribe(self, topics: List[str]) -> None:
        topics = P.validate_patterns(topics)
        self.topics = [t for t in self.topics if t not in topics]
        self._send({"type": P.UNSUBSCRIBE, "topics": topics})

    def _send(self, frame: Dict[str, Any]) -> None:
        if self._stream is not None and self._connected.is_set():
            try:
                self._stream.send(frame)
            except OSError:
                pass      # not connected: applied on the next CONNECT

    # ---- T1 + T6
    def run(self, host: str = P.DEFAULT_HOST, port: int = P.DEFAULT_PORT) -> None:
        """Connect and receive; reconnect with exponential backoff until stopped."""
        delay = 0.5
        while not self._stop.is_set():
            if self._session(host, port):
                delay = 0.5                           # we were connected: reset backoff
            if self._stop.is_set() or self._fatal:
                break
            wait = delay * random.uniform(0.8, 1.2)
            log.warning("disconnected; reconnecting in %.1fs (same client_id)", wait)
            if self._stop.wait(wait):
                break
            delay = min(delay * 2, 8.0)
        self.seen.close()

    def start_background(self, host: str = P.DEFAULT_HOST, port: int = P.DEFAULT_PORT) -> threading.Thread:
        t = threading.Thread(target=self.run, args=(host, port), daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        """Graceful stop. Subscriptions stay durable on the broker."""
        self._stop.set()
        if self._stream is not None:
            try:
                self._stream.send({"type": P.DISCONNECT})
            except OSError:
                pass
            self._stream.close()

    def _session(self, host: str, port: int) -> bool:
        try:
            sock = socket.create_connection((host, port), timeout=5)
        except OSError as exc:
            log.warning("cannot reach broker %s:%s (%s)", host, port, exc)
            return False
        sock.settimeout(None)
        stream = FrameStream(sock)
        self._stream = stream
        got_connack = False
        try:
            stream.send({"type": P.CONNECT, "client_id": self.client_id,
                         "role": P.ROLE_SUBSCRIBER, "version": P.PROTOCOL_VERSION})
            # T3: receive loop - the only caller of recv()
            while not self._stop.is_set():
                frame = stream.recv()
                if frame is None:
                    log.warning("broker closed the connection")
                    break
                ftype = frame["type"]
                if ftype == P.CONNACK:
                    got_connack = True
                    log.info("CONNECTED as %s (session_present=%s)", self.client_id,
                             frame.get("session_present"))
                    if self.topics:                   # SUBSCRIBE is idempotent
                        stream.send({"type": P.SUBSCRIBE, "topics": self.topics})
                    self._connected.set()
                elif ftype == P.DELIVER:
                    self._on_deliver(frame, stream)
                elif ftype in (P.SUBACK, P.UNSUBACK):
                    log.info("%s -> %s", ftype, frame.get("subscriptions"))
                elif ftype == P.ERROR:
                    log.error("broker ERROR %s %s: %s", frame.get("code"), frame.get("error"),
                              frame.get("message"))
                    if frame.get("code") in (403, 426):   # retrying can never succeed
                        self._fatal = True
                        break
        except (ProtocolError, OSError) as exc:
            if not self._stop.is_set():
                log.warning("connection error: %s", exc)
        finally:
            self._connected.clear()
            stream.close()
            self._stream = None
        return got_connack

    # ---- T4 + T5
    def _on_deliver(self, frame: Dict[str, Any], stream: FrameStream) -> None:
        try:
            msg_id = P.validate_msg_id(frame.get("msg_id"))
        except ProtocolError:
            return
        if msg_id in self.seen:
            log.info("DUPLICATE %s - ACK only", msg_id[:8])        # T5: ACK, do not show again
        else:
            print(format_alert(frame), flush=True)                 # process = display
            self.seen.add(msg_id)                                  # record BEFORE the ACK
        stream.send({"type": P.ACK, "msg_id": msg_id})             # T4: ACK after processing


# CLI (T2)

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="subscriber", description="Pub/Sub subscriber client")
    p.add_argument("--id", required=True, help="persistent client_id (keep it the same across restarts)")
    p.add_argument("--topics", nargs="+", default=[], help="topic patterns, e.g. stocks.banking crypto")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")
    try:
        client = SubscriberClient(args.id, [t for c in args.topics for t in c.split(",") if t])
    except ProtocolError as exc:
        print(f"error: {exc.message}", file=sys.stderr)
        return 2
    print(f"[{args.id}] subscriber starting  topics={client.topics or '(none)'}", flush=True)
    worker = client.start_background()
    try:
        while worker.is_alive():
            worker.join(0.2)                  # short joins so Ctrl+C is handled promptly
    except KeyboardInterrupt:
        print(f"\n[{args.id}] stopping (subscriptions stay durable on the broker)", flush=True)
        client.stop()
        worker.join(timeout=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())