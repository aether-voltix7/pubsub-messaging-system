"""
Multi-client TCP broker (J3) - thread-per-connection.

    python -m broker --port 5555 --store sqlite --db broker.db

Each accepted TCP connection gets its own handler thread that reads frames,
validates them and dispatches to the router / delivery engine. A client is
considered disconnected when recv() hits EOF (clean close), when the socket
raises (reset / broken pipe), or when a send to it fails. TCP keep-alive is
enabled so half-open connections are eventually detected by the OS.

Thread-per-connection is the "thread-per-connection" server architecture of
Coulouris et al. Sec 7.4: simple, and blocking I/O on one client never stalls
another. At our scale (2 publishers, 3 subscribers) the cost is negligible.
"""
from __future__ import annotations

import argparse
import logging
import signal
import socket
import sys
import threading
from typing import Any, Dict, Optional, Set

from common import protocol as P
from common.protocol import ErrorCode, FrameStream, ProtocolError
from broker.delivery import DeliveryEngine
from broker.router import Router, SubscriptionRegistry
from storage.interface import QueueStore

log = logging.getLogger("broker.server")


class ClientConnection:
    """One TCP connection. client_id/role are set by CONNECT."""

    def __init__(self, sock: socket.socket, addr):
        self.stream = FrameStream(sock)
        self.addr = addr
        self.client_id: Optional[str] = None
        self.role: Optional[str] = None
        self.closing = False

    def label(self) -> str:
        return f"{self.client_id or '?'}@{self.addr[0]}:{self.addr[1]}"


class Broker:
    def __init__(self, store: QueueStore, host: str = P.DEFAULT_HOST, port: int = P.DEFAULT_PORT,
                 ack_timeout: float = 5.0, max_inflight: int = 32) -> None:
        self.host, self.port = host, port
        self.store = store
        self.registry = SubscriptionRegistry()
        self.registry.load(store.load_subscriptions())      # durable subscriptions survive restart
        self.delivery = DeliveryEngine(store, ack_timeout=ack_timeout, max_inflight=max_inflight)
        self.router = Router(self.registry, store, self.delivery)
        self._lock = threading.Lock()
        self._conns: Set[ClientConnection] = set()
        self._clients: Dict[str, ClientConnection] = {}      # client_id -> live connection
        self._sock: Optional[socket.socket] = None
        self._stopped = threading.Event()
        self._accept_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------ lifecycle
    def start(self):
        """Bind, listen and start accepting in a background thread.
        Returns the actual (host, port) - useful with port=0 in tests."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self.host, self.port = self._sock.getsockname()[:2]
        self.delivery.start()
        self._accept_thread = threading.Thread(target=self._accept_loop, name="accept", daemon=True)
        self._accept_thread.start()
        subs = self.registry.snapshot()
        log.info("broker listening on %s:%d (%d durable subscriber(s) restored%s)", self.host, self.port,
                 len(subs), "".join(f"; {c}={p}" for c, p in subs.items()))
        return self.host, self.port

    def stop(self) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        if self._sock:
            # Wake the thread blocked in accept() with a throwaway connection to
            # ourselves: closing the listening socket does not wake it on Windows.
            wake_host = "127.0.0.1" if self.host in ("0.0.0.0", "") else self.host
            try:
                socket.create_connection((wake_host, self.port), timeout=0.5).close()
            except OSError:
                pass
            for op in (lambda: self._sock.shutdown(socket.SHUT_RDWR), self._sock.close):
                try:
                    op()
                except OSError:
                    pass
        self.delivery.stop()
        with self._lock:
            conns = list(self._conns)
        for c in conns:
            c.stream.close()
        if self._accept_thread:
            self._accept_thread.join(timeout=2)
        log.info("broker stopped")

    def wait(self) -> None:
        while not self._stopped.wait(0.5):
            pass

    # ------------------------------------------------------------ accepting
    def _accept_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                sock, addr = self._sock.accept()
            except OSError:
                break                                     # listening socket closed
            if self._stopped.is_set():                    # the wake-up connection from stop()
                sock.close()
                break
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            conn = ClientConnection(sock, addr)
            with self._lock:
                self._conns.add(conn)
            threading.Thread(target=self._handle, args=(conn,),
                             name=f"conn-{addr[1]}", daemon=True).start()

    def _handle(self, conn: ClientConnection) -> None:
        """Reader loop for one client (runs in its own thread)."""
        log.debug("accepted %s", conn.label())
        try:
            while not conn.closing:
                try:
                    frame = conn.stream.recv()
                except ProtocolError as err:
                    self._send_error(conn, err)
                    if err.fatal:
                        break
                    continue
                if frame is None:
                    break                                 # EOF: peer closed
                try:
                    self._dispatch(conn, frame)
                except ProtocolError as err:
                    self._send_error(conn, err, frame.get("req_id"))
                    if err.fatal:
                        break
                except Exception:                         # storage failure etc.
                    log.exception("error handling %s from %s", frame.get("type"), conn.label())
                    self._send_error(conn, ProtocolError(ErrorCode.INTERNAL_ERROR, "internal broker error",
                                                         ref=frame.get("msg_id")), frame.get("req_id"))
        except OSError:
            pass                                          # reset / closed under us
        finally:
            self._cleanup(conn)

    def _cleanup(self, conn: ClientConnection) -> None:
        with self._lock:
            self._conns.discard(conn)
            if conn.client_id and self._clients.get(conn.client_id) is conn:
                del self._clients[conn.client_id]
        if conn.role == P.ROLE_SUBSCRIBER and conn.client_id:
            self.delivery.detach(conn.client_id, conn.stream)
        conn.stream.close()
        if conn.client_id:
            log.info("DISCONNECT %s (%s)", conn.client_id, conn.role)

    # ------------------------------------------------------------ dispatch
    def _dispatch(self, conn: ClientConnection, f: Dict[str, Any]) -> None:
        ftype = f["type"]
        if ftype == P.PING:
            return self._reply(conn, f, {"type": P.PONG})
        if ftype == P.CONNECT:
            return self._on_connect(conn, f)
        if ftype not in P.CLIENT_TO_BROKER:
            raise ProtocolError(ErrorCode.UNKNOWN_TYPE, f"unknown message type {ftype!r}")
        if conn.client_id is None:
            raise ProtocolError(ErrorCode.NOT_CONNECTED, f"send CONNECT before {ftype}")
        if ftype == P.DISCONNECT:
            conn.closing = True
            return
        if ftype in (P.SUBSCRIBE, P.UNSUBSCRIBE, P.ACK):
            self._require_role(conn, P.ROLE_SUBSCRIBER, ftype)
        if ftype == P.PUBLISH:
            self._require_role(conn, P.ROLE_PUBLISHER, ftype)
            return self._on_publish(conn, f)
        if ftype == P.SUBSCRIBE:
            granted = self.router.subscribe(conn.client_id, P.validate_patterns(f.get("topics")))
            return self._reply(conn, f, {"type": P.SUBACK, "topics": granted,
                                         "subscriptions": self.registry.patterns_for(conn.client_id)})
        if ftype == P.UNSUBSCRIBE:
            removed = self.router.unsubscribe(conn.client_id, P.validate_patterns(f.get("topics")))
            return self._reply(conn, f, {"type": P.UNSUBACK, "topics": removed,
                                         "subscriptions": self.registry.patterns_for(conn.client_id)})
        if ftype == P.ACK:
            self.delivery.ack(conn.client_id, P.validate_msg_id(f.get("msg_id")))
            return

    def _on_connect(self, conn: ClientConnection, f: Dict[str, Any]) -> None:
        if conn.client_id is not None:
            raise ProtocolError(ErrorCode.ALREADY_CONNECTED, "CONNECT already received on this connection")
        version = f.get("version", P.PROTOCOL_VERSION)
        if version != P.PROTOCOL_VERSION:
            raise ProtocolError(ErrorCode.UNSUPPORTED_VERSION,
                                f"protocol version {version!r} not supported (broker speaks {P.PROTOCOL_VERSION})",
                                fatal=True)
        client_id = P.validate_client_id(f.get("client_id"))
        role = f.get("role")
        if role not in P.ROLES:
            raise ProtocolError(ErrorCode.MISSING_FIELD, '"role" must be "publisher" or "subscriber"')

        # Session takeover: if this client_id is still attached on another
        # socket (e.g. the old TCP connection is half-open after a crash),
        # the newest connection wins and the old one is closed.
        with self._lock:
            old = self._clients.get(client_id)
            conn.client_id, conn.role = client_id, role
            self._clients[client_id] = conn
        if old is not None and old is not conn:
            log.info("TAKEOVER %s: closing previous connection %s", client_id, old.label())
            if old.role == P.ROLE_SUBSCRIBER:
                self.delivery.detach(client_id, old.stream)
            old.stream.close()

        subs = self.registry.patterns_for(client_id) if role == P.ROLE_SUBSCRIBER else []
        # CONNACK is sent BEFORE attach() so it always precedes backlog DELIVERs.
        self._reply(conn, f, {"type": P.CONNACK, "client_id": client_id, "role": role,
                              "version": P.PROTOCOL_VERSION,
                              "session_present": bool(subs), "subscriptions": subs})
        backlog = 0
        if role == P.ROLE_SUBSCRIBER:
            backlog = self.delivery.attach(client_id, conn.stream)
        log.info("CONNECT %s as %s from %s:%d%s", client_id, role, conn.addr[0], conn.addr[1],
                 f" (durable session restored: {subs}, backlog={backlog})" if subs else "")

    def _on_publish(self, conn: ClientConnection, f: Dict[str, Any]) -> None:
        msg_id = P.validate_msg_id(f.get("msg_id"))
        topic = P.validate_topic(f.get("topic"))
        if "payload" not in f:
            raise ProtocolError(ErrorCode.MISSING_FIELD, '"payload" is required', ref=msg_id)
        ts = f.get("timestamp")
        if not isinstance(ts, int) or isinstance(ts, bool):
            ts = P.now_ms()
        is_new, recipients = self.router.publish(conn.client_id, msg_id, topic, f["payload"], ts)
        # PUBACK only after save_message() has committed -> a PUBACKed message
        # can never be lost by the broker.
        self._reply(conn, f, {"type": P.PUBACK, "msg_id": msg_id, "duplicate": not is_new,
                              "recipients": len(recipients)})

    # ------------------------------------------------------------ helpers
    @staticmethod
    def _require_role(conn: ClientConnection, role: str, ftype: str) -> None:
        if conn.role != role:
            raise ProtocolError(ErrorCode.ROLE_VIOLATION, f"{ftype} is only allowed for {role}s")

    @staticmethod
    def _reply(conn: ClientConnection, request: Dict[str, Any], frame: Dict[str, Any]) -> None:
        if "req_id" in request:
            frame["req_id"] = request["req_id"]
        conn.stream.send(frame)

    @staticmethod
    def _send_error(conn: ClientConnection, err: ProtocolError, req_id: Any = None) -> None:
        log.warning("ERROR to %s: %s %s", conn.label(), err.code, err.message)
        try:
            conn.stream.send(err.to_frame(req_id))
        except OSError:
            pass


# ---------------------------------------------------------------- entry point
def create_store(kind: str, db_path: str) -> QueueStore:
    if kind == "memory":
        from storage.memory_store import MemoryQueueStore
        log.warning("using IN-MEMORY store: queues are NOT persisted across broker restarts")
        return MemoryQueueStore()
    try:
        from storage.queue_store import SQLiteQueueStore   # M2 - Labrador
    except ImportError as exc:
        sys.exit(f"cannot load storage.queue_store.SQLiteQueueStore ({exc}). "
                 f"Run with --store memory until M2 is merged.")
    return SQLiteQueueStore(db_path)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="python -m broker", description="Publish-subscribe broker")
    ap.add_argument("--host", default=P.DEFAULT_HOST)
    ap.add_argument("--port", type=int, default=P.DEFAULT_PORT)
    ap.add_argument("--store", choices=["sqlite", "memory"], default="sqlite")
    ap.add_argument("--db", default="broker.db", help="SQLite file (with --store sqlite)")
    ap.add_argument("--ack-timeout", type=float, default=5.0, help="seconds before redelivery")
    ap.add_argument("--max-inflight", type=int, default=32, help="un-acked messages per subscriber")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    args = ap.parse_args(argv)

    logging.basicConfig(level=args.log_level,
                        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    store = create_store(args.store, args.db)
    broker = Broker(store, args.host, args.port, args.ack_timeout, args.max_inflight)
    signal.signal(signal.SIGINT, lambda *_: broker.stop())
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_: broker.stop())
    broker.start()
    try:
        broker.wait()
    finally:
        broker.stop()
        store.close()


if __name__ == "__main__":
    main()