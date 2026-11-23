"""Publisher client for the project's frozen Pub/Sub protocol (v1)."""
from __future__ import annotations

import queue
import socket
import threading
import time
from typing import Any, Dict, Optional

from common import protocol as P
from common.protocol import FrameStream, ProtocolError


class PublisherClient:
    """Synchronous publisher with PUBACK timeout and retry support."""

    def __init__(
        self,
        client_id: str,
        host: str = P.DEFAULT_HOST,
        port: int = P.DEFAULT_PORT,
        ack_timeout: float = 5.0,
        max_retries: int = 3,
        retry_delay: float = 0.25,
    ) -> None:
        self.client_id = client_id
        self.host = host
        self.port = port
        self.ack_timeout = ack_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.sock: Optional[socket.socket] = None
        self.stream: Optional[FrameStream] = None
        self.connected = False

        self._responses: "queue.Queue[Any]" = queue.Queue()
        self._reader_thread: Optional[threading.Thread] = None
        self._publish_lock = threading.Lock()
        self._state_lock = threading.Lock()

    def _reader_loop(
        self, stream: FrameStream, responses: "queue.Queue[Any]"
    ) -> None:
        """Read broker frames; this is the only thread that calls recv()."""
        try:
            while True:
                frame = stream.recv()
                if frame is None:
                    responses.put(ConnectionError("Broker closed the connection."))
                    return
                responses.put(frame)
        except (OSError, ProtocolError, ValueError) as exc:
            responses.put(exc)

    def _close_connection(self) -> None:
        with self._state_lock:
            stream, sock = self.stream, self.sock
            self.stream = None
            self.sock = None
            self.connected = False

        if stream is not None:
            stream.close()
        elif sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def _next_response(self, timeout: float) -> Dict[str, Any]:
        try:
            item = self._responses.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError("Timed out waiting for broker response.") from exc

        if isinstance(item, BaseException):
            raise ConnectionError(str(item)) from item

        if not isinstance(item, dict):
            raise ConnectionError("Received an invalid broker response.")
        return item

    def connect(self) -> bool:
        """Connect and complete the CONNECT/CONNACK handshake."""
        if self.connected:
            return True

        self._close_connection()
        self._responses = queue.Queue()

        try:
            sock = socket.create_connection(
                (self.host, self.port), timeout=self.ack_timeout
            )
            # FrameStream uses a buffered file reader; keep the socket blocking.
            sock.settimeout(None)
            stream = FrameStream(sock)

            with self._state_lock:
                self.sock = sock
                self.stream = stream

            response_queue = self._responses
            self._reader_thread = threading.Thread(
                target=self._reader_loop,
                args=(stream, response_queue),
                name=f"publisher-reader-{self.client_id}",
                daemon=True,
            )
            self._reader_thread.start()

            stream.send({
                "type": P.CONNECT,
                "client_id": self.client_id,
                "role": P.ROLE_PUBLISHER,
                "version": P.PROTOCOL_VERSION,
            })

            response = self._next_response(self.ack_timeout)

            if response.get("type") == P.ERROR:
                raise RuntimeError(
                    f"Broker rejected CONNECT: "
                    f"{response.get('error')} - {response.get('message')}"
                )

            if response.get("type") != P.CONNACK:
                raise RuntimeError(
                    f"Expected CONNACK, received {response.get('type')!r}"
                )

            self.connected = True
            print(
                f"[CONNECTED] {self.client_id} -> "
                f"{self.host}:{self.port}"
            )
            return True

        except (OSError, TimeoutError, ConnectionError, RuntimeError,
                ProtocolError) as exc:
            print(f"[CONNECT ERROR] {exc}")
            self._close_connection()
            return False

    def publish(self, topic: str, payload: Any) -> bool:
        """Publish one message; retries reuse the exact same msg_id/frame."""
        with self._publish_lock:
            if not self.connected and not self.connect():
                return False

            msg_id = P.new_msg_id()
            frame = {
                "type": P.PUBLISH,
                "msg_id": msg_id,
                "topic": topic,
                "payload": payload,
                "timestamp": P.now_ms(),
            }

            # max_retries means retries after the initial attempt.
            total_attempts = self.max_retries + 1

            for attempt in range(1, total_attempts + 1):
                try:
                    if not self.connected and not self.connect():
                        raise ConnectionError("Could not reconnect to broker.")

                    assert self.stream is not None
                    print(
                        f"[PUBLISH] topic={topic} msg_id={msg_id} "
                        f"attempt={attempt}/{total_attempts}"
                    )
                    self.stream.send(frame)

                    deadline = time.monotonic() + self.ack_timeout
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("PUBACK timeout.")

                        response = self._next_response(remaining)
                        response_type = response.get("type")

                        if response_type == P.ERROR:
                            # Internal broker/storage errors are retryable.
                            if response.get("error") == "INTERNAL_ERROR":
                                raise RuntimeError(
                                    response.get("message", "Broker internal error")
                                )
                            print(
                                f"[PUBLISH REJECTED] "
                                f"{response.get('error')}: "
                                f"{response.get('message')}"
                            )
                            return False

                        if response_type != P.PUBACK:
                            # Ignore unrelated frames; keep waiting for this ACK.
                            continue

                        if response.get("msg_id") != msg_id:
                            continue

                        print(
                            f"[PUBACK] msg_id={msg_id} "
                            f"recipients={response.get('recipients', 0)} "
                            f"duplicate={response.get('duplicate', False)}"
                        )
                        return True

                except (OSError, TimeoutError, ConnectionError, RuntimeError,
                        ProtocolError) as exc:
                    print(f"[RETRY] {exc}")
                    self._close_connection()

                    if attempt < total_attempts:
                        time.sleep(self.retry_delay)

            print(
                f"[PUBLISH FAILED] msg_id={msg_id} after "
                f"{total_attempts} total attempt(s)."
            )
            return False

    def disconnect(self) -> None:
        """Gracefully end the publisher session."""
        stream = self.stream
        if self.connected and stream is not None:
            try:
                stream.send({"type": P.DISCONNECT})
            except OSError:
                pass

        self._close_connection()
        print(f"[DISCONNECTED] {self.client_id}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test the Pub/Sub publisher.")
    parser.add_argument("--id", default="stock-exchange")
    parser.add_argument("--host", default=P.DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=P.DEFAULT_PORT)
    parser.add_argument("--topic", default="stocks.tech")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--price", type=float, default=191.20)
    args = parser.parse_args()

    publisher = PublisherClient(args.id, args.host, args.port)
    try:
        if publisher.connect():
            publisher.publish(
                args.topic,
                {"symbol": args.symbol, "price": args.price, "change_pct": 0.0},
            )
    finally:
        publisher.disconnect()
