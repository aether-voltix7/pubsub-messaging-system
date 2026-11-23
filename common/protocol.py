"""
Wire protocol shared by the broker, publisher and subscriber.

Every frame is one JSON object encoded as UTF-8 and terminated by a single
newline ("\\n"), i.e. newline-delimited JSON over a TCP stream. TCP gives us a
reliable *byte stream*, not message boundaries (Coulouris et al., Sec 4.2.4),
so the newline is our framing marker. See PROTOCOL.md for the full spec.

Owner: Jainar (M1). Changes require Jainar approval + team notice.
"""
from __future__ import annotations

import json
import re
import socket
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

PROTOCOL_VERSION = 1
ENCODING = "utf-8"
MAX_FRAME_BYTES = 64 * 1024          # one frame (including the newline)
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5555

# --------------------------------------------------------------------------
# Message types
# --------------------------------------------------------------------------
CONNECT = "CONNECT"          # client -> broker
CONNACK = "CONNACK"          # broker -> client
SUBSCRIBE = "SUBSCRIBE"      # subscriber -> broker
SUBACK = "SUBACK"            # broker -> subscriber
UNSUBSCRIBE = "UNSUBSCRIBE"  # subscriber -> broker
UNSUBACK = "UNSUBACK"        # broker -> subscriber
PUBLISH = "PUBLISH"          # publisher -> broker
PUBACK = "PUBACK"            # broker -> publisher
DELIVER = "DELIVER"          # broker -> subscriber
ACK = "ACK"                  # subscriber -> broker
PING = "PING"                # client -> broker (optional keep-alive)
PONG = "PONG"                # broker -> client
DISCONNECT = "DISCONNECT"    # client -> broker (graceful close)
ERROR = "ERROR"              # broker -> client

CLIENT_TO_BROKER = {CONNECT, SUBSCRIBE, UNSUBSCRIBE, PUBLISH, ACK, PING, DISCONNECT}

ROLE_PUBLISHER = "publisher"
ROLE_SUBSCRIBER = "subscriber"
ROLES = {ROLE_PUBLISHER, ROLE_SUBSCRIBER}


class ErrorCode:
    """Error codes carried in ERROR frames (numeric code + symbolic name)."""
    MALFORMED_FRAME = (400, "MALFORMED_FRAME")     # not JSON / not an object / no "type"
    MISSING_FIELD = (401, "MISSING_FIELD")         # required field absent or wrong type
    INVALID_TOPIC = (402, "INVALID_TOPIC")         # bad topic name or pattern
    INVALID_CLIENT_ID = (403, "INVALID_CLIENT_ID")
    UNKNOWN_TYPE = (404, "UNKNOWN_TYPE")           # unrecognised "type"
    NOT_CONNECTED = (405, "NOT_CONNECTED")         # frame sent before CONNECT
    ALREADY_CONNECTED = (406, "ALREADY_CONNECTED") # second CONNECT on same socket
    ROLE_VIOLATION = (407, "ROLE_VIOLATION")       # e.g. publisher sends SUBSCRIBE
    FRAME_TOO_LARGE = (413, "FRAME_TOO_LARGE")     # fatal: connection is closed
    UNSUPPORTED_VERSION = (426, "UNSUPPORTED_VERSION")
    INTERNAL_ERROR = (500, "INTERNAL_ERROR")       # e.g. storage failure; retry later


class ProtocolError(Exception):
    """Raised when a peer violates the protocol. `fatal` closes the connection."""

    def __init__(self, code, message: str, ref: Optional[str] = None, fatal: bool = False):
        super().__init__(message)
        self.code_num, self.code = code
        self.message = message
        self.ref = ref
        self.fatal = fatal

    def to_frame(self, req_id: Any = None) -> Dict[str, Any]:
        frame = {"type": ERROR, "code": self.code_num, "error": self.code, "message": self.message}
        if self.ref is not None:
            frame["ref"] = self.ref
        if req_id is not None:
            frame["req_id"] = req_id
        return frame


# --------------------------------------------------------------------------
# Encoding / decoding
# --------------------------------------------------------------------------
def encode(frame: Dict[str, Any]) -> bytes:
    """Serialise one frame to newline-terminated UTF-8 JSON."""
    data = json.dumps(frame, separators=(",", ":"), ensure_ascii=False).encode(ENCODING) + b"\n"
    if len(data) > MAX_FRAME_BYTES:
        raise ProtocolError(ErrorCode.FRAME_TOO_LARGE, f"frame is {len(data)} bytes; max {MAX_FRAME_BYTES}")
    return data


def decode(line: bytes) -> Dict[str, Any]:
    """Parse one line into a frame dict, validating the envelope only."""
    try:
        obj = json.loads(line.decode(ENCODING))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(ErrorCode.MALFORMED_FRAME, f"invalid JSON: {exc}")
    if not isinstance(obj, dict):
        raise ProtocolError(ErrorCode.MALFORMED_FRAME, "frame must be a JSON object")
    if not isinstance(obj.get("type"), str):
        raise ProtocolError(ErrorCode.MALFORMED_FRAME, 'frame must have a string "type" field')
    return obj


class FrameStream:
    """
    Thread-safe newline-delimited JSON stream over a connected TCP socket.

    - send() may be called from several threads (a lock serialises writes so
      frames never interleave on the wire).
    - recv() must be called from a single reader thread. It returns None on a
      clean EOF (peer closed the connection).
    - close() may be called from any thread, even while another thread is
      blocked in recv(); that recv() then returns None or raises OSError.
    Publisher/subscriber clients are free to reuse this class.

    Reads use sock.recv() into our own buffer rather than sock.makefile():
    on Windows, closing a makefile() reader blocks while another thread is
    inside readline(), and the socket is not really closed while a makefile()
    is open, so a blocked reader is never woken. Owning the buffer avoids both.
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._buf = bytearray()
        self._wlock = threading.Lock()
        self._closed = False

    def send(self, frame: Dict[str, Any]) -> None:
        data = encode(frame)
        with self._wlock:
            self.sock.sendall(data)

    def recv(self) -> Optional[Dict[str, Any]]:
        while True:
            nl = self._buf.find(b"\n")
            if nl != -1:
                if nl + 1 > MAX_FRAME_BYTES:
                    raise ProtocolError(ErrorCode.FRAME_TOO_LARGE,
                                        f"frame exceeds {MAX_FRAME_BYTES} bytes", fatal=True)
                line = bytes(self._buf[:nl]).strip()
                del self._buf[:nl + 1]
                if line:                                      # ignore blank keep-alive lines
                    return decode(line)
                continue
            if len(self._buf) >= MAX_FRAME_BYTES:             # no newline within the limit
                raise ProtocolError(ErrorCode.FRAME_TOO_LARGE,
                                    f"frame exceeds {MAX_FRAME_BYTES} bytes", fatal=True)
            if self._closed:
                return None
            chunk = self.sock.recv(65536)
            if not chunk:
                return None                                   # EOF (a partial frame is dropped)
            self._buf += chunk

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # shutdown() wakes a blocked recv() on Linux/macOS; close() really
        # closes the socket (no makefile() reference keeps it open), which
        # aborts a blocked recv() on Windows.
        for op in (lambda: self.sock.shutdown(socket.SHUT_RDWR), self.sock.close):
            try:
                op()
            except OSError:
                pass


# --------------------------------------------------------------------------
# Field validation
# --------------------------------------------------------------------------
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
_CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
MAX_TOPIC_LEN = 200
MAX_MSG_ID_LEN = 128


def require(frame: Dict[str, Any], field: str, kind, ref: Optional[str] = None):
    value = frame.get(field)
    # bool is a subclass of int in Python, so reject it explicitly
    if value is None or not isinstance(value, kind) or (isinstance(value, bool) and kind is not bool):
        raise ProtocolError(ErrorCode.MISSING_FIELD, f'"{field}" is required ({_kind_name(kind)})', ref=ref)
    return value


def _kind_name(kind) -> str:
    if isinstance(kind, tuple):
        return " or ".join(k.__name__ for k in kind)
    return kind.__name__


def validate_client_id(client_id: Any) -> str:
    if not isinstance(client_id, str) or not _CLIENT_ID_RE.match(client_id):
        raise ProtocolError(ErrorCode.INVALID_CLIENT_ID,
                            "client_id must be 1-64 chars of letters, digits, '.', '_' or '-'")
    return client_id


def validate_topic(topic: Any) -> str:
    """A concrete topic used in PUBLISH: dot-separated segments, no wildcards."""
    if not isinstance(topic, str) or not topic or len(topic) > MAX_TOPIC_LEN:
        raise ProtocolError(ErrorCode.INVALID_TOPIC, "topic must be a non-empty string")
    for seg in topic.split("."):
        if not _SEGMENT_RE.match(seg):
            raise ProtocolError(ErrorCode.INVALID_TOPIC,
                                f"invalid topic {topic!r}: segments are [A-Za-z0-9_-]+, wildcards not allowed")
    return topic


def validate_pattern(pattern: Any) -> str:
    """A subscription pattern: like a topic, but '*' = exactly one segment and
    '#' = zero or more trailing segments (only allowed as the last segment)."""
    if not isinstance(pattern, str) or not pattern or len(pattern) > MAX_TOPIC_LEN:
        raise ProtocolError(ErrorCode.INVALID_TOPIC, "topic pattern must be a non-empty string")
    segs = pattern.split(".")
    for i, seg in enumerate(segs):
        if seg == "*":
            continue
        if seg == "#":
            if i != len(segs) - 1:
                raise ProtocolError(ErrorCode.INVALID_TOPIC, f"'#' must be the last segment in {pattern!r}")
            continue
        if not _SEGMENT_RE.match(seg):
            raise ProtocolError(ErrorCode.INVALID_TOPIC, f"invalid topic pattern {pattern!r}")
    return pattern


def validate_patterns(topics: Any) -> List[str]:
    if isinstance(topics, str):
        topics = [topics]
    if not isinstance(topics, list) or not topics:
        raise ProtocolError(ErrorCode.MISSING_FIELD, '"topics" must be a non-empty list of strings')
    return [validate_pattern(t) for t in topics]


def validate_msg_id(msg_id: Any) -> str:
    if not isinstance(msg_id, str) or not msg_id or len(msg_id) > MAX_MSG_ID_LEN:
        raise ProtocolError(ErrorCode.MISSING_FIELD, '"msg_id" must be a non-empty string (<=128 chars)')
    return msg_id


# --------------------------------------------------------------------------
# Small helpers for clients
# --------------------------------------------------------------------------
def new_msg_id() -> str:
    """Globally unique message id (publishers should use this)."""
    return str(uuid.uuid4())


def now_ms() -> int:
    return int(time.time() * 1000)