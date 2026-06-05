"""
replica.py — F1 Telemetry Engine: Replica Node
================================================
Responsibilities:
  1. TCP listener   — accepts one persistent connection from primary.py
  2. Seq# handshake — on connect, sends last known seq# so primary can resume
                      from the right offset (zero-gap recovery)
  3. WAL writer     — appends incoming byte chunks to replica.wal

Packet format (struct '!Qddq' = 32 bytes):
  [0:8 ] timestamp  — unsigned 64-bit int (microseconds since epoch)
  [8:16] speed      — 64-bit double (km/h)
  [16:24] rpm       — 64-bit double (engine RPM)
  [24:32] seq       — signed 64-bit int (monotonically increasing)

Interview talking points:
  - Why TCP for replication (not UDP)?  Replication requires guaranteed,
    ordered delivery. A dropped chunk leaves the replica in a corrupt state
    that validate.py would catch — but better to prevent it entirely.
  - Why send last seq# on reconnect?  So the primary can seek its WAL to
    that offset and retransmit only the missed packets. Without this, a
    primary crash + restart loses the gap between the two WALs.
  - Why append-only (open "ab")?  If we overwrote on reconnect we'd lose
    already-durable data. The WAL only grows; validate.py determines correctness.
  - What happens if the primary dies mid-chunk?  We may receive a partial
    32-byte packet. The recv_exact() helper detects this and logs it — the
    validate script will flag the truncation at the boundary.
"""

import socket
import struct
import os
import time

# ── Configuration ──────────────────────────────────────────────────────────────
LISTEN_HOST   = "0.0.0.0"
LISTEN_PORT   = 9001
WAL_PATH      = "replica.wal"
PACKET_FORMAT = "!Qddq"
PACKET_SIZE   = struct.calcsize(PACKET_FORMAT)   # 32 bytes

# Handshake: replica sends this 8-byte frame to primary on connect
# Format: '!q' = signed 64-bit int (last seq# written, or -1 if empty)
HANDSHAKE_FORMAT = "!q"
HANDSHAKE_SIZE   = struct.calcsize(HANDSHAKE_FORMAT)   # 8 bytes


# ── WAL helpers ────────────────────────────────────────────────────────────────
def get_last_seq() -> int:
    """
    Read the last seq# from replica.wal without loading the whole file.
    Seeks to the last 32-byte record and unpacks offset 24.
    Returns -1 if the WAL doesn't exist or is empty/corrupt.

    Interview: O(1) regardless of WAL size — we seek, not scan.
    """
    if not os.path.exists(WAL_PATH):
        return -1
    size = os.path.getsize(WAL_PATH)
    if size < PACKET_SIZE:
        return -1
    # Align to last complete packet boundary
    aligned = (size // PACKET_SIZE) * PACKET_SIZE
    try:
        with open(WAL_PATH, "rb") as f:
            f.seek(aligned - PACKET_SIZE)
            raw = f.read(PACKET_SIZE)
            if len(raw) != PACKET_SIZE:
                return -1
            return struct.unpack_from("!q", raw, 24)[0]
    except OSError:
        return -1


def recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """
    Read exactly n bytes from a socket, blocking until all arrive.
    Returns None if the connection closed before n bytes were received
    (indicates a mid-chunk primary crash — log and handle gracefully).

    Interview: a plain sock.recv(n) can return fewer than n bytes even
    on a healthy connection (TCP is a stream, not a message protocol).
    recv_exact() is the correct primitive for framed binary protocols.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None   # connection closed
        buf.extend(chunk)
    return bytes(buf)


# ── Connection handler ─────────────────────────────────────────────────────────
def handle_connection(conn: socket.socket, addr):
    """
    Per-connection logic:
      1. Read last seq# from our WAL and send it as the handshake frame
      2. Loop: recv PACKET_SIZE bytes, append to WAL, fsync every 100 packets
    """
    last_seq = get_last_seq()
    print(f"[replica] primary connected from {addr}. Last seq# in WAL: {last_seq}")

    # Send handshake so primary knows where to resume
    try:
        conn.sendall(struct.pack(HANDSHAKE_FORMAT, last_seq))
    except OSError as e:
        print(f"[replica] handshake send failed: {e}")
        conn.close()
        return

    wal      = open(WAL_PATH, "ab")
    received = 0
    written  = 0

    try:
        while True:
            # Receive exactly one packet's worth of bytes
            raw = recv_exact(conn, PACKET_SIZE)

            if raw is None:
                print(f"[replica] primary closed connection (received {received} packets this session)")
                break

            # Sanity check: unpack seq# and verify it's advancing
            seq = struct.unpack_from("!q", raw, 24)[0]

            if seq <= last_seq:
                # Duplicate or out-of-order — primary resumed from a stale offset
                # Don't write; just log. validate.py will confirm WAL integrity.
                print(f"[replica] duplicate seq# {seq} (last={last_seq}), skipping")
                continue

            wal.write(raw)
            written  += 1
            received += 1
            last_seq  = seq

            # Batch fsync every 100 packets (same cadence as primary)
            if written % 100 == 0:
                wal.flush()
                os.fsync(wal.fileno())
                wal_bytes = wal.seek(0, 2)
                print(f"[replica] flushed batch | seq={last_seq} | WAL={wal_bytes:,} bytes")

    except OSError as e:
        print(f"[replica] socket error: {e}")
    finally:
        wal.flush()
        os.fsync(wal.fileno())
        wal.close()
        conn.close()
        print(f"[replica] WAL closed. Total written this session: {written} packets")


# ── Entry Point ────────────────────────────────────────────────────────────────
def main():
    print("=== F1 Telemetry Engine — Replica Node ===")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LISTEN_HOST, LISTEN_PORT))
    server.listen(1)
    print(f"[replica] waiting for primary on {LISTEN_HOST}:{LISTEN_PORT}")

    try:
        while True:
            conn, addr = server.accept()
            # Single-connection model: F1 only has one primary node.
            # If the primary reconnects (after crash), we loop back here.
            handle_connection(conn, addr)
            print("[replica] ready for next primary connection...")
            time.sleep(0.5)   # brief pause before re-accepting

    except KeyboardInterrupt:
        print("\n[replica] shutting down...")
    finally:
        server.close()


if __name__ == "__main__":
    main()
