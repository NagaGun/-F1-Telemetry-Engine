"""
primary.py — F1 Telemetry Engine: Primary Node
================================================
Responsibilities:
  1. UDP listener  — receives 32-byte telemetry packets from car.py
  2. Bounded queue — absorbs traffic spikes; applies backpressure when full
  3. Worker thread  — drains queue, batches WAL writes, replicates over TCP
  4. Stats dict     — shared memory for stats.py to read without extra threads

Packet format (struct '!Qddq' = 32 bytes):
  [0:8 ] timestamp  — unsigned 64-bit int (microseconds since epoch)
  [8:16] speed      — 64-bit double (km/h)
  [16:24] rpm       — 64-bit double (engine RPM)
  [24:32] seq       — signed 64-bit int (monotonically increasing)

Interview talking points:
  - Why UDP for ingestion? Connectionless = no handshake overhead. Packet loss
    at the network layer is acceptable; the WAL + seq numbers catch gaps.
  - Why a bounded queue? Unbounded queues hide backpressure problems until RAM
    runs out. A maxsize forces us to make a deliberate choice (drop vs block).
  - Why batch WAL flushes? fsync on every packet would saturate disk I/O at
    5,000 packets/sec. Batching every 100 packets cuts fsync calls by 99%.
  - Why TCP for replication? Unlike ingestion, replication requires guaranteed,
    ordered delivery — we can't have the replica miss or reorder chunks.
"""

import socket
import struct
import threading
import queue
import time
import os

# ── Configuration ──────────────────────────────────────────────────────────────
UDP_HOST        = "0.0.0.0"
UDP_PORT        = 9000
REPLICA_HOST    = "127.0.0.1"
REPLICA_PORT    = 9001
WAL_PATH        = "primary.wal"
PACKET_FORMAT   = "!Qddq"          # network-order: u64, f64, f64, i64
PACKET_SIZE     = struct.calcsize(PACKET_FORMAT)   # 32 bytes
BATCH_SIZE      = 100              # flush WAL + replicate every N packets
QUEUE_MAXSIZE   = 10_000           # backpressure threshold (~320 KB of packets)

# ── Shared stats dict (written by worker, read by stats.py) ───────────────────
# Using a plain dict + threading.Lock is safe for single-writer, multi-reader.
stats_lock = threading.Lock()
stats = {
    "packets_received": 0,
    "packets_written":  0,
    "queue_depth":      0,
    "wal_bytes":        0,
    "last_seq":         -1,
    "replica_last_seq": -1,   # updated after each successful TCP send
    "drop_count":       0,    # packets dropped due to full queue
    "started_at":       time.monotonic(),
}


def update_stats(**kwargs):
    """Thread-safe stat update."""
    with stats_lock:
        stats.update(kwargs)


# ── TCP Replication ────────────────────────────────────────────────────────────
class ReplicaConnection:
    """
    Maintains a persistent TCP connection to the replica node.
    On connect, sends our current WAL offset so the replica can
    request a resume if it fell behind (handled in replica.py).
    Auto-reconnects with exponential back-off on any socket error.
    """

    def __init__(self, host: str, port: int):
        self.host    = host
        self.port    = port
        self._sock   = None
        self._lock   = threading.Lock()

    def _connect(self):
        backoff = 1
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.connect((self.host, self.port))
                self._sock = s
                print(f"[replica] connected to {self.host}:{self.port}")
                # Read 8-byte handshake containing replica's last written sequence number
                handshake = s.recv(8)
                if len(handshake) == 8:
                    replica_seq = struct.unpack("!q", handshake)[0]
                    update_stats(replica_last_seq=replica_seq)
                    print(f"[replica] handshake received: replica_last_seq={replica_seq}")
                return
            except OSError as e:
                print(f"[replica] connection failed ({e}), retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def send(self, data: bytes) -> bool:
        """
        Send a raw byte chunk to the replica.
        Returns True on success, False if the connection dropped
        (caller should retry on next batch — data is already in WAL).
        """
        with self._lock:
            if self._sock is None:
                self._connect()
            try:
                self._sock.sendall(data)
                return True
            except OSError as e:
                print(f"[replica] send error ({e}), will reconnect on next batch")
                self._sock = None
                return False

    def close(self):
        with self._lock:
            if self._sock:
                self._sock.close()
                self._sock = None


# ── Worker Thread ──────────────────────────────────────────────────────────────
def worker(pkt_queue: queue.Queue, replica: ReplicaConnection, stop_event: threading.Event):
    """
    Drains the queue in batches.
    Each batch is:
      1. Appended to primary.wal as raw bytes
      2. Flushed to OS buffer (flush) — not fsync every packet, only every batch
      3. Sent verbatim over the TCP connection to the replica
    """
    wal = open(WAL_PATH, "ab")   # append-binary; survives process restarts
    batch        = bytearray()
    batch_count  = 0
    last_seq     = -1

    try:
        while not stop_event.is_set() or not pkt_queue.empty():
            try:
                raw = pkt_queue.get(timeout=0.05)
            except queue.Empty:
                # Flush any partial batch on idle so data isn't stuck in buffer
                if batch:
                    _flush_batch(wal, replica, batch, batch_count, last_seq)
                    batch       = bytearray()
                    batch_count = 0
                continue

            # Unpack just the seq number for stats (offset 24, 8 bytes, big-endian)
            last_seq = struct.unpack_from("!q", raw, 24)[0]

            batch       += raw
            batch_count += 1

            if batch_count >= BATCH_SIZE:
                _flush_batch(wal, replica, batch, batch_count, last_seq)
                batch       = bytearray()
                batch_count = 0

    finally:
        if batch:
            _flush_batch(wal, replica, batch, batch_count, last_seq)
        wal.flush()
        wal.close()


def _flush_batch(wal, replica: ReplicaConnection, batch: bytearray, count: int, last_seq: int):
    """Write a batch to WAL and replicate. Updates shared stats."""
    wal.write(batch)
    wal.flush()                              # flush Python buffer to OS
    os.fsync(wal.fileno())                   # guarantee OS → disk (interview: this is the durability guarantee)

    success = replica.send(bytes(batch))     # best-effort; WAL is source of truth

    wal_bytes = wal.seek(0, 2)               # current file size
    if success:
        update_stats(
            packets_written  = stats["packets_written"] + count,
            wal_bytes        = wal_bytes,
            last_seq         = last_seq,
            replica_last_seq = last_seq,
        )
    else:
        update_stats(
            packets_written  = stats["packets_written"] + count,
            wal_bytes        = wal_bytes,
            last_seq         = last_seq,
        )


# ── UDP Listener (main thread) ─────────────────────────────────────────────────
def listen(pkt_queue: queue.Queue, stop_event: threading.Event):
    """
    Tight recvfrom loop on the main thread.
    The ONLY work here is recv → validate size → enqueue.
    Any processing that takes >1µs belongs in the worker thread.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)  # 4 MB kernel buffer
    sock.bind((UDP_HOST, UDP_PORT))
    sock.settimeout(0.5)
    print(f"[udp] listening on {UDP_HOST}:{UDP_PORT}")

    received = 0
    dropped  = 0

    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(PACKET_SIZE)
        except socket.timeout:
            continue
        except OSError:
            break

        if len(data) != PACKET_SIZE:
            print(f"[udp] bad packet size {len(data)} from {addr}, skipping")
            continue

        try:
            pkt_queue.put_nowait(data)
            received += 1
        except queue.Full:
            # Backpressure: worker can't keep up — drop and count
            dropped += 1

        update_stats(
            packets_received = received,
            queue_depth      = pkt_queue.qsize(),
            drop_count       = dropped,
        )

    sock.close()


# ── Entry Point ────────────────────────────────────────────────────────────────
def main():
    print("=== F1 Telemetry Engine — Primary Node ===")

    pkt_queue   = queue.Queue(maxsize=QUEUE_MAXSIZE)
    replica     = ReplicaConnection(REPLICA_HOST, REPLICA_PORT)
    stop_event  = threading.Event()

    import stats as stats_module
    stats_thread = threading.Thread(target=stats_module.run, args=(stats, stats_lock), daemon=True)
    stats_thread.start()

    worker_thread = threading.Thread(
        target=worker,
        args=(pkt_queue, replica, stop_event),
        daemon=True,
        name="wal-worker",
    )
    worker_thread.start()

    try:
        listen(pkt_queue, stop_event)
    except KeyboardInterrupt:
        print("\n[primary] shutting down...")
    finally:
        stop_event.set()
        worker_thread.join(timeout=5)
        replica.close()
        print("[primary] clean shutdown complete")


if __name__ == "__main__":
    main()
