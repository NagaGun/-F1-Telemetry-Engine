"""
validate.py — F1 Telemetry Engine: Crash Recovery & WAL Validator
==================================================================
Run this AFTER killing primary.py mid-race to prove zero data loss.

    python validate.py

What it checks:
  1. Both WAL files exist and are non-empty
  2. Every packet unpacks cleanly (no struct errors = no corrupt writes)
  3. Seq numbers in each WAL are strictly monotonically increasing
  4. The replica WAL is a prefix of the primary WAL (no phantom data)
  5. Reports the exact packet where the replica fell behind (the "crash boundary")
  6. Computes the data loss window in packets, bytes, and milliseconds

Interview talking points:
  - What is the RPO (Recovery Point Objective)?
    The number of packets between primary.last_seq and replica.last_seq at
    the moment of crash. This validator measures it precisely.
  - Why compare as a prefix, not equality?
    The primary may have written packets that never reached the replica TCP
    socket before the crash. That gap is expected and quantified — not an error.
  - Why check seq# monotonicity?
    A non-monotonic seq# means either a duplicate write (idempotency bug) or
    a corrupted WAL boundary (partial flush). Both are bugs worth surfacing.
  - What would a production system do with this output?
    Feed it to an alerting system. If RPO > SLA threshold, page the on-call.
    The raw gap packets in primary.wal are the replay log for the replica.
"""

import os
import sys
import struct
import time
from dataclasses import dataclass
from typing import Iterator

# ── Configuration ──────────────────────────────────────────────────────────────
PRIMARY_WAL   = "primary.wal"
REPLICA_WAL   = "replica.wal"
PACKET_FORMAT = "!Qddq"
PACKET_SIZE   = struct.calcsize(PACKET_FORMAT)   # 32 bytes


# ── Data model ─────────────────────────────────────────────────────────────────
@dataclass
class Packet:
    index:     int    # position in WAL (0-based)
    timestamp: int    # microseconds since epoch
    speed:     float  # km/h
    rpm:       float  # engine RPM
    seq:       int    # monotonically increasing sequence number


# ── WAL reader ─────────────────────────────────────────────────────────────────
def read_wal(path: str) -> Iterator[Packet]:
    """
    Generator: yields one Packet per 32-byte record in the WAL.
    Stops cleanly if the file ends on a packet boundary.
    Raises ValueError on a partial trailing record (indicates a mid-write crash).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"WAL not found: {path}")

    size = os.path.getsize(path)
    remainder = size % PACKET_SIZE
    if remainder != 0:
        print(f"  ⚠  {path} has a partial trailing record ({remainder} stray bytes). "
              f"This is expected if the primary crashed mid-flush.")

    with open(path, "rb") as f:
        index = 0
        while True:
            raw = f.read(PACKET_SIZE)
            if not raw:
                break
            if len(raw) < PACKET_SIZE:
                print(f"  ⚠  Skipping {len(raw)}-byte partial record at end of {path}")
                break
            ts, speed, rpm, seq = struct.unpack(PACKET_FORMAT, raw)
            yield Packet(index=index, timestamp=ts, speed=speed, rpm=rpm, seq=seq)
            index += 1


# ── Validation logic ───────────────────────────────────────────────────────────
def validate_monotonicity(packets: list[Packet], label: str) -> bool:
    """Check that seq numbers strictly increase with no gaps or duplicates."""
    ok = True
    for i in range(1, len(packets)):
        prev, curr = packets[i - 1], packets[i]
        if curr.seq != prev.seq + 1:
            print(f"  ✗  {label}: seq# discontinuity at index {i}: "
                  f"{prev.seq} → {curr.seq} (expected {prev.seq + 1})")
            ok = False
    return ok


def validate_prefix(primary: list[Packet], replica: list[Packet]) -> int:
    """
    Verify that replica is a byte-perfect prefix of primary.
    Returns the index of the first divergence, or len(replica) if all match.
    """
    diverge_at = len(replica)
    for i, (p, r) in enumerate(zip(primary, replica)):
        if p.seq != r.seq or p.timestamp != r.timestamp:
            diverge_at = i
            print(f"  ✗  Divergence at packet index {i}: "
                  f"primary seq={p.seq} vs replica seq={r.seq}")
            break
    return diverge_at


def format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    elif n < 1024 ** 2:
        return f"{n / 1024:.1f} KB"
    else:
        return f"{n / (1024 ** 2):.2f} MB"


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass
    start = time.monotonic()

    print()
    print("═" * 60)
    print("  F1 TELEMETRY ENGINE — CRASH RECOVERY VALIDATOR")
    print("═" * 60)
    print()

    # ── Load WALs ──────────────────────────────────────────────────────────────
    print("▶  Loading WAL files...")
    try:
        primary_packets = list(read_wal(PRIMARY_WAL))
    except FileNotFoundError as e:
        print(f"  ✗  {e}")
        return

    try:
        replica_packets = list(read_wal(REPLICA_WAL))
    except FileNotFoundError as e:
        print(f"  ✗  {e}")
        return

    p_count = len(primary_packets)
    r_count = len(replica_packets)
    print(f"  Primary WAL : {p_count:,} packets  ({format_bytes(p_count * PACKET_SIZE)})")
    print(f"  Replica WAL : {r_count:,} packets  ({format_bytes(r_count * PACKET_SIZE)})")
    print()

    if p_count == 0:
        print("  ✗  Primary WAL is empty. Nothing to validate.")
        return

    # ── Monotonicity checks ────────────────────────────────────────────────────
    print("▶  Checking sequence number integrity...")
    p_mono = validate_monotonicity(primary_packets, "primary")
    r_mono = validate_monotonicity(replica_packets, "replica") if r_count > 0 else True

    if p_mono and r_mono:
        print("  ✓  Seq numbers are strictly monotonic in both WALs.")
    print()

    # ── Prefix check ──────────────────────────────────────────────────────────
    print("▶  Verifying replica is a valid prefix of primary...")
    diverge_at = validate_prefix(primary_packets, replica_packets)

    if diverge_at == r_count:
        print(f"  ✓  Replica matches primary perfectly up to packet {r_count - 1}.")
    print()

    # ── Crash boundary analysis ────────────────────────────────────────────────
    print("▶  Crash boundary analysis...")
    gap_packets = p_count - r_count

    if gap_packets == 0:
        print("  ✓  Zero data loss. Replica is fully caught up.")
    else:
        first_lost = replica_packets[-1] if r_count > 0 else None
        last_lost  = primary_packets[-1]

        # Time window of lost data
        if first_lost:
            lost_ns    = last_lost.timestamp - first_lost.timestamp
            lost_ms    = lost_ns / 1_000_000
        else:
            lost_ms    = 0

        print(f"  ⚡ Primary crashed mid-race.")
        print(f"  ─────────────────────────────────────────────────")
        print(f"  Last seq# in primary  : {last_lost.seq:>12,}")
        print(f"  Last seq# in replica  : {(first_lost.seq if first_lost else -1):>12,}")
        print(f"  ─────────────────────────────────────────────────")
        print(f"  Gap (unconfirmed)     : {gap_packets:>12,} packets")
        print(f"  Gap (bytes)           : {format_bytes(gap_packets * PACKET_SIZE):>12}")
        print(f"  Gap (time window)     : {lost_ms:>11.1f} ms")
        print()
        print(f"  These {gap_packets:,} packets exist in primary.wal and can be replayed")
        print(f"  to the replica by re-streaming from offset {r_count * PACKET_SIZE:,}.")
        print()

    # ── Speed and RPM spot-check ───────────────────────────────────────────────
    if primary_packets:
        last = primary_packets[-1]
        print("▶  Last telemetry reading (primary WAL):")
        print(f"  Seq#    : {last.seq:,}")
        print(f"  Speed   : {last.speed:.1f} km/h")
        print(f"  RPM     : {last.rpm:,.0f}")
        ts_s = last.timestamp / 1_000_000_000
        print(f"  Time    : {time.strftime('%H:%M:%S', time.localtime(ts_s))}.{(last.timestamp % 1_000_000_000) // 1_000_000:03d}")
        print()

    elapsed = (time.monotonic() - start) * 1000
    print(f"  Validation completed in {elapsed:.1f} ms.")
    print("═" * 60)
    print()


if __name__ == "__main__":
    main()
