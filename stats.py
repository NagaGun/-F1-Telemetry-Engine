"""
stats.py — F1 Telemetry Engine: Live Metrics Dashboard
========================================================
Reads the shared `stats` dict exported by primary.py and prints a
live terminal table refreshed every second.

Run this in a separate terminal AFTER starting primary.py:
    python stats.py

How it works:
  - Imports the `stats` dict and `stats_lock` directly from primary.py.
    This works because Python modules are singletons within a process —
    primary.py must import and start stats.py in a daemon thread, OR
    you run stats.py as a subprocess that reads a stats socket.

  Both patterns are shown below. The thread pattern (Option A) is simpler
  for a demo. The socket pattern (Option B) is what you'd use in production.

Interview talking points:
  - Why monitor queue depth?  It's your early-warning system. If depth trends
    toward QUEUE_MAXSIZE, the worker can't keep up — you need to tune batch
    size, add more workers, or throttle the simulator.
  - Why track replication lag (primary seq# - replica seq#)?  In a real system
    this is your RPO (Recovery Point Objective) metric. If the primary crashes
    when lag = 500, you lose 500 * 32 bytes = 16 KB of unconfirmed data.
  - Why packets/sec vs total count?  Rate tells you health; total tells you
    progress. Both belong on a production dashboard.
"""

import time
import os
import sys
import threading

# ── Option A: Thread mode (import stats dict from primary) ─────────────────────
# primary.py calls: stats_thread = threading.Thread(target=stats.run, daemon=True)
# stats_thread.start()

def run(stats: dict, stats_lock: threading.Lock, interval: float = 1.0):
    """
    Infinite loop — print a refreshed metrics table every `interval` seconds.
    Designed to be called as a daemon thread from within primary.py.

    Args:
        stats:      the shared dict from primary.py
        stats_lock: the Lock protecting that dict
        interval:   refresh rate in seconds (default 1.0)
    """
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

    prev_received = 0
    prev_written  = 0
    prev_time     = time.monotonic()

    while True:
        time.sleep(interval)

        now = time.monotonic()
        elapsed = now - prev_time

        with stats_lock:
            snap = dict(stats)   # snapshot under lock, print outside

        # Derived metrics
        rx_rate  = (snap["packets_received"] - prev_received) / elapsed
        wr_rate  = (snap["packets_written"]  - prev_written)  / elapsed
        lag      = snap["last_seq"] - snap["replica_last_seq"]
        uptime   = int(now - snap["started_at"])
        wal_mb   = snap["wal_bytes"] / (1024 * 1024)
        q_pct    = (snap["queue_depth"] / 10_000) * 100

        prev_received = snap["packets_received"]
        prev_written  = snap["packets_written"]
        prev_time     = now

        # Clear line and print table
        # \033[2J = clear screen, \033[H = move cursor to top
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.write(_render_table(snap, rx_rate, wr_rate, lag, uptime, wal_mb, q_pct))
        sys.stdout.flush()


def _bar(value: float, total: float, width: int = 20) -> str:
    """ASCII progress bar."""
    filled = int((value / max(total, 1)) * width)
    return "█" * filled + "░" * (width - filled)


def _render_table(snap, rx_rate, wr_rate, lag, uptime, wal_mb, q_pct) -> str:
    w = 52
    sep = "─" * w

    lag_warning = " ⚠ LAGGING" if lag > 500 else ""
    drop_warning = f" ⚠  {snap['drop_count']} DROPPED" if snap["drop_count"] > 0 else ""

    lines = [
        f"┌{sep}┐",
        f"│{'  F1 TELEMETRY ENGINE — LIVE METRICS':^{w}}│",
        f"│{'  uptime ' + _fmt_uptime(uptime):^{w}}│",
        f"├{sep}┤",
        f"│  {'Ingestion':30s}{'':>{w-32}}│",
        f"│    Packets received  : {snap['packets_received']:>12,}              │",
        f"│    Rate (rx)         : {rx_rate:>10.0f} pkt/s              │",
        f"│    Dropped           : {snap['drop_count']:>12,}{drop_warning:<12}│",
        f"├{sep}┤",
        f"│  {'Storage (WAL)':30s}{'':>{w-32}}│",
        f"│    Packets written   : {snap['packets_written']:>12,}              │",
        f"│    Rate (write)      : {wr_rate:>10.0f} pkt/s              │",
        f"│    WAL size          : {wal_mb:>10.2f} MB                │",
        f"│    Last seq#         : {snap['last_seq']:>12,}              │",
        f"├{sep}┤",
        f"│  {'Queue':30s}{'':>{w-32}}│",
        f"│    Depth             : {snap['queue_depth']:>12,} / 10,000       │",
        f"│    [{_bar(snap['queue_depth'], 10_000)}] {q_pct:4.1f}%      │",
        f"├{sep}┤",
        f"│  {'Replication':30s}{'':>{w-32}}│",
        f"│    Replica seq#      : {snap['replica_last_seq']:>12,}              │",
        f"│    Lag               : {lag:>12,} packets{lag_warning:<12}│",
        f"└{sep}┘",
        "",
        "  Press Ctrl+C in primary.py terminal to simulate a crash.",
        "  Then run:  python validate.py",
        "",
    ]
    return "\n".join(lines) + "\n"


def _fmt_uptime(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


# ── Option B: Standalone socket mode ──────────────────────────────────────────
# If you want stats.py to run as a completely separate process, primary.py
# should expose a tiny UDP stats endpoint:
#
#   import json
#   stats_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#   while True:
#       time.sleep(1)
#       with stats_lock:
#           payload = json.dumps(stats).encode()
#       stats_sock.sendto(payload, ("127.0.0.1", 9002))
#
# Then stats.py binds to port 9002 and recvfrom() the JSON each second.
# This is the production pattern: the monitoring process is fully decoupled
# from the ingestion process and can be restarted independently.


# ── Standalone entry point (for testing the renderer) ─────────────────────────
if __name__ == "__main__":
    import threading

    # Fake stats for visual testing — run `python stats.py` to see the dashboard
    mock_stats = {
        "packets_received": 0,
        "packets_written":  0,
        "queue_depth":      0,
        "wal_bytes":        0,
        "last_seq":         -1,
        "replica_last_seq": -1,
        "drop_count":       0,
        "started_at":       time.monotonic(),
    }
    mock_lock = threading.Lock()

    def _fake_traffic():
        """Simulate increasing packet counts for dashboard demo."""
        seq = 0
        while True:
            time.sleep(0.01)
            with mock_lock:
                seq += 50
                mock_stats["packets_received"] += 50
                mock_stats["packets_written"]  += 48
                mock_stats["queue_depth"]       = min(seq % 300, 10_000)
                mock_stats["wal_bytes"]        += 48 * 32
                mock_stats["last_seq"]          = seq
                mock_stats["replica_last_seq"]  = seq - 12

    t = threading.Thread(target=_fake_traffic, daemon=True)
    t.start()

    try:
        run(mock_stats, mock_lock)
    except KeyboardInterrupt:
        print("\n[stats] exiting.")
