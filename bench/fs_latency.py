"""Measure how long a file written on one node takes to appear on another.

Two processes on two nodes share a directory (on DAS-6: `/home`, NFS). They
play ping-pong: rank 0 writes `ping.<i>`, rank 1 waits until it sees it and
writes `pong.<i>`, rank 0 waits for that. The round trip is timed on rank 0's
clock only, so the nodes' clocks need not agree; one-way latency is about half
of it. Files are written the way `gutsrun.spool` writes them: to a dot-prefixed
name, then `os.replace`d into place.

`--mode listdir` waits like the executor's watcher (`os.listdir` every
`--poll` s); `--mode open` retries `open()` on the known name instead.
`--mode replace` reuses one `ping` and one `pong` file, replaced each round,
and rereads it until the content is that round's: this is how a task on
another node sees `spool/CURRENT` change. Both files exist, and each rank has
read the other's once, before the first round, so only replacement is timed.

Each rank prints one JSON line with its own waits (rank 0's waits are the
round trips), so a stall shows which side did not see the other's file.

Usage (one task per node, e.g. from an sbatch with --nodes=2):

    srun --nodes=2 --ntasks=2 --ntasks-per-node=1 \
        .venv/bin/python bench/fs_latency.py DIR --mode listdir --rounds 30

Rank comes from SLURM_PROCID (or --rank).
"""

import argparse
import json
import os
import socket
import statistics
import sys
import time
from pathlib import Path


def write(path: Path, text: str) -> None:
    tmp = path.with_name("." + path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def wait_for(path: Path, mode: str, poll: float, timeout: float,
             want: str = "") -> str | None:
    """Return the file's content once visible and starting with `want`, or None
    after `timeout` s."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if mode == "listdir":
            if path.name in os.listdir(path.parent):
                text = path.read_text()
                if text.startswith(want):
                    return text
        else:
            try:
                text = path.read_text()
                if text.startswith(want):
                    return text
            except FileNotFoundError:
                pass
        time.sleep(poll)
    return None


def main() -> None:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("dir", type=Path, help="shared directory; created, must be fresh per run")
    p.add_argument("--mode", choices=["listdir", "open", "replace"], default="listdir")
    p.add_argument("--rounds", type=int, default=30)
    p.add_argument("--poll", type=float, default=0.02,
                   help="seconds between checks (executor default)")
    p.add_argument("--timeout", type=float, default=60.0,
                   help="give up on one file after this many s")
    p.add_argument("--rank", type=int, default=int(os.environ.get("SLURM_PROCID", "-1")))
    args = p.parse_args()
    if args.rank not in (0, 1):
        sys.exit("need --rank 0|1 or SLURM_PROCID")

    d = args.dir
    d.mkdir(parents=True, exist_ok=True)
    host = socket.gethostname()
    ping, pong = d / "ping", d / "pong"
    if args.mode == "replace":
        write(ping if args.rank == 0 else pong, "-1 init")
    # Start-up handshake; generous timeout because srun may start ranks apart.
    if args.rank == 1:
        write(d / "ready", host)
        peer = wait_for(d / "go", "listdir", args.poll, 300)
    else:
        peer = wait_for(d / "ready", "listdir", args.poll, 300)
        write(d / "go", host)
    if peer is None:
        sys.exit(f"rank {args.rank}: no peer")
    if args.mode == "replace":  # look the other's file up once, so it is cached
        theirs = pong if args.rank == 0 else ping
        if wait_for(theirs, "listdir", args.poll, 300) is None:
            sys.exit(f"rank {args.rank}: never saw {theirs.name}")

    waits, bad, stalled = [], 0, None
    how = "open" if args.mode == "replace" else args.mode
    for i in range(args.rounds):
        token = f"{i} {time.time_ns()}"
        want = f"{i} " if args.mode == "replace" else ""
        if args.mode != "replace":
            ping, pong = d / f"ping.{i}", d / f"pong.{i}"
        mine, theirs = (ping, pong) if args.rank == 0 else (pong, ping)
        if args.rank == 0:
            write(mine, token)
        start = time.perf_counter()
        got = wait_for(theirs, how, args.poll, args.timeout, want)
        if got is None:
            stalled = i
            print(f"rank {args.rank} round {i}: not seen within {args.timeout} s",
                  file=sys.stderr, flush=True)
            break
        waits.append(time.perf_counter() - start)
        # progress, in case the job's time cap kills us before the summary
        print(f"rank {args.rank} round {i}: {waits[-1]:.4f} s", file=sys.stderr, flush=True)
        bad += not got.startswith(f"{i} ")
        if args.rank == 1:
            write(mine, token)

    q = statistics.quantiles(waits, n=10) if len(waits) >= 2 else [float("nan")] * 9
    print(json.dumps({
        "rank": args.rank, "mode": args.mode, "poll_s": args.poll, "timeout_s": args.timeout,
        "dir": str(d), "hosts": [host, peer], "rounds": args.rounds, "completed": len(waits),
        "stalled_in_round": stalled, "bad_content": bad,
        # rank 0: round trips; rank 1: from its last write (or start) to the next ping
        "wait_median_s": round(statistics.median(waits), 4) if waits else None,
        "wait_p90_s": round(q[8], 4), "wait_max_s": round(max(waits), 4) if waits else None,
        "waits_s": [round(w, 4) for w in waits],
    }), flush=True)


if __name__ == "__main__":
    main()
