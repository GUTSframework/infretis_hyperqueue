"""Report on scripts/probe-signal-timing.sh's jobs: per job, how long before
its planned end each TERM came, and when it was killed.

    python3 scripts/probe-signal-timing-report.py <outdir>

Times are seconds before the planned end (StartTime + TimeLimit, from
Slurm, to the second); negative is after it. The warning was asked for 90 s
before the end. "killed" is the last heartbeat (written every 0.5 s).
"""

import sys
from datetime import datetime
from pathlib import Path


def time_limit_seconds(limit: str) -> int:
    """A TimeLimit such as "4", "00:04:00" or "1-00:00:00" in seconds."""
    days, _, rest = limit.rpartition("-")
    parts = [int(p) for p in rest.split(":")]
    h, m, s = ([0] * (3 - len(parts)) + parts) if len(parts) > 1 else (0, parts[0], 0)
    return int(days or 0) * 86400 + h * 3600 + m * 60 + s


def main(outdir: Path) -> None:
    print(f"{'job':>10} {'limit':>9} {'start':>8} {'TERMs before the end (s)':<28} "
          f"{'killed (s)':>10}")
    for log in sorted(outdir.glob("*.log")):
        fields: dict[str, str] = {}
        terms: list[float] = []
        for line in log.read_text().splitlines():
            if line.startswith("TERM "):
                terms.append(float(line.split()[1]))
            elif "=" in line:
                key, _, value = line.partition("=")
                fields[key] = value
        start = datetime.fromisoformat(fields["StartTime"]).timestamp()
        end = start + time_limit_seconds(fields["TimeLimit"])
        hb = log.with_suffix(".hb")
        killed = f"{end - float(hb.read_text()):10.1f}" if hb.exists() else "         ?"
        before = ", ".join(f"{end - t:.1f}" for t in terms) or "none"
        print(f"{log.stem:>10} {fields['TimeLimit']:>9} {fields['StartTime'][11:]:>8} "
              f"{before:<28} {killed}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
