"""Summarise per-move times from infretis `sim.log` files.

infretis logs one line per finished move:

    [INFO]: worker: 1 total time:1.23s and subcycles: 179

"total time" is measured in the parent, from preparing the move
(`prep_md_items`) to the start of `treat_output`, so it includes dispatch and
the return trip as well as the MD itself.

Usage:

    .venv/bin/python bench/move_times.py runs/2d-w4 [runs/... ...] [--json]
"""

import argparse
import json
import re
import statistics
from pathlib import Path
from typing import Any

LINE = re.compile(r"worker: (\d+) total time:([\d.]+)s and subcycles: (\d+)")


def quantile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    idx = q * (len(sorted_values) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (idx - lo)


def summarise(rundir: Path) -> dict[str, Any]:
    times, subcycles = [], []
    for line in (rundir / "sim.log").read_text().splitlines():
        m = LINE.search(line)
        if m:
            times.append(float(m.group(2)))
            subcycles.append(int(m.group(3)))
    times_sorted = sorted(times)
    runs = json.loads((rundir / "run.json").read_text()) if (rundir / "run.json").exists() else []
    wall = sum(r["wall_s"] for r in runs) or None
    return {
        "run": str(rundir),
        "moves": len(times),
        "median_s": quantile(times_sorted, 0.5),
        "p10_s": quantile(times_sorted, 0.1),
        "p90_s": quantile(times_sorted, 0.9),
        "mean_s": statistics.fmean(times) if times else None,
        "max_s": times_sorted[-1] if times else None,
        "subcycles_median": statistics.median(subcycles) if subcycles else None,
        "wall_s": wall,
        "moves_per_hour": len(times) / wall * 3600 if wall else None,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("rundirs", nargs="+", type=Path)
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    rows = [summarise(d) for d in args.rundirs]
    if args.json:
        print(json.dumps(rows, indent=2))
        return
    cols = ["run", "moves", "median_s", "p10_s", "p90_s", "mean_s", "max_s",
            "subcycles_median", "wall_s", "moves_per_hour"]
    print("\t".join(cols))
    for r in rows:
        print("\t".join(f"{r[c]:.3g}" if isinstance(r[c], float) else str(r[c]) for c in cols))


if __name__ == "__main__":
    main()
