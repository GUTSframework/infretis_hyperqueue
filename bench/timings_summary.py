"""Summarise gutsrun per-task timings (`gutsrun-timings.jsonl`).

Phases per task (wall clock, seconds):

    dispatch   submitted -> start    backend queue + dispatch + Python start-up
    imports    start -> loaded       unpickling the call (imports infretis etc.)
    init       loaded -> inited      initializer (engines, order parameters)
    move       inited -> done        the call itself (the MC move)
    return     done -> collected     result file write + executor poll
    overhead   everything but move

Usage:

    .venv/bin/python bench/timings_summary.py runs/.../gutsrun-timings.jsonl [...] [--json]
"""

import argparse
import json
import statistics
from pathlib import Path

PHASES = {
    "dispatch": ("submitted", "start"),
    "imports": ("start", "loaded"),
    "init": ("loaded", "inited"),
    "move": ("inited", "done"),
    "return": ("done", "collected"),
}


def quantile(values, q):
    s = sorted(values)
    idx = q * (len(s) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


def summarise(path: Path) -> dict:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    rows = [r for r in rows if all(r.get(k) is not None for k in
                                   ("submitted", "start", "loaded", "inited", "done", "collected"))]
    out = {"file": str(path), "tasks": len(rows)}
    per = {name: [r[b] - r[a] for r in rows] for name, (a, b) in PHASES.items()}
    per["overhead"] = [sum(per[n][i] for n in PHASES if n != "move") for i in range(len(rows))]
    per["total"] = [r["collected"] - r["submitted"] for r in rows]
    for name, vals in per.items():
        out[name] = {"median": round(statistics.median(vals), 4),
                     "p10": round(quantile(vals, 0.1), 4),
                     "p90": round(quantile(vals, 0.9), 4)}
    out["overhead_share_of_median_total"] = round(
        out["overhead"]["median"] / out["total"]["median"], 3)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("files", nargs="+", type=Path)
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    results = [summarise(f) for f in args.files]
    if args.json:
        print(json.dumps(results, indent=2))
        return
    names = list(PHASES) + ["overhead", "total"]
    print("file".ljust(48), "n".rjust(5), *(n.rjust(9) for n in names), "ovh%".rjust(6))
    for r in results:
        print(r["file"][-48:].ljust(48), str(r["tasks"]).rjust(5),
              *(f"{r[n]['median']:9.3f}" for n in names),
              f"{100 * r['overhead_share_of_median_total']:6.1f}")


if __name__ == "__main__":
    main()
