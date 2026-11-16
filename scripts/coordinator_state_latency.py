#!/usr/bin/env python3
"""Measure coordinator state-write latency from shard coordinator DEBUG logs.

Usage:
    python3 scripts/coordinator_state_latency.py shard-coordinator-node-*.log
    python3 scripts/coordinator_state_latency.py --csv latencies.csv shard-coordinator-node-*.log

Pairing is per file and entity type. A repeated "Storing" line before a success
is treated as a retry of the same write, timed from the first store attempt.
"""

import argparse
import csv
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


LOG_LINE = re.compile(
    r"^(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) "
    r"\[[^]]+\] DEBUG akka\.cluster\.sharding\."
    r"(?:DDataShardCoordinator|PersistentShardCoordinator|ShardCoordinator) - "
    r"(?P<type>[^:]+): (?P<message>.*)$"
)
STORE = "Storing new coordinator state ["
SUCCESS = "The coordinator state was successfully updated with "


@dataclass
class Summary:
    milliseconds: list[float] = field(default_factory=list)
    retries: int = 0
    unmatched_successes: int = 0
    unmatched_stores: int = 0

    def add(self, other: "Summary") -> None:
        self.milliseconds.extend(other.milliseconds)
        self.retries += other.retries
        self.unmatched_successes += other.unmatched_successes
        self.unmatched_stores += other.unmatched_stores


def percentile(sorted_values: list[float], percent: int) -> float:
    return sorted_values[max(0, math.ceil(len(sorted_values) * percent / 100) - 1)]


def print_summary(label: str, summary: Summary) -> None:
    values = sorted(summary.milliseconds)
    print(
        f"{label}: samples={len(values)} retries={summary.retries} "
        f"unmatched_stores={summary.unmatched_stores} "
        f"unmatched_successes={summary.unmatched_successes}"
    )
    if values:
        print(
            f"  latency_ms: min={values[0]:.3f} mean={sum(values) / len(values):.3f} "
            f"p50={percentile(values, 50):.3f} "
            f"p90={percentile(values, 90):.3f} "
            f"p99={percentile(values, 99):.3f} max={values[-1]:.3f}"
        )


def process_file(path: Path, writer: Any | None) -> Summary:
    summary = Summary()
    pending: dict[str, tuple[datetime, str]] = {}

    with path.open(encoding="utf-8", errors="replace") as lines:
        for line in lines:
            if STORE not in line and SUCCESS not in line:
                continue
            match = LOG_LINE.match(line)
            if match is None:
                continue
            message = match.group("message")
            entity_type = match.group("type")
            timestamp_text = match.group("time")
            timestamp = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S.%f")

            if message.startswith(STORE):
                if entity_type in pending:
                    summary.retries += 1
                else:
                    pending[entity_type] = (timestamp, timestamp_text)
            elif message.startswith(SUCCESS):
                started = pending.pop(entity_type, None)
                if started is None:
                    summary.unmatched_successes += 1
                    continue
                elapsed_ms = (timestamp - started[0]).total_seconds() * 1000
                if elapsed_ms < 0:
                    summary.unmatched_successes += 1
                    summary.unmatched_stores += 1
                    continue
                summary.milliseconds.append(elapsed_ms)
                if writer is not None:
                    writer.writerow(
                        [path.name, entity_type, started[1], timestamp_text, f"{elapsed_ms:.3f}", message[len(SUCCESS):]]
                    )

    summary.unmatched_stores += len(pending)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path, help="coordinator log files")
    parser.add_argument("--csv", type=Path, help="write each matched latency to this CSV file")
    args = parser.parse_args()

    total = Summary()
    output = args.csv.open("w", newline="", encoding="utf-8") if args.csv else None
    try:
        writer = csv.writer(output) if output else None
        if writer is not None:
            writer.writerow(["file", "entity_type", "store_time", "success_time", "latency_ms", "update_event"])
        for path in args.logs:
            summary = process_file(path, writer)
            print_summary(str(path), summary)
            total.add(summary)
    finally:
        if output is not None:
            output.close()

    if len(args.logs) > 1:
        print_summary("ALL", total)


if __name__ == "__main__":
    main()
