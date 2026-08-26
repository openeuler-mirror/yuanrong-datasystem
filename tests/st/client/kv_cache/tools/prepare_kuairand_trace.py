#!/usr/bin/env python3
"""Create a bounded, chronological trace from official KuaiRand log CSV files."""

import argparse
import csv
import os
import tempfile
from pathlib import Path


ENGAGEMENT_FIELDS = (
    "is_click",
    "is_like",
    "is_follow",
    "is_comment",
    "is_forward",
    "long_view",
)
REQUIRED_FIELDS = ("time_ms", "video_id", *ENGAGEMENT_FIELDS)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize KuaiRand logs for KV eviction/rebalance telemetry ST replay."
    )
    parser.add_argument("--input", action="append", required=True, type=Path,
                        help="KuaiRand log_*.csv; repeat to combine multiple periods/policies")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-events", type=int, default=100_000,
                        help="deterministic uniform sample size before chronological sorting")
    parser.add_argument("--source-label", default="KuaiRand")
    parser.add_argument("--archive-md5", default="unknown",
                        help="checksum of the official archive used to extract the inputs")
    return parser.parse_args()


def count_rows(paths):
    total = 0
    for path in paths:
        with path.open("r", newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            missing = [field for field in REQUIRED_FIELDS if field not in (reader.fieldnames or ())]
            if missing:
                raise ValueError(f"{path}: missing fields: {','.join(missing)}")
            total += sum(1 for _ in reader)
    return total


def sample_events(paths, total_rows, max_events):
    target = min(total_rows, max_events)
    if target <= 0:
        raise ValueError("input contains no events or --max-events is not positive")
    sample_positions = ((index * total_rows) // target for index in range(target))
    next_position = next(sample_positions, None)
    events = []
    position = 0
    sequence = 0
    for path in paths:
        with path.open("r", newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                if position == next_position:
                    timestamp = int(row["time_ms"])
                    video_id = int(row["video_id"])
                    engaged = int(any(int(row[field]) != 0 for field in ENGAGEMENT_FIELDS))
                    events.append((timestamp, sequence, video_id, engaged))
                    sequence += 1
                    next_position = next(sample_positions, None)
                position += 1
    if len(events) != target:
        raise RuntimeError(f"selected {len(events)} events, expected {target}")
    events.sort(key=lambda event: (event[0], event[1]))
    return events


def write_trace(path, events, source_label, archive_md5, inputs, total_rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write("# kuairand-trace-v1\n")
            stream.write(f"# source={source_label}\n")
            stream.write(f"# archive_md5={archive_md5}\n")
            stream.write(f"# input_rows={total_rows}\n")
            stream.write("# inputs=" + ",".join(input_path.name for input_path in inputs) + "\n")
            stream.write("time_ms,video_id,engaged\n")
            for timestamp, _, video_id, engaged in events:
                stream.write(f"{timestamp},{video_id},{engaged}\n")
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def main():
    args = parse_args()
    if args.max_events <= 0:
        raise ValueError("--max-events must be positive")
    total_rows = count_rows(args.input)
    events = sample_events(args.input, total_rows, args.max_events)
    write_trace(args.output, events, args.source_label, args.archive_md5, args.input, total_rows)
    engaged = sum(event[3] for event in events)
    print(f"wrote {len(events)} events ({engaged} engaged) from {total_rows} rows to {args.output}")


if __name__ == "__main__":
    main()
