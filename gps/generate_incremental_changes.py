#!/usr/bin/env python3
"""Create new rows plus updates/deletes for the GPS-inspired Fabric dataset."""

from __future__ import annotations

import argparse
from pathlib import Path

from gps_fabric.open_mirroring import (
    mutate_dataset,
    read_snapshot,
    validate_change_rows,
    validate_dataset_primary_keys,
    write_incremental_changes,
    write_snapshot,
)


_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = _REPO_ROOT / "output" / "gps"
DEFAULT_INCREMENTAL_DIR = DEFAULT_OUTPUT_ROOT / "open_mirroring_incremental"
DEFAULT_SNAPSHOT_DIR = DEFAULT_OUTPUT_ROOT / "snapshots"
DEFAULT_PRIOR_SEQUENCE_DIR = DEFAULT_OUTPUT_ROOT / "open_mirroring_initial"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate incremental insert/update/delete change files for Fabric open mirroring."
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Scale multiplier for the amount of change activity. Example: --scale 0.5",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=DEFAULT_SNAPSHOT_DIR,
        help=f"Directory holding the current-state snapshot. Default: {DEFAULT_SNAPSHOT_DIR}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_INCREMENTAL_DIR,
        help=f"Directory for incremental landing-zone parquet files. Default: {DEFAULT_INCREMENTAL_DIR}",
    )
    parser.add_argument(
        "--prior-sequence-dir",
        type=Path,
        default=DEFAULT_PRIOR_SEQUENCE_DIR,
        help=(
            "Directory to inspect for previously generated parquet sequence numbers so incremental "
            "files continue after the initial load. "
            f"Default: {DEFAULT_PRIOR_SEQUENCE_DIR}"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset = read_snapshot(args.snapshot_dir)
    validate_dataset_primary_keys(dataset)
    changes = mutate_dataset(dataset, scale=args.scale)
    validate_change_rows(changes)
    written = write_incremental_changes(
        changes,
        args.output_dir,
        prior_dirs=[args.prior_sequence_dir],
    )
    write_snapshot(dataset, args.snapshot_dir)

    print("Incremental change set generated.")
    print(f"Incremental output: {args.output_dir.resolve()}")
    print(f"Updated snapshot: {args.snapshot_dir.resolve()}")
    print(f"Files written: {len(written)}")
    for path in written:
        print(f" - {path.resolve()}")


if __name__ == "__main__":
    main()
