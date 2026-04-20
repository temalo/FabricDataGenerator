#!/usr/bin/env python3
"""Generate an initial GPS-inspired dataset and open mirroring landing-zone files."""

from __future__ import annotations

import argparse
from pathlib import Path

from gps_fabric.open_mirroring import (
    build_initial_dataset,
    build_initial_open_mirroring_layout,
    ensure_clean_dir,
    validate_dataset_primary_keys,
    write_snapshot,
)


DEFAULT_OUTPUT_ROOT = Path("output")
DEFAULT_INITIAL_DIR = DEFAULT_OUTPUT_ROOT / "open_mirroring_initial"
DEFAULT_SNAPSHOT_DIR = DEFAULT_OUTPUT_ROOT / "snapshots"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate initial relational fake data for carbon and IAQ Fabric open mirroring."
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Scale multiplier for dataset volume. Example: --scale 0.5",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_INITIAL_DIR,
        help=f"Directory for the open mirroring initial load files. Default: {DEFAULT_INITIAL_DIR}",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=DEFAULT_SNAPSHOT_DIR,
        help=f"Directory for the current-state snapshot used by later incremental runs. Default: {DEFAULT_SNAPSHOT_DIR}",
    )
    parser.add_argument(
        "--clean-output-root",
        action="store_true",
        help="Remove the entire output root before generating files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = args.output_dir.parent if args.output_dir.parent != Path(".") else DEFAULT_OUTPUT_ROOT
    if args.clean_output_root:
        ensure_clean_dir(output_root)

    dataset = build_initial_dataset(scale=args.scale)
    validate_dataset_primary_keys(dataset)
    build_initial_open_mirroring_layout(dataset, args.output_dir)
    write_snapshot(dataset, args.snapshot_dir)

    print("Initial dataset generated.")
    print(f"Initial landing-zone files: {args.output_dir.resolve()}")
    print(f"Snapshot state for future increments: {args.snapshot_dir.resolve()}")
    print(f"Tables generated: {', '.join(sorted(dataset.keys()))}")


if __name__ == "__main__":
    main()
