#!/usr/bin/env python3
"""Upload generated open mirroring files to Microsoft Fabric OneLake."""

from __future__ import annotations

import argparse
import mimetypes
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import ContentSettings, DataLakeServiceClient

from gps_fabric.open_mirroring import parse_onelake_landing_zone


DEFAULT_SOURCE_DIR = Path("output/open_mirroring_initial")
DEFAULT_LANDING_ZONE = (
    "https://onelake.dfs.fabric.microsoft.com/"
    "14e9c878-6b48-4ab6-b393-a9b6af015d3a/"
    "d1e820d9-fe3a-41a7-aa70-fa9fa4d90171/"
    "Files/LandingZone"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish local open mirroring files to a Microsoft Fabric landing zone."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f"Local directory to upload recursively. Default: {DEFAULT_SOURCE_DIR}",
    )
    parser.add_argument(
        "--landing-zone-url",
        default=DEFAULT_LANDING_ZONE,
        help="Fabric landing zone URL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the files and remote paths without uploading.",
    )
    return parser.parse_args()


def upload_directory(source_dir: Path, landing_zone_url: str, dry_run: bool) -> None:
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    workspace_id, item_id, relative_root = parse_onelake_landing_zone(landing_zone_url)
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential,
    )
    file_system_client = service_client.get_file_system_client(workspace_id)

    files = sorted(path for path in source_dir.rglob("*") if path.is_file())
    if not files:
        raise ValueError(f"No files found under {source_dir}")

    for file_path in files:
        relative_path = file_path.relative_to(source_dir).as_posix()
        remote_path = f"{item_id}/{relative_root}/{relative_path}"
        print(f"{'DRY RUN ' if dry_run else ''}upload {file_path} -> {remote_path}")
        if dry_run:
            continue

        file_client = file_system_client.get_file_client(remote_path)
        content_type, _ = mimetypes.guess_type(str(file_path))
        with file_path.open("rb") as handle:
            file_client.upload_data(
                handle,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type or "application/octet-stream"),
            )


def main() -> None:
    args = parse_args()
    upload_directory(args.source_dir, args.landing_zone_url, args.dry_run)
    if args.dry_run:
        print("Dry run complete.")
    else:
        print("Upload complete.")


if __name__ == "__main__":
    main()
