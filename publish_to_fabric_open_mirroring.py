#!/usr/bin/env python3
"""Upload generated open mirroring files to Microsoft Fabric OneLake."""

from __future__ import annotations

import argparse
import json
import mimetypes
from pathlib import Path
from typing import Iterable

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings as BlobContentSettings
from azure.storage.filedatalake import ContentSettings as DfsContentSettings
from azure.storage.filedatalake import DataLakeServiceClient

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
        help="Fabric open mirroring landing zone URL.",
    )
    parser.add_argument(
        "--api",
        choices=("blob", "dfs"),
        default="blob",
        help="OneLake API to use for upload. 'blob' is the default and aligns with Microsoft guidance for streaming uploads.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the files and remote paths without uploading.",
    )
    return parser.parse_args()


def _ordered_files(source_dir: Path) -> list[Path]:
    table_metadata = sorted(
        path
        for path in source_dir.rglob("_metadata.json")
        if path.is_file()
    )
    database_metadata = [source_dir / "_partnerEvents.json"] if (source_dir / "_partnerEvents.json").is_file() else []
    data_files = sorted(
        path
        for path in source_dir.rglob("*")
        if path.is_file() and path.name not in {"_metadata.json", "_partnerEvents.json"}
    )
    return database_metadata + table_metadata + data_files


def _validate_metadata_files(source_dir: Path) -> None:
    metadata_files = list(source_dir.rglob("_metadata.json"))
    if not metadata_files:
        raise ValueError("No _metadata.json files found. Fabric mirroring requires keyColumns per table.")
    for metadata_file in metadata_files:
        data = json.loads(metadata_file.read_text(encoding="utf-8"))
        key_columns = data.get("keyColumns")
        if not isinstance(key_columns, list) or not key_columns or not all(isinstance(col, str) and col for col in key_columns):
            raise ValueError(f"Invalid keyColumns metadata in {metadata_file}")


def _content_type(path: Path) -> str:
    content_type, _ = mimetypes.guess_type(str(path))
    return content_type or "application/octet-stream"


def _normalize_landing_zone_url(landing_zone_url: str, api: str) -> str:
    expected_host = f"onelake.{api}.fabric.microsoft.com"
    if expected_host in landing_zone_url:
        return landing_zone_url
    return landing_zone_url.replace("onelake.dfs.fabric.microsoft.com", expected_host).replace(
        "onelake.blob.fabric.microsoft.com",
        expected_host,
    )


def upload_directory(source_dir: Path, landing_zone_url: str, api: str, dry_run: bool) -> None:
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    _validate_metadata_files(source_dir)
    account_host, workspace_id, item_id, relative_root = parse_onelake_landing_zone(
        _normalize_landing_zone_url(landing_zone_url, api)
    )
    credential = DefaultAzureCredential()
    files = _ordered_files(source_dir)
    if not files:
        raise ValueError(f"No files found under {source_dir}")

    if api == "blob":
        service_client = BlobServiceClient(
            account_url=f"https://{account_host}",
            credential=credential,
        )
        container_client = service_client.get_container_client(workspace_id)
        for file_path in files:
            relative_path = file_path.relative_to(source_dir).as_posix()
            remote_path = f"{item_id}/{relative_root}/{relative_path}"
            print(f"{'DRY RUN ' if dry_run else ''}upload {file_path} -> {remote_path}")
            if dry_run:
                continue
            blob_client = container_client.get_blob_client(remote_path)
            with file_path.open("rb") as handle:
                blob_client.upload_blob(
                    handle,
                    overwrite=True,
                    content_settings=BlobContentSettings(content_type=_content_type(file_path)),
                )
        return

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_host}",
        credential=credential,
    )
    file_system_client = service_client.get_file_system_client(workspace_id)
    for file_path in files:
        relative_path = file_path.relative_to(source_dir).as_posix()
        remote_path = f"{item_id}/{relative_root}/{relative_path}"
        print(f"{'DRY RUN ' if dry_run else ''}upload {file_path} -> {remote_path}")
        if dry_run:
            continue
        file_client = file_system_client.get_file_client(remote_path)
        with file_path.open("rb") as handle:
            file_client.upload_data(
                handle,
                overwrite=True,
                content_settings=DfsContentSettings(content_type=_content_type(file_path)),
            )


def main() -> None:
    args = parse_args()
    upload_directory(args.source_dir, args.landing_zone_url, args.api, args.dry_run)
    if args.dry_run:
        print("Dry run complete.")
    else:
        print("Upload complete.")


if __name__ == "__main__":
    main()
