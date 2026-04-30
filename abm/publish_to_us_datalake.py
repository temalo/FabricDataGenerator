#!/usr/bin/env python3
"""Generate a large ABM US dataset and publish as Delta Parquet tables to Azure Data Lake.

Strategy:
  1. Generate data in memory
  2. Write each table as a Delta table to a local staging directory
     (output/abm/delta/) using the deltalake library
  3. Upload the staged files to Azure Blob Storage using the Python SDK
     (which uses the same auth path as the GPS publisher)

Target:  https://abmusdatalake.blob.core.windows.net/usoperations/abm/us/<table>/
Auth:    Entra (DefaultAzureCredential — requires az login or managed identity)
Format:  Delta Lake (Parquet data files + _delta_log/ transaction log)

Usage:
  python publish_to_us_datalake.py
  python publish_to_us_datalake.py --days 730 --scale 5.0
  python publish_to_us_datalake.py --table silver_employee_reference
  python publish_to_us_datalake.py --dry-run
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import timedelta
from pathlib import Path

import pyarrow as pa

# ── Ensure generate_dataset is importable regardless of working directory ─────
_SOLUTION_DIR = Path(__file__).resolve().parent
_REPO_ROOT    = _SOLUTION_DIR.parent
sys.path.insert(0, str(_SOLUTION_DIR))
import generate_dataset as gen  # noqa: E402

_ACCOUNT_URL  = "https://abmusdatalake.blob.core.windows.net"
_CONTAINER    = "usoperations"
_TABLE_PREFIX = "abm/us"
_STAGING_DIR  = _REPO_ROOT / "output" / "abm" / "delta"

DEFAULT_DAYS  = 365
DEFAULT_SCALE = 3.0
DEFAULT_SEED  = gen.DEFAULT_SEED


# ─────────────────────────────────────────────────────────────────────────────
# PyArrow helpers
# ─────────────────────────────────────────────────────────────────────────────

# ISO date string columns to cast to pa.date32()
_ISO_DATE_COLUMNS = {"date", "week_start_date", "open_date", "close_date",
                     "in_service_date", "hire_date", "extract_date"}


def _nullify(rows: list[dict]) -> list[dict]:
    """Convert empty-string sentinels to None for nullable fields."""
    return [{k: None if v == "" else v for k, v in row.items()} for row in rows]


def _to_arrow(rows: list[dict]) -> pa.Table:
    """Convert list-of-dicts to a typed PyArrow Table."""
    tbl = pa.Table.from_pylist(_nullify(rows))
    for i, field in enumerate(tbl.schema):
        if pa.types.is_null(field.type):
            # Delta Lake rejects pa.null() — cast all-None columns to string
            tbl = tbl.set_column(i, field.name,
                                 tbl.column(field.name).cast(pa.string()))
        elif field.name in _ISO_DATE_COLUMNS and pa.types.is_string(field.type):
            try:
                tbl = tbl.set_column(i, field.name,
                                     tbl.column(field.name).cast(pa.date32(), safe=False))
            except Exception:
                pass
    return tbl


# ─────────────────────────────────────────────────────────────────────────────
# Local Delta write
# ─────────────────────────────────────────────────────────────────────────────

def _write_delta_local(table_name: str, arrow_table: pa.Table,
                       staging_dir: Path) -> Path:
    """Write a Delta table to the local staging directory."""
    from deltalake import write_deltalake

    table_path = staging_dir / table_name
    if table_path.exists():
        shutil.rmtree(table_path)
    table_path.mkdir(parents=True, exist_ok=True)

    write_deltalake(
        str(table_path),
        arrow_table,
        mode="overwrite",
        schema_mode="overwrite",
    )
    return table_path


# ─────────────────────────────────────────────────────────────────────────────
# Azure Blob Storage upload
# ─────────────────────────────────────────────────────────────────────────────

def _get_blob_service():
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient
    return BlobServiceClient(_ACCOUNT_URL, credential=DefaultAzureCredential())


def _upload_table(local_table_path: Path, table_name: str,
                  blob_service, prefix: str) -> tuple[int, int]:
    """Upload all files under local_table_path to the container. Returns (file_count, total_bytes)."""
    from azure.storage.blob import ContentSettings

    container = blob_service.get_container_client(_CONTAINER)
    files = sorted(f for f in local_table_path.rglob("*") if f.is_file())

    # Upload _delta_log files first (metadata before data, consistent with open mirroring pattern)
    log_files  = [f for f in files if "_delta_log" in f.parts]
    data_files = [f for f in files if "_delta_log" not in f.parts]
    ordered    = log_files + data_files

    total_bytes = 0
    for file_path in ordered:
        relative  = file_path.relative_to(local_table_path)
        blob_name = f"{prefix}/{table_name}/{relative.as_posix()}"
        content_type = "application/json" if file_path.suffix == ".json" else "application/octet-stream"
        blob_client = container.get_blob_client(blob_name)
        with file_path.open("rb") as fh:
            blob_client.upload_blob(
                fh,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
            )
        total_bytes += file_path.stat().st_size

    return len(ordered), total_bytes


# ─────────────────────────────────────────────────────────────────────────────
# Data generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_us_tables(days: int, scale: float, seed: int) -> dict[str, list[dict]]:
    import random
    from faker import Faker

    gen.rng    = random.Random(seed)
    Faker.seed(seed)
    gen.fake_uk = Faker("en_GB")
    gen.fake_us = Faker("en_US")

    end_date   = gen._END_DATE
    start_date = end_date - timedelta(days=days - 1)
    dates      = gen.date_range(start_date, end_date)

    print(f"  Date range : {start_date} -> {end_date} ({len(dates)} days)")
    print(f"  Scale      : {scale}")

    dim_date         = gen.gen_dim_date(start_date, end_date)
    dim_customer     = gen.gen_dim_customer(n=len(gen.US_CUSTOMERS))
    dim_site         = gen.gen_dim_site(dim_customer, n=max(12, int(35 * scale)))
    dim_service_line = gen.gen_dim_service_line()
    dim_service_cat  = gen.gen_dim_service_catalog(dim_service_line)
    dim_contract     = gen.gen_dim_contract(dim_customer, dim_service_line, dates,
                                             n=max(8, int(15 * scale)))
    dim_employee_us  = gen.gen_dim_employee_us(n=max(50, int(300 * scale)),
                                               service_lines=dim_service_line)
    dim_asset        = gen.gen_dim_asset(dim_site, n=max(20, int(60 * scale)))

    dim_airport_uk   = gen.gen_dim_airport_uk(dim_site)
    dim_employee_uk  = gen.gen_dim_employee_uk(dim_airport_uk, n=max(30, int(200 * scale)))

    silver_emp_ref   = gen.gen_silver_employee_reference(dim_employee_us, dim_employee_uk)
    silver_site_map  = gen.gen_silver_customer_site_map(dim_customer, dim_site)
    silver_svc_cat   = gen.gen_silver_service_catalog(dim_service_cat, dim_service_line)
    silver_contracts = gen.gen_silver_contract_terms(dim_contract, dim_customer)
    silver_kpi       = gen.gen_silver_kpi_benchmarks(dim_service_line)

    fact_wo      = gen.gen_fact_work_order(dim_site, dim_contract, dim_service_cat, dates, scale)
    fact_labor   = gen.gen_fact_labor_time(dim_employee_us, dim_site, dim_service_line, dates, scale)
    fact_billing = gen.gen_fact_billing_invoice_line(dim_contract, dim_site, dim_service_cat, dates, scale)
    fact_safety  = gen.gen_fact_safety_incident(dim_site, dim_service_line, dates, scale)

    return {
        "dim_date":                  dim_date,
        "dim_customer":              dim_customer,
        "dim_site":                  dim_site,
        "dim_service_line":          dim_service_line,
        "dim_service_catalog":       dim_service_cat,
        "dim_contract":              dim_contract,
        "dim_employee_us":           dim_employee_us,
        "dim_asset":                 dim_asset,
        "silver_employee_reference": silver_emp_ref,
        "silver_customer_site_map":  silver_site_map,
        "silver_service_catalog":    silver_svc_cat,
        "silver_contract_terms":     silver_contracts,
        "silver_kpi_benchmarks":     silver_kpi,
        "fact_work_order":           fact_wo,
        "fact_labor_time":           fact_labor,
        "fact_billing_invoice_line": fact_billing,
        "fact_safety_incident":      fact_safety,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish ABM US dataset as Delta tables to Azure Data Lake."
    )
    parser.add_argument("--days",   type=int,   default=DEFAULT_DAYS,
                        help=f"Days of history. Default: {DEFAULT_DAYS}")
    parser.add_argument("--scale",  type=float, default=DEFAULT_SCALE,
                        help=f"Volume multiplier. Default: {DEFAULT_SCALE}")
    parser.add_argument("--seed",   type=int,   default=DEFAULT_SEED,
                        help="Random seed.")
    parser.add_argument("--table",  type=str,   default=None,
                        help="Publish a single named table (default: all).")
    parser.add_argument("--prefix", default=_TABLE_PREFIX,
                        help=f"Blob path prefix. Default: {_TABLE_PREFIX}")
    parser.add_argument("--keep-staging", action="store_true",
                        help="Keep local staged Delta files after upload.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate + stage locally but skip Azure upload.")
    return parser.parse_args()


def main() -> None:
    args  = parse_args()
    scale = max(0.1, args.scale)

    print(f"\nABM US Data Lake Publisher")
    print(f"  Target  : {_ACCOUNT_URL}/{_CONTAINER}/{args.prefix}/<table>/")
    print(f"  Staging : {_STAGING_DIR}")
    print()

    # ── Generate ──────────────────────────────────────────────────────────────
    print("Generating US dataset...")
    t0 = time.perf_counter()
    tables = generate_us_tables(days=args.days, scale=scale, seed=args.seed)
    total_rows = sum(len(v) for v in tables.values())
    print(f"  Generated {total_rows:,} rows across {len(tables)} tables in {time.perf_counter()-t0:.1f}s\n")

    if args.table:
        if args.table not in tables:
            print(f"ERROR: Unknown table '{args.table}'. Available: {', '.join(tables)}")
            sys.exit(1)
        tables = {args.table: tables[args.table]}

    # ── Azure client (skip if dry-run) ────────────────────────────────────────
    blob_service = None
    if not args.dry_run:
        print("Acquiring Azure credential (DefaultAzureCredential)...")
        blob_service = _get_blob_service()
        print("  Connected.\n")

    # ── Per-table: stage locally then upload ──────────────────────────────────
    _STAGING_DIR.mkdir(parents=True, exist_ok=True)

    print(f"{'Table':<45} {'Rows':>8}  {'MB':>6}  {'Files':>6}  {'Time':>6}  Status")
    print("-" * 85)

    t_total = time.perf_counter()
    total_bytes = 0

    for name, rows in tables.items():
        t1 = time.perf_counter()
        arrow_table = _to_arrow(rows)

        # Stage locally as Delta
        local_path = _write_delta_local(name, arrow_table, _STAGING_DIR)
        local_files = list(local_path.rglob("*"))
        local_files = [f for f in local_files if f.is_file()]
        mb = sum(f.stat().st_size for f in local_files) / 1_048_576

        if args.dry_run:
            print(f"  [STAGED]  {name:<45} {len(rows):>8,}  {mb:6.1f}  {len(local_files):>6}  --  (not uploaded)")
            continue

        # Upload to Azure
        n_files, n_bytes = _upload_table(local_path, name, blob_service, args.prefix)
        total_bytes += n_bytes
        elapsed = time.perf_counter() - t1
        print(f"  {name:<45} {len(rows):>8,}  {mb:6.1f}  {n_files:>6}  {elapsed:5.1f}s  OK")

    total_elapsed = time.perf_counter() - t_total

    print("-" * 85)
    if args.dry_run:
        print(f"\nDry run complete. {total_rows:,} rows staged locally at {_STAGING_DIR}")
        print("Re-run without --dry-run to upload.")
    else:
        print(f"\n{total_rows:,} rows uploaded as Delta tables in {total_elapsed:.1f}s "
              f"({total_bytes / 1_048_576:.1f} MB total)")
        print(f"Path: {_ACCOUNT_URL}/{_CONTAINER}/{args.prefix}/")

        if not args.keep_staging:
            shutil.rmtree(_STAGING_DIR)
            print(f"Staging directory cleaned up.")


if __name__ == "__main__":
    main()
