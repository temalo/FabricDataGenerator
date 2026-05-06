#!/usr/bin/env python3
"""Generate ABM UK aviation dataset and publish as Delta tables to Microsoft Fabric OneLake.

Strategy:
  1. Generate UK dataset tables in memory
  2. Write each table directly to OneLake via delta-rs using an ABFSS URL —
     delta-rs handles all path computation so the _delta_log references
     are relative to the OneLake table root, not a local staging path.

Target:  abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<item_id>/Tables/<table>
Auth:    Entra ID via DefaultAzureCredential (az login or managed identity)
Format:  Delta Lake (Parquet data files + _delta_log/ transaction log)

Usage:
  python publish_to_uk_lakehouse.py
  python publish_to_uk_lakehouse.py --days 180 --scale 1.0
  python publish_to_uk_lakehouse.py --table dim_airport_uk
  python publish_to_uk_lakehouse.py --dry-run
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

_WORKSPACE_ID      = "68374151-b147-49a2-b549-8d440b57f678"
_LAKEHOUSE_ITEM_ID = "47768fc7-0a83-41d8-b51c-c400b1faf741"
_SCHEMA_NAME       = "Aviation"
_STAGING_DIR       = _REPO_ROOT / "output" / "abm" / "delta_uk"

DEFAULT_DAYS  = 90
DEFAULT_SCALE = 1.0
DEFAULT_SEED  = gen.DEFAULT_SEED


# ─────────────────────────────────────────────────────────────────────────────
# PyArrow helpers
# ─────────────────────────────────────────────────────────────────────────────

_ISO_DATE_COLUMNS = {"in_service_date", "hire_date", "extract_date"}

_ISO_TIMESTAMP_COLUMNS = {
    "planned_start_utc", "planned_end_utc",
    "actual_start_utc",  "actual_end_utc",
    "requested_time_utc", "start_time_utc", "end_time_utc",
    "extract_timestamp",
}


def _nullify(rows: list[dict]) -> list[dict]:
    return [{k: None if v == "" else v for k, v in row.items()} for row in rows]


def _to_arrow(rows: list[dict]) -> pa.Table:
    tbl = pa.Table.from_pylist(_nullify(rows))
    for i, field in enumerate(tbl.schema):
        if pa.types.is_null(field.type):
            tbl = tbl.set_column(i, field.name,
                                 tbl.column(field.name).cast(pa.string()))
        elif field.name in _ISO_DATE_COLUMNS and pa.types.is_string(field.type):
            try:
                tbl = tbl.set_column(i, field.name,
                                     tbl.column(field.name).cast(pa.date32(), safe=False))
            except Exception:
                pass
        elif field.name in _ISO_TIMESTAMP_COLUMNS and pa.types.is_string(field.type):
            try:
                tbl = tbl.set_column(
                    i, field.name,
                    tbl.column(field.name).cast(pa.timestamp("us"), safe=False),
                )
            except Exception:
                pass
    return tbl


# ─────────────────────────────────────────────────────────────────────────────
# OneLake — direct Delta write via delta-rs ABFSS URL
# ─────────────────────────────────────────────────────────────────────────────

def _onelake_url(table_name: str) -> str:
    return (
        f"abfss://{_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
        f"/{_LAKEHOUSE_ITEM_ID}/Tables/{_SCHEMA_NAME}/{table_name}"
    )


def _get_storage_options() -> dict:
    """Return bearer-token storage options for delta-rs → OneLake."""
    from azure.identity import DefaultAzureCredential
    token = DefaultAzureCredential().get_token("https://storage.azure.com/.default")
    return {"bearer_token": token.token}


def _write_to_onelake(table_name: str, arrow_table: pa.Table,
                      storage_options: dict) -> None:
    from deltalake import write_deltalake
    write_deltalake(
        _onelake_url(table_name),
        arrow_table,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=storage_options,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run — write locally so you can inspect what delta-rs produces
# ─────────────────────────────────────────────────────────────────────────────

def _write_dry_run(table_name: str, arrow_table: pa.Table) -> Path:
    from deltalake import write_deltalake
    table_path = _STAGING_DIR / table_name
    if table_path.exists():
        shutil.rmtree(table_path)
    table_path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(table_path), arrow_table, mode="overwrite", schema_mode="overwrite")
    return table_path


# ─────────────────────────────────────────────────────────────────────────────
# Data generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_uk_tables(days: int, scale: float, seed: int) -> dict[str, list[dict]]:
    import random
    from faker import Faker

    gen.rng     = random.Random(seed)
    Faker.seed(seed)
    gen.fake_uk = Faker("en_GB")
    gen.fake_us = Faker("en_US")

    end_date   = gen._END_DATE
    start_date = end_date - timedelta(days=days - 1)
    dates      = gen.date_range(start_date, end_date)

    print(f"  Date range : {start_date} -> {end_date} ({len(dates)} days)")
    print(f"  Scale      : {scale}")

    # US dimensions required as upstream inputs
    dim_customer     = gen.gen_dim_customer(n=len(gen.US_CUSTOMERS))
    dim_site         = gen.gen_dim_site(dim_customer, n=max(12, int(35 * scale)))
    dim_service_line = gen.gen_dim_service_line()

    dim_airport     = gen.gen_dim_airport_uk(dim_site)
    dim_terminal    = gen.gen_dim_terminal(dim_airport)
    dim_zone        = gen.gen_dim_service_zone(dim_terminal)
    dim_shift_type  = gen.gen_dim_shift_type()
    dim_task_type   = gen.gen_dim_task_type(dim_service_line)
    dim_equipment   = gen.gen_dim_equipment_uk(dim_airport,
                                               n_per_airport=max(5, int(10 * scale)))
    dim_employee_uk = gen.gen_dim_employee_uk(dim_airport, n=max(30, int(200 * scale)))
    dim_roster_src  = gen.gen_dim_roster_source()

    fact_shift = gen.gen_fact_shift_assignment(dim_employee_uk, dim_airport, dim_terminal,
                                               dim_shift_type, dim_roster_src, dates, scale)
    fact_task  = gen.gen_fact_task_execution(dim_airport, dim_zone, dim_task_type,
                                             dim_employee_uk, dates, scale)
    fact_mob   = gen.gen_fact_passenger_mobility_request(dim_airport, dim_terminal, dim_zone,
                                                         dim_task_type, dim_employee_uk,
                                                         dates, scale)
    fact_clean = gen.gen_fact_cleaning_job(dim_airport, dim_zone, dim_task_type,
                                           dim_employee_uk, dim_equipment, dates, scale)
    fact_inc   = gen.gen_fact_ops_incident(dim_airport, dim_service_line, dates, scale)

    bronze_fms    = gen.gen_bronze_findmyshift_roster(fact_shift, dim_employee_uk)
    bronze_oracle = gen.gen_bronze_oracle_finance_extract(dim_employee_uk, dates)
    bronze_tmghcm = gen.gen_bronze_tmghcm_worker_extract(dim_employee_uk)

    return {
        "dim_airport_uk":                  dim_airport,
        "dim_terminal":                    dim_terminal,
        "dim_service_zone":                dim_zone,
        "dim_shift_type":                  dim_shift_type,
        "dim_task_type":                   dim_task_type,
        "dim_equipment_uk":                dim_equipment,
        "dim_employee_uk":                 dim_employee_uk,
        "dim_roster_source":               dim_roster_src,
        "fact_shift_assignment":           fact_shift,
        "fact_task_execution":             fact_task,
        "fact_passenger_mobility_request": fact_mob,
        "fact_cleaning_job":               fact_clean,
        "fact_ops_incident":               fact_inc,
        "bronze_findmyshift_roster":       bronze_fms,
        "bronze_oracle_finance_extract":   bronze_oracle,
        "bronze_tmghcm_worker_extract":    bronze_tmghcm,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish ABM UK aviation dataset as Delta tables to Fabric OneLake."
    )
    parser.add_argument("--days",  type=int,   default=DEFAULT_DAYS,
                        help=f"Days of history. Default: {DEFAULT_DAYS}")
    parser.add_argument("--scale", type=float, default=DEFAULT_SCALE,
                        help=f"Volume multiplier. Default: {DEFAULT_SCALE}")
    parser.add_argument("--seed",  type=int,   default=DEFAULT_SEED,
                        help="Random seed.")
    parser.add_argument("--table", type=str,   default=None,
                        help="Publish a single named table (default: all).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Stage locally to output/abm/delta_uk/ but skip OneLake write.")
    return parser.parse_args()


def main() -> None:
    args  = parse_args()
    scale = max(0.05, min(5.0, args.scale))

    target = f"abfss://{_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{_LAKEHOUSE_ITEM_ID}/Tables/{_SCHEMA_NAME}/"
    print(f"\nABM UK Lakehouse Publisher")
    print(f"  Target  : {target}")
    if args.dry_run:
        print(f"  Staging : {_STAGING_DIR}  (dry-run — no OneLake write)")
    print()

    print("Generating UK dataset...")
    t0 = time.perf_counter()
    tables = generate_uk_tables(days=args.days, scale=scale, seed=args.seed)
    total_rows = sum(len(v) for v in tables.values())
    print(f"  Generated {total_rows:,} rows across {len(tables)} tables "
          f"in {time.perf_counter() - t0:.1f}s\n")

    if args.table:
        if args.table not in tables:
            print(f"ERROR: Unknown table '{args.table}'. Available: {', '.join(tables)}")
            sys.exit(1)
        tables = {args.table: tables[args.table]}

    storage_options: dict = {}
    if not args.dry_run:
        print("Acquiring Azure credential (DefaultAzureCredential)...")
        storage_options = _get_storage_options()
        print("  Token acquired.\n")

    if args.dry_run:
        _STAGING_DIR.mkdir(parents=True, exist_ok=True)

    print(f"{'Table':<48} {'Rows':>8}  {'Time':>6}  Status")
    print("-" * 70)

    t_total = time.perf_counter()

    for name, rows in tables.items():
        t1 = time.perf_counter()
        arrow_table = _to_arrow(rows)

        if args.dry_run:
            local_path  = _write_dry_run(name, arrow_table)
            local_files = [f for f in local_path.rglob("*") if f.is_file()]
            mb = sum(f.stat().st_size for f in local_files) / 1_048_576
            elapsed = time.perf_counter() - t1
            print(f"  [DRY]  {name:<48} {len(rows):>8,}  {elapsed:5.1f}s  "
                  f"staged ({len(local_files)} files, {mb:.1f} MB)")
        else:
            _write_to_onelake(name, arrow_table, storage_options)
            elapsed = time.perf_counter() - t1
            print(f"  {name:<48} {len(rows):>8,}  {elapsed:5.1f}s  OK")

    total_elapsed = time.perf_counter() - t_total
    print("-" * 70)

    if args.dry_run:
        print(f"\nDry run complete. {total_rows:,} rows staged at {_STAGING_DIR}")
        print("Re-run without --dry-run to write directly to OneLake.")
    else:
        print(f"\n{total_rows:,} rows written as Delta tables in {total_elapsed:.1f}s")
        print(f"Path: {target}")


if __name__ == "__main__":
    main()
