#!/usr/bin/env python3
"""Generate a Gensler-inspired workspace performance dataset for Azure SQL MI.

The output is a SQL Server import package:

- schema.sql creates normalized tables with primary keys and useful indexes.
- *.csv files contain deterministic synthetic data in dependency load order.
- bulk_insert.sql contains BULK INSERT statements with a path placeholder.

The dataset is designed to be mirrored from Azure SQL Managed Instance into
Microsoft Fabric and analyzed as a live design feedback loop.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import shutil
import struct
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pyodbc
from dotenv import load_dotenv
from faker import Faker


_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = _REPO_ROOT / "output" / "workspace_performance" / "output"
DEFAULT_SEED = 42027
DEFAULT_DAYS = 45
DEFAULT_SQL_SERVER = "temalo-mi.public.72232dac5390.database.windows.net,3342"
DEFAULT_SQL_DATABASE = "GenslerData"
DEFAULT_SQL_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_SQL_SCHEMA = "workspace"
SQL_COPT_SS_ACCESS_TOKEN = 1256

fake = Faker("en_US")


TABLE_LOAD_ORDER = (
    "clients",
    "buildings",
    "floors",
    "zones",
    "spaces",
    "design_targets",
    "booking_events",
    "badge_events",
    "space_utilization_hourly",
    "environmental_metrics_hourly",
    "experience_surveys",
    "maintenance_tickets",
    "design_outcome_daily",
)

INDUSTRIES = (
    "Technology",
    "Legal",
    "Financial Services",
    "Media",
    "Professional Services",
    "Healthcare",
    "Aviation",
    "Consumer Products",
    "Higher Education",
    "Life Sciences",
)

SPACE_TYPES = (
    "Open Office",
    "Focus Room",
    "Collaboration",
    "Conference Room",
    "Amenity",
    "Lab",
    "Training",
    "Support",
)

SPACE_TYPE_WEIGHTS = {
    "Technology": {
        "Open Office": 30,
        "Focus Room": 18,
        "Collaboration": 20,
        "Conference Room": 10,
        "Amenity": 8,
        "Lab": 3,
        "Training": 5,
        "Support": 6,
    },
    "Legal": {
        "Open Office": 18,
        "Focus Room": 28,
        "Collaboration": 8,
        "Conference Room": 22,
        "Amenity": 6,
        "Lab": 0,
        "Training": 6,
        "Support": 12,
    },
    "Life Sciences": {
        "Open Office": 18,
        "Focus Room": 12,
        "Collaboration": 10,
        "Conference Room": 8,
        "Amenity": 5,
        "Lab": 34,
        "Training": 5,
        "Support": 8,
    },
}

CITIES = (
    ("New York", "Northeast", "United States", 40.7128, -74.0060),
    ("Chicago", "Midwest", "United States", 41.8781, -87.6298),
    ("San Francisco", "West", "United States", 37.7749, -122.4194),
    ("Los Angeles", "West", "United States", 34.0522, -118.2437),
    ("Seattle", "West", "United States", 47.6062, -122.3321),
    ("Dallas", "South", "United States", 32.7767, -96.7970),
    ("Atlanta", "South", "United States", 33.7490, -84.3880),
    ("Boston", "Northeast", "United States", 42.3601, -71.0589),
    ("Toronto", "Canada", "Canada", 43.6532, -79.3832),
    ("London", "EMEA", "United Kingdom", 51.5072, -0.1276),
    ("Singapore", "APAC", "Singapore", 1.3521, 103.8198),
)

TARGETS_BY_SPACE_TYPE = {
    "Open Office": {"utilization": 0.68, "capacity": 24, "energy": 0.38, "comfort": 4.1},
    "Focus Room": {"utilization": 0.58, "capacity": 2, "energy": 0.30, "comfort": 4.3},
    "Collaboration": {"utilization": 0.70, "capacity": 10, "energy": 0.42, "comfort": 4.2},
    "Conference Room": {"utilization": 0.62, "capacity": 12, "energy": 0.46, "comfort": 4.0},
    "Amenity": {"utilization": 0.44, "capacity": 18, "energy": 0.34, "comfort": 4.4},
    "Lab": {"utilization": 0.72, "capacity": 8, "energy": 0.88, "comfort": 3.9},
    "Training": {"utilization": 0.52, "capacity": 28, "energy": 0.50, "comfort": 4.0},
    "Support": {"utilization": 0.36, "capacity": 4, "energy": 0.28, "comfort": 3.8},
}

COMMENTS = {
    "positive": (
        "Great daylight and easy access to teammates.",
        "The room supports focused work without feeling isolated.",
        "Collaboration zones feel active and well located.",
        "Temperature and air quality were comfortable all day.",
    ),
    "neutral": (
        "Space works for most meetings but booking demand is uneven.",
        "Generally acceptable, with some variation by time of day.",
        "The layout is functional but wayfinding could be clearer.",
    ),
    "negative": (
        "Noise carries into nearby focus areas.",
        "The space is often full when people need it.",
        "Too much HVAC runtime for how lightly the zone is used.",
        "Lighting and temperature make long sessions difficult.",
    ),
}


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: Sequence[str]


TABLES = {
    "clients": TableSpec(
        "clients",
        (
            "client_id",
            "client_name",
            "industry",
            "headquarters_city",
            "employee_count",
            "hybrid_policy",
            "portfolio_region",
        ),
    ),
    "buildings": TableSpec(
        "buildings",
        (
            "building_id",
            "client_id",
            "building_name",
            "city",
            "region",
            "country",
            "latitude",
            "longitude",
            "gross_sq_ft",
            "leed_target",
            "well_target",
            "opened_on",
        ),
    ),
    "floors": TableSpec(
        "floors",
        ("floor_id", "building_id", "floor_number", "floor_label", "rentable_sq_ft"),
    ),
    "zones": TableSpec(
        "zones",
        (
            "zone_id",
            "floor_id",
            "zone_name",
            "design_pattern",
            "dominant_space_type",
            "target_utilization_pct",
            "target_energy_kwh_per_sqft_day",
        ),
    ),
    "spaces": TableSpec(
        "spaces",
        (
            "space_id",
            "zone_id",
            "room_number",
            "space_name",
            "space_type",
            "intended_use",
            "sq_ft",
            "seating_capacity",
            "acoustic_rating",
            "daylight_score",
            "has_reservable_booking",
            "sensor_coverage",
        ),
    ),
    "design_targets": TableSpec(
        "design_targets",
        (
            "target_id",
            "space_id",
            "intended_utilization_pct",
            "designed_capacity",
            "target_energy_kwh_per_day",
            "target_satisfaction_score",
            "target_noise_score",
            "target_co2_ppm",
        ),
    ),
    "booking_events": TableSpec(
        "booking_events",
        (
            "booking_id",
            "space_id",
            "booking_start_utc",
            "booking_end_utc",
            "organizer_department",
            "attendee_count",
            "booking_status",
            "source_system",
        ),
    ),
    "badge_events": TableSpec(
        "badge_events",
        (
            "badge_event_id",
            "building_id",
            "event_timestamp_utc",
            "event_type",
            "employee_segment",
            "entry_gate",
        ),
    ),
    "space_utilization_hourly": TableSpec(
        "space_utilization_hourly",
        (
            "utilization_id",
            "space_id",
            "observed_hour_utc",
            "is_occupied",
            "avg_occupancy_count",
            "peak_occupancy_count",
            "utilization_pct",
            "is_underutilized",
            "is_overutilized",
            "signal_source",
        ),
    ),
    "environmental_metrics_hourly": TableSpec(
        "environmental_metrics_hourly",
        (
            "environmental_metric_id",
            "zone_id",
            "observed_hour_utc",
            "energy_kwh",
            "carbon_kgco2e",
            "hvac_runtime_minutes",
            "avg_temperature_f",
            "avg_co2_ppm",
            "avg_pm25_ug_m3",
            "water_gallons",
        ),
    ),
    "experience_surveys": TableSpec(
        "experience_surveys",
        (
            "survey_id",
            "space_id",
            "submitted_at_utc",
            "respondent_role",
            "comfort_score",
            "collaboration_score",
            "noise_score",
            "overall_satisfaction",
            "sentiment_category",
            "comment_text",
        ),
    ),
    "maintenance_tickets": TableSpec(
        "maintenance_tickets",
        (
            "ticket_id",
            "space_id",
            "opened_at_utc",
            "closed_at_utc",
            "ticket_category",
            "priority",
            "status",
            "resolution_hours",
            "description",
        ),
    ),
    "design_outcome_daily": TableSpec(
        "design_outcome_daily",
        (
            "outcome_id",
            "space_id",
            "outcome_date",
            "actual_utilization_pct",
            "intended_utilization_pct",
            "peak_demand_vs_capacity_pct",
            "energy_vs_target_pct",
            "avg_satisfaction_score",
            "design_success_score",
            "outcome_flag",
        ),
    ),
}


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Generate a workspace performance dataset for Azure SQL Managed Instance import."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for CSV and SQL files. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Volume multiplier. Default 1.0 creates a substantial demo dataset.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of trailing operating days to simulate. Default: {DEFAULT_DAYS}",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducible data. Default: {DEFAULT_SEED}",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=None,
        help="First date to simulate as YYYY-MM-DD. Defaults to today minus --days.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove the output directory before generating files.",
    )
    parser.add_argument(
        "--load-sql",
        action="store_true",
        help="Create tables and populate Azure SQL MI/SQL DB after generating the CSV files.",
    )
    parser.add_argument(
        "--sql-server",
        default=os.getenv("AZURE_SQL_SERVER", DEFAULT_SQL_SERVER),
        help="SQL MI/SQL DB server endpoint. Defaults to AZURE_SQL_SERVER from .env.",
    )
    parser.add_argument(
        "--sql-database",
        default=os.getenv("AZURE_SQL_DATABASE", DEFAULT_SQL_DATABASE),
        help="Target database name. Defaults to AZURE_SQL_DATABASE from .env.",
    )
    parser.add_argument(
        "--sql-driver",
        default=os.getenv("AZURE_SQL_DRIVER", DEFAULT_SQL_DRIVER),
        help="Installed ODBC driver name. Defaults to AZURE_SQL_DRIVER from .env.",
    )
    parser.add_argument(
        "--sql-schema",
        default=os.getenv("AZURE_SQL_SCHEMA", DEFAULT_SQL_SCHEMA),
        help="Target schema name. Defaults to AZURE_SQL_SCHEMA from .env.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Rows per executemany batch when --load-sql is used. Default: 5000",
    )
    return parser.parse_args()


def ensure_output_dir(output_dir: Path, clean: bool) -> None:
    if clean and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def make_id(prefix: str, value: int) -> str:
    return f"{prefix}{value:07d}"


def iso_dt(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.replace(microsecond=0).isoformat(sep=" ")


def iso_date(value: date) -> str:
    return value.isoformat()


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def weighted_choice(weights: Mapping[str, int]) -> str:
    return random.choices(list(weights.keys()), weights=list(weights.values()), k=1)[0]


def write_csv(path: Path, columns: Sequence[str], rows: Iterable[Mapping[str, object]]) -> int:
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def validate_sql_identifier(value: str, label: str) -> str:
    if not value or not value.replace("_", "").isalnum() or value[0].isdigit():
        raise ValueError(f"{label} must contain only letters, numbers, and underscores, and cannot start with a digit.")
    return value


def quote_ident(value: str) -> str:
    return f"[{value.replace(']', ']]')}]"


def get_azure_sql_access_token() -> bytes:
    azure_cli = shutil.which("az.cmd") or shutil.which("az.exe") or shutil.which("az")
    if not azure_cli:
        raise RuntimeError("Azure CLI was not found on PATH. Install Azure CLI or run this from an Azure CLI-enabled shell.")
    result = subprocess.run(
        [
            azure_cli,
            "account",
            "get-access-token",
            "--resource",
            "https://database.windows.net/",
            "--query",
            "accessToken",
            "--output",
            "tsv",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    token = result.stdout.strip()
    if not token:
        raise RuntimeError("Azure CLI did not return an access token. Run `az login` and try again.")
    encoded = token.encode("utf-16-le")
    return struct.pack(f"<I{len(encoded)}s", len(encoded), encoded)


def connect_with_azure_cli_token(server: str, database: str, driver: str) -> pyodbc.Connection:
    connection_string = (
        f"Driver={{{driver}}};"
        f"Server=tcp:{server};"
        f"Database={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(
        connection_string,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: get_azure_sql_access_token()},
    )


def split_sql_batches(sql_text: str) -> list[str]:
    batches: list[str] = []
    current: list[str] = []
    for line in sql_text.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
            continue
        current.append(line)
    batch = "\n".join(current).strip()
    if batch:
        batches.append(batch)
    return batches


def execute_sql_file(connection: pyodbc.Connection, path: Path) -> None:
    cursor = connection.cursor()
    for batch in split_sql_batches(path.read_text(encoding="utf-8")):
        cursor.execute(batch)
    connection.commit()


def csv_batches(path: Path, batch_size: int) -> Iterable[list[tuple[object, ...]]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        next(reader)
        batch: list[tuple[object, ...]] = []
        for row in reader:
            batch.append(tuple(None if value == "" else value for value in row))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def load_csvs_to_sql(
    output_dir: Path,
    server: str,
    database: str,
    driver: str,
    schema_name: str,
    batch_size: int,
) -> None:
    validate_sql_identifier(schema_name, "SQL schema")
    if batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0.")

    print(f"Connecting to {server}/{database} with Azure CLI authentication...")
    with connect_with_azure_cli_token(server, database, driver) as connection:
        print("Applying schema.sql...")
        execute_sql_file(connection, output_dir / "schema.sql")

        cursor = connection.cursor()
        cursor.fast_executemany = True
        for table_name in TABLE_LOAD_ORDER:
            spec = TABLES[table_name]
            columns = ", ".join(quote_ident(column) for column in spec.columns)
            placeholders = ", ".join("?" for _ in spec.columns)
            sql = f"INSERT INTO {quote_ident(schema_name)}.{quote_ident(table_name)} ({columns}) VALUES ({placeholders})"
            table_path = table_file(output_dir, table_name)
            inserted = 0
            cursor.setinputsizes([(pyodbc.SQL_WVARCHAR, 1000, 0)] * len(spec.columns))
            for batch in csv_batches(table_path, batch_size):
                cursor.executemany(sql, batch)
                inserted += len(batch)
                if inserted % (batch_size * 10) == 0:
                    connection.commit()
                    print(f" - {table_name}: {inserted:,} rows inserted")
            connection.commit()
            print(f" - {table_name}: {inserted:,} rows inserted")


def business_hour_weight(hour: int, weekday: int) -> float:
    if weekday >= 5:
        return 0.10 if 10 <= hour <= 16 else 0.02
    if 8 <= hour <= 11:
        return 0.78
    if 12 <= hour <= 13:
        return 0.42
    if 14 <= hour <= 17:
        return 0.70
    if 18 <= hour <= 20:
        return 0.20
    return 0.03


def industry_utilization_bias(industry: str) -> float:
    return {
        "Technology": 0.88,
        "Legal": 1.08,
        "Financial Services": 0.96,
        "Media": 0.92,
        "Professional Services": 0.98,
        "Healthcare": 1.04,
        "Aviation": 0.90,
        "Consumer Products": 0.86,
        "Higher Education": 0.82,
        "Life Sciences": 1.02,
    }.get(industry, 0.95)


def space_type_utilization_bias(space_type: str, industry: str) -> float:
    base = {
        "Open Office": 0.82,
        "Focus Room": 1.22,
        "Collaboration": 0.94,
        "Conference Room": 1.05,
        "Amenity": 0.58,
        "Lab": 1.18,
        "Training": 0.62,
        "Support": 0.44,
    }[space_type]
    if industry == "Technology" and space_type == "Collaboration":
        base += 0.24
    if industry == "Legal" and space_type in {"Focus Room", "Conference Room"}:
        base += 0.20
    if industry == "Legal" and space_type == "Open Office":
        base -= 0.18
    return base


def satisfaction_from_conditions(utilization_pct: float, temp_f: float, co2_ppm: float, noise_score: float) -> float:
    score = 4.55
    score -= abs(temp_f - 72.0) * 0.045
    score -= max(0.0, co2_ppm - 850.0) / 420.0
    score -= max(0.0, utilization_pct - 88.0) / 70.0
    score -= max(0.0, 3.4 - noise_score) * 0.35
    score += random.uniform(-0.22, 0.22)
    return round(clamp(score, 1.0, 5.0), 2)


def table_file(output_dir: Path, table_name: str) -> Path:
    return output_dir / f"{table_name}.csv"


def build_static_dimensions(scale: float) -> dict[str, list[dict[str, object]]]:
    client_count = max(8, int(14 * scale))
    building_count = max(10, int(22 * scale))

    clients: list[dict[str, object]] = []
    buildings: list[dict[str, object]] = []
    floors: list[dict[str, object]] = []
    zones: list[dict[str, object]] = []
    spaces: list[dict[str, object]] = []
    design_targets: list[dict[str, object]] = []

    for idx in range(1, client_count + 1):
        industry = random.choice(INDUSTRIES)
        city, region, _country, _lat, _lon = random.choice(CITIES)
        clients.append(
            {
                "client_id": make_id("CL", idx),
                "client_name": f"{fake.company()} {random.choice(('Group', 'Partners', 'Labs', 'Holdings'))}",
                "industry": industry,
                "headquarters_city": city,
                "employee_count": random.randint(850, 62000),
                "hybrid_policy": random.choice(("Office-led", "Hybrid 3-day", "Hybrid flexible", "Remote-first")),
                "portfolio_region": region,
            }
        )

    for idx in range(1, building_count + 1):
        client = random.choice(clients)
        city, region, country, lat, lon = random.choice(CITIES)
        floor_count = random.randint(4, 12)
        avg_floor_sqft = random.randint(18000, 42000)
        building_sqft = floor_count * avg_floor_sqft
        buildings.append(
            {
                "building_id": make_id("BLD", idx),
                "client_id": client["client_id"],
                "building_name": f"{city} {random.choice(('Workplace Center', 'Design Studio', 'Innovation Hub', 'Campus'))}",
                "city": city,
                "region": region,
                "country": country,
                "latitude": round(lat + random.uniform(-0.08, 0.08), 6),
                "longitude": round(lon + random.uniform(-0.08, 0.08), 6),
                "gross_sq_ft": building_sqft,
                "leed_target": random.choice(("Silver", "Gold", "Platinum", "None")),
                "well_target": random.choice(("Bronze", "Silver", "Gold", "Platinum", "None")),
                "opened_on": iso_date(fake.date_between(date(2017, 1, 1), date(2025, 12, 31))),
            }
        )

        for floor_number in range(1, floor_count + 1):
            floors.append(
                {
                    "floor_id": make_id("FLR", len(floors) + 1),
                    "building_id": make_id("BLD", idx),
                    "floor_number": floor_number,
                    "floor_label": f"{floor_number:02d}",
                    "rentable_sq_ft": int(avg_floor_sqft * random.uniform(0.86, 1.12)),
                }
            )

    clients_by_id = {row["client_id"]: row for row in clients}
    buildings_by_id = {row["building_id"]: row for row in buildings}

    for floor in floors:
        building = buildings_by_id[floor["building_id"]]
        client = clients_by_id[building["client_id"]]
        zone_count = random.randint(3, 7)
        for zone_index in range(1, zone_count + 1):
            dominant_type = weighted_choice(SPACE_TYPE_WEIGHTS.get(str(client["industry"]), {
                "Open Office": 28,
                "Focus Room": 18,
                "Collaboration": 16,
                "Conference Room": 14,
                "Amenity": 7,
                "Lab": 4,
                "Training": 5,
                "Support": 8,
            }))
            target = TARGETS_BY_SPACE_TYPE[dominant_type]
            zones.append(
                {
                    "zone_id": make_id("ZN", len(zones) + 1),
                    "floor_id": floor["floor_id"],
                    "zone_name": f"Floor {floor['floor_label']} Zone {zone_index}",
                    "design_pattern": random.choice(
                        (
                            "Activity-Based Workplace",
                            "Neighborhood Planning",
                            "Client-Facing Suite",
                            "Hybrid Collaboration Hub",
                            "Focus-Forward Plan",
                            "Lab-Office Interface",
                        )
                    ),
                    "dominant_space_type": dominant_type,
                    "target_utilization_pct": round(target["utilization"] * 100, 1),
                    "target_energy_kwh_per_sqft_day": target["energy"],
                }
            )

    zones_by_id = {row["zone_id"]: row for row in zones}
    floors_by_id = {row["floor_id"]: row for row in floors}

    for zone in zones:
        floor = floors_by_id[zone["floor_id"]]
        building = buildings_by_id[floor["building_id"]]
        client = clients_by_id[building["client_id"]]
        industry = str(client["industry"])
        space_count = random.randint(4, 11)
        weights = SPACE_TYPE_WEIGHTS.get(industry, SPACE_TYPE_WEIGHTS["Technology"])
        for space_index in range(1, space_count + 1):
            if random.random() < 0.55:
                space_type = str(zone["dominant_space_type"])
            else:
                space_type = weighted_choice(weights)
            baseline = TARGETS_BY_SPACE_TYPE[space_type]
            capacity = max(1, int(random.gauss(float(baseline["capacity"]), max(1.0, float(baseline["capacity"]) * 0.20))))
            sq_ft = int(capacity * random.uniform(42, 95))
            if space_type == "Focus Room":
                sq_ft = random.randint(55, 145)
            elif space_type == "Open Office":
                sq_ft = random.randint(850, 3200)
            elif space_type == "Lab":
                sq_ft = random.randint(600, 2400)
            reservable = space_type in {"Focus Room", "Collaboration", "Conference Room", "Training", "Lab"}
            spaces.append(
                {
                    "space_id": make_id("SP", len(spaces) + 1),
                    "zone_id": zone["zone_id"],
                    "room_number": f"{floor['floor_label']}{space_index:02d}",
                    "space_name": f"{space_type} {floor['floor_label']}-{space_index:02d}",
                    "space_type": space_type,
                    "intended_use": random.choice(
                        (
                            "Focused work",
                            "Team collaboration",
                            "Client presentation",
                            "Project sprint",
                            "Training",
                            "Touchdown work",
                            "Social connection",
                            "Specialized testing",
                        )
                    ),
                    "sq_ft": sq_ft,
                    "seating_capacity": capacity,
                    "acoustic_rating": random.choice(("Low", "Medium", "High", "Enhanced")),
                    "daylight_score": round(random.uniform(38, 96), 1),
                    "has_reservable_booking": int(reservable),
                    "sensor_coverage": random.choice(("Occupancy Sensor", "People Count Sensor", "Badge + WiFi", "Booking Only")),
                }
            )

    for space in spaces:
        baseline = TARGETS_BY_SPACE_TYPE[str(space["space_type"])]
        intended = round(clamp(random.gauss(float(baseline["utilization"]) * 100, 6), 20, 92), 1)
        design_targets.append(
            {
                "target_id": make_id("TGT", len(design_targets) + 1),
                "space_id": space["space_id"],
                "intended_utilization_pct": intended,
                "designed_capacity": space["seating_capacity"],
                "target_energy_kwh_per_day": round(float(space["sq_ft"]) * float(baseline["energy"]), 2),
                "target_satisfaction_score": baseline["comfort"],
                "target_noise_score": round(random.uniform(3.4, 4.6), 1),
                "target_co2_ppm": random.randint(760, 920),
            }
        )

    return {
        "clients": clients,
        "buildings": buildings,
        "floors": floors,
        "zones": zones,
        "spaces": spaces,
        "design_targets": design_targets,
    }


def write_schema_sql(output_dir: Path, schema_name: str = DEFAULT_SQL_SCHEMA) -> None:
    validate_sql_identifier(schema_name, "SQL schema")
    schema = f"""IF SCHEMA_ID('{schema_name}') IS NULL
    EXEC('CREATE SCHEMA {schema_name}');
GO

DROP TABLE IF EXISTS {schema_name}.design_outcome_daily;
DROP TABLE IF EXISTS {schema_name}.maintenance_tickets;
DROP TABLE IF EXISTS {schema_name}.experience_surveys;
DROP TABLE IF EXISTS {schema_name}.environmental_metrics_hourly;
DROP TABLE IF EXISTS {schema_name}.space_utilization_hourly;
DROP TABLE IF EXISTS {schema_name}.badge_events;
DROP TABLE IF EXISTS {schema_name}.booking_events;
DROP TABLE IF EXISTS {schema_name}.design_targets;
DROP TABLE IF EXISTS {schema_name}.spaces;
DROP TABLE IF EXISTS {schema_name}.zones;
DROP TABLE IF EXISTS {schema_name}.floors;
DROP TABLE IF EXISTS {schema_name}.buildings;
DROP TABLE IF EXISTS {schema_name}.clients;
GO

CREATE TABLE {schema_name}.clients (
    client_id varchar(16) NOT NULL PRIMARY KEY,
    client_name nvarchar(160) NOT NULL,
    industry nvarchar(80) NOT NULL,
    headquarters_city nvarchar(80) NOT NULL,
    employee_count int NOT NULL,
    hybrid_policy nvarchar(40) NOT NULL,
    portfolio_region nvarchar(40) NOT NULL
);

CREATE TABLE {schema_name}.buildings (
    building_id varchar(16) NOT NULL PRIMARY KEY,
    client_id varchar(16) NOT NULL,
    building_name nvarchar(160) NOT NULL,
    city nvarchar(80) NOT NULL,
    region nvarchar(40) NOT NULL,
    country nvarchar(80) NOT NULL,
    latitude decimal(9,6) NOT NULL,
    longitude decimal(9,6) NOT NULL,
    gross_sq_ft int NOT NULL,
    leed_target nvarchar(20) NOT NULL,
    well_target nvarchar(20) NOT NULL,
    opened_on date NOT NULL
);

CREATE TABLE {schema_name}.floors (
    floor_id varchar(16) NOT NULL PRIMARY KEY,
    building_id varchar(16) NOT NULL,
    floor_number int NOT NULL,
    floor_label nvarchar(12) NOT NULL,
    rentable_sq_ft int NOT NULL
);

CREATE TABLE {schema_name}.zones (
    zone_id varchar(16) NOT NULL PRIMARY KEY,
    floor_id varchar(16) NOT NULL,
    zone_name nvarchar(120) NOT NULL,
    design_pattern nvarchar(80) NOT NULL,
    dominant_space_type nvarchar(40) NOT NULL,
    target_utilization_pct decimal(5,2) NOT NULL,
    target_energy_kwh_per_sqft_day decimal(8,4) NOT NULL
);

CREATE TABLE {schema_name}.spaces (
    space_id varchar(16) NOT NULL PRIMARY KEY,
    zone_id varchar(16) NOT NULL,
    room_number nvarchar(20) NOT NULL,
    space_name nvarchar(120) NOT NULL,
    space_type nvarchar(40) NOT NULL,
    intended_use nvarchar(80) NOT NULL,
    sq_ft int NOT NULL,
    seating_capacity int NOT NULL,
    acoustic_rating nvarchar(20) NOT NULL,
    daylight_score decimal(5,2) NOT NULL,
    has_reservable_booking bit NOT NULL,
    sensor_coverage nvarchar(40) NOT NULL
);

CREATE TABLE {schema_name}.design_targets (
    target_id varchar(16) NOT NULL PRIMARY KEY,
    space_id varchar(16) NOT NULL,
    intended_utilization_pct decimal(5,2) NOT NULL,
    designed_capacity int NOT NULL,
    target_energy_kwh_per_day decimal(10,2) NOT NULL,
    target_satisfaction_score decimal(4,2) NOT NULL,
    target_noise_score decimal(4,2) NOT NULL,
    target_co2_ppm int NOT NULL
);

CREATE TABLE {schema_name}.booking_events (
    booking_id varchar(16) NOT NULL PRIMARY KEY,
    space_id varchar(16) NOT NULL,
    booking_start_utc datetime2(0) NOT NULL,
    booking_end_utc datetime2(0) NOT NULL,
    organizer_department nvarchar(80) NOT NULL,
    attendee_count int NOT NULL,
    booking_status nvarchar(30) NOT NULL,
    source_system nvarchar(40) NOT NULL
);

CREATE TABLE {schema_name}.badge_events (
    badge_event_id varchar(16) NOT NULL PRIMARY KEY,
    building_id varchar(16) NOT NULL,
    event_timestamp_utc datetime2(0) NOT NULL,
    event_type nvarchar(20) NOT NULL,
    employee_segment nvarchar(60) NOT NULL,
    entry_gate nvarchar(40) NOT NULL
);

CREATE TABLE {schema_name}.space_utilization_hourly (
    utilization_id varchar(20) NOT NULL PRIMARY KEY,
    space_id varchar(16) NOT NULL,
    observed_hour_utc datetime2(0) NOT NULL,
    is_occupied bit NOT NULL,
    avg_occupancy_count decimal(8,2) NOT NULL,
    peak_occupancy_count int NOT NULL,
    utilization_pct decimal(5,2) NOT NULL,
    is_underutilized bit NOT NULL,
    is_overutilized bit NOT NULL,
    signal_source nvarchar(40) NOT NULL
);

CREATE TABLE {schema_name}.environmental_metrics_hourly (
    environmental_metric_id varchar(20) NOT NULL PRIMARY KEY,
    zone_id varchar(16) NOT NULL,
    observed_hour_utc datetime2(0) NOT NULL,
    energy_kwh decimal(10,2) NOT NULL,
    carbon_kgco2e decimal(10,2) NOT NULL,
    hvac_runtime_minutes int NOT NULL,
    avg_temperature_f decimal(5,2) NOT NULL,
    avg_co2_ppm int NOT NULL,
    avg_pm25_ug_m3 decimal(6,2) NOT NULL,
    water_gallons decimal(10,2) NOT NULL
);

CREATE TABLE {schema_name}.experience_surveys (
    survey_id varchar(16) NOT NULL PRIMARY KEY,
    space_id varchar(16) NOT NULL,
    submitted_at_utc datetime2(0) NOT NULL,
    respondent_role nvarchar(60) NOT NULL,
    comfort_score decimal(4,2) NOT NULL,
    collaboration_score decimal(4,2) NOT NULL,
    noise_score decimal(4,2) NOT NULL,
    overall_satisfaction decimal(4,2) NOT NULL,
    sentiment_category nvarchar(20) NOT NULL,
    comment_text nvarchar(400) NOT NULL
);

CREATE TABLE {schema_name}.maintenance_tickets (
    ticket_id varchar(16) NOT NULL PRIMARY KEY,
    space_id varchar(16) NOT NULL,
    opened_at_utc datetime2(0) NOT NULL,
    closed_at_utc datetime2(0) NULL,
    ticket_category nvarchar(40) NOT NULL,
    priority nvarchar(20) NOT NULL,
    status nvarchar(20) NOT NULL,
    resolution_hours decimal(8,2) NULL,
    description nvarchar(240) NOT NULL
);

CREATE TABLE {schema_name}.design_outcome_daily (
    outcome_id varchar(20) NOT NULL PRIMARY KEY,
    space_id varchar(16) NOT NULL,
    outcome_date date NOT NULL,
    actual_utilization_pct decimal(5,2) NOT NULL,
    intended_utilization_pct decimal(5,2) NOT NULL,
    peak_demand_vs_capacity_pct decimal(7,2) NOT NULL,
    energy_vs_target_pct decimal(7,2) NOT NULL,
    avg_satisfaction_score decimal(4,2) NOT NULL,
    design_success_score decimal(5,2) NOT NULL,
    outcome_flag nvarchar(40) NOT NULL
);
GO

ALTER TABLE {schema_name}.buildings ADD CONSTRAINT FK_buildings_clients FOREIGN KEY (client_id) REFERENCES {schema_name}.clients(client_id);
ALTER TABLE {schema_name}.floors ADD CONSTRAINT FK_floors_buildings FOREIGN KEY (building_id) REFERENCES {schema_name}.buildings(building_id);
ALTER TABLE {schema_name}.zones ADD CONSTRAINT FK_zones_floors FOREIGN KEY (floor_id) REFERENCES {schema_name}.floors(floor_id);
ALTER TABLE {schema_name}.spaces ADD CONSTRAINT FK_spaces_zones FOREIGN KEY (zone_id) REFERENCES {schema_name}.zones(zone_id);
ALTER TABLE {schema_name}.design_targets ADD CONSTRAINT FK_design_targets_spaces FOREIGN KEY (space_id) REFERENCES {schema_name}.spaces(space_id);
ALTER TABLE {schema_name}.booking_events ADD CONSTRAINT FK_booking_events_spaces FOREIGN KEY (space_id) REFERENCES {schema_name}.spaces(space_id);
ALTER TABLE {schema_name}.badge_events ADD CONSTRAINT FK_badge_events_buildings FOREIGN KEY (building_id) REFERENCES {schema_name}.buildings(building_id);
ALTER TABLE {schema_name}.space_utilization_hourly ADD CONSTRAINT FK_utilization_spaces FOREIGN KEY (space_id) REFERENCES {schema_name}.spaces(space_id);
ALTER TABLE {schema_name}.environmental_metrics_hourly ADD CONSTRAINT FK_environmental_zones FOREIGN KEY (zone_id) REFERENCES {schema_name}.zones(zone_id);
ALTER TABLE {schema_name}.experience_surveys ADD CONSTRAINT FK_surveys_spaces FOREIGN KEY (space_id) REFERENCES {schema_name}.spaces(space_id);
ALTER TABLE {schema_name}.maintenance_tickets ADD CONSTRAINT FK_tickets_spaces FOREIGN KEY (space_id) REFERENCES {schema_name}.spaces(space_id);
ALTER TABLE {schema_name}.design_outcome_daily ADD CONSTRAINT FK_outcomes_spaces FOREIGN KEY (space_id) REFERENCES {schema_name}.spaces(space_id);
GO

CREATE INDEX IX_buildings_client ON {schema_name}.buildings(client_id);
CREATE INDEX IX_floors_building ON {schema_name}.floors(building_id);
CREATE INDEX IX_zones_floor ON {schema_name}.zones(floor_id);
CREATE INDEX IX_spaces_zone_type ON {schema_name}.spaces(zone_id, space_type);
CREATE INDEX IX_booking_space_time ON {schema_name}.booking_events(space_id, booking_start_utc);
CREATE INDEX IX_badge_building_time ON {schema_name}.badge_events(building_id, event_timestamp_utc);
CREATE INDEX IX_utilization_space_time ON {schema_name}.space_utilization_hourly(space_id, observed_hour_utc);
CREATE INDEX IX_environmental_zone_time ON {schema_name}.environmental_metrics_hourly(zone_id, observed_hour_utc);
CREATE INDEX IX_surveys_space_time ON {schema_name}.experience_surveys(space_id, submitted_at_utc);
CREATE INDEX IX_tickets_space_time ON {schema_name}.maintenance_tickets(space_id, opened_at_utc);
CREATE INDEX IX_outcomes_space_date ON {schema_name}.design_outcome_daily(space_id, outcome_date);
GO
"""
    (output_dir / "schema.sql").write_text(schema, encoding="utf-8")


def write_bulk_insert_sql(output_dir: Path) -> None:
    lines = [
        "-- Azure SQL Managed Instance CSV import script.",
        "-- Upload the generated CSV files to Azure Blob Storage, then replace the placeholders below.",
        "-- SAS token should not include the leading question mark.",
        "-- If the files are at the container root, remove '<optional-folder>/' from each BULK INSERT FROM path.",
        "",
        "-- If your database does not already have a master key, uncomment and set a strong password:",
        "-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<strong-password-here>';",
        "",
        "CREATE DATABASE SCOPED CREDENTIAL WorkspaceCsvSasCredential",
        "WITH IDENTITY = 'SHARED ACCESS SIGNATURE',",
        "SECRET = '<sas-token-without-leading-question-mark>';",
        "GO",
        "",
        "CREATE EXTERNAL DATA SOURCE WorkspaceCsvBlob",
        "WITH (",
        "    TYPE = BLOB_STORAGE,",
        "    LOCATION = 'https://<storage-account>.blob.core.windows.net/<container>',",
        "    CREDENTIAL = WorkspaceCsvSasCredential",
        ");",
        "GO",
        "",
    ]
    for table_name in TABLE_LOAD_ORDER:
        lines.extend(
            [
                f"PRINT 'Loading workspace.{table_name}';",
                f"BULK INSERT workspace.{table_name}",
                f"FROM '<optional-folder>/{table_name}.csv'",
                "WITH (",
                "    DATA_SOURCE = 'WorkspaceCsvBlob',",
                "    FORMAT = 'CSV',",
                "    FIRSTROW = 2,",
                "    FIELDQUOTE = '\"',",
                "    FIELDTERMINATOR = ',',",
                "    ROWTERMINATOR = '0x0a',",
                "    TABLOCK,",
                "    CODEPAGE = '65001'",
                ");",
                "",
            ]
        )
    (output_dir / "bulk_insert.sql").write_text("\n".join(lines), encoding="utf-8")


def summarize_counts(output_dir: Path, counts: Mapping[str, int], args: argparse.Namespace) -> None:
    summary = {
        "dataset": "Workspace Performance & Experience Mirror",
        "target": "Azure SQL Managed Instance mirrored to Microsoft Fabric",
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "seed": args.seed,
        "scale": args.scale,
        "days": args.days,
        "tables": counts,
        "demo_story": [
            "Compare design intent to actual workplace utilization.",
            "Find overbooked focus rooms and underused collaboration zones.",
            "Tie HVAC and carbon impact to real occupancy patterns.",
            "Connect post-occupancy experience sentiment back to design choices.",
        ],
    }
    (output_dir / "dataset_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.scale <= 0:
        raise ValueError("--scale must be greater than 0.")
    if args.days <= 0:
        raise ValueError("--days must be greater than 0.")

    random.seed(args.seed)
    Faker.seed(args.seed)
    ensure_output_dir(args.output_dir, args.clean)

    start_date = args.start_date or (date.today() - timedelta(days=args.days))
    operating_dates = [start_date + timedelta(days=offset) for offset in range(args.days)]

    static = build_static_dimensions(args.scale)
    clients = static["clients"]
    buildings = static["buildings"]
    floors = static["floors"]
    zones = static["zones"]
    spaces = static["spaces"]
    design_targets = static["design_targets"]

    floors_by_id = {row["floor_id"]: row for row in floors}
    zones_by_id = {row["zone_id"]: row for row in zones}
    spaces_by_id = {row["space_id"]: row for row in spaces}
    buildings_by_id = {row["building_id"]: row for row in buildings}
    clients_by_id = {row["client_id"]: row for row in clients}
    targets_by_space = {row["space_id"]: row for row in design_targets}

    building_by_space: dict[str, dict[str, object]] = {}
    client_by_space: dict[str, dict[str, object]] = {}
    spaces_by_zone: dict[str, list[dict[str, object]]] = {}
    for space in spaces:
        zone = zones_by_id[space["zone_id"]]
        floor = floors_by_id[zone["floor_id"]]
        building = buildings_by_id[floor["building_id"]]
        client = clients_by_id[building["client_id"]]
        building_by_space[space["space_id"]] = building
        client_by_space[space["space_id"]] = client
        spaces_by_zone.setdefault(str(space["zone_id"]), []).append(space)

    counts: dict[str, int] = {}
    for table_name in ("clients", "buildings", "floors", "zones", "spaces", "design_targets"):
        counts[table_name] = write_csv(table_file(args.output_dir, table_name), TABLES[table_name].columns, static[table_name])

    departments = ("Design", "Strategy", "Real Estate", "Facilities", "Technology", "Leadership", "Client Team")
    respondent_roles = ("Employee", "Manager", "Visitor", "Facilities", "Project Team")
    ticket_categories = ("Temperature", "Lighting", "Furniture", "Cleaning", "AV", "Noise", "Air Quality")
    priorities = ("Low", "Medium", "High", "Urgent")

    booking_count = 0
    badge_count = 0
    utilization_count = 0
    environmental_count = 0
    survey_count = 0
    ticket_count = 0
    outcome_count = 0

    reservable_spaces = [row for row in spaces if int(row["has_reservable_booking"]) == 1]

    def booking_rows() -> Iterable[dict[str, object]]:
        nonlocal booking_count
        for current_date in operating_dates:
            for space in reservable_spaces:
                if current_date.weekday() >= 5:
                    daily_probability = 0.05
                else:
                    daily_probability = {
                        "Focus Room": 0.34,
                        "Collaboration": 0.30,
                        "Conference Room": 0.42,
                        "Training": 0.14,
                        "Lab": 0.24,
                    }.get(str(space["space_type"]), 0.10)
                if random.random() > daily_probability:
                    continue
                bookings_today = 1 + int(random.random() < 0.28) + int(random.random() < 0.08)
                for _ in range(bookings_today):
                    start_hour = random.choice((8, 9, 10, 11, 13, 14, 15, 16))
                    duration = random.choice((30, 60, 60, 90, 120))
                    start_dt = datetime.combine(current_date, time(start_hour, random.choice((0, 30))))
                    end_dt = start_dt + timedelta(minutes=duration)
                    booking_count += 1
                    yield {
                        "booking_id": make_id("BK", booking_count),
                        "space_id": space["space_id"],
                        "booking_start_utc": iso_dt(start_dt),
                        "booking_end_utc": iso_dt(end_dt),
                        "organizer_department": random.choice(departments),
                        "attendee_count": random.randint(1, max(1, int(space["seating_capacity"]) + 4)),
                        "booking_status": random.choices(("Completed", "Cancelled", "No Show"), weights=(86, 9, 5), k=1)[0],
                        "source_system": random.choice(("Microsoft 365 Rooms", "Condeco", "Robin", "EMS")),
                    }

    counts["booking_events"] = write_csv(
        table_file(args.output_dir, "booking_events"),
        TABLES["booking_events"].columns,
        booking_rows(),
    )

    def badge_rows() -> Iterable[dict[str, object]]:
        nonlocal badge_count
        for current_date in operating_dates:
            for building in buildings:
                client = clients_by_id[building["client_id"]]
                weekday_factor = 0.18 if current_date.weekday() >= 5 else 1.0
                policy_factor = {
                    "Office-led": 0.88,
                    "Hybrid 3-day": 0.58,
                    "Hybrid flexible": 0.44,
                    "Remote-first": 0.24,
                }[str(client["hybrid_policy"])]
                estimated_people = int(math.sqrt(int(building["gross_sq_ft"])) * random.uniform(5.8, 9.6) * policy_factor * weekday_factor)
                for _ in range(max(2, estimated_people)):
                    arrival_hour = int(clamp(random.gauss(8.9, 1.4), 6, 14))
                    event_dt = datetime.combine(current_date, time(arrival_hour, random.randint(0, 59), random.randint(0, 59)))
                    badge_count += 1
                    yield {
                        "badge_event_id": make_id("BDG", badge_count),
                        "building_id": building["building_id"],
                        "event_timestamp_utc": iso_dt(event_dt),
                        "event_type": "Entry",
                        "employee_segment": random.choice(("Assigned Employee", "Visitor", "Vendor", "Project Team")),
                        "entry_gate": random.choice(("Lobby North", "Lobby South", "Garage", "Transit Entry")),
                    }

    counts["badge_events"] = write_csv(
        table_file(args.output_dir, "badge_events"),
        TABLES["badge_events"].columns,
        badge_rows(),
    )

    daily_rollups: dict[tuple[str, date], dict[str, float]] = {}

    def utilization_rows() -> Iterable[dict[str, object]]:
        nonlocal utilization_count
        for current_date in operating_dates:
            for hour in range(24):
                observed = datetime.combine(current_date, time(hour))
                hour_weight = business_hour_weight(hour, current_date.weekday())
                for space in spaces:
                    client = client_by_space[space["space_id"]]
                    target = targets_by_space[space["space_id"]]
                    space_bias = space_type_utilization_bias(str(space["space_type"]), str(client["industry"]))
                    intended = float(target["intended_utilization_pct"])
                    utilization = intended * hour_weight * industry_utilization_bias(str(client["industry"])) * space_bias
                    utilization *= random.uniform(0.72, 1.22)
                    if str(client["hybrid_policy"]) == "Remote-first":
                        utilization *= 0.62
                    if str(space["space_type"]) == "Amenity" and hour in (11, 12, 13):
                        utilization *= 1.85
                    utilization = round(clamp(utilization, 0, 100), 2)
                    capacity = int(space["seating_capacity"])
                    avg_occ = round((utilization / 100.0) * capacity, 2)
                    peak_occ = min(capacity + random.randint(0, 4), max(0, math.ceil(avg_occ * random.uniform(1.05, 1.55))))
                    utilization_count += 1
                    rollup = daily_rollups.setdefault(
                        (str(space["space_id"]), current_date),
                        {"util_sum": 0.0, "peak": 0.0, "hours": 0.0},
                    )
                    rollup["util_sum"] += utilization
                    rollup["peak"] = max(rollup["peak"], float(peak_occ))
                    rollup["hours"] += 1
                    yield {
                        "utilization_id": make_id("UT", utilization_count),
                        "space_id": space["space_id"],
                        "observed_hour_utc": iso_dt(observed),
                        "is_occupied": int(utilization >= 8),
                        "avg_occupancy_count": avg_occ,
                        "peak_occupancy_count": peak_occ,
                        "utilization_pct": utilization,
                        "is_underutilized": int(utilization < intended * 0.45 and 8 <= hour <= 17 and current_date.weekday() < 5),
                        "is_overutilized": int(utilization > min(100, intended * 1.25)),
                        "signal_source": space["sensor_coverage"],
                    }

    counts["space_utilization_hourly"] = write_csv(
        table_file(args.output_dir, "space_utilization_hourly"),
        TABLES["space_utilization_hourly"].columns,
        utilization_rows(),
    )

    def environmental_rows() -> Iterable[dict[str, object]]:
        nonlocal environmental_count
        for current_date in operating_dates:
            seasonal = 1.0 + 0.18 * math.sin((current_date.timetuple().tm_yday / 365.0) * math.tau)
            for hour in range(24):
                observed = datetime.combine(current_date, time(hour))
                hour_weight = business_hour_weight(hour, current_date.weekday())
                for zone in zones:
                    zone_spaces = spaces_by_zone[str(zone["zone_id"])]
                    zone_sqft = sum(int(space["sq_ft"]) for space in zone_spaces)
                    avg_util = 0.0
                    for space in zone_spaces:
                        rollup = daily_rollups.get((str(space["space_id"]), current_date))
                        if rollup:
                            avg_util += rollup["util_sum"] / max(1.0, rollup["hours"])
                    avg_util = avg_util / max(1, len(zone_spaces))
                    base_energy = zone_sqft * float(zone["target_energy_kwh_per_sqft_day"]) / 24.0
                    energy = base_energy * seasonal * (0.62 + hour_weight * 0.55 + avg_util / 240.0) * random.uniform(0.88, 1.16)
                    hvac = int(clamp(energy / max(1, base_energy) * 38 + hour_weight * 22, 0, 60))
                    co2 = int(clamp(430 + avg_util * 6.7 + random.gauss(0, 65), 390, 1650))
                    temp = round(clamp(72 + random.gauss(0, 1.8) + (seasonal - 1) * 5, 66, 80), 2)
                    environmental_count += 1
                    yield {
                        "environmental_metric_id": make_id("ENV", environmental_count),
                        "zone_id": zone["zone_id"],
                        "observed_hour_utc": iso_dt(observed),
                        "energy_kwh": round(energy, 2),
                        "carbon_kgco2e": round(energy * random.uniform(0.18, 0.44), 2),
                        "hvac_runtime_minutes": hvac,
                        "avg_temperature_f": temp,
                        "avg_co2_ppm": co2,
                        "avg_pm25_ug_m3": round(clamp(random.gauss(5.8, 2.2) + avg_util / 45, 1.0, 35.0), 2),
                        "water_gallons": round(max(0.0, avg_util * len(zone_spaces) * random.uniform(0.05, 0.22)), 2),
                    }

    counts["environmental_metrics_hourly"] = write_csv(
        table_file(args.output_dir, "environmental_metrics_hourly"),
        TABLES["environmental_metrics_hourly"].columns,
        environmental_rows(),
    )

    def survey_rows() -> Iterable[dict[str, object]]:
        nonlocal survey_count
        for current_date in operating_dates:
            for space in spaces:
                probability = 0.006
                if str(space["space_type"]) in {"Conference Room", "Focus Room", "Collaboration"}:
                    probability = 0.012
                if random.random() > probability:
                    continue
                rollup = daily_rollups.get((str(space["space_id"]), current_date), {"util_sum": 0, "hours": 1})
                util = rollup["util_sum"] / max(1.0, rollup["hours"])
                noise = round(clamp(4.5 - util / 90 + random.uniform(-0.45, 0.35), 1.0, 5.0), 2)
                satisfaction = satisfaction_from_conditions(util, random.uniform(69, 76), random.uniform(560, 1050), noise)
                sentiment = "positive" if satisfaction >= 4.0 else "neutral" if satisfaction >= 3.0 else "negative"
                survey_count += 1
                yield {
                    "survey_id": make_id("SRV", survey_count),
                    "space_id": space["space_id"],
                    "submitted_at_utc": iso_dt(datetime.combine(current_date, time(random.randint(9, 18), random.randint(0, 59)))),
                    "respondent_role": random.choice(respondent_roles),
                    "comfort_score": satisfaction_from_conditions(util, random.uniform(68, 77), random.uniform(500, 1150), noise),
                    "collaboration_score": round(clamp(satisfaction + random.uniform(-0.4, 0.45), 1.0, 5.0), 2),
                    "noise_score": noise,
                    "overall_satisfaction": satisfaction,
                    "sentiment_category": sentiment,
                    "comment_text": random.choice(COMMENTS[sentiment]),
                }

    counts["experience_surveys"] = write_csv(
        table_file(args.output_dir, "experience_surveys"),
        TABLES["experience_surveys"].columns,
        survey_rows(),
    )

    def ticket_rows() -> Iterable[dict[str, object]]:
        nonlocal ticket_count
        for current_date in operating_dates:
            for space in spaces:
                probability = 0.0025
                if str(space["space_type"]) in {"Lab", "Conference Room"}:
                    probability = 0.005
                if random.random() > probability:
                    continue
                opened = datetime.combine(current_date, time(random.randint(7, 18), random.randint(0, 59)))
                priority = random.choices(priorities, weights=(45, 38, 14, 3), k=1)[0]
                resolution_hours = round(random.uniform(2, 96) * (2.2 if priority == "Low" else 1.0), 2)
                is_open = random.random() < 0.08
                closed = None if is_open else opened + timedelta(hours=resolution_hours)
                category = random.choice(ticket_categories)
                ticket_count += 1
                yield {
                    "ticket_id": make_id("TCK", ticket_count),
                    "space_id": space["space_id"],
                    "opened_at_utc": iso_dt(opened),
                    "closed_at_utc": iso_dt(closed),
                    "ticket_category": category,
                    "priority": priority,
                    "status": "Open" if is_open else "Closed",
                    "resolution_hours": "" if is_open else resolution_hours,
                    "description": f"{category} issue reported in {space['space_name']}",
                }

    counts["maintenance_tickets"] = write_csv(
        table_file(args.output_dir, "maintenance_tickets"),
        TABLES["maintenance_tickets"].columns,
        ticket_rows(),
    )

    def outcome_rows() -> Iterable[dict[str, object]]:
        nonlocal outcome_count
        for current_date in operating_dates:
            for space in spaces:
                target = targets_by_space[space["space_id"]]
                rollup = daily_rollups[(str(space["space_id"]), current_date)]
                actual = round(rollup["util_sum"] / max(1.0, rollup["hours"]), 2)
                intended = float(target["intended_utilization_pct"])
                peak_vs_capacity = round((rollup["peak"] / max(1, int(space["seating_capacity"]))) * 100, 2)
                energy_vs_target = round(clamp(random.gauss(102 + (actual - intended) * 0.55, 17), 45, 210), 2)
                satisfaction = round(clamp(4.4 - abs(actual - intended) / 55 - max(0, peak_vs_capacity - 100) / 160 + random.uniform(-0.25, 0.25), 1.0, 5.0), 2)
                success = round(
                    clamp(
                        100
                        - abs(actual - intended) * 0.7
                        - max(0, energy_vs_target - 100) * 0.18
                        - max(0, peak_vs_capacity - 100) * 0.22
                        + (satisfaction - 3.5) * 8,
                        0,
                        100,
                    ),
                    2,
                )
                if actual < intended * 0.55 and energy_vs_target > 105:
                    flag = "Underused with HVAC waste"
                elif peak_vs_capacity > 115:
                    flag = "Over capacity"
                elif satisfaction < 3.25:
                    flag = "Experience risk"
                elif success >= 82:
                    flag = "Performing to intent"
                else:
                    flag = "Monitor"
                outcome_count += 1
                yield {
                    "outcome_id": make_id("OUT", outcome_count),
                    "space_id": space["space_id"],
                    "outcome_date": iso_date(current_date),
                    "actual_utilization_pct": actual,
                    "intended_utilization_pct": intended,
                    "peak_demand_vs_capacity_pct": peak_vs_capacity,
                    "energy_vs_target_pct": energy_vs_target,
                    "avg_satisfaction_score": satisfaction,
                    "design_success_score": success,
                    "outcome_flag": flag,
                }

    counts["design_outcome_daily"] = write_csv(
        table_file(args.output_dir, "design_outcome_daily"),
        TABLES["design_outcome_daily"].columns,
        outcome_rows(),
    )

    write_schema_sql(args.output_dir, args.sql_schema)
    write_bulk_insert_sql(args.output_dir)
    summarize_counts(args.output_dir, counts, args)

    print("Workspace performance SQL MI dataset generated.")
    print(f"Output directory: {args.output_dir.resolve()}")
    print(f"Simulation window: {operating_dates[0]} through {operating_dates[-1]}")
    print(f"Tables generated: {len(TABLE_LOAD_ORDER)}")
    for table_name in TABLE_LOAD_ORDER:
        print(f" - {table_name}: {counts[table_name]:,} rows")
    print("Import files: schema.sql, bulk_insert.sql, dataset_summary.json")

    if args.load_sql:
        load_csvs_to_sql(
            output_dir=args.output_dir,
            server=args.sql_server,
            database=args.sql_database,
            driver=args.sql_driver,
            schema_name=args.sql_schema,
            batch_size=args.batch_size,
        )
        print("SQL load completed.")


if __name__ == "__main__":
    main()
