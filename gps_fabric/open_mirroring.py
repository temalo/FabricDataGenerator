"""Shared utilities for Gensler GPS-inspired open mirroring datasets."""

from __future__ import annotations

import json
import random
import re
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker


fake = Faker()
Faker.seed(42)
random.seed(42)

DATE_START = date(2023, 1, 1)
DATE_END = date(2026, 4, 1)

INDICATOR_BY_REGION = {
    "United States": "TRACI 2.1",
    "Canada": "TRACI 2.1",
    "United Kingdom": "CML",
    "Germany": "CML",
    "France": "CML",
    "Netherlands": "CML",
}

CATEGORY_DEFINITIONS: Sequence[Mapping[str, object]] = (
    {
        "category_code": "PT",
        "category_name": "Paint",
        "functional_unit": "1 L applied",
        "requires_epd": True,
        "requires_iaq_test": True,
        "gwp_limit_standard": 2.8,
        "gwp_limit_market": 1.9,
    },
    {
        "category_code": "WC",
        "category_name": "Wall Covering",
        "functional_unit": "1 m2 installed",
        "requires_epd": True,
        "requires_iaq_test": True,
        "gwp_limit_standard": 6.3,
        "gwp_limit_market": 4.4,
    },
    {
        "category_code": "CT",
        "category_name": "Carpet Tile",
        "functional_unit": "1 m2 installed",
        "requires_epd": True,
        "requires_iaq_test": True,
        "gwp_limit_standard": 13.5,
        "gwp_limit_market": 9.1,
    },
    {
        "category_code": "RF",
        "category_name": "Resilient Flooring",
        "functional_unit": "1 m2 installed",
        "requires_epd": True,
        "requires_iaq_test": True,
        "gwp_limit_standard": 9.8,
        "gwp_limit_market": 7.0,
    },
    {
        "category_code": "AP",
        "category_name": "Acoustic Panel",
        "functional_unit": "1 m2 installed",
        "requires_epd": True,
        "requires_iaq_test": True,
        "gwp_limit_standard": 11.2,
        "gwp_limit_market": 8.0,
    },
    {
        "category_code": "SF",
        "category_name": "Systems Furniture",
        "functional_unit": "1 workstation",
        "requires_epd": True,
        "requires_iaq_test": True,
        "gwp_limit_standard": 145.0,
        "gwp_limit_market": 105.0,
    },
    {
        "category_code": "IN",
        "category_name": "Insulation",
        "functional_unit": "1 m2 R-13 equivalent",
        "requires_epd": True,
        "requires_iaq_test": False,
        "gwp_limit_standard": 5.9,
        "gwp_limit_market": 3.9,
    },
    {
        "category_code": "CTL",
        "category_name": "Ceiling Tile",
        "functional_unit": "1 m2 installed",
        "requires_epd": True,
        "requires_iaq_test": True,
        "gwp_limit_standard": 8.5,
        "gwp_limit_market": 6.2,
    },
)

CERTIFICATION_TYPES = (
    "Cradle to Cradle v4 Silver",
    "Cradle to Cradle v4 Gold",
    "BIFMA LEVEL 2",
    "BIFMA LEVEL 3",
    "Living Product Challenge",
    "Declare Red List Free",
    "FloorScore",
    "GREENGUARD Gold",
)

IAQ_METHODS = (
    "CDPH Standard Method v1.2-2017",
    "ANSI/BIFMA M7.1-2016",
    "EN 16516",
    "ISO 16000",
)

PROJECT_TYPES = (
    "Workplace",
    "Education",
    "Hospitality",
    "Retail",
    "Mixed Use",
    "Healthcare",
)

SUBMITTAL_STATUSES = (
    "Approved",
    "Approved as Noted",
    "Revise and Resubmit",
    "Rejected",
    "Pending",
)


@dataclass(frozen=True)
class TableSpec:
    name: str
    key_columns: Sequence[str]


TABLE_SPECS: Sequence[TableSpec] = (
    TableSpec("manufacturers", ("manufacturer_id",)),
    TableSpec("manufacturing_facilities", ("facility_id",)),
    TableSpec("product_categories", ("category_id",)),
    TableSpec("products", ("product_id",)),
    TableSpec("epds", ("epd_id",)),
    TableSpec("iaq_test_results", ("iaq_test_id",)),
    TableSpec("product_certifications", ("product_certification_id",)),
    TableSpec("projects", ("project_id",)),
    TableSpec("project_submittals", ("submittal_id",)),
)

TABLE_SPECS_BY_NAME = {spec.name: spec for spec in TABLE_SPECS}


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def daterange_days(start: date, end: date) -> int:
    return (end - start).days


def rand_date(start: date = DATE_START, end: date = DATE_END) -> date:
    return start + timedelta(days=random.randint(0, daterange_days(start, end)))


def rand_dt(start: date = DATE_START, end: date = DATE_END) -> datetime:
    d = rand_date(start, end)
    return datetime(
        d.year,
        d.month,
        d.day,
        random.randint(7, 18),
        random.randint(0, 59),
        random.randint(0, 59),
    )


def to_arrow_table(rows: Sequence[Mapping[str, object]]) -> pa.Table:
    if not rows:
        raise ValueError("Cannot write an empty table without schema.")
    return pa.Table.from_pylist(list(rows))


def write_parquet(rows: Sequence[Mapping[str, object]], path: Path) -> None:
    table = to_arrow_table(rows)
    ensure_dir(path.parent)
    pq.write_table(table, path)


def write_json(data: Mapping[str, object], path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def validate_table_keys(
    rows: Sequence[Mapping[str, object]],
    spec: TableSpec,
    allow_duplicates: bool = False,
) -> None:
    seen_keys = set()
    for index, row in enumerate(rows, start=1):
        key = tuple(row.get(column) for column in spec.key_columns)
        if any(value is None for value in key):
            raise ValueError(
                f"Table '{spec.name}' row {index} has a null primary key value for {spec.key_columns}."
            )
        if not allow_duplicates and key in seen_keys:
            raise ValueError(
                f"Table '{spec.name}' contains duplicate primary key value {key} for {spec.key_columns}."
            )
        seen_keys.add(key)


def validate_dataset_primary_keys(dataset: Mapping[str, Sequence[Mapping[str, object]]]) -> None:
    for spec in TABLE_SPECS:
        if spec.name not in dataset:
            raise ValueError(f"Dataset is missing required table '{spec.name}'.")
        validate_table_keys(dataset[spec.name], spec)


def validate_change_rows(changes: Mapping[str, Sequence[Mapping[str, object]]]) -> None:
    for spec in TABLE_SPECS:
        rows = changes.get(spec.name, [])
        for index, row in enumerate(rows, start=1):
            marker = row.get("__rowMarker__")
            if marker not in (0, 1, 2):
                raise ValueError(
                    f"Table '{spec.name}' change row {index} has invalid __rowMarker__ value {marker}."
                )
            key = tuple(row.get(column) for column in spec.key_columns)
            if any(value is None for value in key):
                raise ValueError(
                    f"Table '{spec.name}' change row {index} has a null primary key value for {spec.key_columns}."
                )


def make_id(prefix: str, number: int) -> str:
    return f"{prefix}{number:05d}"


def choose_indicator(country: str) -> str:
    return INDICATOR_BY_REGION.get(country, "CML")


def compliance_tier(product: Mapping[str, object], epd: Mapping[str, object], iaq: Mapping[str, object] | None) -> str:
    if not product["has_sustainability_action_plan"]:
        return "Non-Compliant"
    if not epd["is_third_party_verified"]:
        return "Non-Compliant"
    if float(epd["gwp_a1_a3_kgco2e"]) > float(product["gwp_limit_standard"]):
        return "Non-Compliant"
    if product["requires_iaq_test"] and (iaq is None or not iaq["passes_gensler_iaq"]):
        return "Non-Compliant"
    if (
        float(epd["gwp_a1_a3_kgco2e"]) <= float(product["gwp_limit_market"])
        and float(product["recycled_content_pct"]) >= 20.0
        and product["has_multi_attribute_certification"]
    ):
        return "Market Differentiator"
    return "Gensler Standard"


def build_initial_dataset(scale: float = 1.0) -> Dict[str, List[MutableMapping[str, object]]]:
    manufacturer_count = max(12, int(24 * scale))
    facility_count = max(24, int(54 * scale))
    product_count = max(80, int(220 * scale))
    project_count = max(25, int(70 * scale))

    categories: List[MutableMapping[str, object]] = []
    manufacturers: List[MutableMapping[str, object]] = []
    facilities: List[MutableMapping[str, object]] = []
    products: List[MutableMapping[str, object]] = []
    epds: List[MutableMapping[str, object]] = []
    iaq_results: List[MutableMapping[str, object]] = []
    certifications: List[MutableMapping[str, object]] = []
    projects: List[MutableMapping[str, object]] = []
    submittals: List[MutableMapping[str, object]] = []

    category_by_id: Dict[str, MutableMapping[str, object]] = {}
    manufacturers_by_id: Dict[str, MutableMapping[str, object]] = {}
    facilities_by_manufacturer: Dict[str, List[MutableMapping[str, object]]] = {}
    products_by_category: Dict[str, List[MutableMapping[str, object]]] = {}
    epd_by_product: Dict[str, MutableMapping[str, object]] = {}
    iaq_by_product: Dict[str, MutableMapping[str, object]] = {}

    for idx, definition in enumerate(CATEGORY_DEFINITIONS, start=1):
        category = {
            "category_id": make_id("CAT", idx),
            "category_code": definition["category_code"],
            "category_name": definition["category_name"],
            "functional_unit": definition["functional_unit"],
            "requires_epd": bool(definition["requires_epd"]),
            "requires_iaq_test": bool(definition["requires_iaq_test"]),
            "gwp_limit_standard": float(definition["gwp_limit_standard"]),
            "gwp_limit_market": float(definition["gwp_limit_market"]),
            "standard_reference": "Gensler GPS 2.0",
        }
        categories.append(category)
        category_by_id[category["category_id"]] = category
        products_by_category[category["category_id"]] = []

    countries = list(INDICATOR_BY_REGION.keys())
    for idx in range(1, manufacturer_count + 1):
        country = random.choice(countries)
        manufacturer = {
            "manufacturer_id": make_id("MFG", idx),
            "manufacturer_name": f"{fake.company()} Materials",
            "headquarters_country": country,
            "headquarters_city": fake.city(),
            "has_sustainability_action_plan": random.random() < 0.88,
            "publishes_esg_report": random.random() < 0.74,
            "supply_chain_transparency_score": round(random.uniform(58, 96), 1),
            "created_at": rand_dt(),
        }
        manufacturers.append(manufacturer)
        manufacturers_by_id[manufacturer["manufacturer_id"]] = manufacturer
        facilities_by_manufacturer[manufacturer["manufacturer_id"]] = []

    for idx in range(1, facility_count + 1):
        manufacturer = random.choice(manufacturers)
        country = random.choice(countries)
        facility = {
            "facility_id": make_id("FAC", idx),
            "manufacturer_id": manufacturer["manufacturer_id"],
            "facility_name": f"{fake.city()} Plant {random.randint(1, 4)}",
            "country": country,
            "region_indicator": choose_indicator(country),
            "final_assembly_site": random.random() < 0.9,
            "renewable_energy_pct": round(random.uniform(5, 98), 1),
            "created_at": rand_dt(),
        }
        facilities.append(facility)
        facilities_by_manufacturer[manufacturer["manufacturer_id"]].append(facility)

    for manufacturer_id, facility_rows in facilities_by_manufacturer.items():
        if not facility_rows:
            manufacturer = manufacturers_by_id[manufacturer_id]
            country = manufacturer["headquarters_country"]
            facility = {
                "facility_id": make_id("FAC", len(facilities) + 1),
                "manufacturer_id": manufacturer_id,
                "facility_name": f"{manufacturer['manufacturer_name']} Main Plant",
                "country": country,
                "region_indicator": choose_indicator(country),
                "final_assembly_site": True,
                "renewable_energy_pct": round(random.uniform(15, 90), 1),
                "created_at": rand_dt(),
            }
            facilities.append(facility)
            facilities_by_manufacturer[manufacturer_id].append(facility)

    for idx in range(1, product_count + 1):
        category = random.choice(categories)
        manufacturer = random.choice(manufacturers)
        facility = random.choice(facilities_by_manufacturer[manufacturer["manufacturer_id"]])
        recycled_content = round(random.uniform(0, 58), 1)
        has_multi_attribute = random.random() < 0.56
        product = {
            "product_id": make_id("PRD", idx),
            "manufacturer_id": manufacturer["manufacturer_id"],
            "facility_id": facility["facility_id"],
            "category_id": category["category_id"],
            "product_name": f"{fake.color_name()} {category['category_name']} {random.choice(['Series', 'Collection', 'Line'])} {random.randint(100, 999)}",
            "sku": f"{category['category_code']}-{random.randint(10000, 99999)}",
            "recycled_content_pct": recycled_content,
            "voc_content_gpl": round(random.uniform(0.0, 45.0), 2),
            "has_multi_attribute_certification": has_multi_attribute,
            "has_sustainability_action_plan": manufacturer["has_sustainability_action_plan"],
            "requires_iaq_test": category["requires_iaq_test"],
            "gwp_limit_standard": category["gwp_limit_standard"],
            "gwp_limit_market": category["gwp_limit_market"],
            "introduced_on": rand_date(date(2023, 1, 1), date(2025, 12, 31)),
            "active_flag": True,
        }
        products.append(product)
        products_by_category[category["category_id"]].append(product)

        epd_gwp = round(
            random.uniform(category["gwp_limit_market"] * 0.7, category["gwp_limit_standard"] * 1.25),
            2,
        )
        epd = {
            "epd_id": make_id("EPD", idx),
            "product_id": product["product_id"],
            "epd_program_operator": random.choice(("UL", "SCS Global", "EPD International", "NSF")),
            "functional_unit": category["functional_unit"],
            "gwp_a1_a3_kgco2e": epd_gwp,
            "indicator_set": facility["region_indicator"],
            "is_third_party_verified": random.random() < 0.94,
            "biogenic_carbon_included": random.random() < 0.15,
            "epd_issue_date": rand_date(date(2023, 1, 1), date(2025, 12, 31)),
            "epd_expiry_date": rand_date(date(2026, 1, 1), date(2030, 12, 31)),
        }
        epds.append(epd)
        epd_by_product[product["product_id"]] = epd

        if category["requires_iaq_test"]:
            tvoc = round(random.uniform(0.03, 6.4), 3)
            iaq = {
                "iaq_test_id": make_id("IAQ", len(iaq_results) + 1),
                "product_id": product["product_id"],
                "test_method": random.choice(IAQ_METHODS),
                "tvoc_after_14_days_mg_m3": tvoc,
                "formaldehyde_mg_m3": round(random.uniform(0.0, 0.06), 3),
                "private_office_modeled": random.random() < 0.93,
                "lab_iso_17025_accredited": random.random() < 0.97,
                "passes_gensler_iaq": tvoc <= 5.0,
                "tested_at": rand_date(date(2023, 1, 1), date(2026, 3, 1)),
            }
            iaq_results.append(iaq)
            iaq_by_product[product["product_id"]] = iaq

        cert_target = random.randint(0, 3 if has_multi_attribute else 2)
        for _ in range(cert_target):
            cert_name = random.choice(CERTIFICATION_TYPES)
            certifications.append(
                {
                    "product_certification_id": make_id("CRT", len(certifications) + 1),
                    "product_id": product["product_id"],
                    "certification_name": cert_name,
                    "issuing_body": cert_name.split()[0],
                    "certificate_number": fake.bothify(text="???-########"),
                    "valid_from": rand_date(date(2023, 1, 1), date(2025, 12, 31)),
                    "valid_to": rand_date(date(2026, 1, 1), date(2029, 12, 31)),
                    "counts_as_multi_attribute": cert_name.startswith(("Cradle", "BIFMA", "Living")),
                }
            )

    for idx in range(1, project_count + 1):
        project = {
            "project_id": make_id("PROJ", idx),
            "project_name": f"{fake.city()} {random.choice(PROJECT_TYPES)} Fitout {random.randint(1, 9)}",
            "project_type": random.choice(PROJECT_TYPES),
            "location_country": random.choice(countries),
            "leed_target": random.choice(("Certified", "Silver", "Gold", "Platinum", "None")),
            "well_target": random.choice(("Bronze", "Silver", "Gold", "Platinum", "None")),
            "created_at": rand_dt(date(2024, 1, 1), DATE_END),
        }
        projects.append(project)

        submittal_target = random.randint(4, 10)
        chosen_categories = random.sample(categories, k=min(submittal_target, len(categories)))
        for category in chosen_categories:
            candidate_product = random.choice(products_by_category[category["category_id"]])
            product_epd = epd_by_product[candidate_product["product_id"]]
            iaq = iaq_by_product.get(candidate_product["product_id"])
            status = random.choice(SUBMITTAL_STATUSES)
            compliance = compliance_tier(candidate_product, product_epd, iaq)
            if status == "Approved" and compliance == "Non-Compliant":
                status = "Revise and Resubmit"
            submittals.append(
                {
                    "submittal_id": make_id("SUB", len(submittals) + 1),
                    "project_id": project["project_id"],
                    "product_id": candidate_product["product_id"],
                    "submitted_on": rand_date(date(2024, 1, 1), DATE_END),
                    "review_status": status,
                    "compliance_tier": compliance,
                    "carbon_review_notes": random.choice(
                        (
                            "EPD verified and benchmark checked",
                            "Awaiting lower-GWP alternate",
                            "Product accepted with client note",
                            "Optimization plan requested",
                            "IAQ test package reviewed",
                        )
                    ),
                    "iaq_review_required": bool(candidate_product["requires_iaq_test"]),
                }
            )

    return {
        "manufacturers": manufacturers,
        "manufacturing_facilities": facilities,
        "product_categories": categories,
        "products": products,
        "epds": epds,
        "iaq_test_results": iaq_results,
        "product_certifications": certifications,
        "projects": projects,
        "project_submittals": submittals,
    }


def build_initial_open_mirroring_layout(
    dataset: Mapping[str, Sequence[Mapping[str, object]]],
    output_dir: Path,
) -> None:
    validate_dataset_primary_keys(dataset)
    ensure_clean_dir(output_dir)
    write_json(
        {
            "partnerName": "FabricDataGenerator",
            "sourceInfo": {
                "sourceType": "Synthetic GPS Dataset",
                "sourceVersion": "1.0",
                "additionalInformation": {
                    "datasetTheme": "carbon-and-indoor-air-quality",
                    "standardReference": "Gensler GPS 2.0 inspired",
                },
            },
        },
        output_dir / "_partnerEvents.json",
    )
    for spec in TABLE_SPECS:
        table_dir = output_dir / spec.name
        ensure_dir(table_dir)
        write_json({"keyColumns": list(spec.key_columns)}, table_dir / "_metadata.json")
        rows = dataset[spec.name]
        write_parquet(rows, table_dir / "00000000000000000001.parquet")


def write_snapshot(dataset: Mapping[str, Sequence[Mapping[str, object]]], snapshot_dir: Path) -> None:
    validate_dataset_primary_keys(dataset)
    ensure_clean_dir(snapshot_dir)
    for table_name, rows in dataset.items():
        write_parquet(rows, snapshot_dir / f"{table_name}.parquet")


def read_snapshot(snapshot_dir: Path) -> Dict[str, List[MutableMapping[str, object]]]:
    if not snapshot_dir.exists():
        raise FileNotFoundError(
            f"Snapshot directory not found: {snapshot_dir}. Run the initial dataset generator first."
        )
    dataset: Dict[str, List[MutableMapping[str, object]]] = {}
    for spec in TABLE_SPECS:
        table_path = snapshot_dir / f"{spec.name}.parquet"
        if not table_path.exists():
            raise FileNotFoundError(f"Missing snapshot table: {table_path}")
        table = pq.read_table(table_path)
        dataset[spec.name] = [dict(row) for row in table.to_pylist()]
    return dataset


def next_identifier(rows: Sequence[Mapping[str, object]], prefix: str, field: str) -> int:
    max_value = 0
    for row in rows:
        match = re.search(r"(\d+)$", str(row[field]))
        if match:
            max_value = max(max_value, int(match.group(1)))
    return max_value + 1


def _index_rows(rows: Sequence[MutableMapping[str, object]], key_field: str) -> Dict[str, MutableMapping[str, object]]:
    return {str(row[key_field]): row for row in rows}


def _last_sequence_number(table_dir: Path) -> int:
    max_number = 0
    if not table_dir.exists():
        return max_number
    for path in table_dir.glob("*.parquet"):
        if path.stem.isdigit():
            max_number = max(max_number, int(path.stem))
    return max_number


def append_row_marker(rows: Iterable[Mapping[str, object]], marker: int) -> List[Dict[str, object]]:
    output: List[Dict[str, object]] = []
    for row in rows:
        new_row = dict(row)
        new_row["__rowMarker__"] = marker
        output.append(new_row)
    return output


def write_incremental_table_file(
    output_dir: Path,
    table_name: str,
    rows: Sequence[Mapping[str, object]],
    prior_dirs: Sequence[Path] = (),
) -> Path | None:
    if not rows:
        return None
    table_dir = output_dir / table_name
    ensure_dir(table_dir)
    existing_max = _last_sequence_number(table_dir)
    for prior_dir in prior_dirs:
        existing_max = max(existing_max, _last_sequence_number(prior_dir / table_name))
    next_number = existing_max + 1
    path = table_dir / f"{next_number:020d}.parquet"
    write_parquet(rows, path)
    metadata_path = table_dir / "_metadata.json"
    if not metadata_path.exists():
        spec = TABLE_SPECS_BY_NAME[table_name]
        write_json({"keyColumns": list(spec.key_columns)}, metadata_path)
    return path


def mutate_dataset(
    dataset: Dict[str, List[MutableMapping[str, object]]],
    scale: float = 1.0,
) -> Dict[str, List[Dict[str, object]]]:
    changes: Dict[str, List[Dict[str, object]]] = {spec.name: [] for spec in TABLE_SPECS}

    manufacturer_rows = dataset["manufacturers"]
    facility_rows = dataset["manufacturing_facilities"]
    product_rows = dataset["products"]
    epd_rows = dataset["epds"]
    iaq_rows = dataset["iaq_test_results"]
    cert_rows = dataset["product_certifications"]
    project_rows = dataset["projects"]
    submittal_rows = dataset["project_submittals"]
    category_rows = dataset["product_categories"]

    manufacturers_by_id = _index_rows(manufacturer_rows, "manufacturer_id")
    facilities_by_id = _index_rows(facility_rows, "facility_id")
    products_by_id = _index_rows(product_rows, "product_id")
    epds_by_product = {row["product_id"]: row for row in epd_rows}
    iaq_by_product = {row["product_id"]: row for row in iaq_rows}

    new_manufacturer_count = max(2, int(5 * scale))
    next_mfg = next_identifier(manufacturer_rows, "MFG", "manufacturer_id")
    next_fac = next_identifier(facility_rows, "FAC", "facility_id")
    next_product = next_identifier(product_rows, "PRD", "product_id")
    next_epd = next_identifier(epd_rows, "EPD", "epd_id")
    next_iaq = next_identifier(iaq_rows, "IAQ", "iaq_test_id")
    next_cert = next_identifier(cert_rows, "CRT", "product_certification_id")
    next_project = next_identifier(project_rows, "PROJ", "project_id")
    next_sub = next_identifier(submittal_rows, "SUB", "submittal_id")

    for _ in range(new_manufacturer_count):
        country = random.choice(list(INDICATOR_BY_REGION.keys()))
        manufacturer = {
            "manufacturer_id": make_id("MFG", next_mfg),
            "manufacturer_name": f"{fake.company()} Sustainable Surfaces",
            "headquarters_country": country,
            "headquarters_city": fake.city(),
            "has_sustainability_action_plan": True,
            "publishes_esg_report": True,
            "supply_chain_transparency_score": round(random.uniform(72, 98), 1),
            "created_at": rand_dt(date(2025, 1, 1), DATE_END),
        }
        next_mfg += 1
        manufacturer_rows.append(manufacturer)
        manufacturers_by_id[manufacturer["manufacturer_id"]] = manufacturer
        changes["manufacturers"].extend(append_row_marker([manufacturer], 0))

        facility = {
            "facility_id": make_id("FAC", next_fac),
            "manufacturer_id": manufacturer["manufacturer_id"],
            "facility_name": f"{fake.city()} Expansion Plant",
            "country": country,
            "region_indicator": choose_indicator(country),
            "final_assembly_site": True,
            "renewable_energy_pct": round(random.uniform(35, 100), 1),
            "created_at": rand_dt(date(2025, 1, 1), DATE_END),
        }
        next_fac += 1
        facility_rows.append(facility)
        facilities_by_id[facility["facility_id"]] = facility
        changes["manufacturing_facilities"].extend(append_row_marker([facility], 0))

        category = random.choice(category_rows)
        product = {
            "product_id": make_id("PRD", next_product),
            "manufacturer_id": manufacturer["manufacturer_id"],
            "facility_id": facility["facility_id"],
            "category_id": category["category_id"],
            "product_name": f"{fake.word().title()} {category['category_name']} Carbon Lite",
            "sku": f"{category['category_code']}-{random.randint(10000, 99999)}",
            "recycled_content_pct": round(random.uniform(18, 62), 1),
            "voc_content_gpl": round(random.uniform(0.0, 18.0), 2),
            "has_multi_attribute_certification": True,
            "has_sustainability_action_plan": True,
            "requires_iaq_test": category["requires_iaq_test"],
            "gwp_limit_standard": category["gwp_limit_standard"],
            "gwp_limit_market": category["gwp_limit_market"],
            "introduced_on": rand_date(date(2025, 1, 1), DATE_END),
            "active_flag": True,
        }
        next_product += 1
        product_rows.append(product)
        products_by_id[product["product_id"]] = product
        changes["products"].extend(append_row_marker([product], 0))

        epd = {
            "epd_id": make_id("EPD", next_epd),
            "product_id": product["product_id"],
            "epd_program_operator": "UL",
            "functional_unit": category["functional_unit"],
            "gwp_a1_a3_kgco2e": round(random.uniform(category["gwp_limit_market"] * 0.7, category["gwp_limit_market"] * 0.98), 2),
            "indicator_set": facility["region_indicator"],
            "is_third_party_verified": True,
            "biogenic_carbon_included": random.random() < 0.25,
            "epd_issue_date": rand_date(date(2025, 1, 1), DATE_END),
            "epd_expiry_date": rand_date(date(2028, 1, 1), date(2031, 12, 31)),
        }
        next_epd += 1
        epd_rows.append(epd)
        epds_by_product[product["product_id"]] = epd
        changes["epds"].extend(append_row_marker([epd], 0))

        if product["requires_iaq_test"]:
            iaq = {
                "iaq_test_id": make_id("IAQ", next_iaq),
                "product_id": product["product_id"],
                "test_method": "CDPH Standard Method v1.2-2017",
                "tvoc_after_14_days_mg_m3": round(random.uniform(0.03, 0.45), 3),
                "formaldehyde_mg_m3": round(random.uniform(0.0, 0.01), 3),
                "private_office_modeled": True,
                "lab_iso_17025_accredited": True,
                "passes_gensler_iaq": True,
                "tested_at": rand_date(date(2025, 1, 1), DATE_END),
            }
            next_iaq += 1
            iaq_rows.append(iaq)
            iaq_by_product[product["product_id"]] = iaq
            changes["iaq_test_results"].extend(append_row_marker([iaq], 0))

        cert = {
            "product_certification_id": make_id("CRT", next_cert),
            "product_id": product["product_id"],
            "certification_name": "Cradle to Cradle v4 Silver",
            "issuing_body": "Cradle",
            "certificate_number": fake.bothify(text="CCC-########"),
            "valid_from": rand_date(date(2025, 1, 1), DATE_END),
            "valid_to": rand_date(date(2028, 1, 1), date(2031, 12, 31)),
            "counts_as_multi_attribute": True,
        }
        next_cert += 1
        cert_rows.append(cert)
        changes["product_certifications"].extend(append_row_marker([cert], 0))

    update_target_count = max(6, int(15 * scale))
    update_targets = random.sample(product_rows, k=min(update_target_count, len(product_rows)))
    for product in update_targets:
        epd = epds_by_product[product["product_id"]]
        epd["gwp_a1_a3_kgco2e"] = round(max(0.1, float(epd["gwp_a1_a3_kgco2e"]) * random.uniform(0.82, 1.08)), 2)
        epd["epd_issue_date"] = rand_date(date(2025, 1, 1), DATE_END)
        changes["epds"].extend(append_row_marker([epd], 1))

        if product["product_id"] in iaq_by_product and random.random() < 0.55:
            iaq = iaq_by_product[product["product_id"]]
            iaq["tvoc_after_14_days_mg_m3"] = round(max(0.01, float(iaq["tvoc_after_14_days_mg_m3"]) * random.uniform(0.65, 1.15)), 3)
            iaq["passes_gensler_iaq"] = float(iaq["tvoc_after_14_days_mg_m3"]) <= 5.0
            iaq["tested_at"] = rand_date(date(2025, 1, 1), DATE_END)
            changes["iaq_test_results"].extend(append_row_marker([iaq], 1))

    for manufacturer in random.sample(manufacturer_rows, k=min(4, len(manufacturer_rows))):
        manufacturer["supply_chain_transparency_score"] = round(
            min(99.0, float(manufacturer["supply_chain_transparency_score"]) + random.uniform(1.2, 6.0)),
            1,
        )
        changes["manufacturers"].extend(append_row_marker([manufacturer], 1))

    delete_candidates = random.sample(cert_rows, k=min(5, len(cert_rows)))
    remaining_cert_rows = []
    for cert in cert_rows:
        if cert in delete_candidates:
            delete_row = {
                "product_certification_id": cert["product_certification_id"],
                "product_id": cert["product_id"],
                "certification_name": None,
                "issuing_body": None,
                "certificate_number": None,
                "valid_from": None,
                "valid_to": None,
                "counts_as_multi_attribute": None,
            }
            changes["product_certifications"].extend(append_row_marker([delete_row], 2))
        else:
            remaining_cert_rows.append(cert)
    dataset["product_certifications"] = remaining_cert_rows

    for _ in range(max(1, int(3 * scale))):
        project = {
            "project_id": make_id("PROJ", next_project),
            "project_name": f"{fake.city()} Workplace Renovation {random.randint(10, 99)}",
            "project_type": random.choice(PROJECT_TYPES),
            "location_country": random.choice(list(INDICATOR_BY_REGION.keys())),
            "leed_target": random.choice(("Certified", "Silver", "Gold", "Platinum")),
            "well_target": random.choice(("Silver", "Gold", "Platinum")),
            "created_at": rand_dt(date(2025, 1, 1), DATE_END),
        }
        next_project += 1
        project_rows.append(project)
        changes["projects"].extend(append_row_marker([project], 0))

        for _sub in range(random.randint(3, 6)):
            product = random.choice(product_rows)
            epd = epds_by_product[product["product_id"]]
            iaq = iaq_by_product.get(product["product_id"])
            submittal = {
                "submittal_id": make_id("SUB", next_sub),
                "project_id": project["project_id"],
                "product_id": product["product_id"],
                "submitted_on": rand_date(date(2025, 1, 1), DATE_END),
                "review_status": random.choice(SUBMITTAL_STATUSES),
                "compliance_tier": compliance_tier(product, epd, iaq),
                "carbon_review_notes": random.choice(
                    (
                        "Alternate reviewed against current EPD",
                        "Accepted for pilot project",
                        "Client requested lower-TVOC option",
                        "Pending updated certification packet",
                    )
                ),
                "iaq_review_required": bool(product["requires_iaq_test"]),
            }
            next_sub += 1
            submittal_rows.append(submittal)
            changes["project_submittals"].extend(append_row_marker([submittal], 0))

    review_targets = random.sample(submittal_rows, k=min(12, len(submittal_rows)))
    for submittal in review_targets:
        product = products_by_id[submittal["product_id"]]
        epd = epds_by_product[product["product_id"]]
        iaq = iaq_by_product.get(product["product_id"])
        submittal["review_status"] = random.choice(("Approved", "Approved as Noted", "Revise and Resubmit"))
        submittal["compliance_tier"] = compliance_tier(product, epd, iaq)
        changes["project_submittals"].extend(append_row_marker([submittal], 1))

    return changes


def write_incremental_changes(
    changes: Mapping[str, Sequence[Mapping[str, object]]],
    output_dir: Path,
    prior_dirs: Sequence[Path] = (),
) -> List[Path]:
    validate_change_rows(changes)
    ensure_dir(output_dir)
    written: List[Path] = []
    for spec in TABLE_SPECS:
        path = write_incremental_table_file(
            output_dir,
            spec.name,
            changes.get(spec.name, []),
            prior_dirs=prior_dirs,
        )
        if path is not None:
            written.append(path)
    return written


def parse_onelake_landing_zone(url: str) -> tuple[str, str, str, str]:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc not in (
        "onelake.dfs.fabric.microsoft.com",
        "onelake.blob.fabric.microsoft.com",
    ):
        raise ValueError(
            "Landing zone URL must use https://onelake.dfs.fabric.microsoft.com/... "
            "or https://onelake.blob.fabric.microsoft.com/..."
        )
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 4:
        raise ValueError("Landing zone URL does not contain workspace GUID, item GUID, and Files path.")
    workspace_id, item_id = parts[0], parts[1]
    relative_path = "/".join(parts[2:])
    return parsed.netloc, workspace_id, item_id, relative_path
