#!/usr/bin/env python3
"""Generate ABM (American Building Maintenance) synthetic dataset for Microsoft Fabric demo.

Produces two dataset packages:
  <output>/us/  — US enterprise services gold star schema (non-PII, shortcut source)
  <output>/uk/  — UK aviation operations gold star schema (GDPR-resident PII)

silver_employee_reference in the US output is the intended OneLake shortcut
target for UK Fabric: pseudonymous worker IDs + role/job_family, no names.

Usage:
  python generate_dataset.py
  python generate_dataset.py --days 90 --scale 0.5
  python generate_dataset.py --output-dir /custom/path
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from faker import Faker

_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = _REPO_ROOT / "output" / "abm"
DEFAULT_SEED = 20240401
DEFAULT_DAYS = 90
_END_DATE = date(2025, 12, 31)

fake_uk = Faker("en_GB")
fake_us = Faker("en_US")
rng = random.Random(DEFAULT_SEED)

# ─────────────────────────────────────────────────────────────────────────────
# Static reference data
# ─────────────────────────────────────────────────────────────────────────────

UK_AIRPORTS = [
    {"iata": "LHR", "icao": "EGLL", "name": "London Heathrow Airport",  "city": "London",      "operator": "Heathrow Airport Ltd"},
    {"iata": "LGW", "icao": "EGKK", "name": "London Gatwick Airport",   "city": "London",      "operator": "Gatwick Airport Ltd"},
    {"iata": "MAN", "icao": "EGCC", "name": "Manchester Airport",        "city": "Manchester",  "operator": "Manchester Airports Group"},
    {"iata": "BHX", "icao": "EGBB", "name": "Birmingham Airport",        "city": "Birmingham",  "operator": "Birmingham Airport Ltd"},
    {"iata": "EDI", "icao": "EGPH", "name": "Edinburgh Airport",          "city": "Edinburgh",   "operator": "Global Infrastructure Partners"},
    {"iata": "GLA", "icao": "EGPF", "name": "Glasgow Airport",            "city": "Glasgow",     "operator": "AGS Airports"},
    {"iata": "LBA", "icao": "EGNM", "name": "Leeds Bradford Airport",     "city": "Leeds",       "operator": "Bridgepoint"},
    {"iata": "STN", "icao": "EGSS", "name": "London Stansted Airport",    "city": "London",      "operator": "Manchester Airports Group"},
]

AIRPORT_TERMINALS: dict[str, list[str]] = {
    "LHR": ["Terminal 2", "Terminal 3", "Terminal 4", "Terminal 5"],
    "LGW": ["North Terminal", "South Terminal"],
    "MAN": ["Terminal 1", "Terminal 2", "Terminal 3"],
    "BHX": ["Terminal 1", "Terminal 2"],
    "EDI": ["Main Terminal"],
    "GLA": ["Main Terminal"],
    "LBA": ["Main Terminal"],
    "STN": ["Main Terminal"],
}

ZONE_TEMPLATES = [
    ("Arrivals Hall",        "arrivals"),
    ("Departures Hall",      "departures"),
    ("Check-in Hall",        "departures"),
    ("Security Checkpoint",  "gate"),
    ("Gate Area A",          "gate"),
    ("Gate Area B",          "gate"),
    ("Baggage Reclaim",      "baggage"),
    ("Lounge",               "lounge"),
    ("Curbside Drop-off",    "curbside"),
    ("Restrooms - Airside",  "departures"),
    ("Restrooms - Landside", "arrivals"),
]

SHIFT_TYPES = [
    {"name": "Early", "start_time": "05:30", "end_time": "13:30", "is_overnight": False},
    {"name": "Late",  "start_time": "13:30", "end_time": "21:30", "is_overnight": False},
    {"name": "Night", "start_time": "21:30", "end_time": "05:30", "is_overnight": True},
]

TASK_TYPES = [
    {"name": "PRM Wheelchair Assist",    "category": "mobility",    "sla_minutes": 15,   "requires_equipment": True},
    {"name": "Aisle Chair Assist",        "category": "mobility",    "sla_minutes": 20,   "requires_equipment": True},
    {"name": "Passenger Escort",          "category": "mobility",    "sla_minutes": 10,   "requires_equipment": False},
    {"name": "Buggy Transfer",            "category": "mobility",    "sla_minutes": 12,   "requires_equipment": True},
    {"name": "Terminal Clean - Routine",  "category": "cleaning",    "sla_minutes": 30,   "requires_equipment": True},
    {"name": "Gate Clean - Turnaround",   "category": "cleaning",    "sla_minutes": 25,   "requires_equipment": True},
    {"name": "Restroom Clean",            "category": "cleaning",    "sla_minutes": 15,   "requires_equipment": False},
    {"name": "Deep Clean",                "category": "cleaning",    "sla_minutes": 120,  "requires_equipment": True},
    {"name": "Spillage Response",         "category": "cleaning",    "sla_minutes": 10,   "requires_equipment": False},
    {"name": "Supervisor On-call",        "category": "staffing",    "sla_minutes": None, "requires_equipment": False},
    {"name": "Dispatch Coordination",     "category": "staffing",    "sla_minutes": None, "requires_equipment": False},
    {"name": "Safety Inspection",         "category": "compliance",  "sla_minutes": 60,   "requires_equipment": False},
]

ROSTER_SOURCES = [
    {"name": "FindMyShift",  "ingestion_method": "API"},
    {"name": "Oracle HCM",   "ingestion_method": "SSIS"},
    {"name": "TMGHCM",       "ingestion_method": "FileDrop"},
    {"name": "Manual Entry", "ingestion_method": "Manual"},
]

EQUIPMENT_TYPES = [
    ("Wheelchair",    "leased"),
    ("Aisle Chair",   "leased"),
    ("Electric Buggy","leased"),
    ("Floor Scrubber","owned"),
    ("Platform Lift", "owned"),
    ("Cleaning Cart", "owned"),
    ("High-Reach Mop","owned"),
]

UK_JOB_ROLES: list[tuple[str, str, str]] = [
    ("PRM Officer",        "mobility",    "Front-line"),
    ("Cleaning Operative", "cleaning",    "Front-line"),
    ("Team Leader",        "operations",  "Supervisory"),
    ("Dispatcher",         "operations",  "Supervisory"),
    ("Shift Supervisor",   "operations",  "Supervisory"),
    ("Trainer",            "operations",  "Support"),
    ("Admin Coordinator",  "operations",  "Support"),
]

# Weighted so most UK employees are front-line
UK_ROLE_WEIGHTS = [30, 30, 10, 8, 8, 7, 7]

US_SERVICE_LINES = [
    {"name": "Janitorial & Facility Services", "category": "Operations", "is_regulated": False},
    {"name": "Aviation Services",               "category": "Operations", "is_regulated": True},
    {"name": "Parking & Transportation",        "category": "Operations", "is_regulated": False},
    {"name": "Facilities Engineering",           "category": "Compliance","is_regulated": True},
    {"name": "Security Services",                "category": "Compliance","is_regulated": True},
    {"name": "Landscape Services",               "category": "Operations", "is_regulated": False},
]

US_SKUS: dict[str, list[dict]] = {
    "Janitorial & Facility Services": [
        {"name": "Daily Office Clean",      "uom": "hour",     "cost": 28.00, "bill": 45.00},
        {"name": "Floor Strip & Wax",       "uom": "sq_meter", "cost": 0.80,  "bill": 1.40},
        {"name": "Window Cleaning",          "uom": "sq_meter", "cost": 0.60,  "bill": 1.10},
        {"name": "Post-Construction Clean",  "uom": "job",      "cost": 800.0, "bill": 1400.0},
        {"name": "Restroom Maintenance",     "uom": "hour",     "cost": 26.00, "bill": 42.00},
        {"name": "Carpet Clean",             "uom": "sq_meter", "cost": 0.50,  "bill": 0.90},
    ],
    "Aviation Services": [
        {"name": "PRM Assist - Wheelchair",  "uom": "task",     "cost": 18.00, "bill": 32.00},
        {"name": "PRM Assist - Aisle Chair", "uom": "task",     "cost": 22.00, "bill": 38.00},
        {"name": "Terminal Clean - Band",    "uom": "hour",     "cost": 26.00, "bill": 44.00},
        {"name": "Gate Turnaround Clean",    "uom": "task",     "cost": 35.00, "bill": 58.00},
        {"name": "Deep Clean - Aircraft",    "uom": "job",      "cost": 600.0, "bill": 950.0},
    ],
    "Parking & Transportation": [
        {"name": "Parking Attendant",        "uom": "hour",     "cost": 22.00, "bill": 36.00},
        {"name": "Shuttle Driver",           "uom": "hour",     "cost": 24.00, "bill": 40.00},
        {"name": "Valet Service",            "uom": "task",     "cost": 15.00, "bill": 28.00},
    ],
    "Facilities Engineering": [
        {"name": "HVAC Preventive Maint.",   "uom": "hour",     "cost": 65.00, "bill": 110.00},
        {"name": "Electrical Inspection",    "uom": "hour",     "cost": 72.00, "bill": 120.00},
        {"name": "Plumbing Repair",          "uom": "hour",     "cost": 68.00, "bill": 115.00},
        {"name": "Energy Audit",             "uom": "job",      "cost": 2000.0,"bill": 3200.0},
    ],
    "Security Services": [
        {"name": "Security Officer",         "uom": "hour",     "cost": 30.00, "bill": 50.00},
        {"name": "Mobile Patrol",            "uom": "hour",     "cost": 35.00, "bill": 58.00},
        {"name": "Access Control Tech",      "uom": "hour",     "cost": 42.00, "bill": 70.00},
    ],
    "Landscape Services": [
        {"name": "Grounds Maintenance",      "uom": "hour",     "cost": 24.00, "bill": 40.00},
        {"name": "Lawn Care",                "uom": "sq_meter", "cost": 0.10,  "bill": 0.18},
        {"name": "Snow Removal",             "uom": "hour",     "cost": 38.00, "bill": 62.00},
    ],
}

US_JOB_FAMILIES: dict[str, list[str]] = {
    "Janitorial & Facility Services": ["Custodian", "Floor Care Tech", "Lead Cleaner", "Supervisor"],
    "Aviation Services":              ["PRM Officer", "Dispatcher", "Aviation Team Lead", "Cleaning Operative"],
    "Parking & Transportation":       ["Parking Attendant", "Shuttle Driver", "Supervisor"],
    "Facilities Engineering":         ["HVAC Tech", "Electrician", "Plumber", "Engineer Supervisor"],
    "Security Services":              ["Security Officer", "Lead Officer", "Security Supervisor"],
    "Landscape Services":             ["Groundskeeper", "Irrigation Tech", "Landscape Supervisor"],
}

US_CUSTOMERS = [
    ("NorthWest Medical Center",    "Healthcare",           "Midwest"),
    ("Pacific Coast Tech Partners", "Technology",           "West"),
    ("Federal Reserve Bank Group",  "Financial Services",   "East"),
    ("City of Chicago DoA",         "Government",           "Midwest"),
    ("Skyport International",       "Aviation",             "South"),
    ("State University Consortium", "Higher Education",     "East"),
    ("Meridian Retail Holdings",    "Retail",               "West"),
    ("BioNorth Life Sciences",      "Life Sciences",        "Northeast"),
    ("Hartwell Legal Group",        "Legal",                "East"),
    ("Cascade Media Studios",       "Media",                "West"),
    ("Great Plains Health System",  "Healthcare",           "Midwest"),
    ("Coastal Financial Corp.",     "Financial Services",   "Southeast"),
]

US_SITE_NAMES: list[tuple[str, str]] = [
    ("Corporate HQ - Building A",    "office"),
    ("Corporate HQ - Building B",    "office"),
    ("Regional Operations Center",   "office"),
    ("Downtown Medical Center",      "hospital"),
    ("Northside Outpatient Clinic",  "hospital"),
    ("Campus Research Facility",     "campus"),
    ("Terminal A Concourse",         "airport"),
    ("Terminal B Concourse",         "airport"),
    ("Main Administration Block",    "government"),
    ("City Hall Annex",              "government"),
    ("West Retail Distribution Hub", "industrial"),
    ("Manufacturing Plant East",     "industrial"),
]

US_STATES = [
    "CA","TX","NY","FL","IL","PA","OH","GA","NC","MI",
    "WA","AZ","MA","TN","CO","MN","WI","MO","IN","OR",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def uid() -> str:
    return str(uuid.uuid4())


def date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def coin(p: float = 0.5) -> bool:
    return rng.random() < p


def wc(items: list, weights: list) -> Any:
    return rng.choices(items, weights=weights, k=1)[0]


def iso_dt(d: date, hour: int, minute: int = 0) -> str:
    return datetime(d.year, d.month, d.day, hour, minute).isoformat(sep="T", timespec="seconds")


def add_minutes(d: date, hour: int, minute: int, offset_min: int) -> str:
    dt = datetime(d.year, d.month, d.day, hour, minute) + timedelta(minutes=offset_min)
    return dt.isoformat(sep="T", timespec="seconds")


def parse_hhmm(t: str) -> tuple[int, int]:
    h, m = t.split(":")
    return int(h), int(m)


def flight_bank_weight(hour: int) -> float:
    """Mobility request demand peaks at morning and afternoon flight banks."""
    if 6 <= hour <= 9:
        return 3.2
    if 10 <= hour <= 13:
        return 1.0
    if 14 <= hour <= 18:
        return 2.8
    if 19 <= hour <= 21:
        return 1.3
    return 0.25


def cleaning_weight(hour: int) -> float:
    """Cleaning jobs cluster at low-passenger-flow periods."""
    if 0 <= hour <= 4:
        return 3.0
    if 5 <= hour <= 6:
        return 1.8
    if 23 == hour:
        return 2.2
    return 0.4


def date_range(start: date, end: date) -> list[date]:
    days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(days)]


def sla_breach(task_sla_min: int | None, actual_min: int | None, understaffed: bool) -> bool:
    if task_sla_min is None or actual_min is None:
        return False
    base = actual_min > task_sla_min
    # Extra breach probability on understaffed days
    return base or (understaffed and coin(0.12))


# ─────────────────────────────────────────────────────────────────────────────
# US dimension generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_dim_date(start: date, end: date) -> list[dict]:
    rows = []
    for d in date_range(start, end):
        rows.append({
            "date_key":        date_key(d),
            "date":            d.isoformat(),
            "week_start_date": (d - timedelta(days=d.weekday())).isoformat(),
            "month":           d.month,
            "month_name":      d.strftime("%B"),
            "quarter":         f"Q{(d.month - 1) // 3 + 1}",
            "year":            d.year,
            "is_weekend":      d.weekday() >= 5,
        })
    return rows


def gen_dim_customer(n: int) -> list[dict]:
    rows = []
    for i in range(min(n, len(US_CUSTOMERS))):
        name, industry, region = US_CUSTOMERS[i]
        rows.append({
            "customer_key":        uid(),
            "customer_name":       name,
            "customer_industry":   industry,
            "customer_region":     region,
            "parent_customer_key": "",
        })
    return rows


def gen_dim_site(customers: list[dict], n: int) -> list[dict]:
    rows = []
    for i in range(n):
        customer = rng.choice(customers)
        template = US_SITE_NAMES[i % len(US_SITE_NAMES)]
        site_name, site_type = template
        state = rng.choice(US_STATES)
        city = fake_us.city()
        rows.append({
            "site_key":       uid(),
            "customer_key":   customer["customer_key"],
            "site_name":      f"{site_name} ({city})",
            "site_type":      site_type,
            "address_city":   city,
            "address_state":  state,
            "address_country":"US",
            "geo_lat":        round(rng.uniform(25.0, 48.0), 6),
            "geo_long":       round(rng.uniform(-122.0, -71.0), 6),
            "open_date":      (date(2015, 1, 1) + timedelta(days=rng.randint(0, 2000))).isoformat(),
            "close_date":     "",
        })
    return rows


def gen_dim_service_line() -> list[dict]:
    rows = []
    for sl in US_SERVICE_LINES:
        rows.append({
            "service_line_key":  uid(),
            "service_line_name": sl["name"],
            "service_category":  sl["category"],
            "is_regulated":      sl["is_regulated"],
        })
    return rows


def gen_dim_service_catalog(service_lines: list[dict]) -> list[dict]:
    rows = []
    sl_by_name = {sl["service_line_name"]: sl["service_line_key"] for sl in service_lines}
    for sl_name, skus in US_SKUS.items():
        sl_key = sl_by_name[sl_name]
        for sku in skus:
            rows.append({
                "service_sku_key":    uid(),
                "service_line_key":   sl_key,
                "service_sku_name":   sku["name"],
                "unit_of_measure":    sku["uom"],
                "standard_cost_rate": sku["cost"],
                "standard_bill_rate": sku["bill"],
                "active_flag":        True,
            })
    return rows


def gen_dim_contract(customers: list[dict], service_lines: list[dict],
                     dates: list[date], n: int) -> list[dict]:
    rows = []
    pricing = ["fixed", "time-and-materials", "outcome-based"]
    tiers   = ["Bronze", "Silver", "Gold", "Platinum"]
    start = dates[0]
    end   = dates[-1]
    for i in range(n):
        customer = rng.choice(customers)
        c_start = start - timedelta(days=rng.randint(180, 730))
        c_end   = end + timedelta(days=rng.randint(30, 540))
        rows.append({
            "contract_key":            uid(),
            "customer_key":            customer["customer_key"],
            "contract_number":         f"C-{20000 + i:05d}",
            "contract_start_date_key": date_key(c_start),
            "contract_end_date_key":   date_key(c_end),
            "pricing_model":           rng.choice(pricing),
            "sla_tier":                wc(tiers, [25, 35, 28, 12]),
            "currency_code":           "USD",
        })
    return rows


def gen_dim_employee_us(n: int, service_lines: list[dict]) -> list[dict]:
    rows = []
    statuses = ["active", "active", "active", "leave", "terminated"]
    for _ in range(n):
        sl = rng.choice(service_lines)
        job_family = rng.choice(US_JOB_FAMILIES[sl["service_line_name"]])
        home_state = rng.choice(US_STATES)
        rows.append({
            "employee_key":         uid(),
            "worker_id":            uid(),      # pseudonymous internal id (= global_employee_ref)
            "home_country":         "US",
            "home_state":           home_state,
            "job_family":           job_family,
            "job_role":             job_family,
            "union_flag":           coin(0.35),
            "manager_employee_key": "",         # filled in after
            "employment_status":    rng.choice(statuses),
        })
    # Assign ~10% as managers
    active = [e for e in rows if e["employment_status"] == "active"]
    managers = rng.sample(active, k=max(1, len(active) // 10))
    manager_keys = [m["employee_key"] for m in managers]
    for emp in rows:
        if emp["employment_status"] == "active" and emp not in managers:
            emp["manager_employee_key"] = rng.choice(manager_keys)
    return rows


def gen_dim_asset(sites: list[dict], n: int) -> list[dict]:
    asset_types = [
        "floor scrubber", "ride-on sweeper", "aerial lift",
        "pressure washer", "cart", "vehicle", "vacuum unit",
    ]
    rows = []
    for _ in range(n):
        site = rng.choice(sites)
        in_svc = date(2018, 1, 1) + timedelta(days=rng.randint(0, 2000))
        retired = "" if coin(0.85) else (in_svc + timedelta(days=rng.randint(365, 1460))).isoformat()
        rows.append({
            "asset_key":          uid(),
            "site_key":           site["site_key"],
            "asset_type":         rng.choice(asset_types),
            "manufacturer":       rng.choice(["Tennant", "Nilfisk", "Kärcher", "Clarke", "Factory Cat"]),
            "model":              f"Model-{rng.randint(100,999)}",
            "in_service_date_key": date_key(in_svc),
            "retired_date_key":   date_key(date.fromisoformat(retired)) if retired else "",
            "ownership_type":     rng.choice(["owned", "leased"]),
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# US silver table generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_silver_employee_reference(us_employees: list[dict],
                                   uk_employees: list[dict]) -> list[dict]:
    """Non-PII workforce reference spanning both US and UK — designed for shortcutting."""
    rows = []
    for emp in us_employees:
        rows.append({
            "global_employee_ref": emp["worker_id"],
            "home_country":        "US",
            "job_family":          emp["job_family"],
            "job_role":            emp["job_role"],
            "employment_status":   emp["employment_status"],
        })
    for emp in uk_employees:
        rows.append({
            "global_employee_ref": emp["global_employee_ref"],
            "home_country":        "GB",
            "job_family":          emp["skill_group"],
            "job_role":            emp["job_role"],
            "employment_status":   emp["employment_status"],
        })
    return rows


def gen_silver_customer_site_map(customers: list[dict],
                                  sites: list[dict]) -> list[dict]:
    site_by_customer: dict[str, list[dict]] = {}
    for site in sites:
        site_by_customer.setdefault(site["customer_key"], []).append(site)
    rows = []
    cust_by_key = {c["customer_key"]: c for c in customers}
    for site in sites:
        c = cust_by_key[site["customer_key"]]
        rows.append({
            "site_key":           site["site_key"],
            "customer_key":       c["customer_key"],
            "customer_name":      c["customer_name"],
            "customer_industry":  c["customer_industry"],
            "site_name":          site["site_name"],
            "site_type":          site["site_type"],
            "address_city":       site["address_city"],
            "address_state":      site["address_state"],
            "address_country":    site["address_country"],
        })
    return rows


def gen_silver_service_catalog(service_catalog: list[dict],
                                service_lines: list[dict]) -> list[dict]:
    sl_by_key = {sl["service_line_key"]: sl for sl in service_lines}
    rows = []
    for sku in service_catalog:
        sl = sl_by_key[sku["service_line_key"]]
        rows.append({
            "service_sku_key":    sku["service_sku_key"],
            "service_sku_name":   sku["service_sku_name"],
            "service_line_name":  sl["service_line_name"],
            "service_category":   sl["service_category"],
            "unit_of_measure":    sku["unit_of_measure"],
            "standard_cost_rate": sku["standard_cost_rate"],
            "standard_bill_rate": sku["standard_bill_rate"],
            "active_flag":        sku["active_flag"],
        })
    return rows


def gen_silver_contract_terms(contracts: list[dict],
                               customers: list[dict]) -> list[dict]:
    cust_by_key = {c["customer_key"]: c for c in customers}
    rows = []
    for c in contracts:
        cust = cust_by_key[c["customer_key"]]
        rows.append({
            "contract_key":     c["contract_key"],
            "contract_number":  c["contract_number"],
            "customer_name":    cust["customer_name"],
            "pricing_model":    c["pricing_model"],
            "sla_tier":         c["sla_tier"],
            "currency_code":    c["currency_code"],
            "contract_start":   str(c["contract_start_date_key"]),
            "contract_end":     str(c["contract_end_date_key"]),
        })
    return rows


def gen_silver_kpi_benchmarks(service_lines: list[dict]) -> list[dict]:
    site_types = ["office", "hospital", "airport", "campus", "government", "industrial"]
    rows = []
    for sl in service_lines:
        for st in site_types:
            rows.append({
                "benchmark_key":          uid(),
                "service_line_key":       sl["service_line_key"],
                "service_line_name":      sl["service_line_name"],
                "site_type":              st,
                "kpi_labor_util_pct":     round(rng.uniform(70.0, 92.0), 1),
                "kpi_sla_compliance_pct": round(rng.uniform(88.0, 99.0), 1),
                "kpi_cost_per_hour_usd":  round(rng.uniform(25.0, 75.0), 2),
                "kpi_incident_rate":      round(rng.uniform(0.5, 4.5), 2),
                "benchmark_year":         2025,
            })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# US fact generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_fact_work_order(sites: list[dict], contracts: list[dict],
                         service_catalog: list[dict], dates: list[date],
                         scale: float) -> list[dict]:
    rows = []
    priorities = ["low", "normal", "normal", "high", "critical"]
    p_weights  = [10, 50, 50, 25, 5]
    statuses   = ["open", "in_progress", "closed", "canceled"]
    s_weights  = [5, 10, 80, 5]
    req_types  = ["customer", "internal", "system"]
    # ~1 work order per site every 2 days, scaled
    orders_per_day = max(1, int(len(sites) * 0.5 * scale))
    for d in dates:
        for _ in range(rng.randint(max(1, orders_per_day - 2), orders_per_day + 2)):
            site = rng.choice(sites)
            sku  = rng.choice(service_catalog)
            contract = rng.choice(contracts) if coin(0.8) else None
            status = wc(statuses, s_weights)
            planned = rng.randint(30, 480)
            actual  = "" if status in ("open", "canceled") else rng.randint(
                max(10, planned - 60), planned + 90
            )
            opened_dk = date_key(d)
            closed_dk = "" if status != "closed" else date_key(
                d + timedelta(days=rng.randint(0, 5))
            )
            rows.append({
                "work_order_key":    uid(),
                "date_opened_key":   opened_dk,
                "date_closed_key":   closed_dk,
                "site_key":          site["site_key"],
                "contract_key":      contract["contract_key"] if contract else "",
                "service_sku_key":   sku["service_sku_key"],
                "priority":          wc(priorities, p_weights),
                "status":            status,
                "requested_by_type": rng.choice(req_types),
                "planned_minutes":   planned,
                "actual_minutes":    actual,
                "labor_cost":        round(float(actual) * rng.uniform(0.40, 0.65), 2) if actual else "",
                "material_cost":     round(rng.uniform(0, 150), 2) if coin(0.3) else "",
            })
    return rows


def gen_fact_labor_time(employees: list[dict], sites: list[dict],
                         service_lines: list[dict], dates: list[date],
                         scale: float) -> list[dict]:
    rows = []
    active = [e for e in employees if e["employment_status"] == "active"]
    # Each active employee logs ~4 days per week on average
    for d in dates:
        is_weekend = d.weekday() >= 5
        sample_size = max(1, int(len(active) * (0.30 if is_weekend else 0.75) * scale))
        for emp in rng.sample(active, k=min(sample_size, len(active))):
            site = rng.choice(sites)
            sl   = rng.choice(service_lines)
            hours_reg = round(rng.uniform(6.5, 8.5), 2)
            hours_ot  = round(rng.uniform(0, 3.0), 2) if coin(0.2) else 0.0
            pay_rate  = rng.uniform(20.0, 60.0)
            rows.append({
                "labor_time_key":    uid(),
                "date_key":          date_key(d),
                "employee_key":      emp["employee_key"],
                "site_key":          site["site_key"],
                "service_line_key":  sl["service_line_key"],
                "hours_regular":     hours_reg,
                "hours_overtime":    hours_ot,
                "pay_cost":          round((hours_reg + hours_ot * 1.5) * pay_rate, 2),
                "billable_flag":     coin(0.85),
            })
    return rows


def gen_fact_billing_invoice_line(contracts: list[dict], sites: list[dict],
                                   service_catalog: list[dict], dates: list[date],
                                   scale: float) -> list[dict]:
    rows = []
    # One invoice run per contract per week roughly
    invoice_dates = [d for d in dates if d.weekday() == 4]  # Fridays
    for inv_date in invoice_dates:
        for contract in contracts:
            n_lines = max(1, int(rng.randint(1, 5) * scale))
            for _ in range(n_lines):
                site = rng.choice(sites)
                sku  = rng.choice(service_catalog)
                qty  = round(rng.uniform(1.0, 200.0), 2)
                unit_price = sku["standard_bill_rate"] * rng.uniform(0.9, 1.1)
                rows.append({
                    "invoice_line_key": uid(),
                    "invoice_date_key": date_key(inv_date),
                    "contract_key":     contract["contract_key"],
                    "site_key":         site["site_key"],
                    "service_sku_key":  sku["service_sku_key"],
                    "quantity":         qty,
                    "unit_price":       round(unit_price, 4),
                    "line_amount":      round(qty * unit_price, 2),
                    "currency_code":    "USD",
                })
    return rows


def gen_fact_safety_incident(sites: list[dict], service_lines: list[dict],
                              dates: list[date], scale: float) -> list[dict]:
    rows = []
    inc_types  = ["slip_trip_fall", "equipment_failure", "chemical", "near_miss", "violence"]
    inc_weights= [35, 25, 15, 20, 5]
    severities = ["minor", "major", "critical", "near_miss"]
    sev_weights= [50, 30, 5, 15]
    # ~2 incidents per week across all sites
    prob_per_day = min(0.8, scale * 0.35)
    for d in dates:
        if coin(prob_per_day):
            n = rng.randint(1, 3)
            for _ in range(n):
                site = rng.choice(sites)
                sl   = rng.choice(service_lines)
                severity = wc(severities, sev_weights)
                lost_time = severity in ("major", "critical") and coin(0.4)
                rows.append({
                    "incident_key":              uid(),
                    "incident_date_key":         date_key(d),
                    "site_key":                  site["site_key"],
                    "service_line_key":          sl["service_line_key"],
                    "severity":                  severity,
                    "incident_type":             wc(inc_types, inc_weights),
                    "lost_time_flag":            lost_time,
                    "days_lost":                 rng.randint(1, 15) if lost_time else "",
                    "reported_to_customer_flag": coin(0.7),
                })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# UK dimension generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_dim_airport_uk(us_sites: list[dict]) -> list[dict]:
    """Map LHR, LGW, MAN to airport-type US sites; rest get empty site_key_us."""
    airport_sites = [s for s in us_sites if s["site_type"] == "airport"]
    rows = []
    for i, ap in enumerate(UK_AIRPORTS):
        site_key_us = airport_sites[i]["site_key"] if i < len(airport_sites) else ""
        rows.append({
            "airport_key":   uid(),
            "iata_code":     ap["iata"],
            "icao_code":     ap["icao"],
            "airport_name":  ap["name"],
            "city":          ap["city"],
            "country":       "GB",
            "operator_name": ap["operator"],
            "timezone":      "Europe/London",
            "site_key_us":   site_key_us,
        })
    return rows


def gen_dim_terminal(airports: list[dict]) -> list[dict]:
    rows = []
    ap_by_key = {ap["airport_key"]: ap for ap in airports}
    for ap in airports:
        iata = ap["iata_code"]
        terminals = AIRPORT_TERMINALS.get(iata, ["Main Terminal"])
        gate_counts = {"LHR": 80, "LGW": 50, "MAN": 60, "BHX": 30,
                       "EDI": 25, "GLA": 20, "LBA": 15, "STN": 40}
        for term in terminals:
            n_gates = gate_counts.get(iata, 20) // len(terminals)
            rows.append({
                "terminal_key": uid(),
                "airport_key":  ap["airport_key"],
                "terminal_name": term,
                "gate_count":   n_gates + rng.randint(-3, 3),
            })
    return rows


def gen_dim_service_zone(terminals: list[dict]) -> list[dict]:
    rows = []
    for term in terminals:
        # Vary the zones per terminal slightly
        n_zones = rng.randint(7, len(ZONE_TEMPLATES))
        for zone_name, zone_type in ZONE_TEMPLATES[:n_zones]:
            rows.append({
                "zone_key":      uid(),
                "terminal_key":  term["terminal_key"],
                "zone_type":     zone_type,
                "zone_name":     zone_name,
            })
    return rows


def gen_dim_shift_type() -> list[dict]:
    rows = []
    for st in SHIFT_TYPES:
        rows.append({
            "shift_type_key":  uid(),
            "shift_name":      st["name"],
            "start_time_local": st["start_time"],
            "end_time_local":   st["end_time"],
            "is_overnight":    st["is_overnight"],
        })
    return rows


def gen_dim_task_type(us_service_lines: list[dict]) -> list[dict]:
    aviation_sl = next((sl for sl in us_service_lines
                        if sl["service_line_name"] == "Aviation Services"), None)
    rows = []
    for tt in TASK_TYPES:
        rows.append({
            "task_type_key":        uid(),
            "service_line_key_us":  aviation_sl["service_line_key"] if aviation_sl else "",
            "task_name":            tt["name"],
            "task_category":        tt["category"],
            "sla_minutes_target":   tt["sla_minutes"] if tt["sla_minutes"] is not None else "",
            "requires_equipment_flag": tt["requires_equipment"],
        })
    return rows


def gen_dim_equipment_uk(airports: list[dict], n_per_airport: int) -> list[dict]:
    rows = []
    for ap in airports:
        for i in range(n_per_airport):
            eq_type, ownership = rng.choice(EQUIPMENT_TYPES)
            in_svc = date(2018, 1, 1) + timedelta(days=rng.randint(0, 2000))
            status = wc(["available", "in_use", "maintenance", "retired"],
                        [55, 25, 15, 5])
            rows.append({
                "equipment_key":    uid(),
                "airport_key":      ap["airport_key"],
                "equipment_type":   eq_type,
                "asset_tag":        f"{ap['iata_code']}-{1000 + i:04d}",
                "ownership_type":   ownership,
                "in_service_date":  in_svc.isoformat(),
                "status":           status,
            })
    return rows


def gen_dim_employee_uk(airports: list[dict], n: int) -> list[dict]:
    """PII-resident UK employees — stays in UK Fabric capacity."""
    statuses = ["active", "active", "active", "active", "leave", "terminated"]
    rows = []
    for _ in range(n):
        ap = rng.choice(airports)
        role, skill, grade = wc(UK_JOB_ROLES, UK_ROLE_WEIGHTS)
        status = rng.choice(statuses)
        first = fake_uk.first_name()
        last  = fake_uk.last_name()
        rows.append({
            "employee_key":       uid(),
            "worker_id":          f"UK{rng.randint(10000, 99999)}",
            "first_name":         first,
            "last_name":          last,
            "work_email":         f"{first.lower()}.{last.lower()}@abm-aviation.co.uk",
            "job_role":           role,
            "skill_group":        skill,
            "employment_status":  status,
            "gdpr_consent_flag":  coin(0.97),
            "global_employee_ref": uid(),   # pseudonymous token linking to silver_employee_reference
            "airport_key":        ap["airport_key"],
        })
    return rows


def gen_dim_roster_source() -> list[dict]:
    rows = []
    for rs in ROSTER_SOURCES:
        rows.append({
            "roster_source_key": uid(),
            "source_name":       rs["name"],
            "ingestion_method":  rs["ingestion_method"],
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# UK fact generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_fact_shift_assignment(employees: list[dict], airports: list[dict],
                               terminals: list[dict], shift_types: list[dict],
                               roster_sources: list[dict], dates: list[date],
                               scale: float) -> list[dict]:
    rows = []
    active = [e for e in employees if e["employment_status"] == "active"]
    term_by_airport: dict[str, list[dict]] = {}
    for t in terminals:
        term_by_airport.setdefault(t["airport_key"], []).append(t)

    # ~85% of active employees scheduled on any weekday, ~40% weekends
    for d in dates:
        is_weekend = d.weekday() >= 5
        # Simulate Monday sick spikes
        sick_spike = d.weekday() == 0 and coin(0.15)
        utilisation = 0.35 if is_weekend else (0.70 if sick_spike else 0.85)
        n_sample = max(1, int(len(active) * utilisation * scale))
        scheduled = rng.sample(active, k=min(n_sample, len(active)))
        for emp in scheduled:
            ap_key = emp["airport_key"]
            apt_terms = term_by_airport.get(ap_key, [])
            term = rng.choice(apt_terms) if apt_terms else None
            shift = rng.choice(shift_types)
            source = rng.choice(roster_sources)
            sh, sm = parse_hhmm(shift["start_time_local"])
            eh, em = parse_hhmm(shift["end_time_local"])
            absent = coin(0.04)   # 4% no-show / absence
            actual_start = "" if absent else iso_dt(d, sh + rng.randint(-5, 15) // 60,
                                                        (sm + rng.randint(-5, 15)) % 60)
            planned_min = 480  # 8-hour shifts
            actual_min  = "" if absent else rng.randint(440, 510)
            rows.append({
                "shift_assignment_key": uid(),
                "date_key":             date_key(d),
                "airport_key":          ap_key,
                "terminal_key":         term["terminal_key"] if term else "",
                "employee_key":         emp["employee_key"],
                "shift_type_key":       shift["shift_type_key"],
                "roster_source_key":    source["roster_source_key"],
                "planned_start_utc":    iso_dt(d, sh, sm),
                "planned_end_utc":      iso_dt(d, eh, em),
                "actual_start_utc":     actual_start,
                "actual_end_utc":       "" if absent else add_minutes(d, sh, sm, actual_min),
                "planned_minutes":      planned_min,
                "actual_minutes":       actual_min if not absent else "",
                "absence_reason":       rng.choice(["sick", "annual_leave", "no_show",
                                                     "emergency_leave"]) if absent else "",
            })
    return rows


def gen_fact_task_execution(airports: list[dict], zones: list[dict],
                             task_types: list[dict], employees: list[dict],
                             dates: list[date], scale: float) -> list[dict]:
    rows = []
    active = [e for e in employees if e["employment_status"] == "active"]
    zones_by_airport: dict[str, list[dict]] = {}
    for z in zones:
        # We need terminal_key → airport_key mapping; we'll just use all zones
        pass
    # Build zone lookup: zone_key → airport_key via terminal lookup is complex;
    # simplify by keying zones by terminal_key and building airport→zones from dim_terminal
    # zones list has terminal_key; we join via airports→terminals→zones lazily
    all_zones = zones  # use all zones; airport filtering not critical for demo
    dispatch = ["radio", "app", "phone", "supervisor"]
    d_weights = [30, 40, 20, 10]

    tasks_per_airport_per_day = max(2, int(18 * scale))
    for d in dates:
        for ap in airports:
            ap_emps = [e for e in active if e["airport_key"] == ap["airport_key"]]
            if not ap_emps:
                continue
            # LHR gets more volume
            multiplier = 2.5 if ap["iata_code"] == "LHR" else (1.5 if ap["iata_code"] in ("LGW", "MAN") else 1.0)
            n = max(1, int(tasks_per_airport_per_day * multiplier))
            for _ in range(n):
                tt = rng.choice(task_types)
                zone = rng.choice(all_zones)
                emp = rng.choice(ap_emps)
                hour = rng.choices(range(24), weights=[flight_bank_weight(h) for h in range(24)])[0]
                start = iso_dt(d, hour, rng.randint(0, 59))
                sla = tt["sla_minutes_target"]
                actual_dur = "" if coin(0.05) else rng.randint(
                    max(5, int(float(sla) * 0.6)) if sla else 5,
                    int(float(sla) * 1.6) if sla else 90
                )
                understaffed = coin(0.15)
                breached = sla_breach(
                    int(sla) if sla else None,
                    int(actual_dur) if actual_dur else None,
                    understaffed,
                )
                rows.append({
                    "task_execution_key":   uid(),
                    "date_key":             date_key(d),
                    "airport_key":          ap["airport_key"],
                    "zone_key":             zone["zone_key"],
                    "task_type_key":        tt["task_type_key"],
                    "assigned_employee_key": emp["employee_key"],
                    "dispatch_channel":     wc(dispatch, d_weights),
                    "requested_time_utc":   start,
                    "start_time_utc":       add_minutes(d, hour, rng.randint(0, 59), rng.randint(0, 8)),
                    "end_time_utc":         add_minutes(d, hour, rng.randint(0, 59), int(actual_dur)) if actual_dur else "",
                    "duration_minutes":     actual_dur,
                    "sla_breached_flag":    breached,
                    "quality_score":        round(rng.uniform(3.0, 5.0), 2) if coin(0.6) else "",
                    "notes":                "",
                })
    return rows


def gen_fact_passenger_mobility_request(airports: list[dict], terminals: list[dict],
                                         zones: list[dict], task_types: list[dict],
                                         employees: list[dict], dates: list[date],
                                         scale: float) -> list[dict]:
    rows = []
    active = [e for e in employees if e["employment_status"] == "active"]
    mobility_types = [tt for tt in task_types if tt["task_category"] == "mobility"]
    term_by_airport: dict[str, list[dict]] = {}
    for t in terminals:
        term_by_airport.setdefault(t["airport_key"], []).append(t)

    req_sources = ["airline", "airport", "walk-up"]
    req_types   = ["wheelchair", "aisle-chair", "escort", "buggy"]
    outcomes    = ["completed", "canceled", "no_show"]
    out_weights = [88, 8, 4]

    # LHR ~80/day, LGW/MAN ~40/day, others ~15/day
    daily_volumes = {"LHR": 80, "LGW": 40, "MAN": 40, "BHX": 20,
                     "EDI": 15, "GLA": 15, "LBA": 10, "STN": 25}
    for d in dates:
        for ap in airports:
            vol = max(1, int(daily_volumes.get(ap["iata_code"], 15) * scale))
            ap_emps = [e for e in active if e["airport_key"] == ap["airport_key"]]
            ap_terms = term_by_airport.get(ap["airport_key"], [])
            for _ in range(vol):
                hour = rng.choices(range(24), weights=[flight_bank_weight(h) for h in range(24)])[0]
                req_time = iso_dt(d, hour, rng.randint(0, 59))
                wait     = rng.randint(1, 25)
                outcome  = wc(outcomes, out_weights)
                req_type = rng.choice(req_types)
                tt = rng.choice(mobility_types) if mobility_types else None
                rows.append({
                    "mobility_request_key":  uid(),
                    "date_key":              date_key(d),
                    "airport_key":           ap["airport_key"],
                    "terminal_key":          rng.choice(ap_terms)["terminal_key"] if ap_terms else "",
                    "request_source":        rng.choice(req_sources),
                    "request_type":          req_type,
                    "requested_time_utc":    req_time,
                    "pickup_zone_key":       rng.choice(zones)["zone_key"],
                    "dropoff_zone_key":      rng.choice(zones)["zone_key"],
                    "assigned_employee_key": rng.choice(ap_emps)["employee_key"] if ap_emps else "",
                    "start_time_utc":        add_minutes(d, hour, rng.randint(0, 59), wait),
                    "end_time_utc":          add_minutes(d, hour, rng.randint(0, 59), wait + rng.randint(5, 40)) if outcome == "completed" else "",
                    "wait_minutes":          wait,
                    "outcome":               outcome,
                    "passenger_token":       uid(),    # pseudonymous — no names stored
                })
    return rows


def gen_fact_cleaning_job(airports: list[dict], zones: list[dict],
                           task_types: list[dict], employees: list[dict],
                           equipment: list[dict], dates: list[date],
                           scale: float) -> list[dict]:
    rows = []
    active = [e for e in employees if e["employment_status"] == "active"]
    cleaning_types_list = ["terminal", "gate", "restroom", "deep_clean", "spillage"]
    ctype_weights       = [30, 25, 25, 10, 10]
    avail_equip = [eq for eq in equipment if eq["status"] in ("available", "in_use")]

    daily_vol = {"LHR": 120, "LGW": 70, "MAN": 80, "BHX": 50,
                 "EDI": 40,  "GLA": 35, "LBA": 25, "STN": 60}
    for d in dates:
        for ap in airports:
            vol = max(1, int(daily_vol.get(ap["iata_code"], 40) * scale))
            ap_emps  = [e for e in active if e["airport_key"] == ap["airport_key"]]
            ap_equip = [eq for eq in avail_equip if eq["airport_key"] == ap["airport_key"]]
            for _ in range(vol):
                hour = rng.choices(range(24), weights=[cleaning_weight(h) for h in range(24)])[0]
                ctype = wc(cleaning_types_list, ctype_weights)
                duration = {
                    "terminal":  rng.randint(45, 90),
                    "gate":      rng.randint(20, 35),
                    "restroom":  rng.randint(10, 20),
                    "deep_clean":rng.randint(90, 180),
                    "spillage":  rng.randint(5, 20),
                }[ctype]
                uses_equip = ctype in ("terminal", "gate", "deep_clean")
                rows.append({
                    "cleaning_job_key":        uid(),
                    "date_key":                date_key(d),
                    "airport_key":             ap["airport_key"],
                    "zone_key":                rng.choice(zones)["zone_key"],
                    "cleaning_type":           ctype,
                    "square_meters":           round(rng.uniform(20, 800), 1) if ctype != "spillage" else "",
                    "assigned_employee_key":   rng.choice(ap_emps)["employee_key"] if ap_emps else "",
                    "equipment_key":           rng.choice(ap_equip)["equipment_key"] if ap_equip and uses_equip else "",
                    "start_time_utc":          iso_dt(d, hour, rng.randint(0, 59)),
                    "end_time_utc":            add_minutes(d, hour, rng.randint(0, 59), duration),
                    "chemicals_used_flag":     ctype in ("restroom", "deep_clean"),
                    "inspection_required_flag":ctype == "deep_clean",
                })
    return rows


def gen_fact_ops_incident(airports: list[dict], service_lines: list[dict],
                           dates: list[date], scale: float) -> list[dict]:
    rows = []
    categories = ["safety", "staffing", "equipment", "service_delay"]
    cat_weights = [20, 25, 30, 25]
    severities  = ["low", "medium", "high", "critical"]
    sev_weights = [45, 35, 15, 5]
    prob = min(0.9, scale * 0.65)
    for d in dates:
        for ap in airports:
            if coin(prob):
                n = rng.randint(1, 3)
                for _ in range(n):
                    cat = wc(categories, cat_weights)
                    sev = wc(severities, sev_weights)
                    rows.append({
                        "ops_incident_key":            uid(),
                        "date_key":                    date_key(d),
                        "airport_key":                 ap["airport_key"],
                        "incident_category":           cat,
                        "severity":                    sev,
                        "related_task_execution_key":  "",
                        "impacted_service_line_key_us": rng.choice(service_lines)["service_line_key"],
                        "minutes_impact":              rng.randint(5, 120) if sev in ("high", "critical") else rng.randint(0, 30),
                        "notes":                       "",
                    })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# UK bronze staging generators (intentionally messy)
# ─────────────────────────────────────────────────────────────────────────────

def _typo(s: str) -> str:
    """Introduce a random typo in ~30% of calls."""
    if not s or not coin(0.30):
        return s
    i = rng.randint(0, len(s) - 1)
    return s[:i] + rng.choice("aeiou") + s[i + 1:]


def gen_bronze_findmyshift_roster(shift_assignments: list[dict],
                                   employees: list[dict]) -> list[dict]:
    """Dirty version of shift assignments: duplicates, typos, missing fields."""
    emp_by_key = {e["employee_key"]: e for e in employees}
    rows = []
    for sa in shift_assignments:
        emp = emp_by_key.get(sa["employee_key"], {})
        first = _typo(emp.get("first_name", ""))
        last  = _typo(emp.get("last_name", ""))
        rows.append({
            "raw_id":          f"FMS-{rng.randint(100000, 999999)}",
            "employee_worker_id": emp.get("worker_id", ""),
            "employee_name":   f"{first} {last}",
            "shift_date":      str(sa["date_key"]),
            "shift_name":      "",          # often blank in raw export
            "start_time":      sa["planned_start_utc"],
            "end_time":        sa["planned_end_utc"],
            "location":        _typo(emp.get("airport_key", "")[:8]),  # truncated/typo
            "absence_flag":    "Y" if sa.get("absence_reason") else "",
            "source_file":     f"FMS_export_{sa['date_key']}.csv",
        })
        # Introduce ~8% duplicate rows (same shift imported twice)
        if coin(0.08):
            rows.append(rows[-1].copy())
    return rows


def gen_bronze_oracle_finance_extract(employees: list[dict],
                                       dates: list[date]) -> list[dict]:
    """Monthly cost-centre extract — one row per employee per month."""
    rows = []
    months_seen: set = set()
    for d in dates:
        month_key = (d.year, d.month)
        if month_key in months_seen:
            continue
        months_seen.add(month_key)
        for emp in employees:
            if emp["employment_status"] == "terminated":
                continue
            rows.append({
                "extract_month":  f"{d.year}-{d.month:02d}",
                "worker_id":      emp["worker_id"],
                "cost_centre":    f"CC-{rng.randint(1000, 9999)}",
                "job_code":       f"JC{rng.randint(100, 999)}",
                "grade":          rng.choice(["G1", "G2", "G3", "G4", "G5"]),
                "base_salary_gbp":round(rng.uniform(21000, 55000) / 12, 2),
                "source_system":  "Oracle Fusion",
                "extract_date":   d.isoformat(),
            })
    return rows


def gen_bronze_tmghcm_worker_extract(employees: list[dict]) -> list[dict]:
    """Worker status/hierarchy extract from TMGHCM — one row per employee."""
    rows = []
    active = [e for e in employees if e["employment_status"] == "active"]
    supervisors = rng.sample(active, k=max(1, len(active) // 8))
    sup_ids = [s["worker_id"] for s in supervisors]
    for emp in employees:
        rows.append({
            "hcm_worker_id":     emp["worker_id"],
            "first_name":        emp["first_name"],
            "last_name":         emp["last_name"],
            "email":             emp.get("work_email", ""),
            "job_role":          emp["job_role"],
            "department":        emp["skill_group"],
            "employment_status": emp["employment_status"],
            "supervisor_id":     rng.choice(sup_ids) if emp["employment_status"] == "active" and emp not in supervisors else "",
            "hire_date":         (date(2018, 1, 1) + timedelta(days=rng.randint(0, 2500))).isoformat(),
            "source_system":     "TMGHCM",
            "extract_timestamp": datetime.now().isoformat(timespec="seconds"),
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Schema SQL
# ─────────────────────────────────────────────────────────────────────────────

US_SCHEMA_SQL = """-- ABM US Enterprise Services Schema
-- Generated by abm/generate_dataset.py
-- Schema: abm_us

CREATE SCHEMA IF NOT EXISTS abm_us;

CREATE TABLE abm_us.dim_date (
    date_key         INT           NOT NULL PRIMARY KEY,
    date             DATE          NOT NULL,
    week_start_date  DATE          NOT NULL,
    month            TINYINT       NOT NULL,
    month_name       VARCHAR(12)   NOT NULL,
    quarter          VARCHAR(2)    NOT NULL,
    year             SMALLINT      NOT NULL,
    is_weekend       BIT           NOT NULL
);

CREATE TABLE abm_us.dim_customer (
    customer_key         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    customer_name        NVARCHAR(200)    NOT NULL,
    customer_industry    NVARCHAR(100)    NOT NULL,
    customer_region      NVARCHAR(50)     NOT NULL,
    parent_customer_key  UNIQUEIDENTIFIER NULL
);

CREATE TABLE abm_us.dim_site (
    site_key         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    customer_key     UNIQUEIDENTIFIER NOT NULL,
    site_name        NVARCHAR(200)    NOT NULL,
    site_type        NVARCHAR(50)     NOT NULL,
    address_city     NVARCHAR(100)    NOT NULL,
    address_state    CHAR(2)          NOT NULL,
    address_country  CHAR(2)          NOT NULL,
    geo_lat          DECIMAL(9,6)     NULL,
    geo_long         DECIMAL(9,6)     NULL,
    open_date        DATE             NULL,
    close_date       DATE             NULL
);

CREATE TABLE abm_us.dim_service_line (
    service_line_key  UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    service_line_name NVARCHAR(100)    NOT NULL,
    service_category  NVARCHAR(50)     NOT NULL,
    is_regulated      BIT              NOT NULL
);

CREATE TABLE abm_us.dim_service_catalog (
    service_sku_key     UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    service_line_key    UNIQUEIDENTIFIER NOT NULL,
    service_sku_name    NVARCHAR(200)    NOT NULL,
    unit_of_measure     NVARCHAR(20)     NOT NULL,
    standard_cost_rate  DECIMAL(18,4)    NULL,
    standard_bill_rate  DECIMAL(18,4)    NULL,
    active_flag         BIT              NOT NULL
);

CREATE TABLE abm_us.dim_contract (
    contract_key              UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    customer_key              UNIQUEIDENTIFIER NOT NULL,
    contract_number           NVARCHAR(20)     NOT NULL,
    contract_start_date_key   INT              NOT NULL,
    contract_end_date_key     INT              NOT NULL,
    pricing_model             NVARCHAR(30)     NOT NULL,
    sla_tier                  NVARCHAR(20)     NOT NULL,
    currency_code             CHAR(3)          NOT NULL
);

CREATE TABLE abm_us.dim_employee_us (
    employee_key         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    worker_id            UNIQUEIDENTIFIER NOT NULL,  -- = global_employee_ref
    home_country         CHAR(2)          NOT NULL,
    home_state           CHAR(2)          NULL,
    job_family           NVARCHAR(100)    NOT NULL,
    job_role             NVARCHAR(100)    NOT NULL,
    union_flag           BIT              NOT NULL,
    manager_employee_key UNIQUEIDENTIFIER NULL,
    employment_status    NVARCHAR(20)     NOT NULL
);

CREATE TABLE abm_us.dim_asset (
    asset_key           UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    site_key            UNIQUEIDENTIFIER NOT NULL,
    asset_type          NVARCHAR(100)    NOT NULL,
    manufacturer        NVARCHAR(100)    NULL,
    model               NVARCHAR(100)    NULL,
    in_service_date_key INT              NULL,
    retired_date_key    INT              NULL,
    ownership_type      NVARCHAR(20)     NOT NULL
);

CREATE TABLE abm_us.silver_employee_reference (
    global_employee_ref UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,  -- shortcut join key
    home_country        CHAR(2)          NOT NULL,
    job_family          NVARCHAR(100)    NOT NULL,
    job_role            NVARCHAR(100)    NOT NULL,
    employment_status   NVARCHAR(20)     NOT NULL
);

CREATE TABLE abm_us.silver_customer_site_map (
    site_key            UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    customer_key        UNIQUEIDENTIFIER NOT NULL,
    customer_name       NVARCHAR(200)    NOT NULL,
    customer_industry   NVARCHAR(100)    NOT NULL,
    site_name           NVARCHAR(200)    NOT NULL,
    site_type           NVARCHAR(50)     NOT NULL,
    address_city        NVARCHAR(100)    NOT NULL,
    address_state       CHAR(2)          NOT NULL,
    address_country     CHAR(2)          NOT NULL
);

CREATE TABLE abm_us.silver_service_catalog (
    service_sku_key     UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    service_sku_name    NVARCHAR(200)    NOT NULL,
    service_line_name   NVARCHAR(100)    NOT NULL,
    service_category    NVARCHAR(50)     NOT NULL,
    unit_of_measure     NVARCHAR(20)     NOT NULL,
    standard_cost_rate  DECIMAL(18,4)    NULL,
    standard_bill_rate  DECIMAL(18,4)    NULL,
    active_flag         BIT              NOT NULL
);

CREATE TABLE abm_us.silver_contract_terms (
    contract_key    UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    contract_number NVARCHAR(20)     NOT NULL,
    customer_name   NVARCHAR(200)    NOT NULL,
    pricing_model   NVARCHAR(30)     NOT NULL,
    sla_tier        NVARCHAR(20)     NOT NULL,
    currency_code   CHAR(3)          NOT NULL,
    contract_start  NVARCHAR(8)      NOT NULL,
    contract_end    NVARCHAR(8)      NOT NULL
);

CREATE TABLE abm_us.silver_kpi_benchmarks (
    benchmark_key         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    service_line_key      UNIQUEIDENTIFIER NOT NULL,
    service_line_name     NVARCHAR(100)    NOT NULL,
    site_type             NVARCHAR(50)     NOT NULL,
    kpi_labor_util_pct    DECIMAL(5,1)     NOT NULL,
    kpi_sla_compliance_pct DECIMAL(5,1)   NOT NULL,
    kpi_cost_per_hour_usd DECIMAL(18,2)   NOT NULL,
    kpi_incident_rate     DECIMAL(5,2)    NOT NULL,
    benchmark_year        SMALLINT        NOT NULL
);

CREATE TABLE abm_us.fact_work_order (
    work_order_key    UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    date_opened_key   INT              NOT NULL,
    date_closed_key   INT              NULL,
    site_key          UNIQUEIDENTIFIER NOT NULL,
    contract_key      UNIQUEIDENTIFIER NULL,
    service_sku_key   UNIQUEIDENTIFIER NOT NULL,
    priority          NVARCHAR(20)     NOT NULL,
    status            NVARCHAR(20)     NOT NULL,
    requested_by_type NVARCHAR(20)     NOT NULL,
    planned_minutes   INT              NULL,
    actual_minutes    INT              NULL,
    labor_cost        DECIMAL(18,2)    NULL,
    material_cost     DECIMAL(18,2)    NULL
);

CREATE TABLE abm_us.fact_labor_time (
    labor_time_key   UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    date_key         INT              NOT NULL,
    employee_key     UNIQUEIDENTIFIER NOT NULL,
    site_key         UNIQUEIDENTIFIER NOT NULL,
    service_line_key UNIQUEIDENTIFIER NOT NULL,
    hours_regular    DECIMAL(9,2)     NOT NULL,
    hours_overtime   DECIMAL(9,2)     NOT NULL,
    pay_cost         DECIMAL(18,2)    NOT NULL,
    billable_flag    BIT              NOT NULL
);

CREATE TABLE abm_us.fact_billing_invoice_line (
    invoice_line_key UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    invoice_date_key INT              NOT NULL,
    contract_key     UNIQUEIDENTIFIER NOT NULL,
    site_key         UNIQUEIDENTIFIER NOT NULL,
    service_sku_key  UNIQUEIDENTIFIER NOT NULL,
    quantity         DECIMAL(18,4)    NOT NULL,
    unit_price       DECIMAL(18,4)    NOT NULL,
    line_amount      DECIMAL(18,2)    NOT NULL,
    currency_code    CHAR(3)          NOT NULL
);

CREATE TABLE abm_us.fact_safety_incident (
    incident_key              UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    incident_date_key         INT              NOT NULL,
    site_key                  UNIQUEIDENTIFIER NOT NULL,
    service_line_key          UNIQUEIDENTIFIER NOT NULL,
    severity                  NVARCHAR(20)     NOT NULL,
    incident_type             NVARCHAR(50)     NOT NULL,
    lost_time_flag            BIT              NOT NULL,
    days_lost                 INT              NULL,
    reported_to_customer_flag BIT              NOT NULL
);
"""

UK_SCHEMA_SQL = """-- ABM UK Aviation Operations Schema
-- Generated by abm/generate_dataset.py
-- Schema: abm_uk  (UK Fabric capacity — GDPR-resident PII)

CREATE SCHEMA IF NOT EXISTS abm_uk;

CREATE TABLE abm_uk.dim_airport_uk (
    airport_key   UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    iata_code     CHAR(3)          NOT NULL,
    icao_code     CHAR(4)          NULL,
    airport_name  NVARCHAR(200)    NOT NULL,
    city          NVARCHAR(100)    NOT NULL,
    country       CHAR(2)          NOT NULL,
    operator_name NVARCHAR(200)    NULL,
    timezone      NVARCHAR(50)     NOT NULL,
    site_key_us   UNIQUEIDENTIFIER NULL   -- maps to US dim_site via OneLake shortcut
);

CREATE TABLE abm_uk.dim_terminal (
    terminal_key UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    airport_key  UNIQUEIDENTIFIER NOT NULL,
    terminal_name NVARCHAR(100)   NOT NULL,
    gate_count    INT             NULL
);

CREATE TABLE abm_uk.dim_service_zone (
    zone_key      UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    terminal_key  UNIQUEIDENTIFIER NOT NULL,
    zone_type     NVARCHAR(50)     NOT NULL,
    zone_name     NVARCHAR(100)    NOT NULL
);

CREATE TABLE abm_uk.dim_shift_type (
    shift_type_key  UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    shift_name      NVARCHAR(20)     NOT NULL,
    start_time_local VARCHAR(5)      NOT NULL,
    end_time_local  VARCHAR(5)       NOT NULL,
    is_overnight    BIT              NOT NULL
);

CREATE TABLE abm_uk.dim_task_type (
    task_type_key         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    service_line_key_us   UNIQUEIDENTIFIER NULL,   -- maps to US dim_service_line via shortcut
    task_name             NVARCHAR(100)    NOT NULL,
    task_category         NVARCHAR(50)     NOT NULL,
    sla_minutes_target    INT              NULL,
    requires_equipment_flag BIT            NOT NULL
);

CREATE TABLE abm_uk.dim_equipment_uk (
    equipment_key    UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    airport_key      UNIQUEIDENTIFIER NOT NULL,
    equipment_type   NVARCHAR(100)    NOT NULL,
    asset_tag        NVARCHAR(20)     NULL,
    ownership_type   NVARCHAR(20)     NOT NULL,
    in_service_date  DATE             NULL,
    status           NVARCHAR(20)     NOT NULL
);

CREATE TABLE abm_uk.dim_employee_uk (
    employee_key        UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    worker_id           NVARCHAR(20)     NOT NULL,
    first_name          NVARCHAR(100)    NOT NULL,  -- PII: UK only
    last_name           NVARCHAR(100)    NOT NULL,  -- PII: UK only
    work_email          NVARCHAR(200)    NULL,       -- PII: UK only
    job_role            NVARCHAR(100)    NOT NULL,
    skill_group         NVARCHAR(50)     NOT NULL,
    employment_status   NVARCHAR(20)     NOT NULL,
    gdpr_consent_flag   BIT              NOT NULL,
    global_employee_ref UNIQUEIDENTIFIER NOT NULL,  -- pseudonymous; joins to US silver_employee_reference
    airport_key         UNIQUEIDENTIFIER NOT NULL
);

CREATE TABLE abm_uk.dim_roster_source (
    roster_source_key UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    source_name       NVARCHAR(100)    NOT NULL,
    ingestion_method  NVARCHAR(50)     NOT NULL
);

CREATE TABLE abm_uk.fact_shift_assignment (
    shift_assignment_key UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    date_key             INT              NOT NULL,
    airport_key          UNIQUEIDENTIFIER NOT NULL,
    terminal_key         UNIQUEIDENTIFIER NULL,
    employee_key         UNIQUEIDENTIFIER NOT NULL,
    shift_type_key       UNIQUEIDENTIFIER NOT NULL,
    roster_source_key    UNIQUEIDENTIFIER NOT NULL,
    planned_start_utc    DATETIME2        NOT NULL,
    planned_end_utc      DATETIME2        NOT NULL,
    actual_start_utc     DATETIME2        NULL,
    actual_end_utc       DATETIME2        NULL,
    planned_minutes      INT              NOT NULL,
    actual_minutes       INT              NULL,
    absence_reason       NVARCHAR(50)     NULL
);

CREATE TABLE abm_uk.fact_task_execution (
    task_execution_key   UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    date_key             INT              NOT NULL,
    airport_key          UNIQUEIDENTIFIER NOT NULL,
    zone_key             UNIQUEIDENTIFIER NULL,
    task_type_key        UNIQUEIDENTIFIER NOT NULL,
    assigned_employee_key UNIQUEIDENTIFIER NULL,
    dispatch_channel     NVARCHAR(20)     NOT NULL,
    requested_time_utc   DATETIME2        NULL,
    start_time_utc       DATETIME2        NULL,
    end_time_utc         DATETIME2        NULL,
    duration_minutes     INT              NULL,
    sla_breached_flag    BIT              NOT NULL,
    quality_score        DECIMAL(5,2)     NULL,
    notes                NVARCHAR(500)    NULL
);

CREATE TABLE abm_uk.fact_passenger_mobility_request (
    mobility_request_key  UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    date_key              INT              NOT NULL,
    airport_key           UNIQUEIDENTIFIER NOT NULL,
    terminal_key          UNIQUEIDENTIFIER NULL,
    request_source        NVARCHAR(20)     NOT NULL,
    request_type          NVARCHAR(20)     NOT NULL,
    requested_time_utc    DATETIME2        NOT NULL,
    pickup_zone_key       UNIQUEIDENTIFIER NULL,
    dropoff_zone_key      UNIQUEIDENTIFIER NULL,
    assigned_employee_key UNIQUEIDENTIFIER NULL,
    start_time_utc        DATETIME2        NULL,
    end_time_utc          DATETIME2        NULL,
    wait_minutes          INT              NULL,
    outcome               NVARCHAR(20)     NOT NULL,
    passenger_token       UNIQUEIDENTIFIER NULL   -- pseudonymous only
);

CREATE TABLE abm_uk.fact_cleaning_job (
    cleaning_job_key          UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    date_key                  INT              NOT NULL,
    airport_key               UNIQUEIDENTIFIER NOT NULL,
    zone_key                  UNIQUEIDENTIFIER NOT NULL,
    cleaning_type             NVARCHAR(20)     NOT NULL,
    square_meters             DECIMAL(18,2)    NULL,
    assigned_employee_key     UNIQUEIDENTIFIER NULL,
    equipment_key             UNIQUEIDENTIFIER NULL,
    start_time_utc            DATETIME2        NULL,
    end_time_utc              DATETIME2        NULL,
    chemicals_used_flag       BIT              NOT NULL,
    inspection_required_flag  BIT              NOT NULL
);

CREATE TABLE abm_uk.fact_ops_incident (
    ops_incident_key            UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    date_key                    INT              NOT NULL,
    airport_key                 UNIQUEIDENTIFIER NOT NULL,
    incident_category           NVARCHAR(30)     NOT NULL,
    severity                    NVARCHAR(20)     NOT NULL,
    related_task_execution_key  UNIQUEIDENTIFIER NULL,
    impacted_service_line_key_us UNIQUEIDENTIFIER NULL,
    minutes_impact              INT              NULL,
    notes                       NVARCHAR(500)    NULL
);

CREATE TABLE abm_uk.bronze_findmyshift_roster (
    raw_id             NVARCHAR(20)     NOT NULL,
    employee_worker_id NVARCHAR(20)     NULL,
    employee_name      NVARCHAR(200)    NULL,
    shift_date         NVARCHAR(8)      NULL,
    shift_name         NVARCHAR(50)     NULL,
    start_time         NVARCHAR(30)     NULL,
    end_time           NVARCHAR(30)     NULL,
    location           NVARCHAR(100)    NULL,
    absence_flag       CHAR(1)          NULL,
    source_file        NVARCHAR(200)    NULL
);

CREATE TABLE abm_uk.bronze_oracle_finance_extract (
    extract_month    NVARCHAR(7)   NOT NULL,
    worker_id        NVARCHAR(20)  NOT NULL,
    cost_centre      NVARCHAR(20)  NULL,
    job_code         NVARCHAR(20)  NULL,
    grade            NVARCHAR(5)   NULL,
    base_salary_gbp  DECIMAL(18,2) NULL,
    source_system    NVARCHAR(50)  NULL,
    extract_date     DATE          NULL
);

CREATE TABLE abm_uk.bronze_tmghcm_worker_extract (
    hcm_worker_id     NVARCHAR(20)  NOT NULL,
    first_name        NVARCHAR(100) NULL,
    last_name         NVARCHAR(100) NULL,
    email             NVARCHAR(200) NULL,
    job_role          NVARCHAR(100) NULL,
    department        NVARCHAR(50)  NULL,
    employment_status NVARCHAR(20)  NULL,
    supervisor_id     NVARCHAR(20)  NULL,
    hire_date         DATE          NULL,
    source_system     NVARCHAR(50)  NULL,
    extract_timestamp NVARCHAR(30)  NULL
);
"""


# ─────────────────────────────────────────────────────────────────────────────
# CSV / file output helpers
# ─────────────────────────────────────────────────────────────────────────────

def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_summary(output_dir: Path, tables: dict[str, list[dict]], side: str) -> None:
    summary = {
        "side": side,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "tables": {name: len(rows) for name, rows in tables.items()},
        "total_rows": sum(len(rows) for rows in tables.values()),
    }
    (output_dir / "dataset_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(f"\n[{side.upper()}] Tables written to {output_dir}")
    for name, count in summary["tables"].items():
        print(f"  {name:<45} {count:>7,} rows")
    print(f"  {'TOTAL':<45} {summary['total_rows']:>7,} rows")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate ABM synthetic dataset for Microsoft Fabric demo."
    )
    parser.add_argument("--days",   type=int,   default=DEFAULT_DAYS,
                        help=f"Number of historical days to generate. Default: {DEFAULT_DAYS}")
    parser.add_argument("--scale",  type=float, default=1.0,
                        help="Volume multiplier (0.1–2.0). Default: 1.0")
    parser.add_argument("--seed",   type=int,   default=DEFAULT_SEED,
                        help=f"Random seed. Default: {DEFAULT_SEED}")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                        help=f"Root output directory. Default: {DEFAULT_OUTPUT_DIR}")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    scale = max(0.05, min(5.0, args.scale))

    # Re-seed everything
    global rng
    rng = random.Random(args.seed)
    Faker.seed(args.seed)

    end_date   = _END_DATE
    start_date = end_date - timedelta(days=args.days - 1)
    dates      = date_range(start_date, end_date)

    us_dir = args.output_dir / "us"
    uk_dir = args.output_dir / "uk"
    us_dir.mkdir(parents=True, exist_ok=True)
    uk_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating ABM dataset: {start_date} -> {end_date} ({len(dates)} days, scale={scale})")

    # ── US dimensions ─────────────────────────────────────────────────────────
    print("\nGenerating US dimensions...")
    dim_date         = gen_dim_date(start_date, end_date)
    dim_customer     = gen_dim_customer(n=len(US_CUSTOMERS))
    dim_site         = gen_dim_site(dim_customer, n=max(12, int(35 * scale)))
    dim_service_line = gen_dim_service_line()
    dim_service_cat  = gen_dim_service_catalog(dim_service_line)
    dim_contract     = gen_dim_contract(dim_customer, dim_service_line, dates, n=max(8, int(15 * scale)))
    dim_employee_us  = gen_dim_employee_us(n=max(50, int(300 * scale)), service_lines=dim_service_line)
    dim_asset        = gen_dim_asset(dim_site, n=max(20, int(60 * scale)))

    # ── UK dimensions ─────────────────────────────────────────────────────────
    print("Generating UK dimensions...")
    dim_airport      = gen_dim_airport_uk(dim_site)
    dim_terminal     = gen_dim_terminal(dim_airport)
    dim_zone         = gen_dim_service_zone(dim_terminal)
    dim_shift_type   = gen_dim_shift_type()
    dim_task_type    = gen_dim_task_type(dim_service_line)
    n_equip_per_ap   = max(5, int(10 * scale))
    dim_equipment    = gen_dim_equipment_uk(dim_airport, n_per_airport=n_equip_per_ap)
    dim_employee_uk  = gen_dim_employee_uk(dim_airport, n=max(30, int(200 * scale)))
    dim_roster_src   = gen_dim_roster_source()

    # ── US silver ─────────────────────────────────────────────────────────────
    print("Generating US silver tables...")
    silver_emp_ref    = gen_silver_employee_reference(dim_employee_us, dim_employee_uk)
    silver_site_map   = gen_silver_customer_site_map(dim_customer, dim_site)
    silver_svc_cat    = gen_silver_service_catalog(dim_service_cat, dim_service_line)
    silver_contracts  = gen_silver_contract_terms(dim_contract, dim_customer)
    silver_kpi        = gen_silver_kpi_benchmarks(dim_service_line)

    # ── US facts ──────────────────────────────────────────────────────────────
    print("Generating US facts...")
    fact_wo      = gen_fact_work_order(dim_site, dim_contract, dim_service_cat, dates, scale)
    fact_labor   = gen_fact_labor_time(dim_employee_us, dim_site, dim_service_line, dates, scale)
    fact_billing = gen_fact_billing_invoice_line(dim_contract, dim_site, dim_service_cat, dates, scale)
    fact_safety  = gen_fact_safety_incident(dim_site, dim_service_line, dates, scale)

    # ── UK facts ──────────────────────────────────────────────────────────────
    print("Generating UK facts...")
    fact_shift   = gen_fact_shift_assignment(dim_employee_uk, dim_airport, dim_terminal,
                                             dim_shift_type, dim_roster_src, dates, scale)
    fact_task    = gen_fact_task_execution(dim_airport, dim_zone, dim_task_type,
                                           dim_employee_uk, dates, scale)
    fact_mob     = gen_fact_passenger_mobility_request(dim_airport, dim_terminal, dim_zone,
                                                        dim_task_type, dim_employee_uk,
                                                        dates, scale)
    fact_clean   = gen_fact_cleaning_job(dim_airport, dim_zone, dim_task_type,
                                          dim_employee_uk, dim_equipment, dates, scale)
    fact_inc     = gen_fact_ops_incident(dim_airport, dim_service_line, dates, scale)

    # ── UK bronze ─────────────────────────────────────────────────────────────
    print("Generating UK bronze staging tables...")
    bronze_fms    = gen_bronze_findmyshift_roster(fact_shift, dim_employee_uk)
    bronze_oracle = gen_bronze_oracle_finance_extract(dim_employee_uk, dates)
    bronze_tmghcm = gen_bronze_tmghcm_worker_extract(dim_employee_uk)

    # ── Write US output ───────────────────────────────────────────────────────
    us_tables: dict[str, list[dict]] = {
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
    for name, rows in us_tables.items():
        write_csv(us_dir / f"{name}.csv", rows)
    (us_dir / "schema.sql").write_text(US_SCHEMA_SQL, encoding="utf-8")
    write_summary(us_dir, us_tables, "US")

    # ── Write UK output ───────────────────────────────────────────────────────
    uk_tables: dict[str, list[dict]] = {
        "dim_airport_uk":                   dim_airport,
        "dim_terminal":                      dim_terminal,
        "dim_service_zone":                  dim_zone,
        "dim_shift_type":                    dim_shift_type,
        "dim_task_type":                     dim_task_type,
        "dim_equipment_uk":                  dim_equipment,
        "dim_employee_uk":                   dim_employee_uk,
        "dim_roster_source":                 dim_roster_src,
        "fact_shift_assignment":             fact_shift,
        "fact_task_execution":               fact_task,
        "fact_passenger_mobility_request":   fact_mob,
        "fact_cleaning_job":                 fact_clean,
        "fact_ops_incident":                 fact_inc,
        "bronze_findmyshift_roster":         bronze_fms,
        "bronze_oracle_finance_extract":     bronze_oracle,
        "bronze_tmghcm_worker_extract":      bronze_tmghcm,
    }
    for name, rows in uk_tables.items():
        write_csv(uk_dir / f"{name}.csv", rows)
    (uk_dir / "schema.sql").write_text(UK_SCHEMA_SQL, encoding="utf-8")
    write_summary(uk_dir, uk_tables, "UK")

    print("\nDone.")


if __name__ == "__main__":
    main()
