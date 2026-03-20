"""
Gold Layer Iceberg Table Generator
===================================
Simulates a Lakehouse Gold layer for a large-scale Industrial Supply Company
data warehouse and writes data to Amazon S3 in Apache Iceberg format.

Gold Layer Tables:
  Dimensions:
    - dim_customer
    - dim_product
    - dim_supplier
    - dim_employee
    - dim_location
    - dim_date

  Facts:
    - fact_sales_order
    - fact_purchase_order
    - fact_inventory_snapshot
    - fact_returns

  Aggregates / Mart Tables:
    - mart_sales_summary_daily
    - mart_product_velocity
    - mart_customer_360

Usage:
  1. Install dependencies:
       pip install "pyiceberg[s3,pyarrow]" faker pyarrow boto3 python-dotenv

  2. Populate .env in the same directory (copy from .env.example and fill in values):
       AWS_ACCESS_KEY_ID=YOUR_KEY
       AWS_SECRET_ACCESS_KEY=YOUR_SECRET
       AWS_DEFAULT_REGION=us-east-2
       S3_BUCKET=lakehousedata-638442050476-us-east-2-an
       S3_PREFIX=lakehouse/gold

  3. Run:
       python generate_gold_iceberg.py

  NOTE: Never commit .env to version control — it is listed in .gitignore.
"""

import os
import uuid
import random
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ─── Load .env ─────────────────────────────────────────────────────────────────
from dotenv import load_dotenv

_env_path = Path(__file__).parent / ".env"
if not _env_path.exists():
    raise FileNotFoundError(
        f"\n[ERROR] .env file not found at: {_env_path}\n"
        "Copy .env.example to .env and fill in your AWS credentials."
    )
load_dotenv(dotenv_path=_env_path)

# ─── Third-party imports (after env is loaded) ─────────────────────────────────
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType,
    DateType, TimestampType, BooleanType, IntegerType, FloatType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

# ─── Config ────────────────────────────────────────────────────────────────────

def _require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"[ERROR] Required variable '{key}' is missing or empty in .env"
        )
    return val


AWS_ACCESS_KEY_ID     = _require_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = _require_env("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET             = _require_env("S3_BUCKET")
S3_PREFIX             = os.environ.get("S3_PREFIX", "lakehouse/gold")
WAREHOUSE             = f"s3://{S3_BUCKET}/{S3_PREFIX}"

# Row counts — scale these up for larger datasets
N_CUSTOMERS  = 5_000
N_PRODUCTS   = 3_000
N_SUPPLIERS  = 500
N_EMPLOYEES  = 800
N_LOCATIONS  = 120
N_SALES      = 200_000
N_PO         = 40_000
N_INV_SNAPS  = 50_000
N_RETURNS    = 8_000

DATE_START = date(2020, 1, 1)
DATE_END   = date(2024, 12, 31)

# ─── Industry-realistic reference data ────────────────────────────────────────

PRODUCT_CATEGORIES = [
    "Fasteners", "Cutting Tools", "Abrasives", "Safety Equipment",
    "Power Tools", "Hand Tools", "Electrical Supplies", "Fluid Power",
    "Material Handling", "Janitorial & Maintenance", "Welding",
    "Pneumatics", "Adhesives & Sealants", "Measuring & Inspection",
    "Hydraulics", "Bearings & Power Transmission",
]
PRODUCT_UOM      = ["EA", "BX", "CS", "PK", "BG", "RL", "FT", "LB", "GAL", "PR"]
CUSTOMER_SEGMENTS = ["Manufacturing", "Construction", "Government", "Automotive",
                     "Aerospace", "Energy", "Food & Beverage", "Distribution"]
CUSTOMER_TIERS   = ["Platinum", "Gold", "Silver", "Bronze"]
SUPPLIER_TYPES   = ["Manufacturer", "Distributor", "Broker", "Direct Import"]
EMPLOYEE_ROLES   = ["Inside Sales Rep", "Outside Sales Rep", "Account Manager",
                    "Warehouse Associate", "Purchasing Agent", "Operations Mgr",
                    "Branch Manager", "Customer Service Rep"]
SALES_CHANNELS   = ["EDI", "Web", "Phone", "Counter", "VMI", "Field Sales"]
PAYMENT_TERMS    = ["Net 30", "Net 60", "Net 90", "2/10 Net 30", "COD", "Credit Card"]
ORDER_STATUSES   = ["Shipped", "Delivered", "Backordered", "Cancelled", "Partial"]
RETURN_REASONS   = ["Defective", "Wrong Item Ordered", "Duplicate Order",
                    "Damaged in Transit", "Overstock", "Product No Longer Needed"]
US_STATES = [
    "AL","AR","AZ","CA","CO","CT","FL","GA","IA","ID","IL","IN","KS","KY",
    "LA","MA","MD","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM",
    "NV","NY","OH","OK","OR","PA","SC","SD","TN","TX","UT","VA","WA","WI","WV","WY",
]

# ─── Helpers ───────────────────────────────────────────────────────────────────

def rand_date(start: date = DATE_START, end: date = DATE_END) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_ts(start: date = DATE_START, end: date = DATE_END) -> datetime:
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day,
                    random.randint(6, 22), random.randint(0, 59), random.randint(0, 59))

def weighted_choice(items, weights=None):
    return random.choices(items, weights=weights, k=1)[0]

# ─── Catalog ───────────────────────────────────────────────────────────────────

def get_catalog():
    """
    SqlCatalog: metadata tracked in a local SQLite file; data files written
    directly to S3 via PyIceberg's S3FileIO (credentials come from the env).

    Swap 'type: sql' for 'type: glue' or a REST catalog URL in production.
    """
    return load_catalog(
        "local",
        **{
            "type": "sql",
            "uri": "sqlite:///iceberg_catalog.db",
            "warehouse": WAREHOUSE,
            "s3.access-key-id": AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
            "s3.region": AWS_REGION,
            "s3.endpoint": f"https://s3.{AWS_REGION}.amazonaws.com",
        },
    )


def ensure_namespace(catalog, namespace: str):
    try:
        catalog.create_namespace(namespace)
        log.info(f"Created namespace: {namespace}")
    except Exception:
        log.info(f"Namespace already exists: {namespace}")

# ─── Iceberg schema definitions ───────────────────────────────────────────────

SCHEMAS = {

    "dim_customer": Schema(
        NestedField(1,  "customer_key",       LongType(),   required=True),
        NestedField(2,  "customer_id",        StringType(), required=True),
        NestedField(3,  "customer_name",      StringType()),
        NestedField(4,  "segment",            StringType()),
        NestedField(5,  "tier",               StringType()),
        NestedField(6,  "payment_terms",      StringType()),
        NestedField(7,  "credit_limit",       DoubleType()),
        NestedField(8,  "primary_state",      StringType()),
        NestedField(9,  "primary_city",       StringType()),
        NestedField(10, "account_manager_id", LongType()),
        NestedField(11, "is_active",          BooleanType()),
        NestedField(12, "created_date",       DateType()),
        NestedField(13, "last_order_date",    DateType()),
        NestedField(14, "lifetime_revenue",   DoubleType()),
        NestedField(15, "dw_insert_ts",       TimestampType()),
    ),

    "dim_product": Schema(
        NestedField(1,  "product_key",   LongType(),   required=True),
        NestedField(2,  "product_id",    StringType(), required=True),
        NestedField(3,  "sku",           StringType()),
        NestedField(4,  "product_name",  StringType()),
        NestedField(5,  "category",      StringType()),
        NestedField(6,  "subcategory",   StringType()),
        NestedField(7,  "brand",         StringType()),
        NestedField(8,  "supplier_key",  LongType()),
        NestedField(9,  "uom",           StringType()),
        NestedField(10, "unit_cost",     DoubleType()),
        NestedField(11, "list_price",    DoubleType()),
        NestedField(12, "weight_lbs",    FloatType()),
        NestedField(13, "is_hazmat",     BooleanType()),
        NestedField(14, "is_active",     BooleanType()),
        NestedField(15, "dw_insert_ts",  TimestampType()),
    ),

    "dim_supplier": Schema(
        NestedField(1,  "supplier_key",   LongType(),   required=True),
        NestedField(2,  "supplier_id",    StringType(), required=True),
        NestedField(3,  "supplier_name",  StringType()),
        NestedField(4,  "supplier_type",  StringType()),
        NestedField(5,  "primary_state",  StringType()),
        NestedField(6,  "lead_time_days", IntegerType()),
        NestedField(7,  "payment_terms",  StringType()),
        NestedField(8,  "is_preferred",   BooleanType()),
        NestedField(9,  "is_active",      BooleanType()),
        NestedField(10, "dw_insert_ts",   TimestampType()),
    ),

    "dim_employee": Schema(
        NestedField(1, "employee_key",        LongType(),   required=True),
        NestedField(2, "employee_id",         StringType(), required=True),
        NestedField(3, "full_name",           StringType()),
        NestedField(4, "role",                StringType()),
        NestedField(5, "branch_location_key", LongType()),
        NestedField(6, "hire_date",           DateType()),
        NestedField(7, "is_active",           BooleanType()),
        NestedField(8, "dw_insert_ts",        TimestampType()),
    ),

    "dim_location": Schema(
        NestedField(1,  "location_key",           LongType(),   required=True),
        NestedField(2,  "location_id",            StringType(), required=True),
        NestedField(3,  "location_name",          StringType()),
        NestedField(4,  "city",                   StringType()),
        NestedField(5,  "state",                  StringType()),
        NestedField(6,  "zip_code",               StringType()),
        NestedField(7,  "region",                 StringType()),
        NestedField(8,  "is_distribution_center", BooleanType()),
        NestedField(9,  "sq_footage",             IntegerType()),
        NestedField(10, "dw_insert_ts",           TimestampType()),
    ),

    "dim_date": Schema(
        NestedField(1,  "date_key",      IntegerType(), required=True),
        NestedField(2,  "full_date",     DateType(),    required=True),
        NestedField(3,  "year",          IntegerType()),
        NestedField(4,  "quarter",       IntegerType()),
        NestedField(5,  "month",         IntegerType()),
        NestedField(6,  "month_name",    StringType()),
        NestedField(7,  "week_of_year",  IntegerType()),
        NestedField(8,  "day_of_week",   IntegerType()),
        NestedField(9,  "day_name",      StringType()),
        NestedField(10, "is_weekend",    BooleanType()),
        NestedField(11, "is_holiday",    BooleanType()),
        NestedField(12, "fiscal_year",   IntegerType()),
        NestedField(13, "fiscal_quarter",IntegerType()),
    ),

    "fact_sales_order": Schema(
        NestedField(1,  "order_line_key",    StringType(),  required=True),
        NestedField(2,  "order_id",          StringType(),  required=True),
        NestedField(3,  "order_line_number", IntegerType()),
        NestedField(4,  "order_date_key",    IntegerType()),
        NestedField(5,  "ship_date_key",     IntegerType()),
        NestedField(6,  "customer_key",      LongType()),
        NestedField(7,  "product_key",       LongType()),
        NestedField(8,  "employee_key",      LongType()),
        NestedField(9,  "ship_location_key", LongType()),
        NestedField(10, "channel",           StringType()),
        NestedField(11, "quantity_ordered",  IntegerType()),
        NestedField(12, "quantity_shipped",  IntegerType()),
        NestedField(13, "unit_list_price",   DoubleType()),
        NestedField(14, "unit_sell_price",   DoubleType()),
        NestedField(15, "discount_pct",      DoubleType()),
        NestedField(16, "gross_revenue",     DoubleType()),
        NestedField(17, "discount_amount",   DoubleType()),
        NestedField(18, "net_revenue",       DoubleType()),
        NestedField(19, "cost_of_goods",     DoubleType()),
        NestedField(20, "gross_margin",      DoubleType()),
        NestedField(21, "order_status",      StringType()),
        NestedField(22, "order_ts",          TimestampType()),
        NestedField(23, "dw_insert_ts",      TimestampType()),
    ),

    "fact_purchase_order": Schema(
        NestedField(1,  "po_line_key",        StringType(),  required=True),
        NestedField(2,  "po_id",              StringType(),  required=True),
        NestedField(3,  "po_line_number",     IntegerType()),
        NestedField(4,  "po_date_key",        IntegerType()),
        NestedField(5,  "receipt_date_key",   IntegerType()),
        NestedField(6,  "supplier_key",       LongType()),
        NestedField(7,  "product_key",        LongType()),
        NestedField(8,  "location_key",       LongType()),
        NestedField(9,  "buyer_employee_key", LongType()),
        NestedField(10, "quantity_ordered",   IntegerType()),
        NestedField(11, "quantity_received",  IntegerType()),
        NestedField(12, "unit_cost",          DoubleType()),
        NestedField(13, "extended_cost",      DoubleType()),
        NestedField(14, "freight_cost",       DoubleType()),
        NestedField(15, "total_landed_cost",  DoubleType()),
        NestedField(16, "is_on_time",         BooleanType()),
        NestedField(17, "po_ts",              TimestampType()),
        NestedField(18, "dw_insert_ts",       TimestampType()),
    ),

    "fact_inventory_snapshot": Schema(
        NestedField(1,  "snapshot_key",      StringType(),  required=True),
        NestedField(2,  "snapshot_date_key", IntegerType()),
        NestedField(3,  "product_key",       LongType()),
        NestedField(4,  "location_key",      LongType()),
        NestedField(5,  "qty_on_hand",       IntegerType()),
        NestedField(6,  "qty_on_order",      IntegerType()),
        NestedField(7,  "qty_allocated",     IntegerType()),
        NestedField(8,  "qty_available",     IntegerType()),
        NestedField(9,  "unit_cost",         DoubleType()),
        NestedField(10, "inventory_value",   DoubleType()),
        NestedField(11, "reorder_point",     IntegerType()),
        NestedField(12, "is_below_reorder",  BooleanType()),
        NestedField(13, "days_of_supply",    FloatType()),
        NestedField(14, "dw_insert_ts",      TimestampType()),
    ),

    "fact_returns": Schema(
        NestedField(1,  "return_key",        StringType(),  required=True),
        NestedField(2,  "rma_id",            StringType(),  required=True),
        NestedField(3,  "original_order_id", StringType()),
        NestedField(4,  "return_date_key",   IntegerType()),
        NestedField(5,  "customer_key",      LongType()),
        NestedField(6,  "product_key",       LongType()),
        NestedField(7,  "location_key",      LongType()),
        NestedField(8,  "quantity_returned", IntegerType()),
        NestedField(9,  "return_reason",     StringType()),
        NestedField(10, "credit_amount",     DoubleType()),
        NestedField(11, "restocking_fee",    DoubleType()),
        NestedField(12, "net_credit",        DoubleType()),
        NestedField(13, "is_resaleable",     BooleanType()),
        NestedField(14, "return_ts",         TimestampType()),
        NestedField(15, "dw_insert_ts",      TimestampType()),
    ),

    "mart_sales_summary_daily": Schema(
        NestedField(1,  "summary_key",     StringType(),  required=True),
        NestedField(2,  "date_key",        IntegerType()),
        NestedField(3,  "customer_key",    LongType()),
        NestedField(4,  "product_key",     LongType()),
        NestedField(5,  "channel",         StringType()),
        NestedField(6,  "order_count",     IntegerType()),
        NestedField(7,  "line_count",      IntegerType()),
        NestedField(8,  "total_qty",       IntegerType()),
        NestedField(9,  "gross_revenue",   DoubleType()),
        NestedField(10, "net_revenue",     DoubleType()),
        NestedField(11, "cost_of_goods",   DoubleType()),
        NestedField(12, "gross_margin",    DoubleType()),
        NestedField(13, "gross_margin_pct",DoubleType()),
        NestedField(14, "dw_insert_ts",    TimestampType()),
    ),

    "mart_product_velocity": Schema(
        NestedField(1,  "velocity_key",       StringType(),  required=True),
        NestedField(2,  "product_key",        LongType()),
        NestedField(3,  "location_key",       LongType()),
        NestedField(4,  "period_month_key",   IntegerType()),
        NestedField(5,  "units_sold",         IntegerType()),
        NestedField(6,  "orders_containing",  IntegerType()),
        NestedField(7,  "avg_sell_price",     DoubleType()),
        NestedField(8,  "net_revenue",        DoubleType()),
        NestedField(9,  "fill_rate_pct",      DoubleType()),
        NestedField(10, "return_rate_pct",    DoubleType()),
        NestedField(11, "avg_days_of_supply", FloatType()),
        NestedField(12, "velocity_class",     StringType()),
        NestedField(13, "dw_insert_ts",       TimestampType()),
    ),

    "mart_customer_360": Schema(
        NestedField(1,  "customer_key",           LongType(),  required=True),
        NestedField(2,  "snapshot_date_key",       IntegerType()),
        NestedField(3,  "ytd_revenue",             DoubleType()),
        NestedField(4,  "ytd_orders",              IntegerType()),
        NestedField(5,  "ytd_lines",               IntegerType()),
        NestedField(6,  "ytd_return_rate_pct",     DoubleType()),
        NestedField(7,  "l12m_revenue",            DoubleType()),
        NestedField(8,  "l12m_orders",             IntegerType()),
        NestedField(9,  "avg_order_value",         DoubleType()),
        NestedField(10, "avg_gross_margin_pct",    DoubleType()),
        NestedField(11, "days_since_last_order",   IntegerType()),
        NestedField(12, "preferred_channel",       StringType()),
        NestedField(13, "top_category",            StringType()),
        NestedField(14, "churn_risk_score",        FloatType()),
        NestedField(15, "customer_lifetime_value", DoubleType()),
        NestedField(16, "dw_insert_ts",            TimestampType()),
    ),
}

# ─── Partition specs ───────────────────────────────────────────────────────────

# Partition specs — source_id must reference a Date or Timestamp field.
# Fact tables partition on their natural event timestamp; mart tables on dw_insert_ts.
#   fact_sales_order        field 22 = order_ts        (TimestampType)
#   fact_purchase_order     field 17 = po_ts            (TimestampType)
#   fact_inventory_snapshot field 14 = dw_insert_ts     (TimestampType, no event ts on snapshot)
#   fact_returns            field 14 = return_ts        (TimestampType)
#   mart_sales_summary_daily field 14 = dw_insert_ts    (TimestampType, date_key is int)
PARTITION_SPECS = {
    "fact_sales_order":        PartitionSpec(PartitionField(source_id=22, field_id=1001, transform=MonthTransform(), name="order_month")),
    "fact_purchase_order":     PartitionSpec(PartitionField(source_id=17, field_id=1001, transform=MonthTransform(), name="po_month")),
    "fact_inventory_snapshot": PartitionSpec(PartitionField(source_id=14, field_id=1001, transform=MonthTransform(), name="snapshot_month")),
    "fact_returns":            PartitionSpec(PartitionField(source_id=14, field_id=1001, transform=MonthTransform(), name="return_month")),
    "mart_sales_summary_daily":PartitionSpec(PartitionField(source_id=14, field_id=1001, transform=MonthTransform(), name="load_month")),
}

# ─── Data generators ──────────────────────────────────────────────────────────

def gen_dim_customer(n):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i in range(1, n + 1):
        created    = rand_date(date(2010, 1, 1), date(2022, 12, 31))
        last_order = rand_date(created, DATE_END)
        rows.append({
            "customer_key":       i,
            "customer_id":        f"CUST-{i:06d}",
            "customer_name":      fake.company(),
            "segment":            random.choice(CUSTOMER_SEGMENTS),
            "tier":               weighted_choice(CUSTOMER_TIERS, [10, 25, 35, 30]),
            "payment_terms":      random.choice(PAYMENT_TERMS),
            "credit_limit":       float(random.choice([10_000, 25_000, 50_000, 100_000, 250_000, 500_000])),
            "primary_state":      random.choice(US_STATES),
            "primary_city":       fake.city(),
            "account_manager_id": random.randint(1, N_EMPLOYEES),
            "is_active":          random.random() > 0.08,
            "created_date":       created,
            "last_order_date":    last_order,
            "lifetime_revenue":   round(random.uniform(5_000, 5_000_000), 2),
            "dw_insert_ts":       now,
        })
    return rows


def gen_dim_product(n, supplier_keys):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    brands = [fake.company() for _ in range(80)]
    rows = []
    for i in range(1, n + 1):
        cat  = random.choice(PRODUCT_CATEGORIES)
        cost = round(random.uniform(0.50, 1_500.00), 4)
        rows.append({
            "product_key":  i,
            "product_id":   f"PROD-{i:07d}",
            "sku":          fake.bothify("??###-####").upper(),
            "product_name": f"{random.choice(brands)} {cat} {fake.word().title()}",
            "category":     cat,
            "subcategory":  fake.word().title(),
            "brand":        random.choice(brands),
            "supplier_key": random.choice(supplier_keys),
            "uom":          random.choice(PRODUCT_UOM),
            "unit_cost":    cost,
            "list_price":   round(cost * random.uniform(1.25, 2.80), 4),
            "weight_lbs":   round(random.uniform(0.1, 200.0), 2),
            "is_hazmat":    random.random() < 0.07,
            "is_active":    random.random() > 0.05,
            "dw_insert_ts": now,
        })
    return rows


def gen_dim_supplier(n):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "supplier_key":   i,
            "supplier_id":    f"SUP-{i:05d}",
            "supplier_name":  fake.company(),
            "supplier_type":  random.choice(SUPPLIER_TYPES),
            "primary_state":  random.choice(US_STATES),
            "lead_time_days": random.randint(1, 45),
            "payment_terms":  random.choice(PAYMENT_TERMS),
            "is_preferred":   random.random() > 0.5,
            "is_active":      random.random() > 0.06,
            "dw_insert_ts":   now,
        })
    return rows


def gen_dim_employee(n, location_keys):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "employee_key":        i,
            "employee_id":         f"EMP-{i:05d}",
            "full_name":           fake.name(),
            "role":                random.choice(EMPLOYEE_ROLES),
            "branch_location_key": random.choice(location_keys),
            "hire_date":           rand_date(date(2005, 1, 1), date(2023, 12, 31)),
            "is_active":           random.random() > 0.10,
            "dw_insert_ts":        now,
        })
    return rows


def gen_dim_location(n):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    state_region = {
        "CA":"W","OR":"W","WA":"W","NV":"W","AZ":"W",
        "IL":"MW","OH":"MW","MI":"MW","MN":"MW","MO":"MW","WI":"MW","IN":"MW","IA":"MW",
        "TX":"S","FL":"S","GA":"S","NC":"S","TN":"S","AL":"S","LA":"S","SC":"S",
        "MS":"S","AR":"S","OK":"S",
        "NY":"NE","PA":"NE","MA":"NE","NJ":"NE","CT":"NE","MD":"NE",
        "NH":"NE","ME":"NE","VT":"NE","WV":"NE",
        "CO":"MW2","KS":"MW2","NE":"MW2","SD":"MW2","ND":"MW2",
        "MT":"MW2","ID":"MW2","WY":"MW2","UT":"MW2","NM":"MW2",
    }
    rows = []
    for i in range(1, n + 1):
        state = random.choice(US_STATES)
        rows.append({
            "location_key":           i,
            "location_id":            f"LOC-{i:04d}",
            "location_name":          f"{fake.city()} Branch",
            "city":                   fake.city(),
            "state":                  state,
            "zip_code":               fake.zipcode(),
            "region":                 state_region.get(state, "Other"),
            "is_distribution_center": random.random() < 0.20,
            "sq_footage":             random.choice([10_000, 25_000, 50_000, 100_000, 200_000]),
            "dw_insert_ts":           now,
        })
    return rows


def gen_dim_date(start: date, end: date):
    holidays = set()
    for y in range(start.year, end.year + 1):
        holidays.update([date(y, 1, 1), date(y, 7, 4), date(y, 12, 25), date(y, 11, 11)])
    rows, d = [], start
    while d <= end:
        rows.append({
            "date_key":      int(d.strftime("%Y%m%d")),
            "full_date":     d,
            "year":          d.year,
            "quarter":       (d.month - 1) // 3 + 1,
            "month":         d.month,
            "month_name":    d.strftime("%B"),
            "week_of_year":  int(d.strftime("%W")),
            "day_of_week":   d.weekday(),
            "day_name":      d.strftime("%A"),
            "is_weekend":    d.weekday() >= 5,
            "is_holiday":    d in holidays,
            "fiscal_year":   d.year if d.month >= 4 else d.year - 1,
            "fiscal_quarter":((d.month - 4) % 12) // 3 + 1,
        })
        d += timedelta(days=1)
    return rows


def gen_fact_sales_order(n, customer_keys, product_keys, employee_keys, location_keys, date_keys):
    now, rows = datetime.now(timezone.utc).replace(tzinfo=None), []
    for _ in range(n):
        order_date_key = random.choice(date_keys)
        ship_date_key  = min(order_date_key + random.randint(1, 14), max(date_keys))
        cost       = round(random.uniform(1, 800), 4)
        list_price = round(cost * random.uniform(1.3, 2.5), 4)
        disc_pct   = round(random.uniform(0, 0.35), 4)
        sell_price = round(list_price * (1 - disc_pct), 4)
        qty_ord    = random.randint(1, 200)
        qty_ship   = qty_ord if random.random() > 0.10 else random.randint(1, qty_ord)
        gross_rev  = round(list_price * qty_ship, 4)
        disc_amt   = round(gross_rev * disc_pct, 4)
        net_rev    = round(gross_rev - disc_amt, 4)
        cogs       = round(cost * qty_ship, 4)
        rows.append({
            "order_line_key":    str(uuid.uuid4()),
            "order_id":          f"SO-{random.randint(100000, 999999)}",
            "order_line_number": random.randint(1, 10),
            "order_date_key":    order_date_key,
            "ship_date_key":     ship_date_key,
            "customer_key":      random.choice(customer_keys),
            "product_key":       random.choice(product_keys),
            "employee_key":      random.choice(employee_keys),
            "ship_location_key": random.choice(location_keys),
            "channel":           random.choice(SALES_CHANNELS),
            "quantity_ordered":  qty_ord,
            "quantity_shipped":  qty_ship,
            "unit_list_price":   list_price,
            "unit_sell_price":   sell_price,
            "discount_pct":      disc_pct,
            "gross_revenue":     gross_rev,
            "discount_amount":   disc_amt,
            "net_revenue":       net_rev,
            "cost_of_goods":     cogs,
            "gross_margin":      round(net_rev - cogs, 4),
            "order_status":      random.choice(ORDER_STATUSES),
            "order_ts":          rand_ts(),
            "dw_insert_ts":      now,
        })
    return rows


def gen_fact_purchase_order(n, supplier_keys, product_keys, location_keys, employee_keys, date_keys):
    now, rows = datetime.now(timezone.utc).replace(tzinfo=None), []
    for _ in range(n):
        po_date_key     = random.choice(date_keys)
        receipt_date_key= min(po_date_key + random.randint(1, 45), max(date_keys))
        qty_ord  = random.randint(10, 500)
        qty_recv = qty_ord if random.random() > 0.12 else random.randint(1, qty_ord)
        unit_cost= round(random.uniform(0.5, 800), 4)
        ext_cost = round(unit_cost * qty_recv, 4)
        freight  = round(ext_cost * random.uniform(0.01, 0.06), 4)
        rows.append({
            "po_line_key":        str(uuid.uuid4()),
            "po_id":              f"PO-{random.randint(100000, 999999)}",
            "po_line_number":     random.randint(1, 8),
            "po_date_key":        po_date_key,
            "receipt_date_key":   receipt_date_key,
            "supplier_key":       random.choice(supplier_keys),
            "product_key":        random.choice(product_keys),
            "location_key":       random.choice(location_keys),
            "buyer_employee_key": random.choice(employee_keys),
            "quantity_ordered":   qty_ord,
            "quantity_received":  qty_recv,
            "unit_cost":          unit_cost,
            "extended_cost":      ext_cost,
            "freight_cost":       freight,
            "total_landed_cost":  round(ext_cost + freight, 4),
            "is_on_time":         random.random() > 0.15,
            "po_ts":              rand_ts(),
            "dw_insert_ts":       now,
        })
    return rows


def gen_fact_inventory_snapshot(n, product_keys, location_keys, date_keys):
    now, rows = datetime.now(timezone.utc).replace(tzinfo=None), []
    for _ in range(n):
        on_hand   = random.randint(0, 5000)
        allocated = random.randint(0, min(on_hand, 500))
        unit_cost = round(random.uniform(0.5, 800), 4)
        reorder   = random.randint(5, 200)
        rows.append({
            "snapshot_key":      str(uuid.uuid4()),
            "snapshot_date_key": random.choice(date_keys),
            "product_key":       random.choice(product_keys),
            "location_key":      random.choice(location_keys),
            "qty_on_hand":       on_hand,
            "qty_on_order":      random.randint(0, 1000),
            "qty_allocated":     allocated,
            "qty_available":     max(0, on_hand - allocated),
            "unit_cost":         unit_cost,
            "inventory_value":   round(on_hand * unit_cost, 4),
            "reorder_point":     reorder,
            "is_below_reorder":  on_hand < reorder,
            "days_of_supply":    round(random.uniform(0, 180), 1),
            "dw_insert_ts":      now,
        })
    return rows


def gen_fact_returns(n, customer_keys, product_keys, location_keys, date_keys):
    now, rows = datetime.now(timezone.utc).replace(tzinfo=None), []
    for _ in range(n):
        credit  = round(random.uniform(10, 5000), 4)
        restock = round(credit * random.uniform(0, 0.15), 4)
        rows.append({
            "return_key":        str(uuid.uuid4()),
            "rma_id":            f"RMA-{random.randint(10000, 99999)}",
            "original_order_id": f"SO-{random.randint(100000, 999999)}",
            "return_date_key":   random.choice(date_keys),
            "customer_key":      random.choice(customer_keys),
            "product_key":       random.choice(product_keys),
            "location_key":      random.choice(location_keys),
            "quantity_returned": random.randint(1, 50),
            "return_reason":     random.choice(RETURN_REASONS),
            "credit_amount":     credit,
            "restocking_fee":    restock,
            "net_credit":        round(credit - restock, 4),
            "is_resaleable":     random.random() > 0.25,
            "return_ts":         rand_ts(),
            "dw_insert_ts":      now,
        })
    return rows


def gen_mart_sales_summary_daily(n, customer_keys, product_keys, date_keys):
    now, rows = datetime.now(timezone.utc).replace(tzinfo=None), []
    for _ in range(n):
        gross_rev = round(random.uniform(500, 500_000), 4)
        net_rev   = round(gross_rev * random.uniform(0.75, 1.0), 4)
        cogs      = round(net_rev * random.uniform(0.45, 0.72), 4)
        gm        = round(net_rev - cogs, 4)
        rows.append({
            "summary_key":      str(uuid.uuid4()),
            "date_key":         random.choice(date_keys),
            "customer_key":     random.choice(customer_keys),
            "product_key":      random.choice(product_keys),
            "channel":          random.choice(SALES_CHANNELS),
            "order_count":      random.randint(1, 50),
            "line_count":       random.randint(1, 200),
            "total_qty":        random.randint(1, 2000),
            "gross_revenue":    gross_rev,
            "net_revenue":      net_rev,
            "cost_of_goods":    cogs,
            "gross_margin":     gm,
            "gross_margin_pct": round(gm / net_rev * 100 if net_rev else 0, 4),
            "dw_insert_ts":     now,
        })
    return rows


def gen_mart_product_velocity(product_keys, location_keys):
    now, rows = datetime.now(timezone.utc).replace(tzinfo=None), []
    for pk in random.sample(product_keys, min(500, len(product_keys))):
        for lk in random.sample(location_keys, min(5, len(location_keys))):
            for ym in range(202001, 202413):
                month = ym % 100
                if month < 1 or month > 12:
                    continue
                units   = random.randint(0, 2000)
                net_rev = round(units * random.uniform(5, 300), 4)
                rows.append({
                    "velocity_key":       str(uuid.uuid4()),
                    "product_key":        pk,
                    "location_key":       lk,
                    "period_month_key":   ym,
                    "units_sold":         units,
                    "orders_containing":  random.randint(1, max(1, units // 10)),
                    "avg_sell_price":     round(random.uniform(5, 300), 4),
                    "net_revenue":        net_rev,
                    "fill_rate_pct":      round(random.uniform(85, 100), 4),
                    "return_rate_pct":    round(random.uniform(0, 8), 4),
                    "avg_days_of_supply": round(random.uniform(5, 90), 1),
                    "velocity_class":     weighted_choice(["A","B","C","D"], [10, 25, 35, 30]),
                    "dw_insert_ts":       now,
                })
    return rows


def gen_mart_customer_360(customer_keys):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    snapshot_date_key = int(DATE_END.strftime("%Y%m%d"))
    rows = []
    for ck in customer_keys:
        ytd_rev  = round(random.uniform(0, 2_000_000), 4)
        l12m_rev = round(ytd_rev * random.uniform(0.8, 1.3), 4)
        orders   = random.randint(0, 500)
        rows.append({
            "customer_key":           ck,
            "snapshot_date_key":      snapshot_date_key,
            "ytd_revenue":            ytd_rev,
            "ytd_orders":             orders,
            "ytd_lines":              orders * random.randint(1, 12),
            "ytd_return_rate_pct":    round(random.uniform(0, 10), 4),
            "l12m_revenue":           l12m_rev,
            "l12m_orders":            orders + random.randint(0, 50),
            "avg_order_value":        round(ytd_rev / orders if orders else 0, 4),
            "avg_gross_margin_pct":   round(random.uniform(20, 55), 4),
            "days_since_last_order":  random.randint(0, 730),
            "preferred_channel":      random.choice(SALES_CHANNELS),
            "top_category":           random.choice(PRODUCT_CATEGORIES),
            "churn_risk_score":       round(random.uniform(0, 1), 4),
            "customer_lifetime_value":round(l12m_rev * random.uniform(2, 8), 4),
            "dw_insert_ts":           now,
        })
    return rows

# ─── PyArrow helpers ───────────────────────────────────────────────────────────

ICEBERG_TO_PA = {
    LongType:      pa.int64(),
    IntegerType:   pa.int32(),
    DoubleType:    pa.float64(),
    FloatType:     pa.float32(),
    StringType:    pa.string(),
    BooleanType:   pa.bool_(),
    DateType:      pa.date32(),
    TimestampType: pa.timestamp("us"),
}


def rows_to_arrow(rows: list, iceberg_schema: Schema) -> pa.Table:
    arrays = {}
    for f in iceberg_schema.fields:
        pa_type = ICEBERG_TO_PA.get(type(f.field_type))
        if pa_type is None:
            raise ValueError(f"Unhandled Iceberg type {f.field_type} for field '{f.name}'")
        arrays[f.name] = pa.array([r[f.name] for r in rows], type=pa_type)
    pa_fields = [
        pa.field(f.name, ICEBERG_TO_PA[type(f.field_type)], nullable=not f.required)
        for f in iceberg_schema.fields
    ]
    return pa.table(arrays, schema=pa.schema(pa_fields))

# ─── Table writer ─────────────────────────────────────────────────────────────

def drop_table_if_exists(catalog, full_name: str):
    """Drop a table from the catalog if it exists (does not delete S3 data files)."""
    try:
        catalog.drop_table(full_name)
        log.info(f"Dropped existing table {full_name} (stale metadata)")
    except Exception:
        pass  # Table didn't exist — nothing to do


def write_table(catalog, namespace: str, table_name: str, rows: list,
                recreate: bool = False):
    """
    Write rows to an Iceberg table.

    Args:
        recreate: If True, drop and recreate the table before writing.
                  Use this for tables whose partition spec may have changed
                  between runs (i.e. all fact and mart tables).
    """
    full_name      = f"{namespace}.{table_name}"
    iceberg_schema = SCHEMAS[table_name]
    partition_spec = PARTITION_SPECS.get(table_name, PartitionSpec())

    if recreate:
        drop_table_if_exists(catalog, full_name)

    try:
        table = catalog.load_table(full_name)
        log.info(f"Table {full_name} already exists — appending")
    except Exception:
        log.info(f"Creating table {full_name}")
        table = catalog.create_table(
            identifier=full_name,
            schema=iceberg_schema,
            partition_spec=partition_spec,
        )

    table.append(rows_to_arrow(rows, iceberg_schema))
    log.info(f"  ✓ {len(rows):,} rows → {full_name}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info(f"Warehouse : {WAREHOUSE}")
    log.info(f"Region    : {AWS_REGION}")
    log.info("=" * 60)

    catalog = get_catalog()
    ns = "gold"
    ensure_namespace(catalog, ns)

    # Dimensions (order matters — later dims reference earlier keys)
    log.info("── Dimensions ──────────────────────────────────────────")

    suppliers = gen_dim_supplier(N_SUPPLIERS)
    write_table(catalog, ns, "dim_supplier", suppliers)
    supplier_keys = [r["supplier_key"] for r in suppliers]

    locations = gen_dim_location(N_LOCATIONS)
    write_table(catalog, ns, "dim_location", locations)
    location_keys = [r["location_key"] for r in locations]

    employees = gen_dim_employee(N_EMPLOYEES, location_keys)
    write_table(catalog, ns, "dim_employee", employees)
    employee_keys = [r["employee_key"] for r in employees]

    customers = gen_dim_customer(N_CUSTOMERS)
    write_table(catalog, ns, "dim_customer", customers)
    customer_keys = [r["customer_key"] for r in customers]

    products = gen_dim_product(N_PRODUCTS, supplier_keys)
    write_table(catalog, ns, "dim_product", products)
    product_keys = [r["product_key"] for r in products]

    dates = gen_dim_date(DATE_START, DATE_END)
    write_table(catalog, ns, "dim_date", dates)
    date_keys = [r["date_key"] for r in dates]

    # Facts
    log.info("── Facts ───────────────────────────────────────────────")

    log.info(f"Generating fact_sales_order ({N_SALES:,} rows) …")
    write_table(catalog, ns, "fact_sales_order",
                gen_fact_sales_order(N_SALES, customer_keys, product_keys,
                                     employee_keys, location_keys, date_keys),
                recreate=True)

    log.info(f"Generating fact_purchase_order ({N_PO:,} rows) …")
    write_table(catalog, ns, "fact_purchase_order",
                gen_fact_purchase_order(N_PO, supplier_keys, product_keys,
                                        location_keys, employee_keys, date_keys),
                recreate=True)

    log.info(f"Generating fact_inventory_snapshot ({N_INV_SNAPS:,} rows) …")
    write_table(catalog, ns, "fact_inventory_snapshot",
                gen_fact_inventory_snapshot(N_INV_SNAPS, product_keys, location_keys, date_keys),
                recreate=True)

    log.info(f"Generating fact_returns ({N_RETURNS:,} rows) …")
    write_table(catalog, ns, "fact_returns",
                gen_fact_returns(N_RETURNS, customer_keys, product_keys, location_keys, date_keys),
                recreate=True)

    # Marts
    log.info("── Marts ───────────────────────────────────────────────")

    log.info("Generating mart_sales_summary_daily (20,000 rows) …")
    write_table(catalog, ns, "mart_sales_summary_daily",
                gen_mart_sales_summary_daily(20_000, customer_keys, product_keys, date_keys),
                recreate=True)

    log.info("Generating mart_product_velocity …")
    write_table(catalog, ns, "mart_product_velocity",
                gen_mart_product_velocity(product_keys, location_keys),
                recreate=True)

    log.info("Generating mart_customer_360 …")
    write_table(catalog, ns, "mart_customer_360",
                gen_mart_customer_360(customer_keys),
                recreate=True)

    log.info("=" * 60)
    log.info("All Gold layer tables written successfully!")
    log.info(f"Warehouse : {WAREHOUSE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
