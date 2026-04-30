GPS-Inspired Fabric Data Generator
==================================

This repo now includes a synthetic dataset generator for a relational sustainability model inspired by the Gensler Product Sustainability Standards 2.0 focus areas around:

- embodied carbon / GWP disclosed through EPDs
- indoor air quality / VOC emissions testing
- manufacturer and facility provenance
- product certifications
- project-level product submittal review decisions

The generated output is shaped for Microsoft Fabric open mirroring.

Files
-----

- `generate_gps_initial_dataset.py`
  Creates the initial relational dataset and writes open mirroring initial-load files.
- `generate_gps_incremental_changes.py`
  Creates new rows plus updates and deletes for existing data, and writes change files with `__rowMarker__`.
- `generate_workspace_performance_sqlmi_dataset.py`
  Creates a Gensler-inspired workspace performance dataset as CSV plus SQL Server schema/import scripts for Azure SQL Managed Instance.
- `publish_to_fabric_open_mirroring.py`
  Uploads a local open mirroring folder tree into a Fabric OneLake landing zone using the ADLS Gen2 compatible SDK.
- `initial_load.ps1`
  PowerShell wrapper for install, initial generation, and publish.
- `incremental_load.ps1`
  PowerShell wrapper for incremental generation and publish.
- `gps_fabric/open_mirroring.py`
  Shared data model, generation logic, file sequencing, and OneLake URL parsing.

Generated Tables
----------------

- `manufacturers`
- `manufacturing_facilities`
- `product_categories`
- `products`
- `epds`
- `iaq_test_results`
- `product_certifications`
- `projects`
- `project_submittals`

Install
-------

Create and activate a repo-local virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Run commands through the venv explicitly when it is not activated:

```bash
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Generate Initial Data
---------------------

```powershell
.\.venv\Scripts\python.exe generate_gps_initial_dataset.py --scale 1.0
```

This creates:

- `output/open_mirroring_initial/`
- `output/snapshots/`

The initial landing-zone folder contains one folder per table, each with:

- `_metadata.json`
- `00000000000000000001.parquet`

Generate Azure SQL MI Workspace Demo Data
-----------------------------------------

```powershell
.\.venv\Scripts\python.exe generate_workspace_performance_sqlmi_dataset.py --clean
```

This creates an import package under `output/sqlmi_workspace_performance/`:

- `schema.sql`
- `bulk_insert.sql` for Azure SQL Managed Instance import from Azure Blob Storage
- one CSV per table
- `dataset_summary.json`

The default dataset models clients, buildings, floors, zones, spaces, design targets, bookings, badge events, hourly utilization, hourly environmental metrics, surveys, maintenance tickets, and daily design outcome scores.
Run `schema.sql` in the target SQL MI database, upload the generated CSVs to Azure Blob Storage, update the placeholders in `bulk_insert.sql`, and then run `bulk_insert.sql`.

To generate and directly populate Azure SQL Managed Instance / Azure SQL DB using the current Azure CLI login:

```powershell
az login
.\.venv\Scripts\python.exe generate_workspace_performance_sqlmi_dataset.py --clean --load-sql
```

The SQL connection settings are read from `.env`:

- `AZURE_SQL_SERVER`
- `AZURE_SQL_DATABASE`
- `AZURE_SQL_DRIVER`
- `AZURE_SQL_SCHEMA`

For a smaller trial run:

```powershell
.\.venv\Scripts\python.exe generate_workspace_performance_sqlmi_dataset.py --output-dir output/sqlmi_workspace_sample --scale 0.1 --days 7 --clean
```

Generate Incremental Changes
----------------------------

```powershell
.\.venv\Scripts\python.exe generate_gps_incremental_changes.py --scale 1.0
```

This reads the saved snapshot, mutates the dataset, and writes incremental files such as:

- `output/open_mirroring_incremental/products/00000000000000000002.parquet`
- `output/open_mirroring_incremental/epds/00000000000000000002.parquet`

The script continues numbering after the initial load so the incremental filenames stay monotonic per table.

Publish to Microsoft Fabric Open Mirroring
------------------------------------------

Your landing zone is already set as the default:

`https://onelake.dfs.fabric.microsoft.com/14e9c878-6b48-4ab6-b393-a9b6af015d3a/d1e820d9-fe3a-41a7-aa70-fa9fa4d90171/Files/LandingZone`

1. Authenticate to Azure / Fabric with a Microsoft Entra identity that has access to the workspace.

```bash
az login
```

2. Dry-run the upload first.

```powershell
.\.venv\Scripts\python.exe publish_to_fabric_open_mirroring.py --source-dir output/open_mirroring_initial --api blob --dry-run
```

3. Upload the initial load.

```powershell
.\.venv\Scripts\python.exe publish_to_fabric_open_mirroring.py --source-dir output/open_mirroring_initial --api blob
```

4. Upload the incremental changes later.

```powershell
.\.venv\Scripts\python.exe publish_to_fabric_open_mirroring.py --source-dir output/open_mirroring_incremental --api blob
```

PowerShell wrappers
-------------------

The PowerShell wrappers automatically use `.venv\Scripts\python.exe` when it exists. You can still override the interpreter with `-PythonCommand`.

Initial load:

```powershell
.\initial_load.ps1
```

Initial load with dependency install and a dry run:

```powershell
.\initial_load.ps1 -InstallRequirements -DryRunPublish
```

Incremental load:

```powershell
.\incremental_load.ps1
```

Incremental load dry run:

```powershell
.\incremental_load.ps1 -DryRunPublish
```

Notes
-----

- The initial load files do not include `__rowMarker__`, which matches Fabric guidance for first-load data.
- Incremental files do include `__rowMarker__` as the final column.
- Every table has a declared primary key in `_metadata.json`, and the generator validates that keys are non-null and unique before writing files.
- Delete rows are emitted for selected certification records to simulate lifecycle changes in source systems.
- The publisher uses `DefaultAzureCredential`, so `az login` is the easiest local auth path.
- The publisher uploads `_metadata.json` before table data files during the initial load.
- The publisher can use either the OneLake Blob or DFS endpoint; `--api blob` is the default recommendation for uploads.

Reference Basis
---------------

The generator logic is informed by:

- Gensler GPS 2.0 concepts such as third-party verified EPDs, GWP thresholds, VOC/IAQ testing, and multi-attribute certifications
- Microsoft Fabric open mirroring requirements for table folders, `_metadata.json`, monotonically increasing file names, and `__rowMarker__` operations
