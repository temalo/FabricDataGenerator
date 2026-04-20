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
- `publish_to_fabric_open_mirroring.py`
  Uploads a local open mirroring folder tree into a Fabric OneLake landing zone using the ADLS Gen2 compatible SDK.
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

```bash
python3 -m pip install -r requirements.txt
```

Generate Initial Data
---------------------

```bash
python3 generate_gps_initial_dataset.py --scale 1.0
```

This creates:

- `output/open_mirroring_initial/`
- `output/snapshots/`

The initial landing-zone folder contains one folder per table, each with:

- `_metadata.json`
- `00000000000000000001.parquet`

Generate Incremental Changes
----------------------------

```bash
python3 generate_gps_incremental_changes.py --scale 1.0
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

```bash
python3 publish_to_fabric_open_mirroring.py --source-dir output/open_mirroring_initial --dry-run
```

3. Upload the initial load.

```bash
python3 publish_to_fabric_open_mirroring.py --source-dir output/open_mirroring_initial
```

4. Upload the incremental changes later.

```bash
python3 publish_to_fabric_open_mirroring.py --source-dir output/open_mirroring_incremental
```

Notes
-----

- The initial load files do not include `__rowMarker__`, which matches Fabric guidance for first-load data.
- Incremental files do include `__rowMarker__` as the final column.
- Delete rows are emitted for selected certification records to simulate lifecycle changes in source systems.
- The publisher uses `DefaultAzureCredential`, so `az login` is the easiest local auth path.
- The current implementation uploads files recursively and overwrites any matching remote file path.

Reference Basis
---------------

The generator logic is informed by:

- Gensler GPS 2.0 concepts such as third-party verified EPDs, GWP thresholds, VOC/IAQ testing, and multi-attribute certifications
- Microsoft Fabric open mirroring requirements for table folders, `_metadata.json`, monotonically increasing file names, and `__rowMarker__` operations
