# WideWorldImportersDW → Microsoft Fabric Lakehouse

Load the [WideWorldImportersDW](https://learn.microsoft.com/en-us/sql/samples/wide-world-importers-dw-database-catalog) sample data warehouse into a Microsoft Fabric Lakehouse as Delta tables.

## Prerequisites

| Requirement | Details |
|---|---|
| **OS** | Windows 10/11 |
| **SQL Server** | 2019 or 2022, Developer or Express edition, **running** with Windows Authentication |
| **Python** | 3.10+ with `pyodbc` (`pip install pyodbc`) |
| **winget** | Ships with Windows 11; [install on Windows 10](https://learn.microsoft.com/en-us/windows/package-manager/winget/) |
| **Fabric** | A workspace with a Lakehouse (any capacity) |

## Quick Start

### 1. Download the bacpac

Download **WideWorldImportersDW-Standard.bacpac** from the [SQL Server samples releases](https://github.com/Microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0) and save it to your `Downloads` folder.

### 2. Extract to CSV

```bash
pip install pyodbc
python extract_bacpac.py
```

This will:
- Install `sqlpackage` via winget (if not already installed)
- Import the `.bacpac` into your local SQL Server as `WideWorldImportersDW`
- Export all 14 Dimension/Fact tables to `csv_export/` as properly-quoted CSV files

**Options:**
```bash
python extract_bacpac.py --skip-import          # DB already exists, just export CSVs
python extract_bacpac.py --server MYPC\SQLEXPRESS  # custom SQL Server instance
python extract_bacpac.py --bacpac path/to/file.bacpac  # explicit bacpac path
```

### 3. Upload CSVs to Fabric

1. Open your Fabric workspace at [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Navigate to your Lakehouse → **Files** section
3. Click **Upload** → **Upload folder** → select the `csv_export` folder
4. Wait for all 14 files to upload (~100 MB total)

### 4. Run the Fabric notebook

1. In your Fabric workspace, click **Import notebook**
2. Upload `wwi_load_from_csv.ipynb`
3. Open it and attach it to your Lakehouse
4. Click **Run All**

The notebook reads each CSV from `Files/csv_export/`, creates typed Delta tables in the `wwi` schema, adds enrichment layers, and prints a validation summary.

## What Gets Loaded

### 14 Base Tables

| Table | Rows | Description |
|---|---:|---|
| `Dimension_City` | 116,295 | Cities with SCD history |
| `Dimension_Customer` | 403 | Customer dimension |
| `Dimension_Date` | 1,461 | Calendar 2013-2016 |
| `Dimension_Employee` | 213 | Employee dimension with SCD |
| `Dimension_Payment_Method` | 6 | Payment types |
| `Dimension_Stock_Item` | 672 | Products with SCD history |
| `Dimension_Supplier` | 28 | Supplier dimension |
| `Dimension_Transaction_Type` | 15 | Transaction categories |
| `Fact_Sale` | 228,265 | Sales transactions |
| `Fact_Order` | 231,412 | Customer orders |
| `Fact_Purchase` | 8,367 | Purchase orders |
| `Fact_Movement` | 236,667 | Stock movements |
| `Fact_Stock_Holding` | 227 | Current inventory levels |
| `Fact_Transaction` | 99,585 | Financial transactions |

### 4 Enrichment Layers (added by notebook)

| Table | Description |
|---|---|
| `Dimension_Customer_SCD2` | SCD Type 2 with `Is Current` flag + ~150 historical rows |
| `Dimension_StockItem_SCD2` | SCD Type 2 with price history (85%/92% tiers) |
| `Bridge_SupplierSubstitution` | M2M: stock items ↔ substitute suppliers |
| `Dimension_CustomerSegment` + `Bridge_CustomerSegment` | Customer segmentation with weighted allocation |

Plus views: `vw_Customer_Current`, `vw_StockItem_Current`

## Repository Contents

```
├── extract_bacpac.py          # Step 2: bacpac → SQL Server → CSV
├── build_csv_notebook.py      # Generates the Fabric notebook
├── wwi_load_from_csv.ipynb    # Step 4: Fabric notebook (CSV → Delta)
├── build_notebook.py          # (alt) Generates synthetic-data notebook
├── wwi_dw_fabric_load.ipynb   # (alt) Self-contained notebook with synthetic data
├── export_tables.py           # Standalone CSV export (used by extract_bacpac.py)
├── .gitignore
└── csv_export/                # Generated CSV files (not in git)
    ├── Dimension_City.csv
    ├── Dimension_Customer.csv
    └── ...
```

## Notes

- **Binary columns skipped:** `Location` (geography), `Photo` (varbinary) columns are excluded from the CSV export — they aren't needed for analytics.
- **Column names:** Original SQL Server names with spaces are preserved (e.g. `City Key`, `Valid From`). Fabric Delta tables support this via backtick quoting in SQL.
- **Idempotent:** Set `OVERWRITE_TABLES = True` (default) in the notebook for clean reruns.
