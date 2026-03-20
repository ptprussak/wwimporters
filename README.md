# WideWorldImportersDW → Microsoft Fabric Lakehouse

Load the [WideWorldImportersDW](https://learn.microsoft.com/en-us/sql/samples/wide-world-importers-dw-database-catalog) sample data warehouse into a Microsoft Fabric Lakehouse as Delta tables, then configure Fabric data agents and a Copilot Studio agent to demonstrate how instruction quality affects query accuracy.

## Prerequisites

| Requirement | Details |
|---|---|
| **OS** | Windows 10/11 |
| **SQL Server** | 2019 or 2022, Developer or Express edition, **running** with Windows Authentication |
| **Python** | 3.10+ with `pyodbc` (`pip install pyodbc`) |
| **winget** | Ships with Windows 11; [install on Windows 10](https://learn.microsoft.com/en-us/windows/package-manager/winget/) |
| **Fabric** | A workspace with a Lakehouse named **wwimporters** (any capacity, F2+) |

## Pipeline: End-to-End Setup Order

### Step 1 — Download the bacpac

Download **WideWorldImportersDW-Standard.bacpac** from the [SQL Server samples releases](https://github.com/Microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0) and save it to your `Downloads` folder.

### Step 2 — Extract to CSV

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
python extract_bacpac.py --skip-import            # DB already exists, just export CSVs
python extract_bacpac.py --server MYPC\SQLEXPRESS  # custom SQL Server instance
python extract_bacpac.py --bacpac path/to/file.bacpac  # explicit bacpac path
```

### Step 3 — Upload CSVs to Fabric

1. Open your Fabric workspace at [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Navigate to your Lakehouse → **Files** section
3. Upload all 14 CSV files from `csv_export/` directly into the **Files** root (~100 MB total)

### Step 4 — Run the Fabric notebook

1. In your Fabric workspace, click **Import notebook**
2. Upload `wwi_load_from_csv.ipynb`
3. Open it and attach it to your **wwimporters** Lakehouse
4. Click **Run All**

The notebook reads each CSV from `Files/`, creates typed Delta tables in the `dbo` schema (with underscore column names), adds enrichment layers (SCD2, bridge tables, segment dimension), creates two views, and prints a validation summary.

### Step 5 — Configure Fabric data agents

Create two Fabric data agents that connect to the Lakehouse SQL analytics endpoint:

1. **WWI Simple Agent** — configured per [`agents/simple_agent.md`](agents/simple_agent.md)
   - Agent Instructions only (minimal), Example Queries only (4 basic)
   - Data Source Description and Data Source Instructions left blank
2. **WWI Advanced Agent** — configured per [`agents/advanced_agent.md`](agents/advanced_agent.md)
   - All 4 config sections filled: Agent Instructions, Data Source Description, Data Source Instructions, Example Queries (14 queries)

### Step 6 — (Optional) Configure Copilot Studio agent

Create a Copilot Studio agent that wraps the advanced Fabric data agent:
- Configured per [`agents/copilot_studio_agent.md`](agents/copilot_studio_agent.md)
- Adds a conversational UI layer on top of the Fabric data agent
- Can be published to Teams for end-user access

### Step 7 — Run the demo

Follow [`agents/demo_script.md`](agents/demo_script.md) to run 10 prompts side-by-side against both agents. The simple agent silently produces wrong answers for SCD2, bridge table, and weighted allocation queries. The advanced agent handles them all correctly — same LLM, same data, different instructions.

---

## Alternative Pipeline (Synthetic Data)

A self-contained notebook that creates synthetic data directly in Fabric without needing CSV upload or SQL Server:

```bash
python build_notebook.py          # generates wwi_dw_fabric_load.ipynb
```

Import and run `wwi_dw_fabric_load.ipynb` in Fabric. Useful for quick demos without the bacpac extraction step.

---

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

### Enrichment Layers (added by notebook)

| Table | Description |
|---|---|
| `Dimension_Customer_SCD2` | SCD Type 2 with `Is_Current` flag + ~150 historical rows |
| `Dimension_StockItem_SCD2` | SCD Type 2 with price history (85%/92% tiers) |
| `Bridge_SupplierSubstitution` | M2M: stock items ↔ substitute suppliers |
| `Dimension_CustomerSegment` + `Bridge_CustomerSegment` | Customer segmentation with weighted allocation |

### Views

| View | Description |
|---|---|
| `vw_Customer_Current` | `Dimension_Customer_SCD2` filtered to `Is_Current = true` |
| `vw_StockItem_Current` | `Dimension_StockItem_SCD2` filtered to `Is_Current = true` |

## Repository Contents

```
├── extract_bacpac.py              # Step 2: bacpac → SQL Server → CSV
├── export_tables.py               # Standalone CSV export (used by extract_bacpac.py)
├── build_csv_notebook.py          # Generates wwi_load_from_csv.ipynb
├── wwi_load_from_csv.ipynb        # Step 4: Fabric notebook (CSV → Delta)
├── build_notebook.py              # (alt) Generates synthetic-data notebook
├── wwi_dw_fabric_load.ipynb       # (alt) Self-contained notebook with synthetic data
├── agents/
│   ├── simple_agent.md            # Step 5: Minimal Fabric data agent config
│   ├── advanced_agent.md          # Step 5: Full Fabric data agent config
│   ├── copilot_studio_agent.md    # Step 6: Copilot Studio agent config
│   └── demo_script.md            # Step 7: Side-by-side demo run script
├── csv_export/                    # Generated CSV files (not in git)
└── .gitignore
```

## Notes

- **Notebooks are generated, not hand-edited.** To change `wwi_load_from_csv.ipynb`, edit `build_csv_notebook.py` and re-run it. Same for `wwi_dw_fabric_load.ipynb` via `build_notebook.py`.
- **Column names use underscores** (e.g., `Customer_Key`, `Valid_From`). The CSV export preserves original SQL Server names with spaces, but `write_table` renames all columns to use underscores when saving to Delta.
- **Schema is `dbo`** — tables are created under `[wwimporters].[dbo]`, the default schema for Fabric Lakehouse SQL analytics endpoints.
- **Binary columns skipped:** `Location` (geography), `Photo` (varbinary) columns are excluded from the CSV export.
- **Idempotent:** Set `OVERWRITE_TABLES = True` (default) in the notebook for clean reruns.
