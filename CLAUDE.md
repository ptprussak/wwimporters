# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repo loads the **WideWorldImportersDW** sample data warehouse into a **Microsoft Fabric Lakehouse** as Delta tables, then provides two Fabric data agent configurations (simple vs. advanced) to demonstrate how agent instruction quality affects query accuracy.

The project targets a **demo scenario**: the simple agent silently produces wrong answers for SCD2, bridge table, and weighted allocation queries, while the advanced agent handles them correctly — same LLM, same data, different instructions.

## Architecture

There are two independent data pipelines and a set of demo agent configs:

### Pipeline: bacpac → CSV → Fabric Delta
1. **extract_bacpac.py** — Installs sqlpackage via winget, imports `.bacpac` into local SQL Server, then calls `export_tables.py` to export 14 tables to `csv_export/` as quoted CSV. Windows-only, requires SQL Server with Windows Auth.
2. **build_csv_notebook.py** — Python script that *generates* the Fabric notebook `wwi_load_from_csv.ipynb`. The notebook is not hand-edited; regenerate it by running this script.
3. **wwi_load_from_csv.ipynb** — Generated Fabric PySpark notebook. Reads CSVs from Lakehouse `Files/`, creates typed Delta tables in `dbo` schema with underscore column names, adds 4 enrichment layers (SCD2 dimensions, bridge tables, segment dimension, views).

### Alternative pipeline (synthetic data, self-contained)
- **build_notebook.py** → generates **wwi_dw_fabric_load.ipynb** — a self-contained notebook that creates synthetic data directly in Fabric without needing CSV upload.

### Agent configs (`agents/`)
- **simple_agent.md** — Minimal Fabric data agent config (Agent Instructions + Example Queries only). Deliberately under-specified to show failure modes.
- **advanced_agent.md** — Fully-instrumented config using all 4 GA config sections (Agent Instructions, Data Source Description, Data Source Instructions, Example Queries). Covers SCD2 temporal joins, bridge tables, weighted allocations.
- **copilot_studio_agent.md** — Copilot Studio agent config that wraps the Fabric data agent. Covers model selection, settings, suggested prompts, and test sets.
- **demo_script.md** — Side-by-side demo run script with 10 prompts, expected outcomes, and narrative arc.

## Key Commands

```bash
# Extract bacpac to CSV (Windows, requires SQL Server + pyodbc)
pip install pyodbc
python extract_bacpac.py
python extract_bacpac.py --skip-import          # skip SQL import, just export CSVs
python extract_bacpac.py --server MYPC\SQLEXPRESS

# Regenerate the Fabric notebook from its generator script
python build_csv_notebook.py      # outputs wwi_load_from_csv.ipynb
python build_notebook.py          # outputs wwi_dw_fabric_load.ipynb (synthetic data variant)
```

## Important Patterns

- **Notebooks are generated, not hand-edited.** To change `wwi_load_from_csv.ipynb`, edit `build_csv_notebook.py` and re-run it. Same for `wwi_dw_fabric_load.ipynb` via `build_notebook.py`.
- **Column names use underscores** (e.g., `Customer_Key`, `Valid_From`). CSV export preserves original SQL Server names with spaces, but `write_table` renames all columns to underscores when saving to Delta.
- **Binary columns are excluded** — `Location` (geography) and `Photo` (varbinary) are skipped in CSV export.
- The `csv_export/` directory is gitignored and contains ~100 MB of generated data.
- `export_tables.py` is a standalone CSV exporter but is also called by `extract_bacpac.py` as a subprocess.

## Data Model Concepts (for agent config work)

The enrichment layers added by the notebook are central to the demo:
- **SCD2 tables** (`Dimension_Customer_SCD2`, `Dimension_StockItem_SCD2`) — require temporal joins using `Valid_From`/`Valid_To` ranges
- **Bridge tables** (`Bridge_CustomerSegment`, `Bridge_SupplierSubstitution`) — many-to-many with `Allocation_Weight` for segments and dual-key supplier joins
- **Views** (`vw_Customer_Current`, `vw_StockItem_Current`) — convenience filters on SCD2 tables for current-state queries
