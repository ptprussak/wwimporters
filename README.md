# Fabric Data Agent Demo — Wide World Importers

Two sample **Microsoft Fabric data agents** built on the WideWorldImportersDW data warehouse. A **simple agent** with minimal instructions silently returns wrong answers for complex queries, while an **advanced agent** with full data model documentation handles them correctly — same LLM, same data, different instructions.

The demo proves that **agent instruction quality is the single biggest lever for accuracy** when building data agents over enterprise data warehouses with SCD2 dimensions, bridge tables, and weighted allocations.

---

## Option A: Just Review the Agent Configs

No Fabric workspace needed. The agent configuration files are self-contained and ready to read:

| File | What it is |
|---|---|
| [`agents/simple_agent.md`](agents/simple_agent.md) | Minimal agent config — deliberately under-specified to show failure modes |
| [`agents/advanced_agent.md`](agents/advanced_agent.md) | Full agent config — all 4 GA config sections, covers SCD2, bridge tables, weighted allocations |
| [`agents/copilot_studio_agent.md`](agents/copilot_studio_agent.md) | Copilot Studio wrapper — model selection, settings, suggested prompts, test sets |
| [`agents/demo_script.md`](agents/demo_script.md) | Side-by-side demo run script — 10 prompts with expected outcomes for each agent |

See [`agents/README.md`](agents/README.md) for a detailed guide to the agent configs and demo narrative.

---

## Option B: Full Setup — Run the Demo Yourself

Set up the data warehouse in Fabric, create both agents, and run the side-by-side demo.

### Prerequisites

| Requirement | Details |
|---|---|
| **OS** | Windows 10/11 |
| **SQL Server** | 2019 or 2022, Developer or Express edition, with Windows Authentication |
| **Python** | 3.10+ with `pyodbc` (`pip install pyodbc`) |
| **Fabric** | A workspace with a Lakehouse named **wwimporters** (F2+ capacity) |

### Step 1 — Extract the bacpac to CSV

Download **WideWorldImportersDW-Standard.bacpac** from [SQL Server samples](https://github.com/Microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0), then:

```bash
pip install pyodbc
python extract_bacpac.py
```

This imports the bacpac into local SQL Server and exports 14 tables to `csv_export/` as CSV files.

```bash
# Options
python extract_bacpac.py --skip-import            # DB already exists, just export CSVs
python extract_bacpac.py --server MYPC\SQLEXPRESS  # custom SQL Server instance
```

### Step 2 — Set up the Lakehouse

1. Open your Fabric workspace at [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Create a Lakehouse named **wwimporters** (if it doesn't exist)
3. Navigate to the Lakehouse → **Files** section
4. Upload all 14 CSV files from `csv_export/` directly into the **Files** root (~100 MB total)

### Step 3 — Run the notebook to load data

1. In your Fabric workspace, click **Import notebook**
2. Upload `wwi_load_from_csv.ipynb`
3. Open it and attach it to the **wwimporters** Lakehouse
4. Click **Run All**

The notebook creates 14 typed Delta tables in the `dbo` schema, adds 4 enrichment layers (SCD2 dimensions, bridge tables, customer segments), creates 2 views, and prints a validation summary. Takes about 2 minutes.

### Step 4 — Create the Fabric data agents

Create two agents that connect to the Lakehouse SQL analytics endpoint:

1. **WWI Simple Agent** — configured per [`agents/simple_agent.md`](agents/simple_agent.md)
   - Only Agent Instructions (minimal) and Example Queries (4 basic) are filled
   - Data Source Description and Data Source Instructions left **intentionally blank**

2. **WWI Advanced Agent** — configured per [`agents/advanced_agent.md`](agents/advanced_agent.md)
   - All 4 config sections filled: Agent Instructions, Data Source Description, Data Source Instructions, Example Queries (14 queries covering all patterns)

### Step 5 — (Optional) Create a Copilot Studio agent

Wrap the advanced Fabric data agent in a Copilot Studio agent for a Teams-ready chat interface. Configured per [`agents/copilot_studio_agent.md`](agents/copilot_studio_agent.md).

### Step 6 — Run the demo

Follow [`agents/demo_script.md`](agents/demo_script.md) to run 10 prompts side-by-side against both agents.

---

## What Gets Loaded

### 14 Base Tables (~930K rows)

| Table | Rows | Description |
|---|---:|---|
| `Dimension_Date` | 1,461 | Calendar 2013-2016 |
| `Dimension_City` | 116,295 | Cities with SCD history |
| `Dimension_Customer` | 403 | Customer dimension |
| `Dimension_Employee` | 213 | Employee dimension |
| `Dimension_Payment_Method` | 6 | Payment types |
| `Dimension_Stock_Item` | 672 | Products |
| `Dimension_Supplier` | 28 | Supplier dimension |
| `Dimension_Transaction_Type` | 15 | Transaction categories |
| `Fact_Sale` | 228,265 | Sales transactions |
| `Fact_Order` | 231,412 | Customer orders |
| `Fact_Purchase` | 8,367 | Purchase orders |
| `Fact_Movement` | 236,667 | Stock movements |
| `Fact_Stock_Holding` | 227 | Current inventory |
| `Fact_Transaction` | 99,585 | Financial transactions |

### Enrichment Layers (added by notebook)

| Table/View | Description |
|---|---|
| `Dimension_Customer_SCD2` | SCD Type 2 with ~150 historical rows (buying group changes) |
| `Dimension_StockItem_SCD2` | SCD Type 2 with price history (85%/92%/100% tiers) |
| `Bridge_SupplierSubstitution` | Stock item → primary/substitute supplier network |
| `Dimension_CustomerSegment` + `Bridge_CustomerSegment` | 7 segments with weighted allocation |
| `vw_Customer_Current` | Current-state filter on Customer SCD2 |
| `vw_StockItem_Current` | Current-state filter on StockItem SCD2 |

---

## Repository Contents

```
├── extract_bacpac.py              # bacpac → SQL Server → CSV
├── build_csv_notebook.py          # Generates wwi_load_from_csv.ipynb
├── wwi_load_from_csv.ipynb        # Fabric notebook: CSV → Delta tables + enrichment
├── agents/
│   ├── README.md                  # Guide to agent configs and demo narrative
│   ├── simple_agent.md            # Minimal Fabric data agent config
│   ├── advanced_agent.md          # Full Fabric data agent config
│   ├── copilot_studio_agent.md    # Copilot Studio agent config
│   └── demo_script.md            # Side-by-side demo run script (10 prompts)
├── csv_export/                    # Generated CSV files (~100 MB, not in git)
└── .gitignore
```

## Notes

- **Notebooks are generated, not hand-edited.** Edit `build_csv_notebook.py` and re-run to regenerate `wwi_load_from_csv.ipynb`.
- **Column names use underscores** (e.g., `Customer_Key`, `Valid_From`). The CSV export preserves original SQL Server names with spaces, but the notebook renames all columns when saving to Delta.
- **Schema is `dbo`** — all tables live under `[wwimporters].[dbo]`.
- **Binary columns skipped** — `Location` (geography) and `Photo` (varbinary) are excluded from CSV export.
