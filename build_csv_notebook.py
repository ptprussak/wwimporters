#!/usr/bin/env python3
"""Generate Fabric notebook that loads WWI DW from CSV files into Delta tables."""
import json, os

cells = []

def md(src):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": src.strip()})

def code(src):
    cells.append({
        "cell_type": "code", "metadata": {},
        "source": src.strip(), "outputs": [], "execution_count": None
    })


# ═══════════════════════════════════════════════════════════════════
#  TITLE
# ═══════════════════════════════════════════════════════════════════
md("""# WideWorldImportersDW \u2192 Fabric Lakehouse

Loads **14 base tables** from CSV files, then adds **4 enrichment layers** as Delta tables.

| Setting | Value |
|---|---|
| Source | `Files/csv_export/*.csv` (uploaded to Lakehouse) |
| Schema | `dbo` |
| Kernel | Fabric Synapse PySpark |

> **Pre-requisite:** Upload the `csv_export/` folder to your Lakehouse **Files** section before running.""")


# ═══════════════════════════════════════════════════════════════════
#  CONFIGURATION & HELPERS
# ═══════════════════════════════════════════════════════════════════
code('''# ── Configuration ────────────────────────────────────────────────
OVERWRITE_TABLES = True          # False = skip tables that already exist
SCHEMA = "dbo"
CSV_ROOT = "Files"               # path inside Lakehouse Files/ area

# ── Imports ──────────────────────────────────────────────────────
from pyspark.sql import functions as F
import random

# ── Schema ───────────────────────────────────────────────────────
# dbo schema exists by default in Fabric Lakehouse; no need to create it

# ── Helpers ──────────────────────────────────────────────────────
_summary = []

def write_table(df, name):
    """Write DataFrame as a Delta table; track row count."""
    # Rename columns: replace spaces with underscores for SQL analytics endpoint
    for col_name in df.columns:
        if " " in col_name:
            df = df.withColumnRenamed(col_name, col_name.replace(" ", "_"))
    full = f"{SCHEMA}.{name}"
    mode = "overwrite" if OVERWRITE_TABLES else "ignore"
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(full)
    n = spark.table(full).count()
    _summary.append((name, n))
    print(f"  \\u2713 {full}: {n:,} rows")

def read_csv(file_name, casts):
    """Read CSV and cast columns. Only non-string columns need to be listed.
    casts: list of (column_name, spark_type_string) tuples.
    """
    df = (spark.read
        .option("header", "true")
        .option("nullValue", "")
        .csv(f"{CSV_ROOT}/{file_name}"))
    for col_name, col_type in casts:
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df

def write_view(sql, view_name):
    """Create or replace a SQL view."""
    spark.sql(sql)
    print(f"  \\u2713 view {SCHEMA}.{view_name} created")

print("Configuration ready.")''')


# ═══════════════════════════════════════════════════════════════════
#  DIMENSION TABLES
# ═══════════════════════════════════════════════════════════════════
md("## Base Dimension Tables")

code('''print("\\u2500\\u2500 Dimension_Date \\u2500\\u2500")
write_table(read_csv("Dimension_Date.csv", [
    ("Date", "date"),
    ("Day Number", "int"),
    ("Day", "int"),
    ("Calendar Month Number", "int"),
    ("Calendar Year", "int"),
    ("Fiscal Month Number", "int"),
    ("Fiscal Year", "int"),
    ("ISO Week Number", "int"),
]), "Dimension_Date")''')

code('''print("\\u2500\\u2500 Dimension_City \\u2500\\u2500")
write_table(read_csv("Dimension_City.csv", [
    ("City Key", "int"),
    ("WWI City ID", "int"),
    ("Latest Recorded Population", "long"),
    ("Valid From", "timestamp"),
    ("Valid To", "timestamp"),
    ("Lineage Key", "int"),
]), "Dimension_City")''')

code('''print("\\u2500\\u2500 Dimension_Customer \\u2500\\u2500")
write_table(read_csv("Dimension_Customer.csv", [
    ("Customer Key", "int"),
    ("WWI Customer ID", "int"),
    ("Valid From", "timestamp"),
    ("Valid To", "timestamp"),
    ("Lineage Key", "int"),
]), "Dimension_Customer")''')

code('''print("\\u2500\\u2500 Dimension_Employee \\u2500\\u2500")
write_table(read_csv("Dimension_Employee.csv", [
    ("Employee Key", "int"),
    ("WWI Employee ID", "int"),
    ("Is Salesperson", "boolean"),
    ("Valid From", "timestamp"),
    ("Valid To", "timestamp"),
    ("Lineage Key", "int"),
]), "Dimension_Employee")''')

code('''print("\\u2500\\u2500 Dimension_Payment_Method \\u2500\\u2500")
write_table(read_csv("Dimension_Payment_Method.csv", [
    ("Payment Method Key", "int"),
    ("WWI Payment Method ID", "int"),
    ("Valid From", "timestamp"),
    ("Valid To", "timestamp"),
    ("Lineage Key", "int"),
]), "Dimension_Payment_Method")''')

code('''print("\\u2500\\u2500 Dimension_Stock_Item \\u2500\\u2500")
write_table(read_csv("Dimension_Stock_Item.csv", [
    ("Stock Item Key", "int"),
    ("WWI Stock Item ID", "int"),
    ("Lead Time Days", "int"),
    ("Quantity Per Outer", "int"),
    ("Is Chiller Stock", "boolean"),
    ("Tax Rate", "decimal(18,3)"),
    ("Unit Price", "decimal(18,2)"),
    ("Recommended Retail Price", "decimal(18,2)"),
    ("Typical Weight Per Unit", "decimal(18,3)"),
    ("Valid From", "timestamp"),
    ("Valid To", "timestamp"),
    ("Lineage Key", "int"),
]), "Dimension_Stock_Item")''')

code('''print("\\u2500\\u2500 Dimension_Supplier \\u2500\\u2500")
write_table(read_csv("Dimension_Supplier.csv", [
    ("Supplier Key", "int"),
    ("WWI Supplier ID", "int"),
    ("Payment Days", "int"),
    ("Valid From", "timestamp"),
    ("Valid To", "timestamp"),
    ("Lineage Key", "int"),
]), "Dimension_Supplier")''')

code('''print("\\u2500\\u2500 Dimension_Transaction_Type \\u2500\\u2500")
write_table(read_csv("Dimension_Transaction_Type.csv", [
    ("Transaction Type Key", "int"),
    ("WWI Transaction Type ID", "int"),
    ("Valid From", "timestamp"),
    ("Valid To", "timestamp"),
    ("Lineage Key", "int"),
]), "Dimension_Transaction_Type")''')


# ═══════════════════════════════════════════════════════════════════
#  FACT TABLES
# ═══════════════════════════════════════════════════════════════════
md("## Fact Tables")

code('''print("\\u2500\\u2500 Fact_Sale \\u2500\\u2500")
write_table(read_csv("Fact_Sale.csv", [
    ("Sale Key", "long"),
    ("City Key", "int"),
    ("Customer Key", "int"),
    ("Bill To Customer Key", "int"),
    ("Stock Item Key", "int"),
    ("Invoice Date Key", "date"),
    ("Delivery Date Key", "date"),
    ("Salesperson Key", "int"),
    ("WWI Invoice ID", "int"),
    ("Quantity", "int"),
    ("Unit Price", "decimal(18,2)"),
    ("Tax Rate", "decimal(18,3)"),
    ("Total Excluding Tax", "decimal(18,2)"),
    ("Tax Amount", "decimal(18,2)"),
    ("Profit", "decimal(18,2)"),
    ("Total Including Tax", "decimal(18,2)"),
    ("Total Dry Items", "int"),
    ("Total Chiller Items", "int"),
    ("Lineage Key", "int"),
]), "Fact_Sale")''')

code('''print("\\u2500\\u2500 Fact_Order \\u2500\\u2500")
write_table(read_csv("Fact_Order.csv", [
    ("Order Key", "long"),
    ("City Key", "int"),
    ("Customer Key", "int"),
    ("Stock Item Key", "int"),
    ("Order Date Key", "date"),
    ("Picked Date Key", "date"),
    ("Salesperson Key", "int"),
    ("Picker Key", "int"),
    ("WWI Order ID", "int"),
    ("WWI Backorder ID", "int"),
    ("Quantity", "int"),
    ("Unit Price", "decimal(18,2)"),
    ("Tax Rate", "decimal(18,3)"),
    ("Total Excluding Tax", "decimal(18,2)"),
    ("Tax Amount", "decimal(18,2)"),
    ("Total Including Tax", "decimal(18,2)"),
    ("Lineage Key", "int"),
]), "Fact_Order")''')

code('''print("\\u2500\\u2500 Fact_Purchase \\u2500\\u2500")
write_table(read_csv("Fact_Purchase.csv", [
    ("Purchase Key", "long"),
    ("Date Key", "date"),
    ("Supplier Key", "int"),
    ("Stock Item Key", "int"),
    ("WWI Purchase Order ID", "int"),
    ("Ordered Outers", "int"),
    ("Ordered Quantity", "int"),
    ("Received Outers", "int"),
    ("Is Order Finalized", "boolean"),
    ("Lineage Key", "int"),
]), "Fact_Purchase")''')

code('''print("\\u2500\\u2500 Fact_Movement \\u2500\\u2500")
write_table(read_csv("Fact_Movement.csv", [
    ("Movement Key", "long"),
    ("Date Key", "date"),
    ("Stock Item Key", "int"),
    ("Customer Key", "int"),
    ("Supplier Key", "int"),
    ("Transaction Type Key", "int"),
    ("WWI Stock Item Transaction ID", "int"),
    ("WWI Invoice ID", "int"),
    ("WWI Purchase Order ID", "int"),
    ("Quantity", "int"),
    ("Lineage Key", "int"),
]), "Fact_Movement")''')

code('''print("\\u2500\\u2500 Fact_Stock_Holding \\u2500\\u2500")
write_table(read_csv("Fact_Stock_Holding.csv", [
    ("Stock Holding Key", "long"),
    ("Stock Item Key", "int"),
    ("Quantity On Hand", "int"),
    ("Last Stocktake Quantity", "int"),
    ("Last Cost Price", "decimal(18,2)"),
    ("Reorder Level", "int"),
    ("Target Stock Level", "int"),
    ("Lineage Key", "int"),
]), "Fact_Stock_Holding")''')

code('''print("\\u2500\\u2500 Fact_Transaction \\u2500\\u2500")
write_table(read_csv("Fact_Transaction.csv", [
    ("Transaction Key", "long"),
    ("Date Key", "date"),
    ("Customer Key", "int"),
    ("Bill To Customer Key", "int"),
    ("Supplier Key", "int"),
    ("Transaction Type Key", "int"),
    ("Payment Method Key", "int"),
    ("WWI Customer Transaction ID", "int"),
    ("WWI Supplier Transaction ID", "int"),
    ("WWI Invoice ID", "int"),
    ("WWI Purchase Order ID", "int"),
    ("Total Excluding Tax", "decimal(18,2)"),
    ("Tax Amount", "decimal(18,2)"),
    ("Total Including Tax", "decimal(18,2)"),
    ("Outstanding Balance", "decimal(18,2)"),
    ("Is Finalized", "boolean"),
    ("Lineage Key", "int"),
]), "Fact_Transaction")''')


# ═══════════════════════════════════════════════════════════════════
#  ENRICHMENT LAYERS
# ═══════════════════════════════════════════════════════════════════
md("## Enrichment Layers")

# ── SCD2: Dimension_Customer_SCD2 ────────────────────────────────
code('''print("\\u2500\\u2500 Enrichment: Dimension_Customer_SCD2 \\u2500\\u2500")

base = spark.table(f"{SCHEMA}.Dimension_Customer")

# Current rows: Is_Current = true
current = (
    base
    .withColumn("Valid_From", F.to_date(F.col("Valid_From")))
    .withColumn("Valid_To", F.to_date(F.lit("9999-12-31")))
    .withColumn("Is_Current", F.lit(True))
)

# Historical rows: first 150 customers with flipped Buying_Group
flip = {"Tailspin Toys": "Wingtip Toys", "Wingtip Toys": "Tailspin Toys", "N/A": "Tailspin Toys"}
flip_expr = F.create_map(*[item for k, v in flip.items() for item in (F.lit(k), F.lit(v))])

historical = (
    base.filter(F.col("Customer_Key") <= 150)
    .withColumn("Buying_Group", flip_expr[F.col("Buying_Group")])
    .withColumn("Valid_From", F.to_date(F.lit("2013-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("2018-12-31")))
    .withColumn("Is_Current", F.lit(False))
)

# Adjust current rows for those 150 customers to start from 2019
current_updated = (
    current.filter(F.col("Customer_Key") <= 150)
    .withColumn("Valid_From", F.to_date(F.lit("2019-01-01")))
)
current_unchanged = current.filter(F.col("Customer_Key") > 150)

scd2 = historical.unionByName(current_updated).unionByName(current_unchanged)
write_table(scd2, "Dimension_Customer_SCD2")''')

# ── SCD2: Dimension_StockItem_SCD2 ──────────────────────────────
code('''print("\\u2500\\u2500 Enrichment: Dimension_StockItem_SCD2 \\u2500\\u2500")

base = spark.table(f"{SCHEMA}.Dimension_Stock_Item")

# Current rows
current = (
    base
    .withColumn("Valid_From", F.to_date(F.lit("2020-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("9999-12-31")))
    .withColumn("Is_Current", F.lit(True))
)

# 2019 historical: prices at 85% (first 100 items)
hist_2019 = (
    base.filter(F.col("Stock_Item_Key") <= 100)
    .withColumn("Unit_Price", F.round(F.col("Unit_Price") * 0.85, 2))
    .withColumn("Recommended_Retail_Price",
        F.round(F.col("Recommended_Retail_Price") * 0.85, 2))
    .withColumn("Valid_From", F.to_date(F.lit("2013-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("2018-12-31")))
    .withColumn("Is_Current", F.lit(False))
)

# 2020 historical: prices at 92% (first 100 items)
hist_2020 = (
    base.filter(F.col("Stock_Item_Key") <= 100)
    .withColumn("Unit_Price", F.round(F.col("Unit_Price") * 0.92, 2))
    .withColumn("Recommended_Retail_Price",
        F.round(F.col("Recommended_Retail_Price") * 0.92, 2))
    .withColumn("Valid_From", F.to_date(F.lit("2019-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("2019-12-31")))
    .withColumn("Is_Current", F.lit(False))
)

scd2 = hist_2019.unionByName(hist_2020).unionByName(current)
write_table(scd2, "Dimension_StockItem_SCD2")''')

# ── Bridge_SupplierSubstitution ──────────────────────────────────
code('''print("\\u2500\\u2500 Enrichment: Bridge_SupplierSubstitution \\u2500\\u2500")

# Get actual key ranges from loaded tables
max_stock = spark.table(f"{SCHEMA}.Dimension_Stock_Item").agg(
    F.max("Stock_Item_Key")).collect()[0][0]
max_supplier = spark.table(f"{SCHEMA}.Dimension_Supplier").agg(
    F.max("Supplier_Key")).collect()[0][0]

rng = random.Random(42)
rel_types = ["Primary", "Secondary", "Emergency"]
rows = []
for i in range(200):
    si = (i % max_stock) + 1
    primary = (i % max_supplier) + 1
    substitute = ((i + rng.randint(1, max_supplier - 1)) % max_supplier) + 1
    if substitute == primary:
        substitute = (primary % max_supplier) + 1
    rows.append((si, primary, substitute, rel_types[i % 3],
                 rng.randint(1, 45), round(rng.uniform(0, 25.0), 2),
                 rng.random() < 0.85))

from pyspark.sql.types import *
schema = StructType([
    StructField("Stock_Item_Key", IntegerType()),
    StructField("Primary_Supplier_Key", IntegerType()),
    StructField("Substitute_Supplier_Key", IntegerType()),
    StructField("Relationship_Type", StringType()),
    StructField("Lead_Time_Days", IntegerType()),
    StructField("Unit_Cost_Premium_Pct", DoubleType()),
    StructField("Is_Active", BooleanType()),
])
write_table(spark.createDataFrame(rows, schema), "Bridge_SupplierSubstitution")''')

# ── Dimension_CustomerSegment + Bridge ───────────────────────────
code('''print("\\u2500\\u2500 Enrichment: CustomerSegment + Bridge \\u2500\\u2500")

from pyspark.sql.types import *

# Dimension_CustomerSegment (7 rows)
segments = ["High Value", "Growth", "Loyal", "Seasonal",
            "At Risk", "New Business", "Tail"]
seg_rows = [(i + 1, segments[i]) for i in range(7)]
seg_schema = StructType([
    StructField("Segment_Key", IntegerType()),
    StructField("Segment", StringType()),
])
write_table(spark.createDataFrame(seg_rows, seg_schema), "Dimension_CustomerSegment")

# Bridge_CustomerSegment — 1-3 segments per customer, weights sum to 1.0
max_cust = spark.table(f"{SCHEMA}.Dimension_Customer").agg(
    F.max("Customer_Key")).collect()[0][0]

rng = random.Random(42)
bridge_rows = []
for ck in range(1, max_cust + 1):
    n = rng.randint(1, 3)
    chosen = rng.sample(range(1, 8), n)
    if n == 1:
        weights = [1.0]
    elif n == 2:
        w = round(rng.uniform(0.3, 0.7), 2)
        weights = [w, round(1.0 - w, 2)]
    else:
        w1 = round(rng.uniform(0.2, 0.5), 2)
        w2 = round(rng.uniform(0.2, round(1.0 - w1 - 0.1, 2)), 2)
        weights = [w1, w2, round(1.0 - w1 - w2, 2)]
    for sk, wt in zip(chosen, weights):
        bridge_rows.append((ck, sk, wt))

br_schema = StructType([
    StructField("Customer_Key", IntegerType()),
    StructField("Segment_Key", IntegerType()),
    StructField("Allocation_Weight", DoubleType()),
])
write_table(spark.createDataFrame(bridge_rows, br_schema), "Bridge_CustomerSegment")''')


# ── Views ─────────────────────────────────────────────────────────
code('''print("\\u2500\\u2500 Views \\u2500\\u2500")

write_view(
    f"CREATE OR REPLACE VIEW {SCHEMA}.vw_Customer_Current AS "
    f"SELECT * FROM {SCHEMA}.Dimension_Customer_SCD2 WHERE Is_Current = true",
    "vw_Customer_Current"
)

write_view(
    f"CREATE OR REPLACE VIEW {SCHEMA}.vw_StockItem_Current AS "
    f"SELECT * FROM {SCHEMA}.Dimension_StockItem_SCD2 WHERE Is_Current = true",
    "vw_StockItem_Current"
)''')


# ═══════════════════════════════════════════════════════════════════
#  SUMMARY
# ═══════════════════════════════════════════════════════════════════
md("## Validation Summary")

code('''print("=" * 60)
print(f"{'Table':<40} {'Rows':>12}")
print("=" * 60)
total = 0
for name, n in _summary:
    print(f"{name:<40} {n:>12,}")
    total += n
print("-" * 60)
print(f"{'TOTAL':<40} {total:>12,}")
print("=" * 60)
print("\\nAll tables loaded successfully.")''')


# ═══════════════════════════════════════════════════════════════════
#  WRITE NOTEBOOK
# ═══════════════════════════════════════════════════════════════════
notebook = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernel_info": {"name": "synapse_pyspark"},
        "kernelspec": {
            "name": "synapse_pyspark",
            "display_name": "Synapse PySpark"
        },
        "language_info": {
            "name": "python",
            "version": "3.10"
        },
        "microsoft": {
            "language": "python"
        }
    },
    "cells": cells
}

out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "wwi_load_from_csv.ipynb")
with open(out, "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=1)

print(f"Created {out}")
print(f"Total cells: {len(cells)} ({sum(1 for c in cells if c['cell_type'] == 'code')} code, "
      f"{sum(1 for c in cells if c['cell_type'] == 'markdown')} markdown)")
