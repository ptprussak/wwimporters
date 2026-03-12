"""Export WWI DW tables from local SQL Server to properly-quoted CSV files.

Uses pyodbc + Python csv module for correct quoting of text containing commas.
Skips geography/varbinary columns (Location, Photo).
"""
import pyodbc, csv, os, time

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=WideWorldImportersDW;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=120;"
)
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv_export")
os.makedirs(OUT_DIR, exist_ok=True)

# (output_name, schema.table, columns_or_star)
TABLES = [
    ("Dimension_City", "Dimension.[City]",
     """[City Key],[WWI City ID],[City],[State Province],[Country],[Continent],
        [Sales Territory],[Region],[Subregion],[Latest Recorded Population],
        [Valid From],[Valid To],[Lineage Key]"""),
    ("Dimension_Customer", "Dimension.[Customer]", "*"),
    ("Dimension_Date", "Dimension.[Date]", "*"),
    ("Dimension_Employee", "Dimension.[Employee]",
     """[Employee Key],[WWI Employee ID],[Employee],[Preferred Name],
        [Is Salesperson],[Valid From],[Valid To],[Lineage Key]"""),
    ("Dimension_Payment_Method", "Dimension.[Payment Method]", "*"),
    ("Dimension_Stock_Item", "Dimension.[Stock Item]",
     """[Stock Item Key],[WWI Stock Item ID],[Stock Item],[Color],
        [Selling Package],[Buying Package],[Brand],[Size],[Lead Time Days],
        [Quantity Per Outer],[Is Chiller Stock],[Barcode],[Tax Rate],
        [Unit Price],[Recommended Retail Price],[Typical Weight Per Unit],
        [Valid From],[Valid To],[Lineage Key]"""),
    ("Dimension_Supplier", "Dimension.[Supplier]", "*"),
    ("Dimension_Transaction_Type", "Dimension.[Transaction Type]", "*"),
    ("Fact_Sale", "Fact.[Sale]", "*"),
    ("Fact_Order", "Fact.[Order]", "*"),
    ("Fact_Purchase", "Fact.[Purchase]", "*"),
    ("Fact_Movement", "Fact.[Movement]", "*"),
    ("Fact_Stock_Holding", "Fact.[Stock Holding]", "*"),
    ("Fact_Transaction", "Fact.[Transaction]", "*"),
]


def export_table(conn, name, table, columns):
    csv_path = os.path.join(OUT_DIR, f"{name}.csv")
    col_expr = columns.replace("\n", " ").strip() if columns != "*" else "*"
    query = f"SELECT {col_expr} FROM {table}"

    print(f"  {name}...", end=" ", flush=True)
    t0 = time.time()

    cursor = conn.cursor()
    cursor.execute(query)
    col_names = [desc[0] for desc in cursor.description]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(col_names)
        count = 0
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            for row in rows:
                writer.writerow([str(v) if v is not None else "" for v in row])
                count += 1

    elapsed = time.time() - t0
    print(f"{count:>10,} rows  ({elapsed:.1f}s)")
    return count


if __name__ == "__main__":
    print(f"Connecting to SQL Server...")
    conn = pyodbc.connect(CONN_STR)
    conn.timeout = 600

    print(f"Exporting {len(TABLES)} tables to {OUT_DIR}")
    print("=" * 60)

    total = 0
    for name, table, columns in TABLES:
        n = export_table(conn, name, table, columns)
        total += n

    conn.close()
    print("=" * 60)
    print(f"Done: {total:,} total rows across {len(TABLES)} tables")
