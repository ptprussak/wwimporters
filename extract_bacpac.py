#!/usr/bin/env python3
"""
extract_bacpac.py — Extract WideWorldImportersDW tables from .bacpac to CSV.

End-to-end script that:
  1. Ensures sqlpackage is installed (via winget)
  2. Imports the .bacpac into a local SQL Server instance
  3. Exports all 14 Dimension/Fact tables to properly-quoted CSV files

Prerequisites:
  - Windows with SQL Server 2022 (Developer or Express) RUNNING
  - Windows Authentication enabled on the SQL Server instance
  - Python 3.10+ with pyodbc:  pip install pyodbc
  - winget available (ships with Windows 11, optional on Windows 10)

Usage:
  python extract_bacpac.py                              # auto-detect bacpac in Downloads
  python extract_bacpac.py --bacpac path/to/file.bacpac # explicit path
  python extract_bacpac.py --skip-import                # skip import, just export CSVs
  python extract_bacpac.py --server MYSERVER\\SQLEXPRESS  # custom SQL Server instance
"""
import argparse, csv, glob, os, shutil, subprocess, sys, time

# ── Defaults ─────────────────────────────────────────────────────
DEFAULT_SERVER = "localhost"
DEFAULT_DB = "WideWorldImportersDW"
BACPAC_PATTERN = "WideWorldImportersDW*.bacpac"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(SCRIPT_DIR, "csv_export")

# ── Table definitions ────────────────────────────────────────────
# (output_name, schema.table, column_list_or_star)
# Explicit column lists skip binary/geography columns (Photo, Location)
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


# ── Step 1: Ensure sqlpackage ────────────────────────────────────
def find_sqlpackage():
    """Locate sqlpackage.exe on the system."""
    # Check PATH first
    sp = shutil.which("sqlpackage") or shutil.which("sqlpackage.exe")
    if sp:
        return sp
    # Check winget install location
    home = os.environ.get("USERPROFILE", os.environ.get("HOME", ""))
    winget_path = os.path.join(
        home, "AppData", "Local", "Microsoft", "WinGet", "Packages")
    if os.path.isdir(winget_path):
        for d in os.listdir(winget_path):
            if "SqlPackage" in d:
                candidate = os.path.join(winget_path, d, "sqlpackage.exe")
                if os.path.isfile(candidate):
                    return candidate
    return None


def ensure_sqlpackage():
    """Install sqlpackage via winget if not found."""
    sp = find_sqlpackage()
    if sp:
        print(f"  Found sqlpackage: {sp}")
        return sp

    print("  sqlpackage not found — installing via winget...")
    result = subprocess.run(
        ["winget", "install", "Microsoft.SqlPackage",
         "--accept-package-agreements", "--accept-source-agreements"],
        capture_output=True, text=True, timeout=180)
    if result.returncode != 0:
        print(f"  ERROR: winget install failed:\n{result.stderr[:500]}")
        sys.exit(1)

    sp = find_sqlpackage()
    if not sp:
        print("  ERROR: sqlpackage installed but not found. Restart your shell and retry.")
        sys.exit(1)
    print(f"  Installed sqlpackage: {sp}")
    return sp


# ── Step 2: Import bacpac ────────────────────────────────────────
def find_bacpac():
    """Auto-detect bacpac in common locations."""
    search_dirs = [
        os.path.join(os.environ.get("USERPROFILE", ""), "Downloads"),
        SCRIPT_DIR,
        os.getcwd(),
    ]
    for d in search_dirs:
        matches = glob.glob(os.path.join(d, BACPAC_PATTERN))
        if matches:
            return matches[0]
    return None


def db_exists(server, db_name):
    """Check if the database already exists on the server."""
    try:
        import pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE=master;"
            f"Trusted_Connection=yes;TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sys.databases WHERE name = ?", (db_name,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    except Exception:
        return False


def import_bacpac(sqlpackage_path, bacpac_path, server, db_name):
    """Import a .bacpac file into SQL Server."""
    if db_exists(server, db_name):
        print(f"  Database [{db_name}] already exists — skipping import.")
        print(f"  (Drop it first if you want a fresh import.)")
        return True

    print(f"  Importing {os.path.basename(bacpac_path)} into [{db_name}]...")
    print(f"  This may take 5-10 minutes...")

    conn_str = (
        f"Server={server};Database={db_name};"
        f"Trusted_Connection=True;TrustServerCertificate=True"
    )
    cmd = [
        sqlpackage_path,
        "/Action:Import",
        f"/SourceFile:{bacpac_path}",
        f"/TargetConnectionString:{conn_str}",
        "/p:CommandTimeout=300",
    ]

    t0 = time.time()
    # On Windows, avoid Git Bash path translation issues
    env = os.environ.copy()
    env["MSYS_NO_PATHCONV"] = "1"

    result = subprocess.run(cmd, capture_output=True, text=True,
                            timeout=900, env=env)
    elapsed = time.time() - t0

    if result.returncode != 0:
        print(f"  ERROR after {elapsed:.0f}s:")
        print(result.stdout[-500:] if result.stdout else "")
        print(result.stderr[-500:] if result.stderr else "")
        sys.exit(1)

    print(f"  Import complete in {elapsed:.0f}s")
    return True


# ── Step 3: Export tables to CSV ─────────────────────────────────
def export_tables(server, db_name, out_dir):
    """Export all 14 tables to CSV using pyodbc."""
    import pyodbc

    os.makedirs(out_dir, exist_ok=True)

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={db_name};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
        f"Connection Timeout=120;"
    )
    print(f"  Connecting to {server}/{db_name}...")
    conn = pyodbc.connect(conn_str)
    conn.timeout = 600

    total = 0
    for name, table, columns in TABLES:
        csv_path = os.path.join(out_dir, f"{name}.csv")
        col_expr = columns.replace("\n", " ").strip() if columns != "*" else "*"
        query = f"SELECT {col_expr} FROM {table}"

        print(f"    {name}...", end=" ", flush=True)
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
                    writer.writerow(
                        [str(v) if v is not None else "" for v in row])
                    count += 1

        elapsed = time.time() - t0
        print(f"{count:>10,} rows  ({elapsed:.1f}s)")
        total += count

    conn.close()
    return total


# ── Main ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Extract WideWorldImportersDW from .bacpac to CSV files.")
    parser.add_argument("--bacpac", help="Path to .bacpac file (auto-detected if omitted)")
    parser.add_argument("--server", default=DEFAULT_SERVER,
                        help=f"SQL Server instance (default: {DEFAULT_SERVER})")
    parser.add_argument("--database", default=DEFAULT_DB,
                        help=f"Target database name (default: {DEFAULT_DB})")
    parser.add_argument("--output", default=OUT_DIR,
                        help=f"CSV output directory (default: {OUT_DIR})")
    parser.add_argument("--skip-import", action="store_true",
                        help="Skip bacpac import, just export CSVs from existing DB")
    args = parser.parse_args()

    print("=" * 60)
    print("WideWorldImportersDW — bacpac to CSV extraction")
    print("=" * 60)

    # Check pyodbc
    try:
        import pyodbc
    except ImportError:
        print("\nERROR: pyodbc not installed. Run:  pip install pyodbc")
        sys.exit(1)

    # Step 1 & 2: Import bacpac (unless --skip-import)
    if not args.skip_import:
        print("\n[1/3] Checking sqlpackage...")
        sp = ensure_sqlpackage()

        print("\n[2/3] Importing bacpac...")
        bacpac = args.bacpac or find_bacpac()
        if not bacpac:
            print("  ERROR: No .bacpac file found.")
            print(f"  Download WideWorldImportersDW-Standard.bacpac from:")
            print(f"    https://github.com/Microsoft/sql-server-samples/releases")
            print(f"  Then re-run, or use --bacpac to specify the path.")
            sys.exit(1)
        print(f"  Using: {bacpac}")
        import_bacpac(sp, bacpac, args.server, args.database)
    else:
        print("\n[1/3] Skipped (--skip-import)")
        print("[2/3] Skipped (--skip-import)")

    # Step 3: Export
    print(f"\n[3/3] Exporting tables to {args.output}...")
    total = export_tables(args.server, args.database, args.output)

    print("\n" + "=" * 60)
    print(f"Done! {total:,} rows exported to {args.output}")
    print("=" * 60)
    print("\nNext steps:")
    print(f"  1. Upload the {args.output} folder to your Fabric Lakehouse Files/ area")
    print(f"  2. Import wwi_load_from_csv.ipynb into Fabric")
    print(f"  3. Attach it to your Lakehouse and Run All")


if __name__ == "__main__":
    main()
