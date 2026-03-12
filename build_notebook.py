#!/usr/bin/env python3
"""Generate WideWorldImportersDW Fabric Lakehouse notebook (.ipynb)."""
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

Loads **14 base tables** (8 dimensions + 6 facts) and **4 enrichment layers** as Delta tables.

| Config | Value |
|---|---|
| Schema | `wwi` |
| Kernel | Fabric Synapse PySpark |
| Overwrite | Controlled by `OVERWRITE_TABLES` flag |""")

# ═══════════════════════════════════════════════════════════════════
#  CONFIGURATION & HELPERS
# ═══════════════════════════════════════════════════════════════════
code('''# ── Configuration ────────────────────────────────────────────────
OVERWRITE_TABLES = True          # False = skip tables that already exist
SCHEMA = "wwi"

# ── Imports ──────────────────────────────────────────────────────
from pyspark.sql.types import *
from pyspark.sql import functions as F
import random, datetime, calendar, itertools

random.seed(42)

# ── Create schema ────────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# ── Helpers ──────────────────────────────────────────────────────
_summary = []

def write_table(df, name):
    """Write DataFrame as a Delta table; track row count."""
    full = f"{SCHEMA}.{name}"
    mode = "overwrite" if OVERWRITE_TABLES else "ignore"
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(full)
    n = spark.table(full).count()
    _summary.append((name, n))
    print(f"  \\u2713 {full}: {n:,} rows")

def write_view(sql, view_name):
    """Create or replace a SQL view."""
    spark.sql(sql)
    print(f"  \\u2713 view {SCHEMA}.{view_name} created")

print("Configuration ready.")''')

# ═══════════════════════════════════════════════════════════════════
#  DIMENSION_DATE
# ═══════════════════════════════════════════════════════════════════
md("## Base Dimension Tables")

code('''print("\\u2500\\u2500 Dimension_Date \\u2500\\u2500")
rows = []
for yr in range(2013, 2017):
    for mo in range(1, 13):
        for dy in range(1, calendar.monthrange(yr, mo)[1] + 1):
            d = datetime.date(yr, mo, dy)
            fy = yr + 1 if mo >= 7 else yr
            fm = ((mo - 7) % 12) + 1
            rows.append((
                d, d.timetuple().tm_yday, d.strftime("%A"), d.strftime("%B"),
                d.strftime("%b"), mo, f"CY{yr}-{d.strftime('%b')}", yr, f"CY{yr}",
                fm, f"FY{fy}-{d.strftime('%b')}", fy, f"FY{fy}",
                d.isocalendar()[1],
            ))

schema = StructType([
    StructField("Date", DateType()),
    StructField("Day_Number", IntegerType()),
    StructField("Day", StringType()),
    StructField("Month", StringType()),
    StructField("Short_Month", StringType()),
    StructField("Calendar_Month_Number", IntegerType()),
    StructField("Calendar_Month_Label", StringType()),
    StructField("Calendar_Year", IntegerType()),
    StructField("Calendar_Year_Label", StringType()),
    StructField("Fiscal_Month_Number", IntegerType()),
    StructField("Fiscal_Month_Label", StringType()),
    StructField("Fiscal_Year", IntegerType()),
    StructField("Fiscal_Year_Label", StringType()),
    StructField("ISO_Week_Number", IntegerType()),
])
write_table(spark.createDataFrame(rows, schema), "Dimension_Date")''')

# ═══════════════════════════════════════════════════════════════════
#  SMALL DIMENSIONS (Employee, Payment Method, Transaction Type)
# ═══════════════════════════════════════════════════════════════════
code('''print("\\u2500\\u2500 Small Dimensions \\u2500\\u2500")
ts0 = datetime.datetime(2013, 1, 1)
ts9 = datetime.datetime(9999, 12, 31, 23, 59, 59)

# ── Dimension_Employee (13 rows) ─────────────────────────────────
emp_names = [
    "Kayla Woodcock", "Hudson Onslow", "Isabella Rupp", "Eva Muir",
    "Sophia Hinton", "Amy Trefl", "Anthony Grosse", "Piper Koch",
    "Lily Code", "Archer Lamble", "Jack Potter", "Taj Shand", "Jai Shand",
]
is_sales = [False, False, True, True, True, True, True, True, False, False, False, False, False]
emp_rows = [
    (i + 1, i + 1, emp_names[i], emp_names[i].split()[0], is_sales[i], ts0, ts9, 0)
    for i in range(13)
]
emp_schema = StructType([
    StructField("Employee_Key", IntegerType()),
    StructField("WWI_Employee_ID", IntegerType()),
    StructField("Employee", StringType()),
    StructField("Preferred_Name", StringType()),
    StructField("Is_Salesperson", BooleanType()),
    StructField("Valid_From", TimestampType()),
    StructField("Valid_To", TimestampType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(emp_rows, emp_schema), "Dimension_Employee")

# ── Dimension_Payment_Method (6 rows) ────────────────────────────
pm = ["Cash", "Check", "EFT", "Credit Card", "Wire Transfer", "Debit Card"]
pm_rows = [(i + 1, i + 1, pm[i], ts0, ts9, 0) for i in range(6)]
pm_schema = StructType([
    StructField("Payment_Method_Key", IntegerType()),
    StructField("WWI_Payment_Method_ID", IntegerType()),
    StructField("Payment_Method", StringType()),
    StructField("Valid_From", TimestampType()),
    StructField("Valid_To", TimestampType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(pm_rows, pm_schema), "Dimension_Payment_Method")

# ── Dimension_Transaction_Type (15 rows) ─────────────────────────
tt = [
    "Customer Invoice", "Customer Credit Note", "Customer Payment Received",
    "Customer Refund", "Supplier Invoice", "Supplier Credit Note",
    "Supplier Payment Issued", "Stock Transfer", "Stock Adjustment at Stocktake",
    "Stock Shrinkage", "Stock Issue", "Stock Receipt", "Stock Return",
    "Purchase Return", "Other",
]
tt_rows = [(i + 1, i + 1, tt[i], ts0, ts9, 0) for i in range(15)]
tt_schema = StructType([
    StructField("Transaction_Type_Key", IntegerType()),
    StructField("WWI_Transaction_Type_ID", IntegerType()),
    StructField("Transaction_Type", StringType()),
    StructField("Valid_From", TimestampType()),
    StructField("Valid_To", TimestampType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(tt_rows, tt_schema), "Dimension_Transaction_Type")''')

# ═══════════════════════════════════════════════════════════════════
#  DIMENSION_CITY (28,798 rows)
# ═══════════════════════════════════════════════════════════════════
code('''print("\\u2500\\u2500 Dimension_City \\u2500\\u2500")
ts0 = datetime.datetime(2013, 1, 1)
ts9 = datetime.datetime(9999, 12, 31, 23, 59, 59)

A = [
    "Spring", "Crystal", "Silver", "Golden", "Shadow", "Sunset", "River",
    "Mountain", "Cedar", "Maple", "Oak", "Pine", "Willow", "Coral", "Amber",
    "Harbor", "Eagle", "Bear", "Wolf", "Fox", "Deer", "Hawk", "Falcon",
    "Storm", "Thunder", "Cloud", "Rain", "Snow", "Star", "Moon",
]
B = [
    "field", "brook", "wood", "dale", "creek", "ridge", "view", "haven",
    "glen", "crest", "vale", "ford", "port", "side", "bury", "hill",
    "ton", "gate", "well", "cliff", "stone", "bend", "falls", "grove",
    "park", "hollow", "shore", "bridge", "land", "shire", "mont", "bluff",
    "point", "crossing", "junction", "landing", "springs", "mills",
    "center", "heights",
]
P = [
    "", "North ", "South ", "East ", "West ", "New ", "Old ", "Lake ",
    "Port ", "Fort ", "Grand ", "Little ", "Big ", "Upper ", "Lower ",
    "Royal ", "Cape ", "Bay ", "Fair ", "Saint ", "San ", "Mid ",
    "Alta ", "Del ", "Mount ",
]
city_names = [
    f"{p}{a}{b}" for p, a, b in
    itertools.islice(itertools.product(P, A, B), 28798)
]

us_states = [
    ("Alabama","AL"), ("Alaska","AK"), ("Arizona","AZ"), ("Arkansas","AR"),
    ("California","CA"), ("Colorado","CO"), ("Connecticut","CT"), ("Delaware","DE"),
    ("Florida","FL"), ("Georgia","GA"), ("Hawaii","HI"), ("Idaho","ID"),
    ("Illinois","IL"), ("Indiana","IN"), ("Iowa","IA"), ("Kansas","KS"),
    ("Kentucky","KY"), ("Louisiana","LA"), ("Maine","ME"), ("Maryland","MD"),
    ("Massachusetts","MA"), ("Michigan","MI"), ("Minnesota","MN"), ("Mississippi","MS"),
    ("Missouri","MO"), ("Montana","MT"), ("Nebraska","NE"), ("Nevada","NV"),
    ("New Hampshire","NH"), ("New Jersey","NJ"), ("New Mexico","NM"), ("New York","NY"),
    ("North Carolina","NC"), ("North Dakota","ND"), ("Ohio","OH"), ("Oklahoma","OK"),
    ("Oregon","OR"), ("Pennsylvania","PA"), ("Rhode Island","RI"), ("South Carolina","SC"),
    ("South Dakota","SD"), ("Tennessee","TN"), ("Texas","TX"), ("Utah","UT"),
    ("Vermont","VT"), ("Virginia","VA"), ("Washington","WA"), ("West Virginia","WV"),
    ("Wisconsin","WI"), ("Wyoming","WY"),
]

territories = {
    "Southeast": ["FL","GA","SC","NC","VA","AL","MS","TN","KY","LA","AR"],
    "Great Lakes": ["IL","IN","MI","OH","WI","MN"],
    "Mideast": ["NY","NJ","PA","DE","MD","CT"],
    "New England": ["MA","ME","NH","VT","RI"],
    "Plains": ["IA","KS","MO","NE","ND","SD","OK"],
    "Rocky Mountain": ["CO","ID","MT","UT","WY","NV"],
    "Southwest": ["TX","AZ","NM"],
    "Far West": ["CA","OR","WA","HI","AK"],
}
st_terr = {}
for t, sts in territories.items():
    for s in sts:
        st_terr[s] = t

intl = [
    ("Canada","North America","Americas"), ("Mexico","North America","Americas"),
    ("Brazil","South America","Americas"), ("Argentina","South America","Americas"),
    ("United Kingdom","Northern Europe","Europe"), ("Germany","Western Europe","Europe"),
    ("France","Western Europe","Europe"), ("Spain","Southern Europe","Europe"),
    ("Italy","Southern Europe","Europe"), ("Australia","Oceania","Oceania"),
    ("Japan","Eastern Asia","Asia"), ("China","Eastern Asia","Asia"),
    ("India","Southern Asia","Asia"), ("South Africa","Southern Africa","Africa"),
]

rng = random.Random(42)
rows = []
for i, name in enumerate(city_names):
    k = i + 1
    if i < 24000:
        sn, sc = us_states[i % 50]
        rows.append((k, k, name, sn, "United States", "North America",
                      st_terr.get(sc, "Southeast"), "Americas", sn,
                      rng.randint(500, 2_000_000), ts0, ts9, 0))
    else:
        idx = (i - 24000) % len(intl)
        country, subregion, continent = intl[idx]
        rows.append((k, k, name, country, country, continent,
                      "External", continent, subregion,
                      rng.randint(500, 2_000_000), ts0, ts9, 0))

city_schema = StructType([
    StructField("City_Key", IntegerType()),
    StructField("WWI_City_ID", IntegerType()),
    StructField("City", StringType()),
    StructField("State_Province", StringType()),
    StructField("Country", StringType()),
    StructField("Continent", StringType()),
    StructField("Sales_Territory", StringType()),
    StructField("Region", StringType()),
    StructField("Subregion", StringType()),
    StructField("Latest_Recorded_Population", LongType()),
    StructField("Valid_From", TimestampType()),
    StructField("Valid_To", TimestampType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(rows, city_schema), "Dimension_City")''')

# ═══════════════════════════════════════════════════════════════════
#  DIMENSION_CUSTOMER (408 rows)
# ═══════════════════════════════════════════════════════════════════
code('''print("\\u2500\\u2500 Dimension_Customer \\u2500\\u2500")
ts0 = datetime.datetime(2013, 1, 1)
ts9 = datetime.datetime(9999, 12, 31, 23, 59, 59)

categories = [
    "Novelty Shop", "Gift Store", "Supermarket", "Computer Store",
    "Kiosk", "Department Store", "Toy Store", "General Retailer",
]
buying_groups = ["Tailspin Toys", "Wingtip Toys", "N/A"]
parent_names = [
    "Tailspin Toys", "Wingtip Toys", "Contoso Ltd", "Fabrikam Inc",
    "Northwind Traders", "Adventure Works", "Trey Research",
    "Graphic Design Institute", "Woodgrove Bank", "Proseware Inc",
    "Datum Corporation", "VanArsdel Ltd", "Coho Winery", "Lucerne Publishing",
    "Alpine Ski House", "Consolidated Messenger", "Fourth Coffee",
    "Litware Inc", "Margie Travel", "Relecloud",
]
rng = random.Random(42)
cust_rows = []
for i in range(408):
    k = i + 1
    parent = parent_names[i % len(parent_names)]
    if i < len(parent_names):
        name = f"{parent} (Head Office)"
    else:
        name = f"{parent} (Store #{k})"
    bill_to = f"{parent} (Head Office)"
    cat = categories[i % len(categories)]
    bg = buying_groups[i % len(buying_groups)]
    contact = f"{parent.split()[0]} Contact-{k}"
    postal = f"{10000 + rng.randint(0, 89999):05d}"
    cust_rows.append((k, k, name, bill_to, cat, bg, contact, postal, ts0, ts9, 0))

cust_schema = StructType([
    StructField("Customer_Key", IntegerType()),
    StructField("WWI_Customer_ID", IntegerType()),
    StructField("Customer", StringType()),
    StructField("Bill_To_Customer", StringType()),
    StructField("Category", StringType()),
    StructField("Buying_Group", StringType()),
    StructField("Primary_Contact", StringType()),
    StructField("Postal_Code", StringType()),
    StructField("Valid_From", TimestampType()),
    StructField("Valid_To", TimestampType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(cust_rows, cust_schema), "Dimension_Customer")''')

# ═══════════════════════════════════════════════════════════════════
#  DIMENSION_STOCK_ITEM (228 rows)
# ═══════════════════════════════════════════════════════════════════
code('''print("\\u2500\\u2500 Dimension_Stock_Item \\u2500\\u2500")
ts0 = datetime.datetime(2013, 1, 1)
ts9 = datetime.datetime(9999, 12, 31, 23, 59, 59)

item_types = [
    ("USB food flash drive", "USB Novelties"),
    ("USB missile launcher", "USB Novelties"),
    ("USB people set", "USB Novelties"),
    ("Novelty chili chocolates", "Chocolate"),
    ("RC stock car", "Computing Novelties"),
    ("Ride on big wheel", "Furry Footwear"),
    ("Developer joke mug", "Mugs"),
    ("Superhero action figure", "Computing Novelties"),
    ("Bluetooth dinosaur", "Novelty Items"),
    ("AI unicorn plushie", "Novelty Items"),
    ("3 kg Bag of Cats", "Novelty Items"),
    ("10 mm Anti-static bubble wrap", "Packaging Materials"),
    ("20 mm Double sided bubble wrap", "Packaging Materials"),
    ("Air cushion machine", "Packaging Materials"),
    ("DBA joke mug", "Mugs"),
    ("Halloween zombie  mask", "Novelty Items"),
]
colors = [
    "Red", "Blue", "Green", "Yellow", "Black", "White", "Gray", "Orange",
    "Purple", "Pink", "Brown", "Navy", "Teal", "Gold", "Silver",
]
sell_pkg = ["Each", "Pair", "Carton", "Packet", "Box", "Bag"]
rng = random.Random(42)
stock_rows = []
for i in range(228):
    k = i + 1
    base_name, brand = item_types[i % len(item_types)]
    color = colors[i % len(colors)]
    full_name = f"{base_name} ({color})" if i < 200 else f"{base_name} v{i}"
    up = round(rng.uniform(1.0, 500.0), 2)
    rrp = round(up * rng.uniform(1.1, 1.5), 2)
    stock_rows.append((
        k, k, full_name, color,
        sell_pkg[i % len(sell_pkg)], sell_pkg[(i + 1) % len(sell_pkg)],
        brand, f"{rng.randint(1,50)}",
        rng.randint(1, 30), rng.randint(1, 50),
        i % 5 == 0,
        f"{4000000000000 + k}",
        float(15), float(up), float(rrp),
        round(rng.uniform(0.1, 25.0), 3),
        ts0, ts9, 0,
    ))

stock_schema = StructType([
    StructField("Stock_Item_Key", IntegerType()),
    StructField("WWI_Stock_Item_ID", IntegerType()),
    StructField("Stock_Item", StringType()),
    StructField("Color", StringType()),
    StructField("Selling_Package", StringType()),
    StructField("Buying_Package", StringType()),
    StructField("Brand", StringType()),
    StructField("Size", StringType()),
    StructField("Lead_Time_Days", IntegerType()),
    StructField("Quantity_Per_Outer", IntegerType()),
    StructField("Is_Chiller_Stock", BooleanType()),
    StructField("Barcode", StringType()),
    StructField("Tax_Rate", DoubleType()),
    StructField("Unit_Price", DoubleType()),
    StructField("Recommended_Retail_Price", DoubleType()),
    StructField("Typical_Weight_Per_Unit", DoubleType()),
    StructField("Valid_From", TimestampType()),
    StructField("Valid_To", TimestampType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(stock_rows, stock_schema), "Dimension_Stock_Item")''')

# ═══════════════════════════════════════════════════════════════════
#  DIMENSION_SUPPLIER (31 rows)
# ═══════════════════════════════════════════════════════════════════
code('''print("\\u2500\\u2500 Dimension_Supplier \\u2500\\u2500")
ts0 = datetime.datetime(2013, 1, 1)
ts9 = datetime.datetime(9999, 12, 31, 23, 59, 59)

sup_names = [
    ("Fabrikam Inc", "Novelty Goods Supplier"),
    ("Litware Inc", "Novelty Goods Supplier"),
    ("Nod Publishers", "Novelty Goods Supplier"),
    ("Contoso Ltd", "Packaging Supplier"),
    ("Northwind Traders", "Toy Supplier"),
    ("A Datum Corporation", "Novelty Goods Supplier"),
    ("Trey Research", "Novelty Goods Supplier"),
    ("Graphic Design Institute", "Packaging Supplier"),
    ("Woodgrove Bank", "Novelty Goods Supplier"),
    ("Proseware Inc", "Toy Supplier"),
    ("Coho Winery", "Novelty Goods Supplier"),
    ("Lucerne Publishing", "Novelty Goods Supplier"),
    ("Alpine Ski House", "Toy Supplier"),
    ("Consolidated Messenger", "Packaging Supplier"),
    ("Fourth Coffee", "Novelty Goods Supplier"),
    ("Humongous Insurance", "Novelty Goods Supplier"),
    ("Margie Travel", "Novelty Goods Supplier"),
    ("Relecloud", "Packaging Supplier"),
    ("Tailspin Toys", "Toy Supplier"),
    ("Wingtip Toys", "Toy Supplier"),
    ("Wide World Importers", "Novelty Goods Supplier"),
    ("VanArsdel Ltd", "Toy Supplier"),
    ("Adventure Works", "Novelty Goods Supplier"),
    ("Blue Yonder Airlines", "Packaging Supplier"),
    ("City Power & Light", "Novelty Goods Supplier"),
    ("Coho Vineyard", "Novelty Goods Supplier"),
    ("Southridge Video", "Toy Supplier"),
    ("The Phone Company", "Novelty Goods Supplier"),
    ("Munson Pickles", "Novelty Goods Supplier"),
    ("Liberty Cabinets", "Packaging Supplier"),
    ("Best For You Organics", "Novelty Goods Supplier"),
]
rng = random.Random(42)
sup_rows = []
for i in range(31):
    k = i + 1
    name, cat = sup_names[i]
    sup_rows.append((
        k, k, name, cat,
        f"{name.split()[0]} Sales",
        f"SUP-{k:04d}",
        rng.randint(7, 60),
        f"{10000 + rng.randint(0, 89999):05d}",
        ts0, ts9, 0,
    ))

sup_schema = StructType([
    StructField("Supplier_Key", IntegerType()),
    StructField("WWI_Supplier_ID", IntegerType()),
    StructField("Supplier", StringType()),
    StructField("Category", StringType()),
    StructField("Primary_Contact", StringType()),
    StructField("Supplier_Reference", StringType()),
    StructField("Payment_Days", IntegerType()),
    StructField("Postal_Code", StringType()),
    StructField("Valid_From", TimestampType()),
    StructField("Valid_To", TimestampType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(sup_rows, sup_schema), "Dimension_Supplier")''')

# ═══════════════════════════════════════════════════════════════════
#  FACT TABLES (Spark-native generation for volume)
# ═══════════════════════════════════════════════════════════════════
md("## Fact Tables")

# ── Fact_Sale ────────────────────────────────────────────────────
code('''print("\\u2500\\u2500 Fact_Sale \\u2500\\u2500")
packages = F.array(*[F.lit(p) for p in ["Each", "Pair", "Carton", "Packet"]])

df = (
    spark.range(1, 228266).toDF("Sale_Key")
    .withColumn("City_Key", (F.rand(seed=1) * 28798 + 1).cast("int"))
    .withColumn("Customer_Key", (F.rand(seed=2) * 408 + 1).cast("int"))
    .withColumn("Bill_To_Customer_Key", F.col("Customer_Key"))
    .withColumn("Stock_Item_Key", (F.rand(seed=3) * 228 + 1).cast("int"))
    .withColumn("Invoice_Date_Key",
        F.date_add(F.lit("2013-01-01"), (F.rand(seed=4) * 1461).cast("int")))
    .withColumn("Delivery_Date_Key",
        F.date_add(F.col("Invoice_Date_Key"), (F.rand(seed=5) * 14 + 1).cast("int")))
    .withColumn("Salesperson_Key", (F.rand(seed=6) * 6 + 3).cast("int"))
    .withColumn("WWI_Invoice_ID", F.col("Sale_Key").cast("int"))
    .withColumn("Description",
        F.concat(F.lit("Sale of item "), F.col("Stock_Item_Key").cast("string")))
    .withColumn("_pr", F.rand(seed=7))
    .withColumn("Package",
        F.when(F.col("_pr") < 0.4, "Each")
         .when(F.col("_pr") < 0.7, "Pair")
         .when(F.col("_pr") < 0.9, "Carton")
         .otherwise("Packet"))
    .withColumn("Quantity", (F.rand(seed=8) * 99 + 1).cast("int"))
    .withColumn("Unit_Price", F.round(F.rand(seed=9) * 200 + 1, 2))
    .withColumn("Tax_Rate", F.lit(15.0))
    .withColumn("Total_Excluding_Tax",
        F.round(F.col("Quantity") * F.col("Unit_Price"), 2))
    .withColumn("Tax_Amount",
        F.round(F.col("Total_Excluding_Tax") * 0.15, 2))
    .withColumn("Profit",
        F.round(F.col("Total_Excluding_Tax") * (F.rand(seed=10) * 0.3 + 0.05), 2))
    .withColumn("Total_Including_Tax",
        F.round(F.col("Total_Excluding_Tax") + F.col("Tax_Amount"), 2))
    .withColumn("Total_Dry_Items",
        F.when(F.rand(seed=11) > 0.2, F.col("Quantity")).otherwise(F.lit(0)))
    .withColumn("Total_Chiller_Items",
        F.col("Quantity") - F.col("Total_Dry_Items"))
    .withColumn("Lineage_Key", F.lit(1))
    .drop("_pr")
)
write_table(df, "Fact_Sale")''')

# ── Fact_Order ───────────────────────────────────────────────────
code('''print("\\u2500\\u2500 Fact_Order \\u2500\\u2500")
df = (
    spark.range(1, 231413).toDF("Order_Key")
    .withColumn("City_Key", (F.rand(seed=20) * 28798 + 1).cast("int"))
    .withColumn("Customer_Key", (F.rand(seed=21) * 408 + 1).cast("int"))
    .withColumn("Stock_Item_Key", (F.rand(seed=22) * 228 + 1).cast("int"))
    .withColumn("Order_Date_Key",
        F.date_add(F.lit("2013-01-01"), (F.rand(seed=23) * 1461).cast("int")))
    .withColumn("Picked_Date_Key",
        F.date_add(F.col("Order_Date_Key"), (F.rand(seed=24) * 7 + 1).cast("int")))
    .withColumn("Salesperson_Key", (F.rand(seed=25) * 6 + 3).cast("int"))
    .withColumn("Picker_Key", (F.rand(seed=26) * 13 + 1).cast("int"))
    .withColumn("WWI_Order_ID", F.col("Order_Key").cast("int"))
    .withColumn("WWI_Backorder_ID",
        F.when(F.rand(seed=27) < 0.05, F.col("Order_Key").cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("Description",
        F.concat(F.lit("Order for item "), F.col("Stock_Item_Key").cast("string")))
    .withColumn("_pr", F.rand(seed=28))
    .withColumn("Package",
        F.when(F.col("_pr") < 0.4, "Each")
         .when(F.col("_pr") < 0.7, "Pair")
         .when(F.col("_pr") < 0.9, "Carton")
         .otherwise("Packet"))
    .withColumn("Quantity", (F.rand(seed=29) * 99 + 1).cast("int"))
    .withColumn("Unit_Price", F.round(F.rand(seed=30) * 200 + 1, 2))
    .withColumn("Tax_Rate", F.lit(15.0))
    .withColumn("Total_Excluding_Tax",
        F.round(F.col("Quantity") * F.col("Unit_Price"), 2))
    .withColumn("Tax_Amount",
        F.round(F.col("Total_Excluding_Tax") * 0.15, 2))
    .withColumn("Total_Including_Tax",
        F.round(F.col("Total_Excluding_Tax") + F.col("Tax_Amount"), 2))
    .withColumn("Lineage_Key", F.lit(1))
    .drop("_pr")
)
write_table(df, "Fact_Order")''')

# ── Fact_Purchase ────────────────────────────────────────────────
code('''print("\\u2500\\u2500 Fact_Purchase \\u2500\\u2500")
df = (
    spark.range(1, 10552).toDF("Purchase_Key")
    .withColumn("Date_Key",
        F.date_add(F.lit("2013-01-01"), (F.rand(seed=40) * 1461).cast("int")))
    .withColumn("Supplier_Key", (F.rand(seed=41) * 31 + 1).cast("int"))
    .withColumn("Stock_Item_Key", (F.rand(seed=42) * 228 + 1).cast("int"))
    .withColumn("WWI_Purchase_Order_ID", F.col("Purchase_Key").cast("int"))
    .withColumn("Ordered_Outers", (F.rand(seed=43) * 20 + 1).cast("int"))
    .withColumn("Ordered_Quantity",
        F.col("Ordered_Outers") * (F.rand(seed=44) * 10 + 5).cast("int"))
    .withColumn("Received_Outers",
        F.when(F.rand(seed=45) < 0.9, F.col("Ordered_Outers"))
         .otherwise((F.col("Ordered_Outers") * 0.8).cast("int")))
    .withColumn("_pr", F.rand(seed=46))
    .withColumn("Package",
        F.when(F.col("_pr") < 0.4, "Each")
         .when(F.col("_pr") < 0.7, "Carton")
         .otherwise("Packet"))
    .withColumn("Is_Order_Finalized",
        F.when(F.rand(seed=47) < 0.85, F.lit(True)).otherwise(F.lit(False)))
    .withColumn("Lineage_Key", F.lit(1))
    .drop("_pr")
)
write_table(df, "Fact_Purchase")''')

# ── Fact_Movement ────────────────────────────────────────────────
code('''print("\\u2500\\u2500 Fact_Movement \\u2500\\u2500")
df = (
    spark.range(1, 253023).toDF("Movement_Key")
    .withColumn("Date_Key",
        F.date_add(F.lit("2013-01-01"), (F.rand(seed=50) * 1461).cast("int")))
    .withColumn("Stock_Item_Key", (F.rand(seed=51) * 228 + 1).cast("int"))
    .withColumn("Customer_Key",
        F.when(F.rand(seed=52) < 0.7, (F.rand(seed=53) * 408 + 1).cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("Supplier_Key",
        F.when(F.col("Customer_Key").isNull(), (F.rand(seed=54) * 31 + 1).cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("Transaction_Type_Key", (F.rand(seed=55) * 15 + 1).cast("int"))
    .withColumn("WWI_Stock_Item_Transaction_ID", F.col("Movement_Key").cast("int"))
    .withColumn("WWI_Invoice_ID",
        F.when(F.col("Customer_Key").isNotNull(), F.col("Movement_Key").cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("WWI_Purchase_Order_ID",
        F.when(F.col("Supplier_Key").isNotNull(), F.col("Movement_Key").cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("Quantity", (F.rand(seed=56) * 200 - 100).cast("int"))
    .withColumn("Lineage_Key", F.lit(1))
)
write_table(df, "Fact_Movement")''')

# ── Fact_Stock_Holding ───────────────────────────────────────────
code('''print("\\u2500\\u2500 Fact_Stock_Holding \\u2500\\u2500")
rng = random.Random(42)
sh_rows = []
bins = ["A", "B", "C", "D", "E", "F", "G", "H"]
for i in range(443):
    k = i + 1
    si = (i % 228) + 1
    qoh = rng.randint(0, 5000)
    sh_rows.append((
        k, si, qoh,
        f"{bins[i % len(bins)]}-{rng.randint(1,20):02d}-{rng.randint(1,10)}",
        rng.randint(0, qoh),
        round(rng.uniform(0.5, 400.0), 2),
        rng.randint(10, 200),
        rng.randint(50, 1000),
        0,
    ))

sh_schema = StructType([
    StructField("Stock_Holding_Key", LongType()),
    StructField("Stock_Item_Key", IntegerType()),
    StructField("Quantity_On_Hand", IntegerType()),
    StructField("Bin_Location", StringType()),
    StructField("Last_Stocktake_Quantity", IntegerType()),
    StructField("Last_Cost_Price", DoubleType()),
    StructField("Reorder_Level", IntegerType()),
    StructField("Target_Stock_Level", IntegerType()),
    StructField("Lineage_Key", IntegerType()),
])
write_table(spark.createDataFrame(sh_rows, sh_schema), "Fact_Stock_Holding")''')

# ── Fact_Transaction ─────────────────────────────────────────────
code('''print("\\u2500\\u2500 Fact_Transaction \\u2500\\u2500")
df = (
    spark.range(1, 415455).toDF("Transaction_Key")
    .withColumn("Date_Key",
        F.date_add(F.lit("2013-01-01"), (F.rand(seed=60) * 1461).cast("int")))
    .withColumn("_is_cust", F.rand(seed=61) < 0.6)
    .withColumn("Customer_Key",
        F.when(F.col("_is_cust"), (F.rand(seed=62) * 408 + 1).cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("Bill_To_Customer_Key", F.col("Customer_Key"))
    .withColumn("Supplier_Key",
        F.when(~F.col("_is_cust"), (F.rand(seed=63) * 31 + 1).cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("Transaction_Type_Key", (F.rand(seed=64) * 15 + 1).cast("int"))
    .withColumn("Payment_Method_Key", (F.rand(seed=65) * 6 + 1).cast("int"))
    .withColumn("WWI_Customer_Transaction_ID",
        F.when(F.col("_is_cust"), F.col("Transaction_Key").cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("WWI_Supplier_Transaction_ID",
        F.when(~F.col("_is_cust"), F.col("Transaction_Key").cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("WWI_Invoice_ID",
        F.when(F.col("_is_cust"), F.col("Transaction_Key").cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("WWI_Purchase_Order_ID",
        F.when(~F.col("_is_cust"), F.col("Transaction_Key").cast("int"))
         .otherwise(F.lit(None).cast("int")))
    .withColumn("Supplier_Invoice_Number",
        F.when(~F.col("_is_cust"),
            F.concat(F.lit("SINV-"), F.col("Transaction_Key").cast("string")))
         .otherwise(F.lit(None).cast("string")))
    .withColumn("Total_Excluding_Tax",
        F.round(F.rand(seed=66) * 5000 + 10, 2))
    .withColumn("Tax_Amount",
        F.round(F.col("Total_Excluding_Tax") * 0.15, 2))
    .withColumn("Total_Including_Tax",
        F.round(F.col("Total_Excluding_Tax") + F.col("Tax_Amount"), 2))
    .withColumn("Outstanding_Balance",
        F.when(F.rand(seed=67) < 0.3, F.col("Total_Including_Tax"))
         .otherwise(F.lit(0.0)))
    .withColumn("Is_Finalized",
        F.when(F.col("Outstanding_Balance") == 0.0, F.lit(True))
         .otherwise(F.lit(False)))
    .withColumn("Lineage_Key", F.lit(1))
    .drop("_is_cust")
)
write_table(df, "Fact_Transaction")''')

# ═══════════════════════════════════════════════════════════════════
#  ENRICHMENT LAYERS
# ═══════════════════════════════════════════════════════════════════
md("## Enrichment Layers")

# ── SCD2: Dimension_Customer_SCD2 ────────────────────────────────
code('''print("\\u2500\\u2500 Enrichment: Dimension_Customer_SCD2 \\u2500\\u2500")

base = spark.table(f"{SCHEMA}.Dimension_Customer")

# Current rows: Is_Current = true, Valid_From = 2013-01-01, Valid_To = 9999-12-31
current = (
    base
    .withColumn("Valid_From", F.to_date(F.lit("2013-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("9999-12-31")))
    .withColumn("Is_Current", F.lit(True))
    .drop("Lineage_Key")
)

# Historical rows: ~150 customers with flipped Buying_Group
flip = {"Tailspin Toys": "Wingtip Toys", "Wingtip Toys": "Tailspin Toys", "N/A": "Tailspin Toys"}
flip_expr = F.create_map(*[item for k, v in flip.items() for item in (F.lit(k), F.lit(v))])

historical = (
    base.filter(F.col("Customer_Key") <= 150)
    .withColumn("Buying_Group", flip_expr[F.col("Buying_Group")])
    .withColumn("Valid_From", F.to_date(F.lit("2013-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("2018-12-31")))
    .withColumn("Is_Current", F.lit(False))
    .drop("Lineage_Key")
)

# Update current rows for those 150 to start from 2019
current_updated = (
    current.filter(F.col("Customer_Key") <= 150)
    .withColumn("Valid_From", F.to_date(F.lit("2019-01-01")))
)
current_unchanged = current.filter(F.col("Customer_Key") > 150)

scd2 = historical.unionByName(current_updated).unionByName(current_unchanged)
write_table(scd2, "Dimension_Customer_SCD2")

write_view(
    f"CREATE OR REPLACE VIEW {SCHEMA}.vw_Customer_Current AS "
    f"SELECT * FROM {SCHEMA}.Dimension_Customer_SCD2 WHERE Is_Current = true",
    "vw_Customer_Current"
)''')

# ── SCD2: Dimension_StockItem_SCD2 ──────────────────────────────
code('''print("\\u2500\\u2500 Enrichment: Dimension_StockItem_SCD2 \\u2500\\u2500")

base = spark.table(f"{SCHEMA}.Dimension_Stock_Item")

# Current rows
current = (
    base
    .withColumn("Valid_From", F.to_date(F.lit("2020-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("9999-12-31")))
    .withColumn("Is_Current", F.lit(True))
    .drop("Lineage_Key")
)

# 2019 historical: prices at 85% — first 100 items
hist_2019 = (
    base.filter(F.col("Stock_Item_Key") <= 100)
    .withColumn("Unit_Price", F.round(F.col("Unit_Price") * 0.85, 2))
    .withColumn("Recommended_Retail_Price",
        F.round(F.col("Recommended_Retail_Price") * 0.85, 2))
    .withColumn("Valid_From", F.to_date(F.lit("2013-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("2018-12-31")))
    .withColumn("Is_Current", F.lit(False))
    .drop("Lineage_Key")
)

# 2020 historical: prices at 92% — first 100 items
hist_2020 = (
    base.filter(F.col("Stock_Item_Key") <= 100)
    .withColumn("Unit_Price", F.round(F.col("Unit_Price") * 0.92, 2))
    .withColumn("Recommended_Retail_Price",
        F.round(F.col("Recommended_Retail_Price") * 0.92, 2))
    .withColumn("Valid_From", F.to_date(F.lit("2019-01-01")))
    .withColumn("Valid_To", F.to_date(F.lit("2019-12-31")))
    .withColumn("Is_Current", F.lit(False))
    .drop("Lineage_Key")
)

scd2 = hist_2019.unionByName(hist_2020).unionByName(current)
write_table(scd2, "Dimension_StockItem_SCD2")

write_view(
    f"CREATE OR REPLACE VIEW {SCHEMA}.vw_StockItem_Current AS "
    f"SELECT * FROM {SCHEMA}.Dimension_StockItem_SCD2 WHERE Is_Current = true",
    "vw_StockItem_Current"
)''')

# ── Bridge_SupplierSubstitution ──────────────────────────────────
code('''print("\\u2500\\u2500 Enrichment: Bridge_SupplierSubstitution \\u2500\\u2500")

rng = random.Random(42)
rel_types = ["Primary", "Secondary", "Emergency"]
rows = []
for i in range(200):
    si = (i % 228) + 1
    primary = (i % 31) + 1
    substitute = ((i + rng.randint(1, 30)) % 31) + 1
    if substitute == primary:
        substitute = (primary % 31) + 1
    rows.append((
        si, primary, substitute,
        rel_types[i % 3],
        rng.randint(1, 45),
        round(rng.uniform(0, 25.0), 2),
        rng.random() < 0.85,
    ))

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

# Dimension_CustomerSegment (7 rows)
segments = ["High Value", "Growth", "Loyal", "Seasonal", "At Risk", "New Business", "Tail"]
seg_rows = [(i + 1, segments[i]) for i in range(7)]
seg_schema = StructType([
    StructField("Segment_Key", IntegerType()),
    StructField("Segment", StringType()),
])
write_table(spark.createDataFrame(seg_rows, seg_schema), "Dimension_CustomerSegment")

# Bridge_CustomerSegment — each customer gets 1-3 segments, weights sum to 1.0
rng = random.Random(42)
bridge_rows = []
for ck in range(1, 409):
    n_segs = rng.randint(1, 3)
    chosen = rng.sample(range(1, 8), n_segs)
    if n_segs == 1:
        weights = [1.0]
    elif n_segs == 2:
        w = round(rng.uniform(0.3, 0.7), 2)
        weights = [w, round(1.0 - w, 2)]
    else:
        w1 = round(rng.uniform(0.2, 0.5), 2)
        w2 = round(rng.uniform(0.2, round(1.0 - w1 - 0.1, 2)), 2)
        weights = [w1, w2, round(1.0 - w1 - w2, 2)]
    for seg_key, weight in zip(chosen, weights):
        bridge_rows.append((ck, seg_key, weight))

br_schema = StructType([
    StructField("Customer_Key", IntegerType()),
    StructField("Segment_Key", IntegerType()),
    StructField("Allocation_Weight", DoubleType()),
])
write_table(spark.createDataFrame(bridge_rows, br_schema), "Bridge_CustomerSegment")''')

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
                   "wwi_dw_fabric_load.ipynb")
with open(out, "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=1)

print(f"Created {out}")
print(f"Total cells: {len(cells)} ({sum(1 for c in cells if c['cell_type'] == 'code')} code, "
      f"{sum(1 for c in cells if c['cell_type'] == 'markdown')} markdown)")
