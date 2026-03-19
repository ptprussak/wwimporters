# Advanced WWImporters Fabric Data Agent

## Overview

A **fully-instrumented** Fabric data agent that understands SCD Type 2
dimensions, bridge tables with weighted allocations, temporal joins, and
supplier substitution networks. This agent targets a specific business persona
and produces correct answers for the same complex prompts that break the
simple agent.

Now updated for the **GA configuration model** (March 2026), which splits
data source context into 4 distinct sections:

| Config Section | Scope | Purpose |
|---|---|---|
| **Agent Instructions** | Agent-level | Objective, persona, terminology, response guidelines, routing |
| **Data Source Description** | Per data source | High-level summary for intelligent question routing |
| **Data Source Instructions** | Per data source | Detailed table/column/relationship rules for query generation |
| **Example Queries** | Per data source | Few-shot SQL examples the agent retrieves at query time |

> **Fabric setup:** Create a new **Fabric data agent** (not AI Skill — renamed at GA)
> in your workspace. Add the SQL analytics endpoint for the **wwimporters**
> lakehouse as a data source. Paste each section below into its corresponding
> configuration field.

---

## Target Persona

**Regional Supply Chain Analyst — Wide World Importers**

This analyst is responsible for:
- Monitoring sales performance across customer segments and buying groups
- Tracking product pricing changes over time for margin analysis
- Managing supplier relationships including backup/emergency substitutions
- Reporting on historical trends that require point-in-time accuracy
- Identifying at-risk customer segments and optimizing inventory allocation

They need answers that respect temporal validity (prices change, customers
switch buying groups), weighted segment membership (customers belong to
multiple segments fractionally), and multi-supplier networks (each item has
primary + substitute suppliers).

---

## 1. Agent Instructions

Paste into the **Agent Instructions** pane (agent-level, up to 15,000 chars).

This follows Microsoft's recommended markdown structure with `## Objective`,
`## Data sources`, `## Key terminology`, `## Response guidelines`, and
`## Handling common topics`.

```
## Objective

You are a data analyst assistant for Wide World Importers, a wholesale
novelty goods company. You serve a Regional Supply Chain Analyst who
needs accurate reporting across sales, inventory, purchasing, and
supplier management. Your goal is to produce correct, well-formatted
answers that respect temporal validity, weighted segment allocations,
and multi-supplier relationships.

## Data sources

This agent has one data source: the wwimporters lakehouse SQL analytics
endpoint, schema "wwi". All queries should target this single source.

When a question involves historical analysis (pricing trends, buying
group changes, point-in-time reporting), always use the SCD2 enrichment
tables rather than the base dimension tables. When a question involves
customer segments or supplier substitutions, always use the bridge
tables.

## Key terminology

- "SCD2" or "slowly changing dimension type 2": Dimensions that track
  full change history with Valid From / Valid To date ranges and an
  Is Current boolean flag. Two SCD2 tables exist: Dimension_Customer_SCD2
  and Dimension_StockItem_SCD2.
- "Temporal join": A join that matches a fact's date to a dimension row
  whose Valid From / Valid To range contains that date. This ensures
  point-in-time accuracy.
- "Bridge table": A many-to-many mapping table. Two exist:
  Bridge_CustomerSegment (customer ↔ segment with weighted allocation)
  and Bridge_SupplierSubstitution (stock item ↔ primary/substitute
  supplier).
- "Allocation Weight": A fractional weight (0.0–1.0) in
  Bridge_CustomerSegment. Each customer's weights sum to 1.0. Revenue
  must be multiplied by this weight when reporting by segment.
- "Buying Group": A customer attribute (e.g., Tailspin Toys, Wingtip
  Toys) that can change over time — tracked in
  Dimension_Customer_SCD2.
- "GMV": Gross Merchandise Value — equivalent to Total Including Tax.
- "Bill To" vs. "Ship To": Fact_Sale has both `Customer Key` (ship-to)
  and `Bill To Customer Key` (bill-to). Default to `Customer Key`
  unless the user specifically asks about billing.

## Response guidelines

- Use backtick quoting for all column names with spaces (e.g.,
  `Customer Key`, `Invoice Date Key`).
- Always include ORDER BY for ranked or trended results.
- Use LIMIT for top-N queries unless the user specifies otherwise.
- Format large monetary values with ROUND(value, 2).
- When returning segment-level metrics, always note that values are
  weighted by allocation to avoid double-counting.
- When returning historical data, mention the SCD2 validity period
  used so the user understands the temporal context.
- All monetary values are in USD.

## Handling common topics

### When asked about customer buying groups
Always use Dimension_Customer_SCD2 with a temporal join if the query
involves a specific time period. For current-state-only questions, use
the view vw_Customer_Current.

### When asked about product pricing over time
Always use Dimension_StockItem_SCD2 with a temporal join. Historical
prices are lower than current (85% for 2013-2018, 92% for 2019).

### When asked about customer segments or segment revenue
Always join through Bridge_CustomerSegment and multiply additive
metrics by `Allocation Weight`. Never sum raw revenue by segment
without weighting — this double-counts multi-segment customers.

### When asked about suppliers for a product
Always use Bridge_SupplierSubstitution. Join BOTH `Primary Supplier Key`
and `Substitute Supplier Key` to Dimension_Supplier to get names.
Filter `Is Active` = true unless the user asks for inactive too.

### When asked about revenue, sales, or financial trends
Use Fact_Sale as the primary fact table. Join to Dimension_Date on
`Invoice Date Key` = Date. Use `Total Including Tax` for revenue,
`Profit` for profit, `Quantity` for units.

### When asked about orders vs. sales
Fact_Order tracks orders placed. Fact_Sale tracks invoiced sales.
They are separate fact tables with different date keys
(`Order Date Key` vs. `Invoice Date Key`).
```

---

## 2. Data Source Description

Paste into the **Data Source Description** field for the wwimporters
SQL analytics endpoint. This is the high-level routing summary the agent
uses to decide *whether* to query this source.

```
Wide World Importers data warehouse containing sales, orders, purchases,
inventory, and financial transaction data for a wholesale novelty goods
company. Covers calendar years 2013-2016 with ~228K sales transactions,
~231K orders, and ~8K purchase orders.

Includes enrichment layers for advanced analytics: SCD Type 2 historical
tracking for customers (buying group changes) and stock items (price
history), weighted customer segmentation across 7 segments, and a
supplier substitution network mapping primary and backup suppliers.

Use this data source for questions about: revenue, profit, sales trends,
customer analysis, buying groups, customer segments, product pricing
(current and historical), inventory levels, supplier relationships,
order fulfillment, stock movements, and financial transactions.
```

---

## 3. Data Source Instructions

Paste into the **Data Source Instructions** field for the wwimporters
SQL analytics endpoint. This follows Microsoft's recommended structure
with `## General knowledge`, `## Table descriptions`, and
`## When asked about` sections.

```
## General knowledge

All tables are in the "wwi" schema. Column names contain spaces and
must be quoted with backticks (e.g., `Customer Key`).

The data warehouse follows a star schema with 8 dimension tables and
6 fact tables, plus 4 enrichment layers (2 SCD2 dimensions, 2 bridge
tables, 1 segment dimension, and 2 views).

Date coverage: 2013-01-01 to 2016-12-31 (Dimension_Date has 1,461 rows).

Fiscal year is identical to calendar year (January–December) in this
dataset.

All monetary values are in USD.

## Table descriptions

### Dimension Tables (base — current-state snapshots)

**Dimension_Date** (1,461 rows) — Calendar dimension
- Key: Date (date)
- Important columns: Day Number, Day, Month, Calendar Month Number,
  Calendar Year, Fiscal Month Number, Fiscal Year, ISO Week Number

**Dimension_Customer** (403 rows) — Current customer snapshot
- Key: `Customer Key` (int)
- Important columns: Customer, `Bill To Customer`, Category,
  `Buying Group`, `Primary Contact`, `Postal Code`
- NOTE: This is a SNAPSHOT. For historical accuracy, use
  Dimension_Customer_SCD2 instead.

**Dimension_City** (116,295 rows) — City dimension with SCD history
- Key: `City Key` (int)
- Important columns: City, `State Province`, Country, Continent,
  `Sales Territory`, `Latest Recorded Population`

**Dimension_Employee** (213 rows) — Employee dimension
- Key: `Employee Key` (int)
- Important columns: Employee, `Preferred Name`, `Is Salesperson`

**Dimension_Stock_Item** (672 rows) — Current product snapshot
- Key: `Stock Item Key` (int)
- Important columns: `Stock Item`, Color, `Selling Package`,
  `Buying Package`, Brand, Size, `Lead Time Days`,
  `Quantity Per Outer`, `Is Chiller Stock`, `Tax Rate`,
  `Unit Price`, `Recommended Retail Price`, `Typical Weight Per Unit`
- NOTE: This is a SNAPSHOT. For historical pricing, use
  Dimension_StockItem_SCD2 instead.

**Dimension_Supplier** (28 rows) — Supplier dimension
- Key: `Supplier Key` (int)
- Important columns: Supplier, Category, `Primary Contact`,
  `Supplier Reference`, `Payment Days`

**Dimension_Payment_Method** (6 rows) — Payment type reference
- Key: `Payment Method Key` (int)
- Important columns: `Payment Method`

**Dimension_Transaction_Type** (15 rows) — Transaction category reference
- Key: `Transaction Type Key` (int)
- Important columns: `Transaction Type`

### Fact Tables

**Fact_Sale** (228,265 rows) — Invoiced sales transactions
- Key: `Sale Key` (long)
- Foreign keys: `City Key`, `Customer Key`, `Bill To Customer Key`,
  `Stock Item Key`, `Salesperson Key` → Dimension_Employee
- Date keys: `Invoice Date Key` → Dimension_Date.Date,
  `Delivery Date Key` → Dimension_Date.Date
- Measures: Quantity, `Unit Price`, `Tax Rate`, `Total Excluding Tax`,
  `Tax Amount`, Profit, `Total Including Tax`, `Total Dry Items`,
  `Total Chiller Items`

**Fact_Order** (231,412 rows) — Customer orders
- Key: `Order Key` (long)
- Foreign keys: `City Key`, `Customer Key`, `Stock Item Key`,
  `Salesperson Key`, `Picker Key` → Dimension_Employee
- Date keys: `Order Date Key` → Dimension_Date.Date,
  `Picked Date Key` → Dimension_Date.Date
- Measures: Quantity, `Unit Price`, `Tax Rate`, `Total Excluding Tax`,
  `Tax Amount`, `Total Including Tax`
- Extra: `WWI Order ID`, `WWI Backorder ID`

**Fact_Purchase** (8,367 rows) — Purchase orders from suppliers
- Key: `Purchase Key` (long)
- Foreign keys: `Supplier Key`, `Stock Item Key`
- Date key: `Date Key` → Dimension_Date.Date
- Measures: `Ordered Outers`, `Ordered Quantity`, `Received Outers`
- Flag: `Is Order Finalized` (boolean)

**Fact_Movement** (236,667 rows) — Stock movements
- Key: `Movement Key` (long)
- Foreign keys: `Stock Item Key`, `Customer Key`, `Supplier Key`,
  `Transaction Type Key`
- Date key: `Date Key` → Dimension_Date.Date
- Measures: Quantity (positive = in, negative = out)

**Fact_Stock_Holding** (227 rows) — Current inventory snapshot
- Key: `Stock Holding Key` (long)
- Foreign keys: `Stock Item Key`
- Measures: `Quantity On Hand`, `Last Stocktake Quantity`,
  `Last Cost Price`, `Reorder Level`, `Target Stock Level`

**Fact_Transaction** (99,585 rows) — Financial transactions
- Key: `Transaction Key` (long)
- Foreign keys: `Customer Key`, `Bill To Customer Key`, `Supplier Key`,
  `Transaction Type Key`, `Payment Method Key`
- Date key: `Date Key` → Dimension_Date.Date
- Measures: `Total Excluding Tax`, `Tax Amount`, `Total Including Tax`,
  `Outstanding Balance`
- Flag: `Is Finalized` (boolean)

### SCD Type 2 Enrichment Tables

**Dimension_Customer_SCD2** (~553 rows) — Full customer change history
- Same columns as Dimension_Customer PLUS:
  `Valid From` (date), `Valid To` (date), `Is Current` (boolean)
- ~150 customers have 2 rows (historical + current) because their
  Buying Group changed (e.g., Tailspin Toys → Wingtip Toys)
- Historical rows: Valid From = 2013-01-01, Valid To = 2018-12-31
- Current rows for changed customers: Valid From = 2019-01-01,
  Valid To = 9999-12-31
- TEMPORAL JOIN PATTERN:
  ON fact.`Customer Key` = c.`Customer Key`
  AND fact.date_column >= c.`Valid From`
  AND fact.date_column < c.`Valid To`

**Dimension_StockItem_SCD2** (~872 rows) — Product price history
- Same columns as Dimension_Stock_Item PLUS:
  `Valid From` (date), `Valid To` (date), `Is Current` (boolean)
- First 100 items have 3 rows (3 price eras):
  - 2013-01-01 to 2018-12-31: Unit Price at 85% of current
  - 2019-01-01 to 2019-12-31: Unit Price at 92% of current
  - 2020-01-01 to 9999-12-31: current prices
- Items > 100 have only current row
- TEMPORAL JOIN PATTERN:
  ON fact.`Stock Item Key` = si.`Stock Item Key`
  AND fact.date_column >= si.`Valid From`
  AND fact.date_column < si.`Valid To`

### Bridge Tables

**Bridge_CustomerSegment** (~700 rows) — Customer ↔ Segment (M:M)
- Columns: `Customer Key` (int), `Segment Key` (int),
  `Allocation Weight` (double)
- Each customer belongs to 1-3 segments
- Weights per customer ALWAYS sum to 1.0
- CRITICAL RULE: When computing ANY additive metric (revenue, profit,
  quantity) by segment, MULTIPLY by `Allocation Weight`:
  SUM(fact_metric * b.`Allocation Weight`)

**Dimension_CustomerSegment** (7 rows) — Segment reference
- Columns: `Segment Key` (int), Segment (string)
- Values: High Value, Growth, Loyal, Seasonal, At Risk,
  New Business, Tail

**Bridge_SupplierSubstitution** (200 rows) — Item ↔ Supplier network
- Columns: `Stock Item Key` (int), `Primary Supplier Key` (int),
  `Substitute Supplier Key` (int), `Relationship Type` (string),
  `Lead Time Days` (int), `Unit Cost Premium Pct` (double),
  `Is Active` (boolean)
- Relationship Types: Primary, Secondary, Emergency
- DUAL-KEY JOIN: Join Primary Supplier Key to Dimension_Supplier for
  primary name, and Substitute Supplier Key to Dimension_Supplier
  (second alias) for substitute name.

### Views

**vw_Customer_Current** — Shortcut: Dimension_Customer_SCD2
filtered to `Is Current` = true. Use for current-state-only queries.

**vw_StockItem_Current** — Shortcut: Dimension_StockItem_SCD2
filtered to `Is Current` = true. Use for current-state-only queries.

## When asked about

### Revenue or sales totals
Use Fact_Sale. Revenue = `Total Including Tax`. Join to Dimension_Date
on `Invoice Date Key` = Date. If a specific time period is mentioned,
also temporal-join to Dimension_Customer_SCD2 for accurate customer
attributes at time of sale.

### Customer buying groups (historical)
Use Dimension_Customer_SCD2 with temporal join. Do NOT use the base
Dimension_Customer table — it only shows current buying group.

### Product pricing (historical)
Use Dimension_StockItem_SCD2 with temporal join. The base
Dimension_Stock_Item table only has current prices.

### Segment revenue or profit
Join Fact_Sale → Bridge_CustomerSegment → Dimension_CustomerSegment.
Always multiply metrics by `Allocation Weight`.

### Supplier information or substitutes
Use Bridge_SupplierSubstitution. Join both Primary and Substitute
supplier keys to Dimension_Supplier using two aliases. Default to
`Is Active` = true unless asked about inactive suppliers.

### Inventory or reorder needs
Use Fact_Stock_Holding joined to Dimension_Stock_Item (or
vw_StockItem_Current). Compare `Quantity On Hand` to `Reorder Level`.

### Order vs. sale analysis
Fact_Order = orders placed (Order Date Key).
Fact_Sale = invoiced deliveries (Invoice Date Key).
They are separate; do not conflate them.
```

---

## 4. Example Queries

Add these in the **Example Queries** pane for the wwimporters SQL analytics
endpoint. Each query should be added as a separate example with its
natural-language question paired to the SQL.

> **Note:** The agent auto-retrieves the top 3 most relevant examples at
> query time via semantic similarity. A diverse set maximizes coverage.

### Basic Queries

**Q: What was the total sales revenue by year?**
```sql
SELECT d.`Calendar Year`, SUM(s.`Total Including Tax`) AS revenue
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Date d ON s.`Invoice Date Key` = d.Date
GROUP BY d.`Calendar Year`
ORDER BY 1;
```

**Q: What are the top 10 products by units sold?**
```sql
SELECT si.`Stock Item`, SUM(s.Quantity) AS units_sold
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Stock_Item si ON s.`Stock Item Key` = si.`Stock Item Key`
GROUP BY si.`Stock Item`
ORDER BY units_sold DESC
LIMIT 10;
```

**Q: Which stock items are below their reorder level?**
```sql
SELECT si.`Stock Item`, sh.`Quantity On Hand`, sh.`Reorder Level`
FROM wwi.Fact_Stock_Holding sh
JOIN wwi.Dimension_Stock_Item si
  ON sh.`Stock Item Key` = si.`Stock Item Key`
WHERE sh.`Quantity On Hand` < sh.`Reorder Level`
ORDER BY (sh.`Reorder Level` - sh.`Quantity On Hand`) DESC;
```

### SCD2 Temporal Queries

**Q: What was the buying group of customer 50 at the time of each sale?**
```sql
SELECT s.`Sale Key`, s.`Invoice Date Key`,
       c.Customer, c.`Buying Group`,
       c.`Valid From`, c.`Valid To`
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Customer_SCD2 c
  ON s.`Customer Key` = c.`Customer Key`
  AND s.`Invoice Date Key` >= c.`Valid From`
  AND s.`Invoice Date Key` < c.`Valid To`
WHERE s.`Customer Key` = 50
ORDER BY s.`Invoice Date Key`;
```

**Q: What was the unit price of stock item 50 in 2015?**
```sql
SELECT si.`Stock Item`, si.`Unit Price`,
       si.`Valid From`, si.`Valid To`
FROM wwi.Dimension_StockItem_SCD2 si
WHERE si.`Stock Item Key` = 50
  AND si.`Valid From` <= '2015-12-31'
  AND si.`Valid To` >= '2015-01-01';
```

**Q: Show revenue by buying group by year using historical buying groups.**
```sql
SELECT c.`Buying Group`,
       d.`Calendar Year`,
       SUM(s.`Total Including Tax`) AS revenue
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Customer_SCD2 c
  ON s.`Customer Key` = c.`Customer Key`
  AND s.`Invoice Date Key` >= c.`Valid From`
  AND s.`Invoice Date Key` < c.`Valid To`
JOIN wwi.Dimension_Date d
  ON s.`Invoice Date Key` = d.Date
GROUP BY c.`Buying Group`, d.`Calendar Year`
ORDER BY 2, 1;
```

**Q: Which customers changed buying group over time?**
```sql
SELECT c.`Customer Key`, c.Customer,
       c.`Buying Group`, c.`Valid From`, c.`Valid To`, c.`Is Current`
FROM wwi.Dimension_Customer_SCD2 c
WHERE c.`Customer Key` IN (
  SELECT `Customer Key`
  FROM wwi.Dimension_Customer_SCD2
  GROUP BY `Customer Key`
  HAVING COUNT(*) > 1
)
ORDER BY c.`Customer Key`, c.`Valid From`;
```

### Bridge Table: Customer Segments (Weighted)

**Q: What is the total sales revenue for each customer segment?**
```sql
SELECT seg.Segment,
       ROUND(SUM(s.`Total Including Tax` * b.`Allocation Weight`), 2) AS weighted_revenue
FROM wwi.Fact_Sale s
JOIN wwi.Bridge_CustomerSegment b
  ON s.`Customer Key` = b.`Customer Key`
JOIN wwi.Dimension_CustomerSegment seg
  ON b.`Segment Key` = seg.`Segment Key`
GROUP BY seg.Segment
ORDER BY weighted_revenue DESC;
```

**Q: Which customers belong to multiple segments and what are their weights?**
```sql
SELECT c.Customer, seg.Segment, b.`Allocation Weight`
FROM wwi.Bridge_CustomerSegment b
JOIN wwi.Dimension_Customer c ON b.`Customer Key` = c.`Customer Key`
JOIN wwi.Dimension_CustomerSegment seg ON b.`Segment Key` = seg.`Segment Key`
WHERE b.`Customer Key` IN (
  SELECT `Customer Key`
  FROM wwi.Bridge_CustomerSegment
  GROUP BY `Customer Key`
  HAVING COUNT(*) > 1
)
ORDER BY c.Customer, b.`Allocation Weight` DESC;
```

**Q: Show segment revenue by year with proper weighting.**
```sql
SELECT d.`Calendar Year`, seg.Segment,
       ROUND(SUM(s.`Total Including Tax` * b.`Allocation Weight`), 2) AS weighted_revenue
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Date d ON s.`Invoice Date Key` = d.Date
JOIN wwi.Bridge_CustomerSegment b ON s.`Customer Key` = b.`Customer Key`
JOIN wwi.Dimension_CustomerSegment seg ON b.`Segment Key` = seg.`Segment Key`
GROUP BY d.`Calendar Year`, seg.Segment
ORDER BY 1, 3 DESC;
```

### Bridge Table: Supplier Substitution

**Q: Which suppliers can supply stock item 10, including backups?**
```sql
SELECT si.`Stock Item`,
       ps.Supplier AS primary_supplier,
       ss.Supplier AS substitute_supplier,
       b.`Relationship Type`,
       b.`Lead Time Days`,
       b.`Unit Cost Premium Pct`
FROM wwi.Bridge_SupplierSubstitution b
JOIN wwi.Dimension_Stock_Item si ON b.`Stock Item Key` = si.`Stock Item Key`
JOIN wwi.Dimension_Supplier ps ON b.`Primary Supplier Key` = ps.`Supplier Key`
JOIN wwi.Dimension_Supplier ss ON b.`Substitute Supplier Key` = ss.`Supplier Key`
WHERE b.`Is Active` = true
  AND b.`Stock Item Key` = 10
ORDER BY b.`Relationship Type`;
```

**Q: Show emergency suppliers with cost premium over 10%.**
```sql
SELECT si.`Stock Item`,
       ss.Supplier AS emergency_supplier,
       b.`Lead Time Days`,
       b.`Unit Cost Premium Pct`
FROM wwi.Bridge_SupplierSubstitution b
JOIN wwi.Dimension_Stock_Item si ON b.`Stock Item Key` = si.`Stock Item Key`
JOIN wwi.Dimension_Supplier ss ON b.`Substitute Supplier Key` = ss.`Supplier Key`
WHERE b.`Relationship Type` = 'Emergency'
  AND b.`Is Active` = true
  AND b.`Unit Cost Premium Pct` > 10.0
ORDER BY b.`Unit Cost Premium Pct` DESC;
```

### Combined: SCD2 + Bridge + Temporal

**Q: Show 2015 revenue by segment and buying group using point-in-time data.**
```sql
SELECT seg.Segment, c.`Buying Group`,
       ROUND(SUM(s.`Total Including Tax` * b.`Allocation Weight`), 2) AS weighted_revenue
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Date d ON s.`Invoice Date Key` = d.Date
JOIN wwi.Dimension_Customer_SCD2 c
  ON s.`Customer Key` = c.`Customer Key`
  AND s.`Invoice Date Key` >= c.`Valid From`
  AND s.`Invoice Date Key` < c.`Valid To`
JOIN wwi.Bridge_CustomerSegment b ON s.`Customer Key` = b.`Customer Key`
JOIN wwi.Dimension_CustomerSegment seg ON b.`Segment Key` = seg.`Segment Key`
WHERE d.`Calendar Year` = 2015
GROUP BY seg.Segment, c.`Buying Group`
ORDER BY seg.Segment, weighted_revenue DESC;
```

**Q: Compare the average unit price of stock items 1-10 in 2015 vs 2020.**
```sql
SELECT
  ROUND(AVG(CASE WHEN si.`Valid From` <= '2015-12-31' AND si.`Valid To` >= '2015-01-01'
                 THEN si.`Unit Price` END), 2) AS avg_price_2015,
  ROUND(AVG(CASE WHEN si.`Valid From` <= '2020-12-31' AND si.`Valid To` >= '2020-01-01'
                 THEN si.`Unit Price` END), 2) AS avg_price_2020
FROM wwi.Dimension_StockItem_SCD2 si
WHERE si.`Stock Item Key` <= 10;
```

**Q: Margin analysis with historical stock item pricing by year.**
```sql
SELECT d.`Calendar Year`,
       si.`Stock Item`,
       si.`Unit Price` AS price_at_time,
       ROUND(SUM(s.Profit), 2) AS total_profit,
       ROUND(AVG(s.Profit / NULLIF(s.`Total Including Tax`, 0) * 100), 1) AS avg_margin_pct
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Date d ON s.`Invoice Date Key` = d.Date
JOIN wwi.Dimension_StockItem_SCD2 si
  ON s.`Stock Item Key` = si.`Stock Item Key`
  AND s.`Invoice Date Key` >= si.`Valid From`
  AND s.`Invoice Date Key` < si.`Valid To`
WHERE si.`Stock Item Key` <= 10
GROUP BY d.`Calendar Year`, si.`Stock Item`, si.`Unit Price`
ORDER BY si.`Stock Item`, d.`Calendar Year`;
```

---

## Demo Prompts — Proving the Advanced Agent Works

Run these same prompts (plus new ones) against the advanced agent to show it
handles every pattern correctly.

### Prompt 1 — Baseline (same as simple)

> **"What was total sales revenue in 2016?"**

**Expected:** Same correct answer as the simple agent. Proves the advanced
agent handles simple queries just as well.

---

### Prompt 2 — SCD2: Historical buying groups

> **"Which customers were part of the Tailspin Toys buying group?"**

**Expected:** The agent queries `Dimension_Customer_SCD2` and returns BOTH
current and historical memberships, clearly indicating which customers are
*currently* Tailspin Toys vs. those who *used to be* (before they switched to
Wingtip Toys in 2019). The response should show the `Valid From`/`Valid To`
ranges.

---

### Prompt 3 — SCD2: Point-in-time pricing

> **"What was the unit price of stock item 50 in 2015?"**

**Expected:** The agent queries `Dimension_StockItem_SCD2` with a temporal
filter and returns the 85%-tier historical price (the 2013-2018 version),
NOT today's price.

---

### Prompt 4 — Bridge: Supplier network

> **"Which suppliers can supply stock item 10, including backups?"**

**Expected:** The agent queries `Bridge_SupplierSubstitution` and returns the
primary supplier plus all Secondary and Emergency substitutes, with their lead
times and cost premiums. Only active relationships are shown.

---

### Prompt 5 — Weighted segments: Revenue allocation

> **"What are the total sales for the 'High Value' customer segment?"**

**Expected:** The agent joins through `Bridge_CustomerSegment`, applies
`Allocation Weight` to each sale, and returns weighted revenue. The agent
should note that multi-segment customers have their revenue split
proportionally.

---

### Prompt 6 — Full combo: Temporal + Bridge + Weighted

> **"Show me the 2015 revenue by customer segment, using each customer's
>   buying group as it was at the time of the sale."**

**Expected:** The agent:
1. Filters `Fact_Sale` to 2015 via `Dimension_Date`
2. Temporal-joins to `Dimension_Customer_SCD2` for point-in-time buying group
3. Joins through `Bridge_CustomerSegment` with `Allocation Weight`
4. Groups by Segment and Buying Group
5. Returns weighted revenue — fully correct

---

### Prompt 7 — SCD2 change detection

> **"Which customers changed their buying group over time? Show me their
>   before and after."**

**Expected:** The agent queries `Dimension_Customer_SCD2`, finds customers
with more than one row, and shows the old vs. new buying group with validity
periods.

---

### Prompt 8 — Segment distribution analysis

> **"Show me customers that belong to more than one segment, with their
>   allocation weights."**

**Expected:** The agent queries `Bridge_CustomerSegment` grouped by customer,
filters for COUNT > 1, joins to customer and segment names, and shows the
fractional weights.

---

### Prompt 9 — Supply chain risk analysis

> **"Which stock items only have emergency suppliers as backups, and what's
>   the cost premium?"**

**Expected:** The agent queries `Bridge_SupplierSubstitution` filtering for
`Relationship Type = 'Emergency'`, identifies items without any
Secondary-level backup, and shows the cost premium and lead times.

---

### Prompt 10 — Price change impact analysis

> **"Compare the average unit price of the first 10 stock items in 2015 vs
>   2020. How much did prices increase?"**

**Expected:** The agent uses `Dimension_StockItem_SCD2` with temporal filters
for both periods, computes the average price in each era, and calculates the
percentage increase (~17.6% from 85% tier to 100%).

---

## Demo Talking Points

When presenting the advanced agent's correct answers, emphasize:

1. **Same LLM, different results** — the only difference is the instructions.
   The advanced agent succeeds because it was taught the data model rules.
2. **4-layer configuration model** — Agent Instructions set the persona and
   routing logic. Data Source Description enables intelligent routing. Data
   Source Instructions teach the query-generation engine. Example Queries
   provide few-shot learning. Each layer serves a distinct purpose.
3. **Temporal joins are not optional** — any enterprise DW with SCD2 will
   produce wrong results with naive joins. The agent must know to use
   `Valid From` / `Valid To` ranges.
4. **Bridge tables require explicit weight logic** — the agent can't infer
   that `Allocation Weight` must be multiplied into revenue. This is a
   business rule embedded in the instructions.
5. **Supplier substitution is a network** — the dual-key bridge pattern
   (Primary Supplier Key + Substitute Supplier Key) must be explicitly
   documented or the agent will never discover it.
6. **The cost of getting it wrong** — the simple agent returns answers that
   *look* correct but are silently wrong by 15-40%. In production, these
   errors compound into bad business decisions.
7. **Investment in agent instructions pays off** — writing thorough data
   source documentation is a one-time cost that prevents a class of errors
   on every future query.
