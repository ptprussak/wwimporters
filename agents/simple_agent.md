# Simple WWImporters Fabric Data Agent

## Overview

A **deliberately minimal** Fabric data agent to demonstrate the limitations of
under-specified agent instructions. This agent has basic awareness of the
tables but no guidance on SCD2 patterns, bridge tables, weighted segments, or
temporal joins. It will produce plausible-looking but **wrong** answers for
anything beyond simple single-table lookups.

> **Fabric setup:** Create a new **Fabric data agent** (GA, March 2026) in your
> workspace. Add the SQL analytics endpoint for the **wwimporters** lakehouse
> as a data source. Only the Agent Instructions and Example Queries sections
> are filled — Data Source Description and Data Source Instructions are left
> **intentionally blank** to show what happens with minimal configuration.

---

## 1. Agent Instructions

Paste into the **Agent Instructions** pane (agent-level):

```
You are a data assistant for the Wide World Importers data warehouse.
The data is stored in a Fabric Lakehouse called "wwimporters" under the
schema "wwi".

When the user asks a question, write a SQL query against the lakehouse
SQL analytics endpoint and return the results.

Available tables:
  Dimension: City, Customer, Date, Employee, Payment_Method, Stock_Item,
             Supplier, Transaction_Type
  Fact:      Sale, Order, Purchase, Movement, Stock_Holding, Transaction

Use backtick quoting for column names that contain spaces, e.g.
`Customer Key`.

Always return clear, concise answers.
```

## 2. Data Source Description

**Left intentionally blank.** The simple agent has no routing guidance.

---

## 3. Data Source Instructions

**Left intentionally blank.** The simple agent has no table-level documentation,
no SCD2 rules, no bridge table guidance, no weight logic.

---

## 4. Example Queries

Add to the **Example Queries** pane for the wwimporters SQL analytics endpoint:

```
-- Total sales revenue
SELECT SUM(`Total Including Tax`) AS total_revenue
FROM wwi.Fact_Sale;

-- Top 10 customers by revenue
SELECT c.Customer, SUM(s.`Total Including Tax`) AS revenue
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Customer c
  ON s.`Customer Key` = c.`Customer Key`
GROUP BY c.Customer
ORDER BY revenue DESC
LIMIT 10;

-- Monthly sales trend
SELECT d.`Calendar Year`, d.`Calendar Month Number`, SUM(s.`Total Including Tax`) AS revenue
FROM wwi.Fact_Sale s
JOIN wwi.Dimension_Date d
  ON s.`Invoice Date Key` = d.Date
GROUP BY d.`Calendar Year`, d.`Calendar Month Number`
ORDER BY 1, 2;

-- Stock items low on inventory
SELECT si.`Stock Item`, sh.`Quantity On Hand`, sh.`Reorder Level`
FROM wwi.Fact_Stock_Holding sh
JOIN wwi.Dimension_Stock_Item si
  ON sh.`Stock Item Key` = si.`Stock Item Key`
WHERE sh.`Quantity On Hand` < sh.`Reorder Level`;
```

---

## Demo Prompts & Expected Limitations

Run these prompts against the simple agent during the demo. Each one is
designed to expose a specific gap.

### Prompt 1 — Simple (should work)

> **"What was total sales revenue in 2016?"**

**Expected:** Agent writes a straightforward Date join and returns a correct
number. This is the happy path.

---

### Prompt 2 — SCD2 Trap: Customer buying group change

> **"Which customers were part of the Tailspin Toys buying group?"**

**Why it will be wrong:** The agent only knows `Dimension_Customer` (the base
table, which is a snapshot). It has no idea that `Dimension_Customer_SCD2`
exists and that ~150 customers **changed** buying group over time. The agent
will return only the *current* buying group assignments and miss every customer
who was previously in Tailspin Toys but is now in Wingtip Toys.

**What a correct answer requires:**
- Query `Dimension_Customer_SCD2`
- Include both current and historical rows
- Optionally indicate which are current vs. historical

---

### Prompt 3 — SCD2 Trap: Historical pricing

> **"What was the unit price of stock item 50 in 2015?"**

**Why it will be wrong:** The agent will join to `Dimension_Stock_Item` and
return today's price. It has no knowledge of `Dimension_StockItem_SCD2` which
contains historical price tiers (85% of current price for 2013-2018, 92% for
2019). The answer will be off by ~15%.

**What a correct answer requires:**
- Query `Dimension_StockItem_SCD2`
- Filter where `Valid From <= '2015-12-31'` AND `Valid To >= '2015-01-01'`

---

### Prompt 4 — Bridge Table Trap: Substitute suppliers

> **"Which suppliers can supply stock item 10?"**

**Why it will be wrong:** The agent will look at `Fact_Purchase` to find
suppliers that *have* supplied item 10. It has no knowledge of
`Bridge_SupplierSubstitution` which maps primary and substitute suppliers.
The answer will miss backup/emergency suppliers and the relationship type
(Primary, Secondary, Emergency).

**What a correct answer requires:**
- Query `Bridge_SupplierSubstitution`
- Join both `Primary Supplier Key` and `Substitute Supplier Key` to
  `Dimension_Supplier`
- Show relationship type, lead time, and cost premium

---

### Prompt 5 — Weighted Segment Trap: Customer segmentation

> **"What are the total sales for the 'High Value' customer segment?"**

**Why it will be wrong:** The agent does not know that
`Dimension_CustomerSegment` and `Bridge_CustomerSegment` exist. It will either:
- Fail entirely ("I don't have segment data"), or
- Hallucinate by guessing which customers are "High Value" based on revenue

Even if it somehow found the bridge table, it wouldn't know to apply the
`Allocation Weight` — a customer can belong to multiple segments with fractional
weights that sum to 1.0. Simply summing all sales for customers tagged "High
Value" **double-counts** revenue for multi-segment customers.

**What a correct answer requires:**
- Join `Fact_Sale` → `Bridge_CustomerSegment` → `Dimension_CustomerSegment`
- Multiply each sale's `Total Including Tax` by `Allocation Weight`
- Filter for Segment = 'High Value'

---

### Prompt 6 — Temporal + Bridge combo

> **"Show me the 2015 revenue by customer segment, using each customer's
>   buying group as it was at the time of the sale."**

**Why it will be catastrophically wrong:** This requires:
1. Temporal join to `Dimension_Customer_SCD2` (point-in-time lookup)
2. Bridge join through `Bridge_CustomerSegment` with allocation weights
3. Date filtering on `Fact_Sale`

The simple agent has none of this context. It will produce a flat join to
`Dimension_Customer` (current snapshot), ignore segments entirely or fail,
and return numbers that are wrong on multiple dimensions simultaneously.

---

## Demo Talking Points

When presenting the simple agent's failures, emphasize:

1. **The agent isn't "dumb"** — it correctly handles simple queries. The
   problem is that **it was never told** about the complexity of the data model.
2. **SCD2 is invisible** unless the agent knows to look for temporal validity
   columns (`Valid From`, `Valid To`, `Is Current`).
3. **Bridge tables break the 1:1 assumption** — the agent assumes each
   customer has one segment and each item has one supplier.
4. **Weighted allocations are a business rule** that cannot be inferred from
   the schema alone. The agent must be explicitly taught that revenue should
   be multiplied by `Allocation Weight`.
5. **These aren't edge cases** — in any real enterprise DW, SCD2 and bridge
   tables are everywhere. An agent without proper instructions will silently
   return wrong answers that *look* correct.
