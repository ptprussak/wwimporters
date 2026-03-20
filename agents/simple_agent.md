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
The data is stored in a Fabric Lakehouse called "wwimporters" accessed
via the SQL analytics endpoint.

When the user asks a question, write a T-SQL query against the lakehouse
SQL analytics endpoint and return the results.

Available tables (all in [wwimporters].[dbo]):
  Dimension: dimension_city, dimension_customer, dimension_date,
             dimension_employee, dimension_payment_method,
             dimension_stock_item, dimension_supplier,
             dimension_transaction_type
  Fact:      fact_sale, fact_order, fact_purchase, fact_movement,
             fact_stock_holding, fact_transaction

Column names use underscores (e.g., Customer_Key, Total_Including_Tax).

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

```sql
-- Total sales revenue
SELECT SUM(Total_Including_Tax) AS total_revenue
FROM [wwimporters].[dbo].[fact_sale];

-- Top 10 customers by revenue
SELECT TOP 10 c.Customer, SUM(s.Total_Including_Tax) AS revenue
FROM [wwimporters].[dbo].[fact_sale] s
JOIN [wwimporters].[dbo].[dimension_customer] c
  ON s.Customer_Key = c.Customer_Key
GROUP BY c.Customer
ORDER BY revenue DESC;

-- Monthly sales trend
SELECT d.Calendar_Year, d.Calendar_Month_Number,
       SUM(s.Total_Including_Tax) AS revenue
FROM [wwimporters].[dbo].[fact_sale] s
JOIN [wwimporters].[dbo].[dimension_date] d
  ON s.Invoice_Date_Key = d.Date
GROUP BY d.Calendar_Year, d.Calendar_Month_Number
ORDER BY 1, 2;

-- Stock items low on inventory
SELECT si.Stock_Item, sh.Quantity_On_Hand, sh.Reorder_Level
FROM [wwimporters].[dbo].[fact_stock_holding] sh
JOIN [wwimporters].[dbo].[dimension_stock_item] si
  ON sh.Stock_Item_Key = si.Stock_Item_Key
WHERE sh.Quantity_On_Hand < sh.Reorder_Level;
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

**Why it will be wrong:** The agent only knows `dimension_customer` (the base
table, which is a snapshot). It has no idea that `dimension_customer_scd2`
exists and that ~150 customers **changed** buying group over time. The agent
will return only the *current* buying group assignments and miss every customer
who was previously in Tailspin Toys but is now in Wingtip Toys.

**What a correct answer requires:**
- Query `dimension_customer_scd2`
- Include both current and historical rows
- Optionally indicate which are current vs. historical

---

### Prompt 3 — SCD2 Trap: Historical pricing

> **"What was the unit price of stock item 50 in 2015?"**

**Why it will be wrong:** The agent will join to `dimension_stock_item` and
return today's price. It has no knowledge of `dimension_stockitem_scd2` which
contains historical price tiers (85% of current price for 2013-2018, 92% for
2019). The answer will be off by ~15%.

**What a correct answer requires:**
- Query `dimension_stockitem_scd2`
- Filter where `Valid_From <= '2015-12-31'` AND `Valid_To >= '2015-01-01'`

---

### Prompt 4 — Bridge Table Trap: Substitute suppliers

> **"Which suppliers can supply stock item 10?"**

**Why it will be wrong:** The agent will look at `fact_purchase` to find
suppliers that *have* supplied item 10. It has no knowledge of
`bridge_suppliersubstitution` which maps primary and substitute suppliers.
The answer will miss backup/emergency suppliers and the relationship type
(Primary, Secondary, Emergency).

**What a correct answer requires:**
- Query `bridge_suppliersubstitution`
- Join both `Primary_Supplier_Key` and `Substitute_Supplier_Key` to
  `dimension_supplier`
- Show relationship type, lead time, and cost premium

---

### Prompt 5 — Weighted Segment Trap: Customer segmentation

> **"What are the total sales for the 'High Value' customer segment?"**

**Why it will be wrong:** The agent does not know that
`dimension_customersegment` and `bridge_customersegment` exist. It will either:
- Fail entirely ("I don't have segment data"), or
- Hallucinate by guessing which customers are "High Value" based on revenue

Even if it somehow found the bridge table, it wouldn't know to apply the
`Allocation_Weight` — a customer can belong to multiple segments with fractional
weights that sum to 1.0. Simply summing all sales for customers tagged "High
Value" **double-counts** revenue for multi-segment customers.

**What a correct answer requires:**
- Join `fact_sale` → `bridge_customersegment` → `dimension_customersegment`
- Multiply each sale's `Total_Including_Tax` by `Allocation_Weight`
- Filter for Segment = 'High Value'

---

### Prompt 6 — Temporal + Bridge combo

> **"Show me the 2015 revenue by customer segment, using each customer's
>   buying group as it was at the time of the sale."**

**Why it will be catastrophically wrong:** This requires:
1. Temporal join to `dimension_customer_scd2` (point-in-time lookup)
2. Bridge join through `bridge_customersegment` with allocation weights
3. Date filtering on `fact_sale`

The simple agent has none of this context. It will produce a flat join to
`dimension_customer` (current snapshot), ignore segments entirely or fail,
and return numbers that are wrong on multiple dimensions simultaneously.

---

## Demo Talking Points

When presenting the simple agent's failures, emphasize:

1. **The agent isn't "dumb"** — it correctly handles simple queries. The
   problem is that **it was never told** about the complexity of the data model.
2. **SCD2 is invisible** unless the agent knows to look for temporal validity
   columns (`Valid_From`, `Valid_To`, `Is_Current`).
3. **Bridge tables break the 1:1 assumption** — the agent assumes each
   customer has one segment and each item has one supplier.
4. **Weighted allocations are a business rule** that cannot be inferred from
   the schema alone. The agent must be explicitly taught that revenue should
   be multiplied by `Allocation_Weight`.
5. **These aren't edge cases** — in any real enterprise DW, SCD2 and bridge
   tables are everywhere. An agent without proper instructions will silently
   return wrong answers that *look* correct.
