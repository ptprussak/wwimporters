# Fabric Data Agent Demo — Run Script

> Uses the **GA configuration model** (March 2026) with 4 config sections:
> Agent Instructions, Data Source Description, Data Source Instructions,
> and Example Queries.

## Setup Checklist

- [ ] Lakehouse **wwimporters** exists with all tables loaded (run `wwi_load_from_csv.ipynb`)
- [ ] SQL analytics endpoint is available for the lakehouse
- [ ] **Fabric data agent 1** created: "WWI Simple Agent" — configured per `simple_agent.md`
  - Agent Instructions: filled (minimal)
  - Data Source Description: **blank**
  - Data Source Instructions: **blank**
  - Example Queries: 4 basic queries
- [ ] **Fabric data agent 2** created: "WWI Advanced Agent" — configured per `advanced_agent.md`
  - Agent Instructions: filled (persona + objective + terminology + routing)
  - Data Source Description: filled (high-level routing summary)
  - Data Source Instructions: filled (full table docs, SCD2 rules, bridge rules)
  - Example Queries: 14 queries covering all patterns
- [ ] Both agents have the SQL analytics endpoint attached as a data source
- [ ] Both agents are published for testing

---

## Side-by-Side Demo Prompts

Run each prompt against BOTH agents. The table below shows the expected
outcome for each.

| # | Prompt | Simple Agent | Advanced Agent |
|---|--------|-------------|----------------|
| 1 | "What was total sales revenue in 2016?" | Correct | Correct |
| 2 | "Which customers were part of the Tailspin Toys buying group?" | Wrong — current snapshot only, misses ~150 historical members | Correct — shows current AND historical from SCD2 |
| 3 | "What was the unit price of stock item 50 in 2015?" | Wrong — returns today's price | Correct — returns 85% historical price from SCD2 |
| 4 | "If our primary supplier for stock item 10 can't deliver, what's our cost exposure for alternatives?" | Partial — finds bridge but misses cost premium / lead time analysis | Correct — shows substitutes with cost premium %, lead times, and risk assessment |
| 5 | "Break down total 2016 revenue across all customer segments." | **Visibly wrong** — segment totals sum to ~140% of actual revenue (double-counts multi-segment customers) | Correct — weighted totals sum to exactly the same figure as Prompt 1 |
| 6 | "Show me 2015 revenue by customer segment, using buying group as of sale date." | Catastrophically wrong — wrong on 3 axes | Correct — temporal SCD2 + weighted bridge |

### Extended prompts (advanced agent only)

| # | Prompt | Expected Behavior |
|---|--------|-------------------|
| 7 | "Which customers changed their buying group over time?" | Lists ~150 customers with before/after buying groups and validity dates |
| 8 | "Show customers in more than one segment with allocation weights." | Multi-segment customers with fractional weights summing to 1.0 |
| 9 | "Which stock items only have emergency supplier backups?" | Items with Emergency but no Secondary substitutes |
| 10 | "Compare average unit price of first 10 stock items in 2015 vs 2020." | ~17.6% increase (85% → 100% of current price) |

---

## Key Narrative Arc

### Act 1 — "It works!" (Prompt 1)
Both agents handle simple queries perfectly. Build confidence in the
technology.

### Act 2 — "But wait..." (Prompts 2-5)
The simple agent silently returns **wrong** answers for every complex query.
Prompt 5 is the showstopper: the segment revenue totals visibly contradict the
total from Prompt 1, proving the agent is double-counting. The numbers look
plausible individually — that's what makes this dangerous.

### Act 3 — "The fix" (Prompts 2-6 on advanced agent)
Same prompts, same LLM, same data — but now with proper instructions, every
answer is correct. The difference is documentation, not intelligence.

### Act 4 — "Going further" (Prompts 7-10)
The advanced agent handles novel analytical questions that combine multiple
patterns. This is where real business value lives.

---

## Why This Matters — Closing Points

1. **Silent failures are worse than errors.** The simple agent never says
   "I don't know." It confidently returns wrong numbers.

2. **Schema discovery is not enough.** The LLM can find bridge tables on its
   own, but it can't infer that `Allocation_Weight` must be multiplied into
   revenue. Prompt 5 proves this: the simple agent finds the segment table
   but double-counts, producing totals that exceed the known total from
   Prompt 1.

3. **Bridge tables break assumptions.** LLMs default to 1:1 joins. Without
   explicit instructions, they will never generate the dual-key pattern needed
   for supplier substitution or the weight multiplication for segments.

4. **The 4-layer GA config model is key.** Agent Instructions set persona
   and routing. Data Source Description enables intelligent question routing.
   Data Source Instructions teach table-level query logic. Example Queries
   provide few-shot learning. Leaving any layer blank creates blind spots.

5. **Investment in instructions = ROI on accuracy.** Writing 2 pages of data
   model documentation prevents an entire class of analytical errors across
   every future query the agent handles.

6. **This pattern applies everywhere.** Any enterprise DW with SCD2, bridge
   tables, or weighted allocations will have the same problem. The WWI demo
   is a microcosm of real-world data modeling complexity.
