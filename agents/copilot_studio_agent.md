# Copilot Studio Agent — WWI Supply Chain Analyst

## Overview

A **Microsoft Copilot Studio** agent that wraps the Fabric data agent to provide
a user-facing conversational interface. This agent handles persona, tone, and
response formatting while delegating all data queries to the connected Fabric
data agent (which contains the SQL logic, SCD2 rules, bridge table instructions,
and example queries).

This approach separates concerns: Copilot Studio owns the **user experience**,
the Fabric data agent owns the **data logic**.

> **Architecture:** Copilot Studio agent → connected Fabric data agent →
> SQL analytics endpoint → wwimporters lakehouse

---

## Prerequisites

- [ ] Fabric data agent ("WWI Advanced Agent") is published and responding to
  queries — configured per `advanced_agent.md`
- [ ] Copilot Studio and Fabric are on the **same tenant**
- [ ] You are signed in to both with the **same account** that has access to the
  Fabric data agent
- [ ] Fabric data agent tenant settings are enabled in the Fabric admin portal

---

## Setup Steps

### Step 1 — Create the agent

1. Open [Copilot Studio](https://copilotstudio.microsoft.com)
2. Select **Create** → **+ New agent**
3. Fill in:
   - **Name:** WWI Supply Chain Analyst
   - **Description:** Supply chain analyst assistant for Wide World Importers.
     Answers questions about sales, customer segments, product pricing history,
     supplier relationships, and inventory using the wwimporters data warehouse.

### Step 1b — Select the language model

1. On the agent's **Overview** page, find the **Model** section
2. Select **GPT-5** from the dropdown
   - GPT-5 provides the strongest reasoning for complex analytical queries
     (temporal joins, weighted allocations, multi-hop supplier lookups)
   - GPT-4.1 is the default for new agents but GPT-5 is preferred for this
     demo to maximize query accuracy

### Step 2 — Connect the Fabric data agent

1. In the agent editor, go to the **Agents** tab at the top
2. Select **+ Add** → choose **Microsoft Fabric**
3. If no connection exists, select **Create new connection** and authenticate
4. From the list of available Fabric data agents, select **WWI Advanced Agent**
5. Under **Credentials to use**, select **Maker-provided credentials**
6. Select **Add**

### Step 3 — Configure agent instructions

Paste the instructions from Section 1 below into the **Instructions** pane.

### Step 4 — Configure settings (critical)

These settings control what the agent can and cannot use beyond the connected
Fabric data agent. Navigate to **Settings** → **Generative AI** to configure
each one.

| Setting | Value | Why |
|---------|-------|-----|
| **Content moderation** | **High** (default) | Prevents harmful content in responses. Keep at High unless you have a specific reason to lower it. |
| **Web search** | **Off** | The agent must answer strictly from the data warehouse. Web results would introduce unverified numbers and confuse the demo narrative. |
| **General knowledge** | **Off** | Prevents the LLM from falling back to its training data when the Fabric data agent returns no results. The agent should say "I don't have that data" rather than guess. |
| **Code interpreter** | **Off** | Not needed — all computation happens in T-SQL via the Fabric data agent. Enabling it adds latency and could produce conflicting calculations. |
| **File processing** | **Off** | The agent does not need to process uploaded files. All data lives in the lakehouse. |

> **Why this matters for the demo:** If web search or general knowledge is left
> on, the simple agent may pull correct-looking answers from the web instead of
> generating wrong SQL — defeating the purpose of the demo. Both agents must be
> constrained to the data warehouse only.

### Step 5 — Configure suggested prompts

On the agent's **Overview** page, select the **Edit** icon in the **Suggested
prompts** section. Add up to 6 prompts that guide users toward the demo
scenarios:

| # | Title | Prompt |
|---|-------|--------|
| 1 | Total revenue | What was total sales revenue in 2016? |
| 2 | Buying group history | Which customers were part of the Tailspin Toys buying group? |
| 3 | Historical pricing | What was the unit price of stock item 50 in 2015? |
| 4 | Supplier cost exposure | If our primary supplier for stock item 10 can't deliver, what's our cost exposure for alternatives? |
| 5 | Segment revenue | Break down total 2016 revenue across all customer segments. |
| 6 | Full combo analysis | Show me 2015 revenue by customer segment, using buying group as of sale date. |

These appear on the agent's welcome page in Teams and Copilot Chat before the
user starts typing. They won't appear in the Copilot Studio test pane.

### Step 6 — Create test sets

Use Copilot Studio's **Agent Evaluation** feature to create a reusable test set
that validates the agent after any configuration change.

1. In the agent editor, go to **Evaluate** → **Test sets** → **+ New test set**
2. Select **Import from file** and upload a CSV with the columns below, or
   add test cases manually

**Test set: WWI Demo Validation (10 cases)**

| # | User message | Test method | Expected behavior |
|---|-------------|-------------|-------------------|
| 1 | What was total sales revenue in 2016? | General Quality | Returns a single revenue number with year filter |
| 2 | Which customers were part of the Tailspin Toys buying group? | General Quality | Uses SCD2 table, shows current AND historical members |
| 3 | What was the unit price of stock item 50 in 2015? | General Quality | Returns 85%-tier historical price, not current price |
| 4 | If our primary supplier for stock item 10 can't deliver, what's our cost exposure for alternatives? | Capability Use | Invokes the WWI data agent; mentions cost premium and lead time |
| 5 | Break down total 2016 revenue across all customer segments. | General Quality | Weighted segment totals sum to the same figure as test 1 |
| 6 | Show me 2015 revenue by customer segment, using buying group as of sale date. | General Quality | Combines temporal SCD2 join with weighted bridge table |
| 7 | Which customers changed their buying group over time? | General Quality | Lists ~150 customers with before/after and validity dates |
| 8 | Show customers in more than one segment with allocation weights. | General Quality | Multi-segment customers with fractional weights summing to 1.0 |
| 9 | Which stock items only have emergency supplier backups? | General Quality | Items with Emergency but no Secondary substitutes |
| 10 | Compare average unit price of first 10 stock items in 2015 vs 2020. | General Quality | ~17.6% increase (85% → 100%) |

3. Select a **User profile** with access to the Fabric data agent
4. Run the test set and review results — all 10 should pass with General
   Quality scores above the threshold
5. Re-run after any instruction or settings change to catch regressions

> **Tip:** Use **Quick question set** to auto-generate additional test cases
> from your agent's description and instructions, then merge them into this
> test set for broader coverage.

### Step 7 — Test and publish

1. Use the built-in **Test chat** pane on the right to validate interactively
2. Run the test set from Step 6 to validate systematically
3. When satisfied, select **Publish** to make the agent available

---

## 1. Agent Instructions

Paste into the **Instructions** pane (up to 8,000 characters):

```
## Objective

You are a supply chain analyst assistant for Wide World Importers, a wholesale
novelty goods company. You help a Regional Supply Chain Analyst explore sales
performance, customer segments, product pricing history, supplier relationships,
and inventory status. Your goal is to provide accurate, well-formatted answers
grounded in the company's data warehouse.

## Data routing

Route ALL data questions to the connected WWI data agent. Do not attempt to
answer data questions from your own knowledge or make up numbers. If the data
agent returns no results, say so clearly rather than guessing.

Questions to route to the data agent:
- Revenue, sales, profit, or financial metrics
- Customer information, buying groups, or segment breakdowns
- Product pricing (current or historical)
- Supplier relationships or substitution networks
- Inventory levels, reorder status, or stock movements
- Order or purchase analysis
- Any question that requires querying the data warehouse

## Key terminology

Use these terms consistently when presenting results:

- "Buying Group" — a customer attribute (e.g., Tailspin Toys, Wingtip Toys)
  that can change over time. Historical queries use point-in-time values.
- "Customer Segment" — customers belong to one or more of 7 segments (High
  Value, Growth, Loyal, Seasonal, At Risk, New Business, Tail) with fractional
  allocation weights.
- "Weighted revenue" — when reporting revenue by segment, values are
  proportionally allocated using each customer's segment weight. This prevents
  double-counting for customers in multiple segments.
- "SCD2" / "historical" — indicates the answer uses point-in-time data from
  slowly changing dimension tables, not just today's snapshot.
- "Primary / Secondary / Emergency supplier" — the supplier substitution
  network for each stock item, with associated lead times and cost premiums.

## Response guidelines

- Present tabular results in markdown tables for readability.
- For monetary values, use USD formatting with 2 decimal places.
- When results involve weighted segment allocations, include a note:
  "Values are weighted by allocation to avoid double-counting multi-segment
  customers."
- When results involve historical/SCD2 data, mention the validity period so
  the user understands the temporal context (e.g., "Based on buying group
  assignments as of 2015").
- For top-N or ranked results, always specify the ranking criteria.
- Keep explanations concise — lead with the data, add context after.
- If the user asks a follow-up, maintain context from the previous answer.

## Handling common topics

### Revenue or sales questions
Route to the data agent. If the user asks for a segment breakdown after a
total, remind them that weighted segment totals should match the overall total.

### Customer segment analysis
Route to the data agent. Always note that segment revenue is weighted. If the
user asks "which segment is biggest," clarify this is by weighted revenue.

### Pricing questions
Route to the data agent. If the user asks about historical prices, mention
that the data warehouse tracks price changes over time (prices were lower in
earlier periods).

### Supplier questions
Route to the data agent. When presenting substitution options, highlight the
cost premium and lead time tradeoffs between Secondary and Emergency suppliers.

### Questions you cannot answer
If a question is outside the scope of the data warehouse (e.g., HR data,
marketing campaigns, external market data), say: "I don't have access to that
data. I can help with sales, inventory, customer, supplier, and financial data
from the Wide World Importers data warehouse."
```

---

## 2. Knowledge Sources

No additional knowledge sources are needed. All data queries are handled by the
connected Fabric data agent. The Copilot Studio agent does not need direct
access to the SQL analytics endpoint.

If you want to add supplementary context (e.g., a company glossary or FAQ),
you can add it as a **Public website** or **File upload** knowledge source,
but this is optional.

---

## 3. Topics (Optional)

The default **generative orchestration** mode handles routing to the connected
Fabric data agent automatically based on the instructions above. Custom topics
are not required for this demo.

If you want to add structured conversation flows later (e.g., a guided
"monthly report" wizard), you can create custom topics that invoke the data
agent as an action step.

---

## Demo Usage

### Running the demo prompts

Use the same prompts from `demo_script.md` against this Copilot Studio agent.
Since it wraps the advanced Fabric data agent, it should produce correct answers
for all 10 prompts with improved formatting and conversational context.

### Comparing simple vs. advanced in Copilot Studio

To run the side-by-side demo in Copilot Studio instead of the Fabric UI:

1. Create a second Copilot Studio agent ("WWI Simple Assistant") that connects
   to the **simple** Fabric data agent instead
2. Use the same instructions from Section 1 above (the Copilot Studio
   instructions are the same — only the underlying Fabric data agent differs)
3. Run prompts against both agents to show the same simple-vs-advanced contrast

This is effective for demos where the audience is more familiar with a chat
interface than the Fabric data agent UI.

### Publishing to Teams

1. In the agent editor, go to **Channels**
2. Select **Microsoft Teams**
3. Follow the prompts to publish to your Teams environment
4. Users can then chat with the agent directly in Teams
