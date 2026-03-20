# Fabric Data Agent Configs

Two Fabric data agent configurations that demonstrate how instruction quality affects query accuracy over the same data warehouse.

## The Demo Thesis

An LLM-powered data agent can discover tables and columns on its own via schema introspection. But **schema discovery is not enough** — the agent cannot infer:

- That `Valid_From` / `Valid_To` columns define temporal ranges for point-in-time joins (SCD2)
- That `Allocation_Weight` must be multiplied into revenue to avoid double-counting (bridge tables)
- That `Primary_Supplier_Key` and `Substitute_Supplier_Key` both need to be joined to the supplier dimension (dual-key pattern)

The simple agent finds the right tables but uses them wrong. The advanced agent gets correct answers because it was taught the rules.

## Agent Configs

### Simple Agent ([simple_agent.md](simple_agent.md))

A deliberately minimal configuration:

- **Agent Instructions:** Basic — lists only the 14 base tables, no enrichment tables
- **Data Source Description:** Blank
- **Data Source Instructions:** Blank
- **Example Queries:** 4 basic single-table queries

The simple agent will handle straightforward queries (total revenue, top customers) correctly, but silently produces wrong answers for anything involving SCD2, bridge tables, or weighted allocations.

### Advanced Agent ([advanced_agent.md](advanced_agent.md))

A fully-instrumented configuration using all 4 GA config sections:

- **Agent Instructions:** Persona, objective, terminology glossary, response guidelines, topic-specific routing rules
- **Data Source Description:** High-level summary for question routing
- **Data Source Instructions:** Full table documentation, SCD2 temporal join patterns, bridge table rules, weight logic
- **Example Queries:** 14 queries covering basic, SCD2 temporal, bridge weighted, bridge supplier, and combined patterns

### Copilot Studio Agent ([copilot_studio_agent.md](copilot_studio_agent.md))

A Copilot Studio agent that wraps the advanced Fabric data agent:

- Model selection (GPT-5), critical settings (disable web search, general knowledge, code interpreter)
- Suggested prompts matching the demo script
- 10-case test set for agent evaluation

## Demo Script ([demo_script.md](demo_script.md))

10 prompts run side-by-side against both agents:

| Prompt | Simple Agent | Advanced Agent |
| --- | --- | --- |
| 1. Total 2016 revenue | Correct | Correct |
| 2. Tailspin Toys buying group members | Wrong — current only | Correct — current + historical |
| 3. Stock item 50 price in 2015 | Wrong — today's price | Correct — 85% historical price |
| 4. Supplier cost exposure for item 10 | Partial — misses cost analysis | Correct — cost premium + lead time |
| 5. 2016 revenue by segment | **Visibly wrong** — totals > 140% of actual | Correct — weighted, sums to prompt 1 |
| 6. 2015 segment revenue by buying group | Catastrophically wrong | Correct — temporal + weighted |

Prompt 5 is the showstopper: the audience already knows the 2016 total from prompt 1, and the simple agent's segment breakdown visibly exceeds it — proving double-counting without any explanation needed.

## Narrative Arc

1. **"It works!"** (Prompt 1) — Both agents handle simple queries. Build confidence.
2. **"But wait..."** (Prompts 2-5) — The simple agent silently returns wrong answers that look plausible.
3. **"The fix"** (Prompts 2-6 on advanced) — Same prompts, correct answers. The difference is documentation.
4. **"Going further"** (Prompts 7-10) — The advanced agent handles novel analytical questions.

## GA Configuration Model (March 2026)

Fabric data agents use 4 distinct configuration sections:

| Section | Scope | Purpose |
| --- | --- | --- |
| **Agent Instructions** | Agent-level | Persona, terminology, routing rules |
| **Data Source Description** | Per data source | High-level summary for question routing |
| **Data Source Instructions** | Per data source | Table docs, join patterns, business rules |
| **Example Queries** | Per data source | Few-shot SQL examples (top 3 retrieved by similarity) |

The simple agent leaves 2 of 4 sections blank. The advanced agent fills all 4. That's the entire difference.
