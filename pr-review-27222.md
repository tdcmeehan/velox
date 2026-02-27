# Review of PR #27222: Cost-based MV candidate selection for query rewriting

## Summary

This PR changes materialized view (MV) query rewriting from "take the first compatible MV" to "collect all compatible MVs, cost them, and pick the cheapest." It introduces a 4-stage pipeline:

1. **AST Rewriting** (`MaterializedViewQueryOptimizer`): Collects all compatible MV rewrites into a new `QueryWithMVRewriteCandidates` AST node
2. **Semantic Analysis** (`StatementAnalyzer`): Analyzes each candidate's rewritten query
3. **Logical Planning** (`RelationPlanner`): Creates `MVRewriteCandidatesNode` holding the original plan + all candidate plans
4. **Cost-Based Selection** (`SelectLowestCostMVRewrite`): Costs each candidate and picks the winner

Gated by session property `materialized_view_query_rewrite_cost_based_selection_enabled` (default: false).

---

## Key Question: Aren't there already ways to hold multiple plans and choose the lowest cost?

**Short answer: Not at the framework level — and while `ReorderJoins` provides a pattern for doing it within a single rule, that pattern doesn't apply here due to how MV compensation works.**

### Presto's optimizer architecture

Presto's optimizer is a **sequential, rule-based, single-plan-at-a-time** system — NOT a Cascades/Volcano-style optimizer:

- **70+ sequential phases**, each accepting one plan tree and producing one plan tree
- The `IterativeOptimizer` applies rules greedily — when a rule fires, it **unconditionally replaces** the matched sub-tree. There is no framework-level mechanism to hold two alternatives side-by-side and compare costs
- Presto's `Memo` class stores exactly **one operator per group**, not multiple equivalent expressions. It's used for efficient in-place mutation, not multi-plan comparison
- A [New Optimizer wiki page](https://github.com/prestodb/presto/wiki/New-Optimizer) describes a proper Cascades-style design, but it has been "work in progress" since **2017** and is not implemented

### Where cost comparison already exists

Cost-based comparison happens **inside individual rules** today:

- **`ReorderJoins`**: Internally enumerates multiple join orderings via dynamic programming, costs them all, and emits the single cheapest as the rule's output. The alternatives never exist at the framework level — they're internal to the rule.
- **`DetermineJoinDistributionType`**: Similarly picks between broadcast/partitioned internally

This is the established pattern: enumerate alternatives, cost them, emit the winner — all self-contained within one rule.

---

## Why the PR can't follow the `ReorderJoins` pattern

### MV compensation happens at the AST level, not the plan level

This is the critical architectural constraint. Presto's `MaterializedViewQueryOptimizer` is an **AST rewriter** — it transforms the original `QuerySpecification` into a new SQL AST that reads from the MV table with compensation operations baked in:

1. **Table replacement**: `FROM base_table` → `FROM materialized_view`
2. **Column remapping**: base table columns → MV columns via `baseToViewColumnMap`
3. **Aggregate compensation**: `COUNT(x)` → `SUM(mv_count_x)` (re-aggregation over pre-aggregated MV data)
4. **Filter compensation**: additional WHERE predicates if the MV's filter is broader than the query's
5. **GROUP BY compensation**: rollup aggregation if the MV pre-aggregates at a finer granularity

Each MV candidate produces a structurally different `QuerySpecification` — essentially a different SQL query. For example:

```sql
-- Original query:
SELECT region, SUM(revenue) FROM sales WHERE date >= '2024-01-01' GROUP BY region

-- MV1 (daily aggregates): needs rollup compensation
SELECT region, SUM(daily_revenue) FROM mv_daily_sales WHERE date >= '2024-01-01' GROUP BY region

-- MV2 (monthly aggregates): no rollup needed, but different columns
SELECT region, monthly_revenue FROM mv_monthly_sales WHERE month >= '2024-01'
```

### The sequencing problem

To cost a candidate, you need a plan. To get a plan, you need analysis. To get analysis, you need the compensated AST:

```
AST rewriting (compensation) → Analysis → Planning → [Optimization] → Costing
         ↑                                                              ↑
    happens here                                                  need this
```

`ReorderJoins` works entirely in the optimization phase — it rearranges existing, already-planned `JoinNode`s. It never introduces new tables, never needs type resolution, never invokes the analyzer. But MV compensation produces **new SQL ASTs referencing different tables with different schemas** that must traverse analysis → planning before they can be costed.

### Specific barriers to a `ReorderJoins`-style approach

1. **You'd need to invoke the analyzer + planner from within an optimizer rule.** The `Rule.Context` interface provides `CostProvider`, `StatsProvider`, `IdAllocator` — but NOT access to `StatementAnalyzer`, `RelationPlanner`, or `QueryPlanner`. Injecting the full analysis+planning pipeline into a rule would be architecturally unprecedented.

2. **The plans need optimization before costing is meaningful.** Even if you could produce plans inside the rule, they'd be un-optimized (no predicate pushdown, no join reordering, no column pruning). Comparing "scan base table with complex compensating aggregation" vs "scan MV with simple projection" requires at least partial optimization to produce reliable cost estimates.

3. **Each MV introduces entirely new table references.** `ReorderJoins` shuffles existing, already-resolved plan nodes. MV rewriting introduces `TableScanNode`s for tables the original query never referenced — requiring catalog lookups, schema resolution, column handle resolution, and permission checks that all happen during analysis, not optimization.

### What would the real alternative be?

The proper alternative would be Calcite-style **plan-level MV rewriting** — where compensation is computed as plan-level operations (inserting `FilterNode`, `AggregationNode`, `ProjectNode` on top of an MV `TableScanNode`) rather than AST-level SQL rewriting. This would allow MV selection to happen entirely within the optimizer. But it would require:

- MV definitions stored as plan trees (not SQL text)
- Plan nodes that carry full schema information (Calcite's `RelNode` is self-contained; Presto's `PlanNode` relies on the separate `Analysis` object)
- A Cascades-style optimizer with memo structures to naturally hold multiple equivalent plans

This is essentially the [New Optimizer](https://github.com/prestodb/presto/wiki/New-Optimizer) effort — a major architectural overhaul, not a pragmatic fix.

### Assessment of the PR's approach

Given Presto's current architecture, the 4-stage pipeline is a **reasonable pragmatic choice**:

- Stages 1-3 (AST rewriting, analysis, planning) happen in the normal compilation pipeline, where each candidate gets a proper plan
- Stage 4 (cost selection) happens as an optimizer rule where `CostProvider` is available
- The `MVRewriteCandidatesNode` is conceptually analogous to `ReorderJoins`'s internal enumeration — it's just that the enumeration must be spread across compilation phases rather than contained in one rule

The tradeoff is real but acceptable: the new plan node type adds visitor maintenance burden, but the alternative (plan-level MV rewriting) is a much larger effort.

---

## Remaining design concerns

### 1. Costing happens at a potentially unreliable point in the pipeline

The `SelectLowestCostMVRewrite` rule is registered in `PlanOptimizers.java` using `costCalculator` (not `estimatedExchangesCostCalculator`). Key questions:

- **Where in the pipeline is this positioned?** If it runs before predicate pushdown, join reordering, and other transformations, the cost estimates for candidates will be based on unoptimized sub-plans and may be misleading. Ideally this rule should run after basic simplification passes but before physical planning.
- **Why `costCalculator` instead of `estimatedExchangesCostCalculator`?** Other cost-sensitive rules like `ReorderJoins` use `estimatedExchangesCostCalculator`. The PR should document this choice.
- **Do other optimizer rules fire on the sub-plans inside `MVRewriteCandidatesNode`?** If predicate pushdown, column pruning, etc. don't traverse into the candidate sub-plans, then the costs being compared are for un-optimized plans — making the comparison unreliable.

### 2. The unknown-cost handling has an asymmetric bias

From the `SelectLowestCostMVRewrite.apply()` method:

```java
// Skip candidates with unknown costs
if (candidateCost.hasUnknownComponents()) {
    continue;
}

// Compare costs: if candidate is cheaper, select it
if (lowestCost.hasUnknownComponents() ||
        costComparator.compare(session, candidateCost, lowestCost) < 0) {
    lowestCost = candidateCost;
    selectedPlan = candidate.getPlan();
}
```

This means:
- If the **original** has unknown cost and **any** candidate has computable cost → candidate wins (even if its cost is high)
- If **all** candidates have unknown cost → original wins (even if its cost is also unknown)
- If both original and candidate have known cost → lowest wins

The asymmetry is concerning: when statistics are missing for the base table but present for an MV (or vice versa), the selection is driven by which side happens to have stats rather than which is actually cheaper. This should be documented and may warrant a more conservative default (prefer original when costs can't be meaningfully compared).

### 3. Data consistency check is bypassed

In `MaterializedViewQueryOptimizer`, the data consistency check is skipped when cost-based selection is enabled:

```java
if (!isMaterializedViewDataConsistencyEnabled(session) ||
    isMaterializedViewQueryRewriteCostBasedSelectionEnabled(session)) {
```

This means enabling cost-based MV selection implicitly disables data consistency validation. This seems like a correctness concern that should be called out explicitly — it should be handled independently, not tied to the cost-based selection flag.

### 4. Projection mapping assumes positional correspondence

```java
for (int i = 0; i < expectedOutputs.size(); i++) {
    VariableReferenceExpression expectedVar = expectedOutputs.get(i);
    VariableReferenceExpression selectedVar = selectedOutputs.get(i);
    assignments.put(expectedVar, selectedVar);
}
```

The projection maps output variables by **position**, not by name or semantic meaning. If two MV rewrites produce the same columns in different orders, this will silently produce incorrect results. The `ValidateDependenciesChecker` only checks that output variable **counts** match, not that the semantic mapping is correct.

### 5. Test gaps

- **No test for feature disabled**: All tests set `materialized_view_query_rewrite_cost_based_selection_enabled=true`. Need a test verifying the rule is a no-op when disabled.
- **No integration test**: All tests use synthetic `ValuesNode`/`FilterNode` plans. No test verifies the full pipeline from SQL string → MV rewriting → cost selection → correct output.
- **No test for interaction with other optimizer phases**: Does predicate pushdown work correctly through `MVRewriteCandidatesNode`? What about column pruning?
- **Projection test is weak**: `testAddsProjectionForDifferentOutputVariables` doesn't verify which candidate was selected — only that a `ProjectNode` was added.
- **No test for the data consistency bypass**: The PR silently disables data consistency checks; this should have explicit test coverage.

---

## Summary

The feature goal (cost-based MV selection) is sound and addresses a real limitation. The architectural approach — carrying multiple candidate plans through the compilation pipeline in `MVRewriteCandidatesNode` — is a pragmatic necessity given that MV compensation happens at the AST level. The `ReorderJoins` pattern doesn't apply because MV rewriting crosses the AST→analysis→planning boundary, unlike join reordering which operates entirely within the plan domain.

The main concerns are:

1. **Cost reliability**: The quality of the cost comparison depends critically on where `SelectLowestCostMVRewrite` sits in the optimizer pipeline and whether candidate sub-plans get optimized before costing. This needs documentation and possibly integration tests demonstrating the cost estimates are meaningful.

2. **Correctness risks**: Positional output mapping, data consistency bypass, and asymmetric unknown-cost handling all have potential correctness implications that need more careful treatment.

3. **Testing**: Needs integration tests, disabled-state tests, and stronger assertions on the projection path.
