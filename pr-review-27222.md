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

**Short answer: Not at the framework level — but there is a well-established pattern for doing it within a single rule, and this PR doesn't follow that pattern.**

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

## Design Concerns

### 1. The `MVRewriteCandidatesNode` approach diverges from the established `ReorderJoins` pattern

`ReorderJoins` handles multi-plan comparison entirely within the rule — it enumerates alternatives, costs them, and returns the winner. No special plan node needed.

This PR takes a fundamentally different approach: it creates an `MVRewriteCandidatesNode` that carries unresolved alternatives **through** the plan tree across multiple optimizer phases, then has a later rule resolve it.

This means `MVRewriteCandidatesNode` must be handled by **every visitor** that traverses the plan tree between creation and resolution:
- `ValidateDependenciesChecker` needed new code
- `PlanVisitor` needed a new method
- `Patterns` needed a new matcher
- Any future plan visitors must also account for this node type

If the resolve rule doesn't fire (e.g., due to a bug, optimizer ordering issue, or the session property being toggled mid-optimization), you get a plan node with **no physical execution semantics** reaching the execution engine.

**Recommendation**: Follow the `ReorderJoins` pattern — do the cost comparison at plan creation time in `RelationPlanner`, or in a single self-contained `PlanOptimizer` implementation (not an `IterativeOptimizer` rule) that materializes all candidate plans, costs them, and emits only the winner. This avoids needing a new plan node type entirely.

### 2. Costing happens at a potentially unreliable point in the pipeline

The `SelectLowestCostMVRewrite` rule is registered in `PlanOptimizers.java` using `costCalculator` (not `estimatedExchangesCostCalculator`). Looking at where it's placed:

```java
builder.add(new IterativeOptimizer(
    metadata, ruleStats, statsCalculator, costCalculator,
    ImmutableSet.of(new SelectLowestCostMVRewrite(costComparator))));
```

Key questions:
- **Where in the pipeline is this positioned?** If it runs before predicate pushdown, join reordering, and other transformations, the cost estimates for candidates will be based on unoptimized sub-plans and may be misleading
- **Why `costCalculator` instead of `estimatedExchangesCostCalculator`?** Other cost-sensitive rules like `ReorderJoins` use `estimatedExchangesCostCalculator`. The PR should document this choice

### 3. The unknown-cost handling has an asymmetric bias

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

### 4. Data consistency check is bypassed

In `MaterializedViewQueryOptimizer`, the data consistency check is skipped when cost-based selection is enabled:

```java
if (!isMaterializedViewDataConsistencyEnabled(session) ||
    isMaterializedViewQueryRewriteCostBasedSelectionEnabled(session)) {
```

This means enabling cost-based MV selection implicitly disables data consistency validation. This seems like a correctness concern that should be called out explicitly — it should be handled independently, not tied to the cost-based selection flag.

### 5. Projection mapping assumes positional correspondence

```java
for (int i = 0; i < expectedOutputs.size(); i++) {
    VariableReferenceExpression expectedVar = expectedOutputs.get(i);
    VariableReferenceExpression selectedVar = selectedOutputs.get(i);
    assignments.put(expectedVar, selectedVar);
}
```

The projection maps output variables by **position**, not by name or semantic meaning. If two MV rewrites produce the same columns in different orders, this will silently produce incorrect results. The `ValidateDependenciesChecker` only checks that output variable **counts** match, not that the semantic mapping is correct.

### 6. Test gaps

- **No test for feature disabled**: All tests set `materialized_view_query_rewrite_cost_based_selection_enabled=true`. Need a test verifying the rule is a no-op when disabled.
- **No integration test**: All tests use synthetic `ValuesNode`/`FilterNode` plans. No test verifies the full pipeline from SQL string → MV rewriting → cost selection → correct output.
- **No test for interaction with other optimizer phases**: Does predicate pushdown work correctly through `MVRewriteCandidatesNode`? What about column pruning?
- **Projection test is weak**: `testAddsProjectionForDifferentOutputVariables` doesn't verify which candidate was selected — only that a `ProjectNode` was added.
- **No test for the data consistency bypass**: The PR silently disables data consistency checks; this should have explicit test coverage.

---

## Summary

The feature goal (cost-based MV selection) is sound and addresses a real limitation. However:

1. **Architecture**: The approach of carrying unresolved multi-plan nodes through the plan tree diverges from the established `ReorderJoins` pattern and introduces unnecessary complexity. Consider resolving MV selection within a single self-contained step.

2. **Correctness risks**: Positional output mapping, data consistency bypass, and unknown-cost handling all have potential correctness implications that need more careful treatment.

3. **Testing**: Needs integration tests, disabled-state tests, and stronger assertions on the projection path.
