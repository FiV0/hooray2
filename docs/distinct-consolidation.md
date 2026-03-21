# Distinct Consolidation in EDN Datalog (Non-Recursive Fragment)

Based on DBSP Propositions 4.5 and 4.6 (Budiu et al., 2022).

---

## The Two Propositions

**Prop 4.5 (delay distinct):** For `Q ∈ {σ, ⊲⊳, ×}`:
`ispositive(i) ⇒ Q(distinct(i)) = distinct(Q(i))`

**Prop 4.6 (strip inner distinct):** For `Q ∈ {σ, π, map(f), +, ⊲⊳, ×}`:
`ispositive(i) ⇒ distinct(Q(distinct(i))) = distinct(Q(i))`

Both require `ispositive(i)` — every element has weight ≥ 0.

---

## Operator Classification

Every operator in a non-recursive, non-aggregate `:where` + `:find` clause falls
into one of two categories:

**Positivity-preserving** (appears in Prop 4.6 list):

| Datalog construct | DBSP operator | Notes |
|---|---|---|
| Triple pattern `[?e :a ?v]` | Input Z-set (set, all weights = 1) | Always a set |
| Shared variables | `⊲⊳` (equi-join) | set ⊲⊳ set = set (1·1=1) |
| Predicate `[(> ?x 5)]` | `σ_P` | Preserves weights |
| Function `[(f ?x) ?y]` | `map(f)` | Positive, but may produce bag if f non-injective |
| `or` / `or-join` | `+` | Positive, but may produce bag (weight 2) |
| `:find` projection | `π` | Positive, but may produce bag (sums weights) |

**Positivity-breaking:**

| Datalog construct | DBSP operator | Notes |
|---|---|---|
| `not` / `not-join` / `missing?` | `⊲⊳ → (−)` or just `(−)` | Can produce negative weights |

---

## Core Rule

**In the positive fragment (no negation), defer ALL distinct operators to one
terminal distinct after the final `:find` projection.**

The pipeline:

```
scan₁ ─┐
       ⊲⊳ ─── σ_P ─── map(f) ─── + ─── π ─── distinct ─── O
scan₂ ─┘                          ↑             ^ single
scan₃ ──── (or branch) ───────────┘               terminal
```

Zero intermediate distincts. The terminal one resolves any bag-ness accumulated
from non-injective maps, projections, or unions.

**Why this is correct:** Every operator in the chain preserves positivity.
Prop 4.6 lets us strip any inner distinct when an outer one exists. By induction
over the chain, all intermediate distincts collapse into the terminal one.

---

## Negation Firewall

Set difference `distinct(a − b)` is the one construct that can produce negative
weights. The `distinct` immediately after it is **mandatory** — it cannot be deferred
or consolidated away.

```
scan₁ ─┐                              ┌─── (positive again) ─── π ─── distinct ─── O
       ⊲⊳ ──────────────────── (−) ── distinct
scan₂ ─┘                        ↑       ^ mandatory
scan₃ ──────────── (negate) ────┘         firewall
```

After the firewall, the output is a set (all weights 0 or 1), so everything
downstream is back in the positive fragment. Any further distincts can again
be deferred to the terminal position.

**Multiple negations:** Each one gets its own firewall. Between firewalls and after
the last one, no intermediate distincts are needed.

---

## Aggregation Boundary

Aggregate functions are sensitive to multiplicities:

- `count` sums weights — bag input inflates the count
- `sum` computes `Σ x · s[x]` — bag input inflates the total
- `min`/`max` only check `s[x] > 0` — multiplicity-insensitive

**A `distinct` is mandatory immediately before grouping/aggregation.** This is
where all deferred bag-ness from the positive fragment gets resolved.

```
where-clause operators ─── distinct ─── G_p ─── Agg_a ─── O
                            ^ mandatory
                              pre-aggregation
```

After `Agg_a ∘ makeset`, the output is a set (one result per group key), so no
further distinct is needed downstream.

**`:with` clause:** Widens the projection before the pre-aggregation distinct.
Variables listed in `:with` are retained for distinctness purposes but dropped
after aggregation. This changes what survives deduplication and therefore changes
aggregate results.

```
;; Without :with:  π(?cat, ?name) → distinct → G_p → count
;; With :with ?e:  π(?cat, ?name, ?e) → distinct → G_p → count
```

**Optimization:** If every aggregate in the query is `min` or `max`, the
pre-aggregation distinct can be skipped (these functions are weight-insensitive).
Narrow case, likely not worth special-casing early.

---

## Combined: Negation + Aggregation

Two mandatory distinct points, nothing in between:

```
⊲⊳ ─── (−) ─── distinct ─── σ ─── π ─── distinct ─── G_p ─── Agg_a ─── O
                  ^ firewall                ^ pre-agg

                  (cannot consolidate — firewall is load-bearing)
```

The firewall cannot be removed even though there's a downstream distinct.
Without it, negative weights propagate through `σ` and `π` (they faithfully
pass through negatives), and intermediate results are wrong even if the
pre-aggregation distinct would eventually clamp them.

---

## Implementation Strategy

Track a `known_set: bool` flag during query plan construction:

1. **Start:** `known_set = true` (index scans produce sets)
2. **After `⊲⊳` of two sets:** stays `true` (1·1=1)
3. **After `σ_P`:** preserves flag
4. **After `map(f)`, `π`, `+`:** set to `false` (may produce bag)
5. **Before negation `(−)`:** emit `distinct` if `known_set = false`, then proceed;
   emit mandatory firewall `distinct` after the difference; set `known_set = true`
6. **Before `G_p` (aggregation):** emit `distinct` if `known_set = false`
7. **At terminal position (`:find`):** emit `distinct` if `known_set = false`

This produces the minimum number of `distinct` nodes, which matters because
`distinct` is the only non-linear unary operator and the most expensive to
incrementalize — it requires the `H` function circuit with `I + z⁻¹` state
(DBSP Prop 4.7).

---

## Count of Distinct Operators (Worst Case)

| Query shape | Distinct count |
|---|---|
| Positive only, no aggregation | 1 (terminal) |
| Positive only, with aggregation | 1 (pre-agg; terminal unnecessary) |
| One negation, no aggregation | 2 (firewall + terminal) |
| One negation, with aggregation | 2 (firewall + pre-agg) |
| N negations, with aggregation | N+1 (N firewalls + pre-agg) |

In all cases, zero intermediate distincts between the mandatory boundaries.
