# Horizon A/B pre-registration — 2026-09-22

**STATUS: DRAFT — decision-rule numbers below are proposals. They become FROZEN when this
file merges to main; the merge commit's timestamp is the pre-registration proof. No edits
after merge except the Readout section, which is appended once, after the evidence run.**

## Why this exists

The 2026-09-22 agg_rank audit measured, on live prod data, that ~80% of all BUY-vote weight
on the serving board comes from short-horizon cells (fut_lag 1-7 months), ~20% from mid
(8-13), and 0% from long (20-33) — while the book's holding rule is ~12 months (HZ=12).
Entry timing is therefore driven by horizons the book does not trade: a recent pop lights up
several correlated short cells at once, density crosses the 0.3 BUY floor, votes accumulate,
and the name is adopted near a local top ("buys right before downturns" on the Lifecycle
sketch). Long-horizon cells contribute nothing to timing because the per-month alpha bar
mechanically decays with horizon.

This file freezes the QUESTION — does a horizon-matched action rule select better 12-month
entries than the current all-horizon mix? — so the answer cannot be argued post-hoc. Same
discipline as `docs/frozen_gate_2026-08-13.md` (gate freeze) and
`docs/qualstream_verdict_preregistration_2026-09-01.md` (grading verdict).

The live money rule does not change in ANY branch of this test before the frozen-gate
checkpoint 2027-02-13.

## Arms (frozen)

All arms use identical machinery except the future-horizon universe. Credibility keying =
`rekeyed` (stable id, no vol dimension — matches the live join since the 2026-07-24 grain
fix). Action rule per arm = the live literals verbatim: per-row BUY iff density >= 0.3 AND
n_cells >= 3; per-row weight LN(1 + n_cells); global BUY iff weighted margin >= 2 AND 2x
dominance (mirrors `return_cluster_ticker_global_action_current.sql`). No per-arm threshold
tuning, ever.

| arm        | fut_lag universe | role |
|------------|------------------|------|
| `mix`      | {1,2,4,7,12,20,33} | control = live universe, same-generation comparator |
| `fut_ge12` | {12,20,33}         | treatment: horizon-matched to the ~12mo book |
| `fut_le7`  | {1,2,4,7}          | falsification: if it beats fut_ge12, the theory dies |
| `fut_ge20` | {20,33}            | exploratory only; expected thin (label starvation is a known input property, NOT evidence about horizon quality); never re-thresholded |

## Stage 1 — historical backtest (84 quarterly cutoffs, 2004Q1-2024Q4)

Substrate: `scripts/walk_forward_serving_ic.py` arm variants →
`validation.walk_forward_serving_rank` (per cutoff/variant/id/ticker: density, n_cells,
score) → `validation.walk_forward_horizon_arm_grades` (dbt): derives each arm's global
BUY entries per cutoff and grades them with REAL adjusted-close prices vs SPY (entry = first
close after cutoff; exits at +12/+7/+4 months), with the as-of liveness gate from
`walk_forward_top_picks.sql`.

The stored holdout label is SMOOTHED; therefore real-price grading is the primary basis.
`fwd_12` (smoothed) and serving IC/decile outputs are secondary diagnostics.

### Disclosed approximations (identical across arms; relative claims only)
- Cell vote = sign(train_future_mean), not the minted STRONG_PICK/BUY labels (never
  snapshotted historically).
- No cluster_is_tradeable / archetype replay (would be lookahead); density sign carries
  direction.
- Coverage proxy = post-tier-filter n_cells.
- Absolute BUY counts will not match the live board; only arm-vs-arm comparison is claimed.

### Metrics
- PRIMARY: pooled mean and per-cutoff median of `real_excess_pct_12` per BUY entry
  (equal-weight per entry, real prices vs SPY).
- SECONDARY (reported, non-binding): 7mo and 4mo excess; per-cutoff hit rate; within-id
  Spearman IC and decile spread at F=12 from `walk_forward_serving_ic`; entries per cutoff;
  quarter-over-quarter turnover (1 − repeat-entry share).

### Decision rule (DRAFT until merge; then frozen)
GO to Stage 2 iff ALL of:
- (a) `fut_ge12` pooled mean 12mo excess exceeds `mix` by >= **2.0pp**;
- (b) per-cutoff win share (cutoffs where both arms emit >= 1 entry) >= **55%**;
- (c) `fut_le7` does NOT beat `fut_ge12` on (a) — if it does, the horizon-matching theory is
  falsified regardless of (a)/(b): verdict = investigate, do not ship;
- (d) validity floor: `fut_ge12` emits >= **300** total entries across >= **30** cutoffs —
  otherwise the verdict is "insufficient signal", which is NOT a `mix` win.

### Readout classification (pre-registered posteriors)
Prior going in: P(horizon mismatch is real and costly) ≈ 70%. The readout MUST state which
one case occurred; the posterior moves accordingly and no further than:

| outcome | definition | posterior |
|---------|-----------|-----------|
| clean GO | (a)-(d) met with wide margin (>= 2x the (a) threshold) | ~80-85% |
| marginal GO | (a)-(d) met, margins near thresholds | ~75% |
| starved NO-GO | (d) unmet | ~60-65% (barely updates) |
| clean NO-GO | (d) met, `mix` wins (a) outright | ~35-40% |

Reasons these caps exist (on record so they cannot be argued later): the 84 cutoffs are
~20-30 effective samples (overlapping 12mo windows, regime clustering); the harness shares
DNA with the system it tests (sign-vote approximation, smoothed-label substrate — mitigated
by real-price primary); pooled results average over regimes including ones where the live
edge was negative (2020-21).

## Stage 2 — live shadow (build ONLY on GO)

Shadow twins of summary/global_action with `fut_lag >= 12`, shadow ledger + grader
(separate tables; the live ledger's row population is frozen), non-blocking DAG leaf.
No historical backfill is possible (daily cell states are not stored): the shadow accrues
from deploy date. 2027-02-13 checkpoint = 4/7mo partials only; first 12mo-mature shadow
entries ~Oct 2027 → the binding Stage-2 verdict is 2027-08-13 at the earliest.
Comparison: shadow BUY entries vs live ledger BUY entries over identical calendar windows on
excess-vs-SPY, plus dwell/turnover.

## Preferred end-state (if GO)

The hard floor is a SCREENING test, not the end design. On GO, the build that follows is
credibility re-targeted to the holding horizon — every cell weighted by its measured
walk-forward ability to predict 12-month outcomes — so the data, not a floor or a hand-tuned
curve, decides how much short cells count. Hand-tuned horizon weights are rejected outright
(tunable curve chosen mid-drawdown = the re-tuning the freeze forbids). A "long decides WHAT,
short decides WHEN" two-stage rule is a separate hypothesis; if ever pursued it gets its own
pre-registration.

## Snapshot integrity (A1, run before the evidence run)

The Sep-1 failed re-mint restored the guarded evidence tables to the July generation, but
`walk_forward_ticker_holdout` is OUTSIDE `wf_evidence_guard.py`'s restore scope. Before the
readout run: verify per-cutoff generation coherence across walk_forward_results /
walk_forward_ticker_holdout / walk_forward_cluster_id_map for all 84 cutoffs, plus a full
`--variant rekeyed --dry-run` with zero unmapped/skip cutoffs. Any excluded or surgically
re-minted cutoffs are listed HERE before results are computed:

- (none yet)

## Standing prohibitions

- The live gate, live action thresholds, ledger semantics, portfolio_sync, and adopt-queue
  ordering are untouched in every branch before 2027-02-13.
- No per-arm threshold tuning; no metric substitution after the readout; no re-grades with
  hindsight.
- Cold streaks between now and the readout are not evidence against the freeze; they are the
  test's operating conditions.

## Readout

*(appended once, after the evidence run; states the outcome case from the table above)*
