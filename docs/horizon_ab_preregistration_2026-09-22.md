# Horizon A/B pre-registration — 2026-09-22

**STATUS: DRAFT — decision-rule numbers below are proposals. They become FROZEN when this
file merges to main; the merge commit's timestamp is the pre-registration proof. No edits
after merge except the Readout section, which is appended once, after the evidence run.**

**Revision 2026-09-23 (adversarial re-audit, 35 agents).** The design is sound (band mechanics
correct, no look-ahead, freeze-clean); three suspected flaws were refuted on data (recency
truncation, survivorship deletion, DB mis-targeting). Two real problems are fixed here BEFORE
freeze, because the merge freezes the thresholds: (1) a **selectivity/attribution confound** —
banding shrinks each arm's cell pool so the fixed thresholds bite harder on narrow arms; a
fut_ge12 win on the old pooled-mean bar could be selectivity, not horizon; (2) the old rule was
**under-powered and used the wrong standard error** (independent-sample assumptions on nested,
overlapping-window arms). Both are resolved by the new BINDING statistic below (paired,
selectivity-matched, per-cutoff, bootstrap CI). The numbers are still Kevin's to freeze.

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
All 12mo grades are filtered to `mature_12` (the stock's own exit bar reached ~12 months, so a
mid-window delisting cannot difference a PARTIAL stock move against SPY's FULL-window move).
- BINDING (the primary estimand): the **selectivity-matched paired per-cutoff difference**. For
  each cutoff where both arms emit >= 1 mature entry, truncate `mix` to its top-N-by-conviction
  (`buy_n_cells`) entries where N = `fut_ge12`'s entry count that cutoff, then take
  median(fut_ge12) − median(mix_topN); the statistic is the MEDIAN of that per-cutoff series with
  a stationary-block bootstrap CI over cutoffs (block ~4 to respect overlapping 12mo windows).
  Computed by `scripts/walk_forward_horizon_arm_readout.py`. Rationale: pairing on common cutoffs
  differences out the shared regime shock (the dominant variance the old pooled-mean bar
  mis-handled); the top-N-by-conviction truncation removes the band-selectivity confound so a win
  is attributable to HORIZON, not to "the narrow arm just got more selective"; the median is
  tail-robust.
- REPORTED (context, non-binding): the RAW (un-matched) paired median-diff and pooled
  means/medians per arm; per-arm `buy_n_cells` and entries-per-cutoff distributions (so a
  conviction/selectivity gap is visible); 7mo/4mo excess; within-id Spearman IC and decile spread
  at F=12 from `walk_forward_serving_ic`; turnover (1 − repeat-entry share). Size/liquidity tilt
  is NOT reported (the price feed carries adj_close only; the ~66-name mega-cap universe bounds it).

### Decision rule (DRAFT until merge; then frozen — numbers are PROPOSALS Kevin sets)
GO to Stage 2 iff ALL of:
- (a) BINDING: the selectivity-matched paired median-diff >= **2.0pp** AND its bootstrap CI lower
  bound > **0** (proposed CI level **90%**). Replaces the old "pooled-mean beats by 2pp" bar,
  which was regime-uncontrolled, tail-sensitive, and used the wrong SE for nested arms.
- (b) breadth: selectivity-matched per-cutoff win share >= **55%** (proposed). NOTE: on ~30
  effective cutoffs a 55% share is weak as a standalone significance test (near coin-flip); it is
  a secondary robustness check, NOT the primary evidence — the CI in (a) carries the weight.
- (c) falsification: `fut_le7` does NOT beat `fut_ge12` on (a). WEAK/CONSERVATIVE — the arms are
  selectivity-mismatched (fut_le7 has 4 fut-lags vs fut_ge12's 3), so this is directional only:
  if it trips, verdict = investigate, do not ship; it cannot by itself license a GO.
- (d) validity floor: `fut_ge12` emits >= **300** total MATURE entries across >= **30** cutoffs —
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
DNA with the system it tests (sign-vote approximation whose error is itself horizon-correlated,
smoothed-label substrate — mitigated by real-price primary + the Stage-2 live-mechanism shadow);
pooled results average over regimes including ones where the live edge was negative (2020-21).

**What a NO-GO does and does NOT refute (scope, on record).** A NO-GO refutes only the narrow
tested claim: "restricting the voting pool to fut>=12 does not improve 12mo entries under the
frozen action rule." It does NOT refute the broader belief that the horizon mismatch is costly —
the dual remedy (SHORTEN the hold to match the short signal; "long decides WHAT, short decides
WHEN") is not tested here and keeps its own prior. It also does NOT invalidate the continuous
re-target end-state (below): the hard >=12 cut is a strictly more extreme intervention, so a
hard-cut NO-GO is weaker evidence against a data-driven downweight than against the hard rule.
The 4mo/7mo secondary excess per arm is where a "short signal good, held too long" pattern would
show; read it before concluding.

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

Neither Stage 1 nor the Stage-2 hard-band shadow exercises the continuous mechanism's per-cell
12mo-ability estimator (an estimation step with its own look-ahead/overfit exposure). So the
continuous re-target does NOT inherit this test's verdict: on GO it gets its OWN pre-registration
with an embargoed, overfit-controlled validation before it can replace the live rule. A GO here
only authorizes BUILDING the Stage-2 hard-band shadow.

## Snapshot integrity (A1, run before the evidence run)

The Sep-1 failed re-mint restored the guarded evidence tables to the July generation, but
`walk_forward_ticker_holdout` is OUTSIDE `wf_evidence_guard.py`'s restore scope. Before the
readout run, ALL of the following must pass (the re-audit strengthened this list):
- **Band-stratified generation coherence** across walk_forward_results /
  walk_forward_ticker_holdout / walk_forward_cluster_id_map for all 84 cutoffs, stratified BY
  fut_lag (not just per-cutoff):
  `psql "$DATABASE_URL" -f elt-inference-models/scripts/horizon_ab_a1_band_coherence.sql` must
  return ZERO rows. A fut_lag-localized generation skew maps onto the treatment band and would
  masquerade as a horizon effect — the single most dangerous substrate artifact for a test
  defined on fut_lag.
- A full `--variant rekeyed --dry-run` with zero unmapped/skip cutoffs.
- **Same-DB assertion**: `walk_forward_serving_ic.py` (DATABASE_URL) and the dbt model
  (DB_DATABASE) target the same database, and serving_rank arm-variant row counts are non-zero in
  the DB dbt reads, before the grades are trusted. (Verified 2026-09-23: both = `prod`.)
- **mix == rekeyed tripwire**: `variant='mix'` must equal the stored `rekeyed` variant
  row-for-row (same math, different namespace) — a free snapshot-drift check.
- **Entry-count probe**: confirm `fut_ge12` can plausibly reach the (d) floor (300/30) via a cheap
  yearly-cutoff `--variant arms --dry-run` BEFORE committing the ~12h full run; if not, the band
  or a per-arm floor relaxation must be pre-registered HERE, not chosen after seeing results.

At READOUT: snapshot the price slice used (materialize the 84-cutoff entry/exit/SPY closes into a
frozen table, or record a data-vintage hash) and cite it in the Readout section — the grade reads
a LIVE price table, so without this a re-run on another day drifts and the "frozen verdict" is not
reproducible.

Any excluded or surgically re-minted cutoffs are listed HERE before results are computed:

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
