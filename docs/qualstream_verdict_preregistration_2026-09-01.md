# Qualstream grading verdict — pre-registered 2026-09-01

## Why this exists
Six weeks into live following, the within-universe selection looks inverted:
since 2026-08-14 the ten qualstream-passed board buys average **-11.0%** while
the two hardest vetoes did better (FTDR, graded 18: **-6.5%**; TSM, graded 25:
**-2.6%**; SPY **-1.2%**). In the cluster-8/9 view the passed line trails the
all-graded line. The gate itself is fine (all-graded beats SPY); the suspect is
the 68-bar ranking within the gated universe.

Sample is 10 vs 2 names over six weeks in a drawdown, and the 2026-08-25
robustness addendum found grade signal only at the 12-month horizon (p≈.08).
So no action now. This file freezes the QUESTION so the checkpoint verdict on
grading cannot be argued post-hoc, exactly like docs/frozen_gate_2026-08-13.md
froze the gate.

## Pre-registered tests (evaluate at the frozen-gate checkpoints: 2027-02-13 and 2027-08-13)
1. **Green vs orange, all clusters** (the grading test with the gate held
   constant): equal-weight realized return of qualstream-passed (>=68,
   non-vetoed) board names vs ALL graded board names, each from its own entry.
   Grading adds value iff passed > graded by a margin that survives the
   sketch's known noise. Passed <= graded at both checkpoints = the 68 bar
   selects nothing.
2. **Vetoed vs passed**: realized return of every vetoed name from its veto
   date vs the passed basket over the same windows. Vetoes are supposed to be
   the worst of the universe; if vetoed >= passed again at the checkpoints,
   the rubric's ordering is inverted, not just noisy.
3. **Adopt-queue rank IC**: within each adopt cohort, Spearman correlation of
   grade vs realized 4/12-month return. The 2026-08-25 finding (signal only at
   12mo, p≈.08) is the prior; the checkpoint asks whether live data confirms
   or kills it.

## Decision rule (written before knowing the answer)
- All three tests negative at the 6-month checkpoint -> demote qualstream to
  advisory (grades displayed, no adopt gating, vetoes kept only for
  fraud/delisting-class findings) and re-run the tests to 12 months before any
  rubric rework.
- Mixed -> no change until 12 months.
- Positive -> keep the bar, revisit the QS_CAP only.
- Nothing about the walk-forward gate changes in ANY branch (separate freeze).

## Standing prohibition
No rubric edits, bar moves, veto-policy changes, or re-grades-with-hindsight
before 2027-02-13. Cold streaks are not evidence against the test; they are
the test.
