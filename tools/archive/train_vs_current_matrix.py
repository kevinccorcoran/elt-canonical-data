#!/usr/bin/env python
"""AlphaStream data-flow: comparison matrix + mini-timeline (variant 3-matrix)."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle
from matplotlib.path import Path
import matplotlib.patches as mpatches

import os as _os
OUT = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "train_vs_current_matrix.png")

# ----- palette -------------------------------------------------------------
INK      = "#1f2733"
SUBINK   = "#5b6675"
PAGE     = "#ffffff"
PANEL    = "#f4f6fa"
FOUND_BG = "#e7edf6"
FOUND_ED = "#b9c8de"
FOUND_TX = "#27486f"

# soft row tints (restrained, coordinated)
LIVE_T   = "#e9f3ee"; LIVE_E = "#9ecbb4"; LIVE_K = "#1f7a52"   # green = live
BACK_T   = "#fdeeea"; BACK_E = "#eeae9d";  BACK_K = "#b6622f"  # warm = train/backtest
WALK_T   = "#eceaf6"; WALK_E = "#bcb4e0";  WALK_K = "#5a45a8"  # violet = walk-forward

CONTRAST = "#c2462f"   # for the "pop" emphasis text

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "text.color": INK,
})

fig = plt.figure(figsize=(15.2, 8.6), dpi=185)
fig.patch.set_facecolor(PAGE)
ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, 100); ax.set_ylim(0, 100)
ax.axis("off")

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
ax.text(3.2, 96.2, "AlphaStream  ·  which data is used when",
        fontsize=21, fontweight="bold", color=INK, va="top")
ax.text(3.2, 91.6,
        "LIVE builds the odds from ALL history, then scores today's single latest bar.   "
        "TRAIN/BACKTEST splits history at a cutoff to score accuracy only.",
        fontsize=10.5, color=SUBINK, va="top")

# ---------------------------------------------------------------------------
# Shared foundation strip
# ---------------------------------------------------------------------------
def chip(x, w, y, h, title, sub, ax):
    box = FancyBboxPatch((x, y), w, h,
                         boxstyle="round,pad=0.02,rounding_size=1.1",
                         linewidth=1.4, edgecolor=FOUND_ED, facecolor="#ffffff",
                         mutation_aspect=1.0, zorder=3)
    ax.add_patch(box)
    ax.text(x + w/2, y + h*0.62, title, ha="center", va="center",
            fontsize=10.6, fontweight="bold", color=FOUND_TX, zorder=4)
    ax.text(x + w/2, y + h*0.27, sub, ha="center", va="center",
            fontsize=8.3, color=SUBINK, zorder=4)

# foundation backing panel
fb = FancyBboxPatch((3.0, 79.0), 94.0, 8.6,
                    boxstyle="round,pad=0.02,rounding_size=1.4",
                    linewidth=0, facecolor=FOUND_BG, zorder=1)
ax.add_patch(fb)
ax.text(4.4, 86.7, "SHARED FOUNDATION  ·  built once, all three modes read it",
        fontsize=9.2, fontweight="bold", color=FOUND_TX, va="top")

cy, ch = 79.9, 5.0
chips = [
    (6.5,  24.0, "Canonical  (cdm)", "clean daily price bars · source of truth"),
    (38.0, 24.0, "Metrics",          "excess return vs SPY · volatility · clusters"),
    (69.5, 24.0, "return_cluster_feature_set", "the feature matrix: past return → future return"),
]
for x, w, t, s in chips:
    chip(x, w, cy, ch, t, s, ax)

# connecting arrows between chips
for x0 in (30.5, 62.0):
    ar = FancyArrowPatch((x0, cy + ch/2), (x0 + 7.5, cy + ch/2),
                         arrowstyle="-|>", mutation_scale=14,
                         linewidth=2.0, color=FOUND_TX, zorder=4)
    ax.add_patch(ar)

# ---------------------------------------------------------------------------
# Comparison matrix
# ---------------------------------------------------------------------------
# column geometry (x-left, width)
L = 3.0
COL = [
    ("MODE",                       L,      16.0),
    ("When it runs",               19.0,   13.0),
    ("Data to BUILD the odds",     32.0,   21.5),
    ("Data to PREDICT",            53.5,   22.0),
    ("Purpose / output",           75.5,   21.5),
]
TABLE_RIGHT = 97.0

HEADER_Y = 73.4
HEADER_H = 3.6
ROW_TOP  = HEADER_Y          # rows start just under header
ROW_H    = 17.6
GAP      = 1.4

# Header band
hb = FancyBboxPatch((L, HEADER_Y - HEADER_H), TABLE_RIGHT - L, HEADER_H,
                    boxstyle="round,pad=0.0,rounding_size=0.8",
                    linewidth=0, facecolor="#2b3banchor", zorder=2) if False else None
hb = Rectangle((L, HEADER_Y - HEADER_H), TABLE_RIGHT - L, HEADER_H,
               linewidth=0, facecolor="#eef1f6", zorder=2)
ax.add_patch(hb)
for name, x, w in COL:
    ax.text(x + 0.8, HEADER_Y - HEADER_H/2, name.upper(),
            ha="left", va="center", fontsize=8.6, fontweight="bold",
            color=SUBINK, zorder=3)

# mini-timeline glyph: draws a horizontal axis with a shaded "used" window
def timeline(ax, x, y, w, mode):
    """mode in {'all','pre','post','live','roll'}"""
    h = 1.0
    base_y = y
    # baseline track
    ax.add_patch(Rectangle((x, base_y - h/2), w, h, facecolor="#dfe4ec",
                           edgecolor="none", zorder=4))
    if mode == "all":      # entire span filled
        ax.add_patch(Rectangle((x, base_y - h/2), w, h, facecolor=LIVE_K,
                               edgecolor="none", zorder=5))
    elif mode == "live":   # single tick at far right
        tw = w * 0.06
        ax.add_patch(Rectangle((x + w - tw, base_y - h/2), tw, h,
                               facecolor=LIVE_K, edgecolor="none", zorder=5))
    elif mode == "pre":    # left portion up to cutoff
        cw = w * 0.62
        ax.add_patch(Rectangle((x, base_y - h/2), cw, h, facecolor=BACK_K,
                               edgecolor="none", zorder=5))
        ax.plot([x + cw, x + cw], [base_y - h*1.0, base_y + h*1.0],
                color=INK, lw=1.3, zorder=6)
    elif mode == "post":   # right portion after cutoff
        cw = w * 0.62
        ax.add_patch(Rectangle((x + cw, base_y - h/2), w - cw, h,
                               facecolor=BACK_K, edgecolor="none", zorder=5))
        ax.plot([x + cw, x + cw], [base_y - h*1.0, base_y + h*1.0],
                color=INK, lw=1.3, zorder=6)
    elif mode == "roll":   # several stepped windows
        n = 4
        seg = w / (n + 1.2)
        for i in range(n):
            sx = x + i * (w - seg) / (n - 1)
            ax.add_patch(Rectangle((sx, base_y - h/2), seg, h,
                                   facecolor=WALK_K, edgecolor="white",
                                   linewidth=0.6, zorder=5,
                                   alpha=0.55 + 0.12*i))

# row content -------------------------------------------------------------
rows = [
    dict(tint=LIVE_T, edge=LIVE_E, key=LIVE_K,
         mode="LIVE / CURRENT",
         tag="inference_dbt_models",
         when="DAILY",
         when_sub="feeds dashboard",
         build_lead="ALL history",
         build_sub="every past observation →\ntransition_confidence → cell_score",
         build_tl="all",
         pred_lead="Today's latest bar",
         pred_sub="feature_set_current = newest\nbar per ticker → BUY / SELL / SKIP",
         pred_tl="live",
         purpose="ticker_global_action_current",
         purpose_sub="prediction_ledger → Dashboard;\nscored vs SPY going forward",
         contrast_build=False, contrast_pred=False, islive=True),
    dict(tint=BACK_T, edge=BACK_E, key=BACK_K,
         mode="BACKTEST",
         tag="inference_backtest_dbt_models",
         when="DAILY",
         when_sub="validation only",
         build_lead="Pre-cutoff history only",
         build_sub="feature_set_train, strictly\nBEFORE the cutoff (no peeking)",
         build_tl="pre",
         pred_lead="Held-out post-cutoff bars",
         pred_sub="transition_scored_split scores\nthe AFTER-cutoff future",
         pred_tl="post",
         purpose="validation_agreement",
         purpose_sub="sign-agreement % +\npayoff_backtest (simulated P&L)",
         contrast_build=True, contrast_pred=True, islive=False),
    dict(tint=WALK_T, edge=WALK_E, key=WALK_K,
         mode="WALK-FORWARD",
         tag="walk_forward_dbt_models",
         when="MONTHLY",
         when_sub="42 rolling cutoffs",
         build_lead="Past slice (rolls forward)",
         build_sub="loops train/test across 42\nsemi-annual cutoffs, 2004→2024",
         build_tl="roll",
         pred_lead="Next slice each step",
         pred_sub="test the period after each\ncutoff, then step forward",
         pred_tl="roll",
         purpose="cell_credibility",
         purpose_sub="walk_forward_results → out-of-\nsample track record per cell",
         contrast_build=False, contrast_pred=False, islive=False),
]

def text_block(ax, x, y_top, lead, sub, key, contrast=False, tl_mode=None,
               tl_x=None, tl_w=None):
    lead_color = CONTRAST if contrast else INK
    ax.text(x, y_top, lead, ha="left", va="top", fontsize=10.6,
            fontweight="bold", color=lead_color, zorder=6)
    ax.text(x, y_top - 3.3, sub, ha="left", va="top", fontsize=8.5,
            color=SUBINK, zorder=6, linespacing=1.25)
    if tl_mode:
        timeline(ax, tl_x, y_top - 11.0, tl_w, tl_mode)

y = ROW_TOP - HEADER_H
for r in rows:
    y0 = y - GAP - ROW_H
    # row card
    card = FancyBboxPatch((L, y0), TABLE_RIGHT - L, ROW_H,
                          boxstyle="round,pad=0.0,rounding_size=1.0",
                          linewidth=1.2, edgecolor=r["edge"],
                          facecolor=r["tint"], zorder=2)
    ax.add_patch(card)
    # left accent bar
    ax.add_patch(Rectangle((L, y0), 0.9, ROW_H, facecolor=r["key"],
                           edgecolor="none", zorder=3))

    ytop = y0 + ROW_H - 2.6
    # --- MODE column
    mx = COL[0][1] + 1.7
    ax.text(mx, ytop, r["mode"], ha="left", va="top", fontsize=12.0,
            fontweight="bold", color=r["key"], zorder=6)
    # DAG tag pill
    pill_w = 0.0
    ax.text(mx, ytop - 4.2, "DAG", ha="left", va="top", fontsize=7.0,
            fontweight="bold", color="#ffffff", zorder=7,
            bbox=dict(boxstyle="round,pad=0.25", facecolor=r["key"],
                      edgecolor="none"))
    ax.text(mx + 3.6, ytop - 4.35, r["tag"], ha="left", va="top",
            fontsize=6.9, color=SUBINK, zorder=6, family="DejaVu Sans Mono")
    if r["islive"]:
        ax.text(mx, ytop - 8.8, "★ CURRENT", ha="left", va="top", fontsize=8.4,
                fontweight="bold", color=LIVE_K, zorder=6)
    elif r["mode"] == "BACKTEST":
        ax.text(mx, ytop - 8.8, "= TRAIN", ha="left", va="top", fontsize=8.4,
                fontweight="bold", color=BACK_K, zorder=6)

    # --- When
    wx = COL[1][1] + 0.8
    ax.text(wx, ytop, r["when"], ha="left", va="top", fontsize=11.2,
            fontweight="bold", color=INK, zorder=6)
    ax.text(wx, ytop - 3.6, r["when_sub"], ha="left", va="top", fontsize=8.4,
            color=SUBINK, zorder=6)

    # --- Build odds
    bx = COL[2][1] + 0.8
    text_block(ax, bx, ytop, r["build_lead"], r["build_sub"], r["key"],
               contrast=r["contrast_build"], tl_mode=r["build_tl"],
               tl_x=bx, tl_w=COL[2][2] - 2.0)

    # --- Predict
    px = COL[3][1] + 0.8
    text_block(ax, px, ytop, r["pred_lead"], r["pred_sub"], r["key"],
               contrast=r["contrast_pred"], tl_mode=r["pred_tl"],
               tl_x=px, tl_w=COL[3][2] - 2.0)

    # --- Purpose
    ux = COL[4][1] + 0.8
    ax.text(ux, ytop, r["purpose"], ha="left", va="top", fontsize=9.6,
            fontweight="bold", color=r["key"], zorder=6,
            family="DejaVu Sans Mono")
    ax.text(ux, ytop - 3.4, r["purpose_sub"], ha="left", va="top",
            fontsize=8.5, color=SUBINK, zorder=6, linespacing=1.25)

    y = y0

# vertical column separators (light)
sep_top = HEADER_Y
sep_bot = y + GAP
for _, x, w in COL[1:]:
    ax.plot([x, x], [sep_bot, sep_top], color="#d4dae3", lw=0.8, zorder=1)

# ---------------------------------------------------------------------------
# Feedback loop footnote: credibility = TRUST GATE back into LIVE
# ---------------------------------------------------------------------------
fy = y - 0.2
foot = FancyBboxPatch((L, fy - 6.2), TABLE_RIGHT - L, 5.6,
                      boxstyle="round,pad=0.0,rounding_size=0.8",
                      linewidth=0, facecolor=PANEL, zorder=2)
ax.add_patch(foot)

# curved feedback arrow glyph
arr = FancyArrowPatch((L + 22.0, fy - 3.4), (L + 5.5, fy - 3.4),
                      connectionstyle="arc3,rad=0.0",
                      arrowstyle="-|>", mutation_scale=15,
                      linewidth=2.0, color=WALK_K, linestyle=(0, (4, 2)),
                      zorder=4)
ax.add_patch(arr)
ax.text(L + 1.2, fy - 1.0,
        "FEEDBACK  (trust gate, not a data feed)",
        ha="left", va="top", fontsize=8.6, fontweight="bold", color=WALK_K)
ax.text(L + 24.0, fy - 1.0,
        "cell_credibility from walk-forward / backtest weights & filters which LIVE calls to trust.  "
        "Backtest data never enters live predictions directly.",
        ha="left", va="top", fontsize=9.0, color=INK, linespacing=1.3)
ax.text(L + 24.0, fy - 4.4,
        "Walk-forward: 61.2% sign agreement across 643,466 cells (June 2026).",
        ha="left", va="top", fontsize=8.6, color=SUBINK, fontstyle="italic")

fig.savefig(OUT, dpi=185, facecolor=PAGE, bbox_inches=None)
print("wrote", OUT)
