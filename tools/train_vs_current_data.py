#!/usr/bin/env python
"""AlphaStream data-window timeline: which data is used when (LIVE vs TRAIN/BACKTEST vs WALK-FORWARD)."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle, Polygon
from matplotlib.lines import Line2D
import numpy as np

import os as _os
OUT = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "train_vs_current_data.png")

# ---- Palette (restrained, coordinated) -------------------------------------
INK      = "#1f2933"   # near-black text
SUBINK   = "#52606d"   # secondary text
PAGE     = "#ffffff"
SPINE    = "#9aa5b1"

C_FULL   = "#cbd2d9"   # neutral full-history bar
C_LIVE   = "#3b6ea5"   # blue  -> live
C_LIVE_L = "#d6e3f0"   # light blue shade
C_TRAIN  = "#2f8f6b"   # green -> train block
C_HELD   = "#bfe3d4"   # light green held-out
C_WF     = "#b5772e"   # amber -> walk-forward
C_WF_L   = "#f1e2c8"
C_CALL   = "#c0392b"   # red marker for "today's call"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "text.color": INK,
    "axes.edgecolor": SPINE,
})

# ---- Time axis -------------------------------------------------------------
T0, T1 = 2004.0, 2026.5          # data coordinate space along x
def x(year):                      # passthrough; kept for clarity
    return year

fig, ax = plt.subplots(figsize=(16, 9), dpi=180)
fig.patch.set_facecolor(PAGE)
ax.set_facecolor(PAGE)
# Reserve a left gutter for row labels and a right gutter for the call/trust annotations
GUTTER_L = 6.8
GUTTER_R = 5.8
ax.set_xlim(T0 - GUTTER_L, T1 + GUTTER_R)
ax.set_ylim(0, 10)
ax.axis("off")

BAR_H = 0.92
LEFT  = T0
RIGHT = T1
LABX   = T0 - 0.5                 # right edge for row labels; they grow LEFT into the gutter
TITLEX = T0 - GUTTER_L + 0.2      # far-left anchor for title / footnotes

def rbar(x0, x1, yc, h, fc, ec=None, hatch=None, alpha=1.0, lw=1.1, z=2):
    p = FancyBboxPatch((x0, yc - h/2), x1 - x0, h,
                       boxstyle="round,pad=0,rounding_size=0.12",
                       linewidth=lw, facecolor=fc,
                       edgecolor=ec if ec else fc,
                       alpha=alpha, mutation_aspect=0.18, zorder=z)
    if hatch:
        p.set_hatch(hatch)
    ax.add_patch(p)
    return p

# row y-centers
Y_FULL = 8.55
Y_LIVE = 6.55
Y_BACK = 4.55
Y_WF   = 2.55

# ---- Title -----------------------------------------------------------------
ax.text(TITLEX, 9.72, "AlphaStream  ·  which data is used when",
        fontsize=22, fontweight="bold", color=INK, ha="left", va="center")
ax.text(TITLEX, 9.22,
        "One shared feature matrix (return_cluster_feature_set), sliced three ways along the time axis",
        fontsize=12.5, color=SUBINK, ha="left", va="center")

# ---- Shared time spine -----------------------------------------------------
spine_y = 1.05
ax.add_line(Line2D([T0 - 0.2, T1 + 0.45], [spine_y, spine_y],
                   color=SPINE, lw=1.4, zorder=1))
# arrowhead
ax.add_patch(Polygon([[T1 + 0.45, spine_y], [T1 + 0.2, spine_y + 0.12],
                      [T1 + 0.2, spine_y - 0.12]], closed=True,
                     facecolor=SPINE, edgecolor=SPINE, zorder=1))
ticks = [2004, 2008, 2012, 2016, 2020, 2024]
for t in ticks:
    ax.add_line(Line2D([t, t], [spine_y - 0.10, spine_y + 0.10],
                       color=SPINE, lw=1.2, zorder=1))
    ax.text(t, spine_y - 0.34, str(t), fontsize=10.5, color=SUBINK,
            ha="center", va="top")
ax.text(T1 + 0.42, spine_y - 0.34, "today", fontsize=10.5, color=INK,
        ha="right", va="top", fontstyle="italic")
ax.text(LABX, spine_y, "TIME", fontsize=10, color=SUBINK,
        ha="right", va="center", fontweight="bold")

# faint vertical guide lines from spine up through rows
for t in ticks:
    ax.add_line(Line2D([t, t], [spine_y + 0.18, 9.0],
                       color="#eceff1", lw=0.9, zorder=0))

# ---- Row label helper ------------------------------------------------------
def row_label(yc, tag, color, sub):
    ax.text(LABX, yc + 0.26, tag, fontsize=12, fontweight="bold",
            color=color, ha="right", va="center")
    ax.text(LABX, yc - 0.20, sub, fontsize=8.5, color=SUBINK,
            ha="right", va="center")

# =====================  FULL HISTORY  =======================================
rbar(LEFT, RIGHT, Y_FULL, BAR_H, C_FULL, ec="#aeb6bf")
ax.text((LEFT + RIGHT) / 2, Y_FULL + 0.16,
        "return_cluster_feature_set  —  ALL history",
        fontsize=13, fontweight="bold", color=INK, ha="center", va="center")
ax.text((LEFT + RIGHT) / 2, Y_FULL - 0.26,
        "one row per (ticker, date, lag) · past return paired with realized future return",
        fontsize=9, color=SUBINK, ha="center", va="center")
# row label in left gutter
ax.text(LABX, Y_FULL + 0.02, "SHARED", fontsize=12, fontweight="bold",
        color="#7b8794", ha="right", va="center")

# =====================  LIVE / CURRENT  =====================================
row_label(Y_LIVE, "LIVE / CURRENT", C_LIVE, "daily · dashboard")
# light shade across the whole bar = odds from all history
rbar(LEFT, RIGHT, Y_LIVE, BAR_H, C_LIVE_L, ec="#a9c4dd")
ax.text(LEFT + (RIGHT - LEFT) * 0.42, Y_LIVE,
        "odds built from ALL history  →  cell_score",
        fontsize=11.5, color="#2c5680", ha="center", va="center",
        fontweight="bold")
# solid slab at the right edge = the single latest bar
rbar(RIGHT - 0.18, RIGHT, Y_LIVE, BAR_H, C_LIVE, ec=C_LIVE, z=4)
# bold down-triangle marker ABOVE the slab, inside the LIVE row's own headroom
tcy = Y_LIVE + BAR_H/2 + 0.22
ax.add_patch(Polygon([[RIGHT - 0.09, tcy],
                      [RIGHT - 0.09 - 0.24, tcy + 0.46],
                      [RIGHT - 0.09 + 0.24, tcy + 0.46]],
                     closed=True, facecolor=C_CALL, edgecolor="white",
                     lw=1.0, zorder=6))
# call-out labels in the RIGHT gutter
ax.text(RIGHT + 0.30, Y_LIVE + 0.18, "today's bar = the call",
        fontsize=10.5, fontweight="bold", color=C_CALL, ha="left", va="center")
ax.text(RIGHT + 0.30, Y_LIVE - 0.20, "BUY / SELL / SKIP\nper ticker",
        fontsize=9, color=SUBINK, ha="left", va="center", linespacing=1.2)

# =====================  BACKTEST / TRAIN  ===================================
row_label(Y_BACK, "BACKTEST", C_TRAIN,
          "daily · validation only")
CUT = 2018.5
rbar(LEFT, CUT, Y_BACK, BAR_H, C_TRAIN, ec=C_TRAIN)
rbar(CUT, RIGHT, Y_BACK, BAR_H, C_HELD, ec="#8fc7b0", hatch="////")
# cutoff line
ax.add_line(Line2D([CUT, CUT], [Y_BACK - BAR_H/2 - 0.18, Y_BACK + BAR_H/2 + 0.5],
                   color=INK, lw=1.8, zorder=6))
ax.text(CUT, Y_BACK + BAR_H/2 + 0.66, "cutoff date", fontsize=9.5,
        fontweight="bold", color=INK, ha="center", va="center")
ax.text((LEFT + CUT) / 2, Y_BACK, "TRAIN\n(pre-cutoff)", fontsize=10.5,
        fontweight="bold", color="white", ha="center", va="center")
ax.text((CUT + RIGHT) / 2, Y_BACK, "HELD-OUT\n(post-cutoff)", fontsize=10,
        fontweight="bold", color="#23624a", ha="center", va="center")
ax.text(RIGHT, Y_BACK - BAR_H/2 - 0.32,
        "→ score sign-agreement + payoff",
        fontsize=9, color=SUBINK, ha="right", va="center", fontstyle="italic")

# =====================  WALK-FORWARD  =======================================
row_label(Y_WF, "WALK-FORWARD", C_WF, "monthly · rolling track record")
rbar(LEFT, RIGHT, Y_WF, BAR_H, C_WF_L, ec="#d9c293")
# stepped cutoffs marching left to right
cuts = np.linspace(2008.5, 2023.0, 6)
for i, c in enumerate(cuts):
    # train portion (left of cut) subtle solid, test slice just after
    test_w = 1.2
    rbar(c, min(c + test_w, RIGHT), Y_WF, BAR_H - 0.30, C_WF, ec=C_WF, z=3, alpha=0.85)
    ax.add_line(Line2D([c, c], [Y_WF - BAR_H/2, Y_WF + BAR_H/2 + 0.14],
                       color=C_WF, lw=1.6, zorder=4))
# train|test bracket above one representative cutoff for legibility
bc = cuts[1]
ax.add_line(Line2D([bc - 1.0, bc], [Y_WF + BAR_H/2 + 0.30, Y_WF + BAR_H/2 + 0.30],
                   color="#9c7b46", lw=1.1, zorder=4))
ax.add_line(Line2D([bc, bc + 1.2], [Y_WF + BAR_H/2 + 0.30, Y_WF + BAR_H/2 + 0.30],
                   color=C_WF, lw=2.2, zorder=4))
ax.text(bc - 0.5, Y_WF + BAR_H/2 + 0.52, "train", fontsize=8.5,
        color="#9c7b46", ha="center", va="center")
ax.text(bc + 0.6, Y_WF + BAR_H/2 + 0.52, "test", fontsize=8.5,
        color=C_WF, ha="center", va="center", fontweight="bold")
ax.text((LEFT + RIGHT) / 2, Y_WF - BAR_H/2 - 0.30,
        "42 rolling cutoffs · 2004-H1 → 2024-H2  (train past, test next slice, step forward)",
        fontsize=9, color=SUBINK, ha="center", va="center", fontstyle="italic")

# ---- Feedback loop: credibility -> live trust gate (subordinate) -----------
# a thin curved arrow from walk-forward/backtest area up to the live call,
# drawn lightly so it stays visually subordinate.
fb_x = RIGHT + 3.05   # run the loop up the right gutter, clear of the bars and callouts
ax.annotate(
    "",
    xy=(fb_x, Y_LIVE - BAR_H/2 - 0.10),     # up into the LIVE row
    xytext=(fb_x, Y_WF + 0.10),             # from walk-forward / backtest area
    arrowprops=dict(arrowstyle="-|>", color="#aab2bd", lw=1.5,
                    linestyle=(0, (5, 3)),
                    connectionstyle="arc3,rad=0.0"),
    zorder=2,
)
ax.text(fb_x + 0.12, (Y_LIVE + Y_WF) / 2,
        "cell_credibility\nTRUST GATE\nweights live calls\n(not a data feed)",
        fontsize=8.5, color="#6b7480", ha="left", va="center",
        linespacing=1.3)

# ---- Legend (compact) ------------------------------------------------------
handles = [
    Rectangle((0, 0), 1, 1, fc=C_LIVE_L, ec="#a9c4dd"),
    Rectangle((0, 0), 1, 1, fc=C_CALL),
    Rectangle((0, 0), 1, 1, fc=C_TRAIN),
    Rectangle((0, 0), 1, 1, fc=C_HELD, ec="#8fc7b0", hatch="////"),
    Rectangle((0, 0), 1, 1, fc=C_WF),
]
labels = [
    "odds from all history",
    "today's bar = the call",
    "TRAIN (pre-cutoff)",
    "HELD-OUT (post-cutoff)",
    "rolling test slice",
]
leg = ax.legend(handles, labels, loc="lower left",
                bbox_to_anchor=(0.0, -0.045), ncol=5, frameon=False,
                handlelength=1.3, handleheight=1.1, columnspacing=1.6,
                fontsize=9.5, borderaxespad=0)

# ---- Footnote: plain-English key takeaway ----------------------------------
fig.text(0.012, 0.045,
         "KEY TAKEAWAY   LIVE = all history to build the odds + today's single latest bar to make the call.   "
         "TRAIN / BACKTEST = pre-cutoff history vs held-out future, used only to score accuracy and to weight trust.",
         fontsize=10.5, color=INK, ha="left", va="center", fontweight="bold")
fig.text(0.012, 0.012,
         "Walk-forward: 61.2% sign agreement across 643,466 cells (June 2026).",
         fontsize=9, color=SUBINK, ha="left", va="center", fontstyle="italic")

plt.tight_layout(rect=(0.0, 0.07, 1.0, 1.0))
fig.savefig(OUT, dpi=180, facecolor=PAGE, bbox_inches="tight", pad_inches=0.25)
print("wrote", OUT)
