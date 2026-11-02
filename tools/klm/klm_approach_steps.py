import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import os

# ---------------------------------------------------------------------
# "How I'd approach 'the forecast is off'" — minimal: 5 moves, one line
# + one example each.
# ---------------------------------------------------------------------

BLACK = "#1F2933"
RED = "#B23A48"
GREEN = "#2E7D57"
BLUE = "#2C5C8A"
GREY = "#52606D"

# (num, title, accent, example)
steps = [
    ("1", "DEFINE THE ERROR", BLUE,
     "one table: forecast vs actual, Δ per flight + class"),
    ("2", "CLEAN FIRST", RED,
     "dupes? cancels? → COUNT(*) vs COUNT(DISTINCT id)"),
    ("3", "DRILL TO THE WORST", BLUE,
     "network → market → flight, rank by error → KL641"),
    ("4", "SLICE THE CELL", GREEN,
     "by fare class + booking horizon → late business demand"),
    ("5", "FIX + BACKTEST", GREEN,
     "one change, replay history, keep the winner, ship"),
]

fig, ax = plt.subplots(figsize=(11, 9))
ax.set_xlim(0, 100)
ax.set_ylim(0, 100)
ax.axis("off")
fig.patch.set_facecolor("white")

ax.text(50, 96, "How I'd approach “the forecast is off”",
        fontsize=19, fontweight="bold", ha="center", color=BLACK)
ax.text(50, 91, "follow the biggest error down, then test the fix",
        fontsize=12.5, ha="center", color=GREY)

bx, bw, bh, gap = 12, 76, 11, 4.5
top = 84
for i, (num, title, accent, eg) in enumerate(steps):
    y = top - i * (bh + gap) - bh
    ax.add_patch(FancyBboxPatch((bx, y), bw, bh,
                 boxstyle="round,pad=0,rounding_size=1.0",
                 facecolor="white", edgecolor=accent, linewidth=2.0, zorder=2))
    # number chip
    ax.add_patch(plt.Circle((bx + 5.5, y + bh / 2), 3.2, facecolor=accent,
                 edgecolor="none", zorder=4))
    ax.text(bx + 5.5, y + bh / 2, num, color="white", fontsize=14,
            fontweight="bold", va="center", ha="center", zorder=5)
    ax.text(bx + 11, y + bh * 0.62, title, color=accent, fontsize=14,
            fontweight="bold", va="center", ha="left", zorder=4)
    ax.text(bx + 11, y + bh * 0.28, eg, color="#3E4C59", fontsize=10.5,
            va="center", ha="left", zorder=4, style="italic")
    if i < len(steps) - 1:
        ax.add_patch(FancyArrowPatch((bx + bw / 2, y - 0.4),
                     (bx + bw / 2, y - gap + 0.5),
                     arrowstyle="-|>", mutation_scale=18,
                     color=BLACK, linewidth=2.0, zorder=1))

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_approach_steps.png")
plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
print(f"wrote {out}")
