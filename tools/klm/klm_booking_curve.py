import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import os

# ---------------------------------------------------------------------
# The booking curve: cumulative % of seats sold vs days before departure.
# Two lines: normal curve (history) and this year (booking later).
# The point: a snapshot below the curve is ambiguous — less demand, or
# just later booking? The last-2-weeks business surge decides it.
# ---------------------------------------------------------------------

BLUE = "#2563EB"    # history
AMBER = "#C2700F"   # this year
INK = "#1F2933"
MUT = "#616E7C"
GRID = "#E4E7EB"
SURF = "#fcfcfb"

days = np.linspace(120, 0, 400)          # days before departure
t = 1 - days / 120                        # 0 → 1 as departure approaches

# history: leisure builds early/steadily, business kick in last 14 days
hist = 78 * (t ** 1.35)
hist += 22 * np.exp(-(days / 7.0)) / np.exp(0)        # late business surge
hist = np.clip(hist, 0, 100)

# this year: same final demand, but everything books LATER
t2 = np.clip((t - 0.12) / 0.88, 0, 1)
this = 74 * (t2 ** 1.6)
this += 26 * np.exp(-(days / 6.0))
this = np.clip(this, 0, 100)

fig, ax = plt.subplots(figsize=(11.5, 6.8))
fig.patch.set_facecolor(SURF)
ax.set_facecolor(SURF)

# business window band
ax.axvspan(0, 14, color="#F3EDE2", zorder=0)
ax.text(7, 14, "last 2 weeks:\nbusiness\nbooks here", ha="center", va="center",
        fontsize=9.5, color=MUT)

l_hist, = ax.plot(days, hist, color=BLUE, linewidth=2, zorder=3)
l_this, = ax.plot(days, this, color=AMBER, linewidth=2, zorder=3)

# direct labels
ax.text(58, hist[np.argmin(np.abs(days - 55))] + 3, "normal curve (history)",
        color=BLUE, fontsize=11, fontweight="bold", ha="left")
ax.text(46, this[np.argmin(np.abs(days - 48))] - 7.5, "this flight, now",
        color=AMBER, fontsize=11, fontweight="bold", ha="left")

# the ambiguous gap at 30 days out
i30 = np.argmin(np.abs(days - 30))
y_h, y_t = hist[i30], this[i30]
ax.annotate("", xy=(30, y_t), xytext=(30, y_h),
            arrowprops=dict(arrowstyle="<->", color=INK, lw=1.6))
ax.text(78, 62, "30 days out: 12 pts behind.\nLess demand — or just later booking?\nOnly the curve shape tells you.",
        fontsize=10, color=INK, va="center", ha="left")

# axes
ax.set_xlim(120, 0)                      # time flows toward departure
ax.set_ylim(0, 104)
ax.set_xticks([120, 90, 60, 30, 14, 0])
ax.set_xticklabels(["120", "90", "60", "30", "14", "departure"])
ax.set_xlabel("days before departure", fontsize=11, color=MUT)
ax.set_ylabel("% of seats booked (cumulative)", fontsize=11, color=MUT)
ax.tick_params(colors=MUT, labelsize=10)
for s in ["top", "right"]:
    ax.spines[s].set_visible(False)
for s in ["left", "bottom"]:
    ax.spines[s].set_color(GRID)
ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)

ax.set_title("The booking curve — how a flight fills up",
             fontsize=15, fontweight="bold", color=INK, pad=14, loc="left")
ax.legend([l_hist, l_this], ["normal curve (history)", "this flight, now"],
          loc="upper left", frameon=False, fontsize=9.5,
          bbox_to_anchor=(0.0, 0.97))

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_booking_curve.png")
plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=SURF)
print(f"wrote {out}")
