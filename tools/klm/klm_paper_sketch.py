import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch
import os

# ---------------------------------------------------------------------
# Paper-sketch version WITH the full detail, drawn as Kevin would with
# a 4-colour pen: BLACK structure, RED drill path, GREEN own-data/HIGH,
# BLUE external/labels. xkcd style = hand-drawn look.
# ---------------------------------------------------------------------

BLACK = "#1F2933"
RED = "#C0392B"
GREEN = "#1E7D45"
BLUE = "#1F5C99"
ORANGE = "#C87612"
PURPLE = "#7B3FA0"
GREY = "#6B7280"

with plt.xkcd(scale=1.05, length=110, randomness=2):
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 140)
    ax.set_ylim(0, 100)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    def box(x, y, w, h, label, note="", edge=BLACK, lw=1.6):
        ax.add_patch(plt.Rectangle((x, y), w, h, facecolor="white",
                     edgecolor=edge, linewidth=lw, zorder=2))
        ax.text(x + 2, y + h / 2, label, fontsize=12, va="center",
                ha="left", color=BLACK, zorder=3)
        if note:
            ax.text(x + w - 2, y + h / 2, note, fontsize=9, va="center",
                    ha="right", color=BLUE, zorder=3)

    # ---- title ----
    ax.text(70, 97, "MEASURE — drill to find WHERE, slice to see WHAT + WHEN",
            fontsize=14, ha="center", color=BLACK)

    # ---- left: drill ladder ----
    ax.text(6, 90.5, "DRILL (nested)", fontsize=11, color=RED)
    bx, bw, bh = 6, 50, 7.5
    ys = [81, 69, 57, 45]
    ladder = [("NETWORK", "all flights"),
              ("MARKET", "AMS-JFK"),
              ("FLIGHT", "KL641"),
              ("DEPARTURE", "12 Mar")]
    for y, (lab, note) in zip(ys, ladder):
        box(bx, y, bw, bh, lab, note)
    for y1, y2 in zip(ys[:-1], ys[1:]):
        ax.add_patch(FancyArrowPatch((bx + bw / 2, y1 - 0.4),
                     (bx + bw / 2, y2 + bh + 0.4),
                     arrowstyle="-|>", mutation_scale=15,
                     color=RED, linewidth=1.8, zorder=1))
    ax.text(bx + bw + 2, 63, "follow\nbiggest\nerror", fontsize=9.5,
            color=RED, va="center")

    # then slice
    ax.add_patch(FancyArrowPatch((bx + bw / 2, 44.5), (bx + bw / 2, 37),
                 arrowstyle="-|>", mutation_scale=15, color=RED,
                 linewidth=1.8, linestyle="--", zorder=1))
    ax.text(bx + bw / 2 + 2, 40.5, "then SLICE", fontsize=10, color=RED)

    # slice band
    sy, sh = 28, 8.5
    ax.add_patch(plt.Rectangle((bx, sy), bw, sh, facecolor="white",
                 edgecolor=GREEN, linewidth=1.8, zorder=2))
    ax.plot([bx + bw / 2, bx + bw / 2], [sy, sy + sh], color=GREEN,
            linewidth=1.5, zorder=3)
    ax.text(bx + bw / 4, sy + sh / 2, "FARE CLASS\ncheap vs prem",
            fontsize=9, ha="center", va="center", color=BLACK, zorder=3)
    ax.text(bx + 3 * bw / 4, sy + sh / 2, "TIME\nbook curve",
            fontsize=9, ha="center", va="center", color=BLACK, zorder=3)
    ax.text(bx, sy - 3, "cuts across ANY level", fontsize=9, color=GREEN)

    # data-first warning
    ax.text(bx, 14, "!! DATA first: cancels/dupes\nlook like demand",
            fontsize=10.5, ha="left", va="center", color=RED,
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white",
                      edgecolor=RED, linewidth=1.8))

    # ---- right: causes table with headers ----
    tx = 72
    ax.text(tx, 92, "WHY IS THE FORECAST OFF?", fontsize=12, color=BLACK)
    # column headers
    C1, C2, C3 = tx, tx + 15, tx + 42
    C4 = tx + 64
    hy = 87
    ax.text(C1, hy, "level", fontsize=9, color=GREY, style="italic")
    ax.text(C2, hy, "what happened", fontsize=9, color=GREY, style="italic")
    ax.text(C3, hy, "data that shows it", fontsize=9, color=GREY, style="italic")
    ax.text(C4, hy, "mine?", fontsize=9, color=GREY, style="italic", ha="center")
    ax.plot([tx - 1, tx + 68], [hy - 1.8, hy - 1.8], color=GREY, linewidth=1.1)

    rows = [
        ("NETWORK", "economy shifts,\nholiday moves", "external\nmacro feeds", PURPLE, "LOW", PURPLE),
        ("MARKET", "competitor adds\nseats / cuts price", "competitor feed\n(schedules+fares)", PURPLE, "LOW", PURPLE),
        ("FLIGHT", "bigger plane,\nnew departure time", "our schedule\nsystem", GREEN, "MED", GREEN),
        ("DEPARTURE", "one-off: storm,\nstrike, event", "ops system\n+ cancellations", PURPLE, "MED", PURPLE),
        ("FARE CLASS", "business demand\ndrops, leisure holds", "our bookings\nby class", GREEN, "HIGH", GREEN),
        ("TIME", "people book later\nthan they used to", "our booking\ncurve history", GREEN, "HIGH", GREEN),
        ("DATA", "cancels + duplicates\ncounted as real", "our pipeline\nQA checks", GREEN, "HIGH", GREEN),
    ]
    ry = 82
    for lab, cause, src, scol, tag, tcol in rows:
        ax.text(C1, ry, lab, fontsize=9, color=BLACK, fontweight="bold", va="top")
        ax.text(C2, ry, cause, fontsize=8.6, color=BLACK, va="top", linespacing=1.15)
        ax.text(C3, ry, src, fontsize=8.6, color=scol, va="top", linespacing=1.15)
        ax.text(C4, ry - 1.5, tag, fontsize=11, color=tcol, ha="center",
                fontweight="bold")
        ry -= 9.6

    # legend
    ax.text(tx, 12, "word = how much of it is my work\ncolor: green = our data, purple = external feed\nred = the path I follow",
            fontsize=9.5, color=BLACK, va="center",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white",
                      edgecolor=BLACK, linewidth=1.4))

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_paper_sketch.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"wrote {out}")
