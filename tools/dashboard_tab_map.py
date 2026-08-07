"""3838 dashboard tab map: a ring of the dashboard's ten tabs.

Keeps the hand sketch's ring-of-nodes idea but treats it as a designed piece:
the tabs are arranged in workflow order and grouped into the four stages they
belong to (foundation -> analysis -> validation -> decisions), each stage its
own colour. Every node carries a small icon of what the tab does rather than a
bare number. Descriptions stay domain-agnostic on purpose.

Drawn with matplotlib. Output: tools/dashboard_tab_map.(png|svg).
"""
import math
import os
import textwrap

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Arc, Circle, FancyArrowPatch

matplotlib.rcParams["font.sans-serif"] = ["Helvetica Neue", "Helvetica",
                                          "Arial", "DejaVu Sans"]
matplotlib.rcParams["font.family"] = "sans-serif"

# ── palette: deep navy ground, one colour per workflow stage ──
BG      = "#0e1420"
INK     = "#e8eef6"
MUTED   = "#93a1b3"
FAINT   = "#5a6678"
RING    = "#283345"
FOUND   = "#38bdf8"   # foundation  · data health
ANALY   = "#a78bfa"   # analysis    · find & rank
VALID   = "#fbbf24"   # validation  · does it hold
DECIDE  = "#34d399"   # decisions   · act & track

HERE = os.path.dirname(os.path.abspath(__file__))

# icon-key, tab name, few-word agnostic description, stage colour.
# Order = the analytical workflow, clockwise from the top.
TABS = [
    ("magnifier", "Data QA",          "scans sources for data problems", FOUND),
    ("timeline",  "Coverage",         "how much history each item has",  FOUND),
    ("clusters",  "Clusters",         "groups lookalike items",          ANALY),
    ("range",     "Transition Range", "the likely range ahead",          ANALY),
    ("bars",      "Shortlist",        "top-ranked by reliability",       ANALY),
    ("levels",    "Rank Stability",   "do rankings hold over time",      VALID),
    ("check",     "Model Validation", "tested on unseen history",        VALID),
    ("target",    "Predictions",      "the current selections",          DECIDE),
    ("forecast",  "Forecast",         "past selections tracked to now",  DECIDE),
    ("loop",      "Lifecycle",        "enter, hold, or exit calls",      DECIDE),
]

STAGES = [("Foundation", FOUND), ("Analysis", ANALY),
          ("Validation", VALID), ("Decisions", DECIDE)]


# ── icons: simple knockout glyphs drawn in the ground colour ──
def _icon(ax, key, x, y):
    ko = BG
    z = 6
    def line(xs, ys, lw=2.3):
        ax.plot(xs, ys, color=ko, lw=lw, solid_capstyle="round",
                solid_joinstyle="round", zorder=z)
    def dot(cx, cy, r):
        ax.add_patch(Circle((cx, cy), r, facecolor=ko, edgecolor="none", zorder=z))
    def ring(cx, cy, r, lw=2.1):
        ax.add_patch(Circle((cx, cy), r, facecolor="none", edgecolor=ko, lw=lw, zorder=z))

    if key == "magnifier":
        ring(x - 0.02, y + 0.03, 0.12)
        line([x + 0.065, x + 0.15], [y - 0.055, y - 0.14], lw=2.6)
    elif key == "timeline":
        line([x - 0.16, x + 0.16], [y, y])
        ring(x - 0.16, y, 0.045, lw=2.0)
        dot(x + 0.13, y, 0.05)
    elif key == "clusters":
        for dx, dy in [(-0.10, 0.05), (0.01, 0.11), (0.10, 0.0),
                       (-0.01, -0.09), (0.11, 0.09)]:
            dot(x + dx, y + dy, 0.037)
    elif key == "range":
        line([x, x], [y - 0.15, y + 0.15])
        line([x - 0.06, x + 0.06], [y + 0.15, y + 0.15])
        line([x - 0.06, x + 0.06], [y - 0.15, y - 0.15])
        dot(x, y, 0.05)
    elif key == "bars":
        for ln, yo in [(0.17, 0.10), (0.115, 0.0), (0.06, -0.10)]:
            line([x - 0.13, x - 0.13 + ln], [y + yo, y + yo], lw=3.2)
    elif key == "levels":
        for dx in (-0.12, 0.0, 0.12):                 # equal-height bars = holds steady
            line([x + dx, x + dx], [y - 0.12, y + 0.10], lw=2.6)
        line([x - 0.16, x + 0.16], [y - 0.12, y - 0.12])
    elif key == "check":
        line([x - 0.14, x - 0.03, x + 0.16], [y - 0.01, y - 0.12, y + 0.14], lw=2.8)
    elif key == "target":
        ring(x, y, 0.15, lw=2.0)
        ring(x, y, 0.075, lw=2.0)
        dot(x, y, 0.028)
    elif key == "forecast":
        ax.add_patch(FancyArrowPatch((x - 0.15, y - 0.12), (x + 0.16, y + 0.14),
                     arrowstyle="-|>", mutation_scale=11, lw=2.4,
                     color=ko, shrinkA=0, shrinkB=0, zorder=z))
    elif key == "loop":
        ax.add_patch(Arc((x, y), 0.30, 0.30, angle=0, theta1=-50, theta2=210,
                         color=ko, lw=2.4, zorder=z))
        th = math.radians(-50)
        ex, ey = x + 0.15 * math.cos(th), y + 0.15 * math.sin(th)
        tx, ty = -math.sin(th), math.cos(th)          # clockwise tangent
        ax.add_patch(FancyArrowPatch((ex - 0.01 * tx, ey - 0.01 * ty),
                     (ex + 0.02 * tx, ey + 0.02 * ty), arrowstyle="-|>",
                     mutation_scale=10, lw=2.2, color=ko, shrinkA=0, shrinkB=0, zorder=z))


def build(ax):
    ax.set_xlim(-7.6, 7.6)
    ax.set_ylim(-6.4, 5.7)
    ax.set_aspect("equal")
    ax.axis("off")

    n = len(TABS)
    R = 3.35
    r_node = 0.44
    angles = [math.radians(90 - i * (360 / n)) for i in range(n)]
    pos = [(R * math.cos(a), R * math.sin(a)) for a in angles]

    # ring joining the nodes (under everything)
    for i in range(n):
        x0, y0 = pos[i]
        x1, y1 = pos[(i + 1) % n]
        ax.plot([x0, x1], [y0, y1], color=RING, lw=2.4, zorder=1,
                solid_capstyle="round")

    # nodes: soft glow + filled stage-colour disc + knockout icon
    for (key, name, desc, col), (x, y) in zip(TABS, pos):
        ax.add_patch(Circle((x, y), r_node + 0.13, facecolor=col, alpha=0.12,
                            edgecolor="none", zorder=2))
        ax.add_patch(Circle((x, y), r_node, facecolor=col, edgecolor=BG,
                            linewidth=1.5, zorder=3))
        _icon(ax, key, x, y)

    # labels: stage-colour name + muted agnostic description
    push = r_node + 0.72
    for a, (x, y), (key, name, desc, col) in zip(angles, pos, TABS):
        ca, sa = math.cos(a), math.sin(a)
        cx, cy = x + push * ca, y + push * sa
        ha = "left" if ca > 0.25 else "right" if ca < -0.25 else "center"
        ax.text(cx, cy + 0.24, name, ha=ha, va="bottom", color=col,
                fontsize=12, fontweight="bold", zorder=5)
        ax.text(cx, cy - 0.12, textwrap.fill(desc, 16), ha=ha, va="top",
                color=MUTED, fontsize=8.8, zorder=5, linespacing=1.35)

    # center hub
    ax.text(0, 0.52, "3838", ha="center", va="center", color=INK,
            fontsize=34, fontweight="bold", zorder=5)
    ax.text(0, -0.30, "dashboard map", ha="center", va="center", color=MUTED,
            fontsize=11, zorder=5)
    ax.text(0, -0.92, "ten views · one workflow", ha="center", va="center",
            color=FAINT, fontsize=8.5, style="italic", zorder=5)

    # stage legend along the bottom
    ax.plot([-4.9, 4.9], [-5.35, -5.35], color=RING, lw=1.1, zorder=1)
    xs = [-4.35, -1.55, 1.25, 4.05]
    for (label, col), lx in zip(STAGES, xs):
        ax.add_patch(Circle((lx, -5.95), 0.12, facecolor=col, edgecolor="none",
                            zorder=5))
        ax.text(lx + 0.26, -5.95, label, ha="left", va="center", color=INK,
                fontsize=10, zorder=5)


def main():
    fig, ax = plt.subplots(figsize=(12.8, 10.4), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    build(ax)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    base = os.path.join(HERE, "dashboard_tab_map")
    fig.savefig(base + ".png", facecolor=BG, bbox_inches="tight", pad_inches=0.25)
    fig.savefig(base + ".svg", facecolor=BG, bbox_inches="tight", pad_inches=0.25)
    print(f"wrote {base}.png and {base}.svg")


if __name__ == "__main__":
    main()
