"""AlphaStream architecture, represented as a refinement spine (light theme).

An alternate to the component diagram (project_diagram.py): instead of every
box and wire, it shows the system as layers that each refine the one before,
with the DATA CONTRACT labelled on every hand-off, plus the two structural
moves that make it more than a straight line -- a walk-forward feedback loop
and a decoupled qualitative sidecar.

Styled to read like the original diagram (light ground, familiar layer
colours, dark text) while keeping the layered framing.

matplotlib. Output: tools/alphastream_architecture_layers.(png|svg). Does not
touch project_diagram.py / alphastream_system_architecture.png.
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

BG    = "#ffffff"
INK   = "#1b2130"
MUT   = "#5b6472"
FAINT = "#98a1b0"
EDGE  = "#d6dbe3"
LINE  = "#8a93a3"

# (light fill, strong stroke/title) per layer -- the original's palette
INFRA    = ("#eceff1", "#607d8b")
DATA     = ("#e3f2fd", "#1565c0")
ANALYSIS = ("#e8f5e9", "#2e7d32")
SCORE    = ("#f3e5f5", "#8e24aa")
SERVE    = ("#fce4ec", "#c2185b")
PRESENT  = ("#e0f2f1", "#00897b")
VALID    = ("#fff3e0", "#ef6c00")
QUAL     = ("#ede7f6", "#5e35b1")

HERE = os.path.dirname(os.path.abspath(__file__))


def block(ax, cx, cy, w, h, tone, name, comps, repo, big=True):
    fill, stroke = tone
    ax.add_patch(FancyBboxPatch((cx - w / 2, cy - h / 2), w, h,
        boxstyle="round,pad=0.02,rounding_size=0.14",
        facecolor=fill, edgecolor=stroke, linewidth=2, zorder=3))
    ax.add_patch(FancyBboxPatch((cx - w / 2, cy + h / 2 - 0.06), w, 0.06,
        boxstyle="round,pad=0,rounding_size=0.02",
        facecolor=stroke, edgecolor="none", zorder=4))
    ax.text(cx, cy + h / 2 - 0.34, name, ha="center", va="center",
            color=stroke, fontsize=13.5 if big else 11.5, fontweight="bold", zorder=5)
    ax.text(cx, cy - 0.02, comps, ha="center", va="center", color=INK,
            fontsize=9.5, family="monospace", zorder=5, linespacing=1.4)
    if repo:
        ax.text(cx, cy - h / 2 + 0.24, repo, ha="center", va="center",
                color=FAINT, fontsize=8, family="monospace", zorder=5)


def arrow(ax, p0, p1, color=LINE, rad=0.0, lw=1.9, ls="-"):
    ax.add_patch(FancyArrowPatch(p0, p1, arrowstyle="-|>", mutation_scale=14,
        lw=lw, color=color, linestyle=ls, shrinkA=3, shrinkB=3,
        connectionstyle=f"arc3,rad={rad}", zorder=2))


def main():
    fig, ax = plt.subplots(figsize=(16.5, 8.6), dpi=150)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 9.4)
    ax.axis("off")

    ax.text(0.5, 8.95, "AlphaStream", fontsize=23, fontweight="bold", color=INK, va="top")
    ax.text(0.55, 8.26, "one architecture, read as layers: each refines the one before, "
            "gated by a loop and graded on the side",
            fontsize=11.5, color=MUT, va="top")
    ax.text(19.5, 8.7, "cool → warm  =  raw → decision", fontsize=9,
            color=FAINT, va="top", ha="right", family="monospace")

    # ── spine ──
    sy = 5.2
    bw, bh = 2.1, 1.55
    cx = [1.6, 5.0, 8.4, 11.8, 15.2, 18.6]
    specs = [
        (INFRA,    "Sources",      "Massive · Yahoo\nPolygon", "external"),
        (DATA,     "Canonical",    "raw · cdm",                "public repo"),
        (ANALYSIS, "Analysis",     "features\nclustering",     "private repo"),
        (SCORE,    "Scoring",      "scoring",                  "private repo"),
        (SERVE,    "Serving",      "serving\nmonitoring",      "private repo"),
        (PRESENT,  "Presentation", "Shiny →\nAnalysts",        "app"),
    ]
    for x, (tone, name, comps, repo) in zip(cx, specs):
        block(ax, x, sy, bw, bh, tone, name, comps, repo)

    contracts = ["raw feeds", "one clean series\nper item",
                 "feature matrix\n+ peer groups", "forecast\n+ rank",
                 "served rollup\n+ grades"]
    for i, c in enumerate(contracts):
        x0, x1 = cx[i] + bw / 2, cx[i + 1] - bw / 2
        arrow(ax, (x0, sy), (x1, sy))
        ax.text((x0 + x1) / 2, sy + 0.78, c, ha="center", va="center",
                color=MUT, fontsize=8.5, family="monospace", linespacing=1.35,
                bbox=dict(boxstyle="round,pad=0.2", facecolor=BG, edgecolor="none"))

    # ── orchestration: a single cross-cutting line under the spine ──
    ox0, ox1 = cx[1] - bw / 2, cx[4] + bw / 2
    oy = 3.75
    ax.plot([ox0, ox1], [oy, oy], color="#b6bdc8", lw=1.3, ls=(0, (5, 3)), zorder=1)
    for x in (cx[1], cx[2], cx[3], cx[4]):
        arrow(ax, (x, oy), (x, sy - bh / 2), color="#c4cbd4", lw=1.1, ls=(0, (2, 3)))
    ax.text((ox0 + ox1) / 2, oy,
            "  ORCHESTRATION · Airflow + dbt · schedules ingestion and every transform  ",
            ha="center", va="center", color=MUT, fontsize=8.8, family="monospace",
            bbox=dict(boxstyle="round,pad=0.35", facecolor=BG, edgecolor="none"), zorder=2)

    # ── feedback loop: Scoring -> Validation -> (credibility) -> Serving ──
    vstroke = VALID[1]
    vy = 1.3
    block(ax, 13.5, vy, 2.5, 1.1, VALID, "Validation",
          "walk-forward\nbacktest", "", big=False)
    arrow(ax, (cx[3], sy - bh / 2), (12.6, vy + 0.5), color=vstroke, rad=-0.16, lw=1.9)
    ax.text(11.85, 2.45, "backtest", color=vstroke, fontsize=8.3, family="monospace", ha="center")
    arrow(ax, (14.7, vy + 0.35), (cx[4], sy - bh / 2), color=vstroke, rad=-0.28, lw=1.9)
    ax.text(16.5, 2.5, "credibility gate:\nonly proven ranks serve", color=vstroke,
            fontsize=8.3, family="monospace", ha="center", linespacing=1.3)

    # ── qual sidecar: Claude -> qual -> (joins) Serving ──
    qstroke = QUAL[1]
    qy = 7.5
    block(ax, 11.8, qy, 2.3, 1.2, QUAL, "qualstream", "qual\nscorecards", "decoupled", big=False)
    block(ax, 8.4, qy, 2.1, 1.0, INFRA, "Claude API", "LLM grade", "", big=False)
    arrow(ax, (8.4 + 2.1 / 2, qy), (11.8 - 2.3 / 2, qy), color=qstroke, lw=1.8)
    arrow(ax, (11.8, qy - 0.6), (cx[4], sy + bh / 2), color=qstroke, rad=0.12, lw=1.9)
    ax.text(13.6, qy - 1.05, "qual grades\njoin at the edge", color=qstroke, fontsize=8.3,
            family="monospace", ha="left", linespacing=1.3)

    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    base = os.path.join(HERE, "alphastream_architecture_layers")
    fig.savefig(base + ".png", facecolor=BG, bbox_inches="tight", pad_inches=0.22)
    fig.savefig(base + ".svg", facecolor=BG, bbox_inches="tight", pad_inches=0.22)
    print(f"wrote {base}.png and {base}.svg")


if __name__ == "__main__":
    main()
