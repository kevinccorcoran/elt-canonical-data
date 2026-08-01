"""Conceptual pipeline schematic: data streams -> quality gates -> live.

A faithful digital reproduction of the hand sketch (distinct from
pipeline_grouped.py, the concrete schema-by-schema pipeline):

    Start -> three parallel DATA STREAMS -> a STAGE-1 box of per-stream
    quality-gate funnels -> a STAGE-2 box where the streams merge -> two
    planned live gates that cross into a LIVE finish line at two endpoints.

The funnels are drawn exactly as sketched: NO gradient. Each is a wide-left ->
pointed-right silhouette split into three discrete colour BANDS -- a green
intake block, an orange narrowing middle, and a small red tip -- and each band's
pen style encodes build progress:

    solid fill      done
    diagonal hatch  in progress
    dotted outline  planned

The bottom-left circle cluster is the team-role key (one circle per role, one
split into two colours). Role labels are intentionally left blank for now.

Drawn with matplotlib. Output: tools/pipeline_schematic.(png|svg).
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import (Circle, Ellipse, FancyArrowPatch, Polygon,
                                Rectangle, Wedge)

# ── palette ──
INK    = "#1b2130"
MUTED  = "#8a93a3"
ACCENT = "#2f9e6b"           # "live"
GREEN  = "#2f9e6b"
AMBER  = "#e2871b"
RED    = "#d6402a"
BLACK  = "#1b2130"
BOX1   = INK
BOX2   = "#c0553a"           # stage-2 box drawn red in the sketch
PAPER  = "#ffffff"

HERE = os.path.dirname(os.path.abspath(__file__))

# per-funnel section styles (green, orange, red) as read from the sketch
FA = [(GREEN, "solid"),  (AMBER, "solid"),  (RED, "solid")]    # box1 top    (done)
FB = [(GREEN, "hatch"),  (AMBER, "solid"),  (RED, "solid")]    # box1 mid
FC = [(GREEN, "dotted"), (AMBER, "hatch"),  (RED, "solid")]    # box1 bottom
FD = [(GREEN, "dotted"), (AMBER, "hatch"),  (RED, "hatch")]    # box2 upper
FE = [(GREEN, "dotted"), (AMBER, "dotted"), (RED, "dotted")]   # box2 lower  (planned)
PG = [(RED,   "dotted")]                                        # small live gates


def _band(ax, verts, color, style):
    """One colour band of a funnel, drawn in its pen style (no gradient)."""
    if style == "solid":
        ax.add_patch(Polygon(verts, closed=True, facecolor=color,
                             edgecolor=INK, linewidth=1.1, zorder=3))
    elif style == "hatch":
        ax.add_patch(Polygon(verts, closed=True, facecolor="white",
                             edgecolor=color, linewidth=1.5, hatch="////",
                             zorder=3))
    elif style == "dotted":
        ax.add_patch(Polygon(verts, closed=True, facecolor="none",
                             edgecolor=color, linewidth=2,
                             linestyle=(0, (1, 1.8)), zorder=3))


def funnel(ax, x_left, y_c, w, h, spec):
    """Banded quality-gate funnel: wide intake (left) -> refined tip (right).

    spec is a list of (colour, style): 3 entries -> green/orange/red bands;
    1 entry -> a plain single-colour triangle (the small live gates).
    """
    hh = h / 2
    xr = x_left + w
    if len(spec) == 1:
        _band(ax, [(x_left, y_c - hh), (x_left, y_c + hh), (xr, y_c)],
              spec[0][0], spec[0][1])
        return
    xg = x_left + 0.42 * w         # green rectangle ends
    xo = x_left + 0.80 * w         # orange ends, red tip begins
    ho = hh * (xr - xo) / (xr - xg)
    green  = [(x_left, y_c - hh), (x_left, y_c + hh), (xg, y_c + hh), (xg, y_c - hh)]
    orange = [(xg, y_c - hh), (xg, y_c + hh), (xo, y_c + ho), (xo, y_c - ho)]
    red    = [(xo, y_c - ho), (xo, y_c + ho), (xr, y_c)]
    for verts, (color, style) in zip((green, orange, red), spec):
        _band(ax, verts, color, style)


def arrow(ax, p0, p1, rad=0.0):
    ax.add_patch(FancyArrowPatch(
        p0, p1, arrowstyle="-|>", mutation_scale=13, lw=1.6, color=MUTED,
        shrinkA=2, shrinkB=3, connectionstyle=f"arc3,rad={rad}", zorder=1))


def build(ax):
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 9)
    ax.axis("off")

    yT, yM, yB = 6.35, 4.5, 2.65          # three stream lanes

    # ── Start ──
    ax.add_patch(Ellipse((1.15, yM), 1.5, 1.0, facecolor=PAPER,
                         edgecolor=INK, linewidth=2, zorder=4))
    ax.text(1.15, yM, "Start", ha="center", va="center",
            fontsize=12, fontweight="bold", color=INK, zorder=5)

    # ── Data Streams axis ──
    ax.plot([2.75, 2.75], [1.5, 7.6], color=MUTED, lw=1.3,
            ls=(0, (1, 4)), zorder=1)
    ax.annotate("", xy=(2.75, 7.05), xytext=(2.75, 7.5),
                arrowprops=dict(arrowstyle="-|>", color=MUTED, lw=1.4))
    ax.text(2.75, 7.9, "DATA STREAMS", ha="center", va="bottom",
            fontsize=11, color=INK, family="monospace")
    for y in (yT, yM, yB):
        ax.add_patch(Circle((2.75, y), 0.11, color=INK, zorder=4))

    # ── Stage boxes ──
    ax.add_patch(Rectangle((4.15, 1.15), 3.15, 6.7, facecolor="none",
                          edgecolor=BOX1, linewidth=2, zorder=1))
    ax.add_patch(Rectangle((8.35, 1.7), 2.5, 5.6, facecolor="none",
                          edgecolor=BOX2, linewidth=2, zorder=1))

    # ── Stage 1 funnels ──
    fw, fh, x1 = 2.1, 1.35, 4.55
    funnel(ax, x1, yT, fw, fh, FA)
    funnel(ax, x1, yM, fw, fh, FB)
    funnel(ax, x1, yB, fw, fh, FC)

    # ── Stage 2 funnels (merged upper + lower) ──
    yU, yL = 5.55, 3.15
    fw2, x2 = 1.85, 8.6
    funnel(ax, x2, yU, fw2, 1.3, FD)
    funnel(ax, x2, yL, fw2, 1.3, FE)

    # ── Pre-finish live gates ──
    x3, pw = 11.55, 1.15
    funnel(ax, x3, yT, pw, 0.85, PG)
    funnel(ax, x3, yB, pw, 0.85, PG)

    # ── Finish · Live ──
    ax.plot([14.1, 14.1], [1.2, 7.8], color=ACCENT, lw=3, zorder=2)
    for y in (yT, yB):
        ax.add_patch(Circle((14.1, y), 0.11, color=ACCENT, zorder=4))
    ax.text(14.55, yM, "Finish · Live", rotation=90, ha="left",
            va="center", fontsize=12, color=ACCENT, family="monospace",
            fontweight="bold")

    # ── flow ──
    arrow(ax, (1.9, yM), (2.64, yT), rad=-0.06)
    arrow(ax, (1.9, yM), (2.64, yM))
    arrow(ax, (1.9, yM), (2.64, yB), rad=0.06)
    for y in (yT, yM, yB):
        arrow(ax, (2.86, y), (x1 - 0.03, y))
    # stage 1 tips -> stage 2 (top+mid merge into upper; bottom -> lower)
    arrow(ax, (x1 + fw, yT), (x2 - 0.03, yU), rad=-0.05)
    arrow(ax, (x1 + fw, yM), (x2 - 0.03, yU), rad=0.05)
    arrow(ax, (x1 + fw, yB), (x2 - 0.03, yL), rad=0.05)
    # stage 2 tips cross into two live endpoints (the X in the sketch)
    arrow(ax, (x2 + fw2, yU), (x3 - 0.03, yB))
    arrow(ax, (x2 + fw2, yL), (x3 - 0.03, yT))
    # live gates -> finish
    arrow(ax, (x3 + pw, yT), (14.05, yT))
    arrow(ax, (x3 + pw, yB), (14.05, yB))

    _role_circles(ax)
    _style_key(ax)


def _role_circles(ax):
    """Team-role key from the sketch: one circle per role, one split two-colour.
    Colours only for now; role labels intentionally blank."""
    r = 0.17
    ax.text(2.55, 1.75, "TEAM", fontsize=9.5, color=MUTED, family="monospace")
    solids = [(2.55, 1.35, GREEN), (3.55, 1.35, RED),
              (2.55, 0.75, BLACK), (3.05, 0.75, AMBER), (3.55, 0.75, RED)]
    for x, y, c in solids:
        ax.add_patch(Circle((x, y), r, facecolor=c, edgecolor=INK,
                           linewidth=0.8, zorder=4))
    # the one split (two-colour) role circle
    sx, sy = 3.05, 1.35
    ax.add_patch(Wedge((sx, sy), r, 90, 270, facecolor=GREEN,
                      edgecolor=INK, linewidth=0.8, zorder=4))
    ax.add_patch(Wedge((sx, sy), r, 270, 450, facecolor=AMBER,
                      edgecolor=INK, linewidth=0.8, zorder=4))


def _style_key(ax):
    """Small key for the pen styles (done / in progress / planned)."""
    x0, y0 = 5.4, 0.95
    samples = [("solid", "done"), ("hatch", "in progress"), ("dotted", "planned")]
    for i, (style, label) in enumerate(samples):
        y = y0 - i * 0.42
        rect = [(x0, y - 0.14), (x0 + 0.5, y - 0.14),
                (x0 + 0.5, y + 0.14), (x0, y + 0.14)]
        _band(ax, rect, INK if style == "solid" else MUTED, style)
        ax.text(x0 + 0.68, y, label, va="center", fontsize=9, color=INK)


def main():
    fig, ax = plt.subplots(figsize=(15.5, 7.0), dpi=150)
    fig.patch.set_facecolor(PAPER)
    build(ax)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    base = os.path.join(HERE, "pipeline_schematic")
    fig.savefig(base + ".png", facecolor=PAPER, bbox_inches="tight", pad_inches=0.15)
    fig.savefig(base + ".svg", facecolor=PAPER, bbox_inches="tight", pad_inches=0.15)
    print(f"wrote {base}.png and {base}.svg")


if __name__ == "__main__":
    main()
