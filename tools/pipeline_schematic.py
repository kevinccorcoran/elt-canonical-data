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
GREEN_L = "#e8f5ee"          # light fill for green deliverables
GREEN_D = "#1f6b4a"
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
    ax.set_ylim(-3.5, 9)
    ax.set_aspect("equal")                # never distort funnels/circles
    ax.axis("off")

    yT, yM, yB = 6.7, 4.7, 2.7            # three stream lanes (even 2.0 pitch)

    # ── Start ──
    ax.add_patch(Ellipse((1.25, yM), 1.6, 1.05, facecolor=PAPER,
                         edgecolor=INK, linewidth=2, zorder=4))
    ax.text(1.25, yM, "Start", ha="center", va="center",
            fontsize=12, fontweight="bold", color=INK, zorder=5)

    # ── Data Streams axis ──
    ax.plot([2.9, 2.9], [1.9, 7.85], color=MUTED, lw=1.3, ls=(0, (1, 4)), zorder=1)
    ax.annotate("", xy=(2.9, 7.35), xytext=(2.9, 7.8),
                arrowprops=dict(arrowstyle="-|>", color=MUTED, lw=1.4))
    ax.text(2.9, 8.15, "DATA STREAMS", ha="center", va="bottom",
            fontsize=11, color=INK, family="monospace")
    for y in (yT, yM, yB):
        ax.add_patch(Circle((2.9, y), 0.12, color=INK, zorder=4))

    # ── Stage 1: box + three per-stream funnels ──
    fw, fh, x1 = 2.05, 1.5, 4.35
    ax.add_patch(Rectangle((4.05, 1.6), 3.05, 6.2, facecolor="none",
                          edgecolor=BOX1, linewidth=2, zorder=1))
    funnel(ax, x1, yT, fw, fh, FA)
    funnel(ax, x1, yM, fw, fh, FB)
    funnel(ax, x1, yB, fw, fh, FC)

    # ── Stage 2: tighter box + two merged funnels (centred in the box) ──
    yU, yL = 5.7, 3.7
    fw2, fh2, x2 = 1.8, 1.4, 8.15
    ax.add_patch(Rectangle((7.85, 2.55), 2.35, 4.3, facecolor="none",
                          edgecolor=BOX2, linewidth=2, zorder=1))
    funnel(ax, x2, yU, fw2, fh2, FD)
    funnel(ax, x2, yL, fw2, fh2, FE)

    # ── Pre-finish live gates: small red, ~size of a funnel's red tip ──
    x3, pw, ph = 12.2, 0.5, 0.6
    yTg, yBg = 6.3, 3.1
    funnel(ax, x3, yTg, pw, ph, PG)
    funnel(ax, x3, yBg, pw, ph, PG)

    # ── Finish · Live ──
    xF = 13.7
    ax.plot([xF, xF], [2.2, 7.2], color=ACCENT, lw=3, zorder=2)
    for y in (yTg, yBg):
        ax.add_patch(Circle((xF, y), 0.12, color=ACCENT, zorder=4))
    ax.text(xF + 0.45, yM, "Finish · Live", rotation=90, ha="left",
            va="center", fontsize=12, color=ACCENT, family="monospace",
            fontweight="bold")

    # ── flow ──
    for y, r in ((yT, -0.06), (yM, 0.0), (yB, 0.06)):
        arrow(ax, (2.05, yM), (2.78, y), rad=r)
    for y in (yT, yM, yB):
        arrow(ax, (3.02, y), (x1 - 0.03, y))
    # stage 1 tips -> stage 2 (top+mid merge into upper; bottom -> lower)
    arrow(ax, (x1 + fw, yT), (x2 - 0.03, yU), rad=-0.05)
    arrow(ax, (x1 + fw, yM), (x2 - 0.03, yU), rad=0.05)
    arrow(ax, (x1 + fw, yB), (x2 - 0.03, yL), rad=0.05)
    # stage 2 tips cross into two live endpoints (the X in the sketch)
    arrow(ax, (x2 + fw2, yU), (x3 - 0.03, yBg))
    arrow(ax, (x2 + fw2, yL), (x3 - 0.03, yTg))
    # live gates -> finish
    arrow(ax, (x3 + pw, yTg), (xF - 0.05, yTg))
    arrow(ax, (x3 + pw, yBg), (xF - 0.05, yBg))

    _deliverables(ax)
    _team_roles(ax)
    _style_key(ax)


def _split_circle(ax, x, y, r, c1, c2):
    ax.add_patch(Wedge((x, y), r, 90, 270, facecolor=c1,
                      edgecolor=INK, linewidth=0.8, zorder=4))
    ax.add_patch(Wedge((x, y), r, 270, 450, facecolor=c2,
                      edgecolor=INK, linewidth=0.8, zorder=4))


def _deliverables(ax):
    """The testing & framework dev's two green deliverables, shown along the
    pipeline: the automated test suite (validates the models) and the quality
    dashboard (reads the live output)."""
    gy0, h = 0.5, 0.78
    def gbox(x0, x1, label):
        ax.add_patch(Rectangle((x0, gy0), x1 - x0, h, facecolor=GREEN_L,
                              edgecolor=GREEN, linewidth=1.8, zorder=2))
        ax.text((x0 + x1) / 2, gy0 + h / 2, label, ha="center", va="center",
                color=GREEN_D, fontsize=9, fontweight="bold", zorder=3)
    gbox(4.05, 10.2, "Automated test suite  ·  pytest / matrix-notify")
    gbox(11.55, 14.6, "Quality dashboard · Shiny")
    top = gy0 + h
    for x, ytop in ((5.4, 1.6), (9.0, 2.55)):          # suite validates the models
        ax.plot([x, x], [top, ytop], color=GREEN, ls=(0, (1, 2.2)), lw=1.3, zorder=1)
    ax.plot([13.7, 13.05], [2.2, top], color=GREEN, ls=(0, (1, 2.2)), lw=1.3, zorder=1)


def _team_roles(ax):
    """Team roster (7 circles, one split) + role key. Colour also encodes model
    state: red = delivered but imperfect, orange = done, green = tested."""
    ax.plot([0.4, 15.6], [0.2, 0.2], color="#e0e5eb", lw=1, zorder=0)   # divider
    r, dx = 0.18, 0.5
    ax.text(0.55, -0.25, "TEAM", fontsize=10, color=MUTED, family="monospace")
    top = [GREEN, "split", AMBER, AMBER]
    bot = [BLACK, RED, RED]
    xs_top = [0.72 + i * dx for i in range(4)]
    span = xs_top[-1] - xs_top[0]
    xs_bot = [(xs_top[0] + span / 2) + (i - 1) * dx for i in range(3)]
    for y, xs, row in ((-0.8, xs_top, top), (-1.35, xs_bot, bot)):
        for x, c in zip(xs, row):
            if c == "split":
                _split_circle(ax, x, y, r, GREEN, AMBER)
            else:
                ax.add_patch(Circle((x, y), r, facecolor=c, edgecolor=INK,
                                   linewidth=0.8, zorder=4))
    # role · meaning key
    ax.text(3.2, -0.25, "ROLE", fontsize=8.5, color=MUTED, family="monospace")
    ax.text(6.15, -0.25, "role · model state · dbt", fontsize=8.5,
            color=MUTED, family="monospace")
    rows = [
        (BLACK, "Tech lead",
         "owns dbt strategy, used efficiently per client · hands-off code · dbt: deep"),
        (RED, "Model dev lead",
         "initial model, imperfect but delivered · test dev · dbt: med–high (variables)"),
        (AMBER, "Dev · QA & optimization",
         "model done: query opt, splitting, index strategy · test dev · dbt: working"),
        (GREEN, "Dev · testing & framework",
         "proper testing at each model level (pytest/matrix-notify, Shiny) · dbt: none"),
    ]
    y = -0.85
    for c, role, means in rows:
        ax.add_patch(Circle((3.05, y), 0.12, facecolor=c, edgecolor=INK,
                           linewidth=0.8, zorder=4))
        ax.text(3.3, y, role, fontsize=8.8, color=INK, va="center", fontweight="bold")
        ax.text(6.15, y, means, fontsize=8.2, color=INK, va="center")
        y -= 0.6
    ax.text(3.3, y + 0.03,
            "Split circle = one person covering testing framework + QA/optimization.",
            fontsize=8, color=MUTED, va="center", style="italic")


def _style_key(ax):
    """Pen-style key (done / in progress / planned), top-right."""
    x0, y0 = 11.5, 8.45
    ax.text(x0, y0 + 0.42, "BUILD", fontsize=8.5, color=MUTED, family="monospace")
    samples = [("solid", "done"), ("hatch", "in progress"), ("dotted", "planned")]
    for i, (style, label) in enumerate(samples):
        y = y0 - i * 0.48
        rect = [(x0, y - 0.15), (x0 + 0.5, y - 0.15),
                (x0 + 0.5, y + 0.15), (x0, y + 0.15)]
        _band(ax, rect, INK if style == "solid" else MUTED, style)
        ax.text(x0 + 0.66, y, label, va="center", fontsize=9, color=INK)


def main():
    fig, ax = plt.subplots(figsize=(12.8, 10.0), dpi=150)
    fig.patch.set_facecolor(PAPER)
    build(ax)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    base = os.path.join(HERE, "pipeline_schematic")
    fig.savefig(base + ".png", facecolor=PAPER, bbox_inches="tight", pad_inches=0.15)
    fig.savefig(base + ".svg", facecolor=PAPER, bbox_inches="tight", pad_inches=0.15)
    print(f"wrote {base}.png and {base}.svg")


if __name__ == "__main__":
    main()
