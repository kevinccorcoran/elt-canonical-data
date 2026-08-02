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
import textwrap

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
CONTAINER = "#3f6d8c"        # steel blue: the Docker container layer
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
    ax.set_ylim(-4.7, 10.2)
    ax.set_aspect("equal")                # never distort funnels/circles
    ax.axis("off")

    # ── title ──
    ax.text(0.3, 10.0, "Team Architecture", fontsize=21, fontweight="bold",
            color=INK, va="top")
    ax.text(0.3, 9.35, "A proposed analytics-engineering delivery team, mapped to the "
            "dbt model pipeline — from raw data streams to production.",
            fontsize=10.5, color=MUTED, va="top")
    ax.plot([0.3, 15.7], [9.02, 9.02], color="#d9dee6", lw=1.1)

    # ── model-band legend: what the funnel colours mean ──
    ax.text(0.3, 8.72, "MODEL BANDS", fontsize=9, color=MUTED,
            family="monospace", va="center")
    for color, label, x in ((RED, "actual model", 2.55),
                            (AMBER, "model optimization", 5.35),
                            (GREEN, "model testing", 9.05)):
        ax.add_patch(Polygon([(x, 8.72 - 0.15), (x, 8.72 + 0.15), (x + 0.3, 8.72)],
                           closed=True, facecolor=color, edgecolor=INK,
                           linewidth=0.8, zorder=4))
        ax.text(x + 0.45, 8.72, label, fontsize=9, color=INK, va="center")

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

    # ── Docker container: the whole pipeline + deliverables run containerized ──
    ax.add_patch(Rectangle((3.5, 0.3), 11.15, 8.1, facecolor="none",
                          edgecolor=CONTAINER, linewidth=1.6,
                          linestyle=(0, (6, 3)), zorder=0))
    ax.text(14.5, 8.28, "DOCKER CONTAINER · orchestrator · dbt · Shiny", fontsize=9,
            color=CONTAINER, family="monospace", va="top", ha="right")

    # ── Tech lead owns the whole model pipeline: one big black box ──
    ax.add_patch(Rectangle((3.85, 1.45), 9.2, 6.6, facecolor="none",
                          edgecolor=BOX1, linewidth=2.2, zorder=1))
    ax.text(3.98, 7.9, "TECH LEAD · dbt architecture", fontsize=9,
            color=INK, family="monospace", va="top")

    # ── Stage 1: three per-stream funnels ──
    fw, fh, x1 = 2.05, 1.5, 4.35
    funnel(ax, x1, yT, fw, fh, FA)
    funnel(ax, x1, yM, fw, fh, FB)
    funnel(ax, x1, yB, fw, fh, FC)

    # ── Stage 2: two merged funnels ──
    yU, yL = 5.7, 3.7
    fw2, fh2, x2 = 1.8, 1.4, 8.15
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
    # stage 2 -> live gates: many-to-many (each model feeds each endpoint)
    for ys in (yU, yL):
        for yg in (yTg, yBg):
            arrow(ax, (x2 + fw2, ys), (x3 - 0.03, yg))
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
    # Two deliverables built by the testing & framework dev, under the models
    # they act on. Each box: title + what it does + tool.
    gy0, h, w = 0.3, 1.0, 3.1
    def gbox(cx, title, does, tool):
        ax.add_patch(Rectangle((cx - w / 2, gy0), w, h, facecolor=GREEN_L,
                              edgecolor=GREEN, linewidth=1.8, zorder=2))
        cy = gy0 + h / 2
        ax.text(cx, cy + 0.27, title, ha="center", va="center", color=GREEN_D,
                fontsize=9, fontweight="bold", zorder=3)
        ax.text(cx, cy - 0.01, does, ha="center", va="center", color=GREEN_D,
                fontsize=8.2, zorder=3)
        ax.text(cx, cy - 0.28, tool, ha="center", va="center", color=GREEN,
                fontsize=7.4, style="italic", zorder=3)
    gbox(6.0, "Automated test suite", "tests every model", "pytest · matrix-notify")
    gbox(10.2, "Quality dashboard", "monitors all models", "Shiny")
    for cx in (6.0, 10.2):                       # ticks up to the models
        ax.plot([cx, cx], [gy0 + h, 1.45], color=GREEN, ls=(0, (1, 2.2)),
                lw=1.3, zorder=1)


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
                _split_circle(ax, x, y, r, AMBER, RED)
            else:
                ax.add_patch(Circle((x, y), r, facecolor=c, edgecolor=INK,
                                   linewidth=0.8, zorder=4))
    # role · meaning key
    ax.text(3.2, -0.25, "ROLE", fontsize=8.5, color=MUTED, family="monospace")
    ax.text(6.15, -0.25, "role · responsibilities · dbt", fontsize=8.5,
            color=MUTED, family="monospace")
    rows = [
        (BLACK, "Tech lead",
         "designs the dbt architecture for the client's architecture + business needs — future-proofing (hard) · dbt: deep"),
        (RED, "Model dev lead",
         "goal: across the finish line as fast as possible — initial model, imperfect but delivered · dbt: med–high (variables)"),
        (AMBER, "Dev · QA & optimization",
         "ensures each model is optimized — logic + performance (query opt, splitting, index strategy) · dbt: working"),
        (GREEN, "Dev · testing & framework",
         "goal: every model auto-monitored for source-data, logic + pipeline issues · integration, refinement, deep-dive defect analysis · dbt: small (adds dbt validation)"),
    ]
    y = -0.72
    for c, role, means in rows:
        wrapped = textwrap.fill(means, width=94)
        nlines = wrapped.count("\n") + 1
        yc = y - 0.11                         # first-line centre for swatch + role
        ax.add_patch(Circle((3.05, yc), 0.12, facecolor=c, edgecolor=INK,
                           linewidth=0.8, zorder=4))
        ax.text(3.3, yc, role, fontsize=8.6, color=INK, va="center", fontweight="bold")
        ax.text(6.15, y, wrapped, fontsize=7.6, color=INK, va="top", linespacing=1.5)
        y -= 0.24 * nlines + 0.28
    ax.text(3.3, y + 0.06,
            "Split circle = one person covering QA/optimization + model dev lead.",
            fontsize=8, color=MUTED, va="center", style="italic")


def _style_key(ax):
    """Pen-style key (done / in progress / planned) as a bottom strip."""
    y = -4.2
    ax.text(0.55, y, "BUILD", fontsize=9.5, color=MUTED, family="monospace",
            va="center")
    for style, label, x in (("solid", "done", 2.0),
                            ("hatch", "in progress", 4.1),
                            ("dotted", "planned", 6.9)):
        rect = [(x, y - 0.14), (x + 0.5, y - 0.14),
                (x + 0.5, y + 0.14), (x, y + 0.14)]
        _band(ax, rect, INK if style == "solid" else MUTED, style)
        ax.text(x + 0.62, y, label, va="center", fontsize=9, color=INK)


def main():
    fig, ax = plt.subplots(figsize=(11.9, 11.1), dpi=150)
    fig.patch.set_facecolor(PAPER)
    build(ax)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    base = os.path.join(HERE, "pipeline_schematic")
    fig.savefig(base + ".png", facecolor=PAPER, bbox_inches="tight", pad_inches=0.15)
    fig.savefig(base + ".svg", facecolor=PAPER, bbox_inches="tight", pad_inches=0.15)
    print(f"wrote {base}.png and {base}.svg")


if __name__ == "__main__":
    main()
