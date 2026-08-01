"""Conceptual pipeline schematic: data streams -> quality gates -> live.

A faithful digital rendering of the hand sketch (distinct from
pipeline_grouped.py, the concrete schema-by-schema pipeline):

    Start -> three parallel DATA STREAMS -> a STAGE-1 box of per-stream
    quality-gate funnels -> a STAGE-2 box where the streams merge -> two
    planned live gates that cross into a LIVE finish line at two endpoints.

Drawn with matplotlib (not graphviz): the funnels need real right-pointing
triangles with a green -> amber -> red gradient (raw volume screened to refined
output), which graphviz cannot do (its gradients are two-stop and it mirrors
oriented triangles inside clusters). matplotlib gives exact shape + 3-tone.

Encoding:
    funnel        a quality gate: wide intake on the left, refined tip on the right
    green->red    raw volume in, screened + refined out
    solid outline built
    dashed outline planned / in progress

Output: tools/pipeline_schematic.(png|svg).
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Ellipse, FancyArrowPatch, PathPatch, Rectangle
from matplotlib.path import Path

# ── palette ──
INK    = "#1b2130"
MUTED  = "#8a93a3"
ACCENT = "#2f9e6b"           # "live"
GREEN  = "#2f9e6b"
AMBER  = "#e2871b"
RED    = "#d6402a"
BOX1   = INK
BOX2   = "#c0553a"           # stage-2 box drawn red in the sketch
PAPER  = "#ffffff"

GATE_CMAP = LinearSegmentedColormap.from_list("gate", [GREEN, AMBER, RED])

HERE = os.path.dirname(os.path.abspath(__file__))


def funnel(ax, x_left, y_c, w, h, planned=False):
    """Right-pointing gate: wide intake (left) narrowing to a refined tip."""
    verts = [(x_left, y_c - h / 2), (x_left, y_c + h / 2), (x_left + w, y_c)]
    clip = PathPatch(Path(verts + [verts[0]], closed=True),
                     facecolor="none", edgecolor="none", zorder=2)
    ax.add_patch(clip)
    grad = np.linspace(0, 1, 256).reshape(1, -1)
    im = ax.imshow(grad, extent=[x_left, x_left + w, y_c - h / 2, y_c + h / 2],
                   origin="lower", aspect="auto", cmap=GATE_CMAP,
                   alpha=0.42 if planned else 1.0, zorder=2)
    im.set_clip_path(clip)
    ax.add_patch(PathPatch(
        Path(verts + [verts[0]], closed=True), facecolor="none",
        edgecolor=MUTED if planned else INK, linewidth=2,
        linestyle=(0, (5, 4)) if planned else "solid", zorder=3))


def arrow(ax, p0, p1, rad=0.0):
    ax.add_patch(FancyArrowPatch(
        p0, p1, arrowstyle="-|>", mutation_scale=13, lw=1.6, color=MUTED,
        shrinkA=2, shrinkB=3, connectionstyle=f"arc3,rad={rad}", zorder=1))


def line(ax, p0, p1, color=MUTED, lw=1.6, ls="solid"):
    ax.plot([p0[0], p1[0]], [p0[1], p1[1]], color=color, lw=lw, ls=ls, zorder=1)


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
        ax.add_patch(plt.Circle((2.75, y), 0.11, color=INK, zorder=4))

    # ── Stage boxes ──
    ax.add_patch(Rectangle((4.15, 1.15), 3.15, 6.7, facecolor="none",
                          edgecolor=BOX1, linewidth=2, zorder=1))
    ax.add_patch(Rectangle((8.35, 1.7), 2.5, 5.6, facecolor="none",
                          edgecolor=BOX2, linewidth=2, zorder=1))

    # ── Stage 1 funnels (top/mid built, bottom planned) ──
    fw, fh, x1 = 2.1, 1.35, 4.55
    funnel(ax, x1, yT, fw, fh, planned=False)
    funnel(ax, x1, yM, fw, fh, planned=False)
    funnel(ax, x1, yB, fw, fh, planned=True)

    # ── Stage 2 funnels (merged upper + lower, both planned) ──
    yU, yL = 5.55, 3.15
    fw2, x2 = 1.85, 8.6
    funnel(ax, x2, yU, fw2, 1.3, planned=True)
    funnel(ax, x2, yL, fw2, 1.3, planned=True)

    # ── Pre-finish live gates (planned) ──
    x3, pw = 11.55, 1.15
    funnel(ax, x3, yT, pw, 0.85, planned=True)
    funnel(ax, x3, yB, pw, 0.85, planned=True)

    # ── Finish · Live ──
    ax.plot([14.1, 14.1], [1.2, 7.8], color=ACCENT, lw=3, zorder=2)
    for y in (yT, yB):
        ax.add_patch(plt.Circle((14.1, y), 0.11, color=ACCENT, zorder=4))
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
    arrow(ax, (x2 + fw2, yU), (x3 - 0.03, yB), rad=0.0)
    arrow(ax, (x2 + fw2, yL), (x3 - 0.03, yT), rad=0.0)
    # live gates -> finish
    arrow(ax, (x3 + pw, yT), (14.05, yT))
    arrow(ax, (x3 + pw, yB), (14.05, yB))

    _legend(ax)


def _legend(ax):
    x0, y0 = 0.35, 0.55
    # gradient swatch
    grad = np.linspace(0, 1, 256).reshape(1, -1)
    ax.imshow(grad, extent=[x0, x0 + 0.9, y0 + 0.55, y0 + 0.85],
              origin="lower", aspect="auto", cmap=GATE_CMAP, zorder=3)
    ax.add_patch(Rectangle((x0, y0 + 0.55), 0.9, 0.3, facecolor="none",
                          edgecolor=INK, lw=1, zorder=4))
    ax.text(x0 + 1.05, y0 + 0.70, "quality gate: raw (green) → refined (red)",
            va="center", fontsize=9.5, color=INK)
    ax.text(x0, y0 + 0.20, "solid border", va="center", fontsize=9.5,
            color=INK, fontweight="bold")
    ax.text(x0 + 1.6, y0 + 0.20, "built", va="center", fontsize=9.5, color=MUTED)
    ax.text(x0, y0 - 0.15, "dashed border", va="center", fontsize=9.5,
            color=MUTED, fontweight="bold")
    ax.text(x0 + 1.6, y0 - 0.15, "planned / in progress", va="center",
            fontsize=9.5, color=MUTED)


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
