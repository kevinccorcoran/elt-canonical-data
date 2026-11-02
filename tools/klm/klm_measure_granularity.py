import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import os

# ---------------------------------------------------------------------
# MEASURE, done correctly: two distinct operations.
#   DRILL (nested hierarchy):  Network -> Market -> Route -> Departure
#   SLICE (cross-cutting, any level):  Fare class  +  Time (horizon/period)
# Right: causes of the gap, grouped the same way. Bottom: line to say.
# ---------------------------------------------------------------------

BLUE = "#2C5C8A"
AMBER = "#B5651D"
GREEN = "#2E7D57"
GREY = "#8A94A6"
RED = "#B23A48"
INK = "#1F2933"

fig, ax = plt.subplots(figsize=(14, 9.6))
ax.set_xlim(0, 100)
ax.set_ylim(0, 100)
ax.axis("off")
fig.patch.set_facecolor("white")


def hdr_card(x, y, w, h, color, title, grain, body, drill, hf=0.46, fs=12.5):
    edge = RED if drill else "#CBD5E1"
    lw = 2.2 if drill else 1.0
    ax.add_patch(FancyBboxPatch((x, y), w, h,
                 boxstyle="round,pad=0,rounding_size=1.1",
                 facecolor="white", edgecolor=edge, linewidth=lw, zorder=2))
    hh = h * hf
    ax.add_patch(FancyBboxPatch((x, y + h - hh), w, hh,
                 boxstyle="round,pad=0,rounding_size=1.1",
                 facecolor=color, edgecolor=color, linewidth=0, zorder=3))
    ax.add_patch(plt.Rectangle((x, y + h - hh), w, hh * 0.5,
                 facecolor=color, edgecolor="none", zorder=3))
    ax.text(x + 1.5, y + h - hh / 2, title, color="white", fontsize=fs,
            fontweight="bold", va="center", ha="left", zorder=4)
    if grain:
        ax.text(x + w - 1.5, y + h - hh / 2, grain, color="white", fontsize=9.5,
                va="center", ha="right", zorder=4)
    ax.text(x + 1.5, y + (h - hh) / 2, body, color="#3E4C59", fontsize=9.6,
            va="center", ha="left", zorder=4)
    if drill:
        ax.text(x + w - 1.5, y + (h - hh) / 2, "◀ drill here", color=RED,
                fontsize=9, va="center", ha="right", zorder=4, style="italic")


# ---- title ----
ax.text(50, 97.5, "MEASURE — drill the hierarchy, then slice the cell",
        fontsize=18.5, fontweight="bold", ha="center", color=INK)
ax.text(50, 93.4, "structure finds WHERE the error is  ·  slicing finds WHAT and WHEN",
        fontsize=12, ha="center", color="#52606D")

# ---- left: DRILL (nested hierarchy) ----
ax.text(4, 89.5, "DRILL — nested, each inside the one above",
        fontsize=11, fontweight="bold", color=INK, ha="left")
LX, LW = 4, 46
drill = [
    (GREY, "1 · Network", "all flights",
     "Total forecast vs actual. Baseline only — too coarse.", False),
    (BLUE, "2 · Market (O&D)", "AMS–JFK",
     "Rank city-pairs by forecast error. Start here.", True),
    (BLUE, "3 · Route / flight no.", "KL641",
     "Within the worst market, which flights drive it?", True),
    (AMBER, "4 · Departure", "KL641, 12 Mar",
     "End of the drill — the isolated cell. One-offs live here.", False),
]
top, ch, gap = 88, 8.4, 3.0
for i, (c, name, grain, body, dr) in enumerate(drill):
    yy = top - i * (ch + gap) - ch
    hdr_card(LX, yy, LW, ch, c, name, grain, body, dr)
    if i < len(drill) - 1:
        ax.add_patch(FancyArrowPatch((LX + LW / 2, yy - 0.3),
                     (LX + LW / 2, yy - gap + 0.5),
                     arrowstyle="-|>", mutation_scale=18,
                     color=RED, linewidth=2.2, zorder=1))

# ---- left-bottom: SLICE (cross-cutting) ----
sy, sh = 27, 15
ax.add_patch(FancyBboxPatch((LX, sy), LW, sh,
             boxstyle="round,pad=0,rounding_size=1.1",
             facecolor="#FBFBF9", edgecolor=INK, linewidth=1.6, zorder=2))
hh = sh * 0.26
ax.add_patch(FancyBboxPatch((LX, sy + sh - hh), LW, hh,
             boxstyle="round,pad=0,rounding_size=1.1",
             facecolor=INK, edgecolor=INK, linewidth=0, zorder=3))
ax.add_patch(plt.Rectangle((LX, sy + sh - hh), LW, hh * 0.5, facecolor=INK, zorder=3))
ax.text(LX + LW / 2, sy + sh - hh / 2,
        "SLICE — cuts across ANY level above",
        color="white", fontsize=11, fontweight="bold", va="center", ha="center", zorder=4)
# two chips
cw = (LW - 4.5) / 2
for j, (col, lab, txt) in enumerate([
        (GREEN, "Fare class / cabin", "cheap vs premium demand"),
        (AMBER, "Time — horizon / period", "late-booking curve, season")]):
    cx = LX + 1.5 + j * (cw + 1.5)
    ax.add_patch(FancyBboxPatch((cx, sy + 1.3), cw, sh - hh - 2.4,
                 boxstyle="round,pad=0,rounding_size=0.9",
                 facecolor="white", edgecolor=col, linewidth=1.6, zorder=3))
    ax.text(cx + cw / 2, sy + sh - hh - 2.3, lab, color=col, fontsize=9.8,
            fontweight="bold", va="center", ha="center", zorder=4)
    ax.text(cx + cw / 2, sy + 3.2, txt, color="#3E4C59", fontsize=8.6,
            va="center", ha="center", zorder=4)
# arrow: drill feeds slice, labeled
ay0 = top - 3 * (ch + gap) - ch - 0.3
ax.add_patch(FancyArrowPatch((LX + LW / 2, ay0), (LX + LW / 2, sy + sh + 0.4),
             arrowstyle="-|>", mutation_scale=16, color=INK,
             linewidth=1.6, linestyle=(0, (4, 3)), zorder=1))
ax.text(LX + LW / 2 + 1.2, (ay0 + sy + sh) / 2, "then slice the cell you found",
        color=INK, fontsize=8.6, fontweight="bold", va="center", ha="left", zorder=4)

# ---- right: causes, grouped ----
RX, RW = 54, 43
PY, PH = 27, 61
ax.add_patch(FancyBboxPatch((RX, PY), RW, PH,
             boxstyle="round,pad=0,rounding_size=1.1",
             facecolor="#FBFBF9", edgecolor=INK, linewidth=1.6, zorder=2))
hh = PH * 0.085
ax.add_patch(FancyBboxPatch((RX, PY + PH - hh), RW, hh,
             boxstyle="round,pad=0,rounding_size=1.1",
             facecolor=INK, edgecolor=INK, linewidth=0, zorder=3))
ax.add_patch(plt.Rectangle((RX, PY + PH - hh), RW, hh * 0.5, facecolor=INK, zorder=3))
ax.text(RX + RW / 2, PY + PH - hh / 2, "What causes Δ — source + your involvement",
        color="white", fontsize=11, fontweight="bold", va="center", ha="center", zorder=4)

# (color, label, cause, source, external?, rank)  rank 1 = you'd own it most
EXT, OWN = "#8A3FFC", "#52606D"

def tier(r):
    return GREEN if r <= 3 else (AMBER if r <= 5 else GREY)

rows = [
    ("group", "DRILL levels", None, None, None, None),
    (GREY, "Network", "macro shock (fuel, FX)", "macro / fuel / FX feeds", True, 6),
    (BLUE, "Market", "rival adds seats / cuts fares", "OAG/Cirium; Infare", True, 7),
    (BLUE, "Route", "up-gauge, retiming; bank", "own schedule system", False, 4),
    (AMBER, "Departure", "one-off — weather, strike", "ops/weather; cancel spike", True, 5),
    ("group", "SLICE dimensions", None, None, None, None),
    (GREEN, "Fare class", "mix shift — corporate down", "bookings by class & channel", False, 3),
    (AMBER, "Time", "booking curve later", "booking curve (days-to-dep)", False, 2),
    ("group", "Always check first", None, None, None, None),
    (RED, "Data", "cancels/dupes, feed delay", "QA vs reservation system", False, 1),
]
ax.text(RX + RW - 2, PY + PH - hh - 1.4, "tag = how much you'd own it",
        color="#7B8794", fontsize=7.4, va="center", ha="right", zorder=4, style="italic")

def tier_word(r):
    return ("HIGH", GREEN) if r <= 3 else (("MED", AMBER) if r <= 5 else ("LOW", GREY))
iy = PY + PH - hh - 4.4
for col, lab, txt, src, ext, rank in rows:
    if col == "group":
        ax.text(RX + 2, iy, lab, color=INK, fontsize=9.5, fontweight="bold",
                va="center", ha="left", zorder=4)
        iy -= 3.6
        continue
    ax.add_patch(plt.Rectangle((RX + 2.4, iy - 0.4), 1.5, 1.5, facecolor=col, zorder=4))
    ax.text(RX + 5, iy + 0.3, lab, color=col, fontsize=9.2, fontweight="bold",
            va="center", ha="left", zorder=4)
    ax.text(RX + 14.5, iy + 0.3, txt, color="#3E4C59", fontsize=8.3,
            va="center", ha="left", zorder=4)
    tag = "external:" if ext else "own data:"
    ax.text(RX + 14.5, iy - 2.1, tag, color=(EXT if ext else OWN), fontsize=7.6,
            fontweight="bold", va="center", ha="left", zorder=4, style="italic")
    ax.text(RX + 22.5, iy - 2.1, src, color="#7B8794", fontsize=7.6,
            va="center", ha="left", zorder=4, style="italic")
    # involvement tier tag (right)
    word, wcol = tier_word(rank)
    bw = 7.2
    bx = RX + RW - bw - 1.6
    ax.add_patch(FancyBboxPatch((bx, iy - 1.3), bw, 2.8,
                 boxstyle="round,pad=0,rounding_size=0.7",
                 facecolor=wcol, edgecolor="none", zorder=4))
    ax.text(bx + bw / 2, iy + 0.1, word, color="white", fontsize=8.2,
            fontweight="bold", va="center", ha="center", zorder=5)
    iy -= 5.6

# ---- bottom banner ----
ax.add_patch(FancyBboxPatch((4, 2), 92, 17.5,
             boxstyle="round,pad=0,rounding_size=1.1",
             facecolor=INK, edgecolor=INK, zorder=2))
ax.text(7, 17.2, "Say it like this:", color="#F4C430",
        fontsize=12, fontweight="bold", va="top", ha="left", zorder=4)
ax.text(7, 13.2,
        "“I drill the hierarchy — network, market, flight, departure — to find WHERE the error\n"
        "concentrates, then slice that cell by fare class and booking horizon to see WHAT and WHEN.\n"
        "And before I blame the model, I check the data — a cancel left in the feed looks like over-demand.”",
        color="white", fontsize=10.6, va="top", ha="left", zorder=4, linespacing=1.5)

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_measure_granularity.png")
plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
print(f"wrote {out}")
