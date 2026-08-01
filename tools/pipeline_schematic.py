"""Conceptual pipeline schematic: data streams -> quality gates -> live.

A hand-sketched, deliberately abstract view of the platform as a flow (distinct
from pipeline_grouped.py, which is the concrete schema-by-schema pipeline):

    Start -> three parallel DATA STREAMS -> per-stream quality-gate funnels
    (raw volume in, refined output at the tip) -> the streams merge as they
    mature -> planned gates -> a LIVE finish line crossed at two endpoints.

Funnel shape = a quality gate (wide intake on the left, refined tip on the
right). The green->red gradient reads raw -> refined. graphviz linear gradients
carry two stops only (and `striped` is unsupported on triangles), so the gate
is green->red rather than the sketch's green/amber/red three-tone.

Border encodes maturity:
    solid  = built
    dashed = planned / in progress

Renders with the `graphviz` package + the `dot` binary, same as
pipeline_grouped.py. Output: tools/pipeline_schematic.(png|svg).
"""
import os

import graphviz

# ── palette (matches the README schematic / dashboard language) ──
INK      = "#1b2130"
MUTED    = "#6b7382"
ACCENT   = "#2f9e6b"          # "live"
FUNNEL   = "#2f9e6b:#d6402a"  # green (raw) -> red (refined); 2-stop gradient
PAPER    = "#ffffff"

HERE = os.path.dirname(os.path.abspath(__file__))


def _funnel(g, name, planned=False, small=False):
    """A right-pointing quality-gate funnel (wide intake -> refined tip)."""
    g.node(
        name, "",
        shape="triangle", orientation="90",         # apex points right (east)
        style="filled,dashed" if planned else "filled",
        fillcolor=FUNNEL, gradientangle="0",
        color=MUTED if planned else INK,
        penwidth="2",
        fixedsize="true",
        width="1.05" if small else "1.5",
        height="0.8" if small else "1.05",
    )


def build() -> graphviz.Digraph:
    g = graphviz.Digraph("pipeline_schematic", format="png")
    g.attr(
        rankdir="LR", bgcolor=PAPER, fontname="Helvetica",
        nodesep="0.5", ranksep="0.85", pad="0.4",
    )
    g.attr("node", fontname="Helvetica", fontcolor=INK)
    g.attr("edge", color=MUTED, penwidth="1.6", arrowsize="0.8")

    # Start
    g.node("start", "Start", shape="ellipse", style="filled", fillcolor=PAPER,
           color=INK, penwidth="2", width="1.1", height="0.7", fixedsize="true")

    # Data Streams: three entry nodes on a labelled axis
    with g.subgraph(name="cluster_streams") as s:
        s.attr(label="DATA STREAMS", labelloc="t", fontname="Helvetica",
               fontcolor=INK, color=MUTED, style="dashed", penwidth="1.2")
        for n in ("s1", "s2", "s3"):
            s.node(n, "", shape="point", width="0.14", color=INK)

    # Stage 1 funnels: top two built, bottom planned
    _funnel(g, "fa", planned=False)
    _funnel(g, "fb", planned=False)
    _funnel(g, "fc", planned=True)

    # Stage 2: merged upper + lower branch (both planned)
    _funnel(g, "fd", planned=True)
    _funnel(g, "fe", planned=True)

    # Pre-finish live gates (planned)
    _funnel(g, "p1", planned=True, small=True)
    _funnel(g, "p2", planned=True, small=True)

    # Finish · Live
    g.node("finish", "Finish\n·\nLive", shape="box", style="filled",
           fillcolor=ACCENT, fontcolor="white", color=ACCENT,
           width="1.05", height="2.6", fixedsize="true", fontname="Helvetica")

    # ── flow ──
    for n in ("s1", "s2", "s3"):
        g.edge("start", n)
    g.edge("s1", "fa"); g.edge("s2", "fb"); g.edge("s3", "fc")
    g.edge("fa", "fd"); g.edge("fb", "fd")   # top two merge
    g.edge("fc", "fe")                        # lower branch
    g.edge("fd", "p1"); g.edge("fd", "p2")    # upper serves two endpoints
    g.edge("fe", "p2")                        # lower joins the second
    g.edge("p1", "finish"); g.edge("p2", "finish")

    # legend
    with g.subgraph(name="cluster_legend") as lg:
        lg.attr(label="", color=MUTED, style="dashed", penwidth="1")
        lg.node("lg", shape="plaintext", fontname="Helvetica", label=f'''<
          <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="4" CELLPADDING="3">
            <TR><TD BGCOLOR="{ACCENT}" WIDTH="26"></TD>
                <TD ALIGN="LEFT"><FONT COLOR="{INK}">quality gate: raw (green) &#8594; refined (red)</FONT></TD></TR>
            <TR><TD ALIGN="LEFT"><FONT COLOR="{INK}">solid border</FONT></TD>
                <TD ALIGN="LEFT"><FONT COLOR="{MUTED}">built</FONT></TD></TR>
            <TR><TD ALIGN="LEFT"><FONT COLOR="{MUTED}">dashed border</FONT></TD>
                <TD ALIGN="LEFT"><FONT COLOR="{MUTED}">planned / in progress</FONT></TD></TR>
          </TABLE>>''')

    return g


def main() -> None:
    g = build()
    base = os.path.join(HERE, "pipeline_schematic")
    g.render(base, format="png", cleanup=True)
    g.render(base, format="svg", cleanup=True)
    print(f"wrote {base}.png and {base}.svg")


if __name__ == "__main__":
    main()
