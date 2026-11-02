import graphviz
import os

# ---------------------------------------------------------------------
# What Segment is: one pipe in the middle. Sources fan in on the left,
# Segment sits in the center (with its add-ons), destinations fan out
# on the right. Interview-prep reference, not pipeline code.
# ---------------------------------------------------------------------

TITLE = (
    "What Segment is: track an event once, send it everywhere\n"
    "A CDP (customer data platform): one pipe between your apps and your tools"
)


def box(g, name, title, sub, fill, border, w="150"):
    label = (
        f'<<TABLE BORDER="2" COLOR="{border}" CELLBORDER="0" CELLSPACING="0" '
        f'CELLPADDING="8" BGCOLOR="{fill}"><TR><TD WIDTH="{w}">'
        f'<FONT POINT-SIZE="13"><B>{title}</B></FONT><BR/>'
        f'<FONT POINT-SIZE="10" COLOR="#555555">{sub}</FONT></TD></TR></TABLE>>'
    )
    g.node(name, label)


g = graphviz.Digraph("segment_flow", format="png")
g.attr(
    rankdir="LR",
    bgcolor="white",
    pad="0.4",
    nodesep="0.35",
    ranksep="1.1",
    label=TITLE,
    labelloc="t",
    fontname="Helvetica-Bold",
    fontsize="18",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", color="#607D8B", penwidth="1.8", arrowsize="0.8")

# --- sources (fan in) --------------------------------------------------
sources = [
    ("s_web", "Website", "clicks, pageviews"),
    ("s_app", "Mobile app", "taps, screens"),
    ("s_backend", "Backend / server", "signups, payments"),
]
for n, t, s in sources:
    box(g, n, t, s, "#E3F2FD", "#1565C0")

# --- Segment in the middle --------------------------------------------
seg_label = (
    '<<TABLE BORDER="3" COLOR="#00695C" CELLBORDER="0" CELLSPACING="0" '
    'CELLPADDING="10" BGCOLOR="#E0F2F1"><TR><TD WIDTH="230">'
    '<FONT POINT-SIZE="17"><B>SEGMENT</B></FONT><BR/>'
    '<FONT POINT-SIZE="11" COLOR="#555555">the pipe in the middle</FONT></TD></TR>'
    '<TR><TD ALIGN="LEFT" BALIGN="LEFT" WIDTH="230">'
    '<FONT POINT-SIZE="11"><B>Catch every event once</B><BR/>'
    '<FONT COLOR="#555555">one install, all sources</FONT><BR/><BR/>'
    '<B>+ Protocols</B> <FONT COLOR="#555555">validate vs the tracking plan</FONT><BR/>'
    '<B>+ Functions</B> <FONT COLOR="#555555">reshape / route with code</FONT><BR/>'
    '<B>Fan out to every tool</B> <FONT COLOR="#555555">no per-tool wiring</FONT>'
    '</FONT></TD></TR></TABLE>>'
)
g.node("segment", seg_label)

# --- destinations (fan out) -------------------------------------------
dests = [
    ("d_wh", "Warehouse", "BigQuery, dbt, metrics"),
    ("d_mixpanel", "Mixpanel", "product analytics"),
    ("d_ads", "Ad + email tools", "marketing"),
    ("d_support", "Support / CRM", "customer tools"),
]
for n, t, s in dests:
    box(g, n, t, s, "#F3E5F5", "#7B1FA2")

# keep the fan-in and fan-out columns tidy
with g.subgraph() as s:
    s.attr(rank="same")
    for n, *_ in sources:
        s.node(n)
with g.subgraph() as s:
    s.attr(rank="same")
    for n, *_ in dests:
        s.node(n)

for n, *_ in sources:
    g.edge(n, "segment")
for n, *_ in dests:
    g.edge("segment", n, color="#8E24AA")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "segment_flow_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
