import graphviz
import os

# ---------------------------------------------------------------------
# The whole story in five boxes: customers use DoiT's software, usage
# is captured, modeled into metrics, turned into insight, and product
# teams decide with it. Pink border = the Product Analyst's job.
# Maximum simplification of the other prep diagrams.
# ---------------------------------------------------------------------

TITLE = (
    "DoiT Product Analyst, the whole story\n"
    "Pink boxes = my job"
)

STEPS = [
    ("use", "CUSTOMERS USE THE SOFTWARE", "clicks, dashboards, alerts", False),
    ("capture", "CAPTURE", "every tracked action becomes an event (Segment)", True),
    ("model", "MODEL", "dbt turns events into trusted metrics", True),
    ("insight", "INSIGHT", "dashboards + readouts for product managers", True),
    ("decide", "PRODUCT TEAMS DECIDE", "build what customers actually use", False),
]

g = graphviz.Digraph("doit_simple", format="png")
g.attr(
    rankdir="LR",
    bgcolor="white",
    pad="0.5",
    nodesep="0.6",
    ranksep="0.7",
    label=TITLE,
    labelloc="t",
    fontname="Helvetica-Bold",
    fontsize="22",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", color="#546E7A", penwidth="2.6", arrowsize="1.1")

for key, title, gloss, mine in STEPS:
    fill = "#FCE4EC" if mine else "#ECEFF1"
    border = "#AD1457" if mine else "#546E7A"
    g.node(key, (
        f'<<TABLE BORDER="3" COLOR="{border}" CELLBORDER="0" CELLSPACING="0" '
        f'CELLPADDING="12" BGCOLOR="{fill}"><TR><TD WIDTH="200">'
        f'<FONT POINT-SIZE="16"><B>{title}</B></FONT><BR/><BR/>'
        f'<FONT POINT-SIZE="12" COLOR="#444444">{gloss}</FONT></TD></TR></TABLE>>'
    ))

for (a, *_), (b, *_) in zip(STEPS, STEPS[1:]):
    g.edge(a, b)

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "doit_simple_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
