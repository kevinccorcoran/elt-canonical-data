import graphviz
import os

# ---------------------------------------------------------------------
# FLYR ancillary product: the five feeds an airline (Virgin Atlantic)
# sends, how they land in the canonical model, and the sales loop that
# closes back on them. Pink border = the data-quality / CDM job (mine).
# Same shape as the KLM task-loop story and AlphaStream: sources land,
# get canonicalized, drive a decision/product, and feed back.
# Conceptual reconstruction of the source categories, not a live schema.
# ---------------------------------------------------------------------

TITLE = (
    "FLYR ancillary product, the five feeds\n"
    "Pink = my job (data quality + CDM)"
)

# key, title, gloss, mine
SOURCES = [
    ("pnr",   "BOOKING / PNR",            "passenger records, itineraries", False),
    ("fare",  "FARE &amp; PRICING",       "fare classes, rules",            False),
    ("inv",   "INVENTORY / AVAILABILITY", "seat maps, capacity",            False),
    ("sched", "FLIGHT SCHEDULE / OPS",    "routes, times, aircraft",        False),
    ("sales", "TRANSACTION / SALES",      "units, revenue, attach rate",    False),
]

PLATFORM = [
    ("ingest", "INGEST &amp; CANONICALIZE", "clean, dedupe, standardize",           True),
    ("merch",  "MERCHANDISING ENGINE",      "build &amp; price the extras",         False),
    ("offer",  "ANCILLARY OFFERS",          "seats · bags · lounge · wifi · meals", False),
]


def cell(title, gloss, mine):
    fill = "#FCE4EC" if mine else "#ECEFF1"
    border = "#AD1457" if mine else "#546E7A"
    return (
        f'<<TABLE BORDER="3" COLOR="{border}" CELLBORDER="0" CELLSPACING="0" '
        f'CELLPADDING="12" BGCOLOR="{fill}"><TR><TD WIDTH="210">'
        f'<FONT POINT-SIZE="15"><B>{title}</B></FONT><BR/><BR/>'
        f'<FONT POINT-SIZE="11" COLOR="#444444">{gloss}</FONT></TD></TR></TABLE>>'
    )


g = graphviz.Digraph("flyr_ancillary", format="png")
g.attr(
    rankdir="LR",
    bgcolor="white",
    pad="0.5",
    nodesep="0.4",
    ranksep="0.9",
    label=TITLE,
    labelloc="t",
    fontname="Helvetica-Bold",
    fontsize="22",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", color="#546E7A", penwidth="2.4", arrowsize="1.0")

# Virgin Atlantic source feeds, boxed together
with g.subgraph(name="cluster_vs") as c:
    c.attr(
        label="Virgin Atlantic — source feeds",
        fontname="Helvetica-Bold",
        fontsize="14",
        color="#3B6FB0",
        bgcolor="#F4F8FF",
        margin="16",
    )
    for key, title, gloss, mine in SOURCES:
        c.node(key, cell(title, gloss, mine))

# Platform spine
for key, title, gloss, mine in PLATFORM:
    g.node(key, cell(title, gloss, mine))

# Fan-in: every feed lands in ingest / canonicalize
for key, *_ in SOURCES:
    g.edge(key, "ingest")

# Spine: canonical -> merchandising -> offers
g.edge("ingest", "merch")
g.edge("merch", "offer")

# Feedback loop: sold offers become new sales records
g.edge(
    "offer", "sales",
    style="dashed", color="#AD1457",
    label="sold", fontcolor="#AD1457",
    constraint="false",
)

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flyr_ancillary_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
