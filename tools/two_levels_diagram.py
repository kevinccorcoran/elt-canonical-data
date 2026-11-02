import graphviz
import os

# ---------------------------------------------------------------------
# The two levels of analytics that are easy to conflate:
#   top    = what DoiT's PRODUCT analyzes (customers' cloud bills)
#   bottom = what I analyze as Product Analyst (usage OF that product)
# Same shape, one level apart. Interview-prep reference.
# ---------------------------------------------------------------------

TITLE = (
    "Two levels: the product analyzes cloud bills; I analyze the product\n"
    "Same shape of work, one level apart"
)


def box(g, name, title, sub, fill, border, w="215"):
    g.node(name, (
        f'<<TABLE BORDER="3" COLOR="{border}" CELLBORDER="0" CELLSPACING="0" '
        f'CELLPADDING="11" BGCOLOR="{fill}"><TR><TD WIDTH="{w}">'
        f'<FONT POINT-SIZE="15"><B>{title}</B></FONT><BR/><BR/>'
        f'<FONT POINT-SIZE="11" COLOR="#444444">{sub}</FONT></TD></TR></TABLE>>'
    ))


g = graphviz.Digraph("two_levels", format="png")
g.attr(
    rankdir="LR",
    bgcolor="white",
    pad="0.5",
    nodesep="0.9",
    ranksep="0.8",
    label=TITLE,
    labelloc="t",
    fontname="Helvetica-Bold",
    fontsize="21",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", penwidth="2.4", arrowsize="1.0")

B_FILL, B_BORDER = "#E3F2FD", "#1565C0"
P_FILL, P_BORDER = "#FCE4EC", "#AD1457"

# --- level 1: the product's analytics (blue) ---------------------------
box(g, "bills", "CLOUD BILLS", "customers' AWS / GCP / Azure spend",
    B_FILL, B_BORDER)
box(g, "product", "THE DOIT PRODUCT ANALYZES THEM",
    "dashboards, alerts, recommendations, Flexsave", B_FILL, B_BORDER)
box(g, "saves", "CUSTOMER CUTS WASTE", "lower bill, accountable teams",
    B_FILL, B_BORDER)

# --- level 2: my analytics (pink) --------------------------------------
box(g, "usage", "PRODUCT USAGE", "every customer click in those features",
    P_FILL, P_BORDER)
box(g, "me", "I ANALYZE THE PRODUCT", "Segment, dbt, metrics: what gets used",
    P_FILL, P_BORDER)
box(g, "team", "PRODUCT TEAM BUILDS BETTER", "roadmap follows real usage",
    P_FILL, P_BORDER)

with g.subgraph() as s:
    s.attr(rank="same")
    s.node("bills")
    s.node("usage")

g.edge("bills", "product", color=B_BORDER)
g.edge("product", "saves", color=B_BORDER)
g.edge("usage", "me", color=P_BORDER)
g.edge("me", "team", color=P_BORDER)

# the links between the levels
g.edge("product", "usage", style="dashed", color="#78909C",
       label="  using it creates events", fontname="Helvetica",
       fontsize="12", fontcolor="#546E7A")
g.edge("team", "saves", style="dashed", color="#78909C",
       label="  better product, more use, renewals  ", fontname="Helvetica",
       fontsize="12", fontcolor="#546E7A", constraint="false")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "two_levels_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
