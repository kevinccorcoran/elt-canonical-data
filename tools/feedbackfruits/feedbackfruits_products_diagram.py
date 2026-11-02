import graphviz
import os

# ---------------------------------------------------------------------
# FeedbackFruits, simple view: the instructor picks a tool in the LMS,
# the tools fall into four purposes, and all activity becomes data for
# the Analytics Engineer (pink).
# Conceptual reconstruction from public product pages, not a live schema.
# ---------------------------------------------------------------------

TITLE = "FeedbackFruits: what the products do"

# purpose, tools, what it is for
GROUPS = [
    ("PEER FEEDBACK",     ["Peer Review", "Group Member Evaluation"],  "students give each other feedback"),
    ("ENGAGEMENT",        ["Interactive Video", "Interactive Document"], "students answer questions while\nwatching or reading"),
    ("ASSESSMENT",        ["Assignment Review", "Skill Review"],       "teachers grade against a rubric"),
    ("AI FEEDBACK",       ["Automated Feedback"],                      "instant feedback on drafts"),
]


def box(title, lines, gloss, mine=False, width=230):
    fill = "#FCE4EC" if mine else "#ECEFF1"
    border = "#AD1457" if mine else "#546E7A"
    items = "<BR/>".join(lines)
    gloss = gloss.replace("\n", "<BR/>")
    return (
        f'<<TABLE BORDER="3" COLOR="{border}" CELLBORDER="0" CELLSPACING="0" '
        f'CELLPADDING="10" BGCOLOR="{fill}"><TR><TD WIDTH="{width}">'
        f'<FONT POINT-SIZE="15"><B>{title}</B></FONT><BR/><BR/>'
        f'<FONT POINT-SIZE="12">{items}</FONT>'
        + (f'<BR/><BR/><FONT POINT-SIZE="11" COLOR="#555555"><I>{gloss}</I></FONT>' if gloss else "")
        + '</TD></TR></TABLE>>'
    )


g = graphviz.Digraph("feedbackfruits_products", format="png")
g.attr(
    rankdir="LR",
    bgcolor="white",
    pad="0.5",
    nodesep="0.3",
    ranksep="0.8",
    label=TITLE,
    labelloc="t",
    fontname="Helvetica-Bold",
    fontsize="22",
    compound="true",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", color="#546E7A", penwidth="2.6", arrowsize="1.1")

g.node("lms", box("TEACHER", ["adds an activity in the", "university LMS (Canvas etc.)"], "", width=200))

with g.subgraph(name="cluster_tools") as c:
    c.attr(
        label="FeedbackFruits tools",
        fontname="Helvetica-Bold",
        fontsize="15",
        color="#3B6FB0",
        bgcolor="#F4F8FF",
        margin="16",
    )
    for i, (title, tools, gloss) in reversed(list(enumerate(GROUPS))):
        c.node(f"g{i}", box(title, tools, gloss))

g.node("data", box("ANALYTICS ENGINEER", ["all activity becomes data", "dbt models, dashboards"], "", mine=True, width=210))

# One arrow in, one arrow out
g.edge("lms", "g1", lhead="cluster_tools")
g.edge("g1", "data", ltail="cluster_tools")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feedbackfruits_products_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
