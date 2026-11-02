import graphviz
import os

# ---------------------------------------------------------------------
# FeedbackFruits, ownership view: what is external (owned by the
# university, FeedbackFruits depends on it) versus what FeedbackFruits
# owns. Pink = the Analytics Engineer job.
# Conceptual reconstruction from public product pages, not a live schema.
# ---------------------------------------------------------------------

TITLE = "FeedbackFruits: external dependencies vs what it owns"

EXTERNAL = [
    ("UNIVERSITY",          ["the paying customer", "buys the license"]),
    ("COURSE SYSTEM (LMS)", ["Canvas, Brightspace, etc.", "hosts the course", "logs users in (LTI)"]),
    ("TEACHERS + STUDENTS", ["no FeedbackFruits signup", "arrive through the LMS"]),
]

TOOLS = [
    ("PEER FEEDBACK", ["Peer Review", "Group Member Evaluation"],    "students give each other feedback"),
    ("ENGAGEMENT",    ["Interactive Video", "Interactive Document"], "students answer questions while watching or reading"),
    ("ASSESSMENT",    ["Assignment Review", "Skill Review"],         "teachers grade against a rubric"),
    ("AI FEEDBACK",   ["Automated Feedback"],                        "instant feedback on drafts"),
]


def card(title, lines, gloss="", fill="#ECEFF1", border="#546E7A", width=240):
    items = "<BR/>".join(lines)
    g_html = f'<BR/><BR/><FONT POINT-SIZE="11" COLOR="#555555"><I>{gloss}</I></FONT>' if gloss else ""
    return (
        f'<TABLE BORDER="3" COLOR="{border}" CELLBORDER="0" CELLSPACING="0" '
        f'CELLPADDING="10" BGCOLOR="{fill}"><TR><TD WIDTH="{width}">'
        f'<FONT POINT-SIZE="15"><B>{title}</B></FONT><BR/><BR/>'
        f'<FONT POINT-SIZE="12">{items}</FONT>{g_html}</TD></TR></TABLE>'
    )


def column(cards):
    rows = "".join(f"<TR><TD>{c}</TD></TR>" for c in cards)
    return f'<<TABLE BORDER="0" CELLSPACING="14" CELLPADDING="0">{rows}</TABLE>>'


g = graphviz.Digraph("feedbackfruits_ownership", format="png")
g.attr(
    rankdir="LR", bgcolor="white", pad="0.5", ranksep="0.9",
    label=TITLE, labelloc="t", fontname="Helvetica-Bold", fontsize="22",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", color="#546E7A", penwidth="2.6", arrowsize="1.1",
       fontname="Helvetica-Bold", fontsize="12")

# LEFT: external, FeedbackFruits depends on it
with g.subgraph(name="cluster_ext") as e:
    e.attr(
        label="EXTERNAL: owned by the university\nFeedbackFruits depends on it",
        fontname="Helvetica-Bold", fontsize="15", fontcolor="#6D4C41",
        color="#8D6E63", bgcolor="#FBF6F2", style="dashed", margin="16",
    )
    e.node("ext", column([card(t, l, width=220) for t, l in EXTERNAL]))

# RIGHT: owned by FeedbackFruits
with g.subgraph(name="cluster_own") as o:
    o.attr(
        label="OWNED BY FEEDBACKFRUITS",
        fontname="Helvetica-Bold", fontsize="15", fontcolor="#3B6FB0",
        color="#3B6FB0", bgcolor="#F4F8FF", margin="16",
    )
    o.node("tools", column([card(t, l, gl) for t, l, gl in TOOLS]))
    o.node("data", "<" + card(
        "ANALYTICS ENGINEER",
        ["all tool activity becomes data", "dbt models, dashboards"],
        fill="#FCE4EC", border="#AD1457", width=220,
    ) + ">")

g.edge("ext", "tools", label="  launches tools\n  + logs users in  ")
g.edge("tools", "data", label="  activity data  ")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feedbackfruits_ownership_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
