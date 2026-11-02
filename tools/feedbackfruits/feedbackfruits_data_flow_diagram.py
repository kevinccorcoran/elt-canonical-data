import graphviz
import os

# ---------------------------------------------------------------------
# FeedbackFruits data flow: the LMS logs users in and gets grades back;
# all activity is recorded in FeedbackFruits' own app; the Analytics
# Engineer (pink) turns it into datalake -> dbt -> outputs.
# Conceptual reconstruction from public pages, not a live schema.
# ---------------------------------------------------------------------

TITLE = "FeedbackFruits data flow\nPink = the Analytics Engineer job"

EXT = ("#FBF6F2", "#8D6E63")
APP = ("#ECEFF1", "#546E7A")
AE = ("#FCE4EC", "#AD1457")


def cell(title, gloss, colors, width=190):
    fill, border = colors
    return (
        f'<<TABLE BORDER="3" COLOR="{border}" CELLBORDER="0" CELLSPACING="0" '
        f'CELLPADDING="10" BGCOLOR="{fill}"><TR><TD WIDTH="{width}">'
        f'<FONT POINT-SIZE="15"><B>{title}</B></FONT><BR/><BR/>'
        f'<FONT POINT-SIZE="11" COLOR="#444444">{gloss}</FONT></TD></TR></TABLE>>'
    )


g = graphviz.Digraph("feedbackfruits_data_flow", format="png")
g.attr(
    rankdir="LR", bgcolor="white", pad="0.5", nodesep="0.4", ranksep="0.7",
    label=TITLE, labelloc="t", fontname="Helvetica-Bold", fontsize="22",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", color="#546E7A", penwidth="2.4", arrowsize="1.0",
       fontname="Helvetica-Bold", fontsize="11")

g.node("lms", cell("LMS", "Canvas etc.<BR/>external", EXT, width=150))
g.node("app", cell("FEEDBACKFRUITS APP", "students do the activity", APP))
g.node("db", cell("APP DATABASE", "submissions, ratings, answers", APP))
g.node("mix", cell("MIXPANEL", "clicks, views, completions", APP))
g.node("lake", cell("DATALAKE", "everything lands here", AE))
g.node("dbt", cell("dbt MODELS", "clean and join:<BR/>institution, course, user", AE))
g.node("out", cell("OUTPUTS", "dashboards · data API<BR/>in-product analytics · MCP", AE, width=210))

g.edge("lms", "app", label=" login ")
g.edge("app", "db")
g.edge("app", "mix")
g.edge("db", "lake")
g.edge("mix", "lake")
g.edge("lake", "dbt")
g.edge("dbt", "out")
g.edge("app", "lms", label=" grades back ", style="dashed", constraint="false")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feedbackfruits_data_flow_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
