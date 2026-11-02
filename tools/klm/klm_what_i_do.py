import graphviz
import os

# ---------------------------------------------------------------------
# "What would I mostly do, how, and where?" -> the optimization loop as
# an ERD-style flow. Each box: action + HOW (tools) + WHERE (system/data).
# Loop = ~70% of the week, context = ~30%.
# ---------------------------------------------------------------------

TITLE = (
    "What I'd do — how &amp; where — the optimization loop\n"
    "Data → Forecast → Optimize      (loop ≈ 70% of the week)"
)

BLUE = "#2C5C8A"
AMBER = "#B5651D"
GREEN = "#2E7D57"
PURPLE = "#5A4FCF"
GREY = "#8A94A6"


g = graphviz.Digraph("klm_what_i_do", format="png")
g.attr(rankdir="LR", bgcolor="white", pad="0.5", nodesep="0.55", ranksep="0.9",
       splines="spline", dpi="150", label=TITLE, labelloc="t",
       fontname="Helvetica-Bold", fontsize="21")
g.attr("node", shape="plaintext", fontname="Helvetica")


def box(title, color, share, body, how, where):
    return (
        f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" COLOR="#CBD5E1" WIDTH="230">'
        # header
        f'<TR><TD BGCOLOR="{color}" CELLPADDING="8" ALIGN="CENTER">'
        f'<FONT COLOR="white" POINT-SIZE="14"><B>{title}</B></FONT>'
        f'<FONT COLOR="white" POINT-SIZE="12">   {share}</FONT></TD></TR>'
        # what
        f'<TR><TD CELLPADDING="8" ALIGN="LEFT" BGCOLOR="white">'
        f'<FONT COLOR="#3E4C59" POINT-SIZE="12">{body}</FONT></TD></TR>'
        # how
        f'<TR><TD CELLPADDING="7" ALIGN="LEFT" BGCOLOR="#F3F7FB">'
        f'<FONT COLOR="#2C5C8A" POINT-SIZE="10.5"><B>HOW  </B>{how}</FONT></TD></TR>'
        # where
        f'<TR><TD CELLPADDING="7" ALIGN="LEFT" BGCOLOR="#FBF6EF">'
        f'<FONT COLOR="#8A6D00" POINT-SIZE="10.5"><B>WHERE  </B>{where}</FONT></TD></TR>'
        f'</TABLE>>'
    )


g.node("m", box("MEASURE", BLUE, "40%",
                "Find where the forecast is<BR/>wrong. Data QA on inputs:<BR/>clean, reconcile, monitor.",
                "SQL queries forecast vs<BR/>actuals; dbt-style data tests.",
                "booking + flight tables &amp;<BR/>forecast tables (DSS warehouse)."))
g.node("p", box("PROPOSE", AMBER, "· 20% ·",
                "Change one rule:<BR/>hold back more/fewer seats,<BR/>or shift a price.",
                "edit a parameter / rule;<BR/>pair with the OR team.",
                "optimization config /<BR/>rule layer."))
g.node("b", box("BACKTEST", GREEN, "· 20% ·",
                "Replay history.<BR/>Old rule vs new —<BR/>which earns more?",
                "Python replay over historical<BR/>bookings; compare revenue.",
                "backtest sandbox against<BR/>historical dataset."))
g.node("s", box("SHIP", PURPLE, "10%",
                "Push the winner live.<BR/>Monitor revenue.<BR/>Then measure again.",
                "Git / CI deploy;<BR/>build a monitoring dashboard.",
                "production DSS +<BR/>monitoring / alerts."))

FLOW = {"color": "#2E7D57", "penwidth": "2.6", "arrowsize": "1.25"}
g.edge("m", "p", **FLOW)
g.edge("p", "b", **FLOW)
g.edge("b", "s", **FLOW)
g.edge("s", "m", color="#546E7A", penwidth="2.0", style="dashed",
       constraint="false", label="  repeat", fontname="Helvetica-Bold",
       fontcolor="#546E7A", fontsize="12", arrowsize="1.1")

g.node("ctx", box("CONTEXT — feeds the loop", GREY, "30%",
                  "Understand the system.<BR/>Stakeholders: translate,<BR/>present, defend.",
                  "read code + trace lineage;<BR/>meetings, demos.",
                  "codebase &amp; docs;<BR/>stakeholders AMS ↔ Paris."))
g.edge("ctx", "m", style="dotted", color=GREY, penwidth="2.0",
       arrowhead="normal", arrowsize="1.0", label="  supports", fontcolor=GREY,
       fontname="Helvetica", fontsize="11", constraint="false")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_what_i_do")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
