import graphviz
import os

# ---------------------------------------------------------------------
# What the role concretely does, abstracted to its essence.
# Top row: the loop you repeat.   MEASURE -> PROPOSE -> BACKTEST -> SHIP
# Bottom: one concrete worked example so "optimize" stops being abstract.
# ---------------------------------------------------------------------

TITLE = (
    "What you'd actually do — the optimization loop\n"
    "Optimize = choose how many seats to hold back, and at what price"
)

BLUE = "#2C5C8A"
GREEN = "#2E7D57"
AMBER = "#B5651D"
NOTE = "#F4C430"

g = graphviz.Digraph("klm_optimization_loop", format="png")
g.attr(rankdir="LR", bgcolor="white", pad="0.5", nodesep="0.6", ranksep="0.9",
       splines="spline", dpi="150", label=TITLE, labelloc="t",
       fontname="Helvetica-Bold", fontsize="22")
g.attr("node", shape="plaintext", fontname="Helvetica")


def step(num, title, body, color):
    return (
        f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" COLOR="#CBD5E1">'
        f'<TR><TD BGCOLOR="{color}" CELLPADDING="8" ALIGN="CENTER">'
        f'<FONT COLOR="white" POINT-SIZE="15"><B>{num}. {title}</B></FONT></TD></TR>'
        f'<TR><TD CELLPADDING="9" ALIGN="LEFT">'
        f'<FONT COLOR="#3E4C59" POINT-SIZE="12">{body}</FONT></TD></TR>'
        f'</TABLE>>'
    )


def card(title, body, color):
    return (
        f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" COLOR="{color}" BGCOLOR="#FFFDF4">'
        f'<TR><TD BGCOLOR="{color}" CELLPADDING="8" ALIGN="CENTER">'
        f'<FONT COLOR="white" POINT-SIZE="14"><B>{title}</B></FONT></TD></TR>'
        f'<TR><TD CELLPADDING="11" ALIGN="LEFT">'
        f'<FONT COLOR="#3E4C59" POINT-SIZE="13">{body}</FONT></TD></TR>'
        f'</TABLE>>'
    )


# ---- the abstract loop -------------------------------------------------
with g.subgraph() as s:
    s.attr(rank="same")
    s.node("m", step("1", "MEASURE",
                     "Where is the forecast wrong?<BR/>"
                     "Compare <B>expected_demand</B><BR/>to what actually booked.", BLUE))
    s.node("p", step("2", "PROPOSE",
                     "Change one rule:<BR/>hold back more/fewer seats,<BR/>"
                     "or shift a <B>price</B>.", AMBER))
    s.node("b", step("3", "BACKTEST",
                     "Replay last year's bookings.<BR/>"
                     "Old rule vs new rule —<BR/><B>which earns more?</B>", GREEN))
    s.node("s", step("4", "SHIP",
                     "Push the winner live.<BR/>Watch revenue.<BR/>Then measure again.", "#5A4FCF"))

g.edge("m", "p", color="#546E7A", penwidth="2.4", arrowsize="1.2")
g.edge("p", "b", color="#546E7A", penwidth="2.4", arrowsize="1.2")
g.edge("b", "s", color="#546E7A", penwidth="2.4", arrowsize="1.2")
g.edge("s", "m", color="#546E7A", penwidth="2.0", style="dashed",
       constraint="false", label="  repeat", fontname="Helvetica-Bold",
       fontcolor="#546E7A", fontsize="12", arrowsize="1.1")

# ---- the concrete worked example --------------------------------------
g.node("ex", card(
    "Concrete example — one flight: AMS → JFK, 100 seats",
    "<B>Forecast says:</B> ~25 last-minute business travelers who pay €800,<BR/>"
    "plus plenty of early leisure demand at €250.<BR/><BR/>"
    "<B>The only real question optimization answers:</B><BR/>"
    "how many of the 100 seats do I <B>hold back</B> for the €800 buyers,<BR/>"
    "instead of selling them early at €250?<BR/><BR/>"
    "<B>You test it:</B> protect 20 seats vs protect 25 → replay history →<BR/>"
    "protect 25 earned <B>+€1,900 per flight</B>. So set <B>seat_limit = 25</B>.<BR/>"
    "<FONT COLOR='#8A6D00'>That is the whole of “optimization”: sell cheap now, "
    "or wait for someone who pays more.</FONT>",
    GREEN))

# place the example under the loop
g.edge("b", "ex", style="dotted", color=NOTE, penwidth="2.2", arrowhead="none",
       constraint="false")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_optimization_loop")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
