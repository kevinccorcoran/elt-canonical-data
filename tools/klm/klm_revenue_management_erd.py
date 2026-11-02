import graphviz
import os

# ---------------------------------------------------------------------
# KLM revenue-management data model (ER diagram) + what the role does.
# Tables in three left-to-right zones:
#   Data (Flight, Booking, FareClass) -> Forecast (DemandForecast)
#   -> Optimize (Optimization, Inventory)
# Overlaid: the two places you actually work (numbered callouts) and
# the accuracy feedback loop (actual bookings vs forecast).
# Crow's foot: bar (tee) = the "one" side, crow = the "many" side.
# ---------------------------------------------------------------------

TITLE = (
    "KLM revenue management — data model + where you optimize\n"
    "Data  →  Forecast  →  Optimize      (① and ② are your levers)"
)

BLUE = "#2C5C8A"    # Data zone
AMBER = "#B5651D"   # Forecast zone
GREEN = "#2E7D57"   # Optimize zone
NOTE = "#F4C430"    # callout / sticky

FLIGHT = [("flight_id", "PK"), ("route", ""), ("departure_datetime", ""), ("capacity", "")]
BOOKING = [("booking_id", "PK"), ("flight_id", "FK"), ("fareclass", "FK"),
           ("price_paid", ""), ("book_datetime", "")]
FARECLASS = [("fareclass_id", "PK"), ("code", ""), ("cabin", "")]
DEMAND = [("flight_id", "FK"), ("fareclass", ""), ("expected_demand", "")]
OPT = [("flight_id", "FK"), ("fareclass", ""), ("recommended_price", ""), ("seat_limit", "")]
INV = [("flight_id", "FK"), ("fareclass", ""), ("seats_open", ""), ("current_price", "")]


def entity(name, header, cols, highlight=None):
    rows = [
        f'<TR><TD BGCOLOR="{header}" ALIGN="CENTER" CELLPADDING="7">'
        f'<FONT COLOR="white" POINT-SIZE="14"><B>{name}</B></FONT></TD></TR>'
    ]
    for col, key in cols:
        bg = ' BGCOLOR="#FFF6D6"' if highlight and col in highlight else ""
        if key == "PK":
            txt = f'<B>{col}</B>  <FONT COLOR="#9AA5B1" POINT-SIZE="9">PK</FONT>'
        elif key == "FK":
            txt = f'{col}  <FONT COLOR="#9AA5B1" POINT-SIZE="9">FK</FONT>'
        else:
            txt = f'<FONT COLOR="#3E4C59">{col}</FONT>'
        rows.append(f'<TR><TD ALIGN="LEFT" CELLPADDING="5"{bg}>{txt}</TD></TR>')
    inner = "".join(rows)
    return (
        f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" COLOR="#CBD5E1">'
        f'{inner}</TABLE>>'
    )


def note(text):
    return (
        f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" COLOR="{NOTE}" BGCOLOR="#FFF6D6">'
        f'<TR><TD ALIGN="LEFT" CELLPADDING="8"><FONT COLOR="#5A4600" POINT-SIZE="12">{text}</FONT></TD></TR>'
        f'</TABLE>>'
    )


g = graphviz.Digraph("klm_revenue_management_erd", format="png")
g.attr(
    rankdir="LR", bgcolor="white", pad="0.5", nodesep="0.5", ranksep="1.2",
    splines="spline", dpi="150", label=TITLE, labelloc="t",
    fontname="Helvetica-Bold", fontsize="22",
)
g.attr("node", shape="plaintext", fontname="Helvetica")

ZONE = {"fontname": "Helvetica-Bold", "fontsize": "15", "style": "rounded",
        "penwidth": "1.4", "margin": "16"}

with g.subgraph(name="cluster_data") as c:
    c.attr(label="Data — keep it clean (your dbt/pipeline job)",
           color=BLUE, fontcolor=BLUE, bgcolor="#F3F7FB", **ZONE)
    c.node("Flight", entity("Flight", BLUE, FLIGHT))
    c.node("FareClass", entity("FareClass", BLUE, FARECLASS))
    c.node("Booking", entity("Booking", BLUE, BOOKING))

with g.subgraph(name="cluster_forecast") as c:
    c.attr(label="Forecast", color=AMBER, fontcolor=AMBER, bgcolor="#FBF6EF", **ZONE)
    c.node("DemandForecast", entity("DemandForecast", AMBER, DEMAND, highlight={"expected_demand"}))
    c.node("note1", note("① Make <B>expected_demand</B> accurate.<BR/>"
                         "Compare forecast vs what actually<BR/>booked; fix where it's off."))

with g.subgraph(name="cluster_optimize") as c:
    c.attr(label="Optimize", color=GREEN, fontcolor=GREEN, bgcolor="#F1F8F4", **ZONE)
    c.node("Optimization", entity("Optimization", GREEN, OPT, highlight={"recommended_price", "seat_limit"}))
    c.node("Inventory", entity("Inventory", GREEN, INV))
    c.node("note2", note("② Tune <B>seat_limit</B> &amp; <B>recommended_price</B>.<BR/>"
                         "How many seats to hold for late<BR/>high-fare buyers. Backtest on history,<BR/>ship the version that earns more."))

# Crow's-foot relationships: one (tee) -> many (crow)
REL = {"dir": "both", "arrowtail": "tee", "arrowhead": "crow",
       "color": "#546E7A", "penwidth": "1.5", "arrowsize": "1.2"}
g.edge("Flight", "Booking", **REL)
g.edge("FareClass", "Booking", **REL)
g.edge("Flight", "DemandForecast", **REL)
g.edge("Flight", "Optimization", **REL)
g.edge("Flight", "Inventory", **REL)

# Thick flow arrows: the pipeline direction
FLOW = {"color": "#2E7D57", "penwidth": "2.6", "arrowsize": "1.3", "fontname": "Helvetica-Bold",
        "fontsize": "12", "fontcolor": "#2E7D57"}
g.edge("Booking", "DemandForecast", label="  trains", **FLOW)
g.edge("DemandForecast", "Optimization", label="  feeds", **FLOW)
g.edge("Optimization", "Inventory", label="  sets", **FLOW)

# Feedback loop: actual bookings measure the forecast
g.edge("Inventory", "Booking", style="dashed", color="#B5651D", penwidth="2.0",
       constraint="false", label="  seats sell → new bookings", fontcolor="#B5651D",
       fontname="Helvetica-Bold", fontsize="11", arrowsize="1.1")

# Callouts attach to their boxes
g.edge("note1", "DemandForecast", style="dotted", color=NOTE, penwidth="2.0",
       arrowhead="none", constraint="false")
g.edge("note2", "Optimization", style="dotted", color=NOTE, penwidth="2.0",
       arrowhead="none", constraint="false")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_revenue_management_erd")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
