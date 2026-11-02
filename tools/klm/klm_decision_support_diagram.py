"""
KLM Decision Support (Amstelveen) — the system landscape I'd be working in.
Built with the `diagrams` library (Diagram as Code) for interview prep.

One clean left-to-right story:
  feeds (already bought) -> legacy estate + the GCP/Azure rebuild ->
  apps + dashboards -> commercial departments -> a revenue decision.

BLUE    = what already exists (feeds, legacy C++ estate).
CRIMSON = the rebuild I'd own.
AMBER   = consumption: people using it to decide.
"""
import os

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.client import Users
from diagrams.onprem.analytics import Dbt
from diagrams.gcp.analytics import BigQuery, Composer, Looker
from diagrams.gcp.compute import Run
from diagrams.gcp.ml import AIPlatform
from diagrams.programming.framework import React
from diagrams.programming.language import Cpp
from diagrams.generic.database import SQL
from diagrams.programming.flowchart import Document, Decision, Display

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_decision_support_diagram")

BLUE = "#1565C0"
CRIMSON = "#AD1457"
AMBER = "#E65100"

graph_attr = {
    "fontname": "Helvetica-Bold",
    "fontsize": "22",
    "bgcolor": "white",
    "pad": "0.5",
    "nodesep": "0.5",
    "ranksep": "0.9",
    "splines": "ortho",
    "dpi": "140",
}
node_attr = {"fontname": "Helvetica", "fontsize": "12"}
edge_attr = {"penwidth": "2.0", "fontname": "Helvetica", "fontsize": "11"}

CLUSTER = {"fontsize": "14", "fontname": "Helvetica-Bold",
           "bgcolor": "#F8FAFC", "pencolor": "#CBD5E1", "margin": "16"}

with Diagram(
    "KLM Decision Support — market data to a revenue decision",
    filename=OUT,
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    with Cluster("Feeds — already bought", graph_attr=CLUSTER):
        fares = Document("Competitor fares\nATPCO · Infare")
        market = Document("Schedules + bookings\nOAG · Amadeus GDS")
        internal = SQL("KLM internal\nreservations · cargo")

    with Cluster("Legacy estate — still runs the business", graph_attr=CLUSTER):
        legacy = Cpp("In-house RM tools\nC++, back + front end")
        legacy_dash = Display("Existing\ndashboards")

    with Cluster("The rebuild — GCP + Azure (what I'd own)", graph_attr=CLUSTER):
        composer = Composer("Cloud Composer\nscheduled pipelines")
        bq = BigQuery("BigQuery\ncanonical + marts")
        dbt = Dbt("dbt / SQL\nmodels")
        models = AIPlatform("Forecasting +\noptimization")
        api = Run("Cloud Run\nAPIs")

    with Cluster("What people click on", graph_attr=CLUSTER):
        webapp = React("Internal web apps\nReact / Vue on Node")
        dash = Looker("Dashboards")

    with Cluster("Commercial departments", graph_attr=CLUSTER):
        analysts = Users("Pricing · RM ·\nCargo analysts")
        directors = Users("Directors\nAmsterdam + Paris")

    outcome = Decision("Revenue\ndecision")

    # existing world (blue)
    fares >> Edge(color=BLUE) >> composer
    market >> Edge(color=BLUE) >> composer
    internal >> Edge(color=BLUE) >> composer
    internal >> Edge(color=BLUE, style="dashed") >> legacy
    legacy >> Edge(color=BLUE) >> legacy_dash

    # the rebuild (crimson)
    legacy >> Edge(color=CRIMSON, style="dashed", label="rebuild, slice by slice") >> bq
    composer >> Edge(color=CRIMSON) >> bq
    bq >> Edge(color=CRIMSON) >> dbt
    dbt >> Edge(color=CRIMSON) >> models
    models >> Edge(color=CRIMSON) >> api
    api >> Edge(color=CRIMSON) >> webapp
    dbt >> Edge(color=CRIMSON) >> dash

    # consumption (amber)
    webapp >> Edge(color=AMBER) >> analysts
    dash >> Edge(color=AMBER) >> analysts
    dash >> Edge(color=AMBER) >> directors
    legacy_dash >> Edge(color=AMBER, style="dashed") >> directors
    analysts >> Edge(color=AMBER) >> outcome
    directors >> Edge(color=AMBER) >> outcome

print(f"wrote {OUT}.png")
