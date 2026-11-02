"""
DoiT product-analytics stack: click -> decision, with the table layout.
Built with the `diagrams` library (Diagram as Code) for interview prep.

Ownership is split into clusters:
  - Engineering owns the product + the pipe that collects events.
  - I own the modeling: a classic star schema (one shared events fact,
    users + accounts dimensions, metric marts built on top) and the
    metric / insight layer.

Blue edges  = event ingestion (engineering).
Crimson edges = my modeling + insight work.
"""
import os

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.client import Users, Client
from diagrams.onprem.analytics import Dbt
from diagrams.saas.analytics import Snowflake
from diagrams.gcp.analytics import Looker
from diagrams.generic.database import SQL
from diagrams.programming.flowchart import InputOutput, Display, Document

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "doit_stack_diagram")

BLUE = "#1565C0"      # ingestion / engineering
CRIMSON = "#AD1457"   # my work

graph_attr = {
    "fontname": "Helvetica-Bold",
    "fontsize": "20",
    "bgcolor": "white",
    "pad": "0.6",
    "nodesep": "0.55",
    "ranksep": "1.05",
    "splines": "spline",
}
node_attr = {"fontname": "Helvetica", "fontsize": "12"}
edge_attr = {"penwidth": "2.2", "fontname": "Helvetica", "fontsize": "11"}

with Diagram(
    "DoiT product analytics: from a click to a decision",
    filename=OUT,
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    customers = Users("Customers\n(engineers, finance)")
    tracking = Document("Tracking plan\n(event taxonomy)")

    with Cluster("Product + pipeline  —  Engineering"):
        app = Client("DoiT product\n(web app)")
        segment = InputOutput("Segment\n(event pipe)")
        mixpanel = Display("Mixpanel\n(analytics UI)")
        warehouse = Snowflake("Warehouse\n(raw event tables)")

    with Cluster("You own it  —  dbt modeling → insight"):
        dbt = Dbt("dbt models")

        with Cluster("Star schema (fact + dimensions)"):
            events_fact = SQL("events  (FACT)\n1 row / action\nall features, sliced by WHERE")
            users_dim = SQL("users  (dim)\n1 row / user")
            accounts_dim = SQL("accounts  (dim)\n1 row / account")
            marts = SQL("metric marts\nadoption, retention, DAU")

        looker = Looker("Looker\ndashboards + AI")

    team = Users("Product team\n(roadmap decisions)")

    # your tracking plan defines what the app fires
    tracking >> Edge(style="dashed", color=CRIMSON, label="you define") >> app

    # ingestion (blue)
    customers >> Edge(color=BLUE) >> app
    app >> Edge(color=BLUE, label="fires events") >> segment
    segment >> Edge(color=BLUE) >> mixpanel
    segment >> Edge(color=BLUE, label="raw events") >> warehouse

    # your modeling: warehouse -> dbt -> star schema -> marts (crimson)
    warehouse >> Edge(color=CRIMSON) >> dbt
    dbt >> Edge(color=CRIMSON) >> events_fact
    dbt >> Edge(color=CRIMSON) >> users_dim
    dbt >> Edge(color=CRIMSON) >> accounts_dim

    events_fact >> Edge(color=CRIMSON) >> marts
    users_dim >> Edge(color=CRIMSON) >> marts
    accounts_dim >> Edge(color=CRIMSON) >> marts

    # consumption
    marts >> Edge(color=CRIMSON, label="trusted metrics") >> looker
    looker >> Edge(color=CRIMSON) >> team

print(f"wrote {OUT}.png")
