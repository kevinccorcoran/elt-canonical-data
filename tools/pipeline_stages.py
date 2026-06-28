from diagrams import Diagram, Edge
from diagrams.aws.general import InternetAlt1
from diagrams.programming.language import Python, R
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.onprem.analytics import Dbt
import os

# ---------------------------------------------------------------------
# Very high-level view: every pipeline stage as one clean left-to-right flow.
# ---------------------------------------------------------------------

graph_attr = {
    "fontsize": "30",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "0.6",
    "ranksep": "1.0",
    "pad": "0.5",
    "fontname": "Sans-Serif",
}

node_attr = {
    "fontsize": "16",
    "fontname": "Sans-Serif",
    "width": "1.5",
    "height": "1.5",
    "fixedsize": "true",
}

out_name = os.environ.get("DIAGRAM_OUTPUT", "pipeline_stages")

with Diagram("AlphaStream Pipeline", filename=out_name, show=False, direction="LR",
             graph_attr=graph_attr, node_attr=node_attr):

    sources = InternetAlt1("Sources")
    ingest = Python("Ingest")
    raw = DbaasPrimary("Raw")
    canonical = DbaasPrimary("Canonical")
    metrics = DbaasPrimary("Metrics")
    clustering = DbaasPrimary("Clustering")
    forecast = DbaasPrimary("Forecast")
    backtest = Dbt("Backtest")
    dashboard = R("Dashboard")

    # Main pipeline spine
    sources >> ingest >> raw >> canonical >> metrics >> clustering >> forecast >> dashboard

    # Validation branch off the forecast stage
    forecast >> Edge(style="dashed", color="#EF6C00", label="validate") >> backtest
