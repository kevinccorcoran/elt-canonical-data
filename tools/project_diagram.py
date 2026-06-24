from diagrams.aws.general import InternetAlt1
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.programming.language import Python, R
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.client import Users
import os

# ---------------------------------------------------------------------
# Clean left-to-right spine: Sources -> Ingest -> Postgres layers
# (raw -> cdm -> metrics -> analysis -> inference) -> Dashboard -> Analysts,
# with Airflow orchestrating and a walk-forward backtest validating inference.
# ---------------------------------------------------------------------

graph_attr = {
    "fontsize": "26",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "0.35",
    "ranksep": "0.8",
    "pad": "0.4",
    "pencolor": "#263238",
    "fontname": "Sans-Serif",
    "newrank": "true",
}

node_attr = {
    "fontsize": "15",
    "fontname": "Sans-Serif",
    "width": "1.4",
    "height": "1.4",
    "fixedsize": "true",
}

edge_cfg = {
    "orchestrate": {"style": "dashed", "color": "#D32F2F"},
    "ingest": {"color": "#2E7D32"},
    "transform": {"color": "#1565C0"},
    "serve": {"color": "#7B1FA2", "penwidth": "2.5"},
}

out_name = os.environ.get("DIAGRAM_OUTPUT", "alphastream_system_architecture")

with Diagram("AlphaStream System Architecture", filename=out_name, show=False,
             direction="LR", graph_attr=graph_attr, node_attr=node_attr):

    # Sources (grouped tidily in one box)
    with Cluster("External APIs", graph_attr={"bgcolor": "white", "pencolor": "#BDBDBD", "style": "dashed"}):
        massive = InternetAlt1("Massive Data API")
        yfinance = InternetAlt1("Yahoo Finance API")
        delisted = InternetAlt1("Polygon Delisted\n(Flat Files)")

    with Cluster("DigitalOcean Production (Docker)", graph_attr={"bgcolor": "#E1F5FE", "pencolor": "#0288D1", "penwidth": "2.0"}):

        # Control plane
        with Cluster("Orchestration", graph_attr={"bgcolor": "#FFFFFF", "style": "dashed", "pencolor": "#455A64"}):
            airflow = Airflow("Airflow")
            ingestor = Python("Ingestion")
            dbt_runner = Dbt("dbt Transforms")

        # Data plane (left-to-right layers)
        with Cluster("Managed PostgreSQL", graph_attr={"bgcolor": "#E8EAF6", "pencolor": "#3F51B5", "penwidth": "2.0"}):
            raw = DbaasPrimary("Raw")
            cdm = DbaasPrimary("CDM")
            metrics = DbaasPrimary("Metrics")
            analysis = DbaasPrimary("Analysis\n(Clusters)")
            inference = DbaasPrimary("Inference")

        # Validation
        with Cluster("Validation", graph_attr={"bgcolor": "#FFF3E0", "pencolor": "#EF6C00", "penwidth": "2.0"}):
            backtest = Dbt("Backtest\n(Walk-forward)")

        # Serving
        with Cluster("Serving", graph_attr={"bgcolor": "#F3E5F5", "pencolor": "#7B1FA2", "penwidth": "2.0"}):
            dashboard = R("Shiny Dashboard")

    analysts = Users("Analysts")

    # Ingest
    massive >> Edge(**edge_cfg["ingest"], label="Fetch") >> ingestor
    yfinance >> Edge(**edge_cfg["ingest"]) >> ingestor
    delisted >> Edge(**edge_cfg["ingest"], label="Backfill") >> ingestor
    ingestor >> Edge(**edge_cfg["ingest"], label="Load") >> raw

    # Transform
    raw >> Edge(**edge_cfg["transform"], label="Standardize") >> cdm
    cdm >> Edge(**edge_cfg["transform"], label="Aggregate") >> metrics
    metrics >> Edge(**edge_cfg["transform"], label="Cluster") >> analysis
    analysis >> Edge(**edge_cfg["transform"], label="Predict") >> inference

    # Orchestrate
    airflow >> Edge(**edge_cfg["orchestrate"], label="Trigger") >> ingestor
    airflow >> Edge(**edge_cfg["orchestrate"]) >> dbt_runner

    # Validate
    inference >> Edge(**edge_cfg["transform"], label="Validate") >> backtest

    # Serve
    analysis >> Edge(**edge_cfg["serve"], label="Serve") >> dashboard
    inference >> Edge(**edge_cfg["serve"]) >> dashboard
    backtest >> Edge(**edge_cfg["serve"], label="Credibility") >> dashboard
    dashboard >> Edge(**edge_cfg["serve"], label="Explore") >> analysts
