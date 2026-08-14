"""AlphaStream system architecture -- redesigned component view (v2).

Same infrastructure-with-logos style as project_diagram.py, but designed to be
self-documenting and to lay out cleanly:

  * an explicit EDGE-TYPE LEGEND, so the colour grammar (ingest / transform /
    serve / feedback / orchestrate) is stated, not guessed.
  * validation lives INSIDE the Inference repo (it is an elt-inference-models
    schema), which also keeps the feedback loop local instead of crossing the
    whole board.
  * every hand-off carries a short label; more rank/---node spacing so nothing
    collides.

Writes tools/alphastream_system_architecture_v2.(png). Does not touch
project_diagram.py / alphastream_system_architecture.png.
"""
from diagrams import Cluster, Diagram, Edge
from diagrams.aws.general import InternetAlt1
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.generic.blank import Blank
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.client import Users
from diagrams.onprem.workflow import Airflow
from diagrams.programming.language import Python, R
import os

graph_attr = {
    "fontsize": "26",
    "bgcolor": "white",
    "splines": "spline",
    "nodesep": "0.5",
    "ranksep": "1.05",
    "pad": "0.5",
    "pencolor": "#263238",
    "fontname": "Sans-Serif",
    "newrank": "true",
    "compound": "true",
}

node_attr = {
    "fontsize": "15",
    "fontname": "Sans-Serif",
    "width": "1.4",
    "height": "1.4",
    "fixedsize": "true",
}

edge_attr = {"fontsize": "13", "fontname": "Sans-Serif"}

E = {
    "ingest":      {"color": "#2E7D32"},
    "transform":   {"color": "#1565C0"},
    "serve":       {"color": "#7B1FA2", "penwidth": "2.5"},
    "feedback":    {"color": "#EF6C00", "style": "dashed"},
    "orchestrate": {"color": "#C62828", "style": "dashed"},
}

out_name = os.environ.get("DIAGRAM_OUTPUT", "tools/alphastream_system_architecture_v2")

with Diagram("AlphaStream System Architecture", filename=out_name, show=False,
             direction="LR", graph_attr=graph_attr, node_attr=node_attr,
             edge_attr=edge_attr):

    # ── legend: state the colour grammar up front (compact blank nodes) ──
    with Cluster("How to read the edges", graph_attr={
            "bgcolor": "#FAFAFA", "pencolor": "#BDBDBD", "style": "rounded",
            "fontsize": "15"}):
        _sz = {"width": "0.06", "height": "0.06", "fixedsize": "true"}
        for label, cfg in [("ingest  ·  data in", E["ingest"]),
                           ("transform  ·  dbt models", E["transform"]),
                           ("serve  ·  to dashboard", E["serve"]),
                           ("feedback  ·  credibility gate", E["feedback"]),
                           ("orchestrate  ·  Airflow", E["orchestrate"])]:
            a, b = Blank("", **_sz), Blank("", **_sz)
            a >> Edge(label=label, minlen="2.4", **cfg) >> b

    # ── sources ──
    with Cluster("External APIs", graph_attr={"bgcolor": "white", "pencolor": "#BDBDBD", "style": "dashed"}):
        massive = InternetAlt1("Massive Data API")
        yfinance = InternetAlt1("Yahoo Finance API")
        delisted = InternetAlt1("Polygon Delisted\n(Flat Files)")

    with Cluster("LLM  ·  Anthropic", graph_attr={"bgcolor": "white", "pencolor": "#5E35B1", "style": "dashed"}):
        claude = InternetAlt1("Claude API\n(qual grading)")

    with Cluster("DigitalOcean Production (Docker)", graph_attr={"bgcolor": "#E1F5FE", "pencolor": "#0288D1", "penwidth": "2.0"}):

        with Cluster("Orchestration  ·  control plane", graph_attr={"bgcolor": "#FFFFFF", "style": "dashed", "pencolor": "#455A64"}):
            airflow = Airflow("Airflow")
            ingestor = Python("Ingestion")
            dbt_runner = Dbt("dbt Transforms")

        with Cluster("Managed PostgreSQL  ·  data plane", graph_attr={"bgcolor": "#E8EAF6", "pencolor": "#3F51B5", "penwidth": "2.0"}):

            with Cluster("Canonical  |  elt-canonical-data (public)", graph_attr={"bgcolor": "#E3F2FD", "pencolor": "#1E88E5"}):
                raw = DbaasPrimary("raw")
                cdm = DbaasPrimary("cdm")

            with Cluster("Inference  |  elt-inference-models (private)", graph_attr={"bgcolor": "#E8F5E9", "pencolor": "#2E7D32"}):
                features = DbaasPrimary("features")
                clustering = DbaasPrimary("clustering")
                analysis = DbaasPrimary("analysis")
                scoring = DbaasPrimary("scoring")
                serving = DbaasPrimary("serving")
                monitoring = DbaasPrimary("monitoring")
                validation = DbaasPrimary("validation")

            with Cluster("Qualitative  |  qualstream (decoupled)", graph_attr={"bgcolor": "#EDE7F6", "pencolor": "#5E35B1", "penwidth": "2.0"}):
                qual = DbaasPrimary("qual")

        with Cluster("Dashboard", graph_attr={"bgcolor": "#F3E5F5", "pencolor": "#7B1FA2", "penwidth": "2.0"}):
            dashboard = R("Shiny Dashboard")

    analysts = Users("Analysts")

    # ── ingest ──
    massive >> Edge(**E["ingest"], label="fetch") >> ingestor
    yfinance >> Edge(**E["ingest"]) >> ingestor
    delisted >> Edge(**E["ingest"], label="backfill") >> ingestor
    ingestor >> Edge(**E["ingest"], label="load") >> raw

    # ── canonical ──
    raw >> Edge(**E["transform"], label="standardize") >> cdm

    # ── analysis: two parallel branches off cdm ──
    cdm >> Edge(**E["transform"], label="returns") >> features
    cdm >> Edge(**E["transform"], label="rank") >> clustering
    clustering >> Edge(**E["transform"], label="segments (py)") >> analysis
    analysis >> Edge(**E["transform"]) >> clustering

    # ── scoring & serving ──
    features >> Edge(**E["transform"], label="score") >> scoring
    clustering >> Edge(**E["transform"]) >> scoring
    scoring >> Edge(**E["transform"], label="roll up") >> serving
    clustering >> Edge(**E["transform"]) >> serving
    serving >> Edge(**E["transform"], label="log calls") >> monitoring

    # ── orchestrate ──
    airflow >> Edge(**E["orchestrate"], label="trigger") >> ingestor
    airflow >> Edge(**E["orchestrate"]) >> dbt_runner

    # ── feedback loop (now local to Inference) ──
    scoring >> Edge(**E["transform"], label="backtest") >> validation
    validation >> Edge(**E["feedback"], label="credibility") >> serving

    # ── qualitative sidecar ──
    analysis >> Edge(**E["transform"], label="tickers") >> qual
    claude >> Edge(**E["ingest"], label="LLM grade") >> qual

    # ── serve ──
    serving >> Edge(**E["serve"], label="serve") >> dashboard
    monitoring >> Edge(**E["serve"]) >> dashboard
    qual >> Edge(**E["serve"], label="qual grades") >> dashboard
    dashboard >> Edge(**E["serve"], label="explore") >> analysts
