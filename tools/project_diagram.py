from diagrams.aws.general import InternetAlt1
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.programming.language import Python, R
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.client import Users
import os

# ---------------------------------------------------------------------
# Clean left-to-right spine: Sources -> Ingest -> Postgres schemas -> Dashboard.
# Postgres is organized into stage-named schemas split across the two repos:
#   Canonical (elt-canonical-data):   raw -> cdm
#   Inference (elt-inference-models):  features -> clustering -> scoring
#                                      -> serving, plus validation
#                                      (walk-forward) and monitoring
#                                      (prediction ledger).
#   Qualitative (qualstream):          qual scorecards, one forced-tool Claude
#                                      call per ticker, read straight by the
#                                      dashboard. Decoupled: shared Postgres is
#                                      the only integration.
# Airflow orchestrates; walk-forward validation feeds credibility back into
# the serving rollup.
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
    "feedback": {"color": "#EF6C00", "style": "dashed"},
}

out_name = os.environ.get("DIAGRAM_OUTPUT", "tools/alphastream_system_architecture")

with Diagram("AlphaStream System Architecture", filename=out_name, show=False,
             direction="LR", graph_attr=graph_attr, node_attr=node_attr):

    # Sources (grouped tidily in one box)
    with Cluster("External APIs", graph_attr={"bgcolor": "white", "pencolor": "#BDBDBD", "style": "dashed"}):
        massive = InternetAlt1("Massive Data API")
        yfinance = InternetAlt1("Yahoo Finance API")
        delisted = InternetAlt1("Polygon Delisted\n(Flat Files)")

    # LLM grader (external Anthropic API) — qualstream's only non-Postgres
    # dependency; kept in its own cluster so External APIs stays compact.
    with Cluster("LLM  ·  Anthropic", graph_attr={"bgcolor": "white", "pencolor": "#5E35B1", "style": "dashed"}):
        claude = InternetAlt1("Claude API\n(qual grading)")

    with Cluster("DigitalOcean Production (Docker)", graph_attr={"bgcolor": "#E1F5FE", "pencolor": "#0288D1", "penwidth": "2.0"}):

        # Control plane
        with Cluster("Orchestration", graph_attr={"bgcolor": "#FFFFFF", "style": "dashed", "pencolor": "#455A64"}):
            airflow = Airflow("Airflow")
            ingestor = Python("Ingestion")
            dbt_runner = Dbt("dbt Transforms")

        # Data plane: stage-named schemas, grouped by owning repo
        with Cluster("Managed PostgreSQL", graph_attr={"bgcolor": "#E8EAF6", "pencolor": "#3F51B5", "penwidth": "2.0"}):

            with Cluster("Canonical  |  elt-canonical-data", graph_attr={"bgcolor": "#E3F2FD", "pencolor": "#1E88E5"}):
                raw = DbaasPrimary("raw")
                cdm = DbaasPrimary("cdm")

            with Cluster("Inference  |  elt-inference-models", graph_attr={"bgcolor": "#E8F5E9", "pencolor": "#2E7D32"}):
                features = DbaasPrimary("features")
                clustering = DbaasPrimary("clustering")
                analysis = DbaasPrimary("analysis")   # python-produced clustering intermediate
                scoring = DbaasPrimary("scoring")
                serving = DbaasPrimary("serving")
                monitoring = DbaasPrimary("monitoring")

            # Validation schema (holds the walk-forward backtest output)
            with Cluster("Validation (walk-forward)", graph_attr={"bgcolor": "#FFF3E0", "pencolor": "#EF6C00", "penwidth": "2.0"}):
                validation = DbaasPrimary("validation")

            # Qualitative schema (qualstream's LLM scorecards; decoupled repo)
            with Cluster("Qualitative  |  qualstream", graph_attr={"bgcolor": "#EDE7F6", "pencolor": "#5E35B1", "penwidth": "2.0"}):
                qual = DbaasPrimary("qual")

        # Dashboard app (reads the serving + monitoring schemas)
        with Cluster("Dashboard", graph_attr={"bgcolor": "#F3E5F5", "pencolor": "#7B1FA2", "penwidth": "2.0"}):
            dashboard = R("Shiny Dashboard")

    analysts = Users("Analysts")

    # Ingest
    massive >> Edge(**edge_cfg["ingest"], label="Fetch") >> ingestor
    yfinance >> Edge(**edge_cfg["ingest"]) >> ingestor
    delisted >> Edge(**edge_cfg["ingest"], label="Backfill") >> ingestor
    ingestor >> Edge(**edge_cfg["ingest"], label="Load") >> raw

    # Canonical layer: raw -> cdm
    raw >> Edge(**edge_cfg["transform"], label="Standardize") >> cdm

    # Two PARALLEL branches off the canonical layer, both consumed by scoring:
    #   features    = excess returns + Fibonacci-offset sampling (from cdm/raw)
    #   clustering  = growth ranking + volatility groups (from cdm)
    cdm >> Edge(**edge_cfg["transform"], label="Returns") >> features
    cdm >> Edge(**edge_cfg["transform"], label="Rank") >> clustering

    # clustering <-> analysis loop: a Python step segments the growth ranking
    # into analysis.ticker_cluster_segments, which clustering reads back for
    # its volatility summary.
    clustering >> Edge(**edge_cfg["transform"], label="Segments (py)") >> analysis
    analysis >> Edge(**edge_cfg["transform"]) >> clustering

    # scoring consumes BOTH features and clustering
    features >> Edge(**edge_cfg["transform"], label="Score") >> scoring
    clustering >> Edge(**edge_cfg["transform"]) >> scoring

    # serving rolls up scoring and also joins clustering labels
    scoring >> Edge(**edge_cfg["transform"], label="Roll up") >> serving
    clustering >> Edge(**edge_cfg["transform"]) >> serving

    # monitoring logs the served calls
    serving >> Edge(**edge_cfg["transform"], label="Log calls") >> monitoring

    # Orchestrate
    airflow >> Edge(**edge_cfg["orchestrate"], label="Trigger") >> ingestor
    airflow >> Edge(**edge_cfg["orchestrate"]) >> dbt_runner

    # Validate: scoring is backtested into the validation schema
    scoring >> Edge(**edge_cfg["transform"], label="Backtest") >> validation

    # Qualitative: qualstream resolves cluster tickers, grades each with one
    # forced-tool Claude call, and writes qual.ticker_scorecards. The dashboard
    # reads that table directly (shared Postgres is the only integration).
    analysis >> Edge(**edge_cfg["transform"], label="Tickers") >> qual
    claude >> Edge(**edge_cfg["ingest"], label="LLM grade") >> qual

    # Serve
    serving >> Edge(**edge_cfg["serve"], label="Serve") >> dashboard
    monitoring >> Edge(**edge_cfg["serve"]) >> dashboard
    qual >> Edge(**edge_cfg["serve"], label="Qual grades") >> dashboard
    dashboard >> Edge(**edge_cfg["serve"], label="Explore") >> analysts

    # Feedback: walk-forward credibility gates the serving rollup
    validation >> Edge(**edge_cfg["feedback"], label="Credibility") >> serving
