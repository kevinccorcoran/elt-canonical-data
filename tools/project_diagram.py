from diagrams.aws.general import InternetAlt1
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.container import Docker
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.programming.language import Python
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.database import Postgresql

# ---------------------------------------------------------------------
# FINAL POLISH & TERMINOLOGY ALIGNMENT
# ---------------------------------------------------------------------

graph_attr = {
    "fontsize": "24",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "1.2",         # More horizontal space between nodes
    "ranksep": "1.5",         # More vertical space between ranks
    "pad": "1.0",             
    "pencolor": "#263238",
    "fontname": "Sans-Serif"
}

# Smaller nodes = smaller icons = labels don't get covered
node_attr = {
    "width": "1.5",           
    "height": "1.5",
    "fontsize": "13",
    "fontname": "Sans-Serif",
    "fixedsize": "true"
}

edge_cfg = {
    "deploy": {"style": "dotted", "color": "#B0BEC5"},
    "orchestrate": {"style": "dashed", "color": "#D32F2F"},
    "ingest": {"color": "#2E7D32"},
    "transform": {"color": "#1565C0", "style": "bold"},
    "serve": {"color": "#7B1FA2", "penwidth": "3.0"}
}

with Diagram("AlphaStream: Data Pipeline Architecture", show=False, direction="LR", 
             graph_attr=graph_attr, node_attr=node_attr):

    # 1. Global Data Sources (Input)
    with Cluster("External APIs", graph_attr={"bgcolor": "white", "pencolor": "#BDBDBD", "style": "dashed"}):
        massive = InternetAlt1("Massive Data API")
        yfinance = InternetAlt1("Yahoo Finance API")

    # 2. Production Platform
    with Cluster("DigitalOcean Production Environment", graph_attr={"bgcolor": "#E1F5FE", "pencolor": "#0288D1", "penwidth": "2.0"}):
        
        # A. COMPUTE RUNTIME
        with Cluster("Docker Container Layer", graph_attr={"bgcolor": "#FFFFFF", "style": "dashed", "pencolor": "#455A64"}):
             docker_img = Docker("Docker Runtime")
             
             with Cluster("Workloads"):
                 airflow = Airflow("Orchestrator")
                 ingestor = Python("Ingestion Scripts")
                 dbt_runner = Dbt("dbt Transforms")

        # B. DATA STORE (Managed Postgres)
        with Cluster("Data Store (Managed PostgreSQL)", graph_attr={"bgcolor": "#E8EAF6", "pencolor": "#3F51B5", "penwidth": "2.0"}):
            
            # Layer 1: The Raw Landing
            with Cluster("1. Ingestion"):
                raw = DbaasPrimary("Raw Schema")
            
            # Layer 2: THE CANONICAL DATA MODEL (The Foundation)
            with Cluster("2. Canonical Layer"):
                cdm = DbaasPrimary("CDM Schema\n(Canonical Data)")
                
            # Layer 3 & 4: ADVANCED ANALYTICS (The Outcomes)
            with Cluster("3. Prediction Engine"):
                metrics = DbaasPrimary("Metrics Schema")
                inference = DbaasPrimary("Inference Schema")

    # ---------------------------------------------------------
    # DATA VALUE CHAIN

    # Orchestration
    airflow - Edge(**edge_cfg["orchestrate"], label="Trigger") - ingestor
    airflow - Edge(**edge_cfg["orchestrate"], label="Trigger") - dbt_runner

    # Ingestion (Python)
    massive >> Edge(**edge_cfg["ingest"], label="Fetch") >> ingestor
    yfinance >> Edge(**edge_cfg["ingest"]) >> ingestor
    ingestor >> Edge(**edge_cfg["ingest"], label="Load") >> raw
    
    # Transformation (DBT)
    # Visualizing the progression through the layers
    raw >> Edge(**edge_cfg["transform"], label="Standardize") >> cdm
    cdm >> Edge(**edge_cfg["transform"], label="Aggregate") >> metrics
    metrics >> Edge(**edge_cfg["transform"], label="Predict") >> inference
