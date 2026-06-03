from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.client import Users
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.database import PostgreSQL

# ---------------------------------------------------------------------
# Local Airflow container topology after the split.
# Each long-running service is its own container with its own PID 1,
# so Docker's restart policy actually takes effect on crashes.
# ---------------------------------------------------------------------

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "1.2",
    "ranksep": "1.4",
    "pad": "1.0",
    "pencolor": "#263238",
    "fontname": "Sans-Serif",
}

node_attr = {
    "width": "2.0",
    "height": "2.0",
    "fontsize": "12",
    "fontname": "Sans-Serif",
    "fixedsize": "true",
}

depends_healthy   = {"color": "#1565C0", "style": "bold", "label": "depends_on:\nhealthy"}
depends_completed = {"color": "#2E7D32", "style": "bold", "label": "depends_on:\ncompleted"}
serve             = {"color": "#6A1B9A"}
gate              = {"style": "dashed", "color": "#D32F2F"}

with Diagram("Local Airflow — Split Topology",
             show=False, direction="TB",
             graph_attr=graph_attr, node_attr=node_attr,
             filename="airflow_split_topology"):

    operator = Users("Operator\n(localhost)")

    with Cluster("Long-running services\nrestart: unless-stopped",
                 graph_attr={"bgcolor": "#E8F5E9", "pencolor": "#2E7D32", "penwidth": "2.0"}):
        webserver = Airflow("airflow-webserver\n:8080\nhealth: GET /health")
        shiny     = Airflow("airflow-shiny\n:3838  :3839\n(wait -n on R apps)")
        scheduler = Airflow("airflow-scheduler\nhealth:\nairflow jobs check")

    with Cluster("One-shot\nrestart: no",
                 graph_attr={"bgcolor": "#FFF3E0", "pencolor": "#E65100", "penwidth": "2.0"}):
        init = Airflow("airflow-init\nairflow db upgrade\n(exits 0)")

    postgres = PostgreSQL("postgres :5435\nhealth: pg_isready\nbind mount\n./airflow-postgres")

    # Operator interactions
    operator >> Edge(**serve, label="UI") >> webserver
    operator >> Edge(**serve, label="dashboards") >> shiny

    # Service dependency gates
    webserver >> Edge(**depends_completed) >> init
    scheduler >> Edge(**depends_completed) >> init
    init      >> Edge(**depends_healthy)   >> postgres
    webserver >> Edge(**depends_healthy)   >> postgres
    scheduler >> Edge(**depends_healthy)   >> postgres
    shiny     >> Edge(**depends_healthy)   >> postgres

    # Scheduler issues work to webserver (via DB) and is the active workflow engine
    scheduler >> Edge(**gate, label="DAG state\nin metadata DB") >> postgres
