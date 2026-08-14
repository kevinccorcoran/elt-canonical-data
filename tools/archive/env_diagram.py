from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.container import Docker
from diagrams.onprem.database import Postgresql
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.onprem.compute import Server

graph_attr = {
    "fontsize": "24",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "1.2",
    "ranksep": "1.5",
    "pad": "1.0",
    "pencolor": "#263238",
    "fontname": "Sans-Serif"
}

node_attr = {
    "width": "1.5",           
    "height": "1.5",
    "fontsize": "13",
    "fontname": "Sans-Serif",
    "fixedsize": "true"
}

with Diagram("Environment Architecture", show=False, direction="LR", graph_attr=graph_attr, node_attr=node_attr, filename="tools/environment_architecture"):

    with Cluster("Local Environment (MacBook)", graph_attr={"bgcolor": "#F5F5F5", "pencolor": "#9E9E9E"}):
        local_runtime = Docker("Local Docker Runtime\n(Airflow / dbt / Python)")
        
        with Cluster("Local PostgreSQL"):
            dev_db = Postgresql("Dev DB")
            staging_local_db = Postgresql("Staging DB")
            
        local_runtime >> Edge(color="#1565C0", style="bold", label="dev mode") >> dev_db
        local_runtime >> Edge(color="#F57C00", style="bold", label="staging mode") >> staging_local_db

    with Cluster("Cloud Environment (DigitalOcean Staging)", graph_attr={"bgcolor": "#E1F5FE", "pencolor": "#0288D1"}):
        cloud_runtime = Docker("Cloud Docker Runtime\n(Airflow / dbt / Python)")
        
        with Cluster("Managed Database"):
            prod_db = DbaasPrimary("Prod DB\n(Managed Postgres)")
            
        cloud_runtime >> Edge(color="#D32F2F", style="bold", label="prod mode") >> prod_db
