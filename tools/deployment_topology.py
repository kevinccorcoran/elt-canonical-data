"""
Deployment / hosting topology: where the software actually runs.

  Outside:  Operator (SSH/tunnel)   and   external market-data APIs (outbound fetch)
  DigitalOcean VPC (private network):
    - Cloud Firewall at the edge (inbound: SSH 22 + Airflow UI via tunnel, whitelisted IPs)
    - Droplet (Ubuntu, Docker host) running the runtime containers via docker compose:
        airflow-webserver, airflow-scheduler (ingest + dbt), airflow-shiny
    - Managed PostgreSQL, reached over SSL on the private network

Render:  python tools/deployment_topology.py
Output:  tools/deployment_topology.png
"""

import os
from diagrams import Diagram, Cluster, Edge
from diagrams.digitalocean.database import DbaasPrimary
from diagrams.digitalocean.network import Firewall
from diagrams.onprem.workflow import Airflow
from diagrams.programming.language import R
from diagrams.onprem.client import Users
from diagrams.onprem.network import Internet

graph_attr = {
    "fontsize": "22", "bgcolor": "white", "splines": "ortho",
    "nodesep": "0.5", "ranksep": "1.2", "pad": "0.4",
    "pencolor": "#263238", "fontname": "Sans-Serif",
}
node_attr = {"fontsize": "12", "fontname": "Sans-Serif"}

edge_admin = {"color": "#6A1B9A", "penwidth": "2.0"}
edge_db = {"color": "#1565C0", "penwidth": "2.0"}
edge_out = {"color": "#2E7D32", "style": "dashed"}

out = os.environ.get("DIAGRAM_OUTPUT", "deployment_topology")

with Diagram("AlphaStream Deployment Topology", filename=out, show=False,
             direction="LR", graph_attr=graph_attr, node_attr=node_attr):

    operator = Users("Operator\n(whitelisted IP)")
    apis = Internet("External market-data APIs\n(Massive / Yahoo / Polygon)")

    with Cluster("DigitalOcean VPC  ·  private network",
                 graph_attr={"bgcolor": "#E1F5FE", "pencolor": "#0288D1", "penwidth": "2.2"}):

        fw = Firewall("Cloud Firewall\ninbound: SSH 22 + Airflow UI (tunnel)\nwhitelisted IPs only")

        with Cluster("Droplet  ·  Ubuntu, Docker host  (docker compose)",
                     graph_attr={"bgcolor": "#E8F5E9", "pencolor": "#2E7D32", "penwidth": "1.8"}):
            web = Airflow("airflow-webserver\n:8080")
            sched = Airflow("airflow-scheduler\n(ingest + dbt)")
            shiny = R("airflow-shiny\n:3838 / :3839")

        pg = DbaasPrimary("Managed PostgreSQL\n:25060  ·  SSL")

    # Admin traffic enters through the firewall
    operator >> Edge(**edge_admin, label="SSH 22 / UI tunnel") >> fw
    fw >> Edge(**edge_admin) >> web
    fw >> Edge(**edge_admin) >> shiny

    # Containers reach the managed DB over the private network
    web >> Edge(**edge_db) >> pg
    sched >> Edge(**edge_db, label="SSL :25060") >> pg
    shiny >> Edge(**edge_db) >> pg

    # Scheduler fetches from vendors (outbound)
    sched >> Edge(**edge_out, label="outbound fetch") >> apis
