from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.network import Internet
from diagrams.onprem.database import Postgresql
from diagrams.onprem.identity import Dex
from diagrams.onprem.compute import Server

# ---------------------------------------------------------------------
# EUROPEAN MEDICINES AGENCY (EMA) — HIGH-LEVEL SYSTEM ARCHITECTURE
# Four layers:
#   1. Actors (EU regulatory network)
#   2. EAM (cross-cutting identity / SSO)
#   3. Domain Systems (EudraVigilance, CTIS, IRIS, EudraGMDP, UPD)
#   4. SPOR Master Data (SMS, PMS, OMS, RMS) — ISO IDMP aligned
# ---------------------------------------------------------------------

graph_attr = {
    "fontsize": "24",
    "bgcolor": "white",
    "splines": "ortho",
    "nodesep": "1.0",
    "ranksep": "1.4",
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

edge_auth    = {"style": "dashed", "color": "#7B1FA2", "label": "auth"}
edge_use     = {"color": "#2E7D32"}
edge_consume = {"color": "#1565C0", "style": "bold", "label": "consume API"}

with Diagram("EMA EU Telematics Architecture", show=False, direction="TB",
             graph_attr=graph_attr, node_attr=node_attr,
             filename="ema_system_architecture"):

    # 1. Actors
    actors = Internet("EU Regulatory\nNetwork Actors\n(EMA / NCAs / Industry)")

    # 2. EAM (cross-cutting)
    eam = Dex("EAM\n(SSO / Roles)")

    # 3. Domain Systems
    with Cluster("Domain Systems",
                 graph_attr={"bgcolor": "#E1F5FE", "pencolor": "#0288D1", "penwidth": "2.0"}):
        ev   = Server("EudraVigilance")
        ctis = Server("CTIS")
        iris = Server("IRIS")
        gmdp = Server("EudraGMDP")
        upd  = Server("UPD")

    # 4. SPOR Master Data
    with Cluster("SPOR — Master Data Layer (ISO IDMP)",
                 graph_attr={"bgcolor": "#E8EAF6", "pencolor": "#3F51B5", "penwidth": "2.0"}):
        sms = Postgresql("SMS\nSubstances")
        pms = Postgresql("PMS\nProducts")
        oms = Postgresql("OMS\nOrganisations")
        rms = Postgresql("RMS\nReferentials")

    # Auth (cross-cutting)
    actors >> Edge(**edge_auth) >> eam

    # Actors use the domain systems (representative edges)
    actors >> Edge(**edge_use) >> ev
    actors >> Edge(**edge_use) >> ctis
    actors >> Edge(**edge_use) >> iris

    # Domain systems consume SPOR (one representative edge per system)
    ev   >> Edge(**edge_consume) >> pms
    ctis >> Edge(**edge_consume) >> oms
    iris >> Edge(**edge_consume) >> pms
    gmdp >> Edge(**edge_consume) >> oms
    upd  >> Edge(**edge_consume) >> sms
