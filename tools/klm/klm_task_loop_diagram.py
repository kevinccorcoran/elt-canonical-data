"""
KLM Decision Support — the loop every task runs through.
The one mental model to hold in the interview: whatever the request is,
the shape of the work is the same five steps.

Same shape as the FLYR CDM story and as AlphaStream, which is why both
map onto this role without stretching.
"""
import os

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.client import Users
from diagrams.programming.flowchart import (
    Decision,
    InputOutput,
    PredefinedProcess,
    Inspection,
    Display,
)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klm_task_loop_diagram")

CRIMSON = "#AD1457"
AMBER = "#E65100"

graph_attr = {
    "fontname": "Helvetica-Bold",
    "fontsize": "22",
    "bgcolor": "white",
    "pad": "0.5",
    "nodesep": "0.45",
    "ranksep": "0.75",
    "dpi": "140",
}
node_attr = {"fontname": "Helvetica", "fontsize": "12"}
edge_attr = {"penwidth": "2.2", "fontname": "Helvetica", "fontsize": "11"}

CLUSTER = {"fontsize": "14", "fontname": "Helvetica-Bold",
           "bgcolor": "#F8FAFC", "pencolor": "#CBD5E1", "margin": "18"}

with Diagram(
    "Every task, same loop",
    filename=OUT,
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    stakeholder = Users('Analyst or director\n"I can\'t see X"')

    with Cluster("I own all five — no BA or PO in between", graph_attr=CLUSTER):
        decision = Decision("1 · Decision\nwhat call are they\ntrying to make?")
        data = InputOutput("2 · Data\nfind it, clean it,\nland it in the warehouse")
        build = PredefinedProcess("3 · Tool\nsmallest thing\nthat answers it")
        validate = Inspection("4 · Validate\nagainst cases\nthey already know")
        monitor = Display("5 · Monitor\nalert me before\nthey notice")

    outcome = Decision("Better revenue decision\nfares · seats · capacity")

    stakeholder >> Edge(color=AMBER, label="  request") >> decision
    decision >> Edge(color=CRIMSON) >> data
    data >> Edge(color=CRIMSON) >> build
    build >> Edge(color=CRIMSON) >> validate
    validate >> Edge(color=CRIMSON) >> monitor
    monitor >> Edge(color=AMBER) >> outcome

print(f"wrote {OUT}.png")
