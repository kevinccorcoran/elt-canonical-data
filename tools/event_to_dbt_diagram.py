import graphviz
import os

# ---------------------------------------------------------------------
# DoiT's product-analytics pipeline top to bottom, and next to every
# stage: what the Product Ops Analyst actually does there.
# Left lane  (blue)  = what happens mechanically
# Right lane (pink)  = my job at that stage
# Interview-prep reference, not pipeline code.
# ---------------------------------------------------------------------

TITLE = (
    "The product-analytics pipeline, and my job at each stage\n"
    "Left: what happens.  Right: what the Product Analyst does."
)

W = 300


def cell(title, fill, border, rows, width=W, tsize="15", white=False):
    tcolor = ' COLOR="white"' if white else ""
    gcolor = "#DDDDDD" if white else "#444444"
    body = "".join(
        f'<TR><TD ALIGN="LEFT" BALIGN="LEFT" WIDTH="{width}">'
        f'<FONT POINT-SIZE="12"{tcolor}><B>{name}</B></FONT><BR/>'
        f'<FONT POINT-SIZE="11" COLOR="{gcolor}">{gloss}</FONT></TD></TR>'
        for name, gloss in rows
    )
    return (
        f'<<TABLE BORDER="2" COLOR="{border}" CELLBORDER="0" '
        f'CELLSPACING="0" CELLPADDING="8" BGCOLOR="{fill}">'
        f'<TR><TD ALIGN="CENTER" WIDTH="{width}">'
        f'<FONT POINT-SIZE="{tsize}"{tcolor}><B>{title}</B></FONT></TD></TR>'
        f"{body}</TABLE>>"
    )


g = graphviz.Digraph("event_to_dbt", format="png")
g.attr(
    rankdir="TB",
    bgcolor="white",
    pad="0.5",
    nodesep="0.9",
    ranksep="0.5",
    label=TITLE,
    labelloc="t",
    fontname="Helvetica-Bold",
    fontsize="21",
)
g.attr("node", shape="plaintext", fontname="Helvetica")
g.attr("edge", penwidth="2.2", arrowsize="1.0")

PIPE_FILL, PIPE_BORDER = "#E3F2FD", "#1565C0"
JOB_FILL, JOB_BORDER = "#FCE4EC", "#AD1457"

g.node("p_head", cell("THE PIPELINE", PIPE_BORDER, PIPE_BORDER,
                      [("What happens to the data", "top to bottom")],
                      tsize="17", white=True))
g.node("j_head", cell("MY JOB AT THIS STAGE", JOB_BORDER, JOB_BORDER,
                      [("Product Ops Analyst", "what I own or contribute")],
                      tsize="17", white=True))

# key, pipeline(title, rows), job rows
STAGES = [
    ("source", "1. SOURCE — events fire",
     [("User acts in the product", "opens a dashboard, sets a budget alert"),
      ("Tracking code fires an event", "dashboard_viewed + user, account, time")],
     [("Define WHAT to track", "event taxonomy: names, required properties"),
      ("Partner with engineering", "new feature = tracking spec before launch"),
      ("TOOLS + PROCESS", "tracking plan in Segment Protocols (or shared doc); "
                          "spec reviewed at feature kickoff, before code ships")]),
    ("collect", "2. COLLECT — Segment streams",
     [("Segment / Mixpanel SDK", "catches every event and streams it"),
      ("Lands in BigQuery", "continuous, no manual step")],
     [("Watch tracking quality", "data arriving? gaps after a release?"),
      ("TOOLS + PROCESS", "Segment source debugger + delivery monitors; "
                          "post-release check is a standing step, not ad hoc")]),
    ("raw", "3. RAW TABLES",
     [("One row per event", "exactly as fired, never edited")],
     [("Audit against the spec", "do fired events match the taxonomy?"),
      ("TOOLS + PROCESS", "SQL in BigQuery + dbt source freshness checks; "
                          "scheduled, alert on staleness")]),
    ("staging", "4. dbt STAGING",
     [("Rename, cast, dedupe", "one consistent shape for all events")],
     [("Build the staging models", "one staging model per event source"),
      ("Write quality tests", "broken tracking caught here, automatically"),
      ("TOOLS + PROCESS", "dbt + dbt tests (unique, not_null, accepted_values); "
                          "changes via Git PR, tests run in CI")]),
    ("entities", "5. dbt ENTITIES",
     [("Users, accounts, sessions", "events stitched into things"),
      ("Feature usage per day", "one row per account per feature")],
     [("Model the entities", "usage rolled up: user, account, feature, day"),
      ("TOOLS + PROCESS", "dbt marts + dbt docs for lineage and definitions; "
                          "every model documented as it merges")]),
    ("metrics", "6. METRIC LAYER",
     [("Active users, stickiness", "per account, per day/month"),
      ("Adoption", "per account, per feature"),
      ("Retention, churn-warning, expansion", "per account cohort, per month"),
      ("Time-to-value", "per account, once"),
      ("Alerts + recommendations acted on", "per account, per event"),
      ("Savings delivered", "per account, per month"),
      ("Usage vs renewal", "per account; the one execs care about"),
      ("Roll-ups", "platform, segment, region")],
     [("Define each metric ONCE", "written definition, stakeholders agree"),
      ("TOOLS + PROCESS", "dbt metric models + a metrics glossary page; "
                          "new metric = written definition + sign-off, then built")]),
    ("consume", "7. CONSUME — decisions",
     [("Dashboards, readouts, AI insights", "product teams and executives")],
     [("Self-service dashboards", "PMs answer their own questions"),
      ("Recurring readouts", "adoption, engagement, retention + a recommendation"),
      ("Connect usage to money", "does feature use predict retention/revenue?"),
      ("TOOLS + PROCESS", "Looker on the metric layer; monthly readout cadence; "
                          "AI (Claude) drafts insights, I verify before it ships")]),
]

for key, title, p_rows, j_rows in STAGES:
    g.node(f"p_{key}", cell(title, PIPE_FILL, PIPE_BORDER, p_rows))
    g.node(f"j_{key}", cell("MY JOB", JOB_FILL, JOB_BORDER, j_rows, tsize="13"))
    with g.subgraph() as s:
        s.attr(rank="same")
        s.node(f"p_{key}")
        s.node(f"j_{key}")
    g.edge(f"p_{key}", f"j_{key}", color="#AD1457", style="dashed",
           penwidth="1.6", arrowsize="0.8", constraint="false")

with g.subgraph() as s:
    s.attr(rank="same")
    s.node("p_head")
    s.node("j_head")

g.edge("p_head", "p_source", color="#1565C0")
g.edge("j_head", "j_source", style="invis")
for (a, *_), (b, *_) in zip(STAGES, STAGES[1:]):
    g.edge(f"p_{a}", f"p_{b}", color="#1565C0")
    g.edge(f"j_{a}", f"j_{b}", style="invis")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "event_to_dbt_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
