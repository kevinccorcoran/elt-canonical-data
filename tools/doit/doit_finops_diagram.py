import graphviz
import os

# ---------------------------------------------------------------------
# One-glance cheat sheet for the DoiT HR call.
# Left to right: where the data comes from -> one clean cost dataset ->
# the four things the platform does with it -> what the customer gets.
# Rightmost column also carries company facts and where the Product
# Ops Analyst role fits. Interview-prep reference, not pipeline code.
# ---------------------------------------------------------------------

TITLE = (
    "DoiT Cloud Intelligence  (FinOps)\n"
    "Software that shows companies where their AWS / Google Cloud / Azure "
    "money goes, and cuts the waste.\nSold with DoiT's own cloud consultants "
    "included: the pitch is technology + human expertise."
)


def cell(title, subtitle, fill, border, rows, width=230):
    body = "".join(
        f'<TR><TD ALIGN="LEFT" BALIGN="LEFT" WIDTH="{width}"><B>{name}</B><BR/>'
        f'<FONT POINT-SIZE="10" COLOR="#444444">{gloss}</FONT></TD></TR>'
        for name, gloss in rows
    )
    return (
        f'<<TABLE BORDER="2" COLOR="{border}" CELLBORDER="0" '
        f'CELLSPACING="0" CELLPADDING="7" BGCOLOR="{fill}">'
        f'<TR><TD ALIGN="CENTER" WIDTH="{width}">'
        f'<FONT POINT-SIZE="15"><B>{title}</B></FONT><BR/>'
        f'<FONT POINT-SIZE="10" COLOR="#444444"><I>{subtitle}</I></FONT></TD></TR>'
        f"{body}</TABLE>>"
    )


g = graphviz.Digraph("doit_finops", format="png")
g.attr(
    rankdir="LR",
    bgcolor="white",
    pad="0.4",
    nodesep="0.45",
    ranksep="0.9",
    label=TITLE,
    labelloc="t",
    fontname="Helvetica-Bold",
    fontsize="19",
)
g.attr("node", shape="plaintext", fontname="Helvetica", fontsize="12")
g.attr("edge", color="#607D8B", penwidth="1.8", arrowsize="0.9")

# --- the data flow -----------------------------------------------------
g.node("clouds", cell(
    "1. CUSTOMER'S CLOUDS", "where the money is spent",
    "#ECEFF1", "#546E7A", [
        ("AWS, Google Cloud, Azure", "a company's entire cloud footprint"),
        ("Billing exports", "the itemized bill: millions of line items"),
        ("Usage data + tags", "which team / project / product used what"),
    ]))

g.node("dataset", cell(
    "2. ONE CLEAN COST DATASET", "pulled daily, automatically",
    "#E3F2FD", "#1565C0", [
        ("Normalize", "AWS and GCP costs forced into one standard shape"),
        ("Allocate", "tags map every euro to an owning team or product"),
        ("Split shared costs", "common bills divided fairly across teams"),
    ]))

# --- the four capabilities --------------------------------------------
g.node("see", cell(
    "3a. SEE — analytics", "where the money goes",
    "#E8EAF6", "#3949AB", [
        ("Dashboards + reports", "spend by team, product, cloud service"),
        ("Trends + forecasts", "where the bill is heading next month"),
        ("Unit costs", "cost per customer, cost per feature"),
    ]))

g.node("catch", cell(
    "3b. CATCH — alerts", "problems found same day",
    "#FFEBEE", "#C62828", [
        ("Anomaly detection", "sudden spend spike flagged immediately"),
        ("Budgets", "warns before overspend, not after the invoice"),
    ]))

g.node("fix", cell(
    "3c. FIX — recommendations", "what to shut off or shrink",
    "#E8F5E9", "#2E7D32", [
        ("Rightsizing", "shrink oversized machines"),
        ("Idle resources", "turn off what nobody uses"),
        ("Kubernetes costs", "see inside clusters, normally a black box"),
    ]))

g.node("save", cell(
    "3d. SAVE — Flexsave", "automated discounts",
    "#F3E5F5", "#7B1FA2", [
        ("Commitment discounts", "clouds charge less for promised usage"),
        ("Fully automated", "DoiT manages it and carries the lock-in risk"),
    ]))

# --- the right-hand takeaway column ------------------------------------
g.node("outcome", cell(
    "4. WHAT THE CUSTOMER GETS", "why 4,000+ companies pay",
    "#E0F2F1", "#00695C", [
        ("A lower cloud bill", "waste cut, discounts captured"),
        ("Accountability", "every team sees and owns its own spend"),
        ("Human experts included", "DoiT consultants come with the product"),
    ]))

g.node("company", cell(
    "THE COMPANY", "facts to drop on the call",
    "#FFF8E1", "#F9A825", [
        ("4,000+ customers worldwide", "cloud-native and enterprise"),
        ("Partner of all three clouds", "AWS, Google Cloud, Microsoft Azure"),
        ("Fully remote, global team", "“Do'er” culture: builders, move fast"),
    ]))

g.node("role", cell(
    "WHERE MY ROLE FITS", "Product Ops Analyst = measure the platform itself",
    "#FCE4EC", "#AD1457", [
        ("Instrument the product", "define event tracking for features 3a-3d"),
        ("dbt models + metric definitions", "one trusted meaning for adoption, retention"),
        ("Dashboards + readouts", "show PMs what customers actually use"),
        ("Customer voice", "NPS, churn, support signals fed into the roadmap"),
        ("AI everywhere", "automate the reporting and insight generation"),
    ]))

g.node("ecosystem", cell(
    "THE WIDER ECOSYSTEM", "the platform is one piece of the business",
    "#EFEBE9", "#5D4037", [
        ("Cloud resale (the revenue engine)", "customers buy AWS/GCP spend THROUGH DoiT; "
                                             "DoiT earns a margin, platform comes bundled"),
        ("DoiT Cloud Solve", "the consulting arm: Kubernetes, GenAI, CloudOps "
                             "specializations, migrations, training"),
        ("PerfectScale (acquired 2025)", "Kubernetes optimization product being "
                                         "integrated into the platform"),
    ]))

# --- the call column ---------------------------------------------------
g.node("priorities", cell(
    "CALL PRIORITIES", "30 minutes: spend them in this order",
    "#FFF3E0", "#E65100", [
        ("P1 — must land", "90-sec background ending at the platform; salary "
                           "75-85k euro; why DoiT in one line"),
        ("P2 — should land", "product one-liner from the title above; role fit "
                             "= dbt + data quality + AI daily; my 3 questions"),
        ("P3 — only if asked", "feature detail 3a-3d, ecosystem, Mixpanel gap"),
    ]))

g.node("questions", cell(
    "MY QUESTIONS (pick 3)", "ask in the last 10 minutes",
    "#E1F5FE", "#0277BD", [
        ("Process + assignment", "what comes after this call, is there a case study"),
        ("Why is the role open", "new position or backfill"),
        ("Team + reporting", "how big is Product Ops, who does this report to"),
        ("Salary band for NL", "fair to ask HR directly"),
        ("Decision timeline", "when do you expect to decide"),
    ]))

g.node("tooling", cell(
    "THEIR STACK vs MINE", "say the right column plainly if asked",
    "#F1F8E9", "#33691E", [
        ("SQL — HAVE", "advanced, daily, required by the posting"),
        ("dbt — HAVE", "52 models in production, my strongest card"),
        ("GCP / BigQuery — HAVE", "FLYR, 16M records a day"),
        ("AI tooling — HAVE", "Claude Code daily for development and review"),
        ("Looker — NEW", "I use Looker Studio; different tool, say so myself"),
        ("Mixpanel / Segment — NEW", "haven't used them; event-scale data yes; "
                                     "tools are learnable"),
    ]))

# --- wiring ------------------------------------------------------------
g.edge("clouds", "dataset")
for cap in ("see", "catch", "fix", "save"):
    g.edge("dataset", cap)
    g.edge(cap, "outcome")

# keep 3a-3d stacked top-to-bottom in one column
with g.subgraph() as s:
    s.attr(rank="same")
    for cap in ("see", "catch", "fix", "save"):
        s.node(cap)
g.edge("see", "catch", style="invis", weight="10")
g.edge("catch", "fix", style="invis", weight="10")
g.edge("fix", "save", style="invis", weight="10")

g.edge("catch", "company", style="invis")
g.edge("fix", "role", style="invis")
g.edge("save", "ecosystem", style="invis")

# rightmost column: the call sheet
g.edge("company", "priorities", style="invis")
g.edge("outcome", "questions", style="invis")
g.edge("role", "tooling", style="invis")

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "doit_finops_diagram")
g.render(out, cleanup=True)
print(f"wrote {out}.png")
