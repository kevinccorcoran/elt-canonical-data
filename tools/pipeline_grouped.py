import graphviz
import os

# ---------------------------------------------------------------------
# Mid-level "grouped by stage" view: flat color boxes, left-to-right.
# Each box = stage title + the real Postgres SCHEMA it lands in + a
# one-line action + owning repo + concepts (quant terms) with a short
# plain-English gloss.
#
# Schemas (verified against the dbt manifests in both repos):
#   elt-canonical-data    raw -> quality -> cdm
#   elt-inference-models  features + clustering (both off cdm, in parallel)
#                         -> scoring -> serving,  plus validation (walk-forward)
#                         and monitoring (prediction ledger).
#   qualstream            qual = LLM scorecards; grades the clustered tickers
#                         (one Claude call each) and feeds serving. Decoupled
#                         repo, shares only Postgres.
#   analysis  = python-produced clustering intermediate (folded into CLUSTERING)
#   monitoring = served-call ledger (folded into SERVING)
# ---------------------------------------------------------------------

# key, title, schema, fill, border, repo, action, [(concept, plain-english gloss)]
STAGES = [
    ("sources", "SOURCES", "", "#ECEFF1", "#546E7A", "external feeds", "Pull market data", [
        ("Primary price feed", "main daily price source"),
        ("Secondary price feed", "backup price source"),
        ("Delisted-universe feed", "prices for removed companies"),
    ]),
    ("raw", "RAW", "raw", "#E3F2FD", "#1E88E5", "elt-canonical-data", "Land it untouched", [
        ("Live price bars", "saved exactly as received"),
        ("Fallback price bars", "backup prices, unedited"),
        ("Delisted price bars", "delisted prices, unedited"),
    ]),
    ("qa", "DATA VALIDATION (QA)", "quality", "#FFF8E1", "#F9A825", "elt-canonical-data", "Screen out bad data", [
        ("Cross-feed agreement checks", "do the two sources match?"),
        ("Symbol-reuse and regime breaks", "reused ticker or sudden jump"),
        ("Outlier and stale-data checks", "impossible or frozen values"),
        ("Manual exclusion overrides", "hand-picked drops"),
        ("Tradeable-universe allowlist", "final list of valid tickers"),
    ]),
    ("canonical", "CANONICAL (CDM)", "cdm", "#E8EAF6", "#3949AB", "elt-canonical-data", "Clean and unify", [
        ("Single source of truth", "one clean table for all"),
        ("Conformed primary feed", "main feed, standard shape"),
        ("Conformed secondary feed", "backup feed, standard shape"),
    ]),
    ("features", "FEATURES", "features", "#E8F5E9", "#2E7D32", "elt-inference-models", "Measure return", [
        ("Benchmark-relative return (alpha)", "return above the market"),
        ("Multi-horizon return sampling", "returns over many spans"),
        ("Feature matrix", "the model inputs"),
    ]),
    ("clustering", "CLUSTERING", "clustering + analysis", "#E0F2F1", "#00897B", "elt-inference-models", "Group similar stocks", [
        ("Cross-sectional growth ranking", "ranked against peers"),
        ("Volatility profiling", "how much it swings"),
        ("Unsupervised grouping", "find look-alike stocks"),
        ("Cluster assignment", "tag each with its group"),
        ("Segment analysis (py)", "the analysis intermediate"),
    ]),
    ("scoring", "SCORING", "scoring", "#F3E5F5", "#8E24AA", "elt-inference-models", "Forecast direction", [
        ("State-transition probabilities", "odds of each move"),
        ("Scored transitions", "moves rated by strength"),
        ("Directional return forecast", "predicted up or down"),
        ("Per-ticker signal summary", "one verdict per stock"),
        ("Buy / sell recommendations", "final calls"),
    ]),
    ("qual", "QUALITATIVE GRADING", "qual", "#EDE7F6", "#5E35B1", "qualstream", "Grade each pick (LLM)", [
        ("Business quality  ·  25%", "core weight of the score"),
        ("Moat  ·  20%", "durability of the advantage"),
        ("Valuation  ·  20%", "price vs intrinsic worth"),
        ("Management  ·  15%", "quality of stewardship"),
        ("Industry structure  ·  10%", "the operating environment"),
        ("Investor lenses  ·  10%", "avg of Buffett, Graham, Fisher, Lynch, Klarman"),
        ("Survival gate", "caps overall if score &lt; 40"),
        ("Veto", "any red flag caps overall at 25"),
        ("Overall = weighted sum  ·  0-100", "deterministic in code, backtestable"),
    ]),
    ("validation", "MODEL VALIDATION", "validation", "#FFF3E0", "#EF6C00", "elt-inference-models", "Backtest on history", [
        ("Walk-forward out-of-sample test", "tested on unseen data"),
        ("Directional accuracy (sign agreement)", "how often direction was right"),
        ("Payoff backtest", "simulated profit and loss"),
        ("Reliability weighting", "how much to trust it"),
    ]),
    ("serving", "SERVING", "serving + monitoring", "#FCE4EC", "#C2185B", "elt-inference-models", "Show on a dashboard", [
        ("Interactive dashboard", "Shiny app (canonical repo)"),
        ("Forecast and cluster views", "predictions and groups"),
        ("Coverage and quality views", "data health"),
        ("Prediction ledger", "monitoring: served calls logged"),
    ]),
]


# Stages that are machine learning / modeling, with the badge text.
ML_TAGS = {
    "clustering": "MACHINE LEARNING  ·  unsupervised",
    "scoring": "MACHINE LEARNING  ·  forecasting model",
    "qual": "LLM  ·  qualitative grader",
}


def html_label(title, schema, action, repo, items, color, ml_tag=""):
    rows = ""
    for concept, gloss in items:
        rows += (
            f'<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="11" COLOR="#37474F">{concept}</FONT></TD></TR>'
            f'<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="9" COLOR="#90A4AE"><I>{gloss}</I></FONT></TD></TR>'
        )
    schema_row = ""
    if schema:
        schema_row = (
            f'<TR><TD ALIGN="CENTER"><FONT POINT-SIZE="11" FACE="Courier New" COLOR="{color}">'
            f'schema: {schema}</FONT></TD></TR>'
        )
    ml_row = ""
    if ml_tag:
        ml_row = (
            '<TR><TD ALIGN="CENTER"><TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0">'
            f'<TR><TD BGCOLOR="{color}"><FONT POINT-SIZE="9" COLOR="white"><B>'
            f'&#160;&#160;{ml_tag}&#160;&#160;</B></FONT></TD></TR></TABLE></TD></TR>'
        )
    return (
        '<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1" CELLPADDING="3">'
        f'<TR><TD ALIGN="CENTER"><B><FONT POINT-SIZE="15" COLOR="{color}">{title}</FONT></B></TD></TR>'
        f'{schema_row}'
        f'{ml_row}'
        f'<TR><TD ALIGN="CENTER"><FONT POINT-SIZE="11" COLOR="#455A64"><I>{action}</I></FONT></TD></TR>'
        f'<TR><TD ALIGN="CENTER"><FONT POINT-SIZE="9" COLOR="#B0BEC5"><I>{repo}</I></FONT></TD></TR>'
        '<HR/>'
        f'{rows}'
        '</TABLE>>'
    )


out_name = os.environ.get("DIAGRAM_OUTPUT", "pipeline_grouped")

g = graphviz.Digraph("AlphaStream Pipeline (grouped by stage)", format="png")
g.attr(rankdir="LR", bgcolor="white", pad="0.4", nodesep="0.5", ranksep="1.0",
       fontname="Sans-Serif", labelloc="b", fontsize="20",
       label="AlphaStream Pipeline")
g.attr("node", shape="box", style="rounded,filled", fontname="Sans-Serif", penwidth="2")
g.attr("edge", color="#37474F", penwidth="2", fontname="Sans-Serif", fontsize="11")

for key, title, schema, fill, border, repo, action, items in STAGES:
    g.node(key, label=html_label(title, schema, action, repo, items, border,
                                 ml_tag=ML_TAGS.get(key, "")),
           fillcolor=fill, color=border)

# Canonical spine
for a, b in [("sources", "raw"), ("raw", "qa"), ("qa", "canonical")]:
    g.edge(a, b)

# cdm fans out to TWO parallel branches; both feed scoring
g.edge("canonical", "features")
g.edge("canonical", "clustering")
g.edge("features", "scoring")
g.edge("clustering", "scoring")

# scoring -> serving (the served rollup the dashboard reads)
g.edge("scoring", "serving")

# Qualitative track: qualstream grades the clustered tickers with an LLM and
# feeds its scorecards into serving (the dashboard reads qual directly).
g.edge("clustering", "qual")
g.edge("qual", "serving")

# Model-validation loop: scoring is backtested, credibility feeds back into scoring
g.edge("scoring", "validation", label="validate")
g.edge("validation", "scoring", label="credibility", color="#EF6C00", style="dashed")

g.render(filename=out_name, directory="tools", cleanup=True)
