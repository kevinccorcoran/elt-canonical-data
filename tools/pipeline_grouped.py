import graphviz
import os

# ---------------------------------------------------------------------
# Mid-level "grouped by stage" view: flat color boxes, left-to-right.
# Each box = stage title + one-line action subtitle + owning repo +
# concepts (quant terms), each with a short plain-English gloss.
#   elt-canonical-data   -> raw / data validation / canonical + orchestration + dashboard
#   elt-inference-models -> metrics / clustering / inference / model validation
# ---------------------------------------------------------------------

# key, title, fill, border, repo, action, [(concept, plain-english gloss)]
STAGES = [
    ("sources", "SOURCES", "#ECEFF1", "#546E7A", "external feeds", "Pull market data", [
        ("Primary price feed", "main daily price source"),
        ("Secondary price feed", "backup price source"),
        ("Delisted-universe feed", "prices for removed companies"),
    ]),
    ("raw", "RAW", "#E3F2FD", "#1E88E5", "elt-canonical-data", "Land it untouched", [
        ("Live price bars", "saved exactly as received"),
        ("Fallback price bars", "backup prices, unedited"),
        ("Delisted price bars", "delisted prices, unedited"),
    ]),
    ("qa", "DATA VALIDATION (QA)", "#FFF8E1", "#F9A825", "elt-canonical-data", "Screen out bad data", [
        ("Cross-feed agreement checks", "do the two sources match?"),
        ("Symbol-reuse and regime breaks", "reused ticker or sudden jump"),
        ("Outlier and stale-data checks", "impossible or frozen values"),
        ("Manual exclusion overrides", "hand-picked drops"),
        ("Tradeable-universe allowlist", "final list of valid tickers"),
    ]),
    ("canonical", "CANONICAL (CDM)", "#E8EAF6", "#3949AB", "elt-canonical-data", "Clean and unify", [
        ("Single source of truth", "one clean table for all"),
        ("Conformed primary feed", "main feed, standard shape"),
        ("Conformed secondary feed", "backup feed, standard shape"),
    ]),
    ("metrics", "METRICS", "#E8F5E9", "#2E7D32", "elt-inference-models", "Measure return and risk", [
        ("Benchmark-relative return (alpha)", "return above the market"),
        ("Multi-horizon return sampling", "returns over many spans"),
        ("Volatility profiling", "how much it swings"),
        ("Cross-sectional growth ranking", "ranked against peers"),
    ]),
    ("clustering", "CLUSTERING", "#E0F2F1", "#00897B", "elt-inference-models", "Group similar stocks", [
        ("Unsupervised grouping", "find look-alike stocks"),
        ("Cluster assignment", "tag each with its group"),
    ]),
    ("inference", "INFERENCE", "#F3E5F5", "#8E24AA", "elt-inference-models", "Forecast direction", [
        ("Feature matrix", "the model inputs"),
        ("State-transition probabilities", "odds of each move"),
        ("Scored transitions", "moves rated by strength"),
        ("Directional return forecast", "predicted up or down"),
        ("Per-ticker signal summary", "one verdict per stock"),
        ("Buy / sell recommendations", "final calls"),
    ]),
    ("validation", "MODEL VALIDATION", "#FFF3E0", "#EF6C00", "elt-inference-models", "Backtest on history", [
        ("Walk-forward out-of-sample test", "tested on unseen data"),
        ("Directional accuracy (sign agreement)", "how often direction was right"),
        ("Payoff backtest", "simulated profit and loss"),
        ("Reliability weighting", "how much to trust it"),
    ]),
    ("serving", "SERVING", "#FCE4EC", "#C2185B", "elt-canonical-data", "Show on a dashboard", [
        ("Interactive dashboard", "explore in the browser"),
        ("Forecast and cluster views", "predictions and groups"),
        ("Coverage and quality views", "data health"),
    ]),
]


def html_label(title, action, repo, items, color):
    rows = ""
    for concept, gloss in items:
        rows += (
            f'<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="11" COLOR="#37474F">{concept}</FONT></TD></TR>'
            f'<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="9" COLOR="#90A4AE"><I>{gloss}</I></FONT></TD></TR>'
        )
    return (
        '<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1" CELLPADDING="3">'
        f'<TR><TD ALIGN="CENTER"><B><FONT POINT-SIZE="15" COLOR="{color}">{title}</FONT></B></TD></TR>'
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

for key, title, fill, border, repo, action, items in STAGES:
    g.node(key, label=html_label(title, action, repo, items, border), fillcolor=fill, color=border)

# Main left-to-right flow
flow = ["sources", "raw", "qa", "canonical", "metrics", "clustering", "inference", "serving"]
for a, b in zip(flow, flow[1:]):
    g.edge(a, b)

# Model-validation loop off inference
g.edge("inference", "validation", label="validate")
g.edge("validation", "inference", label="credibility", color="#EF6C00", style="dashed")

g.render(filename=out_name, directory="tools", cleanup=True)
