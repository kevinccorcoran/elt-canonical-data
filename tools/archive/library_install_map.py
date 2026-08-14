"""
Where every dependency is installed.

There are only two install targets:
  - Local .venv   (host-native Python on the MacBook, dev only)
  - Docker image  (the same image runs locally via `docker compose up`
                   AND on the Droplet in prod - Shiny runs inside it either way)

The single fact that matters: the Python packages (requirements.txt) are the ONLY
group installed in BOTH.  Everything else - the Airflow base image, the apt system
libraries, and the R/Shiny packages - lives ONLY in the container.

Rendered as one matrix: rows = dependency group, columns = install target.
Render:  python tools/library_install_map.py
Output:  tools/library_install_map.png
"""

import os
import graphviz

INK = "#37474F"
YES = "#2E7D32"   # installed here
NO = "#B0BEC5"    # not installed here


def cell(mark, color, note=""):
    inner = f'<FONT POINT-SIZE="16" COLOR="{color}"><B>{mark}</B></FONT>'
    if note:
        inner += f'<BR/><FONT POINT-SIZE="9" COLOR="#90A4AE">{note}</FONT>'
    return f'<TD ALIGN="CENTER" BGCOLOR="#FFFFFF">{inner}</TD>'


def group(name, detail, color):
    return (
        f'<TD ALIGN="LEFT" BGCOLOR="#FFFFFF">'
        f'<FONT POINT-SIZE="13" COLOR="{color}"><B>{name}</B></FONT>'
        f'<BR/><FONT POINT-SIZE="10" COLOR="#78909C">{detail}</FONT></TD>'
    )


def header(text, sub):
    return (
        f'<TD ALIGN="CENTER" BGCOLOR="#ECEFF1">'
        f'<FONT POINT-SIZE="13" COLOR="{INK}"><B>{text}</B></FONT>'
        f'<BR/><FONT POINT-SIZE="9" COLOR="#78909C">{sub}</FONT></TD>'
    )


# rows: (group name, detail, in local .venv?, in container?)
ROWS = [
    (
        "Python packages", "pip install -r requirements.txt<BR/>"
        "airflow &#183; dbt &#183; pandas &#183; polars &#183; numpy &#183; "
        "psycopg &#183; sqlalchemy &#183; scikit-learn &#183; anthropic",
        "#6A1B9A", True, True,
    ),
    (
        "Airflow base image", "apache/airflow:2.9.3 (Python 3 runtime)",
        "#00897B", False, True,
    ),
    (
        "System libraries", "Dockerfile apt-get + Aptfile<BR/>"
        "git &#183; build-essential &#183; r-base &#183; libpq &#183; libssl &#183; libxml2",
        "#EF6C00", False, True,
    ),
    (
        "R + Shiny", "Rscript install.packages<BR/>"
        "shiny &#183; RPostgres &#183; plotly &#183; DT &#183; nanoparquet",
        "#C2185B", False, True,
    ),
]

rows_html = ""
for name, detail, color, in_venv, in_ctr in ROWS:
    left = cell("&#10003;", YES) if in_venv else cell("&#8212;", NO)
    right = cell("&#10003;", YES) if in_ctr else cell("&#8212;", NO)
    rows_html += f"<TR>{group(name, detail, color)}{left}{right}</TR>"

table = (
    '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="10" COLOR="#CFD8DC">'
    f'<TR>{header("Dependency group", "what gets installed")}'
    f'{header("Local .venv", "host Python &#183; dev only")}'
    f'{header("Docker image", "runs locally + on Droplet")}</TR>'
    f'{rows_html}'
    '</TABLE>>'
)

g = graphviz.Digraph("library_install_map", format="png")
g.attr(bgcolor="white", pad="0.5", fontname="Sans-Serif", labelloc="t", fontsize="20",
       label="AlphaStream - libraries and where each is installed")
g.attr("node", shape="plaintext", fontname="Sans-Serif")
g.node("matrix", table)

out = os.environ.get("DIAGRAM_OUTPUT", "library_install_map")
print("wrote", g.render(filename=out, directory="tools", cleanup=True))
