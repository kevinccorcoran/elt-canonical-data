# AlphaStream

![Architecture Diagram](tools/alphastream_system_architecture.png?v=7)

AlphaStream is a data and machine learning system that picks stocks and tests each pick against history. Python loads daily prices and SEC filings into PostgreSQL. dbt cleans the prices into one canonical table. Gaussian mixture clustering groups stocks by long-run growth and age. A forecasting model scores each stock within its cluster. A walk-forward backtest validates every score. An LLM grader reviews each pick. A Shiny front end shows the results. Airflow runs it all on a schedule.

## Status

The build is complete and runs in production.

The pick rules were frozen on 2026-08-13 as a pre-registered forward test. In October 2026 a point-in-time audit found look-ahead in the walk-forward backtest. The backtest scored picks with data dated after its own cutoffs. Rebuilt point in time on real prices, the picks show no proven edge. The coarse cluster signal holds in the typical stock: falling clusters keep losing. What fails is picking winners inside the rising clusters.

Model buys are stopped for good. The live log keeps running as a record. Any future model must pass the same point-in-time replay before it buys.

Classic price signals such as momentum and low volatility show no proven edge either. So work now goes into data beyond prices. A raw layer of SEC EDGAR fundamentals is built and tested. It keeps every filed version. A fact counts from the trading day after it was filed. Valuation and quality indicators come next.

## The system

Five layers. Data sits at the center.

| Layer | What it does | Built with |
|---|---|---|
| **Data** | Ingests prices from Massive and Yahoo Finance. Ingests filings from SEC EDGAR. Builds the canonical price table. Clusters and scores stocks. Validates every score with a walk-forward backtest. | Python, dbt, PostgreSQL, scikit-learn |
| **Backend** | Runs every job on a schedule. Grades picks with an LLM. Keeps the portfolio book. Sends alerts. | Airflow, cron, Claude API, Telegram |
| **Frontend** | Shows the board and the picks. Shows the checks on the model and the data. | R, Shiny, Plotly |
| **Infra** | Prod runs on a cloud server with a managed database. A Mac mirrors it with local dev and staging databases. | DigitalOcean, Docker, PostgreSQL, OrbStack |
| **Dev tools** | Version control, tests, deploys and docs for every layer. Pre-registered rules for every experiment. | Git, GitHub, pytest, dbt tests, dbt docs |

## Repositories

- **elt-canonical-data** (this repo, public). The data layer: ingestion, raw storage and the canonical tables. Also the shared Airflow DAGs, the front end and the docs.
- **elt-inference-models** (private). The modeling layer: clustering, scoring, walk-forward validation and serving tables in dbt and Python.
- **qualstream** (private). The LLM grader. It grades each pick with one Claude call. The call sees only data from that date and has no web search. So every grade can be replayed and backtested.

## Pipeline

![Pipeline](tools/pipeline_grouped.png?v=3)

Each box names the database schema it lands in. Data is pulled in and screened for quality. It becomes one canonical price per stock per day. From there it splits into return features and clusters. A forecasting model scores the transitions in each cluster. A walk-forward backtest validates each score and weights it by credibility. In parallel the LLM grader scores each pick on a weighted rubric. Both feed the serving tables and the front end.

## Data lineage

### Canonical data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.jpg)

The dbt lineage from raw ingestion to the canonical tables every model reads.

### Modeling and scoring
![ELT Inference Lineage](tools/inference_lineage_graph.jpg?v=2)

Clustering, scoring and walk-forward validation on top of the canonical layer.

## Front end

Eleven views in four stages: foundation, analysis, validation and decisions. Built with R, Shiny and Plotly. It runs in its own Docker image.

![The front end's eleven views, grouped into four workflow stages](tools/dashboard_tab_map.png?v=3)

### Lifecycle

![Lifecycle: the enter, hold, or exit board for active selections](tools/dashboard_lifecycle.jpg?v=2)

The board. Every active pick sits in one column: enter, hold or exit. The hold window and the LLM grade decide the column. It also tracks the portfolio against the benchmark.

### Forecast

![Forecast: selections vs benchmark vs walk-forward backtest](tools/dashboard_forecast.jpg?v=2)

Follows the picks forward from any past date against the benchmark. The walk-forward backtest is the expected path. A live out-of-sample log tracks the real calls. A banner warns that the backtest has look-ahead.

### Predictions

![Predictions: per-selection realized excess vs benchmark](tools/dashboard_predictions.jpg?v=1)

The current buy list. It also replays every past pick as it stood at each walk-forward cutoff. Each bar is one pick's excess return against the benchmark.

### Rank Stability

![Rank Stability across walk-forward cohorts](tools/rank_stability.jpg)

Heatmaps of every rank slot in every cluster across the walk-forward cutoffs. Green means the slot beat the benchmark and called the direction right. The October audit found that these cells grade picks on data from after the cutoff. So the view does not measure rank skill.

### Clusters

![Cluster scatter](tools/clusters_scatter.jpg)

Every stock on one chart, placed by age and growth. Color shows the cluster.

### Transition Range

![Transition Range](tools/alpha_forecast.jpg?v=2)

Compares each cluster's past and future return distributions across lag horizons.

## Timeline

### Data
- **2024.** First ELT pipeline and the canonical price model.
- **2025.** Daily prices move from Yahoo Finance to Polygon, now Massive. Both feeds merge into one canonical price table. A metrics layer adds returns against SPY over the full history.
- **2026 Q2.** Delisted stocks added. Coverage grows by 300% and survivorship bias is removed. Forecasts get walk-forward validation and a credibility weight per cell.
- **2026 Q3.** A live log records every call. Picks are gated to the rank slots that beat the benchmark in the backtest. The gate is frozen on 2026-08-13 as a pre-registered forward test.
- **2026 Q4.** A point-in-time audit finds look-ahead in the backtest. On real prices the picks show no proven edge. Classic price signals show none either. A raw layer of SEC EDGAR fundamentals is added.

### Backend
- **2024.** Airflow schedules the pipeline.
- **2026 Q3.** qualstream grades each pick with Claude. The Lifecycle board updates itself each day. Every stage sends Telegram alerts. Watchdogs catch stalled runs, low disk and missed syncs.
- **2026 Q4.** Model buys stopped for good. A lock keeps the daily model jobs idle during the monthly walk-forward run. Airflow jobs added for SEC EDGAR.

### Frontend
- **2026 Q2.** Transition Range, Coverage and Clusters views.
- **2026 Q3.** Predictions replays the backtest. Forecast adds the live log. The Lifecycle board arrives.
- **2026 Q4.** Look-ahead warning on the backtest views.

### Infra
- **2024.** Local PostgreSQL with separate dev and staging databases.
- **2025.** The pipeline moves to the cloud.
- **2026 Q1.** The stack moves into Docker. Hosting moves to DigitalOcean with managed PostgreSQL.
- **2026 Q4.** The Shiny app gets its own Docker image.

### Dev tools
- **2024.** Python, dbt and Airflow as the core stack.
- **2026 Q1.** Code splits into a public infrastructure repo and private model repos.
- **2026 Q3.** Experiments are pre-registered. Decision rules are committed before each run.
- **2026 Q4.** Point-in-time audit scripts replay the walk-forward on real prices.

### Next
- Build valuation and quality indicators from the fundamentals, such as P/E, EV/EBITDA and ROIC.
- Backtest them point in time on real prices before any use.

## Documentation

- [Architecture and Design](docs/architecture.md)
- [Development Guide](docs/development_guide.md)
- [Operations Manual](docs/operations_manual.md)
- [Company Fundamentals](docs/edgar_fundamentals.md)
- [Security](docs/security.md)
- [Cheat Sheet](docs/cheat_sheet.md)
