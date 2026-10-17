# AlphaStream

![Architecture Diagram](tools/alphastream_system_architecture.png?v=7)

AlphaStream is a data and machine-learning system. It ingests time-series data, cleans it into a canonical layer, ranks groups with unsupervised clustering and statistical scoring, and validates every ranking on periods outside the training data. A separate LLM layer grades each selection against a weighted qualitative rubric. An interactive front end sits on top.

Over 2012–2024, its selections outperformed the benchmark by roughly 8 percentage points a year.

## Project status

**Build phase: complete.** The system runs in prod on a schedule: ingestion, canonical layer, clustering and scoring, walk-forward validation, the qualitative LLM grade, and the front end. The selection gate in the modeling layer was **frozen on 2026-08-13** as a pre-registered forward test, so results are graded out-of-sample without re-tuning on the same history.

Remaining work is monitoring and optimization:

- **Monitor** — the pipeline runs unattended. Every DAG alerts on failure, and two independent watchdogs alert when a scheduled run goes missing. See the [Operations Manual](docs/operations_manual.md).
- **Do not re-tune before the checkpoints.** Changing the objective, sizing, or filters resets the forward clock. Optimization ideas wait until the 6- and 12-month checkpoints grade the frozen rules.

## Repositories

Three repositories:

- **`elt-canonical-data`** (this repo, public) — the data layer: ingestion, raw storage, cleaned canonical tables, shared infrastructure and documentation.
- **`inference-models`** (private) — the modeling layer: unsupervised clustering, statistical scoring, walk-forward validation, and serving tables on top of the canonical tables.
- **`qualstream`** (private, decoupled) — a standalone agent that grades each group member with one Claude call, using only a point-in-time data block (no web search), so every grade is reproducible and backtestable. Refreshed every 4 months.

## Environments

- Two pipeline setups: local and cloud (production)
- Three databases: dev and staging (local), prod (managed)

## Pipeline

![Pipeline](tools/pipeline_grouped.png?v=3)

The flow, grouped by stage; each box is labeled with the database schema it lands in. Data is pulled in, screened for quality, and standardized into the canonical layer. From there it splits into return features and clusters (unsupervised machine learning), feeds a statistical scoring stage, and is validated before reaching the front end. A parallel qualitative stage (qualstream) grades each selection with a single LLM call and feeds the same front end. The machine-learning and LLM stages are marked on the diagram.

## Data Lineage

### ELT — Canonical Data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.jpg)

From raw ingestion to the standardized, deduplicated tables that every downstream model reads.

### ELT — Modeling & Scoring
![ELT Inference Lineage](tools/inference_lineage_graph.jpg?v=2)

Unsupervised clustering, statistical scoring, and walk-forward validation tables on top of the canonical layer.

## Analytics Front End

Ten views: data checks, group ranking, validation, and past selections against the benchmark. Built with R, Shiny, Plotly, and PostgreSQL, in Docker.

![The front end's ten views, grouped into four workflow stages](tools/dashboard_tab_map.png?v=2)

### Transition Range

![Transition Range](tools/alpha_forecast.jpg?v=2)

Compares each group's past and future return distributions across lag horizons up to 60 years.

Supports:
- Side-by-side comparison of past and future distributions across groups.
- Filtering by group, time window, and environment.
- Record counts per group.

### Clusters

![Cluster scatter](tools/clusters_scatter.jpg)

Groups items that behave alike into clusters and plots every member in one view.

Supports:
- One point per member, placed by age and rate of change.
- Color-coding by cluster.
- Toggling clusters on or off and switching environments.

### Rank Stability

![Rank Stability across walk-forward cohorts](tools/rank_stability.jpg)

Small multiples of rank stability across 84 walk-forward cohorts, one heatmap per cluster id. Each cell combines two signals at a given vingtile (5% rank bin) and horizon: whether the median beat its benchmark and whether the forecast direction was correct.

Supports:
- Green means both held: the median beat the benchmark and the forecast direction was correct.
- Top-ranked ids (1-12) shade green when both hold; bottom-ranked ids (13-19) shade purple when the reverse held.
- Filtering by environment, cluster, vingtile depth, metric, and cutoff range.

### Predictions (Backtest Replay)

![Predictions: per-selection realized excess vs benchmark](tools/dashboard_predictions.jpg?v=1)

Replays every ranked selection as it stood at each walk-forward cutoff and shows how it performed against the benchmark.

Supports:
- One bar per selection, sized by its realized excess versus the benchmark over the hold window.
- Color by outcome: beat the benchmark, lagged, or dropped out.
- Filtering by as-of date, replay horizon, cluster, and rank depth.

### Forecast

![Forecast: selections vs benchmark vs walk-forward backtest](tools/dashboard_forecast.jpg?v=2)

Starts at any past date and tracks the model's selections forward in real values against the benchmark, with the walk-forward backtest as the expected path.

Supports:
- Selected set versus benchmark versus per-cluster backtest over the chosen hold length.
- A live out-of-sample log since the model went live.
- Excess return, sensitivity, and a risk-adjusted ratio for the selected window.

### Lifecycle

![Lifecycle: the enter, hold, or exit board for active selections](tools/dashboard_lifecycle.jpg?v=2)

Sorts every active selection into enter, hold, or exit by its place in the hold window and whether it still clears the qualitative gate.

Supports:
- Enter, hold, and exit columns, each showing entry date and hold-window progress.
- A hindsight view of the graded and gate-passing sets against the benchmark.
- A simulator tracking adopted selections against the benchmark.

## Project Timeline

**2024 — Foundation**
- ELT pipeline on Python, dbt, and Airflow
- Initial canonical data model
- Separate dev and staging environments

**2025 — Scale**
- Migrated ingestion to a new data provider
- Consolidated multiple sources into one pipeline
- Added a semantic layer of standardized metrics over the history
- Moved execution to the cloud

**2026 Q1 — Production Infrastructure**
- Split into public infrastructure and private logic repos
- Containerized the stack with Docker
- Moved hosting to DigitalOcean with managed PostgreSQL

**2026 Q2 — Forecasting & Front End**
- Reworked forecasting with walk-forward validation and trust scoring
- Added front-end views for ranges, coverage, and clusters
- Expanded data coverage by 300%, removing survivorship bias

**2026 Q3 — Reliability & Data Quality**
- Point-in-time forecasting with backtest replay and a live log
- Gated forecasts to the rank ranges with proven edge
- Added qualstream, a qualitative LLM grade per selection
- Built the Lifecycle board: enter, hold, or exit

**2026 Q3 — Hands-off Operation & Alerting**
- Froze the selection gate as a pre-registered forward test
- Automated daily maintenance: enter, hold, or exit
- Plain-language alerts on every pipeline stage
- Watchdogs for stalled runs, low disk, and missed syncs

**Ongoing — Monitoring & Optimization**
- Monitor the live selection against realized outcomes
- Optimize and test the qualitative grading layer
- Audit the design against decision principles

---

## Documentation

See the `docs/` directory:

- [**Architecture & Design**](docs/architecture.md)
- [**Development Guide**](docs/development_guide.md)
- [**Operations Manual**](docs/operations_manual.md)
- [**Security**](docs/security.md)
- [**Cheat Sheet**](docs/cheat_sheet.md)
