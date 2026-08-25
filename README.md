# AlphaStream

![Architecture Diagram](tools/alphastream_system_architecture.png?v=7)

AlphaStream is an end-to-end data and machine-learning system. It ingests time-series data, cleans it into a canonical layer, then ranks groups with unsupervised clustering and statistical scoring, and proves every ranking against periods it never trained on. A separate LLM layer grades each pick against a weighted qualitative rubric. An interactive front end sits on top.

Over 2012–2024, its picks beat the benchmark by roughly 8 percentage points a year.

## Project status

**Build phase: complete.** The end-to-end system is deployed and running in prod on a schedule: ingestion, canonical layer, clustering and scoring, walk-forward validation, the qualitative LLM grade, and the front end. The selection gate in the modeling layer was **frozen on 2026-08-13** as a pre-registered forward test, so results can be graded out-of-sample without re-tuning on the same market history.

Work from here is **monitoring and optimization**, not new construction:

- **Monitor** — the pipeline runs unattended, with alerting on every DAG and two independent watchdogs (a pipeline watch and a portfolio deadman) that page when silence would otherwise be mistaken for health. See the [Operations Manual](docs/operations_manual.md).
- **Do not re-tune before the checkpoints.** Any objective, sizing, or filter change resets the forward clock the freeze exists to measure. Optimization ideas are parked until the 6-/12-month checkpoints grade the frozen rules.

## Repositories

AlphaStream is organized into three repositories:

- **`elt-canonical-data`** (this repo, public) — the data layer. Ingestion, raw storage, cleaned canonical tables, and shared infrastructure and documentation.
- **`inference-models`** (private) — the modeling layer: unsupervised clustering, statistical scoring, walk-forward validation, and serving tables that sit on top of the canonical tables.
- **`qualstream`** (private, newly integrated) — a standalone agent that grades each group member qualitatively with one Claude call per pick, judged only from a point-in-time data block (no web search, so every grade is reproducible and backtestable), refreshed every 4 months.

## Environments

- Two pipeline setups: local and cloud (production)
- Three databases: dev and staging (local), prod (managed)

## Pipeline

![Pipeline](tools/pipeline_grouped.png?v=3)

The end-to-end flow, grouped by stage, each box labeled with the database schema it lands in. The pipeline pulls data in, screens it for quality, and standardizes it into a canonical layer, then splits it into return features and clusters (unsupervised machine learning) that feed a statistical scoring stage, and validates it before it reaches the front end. A parallel qualitative stage (qualstream) grades each pick against a weighted rubric with a single LLM call and feeds the same front end. The diagram flags the machine-learning and LLM stages.

## Data Lineage

### ELT — Canonical Data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.jpg)

From raw ingestion to the standardized, deduplicated tables that serve as the single source of truth for every downstream model.

### ELT — Modeling & Scoring
![ELT Inference Lineage](tools/inference_lineage_graph.jpg?v=2)

Unsupervised clustering, statistical scoring, and walk-forward validation tables that sit on top of the canonical layer.

## Analytics Front End

Ten linked views that walk the model end to end: check the data, find and rank the groups, confirm the ranking holds up, then see how past selections played out against the benchmark. Built with R, Shiny, Plotly, and PostgreSQL, in Docker.

![The front end's ten views, grouped into four workflow stages](tools/dashboard_tab_map.png?v=2)

### Transition Range

![Transition Range](tools/alpha_forecast.jpg?v=2)

Compares each group's past and future return distributions across lag horizons spanning up to 60 years.

Supports:
- Side-by-side comparison of past and future distributions across groups.
- Filtering by group, time window, and environment.
- Record counts per group to gauge reliability.

### Clusters

![Cluster scatter](tools/clusters_scatter.jpg)

Groups items that behave alike into clusters and plots every member in a single view.

Supports:
- One point per member, placed by age and rate of change.
- Color-coding by cluster to compare groups at a glance.
- Toggling clusters on or off and switching environments.

### Rank Stability

![Rank Stability across walk-forward cohorts](tools/rank_stability.jpg)

Small multiples of rank stability across 84 walk-forward cohorts, one heatmap per cluster id. Each cell combines two signals at a given vingtile (5% rank bin) and horizon: whether the median beat its benchmark and whether the forecast direction was correct.

Supports:
- Green means the model was right: the median beat the benchmark and the forecast direction was correct.
- Longs (id 1-12) shade green when both hold; shorts (id 13-19) shade purple when the short worked.
- Filtering by environment, cluster, vingtile depth, metric, and cutoff range.

### Predictions (Backtest Replay)

![Predictions: per-pick realized excess vs benchmark](tools/dashboard_predictions.jpg?v=1)

Replays every ranked pick as it stood at each walk-forward cutoff and shows how it actually performed against the benchmark.

Supports:
- One bar per pick, sized by its realized excess return versus the benchmark over the hold window.
- Color by outcome: beat the benchmark, lagged, or delisted.
- Filtering by as-of date, replay horizon, cluster, and rank depth.

### Forecast

![Forecast: selections vs benchmark vs walk-forward backtest](tools/dashboard_forecast.jpg?v=2)

Stands at any past date and tracks the model's selections forward in real prices against the benchmark, with the walk-forward backtest as the expected path.

Supports:
- Selected set versus benchmark versus per-cluster backtest over the chosen hold length.
- A live out-of-sample log tracked on its own clock since the strategy went live.
- Alpha, beta, and information ratio for the selected window.

### Lifecycle

![Lifecycle: the enter, hold, or exit board for active selections](tools/dashboard_lifecycle.jpg?v=2)

Sorts every active selection into enter, hold, or exit by its place in the hold window and whether it still clears the qualitative gate.

Supports:
- Enter, hold, and exit columns, each showing entry date and hold-window progress.
- A hindsight view of the graded and gate-passing sets against the benchmark.
- A follow-along simulator tracking adopted selections against the benchmark.

## Project Timeline

**2024 — Foundation**
- ELT pipeline built on Python, dbt, and Airflow
- Initial canonical data model
- Separate dev and staging environments

**2025 — Scale**
- Migrated ingestion to a new data provider after the previous one was deprecated
- Consolidated multiple sources into a single pipeline
- Added a metrics layer over the historical dataset
- Moved pipeline execution to the cloud

**2026 Q1 — Production Infrastructure**
- Codebase split into a public infrastructure repo and a private logic repo
- Stack containerized with Docker
- Hosting moved to DigitalOcean with managed PostgreSQL

**2026 Q2 — Forecasting & Front End**
- Reworked the forecasting layer with walk-forward validation and trust scoring
- Expanded the front end with new views for ranges, coverage, and clusters
- Added 300% data coverage, removing survivorship bias

**2026 Q3 — Reliability & Data Quality**
- Added point-in-time forecasting with a backtest replay and a live out-of-sample log
- Gated forecasts to the rank ranges with proven edge
- Integrated qualstream, a qualitative LLM grade on each selection
- Built the Lifecycle decision board: enter, hold, or exit for every active selection

**2026 Q3 — Hands-off Operation & Alerting**
- Froze the selection gate as a pre-registered forward test (build phase complete)
- Hands-off daily portfolio sync: adopt buys, snapshot states, archive sells
- Telegram alerting on every DAG, with plain-language, action-first pings
- Pipeline watch: stale / failed / stuck-run detection plus a host-disk page
- Portfolio deadman: catches a missed sync, an undelivered ping, or reconcile drift outside Airflow

**Ongoing — Monitoring & Optimization**
- Grade the frozen selection against realized outcomes at the 6-/12-month checkpoints
- Optimize and test the qualstream grading layer
- Audit the design against a structured set of decision principles

---

## Documentation

See the `docs/` directory:

- [**Architecture & Design**](docs/architecture.md)
- [**Development Guide**](docs/development_guide.md)
- [**Operations Manual**](docs/operations_manual.md)
- [**Security**](docs/security.md)
- [**Cheat Sheet**](docs/cheat_sheet.md)
