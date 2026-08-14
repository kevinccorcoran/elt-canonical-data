# Architecture & Design

![AlphaStream System Architecture](../tools/alphastream_system_architecture.png)

A conceptual map of AlphaStream: the data flow, the three repositories, and how it runs. Written to be read top to bottom.

---

## 1. What it is

AlphaStream refines raw time-series into ranked, quality-graded selections and serves them to an interactive dashboard. It follows an **ELT** pattern: raw source data is loaded before it is transformed, so the source is preserved independently of transformation logic and can be safely reprocessed if rules change.

---

## 2. Data flow — the refinement spine

Each stage is a PostgreSQL schema; data flows left to right, each layer more trustworthy than the last.

1. **Ingest → `raw`** — Python fetches from **Massive** (live API plus delisted flat-file backfill) and **Yahoo Finance**; vendor data lands unchanged.
2. **Canonicalize → `cdm`** — dbt cleans, dedupes, and standardizes into the canonical data model: the single source of truth.
3. **Feature & cluster → `features`, `clustering`, `analysis`** — benchmark-relative returns, plus unsupervised groups of lookalike items (a Python step segments the ranking).
4. **Score → `scoring`** — forecasts direction and ranks every item.
5. **Serve → `serving`, `monitoring`** — rolls scored picks into the served answer and logs every call.
6. **Validate (feedback loop) → `validation`** — a walk-forward backtest replays past calls on data it never trained on; the resulting credibility gates which rank ranges are allowed to reach `serving`.
7. **Grade (sidecar) → `qual`** — qualstream grades each pick with one point-in-time LLM call and writes its own schema, joined only at the dashboard.

Two moves make it more than a straight line: the **walk-forward loop** (6) gates what reaches the surface, and the **decoupled grader** (7) joins only at the edge.

---

## 3. Three repositories — trust seams

| Repo | Visibility | Owns |
| :--- | :--- | :--- |
| `elt-canonical-data` | public | ingestion + canonical (`raw`, `cdm`), shared infra/docs |
| `inference-models` | private | modeling (`features` → `scoring` → `serving`), `validation`, `monitoring` |
| `qualstream` | decoupled | LLM qualitative grader (`qual`); shares only the database |

The split is about coupling, not the org chart: public data, private logic, decoupled grading. Each boundary contains a failure or a leak.

---

## 4. Delivery

*   **Shiny dashboard** — ten linked views walking the model end to end (data health → groups & ranking → validation → decisions), ending in the **Lifecycle** decision board: enter / hold / exit calls on active selections.
*   **WhatsApp push channel** — alerts on the standing selections; a push surface alongside the pull dashboard.

---

## 5. Runtime components

*   **Ingestion** — Python; resilient to API failures, rate limits, and retries; writes `raw`.
*   **Transformation** — dbt; applies business logic and runs automated data tests before promoting data.
*   **Orchestration** — Apache Airflow; runs the DAGs in dependency order (ingest before transform).
*   **Storage** — DigitalOcean Managed PostgreSQL; daily backups with point-in-time recovery.

---

## 6. Environment strategy

| | Local (Mac) | Production (Linux server) |
| :--- | :--- | :--- |
| Purpose | development & testing | live execution, source of truth |
| Data | subset / test | full historical dataset |
| Infrastructure | Docker Desktop | Docker on a DigitalOcean droplet |
| State | ephemeral | persistent (managed DB) |

**Rule:** the production server is an execution target only. Code is committed and tested locally, then deployed from GitHub `main`.

---

## 7. Security

*   **Network** — inbound blocked by default; only SSH and the Airflow UI (via tunnel) are reachable from whitelisted IPs.
*   **Access** — SSH keys only, no passwords.
*   **Secrets** — injected via environment variables at runtime; never stored in code.
