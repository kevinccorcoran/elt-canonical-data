#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import psycopg2
import logging
import polars as pl
import numpy as np
from decimal import Decimal
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

# ───────────────────────────── Logging ─────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ───────────────────────────── Environment ─────────────────────────────
def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

db_env = os.getenv("DB_DATABASE", "dev").lower()

if "DATABASE_URL" in os.environ:
    connection_string = require_env("DATABASE_URL")
elif db_env == "staging":
    connection_string = require_env("DATABASE_URL_STAGING")
else:
    connection_string = require_env("DATABASE_URL_DEV")

connection_string = connection_string.replace(
    "postgresql+psycopg2://", "postgresql://"
)

logging.info("Using DB env: %s", db_env)

# ───────────────────────────── Helpers ─────────────────────────────
def clean_row(row, colnames):
    return {
        col: float(val) if isinstance(val, Decimal) else val
        for col, val in zip(colnames, row)
    }

# ───────────────────────────── Load data ─────────────────────────────
query = """
SELECT
    ticker,
    months_count,
    growth_pct_per_month,
    weighted_growth,
    monthly_growth_vol_z_bucket,
    monthly_growth_vol_z_bucket_num
FROM metrics.ticker_weighted_growth_ranking
"""

with psycopg2.connect(connection_string) as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

df = pl.DataFrame([clean_row(r, cols) for r in rows])
logging.info("Loaded %d rows", df.height)

# ───────────────────────────── Clustering ─────────────────────────────
clustered_frames = []

for bucket in df.select("monthly_growth_vol_z_bucket").unique().to_series():
    bdf = df.filter(pl.col("monthly_growth_vol_z_bucket") == bucket)
    n = bdf.height

    # Too small to cluster reliably
    if n < 6:
        clustered_frames.append(
            bdf.with_columns(pl.lit(0).alias("cluster_id"))
        )
        continue

    # Feature space: maturity + avg monthly growth
    X = (
        bdf
        .with_columns(
            pl.col("months_count").log().alias("log_months")
        )
        .select([
            "log_months",
            "growth_pct_per_month"
        ])
        .to_numpy()
    )

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    max_k = min(5, n // 2)
    best_k, best_score = 1, -1

    for k in range(2, max_k + 1):
        labels = KMeans(
            k,
            random_state=42,
            n_init="auto"
        ).fit_predict(Xs)

        score = silhouette_score(Xs, labels)
        if score > best_score:
            best_k, best_score = k, score

    model = KMeans(best_k, random_state=42, n_init="auto")
    labels = model.fit_predict(Xs)

    # Order clusters by avg growth_pct_per_month (descending)
    centroids = scaler.inverse_transform(model.cluster_centers_)
    order = np.argsort(-centroids[:, 1])
    remap = {old: new for new, old in enumerate(order)}
    labels = np.vectorize(remap.get)(labels)

    clustered_frames.append(
        bdf.with_columns(pl.Series("cluster_id", labels))
    )

final_df = pl.concat(clustered_frames)

# ───────────────────────────── Aggregation ─────────────────────────────
cluster_summary = (
    final_df
    .group_by(["monthly_growth_vol_z_bucket", "cluster_id"])
    .agg([
        pl.col("months_count").min().alias("min_months"),
        pl.col("months_count").max().alias("max_months"),
        pl.col("months_count").mean().round(1).alias("avg_months"),

        pl.col("growth_pct_per_month").min().alias("min_growth"),
        pl.col("growth_pct_per_month").max().alias("max_growth"),
        pl.col("growth_pct_per_month").mean().round(2).alias("avg_growth_pct_per_month"),

        pl.col("weighted_growth")
          .mean()
          .round(4)
          .alias("avg_weighted_growth"),

        pl.len().alias("count"),
    ])
)

final_df = final_df.join(
    cluster_summary,
    on=["monthly_growth_vol_z_bucket", "cluster_id"],
    how="left"
)

# ───────────────────────────── Final rounding (storage only) ─────────────────────────────
final_df = final_df.with_columns(
    pl.col("growth_pct_per_month").round(2)
)

# ───────────────────────────── Save to DB ─────────────────────────────
schema = "analysis"
table = "ticker_cluster_segments"

with psycopg2.connect(connection_string) as conn:
    with conn.cursor() as cur:
        cur.execute(f"""
            DROP TABLE IF EXISTS {schema}.{table};

            CREATE TABLE {schema}.{table} (
                ticker TEXT,
                months_count INT,
                growth_pct_per_month FLOAT,
                weighted_growth FLOAT,
                monthly_growth_vol_z_bucket TEXT,
                monthly_growth_vol_z_bucket_num FLOAT,
                cluster_id INT,
                min_months INT,
                max_months INT,
                avg_months FLOAT,
                min_growth FLOAT,
                max_growth FLOAT,
                avg_growth_pct_per_month FLOAT,
                avg_weighted_growth FLOAT,
                count INT
            );
        """)
        conn.commit()

    csv = io.StringIO()
    final_df.write_csv(csv)
    csv.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            f"""
            COPY {schema}.{table}
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
            """,
            csv
        )
    conn.commit()

logging.info(
    "Inserted %d rows into %s.%s",
    final_df.height,
    schema,
    table
)
logging.info("Process completed successfully.")
