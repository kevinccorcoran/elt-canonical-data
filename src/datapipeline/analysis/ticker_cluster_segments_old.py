#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ───────────────────────────── Secure Environment ─────────────────────────────
def require_env(name: str) -> str:
    """Fail loudly if an env var is missing."""
    value = os.getenv(name)
    if not value or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

db_env = os.getenv("DB_DATABASE", "dev").lower()

# Priority:
# 1. DATABASE_URL override
# 2. DATABASE_URL_<ENV>
if "DATABASE_URL" in os.environ:
    connection_string = require_env("DATABASE_URL")

else:
    if db_env == "staging":
        connection_string = require_env("DATABASE_URL_STAGING")
    elif db_env == "heroku_postgres":
        connection_string = require_env("DATABASE_URL")
    else:
        connection_string = require_env("DATABASE_URL_DEV")

# Normalize psycopg2 style
connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://")

logging.info(f"Using database environment: {db_env}")
logging.info("Connection string loaded securely from environment.")

# ───────────────────────────── Helpers ─────────────────────────────
def clean_row(row, colnames):
    return {col: float(val) if isinstance(val, Decimal) else val for col, val in zip(colnames, row)}

# ───────────────────────────── Query ─────────────────────────────
query = """
SELECT
    ticker,
    months_count,
    growth_pct_per_month,
    weighted_growth,
    stddev_bucket,
    stddev_bucket_num
FROM metrics.ticker_weighted_growth_ranking
"""

# ───────────────────────────── Load Data ─────────────────────────────
try:
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]

        if not rows:
            logging.warning("No rows returned.")
            sys.exit(0)

        df = pl.DataFrame([clean_row(row, colnames) for row in rows])
        logging.info("Loaded %d rows.", df.shape[0])

except Exception as e:
    logging.error("Error loading data: %s", e)
    sys.exit(1)

# ───────────────────────────── Clustering ─────────────────────────────
unique_buckets = df.select("stddev_bucket").unique().to_series().to_list()
clustered_frames = []

for bucket in unique_buckets:
    bucket_df = df.filter(pl.col("stddev_bucket") == bucket)
    n_rows = bucket_df.shape[0]

    if n_rows < 3:
        logging.info("Bucket '%s' has only %d rows — assigning cluster_id = 0", bucket, n_rows)
        bucket_with_clusters = bucket_df.with_columns(pl.lit(0).alias("cluster_id"))
        clustered_frames.append(bucket_with_clusters)
        continue

    X = bucket_df.select(["months_count", "growth_pct_per_month"]).to_numpy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    best_score, best_k = -1, 1
    for k in range(2, min(6, len(X))):
        model = KMeans(n_clusters=k, random_state=42, n_init="auto")
        labels = model.fit_predict(X_scaled)
        if len(np.unique(labels)) >= 2:
            score = silhouette_score(X_scaled, labels)
            if score > best_score:
                best_score, best_k = score, k

    model = KMeans(n_clusters=best_k, random_state=42, n_init="auto")
    labels = model.fit_predict(X_scaled)

    centroids_unscaled = scaler.inverse_transform(model.cluster_centers_)
    months = centroids_unscaled[:, 0]
    growth = centroids_unscaled[:, 1]
    scores = growth / np.maximum(months, 1)

    rank_order = np.argsort(-scores)
    mapping = {old: new for new, old in enumerate(rank_order)}
    labels = np.vectorize(mapping.get)(labels)

    bucket_with_clusters = bucket_df.with_columns(pl.Series("cluster_id", labels))
    clustered_frames.append(bucket_with_clusters)

    logging.info("Bucket '%s' → %d rows, %d clusters (silhouette=%.3f)",
                 bucket, n_rows, best_k, best_score)

# ───────────────── Normalize integer dtypes ─────────────────
clustered_frames = [
    frame.with_columns([
        pl.col(pl.Int8).cast(pl.Int64),
        pl.col(pl.Int16).cast(pl.Int64),
        pl.col(pl.Int32).cast(pl.Int64),
    ])
    for frame in clustered_frames
]

# ───────── Concatenate safely ─────────
final_df = pl.concat(clustered_frames)

# ───────────────────────────── Summaries ─────────────────────────────
cluster_summary = (
    final_df.group_by(["stddev_bucket", "cluster_id"])
    .agg([
        pl.col("months_count").min().alias("min_months"),
        pl.col("months_count").max().alias("max_months"),
        pl.col("months_count").mean().alias("avg_months"),
        pl.col("growth_pct_per_month").min().alias("min_growth"),
        pl.col("growth_pct_per_month").max().alias("max_growth"),
        pl.col("growth_pct_per_month").mean().alias("avg_growth"),
        pl.len().alias("count"),
    ])
)

final_df = final_df.join(cluster_summary, on=["stddev_bucket", "cluster_id"], how="left")
print(final_df.head(10))

# ───────────────────────────── Save to Database ─────────────────────────────
schema_name = "analysis"
table_name = "ticker_cluster_segments"

try:
    with psycopg2.connect(connection_string) as conn:

        # Create schema + table
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {schema_name};

                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    ticker TEXT,
                    months_count INT,
                    growth_pct_per_month FLOAT,
                    stddev_bucket TEXT,
                    stddev_bucket_num FLOAT,
                    cluster_id INT,
                    min_months INT,
                    max_months INT,
                    avg_months INT,
                    min_growth FLOAT,
                    max_growth FLOAT,
                    avg_growth INT,
                    count INT
                );
            """)
            conn.commit()

        # Truncate old data
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name};")
            conn.commit()

        expected_columns = [
            "ticker",
            "months_count",
            "growth_pct_per_month",
            "stddev_bucket",
            "stddev_bucket_num",
            "cluster_id",
            "min_months",
            "max_months",
            "avg_months",
            "min_growth",
            "max_growth",
            "avg_growth",
            "count",
        ]

        final_df = final_df.with_columns([
            pl.col("avg_months").round(0).cast(pl.Int64),
            pl.col("avg_growth").round(0).cast(pl.Int64)
        ]).select(expected_columns)

        csv_buffer = io.StringIO()
        final_df.write_csv(csv_buffer)
        csv_buffer.seek(0)

        # COPY into table
        with conn.cursor() as cursor:
            cursor.copy_expert(
                f"""
                COPY {schema_name}.{table_name} (
                    ticker,
                    months_count,
                    growth_pct_per_month,
                    stddev_bucket,
                    stddev_bucket_num,
                    cluster_id,
                    min_months,
                    max_months,
                    avg_months,
                    min_growth,
                    max_growth,
                    avg_growth,
                    count
                )
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                """,
                csv_buffer,
            )
        conn.commit()

        # Corrected index creation (no syntax errors)
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_ticker
                    ON {schema_name}.{table_name} (ticker);

                CREATE INDEX IF NOT EXISTS idx_{table_name}_cluster
                    ON {schema_name}.{table_name} (cluster_id);

                CREATE INDEX IF NOT EXISTS idx_{table_name}_bucket
                    ON {schema_name}.{table_name} (stddev_bucket);

                CREATE INDEX IF NOT EXISTS idx_{table_name}_bucketnum
                    ON {schema_name}.{table_name} (stddev_bucket_num);
            """)
            conn.commit()

        logging.info("Inserted %d rows into %s.%s",
                     final_df.shape[0], schema_name, table_name)

except Exception as e:
    logging.error("Error saving to database: %s", e)
    sys.exit(1)

logging.info("Process completed successfully.")


# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# import os
# import sys
# import io
# import psycopg2
# import logging
# import polars as pl
# import numpy as np
# from decimal import Decimal
# from sklearn.cluster import KMeans
# from sklearn.preprocessing import StandardScaler
# from sklearn.metrics import silhouette_score

# # ───────────────────────────── Logging ─────────────────────────────
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # ───────────────────────────── Environment (bulletproof) ─────────────────────────────
# database_url = os.getenv("DATABASE_URL")
# db_env = os.getenv("DB_DATABASE", "dev").lower()

# if database_url:
#     connection_string = database_url
#     db_env = "custom"
# else:
#     if db_env == "staging":
#         connection_string = os.getenv(
#             "DATABASE_URL_STAGING",
#             "postgresql://postgres:@localhost:5432/staging"
#         )
#     else:
#         connection_string = os.getenv(
#             "DATABASE_URL_DEV",
#             "postgresql://postgres:@localhost:5432/dev"
#         )

# connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://")
# logging.info(f"Using database environment: {db_env}")
# logging.info(f"Connection string: {connection_string}")

# # ───────────────────────────── Helpers ─────────────────────────────
# def clean_row(row, colnames):
#     return {col: float(val) if isinstance(val, Decimal) else val for col, val in zip(colnames, row)}

# # ───────────────────────────── Query ─────────────────────────────
# query = """
# SELECT
#     ticker,
#     months_count,
#     growth_pct_per_month,
#     weighted_growth,
#     stddev_bucket,
#     stddev_bucket_num
# FROM metrics.ticker_weighted_growth_ranking
# """

# # ───────────────────────────── Load Data ─────────────────────────────
# try:
#     with psycopg2.connect(connection_string) as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(query)
#             rows = cursor.fetchall()
#             colnames = [desc[0] for desc in cursor.description]

#         if not rows:
#             logging.warning("No rows returned.")
#             sys.exit(0)

#         df = pl.DataFrame([clean_row(row, colnames) for row in rows])
#         logging.info("Loaded %d rows.", df.shape[0])

# except Exception as e:
#     logging.error("Error loading data: %s", e)
#     sys.exit(1)

# # ───────────────────────────── Clustering ─────────────────────────────
# unique_buckets = df.select("stddev_bucket").unique().to_series().to_list()
# clustered_frames = []

# for bucket in unique_buckets:
#     bucket_df = df.filter(pl.col("stddev_bucket") == bucket)
#     n_rows = bucket_df.shape[0]

#     if n_rows < 3:
#         logging.info("Bucket '%s' has only %d rows — assigning cluster_id = 0", bucket, n_rows)
#         bucket_with_clusters = bucket_df.with_columns(pl.lit(0).alias("cluster_id"))
#         clustered_frames.append(bucket_with_clusters)
#         continue

#     X = bucket_df.select(["months_count", "growth_pct_per_month"]).to_numpy()
#     scaler = StandardScaler()
#     X_scaled = scaler.fit_transform(X)

#     best_score, best_k = -1, 1
#     for k in range(2, min(6, len(X))):
#         model = KMeans(n_clusters=k, random_state=42, n_init="auto")
#         labels = model.fit_predict(X_scaled)
#         if len(np.unique(labels)) >= 2:
#             score = silhouette_score(X_scaled, labels)
#             if score > best_score:
#                 best_score, best_k = score, k

#     model = KMeans(n_clusters=best_k, random_state=42, n_init="auto")
#     labels = model.fit_predict(X_scaled)

#     centroids_unscaled = scaler.inverse_transform(model.cluster_centers_)
#     months = centroids_unscaled[:, 0]
#     growth = centroids_unscaled[:, 1]
#     scores = growth / np.maximum(months, 1)

#     rank_order = np.argsort(-scores)
#     mapping = {old: new for new, old in enumerate(rank_order)}
#     labels = np.vectorize(mapping.get)(labels)

#     bucket_with_clusters = bucket_df.with_columns(pl.Series("cluster_id", labels))
#     clustered_frames.append(bucket_with_clusters)

#     logging.info("Bucket '%s' → %d rows, %d clusters (silhouette=%.3f)",
#                  bucket, n_rows, best_k, best_score)

# # ───────── Normalize integer dtypes across frames ─────────
# clustered_frames = [
#     frame.with_columns([
#         pl.col(pl.Int8).cast(pl.Int64),
#         pl.col(pl.Int16).cast(pl.Int64),
#         pl.col(pl.Int32).cast(pl.Int64),
#     ])
#     for frame in clustered_frames
# ]

# # ───────── Concatenate safely ─────────
# final_df = pl.concat(clustered_frames)

# # ───────────────────────────── Summaries ─────────────────────────────
# cluster_summary = (
#     final_df.group_by(["stddev_bucket", "cluster_id"])
#     .agg([
#         pl.col("months_count").min().alias("min_months"),
#         pl.col("months_count").max().alias("max_months"),
#         pl.col("months_count").mean().alias("avg_months"),
#         pl.col("growth_pct_per_month").min().alias("min_growth"),
#         pl.col("growth_pct_per_month").max().alias("max_growth"),
#         pl.col("growth_pct_per_month").mean().alias("avg_growth"),
#         pl.len().alias("count"),
#     ])
# )

# final_df = final_df.join(cluster_summary, on=["stddev_bucket", "cluster_id"], how="left")
# print(final_df.head(10))

# # ───────────────────────────── Save to Database ─────────────────────────────
# schema_name = "analysis"
# table_name = "ticker_cluster_segments"

# try:
#     with psycopg2.connect(connection_string) as conn:
#         with conn.cursor() as cursor:

#             cursor.execute(f"""
#                 CREATE SCHEMA IF NOT EXISTS {schema_name};
#                 CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
#                     ticker TEXT,
#                     months_count INT,
#                     growth_pct_per_month FLOAT,
#                     stddev_bucket TEXT,
#                     stddev_bucket_num FLOAT,
#                     cluster_id INT,
#                     min_months INT,
#                     max_months INT,
#                     avg_months INT,
#                     min_growth FLOAT,
#                     max_growth FLOAT,
#                     avg_growth INT,
#                     count INT
#                 );
#             """)
#             conn.commit()

#         with conn.cursor() as cursor:
#             cursor.execute(f"TRUNCATE TABLE {schema_name}.{table_name};")
#             conn.commit()

#         expected_columns = [
#             "ticker",
#             "months_count",
#             "growth_pct_per_month",
#             "stddev_bucket",
#             "stddev_bucket_num",
#             "cluster_id",
#             "min_months",
#             "max_months",
#             "avg_months",
#             "min_growth",
#             "max_growth",
#             "avg_growth",
#             "count",
#         ]

#         final_df = final_df.with_columns([
#             pl.col("avg_months").round(0).cast(pl.Int64),
#             pl.col("avg_growth").round(0).cast(pl.Int64)
#         ]).select(expected_columns)

#         csv_buffer = io.StringIO()
#         final_df.write_csv(csv_buffer)
#         csv_buffer.seek(0)

#         with conn.cursor() as cursor:
#             cursor.copy_expert(
#                 f"""
#                 COPY {schema_name}.{table_name} (
#                     ticker,
#                     months_count,
#                     growth_pct_per_month,
#                     stddev_bucket,
#                     stddev_bucket_num,
#                     cluster_id,
#                     min_months,
#                     max_months,
#                     avg_months,
#                     min_growth,
#                     max_growth,
#                     avg_growth,
#                     count
#                 )
#                 FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
#                 """,
#                 csv_buffer,
#             )
#         conn.commit()

#         with conn.cursor() as cursor:
#             cursor.execute(f"""
#                 CREATE INDEX IF NOT EXISTS idx_{table_name}_ticker     ON {schema_name}.{table_name} (ticker);
#                 CREATE INDEX IF NOT EXISTS idx_{table_name}_cluster    ON {schema_name}.{table_name} (cluster_id);
#                 CREATE INDEX IF NOT EXISTS idx_{table_name}_bucket     ON {schema_name}.{table_name} (stddev_bucket);
#                 CREATE INDEX IF NOT EXISTS idx_{table_name}_bucketnum  ON {schema_name}.{table_name} (stddev_bucket_num);
#             """)
#             conn.commit()

#         logging.info("Inserted %d rows into %s.%s",
#                      final_df.shape[0], schema_name, table_name)

# except Exception as e:
#     logging.error("Error saving to database: %s", e)
#     sys.exit(1)

# logging.info("Process completed successfully.")
