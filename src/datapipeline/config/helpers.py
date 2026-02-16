import io
import logging
import psycopg2
import polars as pl


def save_to_database(
    df: pl.DataFrame,
    table_name: str,
    schema_name: str,
    connection_string: str,
    conflict_key: str = None,
) -> None:

    if df.is_empty():
        logging.info("No rows to insert — DataFrame is empty.")
        return


    if connection_string.startswith("postgresql+psycopg2://"):
        connection_string = connection_string.replace(
            "postgresql+psycopg2://", "postgresql://", 1
        )

    columns = df.columns
    row_count = df.height
    logging.info(f"Inserting {row_count:,} rows into {schema_name}.{table_name}...")


    csv_buffer = io.StringIO()
    df.write_csv(
        csv_buffer,
        include_header=False,
        null_value="",  # empty fields map to SQL NULL
    )
    csv_buffer.seek(0)

    col_list_str = ", ".join([f'"{c}"' for c in columns])


    if conflict_key is not None:
        conflict_clause = f"ON CONFLICT ({conflict_key}) DO NOTHING"
    else:
        if "ticker_date_id" in columns:
            conflict_clause = "ON CONFLICT (ticker_date_id) DO NOTHING"
        elif "ticker" in columns:
            conflict_clause = "ON CONFLICT (ticker) DO NOTHING"
        else:
            conflict_clause = ""
            logging.warning(f"No conflict key detected for {table_name}.")


    copy_sql = f"""
        COPY tmp_ingest ({col_list_str})
        FROM STDIN WITH (
            FORMAT csv,
            DELIMITER ',',
            NULL ''
        );
    """

    insert_sql = f"""
        INSERT INTO {schema_name}.{table_name} ({col_list_str})
        SELECT {col_list_str}
        FROM tmp_ingest
        {conflict_clause};
    """

    try:

        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
                cur.execute(f"SET search_path TO {schema_name}, public;")
                cur.execute("DROP TABLE IF EXISTS tmp_ingest;")
                cur.execute(f"""
                    CREATE TEMP TABLE tmp_ingest (
                        LIKE {schema_name}.{table_name} INCLUDING ALL
                    ) ON COMMIT DROP;
                """)
                cur.copy_expert(copy_sql, csv_buffer)
                cur.execute(insert_sql)
            conn.commit()

        logging.info(f"Successfully inserted {row_count:,} rows.")

    except Exception as e:

        logging.error(f"Failed to insert into {schema_name}.{table_name}: {e}")
        raise
