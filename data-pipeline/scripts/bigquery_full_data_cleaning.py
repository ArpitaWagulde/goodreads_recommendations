# bigquery_clean_books_and_interactions_fixed.py

import os
import logging
from google.cloud import bigquery
import pandas as pd

# 1️⃣ Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# 2️⃣ GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_credentials.json"

# 3️⃣ BigQuery client
project_id = "recommendation-system-475301"
client = bigquery.Client(project=project_id)

# 4️⃣ Columns that need global median
median_numeric_cols = ["publication_year", "num_pages"]

# 5️⃣ Function to clean table
def clean_table(dataset_id: str, table_name: str, destination_table: str, apply_global_median: bool = False):
    try:
        logger.info(f"Starting cleaning for table: {dataset_id}.{table_name}")

        # Get all columns
        columns_info = client.query(f"""
            SELECT column_name, data_type
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """).to_dataframe()
        logger.info(f"Retrieved {len(columns_info)} columns for table {table_name}")

        array_cols = [row['column_name'] for _, row in columns_info.iterrows() if row['data_type'].startswith('ARRAY')]
        string_cols = [row['column_name'] for _, row in columns_info.iterrows() if row['data_type'] in ('STRING', 'CHAR', 'TEXT')]
        bool_cols = [row['column_name'] for _, row in columns_info.iterrows() if row['data_type'] == 'BOOL']

        # Build select expressions
        select_exprs = []
        for _, row in columns_info.iterrows():
            col = row['column_name']

            if apply_global_median and col in median_numeric_cols:
                select_exprs.append(
                    f"COALESCE({col}, global_medians.{col}_median) AS {col}"
                )
            elif col in string_cols:
                select_exprs.append(f"COALESCE(NULLIF(TRIM({col}), ''), 'Unknown') AS {col}_clean")
            elif col in bool_cols:
                select_exprs.append(f"COALESCE({col}, FALSE) AS {col}")
            elif col in array_cols:
                select_exprs.append(
                    f"ARRAY(SELECT TO_JSON_STRING(x) FROM UNNEST({col}) AS x WHERE x IS NOT NULL) AS {col}_flat"
                )
            else:
                select_exprs.append(col)

        select_sql = ",\n  ".join(select_exprs)

        # Build query
        if apply_global_median:
            query = f"""
            WITH main AS (
                SELECT *
                FROM `{project_id}.{dataset_id}.{table_name}`
            ),
            global_medians AS (
                SELECT
                    {', '.join([f'APPROX_QUANTILES({col}, 2)[OFFSET(1)] AS {col}_median' for col in median_numeric_cols])}
                FROM main
            )
            SELECT DISTINCT
              {select_sql}
            FROM main
            LEFT JOIN global_medians ON TRUE
            """
        else:
            query = f"""
            SELECT DISTINCT
              {select_sql}
            FROM `{project_id}.{dataset_id}.{table_name}`
            """

        logger.info(f"Executing cleaning query for {table_name}...")
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition="WRITE_TRUNCATE"
        )
        client.query(query, job_config=job_config).result()
        logger.info(f"✅ Cleaned table saved: {destination_table}")

    except Exception as e:
        logger.error(f"Error cleaning table {dataset_id}.{table_name}: {e}", exc_info=True)

# 6️⃣ Clean the books table with global median
clean_table(
    dataset_id="books",
    table_name="goodreads_books_mystery_thriller_crime",
    destination_table=f"{project_id}.books.goodreads_books_cleaned",
    apply_global_median=True
)

# 7️⃣ Clean the interactions table without median
clean_table(
    dataset_id="books",
    table_name="goodreads_interactions_mystery_thriller_crime",
    destination_table=f"{project_id}.books.goodreads_interactions_cleaned",
    apply_global_median=False
)

# 8️⃣ Fetch small samples for verification
try:
    df_books_sample = client.query(
        f"SELECT * FROM `{project_id}.books.goodreads_books_cleaned` LIMIT 5"
    ).to_dataframe()
    df_interactions_sample = client.query(
        f"SELECT * FROM `{project_id}.books.goodreads_interactions_cleaned` LIMIT 5"
    ).to_dataframe()

    logger.info("Books sample:")
    logger.info("\n%s", df_books_sample)
    logger.info("Interactions sample:")
    logger.info("\n%s", df_interactions_sample)
except Exception as e:
    logger.error(f"Error fetching sample data: {e}", exc_info=True)
