import os
import logging
from google.cloud import bigquery
from logger_setup import get_logger
import time
from datetime import datetime
from gender_guesser.detector import Detector
from tqdm import tqdm

class DataCleaning:
    
    def __init__(self):
        # Set Google Application Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.path.dirname(__file__), "gcp_credentials.json")
        
        # Logging configuration
        self.logger = get_logger("data_cleaning")

        self.median_numeric_cols = ["publication_year", "num_pages"]

        # BigQuery client
        self.client = bigquery.Client()
        self.project_id = self.client.project
        

    def clean_table(self, dataset_id: str, table_name: str, destination_table: str, apply_global_median: bool = False):

        try:
            self.logger.info(f"Starting cleaning for table: {dataset_id}.{table_name}")

            # Get all columns
            columns_info = self.client.query(f"""
                SELECT column_name, data_type
                FROM `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """).to_dataframe(create_bqstorage_client=False)
            self.logger.info(f"Retrieved {len(columns_info)} columns for table {table_name}")

            array_cols = [row['column_name'] for _, row in columns_info.iterrows() if row['data_type'].startswith('ARRAY')]
            string_cols = [row['column_name'] for _, row in columns_info.iterrows() if row['data_type'] in ('STRING', 'CHAR', 'TEXT')]
            bool_cols = [row['column_name'] for _, row in columns_info.iterrows() if row['data_type'] == 'BOOL']

            # Build select expressions
            select_exprs = []
            for _, row in columns_info.iterrows():
                col = row['column_name']

                if apply_global_median and col in self.median_numeric_cols:
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
                    FROM `{self.project_id}.{dataset_id}.{table_name}`
                ),
                global_medians AS (
                    SELECT
                        {', '.join([f'APPROX_QUANTILES({col}, 2)[OFFSET(1)] AS {col}_median' for col in self.median_numeric_cols])}
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
                FROM `{self.project_id}.{dataset_id}.{table_name}`
                """

            # Execute query and save to destination table
            self.logger.info(f"Executing cleaning query for {table_name}...")
            job_config = bigquery.QueryJobConfig(
                destination=destination_table,
                write_disposition="WRITE_TRUNCATE"
            )
            self.client.query(query, job_config=job_config).result()
            self.logger.info(f" Cleaned table saved: {destination_table}")

        except Exception as e:
            self.logger.error(f"Error cleaning table {dataset_id}.{table_name}: {e}", exc_info=True)

    def run(self):
        # Clean books and interactions tables
        self.logger.info("=" * 60)
        self.logger.info("Good Reads Data Cleaning Pipeline")
        start_time = time.time()
        self.logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)
        self.clean_table(
            dataset_id="books",
            table_name="goodreads_books_mystery_thriller_crime",
            destination_table=f"{self.project_id}.books.goodreads_books_cleaned",
            apply_global_median=True
        )

        self.clean_table(
            dataset_id="books",
            table_name="goodreads_interactions_mystery_thriller_crime",
            destination_table=f"{self.project_id}.books.goodreads_interactions_cleaned",
            apply_global_median=False
        )

        # Fetch and log sample rows from cleaned tables
        try:
            df_books_sample = self.client.query(
                f"SELECT * FROM `{self.project_id}.books.goodreads_books_cleaned` LIMIT 5"
            ).to_dataframe(create_bqstorage_client=False)

            df_interactions_sample = self.client.query(
                f"SELECT * FROM `{self.project_id}.books.goodreads_interactions_cleaned` LIMIT 5"
            ).to_dataframe(create_bqstorage_client=False)

            self.logger.info("Books sample:")
            self.logger.info("\n%s", df_books_sample)
            self.logger.info("Interactions sample:")
            self.logger.info("\n%s", df_interactions_sample)
        except Exception as e:
            self.logger.error(f"Error fetching sample data: {e}", exc_info=True)
            print("Books sample:")
        self.create_author_gender_map()
        end_time = time.time()
        self.logger.info("=" * 60)
        self.logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Total runtime: {(end_time - start_time):.2f} seconds")
        self.logger.info("=" * 60)

    # --- 1️⃣ Load authors table from BigQuery ---
    def create_author_gender_map(self):
        """Generate and upload author gender mapping table to BigQuery."""
        try:
            self.logger.info("Starting gender mapping for authors...")

            # 1️⃣ Load authors table dynamically using project from GCP creds
            query = f"""
                SELECT author_id, name
                FROM `{self.project_id}.books.goodreads_book_authors`
                WHERE name IS NOT NULL
            """
            authors_df = self.client.query(query).to_dataframe()
            self.logger.info(f"Retrieved {len(authors_df)} author rows.")

            # 2️⃣ Infer gender locally
            detector = Detector(case_sensitive=False)

            def get_gender(name):
                if not name or '.' in name or len(name.split()) == 0:
                    return "Unknown"
                g = detector.get_gender(name.split()[0])
                if g in ["male", "mostly_male"]:
                    return "Male"
                elif g in ["female", "mostly_female"]:
                    return "Female"
                else:
                    return "Unknown"

            tqdm.pandas(desc="Inferring author gender")
            authors_df["author_gender_group"] = authors_df["name"].progress_apply(get_gender)

            # ⃣Upload mapping back to BigQuery
            table_id = f"{self.project_id}.books.goodreads_author_gender_map"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = self.client.load_table_from_dataframe(authors_df, table_id, job_config=job_config)
            job.result()  # Wait for upload to complete
            self.logger.info(f"Uploaded {len(authors_df)} rows to {table_id}")
            self.logger.info("Uploaded gender map to books.goodreads_author_gender_map")

        except Exception as e:
            self.logger.error(f"Error creating author gender map: {e}", exc_info=True)


def main():
    data_cleaner = DataCleaning()
    data_cleaner.run()

if __name__ == "__main__":
    main()
