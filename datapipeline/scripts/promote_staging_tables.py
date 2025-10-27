import os
from google.cloud import bigquery
from datapipeline.scripts.logger_setup import get_logger

class GoodreadsNormalization:

    def __init__(self):
        # Set Google credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ.get("AIRFLOW_HOME", ".") + "/gcp_credentials.json"

        # Logging configuration
        self.logger = get_logger("promote_staging_tables")

        self.client = bigquery.Client()
        self.project_id = self.client.project
        self.dataset_id = "books"
    
    def promote_staging_tables(self):
        """Promote staging tables to production by replacing the production tables."""
        try:
            staging_tables = {
                "goodreads_books_cleaned_staging": "goodreads_books_cleaned",
                "goodreads_interactions_cleaned_staging": "goodreads_interactions_cleaned",
                "goodreads_features_cleaned_staging": "goodreads_features"
            }

            for staging_table, prod_table in staging_tables.items():
                self.logger.info(f"Promoting {staging_table} to {prod_table}...")
                query = f"""
                CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.{prod_table}` AS
                SELECT * FROM `{self.project_id}.{self.dataset_id}.{staging_table}`;
                """
                self.client.query(query).result()
                self.logger.info(f"Successfully promoted {staging_table} to {prod_table}.")
            
                # Remove staging tables after promotion
                self.logger.info(f"Dropping staging table: {staging_table}...")
                drop_query = f"DROP TABLE `{self.project_id}.{self.dataset_id}.{staging_table}`;"
                self.client.query(drop_query).result()
                self.logger.info(f"Successfully dropped staging table: {staging_table}.")

        except Exception as e:
            self.logger.error("Error promoting staging tables.", exc_info=True)
            raise

    def run(self):
        self.promote_staging_tables()

def main():
    promoter = GoodreadsNormalization()
    promoter.run()

if __name__ == "__main__":
    main()