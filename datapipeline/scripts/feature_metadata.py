import os
import json
from datetime import datetime
from datapipeline.scripts.logger_setup import get_logger
from google.cloud import bigquery

import os
import json
from datetime import datetime
from datapipeline.scripts.logger_setup import get_logger
from google.cloud import bigquery


class FeatureMetadata:

    def __init__(self):
        # Set Google Application Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ.get("AIRFLOW_HOME", ".") + "/gcp_credentials.json"

        # Logging configuration
        # use a descriptive logger name for this module
        self.logger = get_logger("feature_metadata")

        # BigQuery client
        self.client = bigquery.Client()
        self.project_id = self.client.project
        self.dataset_id = "books"

    def run(self):
        try:
            self.logger.info("Starting feature metadata collection")
            print("Starting feature metadata collection")
            query = f"SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.goodreads_features`"

            job = self.client.query(query)

            # Get table schema from BigQuery
            table = self.client.get_table(f"{self.project_id}.{self.dataset_id}.goodreads_features")
            schema = [
                {"name": field.name, "type": field.field_type, "mode": field.mode}
                for field in table.schema
            ]

            timestamp = datetime.now()

            # Get row count from query result
            row_count = job.to_dataframe(create_bqstorage_client=False).iloc[0, 0]

            metadata = {
                "table": f"{self.dataset_id}.goodreads_features",
                "schema": schema,
                "row_count": int(row_count) if row_count is not None else None,
                "query_hash": hash(query),
                "timestamp": timestamp.isoformat()
            }

            os.makedirs("data/metadata", exist_ok=True)
            metadata_path = "data/metadata/goodreads_features_metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
            self.logger.info("Wrote metadata to %s", metadata_path)
            print("Wrote metadata to %s", metadata_path)

            # timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
            # data_location = f"gs://goodreads-db/data/goodreads_features_{timestamp_str}.csv"

            # self.logger.info("Starting table extract to %s", data_location)
            # print("Starting table extract to %s", data_location)
            # extract_job = self.client.extract_table(f"{self.dataset_id}.goodreads_features", data_location)
            # extract_job.result()
            # self.logger.info("Table extract completed: %s", data_location)
            # print("Table extract completed: %s", data_location)
            # return data_location
        except Exception:
            self.logger.exception("Failed to run feature metadata collection")
            raise

def main():
    feature_metadata = FeatureMetadata()
    feature_metadata.run()

if __name__ == "__main__":
    main()