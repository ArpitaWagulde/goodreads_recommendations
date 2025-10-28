"""
Anomaly Detection using BigQuery for Goodreads Data Pipeline
Performs data quality validation using BigQuery SQL queries
"""

import os
import pandas as pd
from google.cloud import bigquery
from airflow.utils.email import send_email
from datapipeline.scripts.logger_setup import get_logger
import time
from datetime import datetime

class AnomalyDetection:
    
    def __init__(self):
        # Set Google Application Credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ.get("AIRFLOW_HOME")+"/gcp_credentials.json"
        
        # Logging configuration
        self.logger = get_logger("anomaly_detection")
        
        # BigQuery client
        self.client = bigquery.Client()
        self.project_id = self.client.project
        self.dataset = "books"

    def validate_data_quality(self, use_cleaned_tables=False):
        """
        Data quality validation using BigQuery SQL queries
        Args:
            use_cleaned_tables (bool): If True, validate cleaned tables; if False, validate source tables
        """
        try:
            validation_type = "cleaned" if use_cleaned_tables else "source"
            self.logger.info(f"Starting BigQuery data validation for {validation_type} tables...")
            
            # Validate books table
            books_success = self.validate_books_with_bigquery(use_cleaned_tables)
            
            # Validate interactions table  
            interactions_success = self.validate_interactions_with_bigquery(use_cleaned_tables)
            
            # Check overall success
            if not books_success or not interactions_success:
                self.send_failure_email(f"Data validation failed for {validation_type} tables - check logs for details")
                raise Exception(f"Data validation failed for {validation_type} tables - critical issues found")
            
            self.logger.info(f"All {validation_type} data quality validations passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {e}")
            raise

    def get_table_structure(self, table_name):
        """
        Get table structure from BigQuery INFORMATION_SCHEMA
        """
        try:
            columns_info = self.client.query(f"""
                SELECT column_name, data_type
                FROM `{self.project_id}.{self.dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """).to_dataframe(create_bqstorage_client=False)
            
            self.logger.info(f"Retrieved {len(columns_info)} columns for table {table_name}")
            return columns_info
        except Exception as e:
            self.logger.error(f"Error fetching table structure for {table_name}: {e}")
            return None

    def validate_books_with_bigquery(self, use_cleaned_tables=False):
        """
        Validate books table using BigQuery SQL queries
        Args:
            use_cleaned_tables (bool): If True, validate cleaned table; if False, validate source table
        """
        try:
            # Choose table based on validation type
            table_name = "goodreads_books_cleaned_staging" if use_cleaned_tables else "goodreads_books_mystery_thriller_crime"
            
            self.logger.info(f"Validating books table: {table_name}")
            
            # Check if table exists and get row count
            count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.project_id}.{self.dataset}.{table_name}`
            """
            count_result = self.client.query(count_query).to_dataframe(create_bqstorage_client=False)
            row_count = count_result['row_count'].iloc[0]
            
            if row_count == 0:
                self.logger.error("Books table is empty")
                return False
            
            self.logger.info(f"Books table has {row_count} rows")
            
            # Data quality validation queries
            if not use_cleaned_tables:
                validation_queries = [
                    {
                        "name": "Check for null book_id",
                        "query": f"""
                        SELECT COUNT(*) as null_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE book_id IS NULL
                        """,
                        "max_allowed": 0
                    },
                    {
                        "name": "Check for null title",
                        "query": f"""
                        SELECT COUNT(*) as null_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE title IS NULL
                        """,
                        "max_allowed": 0
                    },
                ]
            else:
                validation_queries = [
                    {
                        "name": "Check for null title",
                        "query": f"""
                        SELECT COUNT(*) as null_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE title_clean IS NULL
                        """,
                        "max_allowed": 0
                    },
                    {
                        "name": "Check publication_year range",
                        "query": f"""
                        SELECT COUNT(*) as invalid_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE publication_year IS NULL OR publication_year <= 0
                        """,
                        "max_allowed": 0
                    },
                    {
                        "name": "Check num_pages range",
                        "query": f"""
                        SELECT COUNT(*) as invalid_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE num_pages IS NULL OR num_pages <= 0
                        """,
                        "max_allowed": 0
                    }
                ]
            
            # Run validation queries (uniform handling for pre/post)
            all_passed = True
            for validation in validation_queries:
                try:
                    result = self.client.query(validation["query"]).to_dataframe(create_bqstorage_client=False)
                    invalid_count = result.iloc[0, 0]
                    if invalid_count > validation["max_allowed"]:
                        self.logger.error(f"{validation['name']}: {invalid_count} violations found (max allowed: {validation['max_allowed']})")
                        all_passed = False
                    else:
                        self.logger.info(f"{validation['name']}: PASSED ({invalid_count} violations)")
                except Exception as e:
                    self.logger.error(f"Error running validation '{validation['name']}': {e}")
                    all_passed = False
            
            if all_passed:
                self.logger.info("Books table validation passed")
                return True
            else:
                self.logger.error("Books table validation failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Error validating books table: {e}")
            return False

    def validate_interactions_with_bigquery(self, use_cleaned_tables=False):
        """
        Validate interactions table using BigQuery SQL queries
        Args:
            use_cleaned_tables (bool): If True, validate cleaned table; if False, validate source table
        """
        try:
            # Choose table based on validation type
            table_name = "goodreads_interactions_cleaned_staging" if use_cleaned_tables else "goodreads_interactions_mystery_thriller_crime"
            
            self.logger.info(f"Validating interactions table: {table_name}")
            
            # Check if table exists and get row count
            count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.project_id}.{self.dataset}.{table_name}`
            """
            count_result = self.client.query(count_query).to_dataframe(create_bqstorage_client=False)
            row_count = count_result['row_count'].iloc[0]
            
            if row_count == 0:
                self.logger.error("Interactions table is empty")
                return False
            
            self.logger.info(f"Interactions table has {row_count} rows")
            
            # Data quality validation queries
            if not use_cleaned_tables:
                validation_queries = [
                    {
                        "name": "Check for null user_id",
                        "query": f"""
                        SELECT COUNT(*) as null_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE user_id IS NULL
                        """,
                        "max_allowed": 0
                    },
                    {
                        "name": "Check for null book_id",
                        "query": f"""
                        SELECT COUNT(*) as null_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE book_id IS NULL
                        """,
                        "max_allowed": 0
                    },
                ]
            else:
                validation_queries = [
                    {
                        "name": "Check for null user_id",
                        "query": f"""
                        SELECT COUNT(*) as null_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE user_id_clean IS NULL
                        """,
                        "max_allowed": 0
                    },
                    {
                        "name": "Check for null book_id",
                        "query": f"""
                        SELECT COUNT(*) as null_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE book_id IS NULL
                        """,
                        "max_allowed": 0
                    },
                    {
                        "name": "Check rating range",
                        "query": f"""
                        SELECT COUNT(*) as invalid_count
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        WHERE rating < 0 OR rating > 5
                        """,
                        "max_allowed": 0
                    }
                ]
            
            # Run validation queries (uniform handling for pre/post)
            all_passed = True
            for validation in validation_queries:
                try:
                    result = self.client.query(validation["query"]).to_dataframe(create_bqstorage_client=False)
                    invalid_count = result.iloc[0, 0]
                    if invalid_count > validation["max_allowed"]:
                        self.logger.error(f"{validation['name']}: {invalid_count} violations found (max allowed: {validation['max_allowed']})")
                        all_passed = False
                    else:
                        self.logger.info(f"{validation['name']}: PASSED ({invalid_count} violations)")
                except Exception as e:
                    self.logger.error(f"Error running validation '{validation['name']}': {e}")
                    all_passed = False
            
            if all_passed:
                self.logger.info("Interactions table validation passed")
                return True
            else:
                self.logger.error("Interactions table validation failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Error validating interactions table: {e}")
            return False

    

    def send_failure_email(self, message):
        """
        Send email notification for validation failures
        """
        try:
            subject = "[CRITICAL] Data Validation Failed - Goodreads Pipeline"
            
            html_content = f"""
            <h2>Data Validation Failure</h2>
            <p><strong>Pipeline:</strong> Goodreads Recommendation System</p>
            <p><strong>Status:</strong> FAILED - Pipeline stopped</p>
            <p><strong>Error:</strong> {message}</p>
            
            <p><strong>Action Required:</strong> Please investigate and fix the data quality issues before re-running the pipeline.</p>
            <p><em>This is an automated alert from the Goodreads Data Pipeline.</em></p>
            """
            
            send_email(to=os.environ.get("AIRFLOW__SMTP__SMTP_USER"), subject=subject, html_content=html_content)
            
            self.logger.info("Validation failure email sent")
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")

    def run_pre_validation(self):
        """
        Run pre-cleaning validation
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Pre-Cleaning Data Validation Pipeline")
            start_time = time.time()
            self.logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("=" * 60)
            
            result = self.validate_data_quality(use_cleaned_tables=False)
            
            end_time = time.time()
            self.logger.info("=" * 60)
            self.logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"Total runtime: {(end_time - start_time):.2f} seconds")
            self.logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in pre-cleaning validation: {e}")
            raise

    def run_post_validation(self):
        """
        Run post-cleaning validation
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Post-Cleaning Data Validation Pipeline")
            start_time = time.time()
            self.logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("=" * 60)
            
            result = self.validate_data_quality(use_cleaned_tables=True)
            
            end_time = time.time()
            self.logger.info("=" * 60)
            self.logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"Total runtime: {(end_time - start_time):.2f} seconds")
            self.logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in post-cleaning validation: {e}")
            raise

def main_pre_validation():
    """
    Pre-cleaning validation function - validates source tables
    """
    anomaly_detector = AnomalyDetection()
    return anomaly_detector.run_pre_validation()

def main_post_validation():
    """
    Post-cleaning validation function - validates cleaned tables
    """
    anomaly_detector = AnomalyDetection()
    return anomaly_detector.run_post_validation()

def main(use_cleaned_tables=False):
    """
    Main function called by Airflow DAG
    Args:
        use_cleaned_tables (bool): If True, validate cleaned tables; if False, validate source tables
    """
    anomaly_detector = AnomalyDetection()
    if use_cleaned_tables:
        return anomaly_detector.run_post_validation()
    else:
        return anomaly_detector.run_pre_validation()

if __name__ == "__main__":
    main()