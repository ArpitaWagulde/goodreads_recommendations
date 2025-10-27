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
            table_name = "goodreads_books_cleaned_stage" if use_cleaned_tables else "goodreads_books_mystery_thriller_crime"
            
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
                    "name": "Check average_rating range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE average_rating < 0 OR average_rating > 5
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check publication_year range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE publication_year < 1000 OR publication_year > 2030
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check num_pages range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE num_pages < 1 OR num_pages > 10000
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check ratings_count range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE ratings_count < 0
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check publication_month range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE publication_month < 1 OR publication_month > 12
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check publication_day range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE publication_day < 1 OR publication_day > 31
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check for duplicate book_id",
                    "query": f"""
                    SELECT COUNT(*) as duplicate_count
                    FROM (
                        SELECT book_id, COUNT(*) as cnt
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        GROUP BY book_id
                        HAVING cnt > 1
                    )
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check missing value percentage",
                    "query": f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) as null_title,
                        SUM(CASE WHEN average_rating IS NULL THEN 1 ELSE 0 END) as null_rating,
                        SUM(CASE WHEN publication_year IS NULL THEN 1 ELSE 0 END) as null_year
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    """,
                    "max_allowed": 0.2  # Allow up to 20% missing values
                }
            ]
            
            # Run validation queries
            all_passed = True
            for validation in validation_queries:
                try:
                    result = self.client.query(validation["query"]).to_dataframe(create_bqstorage_client=False)
                    
                    if validation["name"] == "Check missing value percentage":
                        # Special handling for missing value check
                        total_rows = result['total_rows'].iloc[0]
                        null_title = result['null_title'].iloc[0]
                        null_rating = result['null_rating'].iloc[0]
                        null_year = result['null_year'].iloc[0]
                        
                        title_missing_pct = null_title / total_rows if total_rows > 0 else 0
                        rating_missing_pct = null_rating / total_rows if total_rows > 0 else 0
                        year_missing_pct = null_year / total_rows if total_rows > 0 else 0
                        
                        if title_missing_pct > validation["max_allowed"]:
                            self.logger.error(f"Title missing percentage {title_missing_pct:.2%} exceeds threshold {validation['max_allowed']:.2%}")
                            all_passed = False
                        if rating_missing_pct > validation["max_allowed"]:
                            self.logger.error(f"Rating missing percentage {rating_missing_pct:.2%} exceeds threshold {validation['max_allowed']:.2%}")
                            all_passed = False
                        if year_missing_pct > validation["max_allowed"]:
                            self.logger.error(f"Year missing percentage {year_missing_pct:.2%} exceeds threshold {validation['max_allowed']:.2%}")
                            all_passed = False
                            
                        self.logger.info(f"Missing value check - Title: {title_missing_pct:.2%}, Rating: {rating_missing_pct:.2%}, Year: {year_missing_pct:.2%}")
                    else:
                        # Standard validation check
                        invalid_count = result.iloc[0, 0]  # First column, first row
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
            table_name = "goodreads_interactions_cleaned_stage" if use_cleaned_tables else "goodreads_interactions_mystery_thriller_crime"
            
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
                {
                    "name": "Check rating range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE rating < 0 OR rating > 5
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check for duplicate user-book pairs",
                    "query": f"""
                    SELECT COUNT(*) as duplicate_count
                    FROM (
                        SELECT user_id, book_id, COUNT(*) as cnt
                        FROM `{self.project_id}.{self.dataset}.{table_name}`
                        GROUP BY user_id, book_id
                        HAVING cnt > 1
                    )
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check user_id range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE user_id < 1
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check book_id range",
                    "query": f"""
                    SELECT COUNT(*) as invalid_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    WHERE book_id < 1
                    """,
                    "max_allowed": 0
                },
                {
                    "name": "Check missing value percentage",
                    "query": f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) as null_user_id,
                        SUM(CASE WHEN book_id IS NULL THEN 1 ELSE 0 END) as null_book_id,
                        SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) as null_rating
                    FROM `{self.project_id}.{self.dataset}.{table_name}`
                    """,
                    "max_allowed": 0.1  # Allow up to 10% missing values for interactions
                },
                {
                    "name": "Check for orphaned book_id references",
                    "query": f"""
                    SELECT COUNT(*) as orphaned_count
                    FROM `{self.project_id}.{self.dataset}.{table_name}` i
                    LEFT JOIN `{self.project_id}.{self.dataset}.{table_name}` b
                    ON i.book_id = b.book_id
                    WHERE b.book_id IS NULL
                    """,
                    "max_allowed": 0  # No orphaned references allowed
                }
            ]
            
            # Run validation queries
            all_passed = True
            for validation in validation_queries:
                try:
                    result = self.client.query(validation["query"]).to_dataframe(create_bqstorage_client=False)
                    
                    if validation["name"] == "Check missing value percentage":
                        # Special handling for missing value check
                        total_rows = result['total_rows'].iloc[0]
                        null_user_id = result['null_user_id'].iloc[0]
                        null_book_id = result['null_book_id'].iloc[0]
                        null_rating = result['null_rating'].iloc[0]
                        
                        user_id_missing_pct = null_user_id / total_rows if total_rows > 0 else 0
                        book_id_missing_pct = null_book_id / total_rows if total_rows > 0 else 0
                        rating_missing_pct = null_rating / total_rows if total_rows > 0 else 0
                        
                        if user_id_missing_pct > validation["max_allowed"]:
                            self.logger.error(f"User ID missing percentage {user_id_missing_pct:.2%} exceeds threshold {validation['max_allowed']:.2%}")
                            all_passed = False
                        if book_id_missing_pct > validation["max_allowed"]:
                            self.logger.error(f"Book ID missing percentage {book_id_missing_pct:.2%} exceeds threshold {validation['max_allowed']:.2%}")
                            all_passed = False
                        if rating_missing_pct > validation["max_allowed"]:
                            self.logger.error(f"Rating missing percentage {rating_missing_pct:.2%} exceeds threshold {validation['max_allowed']:.2%}")
                            all_passed = False
                            
                        self.logger.info(f"Missing value check - User ID: {user_id_missing_pct:.2%}, Book ID: {book_id_missing_pct:.2%}, Rating: {rating_missing_pct:.2%}")
                    else:
                        # Standard validation check
                        invalid_count = result.iloc[0, 0]  # First column, first row
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
            
            send_email(
                to="7d936ad4-351b-4493-99a7-110ed7b6b2f6@emailhook.site",
                subject=subject,
                html_content=html_content
            )
            
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