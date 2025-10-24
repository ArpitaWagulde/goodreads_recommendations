# feature_engineering.py

import os
import logging
from google.cloud import bigquery
from datetime import datetime
import pandas as pd

# 1️⃣ Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# 2️⃣ GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_credentials.json"

# 3️⃣ BigQuery client and configuration
project_id = "recommendation-system-475301"
dataset_id = "books"
client = bigquery.Client(project=project_id)

# 4️⃣ Source and destination tables
books_table = f"{project_id}.{dataset_id}.goodreads_books_cleaned"
interactions_table = f"{project_id}.{dataset_id}.goodreads_interactions_cleaned"
destination_table = f"{project_id}.{dataset_id}.goodreads_features_cleaned"

# 5️⃣ Feature engineering parameters
MIN_READING_DAYS = 1
MAX_READING_DAYS = 365
DEFAULT_PAGE_COUNT = 300
DEFAULT_READING_DAYS = 14


# 6️⃣ Main feature engineering function
def create_features():
    try:
        logger.info(f"Starting feature engineering pipeline")
        logger.info(f"Source tables: {books_table}, {interactions_table}")
        logger.info(f"Destination: {destination_table}")

        # Build the feature engineering query
        query = f"""
        -- ==============================================================
        -- BOOK-LEVEL FEATURES (including average reading time per book)
        -- ==============================================================
        WITH book_reading_times AS (
          -- Calculate average reading time for each book across all readers
          SELECT
            book_id,
            AVG(
              CASE 
                WHEN SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean) IS NOT NULL 
                 AND SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean) IS NOT NULL
                 AND DATE_DIFF(
                   DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean)),
                   DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean)),
                   DAY
                 ) BETWEEN {MIN_READING_DAYS} AND {MAX_READING_DAYS}
                THEN DATE_DIFF(
                  DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean)),
                  DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean)),
                  DAY
                )
                ELSE NULL
              END
            ) AS avg_book_reading_time,
            COUNT(
              CASE 
                WHEN SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean) IS NOT NULL 
                 AND SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean) IS NOT NULL
                THEN 1 
              END
            ) AS num_readers_with_time
          FROM `{interactions_table}`
          WHERE is_read = TRUE
          GROUP BY book_id
        ),

        base_books AS (
          SELECT
            b.book_id,
            COALESCE(b.title_clean, '') AS title_clean,
            COALESCE(b.average_rating, 0.0) AS average_rating,
            COALESCE(b.ratings_count, 0) AS ratings_count,
            COALESCE(b.text_reviews_count, 0) AS text_reviews_count,
            LOG(1 + COALESCE(b.ratings_count, 0)) AS log_ratings_count,
            COALESCE(b.ratings_count, 0) + COALESCE(b.text_reviews_count, 0) AS popularity_score,
            LENGTH(COALESCE(b.title_clean, '')) AS title_length_in_characters,
            ARRAY_LENGTH(SPLIT(COALESCE(b.title_clean, ''), ' ')) AS title_length_in_words,
            LENGTH(COALESCE(b.description_clean, '')) AS description_length,
            COALESCE(ARRAY_LENGTH(b.popular_shelves_flat), 0) AS num_genres,
            COALESCE(ARRAY_LENGTH(b.series_flat) > 0, FALSE) AS is_series,

            -- Pages and publication year features
            COALESCE(b.num_pages, {DEFAULT_PAGE_COUNT}) AS num_pages,
            COALESCE(b.publication_year, EXTRACT(YEAR FROM CURRENT_DATE()) - 5) AS publication_year,

            -- Book age calculation
            EXTRACT(YEAR FROM CURRENT_DATE()) - COALESCE(b.publication_year, EXTRACT(YEAR FROM CURRENT_DATE()) - 5) AS book_age_years,

            -- Book-level average reading time
            COALESCE(rt.avg_book_reading_time, {DEFAULT_READING_DAYS}) AS avg_book_reading_time_days,
            COALESCE(rt.num_readers_with_time, 0) AS num_readers_with_reading_time,

            -- Adjusted rating (Bayesian average)
            CASE
              WHEN COALESCE(b.ratings_count, 0) = 0 THEN 3.0
              ELSE (
                1 + 4 * (
                  ((COALESCE(b.average_rating, 3.0) - 1)/4) + POW(1.96,2)/(2*b.ratings_count)
                  - 1.96 * SQRT((( (COALESCE(b.average_rating, 3.0) - 1)/4)*(1 - (COALESCE(b.average_rating, 3.0) - 1)/4) + POW(1.96,2)/(4*b.ratings_count)) / b.ratings_count)
                ) / (1 + POW(1.96,2)/b.ratings_count)
              )
            END AS adjusted_average_rating
          FROM `{books_table}` b
          LEFT JOIN book_reading_times rt ON b.book_id = rt.book_id
          WHERE b.book_id IS NOT NULL
        ),

        threshold AS (
          SELECT 
            APPROX_QUANTILES(adjusted_average_rating, 100)[OFFSET(80)] AS rating_threshold
          FROM base_books
        ),

        book_final AS (
          SELECT
            b.*,
            COALESCE(b.adjusted_average_rating >= t.rating_threshold, FALSE) AS great,
            COALESCE(SAFE_DIVIDE(
              b.popularity_score - MIN(b.popularity_score) OVER(),
              NULLIF(MAX(b.popularity_score) OVER() - MIN(b.popularity_score) OVER(), 0)
            ), 0.5) AS book_popularity_normalized,

            -- Book difficulty indicator based on reading time
            CASE 
              WHEN avg_book_reading_time_days <= 7 THEN 'quick_read'
              WHEN avg_book_reading_time_days <= 14 THEN 'moderate'
              WHEN avg_book_reading_time_days <= 30 THEN 'long_read'
              ELSE 'very_long'
            END AS reading_pace_category,

            -- Pages-based categories
            CASE 
              WHEN num_pages <= 200 THEN 'short'
              WHEN num_pages <= 350 THEN 'medium'
              WHEN num_pages <= 500 THEN 'long'
              ELSE 'very_long'
            END AS book_length_category,

            -- Average pages per day (for books with reading time data)
            COALESCE(
              SAFE_DIVIDE(num_pages, NULLIF(avg_book_reading_time_days, 0)),
              25
            ) AS avg_pages_per_day,

            -- Era classification
            CASE 
              WHEN book_age_years <= 2 THEN 'new_release'
              WHEN book_age_years <= 5 THEN 'recent'
              WHEN book_age_years <= 10 THEN 'contemporary'
              WHEN book_age_years <= 20 THEN 'modern'
              ELSE 'classic'
            END AS book_era
          FROM base_books b CROSS JOIN threshold t
        ),

        -- ==============================================================
        -- USER-LEVEL FEATURES
        -- ==============================================================
        user_features AS (
          SELECT
            user_id_clean,
            COALESCE(COUNTIF(is_read), 0) AS num_books_read,
            COALESCE(AVG(IF(rating > 0, rating, NULL)), 3.0) AS avg_rating_given,
            COUNT(book_id) AS user_activity_count,
            COALESCE(
              DATE_DIFF(
                CURRENT_DATE(),
                DATE(MAX(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', date_updated_clean))),
                DAY
              ), 
              365
            ) AS recent_activity_days,
            -- User's personal average reading speed
            COALESCE(
              AVG(
                CASE 
                  WHEN SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean) IS NOT NULL 
                   AND SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean) IS NOT NULL
                   AND DATE_DIFF(
                     DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean)),
                     DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean)),
                     DAY
                   ) BETWEEN {MIN_READING_DAYS} AND {MAX_READING_DAYS}
                  THEN DATE_DIFF(
                    DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean)),
                    DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean)),
                    DAY
                  )
                  ELSE NULL
                END
              ), 
              {DEFAULT_READING_DAYS}
            ) AS user_avg_reading_time_days
          FROM `{interactions_table}`
          WHERE user_id_clean IS NOT NULL
          GROUP BY user_id_clean
        ),

        -- ==============================================================
        -- INTERACTION-LEVEL FEATURES
        -- ==============================================================
        interaction_features AS (
          SELECT
            user_id_clean,
            book_id,
            COALESCE(rating, 0) AS rating,
            COALESCE(is_read, FALSE) AS is_read,
            COALESCE(
              CASE 
                WHEN SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean) IS NOT NULL 
                 AND SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean) IS NOT NULL
                 AND DATE_DIFF(
                   DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean)),
                   DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean)),
                   DAY
                 ) BETWEEN {MIN_READING_DAYS} AND {MAX_READING_DAYS}
                THEN DATE_DIFF(
                  DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean)),
                  DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', started_at_clean)),
                  DAY
                )
                ELSE NULL
              END,
              {DEFAULT_READING_DAYS}
            ) AS user_days_to_read,
            COALESCE(
              DATE_DIFF(
                CURRENT_DATE(),
                DATE(SAFE.PARSE_TIMESTAMP('%a %b %d %H:%M:%S %z %Y', read_at_clean)),
                DAY
              ),
              365
            ) AS user_book_recency
          FROM `{interactions_table}`
          WHERE user_id_clean IS NOT NULL AND book_id IS NOT NULL
        ),

        -- ==============================================================
        -- FINAL MERGE
        -- ==============================================================
        merged AS (
          SELECT
            i.*,
            u.num_books_read,
            u.avg_rating_given,
            u.user_activity_count,
            u.recent_activity_days,
            u.user_avg_reading_time_days,
            b.title_clean,
            b.average_rating,
            b.adjusted_average_rating,
            b.great,
            b.ratings_count,
            b.log_ratings_count,
            b.popularity_score,
            b.book_popularity_normalized,
            b.num_genres,
            b.is_series,
            b.title_length_in_characters,
            b.title_length_in_words,
            b.description_length,

            -- Pages and publication features
            b.num_pages,
            b.publication_year,
            b.book_age_years,
            b.book_length_category,
            b.book_era,
            b.avg_pages_per_day,

            -- Reading time features
            b.avg_book_reading_time_days,
            b.num_readers_with_reading_time,
            b.reading_pace_category,

            COALESCE(u.avg_rating_given - b.adjusted_average_rating, 0.0) AS user_avg_rating_vs_book,

            -- Compare user's reading speed to average for this book
            COALESCE(
              SAFE_DIVIDE(i.user_days_to_read, NULLIF(b.avg_book_reading_time_days, 0)),
              1.0
            ) AS user_reading_speed_ratio,

            -- User's pages per day for this specific book
            COALESCE(
              SAFE_DIVIDE(b.num_pages, NULLIF(i.user_days_to_read, 0)),
              25
            ) AS user_pages_per_day_this_book
          FROM interaction_features i
          INNER JOIN user_features u ON i.user_id_clean = u.user_id_clean
          INNER JOIN book_final b ON i.book_id = b.book_id
        )

        SELECT * FROM merged
        WHERE 
          num_books_read > 0
          AND ratings_count > 0
          AND num_pages > 0
          AND publication_year > 1900
          AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE())
        """

        # Execute the query
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition="WRITE_TRUNCATE"
        )

        logger.info("Executing feature engineering query...")
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        logger.info(f"✅ Features table successfully created: {destination_table}")

    except Exception as e:
        logger.error(f"Error in feature engineering: {e}", exc_info=True)
        raise


# 7️⃣ Function to get table statistics
def get_table_stats():
    try:
        logger.info("Gathering table statistics...")
        stats_query = f"""
        SELECT 
          COUNT(*) as total_rows,
          COUNT(DISTINCT user_id_clean) as unique_users,
          COUNT(DISTINCT book_id) as unique_books,
          ROUND(AVG(num_books_read), 2) as avg_books_per_user,
          ROUND(AVG(avg_book_reading_time_days), 2) as avg_reading_time_days,
          ROUND(AVG(num_pages), 0) as avg_pages,
          ROUND(AVG(rating), 2) as avg_rating
        FROM `{destination_table}`
        """

        stats = client.query(stats_query).to_dataframe()
        logger.info("📊 Table Statistics:")
        logger.info(f"Total rows: {stats['total_rows'].iloc[0]:,}")
        logger.info(f"Unique users: {stats['unique_users'].iloc[0]:,}")
        logger.info(f"Unique books: {stats['unique_books'].iloc[0]:,}")
        logger.info(f"Avg books per user: {stats['avg_books_per_user'].iloc[0]}")
        logger.info(f"Avg reading time (days): {stats['avg_reading_time_days'].iloc[0]}")
        logger.info(f"Avg pages: {stats['avg_pages'].iloc[0]}")
        logger.info(f"Avg rating: {stats['avg_rating'].iloc[0]}")

    except Exception as e:
        logger.error(f"Error getting table stats: {e}", exc_info=True)


# 8️⃣ Function to export sample data
def export_sample(sample_size=1000):
    try:
        logger.info(f"Exporting sample of {sample_size} rows...")

        sample_query = f"""
        SELECT * 
        FROM `{destination_table}` 
        TABLESAMPLE SYSTEM (0.1 PERCENT)
        LIMIT {sample_size}
        """

        sample_df = client.query(sample_query).to_dataframe()

        # Create data directory if it doesn't exist
        os.makedirs("data", exist_ok=True)

        # Save as parquet
        output_path = f"data/features_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        sample_df.to_parquet(output_path, index=False)

        logger.info(f"✅ Sample saved to {output_path}")
        logger.info(f"   Shape: {sample_df.shape}")

        # Show sample
        logger.info("Sample data preview:")
        display_cols = ['user_id_clean', 'book_id', 'rating', 'num_pages', 'book_era']
        if all(col in sample_df.columns for col in display_cols):
            logger.info("\n%s", sample_df[display_cols].head())

    except Exception as e:
        logger.error(f"Error exporting sample: {e}", exc_info=True)


# 9️⃣ Main execution
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("GOODREADS FEATURE ENGINEERING PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now()}")

    # Create features
    create_features()

    # Get statistics
    get_table_stats()

    # Export sample
    export_sample(sample_size=1000)

    logger.info(f"Completed at: {datetime.now()}")
    logger.info("=" * 60)