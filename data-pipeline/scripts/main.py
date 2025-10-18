from google.cloud import bigquery, storage
from data_cleaning import clean_goodreads_df
import pandas as pd
import os

def connect_bigquery():
    # Use credentials file if available
    if os.path.exists("gcp_credentials.json"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_credentials.json"

    client = bigquery.Client(project="recommendation-system-475301")
    print("✅ Connected to BigQuery!")
    return client

def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    if os.path.exists("gcp_credentials.json"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_credentials.json"
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"✅ Uploaded {local_file_path} to gs://{bucket_name}/{destination_blob_name}")

def sample_query():
    client = connect_bigquery()

    query = """
        SELECT
        book_id,
        title,
        authors,
        average_rating,
        ratings_count,
        popular_shelves,
        description
        FROM `recommendation-system-475301.books.goodreads_books_mystery_thriller_crime`
        WHERE title IS NOT NULL
        LIMIT 10000
    """
    df = client.query(query).to_dataframe()
    print(df.head())
    return df

if __name__ == "__main__":
    raw_df = sample_query()
    cleaned_df = clean_goodreads_df(raw_df)
    print(cleaned_df.head())
    bucket_name = "cleaned_data" 
    destination_blob_name = "cleaned_data/cleaned_goodreads.csv"
    local_path = "../data/processed/goodreads_cleaned.csv"
    #upload_to_gcs(local_path, bucket_name, destination_blob_name)

