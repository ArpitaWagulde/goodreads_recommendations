from google.cloud import bigquery
import pandas as pd
import os

def connect_bigquery():
    # Use credentials file if available
    if os.path.exists("gcp_credentials.json"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_credentials.json"

    client = bigquery.Client(project="recommendation-system-475301")
    print("✅ Connected to BigQuery!")
    return client

def sample_query():
    client = connect_bigquery()

    query = """
    SELECT book_id, title, authors
    FROM `recommendation-system-475301.books.goodreads_books_mystery_thriller_crime`
    LIMIT 5
    """
    df = client.query(query).to_dataframe()
    print(df.head())
    return df

if __name__ == "__main__":
    sample_query()
