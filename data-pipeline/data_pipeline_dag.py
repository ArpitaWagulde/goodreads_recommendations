from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import os

from scripts.bigquery_full_data_cleaning import main as data_cleaning_main

# Default arguments for the DAG
default_args = {
    'owner': 'Arpita Wagulde',
    'start_date': datetime(2025, 1, 18),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

def log_query_results(**kwargs):
    ti = kwargs['ti']
    job_id = ti.xcom_pull(task_ids='read_data_from_bigquery')

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    import logging

    hook = BigQueryHook(gcp_conn_id="goodreads_conn")
    client = hook.get_client()

    query_job = client.get_job(job_id)
    rows = list(query_job.result())

    logging.info("✅ Query Results:")
    for row in rows:
        logging.info(dict(row))


with DAG(
    dag_id='goodreads_recommendation_pipeline',
    default_args=default_args,
    description='Goodreads Recommendation System Data Pipeline',
    catchup=False
) as dag:
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ.get("AIRFLOW_HOME")+"/gcp_credentials.json"

    start = EmptyOperator(task_id='start')

    data_reading_task = BigQueryInsertJobOperator(
        task_id='read_data_from_bigquery',
        configuration={
            "query": {
                "query": """
                    SELECT * FROM `recommendation-system-475301.books.goodreads_interactions_mystery_thriller_crime` LIMIT 10
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='goodreads_conn',
    )

    log_results_task = PythonOperator(
        task_id='log_bq_results',
        python_callable=log_query_results,
    )

    data_cleaning_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=data_cleaning_main,
    )

    end = EmptyOperator(task_id='end')

    start >> data_reading_task >> log_results_task >> data_cleaning_task >> end