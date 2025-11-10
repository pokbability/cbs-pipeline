from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

from src.ingestion import Ingestion
from src.transform import Transformation
from src.load import Load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_ingestion():
    ingestion = Ingestion()
    ingestion.ingest_all()

def run_transformation():
    transform = Transformation()
    transform.transform_all()

def run_load():
    load = Load()
    load.create_star_schema()

with DAG(
    'cbs_pipeline',
    default_args=default_args,
    description='Coventry Building Society ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=run_ingestion,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=run_transformation,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=run_load,
    )

    # Set task dependencies
    ingest_task >> transform_task >> load_task


