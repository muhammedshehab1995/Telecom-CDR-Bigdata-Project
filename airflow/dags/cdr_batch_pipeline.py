from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os

default_args = {
    'owner': 'muhammedshehab1995',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def upload_to_s3_task():
    """Upload processed Parquet files from HDFS export to S3."""
    import subprocess
    subprocess.run([
        "python3", "/opt/airflow/scripts/upload_to_s3.py",
        "--source", "/mnt/data",
        "--bucket", os.getenv("CDR_S3_BUCKET", "cdr-telecom-data-lake-dev"),
        "--region", os.getenv("AWS_DEFAULT_REGION", "me-south-1")
    ], check=True)

with DAG(
    'cdr_batch_pipeline',
    default_args=default_args,
    description='CDR batch ELT: Spark processing + S3 upload — Egypt Telecom',
    schedule_interval='@daily',
    catchup=False,
    tags=['cdr', 'batch', 'egypt', 's3']
) as dag:

    spark_ingestion = SparkSubmitOperator(
        task_id='spark_ingest_and_clean',
        application='/opt/spark-apps/cdr_batch_job.py',
        conn_id='spark_default',
        executor_memory='4g',
        driver_memory='2g',
        name='cdr_batch_ingestion'
    )

    spark_features = SparkSubmitOperator(
        task_id='spark_feature_engineering',
        application='/opt/spark-apps/spark-etl.py',
        conn_id='spark_default',
        executor_memory='4g',
        driver_memory='2g',
        name='cdr_feature_engineering'
    )

    upload_s3 = PythonOperator(
        task_id='upload_analytics_to_s3',
        python_callable=upload_to_s3_task
    )

    spark_ingestion >> spark_features >> upload_s3
