from airflow import DAG, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

with DAG(
        dag_id='spark_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        description='A simple DAG to run a PySpark job using SparkSubmitOperator',
) as dag:
    spark_submit_test = SparkSubmitOperator(
        task_id="test_id",
        application="/opt/app/src/job/test.py",
        conn_id="spark_connection",
        conf={
            "spark.master": "spark://etl-spark-master-1:7077",
        },
        verbose=True
    )

    spark_submit_test
