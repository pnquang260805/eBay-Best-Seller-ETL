import clickhouse_connect

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_sql_file(**kwargs):
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username="admin", password="26082005qa", database="warehouse")
    filename = kwargs['filename']
    with open(f'/opt/app/src/sql/{filename}.sql', 'r') as f:
        query = f.read()
    client.command(query)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 13),
}

with DAG(
    dag_id="create_dwh",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    description="This task will create data warehouse",
) as dag:
    create_dim_category = PythonOperator(
        task_id="dim_category_creation",
        python_callable=run_sql_file,
        op_kwargs={"filename":"create_dim_category"}
    ),
    create_dim_date = PythonOperator(
        task_id="dim_date_creation",
        python_callable=run_sql_file,
        op_kwargs={"filename":"create_dim_date"}
    ),
    create_dim_item = PythonOperator(
        task_id="dim_item_creation",
        python_callable=run_sql_file,
        op_kwargs={"filename":"create_dim_item"}
    ),
    create_dim_seller = PythonOperator(
        task_id="dim_seller_creation",
        python_callable=run_sql_file,
        op_kwargs={"filename":"create_dim_seller"}
    ),
    create_fact = PythonOperator(
        task_id="fact_creation",
        python_callable=run_sql_file,
        op_kwargs={"filename":"create_fact"}
    )

    [create_fact, create_dim_category, create_dim_item, create_dim_seller, create_dim_date]


