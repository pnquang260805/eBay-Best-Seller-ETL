from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.dag import dag

@dag(dag_id="create_dw", template_searchpath="/opt/app/src/sql")
def create_dwh():
    create_dwh_job = SQLExecuteQueryOperator(
        task_id="create_dw",
        conn_id="dwh",
        sql="create_dwh.sql"
    )

    create_dwh_job

create_dwh()