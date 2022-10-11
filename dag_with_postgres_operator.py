from datetime import datetime, timedelta
from email.policy import default

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id= 'dag_with_postgres-operator_v02',
    default_args=default_args,
    start_date = datetime(2021,12,19),
    schedule_interval ='@daily'
) as dag:
    task1 = PostgresOperator(
            task_id='create_postgres_table',
            postgres_conn_id='postgres_default',
            sql = """
                create table if not exists test_postgres (
                    dt date,
                    dag_id character varying,
                    primary key (dt, dag_id)               
                    )
            """
    )
    task2 = PostgresOperator(
        task_id = 'insert_into_table',
        postgres_conn_id='postgres_default',
        sql= """
                    insert into dag_runs (dt,dag_id) values ('{{ds}}', '{{dag.dag_id}}' )
                    """
    )
    task1 >> task2