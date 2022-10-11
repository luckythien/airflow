from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators import PostgresOperator

from datetime import datetime

with DAG('parallel_dag',start_date=datetime(2022,1,1),
        schedule_interval='@daily', catchup=False) as dag:
    
    extract_a =  BashOperator(
        task_id = 'extract_a',
        bash_command='sleep 10'
    )

    extract_b = BashOperator(
        task_id = 'extract_b',
        bash_command = 'sleep 10'
    )

    load_a = BashOperator(
        task_id = 'load_a',
        bash_command='sleep 10'
    )

    load_b = BashOperator(
        task_id = 'load_b',
        bash_command = 'sleep 10'
    )
    transform = BashOperator(
        task_id = 'transform',
        queue = 'high_cpu',
        bash_command='sleep 30'
    )
    task_create = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
            create table if not exists dag_runs(
                dt date,
                dag_id int
            );
        """
    )
    task_create 
    # extract_a >> load_a
    # extract_b >> load_b
    # [load_a,load_b] >> transform