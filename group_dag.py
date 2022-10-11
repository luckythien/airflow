from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from group.group_downloads import download_tasks
with DAG('group_dag', start_date=datetime(2022,1,1),
            schedule_interval='@daily',catchup=False) as dag:
    
    args = {'start_date': dag.start_date,'schedule_interval':dag.schedule_interval,'catchup':dag.catchup}
    downloads = download_tasks()
    
    check_files = BashOperator(
                task_id = 'check_files',
                bash_command = 'sleep 30')
    
    transform_a = BashOperator(
                task_id = 'transform_a',
                bash_command = 'sleep 30')
    
    transform_b = BashOperator(
                task_id = 'transform_b',
                bash_command = 'sleep 30')
    
    transform_c = BashOperator(
                task_id = 'transform_c',
                bash_command = 'sleep 30'
    )
    downloads >> check_files >> [transform_a,transform_b,transform_c]

                        