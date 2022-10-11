from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import functools
from datetime import datetime, timedelta


def backup_database():
    import pymssql
    server =  Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server,user=username,password=password,database=database,port='1433',autocommit=True)
    cur = conn.cursor()
    query = """BACKUP DATABASE [Demo] TO DISK = N'Demo-23-09.bak'"""
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'coder2j',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="backup_job",
    start_date=datetime(2021, 10, 12),
    schedule_interval='@once',
) as dag:
    backup = PythonOperator(
        task_id = 'Backup_database_Demo',
        python_callable = backup_database,
        dag = dag
    )
    
backup