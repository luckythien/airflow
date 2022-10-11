import imp
from airflow import DAG
from datetime import datetime
import io
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator


    

default_args = {"owner":"airflow",
                "email_on_failure":True,
                "email_on_retry":"True",
                "email":{"zunhuynh1999@gmail.com"}}
with DAG(dag_id="test_workflow_for_email",start_date=datetime(2022,2,11),schedule_interval="@hourly",default_args=default_args) as dag:
    email_function = EmailOperator(
        task_id = "email_function",
        to = "zunhuynh1999@gmail.com",
        subject="Airflow success alert",
        html_content = """<h1>test email notification</h1>"""

    )
    email_function