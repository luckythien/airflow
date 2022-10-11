import airflow
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# my_var = Variable.get("server")

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

def display_variable():
    my_var = Variable.get("server")
    print('variable:'+ my_var)
    return my_var

with DAG(dag_id="display_variable",
            default_args = default_args,
            schedule_interval="@daily" ) as dag:
    
    task = PythonOperator(
        task_id = "display_variable",
        python_callable = display_variable
    )

    task