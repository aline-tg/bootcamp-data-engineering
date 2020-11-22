#first DAG with airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# default arguments

default_args = {
    'onwer': 'Aline',
    'depends_on_past': False,
    'start_date': datetime(2020,11,15,15),
    'email': ['alinetg@msn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

#DAG flow

dag = DAG(
    'train-01',
    description = 'Basic of Bash Operatores and Python Operators',
    default_args = default_args,
    schedule_interval = timedelta(minutes=2)
)

# Tasks
hello_bash = BashOperator(
    task_id = 'Hello_Bash',
    bash_command = 'echo "Hello Airflow from Python"',
    dag=dag
)

def say_hello():
    print('Hello Airflow from Python')

hello_python = PythonOperator(
    task_id = 'Hello_Python',
    python_callable = say_hello,
    dag=dag
)

hello_bash >> hello_python