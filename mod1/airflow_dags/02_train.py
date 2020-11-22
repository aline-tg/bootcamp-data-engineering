#first DAG with airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# default arguments

default_args = {
    'onwer': 'Aline',
    'depends_on_past': False,
    'start_date': datetime(2020,11,22,11),
    'email': ['alinetg@msn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1) #@once, @daily, cron_tab
}

#DAG flow
dag = DAG(
    'train-02',
    description = 'Extract Titanic data and compute average age data',
    default_args = default_args,
    schedule_interval = timedelta(minutes=2)  #@once, @daily, cron_tab
)

# Tasks
get_data = BashOperator(
    task_id = 'get-data',
    bash_command = 'curl https://raw.githubusercontent.com/A3DAta/hermione/master/hermione/file_text/train.csv -o ~/train.csv',
    dag=dag
)

def compute_avg_age():
    df = pd.read_csv('~/train.csv')
    med = df.Age.mean()
    return med

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids = 'compute-avg-age')
    print(f"The avg age of Titanic was {value} years old.")

task_mean_age = PythonOperator(
    task_id = 'compute-avg-age',
    python_callable = compute_avg_age,
    dag=dag
)

task_print_age = PythonOperator(
    task_id = 'show-age',
    python_callable = print_age,
    provide_context=True,
    dag=dag
)

get_data >> task_mean_age >> task_print_age

