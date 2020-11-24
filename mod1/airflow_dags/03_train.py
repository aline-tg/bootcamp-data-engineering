#DAG that read external files and use conditionals

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

# default arguments

default_args = {
    'owner': 'Aline',
    'depends_on_past': False,
    'start_date': datetime(2020,11,22,11),
    'email': ['alinetg@msn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2) #@once, @daily, cron_tab
}

#DAG flow
dag = DAG(
    'train-03',
    description = 'Extract Titanic data and compute average age data to man or womans',
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

def random_gender():
    return random.choice(['male','female'])

choose_m_w = PythonOperator(
    task_id = 'choose-m-w',
    python_callable = random_gender,
    dag=dag
)

def M_or_W(**context):
    value = context['task_instance'].xcom_pull(task_ids='choose-m-w')
    if value == 'male':
        return 'branch_men'
    if value == 'female':
        return 'branch_women'

male_female = BranchPythonOperator(
    task_id = 'conditional',
    python_callable = M_or_W,
    provide_context = True,
    dag=dag
)

def avg_men():
    df = pd.read_csv('~/train.csv')
    df = df.loc[df.Sex == 'male']
    print(f'Men avg mean of age: {df.Age.mean()}')

branch_men = PythonOperator(
    task_id = 'branch_men',
    python_callable = avg_men,
    dag=dag
)

def avg_women():
    df = pd.read_csv('~/train.csv')
    df = df.loc[df.Sex == 'female']
    print(f'Women avg mean of age: {df.Age.mean()}')

branch_women = PythonOperator(
    task_id = 'branch_women',
    python_callable = avg_women,
    dag=dag
)

get_data >> choose_m_w >> male_female >> [branch_men,branch_women]

