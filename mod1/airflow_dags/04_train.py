#parallelization of tasks

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import zipfile

#fixed parameters
data_path = '~/data/microdados_enade_2019/2019/3.DADOS/'
print(data_path)
file = data_path + 'microdados_enade_2019.txt'

# default arguments
default_args = {
    'owner': 'Aline',
    'depends_on_past': False,
    'start_date': datetime(2020,11,22,13),
    'email': ['alinetg@msn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2) #@once, @daily, cron_tab
}

#DAG flow
dag = DAG(
    'train-04',
    description = 'Parallelism',
    default_args = default_args,
    schedule_interval = "*/10 * * * *"
)

# Tasks
start_processing = BashOperator(
    task_id = 'start_processing',
    bash_command='echo "Start Preprocesing!"',
    dag=dag
)

get_data = BashOperator(
    task_id = 'get-data',
    bash_command = 'curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o ~/microdados_enade_2019.zip',
    dag=dag
)

def unzip_file():
    with zipfile.ZipFile("/root/microdados_enade_2019.zip", "r") as zipped:
        zipped.extractall("/root/data")

unzip_data = PythonOperator(
    task_id = 'unzip_data',
    python_callable = unzip_file,
    dag = dag
)

def apply_filters():
    cols = ["CO_GRUPO", "TP_SEXO","NU_IDADE","NT_GER","NT_FG","NT_CE",
          "QE_I01", "QE_I02", "QE_I04","QE_I05","QE_I08"]
    enade = pd.read_csv(file, sep = ";", decimal = ',', usecols= cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0) 
    ]
    enade.to_csv(data_path + 'filtered_enade.csv', index=False)

task_apply_filter = PythonOperator(
    task_id = 'apply_filter',
    python_callable = apply_filters,
    dag = dag
)

#Age centered on mean
def centered_age():
    age = pd.read_csv(data_path + 'filtered_enade.csv', usecols = ['NU_IDADE'])
    age['idadecent'] = age.NU_IDADE - age.NU_IDADE.mean()
    age[['idadecent']].to_csv(data_path + 'age_cent.csv', index=False)

#Age centered squared
def centered_age_squared():
    agecent = pd.read_csv(data_path + 'age_cent.csv')
    agecent['idade2'] = agecent.idadecent ** 2
    agecent[['idade2']].to_csv(data_path + 'age_squared.csv', index=False)

task_age_cent = PythonOperator(
    task_id = 'compute-age-cent',
    python_callable = centered_age,
    dag = dag
)

task_age_cent_squared = PythonOperator(
    task_id = 'compute-age-cent-squared',
    python_callable = centered_age_squared,
    dag = dag
)

def civil_state():
    filters = pd.read_csv(data_path + 'filtered_enade.csv', usecols=['QE_I01'])
    filters['estcivil'] = filters.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    filters[['estcivil']].to_csv(data_path + 'civil_state.csv', index=False)

task_civil_state = PythonOperator(
    task_id = 'compute-civil-state',
    python_callable = civil_state,
    dag = dag
)

def skin_color():
    filters = pd.read_csv(data_path + 'filtered_enade.csv', usecols=['QE_I02'])
    filters['cor'] = filters.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': "",
        ' ': "",
    })
    filters[['cor']].to_csv(data_path + 'skin_color.csv', index=False)

task_skin_color = PythonOperator(
    task_id = 'compute-skin-color',
    python_callable = skin_color,
    dag = dag
)

#task join
def join_data():
    filter2 = pd.read_csv(data_path + 'filtered_enade.csv')
    agecent = pd.read_csv(data_path + 'age_cent.csv')
    agecentsquared = pd.read_csv(data_path + 'age_squared.csv')
    civilstate = pd.read_csv(data_path + 'civil_state.csv')
    skincolor = pd.read_csv(data_path + 'skin_color.csv')

    df_final =pd.concat([
        filter2, agecent, agecentsquared, civilstate, skincolor
    ], axis=1)

    df_final.to_csv(data_path + 'enade_final.csv', index=False)
    print(df_final)

task_join_data = PythonOperator(
    task_id = 'join-data',
    python_callable = join_data,
    dag = dag
)

start_processing >> get_data >> unzip_data >> task_apply_filter
task_apply_filter >> [task_age_cent, task_civil_state, task_skin_color]
task_age_cent_squared.set_upstream(task_age_cent)

task_join_data.set_upstream([
    task_civil_state,task_skin_color
])