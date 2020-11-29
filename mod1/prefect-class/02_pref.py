from datetime import datetime, timedelta
import prefect
import pendulum
from prefect import task,Flow
from prefect.schedules import CronSchedule
import pandas as pd
from io import BytesIO
import zipfile
import requests
import sqlalchemy
import pyodbc

#setting retry and schedule
retry_delay = timedelta(minutes=1)
schedule = CronSchedule(
    cron = "*/10 * * * *",
    start_date = pendulum.datetime(2020,11,29,11,53, tz="America/Sao_Paulo")
)

#reading data
@task
def get_raw_data():
    url = "http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip"
    filebytes = BytesIO(requests.get(url).content)
    myzip = zipfile.ZipFile(filebytes)
    myzip.extractall()
    path = '/mnt/c/Prefect/microdados_enade_2019/2019/3.DADOS/'
    return path

#applying some filters
def apply_filters(path):

    cols = ["CO_GRUPO", "TP_SEXO",
            "NU_IDADE","NT_GER",
            "NT_FG","NT_CE",
            "QE_I01", "QE_I02", 
            "QE_I04","QE_I05",
            "QE_I08"]

    enade = pd.read_csv('/mnt/c/Prefect/microdados_enade_2019/2019/3.DADOS/microdados_enade_2019.txt', 
                        sep = ';', 
                        decimal = ',', 
                        usecols= cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0) 
    ]

    return enade

#compute age avg
@task
def centered_age(df):
    age = df[['NU_IDADE']]
    age['idadecent'] = age.NU_IDADE - age.NU_IDADE.mean()
    return age[['idadecent']]

#compute centered age squared
@task
def compute_centered_age_squared(df):
    agecent = df.copy
    agecent['idade2'] = agecent.idadecent ** 2
    return agecent[['idade2']]

#civil_state
@task
def civil_state(df):
    filters =  df[['QE_I01']]
    filters['estcivil'] = filters.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    return filters['estcivil']

#transforming skin-color
@task
def skin_color(df):
    filters =  df[['QE_I02']] 
    filters['cor'] = filters.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': "",
        ' ': "",
    })
    return filters['cor']

#transforming father-school
@task
def father_school(df):
    filters =  df[['QE_I04']] 
    filters['escopai'] = filters.QE_I04.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    return filters[['escopai']]

#transforming mother_school
@task
def mother_school(df):
    filters =  df[['QE_I05']] 
    filters['escomae'] = filters.QE_I05.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    return filters[['escomae']]

#transforming month_amount
@task
def monthly_amount(df):
    filters =  df[['QE_I08']] 
    filters['renda'] = filters.QE_I08.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5,
        'G': 6
    })
    return filters[['renda']]

#join all columns created
@task
def join_data(df, centered_age, 
              #centered_age_squared,
              civil_state,
              skin_color,
              father_school,
              mother_school,
              monthly_amount):

    final = pd.concat([df, centered_age, 
              #centered_age_squared,
              civil_state,
              skin_color,
              father_school,
              mother_school,
              monthly_amount],axis = 1)

    final = final[['CO_GRUPO', 'TP_SEXO',
                    'idadecent',
                    #'idade2',
                    'estcivil',
                    'cor',
                    'escopai',
                    'escomae',
                    'renda']]
    logger = prefect.context.get('logger')
    logger.info(final.head().to_json())
    #final.to_csv("enade_tratado.csv", index=False)
    return final

#@task
#def write_dw(df):
#    engine = sqlalchemy.create_engine('mssql+pyodbc://@localhost/enade?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
#    df.to_sql("tratado", con=engine, index=False,if_exists="append",method="multi")

with Flow("Enade", schedule) as flow:
    path = get_raw_data()
    filters = apply_filters(path)
    centered_age = centered_age(filters)
    #centered_age_squared = compute_centered_age_squared(centered_age)
    civil_state = civil_state(filters)
    skin_color = skin_color(filters)
    father_school = father_school(filters)
    mother_school = mother_school(filters)
    monthly_amount = monthly_amount(filters)
    final_data = join_data(filters,
                           centered_age,
                           #centered_age_squared,
                           civil_state,
                           skin_color,
                           father_school,
                           mother_school,
                           monthly_amount)
    #write_dw(final_data)

flow.register(project_name = "de-course", idempotency_key = flow.serialized_hash())
flow.run_agent(token = "")