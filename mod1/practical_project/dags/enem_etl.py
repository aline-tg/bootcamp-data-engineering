#First project of Module 1 from IGTI data engineering course
#Developer: Aline Guerreiro

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import zipfile

#fixed parameters
data_path = '~/data/microdados_enem_2019/DADOS/'
enem_initial_file = data_path + 'MICRODADOS_ENEM_2019.csv'

# default arguments
default_args = {
    'owner': 'Aline',
    'depends_on_past': False,
    'start_date': datetime(2020,11,22,13),
    'email': ['alinetg@msn.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

#DAG flow
dag = DAG(
    'practical-project-enem-data',
    description = 'Extract ENEM 2019 data, transform data and ingest this in sql db',
    default_args = default_args,
    schedule_interval = None
)

#methods
#unzip your file
def unzip_file():
    with zipfile.ZipFile("/root/microdados_enem_2019.zip", "r") as zipped:
        zipped.extractall("/root/enem-data")

#filter columns and resize number of rows
def resizing_data():
    #selection only interest fields
    cols = ["NU_INSCRICAO", 
            "NU_NOTA_CN",
            "NU_NOTA_CH",
            "NU_NOTA_LC",
            "NU_NOTA_MT",
            "TP_SEXO",
            "NU_IDADE",
            "SG_UF_RESIDENCIA",
            "NO_MUNICIPIO_RESIDENCIA",
            "TP_LINGUA",
            "IN_SURDEZ",
            "IN_DISLEXIA",
            "Q001",
            "Q002",
            "Q005",
            "Q006",
            "Q016",
            "Q021"]
    enem_data = pd.read_csv(file, sep = ";", 
                            decimal = ',', 
                            encoding = 'latin-1', 
                            usecols= cols)
    #getting only info from MG
    enem_data = enem_data.loc[
        (enem_data.SG_UF_RESIDENCIA == 'MG')
    ]

    #saving new file with less rows and less fields :) 
    enem_data.to_csv(data_path + 'mg_data.csv',
                     index=False,
                     encoding="latin-1")

def common_fields():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO',
                                    "NU_NOTA_CN",
                                    "NU_NOTA_CH",
                                    "NU_NOTA_LC",
                                    "NU_NOTA_MT",
                                    "NO_MUNICIPIO_RESIDENCIA",
                                    "TP_SEXO"])
    field_data.to_csv(data_path + 'common_fields.csv', 
                                    index=False)


#transforming foreign language field
def forlanguage():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','TP_LINGUA'])
    field_data['forlang'] = field_data.TP_LINGUA.replace({
        0: 'English',
        1: 'Spanish'
    })
    field_data[['NU_INSCRICAO','forlang']].to_csv(data_path + 'forlang.csv', 
                                    index=False)

def deafness():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','IN_SURDEZ'])
    field_data['deafness'] = field_data.IN_SURDEZ.replace({
        0: 'N',
        1: 'Y'
    })
    field_data[['NU_INSCRICAO','deafness']].to_csv(data_path + 'deafness.csv', 
                                    index=False)

def dyslexia():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','IN_DISLEXIA'])
    field_data['dyslexia'] = field_data.IN_DISLEXIA.replace({
        0: 'N',
        1: 'Y'
    })
    field_data[['NU_INSCRICAO','dyslexia']].to_csv(data_path + 'dyslexia.csv', 
                                    index=False)
def father_school_dummie():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','Q001'])
    field_data['fatherschool'] = field_data.Q001.replace({
        'A': 0,
        'B': 0,
        'C': 0,
        'D': 0,
        'E': 0,
        'F': 0,
        'G': 1,
        'H': ""
    })
    field_data[['NU_INSCRICAO','fatherschool']].to_csv(data_path + 'fatherschool.csv', 
                                        index=False)

def mother_school_dummie():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','Q002'])
    field_data['motherschool'] = field_data.Q002.replace({
        'A': 0,
        'B': 0,
        'C': 0,
        'D': 0,
        'E': 0,
        'F': 0,
        'G': 1,
        'H': ""
    })
    field_data[['NU_INSCRICAO','motherschool']].to_csv(data_path + 'motherschool.csv', 
                                        index=False)

def alone_living_dummie():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','Q005'])
    field_data['alone_living'] = field_data.Q005.replace({
        1: 1,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
        7: 0,
        8: 0,
        9: 0,
        10: 0, 
        11: 0,
        12: 0,
        13: 0, 
        14: 0,
        15: 0,
        16: 0,
        17: 0,
        18: 0,
        19: 0,
        20: 0
    })
    field_data[['NU_INSCRICAO','alone_living']].to_csv(data_path + 'alone_living.csv', 
                                        index=False)

def familyincome():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','Q006'])
    field_data['familyincome'] = field_data.Q006.replace({
        'A': 'No income',
        'B': 'to R$ 998,00',
        'C': 'From R$ 998,01 to R$ 1.497,00',
        'D': 'From R$ 1.497,01 to R$ 1.996,00',
        'E': 'From R$ 1.996,01 to R$ 2.495,00',
        'F': 'From R$ 2.495,01 to R$ 2.994,00',
        'G': 'From R$ 2.994,01 to R$ 3.992,00',
        'H': "From R$ 3.992,01 to R$ 4.990,00",
        'I': "From R$ 4.990,01 to R$ 5.988,00.",
        'J': 'From R$ 5.988,01 to R$ 6.986,00',
        'K': 'From R$ 6.986,01 to R$ 7.984,00',
        'L': 'From R$ 7.984,01 to R$ 8.982,00',
        'M': 'From R$ 8.982,01 to R$ 9.980,00',
        'N': 'From R$ 9.980,01 to R$ 11.976,00',
        'O': "From R$ 11.976,01 to R$ 14.970,00",
        'P': 'From R$ 14.970,01 to R$ 19.960,00',
        'Q': "More than R$ 19.960,00",
        ' ': ""
    })

    field_data[['NU_INSCRICAO','familyincome']].to_csv(data_path + 'familyincome.csv', 
                                        index=False)
    
def microwavesnumber():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','Q016'])
    field_data['microwavesnumber'] = field_data.Q016.replace({
        'A': '0',
        'B': '1',
        'C': '2',
        'D': '3',
        'E': '+4',
        ' ': ""
    })

    field_data[['NU_INSCRICAO','microwavesnumber']].to_csv(data_path + 'microwavesnumber.csv', 
                                        index=False)
    
def tvsignature():
    field_data = pd.read_csv(data_path + 'mg_data.csv', 
                          usecols=['NU_INSCRICAO','Q021'])
    field_data['tvsignature'] = field_data.Q021.replace({
        'A': 'N',
        'B': 'Y',
        ' ': ""
    })

    field_data[['NU_INSCRICAO','tvsignature']].to_csv(data_path + 'tvsignature.csv', 
                                        index=False)

#task join
def join_data():
    commonfields = pd.read_csv(data_path + 'common_fields.csv')
    gradesdata = pd.read_csv(data_path + 'grades_fields.csv')
    forlang = pd.read_csv(data_path + 'forlang.csv')
    deafness = pd.read_csv(data_path + 'deafness.csv')
    dyslexia = pd.read_csv(data_path + 'dyslexia.csv')
    fatherschool = pd.read_csv(data_path + 'fatherschool.csv')
    motherschool = pd.read_csv(data_path + 'motherschool.csv')
    aloneliving = pd.read_csv(data_path + 'alone_living.csv')
    familyincome = pd.read_csv(data_path + 'familyincome.csv')
    microwavesnumber = pd.read_csv(data_path + 'microwavesnumber.csv')
    tvsignature = pd.read_csv(data_path + 'tvsignature.csv')

    df_final = pd.concat([
        commonfields,
        gradesdata,
        forlang,
        deafness,
        dyslexia,
        fatherschool,
        motherschool,
        aloneliving,
        familyincome,
        microwavesnumber,
        tvsignature], 
        join = "inner",
        keys = ['NU_INSCRICAO'], axis=1)

    df_final.to_csv(data_path + 'enem_mg_treated.csv', index=False)

def compute_questions():
     enem_data = pd.read_csv(data_path + 'enem_mg_treated.csv')

     #Q1 - avg math grades
     #Q2 - avg language and code grades
     print(enem_data.agg({
        "NU_NOTA_MT": "mean",
        "NU_NOTA_LC": "mean"
        }))
     #Q3 - avg human grades, feminine gender
     #Q4 - avg human grades, masculine gender
     print(enem_data.groupby('TP_SEXO').agg({
            "NU_NOTA_CH": "mean"
         }))

     #Q5 - avg math grade, feminine, Montes Claros City
     print(enem_data.loc[
        (enem_data.TP_SEXO == "F") &
        (enem_data.NO_MUNICIPIO_RESIDENCIA == "Montes Claros"),
        "NU_NOTA_MT"
        ].mean())

     #Q6 - avg math grade, Municipe Sabará, signature TV
     print(enem_data.loc[
        (enem_data.tvsignature == "Y") &
        (enem_data.NO_MUNICIPIO_RESIDENCIA == "Sabará"),
        "NU_NOTA_MT"
        ].mean())
     #Q7 - avg humans grade, 2 microwaves
     print(enem_data.loc[
        (enem_data.microwavesnumber == '2'),
        "NU_NOTA_CH"
        ].mean())
     #Q8 - avg math grade, mother pos-grade 
     print(enem_data.loc[
        (enem_data.motherschool == '1'),
        "NU_NOTA_MT"
        ].mean())
     #Q9 - avg math grade, BH and conselheiro lafaiete
     print(enem_data.loc[
        (enem_data.NO_MUNICIPIO_RESIDENCIA.isin(["Conselheiro Lafaiete","Belo Horizonte"])),
        "NU_NOTA_MT"
    ].mean())
     #Q10 - avg humans grade, alone living
     print(enem_data.loc[
         (enem_data.alone_living == 1),
        "NU_NOTA_CH"
        ].mean())
     #Q11 - avg humans grade, father pos-grade, family income R$ 8.982,01 e R$ 9.980,00.
     print(enem_data.loc[(enem_data.fatherschool == '1') &
        (enem_data.familyincome == 'From R$ 8.982,01 to R$ 9.980,00'),
        "NU_NOTA_CH"].mean())
     #Q12 - avg math grade, Lavras city , Spanish for language
     print(enem_data.loc[(enem_data.TP_SEXO == 'F') &
         (enem_data.NO_MUNICIPIO_RESIDENCIA == 'Lavras') &
        (enem_data.forlang == 'Spanish'),
        "NU_NOTA_MT"].mean())
     #Q13 - avg Math grade,  masculin , Ouro Preto
     print(enem_data.loc[(enem_data.TP_SEXO == 'M') &
        (enem_data.NO_MUNICIPIO_RESIDENCIA == 'Ouro Preto'),
        "NU_NOTA_MT"].mean())
     #Q14 - avg human grade, deafness 
     print(enem_data.loc[
         (enem_data.deafness == 'Y'),
        "NU_NOTA_CH"].mean())
     #Q15 - avg Math grade, female, BH, Sabará, Nova Lima and Betim, dyslexia
     print(enem_data.loc[
        (enem_data.NO_MUNICIPIO_RESIDENCIA.isin(["Belo Horizonte","Sabará", "Nova Lima", "Betim"])) &
        (enem_data.dyslexia == 'Y') & 
        (enem_data.TP_SEXO == 'F'),
        "NU_NOTA_MT"
        ].mean())

# def writeDL(sql):
#     if sql = 1
#         final = pd.read_csv(data_path + 'enade_final.csv')
#         engine = sqlalchemy.create_engine('mssql+pyodbc://@localhost/enade?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
#         final.to_sql("treated", con = engine, index=False, if_exists='append')
#     else 

# Tasks
#reading Enem data from 2019
get_data = BashOperator(
    task_id = 'get-full-enem-data',
    bash_command = 'curl http://download.inep.gov.br/microdados/microdados_enem_2019.zip -o ~/microdados_enem_2019.zip',
    dag=dag
)

unzip_data = PythonOperator(
    task_id = 'unzip_data',
    python_callable = unzip_file,
    dag = dag
)

task_resizing_data = PythonOperator(
    task_id = 'resizing_data',
    python_callable = resizing_data,
    dag = dag
)

task_common_fields = PythonOperator(
    task_id = 'common_fields',
    python_callable = common_fields,
    dag = dag
)

task_foreigner = PythonOperator(
    task_id = 'foreigner_field',
    python_callable = forlanguage,
    dag = dag
)

task_deafness = PythonOperator(
    task_id = 'deafness_field',
    python_callable = deafness,
    dag = dag
)

task_dyslexia = PythonOperator(
    task_id = 'dyslexia_field',
    python_callable = dyslexia,
    dag = dag
)

task_father_school = PythonOperator(
    task_id = 'father_school_field',
    python_callable = father_school_dummie,
    dag = dag
)

task_mother_school = PythonOperator(
    task_id = 'mother_school_field-data',
    python_callable = mother_school_dummie,
    dag = dag
)

task_alone_living = PythonOperator(
    task_id = 'alone_living_field',
    python_callable = alone_living_dummie,
    dag = dag
)

task_familyincome = PythonOperator(
    task_id = 'familyincome_field',
    python_callable = familyincome,
    dag = dag
)

task_microwavesnumber = PythonOperator(
    task_id = 'microwavesnumber_field',
    python_callable = microwavesnumber,
    dag = dag
)

task_tvsignature = PythonOperator(
    task_id = 'tvsignature_field',
    python_callable = tvsignature,
    dag = dag
)

task_join_data = PythonOperator(
    task_id = 'join_data',
    python_callable = join_data,
    dag = dag
)


task_compute_questions = PythonOperator(
    task_id = 'compute_questions',
    python_callable = compute_questions,
    dag = dag
)


get_data >> unzip_data >> task_resizing_data >> [task_common_fields,
                                                                    task_foreigner, 
                                                                    task_deafness, 
                                                                    task_dyslexia,
                                                                    task_father_school,
                                                                    task_mother_school,
                                                                    task_alone_living,
                                                                    task_familyincome,
                                                                    task_microwavesnumber,
                                                                    task_tvsignature]
task_join_data.set_upstream([ 
    task_common_fields,
    task_foreigner, 
    task_deafness, 
    task_dyslexia,
    task_father_school,
    task_mother_school,
    task_alone_living,
    task_familyincome,
    task_microwavesnumber,
    task_tvsignature
])

task_join_data >> task_compute_questions
