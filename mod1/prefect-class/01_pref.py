from datetime import datetime, timedelta
import prefect
from prefect import task,Flow
from prefect.schedules import IntervalSchedule
import pandas as pd

#setting retry and schedule
retry_delay = timedelta(minutes=1)
schedule = IntervalSchedule(interval=timedelta(minutes=2))

#reading data
@task
def get_data():
    df = pd.read_csv("https://raw.githubusercontent.com/A3DAta/hermione/master/hermione/file_text/train.csv")
    return df

#compute age avg
@task
def compute_age_avg(df):
    return df.Age.mean()

#print computed avg
@task
def show_computed_avg(m):
    logger = prefect.context.get("logger")
    logger.info(f"The age avg computed was {m}")

#print dataframe
@task
def show_dataframe(df):
    logger = prefect.context.get("logger")
    logger.info(df.head(3).to_json())

#specifying flow execution
with Flow("Titanic01",schedule = schedule) as flow:
    df = get_data()
    avg = compute_age_avg(df)
    print1 = show_computed_avg(avg)
    print2 = show_dataframe(df)

flow.register(project_name = "de-course", idempotency_key = flow.serialized_hash())
flow.run_agent(token = "")