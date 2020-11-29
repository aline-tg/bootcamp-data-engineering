import prefect 
from prefect import task,Flow

@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello,Cloud!")

with Flow("de-course") as flow:
    hello_task()

flow.register(project_name = "de-course")
flow.run_agent(token = "")