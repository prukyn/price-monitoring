import json 
from pathlib import Path

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.S3_hook import S3Hook

@task
def write_text_file():

    curdir = Path().cwd() / "tmp" / "silpo"

    s3 = S3Hook('minio-storage')
    
    # for file_path in filter(Path.is_file, curdir.glob("*/*/*")):
    #     print(file_path)
    #     print(file_path.parts[-3:])

    print(
        json.load(s3.get_key("GROCERIES_65/2022-09-26/501_600.json", "silpo-api-data").get()["Body"])  
    )

@dag(
    schedule_interval="@once",
    start_date=datetime(2022, 10, 5, 11, 0, 0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
def minio_test_dag():
# Create a task to call your processing function
    t1 = write_text_file()

    t1 >> EmptyOperator(task_id="everything_is_okay")

dag = minio_test_dag()