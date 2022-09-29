import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from operators.silpo_operators import SilpoGetAPIDataOperator, SilpoReadStoredDataOperator

from utils.silpo_utils import SilpoCategories

@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2022, 8, 17, 14, 0, 0, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def SilpoExtract():
    
    categories_extract_tasks = [SilpoGetAPIDataOperator(category=category, task_id=f"silpo-{category.name.lower()}-extract") for category in SilpoCategories]

    all_extract_done = EmptyOperator(task_id="all_extracts_done")

    categories_extract_tasks >> all_extract_done
    
    categories_parseJSON_tasks = [SilpoReadStoredDataOperator(category=category, task_id=f"silpo-{category.name.lower()}-parseJSON") for category in SilpoCategories]

    all_extract_done >> categories_parseJSON_tasks

    all_parseJSON_done = EmptyOperator(task_id="all_parseJSON_done")

    categories_parseJSON_tasks >> all_parseJSON_done




dag = SilpoExtract()