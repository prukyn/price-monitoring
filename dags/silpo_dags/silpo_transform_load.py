import datetime
import logging
from collections import defaultdict
import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from operators.silpo_operators import SilpoGetLatestDateForCategoriesOperator, SilpoIngestAllFromBucketOperator, SiploIngestFromDateOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from utils.silpo_utils import SilpoCategories

latest_parse_date_for_category_result = defaultdict(lambda: '1970-01-01')

@task.branch(task_id="condition")
def select_branch_based_on_condition_func(**kwargs):
    xcom_value = kwargs["ti"].xcom_pull(task_ids="latest_parse_date_for_category")
    if len(xcom_value) == 0:
        return ["ingest_all_dates"]
    
    return ["ingest_from_particular_date"]



@dag(   
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 10, 18, 12, 0, 0, tz="UTC"),
    catchup=False,
)
def SilpoParseAndLoadToClickHouse():
    
    latest_parse_date_for_category = SilpoGetLatestDateForCategoriesOperator(
        task_id="latest_parse_date_for_category",
        database="stage",
        sql=(
            f"""
            SELECT 
                category,
                toString(max(parsed_date))
            FROM stage.silpo_products
            ARRAY JOIN categories as category
            GROUP BY category 
            HAVING category in {tuple(str(el.value) for el in SilpoCategories)}
            """
        ),
        clickhouse_conn_id="clickhouse-connection"
    )

    branching = select_branch_based_on_condition_func()
    ingest_all_dates = EmptyOperator(task_id="ingest_all_dates")
    ingest_from_particular_date = EmptyOperator(task_id="ingest_from_particular_date")

    latest_parse_date_for_category >> branching
    branching >> ingest_all_dates
    branching >> ingest_from_particular_date
    
    ingest_all_dates >> [SilpoIngestAllFromBucketOperator(category=category, task_id=f"silpo-{category.name.lower()}-load") for category in SilpoCategories]
    
    for category in SilpoCategories:
        ingest_from_particular_date >> SiploIngestFromDateOperator(
            category=category,
            task_id=f"silpo-{category.name.lower()}-load-from-date"
        )


dag = SilpoParseAndLoadToClickHouse()