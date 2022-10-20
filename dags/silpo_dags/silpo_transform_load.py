import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from operators.silpo_operators import SilpoGetLatestDateForCategoriesOperator, SilpoIngestAllFromBucketOperator, SiploIngestFromDateOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from utils.silpo_utils import SilpoCategories

@task.branch(task_id="condition")
def select_branch_based_on_condition_func(**kwargs):
    xcom_value = kwargs["ti"].xcom_pull(task_ids="latest_record_date_for_categories")
    if len(xcom_value) == 0:
        return ["ingest_all_dates"]
    
    return ["ingest_from_date"]



@dag(   
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 10, 18, 12, 0, 0, tz="UTC"),
    catchup=False,
)
def SilpoParseAndLoadToClickHouse():
    
    latest_date_record = SilpoGetLatestDateForCategoriesOperator(
        task_id="latest_record_date_for_categories",
        database="stage",
        sql=(
            """
            SELECT 
                arrayJoin(categories) as category,
                toString(max(parsed_date))
            FROM stage.silpo_products
            GROUP BY categories 
            """
        ),
        clickhouse_conn_id="clickhouse-connection"
    )
    #  >> PythonOperator(
    #     task_id="print_query_result",
    #     provide_context=True,
    #     python_callable=lambda task_instance, **_:
    #         print(task_instance.xcom_pull(task_ids="latest_record_date_for_categories"))
    # )


    branching = select_branch_based_on_condition_func()

    latest_date_record >> branching

    ingest_all_dates = EmptyOperator(task_id="ingest_all_dates")
    ingest_from_date = SiploIngestFromDateOperator(latest_date=None, category=None, task_id="ingest_from_date")

    branching >> ingest_all_dates
    branching >> ingest_from_date

    ingest_all_dates >> [SilpoIngestAllFromBucketOperator(category=category, task_id=f"silpo-{category.name.lower()}-load") for category in SilpoCategories]
    # ingest_all_dates >> [SilpoIngestAllFromBucketOperator(category=category, task_id=f"silpo-{category.name.lower()}-load") for category in [SilpoCategories.GROCERIES]]

    # tasks = [SilpoReadStoredDataAndSaveToClickHouseOperator(category=category, task_id=f"silpo-{category.name.lower()}-load_to_CH") for category in SilpoCategories]

    # tasks >> EmptyOperator(task_id="stored_successfully")
    # categories_extract_tasks = [SilpoGetAPIDataOperator(category=category, task_id=f"silpo-{category.name.lower()}-extract") for category in SilpoCategories]
    # all_extract_done = EmptyOperator(task_id="all_extracts_done")
    # # categories_parseJSON_tasks = [SilpoReadStoredDataOperator(category=category, task_id=f"silpo-{category.name.lower()}-parseJSON") for category in SilpoCategories]
    # # all_parseJSON_done = EmptyOperator(task_id="all_parseJSON_done")

    # categories_extract_tasks >> all_extract_done
    # all_extract_done >> categories_parseJSON_tasks
    # categories_parseJSON_tasks >> all_parseJSON_done


dag = SilpoParseAndLoadToClickHouse()