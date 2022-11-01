import logging
from typing import Any, List, Union
from airflow.models.operator import BaseOperator
from utils.silpo_utils import SilpoCategories, SiploBuckets
from hooks.silpo_hooks import SilpoHook

from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator


class SilpoGetAPIDataOperator(BaseOperator):
    
    def __init__(self, category: SilpoCategories, **kwargs) -> None:
        super().__init__(**kwargs)
        self.category = category
    
    def execute(self, context) -> Any:
        
        self.hook = SilpoHook(self.category, "minio-storage", SiploBuckets.API_BUCKET)

        self.hook.get_data()
        
        # context["ti"].xcom_push(key=f"{self.category.name.lower()}_raw_directory", value=str(self.hook.todays_date_path))

class SilpoReadStoredDataAndSaveToClickHouseOperator(BaseOperator):
    
    def __init__(self, category: SilpoCategories, **kwargs) -> None:
        super().__init__(**kwargs)
        self.category = category
    
    def execute(self, context) -> Any:
        
        self.hook = SilpoHook(self.category, "minio-storage", SiploBuckets.API_BUCKET)

        processed = self.hook.process_stored_data()

        self.hook.load_data(processed)

class SilpoGetLatestDateForCategoriesOperator(ClickHouseOperator):
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

class SilpoIngestAllFromBucketOperator(BaseOperator):
    
    def __init__(self, category: SilpoCategories, **kwargs) -> None:
        super().__init__(**kwargs)
        self.category = category
    
    def execute(self, context) -> Any:
        self.hook = SilpoHook(self.category, "minio-storage", SiploBuckets.API_BUCKET)
        self.hook.load_to_db()


class SiploIngestFromDateOperator(BaseOperator):
    
    def __init__(self, category: SilpoCategories, **kwargs) -> None:
        super().__init__(**kwargs)
        self.category = category

    def _return_right_date_for_category(self, data) -> str:
        print(data)
        print(list(filter(lambda x: x[0] == self.category.value, data)))
        return list(filter(lambda x: x[0] == self.category.value, data))[0][1]

    def execute(self, context) -> Any:
        self.hook = SilpoHook(self.category, "minio-storage", SiploBuckets.API_BUCKET)
        
        latest_date_str = self._return_right_date_for_category(
            context["ti"].xcom_pull(task_ids="latest_parse_date_for_category")
        )
        logging.info(f"Loading new records for {self.category} starts from {latest_date_str}")
        self.hook.load_to_db(date_from=latest_date_str)