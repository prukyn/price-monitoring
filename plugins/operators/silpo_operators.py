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
    
     def __init__(self, category: SilpoCategories, latest_date: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.category = category
        self.latest_date_str = latest_date