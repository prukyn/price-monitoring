from typing import Any, List, Union
from airflow.models.operator import BaseOperator
from utils.silpo_utils import SilpoCategories
from hooks.silpo_hooks import SilpoHook



class SilpoGetAPIDataOperator(BaseOperator):
    
    def __init__(self, category: SilpoCategories, **kwargs) -> None:
        super().__init__(**kwargs)
        self.category = category
    
    def execute(self, context) -> Any:
        
        self.hook = SilpoHook(self.category)

        self.hook.get_data()
        
        context["ti"].xcom_push(key=f"{self.category.name.lower()}_raw_directory", value=str(self.hook.todays_date_path))

class SilpoReadStoredDataOperator(BaseOperator):
    
    def __init__(self, category: SilpoCategories, **kwargs) -> None:
        super().__init__(**kwargs)
        self.category = category
    
    def execute(self, context) -> Any:
        
        self.hook = SilpoHook(self.category)
        dir_path = context["ti"].xcom_pull(
            task_ids=f"silpo-{self.category.name.lower()}-extract", 
            key=f"{self.category.name.lower()}_raw_directory",
            include_prior_dates=False
        )
        self.hook.process_stored_data(dir_path)
