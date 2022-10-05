import datetime
from pathlib import Path
import logging
from typing import Any
import json
import io

from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.S3_hook import S3Hook

from utils.silpo_utils import SilpoCategories, SiploBuckets
from utils.common import chunkize

class SilpoHook(HttpHook):
    
    headers = {
        "Content-Type": "application/json;chaset=UTF-8",
    }

    def __init__(self, category: SilpoCategories, storage_conn_name, bucket_name, **kwargs) -> None:
        super().__init__(method="POST", http_conn_id=None, **kwargs)
        self.category = category
        self.url = "https://api.catalog.ecom.silpo.ua/api/2.0/exec/EcomCatalogGlobal"

        self._target_dest = f"{self.category.name}_{self.category.value}/{datetime.date.today().strftime('%Y-%m-%d')}"
        self.bucket_name = bucket_name
        self._object_storage = S3Hook(storage_conn_name)

        self.parsed_data = []

    def _request_data(self, items_from=1, items_to=100) -> dict:
        return {
            "data": {
                "categoryId": self.category.value,
                "From": items_from,
                "To": items_to,
                "filialId":	2043,
                "sortBy": "popular-asc",
            },
            "method": "GetSimpleCatalogItems"
        }

    def get_items_count_in_category(self):
        response = self.run(self.url, data=self._request_data(), headers=self.headers)
        return response.get("itemsCount")

    def get_data(self):            
        response = self.run(self.url, json=self._request_data(), headers=self.headers).json()
        self._save_to_bucket(response, self.bucket_name, 1, 100)
        items_count = response.get("itemsCount")

        logging.info(f"Total: {items_count} products")
        logging.info(f"Products from: 1 to 100")

        for _from, _to in list(chunkize(items_count))[1:]:
            response = self.run(self.url, json=self._request_data(items_from=_from, items_to=_to), headers=self.headers).json()
            logging.info(f"Products from: {_from} to {_to}")
            
            self._save_to_bucket(response, self.bucket_name, _from, _to)

    def _save_to_bucket(self, data, bucket_name, items_from, items_to):
        file_path = f"{self._target_dest}/{items_from}_{items_to}.json"
        binary_data = json.dumps(data, ensure_ascii=False).encode("utf-8")
        with io.BytesIO(binary_data) as buffer:
            logging.info(f"Saving to bucket: {file_path}")
            self._object_storage.load_file_obj(
                file_obj=buffer, 
                key=file_path, 
                bucket_name=self.bucket_name,
                replace=True
            )
            logging.info(f"Saved to bucket: {file_path} - success")


    # def _save_data_locally(self, response, items_from, items_to):
    #     if not self.todays_date_path.exists():
    #         self.todays_date_path.mkdir(parents=True)
    #         logging.info("Today's path successfully created")

    #     file_path = self.todays_date_path / f"{items_from}_{items_to}.json"
        
    #     with open(file_path, "w") as file:
    #         json.dump(response, file, ensure_ascii=False)
    #         logging.info(f"Saved file: {file_path}")
    
    def _parse_json(self, data) -> dict:
        for item in data.get("items"):
            
            result_data = {
                "categories": [category.get("id") for category in item.get("categories")],
                "name": item.get("name"),
                "unit": item.get("unit"),
                "price": item.get("price"),
                "oldPrice": item.get("oldPrice"),
                "image": item.get("mainImage"),
                "page_link": f"https://shop.silpo.ua/product/{item.get('slug')}",
                "quantity": item.get("quantity")
            }
            if item.get("parameters") is None:
                logging.info(f"No params in {item.get('name')}")
                result_data.update({
                    "country": None,
                    "packageType": None,
                })
            else:
                for param in item.get("parameters", []):
                    result_data.update({
                        "country": param.get("country"),
                        "packageType": param.get("packageType"),
                    })
            if item.get("promotions") is None:
                result_data.update({
                        "promo_title": None,
                        "promo_startFrom": None,
                        "promo_stopAfter": None,
                        "promo_description": None,
                    })
            else:
                for promo in item.get("promotions"):
                    result_data.update({
                        "promo_title": promo.get("title"),
                        "promo_startFrom": promo.get("startFrom"),
                        "promo_stopAfter": promo.get("stopAfter"),
                        "promo_description": promo.get("description"),
                    })
            self.parsed_data.append(result_data)

    def process_stored_data(self, dir_path):
        dir_path = Path(dir_path)
        for file in filter(Path.is_file, dir_path.glob("*")):
            with open(file) as f:
                logging.info(f"Processing file: {file}")
                self._parse_json(json.load(f))
                self._save_parsed_file(dir_path)
    
    def _save_parsed_file(self, path):
        path = path / "filtered"
        if not path.exists():
            path.mkdir(parents=True)
        
        with open(path / "parsed.json", "w", encoding="utf-8") as file:
            json.dump(self.parsed_data, file, ensure_ascii=False) 

    
class SilpoCategoriesHook(HttpHook):

    headers = {"Content-Type": "application/json;chaset=UTF-8",}
    data = {
        "method": "GetMainPageCategories",
        "data": {
            "merchantId":1,
            "basketGuid":"",
            "filialId":2043,
            "deliveryType":2,
            "size":15
        }
    }

    def __init__(self):
        super().__init__(method="POST", http_conn_id=None, **kwargs)
        self.url = "https://api.catalog.ecom.silpo.ua/api/2.0/exec/EcomCatalogGlobal"
        self.silpo_dir_path = Path(f"/opt/airflow/tmp/silpo/categories")


    def _parse_categories(self, data: dict):
        headers = ["id", "name", "slug"]

        for item in data.get("items", []):
            parsed.append({
                "id": item.get("id"),
                "name": item.get("name"),
                "slug": item.get("slug")
            })

        return parsed

    def _save_categories(self, data: dict):
        pass

    def siplo_categories(self):
        response = self.run(self.url, json=self.data, headers=self.headers).json()




