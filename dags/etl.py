#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""
import os
import datetime
import json
from typing import Dict, List
from enum import Enum
import requests
import pendulum
from pathlib import Path
from airflow import DAG
from airflow.decorators import dag, task

import logging


class SilpoCategories(Enum):
    GROCERIES = 65


class SiploJsonReader:
    def __init__(self, category: SilpoCategories, folder_path: Path) -> None:
        self.category = category
        self.folder: Path = folder_path
        self.parsed_data: List[dict] = []

    def run(self):
        for file in filter(Path.is_file, self.folder.glob("*")):
            with open(file) as f:
                logging.info(f"Processing file: {file}")
                self.parse_json(json.load(f))
                self.save_file()

    def parse_json(self, data: dict) -> dict:
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

    def save_file(self):
        path = self.folder / "filtered"
        if not path.exists():
            path.mkdir(parents=True)
        
        with open(path / "parsed.json", "w", encoding="utf-8") as file:
            json.dump(self.parsed_data, file, ensure_ascii=False) 


class SilpoAPI:
    def __init__(
        self, 
        category: SilpoCategories, 
        items_from: int = 1, 
        items_to: int = 32, 
        filialId: int = 2043
    ) -> None:
        self.category: SilpoCategories = category
        self._items_from: int = items_from
        self._items_to: int = items_to
        self._filialId: int = filialId
        self.url = "https://api.catalog.ecom.silpo.ua/api/2.0/exec/EcomCatalogGlobal"
        self.items_count: int = 5000
        self.silpo_dir_path = Path(f"/opt/airflow/tmp/silpo/{self.category.name}_{self.category.value}")
        self.todays_date_path = self.silpo_dir_path / f"{datetime.date.today().strftime('%Y-%m-%d')}"
    
    def _generate_headers(self) -> dict:
        return {
            "Content-Type": "application/json;chaset=UTF-8",
        }
    
    def _generate_request_data(self) -> dict:
        return {
            "data": {
                "categoryId": self.category.value,
                "From": self._items_from,
                "To": self._items_to,
                "filialId": self._filialId,
                "sortBy": "popular-asc",
            },
            "method": "GetSimpleCatalogItems"
        }
    
    def chunkize(self, max_item, chunksize=100):
        _first = 1
        _second = chunksize
        while _first < max_item:
            yield [_first, _second]
            _first += chunksize
            _second += chunksize

    def request(self) -> dict:
        _request = requests.post(self.url, json=self._generate_request_data(), headers=self._generate_headers())

        if _request.status_code != 200:
            return {}
        
        _response = _request.json()
        self.items_count = _response.get("itemsCount")
        
        logging.info(f"Total: {self.items_count} products")
        logging.info(f"Products from: {self._items_from} to: {self._items_to}")

        return _response
    
    def make_dir(self):
        if not self.todays_date_path.exists():
            self.todays_date_path.mkdir(parents=True)
            logging.info("Today's path successfully created")

    def save_files(self, response):
        file_path = self.todays_date_path / f"{self._items_from}_{self._items_to}.json"

        with open(file_path, "w") as file:
            json.dump(response, file, ensure_ascii=False)
            logging.info("Saved file")

    def run(self):
        response = self.request()

        for _from, _to in self.chunkize(self.items_count):
            logging.info(f"Getting chunk {_from} - {_to}")

            self._items_from = _from
            self._items_to = _to
            
            response = self.request()

            self.make_dir()
            self.save_files(response)

    @classmethod
    def generate_requests(cls, category, items_from, items_to):
        return cls(category, items_from, items_to)


@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2022, 8, 17, 14, 0, 0, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def SiploProducts():

    @task
    def start():
        pass

    for category in SilpoCategories:
        @task(task_id=f"{category.name}-Siplo-SaveJSON")
        def get_api_data(instance):
            return instance.run()
        
        @task(task_id=f"{category.name}-Siplo-JSONReader")
        def json_reader(path):
            SiploJsonReader(category.name, path).run()

        instance = SilpoAPI(category)
        get_api_data_ = get_api_data(instance)
        json_reader_ = json_reader(instance.todays_date_path)
        start_ = start()
        start_ >> get_api_data_ >> json_reader_





dag = SiploProducts()
