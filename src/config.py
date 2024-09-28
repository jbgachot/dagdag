from dagster import Config
from typing import List, Dict

class DataSourceConfig(Config):
    sources: List[Dict[str, str]] = [
        {
            "id": "wa_gov_csv",
            "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv",
            "type": "csv"
        },
        {
            "id": "wa_gov_json",
            "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.json",
            "type": "json"
        }
    ]