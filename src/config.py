from typing import dict, list

from dagster import Config


class DataSourceConfig(Config):
    sources: list[dict[str, str]] = [
        {"id": "wa_gov_csv", "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv", "type": "csv"},
        {"id": "wa_gov_json", "url": "https://data.wa.gov/api/views/f6w7-q2d2/rows.json", "type": "json"},
    ]
