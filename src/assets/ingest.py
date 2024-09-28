import requests
from dagster import Config, EnvVar, asset

from src.config import DataSourceConfig
from src.resources.s3 import S3Resource


@asset
def ingest_data(context, s3: S3Resource, config: DataSourceConfig):
    for source in config.sources:
        source_id = source["id"]
        url = source["url"]
        file_type = source["type"]

        # Fetch data
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from {url}")

        # Upload to S3
        s3.put_object(
            Bucket=EnvVar("BUCKET_NAME").get_value(),
            Key=f"raw_data/{source_id}.{file_type}",
            Body=response.content,
        )

        context.log.info(f"Uploaded raw data from {url} to S3")
