from dagster import Definitions, EnvVar, load_assets_from_modules

from src import assets
from src.config import DataSourceConfig
from src.resources.postgres import PostgresResource
from src.resources.s3 import S3Resource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": S3Resource(region_name=EnvVar("AWS_REGION")),
        "postgres": PostgresResource(connection_string=EnvVar("POSTGRES_CONNECTION_STRING")),
    },
    config=DataSourceConfig,
)
