import io

import duckdb
from dagster import EnvVar, asset

from src.resources.s3 import S3Resource


@asset(deps=["ingest_data"])
def convert_data(context, s3: S3Resource):
    bucket_name = EnvVar("BUCKET_NAME").get_value()

    # List all objects in the raw_data prefix
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="raw_data/")

    for obj in objects.get("Contents", []):
        key = obj["Key"]
        file_type = key.split(".")[-1]

        # Download the raw file
        # raw_file = s3.get_object(Bucket=EnvVar("BUCKET_NAME").get_value(), Key=key)
        # raw_data = raw_file["Body"].read()
        table_name = "raw_data"

        # Create an in-memory DuckDB database
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        if EnvVar("ENV").get_value() == "dev":
            secret_query = """
                        CREATE SECRET s3config (
                            TYPE S3,
                            ENDPOINT 'localhost',
                            KEY_ID 'fake',
                            SECRET 'fake',
                            USE_SSL 'false',
                            URL_STYLE 'path'
                        );
                    """
        else:
            secret_query = """
                CREATE SECRET s3config (
                    TYPE S3,
                    PROVIDER CREDENTIAL_CHAIN
                );
            """

        con.execute(secret_query)
        # Load data into DuckDB
        if file_type == "csv":
            con.execute(f"""
                        CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('s3://{bucket_name}/{key}')
                        """)
        elif file_type == "json":
            con.execute(f"""
                        CREATE TABLE {table_name} AS SELECT * FROM read_json('s3://{bucket_name}/{key}', maximum_object_size=104857600)
                        """)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Convert to Parquet
        parquet_buffer = io.BytesIO()
        con.execute("COPY raw_data TO 'data.parquet' (FORMAT PARQUET)")
        with open("data.parquet", "rb") as f:
            parquet_buffer.write(f.read())
        parquet_buffer.seek(0)

        # Upload Parquet file to S3
        new_key = key.replace("raw_data", "parquet_data").rsplit(".", 1)[0] + ".parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=new_key,
            Body=parquet_buffer.getvalue(),
        )

        context.log.info(f"Converted {key} to Parquet and uploaded to S3")
