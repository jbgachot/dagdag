import io

import duckdb
from dagster import EnvVar, asset

from src.resources.s3 import S3Resource


@asset
def convert_to_parquet(context, s3: S3Resource):
    # List all objects in the raw_data prefix
    objects = s3.list_objects_v2(Bucket=EnvVar("BUCKET_NAME"), Prefix="raw_data/")

    for obj in objects.get("Contents", []):
        key = obj["Key"]
        file_type = key.split(".")[-1]

        # Download the raw file
        raw_file = s3.get_object(Bucket=EnvVar("BUCKET_NAME"), Key=key)
        raw_data = raw_file["Body"].read()

        # Create an in-memory DuckDB database
        con = duckdb.connect(database=":memory:")

        # Load data into DuckDB
        if file_type == "csv":
            con.execute("CREATE TABLE raw_data AS SELECT * FROM read_csv_auto(?)", [raw_data])
        elif file_type == "json":
            con.execute("CREATE TABLE raw_data AS SELECT * FROM json_objects(?)", [raw_data])
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
            Bucket=EnvVar("BUCKET_NAME"),
            Key=new_key,
            Body=parquet_buffer.getvalue(),
        )

        context.log.info(f"Converted {key} to Parquet and uploaded to S3")
