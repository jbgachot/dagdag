import io
import duckdb
from dagster import asset, EnvVar
from src.resources.s3 import S3Resource

@asset
def transform_data(context, s3:S3Resource):
    # List all Parquet files
    objects = s3.list_objects_v2(Bucket=EnvVar("BUCKET_NAME"), Prefix="parquet_data/")

    # Create an in-memory DuckDB database
    con = duckdb.connect(database=':memory:')

    # Load all Parquet files into DuckDB
    for obj in objects.get('Contents', []):
        key = obj['Key']
        con.execute(f"CREATE TABLE IF NOT EXISTS all_data AS SELECT * FROM parquet_scan('s3://my-bucket/{key}')")
        con.execute(f"INSERT INTO all_data SELECT * FROM parquet_scan('s3://my-bucket/{key}')")

    # Perform complex ETL operations
    result = con.execute("""
        SELECT 
            source_id,
            COUNT(*) as record_count,
            AVG(CASE WHEN TRY_CAST(column1 AS FLOAT) IS NOT NULL THEN CAST(column1 AS FLOAT) ELSE NULL END) as avg_column1
        FROM all_data
        GROUP BY source_id
    """).fetchdf()

    # Convert result to Parquet
    result_parquet_buffer = io.BytesIO()
    con.execute("COPY result TO 'result.parquet' (FORMAT PARQUET)")
    with open('result.parquet', 'rb') as f:
        result_parquet_buffer.write(f.read())
    result_parquet_buffer.seek(0)

    # Upload result to S3
    s3.put_object(
        Bucket=EnvVar("BUCKET_NAME"),
        Key="processed_data/etl_result.parquet",
        Body=result_parquet_buffer.getvalue(),
    )

    context.log.info(f"Completed complex ETL and uploaded results to S3")
