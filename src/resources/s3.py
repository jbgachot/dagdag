import os
import boto3
from dagster import ConfigurableResource
from pydantic import Field

class S3Resource(ConfigurableResource):
    # aws_access_key_id: str = Field(description="AWS Access Key ID")
    # aws_secret_access_key: str = Field(description="AWS Secret Access Key")
    region_name: str = Field(description="AWS Region")

    def setup_resource(self):
        if os.environ.get("DAGSTER_ENV") == "dev":
            # Use LocalStack for local development
            return boto3.client(
                "s3",
                endpoint_url="http://localhost:4566",
                # aws_access_key_id="test",
                # aws_secret_access_key="test",
                region_name=self.region_name,
            )
        else:
            # Use actual AWS S3 for production
            return boto3.client(
                "s3",
                region_name=self.region_name
            )
        # self.client = boto3.client(
        #     's3',
        #     aws_access_key_id=self.aws_access_key_id,
        #     aws_secret_access_key=self.aws_secret_access_key,
        #     region_name=self.region_name
        # )

    def put_object(self, **kwargs):
        return self.client.put_object(**kwargs)

    def get_object(self, **kwargs):
        return self.client.get_object(**kwargs)
