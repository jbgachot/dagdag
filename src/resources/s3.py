import boto3
from dagster import ConfigurableResource, EnvVar, InitResourceContext
from pydantic import Field, PrivateAttr


class S3Resource(ConfigurableResource):
    region_name: str = Field(description="AWS Region")

    _client: any = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        if EnvVar("ENV").get_value() == "dev":
            # Use LocalStack for local development
            self._client = boto3.client(
                "s3",
                endpoint_url="http://localhost",
                # This is mandatory to avoid boto3 "Unable to locate credentials" error
                aws_access_key_id="fake",
                aws_secret_access_key="fake",
                region_name=self.region_name,
            )
        else:
            # Use actual AWS S3 for production
            self._client = boto3.client("s3", region_name=self.region_name)

    def put_object(self, **kwargs):
        return self._client.put_object(**kwargs)

    def get_object(self, **kwargs):
        return self._client.get_object(**kwargs)

    def list_objects_v2(self, **kwargs):
        return self._client.list_objects_v2(**kwargs)
