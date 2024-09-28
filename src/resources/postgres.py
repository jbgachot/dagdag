import psycopg2
from dagster import ConfigurableResource
from psycopg2.extras import RealDictCursor
from pydantic import Field


class PostgresResource(ConfigurableResource):
    connection_string: str = Field(description="PostgreSQL connection string")

    def setup_resource(self):
        self.conn = psycopg2.connect(self.connection_string, cursor_factory=RealDictCursor)

    def connect(self):
        return self.conn

    def teardown_resource(self):
        if hasattr(self, "conn"):
            self.conn_close()
