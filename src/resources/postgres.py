import psycopg2
from dagster import ConfigurableResource, InitResourceContext
from psycopg2.extras import RealDictCursor
from pydantic import Field, PrivateAttr


class PostgresResource(ConfigurableResource):
    connection_string: str = Field(description="PostgreSQL connection string")

    _conn: any = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._conn = psycopg2.connect(self.connection_string, cursor_factory=RealDictCursor)

    def connect(self):
        return self._conn

    def teardown_after_execution(self):
        if hasattr(self, "_conn"):
            self.conn_close()
