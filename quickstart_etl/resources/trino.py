import os
from dagster import ConfigurableResource
from dotenv import load_dotenv
from sqlalchemy import create_engine
from trino.sqlalchemy import URL
from sqlalchemy.engine import Engine


class TrinoDatabase(ConfigurableResource):

    def connection(self) -> Engine :
        """
        Establish a connection to the Trino database, NB: local doesn't require a password.
        see: https://jupysql.ploomber.io/en/latest/integrations/trinodb.html
        """ 
        load_dotenv()
        return create_engine(
            URL(
                host=os.getenv("TRINO_HOST", "localhost"),
                port=os.getenv("TRINO_PORT", "8080"),
                user=os.getenv("TRINO_USERNAME", "trino"),
                # Trino catalogs reference a data source/connector.
                # In this case, these are defined in ./trino/catalog
                catalog="memory",
                schema="default",
            )
        )
        # return connect(
        #     host=os.getenv("TRINO_HOST", "localhost"),
        #     port=os.getenv("TRINO_PORT", "8080"),
        #     user=os.getenv("TRINO_USERNAME", "trino")
        # )
