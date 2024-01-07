import os
import psycopg2
from dagster import ConfigurableResource
from dotenv import load_dotenv


class PagilaDatabase(ConfigurableResource):

    def connection(self):
        load_dotenv()
        return psycopg2.connect(
            database=os.getenv("PG_DATABASE", "postgres"),
            user=os.getenv("PG_USERNAME", "postgres"),
            password=os.getenv("PG_PASSWORD", "postgres"),
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", "5432"),
        )
