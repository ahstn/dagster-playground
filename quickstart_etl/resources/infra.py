import os
import psycopg2
from dagster import ConfigurableResource
from dotenv import load_dotenv


class PagilaDatabase(ConfigurableResource):

    def connection(self):
        load_dotenv()
        return psycopg2.connect(
            database=os.environ["PG_DATABASE"] or "postgres",
            user=os.environ["PG_USERNAME"] or "postgres",
            password=os.environ["PG_PASSWORD"] or "postgres",
            host=os.environ["PG_HOST"] or "localhost",
            port=os.environ["PG_PORT"] or "5432",
        )
