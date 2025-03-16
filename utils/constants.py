# built-in imports
from datetime import datetime, timedelta
import os

# Airflow imports
from airflow.models import Variable


START_DATE = datetime(2025,1,1)

# Defines default behaviour for DAGs
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": START_DATE,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

DEFAULT_SCHEDULE = "0 9 * * *"

BREWERY_API_BASE_URL = "https://api.openbrewerydb.org/breweries"
BREWERY_API_PAGINATION_LIMIT = 50
BREWERY_API_PAGINATION_OFFSET = int(Variable.get("BREWERY_API_PAGE_OFFSET", default_var=0))
BREWERY_META_ENDPOINT = "/meta"
BREWERY_EXTRACTION_TASKS_GROUPED_BY = 40

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

BRONZE_LAYER_PATH = 'data/bronze'
SILVER_LAYER_PATH = 'data/silver'
GOLD_LAYER_PATH = 'data/gold'

DUCKDB_CONNECTION = f"{AIRFLOW_HOME}/db/duckdb.db"

EXTRACTION_DAG_ID = "brewery_bronze_extraction_from_api"
