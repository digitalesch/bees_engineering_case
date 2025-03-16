from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.operators.dubckdb_operator import DuckDBOperator
from datetime import timedelta

# Loads constans for all dags
from utils.constants import (
    DEFAULT_ARGS, 
    DEFAULT_SCHEDULE, 
    AIRFLOW_HOME, 
    BRONZE_LAYER_PATH, 
    SILVER_LAYER_PATH,
    EXTRACTION_DAG_ID
)

# Create the DAG
with DAG(
    dag_id="brewery_silver_transformation",
    default_args=DEFAULT_ARGS,
    schedule_interval=DEFAULT_SCHEDULE,
    catchup=False,
    tags=["transformation", "silver", "cleaning"],
) as dag:
    
    start_task = DummyOperator(task_id="start")
    end_task = DummyOperator(task_id="end")
    
    wait_for_extration_dependency = ExternalTaskSensor(
        task_id=f"wait_for_{EXTRACTION_DAG_ID}",
        external_dag_id=EXTRACTION_DAG_ID,    # ✅ DAG ID of the external DAG
        external_task_id="end",  # ✅ Task ID of the external task
        mode="poke",                # "poke" or "reschedule" mode
        timeout=600,                # Wait for 10 minutes max
        poke_interval=30,           # Check every 30 seconds
        execution_delta=timedelta(days=0)  # ✅ Ensures same execution date
    )

    create_delta_table = DuckDBOperator(
        task_id="cleanup_data",
        duckdb_conn=":memory:",
        sql=f"""
            SELECT 
                cast(id as varchar) as brewery_id,
                name,
                brewery_type,
                address_1,
                address_2,
                address_3,
                city,
                state_province,
                postal_code,
                country,
                replace(trim(upper(country)),' ','_') as location,
                longitude,
                latitude,
                phone,
                website_url,
                state,
                street
            FROM 
                read_json_auto('{AIRFLOW_HOME}/{BRONZE_LAYER_PATH}/*.json');""",
        output_path=f"{AIRFLOW_HOME}/{SILVER_LAYER_PATH}/brewery_delta_table",
        parition_by="location"
    )

    start_task >> wait_for_extration_dependency >> create_delta_table >> end_task