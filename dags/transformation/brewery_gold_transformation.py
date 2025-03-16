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
    GOLD_LAYER_PATH, 
)

# Create the DAG
with DAG(
    dag_id="brewery_gold_transformation",
    default_args=DEFAULT_ARGS,
    schedule_interval=DEFAULT_SCHEDULE,
    catchup=False,
    tags=["transformation", "gold", "aggregation"],
) as dag:
    
    start_task = DummyOperator(task_id="start")
    end_task = DummyOperator(task_id="end")
    
    wait_for_extration_dependency = ExternalTaskSensor(
        task_id=f"wait_for_brewery_silver_transformation",
        external_dag_id='brewery_silver_transformation',
        external_task_id="end",
        mode="poke",
        timeout=600,
        poke_interval=30,
        execution_delta=timedelta(days=0)
    )

    create_delta_table = DuckDBOperator(
        task_id="aggregate_results",
        duckdb_conn=":memory:",
        sql=f"""
            SELECT 
                country,
                brewery_type,
                count(1) as quantity
            FROM 
                delta_scan('./data/silver/brewery_delta_table')
            group by
                country,
                brewery_type
        """,
        output_path=f"{AIRFLOW_HOME}/{GOLD_LAYER_PATH}/breweries_total_per_type_and_country_delta_table",
    )

    start_task >> wait_for_extration_dependency >> create_delta_table >> end_task