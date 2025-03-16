from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import time

import json
import re

# Loads constans for all dags
from utils import constants
from utils.operators.api_extract_operator import SimpleApiExtractOperator

DEFAULT_TASK_TO_BRANCH = 'set_final_result_variable'
EXTRACTION_BLOCK_TASK_ID = 'extraction_block'

def update_brewery_variables(**kwargs):
    task_instance = kwargs["ti"]  # Task instance
    upstream_task_ids = kwargs["task"].upstream_task_ids
    task_instance.log.info(f"Upsteram tasks {upstream_task_ids}")
    success_tasks = [
        task_id.split('.')[-1] # gets only task id through
        # task_id
            for task_id in upstream_task_ids
                if 
                    task_instance.get_dagrun().get_task_instance(task_id).state == "success" 
                    and bool(re.match(r'^brewery_api_extract_offset_page_\d+$', task_id.split('.')[-1]))
    ]

    task_instance.log.info(f"Tasks that succeded {success_tasks}")
    
    last_value = set(Variable.get('BREWERY_API_PAGINATION_PARTITIONS',deserialize_json=True))
    total_partitions = list(last_value | set(success_tasks))

    Variable.set('BREWERY_API_PAGINATION_PARTITIONS',json.dumps(total_partitions))

    # updates BREWERY_API_PAGE_OFFSET with BREWERY_API_TOTAL.total
    Variable.set(
        "BREWERY_API_PAGE_OFFSET",
        Variable.get('BREWERY_API_TOTAL',deserialize_json=True).get('total')
    )

def wait_function():
    time.sleep(60)

def extract_api_or_branch_out(
    reprocess_block: bool, 
    condition: bool,
    task_id_if_true: str,
    task_id_if_false: str,
    **kwargs
):
    dag_run = kwargs['dag_run']
    available_tasks = list(dag_run.dag.task_dict.keys())  # Print all tasks
    print(condition, task_id_if_true, task_id_if_false)
    print("🔥 Available tasks in DAG:", available_tasks)  # Debugging output
    return task_id_if_true if reprocess_block else task_id_if_false

# Create the DAG
with DAG(
    dag_id=constants.EXTRACTION_DAG_ID,
    default_args=constants.DEFAULT_ARGS,
    schedule_interval=constants.DEFAULT_SCHEDULE,
    catchup=False,
    tags=["api", "bronze", "extraction"],
) as dag:
    # dummy tasks to start and end DAG
    start_task = DummyOperator(task_id="start")
    end_task = DummyOperator(task_id="end")

    # creates final task to update variables used in DAG
    update_partitions_and_offset_variable = PythonOperator(
        task_id=DEFAULT_TASK_TO_BRANCH,
        python_callable=update_brewery_variables,
        provide_context=True,
        trigger_rule="none_failed_or_skipped"
    )

    # creates central data_processing group
    with TaskGroup("data_processing") as data_processing:
        # generates total API values and sets variable 'BREWERY_API_TOTAL'
        get_total_breweries = SimpleApiExtractOperator(
            task_id="brewery_api_extract_total",
            api_url=f"{constants.BREWERY_API_BASE_URL}{constants.BREWERY_META_ENDPOINT}",
            set_response_to_variable='BREWERY_API_TOTAL',
        )

        wait_task = PythonOperator(
            task_id="wait_period",
            python_callable=wait_function,
        )

        get_total_breweries >> wait_task
        # creates dependencies between start and end task dummy operator
        start_task >> get_total_breweries >> wait_task >> update_partitions_and_offset_variable >> end_task
        
        # creates total pages to paginate over
        total_records = int(Variable.get('BREWERY_API_TOTAL', deserialize_json=True).get('total'))
        total_pages = int(total_records/constants.BREWERY_API_PAGINATION_LIMIT)
        
        # checks if any pages need to be reprocessed, based on offset
        total_offseted_records = int(Variable.get('BREWERY_API_PAGE_OFFSET'))
        offset_differences = total_records - total_offseted_records
        recreate_pages = int(offset_differences / constants.BREWERY_API_PAGINATION_LIMIT)
        total_pages_to_recreate = [i for i in range(total_pages,total_pages-(recreate_pages+1),-1)] if total_offseted_records != total_records else []
        
        # creates individual task groups for api_extraction, grouping N groups for each 30 extraction tasks
        tasks_grouped_by = constants.BREWERY_EXTRACTION_TASKS_GROUPED_BY
        total_task_groups = int(total_pages / tasks_grouped_by) 
        task_groups = []
        for i in range(1,total_task_groups+2):
            task_groups.append(TaskGroup(f"{EXTRACTION_BLOCK_TASK_ID}_{i}"))

        # gets variable that has task name, to see if it was already materialized or not
        steps_already_materialized = Variable.get('BREWERY_API_PAGINATION_PARTITIONS', deserialize_json=True)

        # generate extractions for each group, using SimpleApiExtractOperator operator
        for i in range(1,total_pages+1):
            selected_task_group = int((i // tasks_grouped_by) * tasks_grouped_by / tasks_grouped_by)
            with task_groups[selected_task_group] as extraction_block:
                api_offset_task_id = f"brewery_api_extract_offset_page_{i}"
                
                # checks if the step has already been materialized or if new data has arrived
                reprocess_block = True if (api_offset_task_id not in steps_already_materialized) or (i in total_pages_to_recreate) else False

                # gets API results, writing to a file and creating parameters for querying the API
                get_offseted_results = SimpleApiExtractOperator(
                    task_id=api_offset_task_id,
                    api_url=constants.BREWERY_API_BASE_URL,
                    write_to_file=f"{constants.AIRFLOW_HOME}/{constants.BRONZE_LAYER_PATH}/{api_offset_task_id}.json",
                    query_parameters={
                        "per_page": constants.BREWERY_API_PAGINATION_LIMIT,
                        "page": str(i)
                    }
                )
                
                # creates branching operator, to test if block will be skipped (no new data or step was already materialized)
                branch_task = BranchPythonOperator(
                    task_id=f"branch_task_{api_offset_task_id}",
                    python_callable=extract_api_or_branch_out,
                    op_kwargs={
                        "reprocess_block": reprocess_block, 
                        "condition": total_pages_to_recreate,
                        "task_id_if_true": f"data_processing.{EXTRACTION_BLOCK_TASK_ID}_{str(selected_task_group+1)}.{api_offset_task_id}", # example "data_processing.extraction_block_1.brewery_api_extract_offset_page_2"
                        "task_id_if_false": f"data_processing.{EXTRACTION_BLOCK_TASK_ID}_{str(selected_task_group+1)}.branch_skip_dummy_{i}" # example "data_processing.extraction_block_1.branch_skip_dummy_2"
                    },
                )

                # skip task dummy for later usage
                branch_skip_task = DummyOperator(task_id=f"branch_skip_dummy_{i}")


                # creates inicial dependency between total API call, branch tasks and their respective branching options and final variable update task
                wait_task >> branch_task >> [get_offseted_results , branch_skip_task] >> update_partitions_and_offset_variable
