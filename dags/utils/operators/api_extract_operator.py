import requests
import json
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

class SimpleApiExtractOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, 
        api_url: str, 
        write_to_file: str = None,
        push_to_xcom: bool = False,
        pull_xcom_from_task: bool = False,
        set_response_to_variable: str = '',
        query_parameters: dict = {},
        *args, 
        **kwargs
    ):
        # super().__init__(*args, **kwargs)
        recognized_kwargs = {k: v for k, v in kwargs.items() if k in ["task_id", "dag"]}
        super().__init__(**recognized_kwargs)
        self.api_url = api_url
        self.write_to_file = write_to_file
        self.push_to_xcom = push_to_xcom
        self.pull_xcom_from_task = pull_xcom_from_task
        self.query_parameters = query_parameters
        self.set_response_to_variable = set_response_to_variable

    def execute(
        self, 
        context
    ):
        task_instance = context['ti']  # 'ti' is the TaskInstance object
        task_instance.log.info("Executing SimpleApiExtractOperator!")
        
        # checks if there are more parameters to make the request
        if self.query_parameters:
            added_params = "&".join([f"{k}={v}" for k, v in self.query_parameters.items()])
            task_instance.log.info(f"Adding parameters to query. Built parameters are {added_params}")
        
        define_final_url = f"{self.api_url}{('?'+added_params) if self.query_parameters else ''}"

        self.log.info(f"Fetching data from API URL: {define_final_url} ")
        response = requests.get(define_final_url, verify=False)

        if response.status_code == 200:
            if self.set_response_to_variable:
                Variable.set(key=self.set_response_to_variable,value=json.dumps(response.json()))

            if self.push_to_xcom:
                task_instance.xcom_push(key="api_response", value=response.json())
                task_instance.log.info(f"Pushed value {self.push_to_xcom} to XCOM!")
            
            if self.pull_xcom_from_task:
                task_instance.xcom_pull(task_ids="push_task", key="api_response")
                task_instance.log.info(f"Pulling value {self.pull_xcom_from_task} from XCOM!")

            if self.write_to_file:
                task_instance.log.info("Writing data to file")
                data = response.json()
                with open(self.write_to_file, "w+") as file:
                    json.dump(data, file, indent=4)
                self.log.info(f"Data saved to {self.write_to_file}")
        else:
            raise Exception(f"API Request Failed! Status Code: {response.status_code}")
