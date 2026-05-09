from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_dag_manual",
)
def xcoms_dag_manual():

    @task.python
    def first_task(**kwargs):
        #Extracting Ti(task instance) from kwargs to push xcoms manually
        ti = kwargs["ti"]
        print("Extracting data...This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        ti.xcom_push(key="return_result", value=fetched_data)
    
    @task.python
    def second_task(**kwargs):
        ti = kwargs["ti"]
        fetched_data = ti.xcom_pull(task_ids="first_task", key="return_result")["data"]
        print("Transforming data... This is the second task")
        transform_data = fetched_data*2
        transform_data_dict = {"transf_data":transform_data}
        ti.xcom_push(key="return_result", value=transform_data_dict)

    @task.python
    def third_task(**kwargs):
        ti = kwargs["ti"]
        load_data = ti.xcom_pull(task_ids="second_task", key="return_result")
        return load_data
    
    #Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#Instantiating the dag
xcoms_dag_manual()