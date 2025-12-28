from datetime import datetime, timedelta
from airflow.sdk import task, dag


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id = "taskflow_api_v2",
    description = "A simple taskflow api DAG",
    start_date = datetime(2024, 6, 1),
    schedule='@daily'
)


def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "John", "last_name": "Doe"}

    @task
    def get_age():
        return 30
    
    @task
    def greet(first_name, last_name, age):  
        print(f"Hello World! My name is {first_name} {last_name}, and I am {age} years old")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greet_dag = hello_world_etl()