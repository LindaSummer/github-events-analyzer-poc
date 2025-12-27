from __future__ import annotations

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="simple_hello_world",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["demo", "hello-world"],
)
def simple_hello_world():
    @task
    def hello() -> str:
        message = "Hello, World from Airflow 3!"
        print(message)
        return message

    @task
    def goodbye(msg) -> None:
        print(f"Received: {msg}")
        print("Goodbye!")

    greeting = hello()
    goodbye(greeting)


dag = simple_hello_world()
