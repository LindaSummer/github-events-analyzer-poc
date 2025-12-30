FROM apache/airflow:3.0.2

RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    kafka-python \
    requests \
    pendulum \
    boto3

COPY ./dags/ /opt/airflow/dags/
