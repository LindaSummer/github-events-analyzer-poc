from __future__ import annotations

import gzip
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from kafka import KafkaProducer

default_args = {"owner": "airflow"}


@dag(
    dag_id="github_archive_minio_kafka",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["gh-archive", "minio", "kafka"],
)
def github_archive_minio_kafka():
    @task
    def fetch_to_s3(
        year: str = "{{ (logical_date.subtract(hours=1)).format('YYYY') }}",
        month: str = "{{ (logical_date.subtract(hours=1)).format('MM') }}",
        day: str = "{{ (logical_date.subtract(hours=1)).format('DD') }}",
        hour: str = "{{ (logical_date.subtract(hours=1)).hour }}",
    ) -> str:
        url = f"https://data.gharchive.org/{year}-{month}-{day}-{hour}.json.gz"

        bucket = Variable.get("GH_ARCHIVE_BUCKET", default_var="gharchive")
        prefix = Variable.get("GH_ARCHIVE_S3_PREFIX", default_var="gharchive")
        key = f"{prefix}/{year}/{month}/{day}/{hour}.json.gz"

        hook = S3Hook(aws_conn_id="minio")
        if not hook.check_for_bucket(bucket_name=bucket):
            hook.create_bucket(bucket_name=bucket)

        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        resp.raw.decode_content = True

        hook.load_file_obj(file_obj=resp.raw, key=key, bucket_name=bucket, replace=True)
        return key

    @task
    def send_to_kafka(s3_key) -> int:
        bucket = Variable.get("GH_ARCHIVE_BUCKET", default_var="gh-archive")
        topic = Variable.get("GH_ARCHIVE_KAFKA_TOPIC", default_var="github-events-archive")
        bootstrap = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var="github-events-kafka-kafka-bootstrap.github-events-poc:9092").split(",")

        hook = S3Hook(aws_conn_id="minio")
        obj = hook.get_key(key=s3_key, bucket_name=bucket)
        body = obj.get()["Body"]

        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all")
        count = 0
        with gzip.GzipFile(fileobj=body) as gz:
            for line in gz:
                line = line.strip()
                if not line:
                    continue
                producer.send(topic, line)
                count += 1
        producer.flush()
        producer.close()
        return count

    uploaded = fetch_to_s3()
    send_to_kafka(uploaded)


dag = github_archive_minio_kafka()