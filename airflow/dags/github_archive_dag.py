from __future__ import annotations

import gzip
import logging
import tempfile

import pendulum
import requests
from kafka import KafkaProducer

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

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
    def fetch_to_s3(logical_date=None) -> str:
        # Parse logical_date from context
        if isinstance(logical_date, str):
            logical_date = pendulum.parse(logical_date)
        
        target_dt = logical_date.subtract(hours=1)
        year = target_dt.format('YYYY')
        month = target_dt.format('MM')
        day = target_dt.format('DD')
        hour = target_dt.hour
        
        url = f"https://data.gharchive.org/{year}-{month}-{day}-{hour}.json.gz"

        bucket = Variable.get("GH_ARCHIVE_BUCKET", default_var="gharchive")
        prefix = Variable.get("GH_ARCHIVE_S3_PREFIX", default_var="gharchive")
        key = f"{prefix}/{year}/{month}/{day}/{hour}.json.gz"

        hook = S3Hook(aws_conn_id="minio")
        if not hook.check_for_bucket(bucket_name=bucket):
            logger.info(f"Bucket {bucket} does not exist. Creating bucket.")
            hook.create_bucket(bucket_name=bucket)
            logger.info(f"Bucket {bucket} created.")
        
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        logger.info(f"Successfully fetched data from {url}")
        resp.raw.decode_content = True

        # Write response to temporary file and upload
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json.gz") as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name
        
        logger.info(f"Saved fetched data to temporary file: {tmp_path}")
        
        hook.load_file(tmp_path, key=key, bucket_name=bucket, replace=True)
        logger.info(f"Successfully uploaded data to s3://{bucket}/{key}")
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