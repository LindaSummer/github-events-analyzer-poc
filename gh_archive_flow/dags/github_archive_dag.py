from __future__ import annotations

from collections import namedtuple
import logging

import boto3
import pendulum
import requests
from kafka import KafkaProducer

from airflow.decorators import dag, task
from airflow.models import Variable

from gh_archive_flow.utils import ProgressPercentage

logger = logging.getLogger(__name__)

default_args = {"owner": "airflow"}

# create namedtuple, for key and presigned url
S3Object = namedtuple("S3Object", ["presigned_url", "key"])

@dag(
    dag_id="github_archive_minio_kafka",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["gh-archive", "minio", "kafka"],
)
def github_archive_minio_kafka():
    bucket = Variable.get("GH_ARCHIVE_BUCKET", default_var="gharchive")
    prefix = Variable.get("GH_ARCHIVE_S3_PREFIX", default_var="gharchive")
    endpoint = Variable.get("GH_ARCHIVE_S3_ENDPOINT", default_var=None)
    access_key = Variable.get("GH_ARCHIVE_ACCESS_KEY")
    secret_key = Variable.get("GH_ARCHIVE_SECRET_KEY")
    
    @task
    def download_github_archive(logical_date=None) -> S3Object:
        """Download GitHub Archive data for the previous hour."""
        # Parse logical_date from context
        if isinstance(logical_date, str):
            logical_date = pendulum.parse(logical_date)
        
        target_dt = logical_date.subtract(hours=1)
        year = target_dt.format('YYYY')
        month = target_dt.format('MM')
        day = target_dt.format('DD')
        hour = target_dt.format('H')
        
        url = f"https://data.gharchive.org/{year}-{month}-{day}-{hour}.json.gz"
        
        logger.info(f"Downloading from {url}")
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        logger.info(f"Uploading file from {url} to S3")
        
        year = target_dt.format('YYYY')
        month = target_dt.format('MM')
        day = target_dt.format('DD')
        hour = target_dt.format('HH')
        
        key = f"{prefix}/{year}/{month}/{day}/{hour}.json.gz"
        
        s3 = boto3.client(
            "s3",
            **({
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key,
                } |
                ({"endpoint_url": endpoint} if endpoint else {})),
        )
        
        content_length = int(resp.headers.get('content-length', 0))
        
        s3.upload_fileobj(Fileobj=ProgressPercentage(total_size=content_length, stream=resp.raw),
                          Bucket=bucket,
                          Key=key,
                          ExtraArgs={"ContentType": resp.headers.get("Content-Type"), "Metadata": {"original-url": url}},
                          )
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
        
        # generate presigned url (valid for 30 days)
        presigned_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=30 * 24 * 60 * 60,  # 30 days
        )
        logger.info(f"generate presigned url for s3://{bucket}/{key}: {presigned_url}")
        return S3Object(presigned_url=presigned_url, key=key)

    @task
    def push_to_kafka(s3_object: S3Object) -> None:
        """Download file from S3 and push the object reference to Kafka."""
        logger.info(f"Pushing events from {s3_object.key} to Kafka")
        topic = Variable.get("GH_ARCHIVE_KAFKA_TOPIC", default_var="github-events")
        bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var="kafka:9092").split(",")

        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks="all")
        producer.init_transactions()
        producer.send(topic, key=s3_object.key.encode("utf-8"), value=s3_object.presigned_url.encode("utf-8"))
        
        producer.flush()
        producer.close()
        logger.info(f"Pushed {s3_object.key} to Kafka topic {topic}")

    # Task pipeline: download -> upload -> push
    s3_object = download_github_archive()
    push_to_kafka(s3_object)

dag = github_archive_minio_kafka()