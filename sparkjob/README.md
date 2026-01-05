# Spark Iceberg ETL

This job consumes presigned URLs from Kafka, downloads the archived GitHub events from S3/MinIO, and appends them as Iceberg tables in S3.

## Environment

| Variable | Purpose | Default |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | Kafka bootstrap servers | `kafka-service:9092` |
| `KAFKA_TOPIC` | Kafka topic carrying presigned URLs | `gh-archive-urls` |
| `KAFKA_STARTING_OFFSETS` | `earliest` or `latest` | `latest` |
| `S3_ENDPOINT` | S3/MinIO endpoint URL | `http://minio:9000` |
| `S3_ACCESS_KEY` | Access key for S3/MinIO | `minioadmin` |
| `S3_SECRET_KEY` | Secret key for S3/MinIO | `minioadmin` |
| `ICEBERG_WAREHOUSE` | Warehouse root for Iceberg tables | `s3a://iceberg-data/gh-archive` |
| `CHECKPOINT_LOCATION` | Streaming checkpoint path | `s3a://checkpoints/gh-etl/` |

## Running with Spark Operator

1. Mount secrets/env for `S3_ACCESS_KEY` and `S3_SECRET_KEY` (or use IAM).
2. Ensure the Spark image includes Iceberg and Kafka connectors.
3. Set the entrypoint to `spark-submit etl.py` (or `main.py`).
4. Provide `--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` if not baked in.
5. Confirm the checkpoint bucket and warehouse bucket exist and are accessible.
